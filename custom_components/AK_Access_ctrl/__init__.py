from __future__ import annotations

import asyncio
import os
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple, Callable

from homeassistant.const import Platform
from homeassistant.core import HomeAssistant, callback
from homeassistant.config_entries import ConfigEntry
from homeassistant.helpers.storage import Store
from homeassistant.helpers.event import (
    async_call_later,
    async_track_time_change,
    async_track_time_interval,
)
from homeassistant.helpers.aiohttp_client import async_get_clientsession

from .const import (
    DOMAIN,
    PLATFORMS,
    GROUPS_STORAGE_KEY,
    USERS_STORAGE_KEY,
    CONF_DEVICE_NAME,
    CONF_DEVICE_TYPE,
    CONF_HOST,
    CONF_PORT,
    CONF_USERNAME,
    CONF_PASSWORD,
    DEFAULT_USE_HTTPS,
    DEFAULT_VERIFY_SSL,
    DEFAULT_POLL_INTERVAL,
    CONF_PARTICIPATE,
    CONF_POLL_INTERVAL,
    CONF_DEVICE_GROUPS,
    ENTRY_VERSION,
)

from .api import AkuvoxAPI
from .coordinator import AkuvoxCoordinator
from .http import register_ui  # provides /api/akuvox_ac/ui/*

HA_EVENT_ACCCESS = "akuvox_access_event"  # fired for access denied / exit override

# Face images base URL (public HA, capital FaceData as per device requirement)
FACE_BASE_URL = "http://149.40.108.146:8123/local/akuvox_ac/FaceData"


# ---------------------- Helpers ---------------------- #
def _now_hh_mm() -> str:
    try:
        return datetime.now().strftime("%H:%M")
    except Exception:
        return ""


def _key_of_user(u: Dict[str, Any]) -> str:
    return str(u.get("UserID") or u.get("ID") or u.get("Name") or "")


def _ha_id_from_int(n: int) -> str:
    return f"HA{n:03d}"  # no dash


def _is_ha_id(s: str) -> bool:
    return isinstance(s, str) and len(s) == 5 and s.startswith("HA") and s[2:].isdigit()


# ---------------------- Persistent stores ---------------------- #
class AkuvoxGroupsStore(Store):
    def __init__(self, hass: HomeAssistant):
        super().__init__(hass, 1, GROUPS_STORAGE_KEY)
        self.data: Dict[str, Any] = {"groups": ["Default"]}

    async def async_load(self):
        existing = await super().async_load()
        if existing and isinstance(existing.get("groups"), list):
            self.data = existing
        else:
            await self.async_save()

    async def async_save(self):
        await super().async_save(self.data)

    def groups(self) -> List[str]:
        return list(dict.fromkeys(self.data["groups"]))

    async def add_group(self, name: str):
        name = (name or "").strip()
        if name and name not in self.data["groups"]:
            self.data["groups"].append(name)
            await self.async_save()

    async def delete_groups(self, names: List[str]):
        keep = [g for g in self.data["groups"] if g not in (names or []) or g == "Default"]
        if keep != self.data["groups"]:
            self.data["groups"] = keep
            await self.async_save()


class AkuvoxSchedulesStore(Store):
    """Named week schedules stored centrally, synced to devices during reconcile."""
    def __init__(self, hass: HomeAssistant):
        super().__init__(hass, 1, f"{DOMAIN}_schedules.json")
        self.data: Dict[str, Any] = {"schedules": {}}

    async def async_load(self):
        x = await super().async_load()
        if x and isinstance(x.get("schedules"), dict):
            self.data = x
        sdata = self.data.get("schedules") or {}
        changed = False
        if "24/7 Access" not in sdata:
            sdata["24/7 Access"] = {d: [["00:00", "24:00"]] for d in ("mon", "tue", "wed", "thu", "fri", "sat", "sun")}
            changed = True
        if "No Access" not in sdata:
            sdata["No Access"] = {d: [] for d in ("mon", "tue", "wed", "thu", "fri", "sat", "sun")}
            changed = True
        if changed:
            self.data["schedules"] = sdata
            await self.async_save()

    async def async_save(self):
        await super().async_save(self.data)

    def all(self) -> Dict[str, Any]:
        return dict(self.data.get("schedules") or {})

    async def upsert(self, name: str, payload: Dict[str, Any]):
        self.data.setdefault("schedules", {})[name] = payload
        await self.async_save()

    async def delete(self, name: str):
        if name in ("24/7 Access", "No Access"):
            return
        self.data.setdefault("schedules", {}).pop(name, None)
        await self.async_save()


class AkuvoxUsersStore(Store):
    def __init__(self, hass: HomeAssistant):
        super().__init__(hass, 1, USERS_STORAGE_KEY)
        self.data: Dict[str, Any] = {"users": {}}

    async def async_load(self):
        existing = await super().async_load()
        if existing and isinstance(existing.get("users"), dict):
            self.data = existing

    async def async_save(self):
        await super().async_save(self.data)

    def get(self, key: str, default=None):
        return (self.data.get("users") or {}).get(key, default)

    def all(self) -> Dict[str, Any]:
        return dict(self.data.get("users") or {})

    def all_ha_ids(self) -> List[str]:
        return [k for k in (self.data.get("users") or {}).keys() if _is_ha_id(k)]

    def reserve_id(self, ha_id: str):
        self.data["users"].setdefault(ha_id, {})

    async def upsert_profile(
        self,
        key: str,
        *,
        name: Optional[str] = None,
        groups: Optional[List[str]] = None,
        pin: Optional[str] = None,
        face_url: Optional[str] = None,
        phone: Optional[str] = None,
        status: Optional[str] = None,
        schedule_name: Optional[str] = None,
        key_holder: Optional[bool] = None,
        access_level: Optional[str] = None,
        schedule_id: Optional[str] = None,  # allow explicit schedule ID (1001/1002/1003/…)
    ):
        u = self.data["users"].setdefault(key, {})
        if name is not None:
            u["name"] = name
        if groups is not None:
            u["groups"] = list(groups)
        if pin is not None:
            u["pin"] = str(pin) if pin not in (None, "") else ""
        if face_url is not None:
            u["face_url"] = face_url
        if phone is not None:
            u["phone"] = str(phone)
        if status is not None:
            u["status"] = status
        if schedule_name is not None:
            u["schedule_name"] = schedule_name
        if key_holder is not None:
            u["key_holder"] = bool(key_holder)
        if access_level is not None:
            u["access_level"] = access_level
        if schedule_id is not None:
            u["schedule_id"] = str(schedule_id)
        await self.async_save()

    async def delete(self, key: str):
        self.data.get("users", {}).pop(key, None)
        await self.async_save()


class AkuvoxSettingsStore(Store):
    def __init__(self, hass: HomeAssistant):
        super().__init__(hass, 1, f"{DOMAIN}_settings.json")
        self.data: Dict[str, Any] = {
            "auto_sync_time": None,
            "auto_reboot": {"time": None, "days": []},
        }

    async def async_load(self):
        x = await super().async_load()
        if x:
            self.data = x

    async def async_save(self):
        await super().async_save(self.data)

    def get_auto_sync_time(self) -> Optional[str]:
        return self.data.get("auto_sync_time")

    async def set_auto_sync_time(self, hhmm: Optional[str]):
        self.data["auto_sync_time"] = hhmm
        await self.async_save()

    def get_auto_reboot(self) -> Dict[str, Any]:
        return dict(self.data.get("auto_reboot") or {"time": None, "days": []})

    async def set_auto_reboot(self, time_hhmm: Optional[str], days: List[str]):
        self.data["auto_reboot"] = {"time": time_hhmm, "days": list(days or [])}
        await self.async_save()


async def _allocate_lowest_ha_id(users_store: AkuvoxUsersStore, in_use_ids: List[str]) -> str:
    used = set(in_use_ids or [])
    used.update(users_store.all_ha_ids())
    n = 1
    while True:
        candidate = _ha_id_from_int(n)
        if candidate not in used:
            users_store.reserve_id(candidate)
            await users_store.async_save()
            return candidate
        n += 1


# ---------------------- Robust device user lookup + delete ---------------------- #
async def _lookup_device_user_ids_by_ha_key(api: AkuvoxAPI, ha_key: str) -> List[Dict[str, str]]:
    out: List[Dict[str, str]] = []
    try:
        dev_users = await api.user_list()
    except Exception:
        dev_users = []

    for u in dev_users or []:
        kid = _key_of_user(u)
        if kid == ha_key:
            out.append(
                {
                    "ID": str(u.get("ID") or ""),
                    "UserID": str(u.get("UserID") or ""),
                    "Name": str(u.get("Name") or ""),
                }
            )
    return out


async def _delete_user_every_way(api: AkuvoxAPI, rec: Dict[str, str]):
    tried = set()

    async def try_one(val: str):
        if not val or val in tried:
            return
        tried.add(val)
        try:
            await api.user_delete(val)
        except Exception:
            pass

    await try_one(rec.get("ID", ""))
    await try_one(rec.get("UserID", ""))
    await try_one(rec.get("Name", ""))


# ---------------------- Debounced sync queue ---------------------- #
class SyncQueue:
    def __init__(self, hass: HomeAssistant):
        self.hass = hass
        self._handle: Optional[Callable[[], None]] = None
        self._lock = asyncio.Lock()
        self._pending_all = False
        self._pending_devices: set[str] = set()
        self.next_sync_eta: Optional[datetime] = None

    def _root(self) -> Dict[str, Any]:
        return self.hass.data.get(DOMAIN, {}) or {}

    def _mark_health_pending(self, entry_id: Optional[str]):
        root = self._root()

        def mark(coord: AkuvoxCoordinator):
            coord.health["sync_status"] = "pending"

        if entry_id:
            data = root.get(entry_id)
            if data and data.get("coordinator"):
                mark(data["coordinator"])
        else:
            for k, data in root.items():
                if k in (
                    "groups_store",
                    "users_store",
                    "schedules_store",
                    "sync_manager",
                    "sync_queue",
                    "_ui_registered",
                    "settings_store",
                ):
                    continue
                coord = data.get("coordinator")
                if coord:
                    mark(coord)

    def mark_change(self, entry_id: Optional[str] = None, delay_minutes: int = 30):
        self._mark_health_pending(entry_id)
        if entry_id:
            self._pending_devices.add(entry_id)
        else:
            self._pending_all = True

        if self._handle is not None:
            try:
                self._handle()
            except Exception:
                pass
            self._handle = None

        eta = datetime.now() + timedelta(minutes=delay_minutes)
        self.next_sync_eta = eta

        def _schedule_cb(_now):
            self.hass.async_create_task(self.run())

        self._handle = async_call_later(self.hass, delay_minutes * 60, _schedule_cb)

    async def run(self, only_entry: Optional[str] = None):
        async with self._lock:
            root = self._root()
            targets: List[Tuple[str, AkuvoxCoordinator, AkuvoxAPI]] = []
            if only_entry:
                data = root.get(only_entry)
                if data and data.get("coordinator") and data.get("api"):
                    targets.append((only_entry, data["coordinator"], data["api"]))
            else:
                for k, data in root.items():
                    if k in (
                        "groups_store",
                        "users_store",
                        "schedules_store",
                        "sync_manager",
                        "sync_queue",
                        "_ui_registered",
                        "settings_store",
                    ):
                        continue
                    coord = data.get("coordinator")
                    api = data.get("api")
                    if coord and api:
                        if not self._pending_all and self._pending_devices and k not in self._pending_devices:
                            continue
                        targets.append((k, coord, api))

            manager: SyncManager = root.get("sync_manager")  # type: ignore
            if not manager:
                self._handle = None
                self.next_sync_eta = None
                return

            for entry_id, coord, _api in targets:
                try:
                    await manager.reconcile_device(entry_id, full=True)
                    coord.health["sync_status"] = "in_sync"
                    coord.health["last_sync"] = _now_hh_mm()
                    try:
                        coord._append_event("Sync succeeded")  # type: ignore[attr-defined]
                    except Exception:
                        pass
                except Exception:
                    pass
                try:
                    await coord.async_request_refresh()
                except Exception:
                    pass

            self._pending_all = False
            self._pending_devices.clear()
            self._handle = None
            self.next_sync_eta = None

    async def sync_now(self, entry_id: Optional[str] = None):
        if self._handle is not None:
            try:
                self._handle()
            except Exception:
                pass
            self._handle = None
        self.next_sync_eta = None
        await self.run(only_entry=entry_id)


# ---------------------- Sync manager ---------------------- #
class SyncManager:
    """
    HA registry is the source of truth.
    Also: periodic 15-min integrity check when idle.
    30-min interval full reconcile.
    """

    def __init__(self, hass: HomeAssistant):
        self.hass = hass
        self._auto_unsub = None
        self._integrity_unsub = async_track_time_interval(
            hass,
            self._integrity_check_cb,
            timedelta(minutes=15),
        )
        self._reboot_unsub = None
        self._interval_unsub = async_track_time_interval(
            hass,
            self._interval_sync_cb,
            timedelta(minutes=30),
        )

    def _root(self) -> Dict[str, Any]:
        return self.hass.data.get(DOMAIN, {}) or {}

    def _users_store(self) -> AkuvoxUsersStore:
        return self._root().get("users_store")

    def _schedules_store(self) -> AkuvoxSchedulesStore:
        return self._root().get("schedules_store")

    def _settings_store(self) -> AkuvoxSettingsStore:
        return self._root().get("settings_store")

    def _devices(self) -> List[Tuple[str, AkuvoxCoordinator, AkuvoxAPI, Dict[str, Any]]]:
        out: List[Tuple[str, AkuvoxCoordinator, AkuvoxAPI, Dict[str, Any]]] = []
        for k, v in self._root().items():
            if k in (
                "groups_store",
                "users_store",
                "schedules_store",
                "sync_manager",
                "sync_queue",
                "_ui_registered",
                "settings_store",
            ):
                continue
            coord = v.get("coordinator")
            api = v.get("api")
            opts = v.get("options") or {}
            if coord and api:
                out.append((k, coord, api, opts))
        return out

    def set_auto_sync_time(self, hhmm: str):
        if not isinstance(hhmm, str) or ":" not in hhmm:
            raise ValueError("Invalid time format")
        settings: AkuvoxSettingsStore = self._settings_store()
        self.hass.async_create_task(settings.set_auto_sync_time(hhmm))

        if self._auto_unsub:
            try:
                self._auto_unsub()
            except Exception:
                pass
            self._auto_unsub = None

        hh, mm = [int(x) for x in hhmm.split(":", 1)]

        def _cb(now):
            try:
                self._root()["sync_queue"].mark_change(None, delay_minutes=0)
            except Exception:
                pass
            self.hass.async_create_task(self._root()["sync_queue"].sync_now(None))  # type: ignore

        self._auto_unsub = async_track_time_change(self.hass, _cb, hour=hh, minute=mm, second=0)

    def get_next_sync_text(self) -> str:
        sq: SyncQueue = self._root().get("sync_queue")
        if sq and sq.next_sync_eta:
            return sq.next_sync_eta.strftime("%H:%M")
        settings: AkuvoxSettingsStore = self._settings_store()
        return settings.get_auto_sync_time() or "—"

    def set_auto_reboot(self, time_hhmm: Optional[str], days: List[str]):
        settings: AkuvoxSettingsStore = self._settings_store()
        self.hass.async_create_task(settings.set_auto_reboot(time_hhmm, days))

    # ---------- NEW: device schedule map ----------
    async def _device_schedule_map(self, api: AkuvoxAPI) -> Dict[str, str]:
        """
        Return Name->ScheduleID map as strings for this device.
        Always includes the built-ins:
          '24/7 Access' -> '1001'
          'No Access'   -> '1002'
        Then overlays whatever the device reports via schedule_get().
        """
        name_to_id: Dict[str, str] = {
            "24/7 access": "1001",
            "no access": "1002",
        }
        try:
            dev_scheds = await api.schedule_get()  # [{"Name": "...", "ScheduleID":"1xxx"}, ...]
            for it in dev_scheds or []:
                n = str(it.get("Name") or "").strip()
                sid = str(it.get("ScheduleID") or "").strip()
                if n and sid:
                    name_to_id[n.lower()] = sid
        except Exception:
            # best-effort; built-ins still usable
            pass
        return name_to_id

    async def _replace_user_on_device(self, api: AkuvoxAPI, desired: Dict[str, Any], ha_key: str):
        """Delete the device record for ha_key (by ID/UserID/Name) then re-add with desired payload."""
        del_key = desired.get("ID") or desired.get("UserID") or desired.get("Name") or ha_key
        try:
            await api.user_delete(str(del_key))
        except Exception:
            try:
                recs = await _lookup_device_user_ids_by_ha_key(api, ha_key)
                for rec in recs:
                    await _delete_user_every_way(api, rec)
            except Exception:
                pass
        await asyncio.sleep(0.25)
        try:
            await api.user_add([desired])
        except Exception:
            pass

    async def reconcile(self, full: bool = True):
        for entry_id, *_ in self._devices():
            await self.reconcile_device(entry_id, full=full)

    async def _sync_contacts_for_intercom(self, api: AkuvoxAPI, registry_items: List[Dict[str, Any]]):
        to_set = []
        to_add = []
        for prof in registry_items:
            phone = prof.get("phone")
            name = prof.get("name") or prof.get("id") or ""
            if not phone or not name:
                continue
            to_set.append({"Name": name, "Phone": str(phone), "DialType": "0"})
            to_add.append({"Name": name, "Phone": str(phone), "DialType": "0"})
        if to_set:
            try:
                await api.contact_set(to_set)
            except Exception:
                pass
        if to_add:
            try:
                await api.contact_add(to_add)
            except Exception:
                pass

    async def _push_schedules(self, api: AkuvoxAPI, schedules: Dict[str, Any]):
        if not schedules:
            return
        for name, spec in (schedules or {}).items():
            if name in ("24/7 Access", "No Access"):
                continue
            try:
                await api.schedule_set(name, spec)
            except Exception:
                try:
                    await api.schedule_add(name, spec)
                except Exception:
                    pass

    async def _remove_missing_users(self, api: AkuvoxAPI, local_users: List[Dict[str, Any]], registry_keys_set: set):
        rogue_keys: List[str] = []
        for u in local_users or []:
            kid = _key_of_user(u)
            if _is_ha_id(kid) and kid not in registry_keys_set:
                rogue_keys.append(kid)
        if not rogue_keys:
            return
        for ha_key in rogue_keys:
            try:
                recs = await _lookup_device_user_ids_by_ha_key(api, ha_key)
                if recs:
                    for rec in recs:
                        await _delete_user_every_way(api, rec)
                else:
                    try:
                        await api.user_delete(ha_key)
                    except Exception:
                        pass
            except Exception:
                pass

    async def reconcile_device(self, entry_id: str, full: bool = True):
        root = self._root()
        data = root.get(entry_id)
        if not data:
            return
        coord: AkuvoxCoordinator = data.get("coordinator")
        api: AkuvoxAPI = data.get("api")
        opts = data.get("options") or {}
        if not coord or not api:
            return

        try:
            local_users: List[Dict[str, Any]] = await api.user_list()
        except Exception:
            local_users = list(coord.users or [])
        try:
            coord.users = local_users
        except Exception:
            pass

        if not opts.get("sync_groups"):
            opts["sync_groups"] = ["Default"]

        device_groups: List[str] = list(opts.get("sync_groups", ["Default"]))
        users_store = self._users_store()
        schedules_store = self._schedules_store()

        device_type = (coord.health.get("device_type") or "").strip().lower()
        is_intercom = device_type == "intercom"

        registry: Dict[str, Any] = users_store.all() if users_store else {}
        registry_keys = [k for k in registry.keys() if _is_ha_id(k)]
        reg_key_set = set(registry_keys)

        await self._remove_missing_users(api, local_users, reg_key_set)

        if full and schedules_store:
            try:
                await self._push_schedules(api, schedules_store.all())
            except Exception:
                pass

        # Resolve device schedule IDs after pushing (so we use what the device knows)
        sched_map = await self._device_schedule_map(api)

        def _find_local_by_key(ha_key: str) -> Optional[Dict[str, Any]]:
            for u in local_users:
                if _key_of_user(u) == ha_key:
                    return u
            return None

        add_batch: List[Dict[str, Any]] = []
        replace_list: List[Tuple[str, Dict[str, Any]]] = []  # (ha_key, desired_payload)
        delete_only_keys: List[str] = []

        for ha_key in registry_keys:
            prof = registry.get(ha_key) or {}
            ha_groups = list(prof.get("groups") or ["Default"])
            should_have_access = any(g in device_groups for g in ha_groups)
            local = _find_local_by_key(ha_key)

            # ----- Schedule / relays -----
            schedule_name = (prof.get("schedule_name") or "24/7 Access").strip()
            key_holder = bool(prof.get("key_holder"))
            exit_override = bool(opts.get("exit_device"))
            effective_schedule = "24/7 Access" if exit_override else schedule_name

            explicit_id = str(prof.get("schedule_id") or "").strip()
            if explicit_id and explicit_id.isdigit():
                schedule_id = explicit_id
            else:
                schedule_id = sched_map.get(effective_schedule.lower(), "1001")

            relay_suffix = "12" if key_holder else "1"
            schedule_relay = f"{schedule_id}-{relay_suffix};"  # e.g. "1001-12;" or "1001-1;"

            # ----- Build device payload -----
            face_url_canonical = f"{FACE_BASE_URL}/{ha_key}.jpg"

            desired_base: Dict[str, Any] = {
                "UserID": ha_key,
                "ID": (local or {}).get("ID") or ha_key,
                "Name": prof.get("name") or ha_key,
                "WebRelay": "0",
                "ScheduleRelay": schedule_relay,
                # Hints some firmwares accept:
                "ScheduleID": schedule_id,
                "Schedule": effective_schedule,
                # Face URL: send both casings for compatibility
                "FaceUrl": face_url_canonical,
                "FaceURL": face_url_canonical,
            }
            if "pin" in prof and prof.get("pin") not in (None, ""):
                desired_base["PrivatePIN"] = str(prof["pin"])
            if prof.get("phone"):
                desired_base["PhoneNum"] = str(prof["phone"])

            # If UI stored face_url, let it override the canonical path
            if prof.get("face_url"):
                desired_base["FaceUrl"] = prof["face_url"]
                desired_base["FaceURL"] = prof["face_url"]

            if should_have_access:
                if not local:
                    add_batch.append(desired_base)
                else:
                    # replace on pending or any diff
                    replace = str(prof.get("status") or "").lower() == "pending" or any(
                        str(local.get(k)) != str(v) for k, v in desired_base.items()
                    )
                    if replace:
                        replace_list.append((ha_key, desired_base))
            else:
                if local:
                    delete_only_keys.append(ha_key)
            # -----------------------------------------

        # 1) Add new users
        if add_batch:
            try:
                await api.user_add(add_batch)
            except Exception:
                pass

        # 2) Delete-only
        for ha_key in delete_only_keys:
            try:
                recs = await _lookup_device_user_ids_by_ha_key(api, ha_key)
                if recs:
                    for rec in recs:
                        await _delete_user_every_way(api, rec)
                else:
                    try:
                        await api.user_delete(ha_key)
                    except Exception:
                        pass
            except Exception:
                pass

        # 3) Replace changed users (delete + re-add)
        for ha_key, desired in replace_list:
            try:
                await self._replace_user_on_device(api, desired, ha_key)
            except Exception:
                pass

        # Mark pending -> active
        try:
            for k in registry_keys:
                if (registry.get(k) or {}).get("status") == "pending":
                    await users_store.upsert_profile(k, status="active")
        except Exception:
            pass

        if is_intercom:
            reg_items = [{"id": k, **(registry.get(k) or {})} for k in registry_keys]
            try:
                await self._sync_contacts_for_intercom(api, reg_items)
            except Exception:
                pass

        try:
            await coord.async_request_refresh()
        except Exception:
            pass

    async def _interval_sync_cb(self, now):
        await self.reconcile(full=True)

    async def _integrity_check_cb(self, now):
        root = self._root()
        sq = root.get("sync_queue")
        if sq and getattr(sq, "_handle", None) is not None:
            return

        for _, coord, *_ in self._devices():
            if coord.health.get("sync_status") != "in_sync":
                return

        users_store = self._users_store()
        registry = users_store.all() if users_store else {}
        reg_keys = [k for k in registry.keys() if _is_ha_id(k)]

        for entry_id, coord, api, opts in self._devices():
            try:
                dev_users = await api.user_list()
                coord.users = dev_users or []
                local_keys = set(_key_of_user(u) for u in (coord.users or []))

                device_groups: List[str] = list((opts or {}).get("sync_groups", ["Default"]))
                should_have = set()
                for k in reg_keys:
                    prof = registry.get(k) or {}
                    ha_groups = list(prof.get("groups") or ["Default"])
                    if any(g in device_groups for g in ha_groups):
                        should_have.add(k)

                if should_have == local_keys.intersection(should_have):
                    try:
                        coord._append_event("Integrity check passed")  # type: ignore[attr-defined]
                    except Exception:
                        pass
                else:
                    try:
                        coord._append_event("Integrity mismatch — queued sync")  # type: ignore[attr-defined]
                    except Exception:
                        pass
                    sq.mark_change(entry_id)
                    await sq.sync_now(entry_id)
            except Exception:
                try:
                    coord._append_event("Integrity check error")  # type: ignore[attr-defined]
                except Exception:
                    pass


# ---------------------- Setup / teardown ---------------------- #
async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry):
    hass.data.setdefault(DOMAIN, {})
    root = hass.data[DOMAIN]

    if "groups_store" not in root:
        gs = AkuvoxGroupsStore(hass)
        await gs.async_load()
        us = AkuvoxUsersStore(hass)
        await us.async_load()
        schedules = AkuvoxSchedulesStore(hass)
        await schedules.async_load()
        settings = AkuvoxSettingsStore(hass)
        await settings.async_load()

        root["groups_store"] = gs
        root["users_store"] = us
        root["schedules_store"] = schedules
        root["settings_store"] = settings
        root["sync_manager"] = SyncManager(hass)
        root["sync_queue"] = SyncQueue(hass)

        t = settings.get_auto_sync_time()
        if t:
            try:
                root["sync_manager"].set_auto_sync_time(t)
            except Exception:
                pass
        ar = settings.get_auto_reboot()
        if ar and (ar.get("time") and (ar.get("days"))):
            try:
                root["sync_manager"].set_auto_reboot(ar.get("time"), ar.get("days"))
            except Exception:
                pass

    cfg = {**entry.data, **entry.options}
    session = async_get_clientsession(hass)

    api = AkuvoxAPI(
        host=cfg.get(CONF_HOST),
        port=cfg.get(CONF_PORT, 80),
        username=cfg.get(CONF_USERNAME) or None,
        password=cfg.get(CONF_PASSWORD) or None,
        use_https=cfg.get("use_https", DEFAULT_USE_HTTPS),
        verify_ssl=cfg.get("verify_ssl", DEFAULT_VERIFY_SSL),
        session=session,
    )

    storage = AkuvoxStorage(hass, entry.entry_id)
    await storage.async_load()

    device_name = cfg.get(CONF_DEVICE_NAME, entry.title)
    device_type = cfg.get(CONF_DEVICE_TYPE, "Intercom")

    coord = AkuvoxCoordinator(hass, api, storage, entry.entry_id, device_name)
    coord.health["device_type"] = device_type
    coord.health["ip"] = cfg.get(CONF_HOST)

    interval = int(cfg.get(CONF_POLL_INTERVAL, DEFAULT_POLL_INTERVAL))
    coord.update_interval = timedelta(seconds=max(10, interval))

    initial_groups = list(cfg.get(CONF_DEVICE_GROUPS, ["Default"])) or ["Default"]
    exit_device = bool(cfg.get("exit_device", False))

    root[entry.entry_id] = {
        "api": api,
        "coordinator": coord,
        "session": session,
        "options": {
            "participate_in_sync": bool(cfg.get(CONF_PARTICIPATE, True)),
            "sync_groups": initial_groups,
            "exit_device": exit_device,
        },
    }

    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
    await coord.async_config_entry_first_refresh()

    # ---------- Services ----------
    async def _ensure_local_face_for_user(user_id: str) -> str:
        """
        Ensure local path exists (we only manage the path; actual upload/capture handled elsewhere).
        Path: /config/www/akuvox_ac/FaceData/<USER>.jpg -> /local/akuvox_ac/FaceData/<USER>.jpg
        """
        www_root = os.path.join(hass.config.path(), "www", "akuvox_ac", "FaceData")
        os.makedirs(www_root, exist_ok=True)
        filename = f"{user_id}.jpg"
        rel = os.path.join("akuvox_ac", "FaceData", filename)
        return "/local/" + rel.replace("\\", "/")

    async def svc_add_user(call):
        d = call.data
        name: str = d["name"].strip()

        # collect in-use HA ids from all device snapshots
        in_use_ha_ids: List[str] = []
        for k, v in hass.data[DOMAIN].items():
            if k in (
                "groups_store",
                "users_store",
                "schedules_store",
                "settings_store",
                "sync_manager",
                "sync_queue",
                "_ui_registered",
            ):
                continue
            c: AkuvoxCoordinator = v.get("coordinator")
            if not c:
                continue
            for u in c.users or []:
                uid = str(u.get("UserID") or u.get("ID") or "")
                if _is_ha_id(uid):
                    in_use_ha_ids.append(uid)

        users_store: AkuvoxUsersStore = hass.data[DOMAIN]["users_store"]
        ha_id = await _allocate_lowest_ha_id(users_store, in_use_ha_ids)

        # Canonical FaceUrl that the device will fetch
        face_url = f"{FACE_BASE_URL}/{ha_id}.jpg"

        await users_store.upsert_profile(
            ha_id,
            name=name,
            groups=list(d.get("groups") or []),
            pin=str(d.get("pin")) if d.get("pin") else None,
            phone=str(d.get("phone")) if d.get("phone") else None,
            schedule_name=d.get("schedule_name") or "24/7 Access",
            key_holder=bool(d.get("key_holder", False)),
            access_level=d.get("access_level") or None,
            face_url=face_url,
            status="pending",
            # allow passing schedule_id explicitly, else resolver will map by name
            schedule_id=str(d.get("schedule_id")) if d.get("schedule_id") else None,
        )

        hass.data[DOMAIN]["sync_queue"].mark_change(None)

    async def svc_edit_user(call):
        d = call.data
        key = str(d["id"])
        users_store: AkuvoxUsersStore = hass.data[DOMAIN]["users_store"]

        new_face_url = d.get("face_url") if "face_url" in d else f"{FACE_BASE_URL}/{key}.jpg"

        await users_store.upsert_profile(
            key,
            name=d.get("name"),
            groups=list(d.get("groups") or []) if "groups" in d else None,
            pin=str(d.get("pin")) if "pin" in d else None,
            phone=str(d.get("phone")) if "phone" in d else None,
            schedule_name=d.get("schedule_name") if "schedule_name" in d else None,
            key_holder=bool(d.get("key_holder")) if "key_holder" in d else None,
            access_level=d.get("access_level") if "access_level" in d else None,
            face_url=new_face_url,
            status="pending",
            schedule_id=str(d.get("schedule_id")) if d.get("schedule_id") else None,
        )

        hass.data[DOMAIN]["sync_queue"].mark_change(None)

    async def svc_delete_user(call):
        key = str(call.data["id"])
        users_store: AkuvoxUsersStore = hass.data[DOMAIN]["users_store"]
        await users_store.delete(key)

        # immediate cascade: delete from every device using robust lookup
        for entry_id, coord, api, _ in hass.data[DOMAIN]["sync_manager"]._devices():
            try:
                id_records = await _lookup_device_user_ids_by_ha_key(api, key)
                if id_records:
                    for rec in id_records:
                        await _delete_user_every_way(api, rec)
                else:
                    try:
                        await api.user_delete(key)
                    except Exception:
                        pass
                try:
                    await coord.async_request_refresh()
                except Exception:
                    pass
            except Exception:
                pass

        # remove face file (check both common casings)
        for sub in ("FaceData", "facedata"):
            try:
                path = os.path.join(hass.config.path(), "www", "akuvox_ac", sub, f"{key}.jpg")
                if os.path.exists(path):
                    os.remove(path)
            except Exception:
                pass

        hass.data[DOMAIN]["sync_queue"].mark_change(None)

    async def svc_upload_face(call):
        """
        Legacy helper kept: simply records the canonical /local path.
        Actual file writing/placing happens outside this service.
        """
        d = call.data
        key = str(d["id"])
        face_url = await _ensure_local_face_for_user(key)
        users_store: AkuvoxUsersStore = hass.data[DOMAIN]["users_store"]
        await users_store.upsert_profile(key, face_url=face_url, status="pending")
        hass.data[DOMAIN]["sync_queue"].mark_change(None)

    async def svc_reboot_device(call):
        entry_id = call.data.get("entry_id")
        if entry_id:
            data = hass.data[DOMAIN].get(entry_id)
            if data and data.get("api"):
                try:
                    await data["api"].system_reboot()
                except Exception:
                    pass
        else:
            for entry_id2, coord2, api2, _ in hass.data[DOMAIN]["sync_manager"]._devices():
                try:
                    await api2.system_reboot()
                except Exception:
                    pass

    async def svc_sync_now(call):
        entry_id = call.data.get("entry_id")
        await hass.data[DOMAIN]["sync_queue"].sync_now(entry_id)

    async def svc_create_group(call):
        await hass.data[DOMAIN]["groups_store"].add_group(call.data["name"])

    async def svc_delete_groups(call):
        await hass.data[DOMAIN]["groups_store"].delete_groups(call.data.get("names") or [])

    async def svc_set_user_groups(call):
        key = str(call.data["key"])
        groups = list(call.data.get("groups") or [])
        await hass.data[DOMAIN]["users_store"].upsert_profile(key, groups=groups, status="pending")
        hass.data[DOMAIN]["sync_queue"].mark_change(None)

    async def svc_set_exit_device(call):
        entry_id = str(call.data["entry_id"])
        enabled = bool(call.data.get("enabled", True))
        if entry_id in hass.data[DOMAIN]:
            hass.data[DOMAIN][entry_id]["options"]["exit_device"] = enabled
            await hass.data[DOMAIN]["sync_queue"].sync_now(entry_id)

    async def svc_set_auto_reboot(call):
        time_hhmm = call.data.get("time")
        days = list(call.data.get("days") or [])
        hass.data[DOMAIN]["sync_manager"].set_auto_reboot(time_hhmm, days)

    async def svc_upsert_schedule(call):
        name = call.data["name"]
        spec = call.data["spec"]
        await hass.data[DOMAIN]["schedules_store"].upsert(name, spec)
        hass.data[DOMAIN]["sync_queue"].mark_change(None)

    async def svc_delete_schedule(call):
        name = call.data["name"]
        await hass.data[DOMAIN]["schedules_store"].delete(name)
        hass.data[DOMAIN]["sync_queue"].mark_change(None)

    hass.services.async_register(DOMAIN, "add_user", svc_add_user)
    hass.services.async_register(DOMAIN, "edit_user", svc_edit_user)
    hass.services.async_register(DOMAIN, "delete_user", svc_delete_user)
    hass.services.async_register(DOMAIN, "upload_face", svc_upload_face)
    hass.services.async_register(DOMAIN, "reboot_device", svc_reboot_device)
    hass.services.async_register(DOMAIN, "sync_now", svc_sync_now)
    hass.services.async_register(DOMAIN, "create_group", svc_create_group)
    hass.services.async_register(DOMAIN, "delete_groups", svc_delete_groups)
    hass.services.async_register(DOMAIN, "set_user_groups", svc_set_user_groups)
    hass.services.async_register(DOMAIN, "set_exit_device", svc_set_exit_device)
    hass.services.async_register(DOMAIN, "set_auto_reboot", svc_set_auto_reboot)
    hass.services.async_register(DOMAIN, "upsert_schedule", svc_upsert_schedule)
    hass.services.async_register(DOMAIN, "delete_schedule", svc_delete_schedule)

    if not hass.data[DOMAIN].get("_ui_registered"):
        register_ui(hass)
        hass.data[DOMAIN]["_ui_registered"] = True

    async def _options_updated(_hass: HomeAssistant, updated_entry: ConfigEntry):
        if updated_entry.entry_id != entry.entry_id:
            return
        new_cfg = {**updated_entry.data, **updated_entry.options}
        new_interval = int(new_cfg.get(CONF_POLL_INTERVAL, DEFAULT_POLL_INTERVAL))
        coord.update_interval = timedelta(seconds=max(10, new_interval))
        new_groups = list(new_cfg.get(CONF_DEVICE_GROUPS, ["Default"])) or ["Default"]
        hass.data[DOMAIN][entry.entry_id]["options"].update(
            {
                "participate_in_sync": bool(new_cfg.get(CONF_PARTICIPATE, True)),
                "sync_groups": new_groups,
                "exit_device": bool(new_cfg.get("exit_device", False)),
            }
        )

    entry.async_on_unload(entry.add_update_listener(_options_updated))
    return True


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry):
    unload_ok = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
    if unload_ok:
        root = hass.data.get(DOMAIN, {})
        root.pop(entry.entry_id, None)

        only_special = all(
            k
            in (
                "groups_store",
                "users_store",
                "schedules_store",
                "settings_store",
                "sync_manager",
                "sync_queue",
                "_ui_registered",
            )
            for k in root.keys()
        )
        if only_special:
            sq = root.get("sync_queue")
            if sq and sq._handle is not None:
                try:
                    sq._handle()
                except Exception:
                    pass
                root["sync_queue"]._handle = None  # type: ignore[attr-defined]
    return unload_ok


async def async_migrate_entry(hass, entry):
    current = entry.version
    target = ENTRY_VERSION
    if current == target:
        return True
    data = {**entry.data}
    options = {**entry.options}
    hass.config_entries.async_update_entry(entry, data=data, options=options, version=target)
    return True


class AkuvoxStorage(Store):
    def __init__(self, hass: HomeAssistant, entry_id: str):
        super().__init__(hass, 1, f"{DOMAIN}_state_{entry_id}.json")
        self.data: Dict[str, Any] = {"last_access": {}}

    async def async_load(self):
        x = await super().async_load()
        if x:
            self.data = x

    async def async_save(self):
        await super().async_save(self.data)

    def __getitem__(self, k):
        return self.data.get(k)

    def __setitem__(self, k, v):
        self.data[k] = v
