from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from homeassistant.core import HomeAssistant
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator
from homeassistant.helpers.event import async_call_later
from homeassistant.util import dt as dt_util

from .const import DOMAIN, EVENT_NON_KEY_ACCESS_GRANTED
from .api import AkuvoxAPI

_LOGGER = logging.getLogger(__name__)


def _safe_str(x) -> str:
    try:
        return str(x)
    except Exception:
        return ""


def _now_iso(hass: HomeAssistant) -> str:
    from homeassistant.util import dt as dt_util
    return dt_util.utcnow().isoformat() + "Z"


def _looks_like_ha_id(val: Any) -> bool:
    try:
        text = str(val)
    except Exception:
        return False
    text = text.strip()
    return len(text) == 5 and text.startswith("HA") and text[2:].isdigit()


class AkuvoxCoordinator(DataUpdateCoordinator):
    """Polls device, tracks health/events/users, and keeps a stable friendly name."""

    def __init__(self, hass: HomeAssistant, api: AkuvoxAPI, storage, entry_id: str, device_name: str):
        # NOTE: DataUpdateCoordinator.name is used by HA logs; keep it technical
        super().__init__(hass, _LOGGER, name=f"akuvox_ac:{entry_id}", update_interval=timedelta(seconds=30))

        self.api = api
        self.entry_id = entry_id
        self.storage = storage

        # Friendly display name (persist and surface in multiple places for UI)
        self.device_name: str = device_name or "Akuvox Device"
        self.friendly_name: str = self.device_name  # some UIs look for this

        self.health: Dict[str, Any] = {
            "name": self.device_name,  # always keep friendly name here for UI
            "device_type": "",         # set by __init__.py
            "ip": "",
            "online": False,
            "sync_status": "pending",
            "last_sync": None,
            "last_error": None,
            "last_ping": None,
        }
        self.users: List[Dict[str, Any]] = []
        self.events: List[Dict[str, Any]] = []  # newest first
        self.webhook_meta: Dict[str, Any] = {}
        self.webhook_states: Dict[str, Any] = {}
        self._was_online: Optional[bool] = None
        self._offline_job = None

    # Stable accessor other code can use
    @property
    def display_name(self) -> str:
        return self.device_name

    def set_display_name(self, name: str) -> None:
        """Update friendly name everywhere we surface it."""
        name = (name or "").strip() or "Akuvox Device"
        self.device_name = name
        self.friendly_name = name
        self.health["name"] = name

    def _append_event(self, text: str):
        evt = {"timestamp": _now_iso(self.hass), "Event": text}
        self.events.insert(0, evt)
        # keep a generous history to make UI feel “unlimited”
        self.events[:] = self.events[:1000]

    def _notification_targets(self, event_key: str) -> List[str]:
        try:
            root = self.hass.data.get(DOMAIN, {}) or {}
            store = root.get("notifications_store")
            if store:
                return store.targets_for(self.entry_id, event_key)
        except Exception:
            pass
        return []

    def _resolve_registry_profile(self, item: Dict[str, Any]) -> tuple[Optional[str], Optional[Dict[str, Any]]]:
        try:
            root = self.hass.data.get(DOMAIN, {}) or {}
            store = root.get("users_store")
            if not store:
                return None, None

            candidates: List[str] = []
            for key in ("UserID", "UserName", "Name", "Account", "CardNo"):
                val = item.get(key)
                if not val:
                    continue
                text = str(val).strip()
                if text:
                    candidates.append(text)

            for cand in candidates:
                if _looks_like_ha_id(cand):
                    profile = store.get(cand)
                    if profile:
                        return cand, profile

            user_name = self._event_user(item)
            if user_name:
                lookup = user_name.strip().lower()
                if lookup:
                    all_users = store.all()
                    for ha_id, profile in (all_users or {}).items():
                        try:
                            prof_name = str((profile or {}).get("name") or "").strip().lower()
                        except Exception:
                            prof_name = ""
                        if prof_name and prof_name == lookup:
                            return ha_id, profile
        except Exception:
            pass
        return None, None

    async def _dispatch_notification(
        self,
        event_key: str,
        title: str,
        message: str,
        data: Optional[Dict[str, Any]] = None,
    ) -> None:
        targets = self._notification_targets(event_key)
        if not targets:
            return
        payload: Dict[str, Any] = {"title": title, "message": message}
        if data:
            payload["data"] = data
        for target in targets:
            try:
                await self.hass.services.async_call(
                    "notify",
                    target,
                    payload,
                    blocking=False,
                )
            except Exception:
                _LOGGER.debug("Failed to send %s notification via %s", event_key, target)

    def _cancel_offline_notification(self) -> None:
        if self._offline_job:
            try:
                self._offline_job()
            except Exception:
                pass
            self._offline_job = None

    def _schedule_offline_notification(self) -> None:
        if self._offline_job:
            return

        def _cb(_now) -> None:
            self._offline_job = None
            if not self.health.get("online"):
                self.hass.async_create_task(self._notify_device_offline())

        # 5 minute grace period
        self._offline_job = async_call_later(self.hass, 5 * 60, _cb)

    async def _notify_device_offline(self) -> None:
        await self._dispatch_notification(
            "device_offline",
            f"{self.display_name}: Device offline",
            f"{self.display_name} has been offline for at least 5 minutes.",
            {"device_id": self.entry_id},
        )

    def _door_event_timestamp(self, item: Dict[str, Any]) -> Optional[datetime]:
        for key in ("Time", "time", "CreateTime", "Timestamp", "DateTime"):
            val = item.get(key)
            if not val:
                continue
            text = str(val).strip()
            if not text:
                continue
            try:
                parsed = dt_util.parse_datetime(text)
                if parsed:
                    return parsed
            except Exception:
                pass
            for fmt in ("%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M:%S"):
                try:
                    return datetime.strptime(text[:19], fmt)
                except Exception:
                    continue
        return None

    def _door_event_key(self, item: Dict[str, Any]) -> str:
        stamp = self._door_event_timestamp(item)
        pieces = [
            str(item.get("EventID") or item.get("ID") or item.get("Index") or ""),
            stamp.isoformat() if stamp else "",
            str(item.get("UserID") or item.get("UserName") or item.get("Name") or ""),
            str(item.get("Result") or item.get("Event") or item.get("Type") or ""),
        ]
        base = "|".join(p for p in pieces if p)
        if base:
            return base
        try:
            return str(sorted(item.items()))
        except Exception:
            return str(item)

    def _classify_door_event(self, item: Dict[str, Any]) -> Optional[str]:
        parts: List[str] = []
        for key in ("Result", "Event", "Type", "Reason", "Description", "Status"):
            val = item.get(key)
            if val:
                parts.append(str(val))
        combined = " ".join(parts).lower()
        if not combined:
            return None
        if "denied" in combined or "refuse" in combined or "invalid" in combined:
            if "time" in combined or "schedule" in combined or "zone" in combined:
                return "denied_outside_time"
            if "no access" in combined or "no auth" in combined or "authority" in combined or "auth" in combined:
                return "denied_no_access"
            return "denied_no_access"
        if "grant" in combined or "pass" in combined or "allow" in combined or "success" in combined or "opened" in combined:
            return "granted"
        return None

    def _event_user(self, item: Dict[str, Any]) -> str:
        for key in ("UserName", "User", "Name", "Person"):
            val = item.get(key)
            if val:
                return str(val)
        uid = item.get("UserID") or item.get("CardNo") or item.get("Account")
        return str(uid) if uid else "Unknown user"

    def _format_event_time(self, stamp: Optional[datetime]) -> str:
        if not stamp:
            return ""
        try:
            local_dt = dt_util.as_local(stamp)
            return local_dt.strftime("%Y-%m-%d %H:%M")
        except Exception:
            return stamp.isoformat()

    def _event_sort_key(self, item: Dict[str, Any]) -> str:
        stamp = self._door_event_timestamp(item)
        return stamp.isoformat() if stamp else ""

    async def _store_seen_event_keys(self, keys: List[str]) -> None:
        if not keys:
            return
        try:
            seen = self.storage.data.setdefault("doorlog_seen", [])
            if not isinstance(seen, list):
                seen = []
                self.storage.data["doorlog_seen"] = seen
            seen.extend(keys)
            del seen[:-100]
            await self.storage.async_save()
        except Exception:
            pass

    def _get_seen_event_keys(self) -> List[str]:
        try:
            seen = self.storage.data.setdefault("doorlog_seen", [])
            if isinstance(seen, list):
                return list(seen)
        except Exception:
            pass
        return []

    async def _handle_door_event(self, item: Dict[str, Any]) -> None:
        category = self._classify_door_event(item)
        if not category:
            return
        user = self._event_user(item)
        door = str(
            item.get("DoorName")
            or item.get("Door")
            or item.get("Device")
            or self.display_name
        )
        method = item.get("Method") or item.get("PassMode") or item.get("Mode") or ""
        method_text = f" using {method}" if method else ""
        stamp = self._door_event_timestamp(item)
        when = self._format_event_time(stamp)
        suffix = f" — {when}" if when else ""
        if category == "granted":
            title = f"{self.display_name}: Access granted"
            message = f"{user} granted access at {door}{method_text}{suffix}"
        elif category == "denied_outside_time":
            title = f"{self.display_name}: Access denied"
            message = f"{user} denied at {door} (outside schedule){suffix}"
        else:
            title = f"{self.display_name}: Access denied"
            message = f"{user} denied at {door} (no access){suffix}"
        await self._dispatch_notification(
            category,
            title,
            message,
            {"device_id": self.entry_id, "event": item},
        )

        if category == "granted":
            ha_key, profile = self._resolve_registry_profile(item)
            if profile is not None and not bool(profile.get("key_holder")):
                event_data: Dict[str, Any] = {
                    "device_id": self.entry_id,
                    "device_name": self.display_name,
                    "user_id": ha_key,
                    "user_name": user,
                    "timestamp": stamp.isoformat() if stamp else None,
                    "method": str(method) if method else None,
                    "raw_event": item,
                    "key_holder": bool(profile.get("key_holder")),
                }
                if isinstance(profile, dict):
                    if profile.get("schedule_name"):
                        event_data["schedule_name"] = profile.get("schedule_name")
                    if profile.get("groups"):
                        try:
                            event_data["groups"] = list(profile.get("groups") or [])
                        except Exception:
                            pass
                self.hass.bus.async_fire(EVENT_NON_KEY_ACCESS_GRANTED, event_data)

    async def _process_door_events(self) -> None:
        if not any(
            self._notification_targets(key)
            for key in ("denied_no_access", "denied_outside_time", "granted")
        ):
            return
        try:
            items = await self.api.events_last()
        except Exception:
            return
        if not isinstance(items, list):
            return
        seen = set(self._get_seen_event_keys())
        new_items: List[Dict[str, Any]] = []
        new_keys: List[str] = []
        for raw in items:
            if not isinstance(raw, dict):
                continue
            key = self._door_event_key(raw)
            if not key or key in seen:
                continue
            new_items.append(raw)
            new_keys.append(key)
            seen.add(key)
        if not new_items:
            return
        new_items.sort(key=self._event_sort_key)
        await self._store_seen_event_keys(new_keys)
        for item in new_items:
            await self._handle_door_event(item)

    async def _kick_sync_now(self):
        """Ask the SyncQueue to sync this device immediately."""
        try:
            root = self.hass.data.get(DOMAIN, {}) or {}
            sq = root.get("sync_queue")
            if sq:
                # Show 'pending' right away so UI reflects the action
                self.health["sync_status"] = "pending"
                await sq.sync_now(self.entry_id)
        except Exception:
            # best-effort only
            pass

    async def _async_update_data(self):
        """HA calls this: refresh health/users/events."""
        # make sure our friendly name can't be lost if something rewrites health elsewhere
        if self.health.get("name") != self.device_name:
            self.health["name"] = self.device_name

        last_error = None
        last_ping = None
        try:
            info = await self.api.ping_info()
            last_ping = info
            is_up = bool(info.get("ok"))
            prev = self._was_online
            self.health["online"] = is_up
            self._was_online = is_up

            # Online/offline transition events (+ on-online sync)
            if is_up:
                self._cancel_offline_notification()
                if prev is False:
                    self._append_event("Device came online")
                    await self._kick_sync_now()
                elif prev is None and not self.health.get("last_sync"):
                    # First time we see it online after startup and never synced
                    await self._kick_sync_now()
            else:
                if prev is True or prev is None:
                    self._append_event("Device went offline")
                self._schedule_offline_notification()
                self.health["last_error"] = None
                self.health["last_ping"] = last_ping
                return

            # Load users so integrity checker & UI can see them
            try:
                users = await self.api.user_list()
                if isinstance(users, list):
                    self.users = users
            except Exception:
                # don't fail the whole refresh just because the user list failed
                pass

            await self._process_door_events()

        except Exception as e:
            last_error = _safe_str(e)
        finally:
            self.health["last_error"] = last_error
            self.health["last_ping"] = last_ping