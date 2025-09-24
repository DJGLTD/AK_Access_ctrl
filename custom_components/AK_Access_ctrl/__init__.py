from __future__ import annotations

import asyncio
from datetime import date, datetime, timedelta
import time
from pathlib import Path
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
    DEFAULT_DIAGNOSTICS_HISTORY_LIMIT,
    MIN_DIAGNOSTICS_HISTORY_LIMIT,
    MAX_DIAGNOSTICS_HISTORY_LIMIT,
    CONF_PARTICIPATE,
    CONF_POLL_INTERVAL,
    CONF_DEVICE_GROUPS,
    CONF_RELAY_ROLES,
    ENTRY_VERSION,
    ADMIN_DASHBOARD_ICON,
    ADMIN_DASHBOARD_TITLE,
    ADMIN_DASHBOARD_URL_PATH,
)

from .relay import (
    normalize_roles as normalize_relay_roles,
    relay_suffix_for_user,
    door_relays,
)

from .api import AkuvoxAPI
from .coordinator import AkuvoxCoordinator
from .http import (
    face_base_url,
    face_filename_from_reference,
    face_storage_dir,
    register_ui,
    FACE_FILE_EXTENSIONS,
)  # provides /api/akuvox_ac/ui/* + /api/AK_AC/* assets
from .ha_id import ha_id_from_int, is_ha_id, normalize_ha_id

HA_EVENT_ACCCESS = "akuvox_access_event"  # fired for access denied / exit override


def _register_admin_dashboard(hass: HomeAssistant) -> bool:
    """Register the Akuvox admin dashboard panel."""

    try:
        from homeassistant.components import frontend
    except ImportError:
        return False

    panel_config = {"url": "/akuvox-ac/"}

    try:
        frontend.async_register_built_in_panel(
            hass,
            "iframe",
            ADMIN_DASHBOARD_TITLE,
            ADMIN_DASHBOARD_ICON,
            frontend_url_path=ADMIN_DASHBOARD_URL_PATH,
            config=panel_config,
            require_admin=True,
            update=True,
        )
    except Exception:
        return False

    return True


def _remove_admin_dashboard(hass: HomeAssistant) -> None:
    """Remove the Akuvox admin dashboard panel if registered."""

    try:
        from homeassistant.components import frontend
    except ImportError:
        return

    try:
        frontend.async_remove_panel(hass, ADMIN_DASHBOARD_URL_PATH)
    except Exception:
        return


# ---------------------- Helpers ---------------------- #
def _now_hh_mm() -> str:
    try:
        return datetime.now().strftime("%H:%M")
    except Exception:
        return ""


def _context_user_name(hass: HomeAssistant, context) -> str:
    """Best-effort friendly name for the actor behind a service/http call."""

    default = "HA User"
    if context is None:
        return default

    user_id = getattr(context, "user_id", None)
    if not user_id:
        return default

    try:
        user = hass.auth.async_get_user(user_id)
        if user and user.name:
            return user.name
        if user:
            return user.id
    except Exception:
        return default

    return default


def _key_of_user(u: Dict[str, Any]) -> str:
    return str(u.get("UserID") or u.get("ID") or u.get("Name") or "")


def _normalize_access_date(value: Any) -> Optional[str]:
    """Normalize user access dates to ISO format or clear them."""

    if value is None:
        return None

    if isinstance(value, date):
        return value.isoformat()

    if isinstance(value, datetime):
        return value.date().isoformat()

    if isinstance(value, str):
        text = value.strip()
        if not text:
            return ""
        try:
            normalized = datetime.strptime(text.split("T", 1)[0], "%Y-%m-%d")
        except ValueError:
            return ""
        return normalized.date().isoformat()

    return ""


def _parse_access_date(value: Any) -> Optional[date]:
    """Parse a stored access date into a ``date`` object."""

    if value is None:
        return None

    if isinstance(value, date):
        return value

    if isinstance(value, datetime):
        return value.date()

    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            base = text.split("T", 1)[0]
            parsed = datetime.strptime(base, "%Y-%m-%d")
        except ValueError:
            return None
        return parsed.date()

    return None


_BOOLISH_TRUE = {
    "1",
    "true",
    "t",
    "yes",
    "y",
    "on",
    "enable",
    "enabled",
    "active",
    "present",
    "available",
    "linked",
}

_BOOLISH_FALSE = {
    "0",
    "false",
    "f",
    "no",
    "n",
    "off",
    "disable",
    "disabled",
    "inactive",
    "absent",
    "missing",
    "unlinked",
}

_FACE_FLAG_KEYS = (
    "face_active",
    "faceActive",
    "FaceActive",
    "face",
    "Face",
    "face_status",
    "FaceStatus",
    "faceEnabled",
    "FaceEnabled",
    "face_enable",
    "FaceEnable",
    "faceRecognition",
    "FaceRecognition",
    "has_face",
    "hasFace",
    "HasFace",
)


def _normalize_boolish(value: Any) -> Optional[bool]:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        lower = value.strip().lower()
        if not lower:
            return None
        if lower in _BOOLISH_TRUE:
            return True
        if lower in _BOOLISH_FALSE:
            return False
    return None


def _face_flag_from_record(record: Dict[str, Any]) -> Optional[bool]:
    if not isinstance(record, dict):
        return None
    for key in _FACE_FLAG_KEYS:
        if key in record:
            flag = _normalize_boolish(record.get(key))
            if flag is not None:
                return flag
    return None


def _key_holder_from_record(record: Dict[str, Any]) -> Optional[bool]:
    if not isinstance(record, dict):
        return None
    if "key_holder" in record:
        flag = _normalize_boolish(record.get("key_holder"))
        if flag is not None:
            return flag
    if "KeyHolder" in record:
        flag = _normalize_boolish(record.get("KeyHolder"))
        if flag is not None:
            return flag
    return None


def _face_asset_exists(hass: HomeAssistant, user_id: str) -> bool:
    try:
        search_paths: List[Path] = []
        try:
            search_paths.append(face_storage_dir(hass))
        except Exception:
            pass

        search_paths.append(Path(__file__).parent / "www" / "FaceData")

        try:
            search_paths.append(
                Path(hass.config.path("www")) / "AK_Access_ctrl" / "FaceData"
            )
        except Exception:
            pass

        for base in search_paths:
            try:
                for ext in FACE_FILE_EXTENSIONS:
                    candidate = (base / f"{user_id}.{ext}").resolve()
                    candidate.relative_to(base.resolve())
                    if candidate.exists():
                        return True
            except Exception:
                continue
    except Exception:
        return False

    return False


def _migrate_face_storage(hass: HomeAssistant) -> None:
    """Copy face assets from legacy locations into the persistent store."""

    try:
        dest_dir = face_storage_dir(hass)
    except Exception:
        return

    try:
        dest_dir.mkdir(parents=True, exist_ok=True)
    except Exception:
        return

    sources: List[Path] = [Path(__file__).parent / "www" / "FaceData"]

    try:
        sources.append(Path(hass.config.path("www")) / "AK_Access_ctrl" / "FaceData")
    except Exception:
        pass

    for source in sources:
        try:
            resolved_source = source.resolve()
        except Exception:
            continue

        if not resolved_source.exists() or not resolved_source.is_dir():
            continue

        try:
            entries = list(resolved_source.iterdir())
        except Exception:
            continue

        for candidate in entries:
            if not candidate.is_file():
                continue

            suffix = candidate.suffix.lower().lstrip(".")
            if suffix not in FACE_FILE_EXTENSIONS:
                continue

            dest = dest_dir / candidate.name

            try:
                dest.relative_to(dest_dir)
            except Exception:
                continue

            if dest.exists():
                continue

            try:
                dest.write_bytes(candidate.read_bytes())
            except Exception:
                continue


def _desired_device_user_payload(
    hass: HomeAssistant,
    ha_key: str,
    profile: Optional[Dict[str, Any]],
    local: Optional[Dict[str, Any]],
    *,
    opts: Dict[str, Any],
    sched_map: Optional[Dict[str, str]],
    exit_allow_map: Optional[Dict[str, bool]],
    face_root_base: str,
    device_type_raw: str,
) -> Dict[str, Any]:
    """Build the canonical payload a device record should expose for a registry profile."""

    profile = profile or {}
    local = local or {}
    sched_map = sched_map or {}
    exit_allow_map = exit_allow_map or {}

    schedule_name = (profile.get("schedule_name") or local.get("Schedule") or "24/7 Access").strip()
    schedule_lower = schedule_name.lower()
    key_holder_flag = _key_holder_from_record(profile)
    if key_holder_flag is None:
        key_holder_flag = _key_holder_from_record(local)
    key_holder = bool(key_holder_flag)
    explicit_id = str(profile.get("schedule_id") or "").strip()

    schedule_exit_enabled = bool(exit_allow_map.get(schedule_lower, False))
    if not schedule_exit_enabled and explicit_id == "1001":
        schedule_exit_enabled = True

    exit_override = bool(opts.get("exit_device")) and bool(schedule_exit_enabled)
    effective_schedule = "24/7 Access" if exit_override else schedule_name

    if exit_override:
        schedule_id = "1001"
    elif explicit_id and explicit_id.isdigit():
        schedule_id = explicit_id
    else:
        schedule_id = sched_map.get(effective_schedule.lower(), "1001")

    relay_roles = normalize_relay_roles(opts.get("relay_roles"), device_type_raw)
    try:
        opts["relay_roles"] = relay_roles
    except Exception:
        pass

    door_digits = door_relays(relay_roles)
    relay_suffix = relay_suffix_for_user(relay_roles, key_holder, device_type_raw)
    schedule_relay = f"{schedule_id}-{relay_suffix};"

    device_type = str(device_type_raw or "").strip().lower()
    is_keypad = device_type == "keypad"

    def _string_or_default(*values: Any, default: str) -> str:
        for value in values:
            if value is None:
                continue
            text = str(value).strip()
            if text or text == "0":
                return text
        return default

    def _first_group(*sources: Any) -> str:
        for source in sources:
            if isinstance(source, (list, tuple)):
                for entry in source:
                    text = _string_or_default(entry, default="")
                    if text:
                        return text
            else:
                text = _string_or_default(source, default="")
                if text:
                    return text
        return "Default"

    def _schedule_list_from_local() -> List[str]:
        schedule_list: List[str] = []
        local_schedule = local.get("Schedule")
        if isinstance(local_schedule, (list, tuple, set)):
            for entry in local_schedule:
                text = _string_or_default(entry, default="")
                if text:
                    schedule_list.append(text)
        elif local_schedule not in (None, ""):
            text = _string_or_default(local_schedule, default="")
            if text:
                schedule_list.append(text)
        local_schedule_id = _string_or_default(local.get("ScheduleID"), default="")
        if local_schedule_id:
            if not schedule_list:
                schedule_list.append(local_schedule_id)
            elif local_schedule_id not in schedule_list:
                schedule_list.insert(0, local_schedule_id)
        return schedule_list

    def _normalise_license_plate() -> List[Dict[str, Any]]:
        source = profile.get("license_plate")
        if not isinstance(source, (list, tuple)):
            source = profile.get("LicensePlate")
        if not isinstance(source, (list, tuple)):
            source = local.get("LicensePlate")
        result: List[Dict[str, Any]] = []
        if isinstance(source, (list, tuple)):
            for entry in source:
                result.append(entry if isinstance(entry, dict) else {})
        while len(result) < 5:
            result.append({})
        if len(result) > 5:
            result = result[:5]
        return result

    name_value = profile.get("name")
    if name_value in (None, ""):
        name_value = local.get("Name") or ha_key
    name = str(name_value)

    group_value = _first_group(
        profile.get("groups"),
        local.get("Groups"),
        local.get("Group"),
    )

    door_num = _string_or_default(
        profile.get("door_num"),
        profile.get("DoorNum"),
        local.get("DoorNum"),
        door_digits[0] if door_digits else None,
        default="1",
    )

    lift_floor = _string_or_default(
        profile.get("lift_floor_num"),
        profile.get("lift_floor"),
        local.get("LiftFloorNum"),
        default="0",
    )

    schedule_list = _schedule_list_from_local()
    if not schedule_list:
        if schedule_id:
            schedule_list = [schedule_id]
        else:
            schedule_list = ["1001"]

    web_relay = _string_or_default(profile.get("web_relay"), local.get("WebRelay"), default="0")
    priority_call = _string_or_default(profile.get("priority_call"), local.get("PriorityCall"), default="0")
    dial_account = _string_or_default(profile.get("dial_account"), local.get("DialAccount"), default="0")
    c4_event = _string_or_default(profile.get("c4_event_no"), local.get("C4EventNo"), default="0")
    auth_mode = _string_or_default(profile.get("auth_mode"), local.get("AuthMode"), default="0")
    card_code = _string_or_default(profile.get("card_code"), profile.get("CardCode"), local.get("CardCode"), default="")
    ble_auth = _string_or_default(profile.get("ble_auth_code"), profile.get("BLEAuthCode"), local.get("BLEAuthCode"), default="")

    desired: Dict[str, Any] = {
        "UserID": ha_key,
        "Name": name,
        "DoorNum": door_num,
        "LiftFloorNum": lift_floor,
        "Schedule": schedule_list,
        "ScheduleRelay": schedule_relay,
        "WebRelay": web_relay,
        "Group": group_value,
        "PriorityCall": priority_call,
        "DialAccount": dial_account,
        "C4EventNo": c4_event,
        "AuthMode": auth_mode,
        "LicensePlate": _normalise_license_plate(),
        "CardCode": card_code,
        "BLEAuthCode": ble_auth,
    }

    pin_value = profile.get("pin")
    if pin_value in (None, ""):
        pin_value = local.get("PrivatePIN") or local.get("Pin")
    if pin_value not in (None, ""):
        desired["PrivatePIN"] = str(pin_value)

    if not is_keypad:
        phone_value = profile.get("phone")
        if phone_value in (None, ""):
            phone_value = local.get("PhoneNum") or local.get("Phone")
        if phone_value not in (None, ""):
            desired["PhoneNum"] = str(phone_value)

        face_url = profile.get("face_url") or local.get("FaceUrl") or local.get("FaceURL")
        if face_url in (None, ""):
            face_url = f"{face_root_base}/{ha_key}.jpg"
        if face_url not in (None, ""):
            face_url_str = str(face_url)
            filename_source = _string_or_default(local.get("FaceFileName"), default="")
            if not filename_source:
                filename_source = face_url_str
            face_filename = face_filename_from_reference(filename_source, ha_key)
            desired["FaceUrl"] = face_filename
            desired["FaceFileName"] = face_filename

    return desired


def _integrity_field_differences(local: Dict[str, Any], expected: Dict[str, Any]) -> List[str]:
    """Return the list of high-level fields that differ between device data and expectations."""

    diffs: List[str] = []

    def _norm(val: Any) -> str:
        return str(val or "").strip()

    if _norm(local.get("Name")) != _norm(expected.get("Name")):
        diffs.append("name")

    expected_id = _norm(expected.get("ID"))
    if expected_id:
        if _norm(local.get("ID")) != expected_id:
            diffs.append("device id")

    if _norm(local.get("UserID")) != _norm(expected.get("UserID")):
        diffs.append("user id")

    if _norm(local.get("PrivatePIN") or local.get("Pin") or local.get("PIN")) != _norm(expected.get("PrivatePIN")):
        diffs.append("pin")

    expected_face = _face_flag_from_record(expected)
    actual_face = _face_flag_from_record(local)
    if expected_face is not None:
        if bool(actual_face) != bool(expected_face):
            diffs.append("face status")

    expected_url = _norm(expected.get("FaceUrl") or expected.get("FaceURL"))
    if expected_url:
        if _norm(local.get("FaceUrl") or local.get("FaceURL")) != expected_url:
            diffs.append("face url")

    return diffs


def _ha_id_from_int(n: int) -> str:
    return ha_id_from_int(n)


def _is_ha_id(s: str) -> bool:
    return is_ha_id(s)


def _mark_coordinator_rebooting(coord: AkuvoxCoordinator, *, triggered_by: str, duration: float = 300.0) -> None:
    """Flag coordinator as rebooting for UI purposes and log the event."""

    try:
        coord.health["status"] = "rebooting"
        coord.health["online"] = False
        coord.health["rebooting_until"] = time.time() + duration
        coord.health["last_error"] = None
    except Exception:
        pass

    try:
        coord._append_event(f"Device Rebooted by - {triggered_by}")  # type: ignore[attr-defined]
    except Exception:
        pass


def _log_full_sync(coord: AkuvoxCoordinator, triggered_by: str) -> None:
    try:
        coord._append_event(f"Full Sync Triggered by - {triggered_by}")  # type: ignore[attr-defined]
    except Exception:
        pass


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
    """Named access schedules stored centrally, synced to devices during reconcile."""

    _DAYS = ("mon", "tue", "wed", "thu", "fri", "sat", "sun")

    _API_DAY_KEYS = {
        "mon": "Mon",
        "tue": "Tue",
        "wed": "Wed",
        "thu": "Thur",
        "fri": "Fri",
        "sat": "Sat",
        "sun": "Sun",
    }

    def __init__(self, hass: HomeAssistant):
        super().__init__(hass, 1, f"{DOMAIN}_schedules.json")
        self.data: Dict[str, Any] = {"schedules": {}}

    @staticmethod
    def _as_bool(val: Any) -> bool:
        if isinstance(val, str):
            return val.strip().lower() in {"1", "true", "yes", "y", "on", "enable", "enabled"}
        return bool(val)

    @staticmethod
    def _time_to_minutes(value: Any) -> Optional[int]:
        if value is None:
            return None
        if isinstance(value, (int, float)):
            minutes = int(value)
            if minutes < 0:
                minutes = 0
            if minutes > 23 * 60 + 59:
                minutes = 23 * 60 + 59
            return minutes
        text = str(value).strip()
        if not text:
            return None
        if text.isdigit() and len(text) in (3, 4):
            if len(text) == 3:
                text = f"0{text}"
            try:
                hours = int(text[:2])
                minutes = int(text[2:])
            except ValueError:
                return None
        else:
            parts = text.split(":")
            if len(parts) < 2:
                return None
            try:
                hours = int(parts[0])
                minutes = int(parts[1])
            except ValueError:
                return None
        if hours < 0:
            hours = 0
        if minutes < 0:
            minutes = 0
        if hours > 23:
            hours = 23
        if minutes > 59:
            minutes = 59
        return hours * 60 + minutes

    @classmethod
    def _clean_time(cls, value: Any, *, default: str) -> str:
        minutes = cls._time_to_minutes(value)
        if minutes is None:
            minutes = cls._time_to_minutes(default)
        if minutes is None:
            minutes = 0
        hours = minutes // 60
        mins = minutes % 60
        return f"{hours:02d}:{mins:02d}"

    def _default_exit_flag(self, name: str) -> bool:
        low = (name or "").strip().lower()
        return low in {"24/7 access", "24/7", "24x7", "always"}

    def _normalize_payload(self, name: str, payload: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        normalized: Dict[str, Any] = {
            "start": "00:00",
            "end": "23:59",
            "days": list(self._DAYS),
            "always_permit_exit": self._default_exit_flag(name),
            "type": "2",
            "date_start": "",
            "date_end": "",
        }

        days_selected: set[str] = set()
        found_start: Optional[int] = None
        found_end: Optional[int] = None

        if isinstance(payload, dict):
            raw_start = (
                payload.get("start")
                or payload.get("Start")
                or payload.get("time_start")
                or payload.get("TimeStart")
            )
            raw_end = (
                payload.get("end")
                or payload.get("End")
                or payload.get("time_end")
                or payload.get("TimeEnd")
            )

            if raw_start is not None:
                normalized["start"] = self._clean_time(raw_start, default=normalized["start"])
            if raw_end is not None:
                normalized["end"] = self._clean_time(raw_end, default=normalized["end"])

            if "type" in payload or "Type" in payload:
                normalized["type"] = str(payload.get("type") or payload.get("Type") or "2")

            if "date_start" in payload or "DateStart" in payload:
                normalized["date_start"] = str(payload.get("date_start") or payload.get("DateStart") or "").strip()
            if "date_end" in payload or "DateEnd" in payload:
                normalized["date_end"] = str(payload.get("date_end") or payload.get("DateEnd") or "").strip()

            raw_days = payload.get("days")
            if isinstance(raw_days, (list, tuple, set)):
                for entry in raw_days:
                    key = str(entry or "").strip().lower()
                    if key in self._DAYS:
                        days_selected.add(key)
                    else:
                        short = key[:3]
                        if short in {"mon", "tue", "wed", "thu", "fri", "sat", "sun"}:
                            days_selected.add(short)
            elif isinstance(raw_days, dict):
                for key, value in raw_days.items():
                    normalized_key = str(key or "").strip().lower()
                    if normalized_key in self._DAYS and self._as_bool(value):
                        days_selected.add(normalized_key)

            for day in self._DAYS:
                api_key = self._API_DAY_KEYS[day]
                if api_key in payload:
                    if self._as_bool(payload.get(api_key)):
                        days_selected.add(day)
                elif day in payload:
                    spans = payload.get(day)
                    if isinstance(spans, (list, tuple)):
                        for span in spans:
                            if not isinstance(span, (list, tuple)) or len(span) < 2:
                                continue
                            start = self._time_to_minutes(span[0])
                            end = self._time_to_minutes(span[1])
                            if start is None or end is None:
                                continue
                            days_selected.add(day)
                            found_start = start if found_start is None else min(found_start, start)
                            found_end = end if found_end is None else max(found_end, end)

            if found_start is not None:
                normalized["start"] = self._clean_time(found_start, default=normalized["start"])
            if found_end is not None:
                normalized["end"] = self._clean_time(found_end, default=normalized["end"])

            if "always_permit_exit" in payload:
                normalized["always_permit_exit"] = self._as_bool(payload.get("always_permit_exit"))

        if not days_selected:
            # Default built-ins preserve their expected behaviour; custom schedules default to weekdays
            if name.strip().lower() == "no access":
                days_selected = set()
                normalized["start"] = "00:00"
                normalized["end"] = "00:00"
            elif name.strip().lower() in {"24/7 access", "24/7", "24x7", "always"}:
                days_selected = set(self._DAYS)
                normalized["start"] = "00:00"
                normalized["end"] = "23:59"
            else:
                days_selected = {"mon", "tue", "wed", "thu", "fri"}

        normalized["days"] = [day for day in self._DAYS if day in days_selected]

        return normalized

    async def async_load(self):
        x = await super().async_load()
        if x and isinstance(x.get("schedules"), dict):
            self.data = x

        original = self.data.get("schedules") or {}
        existing: Dict[str, Any] = {}
        for name, spec in original.items():
            existing[name] = self._normalize_payload(name, spec if isinstance(spec, dict) else {})

        if "24/7 Access" not in existing:
            existing["24/7 Access"] = self._normalize_payload(
                "24/7 Access",
                {
                    "start": "00:00",
                    "end": "23:59",
                    "days": list(self._DAYS),
                    "always_permit_exit": True,
                    "type": "2",
                },
            )
        if "No Access" not in existing:
            existing["No Access"] = self._normalize_payload(
                "No Access",
                {
                    "start": "00:00",
                    "end": "00:00",
                    "days": [],
                    "always_permit_exit": False,
                    "type": "2",
                },
            )

        changed = original != existing
        self.data["schedules"] = existing
        if changed:
            await self.async_save()

    async def async_save(self):
        await super().async_save(self.data)

    def all(self) -> Dict[str, Any]:
        return dict(self.data.get("schedules") or {})

    async def upsert(self, name: str, payload: Dict[str, Any]):
        self.data.setdefault("schedules", {})[name] = self._normalize_payload(name, payload)
        await self.async_save()

    async def delete(self, name: str):
        if name in ("24/7 Access", "No Access"):
            return
        self.data.setdefault("schedules", {}).pop(name, None)
        await self.async_save()


class AkuvoxUsersStore(Store):
    """Persistent store for HA-managed users and their schedule/key-holder metadata."""
    def __init__(self, hass: HomeAssistant):
        super().__init__(hass, 1, USERS_STORAGE_KEY)
        self.data: Dict[str, Any] = {"users": {}}

    async def async_load(self):
        existing = await super().async_load()
        if existing and isinstance(existing.get("users"), dict):
            self.data = existing
        changed = self._normalize_user_ids()
        if changed:
            await self.async_save()

    async def async_save(self):
        await super().async_save(self.data)

    def _normalize_user_ids(self) -> bool:
        users = self.data.setdefault("users", {})
        changed = False
        for key in list(users.keys()):
            canonical = normalize_ha_id(key)
            if not canonical or canonical == key:
                continue
            entry = users.pop(key)
            changed = True
            existing = users.get(canonical)
            if isinstance(existing, dict) and isinstance(entry, dict):
                merged = dict(existing)
                for field, value in entry.items():
                    if field not in merged or merged[field] in (None, "", [], {}):
                        merged[field] = value
                users[canonical] = merged
            else:
                users[canonical] = entry if entry is not None else existing
        return changed

    def get(self, key: str, default=None):
        users = self.data.get("users") or {}
        canonical = normalize_ha_id(key)
        if canonical and canonical in users:
            return users.get(canonical, default)
        return users.get(key, default)

    def all(self) -> Dict[str, Any]:
        return dict(self.data.get("users") or {})

    def all_ha_ids(self) -> List[str]:
        seen: Dict[str, None] = {}
        for key in (self.data.get("users") or {}).keys():
            canonical = normalize_ha_id(key)
            if canonical:
                seen.setdefault(canonical, None)
        return list(seen.keys())

    def next_free_ha_id(self, *, blocked: Optional[List[str]] = None) -> str:
        used: set[str] = set(self.all_ha_ids())
        if blocked:
            for candidate in blocked:
                canonical = normalize_ha_id(candidate)
                if canonical:
                    used.add(canonical)

        n = 1
        while True:
            candidate = _ha_id_from_int(n)
            if candidate not in used:
                return candidate
            n += 1

    def reserve_id(self, ha_id: str):
        canonical = normalize_ha_id(ha_id)
        if not canonical:
            raise ValueError(f"Invalid HA id: {ha_id}")
        self.data["users"].setdefault(canonical, {})

    async def upsert_profile(
        self,
        key: str,
        *,
        name: Optional[str] = None,
        groups: Optional[List[str]] = None,
        pin: Optional[str] = None,
        face_url: Optional[str] = None,
        face_status: Optional[str] = None,
        face_synced_at: Optional[str] = None,
        phone: Optional[str] = None,
        status: Optional[str] = None,
        schedule_name: Optional[str] = None,
        key_holder: Optional[bool] = None,
        access_level: Optional[str] = None,
        schedule_id: Optional[str] = None,  # allow explicit schedule ID (1001/1002/1003/…)
        access_start: Optional[str] = None,
        access_end: Optional[str] = None,
    ):
        canonical = normalize_ha_id(key) or str(key)
        u = self.data["users"].setdefault(canonical, {})
        if name is not None:
            u["name"] = name
        if groups is not None:
            u["groups"] = list(groups)
        if pin is not None:
            u["pin"] = str(pin) if pin not in (None, "") else ""
        if face_url is not None:
            u["face_url"] = face_url
        if face_status is not None:
            normalized = str(face_status).strip().lower()
            if normalized:
                u["face_status"] = normalized
            else:
                u.pop("face_status", None)
        if face_synced_at is not None:
            if face_synced_at:
                u["face_synced_at"] = str(face_synced_at)
            else:
                u.pop("face_synced_at", None)
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
        if access_start is not None:
            normalized_start = _normalize_access_date(access_start)
            if normalized_start:
                u["access_start"] = normalized_start
            else:
                u.pop("access_start", None)
        if access_end is not None:
            normalized_end = _normalize_access_date(access_end)
            if normalized_end:
                u["access_end"] = normalized_end
            else:
                u.pop("access_end", None)
        await self.async_save()

    async def delete(self, key: str):
        users = self.data.get("users", {})
        canonical = normalize_ha_id(key)
        if canonical:
            users.pop(canonical, None)
        users.pop(key, None)
        await self.async_save()


class AkuvoxSettingsStore(Store):
    DEFAULT_INTEGRITY_MINUTES = 15

    def __init__(self, hass: HomeAssistant):
        super().__init__(hass, 1, f"{DOMAIN}_settings.json")
        self.data: Dict[str, Any] = {
            "auto_sync_time": None,
            "auto_reboot": {"time": None, "days": []},
            "integrity_interval_minutes": self.DEFAULT_INTEGRITY_MINUTES,
            "auto_sync_delay_minutes": 30,
            "alerts": {"targets": {}},
            "diagnostics_history_limit": DEFAULT_DIAGNOSTICS_HISTORY_LIMIT,
        }

    async def async_load(self):
        x = await super().async_load()
        if x:
            base = dict(self.data)
            base.update(x)
            self.data = base

        if not isinstance(self.data.get("auto_reboot"), dict):
            self.data["auto_reboot"] = {"time": None, "days": []}

        delay = self.data.get("auto_sync_delay_minutes", 30)
        try:
            delay = int(delay)
        except Exception:
            delay = 30
        delay = max(5, min(60, delay))
        self.data["auto_sync_delay_minutes"] = delay

        integ = self.data.get("integrity_interval_minutes", self.DEFAULT_INTEGRITY_MINUTES)
        try:
            integ = int(integ)
        except Exception:
            integ = self.DEFAULT_INTEGRITY_MINUTES
        self.data["integrity_interval_minutes"] = int(integ)

        alerts = self.data.get("alerts")
        if not isinstance(alerts, dict):
            alerts = {}
        targets = alerts.get("targets") if isinstance(alerts, dict) else {}
        alerts["targets"] = self._sanitize_alert_targets(targets)
        self.data["alerts"] = alerts

        try:
            history_limit = self._normalize_diagnostics_history_limit(
                self.data.get("diagnostics_history_limit", DEFAULT_DIAGNOSTICS_HISTORY_LIMIT)
            )
        except ValueError:
            history_limit = DEFAULT_DIAGNOSTICS_HISTORY_LIMIT
        self.data["diagnostics_history_limit"] = history_limit

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

    def get_auto_sync_delay_minutes(self) -> int:
        try:
            value = int(self.data.get("auto_sync_delay_minutes", 30))
        except Exception:
            value = 30
        return max(5, min(60, value))

    async def set_auto_sync_delay_minutes(self, minutes: int):
        try:
            value = int(minutes)
        except Exception as err:
            raise ValueError("Invalid auto sync delay") from err
        value = max(5, min(60, value))
        self.data["auto_sync_delay_minutes"] = value
        await self.async_save()

    def _normalize_diagnostics_history_limit(self, limit: Any) -> int:
        if limit is None:
            raise ValueError("Invalid diagnostics history limit")
        try:
            value = int(limit)
        except Exception as err:
            raise ValueError("Invalid diagnostics history limit") from err
        if value < MIN_DIAGNOSTICS_HISTORY_LIMIT:
            return MIN_DIAGNOSTICS_HISTORY_LIMIT
        if value > MAX_DIAGNOSTICS_HISTORY_LIMIT:
            return MAX_DIAGNOSTICS_HISTORY_LIMIT
        return value

    def get_diagnostics_history_limit(self) -> int:
        try:
            return self._normalize_diagnostics_history_limit(
                self.data.get("diagnostics_history_limit", DEFAULT_DIAGNOSTICS_HISTORY_LIMIT)
            )
        except ValueError:
            return DEFAULT_DIAGNOSTICS_HISTORY_LIMIT

    def get_diagnostics_history_bounds(self) -> Tuple[int, int]:
        return (MIN_DIAGNOSTICS_HISTORY_LIMIT, MAX_DIAGNOSTICS_HISTORY_LIMIT)

    async def set_diagnostics_history_limit(self, limit: Any) -> int:
        value = self._normalize_diagnostics_history_limit(limit)
        self.data["diagnostics_history_limit"] = value
        await self.async_save()
        return value

    def get_integrity_interval_minutes(self) -> int:
        try:
            return int(self.data.get("integrity_interval_minutes", self.DEFAULT_INTEGRITY_MINUTES))
        except Exception:
            return self.DEFAULT_INTEGRITY_MINUTES

    async def set_integrity_interval_minutes(self, minutes: int):
        self.data["integrity_interval_minutes"] = int(minutes)
        await self.async_save()

    def _sanitize_alert_targets(self, raw: Any) -> Dict[str, Dict[str, Any]]:
        cleaned: Dict[str, Dict[str, Any]] = {}
        if isinstance(raw, dict):
            for target, cfg in raw.items():
                if not isinstance(target, str) or not target.strip():
                    continue
                data: Dict[str, Any] = {}
                if isinstance(cfg, dict):
                    data["device_offline"] = bool(cfg.get("device_offline"))
                    data["integrity_failed"] = bool(cfg.get("integrity_failed"))
                    data["any_denied"] = bool(cfg.get("any_denied"))
                    granted_cfg = cfg.get("granted") if isinstance(cfg.get("granted"), dict) else {}
                    if not granted_cfg and isinstance(cfg.get("granted_users"), list):
                        granted_cfg = {
                            "any": bool(cfg.get("granted_any")),
                            "users": cfg.get("granted_users"),
                        }
                else:
                    data["device_offline"] = False
                    data["integrity_failed"] = False
                    data["any_denied"] = False
                    granted_cfg = {}

                users_raw = []
                if isinstance(granted_cfg, dict):
                    users_raw = granted_cfg.get("users") or []
                    any_flag = bool(granted_cfg.get("any"))
                else:
                    any_flag = False

                users_list: List[str] = []
                if isinstance(users_raw, (list, tuple, set)):
                    for item in users_raw:
                        s = str(item).strip()
                        if s:
                            users_list.append(s)
                elif isinstance(users_raw, str) and users_raw.strip():
                    users_list = [users_raw.strip()]

                data["granted"] = {"any": any_flag, "users": users_list}
                cleaned[target] = data
        return cleaned

    def get_alert_targets(self) -> Dict[str, Dict[str, Any]]:
        alerts = self.data.get("alerts") or {}
        targets = alerts.get("targets") if isinstance(alerts, dict) else {}
        return self._sanitize_alert_targets(targets)

    async def set_alert_targets(self, targets: Dict[str, Any]):
        self.data.setdefault("alerts", {})["targets"] = self._sanitize_alert_targets(targets)
        await self.async_save()

    def targets_for_event(self, event_type: str, *, user_id: Optional[str] = None) -> List[str]:
        mapping = self.get_alert_targets()
        out: List[str] = []
        norm_user = str(user_id).strip() if user_id not in (None, "") else None
        for target, cfg in mapping.items():
            if event_type == "device_offline" and cfg.get("device_offline"):
                out.append(target)
            elif event_type == "integrity_failed" and cfg.get("integrity_failed"):
                out.append(target)
            elif event_type == "any_denied" and cfg.get("any_denied"):
                out.append(target)
            elif event_type == "user_granted":
                granted = cfg.get("granted") or {}
                if granted.get("any"):
                    out.append(target)
                elif norm_user and norm_user in (granted.get("users") or []):
                    out.append(target)
        return out


# ---------------------- Robust device user lookup + delete ---------------------- #
async def _lookup_device_user_ids_by_ha_key(api: AkuvoxAPI, ha_key: str) -> List[Dict[str, str]]:
    out: List[Dict[str, str]] = []
    target = str(ha_key or "").strip()
    if not target:
        return out
    target_norm = normalize_ha_id(target)

    try:
        dev_users = await api.user_list()
    except Exception:
        dev_users = []

    seen: set[Tuple[str, str, str]] = set()
    for u in dev_users or []:
        dev_id = str(u.get("ID") or "")
        user_id = str(u.get("UserID") or "")
        name = str(u.get("Name") or "")
        candidates = {c for c in (dev_id, user_id, name, _key_of_user(u)) if c}
        candidate_norms = {normalize_ha_id(c) for c in candidates if normalize_ha_id(c)}
        if target not in candidates and (not target_norm or target_norm not in candidate_norms):
            continue

        key_tuple = (dev_id, user_id, name)
        if key_tuple in seen:
            continue
        seen.add(key_tuple)
        out.append({"ID": dev_id, "UserID": user_id, "Name": name})

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
        self._last_mark: Optional[datetime] = None
        self._last_delay_from_default = False
        self._active: bool = False

    def _root(self) -> Dict[str, Any]:
        return self.hass.data.get(DOMAIN, {}) or {}

    def _default_delay_minutes(self) -> int:
        root = self._root()
        settings = root.get("settings_store")
        if settings and hasattr(settings, "get_auto_sync_delay_minutes"):
            try:
                return settings.get_auto_sync_delay_minutes()
            except Exception:
                pass
        return 30

    def _normalize_delay(self, delay_minutes: Optional[int]) -> int:
        if delay_minutes is None:
            return self._default_delay_minutes()
        try:
            value = int(delay_minutes)
        except Exception:
            return self._default_delay_minutes()
        if value <= 0:
            return 0
        return max(5, min(60, value))

    def _set_health_status(self, entry_id: Optional[str], status: str):
        root = self._root()

        try:
            status_value = str(status or "pending")
        except Exception:
            status_value = "pending"

        def mark(coord: AkuvoxCoordinator):
            coord.health["sync_status"] = status_value

        if entry_id:
            data = root.get(entry_id)
            if data and data.get("coordinator"):
                mark(data["coordinator"])
            return

        pending_targets: set[str] = set()
        if self._pending_devices:
            pending_targets.update(self._pending_devices)

        if not pending_targets or self._pending_all:
            for key, data in root.items():
                if key in (
                    "groups_store",
                    "users_store",
                    "schedules_store",
                    "sync_manager",
                    "sync_queue",
                    "_ui_registered",
                    "settings_store",
                ):
                    continue
                pending_targets.add(key)

        for key in pending_targets:
            data = root.get(key)
            coord = data.get("coordinator") if isinstance(data, dict) else None
            if coord:
                mark(coord)

    def mark_change(self, entry_id: Optional[str] = None, delay_minutes: Optional[int] = None):
        self._set_health_status(entry_id, "pending")
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

        effective_delay = self._normalize_delay(delay_minutes)
        self._last_delay_from_default = delay_minutes is None
        now = datetime.now()
        self._last_mark = now
        eta = now + timedelta(minutes=effective_delay)
        self.next_sync_eta = eta

        if effective_delay <= 0:
            self.hass.async_create_task(self.run())
            return

        def _schedule_cb(_now):
            self.hass.async_create_task(self.run())

        self._handle = async_call_later(self.hass, effective_delay * 60, _schedule_cb)

    def refresh_default_delay(self):
        if self._handle is None or not self._last_delay_from_default or not self._last_mark:
            return

        default_minutes = self._default_delay_minutes()
        eta = self._last_mark + timedelta(minutes=default_minutes)
        remaining = (eta - datetime.now()).total_seconds()

        try:
            self._handle()
        except Exception:
            pass
        self._handle = None

        if remaining <= 0:
            self.next_sync_eta = datetime.now()
            self._last_delay_from_default = True
            self.hass.async_create_task(self.run())
            return

        self.next_sync_eta = eta
        self._last_delay_from_default = True

        def _schedule_cb(_now):
            self.hass.async_create_task(self.run())

        self._handle = async_call_later(self.hass, remaining, _schedule_cb)

    async def run(self, only_entry: Optional[str] = None):
        async with self._lock:
            self.next_sync_eta = None
            self._active = True
            try:
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
                            if (
                                not self._pending_all
                                and self._pending_devices
                                and k not in self._pending_devices
                            ):
                                continue
                            targets.append((k, coord, api))

                manager: SyncManager = root.get("sync_manager")  # type: ignore
                if not manager or not targets:
                    return

                for entry_id, coord, _api in targets:
                    try:
                        coord.health["sync_status"] = "in_progress"
                    except Exception:
                        pass
                    try:
                        await manager.reconcile_device(entry_id, full=True)
                        coord.health["sync_status"] = "in_sync"
                        coord.health["last_sync"] = _now_hh_mm()
                        try:
                            coord._append_event("Sync succeeded")  # type: ignore[attr-defined]
                        except Exception:
                            pass
                    except Exception as err:
                        coord.health["sync_status"] = "pending"
                        try:
                            coord._append_event(f"Sync failed: {err}")  # type: ignore[attr-defined]
                        except Exception:
                            pass
                    try:
                        await coord.async_request_refresh()
                    except Exception:
                        pass
            finally:
                self._pending_all = False
                self._pending_devices.clear()
                self._handle = None
                self._active = False

    async def sync_now(self, entry_id: Optional[str] = None):
        self._set_health_status(entry_id, "in_progress" if entry_id else "pending")
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
        self._integrity_unsub = None
        self._integrity_minutes = 15
        self._apply_integrity_interval(self._integrity_minutes)
        self._reboot_unsub = None
        self._interval_unsub = async_track_time_interval(
            hass,
            self._interval_sync_cb,
            timedelta(minutes=30),
        )

    def _apply_integrity_interval(self, minutes: int):
        minutes = max(5, min(24 * 60, int(minutes)))
        if self._integrity_unsub:
            try:
                self._integrity_unsub()
            except Exception:
                pass
            self._integrity_unsub = None

        self._integrity_minutes = minutes
        self._integrity_unsub = async_track_time_interval(
            self.hass,
            self._integrity_check_cb,
            timedelta(minutes=minutes),
        )

    def set_integrity_interval(self, minutes: Optional[int]):
        if minutes is None:
            minutes = self._settings_store().get_integrity_interval_minutes()
        try:
            value = int(minutes)
        except Exception as exc:
            raise ValueError("Invalid integrity interval") from exc
        value = max(5, min(24 * 60, value))
        settings: AkuvoxSettingsStore = self._settings_store()
        self.hass.async_create_task(settings.set_integrity_interval_minutes(value))
        if value != getattr(self, "_integrity_minutes", None):
            self._apply_integrity_interval(value)

    def get_integrity_interval_minutes(self) -> int:
        return getattr(self, "_integrity_minutes", 15)

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
                "_panel_registered",
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
        if sq:
            if getattr(sq, "_active", False):
                return "Syncing…"
            if sq.next_sync_eta:
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
            sanitized: Dict[str, Any]
            if isinstance(spec, dict):
                sanitized = dict(spec)
                sanitized["days"] = list(spec.get("days") or [])
            else:
                sanitized = {}
            try:
                await api.schedule_set(name, sanitized)
            except Exception:
                try:
                    await api.schedule_add(name, sanitized)
                except Exception:
                    pass

    async def _remove_missing_users(self, api: AkuvoxAPI, local_users: List[Dict[str, Any]], registry_keys_set: set):
        rogue_keys: List[str] = []
        for u in local_users or []:
            kid = _key_of_user(u)
            canonical_kid = normalize_ha_id(kid)
            if canonical_kid and canonical_kid not in registry_keys_set:
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
        schedules_all: Dict[str, Any] = {}
        if schedules_store:
            try:
                schedules_all = schedules_store.all()
            except Exception:
                schedules_all = {}

        device_type_raw = (coord.health.get("device_type") or "").strip()
        device_type = device_type_raw.lower()
        is_intercom = device_type == "intercom"

        raw_registry: Dict[str, Any] = users_store.all() if users_store else {}
        registry: Dict[str, Any] = {}
        for key, value in (raw_registry or {}).items():
            canonical = normalize_ha_id(key)
            if canonical:
                registry[canonical] = value
        registry_keys = list(registry.keys())
        reg_key_set = set(registry_keys)

        await self._remove_missing_users(api, local_users, reg_key_set)

        if full and schedules_store:
            try:
                await self._push_schedules(api, schedules_all)
            except Exception:
                pass

        # Resolve device schedule IDs after pushing (so we use what the device knows)
        sched_map = await self._device_schedule_map(api)

        exit_allow_map: Dict[str, bool] = {}
        for name, spec in (schedules_all or {}).items():
            if not isinstance(spec, dict):
                continue
            exit_allow_map[name.strip().lower()] = bool(spec.get("always_permit_exit"))
        for builtin in ("24/7 access", "24/7", "24x7", "always"):
            exit_allow_map.setdefault(builtin, True)

        def _find_local_by_key(ha_key: str) -> Optional[Dict[str, Any]]:
            for u in local_users:
                if _key_of_user(u) == ha_key:
                    return u
            return None

        add_batch: List[Dict[str, Any]] = []
        replace_list: List[Tuple[str, Dict[str, Any]]] = []  # (ha_key, desired_payload)
        delete_only_keys: List[str] = []
        face_root_base = face_base_url(self.hass)

        for ha_key in registry_keys:
            prof = registry.get(ha_key) or {}
            ha_groups = list(prof.get("groups") or ["Default"])
            should_have_access = any(g in device_groups for g in ha_groups)
            local = _find_local_by_key(ha_key)

            desired_base = _desired_device_user_payload(
                self.hass,
                ha_key,
                prof,
                local,
                opts=opts,
                sched_map=sched_map,
                exit_allow_map=exit_allow_map,
                face_root_base=face_root_base,
                device_type_raw=device_type_raw,
            )

            if should_have_access:
                if not local:
                    add_batch.append(desired_base)
                else:
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
        raw_registry = users_store.all() if users_store else {}
        registry: Dict[str, Any] = {}
        reg_keys: List[str] = []
        for key, value in (raw_registry or {}).items():
            canonical = normalize_ha_id(key)
            if canonical:
                registry.setdefault(canonical, value)
                reg_keys.append(canonical)

        schedules_store = self._schedules_store()
        schedules_all: Dict[str, Any] = {}
        if schedules_store:
            try:
                schedules_all = schedules_store.all()
            except Exception:
                schedules_all = {}

        exit_allow_map: Dict[str, bool] = {}
        for name, spec in (schedules_all or {}).items():
            if not isinstance(spec, dict):
                continue
            exit_allow_map[name.strip().lower()] = bool(spec.get("always_permit_exit"))
        for builtin in ("24/7 access", "24/7", "24x7", "always"):
            exit_allow_map.setdefault(builtin, True)

        face_root_base = face_base_url(self.hass)

        for entry_id, coord, api, opts in self._devices():
            try:
                opts = opts or {}
                dev_users = await api.user_list()
                coord.users = dev_users or []
                device_records: Dict[str, List[Dict[str, Any]]] = {}
                for record in coord.users or []:
                    key = _key_of_user(record)
                    canonical_key = normalize_ha_id(key)
                    if canonical_key:
                        device_records.setdefault(canonical_key, []).append(record)
                    else:
                        device_records.setdefault(key, []).append(record)

                device_groups: List[str] = list(opts.get("sync_groups") or ["Default"])
                should_have: set[str] = set()
                for k in reg_keys:
                    prof = registry.get(k) or {}
                    ha_groups = list(prof.get("groups") or ["Default"])
                    if any(g in device_groups for g in ha_groups):
                        should_have.add(k)

                sched_map = await self._device_schedule_map(api)
                device_type_raw = (coord.health.get("device_type") or "").strip()
                mismatch_reason: Optional[str] = None

                for ha_key in should_have:
                    records = device_records.get(ha_key, [])
                    if not records:
                        mismatch_reason = f"missing {ha_key}"
                        break
                    if len(records) > 1:
                        mismatch_reason = f"duplicate {ha_key}"
                        break
                    local = records[0]
                    desired = _desired_device_user_payload(
                        self.hass,
                        ha_key,
                        registry.get(ha_key) or {},
                        local,
                        opts=opts,
                        sched_map=sched_map,
                        exit_allow_map=exit_allow_map,
                        face_root_base=face_root_base,
                        device_type_raw=device_type_raw,
                    )
                    diffs = _integrity_field_differences(local, desired)
                    if diffs:
                        mismatch_reason = f"{ha_key} mismatch: {', '.join(diffs)}"
                        break

                if mismatch_reason is None:
                    for key, records in device_records.items():
                        canonical_key = normalize_ha_id(key)
                        if not canonical_key:
                            continue
                        if canonical_key in should_have:
                            continue
                        if records:
                            mismatch_reason = f"rogue user {key}"
                            break

                if mismatch_reason is None:
                    try:
                        coord._append_event("Integrity check passed")  # type: ignore[attr-defined]
                    except Exception:
                        pass
                else:
                    try:
                        coord._append_event(f"Integrity mismatch — {mismatch_reason}; queued sync")  # type: ignore[attr-defined]
                    except Exception:
                        pass
                    try:
                        if hasattr(coord, "_send_alert_notification"):
                            await coord._send_alert_notification("integrity_failed")  # type: ignore[attr-defined]
                    except Exception:
                        pass
                    if sq:
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

    _migrate_face_storage(hass)

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
        try:
            root["sync_manager"].set_integrity_interval(settings.get_integrity_interval_minutes())
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

    settings_store: AkuvoxSettingsStore = root.get("settings_store")
    try:
        diagnostics_history_limit = (
            settings_store.get_diagnostics_history_limit()
            if settings_store
            else DEFAULT_DIAGNOSTICS_HISTORY_LIMIT
        )
    except Exception:
        diagnostics_history_limit = DEFAULT_DIAGNOSTICS_HISTORY_LIMIT

    api = AkuvoxAPI(
        host=cfg.get(CONF_HOST),
        port=cfg.get(CONF_PORT, 80),
        username=cfg.get(CONF_USERNAME) or None,
        password=cfg.get(CONF_PASSWORD) or None,
        use_https=cfg.get("use_https", DEFAULT_USE_HTTPS),
        verify_ssl=cfg.get("verify_ssl", DEFAULT_VERIFY_SSL),
        session=session,
        diagnostics_history_limit=diagnostics_history_limit,
    )

    storage = AkuvoxStorage(hass, entry.entry_id)
    await storage.async_load()

    device_name = cfg.get(CONF_DEVICE_NAME, entry.title)
    device_type = cfg.get(CONF_DEVICE_TYPE, "Intercom")

    raw_relays = cfg.get(CONF_RELAY_ROLES)
    if not isinstance(raw_relays, dict):
        raw_relays = {
            "relay_a": cfg.get("relay_a_role"),
            "relay_b": cfg.get("relay_b_role"),
        }
    relay_roles = normalize_relay_roles(raw_relays, device_type)

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
            "relay_roles": relay_roles,
        },
    }

    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
    await coord.async_config_entry_first_refresh()

    # ---------- Services ----------
    async def _ensure_local_face_for_user(user_id: str) -> str:
        """
        Ensure component FaceData path exists (actual upload/capture handled elsewhere).
        Returns the canonical API URL (e.g. /api/AK_AC/FaceData/<USER>.jpg).
        """
        face_root = face_storage_dir(hass)
        face_root.mkdir(parents=True, exist_ok=True)
        filename = f"{user_id}.jpg"
        return f"/api/AK_AC/FaceData/{filename}"

    async def svc_add_user(call):
        d = call.data
        name: str = d["name"].strip()

        users_store: AkuvoxUsersStore = hass.data[DOMAIN]["users_store"]
        ha_id = users_store.next_free_ha_id()
        users_store.reserve_id(ha_id)
        await users_store.async_save()

        # Canonical FaceUrl that the device will fetch
        face_url = f"{face_base_url(hass)}/{ha_id}.jpg"

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
            access_start=d.get("access_start")
            if "access_start" in d
            else date.today().isoformat(),
            access_end=d.get("access_end") if "access_end" in d else None,
        )

        hass.data[DOMAIN]["sync_queue"].mark_change(None)

    async def svc_edit_user(call):
        d = call.data
        raw_key = d.get("id")
        canonical_key = normalize_ha_id(raw_key)
        key = canonical_key or str(raw_key)
        users_store: AkuvoxUsersStore = hass.data[DOMAIN]["users_store"]

        effective_id = canonical_key or key
        new_face_url = d.get("face_url") if "face_url" in d else f"{face_base_url(hass)}/{effective_id}.jpg"

        await users_store.upsert_profile(
            effective_id,
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
            access_start=d.get("access_start") if "access_start" in d else None,
            access_end=d.get("access_end") if "access_end" in d else None,
        )

        hass.data[DOMAIN]["sync_queue"].mark_change(None)

    async def svc_delete_user(call):
        raw_key = call.data.get("id") or call.data.get("key")
        key = str(raw_key or "").strip()
        if not key:
            return

        canonical = normalize_ha_id(key)
        lookup_key = canonical or key
        removal_keys = {key}
        if canonical:
            removal_keys.add(canonical)

        root = hass.data.get(DOMAIN, {})
        users_store: Optional[AkuvoxUsersStore] = root.get("users_store")
        if users_store:
            await users_store.delete(lookup_key)

        # immediate cascade: delete from every device using robust lookup
        manager: SyncManager | None = root.get("sync_manager")  # type: ignore[assignment]
        if manager:
            for entry_id, coord, api, _ in manager._devices():
                try:
                    id_records = await _lookup_device_user_ids_by_ha_key(api, lookup_key)
                    if id_records:
                        for rec in id_records:
                            await _delete_user_every_way(api, rec)
                    else:
                        await _delete_user_every_way(
                            api,
                            {
                                "ID": lookup_key,
                                "UserID": lookup_key,
                                "Name": lookup_key,
                            },
                        )
                    try:
                        await coord.async_request_refresh()
                    except Exception:
                        pass
                except Exception:
                    pass

        # remove face files from all known storage locations
        face_dirs: List[Path] = []
        try:
            face_dirs.append(face_storage_dir(hass))
        except Exception:
            pass

        face_dirs.append(Path(__file__).parent / "www" / "FaceData")

        try:
            face_dirs.append(Path(hass.config.path("www")) / "AK_Access_ctrl" / "FaceData")
        except Exception:
            pass

        for base in face_dirs:
            try:
                resolved_base = base.resolve()
            except Exception:
                continue

            for ext in FACE_FILE_EXTENSIONS:
                for removal_key in removal_keys:
                    try:
                        candidate = (resolved_base / f"{removal_key}.{ext}").resolve()
                        candidate.relative_to(resolved_base)
                    except Exception:
                        continue

                    if candidate.exists():
                        try:
                            candidate.unlink()
                        except Exception:
                            continue

        queue: Optional[SyncQueue] = root.get("sync_queue")  # type: ignore[assignment]
        if queue:
            queue.mark_change(None)

    async def svc_upload_face(call):
        """
        Legacy helper kept: simply records the canonical /api/AK_AC face URL.
        Actual file writing/placing happens outside this service.
        """
        d = call.data
        raw_key = d.get("id")
        canonical = normalize_ha_id(raw_key)
        key = canonical or str(raw_key)
        face_url = await _ensure_local_face_for_user(canonical or key)
        users_store: AkuvoxUsersStore = hass.data[DOMAIN]["users_store"]
        await users_store.upsert_profile(canonical or key, face_url=face_url, status="pending")
        hass.data[DOMAIN]["sync_queue"].mark_change(None)

    async def svc_reboot_device(call):
        entry_id = call.data.get("entry_id")
        triggered_by = _context_user_name(hass, getattr(call, "context", None))

        root = hass.data[DOMAIN]
        manager = root.get("sync_manager")
        targets: List[Tuple[AkuvoxCoordinator, AkuvoxAPI]] = []

        if entry_id:
            data = root.get(entry_id)
            coord = data and data.get("coordinator")
            api = data and data.get("api")
            if coord and api:
                targets.append((coord, api))
        elif manager:
            for _entry_id, coord, api, _ in manager._devices():
                if coord and api:
                    targets.append((coord, api))

        for coord, api in targets:
            try:
                await api.system_reboot()
            except Exception as err:
                try:
                    coord._append_event(f"Reboot failed: {err}")  # type: ignore[attr-defined]
                except Exception:
                    pass
                continue

            _mark_coordinator_rebooting(coord, triggered_by=triggered_by)

            try:
                await coord.async_request_refresh()
            except Exception:
                pass

    async def svc_force_full_sync(call):
        entry_id = call.data.get("entry_id")
        triggered_by = _context_user_name(hass, getattr(call, "context", None))

        root = hass.data[DOMAIN]
        manager: SyncManager = root.get("sync_manager")  # type: ignore[assignment]
        queue: SyncQueue = root.get("sync_queue")  # type: ignore[assignment]

        if not manager or not queue:
            return

        coords: List[AkuvoxCoordinator] = []
        for entry, coord, *_ in manager._devices():
            if entry_id and entry != entry_id:
                continue
            if coord:
                coords.append(coord)

        if not coords:
            return

        for coord in coords:
            _log_full_sync(coord, triggered_by)

        try:
            await queue.sync_now(entry_id)
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
            entry_obj = hass.config_entries.async_get_entry(entry_id)
            if entry_obj:
                new_options = dict(entry_obj.options)
                new_options["exit_device"] = enabled
                hass.config_entries.async_update_entry(entry_obj, options=new_options)
            queue: SyncQueue = hass.data[DOMAIN].get("sync_queue")  # type: ignore[assignment]
            if queue:
                queue.mark_change(entry_id)

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
    hass.services.async_register(DOMAIN, "force_full_sync", svc_force_full_sync)
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

    if not hass.data[DOMAIN].get("_panel_registered"):
        if _register_admin_dashboard(hass):
            hass.data[DOMAIN]["_panel_registered"] = True

    async def _options_updated(_hass: HomeAssistant, updated_entry: ConfigEntry):
        if updated_entry.entry_id != entry.entry_id:
            return
        new_cfg = {**updated_entry.data, **updated_entry.options}
        new_interval = int(new_cfg.get(CONF_POLL_INTERVAL, DEFAULT_POLL_INTERVAL))
        coord.update_interval = timedelta(seconds=max(10, new_interval))
        new_groups = list(new_cfg.get(CONF_DEVICE_GROUPS, ["Default"])) or ["Default"]
        raw_roles = new_cfg.get(CONF_RELAY_ROLES)
        if not isinstance(raw_roles, dict):
            raw_roles = {
                "relay_a": new_cfg.get("relay_a_role"),
                "relay_b": new_cfg.get("relay_b_role"),
            }
        new_relay_roles = normalize_relay_roles(raw_roles, new_cfg.get(CONF_DEVICE_TYPE, "Intercom"))
        hass.data[DOMAIN][entry.entry_id]["options"].update(
            {
                "participate_in_sync": bool(new_cfg.get(CONF_PARTICIPATE, True)),
                "sync_groups": new_groups,
                "exit_device": bool(new_cfg.get("exit_device", False)),
                "relay_roles": new_relay_roles,
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
                "_panel_registered",
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

            if root.pop("_panel_registered", False):
                _remove_admin_dashboard(hass)
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
