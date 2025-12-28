from __future__ import annotations

import asyncio
from collections.abc import Mapping
from datetime import date, datetime, timedelta
import logging
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Callable, Set

from homeassistant.const import Platform

try:  # pragma: no cover - fallback for test stubs without full HA constants
    from homeassistant.const import EVENT_HOMEASSISTANT_STARTED
except (ImportError, AttributeError):  # pragma: no cover - executed in unit tests
    EVENT_HOMEASSISTANT_STARTED = "homeassistant_start"
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
    MIN_HEALTH_CHECK_INTERVAL,
    MAX_HEALTH_CHECK_INTERVAL,
    DEFAULT_ACCESS_HISTORY_LIMIT,
    MIN_ACCESS_HISTORY_LIMIT,
    MAX_ACCESS_HISTORY_LIMIT,
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
from .access_history import AccessHistory
from .http import (
    face_base_url,
    face_filename_from_reference,
    face_storage_dir,
    register_ui,
    FACE_FILE_EXTENSIONS,
)  # provides /api/akuvox_ac/ui/* + /api/AK_AC/* assets
from .ha_id import ha_id_from_int, is_ha_id, normalize_ha_id

HA_EVENT_ACCCESS = "akuvox_access_event"  # fired for access denied / exit override


_LOGGER = logging.getLogger(__name__)


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
    return str(
        u.get("UserID")
        or u.get("UserId")
        or u.get("ID")
        or u.get("Name")
        or ""
    )


def _name_matches_user_id(name: Any, user_id: Any) -> bool:
    """Return True when the name matches the user id (normalized)."""

    try:
        name_text = str(name or "").strip()
    except Exception:
        name_text = ""
    try:
        user_id_text = str(user_id or "").strip()
    except Exception:
        user_id_text = ""
    if not name_text or not user_id_text:
        return False
    name_norm = normalize_ha_id(name_text) or name_text
    user_norm = normalize_ha_id(user_id_text) or user_id_text
    return name_norm == user_norm


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

EXIT_PERMISSION_MATCH = "match"
EXIT_PERMISSION_WORKING_DAYS = "working_days"
EXIT_PERMISSION_ALWAYS = "always"
_VALID_EXIT_PERMISSIONS: set[str] = {
    EXIT_PERMISSION_MATCH,
    EXIT_PERMISSION_WORKING_DAYS,
    EXIT_PERMISSION_ALWAYS,
}

EXIT_CLONE_SUFFIX = " - EP"


def _normalize_exit_permission(value: Any) -> Optional[str]:
    """Coerce arbitrary representations into one of the supported exit policies."""

    if value is None:
        return None

    if isinstance(value, bool):
        return EXIT_PERMISSION_ALWAYS if value else EXIT_PERMISSION_MATCH

    text = str(value).strip().lower()
    if not text:
        return None

    cleaned = text.replace("-", "_").replace(" ", "_")

    if cleaned in {"same_as_entry", "match", "matching", "default"}:
        return EXIT_PERMISSION_MATCH

    if cleaned in {"working_days", "work_days", "workingdays", "workdays"}:
        return EXIT_PERMISSION_WORKING_DAYS

    if cleaned in {
        "always",
        "always_allow",
        "always_permit",
        "always_permit_exit",
        "24_7",
        "24x7",
        "1",
        "true",
        "yes",
    }:
        return EXIT_PERMISSION_ALWAYS

    if cleaned in _VALID_EXIT_PERMISSIONS:
        return cleaned

    return None

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
    "FaceRegister",
    "faceRegister",
    "face_register",
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


_FACE_FILENAME_KEYS = (
    "FaceFileName",
    "faceFileName",
    "face_filename",
    "face_file_name",
)


_FACE_URL_KEYS = (
    "FaceUrl",
    "FaceURL",
    "faceUrl",
    "faceURL",
)


_FACE_REGISTER_KEYS = (
    "FaceRegister",
    "faceRegister",
    "face_register",
)


_FACE_STATUS_KEYS = (
    "FaceRegisterStatus",
    "face_register_status",
)


def _face_status_from_record(record: Dict[str, Any]) -> Optional[bool]:
    if not isinstance(record, dict):
        return None
    for key in _FACE_STATUS_KEYS:
        if key in record:
            flag = _normalize_boolish(record.get(key))
            if flag is not None:
                return flag
    return None


def _ensure_face_payload_fields(
    payload: Dict[str, Any],
    *,
    ha_key: str,
    sources: Tuple[Optional[Dict[str, Any]], ...],
) -> None:
    """Ensure FaceUrl/FaceRegister fields are canonically present in payload."""

    def _extract_first(keys: Tuple[str, ...]) -> Optional[str]:
        for source in (payload, *sources):
            if not isinstance(source, dict):
                continue
            for key in keys:
                value = source.get(key)
                if value in (None, ""):
                    continue
                text = str(value).strip()
                if text:
                    return text
        return None

    face_url = _extract_first(_FACE_URL_KEYS)
    if not face_url:
        face_url = _extract_first(_FACE_FILENAME_KEYS)

    if face_url:
        payload["FaceUrl"] = str(face_url)

    for key in _FACE_FILENAME_KEYS:
        payload.pop(key, None)
    for key in _FACE_URL_KEYS:
        if key != "FaceUrl":
            payload.pop(key, None)

    register_value = _extract_first(_FACE_REGISTER_KEYS)
    if register_value:
        register_value = str(register_value).strip()

    face_flag = None
    for source in (payload, *sources):
        if not isinstance(source, dict):
            continue
        flag = _face_flag_from_record(source)
        if flag is not None:
            face_flag = flag
            break

    if face_url and register_value != "1":
        payload["FaceRegister"] = 1
    elif face_flag:
        payload["FaceRegister"] = 1
    elif register_value:
        try:
            payload["FaceRegister"] = int(register_value)
        except Exception:
            payload["FaceRegister"] = register_value

    for key in _FACE_REGISTER_KEYS:
        if key != "FaceRegister":
            payload.pop(key, None)

    payload.setdefault("Type", "0")


def _prepare_user_add_payload(
    ha_key: str,
    payload: Dict[str, Any],
    *,
    sources: Tuple[Optional[Dict[str, Any]], ...] = (),
) -> Dict[str, Any]:
    """Return a device-friendly payload for user.add."""

    cleaned: Dict[str, Any] = {k: v for k, v in (payload or {}).items() if v is not None}

    canonical_key = str(ha_key or "").strip()
    if not canonical_key:
        canonical_key = _key_of_user(cleaned)

    if canonical_key:
        cleaned.setdefault("UserID", canonical_key)

    cleaned.pop("ID", None)

    source_tuple: Tuple[Optional[Dict[str, Any]], ...] = sources or (payload,)

    _ensure_face_payload_fields(
        cleaned,
        ha_key=str(cleaned.get("UserID") or canonical_key or ""),
        sources=source_tuple,
    )

    face_status: Optional[bool] = None
    for source in source_tuple:
        candidate = _face_status_from_record(source or {})
        if candidate is not None:
            face_status = candidate
            break
    if face_status is None:
        for source in source_tuple:
            candidate = _face_flag_from_record(source or {})
            if candidate is not None:
                face_status = candidate
                break
    if face_status is None:
        face_status = False

    cleaned["FaceRegisterStatus"] = "1" if face_status else "0"

    if cleaned.get("ScheduleRelay") and "Schedule-Relay" not in cleaned:
        cleaned["Schedule-Relay"] = cleaned["ScheduleRelay"]

    cleaned.setdefault("PriorityCall", "0")
    cleaned.setdefault("DialAccount", "0")
    cleaned.setdefault("Group", "Default")
    cleaned.setdefault("AnalogSystem", "0")
    cleaned.setdefault("AnalogNumber", "")
    cleaned.setdefault("AnalogReplace", "")
    cleaned.setdefault("AnalogProxyAddress", "")

    return cleaned

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
    exit_schedule_map: Optional[Dict[str, Dict[str, Any]]],
    face_root_base: str,
    device_type_raw: str,
) -> Dict[str, Any]:
    """Build the canonical payload a device record should expose for a registry profile."""

    profile = profile or {}
    local = local or {}
    sched_map = sched_map or {}
    exit_schedule_map = exit_schedule_map or {}

    schedule_name = (profile.get("schedule_name") or local.get("Schedule") or "24/7 Access").strip()
    schedule_lower = schedule_name.lower()
    exit_info = exit_schedule_map.get(schedule_lower, {})
    exit_permission = _normalize_exit_permission(profile.get("exit_permission"))
    if exit_permission is None:
        exit_permission = _normalize_exit_permission(local.get("exit_permission"))
    if exit_permission is None:
        default_mode = str(exit_info.get("default_mode") or "").strip().lower()
        if default_mode == EXIT_PERMISSION_ALWAYS:
            exit_permission = EXIT_PERMISSION_ALWAYS
        elif default_mode == EXIT_PERMISSION_WORKING_DAYS:
            exit_permission = EXIT_PERMISSION_WORKING_DAYS
    if exit_permission is None:
        exit_permission = EXIT_PERMISSION_MATCH
    key_holder_flag = _key_holder_from_record(profile)
    if key_holder_flag is None:
        key_holder_flag = _key_holder_from_record(local)
    key_holder = bool(key_holder_flag)
    explicit_id = str(profile.get("schedule_id") or "").strip()

    exit_override = bool(opts.get("exit_device"))
    exit_schedule_id: Optional[str] = None
    effective_schedule = schedule_name

    if exit_override:
        clone_name = str(exit_info.get("clone_name") or "").strip()
        clone_schedule_id = clone_name and sched_map.get(clone_name.lower()) or ""

        if exit_permission == EXIT_PERMISSION_ALWAYS:
            if clone_schedule_id:
                exit_schedule_id = clone_schedule_id
                effective_schedule = clone_name
            else:
                exit_schedule_id = "1001"
                effective_schedule = "24/7 Access"
        elif exit_permission == EXIT_PERMISSION_WORKING_DAYS:
            if clone_schedule_id:
                exit_schedule_id = clone_schedule_id
                effective_schedule = clone_name
            if not exit_schedule_id:
                exit_permission = EXIT_PERMISSION_MATCH
                effective_schedule = schedule_name

    if exit_override and exit_permission != EXIT_PERMISSION_MATCH and exit_schedule_id:
        schedule_id = exit_schedule_id
    elif explicit_id and explicit_id.isdigit():
        schedule_id = explicit_id
    else:
        schedule_id = sched_map.get(effective_schedule.lower(), "")
    if not schedule_id:
        schedule_id = "1001"

    relay_roles = normalize_relay_roles(opts.get("relay_roles"), device_type_raw)
    try:
        opts["relay_roles"] = relay_roles
    except Exception:
        pass

    door_digits = door_relays(relay_roles)
    relay_suffix = relay_suffix_for_user(relay_roles, key_holder, device_type_raw)
    if not relay_suffix:
        relay_suffix = "1"
    if not schedule_id:
        schedule_id = "1001"
    schedule_relay = f"{schedule_id}-{relay_suffix}".rstrip(";") + ";"

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

        def _append_token(value: str) -> None:
            token = value.strip()
            if token.endswith(";"):
                token = token.rstrip(";")
            if token and token not in schedule_list:
                schedule_list.append(token)

        local_schedule = local.get("Schedule")
        if isinstance(local_schedule, (list, tuple, set)):
            for entry in local_schedule:
                text = _string_or_default(entry, default="")
                if text:
                    _append_token(text)
        elif local_schedule not in (None, ""):
            text = _string_or_default(local_schedule, default="")
            if text:
                _append_token(text)

        local_schedule_id = _string_or_default(local.get("ScheduleID"), default="")
        if local_schedule_id:
            cleaned_id = local_schedule_id.rstrip(";") or local_schedule_id
            if not schedule_list:
                schedule_list.append(cleaned_id)
            elif cleaned_id not in schedule_list:
                schedule_list.insert(0, cleaned_id)

        return schedule_list

    def _normalise_license_plate() -> List[Dict[str, Any]]:
        source = profile.get("license_plate")
        if not isinstance(source, (list, tuple)):
            source = profile.get("LicensePlate")
        if not isinstance(source, (list, tuple)):
            source = local.get("LicensePlate")
        result: List[Dict[str, Any]] = []
        seen: Set[str] = set()
        if isinstance(source, (list, tuple)):
            for entry in source:
                normalized = ""
                cleaned: Dict[str, Any] = {}
                if isinstance(entry, dict):
                    for key, value in entry.items():
                        if value in (None, ""):
                            continue
                        if isinstance(value, str):
                            value = value.strip()
                            if not value:
                                continue
                        cleaned[key] = value
                    candidate = (
                        entry.get("Plate")
                        or entry.get("plate")
                        or entry.get("value")
                        or entry.get("Value")
                    )
                    if candidate not in (None, ""):
                        normalized = str(candidate).strip().upper()
                else:
                    normalized = str(entry or "").strip().upper()

                if normalized:
                    lowered = normalized.lower()
                    if lowered in seen:
                        continue
                    seen.add(lowered)
                    cleaned["Plate"] = normalized
                    result.append(cleaned)
                elif cleaned:
                    result.append(cleaned)

                if len(result) >= 5:
                    break
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
    elif schedule_id and schedule_id not in schedule_list:
        schedule_list.insert(0, schedule_id)

    schedule_value = schedule_id or "1001"
    fallback_schedule: Optional[str] = None
    for token in schedule_list:
        candidate = str(token or "").strip().rstrip(";")
        if not candidate:
            continue
        if candidate.isdigit():
            schedule_value = candidate
            break
        if fallback_schedule is None:
            fallback_schedule = candidate
    else:
        if fallback_schedule:
            schedule_value = fallback_schedule

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
        "ScheduleID": schedule_id,
        "Schedule": schedule_value,
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
        "Type": "0",
    }

    if schedule_relay:
        desired["Schedule-Relay"] = schedule_relay

    device_id = _string_or_default(profile.get("device_id"), local.get("ID"), default="")
    if device_id:
        desired["ID"] = device_id

    profile_pin_present = False
    profile_pin_value: Any = None
    for key in ("pin", "PrivatePIN", "private_pin", "Pin"):
        if isinstance(profile, Mapping) and key in profile:
            profile_pin_present = True
            profile_pin_value = profile.get(key)
            break

    if profile_pin_present:
        if profile_pin_value in (None, ""):
            desired["PrivatePIN"] = ""
        else:
            desired["PrivatePIN"] = str(profile_pin_value).strip()
    else:
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
            desired["FaceUrl"] = face_url_str

            face_active: Optional[bool] = _face_flag_from_record(profile)
            if face_active is None:
                face_active = _face_flag_from_record(local)
            if face_active is None:
                try:
                    face_active = _face_asset_exists(hass, ha_key)
                except Exception:
                    face_active = None
            if face_active:
                desired["FaceRegister"] = 1

    if desired.get("FaceRegister"):
        status_flag: Optional[bool] = None
        for source in (profile, local):
            candidate = _face_status_from_record(source or {})
            if candidate is not None:
                status_flag = candidate
                break
        if status_flag is None:
            for source in (local, profile):
                candidate = _face_flag_from_record(source or {})
                if candidate is not None:
                    status_flag = candidate
                    break
        if status_flag is None:
            status_flag = False
        desired["FaceRegisterStatus"] = "1" if status_flag else "0"
    else:
        desired["FaceRegisterStatus"] = "0"

    return desired


def _build_exit_schedule_map(schedules: Optional[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """Map normalized schedule names to their exit-clone metadata."""

    result: Dict[str, Dict[str, Any]] = {}
    if not isinstance(schedules, dict):
        return result

    def _norm(text: Any) -> str:
        return str(text or "").strip().lower()

    for name, spec in schedules.items():
        if not isinstance(spec, dict):
            continue
        normalized_name = _norm(name)
        if not normalized_name:
            continue

        is_clone = bool(spec.get("system_exit_clone"))
        clone_for = _norm(spec.get("exit_clone_for"))
        if is_clone and clone_for and clone_for != normalized_name:
            entry = result.setdefault(clone_for, {})
            entry.setdefault("clone_name", str(name).strip())
            continue

        entry = result.setdefault(normalized_name, {})
        clone_name = str(spec.get("exit_clone_name") or "").strip()
        if clone_name:
            entry["clone_name"] = clone_name
        if spec.get("always_permit_exit"):
            entry["default_mode"] = EXIT_PERMISSION_ALWAYS

    for name, spec in schedules.items():
        if not isinstance(spec, dict):
            continue
        normalized_name = _norm(name)
        if normalized_name in result and result[normalized_name].get("clone_name"):
            continue
        if normalized_name.endswith(EXIT_CLONE_SUFFIX.lower()):
            base = _norm(name[: -len(EXIT_CLONE_SUFFIX)])
            if base:
                entry = result.setdefault(base, {})
                entry.setdefault("clone_name", str(name).strip())

    for builtin in ("24/7 access", "24/7", "24x7", "always"):
        entry = result.setdefault(builtin, {})
        entry.setdefault("default_mode", EXIT_PERMISSION_ALWAYS)

    return result


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

    def _norm_user_id(record: Dict[str, Any]) -> str:
        return _norm(record.get("UserID") or record.get("UserId"))

    if _norm_user_id(local) != _norm_user_id(expected):
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
        local_url = _norm(local.get("FaceUrl") or local.get("FaceURL"))

        def _url_basename(value: str) -> str:
            cleaned = (value or "").strip()
            if not cleaned:
                return ""
            cleaned = cleaned.split("?", 1)[0].split("#", 1)[0]
            cleaned = cleaned.rstrip("/").replace("\\", "/")
            return cleaned.rsplit("/", 1)[-1]

        if not local_url:
            diffs.append("face url")
        elif local_url != expected_url:
            if _url_basename(local_url) != _url_basename(expected_url):
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

    def _exit_clone_name(self, name: str) -> str:
        base = str(name or "").strip()
        if not base:
            return ""
        if base.lower().endswith(EXIT_CLONE_SUFFIX.lower()):
            return base
        return f"{base}{EXIT_CLONE_SUFFIX}"

    def _is_exit_clone(self, name: str, spec: Optional[Mapping[str, Any]] = None) -> bool:
        if spec and spec.get("system_exit_clone"):
            return True
        if spec and spec.get("exit_clone_for"):
            clone_for = str(spec.get("exit_clone_for") or "").strip().lower()
            if clone_for and clone_for != str(name or "").strip().lower():
                return True
        return str(name or "").strip().lower().endswith(EXIT_CLONE_SUFFIX.lower())

    def _ensure_exit_clones(self, schedules: Optional[Dict[str, Any]] = None) -> bool:
        store = schedules if isinstance(schedules, dict) else self.data.setdefault("schedules", {})
        if not isinstance(store, dict):
            return False

        changed = False
        base_lookup: Dict[str, str] = {}
        expected_clones: Dict[str, Dict[str, Any]] = {}

        for name, spec in list(store.items()):
            if not isinstance(spec, dict):
                continue
            if self._is_exit_clone(name, spec):
                continue
            normalized = str(name or "").strip().lower()
            if normalized:
                base_lookup[normalized] = name

        for name, spec in list(store.items()):
            if not isinstance(spec, dict):
                continue
            if self._is_exit_clone(name, spec):
                continue

            clone_name = str(spec.get("exit_clone_name") or self._exit_clone_name(name)).strip()
            if spec.get("exit_clone_name") != clone_name:
                spec = dict(spec)
                spec["exit_clone_name"] = clone_name
                store[name] = spec
                changed = True

            clone_payload = {
                "start": "00:00",
                "end": "23:59",
                "days": list(spec.get("days") or []),
                "always_permit_exit": True,
                "type": spec.get("type", "0"),
                "date_start": spec.get("date_start", ""),
                "date_end": spec.get("date_end", ""),
                "system_exit_clone": True,
                "exit_clone_for": name,
            }

            normalized_clone = self._normalize_payload(clone_name, clone_payload)
            normalized_clone["exit_clone_for"] = name
            expected_clones[clone_name] = normalized_clone

        for clone_name, normalized_clone in expected_clones.items():
            current_clone = store.get(clone_name)
            if current_clone != normalized_clone:
                store[clone_name] = normalized_clone
                changed = True

        for name, spec in list(store.items()):
            if not isinstance(spec, dict):
                continue
            if not self._is_exit_clone(name, spec):
                continue

            if name in expected_clones:
                # Ensure the stored payload stays normalised and points at the canonical base name.
                normalized_clone = self._normalize_payload(name, spec)
                target_base = normalized_clone.get("exit_clone_for", "")
                if target_base:
                    base_key = str(target_base).strip().lower()
                    canonical = base_lookup.get(base_key)
                    if canonical and canonical != target_base:
                        normalized_clone["exit_clone_for"] = canonical
                if store[name] != normalized_clone:
                    store[name] = normalized_clone
                    changed = True
                continue

            normalized_clone = self._normalize_payload(name, spec)
            target_base = str(normalized_clone.get("exit_clone_for") or "").strip().lower()
            canonical = base_lookup.get(target_base)

            if canonical:
                desired_name = self._exit_clone_name(canonical)
                desired_payload = expected_clones.get(desired_name)
                if desired_payload:
                    if name != desired_name:
                        store.pop(name, None)
                        store[desired_name] = desired_payload
                        changed = True
                    elif store[name] != desired_payload:
                        store[name] = desired_payload
                        changed = True
                    continue

            if normalized_clone.get("system_exit_clone") or normalized_clone.get("exit_clone_for"):
                store.pop(name, None)
                changed = True

        return changed

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
            "type": "0",
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
                normalized["type"] = str(payload.get("type") or payload.get("Type") or "0")

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

        system_clone_flag = self._as_bool(payload.get("system_exit_clone")) if payload else False
        normalized["system_exit_clone"] = system_clone_flag
        if system_clone_flag or (payload and "exit_clone_for" in payload):
            normalized["exit_clone_for"] = str((payload or {}).get("exit_clone_for") or "").strip()
        elif "exit_clone_for" in normalized:
            normalized["exit_clone_for"] = str(normalized.get("exit_clone_for") or "").strip()

        if payload and "exit_clone_name" in payload:
            normalized["exit_clone_name"] = str(payload.get("exit_clone_name") or "").strip()
        elif not system_clone_flag:
            normalized["exit_clone_name"] = self._exit_clone_name(name)

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
                    "type": "0",
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
                    "type": "0",
                },
            )

        changed = original != existing
        if self._ensure_exit_clones(existing):
            changed = True
        self.data["schedules"] = existing
        if changed:
            await self.async_save()

    async def async_save(self):
        await super().async_save(self.data)

    def all(self) -> Dict[str, Any]:
        return dict(self.data.get("schedules") or {})

    async def upsert(self, name: str, payload: Dict[str, Any]):
        schedules = self.data.setdefault("schedules", {})
        schedules[name] = self._normalize_payload(name, payload)
        self._ensure_exit_clones(schedules)
        await self.async_save()

    async def delete(self, name: str):
        if name in ("24/7 Access", "No Access"):
            return
        schedules = self.data.setdefault("schedules", {})
        removed = False
        spec = schedules.get(name)
        if self._is_exit_clone(name, spec):
            removed = schedules.pop(name, None) is not None
        else:
            if name in schedules:
                schedules.pop(name, None)
                removed = True
            clone_name = self._exit_clone_name(name)
            if clone_name in schedules:
                schedules.pop(clone_name, None)
                removed = True
        if removed:
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
        used: set[str] = set()
        users = self.data.get("users") or {}
        if isinstance(users, dict):
            for key, profile in users.items():
                canonical = normalize_ha_id(key)
                if not canonical:
                    continue
                if isinstance(profile, dict):
                    status = str(profile.get("status") or "").strip().lower()
                    if status == "deleted":
                        # Profiles marked as deleted have already been freed for reuse.
                        continue
                used.add(canonical)
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
        source: Optional[str] = None,
        license_plate: Optional[List[Any]] = None,
        exit_permission: Optional[str] = None,
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
        if source is not None:
            normalized_source = str(source).strip()
            if normalized_source:
                u["Source"] = normalized_source
            else:
                u.pop("Source", None)
        if license_plate is not None:
            cleaned: List[str] = []
            seen: Set[str] = set()
            iterable = license_plate if isinstance(license_plate, (list, tuple)) else []
            for entry in iterable:
                text = ""
                if isinstance(entry, str):
                    text = entry.strip().upper()
                elif isinstance(entry, dict):
                    candidate = (
                        entry.get("Plate")
                        or entry.get("plate")
                        or entry.get("value")
                        or entry.get("Value")
                    )
                    if candidate is not None:
                        text = str(candidate).strip().upper()
                if not text:
                    continue
                lowered = text.lower()
                if lowered in seen:
                    continue
                seen.add(lowered)
                cleaned.append(text)
                if len(cleaned) >= 5:
                    break
            if cleaned:
                u["license_plate"] = cleaned
            else:
                u.pop("license_plate", None)
        if exit_permission is not None:
            normalized_exit = _normalize_exit_permission(exit_permission)
            if normalized_exit:
                u["exit_permission"] = normalized_exit
            else:
                u.pop("exit_permission", None)
        await self.async_save()

    async def delete(self, key: str):
        raw = str(key or "").strip()
        if not raw:
            return

        users = self.data.get("users")
        if not isinstance(users, dict):
            users = {}
            self.data["users"] = users

        canonical = normalize_ha_id(raw)
        removal_keys = {raw}
        if canonical:
            removal_keys.add(canonical)

        for stored_key in list(users.keys()):
            if stored_key in removal_keys:
                users.pop(stored_key, None)
                continue
            if canonical and normalize_ha_id(stored_key) == canonical:
                users.pop(stored_key, None)
        await self.async_save()


class AkuvoxSettingsStore(Store):
    DEFAULT_HEALTH_SECONDS = DEFAULT_POLL_INTERVAL
    MIN_HEALTH_SECONDS = MIN_HEALTH_CHECK_INTERVAL
    MAX_HEALTH_SECONDS = MAX_HEALTH_CHECK_INTERVAL

    DEFAULT_INTEGRITY_MINUTES = 15
    DEFAULT_CREDENTIAL_PROMPTS = {
        "code": True,
        "token": True,
        "anpr": False,
        "face": True,
        "phone": True,
    }

    def __init__(self, hass: HomeAssistant):
        super().__init__(hass, 1, f"{DOMAIN}_settings.json")
        self.data: Dict[str, Any] = {
            "auto_sync_time": None,
            "auto_reboot": {"time": None, "days": []},
            "integrity_interval_minutes": self.DEFAULT_INTEGRITY_MINUTES,
            "auto_sync_delay_minutes": 30,
            "alerts": {"targets": {}},
            "diagnostics_history_limit": DEFAULT_DIAGNOSTICS_HISTORY_LIMIT,
            "health_check_interval_seconds": self.DEFAULT_HEALTH_SECONDS,
            "credential_prompts": dict(self.DEFAULT_CREDENTIAL_PROMPTS),
            "access_history_limit": DEFAULT_ACCESS_HISTORY_LIMIT,
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

        try:
            health_interval = self._normalize_health_interval(
                self.data.get("health_check_interval_seconds", self.DEFAULT_HEALTH_SECONDS)
            )
        except ValueError:
            health_interval = self.DEFAULT_HEALTH_SECONDS
        self.data["health_check_interval_seconds"] = health_interval

        self.data["credential_prompts"] = self._sanitize_credential_prompts(
            self.data.get("credential_prompts")
        )

        try:
            access_limit = self._normalize_access_history_limit(
                self.data.get("access_history_limit", DEFAULT_ACCESS_HISTORY_LIMIT)
            )
        except ValueError:
            access_limit = DEFAULT_ACCESS_HISTORY_LIMIT
        self.data["access_history_limit"] = access_limit

    async def async_save(self):
        await super().async_save(self.data)

    def _sanitize_credential_prompts(self, raw: Any) -> Dict[str, bool]:
        defaults = dict(self.DEFAULT_CREDENTIAL_PROMPTS)
        if not isinstance(raw, dict):
            return defaults

        for key in defaults.keys():
            if isinstance(raw.get(key), bool):
                defaults[key] = raw[key]

        # Backwards compatibility: if "phone" isn't explicitly provided but
        # legacy "token" was toggled, mirror the value so existing settings
        # still govern the phone prompt.
        if "phone" in defaults and not isinstance(raw.get("phone"), bool):
            token_value = raw.get("token")
            if isinstance(token_value, bool):
                defaults["phone"] = token_value

        return defaults

    def get_credential_prompts(self) -> Dict[str, bool]:
        return self._sanitize_credential_prompts(self.data.get("credential_prompts"))

    async def set_credential_prompts(self, prompts: Any) -> Dict[str, bool]:
        sanitized = self._sanitize_credential_prompts(prompts)
        self.data["credential_prompts"] = sanitized
        await self.async_save()
        return sanitized

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

    def _normalize_health_interval(self, seconds: Any) -> int:
        try:
            value = int(seconds)
        except Exception as err:
            raise ValueError("Invalid health check interval") from err
        if value < self.MIN_HEALTH_SECONDS:
            return self.MIN_HEALTH_SECONDS
        if value > self.MAX_HEALTH_SECONDS:
            return self.MAX_HEALTH_SECONDS
        return value

    def get_health_check_interval_seconds(self) -> int:
        try:
            return self._normalize_health_interval(
                self.data.get("health_check_interval_seconds", self.DEFAULT_HEALTH_SECONDS)
            )
        except ValueError:
            return self.DEFAULT_HEALTH_SECONDS

    def get_health_check_interval_bounds(self) -> Tuple[int, int]:
        return (self.MIN_HEALTH_SECONDS, self.MAX_HEALTH_SECONDS)

    async def set_health_check_interval_seconds(self, seconds: Any) -> int:
        value = self._normalize_health_interval(seconds)
        self.data["health_check_interval_seconds"] = value
        await self.async_save()
        return value

    def _normalize_access_history_limit(self, limit: Any) -> int:
        if limit is None:
            raise ValueError("Invalid access history limit")
        try:
            value = int(limit)
        except Exception as err:
            raise ValueError("Invalid access history limit") from err
        if value < MIN_ACCESS_HISTORY_LIMIT:
            return MIN_ACCESS_HISTORY_LIMIT
        if value > MAX_ACCESS_HISTORY_LIMIT:
            return MAX_ACCESS_HISTORY_LIMIT
        return value

    def get_access_history_limit(self) -> int:
        try:
            return self._normalize_access_history_limit(
                self.data.get("access_history_limit", DEFAULT_ACCESS_HISTORY_LIMIT)
            )
        except ValueError:
            return DEFAULT_ACCESS_HISTORY_LIMIT

    def get_access_history_bounds(self) -> Tuple[int, int]:
        return (MIN_ACCESS_HISTORY_LIMIT, MAX_ACCESS_HISTORY_LIMIT)

    async def set_access_history_limit(self, limit: Any) -> int:
        value = self._normalize_access_history_limit(limit)
        self.data["access_history_limit"] = value
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
        user_id = str(u.get("UserID") or u.get("UserId") or "")
        name = str(u.get("Name") or "")
        user_id_alt = str(u.get("UserId") or "")
        candidates = {
            c for c in (dev_id, user_id, user_id_alt, name, _key_of_user(u)) if c
        }
        candidate_norms = {normalize_ha_id(c) for c in candidates if normalize_ha_id(c)}
        if target not in candidates and (not target_norm or target_norm not in candidate_norms):
            continue

        key_tuple = (dev_id, user_id or user_id_alt, name)
        if key_tuple in seen:
            continue
        seen.add(key_tuple)
        out.append({"ID": dev_id, "UserID": user_id or user_id_alt, "Name": name})

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
    await try_one(rec.get("UserId", ""))
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
        self._tick_unsub: Optional[Callable[[], None]] = None
        self._startup_unsub: Optional[Callable[[], None]] = None

        try:
            self._tick_unsub = async_track_time_interval(
                hass,
                self._background_tick,
                timedelta(minutes=1),
            )
        except Exception:
            self._tick_unsub = None

        try:
            self._startup_unsub = hass.bus.async_listen_once(
                EVENT_HOMEASSISTANT_STARTED,
                self._handle_hass_started,
            )
        except Exception:
            self._startup_unsub = None

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
                    "_panel_registered",
                    "settings_store",
                ):
                    continue
                if not isinstance(data, Mapping):
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

    def ensure_future_run(self):
        if self._active:
            return

        eta = self.next_sync_eta
        if not isinstance(eta, datetime):
            return

        if eta > datetime.now():
            return

        handle = self._handle
        self._handle = None

        if handle:
            try:
                handle()
            except Exception:
                pass

        self.next_sync_eta = datetime.now()
        self.hass.async_create_task(self.run())

    def _handle_hass_started(self, _event):
        try:
            self.ensure_future_run()
        except Exception:
            pass

        if self._startup_unsub:
            try:
                self._startup_unsub()
            except Exception:
                pass
            self._startup_unsub = None

        try:
            self.hass.async_create_task(self._background_tick(datetime.now()))
        except Exception:
            pass

    async def _background_tick(self, _now):
        try:
            self.ensure_future_run()
        except Exception:
            pass

        if self._active or self._handle is not None:
            return

        if self._pending_all or self._pending_devices:
            return

        root = self._root()
        pending_detected = False

        for data in root.values():
            if not isinstance(data, Mapping):
                continue
            coord = data.get("coordinator")
            if not coord:
                continue
            health = getattr(coord, "health", {}) or {}
            status = str(health.get("sync_status") or "").strip().lower()
            online = bool(health.get("online", True))
            if online and status and status != "in_sync":
                pending_detected = True
                break

        if not pending_detected:
            users_store = root.get("users_store")
            if users_store and hasattr(users_store, "all"):
                try:
                    profiles = users_store.all() or {}
                except Exception:
                    profiles = {}
                for profile in profiles.values():
                    status = str((profile or {}).get("status") or "").strip().lower()
                    face_status = str((profile or {}).get("face_status") or "").strip().lower()
                    if status == "pending" or face_status == "pending":
                        pending_detected = True
                        break

        if pending_detected:
            try:
                self.mark_change(None, delay_minutes=0)
            except Exception:
                pass

    def shutdown(self):
        if self._tick_unsub:
            try:
                self._tick_unsub()
            except Exception:
                pass
            self._tick_unsub = None

        if self._startup_unsub:
            try:
                self._startup_unsub()
            except Exception:
                pass
            self._startup_unsub = None

        if self._handle:
            try:
                self._handle()
            except Exception:
                pass
            self._handle = None

        self._pending_all = False
        self._pending_devices.clear()

    async def run(self, only_entry: Optional[str] = None):
        async with self._lock:
            self.next_sync_eta = None
            self._active = True
            try:
                root = self._root()
                targets: List[Tuple[str, AkuvoxCoordinator, AkuvoxAPI]] = []
                if only_entry:
                    data = root.get(only_entry)
                    if isinstance(data, Mapping):
                        coord = data.get("coordinator")
                        api = data.get("api")
                        if coord and api:
                            targets.append((only_entry, coord, api))
                else:
                    for k, data in root.items():
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
                        if not isinstance(data, Mapping):
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

    async def sync_now(
        self, entry_id: Optional[str] = None, *, include_all: bool = False
    ):
        if include_all and entry_id:
            include_all = False

        if include_all and not entry_id:
            self._pending_all = True

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
        self._contact_sync_unsub = None
        self._integrity_unsub = None
        self._integrity_minutes = 15
        self._apply_integrity_interval(self._integrity_minutes)
        self._reboot_unsub = None
        self._interval_unsub = async_track_time_interval(
            hass,
            self._interval_sync_cb,
            timedelta(minutes=30),
        )
        self._contact_sync_unsub = async_track_time_change(
            hass,
            self._daily_contact_sync_cb,
            hour=23,
            minute=0,
            second=0,
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
                "access_history",
            ):
                continue
            if not isinstance(v, dict):
                continue
            coord = v.get("coordinator")
            api = v.get("api")
            opts = v.get("options") or {}
            if coord and api:
                out.append((k, coord, api, opts))
        return out

    @staticmethod
    def _normalize_phone(value: Any) -> str:
        text = str(value or "").strip()
        return "".join(ch for ch in text if ch.isdigit())

    @staticmethod
    def _extract_contact_items(payload: Any) -> List[Dict[str, Any]]:
        if not payload:
            return []
        if isinstance(payload, dict):
            for key in ("data", "Data"):
                data = payload.get(key)
                if isinstance(data, dict):
                    items = data.get("item") or data.get("items")
                    if isinstance(items, list):
                        return [it for it in items if isinstance(it, dict)]
            for key in ("item", "items", "Contact", "Contacts", "contact", "contacts"):
                items = payload.get(key)
                if isinstance(items, list):
                    return [it for it in items if isinstance(it, dict)]
        if isinstance(payload, list):
            return [it for it in payload if isinstance(it, dict)]
        return []

    async def _daily_contact_sync_cb(self, now):
        users_store = self._users_store()
        if not users_store:
            return

        raw_profiles = users_store.all()
        user_phone_raw: set[str] = set()
        user_phone_norm: set[str] = set()
        for profile in (raw_profiles or {}).values():
            if not isinstance(profile, dict):
                continue
            phone = str(profile.get("phone") or "").strip()
            if not phone:
                continue
            user_phone_raw.add(phone)
            normalized = self._normalize_phone(phone)
            if normalized:
                user_phone_norm.add(normalized)

        for _, coord, api, _ in self._devices():
            try:
                response = await api.contact_get()
            except Exception:
                continue

            contacts = self._extract_contact_items(response)
            for contact in contacts:
                phone = str(contact.get("Phone") or contact.get("PhoneNum") or contact.get("phone") or "").strip()
                if not phone:
                    continue
                normalized = self._normalize_phone(phone)
                if phone in user_phone_raw or (normalized and normalized in user_phone_norm):
                    continue
                try:
                    await api.contact_delete([{"Phone": phone}])
                except Exception:
                    continue
            try:
                await coord.async_request_refresh()
            except Exception:
                pass

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
            if hasattr(sq, "ensure_future_run"):
                try:
                    sq.ensure_future_run()
                except Exception:
                    pass
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

    async def _replace_user_on_device(
        self,
        api: AkuvoxAPI,
        desired: Dict[str, Any],
        ha_key: str,
        existing: Optional[Dict[str, Any]] = None,
    ):
        """Recreate the device record for ha_key via delete + add."""

        payload = _prepare_user_add_payload(ha_key, desired, sources=(desired, existing))

        delete_candidates: List[Dict[str, Any]] = []
        if isinstance(existing, dict):
            delete_candidates.append(existing)

        try:
            lookup_records = await _lookup_device_user_ids_by_ha_key(api, ha_key)
        except Exception:
            lookup_records = []
        for rec in lookup_records:
            if rec:
                delete_candidates.append(rec)

        seen_delete_keys: set[Tuple[str, str, str]] = set()
        for rec in delete_candidates:
            if not isinstance(rec, dict):
                continue
            key_tuple = (
                str(rec.get("ID") or ""),
                str(rec.get("UserID") or rec.get("UserId") or ""),
                str(rec.get("Name") or ""),
            )
            if key_tuple in seen_delete_keys:
                continue
            seen_delete_keys.add(key_tuple)
            try:
                await _delete_user_every_way(api, rec)
            except Exception:
                continue

        try:
            await api.user_add([payload])
        except Exception as err:
            _LOGGER.warning("Failed to replace user %s via delete+add: %s", ha_key, err)
            raise

    async def reconcile(self, full: bool = True):
        active_device_keys: set[str] = set()

        for entry_id, coord, *_ in self._devices():
            await self.reconcile_device(entry_id, full=full)

            try:
                current_users = list(getattr(coord, "users", []) or [])
            except Exception:
                current_users = []

            for record in current_users:
                candidates = {
                    str(record.get("ID") or ""),
                    str(record.get("UserID") or record.get("UserId") or ""),
                    str(record.get("Name") or ""),
                    _key_of_user(record),
                }
                for candidate in candidates:
                    canonical = normalize_ha_id(candidate)
                    if canonical:
                        active_device_keys.add(canonical)

        users_store = self._users_store()
        if not users_store:
            return

        try:
            registry_all = users_store.all()
        except Exception:
            registry_all = {}

        for key, profile in (registry_all or {}).items():
            status_raw = str((profile or {}).get("status") or "").strip().lower()
            if status_raw != "deleted":
                continue

            canonical = normalize_ha_id(key) or str(key)
            if canonical and canonical not in active_device_keys:
                try:
                    await users_store.delete(canonical)
                except Exception:
                    continue

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

        auto_delete_keys: Set[str] = set()
        for record in local_users or []:
            name_value = record.get("Name")
            user_id_value = (
                record.get("UserID")
                or record.get("UserId")
                or record.get("ID")
            )
            if _name_matches_user_id(name_value, user_id_value):
                key = normalize_ha_id(user_id_value) or str(user_id_value or "").strip()
                if key and key not in reg_key_set:
                    auto_delete_keys.add(key)

        if auto_delete_keys and users_store:
            for ha_key in sorted(auto_delete_keys):
                try:
                    await users_store.upsert_profile(
                        ha_key,
                        status="deleted",
                        groups=["No Access"],
                        schedule_name="No Access",
                        schedule_id="1002",
                    )
                except Exception:
                    pass

        await self._remove_missing_users(api, local_users, reg_key_set)

        if full and schedules_store:
            try:
                await self._push_schedules(api, schedules_all)
            except Exception:
                pass

        # Resolve device schedule IDs after pushing (so we use what the device knows)
        sched_map = await self._device_schedule_map(api)

        exit_schedule_map = _build_exit_schedule_map(schedules_all)

        def _find_local_by_key(ha_key: str) -> Optional[Dict[str, Any]]:
            for u in local_users:
                if _key_of_user(u) == ha_key:
                    return u
            return None

        add_batch: List[Dict[str, Any]] = []
        replace_list: List[Tuple[str, Dict[str, Any], Optional[Dict[str, Any]]]] = []
        delete_only_keys: List[str] = []
        face_root_base = face_base_url(self.hass)

        for ha_key in registry_keys:
            if ha_key in auto_delete_keys:
                local = _find_local_by_key(ha_key)
                if local:
                    delete_only_keys.append(ha_key)
                continue
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
                exit_schedule_map=exit_schedule_map,
                face_root_base=face_root_base,
                device_type_raw=device_type_raw,
            )

            if should_have_access:
                if not local:
                    add_batch.append(desired_base)
                else:
                    replace = full or str(prof.get("status") or "").lower() == "pending" or any(
                        str(local.get(k)) != str(v) for k, v in desired_base.items()
                    )
                    if replace:
                        replace_list.append((ha_key, desired_base, local))
            else:
                if local:
                    delete_only_keys.append(ha_key)
            # -----------------------------------------

        # 1) Add new users
        if add_batch:
            prepared_add_batch: List[Dict[str, Any]] = []
            for candidate in add_batch:
                ha_candidate = _key_of_user(candidate)
                prepared_add_batch.append(
                    _prepare_user_add_payload(ha_candidate, candidate, sources=(candidate,))
                )
            try:
                await api.user_add(prepared_add_batch)
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

        # 3) Replace changed users (update in place)
        for ha_key, desired, existing in replace_list:
            try:
                await self._replace_user_on_device(api, desired, ha_key, existing)
            except Exception:
                pass

        # Mark pending -> active
        try:
            for k in registry_keys:
                if (registry.get(k) or {}).get("status") == "pending":
                    await users_store.upsert_profile(k, status="active")
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

        exit_schedule_map = _build_exit_schedule_map(schedules_all)

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
                        exit_schedule_map=exit_schedule_map,
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

    if "access_history" not in root:
        root["access_history"] = AccessHistory()

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

    try:
        health_interval_override = (
            settings_store.get_health_check_interval_seconds()
            if settings_store and hasattr(settings_store, "get_health_check_interval_seconds")
            else None
        )
    except Exception:
        health_interval_override = None

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
    if health_interval_override:
        try:
            interval = int(health_interval_override)
        except Exception:
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

    def _normalise_face_filename(reference: Any, user_id: str) -> str:
        """Return a safe face filename derived from *reference* or fallback to <user_id>.jpg."""

        try:
            candidate = face_filename_from_reference(str(reference or ""), user_id)
        except Exception:
            candidate = ""

        cleaned = str(candidate or "").strip()
        if not cleaned:
            cleaned = f"{user_id}.jpg"

        suffix = Path(cleaned).suffix.lower().lstrip(".")
        if suffix and suffix not in FACE_FILE_EXTENSIONS:
            stem = Path(cleaned).stem or str(user_id or "face")
            cleaned = f"{stem}.jpg"
        elif not suffix:
            stem = Path(cleaned).stem or str(user_id or "face")
            cleaned = f"{stem}.jpg"

        return cleaned

    def _resolve_face_source_path(raw: Any) -> Optional[Path]:
        """Resolve a face image path relative to the HA config directory."""

        text = str(raw or "").strip()
        if not text:
            return None

        config_root = Path(hass.config.path(""))
        try:
            config_root = config_root.resolve()
        except Exception:
            pass

        candidate = Path(text)
        if text.startswith("/config/"):
            candidate = config_root / text[len("/config/") :]
        elif not candidate.is_absolute():
            candidate = Path(hass.config.path(text))

        try:
            resolved = candidate.resolve()
        except Exception:
            return None

        try:
            resolved.relative_to(config_root)
        except Exception:
            return None

        return resolved

    def _store_face_bytes(filename: str, data: Optional[bytes], *, source: Optional[Path] = None) -> None:
        """Persist *data* to the integration's FaceData directory under *filename*."""

        if not filename or not data:
            return

        try:
            dest_dir = face_storage_dir(hass)
        except Exception:
            return

        try:
            dest_dir.mkdir(parents=True, exist_ok=True)
        except Exception:
            return

        try:
            dest_path = (dest_dir / filename).resolve()
            dest_path.relative_to(dest_dir)
        except Exception:
            return

        if source is not None:
            try:
                if dest_path == source:
                    return
            except Exception:
                pass

        try:
            dest_path.write_bytes(data)
        except Exception:
            return

    async def svc_add_user(call):
        d = call.data
        name: str = d["name"].strip()

        users_store: AkuvoxUsersStore = hass.data[DOMAIN]["users_store"]
        ha_id = users_store.next_free_ha_id()
        users_store.reserve_id(ha_id)
        await users_store.async_save()

        raw_groups = d.get("groups") if d.get("groups") is not None else d.get("sync_groups")
        if isinstance(raw_groups, (list, tuple, set)):
            groups = [str(g).strip() for g in raw_groups if str(g or "").strip()]
        elif raw_groups in (None, ""):
            groups = []
        else:
            groups = [str(raw_groups).strip()]

        face_reference: Optional[str] = None
        face_reference_supplied = False
        for key in (
            "face_file_name",
            "face_filename",
            "FaceFileName",
            "faceFileName",
            "face_url",
            "FaceUrl",
            "FaceURL",
        ):
            if key not in d:
                continue
            text = str(d.get(key) or "").strip()
            if not text:
                continue
            face_reference = text
            face_reference_supplied = True
            break

        face_image_path_raw = d.get("face_image_path")
        face_image_path = str(face_image_path_raw or "").strip()
        face_source_path: Optional[Path] = None
        face_bytes: Optional[bytes] = None
        if face_image_path:
            face_source_path = _resolve_face_source_path(face_image_path)
            if face_source_path and face_source_path.is_file():
                try:
                    face_bytes = face_source_path.read_bytes()
                except Exception:
                    face_bytes = None
            if not face_reference:
                face_reference = face_image_path
                face_reference_supplied = True

        face_filename = _normalise_face_filename(face_reference or f"{ha_id}.jpg", ha_id)
        if face_bytes:
            _store_face_bytes(face_filename, face_bytes, source=face_source_path)

        face_url = f"{face_base_url(hass)}/{face_filename}"

        pin_payload: Optional[str] = None
        if "pin" in d:
            raw_pin = d.get("pin")
            if raw_pin in (None, ""):
                pin_payload = ""
            else:
                pin_payload = str(raw_pin)

        await users_store.upsert_profile(
            ha_id,
            name=name,
            groups=groups,
            pin=pin_payload,
            phone=str(d.get("phone")) if d.get("phone") else None,
            schedule_name=d.get("schedule_name") or "24/7 Access",
            key_holder=bool(d.get("key_holder", False)),
            access_level=d.get("access_level") or None,
            face_url=face_url,
            face_status="pending" if face_reference_supplied else None,
            face_synced_at="" if face_reference_supplied else None,
            status="pending",
            schedule_id=str(d.get("schedule_id")) if d.get("schedule_id") else None,
            access_start=d.get("access_start") if "access_start" in d else date.today().isoformat(),
            access_end=d.get("access_end") if "access_end" in d else None,
            source="Local",
            exit_permission=d.get("exit_permission"),
        )

        hass.data[DOMAIN]["sync_queue"].mark_change(None)

    async def svc_edit_user(call):
        d = call.data
        raw_key = d.get("id")
        canonical_key = normalize_ha_id(raw_key)
        key = canonical_key or str(raw_key)
        users_store: AkuvoxUsersStore = hass.data[DOMAIN]["users_store"]

        effective_id = canonical_key or key
        new_face_url = d.get("face_url") if "face_url" in d else None

        lp_payload = d.get("license_plate") if "license_plate" in d else None

        pin_payload_edit: Optional[str] = None
        if "pin" in d:
            raw_pin = d.get("pin")
            if raw_pin in (None, ""):
                pin_payload_edit = ""
            else:
                pin_payload_edit = str(raw_pin)

        await users_store.upsert_profile(
            effective_id,
            name=d.get("name"),
            groups=list(d.get("groups") or []) if "groups" in d else None,
            pin=pin_payload_edit,
            phone=str(d.get("phone")) if "phone" in d else None,
            schedule_name=d.get("schedule_name") if "schedule_name" in d else None,
            key_holder=bool(d.get("key_holder")) if "key_holder" in d else None,
            access_level=d.get("access_level") if "access_level" in d else None,
            face_url=new_face_url,
            status="pending",
            schedule_id=str(d.get("schedule_id")) if d.get("schedule_id") else None,
            access_start=d.get("access_start") if "access_start" in d else None,
            access_end=d.get("access_end") if "access_end" in d else None,
            source="Local",
            license_plate=lp_payload,
            exit_permission=d.get("exit_permission") if "exit_permission" in d else None,
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
        phone_to_remove: Optional[str] = None
        if users_store:
            try:
                profile = users_store.get(lookup_key) or {}
                raw_phone = str(profile.get("phone") or "").strip()
                if raw_phone:
                    phone_to_remove = raw_phone
            except Exception:
                phone_to_remove = None
        if users_store:
            try:
                await users_store.upsert_profile(
                    lookup_key,
                    status="deleted",
                    groups=["No Access"],
                    schedule_name="No Access",
                    schedule_id="1002",
                )
            except Exception:
                pass

        # immediate cascade: delete from every device using robust lookup
        manager: SyncManager | None = root.get("sync_manager")  # type: ignore[assignment]
        if manager:
            for entry_id, coord, api, _ in manager._devices():
                try:
                    if phone_to_remove:
                        try:
                            await api.contact_delete([{"Phone": phone_to_remove}])
                        except Exception:
                            pass
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
                                "UserId": lookup_key,
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

    async def svc_refresh_events(call):
        entry_id = call.data.get("entry_id")
        root = hass.data[DOMAIN]

        coords: List[AkuvoxCoordinator] = []
        if entry_id:
            data = root.get(entry_id)
            coord = data and data.get("coordinator")
            if coord:
                coords.append(coord)
        else:
            for key, data in root.items():
                if key in (
                    "groups_store",
                    "users_store",
                    "schedules_store",
                    "settings_store",
                    "sync_manager",
                    "sync_queue",
                    "_ui_registered",
                    "_panel_registered",
                ):
                    continue
                if not isinstance(data, dict):
                    continue
                coord = data.get("coordinator")
                if coord:
                    coords.append(coord)

        for coord in coords:
            try:
                await coord.async_refresh_access_history()
            except Exception:
                pass

    async def svc_force_full_sync(call):
        data = call.data if isinstance(call.data, Mapping) else {}
        entry_id = data.get("entry_id")
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

        include_all = not entry_id

        try:
            await queue.sync_now(entry_id, include_all=include_all)
        except Exception:
            pass

    async def svc_sync_now(call):
        data = call.data if isinstance(call.data, Mapping) else {}
        entry_id = data.get("entry_id")
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
    hass.services.async_register(DOMAIN, "refresh_events", svc_refresh_events)
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
            if sq:
                if hasattr(sq, "shutdown"):
                    try:
                        sq.shutdown()
                    except Exception:
                        pass
                elif getattr(sq, "_handle", None) is not None:
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
