from __future__ import annotations

import datetime as dt
import json
import logging
import json
import re
import time
from datetime import timedelta
from pathlib import Path
from collections import OrderedDict
from typing import Any, Dict, Iterable, List, Optional, Mapping, Set
from urllib.parse import urlencode, urlsplit, unquote

from aiohttp import web
from homeassistant.components.http.view import HomeAssistantView
from homeassistant.components.http.auth import async_sign_path
from homeassistant.components.http.const import KEY_HASS_REFRESH_TOKEN_ID, KEY_HASS_USER
from homeassistant.components.persistent_notification import async_create as notify
from homeassistant.core import HomeAssistant

from homeassistant.helpers.network import get_url

from .const import (
    DOMAIN,
    CONF_DEVICE_GROUPS,
    CONF_RELAY_ROLES,
    INTEGRATION_VERSION,
    INTEGRATION_VERSION_LABEL,
    DEFAULT_DIAGNOSTICS_HISTORY_LIMIT,
    MIN_DIAGNOSTICS_HISTORY_LIMIT,
    MAX_DIAGNOSTICS_HISTORY_LIMIT,
    MIN_HEALTH_CHECK_INTERVAL,
    MAX_HEALTH_CHECK_INTERVAL,
    EVENT_INBOUND_CALL,
    INBOUND_CALL_RESULT_APPROVED,
    INBOUND_CALL_RESULT_APPROVED_KEY_HOLDER,
    INBOUND_CALL_RESULT_DENIED,
    DEFAULT_ACCESS_HISTORY_LIMIT,
    MIN_ACCESS_HISTORY_LIMIT,
    MAX_ACCESS_HISTORY_LIMIT,
)

from .relay import alarm_capable as relay_alarm_capable, normalize_roles as normalize_relay_roles
from .ha_id import ha_id_from_int, is_ha_id, normalize_ha_id, normalize_user_id
from .access_history import AccessHistory, categorize_event

COMPONENT_ROOT = Path(__file__).parent
STATIC_ROOT = COMPONENT_ROOT / "www"
FACE_DATA_PATH = "/api/AK_AC/FaceData"
FACE_FILE_EXTENSIONS = ("jpg", "jpeg", "png", "webp")

EXIT_PERMISSION_MATCH = "match"
EXIT_PERMISSION_WORKING_DAYS = "working_days"
EXIT_PERMISSION_ALWAYS = "always"


def _component_face_dir() -> Path:
    """Return the canonical location for bundled face assets."""

    return (COMPONENT_ROOT / "www" / "FaceData").resolve()


def face_storage_dir(hass: HomeAssistant) -> Path:
    """Return the persistent storage location for uploaded face images."""

    base = Path(hass.config.path(DOMAIN))
    return (base / "FaceData").resolve()


def _legacy_face_dir(hass: HomeAssistant) -> Path:
    """Legacy location used by earlier builds for face images."""

    root = Path(hass.config.path("www"))
    return (root / "AK_Access_ctrl" / "FaceData").resolve()


def _folder_migration_candidates(hass: HomeAssistant) -> List[Path]:
    """Return possible legacy integration roots for migration."""

    candidates = [Path(hass.config.path("custom_components"))]
    try:
        config_root = Path(hass.config.path())
    except Exception:
        config_root = None
    if config_root is not None:
        candidates.extend([config_root, config_root.parent])
    for base in list(candidates):
        try:
            candidates.append(Path(str(base)))
        except Exception:
            continue
    seen: Set[Path] = set()
    unique: List[Path] = []
    for base in candidates:
        try:
            resolved = base.resolve()
        except Exception:
            continue
        if resolved in seen:
            continue
        seen.add(resolved)
        unique.append(resolved)
    return unique


def _maybe_migrate_component_folder(hass: HomeAssistant) -> bool:
    """Ensure the integration folder matches the domain name."""

    migrated = False
    for base in _folder_migration_candidates(hass):
        legacy_dir = base / "AK_Access_ctrl"
        target_dir = base / DOMAIN
        if not legacy_dir.exists() or target_dir.exists():
            continue
        try:
            legacy_dir.rename(target_dir)
        except Exception:
            continue
        else:
            migrated = True
    return migrated


def _face_candidate(base: Path, user_id: str, ext: str) -> Optional[Path]:
    try:
        candidate = (base / f"{user_id}.{ext}").resolve()
        candidate.relative_to(base)
    except Exception:
        return None
    return candidate


def _face_file_exists_in(base: Path, user_id: str) -> bool:
    for ext in FACE_FILE_EXTENSIONS:
        candidate = _face_candidate(base, user_id, ext)
        if candidate is not None and candidate.is_file():
            return True
    return False


def _remove_face_files(hass: HomeAssistant, user_id: str) -> None:
    removal_keys = {str(user_id or "").strip()}
    canonical = normalize_user_id(user_id)
    if canonical:
        removal_keys.add(canonical)
    removal_keys = {key for key in removal_keys if key}
    if not removal_keys:
        return

    face_dirs: List[Path] = []
    try:
        face_dirs.append(face_storage_dir(hass))
    except Exception:
        pass
    face_dirs.append(_component_face_dir())
    try:
        face_dirs.append(_legacy_face_dir(hass))
    except Exception:
        pass

    for base in face_dirs:
        try:
            resolved_base = base.resolve()
        except Exception:
            continue
        for ext in FACE_FILE_EXTENSIONS:
            for removal_key in removal_keys:
                candidate = _face_candidate(resolved_base, removal_key, ext)
                if candidate is None:
                    continue
                if candidate.exists():
                    try:
                        candidate.unlink()
                    except Exception:
                        continue


def _normalize_exit_permission_http(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, bool):
        return EXIT_PERMISSION_ALWAYS if value else EXIT_PERMISSION_MATCH
    text = str(value).strip().lower()
    if not text:
        return None
    cleaned = text.replace("-", "_").replace(" ", "_")
    if cleaned in {"match", "matching", "same_as_entry", "default"}:
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
    if cleaned in {
        EXIT_PERMISSION_MATCH,
        EXIT_PERMISSION_WORKING_DAYS,
        EXIT_PERMISSION_ALWAYS,
    }:
        return cleaned
    return None


def _face_image_exists(hass: HomeAssistant, user_id: str) -> bool:
    if not user_id:
        return False
    try:
        persistent_dir = face_storage_dir(hass)
        if _face_file_exists_in(persistent_dir, user_id):
            return True
    except Exception:
        pass
    try:
        if _face_file_exists_in(_component_face_dir(), user_id):
            return True
    except Exception:
        pass
    try:
        legacy_dir = _legacy_face_dir(hass)
    except Exception:
        return False
    try:
        return _face_file_exists_in(legacy_dir, user_id)
    except Exception:
        return False


def _parse_access_date(value: Any) -> Optional[dt.date]:
    """Normalize stored access dates to ``date`` objects."""

    if value is None:
        return None

    if isinstance(value, dt.date) and not isinstance(value, dt.datetime):
        return value

    if isinstance(value, dt.datetime):
        return value.date()

    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            base = text.split("T", 1)[0]
            parsed = dt.datetime.strptime(base, "%Y-%m-%d")
        except ValueError:
            return None
        return parsed.date()

    return None


def _extract_license_plates(record: Mapping[str, Any]) -> List[str]:
    if not isinstance(record, Mapping):
        return []

    raw = record.get("license_plate")
    if not isinstance(raw, (list, tuple)):
        raw = record.get("LicensePlate")

    result: List[str] = []
    if not isinstance(raw, (list, tuple)):
        return result

    seen: Set[str] = set()
    for entry in raw:
        text = ""
        if isinstance(entry, str):
            text = entry.strip()
        elif isinstance(entry, Mapping):
            candidate = (
                entry.get("Plate")
                or entry.get("plate")
                or entry.get("value")
                or entry.get("Value")
            )
            if candidate is not None:
                text = str(candidate).strip()
        if not text:
            continue
        text = text.upper()
        lowered = text.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        result.append(text)
        if len(result) >= 5:
            break

    return result


def _normalize_boolish(value: Any) -> Optional[bool]:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return bool(value)
    if isinstance(value, str):
        lowered = value.strip().lower()
        if not lowered:
            return None
        if lowered in {"1", "true", "yes", "y", "on"}:
            return True
        if lowered in {"0", "false", "no", "n", "off"}:
            return False
    return None


def _stringify_device_field(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, (int, float)):
        return str(int(value) if isinstance(value, bool) else value)
    if isinstance(value, bytes):
        try:
            return value.decode()
        except Exception:
            return None
    if isinstance(value, str):
        cleaned = value.strip()
        return cleaned or None
    return str(value)


def _sanitise_device_record(record: Optional[Dict[str, Any]]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    if not isinstance(record, dict):
        return out
    for key, value in record.items():
        if isinstance(value, (dict, list, tuple, set)):
            continue
        coerced = _stringify_device_field(value)
        if coerced is not None:
            out[str(key)] = coerced
    return out


def _record_matches_user(record: Dict[str, Any], user_id: str) -> bool:
    if not isinstance(record, dict):
        return False
    target = str(user_id or "").strip()
    if not target:
        return False
    for key in ("UserID", "UserId", "userId", "UserID", "ID", "Name"):
        candidate = record.get(key)
        if candidate is None:
            continue
        if str(candidate).strip() == target:
            return True
    return False


def _build_face_upload_payload(
    profile: Dict[str, Any],
    device_record: Optional[Dict[str, Any]],
    user_id: str,
    face_reference: str,
) -> Dict[str, Any]:
    base = _sanitise_device_record(device_record)
    payload: Dict[str, Any] = dict(base)

    payload["UserID"] = str(user_id)
    payload["UserId"] = str(user_id)
    payload["Name"] = str(profile.get("name") or payload.get("Name") or user_id)

    groups = profile.get("groups") if isinstance(profile.get("groups"), list) else []
    primary_group = None
    if isinstance(groups, list):
        for g in groups:
            if g not in (None, ""):
                primary_group = str(g)
                break
    if not primary_group:
        primary_group = payload.get("Group") or "Default"
    payload["Group"] = str(primary_group)

    schedule_id = str(profile.get("schedule_id") or payload.get("ScheduleID") or "").strip()
    if schedule_id:
        payload["ScheduleID"] = schedule_id
    schedule_name = profile.get("schedule_name") or payload.get("Schedule") or "24/7 Access"
    payload["Schedule"] = str(schedule_name)

    pin_present = False
    pin_value: Any = None
    for key in ("pin", "PrivatePIN", "private_pin", "Pin"):
        if isinstance(profile, Mapping) and key in profile:
            pin_present = True
            pin_value = profile.get(key)
            break

    if pin_present:
        if pin_value in (None, ""):
            payload["PrivatePIN"] = ""
        else:
            payload["PrivatePIN"] = str(pin_value).strip()
        for alias in ("Pin", "PIN"):
            payload.pop(alias, None)

    phone = profile.get("phone")
    if phone in (None, ""):
        phone = payload.get("PhoneNum") or payload.get("Phone")
    if phone not in (None, ""):
        payload["PhoneNum"] = str(phone)

    access_level = profile.get("access_level")
    if access_level not in (None, ""):
        payload["Level"] = str(access_level)

    key_holder_value = profile.get("key_holder")
    parsed = _normalize_boolish(key_holder_value)
    if parsed is None:
        parsed = _normalize_boolish(payload.get("KeyHolder"))
    payload["KeyHolder"] = "1" if parsed else "0"

    if "WebRelay" not in payload:
        payload["WebRelay"] = "0"

    if face_reference not in (None, ""):
        payload["FaceUrl"] = str(face_reference)

    # Use device path in FaceUrl (from upload result); never send FaceFileName to device
    for key in ("FaceFileName", "faceFileName"):
        payload.pop(key, None)
    payload.pop("faceInfo", None)
    payload["FaceRegister"] = 1
    payload.setdefault("Type", "0")

    return payload


async def _push_face_to_devices(
    hass: HomeAssistant,
    root: Dict[str, Any],
    user_id: str,
    face_bytes: bytes,
    face_url_public: str,
) -> None:
    manager = root.get("sync_manager")
    if not manager:
        return

    users_store = root.get("users_store")
    profile: Dict[str, Any] = {}
    if users_store:
        try:
            profile = users_store.get(user_id) or {}
        except Exception:
            profile = {}

    for entry_id, coord, api, _opts in manager._devices():
        device_name = getattr(coord, "device_name", entry_id)
        device_type = str((getattr(coord, "health", {}) or {}).get("device_type") or "").strip().lower()
        if device_type == "keypad":
            _LOGGER.debug("Skipping face upload for keypad %s", device_name)
            continue
        record = None
        try:
            for candidate in list(getattr(coord, "users", []) or []):
                if _record_matches_user(candidate, user_id):
                    record = candidate
                    break
        except Exception:
            record = None

        if record is None:
            try:
                device_users = await api.user_list()
            except Exception as err:
                _LOGGER.debug("Unable to refresh users before face upload on %s: %s", device_name, err)
                device_users = []
            for candidate in device_users or []:
                if _record_matches_user(candidate, user_id):
                    record = candidate
                    break

        record_face_source: Optional[str] = None
        if isinstance(record, dict):
            for key in ("FaceFileName", "faceFileName", "FaceUrl", "FaceURL"):
                candidate = record.get(key)
                if candidate in (None, ""):
                    continue
                record_face_source = str(candidate)
                break

        profile_face_source = profile.get("face_url") if isinstance(profile, dict) else None
        reference = record_face_source or profile_face_source or face_url_public
        face_filename = face_filename_from_reference(reference, user_id)

        if not isinstance(record, dict) or not str(record.get("ID") or "").strip():
            lookup_values: List[str] = []
            if isinstance(record, dict):
                for key in ("UserID", "Name"):
                    candidate = str(record.get(key) or "").strip()
                    if candidate:
                        lookup_values.append(candidate)
            lookup_values.append(user_id)

            seen_lookup: set[str] = set()
            for lookup in lookup_values:
                clean_lookup = lookup.strip()
                if not clean_lookup or clean_lookup in seen_lookup:
                    continue
                seen_lookup.add(clean_lookup)
                try:
                    matches = await api.user_get(clean_lookup)
                except Exception as err:
                    _LOGGER.debug(
                        "Unable to fetch user.get for %s on %s while resolving numeric ID: %s",
                        clean_lookup,
                        device_name,
                        err,
                    )
                    continue
                for candidate in matches or []:
                    try:
                        if _record_matches_user(candidate, user_id):
                            record = candidate
                            break
                    except Exception:
                        continue
                if isinstance(record, dict) and str(record.get("ID") or "").strip():
                    break

        try:
            upload_result = await api.face_upload(
                face_bytes,
                filename=face_filename,
            )
        except Exception as err:
            _LOGGER.debug(
                "Direct face upload failed for %s on %s: %s", user_id, device_name, err
            )
            continue

        face_import_path = ""
        if isinstance(upload_result, dict):
            raw_path = upload_result.get("path")
            if isinstance(raw_path, str):
                face_import_path = raw_path.strip()
            if not face_import_path:
                raw_field = upload_result.get("raw")
                if isinstance(raw_field, str) and raw_field.strip():
                    face_import_path = raw_field.strip()
        elif isinstance(upload_result, str):
            face_import_path = upload_result.strip()

        face_link_reference = face_import_path or reference

        try:
            payload = _build_face_upload_payload(
                profile, record, user_id, face_link_reference
            )
        except Exception as err:
            _LOGGER.debug(
                "Failed to prepare face payload for %s on %s: %s",
                user_id,
                device_name,
                err,
            )
            continue

        existing_record = record if isinstance(record, dict) else None

        try:
            await manager._replace_user_on_device(
                api,
                payload,
                user_id,
                existing=existing_record,
            )
        except Exception as err:
            _LOGGER.debug(
                "Failed to recreate user %s on %s after face upload: %s",
                user_id,
                device_name,
                err,
            )

RESERVATION_TTL_MINUTES = 2
SIGNED_API_PATHS: Dict[str, str] = {
    "state": "/api/akuvox_ac/ui/state",
    "action": "/api/akuvox_ac/ui/action",
    "settings": "/api/akuvox_ac/ui/settings",
    "phones": "/api/akuvox_ac/ui/phones",
    "diagnostics": "/api/akuvox_ac/ui/diagnostics",
    "reserve_id": "/api/akuvox_ac/ui/reserve_id",
    "release_id": "/api/akuvox_ac/ui/release_id",
    "reservation_ping": "/api/akuvox_ac/ui/reservation_ping",
    "upload_face": "/api/akuvox_ac/ui/upload_face",
    "remote_enrol": "/api/akuvox_ac/ui/remote_enrol",
    "devices": "/api/akuvox_ac/ui/devices",
    "service_edit_user": "/api/services/akuvox_ac/edit_user",
}

_LOGGER = logging.getLogger(__name__)


CALL_LOG_LOOKBACK_SECONDS_DEFAULT = 60
CALL_LOG_LOOKBACK_MIN_SECONDS = 5
CALL_LOG_LOOKBACK_MAX_SECONDS = 600

_CALL_TYPE_MAP = {
    "0": "all",
    "1": "dialed",
    "2": "received",
    "3": "missed",
    "4": "forwarded",
    "5": "unknown",
    "all": "all",
    "dialed": "dialed",
    "dialled": "dialed",
    "received": "received",
    "incoming": "received",
    "incoming call": "received",
    "received call": "received",
    "missed": "missed",
    "missed call": "missed",
    "forwarded": "forwarded",
    "unknown": "unknown",
}

_PHONE_SPLIT_RE = re.compile(r"[,;/\\|\n\r]+")

INBOUND_CALL_LABEL_APPROVED_KEY_HOLDER = "Inbound call - access approved (key holder)"
INBOUND_CALL_LABEL_APPROVED = "Inbound call - access approved"
INBOUND_CALL_LABEL_DENIED_TEMPLATE = "Inbound call - access Denied ({number})"


def _json_clone(value: Any) -> Any:
    try:
        return json.loads(json.dumps(value))
    except Exception:
        return value


def _ingest_history_event(hass: HomeAssistant, event: Dict[str, Any]) -> None:
    if not isinstance(event, dict):
        return

    root = hass.data.get(DOMAIN, {}) or {}
    history = root.get("access_history")
    if not history or not hasattr(history, "ingest"):
        return

    settings = root.get("settings_store")
    try:
        limit = (
            settings.get_access_history_limit()
            if settings and hasattr(settings, "get_access_history_limit")
            else DEFAULT_ACCESS_HISTORY_LIMIT
        )
    except Exception:
        limit = DEFAULT_ACCESS_HISTORY_LIMIT

    event_copy = dict(event)

    timestamp = event_copy.get("timestamp") or event_copy.get("Time")
    if not timestamp:
        timestamp = dt.datetime.utcnow().isoformat() + "Z"
        event_copy.setdefault("timestamp", timestamp)

    event_copy["_t"] = AccessHistory._coerce_timestamp(event_copy.get("_t") or timestamp)

    key = AccessHistory._coerce_key(event_copy.get("_key"))
    if not key:
        parts = [
            event_copy.get("_source") or "call",
            event_copy.get("_device_id") or event_copy.get("entry_id") or "",
            event_copy.get("call_id") or event_copy.get("CallID") or event_copy.get("CallId") or "",
            event_copy.get("timestamp") or "",
        ]
        key = ":".join(str(part) for part in parts if part)
        if not key:
            key = f"call:{time.time()}"
    event_copy["_key"] = key

    if "_device" not in event_copy:
        event_copy["_device"] = event_copy.get("device_name") or ""
    if "_device_id" not in event_copy and event_copy.get("entry_id"):
        event_copy["_device_id"] = event_copy["entry_id"]

    if "_category" not in event_copy:
        event_copy["_category"] = categorize_event(event_copy)

    try:
        history.ingest([event_copy], limit)
    except Exception as err:
        _LOGGER.debug("Failed to ingest aggregated event: %s", err)


def _normalize_call_number(value: Any) -> str:
    if value in (None, ""):
        return ""

    text = str(value).strip()
    if not text:
        return ""

    lowered = text.lower()
    for prefix in ("sip:", "tel:", "callto:"):
        if lowered.startswith(prefix):
            text = text[len(prefix):]
            break

    if "@" in text:
        text = text.split("@", 1)[0]

    # Remove separators that commonly appear in numbers
    cleaned = text.replace(" ", "").replace("-", "").replace("(", "").replace(")", "")
    cleaned = cleaned.replace(".", "")

    if cleaned.startswith("+"):
        digits = "".join(ch for ch in cleaned if ch.isdigit())
        return f"+{digits}" if digits else ""

    digits = "".join(ch for ch in cleaned if ch.isdigit())
    if not digits:
        return ""

    if cleaned.startswith("00") and digits.startswith("00"):
        digits = digits[2:]
        return f"+{digits}" if digits else ""

    return digits


def _digits_only(value: str) -> str:
    return "".join(ch for ch in value if ch.isdigit())


def _numbers_equal(a: str, b: str) -> bool:
    if not a or not b:
        return False
    if a == b:
        return True

    stripped_a = a.lstrip("0")
    stripped_b = b.lstrip("0")
    if stripped_a and stripped_a == stripped_b:
        return True

    if len(a) >= 7 and len(b) >= 7:
        if a.endswith(b) or b.endswith(a):
            return True

    return False


def _profile_phone_values(profile: Dict[str, Any]) -> List[str]:
    raw = profile.get("phone")
    values: List[str] = []

    if isinstance(raw, str):
        parts = [part.strip() for part in _PHONE_SPLIT_RE.split(raw) if part and part.strip()]
        if parts:
            values.extend(parts)
        else:
            cleaned = raw.strip()
            if cleaned:
                values.append(cleaned)
    elif isinstance(raw, (list, tuple, set)):
        for item in raw:
            if item in (None, ""):
                continue
            values.append(str(item).strip())
    elif raw not in (None, ""):
        values.append(str(raw).strip())

    return values


def _build_phone_index(root: Dict[str, Any]) -> List[Dict[str, Any]]:
    store = root.get("users_store")
    if not store:
        return []

    try:
        users = store.all()
    except Exception:
        users = {}

    index: List[Dict[str, Any]] = []
    for ha_id, profile in (users or {}).items():
        if not isinstance(profile, dict):
            continue

        numbers: List[Dict[str, str]] = []
        for candidate in _profile_phone_values(profile):
            normalized = _normalize_call_number(candidate)
            digits = _digits_only(normalized)
            if not digits:
                continue
            numbers.append({
                "raw": candidate,
                "normalized": normalized,
                "digits": digits,
            })

        if not numbers:
            continue

        entry = {
            "ha_id": ha_id,
            "name": str(profile.get("name") or ha_id),
            "key_holder": bool(profile.get("key_holder")),
            "numbers": numbers,
            "profile": _json_clone(profile),
        }
        index.append(entry)

    return index


def _call_entry_id(entry: Dict[str, Any]) -> str:
    for key in ("ID", "Id", "id"):
        value = entry.get(key)
        if value not in (None, ""):
            return str(value)
    return ""


def _call_entry_number(entry: Dict[str, Any]) -> str:
    for key in (
        "Number",
        "number",
        "Num",
        "num",
        "CallNum",
        "Callnum",
        "CallNumber",
        "Phone",
        "RemoteNumber",
        "RemoteNum",
    ):
        value = entry.get(key)
        if value not in (None, ""):
            return str(value)

    fallback = entry.get("Name") or entry.get("RemoteDisplayName")
    if fallback:
        text = str(fallback)
        if any(ch.isdigit() for ch in text):
            return text

    return ""


def _call_entry_type(entry: Dict[str, Any]) -> str:
    for key in ("Type", "type", "CallType", "callType"):
        raw = entry.get(key)
        if raw in (None, ""):
            continue
        if isinstance(raw, (int, float)):
            lowered = str(int(raw))
        else:
            lowered = str(raw).strip().lower()
        if not lowered:
            continue
        mapped = _CALL_TYPE_MAP.get(lowered)
        if mapped:
            return mapped
        if "receive" in lowered or "incoming" in lowered or "inbound" in lowered:
            return "received"
        if "miss" in lowered:
            return "missed"
        if "dial" in lowered:
            return "dialed"
    return ""


def _call_entry_is_received(call_type: str) -> bool:
    if not call_type:
        return False
    lowered = call_type.lower()
    if lowered in ("received", "incoming"):
        return True
    return any(token in lowered for token in ("receive", "incoming", "inbound"))


def _parse_datetime_value(value: Any) -> Optional[dt.datetime]:
    if value in (None, ""):
        return None
    if isinstance(value, dt.datetime):
        return value
    if isinstance(value, (int, float)):
        try:
            return dt.datetime.fromtimestamp(float(value))
        except Exception:
            return None

    text = str(value).strip()
    if not text:
        return None

    try:
        cleaned = text.replace("Z", "+00:00")
        return dt.datetime.fromisoformat(cleaned)
    except Exception:
        pass

    for fmt in (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y/%m/%d %H:%M:%S",
        "%Y/%m/%d %H:%M",
        "%d/%m/%Y %H:%M:%S",
        "%d/%m/%Y %H:%M",
    ):
        try:
            return dt.datetime.strptime(text, fmt)
        except ValueError:
            continue

    return None


def _parse_date_value(value: Any) -> Optional[dt.date]:
    if value in (None, ""):
        return None
    if isinstance(value, dt.date) and not isinstance(value, dt.datetime):
        return value
    if isinstance(value, dt.datetime):
        return value.date()

    text = str(value).strip()
    if not text:
        return None

    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%d/%m/%Y"):
        try:
            return dt.datetime.strptime(text, fmt).date()
        except ValueError:
            continue

    return None


def _parse_time_value(value: Any) -> Optional[dt.time]:
    if value in (None, ""):
        return None
    if isinstance(value, dt.time):
        return value
    if isinstance(value, dt.datetime):
        return value.time()

    text = str(value).strip()
    if not text:
        return None

    for fmt in ("%H:%M:%S", "%H:%M"):
        try:
            return dt.datetime.strptime(text, fmt).time()
        except ValueError:
            continue

    return None


def _call_entry_timestamp(entry: Dict[str, Any]) -> Optional[dt.datetime]:
    date_raw = entry.get("Date") or entry.get("date")
    time_raw = entry.get("Time") or entry.get("time")

    if date_raw and time_raw:
        combined = _parse_datetime_value(f"{date_raw} {time_raw}")
        if combined:
            return combined

    for key in ("DateTime", "datetime", "DateTimeStr", "dateTime"):
        candidate = _parse_datetime_value(entry.get(key))
        if candidate:
            return candidate

    for key in ("Timestamp", "timestamp", "Ts", "ts"):
        candidate = _parse_datetime_value(entry.get(key))
        if candidate:
            return candidate

    if date_raw and time_raw:
        date_part = _parse_date_value(date_raw)
        time_part = _parse_time_value(time_raw)
        if date_part and time_part:
            return dt.datetime.combine(date_part, time_part)

    if date_raw:
        date_part = _parse_date_value(date_raw)
        if date_part:
            return dt.datetime.combine(date_part, dt.time.min)

    return None


def _match_user_by_number(digits: str, phone_index: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not digits:
        return None

    best: Optional[Dict[str, Any]] = None

    for entry in phone_index:
        numbers = entry.get("numbers") or []
        for candidate in numbers:
            candidate_digits = candidate.get("digits")
            if not candidate_digits:
                continue
            if _numbers_equal(digits, candidate_digits):
                result = {
                    "ha_id": entry.get("ha_id"),
                    "name": entry.get("name"),
                    "key_holder": bool(entry.get("key_holder")),
                    "number": candidate.get("normalized") or candidate_digits,
                    "profile": entry.get("profile"),
                }
                if result["key_holder"]:
                    return result
                if best is None:
                    best = result

    return best


async def _process_inbound_call_webhook(
    hass: HomeAssistant,
    *,
    number_hint: Optional[str] = None,
    lookback_seconds: Optional[Any] = None,
) -> Dict[str, Any]:
    root = hass.data.get(DOMAIN, {}) or {}

    try:
        lookback = int(lookback_seconds) if lookback_seconds is not None else CALL_LOG_LOOKBACK_SECONDS_DEFAULT
    except Exception:
        lookback = CALL_LOG_LOOKBACK_SECONDS_DEFAULT

    if lookback < CALL_LOG_LOOKBACK_MIN_SECONDS:
        lookback = CALL_LOG_LOOKBACK_MIN_SECONDS
    elif lookback > CALL_LOG_LOOKBACK_MAX_SECONDS:
        lookback = CALL_LOG_LOOKBACK_MAX_SECONDS

    phone_index = _build_phone_index(root)

    normalized_hint = _normalize_call_number(number_hint) if number_hint else ""
    hint_digits = _digits_only(normalized_hint)

    now_local = dt.datetime.now()
    now_utc = dt.datetime.now(dt.timezone.utc)

    candidates: List[Dict[str, Any]] = []

    for entry_id, data in root.items():
        if not isinstance(data, dict):
            continue

        api = data.get("api")
        coord = data.get("coordinator")
        if not api or not coord:
            continue

        device_type = str((getattr(coord, "health", {}) or {}).get("device_type") or "").strip().lower()
        if device_type and device_type != "intercom":
            continue

        try:
            log_items = await api.call_log()
        except Exception as err:
            _LOGGER.debug("Failed to fetch call log for %s: %s", entry_id, err)
            continue

        if isinstance(log_items, dict):
            log_items = [log_items]
        if not isinstance(log_items, list):
            continue

        device_name = getattr(coord, "device_name", entry_id)

        for raw in log_items:
            if not isinstance(raw, dict):
                continue

            call_type = _call_entry_type(raw)
            if not _call_entry_is_received(call_type or ""):
                continue

            timestamp = _call_entry_timestamp(raw)
            if not timestamp:
                continue

            if timestamp.tzinfo is None:
                age_seconds = (now_local - timestamp).total_seconds()
            else:
                age_seconds = (now_utc - timestamp.astimezone(dt.timezone.utc)).total_seconds()

            if age_seconds < 0 or age_seconds > lookback:
                continue

            raw_number = _call_entry_number(raw)
            normalized_number = _normalize_call_number(raw_number)
            digits = _digits_only(normalized_number)

            candidate = {
                "entry_id": entry_id,
                "device_name": device_name,
                "call_id": _call_entry_id(raw),
                "timestamp": timestamp,
                "age_seconds": round(age_seconds, 2),
                "raw": _json_clone(raw),
                "call_type": call_type or "received",
                "raw_number": raw_number or "",
                "number": normalized_number,
                "digits": digits,
            }
            candidates.append(candidate)

    if not candidates:
        error = "no recent received calls"
        if number_hint:
            error = f"no recent received calls for {number_hint}"
        return {"ok": False, "error": error, "lookback_seconds": lookback}

    if hint_digits:
        filtered = [c for c in candidates if _numbers_equal(c.get("digits", ""), hint_digits)]
        if filtered:
            candidates = filtered
        else:
            return {
                "ok": False,
                "error": f"no recent received calls matching {normalized_hint or number_hint}",
                "lookback_seconds": lookback,
            }

    candidates.sort(key=lambda item: item.get("timestamp"), reverse=True)
    best = candidates[0]

    match = _match_user_by_number(best.get("digits", ""), phone_index)

    base_number = best.get("number") or best.get("raw_number") or normalized_hint or number_hint
    display_number = match.get("number") if match and match.get("number") else base_number

    if match and match.get("key_holder"):
        result = INBOUND_CALL_RESULT_APPROVED_KEY_HOLDER
        status_label = INBOUND_CALL_LABEL_APPROVED_KEY_HOLDER
    elif match:
        result = INBOUND_CALL_RESULT_APPROVED
        status_label = INBOUND_CALL_LABEL_APPROVED
    else:
        result = INBOUND_CALL_RESULT_DENIED
        denied_display = display_number or "Unknown"
        status_label = INBOUND_CALL_LABEL_DENIED_TEMPLATE.format(number=denied_display)
        display_number = denied_display

    event_payload: Dict[str, Any] = {
        "entry_id": best.get("entry_id"),
        "device_name": best.get("device_name"),
        "call_id": best.get("call_id"),
        "timestamp": best.get("timestamp").isoformat() if best.get("timestamp") else None,
        "call_type": best.get("call_type"),
        "number": display_number or "",
        "raw_number": best.get("raw_number"),
        "digits": best.get("digits"),
        "status": result,
        "status_label": status_label,
        "lookback_seconds": lookback,
        "call": best.get("raw"),
    }

    if match:
        event_payload.update(
            {
                "user_id": match.get("ha_id"),
                "user_name": match.get("name"),
                "key_holder": bool(match.get("key_holder")),
                "user_number": match.get("number"),
                "user_profile": match.get("profile"),
            }
        )
    else:
        event_payload.update({"user_id": None, "user_name": None, "key_holder": False})

    history_event = {
        "timestamp": event_payload.get("timestamp"),
        "Event": status_label,
        "Result": status_label,
        "entry_id": event_payload.get("entry_id"),
        "device_name": event_payload.get("device_name"),
        "call_id": event_payload.get("call_id"),
        "call_type": event_payload.get("call_type"),
        "CallNumber": event_payload.get("number"),
        "raw_number": event_payload.get("raw_number"),
        "digits": event_payload.get("digits"),
        "status": result,
        "status_label": status_label,
        "_source": "inbound_call",
    }
    if event_payload.get("user_name"):
        history_event["User"] = event_payload.get("user_name")
    if event_payload.get("user_id"):
        history_event["UserID"] = event_payload.get("user_id")
    if event_payload.get("key_holder") is not None:
        history_event["key_holder"] = bool(event_payload.get("key_holder"))

    _ingest_history_event(hass, history_event)

    hass.bus.async_fire(EVENT_INBOUND_CALL, event_payload)

    _LOGGER.debug(
        "Inbound call webhook result=%s device=%s number=%s user=%s key_holder=%s",
        result,
        best.get("device_name"),
        display_number or "",
        event_payload.get("user_id"),
        event_payload.get("key_holder"),
    )

    response: Dict[str, Any] = {
        "ok": True,
        "result": result,
        "status_label": status_label,
        "entry_id": best.get("entry_id"),
        "device_name": best.get("device_name"),
        "call_id": best.get("call_id"),
        "timestamp": event_payload.get("timestamp"),
        "call_type": best.get("call_type"),
        "number": display_number or "",
        "raw_number": best.get("raw_number"),
        "digits": best.get("digits"),
        "age_seconds": best.get("age_seconds"),
        "lookback_seconds": lookback,
        "call": best.get("raw"),
        "event": event_payload,
    }

    if match:
        response["user"] = {
            "id": match.get("ha_id"),
            "name": match.get("name"),
            "key_holder": bool(match.get("key_holder")),
            "number": match.get("number"),
        }
    else:
        response["user"] = None

    return response


class AkuvoxInboundCallWebhook(HomeAssistantView):
    url = "/api/akuvox_ac/webhook/inbound_call"
    name = "api:akuvox_ac:webhook_inbound_call"
    requires_auth = False

    async def _handle(self, request: web.Request):
        hass: HomeAssistant = request.app["hass"]

        payload: Dict[str, Any] = {}
        if request.can_read_body:
            try:
                payload = await request.json()
            except Exception:
                payload = {}

        number_hint = request.query.get("number") or request.query.get("phone")
        if not number_hint and isinstance(payload, dict):
            number_hint = payload.get("number") or payload.get("phone")

        lookback = (
            request.query.get("within")
            or request.query.get("lookback")
            or request.query.get("within_seconds")
        )
        if lookback is None and isinstance(payload, dict):
            lookback = (
                payload.get("within")
                or payload.get("lookback")
                or payload.get("within_seconds")
            )

        result = await _process_inbound_call_webhook(
            hass,
            number_hint=number_hint,
            lookback_seconds=lookback,
        )

        status = 200 if result.get("ok") else 200
        return web.json_response(result, status=status)

    async def get(self, request: web.Request):
        return await self._handle(request)

    async def post(self, request: web.Request):
        return await self._handle(request)


def _persistent_face_dir(hass: HomeAssistant) -> Path:
    """Compatibility shim for callers expecting the persistent face directory."""

    return face_storage_dir(hass)


def _legacy_face_candidate(hass: HomeAssistant, relative: str) -> Optional[Path]:
    """Return a resolved legacy face path for migration, if it exists."""

    try:
        legacy_root = _legacy_face_dir(hass)
        candidate = (legacy_root / relative).resolve()
        candidate.relative_to(legacy_root)
    except Exception:
        return None
    return candidate

DASHBOARD_ROUTES: Dict[str, str] = {
    "head": "head",
    "head.html": "head",
    "head-mob": "head-mob",
    "head-mob.html": "head-mob",
    "device-edit": "device_edit",
    "device_edit": "device_edit",
    "device_edit.html": "device_edit",
    "device-edit-mob": "device_edit-mob",
    "device_edit-mob": "device_edit-mob",
    "device_edit-mob.html": "device_edit-mob",
    "device-management": "device_management",
    "device_management": "device_management",
    "device_management.html": "device_management",
    "device-management-mob": "device_management-mob",
    "device_management-mob": "device_management-mob",
    "device_management-mob.html": "device_management-mob",
    "face-rec": "face_rec",
    "face_rec": "face_rec",
    "face_rec.html": "face_rec",
    "face-rec-mob": "face_rec-mob",
    "face_rec-mob": "face_rec-mob",
    "face_rec-mob.html": "face_rec-mob",
    "index": "index",
    "index.html": "index",
    "index-mob": "index-mob",
    "index-mob.html": "index-mob",
    "event-history": "event_history",
    "event_history": "event_history",
    "event_history.html": "event_history",
    "event-history-mob": "event_history-mob",
    "event_history-mob": "event_history-mob",
    "event_history-mob.html": "event_history-mob",
    "schedules": "schedules",
    "schedules.html": "schedules",
    "schedules-mob": "schedules-mob",
    "schedules-mob.html": "schedules-mob",
    "users": "users",
    "users.html": "users",
    "users-mob": "users-mob",
    "users-mob.html": "users-mob",
    "user-overview": "user_overview",
    "user_overview": "user_overview",
    "user_overview.html": "user_overview",
    "user-overview-mob": "user_overview-mob",
    "user_overview-mob": "user_overview-mob",
    "user_overview-mob.html": "user_overview-mob",
    "temp-user": "temp_user",
    "temp_user": "temp_user",
    "temp_user.html": "temp_user",
    "temp-user-mob": "temp_user-mob",
    "temp_user-mob": "temp_user-mob",
    "temp_user-mob.html": "temp_user-mob",
    "settings": "settings",
    "settings.html": "settings",
    "settings-mob": "settings-mob",
    "settings-mob.html": "settings-mob",
    "diagnostics": "diagnostics",
    "diagnostics.html": "diagnostics",
    "diagnostics-mob": "diagnostics-mob",
    "diagnostics-mob.html": "diagnostics-mob",
    "unauthorized": "unauthorized",
    "unauthorized.html": "unauthorized",
    "unauthorized-mob": "unauthorized-mob",
    "unauthorized-mob.html": "unauthorized-mob",
}


def _query_mobile_override(query: Mapping[str, str]) -> Optional[bool]:
    """Return an explicit mobile preference from the request query if present."""

    preference_keys = ("variant", "layout")
    for key in preference_keys:
        try:
            raw = query.get(key)  # type: ignore[arg-type]
        except Exception:
            raw = None
        if not raw:
            continue
        lowered = str(raw).strip().lower()
        if lowered in {"mobile", "mob", "phone", "narrow"}:
            return True
        if lowered in {"desktop", "web", "full", "wide"}:
            return False

    boolish_keys = ("mobile", "is_mobile", "mobile_view")
    for key in boolish_keys:
        if key not in query:  # type: ignore[operator]
            continue
        try:
            raw = query.get(key)  # type: ignore[arg-type]
        except Exception:
            raw = None
        if raw is None:
            return True
        lowered = str(raw).strip().lower()
        if lowered in {"", "1", "true", "t", "yes", "y", "on"}:
            return True
        if lowered in {"0", "false", "f", "no", "n", "off"}:
            return False
        return bool(lowered)

    return None


def _request_prefers_mobile(request: Optional[web.Request]) -> bool:
    """Determine whether the current HTTP request prefers the mobile dashboard."""

    if request is None:
        return False

    try:
        query = request.rel_url.query  # type: ignore[attr-defined]
    except Exception:
        query = None

    if query is not None:
        override = _query_mobile_override(query)
        if override is not None:
            return override

    try:
        user_agent = request.headers.get("User-Agent", "")
    except Exception:
        user_agent = ""

    lowered = user_agent.lower()
    if not lowered:
        return False

    if "mobi" in lowered or "iphone" in lowered or "ipod" in lowered:
        return True
    if "ipad" in lowered or "tablet" in lowered:
        return True
    if "android" in lowered and "windows" not in lowered:
        if "tv" in lowered and "mobile" not in lowered and "tablet" not in lowered:
            pass
        else:
            return True
    if "iemobile" in lowered or "windows phone" in lowered:
        return True
    if "opera mini" in lowered:
        return True

    return False


def _resolve_dashboard_asset(name: str, request: Optional[web.Request]) -> Path:
    """Return the concrete asset path for the requested dashboard slug."""

    base = (name or "").strip()
    if not base:
        raise web.HTTPNotFound()

    if base.endswith(".html"):
        base = base[:-5]

    explicit_mobile = base.endswith("-mob")
    prefer_mobile = _request_prefers_mobile(request) if not explicit_mobile else True
    candidates: List[str] = []

    if explicit_mobile:
        candidates.append(f"{base}.html")
        if base[:-4]:
            candidates.append(f"{base[:-4]}.html")
    elif prefer_mobile:
        candidates.extend([f"{base}-mob.html", f"{base}.html"])
    else:
        candidates.extend([f"{base}.html", f"{base}-mob.html"])

    last_error: Optional[Exception] = None
    for candidate in candidates:
        try:
            return _static_asset(candidate)
        except web.HTTPNotFound as err:
            last_error = err
            continue

    if last_error:
        raise last_error

    raise web.HTTPNotFound()


def face_base_url(hass: HomeAssistant, request: Optional[web.Request] = None) -> str:
    """Return the absolute base URL that serves face images."""

    candidates: List[str] = []

    internal_cfg = getattr(hass.config, "internal_url", None)
    if internal_cfg:
        candidates.append(str(internal_cfg))

    try:
        internal_guess = get_url(
            hass,
            allow_internal=True,
            allow_external=False,
            allow_cloud=False,
            prefer_external=False,
        )
        if internal_guess:
            candidates.append(str(internal_guess))
    except Exception:
        pass

    if request is not None:
        try:
            origin = str(request.url.origin())
        except Exception:
            origin = ""
        if origin:
            candidates.append(origin)

    try:
        fallback = get_url(hass, prefer_external=False)
        if fallback:
            candidates.append(str(fallback))
    except Exception:
        pass

    external_cfg = getattr(hass.config, "external_url", None)
    if external_cfg:
        candidates.append(str(external_cfg))

    for base in candidates:
        cleaned = str(base or "").strip().rstrip("/")
        if cleaned:
            return f"{cleaned}{FACE_DATA_PATH}"

    return FACE_DATA_PATH


def face_filename_from_reference(reference: str, user_id: str, default_ext: str = "jpg") -> str:
    """Return a sanitised filename for a face image reference or fallback to <user_id>.ext."""

    candidate = str(reference or "").strip()
    if candidate:
        try:
            parsed = urlsplit(candidate)
            extracted = parsed.path or candidate
        except Exception:
            extracted = candidate
        extracted = unquote(extracted)
        try:
            name = Path(extracted).name
        except Exception:
            name = ""
        cleaned = str(name or "").strip()
        if cleaned:
            suffix = Path(cleaned).suffix
            if not suffix and default_ext:
                cleaned = f"{cleaned}.{default_ext}"
            return cleaned

    fallback = str(user_id or "").strip() or "face"
    if default_ext:
        suffix = Path(fallback).suffix
        if not suffix:
            return f"{fallback}.{default_ext}"
    return fallback


def _static_asset(path: str) -> Path:
    clean = path.strip()
    if not clean or clean.endswith("/"):
        clean = (clean.rstrip("/") + "/index.html") if clean else "index.html"
    candidate = (STATIC_ROOT / clean.lstrip("/")).resolve()
    try:
        candidate.relative_to(STATIC_ROOT.resolve())
    except ValueError:
        raise web.HTTPForbidden()
    if not candidate.is_file():
        raise web.HTTPNotFound()
    return candidate


def _signed_paths_for_request(
    hass: HomeAssistant, request: web.Request
) -> Dict[str, str]:
    refresh_id = request.get(KEY_HASS_REFRESH_TOKEN_ID)
    if not refresh_id:
        return {}

    signed: Dict[str, str] = {}

    for key, path in SIGNED_API_PATHS.items():
        try:
            signed[key] = async_sign_path(
                hass,
                path,
                dt.timedelta(minutes=10),
                refresh_token_id=refresh_id,
            )
        except Exception as err:  # pragma: no cover - best effort
            _LOGGER.debug("Failed to sign %s for Akuvox UI: %s", path, err)

    return signed


def _inject_signed_paths(html: str, signed: Dict[str, str]) -> str:
    if not signed:
        return html

    try:
        payload = json.dumps(signed)
    except Exception:  # pragma: no cover - shouldn't happen
        return html

    script = (
        "<script>"
        "(function(){"
        "  try {"
        "    const incoming = Object.freeze(%s);"
        "    const target = Object.assign({}, window.AK_AC_SIGNED_PATHS || {}, incoming);"
        "    window.AK_AC_SIGNED_PATHS = target;"
        "    try { sessionStorage.setItem('akuvox_signed_paths', JSON.stringify(target)); } catch (err) {}"
        "    try { localStorage.setItem('akuvox_signed_paths', JSON.stringify(target)); } catch (err) {}"
        "  } catch (err) {}"
        "})();"
        "</script>"
    ) % payload

    lowered = html.lower()
    head_index = lowered.find("</head>")
    if head_index != -1:
        return html[:head_index] + script + html[head_index:]

    body_index = lowered.find("<body")
    if body_index != -1:
        insert_at = lowered.find(">", body_index)
        if insert_at != -1:
            insert_at += 1
            return html[:insert_at] + script + html[insert_at:]

    return script + html


def _request_device_id(
    hass: HomeAssistant, request: Optional[web.Request]
) -> Optional[str]:
    if request is None:
        return None
    refresh_id = request.get(KEY_HASS_REFRESH_TOKEN_ID)
    if not refresh_id:
        return None
    try:
        token = hass.auth.async_get_refresh_token(refresh_id)
    except Exception:
        token = None
    device_id = getattr(token, "device_id", None) if token else None
    if not device_id:
        return None
    return str(device_id)


def _request_user_id(
    hass: HomeAssistant, request: Optional[web.Request]
) -> Optional[str]:
    if request is None:
        return None
    user = request.get(KEY_HASS_USER)
    if user is not None:
        user_id = getattr(user, "id", None)
        return str(user_id) if user_id else None
    refresh_id = request.get(KEY_HASS_REFRESH_TOKEN_ID)
    if not refresh_id:
        return None
    try:
        token = hass.auth.async_get_refresh_token(refresh_id)
    except Exception:
        token = None
    user_id = getattr(token, "user_id", None) if token else None
    if not user_id:
        return None
    return str(user_id)


def _request_user_is_admin(
    hass: HomeAssistant, request: Optional[web.Request]
) -> bool:
    if request is None:
        return False
    user = request.get(KEY_HASS_USER)
    if user is not None:
        return bool(getattr(user, "is_admin", False))
    refresh_id = request.get(KEY_HASS_REFRESH_TOKEN_ID)
    if not refresh_id:
        return False
    try:
        token = hass.auth.async_get_refresh_token(refresh_id)
    except Exception:
        return False
    if not token:
        return False
    user_id = getattr(token, "user_id", None)
    if not user_id:
        return False
    try:
        stored_user = hass.auth.async_get_user(user_id)
    except Exception:
        return False
    return bool(getattr(stored_user, "is_admin", False))


def _dashboard_access_allowed(
    hass: HomeAssistant, request: Optional[web.Request]
) -> bool:
    if request is None:
        return False
    if _request_user_is_admin(hass, request):
        return True
    root = hass.data.get(DOMAIN, {}) or {}
    settings = root.get("settings_store")
    if not settings:
        return True
    allowed_users = (
        settings.get_dashboard_user_ids()
        if hasattr(settings, "get_dashboard_user_ids")
        else []
    )
    if allowed_users:
        user_id = _request_user_id(hass, request)
        if not user_id:
            return False
        return user_id in allowed_users

    allowed_devices = (
        settings.get_dashboard_device_ids()
        if hasattr(settings, "get_dashboard_device_ids")
        else []
    )
    if allowed_devices:
        device_id = _request_device_id(hass, request)
        if not device_id:
            return False
        return device_id in allowed_devices
    return False


def _only_hhmm(v: Optional[str]) -> str:
    if not v or v == "—":
        return "—"
    try:
        value = v.strip()
        if (
            len(value) >= 5
            and value[2] == ":"
            and value[:2].isdigit()
            and value[3:5].isdigit()
        ):
            return value[:5]
        return value
    except Exception:
        return str(v)


def _is_ha_id(value: Any) -> bool:
    return is_ha_id(value)


def _ha_id_from_int(n: int) -> str:
    return ha_id_from_int(n)


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


def _face_flag_from_record(record: Mapping[str, Any]) -> Optional[bool]:
    if not isinstance(record, Mapping):
        return None
    for key in _FACE_FLAG_KEYS:
        if key not in record:
            continue
        flag = _normalize_boolish(record.get(key))
        if flag is not None:
            return flag
    return None


def _user_key(record: Mapping[str, Any]) -> str:
    return str(
        record.get("UserID")
        or record.get("UserId")
        or record.get("ID")
        or record.get("Name")
        or record.get("user_id")
        or ""
    )


def _group_tokens(values: Any) -> set[str]:
    tokens: set[str] = set()
    if isinstance(values, (list, tuple, set)):
        iterable = values
    elif values in (None, ""):
        iterable = []
    else:
        iterable = [values]
    for item in iterable:
        try:
            text = str(item).strip()
        except Exception:
            continue
        if not text:
            continue
        tokens.add(text.lower())
    if not tokens:
        tokens.add("default")
    return tokens


def _groups_overlap(user_groups: Any, device_groups: Any) -> bool:
    return bool(_group_tokens(user_groups) & _group_tokens(device_groups))


def _device_supports_face(record: Mapping[str, Any]) -> bool:
    device_type = str(
        record.get("type")
        or record.get("device_type")
        or ""
    ).strip().lower()
    if not device_type:
        return True
    return device_type == "intercom"


def _device_face_is_active(record: Mapping[str, Any]) -> bool:
    flag = _face_flag_from_record(record)
    if flag is not None:
        return bool(flag)

    url = str(
        record.get("FaceUrl")
        or record.get("FaceURL")
        or record.get("face_url")
        or ""
    ).strip()
    if not url:
        return False

    status = str(
        record.get("face_status")
        or record.get("FaceStatus")
        or record.get("status")
        or record.get("Status")
        or ""
    ).strip().lower()
    if status in ("pending", "0", "false", "inactive", "waiting"):
        return False

    return True


def _evaluate_face_status(
    hass: HomeAssistant,
    user: Mapping[str, Any],
    devices: List[Dict[str, Any]],
    stored_status: str,
) -> str:
    user_id = str(user.get("id") or "").strip()
    if not user_id:
        return "none"

    has_face_asset = _face_image_exists(hass, user_id)
    wants_face = stored_status in {"pending", "active"} or has_face_asset
    if not wants_face:
        return "none"

    if stored_status == "active":
        return "active"

    user_groups = user.get("groups") or []
    relevant_devices = [
        dev
        for dev in devices
        if dev.get("participate_in_sync", True)
        and _device_supports_face(dev)
        and _groups_overlap(user_groups, dev.get("sync_groups"))
    ]

    if not relevant_devices:
        return "active"

    for dev in relevant_devices:
        if not dev.get("online", True):
            return "pending"
        sync_state = str(dev.get("sync_status") or "").strip().lower()
        record = None
        for candidate in dev.get("_users") or dev.get("users") or []:
            if _user_key(candidate) == user_id:
                record = candidate
                break
        if record is None:
            return "pending"
        face_active = _device_face_is_active(record)
        if not face_active:
            return "pending"
        if sync_state != "in_sync" and stored_status == "pending":
            return "pending"

    return "active"


async def _refresh_face_statuses(
    hass: HomeAssistant,
    users_store,
    registry_users: List[Dict[str, Any]],
    devices: List[Dict[str, Any]],
    profiles: Mapping[str, Any],
) -> None:
    if not registry_users:
        return

    profile_lookup = profiles or {}
    for entry in registry_users:
        user_id = normalize_user_id(entry.get("id"))
        if not user_id:
            continue
        entry["id"] = user_id

        stored = profile_lookup.get(user_id) or {}
        stored_status = str(stored.get("face_status") or "").strip().lower()
        stored_synced_at = stored.get("face_synced_at")

        desired_status = _evaluate_face_status(hass, entry, devices, stored_status)

        if desired_status == "active":
            if stored_status != "active" or not stored_synced_at:
                desired_synced_at = _now_iso()
            else:
                desired_synced_at = stored_synced_at
        else:
            desired_synced_at = None

        entry["face_status"] = desired_status if desired_status != "none" else ""
        entry["face_active"] = desired_status == "active"
        if desired_synced_at:
            entry["face_synced_at"] = desired_synced_at
        else:
            entry.pop("face_synced_at", None)

        if not users_store:
            continue

        status_for_store = desired_status if desired_status in {"pending", "active"} else ""
        stored_status_norm = stored_status if stored_status else ""
        stored_errors = stored.get("face_error_count")
        clear_errors = bool(stored_errors) and desired_status == "active"

        if (
            status_for_store != stored_status_norm
            or desired_synced_at != stored_synced_at
            or clear_errors
        ):
            try:
                await users_store.upsert_profile(
                    user_id,
                    face_status=status_for_store,
                    face_synced_at=desired_synced_at or "",
                    face_error_count=0 if clear_errors else None,
                )
                updated = dict(stored)
                if status_for_store:
                    updated["face_status"] = status_for_store
                else:
                    updated.pop("face_status", None)
                if desired_synced_at:
                    updated["face_synced_at"] = desired_synced_at
                else:
                    updated.pop("face_synced_at", None)
                if clear_errors:
                    updated.pop("face_error_count", None)
                profile_lookup[user_id] = updated
            except Exception:
                pass
def _context_user_name(hass: HomeAssistant, context) -> str:
    """Return a friendly name for the user behind an HTTP/service call."""

    default = "HA User"
    if context is None:
        return default

    user_id = getattr(context, "user_id", None)
    if not user_id:
        return default

    try:
        user = hass.auth.async_get_user(user_id)
        if user:
            if user.name:
                return user.name
            if user.id:
                return user.id
    except Exception:
        return default

    return default


def _flag_rebooting(coord, *, triggered_by: str, duration: float = 300.0) -> None:
    """Mirror the reboot status tracking used by the service helper."""

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


async def _reboot_devices_direct(
    root: Dict[str, Any], *, entry_id: Optional[str], triggered_by: str
) -> bool:
    """Best-effort reboot helper used when the HA service call fails."""

    manager = root.get("sync_manager")
    targets: List[Any] = []

    if entry_id:
        data = root.get(entry_id) or {}
        coord = data.get("coordinator")
        api = data.get("api")
        if coord and api:
            targets.append((coord, api))
        else:
            raise RuntimeError("Device not available for reboot")
    elif manager:
        for _entry_id, coord, api, *_ in manager._devices():
            if coord and api:
                targets.append((coord, api))

    if not targets:
        raise RuntimeError("No devices available to reboot")

    success = False

    for coord, api in targets:
        try:
            await api.system_reboot()
        except Exception as err:
            try:
                coord._append_event(f"Reboot failed: {err}")  # type: ignore[attr-defined]
            except Exception:
                pass
            continue

        success = True
        _flag_rebooting(coord, triggered_by=triggered_by)

        try:
            await coord.async_request_refresh()
        except Exception:
            pass

    if not success:
        raise RuntimeError("Failed to trigger reboot")

    return True


def _normalize_groups(groups: Any) -> List[str]:
    """Return a JSON-serialisable list of group names."""

    if isinstance(groups, list):
        return [str(g) for g in groups]
    if isinstance(groups, (set, tuple)):
        return [str(g) for g in groups]
    if groups in (None, ""):
        return []
    return [str(groups)]


def _best_name(coord, entry_bucket: Dict[str, Any]) -> str:
    # Try a few places for a friendly, stable device name
    for key in ("device_name", "friendly_name", "name"):
        n = getattr(coord, key, None)
        if isinstance(n, str) and n.strip():
            return n.strip()
    try:
        n = coord.health.get("name") or coord.health.get("device_name")
        if isinstance(n, str) and n.strip():
            return n.strip()
    except Exception:
        pass
    try:
        opts = entry_bucket.get("options") or {}
        n = opts.get("device_name") or opts.get("name")
        if isinstance(n, str) and n.strip():
            return n.strip()
    except Exception:
        pass
    try:
        return str(getattr(coord, "name", "")) or "Akuvox Device"
    except Exception:
        return "Akuvox Device"


def _now_iso() -> str:
    return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def _profile_is_empty_reserved(p: Dict[str, Any]) -> bool:
    """True if this looks like a reservation that was never completed."""
    if not isinstance(p, dict):
        return False
    if str(p.get("status", "")).lower() not in ("pending", "reserved"):
        return False
    # no user content
    has_core = any(bool(p.get(k)) for k in ("name", "pin", "phone"))
    if has_core:
        return False
    return True


def _cleanup_stale_reservations(hass: HomeAssistant, max_age_minutes: int = 120) -> int:
    """
    Remove pending empty reservations older than max_age_minutes.
    Returns count removed.
    """
    root = hass.data.get(DOMAIN, {}) or {}
    us = root.get("users_store")
    if not us:
        return 0
    try:
        allu = us.all() or {}
    except Exception:
        return 0

    cutoff = dt.datetime.utcnow() - dt.timedelta(minutes=max_age_minutes)
    to_del: List[str] = []
    for key, prof in allu.items():
        if not _is_ha_id(key):
            continue
        if not _profile_is_empty_reserved(prof):
            continue
        ts = str(prof.get("reserved_at") or "")
        too_old = True
        try:
            if ts:
                tsv = ts.rstrip("Z")
                stamp = dt.datetime.fromisoformat(tsv)
                too_old = stamp < cutoff
        except Exception:
            pass
        if too_old:
            to_del.append(key)

    removed = 0
    for k in to_del:
        try:
            us.data.get("users", {}).pop(k, None)  # type: ignore[attr-defined]
            removed += 1
        except Exception:
            pass
    if removed:
        try:
            hass.async_create_task(us.async_save())  # type: ignore[attr-defined]
        except Exception:
            pass
    return removed


def _parse_reserved_at(value: Any) -> Optional[dt.datetime]:
    if isinstance(value, str) and value:
        try:
            return dt.datetime.fromisoformat(value.rstrip("Z"))
        except Exception:
            return None
    return None


def _select_reusable_reservation(users: Mapping[str, Any]) -> Optional[tuple[str, str]]:
    """Return ``(canonical_id, store_key)`` for the freshest empty reservation."""

    if not isinstance(users, Mapping):
        return None

    best: Optional[tuple[str, str]] = None
    best_stamp: Optional[dt.datetime] = None

    for store_key, profile in users.items():
        if not _profile_is_empty_reserved(profile):
            continue
        canonical = normalize_user_id(store_key)
        if not canonical:
            continue
        stamp: Optional[dt.datetime] = None
        if isinstance(profile, Mapping):
            stamp = _parse_reserved_at(profile.get("reserved_at"))
        if best is None:
            best = (canonical, store_key)
            best_stamp = stamp
            continue
        if stamp and (best_stamp is None or stamp > best_stamp):
            best = (canonical, store_key)
            best_stamp = stamp
            continue
        if best_stamp is None and stamp is None and canonical < best[0]:
            best = (canonical, store_key)

    return best


def _prune_inactive_reservations(
    users: Dict[str, Any],
    *,
    keep_key: Optional[str] = None,
    max_age_minutes: int = RESERVATION_TTL_MINUTES,
) -> bool:
    """Remove abandoned empty reservations.

    Returns True if any entries were removed.
    """

    if not isinstance(users, dict):
        return False

    cutoff = dt.datetime.utcnow() - dt.timedelta(minutes=max_age_minutes)
    changed = False

    for store_key in list(users.keys()):
        if keep_key is not None and store_key == keep_key:
            continue
        profile = users.get(store_key)
        if not _profile_is_empty_reserved(profile):
            continue
        stamp: Optional[dt.datetime] = None
        if isinstance(profile, Mapping):
            stamp = _parse_reserved_at(profile.get("reserved_at"))
        if stamp and stamp >= cutoff:
            continue
        users.pop(store_key, None)
        changed = True

    return changed


_SPECIAL_DEVICE_KEYS = {
    "groups_store",
    "users_store",
    "schedules_store",
    "settings_store",
    "sync_manager",
    "sync_queue",
    "_ui_registered",
    "_panel_registered",
}


def _iter_device_buckets(root: Dict[str, Any]):
    for entry_id, data in list((root or {}).items()):
        if entry_id in _SPECIAL_DEVICE_KEYS:
            continue
        if not isinstance(data, dict):
            continue
        coord = data.get("coordinator")
        if not coord:
            continue
        opts = data.get("options")
        if not isinstance(opts, dict):
            opts = {}
            data["options"] = opts
        yield entry_id, data, coord, opts


async def _fetch_device_schedule_ids(root: Dict[str, Any]) -> Dict[str, str]:
    schedule_ids: Dict[str, str] = {
        "24/7 Access": "1001",
        "No Access": "1002",
    }
    for _, data, _, _ in _iter_device_buckets(root):
        api = data.get("api")
        if not api:
            continue
        try:
            items = await api.schedule_get()
        except Exception:
            continue
        if not isinstance(items, list):
            continue
        matched = False
        for item in items:
            if not isinstance(item, dict):
                continue
            name = str(item.get("Name") or "").strip()
            schedule_id = str(
                item.get("ScheduleID")
                or item.get("ScheduleId")
                or item.get("ID")
                or ""
            ).strip()
            if name and schedule_id:
                schedule_ids[name] = schedule_id
                matched = True
        if matched:
            break
    return schedule_ids


def _normalize_user_match_value(value: Any) -> str:
    if value is None:
        return ""
    try:
        text = str(value).strip()
    except Exception:
        return ""
    return text


def _build_user_match_index(users: Dict[str, Any]) -> Dict[str, str]:
    index: Dict[str, str] = {}
    if not isinstance(users, dict):
        return index

    def _add(value: Any, canonical: str) -> None:
        text = _normalize_user_match_value(value)
        if not text:
            return
        index.setdefault(text.lower(), canonical)
        normalized = normalize_user_id(text)
        if normalized:
            index.setdefault(normalized.lower(), canonical)

    for key, profile in users.items():
        canonical = normalize_user_id(key) or _normalize_user_match_value(key)
        if not canonical:
            continue
        _add(canonical, canonical)
        if isinstance(profile, Mapping):
            _add(profile.get("name"), canonical)
            _add(profile.get("device_id"), canonical)
            _add(profile.get("UserID"), canonical)
            _add(profile.get("UserId"), canonical)
            _add(profile.get("ID"), canonical)
            _add(profile.get("card_code"), canonical)
            _add(profile.get("CardCode"), canonical)

    return index


def _merge_last_access(root: Dict[str, Any], users: Dict[str, Any]) -> Dict[str, str]:
    match_index = _build_user_match_index(users)
    merged: Dict[str, str] = {}

    for _entry_id, _bucket, coord, _opts in _iter_device_buckets(root):
        storage = getattr(coord, "storage", None)
        data = getattr(storage, "data", {}) if storage else {}
        last_access = data.get("last_access")
        if not isinstance(last_access, dict):
            continue
        for raw_id, timestamp in last_access.items():
            raw_text = _normalize_user_match_value(raw_id)
            if not raw_text:
                continue
            match_id = None
            candidates = (normalize_user_id(raw_text), raw_text)
            for candidate in candidates:
                if not candidate:
                    continue
                match_id = match_index.get(candidate.lower())
                if match_id:
                    break
            if not match_id:
                continue
            timestamp_text = _normalize_user_match_value(timestamp)
            if not timestamp_text:
                continue
            current = merged.get(match_id)
            current_epoch = AccessHistory._coerce_timestamp(current) if current else 0.0
            candidate_epoch = AccessHistory._coerce_timestamp(timestamp_text)
            if candidate_epoch >= current_epoch:
                merged[match_id] = timestamp_text

    return merged


_EVENT_USER_KEYS = (
    "UserID",
    "UserId",
    "User",
    "UserName",
    "user_id",
    "user_name",
    "NameID",
    "Name",
    "ID",
    "CardNo",
    "CardNumber",
)


def _event_timestamp_text(event: Dict[str, Any]) -> str:
    for key in ("timestamp", "Time", "DateTime", "datetime", "EventTime", "LogTime"):
        text = _normalize_user_match_value(event.get(key))
        if text:
            return text
    date_text = _normalize_user_match_value(event.get("Date")) or _normalize_user_match_value(
        event.get("date")
    )
    time_text = _normalize_user_match_value(event.get("Time")) or _normalize_user_match_value(
        event.get("time")
    )
    if date_text and time_text:
        return f"{date_text} {time_text}"
    ts_value = AccessHistory._coerce_timestamp(event.get("_t"))
    if ts_value:
        try:
            return dt.datetime.fromtimestamp(ts_value, dt.timezone.utc).isoformat()
        except Exception:
            return _normalize_user_match_value(event.get("_t"))
    return ""


def _merge_last_access_from_events(
    events: Iterable[Dict[str, Any]], users: Dict[str, Any]
) -> Dict[str, str]:
    match_index = _build_user_match_index(users)
    merged: Dict[str, str] = {}

    for event in events or []:
        if not isinstance(event, dict):
            continue

        raw_value = None
        for key in _EVENT_USER_KEYS:
            raw_value = event.get(key)
            if raw_value not in (None, ""):
                break

        raw_text = _normalize_user_match_value(raw_value)
        if isinstance(raw_text, str):
            raw_text = raw_text.strip()
        if not raw_text:
            continue

        match_id = None
        for candidate in (normalize_user_id(raw_text), raw_text):
            if not candidate:
                continue
            match_id = match_index.get(candidate.lower())
            if match_id:
                break

        if not match_id:
            continue

        timestamp_text = _event_timestamp_text(event)
        if not timestamp_text:
            continue

        current = merged.get(match_id)
        current_epoch = AccessHistory._coerce_timestamp(current) if current else 0.0
        candidate_epoch = AccessHistory._coerce_timestamp(timestamp_text)
        if candidate_epoch >= current_epoch:
            merged[match_id] = timestamp_text

    return merged


def _device_relay_roles(opts: Dict[str, Any], device_type: Any) -> Dict[str, str]:
    raw = opts.get(CONF_RELAY_ROLES)
    if not isinstance(raw, dict):
        raw = {
            "relay_a": opts.get("relay_a_role"),
            "relay_b": opts.get("relay_b_role"),
        }
    return normalize_relay_roles(raw, device_type)


def _serialize_devices(root: Dict[str, Any]) -> tuple[List[Dict[str, Any]], bool]:
    devices: List[Dict[str, Any]] = []
    any_alarm = False
    for entry_id, bucket, coord, opts in _iter_device_buckets(root):
        health = getattr(coord, "health", {}) or {}
        device_type_raw = str(health.get("device_type") or "").strip()
        relay_roles = _device_relay_roles(opts, device_type_raw)
        try:
            opts[CONF_RELAY_ROLES] = relay_roles
        except Exception:
            pass

        dev = {
            "entry_id": entry_id,
            "name": _best_name(coord, bucket),
            "type": health.get("device_type"),
            "ip": health.get("ip"),
            "online": health.get("online", True),
            "status": health.get("status"),
            "sync_status": health.get("sync_status", "pending"),
            "last_sync": health.get("last_sync", "—"),
            "events": list(getattr(coord, "events", []) or []),
            "_users": list(getattr(coord, "users", []) or []),
            "users": list(getattr(coord, "users", []) or []),
            "exit_device": bool(opts.get("exit_device", False)),
            "participate_in_sync": bool(opts.get("participate_in_sync", True)),
            "sync_groups": list(opts.get("sync_groups") or ["Default"]),
            "relay_roles": relay_roles,
        }
        dev["alarm_capable"] = relay_alarm_capable(relay_roles)
        devices.append(dev)
        if dev["alarm_capable"]:
            any_alarm = True

    return devices, any_alarm


# ========================= STATE =========================
class AkuvoxStaticAssets(HomeAssistantView):
    url = "/api/AK_AC/{path:.*}"
    name = "api:akuvox_ac:static"
    requires_auth = False

    async def get(self, request: web.Request, path: str = ""):
        hass: HomeAssistant = request.app["hass"]
        clean = (path or "").lstrip("/")
        is_face_request = clean.lower().startswith("facedata")
        if is_face_request:
            rel = clean[8:].lstrip("/")
            if rel:
                base = _persistent_face_dir(hass)
                candidate = (base / rel).resolve()
                try:
                    candidate.relative_to(base)
                except ValueError:
                    raise web.HTTPForbidden()
                if candidate.is_file():
                    return web.FileResponse(candidate)

                legacy_candidate = _legacy_face_candidate(hass, rel)
                if legacy_candidate and legacy_candidate.is_file():
                    try:
                        base.mkdir(parents=True, exist_ok=True)
                        candidate.write_bytes(legacy_candidate.read_bytes())
                    except Exception:
                        return web.FileResponse(legacy_candidate)
                    else:
                        try:
                            legacy_candidate.unlink()
                        except Exception:
                            pass
                        return web.FileResponse(candidate)

        asset = _static_asset(path)
        if is_face_request:
            rel = clean[8:].lstrip("/")
            if rel:
                try:
                    dest_dir = _persistent_face_dir(hass)
                    dest_dir.mkdir(parents=True, exist_ok=True)
                    dest = (dest_dir / rel).resolve()
                    dest.relative_to(dest_dir)
                    if not dest.exists():
                        legacy_candidate = _legacy_face_candidate(hass, rel)
                        if legacy_candidate and legacy_candidate.exists():
                            dest.write_bytes(legacy_candidate.read_bytes())
                            try:
                                legacy_candidate.unlink()
                            except Exception:
                                pass
                        elif asset.is_file() and asset != dest:
                            dest.write_bytes(asset.read_bytes())
                except Exception:
                    pass
        return web.FileResponse(asset)


class AkuvoxDashboardView(HomeAssistantView):
    url = "/akuvox-ac/{slug:.*}"
    name = "akuvox_ac:dashboard"
    requires_auth = True

    async def get(self, request: web.Request, slug: str = ""):
        clean = (slug or "").strip().strip("/").lower()
        if not clean:
            clean = "head"

        target = DASHBOARD_ROUTES.get(clean)
        if not target and clean.endswith(".html"):
            target = DASHBOARD_ROUTES.get(clean[:-5])

        if not target:
            raise web.HTTPNotFound()

        hass: HomeAssistant = request.app["hass"]
        if not _dashboard_access_allowed(hass, request) and not target.startswith("unauthorized"):
            target = "unauthorized"

        asset = _resolve_dashboard_asset(target, request)
        variant = "mobile" if asset.name.endswith("-mob.html") else "desktop"
        if asset.suffix.lower() == ".html":
            hass: HomeAssistant = request.app["hass"]
            signed = _signed_paths_for_request(hass, request)
            try:
                html = asset.read_text(encoding="utf-8")
            except Exception:
                html = asset.read_text()
            html = _inject_signed_paths(html, signed)
            response = web.Response(text=html, content_type="text/html")
            response.headers["X-AK-AC-Variant"] = variant
            return response

        return web.FileResponse(asset, headers={"X-AK-AC-Variant": variant})


class AkuvoxUIPanel(HomeAssistantView):
    url = "/api/akuvox_ac/ui/panel"
    name = "api:akuvox_ac:ui_panel"
    requires_auth = True

    async def get(self, request: web.Request):
        hass: HomeAssistant = request.app["hass"]
        refresh_id = request.get(KEY_HASS_REFRESH_TOKEN_ID)
        if not refresh_id:
            if request.get(KEY_HASS_USER):
                raise web.HTTPFound("/akuvox-ac/")
            raise web.HTTPUnauthorized()
        try:
            signed = async_sign_path(
                hass,
                "/akuvox-ac/",
                dt.timedelta(minutes=10),
                refresh_token_id=refresh_id,
            )
        except Exception:
            raise web.HTTPUnauthorized()
        raise web.HTTPFound(signed)


class AkuvoxUIView(HomeAssistantView):
    url = "/api/akuvox_ac/ui/state"
    name = "api:akuvox_ac:ui_state"
    requires_auth = True

    async def get(self, request: web.Request):
        hass: HomeAssistant = request.app["hass"]
        root = hass.data.get(DOMAIN, {}) or {}

        try:
            _cleanup_stale_reservations(hass, max_age_minutes=RESERVATION_TTL_MINUTES)
        except Exception as err:
            _LOGGER.debug("Reservation cleanup failed: %s", err)

        response: Dict[str, Any] = {
            "kpis": {
                "devices": 0,
                "users": 0,
                "pending": 0,
                "next_sync": "—",
                "events_last_sync": None,
                "auto_sync_time": None,
                "next_sync_eta": None,
                "version": INTEGRATION_VERSION_LABEL,
                "version_raw": INTEGRATION_VERSION,
            },
            "devices": [],
            "access_events": [],
            "access_event_limit": DEFAULT_ACCESS_HISTORY_LIMIT,
            "registry_users": [],
            "schedules": {},
            "schedule_ids": {},
            "groups": [],
            "all_groups": [],
            "capabilities": {"alarm_relay": False},
            "credential_prompts": {
                "code": True,
                "token": True,
                "anpr": False,
                "face": True,
                "phone": True,
            },
        }

        kpis: Dict[str, Any] = response["kpis"]

        try:
            devices_serialized, any_alarm = _serialize_devices(root)
            response["devices"] = devices_serialized
            response["capabilities"] = {"alarm_relay": any_alarm}
            devices = devices_serialized

            kpis["devices"] = len(devices)
            queue_active = bool(getattr(root.get("sync_queue"), "_active", False))
            kpis["pending"] = sum(
                1
                for d in devices
                if d.get("sync_status") != "in_sync" and d.get("online", True)
            )
            kpis["sync_active"] = queue_active or any(
                d.get("online", True) and d.get("sync_status") == "in_progress"
                for d in devices
            )

            us = root.get("users_store")
            if us:
                try:
                    profiles = us.all() or {}
                except Exception:
                    profiles = {}
                kpis["users"] = sum(
                    1
                    for key, prof in profiles.items()
                    if normalize_user_id(key) and not _profile_is_empty_reserved(prof)
                )

            settings_store = root.get("settings_store")
            try:
                access_limit = (
                    settings_store.get_access_history_limit()
                    if settings_store and hasattr(settings_store, "get_access_history_limit")
                    else DEFAULT_ACCESS_HISTORY_LIMIT
                )
            except Exception:
                access_limit = DEFAULT_ACCESS_HISTORY_LIMIT

            history = root.get("access_history")
            aggregated_events: List[Dict[str, Any]] = []
            if history and hasattr(history, "snapshot"):
                try:
                    aggregated_events = history.snapshot(access_limit)
                except Exception:
                    aggregated_events = []
            response["access_events"] = aggregated_events
            response["access_event_limit"] = access_limit

            last_event_epoch = 0.0

            def _consider_event_timestamp(event: Optional[Dict[str, Any]]) -> None:
                nonlocal last_event_epoch
                if not isinstance(event, dict):
                    return
                ts_value = AccessHistory._coerce_timestamp(event.get("_t"))
                if not ts_value:
                    ts_value = AccessHistory._coerce_timestamp(event.get("timestamp"))
                if not ts_value:
                    ts_value = AccessHistory._coerce_timestamp(event.get("Time"))
                if ts_value and ts_value > last_event_epoch:
                    last_event_epoch = ts_value

            for item in aggregated_events:
                _consider_event_timestamp(item)

            for device in devices:
                for event in device.get("events", []) or []:
                    _consider_event_timestamp(event)

            if last_event_epoch:
                try:
                    kpis["events_last_sync"] = dt.datetime.fromtimestamp(
                        last_event_epoch, dt.timezone.utc
                    ).isoformat()
                except Exception:
                    kpis["events_last_sync"] = last_event_epoch

            mgr = root.get("sync_manager")
            if queue_active:
                kpis["next_sync"] = "Syncing…"
            elif mgr:
                try:
                    kpis["next_sync"] = _only_hhmm(mgr.get_next_sync_text())
                except Exception:
                    pass
            sq = root.get("sync_queue")
            if not queue_active and getattr(sq, "next_sync_eta", None):
                try:
                    kpis["next_sync_eta"] = getattr(sq, "next_sync_eta").isoformat()
                except Exception:
                    pass
            settings = root.get("settings_store")
            if settings:
                try:
                    kpis["auto_sync_time"] = settings.get_auto_sync_time()
                    kpis["auto_reboot"] = settings.get_auto_reboot()
                except Exception:
                    pass
                try:
                    response["credential_prompts"] = settings.get_credential_prompts()
                except Exception:
                    pass

            registry_users: List[Dict[str, Any]] = []
            all_users: Dict[str, Any] = {}
            if us:
                try:
                    all_users = us.all() or {}
                except Exception:
                    all_users = {}
                today = dt.date.today()
                for key, prof in all_users.items():
                    canonical = normalize_user_id(key)
                    if not canonical or _profile_is_empty_reserved(prof):
                        continue
                    if str(prof.get("status") or "").strip().lower() == "deleted":
                        continue
                    groups = _normalize_groups(prof.get("groups"))
                    face_status = str(prof.get("face_status") or "").strip().lower()
                    face_synced_at = prof.get("face_synced_at")
                    access_start = _parse_access_date(prof.get("access_start"))
                    access_end = _parse_access_date(prof.get("access_end"))
                    registry_users.append(
                        {
                            "id": canonical,
                            "name": (prof.get("name") or canonical),
                            "groups": groups,
                            "pin": prof.get("pin") or "",
                            "face_url": prof.get("face_url") or "",
                            "face_status": face_status,
                            "face_synced_at": face_synced_at,
                            "face_active": face_status == "active"
                            or _face_image_exists(hass, canonical),
                            "face_error_count": int(prof.get("face_error_count") or 0),
                            "phone": prof.get("phone") or "",
                            "status": prof.get("status") or "active",
                            "schedule_name": prof.get("schedule_name")
                            or "24/7 Access",
                            "schedule_id": prof.get("schedule_id") or "",
                            "paused": bool(prof.get("paused")),
                            "paused_schedule_id": prof.get("paused_schedule_id") or "",
                            "paused_schedule_name": prof.get("paused_schedule_name") or "",
                            "key_holder": bool(prof.get("key_holder", False)),
                            "pedestrian_access": prof.get("pedestrian_access"),
                            "access_level": prof.get("access_level") or "",
                            "access_start": access_start.isoformat() if access_start else "",
                            "access_end": access_end.isoformat() if access_end else "",
                            "access_expired": bool(access_end and access_end <= today),
                            "access_in_future": bool(access_start and access_start > today),
                            "temporary": bool(prof.get("temporary")),
                            "temporary_one_time": bool(prof.get("temporary_one_time")),
                            "temporary_expires_at": prof.get("temporary_expires_at") or "",
                            "temporary_used_at": prof.get("temporary_used_at") or "",
                            "temporary_created_at": prof.get("temporary_created_at") or "",
                            "remote_enrol_pending": bool(
                                prof.get("remote_enrol_pending")
                            ),
                            "license_plate": _extract_license_plates(prof),
                            "exit_permission": _normalize_exit_permission_http(
                                prof.get("exit_permission")
                            )
                            or EXIT_PERMISSION_MATCH,
                        }
                    )
            await _refresh_face_statuses(hass, us, registry_users, devices, all_users)
            last_access_by_user = _merge_last_access(root, all_users)
            event_last_access = _merge_last_access_from_events(aggregated_events, all_users)
            if event_last_access:
                for user_id, timestamp in event_last_access.items():
                    if not timestamp:
                        continue
                    current = last_access_by_user.get(user_id)
                    current_epoch = (
                        AccessHistory._coerce_timestamp(current) if current else 0.0
                    )
                    candidate_epoch = AccessHistory._coerce_timestamp(timestamp)
                    if candidate_epoch >= current_epoch:
                        last_access_by_user[user_id] = timestamp
            if last_access_by_user:
                for entry in registry_users:
                    user_id = entry.get("id")
                    if user_id and user_id in last_access_by_user:
                        entry["last_access"] = last_access_by_user[user_id]
            response["registry_users"] = registry_users

            schedules = {}
            ss = root.get("schedules_store")
            if ss:
                try:
                    schedules = ss.all()
                except Exception:
                    schedules = {}
            response["schedules"] = schedules
            try:
                response["schedule_ids"] = await _fetch_device_schedule_ids(root)
            except Exception:
                response["schedule_ids"] = {
                    "24/7 Access": "1001",
                    "No Access": "1002",
                }

            groups: List[str] = []
            gs = root.get("groups_store")
            if gs:
                try:
                    groups = gs.groups()
                except Exception:
                    groups = []
            response["groups"] = groups
            response["all_groups"] = groups

        except Exception as err:
            _LOGGER.debug("Failed to build Akuvox state payload: %s", err)

        return web.json_response(response)


# ========================= ACTIONS =========================
class AkuvoxUIAction(AkuvoxUIView):
    url = "/api/akuvox_ac/ui/action"
    name = "api:akuvox_ac:ui_action"

    async def post(self, request: web.Request):
        hass: HomeAssistant = request.app["hass"]
        raw_root = hass.data.get(DOMAIN, {}) or {}
        root = raw_root if isinstance(raw_root, dict) else {}

        try:
            data = await request.json()
        except Exception:
            data = {}
        if not isinstance(data, dict):
            data = {}

        action = data.get("action")
        payload = data.get("payload") or {}
        if not isinstance(payload, dict):
            payload = {}
        entry_id = data.get("entry_id")
        ctx = request["context"] if "context" in request else None

        def err(msg: Exception | str, code: int = 400):
            text = str(msg) if isinstance(msg, str) else (str(msg) or "unknown error")
            return web.json_response({"ok": False, "error": text}, status=code)

        # Settings
        if action == "set_daily_sync":
            try:
                root["sync_manager"].set_auto_sync_time(payload["time"])
                return web.json_response({"ok": True})
            except Exception as e:
                return err(e)

        if action == "set_auto_reboot":
            try:
                t = payload.get("time")
                days = payload.get("days") or []
                root["sync_manager"].set_auto_reboot(t, days)
                return web.json_response({"ok": True})
            except Exception as e:
                return err(e)

        # Sync / Reboot
        if action == "sync_now":
            queue = root.get("sync_queue")
            try:
                service_data = {"entry_id": entry_id} if entry_id else {}
                await hass.services.async_call(DOMAIN, "sync_now", service_data, blocking=True, context=ctx)
                return web.json_response({"ok": True})
            except Exception as service_err:
                _LOGGER.debug("Sync-now service call failed via UI: %s", service_err)
                if not queue:
                    return err(service_err)
                try:
                    await queue.sync_now(entry_id, include_all=not entry_id, full=True)
                    return web.json_response({"ok": True})
                except Exception as queue_err:
                    return err(queue_err)

        if action == "refresh_events":
            try:
                service_data = {"entry_id": entry_id} if entry_id else {}
                await hass.services.async_call(DOMAIN, "refresh_events", service_data, blocking=True, context=ctx)
                return web.json_response({"ok": True})
            except Exception as service_err:
                _LOGGER.debug("Refresh-events service call failed via UI: %s", service_err)
                return err(service_err)

        if action in ("force_full_sync", "sync_all"):
            queue = root.get("sync_queue")
            manager = root.get("sync_manager")
            try:
                service_data = {"entry_id": entry_id} if entry_id else {}
                await hass.services.async_call(
                    DOMAIN, "force_full_sync", service_data, blocking=True, context=ctx
                )
                return web.json_response({"ok": True})
            except Exception as service_err:
                _LOGGER.debug("Force full sync service call failed via UI: %s", service_err)
                if not queue or not manager:
                    return err(service_err)

                triggered_by = _context_user_name(hass, ctx)
                coords: List[Any] = []

                if entry_id:
                    data = root.get(entry_id) or {}
                    coord = data.get("coordinator")
                    if coord:
                        coords.append(coord)
                else:
                    try:
                        for _entry_id, coord, *_ in manager._devices():
                            if coord:
                                coords.append(coord)
                    except Exception:
                        coords = []

                for coord in coords:
                    try:
                        coord._append_event(  # type: ignore[attr-defined]
                            f"Full Sync Triggered by - {triggered_by}"
                        )
                    except Exception:
                        pass

                try:
                    await queue.sync_now(entry_id, include_all=not entry_id)
                    return web.json_response({"ok": True})
                except Exception as queue_err:
                    return err(queue_err)

        if action == "reboot_all":
            try:
                await hass.services.async_call(
                    DOMAIN, "reboot_device", {}, blocking=True, context=ctx
                )
                return web.json_response({"ok": True})
            except Exception as service_err:
                _LOGGER.debug("Reboot-all service call failed via UI: %s", service_err)
                triggered_by = _context_user_name(hass, ctx)
                try:
                    await _reboot_devices_direct(root, entry_id=None, triggered_by=triggered_by)
                    return web.json_response({"ok": True})
                except Exception as fallback_err:
                    return err(fallback_err)

        if action == "reboot_device":
            if not entry_id:
                return err("entry_id required")
            try:
                await hass.services.async_call(
                    DOMAIN,
                    "reboot_device",
                    {"entry_id": entry_id},
                    blocking=True,
                    context=ctx,
                )
                return web.json_response({"ok": True})
            except Exception as service_err:
                _LOGGER.debug(
                    "Reboot service call failed via UI for %s: %s", entry_id, service_err
                )
                triggered_by = _context_user_name(hass, ctx)
                try:
                    await _reboot_devices_direct(
                        root, entry_id=entry_id, triggered_by=triggered_by
                    )
                    return web.json_response({"ok": True})
                except Exception as fallback_err:
                    return err(fallback_err)

        if action == "remove_device":
            if not entry_id:
                return err("entry_id required")
            entry = hass.config_entries.async_get_entry(entry_id)
            if not entry:
                return err("device entry not found", code=404)
            try:
                await hass.config_entries.async_remove(entry_id)
                return web.json_response({"ok": True})
            except Exception as remove_err:
                return err(remove_err)

        if action == "remove_face":
            raw_id = payload.get("id") or payload.get("user_id")
            user_id = str(raw_id or "").strip()
            if not user_id:
                return err("id required")
            canonical = normalize_user_id(user_id) or user_id

            users_store = root.get("users_store")
            if users_store:
                try:
                    await users_store.upsert_profile(
                        canonical,
                        face_url="",
                        face_status="",
                        face_synced_at="",
                    )
                except Exception:
                    pass

            manager = root.get("sync_manager")
            if manager:
                try:
                    for _entry_id, coord, api, _ in manager._devices():
                        try:
                            await api.face_delete(canonical)
                        except Exception:
                            continue
                        try:
                            await coord.async_request_refresh()
                        except Exception:
                            pass
                except Exception:
                    pass

            try:
                _remove_face_files(hass, canonical)
            except Exception:
                pass

            queue = root.get("sync_queue")
            if queue:
                try:
                    queue.mark_change(None)
                except Exception:
                    pass

            return web.json_response({"ok": True})

        # Device options
        if action == "set_exit_device":
            if not entry_id:
                return err("entry_id required")
            try:
                enabled = bool(payload.get("enabled", True))
                bucket = root.get(entry_id)
                if not isinstance(bucket, dict):
                    return err("device entry not found", code=404)
                opts = bucket.get("options")
                if not isinstance(opts, dict):
                    opts = {}
                    bucket["options"] = opts
                opts["exit_device"] = enabled

                entry_obj = hass.config_entries.async_get_entry(entry_id)
                if entry_obj:
                    new_options = dict(entry_obj.options)
                    new_options["exit_device"] = enabled
                    hass.config_entries.async_update_entry(entry_obj, options=new_options)

                queue = root.get("sync_queue")
                if queue:
                    try:
                        queue.mark_change(entry_id)
                    except Exception:
                        pass
                return web.json_response({"ok": True, "exit_device": enabled})
            except Exception as e:
                return err(e)

        if action == "set_device_relays":
            if not entry_id:
                return err("entry_id required")
            try:
                bucket = root.get(entry_id)
                if not isinstance(bucket, dict):
                    return err("device entry not found", code=404)
                coord = bucket.get("coordinator")
                if not coord:
                    return err("device coordinator not ready", code=409)
                health = getattr(coord, "health", {}) or {}
                device_type = str(health.get("device_type") or "").strip()

                opts = bucket.get("options")
                if not isinstance(opts, dict):
                    opts = {}
                    bucket["options"] = opts

                relays_payload = (
                    payload.get("relays") if isinstance(payload.get("relays"), dict) else payload
                )
                current_roles = _device_relay_roles(opts, device_type)
                if isinstance(relays_payload, dict):
                    for key in ("relay_a", "relay_b"):
                        if key in relays_payload:
                            current_roles[key] = relays_payload[key]
                normalized = normalize_relay_roles(current_roles, device_type)
                opts[CONF_RELAY_ROLES] = normalized

                entry_obj = hass.config_entries.async_get_entry(entry_id)
                if entry_obj:
                    new_options = dict(entry_obj.options)
                    new_options[CONF_RELAY_ROLES] = normalized
                    hass.config_entries.async_update_entry(entry_obj, options=new_options)

                queue = root.get("sync_queue")
                if queue:
                    try:
                        queue.mark_change(entry_id)
                    except Exception:
                        pass

                alarm_any = False
                try:
                    _, alarm_any = _serialize_devices(root)
                except Exception:
                    alarm_any = False

                return web.json_response(
                    {
                        "ok": True,
                        "relay_roles": normalized,
                        "alarm_capable": relay_alarm_capable(normalized),
                        "device_alarm_any": alarm_any,
                    }
                )
            except Exception as e:
                return err(e)

        if action == "wipe_device_records":
            if not entry_id:
                return err("entry_id required")
            try:
                bucket = root.get(entry_id)
                if not isinstance(bucket, dict):
                    return err("device entry not found", code=404)
                api = bucket.get("api")
                coord = bucket.get("coordinator")
                if not api:
                    return err("device api not ready", code=409)
                await api.user_delete_all()
                if coord:
                    try:
                        coord.users = []
                    except Exception:
                        pass
                return web.json_response({"ok": True})
            except Exception as e:
                return err(e)

        # Groups
        if action == "create_group":
            name = str(payload.get("name") or "").strip()
            if not name:
                return err("group name required")
            store = root.get("groups_store")
            if not store:
                return err("groups store not ready", code=500)
            try:
                await store.add_group(name)
                return web.json_response({"ok": True, "groups": store.groups()})
            except Exception as e:
                return err(e)

        if action == "device_set_groups":
            if not entry_id:
                return err("entry_id required")
            raw_groups = payload.get("groups")
            if isinstance(raw_groups, list):
                groups = [str(g).strip() for g in raw_groups if str(g).strip()]
            elif raw_groups:
                groups = [str(raw_groups).strip()]
            else:
                groups = []
            if not groups:
                groups = ["Default"]

            try:
                store = root.get("groups_store")
                if store:
                    valid = set(store.groups())
                    ordered: List[str] = []
                    for g in groups:
                        if g in valid and g not in ordered:
                            ordered.append(g)
                    groups = ordered or ["Default"]

                entry = hass.config_entries.async_get_entry(entry_id)
                if not entry:
                    return err("device entry not found", code=404)

                new_options = dict(entry.options)
                new_options[CONF_DEVICE_GROUPS] = groups
                hass.config_entries.async_update_entry(entry, options=new_options)

                try:
                    bucket = root.get(entry_id)
                    if isinstance(bucket, dict):
                        bucket.setdefault("options", {})["sync_groups"] = groups
                except Exception:
                    pass

                queue = root.get("sync_queue")
                if queue:
                    try:
                        queue.mark_change(entry_id)
                    except Exception:
                        pass

                return web.json_response({"ok": True, "groups": groups})
            except Exception as e:
                return err(e)

        # Schedules
        if action == "upsert_schedule":
            try:
                name = payload["name"]
                spec = payload["spec"]
                await root["schedules_store"].upsert(name, spec)
                root["sync_queue"].mark_change(None, full=True)
                return web.json_response({"ok": True})
            except Exception as e:
                return err(e)

        if action == "delete_schedule":
            try:
                name = payload["name"]
                await root["schedules_store"].delete(name)
                root["sync_queue"].mark_change(None, full=True)
                return web.json_response({"ok": True})
            except Exception as e:
                return err(e)

        if action == "diagnostics_send_request":
            if not entry_id:
                return err("entry_id required")

            manager = root.get("sync_manager")
            if not manager:
                return err("sync manager unavailable", code=500)

            method = str(payload.get("method") or "POST").strip().upper()
            if method not in {"GET", "POST"}:
                return err("method must be GET or POST")

            raw_path = str(payload.get("path") or "").strip()
            if not raw_path:
                return err("path required")

            def _normalize_rel(value: str) -> Optional[str]:
                text = str(value or "").strip()
                if not text:
                    return None
                if not text.startswith("/"):
                    text = "/" + text
                return text

            primary_path = _normalize_rel(raw_path)
            if not primary_path:
                return err("path required")

            rel_paths: List[str] = [primary_path]
            fallbacks = payload.get("fallbacks") or payload.get("rel_paths") or payload.get("alternate_paths")
            if isinstance(fallbacks, (list, tuple)):
                for extra in fallbacks:
                    normalized = _normalize_rel(extra)
                    if normalized and normalized not in rel_paths:
                        rel_paths.append(normalized)

            api = None
            try:
                for dev_entry_id, _coord, dev_api, _opts in manager._devices():  # type: ignore[attr-defined]
                    if dev_entry_id == entry_id:
                        api = dev_api
                        break
            except Exception:
                api = None

            if not api:
                return err("device entry not found", code=404)

            body = None
            if method == "POST":
                body = payload.get("body")
                if body is None:
                    body = {}
                elif not isinstance(body, (dict, list)):
                    return err("payload body must be a JSON object or array")

            try:
                response_payload = await api._request_attempts(  # type: ignore[attr-defined]
                    method, tuple(rel_paths), body if method == "POST" else None
                )
            except Exception as request_err:
                return err(request_err, code=500)

            return web.json_response(
                {
                    "ok": True,
                    "response": response_payload,
                    "request": {
                        "entry_id": entry_id,
                        "method": method,
                        "paths": rel_paths,
                    },
                }
            )

        return err("unknown action")


# ========================= Devices list (still available if needed) =========================
class AkuvoxUIDevices(HomeAssistantView):
    url = "/api/akuvox_ac/ui/devices"
    name = "api:akuvox_ac:ui_devices"
    requires_auth = True

    async def get(self, request: web.Request):
        hass: HomeAssistant = request.app["hass"]
        root = hass.data.get(DOMAIN, {}) or {}

        out: List[Dict[str, str]] = []
        try:
            mgr = root.get("sync_manager")
            if not mgr:
                return web.json_response({"devices": out})

            for entry_id, coord, api, bucket_opts in mgr._devices():
                disp = _best_name(coord, hass.data[DOMAIN].get(entry_id, {}))
                out.append({"id": entry_id, "name": disp})
        except Exception:
            pass

        return web.json_response({"devices": out})


class AkuvoxUISettings(HomeAssistantView):
    url = "/api/akuvox_ac/ui/settings"
    name = "api:akuvox_ac:ui_settings"
    requires_auth = True

    async def get(self, request: web.Request):
        hass: HomeAssistant = request.app["hass"]
        root = hass.data.get(DOMAIN, {}) or {}

        settings = root.get("settings_store")
        users_store = root.get("users_store")

        devices, any_alarm = _serialize_devices(root)

        interval = None
        delay = None
        health_interval = None
        health_bounds = (MIN_HEALTH_CHECK_INTERVAL, MAX_HEALTH_CHECK_INTERVAL)
        access_limit = DEFAULT_ACCESS_HISTORY_LIMIT
        access_bounds = (MIN_ACCESS_HISTORY_LIMIT, MAX_ACCESS_HISTORY_LIMIT)
        alerts = {"targets": {}}
        face_integrity_enabled = True
        dashboard_access_user_ids: List[str] = []
        if settings:
            try:
                interval = settings.get_integrity_interval_minutes()
            except Exception:
                interval = None
            try:
                face_integrity_enabled = settings.get_face_integrity_enabled()
            except Exception:
                face_integrity_enabled = True
            try:
                delay = settings.get_auto_sync_delay_minutes()
            except Exception:
                delay = None
            try:
                health_interval = settings.get_health_check_interval_seconds()
            except Exception:
                health_interval = None
            try:
                hb = settings.get_health_check_interval_bounds()
                if isinstance(hb, (tuple, list)) and len(hb) >= 2:
                    health_bounds = (int(hb[0]), int(hb[1]))
            except Exception:
                health_bounds = (MIN_HEALTH_CHECK_INTERVAL, MAX_HEALTH_CHECK_INTERVAL)
            try:
                access_limit = settings.get_access_history_limit()
            except Exception:
                access_limit = DEFAULT_ACCESS_HISTORY_LIMIT
            try:
                ab = settings.get_access_history_bounds()
                if isinstance(ab, (tuple, list)) and len(ab) >= 2:
                    access_bounds = (int(ab[0]), int(ab[1]))
            except Exception:
                access_bounds = (MIN_ACCESS_HISTORY_LIMIT, MAX_ACCESS_HISTORY_LIMIT)
            try:
                alerts = {"targets": settings.get_alert_targets()}
            except Exception:
                data = getattr(settings, "data", {})
                if isinstance(data, dict):
                    raw = data.get("alerts") or {}
                    targets = raw.get("targets") if isinstance(raw, dict) else {}
                    alerts = {"targets": targets if isinstance(targets, dict) else {}}
            try:
                dashboard_access_user_ids = settings.get_dashboard_user_ids()
            except Exception:
                dashboard_access_user_ids = []

        registry_users: List[Dict[str, str]] = []
        if users_store:
            try:
                for key, prof in (users_store.all() or {}).items():
                    canonical = normalize_user_id(key)
                    if not canonical:
                        continue
                    if _profile_is_empty_reserved(prof):
                        continue
                    name = prof.get("name") or canonical
                    registry_users.append({"id": canonical, "name": name})
            except Exception:
                pass

        registry_users.sort(key=lambda x: x.get("name", "").lower())

        ha_users: List[Dict[str, Any]] = []
        try:
            for user in hass.auth.async_get_users():
                if getattr(user, "is_active", True) is False:
                    continue
                name = getattr(user, "name", None) or getattr(user, "id", "")
                ha_users.append(
                    {
                        "id": getattr(user, "id", ""),
                        "name": name,
                        "is_admin": bool(getattr(user, "is_admin", False)),
                    }
                )
        except Exception:
            ha_users = []

        ha_users.sort(key=lambda x: str(x.get("name", "")).lower())

        schedules: Dict[str, Any] = {}
        schedules_store = root.get("schedules_store")
        if schedules_store:
            try:
                schedules = schedules_store.all()
            except Exception:
                schedules = {}
        groups: List[str] = []
        groups_store = root.get("groups_store")
        if groups_store:
            try:
                groups = groups_store.groups()
            except Exception:
                groups = []

        credential_prompts = (
            settings.get_credential_prompts()
            if settings and hasattr(settings, "get_credential_prompts")
            else {
                "code": True,
                "token": True,
                "anpr": False,
                "face": True,
            }
        )

        return web.json_response(
            {
                "ok": True,
                "integrity_interval_minutes": interval,
                "face_integrity_enabled": face_integrity_enabled,
                "auto_sync_delay_minutes": delay,
                "health_check_interval_seconds": health_interval,
                "alerts": alerts,
                "registry_users": registry_users,
                "schedules": schedules,
                "devices": devices,
                "capabilities": {"alarm_relay": any_alarm},
                "min_minutes": 5,
                "max_minutes": 24 * 60,
                "min_auto_sync_delay_minutes": 5,
                "max_auto_sync_delay_minutes": 60,
                "min_health_check_interval_seconds": health_bounds[0],
                "max_health_check_interval_seconds": health_bounds[1],
                "access_event_limit": access_limit,
                "min_access_event_limit": access_bounds[0],
                "max_access_event_limit": access_bounds[1],
                "credential_prompts": credential_prompts,
                "groups": groups,
                "dashboard_access_user_ids": dashboard_access_user_ids,
                "ha_users": ha_users,
            }
        )

    async def post(self, request: web.Request):
        hass: HomeAssistant = request.app["hass"]
        root = hass.data.get(DOMAIN, {}) or {}

        try:
            payload = await request.json()
        except Exception:
            payload = {}

        settings = root.get("settings_store")
        manager = root.get("sync_manager")
        queue = root.get("sync_queue")

        response: Dict[str, Any] = {"ok": True}

        if "integrity_interval_minutes" in payload:
            minutes = payload.get("integrity_interval_minutes")
            if manager and hasattr(manager, "set_integrity_interval"):
                try:
                    manager.set_integrity_interval(minutes)
                    if hasattr(manager, "get_integrity_interval_minutes"):
                        response["integrity_interval_minutes"] = manager.get_integrity_interval_minutes()
                except Exception as err:
                    return web.json_response({"ok": False, "error": str(err)}, status=400)
            elif settings:
                try:
                    value = int(minutes)
                except Exception:
                    return web.json_response({"ok": False, "error": "invalid interval"}, status=400)
                value = max(5, min(24 * 60, value))
                try:
                    await settings.set_integrity_interval_minutes(value)
                    response["integrity_interval_minutes"] = value
                except Exception as err:
                    return web.json_response({"ok": False, "error": str(err)}, status=400)

        if "face_integrity_enabled" in payload:
            if not settings or not hasattr(settings, "set_face_integrity_enabled"):
                return web.json_response({"ok": False, "error": "settings unavailable"}, status=500)
            enabled = payload.get("face_integrity_enabled")
            try:
                await settings.set_face_integrity_enabled(bool(enabled))
                response["face_integrity_enabled"] = settings.get_face_integrity_enabled()
            except Exception as err:
                return web.json_response({"ok": False, "error": str(err)}, status=400)

        if "auto_sync_delay_minutes" in payload:
            if not settings or not hasattr(settings, "set_auto_sync_delay_minutes"):
                return web.json_response({"ok": False, "error": "settings unavailable"}, status=500)

            minutes = payload.get("auto_sync_delay_minutes")
            try:
                value = int(minutes)
            except Exception:
                return web.json_response({"ok": False, "error": "invalid delay"}, status=400)
            value = max(5, min(60, value))
            try:
                await settings.set_auto_sync_delay_minutes(value)
                response["auto_sync_delay_minutes"] = settings.get_auto_sync_delay_minutes()
            except Exception as err:
                return web.json_response({"ok": False, "error": str(err)}, status=400)

            if queue and hasattr(queue, "refresh_default_delay"):
                try:
                    queue.refresh_default_delay()
                except Exception:
                    pass

        if "health_check_interval_seconds" in payload:
            if not settings or not hasattr(settings, "set_health_check_interval_seconds"):
                return web.json_response({"ok": False, "error": "settings unavailable"}, status=500)

            seconds_raw = payload.get("health_check_interval_seconds")
            try:
                seconds = await settings.set_health_check_interval_seconds(seconds_raw)
            except ValueError as err:
                return web.json_response({"ok": False, "error": str(err)}, status=400)
            except Exception as err:
                return web.json_response({"ok": False, "error": str(err)}, status=400)

            response["health_check_interval_seconds"] = seconds

            interval_td = timedelta(seconds=max(MIN_HEALTH_CHECK_INTERVAL, min(MAX_HEALTH_CHECK_INTERVAL, seconds)))
            for key, data in list(root.items()):
                if not isinstance(data, dict):
                    continue
                coord = data.get("coordinator")
                if coord and hasattr(coord, "update_interval"):
                    coord.update_interval = interval_td

        if "access_event_limit" in payload:
            if not settings or not hasattr(settings, "set_access_history_limit"):
                return web.json_response({"ok": False, "error": "settings unavailable"}, status=500)

            try:
                limit_value = await settings.set_access_history_limit(payload.get("access_event_limit"))
            except ValueError as err:
                return web.json_response({"ok": False, "error": str(err)}, status=400)
            except Exception as err:
                return web.json_response({"ok": False, "error": str(err)}, status=400)

            response["access_event_limit"] = limit_value

            history = root.get("access_history")
            if history and hasattr(history, "prune"):
                try:
                    history.prune(limit_value)
                except Exception:
                    pass

        if "alerts" in payload and settings and hasattr(settings, "set_alert_targets"):
            alerts_payload = payload.get("alerts") or {}
            targets = alerts_payload.get("targets") if isinstance(alerts_payload, dict) else {}
            try:
                await settings.set_alert_targets(targets)
                response["alerts"] = {"targets": settings.get_alert_targets()}
            except Exception as err:
                return web.json_response({"ok": False, "error": str(err)}, status=400)

        if "credential_prompts" in payload:
            if not settings or not hasattr(settings, "set_credential_prompts"):
                return web.json_response({"ok": False, "error": "settings unavailable"}, status=500)
            try:
                updated = await settings.set_credential_prompts(payload.get("credential_prompts"))
                response["credential_prompts"] = updated
            except Exception as err:
                return web.json_response({"ok": False, "error": str(err)}, status=400)

        if "dashboard_access_user_ids" in payload:
            if not settings or not hasattr(settings, "set_dashboard_user_ids"):
                return web.json_response({"ok": False, "error": "settings unavailable"}, status=500)
            try:
                user_ids = payload.get("dashboard_access_user_ids") or []
                cleaned = await settings.set_dashboard_user_ids(user_ids)
                response["dashboard_access_user_ids"] = cleaned
            except Exception as err:
                return web.json_response({"ok": False, "error": str(err)}, status=400)

        return web.json_response(response)


# ========================= NEW: list HA mobile app targets =========================
class AkuvoxUIPhones(HomeAssistantView):
    """
    GET -> { phones: [ { service: "mobile_app_johns_iphone", name: "Johns Iphone" }, ... ] }
    """
    url = "/api/akuvox_ac/ui/phones"
    name = "api:akuvox_ac:ui_phones"
    requires_auth = True

    async def get(self, request: web.Request):
        hass: HomeAssistant = request.app["hass"]
        phones: List[Dict[str, str]] = []

        try:
            svc = hass.services.async_services()  # { domain: { service: Service } }
            notify_services = svc.get("notify", {}) or {}
            for service_name in notify_services.keys():
                if not isinstance(service_name, str):
                    continue
                if not service_name.startswith("mobile_app_"):
                    continue
                # derive a friendlier label
                pretty = service_name.replace("mobile_app_", "").replace("_", " ").strip()
                if not pretty:
                    pretty = service_name
                phones.append({"service": service_name, "name": pretty.title()})
        except Exception:
            pass

        # stable ordering
        phones.sort(key=lambda x: x["name"].lower())
        return web.json_response({"phones": phones})


# ========================= Device diagnostics =========================
class AkuvoxUIDiagnostics(HomeAssistantView):
    url = "/api/akuvox_ac/ui/diagnostics"
    name = "api:akuvox_ac:ui_diagnostics"
    requires_auth = True

    def _resolve_history_limits(self, root: Dict[str, Any]) -> Tuple[int, int, int]:
        settings = root.get("settings_store")
        current = DEFAULT_DIAGNOSTICS_HISTORY_LIMIT
        min_limit = MIN_DIAGNOSTICS_HISTORY_LIMIT
        max_limit = MAX_DIAGNOSTICS_HISTORY_LIMIT

        if settings and hasattr(settings, "get_diagnostics_history_limit"):
            try:
                current = settings.get_diagnostics_history_limit()
            except Exception:
                current = DEFAULT_DIAGNOSTICS_HISTORY_LIMIT
            try:
                bounds = settings.get_diagnostics_history_bounds()
                if isinstance(bounds, (tuple, list)) and len(bounds) >= 2:
                    min_limit = int(bounds[0])
                    max_limit = int(bounds[1])
            except Exception:
                min_limit = MIN_DIAGNOSTICS_HISTORY_LIMIT
                max_limit = MAX_DIAGNOSTICS_HISTORY_LIMIT

        if min_limit > max_limit:
            min_limit, max_limit = max_limit, min_limit

        if current < min_limit:
            current = min_limit
        if current > max_limit:
            current = max_limit

        return current, min_limit, max_limit

    @staticmethod
    def _copy_json(value: Any) -> Any:
        try:
            return json.loads(json.dumps(value))
        except Exception:
            if isinstance(value, dict):
                out: Dict[str, Any] = {}
                for key, item in value.items():
                    try:
                        out[key] = AkuvoxUIDiagnostics._copy_json(item)
                    except Exception:
                        pass
                return out
            if isinstance(value, list):
                out_list: List[Any] = []
                for item in value:
                    try:
                        out_list.append(AkuvoxUIDiagnostics._copy_json(item))
                    except Exception:
                        pass
                return out_list
            return value

    @staticmethod
    def _normalize_path_from_request(req: Dict[str, Any]) -> str:
        path_candidate = req.get("path")
        if isinstance(path_candidate, str) and path_candidate.strip():
            candidate = path_candidate.strip()
        else:
            url_candidate = req.get("url")
            candidate = ""
            if isinstance(url_candidate, str) and url_candidate.strip():
                try:
                    parsed = urlsplit(url_candidate)
                    candidate = parsed.path or ""
                except Exception:
                    candidate = url_candidate
        candidate = (candidate or "").strip()
        if candidate and not candidate.startswith("/"):
            candidate = "/" + candidate
        return candidate

    @staticmethod
    def _payload_has_face(value: Any) -> bool:
        if isinstance(value, dict):
            for key, item in value.items():
                if isinstance(key, str) and "face" in key.lower():
                    return True
                if AkuvoxUIDiagnostics._payload_has_face(item):
                    return True
            return False
        if isinstance(value, (list, tuple, set)):
            for item in value:
                if AkuvoxUIDiagnostics._payload_has_face(item):
                    return True
            return False
        if isinstance(value, str):
            return "face" in value.lower()
        return False

    @staticmethod
    def _request_is_face_related(req: Dict[str, Any]) -> bool:
        if not isinstance(req, dict):
            return False
        for key in ("diag_type", "diagType", "type"):
            value = req.get(key)
            if isinstance(value, str) and "face" in value.lower():
                return True
        for key in ("path", "url"):
            value = req.get(key)
            if isinstance(value, str) and "face" in value.lower():
                return True
        payload = req.get("payload")
        if isinstance(payload, dict):
            if AkuvoxUIDiagnostics._payload_has_face(payload):
                return True
            action = payload.get("action") or payload.get("Action")
            target = payload.get("target") or payload.get("Target")
            if isinstance(action, str) and "face" in action.lower():
                return True
            if isinstance(target, str) and "face" in target.lower():
                return True
        return False

    def _summarize_face_attempts(
        self, devices: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        summary: "OrderedDict[str, Dict[str, Any]]" = OrderedDict()

        for device in devices:
            if not isinstance(device, dict):
                continue
            entry_id = str(device.get("entry_id") or "").strip()
            device_name = str(device.get("name") or "").strip() or entry_id or "Akuvox Device"
            requests = device.get("requests") if isinstance(device.get("requests"), list) else []
            for req in requests:
                if not isinstance(req, dict):
                    continue
                if not self._request_is_face_related(req):
                    continue

                method = str(req.get("method") or "GET").upper()
                path = self._normalize_path_from_request(req)

                diag_type = ""
                for key in ("diag_type", "diagType", "type"):
                    candidate = req.get(key)
                    if isinstance(candidate, str) and candidate.strip():
                        diag_type = candidate.strip()
                        break

                payload_raw = req.get("payload")
                payload_dict = payload_raw if isinstance(payload_raw, dict) else None

                target = ""
                action = ""
                if payload_dict:
                    for key in ("target", "Target"):
                        candidate = payload_dict.get(key)
                        if isinstance(candidate, str) and candidate.strip():
                            target = candidate.strip()
                            break
                    for key in ("action", "Action"):
                        candidate = payload_dict.get(key)
                        if isinstance(candidate, str) and candidate.strip():
                            action = candidate.strip()
                            break

                payload_keys: List[str] = []
                data_keys: List[str] = []
                if payload_dict:
                    payload_keys = sorted({str(k).lower() for k in payload_dict.keys() if str(k).strip()})
                    data_section = payload_dict.get("data")
                    if isinstance(data_section, dict):
                        data_keys = sorted({str(k).lower() for k in data_section.keys() if str(k).strip()})

                signature_data = {
                    "method": method,
                    "path": path,
                    "diag": diag_type.lower(),
                    "target": target.lower(),
                    "action": action.lower(),
                    "payload_keys": tuple(payload_keys),
                    "data_keys": tuple(data_keys),
                }
                signature = json.dumps(signature_data, sort_keys=True)

                entry = summary.get(signature)
                if not entry:
                    entry = {
                        "method": method,
                        "path": path,
                        "diag_type": diag_type,
                        "target": target,
                        "action": action,
                        "payload_keys": payload_keys,
                        "data_keys": data_keys,
                        "count": 0,
                        "devices": {},
                        "first_seen": None,
                        "last_seen": None,
                        "last_status": None,
                        "last_error": None,
                        "status_codes": [],
                        "ok_count": 0,
                        "error_count": 0,
                        "payload": None,
                        "last_entry_id": "",
                        "last_device_name": "",
                    }
                    summary[signature] = entry

                entry["count"] += 1
                timestamp = req.get("timestamp")
                if isinstance(timestamp, str) and timestamp:
                    if not entry["first_seen"] or timestamp < entry["first_seen"]:
                        entry["first_seen"] = timestamp
                    if not entry["last_seen"] or timestamp > entry["last_seen"]:
                        entry["last_seen"] = timestamp

                status = req.get("status")
                if status is not None:
                    entry["last_status"] = status
                    entry["status_codes"].append(status)

                if req.get("ok") is True:
                    entry["ok_count"] += 1
                elif req.get("ok") is False:
                    entry["error_count"] += 1

                if req.get("error") and isinstance(req.get("error"), str):
                    entry["last_error"] = req.get("error")

                if payload_dict is not None:
                    entry["payload"] = self._copy_json(payload_dict)

                if entry_id:
                    entry.setdefault("devices", {})[entry_id] = device_name
                    entry["last_entry_id"] = entry_id
                if device_name:
                    entry["last_device_name"] = device_name

        results: List[Dict[str, Any]] = []
        sorted_entries = sorted(
            summary.values(), key=lambda item: item.get("last_seen") or "", reverse=True
        )
        for idx, entry in enumerate(sorted_entries, start=1):
            devices_list = []
            for dev_id, name in sorted(entry.get("devices", {}).items(), key=lambda pair: (pair[1] or pair[0])):
                devices_list.append({"entry_id": dev_id, "name": name})

            result: Dict[str, Any] = {
                "key": f"face_attempt_{idx}",
                "method": entry.get("method"),
                "path": entry.get("path"),
                "diag_type": entry.get("diag_type"),
                "target": entry.get("target"),
                "action": entry.get("action"),
                "payload_keys": entry.get("payload_keys", []),
                "data_keys": entry.get("data_keys", []),
                "count": entry.get("count", 0),
                "first_seen": entry.get("first_seen"),
                "last_seen": entry.get("last_seen"),
                "last_status": entry.get("last_status"),
                "last_error": entry.get("last_error"),
                "ok_count": entry.get("ok_count", 0),
                "error_count": entry.get("error_count", 0),
                "devices": devices_list,
                "payload": entry.get("payload"),
                "last_entry_id": entry.get("last_entry_id"),
                "last_device_name": entry.get("last_device_name"),
            }

            status_codes = entry.get("status_codes")
            if status_codes:
                result["status_codes"] = status_codes[-5:]

            results.append(result)

        return results

    async def _build_payload(
        self, root: Dict[str, Any], limit_override: Optional[int] = None
    ) -> Dict[str, Any]:
        current_limit, min_limit, max_limit = self._resolve_history_limits(root)
        limit = current_limit if limit_override is None else limit_override
        try:
            limit = int(limit)
        except Exception:
            limit = current_limit
        if limit < min_limit:
            limit = min_limit
        if limit > max_limit:
            limit = max_limit

        devices: List[Dict[str, Any]] = []
        manager = root.get("sync_manager")
        if manager:
            try:
                for entry_id, coord, api, _opts in manager._devices():  # type: ignore[attr-defined]
                    try:
                        recent = api.recent_requests(limit)
                    except Exception:
                        recent = []

                    name = (
                        getattr(coord, "display_name", None)
                        or coord.health.get("name")
                        or entry_id
                    )
                    info: Dict[str, Any] = {
                        "entry_id": entry_id,
                        "name": name,
                        "device_type": coord.health.get("device_type"),
                        "ip": coord.health.get("ip"),
                        "host": getattr(api, "host", None),
                        "port": getattr(api, "port", None),
                        "requests": recent,
                    }
                    if recent:
                        info["last_request_at"] = recent[0].get("timestamp")
                    devices.append(info)
            except Exception as err:  # pragma: no cover - best effort
                _LOGGER.debug("Failed to assemble diagnostics payload: %s", err)

        settings_store = root.get("settings_store")
        access_limit = DEFAULT_ACCESS_HISTORY_LIMIT
        access_bounds = (MIN_ACCESS_HISTORY_LIMIT, MAX_ACCESS_HISTORY_LIMIT)
        if settings_store and hasattr(settings_store, "get_access_history_limit"):
            try:
                access_limit = settings_store.get_access_history_limit()
            except Exception:
                access_limit = DEFAULT_ACCESS_HISTORY_LIMIT
            try:
                bounds = settings_store.get_access_history_bounds()
                if isinstance(bounds, (tuple, list)) and len(bounds) >= 2:
                    access_bounds = (int(bounds[0]), int(bounds[1]))
            except Exception:
                access_bounds = (MIN_ACCESS_HISTORY_LIMIT, MAX_ACCESS_HISTORY_LIMIT)

        min_access, max_access = access_bounds
        if min_access > max_access:
            min_access, max_access = max_access, min_access
        access_bounds = (min_access, max_access)

        if access_limit < min_access:
            access_limit = min_access
        if access_limit > max_access:
            access_limit = max_access

        history = root.get("access_history")
        aggregated_events: List[Dict[str, Any]] = []
        if history and hasattr(history, "snapshot"):
            try:
                aggregated_events = history.snapshot(access_limit)
            except Exception:
                aggregated_events = []

        events_last_sync: Optional[str] = None
        last_event_epoch = 0.0
        for event in aggregated_events:
            if not isinstance(event, dict):
                continue
            ts_value = AccessHistory._coerce_timestamp(event.get("_t"))
            if not ts_value:
                ts_value = AccessHistory._coerce_timestamp(event.get("timestamp"))
            if not ts_value:
                ts_value = AccessHistory._coerce_timestamp(event.get("Time"))
            if ts_value and ts_value > last_event_epoch:
                last_event_epoch = ts_value

        if last_event_epoch:
            try:
                events_last_sync = dt.datetime.fromtimestamp(
                    last_event_epoch, dt.timezone.utc
                ).isoformat()
            except Exception:
                events_last_sync = str(last_event_epoch)

        return {
            "ok": True,
            "devices": devices,
            "history_limit": limit,
            "min_history_limit": min_limit,
            "max_history_limit": max_limit,
            "face_attempts": self._summarize_face_attempts(devices),
            "access_events": aggregated_events,
            "access_event_limit": access_limit,
            "min_access_event_limit": min_access,
            "max_access_event_limit": max_access,
            "events_last_sync": events_last_sync,
        }

    async def get(self, request: web.Request):
        hass: HomeAssistant = request.app["hass"]
        root = hass.data.get(DOMAIN, {}) or {}
        payload = await self._build_payload(root)
        return web.json_response(payload)

    async def post(self, request: web.Request):
        hass: HomeAssistant = request.app["hass"]
        root = hass.data.get(DOMAIN, {}) or {}

        settings = root.get("settings_store")
        if not settings or not hasattr(settings, "set_diagnostics_history_limit"):
            return web.json_response(
                {"ok": False, "error": "settings unavailable"}, status=500
            )

        try:
            data = await request.json()
        except Exception:
            data = {}

        if "history_limit" not in data:
            return web.json_response(
                {"ok": False, "error": "history_limit required"}, status=400
            )

        try:
            new_limit = await settings.set_diagnostics_history_limit(
                data.get("history_limit")
            )
        except ValueError as err:
            return web.json_response({"ok": False, "error": str(err)}, status=400)
        except Exception as err:
            return web.json_response({"ok": False, "error": str(err)}, status=500)

        manager = root.get("sync_manager")
        if manager:
            try:
                for _entry_id, _coord, api, _opts in manager._devices():  # type: ignore[attr-defined]
                    try:
                        api.set_diagnostics_history_limit(new_limit)
                    except Exception:
                        pass
            except Exception:  # pragma: no cover - best effort
                pass

        payload = await self._build_payload(root, limit_override=new_limit)
        return web.json_response(payload)


# ========================= Reserve a fresh HA ID =========================
class AkuvoxUIReserveId(HomeAssistantView):
    """
    GET → { ok: true, id: "HAxyz" }
    Pre-allocates the next free HA id with status='pending' and reserved_at.
    """
    url = "/api/akuvox_ac/ui/reserve_id"
    name = "api:akuvox_ac:ui_reserve_id"
    requires_auth = True

    async def get(self, request: web.Request):
        hass: HomeAssistant = request.app["hass"]
        root = hass.data.get(DOMAIN, {}) or {}
        users_store = root.get("users_store")
        if not users_store:
            return web.json_response({"ok": False, "error": "users_store not ready"}, status=500)

        try:
            _cleanup_stale_reservations(hass, max_age_minutes=RESERVATION_TTL_MINUTES)
        except Exception as err:
            _LOGGER.debug("Reservation cleanup before reserve failed: %s", err)

        store_data = users_store.data.setdefault("users", {})  # type: ignore[attr-defined]

        reusable = _select_reusable_reservation(store_data)
        if reusable is not None:
            candidate, store_key = reusable
            existing_profile = store_data.get(store_key)
            if isinstance(existing_profile, dict):
                profile = existing_profile
            elif isinstance(existing_profile, Mapping):
                profile = dict(existing_profile)
            else:
                profile = {}
            profile["status"] = "pending"
            profile["reserved_at"] = _now_iso()
            store_data[store_key] = profile
            _prune_inactive_reservations(store_data, keep_key=store_key)
            try:
                await users_store.async_save()  # type: ignore[attr-defined]
            except Exception:
                pass
            return web.json_response({"ok": True, "id": candidate})

        # Find lowest free HA### using local registry only
        try:
            if hasattr(users_store, "next_free_ha_id"):
                candidate = users_store.next_free_ha_id()  # type: ignore[attr-defined]
            else:
                existing: set[str] = set()
                for key in (users_store.all() or {}).keys():
                    canonical = normalize_ha_id(key)
                    if canonical:
                        existing.add(canonical)
                n = 1
                while True:
                    candidate = _ha_id_from_int(n)
                    if candidate not in existing:
                        break
                    n += 1
        except Exception:
            n = 1
            while True:
                candidate = _ha_id_from_int(n)
                try:
                    current = users_store.all()  # type: ignore[attr-defined]
                except Exception:
                    current = {}
                existing_keys: set[str] = set()
                for key in (current or {}).keys():
                    canonical = normalize_ha_id(key)
                    if canonical:
                        existing_keys.add(canonical)
                if candidate not in existing_keys:
                    break
                n += 1

        # Reserve pending profile; set reserved_at
        try:
            if hasattr(users_store, "upsert_profile"):
                await users_store.upsert_profile(candidate, status="pending")
                users_store.data.setdefault("users", {}).setdefault(candidate, {})["reserved_at"] = _now_iso()  # type: ignore[attr-defined]
                await users_store.async_save()  # type: ignore[attr-defined]
            else:
                data = users_store.data  # type: ignore[attr-defined]
                data.setdefault("users", {}).setdefault(candidate, {})["status"] = "pending"
                data["users"][candidate]["reserved_at"] = _now_iso()
                await users_store.async_save()  # type: ignore[attr-defined]
        except Exception:
            pass

        return web.json_response({"ok": True, "id": candidate})


# ========================= NEW: release a reservation =========================
class AkuvoxUIReleaseId(HomeAssistantView):
    """
    POST JSON: { "id": "HA001" }
    If the profile is still an empty reservation (pending, no name/pin/phone/face),
    delete it from the registry.
    """
    url = "/api/akuvox_ac/ui/release_id"
    name = "api:akuvox_ac:ui_release_id"
    requires_auth = True

    async def post(self, request: web.Request):
        hass: HomeAssistant = request.app["hass"]
        root = hass.data.get(DOMAIN, {}) or {}
        users_store = root.get("users_store")
        if not users_store:
            return web.json_response({"ok": False, "error": "users_store not ready"}, status=500)

        try:
            data = await request.json()
        except Exception:
            data = {}
        uid = normalize_ha_id(data.get("id"))
        if not uid:
            return web.json_response({"ok": False, "error": "valid HA id required"}, status=400)

        try:
            all_users = users_store.all() or {}
            prof = all_users.get(uid) or {}
            store_key = uid
            if not prof:
                for candidate_key in all_users.keys():
                    if normalize_ha_id(candidate_key) == uid:
                        prof = all_users.get(candidate_key) or {}
                        store_key = candidate_key
                        break
            if _profile_is_empty_reserved(prof):
                users_store.data.get("users", {}).pop(store_key, None)  # type: ignore[attr-defined]
                await users_store.async_save()  # type: ignore[attr-defined]
                return web.json_response({"ok": True, "released": True})
            return web.json_response({"ok": True, "released": False})
        except Exception as e:
            return web.json_response({"ok": False, "error": str(e)}, status=500)


class AkuvoxUIReservationPing(HomeAssistantView):
    """
    POST JSON: { "id": "HA001" }
    Refreshes reserved_at for empty reservations to keep them alive while the
    add-user page remains open. Returns { ok, active } where active indicates
    whether the reservation is still valid.
    """

    url = "/api/akuvox_ac/ui/reservation_ping"
    name = "api:akuvox_ac:ui_reservation_ping"
    requires_auth = True

    async def post(self, request: web.Request):
        hass: HomeAssistant = request.app["hass"]
        root = hass.data.get(DOMAIN, {}) or {}
        users_store = root.get("users_store")
        if not users_store:
            return web.json_response({"ok": False, "error": "users_store not ready"}, status=500)

        try:
            data = await request.json()
        except Exception:
            data = {}

        uid = normalize_ha_id(data.get("id"))
        if not uid:
            return web.json_response({"ok": False, "error": "valid HA id required"}, status=400)

        store_data = users_store.data.setdefault("users", {})  # type: ignore[attr-defined]
        store_key = uid
        profile = store_data.get(store_key)
        if not profile:
            for candidate_key in list(store_data.keys()):
                if normalize_ha_id(candidate_key) == uid:
                    store_key = candidate_key
                    profile = store_data.get(store_key)
                    break
        if not profile:
            return web.json_response({"ok": True, "active": False})

        if not _profile_is_empty_reserved(profile):
            return web.json_response({"ok": True, "active": False})

        profile = store_data.setdefault(store_key, {})
        profile["reserved_at"] = _now_iso()
        try:
            await users_store.async_save()  # type: ignore[attr-defined]
        except Exception:
            pass
        return web.json_response({"ok": True, "active": True})


# ========================= Face upload =========================
class AkuvoxUIUploadFace(HomeAssistantView):
    """
    POST multipart/form-data:
      - id: HA001 (required)
      - file: image/jpeg (required)

    Saves to config/akuvox_ac/FaceData/<ID>.jpg
    Updates users_store face_url (public URL) and marks status=pending.
    Triggers immediate sync.
    """
    url = "/api/akuvox_ac/ui/upload_face"
    name = "api:akuvox_ac:ui_upload_face"
    requires_auth = True

    async def post(self, request: web.Request):
        hass: HomeAssistant = request.app["hass"]
        root = hass.data.get(DOMAIN, {}) or {}

        id_val_raw: Optional[str] = None
        file_bytes: Optional[bytes] = None

        content_type = (request.content_type or "").lower()
        if "multipart" in content_type:
            try:
                data = await request.post()
            except Exception:
                data = {}

            if data:
                # Common HTML <input name="id"/> field
                raw_id = data.get("id")
                if isinstance(raw_id, (str, bytes, bytearray)):
                    id_val = raw_id.decode() if isinstance(raw_id, (bytes, bytearray)) else raw_id
                    id_val_raw = id_val.strip()

                # File uploads arrive as aiohttp.web.FileField instances
                candidate = data.get("file") or data.get("upload") or data.get("image")
                if isinstance(candidate, web.FileField):
                    with candidate.file:
                        try:
                            candidate.file.seek(0)
                        except Exception:
                            pass
                        file_bytes = candidate.file.read()
                elif isinstance(candidate, (bytes, bytearray)):
                    file_bytes = bytes(candidate)

                if file_bytes is not None and not isinstance(file_bytes, (bytes, bytearray)):
                    try:
                        file_bytes = bytes(file_bytes)
                    except Exception:
                        file_bytes = None
        else:
            try:
                data = await request.json()
            except Exception:
                data = {}
            candidate = data.get("id")
            if isinstance(candidate, (str, bytes, bytearray)):
                if isinstance(candidate, (bytes, bytearray)):
                    try:
                        candidate = candidate.decode()
                    except Exception:
                        candidate = ""
                id_val_raw = str(candidate).strip()
            else:
                id_val_raw = str(candidate or "").strip()

        id_val = normalize_ha_id(id_val_raw)
        if not id_val:
            return web.json_response(
                {"ok": False, "error": "valid HA user id required (e.g. HA001 or HA-001)"},
                status=400,
            )
        if not file_bytes:
            return web.json_response({"ok": False, "error": "file is required (multipart/form-data)"}, status=400)

        # Save under persistent FaceData folder inside the Home Assistant config
        dest_dir = _persistent_face_dir(hass)
        try:
            dest_dir.mkdir(parents=True, exist_ok=True)
        except Exception:
            pass
        dest_path = (dest_dir / f"{id_val}.jpg").resolve()
        try:
            dest_path.relative_to(dest_dir)
        except ValueError:
            return web.json_response({"ok": False, "error": "invalid filename"}, status=400)
        try:
            dest_path.write_bytes(file_bytes)
        except Exception as e:
            return web.json_response({"ok": False, "error": f"write failed: {e}"}, status=500)
        try:
            legacy = _legacy_face_candidate(hass, f"{id_val}.jpg")
            if legacy and legacy.exists():
                legacy.unlink()
        except Exception:
            pass

        # Store public URL so intercom can fetch it
        face_url_public = f"{face_base_url(hass, request)}/{id_val}.jpg"

        # Update registry profile and mark pending
        try:
            users_store = root.get("users_store")
            if users_store:
                await users_store.upsert_profile(
                    id_val,
                    face_url=face_url_public,
                    status="pending",
                    face_status="pending",
                    face_synced_at="",
                    remote_enrol_pending=False,
                )
        except Exception:
            pass

        # Queue a sync so FaceUrl is pushed during the next cycle
        queue = root.get("sync_queue")
        if queue:
            try:
                queue.mark_change(None)
            except Exception:
                pass

        try:
            await _push_face_to_devices(hass, root, id_val, file_bytes, face_url_public)
        except Exception as err:
            _LOGGER.debug("Failed to push face to devices for %s: %s", id_val, err)

        return web.json_response({"ok": True, "face_url": face_url_public})


# ========================= Remote enrol trigger (push to phone) =========================
class AkuvoxUIRemoteEnrol(HomeAssistantView):
    """
    POST JSON:
      {
        "id": "HA001",
        "phone_service": "mobile_app_johns_iphone"   # name of notify service (without 'notify.')
      }

    Sends a push notification to the selected HA mobile app with a deep link to the
    enrol page, marks user pending, and preserves face_url if already present.
    Also leaves a persistent notification as a fallback.
    """
    url = "/api/akuvox_ac/ui/remote_enrol"
    name = "api:akuvox_ac:ui_remote_enrol"
    requires_auth = True

    async def post(self, request: web.Request):
        hass: HomeAssistant = request.app["hass"]
        root = hass.data.get(DOMAIN, {}) or {}

        try:
            data = await request.json()
        except Exception:
            data = {}
        user_id = normalize_ha_id(data.get("id"))
        phone_service = str(data.get("phone_service") or "").strip()
        raw_name = data.get("name")
        provided_name = ""
        if isinstance(raw_name, str):
            provided_name = raw_name.strip()
        elif raw_name not in (None, ""):
            provided_name = str(raw_name).strip()

        if not user_id:
            return web.json_response({"ok": False, "error": "valid HA user id required"}, status=400)
        if not phone_service:
            return web.json_response({"ok": False, "error": "phone_service required"}, status=400)

        users_store = root.get("users_store")
        profile_name = ""
        if users_store:
            try:
                existing = users_store.get(user_id, {}) or {}
            except Exception:
                existing = {}
            candidate = existing.get("name")
            if isinstance(candidate, str):
                profile_name = candidate.strip()
            elif candidate not in (None, ""):
                profile_name = str(candidate).strip()

        display_name = profile_name or provided_name or user_id

        # Construct enrol URL (served from /api/AK_AC)
        params = {"user": user_id}
        if profile_name or provided_name:
            params["name"] = profile_name or provided_name
        enrol_url = f"/akuvox-ac/face-rec?{urlencode(params)}"

        # Push via HA mobile app notify service
        try:
            await hass.services.async_call(
                "notify",
                phone_service,
                {
                    "title": "Akuvox: Face Enrolment",
                    "message": f"Tap To Enrol {display_name}",
                    "data": {
                        "url": enrol_url
                    },
                },
                blocking=False,
            )
        except Exception:
            # Non-fatal; still proceed with persistent notification
            pass

        # Persistent notification fallback
        try:
            if display_name != user_id:
                display_label = f"**{display_name}** ({user_id})"
            else:
                display_label = f"**{user_id}**"
            notify(
                hass,
                f"Face enrolment requested for {display_label}.\n\n"
                "Open enrolment page",
                title="Akuvox: Face Enrolment",
                notification_id=f"akuvox_face_enrol_{user_id}",
            )
        except Exception:
            pass

        # Ensure profile is pending
        if users_store:
            try:
                await users_store.upsert_profile(
                    user_id,
                    status="pending",
                    name=profile_name or provided_name or None,
                    face_status="pending",
                    remote_enrol_pending=True,
                )
            except Exception:
                pass

        # Ensure the change is picked up on the next sync cycle
        queue = root.get("sync_queue")
        if queue:
            try:
                queue.mark_change(None)
            except Exception:
                pass

        return web.json_response({"ok": True, "enrol_url": enrol_url, "name": display_name})


# ========================= REGISTER =========================
def register_ui(hass: HomeAssistant) -> None:
    hass.http.register_view(AkuvoxStaticAssets())
    hass.http.register_view(AkuvoxDashboardView())
    hass.http.register_view(AkuvoxUIPanel())
    hass.http.register_view(AkuvoxUIView())
    hass.http.register_view(AkuvoxUIAction())
    hass.http.register_view(AkuvoxUIDevices())
    hass.http.register_view(AkuvoxUISettings())
    hass.http.register_view(AkuvoxUIPhones())
    hass.http.register_view(AkuvoxUIDiagnostics())
    hass.http.register_view(AkuvoxUIReserveId())
    hass.http.register_view(AkuvoxUIReleaseId())   # <-- new
    hass.http.register_view(AkuvoxUIReservationPing())
    hass.http.register_view(AkuvoxUIUploadFace())
    hass.http.register_view(AkuvoxInboundCallWebhook())
    hass.http.register_view(AkuvoxUIRemoteEnrol())
