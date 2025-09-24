from __future__ import annotations

import base64
import datetime as dt
import json
import logging
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Mapping
from urllib.parse import urlencode

from aiohttp import web
from homeassistant.components.http.view import HomeAssistantView
from homeassistant.components.http.auth import async_sign_path
from homeassistant.components.http.const import KEY_HASS_REFRESH_TOKEN_ID
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
)

from .relay import alarm_capable as relay_alarm_capable, normalize_roles as normalize_relay_roles
from .ha_id import ha_id_from_int, is_ha_id, normalize_ha_id

COMPONENT_ROOT = Path(__file__).parent
STATIC_ROOT = COMPONENT_ROOT / "www"
FACE_DATA_PATH = "/api/AK_AC/FaceData"
FACE_FILE_EXTENSIONS = ("jpg", "jpeg", "png", "webp")


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
    face_url: str,
    face_data_b64: str,
) -> Dict[str, Any]:
    base = _sanitise_device_record(device_record)
    payload: Dict[str, Any] = dict(base)

    payload["UserID"] = str(user_id)
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

    pin = profile.get("pin")
    if pin not in (None, ""):
        payload["PrivatePIN"] = str(pin)

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

    payload["FaceUrl"] = face_url
    payload["faceInfo"] = {
        "fileName": f"{user_id}.jpg",
        "fileData": face_data_b64,
    }

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

    try:
        face_b64 = base64.b64encode(face_bytes).decode("ascii")
    except Exception:
        _LOGGER.debug("Failed to encode face bytes for %s", user_id)
        return

    for entry_id, coord, api, _opts in manager._devices():
        device_name = getattr(coord, "device_name", entry_id)
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

        try:
            payload = _build_face_upload_payload(profile, record, user_id, face_url_public, face_b64)
        except Exception as err:
            _LOGGER.debug("Failed to prepare face payload for %s on %s: %s", user_id, device_name, err)
            continue

        try:
            await api.face_upload(payload)
        except Exception as err:
            _LOGGER.debug("Direct face upload failed for %s on %s: %s", user_id, device_name, err)
            continue

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

    face_url = str(user.get("face_url") or "").strip()
    has_face_asset = _face_image_exists(hass, user_id)
    wants_face = bool(face_url) or stored_status in {"pending", "active"} or has_face_asset
    if not wants_face:
        return "none"

    user_groups = user.get("groups") or []
    relevant_devices = [
        dev
        for dev in devices
        if dev.get("participate_in_sync", True)
        and _groups_overlap(user_groups, dev.get("sync_groups"))
    ]

    if not relevant_devices:
        return "active"

    for dev in relevant_devices:
        if not dev.get("online", True):
            return "pending"
        sync_state = str(dev.get("sync_status") or "").strip().lower()
        if sync_state != "in_sync":
            return "pending"
        record = None
        for candidate in dev.get("_users") or dev.get("users") or []:
            if _user_key(candidate) == user_id:
                record = candidate
                break
        if record is None:
            return "pending"
        if not _device_face_is_active(record):
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
        user_id = normalize_ha_id(entry.get("id"))
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

        if status_for_store != stored_status_norm or desired_synced_at != stored_synced_at:
            try:
                await users_store.upsert_profile(
                    user_id,
                    face_status=status_for_store,
                    face_synced_at=desired_synced_at or "",
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
    requires_auth = False

    async def get(self, request: web.Request, slug: str = ""):
        clean = (slug or "").strip().strip("/").lower()
        if not clean:
            clean = "head"

        target = DASHBOARD_ROUTES.get(clean)
        if not target and clean.endswith(".html"):
            target = DASHBOARD_ROUTES.get(clean[:-5])

        if not target:
            raise web.HTTPNotFound()

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
                "auto_sync_time": None,
                "next_sync_eta": None,
                "version": INTEGRATION_VERSION_LABEL,
                "version_raw": INTEGRATION_VERSION,
            },
            "devices": [],
            "registry_users": [],
            "schedules": {},
            "groups": [],
            "all_groups": [],
            "capabilities": {"alarm_relay": False},
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
                    if normalize_ha_id(key) and not _profile_is_empty_reserved(prof)
                )

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

            registry_users: List[Dict[str, Any]] = []
            all_users: Dict[str, Any] = {}
            if us:
                try:
                    all_users = us.all() or {}
                except Exception:
                    all_users = {}
                today = dt.date.today()
                for key, prof in all_users.items():
                    canonical = normalize_ha_id(key)
                    if not canonical or _profile_is_empty_reserved(prof):
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
                            "phone": prof.get("phone") or "",
                            "status": prof.get("status") or "active",
                            "schedule_name": prof.get("schedule_name")
                            or "24/7 Access",
                            "schedule_id": prof.get("schedule_id") or "",
                            "key_holder": bool(prof.get("key_holder", False)),
                            "access_level": prof.get("access_level") or "",
                            "access_start": access_start.isoformat() if access_start else "",
                            "access_end": access_end.isoformat() if access_end else "",
                            "access_expired": bool(access_end and access_end <= today),
                            "access_in_future": bool(access_start and access_start > today),
                        }
                    )
            await _refresh_face_statuses(hass, us, registry_users, devices, all_users)
            response["registry_users"] = registry_users

            schedules = {}
            ss = root.get("schedules_store")
            if ss:
                try:
                    schedules = ss.all()
                except Exception:
                    schedules = {}
            response["schedules"] = schedules

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
                    await queue.sync_now(entry_id)
                    return web.json_response({"ok": True})
                except Exception as queue_err:
                    return err(queue_err)

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
                    await queue.sync_now(entry_id)
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
                root["sync_queue"].mark_change(None)
                return web.json_response({"ok": True})
            except Exception as e:
                return err(e)

        if action == "delete_schedule":
            try:
                name = payload["name"]
                await root["schedules_store"].delete(name)
                root["sync_queue"].mark_change(None)
                return web.json_response({"ok": True})
            except Exception as e:
                return err(e)

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
        alerts = {"targets": {}}
        if settings:
            try:
                interval = settings.get_integrity_interval_minutes()
            except Exception:
                interval = None
            try:
                delay = settings.get_auto_sync_delay_minutes()
            except Exception:
                delay = None
            try:
                alerts = {"targets": settings.get_alert_targets()}
            except Exception:
                data = getattr(settings, "data", {})
                if isinstance(data, dict):
                    raw = data.get("alerts") or {}
                    targets = raw.get("targets") if isinstance(raw, dict) else {}
                    alerts = {"targets": targets if isinstance(targets, dict) else {}}

        registry_users: List[Dict[str, str]] = []
        if users_store:
            try:
                for key, prof in (users_store.all() or {}).items():
                    canonical = normalize_ha_id(key)
                    if not canonical:
                        continue
                    if _profile_is_empty_reserved(prof):
                        continue
                    name = prof.get("name") or canonical
                    registry_users.append({"id": canonical, "name": name})
            except Exception:
                pass

        registry_users.sort(key=lambda x: x.get("name", "").lower())

        schedules: Dict[str, Any] = {}
        schedules_store = root.get("schedules_store")
        if schedules_store:
            try:
                schedules = schedules_store.all()
            except Exception:
                schedules = {}

        return web.json_response(
            {
                "ok": True,
                "integrity_interval_minutes": interval,
                "auto_sync_delay_minutes": delay,
                "alerts": alerts,
                "registry_users": registry_users,
                "schedules": schedules,
                "devices": devices,
                "capabilities": {"alarm_relay": any_alarm},
                "min_minutes": 5,
                "max_minutes": 24 * 60,
                "min_auto_sync_delay_minutes": 5,
                "max_auto_sync_delay_minutes": 60,
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

        if "alerts" in payload and settings and hasattr(settings, "set_alert_targets"):
            alerts_payload = payload.get("alerts") or {}
            targets = alerts_payload.get("targets") if isinstance(alerts_payload, dict) else {}
            try:
                await settings.set_alert_targets(targets)
                response["alerts"] = {"targets": settings.get_alert_targets()}
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

        return {
            "ok": True,
            "devices": devices,
            "history_limit": limit,
            "min_history_limit": min_limit,
            "max_history_limit": max_limit,
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
    Pre-allocates the next free HA id with status='pending', reserved_at,
    and pre-fills face_url (public URL).
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

        # Reserve pending profile; set reserved_at and prefill face_url
        try:
            face_url = f"{face_base_url(hass, request)}/{candidate}.jpg"
            if hasattr(users_store, "upsert_profile"):
                await users_store.upsert_profile(candidate, status="pending", face_url=face_url)
                users_store.data.setdefault("users", {}).setdefault(candidate, {})["reserved_at"] = _now_iso()  # type: ignore[attr-defined]
                await users_store.async_save()  # type: ignore[attr-defined]
            else:
                data = users_store.data  # type: ignore[attr-defined]
                data.setdefault("users", {}).setdefault(candidate, {})["status"] = "pending"
                data["users"][candidate]["face_url"] = face_url
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
    enrol page, marks user pending, and ensures face_url is set.
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
                f"[Open enrolment page]({enrol_url})",
                title="Akuvox: Face Enrolment",
                notification_id=f"akuvox_face_enrol_{user_id}",
            )
        except Exception:
            pass

        # Ensure profile is pending and has a canonical face_url
        if users_store:
            try:
                await users_store.upsert_profile(
                    user_id,
                    status="pending",
                    face_url=f"{face_base_url(hass, request)}/{user_id}.jpg",
                    name=profile_name or provided_name or None,
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
    hass.http.register_view(AkuvoxUIRemoteEnrol())
