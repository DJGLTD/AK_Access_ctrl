from __future__ import annotations

import asyncio
from collections.abc import Mapping
from datetime import date, datetime, timedelta
import logging
import re
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple, Callable, Set, Coroutine
from urllib.parse import urlencode

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
from homeassistant.util import dt as dt_util

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
    HA_CONTACT_GROUP_NAME,
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
    pedestrian_relays,
    RELAY_ROLE_PEDESTRIAN,
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
    _maybe_migrate_component_folder,
)  # provides /api/akuvox_ac/ui/* + /api/AK_AC/* assets
from .ha_id import (
    ha_id_from_int,
    is_ha_id,
    normalize_ha_id,
    normalize_temp_id,
    normalize_user_id,
    temp_id_from_int,
)

HA_EVENT_ACCCESS = "akuvox_access_event"  # fired for access denied / exit override


_LOGGER = logging.getLogger(__name__)
FACE_SYNC_ERROR_THRESHOLD = 5
LEGACY_INTEGRATION_DEVICE_NAME = "Akuvox Access Control"
LEGACY_INTEGRATION_DEVICE_MODEL = "Home Assistant Integration"
OBSOLETE_ENTITY_UNIQUE_SUFFIXES: Dict[str, Set[str]] = {
    "button": {
        "access_denied",
        "refresh_caller",
        "refresh_caller_id",
    },
    "sensor": {
        "caller_id",
        "denied_access",
        "events",
        "granted_access",
        "granted_access_key_holder",
        "sync",
        "users",
    },
    "binary_sensor": {
        "denied_access",
        "granted_access",
        "granted_access_key_holder",
        "sync",
        "users",
    },
}
OBSOLETE_ENTITY_NAME_SUFFIXES: Dict[str, Set[str]] = {
    "button": {
        "access denied",
        "refresh caller",
        "refresh caller id",
    },
    "sensor": {
        "caller id",
        "denied access",
        "events",
        "granted access",
        "granted access key holder",
        "sync",
        "users",
    },
    "binary_sensor": {
        "denied access",
        "granted access",
        "granted access key holder",
        "sync",
        "users",
    },
}
CURRENT_ENTITY_UNIQUE_SUFFIXES: Set[str] = {
    "access_permitted",
    "call_end",
    "last_access_user",
    "last_accessed",
    "last_sync",
    "online",
}
CURRENT_ENTITY_NAME_SUFFIXES: Set[str] = {
    "access permitted",
    "call ended",
    "last access user",
    "last accessed",
    "last sync",
    "online",
}


def _register_admin_dashboard(hass: HomeAssistant) -> bool:
    """Register the Akuvox dashboard panel.

    Home Assistant only supports sidebar visibility for admins or everyone.
    Akuvox enforces the per-user allow-list inside its own UI/API views.
    """

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
            require_admin=False,
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


def _device_config_entry_ids(device: Any) -> Set[str]:
    raw = getattr(device, "config_entries", None)
    if raw is None:
        raw = getattr(device, "config_entry_ids", None)
    if raw is None:
        raw = getattr(device, "config_entry_id", None)
    if raw is None:
        return set()
    if isinstance(raw, str):
        return {raw}
    try:
        return {str(item) for item in raw if item not in (None, "")}
    except TypeError:
        return {str(raw)}


def _device_text_values(device: Any, *names: str) -> Set[str]:
    values: Set[str] = set()
    for name in names:
        value = getattr(device, name, None)
        if value not in (None, ""):
            values.add(str(value).strip())
    return {value for value in values if value}


def _contains_text(values: Set[str], expected: str) -> bool:
    expected_folded = str(expected or "").casefold()
    return any(str(value or "").casefold() == expected_folded for value in values)


def _device_has_legacy_identifier(device: Any) -> bool:
    identifiers = getattr(device, "identifiers", None) or set()
    try:
        iterator = iter(identifiers)
    except TypeError:
        return False
    legacy_ids = {
        DOMAIN,
        LEGACY_INTEGRATION_DEVICE_NAME,
        LEGACY_INTEGRATION_DEVICE_NAME.lower().replace(" ", "_"),
        "home_assistant_integration",
        "integration",
    }
    for identifier in iterator:
        if not isinstance(identifier, (tuple, list)) or len(identifier) < 2:
            continue
        domain, key = identifier[0], identifier[1]
        if str(domain) == DOMAIN and str(key) in legacy_ids:
            return True
    return False


def _is_legacy_integration_device(device: Any, entry_id: str) -> bool:
    if entry_id not in _device_config_entry_ids(device):
        return False
    names = _device_text_values(device, "name", "name_by_user", "original_name")
    if not _contains_text(names, LEGACY_INTEGRATION_DEVICE_NAME):
        return False
    models = _device_text_values(device, "model", "model_id")
    return _contains_text(
        models,
        LEGACY_INTEGRATION_DEVICE_MODEL,
    ) or _device_has_legacy_identifier(device)


def _registry_entries(registry: Any, attr: str) -> List[Any]:
    values = getattr(registry, attr, None)
    if values is None:
        return []
    if isinstance(values, Mapping):
        return list(values.values())
    if hasattr(values, "values"):
        return list(values.values())
    try:
        return list(values)
    except TypeError:
        return []


def _entity_config_entry_ids(entity_entry: Any) -> Set[str]:
    raw = getattr(entity_entry, "config_entry_id", None)
    if raw is None:
        raw = getattr(entity_entry, "config_entry_ids", None)
    if raw is None:
        return set()
    if isinstance(raw, str):
        return {raw}
    try:
        return {str(item) for item in raw if item not in (None, "")}
    except TypeError:
        return {str(raw)}


def _entity_domain(entity_entry: Any) -> str:
    domain = getattr(entity_entry, "domain", None)
    if domain:
        return str(domain).strip().lower()
    entity_id = str(getattr(entity_entry, "entity_id", "") or "").strip().lower()
    if "." in entity_id:
        return entity_id.split(".", 1)[0]
    return ""


def _entity_slug(entity_entry: Any) -> str:
    entity_id = str(getattr(entity_entry, "entity_id", "") or "").strip().lower()
    if "." in entity_id:
        return entity_id.split(".", 1)[1]
    return entity_id


def _entity_name_values(entity_entry: Any) -> Set[str]:
    return _device_text_values(
        entity_entry,
        "name",
        "name_by_user",
        "original_name",
        "translation_key",
    )


def _is_obsolete_akuvox_entity(entity_entry: Any, entry_id: str) -> bool:
    platform = str(getattr(entity_entry, "platform", DOMAIN) or "").strip()
    if platform and platform != DOMAIN:
        return False
    if entry_id not in _entity_config_entry_ids(entity_entry):
        return False

    domain = _entity_domain(entity_entry)
    unique_suffixes = OBSOLETE_ENTITY_UNIQUE_SUFFIXES.get(domain)
    name_suffixes = OBSOLETE_ENTITY_NAME_SUFFIXES.get(domain)
    if not unique_suffixes and not name_suffixes:
        return False

    unique_id = str(getattr(entity_entry, "unique_id", "") or "").strip()
    slug = _entity_slug(entity_entry)
    name_values = _entity_name_values(entity_entry)

    for suffix in CURRENT_ENTITY_UNIQUE_SUFFIXES:
        if unique_id == f"{entry_id}_{suffix}" or unique_id.endswith(f"_{suffix}"):
            return False
        if slug == suffix or slug.endswith(f"_{suffix}"):
            return False

    for value in name_values:
        folded = value.casefold()
        if any(
            folded == suffix or folded.endswith(f" {suffix}")
            for suffix in CURRENT_ENTITY_NAME_SUFFIXES
        ):
            return False

    for suffix in unique_suffixes or set():
        if unique_id == f"{entry_id}_{suffix}" or unique_id == suffix:
            return True

    for suffix in unique_suffixes or set():
        if slug == suffix or slug.endswith(f"_{suffix}"):
            return True

    for value in name_values:
        folded = value.casefold()
        if any(folded == suffix or folded.endswith(f" {suffix}") for suffix in name_suffixes or set()):
            return True

    return False


async def _remove_obsolete_device_entities(hass: HomeAssistant, entry: ConfigEntry) -> None:
    try:
        from homeassistant.helpers import entity_registry as er
    except Exception:
        return

    try:
        entity_registry = er.async_get(hass)
    except Exception:
        return

    for entity_entry in list(_registry_entries(entity_registry, "entities")):
        if not _is_obsolete_akuvox_entity(entity_entry, entry.entry_id):
            continue
        entity_id = getattr(entity_entry, "entity_id", None)
        if not entity_id:
            continue
        try:
            entity_registry.async_remove(entity_id)
        except Exception:
            pass


async def _remove_legacy_integration_device(hass: HomeAssistant, entry: ConfigEntry) -> None:
    try:
        from homeassistant.helpers import device_registry as dr
        from homeassistant.helpers import entity_registry as er
    except Exception:
        return

    try:
        device_registry = dr.async_get(hass)
        entity_registry = er.async_get(hass)
    except Exception:
        return

    try:
        devices = list(dr.async_entries_for_config_entry(device_registry, entry.entry_id))
    except Exception:
        devices = _registry_entries(device_registry, "devices")

    for device in devices:
        if not _is_legacy_integration_device(device, entry.entry_id):
            continue

        device_id = getattr(device, "id", None)
        if not device_id:
            continue

        try:
            entity_entries = list(
                er.async_entries_for_device(
                    entity_registry,
                    device_id,
                    include_disabled_entities=True,
                )
            )
        except TypeError:
            try:
                entity_entries = list(er.async_entries_for_device(entity_registry, device_id))
            except Exception:
                entity_entries = []
        except Exception:
            entity_entries = [
                item
                for item in _registry_entries(entity_registry, "entities")
                if getattr(item, "device_id", None) == device_id
            ]

        for entity_entry in entity_entries:
            if getattr(entity_entry, "platform", DOMAIN) != DOMAIN:
                continue
            entity_id = getattr(entity_entry, "entity_id", None)
            if not entity_id:
                continue
            try:
                entity_registry.async_remove(entity_id)
            except Exception:
                pass

        try:
            device_registry.async_remove_device(device_id)
        except Exception:
            pass


def _is_ha_group_record(record: Mapping[str, Any]) -> bool:
    if not isinstance(record, Mapping):
        return False
    group = str(record.get("Group") or record.get("group") or "").strip()
    return group.lower() == HA_CONTACT_GROUP_NAME.lower()


# ---------------------- Helpers ---------------------- #
def _now_hh_mm() -> str:
    try:
        return datetime.now().strftime("%H:%M")
    except Exception:
        return ""


def _coerce_bool(value: Any) -> Optional[bool]:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value or "").strip().lower()
    if text in ("true", "1", "yes", "on"):
        return True
    if text in ("false", "0", "no", "off"):
        return False
    return None


def _canonical_notify_user_id(value: Any) -> str:
    try:
        text = str(value or "").strip()
    except Exception:
        return ""
    if not text:
        return ""
    return normalize_user_id(text) or text


def _notify_user_matches(candidate: Any, user_id: Any) -> bool:
    candidate_text = _canonical_notify_user_id(candidate)
    user_text = _canonical_notify_user_id(user_id)
    if not candidate_text or not user_text:
        return False

    candidate_norm = normalize_user_id(candidate_text)
    user_norm = normalize_user_id(user_text)
    if candidate_norm and user_norm:
        return candidate_norm == user_norm
    return candidate_text.casefold() == user_text.casefold()


def _normalize_notify_target_services(raw: Any) -> List[str]:
    if isinstance(raw, (list, tuple, set)):
        values = raw
    elif raw in (None, ""):
        values = []
    else:
        values = [raw]

    out: List[str] = []
    seen: set[str] = set()
    for value in values:
        try:
            text = str(value or "").strip()
        except Exception:
            continue
        if text.lower().startswith("notify."):
            text = text.split(".", 1)[1].strip()
        if not text or text in seen:
            continue
        seen.add(text)
        out.append(text)
    return out


def _sync_notify_on_access_targets(
    raw_targets: Any,
    user_id: Any,
    *,
    enabled: bool,
    selected_targets: Any,
) -> Tuple[Dict[str, Dict[str, Any]], bool]:
    user_key = _canonical_notify_user_id(user_id)
    if not user_key:
        return raw_targets if isinstance(raw_targets, dict) else {}, False

    targets: Dict[str, Dict[str, Any]] = {}
    if isinstance(raw_targets, dict):
        for target, cfg in raw_targets.items():
            if not isinstance(target, str) or not target.strip():
                continue
            targets[target] = dict(cfg) if isinstance(cfg, dict) else {}

    selected = set(_normalize_notify_target_services(selected_targets)) if enabled else set()
    changed = False

    for target in selected:
        if target not in targets:
            targets[target] = {}
            changed = True

    for target, cfg in list(targets.items()):
        granted = cfg.get("granted") if isinstance(cfg.get("granted"), dict) else {}
        original_users_raw = granted.get("users") or []
        if isinstance(original_users_raw, (list, tuple, set)):
            original_users = [str(item).strip() for item in original_users_raw if str(item or "").strip()]
        elif isinstance(original_users_raw, str) and original_users_raw.strip():
            original_users = [original_users_raw.strip()]
        else:
            original_users = []

        next_users: List[str] = []
        removed_user = False
        for existing in original_users:
            if _notify_user_matches(existing, user_key):
                removed_user = True
                continue
            if not any(_notify_user_matches(existing, kept) for kept in next_users):
                next_users.append(existing)

        should_have_user = enabled and target in selected
        added_user = False
        if should_have_user and not any(_notify_user_matches(existing, user_key) for existing in next_users):
            next_users.append(user_key)
            added_user = True

        if removed_user or added_user:
            granted["users"] = next_users
            granted["specific"] = bool(next_users)
            cfg["granted"] = granted
            targets[target] = cfg
            changed = True
        elif should_have_user and not bool(granted.get("specific")):
            granted["specific"] = True
            cfg["granted"] = granted
            targets[target] = cfg
            changed = True

    return targets, changed


def _active_notification_user_ids(users_store: Any) -> Set[str]:
    """Return user IDs still present in the local user table."""

    if not users_store or not hasattr(users_store, "all"):
        return set()
    try:
        users = users_store.all() or {}
    except Exception:
        return set()
    if not isinstance(users, Mapping):
        return set()

    active: Set[str] = set()
    for key, profile in users.items():
        canonical = normalize_user_id(key)
        if not canonical:
            continue
        if not isinstance(profile, Mapping):
            continue
        if _profile_is_empty_reserved(profile):
            continue
        status = str(profile.get("status") or "").strip().lower()
        if status == "deleted":
            continue
        active.add(canonical)
    return active


def _prune_notify_targets_to_users(
    raw_targets: Any,
    active_user_ids: Set[str],
) -> Tuple[Dict[str, Dict[str, Any]], bool]:
    targets: Dict[str, Dict[str, Any]] = {}
    if isinstance(raw_targets, dict):
        for target, cfg in raw_targets.items():
            if not isinstance(target, str) or not target.strip():
                continue
            targets[target] = dict(cfg) if isinstance(cfg, dict) else {}

    changed = False
    for target, cfg in list(targets.items()):
        granted = cfg.get("granted") if isinstance(cfg.get("granted"), dict) else {}
        original_users_raw = granted.get("users") or []
        if isinstance(original_users_raw, (list, tuple, set)):
            original_users = [
                str(item).strip() for item in original_users_raw if str(item or "").strip()
            ]
        elif isinstance(original_users_raw, str) and original_users_raw.strip():
            original_users = [original_users_raw.strip()]
        else:
            original_users = []

        next_users: List[str] = []
        seen: Set[str] = set()
        for user in original_users:
            canonical = normalize_user_id(user)
            if not canonical or canonical not in active_user_ids or canonical in seen:
                changed = True
                continue
            if canonical != user:
                changed = True
            seen.add(canonical)
            next_users.append(canonical)

        if next_users != original_users:
            changed = True
            granted["users"] = next_users
            if not next_users:
                granted["specific"] = False
            cfg["granted"] = granted
            targets[target] = cfg

    return targets, changed


async def _set_notify_on_access_for_user(
    settings_store: Any,
    user_id: Any,
    *,
    enabled: bool,
    selected_targets: Any,
) -> None:
    if not settings_store or not hasattr(settings_store, "get_alert_targets"):
        return
    if not hasattr(settings_store, "set_alert_targets"):
        return
    try:
        targets = settings_store.get_alert_targets()
        updated, changed = _sync_notify_on_access_targets(
            targets,
            user_id,
            enabled=enabled,
            selected_targets=selected_targets,
        )
        if changed:
            await settings_store.set_alert_targets(updated)
    except Exception as err:
        _LOGGER.debug(
            "Failed to update notify-on-access targets for %s: %s",
            _canonical_notify_user_id(user_id) or user_id,
            err,
        )


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


async def _store_device_user_ids(storage: Any, users: Iterable[Dict[str, Any]]) -> None:
    if not storage:
        return
    data = getattr(storage, "data", None)
    if not isinstance(data, dict):
        return
    mapping = data.get("user_ids")
    if not isinstance(mapping, dict):
        mapping = {}
        data["user_ids"] = mapping
    changed = False
    for record in users or []:
        if not isinstance(record, dict):
            continue
        ha_ref = record.get("UserID") or record.get("UserId") or record.get("Name")
        if ha_ref in (None, ""):
            continue
        canonical = normalize_user_id(ha_ref) or str(ha_ref).strip()
        if not canonical:
            continue
        device_id = str(record.get("ID") or "").strip()
        if not device_id:
            continue
        if mapping.get(canonical) != device_id:
            mapping[canonical] = device_id
            changed = True
    if changed:
        try:
            await storage.async_save()
        except Exception:
            pass


def _device_user_id(storage: Any, ha_key: str) -> Optional[str]:
    if not storage:
        return None
    data = getattr(storage, "data", None)
    if not isinstance(data, dict):
        return None
    mapping = data.get("user_ids")
    if not isinstance(mapping, dict):
        return None
    canonical = normalize_user_id(ha_key) or str(ha_key or "").strip()
    if not canonical:
        return None
    device_id = mapping.get(canonical)
    if device_id in (None, ""):
        return None
    return str(device_id).strip()


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
    name_norm = normalize_user_id(name_text) or name_text
    user_norm = normalize_user_id(user_id_text) or user_id_text
    return name_norm == user_norm


def _user_id_sort_key(value: Any) -> Tuple[int, int, str]:
    """Sort IDs by prefix family and numeric suffix (HA first, then TMP)."""

    text = normalize_user_id(value) or str(value or "").strip()
    if not text:
        return (3, 0, "")

    match = re.match(r"^(HA|TMP)-?(\d+)$", text, flags=re.IGNORECASE)
    if not match:
        return (3, 0, text.lower())

    family = match.group(1).upper()
    priority = 0 if family == "HA" else 1
    return (priority, int(match.group(2)), text.upper())


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


def _normalize_temp_datetime(value: Any) -> Optional[str]:
    """Normalize temporary access datetimes to ISO format or clear them."""

    if value is None:
        return None

    if isinstance(value, datetime):
        normalized = value
    elif isinstance(value, date):
        normalized = datetime.combine(value, datetime.min.time())
    elif isinstance(value, str):
        text = value.strip()
        if not text:
            return ""
        normalized = dt_util.parse_datetime(text)
        if normalized is None:
            try:
                parsed_date = datetime.strptime(text.split("T", 1)[0], "%Y-%m-%d")
            except ValueError:
                return ""
            normalized = datetime.combine(parsed_date.date(), datetime.min.time())
    else:
        return ""

    if normalized.tzinfo is None:
        normalized = normalized.replace(tzinfo=dt_util.DEFAULT_TIME_ZONE)

    return normalized.isoformat()


def _parse_temp_datetime(value: Any) -> Optional[datetime]:
    """Parse a temporary access datetime string into a timezone-aware datetime."""

    if value is None:
        return None

    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, date):
        parsed = datetime.combine(value, datetime.min.time())
    elif isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        parsed = dt_util.parse_datetime(text)
        if parsed is None:
            try:
                parsed_date = datetime.strptime(text.split("T", 1)[0], "%Y-%m-%d")
            except ValueError:
                return None
            parsed = datetime.combine(parsed_date.date(), datetime.min.time())
    else:
        return None

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt_util.DEFAULT_TIME_ZONE)

    return parsed


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
    "FaceRegisterStatus",
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


def _pedestrian_access_from_record(record: Dict[str, Any]) -> Optional[bool]:
    if not isinstance(record, dict):
        return None
    if "pedestrian_access" in record:
        flag = _normalize_boolish(record.get("pedestrian_access"))
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


def _face_asset_path(
    hass: HomeAssistant,
    user_id: str,
    reference: Optional[Any] = None,
) -> Optional[Path]:
    """Return the best local face image path for a user/reference."""

    names: List[str] = []
    if reference not in (None, ""):
        try:
            name = face_filename_from_reference(str(reference), user_id)
        except Exception:
            name = ""
        if name:
            names.append(name)

    clean_user_id = str(user_id or "").strip()
    if clean_user_id:
        for ext in FACE_FILE_EXTENSIONS:
            names.append(f"{clean_user_id}.{ext}")

    unique_names = [name for name in dict.fromkeys(names) if str(name or "").strip()]
    if not unique_names:
        return None

    search_paths: List[Path] = []
    try:
        search_paths.append(face_storage_dir(hass))
    except Exception:
        pass

    search_paths.append(Path(__file__).parent / "www" / "FaceData")

    try:
        search_paths.append(Path(hass.config.path("www")) / "AK_Access_ctrl" / "FaceData")
    except Exception:
        pass

    for base in search_paths:
        try:
            resolved_base = base.resolve()
        except Exception:
            continue
        for name in unique_names:
            try:
                candidate = (resolved_base / Path(name).name).resolve()
                candidate.relative_to(resolved_base)
            except Exception:
                continue
            if candidate.is_file():
                return candidate

    return None


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


def _face_reference_is_device_import(reference: Any) -> bool:
    if reference in (None, ""):
        return False
    text = str(reference or "").strip().replace("\\", "/").lower()
    return text.startswith("/mnt/face/") or text.startswith("mnt/face/")


def _face_import_filename_from_sources(
    ha_key: str,
    *sources: Optional[Dict[str, Any]],
) -> str:
    for source in sources:
        if not isinstance(source, dict):
            continue
        import_file = source.get("importFile") or source.get("ImportFile")
        if isinstance(import_file, dict):
            for key in ("fileName", "filename", "name", "FileName"):
                value = import_file.get(key)
                if value not in (None, ""):
                    name = Path(str(value)).name
                    if name:
                        return name
        for key in _FACE_FILENAME_KEYS:
            value = source.get(key)
            if value not in (None, ""):
                name = Path(str(value)).name
                if name:
                    return name

    for source in sources:
        if not isinstance(source, dict):
            continue
        for key in _FACE_URL_KEYS:
            value = source.get(key)
            if value not in (None, "") and _face_reference_is_device_import(value):
                try:
                    name = face_filename_from_reference(str(value), ha_key)
                except Exception:
                    name = Path(str(value)).name
                if name:
                    return Path(str(name)).name

    return ""


def _apply_face_import_fields(
    payload: Dict[str, Any],
    *,
    ha_key: str,
    sources: Tuple[Optional[Dict[str, Any]], ...],
) -> bool:
    filename = _face_import_filename_from_sources(ha_key, payload, *sources)
    if not filename:
        return False

    payload["FaceFileName"] = filename
    payload.pop("importFile", None)
    payload.pop("ImportFile", None)
    payload.pop("FaceUrl", None)
    payload.pop("FaceURL", None)
    return True


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

    raw_face_url = _extract_first(_FACE_URL_KEYS)
    face_url = raw_face_url
    if not face_url:
        face_url = _extract_first(_FACE_FILENAME_KEYS)

    import_linked = _apply_face_import_fields(
        payload,
        ha_key=ha_key,
        sources=sources,
    )

    if face_url and not import_linked:
        payload["FaceUrl"] = str(face_url)
    elif import_linked:
        payload.pop("FaceUrl", None)
        payload.pop("FaceURL", None)

    for key in _FACE_FILENAME_KEYS:
        if key != "FaceFileName":
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

    if import_linked:
        payload["FaceRegister"] = 1
    elif face_url and register_value != "1":
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

    for key in ("ID", "Id", "id", "Schedule-Relay"):
        cleaned.pop(key, None)
    for key in ("DoorNum", "ScheduleID", "PriorityCall", "Type"):
        cleaned.pop(key, None)

    source_tuple: Tuple[Optional[Dict[str, Any]], ...] = sources or (payload,)

    _ensure_face_payload_fields(
        cleaned,
        ha_key=str(cleaned.get("UserID") or canonical_key or ""),
        sources=source_tuple,
    )

    cleaned.setdefault("DialAccount", "0")
    cleaned.setdefault("Group", HA_CONTACT_GROUP_NAME)
    cleaned.setdefault("AnalogSystem", "0")
    cleaned.setdefault("AnalogNumber", "")
    cleaned.setdefault("AnalogReplace", "")
    cleaned.setdefault("AnalogProxyAddress", "")

    return cleaned


def _prepare_user_set_payload(
    ha_key: str,
    desired: Dict[str, Any],
    existing: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Return a device-friendly payload for user.set."""

    def _normalize_fixed_plate(value: Any) -> List[Dict[str, Any]]:
        items: List[Dict[str, Any]] = []
        if isinstance(value, list):
            for entry in value:
                items.append(entry if isinstance(entry, dict) else {})
        while len(items) < 5:
            items.append({})
        return items[:5]

    def _remap_user_set_keys(source: Dict[str, Any]) -> Dict[str, Any]:
        mapping = {
            "BLE_AuthCode": "BLEAuthCode",
            "FaceRegister": "FaceRegisterStatus",
            "Priority": "PriorityCall",
            "ScheduleRelay": "Schedule-Relay",
            "Id": "ID",
            "id": "ID",
        }
        forbidden = set(mapping.keys())
        cleaned: Dict[str, Any] = {}
        for key, value in source.items():
            if key in forbidden:
                key = mapping[key]
            if key in ("ScheduleRelay", "BLE_AuthCode", "FaceRegister", "Priority"):
                continue
            cleaned[key] = value
        return cleaned

    payload: Dict[str, Any] = {
        "UserID": str(ha_key or "").strip(),
        "Name": "",
        "Group": "Default",
        "DoorNum": "1",
        "Schedule": ["1001"],
        "Schedule-Relay": "1001-1;",
        "PrivatePIN": "",
        "AnalogNumber": "",
        "AnalogProxyAddress": "",
        "AnalogReplace": "",
        "AnalogSystem": "0",
        "AuthMode": "0",
        "BLE_KEY_ID": "-1",
        "BLEAuthCode": "",
        "BLE_Expired": "",
        "BLE_Status": "",
        "BleKeyDelete": "0",
        "C4EventNo": "0",
        "CardCode": "",
        "ContactID": "-1",
        "DialAccount": "0",
        "FaceRegisterStatus": "0",
        "LiftFloorNum": "0",
        "PhoneNum": "",
        "PriorityCall": "0",
        "Source": "Local",
        "WebRelay": "0",
        "LicensePlate": [{}, {}, {}, {}, {}],
        "LicensePlateTime": [{}, {}, {}, {}, {}],
    }
    base_keys = set(payload.keys()) | {"ID", "FaceUrl", "FaceFileName", "importFile"}

    if isinstance(existing, dict):
        payload.update({k: v for k, v in _remap_user_set_keys(existing).items() if v is not None})
    if isinstance(desired, dict):
        payload.update({k: v for k, v in _remap_user_set_keys(desired).items() if v is not None})

    for key in list(payload.keys()):
        if key not in base_keys:
            payload.pop(key, None)

    priority_value = str(payload.get("PriorityCall") or "").strip()
    if not priority_value or priority_value == "-1":
        payload["PriorityCall"] = "0"

    if ha_key:
        payload["UserID"] = str(ha_key)

    face_url_value: Optional[str] = None
    raw_face_url_value: Optional[str] = None
    face_url_present = False
    face_filename_value = _face_import_filename_from_sources(ha_key, desired, existing, payload)
    for source in (desired, existing, payload):
        if not isinstance(source, dict):
            continue
        for key in _FACE_URL_KEYS:
            if key not in source:
                continue
            face_url_present = True
            raw_value = source.get(key)
            if raw_value in (None, ""):
                continue
            text = str(raw_value).strip()
            if text:
                face_url_value = text
                raw_face_url_value = text
                break
        if face_url_value is not None:
            break

    if face_filename_value:
        payload["FaceFileName"] = face_filename_value
        payload.pop("importFile", None)
        payload.pop("ImportFile", None)

    if face_url_value is not None and not (
        face_filename_value and _face_reference_is_device_import(raw_face_url_value)
    ):
        payload["FaceUrl"] = face_url_value
    elif face_filename_value and _face_reference_is_device_import(raw_face_url_value):
        payload.pop("FaceUrl", None)
    elif face_url_present:
        payload["FaceUrl"] = ""

    for key in (*_FACE_URL_KEYS, *_FACE_FILENAME_KEYS):
        if key not in {"FaceUrl", "FaceFileName"}:
            payload.pop(key, None)

    for key in ("ScheduleID", "Type", "Id", "id"):
        payload.pop(key, None)

    if "FaceRegisterStatus" not in payload:
        face_flag = _face_flag_from_record(desired or {})
        if face_flag is None:
            face_flag = _face_flag_from_record(existing or {})
        if face_flag is not None:
            payload["FaceRegisterStatus"] = "1" if face_flag else "0"

    if str(payload.get("FaceUrl") or "").strip() and str(payload.get("FaceRegisterStatus") or "").strip() != "1":
        payload["FaceRegisterStatus"] = "1"

    if "FaceRegisterStatus" in payload:
        face_status_flag = _normalize_boolish(payload.get("FaceRegisterStatus"))
        if face_status_flag is None:
            payload["FaceRegisterStatus"] = str(payload.get("FaceRegisterStatus") or "")
        else:
            payload["FaceRegisterStatus"] = "1" if face_status_flag else "0"

    payload["LicensePlate"] = _normalize_fixed_plate(payload.get("LicensePlate"))
    payload["LicensePlateTime"] = _normalize_fixed_plate(payload.get("LicensePlateTime"))

    return payload


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
    pedestrian_access_flag = _pedestrian_access_from_record(profile)
    if pedestrian_access_flag is None:
        pedestrian_access_flag = _pedestrian_access_from_record(local)
    pedestrian_access = True if pedestrian_access_flag is None else bool(pedestrian_access_flag)
    explicit_id = str(profile.get("schedule_id") or "").strip()
    if explicit_id and explicit_id.isdigit():
        mapped_id = sched_map.get(schedule_lower, "")
        if mapped_id and mapped_id != explicit_id:
            explicit_id = ""

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
    pedestrian_digits = pedestrian_relays(relay_roles)
    pedestrian_only = pedestrian_access and any(
        role == RELAY_ROLE_PEDESTRIAN for role in relay_roles.values()
    )
    relay_suffix = relay_suffix_for_user(
        relay_roles,
        key_holder,
        pedestrian_access,
        device_type_raw,
    )
    if not relay_suffix and not pedestrian_only:
        relay_suffix = "1"
    if not schedule_id:
        schedule_id = "1001"
    schedule_relay = f"{schedule_id}-{relay_suffix}".rstrip(";") + ";"

    device_type = str(device_type_raw or "").strip().lower()
    is_keypad = device_type == "keypad"
    root = hass.data.get(DOMAIN, {}) if hass else {}
    settings_store = root.get("settings_store")
    anpr_enabled = False
    if settings_store and hasattr(settings_store, "get_credential_prompts"):
        try:
            anpr_enabled = bool(settings_store.get_credential_prompts().get("anpr"))
        except Exception:
            anpr_enabled = False

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

    def _normalise_license_plate() -> List[Dict[str, Any]]:
        if not anpr_enabled:
            return []
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
                    if len(result) >= 5:
                        break
        if len(result) > 5:
            result = result[:5]
        return result

    name_value = profile.get("name")
    if name_value in (None, ""):
        name_value = local.get("Name") or ha_key
    name = str(name_value)

    group_value = HA_CONTACT_GROUP_NAME

    door_digit_fallback = (
        pedestrian_digits[0]
        if pedestrian_only and pedestrian_digits
        else door_digits[0] if door_digits else None
    )
    door_num = _string_or_default(
        profile.get("door_num"),
        profile.get("DoorNum"),
        local.get("DoorNum"),
        door_digit_fallback,
        default="1",
    )

    lift_floor = _string_or_default(
        profile.get("lift_floor_num"),
        profile.get("lift_floor"),
        local.get("LiftFloorNum"),
        default="0",
    )

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
        paused_profile = _coerce_bool(profile.get("paused")) is True
        if paused_profile:
            # Paused users should not be callable from the intercom.
            # Force an explicit empty PhoneNum so user.set clears any
            # previously configured phone target on the device.
            desired["PhoneNum"] = ""
        else:
            phone_value = profile.get("phone")
            if phone_value in (None, ""):
                phone_value = local.get("PhoneNum") or local.get("Phone")
            if phone_value not in (None, ""):
                desired["PhoneNum"] = str(phone_value)

        face_url = profile.get("face_url") or local.get("FaceUrl") or local.get("FaceURL")
        face_asset_exists: Optional[bool] = None
        if face_url in (None, ""):
            try:
                face_asset_exists = _face_asset_exists(hass, ha_key)
            except Exception:
                face_asset_exists = None
            if face_asset_exists:
                face_url = f"{face_root_base}/{ha_key}.jpg"
        else:
            face_url_str = str(face_url)
            if face_url_str.startswith(face_root_base):
                try:
                    face_asset_exists = _face_asset_exists(hass, ha_key)
                except Exception:
                    face_asset_exists = None
                if face_asset_exists is False:
                    face_url = None

        if face_url not in (None, ""):
            face_url_str = str(face_url)
            desired["FaceUrl"] = face_url_str
            # Keep face registration enabled whenever a face asset reference is
            # present. Some firmwares temporarily report face state as pending
            # after enrollment; forcing "0" during a later sync clears the face.
            desired["FaceRegister"] = 1

            face_active: Optional[bool] = _face_flag_from_record(profile)
            if face_active is None:
                face_active = _face_flag_from_record(local)
            if face_active is None:
                if face_asset_exists is not None:
                    face_active = face_asset_exists
                else:
                    try:
                        face_active = _face_asset_exists(hass, ha_key)
                    except Exception:
                        face_active = None
        else:
            desired["FaceRegister"] = "0"

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


def _integrity_field_differences(
    local: Dict[str, Any],
    expected: Dict[str, Any],
    *,
    include_face: bool = True,
) -> List[str]:
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

    if include_face:
        expected_face = _face_flag_from_record(expected)
        actual_face = _face_flag_from_record(local)
        if expected_face is True and actual_face is not True:
            diffs.append("face status")

    return diffs


def _record_matches_desired_fields(local: Dict[str, Any], desired: Dict[str, Any]) -> bool:
    """Return True when ``local`` already satisfies the desired payload values."""

    if not isinstance(local, dict) or not isinstance(desired, dict):
        return False

    def _text(value: Any) -> str:
        return str(value or "").strip()

    def _face_flag(record: Dict[str, Any]) -> Optional[bool]:
        raw = record.get("FaceRegister")
        if raw in (None, ""):
            raw = record.get("FaceRegisterStatus")
        return _normalize_boolish(raw)

    for key, desired_value in desired.items():
        if key == "FaceRegister":
            expected_face = _normalize_boolish(desired_value)
            actual_face = _face_flag(local)
            if expected_face is True and actual_face is not True:
                return False
            if expected_face is False and actual_face is True:
                return False
            continue

        if key in (*_FACE_URL_KEYS, *_FACE_FILENAME_KEYS, "importFile"):
            expected_face = _face_flag_from_record(desired)
            actual_face = _face_flag(local)
            if expected_face is True and actual_face is True:
                continue

        local_value = local.get(key)
        if _text(local_value) != _text(desired_value):
            return False

    return True


def _schedule_times_out_of_order(spec: Mapping[str, Any]) -> bool:
    start = AkuvoxSchedulesStore._time_to_minutes(spec.get("start"))
    end = AkuvoxSchedulesStore._time_to_minutes(spec.get("end"))
    if start is None or end is None:
        return False
    return start > end


def _normalize_schedule_for_integrity(
    schedules_store: AkuvoxSchedulesStore,
    name: str,
    spec: Optional[Mapping[str, Any]],
) -> Dict[str, Any]:
    payload = dict(spec) if isinstance(spec, Mapping) else {}
    return schedules_store._normalize_payload(name, payload)


def _profile_is_empty_reserved(profile: Mapping[str, Any]) -> bool:
    """Return True for empty pending/reserved profiles created by ID reservations."""

    if not isinstance(profile, Mapping):
        return False
    status = str(profile.get("status") or "").strip().lower()
    if status not in ("pending", "reserved"):
        return False
    has_core = any(bool(profile.get(k)) for k in ("name", "pin", "phone"))
    return not has_core


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
            "type": "1",
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
            daily_range = payload.get("daily") or payload.get("Daily")
            if (raw_start is None or raw_end is None) and isinstance(daily_range, str) and "-" in daily_range:
                parts = [chunk.strip() for chunk in daily_range.split("-", 1)]
                if len(parts) == 2:
                    if raw_start is None and parts[0]:
                        raw_start = parts[0]
                    if raw_end is None and parts[1]:
                        raw_end = parts[1]

            if raw_start is not None:
                normalized["start"] = self._clean_time(raw_start, default=normalized["start"])
            if raw_end is not None:
                normalized["end"] = self._clean_time(raw_end, default=normalized["end"])

            if "type" in payload or "Type" in payload:
                normalized["type"] = str(payload.get("type") or payload.get("Type") or "1")

            if "date_start" in payload or "DateStart" in payload:
                normalized["date_start"] = str(payload.get("date_start") or payload.get("DateStart") or "").strip()
            if "date_end" in payload or "DateEnd" in payload:
                normalized["date_end"] = str(payload.get("date_end") or payload.get("DateEnd") or "").strip()

            raw_days = payload.get("days")
            week_text = str(payload.get("Week") or payload.get("week") or "").strip()
            if week_text:
                week_map = {
                    "0": "sun",
                    "1": "mon",
                    "2": "tue",
                    "3": "wed",
                    "4": "thu",
                    "5": "fri",
                    "6": "sat",
                }
                for ch in week_text:
                    mapped = week_map.get(ch)
                    if mapped:
                        days_selected.add(mapped)
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
            canonical = normalize_user_id(key)
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
        canonical = normalize_user_id(key)
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

    def next_free_temp_id(self, *, blocked: Optional[List[str]] = None) -> str:
        used: set[str] = set()
        users = self.data.get("users") or {}
        if isinstance(users, dict):
            for key, profile in users.items():
                canonical = normalize_temp_id(key)
                if not canonical:
                    continue
                if isinstance(profile, dict):
                    status = str(profile.get("status") or "").strip().lower()
                    if status == "deleted":
                        continue
                used.add(canonical)
        if blocked:
            for candidate in blocked:
                canonical = normalize_temp_id(candidate)
                if canonical:
                    used.add(canonical)

        n = 1
        while True:
            candidate = temp_id_from_int(n)
            if candidate not in used:
                return candidate
            n += 1

    def reserve_id(self, ha_id: str):
        canonical = normalize_ha_id(ha_id)
        if not canonical:
            raise ValueError(f"Invalid HA id: {ha_id}")
        self.data["users"].setdefault(canonical, {})

    def reserve_temp_id(self, temp_id: str):
        canonical = normalize_temp_id(temp_id)
        if not canonical:
            raise ValueError(f"Invalid temporary id: {temp_id}")
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
        pedestrian_access: Optional[bool] = None,
        access_level: Optional[str] = None,
        schedule_id: Optional[str] = None,  # allow explicit schedule ID (1001/1002/1003/…)
        access_start: Optional[str] = None,
        access_end: Optional[str] = None,
        source: Optional[str] = None,
        license_plate: Optional[List[Any]] = None,
        exit_permission: Optional[str] = None,
        face_error_count: Optional[int] = None,
        temporary: Optional[bool] = None,
        temporary_one_time: Optional[bool] = None,
        temporary_expires_at: Optional[str] = None,
        temporary_used_at: Optional[str] = None,
        temporary_created_at: Optional[str] = None,
        paused: Optional[bool] = None,
        paused_schedule_id: Optional[str] = None,
        paused_schedule_name: Optional[str] = None,
    ):
        canonical = normalize_user_id(key) or str(key)
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
        if pedestrian_access is not None:
            u["pedestrian_access"] = bool(pedestrian_access)
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
        if face_error_count is not None:
            try:
                count = int(face_error_count)
            except (TypeError, ValueError):
                count = 0
            if count > 0:
                u["face_error_count"] = count
            else:
                u.pop("face_error_count", None)
        if temporary is not None:
            if temporary:
                u["temporary"] = True
            else:
                u.pop("temporary", None)
        if temporary_one_time is not None:
            if temporary_one_time:
                u["temporary_one_time"] = True
            else:
                u.pop("temporary_one_time", None)
        if temporary_expires_at is not None:
            normalized_expiry = _normalize_temp_datetime(temporary_expires_at)
            if normalized_expiry:
                u["temporary_expires_at"] = normalized_expiry
            else:
                u.pop("temporary_expires_at", None)
        if temporary_used_at is not None:
            normalized_used = _normalize_temp_datetime(temporary_used_at)
            if normalized_used:
                u["temporary_used_at"] = normalized_used
            else:
                u.pop("temporary_used_at", None)
        if temporary_created_at is not None:
            normalized_created = _normalize_temp_datetime(temporary_created_at)
            if normalized_created:
                u["temporary_created_at"] = normalized_created
            else:
                u.pop("temporary_created_at", None)
        if paused is not None:
            if paused:
                u["paused"] = True
            else:
                u.pop("paused", None)
                u.pop("paused_schedule_id", None)
                u.pop("paused_schedule_name", None)
        if paused_schedule_id is not None:
            cleaned = str(paused_schedule_id or "").strip()
            if cleaned:
                u["paused_schedule_id"] = cleaned
            else:
                u.pop("paused_schedule_id", None)
        if paused_schedule_name is not None:
            cleaned = str(paused_schedule_name or "").strip()
            if cleaned:
                u["paused_schedule_name"] = cleaned
            else:
                u.pop("paused_schedule_name", None)
        await self.async_save()

    async def delete(self, key: str):
        raw = str(key or "").strip()
        if not raw:
            return

        users = self.data.get("users")
        if not isinstance(users, dict):
            users = {}
            self.data["users"] = users

        canonical = normalize_user_id(raw)
        removal_keys = {raw}
        if canonical:
            removal_keys.add(canonical)

        for stored_key in list(users.keys()):
            if stored_key in removal_keys:
                users.pop(stored_key, None)
                continue
            if canonical and normalize_user_id(stored_key) == canonical:
                users.pop(stored_key, None)
        await self.async_save()


class AkuvoxSettingsStore(Store):
    DEFAULT_HEALTH_SECONDS = DEFAULT_POLL_INTERVAL
    MIN_HEALTH_SECONDS = MIN_HEALTH_CHECK_INTERVAL
    MAX_HEALTH_SECONDS = MAX_HEALTH_CHECK_INTERVAL
    MIN_HACS_AUTO_UPDATE_HOURS = 1
    MAX_HACS_AUTO_UPDATE_HOURS = 168

    DEFAULT_INTEGRITY_MINUTES = 15
    DEFAULT_FACE_INTEGRITY_ENABLED = True
    DEFAULT_CREDENTIAL_PROMPTS = {
        "code": True,
        "token": True,
        "anpr": False,
        "face": True,
        "phone": True,
    }
    DEFAULT_HACS_AUTO_UPDATE = {
        "enabled": False,
        "interval_hours": 24,
        "check_time": "02:00",
        "auto_install": False,
        "restart_after_install": False,
        "update_entity": "",
        "backup": False,
        "last_checked": None,
        "last_installed": None,
        "last_result": "disabled",
        "last_error": None,
        "last_entity_id": None,
        "installed_version": None,
        "latest_version": None,
        "pending_version": None,
        "pending_version_full": None,
        "restart_scheduled_for": None,
    }
    DEFAULT_EXPIRY_REMINDERS = {
        "last_sent": {},
    }

    def __init__(self, hass: HomeAssistant):
        super().__init__(hass, 1, f"{DOMAIN}_settings.json")
        self.data: Dict[str, Any] = {
            "auto_sync_time": None,
            "auto_reboot": {"time": None, "days": []},
            "integrity_interval_minutes": self.DEFAULT_INTEGRITY_MINUTES,
            "face_integrity_enabled": self.DEFAULT_FACE_INTEGRITY_ENABLED,
            "auto_sync_delay_minutes": 15,
            "alerts": {"targets": {}},
            "diagnostics_history_limit": DEFAULT_DIAGNOSTICS_HISTORY_LIMIT,
            "health_check_interval_seconds": self.DEFAULT_HEALTH_SECONDS,
            "credential_prompts": dict(self.DEFAULT_CREDENTIAL_PROMPTS),
            "access_history_limit": DEFAULT_ACCESS_HISTORY_LIMIT,
            "hacs_auto_update": dict(self.DEFAULT_HACS_AUTO_UPDATE),
            "dashboard_access": {"allowed_user_ids": []},
            "expiry_reminders": {"last_sent": {}},
        }

    async def async_load(self):
        x = await super().async_load()
        if x:
            base = dict(self.data)
            base.update(x)
            self.data = base

        if not isinstance(self.data.get("auto_reboot"), dict):
            self.data["auto_reboot"] = {"time": None, "days": []}

        delay = self.data.get("auto_sync_delay_minutes", 15)
        try:
            delay = int(delay)
        except Exception:
            delay = 15
        delay = max(5, min(60, delay))
        self.data["auto_sync_delay_minutes"] = delay

        integ = self.data.get("integrity_interval_minutes", self.DEFAULT_INTEGRITY_MINUTES)
        try:
            integ = int(integ)
        except Exception:
            integ = self.DEFAULT_INTEGRITY_MINUTES
        self.data["integrity_interval_minutes"] = int(integ)

        face_integrity = self.data.get("face_integrity_enabled", self.DEFAULT_FACE_INTEGRITY_ENABLED)
        if not isinstance(face_integrity, bool):
            face_integrity = self.DEFAULT_FACE_INTEGRITY_ENABLED
        self.data["face_integrity_enabled"] = face_integrity

        alerts = self.data.get("alerts")
        if not isinstance(alerts, dict):
            alerts = {}
        targets = alerts.get("targets") if isinstance(alerts, dict) else {}
        alerts["targets"] = self._sanitize_alert_targets(targets)
        self.data["alerts"] = alerts
        self.data["expiry_reminders"] = self._sanitize_expiry_reminders(
            self.data.get("expiry_reminders")
        )

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
        self.data["hacs_auto_update"] = self._sanitize_hacs_auto_update(
            self.data.get("hacs_auto_update")
        )
        self.data["dashboard_access"] = self._sanitize_dashboard_access(
            self.data.get("dashboard_access")
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

    def _sanitize_dashboard_access(self, raw: Any) -> Dict[str, Any]:
        allowed_raw: Any = []
        if isinstance(raw, dict):
            allowed_raw = raw.get("allowed_user_ids") or raw.get("users") or []
        elif isinstance(raw, (list, tuple, set)):
            allowed_raw = raw

        allowed: List[str] = []
        seen: Set[str] = set()
        if isinstance(allowed_raw, str):
            allowed_iterable: Iterable[Any] = [allowed_raw]
        elif isinstance(allowed_raw, (list, tuple, set)):
            allowed_iterable = allowed_raw
        else:
            allowed_iterable = []

        for item in allowed_iterable:
            text = str(item or "").strip()
            if not text or text in seen:
                continue
            seen.add(text)
            allowed.append(text)

        return {"allowed_user_ids": allowed}

    def get_dashboard_access(self) -> Dict[str, Any]:
        return self._sanitize_dashboard_access(self.data.get("dashboard_access"))

    async def set_dashboard_access(self, config: Any) -> Dict[str, Any]:
        sanitized = self._sanitize_dashboard_access(config)
        self.data["dashboard_access"] = sanitized
        await self.async_save()
        return dict(sanitized)

    def _normalize_hacs_auto_update_hours(self, hours: Any) -> int:
        try:
            value = int(hours)
        except Exception:
            value = self.DEFAULT_HACS_AUTO_UPDATE["interval_hours"]
        if value < self.MIN_HACS_AUTO_UPDATE_HOURS:
            return self.MIN_HACS_AUTO_UPDATE_HOURS
        if value > self.MAX_HACS_AUTO_UPDATE_HOURS:
            return self.MAX_HACS_AUTO_UPDATE_HOURS
        return value

    def _normalize_hacs_auto_update_time(self, value: Any) -> str:
        text = str(value or "").strip()
        match = re.fullmatch(r"(\d{1,2}):(\d{2})", text)
        if not match:
            return str(self.DEFAULT_HACS_AUTO_UPDATE["check_time"])
        hour = int(match.group(1))
        minute = int(match.group(2))
        if hour < 0 or hour > 23 or minute < 0 or minute > 59:
            return str(self.DEFAULT_HACS_AUTO_UPDATE["check_time"])
        return f"{hour:02d}:{minute:02d}"

    def _sanitize_hacs_auto_update(self, raw: Any) -> Dict[str, Any]:
        cfg = dict(self.DEFAULT_HACS_AUTO_UPDATE)
        if not isinstance(raw, dict):
            return cfg

        enabled = _coerce_bool(raw.get("enabled"))
        backup = _coerce_bool(raw.get("backup"))
        auto_install = _coerce_bool(raw.get("auto_install"))
        restart_after_install = _coerce_bool(raw.get("restart_after_install"))
        cfg["enabled"] = bool(enabled) if enabled is not None else False
        cfg["backup"] = bool(backup) if backup is not None else False
        cfg["auto_install"] = bool(auto_install) if auto_install is not None else False
        cfg["restart_after_install"] = (
            bool(restart_after_install) if restart_after_install is not None else False
        )
        cfg["interval_hours"] = self._normalize_hacs_auto_update_hours(
            raw.get("interval_hours")
        )
        cfg["check_time"] = self._normalize_hacs_auto_update_time(raw.get("check_time"))
        cfg["update_entity"] = str(raw.get("update_entity") or "").strip()

        for key in (
            "last_checked",
            "last_installed",
            "last_result",
            "last_error",
            "last_entity_id",
            "installed_version",
            "latest_version",
            "pending_version",
            "pending_version_full",
            "restart_scheduled_for",
        ):
            value = raw.get(key)
            cfg[key] = str(value).strip() if value not in (None, "") else None
        if not cfg["last_result"]:
            cfg["last_result"] = "enabled" if cfg["enabled"] else "disabled"
        return cfg

    def get_hacs_auto_update(self) -> Dict[str, Any]:
        return self._sanitize_hacs_auto_update(self.data.get("hacs_auto_update"))

    async def set_hacs_auto_update(self, config: Any) -> Dict[str, Any]:
        current = self.get_hacs_auto_update()
        if isinstance(config, dict):
            current.update(config)
        sanitized = self._sanitize_hacs_auto_update(current)
        if sanitized["enabled"] and sanitized.get("last_result") == "disabled":
            sanitized["last_result"] = "enabled"
        if not sanitized["enabled"]:
            sanitized["last_result"] = "disabled"
        self.data["hacs_auto_update"] = sanitized
        await self.async_save()
        return dict(sanitized)

    async def update_hacs_auto_update_status(self, **updates: Any) -> Dict[str, Any]:
        current = self.get_hacs_auto_update()
        current.update(updates)
        sanitized = self._sanitize_hacs_auto_update(current)
        self.data["hacs_auto_update"] = sanitized
        await self.async_save()
        return dict(sanitized)

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
            value = int(self.data.get("auto_sync_delay_minutes", 15))
        except Exception:
            value = 15
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

    def get_face_integrity_enabled(self) -> bool:
        value = self.data.get("face_integrity_enabled", self.DEFAULT_FACE_INTEGRITY_ENABLED)
        return bool(value)

    async def set_face_integrity_enabled(self, enabled: bool):
        self.data["face_integrity_enabled"] = bool(enabled)
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
                    data["access_expiring"] = bool(cfg.get("access_expiring"))
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
                    data["access_expiring"] = False
                    data["any_denied"] = False
                    granted_cfg = {}

                users_raw = []
                specific_flag = False
                specific_supplied = False
                if isinstance(granted_cfg, dict):
                    users_raw = granted_cfg.get("users") or []
                    any_flag = bool(granted_cfg.get("any"))
                    if "specific" in granted_cfg:
                        specific_flag = bool(granted_cfg.get("specific"))
                        specific_supplied = True
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

                if not specific_supplied and users_list:
                    specific_flag = True

                data["granted"] = {
                    "any": any_flag,
                    "specific": specific_flag,
                    "users": users_list,
                }
                cleaned[target] = data
        return cleaned

    def get_alert_targets(self) -> Dict[str, Dict[str, Any]]:
        alerts = self.data.get("alerts") or {}
        targets = alerts.get("targets") if isinstance(alerts, dict) else {}
        return self._sanitize_alert_targets(targets)

    async def set_alert_targets(self, targets: Dict[str, Any]):
        self.data.setdefault("alerts", {})["targets"] = self._sanitize_alert_targets(targets)
        await self.async_save()

    async def prune_stale_alert_users(self, users_store: Any) -> bool:
        active_user_ids = _active_notification_user_ids(users_store)
        targets = self.get_alert_targets()
        updated, changed = _prune_notify_targets_to_users(targets, active_user_ids)
        if changed:
            await self.set_alert_targets(updated)
        return changed

    def _sanitize_expiry_reminders(self, raw: Any) -> Dict[str, Any]:
        last_sent: Dict[str, str] = {}
        if isinstance(raw, dict):
            raw_last_sent = raw.get("last_sent")
            if isinstance(raw_last_sent, dict):
                for key, value in raw_last_sent.items():
                    canonical = normalize_user_id(key) or str(key or "").strip()
                    normalized_date = _normalize_access_date(value)
                    if canonical and normalized_date:
                        last_sent[canonical] = normalized_date
        return {"last_sent": last_sent}

    def get_expiry_reminders(self) -> Dict[str, Any]:
        return self._sanitize_expiry_reminders(self.data.get("expiry_reminders"))

    def expiry_reminder_sent(self, user_id: Any, access_end: Any) -> bool:
        canonical = normalize_user_id(user_id) or str(user_id or "").strip()
        normalized_date = _normalize_access_date(access_end)
        if not canonical or not normalized_date:
            return False
        last_sent = self.get_expiry_reminders().get("last_sent") or {}
        return str(last_sent.get(canonical) or "") == normalized_date

    async def mark_expiry_reminder_sent(self, user_id: Any, access_end: Any) -> None:
        canonical = normalize_user_id(user_id) or str(user_id or "").strip()
        normalized_date = _normalize_access_date(access_end)
        if not canonical or not normalized_date:
            return
        state = self.get_expiry_reminders()
        last_sent = state.setdefault("last_sent", {})
        last_sent[canonical] = normalized_date
        self.data["expiry_reminders"] = self._sanitize_expiry_reminders(state)
        await self.async_save()

    def targets_for_event(self, event_type: str, *, user_id: Optional[str] = None) -> List[str]:
        mapping = self.get_alert_targets()
        out: List[str] = []
        norm_user = _canonical_notify_user_id(user_id)
        for target, cfg in mapping.items():
            if event_type == "device_offline" and cfg.get("device_offline"):
                out.append(target)
            elif event_type == "integrity_failed" and cfg.get("integrity_failed"):
                out.append(target)
            elif event_type == "access_expiring" and cfg.get("access_expiring"):
                out.append(target)
            elif event_type == "any_denied" and cfg.get("any_denied"):
                out.append(target)
            elif event_type == "user_granted":
                granted = cfg.get("granted") or {}
                if granted.get("any"):
                    out.append(target)
                elif (
                    norm_user
                    and granted.get("specific")
                    and any(_notify_user_matches(user, norm_user) for user in (granted.get("users") or []))
                ):
                    out.append(target)
        return out


class HacsAutoUpdater:
    """Managed HACS update checker for this custom integration."""

    REPOSITORY = "DJGLTD/AK_Access_ctrl"
    DEFAULT_BRANCH = "main"
    GITHUB_RELEASE_URL = f"https://api.github.com/repos/{REPOSITORY}/releases/latest"
    MATCHERS = (
        "djgltd/ak_access_ctrl",
        "ak_access_ctrl",
        "akuvox_ac",
        "akuvox_access_control",
        "akuvox access control",
        "ak access control",
    )

    def __init__(self, hass: HomeAssistant):
        self.hass = hass
        self._interval_unsub: Optional[Callable[[], None]] = None
        self._startup_unsub: Optional[Callable[[], None]] = None
        self._restart_unsub: Optional[Callable[[], None]] = None
        self._lock = asyncio.Lock()

    def _root(self) -> Dict[str, Any]:
        return self.hass.data.get(DOMAIN, {}) or {}

    def _settings_store(self) -> Any:
        settings = self._root().get("settings_store")
        return settings if hasattr(settings, "get_hacs_auto_update") else None

    def _config(self) -> Dict[str, Any]:
        settings = self._settings_store()
        if settings and hasattr(settings, "get_hacs_auto_update"):
            try:
                return settings.get_hacs_auto_update()
            except Exception:
                pass
        return dict(AkuvoxSettingsStore.DEFAULT_HACS_AUTO_UPDATE)

    def status(self) -> Dict[str, Any]:
        status = self._config()
        status["active"] = self._interval_unsub is not None
        return status

    def start(self) -> None:
        self.apply_settings()
        self.apply_restart_schedule()
        if self._startup_unsub is not None:
            return
        try:
            self._startup_unsub = self.hass.bus.async_listen_once(
                EVENT_HOMEASSISTANT_STARTED,
                self._handle_hass_started,
            )
        except Exception:
            self._startup_unsub = None

    def shutdown(self) -> None:
        self._cancel_interval()
        self._cancel_restart_schedule()
        if self._startup_unsub:
            try:
                self._startup_unsub()
            except Exception:
                pass
            self._startup_unsub = None

    def apply_settings(self) -> None:
        self._cancel_interval()
        config = self._config()
        if not config.get("enabled"):
            return

        hour, minute = self._check_time_parts(config)

        def _schedule(_now):
            self.hass.async_create_task(self.async_run_scheduled_update(reason="scheduled"))

        try:
            self._interval_unsub = async_track_time_change(
                self.hass,
                _schedule,
                hour=hour,
                minute=minute,
                second=0,
            )
        except Exception:
            self._interval_unsub = None

    def _cancel_interval(self) -> None:
        if self._interval_unsub:
            try:
                self._interval_unsub()
            except Exception:
                pass
            self._interval_unsub = None

    def _cancel_restart_schedule(self) -> None:
        if self._restart_unsub:
            try:
                self._restart_unsub()
            except Exception:
                pass
            self._restart_unsub = None

    @staticmethod
    def _parse_restart_time(value: Any) -> Optional[datetime]:
        text = str(value or "").strip()
        if not text:
            return None
        try:
            parsed = dt_util.parse_datetime(text)
        except Exception:
            parsed = None
        if parsed is None:
            try:
                parsed = datetime.fromisoformat(text)
            except Exception:
                return None
        if parsed.tzinfo is None:
            try:
                parsed = parsed.replace(tzinfo=dt_util.DEFAULT_TIME_ZONE)
            except Exception:
                parsed = parsed.replace(tzinfo=datetime.now().astimezone().tzinfo)
        try:
            return dt_util.as_utc(parsed)
        except Exception:
            return parsed

    def apply_restart_schedule(self) -> None:
        self._cancel_restart_schedule()
        config = self._config()
        restart_at = self._parse_restart_time(config.get("restart_scheduled_for"))
        if restart_at is None:
            return

        now = dt_util.utcnow()
        if now.tzinfo is None:
            now = now.replace(tzinfo=restart_at.tzinfo)
        remaining = (restart_at - now).total_seconds()
        if remaining <= 0:
            self.hass.async_create_task(
                self._record_status(restart_scheduled_for=None)
            )
            return

        def _restart_cb(_now):
            self.hass.async_create_task(self.async_restart_now(reason="scheduled"))

        try:
            self._restart_unsub = async_call_later(self.hass, remaining, _restart_cb)
        except Exception:
            self._restart_unsub = None

    async def async_schedule_restart(self, restart_at: Any) -> Dict[str, Any]:
        parsed = self._parse_restart_time(restart_at)
        if parsed is None:
            raise ValueError("Enter a valid restart date and time.")

        now = dt_util.utcnow()
        if now.tzinfo is None:
            now = now.replace(tzinfo=parsed.tzinfo)
        if parsed <= now:
            raise ValueError("Choose a restart time in the future.")

        status = await self._record_status(
            last_result="restart_scheduled",
            last_error=None,
            restart_scheduled_for=parsed.isoformat(),
        )
        self.apply_restart_schedule()
        return status

    async def async_cancel_restart(self) -> Dict[str, Any]:
        self._cancel_restart_schedule()
        return await self._record_status(
            last_result="installed",
            last_error=None,
            restart_scheduled_for=None,
        )

    async def async_restart_now(self, *, reason: str = "manual") -> Dict[str, Any]:
        self._cancel_restart_schedule()
        try:
            await self.hass.services.async_call(
                "homeassistant",
                "restart",
                {},
                blocking=False,
            )
        except Exception as err:
            return await self._record_status(
                last_result="restart_failed",
                last_error=str(err),
                restart_scheduled_for=None,
            )
        return await self._record_status(
            last_result="restart_requested",
            last_error=None,
            restart_scheduled_for=None,
        )

    def _handle_hass_started(self, _event) -> None:
        self._startup_unsub = None
        config = self._config()
        if not config.get("enabled"):
            return
        if self._last_check_is_fresh(config):
            return
        if not self._startup_check_due(config):
            return
        self.hass.async_create_task(self.async_run_scheduled_update(reason="startup"))

    @staticmethod
    def _check_time_parts(config: Mapping[str, Any]) -> Tuple[int, int]:
        text = str(
            config.get("check_time")
            or AkuvoxSettingsStore.DEFAULT_HACS_AUTO_UPDATE["check_time"]
        ).strip()
        match = re.fullmatch(r"(\d{1,2}):(\d{2})", text)
        if not match:
            text = str(AkuvoxSettingsStore.DEFAULT_HACS_AUTO_UPDATE["check_time"])
            match = re.fullmatch(r"(\d{1,2}):(\d{2})", text)
        if not match:
            return 2, 0
        hour = int(match.group(1))
        minute = int(match.group(2))
        if hour < 0 or hour > 23 or minute < 0 or minute > 59:
            return 2, 0
        return hour, minute

    def _startup_check_due(self, config: Mapping[str, Any]) -> bool:
        now = dt_util.now()
        hour, minute = self._check_time_parts(config)
        scheduled_today = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
        return now >= scheduled_today

    def _last_check_is_fresh(self, config: Mapping[str, Any]) -> bool:
        last_checked = str(config.get("last_checked") or "").strip()
        if not last_checked:
            return False
        try:
            parsed = dt_util.parse_datetime(last_checked)
        except Exception:
            parsed = None
        if parsed is None or parsed.tzinfo is None:
            return False
        try:
            now = dt_util.now()
        except Exception:
            return False
        try:
            parsed = parsed.astimezone(now.tzinfo) if now.tzinfo else parsed
        except Exception:
            pass
        return parsed.date() == now.date()

    def _score_update_entity(self, state: Any) -> int:
        attrs = getattr(state, "attributes", {}) or {}
        fields = [
            getattr(state, "entity_id", ""),
            attrs.get("title"),
            attrs.get("friendly_name"),
            attrs.get("repository"),
            attrs.get("repository_name"),
            attrs.get("release_url"),
            attrs.get("entity_picture"),
        ]
        text = " ".join(str(field or "").lower() for field in fields)
        score = 0
        if self.REPOSITORY.lower() in text:
            score += 100
        for matcher in self.MATCHERS:
            if matcher in text:
                score += 30
        if "hacs" in text:
            score += 5
        return score

    def _find_update_entity(self, configured: Any = None) -> Optional[str]:
        entity_id = str(configured or "").strip()
        if entity_id:
            state = self.hass.states.get(entity_id)
            return entity_id if state is not None else None

        try:
            states = self.hass.states.async_all("update")
        except TypeError:
            try:
                states = [
                    state
                    for state in self.hass.states.async_all()
                    if str(getattr(state, "entity_id", "")).startswith("update.")
                ]
            except Exception:
                states = []
        except Exception:
            states = []

        best_entity: Optional[str] = None
        best_score = 0
        for state in states:
            score = self._score_update_entity(state)
            if score > best_score:
                best_score = score
                best_entity = getattr(state, "entity_id", None)

        return best_entity if best_score >= 30 else None

    def _update_available(self, state: Any) -> bool:
        state_value = str(getattr(state, "state", "") or "").strip().lower()
        attrs = getattr(state, "attributes", {}) or {}
        installed = str(attrs.get("installed_version") or "").strip()
        latest = str(attrs.get("latest_version") or "").strip()
        skipped = str(attrs.get("skipped_version") or "").strip()
        if not latest or not installed:
            return False
        if not self._is_release_version(latest):
            return False
        if self._versions_match(latest, installed):
            return False
        if skipped and self._versions_match(latest, skipped):
            return False
        if state_value == "off":
            return False
        return True

    @staticmethod
    def _version_text(version: Any) -> str:
        return str(version or "").strip()

    @classmethod
    def _is_release_version(cls, version: Any) -> bool:
        text = cls._version_text(version)
        return bool(re.fullmatch(r"v?\d+(?:\.\d+){1,2}(?:[-+][0-9A-Za-z.-]+)?", text))

    @classmethod
    def _display_version(cls, version: Any) -> Optional[str]:
        text = cls._version_text(version)
        if not text:
            return None
        if cls._is_release_version(text):
            return text[1:] if text.lower().startswith("v") else text
        return text

    @classmethod
    def _comparable_version(cls, version: Any) -> str:
        text = cls._version_text(version).lower()
        if not text:
            return ""
        if text.startswith("v") and len(text) > 1 and text[1].isdigit():
            text = text[1:]
        match = re.fullmatch(r"(\d+)\.(\d+)(?:\.(\d+))?([\-+][0-9a-z.-]+)?", text)
        if match:
            patch = match.group(3) if match.group(3) is not None else "0"
            suffix = match.group(4) or ""
            return f"{int(match.group(1))}.{int(match.group(2))}.{int(patch)}{suffix}"
        return text

    @classmethod
    def _versions_match(cls, left: Any, right: Any) -> bool:
        left_text = cls._comparable_version(left)
        right_text = cls._comparable_version(right)
        if not left_text or not right_text:
            return False
        return left_text == right_text

    async def _fetch_latest_github_release(self) -> Optional[Tuple[str, str]]:
        session = async_get_clientsession(self.hass)
        if asyncio.iscoroutine(session):
            session = await session
        if session is None or not hasattr(session, "get"):
            return None

        response_cm = session.get(
            self.GITHUB_RELEASE_URL,
            headers={
                "Accept": "application/vnd.github+json",
                "User-Agent": "HomeAssistant-Akuvox-Access-Control",
            },
            timeout=20,
        )
        async with response_cm as response:
            status = int(getattr(response, "status", 0) or 0)
            if status == 404:
                return None
            if status >= 400:
                body = ""
                try:
                    body = await response.text()
                except Exception:
                    body = ""
                message = f"GitHub latest release check failed with HTTP {status}"
                if body:
                    message = f"{message}: {body[:200]}"
                raise RuntimeError(message)
            payload = await response.json()

        tag = str((payload or {}).get("tag_name") or "").strip()
        version = self._display_version(tag)
        if not tag or not version:
            raise RuntimeError("GitHub latest release response did not include a valid tag.")
        return tag, version

    async def _record_status(self, **updates: Any) -> Dict[str, Any]:
        settings = self._settings_store()
        if settings and hasattr(settings, "update_hacs_auto_update_status"):
            return await settings.update_hacs_auto_update_status(**updates)
        status = self.status()
        status.update(updates)
        return status

    async def _create_restart_notification(self, entity_id: str) -> None:
        try:
            await self.hass.services.async_call(
                "persistent_notification",
                "create",
                {
                    "notification_id": "akuvox_ac_hacs_auto_update",
                    "title": "Akuvox Access Control updated",
                    "message": (
                        f"HACS installed an update for `{entity_id}`. "
                        "Restart Home Assistant to load the new integration code."
                    ),
                },
                blocking=False,
            )
        except Exception:
            pass

    async def _create_update_available_notification(
        self, entity_id: str, *, installed: Any = None, latest: Any = None
    ) -> None:
        try:
            suffix = ""
            if installed or latest:
                suffix = f" Current: `{installed or 'unknown'}`. Available: `{latest or 'unknown'}`."
            await self.hass.services.async_call(
                "persistent_notification",
                "create",
                {
                    "notification_id": "akuvox_ac_hacs_update_available",
                    "title": "Akuvox Access Control update available",
                    "message": (
                        f"`{entity_id}` has an update available.{suffix} "
                        "Open the Akuvox dashboard to review and install it."
                    ),
                },
                blocking=False,
            )
        except Exception:
            pass

    async def async_run_scheduled_update(
        self, *, reason: str = "scheduled"
    ) -> Dict[str, Any]:
        status = await self.async_run_check(reason=reason)
        if str(status.get("last_result") or "").lower() != "update_available":
            return status

        config = self._config()
        if not config.get("auto_install"):
            return status

        install_status = await self.async_install_update(reason=reason, force=True)
        if (
            str(install_status.get("last_result") or "").lower() == "installed"
            and self._config().get("restart_after_install")
        ):
            return await self.async_restart_now(reason=f"{reason}_auto_restart")
        return install_status

    async def async_run_check(
        self, *, reason: str = "manual", force: bool = False
    ) -> Dict[str, Any]:
        config = self._config()
        if not config.get("enabled") and not force:
            return await self._record_status(last_result="disabled", last_error=None)

        async with self._lock:
            checked_at = dt_util.now().isoformat()
            entity_id = self._find_update_entity(config.get("update_entity"))
            if not entity_id:
                return await self._record_status(
                    last_checked=checked_at,
                    last_result="entity_not_found",
                    last_error="Could not find the HACS update entity for Akuvox Access Control.",
                    last_entity_id=None,
                )

            try:
                await self.hass.services.async_call(
                    "homeassistant",
                    "update_entity",
                    {"entity_id": entity_id},
                    blocking=True,
                )
            except Exception as err:
                return await self._record_status(
                    last_checked=checked_at,
                    last_result="check_failed",
                    last_error=str(err),
                    last_entity_id=entity_id,
                )

            state = self.hass.states.get(entity_id)
            if state is None:
                return await self._record_status(
                    last_checked=checked_at,
                    last_result="entity_not_found",
                    last_error=f"{entity_id} is no longer available.",
                    last_entity_id=entity_id,
                )

            attrs = getattr(state, "attributes", {}) or {}
            installed = attrs.get("installed_version")
            hacs_latest = attrs.get("latest_version")
            hacs_latest_version = (
                self._display_version(hacs_latest) if self._is_release_version(hacs_latest) else None
            )
            latest = hacs_latest_version
            github_latest_tag: Optional[str] = None
            github_latest_version: Optional[str] = None
            github_error: Optional[str] = None
            try:
                latest_release = await self._fetch_latest_github_release()
                if latest_release:
                    github_latest_tag, github_latest_version = latest_release
            except Exception as err:
                github_error = str(err)
            if github_latest_version:
                latest = github_latest_version

            base_status = {
                "last_checked": checked_at,
                "last_entity_id": entity_id,
                "installed_version": installed,
                "latest_version": latest,
            }

            pending_version_full: Optional[str] = None
            has_update = self._update_available(state)
            if github_latest_version and not self._versions_match(installed, github_latest_version):
                has_update = True
                pending_version_full = github_latest_tag or github_latest_version
            elif has_update and hacs_latest_version:
                pending_version_full = self._version_text(hacs_latest)

            pending_version = self._display_version(pending_version_full)

            if not has_update:
                if github_error:
                    return await self._record_status(
                        **base_status,
                        last_result="check_failed",
                        last_error=github_error,
                        pending_version=None,
                        pending_version_full=None,
                    )
                return await self._record_status(
                    **base_status,
                    last_result="up_to_date",
                    last_error=None,
                    pending_version=None,
                    pending_version_full=None,
                )

            await self._create_update_available_notification(
                entity_id,
                installed=installed,
                latest=pending_version or latest,
            )
            return await self._record_status(
                **base_status,
                last_result="update_available",
                last_error=github_error,
                pending_version=pending_version,
                pending_version_full=pending_version_full,
            )

    async def async_install_update(
        self, *, reason: str = "manual", force: bool = False
    ) -> Dict[str, Any]:
        check_status = await self.async_run_check(reason=f"{reason}_preinstall", force=True)
        if str(check_status.get("last_result") or "").lower() != "update_available":
            return check_status

        config = self._config()
        async with self._lock:
            entity_id = str(check_status.get("last_entity_id") or "").strip()
            if not entity_id:
                return await self._record_status(
                    last_result="entity_not_found",
                    last_error="Could not find the HACS update entity for Akuvox Access Control.",
                    last_entity_id=None,
                )

            install_version = str(check_status.get("pending_version_full") or "").strip()
            service_data: Dict[str, Any] = {"entity_id": entity_id}
            if install_version:
                service_data["version"] = install_version
            if config.get("backup"):
                service_data["backup"] = True

            try:
                await self.hass.services.async_call(
                    "update",
                    "install",
                    service_data,
                    blocking=True,
                )
            except Exception as err:
                return await self._record_status(
                    last_checked=dt_util.now().isoformat(),
                    last_entity_id=entity_id,
                    installed_version=check_status.get("installed_version"),
                    latest_version=check_status.get("latest_version"),
                    last_result="install_failed",
                    last_error=str(err),
                )

            try:
                await self.hass.services.async_call(
                    "homeassistant",
                    "update_entity",
                    {"entity_id": entity_id},
                    blocking=True,
                )
            except Exception:
                pass

            state_after = self.hass.states.get(entity_id)
            attrs_after = getattr(state_after, "attributes", {}) if state_after is not None else {}
            installed_after = attrs_after.get("installed_version") or check_status.get("installed_version")
            latest_after = attrs_after.get("latest_version") or check_status.get("latest_version")
            pending_display = self._display_version(install_version)
            confirmed = False
            if install_version and self._versions_match(installed_after, install_version):
                confirmed = True
            elif pending_display and self._versions_match(installed_after, pending_display):
                confirmed = True
            elif (
                not install_version
                and state_after is not None
                and not self._update_available(state_after)
                and installed_after
                and latest_after
                and self._versions_match(installed_after, latest_after)
            ):
                confirmed = True

            if not confirmed:
                return await self._record_status(
                    last_checked=dt_util.now().isoformat(),
                    last_entity_id=entity_id,
                    installed_version=installed_after,
                    latest_version=pending_display or latest_after,
                    last_result="install_unconfirmed",
                    last_error=(
                        "Install command was sent, but HACS still reports "
                        f"{entity_id} at {installed_after or 'unknown'}."
                    ),
                    pending_version=pending_display or check_status.get("pending_version"),
                    pending_version_full=install_version or check_status.get("pending_version_full"),
                )

            await self._create_restart_notification(entity_id)
            return await self._record_status(
                last_checked=dt_util.now().isoformat(),
                last_entity_id=entity_id,
                installed_version=installed_after,
                latest_version=pending_display or latest_after,
                last_installed=dt_util.now().isoformat(),
                last_result="installed",
                last_error=None,
                pending_version=None,
                pending_version_full=None,
                restart_scheduled_for=None,
            )


# ---------------------- Robust device user lookup + delete ---------------------- #
async def _lookup_device_user_ids_by_ha_key(
    api: AkuvoxAPI,
    ha_key: str,
    *,
    allow_non_ha_group: bool = False,
) -> List[Dict[str, str]]:
    out: List[Dict[str, str]] = []
    target = str(ha_key or "").strip()
    if not target:
        return out
    target_norm = normalize_user_id(target)

    try:
        dev_users = await api.user_list()
    except Exception:
        dev_users = []

    seen: set[Tuple[str, str, str]] = set()
    for u in dev_users or []:
        if not allow_non_ha_group and not _is_ha_group_record(u):
            continue
        dev_id = str(u.get("ID") or "")
        user_id = str(u.get("UserID") or u.get("UserId") or "")
        name = str(u.get("Name") or "")
        user_id_alt = str(u.get("UserId") or "")
        candidates = {
            c for c in (dev_id, user_id, user_id_alt, name, _key_of_user(u)) if c
        }
        candidate_norms = {
            normalize_user_id(c) for c in candidates if normalize_user_id(c)
        }
        if target not in candidates and (not target_norm or target_norm not in candidate_norms):
            continue

        key_tuple = (dev_id, user_id or user_id_alt, name)
        if key_tuple in seen:
            continue
        seen.add(key_tuple)
        out.append(
            {
                "ID": dev_id,
                "UserID": user_id or user_id_alt,
                "Name": name,
                "Group": str(u.get("Group") or u.get("group") or ""),
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
        self._pending_full = False
        self._pending_full_devices: set[str] = set()
        self._pending_reason_all: Optional[str] = None
        self._pending_reason_devices: Dict[str, str] = {}
        self.next_sync_eta: Optional[datetime] = None
        self._last_mark: Optional[datetime] = None
        self._last_delay_from_default = False
        self._active: bool = False
        self._tick_unsub: Optional[Callable[[], None]] = None
        self._startup_unsub: Optional[Callable[[], None]] = None

        def _schedule_background_tick(_now):
            self._schedule_task(self._background_tick(_now))

        try:
            self._tick_unsub = async_track_time_interval(
                hass,
                _schedule_background_tick,
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

    def _schedule_task(self, coro: Coroutine[Any, Any, Any]) -> None:
        try:
            loop = self.hass.loop
            if loop and loop.is_running():
                loop.call_soon_threadsafe(self.hass.async_create_task, coro)
                return
        except Exception:
            pass
        self.hass.async_create_task(coro)

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
        return 15

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

    def mark_change(
        self,
        entry_id: Optional[str] = None,
        delay_minutes: Optional[int] = None,
        *,
        full: bool = False,
        trigger: Optional[str] = None,
    ):
        trigger_label = str(trigger or "").strip()
        if entry_id:
            if trigger_label:
                self._pending_reason_devices[entry_id] = trigger_label
        elif trigger_label:
            self._pending_reason_all = trigger_label
        self._set_health_status(entry_id, "pending")
        if entry_id:
            self._pending_devices.add(entry_id)
        else:
            self._pending_all = True
        if full:
            if entry_id:
                self._pending_full_devices.add(entry_id)
            else:
                self._pending_full = True

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
            self._schedule_task(self.run())
            return

        def _schedule_cb(_now):
            self._schedule_task(self.run())

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
            self._schedule_task(self.run())
            return

        self.next_sync_eta = eta
        self._last_delay_from_default = True

        def _schedule_cb(_now):
            self._schedule_task(self.run())

        self._handle = async_call_later(self.hass, remaining, _schedule_cb)

    def ensure_future_run(self):
        if self._active:
            return

        eta = self.next_sync_eta
        if not isinstance(eta, datetime):
            if (
                self._handle is None
                and not self._pending_all
                and not self._pending_devices
                and self._has_auto_pending_work()
            ):
                try:
                    self.mark_change(
                        None,
                        delay_minutes=0,
                        trigger="auto-detected pending state",
                    )
                except Exception:
                    pass
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
        self._schedule_task(self.run())

    def _has_auto_pending_work(self) -> bool:
        root = self._root()

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
                return True

        users_store = root.get("users_store")
        if users_store and hasattr(users_store, "all"):
            try:
                profiles = users_store.all() or {}
            except Exception:
                profiles = {}
            for profile in profiles.values():
                if _profile_is_empty_reserved(profile or {}):
                    continue
                status = str((profile or {}).get("status") or "").strip().lower()
                face_status = str((profile or {}).get("face_status") or "").strip().lower()
                if status == "pending" or face_status in {"pending", "error"}:
                    return True

        return False

    def _handle_hass_started(self, _event):
        try:
            self.ensure_future_run()
        except Exception:
            pass

        self._startup_unsub = None

        try:
            self._schedule_task(self._background_tick(datetime.now()))
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

        if self._has_auto_pending_work():
            try:
                self.mark_change(None, delay_minutes=0, trigger="auto-detected pending state")
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

    async def run(self, only_entry: Optional[str] = None, full: Optional[bool] = None):
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
                    sync_trigger = (
                        self._pending_reason_devices.get(entry_id)
                        or self._pending_reason_all
                        or "unspecified trigger"
                    )
                    if full is None:
                        full_sync = self._pending_full or entry_id in self._pending_full_devices
                    else:
                        full_sync = full
                    try:
                        try:
                            mode = "full sync" if full_sync else "sync"
                            coord._append_event(
                                f"Starting {mode} (trigger: {sync_trigger})"
                            )  # type: ignore[attr-defined]
                        except Exception:
                            pass
                        coord.health["sync_status"] = "in_progress"
                    except Exception:
                        pass
                    try:
                        await manager.reconcile_device(entry_id, full=full_sync)
                        coord.health["sync_status"] = "in_sync"
                        coord.health["last_sync"] = _now_hh_mm()
                        try:
                            coord._append_event(
                                f"Sync succeeded (trigger: {sync_trigger})"
                            )  # type: ignore[attr-defined]
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
                self._pending_full = False
                self._pending_full_devices.clear()
                self._pending_reason_all = None
                self._pending_reason_devices.clear()
                self._handle = None
                self._active = False

    async def sync_now(
        self,
        entry_id: Optional[str] = None,
        *,
        include_all: bool = False,
        full: Optional[bool] = None,
        trigger: Optional[str] = None,
    ):
        if include_all and entry_id:
            include_all = False

        trigger_label = str(trigger or "").strip()
        if include_all and not entry_id:
            self._pending_all = True
            if full:
                self._pending_full = True
            if trigger_label:
                self._pending_reason_all = trigger_label
        elif entry_id and trigger_label:
            self._pending_reason_devices[entry_id] = trigger_label

        self._set_health_status(entry_id, "in_progress" if entry_id else "pending")
        if self._handle is not None:
            try:
                self._handle()
            except Exception:
                pass
            self._handle = None
        self.next_sync_eta = None
        await self.run(only_entry=entry_id, full=full)


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
        self._temp_cleanup_unsub = None
        self._temp_midnight_unsub = None
        self._expiry_reminder_unsub = None
        self._temp_cleanup_lock = asyncio.Lock()
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
        self._temp_cleanup_unsub = async_track_time_interval(
            hass,
            self._temporary_cleanup_interval,
            timedelta(minutes=5),
        )
        self._temp_midnight_unsub = async_track_time_change(
            hass,
            self._temporary_cleanup_midnight,
            hour=0,
            minute=0,
            second=0,
        )
        self._expiry_reminder_unsub = async_track_time_change(
            hass,
            self._access_expiry_reminder_morning,
            hour=8,
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

    async def _prune_stale_alert_users(self) -> None:
        settings = self._settings_store()
        users_store = self._users_store()
        if not settings or not users_store:
            return
        if not hasattr(settings, "prune_stale_alert_users"):
            return
        try:
            await settings.prune_stale_alert_users(users_store)
        except Exception:
            pass

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
    def _profile_is_temporary(profile: Dict[str, Any]) -> bool:
        return bool(
            profile.get("temporary")
            or profile.get("temporary_one_time")
            or profile.get("temporary_expires_at")
        )

    @staticmethod
    def _temp_profile_matches_user(
        profile: Dict[str, Any],
        *,
        user_id: Optional[str],
        user_name: Optional[str],
        profile_key: str,
    ) -> bool:
        if not user_id and not user_name:
            return False

        canonical_key = normalize_user_id(profile_key) or profile_key
        canonical_user = normalize_user_id(user_id) if user_id else None
        if canonical_user and canonical_key and canonical_user == canonical_key:
            return True
        if user_id:
            raw_user = str(user_id).strip().lower()
            if raw_user and raw_user == str(profile_key).strip().lower():
                return True
        name = str(profile.get("name") or "").strip()
        if user_name and name:
            if str(user_name).strip().lower() == name.lower():
                return True
        return False

    async def handle_access_granted(
        self,
        user_id: Optional[str],
        *,
        user_name: Optional[str] = None,
    ) -> None:
        await self._cleanup_temporary_users(
            reason="access_granted",
            event_user_id=user_id,
            event_user_name=user_name,
        )

    async def _temporary_cleanup_interval(self, now):
        await self._cleanup_temporary_users(reason="interval")

    async def _startup_user_cleanup(self) -> None:
        await self._cleanup_temporary_users(reason="startup")
        await self._cleanup_expired_access_users(reason="startup")

    async def _temporary_cleanup_midnight(self, now):
        await self._cleanup_temporary_users(reason="midnight")
        await self._cleanup_expired_access_users(reason="midnight")

    async def _access_expiry_reminder_morning(self, now):
        await self._send_access_expiry_reminders()

    async def _expire_temporary_user(
        self,
        key: str,
        *,
        now: datetime,
        today: date,
        used: bool,
    ) -> None:
        users_store = self._users_store()
        if not users_store:
            return

        try:
            await users_store.upsert_profile(
                key,
                access_end=today.isoformat(),
                schedule_name="No Access",
                schedule_id="1002",
                temporary_used_at=now if used else None,
            )
        except Exception:
            return

        try:
            sync_queue = self._root().get("sync_queue")
            if sync_queue:
                sync_queue.mark_change(None, delay_minutes=0, trigger="temporary user expiry")
        except Exception:
            pass

    async def _cleanup_temporary_users(
        self,
        *,
        reason: str,
        event_user_id: Optional[str] = None,
        event_user_name: Optional[str] = None,
    ) -> None:
        if self._temp_cleanup_lock.locked():
            return

        async with self._temp_cleanup_lock:
            users_store = self._users_store()
            if not users_store:
                return

            try:
                profiles = users_store.all() or {}
            except Exception:
                return

            now = dt_util.now()
            today = now.date()
            to_expire: List[Tuple[str, bool]] = []

            for key, profile in profiles.items():
                if not isinstance(profile, dict):
                    continue
                if not self._profile_is_temporary(profile):
                    continue
                if str(profile.get("status") or "").strip().lower() == "deleted":
                    continue

                is_one_time = bool(profile.get("temporary_one_time"))
                if is_one_time and self._temp_profile_matches_user(
                    profile,
                    user_id=event_user_id,
                    user_name=event_user_name,
                    profile_key=str(key),
                ):
                    to_expire.append((str(key), True))
                    continue

                expires_at = _parse_temp_datetime(profile.get("temporary_expires_at"))
                if expires_at and now >= expires_at:
                    to_expire.append((str(key), False))
                    continue

                if reason == "midnight":
                    access_end = _parse_access_date(profile.get("access_end"))
                    if access_end and access_end < today:
                        to_expire.append((str(key), False))

            if not to_expire:
                return

            for key, used in to_expire:
                await self._expire_temporary_user(
                    key,
                    now=now,
                    today=today,
                    used=used,
                )

    @staticmethod
    def _profile_can_expire(profile: Mapping[str, Any]) -> bool:
        if not isinstance(profile, Mapping):
            return False
        if _profile_is_empty_reserved(profile):
            return False
        status = str(profile.get("status") or "").strip().lower()
        return status != "deleted"

    async def _delete_expired_access_user(
        self,
        key: str,
        profile: Mapping[str, Any],
        *,
        today: date,
        reason: str,
    ) -> None:
        users_store = self._users_store()
        if not users_store:
            return

        canonical = normalize_user_id(key) or str(key or "").strip()
        if not canonical:
            return

        access_end = _parse_access_date(profile.get("access_end")) or today
        name = str(profile.get("name") or canonical).strip()
        phone = str(profile.get("phone") or "").strip()

        try:
            await users_store.upsert_profile(
                canonical,
                status="deleted",
                groups=["No Access"],
                schedule_name="No Access",
                schedule_id="1002",
                access_end=access_end.isoformat(),
            )
        except Exception:
            return

        self._remove_face_files_for_user({canonical, str(key or "").strip()})
        await self._prune_stale_alert_users()

        for _entry_id, coord, api, _opts in self._devices():
            try:
                if name or phone:
                    await self._delete_contacts(api, name=name, phone=phone)
            except Exception:
                pass
            try:
                id_records = await _lookup_device_user_ids_by_ha_key(
                    api,
                    canonical,
                    allow_non_ha_group=True,
                )
            except Exception:
                id_records = []
            for rec in id_records or []:
                try:
                    await _delete_user_every_way(api, rec)
                except Exception:
                    pass
            try:
                coord._append_event(
                    f"Expired user removed: {name or canonical} ({reason})"
                )  # type: ignore[attr-defined]
            except Exception:
                pass
            try:
                await coord.async_request_refresh()
            except Exception:
                pass

        try:
            sync_queue = self._root().get("sync_queue")
            if sync_queue:
                sync_queue.mark_change(
                    None,
                    delay_minutes=0,
                    full=True,
                    trigger=f"expired user cleanup: {canonical}",
                )
        except Exception:
            pass

    def _remove_face_files_for_user(self, user_ids: Iterable[str]) -> None:
        cleaned_ids = {str(item or "").strip() for item in user_ids if str(item or "").strip()}
        if not cleaned_ids:
            return

        face_dirs: List[Path] = []
        try:
            face_dirs.append(face_storage_dir(self.hass))
        except Exception:
            pass
        face_dirs.append(Path(__file__).parent / "www" / "FaceData")
        try:
            face_dirs.append(Path(self.hass.config.path("www")) / "AK_Access_ctrl" / "FaceData")
        except Exception:
            pass

        for base in face_dirs:
            try:
                resolved_base = base.resolve()
            except Exception:
                continue
            for ext in FACE_FILE_EXTENSIONS:
                for user_id in cleaned_ids:
                    try:
                        candidate = (resolved_base / f"{user_id}.{ext}").resolve()
                        candidate.relative_to(resolved_base)
                    except Exception:
                        continue
                    if candidate.exists():
                        try:
                            candidate.unlink()
                        except Exception:
                            pass

    async def _cleanup_expired_access_users(self, *, reason: str) -> None:
        if self._temp_cleanup_lock.locked():
            return

        async with self._temp_cleanup_lock:
            users_store = self._users_store()
            if not users_store:
                return
            try:
                profiles = users_store.all() or {}
            except Exception:
                return

            today = dt_util.now().date()
            to_delete: List[Tuple[str, Mapping[str, Any]]] = []
            for key, profile in profiles.items():
                if not self._profile_can_expire(profile):
                    continue
                access_end = _parse_access_date(profile.get("access_end"))
                if access_end and access_end < today:
                    to_delete.append((str(key), profile))

            for key, profile in to_delete:
                await self._delete_expired_access_user(
                    key,
                    profile,
                    today=today,
                    reason=reason,
                )

    async def _send_access_expiry_reminders(self) -> None:
        settings = self._settings_store()
        users_store = self._users_store()
        if not settings or not users_store:
            return
        if not hasattr(settings, "targets_for_event"):
            return

        try:
            targets = list(settings.targets_for_event("access_expiring"))
        except Exception:
            targets = []
        if not targets:
            return

        try:
            profiles = users_store.all() or {}
        except Exception:
            return

        today = dt_util.now().date()
        for key, profile in profiles.items():
            if not self._profile_can_expire(profile):
                continue
            access_end = _parse_access_date(profile.get("access_end"))
            if access_end != today:
                continue

            canonical = normalize_user_id(key) or str(key or "").strip()
            if not canonical:
                continue
            if hasattr(settings, "expiry_reminder_sent"):
                try:
                    if settings.expiry_reminder_sent(canonical, access_end):
                        continue
                except Exception:
                    pass

            name = str(profile.get("name") or canonical).strip()
            review_url = "/akuvox-ac/expiry-review?" + urlencode(
                {
                    "id": canonical,
                    "name": name,
                    "access_end": access_end.isoformat(),
                }
            )
            message = f"{name}'s Access expires today, Would you like to extend"
            sent = False
            for target in targets:
                try:
                    await self.hass.services.async_call(
                        "notify",
                        target,
                        {
                            "title": "Akuvox: Access expires today",
                            "message": message,
                            "data": {"url": review_url},
                        },
                        blocking=False,
                    )
                    sent = True
                except Exception:
                    pass

            if sent and hasattr(settings, "mark_expiry_reminder_sent"):
                try:
                    await settings.mark_expiry_reminder_sent(canonical, access_end)
                except Exception:
                    pass

    async def _bump_face_error_count(self, ha_key: str) -> int:
        users_store = self._users_store()
        if not users_store:
            return 0
        try:
            profile = users_store.get(ha_key) or {}
        except Exception:
            profile = {}
        try:
            count = int(profile.get("face_error_count") or 0)
        except (TypeError, ValueError):
            count = 0
        count += 1
        try:
            await users_store.upsert_profile(ha_key, face_error_count=count)
        except Exception:
            return count
        return count

    async def _reset_face_error_count(self, ha_key: str) -> None:
        users_store = self._users_store()
        if not users_store:
            return
        try:
            await users_store.upsert_profile(ha_key, face_error_count=0)
        except Exception:
            return

    async def _recreate_user_for_face_mismatch(
        self,
        api: AkuvoxAPI,
        ha_key: str,
        desired: Dict[str, Any],
        existing: Optional[Dict[str, Any]],
    ) -> None:
        await self._replace_user_on_device(api, ha_key, desired, existing=existing)

    async def _upload_face_asset_to_device(
        self,
        api: AkuvoxAPI,
        coord: AkuvoxCoordinator,
        ha_key: str,
        desired: Dict[str, Any],
        profile: Optional[Dict[str, Any]] = None,
        *,
        existing: Optional[Dict[str, Any]] = None,
        force: bool = False,
    ) -> bool:
        """Upload a stored HA face image directly to a device and link it to the user."""

        device_type = str((getattr(coord, "health", {}) or {}).get("device_type") or "").strip().lower()
        if device_type == "keypad":
            return False

        face_reference = str(
            (desired or {}).get("FaceUrl")
            or (desired or {}).get("FaceURL")
            or (profile or {}).get("face_url")
            or ""
        ).strip()
        face_path: Optional[Path] = None
        if not face_reference:
            face_path = _face_asset_path(self.hass, ha_key)
            if face_path:
                face_reference = face_path.name
            else:
                return False

        face_status = str((profile or {}).get("face_status") or "").strip().lower()
        if not force and face_status not in {"pending", "error"}:
            return False

        if face_path is None:
            face_path = _face_asset_path(self.hass, ha_key, face_reference)
        if not face_path:
            try:
                coord._append_event(f"Face upload skipped for {ha_key}: local image not found")  # type: ignore[attr-defined]
            except Exception:
                pass
            return False

        try:
            face_bytes = await self.hass.async_add_executor_job(face_path.read_bytes)
        except Exception:
            try:
                face_bytes = face_path.read_bytes()
            except Exception as err:
                _LOGGER.debug("Unable to read face asset for %s: %s", ha_key, err)
                return False

        try:
            filename = face_filename_from_reference(face_reference or face_path.name, ha_key)
        except Exception:
            filename = face_path.name
        if not filename:
            filename = face_path.name or f"{ha_key}.jpg"

        device_record = existing if isinstance(existing, dict) else None
        if not device_record or not str(device_record.get("ID") or "").strip():
            try:
                matches = await _lookup_device_user_ids_by_ha_key(api, ha_key)
            except Exception:
                matches = []
            if matches:
                device_record = matches[0]

        delete_records: List[Dict[str, Any]] = []
        if isinstance(device_record, dict):
            delete_records.append(device_record)
        else:
            try:
                delete_records = await _lookup_device_user_ids_by_ha_key(api, ha_key)
            except Exception:
                delete_records = []

        seen_delete: Set[Tuple[str, str, str]] = set()
        for rec in delete_records:
            if not isinstance(rec, dict):
                continue
            marker = (
                str(rec.get("ID") or ""),
                str(rec.get("UserID") or rec.get("UserId") or ""),
                str(rec.get("Name") or ""),
            )
            if marker in seen_delete:
                continue
            seen_delete.add(marker)
            try:
                await _delete_user_every_way(api, rec)  # type: ignore[arg-type]
            except Exception:
                pass

        try:
            upload_result = await api.face_upload(face_bytes, filename=filename)
        except Exception as err:
            _LOGGER.debug("Direct face upload failed for %s: %s", ha_key, err)
            try:
                coord._append_event(f"Direct face upload failed for {ha_key}: {err}")  # type: ignore[attr-defined]
            except Exception:
                pass
            return False

        face_device_reference = ""
        if isinstance(upload_result, dict):
            raw_path = upload_result.get("path")
            if isinstance(raw_path, str):
                face_device_reference = raw_path.strip()
            if not face_device_reference:
                raw = upload_result.get("raw")
                if isinstance(raw, str):
                    face_device_reference = raw.strip()
        elif isinstance(upload_result, str):
            face_device_reference = upload_result.strip()

        if not face_device_reference:
            face_device_reference = face_reference

        payload = dict(desired or {})
        payload["FaceFileName"] = filename
        payload.pop("FaceUrl", None)
        payload.pop("FaceURL", None)
        payload.pop("FaceRegister", None)
        payload.pop("importFile", None)
        add_payload = _prepare_user_add_payload(
            ha_key,
            payload,
            sources=(payload, device_record),
        )

        try:
            await api.user_add([add_payload])
        except Exception as err:
            _LOGGER.debug("Failed to link uploaded face for %s: %s", ha_key, err)
            try:
                coord._append_event(f"Face uploaded but link failed for {ha_key}: {err}")  # type: ignore[attr-defined]
            except Exception:
                pass
            return False

        try:
            coord._append_event(f"Uploaded face image for {ha_key} directly to device")  # type: ignore[attr-defined]
        except Exception:
            pass

        try:
            users_store = self._users_store()
            if users_store:
                await users_store.upsert_profile(
                    ha_key,
                    face_status="pending",
                    face_synced_at="",
                    face_error_count=0,
                )
        except Exception:
            pass

        return True

    async def _replace_user_on_device(
        self,
        api: AkuvoxAPI,
        ha_key: str,
        desired: Dict[str, Any],
        *,
        existing: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Delete the current device user record(s) and recreate from desired payload."""
        records = []
        if isinstance(existing, dict):
            records = [existing]
        else:
            try:
                records = await _lookup_device_user_ids_by_ha_key(api, ha_key)
            except Exception:
                records = []

        for rec in records:
            try:
                await _delete_user_every_way(api, rec)
            except Exception:
                pass

        payload = _prepare_user_add_payload(
            ha_key,
            desired,
            sources=(desired, existing),
        )
        await api.user_add([payload])

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

    @staticmethod
    def _contact_phone(value: Mapping[str, Any]) -> str:
        return str(value.get("Phone") or value.get("PhoneNum") or value.get("phone") or "").strip()

    async def _sync_contacts_for_profiles(
        self,
        api: AkuvoxAPI,
        profiles: Iterable[Tuple[str, str]],
        *,
        prune_extra: bool = False,
    ) -> None:
        """Ensure HA contact entries exist for profile (name, phone) pairs."""

        desired: Dict[str, str] = {}
        for raw_name, raw_phone in profiles:
            name = str(raw_name or "").strip()
            phone = str(raw_phone or "").strip()
            if not name or not phone:
                continue
            desired[name] = phone
        if not desired and not prune_extra:
            return

        try:
            response = await api.contact_get()
        except Exception:
            return

        contacts = self._extract_contact_items(response)
        existing_by_name: Dict[str, Dict[str, Any]] = {}
        for contact in contacts:
            if not _is_ha_group_record(contact):
                continue
            name = str(contact.get("Name") or contact.get("name") or "").strip()
            if not name:
                continue
            existing_by_name[name] = contact

        add_items: List[Dict[str, Any]] = []
        delete_items: List[Dict[str, Any]] = []
        seen_delete: Set[str] = set()
        if prune_extra:
            for name in existing_by_name:
                if name in desired:
                    continue
                delete_items.append({"Name": name, "Group": HA_CONTACT_GROUP_NAME})
                seen_delete.add(name)

        for name, phone in desired.items():
            existing = existing_by_name.get(name)
            if existing:
                existing_phone = self._contact_phone(existing)
                if self._normalize_phone(existing_phone) == self._normalize_phone(phone):
                    continue
                if name not in seen_delete:
                    delete_items.append({"Name": name, "Group": HA_CONTACT_GROUP_NAME})
                    seen_delete.add(name)
            add_items.append(
                {"Name": name, "Phone": phone, "PhoneNum": phone, "Group": HA_CONTACT_GROUP_NAME}
            )

        if delete_items:
            try:
                await api.contact_delete(delete_items)
            except Exception:
                pass
        if add_items:
            try:
                await api.contact_add(add_items)
            except Exception:
                pass

    async def _delete_contacts(self, api: AkuvoxAPI, *, name: Optional[str], phone: Optional[str]) -> None:
        match_name = str(name or "").strip()
        match_phone = str(phone or "").strip()
        match_phone_norm = self._normalize_phone(match_phone)
        if not match_name and not match_phone_norm:
            return

        try:
            response = await api.contact_get()
        except Exception:
            return

        contacts = self._extract_contact_items(response)
        delete_items: List[Dict[str, Any]] = []
        seen_names: Set[str] = set()
        for contact in contacts:
            contact_name = str(contact.get("Name") or contact.get("name") or "").strip()
            if not contact_name or not _is_ha_group_record(contact):
                continue
            contact_phone = self._contact_phone(contact)
            contact_phone_norm = self._normalize_phone(contact_phone)
            match = False
            if match_name and contact_name == match_name:
                match = True
            elif match_phone_norm and contact_phone_norm and contact_phone_norm == match_phone_norm:
                match = True
            if match and contact_name not in seen_names:
                delete_items.append({"Name": contact_name, "Group": HA_CONTACT_GROUP_NAME})
                seen_names.add(contact_name)

        if delete_items:
            try:
                await api.contact_delete(delete_items)
            except Exception:
                pass

    async def _daily_contact_sync_cb(self, now):
        users_store = self._users_store()
        if not users_store:
            return

        raw_profiles = users_store.all()
        deleted_profiles: list[tuple[str, str, str]] = []
        for profile in (raw_profiles or {}).values():
            if not isinstance(profile, dict):
                continue
            status = str(profile.get("status") or "").strip().lower()
            if status != "deleted":
                continue
            name = str(profile.get("name") or "").strip()
            phone = str(profile.get("phone") or "").strip()
            normalized = self._normalize_phone(phone)
            deleted_profiles.append((name, phone, normalized))

        if not deleted_profiles:
            return

        for _, coord, api, _ in self._devices():
            try:
                response = await api.contact_get()
            except Exception:
                continue

            contacts = self._extract_contact_items(response)
            delete_items: list[dict[str, Any]] = []
            seen_names: set[str] = set()
            for contact in contacts:
                contact_name = str(contact.get("Name") or contact.get("name") or "").strip()
                if not contact_name:
                    continue
                if not _is_ha_group_record(contact):
                    continue
                contact_phone = str(
                    contact.get("Phone") or contact.get("PhoneNum") or contact.get("phone") or ""
                ).strip()
                contact_normalized = self._normalize_phone(contact_phone)
                for deleted_name, deleted_phone, deleted_normalized in deleted_profiles:
                    match = False
                    if deleted_name and contact_name == deleted_name:
                        match = True
                    elif deleted_phone and contact_phone == deleted_phone:
                        match = True
                    elif deleted_normalized and contact_normalized and deleted_normalized == contact_normalized:
                        match = True

                    if match:
                        if contact_name not in seen_names:
                            delete_items.append(
                                {"Name": contact_name, "Group": HA_CONTACT_GROUP_NAME}
                            )
                            seen_names.add(contact_name)
                        break
            if delete_items:
                try:
                    await api.contact_delete(delete_items)
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
                self._root()["sync_queue"].mark_change(
                    None,
                    delay_minutes=0,
                    trigger="scheduled auto sync",
                )
            except Exception:
                pass
            self.hass.async_create_task(
                self._root()["sync_queue"].sync_now(
                    None,
                    trigger="scheduled auto sync",
                )
            )  # type: ignore

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

    async def add_missing_users(self, entry_id: Optional[str] = None):
        targets: List[Tuple[str, AkuvoxCoordinator]] = []
        for entry, coord, *_ in self._devices():
            if entry_id and entry != entry_id:
                continue
            if coord:
                targets.append((entry, coord))

        for entry, coord in targets:
            try:
                coord.health["sync_status"] = "in_progress"
            except Exception:
                pass
            try:
                await self.reconcile_device(entry, full=False, add_missing_only=True)
                coord.health["sync_status"] = "in_sync"
                coord.health["last_sync"] = _now_hh_mm()
                try:
                    coord._append_event("Missing users added")  # type: ignore[attr-defined]
                except Exception:
                    pass
            except Exception as err:
                coord.health["sync_status"] = "pending"
                try:
                    coord._append_event(f"Missing users add failed: {err}")  # type: ignore[attr-defined]
                except Exception:
                    pass
            try:
                await coord.async_request_refresh()
            except Exception:
                pass

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
            dev_scheds = await api.schedule_get()  # [{"Name": "...", "DisplayID":"1"}, ...]
            for it in dev_scheds or []:
                n = str(it.get("Name") or "").strip()
                sid = str(it.get("DisplayID") or it.get("display_id") or "").strip()
                if not sid:
                    sid = str(
                        it.get("ScheduleID")
                        or it.get("ScheduleId")
                        or it.get("ID")
                        or it.get("Id")
                        or ""
                    ).strip()
                if n and sid:
                    name_to_id[n.lower()] = sid
        except Exception:
            # best-effort; built-ins still usable
            pass
        return name_to_id

    async def _set_user_on_device(
        self,
        api: AkuvoxAPI,
        desired: Dict[str, Any],
        ha_key: str,
        existing: Optional[Dict[str, Any]] = None,
        *,
        storage: Any = None,
    ):
        """Update the device record for ha_key in-place via user.set."""

        if "ID" not in desired and (not existing or not existing.get("ID")):
            device_id = _device_user_id(storage, ha_key)
            if device_id:
                desired = {**desired, "ID": device_id}

        payload = _prepare_user_set_payload(ha_key, desired, existing)

        try:
            await api.user_set([payload])
        except Exception as err:
            _LOGGER.warning("Failed to update user %s via user.set: %s", ha_key, err)
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
                    canonical = normalize_user_id(candidate)
                    if canonical:
                        active_device_keys.add(canonical)

        users_store = self._users_store()
        if not users_store:
            return

        try:
            registry_all = users_store.all()
        except Exception:
            registry_all = {}

        removed_profile = False
        for key, profile in (registry_all or {}).items():
            status_raw = str((profile or {}).get("status") or "").strip().lower()
            if status_raw != "deleted":
                continue

            canonical = normalize_user_id(key) or str(key)
            if canonical and canonical not in active_device_keys:
                try:
                    await users_store.delete(canonical)
                    removed_profile = True
                except Exception:
                    continue

        if removed_profile:
            await self._prune_stale_alert_users()

    async def _push_schedules(self, api: AkuvoxAPI, schedules: Dict[str, Any]):
        if not schedules:
            return
        device_schedule_names: Optional[set[str]] = None
        try:
            device_schedule_names = {
                str(it.get("Name") or "").strip().lower()
                for it in (await api.schedule_get()) or []
                if isinstance(it, dict)
            }
        except Exception:
            device_schedule_names = None
        for name, spec in (schedules or {}).items():
            if name in ("24/7 Access", "No Access"):
                continue
            sanitized: Dict[str, Any]
            if isinstance(spec, dict):
                if spec.get("system_exit_clone") or spec.get("exit_clone_for"):
                    continue
                sanitized = dict(spec)
                sanitized["days"] = list(spec.get("days") or [])
            else:
                sanitized = {}
            try:
                if device_schedule_names is not None and name.strip().lower() in device_schedule_names:
                    await api.schedule_set(name, sanitized)
                else:
                    await api.schedule_add(name, sanitized)
            except Exception:
                try:
                    await api.schedule_set(name, sanitized)
                except Exception:
                    pass

    async def _ensure_device_schedules(self, api: AkuvoxAPI, schedules: Dict[str, Any]) -> None:
        if not schedules:
            return
        device_schedule_names: Optional[set[str]] = None
        try:
            device_schedule_names = {
                str(it.get("Name") or "").strip().lower()
                for it in (await api.schedule_get()) or []
                if isinstance(it, dict)
            }
        except Exception:
            device_schedule_names = None

        for name, spec in (schedules or {}).items():
            if name in ("24/7 Access", "No Access"):
                continue
            if isinstance(spec, dict) and (spec.get("system_exit_clone") or spec.get("exit_clone_for")):
                continue
            if device_schedule_names is not None and name.strip().lower() in device_schedule_names:
                continue
            sanitized: Dict[str, Any]
            if isinstance(spec, dict):
                sanitized = dict(spec)
                sanitized["days"] = list(spec.get("days") or [])
            else:
                sanitized = {}
            try:
                await api.schedule_add(name, sanitized)
            except Exception:
                try:
                    await api.schedule_set(name, sanitized)
                except Exception:
                    pass

    async def _remove_missing_users(self, api: AkuvoxAPI, local_users: List[Dict[str, Any]], registry_keys_set: set):
        rogue_keys: List[str] = []
        for u in local_users or []:
            if not _is_ha_group_record(u):
                continue
            kid = _key_of_user(u)
            canonical_kid = normalize_user_id(kid)
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

    async def reconcile_device(
        self,
        entry_id: str,
        full: bool = True,
        *,
        add_missing_only: bool = False,
    ):
        root = self._root()
        data = root.get(entry_id)
        if not data:
            return
        coord: AkuvoxCoordinator = data.get("coordinator")
        api: AkuvoxAPI = data.get("api")
        opts = data.get("options") or {}
        if not coord or not api:
            return

        schedules_store = self._schedules_store()
        schedules_all: Dict[str, Any] = {}
        if schedules_store:
            try:
                schedules_all = schedules_store.all()
            except Exception:
                schedules_all = {}
        try:
            await self._ensure_device_schedules(api, schedules_all)
        except Exception:
            pass

        try:
            local_users: List[Dict[str, Any]] = await api.user_list()
        except Exception:
            local_users = list(coord.users or [])
        try:
            coord.users = local_users
        except Exception:
            pass
        await _store_device_user_ids(getattr(coord, "storage", None), local_users)

        try:
            await api.ensure_group_exists(HA_CONTACT_GROUP_NAME)
        except Exception:
            pass

        if not opts.get("sync_groups"):
            opts["sync_groups"] = ["Default"]

        device_groups: List[str] = list(opts.get("sync_groups", ["Default"]))
        users_store = self._users_store()
        device_type_raw = (coord.health.get("device_type") or "").strip()
        device_type = device_type_raw.lower()
        is_intercom = device_type == "intercom"

        raw_registry: Dict[str, Any] = users_store.all() if users_store else {}
        registry: Dict[str, Any] = {}
        for key, value in (raw_registry or {}).items():
            if _profile_is_empty_reserved(value or {}):
                continue
            canonical = normalize_user_id(key)
            if canonical:
                registry[canonical] = value
        registry_keys = list(registry.keys())
        registry_keys.sort(key=_user_id_sort_key)
        reg_key_set = set(registry_keys)

        auto_delete_keys: Set[str] = set()
        if not add_missing_only:
            for record in local_users or []:
                if not _is_ha_group_record(record):
                    continue
                name_value = record.get("Name")
                user_id_value = (
                    record.get("UserID")
                    or record.get("UserId")
                    or record.get("ID")
                )
                if _name_matches_user_id(name_value, user_id_value):
                    key = normalize_user_id(user_id_value) or str(user_id_value or "").strip()
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
                await self._prune_stale_alert_users()

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
        update_batch: List[Tuple[str, Dict[str, Any], Optional[Dict[str, Any]]]] = []
        delete_only_keys: List[str] = []
        contact_profiles: List[Tuple[str, str]] = []
        face_root_base = face_base_url(self.hass)
        desired_by_key: Dict[str, Dict[str, Any]] = {}
        face_upload_attempted: Set[str] = set()

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
            needs_group_move = False
            if local and not _is_ha_group_record(local):
                if should_have_access and not add_missing_only:
                    needs_group_move = True
                    local = {**local, "Group": HA_CONTACT_GROUP_NAME}
                else:
                    continue

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
            desired_by_key[ha_key] = desired_base

            if should_have_access:
                phone_text = str(prof.get("phone") or "").strip()
                paused = _coerce_bool(prof.get("paused")) is True
                if phone_text and not paused:
                    name_text = str(prof.get("name") or desired_base.get("Name") or ha_key).strip()
                    if name_text:
                        contact_profiles.append((name_text, phone_text))
                if add_missing_only:
                    if not local:
                        add_batch.append(desired_base)
                elif not local:
                    add_batch.append(desired_base)
                else:
                    replace = (
                        full
                        or str(prof.get("status") or "").lower() == "pending"
                        or not _record_matches_desired_fields(local, desired_base)
                    )
                    if replace or needs_group_move:
                        update_batch.append((ha_key, desired_base, local))
            else:
                if local and not add_missing_only:
                    delete_only_keys.append(ha_key)
            # -----------------------------------------

        # 1) Delete-only
        if not add_missing_only:
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

        # 2) Add new users
        if add_batch:
            prepared_add_batch = []
            for candidate in add_batch:
                ha_candidate = _key_of_user(candidate)
                prepared_add_batch.append(
                    _prepare_user_add_payload(ha_candidate, candidate, sources=(candidate,))
                )
            try:
                await api.user_add_missing(prepared_add_batch)
            except Exception:
                pass
            try:
                local_users = await api.user_list()
                coord.users = local_users
                await _store_device_user_ids(getattr(coord, "storage", None), local_users)
            except Exception:
                pass
            for candidate in add_batch:
                ha_candidate = _key_of_user(candidate)
                if not ha_candidate:
                    continue
                try:
                    face_upload_attempted.add(ha_candidate)
                    await self._upload_face_asset_to_device(
                        api,
                        coord,
                        ha_candidate,
                        candidate,
                        registry.get(ha_candidate) or {},
                        existing=_find_local_by_key(ha_candidate),
                        force=True,
                    )
                except Exception as err:
                    _LOGGER.debug("Post-add face upload failed for %s: %s", ha_candidate, err)

        # 3) Update changed users (delete + recreate to preserve face profile integrity)
        if not add_missing_only:
            for ha_key, desired, existing in update_batch:
                try:
                    await self._replace_user_on_device(
                        api,
                        ha_key,
                        desired,
                        existing=existing,
                    )
                    try:
                        coord._append_event(  # type: ignore[attr-defined]
                            f"User {ha_key} recreated from update payload"
                        )
                    except Exception:
                        pass
                    try:
                        face_upload_attempted.add(ha_key)
                        await self._upload_face_asset_to_device(
                            api,
                            coord,
                            ha_key,
                            desired,
                            registry.get(ha_key) or {},
                            force=True,
                        )
                    except Exception as err:
                        _LOGGER.debug("Post-update face upload failed for %s: %s", ha_key, err)
                except Exception:
                    latest: Optional[Dict[str, Any]] = None
                    try:
                        latest_records = await _lookup_device_user_ids_by_ha_key(api, ha_key)
                    except Exception:
                        latest_records = []
                    if latest_records:
                        latest = latest_records[0]
                    else:
                        latest = existing

                    diffs: List[str] = []
                    try:
                        diffs = _integrity_field_differences(
                            latest or {},
                            desired,
                            include_face=is_intercom,
                        )
                    except Exception:
                        diffs = ["unknown"]

                    try:
                        await self._replace_user_on_device(
                            api,
                            ha_key,
                            desired,
                            existing=latest,
                        )
                        try:
                            if diffs:
                                diff_text = ", ".join(diffs)
                                coord._append_event(  # type: ignore[attr-defined]
                                    f"User {ha_key} recreated after update issue ({diff_text})"
                                )
                            else:
                                coord._append_event(  # type: ignore[attr-defined]
                                    f"User {ha_key} recreated after update issue"
                                )
                        except Exception:
                            pass
                        try:
                            face_upload_attempted.add(ha_key)
                            await self._upload_face_asset_to_device(
                                api,
                                coord,
                                ha_key,
                                desired,
                                registry.get(ha_key) or {},
                                force=True,
                            )
                        except Exception as err:
                            _LOGGER.debug("Post-retry face upload failed for %s: %s", ha_key, err)
                    except Exception:
                        pass

        if is_intercom and not add_missing_only:
            for ha_key in registry_keys:
                if ha_key in face_upload_attempted:
                    continue
                prof = registry.get(ha_key) or {}
                face_status = str(prof.get("face_status") or "").strip().lower()
                if face_status not in {"pending", "error"}:
                    continue
                ha_groups = list(prof.get("groups") or ["Default"])
                if not any(g in device_groups for g in ha_groups):
                    continue
                desired = desired_by_key.get(ha_key)
                if not desired:
                    continue

                try:
                    face_upload_attempted.add(ha_key)
                    repaired = await self._upload_face_asset_to_device(
                        api,
                        coord,
                        ha_key,
                        desired,
                        prof,
                        existing=_find_local_by_key(ha_key),
                        force=True,
                    )
                except Exception as err:
                    _LOGGER.debug("Face repair upload failed for %s: %s", ha_key, err)
                    continue

                if not repaired:
                    continue

                try:
                    coord._append_event(  # type: ignore[attr-defined]
                        f"Uploaded face for {ha_key} due to stored face sync state"
                    )
                except Exception:
                    pass
                try:
                    local_users = await api.user_list()
                    coord.users = local_users
                    await _store_device_user_ids(getattr(coord, "storage", None), local_users)
                except Exception:
                    pass

        try:
            await self._sync_contacts_for_profiles(
                api,
                contact_profiles,
                prune_extra=not add_missing_only,
            )
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

        include_face = True
        settings = self._settings_store()
        if settings and hasattr(settings, "get_face_integrity_enabled"):
            try:
                include_face = settings.get_face_integrity_enabled()
            except Exception:
                include_face = True

        for _, coord, *_ in self._devices():
            if coord.health.get("sync_status") != "in_sync":
                return

        users_store = self._users_store()
        raw_registry = users_store.all() if users_store else {}
        registry: Dict[str, Any] = {}
        reg_keys: List[str] = []
        for key, value in (raw_registry or {}).items():
            if _profile_is_empty_reserved(value or {}):
                continue
            canonical = normalize_user_id(key)
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
                await _store_device_user_ids(getattr(coord, "storage", None), coord.users)
                device_records: Dict[str, List[Dict[str, Any]]] = {}
                for record in coord.users or []:
                    key = _key_of_user(record)
                    canonical_key = normalize_user_id(key)
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
                    diffs = _integrity_field_differences(local, desired, include_face=include_face)
                    if diffs:
                        face_mismatch = (
                            include_face
                            and any(diff in ("face status", "face url") for diff in diffs)
                        )
                        if face_mismatch and device_type_raw.lower() == "intercom":
                            profile_face_status = str(
                                (registry.get(ha_key) or {}).get("face_status") or ""
                            ).strip().lower()
                            force_recreate_on_error = profile_face_status == "error"
                            expected_face = _face_flag_from_record(desired)
                            expected_url = str(
                                desired.get("FaceUrl") or desired.get("FaceURL") or ""
                            ).strip()
                            if expected_url or expected_face:
                                if force_recreate_on_error:
                                    try:
                                        repaired = await self._upload_face_asset_to_device(
                                            api,
                                            coord,
                                            ha_key,
                                            desired,
                                            registry.get(ha_key) or {},
                                            existing=local,
                                            force=True,
                                        )
                                        if not repaired:
                                            raise RuntimeError("face upload repair did not complete")
                                        try:
                                            coord.users = await api.user_list()
                                            await _store_device_user_ids(
                                                getattr(coord, "storage", None),
                                                coord.users,
                                            )
                                        except Exception:
                                            pass
                                        try:
                                            coord._append_event(
                                                f"Uploaded face for {ha_key} due to face sync error state"
                                            )  # type: ignore[attr-defined]
                                        except Exception:
                                            pass
                                        continue
                                    except Exception as err:
                                        _LOGGER.warning(
                                            "Failed to repair face upload for %s after face error state: %s",
                                            ha_key,
                                            err,
                                        )
                                else:
                                    error_count = await self._bump_face_error_count(ha_key)
                                    if error_count >= FACE_SYNC_ERROR_THRESHOLD:
                                        try:
                                            repaired = await self._upload_face_asset_to_device(
                                                api,
                                                coord,
                                                ha_key,
                                                desired,
                                                registry.get(ha_key) or {},
                                                existing=local,
                                                force=True,
                                            )
                                            if not repaired:
                                                raise RuntimeError("face upload repair did not complete")
                                            try:
                                                coord.users = await api.user_list()
                                                await _store_device_user_ids(
                                                    getattr(coord, "storage", None),
                                                    coord.users,
                                                )
                                            except Exception:
                                                pass
                                            try:
                                                coord._append_event(
                                                    f"Uploaded face for {ha_key} after {error_count} face sync errors"
                                                )  # type: ignore[attr-defined]
                                            except Exception:
                                                pass
                                        except Exception as err:
                                            _LOGGER.warning(
                                                "Failed to repair face upload for %s after face sync errors: %s",
                                                ha_key,
                                                err,
                                            )
                        mismatch_reason = f"{ha_key} mismatch: {', '.join(diffs)}"
                        break

                if mismatch_reason is None:
                    for key, records in device_records.items():
                        canonical_key = normalize_user_id(key)
                        if not canonical_key:
                            continue
                        if canonical_key in should_have:
                            continue
                        if records:
                            mismatch_reason = f"rogue user {key}"
                            break

                if mismatch_reason is None and schedules_store:
                    try:
                        device_schedules = await api.schedule_get()
                    except Exception:
                        device_schedules = None

                    if device_schedules is not None:
                        device_map: Dict[str, Dict[str, Any]] = {}
                        for sched in device_schedules or []:
                            if not isinstance(sched, dict):
                                continue
                            name = str(sched.get("Name") or "").strip()
                            if not name:
                                continue
                            device_map[name.lower()] = _normalize_schedule_for_integrity(
                                schedules_store,
                                name,
                                sched,
                            )

                        for name, spec in (schedules_all or {}).items():
                            if not isinstance(spec, dict):
                                continue
                            if name in ("24/7 Access", "No Access"):
                                continue
                            if schedules_store._is_exit_clone(name, spec):
                                continue
                            expected_spec = _normalize_schedule_for_integrity(
                                schedules_store,
                                name,
                                spec,
                            )
                            if _schedule_times_out_of_order(expected_spec):
                                mismatch_reason = (
                                    f"schedule {name} times out of order"
                                    f" (expected {expected_spec.get('start')}-{expected_spec.get('end')})"
                                )
                                break
                            device_spec = device_map.get(name.strip().lower())
                            if not device_spec:
                                mismatch_reason = f"missing schedule {name}"
                                break
                            if _schedule_times_out_of_order(device_spec):
                                mismatch_reason = (
                                    f"schedule {name} times out of order"
                                    f" (device {device_spec.get('start')}-{device_spec.get('end')})"
                                )
                                break
                            if expected_spec.get("days") != device_spec.get("days"):
                                mismatch_reason = (
                                    f"schedule {name} days mismatch"
                                    f" (expected {expected_spec.get('days')},"
                                    f" device {device_spec.get('days')})"
                                )
                                break
                            if (
                                expected_spec.get("start") != device_spec.get("start")
                                or expected_spec.get("end") != device_spec.get("end")
                            ):
                                mismatch_reason = (
                                    f"schedule {name} time mismatch"
                                    f" (expected {expected_spec.get('start')}-{expected_spec.get('end')},"
                                    f" device {device_spec.get('start')}-{device_spec.get('end')})"
                                )
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
                        sq.mark_change(entry_id, full=True, trigger=f"integrity mismatch: {mismatch_reason}")
                        await sq.sync_now(
                            entry_id,
                            full=True,
                            trigger=f"integrity mismatch: {mismatch_reason}",
                        )
            except Exception:
                try:
                    coord._append_event("Integrity check error")  # type: ignore[attr-defined]
                except Exception:
                    pass


# ---------------------- Setup / teardown ---------------------- #
async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry):
    hass.data.setdefault(DOMAIN, {})
    root = hass.data[DOMAIN]

    try:
        _maybe_migrate_component_folder(hass)
    except Exception:
        pass

    _migrate_face_storage(hass)

    if "access_history" not in root:
        root["access_history"] = AccessHistory()

    if "groups_store" not in root:
        gs = AkuvoxGroupsStore(hass)
        await gs.async_load()
        root["groups_store"] = gs

    if "users_store" not in root:
        us = AkuvoxUsersStore(hass)
        await us.async_load()
        root["users_store"] = us

    if "schedules_store" not in root:
        schedules = AkuvoxSchedulesStore(hass)
        await schedules.async_load()
        root["schedules_store"] = schedules

    if "settings_store" not in root:
        settings = AkuvoxSettingsStore(hass)
        await settings.async_load()
        root["settings_store"] = settings

    settings_store = root.get("settings_store")
    users_store = root.get("users_store")
    if settings_store and users_store and hasattr(settings_store, "prune_stale_alert_users"):
        try:
            await settings_store.prune_stale_alert_users(users_store)
        except Exception:
            pass

    if "sync_manager" not in root:
        root["sync_manager"] = SyncManager(hass)
        hass.async_create_task(
            root["sync_manager"]._startup_user_cleanup()
        )

    if "sync_queue" not in root:
        root["sync_queue"] = SyncQueue(hass)

    if "hacs_auto_updater" not in root:
        root["hacs_auto_updater"] = HacsAutoUpdater(hass)
    try:
        root["hacs_auto_updater"].start()
    except Exception:
        pass

    await _remove_legacy_integration_device(hass, entry)
    await _remove_obsolete_device_entities(hass, entry)

    settings = root.get("settings_store")
    if settings:
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
    try:
        await asyncio.wait_for(coord.async_config_entry_first_refresh(), timeout=30)
    except asyncio.TimeoutError:
        _LOGGER.warning(
            "Initial refresh for %s timed out; continuing setup", entry.entry_id
        )
        hass.async_create_task(coord.async_refresh())
    except Exception as err:
        _LOGGER.warning(
            "Initial refresh for %s failed; continuing setup: %s",
            entry.entry_id,
            err,
        )
        hass.async_create_task(coord.async_refresh())

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
        temp_id = users_store.next_free_temp_id()
        users_store.reserve_temp_id(temp_id)
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

        face_url: Optional[str] = None
        if face_reference_supplied:
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

        pin_only = (
            pin_payload not in (None, "")
            and not face_reference_supplied
            and not face_bytes
            and not d.get("phone")
        )
        status_value = "active" if pin_only else "pending"

        await users_store.upsert_profile(
            temp_id,
            name=name,
            groups=groups,
            pin=pin_payload,
            phone=str(d.get("phone")) if d.get("phone") else None,
            schedule_name=d.get("schedule_name") or "24/7 Access",
            key_holder=bool(d.get("key_holder", False)),
            pedestrian_access=bool(d.get("pedestrian_access"))
            if "pedestrian_access" in d
            else None,
            access_level=d.get("access_level") or None,
            face_url=face_url,
            face_status="pending" if face_reference_supplied else None,
            face_synced_at="" if face_reference_supplied else None,
            status=status_value,
            schedule_id=str(d.get("schedule_id")) if d.get("schedule_id") else None,
            access_start=d.get("access_start") if "access_start" in d else date.today().isoformat(),
            access_end=d.get("access_end") if "access_end" in d else None,
            source="Local",
            exit_permission=d.get("exit_permission"),
        )

        if "notify_on_access" in d or "notify_targets" in d:
            settings_store: Optional[AkuvoxSettingsStore] = hass.data[DOMAIN].get("settings_store")
            await _set_notify_on_access_for_user(
                settings_store,
                temp_id,
                enabled=_coerce_bool(d.get("notify_on_access")) is True,
                selected_targets=d.get("notify_targets"),
            )

        hass.data[DOMAIN]["sync_queue"].mark_change(None, trigger="add_user service")

    async def svc_add_temporary_user(call):
        d = call.data
        name = str(d.get("name") or "").strip()
        if not name:
            return

        raw_pin = d.get("pin")
        pin_payload = str(raw_pin or "").strip()
        if not pin_payload:
            return

        users_store: AkuvoxUsersStore = hass.data[DOMAIN]["users_store"]
        temp_id = users_store.next_free_temp_id()
        users_store.reserve_temp_id(temp_id)
        await users_store.async_save()

        one_time = bool(d.get("one_time") or d.get("one_time_use") or d.get("one_time_code"))
        notify_on_access = _coerce_bool(d.get("notify_on_access")) is True
        access_start = d.get("access_start")
        access_end = d.get("access_end")
        expires_at = d.get("expires_at") or d.get("temporary_expires_at")

        await users_store.upsert_profile(
            temp_id,
            name=name,
            groups=[],
            pin=pin_payload,
            schedule_name="24/7 Access",
            key_holder=False,
            pedestrian_access=bool(d.get("pedestrian_access"))
            if "pedestrian_access" in d
            else None,
            status="active",
            schedule_id="1001",
            access_start=access_start if access_start is not None else date.today().isoformat(),
            access_end=access_end if access_end is not None else None,
            source="Temporary",
            temporary=True,
            temporary_one_time=one_time,
            temporary_expires_at=expires_at,
            temporary_created_at=dt_util.now(),
        )

        if "notify_on_access" in d or "notify_targets" in d:
            settings_store: Optional[AkuvoxSettingsStore] = hass.data[DOMAIN].get("settings_store")
            await _set_notify_on_access_for_user(
                settings_store,
                temp_id,
                enabled=notify_on_access,
                selected_targets=d.get("notify_targets"),
            )

        hass.data[DOMAIN]["sync_queue"].mark_change(
            None,
            delay_minutes=0,
            trigger="add_temporary_user service",
        )

    async def svc_edit_user(call):
        d = call.data
        raw_key = d.get("id")
        canonical_key = normalize_user_id(raw_key)
        key = canonical_key or str(raw_key)
        effective_id = canonical_key or key
        users_store: AkuvoxUsersStore = hass.data[DOMAIN]["users_store"]
        try:
            existing_profile = users_store.get(effective_id) or {}
        except Exception:
            existing_profile = {}
        face_reference_supplied = False
        new_face_url = None
        if "face_url" in d:
            new_face_url = str(d.get("face_url") or "").strip()
            face_reference_supplied = bool(new_face_url)
            if not face_reference_supplied:
                new_face_url = ""

        lp_payload = d.get("license_plate") if "license_plate" in d else None

        pin_payload_edit: Optional[str] = None
        if "pin" in d:
            raw_pin = d.get("pin")
            if raw_pin in (None, ""):
                pin_payload_edit = ""
            else:
                pin_payload_edit = str(raw_pin)

        paused_flag: Optional[bool] = None
        if "paused" in d:
            paused_flag = _coerce_bool(d.get("paused"))
        paused_schedule_id: Optional[str] = None
        if "paused_schedule_id" in d:
            paused_schedule_id = str(d.get("paused_schedule_id") or "").strip()
        paused_schedule_name: Optional[str] = None
        if "paused_schedule_name" in d:
            paused_schedule_name = str(d.get("paused_schedule_name") or "").strip()
        if paused_flag:
            existing_paused_id = str(existing_profile.get("paused_schedule_id") or "").strip()
            existing_paused_name = str(existing_profile.get("paused_schedule_name") or "").strip()
            existing_schedule_id = str(existing_profile.get("schedule_id") or "").strip()
            existing_schedule_name = str(existing_profile.get("schedule_name") or "").strip()

            if not paused_schedule_id:
                if existing_paused_id:
                    paused_schedule_id = existing_paused_id
                elif existing_schedule_id and existing_schedule_id != "1002":
                    paused_schedule_id = existing_schedule_id
                else:
                    paused_schedule_id = None
            if not paused_schedule_name:
                if existing_paused_name:
                    paused_schedule_name = existing_paused_name
                elif existing_schedule_name and existing_schedule_name.lower() != "no access":
                    paused_schedule_name = existing_schedule_name
                else:
                    paused_schedule_name = None

        pin_only = (
            pin_payload_edit not in (None, "")
            and not face_reference_supplied
            and not d.get("phone")
        )
        status_value = "active" if pin_only else "pending"
        face_status = "pending" if face_reference_supplied else None
        face_synced_at = "" if face_reference_supplied else None

        await users_store.upsert_profile(
            effective_id,
            name=d.get("name"),
            groups=list(d.get("groups") or []) if "groups" in d else None,
            pin=pin_payload_edit,
            phone=str(d.get("phone")) if "phone" in d else None,
            schedule_name=d.get("schedule_name") if "schedule_name" in d else None,
            key_holder=bool(d.get("key_holder")) if "key_holder" in d else None,
            pedestrian_access=bool(d.get("pedestrian_access"))
            if "pedestrian_access" in d
            else None,
            access_level=d.get("access_level") if "access_level" in d else None,
            face_url=new_face_url,
            face_status=face_status,
            face_synced_at=face_synced_at,
            status=status_value,
            schedule_id=str(d.get("schedule_id")) if d.get("schedule_id") else None,
            access_start=d.get("access_start") if "access_start" in d else None,
            access_end=d.get("access_end") if "access_end" in d else None,
            source="Local",
            license_plate=lp_payload,
            exit_permission=d.get("exit_permission") if "exit_permission" in d else None,
            paused=paused_flag,
            paused_schedule_id=paused_schedule_id,
            paused_schedule_name=paused_schedule_name,
        )

        if "notify_on_access" in d or "notify_targets" in d:
            settings_store: Optional[AkuvoxSettingsStore] = hass.data[DOMAIN].get("settings_store")
            await _set_notify_on_access_for_user(
                settings_store,
                effective_id,
                enabled=_coerce_bool(d.get("notify_on_access")) is True,
                selected_targets=d.get("notify_targets"),
            )

        hass.data[DOMAIN]["sync_queue"].mark_change(None, trigger="edit_user service")

    async def svc_reactivate_temporary_user(call):
        raw_key = call.data.get("id") or call.data.get("key")
        key = str(raw_key or "").strip()
        if not key:
            return

        canonical = normalize_user_id(key)
        effective_id = canonical or key
        users_store: AkuvoxUsersStore = hass.data[DOMAIN]["users_store"]
        today = date.today().isoformat()
        access_start = call.data.get("access_start") or today
        access_end = call.data.get("access_end")
        expires_at = call.data.get("expires_at") or call.data.get("temporary_expires_at")

        await users_store.upsert_profile(
            effective_id,
            access_start=access_start,
            access_end=access_end,
            schedule_name="24/7 Access",
            schedule_id="1001",
            status="active",
            temporary_used_at="",
            temporary_expires_at=expires_at if expires_at is not None else "",
        )

        hass.data[DOMAIN]["sync_queue"].mark_change(
            None,
            delay_minutes=0,
            trigger="reactivate_temporary_user service",
        )

    async def svc_delete_user(call):
        raw_key = call.data.get("id") or call.data.get("key")
        key = str(raw_key or "").strip()
        if not key:
            return

        canonical = normalize_user_id(key)
        lookup_key = canonical or key
        removal_keys = {key}
        if canonical:
            removal_keys.add(canonical)
        removal_norms = {normalize_user_id(value) for value in removal_keys}
        removal_norms.discard(None)

        root = hass.data.get(DOMAIN, {})
        users_store: Optional[AkuvoxUsersStore] = root.get("users_store")
        phone_to_remove: Optional[str] = None
        name_to_remove: Optional[str] = None
        if users_store:
            try:
                profile = users_store.get(lookup_key) or {}
                raw_phone = str(profile.get("phone") or "").strip()
                if raw_phone:
                    phone_to_remove = raw_phone
                raw_name = str(profile.get("name") or "").strip()
                if raw_name:
                    name_to_remove = raw_name
            except Exception:
                phone_to_remove = None
                name_to_remove = None
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

        settings_store: Optional[AkuvoxSettingsStore] = root.get("settings_store")
        if settings_store:
            try:
                targets = settings_store.get_alert_targets()
                updated = False
                for target, cfg in targets.items():
                    if not isinstance(cfg, dict):
                        continue
                    granted = cfg.get("granted") if isinstance(cfg.get("granted"), dict) else {}
                    users = list(granted.get("users") or [])
                    if not users:
                        continue
                    filtered = [
                        user
                        for user in users
                        if user not in removal_keys
                        and normalize_user_id(user) not in removal_norms
                    ]
                    if filtered != users:
                        granted["users"] = filtered
                        if not filtered:
                            granted["specific"] = False
                        cfg["granted"] = granted
                        targets[target] = cfg
                        updated = True
                if updated:
                    await settings_store.set_alert_targets(targets)
                if users_store and hasattr(settings_store, "prune_stale_alert_users"):
                    await settings_store.prune_stale_alert_users(users_store)
            except Exception:
                pass

        # immediate cascade: delete from every device using robust lookup
        manager: SyncManager | None = root.get("sync_manager")  # type: ignore[assignment]
        if manager:
            for entry_id, coord, api, _ in manager._devices():
                try:
                    if phone_to_remove or name_to_remove:
                        await manager._delete_contacts(
                            api,
                            name=name_to_remove,
                            phone=phone_to_remove,
                        )
                    id_records = await _lookup_device_user_ids_by_ha_key(
                        api,
                        lookup_key,
                        allow_non_ha_group=True,
                    )
                    if id_records:
                        for rec in id_records:
                            await _delete_user_every_way(api, rec)
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
            queue.mark_change(None, delay_minutes=0, full=True, trigger="delete_user service")

    async def svc_upload_face(call):
        """
        Legacy helper kept: simply records the canonical /api/AK_AC face URL.
        Actual file writing/placing happens outside this service.
        """
        d = call.data
        raw_key = d.get("id")
        canonical = normalize_user_id(raw_key)
        key = canonical or str(raw_key)
        face_url = await _ensure_local_face_for_user(canonical or key)
        users_store: AkuvoxUsersStore = hass.data[DOMAIN]["users_store"]
        await users_store.upsert_profile(canonical or key, face_url=face_url, status="pending")
        hass.data[DOMAIN]["sync_queue"].mark_change(None, trigger="upload_face service")

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
            await queue.sync_now(
                entry_id,
                include_all=include_all,
                full=True,
                trigger=f"force_full_sync service by {triggered_by}",
            )
        except Exception:
            pass

    async def svc_sync_now(call):
        data = call.data if isinstance(call.data, Mapping) else {}
        entry_id = data.get("entry_id")
        await hass.data[DOMAIN]["sync_queue"].sync_now(entry_id, trigger="sync_now service")

    async def svc_hacs_update_check(call):
        root = hass.data.get(DOMAIN, {})
        updater = root.get("hacs_auto_updater") if isinstance(root, dict) else None
        if updater and hasattr(updater, "async_run_check"):
            await updater.async_run_check(reason="service", force=True)

    async def svc_hacs_update_install(call):
        root = hass.data.get(DOMAIN, {})
        updater = root.get("hacs_auto_updater") if isinstance(root, dict) else None
        if updater and hasattr(updater, "async_install_update"):
            await updater.async_install_update(reason="service", force=True)

    async def svc_add_missing_users(call):
        data = call.data if isinstance(call.data, Mapping) else {}
        entry_id = data.get("entry_id")
        manager: SyncManager = hass.data[DOMAIN].get("sync_manager")  # type: ignore[assignment]
        if not manager:
            return
        await manager.add_missing_users(entry_id)

    async def svc_create_group(call):
        await hass.data[DOMAIN]["groups_store"].add_group(call.data["name"])

    async def svc_delete_groups(call):
        await hass.data[DOMAIN]["groups_store"].delete_groups(call.data.get("names") or [])

    async def svc_set_user_groups(call):
        key = str(call.data["key"])
        groups = list(call.data.get("groups") or [])
        await hass.data[DOMAIN]["users_store"].upsert_profile(key, groups=groups, status="pending")
        hass.data[DOMAIN]["sync_queue"].mark_change(
            None,
            delay_minutes=0,
            trigger="set_user_groups service",
        )

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
                queue.mark_change(entry_id, delay_minutes=0, trigger="set_exit_device service")

    async def svc_set_auto_reboot(call):
        time_hhmm = call.data.get("time")
        days = list(call.data.get("days") or [])
        hass.data[DOMAIN]["sync_manager"].set_auto_reboot(time_hhmm, days)

    async def svc_upsert_schedule(call):
        name = call.data["name"]
        spec = call.data["spec"]
        await hass.data[DOMAIN]["schedules_store"].upsert(name, spec)
        hass.data[DOMAIN]["sync_queue"].mark_change(
            None,
            delay_minutes=0,
            full=True,
            trigger="upsert_schedule service",
        )

    async def svc_delete_schedule(call):
        name = call.data["name"]
        await hass.data[DOMAIN]["schedules_store"].delete(name)
        hass.data[DOMAIN]["sync_queue"].mark_change(
            None,
            delay_minutes=0,
            full=True,
            trigger="delete_schedule service",
        )

    hass.services.async_register(DOMAIN, "add_user", svc_add_user)
    hass.services.async_register(DOMAIN, "add_temporary_user", svc_add_temporary_user)
    hass.services.async_register(DOMAIN, "edit_user", svc_edit_user)
    hass.services.async_register(
        DOMAIN, "reactivate_temporary_user", svc_reactivate_temporary_user
    )
    hass.services.async_register(DOMAIN, "delete_user", svc_delete_user)
    hass.services.async_register(DOMAIN, "upload_face", svc_upload_face)
    hass.services.async_register(DOMAIN, "reboot_device", svc_reboot_device)
    hass.services.async_register(DOMAIN, "refresh_events", svc_refresh_events)
    hass.services.async_register(DOMAIN, "force_full_sync", svc_force_full_sync)
    hass.services.async_register(DOMAIN, "sync_now", svc_sync_now)
    hass.services.async_register(DOMAIN, "hacs_update_check", svc_hacs_update_check)
    hass.services.async_register(DOMAIN, "hacs_update_install", svc_hacs_update_install)
    hass.services.async_register(DOMAIN, "add_missing_users", svc_add_missing_users)
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
                "hacs_auto_updater",
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

            updater = root.get("hacs_auto_updater")
            if updater and hasattr(updater, "shutdown"):
                try:
                    updater.shutdown()
                except Exception:
                    pass

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
