from __future__ import annotations

import asyncio
import logging
import datetime as dt
from datetime import timedelta
import time
import re
from typing import Any, Dict, List, Optional, Tuple, Callable, Awaitable

from homeassistant.core import HomeAssistant
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator
from homeassistant.helpers.event import async_call_later

from .const import DOMAIN, EVENT_NON_KEY_ACCESS_GRANTED, DEFAULT_ACCESS_HISTORY_LIMIT
from .ha_id import normalize_ha_id, normalize_user_id
from .api import AkuvoxAPI
from .http import (
    _build_phone_index,
    _call_entry_is_received,
    _call_entry_number,
    _call_entry_id,
    _call_entry_timestamp,
    _call_entry_type,
    _digits_only,
    _match_user_by_number,
    _normalize_call_number,
)
from .access_history import AccessHistory, categorize_event


CALLER_LOOKBACK_SECONDS = 120
CALLER_CLEAR_DELAY_SECONDS = 10
CALLER_EVENT_WINDOW_SECONDS = 30
ACCESS_PERMITTED_NOTIFICATION_WINDOW_SECONDS = 10
NOTIFICATION_DIAGNOSTICS_LIMIT = 200
USER_LIST_REFRESH_INTERVAL_SECONDS = 300


_LOGGER = logging.getLogger(__name__)


_TIME_ONLY_RE = re.compile(r"^\d{1,2}:\d{2}:\d{2}(?:\.\d+)?$")


def _safe_str(x) -> str:
    try:
        return str(x)
    except Exception:
        return ""


def _now_iso(hass: HomeAssistant) -> str:
    from homeassistant.util import dt as dt_util
    return dt_util.utcnow().isoformat() + "Z"


def _canonical_notify_user_id(value: Any) -> Optional[str]:
    if value in (None, ""):
        return None
    text = _safe_str(value).strip()
    if not text:
        return None
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


def _derive_targets_from_raw(raw: Any, event_type: str, *, user_id: Optional[str] = None) -> List[str]:
    out: List[str] = []
    if not isinstance(raw, dict):
        return out

    norm_user = _canonical_notify_user_id(user_id)
    for target, cfg in raw.items():
        if not isinstance(target, str) or not target:
            continue
        config = cfg if isinstance(cfg, dict) else {}
        if event_type == "device_offline" and config.get("device_offline"):
            out.append(target)
        elif event_type == "integrity_failed" and config.get("integrity_failed"):
            out.append(target)
        elif event_type == "any_denied" and config.get("any_denied"):
            out.append(target)
        elif event_type == "user_granted":
            granted_cfg = config.get("granted")
            if isinstance(granted_cfg, dict):
                any_flag = bool(granted_cfg.get("any"))
                users_raw = granted_cfg.get("users")
                specific_flag = bool(granted_cfg.get("specific")) if "specific" in granted_cfg else bool(users_raw)
            else:
                any_flag = bool(config.get("granted_any"))
                users_raw = config.get("granted_users")
                specific_flag = bool(users_raw)
            if any_flag:
                out.append(target)
            elif norm_user and specific_flag and users_raw:
                if isinstance(users_raw, (list, tuple, set)):
                    normalized = [str(u).strip() for u in users_raw if str(u).strip()]
                elif isinstance(users_raw, str):
                    normalized = [users_raw.strip()]
                else:
                    normalized = []
                if any(_notify_user_matches(item, norm_user) for item in normalized):
                    out.append(target)
    return out


def _alert_targets_for_event(hass: HomeAssistant, event_type: str, *, user_id: Optional[str] = None) -> List[str]:
    root = hass.data.get(DOMAIN, {}) or {}
    settings = root.get("settings_store")
    if not settings:
        return []

    resolver = getattr(settings, "targets_for_event", None)
    if callable(resolver):
        try:
            return list(resolver(event_type, user_id=user_id))
        except Exception:
            pass

    data = getattr(settings, "data", {})
    alerts = {}
    if isinstance(data, dict):
        alerts = data.get("alerts") or {}
    targets = alerts.get("targets") if isinstance(alerts, dict) else {}
    return _derive_targets_from_raw(targets, event_type, user_id=user_id)


class AkuvoxCoordinator(DataUpdateCoordinator):
    """Polls device, tracks health/events/users, and keeps a stable friendly name."""

    def __init__(self, hass: HomeAssistant, api: AkuvoxAPI, storage, entry_id: str, device_name: str):
        # NOTE: DataUpdateCoordinator.name is used by HA logs; keep it technical
        super().__init__(hass, _LOGGER, name=f"akuvox_ac:{entry_id}", update_interval=timedelta(seconds=30))

        self.api = api
        self.entry_id = entry_id
        self.storage = storage
        if "last_access" not in getattr(self.storage, "data", {}):
            self.storage.data["last_access"] = {}
        if "door_events" not in self.storage.data:
            self.storage.data["door_events"] = {}
        notifications = self.storage.data.get("notifications")
        if not isinstance(notifications, dict):
            self.storage.data["notifications"] = {}
        notification_diagnostics = self.storage.data.get("notification_diagnostics")
        if not isinstance(notification_diagnostics, list):
            self.storage.data["notification_diagnostics"] = []
        alerts_state = self.storage.data.get("alerts_state")
        if not isinstance(alerts_state, dict):
            self.storage.data["alerts_state"] = {}
        self._alerts_state = self.storage.data.get("alerts_state", {})

        # Friendly display name (persist and surface in multiple places for UI)
        self.device_name: str = device_name or "Akuvox Device"
        self.friendly_name: str = self.device_name  # some UIs look for this

        self.health: Dict[str, Any] = {
            "name": self.device_name,  # always keep friendly name here for UI
            "device_type": "",         # set by __init__.py
            "ip": "",
            "online": False,
            "status": "offline",
            # Restarting Home Assistant or the device does not imply that
            # credentials are out of sync. Explicit changes and integrity
            # checks move this state to pending when work is actually needed.
            "sync_status": "in_sync",
            "last_sync": None,
            "last_error": None,
            "last_ping": None,
        }
        self.users: List[Dict[str, Any]] = []
        self._last_user_refresh_monotonic = 0.0
        self.schedule_ids: Dict[str, str] = {
            "24/7 Access": "1001",
            "No Access": "1002",
        }
        self.events: List[Dict[str, Any]] = []  # newest first
        self._was_online: Optional[bool] = None
        self.event_state: Dict[str, Any] = {
            "last_user_name": None,
            "last_user_id": None,
            "last_event_type": None,
            "last_event_summary": None,
            "last_event_timestamp": None,
            "last_event_key_holder": None,
            "granted_active": False,
            "granted_key_holder_active": False,
            "denied_active": False,
        }
        self._event_reset_handles: Dict[str, Callable[[], None]] = {}
        self.caller_state: Dict[str, Any] = self._empty_caller_state()
        self._caller_reset_handle: Optional[Callable[[], None]] = None

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

    def set_users(self, users: List[Dict[str, Any]]) -> None:
        """Store a fresh device user snapshot and record its fetch time."""
        self.users = list(users or [])
        self._last_user_refresh_monotonic = time.monotonic()

    async def async_refresh_users(self, *, force: bool = False) -> List[Dict[str, Any]]:
        """Refresh the full user list only when its slower cache is due."""
        now = time.monotonic()
        last_refresh = float(getattr(self, "_last_user_refresh_monotonic", 0.0) or 0.0)
        if (
            not force
            and last_refresh > 0
            and now - last_refresh < USER_LIST_REFRESH_INTERVAL_SECONDS
        ):
            return list(self.users or [])

        users = await self.api.user_list()
        if isinstance(users, list):
            self.set_users(users)
        return list(self.users or [])

    def _append_event(self, text: str):
        evt = {"timestamp": _now_iso(self.hass), "Event": text}
        self.events.insert(0, evt)
        # keep a generous history to make UI feel “unlimited”
        self.events[:] = self.events[:1000]

    async def _kick_sync_now(self):
        """Ask the SyncQueue to sync this device immediately."""
        try:
            root = self.hass.data.get(DOMAIN, {}) or {}
            sq = root.get("sync_queue")
            if sq:
                # Surface an in-progress state immediately so dashboards update
                self.health["sync_status"] = "in_progress"
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
        now_ts = time.time()
        alerts_state: Dict[str, Any] = self.storage.data.setdefault("alerts_state", {})
        alerts_dirty = False
        alerts_saved = False
        reboot_raw = self.health.get("rebooting_until")
        if isinstance(reboot_raw, (int, float)):
            reboot_deadline = float(reboot_raw)
        else:
            try:
                reboot_deadline = float(reboot_raw)
            except (TypeError, ValueError):
                reboot_deadline = 0.0
        reboot_active = reboot_deadline and reboot_deadline > now_ts
        try:
            info = await self.api.ping_info()
            last_ping = info
            is_up = bool(info.get("ok"))
            prev = self._was_online
            self._was_online = is_up

            if is_up:
                if reboot_active and self.health.get("status") == "rebooting" and prev is not False:
                    self.health["status"] = "rebooting"
                    self.health["online"] = False
                    self.health["last_error"] = None
                    self.health["last_ping"] = last_ping
                    return

                self.health["status"] = "online"
                self.health["online"] = True
                if reboot_deadline:
                    self.health.pop("rebooting_until", None)
                if prev is False:
                    self._append_event("Device came online")
                elif prev is None and not self.health.get("last_sync"):
                    # Avoid repairing users during HA startup before FaceData routes
                    # are guaranteed to be registered. Pending profile work is picked
                    # up by SyncQueue after Home Assistant has fully started.
                    pass
                if alerts_state.get("offline_since"):
                    alerts_state["offline_since"] = None
                    alerts_state["offline_notified"] = False
                    alerts_dirty = True
            else:
                self.health["online"] = False
                if reboot_active:
                    self.health["status"] = "rebooting"
                    self._was_online = False
                    self.health["last_error"] = None
                    self.health["last_ping"] = last_ping
                    return

                if reboot_deadline and reboot_deadline <= now_ts and self.health.get("status") == "rebooting":
                    self.health.pop("rebooting_until", None)
                    try:
                        self._append_event("Device still offline after reboot window")
                    except Exception:
                        pass

                self.health["status"] = "offline"
                self.health.pop("rebooting_until", None)
                if prev is True or prev is None:
                    self._append_event("Device went offline")
                self.health["last_error"] = None
                self.health["last_ping"] = last_ping
                offline_since = alerts_state.get("offline_since")
                if not offline_since:
                    alerts_state["offline_since"] = now_ts
                    alerts_state["offline_notified"] = False
                    alerts_dirty = True
                else:
                    try:
                        offline_since_val = float(offline_since)
                    except Exception:
                        offline_since_val = now_ts
                        alerts_state["offline_since"] = offline_since_val
                        alerts_dirty = True
                    if not alerts_state.get("offline_notified") and now_ts - offline_since_val >= 300:
                        try:
                            await self._send_alert_notification("device_offline", extra={"offline_since": offline_since_val})
                        except Exception as err:
                            _LOGGER.debug("Failed to dispatch offline notification: %s", _safe_str(err))
                        alerts_state["offline_notified"] = True
                        alerts_dirty = True
                if alerts_dirty:
                    try:
                        await self.storage.async_save()
                        alerts_saved = True
                    except Exception as err:
                        _LOGGER.debug("Failed to persist offline state: %s", _safe_str(err))
                return

            self.health["last_ping"] = last_ping

            # Load users so integrity checker & UI can see them
            try:
                await self.async_refresh_users()
            except Exception:
                # don't fail the whole refresh just because the user list failed
                pass

            await self._process_door_events()

        except Exception as e:
            last_error = _safe_str(e)
            prev = self._was_online
            self._was_online = False
            self.health["online"] = False
            if reboot_active:
                self.health["status"] = "rebooting"
            else:
                self.health["status"] = "offline"
                if prev is True or prev is None:
                    try:
                        self._append_event("Device went offline")
                    except Exception:
                        pass
                if not alerts_state.get("offline_since"):
                    alerts_state["offline_since"] = now_ts
                    alerts_state["offline_notified"] = False
                    alerts_dirty = True
        finally:
            self.health["last_error"] = last_error
            self.health["last_ping"] = last_ping

        if alerts_dirty and not alerts_saved:
            try:
                await self.storage.async_save()
            except Exception as err:
                _LOGGER.debug("Failed to persist alert state: %s", _safe_str(err))

    async def _process_door_events(
        self,
        *,
        force_latest: bool = False,
        suppress_notifications: bool = False,
    ) -> List[Dict[str, Any]]:
        """Fetch recent door events and handle non-key access notifications."""

        notifications = self.storage.data.get("notifications") or {}
        configured_notify_targets: List[str] = list(notifications.get("targets") or [])
        notify_targets: List[str] = [] if suppress_notifications else configured_notify_targets

        events: List[Dict[str, Any]] = []

        try:
            raw_events = await self.api.events_last()
        except Exception as err:
            _LOGGER.debug("Failed to fetch door events: %s", _safe_str(err))
            return events

        if isinstance(raw_events, list):
            events = list(raw_events)
        elif isinstance(raw_events, dict):
            events = [raw_events]
        else:
            events = []

        if not events:
            return events

        try:
            self._publish_access_history(events)
        except Exception as err:
            _LOGGER.debug(
                "Unable to publish door events to history for %s: %s",
                self.entry_id,
                _safe_str(err),
            )

        state = self.storage.data.setdefault("door_events", {})
        last_seen = _safe_str(state.get("last_event_key")) or None
        last_seen_epoch_raw = state.get("last_event_epoch")
        try:
            last_seen_epoch = float(last_seen_epoch_raw)
        except (TypeError, ValueError):
            last_seen_epoch = 0.0

        events_to_process: List[Tuple[str, Dict[str, Any], float]] = []
        for event in reversed(events):
            key = self._event_unique_key(event)
            if key is None:
                continue
            if last_seen and key == last_seen:
                # Drop everything collected so far (they are older events).
                events_to_process = []
                continue
            timestamp_text = self._extract_event_timestamp(event, fallback=False)
            parsed_ts = 0.0
            if timestamp_text:
                parsed_ts = self._coerce_event_timestamp_to_epoch(timestamp_text)
            if parsed_ts and parsed_ts <= last_seen_epoch:
                continue
            events_to_process.append((key, event, parsed_ts))

        if not events_to_process:
            if force_latest:
                latest_event: Optional[Dict[str, Any]] = None
                latest_epoch = -1.0
                for event in events:
                    timestamp_text = self._extract_event_timestamp(event, fallback=False)
                    parsed_ts = self._coerce_event_timestamp_to_epoch(timestamp_text) if timestamp_text else 0.0
                    if parsed_ts > latest_epoch:
                        latest_epoch = parsed_ts
                        latest_event = event
                if latest_event:
                    if suppress_notifications:
                        latest_event = dict(latest_event)
                        latest_event["_skip_notifications"] = True
                        latest_event["_suppressed_notification_targets"] = configured_notify_targets
                    storage_dirty = await self._handle_door_event(latest_event, notify_targets)
                    if storage_dirty:
                        try:
                            await self.storage.async_save()
                        except Exception as err:
                            _LOGGER.debug(
                                "Unable to persist door event state: %s",
                                _safe_str(err),
                            )
            return events

        # Avoid processing an unbounded backlog.
        max_events = 25
        if len(events_to_process) > max_events:
            events_to_process = events_to_process[-max_events:]

        storage_dirty = False
        last_processed_key = last_seen
        last_processed_epoch = last_seen_epoch
        for key, event, parsed_ts in events_to_process:
            if suppress_notifications:
                event = dict(event)
                event["_skip_notifications"] = True
                event["_suppressed_notification_targets"] = configured_notify_targets
            if await self._handle_door_event(event, notify_targets):
                storage_dirty = True
            last_processed_key = key
            if parsed_ts > last_processed_epoch:
                last_processed_epoch = parsed_ts

        if last_processed_key and last_processed_key != last_seen:
            state["last_event_key"] = last_processed_key
            storage_dirty = True

        if last_processed_epoch > last_seen_epoch:
            state["last_event_epoch"] = last_processed_epoch
            storage_dirty = True

        if storage_dirty:
            try:
                await self.storage.async_save()
            except Exception as err:
                _LOGGER.debug("Unable to persist door event state: %s", _safe_str(err))

        return events

    def _event_unique_key(self, event: Dict[str, Any]) -> Optional[str]:
        """Generate a stable identifier for a door event."""

        for key in ("Index", "ID", "LogID", "LogId", "EventID", "EventId", "SN", "Sn"):
            val = event.get(key)
            if val not in (None, ""):
                return _safe_str(val)

        timestamp = self._extract_event_timestamp(event, fallback=False)
        description = None
        for key in ("Event", "EventType", "Type", "Description"):
            value = event.get(key)
            if value not in (None, ""):
                description = _safe_str(value)
                break
        user_id = self._extract_event_user_id(event)
        parts = [p for p in (timestamp, description, user_id) if p]
        if parts:
            return "|".join(parts)

        if event:
            try:
                return "|".join(f"{k}:{_safe_str(event.get(k))}" for k in sorted(event.keys()))
            except Exception:
                pass

        return None

    @staticmethod
    def _clean_event_component(value: Any) -> Optional[str]:
        if value is None:
            return None
        text = _safe_str(value).strip()
        if not text:
            return None
        lowered = text.lower()
        if lowered in {"-", "--", "—", "n/a", "none", "null"}:
            return None
        return text

    @classmethod
    def _event_date_component(cls, event: Dict[str, Any]) -> Optional[str]:
        for key in ("DateTime", "datetime", "Date", "date", "EventDate", "LogDate"):
            text = cls._clean_event_component(event.get(key))
            if text:
                return text
        return None

    @classmethod
    def _event_time_component(cls, event: Dict[str, Any]) -> Optional[str]:
        for key in ("Time", "time", "EventTime", "LogTime", "RecordTime", "timestamp", "Timestamp"):
            text = cls._clean_event_component(event.get(key))
            if text:
                return text
        return None

    @classmethod
    def _combine_event_date_time(cls, event: Dict[str, Any]) -> Optional[str]:
        date_text = cls._event_date_component(event)
        time_text = cls._event_time_component(event)
        if date_text and time_text:
            return f"{date_text} {time_text}"
        return date_text or time_text

    def _extract_event_timestamp(self, event: Dict[str, Any], *, fallback: bool = True) -> Optional[str]:
        date_text = self._event_date_component(event)
        time_only: Optional[str] = None

        for key in (
            "Time",
            "time",
            "DateTime",
            "datetime",
            "Timestamp",
            "timestamp",
            "CreateTime",
            "RecordTime",
            "LogTime",
            "EventTime",
        ):
            raw = event.get(key)
            cleaned = self._clean_event_component(raw)
            if not cleaned:
                continue
            lowered = key.lower()
            if lowered in {"time", "timestamp", "recordtime", "logtime", "eventtime"}:
                if lowered == "time" and _TIME_ONLY_RE.match(cleaned):
                    if date_text:
                        return f"{date_text} {cleaned}"
                    time_only = time_only or cleaned
                    continue
                return cleaned
            return cleaned

        combined = self._combine_event_date_time(event)
        if combined:
            return combined

        if time_only:
            return time_only

        if fallback:
            return _now_iso(self.hass)
        return None

    def _extract_event_user_id(self, event: Dict[str, Any]) -> Optional[str]:
        for key in (
            "UserID",
            "UserId",
            "User",
            "UserName",
            "Name",
            "CardNo",
            "CardNumber",
            "ID",
        ):
            val = event.get(key)
            if val not in (None, ""):
                return _safe_str(val)
        return None

    @staticmethod
    def _event_user_candidates(event: Dict[str, Any]) -> List[str]:
        candidates: List[str] = []
        seen: set[str] = set()
        for key in (
            "UserID",
            "UserId",
            "user_id",
            "User",
            "UserName",
            "user_name",
            "Name",
            "name",
            "CardNo",
            "CardNumber",
            "ID",
        ):
            value = event.get(key)
            if value in (None, ""):
                continue
            text = _safe_str(value).strip()
            if not text:
                continue
            lookup_key = text.casefold()
            if lookup_key in seen:
                continue
            seen.add(lookup_key)
            candidates.append(text)
        return candidates

    def _resolve_event_user_id(self, event: Dict[str, Any]) -> Optional[str]:
        """Resolve an event user reference to a canonical profile identifier when possible."""

        candidates = self._event_user_candidates(event)
        if not candidates:
            return None

        raw_text = candidates[0]
        if not raw_text:
            return None

        normalized_raw = normalize_user_id(raw_text)
        resolved = normalized_raw or raw_text

        def _candidate_matches(value: Any, candidate: str) -> bool:
            text = _safe_str(value).strip()
            if not text:
                return False
            if text.casefold() == candidate.casefold():
                return True
            text_norm = normalize_user_id(text)
            candidate_norm = normalize_user_id(candidate)
            return bool(text_norm and candidate_norm and text_norm == candidate_norm)

        def _matches_any(value: Any) -> bool:
            return any(_candidate_matches(value, candidate) for candidate in candidates)

        try:
            storage_data = getattr(self.storage, "data", {}) or {}
        except Exception:
            storage_data = {}
        stored_user_ids = storage_data.get("user_ids") if isinstance(storage_data, dict) else {}
        if isinstance(stored_user_ids, dict):
            for ha_key, device_id in stored_user_ids.items():
                canonical = normalize_user_id(ha_key) or _safe_str(ha_key).strip()
                if not canonical:
                    continue
                if _matches_any(ha_key) or _matches_any(device_id):
                    return canonical

        try:
            root = self.hass.data.get(DOMAIN, {}) or {}
            users_store = root.get("users_store")
            registry = users_store.all() if users_store and hasattr(users_store, "all") else {}
        except Exception:
            registry = {}
        if isinstance(registry, dict):
            for key, profile in registry.items():
                canonical = normalize_user_id(key) or _safe_str(key).strip()
                if not canonical:
                    continue
                values: List[Any] = [key, canonical]
                if isinstance(profile, dict):
                    values.extend(
                        [
                            profile.get("name"),
                            profile.get("Name"),
                            profile.get("UserName"),
                            profile.get("UserID"),
                            profile.get("UserId"),
                            profile.get("ID"),
                            profile.get("device_id"),
                            profile.get("card_code"),
                            profile.get("CardCode"),
                        ]
                    )
                    device_ids = profile.get("device_ids") or profile.get("device_user_ids")
                    if isinstance(device_ids, dict):
                        values.extend(device_ids.values())
                    elif isinstance(device_ids, (list, tuple, set)):
                        values.extend(device_ids)
                if any(_matches_any(value) for value in values):
                    return canonical

        users = self.users if isinstance(self.users, list) else []
        if not users:
            return resolved

        def _user_id_from_record(record: Dict[str, Any]) -> Optional[str]:
            for key in ("UserID", "UserId", "user_id", "id"):
                value = record.get(key)
                if value in (None, ""):
                    continue
                text = _safe_str(value).strip()
                if not text:
                    continue
                return normalize_user_id(text) or text

            device_id = record.get("ID")
            if device_id not in (None, ""):
                text = _safe_str(device_id).strip()
                if text:
                    return normalize_user_id(text) or text
            return None

        for user in users:
            if not isinstance(user, dict):
                continue
            user_id = _user_id_from_record(user)
            if not user_id:
                continue
            for key in (
                "ID",
                "UserID",
                "UserId",
                "id",
                "user_id",
                "Name",
                "name",
                "UserName",
                "username",
                "User",
                "user",
                "CardNo",
                "CardNumber",
            ):
                value = user.get(key)
                if value in (None, ""):
                    continue
                text = _safe_str(value).strip()
                if not text:
                    continue
                if any(_candidate_matches(text, candidate) for candidate in candidates):
                    return user_id

        return resolved

    def _is_non_key_access(self, event: Dict[str, Any]) -> bool:
        text_parts: List[str] = []
        for key in (
            "Event",
            "EventType",
            "Type",
            "OpenMethod",
            "AccessMethod",
            "OpenDoorType",
            "Mode",
            "Way",
        ):
            val = event.get(key)
            if isinstance(val, str) and val:
                text_parts.append(val.lower())

        if not text_parts:
            return False

        summary = " ".join(text_parts)
        has_grant = any(
            word in summary
            for word in (
                "grant",
                "granted",
                "permit",
                "permitted",
                "open",
                "unlock",
                "success",
                "allowed",
            )
        )
        if not has_grant:
            return False

        uses_key = any(word in summary for word in ("card", "rfid", "key", "tag", "fob"))
        return not uses_key

    def _event_summary_tokens(self, event: Dict[str, Any]) -> List[str]:
        tokens: List[str] = []
        for key in (
            "Event",
            "EventType",
            "Type",
            "Description",
            "Reason",
            "OpenMethod",
            "AccessMethod",
            "Mode",
            "Way",
        ):
            val = event.get(key)
            if isinstance(val, str) and val:
                tokens.append(val.lower())
        status = event.get("Status")
        if isinstance(status, str) and status:
            tokens.append(status.strip().lower())
        return tokens

    def _event_is_access_denied(self, tokens: List[str]) -> bool:
        if not tokens:
            return False
        summary = " ".join(tokens)
        denied_words = (
            "denied",
            "refused",
            "invalid",
            "failed",
            "fail",
            "error",
            "unauthorized",
            "forbidden",
            "rejected",
        )
        return any(word in summary for word in denied_words)

    def _event_is_access_granted(self, tokens: List[str]) -> bool:
        if not tokens:
            return False
        if self._event_is_access_denied(tokens):
            return False
        summary = " ".join(tokens)
        granted_words = (
            "grant",
            "granted",
            "permit",
            "permitted",
            "allowed",
            "success",
            "succ",
            "opened",
            "open",
            "unlock",
            "passed",
            "access ok",
        )
        return any(word in summary for word in granted_words)

    @staticmethod
    def _is_access_permitted_button_event(event: Dict[str, Any]) -> bool:
        label = _safe_str(event.get("Event")).strip().lower()
        return label == "access permitted button pressed"

    def _has_recent_door_event(self, window_seconds: float) -> bool:
        door_events = self.storage.data.get("door_events") or {}
        last_epoch_raw = door_events.get("last_event_epoch")
        try:
            last_epoch = float(last_epoch_raw)
        except (TypeError, ValueError):
            return False
        if last_epoch <= 0:
            return False
        return (time.time() - last_epoch) <= window_seconds

    async def async_handle_manual_event(self, event: Dict[str, Any]) -> None:
        if not isinstance(event, dict):
            return

        notifications = self.storage.data.get("notifications") or {}
        notify_targets: List[str] = list(notifications.get("targets") or [])
        notification_diag_dirty = False
        if self._is_access_permitted_button_event(event):
            try:
                user_id = self._resolve_event_user_id(event)
            except Exception:
                user_id = self._extract_event_user_id(event)
            user_name = self._extract_event_user_name(event)
            alert_targets = _alert_targets_for_event(
                self.hass,
                "user_granted",
                user_id=user_id,
            )
            planned_targets = self._dedupe_notification_target_items(
                self._notification_target_items(
                    notify_targets,
                    channel="access_notification",
                )
                + self._notification_target_items(
                    alert_targets,
                    channel="alert_notification",
                )
            )
            has_recent_door_event = self._has_recent_door_event(
                ACCESS_PERMITTED_NOTIFICATION_WINDOW_SECONDS
            )
            if not has_recent_door_event:
                event["_skip_notifications"] = True
                notify_targets = []
            notification_diag_dirty = self._record_notification_diagnostic(
                source="access_permitted_button",
                channel="decision",
                event_type="user_granted",
                status="ready" if has_recent_door_event else "skipped",
                targets=planned_targets,
                user_id=user_id,
                user_name=user_name,
                event_summary=self._notification_event_summary(event),
                reason=None
                if has_recent_door_event
                else "No recent door event was available for the Access Permitted button.",
            )

        storage_dirty = await self._handle_door_event(event, notify_targets)
        if storage_dirty or notification_diag_dirty:
            try:
                await self.storage.async_save()
            except Exception as err:
                _LOGGER.debug("Unable to persist manual door event state: %s", _safe_str(err))

    def _event_timestamp_to_epoch(self, timestamp: Any) -> float:
        value = self._coerce_event_timestamp_to_epoch(timestamp)
        if value:
            return value
        try:
            return float(time.time())
        except Exception:
            return 0.0

    def _coerce_event_timestamp_to_epoch(self, timestamp: Any) -> float:
        from homeassistant.util import dt as dt_util

        if isinstance(timestamp, (int, float)):
            try:
                return float(timestamp)
            except Exception:
                return 0.0
        if isinstance(timestamp, dt.datetime):
            try:
                return dt_util.as_utc(timestamp).timestamp()
            except Exception:
                return 0.0
        if timestamp in (None, ""):
            return 0.0
        try:
            text = str(timestamp).strip()
        except Exception:
            return 0.0
        if not text:
            return 0.0

        parsed = dt_util.parse_datetime(text)
        if not parsed:
            normalized = text.replace(" ", "T")
            parsed = dt_util.parse_datetime(normalized)
        if not parsed:
            cleaned = text.replace("T", " ").split("+", 1)[0].split("Z", 1)[0]
            for fmt in ("%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M:%S", "%d/%m/%Y %H:%M:%S"):
                try:
                    parsed = dt.datetime.strptime(cleaned, fmt)
                    break
                except Exception:
                    continue
        if not parsed:
            return 0.0
        try:
            return dt_util.as_utc(parsed).timestamp()
        except Exception:
            return 0.0

    def _prepare_access_history_events(self, events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        prepared: List[Dict[str, Any]] = []
        if not events:
            return prepared

        now_iso = _now_iso(self.hass)

        for event in events:
            if not isinstance(event, dict):
                continue

            base_key = self._event_unique_key(event)
            if not base_key:
                continue

            timestamp_text = self._extract_event_timestamp(event) or now_iso
            ts_value = self._event_timestamp_to_epoch(timestamp_text)

            combined_key = f"{self.entry_id}:{base_key}"

            copy = dict(event)
            copy.setdefault("timestamp", timestamp_text)
            copy.setdefault("Time", timestamp_text)
            copy["_key"] = combined_key
            copy["_device"] = self.device_name
            copy["_device_id"] = self.entry_id
            copy["_source"] = "doorlog"
            copy["_category"] = categorize_event(copy, self.health)
            copy["_t"] = ts_value

            tokens = self._event_summary_tokens(event)
            if tokens:
                if self._event_is_access_denied(tokens):
                    copy.setdefault("Result", "Access denied")
                elif self._event_is_access_granted(tokens):
                    copy.setdefault("Result", "Access granted")

            prepared.append(copy)

        prepared.sort(key=lambda e: e.get("_t", 0.0), reverse=True)
        return prepared

    def _publish_access_history(self, events: List[Dict[str, Any]]) -> None:
        if not events:
            return

        try:
            root = self.hass.data.get(DOMAIN, {}) or {}
        except Exception:
            return

        history = root.get("access_history")
        if history is None or not hasattr(history, "ingest"):
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

        prepared = self._prepare_access_history_events(events)
        if not prepared or limit <= 0:
            return

        try:
            history.ingest(prepared, limit)
        except Exception as err:
            _LOGGER.debug(
                "Failed to update aggregated access history for %s: %s",
                self.entry_id,
                _safe_str(err),
            )

    async def _handle_door_event(self, event: Dict[str, Any], notify_targets: List[str]) -> bool:
        """Handle a single door event, firing HA events as needed."""

        storage_changed = False
        skip_notifications = bool(event.get("_skip_notifications"))
        last_access = self.storage.data.setdefault("last_access", {})

        user_id = self._resolve_event_user_id(event)
        timestamp = self._extract_event_timestamp(event)
        if user_id and timestamp:
            if last_access.get(user_id) != timestamp:
                last_access[user_id] = timestamp
                storage_changed = True

        if self._is_non_key_access(event):
            payload = {
                "entry_id": self.entry_id,
                "device_name": self.device_name,
                "event": event,
                "user_id": user_id,
                "timestamp": timestamp,
            }
            try:
                self.hass.bus.async_fire(EVENT_NON_KEY_ACCESS_GRANTED, payload)
            except Exception as err:
                _LOGGER.debug("Failed to emit non-key access event: %s", _safe_str(err))
            if notify_targets and not skip_notifications:
                await self._dispatch_notification(event, notify_targets)

        tokens = self._event_summary_tokens(event)
        summary_text = " ".join(tokens)
        event_kind: Optional[str] = None
        if tokens and self._event_is_access_denied(tokens):
            if not skip_notifications:
                try:
                    await self._send_alert_notification(
                        "any_denied",
                        user_id=user_id,
                        summary=summary_text,
                        extra={"event": event},
                    )
                except Exception as err:
                    _LOGGER.debug("Failed to dispatch denied alert: %s", _safe_str(err))
            event_kind = "denied"
        elif tokens and self._event_is_access_granted(tokens):
            if not skip_notifications:
                try:
                    await self._send_alert_notification(
                        "user_granted",
                        user_id=user_id,
                        summary=summary_text,
                        extra={"event": event},
                    )
                except Exception as err:
                    _LOGGER.debug("Failed to dispatch granted alert: %s", _safe_str(err))
            event_kind = "granted"

        if event_kind:
            if skip_notifications and self._record_suppressed_notification_diagnostic(
                event,
                event_kind=event_kind,
                user_id=user_id,
                summary=summary_text or None,
                notify_targets=notify_targets,
            ):
                storage_changed = True
            self._update_access_state(event_kind, event, user_id=user_id, summary=summary_text or None)
            if event_kind == "granted":
                manager = self.hass.data.get(DOMAIN, {}).get("sync_manager")
                if manager:
                    try:
                        await manager.handle_access_granted(
                            user_id,
                            user_name=self._extract_event_user_name(event),
                        )
                    except Exception as err:
                        _LOGGER.debug(
                            "Temporary user cleanup failed after access grant: %s",
                            _safe_str(err),
                        )

        return storage_changed

    def _update_access_state(
        self,
        kind: str,
        event: Dict[str, Any],
        *,
        user_id: Optional[str],
        summary: Optional[str],
    ) -> None:
        state = self.event_state

        timestamp = self._extract_event_timestamp(event) or _now_iso(self.hass)
        if state.get("last_event_timestamp") != timestamp:
            state["last_event_timestamp"] = timestamp

        summary_text = summary or " ".join(self._event_summary_tokens(event)) or None
        if state.get("last_event_summary") != summary_text:
            state["last_event_summary"] = summary_text

        if state.get("last_event_type") != kind:
            state["last_event_type"] = kind

        if state.get("last_user_id") != user_id:
            state["last_user_id"] = user_id

        name = self._extract_event_user_name(event) or user_id or ""
        if not name:
            name = "Unknown"
        if state.get("last_user_name") != name:
            state["last_user_name"] = name

        key_holder = self._extract_event_key_holder(event, user_id=user_id)
        if state.get("last_event_key_holder") != key_holder:
            state["last_event_key_holder"] = key_holder

        if kind == "granted":
            self._activate_event_flag("granted_active")
            if key_holder:
                self._activate_event_flag("granted_key_holder_active")
            else:
                self._deactivate_event_flag("granted_key_holder_active")
        elif kind == "denied":
            self._activate_event_flag("denied_active")

        # Always notify listeners so downstream sensors refresh even if the
        # latest event reuses existing state (e.g. duplicate grant within timer).
        self.async_update_listeners()

    def _parse_access_timestamp(self, value: Any) -> Optional[dt.datetime]:
        from homeassistant.util import dt as dt_util

        if isinstance(value, dt.datetime):
            return dt_util.as_utc(value)
        if isinstance(value, (int, float)):
            return dt_util.utc_from_timestamp(value)
        if isinstance(value, str):
            parsed = dt_util.parse_datetime(value)
            if parsed:
                return dt_util.as_utc(parsed)
        return None

    def _lookup_user_name(self, user_id: Optional[str]) -> Optional[str]:
        if not user_id:
            return None
        for user in self.users or []:
            raw_id = (
                user.get("UserID")
                or user.get("UserId")
                or user.get("ID")
                or user.get("id")
                or ""
            )
            if str(raw_id).strip() == user_id:
                name = user.get("Name") or user.get("UserName") or user.get("name") or user.get("User")
                if name:
                    return str(name)
        return None

    def get_last_access_snapshot(self) -> Dict[str, Any]:
        last_access = self.storage.data.get("last_access", {})
        if not isinstance(last_access, dict):
            return {}

        latest_user_id: Optional[str] = None
        latest_timestamp: Optional[str] = None
        latest_dt: Optional[dt.datetime] = None
        for user_id, timestamp in last_access.items():
            if not user_id or not timestamp:
                continue
            parsed = self._parse_access_timestamp(timestamp)
            if not parsed:
                continue
            if latest_dt is None or parsed > latest_dt:
                latest_dt = parsed
                latest_user_id = str(user_id).strip()
                latest_timestamp = timestamp

        user_name = self._lookup_user_name(latest_user_id)
        return {
            "user_id": latest_user_id,
            "user_name": user_name,
            "timestamp": latest_timestamp,
            "timestamp_dt": latest_dt,
        }

    def _activate_event_flag(self, flag: str) -> bool:
        prev = bool(self.event_state.get(flag))
        self.event_state[flag] = True
        handle = self._event_reset_handles.pop(flag, None)
        if handle:
            try:
                handle()
            except Exception:
                pass

        def _reset(_now):
            self.event_state[flag] = False
            self._event_reset_handles.pop(flag, None)
            self.async_update_listeners()

        self._event_reset_handles[flag] = async_call_later(self.hass, 3, _reset)
        return not prev

    def _deactivate_event_flag(self, flag: str) -> bool:
        if not self.event_state.get(flag):
            return False
        self.event_state[flag] = False
        handle = self._event_reset_handles.pop(flag, None)
        if handle:
            try:
                handle()
            except Exception:
                pass
        return True

    def _empty_caller_state(self) -> Dict[str, Any]:
        return {
            "caller_id": None,
            "caller_name": None,
            "caller_number": None,
            "raw_number": None,
            "digits": None,
            "call_id": None,
            "call_type": None,
            "timestamp": None,
            "age_seconds": None,
            "key_holder": None,
            "status": None,
            "error": None,
            "source": None,
        }

    def _cancel_caller_reset(self) -> None:
        handle = self._caller_reset_handle
        if handle:
            try:
                handle()
            except Exception:
                pass
        self._caller_reset_handle = None

    def _schedule_caller_clear(self) -> None:
        self._cancel_caller_reset()

        def _reset(_now):
            self._caller_reset_handle = None
            self._set_caller_state(self._empty_caller_state(), auto_clear=False)

        self._caller_reset_handle = async_call_later(self.hass, CALLER_CLEAR_DELAY_SECONDS, _reset)

    def _set_caller_state(self, state: Dict[str, Any], *, auto_clear: bool) -> None:
        self.caller_state = state
        self.async_update_listeners()
        if auto_clear:
            self._schedule_caller_clear()
        else:
            self._cancel_caller_reset()

    def _extract_event_user_name(self, event: Dict[str, Any]) -> Optional[str]:
        for key in (
            "Name",
            "UserName",
            "User",
            "UserID",
            "UserId",
            "ID",
            "CardNo",
            "CardNumber",
        ):
            val = event.get(key)
            if val in (None, ""):
                continue
            text = _safe_str(val).strip()
            if text:
                return text
        return None

    def _extract_event_key_holder(
        self, event: Dict[str, Any], *, user_id: Optional[str]
    ) -> Optional[bool]:
        for key in ("key_holder", "KeyHolder", "keyHolder"):
            if key in event:
                flag = event.get(key)
                if isinstance(flag, bool):
                    return flag
                if isinstance(flag, (int, float)):
                    return bool(flag)
                if isinstance(flag, str):
                    lower = flag.strip().lower()
                    if lower in {"1", "true", "t", "yes", "y", "on"}:
                        return True
                    if lower in {"0", "false", "f", "no", "n", "off"}:
                        return False

        if not user_id:
            return None

        canonical = normalize_ha_id(user_id) or user_id
        if not canonical:
            return None

        try:
            root = self.hass.data.get(DOMAIN, {}) or {}
            store = root.get("users_store")
        except Exception:
            store = None

        if not store:
            return None

        try:
            profile = store.get(canonical)
        except Exception:
            profile = None

        if isinstance(profile, dict) and "key_holder" in profile:
            try:
                return bool(profile.get("key_holder"))
            except Exception:
                return None

        return None

    async def async_refresh_access_history(
        self,
        *,
        force_latest: bool = False,
        suppress_notifications: bool = False,
    ) -> List[Dict[str, Any]]:
        events: List[Dict[str, Any]] = []
        try:
            result = await self._process_door_events(
                force_latest=force_latest,
                suppress_notifications=suppress_notifications,
            )
            if isinstance(result, list):
                events = result
        finally:
            self.async_update_listeners()
        return events

    async def async_refresh_inbound_call_history(self):
        try:
            from .http import _process_inbound_call_webhook

            await _process_inbound_call_webhook(self.hass)
        except Exception as err:
            _LOGGER.debug("Failed to refresh inbound call history: %s", _safe_str(err))

    async def async_fetch_current_caller(self) -> Dict[str, Any]:
        state = self._empty_caller_state()
        state["source"] = "call_log"

        try:
            log_items = await self.api.call_log()
        except Exception as err:
            _LOGGER.debug(
                "Failed to fetch current caller for %s: %s",
                self.entry_id,
                _safe_str(err),
            )
            state["status"] = "error"
            state["error"] = _safe_str(err) or "call_log_error"
            self._set_caller_state(state, auto_clear=True)
            return state

        if isinstance(log_items, dict):
            items: List[Dict[str, Any]] = [log_items]
        elif isinstance(log_items, list):
            items = log_items
        else:
            items = []

        now_local = dt.datetime.now()
        now_utc = dt.datetime.now(dt.timezone.utc)

        best: Optional[Dict[str, Any]] = None

        for raw in items:
            if not isinstance(raw, dict):
                continue

            call_type = _call_entry_type(raw) or ""
            if not _call_entry_is_received(call_type or ""):
                continue

            timestamp = _call_entry_timestamp(raw)
            if not isinstance(timestamp, dt.datetime):
                continue

            try:
                if timestamp.tzinfo is None:
                    age_seconds = (now_local - timestamp).total_seconds()
                else:
                    age_seconds = (now_utc - timestamp.astimezone(dt.timezone.utc)).total_seconds()
            except Exception:
                continue

            if age_seconds < 0:
                continue

            if CALLER_LOOKBACK_SECONDS and age_seconds > CALLER_LOOKBACK_SECONDS:
                continue

            raw_number = _call_entry_number(raw) or ""
            normalized = _normalize_call_number(raw_number)
            digits = _digits_only(normalized)

            candidate = {
                "raw": raw,
                "call_type": call_type or "received",
                "timestamp": timestamp,
                "age_seconds": round(age_seconds, 2),
                "raw_number": raw_number,
                "normalized": normalized,
                "digits": digits,
                "call_id": _call_entry_id(raw),
            }

            if best is None or candidate["age_seconds"] < best["age_seconds"]:
                best = candidate

        if not best:
            state["status"] = "no_match"
            state["error"] = "no_recent_call"
            self._set_caller_state(state, auto_clear=True)
            return state

        root = self.hass.data.get(DOMAIN, {}) or {}
        try:
            phone_index = _build_phone_index(root)
        except Exception:
            phone_index = []

        match = None
        digits = best.get("digits") or ""
        if digits:
            try:
                match = _match_user_by_number(digits, phone_index)
            except Exception as err:
                _LOGGER.debug(
                    "Failed to match caller digits for %s: %s",
                    self.entry_id,
                    _safe_str(err),
                )

        timestamp = best.get("timestamp")
        if isinstance(timestamp, dt.datetime):
            state["timestamp"] = timestamp.isoformat()

        state.update(
            {
                "call_id": best.get("call_id") or None,
                "call_type": best.get("call_type"),
                "raw_number": best.get("raw_number"),
                "caller_number": best.get("normalized")
                or best.get("raw_number")
                or (digits or None),
                "digits": digits or None,
                "age_seconds": best.get("age_seconds"),
                "key_holder": False,
                "status": "unmatched",
                "error": None,
            }
        )

        if match:
            state["caller_id"] = match.get("ha_id")
            state["caller_name"] = match.get("name")
            if match.get("number"):
                state["caller_number"] = match.get("number")
            state["key_holder"] = bool(match.get("key_holder"))
            state["status"] = "matched"

        self._set_caller_state(state, auto_clear=True)
        return state

    async def async_refresh_caller_via_button(self) -> None:
        """Button workflow: refresh caller, then align with door events."""

        pressed_at = dt.datetime.now(dt.timezone.utc)
        state = await self.async_fetch_current_caller()

        status = (state or {}).get("status") or ""
        if status != "matched":
            return

        await asyncio.sleep(1)

        events_by_device = await self._refresh_access_histories_for_all_devices()
        if not events_by_device:
            return

        self._link_caller_state_to_events(state, pressed_at, events_by_device)

    async def _refresh_access_histories_for_all_devices(self) -> List[Tuple["AkuvoxCoordinator", List[Dict[str, Any]]]]:
        root = self.hass.data.get(DOMAIN, {}) or {}
        if not isinstance(root, dict):
            return []

        coords: List[AkuvoxCoordinator] = []
        tasks: List[Awaitable[List[Dict[str, Any]]]] = []

        for value in root.values():
            if not isinstance(value, dict):
                continue
            coord = value.get("coordinator")
            if not isinstance(coord, AkuvoxCoordinator):
                continue
            coords.append(coord)
            tasks.append(coord.async_refresh_access_history())

        if not tasks:
            return []

        results: List[Tuple[AkuvoxCoordinator, List[Dict[str, Any]]]] = []
        responses = await asyncio.gather(*tasks, return_exceptions=True)
        for coord, response in zip(coords, responses):
            if isinstance(response, Exception):
                continue
            if isinstance(response, list):
                events = response
            elif response is None:
                events = []
            else:
                try:
                    events = list(response)
                except Exception:
                    events = []
            results.append((coord, events))

        return results

    def _link_caller_state_to_events(
        self,
        caller_state: Dict[str, Any],
        pressed_at: dt.datetime,
        events_by_device: List[Tuple["AkuvoxCoordinator", List[Dict[str, Any]]]],
    ) -> None:
        if not caller_state:
            return

        caller_id = caller_state.get("caller_id") or None
        normalized_caller_id = normalize_ha_id(caller_id) if caller_id else None
        caller_name_raw = caller_state.get("caller_name")
        caller_name = _safe_str(caller_name_raw).strip() if caller_name_raw else None
        caller_name_lower = caller_name.lower() if caller_name else None
        key_holder_flag = caller_state.get("key_holder")

        pressed_epoch = 0.0
        try:
            if pressed_at.tzinfo is None:
                pressed_epoch = pressed_at.replace(tzinfo=dt.timezone.utc).timestamp()
            else:
                pressed_epoch = pressed_at.astimezone(dt.timezone.utc).timestamp()
        except Exception:
            try:
                pressed_epoch = float(time.time())
            except Exception:
                pressed_epoch = 0.0

        best_match: Optional[Dict[str, Any]] = None

        for coord, events in events_by_device:
            if not events:
                continue
            for event in events:
                if not isinstance(event, dict):
                    continue

                timestamp_text = coord._extract_event_timestamp(event, fallback=False)
                if not timestamp_text:
                    continue
                event_epoch = coord._coerce_event_timestamp_to_epoch(timestamp_text)
                if not event_epoch:
                    continue

                delta = abs(event_epoch - pressed_epoch)
                if CALLER_EVENT_WINDOW_SECONDS and delta > CALLER_EVENT_WINDOW_SECONDS:
                    continue

                tokens = coord._event_summary_tokens(event)
                if not coord._event_is_access_granted(tokens):
                    continue

                event_user_id = coord._extract_event_user_id(event)
                event_user_name = coord._extract_event_user_name(event)
                event_name_lower = event_user_name.lower() if isinstance(event_user_name, str) else None

                matches = False
                if normalized_caller_id and event_user_id:
                    if normalize_ha_id(event_user_id) == normalized_caller_id:
                        matches = True
                if not matches and caller_name_lower and event_name_lower:
                    if event_name_lower == caller_name_lower:
                        matches = True

                if not matches:
                    continue

                if best_match is None or delta < best_match["delta"]:
                    best_match = {
                        "coord": coord,
                        "event": event,
                        "delta": delta,
                        "event_epoch": event_epoch,
                        "user_id": event_user_id,
                        "user_name": event_user_name,
                    }

        if not best_match:
            return

        coord: AkuvoxCoordinator = best_match["coord"]
        event = dict(best_match["event"] or {})

        if key_holder_flag is not None and "key_holder" not in event:
            normalized_flag = bool(key_holder_flag)
            event["key_holder"] = normalized_flag
            event.setdefault("KeyHolder", normalized_flag)
            event.setdefault("keyHolder", normalized_flag)

        user_name = best_match.get("user_name")
        if isinstance(user_name, str) and user_name.strip():
            display_name = user_name.strip()
        elif caller_name:
            display_name = caller_name
        else:
            display_name = caller_state.get("caller_number") or caller_state.get("raw_number") or "Unknown caller"

        summary = f"Access granted to {display_name} by Call to open"

        try:
            coord._append_event(summary)
        except Exception:
            pass

        coord._update_access_state(
            "granted",
            event,
            user_id=best_match.get("user_id"),
            summary=summary,
        )

    def _notification_event_summary(self, event: Optional[Dict[str, Any]]) -> str:
        if not isinstance(event, dict):
            return ""
        for key in ("Event", "EventType", "Description", "Message", "Result", "Type"):
            value = event.get(key)
            if value in (None, ""):
                continue
            text = _safe_str(value).strip()
            if text:
                return text
        return ""

    @staticmethod
    def _access_method_label(event: Optional[Dict[str, Any]]) -> Optional[str]:
        """Return the user-facing access method reported by a door event."""

        if not isinstance(event, dict):
            return None

        for key in (
            "AccessMethod",
            "OpenMethod",
            "OpenDoorType",
            "AccessMode",
            "Method",
            "CredentialType",
            "Type",
            "EventType",
            "Event",
            "Description",
            "Mode",
            "Way",
        ):
            value = event.get(key)
            if value in (None, ""):
                continue
            text = re.sub(r"[_-]+", " ", _safe_str(value)).strip().casefold()
            if not text:
                continue
            compact = re.sub(r"[^a-z0-9]+", "", text)

            if re.search(r"\bface\b|\bfacial\b", text):
                return "Face"
            if (
                "dtmf" in compact
                or "calltoopen" in compact
                or "openbycall" in compact
                or "callunlock" in compact
                or re.search(r"\bphone call\b|\bsip call\b", text)
            ):
                return "Call"
            if (
                compact in {"pin", "privatepin", "publicpin", "passcode", "keypad", "code"}
                or re.search(
                    r"\bprivate pin\b|\bpublic pin\b|\bpin code\b|\bpasscode\b"
                    r"|\bkeypad\b|\baccess code\b|\bdoor code\b|\bpassword\b",
                    text,
                )
            ):
                return "code"

        return None

    def _access_granted_notification_message(
        self,
        event: Optional[Dict[str, Any]],
        user_name: Optional[str] = None,
    ) -> str:
        who = user_name
        if not who and isinstance(event, dict):
            who = self._extract_event_user_name(event) or self._extract_event_user_id(event)
        who = who or "Unknown user"
        method = self._access_method_label(event)
        suffix = f" via {method}" if method else ""
        return f"{who} opened the gate{suffix}."

    def _notification_target_items(
        self,
        targets: List[Any],
        *,
        channel: Optional[str] = None,
    ) -> List[Dict[str, str]]:
        items: List[Dict[str, str]] = []
        for target in targets or []:
            text = _safe_str(target).strip()
            if not text:
                continue
            item = {
                "target": text,
                "target_label": self._format_notification_target(text),
            }
            if channel:
                item["channel"] = channel
            items.append(item)
        return items

    @staticmethod
    def _dedupe_notification_target_items(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        seen: set[Tuple[str, str]] = set()
        out: List[Dict[str, Any]] = []
        for item in items or []:
            if not isinstance(item, dict):
                continue
            target = _safe_str(item.get("target")).strip()
            if not target:
                continue
            channel = _safe_str(item.get("channel")).strip()
            key = (channel, target)
            if key in seen:
                continue
            seen.add(key)
            out.append(dict(item))
        return out

    def _record_notification_diagnostic(
        self,
        *,
        source: str,
        channel: str,
        event_type: str,
        status: str,
        target: Optional[Any] = None,
        targets: Optional[List[Dict[str, Any]]] = None,
        title: Optional[str] = None,
        message: Optional[str] = None,
        user_id: Optional[str] = None,
        user_name: Optional[str] = None,
        event_summary: Optional[str] = None,
        reason: Optional[str] = None,
        error: Optional[str] = None,
    ) -> bool:
        data = getattr(self.storage, "data", None)
        if not isinstance(data, dict):
            return False

        diagnostics = data.get("notification_diagnostics")
        if not isinstance(diagnostics, list):
            diagnostics = []
            data["notification_diagnostics"] = diagnostics

        record: Dict[str, Any] = {
            "timestamp": _now_iso(self.hass),
            "entry_id": self.entry_id,
            "device_name": self.device_name,
            "source": source,
            "channel": channel,
            "event_type": event_type,
            "status": status,
        }

        target_text = _safe_str(target).strip() if target not in (None, "") else ""
        if target_text:
            record["target"] = target_text
            record["target_label"] = self._format_notification_target(target_text)

        if targets is not None:
            record["targets"] = self._dedupe_notification_target_items(targets)

        for key, value in (
            ("title", title),
            ("message", message),
            ("user_id", user_id),
            ("user_name", user_name),
            ("event_summary", event_summary),
            ("reason", reason),
            ("error", error),
        ):
            if value in (None, ""):
                continue
            record[key] = _safe_str(value)

        diagnostics.insert(0, record)
        del diagnostics[NOTIFICATION_DIAGNOSTICS_LIMIT:]
        return True

    def _record_suppressed_notification_diagnostic(
        self,
        event: Dict[str, Any],
        *,
        event_kind: str,
        user_id: Optional[str],
        summary: Optional[str],
        notify_targets: List[str],
    ) -> bool:
        if self._is_access_permitted_button_event(event):
            return False

        alert_event_type = "user_granted" if event_kind == "granted" else "any_denied"
        alert_targets = _alert_targets_for_event(
            self.hass,
            alert_event_type,
            user_id=user_id,
        )

        suppressed_targets_raw = event.get("_suppressed_notification_targets")
        access_targets: List[str] = []
        if isinstance(suppressed_targets_raw, (list, tuple, set)):
            access_targets = [
                str(target).strip()
                for target in suppressed_targets_raw
                if target not in (None, "") and str(target).strip()
            ]
        elif isinstance(suppressed_targets_raw, str) and suppressed_targets_raw.strip():
            access_targets = [suppressed_targets_raw.strip()]
        elif notify_targets:
            access_targets = list(notify_targets)

        planned_targets = self._dedupe_notification_target_items(
            self._notification_target_items(
                access_targets,
                channel="access_notification",
            )
            + self._notification_target_items(
                alert_targets,
                channel="alert_notification",
            )
        )

        return self._record_notification_diagnostic(
            source="suppressed_door_event",
            channel="decision",
            event_type=alert_event_type,
            status="skipped",
            targets=planned_targets,
            user_id=user_id,
            user_name=self._extract_event_user_name(event),
            event_summary=self._notification_event_summary(event) or summary,
            reason="Notifications were suppressed for this access history refresh.",
        )

    async def _async_save_notification_diagnostics(self) -> None:
        saver = getattr(self.storage, "async_save", None)
        if not callable(saver):
            return
        try:
            await saver()
        except Exception as err:
            _LOGGER.debug("Failed to persist notification diagnostics: %s", _safe_str(err))

    async def _dispatch_notification(self, event: Dict[str, Any], notify_targets: List[str]) -> None:
        """Send notifications for a door event (best effort)."""

        if not notify_targets:
            return

        service = getattr(self.hass, "services", None)
        if service is None or not hasattr(service, "async_call"):
            for target in notify_targets:
                self._record_notification_diagnostic(
                    source="access_event",
                    channel="access_notification",
                    event_type="non_key_access",
                    status="skipped",
                    target=target,
                    title=self.device_name,
                    message=self._notification_event_summary(event),
                    user_id=self._extract_event_user_id(event),
                    user_name=self._extract_event_user_name(event),
                    event_summary=self._notification_event_summary(event),
                    reason="Home Assistant notify service was unavailable.",
                )
            await self._async_save_notification_diagnostics()
            return

        user_label = self._extract_event_user_name(event) or self._extract_event_user_id(event)
        message = self._access_granted_notification_message(event, user_label)
        access_method = self._access_method_label(event)
        notification_data: Dict[str, Any] = {
            "event": event,
            "device_name": self.device_name,
        }
        if access_method:
            notification_data["access_method"] = access_method
        data = {
            "message": message,
            "title": self.device_name,
            "data": notification_data,
        }

        notification_diag_dirty = False
        for target in notify_targets:
            target_label = self._format_notification_target(target)
            user_label = user_label or "Unknown user"
            try:
                await service.async_call("notify", target, data, blocking=False)
                self._append_event(
                    f"System notification sent to {target_label} — {message.rstrip('.')}"
                )
                notification_diag_dirty = self._record_notification_diagnostic(
                    source="access_event",
                    channel="access_notification",
                    event_type="non_key_access",
                    status="sent",
                    target=target,
                    title=self.device_name,
                    message=message,
                    user_id=self._extract_event_user_id(event),
                    user_name=user_label,
                    event_summary=self._notification_event_summary(event),
                ) or notification_diag_dirty
            except Exception as err:
                self._append_event(
                    f"System notification failed for {target_label} — {_safe_str(err)}"
                )
                notification_diag_dirty = self._record_notification_diagnostic(
                    source="access_event",
                    channel="access_notification",
                    event_type="non_key_access",
                    status="failed",
                    target=target,
                    title=self.device_name,
                    message=message,
                    user_id=self._extract_event_user_id(event),
                    user_name=user_label,
                    event_summary=self._notification_event_summary(event),
                    error=_safe_str(err),
                ) or notification_diag_dirty
                _LOGGER.debug("Failed to dispatch notification to %s: %s", target, _safe_str(err))
        if notification_diag_dirty:
            await self._async_save_notification_diagnostics()

    @staticmethod
    def _format_notification_target(target: Any) -> str:
        raw = _safe_str(target).strip()
        if not raw:
            return "unknown target"
        normalized = raw
        if normalized.lower().startswith("mobile_app_"):
            normalized = normalized[len("mobile_app_") :]
        normalized = normalized.replace("_", " ").replace(".", " ")
        normalized = " ".join(part for part in normalized.split() if part)
        return normalized or raw

    async def _send_alert_notification(
        self,
        event_type: str,
        *,
        user_id: Optional[str] = None,
        summary: Optional[str] = None,
        extra: Optional[Dict[str, Any]] = None,
    ) -> None:
        targets = _alert_targets_for_event(self.hass, event_type, user_id=user_id)
        if not targets:
            return

        data: Dict[str, Any] = {
            "event_type": event_type,
            "device_name": self.device_name,
            "entry_id": self.entry_id,
        }
        if user_id:
            data["user_id"] = user_id
        if summary:
            data["summary"] = summary
        if extra:
            for key, value in extra.items():
                if key not in data:
                    data[key] = value

        if event_type == "device_offline":
            message = f"{self.device_name} has been offline for 5 minutes."
        elif event_type == "integrity_failed":
            message = f"Integrity check failed for {self.device_name}."
        elif event_type == "any_denied":
            who = user_id or "Unknown user"
            message = f"Access denied for {who} on {self.device_name}."
        elif event_type == "user_granted":
            event = None
            if extra and isinstance(extra.get("event"), dict):
                event = extra["event"]
            who = self._extract_event_user_name(event) if event else None
            if not who:
                who = user_id or "Unknown user"
            message = self._access_granted_notification_message(event, who)
            access_method = self._access_method_label(event)
            if access_method:
                data["access_method"] = access_method
        else:
            message = summary or f"{event_type} on {self.device_name}"

        title = f"Akuvox • {self.device_name}"
        service = getattr(self.hass, "services", None)
        if not service or not hasattr(service, "async_call"):
            for target in targets:
                self._record_notification_diagnostic(
                    source="system_alert",
                    channel="alert_notification",
                    event_type=event_type,
                    status="skipped",
                    target=target,
                    title=title,
                    message=message,
                    user_id=user_id,
                    user_name=who if event_type == "user_granted" else None,
                    event_summary=summary,
                    reason="Home Assistant notify service was unavailable.",
                )
            await self._async_save_notification_diagnostics()
            return

        notification_diag_dirty = False
        for target in targets:
            try:
                await service.async_call(
                    "notify",
                    target,
                    {"title": title, "message": message, "data": data},
                    blocking=False,
                )
                notification_diag_dirty = self._record_notification_diagnostic(
                    source="system_alert",
                    channel="alert_notification",
                    event_type=event_type,
                    status="sent",
                    target=target,
                    title=title,
                    message=message,
                    user_id=user_id,
                    user_name=who if event_type == "user_granted" else None,
                    event_summary=summary,
                ) or notification_diag_dirty
            except Exception as err:
                notification_diag_dirty = self._record_notification_diagnostic(
                    source="system_alert",
                    channel="alert_notification",
                    event_type=event_type,
                    status="failed",
                    target=target,
                    title=title,
                    message=message,
                    user_id=user_id,
                    user_name=who if event_type == "user_granted" else None,
                    event_summary=summary,
                    error=_safe_str(err),
                ) or notification_diag_dirty
                _LOGGER.debug("Failed to notify %s: %s", target, _safe_str(err))
        if notification_diag_dirty:
            await self._async_save_notification_diagnostics()
