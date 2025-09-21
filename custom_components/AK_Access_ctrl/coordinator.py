from __future__ import annotations

import logging
from datetime import timedelta
import time
from typing import Any, Dict, List, Optional, Tuple

from homeassistant.core import HomeAssistant
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator

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


def _derive_targets_from_raw(raw: Any, event_type: str, *, user_id: Optional[str] = None) -> List[str]:
    out: List[str] = []
    if not isinstance(raw, dict):
        return out

    norm_user = str(user_id).strip() if user_id not in (None, "") else None
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
            granted_cfg = config.get("granted") if isinstance(config.get("granted"), dict) else {}
            any_flag = bool(granted_cfg.get("any")) if isinstance(granted_cfg, dict) else bool(config.get("granted_any"))
            users_raw = granted_cfg.get("users") if isinstance(granted_cfg, dict) else config.get("granted_users")
            if any_flag:
                out.append(target)
            elif norm_user and users_raw:
                if isinstance(users_raw, (list, tuple, set)):
                    normalized = {str(u).strip() for u in users_raw if str(u).strip()}
                elif isinstance(users_raw, str):
                    normalized = {users_raw.strip()}
                else:
                    normalized = set()
                if norm_user in normalized:
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
            "sync_status": "pending",
            "last_sync": None,
            "last_error": None,
            "last_ping": None,
        }
        self.users: List[Dict[str, Any]] = []
        self.events: List[Dict[str, Any]] = []  # newest first
        self._was_online: Optional[bool] = None

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
                    await self._kick_sync_now()
                elif prev is None and not self.health.get("last_sync"):
                    # First time we see it online after startup and never synced
                    await self._kick_sync_now()
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
                users = await self.api.user_list()
                if isinstance(users, list):
                    self.users = users
            except Exception:
                # don't fail the whole refresh just because the user list failed
                pass

            await self._process_door_events()

        except Exception as e:
            last_error = _safe_str(e)
            self.health["online"] = False
            if reboot_active:
                self.health["status"] = "rebooting"
            else:
                self.health["status"] = "offline"
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

    async def _process_door_events(self):
        """Fetch recent door events and handle non-key access notifications."""

        notifications = self.storage.data.get("notifications") or {}
        notify_targets: List[str] = list(notifications.get("targets") or [])

        try:
            events = await self.api.events_last()
        except Exception as err:
            _LOGGER.debug("Failed to fetch door events: %s", _safe_str(err))
            return

        if not isinstance(events, list) or not events:
            return

        state = self.storage.data.setdefault("door_events", {})
        last_seen = _safe_str(state.get("last_event_key")) or None

        events_to_process: List[Tuple[str, Dict[str, Any]]] = []
        for event in reversed(events):
            key = self._event_unique_key(event)
            if key is None:
                continue
            if last_seen and key == last_seen:
                # Drop everything collected so far (they are older events).
                events_to_process = []
                continue
            events_to_process.append((key, event))

        if not events_to_process:
            return

        # Avoid processing an unbounded backlog.
        max_events = 25
        if len(events_to_process) > max_events:
            events_to_process = events_to_process[-max_events:]

        storage_dirty = False
        last_processed_key = last_seen
        for key, event in events_to_process:
            if await self._handle_door_event(event, notify_targets):
                storage_dirty = True
            last_processed_key = key

        if last_processed_key and last_processed_key != last_seen:
            state["last_event_key"] = last_processed_key
            storage_dirty = True

        if storage_dirty:
            try:
                await self.storage.async_save()
            except Exception as err:
                _LOGGER.debug("Unable to persist door event state: %s", _safe_str(err))

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

    def _extract_event_timestamp(self, event: Dict[str, Any], *, fallback: bool = True) -> Optional[str]:
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
            val = event.get(key)
            if val not in (None, ""):
                return _safe_str(val)
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
            "ID",
            "CardNo",
            "CardNumber",
        ):
            val = event.get(key)
            if val not in (None, ""):
                return _safe_str(val)
        return None

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
        has_grant = any(word in summary for word in ("grant", "granted", "open", "unlock", "success", "allowed"))
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
            "Result",
            "Reason",
            "OpenMethod",
            "AccessMethod",
            "Mode",
            "Way",
        ):
            val = event.get(key)
            if isinstance(val, str) and val:
                tokens.append(val.lower())
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
            "allowed",
            "success",
            "opened",
            "open",
            "unlock",
            "passed",
            "access ok",
        )
        return any(word in summary for word in granted_words)

    async def _handle_door_event(self, event: Dict[str, Any], notify_targets: List[str]) -> bool:
        """Handle a single door event, firing HA events as needed."""

        storage_changed = False
        last_access = self.storage.data.setdefault("last_access", {})

        user_id = self._extract_event_user_id(event)
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
            if notify_targets:
                await self._dispatch_notification(event, notify_targets)

        tokens = self._event_summary_tokens(event)
        summary_text = " ".join(tokens)
        if tokens and self._event_is_access_denied(tokens):
            try:
                await self._send_alert_notification(
                    "any_denied",
                    user_id=user_id,
                    summary=summary_text,
                    extra={"event": event},
                )
            except Exception as err:
                _LOGGER.debug("Failed to dispatch denied alert: %s", _safe_str(err))
        elif tokens and self._event_is_access_granted(tokens):
            try:
                await self._send_alert_notification(
                    "user_granted",
                    user_id=user_id,
                    summary=summary_text,
                    extra={"event": event},
                )
            except Exception as err:
                _LOGGER.debug("Failed to dispatch granted alert: %s", _safe_str(err))

        return storage_changed

    async def _dispatch_notification(self, event: Dict[str, Any], notify_targets: List[str]) -> None:
        """Send notifications for a door event (best effort)."""

        if not notify_targets:
            return

        service = getattr(self.hass, "services", None)
        if service is None or not hasattr(service, "async_call"):
            return

        message = _safe_str(event.get("Event") or event.get("EventType") or "Akuvox access granted")
        data = {
            "message": message,
            "title": self.device_name,
            "data": {"event": event, "device_name": self.device_name},
        }

        for target in notify_targets:
            try:
                await service.async_call("notify", target, data, blocking=False)
            except Exception as err:
                _LOGGER.debug("Failed to dispatch notification to %s: %s", target, _safe_str(err))

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
            who = user_id or "Unknown user"
            message = f"{who} granted access on {self.device_name}."
        else:
            message = summary or f"{event_type} on {self.device_name}"

        title = f"Akuvox • {self.device_name}"
        service = getattr(self.hass, "services", None)
        if not service or not hasattr(service, "async_call"):
            return

        for target in targets:
            try:
                await service.async_call(
                    "notify",
                    target,
                    {"title": title, "message": message, "data": data},
                    blocking=False,
                )
            except Exception as err:
                _LOGGER.debug("Failed to notify %s: %s", target, _safe_str(err))
