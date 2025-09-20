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
        now_ts = time.time()
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
        finally:
            self.health["last_error"] = last_error
            self.health["last_ping"] = last_ping

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
