from __future__ import annotations

import logging
from datetime import timedelta
from typing import Any, Dict, List, Optional

from homeassistant.core import HomeAssistant
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator

from .const import DOMAIN
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
        try:
            info = await self.api.ping_info()
            last_ping = info
            is_up = bool(info.get("ok"))
            prev = self._was_online
            self.health["online"] = is_up
            self._was_online = is_up

            # Online/offline transition events (+ on-online sync)
            if is_up:
                if prev is False:
                    self._append_event("Device came online")
                    await self._kick_sync_now()
                elif prev is None and not self.health.get("last_sync"):
                    # First time we see it online after startup and never synced
                    await self._kick_sync_now()
            else:
                if prev is True or prev is None:
                    self._append_event("Device went offline")
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

        except Exception as e:
            last_error = _safe_str(e)
        finally:
            self.health["last_error"] = last_error
            self.health["last_ping"] = last_ping