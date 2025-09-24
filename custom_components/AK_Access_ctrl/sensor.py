from __future__ import annotations

from typing import Any, Dict, List, Optional
from homeassistant.components.sensor import SensorEntity
from homeassistant.core import HomeAssistant
from homeassistant.config_entries import ConfigEntry
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .const import DOMAIN

async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry, async_add_entities: AddEntitiesCallback):
    data = hass.data[DOMAIN][entry.entry_id]
    coord = data["coordinator"]

    entities: List[SensorEntity] = [
        AkuvoxOnlineSensor(coord, entry),
        AkuvoxSyncStatusSensor(coord, entry),
        AkuvoxLastSyncSensor(coord, entry),
        AkuvoxUsersCountSensor(coord, entry),
        AkuvoxEventsCountSensor(coord, entry),
        AkuvoxLastAccessUserSensor(coord, entry),
    ]
    async_add_entities(entities, update_before_add=True)

class _Base(AkuvoxOnlineSensor := object):
    _attr_should_poll = False

    def __init__(self, coord, entry: ConfigEntry):
        self._coord = coord
        self._entry = entry
        self._attr_device_info = {
            "identifiers": {(DOMAIN, entry.entry_id)},
            "name": coord.device_name,
            "manufacturer": "Akuvox",
            "model": coord.health.get("device_type") or "Device",
        }
        coord.async_add_listener(self._coord_updated)

    @property
    def available(self) -> bool:
        return True

    async def async_added_to_hass(self):
        pass

    def _coord_updated(self) -> None:
        self.async_write_ha_state()

class AkuvoxOnlineSensor(_Base, SensorEntity):
    @property
    def name(self) -> str:
        return f"{self._coord.device_name} Online"

    @property
    def unique_id(self) -> str:
        return f"{self._entry.entry_id}_online"

    @property
    def native_value(self):
        return "Online" if self._coord.health.get("online") else "Offline"

class AkuvoxSyncStatusSensor(_Base, SensorEntity):
    @property
    def name(self) -> str:
        return f"{self._coord.device_name} Sync"

    @property
    def unique_id(self) -> str:
        return f"{self._entry.entry_id}_sync"

    @property
    def native_value(self):
        if not self._coord.health.get("online"):
            return "Offline"
        return self._coord.health.get("sync_status") or "pending"

class AkuvoxLastSyncSensor(_Base, SensorEntity):
    @property
    def name(self) -> str:
        return f"{self._coord.device_name} Last Sync"

    @property
    def unique_id(self) -> str:
        return f"{self._entry.entry_id}_last_sync"

    @property
    def native_value(self):
        if not self._coord.health.get("online"):
            return None
        return self._coord.health.get("last_sync")

class AkuvoxUsersCountSensor(_Base, SensorEntity):
    @property
    def name(self) -> str:
        return f"{self._coord.device_name} Users"

    @property
    def unique_id(self) -> str:
        return f"{self._entry.entry_id}_users_count"

    @property
    def native_value(self):
        return len(self._coord.users or [])

class AkuvoxEventsCountSensor(_Base, SensorEntity):
    @property
    def name(self) -> str:
        return f"{self._coord.device_name} Events"

    @property
    def unique_id(self) -> str:
        return f"{self._entry.entry_id}_events_count"

    @property
    def native_value(self):
        return len(self._coord.events or [])


class AkuvoxLastAccessUserSensor(_Base, SensorEntity):
    @property
    def name(self) -> str:
        return f"{self._coord.device_name} Last Access User"

    @property
    def unique_id(self) -> str:
        return f"{self._entry.entry_id}_last_access_user"

    @property
    def native_value(self):
        state = getattr(self._coord, "event_state", {}) or {}
        return state.get("last_user_name")

    @property
    def extra_state_attributes(self):
        state = getattr(self._coord, "event_state", {}) or {}
        return {
            "user_id": state.get("last_user_id"),
            "event_type": state.get("last_event_type"),
            "event_summary": state.get("last_event_summary"),
            "event_timestamp": state.get("last_event_timestamp"),
            "key_holder": state.get("last_event_key_holder"),
        }
