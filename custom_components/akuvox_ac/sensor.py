from __future__ import annotations

from typing import Any, Dict, List
from homeassistant.components.sensor import SensorEntity, SensorDeviceClass
from homeassistant.core import HomeAssistant
from homeassistant.config_entries import ConfigEntry
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .const import DOMAIN

async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry, async_add_entities: AddEntitiesCallback):
    data = hass.data[DOMAIN][entry.entry_id]
    coord = data["coordinator"]

    entities: List[SensorEntity] = [
        AkuvoxOnlineSensor(coord, entry),
        AkuvoxLastSyncSensor(coord, entry),
        AkuvoxLastAccessUserSensor(coord, entry),
        AkuvoxLastAccessedSensor(coord, entry),
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

    def _base_state_attributes(self) -> Dict[str, Any]:
        return {
            "akuvox_entry_id": self._entry.entry_id,
            "akuvox_device_name": self._coord.device_name,
            "akuvox_device_type": (self._coord.health.get("device_type") or ""),
        }

    @property
    def available(self) -> bool:
        return True

    async def async_added_to_hass(self):
        pass

    def _coord_updated(self) -> None:
        self.async_write_ha_state()

    @property
    def extra_state_attributes(self) -> Dict[str, Any]:
        return dict(self._base_state_attributes())

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

    @property
    def extra_state_attributes(self) -> Dict[str, Any]:
        attrs = super().extra_state_attributes
        attrs["akuvox_metric"] = "online"
        return attrs

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

    @property
    def extra_state_attributes(self) -> Dict[str, Any]:
        attrs = super().extra_state_attributes
        attrs["akuvox_metric"] = "last_sync"
        return attrs

class AkuvoxLastAccessUserSensor(_Base, SensorEntity):
    @property
    def name(self) -> str:
        return f"{self._coord.device_name} Last Access User"

    @property
    def unique_id(self) -> str:
        return f"{self._entry.entry_id}_last_access_user"

    @property
    def native_value(self):
        snapshot = self._coord.get_last_access_snapshot()
        value = snapshot.get("user_name") or snapshot.get("user_id")
        if value:
            return value
        state = getattr(self._coord, "event_state", {}) or {}
        return state.get("last_user_name") or state.get("last_user_id")

    @property
    def extra_state_attributes(self):
        snapshot = self._coord.get_last_access_snapshot()
        state = getattr(self._coord, "event_state", {}) or {}
        attrs = super().extra_state_attributes
        attrs.update(
            {
                "akuvox_metric": "last_access_user",
                "user_id": snapshot.get("user_id") or state.get("last_user_id"),
                "user_name": snapshot.get("user_name") or state.get("last_user_name"),
                "last_accessed": snapshot.get("timestamp") or state.get("last_event_timestamp"),
                "event_type": state.get("last_event_type"),
                "event_summary": state.get("last_event_summary"),
                "event_timestamp": state.get("last_event_timestamp"),
                "key_holder": state.get("last_event_key_holder"),
            }
        )
        return attrs


class AkuvoxLastAccessedSensor(_Base, SensorEntity):
    _attr_device_class = SensorDeviceClass.TIMESTAMP

    @property
    def name(self) -> str:
        return f"{self._coord.device_name} Last Accessed"

    @property
    def unique_id(self) -> str:
        return f"{self._entry.entry_id}_last_accessed"

    @property
    def native_value(self):
        snapshot = self._coord.get_last_access_snapshot()
        return snapshot.get("timestamp_dt")

    @property
    def extra_state_attributes(self):
        snapshot = self._coord.get_last_access_snapshot()
        state = getattr(self._coord, "event_state", {}) or {}
        attrs = super().extra_state_attributes
        attrs.update(
            {
                "akuvox_metric": "last_accessed",
                "user_id": snapshot.get("user_id") or state.get("last_user_id"),
                "user_name": snapshot.get("user_name") or state.get("last_user_name"),
                "event_type": state.get("last_event_type"),
                "event_summary": state.get("last_event_summary"),
                "event_timestamp": snapshot.get("timestamp") or state.get("last_event_timestamp"),
                "key_holder": state.get("last_event_key_holder"),
            }
        )
        return attrs
