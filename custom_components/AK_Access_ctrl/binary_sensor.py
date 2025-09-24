from __future__ import annotations

from typing import Any, Dict

from homeassistant.components.binary_sensor import BinarySensorEntity
from homeassistant.core import HomeAssistant
from homeassistant.config_entries import ConfigEntry
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .const import DOMAIN


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
):
    data = hass.data[DOMAIN][entry.entry_id]
    coord = data["coordinator"]

    entities: list[BinarySensorEntity] = [
        AkuvoxGrantedAccessBinarySensor(coord, entry),
        AkuvoxGrantedAccessKeyHolderBinarySensor(coord, entry),
        AkuvoxDeniedAccessBinarySensor(coord, entry),
    ]
    async_add_entities(entities)


class _Base(BinarySensorEntity):
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

    def _coord_updated(self) -> None:
        self.async_write_ha_state()

    @property
    def available(self) -> bool:
        return True

    @property
    def extra_state_attributes(self) -> Dict[str, Any]:
        state = getattr(self._coord, "event_state", {}) or {}
        return {
            "last_user": state.get("last_user_name"),
            "last_user_id": state.get("last_user_id"),
            "last_event_type": state.get("last_event_type"),
            "last_event_summary": state.get("last_event_summary"),
            "last_event_timestamp": state.get("last_event_timestamp"),
            "last_event_key_holder": state.get("last_event_key_holder"),
        }


class AkuvoxGrantedAccessBinarySensor(_Base):
    @property
    def name(self) -> str:
        return f"{self._coord.device_name} Granted Access"

    @property
    def unique_id(self) -> str:
        return f"{self._entry.entry_id}_granted_access"

    @property
    def is_on(self) -> bool:
        state = getattr(self._coord, "event_state", {}) or {}
        return bool(state.get("granted_active"))


class AkuvoxGrantedAccessKeyHolderBinarySensor(_Base):
    @property
    def name(self) -> str:
        return f"{self._coord.device_name} Granted Access Key Holder"

    @property
    def unique_id(self) -> str:
        return f"{self._entry.entry_id}_granted_access_key_holder"

    @property
    def is_on(self) -> bool:
        state = getattr(self._coord, "event_state", {}) or {}
        return bool(state.get("granted_key_holder_active"))


class AkuvoxDeniedAccessBinarySensor(_Base):
    @property
    def name(self) -> str:
        return f"{self._coord.device_name} Denied Access"

    @property
    def unique_id(self) -> str:
        return f"{self._entry.entry_id}_denied_access"

    @property
    def is_on(self) -> bool:
        state = getattr(self._coord, "event_state", {}) or {}
        return bool(state.get("denied_active"))
