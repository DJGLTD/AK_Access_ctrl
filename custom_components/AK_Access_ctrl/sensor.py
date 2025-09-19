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
    ]

    for key, meta in (data.get("webhooks_meta") or {}).items():
        entities.append(AkuvoxWebhookEventSensor(coord, entry, key, meta))

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


class AkuvoxWebhookEventSensor(_Base, SensorEntity):
    def __init__(self, coord, entry: ConfigEntry, key: str, meta: Dict[str, Any]):
        super().__init__(coord, entry)
        self._key = key
        self._meta = dict(meta or {})

    @property
    def name(self) -> str:
        label = self._meta.get("name") or self._key.replace("_", " ").title()
        return f"{self._coord.device_name} {label} Webhook"

    @property
    def unique_id(self) -> str:
        return f"{self._entry.entry_id}_webhook_{self._key}"

    def _state(self) -> Dict[str, Any]:
        try:
            return self._coord.webhook_states.get(self._key, {})
        except Exception:
            return {}

    @property
    def native_value(self):
        return self._state().get("last_triggered")

    @property
    def extra_state_attributes(self) -> Dict[str, Any]:
        attrs: Dict[str, Any] = {
            "event_key": self._key,
            "ha_event": self._meta.get("ha_event"),
            "webhook_id": self._meta.get("webhook_id"),
            "relative_url": self._meta.get("relative_url"),
            "count": self._state().get("count", 0),
        }
        payload = self._state().get("last_payload")
        if payload:
            attrs["last_payload"] = payload
        description = self._meta.get("description")
        if description:
            attrs["description"] = description
        return attrs
