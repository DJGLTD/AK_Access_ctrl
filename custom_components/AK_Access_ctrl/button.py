from __future__ import annotations

from homeassistant.components.button import ButtonEntity
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

    entities: list[ButtonEntity] = [
        AkuvoxAccessPermittedButton(coord, entry),
        AkuvoxAccessDeniedButton(coord, entry),
        AkuvoxCallEndButton(coord, entry),
    ]
    async_add_entities(entities)


class _Base(ButtonEntity):
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


class AkuvoxAccessPermittedButton(_Base):
    @property
    def name(self) -> str:
        return f"{self._coord.device_name} Access Permitted"

    @property
    def unique_id(self) -> str:
        return f"{self._entry.entry_id}_access_permitted"

    async def async_press(self) -> None:
        await self._coord.async_refresh_access_history()


class AkuvoxAccessDeniedButton(_Base):
    @property
    def name(self) -> str:
        return f"{self._coord.device_name} Access Denied"

    @property
    def unique_id(self) -> str:
        return f"{self._entry.entry_id}_access_denied"

    async def async_press(self) -> None:
        await self._coord.async_refresh_access_history()


class AkuvoxCallEndButton(_Base):
    @property
    def name(self) -> str:
        return f"{self._coord.device_name} Call End"

    @property
    def unique_id(self) -> str:
        return f"{self._entry.entry_id}_call_end"

    async def async_press(self) -> None:
        await self._coord.async_refresh_inbound_call_history()
