from __future__ import annotations

from homeassistant.components.update import UpdateEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity import DeviceInfo, EntityCategory
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .const import DOMAIN, INTEGRATION_VERSION, INTEGRATION_VERSION_LABEL


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
):
    async_add_entities([AkuvoxIntegrationUpdate(entry)])


class AkuvoxIntegrationUpdate(UpdateEntity):
    _attr_entity_category = EntityCategory.DIAGNOSTIC
    _attr_has_entity_name = True
    _attr_should_poll = False

    def __init__(self, entry: ConfigEntry) -> None:
        self._entry_id = entry.entry_id
        self._attr_unique_id = f"{entry.entry_id}_integration_version"
        self._attr_name = "Akuvox Access Control"
        self._attr_device_info = DeviceInfo(
            identifiers={(DOMAIN, "integration")},
            name="Akuvox Access Control",
            manufacturer="Akuvox",
            model="Home Assistant Integration",
        )

    @property
    def installed_version(self) -> str:
        return INTEGRATION_VERSION_LABEL

    @property
    def latest_version(self) -> str:
        return INTEGRATION_VERSION_LABEL

    @property
    def extra_state_attributes(self) -> dict:
        return {
            "installed_version_raw": INTEGRATION_VERSION,
        }

    @property
    def available(self) -> bool:
        return True
