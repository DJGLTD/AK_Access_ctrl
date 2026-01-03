from __future__ import annotations

from typing import Any, Dict, Optional, List
import voluptuous as vol

from homeassistant import config_entries
from homeassistant.core import HomeAssistant
from homeassistant.data_entry_flow import FlowResult

from .const import (
    DOMAIN,
    ENTRY_VERSION, 
    CONF_DEVICE_NAME, CONF_DEVICE_TYPE,
    CONF_HOST, CONF_PORT, CONF_USERNAME, CONF_PASSWORD,
    CONF_PARTICIPATE, CONF_POLL_INTERVAL, CONF_DEVICE_GROUPS,
    DEFAULT_POLL_INTERVAL,
)

DEVICE_TYPES = ["Intercom", "Keypad"]

class AkuvoxConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    """Handle the initial config flow."""

    VERSION = ENTRY_VERSION

    async def async_step_user(self, user_input: Optional[Dict[str, Any]] = None) -> FlowResult:
        errors: Dict[str, str] = {}

        if user_input is not None:
            # Always use port 80 for the integration.
            data = {**user_input, CONF_PORT: 80}
            # unique_id as host:port to avoid dup
            uniq = f"{data[CONF_HOST]}:{data[CONF_PORT]}"
            await self.async_set_unique_id(uniq)
            self._abort_if_unique_id_configured()
            return self.async_create_entry(title=data[CONF_DEVICE_NAME], data=data)

        schema = vol.Schema({
            vol.Required(CONF_DEVICE_NAME): str,
            vol.Required(CONF_HOST): str,
            vol.Required(CONF_DEVICE_TYPE, default="Intercom"): vol.In(DEVICE_TYPES),
            vol.Optional(CONF_USERNAME, default=""): str,
            vol.Optional(CONF_PASSWORD, default=""): str,
        })
        return self.async_show_form(step_id="user", data_schema=schema, errors=errors)

    async def async_step_import(self, import_input: Dict[str, Any]) -> FlowResult:
        # support YAML import if ever needed
        return await self.async_step_user(import_input)

    async def async_step_reconfigure(self, user_input=None) -> FlowResult:
        return await self.async_step_user(user_input)

    async def async_get_options_flow(self, config_entry):
        return AkuvoxOptionsFlow(config_entry)


class AkuvoxOptionsFlow(config_entries.OptionsFlow):
    def __init__(self, entry):
        self.entry = entry

    def _groups(self, hass: HomeAssistant) -> List[str]:
        try:
            gs = hass.data[DOMAIN]["groups_store"]
            return gs.groups()
        except Exception:
            return ["Default"]

    async def async_step_init(self, user_input: Optional[Dict[str, Any]] = None) -> FlowResult:
        if user_input is not None:
            # merge/return
            return self.async_create_entry(title="", data=user_input)

        # defaults from entry.options or entry.data
        base = {**self.entry.data, **self.entry.options}
        schema = vol.Schema({
            vol.Optional(CONF_PARTICIPATE, default=base.get(CONF_PARTICIPATE, True)): bool,
            vol.Optional(CONF_POLL_INTERVAL, default=base.get(CONF_POLL_INTERVAL, DEFAULT_POLL_INTERVAL)): int,
            vol.Optional(CONF_DEVICE_GROUPS, default=base.get(CONF_DEVICE_GROUPS, ["Default"])): vol.All(
                lambda v: v, vol.In(self._groups(self.hass)),  # multi-select emulation done by frontend (list)
            ),
        })
        return self.async_show_form(step_id="init", data_schema=schema)
