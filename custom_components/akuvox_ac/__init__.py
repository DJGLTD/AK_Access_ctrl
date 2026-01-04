from __future__ import annotations

import asyncio
from importlib import import_module
from typing import Any

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant

_INTEGRATION_MODULE = f"{__name__}.integration"
_integration: Any | None = None
_integration_lock = asyncio.Lock()


async def _load_integration() -> Any:
    global _integration
    if _integration is not None:
        return _integration

    async with _integration_lock:
        if _integration is not None:
            return _integration
        loop = asyncio.get_running_loop()
        _integration = await loop.run_in_executor(None, import_module, _INTEGRATION_MODULE)
        return _integration


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    integration = await _load_integration()
    return await integration.async_setup_entry(hass, entry)


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    integration = await _load_integration()
    return await integration.async_unload_entry(hass, entry)


async def async_migrate_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    integration = await _load_integration()
    return await integration.async_migrate_entry(hass, entry)
