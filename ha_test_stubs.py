"""Utilities for providing Home Assistant test stubs when running unit tests."""

from __future__ import annotations

import sys
import types


def ensure_homeassistant_stubs() -> None:
    """Install lightweight Home Assistant module stubs when unavailable."""

    if "homeassistant" in sys.modules:
        return

    ha_module = types.ModuleType("homeassistant")
    const_module = types.ModuleType("homeassistant.const")

    class _Platform:
        SENSOR = "sensor"
        BUTTON = "button"
        BINARY_SENSOR = "binary_sensor"
        UPDATE = "update"

    const_module.Platform = _Platform

    core_module = types.ModuleType("homeassistant.core")

    class _HomeAssistant:
        pass

    def _callback(func):
        return func

    core_module.HomeAssistant = _HomeAssistant
    core_module.callback = _callback

    config_entries_module = types.ModuleType("homeassistant.config_entries")

    class _ConfigEntry:
        pass

    config_entries_module.ConfigEntry = _ConfigEntry

    helpers_module = types.ModuleType("homeassistant.helpers")
    helpers_module.__path__ = []

    storage_module = types.ModuleType("homeassistant.helpers.storage")

    class _Store:
        def __init__(self, hass, version, key):
            self.hass = hass
            self.version = version
            self.key = key

        async def async_load(self):
            return {}

        async def async_save(self, data):
            return None

    storage_module.Store = _Store

    event_module = types.ModuleType("homeassistant.helpers.event")

    async def _event_stub(*args, **kwargs):
        return None

    event_module.async_call_later = _event_stub
    event_module.async_track_time_change = _event_stub
    event_module.async_track_time_interval = _event_stub

    aiohttp_client_module = types.ModuleType("homeassistant.helpers.aiohttp_client")

    async def _async_get_clientsession(hass):
        return None

    aiohttp_client_module.async_get_clientsession = _async_get_clientsession

    components_module = types.ModuleType("homeassistant.components")
    components_module.__path__ = []

    frontend_module = types.ModuleType("homeassistant.components.frontend")

    async def _frontend_register(*args, **kwargs):
        return True

    async def _frontend_remove(*args, **kwargs):
        return True

    frontend_module.async_register_built_in_panel = _frontend_register
    frontend_module.async_remove_panel = _frontend_remove

    update_coordinator_module = types.ModuleType("homeassistant.helpers.update_coordinator")

    class _DataUpdateCoordinator:
        def __init__(self, *args, **kwargs):
            self.hass = kwargs.get("hass") if "hass" in kwargs else (args[0] if args else None)

        async def async_config_entry_first_refresh(self):
            return None

        async def async_request_refresh(self):
            return None

    update_coordinator_module.DataUpdateCoordinator = _DataUpdateCoordinator

    http_view_module = types.ModuleType("homeassistant.components.http.view")

    class _HomeAssistantView:
        requires_auth = False

        async def get(self, request):
            raise NotImplementedError

    http_view_module.HomeAssistantView = _HomeAssistantView

    http_auth_module = types.ModuleType("homeassistant.components.http.auth")

    async def _async_sign_path(*args, **kwargs):
        return "signed"

    http_auth_module.async_sign_path = _async_sign_path

    http_const_module = types.ModuleType("homeassistant.components.http.const")
    http_const_module.KEY_HASS_REFRESH_TOKEN_ID = "refresh_token_id"

    persistent_notification_module = types.ModuleType("homeassistant.components.persistent_notification")

    async def _notify_create(*args, **kwargs):
        return None

    persistent_notification_module.async_create = _notify_create

    helpers_network_module = types.ModuleType("homeassistant.helpers.network")

    def _get_url(*args, **kwargs):
        return "http://example.invalid"

    helpers_network_module.get_url = _get_url

    aiohttp_stub = types.ModuleType("aiohttp")

    class _ClientSession:
        pass

    class _BasicAuth:
        pass

    class _FormData:
        pass

    aiohttp_stub.ClientSession = _ClientSession
    aiohttp_stub.BasicAuth = _BasicAuth
    aiohttp_stub.FormData = _FormData

    web_module = types.ModuleType("aiohttp.web")

    class _Request:
        pass

    class _HTTPException(Exception):
        pass

    class _HTTPNotFound(_HTTPException):
        pass

    class _HTTPForbidden(_HTTPException):
        pass

    class _FileField:
        pass

    class _Application:
        def __init__(self, *args, **kwargs):
            self.router = {}

    class _Response:
        def __init__(self, *args, **kwargs):
            self.args = args
            self.kwargs = kwargs

    def _json_response(*args, **kwargs):
        return {"args": args, "kwargs": kwargs}

    def _file_response(*args, **kwargs):
        return {"file": args, "kwargs": kwargs}

    web_module.Request = _Request
    web_module.HTTPNotFound = _HTTPNotFound
    web_module.HTTPForbidden = _HTTPForbidden
    web_module.FileField = _FileField
    web_module.Application = _Application
    web_module.Response = _Response
    web_module.json_response = _json_response
    web_module.FileResponse = _file_response

    # Register the stub modules so imports succeed.
    sys.modules.setdefault("homeassistant", ha_module)
    sys.modules.setdefault("homeassistant.const", const_module)
    sys.modules.setdefault("homeassistant.core", core_module)
    sys.modules.setdefault("homeassistant.config_entries", config_entries_module)
    sys.modules.setdefault("homeassistant.helpers", helpers_module)
    sys.modules.setdefault("homeassistant.helpers.storage", storage_module)
    sys.modules.setdefault("homeassistant.helpers.event", event_module)
    sys.modules.setdefault("homeassistant.helpers.aiohttp_client", aiohttp_client_module)
    sys.modules.setdefault("homeassistant.components", components_module)
    sys.modules.setdefault("homeassistant.components.frontend", frontend_module)
    sys.modules.setdefault("homeassistant.helpers.update_coordinator", update_coordinator_module)
    sys.modules.setdefault("homeassistant.components.http.view", http_view_module)
    sys.modules.setdefault("homeassistant.components.http.auth", http_auth_module)
    sys.modules.setdefault("homeassistant.components.http.const", http_const_module)
    sys.modules.setdefault("homeassistant.components.persistent_notification", persistent_notification_module)
    sys.modules.setdefault("homeassistant.helpers.network", helpers_network_module)
    sys.modules.setdefault("aiohttp", aiohttp_stub)
    sys.modules.setdefault("aiohttp.web", web_module)

    # Link submodules on their parents for attribute-based access.
    ha_module.const = const_module
    helpers_module.storage = storage_module
    helpers_module.event = event_module
    helpers_module.aiohttp_client = aiohttp_client_module
    helpers_module.update_coordinator = update_coordinator_module
    components_module.frontend = frontend_module
    aiohttp_stub.web = web_module

