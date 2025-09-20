import asyncio
import sys
import types
from datetime import datetime
from pathlib import Path

import pytest


class _FakeBus:
    def __init__(self) -> None:
        self.events = []

    def async_fire(self, event_type: str, event_data=None) -> None:
        self.events.append((event_type, event_data))


class _FakeServices:
    def __init__(self) -> None:
        self.calls = []

    async def async_call(self, domain: str, service: str, data: dict, blocking: bool = False):
        self.calls.append((domain, service, data, blocking))


class HomeAssistant:  # pragma: no cover - simple stub for tests
    def __init__(self) -> None:
        self.data = {}
        self.bus = _FakeBus()
        self.loop = asyncio.get_event_loop()
        self.services = _FakeServices()

    def async_create_task(self, coro):
        return self.loop.create_task(coro)


class DataUpdateCoordinator:  # pragma: no cover - minimal behaviour for tests
    def __init__(self, hass, logger, name=None, update_interval=None):
        self.hass = hass
        self.logger = logger
        self.name = name
        self.update_interval = update_interval

    async def async_config_entry_first_refresh(self):
        return


homeassistant = types.ModuleType("homeassistant")
homeassistant.core = types.ModuleType("homeassistant.core")
homeassistant.core.HomeAssistant = HomeAssistant


def _callback(func):
    return func


homeassistant.core.callback = _callback

helpers = types.ModuleType("homeassistant.helpers")
helpers.update_coordinator = types.ModuleType("homeassistant.helpers.update_coordinator")
helpers.update_coordinator.DataUpdateCoordinator = DataUpdateCoordinator
helpers.storage = types.ModuleType("homeassistant.helpers.storage")


class _Store:  # pragma: no cover - minimal stub
    def __init__(self, hass, version, key):
        self.hass = hass
        self.version = version
        self.key = key

    async def async_load(self):
        return None

    async def async_save(self, data):
        return None


helpers.storage.Store = _Store
helpers.event = types.ModuleType("homeassistant.helpers.event")


async def _event_stub(*args, **kwargs):  # pragma: no cover - simple coroutine stub
    return None


helpers.event.async_call_later = _event_stub
helpers.event.async_track_time_change = _event_stub
helpers.event.async_track_time_interval = _event_stub
helpers.aiohttp_client = types.ModuleType("homeassistant.helpers.aiohttp_client")


async def _async_get_clientsession(hass):  # pragma: no cover - simple stub
    return ClientSession()


helpers.aiohttp_client.async_get_clientsession = _async_get_clientsession
helpers.network = types.ModuleType("homeassistant.helpers.network")


def _get_url(*args, **kwargs):  # pragma: no cover - simple stub
    return "http://example.com"


helpers.network.get_url = _get_url

util = types.ModuleType("homeassistant.util")
util.dt = types.ModuleType("homeassistant.util.dt")
util.dt.utcnow = datetime.utcnow

const = types.ModuleType("homeassistant.const")
const.Platform = type(
    "Platform",
    (),
    {
        "SENSOR": "sensor",
        "BUTTON": "button",
        "BINARY_SENSOR": "binary_sensor",
        "UPDATE": "update",
    },
)

config_entries = types.ModuleType("homeassistant.config_entries")


class ConfigEntry:  # pragma: no cover - minimal stub
    def __init__(self, **kwargs):
        self.data = kwargs.get("data", {})
        self.options = kwargs.get("options", {})
        self.entry_id = kwargs.get("entry_id", "test")
        self.title = kwargs.get("title", "Test")


config_entries.ConfigEntry = ConfigEntry

aiohttp = types.ModuleType("aiohttp")


class ClientSession:  # pragma: no cover - simple stub
    pass


class BasicAuth:  # pragma: no cover - simple stub
    def __init__(self, *args, **kwargs):
        return


aiohttp.ClientSession = ClientSession
aiohttp.BasicAuth = BasicAuth
aiohttp.web = types.ModuleType("aiohttp.web")


class _WebRequest:  # pragma: no cover - minimal stub
    def __init__(self):
        self.app = {}
        self.headers = {}
        self.query = {}


class _WebResponse:  # pragma: no cover - minimal stub
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs


def _json_response(data=None, status=200):  # pragma: no cover - simple stub
    return {"data": data, "status": status}


class _HTTPException(Exception):  # pragma: no cover - simple stub
    def __init__(self, *args, **kwargs):
        super().__init__(*args)
        self.status = kwargs.get("status")


class _HTTPFound(_HTTPException):
    pass


class _HTTPNotFound(_HTTPException):
    pass


class _HTTPForbidden(_HTTPException):
    pass


aiohttp.web.Request = _WebRequest
aiohttp.web.FileResponse = _WebResponse
aiohttp.web.Response = _WebResponse
aiohttp.web.json_response = _json_response
aiohttp.web.HTTPFound = _HTTPFound
aiohttp.web.HTTPNotFound = _HTTPNotFound
aiohttp.web.HTTPForbidden = _HTTPForbidden

components = types.ModuleType("homeassistant.components")


class _FrontendModule(types.ModuleType):
    def __init__(self) -> None:
        super().__init__("homeassistant.components.frontend")
        self.registered: list[dict] = []
        self.removed: list[str] = []

    def async_register_built_in_panel(
        self,
        hass,
        component_name,
        sidebar_title=None,
        sidebar_icon=None,
        frontend_url_path=None,
        config=None,
        require_admin=False,
        *,
        update=False,
        config_panel_domain=None,
    ):
        self.registered.append(
            {
                "component_name": component_name,
                "sidebar_title": sidebar_title,
                "sidebar_icon": sidebar_icon,
                "frontend_url_path": frontend_url_path,
                "config": config,
                "require_admin": require_admin,
                "update": update,
            }
        )
        hass.data.setdefault("frontend_panels", {})[
            frontend_url_path or component_name
        ] = {
            "component_name": component_name,
            "config": config,
            "require_admin": require_admin,
        }

    def async_remove_panel(self, hass, frontend_url_path):
        self.removed.append(frontend_url_path)
        hass.data.setdefault("frontend_panels", {}).pop(frontend_url_path, None)


frontend = _FrontendModule()
components.frontend = frontend
components.http = types.ModuleType("homeassistant.components.http")
components.http.view = types.ModuleType("homeassistant.components.http.view")
components.http.view.HomeAssistantView = type(
    "HomeAssistantView",
    (),
    {"__module__": "homeassistant.components.http.view"},
)
components.persistent_notification = types.ModuleType(
    "homeassistant.components.persistent_notification"
)


async def _notify_stub(*args, **kwargs):  # pragma: no cover - simple stub
    return None


components.persistent_notification.async_create = _notify_stub

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

custom_components = types.ModuleType("custom_components")
custom_components.__path__ = [str(ROOT / "custom_components")]

akuvox_pkg = types.ModuleType("custom_components.AK_Access_ctrl")
akuvox_pkg.__path__ = [str(ROOT / "custom_components" / "AK_Access_ctrl")]

sys.modules.setdefault("homeassistant", homeassistant)
sys.modules.setdefault("homeassistant.core", homeassistant.core)
sys.modules.setdefault("homeassistant.helpers", helpers)
sys.modules.setdefault("homeassistant.helpers.update_coordinator", helpers.update_coordinator)
sys.modules.setdefault("homeassistant.helpers.storage", helpers.storage)
sys.modules.setdefault("homeassistant.helpers.event", helpers.event)
sys.modules.setdefault("homeassistant.helpers.aiohttp_client", helpers.aiohttp_client)
sys.modules.setdefault("homeassistant.helpers.network", helpers.network)
sys.modules.setdefault("homeassistant.const", const)
sys.modules.setdefault("homeassistant.config_entries", config_entries)
sys.modules.setdefault("homeassistant.util", util)
sys.modules.setdefault("homeassistant.util.dt", util.dt)
sys.modules.setdefault("homeassistant.components", components)
sys.modules.setdefault("homeassistant.components.http", components.http)
sys.modules.setdefault("homeassistant.components.http.view", components.http.view)
sys.modules.setdefault(
    "homeassistant.components.persistent_notification", components.persistent_notification
)
sys.modules.setdefault("homeassistant.components.frontend", frontend)
sys.modules.setdefault("custom_components", custom_components)
sys.modules.setdefault("custom_components.AK_Access_ctrl", akuvox_pkg)
sys.modules.setdefault("aiohttp", aiohttp)


@pytest.fixture
def hass():
    return HomeAssistant()
