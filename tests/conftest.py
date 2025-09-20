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
sys.modules.setdefault("homeassistant.const", const)
sys.modules.setdefault("homeassistant.config_entries", config_entries)
sys.modules.setdefault("homeassistant.util", util)
sys.modules.setdefault("homeassistant.util.dt", util.dt)
sys.modules.setdefault("custom_components", custom_components)
sys.modules.setdefault("custom_components.AK_Access_ctrl", akuvox_pkg)
sys.modules.setdefault("aiohttp", aiohttp)


@pytest.fixture
def hass():
    return HomeAssistant()
