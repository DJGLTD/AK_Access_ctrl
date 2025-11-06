import sys
from pathlib import Path
import types

import pytest
from unittest.mock import MagicMock

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# Minimal Home Assistant stubs for unit testing
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

aiohttp_module = types.ModuleType("homeassistant.helpers.aiohttp_client")


async def _async_get_clientsession(hass):
    return None


aiohttp_module.async_get_clientsession = _async_get_clientsession

components_module = types.ModuleType("homeassistant.components")
components_module.__path__ = []
frontend_module = types.ModuleType("homeassistant.components.frontend")


async def _frontend_register(*args, **kwargs):
    return True


async def _frontend_remove(*args, **kwargs):
    return True


frontend_module.async_register_built_in_panel = _frontend_register
frontend_module.async_remove_panel = _frontend_remove

sys.modules.setdefault("homeassistant", ha_module)
sys.modules.setdefault("homeassistant.const", const_module)
sys.modules.setdefault("homeassistant.core", core_module)
sys.modules.setdefault("homeassistant.config_entries", config_entries_module)
sys.modules.setdefault("homeassistant.helpers", helpers_module)
sys.modules.setdefault("homeassistant.helpers.storage", storage_module)
sys.modules.setdefault("homeassistant.helpers.event", event_module)
sys.modules.setdefault("homeassistant.helpers.aiohttp_client", aiohttp_module)
sys.modules.setdefault("homeassistant.components", components_module)
sys.modules.setdefault("homeassistant.components.frontend", frontend_module)

helpers_module.storage = storage_module
helpers_module.event = event_module
helpers_module.aiohttp_client = aiohttp_module
components_module.frontend = frontend_module

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

sys.modules.setdefault("aiohttp", aiohttp_stub)

update_coordinator_module = types.ModuleType("homeassistant.helpers.update_coordinator")


class _DataUpdateCoordinator:
    def __init__(self, *args, **kwargs):
        self.hass = kwargs.get("hass") if "hass" in kwargs else (args[0] if args else None)

    async def async_config_entry_first_refresh(self):
        return None

    async def async_request_refresh(self):
        return None


update_coordinator_module.DataUpdateCoordinator = _DataUpdateCoordinator

sys.modules.setdefault("homeassistant.helpers.update_coordinator", update_coordinator_module)
helpers_module.update_coordinator = update_coordinator_module

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

aiohttp_stub.web = web_module
sys.modules.setdefault("aiohttp.web", web_module)

sys.modules.setdefault("homeassistant.components.http.view", http_view_module)
sys.modules.setdefault("homeassistant.components.http.auth", http_auth_module)
sys.modules.setdefault("homeassistant.components.http.const", http_const_module)
sys.modules.setdefault("homeassistant.components.persistent_notification", persistent_notification_module)
sys.modules.setdefault("homeassistant.helpers.network", helpers_network_module)

import custom_components.AK_Access_ctrl.__init__ as akuvox


@pytest.fixture(autouse=True)
def stub_relay_helpers(monkeypatch):
    monkeypatch.setattr(akuvox, "normalize_relay_roles", lambda roles, device_type: {"relay_a": "door"})
    monkeypatch.setattr(akuvox, "door_relays", lambda roles: "1")
    monkeypatch.setattr(akuvox, "relay_suffix_for_user", lambda roles, key_holder, device_type: "1")
    monkeypatch.setattr(akuvox, "_face_asset_exists", lambda hass, user_id: False)


def _make_schedule_store(initial):
    store = akuvox.AkuvoxSchedulesStore.__new__(akuvox.AkuvoxSchedulesStore)
    store.data = {"schedules": initial}
    return store


def test_ensure_exit_clone_created_and_orphan_removed():
    schedules = {
        "Office": {"start": "09:00", "end": "17:00", "days": ["mon", "tue", "wed"], "type": "2"},
        "Old - EP": {"system_exit_clone": True, "exit_clone_for": "Old", "days": ["mon"], "type": "2"},
    }
    store = _make_schedule_store(schedules)
    changed = store._ensure_exit_clones(schedules)

    assert changed is True
    assert "Office - EP" in schedules
    clone = schedules["Office - EP"]
    assert clone["system_exit_clone"] is True
    assert clone["exit_clone_for"] == "Office"
    assert clone["days"] == ["mon", "tue", "wed"]
    assert clone["date_start"] == ""
    assert clone["date_end"] == ""
    assert "Old - EP" not in schedules


def test_ensure_exit_clone_idempotent_when_normalised():
    schedules = {
        "Office": {
            "start": "09:00",
            "end": "17:00",
            "days": ["mon", "tue"],
            "type": "2",
            "exit_clone_name": "Office - EP",
        },
        "Office - EP": {
            "start": "00:00",
            "end": "23:59",
            "days": ["mon", "tue"],
            "type": "2",
            "system_exit_clone": True,
            "exit_clone_for": "Office",
            "always_permit_exit": True,
            "date_start": "",
            "date_end": "",
        },
    }
    store = _make_schedule_store(schedules)
    changed = store._ensure_exit_clones(schedules)

    assert changed is False
    assert schedules["Office"]["exit_clone_name"] == "Office - EP"
    assert schedules["Office - EP"]["exit_clone_for"] == "Office"


def _payload_for_exit_permission(exit_permission, sched_map, exit_schedule_map):
    profile = {
        "name": "Nigel",
        "schedule_name": "Office",
        "schedule_id": "1500",
        "exit_permission": exit_permission,
        "face_url": "http://example.invalid/face.jpg",
        "face_active": False,
    }
    local = {}
    hass = MagicMock()
    opts = {"exit_device": True, "relay_roles": {}}
    return akuvox._desired_device_user_payload(
        hass,
        "HA123",
        profile,
        local,
        opts=opts,
        sched_map=sched_map,
        exit_schedule_map=exit_schedule_map,
        face_root_base="https://faces",
        device_type_raw="intercom",
    )


def test_exit_permission_defaults_to_match_when_clone_missing():
    sched_map = {"office": "1500"}
    exit_schedule_map = {"office": {"clone_name": "Office - EP"}}
    payload = _payload_for_exit_permission(None, sched_map, exit_schedule_map)
    assert payload["ScheduleID"] == "1500"
    assert payload["Schedule"] == "1500"


def test_exit_permission_always_forces_247_clone():
    sched_map = {"office": "1500", "24/7 access": "1001"}
    exit_schedule_map = {"office": {"clone_name": "Office - EP"}}
    payload = _payload_for_exit_permission("always", sched_map, exit_schedule_map)
    assert payload["ScheduleID"] == "1001"
    assert payload["Schedule"] == "1001"


def test_exit_permission_working_days_uses_clone_when_available():
    sched_map = {"office": "1500", "office - ep": "2150"}
    exit_schedule_map = {"office": {"clone_name": "Office - EP"}}
    payload = _payload_for_exit_permission("working_days", sched_map, exit_schedule_map)
    assert payload["ScheduleID"] == "2150"
    assert payload["Schedule"] == "2150"


def test_exit_permission_working_days_falls_back_when_clone_missing():
    sched_map = {"office": "1500"}
    exit_schedule_map = {"office": {"clone_name": "Office - EP"}}
    payload = _payload_for_exit_permission("working_days", sched_map, exit_schedule_map)
    assert payload["ScheduleID"] == "1500"
    assert payload["Schedule"] == "1500"
