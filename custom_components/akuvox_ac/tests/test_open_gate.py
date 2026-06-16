import asyncio
from types import SimpleNamespace

from custom_components.akuvox_ac.ha_test_stubs import ensure_homeassistant_stubs

ensure_homeassistant_stubs()

from custom_components.akuvox_ac.access_history import AccessHistory
from custom_components.akuvox_ac.const import DOMAIN
from custom_components.akuvox_ac.http import async_open_gate


class _ApiStub:
    def __init__(self):
        self.calls = []

    async def trigger_relay(self, relay_number, *, delay=20, mode=0, level=0):
        self.calls.append(
            {
                "relay": relay_number,
                "delay": delay,
                "mode": mode,
                "level": level,
            }
        )
        return {"retcode": 0}


class _CoordinatorStub:
    def __init__(self):
        self.device_name = "Gate"
        self.health = {
            "device_type": "Intercom",
            "device_model": "X912",
            "ip": "10.30.0.73",
            "online": True,
        }
        self.events = []
        self.users = []
        self.manual_events = []

    async def async_handle_manual_event(self, event):
        self.manual_events.append(dict(event))


class _UsersStoreStub:
    def __init__(self, users):
        self._users = dict(users)

    def all(self):
        return dict(self._users)


class _AuthStub:
    async def async_get_user(self, user_id):
        return SimpleNamespace(id=user_id, name="DJGLTD")


def test_open_gate_uses_configured_door_relay_and_publishes_linked_event():
    api = _ApiStub()
    coordinator = _CoordinatorStub()
    root = {
        "access_history": AccessHistory(),
        "users_store": _UsersStoreStub(
            {
                "HA001": {
                    "name": "Daniel",
                    "ha_user_id": "ha-user-1",
                }
            }
        ),
        "entry-1": {
            "api": api,
            "coordinator": coordinator,
            "options": {
                "relay_roles": {
                    "relay_a": "alarm",
                    "relay_b": "door",
                }
            },
        },
    }
    hass = SimpleNamespace(data={DOMAIN: root})

    result = asyncio.run(
        async_open_gate(
            hass,
            root,
            entry_id="entry-1",
            triggered_by_id="ha-user-1",
            triggered_by_name="DJGLTD",
        )
    )

    assert result["ok"] is True
    assert result["relay"] == 2
    assert result["linked_user_id"] == "HA001"
    assert api.calls == [{"relay": 2, "delay": 20, "mode": 0, "level": 0}]

    assert len(coordinator.manual_events) == 1
    event = coordinator.manual_events[0]
    assert event["UserID"] == "HA001"
    assert event["HomeAssistantUserName"] == "DJGLTD"
    assert event["TriggeredBy"] == "Daniel"
    assert event["UserName"] == "Daniel"
    assert event["Event"] == "Opened with Home Assistant by Daniel"

    history = root["access_history"].snapshot(5)
    assert len(history) == 1
    assert history[0]["_category"] == "access"
    assert history[0]["LinkedUserID"] == "HA001"
    assert history[0]["LinkedUserName"] == "Daniel"


def test_open_gate_resolves_raw_home_assistant_context_user_id():
    api = _ApiStub()
    coordinator = _CoordinatorStub()
    raw_ha_user_id = "594d0cb232264d99a62056fae5a4b597"
    root = {
        "access_history": AccessHistory(),
        "users_store": _UsersStoreStub(
            {
                "HA001": {
                    "name": "Sam Smith",
                    "ha_user_id": raw_ha_user_id,
                }
            }
        ),
        "entry-1": {
            "api": api,
            "coordinator": coordinator,
            "options": {
                "relay_roles": {
                    "relay_a": "door",
                    "relay_b": "alarm",
                }
            },
        },
    }
    hass = SimpleNamespace(data={DOMAIN: root}, auth=_AuthStub())

    result = asyncio.run(
        async_open_gate(
            hass,
            root,
            entry_id="entry-1",
            triggered_by_id=raw_ha_user_id,
            triggered_by_name=raw_ha_user_id,
        )
    )

    event = result["event"]
    assert result["triggered_by"] == "Sam Smith"
    assert result["ha_user_name"] == "DJGLTD"
    assert result["linked_user_id"] == "HA001"
    assert event["HomeAssistantUserID"] == raw_ha_user_id
    assert event["HomeAssistantUserName"] == "DJGLTD"
    assert event["UserID"] == "HA001"
    assert event["LinkedUserName"] == "Sam Smith"
    assert event["Event"] == "Opened with Home Assistant by Sam Smith"


def test_open_gate_requires_entry_id_when_multiple_devices_are_configured():
    root = {
        "entry-1": {"api": _ApiStub(), "coordinator": _CoordinatorStub(), "options": {}},
        "entry-2": {"api": _ApiStub(), "coordinator": _CoordinatorStub(), "options": {}},
    }
    hass = SimpleNamespace(data={DOMAIN: root})

    try:
        asyncio.run(async_open_gate(hass, root, triggered_by_name="DJGLTD"))
    except RuntimeError as err:
        assert "entry_id or device_name is required" in str(err)
    else:
        raise AssertionError("Expected entry_id to be required")


def test_open_gate_can_select_device_by_friendly_name():
    first_api = _ApiStub()
    second_api = _ApiStub()
    first_coord = _CoordinatorStub()
    first_coord.device_name = "Gate"
    second_coord = _CoordinatorStub()
    second_coord.device_name = "Garage"
    root = {
        "entry-gate": {
            "api": first_api,
            "coordinator": first_coord,
            "options": {"relay_roles": {"relay_a": "door", "relay_b": "alarm"}},
        },
        "entry-garage": {
            "api": second_api,
            "coordinator": second_coord,
            "options": {"relay_roles": {"relay_a": "door", "relay_b": "alarm"}},
        },
    }
    hass = SimpleNamespace(data={DOMAIN: root})

    result = asyncio.run(
        async_open_gate(hass, root, device_name="Gate", triggered_by_name="DJGLTD")
    )

    assert result["entry_id"] == "entry-gate"
    assert first_api.calls == [{"relay": 1, "delay": 20, "mode": 0, "level": 0}]
    assert second_api.calls == []
