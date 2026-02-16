import asyncio
from typing import Any, Dict, List

import pytest

from custom_components.akuvox_ac.ha_test_stubs import ensure_homeassistant_stubs

ensure_homeassistant_stubs()

from custom_components.akuvox_ac.access_history import AccessHistory
from custom_components.akuvox_ac.coordinator import AkuvoxCoordinator


class _StorageStub:
    def __init__(self) -> None:
        self.data: Dict[str, Any] = {"door_events": {}, "notifications": {}}
        self.saved = False

    async def async_save(self) -> None:
        self.saved = True


class _APIStub:
    def __init__(self, events: List[Dict[str, Any]]) -> None:
        self._events = events

    async def events_last(self) -> List[Dict[str, Any]]:
        return list(self._events)


class _ServiceStub:
    def __init__(self, should_fail: bool = False) -> None:
        self.should_fail = should_fail
        self.calls: List[Dict[str, Any]] = []

    async def async_call(self, domain: str, service: str, data: Dict[str, Any], blocking: bool = False):
        self.calls.append(
            {
                "domain": domain,
                "service": service,
                "data": data,
                "blocking": blocking,
            }
        )
        if self.should_fail:
            raise RuntimeError("push unavailable")


def _build_coordinator(api, storage) -> AkuvoxCoordinator:
    coord = object.__new__(AkuvoxCoordinator)
    coord.api = api
    coord.storage = storage
    coord.entry_id = "device-1"
    coord.device_name = "Akuvox"
    coord.hass = type("H", (), {"data": {}, "loop": None})()
    coord._publish_access_history = lambda events: None  # type: ignore[attr-defined]
    return coord


def test_resolve_event_user_id_matches_profile_name_to_canonical_id():
    storage = _StorageStub()
    coord = _build_coordinator(_APIStub([]), storage)
    coord.users = [
        {"ID": "HA003", "Name": "Alice Walker"},
        {"ID": "HA004", "Name": "Bob Stone"},
    ]

    resolved = coord._resolve_event_user_id({"UserName": "alice walker"})

    assert resolved == "HA003"


def test_resolve_event_user_id_keeps_canonical_id_when_present():
    storage = _StorageStub()
    coord = _build_coordinator(_APIStub([]), storage)
    coord.users = [{"ID": "HA005", "Name": "Charlie"}]

    resolved = coord._resolve_event_user_id({"UserID": "ha005"})

    assert resolved == "HA005"


def test_dispatch_notification_appends_system_event_on_success():
    storage = _StorageStub()
    coord = _build_coordinator(_APIStub([]), storage)
    coord.events = []
    coord.hass.services = _ServiceStub()

    event = {
        "Event": "Door unlocked",
        "UserName": "Neil smalley",
        "Date": "2026-01-08",
        "Time": "08:30:00",
    }

    asyncio.run(coord._dispatch_notification(event, ["mobile_app_elles_iphone"]))

    assert len(coord.events) == 1
    assert (
        coord.events[0]["Event"]
        == "System notification sent to elles iphone — Neil smalley accessed the gate"
    )


def test_dispatch_notification_appends_system_event_on_failure():
    storage = _StorageStub()
    coord = _build_coordinator(_APIStub([]), storage)
    coord.events = []
    coord.hass.services = _ServiceStub(should_fail=True)

    event = {
        "Event": "Door unlocked",
        "UserName": "Neil smalley",
        "Date": "2026-01-08",
        "Time": "08:30:00",
    }

    asyncio.run(coord._dispatch_notification(event, ["mobile_app_elles_iphone"]))

    assert len(coord.events) == 1
    assert coord.events[0]["Event"].startswith("System notification failed for elles iphone —")


def test_process_door_events_skips_events_not_newer_than_last_timestamp():
    storage = _StorageStub()
    last_epoch = AccessHistory._coerce_timestamp("2024-04-10T12:00:00")
    storage.data["door_events"].update(
        {
            "last_event_key": "evt-new",
            "last_event_epoch": last_epoch,
        }
    )

    events = [
        {"ID": "evt-old", "Date": "2024-04-09", "Time": "09:15:00"},
        {"ID": "evt-new", "Date": "2024-04-10", "Time": "12:00:00"},
    ]
    api = _APIStub(events)
    coord = _build_coordinator(api, storage)

    handled: List[Dict[str, Any]] = []

    async def _handle(event, _targets):
        handled.append(event)
        return True

    coord._handle_door_event = _handle  # type: ignore[attr-defined]

    asyncio.run(coord._process_door_events())

    assert handled == []
    assert storage.saved is False
    assert storage.data["door_events"]["last_event_key"] == "evt-new"
    assert storage.data["door_events"]["last_event_epoch"] == last_epoch


def test_process_door_events_force_latest_handles_most_recent_event():
    storage = _StorageStub()
    last_epoch = AccessHistory._coerce_timestamp("2024-04-10T12:00:00")
    storage.data["door_events"].update(
        {
            "last_event_key": "evt-new",
            "last_event_epoch": last_epoch,
        }
    )

    events = [
        {"ID": "evt-new", "Date": "2024-04-10", "Time": "12:00:00"},
        {"ID": "evt-old", "Date": "2024-04-09", "Time": "09:15:00"},
    ]
    api = _APIStub(events)
    coord = _build_coordinator(api, storage)

    handled: List[Dict[str, Any]] = []

    async def _handle(event, _targets):
        handled.append(event)
        return False

    coord._handle_door_event = _handle  # type: ignore[attr-defined]

    asyncio.run(coord._process_door_events(force_latest=True))

    assert [event["ID"] for event in handled] == ["evt-new"]
    assert storage.saved is False
    assert storage.data["door_events"]["last_event_key"] == "evt-new"
    assert storage.data["door_events"]["last_event_epoch"] == last_epoch


def test_process_door_events_updates_state_with_newer_events():
    storage = _StorageStub()
    storage.data["door_events"]["last_event_key"] = "evt-old"
    storage.data["door_events"]["last_event_epoch"] = AccessHistory._coerce_timestamp(
        "2024-04-09T08:00:00"
    )

    events = [
        {"ID": "evt-new", "Date": "2024-04-10", "Time": "13:45:00"},
        {"ID": "evt-old", "Date": "2024-04-09", "Time": "08:00:00"},
    ]

    api = _APIStub(events)
    coord = _build_coordinator(api, storage)

    handled: List[Dict[str, Any]] = []

    async def _handle(event, _targets):
        handled.append(event)
        return True

    coord._handle_door_event = _handle  # type: ignore[attr-defined]

    asyncio.run(coord._process_door_events())

    assert [event["ID"] for event in handled] == ["evt-new"]
    assert storage.saved is True

    last_epoch = storage.data["door_events"]["last_event_epoch"]
    expected_epoch = AccessHistory._coerce_timestamp("2024-04-10T13:45:00")
    assert pytest.approx(last_epoch, rel=1e-6) == expected_epoch
    assert storage.data["door_events"]["last_event_key"] == "evt-new"
