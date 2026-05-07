import asyncio
from typing import Any, Dict, List

import pytest

from custom_components.akuvox_ac.ha_test_stubs import ensure_homeassistant_stubs

ensure_homeassistant_stubs()

from custom_components.akuvox_ac.access_history import AccessHistory
from custom_components.akuvox_ac.const import DOMAIN
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


class _SettingsStub:
    def __init__(self, targets: List[str]) -> None:
        self.targets = list(targets)

    def targets_for_event(self, event_type: str, *, user_id: str | None = None) -> List[str]:
        return list(self.targets)


class _HealthAPIStub:
    async def ping_info(self) -> Dict[str, Any]:
        return {"ok": True}

    async def user_list(self) -> List[Dict[str, Any]]:
        return []


class _SyncQueueStub:
    def __init__(self) -> None:
        self.calls: List[str] = []

    async def sync_now(self, entry_id: str) -> None:
        self.calls.append(entry_id)


def _build_coordinator(api, storage) -> AkuvoxCoordinator:
    coord = object.__new__(AkuvoxCoordinator)
    coord.api = api
    coord.storage = storage
    coord.entry_id = "device-1"
    coord.device_name = "Akuvox"
    coord.hass = type("H", (), {"data": {}, "loop": None})()
    coord._publish_access_history = lambda events: None  # type: ignore[attr-defined]
    coord.users = []
    return coord


def _build_health_coordinator(queue: _SyncQueueStub) -> AkuvoxCoordinator:
    storage = _StorageStub()
    coord = _build_coordinator(_HealthAPIStub(), storage)
    coord.hass.data = {DOMAIN: {"sync_queue": queue}}
    coord.events = []
    coord.users = []
    coord.health = {
        "name": "Akuvox",
        "online": False,
        "status": "offline",
        "sync_status": "pending",
        "last_sync": None,
        "last_error": None,
        "last_ping": None,
    }
    coord._process_door_events = _async_noop_events  # type: ignore[method-assign]
    return coord


async def _async_noop_events(*_args, **_kwargs):
    return []


def test_initial_online_refresh_does_not_sync_during_startup():
    queue = _SyncQueueStub()
    coord = _build_health_coordinator(queue)
    coord._was_online = None

    asyncio.run(coord._async_update_data())

    assert coord.health["online"] is True
    assert queue.calls == []


def test_offline_to_online_refresh_still_syncs():
    queue = _SyncQueueStub()
    coord = _build_health_coordinator(queue)
    coord._was_online = False

    asyncio.run(coord._async_update_data())

    assert queue.calls == ["device-1"]


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
    diag = storage.data["notification_diagnostics"]
    assert diag[0]["status"] == "sent"
    assert diag[0]["channel"] == "access_notification"
    assert diag[0]["target"] == "mobile_app_elles_iphone"


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
    diag = storage.data["notification_diagnostics"]
    assert diag[0]["status"] == "failed"
    assert diag[0]["error"] == "push unavailable"


def test_send_alert_notification_records_system_notification():
    storage = _StorageStub()
    coord = _build_coordinator(_APIStub([]), storage)
    coord.hass.services = _ServiceStub()
    coord.hass.data = {DOMAIN: {"settings_store": _SettingsStub(["mobile_app_admin_phone"])}}

    asyncio.run(
        coord._send_alert_notification(
            "user_granted",
            user_id="HA007",
            summary="access granted",
            extra={"event": {"UserName": "Alice"}},
        )
    )

    diag = storage.data["notification_diagnostics"]
    assert diag[0]["status"] == "sent"
    assert diag[0]["channel"] == "alert_notification"
    assert diag[0]["event_type"] == "user_granted"
    assert diag[0]["message"] == "Alice opened the gate."


def test_access_permitted_button_records_would_notify_targets_when_skipped():
    storage = _StorageStub()
    storage.data["notifications"] = {"targets": ["mobile_app_gate_phone"]}
    coord = _build_coordinator(_APIStub([]), storage)
    coord.hass.data = {DOMAIN: {"settings_store": _SettingsStub(["mobile_app_admin_phone"])}}
    coord._has_recent_door_event = lambda _window: False  # type: ignore[method-assign]

    handled: List[Dict[str, Any]] = []

    async def _handle(event, _targets):
        handled.append(dict(event))
        return False

    coord._handle_door_event = _handle  # type: ignore[attr-defined]

    asyncio.run(coord.async_handle_manual_event({"Event": "Access permitted button pressed"}))

    assert handled[0]["_skip_notifications"] is True
    diag = storage.data["notification_diagnostics"]
    assert diag[0]["source"] == "access_permitted_button"
    assert diag[0]["status"] == "skipped"
    assert [item["target"] for item in diag[0]["targets"]] == [
        "mobile_app_gate_phone",
        "mobile_app_admin_phone",
    ]


def test_suppressed_door_event_records_user_specific_notification_targets():
    storage = _StorageStub()
    coord = _build_coordinator(_APIStub([]), storage)
    coord.hass.data = {DOMAIN: {"settings_store": _SettingsStub(["mobile_app_admin_phone"])}}

    recorded = coord._record_suppressed_notification_diagnostic(
        {
            "Event": "Door unlocked",
            "UserID": "HA007",
            "UserName": "Alice",
            "_suppressed_notification_targets": ["mobile_app_gate_phone"],
        },
        event_kind="granted",
        user_id="HA007",
        summary="door unlocked",
        notify_targets=[],
    )

    assert recorded is True
    diag = storage.data["notification_diagnostics"]
    assert diag[0]["source"] == "suppressed_door_event"
    assert diag[0]["user_id"] == "HA007"
    assert [item["target"] for item in diag[0]["targets"]] == [
        "mobile_app_gate_phone",
        "mobile_app_admin_phone",
    ]


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
