import asyncio
from types import SimpleNamespace

from custom_components.akuvox_ac.ha_test_stubs import ensure_homeassistant_stubs

ensure_homeassistant_stubs()

from custom_components.akuvox_ac import http as http_module  # noqa: E402
from custom_components.akuvox_ac.coordinator import AkuvoxCoordinator  # noqa: E402
from custom_components.akuvox_ac.integration import SyncManager  # noqa: E402


class _ScheduleApiStub:
    def __init__(self, *, fail_get=False) -> None:
        self.fail_get = fail_get
        self.schedule_get_calls = 0
        self.schedule_add_calls = []
        self.schedule_set_calls = []

    async def schedule_get(self):
        self.schedule_get_calls += 1
        if self.fail_get:
            raise RuntimeError("device unavailable")
        return []

    async def schedule_add(self, name, spec):
        self.schedule_add_calls.append((name, dict(spec)))

    async def schedule_set(self, name, spec):
        self.schedule_set_calls.append((name, dict(spec)))


class _UsersStoreStub:
    def all(self):
        return {
            "HA001": {
                "schedule_name": "Night Staff",
                "schedule_id": "8",
            }
        }


def test_dashboard_schedule_ids_use_cached_state_without_device_request():
    class _ApiThatMustNotBeCalled:
        async def schedule_get(self):
            raise AssertionError("dashboard state must not fetch schedules from the device")

    coordinator = type(
        "Coordinator",
        (),
        {"schedule_ids": {"Post Man": "3"}},
    )()
    root = {
        "device-1": {
            "api": _ApiThatMustNotBeCalled(),
            "coordinator": coordinator,
            "options": {},
        },
        "users_store": _UsersStoreStub(),
    }

    schedule_ids = asyncio.run(http_module._fetch_device_schedule_ids(root))

    assert schedule_ids == {
        "24/7 Access": "1001",
        "No Access": "1002",
        "Post Man": "3",
        "Night Staff": "8",
    }


def test_schedule_map_reuses_supplied_device_snapshot():
    manager = object.__new__(SyncManager)
    api = _ScheduleApiStub()
    snapshot = [{"Name": "Post Man", "ID": "5", "DisplayID": "3"}]

    schedule_map = asyncio.run(
        manager._device_schedule_map(api, device_schedules=snapshot)
    )

    assert api.schedule_get_calls == 0
    assert schedule_map["post man"] == "3"


def test_schedule_push_reuses_snapshot_and_supplies_ids_for_update():
    manager = object.__new__(SyncManager)
    api = _ScheduleApiStub()
    snapshot = [{"Name": "Post Man", "ID": "5", "DisplayID": "3"}]

    added = asyncio.run(
        manager._push_schedules(
            api,
            {"Post Man": {"start": "07:00", "end": "18:00"}},
            device_schedules=snapshot,
        )
    )

    assert added is False
    assert api.schedule_get_calls == 0
    assert api.schedule_add_calls == []
    assert api.schedule_set_calls == [
        (
            "Post Man",
                {
                    "start": "07:00",
                    "end": "18:00",
                    "days": [],
                    "ID": "5",
                    "DisplayID": "3",
                },
        )
    ]


def test_schedule_push_does_not_add_when_initial_schedule_read_fails():
    manager = object.__new__(SyncManager)
    api = _ScheduleApiStub(fail_get=True)

    added = asyncio.run(
        manager._push_schedules(
            api,
            {"Post Man": {"start": "07:00", "end": "18:00"}},
        )
    )

    assert added is False
    assert api.schedule_get_calls == 1
    assert api.schedule_add_calls == []
    assert api.schedule_set_calls == []


def test_integrity_tick_refreshes_access_events_before_deferred_sync():
    manager = object.__new__(SyncManager)

    class _Coordinator:
        def __init__(self):
            self.refresh_calls = 0

        async def async_refresh_access_history(self):
            self.refresh_calls += 1

    coordinator = _Coordinator()
    devices = [("entry-1", coordinator, object(), {})]
    manager._devices = lambda: devices
    manager._root = lambda: {"sync_queue": SimpleNamespace(_handle=object())}

    asyncio.run(manager._integrity_check_cb(None))

    assert coordinator.refresh_calls == 1


def test_integrity_tick_records_completed_device_comparison():
    manager = object.__new__(SyncManager)

    class _Api:
        async def user_list(self):
            return []

        async def schedule_get(self):
            return []

    class _Coordinator:
        def __init__(self):
            self.health = {
                "sync_status": "in_sync",
                "device_type": "Intercom",
            }
            self.users = []
            self.storage = None
            self.checked_at = None

        async def async_refresh_access_history(self):
            return None

        async def async_record_integrity_check(self, checked_at):
            self.checked_at = checked_at

        def _append_event(self, _message):
            return None

    coordinator = _Coordinator()
    manager.hass = SimpleNamespace(
        config=SimpleNamespace(internal_url=None, external_url=None)
    )
    manager._devices = lambda: [("entry-1", coordinator, _Api(), {})]
    manager._root = lambda: {"sync_queue": None}
    manager._settings_store = lambda: None
    manager._users_store = lambda: None
    manager._schedules_store = lambda: None

    async def _device_schedule_map(_api, *, device_schedules=None):
        return {"24/7 access": "1001", "no access": "1002"}

    manager._device_schedule_map = _device_schedule_map

    asyncio.run(manager._integrity_check_cb(None))

    assert coordinator.checked_at
    assert "T" in coordinator.checked_at


def test_completed_integrity_check_is_persisted_for_dashboard_restart():
    class _Storage:
        def __init__(self):
            self.data = {}
            self.saved = False

        async def async_save(self):
            self.saved = True

    coordinator = object.__new__(AkuvoxCoordinator)
    coordinator.health = {}
    coordinator.storage = _Storage()
    coordinator.async_update_listeners = lambda: None

    checked_at = "2026-06-14T09:30:00+01:00"
    result = asyncio.run(coordinator.async_record_integrity_check(checked_at))

    assert result == checked_at
    assert coordinator.health["last_checked"] == checked_at
    assert coordinator.storage.data["last_checked"] == checked_at
    assert coordinator.storage.saved is True
