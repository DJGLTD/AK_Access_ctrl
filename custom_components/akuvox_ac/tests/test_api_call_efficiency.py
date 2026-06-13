import asyncio
from types import SimpleNamespace

from custom_components.akuvox_ac.ha_test_stubs import ensure_homeassistant_stubs

ensure_homeassistant_stubs()

from custom_components.akuvox_ac import http as http_module  # noqa: E402
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
