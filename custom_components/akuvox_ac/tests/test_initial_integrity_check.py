"""Device checks must survive long intervals, restarts, and manual syncs."""

import asyncio
from datetime import timedelta
from types import SimpleNamespace

import pytest

from custom_components.akuvox_ac import integration
from custom_components.akuvox_ac.coordinator import AkuvoxCoordinator


class _Storage:
    def __init__(self, last_checked=None):
        self.data = {"last_checked": last_checked}
        self.saved = 0

    async def async_save(self):
        self.saved += 1


class _Api:
    def __init__(self, users=None, *, fail_users=False, fail_schedules=False):
        self.users = list(users or [])
        self.fail_users = fail_users
        self.fail_schedules = fail_schedules
        self.user_reads = 0
        self.schedule_reads = 0

    async def user_list(self, *, strict=False):
        assert strict, "A comparison must distinguish a failed fetch from no users"
        self.user_reads += 1
        if self.fail_users:
            raise RuntimeError("user read unavailable")
        return list(self.users)

    async def schedule_get(self, *, strict=False):
        assert strict, "A comparison must require a valid schedule response"
        self.schedule_reads += 1
        if self.fail_schedules:
            raise RuntimeError("schedule read unavailable")
        return []


def _coordinator(*, last_checked=None, sync_status="in_sync", device_type="Keypad"):
    coord = object.__new__(AkuvoxCoordinator)
    coord.health = {
        "last_checked": last_checked,
        "sync_status": sync_status,
        "device_type": device_type,
        "online": True,
    }
    coord.storage = _Storage(last_checked)
    coord.users = []
    coord.events = []
    coord._append_event = coord.events.append
    coord.async_update_listeners = lambda: None

    async def refresh_access():
        return None

    coord.async_refresh_access_history = refresh_access
    return coord


def _manager(devices, *, profiles=None, running=True, queue=None):
    manager = object.__new__(integration.SyncManager)
    manager._integrity_minutes = 1440
    root = {"sync_queue": queue}
    for entry_id, coord, api, opts in devices:
        root[entry_id] = {"coordinator": coord, "api": api, "options": opts}
    manager.hass = SimpleNamespace(
        is_running=running,
        data={integration.DOMAIN: root},
        config=SimpleNamespace(internal_url=None, external_url=None),
    )
    manager._devices = lambda: devices
    manager._settings_store = lambda: None
    manager._users_store = lambda: SimpleNamespace(all=lambda: profiles or {})
    manager._schedules_store = lambda: None
    root["sync_manager"] = manager
    return manager


def test_new_keypad_checked_at_first_tick_with_daily_interval():
    coord, api = _coordinator(), _Api()
    manager = _manager([("keypad", coord, api, {})])

    asyncio.run(manager._integrity_check_cb(None))

    assert api.user_reads == 1
    assert api.schedule_reads == 1
    assert coord.health["last_checked"]
    assert coord.storage.data["last_checked"] == coord.health["last_checked"]
    assert "Integrity check passed" in coord.events


def test_restart_respects_persisted_due_time_and_skips_recent_device():
    now = integration.dt_util.now()
    recent = (now - timedelta(hours=2)).isoformat()
    overdue = (now - timedelta(hours=25)).isoformat()
    recent_coord, recent_api = _coordinator(last_checked=recent), _Api()
    overdue_coord, overdue_api = _coordinator(last_checked=overdue), _Api()
    manager = _manager([
        ("recent", recent_coord, recent_api, {}),
        ("overdue", overdue_coord, overdue_api, {}),
    ])

    asyncio.run(manager._integrity_check_cb(None))

    assert recent_api.user_reads == 0
    assert recent_coord.health["last_checked"] == recent
    assert overdue_api.user_reads == 1
    assert overdue_coord.health["last_checked"] != overdue


def test_initial_check_waits_until_home_assistant_is_running():
    coord, api = _coordinator(), _Api()
    manager = _manager([("keypad", coord, api, {})], running=False)

    asyncio.run(manager._integrity_check_cb(None))
    assert api.user_reads == 0
    assert coord.health["last_checked"] is None

    manager.hass.is_running = True
    asyncio.run(manager._integrity_check_cb(None))
    assert api.user_reads == 1
    assert coord.health["last_checked"]


def test_pending_device_does_not_block_new_keypad():
    coord, api = _coordinator(), _Api()
    pending_coord, pending_api = _coordinator(sync_status="pending"), _Api()
    queue = SimpleNamespace(_handle=object(), _active=False, _lock=asyncio.Lock())
    manager = _manager([
        ("pending", pending_coord, pending_api, {}),
        ("keypad", coord, api, {}),
    ], queue=queue)

    asyncio.run(manager._integrity_check_cb(None))

    assert pending_api.user_reads == 0
    assert coord.health["last_checked"]


def test_failed_check_preserves_timestamp_and_backs_off(monkeypatch):
    clock = [1000.0]
    monkeypatch.setattr(integration.time, "monotonic", lambda: clock[0])
    coord, api = _coordinator(), _Api(fail_users=True)
    manager = _manager([("keypad", coord, api, {})])

    asyncio.run(manager._integrity_check_cb(None))
    assert api.user_reads == 1
    assert coord.health["last_checked"] is None
    assert coord.storage.data["last_checked"] is None

    clock[0] += 60
    asyncio.run(manager._integrity_check_cb(None))
    assert api.user_reads == 1

    clock[0] += 240
    api.fail_users = False
    asyncio.run(manager._integrity_check_cb(None))
    assert api.user_reads == 2
    assert coord.health["last_checked"]


def test_failed_schedule_read_does_not_claim_completed_check():
    previous = (integration.dt_util.now() - timedelta(days=2)).isoformat()
    coord = _coordinator(last_checked=previous)
    api = _Api(fail_schedules=True)
    manager = _manager([("keypad", coord, api, {})])

    result = asyncio.run(manager.async_check_integrity(entry_id="keypad", repair=False))

    assert result == {}
    assert coord.health["last_checked"] == previous
    assert coord.storage.data["last_checked"] == previous


def _queue(manager):
    queue = object.__new__(integration.SyncQueue)
    queue.hass = manager.hass
    queue._lock = asyncio.Lock()
    queue._active = False
    queue._handle = None
    queue._pending_all = False
    queue._pending_devices = set()
    queue._pending_full = False
    queue._pending_full_devices = set()
    queue._pending_reason_all = None
    queue._pending_reason_devices = {}
    queue.next_sync_eta = None
    manager._root()["sync_queue"] = queue
    return queue


@pytest.mark.parametrize("full", [True, False, None])
def test_sync_verifies_fresh_data_without_recursive_queue_call(full):
    coord, api = _coordinator(), _Api()
    manager = _manager([("keypad", coord, api, {})])
    queue = _queue(manager)
    steps = []

    async def reconcile(entry_id, **kwargs):
        assert entry_id == "keypad"
        assert kwargs["full"] is bool(full)
        assert queue._lock.locked()
        steps.append("reconcile")
        assert api.user_reads == 0

    async def unexpected_recursive_sync(*args, **kwargs):
        raise AssertionError("Verification must not reenter the sync queue")

    manager.reconcile_device = reconcile
    queue.sync_now = unexpected_recursive_sync
    asyncio.run(asyncio.wait_for(queue.run(only_entry="keypad", full=full), timeout=1))

    assert steps == ["reconcile"]
    assert api.user_reads == 1
    assert coord.health["sync_status"] == "in_sync"
    assert coord.health["last_checked"]


def test_full_sync_mismatch_records_check_but_stays_pending():
    coord, api = _coordinator(), _Api()
    manager = _manager([("keypad", coord, api, {})], profiles={
        "HA001": {"name": "Missing User", "groups": ["Default"]},
    })
    queue = _queue(manager)

    async def reconcile(*args, **kwargs):
        return None

    async def unexpected_recursive_sync(*args, **kwargs):
        raise AssertionError("Verification must not start a recursive repair")

    manager.reconcile_device = reconcile
    queue.sync_now = unexpected_recursive_sync
    asyncio.run(asyncio.wait_for(queue.run(only_entry="keypad", full=True), timeout=1))

    assert api.user_reads == 1
    assert coord.health["sync_status"] == "pending"
    assert coord.health["last_checked"]
    assert any("missing HA001" in event for event in coord.events)
    assert not any("queued sync" in event for event in coord.events)


def test_face_cooldown_comparison_does_not_reference_undefined_full(monkeypatch):
    coord = _coordinator(device_type="Intercom")
    api = _Api(users=[{"UserID": "HA001", "Name": "User One", "FaceRegister": 0}])
    manager = _manager([("intercom", coord, api, {})], profiles={
        "HA001": {"name": "User One", "schedule_name": "24/7 Access"},
    })
    monkeypatch.setattr(integration, "_face_sync_on_cooldown", lambda _profile: True)
    monkeypatch.setattr(integration, "_desired_device_user_payload", lambda *a, **kw: {
        "UserID": "HA001", "Name": "User One", "FaceRegister": 1,
    })

    asyncio.run(manager._integrity_check_cb(None))

    assert coord.health["last_checked"]
    assert "Integrity check error" not in coord.events


def test_verification_does_not_repair_missing_face(monkeypatch):
    coord = _coordinator(device_type="Intercom", sync_status="in_progress")
    api = _Api(users=[{"UserID": "HA001", "Name": "User One", "FaceRegister": 0}])
    manager = _manager([("intercom", coord, api, {})], profiles={"HA001": {"name": "User One"}})
    monkeypatch.setattr(integration, "_desired_device_user_payload", lambda *a, **kw: {
        "UserID": "HA001", "Name": "User One", "FaceRegister": 1,
    })

    async def unexpected_repair(*args, **kwargs):
        raise AssertionError("Verification must not upload faces or mutate profiles")

    manager._upload_face_asset_to_device = unexpected_repair
    manager._bump_face_error_count = unexpected_repair
    result = asyncio.run(manager.async_check_integrity(entry_id="intercom", repair=False))

    assert result == {"intercom": False}
    assert coord.health["last_checked"]


def test_scheduler_wakes_promptly_even_with_daily_check_interval(monkeypatch):
    intervals = []
    manager = object.__new__(integration.SyncManager)
    manager.hass = SimpleNamespace()
    manager._integrity_unsub = None
    monkeypatch.setattr(integration, "async_track_time_interval", lambda hass, cb, interval: intervals.append(interval))

    manager._apply_integrity_interval(1440)

    assert intervals == [timedelta(minutes=1)]
    assert manager.get_integrity_interval_minutes() == 1440


def test_busy_queue_does_not_back_off_an_unattempted_first_check():
    coord, api = _coordinator(), _Api()
    manager = _manager([("keypad", coord, api, {})])
    queue = _queue(manager)
    queue._active = True

    asyncio.run(manager._integrity_check_cb(None))
    assert api.user_reads == 0

    queue._active = False
    asyncio.run(manager._integrity_check_cb(None))
    assert api.user_reads == 1
    assert coord.health["last_checked"]


def test_pending_device_is_checked_as_soon_as_eligible():
    coord, api = _coordinator(sync_status="pending"), _Api()
    manager = _manager([("keypad", coord, api, {})])

    asyncio.run(manager._integrity_check_cb(None))
    assert api.user_reads == 0

    coord.health["sync_status"] = "in_sync"
    asyncio.run(manager._integrity_check_cb(None))
    assert api.user_reads == 1


@pytest.mark.parametrize("failure", ["fail_users", "fail_schedules"])
def test_manual_sync_failed_read_keeps_previous_check_and_reports_failure(failure):
    previous = (integration.dt_util.now() - timedelta(days=2)).isoformat()
    coord, api = _coordinator(last_checked=previous), _Api(**{failure: True})
    manager = _manager([("keypad", coord, api, {})])
    queue = _queue(manager)

    async def reconcile(*args, **kwargs):
        return None

    manager.reconcile_device = reconcile
    asyncio.run(queue.sync_now("keypad"))

    assert coord.health["last_checked"] == previous
    assert coord.health["sync_status"] == "pending"
    assert any("Sync failed" in event for event in coord.events)
    assert not any("Sync succeeded" in event for event in coord.events)


def test_manual_sync_waits_for_scheduled_comparison():
    async def scenario():
        coord, api = _coordinator(), _Api()
        manager = _manager([("keypad", coord, api, {})])
        queue = _queue(manager)
        read_started, release_read, reconcile_started = (
            asyncio.Event(), asyncio.Event(), asyncio.Event()
        )
        user_list = api.user_list

        async def blocked_read(**kwargs):
            read_started.set()
            await release_read.wait()
            return await user_list(**kwargs)

        async def reconcile(*args, **kwargs):
            reconcile_started.set()

        api.user_list = blocked_read
        manager.reconcile_device = reconcile
        scheduled = asyncio.create_task(manager._integrity_check_cb(None))
        await read_started.wait()
        manual = asyncio.create_task(queue.sync_now("keypad"))
        await asyncio.sleep(0)
        assert not reconcile_started.is_set()
        release_read.set()
        await asyncio.gather(scheduled, manual)
        assert reconcile_started.is_set()
        assert api.user_reads == 2
        assert coord.health["sync_status"] == "in_sync"

    asyncio.run(asyncio.wait_for(scenario(), timeout=2))


def test_scheduled_mismatch_repairs_after_releasing_queue_lock(monkeypatch):
    coord, api = _coordinator(), _Api()
    manager = _manager([("keypad", coord, api, {})], profiles={
        "HA001": {"name": "User One"},
    })
    queue = _queue(manager)
    monkeypatch.setattr(integration, "_desired_device_user_payload", lambda *a, **kw: {
        "UserID": "HA001", "Name": "User One",
    })
    # Recording a change normally arms a delayed callback; this test exercises
    # the immediate repair and its real queue lock without installing a timer.
    queue.mark_change = lambda *args, **kwargs: None

    async def reconcile(*args, **kwargs):
        api.users = [{"UserID": "HA001", "Name": "User One"}]

    manager.reconcile_device = reconcile
    asyncio.run(asyncio.wait_for(manager._integrity_check_cb(None), timeout=2))

    assert api.user_reads == 2
    assert coord.health["sync_status"] == "in_sync"
    assert coord.health["last_checked"]
    assert "Integrity check passed" in coord.events
