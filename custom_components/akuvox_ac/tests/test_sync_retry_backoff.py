"""Dashboard polling and automatic recovery must not create sync retry storms."""

import asyncio
from copy import deepcopy
from datetime import datetime, timedelta
from types import SimpleNamespace

import pytest

from custom_components.akuvox_ac import integration
from custom_components.akuvox_ac.tests.test_initial_integrity_check import (
    _Api,
    _coordinator,
    _manager,
)


class _SyncScenario:
    """Run real queue operations with deterministic time and HA callbacks."""

    def __init__(self, monkeypatch, *, profiles=None):
        self.seconds = 0.0
        self.tasks = []
        self.timers = []
        self.reconciles = []
        self.clock_start = datetime(2026, 9, 8, 12, 0)
        scenario = self

        class ClockDateTime(datetime):
            @classmethod
            def now(cls, tz=None):
                now = scenario.clock_start + timedelta(seconds=scenario.seconds)
                return cls.fromtimestamp(now.timestamp(), tz=tz)

        monkeypatch.setattr(integration, "datetime", ClockDateTime)
        monkeypatch.setattr(integration.time, "monotonic", lambda: 1000 + self.seconds)
        monkeypatch.setattr(
            integration, "async_track_time_interval", lambda *args: lambda: None
        )
        monkeypatch.setattr(integration, "async_call_later", self._call_later)
        self.keypad, self.keypad_api = _coordinator(), _Api()
        self.intercom, self.intercom_api = _coordinator(), _Api()
        self.devices = [
            ("keypad", self.keypad, self.keypad_api, {}),
            ("intercom", self.intercom, self.intercom_api, {}),
        ]
        self.manager = _manager(self.devices, profiles=profiles)
        self.manager._settings_store = lambda: SimpleNamespace(
            get_auto_sync_time=lambda: "05:00"
        )
        self.manager._root()["users_store"] = self.manager._users_store()
        self.queue = integration.SyncQueue(self.manager.hass)
        self.manager._root()["sync_queue"] = self.queue
        self.queue._schedule_task = self.tasks.append

        async def reconcile(entry_id, *, full=False):
            self.reconciles.append((self.seconds, entry_id, full))

        self.manager.reconcile_device = reconcile

    def _call_later(self, hass, delay, callback):
        timer = {"due": self.seconds + delay, "callback": callback, "cancelled": False}
        self.timers.append(timer)

        def cancel():
            timer["cancelled"] = True

        return cancel

    def advance(self, seconds):
        self.seconds += seconds
        due = [
            timer for timer in self.timers
            if not timer["cancelled"] and timer["due"] <= self.seconds
        ]
        for timer in due:
            timer["cancelled"] = True
            timer["callback"](integration.datetime.now())

    async def drain(self):
        count = 0
        while self.tasks:
            count += 1
            assert count < 20, "Automatic recovery continuously rescheduled itself"
            await self.tasks.pop(0)

    async def tick(self, seconds=0):
        self.advance(seconds)
        await self.queue._background_tick(integration.datetime.now())
        await self.drain()

    def close(self):
        for task in self.tasks:
            task.close()
        self.tasks.clear()
        self.queue.shutdown()


@pytest.fixture
def scenario(monkeypatch):
    result = _SyncScenario(monkeypatch)
    yield result
    result.close()


@pytest.mark.parametrize("eta_offset", [None, -60, 60])
def test_dashboard_reads_do_not_enqueue_or_mutate_pending_sync(scenario, eta_offset):
    scenario.keypad.health["sync_status"] = "pending"
    if eta_offset is not None:
        scenario.queue.next_sync_eta = (
            integration.datetime.now() + timedelta(seconds=eta_offset)
        )
    before = deepcopy((
        scenario.keypad.health,
        scenario.intercom.health,
        scenario.queue.next_sync_eta,
        scenario.queue._pending_devices,
        scenario.queue._pending_all,
    ))

    for _ in range(12):
        scenario.manager.get_next_sync_text()

    assert scenario.tasks == []
    assert scenario.timers == []
    assert scenario.reconciles == []
    assert before == (
        scenario.keypad.health,
        scenario.intercom.health,
        scenario.queue.next_sync_eta,
        scenario.queue._pending_devices,
        scenario.queue._pending_all,
    )


def test_failed_keypad_retries_after_delay_without_syncing_healthy_intercom(scenario):
    async def run():
        scenario.keypad_api.fail_schedules = True
        await scenario.queue.sync_now("keypad", full=True)
        assert scenario.keypad.health["sync_status"] == "pending"
        assert len(scenario.reconciles) == 1

        for _ in range(19):
            await scenario.tick(15)
        assert len(scenario.reconciles) == 1
        assert scenario.intercom.health["sync_status"] == "in_sync"

        await scenario.tick(15)
        assert scenario.reconciles == [
            (0, "keypad", True), (300, "keypad", True),
        ]

        # A second failure waits ten minutes, not another five-minute loop.
        await scenario.tick(599)
        assert len(scenario.reconciles) == 2
        await scenario.tick(1)
        assert scenario.reconciles[-1] == (900, "keypad", True)
        assert len(scenario.reconciles) == 3
        assert scenario.intercom_api.user_reads == 0

    asyncio.run(run())


def test_repeated_failures_back_off_but_never_wait_more_than_an_hour(scenario):
    async def run():
        scenario.keypad_api.fail_users = True
        await scenario.queue.sync_now("keypad", full=True)

        for attempt, delay in enumerate((300, 600, 1200, 2400, 3600, 3600), start=1):
            await scenario.tick(delay - 1)
            assert len(scenario.reconciles) == attempt
            await scenario.tick(1)
            assert len(scenario.reconciles) == attempt + 1
            assert scenario.reconciles[-1][1:] == ("keypad", True)

    asyncio.run(run())


def test_failed_devices_have_independent_retry_deadlines(scenario):
    async def run():
        scenario.keypad_api.fail_users = True
        scenario.intercom_api.fail_users = True
        await scenario.queue.sync_now("keypad")
        scenario.advance(60)
        await scenario.queue.sync_now("intercom")

        await scenario.tick(240)
        assert [entry for _, entry, _ in scenario.reconciles] == [
            "keypad", "intercom", "keypad",
        ]
        await scenario.tick(60)
        assert [entry for _, entry, _ in scenario.reconciles] == [
            "keypad", "intercom", "keypad", "intercom",
        ]

    asyncio.run(run())


@pytest.mark.parametrize("manual_full", [False, True])
def test_manual_sync_bypasses_retry_delay_and_success_clears_it(scenario, manual_full):
    async def run():
        scenario.keypad_api.fail_users = True
        await scenario.queue.sync_now("keypad", full=True)
        assert scenario.keypad.health["sync_status"] == "pending"

        scenario.advance(15)
        scenario.keypad_api.fail_users = False
        await scenario.queue.sync_now("keypad", full=manual_full)
        assert len(scenario.reconciles) == 2
        assert scenario.reconciles[-1] == (15, "keypad", manual_full)
        assert scenario.keypad.health["sync_status"] == "in_sync"
        assert scenario.keypad.health["last_checked"]

        # Passing the old failure deadline must not requeue a recovered device.
        await scenario.tick(300)
        assert len(scenario.reconciles) == 2

        # An independent new failure starts with the initial five-minute delay.
        scenario.keypad_api.fail_users = True
        await scenario.queue.sync_now("keypad", full=False)
        await scenario.tick(299)
        assert len(scenario.reconciles) == 3
        await scenario.tick(1)
        assert len(scenario.reconciles) == 4
        assert scenario.reconciles[-1][1] == "keypad"

    asyncio.run(run())


def test_background_ticks_do_not_duplicate_already_queued_recovery(scenario):
    async def run():
        scenario.keypad.health["sync_status"] = "pending"
        for _ in range(5):
            await scenario.queue._background_tick(integration.datetime.now())
        await scenario.drain()

        assert scenario.reconciles == [(0, "keypad", False)]
        assert scenario.keypad.health["sync_status"] == "in_sync"
        assert scenario.intercom_api.user_reads == 0
        await scenario.tick(60)
        assert len(scenario.reconciles) == 1

    asyncio.run(run())


@pytest.mark.parametrize("followup_delay", [0, 5])
def test_change_during_active_sync_preserves_followup_work(scenario, followup_delay):
    async def reconcile(entry_id, *, full=False):
        scenario.reconciles.append((scenario.seconds, entry_id, full))
        if len(scenario.reconciles) == 1:
            assert scenario.queue._active
            scenario.queue.mark_change(
                "keypad", delay_minutes=followup_delay, full=True,
                trigger="schedule changed during sync",
            )

    scenario.manager.reconcile_device = reconcile

    async def run():
        scenario.queue.mark_change("keypad", delay_minutes=0)
        await scenario.drain()

        if followup_delay:
            assert scenario.reconciles == [(0, "keypad", False)]
            await scenario.tick(followup_delay * 60 - 1)
            assert len(scenario.reconciles) == 1
            await scenario.tick(1)

        assert scenario.reconciles == [
            (0, "keypad", False), (followup_delay * 60, "keypad", True),
        ]
        assert scenario.keypad.health["sync_status"] == "in_sync"
        assert scenario.intercom_api.user_reads == 0
        await scenario.tick(60)
        assert len(scenario.reconciles) == 2

    asyncio.run(run())


def test_manual_sync_supersedes_automatic_work_not_yet_started(scenario):
    async def run():
        scenario.keypad.health["sync_status"] = "pending"
        await scenario.queue._background_tick(integration.datetime.now())
        assert scenario.tasks, "Automatic recovery should have queued work"
        assert scenario.reconciles == []

        await scenario.queue.sync_now("keypad", full=True)
        await scenario.drain()

        assert scenario.reconciles == [(0, "keypad", True)]
        assert scenario.keypad.health["sync_status"] == "in_sync"
        assert scenario.intercom_api.user_reads == 0

    asyncio.run(run())


def test_recovery_waits_until_pending_device_is_online(scenario):
    async def run():
        scenario.keypad.health.update(sync_status="pending", online=False)
        await scenario.tick()
        assert scenario.reconciles == []

        scenario.keypad.health["online"] = True
        await scenario.tick(60)
        assert scenario.reconciles == [(60, "keypad", False)]
        assert scenario.intercom_api.user_reads == 0

    asyncio.run(run())


def test_completed_verification_mismatch_escalates_retry_to_full(monkeypatch):
    scenario = _SyncScenario(monkeypatch, profiles={"HA001": {"name": "User One"}})
    monkeypatch.setattr(integration, "_desired_device_user_payload", lambda *a, **kw: {
        "UserID": "HA001", "Name": "User One",
    })
    scenario.intercom_api.users = [{"UserID": "HA001", "Name": "User One"}]

    async def run():
        await scenario.queue.sync_now("keypad", full=False)
        assert scenario.keypad.health["last_checked"]
        assert scenario.keypad.health["sync_status"] == "pending"
        await scenario.tick(300)
        assert scenario.reconciles == [
            (0, "keypad", False), (300, "keypad", True),
        ]

    try:
        asyncio.run(run())
    finally:
        scenario.close()


def test_manual_sync_default_retains_full_repair_after_confirmed_mismatch(monkeypatch):
    scenario = _SyncScenario(monkeypatch, profiles={"HA001": {"name": "User One"}})
    monkeypatch.setattr(integration, "_desired_device_user_payload", lambda *a, **kw: {
        "UserID": "HA001", "Name": "User One",
    })

    async def reconcile(entry_id, *, full=False):
        scenario.reconciles.append((scenario.seconds, entry_id, full))
        if full:
            scenario.keypad_api.users = [{"UserID": "HA001", "Name": "User One"}]

    scenario.manager.reconcile_device = reconcile

    async def run():
        await scenario.queue.sync_now("keypad", full=False)
        assert scenario.keypad.health["sync_status"] == "pending"
        assert scenario.keypad.health["last_checked"]

        scenario.advance(15)
        # The ordinary Sync button leaves full unspecified. Its corrective
        # work must not drop a full repair requested by the failed comparison.
        await scenario.queue.sync_now("keypad")
        assert scenario.reconciles == [
            (0, "keypad", False), (15, "keypad", True),
        ]
        assert scenario.keypad.health["sync_status"] == "in_sync"

    try:
        asyncio.run(run())
    finally:
        scenario.close()


def test_pending_profile_recovery_does_not_restart_on_every_tick(monkeypatch):
    scenario = _SyncScenario(monkeypatch, profiles={
        "HA001": {"name": "User One", "status": "pending"},
    })
    monkeypatch.setattr(integration, "_desired_device_user_payload", lambda *a, **kw: {
        "UserID": "HA001", "Name": "User One",
    })
    for api in (scenario.keypad_api, scenario.intercom_api):
        api.users = [{"UserID": "HA001", "Name": "User One"}]

    async def run():
        await scenario.tick()
        assert len(scenario.reconciles) == 2
        assert all(coord.health["sync_status"] == "in_sync" for _, coord, *_ in scenario.devices)

        # A stale profile flag must not continuously force otherwise healthy devices.
        for _ in range(19):
            await scenario.tick(15)
        assert len(scenario.reconciles) == 2

    try:
        asyncio.run(run())
    finally:
        scenario.close()
