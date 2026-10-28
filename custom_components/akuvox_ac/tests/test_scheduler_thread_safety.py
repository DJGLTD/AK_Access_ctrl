"""Dispatch timers like HA: only coroutine and marked jobs stay on its loop."""

import asyncio
import inspect
import threading
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from custom_components.akuvox_ac import integration
from .test_initial_integrity_check import _Api, _coordinator, _manager


class _Scheduler:
    def __init__(self, monkeypatch):
        self.loop = asyncio.get_running_loop()
        self.thread_id = threading.get_ident()
        self.jobs = []
        self.tasks = []
        self.violations = []
        self.executor_dispatches = 0
        self.reconciled = []
        self.saved_times = []
        self.devices = [
            (name, _coordinator(), _Api(), {}) for name in ("keypad", "intercom")
        ]
        self.manager = _manager(self.devices)
        self.hass = self.manager.hass
        self.hass.loop = self.loop
        self.hass.async_create_task = self.create_task
        self.hass.bus = SimpleNamespace(
            async_listen_once=lambda event, action: self.register("startup", action)
        )
        monkeypatch.setattr(integration, "async_track_time_interval", lambda hass, action, interval:
                            self.register("interval", action))
        monkeypatch.setattr(integration, "async_track_time_change", lambda hass, action, **kwargs:
                            self.register("daily", action))
        monkeypatch.setattr(integration, "async_call_later", lambda hass, delay, action:
                            self.register("delayed", action))

        async def save_time(value):
            self.assert_loop("save settings")
            self.saved_times.append(value)

        self.settings = SimpleNamespace(
            set_auto_sync_time=save_time,
            get_auto_sync_delay_minutes=lambda: 5,
        )
        self.manager._settings_store = lambda: self.settings
        self.manager._auto_unsub = None
        self.manager._root()["settings_store"] = self.settings
        self.manager._root()["users_store"] = self.manager._users_store()
        self.queue = integration.SyncQueue(self.hass)
        self.manager._root()["sync_queue"] = self.queue

        # Catch state mutation before a callback reaches its thread-safe task
        # bridge. Some production callers swallow errors, so also retain them.
        for name in ("mark_change", "ensure_future_run", "_schedule_run",
                     "_set_health_status", "_invalidate_scheduled_run"):
            original = getattr(self.queue, name)

            def guarded(*args, _name=name, _original=original, **kwargs):
                self.assert_loop(_name)
                return _original(*args, **kwargs)

            setattr(self.queue, name, guarded)

        async def reconcile(entry_id, *, full):
            self.assert_loop("reconcile")
            self.reconciled.append(entry_id)
            await asyncio.sleep(0)

        self.manager.reconcile_device = reconcile

    def assert_loop(self, operation):
        if threading.get_ident() != self.thread_id:
            self.violations.append(operation)
            raise AssertionError(f"{operation} executed outside the HA loop")
        assert asyncio.get_running_loop() is self.loop

    def create_task(self, coro):
        try:
            self.assert_loop("async_create_task")
        except AssertionError:
            coro.close()
            raise
        task = self.loop.create_task(coro)
        self.tasks.append(task)
        return task

    def register(self, kind, action):
        self.assert_loop("register timer")
        job = {"kind": kind, "action": action, "cancelled": False, "cancel_count": 0}
        self.jobs.append(job)

        def cancel():
            self.assert_loop("cancel timer")
            job["cancelled"] = True
            job["cancel_count"] += 1

        return cancel

    def latest(self, kind):
        return next(job for job in reversed(self.jobs) if job["kind"] == kind)

    async def dispatch(self, job):
        if job["cancelled"]:
            return
        action = job["action"]
        if inspect.iscoroutinefunction(action):
            await action(None)
        elif getattr(action, "_hass_callback", False):
            action(None)
        else:
            self.executor_dispatches += 1
            await asyncio.to_thread(action, None)

    async def settle(self):
        # Flush call_soon_threadsafe -> async_create_task -> queued_run, retaining
        # completed tasks as well so their exceptions cannot escape the test.
        for _ in range(4):
            await asyncio.sleep(0)
            if self.tasks:
                await asyncio.gather(*self.tasks)
        assert not self.violations

    def shutdown(self):
        self.manager.shutdown()
        self.queue.shutdown()


@pytest.mark.asyncio
async def test_daily_sync_runs_once_on_loop_and_includes_every_device(monkeypatch):
    scheduler = _Scheduler(monkeypatch)
    scheduler.queue._pending_devices = {"keypad"}
    scheduler.queue.sync_now = AsyncMock(wraps=scheduler.queue.sync_now)
    scheduler.manager.set_auto_sync_time("05:00")
    await scheduler.settle()

    await scheduler.dispatch(scheduler.latest("daily"))
    await scheduler.settle()

    scheduler.queue.sync_now.assert_awaited_once_with(
        None, include_all=True, trigger="scheduled auto sync"
    )
    assert scheduler.reconciled == ["keypad", "intercom"]
    assert scheduler.saved_times == ["05:00"]
    assert scheduler.executor_dispatches == 0
    assert all(coord.health["last_checked"] for _, coord, *_ in scheduler.devices)
    scheduler.shutdown()


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["interval", "startup", "delayed", "rescheduled"])
async def test_queue_callbacks_keep_all_queue_mutations_on_loop(monkeypatch, kind):
    scheduler = _Scheduler(monkeypatch)
    if kind in {"delayed", "rescheduled"}:
        scheduler.queue.mark_change("keypad")
        if kind == "rescheduled":
            previous = scheduler.latest("delayed")
            scheduler.queue.refresh_default_delay()
            assert previous["cancelled"]
        job = scheduler.latest("delayed")
    else:
        scheduler.devices[0][1].health["sync_status"] = "pending"
        job = scheduler.latest(kind)

    await scheduler.dispatch(job)
    await scheduler.settle()

    assert scheduler.reconciled == ["keypad"]
    assert scheduler.executor_dispatches == 0
    scheduler.shutdown()


@pytest.mark.asyncio
async def test_real_worker_can_still_schedule_coroutine_safely(monkeypatch):
    scheduler = _Scheduler(monkeypatch)
    ran = []

    async def work():
        scheduler.assert_loop("worker-scheduled coroutine")
        ran.append(threading.get_ident())

    def from_worker():
        assert threading.get_ident() != scheduler.thread_id
        scheduler.queue._schedule_task(work())

    await asyncio.to_thread(from_worker)
    await scheduler.settle()

    assert ran == [scheduler.thread_id]
    scheduler.shutdown()


@pytest.mark.asyncio
async def test_daily_reconfiguration_and_shutdown_cancel_previous_jobs(monkeypatch):
    scheduler = _Scheduler(monkeypatch)
    scheduler.manager.set_auto_sync_time("05:00")
    previous = scheduler.latest("daily")
    scheduler.manager.set_auto_sync_time("06:00")
    current = scheduler.latest("daily")
    await scheduler.settle()

    assert previous["cancel_count"] == 1
    await scheduler.dispatch(previous)
    assert scheduler.reconciled == []
    await scheduler.dispatch(current)
    await scheduler.settle()
    assert scheduler.reconciled == ["keypad", "intercom"]

    scheduler.shutdown()
    assert current["cancel_count"] == 1
    for job in scheduler.jobs:
        assert job["cancelled"]
        await scheduler.dispatch(job)
    await scheduler.settle()
    assert scheduler.reconciled == ["keypad", "intercom"]


@pytest.mark.asyncio
async def test_daily_callback_after_queue_removal_is_harmless(monkeypatch):
    scheduler = _Scheduler(monkeypatch)
    scheduler.manager.set_auto_sync_time("05:00")
    await scheduler.settle()
    scheduler.manager._root().pop("sync_queue")

    await scheduler.dispatch(scheduler.latest("daily"))
    await scheduler.settle()

    assert scheduler.reconciled == []
    scheduler.shutdown()


@pytest.mark.asyncio
async def test_shutdown_invalidates_immediate_work_before_it_starts(monkeypatch):
    scheduler = _Scheduler(monkeypatch)
    scheduler.queue.mark_change("keypad", delay_minutes=0)
    scheduler.shutdown()

    await scheduler.settle()

    assert scheduler.reconciled == []
