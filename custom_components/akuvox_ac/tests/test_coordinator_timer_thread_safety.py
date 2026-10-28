"""Dispatch coordinator timers like HA: unmarked sync jobs use the executor."""

import asyncio
import inspect
import threading
from types import SimpleNamespace

import pytest

from custom_components.akuvox_ac import coordinator as module


class _Timer:
    def __init__(self, delay, action, *, repeating=False):
        self.delay = delay
        self.action = action
        self.repeating = repeating
        self.cancelled = False
        self.calls = 0

    def cancel(self):
        self.cancelled = True

    async def fire(self):
        if self.cancelled:
            return
        self.calls += 1
        if not self.repeating:
            self.cancelled = True
        # This is the important distinction a direct callback() test misses:
        # HA runs an ordinary synchronous job on a worker, not its event loop.
        if inspect.iscoroutinefunction(self.action):
            await self.action(None)
        elif getattr(self.action, "_hass_callback", False):
            self.action(None)
        else:
            await asyncio.to_thread(self.action, None)


class _LoopOnlyState(dict):
    def __init__(self, value, assert_loop):
        super().__init__(value)
        self.assert_loop = assert_loop

    def __setitem__(self, key, value):
        self.assert_loop()
        super().__setitem__(key, value)


class _Scenario:
    def __init__(self, monkeypatch):
        self.loop_thread = threading.get_ident()
        self.loop = asyncio.get_running_loop()
        self.timers = []
        self.updates = []
        self.entity_states = []
        self.coord = module.AkuvoxCoordinator(
            SimpleNamespace(data={}, loop=self.loop),
            SimpleNamespace(),
            SimpleNamespace(data={}),
            "device-1",
            "Front gate",
        )
        self.coord.event_state = _LoopOnlyState(self.coord.event_state, self.assert_loop)
        self.coord.async_update_listeners = self.update_listeners
        monkeypatch.setattr(module, "async_call_later", self.call_later)
        monkeypatch.setattr(module, "async_track_time_interval", self.track_interval)

    def assert_loop(self):
        assert threading.get_ident() == self.loop_thread, "State update ran off the HA thread"
        assert asyncio.get_running_loop() is self.loop

    def update_listeners(self):
        self.assert_loop()
        state = (dict(self.coord.event_state), dict(self.coord.caller_state))
        self.updates.append(state)
        self.write_entity_state(state)

    def write_entity_state(self, state):
        # Model the entity state write triggered by coordinator listeners;
        # HA also rejects async state-machine operations from an executor.
        self.assert_loop()
        self.entity_states.append(state)

    def call_later(self, hass, delay, action):
        self.assert_loop()
        timer = _Timer(delay, action)
        self.timers.append(timer)
        return timer.cancel

    def track_interval(self, hass, action, interval):
        self.assert_loop()
        timer = _Timer(interval.total_seconds(), action, repeating=True)
        self.timers.append(timer)
        return timer.cancel


@pytest.mark.asyncio
@pytest.mark.parametrize("flag", ["granted_active", "granted_key_holder_active", "denied_active"])
async def test_event_flag_timer_updates_entities_on_home_assistant_loop(monkeypatch, flag):
    scenario = _Scenario(monkeypatch)
    coord = scenario.coord

    assert coord._activate_event_flag(flag)
    timer = scenario.timers[-1]
    assert timer.delay == 3
    assert coord.event_state[flag]

    await timer.fire()

    assert not coord.event_state[flag]
    assert flag not in coord._event_reset_handles
    assert len(scenario.updates) == len(scenario.entity_states) == 1
    coord.shutdown()


@pytest.mark.asyncio
async def test_repeated_event_replaces_reset_timer_without_clearing_new_state(monkeypatch):
    scenario = _Scenario(monkeypatch)
    coord = scenario.coord
    coord._activate_event_flag("granted_active")
    original = scenario.timers[-1]
    assert not coord._activate_event_flag("granted_active")
    replacement = scenario.timers[-1]

    assert original.cancelled
    await original.fire()
    assert coord.event_state["granted_active"]
    assert scenario.updates == []

    await replacement.fire()
    assert not coord.event_state["granted_active"]
    assert len(scenario.updates) == 1
    coord.shutdown()


@pytest.mark.asyncio
async def test_deactivated_event_cancels_its_pending_reset(monkeypatch):
    scenario = _Scenario(monkeypatch)
    coord = scenario.coord
    coord._activate_event_flag("granted_key_holder_active")
    timer = scenario.timers[-1]

    assert coord._deactivate_event_flag("granted_key_holder_active")
    assert timer.cancelled
    await timer.fire()

    assert not coord.event_state["granted_key_holder_active"]
    assert scenario.updates == []
    assert coord._event_reset_handles == {}
    coord.shutdown()


@pytest.mark.asyncio
async def test_caller_clear_timer_updates_entities_on_home_assistant_loop(monkeypatch):
    scenario = _Scenario(monkeypatch)
    coord = scenario.coord
    coord._set_caller_state({"caller_name": "Caller one", "call_id": "1"}, auto_clear=True)
    timer = scenario.timers[-1]
    assert timer.delay == module.CALLER_CLEAR_DELAY_SECONDS

    await timer.fire()

    assert coord.caller_state == coord._empty_caller_state()
    assert coord._caller_reset_handle is None
    assert len(scenario.updates) == len(scenario.entity_states) == 2
    coord.shutdown()


@pytest.mark.asyncio
async def test_new_caller_replaces_clear_timer_without_clearing_new_caller(monkeypatch):
    scenario = _Scenario(monkeypatch)
    coord = scenario.coord
    coord._set_caller_state({"caller_name": "Caller one", "call_id": "1"}, auto_clear=True)
    original = scenario.timers[-1]
    latest = {"caller_name": "Caller two", "call_id": "2"}
    coord._set_caller_state(latest, auto_clear=True)
    replacement = scenario.timers[-1]

    assert original.cancelled
    await original.fire()
    assert coord.caller_state == latest
    assert len(scenario.updates) == 2

    await replacement.fire()
    assert coord.caller_state == coord._empty_caller_state()
    assert len(scenario.updates) == 3
    coord.shutdown()


@pytest.mark.asyncio
async def test_shutdown_cancels_flag_caller_and_poll_timers(monkeypatch):
    scenario = _Scenario(monkeypatch)
    coord = scenario.coord
    coord._activate_event_flag("granted_active")
    coord._activate_event_flag("denied_active")
    coord._set_caller_state({"caller_name": "Caller one"}, auto_clear=True)
    coord.start_event_polling()
    assert len(scenario.timers) == 4
    updates_before_shutdown = len(scenario.updates)

    coord.shutdown()
    coord.shutdown()
    for timer in scenario.timers:
        assert timer.cancelled
        await timer.fire()

    assert len(scenario.updates) == updates_before_shutdown
    assert coord._event_reset_handles == {}
    assert coord._caller_reset_handle is None
    assert coord._event_poll_unsub is None


@pytest.mark.asyncio
async def test_poll_timer_coroutine_runs_on_home_assistant_loop(monkeypatch):
    scenario = _Scenario(monkeypatch)
    coord = scenario.coord
    coord.health.update(online=True, status="online")
    refreshed = []

    async def refresh_history(*, recent_only=False):
        scenario.assert_loop()
        refreshed.append(recent_only)

    coord.async_refresh_access_history = refresh_history
    coord.start_event_polling()
    timer = scenario.timers[-1]
    assert timer.delay == module.ACCESS_EVENT_POLL_INTERVAL_SECONDS

    await timer.fire()

    assert refreshed == [True]
    assert coord._event_poll_task is None
    coord.shutdown()
