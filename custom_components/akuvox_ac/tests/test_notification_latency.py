"""Exercise ordering, overlap and shutdown without relying on wall-clock sleeps."""

import asyncio
from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from custom_components.akuvox_ac import coordinator as module
from custom_components.akuvox_ac.const import DOMAIN


def make_coordinator():
    storage = SimpleNamespace(data={}, async_save=AsyncMock())
    hass = SimpleNamespace(data={}, services=SimpleNamespace(async_call=AsyncMock()))
    api = SimpleNamespace(
        ping_info=AsyncMock(return_value={"ok": True}),
        user_list=AsyncMock(return_value=[]),
        events_last=AsyncMock(return_value=[]),
    )
    coord = module.AkuvoxCoordinator(hass, api, storage, "door-1", "Front gate")
    coord.async_update_listeners = Mock()
    coord.health.update(online=True, status="online")
    coord._was_online = True
    return coord


def start_polling(coord, monkeypatch):
    cancel = Mock()
    schedule = Mock(return_value=cancel)
    monkeypatch.setattr(module, "async_track_time_interval", schedule)
    coord.start_event_polling()
    coord.start_event_polling()
    schedule.assert_called_once_with(coord.hass, coord._async_poll_events, timedelta(seconds=5))
    return cancel


@pytest.mark.asyncio
async def test_startup_notifies_before_fetching_users():
    coord = make_coordinator()
    coord.hass.data[DOMAIN] = {
        "settings_store": SimpleNamespace(targets_for_event=lambda *a, **kw: ["phone"]),
    }
    coord.api.events_last.return_value = [{"ID": "1", "Event": "Access granted", "UserID": "HA001"}]

    async def fetch_users():
        assert coord.hass.services.async_call.await_count == 1
        return []

    coord.api.user_list.side_effect = fetch_users
    await coord._async_update_data()
    coord.api.user_list.assert_awaited_once()


@pytest.mark.asyncio
async def test_event_timer_runs_while_user_refresh_is_blocked(monkeypatch):
    coord = make_coordinator()
    start_polling(coord, monkeypatch)
    entered = asyncio.Event()
    release = asyncio.Event()

    async def slow_users():
        entered.set()
        await release.wait()
        return []

    coord.api.user_list.side_effect = slow_users
    health_task = asyncio.create_task(coord._async_update_data())
    try:
        await asyncio.wait_for(entered.wait(), 1)
        await asyncio.wait_for(coord._async_poll_events(None), 1)
        coord.api.events_last.assert_awaited_once()
        assert not health_task.done()
    finally:
        release.set()
        await health_task
        coord.shutdown()


@pytest.mark.asyncio
async def test_overlapping_refreshes_notify_once():
    coord = make_coordinator()
    coord.api.events_last.return_value = [{"ID": "1", "Event": "Access granted"}]
    entered = asyncio.Event()
    release = asyncio.Event()
    calls = []

    async def handle(event, targets):
        calls.append(event)
        entered.set()
        await release.wait()
        return True

    coord._handle_door_event = handle
    first = asyncio.create_task(coord.async_refresh_access_history())
    await asyncio.wait_for(entered.wait(), 1)
    second = asyncio.create_task(coord.async_refresh_access_history())
    release.set()
    await asyncio.wait_for(asyncio.gather(first, second), 1)
    assert len(calls) == 1
    assert coord.storage.data["door_events"]["last_event_key"]


@pytest.mark.asyncio
async def test_busy_timer_does_not_queue_more_requests_and_shutdown_cancels(monkeypatch):
    coord = make_coordinator()
    cancel = start_polling(coord, monkeypatch)
    entered = asyncio.Event()

    async def slow_events(**_kwargs):
        entered.set()
        await asyncio.Event().wait()

    coord.api.events_last.side_effect = slow_events
    first = asyncio.create_task(coord._async_poll_events(None))
    await asyncio.wait_for(entered.wait(), 1)
    await asyncio.wait_for(coord._async_poll_events(None), 1)
    coord.api.events_last.assert_awaited_once()
    coord.shutdown()
    with pytest.raises(asyncio.CancelledError):
        await first
    assert not coord._door_events_lock.locked()
    assert coord._event_poll_task is None
    coord.shutdown()
    cancel.assert_called_once()
    await coord._async_poll_events(None)
    coord.api.events_last.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("online,status", [(False, "offline"), (False, "rebooting"), (True, "rebooting")])
async def test_offline_or_rebooting_devices_skip_fast_polling(monkeypatch, online, status):
    coord = make_coordinator()
    start_polling(coord, monkeypatch)
    coord.health.update(online=online, status=status)
    await coord._async_poll_events(None)
    coord.api.events_last.assert_not_awaited()
    coord.shutdown()


@pytest.mark.asyncio
async def test_event_timer_recovers_after_fetch_error(monkeypatch):
    coord = make_coordinator()
    start_polling(coord, monkeypatch)
    coord.api.events_last.side_effect = [RuntimeError("network unavailable"), []]
    await coord._async_poll_events(None)
    await coord._async_poll_events(None)
    assert coord.api.events_last.await_count == 2
    assert not coord._door_events_lock.locked()
    coord.shutdown()
