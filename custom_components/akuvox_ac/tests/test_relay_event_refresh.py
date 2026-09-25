"""Relay webhooks can arrive before the device publishes its door-log row."""

import asyncio
import datetime as dt
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from custom_components.akuvox_ac import coordinator as module
from custom_components.akuvox_ac import button as button_module
from custom_components.akuvox_ac.const import DOMAIN
from .test_notification_latency import make_coordinator


def relay_coordinator(monkeypatch):
    coord = make_coordinator()
    coord.hass.async_create_task = asyncio.create_task
    coord.hass.data[DOMAIN] = {
        "settings_store": SimpleNamespace(targets_for_event=lambda *a, **kw: ["phone"]),
    }
    coord._publish_access_history = Mock()
    # Stop the real retry clock in tests; the attempt limit is still exercised.
    monkeypatch.setattr(module, "RELAY_EVENT_RETRY_INTERVAL_SECONDS", 0)
    now = dt.datetime.now(dt.timezone.utc)
    old = {"ID": "1007", "Type": "DTMF", "Status": "Succ", "Name": "Previous caller",
           "timestamp": (now - dt.timedelta(hours=1)).isoformat()}
    new = {"ID": "1008", "Type": "DTMF", "Status": "Succ", "Name": "Current caller",
           "timestamp": now.replace(microsecond=0).isoformat()}
    coord.storage.data["door_events"].update(
        last_event_key="1007", last_event_epoch=(now - dt.timedelta(hours=1)).timestamp()
    )
    return coord, old, new


@pytest.mark.asyncio
async def test_relay_retries_stale_log_and_notifies_real_caller_once(monkeypatch):
    coord, old, new = relay_coordinator(monkeypatch)
    coord.api.events_last.side_effect = [[old], [old], [new, old], [new, old]]
    try:
        await coord.async_refresh_after_access_permitted()
        assert coord.api.events_last.await_count == 3
        assert all(c.kwargs == {"recent_only": True} for c in coord.api.events_last.call_args_list)
        coord.hass.services.async_call.assert_awaited_once()
        message = coord.hass.services.async_call.call_args.args[2]["message"]
        assert message == "Current caller opened the gate via Call."
        await coord.async_refresh_access_history(recent_only=True)
        coord.hass.services.async_call.assert_awaited_once()
    finally:
        coord.shutdown()


@pytest.mark.asyncio
async def test_already_polled_event_is_not_replayed_by_later_webhook(monkeypatch):
    coord, old, new = relay_coordinator(monkeypatch)
    coord.api.events_last.return_value = [new, old]
    try:
        await coord.async_refresh_access_history(recent_only=True)
        await coord.async_refresh_after_access_permitted()
        coord.hass.services.async_call.assert_awaited_once()
        assert coord.api.events_last.await_count == 2
    finally:
        coord.shutdown()


@pytest.mark.asyncio
async def test_slow_read_does_not_start_more_retries_after_deadline(monkeypatch):
    coord, old, new = relay_coordinator(monkeypatch)
    monotonic = 0
    monkeypatch.setattr(module, "time", SimpleNamespace(
        monotonic=lambda: monotonic, time=lambda: dt.datetime.now(dt.timezone.utc).timestamp()
    ))

    async def slow_read(**kwargs):
        nonlocal monotonic
        monotonic = module.RELAY_EVENT_REFRESH_WINDOW_SECONDS + 1
        return [old]

    coord.api.events_last.side_effect = slow_read
    try:
        await coord.async_refresh_after_access_permitted()
        coord.api.events_last.assert_awaited_once()
        coord.hass.services.async_call.assert_not_awaited()
    finally:
        coord.shutdown()


@pytest.mark.asyncio
async def test_empty_read_is_retried_without_sending_placeholder_notification(monkeypatch):
    coord, old, new = relay_coordinator(monkeypatch)
    coord.api.events_last.side_effect = [[], [new, old]]
    try:
        await coord.async_refresh_after_access_permitted()
        assert coord.api.events_last.await_count == 2
        coord.hass.services.async_call.assert_awaited_once()
        assert coord.storage.data["door_events"]["last_event_key"] == "1008"
    finally:
        coord.shutdown()


@pytest.mark.asyncio
async def test_relay_timeout_keeps_cursor_for_normal_poll(monkeypatch):
    coord, old, new = relay_coordinator(monkeypatch)
    coord.api.events_last.return_value = [old]
    try:
        await coord.async_refresh_after_access_permitted()
        assert coord.api.events_last.await_count == module.RELAY_EVENT_REFRESH_ATTEMPTS
        coord.hass.services.async_call.assert_not_awaited()
        assert coord.storage.data["door_events"]["last_event_key"] == "1007"
        coord.api.events_last.return_value = [new, old]
        await coord.async_refresh_access_history(recent_only=True)
        coord.hass.services.async_call.assert_awaited_once()
    finally:
        coord.shutdown()


@pytest.mark.asyncio
async def test_poll_and_overlapping_webhooks_do_not_duplicate_notifications(monkeypatch):
    coord, old, new = relay_coordinator(monkeypatch)
    entered, release = asyncio.Event(), asyncio.Event()

    async def read_events(**kwargs):
        entered.set()
        await release.wait()
        return [new, old]

    coord.api.events_last.side_effect = read_events
    first = asyncio.create_task(coord.async_refresh_after_access_permitted())
    await entered.wait()
    second = asyncio.create_task(coord.async_refresh_after_access_permitted())
    poll = asyncio.create_task(coord.async_refresh_access_history(recent_only=True))
    try:
        release.set()
        await asyncio.gather(first, second, poll)
        assert coord.api.events_last.await_count == 2  # One burst plus one normal poll.
        coord.hass.services.async_call.assert_awaited_once()
    finally:
        coord.shutdown()


@pytest.mark.asyncio
async def test_shutdown_cancels_pending_relay_read(monkeypatch):
    coord, old, new = relay_coordinator(monkeypatch)
    entered = asyncio.Event()

    async def read_events(**kwargs):
        entered.set()
        await asyncio.Event().wait()

    coord.api.events_last.side_effect = read_events
    task = asyncio.create_task(coord.async_refresh_after_access_permitted())
    await entered.wait()
    coord.shutdown()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert not coord._door_events_lock.locked()
    coord.hass.services.async_call.assert_not_awaited()


@pytest.mark.asyncio
async def test_access_button_uses_relay_refresh_instead_of_suppressing_new_event(monkeypatch):
    coord = SimpleNamespace(
        device_name="Gate", health={},
        async_refresh_after_access_permitted=AsyncMock(),
        async_handle_manual_event=AsyncMock(),
        async_refresh_access_history=AsyncMock(),
    )
    button = button_module.AkuvoxAccessPermittedButton(coord, SimpleNamespace(entry_id="gate"))
    button.hass = SimpleNamespace()
    ingest = Mock()
    monkeypatch.setattr(button_module, "_ingest_history_event", ingest)
    await button.async_press()
    ingest.assert_called_once()
    coord.async_refresh_after_access_permitted.assert_awaited_once()
    coord.async_handle_manual_event.assert_not_awaited()
    coord.async_refresh_access_history.assert_not_awaited()
