"""Keep routine notification reads small without losing short access bursts."""

from unittest.mock import AsyncMock, Mock, call

import pytest

from custom_components.akuvox_ac.api import AkuvoxAPI
from .test_notification_latency import make_coordinator, start_polling


def events(count):
    return [{"ID": str(n), "Event": "Access granted"} for n in range(count, 0, -1)]


def payload(items):
    return {"retcode": 0, "data": {"num": 1003, "curPageNum": len(items), "item": items}}


@pytest.mark.asyncio
async def test_api_requests_paging_on_device_instead_of_slicing_full_response():
    api = AkuvoxAPI("example.invalid")
    page = events(10)
    full = events(1003)
    api._get_api = AsyncMock(side_effect=[payload(page), payload(full)])
    assert await api.events_last(recent_only=True) == page
    assert await api.events_last() == full
    assert api._get_api.call_args_list == [call("/api/doorlog/get/?page=1"), call("/api/doorlog/get/")]


@pytest.mark.asyncio
@pytest.mark.parametrize("rejection", [RuntimeError("unsupported page"), {"retcode": -1, "message": "unsupported page"}])
async def test_rejected_paging_falls_back_without_repeated_failed_probes(rejection):
    api = AkuvoxAPI("example.invalid")
    full = events(1003)
    api._get_api = AsyncMock(side_effect=[rejection, payload(full), payload(full)])
    assert await api.events_last(recent_only=True) == full
    assert await api.events_last(recent_only=True) == full
    assert api._get_api.call_args_list == [call("/api/doorlog/get/?page=1"), call("/api/doorlog/get/"), call("/api/doorlog/get/")]


@pytest.mark.asyncio
async def test_firmware_ignoring_page_is_detected_without_an_extra_download():
    api = AkuvoxAPI("example.invalid")
    full = events(1003)
    api._get_api = AsyncMock(return_value=payload(full))
    assert await api.events_last(recent_only=True) == full
    api._get_api.assert_awaited_once()
    await api.events_last(recent_only=True)
    api._get_api.assert_awaited_with("/api/doorlog/get/")


@pytest.mark.asyncio
async def test_failed_paged_and_full_reads_do_not_disable_paging_permanently():
    api = AkuvoxAPI("example.invalid")
    api._get_api = AsyncMock(side_effect=[RuntimeError("offline"), RuntimeError("offline"), payload(events(10))])
    assert await api.events_last(recent_only=True) == []
    assert await api.events_last(recent_only=True) == events(10)
    api._get_api.assert_awaited_with("/api/doorlog/get/?page=1")


@pytest.mark.asyncio
async def test_empty_log_is_valid_without_fallback():
    api = AkuvoxAPI("example.invalid")
    api._get_api = AsyncMock(return_value=payload([]))
    assert await api.events_last(recent_only=True) == []
    api._get_api.assert_awaited_once()


@pytest.mark.asyncio
async def test_timer_reads_one_page_and_notifies_for_two_new_events(monkeypatch):
    coord = make_coordinator()
    start_polling(coord, monkeypatch)
    coord.storage.data["door_events"]["last_event_key"] = "8"
    coord.api.events_last.return_value = events(10)
    coord._handle_door_event = AsyncMock(return_value=True)
    coord._publish_access_history = Mock()
    try:
        await coord._async_poll_events(None)
        coord.api.events_last.assert_awaited_once_with(recent_only=True)
        assert [c.args[0]["ID"] for c in coord._handle_door_event.call_args_list] == ["9", "10"]
        coord._handle_door_event.reset_mock()
        await coord._async_poll_events(None)
        coord._handle_door_event.assert_not_awaited()
    finally:
        coord.shutdown()


@pytest.mark.asyncio
async def test_page_gap_triggers_catch_up_for_events_beyond_first_ten():
    coord = make_coordinator()
    full = events(1003)
    coord.storage.data["door_events"]["last_event_key"] = "991"
    coord.api.events_last.side_effect = [full[:10], full]
    coord._handle_door_event = AsyncMock(return_value=True)
    coord._publish_access_history = Mock()
    await coord.async_refresh_access_history(recent_only=True)
    assert coord.api.events_last.call_args_list == [call(recent_only=True), call()]
    assert [c.args[0]["ID"] for c in coord._handle_door_event.call_args_list] == [str(n) for n in range(992, 1004)]
    assert coord.storage.data["door_events"]["last_event_key"] == "1003"
    assert len(coord._publish_access_history.call_args.args[0]) == 25


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [[], RuntimeError("offline")])
async def test_failed_catch_up_does_not_advance_cursor_or_lose_pending_events(failure):
    coord = make_coordinator()
    coord.storage.data["door_events"]["last_event_key"] = "991"
    coord.api.events_last.side_effect = [events(1003)[:10], failure]
    coord._handle_door_event = AsyncMock(return_value=True)
    assert await coord.async_refresh_access_history(recent_only=True) == []
    coord._handle_door_event.assert_not_awaited()
    assert coord.storage.data["door_events"]["last_event_key"] == "991"


@pytest.mark.asyncio
async def test_unsupported_firmware_has_bounded_local_processing():
    coord = make_coordinator()
    coord.api.events_last.return_value = events(1003)
    coord._handle_door_event = AsyncMock(return_value=True)
    coord._publish_access_history = Mock()
    await coord.async_refresh_access_history(recent_only=True)
    assert coord._handle_door_event.await_count == 25
    assert len(coord._publish_access_history.call_args.args[0]) == 25


@pytest.mark.asyncio
async def test_explicit_history_refresh_keeps_full_history():
    coord = make_coordinator()
    coord.api.events_last.return_value = events(1003)
    coord._handle_door_event = AsyncMock(return_value=True)
    coord._publish_access_history = Mock()
    result = await coord.async_refresh_access_history()
    coord.api.events_last.assert_awaited_once_with()
    assert len(result) == 1003
    assert len(coord._publish_access_history.call_args.args[0]) == 1003


@pytest.mark.asyncio
async def test_distinct_access_events_in_same_second_are_processed_once():
    coord = make_coordinator()
    timestamp = "2026-09-25T10:00:00+00:00"
    epoch = coord._coerce_event_timestamp_to_epoch(timestamp)
    coord.storage.data["door_events"].update(last_event_key="1", last_event_epoch=epoch)
    coord.api.events_last.return_value = [{**e, "timestamp": timestamp} for e in events(3)]
    coord._handle_door_event = AsyncMock(return_value=True)
    await coord.async_refresh_access_history(recent_only=True)
    assert [c.args[0]["ID"] for c in coord._handle_door_event.call_args_list] == ["2", "3"]
    coord._handle_door_event.reset_mock()
    await coord.async_refresh_access_history(recent_only=True)
    coord._handle_door_event.assert_not_awaited()
