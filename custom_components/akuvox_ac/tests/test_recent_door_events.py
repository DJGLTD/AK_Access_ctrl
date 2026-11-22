"""Keep routine notification reads small without losing short access bursts."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, call

import pytest

from custom_components.akuvox_ac.api import AkuvoxAPI
from custom_components.akuvox_ac.access_history import AccessHistory
from custom_components.akuvox_ac.const import DOMAIN
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
async def test_doorlog_falls_back_to_web_route_when_standard_route_is_forbidden():
    api = AkuvoxAPI("example.invalid")
    full = events(31)
    api._get_api = AsyncMock(side_effect=RuntimeError("HTTP 403"))
    api._get_web_api = AsyncMock(return_value=payload(full))

    assert await api.events_last() == full
    api._get_api.assert_awaited_once_with("/api/doorlog/get/")
    api._get_web_api.assert_awaited_once_with("/api/web/doorlog/get/")


@pytest.mark.asyncio
async def test_doorlog_reauthenticates_once_when_web_session_has_expired():
    api = AkuvoxAPI("example.invalid")
    full = events(31)
    api._get_api = AsyncMock(side_effect=RuntimeError("HTTP 403"))
    api._web_token_cookie = "token=stale-test-cookie"
    api._get_web_api = AsyncMock(
        side_effect=[
            {"retcode": -100, "message": "session expired"},
            payload(full),
        ]
    )

    assert await api.events_last() == full
    assert api._web_token_cookie is None
    assert api._get_web_api.await_count == 2


@pytest.mark.asyncio
async def test_web_doorlog_request_uses_session_cookie_over_configured_https():
    class Response:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        async def json(self, content_type=None):
            return payload(events(1))

        def raise_for_status(self):
            return None

    session = SimpleNamespace(get=Mock(return_value=Response()))
    api = AkuvoxAPI(
        "example.invalid",
        use_https=True,
        verify_ssl=True,
        session=session,
    )
    api._detected = (True, 443, True)
    api._web_login_cookie = AsyncMock(return_value="token=unit-test")

    result = await api._get_web_api("/api/web/doorlog/get/")

    assert result == payload(events(1))
    api._web_login_cookie.assert_awaited_once_with(
        use_https=True,
        port=443,
        verify=True,
    )
    url, = session.get.call_args.args
    request = session.get.call_args.kwargs
    assert url == "https://example.invalid:443/api/web/doorlog/get/"
    assert request["headers"]["Cookie"] == "token=unit-test"
    assert request["ssl"] is True
    assert "auth" not in request


@pytest.mark.asyncio
async def test_call_log_falls_back_to_authenticated_web_endpoint():
    api = AkuvoxAPI("example.invalid")
    call_items = [{"ID": "call-1", "Type": "received", "DateTime": "2026-11-20T10:00:00"}]
    api._post_api = AsyncMock(side_effect=RuntimeError("standard call log unavailable"))
    api._get_api = AsyncMock(side_effect=RuntimeError("HTTP 403"))
    api._get_web_api = AsyncMock(return_value={"retcode": 0, "data": {"item": call_items}})

    assert await api.call_log() == call_items
    api._get_web_api.assert_awaited_once_with("/api/web/calllog/get/")


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
async def test_explicit_refresh_restores_device_events_older_than_minimum_retention():
    coord = make_coordinator()
    day = 24 * 60 * 60
    timestamps = [
        f"2026-08-{21 + index:02d}T10:00:00+00:00" for index in range(10)
    ] + [
        "2026-11-02T10:00:00+00:00",
        "2026-11-06T10:00:00+00:00",
        "2026-11-09T10:00:00+00:00",
        "2026-11-20T10:00:00+00:00",
    ]
    full_log = [
        {"ID": str(index), "Event": "Access granted", "UserID": "HA001", "DateTime": ts}
        for index, ts in reversed(list(enumerate(timestamps, start=1)))
    ]
    coord.storage.data["door_events"] = {"last_event_key": str(len(full_log))}
    coord.api.events_last.return_value = full_log
    coord.hass.data[DOMAIN] = {
        "access_history": AccessHistory(),
        "settings_store": type(
            "Settings",
            (),
            {
                "get_access_history_limit": lambda self: 5000,
                "get_access_history_storage_limit": lambda self: 5000,
                "get_access_history_retention_seconds": lambda self: 30 * day,
            },
        )(),
    }

    await coord.async_refresh_access_history()

    history = coord.hass.data[DOMAIN]["access_history"].snapshot(5000)
    assert len(history) == len(full_log)
    assert {event["DateTime"][:10] for event in history} == {
        timestamp[:10] for timestamp in timestamps
    }


@pytest.mark.asyncio
async def test_full_refresh_imports_access_and_call_history_separately():
    coord = make_coordinator()
    history = AccessHistory()
    coord.hass.data[DOMAIN] = {
        "access_history": history,
        "settings_store": SimpleNamespace(
            get_access_history_limit=lambda: 5000,
            get_access_history_storage_limit=lambda: 5000,
            get_access_history_retention_seconds=lambda: 30 * 24 * 60 * 60,
        ),
    }
    coord.storage.data["door_events"] = {"last_event_key": "door-1"}
    coord.api.events_last.return_value = [
        {
            "ID": "door-1",
            "Event": "Access granted",
            "UserID": "HA001",
            "DateTime": "2026-11-20T10:00:00+00:00",
        }
    ]
    coord.api.call_log = AsyncMock(
        return_value=[
            {
                "ID": "call-1",
                "Type": "received",
                "Number": "5550100",
                "DateTime": "2026-11-20T09:55:00+00:00",
            }
        ]
    )

    await coord.async_refresh_access_history()

    history_by_source = {event["_source"]: event for event in history.snapshot(5000)}
    assert history_by_source["doorlog"]["_category"] == "access"
    assert history_by_source["call_log"]["_category"] == "call"
    coord.api.call_log.assert_awaited_once()


@pytest.mark.asyncio
async def test_event_poll_refreshes_calls_at_most_once_per_minute(monkeypatch):
    coord = make_coordinator()
    start_polling(coord, monkeypatch)
    coord.hass.data[DOMAIN] = {
        "access_history": AccessHistory(),
        "settings_store": SimpleNamespace(
            get_access_history_limit=lambda: 5000,
            get_access_history_storage_limit=lambda: 5000,
            get_access_history_retention_seconds=lambda: 30 * 24 * 60 * 60,
        ),
    }
    coord.api.events_last.return_value = []
    coord.api.call_log = AsyncMock(
        return_value=[
            {
                "ID": "call-2",
                "Type": "missed",
                "DateTime": "2026-11-20T10:00:00+00:00",
            }
        ]
    )

    await coord._async_poll_events(None)
    await coord._async_poll_events(None)

    coord.api.call_log.assert_awaited_once()
    event = coord.hass.data[DOMAIN]["access_history"].snapshot(5000)[0]
    assert event["_source"] == "call_log"
    assert event["_category"] == "call"


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
