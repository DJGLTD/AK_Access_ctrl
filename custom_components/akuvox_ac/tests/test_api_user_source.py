import asyncio

import pytest

from custom_components.akuvox_ac.api import AkuvoxAPI, _normalize_user_source


def test_normalize_user_source_prefers_firmware_source_type():
    record = {
        "UserID": "cloud-user",
        "Source": "Local",
        "SourceType": "Cloud",
    }

    normalized = _normalize_user_source(record)

    assert normalized["Source"] == "Cloud"
    assert record["Source"] == "Local"


def test_user_list_exposes_source_type_as_source():
    api = object.__new__(AkuvoxAPI)

    async def post_api(payload, *, rel_paths):
        return {
            "data": {
                "item": [
                    {
                        "UserID": "cloud-user",
                        "Name": "Cloud User",
                        "SourceType": "Cloud",
                    },
                    {
                        "UserID": "HA001",
                        "Name": "Local User",
                        "SourceType": "Local",
                    },
                ]
            }
        }

    api._post_api = post_api

    users = asyncio.run(api.user_list())

    assert users[0]["Source"] == "Cloud"
    assert users[1]["Source"] == "Local"


def test_strict_user_read_falls_back_to_legacy_keypad_endpoint():
    api = object.__new__(AkuvoxAPI)
    attempted = []

    async def post_api(payload, *, rel_paths):
        attempted.append(rel_paths)
        if len(attempted) == 1:
            raise RuntimeError("HTTP 404")
        return {"retcode": 1, "data": {"item": [{"UserID": "HA001"}]}}

    api._post_api = post_api

    assert asyncio.run(api.user_list(strict=True)) == [{"UserID": "HA001"}]
    assert len(attempted) == 2
    assert "/api/user/get" in attempted[1]


@pytest.mark.parametrize("reader", ["user_list", "schedule_get"])
@pytest.mark.parametrize("retcode", [0, 1, "0", "1"])
def test_strict_read_accepts_successful_empty_device(reader, retcode):
    api = object.__new__(AkuvoxAPI)

    async def request(*args, **kwargs):
        return {"retcode": retcode, "data": {"item": []}}

    api._post_api = api._get_api = request

    assert asyncio.run(getattr(api, reader)(strict=True)) == []


@pytest.mark.parametrize("reader", ["user_list", "schedule_get"])
@pytest.mark.parametrize("response", [
    {"retcode": -1, "data": {"item": []}},
    {"retcode": 0, "data": {}},
    {"retcode": 0, "data": {"item": "unavailable"}},
    {"retcode": 0, "data": {"item": [None]}},
    None,
])
def test_strict_read_rejects_failed_or_malformed_snapshots(reader, response):
    api = object.__new__(AkuvoxAPI)

    async def request(*args, **kwargs):
        return response

    api._post_api = api._get_api = request

    with pytest.raises(RuntimeError, match="Unable to read device"):
        asyncio.run(getattr(api, reader)(strict=True))


@pytest.mark.parametrize("reader", ["user_list", "schedule_get"])
def test_strict_read_raises_on_transport_failure_without_changing_default(reader):
    api = object.__new__(AkuvoxAPI)

    async def request(*args, **kwargs):
        raise RuntimeError("device unavailable")

    api._post_api = api._get_api = request

    assert asyncio.run(getattr(api, reader)()) == []
    with pytest.raises(RuntimeError, match="Unable to read device"):
        asyncio.run(getattr(api, reader)(strict=True))


def test_strict_schedule_read_uses_get_fallback_after_api_error():
    api = object.__new__(AkuvoxAPI)
    calls = []

    async def post_api(payload):
        calls.append(payload["action"])
        return {"retcode": -1, "data": {"item": []}}

    async def get_api(path):
        calls.append(path)
        return {"retcode": 0, "data": {"item": [{"Name": "Staff", "ID": "3"}]}}

    api._post_api, api._get_api = post_api, get_api

    assert asyncio.run(api.schedule_get(strict=True)) == [{"Name": "Staff", "ID": "3"}]
    assert calls == ["get", "list", "/api/schedule/get"]
