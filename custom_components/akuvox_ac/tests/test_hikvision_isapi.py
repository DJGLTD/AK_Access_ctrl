import pytest
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

from custom_components.akuvox_ac.hikvision import (
    HikvisionISAPI,
    _digest_authorization,
)
import custom_components.akuvox_ac.integration as integration


def test_digest_authentication_matches_rfc_example():
    challenge = (
        'Digest realm="testrealm@host.com", '
        'qop="auth,auth-int", nonce="dcd98b7102dd2f0e8b11d0f600bfb0c093", '
        'opaque="5ccc069c403ebaf9f0171e9517f40e41"'
    )
    value = _digest_authorization(
        challenge,
        "Mufasa",
        "Circle Of Life",
        "GET",
        "/dir/index.html",
        1,
        "0a4f113b",
    )
    assert value is not None
    assert 'response="6629fae49393a05397450978507c4ef1"' in value
    assert 'opaque="5ccc069c403ebaf9f0171e9517f40e41"' in value


def test_digest_auth_rejects_unsupported_challenge():
    assert _digest_authorization("Basic realm=cam", "user", "pass", "GET", "/", 1, "x") is None
    assert _digest_authorization(
        'Digest realm="cam", nonce="n", algorithm="SHA-512", qop="auth"',
        "user",
        "pass",
        "GET",
        "/",
        1,
        "x",
    ) is None


def test_camera_configuration_validates_host_channel_and_brackets_ipv6():
    config = {
        "host": "2001:db8::10",
        "port": 80,
        "channel": 2,
        "use_https": False,
        "allow_insecure_http": True,
    }
    client = HikvisionISAPI(object(), config)
    assert client.base == "http://[2001:db8::10]:80"
    assert client.channel_path == "/ISAPI/Traffic/channels/2"
    with pytest.raises(ValueError):
        HikvisionISAPI(object(), {"host": "camera/path", "port": 80, "channel": 1})


def test_camera_transport_defaults_to_https_without_verifying_and_allows_explicit_http():
    secure = HikvisionISAPI(object(), {"host": "camera.local", "channel": 1})
    assert secure.base == "https://camera.local:443"
    assert secure.use_https is True
    assert secure.verify_ssl is False

    with pytest.raises(ValueError, match="HTTPS"):
        HikvisionISAPI(
            object(), {"host": "camera.local", "port": 80, "channel": 1, "use_https": False}
        )

    legacy = HikvisionISAPI(
        object(), {
            "host": "camera.local",
            "port": 80,
            "channel": 1,
            "use_https": False,
            "allow_insecure_http": True,
        }
    )
    assert legacy.base == "http://camera.local:80"


def test_camera_registration_payload_validation_and_isapi_response_status():
    assert HikvisionISAPI._validate_plate(" ab123cd ") == "AB123CD"
    with pytest.raises(ValueError):
        HikvisionISAPI._validate_plate("bad\nplate")
    with pytest.raises(ValueError, match="Camera rejected"):
        HikvisionISAPI._check_response(
            '<ResponseStatus><statusCode>4</statusCode><statusString>Camera rejected</statusString></ResponseStatus>'
        )
    assert HikvisionISAPI._check_response('{"statusCode":1,"statusString":"OK"}')


@pytest.mark.asyncio
async def test_camera_add_update_and_delete_use_the_supported_record_operations():
    camera = HikvisionISAPI(object(), {"host": "camera.local", "channel": 1})
    camera.request = AsyncMock(return_value='{"ResponseStatus":{"statusCode":1}}')
    await camera.async_add_plate("ab123cd", name="Resident")
    add_call = camera.request.await_args
    assert add_call.args == (
        "PUT",
        "/ISAPI/Traffic/channels/1/licensePlateAuditData/record?format=json",
    )
    assert '"operation": "new"' in add_call.kwargs["body"]

    camera.async_list_plates = AsyncMock(
        return_value=[{"id": "17", "plate": "AB123CD", "type": "whiteList"}]
    )
    await camera.async_update_plate("AB123CD", "CD456EF", name="Updated")
    update_call = camera.request.await_args
    assert '"id": "17"' in update_call.kwargs["body"]
    assert '"operation": "modify"' in update_call.kwargs["body"]
    assert "CD456EF" in update_call.kwargs["body"]

    await camera.async_delete_plate("CD456EF")
    delete_call = camera.request.await_args
    assert delete_call.args == (
        "PUT",
        "/ISAPI/Traffic/channels/1/DelLicensePlateAuditData?format=json",
    )
    assert '"licensePlate": ["CD456EF"]' in delete_call.kwargs["body"]


@pytest.mark.asyncio
async def test_camera_plate_search_paginates_through_results():
    camera = HikvisionISAPI(object(), {"host": "camera.local", "channel": 1})
    requests = []

    async def response(_method, _path, *, body, **_kwargs):
        requests.append(body)
        position = int(body.split("<searchResultPosition>", 1)[1].split("<", 1)[0])
        plates = range(position, min(position + 200, 201))
        records = "".join(
            f"<LicensePlateInfo><id>{index + 1}</id><LicensePlate>P{index}</LicensePlate><type>whiteList</type></LicensePlateInfo>"
            for index in plates
        )
        return (
            f'<LPListAuditSearchResult xmlns="http://www.std-cgi.org/ver20/XMLSchema">'
            f"<totalMatches>201</totalMatches><LicensePlateInfoList>{records}</LicensePlateInfoList>"
            "</LPListAuditSearchResult>"
        )

    camera.request = response
    rows = await camera.async_list_plates()
    assert len(rows) == 201
    assert len(requests) == 2
    assert "<searchResultPosition>200</searchResultPosition>" in requests[1]


@pytest.mark.asyncio
async def test_hikvision_config_entry_loads_and_unloads_without_an_akuvox_coordinator(monkeypatch):
    created = []

    class FakeCamera:
        def __init__(self, _hass, config):
            created.append(config)

    monkeypatch.setattr("custom_components.akuvox_ac.hikvision.HikvisionISAPI", FakeCamera)
    register_mock = Mock()
    monkeypatch.setattr(integration, "register_ui", register_mock)
    entry = SimpleNamespace(
        entry_id="hikvision-camera",
        data={"device_type": "Hikvision ANPR", "host": "camera.local", "channel": 1},
        options={},
    )
    hass = SimpleNamespace(data={})
    assert await integration.async_setup_entry(hass, entry) is True
    assert hass.data["akuvox_ac"][entry.entry_id]["anpr"].__class__ is FakeCamera
    assert hass.data["akuvox_ac"]["_ui_registered"] is True
    register_mock.assert_called_once_with(hass)
    assert created[0]["host"] == "camera.local"
    assert await integration.async_unload_entry(hass, entry) is True
    assert entry.entry_id not in hass.data["akuvox_ac"]
