import asyncio
import hashlib
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from aiohttp import web
from homeassistant.helpers import network

import custom_components.akuvox_ac.guest_links as guest_links
import custom_components.akuvox_ac.http as http
import custom_components.akuvox_ac.integration as integration
from custom_components.akuvox_ac.const import DOMAIN, EVENT_NON_KEY_ACCESS_GRANTED


class MemoryStore:
    def __init__(self, _hass, _version, _key):
        self.data = {"links": {}, "audit": []}

    async def async_load(self):
        return self.data

    async def async_save(self, data):
        self.data = data


def _request(hass, payload=None, *, remote="203.0.113.9", token=None):
    async def json():
        return payload

    return SimpleNamespace(
        app={"hass": hass},
        json=json,
        remote=remote,
        headers={
            "User-Agent": "Test browser",
            "Accept-Language": "en-GB",
            "Sec-CH-UA": '"Browser"',
            "Authorization": f"Bearer {token}" if token else "",
        },
        match_info={},
    )


@pytest.mark.asyncio
async def test_guest_link_creation_stores_hash_and_use_logs_and_fires_access_event(monkeypatch):
    monkeypatch.setattr(guest_links, "Store", MemoryStore)
    monkeypatch.setattr(http, "_request_is_admin", lambda _hass, _request: True)
    events = []
    hass = SimpleNamespace(
        data={DOMAIN: {}},
        services=SimpleNamespace(async_call=AsyncMock()),
        bus=SimpleNamespace(async_fire=lambda name, data: events.append((name, data))),
    )
    created = await guest_links.GuestLinkAdminView().post(
        _request(
            hass,
            {
                "label": "Visitor",
                "timezone": "UTC",
                "weekdays": list(range(7)),
                "actions": [
                    {"label": "Open gate", "entity_id": "cover.gate", "service": "cover.open_cover"}
                ],
            },
        )
    )
    result = created["args"][0]
    token = result["url"].rsplit("#", 1)[1]
    assert "/akuvox-ac/share#" in result["url"]
    stored = hass.data[DOMAIN]["guest_links_data"]
    link = next(iter(stored["links"].values()))
    assert link["token_hash"] == hashlib.sha256(token.encode()).hexdigest()
    assert token not in str(link)
    assert "token_hash" not in str(result["link"])

    view = guest_links.GuestLinkPublicView()
    info = await view.get(_request(hass, remote="203.0.113.9", token=token))
    assert info["args"][0]["active"] is True
    action_response = await view.post(_request(hass, {"action": 0}, token=token))
    assert action_response["args"][0]["ok"] is True
    hass.services.async_call.assert_awaited_once_with(
        "cover", "open_cover", {"entity_id": "cover.gate"}, blocking=True
    )
    assert any(name == EVENT_NON_KEY_ACCESS_GRANTED for name, _data in events)
    assert any(name == f"{DOMAIN}_guest_action" for name, _data in events)
    outcomes = [entry["outcome"] for entry in stored["audit"]]
    assert "viewed" in outcomes
    assert "action_started" in outcomes
    assert "success" in outcomes
    assert all(entry.get("ip") == "203.0.113.9" for entry in stored["audit"])


@pytest.mark.asyncio
async def test_guest_link_manager_requires_an_administrator(monkeypatch):
    monkeypatch.setattr(guest_links, "Store", MemoryStore)
    monkeypatch.setattr(http, "_request_is_admin", lambda _hass, _request: False)
    hass = SimpleNamespace(data={DOMAIN: {}})
    with pytest.raises(web.HTTPUnauthorized):
        await guest_links.GuestLinkAdminView().get(_request(hass))


@pytest.mark.asyncio
async def test_guest_link_creation_requires_a_public_https_url(monkeypatch):
    monkeypatch.setattr(guest_links, "Store", MemoryStore)
    monkeypatch.setattr(http, "_request_is_admin", lambda _hass, _request: True)
    monkeypatch.setattr(network, "get_url", lambda *_args, **_kwargs: "http://homeassistant.local:8123")
    hass = SimpleNamespace(data={DOMAIN: {}})
    with pytest.raises(web.HTTPServiceUnavailable):
        await guest_links.GuestLinkAdminView().post(
            _request(
                hass,
                {
                    "timezone": "UTC",
                    "weekdays": [0],
                    "actions": [{"entity_id": "light.drive", "service": "light.turn_on"}],
                },
            )
        )
    assert hass.data[DOMAIN]["guest_links_data"]["links"] == {}


@pytest.mark.asyncio
async def test_guest_link_revocation_removes_token_immediately(monkeypatch):
    monkeypatch.setattr(guest_links, "Store", MemoryStore)
    monkeypatch.setattr(http, "_request_is_admin", lambda _hass, _request: True)
    hass = SimpleNamespace(data={DOMAIN: {}}, bus=SimpleNamespace(async_fire=Mock()))
    store, data = await guest_links._store(hass)
    data["links"]["link1"] = {
        "id": "link1",
        "token_hash": hashlib.sha256(b"secret").hexdigest(),
        "label": "Temporary",
        "actions": [],
    }
    await store.async_save(data)
    request = _request(hass)
    request.match_info = {"link_id": "link1"}
    response = await guest_links.GuestLinkRevokeView().delete(request, "link1")
    assert response["args"][0] == {"ok": True}
    with pytest.raises(web.HTTPNotFound):
        await guest_links.GuestLinkPublicView().get(_request(hass, token="secret"))


@pytest.mark.asyncio
async def test_guest_action_rate_limit_serializes_concurrent_requests(monkeypatch):
    monkeypatch.setattr(guest_links, "Store", MemoryStore)

    async def delayed_call(*_args, **_kwargs):
        await asyncio.sleep(0.02)

    hass = SimpleNamespace(
        data={DOMAIN: {}},
        services=SimpleNamespace(async_call=AsyncMock(side_effect=delayed_call)),
        bus=SimpleNamespace(async_fire=Mock()),
    )
    store, data = await guest_links._store(hass)
    token = "high-entropy-test-code"
    data["links"]["link1"] = {
        "id": "link1",
        "token_hash": hashlib.sha256(token.encode()).hexdigest(),
        "label": "Gate",
        "actions": [{"label": "Open", "entity_id": "cover.gate", "service": "cover.open_cover"}],
        "weekdays": list(range(7)),
        "daily_start": "00:00",
        "daily_end": "23:59",
        "timezone": "UTC",
        "enabled": True,
    }
    await store.async_save(data)
    view = guest_links.GuestLinkPublicView()
    results = await asyncio.gather(
        view.post(_request(hass, {"action": 0}, token=token)),
        view.post(_request(hass, {"action": 0}, token=token)),
        return_exceptions=True,
    )
    assert sum(not isinstance(result, Exception) for result in results) == 1
    assert sum(isinstance(result, web.HTTPTooManyRequests) for result in results) == 1
    hass.services.async_call.assert_awaited_once()


@pytest.mark.asyncio
async def test_expired_guest_action_is_audited_and_never_reaches_home_assistant(monkeypatch):
    monkeypatch.setattr(guest_links, "Store", MemoryStore)
    hass = SimpleNamespace(
        data={DOMAIN: {}},
        services=SimpleNamespace(async_call=AsyncMock()),
        bus=SimpleNamespace(async_fire=Mock()),
    )
    store, data = await guest_links._store(hass)
    token = "expired-test-code"
    data["links"]["link1"] = {
        "id": "link1",
        "token_hash": hashlib.sha256(token.encode()).hexdigest(),
        "label": "Old link",
        "actions": [{"label": "Open", "entity_id": "cover.gate", "service": "cover.open_cover"}],
        "expires_at": "2000-01-01T00:00:00+00:00",
        "weekdays": list(range(7)),
        "daily_start": "00:00",
        "daily_end": "23:59",
        "timezone": "UTC",
        "enabled": True,
    }
    await store.async_save(data)
    with pytest.raises(web.HTTPForbidden):
        await guest_links.GuestLinkPublicView().post(_request(hass, {"action": 0}, token=token))
    hass.services.async_call.assert_not_awaited()
    assert data["audit"][-1]["outcome"] == "expired"


@pytest.mark.asyncio
async def test_guest_action_uses_configured_granted_access_notification_targets(monkeypatch):
    monkeypatch.setattr(guest_links, "Store", MemoryStore)
    hass = SimpleNamespace(
        data={
            DOMAIN: {
                "settings_store": SimpleNamespace(
                    targets_for_event=lambda event_type, user_id=None: (
                        ["mobile_app_owner"] if event_type == "guest_access" else []
                    )
                )
            }
        },
        services=SimpleNamespace(async_call=AsyncMock()),
        bus=SimpleNamespace(async_fire=Mock()),
    )
    store, data = await guest_links._store(hass)
    token = "alert-test-code"
    data["links"]["link1"] = {
        "id": "link1",
        "token_hash": hashlib.sha256(token.encode()).hexdigest(),
        "label": "Visitor",
        "actions": [{"label": "Open gate", "entity_id": "cover.gate", "service": "cover.open_cover"}],
        "weekdays": list(range(7)),
        "daily_start": "00:00",
        "daily_end": "23:59",
        "timezone": "UTC",
        "enabled": True,
    }
    await store.async_save(data)
    await guest_links.GuestLinkPublicView().post(_request(hass, {"action": 0}, token=token))
    assert hass.services.async_call.await_count == 2
    alert = hass.services.async_call.await_args_list[1]
    assert alert.args[0:2] == ("notify", "mobile_app_owner")
    assert alert.args[2]["title"] == "Guest access used"
    assert data["audit"][-1]["outcome"] == "alert_sent"


def test_guest_alert_targets_are_opt_in_in_settings_store():
    settings = integration.AkuvoxSettingsStore.__new__(integration.AkuvoxSettingsStore)
    settings.data = {
        "alerts": {
            "targets": {
                "mobile_app_owner": {"guest_access": True},
                "mobile_app_other": {"guest_access": False},
            }
        }
    }
    assert settings.targets_for_event("guest_access") == ["mobile_app_owner"]
