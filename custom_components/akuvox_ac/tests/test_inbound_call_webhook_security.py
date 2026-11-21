import asyncio
from types import SimpleNamespace

import pytest

from custom_components.akuvox_ac.ha_test_stubs import ensure_homeassistant_stubs

ensure_homeassistant_stubs()

from custom_components.akuvox_ac import http as http_module
from custom_components.akuvox_ac.const import DOMAIN


class _Request:
    def __init__(self, hass, *, admin=False, remote="192.0.2.10"):
        self.app = {"hass": hass}
        self.query = {}
        self.remote = remote
        self.can_read_body = False
        self._values = {}
        if admin:
            self._values[http_module.KEY_HASS_USER] = SimpleNamespace(
                id="admin-user", name="Admin", is_admin=True
            )

    def get(self, key, default=None):
        return self._values.get(key, default)


def test_anonymous_webhook_response_does_not_expose_call_data(monkeypatch):
    hass = SimpleNamespace(data={DOMAIN: {}})
    request = _Request(hass)
    monkeypatch.setattr(
        http_module,
        "_process_inbound_call_webhook",
        _return_sensitive_call_data,
    )

    response = asyncio.run(http_module.AkuvoxInboundCallWebhook()._handle(request))

    assert response["args"][0] == {"ok": True}


async def _return_sensitive_call_data(*_args, **_kwargs):
    return {
        "ok": True,
        "number": "private-number",
        "raw_number": "private-number",
        "call": {"caller": "private-number"},
        "user_id": "private-user",
    }


def test_authenticated_dashboard_keeps_detailed_webhook_response(monkeypatch):
    hass = SimpleNamespace(data={DOMAIN: {}})
    request = _Request(hass, admin=True)
    monkeypatch.setattr(
        http_module,
        "_process_inbound_call_webhook",
        _return_sensitive_call_data,
    )

    response = asyncio.run(http_module.AkuvoxInboundCallWebhook()._handle(request))

    assert response["args"][0]["number"] == "private-number"
    assert response["args"][0]["call"]["caller"] == "private-number"


def test_webhook_rate_limit_rejects_bursts(monkeypatch):
    hass = SimpleNamespace(data={DOMAIN: {}})
    calls = []

    async def _record(*_args, **_kwargs):
        calls.append(True)
        return {"ok": True}

    monkeypatch.setattr(http_module, "_process_inbound_call_webhook", _record)
    view = http_module.AkuvoxInboundCallWebhook()
    limit = 20
    for _ in range(limit):
        asyncio.run(view._handle(_Request(hass)))

    assert len(calls) == limit
    with pytest.raises(http_module.web.HTTPTooManyRequests):
        asyncio.run(view._handle(_Request(hass)))
