import asyncio

from custom_components.akuvox_ac.api import AkuvoxAPI
from custom_components.akuvox_ac import api as api_module


class _Response:
    status = 500
    reason = "unavailable"

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return False


class _Session:
    def __init__(self):
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return _Response()


def test_akuvox_client_honors_secure_tls_defaults_and_arguments(monkeypatch):
    monkeypatch.setattr(api_module, "BasicAuth", lambda username, password: (username, password))
    default_api = AkuvoxAPI("intercom.local", username="installer", password="secret")
    explicit_api = AkuvoxAPI(
        "intercom.local", use_https=False, verify_ssl=False
    )

    assert default_api.use_https is True
    assert default_api.verify_ssl is True
    assert explicit_api.use_https is False
    assert explicit_api.verify_ssl is False


def test_akuvox_http_probe_never_sends_basic_auth_credentials(monkeypatch):
    monkeypatch.setattr(api_module, "BasicAuth", lambda username, password: (username, password))
    session = _Session()
    api = AkuvoxAPI(
        "intercom.local", username="installer", password="secret", session=session
    )

    asyncio.run(api._ensure_detected())

    http_calls = [(url, kwargs) for url, kwargs in session.calls if url.startswith("http://")]
    assert http_calls
    assert all(kwargs.get("auth") is None for _url, kwargs in http_calls)
    https_calls = [(url, kwargs) for url, kwargs in session.calls if url.startswith("https://")]
    assert https_calls
    assert all(kwargs.get("ssl") is True for _url, kwargs in https_calls)
