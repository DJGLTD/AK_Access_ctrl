import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any, Dict, List, Tuple

from custom_components.akuvox_ac.ha_test_stubs import ensure_homeassistant_stubs

ensure_homeassistant_stubs()

from custom_components.akuvox_ac import integration as integration_module  # noqa: E402
from custom_components.akuvox_ac.const import DOMAIN  # noqa: E402
from custom_components.akuvox_ac.integration import (  # noqa: E402
    AkuvoxSettingsStore,
    HacsAutoUpdater,
)


LATEST_SHA = "71d1bd2556e9377b183e73b73a8fda88fdfa89cc"


class _State:
    entity_id = "update.akuvox_access_control_update"
    state = "off"
    attributes = {
        "friendly_name": "Akuvox Access Control update",
        "repository": "DJGLTD/AK_Access_ctrl",
        "installed_version": "3175ae3",
        "latest_version": "3175ae3",
    }


class _States:
    def __init__(self, state: _State) -> None:
        self._state = state

    def async_all(self, domain: str | None = None) -> List[_State]:
        return [self._state] if domain in (None, "update") else []

    def get(self, entity_id: str) -> _State | None:
        return self._state if entity_id == self._state.entity_id else None


class _Services:
    def __init__(self, state: _State, *, confirm_install: bool = True) -> None:
        self.calls: List[Tuple[str, str, Dict[str, Any], bool]] = []
        self._state = state
        self._confirm_install = confirm_install

    async def async_call(
        self,
        domain: str,
        service: str,
        data: Dict[str, Any] | None = None,
        *,
        blocking: bool = False,
        **_: Any,
    ) -> None:
        self.calls.append((domain, service, dict(data or {}), blocking))
        if domain == "update" and service == "install" and self._confirm_install:
            version = str((data or {}).get("version") or LATEST_SHA)[:7]
            self._state.attributes = dict(self._state.attributes)
            self._state.attributes["installed_version"] = version
            self._state.attributes["latest_version"] = version
            self._state.state = "off"


class _Hass:
    def __init__(self, settings: AkuvoxSettingsStore, *, confirm_install: bool = True) -> None:
        self._state = _State()
        self._state.state = "off"
        self._state.attributes = dict(_State.attributes)
        self.states = _States(self._state)
        self.services = _Services(self._state, confirm_install=confirm_install)
        self.data = {DOMAIN: {"settings_store": settings}}


class _GithubResponse:
    status = 200

    async def __aenter__(self) -> "_GithubResponse":
        return self

    async def __aexit__(self, *_: Any) -> None:
        return None

    async def json(self) -> Dict[str, str]:
        return {"sha": LATEST_SHA}


class _GithubSession:
    def get(self, *_: Any, **__: Any) -> _GithubResponse:
        return _GithubResponse()


def _settings_store() -> AkuvoxSettingsStore:
    store = object.__new__(AkuvoxSettingsStore)
    store.data = {
        "hacs_auto_update": {
            "enabled": True,
            "interval_hours": 24,
            "update_entity": "",
            "backup": False,
            "last_result": "enabled",
        }
    }
    store.saved = 0

    async def _async_save():
        store.saved += 1

    store.async_save = _async_save
    return store


def test_hacs_auto_update_check_detects_github_head_when_hacs_entity_is_stale(monkeypatch):
    store = _settings_store()
    hass = _Hass(store)
    monkeypatch.setattr(
        integration_module,
        "async_get_clientsession",
        lambda _hass: _GithubSession(),
    )

    status = asyncio.run(HacsAutoUpdater(hass).async_run_check(force=True))

    install_calls = [
        call for call in hass.services.calls if call[0] == "update" and call[1] == "install"
    ]
    assert install_calls == []
    assert status["last_result"] == "update_available"
    assert status["installed_version"] == "3175ae3"
    assert status["latest_version"] == "71d1bd2"
    assert status["pending_version"] == "71d1bd2"
    assert status["pending_version_full"] == LATEST_SHA


def test_hacs_auto_update_install_confirms_github_head(monkeypatch):
    store = _settings_store()
    hass = _Hass(store)
    monkeypatch.setattr(
        integration_module,
        "async_get_clientsession",
        lambda _hass: _GithubSession(),
    )

    status = asyncio.run(HacsAutoUpdater(hass).async_install_update(force=True))

    install_calls = [
        call for call in hass.services.calls if call[0] == "update" and call[1] == "install"
    ]
    assert install_calls == [
        (
            "update",
            "install",
            {
                "entity_id": "update.akuvox_access_control_update",
                "version": LATEST_SHA,
            },
            True,
        )
    ]
    assert status["last_result"] == "installed"
    assert status["installed_version"] == "71d1bd2"
    assert status["latest_version"] == "71d1bd2"
    assert status["pending_version"] is None


def test_hacs_auto_update_install_requires_hacs_confirmation(monkeypatch):
    store = _settings_store()
    hass = _Hass(store, confirm_install=False)
    monkeypatch.setattr(
        integration_module,
        "async_get_clientsession",
        lambda _hass: _GithubSession(),
    )

    status = asyncio.run(HacsAutoUpdater(hass).async_install_update(force=True))

    assert status["last_result"] == "install_unconfirmed"
    assert status["installed_version"] == "3175ae3"
    assert status["pending_version"] == "71d1bd2"


def test_hacs_auto_update_schedules_and_cancels_restart(monkeypatch):
    store = _settings_store()
    hass = _Hass(store)
    scheduled: Dict[str, Any] = {}

    def _schedule(_hass, delay, callback):
        scheduled["delay"] = delay
        scheduled["callback"] = callback
        return lambda: scheduled.__setitem__("cancelled", True)

    monkeypatch.setattr(integration_module, "async_call_later", _schedule)

    restart_at = datetime.now(tz=UTC) + timedelta(minutes=30)
    status = asyncio.run(HacsAutoUpdater(hass).async_schedule_restart(restart_at.isoformat()))

    assert status["last_result"] == "restart_scheduled"
    assert status["restart_scheduled_for"]
    assert scheduled["delay"] > 0

    status = asyncio.run(HacsAutoUpdater(hass).async_cancel_restart())

    assert status["restart_scheduled_for"] is None


def test_hacs_auto_update_matches_short_and_full_commit_versions():
    assert HacsAutoUpdater._versions_match("71d1bd2", LATEST_SHA) is True
    assert HacsAutoUpdater._versions_match("3175ae3", LATEST_SHA) is False
