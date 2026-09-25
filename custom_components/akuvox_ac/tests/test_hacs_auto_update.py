import asyncio
import inspect
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any, Dict, List, Tuple
from unittest.mock import AsyncMock, Mock

import pytest

from custom_components.akuvox_ac.ha_test_stubs import ensure_homeassistant_stubs

ensure_homeassistant_stubs()

from custom_components.akuvox_ac import integration as integration_module  # noqa: E402
from custom_components.akuvox_ac.const import DOMAIN  # noqa: E402
from custom_components.akuvox_ac.integration import (  # noqa: E402
    AkuvoxSettingsStore,
    HacsAutoUpdater,
)


LATEST_TAG = "v3.2.0"
LATEST_VERSION = "3.2.0"


class _State:
    entity_id = "update.akuvox_access_control_update"
    state = "off"
    attributes = {
        "friendly_name": "Akuvox Access Control update",
        "repository": "DJGLTD/AK_Access_ctrl",
        "installed_version": "3.1.0",
        "latest_version": "3.1.0",
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
            version = HacsAutoUpdater._display_version((data or {}).get("version") or LATEST_TAG)
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
        self.is_running = False
        self.bus = SimpleNamespace(async_listen_once=Mock(return_value=Mock()))

    def async_create_task(self, coroutine):
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            coroutine.close()
            raise RuntimeError("async_create_task called outside the event loop")
        return loop.create_task(coroutine)


class _GithubResponse:
    status = 200

    async def __aenter__(self) -> "_GithubResponse":
        return self

    async def __aexit__(self, *_: Any) -> None:
        return None

    async def json(self) -> Dict[str, str]:
        return {"tag_name": LATEST_TAG}


class _GithubSession:
    def get(self, *_: Any, **__: Any) -> _GithubResponse:
        return _GithubResponse()


def _settings_store() -> AkuvoxSettingsStore:
    store = object.__new__(AkuvoxSettingsStore)
    store.data = {
        "hacs_auto_update": {
            "enabled": True,
            "interval_hours": 24,
            "check_time": "02:00",
            "auto_install": False,
            "restart_after_install": False,
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


def test_hacs_auto_update_sanitizes_daily_schedule_options():
    store = object.__new__(AkuvoxSettingsStore)

    cfg = store._sanitize_hacs_auto_update(
        {
            "enabled": "yes",
            "check_time": "2:05",
            "auto_install": "on",
            "restart_after_install": 1,
        }
    )

    assert cfg["enabled"] is True
    assert cfg["check_time"] == "02:05"
    assert cfg["auto_install"] is True
    assert cfg["restart_after_install"] is True

    invalid = store._sanitize_hacs_auto_update({"check_time": "25:99"})

    assert invalid["check_time"] == "02:00"


def test_hacs_auto_update_schedules_daily_check(monkeypatch):
    store = _settings_store()
    store.data["hacs_auto_update"]["check_time"] = "03:15"
    hass = _Hass(store)
    scheduled: Dict[str, Any] = {}

    def _track_time_change(_hass, callback, *, hour=None, minute=None, second=None):
        scheduled["callback"] = callback
        scheduled["hour"] = hour
        scheduled["minute"] = minute
        scheduled["second"] = second
        return lambda: scheduled.__setitem__("cancelled", True)

    monkeypatch.setattr(integration_module, "async_track_time_change", _track_time_change)

    updater = HacsAutoUpdater(hass)
    updater.apply_settings()

    assert scheduled["hour"] == 3
    assert scheduled["minute"] == 15
    assert scheduled["second"] == 0
    assert updater.status()["active"] is True


def test_hacs_auto_update_check_detects_latest_release_when_hacs_entity_is_stale(monkeypatch):
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
    assert status["installed_version"] == "3.1.0"
    assert status["latest_version"] == LATEST_VERSION
    assert status["pending_version"] == LATEST_VERSION
    assert status["pending_version_full"] == LATEST_TAG


def test_hacs_auto_update_install_confirms_latest_release(monkeypatch):
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
                "version": LATEST_TAG,
            },
            True,
        )
    ]
    assert status["last_result"] == "installed"
    assert status["installed_version"] == LATEST_VERSION
    assert status["latest_version"] == LATEST_VERSION
    assert status["pending_version"] is None


def test_hacs_auto_update_scheduled_install_can_restart(monkeypatch):
    store = _settings_store()
    store.data["hacs_auto_update"]["auto_install"] = True
    store.data["hacs_auto_update"]["restart_after_install"] = True
    hass = _Hass(store)
    monkeypatch.setattr(
        integration_module,
        "async_get_clientsession",
        lambda _hass: _GithubSession(),
    )

    status = asyncio.run(HacsAutoUpdater(hass).async_run_scheduled_update())

    install_calls = [
        call for call in hass.services.calls if call[0] == "update" and call[1] == "install"
    ]
    restart_calls = [
        call
        for call in hass.services.calls
        if call[0] == "homeassistant" and call[1] == "restart"
    ]
    assert install_calls
    assert restart_calls
    assert status["last_result"] == "restart_requested"


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
    assert status["installed_version"] == "3.1.0"
    assert status["pending_version"] == LATEST_VERSION


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


def test_hacs_auto_update_matches_release_versions():
    assert HacsAutoUpdater._versions_match("v3.1.0", "3.1") is True
    assert HacsAutoUpdater._versions_match("3.1.1", "v3.1.1") is True
    assert HacsAutoUpdater._versions_match("3.1.1", "3.2.0") is False
    assert HacsAutoUpdater._versions_match("71d1bd2", "3.2.0") is False


async def _dispatch_ha_job(callback, argument):
    """HA runs unmarked synchronous listeners in its executor."""
    if inspect.iscoroutinefunction(callback):
        await callback(argument)
    else:
        await asyncio.to_thread(callback, argument)


def test_daily_timer_runs_repeatedly_and_keeps_install_time(monkeypatch):
    store = _settings_store()
    installed = "2026-09-08T04:17:37+01:00"
    store.data["hacs_auto_update"].update(last_checked=installed, last_installed=installed)
    hass = _Hass(store)
    callbacks = []
    monkeypatch.setattr(integration_module, "async_track_time_change",
                        lambda _hass, callback, **kwargs: callbacks.append(callback) or Mock())
    monkeypatch.setattr(integration_module, "async_get_clientsession", lambda _hass: _GithubSession())
    updater = HacsAutoUpdater(hass)
    updater.apply_settings()

    async def run():
        for day in (25, 26):
            now = datetime(2026, 9, day, 4, 30, tzinfo=UTC)
            monkeypatch.setattr(integration_module.dt_util, "now", lambda: now)
            await _dispatch_ha_job(callbacks[0], now)
            status = updater.status()
            assert status["last_checked"] == now.isoformat()
            assert status["last_installed"] == installed

    asyncio.run(run())


def test_changing_daily_schedule_cancels_previous_timer(monkeypatch):
    store = _settings_store()
    track = Mock(side_effect=lambda *args, **kwargs: Mock())
    monkeypatch.setattr(integration_module, "async_track_time_change", track)
    updater = HacsAutoUpdater(_Hass(store))
    updater.apply_settings()
    cancel = updater._interval_unsub
    asyncio.run(store.set_hacs_auto_update({"check_time": "04:30"}))
    updater.apply_settings()
    cancel.assert_called_once()
    assert track.call_args.kwargs == {"hour": 4, "minute": 30, "second": 0}
    cancel = updater._interval_unsub
    asyncio.run(store.set_hacs_auto_update({"enabled": False}))
    updater.apply_settings()
    cancel.assert_called_once()
    assert updater.status()["active"] is False


def test_stale_settings_form_cannot_overwrite_updater_status():
    store = _settings_store()
    stale = store.get_hacs_auto_update()
    new_status = {"last_checked": "2026-09-25T04:30:00+01:00",
                  "last_installed": "2026-09-08T04:17:37+01:00",
                  "last_result": "check_failed", "last_error": "Connection failed"}
    asyncio.run(store.update_hacs_auto_update_status(**new_status))
    stale["check_time"] = "04:30"
    result = asyncio.run(store.set_hacs_auto_update(stale))
    assert result["check_time"] == "04:30"
    for key, value in new_status.items():
        assert result[key] == value


@pytest.mark.parametrize("running", [False, True])
@pytest.mark.parametrize("due", [False, True])
def test_startup_or_reload_catches_up_once_when_due(monkeypatch, running, due):
    store = _settings_store()
    hass = _Hass(store)
    hass.is_running = running
    updater = HacsAutoUpdater(hass)
    updater.async_run_scheduled_update = AsyncMock()
    monkeypatch.setattr(updater, "_startup_check_due", lambda config: due)
    monkeypatch.setattr(updater, "apply_settings", Mock())
    monkeypatch.setattr(updater, "apply_restart_schedule", Mock())

    async def run():
        updater.start()
        updater.start()  # Multiple configured devices share this updater.
        if running:
            hass.bus.async_listen_once.assert_not_called()
            await asyncio.sleep(0)
        else:
            hass.bus.async_listen_once.assert_called_once()
            callback = hass.bus.async_listen_once.call_args.args[1]
            await _dispatch_ha_job(callback, None)
        updater.start()
        await asyncio.sleep(0)
        assert updater.async_run_scheduled_update.await_count == int(due)
        updater.shutdown()

    asyncio.run(run())


def test_scheduled_restart_callback_runs_on_event_loop(monkeypatch):
    store = _settings_store()
    hass = _Hass(store)
    callbacks = []
    monkeypatch.setattr(integration_module, "async_call_later",
                        lambda _hass, delay, callback: callbacks.append(callback) or Mock())
    updater = HacsAutoUpdater(hass)

    async def run():
        await updater.async_schedule_restart(datetime.now(UTC) + timedelta(minutes=30))
        await _dispatch_ha_job(callbacks[0], None)
        assert updater.status()["last_result"] == "restart_requested"
        assert updater.status()["restart_scheduled_for"] is None

    asyncio.run(run())


@pytest.mark.parametrize("confirm_install", [False, True])
def test_install_keeps_time_of_actual_release_check(monkeypatch, confirm_install):
    store = _settings_store()
    hass = _Hass(store, confirm_install=confirm_install)
    checked_at = datetime(2026, 9, 25, 4, 30, tzinfo=UTC)
    installed_at = checked_at + timedelta(minutes=2)
    now = checked_at
    monkeypatch.setattr(integration_module.dt_util, "now", lambda: now)
    monkeypatch.setattr(integration_module, "async_get_clientsession", lambda _hass: _GithubSession())
    service_call = hass.services.async_call

    async def install_with_delay(domain, service, *args, **kwargs):
        nonlocal now
        if (domain, service) == ("update", "install"):
            now = installed_at
        return await service_call(domain, service, *args, **kwargs)

    hass.services.async_call = install_with_delay
    status = asyncio.run(HacsAutoUpdater(hass).async_install_update(force=True))
    assert status["last_checked"] == checked_at.isoformat()
    assert status["last_installed"] == (installed_at.isoformat() if confirm_install else None)


@pytest.mark.parametrize("enabled", [False, True])
def test_startup_skips_disabled_or_already_checked_today(monkeypatch, enabled):
    store = _settings_store()
    now = datetime(2026, 9, 25, 12, tzinfo=UTC)
    store.data["hacs_auto_update"].update(enabled=enabled, last_checked=now.isoformat())
    monkeypatch.setattr(integration_module.dt_util, "now", lambda: now)
    updater = HacsAutoUpdater(_Hass(store))
    updater.async_run_scheduled_update = AsyncMock()
    asyncio.run(updater._handle_hass_started(None))
    updater.async_run_scheduled_update.assert_not_awaited()


def test_failed_check_records_attempt_without_changing_last_install(monkeypatch):
    store = _settings_store()
    installed = "2026-09-08T04:17:37+01:00"
    store.data["hacs_auto_update"]["last_installed"] = installed
    now = datetime(2026, 9, 25, 4, 30, tzinfo=UTC)
    monkeypatch.setattr(integration_module.dt_util, "now", lambda: now)
    hass = _Hass(store)
    hass.services.async_call = AsyncMock(side_effect=RuntimeError("HACS unavailable"))
    status = asyncio.run(HacsAutoUpdater(hass).async_run_scheduled_update())
    assert status["last_checked"] == now.isoformat()
    assert status["last_installed"] == installed
    assert status["last_result"] == "check_failed"
    assert status["last_error"] == "HACS unavailable"


def test_shutdown_cancels_reload_catchup(monkeypatch):
    hass = _Hass(_settings_store())
    hass.is_running = True
    updater = HacsAutoUpdater(hass)
    updater.async_run_scheduled_update = AsyncMock()
    monkeypatch.setattr(updater, "apply_settings", Mock())
    monkeypatch.setattr(updater, "apply_restart_schedule", Mock())

    async def run():
        updater.start()
        task = updater._startup_task
        updater.shutdown()
        await asyncio.sleep(0)
        assert task.cancelled()
        updater.async_run_scheduled_update.assert_not_awaited()

    asyncio.run(run())
