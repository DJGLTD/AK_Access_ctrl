from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from custom_components.akuvox_ac import integration
from custom_components.akuvox_ac.const import DOMAIN


@pytest.mark.parametrize("other_device,unload_ok", [(False, True), (True, True), (False, False)])
@pytest.mark.asyncio
async def test_shared_workers_stop_only_after_last_successful_unload(other_device, unload_ok):
    workers = {key: SimpleNamespace(shutdown=Mock()) for key in (
        "sync_queue", "sync_manager", "hacs_auto_updater",
    )}
    root = {
        **workers,
        "door-1": {"coordinator": object()},
        "access_history": object(),
        "access_history_store": object(),
        "_dashboard_sessions": {},
    }
    if other_device:
        root["door-2"] = {"coordinator": object()}
    hass = SimpleNamespace(
        data={DOMAIN: root},
        config_entries=SimpleNamespace(async_unload_platforms=AsyncMock(return_value=unload_ok)),
    )
    assert await integration.async_unload_entry(hass, SimpleNamespace(entry_id="door-1")) == unload_ok
    for name, worker in workers.items():
        if unload_ok and not other_device:
            worker.shutdown.assert_called_once()
            assert name not in root
        else:
            worker.shutdown.assert_not_called()
            assert root[name] is worker


def test_sync_manager_cancels_all_schedules_once(monkeypatch):
    handles = []

    def schedule(*args, **kwargs):
        cancel = Mock()
        handles.append(cancel)
        return cancel

    monkeypatch.setattr(integration, "async_track_time_change", schedule)
    monkeypatch.setattr(integration, "async_track_time_interval", schedule)
    manager = integration.SyncManager(SimpleNamespace(data={DOMAIN: {}}))
    manager.shutdown()
    manager.shutdown()
    assert len(handles) == 7
    for cancel in handles:
        cancel.assert_called_once()


@pytest.mark.asyncio
async def test_add_user_service_accepts_face_reference_and_registers_poll_cleanup(monkeypatch, tmp_path):
    registered = {}
    unload_callbacks = []
    root = {
        "sync_manager": SimpleNamespace(set_integrity_interval=Mock()),
        "sync_queue": SimpleNamespace(mark_change=Mock()),
        "hacs_auto_updater": SimpleNamespace(start=Mock()),
        "_ui_registered": True,
        "_panel_registered": True,
    }
    hass = SimpleNamespace(
        data={DOMAIN: root},
        config=SimpleNamespace(path=lambda *parts: str(tmp_path.joinpath(*parts)), internal_url="http://ha.local", external_url=None),
        config_entries=SimpleNamespace(async_forward_entry_setups=AsyncMock()),
        services=SimpleNamespace(async_register=lambda domain, name, handler: registered.update({name: handler})),
    )
    entry = SimpleNamespace(
        entry_id="door-1", title="Gate", data={"host": "example.invalid"}, options={},
        async_on_unload=unload_callbacks.append, add_update_listener=Mock(return_value=lambda: None),
    )
    for name in ("_maybe_migrate_component_folder", "_migrate_face_storage"):
        monkeypatch.setattr(integration, name, Mock())
    for name in ("_remove_legacy_integration_device", "_remove_obsolete_device_entities"):
        monkeypatch.setattr(integration, name, AsyncMock())
    monkeypatch.setattr(integration, "async_get_clientsession", lambda _: object())
    assert await integration.async_setup_entry(hass, entry)
    coord = root["door-1"]["coordinator"]
    assert coord._event_poll_unsub is not None
    assert coord.shutdown in unload_callbacks
    await registered["add_user"](SimpleNamespace(data={"name": "Visitor", "face_file_name": "visitor.jpg"}))
    user = root["users_store"].all()["TMP001"]
    assert user["face_url"].endswith("/visitor.jpg")
    assert user["face_status"] == "pending"
    root["sync_queue"].mark_change.assert_called_once()
    for cancel in unload_callbacks:
        cancel()


@pytest.mark.asyncio
async def test_integrity_check_completes_during_face_retry_cooldown(monkeypatch):
    manager = object.__new__(integration.SyncManager)
    coord = SimpleNamespace(
        health={"sync_status": "in_sync", "device_type": "Intercom"},
        users=[], storage=None,
        async_refresh_access_history=AsyncMock(),
        async_record_integrity_check=AsyncMock(),
        _append_event=Mock(),
    )
    api = SimpleNamespace(
        user_list=AsyncMock(return_value=[{"ID": "1", "UserID": "HA001", "Name": "Alice"}]),
        schedule_get=AsyncMock(return_value=[]),
    )
    manager.hass = SimpleNamespace(is_running=True, config=SimpleNamespace(internal_url=None, external_url=None))
    manager._devices = lambda: [("door-1", coord, api, {})]
    manager._root = lambda: {}
    manager._settings_store = lambda: None
    manager._users_store = lambda: SimpleNamespace(all=lambda: {"HA001": {"name": "Alice", "groups": ["Default"]}})
    manager._schedules_store = lambda: None
    manager._device_schedule_map = AsyncMock(return_value={})
    manager._upload_face_asset_to_device = AsyncMock()
    monkeypatch.setattr(integration, "_desired_device_user_payload", lambda *a, **kw: {"FaceRegister": "1"})
    monkeypatch.setattr(integration, "_integrity_field_differences", lambda *a, **kw: ["face status"])
    cooldown = Mock(return_value=True)
    monkeypatch.setattr(integration, "_face_sync_on_cooldown", cooldown)
    await manager._integrity_check_cb(None)
    cooldown.assert_called_once()
    manager._upload_face_asset_to_device.assert_not_awaited()
    coord.async_record_integrity_check.assert_awaited_once()
    assert not any("Integrity check error" in str(call) for call in coord._append_event.call_args_list)
