"""Manual dashboard services must reconcile and verify the requested devices."""

import ast
import asyncio
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from custom_components.akuvox_ac import integration
from .test_initial_integrity_check import _Api, _coordinator, _manager, _queue


def _register_manual_services(hass):
    """Execute the real nested callbacks and registrations without HA startup."""
    source_path = Path(integration.__file__)
    module = ast.parse(source_path.read_text(encoding="utf-8"))
    setup = next(
        node for node in module.body
        if isinstance(node, ast.AsyncFunctionDef) and node.name == "async_setup_entry"
    )
    service_names = {"sync_now", "force_full_sync"}
    callback_names = {f"svc_{name}" for name in service_names}
    callbacks = [
        node for node in setup.body
        if isinstance(node, ast.AsyncFunctionDef) and node.name in callback_names
    ]
    registrations = [
        node for node in setup.body
        if isinstance(node, ast.Expr)
        and isinstance(node.value, ast.Call)
        and isinstance(node.value.func, ast.Attribute)
        and node.value.func.attr == "async_register"
        and len(node.value.args) >= 3
        and isinstance(node.value.args[1], ast.Constant)
        and node.value.args[1].value in service_names
    ]
    assert len(callbacks) == len(registrations) == 2

    registered = {}
    hass.services = SimpleNamespace(
        async_register=lambda domain, name, callback: registered.update(
            {(domain, name): callback}
        )
    )
    namespace = dict(vars(integration), hass=hass)
    selected = ast.Module(body=[*callbacks, *registrations], type_ignores=[])
    exec(compile(selected, str(source_path), "exec"), namespace)
    return registered


@pytest.mark.parametrize("service_name", ["sync_now", "force_full_sync"])
@pytest.mark.parametrize("entry_id", ["keypad", None], ids=["device", "global"])
def test_manual_sync_services_reconcile_then_verify_requested_devices(service_name, entry_id):
    keypad_coord, keypad_api = _coordinator(), _Api()
    other_coord, other_api = _coordinator(sync_status="pending"), _Api()
    devices = [
        ("keypad", keypad_coord, keypad_api, {}),
        ("other", other_coord, other_api, {}),
    ]
    manager = _manager(devices)
    queue = _queue(manager)
    # A global request must include the new keypad even if another device is
    # the only target already waiting in the automatic sync queue.
    queue._pending_devices = {"other"}
    queue.sync_now = AsyncMock(wraps=queue.sync_now)
    reconciled = []

    async def reconcile(target, *, full):
        coord, api = next((coord, api) for key, coord, api, _ in devices if key == target)
        assert queue._lock.locked()
        assert coord.health["sync_status"] == "in_progress"
        assert api.user_reads == api.schedule_reads == 0
        assert full is (service_name == "force_full_sync")
        reconciled.append(target)

    manager.reconcile_device = reconcile
    services = _register_manual_services(manager.hass)
    callback = services[(integration.DOMAIN, service_name)]
    call = SimpleNamespace(data={"entry_id": entry_id} if entry_id else {}, context=None)

    asyncio.run(asyncio.wait_for(callback(call), timeout=1))

    queue.sync_now.assert_awaited_once()
    args, kwargs = queue.sync_now.await_args
    assert args == (entry_id,)
    assert kwargs["include_all"] is (entry_id is None)
    assert kwargs.get("full") is (True if service_name == "force_full_sync" else None)
    expected_targets = ["keypad"] if entry_id else ["keypad", "other"]
    assert reconciled == expected_targets
    for target, coord, api, _ in devices:
        if target not in expected_targets:
            assert api.user_reads == api.schedule_reads == 0
            assert coord.health["last_checked"] is None
            continue
        assert api.user_reads == api.schedule_reads == 1
        assert coord.health["last_checked"]
        assert coord.storage.data["last_checked"] == coord.health["last_checked"]
        assert coord.health["sync_status"] == "in_sync"
        verified_index = coord.events.index("Integrity check passed")
        succeeded_index = next(
            index for index, event in enumerate(coord.events)
            if event.startswith("Sync succeeded")
        )
        assert verified_index < succeeded_index
