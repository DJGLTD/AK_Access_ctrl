"""PIN entry must tolerate pasted spaces without silently changing digits."""

import ast
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from custom_components.akuvox_ac.http import (
    _build_face_upload_payload,
    sanitize_self_service_profile_payload,
)
from custom_components.akuvox_ac.integration import AkuvoxUsersStore
from custom_components.akuvox_ac import integration
from custom_components.akuvox_ac.pin import PIN_VALIDATION_MESSAGE, validate_pin
from .test_paused_user_sync import _build_desired


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        (" 01 23 ", "0123"),
        ("\t00\n12\r", "0012"),
        ("\u00a000\u202f12\u2009", "0012"),
        ("0000", "0000"),
        ("12345678", "12345678"),
        ("", ""),
        (" \t\u00a0 ", ""),
        (None, ""),
    ],
)
def test_valid_pin_and_self_service_formatting(raw, expected):
    assert validate_pin(raw) == expected
    assert sanitize_self_service_profile_payload({"pin": raw}, "HA001")["pin"] == expected


@pytest.mark.parametrize("raw", ["12a3", "12-34", "+123", "12.3", "123#", "１２３４", "١٢٣٤"])
def test_invalid_pin_is_rejected_without_echoing_the_credential(raw):
    for validate in (validate_pin, lambda pin: sanitize_self_service_profile_payload({"pin": pin}, "HA001")):
        with pytest.raises(ValueError) as error:
            validate(raw)
        assert str(error.value) == PIN_VALIDATION_MESSAGE


@pytest.mark.asyncio
async def test_profile_creation_edit_and_clear_preserve_leading_zeroes():
    store = AkuvoxUsersStore(SimpleNamespace())
    store.async_save = AsyncMock()
    await store.upsert_profile("HA001", name="Test User", pin=" 01\u00a023 ")
    assert store.get("HA001")["pin"] == "0123"
    await store.upsert_profile("HA001", pin=" 00 42 ")
    assert store.get("HA001")["pin"] == "0042"
    await store.upsert_profile("HA001", name="Renamed User")
    assert store.get("HA001")["pin"] == "0042"
    await store.upsert_profile("HA001", pin=" \t ")
    assert store.get("HA001")["pin"] == ""
    assert store.async_save.await_count == 4


@pytest.mark.asyncio
async def test_invalid_pin_cannot_partially_change_or_create_profile():
    store = AkuvoxUsersStore(SimpleNamespace())
    store.async_save = AsyncMock()
    store.data["users"]["HA001"] = {"name": "Original", "pin": "0123"}
    for user_id in ("HA001", "HA002"):
        with pytest.raises(ValueError):
            await store.upsert_profile(user_id, name="Changed", pin="12x3")
    assert store.all() == {"HA001": {"name": "Original", "pin": "0123"}}
    store.async_save.assert_not_awaited()


@pytest.mark.parametrize("key", ["pin", "PrivatePIN", "private_pin", "Pin"])
@pytest.mark.parametrize("raw, expected", [(" 00\u202f12 ", "0012"), (" \t ", "")])
def test_device_sync_and_face_upload_format_profile_pin(key, raw, expected):
    profile = {key: raw}
    local = {"PrivatePIN": "9999", "Pin": "9999"}
    assert _build_desired(profile, local)["PrivatePIN"] == expected
    payload = _build_face_upload_payload(profile, local, "HA001", "")
    assert payload["PrivatePIN"] == expected
    assert "Pin" not in payload


@pytest.mark.parametrize("key", ["PrivatePIN", "Pin"])
def test_device_fallback_pin_is_formatted(key):
    local = {key: " 00 12 "}
    assert _build_desired({}, local)["PrivatePIN"] == "0012"
    assert _build_face_upload_payload({}, local, "HA001", "")[key] == "0012"


def _user_service(name, hass):
    """Exercise real service callbacks without starting Home Assistant."""
    source = Path(integration.__file__)
    module = ast.parse(source.read_text(encoding="utf-8"))
    setup = next(node for node in module.body if isinstance(node, ast.AsyncFunctionDef) and node.name == "async_setup_entry")
    names = {name, "_home_assistant_link_from_service"}
    callbacks = [node for node in setup.body if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name in names]
    namespace = dict(vars(integration), hass=hass)
    exec(compile(ast.Module(body=callbacks, type_ignores=[]), str(source), "exec"), namespace)
    return namespace[name]


@pytest.mark.asyncio
@pytest.mark.parametrize("service", ["svc_add_user", "svc_add_temporary_user", "svc_edit_user"])
async def test_services_validate_before_saving_or_reserving_a_user(service):
    store = AkuvoxUsersStore(SimpleNamespace())
    store.async_save = AsyncMock()
    queue = SimpleNamespace(mark_change=Mock())
    hass = SimpleNamespace(data={integration.DOMAIN: {"users_store": store, "sync_queue": queue}})
    callback = _user_service(service, hass)

    with pytest.raises(ValueError, match="PIN must contain digits only"):
        await callback(SimpleNamespace(data={"id": "HA001", "name": "Test", "pin": "12x3"}, context=None))
    assert store.all() == {}
    store.async_save.assert_not_awaited()
    queue.mark_change.assert_not_called()

    await callback(SimpleNamespace(data={"id": "HA001", "name": "Test", "pin": " 00\u202f12 "}, context=None))
    profiles = list(store.all().values())
    assert len(profiles) == 1
    assert profiles[0]["pin"] == "0012"
    assert profiles[0]["status"] == "active"
    queue.mark_change.assert_called_once()
