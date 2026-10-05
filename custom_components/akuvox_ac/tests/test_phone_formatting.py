"""Regression coverage for phone numbers pasted from mobile contacts."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from custom_components.akuvox_ac.http import _build_face_upload_payload
from custom_components.akuvox_ac.const import HA_CONTACT_GROUP_NAME
from custom_components.akuvox_ac.integration import AkuvoxUsersStore
from custom_components.akuvox_ac.phone import normalize_phone_number
from .test_contact_sync import _ApiStub, _make_manager
from .test_paused_user_sync import _build_desired


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("07000 000000", "07000000000"),
        ("  07000  000000  ", "07000000000"),
        ("\t07000\n000000\r", "07000000000"),
        ("\u00a007000\u202f000\u2009000\u00a0", "07000000000"),
        (" +44 7000 000000 ", "+447000000000"),
        ("0044 7000 000000", "00447000000000"),
        ("07000000000", "07000000000"),
        ("*123#", "*123#"),
        ("", ""),
        (" \t\u00a0 ", ""),
        (None, ""),
    ],
)
def test_phone_whitespace_is_removed_without_changing_dial_characters(raw, expected):
    assert normalize_phone_number(raw) == expected


@pytest.mark.asyncio
async def test_create_and_edit_profile_persist_formatted_phone():
    store = AkuvoxUsersStore(SimpleNamespace())
    store.async_save = AsyncMock()

    await store.upsert_profile("HA001", name="Test User", phone=" 07000\u00a0000000 ")
    assert store.get("HA001")["phone"] == "07000000000"

    await store.upsert_profile("HA001", phone=" +44\u202f7000 000000 ")
    assert store.get("HA001")["phone"] == "+447000000000"

    await store.upsert_profile("HA001", name="Renamed User")
    assert store.get("HA001")["phone"] == "+447000000000"

    await store.upsert_profile("HA001", phone=" \t ")
    assert store.get("HA001")["phone"] == ""
    assert store.async_save.await_count == 4


@pytest.mark.parametrize(
    ("profile", "local", "expected"),
    [
        ({"phone": " 07000\u00a0000000 "}, {}, "07000000000"),
        ({}, {"PhoneNum": " 07000 000000 "}, "07000000000"),
        ({}, {"Phone": " +44\u202f7000 000000 "}, "+447000000000"),
        ({"phone": ""}, {"PhoneNum": "07000 000000"}, ""),
        ({"phone": " \t "}, {"PhoneNum": "07000 000000"}, ""),
    ],
)
def test_sync_and_face_upload_use_formatted_phone(profile, local, expected):
    assert _build_desired(profile, local)["PhoneNum"] == expected
    payload = _build_face_upload_payload(profile, local, "HA001", "")
    assert payload["PhoneNum"] == expected
    if "Phone" in local:
        assert payload["Phone"] == expected


@pytest.mark.asyncio
async def test_existing_contact_with_spaces_is_repaired_and_stays_stable():
    manager = _make_manager()
    contact = {
        "Name": "Test User",
        "Phone": "07000\u00a0000000",
        "Group": HA_CONTACT_GROUP_NAME,
    }
    api = _ApiStub([contact])
    profiles = [("Test User", " 07000 000000 ")]

    await manager._sync_contacts_for_profiles(api, profiles)

    assert len(api.delete_calls) == 1
    assert len(api.add_calls) == 1
    assert api.add_calls[0][0]["Phone"] == "07000000000"
    assert api.add_calls[0][0]["PhoneNum"] == "07000000000"

    clean_api = _ApiStub(api.add_calls[0])
    await manager._sync_contacts_for_profiles(clean_api, profiles)
    assert clean_api.delete_calls == []
    assert clean_api.add_calls == []
