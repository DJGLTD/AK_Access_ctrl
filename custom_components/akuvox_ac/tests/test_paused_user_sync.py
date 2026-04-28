"""Regression tests for paused-user sync behavior."""

from types import SimpleNamespace

# Importing this module first sets up Home Assistant stubs.
from . import test_exit_permissions  # noqa: F401

import custom_components.akuvox_ac.integration as integration


def _make_hass():
    return SimpleNamespace(data={integration.DOMAIN: {}}, config=SimpleNamespace(path=lambda *parts: "/tmp"))


def _build_desired(profile: dict, local: dict | None = None) -> dict:
    return integration._desired_device_user_payload(
        _make_hass(),
        "user1",
        profile,
        local or {},
        opts={},
        sched_map={"24/7 access": "1001", "no access": "1002"},
        exit_schedule_map={},
        face_root_base="https://example.invalid/local/AK_Access_ctrl/FaceData",
        device_type_raw="intercom",
    )


def test_paused_user_clears_phone_number_on_device_payload():
    profile = {
        "name": "User One",
        "paused": True,
        "phone": "15551234567",
        "schedule_name": "No Access",
        "schedule_id": "1002",
    }
    local = {"PhoneNum": "15557654321", "FaceUrl": "https://example.invalid/existing.jpg"}

    desired = _build_desired(profile, local)

    assert desired.get("PhoneNum") == ""


def test_active_user_retains_phone_number_on_device_payload():
    profile = {
        "name": "User One",
        "paused": False,
        "phone": "15551234567",
        "schedule_name": "24/7 Access",
        "schedule_id": "1001",
    }

    desired = _build_desired(profile, {"FaceUrl": "https://example.invalid/existing.jpg"})

    assert desired.get("PhoneNum") == "15551234567"


def test_face_url_keeps_face_register_enabled_during_sync():
    profile = {
        "name": "User One",
        "schedule_name": "24/7 Access",
        "schedule_id": "1001",
        "face_url": "https://example.invalid/device/Face/user1.jpg",
    }

    desired = _build_desired(profile, {})

    assert desired.get("FaceUrl") == profile["face_url"]
    assert desired.get("FaceRegister") == 1


def test_prepare_user_set_payload_keeps_existing_face_url_and_status():
    payload = integration._prepare_user_set_payload(
        "HA001",
        {"UserID": "HA001", "Name": "User One"},
        {"ID": "368", "UserID": "HA001", "Name": "User One", "FaceUrl": "", "FaceRegisterStatus": "1", "ContactID": "16"},
    )

    assert payload.get("FaceUrl") == ""
    assert payload.get("FaceRegisterStatus") == "1"
    assert payload.get("ContactID") == "16"


def test_record_matches_desired_fields_treats_face_register_status_as_registered():
    local = {
        "UserID": "HA001",
        "Name": "User One",
        "FaceUrl": "https://example.invalid/device/Face/HA001.jpg",
        "FaceRegisterStatus": "1",
    }
    desired = {
        "UserID": "HA001",
        "Name": "User One",
        "FaceUrl": "https://example.invalid/device/Face/HA001.jpg",
        "FaceRegister": 1,
    }

    assert integration._record_matches_desired_fields(local, desired) is True


def test_record_matches_desired_fields_detects_missing_face_registration():
    local = {
        "UserID": "HA001",
        "Name": "User One",
        "FaceUrl": "https://example.invalid/device/Face/HA001.jpg",
        "FaceRegisterStatus": "0",
    }
    desired = {
        "UserID": "HA001",
        "Name": "User One",
        "FaceUrl": "https://example.invalid/device/Face/HA001.jpg",
        "FaceRegister": 1,
    }

    assert integration._record_matches_desired_fields(local, desired) is False
