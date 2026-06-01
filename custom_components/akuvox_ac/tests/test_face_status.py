"""Tests for face enrolment status evaluation."""

from types import SimpleNamespace

import pytest

# Importing the existing test module sets up the Home Assistant stubs that
# custom_components.akuvox_ac.http depends on. The import is intentional
# and must remain before importing the module under test.
from . import test_exit_permissions  # noqa: F401

import custom_components.akuvox_ac.http as http


@pytest.fixture(autouse=True)
def _patch_face_helpers(monkeypatch):
    """Prevent filesystem access and control device face responses."""

    monkeypatch.setattr(http, "_face_image_exists", lambda hass, user_id: True)
    monkeypatch.setattr(
        http,
        "_original_device_face_is_active_for_tests",
        http._device_face_is_active,
        raising=False,
    )
    # By default devices report an active face profile.
    monkeypatch.setattr(http, "_device_face_is_active", lambda record: True)


def _make_hass():
    return SimpleNamespace(config=SimpleNamespace(path=lambda *parts: "/tmp"))


def test_face_status_returns_pending_when_stored_active_but_device_pending(monkeypatch):
    """Stored active status should not hide pending device face state."""

    # Simulate a device claiming the face profile is pending.
    monkeypatch.setattr(http, "_device_face_is_active", lambda record: False)

    hass = _make_hass()
    user = {"id": "user1", "face_url": "http://example.invalid/face.jpg"}
    devices = [
        {
            "participate_in_sync": True,
            "sync_status": "pending",
            "online": True,
            "users": [{"id": "user1"}],
        }
    ]

    result = http._evaluate_face_status(hass, user, devices, stored_status="active")

    assert result == "pending"


def test_face_status_errors_when_stored_active_but_device_register_mismatch():
    hass = _make_hass()
    user = {"id": "user1", "face_url": "http://example.invalid/face.jpg"}
    devices = [
        {
            "participate_in_sync": True,
            "sync_status": "in_sync",
            "online": True,
            "users": [
                {
                    "UserID": "user1",
                    "FaceUrl": "/mnt/Face/user1.jpg",
                    "FaceRegister": "0",
                }
            ],
        }
    ]

    result = http._evaluate_face_status(hass, user, devices, stored_status="active")

    assert result == "error"


def test_face_status_keeps_remote_face_url_pending_when_register_flag_stays_zero(monkeypatch):
    monkeypatch.setattr(
        http,
        "_device_face_is_active",
        http._original_device_face_is_active_for_tests,
    )

    hass = _make_hass()
    user = {"id": "user1", "face_url": "http://example.invalid/face.jpg"}
    devices = [
        {
            "participate_in_sync": True,
            "sync_status": "in_sync",
            "online": True,
            "users": [
                {
                    "UserID": "user1",
                    "FaceUrl": "http://example.invalid/face.jpg",
                    "FaceStatus": "0",
                    "FaceRegister": "0",
                }
            ],
        }
    ]

    result = http._evaluate_face_status(hass, user, devices, stored_status="pending")

    assert result == "pending"


def test_face_status_falls_back_to_pending_when_not_stored_active(monkeypatch):
    """When the store is not active we still surface pending state."""

    monkeypatch.setattr(http, "_device_face_is_active", lambda record: False)

    hass = _make_hass()
    user = {"id": "user1", "face_url": "http://example.invalid/face.jpg"}
    devices = [
        {
            "participate_in_sync": True,
            "sync_status": "pending",
            "online": True,
            "users": [{"id": "user1"}],
        }
    ]

    result = http._evaluate_face_status(hass, user, devices, stored_status="")

    assert result == "pending"


def test_face_register_status_is_treated_as_face_flag():
    assert http._face_flag_from_record({"FaceRegisterStatus": "1"}) is True
    assert http._face_flag_from_record({"FaceRegisterStatus": "0"}) is False
