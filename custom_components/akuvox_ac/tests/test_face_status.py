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
    # By default devices report an active face profile.
    monkeypatch.setattr(http, "_device_face_is_active", lambda record: True)


def _make_hass():
    return SimpleNamespace(config=SimpleNamespace(path=lambda *parts: "/tmp"))


def test_face_status_stays_active_when_stored_active(monkeypatch):
    """Stored active status should remain active even if device reports pending."""

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

    assert result == "active"


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
