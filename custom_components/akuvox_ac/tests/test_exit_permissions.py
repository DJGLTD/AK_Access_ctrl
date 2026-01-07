import sys
from pathlib import Path

import pytest
from unittest.mock import MagicMock

from ha_test_stubs import ensure_homeassistant_stubs

# Ensure the repository root is on the path so component modules are importable.
ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

ensure_homeassistant_stubs()

import custom_components.akuvox_ac.integration as akuvox


@pytest.fixture(autouse=True)
def stub_relay_helpers(monkeypatch):
    monkeypatch.setattr(akuvox, "normalize_relay_roles", lambda roles, device_type: {"relay_a": "door"})
    monkeypatch.setattr(akuvox, "door_relays", lambda roles: "1")
    monkeypatch.setattr(
        akuvox,
        "relay_suffix_for_user",
        lambda roles, key_holder, pedestrian_access, device_type: "1",
    )
    monkeypatch.setattr(akuvox, "_face_asset_exists", lambda hass, user_id: False)


def _make_schedule_store(initial):
    store = akuvox.AkuvoxSchedulesStore.__new__(akuvox.AkuvoxSchedulesStore)
    store.data = {"schedules": initial}
    return store


def test_ensure_exit_clone_created_and_orphan_removed():
    schedules = {
        "Office": {"start": "09:00", "end": "17:00", "days": ["mon", "tue", "wed"], "type": "2"},
        "Old - EP": {"system_exit_clone": True, "exit_clone_for": "Old", "days": ["mon"], "type": "2"},
    }
    store = _make_schedule_store(schedules)
    changed = store._ensure_exit_clones(schedules)

    assert changed is True
    assert "Office - EP" in schedules
    clone = schedules["Office - EP"]
    assert clone["system_exit_clone"] is True
    assert clone["exit_clone_for"] == "Office"
    assert clone["days"] == ["mon", "tue", "wed"]
    assert clone["date_start"] == ""
    assert clone["date_end"] == ""
    assert "Old - EP" not in schedules


def test_ensure_exit_clone_idempotent_when_normalised():
    schedules = {
        "Office": {
            "start": "09:00",
            "end": "17:00",
            "days": ["mon", "tue"],
            "type": "2",
            "exit_clone_name": "Office - EP",
        },
        "Office - EP": {
            "start": "00:00",
            "end": "23:59",
            "days": ["mon", "tue"],
            "type": "2",
            "system_exit_clone": True,
            "exit_clone_for": "Office",
            "always_permit_exit": True,
            "date_start": "",
            "date_end": "",
        },
    }
    store = _make_schedule_store(schedules)
    changed = store._ensure_exit_clones(schedules)

    assert changed is False
    assert schedules["Office"]["exit_clone_name"] == "Office - EP"
    assert schedules["Office - EP"]["exit_clone_for"] == "Office"


def _payload_for_exit_permission(exit_permission, sched_map, exit_schedule_map):
    profile = {
        "name": "Nigel",
        "schedule_name": "Office",
        "schedule_id": "1500",
        "exit_permission": exit_permission,
        "face_url": "http://example.invalid/face.jpg",
        "face_active": False,
    }
    local = {}
    hass = MagicMock()
    opts = {"exit_device": True, "relay_roles": {}}
    return akuvox._desired_device_user_payload(
        hass,
        "HA123",
        profile,
        local,
        opts=opts,
        sched_map=sched_map,
        exit_schedule_map=exit_schedule_map,
        face_root_base="https://faces",
        device_type_raw="intercom",
    )


def test_exit_permission_defaults_to_match_when_clone_missing():
    sched_map = {"office": "1500"}
    exit_schedule_map = {"office": {"clone_name": "Office - EP"}}
    payload = _payload_for_exit_permission(None, sched_map, exit_schedule_map)
    assert payload["ScheduleID"] == "1500"
    assert "Schedule" not in payload


def test_exit_permission_always_forces_247_clone():
    sched_map = {"office": "1500", "office - ep": "2150", "24/7 access": "1001"}
    exit_schedule_map = {"office": {"clone_name": "Office - EP"}}
    payload = _payload_for_exit_permission("always", sched_map, exit_schedule_map)
    assert payload["ScheduleID"] == "2150"
    assert "Schedule" not in payload


def test_exit_permission_working_days_uses_clone_when_available():
    sched_map = {"office": "1500", "office - ep": "2150"}
    exit_schedule_map = {"office": {"clone_name": "Office - EP"}}
    payload = _payload_for_exit_permission("working_days", sched_map, exit_schedule_map)
    assert payload["ScheduleID"] == "2150"
    assert "Schedule" not in payload


def test_exit_permission_working_days_falls_back_when_clone_missing():
    sched_map = {"office": "1500"}
    exit_schedule_map = {"office": {"clone_name": "Office - EP"}}
    payload = _payload_for_exit_permission("working_days", sched_map, exit_schedule_map)
    assert payload["ScheduleID"] == "1500"
    assert "Schedule" not in payload
