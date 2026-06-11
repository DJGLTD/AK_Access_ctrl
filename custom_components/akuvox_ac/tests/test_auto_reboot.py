import asyncio
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

import custom_components.akuvox_ac.integration as integration
from custom_components.akuvox_ac.const import CONF_AUTO_REBOOT, DOMAIN
from custom_components.akuvox_ac.reboot_schedule import (
    normalize_reboot_schedule,
    reboot_schedule_is_due,
)


class _ApiStub:
    def __init__(self):
        self.reboot_calls = 0

    async def system_reboot(self):
        self.reboot_calls += 1


class _CoordinatorStub:
    def __init__(self):
        self.health = {"online": True, "status": "online"}
        self.events = []
        self.refresh_calls = 0

    def _append_event(self, event):
        self.events.append(event)

    async def async_request_refresh(self):
        self.refresh_calls += 1


def test_reboot_schedule_defaults_off():
    assert normalize_reboot_schedule(None) == {
        "enabled": False,
        "time": "03:00",
        "days": [],
    }


def test_reboot_schedule_normalizes_days_and_time():
    assert normalize_reboot_schedule(
        {
            "enabled": True,
            "time": "3:05",
            "days": ["Thursday", "tues", "Tuesday"],
        },
        strict=True,
    ) == {
        "enabled": True,
        "time": "03:05",
        "days": ["tue", "thu"],
    }


@pytest.mark.parametrize(
    "schedule,error",
    [
        ({"enabled": True, "time": "25:00", "days": ["tue"]}, "24-hour"),
        ({"enabled": True, "time": "03:00", "days": []}, "at least one"),
        ({"enabled": True, "time": "03:00", "days": ["noday"]}, "Invalid reboot day"),
    ],
)
def test_reboot_schedule_rejects_invalid_enabled_values(schedule, error):
    with pytest.raises(ValueError, match=error):
        normalize_reboot_schedule(schedule, strict=True)


def test_reboot_schedule_due_uses_weekday_and_time():
    schedule = {
        "enabled": True,
        "time": "03:00",
        "days": ["tue", "thu"],
    }
    assert reboot_schedule_is_due(schedule, datetime(2026, 6, 9, 3, 0))
    assert not reboot_schedule_is_due(schedule, datetime(2026, 6, 9, 3, 1))
    assert not reboot_schedule_is_due(schedule, datetime(2026, 6, 10, 3, 0))


def test_scheduled_reboot_targets_only_due_device_once_per_slot():
    due_api = _ApiStub()
    due_coord = _CoordinatorStub()
    other_api = _ApiStub()
    other_coord = _CoordinatorStub()
    root = {
        "due": {
            "api": due_api,
            "coordinator": due_coord,
            "options": {
                CONF_AUTO_REBOOT: {
                    "enabled": True,
                    "time": "03:00",
                    "days": ["tue", "thu"],
                }
            },
        },
        "other": {
            "api": other_api,
            "coordinator": other_coord,
            "options": {
                CONF_AUTO_REBOOT: {
                    "enabled": True,
                    "time": "03:00",
                    "days": ["wed"],
                }
            },
        },
    }
    manager = object.__new__(integration.SyncManager)
    manager.hass = SimpleNamespace(data={DOMAIN: root})
    manager._scheduled_reboot_last_run = {}

    scheduled_time = datetime(2026, 6, 9, 3, 0)
    asyncio.run(manager._scheduled_reboot_cb(scheduled_time))
    asyncio.run(manager._scheduled_reboot_cb(scheduled_time))

    assert due_api.reboot_calls == 1
    assert due_coord.refresh_calls == 1
    assert due_coord.health["status"] == "rebooting"
    assert any("automatic schedule" in event for event in due_coord.events)
    assert other_api.reboot_calls == 0
    assert other_coord.refresh_calls == 0


def test_device_edit_templates_include_auto_reboot_controls():
    www = Path(__file__).resolve().parents[1] / "www"
    for filename in ("device_edit.html", "device_edit-mob.html"):
        html = (www / filename).read_text(encoding="utf-8")
        assert 'id="autoRebootToggle"' in html
        assert 'id="autoRebootTime"' in html
        assert "set_device_auto_reboot" in html
