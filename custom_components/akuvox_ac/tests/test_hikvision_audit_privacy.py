import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from custom_components.akuvox_ac.ha_test_stubs import ensure_homeassistant_stubs

ensure_homeassistant_stubs()

from custom_components.akuvox_ac.const import DOMAIN
from custom_components.akuvox_ac.hikvision import _record_anpr_action


class _Store:
    async def async_save(self, _data):
        return None


class _Bus:
    def __init__(self):
        self.events = []

    def async_fire(self, event_type, event):
        self.events.append((event_type, dict(event)))


def _hass(data=None):
    return SimpleNamespace(
        data={DOMAIN: {"anpr_audit_store": _Store(), "anpr_audit_data": data or {"events": []}}},
        bus=_Bus(),
    )


def test_anpr_audit_masks_vehicle_registration_values():
    hass = _hass()
    event = {
        "entry_id": "camera-1",
        "action": "update",
        "old_plate": "AB12CDE",
        "plate": "XY98ZZZ",
        "outcome": "success",
    }

    asyncio.run(_record_anpr_action(hass, event))

    stored = hass.data[DOMAIN]["anpr_audit_data"]["events"][0]
    assert stored["plate"] == "***ZZZ"
    assert stored["old_plate"] == "***CDE"
    assert "AB12CDE" not in repr(stored)
    assert "XY98ZZZ" not in repr(stored)


def test_anpr_audit_prunes_old_events_and_redacts_existing_records():
    recent = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
    old = (datetime.now(timezone.utc) - timedelta(days=120)).isoformat()
    hass = _hass(
        {
            "events": [
                {"timestamp": old, "plate": "OLD1234", "action": "add"},
                {"timestamp": recent, "plate": "NEW5678", "action": "remove"},
            ]
        }
    )

    asyncio.run(
        _record_anpr_action(
            hass,
            {"entry_id": "camera-1", "action": "add", "plate": "AB12CDE", "outcome": "success"},
        )
    )

    events = hass.data[DOMAIN]["anpr_audit_data"]["events"]
    assert len(events) == 2
    assert events[0]["plate"] == "***678"
    assert events[1]["plate"] == "***CDE"
    assert all("OLD1234" not in repr(event) for event in events)
