from pathlib import Path
import sys
import types

# Ensure the repository root is on the path so component modules are importable.
ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from . import test_exit_permissions  # noqa: F401

ha_module = types.ModuleType("homeassistant")
const_module = types.ModuleType("homeassistant.const")


class _Platform:
    SENSOR = "sensor"
    BUTTON = "button"
    BINARY_SENSOR = "binary_sensor"
    UPDATE = "update"


const_module.Platform = _Platform

sys.modules.setdefault("homeassistant", ha_module)
sys.modules.setdefault("homeassistant.const", const_module)

from custom_components.akuvox_ac.api import AkuvoxAPI


def _extract(payload):
    return AkuvoxAPI._extract_doorlog_items(payload)


def test_extract_doorlog_items_handles_nested_item_list():
    payload = {
        "data": {
            "item": [
                {"Event": "Granted", "Time": "2024-04-10T12:00:00"},
                {"Event": "Denied", "Time": "2024-04-10T11:00:00"},
            ]
        }
    }

    items = _extract(payload)

    assert len(items) == 2
    assert items[0]["Event"] == "Granted"


def test_extract_doorlog_items_handles_single_item_dict():
    payload = {"data": {"item": {"Event": "Opened", "LogTime": "2024-04-10 10:00:00"}}}

    items = _extract(payload)

    assert len(items) == 1
    assert items[0]["LogTime"] == "2024-04-10 10:00:00"


def test_extract_doorlog_items_walks_alternate_keys():
    payload = {
        "Data": {
            "Rows": {
                "Row": [
                    {"EventType": "Access", "Timestamp": "2024-04-10T09:00:00"},
                    {"EventType": "Access", "Timestamp": "2024-04-10T08:00:00"},
                ]
            }
        }
    }

    items = _extract(payload)

    assert len(items) == 2
    assert items[0]["Timestamp"] == "2024-04-10T09:00:00"


def test_extract_doorlog_items_accepts_direct_event_dict():
    payload = {"Event": "Access granted", "LogID": "123", "Time": "2024-04-10T07:00:00"}

    items = _extract(payload)

    assert len(items) == 1
    assert items[0]["LogID"] == "123"


def test_extract_doorlog_items_returns_empty_for_unknown_payload():
    assert _extract({"data": {"unexpected": []}}) == []
