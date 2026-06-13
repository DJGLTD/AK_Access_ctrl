from pathlib import Path
from types import SimpleNamespace

from custom_components.akuvox_ac.const import (
    AKUVOX_DEVICE_MODELS,
    CONF_DEVICE_MODEL,
    DEFAULT_DEVICE_MODEL,
)
from custom_components.akuvox_ac.http import _serialize_devices


WWW = Path(__file__).resolve().parents[1] / "www"


def test_device_model_is_serialized_for_dashboard():
    coordinator = SimpleNamespace(
        device_name="Front Gate",
        health={
            "device_type": "Intercom",
            "device_model": "R29",
            "ip": "10.30.0.73",
            "online": True,
        },
        events=[],
        users=[],
    )
    devices, _ = _serialize_devices(
        {
            "entry-1": {
                "coordinator": coordinator,
                "options": {CONF_DEVICE_MODEL: "R29"},
            }
        }
    )

    assert devices[0]["model"] == "R29"


def test_device_model_falls_back_for_existing_entries():
    coordinator = SimpleNamespace(
        device_name="Legacy Gate",
        health={"device_type": "Intercom", "online": True},
        events=[],
        users=[],
    )
    devices, _ = _serialize_devices(
        {"entry-1": {"coordinator": coordinator, "options": {}}}
    )

    assert devices[0]["model"] == DEFAULT_DEVICE_MODEL


def test_model_selectors_and_artwork_are_bundled():
    assert "R29" in AKUVOX_DEVICE_MODELS
    assert "A08" in AKUVOX_DEVICE_MODELS

    dashboard = (WWW / "index.html").read_text(encoding="utf-8")
    assert 'id="deviceOverview"' in dashboard
    assert "const DEVICE_MODEL_ASSETS" in dashboard
    assert "/api/AK_AC/device-models/${asset}.svg" in dashboard

    for filename in ("device_edit.html", "device_edit-mob.html"):
        editor = (WWW / filename).read_text(encoding="utf-8")
        assert 'id="modelSelect"' in editor
        assert "action: 'set_device_model'" in editor

    artwork = WWW / "device-models"
    for filename in (
        "generic.svg",
        "screen-tall.svg",
        "keypad-door.svg",
        "compact-door.svg",
        "face-slim.svg",
        "access-keypad.svg",
        "controller.svg",
    ):
        assert (artwork / filename).is_file()
