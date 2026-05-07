from custom_components.akuvox_ac.ha_test_stubs import ensure_homeassistant_stubs

ensure_homeassistant_stubs()

from custom_components.akuvox_ac.integration import (  # noqa: E402
    _is_legacy_integration_device,
)


class _Device:
    def __init__(
        self,
        *,
        entry_id: str,
        name: str,
        model: str,
        identifiers=None,
    ) -> None:
        self.config_entries = {entry_id}
        self.name = name
        self.model = model
        self.identifiers = identifiers or set()


def test_legacy_integration_device_matches_empty_home_assistant_device():
    device = _Device(
        entry_id="entry-1",
        name="Akuvox Access Control",
        model="Home Assistant Integration",
    )

    assert _is_legacy_integration_device(device, "entry-1") is True


def test_legacy_integration_device_does_not_match_real_gate_device():
    device = _Device(
        entry_id="entry-1",
        name="Gate",
        model="Intercom",
        identifiers={("akuvox_ac", "entry-1")},
    )

    assert _is_legacy_integration_device(device, "entry-1") is False


def test_legacy_integration_device_requires_same_config_entry():
    device = _Device(
        entry_id="other-entry",
        name="Akuvox Access Control",
        model="Home Assistant Integration",
    )

    assert _is_legacy_integration_device(device, "entry-1") is False
