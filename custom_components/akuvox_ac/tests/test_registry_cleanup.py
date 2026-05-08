from custom_components.akuvox_ac.ha_test_stubs import ensure_homeassistant_stubs

ensure_homeassistant_stubs()

from custom_components.akuvox_ac.integration import (  # noqa: E402
    _is_obsolete_akuvox_entity,
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


class _EntityEntry:
    def __init__(
        self,
        *,
        entry_id: str,
        domain: str,
        entity_id: str,
        unique_id: str,
        platform: str = "akuvox_ac",
        name: str = "",
    ) -> None:
        self.config_entry_id = entry_id
        self.domain = domain
        self.entity_id = entity_id
        self.unique_id = unique_id
        self.platform = platform
        self.name = name


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


def test_obsolete_akuvox_entity_matches_retired_button():
    entity = _EntityEntry(
        entry_id="entry-1",
        domain="button",
        entity_id="button.gate_access_denied",
        unique_id="entry-1_access_denied",
        name="Gate Access Denied",
    )

    assert _is_obsolete_akuvox_entity(entity, "entry-1") is True


def test_obsolete_akuvox_entity_matches_retired_sensor():
    entity = _EntityEntry(
        entry_id="entry-1",
        domain="sensor",
        entity_id="sensor.gate_granted_access_key_holder",
        unique_id="entry-1_granted_access_key_holder",
        name="Gate Granted Access Key Holder",
    )

    assert _is_obsolete_akuvox_entity(entity, "entry-1") is True


def test_obsolete_akuvox_entity_matches_retired_sync_sensor():
    entity = _EntityEntry(
        entry_id="entry-1",
        domain="sensor",
        entity_id="sensor.gate_sync",
        unique_id="entry-1_sync",
        name="Gate Sync",
    )

    assert _is_obsolete_akuvox_entity(entity, "entry-1") is True


def test_obsolete_akuvox_entity_keeps_current_access_sensor():
    entity = _EntityEntry(
        entry_id="entry-1",
        domain="sensor",
        entity_id="sensor.gate_last_access_user",
        unique_id="entry-1_last_access_user",
        name="Gate Last Access User",
    )

    assert _is_obsolete_akuvox_entity(entity, "entry-1") is False


def test_obsolete_akuvox_entity_keeps_current_last_sync_sensor():
    entity = _EntityEntry(
        entry_id="entry-1",
        domain="sensor",
        entity_id="sensor.gate_last_sync",
        unique_id="entry-1_last_sync",
        name="Gate Last Sync",
    )

    assert _is_obsolete_akuvox_entity(entity, "entry-1") is False


def test_obsolete_akuvox_entity_requires_akuvox_platform_and_entry():
    entity = _EntityEntry(
        entry_id="other-entry",
        domain="sensor",
        entity_id="sensor.gate_users",
        unique_id="other-entry_users",
        platform="other_platform",
        name="Gate Users",
    )

    assert _is_obsolete_akuvox_entity(entity, "entry-1") is False
