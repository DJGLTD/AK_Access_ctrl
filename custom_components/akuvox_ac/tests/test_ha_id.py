from custom_components.akuvox_ac.ha_id import (
    normalize_ha_id,
    normalize_temp_id,
    normalize_user_id,
)


def test_normalize_ha_id_returns_padded_canonical_id():
    assert normalize_ha_id("HA1") == "HA001"
    assert normalize_ha_id("ha-12") == "HA012"
    assert normalize_ha_id("HA123") == "HA123"


def test_normalize_temp_id_returns_padded_canonical_id():
    assert normalize_temp_id("TMP1") == "TMP001"
    assert normalize_temp_id("tmp-12") == "TMP012"


def test_normalize_user_id_prefers_supported_namespaces():
    assert normalize_user_id("HA7") == "HA007"
    assert normalize_user_id("TMP7") == "TMP007"
