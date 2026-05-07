from typing import Any, Dict

from custom_components.akuvox_ac.ha_test_stubs import ensure_homeassistant_stubs

ensure_homeassistant_stubs()

from custom_components.akuvox_ac.integration import (  # noqa: E402
    AkuvoxSettingsStore,
    _sync_notify_on_access_targets,
)


def _settings_store(targets: Dict[str, Any]) -> AkuvoxSettingsStore:
    store = object.__new__(AkuvoxSettingsStore)
    store.data = {"alerts": {"targets": targets}}
    return store


def test_settings_store_matches_normalized_specific_notify_user():
    store = _settings_store(
        {
            "mobile_app_lees_iphone": {
                "granted": {"specific": True, "users": ["ha-012"]}
            }
        }
    )

    assert store.targets_for_event("user_granted", user_id="HA012") == [
        "mobile_app_lees_iphone"
    ]


def test_settings_store_preserves_disabled_specific_user_list():
    store = _settings_store(
        {
            "mobile_app_lees_iphone": {
                "granted": {"specific": False, "users": ["HA012"]}
            }
        }
    )

    cleaned = store.get_alert_targets()

    assert cleaned["mobile_app_lees_iphone"]["granted"]["specific"] is False
    assert store.targets_for_event("user_granted", user_id="HA012") == []


def test_settings_store_treats_legacy_user_list_as_specific():
    store = _settings_store(
        {
            "mobile_app_lees_iphone": {
                "granted": {"users": ["HA012"]}
            }
        }
    )

    cleaned = store.get_alert_targets()

    assert cleaned["mobile_app_lees_iphone"]["granted"]["specific"] is True
    assert store.targets_for_event("user_granted", user_id="ha-012") == [
        "mobile_app_lees_iphone"
    ]


def test_sync_notify_on_access_targets_replaces_per_user_targets():
    targets = {
        "mobile_app_lees_iphone": {
            "granted": {"any": False, "specific": True, "users": ["HA012", "HA099"]}
        },
        "mobile_app_verns_iphone": {
            "granted": {"any": True, "specific": False, "users": []}
        },
    }

    updated, changed = _sync_notify_on_access_targets(
        targets,
        "ha-012",
        enabled=True,
        selected_targets=["notify.mobile_app_verns_iphone"],
    )

    assert changed is True
    assert updated["mobile_app_lees_iphone"]["granted"]["users"] == ["HA099"]
    assert updated["mobile_app_lees_iphone"]["granted"]["specific"] is True
    assert updated["mobile_app_verns_iphone"]["granted"]["users"] == ["HA012"]
    assert updated["mobile_app_verns_iphone"]["granted"]["specific"] is True
    assert updated["mobile_app_verns_iphone"]["granted"]["any"] is True
