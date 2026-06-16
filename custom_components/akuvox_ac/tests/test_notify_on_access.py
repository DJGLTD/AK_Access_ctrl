import asyncio
from typing import Any, Dict

from custom_components.akuvox_ac.ha_test_stubs import ensure_homeassistant_stubs

ensure_homeassistant_stubs()

from custom_components.akuvox_ac.integration import (  # noqa: E402
    AkuvoxSettingsStore,
    _sync_notify_on_access_targets,
)


def _settings_store(targets: Dict[str, Any]) -> AkuvoxSettingsStore:
    store = object.__new__(AkuvoxSettingsStore)
    store.data = {"alerts": {"targets": targets}, "expiry_reminders": {"last_sent": {}}}
    store.saved = 0

    async def _async_save():
        store.saved += 1

    store.async_save = _async_save
    return store


class _UsersStoreStub:
    def __init__(self, users: Dict[str, Dict[str, Any]]) -> None:
        self._users = users

    def all(self) -> Dict[str, Dict[str, Any]]:
        return dict(self._users)


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


def test_settings_store_targets_access_expiring_alerts():
    store = _settings_store(
        {
            "mobile_app_lees_iphone": {"access_expiring": True},
            "mobile_app_verns_iphone": {"access_expiring": False},
        }
    )

    assert store.targets_for_event("access_expiring") == ["mobile_app_lees_iphone"]


def test_settings_store_targets_user_profile_change_alerts():
    store = _settings_store(
        {
            "mobile_app_lees_iphone": {"user_changed": True},
            "mobile_app_verns_iphone": {"user_changed": False},
        }
    )

    cleaned = store.get_alert_targets()

    assert cleaned["mobile_app_lees_iphone"]["user_changed"] is True
    assert store.targets_for_event("user_changed", user_id="HA012") == [
        "mobile_app_lees_iphone"
    ]


def test_settings_store_tracks_expiry_reminder_sent_date():
    store = _settings_store({})

    assert store.expiry_reminder_sent("ha-004", "2026-05-21") is False

    asyncio.run(store.mark_expiry_reminder_sent("ha-004", "2026-05-21"))

    assert store.expiry_reminder_sent("HA004", "2026-05-21") is True
    assert store.expiry_reminder_sent("HA004", "2026-05-22") is False
    assert store.saved == 1


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


def test_prune_stale_alert_users_removes_deleted_and_missing_users():
    store = _settings_store(
        {
            "mobile_app_elles_iphone": {
                "granted": {
                    "any": False,
                    "specific": True,
                    "users": ["HA004", "TMP002", "tmp-003", "HA099", "bad-id"],
                }
            }
        }
    )
    users = _UsersStoreStub(
        {
            "HA004": {"name": "Daniel", "status": "active"},
            "TMP003": {"name": "Visitor", "status": "active", "temporary": True},
            "HA099": {"name": "Deleted", "status": "deleted"},
        }
    )

    changed = asyncio.run(store.prune_stale_alert_users(users))

    assert changed is True
    granted = store.get_alert_targets()["mobile_app_elles_iphone"]["granted"]
    assert granted["specific"] is True
    assert granted["users"] == ["HA004", "TMP003"]
    assert store.saved == 1


def test_prune_stale_alert_users_disables_empty_specific_list():
    store = _settings_store(
        {
            "mobile_app_elles_iphone": {
                "granted": {"any": False, "specific": True, "users": ["TMP002"]}
            }
        }
    )

    changed = asyncio.run(store.prune_stale_alert_users(_UsersStoreStub({})))

    assert changed is True
    granted = store.get_alert_targets()["mobile_app_elles_iphone"]["granted"]
    assert granted["specific"] is False
    assert granted["users"] == []


def test_dashboard_access_sanitizes_allowed_user_ids():
    store = _settings_store({})

    updated = asyncio.run(
        store.set_dashboard_access(
            {"allowed_user_ids": [" user-a ", "", "user-a", "user-b"]}
        )
    )

    assert updated == {"allowed_user_ids": ["user-a", "user-b"]}
    assert store.get_dashboard_access() == {"allowed_user_ids": ["user-a", "user-b"]}
    assert store.saved == 1
