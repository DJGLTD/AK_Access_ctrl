import asyncio
from pathlib import Path
from typing import Any, Dict

from custom_components.akuvox_ac.ha_test_stubs import ensure_homeassistant_stubs

ensure_homeassistant_stubs()

from custom_components.akuvox_ac import http as http_module  # noqa: E402
from custom_components.akuvox_ac.integration import AkuvoxSettingsStore  # noqa: E402


def _settings_store(data: Dict[str, Any]) -> AkuvoxSettingsStore:
    store = object.__new__(AkuvoxSettingsStore)
    store.data = data
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


def test_settings_store_sanitizes_event_visibility_rules():
    store = _settings_store(
        {
            "event_visibility": {
                "users": {
                    "ha-002": ["HA001", "HA-001", "HA002", "TMP-3", "not-a-user"],
                    "not-a-viewer": ["HA001"],
                }
            }
        }
    )

    assert store.get_event_visibility() == {
        "users": {"HA002": ["HA001", "TMP003"]}
    }
    assert store.event_visible_user_ids("ha-002") == ["HA002", "HA001", "TMP003"]


def test_prune_stale_event_visibility_users_removes_deleted_and_missing_users():
    store = _settings_store(
        {"event_visibility": {"users": {"HA002": ["HA001", "TMP003"], "HA004": ["HA001"]}}}
    )

    changed = asyncio.run(
        store.prune_stale_event_visibility_users(
            _UsersStoreStub(
                {
                    "HA001": {"name": "Daniel", "status": "active"},
                    "HA002": {"name": "Lee", "status": "active"},
                    "TMP003": {"name": "Deleted", "status": "deleted"},
                }
            )
        )
    )

    assert changed is True
    assert store.get_event_visibility() == {"users": {"HA002": ["HA001"]}}
    assert store.saved == 1


def test_event_filter_matches_only_visible_users():
    users = {
        "HA001": {
            "name": "Daniel",
            "ha_user_name": "DJGLTD",
            "status": "active",
        },
        "HA002": {"name": "Lee", "status": "active"},
        "HA003": {"name": "Hidden", "status": "active"},
    }
    events = [
        {"UserID": "HA001", "Event": "Daniel pin"},
        {"LinkedUserID": "HA002", "Event": "Lee opened"},
        {"UserName": "Daniel", "Event": "Daniel face"},
        {"HomeAssistantUserName": "DJGLTD", "Event": "HA open"},
        {"UserID": "HA003", "Event": "Hidden user"},
        {"Event": "Device came online"},
    ]

    filtered = http_module._filter_events_for_visible_users(
        events,
        users,
        {"HA001", "HA002"},
    )

    assert [event["Event"] for event in filtered] == [
        "Daniel pin",
        "Lee opened",
        "Daniel face",
        "HA open",
    ]


def test_event_history_pages_include_user_filter_selector():
    www = Path(__file__).resolve().parents[1] / "www"

    for page_name in ("event_history.html", "event_history-mob.html"):
        html = (www / page_name).read_text(encoding="utf-8")
        assert 'id="userFilter"' in html
        assert "function populateUserFilter(" in html
        assert "state?.event_access" in html
