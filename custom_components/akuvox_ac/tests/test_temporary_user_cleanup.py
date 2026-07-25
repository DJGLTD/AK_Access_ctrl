import asyncio
from datetime import datetime
from types import SimpleNamespace

from custom_components.akuvox_ac.ha_test_stubs import ensure_homeassistant_stubs

ensure_homeassistant_stubs()

import custom_components.akuvox_ac.integration as integration  # noqa: E402


class _UsersStore:
    def __init__(self, users):
        self.users = users
        self.upserts = []

    def all(self):
        return self.users

    async def upsert_profile(self, key, **kwargs):
        self.upserts.append((key, kwargs))
        self.users.setdefault(key, {}).update(kwargs)


class _Queue:
    def __init__(self):
        self.calls = []

    def mark_change(self, *args, **kwargs):
        self.calls.append((args, kwargs))


def _fixed_now():
    return datetime(2026, 7, 25, 12, 0, tzinfo=integration.dt_util.DEFAULT_TIME_ZONE)


def _manager(store):
    manager = object.__new__(integration.SyncManager)
    manager.hass = SimpleNamespace(config=SimpleNamespace(path=lambda *parts: "/tmp"))
    manager._temp_cleanup_lock = asyncio.Lock()
    queue = _Queue()
    manager._users_store = lambda: store
    manager._root = lambda: {"sync_queue": queue}
    manager._devices = lambda: []
    manager._remove_face_files_for_user = lambda _ids: None

    async def _prune_stale_alert_users():
        return None

    manager._prune_stale_alert_users = _prune_stale_alert_users
    return manager, queue


def test_temporary_retention_elapsed_after_seven_days():
    profile = {
        "temporary": True,
        "temporary_expires_at": "2026-07-12T10:41:00+00:00",
    }

    assert integration.SyncManager._temporary_retention_elapsed(profile, now=_fixed_now())


def test_temporary_retention_keeps_recently_expired_user():
    profile = {
        "temporary": True,
        "temporary_expires_at": "2026-07-22T10:41:00+00:00",
    }

    assert not integration.SyncManager._temporary_retention_elapsed(profile, now=_fixed_now())


def test_retained_temporary_cleanup_deletes_only_old_expired_temps(monkeypatch):
    monkeypatch.setattr(integration.dt_util, "now", _fixed_now)
    store = _UsersStore(
        {
            "TMP001": {
                "name": "Henry toilets",
                "temporary": True,
                "temporary_expires_at": "2026-07-12T10:41:00+00:00",
                "access_end": "2026-07-12",
                "status": "active",
            },
            "TMP002": {
                "name": "Recent visitor",
                "temporary": True,
                "temporary_expires_at": "2026-07-22T10:41:00+00:00",
                "access_end": "2026-07-22",
                "status": "active",
            },
            "HA001": {
                "name": "Normal expired user",
                "access_end": "2026-07-12",
                "status": "active",
            },
        }
    )
    manager, queue = _manager(store)

    asyncio.run(manager._cleanup_retained_temporary_users(reason="test"))

    assert store.users["TMP001"]["status"] == "deleted"
    assert store.users["TMP001"]["schedule_name"] == "No Access"
    assert store.users["TMP002"]["status"] == "active"
    assert store.users["HA001"]["status"] == "active"
    assert queue.calls
    assert queue.calls[-1][1]["trigger"] == "expired user cleanup: TMP001"


def test_general_expired_access_cleanup_skips_temporary_users(monkeypatch):
    monkeypatch.setattr(integration.dt_util, "now", _fixed_now)
    store = _UsersStore(
        {
            "TMP001": {
                "name": "Expired temporary",
                "temporary": True,
                "access_end": "2026-07-12",
                "status": "active",
            },
            "HA001": {
                "name": "Expired standard",
                "access_end": "2026-07-12",
                "status": "active",
            },
        }
    )
    manager, _queue = _manager(store)

    asyncio.run(manager._cleanup_expired_access_users(reason="test"))

    assert store.users["TMP001"]["status"] == "active"
    assert store.users["HA001"]["status"] == "deleted"
