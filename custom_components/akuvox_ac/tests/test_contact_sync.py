"""Tests for contact synchronization helpers."""

from datetime import timedelta
from types import SimpleNamespace

# Importing this module first sets up Home Assistant stubs.
from . import test_exit_permissions  # noqa: F401

import custom_components.akuvox_ac.integration as integration


class _ApiStub:
    def __init__(self, contacts):
        self._contacts = contacts
        self.add_calls = []
        self.delete_calls = []

    async def contact_get(self):
        return {"data": {"item": list(self._contacts)}}

    async def contact_add(self, items):
        self.add_calls.append(items)

    async def contact_delete(self, items):
        self.delete_calls.append(items)


class _ReplaceApiStub:
    def __init__(self):
        self.delete_calls = []
        self.add_calls = []

    async def user_list(self):
        return [
            {
                "ID": "42",
                "UserID": "user1",
                "Name": "User One",
                "Group": integration.HA_CONTACT_GROUP_NAME,
            }
        ]

    async def user_delete(self, value):
        self.delete_calls.append(value)

    async def user_add(self, items):
        self.add_calls.append(items)


class _FaceApiStub:
    def __init__(self, *, face_status_after_add="1"):
        self.upload_calls = []
        self.add_calls = []
        self.set_calls = []
        self.delete_calls = []
        self.face_status_after_add = face_status_after_add
        self._users = []

    async def face_upload(self, face_bytes, *, filename):
        self.upload_calls.append({"bytes": face_bytes, "filename": filename})
        return {"path": "/mnt/Face/HA001.jpg"}

    async def user_delete(self, value):
        self.delete_calls.append(value)
        text = str(value)
        self._users = [
            user
            for user in self._users
            if text
            not in {
                str(user.get("ID") or ""),
                str(user.get("UserID") or ""),
                str(user.get("Name") or ""),
            }
        ]

    async def user_add(self, items):
        self.add_calls.append(items)
        for item in items:
            record = dict(item)
            record.setdefault("ID", str(len(self._users) + 100))
            record["FaceStatus"] = self.face_status_after_add
            self._users = [
                user
                for user in self._users
                if str(user.get("UserID") or "") != str(record.get("UserID") or "")
            ]
            self._users.append(record)

    async def user_set(self, items):
        self.set_calls.append(items)

    async def user_list(self):
        return [dict(user) for user in self._users]


class _OrderedFaceApiStub(_FaceApiStub):
    def __init__(self, *, face_status_after_add="1"):
        super().__init__(face_status_after_add=face_status_after_add)
        self.events = []

    async def face_upload(self, face_bytes, *, filename):
        self.events.append(("upload", filename))
        return await super().face_upload(face_bytes, filename=filename)

    async def user_delete(self, value):
        self.events.append(("delete", str(value)))
        await super().user_delete(value)

    async def user_add(self, items):
        self.events.append(("add", [dict(item) for item in items]))
        await super().user_add(items)


class _FaceHassStub:
    def __init__(self, root):
        self._root = root
        self.data = {integration.DOMAIN: {}}

    @property
    def config(self):
        root = self._root

        class _Config:
            def path(self, *parts):
                return str(root.joinpath(*parts))

        return _Config()

    async def async_add_executor_job(self, func, *args):
        return func(*args)


class _UsersStoreStub:
    def __init__(self):
        self.upserts = []

    async def upsert_profile(self, key, **kwargs):
        self.upserts.append((key, kwargs))


def _make_manager():
    hass = SimpleNamespace(data={integration.DOMAIN: {}}, config=SimpleNamespace(path=lambda *parts: "/tmp"))
    return integration.SyncManager(hass)


def _assert_fresh_face_filename(filename, ha_key="HA001"):
    assert filename.startswith(f"{ha_key}_")
    assert filename.endswith(".jpg")


def test_sync_contacts_adds_missing_contact():
    manager = _make_manager()
    api = _ApiStub([])

    import asyncio

    asyncio.run(manager._sync_contacts_for_profiles(api, [("Jane Doe", "+1 (555) 111-2222")]))

    assert api.add_calls == [[{
        "Name": "Jane Doe",
        "Phone": "+1 (555) 111-2222",
        "PhoneNum": "+1 (555) 111-2222",
        "Group": integration.HA_CONTACT_GROUP_NAME,
    }]]
    assert api.delete_calls == []


def test_sync_contacts_replaces_contact_when_phone_changes():
    manager = _make_manager()
    api = _ApiStub(
        [
            {
                "Name": "Jane Doe",
                "Phone": "5551112222",
                "Group": integration.HA_CONTACT_GROUP_NAME,
            }
        ]
    )

    import asyncio

    asyncio.run(manager._sync_contacts_for_profiles(api, [("Jane Doe", "5551119999")]))

    assert api.delete_calls == [[{"Name": "Jane Doe", "Group": integration.HA_CONTACT_GROUP_NAME}]]
    assert api.add_calls == [[{
        "Name": "Jane Doe",
        "Phone": "5551119999",
        "PhoneNum": "5551119999",
        "Group": integration.HA_CONTACT_GROUP_NAME,
    }]]


def test_sync_contacts_prunes_removed_contact_names():
    manager = _make_manager()
    api = _ApiStub(
        [
            {
                "Name": "Old Name",
                "Phone": "5551112222",
                "Group": integration.HA_CONTACT_GROUP_NAME,
            },
            {
                "Name": "Other System",
                "Phone": "5553334444",
                "Group": "Not Home Assistant",
            },
        ]
    )

    import asyncio

    asyncio.run(
        manager._sync_contacts_for_profiles(
            api,
            [("New Name", "5551112222")],
            prune_extra=True,
        )
    )

    assert api.delete_calls == [[{"Name": "Old Name", "Group": integration.HA_CONTACT_GROUP_NAME}]]
    assert api.add_calls == [[{
        "Name": "New Name",
        "Phone": "5551112222",
        "PhoneNum": "5551112222",
        "Group": integration.HA_CONTACT_GROUP_NAME,
    }]]


def test_sync_contacts_prunes_all_when_no_profiles_need_contacts():
    manager = _make_manager()
    api = _ApiStub(
        [
            {
                "Name": "Jane Doe",
                "Phone": "5551112222",
                "Group": integration.HA_CONTACT_GROUP_NAME,
            }
        ]
    )

    import asyncio

    asyncio.run(manager._sync_contacts_for_profiles(api, [], prune_extra=True))

    assert api.delete_calls == [[{"Name": "Jane Doe", "Group": integration.HA_CONTACT_GROUP_NAME}]]
    assert api.add_calls == []


def test_delete_contacts_matches_normalized_phone():
    manager = _make_manager()
    api = _ApiStub(
        [
            {
                "Name": "Jane Doe",
                "Phone": "+1 (555) 111-2222",
                "Group": integration.HA_CONTACT_GROUP_NAME,
            }
        ]
    )

    import asyncio

    asyncio.run(manager._delete_contacts(api, name=None, phone="15551112222"))

    assert api.delete_calls == [[{"Name": "Jane Doe", "Group": integration.HA_CONTACT_GROUP_NAME}]]


def test_replace_user_on_device_deletes_existing_before_add():
    manager = _make_manager()
    api = _ReplaceApiStub()

    import asyncio

    asyncio.run(
        manager._replace_user_on_device(
            api,
            "user1",
            {
                "UserID": "user1",
                "Name": "User One",
                "Group": integration.HA_CONTACT_GROUP_NAME,
            },
            existing=None,
        )
    )

    assert api.delete_calls == ["42", "user1", "User One"]
    assert api.add_calls == [[{
        "UserID": "user1",
        "Name": "User One",
        "Group": integration.HA_CONTACT_GROUP_NAME,
        "DialAccount": "0",
        "AnalogSystem": "0",
        "AnalogNumber": "",
        "AnalogReplace": "",
        "AnalogProxyAddress": "",
    }]]


def test_upload_face_asset_prefers_local_file_over_remote_face_url(tmp_path):
    hass = _FaceHassStub(tmp_path)
    users_store = _UsersStoreStub()
    hass.data[integration.DOMAIN]["users_store"] = users_store
    manager = integration.SyncManager(hass)
    api = _FaceApiStub()
    coord = SimpleNamespace(
        health={"device_type": "intercom"},
        events=[],
        _append_event=lambda item: None,
    )

    face_dir = tmp_path / integration.DOMAIN / "FaceData"
    face_dir.mkdir(parents=True)
    (face_dir / "HA001.jpg").write_bytes(b"face-bytes")

    import asyncio

    uploaded = asyncio.run(
        manager._upload_face_asset_to_device(
            api,
            coord,
            "HA001",
            {
                "UserID": "HA001",
                "Name": "Lee Fletcher",
                "Group": integration.HA_CONTACT_GROUP_NAME,
                "FaceUrl": "http://ha.local/api/AK_AC/FaceData/HA001.jpg",
                "FaceRegister": 1,
            },
            {"face_status": "pending"},
            existing={"ID": "42", "UserID": "HA001", "Name": "Lee Fletcher"},
            force=True,
        )
    )

    assert uploaded is True
    uploaded_filename = api.upload_calls[0]["filename"]
    _assert_fresh_face_filename(uploaded_filename)
    assert api.upload_calls == [{"bytes": b"face-bytes", "filename": uploaded_filename}]
    assert api.delete_calls == ["42", "HA001", "Lee Fletcher"]
    assert api.add_calls[0][0]["FaceFileName"] == uploaded_filename
    assert api.add_calls[0][0]["importFile"] == {"fileName": uploaded_filename, "fileData": {}}
    assert api.add_calls[0][0]["FaceRegister"] == 1
    assert api.set_calls == []
    assert users_store.upserts[-1][0] == "HA001"
    assert users_store.upserts[-1][1]["face_status"] == "active"
    assert users_store.upserts[-1][1]["face_synced_at"]
    assert users_store.upserts[-1][1]["face_error_count"] == 0
    assert users_store.upserts[-1][1]["face_retry_after"] == ""


def test_upload_face_asset_uses_local_file_without_face_url(tmp_path):
    hass = _FaceHassStub(tmp_path)
    users_store = _UsersStoreStub()
    hass.data[integration.DOMAIN]["users_store"] = users_store
    manager = integration.SyncManager(hass)
    api = _FaceApiStub()
    coord = SimpleNamespace(
        health={"device_type": "intercom"},
        events=[],
        _append_event=lambda item: None,
    )

    face_dir = tmp_path / integration.DOMAIN / "FaceData"
    face_dir.mkdir(parents=True)
    (face_dir / "HA001.jpg").write_bytes(b"face-bytes")

    import asyncio

    uploaded = asyncio.run(
        manager._upload_face_asset_to_device(
            api,
            coord,
            "HA001",
            {
                "UserID": "HA001",
                "Name": "Lee Fletcher",
                "Group": integration.HA_CONTACT_GROUP_NAME,
            },
            {"face_status": "error"},
            force=True,
        )
    )

    assert uploaded is True
    uploaded_filename = api.upload_calls[0]["filename"]
    _assert_fresh_face_filename(uploaded_filename)
    assert api.upload_calls == [{"bytes": b"face-bytes", "filename": uploaded_filename}]
    assert api.add_calls[0][0]["FaceFileName"] == uploaded_filename
    assert api.add_calls[0][0]["importFile"] == {"fileName": uploaded_filename, "fileData": {}}
    assert api.add_calls[0][0]["FaceRegister"] == 1


def test_upload_face_asset_stays_pending_when_device_does_not_activate(tmp_path):
    hass = _FaceHassStub(tmp_path)
    users_store = _UsersStoreStub()
    hass.data[integration.DOMAIN]["users_store"] = users_store
    manager = integration.SyncManager(hass)
    manager._face_enroll_initial_delay_seconds = 0
    manager._face_enroll_poll_timeout_seconds = 0
    api = _FaceApiStub(face_status_after_add="0")
    coord = SimpleNamespace(
        health={"device_type": "intercom"},
        events=[],
        _append_event=lambda item: coord.events.append(item),
    )

    face_dir = tmp_path / integration.DOMAIN / "FaceData"
    face_dir.mkdir(parents=True)
    (face_dir / "HA001.jpg").write_bytes(b"face-bytes")

    import asyncio

    uploaded = asyncio.run(
        manager._upload_face_asset_to_device(
            api,
            coord,
            "HA001",
            {
                "UserID": "HA001",
                "Name": "Lee Fletcher",
                "Group": integration.HA_CONTACT_GROUP_NAME,
            },
            {"face_status": "error"},
            force=True,
        )
    )

    assert uploaded is True
    assert users_store.upserts[-1][0] == "HA001"
    assert users_store.upserts[-1][1]["face_status"] == "pending"
    assert users_store.upserts[-1][1]["face_synced_at"] == ""
    assert users_store.upserts[-1][1]["face_retry_after"]
    assert any("waiting for device activation" in event for event in coord.events)


def test_upload_face_asset_preserves_already_active_device_face(tmp_path):
    hass = _FaceHassStub(tmp_path)
    users_store = _UsersStoreStub()
    hass.data[integration.DOMAIN]["users_store"] = users_store
    manager = integration.SyncManager(hass)
    api = _FaceApiStub()
    coord = SimpleNamespace(
        health={"device_type": "intercom"},
        events=[],
        _append_event=lambda item: None,
    )

    import asyncio

    uploaded = asyncio.run(
        manager._upload_face_asset_to_device(
            api,
            coord,
            "HA001",
            {
                "UserID": "HA001",
                "Name": "Lee Fletcher",
                "FaceFileName": "HA001.jpg",
                "FaceRegister": 1,
            },
            {"face_status": "pending"},
            existing={"ID": "42", "UserID": "HA001", "Name": "Lee Fletcher", "FaceStatus": "1"},
            force=True,
        )
    )

    assert uploaded is True
    assert api.upload_calls == []
    assert api.delete_calls == []
    assert api.add_calls == []
    assert users_store.upserts[-1][0] == "HA001"
    assert users_store.upserts[-1][1]["face_status"] == "active"
    assert users_store.upserts[-1][1]["face_retry_after"] == ""


def test_upload_face_asset_skips_retry_cooldown(tmp_path):
    hass = _FaceHassStub(tmp_path)
    users_store = _UsersStoreStub()
    hass.data[integration.DOMAIN]["users_store"] = users_store
    manager = integration.SyncManager(hass)
    api = _FaceApiStub()
    coord = SimpleNamespace(
        health={"device_type": "intercom"},
        events=[],
        _append_event=lambda item: None,
    )

    face_dir = tmp_path / integration.DOMAIN / "FaceData"
    face_dir.mkdir(parents=True)
    (face_dir / "HA001.jpg").write_bytes(b"face-bytes")

    import asyncio

    retry_after = (integration.dt_util.now() + timedelta(minutes=10)).isoformat()
    uploaded = asyncio.run(
        manager._upload_face_asset_to_device(
            api,
            coord,
            "HA001",
            {
                "UserID": "HA001",
                "Name": "Lee Fletcher",
                "FaceFileName": "HA001.jpg",
                "FaceRegister": 1,
            },
            {"face_status": "pending", "face_retry_after": retry_after},
            force=False,
        )
    )

    assert uploaded is False
    assert api.upload_calls == []
    assert api.delete_calls == []
    assert api.add_calls == []
    assert users_store.upserts == []


def test_upload_face_asset_force_bypasses_retry_cooldown(tmp_path):
    hass = _FaceHassStub(tmp_path)
    users_store = _UsersStoreStub()
    hass.data[integration.DOMAIN]["users_store"] = users_store
    manager = integration.SyncManager(hass)
    api = _FaceApiStub()
    coord = SimpleNamespace(
        health={"device_type": "intercom"},
        events=[],
        _append_event=lambda item: None,
    )

    face_dir = tmp_path / integration.DOMAIN / "FaceData"
    face_dir.mkdir(parents=True)
    (face_dir / "HA001.jpg").write_bytes(b"face-bytes")

    import asyncio

    retry_after = (integration.dt_util.now() + timedelta(minutes=10)).isoformat()
    uploaded = asyncio.run(
        manager._upload_face_asset_to_device(
            api,
            coord,
            "HA001",
            {
                "UserID": "HA001",
                "Name": "Lee Fletcher",
                "FaceFileName": "HA001.jpg",
                "FaceRegister": 1,
            },
            {"face_status": "pending", "face_retry_after": retry_after},
            force=True,
        )
    )

    assert uploaded is True
    uploaded_filename = api.upload_calls[0]["filename"]
    _assert_fresh_face_filename(uploaded_filename)
    assert api.upload_calls == [{"bytes": b"face-bytes", "filename": uploaded_filename}]
    assert api.add_calls[0][0]["FaceFileName"] == uploaded_filename
    assert users_store.upserts[-1][1]["face_status"] == "active"
    assert users_store.upserts[-1][1]["face_retry_after"] == ""


def test_upload_face_asset_deletes_current_record_before_upload(tmp_path):
    hass = _FaceHassStub(tmp_path)
    users_store = _UsersStoreStub()
    hass.data[integration.DOMAIN]["users_store"] = users_store
    manager = integration.SyncManager(hass)
    api = _OrderedFaceApiStub()
    api._users = [
        {
            "ID": "99",
            "UserID": "HA001",
            "Name": "Lee Fletcher",
            "FaceStatus": "0",
        }
    ]
    coord = SimpleNamespace(
        health={"device_type": "intercom"},
        events=[],
        _append_event=lambda item: None,
    )

    face_dir = tmp_path / integration.DOMAIN / "FaceData"
    face_dir.mkdir(parents=True)
    (face_dir / "HA001.jpg").write_bytes(b"face-bytes")

    import asyncio

    uploaded = asyncio.run(
        manager._upload_face_asset_to_device(
            api,
            coord,
            "HA001",
            {
                "UserID": "HA001",
                "Name": "Lee Fletcher",
                "FaceFileName": "HA001.jpg",
                "FaceRegister": 1,
            },
            {"face_status": "pending"},
            existing={
                "ID": "42",
                "UserID": "HA001",
                "Name": "Lee Fletcher",
                "FaceStatus": "0",
            },
            force=True,
        )
    )

    assert uploaded is True
    assert ("delete", "HA001") in api.events
    uploaded_filename = api.upload_calls[0]["filename"]
    _assert_fresh_face_filename(uploaded_filename)
    assert api.events.index(("delete", "HA001")) < api.events.index(("upload", uploaded_filename))
    assert api.upload_calls == [{"bytes": b"face-bytes", "filename": uploaded_filename}]
    assert api.add_calls[0][0]["FaceFileName"] == uploaded_filename
    assert users_store.upserts[-1][1]["face_status"] == "active"


def test_prepare_user_add_payload_prefers_face_filename_over_ha_face_url():
    payload = integration._prepare_user_add_payload(
        "HA001",
        {
            "UserID": "HA001",
            "Name": "Lee Fletcher",
            "Group": integration.HA_CONTACT_GROUP_NAME,
            "FaceFileName": "HA001.jpg",
        },
        sources=(
            {
                "FaceUrl": "http://ha.local/api/AK_AC/FaceData/HA001.jpg",
                "FaceFileName": "HA001.jpg",
            },
        ),
    )

    assert payload["FaceFileName"] == "HA001.jpg"
    assert "FaceUrl" not in payload
    assert payload["importFile"] == {"fileName": "HA001.jpg", "fileData": {}}
    assert payload["FaceRegister"] == 1


def test_replace_user_on_device_preserves_active_face_record():
    manager = _make_manager()
    api = _ReplaceApiStub()

    import asyncio

    asyncio.run(
        manager._replace_user_on_device(
            api,
            "user1",
            {
                "UserID": "user1",
                "Name": "User One Updated",
                "FaceFileName": "user1.jpg",
                "FaceRegister": 1,
            },
            existing={"ID": "42", "UserID": "user1", "Name": "User One", "FaceStatus": "1"},
        )
    )

    assert api.delete_calls == []
    assert api.add_calls == []


def test_set_user_on_device_preserves_active_face_record():
    manager = _make_manager()
    api = _FaceApiStub()

    import asyncio

    asyncio.run(
        manager._set_user_on_device(
            api,
            {
                "UserID": "HA001",
                "Name": "User One Updated",
                "FaceFileName": "HA001.jpg",
                "FaceRegister": 1,
            },
            "HA001",
            existing={"ID": "42", "UserID": "HA001", "Name": "User One", "FaceStatus": "1"},
        )
    )

    assert api.set_calls == []


def test_sync_queue_kicks_stale_face_error_without_existing_eta():
    scheduled = []
    users_store = SimpleNamespace(
        all=lambda: {"HA001": {"status": "active", "face_status": "error"}}
    )
    hass = SimpleNamespace(data={integration.DOMAIN: {"users_store": users_store}})
    queue = object.__new__(integration.SyncQueue)
    queue.hass = hass
    queue._handle = None
    queue._lock = None
    queue._pending_all = False
    queue._pending_devices = set()
    queue._pending_full = False
    queue._pending_full_devices = set()
    queue._pending_reason_all = None
    queue._pending_reason_devices = {}
    queue.next_sync_eta = None
    queue._last_mark = None
    queue._last_delay_from_default = False
    queue._active = False
    queue._tick_unsub = None
    queue._startup_unsub = None
    queue._schedule_task = lambda coro: (scheduled.append(coro), coro.close())

    queue.ensure_future_run()

    assert queue._pending_all is True
    assert queue._pending_reason_all == "auto-detected pending state"
    assert queue.next_sync_eta is not None
    assert scheduled


def test_sync_queue_ignores_non_pending_device_health_states():
    scheduled = []
    coordinator = SimpleNamespace(
        health={"online": True, "sync_status": "in_progress"}
    )
    hass = SimpleNamespace(
        data={integration.DOMAIN: {"device-1": {"coordinator": coordinator}}}
    )
    queue = object.__new__(integration.SyncQueue)
    queue.hass = hass
    queue._handle = None
    queue._lock = None
    queue._pending_all = False
    queue._pending_devices = set()
    queue._pending_full = False
    queue._pending_full_devices = set()
    queue._pending_reason_all = None
    queue._pending_reason_devices = {}
    queue.next_sync_eta = None
    queue._last_mark = None
    queue._last_delay_from_default = False
    queue._active = False
    queue._tick_unsub = None
    queue._startup_unsub = None
    queue._schedule_task = lambda coro: (scheduled.append(coro), coro.close())

    queue.ensure_future_run()

    assert queue._pending_all is False
    assert queue._pending_reason_all is None
    assert queue.next_sync_eta is None
    assert scheduled == []


def test_sync_queue_waits_for_face_retry_cooldown():
    scheduled = []
    retry_after = (integration.dt_util.now() + timedelta(minutes=10)).isoformat()
    users_store = SimpleNamespace(
        all=lambda: {
            "HA001": {
                "status": "active",
                "face_status": "error",
                "face_retry_after": retry_after,
            }
        }
    )
    hass = SimpleNamespace(data={integration.DOMAIN: {"users_store": users_store}})
    queue = object.__new__(integration.SyncQueue)
    queue.hass = hass
    queue._handle = None
    queue._lock = None
    queue._pending_all = False
    queue._pending_devices = set()
    queue._pending_full = False
    queue._pending_full_devices = set()
    queue._pending_reason_all = None
    queue._pending_reason_devices = {}
    queue.next_sync_eta = None
    queue._last_mark = None
    queue._last_delay_from_default = False
    queue._active = False
    queue._tick_unsub = None
    queue._startup_unsub = None
    queue._schedule_task = lambda coro: (scheduled.append(coro), coro.close())

    queue.ensure_future_run()

    assert queue._pending_all is False
    assert queue._pending_reason_all is None
    assert queue.next_sync_eta is None
    assert scheduled == []


def test_sync_queue_retries_after_face_retry_cooldown_expires():
    scheduled = []
    retry_after = (integration.dt_util.now() - timedelta(minutes=1)).isoformat()
    users_store = SimpleNamespace(
        all=lambda: {
            "HA001": {
                "status": "active",
                "face_status": "pending",
                "face_retry_after": retry_after,
            }
        }
    )
    hass = SimpleNamespace(data={integration.DOMAIN: {"users_store": users_store}})
    queue = object.__new__(integration.SyncQueue)
    queue.hass = hass
    queue._handle = None
    queue._lock = None
    queue._pending_all = False
    queue._pending_devices = set()
    queue._pending_full = False
    queue._pending_full_devices = set()
    queue._pending_reason_all = None
    queue._pending_reason_devices = {}
    queue.next_sync_eta = None
    queue._last_mark = None
    queue._last_delay_from_default = False
    queue._active = False
    queue._tick_unsub = None
    queue._startup_unsub = None
    queue._schedule_task = lambda coro: (scheduled.append(coro), coro.close())

    queue.ensure_future_run()

    assert queue._pending_all is True
    assert queue._pending_reason_all == "auto-detected pending state"
    assert queue.next_sync_eta is not None
    assert scheduled
