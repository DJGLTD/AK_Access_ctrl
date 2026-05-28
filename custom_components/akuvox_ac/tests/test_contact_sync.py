"""Tests for contact synchronization helpers."""

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
    def __init__(self):
        self.upload_calls = []
        self.add_calls = []
        self.delete_calls = []

    async def face_upload(self, face_bytes, *, filename):
        self.upload_calls.append({"bytes": face_bytes, "filename": filename})
        return {"path": "/mnt/Face/HA001.jpg"}

    async def user_delete(self, value):
        self.delete_calls.append(value)

    async def user_add(self, items):
        self.add_calls.append(items)


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


def test_upload_face_asset_links_device_import_path(tmp_path):
    hass = _FaceHassStub(tmp_path)
    users_store = _UsersStoreStub()
    hass.data[integration.DOMAIN]["users_store"] = users_store
    manager = integration.SyncManager(hass)
    api = _FaceApiStub()
    coord = SimpleNamespace(health={"device_type": "intercom"}, events=[], _append_event=lambda item: None)

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
    assert api.upload_calls == [{"bytes": b"face-bytes", "filename": "HA001.jpg"}]
    assert api.delete_calls == ["42", "HA001", "Lee Fletcher"]
    assert "FaceUrl" not in api.add_calls[0][0]
    assert api.add_calls[0][0]["FaceFileName"] == "HA001.jpg"
    assert api.add_calls[0][0]["importFile"] == {"fileName": "HA001.jpg", "fileData": {}}
    assert api.add_calls[0][0]["FaceRegister"] == 1
    assert users_store.upserts[-1] == (
        "HA001",
        {"face_status": "pending", "face_synced_at": "", "face_error_count": 0},
    )
