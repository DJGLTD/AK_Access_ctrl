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
