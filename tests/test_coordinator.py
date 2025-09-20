from __future__ import annotations

from unittest.mock import AsyncMock

from custom_components.AK_Access_ctrl.const import EVENT_NON_KEY_ACCESS_GRANTED
from custom_components.AK_Access_ctrl.coordinator import AkuvoxCoordinator


class DummyStorage:
    def __init__(self) -> None:
        self.data = {"last_access": {}}
        self.saved = False

    async def async_save(self):
        self.saved = True


class DummyApi:
    def __init__(self, events):
        self._events = events

    async def events_last(self):
        return list(self._events)


def test_non_key_access_event_emitted_without_notifications(hass):
    storage = DummyStorage()
    storage.data["notifications"] = {"targets": []}
    api = DummyApi(
        [
            {
                "Index": "1",
                "Event": "Password Granted",
                "UserID": "42",
                "Time": "2024-07-01T10:00:00",
            }
        ]
    )

    coord = AkuvoxCoordinator(hass, api, storage, "entry1", "Front Door")

    notify_mock = AsyncMock()
    coord._dispatch_notification = notify_mock

    hass.loop.run_until_complete(coord._process_door_events())

    assert any(evt[0] == EVENT_NON_KEY_ACCESS_GRANTED for evt in hass.bus.events)
    assert notify_mock.await_count == 0
    assert storage.saved is True
    assert storage.data["door_events"]["last_event_key"] == "1"
    assert storage.data["last_access"]["42"] == "2024-07-01T10:00:00"
