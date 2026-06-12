import asyncio
import datetime as dt

from custom_components.akuvox_ac.ha_test_stubs import ensure_homeassistant_stubs

ensure_homeassistant_stubs()

from custom_components.akuvox_ac.const import (  # noqa: E402
    DOMAIN,
    INBOUND_CALL_RESULT_DENIED,
)
from custom_components.akuvox_ac.http import _process_inbound_call_webhook  # noqa: E402


class _CallApiStub:
    async def call_log(self):
        return [
            {
                "ID": "call-1",
                "Type": "received",
                "Number": "07920671814",
                "Timestamp": dt.datetime.now().isoformat(),
            }
        ]


class _CoordinatorStub:
    def __init__(self):
        self.device_name = "Gate"
        self.health = {"device_type": "Intercom"}
        self.notifications = []

    async def _send_alert_notification(self, event_type, **kwargs):
        self.notifications.append((event_type, kwargs))


class _BusStub:
    def __init__(self):
        self.events = []

    def async_fire(self, event_type, payload):
        self.events.append((event_type, payload))


def test_unknown_inbound_call_uses_any_denied_notification_rule():
    coordinator = _CoordinatorStub()
    hass = type(
        "Hass",
        (),
        {
            "data": {
                DOMAIN: {
                    "device-1": {
                        "api": _CallApiStub(),
                        "coordinator": coordinator,
                    }
                }
            },
            "bus": _BusStub(),
        },
    )()

    result = asyncio.run(_process_inbound_call_webhook(hass))

    assert result["result"] == INBOUND_CALL_RESULT_DENIED
    assert len(coordinator.notifications) == 1
    event_type, kwargs = coordinator.notifications[0]
    assert event_type == "any_denied"
    assert kwargs["user_id"] == "07920671814"
    assert kwargs["summary"] == "Inbound call - access Denied (07920671814)"
    assert kwargs["extra"]["event"]["CallNumber"] == "07920671814"
