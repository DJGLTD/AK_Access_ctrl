"""Schedule writes must agree across legacy and compact firmware fields."""

import asyncio
from datetime import timedelta

import pytest

from custom_components.akuvox_ac.api import AkuvoxAPI
from custom_components.akuvox_ac.integration import AkuvoxSchedulesStore
from custom_components.akuvox_ac import integration
from custom_components.akuvox_ac.tests.test_initial_integrity_check import (
    _coordinator,
    _manager,
    _queue,
)


class _ScheduleAPI(AkuvoxAPI):
    def __init__(self):
        self.requests = []

    async def _post_api(self, payload, **kwargs):
        self.requests.append(payload)
        return {"retcode": 1}


@pytest.mark.parametrize("action", ["add", "set"])
@pytest.mark.parametrize(
    "spec, expected_start, expected_end",
    [
        ({"start": "07:00", "end": "17:00"}, "07:00", "17:00"),
        ({"start": "07:15", "end": "17:45"}, "07:15", "17:45"),
        ({"TimeStart": "700", "TimeEnd": "1745"}, "07:00", "17:45"),
        ({"Daily": "0715-1745"}, "07:15", "17:45"),
        ({"start": 420, "end": 1065}, "07:00", "17:45"),
        ({"start": 0, "end": 0}, "00:00", "00:00"),
        ({"start": -10, "end": 2000}, "00:00", "23:59"),
        ({"start": "-1:00", "end": "25:90"}, "00:00", "23:59"),
        ({"start": "invalid", "end": "invalid"}, "00:00", "23:59"),
    ],
)
def test_schedule_writes_use_consistent_normalized_times(
    action, spec, expected_start, expected_end
):
    api = _ScheduleAPI()
    spec = {**spec, "ID": "7", "DisplayID": "1007", "days": ["mon", "wed", "fri"]}

    asyncio.run(getattr(api, f"schedule_{action}")("Office Hours", spec))

    assert len(api.requests) == 1
    request = api.requests[0]
    assert request["target"] == "schedule"
    assert request["action"] == action
    item = request["data"]["item"][0]
    assert item["TimeStart"] == expected_start.replace(":", "")
    assert item["TimeEnd"] == expected_end.replace(":", "")
    assert item["Daily"] == f"{expected_start}-{expected_end}"
    assert item["Week"] == "135"
    assert item["ID"] == "7"
    if action == "set":
        assert item["DisplayID"] == "1007"


@pytest.mark.parametrize("action", ["add", "set"])
@pytest.mark.parametrize("name, week", [("24/7 Access", "0123456"), ("No Access", "")])
def test_builtin_schedule_defaults_keep_consistent_full_day_times(action, name, week):
    api = _ScheduleAPI()

    asyncio.run(getattr(api, f"schedule_{action}")(name, {"ID": "7", "DisplayID": "1007"}))

    item = api.requests[0]["data"]["item"][0]
    assert item["TimeStart"] == "0000"
    assert item["TimeEnd"] == "2359"
    assert item["Daily"] == "00:00-23:59"
    assert item["Week"] == week


@pytest.mark.parametrize("action", ["add", "set"])
@pytest.mark.parametrize("start, end", [("07:00", "17:00"), ("07:15", "17:45")])
def test_a02_compact_schedule_readback_matches_requested_hours(action, start, end):
    api = _ScheduleAPI()
    days = ["mon", "tue", "wed", "thu", "fri"]
    spec = {"start": start, "end": end, "days": days, "ID": "7", "DisplayID": "1007"}

    asyncio.run(getattr(api, f"schedule_{action}")("Office Hours", spec))
    item = api.requests[0]["data"]["item"][0]
    # A02 firmware reports the compact fields, unlike firmware that uses Daily.
    readback = {key: item[key] for key in ("Name", "TimeStart", "TimeEnd", "Week")}
    store = object.__new__(AkuvoxSchedulesStore)
    normalized = store._normalize_payload("Office Hours", readback)

    assert normalized["start"] == start
    assert normalized["end"] == end
    assert normalized["days"] == days


def test_pending_a02_schedule_is_repaired_and_verified_by_next_manual_sync(monkeypatch):
    class A02API(_ScheduleAPI):
        def __init__(self):
            super().__init__()
            self.schedule = {
                "Name": "Office Hours", "ID": "7", "DisplayID": "1007",
                "TimeStart": "0000", "TimeEnd": "0000", "Week": "12345",
            }

        async def _post_api(self, payload, **kwargs):
            self.requests.append(payload)
            if payload["target"] == "schedule":
                if payload["action"] == "get":
                    return {"retcode": 1, "data": {"item": [dict(self.schedule)]}}
                assert payload["action"] == "set", "The existing schedule must be updated"
                item = payload["data"]["item"][0]
                # Simulate A02 firmware persisting/reporting compact times only.
                self.schedule = {key: item[key] for key in self.schedule}
            return {"retcode": 1, "data": {"item": []}}

        async def _get_api(self, *args, **kwargs):
            return {"retcode": 1, "data": {"item": []}}

    api, coord = A02API(), _coordinator()
    manager = _manager([("keypad", coord, api, {})])
    schedules = object.__new__(AkuvoxSchedulesStore)
    schedules.data = {"schedules": {"Office Hours": {
        "start": "07:00", "end": "17:00",
        "days": ["mon", "tue", "wed", "thu", "fri"],
    }}}
    manager._schedules_store = lambda: schedules
    queue = _queue(manager)
    now = [integration.dt_util.now().replace(microsecond=0)]
    monkeypatch.setattr(integration.dt_util, "now", lambda: now[0])

    async def run():
        # Default Sync is initially incremental and cannot repair an existing
        # schedule; the real comparison must detect the compact-time mismatch.
        await queue.sync_now("keypad")
        assert coord.health["sync_status"] == "pending"
        assert queue._retry_state["keypad"]["full"] is True
        assert coord.health["last_checked"] == now[0].isoformat()
        assert any("schedule Office Hours time mismatch" in event for event in coord.events)
        assert not any(request["action"] == "set" for request in api.requests)

        now[0] += timedelta(seconds=1)
        # Clicking ordinary Sync again bypasses cooldown but retains the full
        # repair. Run actual reconciliation, schedule_set, and integrity reads.
        await queue.sync_now("keypad")

    asyncio.run(run())

    writes = [request for request in api.requests
              if request["target"] == "schedule" and request["action"] == "set"]
    assert len(writes) == 1
    payload = writes[0]["data"]["item"][0]
    assert payload["Daily"] == "07:00-17:00"
    assert api.schedule["TimeStart"] == "0700"
    assert api.schedule["TimeEnd"] == "1700"
    assert coord.health["sync_status"] == "in_sync"
    assert coord.health["last_sync"]
    assert coord.health["last_checked"] == now[0].isoformat()
    assert coord.storage.data["last_checked"] == now[0].isoformat()
    assert "Integrity check passed" in coord.events
    assert "keypad" not in queue._retry_state
