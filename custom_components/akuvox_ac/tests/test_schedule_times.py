import pytest

from custom_components.akuvox_ac.api import AkuvoxAPI


@pytest.mark.parametrize(
    "start,end,expected_start,expected_end",
    [
        ("09:30", "17:45", "0930", "1745"),
        ("930", "1745", "0930", "1745"),
        (570, 1065, "0930", "1745"),
        ("00:00", "23:59", "0000", "2359"),
        (None, None, "0000", "2359"),
    ],
)
def test_schedule_payload_preserves_hours(start, end, expected_start, expected_end):
    api = AkuvoxAPI("example.invalid")
    payload = api._sched_payload_from_spec("Staff", {"start": start, "end": end})
    assert payload["TimeStart"] == expected_start
    assert payload["TimeEnd"] == expected_end
