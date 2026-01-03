from custom_components.akuvox_ac.coordinator import AkuvoxCoordinator


def _make_coordinator_stub():
    coord = object.__new__(AkuvoxCoordinator)
    coord.hass = type("H", (), {})()
    return coord


def test_combine_event_date_time_joins_date_and_time():
    event = {"Date": "2025-11-06", "Time": "15:31:31"}
    assert AkuvoxCoordinator._combine_event_date_time(event) == "2025-11-06 15:31:31"


def test_combine_event_date_time_ignores_placeholder_date():
    event = {"Date": "--", "Time": "09:12:30"}
    assert AkuvoxCoordinator._combine_event_date_time(event) == "09:12:30"


def test_extract_event_timestamp_prefers_combined_values():
    coord = _make_coordinator_stub()
    event = {"Date": "2025/11/06", "Time": "08:05:01"}
    assert coord._extract_event_timestamp(event, fallback=False) == "2025/11/06 08:05:01"
