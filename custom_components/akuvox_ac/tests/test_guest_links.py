from datetime import datetime, timezone

from custom_components.akuvox_ac.guest_links import (
    _normalize_schedule_datetime,
    _valid_action,
    _within_policy,
)


def test_guest_action_accepts_only_supported_service_for_entity_domain():
    assert _valid_action(
        {"entity_id": "cover.front_gate", "service": "cover.open_cover"}
    ) == {
        "entity_id": "cover.front_gate",
        "service": "cover.open_cover",
        "label": "cover.front_gate",
    }
    assert _valid_action(
        {"entity_id": "cover.front_gate", "service": "homeassistant.turn_on"}
    ) is None
    assert _valid_action(
        {"entity_id": "light.drive", "service": "light.delete"}
    ) is None


def test_guest_policy_enforces_absolute_expiry_weekday_and_daily_window():
    now = datetime(2026, 11, 23, 10, 0, tzinfo=timezone.utc)  # Monday
    link = {
        "timezone": "UTC",
        "starts_at": "2026-11-23T09:00:00+00:00",
        "expires_at": "2026-11-23T11:00:00+00:00",
        "weekdays": [0],
        "daily_start": "09:00",
        "daily_end": "17:00",
    }
    assert _within_policy(link, now) == (True, "active")
    assert _within_policy(
        {**link, "weekdays": [1]}, now
    ) == (False, "outside_weekday_window")
    assert _within_policy(
        {**link, "daily_start": "11:00", "daily_end": "12:00"}, now
    ) == (False, "outside_daily_window")
    assert _within_policy(
        {**link, "expires_at": "2026-11-23T10:00:00+00:00"}, now
    ) == (False, "expired")


def test_guest_policy_supports_windows_crossing_midnight():
    now = datetime(2026, 11, 23, 23, 30, tzinfo=timezone.utc)
    link = {
        "timezone": "UTC",
        "weekdays": [0],
        "daily_start": "22:00",
        "daily_end": "06:00",
    }
    assert _within_policy(link, now) == (True, "active")


def test_guest_start_and_expiry_local_times_use_the_selected_timezone():
    assert _normalize_schedule_datetime(
        "2026-11-23T10:30", timezone.utc
    ) == "2026-11-23T10:30:00+00:00"
    assert _normalize_schedule_datetime(
        "2026-11-23T10:30:00+01:00", timezone.utc
    ) == "2026-11-23T09:30:00+00:00"
