"""Access history retention should cover the intercom's available log window."""

from custom_components.akuvox_ac.access_history import (
    AccessHistory,
    access_history_retention_cutoff,
)
from custom_components.akuvox_ac.integration import AkuvoxSettingsStore


def test_default_retention_is_30_day_floor_without_pruning_device_log_records():
    now = 1_800_000_000.0
    cutoff = access_history_retention_cutoff({}, now=now)
    history = AccessHistory()

    history.ingest(
        [
            {
                "_key": "within-device-window",
                "_t": now - 90 * 24 * 60 * 60,
                "_source": "doorlog",
            },
            {
                "_key": "outside-minimum-window",
                "_t": now - 121 * 24 * 60 * 60,
                "_source": "call",
            },
            {
                "_key": "older-device-event",
                "_t": now - 121 * 24 * 60 * 60,
                "_source": "doorlog",
            },
        ],
        5000,
        min_timestamp=cutoff,
    )

    assert now - cutoff == 30 * 24 * 60 * 60
    assert [event["_key"] for event in history.snapshot(min_timestamp=cutoff)] == [
        "within-device-window",
        "older-device-event",
    ]


def test_configured_retention_cannot_be_set_below_30_days():
    settings = object.__new__(AkuvoxSettingsStore)
    settings.data = {"access_history_retention_days": 1}

    assert settings.get_access_history_retention_bounds() == (30, 365)
    assert settings.get_access_history_retention_days() == 30
