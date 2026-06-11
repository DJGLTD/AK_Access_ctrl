from __future__ import annotations

import re
from collections.abc import Mapping
from datetime import datetime
from typing import Any, Dict

DEFAULT_REBOOT_TIME = "03:00"
REBOOT_DAY_KEYS = ("mon", "tue", "wed", "thu", "fri", "sat", "sun")

_DAY_ALIASES = {
    "mon": "mon",
    "monday": "mon",
    "tue": "tue",
    "tues": "tue",
    "tuesday": "tue",
    "wed": "wed",
    "wednesday": "wed",
    "thu": "thu",
    "thur": "thu",
    "thurs": "thu",
    "thursday": "thu",
    "fri": "fri",
    "friday": "fri",
    "sat": "sat",
    "saturday": "sat",
    "sun": "sun",
    "sunday": "sun",
}


def _normalize_enabled(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _normalize_time(value: Any, *, strict: bool) -> str:
    text = str(value or DEFAULT_REBOOT_TIME).strip()
    match = re.fullmatch(r"(\d{1,2}):(\d{2})", text)
    if not match:
        if strict:
            raise ValueError("Reboot time must use HH:MM format")
        return DEFAULT_REBOOT_TIME

    hour = int(match.group(1))
    minute = int(match.group(2))
    if hour > 23 or minute > 59:
        if strict:
            raise ValueError("Reboot time is outside the valid 24-hour range")
        return DEFAULT_REBOOT_TIME
    return f"{hour:02d}:{minute:02d}"


def _normalize_days(value: Any, *, strict: bool) -> list[str]:
    if value in (None, ""):
        raw_days = []
    elif isinstance(value, str):
        raw_days = [value]
    elif isinstance(value, (list, tuple, set)):
        raw_days = list(value)
    else:
        if strict:
            raise ValueError("Reboot days must be a list")
        raw_days = []

    selected = set()
    for raw_day in raw_days:
        normalized = _DAY_ALIASES.get(str(raw_day or "").strip().lower())
        if normalized:
            selected.add(normalized)
        elif strict:
            raise ValueError(f"Invalid reboot day: {raw_day}")

    return [day for day in REBOOT_DAY_KEYS if day in selected]


def normalize_reboot_schedule(value: Any, *, strict: bool = False) -> Dict[str, Any]:
    raw = dict(value) if isinstance(value, Mapping) else {}
    enabled = _normalize_enabled(raw.get("enabled", False))
    schedule = {
        "enabled": enabled,
        "time": _normalize_time(raw.get("time"), strict=strict),
        "days": _normalize_days(raw.get("days"), strict=strict),
    }
    if schedule["enabled"] and not schedule["days"]:
        if strict:
            raise ValueError("Select at least one reboot day")
        schedule["enabled"] = False
    return schedule


def reboot_schedule_is_due(value: Any, now: datetime) -> bool:
    schedule = normalize_reboot_schedule(value)
    if not schedule["enabled"]:
        return False
    return (
        now.strftime("%a").lower() in schedule["days"]
        and now.strftime("%H:%M") == schedule["time"]
    )
