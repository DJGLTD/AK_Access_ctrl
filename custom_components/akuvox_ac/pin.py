"""PIN formatting and validation without losing leading zeroes."""

from typing import Any


PIN_VALIDATION_MESSAGE = "PIN must contain digits only (0-9). Spaces are removed automatically."


def normalize_pin(value: Any) -> str:
    """Remove whitespace from a PIN while preserving its characters."""
    return "" if value is None else "".join(str(value).split())


def validate_pin(value: Any) -> str:
    """Return a formatted PIN, allowing an empty value to clear it."""
    pin = normalize_pin(value)
    if any(char not in "0123456789" for char in pin):
        raise ValueError(PIN_VALIDATION_MESSAGE)
    return pin
