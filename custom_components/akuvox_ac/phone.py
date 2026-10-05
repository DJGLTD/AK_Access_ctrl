"""Phone number formatting shared by profile storage and device payloads."""

from typing import Any


def normalize_phone_number(value: Any) -> str:
    """Remove all whitespace without changing leading zeroes or dial syntax."""
    if value is None:
        return ""
    return "".join(str(value).split())
