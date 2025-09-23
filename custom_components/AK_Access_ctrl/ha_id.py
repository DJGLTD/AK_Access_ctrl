"""Utilities for working with Akuvox HA user identifiers."""
from __future__ import annotations

from typing import Any, Optional


def normalize_ha_id(value: Any) -> Optional[str]:
    """Return the canonical HA identifier (HA###…) or None if invalid."""

    if isinstance(value, (bytes, bytearray)):
        try:
            value = value.decode()
        except Exception:
            return None
    if not isinstance(value, str):
        return None

    candidate = value.strip()
    if len(candidate) < 3:
        return None

    prefix = candidate[:2].upper()
    if prefix != "HA":
        return None

    suffix = candidate[2:]
    if suffix.startswith("-"):
        suffix = suffix[1:]
    if not suffix or not suffix.isdigit():
        return None

    return f"HA{suffix}"


def is_ha_id(value: Any) -> bool:
    """Return True if the value looks like a valid HA identifier."""

    return normalize_ha_id(value) is not None


def ha_id_from_int(index: int) -> str:
    """Return the canonical HA identifier for the given numeric index."""

    return f"HA{int(index):03d}"
