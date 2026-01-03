from __future__ import annotations

from typing import Any, Dict, Iterable, List

RELAY_ROLE_NONE = "none"
RELAY_ROLE_DOOR = "door"
RELAY_ROLE_ALARM = "alarm"
RELAY_ROLE_DOOR_ALARM = "door_alarm"

RELAY_ROLE_CHOICES = {
    RELAY_ROLE_NONE,
    RELAY_ROLE_DOOR,
    RELAY_ROLE_ALARM,
    RELAY_ROLE_DOOR_ALARM,
}

RELAY_KEYS = ("relay_a", "relay_b")


def _clean_role(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip().lower()
    text = text.replace("-", "_").replace(" ", "_")
    return text


def default_roles(device_type: Any) -> Dict[str, str]:
    device = str(device_type or "").strip().lower()
    if device == "keypad":
        return {"relay_a": RELAY_ROLE_DOOR, "relay_b": RELAY_ROLE_NONE}
    return {"relay_a": RELAY_ROLE_DOOR, "relay_b": RELAY_ROLE_ALARM}


def normalize_role(value: Any, default: str) -> str:
    base_default = default if default in RELAY_ROLE_CHOICES else RELAY_ROLE_NONE
    cleaned = _clean_role(value)
    if not cleaned:
        return base_default
    if cleaned in RELAY_ROLE_CHOICES:
        return cleaned
    if cleaned in {"not_used", "unused", "disabled", "none"}:
        return RELAY_ROLE_NONE
    if cleaned in {"door", "door_relay"}:
        return RELAY_ROLE_DOOR
    if cleaned in {"alarm", "alarm_relay"}:
        return RELAY_ROLE_ALARM
    if cleaned in {"door_alarm", "door_and_alarm", "door_alarm_relay", "door_and_alarm_relay"}:
        return RELAY_ROLE_DOOR_ALARM
    return base_default


def normalize_roles(raw: Any, device_type: Any) -> Dict[str, str]:
    roles = default_roles(device_type)
    source: Dict[str, Any] = {}
    if isinstance(raw, dict):
        source = raw
    elif raw in (None, "", False):
        source = {}
    else:
        # Allow passing separate keys (e.g. relay_a, relay_b) via iterable pairs
        try:
            source = dict(raw)  # type: ignore[arg-type]
        except Exception:
            source = {}
    for key in RELAY_KEYS:
        if key in source:
            roles[key] = normalize_role(source.get(key), roles[key])
    # Allow single-letter aliases
    if "a" in source:
        roles["relay_a"] = normalize_role(source.get("a"), roles["relay_a"])
    if "b" in source:
        roles["relay_b"] = normalize_role(source.get("b"), roles["relay_b"])
    if str(device_type or "").strip().lower() == "keypad":
        roles["relay_b"] = RELAY_ROLE_NONE
    return roles


def _digits_for_roles(roles: Dict[str, str], targets: Iterable[str]) -> List[str]:
    result: List[str] = []
    target_set = set(targets)
    if roles.get("relay_a") in target_set and "1" not in result:
        result.append("1")
    if roles.get("relay_b") in target_set and "2" not in result:
        result.append("2")
    return result


def door_relays(roles: Dict[str, str]) -> List[str]:
    digits = _digits_for_roles(roles, {RELAY_ROLE_DOOR, RELAY_ROLE_DOOR_ALARM})
    if not digits:
        digits.append("1")
    return digits


def alarm_relays(roles: Dict[str, str]) -> List[str]:
    return _digits_for_roles(roles, {RELAY_ROLE_ALARM, RELAY_ROLE_DOOR_ALARM})


def relay_suffix_for_user(roles: Dict[str, str], key_holder: bool, device_type: Any) -> str:
    """Return relay digits a user should receive based on their key holder flag."""

    def _digit_for_key(key: str) -> str:
        return "1" if key == "relay_a" else "2"

    requested: List[str] = []
    blocked_door_alarm = False

    for key in RELAY_KEYS:
        digit = _digit_for_key(key)
        role = roles.get(key)

        if role == RELAY_ROLE_DOOR:
            if digit not in requested:
                requested.append(digit)
        elif role == RELAY_ROLE_DOOR_ALARM:
            if key_holder:
                if digit not in requested:
                    requested.append(digit)
            else:
                blocked_door_alarm = True

        if key_holder and role in (RELAY_ROLE_ALARM, RELAY_ROLE_DOOR_ALARM):
            if digit not in requested:
                requested.append(digit)

    device = str(device_type or "").strip().lower()
    allowed_digits = {"1"} if device == "keypad" else {"1", "2"}

    ordered: List[str] = []
    for digit in ("1", "2"):
        if digit in requested and digit in allowed_digits and digit not in ordered:
            ordered.append(digit)

    if not ordered and not blocked_door_alarm and "1" in allowed_digits:
        ordered.append("1")

    return "".join(ordered)


def alarm_capable(roles: Dict[str, str]) -> bool:
    return any(role in (RELAY_ROLE_ALARM, RELAY_ROLE_DOOR_ALARM) for role in roles.values())
