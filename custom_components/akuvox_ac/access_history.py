from __future__ import annotations

import asyncio
import datetime as dt
import re
import time
from typing import Any, Dict, Iterable, List, Optional

DEFAULT_ACCESS_HISTORY_STORAGE_LIMIT = 5000
DEFAULT_ACCESS_HISTORY_RETENTION_DAYS = 30


_TYPE_KEYS = (
    "EventType",
    "Type",
    "EventCategory",
    "Category",
    "LogType",
    "LogTypeName",
    "CallType",
    "SubType",
    "Event",
    "Result",
    "action",
    "Message",
    "Description",
    "Detail",
)

_CALL_KEYS = (
    "CallNo",
    "CallNum",
    "CallNumber",
    "CallType",
    "CallMode",
    "CallStatus",
    "PriorityCall",
    "RoomNumber",
    "RoomNo",
    "Terminal",
    "Intercom",
    "SipAccount",
    "SipNumber",
)

_ACCESS_KEYS = (
    "AccessMode",
    "Method",
    "AccessType",
    "AccessPoint",
    "Door",
    "DoorName",
    "Reader",
    "OpenType",
    "User",
    "UserID",
    "UserName",
    "Name",
    "CardNo",
    "CardNumber",
    "Credential",
    "CredentialType",
    "Pin",
    "PIN",
    "Passcode",
    "Password",
    "AccessResult",
    "AccessStatus",
    "OpenResult",
)

_CALL_PATTERN = re.compile(r"\bcall\b|doorbell|ringback|\bsip\b|intercom|monitor", re.IGNORECASE)
_ACCESS_PATTERN = re.compile(
    r"\baccess\b|\bdoor\b|unlock|granted|denied|card|pin|keypad|entry|credential|passcode|face|finger",
    re.IGNORECASE,
)
_SYSTEM_PATTERN = re.compile(
    r"system|integrity|mismatch|sync|reboot|restart|online|offline|power|network|alarm|error|update|config|firmware|tamper|maintenance|diagnostic",
    re.IGNORECASE,
)


def _has_meaningful_value(obj: Optional[Dict[str, Any]], key: str) -> bool:
    if not obj or key not in obj:
        return False
    value = obj.get(key)
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    return True


def _combined_event_text(event: Optional[Dict[str, Any]], device: Optional[Dict[str, Any]]) -> str:
    parts: List[str] = []
    for key in _TYPE_KEYS:
        value = (event or {}).get(key)
        if value is None:
            continue
        try:
            text = str(value).strip()
        except Exception:
            continue
        if text:
            parts.append(text)

    if isinstance(device, dict):
        for key in ("device_type", "deviceModel", "device_model", "model"):
            value = device.get(key)
            if value is None:
                continue
            try:
                text = str(value).strip()
            except Exception:
                continue
            if text:
                parts.append(text)

    return " ".join(parts)


def categorize_event(event: Optional[Dict[str, Any]], device: Optional[Dict[str, Any]] = None) -> str:
    """Return the UI category for *event* (access, call, or system)."""

    if event:
        type_value = event.get("Type") or event.get("type")
        if type_value is not None:
            normalized = str(type_value).strip().lower()
            if normalized == "dtmf":
                return "call"
            if normalized == "face":
                return "access"
            if normalized in {"private pin", "privatepin", "pin", "passcode"}:
                return "access"

    combined = _combined_event_text(event, device)

    call_detected = any(_has_meaningful_value(event, key) for key in _CALL_KEYS)
    if call_detected or _CALL_PATTERN.search(combined):
        return "call"

    if _SYSTEM_PATTERN.search(combined):
        return "system"

    access_detected = any(_has_meaningful_value(event, key) for key in _ACCESS_KEYS)
    if access_detected or _ACCESS_PATTERN.search(combined):
        return "access"

    return "system"


class AccessHistory:
    """In-memory store for aggregated access events across all devices."""

    def __init__(self) -> None:
        self._events: List[Dict[str, Any]] = []  # newest first
        self._seen: set[str] = set()

    def clear(self) -> None:
        """Remove all stored events."""

        self._events.clear()
        self._seen.clear()

    def ingest(
        self,
        events: Iterable[Dict[str, Any]],
        limit: int,
        *,
        min_timestamp: Optional[float] = None,
    ) -> bool:
        """Merge *events* into the history, keeping only the newest *limit* items."""

        limit = self._normalize_limit(limit)
        if limit <= 0:
            changed = bool(self._events or self._seen)
            self.clear()
            return changed

        cutoff = self._coerce_timestamp(min_timestamp)

        # Ensure we work against a sorted baseline so we can compare against the current
        # oldest event when deciding whether to insert older entries.
        self._events.sort(key=lambda e: self._coerce_timestamp(e.get("_t")), reverse=True)
        changed = False

        for event in events:
            if not isinstance(event, dict):
                continue
            key = self._coerce_key(event.get("_key"))
            if not key or key in self._seen:
                continue

            ts_value = self._coerce_timestamp(event.get("_t"))
            if cutoff and ts_value and ts_value < cutoff:
                continue

            if len(self._events) >= limit:
                oldest_ts = self._coerce_timestamp(self._events[-1].get("_t"))
                if ts_value < oldest_ts:
                    # Older than the oldest retained event and at capacity – skip it.
                    continue

            event_copy = dict(event)
            event_copy["_key"] = key
            event_copy["_t"] = ts_value
            if not event_copy.get("_category"):
                event_copy["_category"] = categorize_event(event_copy)

            self._events.append(event_copy)
            self._seen.add(key)
            changed = True

        if not self._events:
            return changed

        self._events.sort(key=lambda e: e.get("_t", 0.0), reverse=True)
        return self.prune(limit, min_timestamp=cutoff) or changed

    def prune(self, limit: int, *, min_timestamp: Optional[float] = None) -> bool:
        """Trim stored events so at most *limit* remain."""

        limit = self._normalize_limit(limit)
        if limit <= 0:
            changed = bool(self._events or self._seen)
            self.clear()
            return changed

        cutoff = self._coerce_timestamp(min_timestamp)
        self._events.sort(key=lambda e: e.get("_t", 0.0), reverse=True)
        if cutoff:
            retained_by_age = [
                evt
                for evt in self._events
                if self._coerce_timestamp(evt.get("_t")) >= cutoff
            ]
            if len(retained_by_age) != len(self._events):
                self._events = retained_by_age
                self._seen = {
                    self._coerce_key(evt.get("_key"))
                    for evt in self._events
                    if self._coerce_key(evt.get("_key"))
                }
                if len(self._events) <= limit:
                    return True

        if len(self._events) <= limit:
            self._seen = {
                self._coerce_key(evt.get("_key"))
                for evt in self._events
                if self._coerce_key(evt.get("_key"))
            }
            return False

        retained = self._events[:limit]
        self._events = retained
        self._seen = {
            self._coerce_key(evt.get("_key"))
            for evt in retained
            if self._coerce_key(evt.get("_key"))
        }
        return True

    def snapshot(
        self,
        limit: Optional[int] = None,
        *,
        min_timestamp: Optional[float] = None,
    ) -> List[Dict[str, Any]]:
        """Return a copy of the newest events, optionally limited to *limit* entries."""

        self._events.sort(key=lambda e: e.get("_t", 0.0), reverse=True)
        cutoff = self._coerce_timestamp(min_timestamp)
        source = (
            [
                evt
                for evt in self._events
                if self._coerce_timestamp(evt.get("_t")) >= cutoff
            ]
            if cutoff
            else list(self._events)
        )

        if limit is not None:
            limit = self._normalize_limit(limit)
            if limit >= 0:
                slice_end = limit if limit > 0 else 0
                events = source[:slice_end]
            else:
                events = []
        else:
            events = source

        return [dict(evt) for evt in events]

    def __len__(self) -> int:  # pragma: no cover - convenience only
        return len(self._events)

    @staticmethod
    def _normalize_limit(limit: Optional[int]) -> int:
        try:
            value = int(limit) if limit is not None else 0
        except (TypeError, ValueError):
            return 0
        return max(0, value)

    @staticmethod
    def _coerce_key(raw: Any) -> str:
        if raw is None:
            return ""
        try:
            text = str(raw).strip()
        except Exception:
            return ""
        return text

    @staticmethod
    def _coerce_timestamp(raw: Any) -> float:
        if isinstance(raw, (int, float)):
            try:
                return float(raw)
            except Exception:
                return 0.0
        if raw in (None, ""):
            return 0.0
        try:
            text = str(raw).strip()
        except Exception:
            return 0.0
        if not text:
            return 0.0

        def _parse(candidate: str) -> Optional[dt.datetime]:
            try:
                return dt.datetime.fromisoformat(candidate)
            except Exception:
                return None

        normalized = text.replace(" ", "T")
        candidates = [normalized]
        if normalized.endswith("Z"):
            candidates.append(normalized[:-1] + "+00:00")
        if "." in normalized:
            base = normalized.split(".", 1)[0]
            candidates.append(base)
            if base.endswith("Z"):
                candidates.append(base[:-1] + "+00:00")

        parsed: Optional[dt.datetime] = None
        for candidate in candidates:
            parsed = _parse(candidate)
            if parsed:
                break

        if not parsed:
            cleaned = normalized.split("+", 1)[0].split("Z", 1)[0].replace("T", " ")
            for fmt in ("%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M:%S", "%d/%m/%Y %H:%M:%S"):
                try:
                    parsed = dt.datetime.strptime(cleaned, fmt)
                    break
                except Exception:
                    continue

        if not parsed:
            return 0.0

        if parsed.tzinfo is None:
            return parsed.timestamp()
        return parsed.timestamp()


def access_history_storage_limit(root: Optional[Dict[str, Any]], fallback: Optional[int] = None) -> int:
    """Return the count cap used for persisted access history."""

    default = fallback if fallback is not None else DEFAULT_ACCESS_HISTORY_STORAGE_LIMIT
    try:
        value = int(default)
    except Exception:
        value = DEFAULT_ACCESS_HISTORY_STORAGE_LIMIT
    value = max(0, value)

    settings = root.get("settings_store") if isinstance(root, dict) else None
    getter = getattr(settings, "get_access_history_storage_limit", None)
    if callable(getter):
        try:
            return max(0, int(getter()))
        except Exception:
            return value

    display_getter = getattr(settings, "get_access_history_limit", None)
    if callable(display_getter):
        try:
            value = max(value, int(display_getter()))
        except Exception:
            pass
    return value


def access_history_retention_cutoff(
    root: Optional[Dict[str, Any]],
    *,
    now: Optional[float] = None,
) -> Optional[float]:
    """Return the oldest event timestamp that should be kept, if configured."""

    settings = root.get("settings_store") if isinstance(root, dict) else None
    getter = getattr(settings, "get_access_history_retention_seconds", None)
    seconds: Optional[float] = None
    if callable(getter):
        try:
            seconds = float(getter())
        except Exception:
            seconds = None
    if seconds is None:
        seconds = DEFAULT_ACCESS_HISTORY_RETENTION_DAYS * 24 * 60 * 60
    if seconds <= 0:
        return None
    base = time.time() if now is None else float(now)
    return base - seconds


def schedule_access_history_persist(
    hass: Any,
    root: Optional[Dict[str, Any]],
    limit: Optional[int] = None,
) -> bool:
    """Persist the current access-history snapshot through the configured store."""

    if not isinstance(root, dict):
        return False
    history = root.get("access_history")
    store = root.get("access_history_store")
    if history is None or store is None:
        return False
    if not hasattr(history, "snapshot"):
        return False

    try:
        persist_limit = access_history_storage_limit(root, fallback=limit)
        cutoff = access_history_retention_cutoff(root)
        events = history.snapshot(persist_limit, min_timestamp=cutoff)
    except Exception:
        return False

    async def _save() -> None:
        try:
            save_events = getattr(store, "async_save_events", None)
            if callable(save_events):
                await save_events(events)
                return

            data = getattr(store, "data", None)
            if isinstance(data, dict):
                data["events"] = list(events)
            save = getattr(store, "async_save", None)
            if callable(save):
                await save()
        except Exception:
            return

    try:
        create_task = getattr(hass, "async_create_task", None)
        if callable(create_task):
            create_task(_save())
            return True

        loop = getattr(hass, "loop", None)
        if loop is not None and hasattr(loop, "create_task"):
            loop.create_task(_save())
            return True

        asyncio.get_running_loop().create_task(_save())
        return True
    except RuntimeError:
        return False


__all__ = [
    "AccessHistory",
    "access_history_retention_cutoff",
    "access_history_storage_limit",
    "categorize_event",
    "schedule_access_history_persist",
]
