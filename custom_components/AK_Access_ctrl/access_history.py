from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional
import datetime as dt


class AccessHistory:
    """In-memory store for aggregated access events across all devices."""

    def __init__(self) -> None:
        self._events: List[Dict[str, Any]] = []  # newest first
        self._seen: set[str] = set()

    def clear(self) -> None:
        """Remove all stored events."""

        self._events.clear()
        self._seen.clear()

    def ingest(self, events: Iterable[Dict[str, Any]], limit: int) -> None:
        """Merge *events* into the history, keeping only the newest *limit* items."""

        limit = self._normalize_limit(limit)
        if limit <= 0:
            self.clear()
            return

        # Ensure we work against a sorted baseline so we can compare against the current
        # oldest event when deciding whether to insert older entries.
        self._events.sort(key=lambda e: self._coerce_timestamp(e.get("_t")), reverse=True)

        for event in events:
            if not isinstance(event, dict):
                continue
            key = self._coerce_key(event.get("_key"))
            if not key or key in self._seen:
                continue

            ts_value = self._coerce_timestamp(event.get("_t"))

            if len(self._events) >= limit:
                oldest_ts = self._coerce_timestamp(self._events[-1].get("_t"))
                if ts_value < oldest_ts:
                    # Older than the oldest retained event and at capacity – skip it.
                    continue

            event_copy = dict(event)
            event_copy["_key"] = key
            event_copy["_t"] = ts_value
            if not event_copy.get("_category"):
                event_copy["_category"] = "access"

            self._events.append(event_copy)
            self._seen.add(key)

        if not self._events:
            return

        self._events.sort(key=lambda e: e.get("_t", 0.0), reverse=True)
        self.prune(limit)

    def prune(self, limit: int) -> None:
        """Trim stored events so at most *limit* remain."""

        limit = self._normalize_limit(limit)
        if limit <= 0:
            self.clear()
            return

        self._events.sort(key=lambda e: e.get("_t", 0.0), reverse=True)

        if len(self._events) <= limit:
            self._seen = {
                self._coerce_key(evt.get("_key"))
                for evt in self._events
                if self._coerce_key(evt.get("_key"))
            }
            return

        retained = self._events[:limit]
        self._events = retained
        self._seen = {
            self._coerce_key(evt.get("_key"))
            for evt in retained
            if self._coerce_key(evt.get("_key"))
        }

    def snapshot(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """Return a copy of the newest events, optionally limited to *limit* entries."""

        self._events.sort(key=lambda e: e.get("_t", 0.0), reverse=True)

        if limit is not None:
            limit = self._normalize_limit(limit)
            if limit >= 0:
                slice_end = limit if limit > 0 else 0
                events = self._events[:slice_end]
            else:
                events = []
        else:
            events = list(self._events)

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


__all__ = ["AccessHistory"]
