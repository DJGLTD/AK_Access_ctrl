from __future__ import annotations

import json
import logging
import time
import re
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Iterable, Set

from aiohttp import ClientSession, BasicAuth, FormData
from urllib.parse import urlsplit, urlencode

from .const import (
    DEFAULT_DIAGNOSTICS_HISTORY_LIMIT,
    MAX_DIAGNOSTICS_HISTORY_LIMIT,
    MIN_DIAGNOSTICS_HISTORY_LIMIT,
)


_LOGGER = logging.getLogger(__name__)


def _utc_now_iso() -> str:
    """Return an ISO8601 UTC timestamp without microseconds."""

    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _json_copy(value: Any) -> Any:
    """Return a JSON-serialisable deep copy, falling back to string repr."""

    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    try:
        return json.loads(json.dumps(value))
    except Exception:
        try:
            return json.loads(json.dumps(str(value)))
        except Exception:
            return str(value)


def _truncate_string(value: str, limit: int = 800) -> str:
    """Trim very long strings so diagnostics stay manageable."""

    if len(value) <= limit:
        return value
    return value[: limit - 3] + "..."


class AkuvoxAPI:
    """Akuvox client with HTTPS-only detection, strict /api usage,
    and verbose debug logging. Designed to pass through modern fields like ScheduleRelay, PhoneNum, FaceUrl, WebRelay.
    """

    def __init__(
        self,
        host: str,
        port: int = 80,
        username: Optional[str] = None,
        password: Optional[str] = None,
        use_https: bool = True,
        verify_ssl: bool = True,
        session: Optional[ClientSession] = None,
        diagnostics_history_limit: Optional[int] = None,
    ):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.use_https = use_https
        self.verify_ssl = verify_ssl
        self._session = session
        self._rest_ok = True

        # Keep a rolling window of recent requests for diagnostics
        self._history_limit = self._coerce_history_limit(diagnostics_history_limit)
        self._request_log = deque(maxlen=self._history_limit)

        # Auto-detected working base; set after first successful probe
        # Tuple: (use_https: bool, port: int, verify_ssl: bool)
        self._detected: Optional[Tuple[bool, int, bool]] = None

        # Build aiohttp BasicAuth if creds provided
        self._auth: Optional[BasicAuth] = None
        if self.username:
            self._auth = BasicAuth(self.username, self.password or "")

    # -------------------- base helpers --------------------
    def _headers(self) -> Dict[str, str]:
        return {
            "Content-Type": "application/json; charset=UTF-8",
            "Accept": "application/json",
        }

    async def _ensure_detected(self):
        """Find a working (scheme, port, verify_ssl) combo; cache it."""
        if self._detected:
            return

        combos: List[Tuple[bool, int, bool]] = []

        def _normalize_port(port: Optional[int], use_https: bool) -> int:
            try:
                value = int(port or 0)
            except Exception:
                value = 0
            if value <= 0:
                return 443 if use_https else 80
            if use_https and value == 80:
                return 443
            if not use_https and value == 443:
                return 80
            return value

        def _add_combo(use_https: bool, port: Optional[int], verify: Optional[bool] = None) -> None:
            normalized_port = _normalize_port(port, use_https)
            verify_flag = bool(verify) if use_https else True
            combo = (use_https, normalized_port, verify_flag)
            if combo not in combos:
                combos.append(combo)

        configured_port: Optional[int] = self.port
        verify_order = [bool(self.verify_ssl), not bool(self.verify_ssl)]

        if self.use_https:
            for verify in verify_order:
                _add_combo(True, configured_port, verify)
            _add_combo(True, 443, False)
            _add_combo(True, 443, True)
            _add_combo(False, configured_port, True)
            _add_combo(False, 80, True)
        else:
            _add_combo(False, configured_port, True)
            _add_combo(False, 80, True)
            for verify in verify_order:
                _add_combo(True, configured_port, verify)
            _add_combo(True, 443, False)
            _add_combo(True, 443, True)

        # Deduplicate preserving order
        seen = set()
        ordered: List[Tuple[bool, int, bool]] = []
        for c in combos:
            if c not in seen:
                seen.add(c)
                ordered.append(c)

        async def _probe(use_https: bool, port: int, verify: bool) -> bool:
            scheme = "https" if use_https else "http"
            base = f"{scheme}://{self.host}:{port}"
            # try a few typical API endpoints
            for path in ("/api/system/status", "/api/"):
                url = f"{base}{path}"
                try:
                    async with self._session.get(
                        url,
                        headers=self._headers(),
                        ssl=(verify if use_https else None),
                        timeout=5,
                        auth=self._auth,
                    ) as r:
                        _LOGGER.debug("Akuvox probe %s -> %s %s", url, r.status, r.reason)
                        if 200 <= r.status < 500:
                            self._detected = (use_https, port, verify if use_https else True)
                            return True
                except Exception as e:
                    _LOGGER.debug("Akuvox probe failed: %s -> %s", url, e)
                    pass
            return False

        for use_https, port, verify in ordered:
            ok = await _probe(use_https, port, verify)
            if ok:
                return

    # -------------------- low-level request helpers --------------------
    async def _request_attempts(self, method: str, rel_paths: Iterable[str], payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Try the provided relative paths against detected + fallback bases."""
        await self._ensure_detected()

        async def _attempt(use_https: bool, port: int, verify: bool, rel: str):
            url = f"{'https' if use_https else 'http'}://{self.host}:{port}{rel}"

            # redact sensitive keys in logs
            def _redact(obj: Any) -> Any:
                if isinstance(obj, dict):
                    out = {}
                    for k, v in obj.items():
                        if str(k).lower() == "password":
                            out[k] = "***"
                        else:
                            out[k] = _redact(v)
                    return out
                if isinstance(obj, list):
                    return [_redact(x) for x in obj]
                return obj

            entry: Dict[str, Any] = {
                "timestamp": _utc_now_iso(),
                "method": method,
                "url": url,
                "path": rel,
                "scheme": "https" if use_https else "http",
                "port": port,
                "verify_ssl": bool(verify if use_https else True),
            }
            if method == "POST" and payload is not None:
                entry["payload"] = _redact(payload)

            start = time.perf_counter()

            try:
                if method == "POST":
                    _LOGGER.debug("POST %s payload=%s", url, _redact(payload or {}))
                    async with self._session.post(
                        url,
                        json=payload or {},
                        headers=self._headers(),
                        ssl=(verify if use_https else None),
                        timeout=15,
                        auth=self._auth,
                    ) as r:
                        txt = None
                        try:
                            data = await r.json(content_type=None)
                        except Exception:
                            txt = await r.text()
                            data = {"_raw": txt}
                        _LOGGER.debug("POST %s -> %s / body=%s", url, r.status, txt or data)
                        entry["status"] = r.status
                        entry["ok"] = 200 <= r.status < 400
                        if txt:
                            entry["response_excerpt"] = _truncate_string(txt)
                        else:
                            entry["response_excerpt"] = _redact(data)
                        r.raise_for_status()
                        return data

                else:
                    _LOGGER.debug("GET %s", url)
                    async with self._session.get(
                        url,
                        headers=self._headers(),
                        ssl=(verify if use_https else None),
                        timeout=15,
                        auth=self._auth,
                    ) as r:
                        txt = None
                        try:
                            data = await r.json(content_type=None)
                        except Exception:
                            txt = await r.text()
                            data = {"_raw": txt}
                        _LOGGER.debug("GET %s -> %s / body=%s", url, r.status, txt or data)
                        entry["status"] = r.status
                        entry["ok"] = 200 <= r.status < 400
                        if txt:
                            entry["response_excerpt"] = _truncate_string(txt)
                        else:
                            entry["response_excerpt"] = _redact(data)
                        r.raise_for_status()
                        return data

            except Exception as err:
                entry.setdefault("status", getattr(err, "status", None))
                entry["error"] = _truncate_string(str(err), 400)
                raise
            finally:
                entry["duration_ms"] = round((time.perf_counter() - start) * 1000, 2)
                if entry.get("payload") is None:
                    entry.pop("payload", None)
                self._remember_request(entry)

        # Compose bases to try (detected first, then common fallbacks)
        bases: List[Tuple[bool, int, bool]] = []

        def _normalize_port(port: Optional[int], use_https: bool) -> int:
            try:
                value = int(port or 0)
            except Exception:
                value = 0
            if value <= 0:
                return 443 if use_https else 80
            if use_https and value == 80:
                return 443
            if not use_https and value == 443:
                return 80
            return value

        def _add_base(use_https: bool, port: Optional[int], verify: Optional[bool] = None) -> None:
            normalized_port = _normalize_port(port, use_https)
            verify_flag = bool(verify) if use_https else True
            combo = (use_https, normalized_port, verify_flag)
            if combo not in bases:
                bases.append(combo)

        if self._detected:
            detected_https, detected_port, detected_verify = self._detected
            _add_base(detected_https, detected_port, detected_verify)

        configured_port: Optional[int] = self.port
        verify_order = [bool(self.verify_ssl), not bool(self.verify_ssl)]

        if self.use_https:
            for verify in verify_order:
                _add_base(True, configured_port, verify)
            _add_base(True, 443, False)
            _add_base(True, 443, True)
            _add_base(False, configured_port, True)
            _add_base(False, 80, True)
        else:
            _add_base(False, configured_port, True)
            _add_base(False, 80, True)
            for verify in verify_order:
                _add_base(True, configured_port, verify)
            _add_base(True, 443, False)
            _add_base(True, 443, True)

        # Try all combinations
        last_exc: Optional[Exception] = None
        for use_https, port, verify in bases:
            for rel in rel_paths:
                try:
                    return await _attempt(use_https, port, verify, rel)
                except Exception as e:
                    last_exc = e
                    _LOGGER.debug(
                        "%s attempt failed for %s://%s:%s%s -> %s",
                        method,
                        self.host,
                        "https" if use_https else "http",
                        port,
                        rel,
                        e,
                    )
                    continue

        # Final attempt: use configured base
        try:
            rel = next(iter(rel_paths))
        except StopIteration:
            rel = "/api/"

        fallback_use_https = bool(self.use_https)
        fallback_port = _normalize_port(configured_port, fallback_use_https)
        fallback_verify = bool(self.verify_ssl) if fallback_use_https else True
        return await _attempt(fallback_use_https, fallback_port, fallback_verify, rel)

    def _coerce_history_limit(self, limit: Optional[int]) -> int:
        try:
            value = int(limit if limit is not None else DEFAULT_DIAGNOSTICS_HISTORY_LIMIT)
        except Exception:
            value = DEFAULT_DIAGNOSTICS_HISTORY_LIMIT
        if value < MIN_DIAGNOSTICS_HISTORY_LIMIT:
            return MIN_DIAGNOSTICS_HISTORY_LIMIT
        if value > MAX_DIAGNOSTICS_HISTORY_LIMIT:
            return MAX_DIAGNOSTICS_HISTORY_LIMIT
        return value

    def diagnostics_history_limit(self) -> int:
        return self._history_limit

    def set_diagnostics_history_limit(self, limit: Optional[int]) -> int:
        value = self._coerce_history_limit(limit)
        if value == self._request_log.maxlen:
            self._history_limit = value
            return value
        existing = list(self._request_log)
        self._request_log = deque(existing[:value], maxlen=value)
        self._history_limit = value
        return value

    def _remember_request(self, entry: Dict[str, Any]) -> None:
        """Persist a sanitised copy of a request diagnostic entry."""

        record: Dict[str, Any] = {}
        base = dict(entry)
        base.setdefault("timestamp", _utc_now_iso())
        for key, value in base.items():
            if isinstance(value, str):
                if key in {"response_excerpt", "error"}:
                    record[key] = _truncate_string(value, 800)
                else:
                    record[key] = value
            else:
                record[key] = _json_copy(value)
        diag_type = base.get("diag_type")
        if isinstance(diag_type, str) and diag_type.strip():
            record["diag_type"] = diag_type.strip()
        elif diag_type not in (None, ""):
            record["diag_type"] = str(diag_type)
        else:
            record["diag_type"] = self._derive_diag_type(record)
        self._request_log.appendleft(record)

    def _derive_diag_type(self, record: Dict[str, Any]) -> str:
        """Produce a consistent diagnostic type string for filtering."""

        def _clean(value: Any) -> str:
            if value is None:
                return ""
            return str(value).strip().lower()

        payload = record.get("payload")
        if isinstance(payload, dict):
            target = ""
            action = ""
            for key in ("target", "Target"):
                token = _clean(payload.get(key))
                if token:
                    target = token
                    break
            for key in ("action", "Action"):
                token = _clean(payload.get(key))
                if token:
                    action = token
                    break
            if target:
                return f"{target}:{action}" if action else target

            for key in ("command", "Command"):
                token = _clean(payload.get(key))
                if token:
                    return f"command:{token}"

        path_candidate = record.get("path") or record.get("url") or ""
        path_text = str(path_candidate or "")
        extracted_path = ""
        if path_text:
            try:
                parsed = urlsplit(path_text)
                extracted_path = parsed.path or ""
            except Exception:
                extracted_path = ""
        if not extracted_path and path_text:
            if path_text.startswith("/"):
                extracted_path = path_text
            elif "://" in path_text:
                try:
                    extracted_path = "/" + path_text.split("://", 1)[1].split("/", 1)[1]
                except Exception:
                    extracted_path = ""
            else:
                extracted_path = path_text

        clean_path = extracted_path.strip("/")
        if clean_path:
            segments = [seg for seg in clean_path.split("/") if seg]
            while segments and segments[0].lower() in {"api", "v0"}:
                segments.pop(0)
            if segments:
                base_segments = segments[:2]
                return "/".join(seg.lower() for seg in base_segments)

        method = _clean(record.get("method"))
        if method:
            return f"method:{method}"

        return "other"

    def recent_requests(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """Return a copy of the most recent request diagnostics."""

        if limit is None:
            limit = self._history_limit
        if limit is None or limit <= 0:
            items = list(self._request_log)
        else:
            items = list(self._request_log)[:limit]
        return [json.loads(json.dumps(item)) for item in items]

    async def _post_api(
        self,
        payload: Dict[str, Any],
        *,
        rel_paths: Optional[Iterable[str]] = None,
    ) -> Dict[str, Any]:
        """POST to common API endpoints, allowing custom fallbacks per target."""

        paths: Tuple[str, ...]
        if rel_paths is None:
            paths = ("/api/",)
        else:
            paths = tuple(rel_paths)
            if not paths:
                paths = ("/api/",)
        return await self._request_attempts("POST", paths, payload)

    async def _get_api(self, primary: str, *fallbacks: str) -> Dict[str, Any]:
        """GET with fallback paths."""
        rels = (primary, *fallbacks)
        return await self._request_attempts("GET", rels, None)

    # -------------------- payload normalization helpers --------------------
    def _map_schedule_fields(self, d: Dict[str, Any]) -> Dict[str, Any]:
        """
        Map our schedule fields to what the device expects:
        - "24/7 Access" / variants -> ScheduleID=1001
        - "No Access" / variants    -> ScheduleID=1002
        - Anything else by explicit name -> Schedule=<name>
        Preserve Schedule lists when provided so we can mirror the native payloads.

        NOTE: We pass ScheduleRelay through untouched (except light normalization below).
        """

        out = dict(d)

        schedule_list: Optional[List[str]] = None
        schedule_name: Optional[str] = None

        def _consume_schedule_value(value: Any) -> None:
            nonlocal schedule_list, schedule_name
            if isinstance(value, (list, tuple, set)):
                cleaned: List[str] = []
                for entry in value:
                    text = str(entry or "").strip()
                    if text:
                        cleaned.append(text)
                if cleaned:
                    schedule_list = cleaned
            elif value not in (None, ""):
                text = str(value).strip()
                if text:
                    schedule_name = text

        for key in ("Schedule", "schedule", "schedule_name"):
            if key in out:
                _consume_schedule_value(out.pop(key))

        explicit_id = str(out.get("ScheduleID") or "").strip()
        if explicit_id:
            sval = explicit_id.lower()
            if sval in ("1001", "always", "24/7", "24x7", "24/7 access"):
                out["ScheduleID"] = "1001"
            elif sval in ("1002", "never", "no access"):
                out["ScheduleID"] = "1002"
            else:
                out["ScheduleID"] = explicit_id
            if schedule_list is None and out["ScheduleID"].isdigit():
                schedule_list = [out["ScheduleID"]]
        elif schedule_name:
            low = schedule_name.lower()
            if low in ("24/7 access", "24/7", "24x7", "always"):
                out["ScheduleID"] = "1001"
                if schedule_list is None:
                    schedule_list = ["1001"]
            elif low in ("no access", "never"):
                out["ScheduleID"] = "1002"
                if schedule_list is None:
                    schedule_list = ["1002"]
            elif schedule_name.isdigit():
                out["ScheduleID"] = schedule_name
                if schedule_list is None:
                    schedule_list = [schedule_name]
            else:
                out["Schedule"] = schedule_name  # custom schedule by name

        if schedule_list is not None:
            out["Schedule"] = schedule_list

        return out

    def _normalize_schedule_relay(self, val: Any) -> Optional[str]:
        """
        Normalize ScheduleRelay:
          - Accept strings like "1001,1;" or "1001-12" and lists of entries
          - Ensure hyphen separator between schedule ID and relay flags ("1" | "12")
          - Ensure a single trailing ';' delimiter between entries
        """

        if val is None:
            return None

        def _flatten(raw: Any) -> List[str]:
            if isinstance(raw, (list, tuple)):
                out: List[str] = []
                for item in raw:
                    out.extend(_flatten(item))
                return out
            text = str(raw or "")
            if not text:
                return []
            return [seg for seg in text.replace("\n", "").split(";") if seg]

        tokens = _flatten(val)
        normalized: List[str] = []

        for token in tokens:
            seg = token.strip().strip(";")
            if not seg:
                continue

            had_separator = False
            if "," in seg:
                sched_part, relay_part = seg.split(",", 1)
                had_separator = True
            elif "-" in seg:
                sched_part, relay_part = seg.split("-", 1)
                had_separator = True
            else:
                sched_part, relay_part = seg, ""

            sched = "".join(ch for ch in sched_part.strip() if ch.isalnum())
            relay_raw = relay_part.strip()
            relays = "".join(ch for ch in relay_raw if ch.isdigit())

            if not sched:
                continue

            # Limit relays to the supported flags ("1" for relay A, "2" for B)
            relays_unique = "".join(dict.fromkeys(ch for ch in relays if ch in ("1", "2")))

            if relays_unique:
                normalized.append(f"{sched}-{relays_unique}")
            elif had_separator and not relay_raw:
                normalized.append(f"{sched}-")
            else:
                normalized.append(f"{sched}-1")

        if not normalized:
            return ""

        return ";".join(normalized)

    @staticmethod
    def _schedule_id_from_relay(val: Any) -> Optional[str]:
        if val is None:
            return None
        text = str(val).strip()
        if not text:
            return None
        segment = text.split(";", 1)[0]
        if "-" in segment:
            segment = segment.split("-", 1)[0]
        segment = segment.strip()
        if not segment or not segment.isdigit():
            return None
        return segment

    @staticmethod
    def _coerce_int(value: Any) -> Optional[int]:
        """Best-effort conversion to ``int`` preserving ``None`` for invalid inputs."""

        if value is None:
            return None
        if isinstance(value, bool):
            return 1 if value else 0
        if isinstance(value, (int, float)):
            try:
                return int(value)
            except Exception:
                return None
        if isinstance(value, str):
            text = value.strip()
            if not text:
                return None
            if text.isdigit() or (text.startswith("-") and text[1:].isdigit()):
                try:
                    return int(text, 10)
                except Exception:
                    return None
        return None

    @staticmethod
    def _should_force_face_register(face_filename: Any) -> bool:
        """Return True when a managed HA face filename should enable FaceRegister."""

        if not isinstance(face_filename, str):
            return False
        name = face_filename.strip()
        if not name:
            return False
        # Home Assistant generated face assets follow HA000123.jpg naming; when
        # such a filename is present we should ensure the register flag stays set.
        return bool(re.fullmatch(r"HA\d+\.jpg", name, flags=re.IGNORECASE))

    def _normalize_user_items_for_add_or_set(
        self,
        items: List[Dict[str, Any]],
        *,
        allow_face_url: bool = False,
    ) -> List[Dict[str, Any]]:
        """
        - Map schedule names to device IDs where applicable (24/7 -> 1001, No Access -> 1002)
        - Convert boolean fields to integers when the firmware expects numeric flags
        - Ensure core stringy fields are strings
        - Normalize ScheduleRelay if provided
        """
        numeric_fields = {
            "DialAccount",
            "DoorNum",
            "LiftFloorNum",
            "PriorityCall",
            "C4EventNo",
            "AuthMode",
            "FaceRegister",
            "KeyHolder",
            "SourceType",
        }

        norm: List[Dict[str, Any]] = []
        for it in items or []:
            it2 = self._map_schedule_fields(it or {})

            # For user.add normalize pass (allow_face_url == False), drop 'Schedule'
            # to avoid firmware retcode -100 (error param) on add.
            if not allow_face_url:
                it2.pop("Schedule", None)
                it2.pop("FaceUrl", None)
                it2.pop("FaceURL", None)

            # ScheduleRelay normalization (pass-through otherwise)
            relay_value = None
            if "ScheduleRelay" in it2:
                relay_value = it2.pop("ScheduleRelay")
            if relay_value is None and "Schedule-Relay" in it2:
                relay_value = it2.pop("Schedule-Relay")
            normalized_relay = self._normalize_schedule_relay(relay_value)
            if normalized_relay is not None:
                it2["ScheduleRelay"] = normalized_relay
            else:
                it2.pop("ScheduleRelay", None)

            d: Dict[str, Any] = {}
            for k, v in (it2 or {}).items():
                if v is None:
                    continue
                if isinstance(v, bool):
                    if k in numeric_fields:
                        d[k] = "1" if v else "0"
                    else:
                        d[k] = "1" if v else "0"
                    continue
                if k == "Schedule" and isinstance(v, (list, tuple, set)):
                    cleaned = [str(entry or "").strip() for entry in v if str(entry or "").strip()]
                    d[k] = cleaned
                    continue
                if k in (
                    "ID",
                    "UserID",
                    "Name",
                    "PrivatePIN",
                    "WebRelay",
                    "ScheduleID",
                    "PhoneNum",
                    "DoorNum",
                    "LiftFloorNum",
                    "PriorityCall",
                    "DialAccount",
                    "C4EventNo",
                    "AuthMode",
                    "Group",
                    "CardCode",
                    "BLEAuthCode",
                    "ScheduleRelay",
                ):
                    if k in numeric_fields:
                        coerced = self._coerce_int(v)
                        if coerced is not None:
                            d[k] = str(coerced)
                        else:
                            text = str(v).strip()
                            if text:
                                d[k] = text
                    else:
                        text = str(v).strip()
                        if text:
                            d[k] = text
                else:
                    d[k] = v
            if allow_face_url:
                for key in ("FaceUrl", "FaceURL"):
                    if key in d and d[key] is not None:
                        d[key] = str(d[key])
            had_user_id_alias = "UserId" in (it2 or {})
            user_id_value = d.get("UserID")
            alias_value = d.get("UserId")
            if user_id_value not in (None, ""):
                text = str(user_id_value)
                d["UserID"] = text
                if had_user_id_alias:
                    d["UserId"] = text
                else:
                    d.pop("UserId", None)
            elif alias_value not in (None, ""):
                text = str(alias_value)
                d["UserID"] = text
                if had_user_id_alias:
                    d["UserId"] = text
                else:
                    d.pop("UserId", None)
            else:
                d.pop("UserID", None)
                if not had_user_id_alias:
                    d.pop("UserId", None)
            face_source = d.pop("FaceFileName", None)
            if not face_source and isinstance(d.get("FaceUrl"), str):
                try:
                    face_source = Path(str(d["FaceUrl"])).name
                    if not face_source:
                        face_source = str(d["FaceUrl"])
                except Exception:
                    face_source = d.get("FaceUrl")

            if self._should_force_face_register(face_source):
                current = d.get("FaceRegister")
                if self._coerce_int(current) != 1:
                    d["FaceRegister"] = 1

            type_value = d.get("Type")
            if type_value in (None, ""):
                d["Type"] = "0"
            else:
                d["Type"] = str(type_value)
            norm.append(d)
        return norm

    @staticmethod
    def _prune_user_items_for_set(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Remove keys that firmwares reject from user.set payloads."""

        trimmed: List[Dict[str, Any]] = []
        for item in items or []:
            if not isinstance(item, dict):
                continue
            trimmed.append(dict(item))
        return trimmed

    async def _ensure_ids_for_set(self, items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        'user set' expects the device 'ID'. If caller provided only UserID/Name,
        resolve to the device 'ID' from the current list first.
        """
        need_lookup = [
            it
            for it in (items or [])
            if not it.get("ID") and (it.get("UserID") or it.get("UserId") or it.get("Name"))
        ]
        if not need_lookup:
            return items

        try:
            dev_users = await self.user_list()
        except Exception:
            dev_users = []

        def _find_id(rec: Dict[str, Any]) -> Optional[str]:
            uid = str(rec.get("UserID") or rec.get("UserId") or "")
            name = str(rec.get("Name") or "")
            for u in dev_users or []:
                if uid and str(u.get("UserID") or u.get("UserId") or "") == uid:
                    return str(u.get("ID") or "")
                if name and str(u.get("Name") or "") == name:
                    return str(u.get("ID") or "")
            return None

        out: List[Dict[str, Any]] = []
        for it in items or []:
            if not it.get("ID"):
                did = _find_id(it)
                if did:
                    it = {**it, "ID": did}
            out.append(it)
        return out

    # -------------------- Unified API call helpers --------------------
    async def _api_user(self, action: str, items: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
        payload: Dict[str, Any] = {"target": "user", "action": action}
        if items is not None:
            payload["data"] = {"item": items}

        rel_paths = (
            f"/api/user/{action}",
            "/api/user",
            f"/api/web/user/{action}",
            "/api/web/user",
        )
        return await self._post_api(payload, rel_paths=rel_paths)

    async def _api_contact(self, action: str, items: List[Dict[str, Any]]) -> Dict[str, Any]:
        payload: Dict[str, Any] = {"target": "contact", "action": action, "data": {"item": items}}
        return await self._post_api(payload, rel_paths=("/api/contact/", "/api/"))

    # -------------------- diagnostics --------------------
    async def ping_info(self) -> Dict[str, Any]:
        attempts: List[Dict[str, Any]] = []

        async def _try(
            use_https: bool,
            port: int,
            path: str,
            method: str = "GET",
            payload: Optional[Dict[str, Any]] = None,
            verify: bool = True,
        ):
            scheme = "https" if use_https else "http"
            item: Dict[str, Any] = {
                "base": f"{scheme}://{self.host}:{port}",
                "scheme": scheme,
                "port": port,
                "verify_ssl": bool(verify if use_https else True),
                "method": method,
                "path": path,
                "ok": False,
                "status": 0,
                "error": None,
            }
            url = f"{item['base']}{path}"
            item["url"] = url
            try:
                if method == "GET":
                    async with self._session.get(
                        url,
                        headers=self._headers(),
                        ssl=(verify if use_https else None),
                        timeout=5,
                        auth=self._auth,
                    ) as r:
                        item["status"] = r.status
                        item["ok"] = 200 <= r.status < 500
                        if not item["ok"]:
                            try:
                                item["error"] = await r.text()
                            except Exception:
                                pass
                elif method == "POST":
                    async with self._session.post(
                        url,
                        json=payload or {},
                        headers=self._headers(),
                        ssl=(verify if use_https else None),
                        timeout=5,
                        auth=self._auth,
                    ) as r:
                        item["status"] = r.status
                        item["ok"] = 200 <= r.status < 500
                        if not item["ok"]:
                            try:
                                item["error"] = await r.text()
                            except Exception:
                                pass
                elif method == "HEAD":
                    async with self._session.head(
                        url,
                        headers=self._headers(),
                        ssl=(verify if use_https else None),
                        timeout=5,
                        auth=self._auth,
                    ) as r:
                        item["status"] = r.status
                        item["ok"] = 200 <= r.status < 500
                else:
                    item["error"] = f"unsupported method {method}"
                return item
            except Exception as e:
                item["error"] = str(e)
                return item

        schemes_ports: List[Tuple[bool, int, bool]] = []

        def _normalize_port(port: Optional[int], use_https: bool) -> int:
            try:
                value = int(port or 0)
            except Exception:
                value = 0
            if value <= 0:
                return 443 if use_https else 80
            if use_https and value == 80:
                return 443
            if not use_https and value == 443:
                return 80
            return value

        def _add_combo(use_https: bool, port: Optional[int], verify: Optional[bool] = None) -> None:
            normalized_port = _normalize_port(port, use_https)
            verify_flag = bool(verify) if use_https else True
            combo = (use_https, normalized_port, verify_flag)
            if combo not in schemes_ports:
                schemes_ports.append(combo)

        if self._detected:
            detected_https, detected_port, detected_verify = self._detected
            _add_combo(detected_https, detected_port, detected_verify)

        configured_port = self.port
        verify_order = [bool(self.verify_ssl), not bool(self.verify_ssl)]

        if self.use_https:
            for verify in verify_order:
                _add_combo(True, configured_port, verify)
            _add_combo(True, 443, False)
            _add_combo(True, 443, True)
            _add_combo(False, configured_port, True)
            _add_combo(False, 80, True)
        else:
            _add_combo(False, configured_port, True)
            _add_combo(False, 80, True)
            for verify in verify_order:
                _add_combo(True, configured_port, verify)
            _add_combo(True, 443, False)
            _add_combo(True, 443, True)

        paths = [
            ("GET", "/api/system/status", None),
            ("GET", "/api/system/info", None),
            ("POST", "/api/", {"target": "system", "action": "get"}),
            ("GET", "/api/device/info", None),
            ("GET", "/api/", None),
            ("HEAD", "/", None),
        ]
        for use_https, port, verify in schemes_ports:
            for m, p, pl in paths:
                attempts.append(await _try(use_https, port, p, m, pl, verify))

        ok = any(a.get("ok") for a in attempts)

        if ok:
            https_attempt = next(
                (a for a in attempts if a.get("ok") and str(a.get("scheme", "https")).lower() == "https"),
                None,
            )
            any_attempt = next((a for a in attempts if a.get("ok")), None)

            should_update = False
            chosen = None
            if not self._detected:
                chosen = https_attempt or any_attempt
                should_update = chosen is not None
            elif not self._detected[0] and https_attempt:
                chosen = https_attempt
                should_update = True

            if should_update and chosen:
                base = chosen.get("base", "")
                try:
                    port = int(base.rsplit(":", 1)[1].split("/", 1)[0])
                except Exception:
                    port = 443 if str(chosen.get("scheme", "https")).lower() == "https" else 80
                verify = bool(chosen.get("verify_ssl", True))
                scheme = str(chosen.get("scheme", "https")).lower()
                if scheme == "https":
                    self._detected = (True, port, verify)
                else:
                    self._detected = (False, port, True)

        return {"ok": ok, "attempts": attempts}

    async def ping(self) -> bool:
        info = await self.ping_info()
        return bool(info.get("ok"))

    # -------------------- public user/event/contact APIs --------------------
    async def events_last(self) -> List[Dict[str, Any]]:
        # GET door log; limited variations in practice
        try:
            r = await self._get_api("/api/doorlog/get", "/api/doorlog/last")
            items = r.get("data", {}).get("item")
            if isinstance(items, list):
                return items
            if isinstance(items, dict):
                return [items]
        except Exception:
            pass
        return []

    async def call_log(self) -> List[Dict[str, Any]]:
        """Return recent call log entries (best effort)."""

        for payload in (
            {"target": "calllog", "action": "get"},
        ):
            try:
                r = await self._post_api(payload)
                items = r.get("data", {}).get("item")
                if isinstance(items, list):
                    return items
                if isinstance(items, dict):
                    return [items]
            except Exception:
                pass

        try:
            r = await self._get_api("/api/calllog/get")
            items = r.get("data", {}).get("item")
            if isinstance(items, list):
                return items
            if isinstance(items, dict):
                return [items]
        except Exception:
            pass

        return []

    async def user_list(self) -> List[Dict[str, Any]]:
        # Try POST get/list then GET fallback
        rel_paths = (
            "/api/web/user/get",
            "/api/web/user",
            "/api/user/get",
            "/api/user",
            "/api/",
        )

        for payload in (
            {"target": "user", "action": "get"},
        ):
            try:
                r = await self._post_api(payload, rel_paths=rel_paths)
                items = r.get("data", {}).get("item")
                if isinstance(items, list):
                    return items
            except Exception:
                pass
        try:
            r = await self._get_api("/api/user/get")
            items = r.get("data", {}).get("item")
            if isinstance(items, list):
                return items
        except Exception:
            pass
        return []

    async def user_get(self, name_or_per_id: str) -> List[Dict[str, Any]]:
        query = str(name_or_per_id or "").strip()
        if not query:
            return []

        rel_paths = (
            "/api/web/user/get",
            "/api/web/user",
            "/api/user/get",
            "/api/user",
            "/api/",
        )

        def _matches(item: Dict[str, Any]) -> bool:
            text = str(query)
            lowered = text.lower()
            for key in ("ID", "UserID", "UserId", "Name", "PerID"):
                candidate = item.get(key)
                if candidate is None:
                    continue
                value = str(candidate).strip()
                if not value:
                    continue
                if key == "Name":
                    if value.lower() == lowered:
                        return True
                else:
                    if value == text:
                        return True
            return False

        for payload in (
            {"target": "user", "action": "get", "data": {"page": -1}},
            {"target": "user", "action": "get"},
        ):
            try:
                result = await self._post_api(payload, rel_paths=rel_paths)
            except Exception:
                result = None
            if not isinstance(result, dict):
                continue
            items = result.get("data", {}).get("item")
            if isinstance(items, list):
                matches = [item for item in items if isinstance(item, dict) and _matches(item)]
                if matches:
                    return matches
                continue

        params = urlencode({"NameOrPerID": query})
        rel = f"/api/user/get?{params}" if params else "/api/user/get"
        try:
            result = await self._get_api(rel)
        except Exception:
            return []

        items = result.get("data", {}).get("item") if isinstance(result, dict) else None
        if isinstance(items, list):
            matches = [item for item in items if isinstance(item, dict) and _matches(item)]
            if matches:
                return matches
            return []
        if isinstance(items, dict):
            return [items] if _matches(items) else []
        return []

    async def face_upload(
        self,
        file_bytes: bytes,
        *,
        filename: Optional[str] = None,
        dest_file: str = "Face",
        index: Optional[str] = None,
        content_type: str = "image/jpeg",
    ) -> Dict[str, Any]:
        """Upload a face image to the device via the filetool import endpoint."""

        if not file_bytes:
            return {}

        safe_filename = str(filename or "face.jpg")
        safe_filename = Path(safe_filename).name or "face.jpg"
        if len(safe_filename) > 120:
            stem, dot, ext = safe_filename.rpartition(".")
            suffix = f".{ext}" if dot else ""
            allowed = max(1, 120 - len(suffix))
            base = stem if dot else safe_filename
            base = (base[:allowed] or "face").strip() or "face"
            safe_filename = f"{base}{suffix}" if suffix else base
        safe_dest_raw = str(dest_file or "Face")
        safe_dest = safe_dest_raw or "Face"
        dest_lower = safe_dest.lower()
        if dest_lower == "face":
            # Devices expect capital "Face" in the query string; lowercase can trigger
            # handler errors (see firmware guidance).
            safe_dest = "Face"

        index_text: Optional[str] = None
        if index is not None:
            trimmed = str(index).strip()
            if trimmed:
                index_text = trimmed

        if dest_lower == "face":
            # Firmware expects the index query parameter to be present but blank for
            # face uploads. Supplying a numeric value causes the uploaded file to be
            # stored under a different slot that is not linked to users automatically.
            index_text = None

        params: Dict[str, str] = {"destFile": safe_dest, "index": index_text or ""}
        query = urlencode(params)

        base_paths = (
            "/api/filetool/import",
            "/filetool/import",
            "/api/web/filetool/import",
            "/web/filetool/import",
        )
        rel_paths = tuple(f"{path}?{query}" if query else path for path in base_paths)

        payload_info: Dict[str, Any] = {
            "target": "upload",
            "action": "face",
            "destFile": safe_dest,
            "filename": safe_filename,
            "size": len(file_bytes),
        }
        if index_text is not None:
            payload_info["index"] = index_text
        else:
            payload_info["index"] = ""

        await self._ensure_detected()

        async def _attempt(use_https: bool, port: int, verify: bool, rel: str) -> Dict[str, Any]:
            scheme = "https" if use_https else "http"
            url = f"{scheme}://{self.host}:{port}{rel}"
            entry: Dict[str, Any] = {
                "timestamp": _utc_now_iso(),
                "method": "POST",
                "url": url,
                "path": rel,
                "scheme": scheme,
                "port": port,
                "verify_ssl": bool(verify if use_https else True),
                "payload": payload_info,
                "diag_type": "upload:face",
            }
            start = time.perf_counter()

            form = FormData()
            form.add_field(
                "file",
                file_bytes,
                filename=safe_filename,
                content_type=content_type or "application/octet-stream",
            )

            try:
                _LOGGER.debug(
                    "POST %s (face upload) filename=%s size=%s", url, safe_filename, len(file_bytes)
                )
                async with self._session.post(
                    url,
                    data=form,
                    headers={"Accept": "application/json, text/plain, */*"},
                    ssl=(verify if use_https else None),
                    timeout=30,
                    auth=self._auth,
                ) as r:
                    txt = None
                    try:
                        data = await r.json(content_type=None)
                    except Exception:
                        txt = await r.text()
                        data = {"_raw": txt}
                    entry["status"] = r.status
                    entry["ok"] = 200 <= r.status < 400
                    if txt is not None:
                        entry["response_excerpt"] = _truncate_string(txt)
                    else:
                        entry["response_excerpt"] = _json_copy(data)
                    r.raise_for_status()
                    return data
            except Exception as err:
                entry.setdefault("status", getattr(err, "status", None))
                entry["error"] = _truncate_string(str(err), 400)
                raise
            finally:
                entry["duration_ms"] = round((time.perf_counter() - start) * 1000, 2)
                self._remember_request(entry)

        bases: List[Tuple[bool, int, bool]] = []

        def _normalize_port(port: Optional[int], use_https: bool) -> int:
            try:
                value = int(port or 0)
            except Exception:
                value = 0
            if value <= 0:
                return 443 if use_https else 80
            if use_https and value == 80:
                return 443
            if not use_https and value == 443:
                return 80
            return value

        def _add_base(use_https: bool, port: Optional[int], verify: Optional[bool] = None) -> None:
            normalized_port = _normalize_port(port, use_https)
            verify_flag = bool(verify) if use_https else True
            combo = (use_https, normalized_port, verify_flag)
            if combo not in bases:
                bases.append(combo)

        if self._detected:
            detected_https, detected_port, detected_verify = self._detected
            _add_base(detected_https, detected_port, detected_verify)

        configured_port: Optional[int] = self.port
        verify_order = [bool(self.verify_ssl), not bool(self.verify_ssl)]

        if self.use_https:
            for verify in verify_order:
                _add_base(True, configured_port, verify)
            _add_base(True, 443, False)
            _add_base(True, 443, True)
            _add_base(False, configured_port, True)
            _add_base(False, 80, True)
        else:
            _add_base(False, configured_port, True)
            _add_base(False, 80, True)
            for verify in verify_order:
                _add_base(True, configured_port, verify)
            _add_base(True, 443, False)
            _add_base(True, 443, True)

        for use_https, port, verify in bases:
            for rel in rel_paths:
                try:
                    data = await _attempt(use_https, port, verify, rel)
                    result = self._coerce_face_upload_result(data)
                    self._validate_face_upload_result(result)
                    return result
                except Exception as exc:
                    _LOGGER.debug(
                        "Face upload attempt failed for %s://%s:%s%s -> %s",
                        self.host,
                        "https" if use_https else "http",
                        port,
                        rel,
                        exc,
                    )
                    continue

        fallback_rel = rel_paths[0] if rel_paths else "/api/web/filetool/import"
        fallback_use_https = bool(self.use_https)
        fallback_port = _normalize_port(configured_port, fallback_use_https)
        fallback_verify = bool(self.verify_ssl) if fallback_use_https else True
        data = await _attempt(fallback_use_https, fallback_port, fallback_verify, fallback_rel)
        result = self._coerce_face_upload_result(data)
        self._validate_face_upload_result(result)
        return result

    @staticmethod
    def _coerce_face_upload_result(data: Any) -> Dict[str, Any]:
        path: Optional[str] = None

        def _extract(candidate: Any) -> Optional[str]:
            if isinstance(candidate, dict):
                for key in ("path", "Path", "facePath", "FacePath", "face", "Face", "url", "URL"):
                    value = candidate.get(key)
                    if isinstance(value, str) and value.strip():
                        return value.strip()
            if isinstance(candidate, (list, tuple)):
                for entry in candidate:
                    extracted = _extract(entry)
                    if extracted:
                        return extracted
            if isinstance(candidate, bytes):
                try:
                    candidate = candidate.decode()
                except Exception:
                    candidate = ""
            if isinstance(candidate, str):
                text = candidate.strip()
                if text and len(text) < 1024 and "/" in text:
                    return text.strip('"')
            return None

        path = _extract(data)

        def _extract_retcode(candidate: Any) -> Optional[Any]:
            if isinstance(candidate, dict):
                for key in (
                    "retcode",
                    "retCode",
                    "RetCode",
                    "ret_code",
                    "code",
                ):
                    if key in candidate:
                        return candidate.get(key)
                for nested_key in ("data", "result"):
                    nested = candidate.get(nested_key)
                    extracted = _extract_retcode(nested)
                    if extracted is not None:
                        return extracted
                return None
            if isinstance(candidate, (list, tuple)):
                for entry in candidate:
                    extracted = _extract_retcode(entry)
                    if extracted is not None:
                        return extracted
            return None

        def _extract_message(candidate: Any) -> Optional[str]:
            if isinstance(candidate, dict):
                for key in ("msg", "message", "error", "detail", "reason"):
                    value = candidate.get(key)
                    if isinstance(value, str) and value.strip():
                        return value.strip()
                for nested_key in ("data", "result"):
                    nested = candidate.get(nested_key)
                    message = _extract_message(nested)
                    if message:
                        return message
                return None
            if isinstance(candidate, (list, tuple)):
                for entry in candidate:
                    message = _extract_message(entry)
                    if message:
                        return message
            return None

        result: Dict[str, Any] = {"raw": data}

        retcode_value = _extract_retcode(data)
        if retcode_value is not None:
            result["retcode_raw"] = retcode_value
            try:
                retcode_int = int(str(retcode_value).strip())
            except Exception:
                retcode_int = None
            if retcode_int is not None:
                result["retcode"] = retcode_int

        message_value = _extract_message(data)
        if message_value:
            result["message"] = message_value

        if path:
            result["path"] = path
        return result

    @staticmethod
    def _parse_result_status(result: Any) -> Tuple[Optional[int], Optional[str]]:
        """Extract (retcode, message) pairs from Akuvox API responses."""

        def _extract_retcode(candidate: Any) -> Optional[Any]:
            if isinstance(candidate, dict):
                for key in ("retcode", "retCode", "RetCode", "ret_code", "code"):
                    if key in candidate:
                        return candidate.get(key)
                for nested_key in ("data", "result"):
                    nested = candidate.get(nested_key)
                    extracted = _extract_retcode(nested)
                    if extracted is not None:
                        return extracted
                return None
            if isinstance(candidate, (list, tuple)):
                for entry in candidate:
                    extracted = _extract_retcode(entry)
                    if extracted is not None:
                        return extracted
            return None

        def _extract_message(candidate: Any) -> Optional[str]:
            if isinstance(candidate, dict):
                for key in ("msg", "message", "error", "detail", "reason"):
                    value = candidate.get(key)
                    if isinstance(value, str) and value.strip():
                        return value.strip()
                for nested_key in ("data", "result"):
                    nested = candidate.get(nested_key)
                    message = _extract_message(nested)
                    if message:
                        return message
                return None
            if isinstance(candidate, (list, tuple)):
                for entry in candidate:
                    message = _extract_message(entry)
                    if message:
                        return message
            return None

        raw_retcode = _extract_retcode(result)
        retcode: Optional[int] = None
        if raw_retcode is not None:
            try:
                retcode = int(str(raw_retcode).strip())
            except Exception:
                retcode = None

        return retcode, _extract_message(result)

    @staticmethod
    def _validate_face_upload_result(result: Dict[str, Any]) -> None:
        retcode, message = AkuvoxAPI._parse_result_status(result)
        if retcode in (None, 0):
            return
        detail = f" (message: {message})" if message else ""
        raise RuntimeError(f"Akuvox face upload returned retcode {retcode}{detail}")

    def _initial_user_add_payload(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """Build a firmware-friendly payload for ``user.add`` without face data."""

        def _string(value: Any, default: str = "") -> str:
            if value in (None, ""):
                return default
            text = str(value).strip()
            return text if text else default

        user_id = _string(item.get("UserID") or item.get("user_id") or item.get("UserId"))
        name = _string(item.get("Name") or item.get("name"), default=user_id or "HA User")
        raw_groups = item.get("groups")
        if isinstance(raw_groups, (list, tuple)) and raw_groups:
            first_group = raw_groups[0]
        else:
            first_group = None
        group = _string(item.get("Group") or first_group)

        base: Dict[str, Any] = {
            "Name": name or (user_id or "HA User"),
            "Type": str(item.get("Type") or item.get("type") or "0"),
        }

        if group and group.strip().lower() != "default":
            base["Group"] = group

        if user_id:
            base["UserID"] = user_id

        # Only pass through schedule metadata when the caller explicitly
        # supplied it. X912 firmwares reject cosmetic defaults such as
        # ScheduleID=1001 when not required.
        schedule_keys = (
            "ScheduleID",
            "schedule_id",
            "Schedule",
            "schedule",
            "schedule_name",
        )
        include_schedule = any(key in item for key in schedule_keys)
        if include_schedule:
            sched_fields = self._map_schedule_fields(item)
            schedule_id = sched_fields.get("ScheduleID")
            if schedule_id not in (None, ""):
                base["ScheduleID"] = str(schedule_id)
        # NOTE: Do not include free-text 'Schedule' in user.add payloads.
        # Some Akuvox firmwares reject this with retcode -100 (error param).
        # Use ScheduleRelay only on user.add; additional scheduling can be
        # applied later via user.set if needed.

        relay_value = item.get("ScheduleRelay")
        if relay_value is None:
            relay_value = item.get("Schedule-Relay")
        normalized_relay = self._normalize_schedule_relay(relay_value)
        if normalized_relay:
            base["ScheduleRelay"] = normalized_relay

        def _first(*keys: str) -> Any:
            for key in keys:
                if key in item:
                    return item.get(key)
            return None

        def _stringify_numeric(value: Any) -> Optional[str]:
            coerced = self._coerce_int(value)
            if coerced is not None:
                return str(coerced)
            if value in (None, ""):
                return None
            text = str(value).strip()
            return text or None

        optional_numeric: Dict[str, Tuple[str, ...]] = {
            "DialAccount": ("dial_account",),
            "DoorNum": ("door_num",),
            "LiftFloorNum": ("lift_floor_num", "lift_floor"),
            "PriorityCall": ("priority_call",),
            "AuthMode": ("auth_mode",),
            "C4EventNo": ("c4_event_no",),
        }

        for target, aliases in optional_numeric.items():
            raw = _first(target, *aliases)
            value = _stringify_numeric(raw)
            if value is not None:
                base[target] = value

        optional_string: Dict[str, Tuple[str, ...]] = {
            "WebRelay": ("web_relay",),
            "CardCode": ("card_code",),
            "Building": tuple(),
            "Room": tuple(),
        }

        for target, aliases in optional_string.items():
            raw = _first(target, *aliases)
            if raw in (None, ""):
                continue
            text = str(raw).strip()
            if text:
                base[target] = text
        pin_value = _first("PrivatePIN", "pin", "Pin", "private_pin")
        if pin_value not in (None, ""):
            pin_text = str(pin_value).strip()
            if pin_text:
                base["PrivatePIN"] = pin_text

        return base

    @staticmethod
    def _extract_created_ids(result: Any) -> List[str]:
        """Extract device numeric IDs from a ``user.add`` response."""

        ids: List[str] = []

        def _append(value: Any) -> None:
            if value in (None, ""):
                return
            text = str(value).strip()
            if not text:
                return
            ids.append(text)

        def _walk(candidate: Any) -> None:
            if isinstance(candidate, dict):
                if "ID" in candidate:
                    _append(candidate.get("ID"))
                elif "Id" in candidate:
                    _append(candidate.get("Id"))
                elif "id" in candidate:
                    _append(candidate.get("id"))
                for value in candidate.values():
                    _walk(value)
            elif isinstance(candidate, (list, tuple)):
                for entry in candidate:
                    _walk(entry)

        if isinstance(result, dict):
            data = result.get("data")
            if isinstance(data, dict):
                items = data.get("item")
                if isinstance(items, list):
                    for entry in items:
                        if isinstance(entry, dict) and "ID" in entry:
                            _append(entry.get("ID"))
                    if ids:
                        return ids
                elif isinstance(items, dict):
                    if "ID" in items:
                        _append(items.get("ID"))
                        return ids
            result_section = result.get("result")
            if isinstance(result_section, dict) and not ids:
                _walk(result_section)
        _walk(result)
        return ids

    async def _apply_user_set_after_add(
        self,
        items: List[Dict[str, Any]],
        created_ids: Optional[List[str]] = None,
    ) -> None:
        """Reapply the desired fields using ``user.set`` after the initial creation."""

        follow_up: List[Dict[str, Any]] = []
        for idx, item in enumerate(items or []):
            if not isinstance(item, dict):
                continue
            payload = dict(item)
            if created_ids and idx < len(created_ids):
                device_id = created_ids[idx]
                if device_id not in (None, ""):
                    payload.setdefault("ID", str(device_id))
            follow_up.append(payload)
        if not follow_up:
            return
        await self.user_set(follow_up)

    async def face_delete_bulk(self, user_ids: Iterable[str]) -> None:
        """Delete face images associated with the provided UserID values."""

        ids = [str(uid).strip() for uid in (user_ids or []) if str(uid).strip()]
        if not ids:
            return

        paths = (
            "/api/web/face/del",
            "/web/face/del",
            "/api/face/del",
            "/face/del",
        )

        for uid in ids:
            payload = {
                "target": "face",
                "action": "del",
                "data": {"UserID": uid, "UserId": uid},
            }
            try:
                await self._post_api(payload, rel_paths=paths)
            except Exception as err:
                _LOGGER.debug("Face delete failed for %s: %s", uid, err)

    async def face_delete(self, user_id: str) -> None:
        await self.face_delete_bulk([user_id])

    async def user_add(self, items: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Per manual: target=user, action=add, data.item=[{UserID/ID/Name/...}].
        We normalize booleans, schedule name/ID, and ScheduleRelay lightly.
        """
        prepared: List[Dict[str, Any]] = []
        follow_up: List[Dict[str, Any]] = []
        updates: List[Dict[str, Any]] = []

        preflight_needed = False
        for original in items or []:
            if not isinstance(original, dict):
                continue
            if any(
                str(original.get(key) or "").strip()
                for key in ("UserID", "user_id", "UserId", "Name", "name")
            ):
                preflight_needed = True
                break

        existing_by_user_id: Dict[str, Dict[str, Any]] = {}
        existing_by_name: Dict[str, Dict[str, Any]] = {}
        if preflight_needed:
            try:
                existing_records = await self.user_list()
            except Exception as err:
                existing_records = []
                _LOGGER.debug("Preflight user.list failed before user.add: %s", err)
            for record in existing_records or []:
                if not isinstance(record, dict):
                    continue
                user_id_value = str(record.get("UserID") or record.get("UserId") or "").strip()
                name_value = str(record.get("Name") or "").strip()
                if user_id_value:
                    existing_by_user_id.setdefault(user_id_value, record)
                if name_value:
                    existing_by_name.setdefault(name_value.lower(), record)

        for original in items or []:
            if not isinstance(original, dict):
                continue

            user_id_value = str(
                original.get("UserID")
                or original.get("user_id")
                or original.get("UserId")
                or ""
            ).strip()
            name_value = str(original.get("Name") or original.get("name") or "").strip()

            matched: Optional[Dict[str, Any]] = None
            if user_id_value and user_id_value in existing_by_user_id:
                matched = existing_by_user_id[user_id_value]
            elif name_value and name_value.lower() in existing_by_name:
                matched = existing_by_name[name_value.lower()]

            if matched:
                update_payload = dict(original)
                device_id = str(
                    matched.get("ID")
                    or matched.get("Id")
                    or matched.get("id")
                    or ""
                ).strip()
                if device_id:
                    update_payload.setdefault("ID", device_id)
                updates.append(update_payload)
                continue

            follow_up.append(dict(original))
            prepared.append(self._initial_user_add_payload(original))

        if updates:
            try:
                await self.user_set(updates)
            except Exception as err:
                _LOGGER.debug("user.set during duplicate preflight failed: %s", err)

        normalized_items = self._normalize_user_items_for_add_or_set(prepared)

        result: Dict[str, Any] = {}
        if normalized_items:
            try:
                result = await self._api_user("add", normalized_items)
            except Exception:
                # small retry in case the device expects the alternate endpoint first
                result = await self._api_user("add", normalized_items)
            retcode, message = self._parse_result_status(result)
            if retcode not in (None, 0):
                detail = f" (message: {message})" if message else ""
                raise RuntimeError(f"Akuvox user.add returned retcode {retcode}{detail}")

            try:
                created_ids = self._extract_created_ids(result)
                await self._apply_user_set_after_add(follow_up, created_ids)
            except Exception as err:
                _LOGGER.debug("user.set follow-up after user.add failed: %s", err)
        elif follow_up:
            try:
                await self.user_set(follow_up)
            except Exception as err:
                _LOGGER.debug("user.set applied without user.add payload failed: %s", err)

        return result

    async def user_set(self, items: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        'user set' updates an existing record and expects device 'ID'.
        - Resolves device IDs when only UserID/Name are supplied
        - Normalizes schedules (names -> ScheduleID 1001/1002)
        - Normalizes booleans and ScheduleRelay
        """
        items = await self._ensure_ids_for_set(items)
        items = self._prune_user_items_for_set(items)
        items = self._normalize_user_items_for_add_or_set(items, allow_face_url=True)
        try:
            result = await self._api_user("set", items)
        except Exception:
            result = await self._api_user("set", items)

        retcode, message = self._parse_result_status(result)
        if retcode not in (None, 0):
            detail = f" (message: {message})" if message else ""
            raise RuntimeError(f"Akuvox user.set returned retcode {retcode}{detail}")

        return result

    async def user_del(self, ids: List[str]) -> Dict[str, Any]:
        """Low-level: delete by device 'ID' list (what most firmwares expect)."""
        items = [{"ID": str(i)} for i in ids]
        return await self._api_user("del", items)

    async def user_delete(self, identifier: str) -> None:
        """Delete by device ID or resolve by (ID/UserID/Name) first."""
        if identifier is None:
            return

        text = str(identifier).strip()
        if not text:
            return

        face_ids: Set[str] = set()

        try:
            users = await self.user_list()
        except Exception:
            users = []

        target_ids: List[str] = []
        for u in users or []:
            dev_id = str(u.get("ID") or "").strip()
            user_id = str(u.get("UserID") or u.get("UserId") or "").strip()
            name = str(u.get("Name") or "").strip()
            if text in (dev_id, user_id, name):
                if dev_id:
                    target_ids.append(dev_id)
                if user_id:
                    face_ids.add(user_id)

        if target_ids:
            try:
                await self.user_del(target_ids)
            except Exception:
                for did in target_ids:
                    try:
                        await self.user_del([did])
                    except Exception:
                        pass
        else:
            deletion_attempted = False
            if text.isdigit():
                try:
                    await self.user_del([text])
                    deletion_attempted = True
                except Exception:
                    pass
            if not deletion_attempted:
                try:
                    await self._api_user("del", [{"UserID": text, "UserId": text}])
                    deletion_attempted = True
                except Exception:
                    pass
            if deletion_attempted and text:
                face_ids.add(text)

        if face_ids:
            await self.face_delete_bulk(face_ids)

    async def user_delete_bulk(
        self,
        device_ids: List[str],
        *,
        face_user_ids: Optional[Iterable[str]] = None,
    ) -> None:
        ids = [str(i).strip() for i in (device_ids or []) if str(i).strip()]
        if not ids:
            return

        face_targets: Set[str] = {
            str(uid).strip() for uid in (face_user_ids or []) if str(uid).strip()
        }
        if not face_targets:
            try:
                users = await self.user_list()
            except Exception:
                users = []
            wanted_ids = set(ids)
            for u in users or []:
                dev_id = str(u.get("ID") or "").strip()
                user_id = str(u.get("UserID") or u.get("UserId") or "").strip()
                if dev_id and dev_id in wanted_ids and user_id:
                    face_targets.add(user_id)

        try:
            await self.user_del(ids)
        except Exception:
            for did in ids:
                try:
                    await self.user_del([did])
                except Exception:
                    pass

        if face_targets:
            await self.face_delete_bulk(face_targets)

    async def user_delete_by_key(self, key: str) -> None:
        await self.user_delete(key)

    async def user_delete_bulk_by_keys(self, keys: List[str]) -> None:
        wanted = {str(k) for k in (keys or []) if str(k)}
        if not wanted:
            return

        try:
            users = await self.user_list()
        except Exception:
            users = []

        dev_ids: List[str] = []
        face_ids: Set[str] = set()
        for u in users or []:
            dev_id = str(u.get("ID") or "")
            user_id = str(u.get("UserID") or u.get("UserId") or "")
            name = str(u.get("Name") or "")
            if user_id in wanted or name in wanted or dev_id in wanted:
                if dev_id:
                    dev_ids.append(dev_id)
                if user_id:
                    face_ids.add(str(user_id))

        if not dev_ids:
            if face_ids:
                for uid in face_ids:
                    try:
                        await self._api_user("del", [{"UserID": uid, "UserId": uid}])
                    except Exception:
                        pass
                await self.face_delete_bulk(face_ids)
            return

        await self.user_delete_bulk(dev_ids, face_user_ids=face_ids)

    async def contact_add(self, items: List[Dict[str, Any]]) -> Dict[str, Any]:
        return await self._api_contact("add", items)

    async def contact_set(self, items: List[Dict[str, Any]]) -> Dict[str, Any]:
        return await self._api_contact("set", items)

    # ---------- Schedules ----------
    def _sched_payload_from_spec(self, name: str, spec: Dict[str, Any]) -> Dict[str, Any]:
        """Translate our HA schedule spec into the API shape."""

        spec = spec or {}

        def _truthy(value: Any) -> bool:
            if isinstance(value, str):
                return value.strip().lower() in {"1", "true", "yes", "y", "on", "enable", "enabled"}
            return bool(value)

        def _minutes(value: Any) -> Optional[int]:
            if value is None:
                return None
            if isinstance(value, (int, float)):
                minutes = int(value)
            else:
                text = str(value).strip()
                if not text:
                    return None
                if text.isdigit() and len(text) in (3, 4):
                    if len(text) == 3:
                        text = f"0{text}"
                    try:
                        hours = int(text[:2])
                        minutes = int(text[2:])
                    except ValueError:
                        return None
                else:
                    parts = text.split(":")
                    if len(parts) < 2:
                        return None
                    try:
                        hours = int(parts[0])
                        minutes = int(parts[1])
                    except ValueError:
                        return None
            if minutes < 0:
                minutes = 0
            max_minutes = 23 * 60 + 59
            if minutes > max_minutes:
                minutes = max_minutes
            return minutes

        def _clean_time(value: Any, *, default: str) -> str:
            minutes = _minutes(value)
            if minutes is None:
                minutes = _minutes(default)
            if minutes is None:
                minutes = 0
            hours = minutes // 60
            mins = minutes % 60
            return f"{hours:02d}:{mins:02d}"

        day_map = {
            "mon": "Mon",
            "tue": "Tue",
            "wed": "Wed",
            "thu": "Thur",
            "fri": "Fri",
            "sat": "Sat",
            "sun": "Sun",
        }

        selected_days: set[str] = set()
        raw_days = spec.get("days")
        if isinstance(raw_days, (list, tuple, set)):
            for entry in raw_days:
                key = str(entry or "").strip().lower()
                if key in day_map:
                    selected_days.add(key)
                else:
                    short = key[:3]
                    if short in day_map:
                        selected_days.add(short)
        elif isinstance(raw_days, dict):
            for key, value in raw_days.items():
                normalized = str(key or "").strip().lower()
                if normalized in day_map and _truthy(value):
                    selected_days.add(normalized)

        for low_key, api_key in day_map.items():
            if api_key in spec and _truthy(spec.get(api_key)):
                selected_days.add(low_key)
            elif low_key in spec and isinstance(spec.get(low_key), (list, tuple)):
                spans = spec.get(low_key) or []
                for span in spans:
                    if not isinstance(span, (list, tuple)) or len(span) < 2:
                        continue
                    start = _minutes(span[0])
                    end = _minutes(span[1])
                    if start is not None and end is not None:
                        selected_days.add(low_key)
                        break

        if not selected_days:
            lowered = name.strip().lower()
            if lowered in {"no access", "never"}:
                selected_days = set()
            elif lowered in {"24/7 access", "24/7", "24x7", "always"}:
                selected_days = set(day_map.keys())
            else:
                selected_days = {"mon", "tue", "wed", "thu", "fri"}

        sched_type = str(spec.get("type") or spec.get("Type") or "2")
        date_start = str(spec.get("date_start") or spec.get("DateStart") or "").strip()
        date_end = str(spec.get("date_end") or spec.get("DateEnd") or "").strip()

        start_time = _clean_time(
            spec.get("start")
            or spec.get("Start")
            or spec.get("time_start")
            or spec.get("TimeStart"),
            default="00:00",
        )
        end_time = _clean_time(
            spec.get("end")
            or spec.get("End")
            or spec.get("time_end")
            or spec.get("TimeEnd"),
            default="23:59",
        )

        item: Dict[str, Any] = {
            "Name": name,
            "Type": sched_type,
            "DateStart": date_start,
            "DateEnd": date_end,
            "TimeStart": start_time,
            "TimeEnd": end_time,
        }

        for low_key, api_key in day_map.items():
            item[api_key] = "1" if low_key in selected_days else "0"

        return item

    async def schedule_get(self) -> List[Dict[str, Any]]:
        # Try POST per manual; GET fallback
        for payload in (
            {"target": "schedule", "action": "get"},
            {"target": "schedule", "action": "list"},
        ):
            try:
                r = await self._post_api(payload)
                items = r.get("data", {}).get("item")
                if isinstance(items, list):
                    return items
            except Exception:
                pass
        try:
            r = await self._get_api("/api/schedule/get")
            items = r.get("data", {}).get("item")
            if isinstance(items, list):
                return items
        except Exception:
            pass
        return []

    async def schedule_add(self, name: str, spec: Dict[str, Any]) -> Dict[str, Any]:
        payload = self._sched_payload_from_spec(name, spec)
        return await self._post_api({"target": "schedule", "action": "add", "data": {"item": [payload]}})

    async def schedule_set(self, name: str, spec: Dict[str, Any]) -> Dict[str, Any]:
        payload = self._sched_payload_from_spec(name, spec)
        return await self._post_api({"target": "schedule", "action": "set", "data": {"item": [payload]}})

    async def schedule_del(self, name: str) -> Dict[str, Any]:
        item = {"Name": name}
        return await self._post_api({"target": "schedule", "action": "del", "data": {"item": [item]}})

    async def system_reboot(self) -> Dict[str, Any]:
        try:
            return await self._get_api("/api/system/reboot")
        except Exception:
            return await self._post_api({"target": "system", "action": "reboot"})
