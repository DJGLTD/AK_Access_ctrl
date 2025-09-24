from __future__ import annotations

import json
import logging
import time
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Iterable

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
    """Akuvox client with HTTPS-first detection, HTTP fallback, endpoint fallback (/api vs /action),
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
        return {"Content-Type": "application/json", "Accept": "application/json"}

    async def _ensure_detected(self):
        """Find a working (scheme, port, verify_ssl) combo; cache it."""
        if self._detected:
            return

        combos: List[Tuple[bool, int, bool]] = []
        initial_https = bool(self.use_https)
        initial_port = int(self.port or (443 if initial_https else 80))

        # Try configured scheme/port first (both verify/no-verify for HTTPS)
        if initial_https:
            https_port = initial_port if initial_port not in (0, 80) else 443
            combos.append((True, https_port, False))
            combos.append((True, https_port, True))
        else:
            combos.append((True, 443, False))
            combos.append((True, 443, True))
            combos.append((False, 80, True))

        # If user provided custom port, test it
        if self.port not in (80, 443):
            combos.insert(0, (initial_https, self.port, False if initial_https else True))
            if initial_https:
                combos.insert(1, (initial_https, self.port, True))

        # Deduplicate preserving order
        seen = set()
        ordered: List[Tuple[bool, int, bool]] = []
        for c in combos:
            if c not in seen:
                seen.add(c)
                ordered.append(c)

        async def _probe(https: bool, port: int, verify: bool) -> bool:
            base = f"{'https' if https else 'http'}://{self.host}:{port}"
            # try a few typical API endpoints
            for path in ("/api/system/status", "/api/", "/action"):
                url = f"{base}{path}"
                try:
                    async with self._session.get(
                        url,
                        headers=self._headers(),
                        ssl=(verify if https else None),
                        timeout=5,
                        auth=self._auth,
                    ) as r:
                        _LOGGER.debug("Akuvox probe %s -> %s %s", url, r.status, r.reason)
                        if 200 <= r.status < 500:
                            self._detected = (https, port, verify if https else True)
                            return True
                except Exception as e:
                    _LOGGER.debug("Akuvox probe failed: %s -> %s", url, e)
                    pass
            return False

        for (https, port, verify) in ordered:
            ok = await _probe(https, port, verify)
            if ok:
                return

    # -------------------- low-level request helpers --------------------
    async def _request_attempts(self, method: str, rel_paths: Iterable[str], payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Try multiple relative paths (/api/, /action, etc.) against detected + fallback bases."""
        await self._ensure_detected()

        async def _attempt(use_https: bool, port: int, verify: bool, rel: str):
            url = f"{'https' if use_https else 'http'}://{self.host}:{port}{rel}"

            # redact sensitive keys in logs
            def _redact(obj: Any) -> Any:
                if isinstance(obj, dict):
                    out = {}
                    for k, v in obj.items():
                        if str(k).lower() in ("privatepin", "password"):
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
        if self._detected:
            bases.append(self._detected)
        bases.extend([(True, 443, False), (True, 443, True)])
        if not self.use_https:
            bases.append((False, 80, True))

        # Try all combinations
        last_exc: Optional[Exception] = None
        for (https, port, verify) in bases:
            for rel in rel_paths:
                try:
                    return await _attempt(https, port, verify, rel)
                except Exception as e:
                    last_exc = e
                    _LOGGER.debug("%s attempt failed for %s://%s:%s%s -> %s",
                                  method, "https" if https else "http", self.host, port, rel, e)
                    continue

        # Final attempt: use configured base
        try:
            rel = next(iter(rel_paths))
        except StopIteration:
            rel = "/api/"
        fallback_port = self.port
        if not fallback_port:
            fallback_port = 443 if self.use_https else 80
        elif self.use_https and fallback_port == 80:
            fallback_port = 443
        elif (not self.use_https) and fallback_port == 443:
            fallback_port = 80
        return await _attempt(self.use_https, fallback_port, self.verify_ssl, rel)

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

    async def _post_api(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """POST to /api/ then /action as fallback."""
        return await self._request_attempts("POST", ("/api/", "/action"), payload)

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

        return ";".join(normalized) + ";"

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

    def _normalize_user_items_for_add_or_set(self, items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        - Map schedule names to device IDs where applicable (24/7 -> 1001, No Access -> 1002)
        - Convert bools -> "1"/"0"
        - Ensure core stringy fields are strings
        - Normalize ScheduleRelay if provided
        """
        norm: List[Dict[str, Any]] = []
        for it in items or []:
            it2 = self._map_schedule_fields(it or {})

            # ScheduleRelay normalization (pass-through otherwise)
            relay_value = None
            if "ScheduleRelay" in it2:
                relay_value = it2.pop("ScheduleRelay")
            if relay_value is None and "Schedule-Relay" in it2:
                relay_value = it2.get("Schedule-Relay")
            normalized_relay = self._normalize_schedule_relay(relay_value)
            if normalized_relay is not None:
                it2["Schedule-Relay"] = normalized_relay
            else:
                it2.pop("Schedule-Relay", None)

            d: Dict[str, Any] = {}
            for k, v in (it2 or {}).items():
                if v is None:
                    continue
                if isinstance(v, bool):
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
                    "KeyHolder",
                    "ScheduleID",
                    "PhoneNum",
                    "FaceUrl",
                    "DoorNum",
                    "LiftFloorNum",
                    "PriorityCall",
                    "DialAccount",
                    "C4EventNo",
                    "AuthMode",
                    "Group",
                    "CardCode",
                    "BLEAuthCode",
                    "FaceFileName",
                    "Schedule-Relay",
                ):
                    d[k] = str(v)
                else:
                    d[k] = v
            norm.append(d)
        return norm

    async def _ensure_ids_for_set(self, items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        'user set' expects the device 'ID'. If caller provided only UserID/Name,
        resolve to the device 'ID' from the current list first.
        """
        need_lookup = [it for it in (items or []) if not it.get("ID") and (it.get("UserID") or it.get("Name"))]
        if not need_lookup:
            return items

        try:
            dev_users = await self.user_list()
        except Exception:
            dev_users = []

        def _find_id(rec: Dict[str, Any]) -> Optional[str]:
            uid = str(rec.get("UserID") or "")
            name = str(rec.get("Name") or "")
            for u in dev_users or []:
                if uid and str(u.get("UserID") or "") == uid:
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
        return await self._post_api(payload)

    async def _api_contact(self, action: str, items: List[Dict[str, Any]]) -> Dict[str, Any]:
        payload: Dict[str, Any] = {"target": "contact", "action": action, "data": {"item": items}}
        return await self._post_api(payload)

    # -------------------- diagnostics --------------------
    async def ping_info(self) -> Dict[str, Any]:
        attempts: List[Dict[str, Any]] = []

        async def _try(
            https: bool,
            port: int,
            path: str,
            method: str = "GET",
            payload: Optional[Dict[str, Any]] = None,
            verify: bool = True,
        ):
            item: Dict[str, Any] = {
                "base": f"{'https' if https else 'http'}://{self.host}:{port}",
                "verify_ssl": verify if https else True,
                "method": method,
                "path": path,
                "ok": False,
                "status": 0,
                "error": None,
            }
            url = f"{item['base']}{path}"
            try:
                if method == "GET":
                    async with self._session.get(
                        url, headers=self._headers(), ssl=(verify if https else None), timeout=5, auth=self._auth
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
                        ssl=(verify if https else None),
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
                        url, headers=self._headers(), ssl=(verify if https else None), timeout=5, auth=self._auth
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
        if self._detected:
            https, port, verify = self._detected
            schemes_ports.append((https, port, verify))

        combos = [(True, 443, False), (True, 443, True)]
        if not self.use_https:
            combos.append((False, 80, True))
        if self.port not in (80, 443):
            combos.insert(0, (self.use_https, self.port, False if self.use_https else True))
            if self.use_https:
                combos.insert(1, (self.use_https, self.port, True))
        if self.use_https and self.port == 80:
            combos.insert(0, (True, 443, False))
        if (not self.use_https) and self.port == 443:
            combos.insert(0, (False, 80, True))

        for c in combos:
            if c not in schemes_ports:
                schemes_ports.append(c)

        paths = [
            ("GET", "/api/system/status", None),
            ("GET", "/api/system/info", None),
            ("POST", "/api/", {"target": "system", "action": "get"}),
            ("GET", "/api/device/info", None),
            ("GET", "/api/", None),
            ("GET", "/action", None),
            ("HEAD", "/", None),
        ]
        for https, port, verify in schemes_ports:
            for m, p, pl in paths:
                attempts.append(await _try(https, port, p, m, pl, verify))

        ok = any(a.get("ok") for a in attempts)

        if ok and not self._detected:
            for a in attempts:
                if a.get("ok"):
                    base = a.get("base", "")
                    https = base.startswith("https://")
                    try:
                        port = int(base.rsplit(":", 1)[1].split("/", 1)[0])
                    except Exception:
                        port = 443 if https else 80
                    verify = bool(a.get("verify_ssl", True))
                    self._detected = (https, port, verify)
                    break

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
        for payload in (
            {"target": "user", "action": "get"},
            {"target": "user", "action": "list"},
        ):
            try:
                r = await self._post_api(payload)
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
        if safe_dest.lower() == "face":
            # Devices expect capital "Face" in the query string; lowercase can trigger
            # handler errors (see firmware guidance).
            safe_dest = "Face"

        index_text: Optional[str] = None
        if index is not None:
            trimmed = str(index).strip()
            if trimmed:
                index_text = trimmed

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
            url = f"{'https' if use_https else 'http'}://{self.host}:{port}{rel}"
            entry: Dict[str, Any] = {
                "timestamp": _utc_now_iso(),
                "method": "POST",
                "url": url,
                "path": rel,
                "scheme": "https" if use_https else "http",
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
        if self._detected:
            bases.append(self._detected)
        bases.extend([(True, 443, False), (True, 443, True)])
        if not self.use_https:
            bases.append((False, 80, True))

        for https, port, verify in bases:
            for rel in rel_paths:
                try:
                    return await _attempt(https, port, verify, rel)
                except Exception as exc:
                    _LOGGER.debug(
                        "Face upload attempt failed for %s://%s:%s%s -> %s",
                        "https" if https else "http",
                        self.host,
                        port,
                        rel,
                        exc,
                    )
                    continue

        fallback_rel = rel_paths[0] if rel_paths else "/api/web/filetool/import"
        fallback_port = self.port
        if not fallback_port:
            fallback_port = 443 if self.use_https else 80
        elif self.use_https and fallback_port == 80:
            fallback_port = 443
        elif (not self.use_https) and fallback_port == 443:
            fallback_port = 80
        return await _attempt(
            self.use_https,
            fallback_port,
            self.verify_ssl,
            fallback_rel,
        )

    async def user_add(self, items: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Per manual: target=user, action=add, data.item=[{UserID/ID/Name/...}].
        We normalize booleans, schedule name/ID, and ScheduleRelay lightly.
        """
        # Ensure defaults for new user fields expected by newer firmwares.
        prepared: List[Dict[str, Any]] = []
        for original in items or []:
            base = dict(original or {})

            def _ensure_str(key: str, default: str) -> None:
                value = base.get(key)
                if value is None:
                    base[key] = default
                    return
                text = str(value).strip()
                if text or text == "0":
                    base[key] = text
                else:
                    base[key] = default

            _ensure_str("AuthMode", "0")
            _ensure_str("C4EventNo", "0")
            _ensure_str("DoorNum", "1")
            _ensure_str("LiftFloorNum", "0")
            _ensure_str("WebRelay", "0")
            _ensure_str("PriorityCall", "0")
            _ensure_str("DialAccount", "0")
            _ensure_str("Group", "Default")
            if "CardCode" in base:
                _ensure_str("CardCode", "")
            else:
                base["CardCode"] = ""
            if "BLEAuthCode" in base:
                _ensure_str("BLEAuthCode", "")
            else:
                base["BLEAuthCode"] = ""

            lp = base.get("LicensePlate")
            if not isinstance(lp, list):
                lp = []
            cleaned_lp: List[Dict[str, Any]] = []
            for entry in lp:
                cleaned_lp.append(entry if isinstance(entry, dict) else {})
            while len(cleaned_lp) < 5:
                cleaned_lp.append({})
            base["LicensePlate"] = cleaned_lp[:5]

            raw_schedule = base.get("Schedule")
            schedule_list: List[str] = []
            if isinstance(raw_schedule, (list, tuple, set)):
                for entry in raw_schedule:
                    text = str(entry or "").strip()
                    if text:
                        schedule_list.append(text)
            elif raw_schedule not in (None, ""):
                text = str(raw_schedule).strip()
                if text:
                    schedule_list.append(text)
            if not schedule_list:
                relay_spec = base.get("Schedule-Relay") or base.get("ScheduleRelay")
                sid = self._schedule_id_from_relay(relay_spec)
                if sid:
                    schedule_list = [sid]
            if schedule_list:
                base["Schedule"] = schedule_list
            else:
                base["Schedule"] = ["1001"]

            if not base.get("FaceFileName"):
                face_url = base.get("FaceUrl") or base.get("FaceURL")
                candidate = None
                if isinstance(face_url, str) and face_url.strip():
                    candidate = Path(urlsplit(face_url).path).name
                if not candidate:
                    uid = str(base.get("UserID") or "").strip()
                    if uid:
                        candidate = f"{uid}.jpg"
                if candidate:
                    base["FaceFileName"] = candidate

            prepared.append(base)

        items = self._normalize_user_items_for_add_or_set(prepared)
        try:
            return await self._api_user("add", items)
        except Exception:
            # small retry in case endpoint flip helps
            return await self._api_user("add", items)

    async def user_set(self, items: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        'user set' updates an existing record and expects device 'ID'.
        - Resolves device IDs when only UserID/Name are supplied
        - Normalizes schedules (names -> ScheduleID 1001/1002)
        - Normalizes booleans and ScheduleRelay
        """
        items = await self._ensure_ids_for_set(items)
        items = self._normalize_user_items_for_add_or_set(items)
        try:
            return await self._api_user("set", items)
        except Exception:
            return await self._api_user("set", items)

    async def user_del(self, ids: List[str]) -> Dict[str, Any]:
        """Low-level: delete by device 'ID' list (what most firmwares expect)."""
        items = [{"ID": str(i)} for i in ids]
        return await self._api_user("del", items)

    async def user_delete(self, identifier: str) -> None:
        """Delete by device ID or resolve by (ID/UserID/Name) first."""
        if not identifier:
            return
        if str(identifier).isdigit():
            try:
                await self.user_del([str(identifier)])
                return
            except Exception:
                pass

        try:
            users = await self.user_list()
        except Exception:
            users = []

        target_ids: List[str] = []
        for u in users or []:
            dev_id = str(u.get("ID") or "")
            user_id = str(u.get("UserID") or "")
            name = str(u.get("Name") or "")
            if identifier in (dev_id, user_id, name):
                if dev_id:
                    target_ids.append(dev_id)

        if not target_ids:
            # Some firmwares accept deleting by UserID as item[]
            try:
                await self._api_user("del", [{"UserID": str(identifier)}])
            except Exception:
                pass
            return

        try:
            await self.user_del(target_ids)
        except Exception:
            for did in target_ids:
                try:
                    await self.user_del([did])
                except Exception:
                    pass

    async def user_delete_bulk(self, device_ids: List[str]) -> None:
        ids = [str(i) for i in (device_ids or []) if str(i)]
        if not ids:
            return
        try:
            await self.user_del(ids)
        except Exception:
            for did in ids:
                try:
                    await self.user_del([did])
                except Exception:
                    pass

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
        for u in users or []:
            dev_id = str(u.get("ID") or "")
            user_id = str(u.get("UserID") or "")
            name = str(u.get("Name") or "")
            if user_id in wanted or name in wanted or dev_id in wanted:
                if dev_id:
                    dev_ids.append(dev_id)

        if not dev_ids:
            return

        await self.user_delete_bulk(dev_ids)

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
