from __future__ import annotations

import json
import logging
import time
from collections import deque
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple, Iterable

from aiohttp import ClientSession, BasicAuth


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
        use_https: bool = False,
        verify_ssl: bool = True,
        session: Optional[ClientSession] = None,
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
        self._request_log = deque(maxlen=50)

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
            combos.append((True, initial_port if initial_port != 80 else 443, False))
            combos.append((True, initial_port if initial_port != 80 else 443, True))
        else:
            combos.append((True, 443, False))
            combos.append((True, 443, True))

        # HTTP on 80
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
        bases.extend([(True, 443, False), (True, 443, True), (False, 80, True)])

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
        return await _attempt(self.use_https, self.port if self.port else (443 if self.use_https else 80), self.verify_ssl, rel)

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
        self._request_log.appendleft(record)

    def recent_requests(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Return a copy of the most recent request diagnostics."""

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
        Never send both ScheduleID and Schedule at the same time.

        NOTE: We pass ScheduleRelay through untouched (except light normalization below).
        """
        out = dict(d)

        # explicit ID wins if present
        if "ScheduleID" in out and str(out["ScheduleID"]).strip():
            sval = str(out["ScheduleID"]).strip().lower()
            if sval in ("1001", "always", "24/7", "24x7", "24/7 access"):
                out["ScheduleID"] = "1001"
            elif sval in ("1002", "never", "no access"):
                out["ScheduleID"] = "1002"
            else:
                out["ScheduleID"] = str(out["ScheduleID"]).strip()
            # do not keep any name key
            out.pop("Schedule", None)
            out.pop("schedule", None)
            out.pop("schedule_name", None)
            return out

        # name-based mapping
        name = None
        for key in ("Schedule", "schedule", "schedule_name"):
            if out.get(key):
                name = str(out.pop(key)).strip()
                break

        if name is None:
            return out

        low = name.lower()
        if low in ("24/7 access", "24/7", "24x7", "always"):
            out["ScheduleID"] = "1001"
        elif low in ("no access", "never"):
            out["ScheduleID"] = "1002"
        else:
            out["Schedule"] = name  # custom schedule by name

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

            if "," in seg:
                sched_part, relay_part = seg.split(",", 1)
            elif "-" in seg:
                sched_part, relay_part = seg.split("-", 1)
            else:
                sched_part, relay_part = seg, ""

            sched = "".join(ch for ch in sched_part.strip() if ch.isalnum())
            relays = "".join(ch for ch in relay_part.strip() if ch.isdigit())

            if not sched:
                continue

            if not relays:
                relays = "1"

            # Limit relays to the supported flags ("1" for relay A, "2" for B)
            relays_unique = "".join(dict.fromkeys(ch for ch in relays if ch in ("1", "2")))
            relays = relays_unique or "1"

            normalized.append(f"{sched}-{relays}")

        if not normalized:
            return ""

        return ";".join(normalized) + ";"

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
            if "ScheduleRelay" in it2:
                it2["ScheduleRelay"] = self._normalize_schedule_relay(it2.get("ScheduleRelay"))

            d: Dict[str, Any] = {}
            for k, v in (it2 or {}).items():
                if v is None:
                    continue
                if isinstance(v, bool):
                    d[k] = "1" if v else "0"
                else:
                    if k in ("ID", "UserID", "Name", "PrivatePIN", "WebRelay", "KeyHolder", "ScheduleID", "Schedule", "PhoneNum", "FaceUrl"):
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

        combos = [(True, 443, False), (True, 443, True), (False, 80, True)]
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
        item: Dict[str, Any],
        *,
        dest_file: str = "Face",
        index: int = 1,
    ) -> Dict[str, Any]:
        """Upload a face template directly to the device via the faceIntegration API."""

        if not item:
            return {}

        # Some firmwares expect the classic data.item[] envelope, others accept a raw list.
        payload_variants: List[Dict[str, Any]] = []
        cleaned = dict(item)
        payload_variants.append({"target": "user", "action": "add", "data": {"item": [cleaned]}})
        payload_variants.append({"target": "user", "action": "add", "data": [cleaned]})

        query = f"destFile={dest_file}&index={index}"
        rel_paths = (
            f"/v0/device/faceIntegration?{query}",
            f"/device/faceIntegration?{query}",
            f"/faceIntegration?{query}",
            f"/api/faceIntegration?{query}",
        )

        last_exc: Exception | None = None
        for payload in payload_variants:
            try:
                return await self._request_attempts("POST", rel_paths, payload)
            except Exception as exc:
                last_exc = exc
                continue

        if last_exc:
            raise last_exc

        return {}

    async def user_add(self, items: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Per manual: target=user, action=add, data.item=[{UserID/ID/Name/...}].
        We normalize booleans, schedule name/ID, and ScheduleRelay lightly.
        """
        items = self._normalize_user_items_for_add_or_set(items)
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
        """Translate our HA spec {mon:[["HH:MM","HH:MM"],...], ...} to API shape."""
        spec = spec or {}
        item: Dict[str, Any] = {"Name": name}
        for key in ("mon", "tue", "wed", "thu", "fri", "sat", "sun"):
            spans = spec.get(key) or []
            # Device accepts array of [start, end] strings
            item[key] = [[str(a), str(b)] for (a, b) in spans if a and b]
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
