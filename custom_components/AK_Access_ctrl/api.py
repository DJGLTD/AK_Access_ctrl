from __future__ import annotations

import logging
import re
from typing import Any, Dict, List, Optional, Tuple, Iterable

from aiohttp import ClientSession, BasicAuth


_LOGGER = logging.getLogger(__name__)


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
                    r.raise_for_status()
                    return data

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

    async def _post_api(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """POST to /api/ then /action as fallback."""
        return await self._request_attempts("POST", ("/api/", "/action"), payload)

    async def config_get(
        self,
        *,
        filters: Optional[Iterable[str]] = None,
        items: Optional[Iterable[str]] = None,
    ) -> Dict[str, Any]:
        """Fetch configuration key/value pairs via the config API."""

        payload: Dict[str, Any] = {"target": "config", "action": "get"}
        data_payload: Dict[str, Any] = {}

        if filters:
            filt_list = [str(f) for f in filters if f]
            if filt_list:
                data_payload["filter"] = filt_list
        if items and not data_payload:
            item_list = [str(i) for i in items if i]
            if item_list:
                data_payload["item"] = item_list
        if data_payload:
            payload["data"] = data_payload

        response: Dict[str, Any] = {}
        try:
            response = await self._post_api(payload)
        except Exception:
            try:
                response = await self._get_api("/api/config/get")
            except Exception:
                response = {}

        data_section = response.get("data") if isinstance(response, dict) else {}
        if not isinstance(data_section, dict):
            return {}

        out: Dict[str, Any] = {}
        for key, value in data_section.items():
            if isinstance(key, str):
                out[key] = value
        return out

    async def config_set(self, updates: Dict[str, Any]) -> Dict[str, Any]:
        """Apply configuration changes via the config API."""

        if not isinstance(updates, dict):
            return {}

        clean: Dict[str, Any] = {}
        for key, value in updates.items():
            if not isinstance(key, str) or not key:
                continue
            if value is None:
                clean[key] = ""
            elif isinstance(value, (list, dict)):
                clean[key] = value
            else:
                try:
                    clean[key] = str(value)
                except Exception:
                    clean[key] = value

        if not clean:
            return {}

        payload = {"target": "config", "action": "set", "data": clean}
        return await self._post_api(payload)

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
          - Ensure comma separator between schedule ID and relay flags ("1" | "12")
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

            normalized.append(f"{sched},{relays}")

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

    async def http_action_catalog(self) -> List[Dict[str, Any]]:
        """Discover HTTP action slots and their metadata from the configuration store."""

        candidates = [
            ["Config.DoorSetting.ACTION"],
            ["Config.DoorSetting.Action"],
            ["Config.EventSetting"],
            ["Config.ActionSetting"],
            ["Config.DoorSetting"],
        ]

        config_blob: Dict[str, Any] = {}
        for flt in candidates:
            try:
                config_blob = await self.config_get(filters=flt)
            except Exception:
                config_blob = {}
            if config_blob:
                break
        if not config_blob:
            return []

        slot_regex = re.compile(r"([A-Za-z]+)([A-Za-z0-9]+)$")
        slots: Dict[str, Dict[str, Any]] = {}

        def _to_text(val: Any) -> str:
            if isinstance(val, str):
                return val
            if val is None:
                return ""
            try:
                return str(val)
            except Exception:
                return ""

        for full_key, value in config_blob.items():
            if not isinstance(full_key, str):
                continue
            last_segment = full_key.rsplit(".", 1)[-1]
            lowered = last_segment.lower()
            if not any(tag in lowered for tag in ("action", "http", "url", "enable", "event", "name", "desc")):
                continue
            match = slot_regex.match(last_segment)
            if not match:
                continue
            field_name, slot_id = match.groups()
            slot = slots.setdefault(slot_id, {"id": slot_id, "fields": {}, "keys": {}})
            slot["fields"][field_name.lower()] = value
            slot["keys"][field_name.lower()] = full_key

        def _classify(slot: Dict[str, Any]) -> Optional[str]:
            texts: List[str] = []
            for key in ("name", "event", "description", "desc", "rule", "title", "info"):
                if key in slot["fields"]:
                    txt = _to_text(slot["fields"].get(key))
                    if txt:
                        texts.append(txt)
            if not texts:
                for val in slot["fields"].values():
                    txt = _to_text(val)
                    if txt:
                        texts.append(txt)
            combined = " ".join(texts).lower()
            if not combined:
                return None
            if "offline" in combined or "off-line" in combined or "disconnected" in combined:
                return "device_offline"
            if "grant" in combined or "pass" in combined or "success" in combined or "allowed" in combined:
                return "granted"
            if "time" in combined or "schedule" in combined or "timezone" in combined:
                if "deny" in combined or "refuse" in combined or "invalid" in combined:
                    return "denied_outside_time"
            if "deny" in combined or "refuse" in combined or "invalid" in combined or "no access" in combined or "noauth" in combined:
                return "denied_no_access"
            return None

        result: List[Dict[str, Any]] = []
        for slot in slots.values():
            if not any(k in slot["fields"] for k in ("actionurl", "httpurl")):
                continue
            slot["event_key"] = _classify(slot)
            label_parts: List[str] = []
            for key in ("name", "event", "description", "desc", "rule", "title", "info"):
                txt = _to_text(slot["fields"].get(key))
                if txt:
                    label_parts.append(txt)
            slot["label"] = ", ".join(label_parts)
            slot["url"] = _to_text(slot["fields"].get("actionurl") or slot["fields"].get("httpurl"))
            slot["enabled"] = _to_text(slot["fields"].get("httpenable") or slot["fields"].get("enable"))
            result.append(slot)

        result.sort(key=lambda item: str(item.get("id")))
        return result

    async def apply_http_actions(
        self,
        mapping: Dict[str, str],
        *,
        disable_unselected: bool = True,
    ) -> bool:
        """Apply webhook URLs to the discovered HTTP action slots."""

        mapping = {k: v for k, v in (mapping or {}).items() if k and v}
        if not mapping and not disable_unselected:
            return True

        try:
            slots = await self.http_action_catalog()
        except Exception:
            return False

        if not slots:
            return False

        updates: Dict[str, Any] = {}

        def _text(val: Any) -> str:
            if isinstance(val, str):
                return val
            if val is None:
                return ""
            try:
                return str(val)
            except Exception:
                return ""

        for slot in slots:
            event_key = slot.get("event_key")
            keys = slot.get("keys") or {}
            fields = slot.get("fields") or {}
            url_key = keys.get("actionurl") or keys.get("httpurl")
            enable_key = keys.get("httpenable") or keys.get("enable")
            method_key = keys.get("httpmethod")
            format_key = keys.get("httpformat")
            header_key = keys.get("httpheader")
            user_key = keys.get("httpuser") or keys.get("username")
            pwd_key = keys.get("httppassword") or keys.get("password")

            if event_key in mapping:
                url_value = mapping[event_key]
                if url_key:
                    updates[url_key] = str(url_value)
                if enable_key:
                    updates[enable_key] = "1"
                if method_key:
                    updates[method_key] = "0"
                if format_key and format_key not in updates:
                    updates.setdefault(format_key, _text(fields.get("httpformat")))
                if header_key and header_key not in updates:
                    updates.setdefault(header_key, _text(fields.get("httpheader")))
                if user_key and user_key not in updates:
                    updates.setdefault(user_key, _text(fields.get("httpuser") or fields.get("username")))
                if pwd_key and pwd_key not in updates:
                    updates.setdefault(pwd_key, _text(fields.get("httppassword") or fields.get("password")))
            elif disable_unselected:
                if enable_key and _text(fields.get("httpenable") or fields.get("enable")) not in ("", "0", "None"):
                    updates[enable_key] = "0"
                if url_key and _text(fields.get("actionurl") or fields.get("httpurl")):
                    updates[url_key] = ""

        if not updates:
            return True

        try:
            await self.config_set(updates)
            return True
        except Exception:
            return False

    async def system_reboot(self) -> Dict[str, Any]:
        try:
            return await self._get_api("/api/system/reboot")
        except Exception:
            return await self._post_api({"target": "system", "action": "reboot"})
