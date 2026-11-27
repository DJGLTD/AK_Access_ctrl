"""Hikvision ANPR camera integration using ISAPI Digest authentication."""

from __future__ import annotations

import hashlib
import json
import re
import secrets
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import urlsplit

import aiohttp
from aiohttp import web
from homeassistant.components.http.view import HomeAssistantView
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from homeassistant.helpers.storage import Store

from .const import (
    CONF_HOST,
    CONF_PASSWORD,
    CONF_PORT,
    CONF_USERNAME,
    DEFAULT_VERIFY_SSL,
    DOMAIN,
)

_DIGEST_RE = re.compile(r'([A-Za-z][A-Za-z0-9_-]*)=(?:"((?:\\.|[^"])*)"|([^,\s]+))')
_XML_NS = "http://www.std-cgi.org/ver20/XMLSchema"
_ANPR_AUDIT_KEY = f"{DOMAIN}_anpr_audit"
_ANPR_AUDIT_RETENTION_DAYS = 90
_ANPR_AUDIT_LIMIT = 2000
_MAX_PLATE_RECORDS = 10_000


def _digest_params(header: str) -> dict[str, str]:
    if not header.lower().startswith("digest "):
        return {}
    values: dict[str, str] = {}
    for match in _DIGEST_RE.finditer(header[7:]):
        values[match.group(1).lower()] = (match.group(2) or match.group(3) or "").replace('\\"', '"')
    return values


def _quote(value: str) -> str:
    return '"' + value.replace("\\", "\\\\").replace('"', '\\"') + '"'


def _mask_plate(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    return f"***{text[-3:]}" if len(text) > 3 else "***"


def _sanitize_anpr_audit_events(events: Any, now: datetime | None = None) -> list[dict[str, Any]]:
    current = now or datetime.now(timezone.utc)
    cutoff = current - timedelta(days=_ANPR_AUDIT_RETENTION_DAYS)
    sanitized = []
    if not isinstance(events, list):
        return sanitized
    for raw_event in events:
        if not isinstance(raw_event, dict):
            continue
        try:
            timestamp = datetime.fromisoformat(str(raw_event.get("timestamp") or ""))
            if timestamp.tzinfo is None:
                timestamp = timestamp.replace(tzinfo=timezone.utc)
            if timestamp < cutoff:
                continue
        except ValueError:
            continue
        event = dict(raw_event)
        for key in list(event):
            if "plate" in str(key).lower():
                event[key] = _mask_plate(event[key])
        sanitized.append(event)
    return sanitized[-_ANPR_AUDIT_LIMIT:]


async def _record_anpr_action(hass: Any, event: dict[str, Any]) -> None:

    root = hass.data.setdefault(DOMAIN, {})
    store = root.get("anpr_audit_store")
    if store is None:
        store = Store(hass, 1, _ANPR_AUDIT_KEY)
        root["anpr_audit_data"] = await store.async_load() or {"events": []}
        root["anpr_audit_store"] = store
    data = root["anpr_audit_data"]
    data["events"] = _sanitize_anpr_audit_events(data.get("events"))
    event = dict(event)
    for key in list(event):
        if "plate" in str(key).lower():
            event[key] = _mask_plate(event[key])
    event["timestamp"] = datetime.now(timezone.utc).isoformat()
    data["events"].append(event)
    data["events"] = data["events"][-_ANPR_AUDIT_LIMIT:]
    await store.async_save(data)
    hass.bus.async_fire(f"{DOMAIN}_anpr_registration", event)


def _digest_authorization(
    challenge: str, username: str, password: str, method: str, uri: str,
    nonce_count: int, cnonce: str,
) -> str | None:
    params = _digest_params(challenge)
    realm, nonce = params.get("realm"), params.get("nonce")
    if not realm or not nonce:
        return None
    algorithm = params.get("algorithm", "MD5").upper()
    hash_fn = hashlib.sha256 if algorithm.startswith("SHA-256") else hashlib.md5
    if algorithm not in {"MD5", "MD5-SESS", "SHA-256", "SHA-256-SESS"}:
        return None
    qop_options = params.get("qop", "auth").replace('"', "").split(",")
    qop = "auth" if "auth" in qop_options else ""
    if not qop:
        return None
    nc = f"{nonce_count:08x}"

    def digest(value: str) -> str:
        return hash_fn(value.encode()).hexdigest()

    ha1 = digest(f"{username}:{realm}:{password}")
    if algorithm.endswith("-SESS"):
        ha1 = digest(f"{ha1}:{nonce}:{cnonce}")
    ha2 = digest(f"{method}:{uri}")
    response = digest(f"{ha1}:{nonce}:{nc}:{cnonce}:{qop}:{ha2}")
    fields = [
        f"username={_quote(username)}", f"realm={_quote(realm)}", f"nonce={_quote(nonce)}",
        f"uri={_quote(uri)}", f"response={_quote(response)}", f"algorithm={algorithm}",
        f"qop={qop}", f"nc={nc}", f"cnonce={_quote(cnonce)}",
    ]
    if params.get("opaque"):
        fields.append(f"opaque={_quote(params['opaque'])}")
    return "Digest " + ", ".join(fields)


class HikvisionISAPI:
    """Small Digest-auth client for traffic channel license-plate management."""

    def __init__(self, hass: Any, config: dict[str, Any]) -> None:
        self.hass = hass
        self.host = str(config[CONF_HOST]).strip()
        bare_host = self.host.strip("[]")
        self.use_https = bool(config.get("use_https", True))
        self.port = int(config.get(CONF_PORT, 443 if self.use_https else 80))
        self.username = str(config.get(CONF_USERNAME, ""))
        self.password = str(config.get(CONF_PASSWORD, ""))
        self.channel = int(config.get("channel", 1))
        self.verify_ssl = bool(config.get("verify_ssl", DEFAULT_VERIFY_SSL))
        self.allow_insecure_http = bool(config.get("allow_insecure_http", False))
        if not bare_host or any(char in bare_host for char in "/?#@ "):
            raise ValueError("Invalid camera host")
        if not 1 <= self.port <= 65535 or not 1 <= self.channel <= 255:
            raise ValueError("Invalid camera port or channel")
        if not self.use_https and not self.allow_insecure_http:
            raise ValueError(
                "HTTPS is required unless allow_insecure_http is explicitly enabled"
            )
        try:
            import ipaddress

            ipv6 = ipaddress.ip_address(bare_host).version == 6
        except ValueError:
            ipv6 = False
        self.base = f"{'https' if self.use_https else 'http'}://{'[' + bare_host + ']' if ipv6 else bare_host}:{self.port}"

    @property
    def channel_path(self) -> str:
        return f"/ISAPI/Traffic/channels/{self.channel}"

    async def request(self, method: str, path: str, *, body: str | bytes | None = None, content_type: str = "application/xml") -> str:
        session = async_get_clientsession(self.hass)
        url = self.base + path
        timeout = aiohttp.ClientTimeout(total=15)
        ssl_context = None if not self.use_https or self.verify_ssl else False
        async with session.request(method, url, data=body, headers={"Content-Type": content_type} if body is not None else {}, timeout=timeout, ssl=ssl_context, allow_redirects=False) as response:
            if response.status != 401:
                response.raise_for_status()
                return await response.text()
            challenge = response.headers.get("WWW-Authenticate", "")
        parsed = urlsplit(url)
        uri = parsed.path + (f"?{parsed.query}" if parsed.query else "")
        authorization = _digest_authorization(challenge, self.username, self.password, method, uri, 1, secrets.token_hex(12))
        if not authorization:
            raise ValueError("Camera did not offer a supported ISAPI Digest challenge")
        headers = {"Authorization": authorization}
        if body is not None:
            headers["Content-Type"] = content_type
        async with session.request(method, url, data=body, headers=headers, timeout=timeout, ssl=ssl_context, allow_redirects=False) as response:
            response.raise_for_status()
            return await response.text()

    @staticmethod
    def _parse_xml(xml: str) -> ET.Element:
        try:
            return ET.fromstring(xml)
        except ET.ParseError as err:
            raise ValueError("Camera returned malformed ISAPI XML") from err

    @staticmethod
    def _check_response(body: str) -> str:
        if not body.strip():
            return body
        try:
            parsed = json.loads(body)
        except json.JSONDecodeError:
            try:
                root = ET.fromstring(body)
            except ET.ParseError:
                return body
            status = root.findtext("{*}statusCode")
            message = root.findtext("{*}statusString") or root.findtext("{*}subStatusCode")
            if status is not None and status != "1":
                raise ValueError(message or f"Camera returned status {status}")
            if root.findtext("{*}responseStatus") == "false":
                raise ValueError(message or "Camera rejected the operation")
            return body
        if isinstance(parsed, dict):
            candidates = [parsed, *(value for value in parsed.values() if isinstance(value, dict))]
            for candidate in candidates:
                status = candidate.get("statusCode")
                if status is not None and str(status) != "1":
                    raise ValueError(str(candidate.get("errorMsg") or candidate.get("statusString") or f"Camera returned status {status}"))
        return body

    async def async_info(self) -> dict[str, str]:
        root = self._parse_xml(await self.request("GET", "/ISAPI/System/deviceInfo"))
        wanted = {"deviceName", "model", "serialNumber", "firmwareVersion"}
        info = {tag: "" for tag in wanted}
        for element in root.iter():
            tag = element.tag.rsplit("}", 1)[-1]
            if tag in info:
                info[tag] = element.text or ""
        return info

    async def async_list_plates(self, list_type: str = "allVehicleList") -> list[dict[str, str]]:
        search_id = secrets.token_hex(12)
        rows: list[dict[str, str]] = []
        position = 0
        total = 1
        while position < total and position < _MAX_PLATE_RECORDS:
            body = (f'<LPListAuditSearchDescription version="2.0" xmlns="{_XML_NS}">'
                    f"<searchID>{search_id}</searchID><searchResultPosition>{position}</searchResultPosition>"
                    "<maxResults>200</maxResults></LPListAuditSearchDescription>")
            root = self._parse_xml(await self.request("POST", self.channel_path + "/searchLPListAudit", body=body))
            total_text = next(((item.text or "").strip() for item in root.iter() if item.tag.rsplit("}", 1)[-1] == "totalMatches"), "0")
            try:
                total = min(_MAX_PLATE_RECORDS, max(0, int(total_text)))
            except ValueError:
                total = 0
            count = 0
            for row in root.iter():
                if row.tag.rsplit("}", 1)[-1] != "LicensePlateInfo":
                    continue
                values = {child.tag.rsplit("}", 1)[-1]: (child.text or "").strip() for child in row}
                row_type = values.get("type", "")
                if list_type == "allVehicleList" or row_type.casefold() == list_type.casefold():
                    rows.append({"id": values.get("id", ""), "plate": values.get("LicensePlate", values.get("licensePlate", "")), "type": row_type, "create_time": values.get("createTime", "")})
                count += 1
            if not count:
                break
            position += count
        return rows

    async def _write_plate(self, plate: str, *, name: str, list_type: str, operation: str, record_id: str = "") -> str:
        plate = self._validate_plate(plate)
        if list_type not in {"whiteList", "blackList", "allVehicleList", "otherVehicleList"}:
            raise ValueError("Unsupported list type")
        payload = {"LicensePlateInfoList": [{"id": record_id, "plateColor": "unknown", "plateType": "unknown", "listType": list_type, "LicensePlate": plate, "cardID": "", "cardNo": name[:64], "certificateNumber": "", "certificateType": "ID", "operation": operation}]}
        result = await self.request("PUT", self.channel_path + "/licensePlateAuditData/record?format=json", body=json.dumps(payload, ensure_ascii=False), content_type="application/json")
        return self._check_response(result)

    async def async_add_plate(self, plate: str, *, name: str = "", list_type: str = "whiteList") -> str:
        return await self._write_plate(plate, name=str(name), list_type=list_type, operation="new")

    async def async_delete_plate(self, plate: str) -> str:
        plate = self._validate_plate(plate)
        result = await self.request("PUT", self.channel_path + "/DelLicensePlateAuditData?format=json", body=json.dumps({"licensePlate": [plate]}, ensure_ascii=False), content_type="application/json")
        return self._check_response(result)

    async def async_update_plate(self, old_plate: str, new_plate: str, *, name: str = "", list_type: str = "whiteList") -> str:
        old_plate, new_plate = self._validate_plate(old_plate), self._validate_plate(new_plate)
        existing = await self.async_list_plates()
        record = next((item for item in existing if item["plate"].casefold() == old_plate.casefold()), None)
        if not record:
            raise ValueError("The existing plate was not found on the camera")
        if not record.get("id"):
            raise ValueError("The camera did not return an ID for the existing plate")
        return await self._write_plate(new_plate, name=name, list_type=list_type, operation="modify", record_id=record["id"])

    @staticmethod
    def _validate_plate(value: str) -> str:
        plate = str(value or "").strip().upper()
        if not plate or len(plate) > 32 or any(ord(char) < 32 for char in plate):
            raise ValueError("Plate must contain between 1 and 32 printable characters")
        return plate


class HikvisionPlateView(HomeAssistantView):
    """Admin-only CRUD API for a configured camera entry."""

    url = "/api/akuvox_ac/anpr/{entry_id}/plates"
    name = "api:akuvox_ac:hikvision_plates"
    requires_auth = False

    async def _camera(self, request: web.Request, entry_id: str) -> HikvisionISAPI:
        from .http import _request_is_admin

        hass = request.app["hass"]
        if not _request_is_admin(hass, request):
            raise web.HTTPUnauthorized()
        camera = (hass.data.get(DOMAIN, {}).get(entry_id) or {}).get("anpr")
        if not isinstance(camera, HikvisionISAPI):
            raise web.HTTPNotFound()
        return camera

    async def get(self, request: web.Request, entry_id: str) -> web.Response:
        camera = await self._camera(request, entry_id)
        try:
            plates = await camera.async_list_plates(str(request.query.get("list_type", "allVehicleList")))
        except (aiohttp.ClientError, ValueError) as err:
            raise web.HTTPBadGateway(text=str(err)) from err
        return web.json_response({"plates": plates}, headers={"Cache-Control": "no-store"})

    async def post(self, request: web.Request, entry_id: str) -> web.Response:
        camera = await self._camera(request, entry_id)
        payload = await request.json()
        if not isinstance(payload, dict):
            raise web.HTTPBadRequest(text="Expected a JSON object")
        try:
            plate = camera._validate_plate(payload.get("plate", ""))
            await camera.async_add_plate(plate, name=str(payload.get("name", "")), list_type=payload.get("list_type", "whiteList"))
        except (aiohttp.ClientError, ValueError) as err:
            await _record_anpr_action(request.app["hass"], {"entry_id": entry_id, "action": "add", "plate": str(payload.get("plate", ""))[:32], "outcome": "failed", "error": type(err).__name__})
            raise web.HTTPBadGateway(text=str(err)) from err
        await _record_anpr_action(request.app["hass"], {"entry_id": entry_id, "action": "add", "plate": plate, "outcome": "success"})
        return web.json_response({"ok": True}, status=201)

    async def put(self, request: web.Request, entry_id: str) -> web.Response:
        camera = await self._camera(request, entry_id)
        payload = await request.json()
        if not isinstance(payload, dict):
            raise web.HTTPBadRequest(text="Expected a JSON object")
        try:
            old_plate = camera._validate_plate(payload.get("old_plate", ""))
            plate = camera._validate_plate(payload.get("plate", ""))
            await camera.async_update_plate(old_plate, plate, name=str(payload.get("name", "")), list_type=payload.get("list_type", "whiteList"))
        except (aiohttp.ClientError, ValueError) as err:
            await _record_anpr_action(request.app["hass"], {"entry_id": entry_id, "action": "update", "plate": str(payload.get("plate", ""))[:32], "outcome": "failed", "error": type(err).__name__})
            raise web.HTTPBadGateway(text=str(err)) from err
        await _record_anpr_action(request.app["hass"], {"entry_id": entry_id, "action": "update", "old_plate": old_plate, "plate": plate, "outcome": "success"})
        return web.json_response({"ok": True})

    async def delete(self, request: web.Request, entry_id: str) -> web.Response:
        camera = await self._camera(request, entry_id)
        payload = await request.json()
        if not isinstance(payload, dict):
            raise web.HTTPBadRequest(text="Expected a JSON object")
        try:
            plate = camera._validate_plate(payload.get("plate", ""))
            await camera.async_delete_plate(plate)
        except (aiohttp.ClientError, ValueError) as err:
            await _record_anpr_action(request.app["hass"], {"entry_id": entry_id, "action": "remove", "plate": str(payload.get("plate", ""))[:32], "outcome": "failed", "error": type(err).__name__})
            raise web.HTTPBadGateway(text=str(err)) from err
        await _record_anpr_action(request.app["hass"], {"entry_id": entry_id, "action": "remove", "plate": plate, "outcome": "success"})
        return web.json_response({"ok": True})


class HikvisionCameraListView(HomeAssistantView):
    url = "/api/akuvox_ac/anpr"
    name = "api:akuvox_ac:hikvision_cameras"
    requires_auth = False

    async def get(self, request: web.Request) -> web.Response:
        from .http import _request_is_admin

        hass = request.app["hass"]
        if not _request_is_admin(hass, request):
            raise web.HTTPUnauthorized()
        cameras = []
        for entry_id, item in hass.data.get(DOMAIN, {}).items():
            camera = item.get("anpr") if isinstance(item, dict) else None
            if isinstance(camera, HikvisionISAPI):
                cameras.append({"entry_id": entry_id, "host": camera.host, "port": camera.port, "channel": camera.channel})
        audit_store = hass.data.get(DOMAIN, {}).get("anpr_audit_store")
        audit_data = hass.data.get(DOMAIN, {}).get("anpr_audit_data", {})
        if audit_store is None:
            audit_store = Store(hass, 1, _ANPR_AUDIT_KEY)
            audit_data = await audit_store.async_load() or {"events": []}
            hass.data[DOMAIN]["anpr_audit_store"] = audit_store
            hass.data[DOMAIN]["anpr_audit_data"] = audit_data
        events = audit_data.get("events", []) if isinstance(audit_data, dict) else []
        sanitized = _sanitize_anpr_audit_events(events)
        if sanitized != events:
            audit_data["events"] = sanitized
            await audit_store.async_save(audit_data)
        return web.json_response({"cameras": cameras, "events": sanitized[-50:]}, headers={"Cache-Control": "no-store"})


class HikvisionAdminPageView(HomeAssistantView):
    url = "/akuvox-ac/anpr"
    name = "akuvox_ac:hikvision_admin_page"
    requires_auth = False

    async def get(self, request: web.Request) -> web.Response:
        html = """<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><meta name="referrer" content="no-referrer"><title>Hikvision ANPR</title><style>*{box-sizing:border-box}body{margin:0;background:#071426;color:#edf5ff;font:15px system-ui}main{width:min(980px,calc(100% - 32px));margin:32px auto}.card{background:#102139;border:1px solid #29415d;border-radius:14px;padding:20px;margin:16px 0}h1{font-size:1.7rem;margin:.2rem 0}.muted{color:#adc0d6}input,select{background:#09172a;border:1px solid #36516f;color:#edf5ff;padding:.65rem;border-radius:7px}button{background:#1686ff;color:#fff;border:0;padding:.65rem .9rem;border-radius:8px;font:inherit;font-weight:700;cursor:pointer}button.danger{background:#913d48}.controls{display:flex;gap:10px;flex-wrap:wrap;margin:12px 0}.controls input{flex:1;min-width:150px}table{width:100%;border-collapse:collapse}td,th{text-align:left;padding:10px;border-top:1px solid #29415d}#status{min-height:1.5rem;color:#8fdec7}code{color:#b8d8ff}</style></head><body><main><h1>Hikvision ANPR registrations</h1><p class="muted">Manage the camera whitelist and blacklist through its local ISAPI interface. Camera firmware support can vary.</p><div class="card"><label for="camera">Configured camera</label><select id="camera"></select> <button id="refresh">Refresh list</button><div class="controls"><input id="plate" placeholder="Vehicle registration" maxlength="32"><input id="name" placeholder="Name / note"><select id="type"><option value="whiteList">Allow list</option><option value="blackList">Block list</option></select><button id="add">Add registration</button></div><p id="status" role="status" aria-live="polite"></p><table><thead><tr><th>Registration</th><th>List type</th><th>Camera ID</th><th></th></tr></thead><tbody id="rows"></tbody></table></div></main><script>const cameraSelect=document.querySelector('#camera');let entries=[];async function init(){try{const r=await fetch('/api/akuvox_ac/anpr',{cache:'no-store'});if(!r.ok)throw Error('Sign in to Home Assistant with dashboard access.');entries=(await r.json()).cameras;cameraSelect.innerHTML='';for(const c of entries){const o=document.createElement('option');o.value=c.entry_id;o.textContent=`${c.host}:${c.port} · channel ${c.channel}`;cameraSelect.append(o)}if(!entries.length)throw Error('Add a Hikvision ANPR camera from Settings → Devices & services → Add integration.');await refresh()}catch(e){document.querySelector('#status').textContent=e.message}}function endpoint(){return '/api/akuvox_ac/anpr/'+encodeURIComponent(cameraSelect.value)+'/plates'}async function refresh(){if(!cameraSelect.value)return;const status=document.querySelector('#status');status.textContent='Reading camera registrations…';try{const r=await fetch(endpoint(),{cache:'no-store'});const data=await r.json();if(!r.ok)throw Error(data.message||'Camera request failed. Check camera connectivity and Digest credentials.');const body=document.querySelector('#rows');body.innerHTML='';for(const p of data.plates){const row=document.createElement('tr');for(const value of [p.plate,p.type,p.id]){const cell=document.createElement('td');cell.textContent=value;row.append(cell)}const cell=document.createElement('td');const edit=document.createElement('button');edit.textContent='Edit';edit.onclick=async()=>{const replacement=prompt('Update vehicle registration',p.plate);if(replacement===null||!replacement.trim())return;const result=await fetch(endpoint(),{method:'PUT',headers:{'Content-Type':'application/json'},body:JSON.stringify({old_plate:p.plate,plate:replacement,name:'',list_type:p.type==='blackList'?'blackList':'whiteList'})});if(!result.ok){status.textContent='Could not update registration.';return}refresh()};const remove=document.createElement('button');remove.textContent='Remove';remove.className='danger';remove.onclick=async()=>{if(!confirm(`Remove ${p.plate} from the camera?`))return;const result=await fetch(endpoint(),{method:'DELETE',headers:{'Content-Type':'application/json'},body:JSON.stringify({plate:p.plate})});if(!result.ok){status.textContent='Could not remove registration.';return}refresh()};cell.append(edit,remove);row.append(cell);body.append(row)}status.textContent=`${data.plates.length} registrations loaded.`}catch(e){status.textContent=e.message}}document.querySelector('#refresh').onclick=refresh;cameraSelect.onchange=refresh;document.querySelector('#add').onclick=async()=>{const plate=document.querySelector('#plate').value.trim();if(!plate)return;const r=await fetch(endpoint(),{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({plate,name:document.querySelector('#name').value,list_type:document.querySelector('#type').value})});if(!r.ok){const d=await r.json();document.querySelector('#status').textContent=d.message||'Could not add registration.';return}document.querySelector('#plate').value='';document.querySelector('#name').value='';refresh()};init();</script></body></html>"""
        return web.Response(text=html, content_type="text/html", headers={"Cache-Control": "no-store", "Referrer-Policy": "no-referrer", "Content-Security-Policy": "default-src 'none'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; connect-src 'self'; base-uri 'none'; form-action 'none'"})


def register_hikvision_views(hass: Any) -> None:
    hass.http.register_view(HikvisionPlateView())
    hass.http.register_view(HikvisionCameraListView())
    hass.http.register_view(HikvisionAdminPageView())
