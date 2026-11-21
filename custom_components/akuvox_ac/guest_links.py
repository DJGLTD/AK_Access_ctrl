"""Revocable, time-scoped guest access links for Home Assistant entities."""

from __future__ import annotations

import hashlib
import ipaddress
import asyncio
import secrets
from datetime import datetime, time, timezone
from typing import Any
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from aiohttp import web
from homeassistant.components.http.view import HomeAssistantView
from homeassistant.helpers.storage import Store

from .const import DOMAIN, EVENT_NON_KEY_ACCESS_GRANTED

STORE_VERSION = 1
STORE_KEY = f"{DOMAIN}_guest_links"
MAX_LINKS = 100
MAX_AUDIT = 2000
RATE_LIMIT_SECONDS = 5
ALLOWED_SERVICES = {
    ("cover", "open_cover"),
    ("cover", "close_cover"),
    ("light", "turn_on"),
    ("light", "turn_off"),
    ("switch", "turn_on"),
    ("switch", "turn_off"),
    ("button", "press"),
    ("script", "turn_on"),
    ("scene", "turn_on"),
}


def _root(hass: Any) -> dict[str, Any]:
    return hass.data.setdefault(DOMAIN, {})


async def _store(hass: Any) -> tuple[Store, dict[str, Any]]:
    root = _root(hass)
    load_lock = root.setdefault("guest_links_store_lock", asyncio.Lock())
    async with load_lock:
        store = root.get("guest_links_store")
        if store is None:
            store = Store(hass, STORE_VERSION, STORE_KEY)
            data = await store.async_load() or {"links": {}, "audit": []}
            if not isinstance(data, dict):
                data = {"links": {}, "audit": []}
            data.setdefault("links", {})
            data.setdefault("audit", [])
            root["guest_links_store"] = store
            root["guest_links_data"] = data
    return store, root["guest_links_data"]


def _json(request: web.Request) -> dict[str, Any]:
    try:
        value = request["json"]
    except KeyError:
        return {}
    return value if isinstance(value, dict) else {}


def _valid_action(action: Any) -> dict[str, str] | None:
    if not isinstance(action, dict):
        return None
    entity_id = str(action.get("entity_id") or "").strip()
    service = str(action.get("service") or "").strip()
    domain, separator, name = service.partition(".")
    if not separator or (domain, name) not in ALLOWED_SERVICES:
        return None
    if entity_id.count(".") != 1 or entity_id.split(".", 1)[0] != domain:
        return None
    label = str(action.get("label") or entity_id).strip()[:80]
    return {"entity_id": entity_id, "service": service, "label": label}


def _zone(timezone_name: str):
    try:
        return ZoneInfo(timezone_name)
    except ZoneInfoNotFoundError:
        if timezone_name.upper() in {"UTC", "ETC/UTC", "GMT"}:
            return timezone.utc
        raise


def _normalize_schedule_datetime(value: Any, zone: Any) -> str:
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=zone)
        round_trip = parsed.astimezone(timezone.utc).astimezone(zone).replace(tzinfo=None)
        if round_trip != datetime.fromisoformat(str(value)):
            raise ValueError("Local time does not exist in the selected timezone")
    return parsed.astimezone(timezone.utc).isoformat()


def _client_info(request: web.Request) -> dict[str, str]:
    peer = request.remote or ""
    try:
        address = str(ipaddress.ip_address(peer))
    except ValueError:
        address = "unknown"
    return {
        "ip": address,
        "user_agent": request.headers.get("User-Agent", "")[:512],
        "accept_language": request.headers.get("Accept-Language", "")[:128],
        "referer": request.headers.get("Referer", "")[:512],
        "client_hints": request.headers.get("Sec-CH-UA", "")[:256],
        "platform_hint": request.headers.get("Sec-CH-UA-Platform", "")[:128],
    }


def _within_policy(link: dict[str, Any], now: datetime) -> tuple[bool, str]:
    timezone_name = str(link.get("timezone") or "UTC")
    try:
        zone = _zone(timezone_name)
    except ZoneInfoNotFoundError:
        return False, "invalid_timezone"
    local_now = now.astimezone(zone)
    if link.get("starts_at"):
        start = datetime.fromisoformat(link["starts_at"])
        if start.tzinfo is None:
            start = start.replace(tzinfo=timezone.utc)
        if now < start:
            return False, "not_yet_active"
    if link.get("expires_at"):
        expiry = datetime.fromisoformat(link["expires_at"])
        if expiry.tzinfo is None:
            expiry = expiry.replace(tzinfo=timezone.utc)
        if now >= expiry:
            return False, "expired"
    weekdays = link.get("weekdays")
    if isinstance(weekdays, list) and weekdays and local_now.weekday() not in weekdays:
        return False, "outside_weekday_window"
    start_text = str(link.get("daily_start") or "00:00")
    end_text = str(link.get("daily_end") or "23:59")
    try:
        start_time = time.fromisoformat(start_text)
        end_time = time.fromisoformat(end_text)
    except ValueError:
        return False, "invalid_daily_window"
    current = local_now.timetz().replace(tzinfo=None, second=0, microsecond=0)
    if start_time <= end_time:
        allowed = start_time <= current <= end_time
    else:
        allowed = current >= start_time or current <= end_time
    return (True, "active") if allowed else (False, "outside_daily_window")


async def _audit(hass: Any, data: dict[str, Any], store: Store, event: dict[str, Any]) -> None:
    event["timestamp"] = datetime.now(timezone.utc).isoformat()
    data["audit"].append(event)
    data["audit"] = data["audit"][-MAX_AUDIT:]
    await store.async_save(data)


def _public_base_url(hass: Any) -> str:
    from homeassistant.helpers.network import get_url

    try:
        base_url = get_url(
            hass,
            allow_internal=False,
            allow_ip=False,
            require_ssl=True,
            prefer_cloud=True,
        )
    except TypeError:
        try:
            base_url = get_url(hass, prefer_external=True)
        except Exception as err:
            raise web.HTTPServiceUnavailable(
                text="Set up Home Assistant Cloud or an HTTPS external URL before creating guest links."
            ) from err
    except Exception as err:
        raise web.HTTPServiceUnavailable(
            text="Set up Home Assistant Cloud or an HTTPS external URL before creating guest links."
        ) from err
    parsed = urlsplit(base_url)
    if (
        parsed.scheme != "https"
        or not parsed.hostname
        or parsed.username
        or parsed.password
        or parsed.query
        or parsed.fragment
    ):
        raise web.HTTPServiceUnavailable(
            text="Set up Home Assistant Cloud or an HTTPS external URL before creating guest links."
        )
    try:
        ipaddress.ip_address(parsed.hostname)
    except ValueError:
        return base_url.rstrip("/")
    raise web.HTTPServiceUnavailable(
        text="Guest links require an HTTPS public hostname; configure Home Assistant Cloud or an external domain."
    )


class GuestLinkAdminView(HomeAssistantView):
    """Admin-only CRUD surface; raw tokens are returned only at creation."""

    url = "/api/akuvox_ac/guest_links"
    name = "api:akuvox_ac:guest_links_admin"
    requires_auth = False

    async def get(self, request: web.Request) -> web.Response:
        from .http import _request_is_admin

        hass = request.app["hass"]
        if not _request_is_admin(hass, request):
            raise web.HTTPUnauthorized()
        store, data = await _store(hass)
        links = [
            {key: value for key, value in item.items() if key != "token_hash"}
            for item in data["links"].values()
        ]
        return web.json_response({"links": links, "audit": data["audit"][-100:]}, headers={"Cache-Control": "no-store"})

    async def post(self, request: web.Request) -> web.Response:
        from .http import _request_is_admin

        hass = request.app["hass"]
        if not _request_is_admin(hass, request):
            raise web.HTTPUnauthorized()
        payload = await request.json()
        if not isinstance(payload, dict):
            raise web.HTTPBadRequest(text="Expected a JSON object")
        submitted_actions = payload.get("actions", [])
        if not isinstance(submitted_actions, list):
            raise web.HTTPBadRequest(text="Actions must be a list")
        actions = [_valid_action(item) for item in submitted_actions]
        actions = [action for action in actions if action]
        if not actions or len(actions) > 10:
            raise web.HTTPBadRequest(text="Select between 1 and 10 supported actions")
        timezone_name = str(payload.get("timezone") or "UTC")
        try:
            zone = _zone(timezone_name)
        except ZoneInfoNotFoundError as err:
            raise web.HTTPBadRequest(text="Unknown timezone") from err
        try:
            daily_start = time.fromisoformat(str(payload.get("daily_start") or "00:00")).isoformat(timespec="minutes")
            daily_end = time.fromisoformat(str(payload.get("daily_end") or "23:59")).isoformat(timespec="minutes")
        except ValueError as err:
            raise web.HTTPBadRequest(text="Daily times must use HH:MM") from err
        starts_at = payload.get("starts_at") or None
        expires_at = payload.get("expires_at") or None
        try:
            starts_at = _normalize_schedule_datetime(starts_at, zone) if starts_at else None
            expires_at = _normalize_schedule_datetime(expires_at, zone) if expires_at else None
        except ValueError as err:
            raise web.HTTPBadRequest(text="Start and expiry must be valid dates in the selected timezone") from err
        if starts_at and expires_at:
            if datetime.fromisoformat(expires_at) <= datetime.fromisoformat(starts_at):
                raise web.HTTPBadRequest(text="Expiry must be after the start time")
        weekdays = payload.get("weekdays", list(range(7)))
        if not isinstance(weekdays, list) or not weekdays or any(not isinstance(day, int) or day < 0 or day > 6 for day in weekdays):
            raise web.HTTPBadRequest(text="Weekdays must be numbers from 0 to 6")
        store, data = await _store(hass)
        if len(data["links"]) >= MAX_LINKS:
            raise web.HTTPConflict(text="Remove an existing link before creating another")
        base_url = _public_base_url(hass)
        raw_token = secrets.token_urlsafe(32)
        link_id = secrets.token_urlsafe(9)
        link = {
            "id": link_id,
            "token_hash": hashlib.sha256(raw_token.encode()).hexdigest(),
            "label": str(payload.get("label") or "Guest access")[:80],
            "actions": actions,
            "starts_at": str(starts_at) if starts_at else None,
            "expires_at": str(expires_at) if expires_at else None,
            "weekdays": weekdays,
            "daily_start": daily_start,
            "daily_end": daily_end,
            "timezone": timezone_name,
            "enabled": True,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "last_used": None,
            "use_count": 0,
        }
        data["links"][link_id] = link
        await store.async_save(data)
        url = f"{base_url}/akuvox-ac/share#{raw_token}"
        return web.json_response({"ok": True, "link": {key: value for key, value in link.items() if key != "token_hash"}, "url": url}, status=201, headers={"Cache-Control": "no-store", "Referrer-Policy": "no-referrer"})

class GuestLinkPublicView(HomeAssistantView):
    """Public capability endpoint; the unguessable path token is the credential."""

    url = "/api/akuvox_ac/guest"
    name = "api:akuvox_ac:guest"
    requires_auth = False

    @staticmethod
    def _token(request: web.Request) -> str:
        authorization = request.headers.get("Authorization", "")
        scheme, separator, token = authorization.partition(" ")
        if not separator or scheme.lower() != "bearer":
            return ""
        return token.strip()

    async def get(self, request: web.Request) -> web.Response:
        hass = request.app["hass"]
        store, data = await _store(hass)
        token = self._token(request)
        digest = hashlib.sha256(token.encode()).hexdigest()
        link = next((entry for entry in data["links"].values() if secrets.compare_digest(entry["token_hash"], digest)), None)
        if not link or not link.get("enabled"):
            raise web.HTTPNotFound()
        now = datetime.now(timezone.utc)
        active, reason = _within_policy(link, now)
        await _audit(hass, data, store, {"link_id": link["id"], "label": link["label"], "outcome": "viewed" if active else reason, **_client_info(request)})
        response = web.json_response({"label": link["label"], "actions": [{"label": action["label"], "id": index} for index, action in enumerate(link["actions"])], "active": active, "reason": reason}, headers={"Cache-Control": "no-store", "Referrer-Policy": "no-referrer", "X-Content-Type-Options": "nosniff"})
        return response

    async def post(self, request: web.Request) -> web.Response:
        hass = request.app["hass"]
        store, data = await _store(hass)
        token = self._token(request)
        digest = hashlib.sha256(token.encode()).hexdigest()
        link = next((entry for entry in data["links"].values() if secrets.compare_digest(entry["token_hash"], digest)), None)
        if not link or not link.get("enabled"):
            raise web.HTTPNotFound()
        locks = _root(hass).setdefault("guest_link_locks", {})
        lock = locks.setdefault(link["id"], asyncio.Lock())
        async with lock:
            return await self._perform_action(hass, request, store, data, link)

    async def _perform_action(self, hass: Any, request: web.Request, store: Store, data: dict[str, Any], link: dict[str, Any]) -> web.Response:
        info = _client_info(request)
        now = datetime.now(timezone.utc)
        active, reason = _within_policy(link, now)
        payload = await request.json()
        action_index = payload.get("action") if isinstance(payload, dict) else None
        try:
            action_index = int(action_index)
            action = link["actions"][action_index]
        except (TypeError, ValueError, IndexError):
            raise web.HTTPBadRequest(text="Unknown action")
        if not active:
            await _audit(hass, data, store, {"link_id": link["id"], "label": link["label"], "action": action["label"], "outcome": reason, **info})
            raise web.HTTPForbidden(text=reason)
        last = link.get("last_used")
        if last:
            try:
                last_at = datetime.fromisoformat(last)
                if (now - last_at).total_seconds() < RATE_LIMIT_SECONDS:
                    await _audit(hass, data, store, {"link_id": link["id"], "label": link["label"], "action": action["label"], "outcome": "rate_limited", **info})
                    raise web.HTTPTooManyRequests()
            except ValueError:
                pass
        link["last_used"] = now.isoformat()
        link["use_count"] = int(link.get("use_count") or 0) + 1
        await store.async_save(data)
        domain, service = action["service"].split(".", 1)
        await _audit(hass, data, store, {"link_id": link["id"], "label": link["label"], "action": action["label"], "outcome": "action_started", **info})
        outcome = "success"
        error = None
        try:
            await hass.services.async_call(domain, service, {"entity_id": action["entity_id"]}, blocking=True)
        except Exception as err:
            outcome = "failed"
            error = type(err).__name__
        event = {"link_id": link["id"], "label": link["label"], "action": action["label"], "outcome": outcome, "error": error, **info}
        await _audit(hass, data, store, event)
        if outcome == "success":
            hass.bus.async_fire(EVENT_NON_KEY_ACCESS_GRANTED, {"source": "guest_link", "link_id": link["id"], "name": link["label"], "action": action["label"], **info})
            hass.bus.async_fire(f"{DOMAIN}_guest_action", event)
            await self._send_access_alert(hass, store, data, link, action, info)
        return web.json_response({"ok": outcome == "success", "error": error}, status=200 if outcome == "success" else 502, headers={"Cache-Control": "no-store", "Referrer-Policy": "no-referrer"})

    @staticmethod
    async def _send_access_alert(hass: Any, store: Store, data: dict[str, Any], link: dict[str, Any], action: dict[str, Any], info: dict[str, str]) -> None:
        """Send guest access alerts to their explicitly selected recipients."""
        try:
            from .coordinator import _alert_targets_for_event

            targets = _alert_targets_for_event(hass, "guest_access")
        except Exception:
            targets = []
        service = getattr(hass, "services", None)
        if not targets or service is None or not hasattr(service, "async_call"):
            return
        notification = {
            "title": "Guest access used",
            "message": f"{link['label']}: {action['label']} was triggered.",
            "data": {
                "group": "akuvox-guest-access",
                "link_id": link["id"],
                "action": action["label"],
            },
        }
        for target in targets:
            try:
                await service.async_call("notify", target, notification, blocking=False)
                await _audit(hass, data, store, {"link_id": link["id"], "label": link["label"], "action": action["label"], "outcome": "alert_sent", "target": target, **info})
            except Exception as err:
                await _audit(hass, data, store, {"link_id": link["id"], "label": link["label"], "action": action["label"], "outcome": "alert_failed", "target": target, "error": type(err).__name__, **info})


class GuestLinkPageView(HomeAssistantView):
    """Self-contained page so guest links do not depend on a HA login or CDN."""

    url = "/akuvox-ac/share"
    name = "akuvox_ac:guest_page"
    requires_auth = False

    async def get(self, request: web.Request) -> web.Response:
        html = """<!doctype html><html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><meta name="referrer" content="no-referrer"><title>Guest access</title><style>body{margin:0;background:#071426;color:#edf5ff;font:16px system-ui;display:grid;min-height:100vh;place-items:center}main{width:min(32rem,calc(100% - 2rem));padding:1.5rem;background:#102139;border:1px solid #29415d;border-radius:1rem}h1{font-size:1.35rem}button{width:100%;margin:.5rem 0;padding:1rem;border:0;border-radius:.65rem;background:#1686ff;color:white;font:inherit;font-weight:700}button:disabled{opacity:.5}#status{color:#b8c9dc}</style><main><h1 id="title">Loading access…</h1><p id="status" role="status" aria-live="polite"></p><section id="actions"></section></main><script>const token=decodeURIComponent(location.hash.slice(1));history.replaceState(null,'',location.pathname);const api='/api/akuvox_ac/guest';const headers={'Authorization':'Bearer '+token};async function load(){try{const r=await fetch(api,{headers,cache:'no-store'});if(!r.ok)throw Error('This access link is invalid or unavailable.');const d=await r.json();document.querySelector('#title').textContent=d.label;document.querySelector('#status').textContent=d.active?'Choose an action below.':({'expired':'This link has expired.','not_yet_active':'This link is not active yet.','outside_weekday_window':'Access is not available today.','outside_daily_window':'Access is not available at this time.'}[d.reason]||'This link is unavailable.');for(const a of d.actions){const b=document.createElement('button');b.textContent=a.label;b.disabled=!d.active;b.onclick=async()=>{b.disabled=true;document.querySelector('#status').textContent='Sending…';try{const result=await fetch(api,{method:'POST',headers:{...headers,'Content-Type':'application/json'},body:JSON.stringify({action:a.id}),cache:'no-store'});if(!result.ok)throw Error('Action was not completed.');document.querySelector('#status').textContent='Action completed.';setTimeout(load,5000)}catch(e){document.querySelector('#status').textContent=e.message;b.disabled=false}};document.querySelector('#actions').append(b)}}catch(e){document.querySelector('#title').textContent='Guest access unavailable';document.querySelector('#status').textContent=e.message}}load();</script></main></html>"""
        return web.Response(text=html, content_type="text/html", headers={"Cache-Control": "no-store", "Referrer-Policy": "no-referrer", "Content-Security-Policy": "default-src 'none'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; connect-src 'self'; base-uri 'none'; form-action 'none'"})


class GuestLinkAdminPageView(HomeAssistantView):
    """Local manager for creating and revoking guest links."""

    url = "/akuvox-ac/guest-links"
    name = "akuvox_ac:guest_link_admin_page"
    requires_auth = False

    async def get(self, request: web.Request) -> web.Response:
        html = """<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><meta name="referrer" content="no-referrer"><title>Guest access links</title><style>
*{box-sizing:border-box}body{margin:0;background:#071426;color:#edf5ff;font:15px system-ui,-apple-system,"Segoe UI",sans-serif}main{width:min(980px,calc(100% - 32px));margin:32px auto}h1{font-size:1.7rem;margin:0}.sub{color:#adc0d6;margin:.4rem 0 1.5rem}.card{background:#102139;border:1px solid #29415d;border-radius:14px;padding:20px;margin:16px 0}label{display:block;font-size:.84rem;color:#c4d2e2;margin:.6rem 0 .25rem}input,select{background:#09172a;border:1px solid #36516f;color:#edf5ff;padding:.65rem;border-radius:7px;width:100%}input[type=checkbox]{width:auto}button{background:#1686ff;color:white;border:0;padding:.7rem 1rem;border-radius:8px;font:inherit;font-weight:700;cursor:pointer}button.secondary{background:#203650}button.danger{background:#913d48}.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(190px,1fr));gap:12px}.action{display:grid;grid-template-columns:1.2fr 1fr auto;gap:9px;align-items:end;margin:.5rem 0}.days{display:flex;gap:13px;flex-wrap:wrap}.days label{display:flex;gap:5px;align-items:center}.row{display:flex;justify-content:space-between;gap:16px;align-items:center;border-top:1px solid #29415d;padding:12px 0}.muted{color:#adc0d6}.result{overflow-wrap:anywhere;background:#09172a;padding:12px;border-radius:8px}.hidden{display:none}.status{min-height:1.5rem;color:#80d7bd}@media(max-width:600px){main{margin:18px auto}.card{padding:15px}.action{grid-template-columns:1fr}.row{align-items:flex-start}}
</style></head><body><main><h1>Guest access links</h1><p class="sub">Create time-limited buttons for a gate, lights, or other approved devices. Anyone with a link can use its listed actions.</p><div class="card"><h2>Create a link</h2><div class="grid"><div><label for="label">Link name</label><input id="label" maxlength="80" placeholder="Visitor access"></div><div><label for="timezone">Time zone</label><input id="timezone"></div><div><label for="starts">Starts at (optional)</label><input id="starts" type="datetime-local"></div><div><label for="expires">Expires at (optional)</label><input id="expires" type="datetime-local"></div><div><label for="daily-start">Daily access starts</label><input id="daily-start" type="time" value="00:00"></div><div><label for="daily-end">Daily access ends</label><input id="daily-end" type="time" value="23:59"></div></div><label>Available days</label><div class="days" id="days"></div><h3>Actions</h3><div id="actions"></div><button class="secondary" id="add-action" type="button">Add another action</button> <button id="create" type="button">Create guest link</button><p class="status" id="status" role="status" aria-live="polite"></p><div id="created" class="hidden"><p>Copy this URL now. It will not be shown again.</p><div class="result" id="url"></div><button class="secondary" id="copy" type="button">Copy URL</button></div></div><div class="card"><h2>Active links</h2><div id="links" class="muted">Loading…</div></div></main><script>
const api='/api/akuvox_ac/guest_links';const dayNames=['Mon','Tue','Wed','Thu','Fri','Sat','Sun'];document.querySelector('#timezone').value=Intl.DateTimeFormat().resolvedOptions().timeZone||'UTC';document.querySelector('#days').innerHTML=dayNames.map((day,i)=>`<label><input type="checkbox" value="${i}" checked>${day}</label>`).join('');function addAction(){const row=document.createElement('div');row.className='action';row.innerHTML='<div><label>Button label</label><input class="action-label" placeholder="Open gate"></div><div><label>Home Assistant entity</label><input class="entity" placeholder="cover.front_gate"></div><div><label>Action</label><select class="service"><option value="cover.open_cover">Open cover / gate</option><option value="cover.close_cover">Close cover / gate</option><option value="light.turn_on">Turn on light</option><option value="light.turn_off">Turn off light</option><option value="switch.turn_on">Turn on switch</option><option value="switch.turn_off">Turn off switch</option><option value="button.press">Press button</option><option value="script.turn_on">Run script</option><option value="scene.turn_on">Activate scene</option></select></div><button class="danger remove" type="button">Remove</button>';row.querySelector('.remove').onclick=()=>row.remove();document.querySelector('#actions').append(row)}addAction();document.querySelector('#add-action').onclick=addAction;async function load(){try{const r=await fetch(api,{cache:'no-store'});if(!r.ok)throw Error('Sign in to Home Assistant and make sure you have dashboard access.');const data=await r.json();const links=document.querySelector('#links');if(!data.links.length){links.textContent='No active links.';return}links.innerHTML='';for(const item of data.links){const row=document.createElement('div');row.className='row';const desc=document.createElement('div');desc.innerHTML='';const title=document.createElement('strong');title.textContent=item.label;const detail=document.createElement('div');detail.className='muted';detail.textContent=`${item.actions.map(a=>a.label).join(', ')} · used ${item.use_count} times · ${item.expires_at?'expires '+item.expires_at:'no expiry'}`;desc.append(title,detail);const revoke=document.createElement('button');revoke.className='danger';revoke.textContent='Revoke';revoke.onclick=async()=>{if(!confirm('Revoke this link now?'))return;const result=await fetch(`${api}/${encodeURIComponent(item.id)}`,{method:'DELETE'});if(result.ok)load()};row.append(desc,revoke);links.append(row)}}catch(e){document.querySelector('#links').textContent=e.message}}document.querySelector('#create').onclick=async()=>{const status=document.querySelector('#status');status.textContent='Creating…';const actions=[...document.querySelectorAll('.action')].map(row=>({label:row.querySelector('.action-label').value.trim(),entity_id:row.querySelector('.entity').value.trim(),service:row.querySelector('.service').value}));const dateValue=id=>document.querySelector(id).value||null;const payload={label:document.querySelector('#label').value,timezone:document.querySelector('#timezone').value,starts_at:dateValue('#starts'),expires_at:dateValue('#expires'),daily_start:document.querySelector('#daily-start').value,daily_end:document.querySelector('#daily-end').value,weekdays:[...document.querySelectorAll('#days input:checked')].map(input=>Number(input.value)),actions};try{const r=await fetch(api,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(payload),cache:'no-store'});const result=await r.json().catch(()=>({}));if(!r.ok)throw Error(result.message||result.error||'Could not create link. Configure an HTTPS external Home Assistant URL or enable Home Assistant Cloud.');document.querySelector('#url').textContent=result.url;document.querySelector('#created').classList.remove('hidden');status.textContent='Link created. Copy it now; the URL is shown once.';await load()}catch(e){status.textContent=e.message}};document.querySelector('#copy').onclick=async()=>{await navigator.clipboard.writeText(document.querySelector('#url').textContent);document.querySelector('#status').textContent='URL copied.'};load();
</script></body></html>"""
        return web.Response(text=html, content_type="text/html", headers={"Cache-Control": "no-store", "Referrer-Policy": "no-referrer", "Content-Security-Policy": "default-src 'none'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; connect-src 'self'; base-uri 'none'; form-action 'none'"})


def register_guest_link_views(hass: Any) -> None:
    hass.http.register_view(GuestLinkAdminView())
    hass.http.register_view(GuestLinkPublicView())
    hass.http.register_view(GuestLinkPageView())
    hass.http.register_view(GuestLinkAdminPageView())
    hass.http.register_view(GuestLinkRevokeView())


class GuestLinkRevokeView(HomeAssistantView):
    """Immediate revocation route for a single link."""

    url = "/api/akuvox_ac/guest_links/{link_id}"
    name = "api:akuvox_ac:guest_link_revoke"
    requires_auth = False

    async def delete(self, request: web.Request, link_id: str) -> web.Response:
        from .http import _request_is_admin

        hass = request.app["hass"]
        if not _request_is_admin(hass, request):
            raise web.HTTPUnauthorized()
        store, data = await _store(hass)
        link = data["links"].pop(link_id, None)
        if link is None:
            raise web.HTTPNotFound()
        await _audit(hass, data, store, {"link_id": link_id, "label": link["label"], "outcome": "revoked", **_client_info(request)})
        return web.json_response({"ok": True})
