from __future__ import annotations

import datetime as dt
from pathlib import Path
from typing import Any, Dict, List, Optional

from aiohttp import web
from homeassistant.components.http.view import HomeAssistantView
from homeassistant.components.persistent_notification import async_create as notify
from homeassistant.core import HomeAssistant
from homeassistant.helpers.network import get_url

try:
    from homeassistant.components.http.const import KEY_HASS_USER
except ImportError:  # pragma: no cover - fallback for older HA cores
    KEY_HASS_USER = "hass_user"  # type: ignore[assignment]

from .const import DOMAIN

COMPONENT_ROOT = Path(__file__).parent
STATIC_ROOT = COMPONENT_ROOT / "www"
LOGIN_REDIRECT = "/"
FACE_DATA_PATH = "/api/AK_AC/FaceData"

DASHBOARD_ROUTES: Dict[str, str] = {
    "device-edit": "device_edit.html",
    "device_edit": "device_edit.html",
    "device_edit.html": "device_edit.html",
    "face-rec": "face_rec.html",
    "face_rec": "face_rec.html",
    "face_rec.html": "face_rec.html",
    "index": "index.html",
    "index.html": "index.html",
    "schedules": "schedules.html",
    "schedules.html": "schedules.html",
    "users": "users.html",
    "users.html": "users.html",
}


def face_base_url(hass: HomeAssistant, request: Optional[web.Request] = None) -> str:
    """Return the absolute base URL that serves face images."""

    base: Optional[str] = None

    if request is not None:
        try:
            base = str(request.url.origin())
        except Exception:
            base = None

    if not base:
        try:
            base = get_url(hass, prefer_external=True)
        except Exception:
            base = None

    if not base:
        base = hass.config.external_url or hass.config.internal_url or ""

    base = (base or "").rstrip("/")
    if base:
        return f"{base}{FACE_DATA_PATH}"
    return FACE_DATA_PATH


async def _require_auth(request: web.Request) -> None:
    if request.get(KEY_HASS_USER) is not None:
        return

    hass: HomeAssistant = request.app["hass"]
    token = ""
    auth_header = request.headers.get("Authorization")
    if auth_header and auth_header.lower().startswith("bearer "):
        token = auth_header.split(" ", 1)[1].strip()
    if not token:
        token = request.query.get("token", "").strip()

    validator = getattr(hass.auth, "async_validate_access_token", None)
    if token and validator:
        try:
            refresh_token = await validator(token)
            if refresh_token and getattr(refresh_token, "user", None):
                request[KEY_HASS_USER] = refresh_token.user  # type: ignore[index]
                return
        except Exception:
            pass

    raise web.HTTPFound(LOGIN_REDIRECT)


def _static_asset(path: str) -> Path:
    clean = path.strip()
    if not clean or clean.endswith("/"):
        clean = (clean.rstrip("/") + "/index.html") if clean else "index.html"
    candidate = (STATIC_ROOT / clean.lstrip("/")).resolve()
    try:
        candidate.relative_to(STATIC_ROOT.resolve())
    except ValueError:
        raise web.HTTPForbidden()
    if not candidate.is_file():
        raise web.HTTPNotFound()
    return candidate


def _only_hhmm(v: Optional[str]) -> str:
    if not v or v == "—":
        return "—"
    try:
        return v.strip()[:5]
    except Exception:
        return str(v)


def _is_ha_id(s: Any) -> bool:
    if not isinstance(s, str):
        return False
    return len(s) == 5 and s.startswith("HA") and s[2:].isdigit()


def _ha_id_from_int(n: int) -> str:
    return f"HA{n:03d}"


def _best_name(coord, entry_bucket: Dict[str, Any]) -> str:
    # Try a few places for a friendly, stable device name
    for key in ("device_name", "friendly_name", "name"):
        n = getattr(coord, key, None)
        if isinstance(n, str) and n.strip():
            return n.strip()
    try:
        n = coord.health.get("name") or coord.health.get("device_name")
        if isinstance(n, str) and n.strip():
            return n.strip()
    except Exception:
        pass
    try:
        opts = entry_bucket.get("options") or {}
        n = opts.get("device_name") or opts.get("name")
        if isinstance(n, str) and n.strip():
            return n.strip()
    except Exception:
        pass
    try:
        return str(getattr(coord, "name", "")) or "Akuvox Device"
    except Exception:
        return "Akuvox Device"


def _now_iso() -> str:
    return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def _profile_is_empty_reserved(p: Dict[str, Any]) -> bool:
    """True if this looks like a reservation that was never completed."""
    if not isinstance(p, dict):
        return False
    if str(p.get("status", "")).lower() not in ("pending", "reserved"):
        return False
    # no user content
    has_content = any(bool(p.get(k)) for k in ("name", "pin", "phone", "face_url"))
    return not has_content


def _cleanup_stale_reservations(hass: HomeAssistant, max_age_minutes: int = 120) -> int:
    """
    Remove pending empty reservations older than max_age_minutes.
    Returns count removed.
    """
    root = hass.data.get(DOMAIN, {}) or {}
    us = root.get("users_store")
    if not us:
        return 0
    try:
        allu = us.all() or {}
    except Exception:
        return 0

    cutoff = dt.datetime.utcnow() - dt.timedelta(minutes=max_age_minutes)
    to_del: List[str] = []
    for key, prof in allu.items():
        if not _is_ha_id(key):
            continue
        if not _profile_is_empty_reserved(prof):
            continue
        ts = str(prof.get("reserved_at") or "")
        too_old = True
        try:
            if ts:
                tsv = ts.rstrip("Z")
                stamp = dt.datetime.fromisoformat(tsv)
                too_old = stamp < cutoff
        except Exception:
            pass
        if too_old:
            to_del.append(key)

    removed = 0
    for k in to_del:
        try:
            us.data.get("users", {}).pop(k, None)  # type: ignore[attr-defined]
            removed += 1
        except Exception:
            pass
    if removed:
        try:
            hass.async_create_task(us.async_save())  # type: ignore[attr-defined]
        except Exception:
            pass
    return removed


# ========================= STATE =========================
class AkuvoxStaticAssets(HomeAssistantView):
    url = "/api/AK_AC/{path:.*}"
    name = "api:akuvox_ac:static"
    requires_auth = False

    async def get(self, request: web.Request, path: str = ""):
        await _require_auth(request)
        asset = _static_asset(path)
        return web.FileResponse(asset)


class AkuvoxDashboardView(HomeAssistantView):
    url = "/akuvox-ac/{slug:.*}"
    name = "akuvox_ac:dashboard"
    requires_auth = False

    async def get(self, request: web.Request, slug: str = ""):
        await _require_auth(request)

        clean = (slug or "").strip().strip("/").lower()
        if not clean:
            clean = "index"

        target = DASHBOARD_ROUTES.get(clean)
        if not target and clean.endswith(".html"):
            target = DASHBOARD_ROUTES.get(clean[:-5])

        if not target:
            raise web.HTTPNotFound()

        asset = _static_asset(target)
        return web.FileResponse(asset)


class AkuvoxUIView(HomeAssistantView):
    url = "/api/akuvox_ac/ui/state"
    name = "api:akuvox_ac:ui_state"
    requires_auth = True

    async def get(self, request: web.Request):
        hass: HomeAssistant = request.app["hass"]
        root = hass.data.get(DOMAIN, {}) or {}

        # Opportunistic cleanup of stale reservations
        try:
            _cleanup_stale_reservations(hass, max_age_minutes=120)
        except Exception:
            pass

        kpis: Dict[str, Any] = {
            "devices": 0,
            "users": 0,
            "pending": 0,
            "next_sync": "—",
            "auto_sync_time": None,
            "next_sync_eta": None,
        }
        devices: List[Dict[str, Any]] = []

        # Devices
        for entry_id, data in (root or {}).items():
            if entry_id in (
                "groups_store",
                "users_store",
                "schedules_store",
                "settings_store",
                "sync_manager",
                "sync_queue",
                "_ui_registered",
            ):
                continue
            coord = data.get("coordinator")
            if not coord:
                continue

            disp_name = _best_name(coord, data)
            dev = {
                "entry_id": entry_id,
                "name": disp_name,
                "type": (coord.health or {}).get("device_type"),
                "ip": (coord.health or {}).get("ip"),
                "online": (coord.health or {}).get("online", True),
                "sync_status": (coord.health or {}).get("sync_status", "pending"),
                "last_sync": (coord.health or {}).get("last_sync", "—"),
                "events": list(getattr(coord, "events", []) or []),
                "_users": list(getattr(coord, "users", []) or []),
                "users": list(getattr(coord, "users", []) or []),
                "exit_device": bool((data.get("options") or {}).get("exit_device", False)),
            }
            devices.append(dev)

        kpis["devices"] = len(devices)
        kpis["pending"] = sum(1 for d in devices if d.get("sync_status") != "in_sync")

        # Users KPI (only HA### ids)
        try:
            us = root.get("users_store")
            if us:
                kpis["users"] = sum(1 for k in (us.all() or {}).keys() if _is_ha_id(k))
        except Exception:
            pass

        # Next Sync + settings
        try:
            mgr = root.get("sync_manager")
            if mgr:
                kpis["next_sync"] = _only_hhmm(mgr.get_next_sync_text())
            sq = root.get("sync_queue")
            if getattr(sq, "next_sync_eta", None):
                kpis["next_sync_eta"] = getattr(sq, "next_sync_eta").isoformat()
            settings = root.get("settings_store")
            if settings:
                kpis["auto_sync_time"] = settings.get_auto_sync_time()
                kpis["auto_reboot"] = settings.get_auto_reboot()
        except Exception:
            pass

        # Registry users
        registry_users: List[Dict[str, Any]] = []
        try:
            us = root.get("users_store")
            if us:
                for key, prof in (us.all() or {}).items():
                    if not _is_ha_id(key):
                        continue
                    registry_users.append(
                        {
                            "id": key,
                            "name": (prof.get("name") or key),
                            "groups": prof.get("groups") or [],
                            "pin": prof.get("pin") or "",
                            "face_url": prof.get("face_url") or "",
                            "phone": prof.get("phone") or "",
                            "status": prof.get("status") or "active",
                            "schedule_name": prof.get("schedule_name") or "24/7 Access",
                            "key_holder": bool(prof.get("key_holder", False)),
                            "access_level": prof.get("access_level") or "",
                        }
                    )
        except Exception:
            pass

        # Schedules
        schedules = {}
        try:
            ss = root.get("schedules_store")
            if ss:
                schedules = ss.all()
        except Exception:
            pass

        return web.json_response(
            {"kpis": kpis, "devices": devices, "registry_users": registry_users, "schedules": schedules}
        )


# ========================= ACTIONS =========================
class AkuvoxUIAction(AkuvoxUIView):
    url = "/api/akuvox_ac/ui/action"
    name = "api:akuvox_ac:ui_action"

    async def post(self, request: web.Request):
        hass: HomeAssistant = request.app["hass"]
        root = hass.data.get(DOMAIN, {}) or {}
        try:
            data = await request.json()
        except Exception:
            data = {}
        action = data.get("action")
        payload = data.get("payload") or {}
        entry_id = data.get("entry_id")

        def err(msg: Exception | str, code: int = 400):
            text = str(msg) if isinstance(msg, str) else (str(msg) or "unknown error")
            return web.json_response({"ok": False, "error": text}, status=code)

        # Settings
        if action == "set_daily_sync":
            try:
                root["sync_manager"].set_auto_sync_time(payload["time"])
                return web.json_response({"ok": True})
            except Exception as e:
                return err(e)

        if action == "set_auto_reboot":
            try:
                t = payload.get("time")
                days = payload.get("days") or []
                root["sync_manager"].set_auto_reboot(t, days)
                return web.json_response({"ok": True})
            except Exception as e:
                return err(e)

        # Sync / Reboot
        if action == "sync_now":
            try:
                await root["sync_queue"].sync_now(entry_id)
                return web.json_response({"ok": True})
            except Exception as e:
                return err(e)

        if action in ("force_full_sync", "sync_all"):
            try:
                await root["sync_manager"].reconcile(full=True)
                return web.json_response({"ok": True})
            except Exception as e:
                return err(e)

        if action == "reboot_all":
            try:
                for eid, coord, api, _ in root["sync_manager"]._devices():
                    await api.system_reboot()
                return web.json_response({"ok": True})
            except Exception as e:
                return err(e)

        if action == "reboot_device":
            if not entry_id:
                return err("entry_id required")
            try:
                bucket = root.get(entry_id)
                api = bucket and bucket.get("api")
                if not api:
                    return err("Device not found", code=404)
                await api.system_reboot()
                return web.json_response({"ok": True})
            except Exception as e:
                return err(e)

        # Device options
        if action == "set_exit_device":
            if not entry_id:
                return err("entry_id required")
            try:
                enabled = bool(payload.get("enabled", True))
                root[entry_id]["options"]["exit_device"] = enabled
                await root["sync_queue"].sync_now(entry_id)
                return web.json_response({"ok": True})
            except Exception as e:
                return err(e)

        # Schedules
        if action == "upsert_schedule":
            try:
                name = payload["name"]
                spec = payload["spec"]
                await root["schedules_store"].upsert(name, spec)
                root["sync_queue"].mark_change(None)
                return web.json_response({"ok": True})
            except Exception as e:
                return err(e)

        if action == "delete_schedule":
            try:
                name = payload["name"]
                await root["schedules_store"].delete(name)
                root["sync_queue"].mark_change(None)
                return web.json_response({"ok": True})
            except Exception as e:
                return err(e)

        return err("unknown action")


# ========================= Devices list (still available if needed) =========================
class AkuvoxUIDevices(HomeAssistantView):
    url = "/api/akuvox_ac/ui/devices"
    name = "api:akuvox_ac:ui_devices"
    requires_auth = True

    async def get(self, request: web.Request):
        hass: HomeAssistant = request.app["hass"]
        root = hass.data.get(DOMAIN, {}) or {}

        out: List[Dict[str, str]] = []
        try:
            mgr = root.get("sync_manager")
            if not mgr:
                return web.json_response({"devices": out})

            for entry_id, coord, api, bucket_opts in mgr._devices():
                disp = _best_name(coord, hass.data[DOMAIN].get(entry_id, {}))
                out.append({"id": entry_id, "name": disp})
        except Exception:
            pass

        return web.json_response({"devices": out})


# ========================= NEW: list HA mobile app targets =========================
class AkuvoxUIPhones(HomeAssistantView):
    """
    GET -> { phones: [ { service: "mobile_app_johns_iphone", name: "Johns Iphone" }, ... ] }
    """
    url = "/api/akuvox_ac/ui/phones"
    name = "api:akuvox_ac:ui_phones"
    requires_auth = True

    async def get(self, request: web.Request):
        hass: HomeAssistant = request.app["hass"]
        phones: List[Dict[str, str]] = []

        try:
            svc = hass.services.async_services()  # { domain: { service: Service } }
            notify_services = svc.get("notify", {}) or {}
            for service_name in notify_services.keys():
                if not isinstance(service_name, str):
                    continue
                if not service_name.startswith("mobile_app_"):
                    continue
                # derive a friendlier label
                pretty = service_name.replace("mobile_app_", "").replace("_", " ").strip()
                if not pretty:
                    pretty = service_name
                phones.append({"service": service_name, "name": pretty.title()})
        except Exception:
            pass

        # stable ordering
        phones.sort(key=lambda x: x["name"].lower())
        return web.json_response({"phones": phones})


# ========================= Reserve a fresh HA ID =========================
class AkuvoxUIReserveId(HomeAssistantView):
    """
    GET → { ok: true, id: "HAxyz" }
    Pre-allocates the next free HA id with status='pending', reserved_at,
    and pre-fills face_url (public URL).
    """
    url = "/api/akuvox_ac/ui/reserve_id"
    name = "api:akuvox_ac:ui_reserve_id"
    requires_auth = True

    async def get(self, request: web.Request):
        hass: HomeAssistant = request.app["hass"]
        root = hass.data.get(DOMAIN, {}) or {}
        users_store = root.get("users_store")
        if not users_store:
            return web.json_response({"ok": False, "error": "users_store not ready"}, status=500)

        # Gather existing HA ids
        try:
            existing = set(k for k in (users_store.all() or {}).keys() if _is_ha_id(k))
        except Exception:
            existing = set()

        # Find lowest free HA###
        n = 1
        while True:
            candidate = _ha_id_from_int(n)
            if candidate not in existing:
                break
            n += 1

        # Reserve pending profile; set reserved_at and prefill face_url
        try:
            face_url = f"{face_base_url(hass, request)}/{candidate}.jpg"
            if hasattr(users_store, "upsert_profile"):
                await users_store.upsert_profile(candidate, status="pending", face_url=face_url)
                users_store.data.setdefault("users", {}).setdefault(candidate, {})["reserved_at"] = _now_iso()  # type: ignore[attr-defined]
                await users_store.async_save()  # type: ignore[attr-defined]
            else:
                data = users_store.data  # type: ignore[attr-defined]
                data.setdefault("users", {}).setdefault(candidate, {})["status"] = "pending"
                data["users"][candidate]["face_url"] = face_url
                data["users"][candidate]["reserved_at"] = _now_iso()
                await users_store.async_save()  # type: ignore[attr-defined]
        except Exception:
            pass

        return web.json_response({"ok": True, "id": candidate})


# ========================= NEW: release a reservation =========================
class AkuvoxUIReleaseId(HomeAssistantView):
    """
    POST JSON: { "id": "HA001" }
    If the profile is still an empty reservation (pending, no name/pin/phone/face),
    delete it from the registry.
    """
    url = "/api/akuvox_ac/ui/release_id"
    name = "api:akuvox_ac:ui_release_id"
    requires_auth = True

    async def post(self, request: web.Request):
        hass: HomeAssistant = request.app["hass"]
        root = hass.data.get(DOMAIN, {}) or {}
        users_store = root.get("users_store")
        if not users_store:
            return web.json_response({"ok": False, "error": "users_store not ready"}, status=500)

        try:
            data = await request.json()
        except Exception:
            data = {}
        uid = str(data.get("id") or "").strip()
        if not _is_ha_id(uid):
            return web.json_response({"ok": False, "error": "valid HA id required"}, status=400)

        try:
            prof = (users_store.all() or {}).get(uid) or {}
            if _profile_is_empty_reserved(prof):
                users_store.data.get("users", {}).pop(uid, None)  # type: ignore[attr-defined]
                await users_store.async_save()  # type: ignore[attr-defined]
                return web.json_response({"ok": True, "released": True})
            return web.json_response({"ok": True, "released": False})
        except Exception as e:
            return web.json_response({"ok": False, "error": str(e)}, status=500)


# ========================= Face upload =========================
class AkuvoxUIUploadFace(HomeAssistantView):
    """
    POST multipart/form-data:
      - id: HA001 (required)
      - file: image/jpeg (required)

    Saves to custom_components/AK_Access_ctrl/www/FaceData/<ID>.jpg
    Updates users_store face_url (public URL) and marks status=pending.
    Triggers immediate sync.
    """
    url = "/api/akuvox_ac/ui/upload_face"
    name = "api:akuvox_ac:ui_upload_face"
    requires_auth = True

    async def post(self, request: web.Request):
        hass: HomeAssistant = request.app["hass"]
        root = hass.data.get(DOMAIN, {}) or {}

        id_val: Optional[str] = None
        file_bytes: Optional[bytes] = None

        if request.content_type and "multipart" in request.content_type.lower():
            reader = await request.multipart()
            while True:
                part = await reader.next()
                if part is None:
                    break
                if part.name == "id":
                    id_val = (await part.text()).strip()
                elif part.name == "file":
                    # read file fully
                    chunks = []
                    while True:
                        chunk = await part.read_chunk()
                        if not chunk:
                            break
                        chunks.append(chunk)
                    file_bytes = b"".join(chunks)
        else:
            try:
                data = await request.json()
            except Exception:
                data = {}
            id_val = str(data.get("id") or "").strip()

        if not id_val or not _is_ha_id(id_val):
            return web.json_response({"ok": False, "error": "valid HA user id required (e.g. HA001)"}, status=400)
        if not file_bytes:
            return web.json_response({"ok": False, "error": "file is required (multipart/form-data)"}, status=400)

        # Save under FaceData (capital D)
        dest_dir = STATIC_ROOT / "FaceData"
        try:
            dest_dir.mkdir(parents=True, exist_ok=True)
        except Exception:
            pass
        dest_path = dest_dir / f"{id_val}.jpg"
        try:
            dest_path.write_bytes(file_bytes)
        except Exception as e:
            return web.json_response({"ok": False, "error": f"write failed: {e}"}, status=500)

        # Store public URL so intercom can fetch it
        face_url_public = f"{face_base_url(hass, request)}/{id_val}.jpg"

        # Update registry profile and mark pending
        try:
            users_store = root.get("users_store")
            if users_store:
                await users_store.upsert_profile(id_val, face_url=face_url_public, status="pending")
        except Exception:
            pass

        # Trigger immediate sync so FaceUrl is pushed right now
        try:
            await root["sync_queue"].sync_now(None)
        except Exception:
            try:
                root.get("sync_queue").mark_change(None, delay_minutes=0)
            except Exception:
                pass

        return web.json_response({"ok": True, "face_url": face_url_public})


# ========================= Remote enrol trigger (push to phone) =========================
class AkuvoxUIRemoteEnrol(HomeAssistantView):
    """
    POST JSON:
      {
        "id": "HA001",
        "phone_service": "mobile_app_johns_iphone"   # name of notify service (without 'notify.')
      }

    Sends a push notification to the selected HA mobile app with a deep link to the
    enrol page, marks user pending, and ensures face_url is set.
    Also leaves a persistent notification as a fallback.
    """
    url = "/api/akuvox_ac/ui/remote_enrol"
    name = "api:akuvox_ac:ui_remote_enrol"
    requires_auth = True

    async def post(self, request: web.Request):
        hass: HomeAssistant = request.app["hass"]
        root = hass.data.get(DOMAIN, {}) or {}

        try:
            data = await request.json()
        except Exception:
            data = {}
        user_id = str(data.get("id") or "").strip()
        phone_service = str(data.get("phone_service") or "").strip()

        if not _is_ha_id(user_id):
            return web.json_response({"ok": False, "error": "valid HA user id required"}, status=400)
        if not phone_service:
            return web.json_response({"ok": False, "error": "phone_service required"}, status=400)

        # Construct enrol URL (served from /api/AK_AC)
        enrol_url = f"/akuvox-ac/face-rec?user={user_id}"

        # Push via HA mobile app notify service
        try:
            await hass.services.async_call(
                "notify",
                phone_service,
                {
                    "title": "Akuvox: Face Enrolment",
                    "message": f"Tap to enrol face for {user_id}",
                    "data": {
                        "url": enrol_url
                    },
                },
                blocking=False,
            )
        except Exception:
            # Non-fatal; still proceed with persistent notification
            pass

        # Persistent notification fallback
        try:
            notify(
                hass,
                f"Face enrolment requested for **{user_id}**.\n\n"
                f"[Open enrolment page]({enrol_url})",
                title="Akuvox: Face Enrolment",
                notification_id=f"akuvox_face_enrol_{user_id}",
            )
        except Exception:
            pass

        # Ensure profile is pending and has a canonical face_url
        try:
            users_store = root.get("users_store")
            if users_store:
                await users_store.upsert_profile(
                    user_id,
                    status="pending",
                    face_url=f"{face_base_url(hass, request)}/{user_id}.jpg",
                )
        except Exception:
            pass

        # Optionally push a sync now
        try:
            await root["sync_queue"].sync_now(None)
        except Exception:
            pass

        return web.json_response({"ok": True, "enrol_url": enrol_url})


# ========================= REGISTER =========================
def register_ui(hass: HomeAssistant) -> None:
    hass.http.register_view(AkuvoxStaticAssets())
    hass.http.register_view(AkuvoxDashboardView())
    hass.http.register_view(AkuvoxUIView())
    hass.http.register_view(AkuvoxUIAction())
    hass.http.register_view(AkuvoxUIDevices())
    hass.http.register_view(AkuvoxUIPhones())
    hass.http.register_view(AkuvoxUIReserveId())
    hass.http.register_view(AkuvoxUIReleaseId())   # <-- new
    hass.http.register_view(AkuvoxUIUploadFace())
    hass.http.register_view(AkuvoxUIRemoteEnrol())
