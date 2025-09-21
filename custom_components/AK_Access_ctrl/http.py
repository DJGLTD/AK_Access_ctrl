from __future__ import annotations

import datetime as dt
import logging
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from aiohttp import web
from homeassistant.components.http.view import HomeAssistantView
from homeassistant.components.persistent_notification import async_create as notify
from homeassistant.core import HomeAssistant

from homeassistant.helpers.network import get_url

from .const import (
    DOMAIN,
    CONF_DEVICE_GROUPS,
    INTEGRATION_VERSION,
    INTEGRATION_VERSION_LABEL,
)

COMPONENT_ROOT = Path(__file__).parent
STATIC_ROOT = COMPONENT_ROOT / "www"
FACE_DATA_PATH = "/api/AK_AC/FaceData"

RESERVATION_TTL_MINUTES = 2

_LOGGER = logging.getLogger(__name__)


def _persistent_face_dir(hass: HomeAssistant) -> Path:
    root = Path(hass.config.path("www"))
    return (root / "AK_Access_ctrl" / "FaceData").resolve()

DASHBOARD_ROUTES: Dict[str, str] = {
    "head": "head.html",
    "head.html": "head.html",
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
    "settings": "settings.html",
    "settings.html": "settings.html",
    "unauthorized": "unauthorized.html",
    "unauthorized.html": "unauthorized.html",
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


def _context_user_name(hass: HomeAssistant, context) -> str:
    """Return a friendly name for the user behind an HTTP/service call."""

    default = "HA User"
    if context is None:
        return default

    user_id = getattr(context, "user_id", None)
    if not user_id:
        return default

    try:
        user = hass.auth.async_get_user(user_id)
        if user:
            if user.name:
                return user.name
            if user.id:
                return user.id
    except Exception:
        return default

    return default


def _flag_rebooting(coord, *, triggered_by: str, duration: float = 300.0) -> None:
    """Mirror the reboot status tracking used by the service helper."""

    try:
        coord.health["status"] = "rebooting"
        coord.health["online"] = False
        coord.health["rebooting_until"] = time.time() + duration
        coord.health["last_error"] = None
    except Exception:
        pass

    try:
        coord._append_event(f"Device Rebooted by - {triggered_by}")  # type: ignore[attr-defined]
    except Exception:
        pass


async def _reboot_devices_direct(
    root: Dict[str, Any], *, entry_id: Optional[str], triggered_by: str
) -> bool:
    """Best-effort reboot helper used when the HA service call fails."""

    manager = root.get("sync_manager")
    targets: List[Any] = []

    if entry_id:
        data = root.get(entry_id) or {}
        coord = data.get("coordinator")
        api = data.get("api")
        if coord and api:
            targets.append((coord, api))
        else:
            raise RuntimeError("Device not available for reboot")
    elif manager:
        for _entry_id, coord, api, *_ in manager._devices():
            if coord and api:
                targets.append((coord, api))

    if not targets:
        raise RuntimeError("No devices available to reboot")

    success = False

    for coord, api in targets:
        try:
            await api.system_reboot()
        except Exception as err:
            try:
                coord._append_event(f"Reboot failed: {err}")  # type: ignore[attr-defined]
            except Exception:
                pass
            continue

        success = True
        _flag_rebooting(coord, triggered_by=triggered_by)

        try:
            await coord.async_request_refresh()
        except Exception:
            pass

    if not success:
        raise RuntimeError("Failed to trigger reboot")

    return True


def _normalize_groups(groups: Any) -> List[str]:
    """Return a JSON-serialisable list of group names."""

    if isinstance(groups, list):
        return [str(g) for g in groups]
    if isinstance(groups, (set, tuple)):
        return [str(g) for g in groups]
    if groups in (None, ""):
        return []
    return [str(groups)]


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
    has_core = any(bool(p.get(k)) for k in ("name", "pin", "phone"))
    if has_core:
        return False
    return True


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
        hass: HomeAssistant = request.app["hass"]
        clean = (path or "").lstrip("/")
        if clean.lower().startswith("facedata"):
            rel = clean[8:].lstrip("/")
            if rel:
                base = _persistent_face_dir(hass)
                candidate = (base / rel).resolve()
                try:
                    candidate.relative_to(base)
                except ValueError:
                    raise web.HTTPForbidden()
                if candidate.is_file():
                    return web.FileResponse(candidate)
        asset = _static_asset(path)
        if clean.lower().startswith("facedata"):
            rel = clean[8:].lstrip("/")
            if rel:
                try:
                    dest_dir = _persistent_face_dir(hass)
                    dest_dir.mkdir(parents=True, exist_ok=True)
                    dest = (dest_dir / rel).resolve()
                    dest.relative_to(dest_dir)
                    if not dest.exists() and asset.is_file():
                        dest.write_bytes(asset.read_bytes())
                except Exception:
                    pass
        return web.FileResponse(asset)


class AkuvoxDashboardView(HomeAssistantView):
    url = "/akuvox-ac/{slug:.*}"
    name = "akuvox_ac:dashboard"
    requires_auth = False

    async def get(self, request: web.Request, slug: str = ""):
        clean = (slug or "").strip().strip("/").lower()
        if not clean:
            clean = "head"

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

        try:
            _cleanup_stale_reservations(hass, max_age_minutes=RESERVATION_TTL_MINUTES)
        except Exception as err:
            _LOGGER.debug("Reservation cleanup failed: %s", err)

        response: Dict[str, Any] = {
            "kpis": {
                "devices": 0,
                "users": 0,
                "pending": 0,
                "next_sync": "—",
                "auto_sync_time": None,
                "next_sync_eta": None,
                "version": INTEGRATION_VERSION_LABEL,
                "version_raw": INTEGRATION_VERSION,
            },
            "devices": [],
            "registry_users": [],
            "schedules": {},
            "groups": [],
            "all_groups": [],
        }

        kpis: Dict[str, Any] = response["kpis"]
        devices: List[Dict[str, Any]] = response["devices"]

        try:
            for entry_id, data in list((root or {}).items()):
                if entry_id in (
                    "groups_store",
                    "users_store",
                    "schedules_store",
                    "settings_store",
                    "sync_manager",
                    "sync_queue",
                    "_ui_registered",
                    "_panel_registered",
                ):
                    continue

                if not isinstance(data, dict):
                    continue

                coord = data.get("coordinator")
                if not coord:
                    continue

                disp_name = _best_name(coord, data)
                opts = data.get("options") or {}
                dev = {
                    "entry_id": entry_id,
                    "name": disp_name,
                    "type": (coord.health or {}).get("device_type"),
                    "ip": (coord.health or {}).get("ip"),
                    "online": (coord.health or {}).get("online", True),
                    "status": (coord.health or {}).get("status"),
                    "sync_status": (coord.health or {}).get("sync_status", "pending"),
                    "last_sync": (coord.health or {}).get("last_sync", "—"),
                    "events": list(getattr(coord, "events", []) or []),
                    "_users": list(getattr(coord, "users", []) or []),
                    "users": list(getattr(coord, "users", []) or []),
                    "exit_device": bool(opts.get("exit_device", False)),
                    "participate_in_sync": bool(opts.get("participate_in_sync", True)),
                    "sync_groups": list(opts.get("sync_groups") or ["Default"]),
                }
                devices.append(dev)

            kpis["devices"] = len(devices)
            kpis["pending"] = sum(
                1
                for d in devices
                if d.get("sync_status") != "in_sync" and d.get("online", True)
            )
            kpis["sync_active"] = any(
                d.get("online", True) and d.get("sync_status") == "in_progress"
                for d in devices
            )

            us = root.get("users_store")
            if us:
                try:
                    profiles = us.all() or {}
                except Exception:
                    profiles = {}
                kpis["users"] = sum(
                    1
                    for key, prof in profiles.items()
                    if _is_ha_id(key) and not _profile_is_empty_reserved(prof)
                )

            mgr = root.get("sync_manager")
            if mgr:
                try:
                    kpis["next_sync"] = _only_hhmm(mgr.get_next_sync_text())
                except Exception:
                    pass
            sq = root.get("sync_queue")
            if getattr(sq, "next_sync_eta", None):
                try:
                    kpis["next_sync_eta"] = getattr(sq, "next_sync_eta").isoformat()
                except Exception:
                    pass
            settings = root.get("settings_store")
            if settings:
                try:
                    kpis["auto_sync_time"] = settings.get_auto_sync_time()
                    kpis["auto_reboot"] = settings.get_auto_reboot()
                except Exception:
                    pass

            registry_users: List[Dict[str, Any]] = []
            if us:
                try:
                    all_users = us.all() or {}
                except Exception:
                    all_users = {}
                for key, prof in all_users.items():
                    if not _is_ha_id(key) or _profile_is_empty_reserved(prof):
                        continue
                    groups = _normalize_groups(prof.get("groups"))
                    registry_users.append(
                        {
                            "id": key,
                            "name": (prof.get("name") or key),
                            "groups": groups,
                            "pin": prof.get("pin") or "",
                            "face_url": prof.get("face_url") or "",
                            "phone": prof.get("phone") or "",
                            "status": prof.get("status") or "active",
                            "schedule_name": prof.get("schedule_name")
                            or "24/7 Access",
                            "schedule_id": prof.get("schedule_id") or "",
                            "key_holder": bool(prof.get("key_holder", False)),
                            "access_level": prof.get("access_level") or "",
                        }
                    )
            response["registry_users"] = registry_users

            schedules = {}
            ss = root.get("schedules_store")
            if ss:
                try:
                    schedules = ss.all()
                except Exception:
                    schedules = {}
            response["schedules"] = schedules

            groups: List[str] = []
            gs = root.get("groups_store")
            if gs:
                try:
                    groups = gs.groups()
                except Exception:
                    groups = []
            response["groups"] = groups
            response["all_groups"] = groups

        except Exception as err:
            _LOGGER.debug("Failed to build Akuvox state payload: %s", err)

        return web.json_response(response)


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
        ctx = request["context"] if "context" in request else None

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
            queue = root.get("sync_queue")
            try:
                service_data = {"entry_id": entry_id} if entry_id else {}
                await hass.services.async_call(DOMAIN, "sync_now", service_data, blocking=True, context=ctx)
                return web.json_response({"ok": True})
            except Exception as service_err:
                _LOGGER.debug("Sync-now service call failed via UI: %s", service_err)
                if not queue:
                    return err(service_err)
                try:
                    await queue.sync_now(entry_id)
                    return web.json_response({"ok": True})
                except Exception as queue_err:
                    return err(queue_err)

        if action in ("force_full_sync", "sync_all"):
            queue = root.get("sync_queue")
            manager = root.get("sync_manager")
            try:
                service_data = {"entry_id": entry_id} if entry_id else {}
                await hass.services.async_call(
                    DOMAIN, "force_full_sync", service_data, blocking=True, context=ctx
                )
                return web.json_response({"ok": True})
            except Exception as service_err:
                _LOGGER.debug("Force full sync service call failed via UI: %s", service_err)
                if not queue or not manager:
                    return err(service_err)

                triggered_by = _context_user_name(hass, ctx)
                coords: List[Any] = []

                if entry_id:
                    data = root.get(entry_id) or {}
                    coord = data.get("coordinator")
                    if coord:
                        coords.append(coord)
                else:
                    try:
                        for _entry_id, coord, *_ in manager._devices():
                            if coord:
                                coords.append(coord)
                    except Exception:
                        coords = []

                for coord in coords:
                    try:
                        coord._append_event(  # type: ignore[attr-defined]
                            f"Full Sync Triggered by - {triggered_by}"
                        )
                    except Exception:
                        pass

                try:
                    await queue.sync_now(entry_id)
                    return web.json_response({"ok": True})
                except Exception as queue_err:
                    return err(queue_err)

        if action == "reboot_all":
            try:
                await hass.services.async_call(
                    DOMAIN, "reboot_device", {}, blocking=True, context=ctx
                )
                return web.json_response({"ok": True})
            except Exception as service_err:
                _LOGGER.debug("Reboot-all service call failed via UI: %s", service_err)
                triggered_by = _context_user_name(hass, ctx)
                try:
                    await _reboot_devices_direct(root, entry_id=None, triggered_by=triggered_by)
                    return web.json_response({"ok": True})
                except Exception as fallback_err:
                    return err(fallback_err)

        if action == "reboot_device":
            if not entry_id:
                return err("entry_id required")
            try:
                await hass.services.async_call(
                    DOMAIN,
                    "reboot_device",
                    {"entry_id": entry_id},
                    blocking=True,
                    context=ctx,
                )
                return web.json_response({"ok": True})
            except Exception as service_err:
                _LOGGER.debug(
                    "Reboot service call failed via UI for %s: %s", entry_id, service_err
                )
                triggered_by = _context_user_name(hass, ctx)
                try:
                    await _reboot_devices_direct(
                        root, entry_id=entry_id, triggered_by=triggered_by
                    )
                    return web.json_response({"ok": True})
                except Exception as fallback_err:
                    return err(fallback_err)

        # Device options
        if action == "set_exit_device":
            if not entry_id:
                return err("entry_id required")
            try:
                enabled = bool(payload.get("enabled", True))
                root[entry_id]["options"]["exit_device"] = enabled
                queue = root.get("sync_queue")
                if queue:
                    try:
                        queue.mark_change(entry_id)
                    except Exception:
                        pass
                return web.json_response({"ok": True})
            except Exception as e:
                return err(e)

        # Groups
        if action == "create_group":
            name = str(payload.get("name") or "").strip()
            if not name:
                return err("group name required")
            store = root.get("groups_store")
            if not store:
                return err("groups store not ready", code=500)
            try:
                await store.add_group(name)
                return web.json_response({"ok": True, "groups": store.groups()})
            except Exception as e:
                return err(e)

        if action == "device_set_groups":
            if not entry_id:
                return err("entry_id required")
            raw_groups = payload.get("groups")
            if isinstance(raw_groups, list):
                groups = [str(g).strip() for g in raw_groups if str(g).strip()]
            elif raw_groups:
                groups = [str(raw_groups).strip()]
            else:
                groups = []
            if not groups:
                groups = ["Default"]

            try:
                store = root.get("groups_store")
                if store:
                    valid = set(store.groups())
                    ordered: List[str] = []
                    for g in groups:
                        if g in valid and g not in ordered:
                            ordered.append(g)
                    groups = ordered or ["Default"]

                entry = hass.config_entries.async_get_entry(entry_id)
                if not entry:
                    return err("device entry not found", code=404)

                new_options = dict(entry.options)
                new_options[CONF_DEVICE_GROUPS] = groups
                hass.config_entries.async_update_entry(entry, options=new_options)

                try:
                    bucket = root.get(entry_id)
                    if isinstance(bucket, dict):
                        bucket.setdefault("options", {})["sync_groups"] = groups
                except Exception:
                    pass

                queue = root.get("sync_queue")
                if queue:
                    try:
                        queue.mark_change(entry_id)
                    except Exception:
                        pass

                return web.json_response({"ok": True, "groups": groups})
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


class AkuvoxUISettings(HomeAssistantView):
    url = "/api/akuvox_ac/ui/settings"
    name = "api:akuvox_ac:ui_settings"
    requires_auth = True

    async def get(self, request: web.Request):
        hass: HomeAssistant = request.app["hass"]
        root = hass.data.get(DOMAIN, {}) or {}

        settings = root.get("settings_store")
        users_store = root.get("users_store")

        interval = None
        delay = None
        alerts = {"targets": {}}
        if settings:
            try:
                interval = settings.get_integrity_interval_minutes()
            except Exception:
                interval = None
            try:
                delay = settings.get_auto_sync_delay_minutes()
            except Exception:
                delay = None
            try:
                alerts = {"targets": settings.get_alert_targets()}
            except Exception:
                data = getattr(settings, "data", {})
                if isinstance(data, dict):
                    raw = data.get("alerts") or {}
                    targets = raw.get("targets") if isinstance(raw, dict) else {}
                    alerts = {"targets": targets if isinstance(targets, dict) else {}}

        registry_users: List[Dict[str, str]] = []
        if users_store:
            try:
                for key, prof in (users_store.all() or {}).items():
                    if not _is_ha_id(key):
                        continue
                    if _profile_is_empty_reserved(prof):
                        continue
                    name = prof.get("name") or key
                    registry_users.append({"id": key, "name": name})
            except Exception:
                pass

        registry_users.sort(key=lambda x: x.get("name", "").lower())

        return web.json_response(
            {
                "ok": True,
                "integrity_interval_minutes": interval,
                "auto_sync_delay_minutes": delay,
                "alerts": alerts,
                "registry_users": registry_users,
                "min_minutes": 5,
                "max_minutes": 24 * 60,
                "min_auto_sync_delay_minutes": 5,
                "max_auto_sync_delay_minutes": 60,
            }
        )

    async def post(self, request: web.Request):
        hass: HomeAssistant = request.app["hass"]
        root = hass.data.get(DOMAIN, {}) or {}

        try:
            payload = await request.json()
        except Exception:
            payload = {}

        settings = root.get("settings_store")
        manager = root.get("sync_manager")
        queue = root.get("sync_queue")

        response: Dict[str, Any] = {"ok": True}

        if "integrity_interval_minutes" in payload:
            minutes = payload.get("integrity_interval_minutes")
            if manager and hasattr(manager, "set_integrity_interval"):
                try:
                    manager.set_integrity_interval(minutes)
                    if hasattr(manager, "get_integrity_interval_minutes"):
                        response["integrity_interval_minutes"] = manager.get_integrity_interval_minutes()
                except Exception as err:
                    return web.json_response({"ok": False, "error": str(err)}, status=400)
            elif settings:
                try:
                    value = int(minutes)
                except Exception:
                    return web.json_response({"ok": False, "error": "invalid interval"}, status=400)
                value = max(5, min(24 * 60, value))
                try:
                    await settings.set_integrity_interval_minutes(value)
                    response["integrity_interval_minutes"] = value
                except Exception as err:
                    return web.json_response({"ok": False, "error": str(err)}, status=400)

        if "auto_sync_delay_minutes" in payload:
            if not settings or not hasattr(settings, "set_auto_sync_delay_minutes"):
                return web.json_response({"ok": False, "error": "settings unavailable"}, status=500)

            minutes = payload.get("auto_sync_delay_minutes")
            try:
                value = int(minutes)
            except Exception:
                return web.json_response({"ok": False, "error": "invalid delay"}, status=400)
            value = max(5, min(60, value))
            try:
                await settings.set_auto_sync_delay_minutes(value)
                response["auto_sync_delay_minutes"] = settings.get_auto_sync_delay_minutes()
            except Exception as err:
                return web.json_response({"ok": False, "error": str(err)}, status=400)

            if queue and hasattr(queue, "refresh_default_delay"):
                try:
                    queue.refresh_default_delay()
                except Exception:
                    pass

        if "alerts" in payload and settings and hasattr(settings, "set_alert_targets"):
            alerts_payload = payload.get("alerts") or {}
            targets = alerts_payload.get("targets") if isinstance(alerts_payload, dict) else {}
            try:
                await settings.set_alert_targets(targets)
                response["alerts"] = {"targets": settings.get_alert_targets()}
            except Exception as err:
                return web.json_response({"ok": False, "error": str(err)}, status=400)

        return web.json_response(response)


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

        try:
            _cleanup_stale_reservations(hass, max_age_minutes=RESERVATION_TTL_MINUTES)
        except Exception as err:
            _LOGGER.debug("Reservation cleanup before reserve failed: %s", err)

        # Find lowest free HA### using local registry only
        try:
            if hasattr(users_store, "next_free_ha_id"):
                candidate = users_store.next_free_ha_id()  # type: ignore[attr-defined]
            else:
                existing = set(k for k in (users_store.all() or {}).keys() if _is_ha_id(k))
                n = 1
                while True:
                    candidate = _ha_id_from_int(n)
                    if candidate not in existing:
                        break
                    n += 1
        except Exception:
            n = 1
            while True:
                candidate = _ha_id_from_int(n)
                try:
                    current = users_store.all()  # type: ignore[attr-defined]
                except Exception:
                    current = {}
                existing_keys = set(k for k in (current or {}).keys() if _is_ha_id(k))
                if candidate not in existing_keys:
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


class AkuvoxUIReservationPing(HomeAssistantView):
    """
    POST JSON: { "id": "HA001" }
    Refreshes reserved_at for empty reservations to keep them alive while the
    add-user page remains open. Returns { ok, active } where active indicates
    whether the reservation is still valid.
    """

    url = "/api/akuvox_ac/ui/reservation_ping"
    name = "api:akuvox_ac:ui_reservation_ping"
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

        store_data = users_store.data.setdefault("users", {})  # type: ignore[attr-defined]
        profile = store_data.get(uid)
        if not profile:
            return web.json_response({"ok": True, "active": False})

        if not _profile_is_empty_reserved(profile):
            return web.json_response({"ok": True, "active": False})

        profile["reserved_at"] = _now_iso()
        try:
            await users_store.async_save()  # type: ignore[attr-defined]
        except Exception:
            pass
        return web.json_response({"ok": True, "active": True})


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

        # Save under persistent FaceData folder inside /config/www/AK_Access_ctrl
        dest_dir = _persistent_face_dir(hass)
        try:
            dest_dir.mkdir(parents=True, exist_ok=True)
        except Exception:
            pass
        dest_path = (dest_dir / f"{id_val}.jpg").resolve()
        try:
            dest_path.relative_to(dest_dir)
        except ValueError:
            return web.json_response({"ok": False, "error": "invalid filename"}, status=400)
        try:
            dest_path.write_bytes(file_bytes)
        except Exception as e:
            return web.json_response({"ok": False, "error": f"write failed: {e}"}, status=500)
        try:
            legacy = (STATIC_ROOT / "FaceData" / f"{id_val}.jpg")
            if legacy.exists():
                legacy.unlink()
        except Exception:
            pass

        # Store public URL so intercom can fetch it
        face_url_public = f"{face_base_url(hass, request)}/{id_val}.jpg"

        # Update registry profile and mark pending
        try:
            users_store = root.get("users_store")
            if users_store:
                await users_store.upsert_profile(id_val, face_url=face_url_public, status="pending")
        except Exception:
            pass

        # Queue a sync so FaceUrl is pushed during the next cycle
        queue = root.get("sync_queue")
        if queue:
            try:
                queue.mark_change(None)
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

        # Ensure the change is picked up on the next sync cycle
        queue = root.get("sync_queue")
        if queue:
            try:
                queue.mark_change(None)
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
    hass.http.register_view(AkuvoxUISettings())
    hass.http.register_view(AkuvoxUIPhones())
    hass.http.register_view(AkuvoxUIReserveId())
    hass.http.register_view(AkuvoxUIReleaseId())   # <-- new
    hass.http.register_view(AkuvoxUIReservationPing())
    hass.http.register_view(AkuvoxUIUploadFace())
    hass.http.register_view(AkuvoxUIRemoteEnrol())
