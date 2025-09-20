import datetime as dt
import importlib
import sys
from dataclasses import dataclass
from types import SimpleNamespace

from custom_components.AK_Access_ctrl.const import (
    ADMIN_DASHBOARD_ICON,
    ADMIN_DASHBOARD_TITLE,
    ADMIN_DASHBOARD_URL_PATH,
    DOMAIN,
)

akuvox_init = importlib.import_module("custom_components.AK_Access_ctrl.__init__")
akuvox_http = importlib.import_module("custom_components.AK_Access_ctrl.http")


def test_register_admin_dashboard_creates_panel(hass):
    frontend = sys.modules["homeassistant.components.frontend"]
    frontend.registered.clear()
    frontend.removed.clear()
    hass.data.clear()

    assert akuvox_init._register_admin_dashboard(hass) is True

    assert frontend.registered
    panel = frontend.registered[-1]
    assert panel["component_name"] == "iframe"
    assert panel["sidebar_title"] == ADMIN_DASHBOARD_TITLE
    assert panel["sidebar_icon"] == ADMIN_DASHBOARD_ICON
    assert panel["frontend_url_path"] == ADMIN_DASHBOARD_URL_PATH
    assert panel["config"] == {"url": "/akuvox-ac/"}
    assert panel["require_admin"] is True

    stored = hass.data["frontend_panels"][ADMIN_DASHBOARD_URL_PATH]
    assert stored["require_admin"] is True

    akuvox_init._remove_admin_dashboard(hass)
    assert ADMIN_DASHBOARD_URL_PATH in frontend.removed
    assert ADMIN_DASHBOARD_URL_PATH not in hass.data.get("frontend_panels", {})


def test_http_views_require_auth_flags():
    """Static assets must be world-readable but API endpoints stay auth protected."""

    html_views = [
        akuvox_http.AkuvoxStaticAssets,
        akuvox_http.AkuvoxDashboardView,
    ]

    for cls in html_views:
        view = cls()
        assert (
            view.requires_auth is False
        ), f"{cls.__name__} should allow unauthenticated access to serve dashboard files"

    api_views = [
        akuvox_http.AkuvoxUIView,
        akuvox_http.AkuvoxUIAction,
        akuvox_http.AkuvoxUIDevices,
        akuvox_http.AkuvoxUIPhones,
        akuvox_http.AkuvoxUIReserveId,
        akuvox_http.AkuvoxUIReleaseId,
        akuvox_http.AkuvoxUIUploadFace,
        akuvox_http.AkuvoxUIRemoteEnrol,
    ]

    for cls in api_views:
        view = cls()
        assert (
            view.requires_auth is True
        ), f"{cls.__name__} should remain protected because it mutates Akuvox data"


@dataclass
class _DummyEvent:
    Event: str
    timestamp: dt.datetime


@dataclass
class _DummyUser:
    ID: str
    Name: str
    Groups: list
    LastAccess: dt.datetime


class _DummyCoordinator:
    def __init__(self):
        self.health = {
            "device_type": "door",
            "ip": "192.168.1.5",
            "online": True,
            "sync_status": "pending",
            "last_sync": dt.datetime(2024, 1, 2, 3, 4, 5),
        }
        self.events = [_DummyEvent("Door Opened", dt.datetime(2024, 1, 2, 4, 0, 0))]
        self.users = [
            _DummyUser(
                ID="HA001",
                Name="Lobby User",
                Groups=["Default"],
                LastAccess=dt.datetime(2024, 1, 1, 12, 0, 0),
            )
        ]


class _DummyUsersStore:
    def __init__(self):
        self._users = {
            "HA001": {
                "name": "Lobby User",
                "groups": ["Default"],
                "status": "active",
                "reserved_at": dt.datetime(2024, 1, 1, 0, 0),
            }
        }
        self.data = {"users": dict(self._users)}

    def all(self):
        return self._users

    async def async_save(self):
        return None


class _DummySettings:
    def get_auto_sync_time(self):
        return dt.time(9, 30)

    def get_auto_reboot(self):
        return {"time": dt.time(2, 15), "days": ["mon", "wed"]}


class _DummySyncQueue:
    def __init__(self):
        self.next_sync_eta = dt.datetime(2024, 1, 2, 5, 0, 0)


class _DummySchedules:
    def all(self):
        return {"Default": {"mon": ["00:00", "23:59"]}}


class _DummySyncManager:
    def get_next_sync_text(self):
        return "2024-01-02T06:00:00"


def test_ui_state_serializes_complex_objects(hass):
    hass.data[DOMAIN] = {
        "entry-1": {
            "coordinator": _DummyCoordinator(),
            "options": {"exit_device": True},
        },
        "users_store": _DummyUsersStore(),
        "schedules_store": _DummySchedules(),
        "settings_store": _DummySettings(),
        "sync_queue": _DummySyncQueue(),
        "sync_manager": _DummySyncManager(),
    }

    request = SimpleNamespace(app={"hass": hass})
    view = akuvox_http.AkuvoxUIView()

    response = hass.loop.run_until_complete(view.get(request))
    payload = response["data"]

    assert response["status"] == 200
    assert isinstance(payload["kpis"]["auto_sync_time"], str)
    assert isinstance(payload["kpis"]["auto_reboot"]["time"], str)
    assert payload["kpis"]["next_sync_eta"].startswith("2024-01-02")

    assert payload["devices"][0]["last_sync"].startswith("2024-01-02")
    assert payload["devices"][0]["events"][0]["timestamp"].startswith("2024-01-02")
    assert payload["devices"][0]["_users"][0]["LastAccess"].startswith("2024-01-01")
