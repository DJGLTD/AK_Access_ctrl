import importlib
import json
import sys
from datetime import datetime
from types import SimpleNamespace
from aiohttp import web
import sys

from custom_components.AK_Access_ctrl.const import (
    ADMIN_DASHBOARD_ICON,
    ADMIN_DASHBOARD_TITLE,
    ADMIN_DASHBOARD_URL_PATH,
    DOMAIN,
)

from custom_components.AK_Access_ctrl.http import AkuvoxUIView

akuvox_init = importlib.import_module("custom_components.AK_Access_ctrl.__init__")
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

class _DummyUsersStore:
    def __init__(self):
        self.data = {
            "users": {
                "HA001": {
                    "name": "Stored User",
                    "groups": {"Default", "VIP"},
                    "pin": "1234",
                    "status": "active",
                }
            }
        }

    def all(self):
        return dict(self.data.get("users") or {})

    async def async_save(self):
        return None


def test_ui_state_sanitizes_sets_and_datetimes(hass):
    hass.data.setdefault(DOMAIN, {})

    coord = SimpleNamespace(
        device_name="Panel A",
        friendly_name="Panel A",
        health={
            "name": "Panel A",
            "device_type": "Door",
            "ip": "1.2.3.4",
            "online": True,
            "sync_status": "pending",
            "last_sync": datetime(2024, 5, 1, 12, 30),
        },
        events=[{"timestamp": datetime(2024, 5, 1, 12, 0), "Event": "Sync"}],
        users=[
            {
                "ID": "HA001",
                "Name": "Door User",
                "Groups": {"Default", "VIP"},
                "LastAccess": datetime(2024, 5, 1, 11, 0),
            }
        ],
    )

    hass.data[DOMAIN].update(
        {
            "entry-1": {
                "coordinator": coord,
                "options": {"exit_device": False},
            },
            "users_store": _DummyUsersStore(),
            "schedules_store": SimpleNamespace(all=lambda: {"24/7 Access": {"mon": [["00:00", "24:00"]]}}),
            "sync_manager": SimpleNamespace(get_next_sync_text=lambda: datetime(2024, 5, 1, 15, 0)),
            "sync_queue": SimpleNamespace(next_sync_eta=datetime(2024, 5, 1, 16, 0)),
            "settings_store": SimpleNamespace(
                get_auto_sync_time=lambda: None,
                get_auto_reboot=lambda: {"time": datetime(2024, 5, 2, 1, 0), "days": {"mon", "tue"}},
            ),
        }
    )

    request = web.Request()
    request.app["hass"] = hass

    view = AkuvoxUIView()
    response = hass.loop.run_until_complete(view.get(request))
    payload = response["data"]

    assert payload["devices"][0]["last_sync"].startswith("2024-05-01T12:30")
    event = payload["devices"][0]["events"][0]
    assert isinstance(event["timestamp"], str)
    user = payload["devices"][0]["users"][0]
    assert sorted(user["Groups"]) == ["Default", "VIP"]

    registry = payload["registry_users"][0]
    assert sorted(registry["groups"]) == ["Default", "VIP"]

    auto_reboot = payload["kpis"].get("auto_reboot")
    assert isinstance(auto_reboot["days"], list)

    json.dumps(payload)  # should not raise for any nested structures
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