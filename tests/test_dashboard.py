import importlib
import sys

from custom_components.AK_Access_ctrl.const import (
    ADMIN_DASHBOARD_ICON,
    ADMIN_DASHBOARD_TITLE,
    ADMIN_DASHBOARD_URL_PATH,
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
