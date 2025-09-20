import importlib
import sys

from custom_components.AK_Access_ctrl.const import (
    ADMIN_DASHBOARD_ICON,
    ADMIN_DASHBOARD_TITLE,
    ADMIN_DASHBOARD_URL_PATH,
)

akuvox_init = importlib.import_module("custom_components.AK_Access_ctrl.__init__")


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
