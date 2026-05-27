from pathlib import Path

from custom_components.akuvox_ac.ha_test_stubs import ensure_homeassistant_stubs

ensure_homeassistant_stubs()

from custom_components.akuvox_ac import http as http_module  # noqa: E402


def test_dashboard_injects_signing_helper_without_initial_signed_paths():
    html = "<html><head></head><body>Dashboard</body></html>"

    rendered = http_module._inject_signed_paths(html, {})

    assert "window.AK_AC_API_PATHS" in rendered
    assert "window.AK_AC_SIGN_URL" in rendered
    assert "window.AK_AC_DASHBOARD_FETCH" in rendered
    assert "akAcFetchWithDashboardSession" in rendered
    assert "auth/sign_path" in rendered
    assert "/api/akuvox_ac/ui/session" in rendered
    assert "X-Akuvox-Dashboard-Token" in rendered
    assert "akuvox_ll_token" in rendered
    assert "/api/akuvox_ac/ui/state" in rendered
    assert "/api/akuvox_ac/ui/support_bundle" in rendered
    assert "sessionStorage.setItem('akuvox_signed_paths'" not in rendered
    assert "localStorage.setItem('akuvox_signed_paths'" not in rendered


def test_dashboard_post_views_use_dashboard_session_token_instead_of_signed_post():
    assert http_module.AkuvoxUISession.requires_auth is True
    assert http_module.AkuvoxUIView.requires_auth is False
    assert http_module.AkuvoxUIAction.requires_auth is False
    assert http_module.AkuvoxUISettings.requires_auth is False
    assert http_module.AkuvoxUISupportBundle.requires_auth is False
    assert "refresh_events" in http_module.ALLOWED_DASHBOARD_SERVICE_PROXY
    assert "delete_user" in http_module.ALLOWED_DASHBOARD_SERVICE_PROXY


def test_support_bundle_is_signed_and_redacts_sensitive_values():
    assert http_module.SIGNED_API_PATHS["support_bundle"] == "/api/akuvox_ac/ui/support_bundle"

    redacted = http_module.AkuvoxUISupportBundle._redact_support_data(
        {
            "pin": "1234",
            "phone": "07123456789",
            "authSig": "secret",
            "has_pin": True,
            "has_phone": True,
            "face_url": "/api/AK_AC/FaceData/HA001.jpg",
            "nested": {"refresh_token": "secret-token"},
        }
    )

    assert redacted["pin"] == "<redacted>"
    assert redacted["phone"] == "<redacted>"
    assert redacted["authSig"] == "<redacted>"
    assert redacted["nested"]["refresh_token"] == "<redacted>"
    assert redacted["has_pin"] is True
    assert redacted["has_phone"] is True
    assert redacted["face_url"] == "/api/AK_AC/FaceData/HA001.jpg"


def test_dashboard_frontend_does_not_send_bearer_authorization_headers():
    www = Path(http_module.STATIC_ROOT)

    for asset in www.glob("*"):
        if asset.suffix.lower() not in {".html", ".js"}:
            continue
        text = asset.read_text(encoding="utf-8")
        assert "Authorization" not in text, asset.name
        assert "Bearer " not in text, asset.name
