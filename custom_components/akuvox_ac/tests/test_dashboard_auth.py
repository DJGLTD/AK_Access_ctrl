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
    assert "_akts" in rendered
    assert "akAcSignedPathFresh" in rendered
    assert "authentication signature expired" in rendered
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


def test_support_bundle_text_contains_copyable_sections():
    text = http_module.AkuvoxUISupportBundle._support_bundle_text(
        {
            "metadata": {
                "generated_at": "2026-05-27T10:00:00+00:00",
                "integration_version_label": "3.5.7",
            },
            "users": {"counts": {"total": 1, "face_active": 0, "face_pending": 0, "face_error": 1}},
            "devices": [{"name": "Gate"}],
            "homeassistant_log_tail": {"lines": ["face profile upload failed"]},
        }
    )

    assert "Akuvox Access Control Support Bundle" in text
    assert "=== Redacted device request diagnostics JSON ===" in text
    assert "=== Filtered Home Assistant log tail ===" in text
    assert "face profile upload failed" in text


def test_support_bundle_filters_access_history_from_device_requests():
    filtered = http_module.AkuvoxUISupportBundle._filter_support_requests(
        [
            {
                "diag_type": "upload:face",
                "path": "/api/filetool/import?destFile=Face&index=",
                "method": "POST",
                "payload": {"filename": "HA001.jpg"},
            },
            {
                "diag_type": "event:history",
                "path": "/api/access/history",
                "method": "GET",
                "response_excerpt": {"events": [1, 2, 3]},
            },
            {
                "diag_type": "user/get",
                "path": "/api/user/get?NameOrPerID=HA001",
                "method": "GET",
            },
        ]
    )

    assert [item["path"] for item in filtered] == [
        "/api/filetool/import?destFile=Face&index=",
        "/api/user/get?NameOrPerID=HA001",
    ]


def test_support_bundle_counts_device_face_registration_mismatches():
    class _UsersStore:
        def all(self):
            return {
                "HA001": {
                    "name": "Lee Fletcher",
                    "groups": ["Default"],
                    "face_status": "active",
                    "face_url": "/api/AK_AC/FaceData/HA001.jpg",
                }
            }

    snapshot = http_module.AkuvoxUISupportBundle._users_snapshot(
        {"users_store": _UsersStore()},
        devices=[
            {
                "name": "Gate",
                "type": "Intercom",
                "participate_in_sync": True,
                "sync_groups": ["Default"],
                "users": [
                    {
                        "UserID": "HA001",
                        "Name": "Lee Fletcher",
                        "FaceUrl": "/mnt/Face/HA001.jpg",
                        "FaceRegister": "0",
                    }
                ],
            }
        ],
    )

    assert snapshot["counts"]["face_active"] == 0
    assert snapshot["counts"]["face_error"] == 1
    assert snapshot["counts"]["face_sync_errors"] == 1
    assert snapshot["profiles"][0]["face"]["status"] == "error"
    assert snapshot["profiles"][0]["face"]["stored_status"] == "active"
    assert snapshot["profiles"][0]["face"]["register_mismatch"] is True
    assert snapshot["profiles"][0]["face"]["matched_devices"] == ["Gate"]


def test_dashboard_frontend_does_not_send_bearer_authorization_headers():
    www = Path(http_module.STATIC_ROOT)

    for asset in www.glob("*"):
        if asset.suffix.lower() not in {".html", ".js"}:
            continue
        text = asset.read_text(encoding="utf-8")
        assert "Authorization" not in text, asset.name
        assert "Bearer " not in text, asset.name
