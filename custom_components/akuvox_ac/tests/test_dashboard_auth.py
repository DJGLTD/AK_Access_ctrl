import asyncio
from pathlib import Path
from types import SimpleNamespace

from custom_components.akuvox_ac.ha_test_stubs import ensure_homeassistant_stubs

ensure_homeassistant_stubs()

from custom_components.akuvox_ac import http as http_module  # noqa: E402
from custom_components.akuvox_ac.const import DOMAIN  # noqa: E402


class _User:
    def __init__(
        self,
        user_id,
        name,
        *,
        is_admin=False,
        is_active=True,
        system_generated=False,
    ):
        self.id = user_id
        self.name = name
        self.is_admin = is_admin
        self.is_active = is_active
        self.system_generated = system_generated


class _Settings:
    def __init__(self, allowed_user_ids=None):
        self._allowed = list(allowed_user_ids or [])

    def get_dashboard_access(self):
        return {"allowed_user_ids": list(self._allowed)}


class _UsersStore:
    def __init__(self, users):
        self.users = users
        self.upserts = []

    def all(self):
        return self.users

    def get(self, user_id):
        return self.users.get(user_id)

    async def upsert_profile(self, user_id, **kwargs):
        self.upserts.append((user_id, kwargs))
        self.users.setdefault(user_id, {}).update(kwargs)


class _Request(dict):
    def __init__(self, *, user=None, headers=None, query=None):
        super().__init__()
        if user is not None:
            self[http_module.KEY_HASS_USER] = user
        self.headers = headers or {}
        self.query = query or {}
        self.rel_url = SimpleNamespace(query=self.query)


class _Auth:
    def __init__(self, users):
        self._users = {user.id: user for user in users}

    def async_get_user(self, user_id):
        return self._users.get(user_id)

    async def async_get_users(self):
        return list(self._users.values())


def _dashboard_request(query=None, user_agent=""):
    return SimpleNamespace(
        rel_url=SimpleNamespace(query=query or {}),
        headers={"User-Agent": user_agent},
    )


def test_mobile_dashboard_uses_the_responsive_web_dashboard():
    mobile_query = _dashboard_request({"variant": "mobile"})
    mobile_agent = _dashboard_request(
        user_agent=(
            "Mozilla/5.0 (iPhone; CPU iPhone OS 18_5 like Mac OS X) "
            "AppleWebKit/605.1.15 Mobile/15E148"
        )
    )

    assert http_module._resolve_dashboard_asset("index", mobile_query).name == "index.html"
    assert http_module._resolve_dashboard_asset("index", mobile_agent).name == "index.html"
    assert http_module._resolve_dashboard_asset("index-mob", mobile_query).name == "index.html"
    assert (
        http_module._resolve_dashboard_asset("user_overview", mobile_query).name
        == "user_overview-mob.html"
    )


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
    assert http_module.AkuvoxUIImpersonation.requires_auth is False
    assert http_module.AkuvoxUIAction.requires_auth is False
    assert http_module.AkuvoxUISettings.requires_auth is False
    assert http_module.AkuvoxUISupportBundle.requires_auth is False
    assert http_module.SIGNED_API_PATHS["impersonation"] == "/api/akuvox_ac/ui/impersonation"
    assert "refresh_events" in http_module.ALLOWED_DASHBOARD_SERVICE_PROXY
    assert "delete_user" in http_module.ALLOWED_DASHBOARD_SERVICE_PROXY


def test_dashboard_access_allows_configured_non_admin_users():
    allowed = _User("ha-allowed", "Allowed")
    denied = _User("ha-denied", "Denied")
    admin = _User("ha-admin", "Admin", is_admin=True)
    settings = _Settings(["ha-allowed"])
    hass = SimpleNamespace(data={DOMAIN: {"settings_store": settings}})

    assert http_module._request_can_access_dashboard(
        hass, _Request(user=admin)
    ) is True
    assert http_module._request_can_access_dashboard(
        hass, _Request(user=allowed)
    ) is True
    assert http_module._request_can_access_dashboard(
        hass, _Request(user=denied)
    ) is False


def test_dashboard_session_access_honors_allowed_user_list():
    settings = _Settings(["ha-allowed"])
    session = {
        "token": "token-1",
        "expires_at": http_module.time.time() + 60,
        "user_id": "ha-allowed",
        "user_name": "Allowed",
        "is_admin": False,
        "dashboard_access": True,
    }
    hass = SimpleNamespace(
        data={DOMAIN: {"settings_store": settings, "dashboard_sessions": {"token-1": session}}}
    )
    request = _Request(headers={http_module.DASHBOARD_SESSION_HEADER: "token-1"})

    assert http_module._request_can_access_dashboard(hass, request) is True

    settings._allowed = []

    assert http_module._request_can_access_dashboard(hass, request) is False


def test_dashboard_impersonation_payload_uses_admin_session_target():
    admin = _User("ha-admin", "Admin", is_admin=True)
    target = _User("ha-target", "Target")
    settings = _Settings(["ha-target"])
    session = {
        "token": "token-1",
        "expires_at": http_module.time.time() + 60,
        "user_id": admin.id,
        "user_name": admin.name,
        "is_admin": True,
        "dashboard_access": True,
        "impersonate_user_id": target.id,
    }
    hass = SimpleNamespace(
        auth=_Auth([admin, target]),
        data={DOMAIN: {"settings_store": settings, "dashboard_sessions": {"token-1": session}}},
    )
    request = _Request(headers={http_module.DASHBOARD_SESSION_HEADER: "token-1"})

    payload = asyncio.run(
        http_module._dashboard_impersonation_payload(hass, request, settings)
    )

    assert payload == {
        "can_manage": True,
        "active": True,
        "user_id": "ha-target",
        "user_name": "Target",
        "is_admin": False,
    }


def test_dashboard_impersonation_allows_standard_user_without_dashboard_access():
    admin = _User("ha-admin", "Admin", is_admin=True)
    target = _User("ha-target", "Target")
    settings = _Settings([])
    session = {
        "token": "token-1",
        "expires_at": http_module.time.time() + 60,
        "user_id": admin.id,
        "user_name": admin.name,
        "is_admin": True,
        "dashboard_access": True,
        "impersonate_user_id": target.id,
    }
    hass = SimpleNamespace(
        auth=_Auth([admin, target]),
        data={DOMAIN: {"settings_store": settings, "dashboard_sessions": {"token-1": session}}},
    )
    request = _Request(headers={http_module.DASHBOARD_SESSION_HEADER: "token-1"})

    payload = asyncio.run(
        http_module._dashboard_impersonation_payload(hass, request, settings)
    )

    assert payload["active"] is True
    assert payload["user_id"] == "ha-target"
    assert payload["is_admin"] is False


def test_dashboard_access_payload_marks_active_standard_users_impersonatable():
    admin = _User("ha-admin", "Admin", is_admin=True)
    target = _User("ha-target", "Target")
    inactive = _User("ha-inactive", "Inactive", is_active=False)
    settings = _Settings([])
    hass = SimpleNamespace(auth=_Auth([admin, target, inactive]))
    request = _Request(user=admin)

    payload = asyncio.run(
        http_module._dashboard_access_payload(hass, settings, request)
    )

    users = {item["id"]: item for item in payload["users"]}
    assert users["ha-target"]["allowed"] is False
    assert users["ha-target"]["can_impersonate"] is True
    assert users["ha-inactive"]["allowed"] is False
    assert users["ha-inactive"]["can_impersonate"] is False


def test_event_viewer_user_id_can_use_impersonated_actor_identity():
    root = {
        "users_store": SimpleNamespace(
            all=lambda: {
                "HA001": {"name": "Admin", "ha_user_id": "ha-admin"},
                "HA002": {"name": "Target", "ha_user_id": "ha-target"},
            }
        )
    }

    assert (
        http_module._event_viewer_user_id(
            SimpleNamespace(),
            _Request(),
            root,
            actor_identity=("ha-target", "Target"),
        )
        == "HA002"
    )


def test_non_admin_dashboard_user_gets_restricted_self_service_context():
    settings = _Settings(["ha-manager"])
    users_store = _UsersStore(
        {
            "HA010": {
                "name": "Manager",
                "ha_user_id": "ha-manager",
                "pin": "1111",
            }
        }
    )
    session = {
        "token": "token-1",
        "expires_at": http_module.time.time() + 60,
        "user_id": "ha-manager",
        "user_name": "Manager",
        "is_admin": False,
        "dashboard_access": True,
    }
    root = {
        "settings_store": settings,
        "dashboard_sessions": {"token-1": session},
        "users_store": users_store,
    }
    hass = SimpleNamespace(data={DOMAIN: root})
    request = _Request(headers={http_module.DASHBOARD_SESSION_HEADER: "token-1"})

    assert http_module._request_can_access_dashboard(hass, request) is True

    context = http_module._effective_self_service_context(
        hass,
        request,
        root,
        has_dashboard_access=True,
        impersonation={"active": False},
    )

    assert context["user_id"] == "HA010"
    assert context["ha_user_id"] == "ha-manager"


def test_admin_impersonating_normal_user_gets_target_self_service_context():
    admin = _User("ha-admin", "Admin", is_admin=True)
    target = _User("ha-target", "Target")
    settings = _Settings(["ha-target"])
    users_store = _UsersStore(
        {
            "HA002": {
                "name": "Target",
                "ha_user_id": "ha-target",
                "pin": "2222",
            }
        }
    )
    session = {
        "token": "token-1",
        "expires_at": http_module.time.time() + 60,
        "user_id": admin.id,
        "user_name": admin.name,
        "is_admin": True,
        "dashboard_access": True,
        "impersonate_user_id": target.id,
    }
    root = {
        "settings_store": settings,
        "dashboard_sessions": {"token-1": session},
        "users_store": users_store,
    }
    hass = SimpleNamespace(auth=_Auth([admin, target]), data={DOMAIN: root})
    request = _Request(headers={http_module.DASHBOARD_SESSION_HEADER: "token-1"})

    impersonation = asyncio.run(
        http_module._dashboard_impersonation_payload(hass, request, settings)
    )
    context = http_module._effective_self_service_context(
        hass,
        request,
        root,
        has_dashboard_access=True,
        impersonation=impersonation,
    )

    assert impersonation["active"] is True
    assert impersonation["is_admin"] is False
    assert context["user_id"] == "HA002"
    assert context["ha_user_id"] == "ha-target"


def test_self_service_profile_edits_are_limited_to_pin():
    payload = {
        "name": "New Name",
        "pin": " 1234 ",
        "phone": "07123456789",
        "license_plate": ["AK01 ABC"],
    }
    users_store = _UsersStore(
        {
            "HA001": {
                "name": "Original",
                "pin": "0000",
                "phone": "07000000000",
                "license_plate": ["OLD123"],
            }
        }
    )

    assert http_module.sanitize_self_service_profile_payload(payload, "HA001") == {
        "id": "HA001",
        "pin": "1234",
    }

    updates, changes = asyncio.run(
        http_module.async_apply_self_service_profile_change(
            SimpleNamespace(),
            {"users_store": users_store},
            user_id="HA001",
            payload=payload,
            actor_name="Manager",
        )
    )

    assert updates == {"id": "HA001", "pin": "1234"}
    assert changes == ["PIN"]
    assert users_store.users["HA001"]["name"] == "Original"
    assert users_store.users["HA001"]["phone"] == "07000000000"
    assert users_store.users["HA001"]["license_plate"] == ["OLD123"]
    assert users_store.upserts == [
        ("HA001", {"pin": "1234", "status": "pending", "source": "Local"})
    ]


def test_dashboard_frontend_contains_impersonation_controls():
    www = Path(http_module.STATIC_ROOT)

    for page_name in ("head.html", "head-mob.html"):
        html = (www / page_name).read_text(encoding="utf-8")
        assert "impersonationBanner" in html
        assert "stopImpersonation" in html
        assert "akuvox-impersonation-changed" in html

    for page_name in ("users.html", "users-mob.html"):
        html = (www / page_name).read_text(encoding="utf-8")
        assert "userAdminSettingsSection" in html
        assert "dashboard_access_enabled" in html
        assert "eventVisibilityTargets" in html
        assert "event-visibility-scroll" in html
        assert "saveUserAdminSettings" in html
        assert "data-impersonate-user" in html
        assert "API_IMPERSONATION" in html

    overview = (www / "user_overview-mob.html").read_text(encoding="utf-8")
    assert "impersonationUrl" in overview
    assert "data-impersonate-user" in overview
    assert "setOverviewImpersonation" in overview
    assert "akuvox-impersonation-changed" in overview

    for page_name in ("settings.html", "settings-mob.html"):
        html = (www / page_name).read_text(encoding="utf-8")
        assert "dashboardAccessCard" not in html
        assert "eventVisibilityCard" not in html
        assert "stopImpersonationFromSettings" not in html


def test_user_frontend_fetches_face_preview_with_dashboard_auth():
    www = Path(http_module.STATIC_ROOT)

    for page_name in ("users.html", "users-mob.html"):
        html = (www / page_name).read_text(encoding="utf-8")
        assert "async function setFacePreviewUrl" in html
        assert "fetchWithAuth(finalUrl" in html
        assert "URL.createObjectURL(blob)" in html
        assert "No local face photo to display" in html


def test_self_service_frontend_limits_profile_identity_fields():
    www = Path(http_module.STATIC_ROOT)

    for page_name in ("users.html", "users-mob.html"):
        html = (www / page_name).read_text(encoding="utf-8")
        assert 'id="nameRow"' in html
        assert "'nameRow'" in html
        assert "'phoneRow'" in html
        assert "'anprSection'" in html
        assert "'userAdminSettingsSection'" in html
        assert "const selfServicePayload = { id: CURRENT.id }" in html
        assert "selfServicePayload.pin = payload.pin" in html
        assert "payload: selfServicePayload" in html


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


def test_support_bundle_sorts_device_requests_by_timestamp():
    filtered = http_module.AkuvoxUISupportBundle._filter_support_requests(
        [
            {
                "diag_type": "upload:face",
                "path": "/api/filetool/import?destFile=Face&index=",
                "method": "POST",
                "timestamp": "2026-06-01T21:14:19Z",
            },
            {
                "diag_type": "user:get",
                "path": "/new_api/user/get",
                "method": "POST",
                "timestamp": "2026-06-01T21:14:32Z",
            },
            {
                "diag_type": "user:get",
                "path": "/new_api/user/get",
                "method": "POST",
                "timestamp": "2026-06-01T21:14:21Z",
            },
        ]
    )

    assert [item["timestamp"] for item in filtered] == [
        "2026-06-01T21:14:32Z",
        "2026-06-01T21:14:21Z",
        "2026-06-01T21:14:19Z",
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
