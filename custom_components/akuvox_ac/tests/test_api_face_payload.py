from custom_components.akuvox_ac.api import AkuvoxAPI
from custom_components.akuvox_ac import api as api_module
from custom_components.akuvox_ac.http import _build_face_upload_payload


class _SessionStub:
    def get(self, *_args, **_kwargs):
        raise RuntimeError("unused in unit test")

    def post(self, *_args, **_kwargs):
        raise RuntimeError("unused in unit test")

    def head(self, *_args, **_kwargs):
        raise RuntimeError("unused in unit test")


class _ResponseStub:
    def __init__(self, status=200, data=None, body=""):
        self.status = status
        self.reason = "stub"
        self._data = data
        self._body = body

    async def json(self, content_type=None):
        if self._data is None:
            raise ValueError("not json")
        return self._data

    async def text(self):
        return self._body

    def raise_for_status(self):
        if not (200 <= self.status < 400):
            raise RuntimeError(f"{self.status}: {self.reason}")


class _RequestContextStub:
    def __init__(self, response):
        self.response = response

    async def __aenter__(self):
        return self.response

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _FaceUploadSessionStub:
    def __init__(self):
        self.get_urls = []
        self.get_kwargs = []
        self.post_urls = []
        self.post_kwargs = []

    def get(self, url, **kwargs):
        self.get_urls.append(url)
        self.get_kwargs.append(kwargs)
        if url.startswith("http://"):
            return _RequestContextStub(_ResponseStub(200, {"retcode": 0}))
        return _RequestContextStub(_ResponseStub(503, body="tls unavailable"))

    def post(self, url, **kwargs):
        self.post_urls.append(url)
        self.post_kwargs.append(kwargs)
        if url.startswith("http://"):
            return _RequestContextStub(
                _ResponseStub(200, {"retcode": 0, "path": "/mnt/Face/HA001.jpg"})
            )
        return _RequestContextStub(_ResponseStub(503, body="tls unavailable"))

    def head(self, *_args, **_kwargs):
        raise RuntimeError("unused in unit test")


class _FaceUploadNoPathSessionStub(_FaceUploadSessionStub):
    def post(self, url, **kwargs):
        self.post_urls.append(url)
        self.post_kwargs.append(kwargs)
        if url.startswith("http://"):
            return _RequestContextStub(_ResponseStub(200, {"retcode": 0}))
        return _RequestContextStub(_ResponseStub(503, body="tls unavailable"))


class _FaceUploadAllFailSessionStub(_FaceUploadSessionStub):
    def get(self, url, **kwargs):
        self.get_urls.append(url)
        self.get_kwargs.append(kwargs)
        return _RequestContextStub(_ResponseStub(200, {"retcode": 0}))

    def post(self, url, **kwargs):
        self.post_urls.append(url)
        self.post_kwargs.append(kwargs)
        return _RequestContextStub(_ResponseStub(503, body="upload unavailable"))


class _FormDataStub:
    def __init__(self):
        self.fields = []

    def add_field(self, *args, **kwargs):
        self.fields.append((args, kwargs))


class _DeleteApiStub(AkuvoxAPI):
    def __init__(self, users):
        super().__init__("127.0.0.1", port=80, username="", password="", session=_SessionStub())
        self._users = users
        self.api_user_calls = []
        self.face_delete_calls = []

    async def user_list(self):
        return list(self._users)

    async def _api_user(self, action, items=None):
        self.api_user_calls.append((action, items))
        return {"retcode": 0}

    async def face_delete_bulk(self, user_ids):
        self.face_delete_calls.append(list(user_ids))


def test_build_face_upload_payload_links_imported_face_by_filename_only():
    payload = _build_face_upload_payload(
        {"name": "Test User"},
        {"ID": "12", "UserID": "HA001", "Name": "Old Name"},
        "HA001",
        "/mnt/Face/HA001.jpg",
    )

    assert payload["FaceFileName"] == "HA001.jpg"
    assert "FaceUrl" not in payload
    assert "importFile" not in payload
    assert payload["FaceRegister"] == 1


def test_normalize_user_add_keeps_face_filename_for_modern_firmware():
    api = AkuvoxAPI("127.0.0.1", port=80, username="", password="", session=_SessionStub())

    normalized = api._normalize_user_items_for_add_or_set(
        [{"UserID": "HA001", "Name": "Test", "FaceFileName": "HA001.jpg"}],
        allow_face_url=True,
        for_set=False,
    )

    assert normalized[0]["FaceFileName"] == "HA001.jpg"
    assert "FaceUrl" not in normalized[0]
    assert "importFile" not in normalized[0]
    assert normalized[0]["FaceRegister"] == 1


def test_normalize_user_set_uses_web_face_import_fields_for_device_file():
    api = AkuvoxAPI("127.0.0.1", port=80, username="", password="", session=_SessionStub())

    normalized = api._normalize_user_items_for_add_or_set(
        [
            {
                "UserID": "HA001",
                "ID": "7",
                "Name": "Test",
                "FaceUrl": "/mnt/Face/HA001.jpg",
                "FaceRegisterStatus": "1",
            }
        ],
        allow_face_url=True,
        for_set=True,
    )

    assert normalized[0]["FaceFileName"] == "HA001.jpg"
    assert "importFile" not in normalized[0]
    assert "FaceUrl" not in normalized[0]
    assert normalized[0]["FaceRegisterStatus"] == "1"


def test_normalize_user_add_links_uploaded_face_with_device_path():
    api = AkuvoxAPI("127.0.0.1", port=80, username="", password="", session=_SessionStub())

    normalized = api._normalize_user_items_for_add_or_set(
        [
            {
                "UserID": "CODEXFACE2",
                "Name": "Test",
                "FaceUrl": "/mnt/Face/CODEXFACE2.jpg",
                "FaceFileName": "CODEXFACE2.jpg",
                "importFile": {"fileName": "CODEXFACE2.jpg", "fileData": {}},
            }
        ],
        allow_face_url=True,
        for_set=False,
    )

    assert normalized[0]["FaceFileName"] == "CODEXFACE2.jpg"
    assert "FaceUrl" not in normalized[0]
    assert "importFile" not in normalized[0]
    assert normalized[0]["FaceRegister"] == 1


def test_normalize_user_add_drops_ha_face_url_when_uploaded_filename_is_present():
    api = AkuvoxAPI("127.0.0.1", port=80, username="", password="", session=_SessionStub())

    normalized = api._normalize_user_items_for_add_or_set(
        [
            {
                "UserID": "HA001",
                "Name": "Test",
                "FaceFileName": "HA001.jpg",
                "FaceUrl": "http://ha.local/api/AK_AC/FaceData/HA001.jpg",
            }
        ],
        allow_face_url=True,
        for_set=False,
    )

    assert normalized[0]["FaceFileName"] == "HA001.jpg"
    assert "FaceUrl" not in normalized[0]
    assert normalized[0]["FaceRegister"] == 1


def test_normalize_user_set_preserves_face_url_field():
    api = AkuvoxAPI("127.0.0.1", port=80, username="", password="", session=_SessionStub())

    normalized = api._normalize_user_items_for_add_or_set(
        [{"UserID": "HA001", "ID": "7", "Name": "Test", "FaceUrl": "", "FaceRegisterStatus": "1"}],
        allow_face_url=True,
        for_set=True,
    )

    assert normalized[0]["FaceUrl"] == ""
    assert normalized[0]["FaceRegisterStatus"] == "1"


def test_face_upload_tries_http_device_endpoint_when_https_unavailable():
    import asyncio

    session = _FaceUploadSessionStub()
    api = AkuvoxAPI("127.0.0.1", port=80, username="", password="", session=session)
    original_form_data = api_module.FormData
    api_module.FormData = _FormDataStub

    try:
        result = asyncio.run(api.face_upload(b"jpg", filename="HA001.jpg"))
    finally:
        api_module.FormData = original_form_data

    assert result["path"] == "/mnt/Face/HA001.jpg"
    assert api._detected == (False, 80, True)
    assert any(url.startswith("https://") for url in session.get_urls)
    assert any(url.startswith("http://") for url in session.get_urls)
    assert session.post_urls[0].startswith("http://")
    assert session.post_kwargs[0]["ssl"] is False
    assert "/api/web/filetool/import" in session.post_urls[0]


def test_face_upload_keeps_ssl_verification_disabled_for_self_signed_devices():
    import asyncio

    session = _FaceUploadSessionStub()
    api = AkuvoxAPI("127.0.0.1", port=80, username="", password="", session=session)
    original_form_data = api_module.FormData
    api_module.FormData = _FormDataStub

    try:
        asyncio.run(api.face_upload(b"jpg", filename="HA001.jpg"))
    finally:
        api_module.FormData = original_form_data

    assert api.verify_ssl is False
    assert session.post_kwargs
    assert all(kwargs["ssl"] is False for kwargs in session.post_kwargs)
    face_requests = [
        req for req in api.recent_requests(10) if req["diag_type"] == "upload:face"
    ]
    assert all(req["verify_ssl"] is False for req in face_requests)


def test_face_upload_uses_web_session_cookie_for_web_import_endpoint():
    import asyncio

    session = _FaceUploadSessionStub()
    api = AkuvoxAPI("127.0.0.1", port=80, username="admin", password="secret", session=session)
    original_form_data = api_module.FormData
    api_module.FormData = _FormDataStub

    async def _fake_web_login_cookie(**_kwargs):
        return "token=fake"

    api._web_login_cookie = _fake_web_login_cookie

    try:
        asyncio.run(api.face_upload(b"jpg", filename="HA001.jpg"))
    finally:
        api_module.FormData = original_form_data

    assert session.post_kwargs
    assert session.post_kwargs[0]["headers"]["Cookie"] == "token=fake"
    assert session.post_kwargs[0]["auth"] is None


def test_face_upload_does_not_retry_verified_ssl_when_disabled():
    import asyncio

    session = _FaceUploadAllFailSessionStub()
    api = AkuvoxAPI("127.0.0.1", port=80, username="", password="", session=session)
    original_form_data = api_module.FormData
    api_module.FormData = _FormDataStub

    try:
        try:
            asyncio.run(api.face_upload(b"jpg", filename="HA001.jpg"))
        except RuntimeError:
            pass
        else:
            raise AssertionError("face_upload unexpectedly succeeded")
    finally:
        api_module.FormData = original_form_data

    assert api._detected == (True, 443, False)
    assert session.post_kwargs
    assert all(kwargs["ssl"] is False for kwargs in session.post_kwargs)
    face_requests = [
        req for req in api.recent_requests(50) if req["diag_type"] == "upload:face"
    ]
    assert all(req["verify_ssl"] is False for req in face_requests)


def test_face_upload_infers_device_face_path_when_upload_response_has_no_path():
    import asyncio

    session = _FaceUploadNoPathSessionStub()
    api = AkuvoxAPI("127.0.0.1", port=80, username="", password="", session=session)
    original_form_data = api_module.FormData
    api_module.FormData = _FormDataStub

    try:
        result = asyncio.run(api.face_upload(b"jpg", filename="HA001.jpg"))
    finally:
        api_module.FormData = original_form_data

    assert result["path"] == "/mnt/Face/HA001.jpg"
    assert result["path_inferred"] is True


def test_user_delete_skips_face_delete_for_inactive_face_record():
    import asyncio

    api = _DeleteApiStub(
        [{"ID": "42", "UserID": "HA001", "Name": "Test", "FaceRegister": "0"}]
    )

    asyncio.run(api.user_delete("HA001"))

    assert api.face_delete_calls == []
    assert api.api_user_calls == [("delete", [{"ID": "42"}])]


def test_user_delete_uses_face_delete_for_active_face_record():
    import asyncio

    api = _DeleteApiStub(
        [{"ID": "42", "UserID": "HA001", "Name": "Test", "FaceRegister": "1"}]
    )

    asyncio.run(api.user_delete("HA001"))

    assert api.face_delete_calls == [["42"]]
    assert api.api_user_calls == [("delete", [{"ID": "42"}])]
