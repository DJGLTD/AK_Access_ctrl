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
        self.post_urls = []

    def get(self, url, **_kwargs):
        self.get_urls.append(url)
        if url.startswith("http://"):
            return _RequestContextStub(_ResponseStub(200, {"retcode": 0}))
        return _RequestContextStub(_ResponseStub(503, body="tls unavailable"))

    def post(self, url, **_kwargs):
        self.post_urls.append(url)
        if url.startswith("http://"):
            return _RequestContextStub(
                _ResponseStub(200, {"retcode": 0, "path": "/mnt/Face/HA001.jpg"})
            )
        return _RequestContextStub(_ResponseStub(503, body="tls unavailable"))

    def head(self, *_args, **_kwargs):
        raise RuntimeError("unused in unit test")


class _FaceUploadNoPathSessionStub(_FaceUploadSessionStub):
    def post(self, url, **_kwargs):
        self.post_urls.append(url)
        if url.startswith("http://"):
            return _RequestContextStub(_ResponseStub(200, {"retcode": 0}))
        return _RequestContextStub(_ResponseStub(503, body="tls unavailable"))


class _FormDataStub:
    def __init__(self):
        self.fields = []

    def add_field(self, *args, **kwargs):
        self.fields.append((args, kwargs))


def test_build_face_upload_payload_includes_legacy_import_fields():
    payload = _build_face_upload_payload(
        {"name": "Test User"},
        {"ID": "12", "UserID": "HA001", "Name": "Old Name"},
        "HA001",
        "/mnt/Face/HA001.jpg",
    )

    assert payload["FaceUrl"] == "/mnt/Face/HA001.jpg"
    assert payload["FaceFileName"] == "HA001.jpg"
    assert payload["importFile"] == {"fileName": "HA001.jpg", "fileData": {}}


def test_normalize_user_add_keeps_face_filename_for_modern_firmware():
    api = AkuvoxAPI("127.0.0.1", port=80, username="", password="", session=_SessionStub())

    normalized = api._normalize_user_items_for_add_or_set(
        [{"UserID": "HA001", "Name": "Test", "FaceFileName": "HA001.jpg"}],
        allow_face_url=True,
        for_set=False,
    )

    assert normalized[0]["FaceFileName"] == "HA001.jpg"
    assert normalized[0]["importFile"] == {"fileName": "HA001.jpg", "fileData": {}}
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
    assert normalized[0]["importFile"] == {"fileName": "HA001.jpg", "fileData": {}}
    assert "FaceUrl" not in normalized[0]
    assert normalized[0]["FaceRegisterStatus"] == "1"


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
    assert "/api/web/filetool/import" in session.post_urls[0]


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
