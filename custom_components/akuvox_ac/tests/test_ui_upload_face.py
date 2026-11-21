import asyncio
from io import BytesIO
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import parse_qs, urlsplit

from PIL import Image

from custom_components.akuvox_ac.ha_test_stubs import ensure_homeassistant_stubs

ensure_homeassistant_stubs()

from custom_components.akuvox_ac import http as http_module  # noqa: E402
from custom_components.akuvox_ac.const import DOMAIN  # noqa: E402


class _ConfigStub:
    internal_url = "http://ha.local"
    external_url = None

    def __init__(self, root):
        self.root = Path(root)

    def path(self, *parts):
        return str(self.root.joinpath(*parts))


class _UsersStoreStub:
    def __init__(self):
        self.upserts = []

    async def upsert_profile(self, user_id, **kwargs):
        self.upserts.append((user_id, kwargs))


class _SyncQueueStub:
    def __init__(self):
        self.marked = 0

    def mark_change(self, *_args, **_kwargs):
        self.marked += 1


class _UploadRequestStub:
    content_type = "multipart/form-data"

    def __init__(self, hass, form):
        self.app = {"hass": hass}
        self.form = form
        self.headers = {}
        self.query = {}
        self._values = {
            http_module.KEY_HASS_USER: SimpleNamespace(
                id="admin-user",
                name="Admin",
                is_admin=True,
            )
        }

    def get(self, key, default=None):
        return self._values.get(key, default)

    async def post(self):
        return dict(self.form)


def _valid_png():
    output = BytesIO()
    Image.new("RGB", (2, 2), (20, 40, 60)).save(output, format="PNG")
    return output.getvalue()


def _file_field(data, *, filename, content_type):
    field = http_module.web.FileField()
    field.file = BytesIO(data)
    field.filename = filename
    field.content_type = content_type
    return field


def test_ui_upload_face_dashboard_request_saves_file_and_marks_profile(tmp_path):
    users_store = _UsersStoreStub()
    sync_queue = _SyncQueueStub()
    hass = SimpleNamespace(
        config=_ConfigStub(tmp_path),
        data={DOMAIN: {"users_store": users_store, "sync_queue": sync_queue}},
    )
    request = _UploadRequestStub(
        hass,
        {
            "id": "HA001",
            "file": _file_field(_valid_png(), filename="photo.txt", content_type="text/plain"),
        },
    )

    response = asyncio.run(http_module.AkuvoxUIUploadFace().post(request))

    assert response["args"][0]["ok"] is True
    face_url = response["args"][0]["face_url"]
    assert urlsplit(face_url).path == "/api/AK_AC/FaceData/HA001.jpg"
    access_token = parse_qs(urlsplit(face_url).query).get("token", [""])[0]
    assert access_token
    stored_face = (tmp_path / DOMAIN / "FaceData" / "HA001.jpg").read_bytes()
    assert stored_face.startswith(b"\xff\xd8")
    with Image.open(BytesIO(stored_face)) as image:
        assert image.format == "JPEG"
    assert users_store.upserts == [
        (
            "HA001",
            {
                "face_url": face_url,
                "face_access_token": access_token,
                "status": "pending",
                "face_status": "pending",
                "face_synced_at": "",
                "remote_enrol_pending": False,
            },
        )
    ]
    assert sync_queue.marked == 1


def test_ui_upload_face_rejects_malformed_image_bytes(tmp_path):
    hass = SimpleNamespace(
        config=_ConfigStub(tmp_path),
        data={DOMAIN: {"users_store": _UsersStoreStub(), "sync_queue": _SyncQueueStub()}},
    )
    request = _UploadRequestStub(
        hass,
        {
            "id": "HA001",
            "file": _file_field(
                b"not an image", filename="photo.jpg", content_type="image/jpeg"
            ),
        },
    )

    response = asyncio.run(http_module.AkuvoxUIUploadFace().post(request))

    assert response["args"][0]["ok"] is False
    assert not (tmp_path / DOMAIN / "FaceData" / "HA001.jpg").exists()


def test_ui_upload_face_rejects_payload_over_size_limit(tmp_path):
    hass = SimpleNamespace(
        config=_ConfigStub(tmp_path),
        data={DOMAIN: {"users_store": _UsersStoreStub(), "sync_queue": _SyncQueueStub()}},
    )
    oversized = b"x" * (5 * 1024 * 1024 + 1)
    request = _UploadRequestStub(hass, {"id": "HA001", "file": oversized})

    response = asyncio.run(http_module.AkuvoxUIUploadFace().post(request))

    assert response["args"][0]["ok"] is False
    assert not (tmp_path / DOMAIN / "FaceData" / "HA001.jpg").exists()
