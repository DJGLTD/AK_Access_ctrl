import asyncio
from pathlib import Path
from types import SimpleNamespace

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
            "file": b"fake-jpeg",
        },
    )

    response = asyncio.run(http_module.AkuvoxUIUploadFace().post(request))

    assert response["args"][0]["ok"] is True
    assert response["args"][0]["face_url"] == "http://ha.local/api/AK_AC/FaceData/HA001.jpg"
    assert (tmp_path / DOMAIN / "FaceData" / "HA001.jpg").read_bytes() == b"fake-jpeg"
    assert users_store.upserts == [
        (
            "HA001",
            {
                "face_url": "http://ha.local/api/AK_AC/FaceData/HA001.jpg",
                "status": "pending",
                "face_status": "pending",
                "face_synced_at": "",
                "remote_enrol_pending": False,
            },
        )
    ]
    assert sync_queue.marked == 1
