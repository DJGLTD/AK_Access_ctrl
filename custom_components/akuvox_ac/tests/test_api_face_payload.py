from custom_components.akuvox_ac.api import AkuvoxAPI
from custom_components.akuvox_ac.http import _build_face_upload_payload


class _SessionStub:
    def get(self, *_args, **_kwargs):
        raise RuntimeError("unused in unit test")

    def post(self, *_args, **_kwargs):
        raise RuntimeError("unused in unit test")

    def head(self, *_args, **_kwargs):
        raise RuntimeError("unused in unit test")


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
