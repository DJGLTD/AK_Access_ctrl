import asyncio
from pathlib import Path
from types import SimpleNamespace

import pytest

from custom_components.akuvox_ac.ha_test_stubs import ensure_homeassistant_stubs

ensure_homeassistant_stubs()

from custom_components.akuvox_ac import http as http_module
from custom_components.akuvox_ac.const import DOMAIN


class _Config:
    def __init__(self, root: Path):
        self.root = root

    def path(self, *parts):
        return str(self.root.joinpath(*parts))


class _UsersStore:
    def __init__(self, profiles):
        self.profiles = profiles

    def get(self, user_id, default=None):
        return self.profiles.get(user_id, default)


class _Request:
    def __init__(self, hass, token=None):
        self.app = {"hass": hass}
        self.query = {"token": token} if token is not None else {}
        self.remote = "203.0.113.10"
        self._values = {}

    def get(self, key, default=None):
        return self._values.get(key, default)


def _hass(root, token="face-capability"):
    return SimpleNamespace(
        config=_Config(root),
        data={DOMAIN: {"users_store": _UsersStore({"HA001": {"face_access_token": token}})}},
    )


def test_face_asset_rejects_anonymous_request_without_capability_token(tmp_path):
    face_dir = tmp_path / DOMAIN / "FaceData"
    face_dir.mkdir(parents=True)
    (face_dir / "HA001.jpg").write_bytes(b"face")
    request = _Request(_hass(tmp_path))

    with pytest.raises(http_module.web.HTTPUnauthorized):
        asyncio.run(http_module.AkuvoxStaticAssets().get(request, "FaceData/HA001.jpg"))


def test_face_asset_accepts_matching_profile_capability_token(tmp_path):
    face_dir = tmp_path / DOMAIN / "FaceData"
    face_dir.mkdir(parents=True)
    (face_dir / "HA001.jpg").write_bytes(b"face")
    request = _Request(_hass(tmp_path), "face-capability")

    response = asyncio.run(http_module.AkuvoxStaticAssets().get(request, "FaceData/HA001.jpg"))

    assert response["file"][0] == face_dir / "HA001.jpg"


def test_face_asset_rejects_token_for_different_profile(tmp_path):
    face_dir = tmp_path / DOMAIN / "FaceData"
    face_dir.mkdir(parents=True)
    (face_dir / "HA001.jpg").write_bytes(b"face")
    request = _Request(_hass(tmp_path), "incorrect-token")

    with pytest.raises(http_module.web.HTTPUnauthorized):
        asyncio.run(http_module.AkuvoxStaticAssets().get(request, "FaceData/HA001.jpg"))
