import asyncio
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import parse_qs, urlsplit

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

    def all(self):
        return dict(self.profiles)

    async def upsert_profile(self, user_id, **values):
        self.profiles.setdefault(user_id, {}).update(values)


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


def test_local_face_url_migration_adds_a_scoped_capability_token(tmp_path):
    face_dir = tmp_path / DOMAIN / "FaceData"
    face_dir.mkdir(parents=True)
    (face_dir / "HA001.jpg").write_bytes(b"face")
    users_store = _UsersStore(
        {
            "HA001": {
                "face_url": "http://ha.local/api/AK_AC/FaceData/HA001.jpg",
            }
        }
    )
    hass = SimpleNamespace(
        config=_Config(tmp_path),
        data={DOMAIN: {"users_store": users_store}},
    )

    asyncio.run(http_module.async_secure_local_face_profiles(hass, users_store))

    profile = users_store.get("HA001")
    assert profile["face_access_token"]
    parsed = urlsplit(profile["face_url"])
    assert parsed.path.endswith("/HA001.jpg")
    assert parse_qs(parsed.query)["token"] == [profile["face_access_token"]]
