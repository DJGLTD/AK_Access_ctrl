import asyncio

from custom_components.akuvox_ac.api import AkuvoxAPI, _normalize_user_source


def test_normalize_user_source_prefers_firmware_source_type():
    record = {
        "UserID": "cloud-user",
        "Source": "Local",
        "SourceType": "Cloud",
    }

    normalized = _normalize_user_source(record)

    assert normalized["Source"] == "Cloud"
    assert record["Source"] == "Local"


def test_user_list_exposes_source_type_as_source():
    api = object.__new__(AkuvoxAPI)

    async def post_api(payload, *, rel_paths):
        return {
            "data": {
                "item": [
                    {
                        "UserID": "cloud-user",
                        "Name": "Cloud User",
                        "SourceType": "Cloud",
                    },
                    {
                        "UserID": "HA001",
                        "Name": "Local User",
                        "SourceType": "Local",
                    },
                ]
            }
        }

    api._post_api = post_api

    users = asyncio.run(api.user_list())

    assert users[0]["Source"] == "Cloud"
    assert users[1]["Source"] == "Local"
