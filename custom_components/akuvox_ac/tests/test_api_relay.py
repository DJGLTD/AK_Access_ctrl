import asyncio

from custom_components.akuvox_ac.ha_test_stubs import ensure_homeassistant_stubs

ensure_homeassistant_stubs()

from custom_components.akuvox_ac.api import AkuvoxAPI  # noqa: E402


class _RelayApi(AkuvoxAPI):
    def __init__(self):
        pass

    async def _get_api(self, *_paths):
        return {
            "retcode": 0,
            "data": {
                "Config.DoorSetting.RELAY.RelayADelay": "2",
                "Config.DoorSetting.RELAY.RelayBDelay": "3",
            },
        }

    async def _post_api(self, payload, *, rel_paths=None):
        self.last_payload = payload
        self.last_paths = tuple(rel_paths or ())
        return {"retcode": 0}


def test_get_relay_delay_reads_device_relay_config():
    api = _RelayApi()

    assert asyncio.run(api.get_relay_delay(1)) == 2
    assert asyncio.run(api.get_relay_delay(2)) == 3


def test_set_relay_delay_posts_config_set_payload():
    api = _RelayApi()

    result = asyncio.run(api.set_relay_delay(2, 9))

    assert result["relay"] == 2
    assert result["delay"] == 9
    assert api.last_paths == ("/api/config/set", "/api/")
    assert api.last_payload == {
        "target": "config",
        "action": "set",
        "data": {"Config.DoorSetting.RELAY.RelayBDelay": "9"},
    }
