from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from custom_components.akuvox_ac.const import DOMAIN
from custom_components.akuvox_ac.guest_links import _valid_action
from custom_components.akuvox_ac.integration import AkuvoxSettingsStore
from custom_components.akuvox_ac.http import (
    AkuvoxUIAction,
    _normalize_gate_controls,
    async_run_gate_control,
)
import custom_components.akuvox_ac.http as http


def test_gate_control_configuration_accepts_safe_services_and_rejects_mismatch():
    assert _normalize_gate_controls({
        "hold_open": {"entity_id": "switch.gate_hold", "service": "switch.turn_on"},
        "release_hold": {"entity_id": "switch.gate_hold", "service": "switch.turn_off"},
        "close_gate": {"entity_id": "light.drive", "service": "homeassistant.turn_off"},
        "unknown": {"entity_id": "switch.gate_hold", "service": "switch.turn_on"},
    }) == {
        "hold_open": {"entity_id": "switch.gate_hold", "service": "switch.turn_on", "label": "Hold Gate Open"},
        "release_hold": {"entity_id": "switch.gate_hold", "service": "switch.turn_off", "label": "Release Gate Hold"},
    }

    assert _valid_action({
        "entity_id": "automation.gate_open_sequence",
        "service": "automation.trigger",
        "label": "Open gate",
    })["service"] == "automation.trigger"


def test_gate_control_alert_targets_are_opt_in():
    settings = AkuvoxSettingsStore.__new__(AkuvoxSettingsStore)
    settings.data = {"alerts": {"targets": {
        "mobile_app_owner": {"gate_control": True},
        "mobile_app_other": {"gate_control": False},
    }}}
    assert settings.targets_for_event("gate_control") == ["mobile_app_owner"]


@pytest.mark.asyncio
async def test_admin_gate_control_settings_persist_and_reject_unsupported_service(monkeypatch):
    monkeypatch.setattr(http, "_request_can_access_dashboard", lambda *_args: True)
    monkeypatch.setattr(http, "_request_is_admin", lambda *_args: True)
    monkeypatch.setattr(http, "_dashboard_impersonation_payload", AsyncMock(return_value={"active": False}))
    monkeypatch.setattr(http, "_effective_self_service_context", lambda *_args, **_kwargs: None)
    entry = SimpleNamespace(options={"existing": True})
    update_entry = Mock()
    hass = SimpleNamespace(
        data={DOMAIN: {"entry-1": {"options": {}}}},
        config_entries=SimpleNamespace(
            async_get_entry=lambda _entry_id: entry,
            async_update_entry=update_entry,
        ),
    )

    class Request:
        app = {"hass": hass}

        def __init__(self, controls):
            self.controls = controls

        async def json(self):
            return {"action": "set_gate_controls", "entry_id": "entry-1", "payload": {"gate_controls": self.controls}}

        def __contains__(self, key):
            return False

    response = await AkuvoxUIAction().post(Request({
        "hold_open": {"entity_id": "switch.gate", "service": "switch.turn_on"}
    }))
    assert response["args"][0]["gate_controls"]["hold_open"]["service"] == "switch.turn_on"
    assert hass.data[DOMAIN]["entry-1"]["options"]["gate_controls"]["hold_open"]["entity_id"] == "switch.gate"
    assert entry.options["existing"] is True
    update_entry.assert_called_once()

    rejected = await AkuvoxUIAction().post(Request({
        "close_gate": {"entity_id": "switch.gate", "service": "homeassistant.turn_off"}
    }))
    assert rejected["kwargs"]["status"] == 400
    assert "close_gate" not in hass.data[DOMAIN]["entry-1"]["options"]["gate_controls"]


@pytest.mark.asyncio
async def test_gate_control_runs_configured_action_logs_actor_and_sends_opt_in_alert():
    calls = []
    services = SimpleNamespace(async_call=AsyncMock(side_effect=lambda *args, **kwargs: calls.append((args, kwargs))))
    history = SimpleNamespace(ingest=lambda event, **kwargs: calls.append(("history", event.copy())))
    coordinator = SimpleNamespace(
        health={"device_type": "door"},
        async_handle_manual_event=AsyncMock(),
    )
    settings = SimpleNamespace(targets_for_event=lambda event_type: ["mobile_app_owner"] if event_type == "gate_control" else [])
    bucket = {
        "options": {"gate_controls": {"hold_open": {"entity_id": "switch.gate_hold", "service": "switch.turn_on"}}},
        "coordinator": coordinator,
    }
    hass = SimpleNamespace(
        data={DOMAIN: {"entry-1": bucket, "access_history": history, "settings_store": settings}},
        services=services,
    )

    result = await async_run_gate_control(
        hass,
        hass.data[DOMAIN],
        entry_id="entry-1",
        control="hold_open",
        triggered_by_id="ha-steve",
        triggered_by_name="Steve",
    )

    assert result["ok"] is True
    assert result["event"]["Event"] == "Hold Gate Open by Steve"
    assert result["event"]["TriggeredBy"] == "Steve"
    coordinator.async_handle_manual_event.assert_awaited_once()
    assert services.async_call.await_args_list[0].args == (
        "switch", "turn_on", {"entity_id": "switch.gate_hold"}
    )
    assert services.async_call.await_args_list[1].args[:2] == ("notify", "mobile_app_owner")


@pytest.mark.asyncio
async def test_pedestrian_open_uses_relay_mapped_as_pedestrian():
    api = SimpleNamespace(trigger_relay=AsyncMock())
    coordinator = SimpleNamespace(health={"device_type": "door"}, async_handle_manual_event=AsyncMock())
    hass = SimpleNamespace(
        data={DOMAIN: {"entry-1": {
            "api": api,
            "coordinator": coordinator,
            "options": {"relay_roles": {"relay_a": "door", "relay_b": "pedestrian"}},
        }}},
        services=SimpleNamespace(async_call=AsyncMock()),
    )
    result = await async_run_gate_control(
        hass, hass.data[DOMAIN], entry_id="entry-1", control="pedestrian_open", triggered_by_name="Steve"
    )
    api.trigger_relay.assert_awaited_once_with(2)
    assert result["event"]["Event"] == "Pedestrian Open by Steve"
    hass.services.async_call.assert_not_awaited()


@pytest.mark.asyncio
async def test_failed_gate_action_is_still_logged_and_alerted():
    coordinator = SimpleNamespace(
        health={"device_type": "door"},
        async_handle_manual_event=AsyncMock(),
    )
    hass = SimpleNamespace(
        data={DOMAIN: {"entry-1": {
            "options": {"gate_controls": {"close_gate": {"entity_id": "cover.gate", "service": "cover.close_cover"}}},
            "coordinator": coordinator,
        }, "settings_store": SimpleNamespace(targets_for_event=lambda _event: ["mobile_app_owner"])}},
        services=SimpleNamespace(async_call=AsyncMock(side_effect=RuntimeError("simulated service failure"))),
    )
    result = await async_run_gate_control(
        hass, hass.data[DOMAIN], entry_id="entry-1", control="close_gate", triggered_by_name="Steve"
    )
    assert result["ok"] is False
    assert result["event"]["Result"] == "Control Action Failed"
    assert result["event"]["Error"] == "RuntimeError"
    assert hass.services.async_call.await_count == 2
    assert hass.services.async_call.await_args_list[-1].args[0:2] == ("notify", "mobile_app_owner")
