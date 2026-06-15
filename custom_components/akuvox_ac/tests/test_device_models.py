from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

from custom_components.akuvox_ac.const import (
    AKUVOX_DEVICE_MODELS,
    CONF_DEVICE_MODEL,
    DEFAULT_DEVICE_MODEL,
)
from custom_components.akuvox_ac.http import (
    _next_health_check_eta,
    _serialize_devices,
)


WWW = Path(__file__).resolve().parents[1] / "www"


def test_device_model_is_serialized_for_dashboard():
    coordinator = SimpleNamespace(
        device_name="Front Gate",
        health={
            "device_type": "Intercom",
            "device_model": "R29",
            "ip": "10.30.0.73",
            "online": True,
            "last_checked": "2026-06-14T09:30:00+01:00",
        },
        events=[],
        users=[],
    )
    devices, _ = _serialize_devices(
        {
            "entry-1": {
                "coordinator": coordinator,
                "options": {CONF_DEVICE_MODEL: "R29"},
            }
        }
    )

    assert devices[0]["model"] == "R29"
    assert devices[0]["last_checked"] == "2026-06-14T09:30:00+01:00"


def test_device_model_falls_back_for_existing_entries():
    coordinator = SimpleNamespace(
        device_name="Legacy Gate",
        health={"device_type": "Intercom", "online": True},
        events=[],
        users=[],
    )
    devices, _ = _serialize_devices(
        {"entry-1": {"coordinator": coordinator, "options": {}}}
    )

    assert devices[0]["model"] == DEFAULT_DEVICE_MODEL


def test_next_health_check_uses_earliest_device_interval():
    first = SimpleNamespace(
        health={"last_health_check": "2026-06-14T09:30:00+00:00"},
        update_interval=timedelta(seconds=30),
    )
    second = SimpleNamespace(
        health={"last_health_check": "2026-06-14T09:29:40+00:00"},
        update_interval=timedelta(seconds=60),
    )
    root = {
        "entry-1": {"coordinator": first, "options": {}},
        "entry-2": {"coordinator": second, "options": {}},
    }

    eta = _next_health_check_eta(
        root,
        now=datetime(2026, 6, 14, 9, 30, 10, tzinfo=timezone.utc),
    )

    assert eta == "2026-06-14T09:30:30+00:00"


def test_next_health_check_rolls_over_missed_intervals():
    coordinator = SimpleNamespace(
        health={"last_health_check": "2026-06-14T09:30:00+00:00"},
        update_interval=timedelta(seconds=30),
    )

    eta = _next_health_check_eta(
        {"entry-1": {"coordinator": coordinator, "options": {}}},
        now=datetime(2026, 6, 14, 9, 31, 5, tzinfo=timezone.utc),
    )

    assert eta == "2026-06-14T09:31:30+00:00"


def test_model_selectors_and_artwork_are_bundled():
    assert "R29" in AKUVOX_DEVICE_MODELS
    assert "A08" in AKUVOX_DEVICE_MODELS

    dashboard = (WWW / "index.html").read_text(encoding="utf-8")
    assert 'id="deviceOverview"' in dashboard
    assert "const DEVICE_MODEL_ASSETS" in dashboard
    assert "/api/AK_AC/device-models/${asset}.svg" in dashboard

    for filename in ("device_edit.html", "device_edit-mob.html"):
        editor = (WWW / filename).read_text(encoding="utf-8")
        assert 'id="modelSelect"' in editor
        assert "action: 'set_device_model'" in editor

    artwork = WWW / "device-models"
    for filename in (
        "generic.svg",
        "x912.svg",
        "screen-tall.svg",
        "keypad-door.svg",
        "compact-door.svg",
        "face-slim.svg",
        "access-keypad.svg",
        "controller.svg",
    ):
        asset = artwork / filename
        assert asset.is_file()
        assert 'preserveAspectRatio="xMidYMid meet"' in asset.read_text(
            encoding="utf-8"
        )

    assert "X912:'x912'" in dashboard
    assert (
        ".device-visual img{display:block;width:auto;height:auto;"
        "max-width:100%;max-height:158px;aspect-ratio:auto;"
        "object-fit:contain;"
    ) in dashboard
    assert "width:100%;height:100%;object-fit:contain" not in dashboard
    assert 'src="/api/AK_AC/project-icon.svg"' in dashboard
    assert (WWW / "project-icon.svg").is_file()


def test_desktop_dashboard_fits_the_viewport():
    dashboard = (WWW / "index.html").read_text(encoding="utf-8")
    assert "@media (min-width:901px)" in dashboard
    assert "height:100dvh;min-height:0" in dashboard
    assert (
        "grid-template-rows:auto auto minmax(0,.95fr) "
        "minmax(0,1.05fr) auto"
    ) in dashboard
    assert ".table thead{position:sticky;top:0;z-index:2}" in dashboard
    assert "height:auto;min-height:0;max-height:100%" in dashboard


def test_dashboard_branding_events_and_update_controls():
    dashboard = (WWW / "index.html").read_text(encoding="utf-8")
    mobile = (WWW / "index-mob.html").read_text(encoding="utf-8")

    assert "DJG Technical Services LTD" in dashboard
    assert "DJG Technical Services LTD" in mobile
    assert 'id="btnSystemUpdate" data-hacs-update' in dashboard
    assert dashboard.count("data-hacs-update") >= 2
    assert "document.querySelectorAll('[data-hacs-update]')" in dashboard
    assert "? 'bi-telephone-fill'" in dashboard
    assert "? 'Access Granted'" in dashboard

    for shell_name in ("head.html", "head-mob.html"):
        shell = (WWW / shell_name).read_text(encoding="utf-8")
        assert "header.app-header," in shell
        assert ".mobile-launcher," in shell
        assert "display: none !important;" in shell
        assert "padding: 0 !important;" in shell


def test_dashboard_user_actions_are_not_clipped():
    dashboard = (WWW / "index.html").read_text(encoding="utf-8")

    assert "#sectionUsers{display:flex;flex-direction:column}" in dashboard
    assert "#sectionUsers .user-panel-header{flex:0 0 auto;overflow:visible}" in dashboard
    assert "#sectionUsers .card-body{flex:1 1 auto;min-height:0}" in dashboard
    assert (
        ".user-heading .btn{display:inline-flex;align-items:center;"
        "min-height:31px;white-space:nowrap}"
    ) in dashboard


def test_dashboard_uses_mobile_cards_instead_of_squeezed_user_columns():
    dashboard = (WWW / "index.html").read_text(encoding="utf-8")

    assert "@media (max-width:600px)" in dashboard
    assert "#sectionUsers .table thead{display:none}" in dashboard
    assert 'data-label="Face Recognition"' in dashboard
    assert 'class="user-actions-cell" data-label="Actions"' in dashboard
    assert ".brand{display:none}" in dashboard
    assert ".top-actions{display:grid;grid-template-columns:1fr;gap:8px}" in dashboard


def test_dashboard_switches_between_sync_and_health_check_kpi():
    dashboard = (WWW / "index.html").read_text(encoding="utf-8")
    mobile = (WWW / "index-mob.html").read_text(encoding="utf-8")

    for page in (dashboard, mobile):
        assert 'id="kpiNextLabel">Next Health Check<' in page
        assert "const syncScheduled = syncActive" in page
        assert "syncScheduled ? 'Next Sync' : 'Next Health Check'" in page
        assert "k.next_health_check_eta" in page


def test_device_overview_uses_persisted_last_checked_time():
    dashboard = (WWW / "index.html").read_text(encoding="utf-8")
    mobile = (WWW / "index-mob.html").read_text(encoding="utf-8")

    assert "Last Checked: ${escapeHtml(lastChecked)}" in dashboard
    assert "d.last_checked" in dashboard
    assert "Awaiting first check" in dashboard
    assert "<th>Last Checked</th>" in mobile
    assert "d.last_checked" in mobile
