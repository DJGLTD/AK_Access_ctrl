# Akuvox Access Control (Home Assistant Custom Component)

This repository packages the **Akuvox Access Control** custom integration for Home Assistant, plus optional web assets, in a GitHub‑ready layout.

## Structure
```text
custom_components/AK_Access_ctrl/
├── __init__.py                # Integration entry point
├── ...                        # Platforms, coordinator, api, etc.
└── www/                       # Web assets served from /api/AK_AC/*
hacs.json                      # Allows adding this repo as a HACS custom repository
.gitignore
README.md
```

## Installation

### Option A: HACS (recommended)
1. In HACS → Integrations → ⋮ → **Custom repositories** → add this repo URL.
2. Category: **Integration**.
3. Install **Akuvox Access Control**, then restart Home Assistant.

### Option B: Manual
1. Copy the `custom_components/AK_Access_ctrl/` folder into your Home Assistant `config/custom_components/` directory.
2. Restart Home Assistant.

## Configuration
This integration supports **config flow** (Settings → Devices & services → Add Integration → search for *Akuvox Access Control*).
Follow the on‑screen steps. See the in‑integration options and services for details.

## Notes
- Web assets (in `custom_components/AK_Access_ctrl/www/`) are served via `/api/AK_AC/...` and require Home Assistant authentication (cookie session or long‑lived token).
- Hidden Akuvox dashboards are available at `/akuvox-ac/index`, `/akuvox-ac/users`, `/akuvox-ac/device-edit`, `/akuvox-ac/schedules`, and `/akuvox-ac/face-rec`; they respect the same authentication (HA session or `?token=` query parameter).
- When a non–key-holder user is granted access the integration fires the `akuvox_ac_non_key_access_granted` event, including the device, user, method, and timestamp so you can trigger automations.
- Webhook endpoints for access granted/denied/device-offline events are pre-generated per device (see the main dashboard). Use them from the Akuvox Action URL feature; each call updates a matching sensor and fires an `akuvox_ac_webhook_<event>` Home Assistant event with the received payload.
- The included `manifest.json` declares the domain as `akuvox_ac`.
- If you previously used a different folder layout (e.g., "Config Files" / "WWW Files"), that has been normalized to the Home Assistant conventions here.

---
### Support
Open an issue in this repository with logs and details of your setup.
