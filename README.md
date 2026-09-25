# Ak Access Control (Home Assistant Custom Component)

This repository packages the **Ak Access Control** custom integration for Home Assistant, plus optional web assets, in a GitHub‑ready layout.

## Structure
```text
custom_components/akuvox_ac/
├── __init__.py                # Integration entry point
├── brand/                     # Home Assistant integration icon/logo assets
├── ...                        # Platforms, coordinator, api, etc.
└── www/                       # Web assets served from /api/AK_AC/*
hacs.json                      # Allows adding this repo as a HACS custom repository
icon.svg                       # Source artwork for the repository icon
.gitignore
README.md
```

## Installation

### Option A: HACS (recommended)
1. In HACS → Integrations → ⋮ → **Custom repositories** → add this repo URL.
2. Category: **Integration**.
3. Install **Ak Access Control**, then restart Home Assistant.

### Option B: Manual
1. Copy the `custom_components/akuvox_ac/` folder into your Home Assistant `config/custom_components/` directory.
2. Restart Home Assistant.

## Configuration
This integration supports **config flow** (Settings → Devices & services → Add Integration → search for *Akuvox Access Control*).
Follow the on‑screen steps. See the in‑integration options and services for details.

## Release Versioning
Release versions are derived from the merged pull request number. The integration manifest and dashboard version label are updated by the release workflow so HACS and Home Assistant show release versions instead of commit hashes.

- PR `#416` releases as `v4.1.6`.
- PR `#500` releases as `v5.0.0`.
- PR `#525` releases as `v5.2.5`.
- PR `#555` releases as `v5.5.5`.
- Direct commits to `main` without an associated pull request do not create a release.

## Notes

- Access events are checked every five seconds while a device is online, independently of the health-check setting and the five-minute user-list cache. Routine checks request the newest ten-record page from the device. If the saved event has fallen outside that page, a catch-up read retains the existing 25-event processing limit. Explicit history refreshes, startup and recovery still read the full log. Firmware that rejects or ignores pagination falls back to unpaged reads, with routine local processing capped at 25 records. The five-second interval is a polling target; device response times and Home Assistant's notification provider still affect delivery.
- Web assets (in `custom_components/akuvox_ac/www/`) are served via `/api/AK_AC/...` and require Home Assistant authentication (cookie session or long‑lived token).
- Uploaded face images are stored in the Home Assistant config at `config/akuvox_ac/FaceData/` and are preserved across updates.
- Hidden Akuvox dashboards are available at `/akuvox-ac/index`, `/akuvox-ac/users`, `/akuvox-ac/device-edit`, `/akuvox-ac/schedules`, and `/akuvox-ac/face-rec`; they respect the same authentication (HA session or `?token=` query parameter).
- The included `manifest.json` declares the domain as `akuvox_ac`.
- Home Assistant brand images are shipped in `custom_components/akuvox_ac/brand/` so the integration card can display the logo and icon locally.
- If you previously used a different folder layout (e.g., "Config Files" / "WWW Files"), that has been normalized to the Home Assistant conventions here.

---
### Development checks

Use Python 3.12 or newer and Node.js. In a virtual environment, install `requirements-dev.txt`, then run:

```text
python -m ruff check .
python -m pytest -q
node scripts/check-web-scripts.cjs
```

The Python tests use lightweight Home Assistant stubs. They do not replace testing on a running Home Assistant instance with an Akuvox device. Pull requests run these checks automatically.

### Support
Open an issue in this repository with logs and details of your setup.
