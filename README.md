# Ak Access Control (Home Assistant Custom Component)

This repository packages the **Ak Access Control** custom integration for Home Assistant, plus optional web assets, in a GitHub‑ready layout.

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
3. Install **Ak Access Control**, then restart Home Assistant.

### Option B: Manual
1. Copy the `custom_components/AK_Access_ctrl/` folder into your Home Assistant `config/custom_components/` directory.
2. Restart Home Assistant.

## Configuration
This integration supports **config flow** (Settings → Devices & services → Add Integration → search for *Akuvox Access Control*).
Follow the on‑screen steps. See the in‑integration options and services for details.

## Notes
- Web assets (in `custom_components/AK_Access_ctrl/www/`) are served via `/api/AK_AC/...` and require Home Assistant authentication (cookie session or long‑lived token).
- The included `manifest.json` declares the domain as `akuvox_ac`.
- If you previously used a different folder layout (e.g., "Config Files" / "WWW Files"), that has been normalized to the Home Assistant conventions here.

---
### Support
Open an issue in this repository with logs and details of your setup.
