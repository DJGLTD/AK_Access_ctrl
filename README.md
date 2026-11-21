# Ak Access Control (Home Assistant Custom Component)

This repository packages the **Ak Access Control** custom integration for Home Assistant, its Akuvox access-control dashboard, Hikvision ANPR camera support, and time-limited guest links.

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

### Hikvision ANPR cameras

Add another **Ak Access Control** integration entry and choose **Hikvision ANPR**. Configure the camera host, port, channel, username/password, and whether to use HTTPS. HTTPS with certificate verification is enabled by default (port 443). Plain HTTP is rejected unless you explicitly enable `allow_insecure_http` and set the camera's HTTP port; HTTP exposes camera traffic to on-path tampering. The dashboard's **ANPR Camera** page can list, add, edit, and remove registrations using ISAPI Digest authentication. The configured camera account needs permission to search and edit the vehicle list.

Hikvision's ISAPI features vary by camera model and firmware. The integration uses the Traffic channel license-plate list API (`searchLPListAudit`, record create/modify, and `DelLicensePlateAuditData`). Confirm that your camera supports these endpoints before relying on it for access decisions. See [docs/system-overview.md](docs/system-overview.md) and Hikvision's [ISAPI developer portal](https://tpp.hikvision.com/download/).

### Guest access links

Dashboard administrators can open **Guest Links** to create an unlisted URL granting selected Home Assistant entity actions. Each link can have a start time, expiry, daily time window, weekdays, and timezone; leaving the start and expiry blank keeps it active until revoked. Actions are limited to selected covers, lights, switches, buttons, scripts, and scenes. Links can be revoked immediately.

The URL is a bearer credential and is shown once when created. Anyone who has it can use its selected actions while the policy is active. The integration prefers the configured Home Assistant Cloud URL when available, then the configured external URL. Link creation requires an HTTPS public hostname; set up Home Assistant Cloud or an HTTPS external URL first. Public pages do not require a Home Assistant login and do not load third-party assets. Each valid page visit and action attempt is persisted with source IP, user-agent, language/browser hints, time, outcome, and action. Audit history is capped at 2,000 records. Successful use fires `akuvox_non_key_access_granted` and `akuvox_ac_guest_action` Home Assistant events and sends a `guest_access` alert to notification targets you explicitly enable in Global Settings.

Guest links currently call explicitly selected Home Assistant entity services. They do not create Akuvox users or update camera registration lists.

## Release Versioning
Release versions are derived from the merged pull request number. The integration manifest and dashboard version label are updated by the release workflow so HACS and Home Assistant show release versions instead of commit hashes.

- PR `#416` releases as `v4.1.6`.
- PR `#500` releases as `v5.0.0`.
- PR `#525` releases as `v5.2.5`.
- PR `#555` releases as `v5.5.5`.
- Direct commits to `main` without an associated pull request do not create a release.

## Notes

- Access events are checked every five seconds while a device is online, independently of the health-check setting and the five-minute user-list cache. Routine checks request the newest ten-record page from the device. If the saved event has fallen outside that page, a catch-up read retains the existing 25-event processing limit. Explicit history refreshes, startup and recovery still read the full log. Firmware that rejects or ignores pagination falls back to unpaged reads, with routine local processing capped at 25 records. The five-second interval is a polling target; device response times and Home Assistant's notification provider still affect delivery.
- The guest link page and capability API are intentionally public; the unlisted, high-entropy URL is their credential. Treat and share those URLs like passwords.
- Uploaded face images are stored in the Home Assistant config at `config/akuvox_ac/FaceData/` and are preserved across updates.
- Dashboard pages are available at `/akuvox-ac/index`, `/akuvox-ac/users`, `/akuvox-ac/device-edit`, `/akuvox-ac/schedules`, and `/akuvox-ac/face-rec`; management requires an authenticated Home Assistant administrator. Guest link administration is at `/akuvox-ac/guest-links`; Hikvision registration management is at `/akuvox-ac/anpr`.
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
