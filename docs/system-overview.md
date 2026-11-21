# System review and implementation notes

## Current system map (fresh clone of `main`, 2026-11-21)

Ak Access Control is a Python Home Assistant custom integration (HACS package), not a standalone server. The `akuvox_ac` config flow creates device entries; `integration.py` initializes shared JSON-backed stores and background workers, creates a coordinator for each Akuvox endpoint, registers HA services/entities, and installs the browser dashboard. `api.py` handles the Akuvox device protocol. `coordinator.py` polls health, users, and events, drives notifications, and refreshes HA state. `http.py` is the web/API surface: it contains public asset and dashboard routes, dashboard access decisions, authenticated management operations, and some unauthenticated enrollment/webhook routes. The browser UI is a set of static desktop/mobile HTML pages in `www/`, generally with inline CSS and JavaScript.

Persistent data is split between Home Assistant `Store` files (users, groups, schedules, settings, access history, per-entry state) and uploaded face images in the integration config directory. Devices are polled locally; dashboard operations call the integration API/services. The repo has config flow and entity tests with Home Assistant test stubs, Python Ruff checks, and a JavaScript syntax checker. The fresh clone starts clean on `main` at `70519c6` (`v4.3.5`).

## UI review

The dashboard already has responsive page variants and a consistent dark palette, but its 25 HTML files duplicate page structure, styles, and browser logic. `head.html` and `index.html` use separate design tokens and Bootstrap is loaded from a public CDN. The route/auth logic and UI API are coupled to the large `http.py` module. A maintainable UI refresh should first consolidate shared navigation, design tokens, form controls, and accessible status/confirmation patterns into local shared assets; then migrate the desktop/mobile pages without changing their access rules. Follow-up work should add an overview/navigation map and usable device states for disconnected, loading, empty, and error conditions.

## ANPR research and constraints

Hikvision describes ISAPI as an HTTP/REST protocol and requires device-model-specific documentation for exact support. The Hikvision ISAPI ANPR guide and camera-specific material show multiple approaches: traffic-channel license-plate lists (`searchLPListAudit`, `licensePlateAuditData`, and `DelLicensePlateAuditData`) and older TCG entrance VCL APIs. Digest authentication is documented for at least the TCG VCL operations. Consequently the camera model, firmware, channel, list type, and endpoint profile must be configurable or capability-detected; a device-free test can validate request/response handling but cannot prove compatibility with a particular camera firmware.

## Guest links: security and operation

A guest link is a bearer credential: anyone holding its URL can use its granted actions. Store only a cryptographic hash of the random code, disclose the raw URL once at creation, and support immediate revocation. Each link must grant only explicitly selected HA entities/actions and enforce optional start/end dates, daily time windows, weekdays, and timezone at the server. The public handler should avoid third-party assets, return no-store/referrer protections, rate-limit actions, validate source metadata, and record access attempts and action outcomes. Home Assistant Cloud/Nabu Casa can expose the configured HA URL, but it does not make arbitrary unauthenticated custom routes safe by itself; this integration explicitly serves a narrowly scoped guest route without HA login and retains the action allowlist and timing checks in that handler. Link creation requires a public HTTPS hostname, with Home Assistant Cloud preferred and a configured external URL as fallback.

## Validation boundary

Automated tests can cover flow, policy checks, timing, token secrecy, revocation, allowlisted service execution, audit records, and device protocol fixtures. Live verification still requires the user's Hikvision model/firmware and a real Home Assistant Cloud URL. No secrets, camera addresses, or installation data were found in the cloned source.

Guest-access notifications use a separate `guest_access` setting per notification target in Global Settings. This keeps guest-link alerts opt-in and separate from the existing per-user access-granted notifications.
