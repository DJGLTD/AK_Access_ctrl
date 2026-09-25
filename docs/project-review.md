# Project review — 25 September 2026

Follow-up: routine event polling now requests the newest ten-record page, verified on the supplied X912. The updated suite has 254 passing tests. See `docs/notification-polling.md` for live measurements, catch-up behavior and the API-route testing limitation. The results below describe the original cleanup pass.

## Result

Access-event polling now runs every five seconds for online devices, separately from health checks and user downloads. The previous default was one event check per 30-second health refresh, after the user download finished. Startup and recovery now process events before downloading users.

With a responsive device, this reduces the nominal polling wait from 0–30 seconds to 0–5 seconds. This is a scheduling improvement, not a measured phone-delivery benchmark: device firmware, network responses, Home Assistant and the notification provider still contribute latency. Busy polls skip subsequent timer ticks instead of accumulating requests. Offline and rebooting devices skip fast polling.

The extra event checks increase door-log requests from roughly two to twelve per minute per online device at the previous default interval. Full user downloads remain on their existing five-minute cache. Device health checks retain their configured interval.

## Changes

| Area | Finding and fix |
| --- | --- |
| Notifications | Independent event timer; startup/recovery events run before user downloads. |
| Duplicate notifications | A per-device lock serializes timer, dashboard and button event refreshes through cursor persistence. |
| Device unloading | Cancel the device event timer, active background poll and state-reset timers. |
| Shared background work | Last-device detection previously overlooked access-history and session entries. Shared sync, cleanup and update schedules now stop after the last successful unload; subsequent setup recreates the workers. |
| Schedules | Text times such as `09:30` lost their hour component. Both colon-separated and compact times now preserve hours. |
| Face creation | The add-user service referenced an undefined `ha_id`; it now uses the allocated temporary user ID. |
| Face integrity | A retry-cooldown branch referenced an undefined `full`; the periodic check now honors the cooldown and completes. |
| Mobile event history | Name-only and alternate-ID events were omitted when filtering by canonical user ID. Mobile now uses the existing desktop matching behavior. |
| Dead code | Removed nine unreferenced private functions/methods, totaling 256 lines, plus unused imports, assignments and an obsolete constant. Public compatibility entry points remain. |
| Diagnostics | Corrected host/scheme argument ordering in API debug messages. |
| Test setup | Scheduling stubs now return unsubscribe callbacks synchronously, matching production use and removing unawaited-coroutine warnings. |
| Project checks | Added development dependencies, Python lint/test configuration, cache exclusions, a dashboard-script syntax check and a pull-request checks workflow. |
| Release process | Removed the hardcoded initial-release bootstrap. Releases continue to derive their version from the merged pull request number. No version strings were changed and no release was published. |

## Verification

- Baseline: 221 tests passed, one mobile event-filter test failed, and scheduling stubs emitted warnings.
- Final: 241 tests passed with no warnings; Python lint passed.
- Parsed all 49 Python files, four JSON files and 32 JavaScript blocks/files across the dashboard assets. There are 25 HTML pages.
- Added tests for notification ordering, stalled user downloads, overlapping refreshes, busy/offline/rebooting devices, cancellation, recovery after errors, schedule time formats, shared-worker cleanup, add-user face references and face retry cooldowns.
- Checked both release scripts for JavaScript syntax; existing PR-number release tests pass.

Run locally:

```text
python -m pip install -r requirements-dev.txt
python -m ruff check .
python -m pytest -q
node scripts/check-web-scripts.cjs
```

## Browser QA

The flow tested was event history → select a user → show only that user's events, including events that identify the person by name rather than canonical ID.

Browser plugin not available. Used the bundled Playwright library with installed Microsoft Edge in headless mode; the bundled Chromium executable was absent. A temporary localhost server supplied the real HTML pages and a synthetic state response. The fixture provided signed URLs locally; it did not exercise production authentication or contact a Home Assistant instance.

Environment: `http://127.0.0.1:50804/event_history.html` at 1366 × 900 and `http://127.0.0.1:50804/event_history-mob.html` at 390 × 844. The temporary server was stopped after testing.

| Check | Result |
| --- | --- |
| Page identity | Both URLs and titles matched Event History. |
| Meaningful content | Filters, summaries and event cards rendered. |
| Error overlay | None. |
| Console health | No page errors, console errors or warnings in the final run. |
| Screenshots | Desktop and mobile captures inspected; no clipping or overlap in the tested flow. |
| Interaction | Selecting Alice showed the name-only Alice event; selecting Bob showed only Bob's ID-based event; switching back restored Alice. |

The temporary fixture initially lacked the signed-path provider expected by the pages and a favicon response. Those fixture omissions were corrected before the final passing run. Page authentication behavior was not changed.

Browser sequence: start fixture server → launch Edge → open page → select Alice → check one matching event → select Bob → check matching event → select Alice → capture screenshot → close browser and server.

## Scope and remaining checks

This was a project-wide static/reference review with focused behavioral testing. It does not establish that every dynamic path has been executed or that every public helper is unused outside this repository. Home Assistant discovers platform/config-flow methods dynamically; compatibility exports and publicly served assets were retained. For example, `www/app.js` has no in-repository HTML consumer, but is still a publicly served legacy asset. Removing it or public API aliases needs a separate compatibility decision.

The dashboard pages repeat substantial authentication and utility code. Consolidating that into shared assets would reduce maintenance, but should be a separate change with authenticated Home Assistant testing across all routes.

No live Akuvox device or running Home Assistant instance was used. Before deployment, check granted and denied access, selected notification recipients, temporary one-time users, reload/unload behavior and device load from the faster polling. Device-originated push events could reduce the polling wait further, but require confirmed firmware support and device-side configuration.

The original workspace was a source snapshot. The release pull request applies its changes to current `main`, retaining newer schedule, integrity-check and mobile event-filter fixes already merged there. The combined suite passes 356 tests; Python lint and all 32 dashboard script syntax checks pass. Development backups and screenshots remain local and are not included in the repository.
