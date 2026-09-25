# Automatic update investigation — 25 September 2026

## Observed failure

A running Home Assistant installation rejected the updater startup callback with a RuntimeError because it called `hass.async_create_task` from an executor thread. A subsequent warning reported that `async_run_scheduled_update` was never awaited. Scheduled checks were enabled, but Last checked remained at the previous installation time.

Inspection was read-only; no installation, restart or device action was triggered.

## Cause and correction

The startup listener, daily timer and scheduled restart used ordinary synchronous callbacks that called an event-loop-only API. Home Assistant dispatches unmarked synchronous jobs to its executor; it rejects this unsafe call. The live traceback confirms the startup failure, and the daily timer uses the same code pattern. The callbacks are now coroutines that await their work on the event loop. See [Home Assistant's thread-safety guidance](https://developers.home-assistant.io/docs/asyncio_thread_safety/#hassasync_create_task).

The updater now also handles integration setup after Home Assistant is already running. It performs an overdue startup check once, while respecting enabled status, today's previous check and the configured local check time. Repeated setup for multiple devices does not duplicate that check, and shutdown cancels a pending reload catch-up task. Scheduling registration failures are logged rather than silently suppressed.

Settings saves now accept only editable options. Previously, the dashboard sent cached status fields with its form; saving an older page could overwrite newer check results. Backend status updates remain authoritative.

Installation previously overwrote Last checked when it wrote Last installed, explaining identical timestamps without proving that daily checks ran. Installation outcomes now preserve the time of the actual pre-install release check. Confirmed installation alone changes Last installed. Checks and installs can still legitimately share a displayed second if they complete quickly.

## Validation and rollout

Regression tests reproduce executor dispatch, repeated daily checks, schedule changes and cancellation, stale settings saves, startup/reload catch-up, skipped fresh/disabled checks, restart dispatch, failed check reporting, separate check/install timestamps, and shutdown cancellation. Eight regression cases failed against the previous implementation before the fixes.

Publish through the normal pull-request release process; the release version follows the merged pull request number. The affected tenant then needs the fixed release loaded before its automatic scheduler can be relied on. Verify the intended check time on the tenant. After rollout, confirm a scheduled check advances Last checked while Last installed remains unchanged unless a new release is installed.

Before opening the pull request, all 356 tests passed against the combined changes on current main. Python lint and all 32 dashboard script syntax checks also passed.
