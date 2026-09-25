# Relay notification timing — 25 September 2026

## What the logs establish

The installation was running v4.3.1, with Home Assistant configured for Europe/London. Times below are British Summer Time; Home Assistant's UTC trace and diagnostic times have been converted to match the intercom's local log.

| Stage | Time | Evidence |
| --- | --- | --- |
| Incoming call logged | 11:53:21 | Intercom call record |
| DTMF access/relay A logged | 11:53:23 | Intercom door record; whole-second resolution |
| Relay webhook starts automation | 11:53:23.513 | Automation trace |
| Automation presses Access Permitted | 11:53:23.515 | First automation action |
| Button skips its synthetic notification | 11:53:23.563 | No recent door event available yet |
| Immediate full-log read | 11:53:23, 300 ms request | Returns 1,007 records, still ending at the previous access |
| Refresh suppresses notification for previous event | 11:53:24.125 | Notification diagnostics and stale response |
| Automation finishes | 11:53:24.162 | Trace; about 0.65 seconds total |
| Regular five-second poll finds new event | 11:53:26, 24 ms request | Ten-record page now contains the 11:53:23 access |
| Notifications queued for three recipients | 11:53:26.807–26.809 | Integration notification diagnostics |

The last recipient was queued **3.296 seconds after the webhook started**. Relative to the intercom's whole-second relay timestamp, this is roughly 3–4 seconds. Recipient dispatches were separated by less than three milliseconds.

The integration records `sent` after scheduling the Home Assistant notify service with `blocking=False`. This confirms hand-off inside Home Assistant, not push-provider acceptance or receipt/display on a phone. The reported approximately eight-second relay-to-phone delay therefore cannot be fully measured from these logs. Roughly four to five seconds remain unaccounted for by the available server-side timing; attributing that specifically to the provider, network or phone would require further measurement.

The 11:59 access independently shows the same pattern: webhook at 11:59:02.207, immediate read still showing the preceding event, and notifications queued at 11:59:06.872–06.875 (about 4.67 seconds later).

## Process and avoidable wait

The intercom sends the relay webhook promptly. The automation presses the integration's Access Permitted button and then evaluates its lighting conditions. There is no eight-second delay in that automation.

The old button path creates a synthetic event, then performs one full-history read with notifications suppressed. In these two examples the device had not yet published the new door-log row when that read occurred. The normal five-second poll subsequently finds the row and sends the named notification.

The paged production API route is working: it returns ten records in approximately 24–25 ms for the relevant polls. Those production reads now verify the standard API route as well as the earlier web-route measurements. The button's full-history read costs about 300 ms, but waiting for the next polling tick is the larger avoidable delay. There is no call-log download in this observed relay-to-notification path.

The precise moment the device publishes its row falls between the stale immediate response and the successful later poll; the existing logs do not narrow it further. This prevents predicting an exact speedup from the historical test.

## Change

Access Permitted now starts an immediate recent-page refresh and retries at 250 ms intervals while waiting for a recent grant, for up to three seconds or thirteen reads. A slow request uses the existing API timeout; the retry deadline does not cancel notification delivery or cursor persistence midway through an event. Normal five-second polling handles anything arriving later.

The normal event cursor and lock remain responsible for notification delivery. New events are neither force-replayed nor suppressed, and notification text comes from the actual device event. Overlapping webhook calls share one refresh task; integration unload cancels it. Routine polling frequency and notification recipients are unchanged. Existing relay automations can keep pressing the same button.

Validation: **364 tests pass**, including eight new cases covering delayed log publication, timeout fallback, overlapping webhooks/polls, already-processed events, slow reads, empty reads, unload cancellation, and the button path. Python lint passes.

This investigation used read-only access to the tenant. No relay, automation, notification, restart, installation or settings change was triggered during the investigation. After deployment, repeat a real call and compare the webhook, notification hand-off and observed phone-display times before claiming an end-to-end improvement.
