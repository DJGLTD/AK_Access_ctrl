# Notification polling: live test and changes

Tested on an Akuvox X912 running firmware `912.30.11.226` on 25 September 2026. All device operations were reads or authentication requests. No relay was triggered, logs deleted, firmware changed or access settings modified. Credentials and individual log entries were not saved in the project.

## Findings

The unpaged door-log request returned 1,003 records. Adding `page=1` returned the newest ten records, in newest-first order. Page two matched the next ten records of the full result. The device's own web interface uses this pagination mechanism.

| Request | Records | Response body | First measured request |
| --- | ---: | ---: | ---: |
| Full log | 1,003 | 221,327 bytes | 514.2 ms |
| First page | 10 | 2,287 bytes | 70.9 ms |

The paged response was approximately 99% smaller. A second test used the updated Python API client with an authenticated web-endpoint adapter. Across three paired reads, full-log times were 415.6, 348.8 and 316.6 ms; recent-page times were 56.8, 55.8 and 55.2 ms. Medians were 348.8 ms and 55.8 ms, approximately 6.3× faster for retrieval and client parsing. The returned page exactly matched the first ten records of the full result.

These are measurements on this device and network, not end-to-end phone notification timings. The five-second poll interval and downstream notification provider still contribute delivery time.

## Implementation

- The five-second event timer requests `/api/doorlog/get/?page=1` instead of the unpaged endpoint.
- It processes new events within that small page, preserving notifications when multiple people access the gate between checks. Fetching exactly one record could lose intervening accesses; ten is the observed device page size.
- The existing cursor prevents repeated notifications. Distinct records in the same second are accepted when they occur after the saved cursor in the returned log.
- If a full ten-record page does not contain the saved cursor, an unpaged catch-up read preserves the previous bounded catch-up behavior. The existing maximum of 25 handled events per batch remains; this is not unlimited backlog recovery.
- A failed catch-up read leaves the cursor unchanged so the next poll can retry.
- An explicit history refresh, startup or recovery retains the unpaged read. Routine processing is capped locally at 25 records even if older firmware ignores pagination.
- Firmware that rejects or ignores the paging parameter uses unpaged requests for the rest of that API instance's lifetime. This preserves compatibility but cannot reduce network traffic on those devices.

Akuvox's published [HTTP API manual](https://pliki.genway.pl/Wideodomofony/Akuvox/API/akuvox_http_api_manual_android.pdf) describes ten-record pagination with model-dependent support. The X912 behavior above was established directly on the supplied device rather than inferred from that manual's model list.

## Verification and limits

All 254 tests passed and Python lint passed. Added regression coverage checks the outgoing page parameter, full-history preservation, empty logs, rejected/ignored pagination, failed requests, multi-access bursts, catch-up, unchanged cursors on failure and same-second events.

This workstation received HTTP 403 on the integration's standard `/api/doorlog/get/` route. Live measurements therefore used `/api/web/doorlog/get/` with a web session. The follow-up client test changed only the endpoint prefix through a temporary adapter; this adapter and the login credentials were not added to the integration. The standard API route and actual notification delivery still need checking from the running Home Assistant installation after deployment. Existing device/API authentication behavior was left unchanged.

The changes are local. No integration update or release has been deployed to Home Assistant.
