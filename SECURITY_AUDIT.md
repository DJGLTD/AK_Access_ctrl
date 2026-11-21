# Security Audit

**Scope:** Static review of the `AK_Access_ctrl` Home Assistant integration in this checkout, including Python integration code, web UI assets, tests, manifest and development configuration. The initial audit was read-only. Subsequent approved hardening changes and verification are recorded with each finding.

**Review method:** Read-only code and configuration inspection, targeted searches for route registration/authentication, input and output handling, network transport, file access, credential patterns, dependency declarations, subprocess/SQL usage, and HTML sinks. The installed ECC `security-review` checklist and a read-only review by the available `security-reviewer` agent informed this audit.

## Executive summary

No Critical issue was confirmed. The two original High findings and one additional High finding identified during feature review have been addressed and verified:

1. Akuvox requests now verify TLS by default and do not fall back to HTTP when credentials are configured unless an explicit insecure transport option is enabled.
2. Face image retrieval now requires dashboard authorization or a per-profile capability token. Existing local face URLs are tokenized before device synchronization.
3. Hikvision ANPR camera management now defaults to verified HTTPS; plain HTTP requires an explicit insecure-transport opt-in.

Three Medium findings were confirmed: public exposure of inbound call data, unvalidated face uploads, and indefinite retention of vehicle registration values in the ANPR audit store. Call-data disclosure, upload validation and ANPR retention have been addressed. The inbound-call webhook remains publicly callable without a shared secret; rate limiting and response minimization reduce the risk, but authentication remains unresolved.

## Phase 1 — Discovery

### Architecture and technology

- Home Assistant custom integration (`custom_components/akuvox_ac`) implemented primarily in Python.
- Integrates Akuvox intercom/keypad devices using `aiohttp` and Home Assistant's HTTP client; includes a Hikvision ANPR client using ISAPI Digest authentication.
- Exposes Home Assistant `HomeAssistantView` HTTP routes and a static dashboard built from HTML, CSS and JavaScript. The UI is served from `custom_components/akuvox_ac/www`.
- Persists integration data through Home Assistant storage (`Store`) and face images in files under Home Assistant's configuration directories. No SQL database or ORM use was found.
- Runtime framework dependencies are supplied by Home Assistant; `manifest.json` lists no extra requirements. `requirements-dev.txt` declares ranged `aiohttp`, `voluptuous`, `pytest`, `pytest-asyncio` and `ruff` dependencies. No dependency lockfile was found in the inspected checkout.

### Authentication, authorization and external routes

- Home Assistant authenticated users and custom dashboard sessions gate dashboard/API operations. Custom dashboard sessions are random tokens with a sliding expiry and are passed by a dedicated header or query parameter (`http.py:602-647`). Admin checks are centralized at `http.py:693-708`.
- Most API views set `requires_auth = False` but enforce dashboard/admin checks inside their handlers before returning or mutating protected state. These include settings, diagnostics, support bundles, reservations, face upload, guest-link management and Hikvision administration. The guest action API is intentionally unauthenticated and uses a capability token plus server-side action allowlists and time policy (`guest_links.py:289-358`).
- The inbound-call webhook is intentionally registered without Home Assistant authentication (`http.py:1602-1605`) and processes current device call logs.
- The dashboard and static UI endpoints are publicly routable; protected dashboard data is normally fetched through separately gated API routes. The face-image branch of the static route is an exception described in Finding 2.

### Sensitive data flows

- Device usernames/passwords are used for Akuvox Basic authentication and Hikvision Digest authentication. Configuration is held in Home Assistant config entries; no credential values are included in this report.
- Face images are written to persistent storage and made available through an intercom-facing URL. They contain biometric personal data.
- Phone numbers and call records are fetched from devices, logged/stored in access history and returned by the inbound-call webhook.
- Vehicle registrations are read from Hikvision cameras and written to the ANPR audit store.
- Guest access audit events store client IP, user-agent, language, referrer and client-hint metadata, with a maximum of 2,000 events (`guest_links.py:100-110,153-156`).

## Phase 2 — Findings

### Finding 1 — Akuvox credentials can traverse unverified TLS or plaintext HTTP

- **Severity:** High
- **Status:** Confirmed and remediated
- **Affected locations:** `custom_components/akuvox_ac/api.py:103-114,296-323,377-412`
- **Explanation:** The API client ignores the constructor's TLS preferences, forces `verify_ssl` to false, and probes multiple schemes including HTTP. The probe and request methods pass `self._auth` to aiohttp regardless of whether the selected scheme is HTTP or HTTPS. This permits Basic authentication credentials to be sent without transport encryption and permits a network attacker to impersonate the device when TLS certificate checks are disabled.
- **Exploitation scenario:** An attacker with access to the device's network path can answer an HTTP fallback probe or intercept a request, capture the reusable Basic authentication credentials, and use them to access or alter the intercom.
- **Recommended remediation:** Honor configured TLS settings and verify certificates by default. Do not send credentials over HTTP. If compatibility requires HTTP or invalid certificates, make that an explicit per-device insecure option with a clear warning and avoid transmitting reusable credentials where possible.
- **Required regression tests:** Assert that default HTTPS requests verify certificates; constructor/configuration TLS choices are honored; HTTP probes and fallback requests never include BasicAuth; and a valid HTTPS device connection continues to work.
- **Remediation:** Implemented. The client now honors TLS constructor settings, enables verification by default, skips HTTP fallback when credentials are configured unless `allow_insecure_http` is explicitly enabled, and exposes TLS/insecure-HTTP settings in Akuvox configuration. Users with self-signed certificates can explicitly disable certificate verification; users whose credentialed devices only support HTTP must explicitly enable insecure HTTP.
- **Verification:** Regression tests first failed against the old behavior, then passed after the fix. `test_api_transport_security.py`, `test_api_face_payload.py` and `test_api_events.py`: **23 passed**. Ruff passed for the changed transport and test files.
- **Existing functionality impact:** HTTP-only devices without configured credentials still use HTTP fallback. Credentialed HTTP-only devices now require the explicit opt-in. Self-signed HTTPS devices require the explicit verification opt-out. No endpoint/API path changed.

### Finding 2 — Face images are served without authentication

- **Severity:** High
- **Status:** Confirmed and remediated
- **Affected locations:** `custom_components/akuvox_ac/http.py:4010-4044`
- **Explanation:** The original route had `requires_auth = False` and returned persistent `FaceData` files without authorization. The path was constrained against traversal, but the image response itself was public.
- **Exploitation scenario:** A remote visitor who can reach Home Assistant and knows or guesses a profile ID/filename can request its face-image path and obtain biometric data without logging in.
- **Recommended remediation:** Require authorization for dashboard retrieval and serve intercom-bound images through unguessable per-profile capability tokens scoped to one image. Preserve the intercom's legitimate fetch capability while preventing anonymous enumeration and retrieval.
- **Required regression tests:** Unauthenticated reads of stored face images fail; authorized requests succeed; signed device URLs expire and cannot access other files; traversal attempts remain rejected; legacy migration still works under the intended authorization model.
- **Remediation:** Implemented. The asset route now requires dashboard authorization or a matching per-profile random capability token. Uploads attach a token to the stored face URL, and the synchronization path adds tokens to existing local face URLs before sending them to devices. Face responses use `Cache-Control: no-store` and `X-Content-Type-Options: nosniff`.
- **Verification:** Added tests for anonymous denial, invalid-token denial, valid scoped-token access and migration of existing local URLs. `test_face_asset_authorization.py`, `test_ui_upload_face.py`, `test_contact_sync.py`, `test_face_status.py` and `test_api_face_payload.py`: **44 passed**. Ruff passed for the changed route, sync and test files.
- **Existing functionality impact:** The path remains unchanged and the device fetch URL gains a `token` query parameter. Existing local URLs are refreshed during synchronization. Home Assistant users with dashboard access can still retrieve face images. Requests lacking dashboard authorization or the scoped token are denied.

### Finding 3 — Inbound-call webhook returns sensitive call records publicly

- **Severity:** Medium
- **Status:** Partially remediated; anonymous data disclosure is fixed, but webhook authentication remains unresolved
- **Affected locations:** `custom_components/akuvox_ac/http.py:1602-1650,1435-1550`
- **Explanation:** The GET/POST webhook has `requires_auth = False`. It queries configured intercom call logs and records events. Before remediation, it also returned the selected event, including caller numbers, raw call data, and matched Home Assistant user details. A caller-supplied number is only used as a filter; it is not an authentication credential.
- **Exploitation scenario:** An unauthenticated remote party who can reach the endpoint can retrieve recent caller information and matched profile data. Repeated requests may also consume device/API resources.
- **Recommended remediation:** Configure a high-entropy secret or signed request in the device webhook URL before requiring authentication. Keep event ingestion and notifications intact. Avoid returning raw call records and personal data to unauthenticated callers, and rate-limit requests.
- **Required regression tests:** Requests without a valid webhook credential are rejected; authorized requests continue to ingest events and send intended alerts; responses omit raw call/profile data; bursts are rate-limited without dropping normal device calls.
- **Remediation:** Anonymous responses now contain only an `ok` flag; authorized dashboard users retain the detailed response. The endpoint applies per-client and global sliding-window limits (20 requests per minute per peer, 240 globally) before reading device call logs.
- **Verification:** New response-redaction and burst-limit regression tests pass, along with existing notification tests. Full results are listed under Verification below.
- **Remaining risk:** The route still accepts unauthenticated event-trigger requests so existing intercom webhook setups continue to work. An attacker can cause repeat processing/notifications within the limits. Adding a secret now would change the device's configured webhook URL; the repository does not provide a device-side secret provisioning flow. Introduce that flow before enforcing authentication.

### Finding 4 — Face uploads lack size and content validation

- **Severity:** Medium
- **Status:** Confirmed and remediated
- **Affected locations:** `custom_components/akuvox_ac/http.py:7525-7604,7645-7662`
- **Explanation:** The upload handler reads multipart file bytes without an explicit file-size check, image MIME/extension validation, or decoding/format verification, then writes the bytes with a `.jpg` extension. Authorization and profile scoping exist, but they do not validate uploaded content.
- **Exploitation scenario:** An authorized dashboard or self-service user can upload a very large or non-image payload. This can consume disk space, leave malformed face assets, or cause downstream device image processing failures.
- **Recommended remediation:** Enforce a conservative byte limit before reading/writing; decode and re-encode only supported image formats; reject malformed images and non-image payloads; preserve existing profile scoping.
- **Required regression tests:** Reject oversized payloads, spoofed MIME types, unsupported extensions, malformed image data and non-image bytes; accept valid supported images; confirm self-service users remain restricted to their own profile.
- **Remediation:** Implemented. The handler caps the request/file at 5 MiB, checks image dimensions/pixel count, decodes JPEG/PNG/WebP content with Pillow, and stores a normalized JPEG. It does not trust upload MIME type or filename extension.
- **Verification:** Upload tests reject oversized data and malformed JPEG-spoofed content, accept a valid PNG even with a text filename/MIME, and confirm the stored data decodes as JPEG. The existing self-service authorization path remains covered by the broader suite.
- **Dependency:** Added Pillow to the integration runtime requirements and development requirements; tests were run with Pillow 13.0.0 in the local virtual environment.
- **Existing functionality impact:** Valid JPEG, PNG and WebP uploads continue to work; stored assets are normalized to JPEG. Animated or other unsupported formats are rejected.

### Finding 5 — ANPR audit records retain vehicle registrations indefinitely

- **Severity:** Medium
- **Status:** Confirmed and remediated
- **Affected locations:** `custom_components/akuvox_ac/hikvision.py:39-51,278-309`
- **Explanation:** The audit store persists full plate values in successful and failed registration-change events. It retains the most recent 2,000 events, but has no age-based expiry or redaction. Vehicle registrations can be personal data, and failed requests also preserve submitted values.
- **Exploitation scenario:** A user with access to Home Assistant's integration storage or backups can retrieve a long history of vehicle registrations and associate them with actions/users. Storage backups may retain the same values beyond the intended operational period.
- **Recommended remediation:** Define a time-based retention policy, minimize or redact plate values in audit events, and document the purpose and access controls for retained records.
- **Required regression tests:** Stored audit records omit or minimize full plate values as designed; records older than the retention window are removed; success/failure audit event behavior and useful non-sensitive fields remain intact.
- **Remediation:** Implemented. Audit events retain only a masked plate suffix and records older than 90 days are removed. Existing records are sanitized when a new action is recorded or an administrator reads the camera list; the sanitized state is persisted.
- **Verification:** New tests confirm masking of new/old plate values and pruning beyond the retention window. ANPR regression suite: **9 passed**.

### Finding 6 — Hikvision ANPR management initially defaulted to unauthenticated transport security

- **Severity:** High
- **Status:** Confirmed during feature review and remediated before publication
- **Affected locations:** `custom_components/akuvox_ac/hikvision.py:138-169`; `custom_components/akuvox_ac/config_flow.py:54-75`
- **Explanation:** The initial ANPR client and configuration flow defaulted to HTTP on port 80. Digest authentication does not encrypt HTTP traffic or authenticate its body, so camera requests and responses could be observed or modified in transit.
- **Exploitation scenario:** A network-adjacent attacker could alter a plaintext license-plate list request or response, changing which vehicles are allowed or blocked, and could capture a Digest challenge/response for offline password guessing.
- **Recommended remediation:** Default to HTTPS with certificate verification and reject HTTP unless an administrator explicitly opts into insecure transport after understanding the network risk.
- **Required regression tests:** Verify the default URL uses HTTPS on port 443 with certificate verification; HTTP configuration is rejected without explicit opt-in and remains available with the opt-in for legacy cameras.
- **Remediation:** Implemented before publication. The client and config flow now default to verified HTTPS on port 443. The client rejects HTTP unless `allow_insecure_http` is explicitly enabled. README documents the risk of that compatibility option.
- **Verification:** `test_hikvision_isapi.py`: **8 passed**, including secure-default and explicit insecure-HTTP opt-in coverage. The full suite passes (**466 passed**).
- **Existing functionality impact:** Cameras that only support HTTP remain configurable by setting HTTP and explicitly enabling `allow_insecure_http`; new camera entries use HTTPS by default.

## Other security areas reviewed

- **SQL injection:** No SQL database, query construction or ORM usage was found.
- **Command injection:** No subprocess or shell execution path was found in application code.
- **XSS:** Inspected dynamic dashboard templates use escaping helpers or `textContent` for user/device-controlled text; no confirmed exploitable XSS was established in this static review. Many inline scripts are present, and a full browser-based review was not performed.
- **CSRF:** Dashboard session tokens are explicitly passed in a custom header or query parameter rather than an ambient cookie, reducing classic cookie-based CSRF exposure. No confirmed CSRF bypass was established in the reviewed routes.
- **Authorization:** Reviewed dashboard, settings, diagnostics, support-bundle, reservation, upload, remote-enrol, guest-link administration and Hikvision administration handlers contain explicit dashboard/admin checks. The public face route and inbound-call webhook are the exceptions reported above.
- **Path traversal:** Face and static-asset filesystem paths are resolved and checked against their expected roots. No confirmed traversal bypass was found in reviewed code.
- **Guest capability links:** Tokens are generated with `secrets`, stored as SHA-256 hashes, compared with `compare_digest`, and actions are constrained by an allowlist, entity-domain match, schedule and expiry checks. No confirmed guest-link authorization bypass was found.
- **Hikvision transport:** The client now defaults to HTTPS on port 443 with certificate verification, disables redirects, and rejects HTTP unless the administrator explicitly enables `allow_insecure_http`. Enabling HTTP permits on-path request/response tampering and should be limited to isolated trusted networks. Regression coverage verifies secure defaults and explicit opt-in behavior.
- **Secrets:** No confirmed hardcoded credential was identified by the limited source-pattern inspection. This is not proof that all credentials are absent from all Git history. A complete blob-by-blob secret history scan was not performed because no suitable secret-scanning tool/runtime was available in the review environment; rotate any credential later found in history.
- **Dependency risks:** No lockfile or runtime dependency list for the integration was found. Vulnerability advisories were not queried, so dependency currency and transitive dependency vulnerabilities remain unverified.
- **Logging and security headers:** The support-bundle code includes sensitive-key redaction. A full review of every log field, upstream Home Assistant header behavior, proxy configuration, CORS policy, cookies and deployed security headers was not possible from this repository alone.

## Phase 3 — Prioritized remediation plan

1. **Completed — High:** Akuvox transport verifies TLS by default and avoids credentialed HTTP unless explicitly enabled.
2. **Completed — High:** Face-image retrieval uses dashboard authorization or a scoped per-profile token while preserving intercom access.
3. **Partially completed — Medium:** Public call-data response and unbounded requests are fixed. Add a device-configurable webhook secret before requiring authentication; unauthenticated trigger abuse remains possible within rate limits.
4. **Completed — Medium:** Face uploads are size-limited, decoded and normalized.
5. **Completed — Medium:** ANPR plate values are masked in audit records and retention is limited to 90 days.
6. **Follow-up review:** Run a complete Git-history secret scan, dependency advisory scan, and deployed header/proxy configuration review in an environment with the required tools and access.

## Remediation verification

- Targeted security and compatibility suites: **68 passed** before the final test additions.
- Hikvision transport regression suite: **8 passed**, including the new secure-default and explicit HTTP opt-in cases.
- Full project suite after all code changes: **466 passed**.
- Ruff on changed application and regression-test files: passed.
- `manifest.json` JSON validation: passed.
- TDD checkpoints were committed for Akuvox transport, face asset access, webhook disclosure, upload validation, ANPR retention and Hikvision transport defaults. Fixes were committed in logical groups where possible.

## Limitations

The initial audit was static and read-only. During the approved remediation pass, relevant unit/integration tests and Ruff were run; the application was not deployed or tested against live devices. No production systems, credentials or stored data were accessed. A dependency advisory database query was not performed. Runtime Home Assistant/reverse-proxy behavior, Nabu Casa exposure, device firmware compatibility, deployed TLS/proxy settings, and actual route reachability still require environment-specific verification. The webhook authentication gap remains as described in Finding 3.
