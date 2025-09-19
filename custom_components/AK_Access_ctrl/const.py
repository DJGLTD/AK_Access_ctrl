from __future__ import annotations
from homeassistant.const import Platform

DOMAIN = "akuvox_ac"

# Bump when you change stored config structure
ENTRY_VERSION = 3

PLATFORMS = [
    Platform.SENSOR,
    Platform.BUTTON,         # OK because we now ship a stub
    Platform.BINARY_SENSOR,  # OK because we now ship a stub
    Platform.UPDATE,         # OK because we now ship a stub
]

# Storage keys
GROUPS_STORAGE_KEY = f"{DOMAIN}_groups.json"
USERS_STORAGE_KEY  = f"{DOMAIN}_users.json"

# Config keys
CONF_DEVICE_NAME   = "device_name"
CONF_DEVICE_TYPE   = "device_type"   # "Intercom" | "Keypad"
CONF_HOST          = "host"
CONF_PORT          = "port"
CONF_USERNAME      = "username"
CONF_PASSWORD      = "password"

# Options
CONF_PARTICIPATE   = "participate_in_sync"
CONF_POLL_INTERVAL = "poll_interval"
CONF_DEVICE_GROUPS = "device_groups"

# Defaults
DEFAULT_USE_HTTPS     = False
DEFAULT_VERIFY_SSL    = False
DEFAULT_POLL_INTERVAL = 30  # seconds

# Events
EVENT_NON_KEY_ACCESS_GRANTED = "akuvox_ac_non_key_access_granted"

# Webhook events exposed for Akuvox action URLs / automation bridging
EVENT_WEBHOOK_PREFIX = "akuvox_ac_webhook_"

WEBHOOK_EVENT_SPECS = (
    {
        "key": "granted",
        "name": "User granted access",
        "description": "Triggered when an Akuvox action URL reports that a user was granted access.",
    },
    {
        "key": "denied_no_access",
        "name": "User denied access (no access set)",
        "description": "Triggered when the device reports an access denial because no access permissions were configured.",
    },
    {
        "key": "denied_outside_time",
        "name": "User denied access (outside schedule)",
        "description": "Triggered when the device reports an access denial due to the user being outside their permitted time window.",
    },
    {
        "key": "device_offline",
        "name": "Device offline",
        "description": "Use for delayed offline alerts from the device itself (5 minute grace period still applies in Home Assistant).",
    },
)

