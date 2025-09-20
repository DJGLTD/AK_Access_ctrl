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

EVENT_NON_KEY_ACCESS_GRANTED = "akuvox_non_key_access_granted"
