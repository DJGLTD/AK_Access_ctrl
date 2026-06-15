from __future__ import annotations
from homeassistant.const import Platform

DOMAIN = "akuvox_ac"

INTEGRATION_VERSION = "3.12.19"
INTEGRATION_VERSION_LABEL = "3.12.19"

# Bump when you change stored config structure
ENTRY_VERSION = 3

PLATFORMS = [
    Platform.SENSOR,
    Platform.BUTTON,         # OK because we now ship a stub
    Platform.BINARY_SENSOR,  # OK because we now ship a stub
]

# Storage keys
GROUPS_STORAGE_KEY = f"{DOMAIN}_groups.json"
USERS_STORAGE_KEY  = f"{DOMAIN}_users.json"

# Config keys
CONF_DEVICE_NAME   = "device_name"
CONF_DEVICE_TYPE   = "device_type"   # "Intercom" | "Keypad"
CONF_DEVICE_MODEL  = "device_model"
CONF_HOST          = "host"
CONF_PORT          = "port"
CONF_USERNAME      = "username"
CONF_PASSWORD      = "password"

DEFAULT_DEVICE_MODEL = "Other Akuvox"
AKUVOX_DEVICE_MODELS = (
    DEFAULT_DEVICE_MODEL,
    "S539",
    "S538",
    "S535",
    "S532",
    "X916",
    "X915",
    "X912",
    "X910",
    "R29",
    "R28",
    "R27",
    "R25",
    "R20",
    "E21",
    "E20",
    "E18",
    "E16",
    "E13",
    "E12",
    "A095",
    "A094",
    "A092",
    "A08",
    "A05",
    "A03",
    "A02",
    "A01",
)

# Options
CONF_PARTICIPATE   = "participate_in_sync"
CONF_POLL_INTERVAL = "poll_interval"
CONF_DEVICE_GROUPS = "device_groups"
CONF_RELAY_ROLES  = "relay_roles"
CONF_AUTO_REBOOT = "auto_reboot"

# Relay roles
RELAY_ROLE_NONE       = "none"
RELAY_ROLE_DOOR       = "door"
RELAY_ROLE_PEDESTRIAN = "pedestrian"
RELAY_ROLE_ALARM      = "alarm"
RELAY_ROLE_DOOR_ALARM = "door_alarm"

# Defaults
DEFAULT_USE_HTTPS     = True
DEFAULT_VERIFY_SSL    = False
DEFAULT_POLL_INTERVAL = 30  # seconds
DEFAULT_DIAGNOSTICS_HISTORY_LIMIT = 50
MIN_DIAGNOSTICS_HISTORY_LIMIT = 10
MAX_DIAGNOSTICS_HISTORY_LIMIT = 200
MIN_HEALTH_CHECK_INTERVAL = 10   # seconds
MAX_HEALTH_CHECK_INTERVAL = 300  # seconds
DEFAULT_ACCESS_HISTORY_LIMIT = 30
MIN_ACCESS_HISTORY_LIMIT = 5
MAX_ACCESS_HISTORY_LIMIT = 200

HA_CONTACT_GROUP_NAME = "HA-Group"

EVENT_NON_KEY_ACCESS_GRANTED = "akuvox_non_key_access_granted"
EVENT_INBOUND_CALL = "akuvox_inbound_call"

INBOUND_CALL_RESULT_APPROVED_KEY_HOLDER = "approved_key_holder"
INBOUND_CALL_RESULT_APPROVED = "approved"
INBOUND_CALL_RESULT_DENIED = "denied"

ADMIN_DASHBOARD_URL_PATH = "akuvox-access-control"
ADMIN_DASHBOARD_TITLE = "Akuvox Access Control"
ADMIN_DASHBOARD_ICON = "mdi:door-closed-lock"
