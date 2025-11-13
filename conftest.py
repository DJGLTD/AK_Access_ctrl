"""Pytest configuration ensuring Home Assistant stubs are installed early."""

from pathlib import Path
import sys


PACKAGE_ROOT = Path(__file__).resolve().parent / "custom_components" / "AK_Access_ctrl"
if str(PACKAGE_ROOT) not in sys.path:
    sys.path.insert(0, str(PACKAGE_ROOT))

from ha_test_stubs import ensure_homeassistant_stubs

ensure_homeassistant_stubs()
