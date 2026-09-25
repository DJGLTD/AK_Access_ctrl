"""Backward compatible import for the Akuvox user store.

The integration historically exposed :class:`AkuvoxUsersStore` from this module.
The implementation now lives alongside the rest of the runtime in ``integration.py``
so we simply re-export it here to keep dotted-imports working and to avoid stale
inline documentation referencing ``relays`` instead of schedule/key-holder data.
"""

from __future__ import annotations

from .integration import AkuvoxUsersStore  # noqa: F401
