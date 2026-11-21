import re
import subprocess
import tempfile
from pathlib import Path
from types import SimpleNamespace

import pytest

from custom_components.akuvox_ac.guest_links import (
    GuestLinkAdminPageView,
    GuestLinkPageView,
)
from custom_components.akuvox_ac.hikvision import HikvisionAdminPageView


async def _scripts(response):
    return re.findall(r"<script>(.*?)</script>", response.kwargs["text"], flags=re.DOTALL)


@pytest.mark.asyncio
async def test_new_admin_and_guest_page_scripts_parse_in_node():
    request = SimpleNamespace(app={})
    pages = [
        await GuestLinkAdminPageView().get(request),
        await GuestLinkPageView().get(request),
        await HikvisionAdminPageView().get(request),
    ]
    scripts = [script for page in pages for script in await _scripts(page)]
    assert len(scripts) == 3
    for script in scripts:
        with tempfile.NamedTemporaryFile("w", suffix=".js", encoding="utf-8", delete=False) as file:
            file.write(script)
            path = Path(file.name)
        try:
            result = subprocess.run(
                ["node", "--check", str(path)],
                capture_output=True,
                text=True,
                check=False,
            )
            assert result.returncode == 0, result.stderr
        finally:
            path.unlink(missing_ok=True)


@pytest.mark.asyncio
async def test_guest_and_anpr_pages_avoid_external_dependencies_and_disable_caching():
    request = SimpleNamespace(app={})
    pages = [
        await GuestLinkAdminPageView().get(request),
        await GuestLinkPageView().get(request),
        await HikvisionAdminPageView().get(request),
    ]
    for page in pages:
        headers = page.kwargs["headers"]
        assert headers["Cache-Control"] == "no-store"
        assert headers["Referrer-Policy"] == "no-referrer"
        assert "connect-src 'self'" in headers["Content-Security-Policy"]
        assert "https://" not in page.kwargs["text"]
    assert "location.hash" in pages[1].kwargs["text"]
    assert "/api/akuvox_ac/guest" in pages[1].kwargs["text"]
