from pathlib import Path


def test_dashboard_templates_show_cloud_and_local_user_sources():
    www = Path(__file__).resolve().parents[1] / "www"

    for filename in ("index.html", "index-mob.html"):
        html = (www / filename).read_text(encoding="utf-8")

        assert "<th>Source</th>" in html
        assert "function isCloudManagedUser(record)" in html
        assert "record?.SourceType" in html
        assert "function userSourceBadge(user)" in html
        assert "const cloud = isCloudManagedUser(user)" in html
        assert "const label = cloud ? 'Cloud' : 'Local'" in html
        assert "class=\"badge user-source-badge ${kind}\"" in html
        assert ".badge-cloud{" in html
        assert ".badge-local{" in html
        assert "isCloudManagedUser(user) ? 'cloud' : 'local'" in html
        assert "const isCloud = isCloudManagedUser(u)" in html
        assert "userSourceBadge(u)}</td>" in html

    desktop = (www / "index.html").read_text(encoding="utf-8")
    mobile = (www / "index-mob.html").read_text(encoding="utf-8")
    overview = (www / "user_overview-mob.html").read_text(encoding="utf-8")

    assert "const columnCount = showPinColumn ? 8 : 7;" in desktop
    assert '<td colspan="7" class="text-muted">Loading' in desktop
    assert '<td colspan="8" class="text-muted">Loading' in mobile
    assert "colspan=\"8\" class=\"text-muted\">No users" in mobile
    assert "function isCloudManagedUser(record)" in overview
    assert "record?.SourceType" in overview
    assert "const isCloud = isCloudManagedUser(u)" in overview
