from pathlib import Path


def test_mobile_dashboard_includes_desktop_event_filter_behavior():
    www = Path(__file__).resolve().parents[1] / "www"
    desktop = (www / "index.html").read_text(encoding="utf-8")
    mobile = (www / "index-mob.html").read_text(encoding="utf-8")

    for html in (desktop, mobile):
        assert 'id="eventFilter"' in html
        assert 'id="eventUserFilter"' in html
        assert '<option value="all">All events</option>' in html
        assert '<option value="system">System events</option>' in html
        assert '<option value="access">Access events</option>' in html
        assert "let eventFilterMode = 'access';" in html
        assert "let eventUserFilterValue = '';" in html
        assert "function applyEventFilter()" in html
        assert "function populateEventUserFilter(" in html
        assert "function buildUserMatchIndex(" in html
        assert "function matchesEventUserFilter(" in html
        assert "state?.event_access" in html
        assert "eventFeedUserMatchIndex = buildUserMatchIndex(devs, lookupUsers)" in html
        assert "evt._category === 'access' || evt._category === 'call'" in html
        assert "applyEventFilter();" in html

    assert ".event-filter{" in mobile
    assert ".event-user-filter{" in mobile
    assert "eventFilter.addEventListener('change'" in mobile
    assert "eventUserFilter.addEventListener('change'" in mobile
