"""The event history pages provide a date filter for the loaded events."""

from pathlib import Path


WWW = Path(__file__).parents[1] / "www"


def test_desktop_and_mobile_history_pages_filter_events_by_selected_calendar_date():
    for page_name in ("event_history.html", "event_history-mob.html"):
        page = (WWW / page_name).read_text(encoding="utf-8")

        assert 'id="eventDateFilter"' in page
        assert 'id="clearDateFilter"' in page
        assert "await callService('akuvox_ac', 'refresh_events', {});" in page
        assert "function applyDateFilter(events)" in page
        assert "const filtered = applyDateFilter(applyCategoryFilter(userEvents));" in page
        assert "dateInput.addEventListener('change', renderFilteredEvents)" in page
