from pathlib import Path


WWW = Path(__file__).resolve().parents[1] / "www"


def read_page(name):
    return (WWW / name).read_text(encoding="utf-8")


def test_standalone_mobile_event_history_has_category_filter():
    mobile = read_page("event_history-mob.html")

    assert 'id="eventFilter"' in mobile
    assert '<option value="all">All events</option>' in mobile
    assert '<option value="system">System events</option>' in mobile
    assert '<option value="access">Access events</option>' in mobile
    assert "let eventFilterMode = 'access';" in mobile
    assert "function applyCategoryFilter(events)" in mobile
    assert "evt._category === 'access' || evt._category === 'call'" in mobile
    assert "renderFilteredEvents();" in mobile


def test_mobile_user_sort_recovers_from_unknown_stored_value():
    overview = read_page("user_overview-mob.html")
    dashboard = read_page("index-mob.html")

    for mobile in (overview, dashboard):
        assert "function normalizeUserSort(value)" in mobile
        assert "Organise by last access" in mobile
        assert "Organise by user ID" in mobile
        assert "Organise by name" in mobile

    assert "return USER_SORT_VALUES.has(normalized) ? normalized : 'name';" in overview
    assert "return ['last_access', 'ha_id', 'name'].includes(normalized) ? normalized : 'ha_id';" in dashboard


def test_mobile_shell_owns_page_back_navigation_and_has_menu_button():
    shell = read_page("head-mob.html")

    assert 'id="mobileBackBtn"' in shell
    assert 'id="mobileMenuBtn"' in shell
    assert "const APP_HISTORY_INDEX_KEY = 'akuvoxHistoryIndex';" in shell
    assert "const APP_HISTORY_SESSION_KEY = 'akuvoxHistorySession';" in shell
    assert "const APP_HISTORY_SESSION_ID = (() => {" in shell
    assert "function getAppHistoryIndex(state = history.state)" in shell
    assert "state[APP_HISTORY_SESSION_KEY] !== APP_HISTORY_SESSION_ID" in shell
    assert "const nextIndex = replaceState ? currentIndex : currentIndex + 1;" in shell
    assert "[APP_HISTORY_SESSION_KEY]: APP_HISTORY_SESSION_ID" in shell
    assert "updateHistory(initialView, params, { replaceState: true });\n  await ensureDashboardSignedPaths();" in shell
    assert "if (getAppHistoryIndex() > 0)" in shell
    assert "history.back();" in shell
    assert "function setMobileStage(stage, { preserveGroups = false, syncHistory = true } = {})" in shell
    assert "setMobileStage('content', { syncHistory: false });" in shell
    assert "const mobileMenuBtn = document.getElementById('mobileMenuBtn');" in shell
    assert "setMobileStage('nav');" in shell

    for name in ("device_edit-mob.html", "diagnostics-mob.html", "schedules-mob.html"):
        page = read_page(name)
        assert 'id="btnBack"' not in page
        assert 'id="backBtn"' not in page


def test_mobile_global_actions_include_update_check():
    shell = read_page("head-mob.html")

    assert 'data-action="hacs_update_check"' in shell
    assert '<span class="tile-title">Check for updates</span>' in shell
    assert "steps.push(() => postJson(API_ACTION, { action: 'hacs_update_check' }));" in shell
    assert "steps.push(() => callService('akuvox_ac', 'hacs_update_check', {}));" in shell
