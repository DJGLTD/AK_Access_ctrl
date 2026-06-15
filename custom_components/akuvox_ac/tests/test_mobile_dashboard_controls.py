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
    assert 'class="mobile-launcher-title">Access Control<' in shell
    assert '<button class="mobile-tile" type="button" data-view="index">' not in shell
    for view in (
        "user-overview",
        "users",
        "temp-user",
        "event-history",
        "device-management",
        "settings",
    ):
        assert f'data-view="{view}"' in shell
    assert "body.mobile-stage-nav .mobile-launcher { display: grid; }" in shell
    assert "body.mobile-stage-content header.app-header { display: flex; }" in shell
    assert "header.app-header .logo-mark { display: none; }" in shell
    assert ".mobile-group {" in shell
    assert "display: contents;" in shell
    assert '.mobile-group[data-group="users"] [data-group-toggle="users"] { order: 10; }' in shell
    assert '.mobile-launcher [data-view="event-history"] { order: 20; }' in shell
    assert '.mobile-group[data-group="users"] .mobile-subgrid { order: 25; }' in shell
    assert '.mobile-launcher [data-view="device-management"] { order: 30; }' in shell
    assert '.mobile-group[data-group="global"] [data-group-toggle="global"] { order: 40; }' in shell
    assert '.mobile-group[data-group="global"] .mobile-subgrid { order: 45; }' in shell
    assert '.mobile-launcher [data-view="settings"] { order: 50; }' in shell
    assert 'class="mobile-subgrid-heading"' in shell
    assert "const APP_HISTORY_INDEX_KEY = 'akuvoxHistoryIndex';" in shell
    assert "const APP_HISTORY_SESSION_KEY = 'akuvoxHistorySession';" in shell
    assert "const APP_HISTORY_SESSION_ID = (() => {" in shell
    assert "function getAppHistoryIndex(state = history.state)" in shell
    assert "state[APP_HISTORY_SESSION_KEY] !== APP_HISTORY_SESSION_ID" in shell
    assert "const nextIndex = replaceState ? currentIndex : currentIndex + 1;" in shell
    assert "[APP_HISTORY_SESSION_KEY]: APP_HISTORY_SESSION_ID" in shell
    assert "mobileMenuEntry: isMobileMode && mobileMenuEntry" in shell
    assert "mobileMenuGroup: isMobileMode ? mobileMenuGroup : null" in shell
    assert "updateHistory(initialView, params, { replaceState: true });\n  await ensureDashboardSignedPaths();" in shell
    assert "let mobileViewStack = [];" in shell
    assert "function rememberMobileDestination(view, params = {}, options = {})" in shell
    assert "if (mobileViewStack.length > 1)" in shell
    assert "mobileViewStack.pop();" in shell
    assert "loadView(previous.view, previous.params, {" in shell
    assert "function returnToMobileMenu(group)" in shell
    assert "mobileViewStack = [];" in shell
    assert "function setMobileStage(stage, { preserveGroups = false, syncHistory = true } = {})" in shell
    assert "setMobileStage('content', { preserveGroups: true, syncHistory: false });" in shell
    assert "replaceState: true,\n        mobileMenuEntry: true," in shell
    assert "const mobileMenuBtn = document.getElementById('mobileMenuBtn');" in shell
    assert "mobileStage = explicitDestination ? 'content' : 'nav';" in shell

    for name in ("device_edit-mob.html", "diagnostics-mob.html", "schedules-mob.html"):
        page = read_page(name)
        assert 'id="btnBack"' not in page
        assert 'id="backBtn"' not in page


def test_mobile_global_actions_include_update_check():
    shell = read_page("head-mob.html")

    assert shell.count('data-action="hacs_update_check"') == 2
    assert (
        '<button type="button" class="quick-btn" data-action="hacs_update_check">'
        in shell
    )
    assert (
        '<button class="mobile-tile sub" type="button" data-action="hacs_update_check">'
        in shell
    )
    assert '<span class="tile-title">System update</span>' in shell
    assert '<span>Check for updates</span>' in shell
    assert "steps.push(() => postJson(API_ACTION, { action: 'hacs_update_check' }));" in shell
    assert "steps.push(() => callService('akuvox_ac', 'hacs_update_check', {}));" in shell


def test_mobile_global_actions_include_access_event_refresh():
    shell = read_page("head-mob.html")

    assert 'data-action="refresh_events"' in shell
    assert '<span class="tile-title">Update access events</span>' in shell
    assert "steps.push(() => postJson(API_ACTION, { action: 'refresh_events' }));" in shell
    assert "steps.push(() => callService('akuvox_ac', 'refresh_events', {}));" in shell
    assert 'id="mobileActionStatus"' in shell
    assert "setMobileActionStatus(`Request sent: ${describeAction(normalized)}.`, 'success');" in shell
    assert "loadView('index', { section: 'global-actions' });" not in shell
    assert "runDashboardAction(action);\n    if (isMobileMode)" not in shell


def test_mobile_user_overview_only_presents_the_user_list():
    overview = read_page("user_overview-mob.html")

    assert "<h1>Current users</h1>" in overview
    assert 'id="userSearch"' in overview
    assert 'id="userSort"' in overview
    assert 'id="userList"' in overview
    for removed_id in (
        "summaryUsers",
        "summaryPending",
        "summaryFaces",
        "lastUpdated",
        "btnMobileAddUser",
        "btnMobileAddTempUser",
        "forceFullSyncBtn",
        "syncNowBtn",
    ):
        assert f'id="{removed_id}"' not in overview


def test_dashboards_start_only_one_state_polling_loop():
    for name in ("index.html", "index-mob.html"):
        page = read_page(name)
        assert page.count("setInterval(refresh, 5000);") == 1
        assert page.count("// initial + poll") == 1


def test_cloud_open_events_use_akuvox_app_label():
    for name in (
        "index.html",
        "index-mob.html",
        "event_history.html",
        "event_history-mob.html",
    ):
        page = read_page(name)
        assert "Opened with Akuvox App" in page
        assert "function isCloudEvent(" in page
        assert " - Cloud - " not in page
