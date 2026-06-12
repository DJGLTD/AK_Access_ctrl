from pathlib import Path


def test_mobile_diagnostics_tabs_use_single_scrollable_row():
    www = Path(__file__).resolve().parents[1] / "www"
    mobile = (www / "diagnostics-mob.html").read_text(encoding="utf-8")

    assert 'class="nav nav-tabs diag-tabs"' in mobile
    assert "flex-wrap:nowrap;" in mobile
    assert "overflow-x:auto;" in mobile
    assert "-webkit-overflow-scrolling:touch;" in mobile
    assert "flex:0 0 auto;" in mobile
    assert "white-space:nowrap;" in mobile
