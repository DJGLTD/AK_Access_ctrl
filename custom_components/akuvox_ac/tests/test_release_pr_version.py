import shutil
import subprocess
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[3]
SCRIPT = ROOT / "scripts" / "release-pr-version.cjs"


@pytest.mark.parametrize(
    ("pr_number", "version"),
    [
        ("416", "4.1.6"),
        ("500", "5.0.0"),
        ("525", "5.2.5"),
        ("555", "5.5.5"),
    ],
)
def test_release_version_matches_pull_request_number(pr_number, version):
    node = shutil.which("node")
    if not node:
        pytest.skip("node is required for release script validation")

    result = subprocess.run(
        [node, str(SCRIPT), "--version-from-pr", pr_number],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )

    assert result.stdout.strip() == version


def test_release_script_uses_pull_request_number_as_source_of_truth():
    source = SCRIPT.read_text(encoding="utf-8")

    assert "const version = versionFromPrNumber(associatedPr.number);" in source
    assert "nextVersion(" not in source
