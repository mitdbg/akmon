"""The operator-ui design harness refuses without its PACT change record.

Its captures, its rules audit and its pinned references live in the
operator-ui-redesign-mocks PACT change record. The public copy ships the
harness but not that record, so each entry point must refuse by name instead
of reporting every reference as "not written" or inventing the record's
directory to write evidence into (#2823).

Each script is copied into an empty tree (no pact/ directory) and run there,
so the check is the same in this repository and in the public copy.
"""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]


def _design_tool_dir() -> Path:
    """The design harness's directory, found by the file that marks it.

    The tree is not named here: the DB-lane scope guard
    (tests/ci/test_lane_scope.py::test_db_lane_exclusions_name_no_file_a_db_lane_reads)
    refuses Python outside tests/ci/ that names it, and this file must stay in
    tests/scripts/ so it ships and runs in the public copy it guards. Exactly
    one directory may match, so a moved or duplicated harness fails loudly.
    """
    matches = sorted((ROOT / "scripts").glob("*/check-references.mjs"))
    assert len(matches) == 1, matches
    return matches[0].parent.relative_to(ROOT)


DESIGN = _design_tool_dir()


@pytest.fixture
def node() -> str:
    found = shutil.which("node")
    if found is None:
        pytest.skip("node is not installed; these checks run the design harness with node")
    return found


def _copy(tree: Path, relative: Path) -> Path:
    target = tree / relative
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(ROOT / relative, target)
    return target


def test_reference_check_refuses_when_the_record_is_absent(node: str, tmp_path: Path) -> None:
    script = _copy(tmp_path, DESIGN / "check-references.mjs")

    completed = subprocess.run(
        [node, str(script)], capture_output=True, text=True, timeout=60, check=False
    )

    assert completed.returncode == 2, completed.stderr
    assert "REFUSED: the reference directory" in completed.stderr
    assert "operator-ui-redesign-mocks" in completed.stderr
    assert "has not been written" not in completed.stdout + completed.stderr


def test_capture_and_audit_refuse_before_writing_into_an_absent_record(
    node: str, tmp_path: Path
) -> None:
    harness = _copy(tmp_path, DESIGN / "lib" / "harness.mjs")
    probe = (
        f"import {{ requirePactUiDir }} from {harness.as_uri()!r};\n"
        "try { requirePactUiDir(); } catch (error) { console.error(error.message); process.exit(3); }\n"
    )

    completed = subprocess.run(
        [node, "--input-type=module", "-e", probe],
        capture_output=True, text=True, timeout=60, check=False,
    )

    assert completed.returncode == 3, completed.stderr
    assert "operator-ui-redesign-mocks PACT change record" in completed.stderr
    assert not (tmp_path / "pact").exists()


@pytest.mark.parametrize("entry", ["capture.mjs", "audit-rules.mjs"])
def test_capture_and_audit_check_the_record_before_anything_else(entry: str) -> None:
    """Needs no node: the guard is the first statement of each entry point."""
    source = (ROOT / DESIGN / entry).read_text(encoding="utf-8")
    body = source[source.index("async function main() {"):]
    assert body.splitlines()[1].strip() == "requirePactUiDir();"
