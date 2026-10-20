from __future__ import annotations

import importlib.util
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def _load():
    path = ROOT / "scripts/check_doc_commands.py"
    spec = importlib.util.spec_from_file_location("check_doc_commands", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


CHECK = _load()


def test_no_retired_command_forms_in_active_docs() -> None:
    result = subprocess.run(
        [sys.executable, "scripts/check_doc_commands.py"],
        cwd=ROOT,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stdout + result.stderr


def test_retired_form_denylist_covers_the_known_rot() -> None:
    patterns = {rx for rx, _ in CHECK.RETIRED_FORMS}
    # The exact retired forms the audit found must stay guarded.
    assert any("format" in p for p in patterns)
    assert any("--tail" in p for p in patterns)
    assert any("source-sync" in p for p in patterns)
    assert any("reconcile" in p for p in patterns)
    assert any("plan-out" in p for p in patterns)


def test_history_trees_are_out_of_scope() -> None:
    scoped = {p.relative_to(ROOT).as_posix() for p in CHECK._active_doc_paths()}
    assert not any(s.startswith("docs/archive/") for s in scoped)
    assert not any(s.startswith("docs/reports/") for s in scoped)
    assert "docs/reference/error_codes.yaml" in scoped
