"""The two residual `stderr or stdout` swallow sites route through
``command_output_tail`` so a logged warning can no longer hide the CLI's
stdout failure report.

Adversarial review of PR "init: stage-failure messages carry both streams"
found the same bug class surviving at:
  * ``okg.bench.per_source.single_commit_breakdown.run_okg`` / ``bootstrap_for``
    (``res.stderr or res.stdout`` around ``okg ... --json`` invocations), and
  * ``academic-researcher/cli/install.py`` (``e.stderr or e.stdout`` wrapping
    ``okg init``).
Both now delegate to the shared helper. The helper's own correctness is
covered by ``test_init_publish_proof.test_stage_failure_output_carries_both_streams``;
here we prove (a) it handles a ``CalledProcessError`` — the install site's
input type — and (b) both sites are wired to it with the swallow removed.
"""
from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from okg.substrate.cli._output import command_output_tail

_REPO_ROOT = Path(__file__).resolve().parents[3]
_BENCH = (
    _REPO_ROOT / "src" / "okg" / "bench" / "per_source"
    / "single_commit_breakdown.py"
)
_INSTALL = (
    _REPO_ROOT / "src" / "okg" / "substrate" / "library" / "profiles"
    / "academic-researcher" / "cli" / "install.py"
)


def test_command_output_tail_handles_called_process_error() -> None:
    """The install site passes a CalledProcessError; its .stdout report must
    survive alongside a .stderr warning."""
    err = subprocess.CalledProcessError(
        returncode=1,
        cmd=["okg", "init"],
        output='{"failed_stage": "ingest", "detail": "REAL_CAUSE"}',
        stderr="WARNING openspec_root not found: /tmp/x\n",
    )
    message = command_output_tail(err)
    assert "REAL_CAUSE" in message, "stdout report must survive"
    assert "openspec_root not found" in message, "stderr must survive"
    assert message.index("stdout") < message.index("stderr")


def test_bench_run_okg_surfaces_both_streams(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A non-zero `okg ... --json` makes run_okg raise with BOTH streams —
    the stdout report is no longer dropped when stderr carries a warning."""
    mod = pytest.importorskip(
        "okg.bench.per_source.single_commit_breakdown"
    )

    def _fake_run(*_a, **_k):
        return subprocess.CompletedProcess(
            args=["okg", "migrate"],
            returncode=1,
            stdout='{"error": "SCHEMA_DRIFT_REPORT"}',
            stderr="WARNING deployment declares no sync_root\n",
        )

    monkeypatch.setattr(mod.subprocess, "run", _fake_run)
    with pytest.raises(RuntimeError) as excinfo:
        mod.run_okg(["migrate"], dsn_url="postgresql://x/y", dep=Path("depX"))
    msg = str(excinfo.value)
    assert "SCHEMA_DRIFT_REPORT" in msg, "stdout report must reach the error"
    assert "sync_root" in msg, "stderr must reach the error too"


@pytest.mark.parametrize("path", [_BENCH, _INSTALL], ids=["bench", "install"])
def test_swallow_pattern_absent_and_helper_wired(path: Path) -> None:
    """Regression guard: neither fixed site may re-introduce a
    `.stderr or ...stdout` swallow, and both route through the shared helper."""
    text = path.read_text(encoding="utf-8")
    assert "command_output_tail" in text, (
        f"{path.name} must route subprocess failures through the shared helper"
    )
    for swallow in (".stderr or ", ".stderr.strip() or "):
        assert swallow not in text, (
            f"{path.name} re-introduced a stream-swallowing "
            f"{swallow!r} composition"
        )
