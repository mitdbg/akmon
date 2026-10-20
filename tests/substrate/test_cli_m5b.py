"""M5b — CLI surface tests (init, library, trace)."""
from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest


# The CLI runs in this interpreter (`uv run` first checks and may sync the
# whole environment, too slow for a 30 s bound on a loaded runner); importing
# it is heavy, so a stuck process still fails well inside the job.
_CLI_TIMEOUT_SECONDS = 120


# --- init ---

def test_cli_init_scaffolds_empty_template(tmp_path: Path) -> None:
    out = subprocess.run(
        [sys.executable, "-m", "okg.substrate.cli.main",
         "--json", "init",
         "--template", "empty",
         "--target", str(tmp_path / "work")],
        capture_output=True, text=True, timeout=_CLI_TIMEOUT_SECONDS,
    )
    assert out.returncode == 0, out.stderr
    payload = json.loads(out.stdout)
    assert payload["template"] == "empty"
    scaffolded = Path(payload["target"])
    assert (scaffolded / "deployment.yaml").is_file()
    assert (scaffolded / "source_registry.yaml").is_file()


def test_cli_init_unknown_template(tmp_path: Path) -> None:
    out = subprocess.run(
        [sys.executable, "-m", "okg.substrate.cli.main",
         "--json", "init",
         "--template", "does_not_exist",
         "--target", str(tmp_path / "x")],
        capture_output=True, text=True, timeout=_CLI_TIMEOUT_SECONDS,
    )
    assert out.returncode != 0
    err = json.loads(out.stderr)
    assert err["error"]["code"] == "library_ref_missing"


def test_cli_init_refuses_nonempty_target(tmp_path: Path) -> None:
    target = tmp_path / "existing"
    (target / "devops").mkdir(parents=True)
    (target / "devops" / "garbage").write_text("x")
    out = subprocess.run(
        [sys.executable, "-m", "okg.substrate.cli.main",
         "--json", "init",
         "--template", "devops",
         "--target", str(target)],
        capture_output=True, text=True, timeout=_CLI_TIMEOUT_SECONDS,
    )
    assert out.returncode != 0
    err = json.loads(out.stderr)
    assert "not empty" in err["error"]["message"].lower()


# --- library ---

def test_cli_library_list_all_kinds() -> None:
    out = subprocess.run(
        [sys.executable, "-m", "okg.substrate.cli.main",
         "--json", "library", "list"],
        capture_output=True, text=True, timeout=_CLI_TIMEOUT_SECONDS,
    )
    assert out.returncode == 0, out.stderr
    payload = json.loads(out.stdout)
    # Per `unify-ontology-systems` (2026-04-26): the legacy
    # `ontology` / `narrowings` kinds are gone; the remaining
    # kinds still surface.
    assert "invariants" in payload
    assert "github" in payload["modules"]
    assert "devops" in payload["templates"]
    assert "ontology" not in payload
    assert "narrowings" not in payload


def test_cli_library_list_single_kind() -> None:
    out = subprocess.run(
        [sys.executable, "-m", "okg.substrate.cli.main",
         "--json", "library", "list", "--kind", "invariants"],
        capture_output=True, text=True, timeout=_CLI_TIMEOUT_SECONDS,
    )
    assert out.returncode == 0
    payload = json.loads(out.stdout)
    assert "invariants" in payload
    assert "no_orphan_edges" in payload["invariants"]


def test_cli_library_show_yaml_ref() -> None:
    out = subprocess.run(
        [sys.executable, "-m", "okg.substrate.cli.main",
         "--json", "library", "show", "okg.invariants.no_orphan_edges"],
        capture_output=True, text=True, timeout=_CLI_TIMEOUT_SECONDS,
    )
    assert out.returncode == 0
    payload = json.loads(out.stdout)
    assert payload["ref"] == "okg.invariants.no_orphan_edges"
    assert "no_orphan_edges" in payload["content"]


# --- trace ---

@pytest.mark.postgres
def test_cli_trace_node_empty(clean_runtime: str) -> None:
    """Trace a node_id with no facts — returns an empty fact log, not error.

    The retired live-reading plane (issue #83) once added a ``live`` key; that
    plane was abandoned, so the trace payload is now ``facts`` plus an optional
    ``graph_node`` preview. An absent node yields an empty ``facts`` list.
    """
    env = os.environ.copy()
    env["OKG_DSN"] = clean_runtime
    out = subprocess.run(
        [sys.executable, "-m", "okg.substrate.cli.main",
         "--json", "trace", "node", "n:absent"],
        capture_output=True, text=True, timeout=_CLI_TIMEOUT_SECONDS,
        env=env,
    )
    assert out.returncode == 0, out.stderr
    payload = json.loads(out.stdout)
    assert payload["node_id"] == "n:absent"
    assert payload["facts"] == []
