"""Smoke tests for the 4 deployment MCP entry points (M6.7.4.3).

Each ``deployments/<name>/server.py`` imports cleanly and,
when launched, exits with a clear message pointing at
``docs/migrating_to_published_generations.md`` if either
(a) ``OKG_DSN`` is not set or (b) the deployment has no
published generation.

These tests exercise the preflight via subprocess so we don't
have to stub out psycopg state in-process.
"""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.postgres

_REPO_ROOT = Path(__file__).resolve().parents[2]
# `cms`, `intersystems`, `memex` are abandoned-on-this-branch but
# planned for re-introduction. Each test below skips when the
# deployment dir is missing (rather than failing) so the suite
# stays green during the abandoned interval; once a deployment's
# `server.py` is restored, the parametrized cases auto-resume.
_DEPLOYMENTS = ("cms", "intersystems", "memex", "af_pubs")


def _skip_if_deployment_missing(deployment: str) -> Path:
    """Return the deployment's server.py path; skip the test if
    the deployment dir or its server.py is not present in this
    checkout."""
    server_py = _REPO_ROOT / "deployments" / deployment / "server.py"
    if not server_py.is_file():
        pytest.skip(
            f"deployment {deployment!r} not present in this "
            f"checkout (expected {server_py})"
        )
    return server_py


def _run_server_py(deployment: str, env_override: dict[str, str]) -> subprocess.CompletedProcess:
    env = {**os.environ, **env_override}
    # Drop OKG_DSN unless explicitly overridden so the tests are
    # deterministic in the developer's shell.
    if "OKG_DSN" not in env_override:
        env.pop("OKG_DSN", None)
    return subprocess.run(
        [sys.executable, str(_REPO_ROOT / "deployments" / deployment / "server.py")],
        capture_output=True,
        text=True,
        timeout=15,
        env=env,
    )


@pytest.mark.parametrize("deployment", _DEPLOYMENTS)
def test_server_stub_imports_cleanly(deployment: str) -> None:
    """Each stub must be importable — no syntax errors, no missing
    substrate modules."""
    module_path = _skip_if_deployment_missing(deployment)
    # Syntax check only — importing the module would try to launch
    # the server.
    out = subprocess.run(
        [sys.executable, "-c", f"compile(open('{module_path}').read(), '{module_path}', 'exec')"],
        capture_output=True, text=True, timeout=10,
    )
    assert out.returncode == 0, out.stderr


@pytest.mark.parametrize("deployment", _DEPLOYMENTS)
def test_missing_dsn_exits_nonzero_with_guide_pointer(deployment: str) -> None:
    """Launching without OKG_DSN exits with the migration-guide
    pointer."""
    _skip_if_deployment_missing(deployment)
    result = _run_server_py(deployment, env_override={})
    assert result.returncode != 0, (
        f"expected non-zero exit for {deployment!r}; "
        f"stdout={result.stdout!r} stderr={result.stderr!r}"
    )
    assert "OKG_DSN" in result.stderr, result.stderr
    assert "migrating_to_published_generations.md" in result.stderr, result.stderr


@pytest.mark.parametrize("deployment", _DEPLOYMENTS)
def test_unbootstrapped_deployment_exits_nonzero_with_guide_pointer(
    deployment: str, clean_runtime: str,
) -> None:
    """A reachable substrate DSN with no published generation for
    this deployment exits non-zero with the migration-guide
    pointer."""
    _skip_if_deployment_missing(deployment)
    # `clean_runtime` truncates graph_generations, so neither of
    # the 4 deployments has a published generation here.
    result = _run_server_py(
        deployment, env_override={"OKG_DSN": clean_runtime},
    )
    assert result.returncode != 0, (
        f"expected non-zero exit for {deployment!r}; "
        f"stdout={result.stdout!r} stderr={result.stderr!r}"
    )
    assert "no published generation" in result.stderr, result.stderr
    assert deployment in result.stderr, result.stderr
    assert "migrating_to_published_generations.md" in result.stderr, result.stderr


def test_no_legacy_imports() -> None:
    """Every server.py must import only from substrate paths —
    no `okg.mcp_server`, `okg.backbone`, `okg.core`, or
    `okg.persistence`."""
    forbidden = (
        "okg.mcp_server",
        "okg.backbone",
        "okg.core.",
        "okg.persistence",
    )
    for deployment in _DEPLOYMENTS:
        path = _REPO_ROOT / "deployments" / deployment / "server.py"
        if not path.is_file():
            # Skip abandoned-but-planned-for-restoration deployments;
            # the per-deployment parametrized tests above also skip
            # in this case. Once the deployment is restored, this
            # check resumes automatically.
            continue
        text = path.read_text()
        for f in forbidden:
            assert f not in text, (
                f"{path}: must not reference legacy runtime "
                f"module {f!r}"
            )
