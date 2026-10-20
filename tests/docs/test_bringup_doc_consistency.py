"""Bring-up doc consistency gate.

PACT ``gate-repo-deploy-golden-path`` / ``req.golden.one-front-door``:
exactly one canonical quickstart for the coding-repo path, one DSN
scheme, one host convention, and one invocation form across the
bring-up docs. The deployment-UX audit behind issues #636-#644 found
seven mutual contradictions across three docs prescribing bring-up for
the same deployment (postgres:// vs postgresql://, localhost vs
127.0.0.1, bare ``okg`` vs ``uv run okg``, three DSN wirings); this
test greps the retired variants so they cannot be reintroduced.

These assertions are maintained TOGETHER with
tests/docs/test_workspace_graph_quickstart.py (which pins the
quickstart's content, including the literal
``postgresql://postgres:okg@127.0.0.1:5433/<db-name>`` DSN) and with
tests/e2e/test_first_run_golden_path.py (which executes the
quickstart's command blocks). A convention change must update all
three in the same diff.
"""
from __future__ import annotations

import re
from pathlib import Path

from tests.mirror_scope import shipped

ROOT = Path(__file__).resolve().parents[2]
README = ROOT / "README.md"
QUICKSTART = ROOT / "docs" / "runbooks" / "workspace-graph" / "setup.md"
CANONICAL_DEV = ROOT / "deployments" / "okg-workspace" / "canonical-dev-okg.md"
DEPLOYMENT_README = ROOT / "deployments" / "okg-workspace" / "README.md"
TRIAGE = ROOT / "docs" / "runbooks" / "troubleshooting" / "first-run.md"
RUNBOOK_INDEX = ROOT / "docs" / "runbooks" / "index.md"

# The two okg-workspace pages are private; the public copy checks the quickstart.
BRINGUP_DOCS = tuple(shipped((QUICKSTART, CANONICAL_DEV, DEPLOYMENT_README)))

_BASH_BLOCK_RE = re.compile(r"```bash\n(.*?)```", re.DOTALL)


def _bash_lines(doc: Path) -> list[tuple[str, str]]:
    lines: list[tuple[str, str]] = []
    for block in _BASH_BLOCK_RE.findall(doc.read_text()):
        for line in block.splitlines():
            lines.append((doc.name, line))
    return lines


def test_readme_routes_to_the_quickstart_not_an_internal_runbook() -> None:
    text = README.read_text()
    # The public quickstart is now the README's front door (public-release-docs,
    # docs-quickstart section); it replaced the install guide here.
    assert "docs/start/quickstart.md" in text
    assert "docs/runbooks/workspace-graph/setup.md" not in text
    assert "docs/runbooks/troubleshooting/first-run.md" not in text


def test_one_dsn_scheme_and_host_across_bringup_docs() -> None:
    for doc in BRINGUP_DOCS:
        text = doc.read_text()
        assert "postgres://" not in text, (
            f"{doc.name}: use the postgresql:// scheme (retired variant "
            "postgres:// reintroduced)"
        )
        assert "@localhost:5433" not in text, (
            f"{doc.name}: use 127.0.0.1 for the bundled Postgres (retired "
            "variant localhost reintroduced)"
        )


def test_one_invocation_form_in_bringup_bash_blocks() -> None:
    # Bare `okg ...` resolves through PATH and can silently run a stale
    # global tool (issue #639); the repo command authority is `uv run okg`
    # (pact/project.yaml). Prose may say `okg provision`; command blocks
    # must not.
    offenders = [
        f"{name}: {line.strip()}"
        for name, line in (
            entry for doc in BRINGUP_DOCS for entry in _bash_lines(doc)
        )
        if line.startswith("okg ")
    ]
    assert not offenders, (
        "bare `okg` command lines in bring-up docs (use `uv run okg`, "
        "optionally with --extra flags): " + "; ".join(offenders)
    )


def test_secondary_docs_defer_to_the_quickstart() -> None:
    for doc in (CANONICAL_DEV, DEPLOYMENT_README):
        assert "workspace-graph/setup.md" in doc.read_text() or "setup.md" in doc.read_text(), (
            f"{doc.name} must point at the canonical quickstart instead of "
            "standing alone as a second front door"
        )


def test_first_run_triage_exists_and_is_indexed() -> None:
    assert TRIAGE.is_file(), "docs/runbooks/troubleshooting/first-run.md is missing"
    text = TRIAGE.read_text()
    for signature in (
        "points at a missing path",
        "createdb",
        "nomos.default_deny.no_active_bundle",
        "TreeSitterNotInstalled",
        "blocked",
        "graph.read.cli_search",
    ):
        assert signature in text, (
            f"first-run-triage.md must cover the failure signature "
            f"{signature!r}"
        )
    assert "troubleshooting/first-run.md" in RUNBOOK_INDEX.read_text(), (
        "docs/operate/index.md must index first-run triage"
    )
    assert "/runbooks/troubleshooting/first-run/" in QUICKSTART.read_text(), (
        "the quickstart must link first-run-triage for failed first runs"
    )
