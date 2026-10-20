"""Executed golden path: the workspace-graph quickstart, run for real.

PACT ``gate-repo-deploy-golden-path`` / ``req.golden.executed-quickstart``:
the canonical quickstart (docs/runbooks/workspace-graph/setup.md) MUST be
EXECUTED against a fixture repo and a dedicated Postgres database — not
just linted — so bring-up regressions (the issue #636/#637 class) turn a
CI lane red instead of shipping silently.

What runs here:

* ``test_quickstart_documented_sequence_publishes_and_searches`` executes
  the doc's own bash blocks (literate extraction via
  ``tests/e2e/quickstart_extract.py``): `okg install` from the
  workspace-graph profile — the documented door, per
  ``define-newcomer-entry-path``, which `okg init` now aliases — with
  enforce-mode Nomos, the deployment-env inspection, the status
  check, the doc's conditional manual-build sequence if (and only if) the
  first publish did not land, and the search block. It then asserts real
  substrate output: a ``published`` generation with ``node_count > 0`` and
  a non-empty ``okg search`` under ``subject:human_operator``.

* ``test_broken_scaffold_turns_the_lane_red`` is the regression canary for
  the #636 failure class: after a ``--no-publish`` install it deletes the
  scaffolded ``schemas/`` dir and asserts the doc's next command sequence
  fails NONZERO with the manifest-resolve error ("points at a missing
  path" naming ``schema_dir``) — proving this lane reds when the scaffold
  breaks.

* The install journeys (PACT ``journey-install-tests``) run the operator
  install pages the way a person follows them, with commands extracted by
  ``tests/e2e/install_journey.py``: build a wheel, install it into a fresh
  environment outside this checkout without the network, then
  ``docs/operate/install.md`` steps 3 and 4 with a home directory that holds
  Codex session logs; the same install while a Codex session is being
  written, followed by install.md's Codex recovery (``okg run --once
  --apply``); the academic-researcher dry run from the wheel; and the
  academic-researcher default install from the wheel, followed by that
  page's ``okg doctor --deployment <name>`` (with ``--json``), which must
  exit 0 with ``"ok": true`` (issue #2281). The live-session install was a
  strict xfail for #2469 until codex_sessions sealed its applied token over
  the bytes it read, and now must pass; the academic steps were strict
  xfails for #2470 and #2331 until the #2331 fix reached dev and turned them
  red, and now must pass.
  Each journey uses its own database on the same server, named after the
  DSN's database with a suffix (``_wheel``, ``_livecodex``, ``_academic``).

Gating: skips unless ``OKG_GOLDEN_PATH_DSN`` is set. The DSN's database
belongs to this test — it is dropped and recreated at the start of each
test. Locally:

    PGPASSWORD=okg createdb -h 127.0.0.1 -p 5433 -U postgres okg_golden_local || true
    OKG_GOLDEN_PATH_DSN="postgresql://postgres:okg@127.0.0.1:5433/okg_golden_local" \\
        uv run --extra code --extra quality --extra dev \\
        pytest tests/e2e/test_first_run_golden_path.py -q -x

In CI the ``first-run-golden-path`` job runs in the database pod executor
(.github/workflows/arc-db.yml) against the pod's own PostgreSQL sidecar on
127.0.0.1:5548.
"""
from __future__ import annotations

import json
import os
import re
import shlex
import shutil
import socket
import subprocess
import sys
import time
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[2]
QUICKSTART = REPO_ROOT / "docs" / "runbooks" / "workspace-graph" / "setup.md"

DEPLOYMENT = "golden-path-e2e"

# Real install + ingest + publish of the fixture repo takes minutes (uv env
# warm-up, schema migration, catalog apply, tree-sitter extraction, Chronos
# publish). Generous on purpose; the CI job caps the whole lane at 30 min.
INIT_TIMEOUT_S = 1500
BUILD_TIMEOUT_S = 1500
CHECK_TIMEOUT_S = 600

pytestmark = [
    pytest.mark.golden_path,
    pytest.mark.skipif(
        not os.environ.get("OKG_GOLDEN_PATH_DSN"),
        reason=(
            "OKG_GOLDEN_PATH_DSN not set — the executed golden path only "
            "runs in its dedicated lane (or locally against a scratch DB)"
        ),
    ),
]


# ---------------------------------------------------------------------------
# database ownership
# ---------------------------------------------------------------------------

def _dsn_parts(dsn: str) -> dict[str, str]:
    from psycopg import conninfo

    parts = {
        k: str(v)
        for k, v in conninfo.conninfo_to_dict(dsn).items()
        if v is not None
    }
    dbname = parts.get("dbname")
    if not dbname or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", dbname):
        pytest.fail(
            f"OKG_GOLDEN_PATH_DSN must name a plain-identifier database "
            f"to own; got dbname={dbname!r}"
        )
    return parts


def _recreate_database(dsn: str) -> None:
    """Drop + recreate the golden database. The DSN's DB is ours to own.

    This is the lane's stand-in for the quickstart's §1 DBA step (start
    Postgres + ``createdb``) — a BARE database. Postgres extensions are
    PER-DATABASE; the documented ``okg provision`` command installs them via
    ``ensure_required_extensions``, and THIS test proves it by deliberately
    not pre-installing any extension here.

    ``TEMPLATE template0`` is what keeps the database bare, and it is load
    bearing (issue #1972). ``ops/pg/initdb`` now creates the extensions in
    ``template1`` as well, and this lane's CI database replays that
    directory (``scripts/ci/apply_pg_initdb.py`` in
    ``.github/workflows/arc-db.yml``), as the local compose stack
    (``ops/pg/docker-compose.yaml``) bind-mounts it. A plain
    ``CREATE DATABASE`` clones ``template1``, so it would hand this test a
    database that ALREADY has every extension and the proof would pass while
    asserting nothing. ``template0`` is never touched by initdb.
    """
    import psycopg

    parts = _dsn_parts(dsn)
    dbname = parts["dbname"]
    server = {**parts, "dbname": "postgres"}
    with psycopg.connect(**server, autocommit=True) as conn:
        conn.execute(f'DROP DATABASE IF EXISTS "{dbname}" WITH (FORCE)')
        conn.execute(f'CREATE DATABASE "{dbname}" TEMPLATE template0')


# ---------------------------------------------------------------------------
# fixture repo
# ---------------------------------------------------------------------------

_PIPELINE_PY = '''\
"""Toy publish pipeline for the OKG golden-path fixture repo.

The docstrings deliberately reuse the quickstart's example search terms
("publisher generation blocked") so the doc's own search block has real
graph content to hit.
"""


class PublisherPipeline:
    """Coordinates publisher cycles and tracks generation state."""

    def __init__(self, name: str) -> None:
        self.name = name
        self.generations: list[str] = []

    def publish(self, generation_id: str) -> str:
        """Publish a generation and record it as the latest."""
        self.generations.append(generation_id)
        return generation_id

    def is_generation_blocked(self, generation_id: str) -> bool:
        """Return True when the publisher marked this generation blocked."""
        return generation_id.endswith("-blocked")
'''

_METRICS_PY = '''\
"""Queue metrics helpers for the golden-path fixture repo."""


def queue_depth(pending: list[str]) -> int:
    """Return the number of pending publisher jobs in the queue."""
    return len(pending)


def dlq_alert(depth: int, *, threshold: int = 10) -> bool:
    """Return True when the dead-letter queue depth breaches the threshold."""
    return depth >= threshold
'''

_README_MD = """\
# Golden fixture repo

A tiny package used by OKG's executed-quickstart gate. It models a
publisher whose generation can be blocked, so a lexical search for
"publisher generation blocked" finds real symbols.
"""


def _git(repo: Path, *args: str) -> None:
    subprocess.run(
        ["git", "-C", str(repo), *args],
        check=True,
        capture_output=True,
        text=True,
    )


def _build_fixture_repo(root: Path) -> Path:
    """A real git repo: small Python package, README, two commits."""
    repo = root / "golden-fixture-repo"
    pkg = repo / "golden_pkg"
    pkg.mkdir(parents=True)
    (pkg / "__init__.py").write_text(
        '"""Golden-path fixture package."""\n', encoding="utf-8"
    )
    (pkg / "pipeline.py").write_text(_PIPELINE_PY, encoding="utf-8")
    (repo / "README.md").write_text(_README_MD, encoding="utf-8")

    subprocess.run(
        ["git", "-c", "init.defaultBranch=main", "init", str(repo)],
        check=True,
        capture_output=True,
        text=True,
    )
    _git(repo, "config", "user.email", "golden-path@example.invalid")
    _git(repo, "config", "user.name", "Golden Path Fixture")
    _git(repo, "config", "commit.gpgsign", "false")
    _git(repo, "add", "-A")
    _git(repo, "commit", "-m", "add publisher pipeline package")

    (pkg / "metrics.py").write_text(_METRICS_PY, encoding="utf-8")
    (repo / "README.md").write_text(
        _README_MD + "\nSecond commit adds queue metrics.\n",
        encoding="utf-8",
    )
    _git(repo, "add", "-A")
    _git(repo, "commit", "-m", "add queue metrics helpers")
    return repo


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


# ---------------------------------------------------------------------------
# doc-block execution
# ---------------------------------------------------------------------------

def _block_env(deployments_root: Path) -> dict[str, str]:
    """Env for executed doc blocks: an operator shell, plus the doc's own
    'export the same root as OKG_DEPLOYMENTS_DIR' instruction."""
    env = os.environ.copy()
    env["OKG_DEPLOYMENTS_DIR"] = str(deployments_root)
    env.update({
        "GIT_AUTHOR_NAME": "Golden Path Fixture",
        "GIT_AUTHOR_EMAIL": "golden-path@example.invalid",
        "GIT_COMMITTER_NAME": "Golden Path Fixture",
        "GIT_COMMITTER_EMAIL": "golden-path@example.invalid",
    })
    # An operator following the quickstart has none of these set; they
    # would silently redirect deployment/DSN/subject resolution.
    for var in (
        "OKG_DEPLOYMENT",
        "PZ_OKG_DEPLOYMENT",
        "OKG_DSN",
        "OKG_NOMOS_SUBJECT",
        "OKG_PACT_DECISION_PROJECTION",
    ):
        env.pop(var, None)
    return env


def _run_block(
    label: str,
    block: str,
    *,
    env: dict[str, str],
    timeout: int,
) -> subprocess.CompletedProcess[str]:
    """Run one extracted doc block with bash, from the framework checkout
    (so `uv run` resolves THIS checkout, as the doc's 'Before You Start'
    instructs)."""
    print(f"\n===== golden-path block: {label} =====\n{block}", flush=True)
    proc = subprocess.run(
        ["bash", "-euo", "pipefail", "-c", block],
        cwd=str(REPO_ROOT),
        env=env,
        capture_output=True,
        text=True,
        errors="replace",
        timeout=timeout,
    )
    print(f"----- {label}: exit={proc.returncode} -----", flush=True)
    if proc.stdout:
        print(f"[{label} stdout]\n{proc.stdout[-8000:]}", flush=True)
    if proc.stderr:
        print(f"[{label} stderr]\n{proc.stderr[-8000:]}", flush=True)
    return proc


def _check_block(
    label: str,
    proc: subprocess.CompletedProcess[str],
) -> subprocess.CompletedProcess[str]:
    if proc.returncode != 0:
        pytest.fail(
            f"quickstart block {label!r} exited {proc.returncode} — the "
            f"documented golden path is broken.\n"
            f"stdout tail:\n{proc.stdout[-4000:]}\n"
            f"stderr tail:\n{proc.stderr[-4000:]}"
        )
    return proc


def _last_json_line(stdout: str, *, label: str) -> dict:
    for line in reversed(stdout.splitlines()):
        line = line.strip()
        if not line:
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            return payload
    return pytest.fail(
        f"no JSON object found in {label!r} stdout:\n{stdout[-4000:]}"
    )


def _substituted_blocks(
    dsn: str,
    fixture_repo: Path,
    deployments_root: Path,
):
    """Extract the doc's blocks and fill its placeholders for this run."""
    from tests.e2e.quickstart_extract import (
        DOC_DSN_LITERAL,
        extract_quickstart_blocks,
        substitute_placeholders,
    )

    blocks = extract_quickstart_blocks(QUICKSTART.read_text(encoding="utf-8"))
    mapping = {
        # The doc pins the local-stack DSN literal; rewrite it wholesale
        # from OKG_GOLDEN_PATH_DSN so the same test runs on the local dev
        # Postgres (5433) and the CI lane's sidecar Postgres (5548).
        DOC_DSN_LITERAL: dsn,
        "<deployment>": DEPLOYMENT,
        "<repo-root>": str(fixture_repo),
        "<db-name>": _dsn_parts(dsn)["dbname"],
        "<mcp-port>": str(_free_port()),
        "<deployment-dir>": str(deployments_root / DEPLOYMENT),
    }
    return blocks, {
        name: substitute_placeholders(getattr(blocks, name), mapping)
        for name in (
            "install_block",
            "commit_deployment_block",
            "deployment_env_block",
            "status_block",
            "manual_build_block",
            "search_block",
        )
    }


def _with_flag(block: str, flag: str) -> str:
    """Append a supported flag to the install command block."""
    return block.rstrip("\n") + f" {flag}\n"


def _status_publishing(payload: dict, *, label: str) -> dict:
    publishing = payload.get("publishing")
    if not isinstance(publishing, dict):
        pytest.fail(
            f"{label}: status JSON has no 'publishing' object: "
            f"{json.dumps(payload)[:2000]}"
        )
    return publishing


def _published_ok(publishing: dict) -> bool:
    status = publishing.get("latest_published_status")
    nodes = publishing.get("latest_published_node_count")
    return status == "published" and isinstance(nodes, int) and nodes > 0


# ---------------------------------------------------------------------------
# tests
# ---------------------------------------------------------------------------

def test_quickstart_documented_sequence_publishes_and_searches(
    tmp_path: Path,
) -> None:
    dsn = os.environ["OKG_GOLDEN_PATH_DSN"]
    _recreate_database(dsn)

    fixture_repo = _build_fixture_repo(tmp_path)
    deployments_root = tmp_path / "deployments"
    deployments_root.mkdir()
    codex_sessions_root = tmp_path / "codex" / "sessions"
    codex_archived_sessions_root = tmp_path / "codex" / "archived_sessions"
    codex_sessions_root.mkdir(parents=True)
    codex_archived_sessions_root.mkdir(parents=True)
    _blocks, cmds = _substituted_blocks(dsn, fixture_repo, deployments_root)
    env = _block_env(deployments_root)

    # §2 Set Up The Deployment — the doc's install command, plus the doc's
    # own instruction for deployment files under a specific directory: "add
    # --deployments-root ...; for later commands, export the same root as
    # OKG_DEPLOYMENTS_DIR" (the export is in `env`). The profile-supported
    # Codex roots isolate this fixture from the operator or CI runner's
    # unrelated local session history.
    install_cmd = _with_flag(
        _with_flag(
            _with_flag(
                cmds["install_block"],
                f"--deployments-root {shlex.quote(str(deployments_root))}",
            ).rstrip("\n"),
            f"--codex-sessions-root {shlex.quote(str(codex_sessions_root))}",
        ).rstrip("\n"),
        "--codex-archived-sessions-root "
        f"{shlex.quote(str(codex_archived_sessions_root))}",
    )
    install = _check_block(
        "install (§2)",
        _run_block(
            "install (§2)", install_cmd, env=env, timeout=INIT_TIMEOUT_S,
        ),
    )
    install_payload = _last_json_line(install.stdout, label="install (§2)")
    publish_outcome = install_payload.get("publish", {})
    print(
        f"install publish outcome: ok={publish_outcome.get('ok')} "
        f"message={publish_outcome.get('message')!r}",
        flush=True,
    )
    _check_block(
        "commit deployment (§2)",
        _run_block(
            "commit deployment (§2)",
            cmds["commit_deployment_block"],
            env=env,
            timeout=CHECK_TIMEOUT_S,
        ),
    )

    _check_block(
        "deployment env (§2)",
        _run_block(
            "deployment env (§2)",
            cmds["deployment_env_block"],
            env=env,
            timeout=CHECK_TIMEOUT_S,
        ),
    )

    # §3 Verify The Graph — build after committing, then check status in the
    # documented order.
    _check_block(
        "manual build (§3)",
        _run_block(
            "manual build (§3)",
            cmds["manual_build_block"],
            env=env,
            timeout=BUILD_TIMEOUT_S,
        ),
    )
    status = _check_block(
        "status (§3)",
        _run_block(
            "status (§3)", cmds["status_block"], env=env,
            timeout=CHECK_TIMEOUT_S,
        ),
    )
    publishing = _status_publishing(
        _last_json_line(status.stdout, label="status (§3)"),
        label="status (§3)",
    )
    assert publishing.get("latest_published_status") == "published", (
        f"quickstart did not reach a published generation: "
        f"{json.dumps(publishing, default=str)}"
    )
    node_count = publishing.get("latest_published_node_count")
    assert isinstance(node_count, int) and node_count > 0, (
        f"published generation has no nodes: "
        f"{json.dumps(publishing, default=str)}"
    )

    # §4 Try A Search — the doc's block includes the OKG_NOMOS_SUBJECT
    # export; it executes as part of the block, exercising the
    # enforce-mode read path end to end.
    search = _check_block(
        "search (§4)",
        _run_block(
            "search (§4)", cmds["search_block"], env=env,
            timeout=CHECK_TIMEOUT_S,
        ),
    )
    # The text form's status line (`search ok · ...`) and one `[n] <id>`
    # block per result row (issue #3666).
    if not re.search(r"^search ok\b", search.stdout, re.M):
        pytest.fail(
            "search (§4) output had no 'search ok' status line:\n"
            f"{search.stdout[-4000:]}"
        )
    counts = [len(re.findall(r"^\[\d+\] \S", search.stdout, re.M))]
    assert int(counts[0]) > 0, (
        "the quickstart's documented search returned zero results against "
        f"the published fixture graph:\n{search.stdout[-4000:]}"
    )


def test_broken_scaffold_turns_the_lane_red(tmp_path: Path) -> None:
    """Regression canary for the issue #636 class.

    A scaffold without its ``schemas/`` dir must make the doc's next
    command sequence fail nonzero with the manifest-resolve error. If this
    test ever finds the sequence exiting 0, the golden-path lane has lost
    its power to catch broken-scaffold regressions.
    """
    dsn = os.environ["OKG_GOLDEN_PATH_DSN"]
    _recreate_database(dsn)
    # The documented provision command installs extensions before resolving
    # the broken manifest, so this canary reaches the #636 signature on the
    # same bare database as the quickstart.

    fixture_repo = _build_fixture_repo(tmp_path)
    deployments_root = tmp_path / "deployments"
    deployments_root.mkdir()
    _blocks, cmds = _substituted_blocks(dsn, fixture_repo, deployments_root)
    env = _block_env(deployments_root)

    # Scaffold only (the doc: "If you only want to write the deployment
    # files, add --no-publish and run the manual build commands ... later").
    install_cmd = _with_flag(
        _with_flag(
            cmds["install_block"],
            f"--deployments-root {shlex.quote(str(deployments_root))}",
        ).rstrip("\n"),
        "--no-publish",
    )
    _check_block(
        "install --no-publish (canary)",
        _run_block(
            "install --no-publish (canary)", install_cmd, env=env,
            timeout=INIT_TIMEOUT_S,
        ),
    )

    schemas_dir = deployments_root / DEPLOYMENT / "schemas"
    assert schemas_dir.is_dir(), (
        f"install did not scaffold {schemas_dir} — the #636 fix regressed "
        "before the canary could even break it"
    )
    shutil.rmtree(schemas_dir)

    manual = _run_block(
        "manual build on broken scaffold (canary)",
        cmds["manual_build_block"],
        env=env,
        timeout=BUILD_TIMEOUT_S,
    )
    assert manual.returncode != 0, (
        "the doc's build sequence exited 0 against a scaffold with no "
        "schemas/ dir — the golden-path lane can no longer catch the "
        "#636 failure class"
    )
    combined = manual.stdout + manual.stderr
    assert "points at a missing path" in combined, (
        "expected the manifest-resolve error ('points at a missing path') "
        f"in the failure output; got:\nstdout:\n{manual.stdout[-4000:]}\n"
        f"stderr:\n{manual.stderr[-4000:]}"
    )
    assert "schema_dir" in combined, (
        "expected the failing manifest key ('schema_dir') to be named in "
        f"the failure output; got:\nstdout:\n{manual.stdout[-4000:]}\n"
        f"stderr:\n{manual.stderr[-4000:]}"
    )


# ---------------------------------------------------------------------------
# install journeys (PACT journey-install-tests)
# ---------------------------------------------------------------------------
#
# The operator install pages, run the way a person follows them: build a
# wheel, install it into a fresh environment OUTSIDE this checkout, and run
# docs/operate/install.md from step 2 with that environment's `okg`, with a
# home directory that holds Codex session logs. Then the academic-researcher
# profile's default install. Commands come from the pages through
# tests/e2e/install_journey.py, so a page edit that changes a command fails
# extraction here.
#
# A step that hits a bug that is still open is its own test, marked
# xfail(strict=True, raises=KnownOpenIssue): it counts as expected only when
# the step fails with that issue's exact signature. Any other failure is a
# real failure, and a pass means the issue is fixed, which turns the strict
# xfail red until the marker is removed (as happened for #2470 and #2331).

from tests.e2e.install_journey import (  # noqa: E402
    ACADEMIC_DOC_DSN,
    DOC_VENV,
    DOC_WHEEL,
    INSTALL_DOC_DSN,
    KnownOpenIssue,
    codex_session_rows,
    dependency_check_argv,
    load_install_journey,
    matches_2331,
    matches_2469,
    matches_2470,
    offline_dependency_sync_argv,
    offline_wheel_install_argv,
    substitute,
)

JOURNEY_DEPLOYMENT = "install-journey"
ACADEMIC_DEPLOYMENT = "install-journey-academic"

#: pytest's terminal reporter, set per module run. The lane runs pytest with
#: -q and no -s and keeps no JUnit artifact when artifact storage is full, so
#: step timings written through the reporter are the only per-step cost
#: record its job log keeps.
_TERMINAL: object | None = None


@pytest.fixture(scope="module", autouse=True)
def _journey_terminal(request: pytest.FixtureRequest):
    global _TERMINAL
    _TERMINAL = request.config.pluginmanager.get_plugin("terminalreporter")
    yield
    _TERMINAL = None
WHEEL_BUILD_TIMEOUT_S = 600
# The function the journey searches for; it is defined in the fixture repo's
# golden_pkg/pipeline.py.
JOURNEY_SEARCH_TERM = "is_generation_blocked"
LIVE_CODEX_DEPLOYMENT = "install-journey-live"
LIVE_CODEX_APPEND_INTERVAL_S = 0.01


def _journey_dsn(suffix: str) -> str:
    """A database of its own on the golden-path server, for one journey.

    The existing golden-path tests drop and recreate the OKG_GOLDEN_PATH_DSN
    database at the start of each test, so a journey that spans several
    tests must not share it.
    """
    from psycopg import conninfo

    dsn = os.environ["OKG_GOLDEN_PATH_DSN"]
    parts = _dsn_parts(dsn)
    return conninfo.make_conninfo(dsn, dbname=f"{parts['dbname']}_{suffix}")


def _dsn_url(dsn: str) -> str:
    """The same database as a postgresql:// URL, as the pages write it."""
    from urllib.parse import quote

    parts = _dsn_parts(dsn)
    user = quote(parts.get("user", "postgres"), safe="")
    password = parts.get("password")
    auth = f"{user}:{quote(password, safe='')}" if password else user
    host = parts.get("host", "127.0.0.1")
    port = parts.get("port", "5432")
    return f"postgresql://{auth}@{host}:{port}/{parts['dbname']}"


def _operator_env(home: Path, *, bin_dir: Path) -> dict[str, str]:
    """A person's shell: their own home, the environment's bin first on PATH.

    Nothing from this checkout's environment may leak in: no PYTHONPATH, no
    VIRTUAL_ENV, and none of the OKG variables that would redirect the
    deployment, DSN or read subject.
    """
    env = os.environ.copy()
    for var in (
        "PYTHONPATH",
        "VIRTUAL_ENV",
        "XDG_DATA_HOME",
        "XDG_CONFIG_HOME",
        "OKG_DEPLOYMENTS_DIR",
        "OKG_DEPLOYMENT",
        "PZ_OKG_DEPLOYMENT",
        "OKG_DSN",
        "OKG_NOMOS_SUBJECT",
        "OKG_PACT_DECISION_PROJECTION",
    ):
        env.pop(var, None)
    env["HOME"] = str(home)
    env["PATH"] = f"{bin_dir}{os.pathsep}{env.get('PATH', '')}"
    # The database pod this lane runs on has no Docker socket. Point Docker
    # at a socket that does not exist, so a developer's run with Docker up
    # takes the same path the pod does instead of passing only locally.
    env["DOCKER_HOST"] = f"unix://{home}/no-docker-here.sock"
    env.update({
        "GIT_AUTHOR_NAME": "Install Journey",
        "GIT_AUTHOR_EMAIL": "install-journey@example.invalid",
        "GIT_COMMITTER_NAME": "Install Journey",
        "GIT_COMMITTER_EMAIL": "install-journey@example.invalid",
    })
    return env


def _run_step(
    label: str,
    command: str | list[str],
    *,
    env: dict[str, str] | None,
    cwd: Path,
    timeout: int,
) -> subprocess.CompletedProcess[str]:
    """Run one journey step and print it, so a red lane shows the step."""
    shown = command if isinstance(command, str) else shlex.join(command)
    print(f"\n===== install journey: {label} (cwd={cwd}) =====\n{shown}",
          flush=True)
    argv = (
        ["bash", "-euo", "pipefail", "-c", command]
        if isinstance(command, str) else command
    )
    started = time.monotonic()
    proc = subprocess.run(
        argv,
        cwd=str(cwd),
        env=env,
        capture_output=True,
        text=True,
        errors="replace",
        timeout=timeout,
    )
    seconds = time.monotonic() - started
    print(f"----- {label}: exit={proc.returncode} -----", flush=True)
    if _TERMINAL is not None:
        _TERMINAL.write_line(
            f"install journey step: {label}: exit={proc.returncode} "
            f"{seconds:.1f}s"
        )
    if proc.stdout:
        print(f"[{label} stdout]\n{proc.stdout[-8000:]}", flush=True)
    if proc.stderr:
        print(f"[{label} stderr]\n{proc.stderr[-8000:]}", flush=True)
    return proc


def _step_output(proc: subprocess.CompletedProcess[str]) -> str:
    return (proc.stdout or "") + "\n" + (proc.stderr or "")


def _require_ok(label: str, proc: subprocess.CompletedProcess[str]) -> None:
    if proc.returncode != 0:
        pytest.fail(
            f"install journey step {label!r} exited {proc.returncode}.\n"
            f"stdout tail:\n{proc.stdout[-4000:]}\n"
            f"stderr tail:\n{proc.stderr[-4000:]}"
        )


def _outside_checkout(path: Path) -> None:
    resolved = path.resolve()
    if resolved == REPO_ROOT or REPO_ROOT in resolved.parents:
        pytest.fail(f"{path} is inside the checkout {REPO_ROOT}")


class _WheelEnvironment:
    def __init__(self, *, home: Path, venv: Path, workdir: Path) -> None:
        self.home = home
        self.venv = venv
        self.workdir = workdir
        self.env = _operator_env(home, bin_dir=venv / "bin")
        self.steps: dict[str, subprocess.CompletedProcess[str]] = {}


def _clean_source_copy(destination: Path) -> Path:
    """Copy the checkout's tracked files, and nothing else, to build from.

    A developer's checkout can hold a stale build/lib or egg-info that
    setuptools would reuse, hiding a file the wheel no longer ships. The copy
    has only what git tracks (with its working-tree content), so the wheel is
    built the way it is built from a fresh clone.
    """
    listed = subprocess.run(
        ["git", "-C", str(REPO_ROOT), "ls-files", "-z"],
        check=True,
        capture_output=True,
    ).stdout.decode("utf-8").split("\0")
    copied = 0
    for relative in filter(None, listed):
        source = REPO_ROOT / relative
        # A tracked path deleted in the working tree is not part of it.
        if not source.is_file() or source.is_symlink():
            continue
        target = destination / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, target)
        copied += 1
    print(f"copied {copied} tracked files to {destination}", flush=True)
    return destination


def _require_cached_build_backend(uv: str, root: Path) -> None:
    """Fail clearly when `uv build --offline` has no cached build backend.

    The wheel is built offline, so setuptools (pyproject.toml
    [build-system]) must already be in the uv cache. On the database pod the
    runner image warms /opt/uv-cache with the locked dependencies only
    (the runner image's Dockerfile, `uv sync --no-install-project`), so
    setuptools gets there when the lane's own `uv run --extra code ... pytest`
    step (.github/workflows/arc-db.yml, first-run-golden-path) builds this
    checkout as an editable install before this test starts. Without that,
    `uv build --offline` fails with a resolver error that names neither
    cause.
    """
    probe = root / "build-backend-probe"
    subprocess.run(
        [sys.executable, "-m", "venv", "--without-pip", str(probe)],
        check=True,
        capture_output=True,
    )
    check = _run_step(
        "is the build backend cached (setuptools, offline)",
        [
            uv, "pip", "install", "--offline", "--dry-run",
            "--python", str(probe / "bin" / "python"), "setuptools",
        ],
        env=None,
        cwd=root,
        timeout=CHECK_TIMEOUT_S,
    )
    if check.returncode != 0:
        pytest.fail(
            "setuptools, the wheel's build backend, is not in the uv cache, "
            "so the offline wheel build cannot run. In CI the lane's `uv run` "
            "step caches it by building this checkout before the test; run "
            "`uv sync` (or `uv build --wheel` once online) first.\n"
            f"{_step_output(check)[-2000:]}"
        )


def _build_wheel_environment(root: Path) -> _WheelEnvironment:
    """install-packaged.md "Install from a wheel", outside the checkout."""
    cmds = load_install_journey()
    uv = shutil.which("uv")
    if uv is None:
        pytest.fail("uv is not on PATH; the wheel journey builds with uv")
    _outside_checkout(root)
    home = root / "home"
    workdir = home / "work"
    workdir.mkdir(parents=True)
    dist = root / "dist"
    wheel_env = _WheelEnvironment(
        home=home, venv=home / DOC_VENV.removeprefix("~/"), workdir=workdir,
    )

    # `uv build --wheel`, run from a source checkout as the page says: a
    # clean copy of this one. --offline keeps PyPI out (the build backend
    # comes from the uv cache, checked first).
    _require_cached_build_backend(uv, root)
    source = _clean_source_copy(root / "source")
    build = _run_step(
        "build wheel",
        f"{cmds.build_wheel} --offline --out-dir {shlex.quote(str(dist))}",
        env=None,
        cwd=source,
        timeout=WHEEL_BUILD_TIMEOUT_S,
    )
    _require_ok("build wheel", build)
    wheels = sorted(dist.glob("okg-*-py3-none-any.whl"))
    if len(wheels) != 1:
        pytest.fail(f"expected one okg wheel in {dist}; found {wheels}")
    wheel = wheels[0]

    # `python3.12 -m venv ~/okg-venv`, with this lane's interpreter for
    # python3.12 and the journey's home for ~.
    _require_ok(
        "make venv",
        _run_step(
            "make venv",
            cmds.make_venv.replace(
                "python3.12", shlex.quote(sys.executable), 1,
            ),
            env=wheel_env.env,
            cwd=workdir,
            timeout=CHECK_TIMEOUT_S,
        ),
    )

    # `~/okg-venv/bin/pip install <wheel>`, without the network: the wheel's
    # locked runtime dependencies from uv.lock, then the same wheel with
    # --no-deps into the same environment, then a check that every
    # requirement the wheel declares is met (see install_journey).
    assert cmds.install_wheel == f"{DOC_VENV}/bin/pip install {DOC_WHEEL}"
    venv_python = wheel_env.venv / "bin" / "python"
    _require_ok(
        "install locked dependencies (offline)",
        _run_step(
            "install locked dependencies (offline)",
            offline_dependency_sync_argv(uv=uv),
            env={**os.environ, "UV_PROJECT_ENVIRONMENT": str(wheel_env.venv)},
            cwd=source,
            timeout=WHEEL_BUILD_TIMEOUT_S,
        ),
    )
    _require_ok(
        "install wheel (offline, no dependency resolution)",
        _run_step(
            "install wheel (offline, no dependency resolution)",
            offline_wheel_install_argv(
                uv=uv, venv_python=venv_python, wheel=wheel,
            ),
            env=None,
            cwd=workdir,
            timeout=WHEEL_BUILD_TIMEOUT_S,
        ),
    )
    _require_ok(
        "the wheel's requirements are all met",
        _run_step(
            "the wheel's requirements are all met",
            dependency_check_argv(uv=uv, venv_python=venv_python),
            env=None,
            cwd=workdir,
            timeout=CHECK_TIMEOUT_S,
        ),
    )

    wheel_env.steps["version"] = _run_step(
        "okg --version",
        cmds.check_version,
        env=wheel_env.env,
        cwd=workdir,
        timeout=CHECK_TIMEOUT_S,
    )
    wheel_env.steps["import"] = _run_step(
        "where okg imports from",
        [str(wheel_env.venv / "bin" / "python"), "-c",
         "import okg; print(okg.__file__)"],
        env=wheel_env.env,
        cwd=workdir,
        timeout=CHECK_TIMEOUT_S,
    )
    return wheel_env


@pytest.fixture(scope="module")
def wheel_environment(tmp_path_factory: pytest.TempPathFactory):
    return _build_wheel_environment(tmp_path_factory.mktemp("install-journey"))


def _write_codex_rollout(home: Path, *, repo: Path, stem: str) -> Path:
    """One Codex rollout under ~/.codex/sessions, where Codex writes them."""
    rollout = (
        home / ".codex" / "sessions" / "2026" / "09" / "26"
        / f"rollout-2026-09-26T00-00-00-{stem}.jsonl"
    )
    rollout.parent.mkdir(parents=True, exist_ok=True)
    rollout.write_text(
        "\n".join(json.dumps(row) for row in codex_session_rows(cwd=str(repo)))
        + "\n",
        encoding="utf-8",
    )
    return rollout


def _journey_mapping(dsn: str, *, name: str, repo: Path) -> dict[str, str]:
    return {
        INSTALL_DOC_DSN: _dsn_url(dsn),
        "<name>": name,
        "<repo>": str(repo),
        "<a function name>": JOURNEY_SEARCH_TERM,
    }


def _failed_at_ingest(install: subprocess.CompletedProcess[str]) -> bool:
    """install.md's Codex case: "the install exits 1 at the ingest stage"."""
    return (
        install.returncode == 1
        and '"failed_stage": "ingest"' in (install.stdout or "")
    )


def _status_counts(status_stdout: str) -> dict[str, object]:
    """The step 4 facts install.md names, read from `okg status` text."""
    published = re.search(r"latest_published_status\W+(\w+)", status_stdout)
    nodes = re.search(r"latest_published_node_count\W+(\d+)", status_stdout)
    edges = re.search(r"latest_published_edge_count\W+(\d+)", status_stdout)
    sessions = re.search(
        r"'subtype': 'agent_session', 'count': (\d+)", status_stdout,
    )
    return {
        "status": published.group(1) if published else None,
        "nodes": int(nodes.group(1)) if nodes else 0,
        "edges": int(edges.group(1)) if edges else 0,
        "agent_sessions": int(sessions.group(1)) if sessions else 0,
    }


@pytest.fixture(scope="module")
def workspace_journey(wheel_environment: _WheelEnvironment):
    """install.md from step 2 with the wheel's okg and Codex session logs.

    Step 2 (start PostgreSQL) is the lane's database server; the journey
    gets a bare database of its own. The home directory holds one finished
    Codex rollout under ~/.codex/sessions, the folder install.md says the
    graph reads, so the install meets a machine that uses Codex.
    """
    cmds = load_install_journey()
    env = wheel_environment.env
    home = wheel_environment.home
    workdir = wheel_environment.workdir
    dsn = _journey_dsn("wheel")
    _recreate_database(dsn)

    repo = _build_fixture_repo(home / "projects")
    _write_codex_rollout(home, repo=repo, stem="install-journey")
    mapping = _journey_mapping(dsn, name=JOURNEY_DEPLOYMENT, repo=repo)

    steps: dict[str, subprocess.CompletedProcess[str]] = {}
    steps["install"] = _run_step(
        "install (install.md step 3)",
        substitute(cmds.workspace_install, mapping),
        env=env,
        cwd=workdir,
        timeout=INIT_TIMEOUT_S,
    )
    # No recovery here: with a finished session the install must exit 0,
    # which test_wheel_workspace_install_with_codex_sessions_exits_zero
    # holds. The Codex recovery runs in the live-session journey below,
    # where install.md's condition for it actually occurs.
    steps["status"] = _run_step(
        "status (install.md step 4)",
        substitute(cmds.status, mapping),
        env=env,
        cwd=workdir,
        timeout=CHECK_TIMEOUT_S,
    )
    steps["search"] = _run_step(
        "search (install.md step 4)",
        substitute(cmds.search, mapping),
        env=env,
        cwd=workdir,
        timeout=CHECK_TIMEOUT_S,
    )
    return steps


def test_wheel_installs_and_runs_outside_the_checkout(
    wheel_environment: _WheelEnvironment,
) -> None:
    version = wheel_environment.steps["version"]
    _require_ok("okg --version", version)
    assert re.search(r"\bokg \d", version.stdout), version.stdout
    where = wheel_environment.steps["import"]
    _require_ok("where okg imports from", where)
    imported = Path(where.stdout.strip().splitlines()[-1]).resolve()
    assert wheel_environment.venv.resolve() in imported.parents, (
        f"the wheel environment imports okg from {imported}, not from its "
        f"own site-packages under {wheel_environment.venv}"
    )
    _outside_checkout(imported)


def test_wheel_workspace_install_with_codex_sessions_exits_zero(
    workspace_journey: dict[str, subprocess.CompletedProcess[str]],
) -> None:
    """A finished Codex session in ~/.codex does not fail the install."""
    install = workspace_journey["install"]
    if install.returncode == 0:
        return
    at_ingest = " at the ingest stage" if _failed_at_ingest(install) else ""
    pytest.fail(
        f"install exited {install.returncode}{at_ingest} with one finished "
        f"Codex session in ~/.codex:\n{_step_output(install)[-4000:]}"
    )


def test_wheel_workspace_journey_publishes_and_searches(
    wheel_environment: _WheelEnvironment,
    workspace_journey: dict[str, subprocess.CompletedProcess[str]],
) -> None:
    """install.md steps 3 and 4 reach a published, searchable graph."""
    config = (
        wheel_environment.home / ".local" / "share" / "okg" / "deployments"
        / JOURNEY_DEPLOYMENT
    )
    assert (config / "deployment.yaml").is_file(), (
        f"install.md says the configuration lives in {config}; it is not "
        "there"
    )

    status = workspace_journey["status"]
    _require_ok("status", status)
    counts = _status_counts(status.stdout)
    assert counts["status"] == "published", status.stdout
    assert counts["nodes"] > 0 and counts["edges"] > 0, status.stdout
    # The Codex rollout in ~/.codex was read into the graph.
    assert counts["agent_sessions"] >= 1, (
        "the published graph holds no agent_session, so the Codex rollout "
        f"in ~/.codex was not read:\n{status.stdout[-4000:]}"
    )

    search = workspace_journey["search"]
    _require_ok("search", search)
    assert (
        f"golden_pkg/pipeline.py:PublisherPipeline.{JOURNEY_SEARCH_TERM}"
        in search.stdout
    ), (
        f"install.md's search for {JOURNEY_SEARCH_TERM!r}, a function in "
        f"the indexed repository, did not find it:\n{search.stdout[-4000:]}"
    )


def _live_codex_journey(
    wheel_env: _WheelEnvironment, root: Path, *, db_suffix: str = "livecodex",
) -> dict[str, object]:
    """install.md step 3 while Codex is still writing a session.

    #2469 was seen on a machine whose ~/.codex held sessions being written
    during the install. The codex_sessions source refuses when the content
    hash of the session files it takes before its run differs from the one
    it takes after (runner.py, "source run cannot acknowledge change
    identity": exact_applied_snapshot false). A writer thread appends a
    Codex event every LIVE_CODEX_APPEND_INTERVAL_S seconds from before the
    install starts until after the diagnostic ends. Appends only ever grow
    the file, so the two hashes differ whenever at least one append lands
    between them. The window between them spans the whole source run (about
    300 ms locally for one small session), so a miss needs this idle test
    process's writer thread to go unscheduled for that whole window.

    Then, as install.md says for this case, the writer stops (the session
    ends) and the documented recovery `okg run --once ... --apply` runs,
    followed by step 4's status.
    """
    import threading

    cmds = load_install_journey()
    home = root / "home"
    workdir = home / "work"
    workdir.mkdir(parents=True)
    env = _operator_env(home, bin_dir=wheel_env.venv / "bin")
    dsn = _journey_dsn(db_suffix)
    _recreate_database(dsn)
    repo = _build_fixture_repo(home / "projects")
    rollout = _write_codex_rollout(home, repo=repo, stem="live")
    mapping = _journey_mapping(dsn, name=LIVE_CODEX_DEPLOYMENT, repo=repo)

    stop = threading.Event()
    appended = 0

    def append_events() -> None:
        nonlocal appended
        while not stop.wait(LIVE_CODEX_APPEND_INTERVAL_S):
            appended += 1
            row = {
                "type": "response_item",
                "timestamp": "2026-09-26T00:01:00Z",
                "payload": {
                    "type": "message",
                    "role": "assistant",
                    "content": f"still working, step {appended}",
                },
            }
            with rollout.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(row) + "\n")

    journey: dict[str, object] = {}
    writer = threading.Thread(target=append_events, daemon=True)
    writer.start()
    try:
        install = _run_step(
            "install while a Codex session is written (install.md step 3)",
            substitute(cmds.workspace_install, mapping),
            env=env,
            cwd=workdir,
            timeout=INIT_TIMEOUT_S,
        )
        journey["install"] = install
        if install.returncode != 0:
            # The install reports only a tail of its ingest log, which does
            # not carry the failing source's error. #2469's own diagnostic
            # command reruns that one source and prints it.
            journey["diagnosis"] = _run_step(
                "codex_sessions alone (the #2469 diagnostic)",
                [
                    "okg", "ingest", "--deployment", LIVE_CODEX_DEPLOYMENT,
                    "--include", "codex_sessions", "--no-predecessors",
                    "--no-publish", "--json",
                ],
                env=env,
                cwd=workdir,
                timeout=BUILD_TIMEOUT_S,
            )
    finally:
        stop.set()
        writer.join()
    journey["appended"] = appended
    print(f"appended {appended} Codex events during the install", flush=True)

    if _failed_at_ingest(install):
        journey["recovery"] = _run_step(
            "Codex recovery after the session ended (install.md step 3)",
            substitute(cmds.codex_recovery, mapping),
            env=env,
            cwd=workdir,
            timeout=BUILD_TIMEOUT_S,
        )
    journey["status"] = _run_step(
        "status after the live-session install (install.md step 4)",
        substitute(cmds.status, mapping),
        env=env,
        cwd=workdir,
        timeout=CHECK_TIMEOUT_S,
    )
    return journey


def _verdict_2469(journey: dict[str, object]) -> tuple[str, str]:
    """Classify the live-session install: fixed, #2469, or something else.

    The #2469 signature may come from either step. The install's own shape
    is the one the issue reports (exit 1 at ingest, codex_sessions the only
    failed source, at its run stage); the diagnostic, when it loses its own
    race, adds nothing, and when it reproduces it names the refusal.
    """
    install = journey["install"]
    assert isinstance(install, subprocess.CompletedProcess)
    if install.returncode == 0:
        return "fixed", "the install exited 0"
    output = _step_output(install)
    only_codex_failed = (
        "failed_sources: [{'source': 'codex_sessions', 'stage': 'run'}]"
        in output
    )
    diagnosis = journey.get("diagnosis")
    diagnosed = isinstance(
        diagnosis, subprocess.CompletedProcess,
    ) and matches_2469(_step_output(diagnosis))
    install_shape = _failed_at_ingest(install) and only_codex_failed
    if install_shape or diagnosed:
        return "known", (
            f"install exit 1 at ingest with only codex_sessions failed: "
            f"{install_shape}; diagnostic names 'source run cannot "
            f"acknowledge change identity': {diagnosed}"
        )
    return "other", (
        f"install exited {install.returncode}; at the ingest stage: "
        f"{_failed_at_ingest(install)}; only codex_sessions failed: "
        f"{only_codex_failed}; diagnostic names the refusal: {diagnosed}"
    )


@pytest.fixture(scope="module")
def workspace_install_live_codex(
    wheel_environment: _WheelEnvironment,
    tmp_path_factory: pytest.TempPathFactory,
):
    return _live_codex_journey(
        wheel_environment, tmp_path_factory.mktemp("install-journey-live"),
    )


def test_wheel_workspace_install_while_codex_writes_exits_zero(
    workspace_install_live_codex,
) -> None:
    journey = workspace_install_live_codex
    assert journey["appended"], "the writer appended nothing during the install"
    verdict, detail = _verdict_2469(journey)
    if verdict == "fixed":
        return
    install = journey["install"]
    diagnosis = journey.get("diagnosis")
    diagnostic_text = (
        f"\n[diagnostic]\n{_step_output(diagnosis)[-3000:]}"
        if isinstance(diagnosis, subprocess.CompletedProcess) else ""
    )
    if verdict == "known":
        raise KnownOpenIssue(f"#2469 reproduced: {detail}{diagnostic_text}")
    pytest.fail(
        f"the live-session install failed for a reason other than #2469 "
        f"({detail}):\n{_step_output(install)[-4000:]}{diagnostic_text}"
    )


def test_wheel_codex_recovery_publishes_after_the_session_ends(
    workspace_install_live_codex,
) -> None:
    """install.md's Codex recovery publishes what the failed install read.

    Runs only when the install failed at ingest, which is install.md's
    condition for the recovery. Once #2469 is fixed the install exits 0,
    there is nothing to recover, and step 4's status still has to show the
    published graph.
    """
    journey = workspace_install_live_codex
    install = journey["install"]
    assert isinstance(install, subprocess.CompletedProcess)
    if install.returncode != 0 and not _failed_at_ingest(install):
        pytest.fail(
            f"install exited {install.returncode} and not at the ingest "
            "stage, the only case install.md's Codex recovery covers:\n"
            f"{_step_output(install)[-4000:]}"
        )
    recovery = journey.get("recovery")
    if isinstance(recovery, subprocess.CompletedProcess):
        _require_ok("Codex recovery (okg run --once --apply)", recovery)
    status = journey["status"]
    assert isinstance(status, subprocess.CompletedProcess)
    _require_ok("status", status)
    # install.md: "Everything else was read. Publish it". The recovery
    # publishes the sources that completed; the refused Codex session is
    # not part of it, so no agent_session is required here.
    counts = _status_counts(status.stdout)
    assert counts["status"] == "published", status.stdout
    assert counts["nodes"] > 0 and counts["edges"] > 0, status.stdout


def test_wheel_academic_dry_run_outside_the_checkout(
    wheel_environment: _WheelEnvironment,
) -> None:
    """install.md's profile dry run, from the wheel, outside a checkout.

    This crashed with chdir(None) until the #2331 fix (issue #2470). It was
    a strict xfail here and turned red when that fix reached dev.

    Without a database the profile plans a Docker PostgreSQL, and the dry
    run is blocked where no Docker daemon answers, as on the database pod
    ("Docker daemon unreachable"). So the dry run gets the option
    academic-researcher.md documents for an existing database,
    --postgres-dsn, pointed at a bare database of its own.
    """
    cmds = load_install_journey()
    dsn = _journey_dsn("academicdry")
    _recreate_database(dsn)
    dsn_flag = cmds.academic_dsn_install.removeprefix(
        "okg install --profile academic-researcher "
    )
    assert dsn_flag == f"--postgres-dsn {ACADEMIC_DOC_DSN}", dsn_flag
    dry_run = _run_step(
        "academic-researcher dry run (install.md next steps)",
        substitute(
            f"{cmds.profile_dry_run} {dsn_flag}",
            {
                "<profile>": "academic-researcher",
                "<name>": ACADEMIC_DEPLOYMENT,
                ACADEMIC_DOC_DSN: _dsn_url(dsn),
            },
        ),
        env=wheel_environment.env,
        cwd=wheel_environment.workdir,
        timeout=CHECK_TIMEOUT_S,
    )
    output = _step_output(dry_run)
    regressed = " (#2470 regressed: chdir(None))" if matches_2470(output) else ""
    assert dry_run.returncode == 0, (
        f"the dry run exited {dry_run.returncode}{regressed}:\n"
        f"{output[-4000:]}"
    )
    assert '"status": "dry_run_ok"' in dry_run.stdout, dry_run.stdout[-4000:]


@pytest.fixture(scope="module")
def academic_install(
    wheel_environment: _WheelEnvironment,
    tmp_path_factory: pytest.TempPathFactory,
):
    """academic-researcher.md's default install onto an existing database.

    The page's wheel route: its install command with the --postgres-dsn
    option, the wheel environment's bin first on PATH, run from a directory
    outside any checkout, with an empty home as on a fresh machine.
    """
    cmds = load_install_journey()
    home = tmp_path_factory.mktemp("academic-journey") / "home"
    home.mkdir(parents=True)
    dsn = _journey_dsn("academic")
    _recreate_database(dsn)
    dsn_flag = cmds.academic_dsn_install.removeprefix(
        "okg install --profile academic-researcher "
    )
    assert dsn_flag == f"--postgres-dsn {ACADEMIC_DOC_DSN}", dsn_flag
    command = substitute(
        f"{cmds.academic_install} {dsn_flag}",
        {ACADEMIC_DOC_DSN: _dsn_url(dsn), "<name>": ACADEMIC_DEPLOYMENT},
    )
    proc = _run_step(
        "academic-researcher install from the wheel (academic-researcher.md)",
        command,
        env=_operator_env(home, bin_dir=wheel_environment.venv / "bin"),
        cwd=home,
        timeout=INIT_TIMEOUT_S,
    )
    return home, proc, wheel_environment


def _doctor_failures(report: dict) -> list[str]:
    """The failing checks in an ``okg doctor --json`` report, one per line."""
    lines = []
    for error in report.get("errors") or []:
        if not isinstance(error, dict):
            lines.append(repr(error))
            continue
        line = f"{error.get('code')}: {error.get('message', '')}"
        details = error.get("details")
        if isinstance(details, dict):
            # deployment_apply_not_converged carries the stages still needed
            # (for #2281, nomos_baseline) and the command that repairs it.
            # Name those first; the full details follow, cut to a length a
            # CI log can show.
            for key in ("status", "problems", "failed_stage", "next_command"):
                if details.get(key) not in (None, "", []):
                    line += f"\n    {key}: {details[key]}"
            needed = [
                f"{action.get('stage')} ({action.get('reason')})"
                for action in details.get("actions") or []
                if isinstance(action, dict) and action.get("needed")
            ]
            if needed:
                line += f"\n    stages needed: {', '.join(needed)}"
            line += "\n    details: " + json.dumps(
                details, sort_keys=True, default=str,
            )[:2000]
        lines.append(line)
    return lines


def test_default_academic_install_writes_the_deployment(academic_install) -> None:
    home, proc, _wheel = academic_install
    deployment = (
        home / ".local" / "share" / "okg" / "deployments" / ACADEMIC_DEPLOYMENT
        / "deployment.yaml"
    )
    assert deployment.is_file(), (
        f"the install did not write {deployment}; exit {proc.returncode}:\n"
        f"{_step_output(proc)[-4000:]}"
    )


def test_default_academic_install_exits_zero(academic_install) -> None:
    """A fresh academic-researcher install with no runnable source exits 0.

    It exited 1 with 'no runnable sources selected for sync enqueue' until
    issue #2331 was fixed. This was a strict xfail and turned red when that
    fix reached dev.
    """
    _home, proc, _wheel = academic_install
    output = _step_output(proc)
    known = [
        name for name, matches in (("#2331", matches_2331), ("#2470", matches_2470))
        if matches(output)
    ]
    regressed = f" ({', '.join(known)} regressed)" if known else ""
    assert proc.returncode == 0, (
        f"the install exited {proc.returncode}{regressed}:\n{output[-4000:]}"
    )


def test_default_academic_install_passes_doctor(academic_install) -> None:
    """The fresh install passes the doctor check its page tells people to run.

    academic-researcher.md, "Verify and remove the deployment": "`doctor`
    should complete without an error." A fresh install exited 0 but failed
    doctor with deployment_apply_not_converged at the nomos_baseline stage
    (issue #2281): init seeded the starting Nomos policy only in enforce
    mode, and this profile runs Nomos in audit mode. The exit code alone
    could not see that, so the journey runs the page's doctor command, as
    the operator, and requires exit 0 and ``"ok": true``.
    """
    home, _proc, wheel = academic_install
    cmds = load_install_journey()
    command = substitute(
        f"{cmds.academic_doctor} --json", {"<name>": ACADEMIC_DEPLOYMENT},
    )
    doctor = _run_step(
        "academic-researcher doctor (academic-researcher.md verify)",
        command,
        env=_operator_env(home, bin_dir=wheel.venv / "bin"),
        cwd=home,
        timeout=CHECK_TIMEOUT_S,
    )
    try:
        report = json.loads(doctor.stdout)
    except json.JSONDecodeError as exc:
        pytest.fail(
            f"`{command}` exited {doctor.returncode} and did not print one "
            f"JSON report ({exc}):\n{_step_output(doctor)[-4000:]}"
        )
    if not isinstance(report, dict):
        pytest.fail(f"`{command}` printed JSON that is not an object: {report!r}")
    if doctor.returncode != 0 or report.get("ok") is not True:
        shown = "\n".join(
            f"  - {line}" for line in _doctor_failures(report)
        ) or "  (the report lists no errors)"
        pytest.fail(
            f"`{command}` on a fresh academic-researcher install exited "
            f"{doctor.returncode} with ok={report.get('ok')!r}. "
            f"Failing checks:\n{shown}\n"
            f"stderr tail:\n{(doctor.stderr or '')[-2000:]}"
        )


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q", "-x"]))
