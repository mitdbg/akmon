"""Execution-mode deployment-lifecycle proofs against a real Postgres.

`deployment-lifecycle-execution-suite` PACT (child of deployment-lifecycle-e2e-ci).
The sibling `test_deployment_lifecycle_e2e.py` proves the pipeline publishes and
that `okg deployment up` walks its full phase sequence under ``--plan``. This
module proves the parts that only show up when things actually execute.

Covered here: ordered migrations applied to a POPULATED database, followed by a
successful republish. Fresh-database migration tests already exist
(tests/substrate/db/test_ordered_migrations.py); the populated path is what a
real deployment always is, and it is where migration 0014 dirties every scope
and forces a one-time full re-anchor whose cost only appears once there is data
to re-anchor.

Each test binds to its OWN database, created and dropped around the test, so a
migration replay can never touch the shared test database.
"""
from __future__ import annotations

import json
import os
import shutil
import signal
import subprocess
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

import psycopg
import pytest
import yaml

from okg.substrate.cli.main import main
from okg.substrate.deployment_upgrade import PHASES

from .._deployment_repo import commit_deployment_tree
from tests.substrate.conftest import enable_okg_mcp_login
from .test_publish_gate_multi_source import (
    _json_out,
    _live_rows,
    _write_sample_sources,
)

pytestmark = pytest.mark.postgres

_REPO_ROOT = Path(__file__).resolve().parents[3]
# The TRACKED fixture deployment. It must be committed: the backup planner reads
# the deployment through `git archive <revision> -- <deployment_dir>`, so an
# untracked tree fails with `pathspec did not match any files`.
_FIXTURE_ROOT = _REPO_ROOT / "tests" / "fixtures" / "deployments"
_FIXTURE_NAME = "lifecycle-exec"
# Identical to the above except for its Nomos posture, which is what admits the
# routine no-dump backup path. Two deployments rather than one because a dogfood
# posture without a recovery receipt refuses instead of forcing.
_DOGFOOD_NAME = "lifecycle-exec-dogfood"


def _git(*args: str) -> str:
    result = subprocess.run(
        ["git", *args], cwd=_REPO_ROOT, check=True,
        capture_output=True, text=True, timeout=30,
    )
    return result.stdout.strip()


def require_planner_preconditions() -> None:
    """Fail loudly (never silently skip) when the backup planner cannot admit.

    A skipped arm must not be reportable as a pass. In CI the checkout is clean
    and the tools are installed, so any of these conditions is a real failure;
    locally a dirty tree is ordinary developer state, so it is reported as a skip
    naming the exact blocker.
    """
    problems: list[str] = []
    if _git("status", "--porcelain=v1", "--untracked-files=normal", "--",
            "src/okg/substrate"):
        problems.append(
            "src/okg/substrate is not pristine, so the planner refuses on "
            "canonical-source integrity"
        )
    # release.channel: dev resolves the target to HEAD and additionally requires
    # a clean worktree for the target-revision check.
    if _git("status", "--porcelain=v1", "--untracked-files=normal"):
        problems.append(
            "the worktree is dirty, so the target revision is not exact; commit "
            "or stash before running the execution arms"
        )
    for tool in ("pg_dump", "pg_restore"):
        if shutil.which(tool) is None:
            problems.append(f"{tool} is not on PATH; a real archive cannot run")
    if not problems:
        return
    detail = "; ".join(problems)
    if os.environ.get("CI") or os.environ.get("OKG_CI_LANE"):
        pytest.fail(f"execution-suite preconditions unmet in CI: {detail}")
    pytest.skip(f"execution-suite preconditions unmet locally: {detail}")

# The tail ordered migrations replayed against populated data. 0044 creates
# the overlay marker and ranges and backfills markers; 0046 drops the attribute
# copies. Both are written with IF NOT EXISTS / IF EXISTS / ON CONFLICT guards,
# so replaying them is safe and exercises the real SQL against real rows. (The
# integrity-receipt migrations 0014 and 0015 used to be replayed here; after
# 0046 drops the tables they update, a replay of them would fail.)
_REPLAYED_MIGRATIONS = (
    "0044_attribute_overlay_generations",
    "0046_attribute_copies_teardown",
)


def _swap_dbname(dsn: str, dbname: str) -> str:
    """Repoint a DSN at another database, preserving URL form.

    URL form matters: the graph store dispatches on the scheme, so a
    keyword/value conninfo string ("dbname=... host=...") is not recognised as
    Postgres and silently falls back to SQLite.
    """
    parts = urlsplit(dsn)
    return urlunsplit(
        (parts.scheme, parts.netloc, f"/{dbname}", parts.query, parts.fragment)
    )


# A fresh database inherits nothing: the compose init script installs these in
# the default database at first boot, so a new one on the same instance needs its
# own CREATE EXTENSION. A developer box often has them in template1 already,
# which silently masks the omission — CI does not. pg_cron is deliberately
# excluded (single-database-per-instance, matching the shared test fixtures).
_REQUIRED_EXTENSIONS = (
    "btree_gist",
    "timescaledb",
    "vector",
    "pg_textsearch",
    "pg_trgm",
    "fuzzystrmatch",
)


@pytest.fixture
def dedicated_database(okg_dsn: str) -> Iterator[str]:
    """A private database for one test, created and dropped around it."""
    name = f"okg_lifecycle_exec_{os.getpid()}_{abs(hash(os.urandom(8))) % 10**6}"
    admin = _swap_dbname(okg_dsn, "postgres")
    dsn = _swap_dbname(okg_dsn, name)
    with psycopg.connect(admin, autocommit=True, connect_timeout=10) as conn:
        conn.execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')
        conn.execute(f'CREATE DATABASE "{name}"')
    try:
        with psycopg.connect(dsn, autocommit=True, connect_timeout=10) as conn:
            for extension in _REQUIRED_EXTENSIONS:
                conn.execute(f"CREATE EXTENSION IF NOT EXISTS {extension}")
        yield dsn
    finally:
        with psycopg.connect(admin, autocommit=True, connect_timeout=10) as conn:
            conn.execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')


def _stage_fixture(tmp_path: Path, dsn: str, capsys, *, name: str) -> Path:
    """Author a small bootstrap fixture bound to the dedicated database."""
    dep, brief = _write_sample_sources(tmp_path)
    for step in (
        ["plan", "--source", "records=fixtures/records.json",
         "--source", "docs=fixtures/corpus", "--brief", str(brief),
         "--name", name],
        ["inspect-source"],
        ["draft"],
    ):
        rc = main([
            "--json", "deployment", "bootstrap", step[0],
            "--deployment", str(dep), *step[1:], "--apply",
        ])
        capsys.readouterr()
        assert rc == 0, step

    target = tmp_path / "deployments" / name
    shutil.copytree(dep / ".okg_bootstrap/staged", target)

    reg = yaml.safe_load((target / "source_registry.bootstrap.yaml").read_text())
    for sid, entry in reg["sources"].items():
        entry["params"] = {"fixture_path": str(target / "fixtures" / f"{sid}.json")}
    (target / "source_registry.yaml").write_text(yaml.safe_dump(reg))

    manifest = yaml.safe_load((target / "deployment.yaml").read_text())
    # Bind the deployment to its own database literally: the drafted manifest
    # defers to `${OKG_DSN}`, but it is first resolved during bootstrap, before
    # this test could export that variable, so a deferred reference resolves
    # empty and the graph store silently falls back to SQLite.
    manifest["postgres"] = {"dsn": dsn}
    (target / "deployment.yaml").write_text(yaml.safe_dump(manifest))
    # The publisher refuses a deployment outside a committed git tree
    # (repository provenance, #1880), so commit before claim/provision.
    commit_deployment_tree(target, "lifecycle execution fixture deployment")
    return target


def _provision(
    name: str,
    target: Path,
    dsn: str,
    capsys,
    *,
    include: tuple[str, ...] = (),
) -> dict:
    """Install the schema, claim write ownership, and publish."""
    rc = main(["--json", "migrate", "--deployment", name, "--dsn", dsn, "--apply"])
    migrated = _json_out(capsys)
    assert rc == 0, migrated
    assert main([
        "--json", "catalog", "ownership", "claim",
        "--deployment", str(target), "--dsn", dsn, "--force",
    ]) == 0
    capsys.readouterr()
    command = ["provision", "--deployment", name, "--dsn", dsn, "--json"]
    if include:
        command.extend(["--include", *include])
    rc = main(command)
    out = _json_out(capsys)
    assert rc == 0, out
    return out


@contextmanager
def _ephemeral_mcp_login_precondition(dsn: str) -> Iterator[None]:
    """Exercise the MCP ``rolcanlogin`` precondition on the disposable cluster.

    Migration deliberately creates ``okg_mcp`` as NOLOGIN because production
    credentials are operator-owned. Readiness checks ``rolcanlogin`` before its
    owner-connection graph probe and correctly refuses NOLOGIN. This fixture
    does not claim credential authentication: it makes sure the flag is set
    while readiness runs.

    It follows the test-server rule in ``tests/substrate/conftest.py``
    (``okg_mcp_login_enabled``): LOGIN is set through
    ``enable_okg_mcp_login``, under the cluster role lock, and is NEVER
    reverted. The role is one row shared by every database on the server, so
    any revert races the other xdist worker's tests. An earlier version
    forced NOLOGIN on exit whenever ``CI`` was set; in the nightly coverage
    run, where two workers share one server, that failed every later MCP
    read-pool test on both workers (run 36224135940).
    """
    admin_dsn = _swap_dbname(dsn, "postgres")
    enable_okg_mcp_login(dsn)
    with psycopg.connect(
        admin_dsn, autocommit=True, connect_timeout=10,
    ) as conn:
        enabled = conn.execute(
            "SELECT rolcanlogin FROM pg_roles WHERE rolname = 'okg_mcp'"
        ).fetchone()
    assert enabled == (True,), enabled
    yield

def _runtime_venv_root() -> Path:
    """The environment the host-supervisor identity names for this checkout."""
    return _REPO_ROOT / ".venv"


def _provision_runtime_interpreter() -> tuple[Path, bool]:
    """Return ``<repo>/.venv/bin/python3``, provisioning it when it is absent.

    Why the path is fixed and not negotiable: ``deployment apply`` derives the
    host-supervisor identity as ``<working_directory>/.venv/bin/python3``
    (``okg.substrate.deployment_apply``), the running worker acknowledges its
    own ``sys.executable``, and the reconcile gate compares the two. CI
    installs the package into setup-python's tool-cache interpreter rather
    than a repository environment, so the fixture supplies that exact path
    instead of asking the test to relax a production identity check.

    Why it is a REAL environment rather than a bare symlink (#1947): a `.venv`
    directory holding only a symlink has no ``pyvenv.cfg``, so CPython resolves
    the link and reports the TOOL CACHE as its own ``sys.prefix``. uv then
    treats the tool cache as this repository's project environment, and any uv
    command run from the checkout while this fixture is open converges the
    shared interpreter -- the #1929 failure. A provisioned environment keeps
    the alias exact and confines uv to a directory this fixture owns.

    An existing `.venv` is reused as-is, so a developer's own environment is
    never touched. It is reused only after the same ``pyvenv.cfg`` check the
    created path gets: a `.venv` without one is the broken shape this change
    removes -- what the previous fixture built, and what a crashed run of it
    left behind -- and reusing it verbatim would reintroduce #1929 on a host
    where the guard test still passes. That case refuses, naming the path,
    rather than deleting a directory this fixture did not create.

    ``--system-site-packages`` is what lets the worker import okg's
    dependencies from wherever they were installed, and it reaches the BASE
    interpreter of ``sys.executable`` -- not an intermediate environment. That
    is sufficient only because CI installs the package into the tool-cache
    interpreter ITSELF (`uv pip install --system`, .github/workflows/test.yml).
    If that ever changes, or if this runs where the repository has no `.venv`
    and pytest is started from some other environment, the provisioned
    interpreter will not see okg's dependencies and the worker will die on an
    import. The previous symlink fixture pointed at the same resolved base, so
    this is an inherited assumption, not a new one -- but it is an assumption,
    and it is pinned here rather than left to be rediscovered.
    """
    venv_root = _runtime_venv_root()
    interpreter = venv_root / "bin" / "python3"
    configuration = venv_root / "pyvenv.cfg"
    if interpreter.exists():
        if not configuration.is_file():
            raise AssertionError(
                f"{venv_root} has an interpreter but no pyvenv.cfg, so "
                f"{interpreter} reports the interpreter it points at as its "
                "own prefix and any uv command run from this checkout would "
                "converge that shared interpreter (#1947, #1929). Remove "
                f"{venv_root} and run again."
            )
        return interpreter, False
    venv_root.parent.mkdir(parents=True, exist_ok=True)
    result = subprocess.run(
        [
            sys.executable, "-m", "venv",
            "--system-site-packages",
            "--without-pip",
            str(venv_root),
        ],
        capture_output=True,
        text=True,
        timeout=300,
        check=False,
    )
    if result.returncode != 0 or not interpreter.exists():
        shutil.rmtree(venv_root, ignore_errors=True)
        raise AssertionError(
            f"could not provision the fixture runtime environment at "
            f"{venv_root} (returncode={result.returncode}):\n"
            f"{result.stdout[-2000:]}{result.stderr[-2000:]}"
        )
    if not configuration.is_file():
        shutil.rmtree(venv_root, ignore_errors=True)
        raise AssertionError(
            f"{venv_root} has no pyvenv.cfg, so its interpreter would report "
            "the tool cache as its own prefix (#1947)"
        )
    return interpreter, True


def _remove_provisioned_runtime_venv() -> None:
    """Remove the environment this fixture provisioned, whole."""
    shutil.rmtree(_runtime_venv_root(), ignore_errors=True)


@contextmanager
def _running_runtime_worker(
    *,
    name: str,
    dsn: str,
    tmp_path: Path,
) -> Iterator[Path]:
    """Run the worker that a real deployment has during source convergence."""
    log_path = tmp_path / "runtime-worker.log"

    # Host supervisor authority names the interpreter installed at this exact
    # path, so the worker launches through it and the production identity
    # check stays load-bearing.  See `_provision_runtime_interpreter`: this is
    # a real environment, never a bare symlink, so uv cannot mistake the shared
    # tool-cache interpreter for this repository's project environment (#1947).
    # Provisioned INSIDE the try, so a failure anywhere below -- the first
    # database read, opening the log, spawning the worker -- still removes an
    # environment this fixture created. A leaked one would be reused by every
    # later run.
    created_runtime_venv = False
    try:
        runtime_interpreter, created_runtime_venv = (
            _provision_runtime_interpreter()
        )

        def application_version_timestamp() -> int:
            with psycopg.connect(dsn) as conn, conn.cursor() as cur:
                cur.execute(
                    "SELECT COALESCE(max(version_timestamp), 0) "
                    "FROM dbos.application_versions"
                )
                return int(cur.fetchone()[0])

        before_version_timestamp = application_version_timestamp()
        env = {
            **os.environ,
            "OKG_DEPLOYMENTS_DIR": str(_FIXTURE_ROOT),
            "OKG_DSN": dsn,
            "PYTHONPATH": str(_REPO_ROOT / "src"),
            "PYTHONUNBUFFERED": "1",
        }
        with log_path.open("w", encoding="utf-8") as log_handle:
            process = subprocess.Popen(
                [
                    str(runtime_interpreter),
                    "-m",
                    "okg.substrate.cli.main",
                    "--json",
                    "runtime",
                    "worker",
                    "--deployment",
                    name,
                    "--dsn",
                    dsn,
                ],
                cwd=_REPO_ROOT,
                env=env,
                stdin=subprocess.DEVNULL,
                stdout=log_handle,
                stderr=subprocess.STDOUT,
                text=True,
                start_new_session=True,
            )
            try:
                deadline = time.monotonic() + 60
                while time.monotonic() < deadline:
                    returncode = process.poll()
                    output = log_path.read_text(encoding="utf-8", errors="replace")
                    if returncode is not None:
                        raise AssertionError(
                            "runtime worker exited before launch "
                            f"(returncode={returncode}):\n{output[-3000:]}"
                        )
                    # DBOS may not emit INFO logs under the active logging profile.
                    # Its application-version row is the durable launch receipt and
                    # is stronger than matching a console string.
                    launched_in_db = (
                        application_version_timestamp() > before_version_timestamp
                    )
                    if (
                        "worker launched:" in output
                        or "DBOS launched" in output
                        or launched_in_db
                    ):
                        break
                    time.sleep(0.1)
                else:
                    raise AssertionError(
                        "runtime worker did not launch within 60s:\n"
                        f"before_version_timestamp={before_version_timestamp} "
                        f"current_version_timestamp={application_version_timestamp()}\n"
                        + log_path.read_text(
                            encoding="utf-8", errors="replace",
                        )[-3000:]
                    )
                yield log_path
            finally:
                if process.poll() is None:
                    os.killpg(process.pid, signal.SIGTERM)
                    try:
                        process.wait(timeout=30)
                    except subprocess.TimeoutExpired:
                        os.killpg(process.pid, signal.SIGKILL)
                        process.wait(timeout=10)
    finally:
        if created_runtime_venv:
            _remove_provisioned_runtime_venv()


def _bootstrap_runtime_with_disposable_toolchain(
    *,
    name: str,
    dsn: str,
) -> int:
    """Bootstrap DBOS through the exact interpreter path in the fixture plan.

    Production deliberately derives the DBOS executable beside
    ``sys.executable``. GitHub's setup-python installs the package into its
    tool-cache interpreter without installing a sibling ``dbos`` console
    script, so the execution fixture supplies both exact-path aliases for the
    duration of bootstrap. This keeps the production identity check
    load-bearing: no PATH fallback or alternate interpreter is admitted.

    The interpreter comes from `_provision_runtime_interpreter`, so the
    directory it lives in is a real environment with its own ``pyvenv.cfg``
    rather than a bare symlink onto the shared tool-cache interpreter (#1947).
    """
    runtime_dbos: Path | None = None
    created_runtime_venv = False
    created_dbos = False
    original_executable = sys.executable
    try:
        # Inside the try for the same reason as the worker fixture: nothing
        # below may leave a provisioned environment behind.
        runtime_interpreter, created_runtime_venv = (
            _provision_runtime_interpreter()
        )
        runtime_dbos = runtime_interpreter.parent / "dbos"
        if not runtime_dbos.exists():
            runtime_dbos.write_text(
                f"#!{runtime_interpreter}\n"
                "from dbos.cli.cli import app\n"
                "app()\n",
                encoding="utf-8",
            )
            runtime_dbos.chmod(0o755)
            created_dbos = True
        sys.executable = str(runtime_interpreter)
        return main([
            "--json", "runtime", "bootstrap", "--deployment", name,
            "--dsn", dsn,
        ])
    finally:
        sys.executable = original_executable
        if created_dbos and runtime_dbos is not None:
            runtime_dbos.unlink(missing_ok=True)
        if created_runtime_venv:
            _remove_provisioned_runtime_venv()


_PREFIX_PROBE = (
    "import json, sys; "
    "print(json.dumps({"
    "'executable': sys.executable, "
    "'prefix': sys.prefix, "
    "'base_prefix': sys.base_prefix}))"
)


def _prefix_probe(command: list[str], *, cwd: Path) -> dict[str, str]:
    """Run an interpreter probe and return the environment it reports."""
    # A uv command must not inherit this session's environment redirection, or
    # the probe would prove the redirection rather than the fixture.
    environment = {
        key: value
        for key, value in os.environ.items()
        if key not in {"VIRTUAL_ENV", "UV_PROJECT_ENVIRONMENT", "PYTHONPATH"}
    }
    result = subprocess.run(
        command,
        cwd=str(cwd),
        env=environment,
        capture_output=True,
        text=True,
        timeout=300,
        check=False,
    )
    assert result.returncode == 0, (command, result.stdout, result.stderr)
    return json.loads(result.stdout.strip().splitlines()[-1])


def test_fixture_runtime_environment_cannot_leak_to_the_tool_cache(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """#1947: a uv command run during the fixture stays out of the tool cache.

    The regression this guards is a `.venv` that is only a symlink onto the
    interpreter CI installed the package into. It has no ``pyvenv.cfg``, so
    CPython resolves the link and reports that shared prefix as its own, and uv
    then converges the shared interpreter for any command run from the
    checkout -- including commands from a concurrent xdist worker that never
    touches this fixture.
    """
    # uv is this test's probe, not a requirement of the fixture: the fixture
    # provisions with `python -m venv`.
    assert shutil.which("uv") is not None, "uv is required by this probe"
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    # The minimum that makes uv treat the directory as a project, which is what
    # the real checkout is.
    (checkout / "pyproject.toml").write_text(
        '[project]\n'
        'name = "lifecycle-exec-probe"\n'
        'version = "0.0.0"\n'
        'requires-python = ">=3.12"\n'
        'dependencies = []\n',
        encoding="utf-8",
    )
    monkeypatch.setattr(sys.modules[__name__], "_REPO_ROOT", checkout)

    interpreter, created = _provision_runtime_interpreter()
    assert created is True
    assert interpreter == checkout / ".venv" / "bin" / "python3"
    try:
        venv_root = checkout / ".venv"
        shared_prefix = _prefix_probe(
            [str(Path(sys.executable).resolve()), "-c", _PREFIX_PROBE],
            cwd=checkout,
        )["prefix"]

        direct = _prefix_probe([str(interpreter), "-c", _PREFIX_PROBE], cwd=checkout)
        assert Path(direct["prefix"]) == venv_root, direct
        assert Path(direct["prefix"]) != Path(shared_prefix), direct

        # The closing condition: uv, run from the checkout with the fixture
        # open, resolves the project environment to the fixture's own directory
        # and never to the shared interpreter.
        through_uv = _prefix_probe(
            ["uv", "run", "--no-sync", "python", "-c", _PREFIX_PROBE],
            cwd=checkout,
        )
        assert Path(through_uv["prefix"]) == venv_root, through_uv
        assert Path(through_uv["prefix"]) != Path(shared_prefix), through_uv
        assert Path(through_uv["executable"]).is_relative_to(venv_root), through_uv

        # The mechanism behind the two assertions above: without this file
        # CPython reports the interpreter's own install prefix as its own.
        assert (venv_root / "pyvenv.cfg").is_file()
        # Reusing it is what a second fixture in the same run does, and the
        # reuse must return the same provisioned environment.
        reused, created_again = _provision_runtime_interpreter()
        assert (reused, created_again) == (interpreter, False)
    finally:
        _remove_provisioned_runtime_venv()
    assert not (checkout / ".venv").exists()
    # A second call finds nothing to reuse, so a leaked environment cannot make
    # a later run pass by accident.
    assert _provision_runtime_interpreter()[1] is True
    _remove_provisioned_runtime_venv()


def test_fixture_refuses_to_reuse_a_venv_that_is_only_a_symlink(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """#1947: the broken shape is refused on the REUSE path too.

    Provisioning a real environment closes nothing if an existing `.venv` is
    taken on trust. The shape this refuses -- an interpreter with no
    ``pyvenv.cfg`` beside it -- is exactly what the previous fixture built and
    what a crashed run of it left behind, and reusing it verbatim brings #1929
    back on a host where every other assertion here still passes.
    """
    checkout = tmp_path / "checkout"
    interpreter = checkout / ".venv" / "bin" / "python3"
    interpreter.parent.mkdir(parents=True)
    interpreter.symlink_to(Path(sys.executable).resolve())
    monkeypatch.setattr(sys.modules[__name__], "_REPO_ROOT", checkout)

    with pytest.raises(AssertionError, match="pyvenv.cfg"):
        _provision_runtime_interpreter()
    # It refuses rather than deleting a directory it did not create.
    assert interpreter.is_symlink()


def test_bootstrap_toolchain_removes_its_environment_when_bootstrap_raises(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A failure after provisioning leaves nothing behind.

    A leaked environment is not a tidiness problem: the next run would reuse
    it, and a run that died before writing ``pyvenv.cfg`` would leave exactly
    the shape the reuse guard above has to refuse.
    """
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    monkeypatch.setattr(sys.modules[__name__], "_REPO_ROOT", checkout)

    def exploding_main(args: list[str]) -> int:
        raise RuntimeError("bootstrap failed")

    monkeypatch.setattr(sys.modules[__name__], "main", exploding_main)
    with pytest.raises(RuntimeError, match="bootstrap failed"):
        _bootstrap_runtime_with_disposable_toolchain(
            name="fixture",
            dsn="postgresql://fixture",
        )
    assert not (checkout / ".venv").exists()
    assert sys.executable != str(checkout / ".venv" / "bin" / "python3")


def test_disposable_runtime_bootstrap_toolchain_is_exact_and_removed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """The CI-only aliases are exact, executable, and leave no fallback."""
    checkout = tmp_path / "checkout"
    monkeypatch.setattr(sys.modules[__name__], "_REPO_ROOT", checkout)
    original_executable = sys.executable
    observed: dict[str, object] = {}

    def observe_main(args: list[str]) -> int:
        import dbos

        runtime_bin = checkout / ".venv" / "bin"
        runtime_interpreter = runtime_bin / "python3"
        runtime_dbos = runtime_bin / "dbos"
        site_packages = Path(dbos.__file__).resolve().parent.parent
        dbos_help = subprocess.run(
            [str(runtime_dbos), "--help"],
            capture_output=True,
            env={**os.environ, "PYTHONPATH": str(site_packages)},
            text=True,
            timeout=30,
        )
        observed.update({
            "args": args,
            "sys_executable": sys.executable,
            "interpreter_target": runtime_interpreter.resolve(),
            "dbos_text": runtime_dbos.read_text(encoding="utf-8"),
            "dbos_executable": bool(runtime_dbos.stat().st_mode & 0o111),
            "dbos_help_returncode": dbos_help.returncode,
        })
        return 0

    monkeypatch.setattr(sys.modules[__name__], "main", observe_main)
    assert _bootstrap_runtime_with_disposable_toolchain(
        name="fixture",
        dsn="postgresql://fixture",
    ) == 0

    runtime_bin = checkout / ".venv" / "bin"
    runtime_interpreter = runtime_bin / "python3"
    assert observed == {
        "args": [
            "--json", "runtime", "bootstrap", "--deployment", "fixture",
            "--dsn", "postgresql://fixture",
        ],
        "sys_executable": str(runtime_interpreter),
        "interpreter_target": Path(original_executable).resolve(),
        "dbos_text": (
            f"#!{runtime_interpreter}\n"
            "from dbos.cli.cli import app\n"
            "app()\n"
        ),
        "dbos_executable": True,
        "dbos_help_returncode": 0,
    }
    assert sys.executable == original_executable
    assert not runtime_interpreter.exists()
    assert not (runtime_bin / "dbos").exists()


def _clear_migration_ledger(dsn: str, migration_ids: tuple[str, ...]) -> int:
    """Forget that the named migrations ran, so the ordered runner replays them.

    The runner computes pending as "known minus ledger", so this is what makes
    the migration SQL execute again — this time against a database that already
    holds published graph rows.
    """
    with psycopg.connect(dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM okg.schema_migrations WHERE migration_id = ANY(%s)",
                (list(migration_ids),),
            )
            return cur.rowcount


_EVIDENCE_PATH = (
    Path(__file__).resolve().parents[3]
    / ".scratch" / "deployment-lifecycle-execution-generations.json"
)


def _record_generation_evidence(payload: dict) -> None:
    """Write the observed published generations for PACT evidence attachment."""
    _EVIDENCE_PATH.parent.mkdir(parents=True, exist_ok=True)
    _EVIDENCE_PATH.write_text(json.dumps(payload, indent=2, sort_keys=True))


def _recorded_migrations(dsn: str) -> set[str]:
    """Which of the replayed migrations the ledger currently records."""
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT migration_id FROM okg.schema_migrations WHERE migration_id = ANY(%s)",
            (list(_REPLAYED_MIGRATIONS),),
        )
        return {str(row[0]) for row in cur.fetchall()}


def _json_payload(capsys) -> dict:
    """Parse the CLI's JSON payload from captured output.

    A final stdout receipt is authoritative even when a long-running command
    emits a later progress heartbeat on stderr. Genuine failure payloads can
    still arrive on stderr, and multi-line reports retain a whole-buffer
    fallback.
    """
    captured = capsys.readouterr()
    stdout = captured.out or ""
    stderr = captured.err or ""

    def is_progress_heartbeat(payload: object) -> bool:
        return (
            isinstance(payload, dict)
            and payload.get("schema") == "okg.deployment_up_progress.v1"
        )

    for line in reversed(stdout.strip().splitlines()):
        line = line.strip()
        if line.startswith("{"):
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue
            if not is_progress_heartbeat(payload):
                return payload

    if stdout.strip():
        try:
            payload = json.loads(stdout.strip())
        except json.JSONDecodeError:
            pass
        else:
            if not is_progress_heartbeat(payload):
                return payload

    for line in reversed(stderr.strip().splitlines()):
        line = line.strip()
        if line.startswith("{"):
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue
            if not is_progress_heartbeat(payload):
                return payload

    for stream in (stderr, stdout + stderr):
        if not stream.strip():
            continue
        try:
            payload = json.loads(stream.strip())
        except json.JSONDecodeError:
            continue
        if not is_progress_heartbeat(payload):
            return payload
    raise json.JSONDecodeError("no JSON payload in captured output", stdout + stderr, 0)


def test_json_payload_prefers_final_stdout_receipt_over_later_stderr_heartbeat(
    capsys,
) -> None:
    receipt = {"ok": True, "status": "complete", "phase": "restore"}
    heartbeat = {
        "schema": "okg.deployment_up_progress.v1",
        "operation": "pg_restore",
        "elapsed_seconds": 30.031,
    }

    print(json.dumps(receipt))
    print(json.dumps(heartbeat), file=sys.stderr)

    assert _json_payload(capsys) == receipt


def test_json_payload_uses_stderr_error_before_later_progress_heartbeat(capsys) -> None:
    error = {"ok": False, "status": "failed", "reason": "restore_failed"}
    heartbeat = {
        "schema": "okg.deployment_up_progress.v1",
        "operation": "pg_restore",
        "elapsed_seconds": 30.031,
    }

    print(json.dumps(error), file=sys.stderr)
    print(json.dumps(heartbeat), file=sys.stderr)

    assert _json_payload(capsys) == error


def test_json_payload_rejects_stderr_progress_without_a_final_receipt(capsys) -> None:
    heartbeat = {
        "schema": "okg.deployment_up_progress.v1",
        "operation": "pg_restore",
        "elapsed_seconds": 30.031,
    }
    print(json.dumps(heartbeat), file=sys.stderr)

    with pytest.raises(json.JSONDecodeError, match="no JSON payload"):
        _json_payload(capsys)


def test_json_payload_rejects_stdout_progress_without_a_final_receipt(capsys) -> None:
    heartbeat = {
        "schema": "okg.deployment_up_progress.v1",
        "operation": "pg_restore",
        "elapsed_seconds": 30.031,
    }
    print(json.dumps(heartbeat))

    with pytest.raises(json.JSONDecodeError, match="no JSON payload"):
        _json_payload(capsys)


def test_json_payload_uses_stderr_error_after_stdout_progress_heartbeat(capsys) -> None:
    heartbeat = {
        "schema": "okg.deployment_up_progress.v1",
        "operation": "pg_restore",
        "elapsed_seconds": 30.031,
    }
    error = {"ok": False, "status": "failed", "reason": "restore_failed"}

    print(json.dumps(heartbeat))
    print(json.dumps(error), file=sys.stderr)

    assert _json_payload(capsys) == error


def _archive_entries(backup_dir: Path) -> set[str]:
    """Names of the archives currently on disk under ``backup_dir``.

    Used to observe whether a backup phase actually wrote one, independently of
    what the phase reports about itself.
    """
    if not backup_dir.exists():
        return set()
    return {
        entry.name for entry in backup_dir.iterdir()
        if not entry.name.startswith(".")
    }


def _phase(payload: dict, phase: str) -> dict:
    for row in payload.get("phases", []):
        if row.get("phase") == phase:
            return row
    return {}


def _upgrade_summary(payload: dict) -> str:
    backup = _phase(payload, "backup")
    failed = _phase(payload, str(payload.get("failed_phase") or ""))
    return json.dumps({
        "ok": payload.get("ok"),
        "failed_phase": payload.get("failed_phase"),
        "reason": payload.get("reason"),
        "failed_source": failed.get("source_name"),
        "failed_blocking_stage": failed.get("blocking_stage"),
        "failed_action": failed.get("action"),
        "failed_workflows": failed.get("workflows"),
        "failed_phase_payload": failed,
        "phase_order": [p.get("phase") for p in payload.get("phases", [])],
        "not_ok": [
            {"phase": p.get("phase"), "reason": p.get("reason"),
             "fix_hint": p.get("fix_hint")}
            for p in payload.get("phases", []) if p.get("ok") is not True
        ],
        "backup_mode": backup.get("verification_mode"),
        "backup_decision": backup.get("backup_policy_decision"),
        "backup_effect_inventory": backup.get("effect_inventory"),
        "worker_log_tail": payload.get("_worker_log_tail"),
    }, indent=2, default=str)[:4000]


@pytest.mark.postgres
def test_execution_mode_upgrade_drives_all_phases_over_published_graph(
    dedicated_database: str,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys,
) -> None:
    """A real `okg deployment up` — no --plan — runs every phase for real against
    the tracked fixture, with the backup admission GRANTED rather than refused."""
    require_planner_preconditions()
    dsn = dedicated_database
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(_FIXTURE_ROOT))
    monkeypatch.setenv("OKG_DSN", dsn)
    monkeypatch.setenv("OKG_DEPLOYMENT_SOURCE_ACTION_TIMEOUT_SECONDS", "120")
    monkeypatch.setenv("OKG_DEPLOYMENT_SOURCE_CONVERGENCE_TIMEOUT_SECONDS", "180")
    monkeypatch.setenv("OKG_DEPLOYMENT_SOURCE_POLL_SECONDS", "0.25")
    monkeypatch.setenv("OKG_DEPLOYMENT_DRAIN_STABLE_ZERO_SECONDS", "0.1")
    monkeypatch.setenv("OKG_DEPLOYMENT_DRAIN_POLL_SECONDS", "0.05")

    target = _FIXTURE_ROOT / _FIXTURE_NAME
    _provision(_FIXTURE_NAME, target, dsn, capsys)
    with _ephemeral_mcp_login_precondition(dsn):
        # A real deployment has its runtime installed; without it readiness
        # cannot even read queue health.
        rc = _bootstrap_runtime_with_disposable_toolchain(
            name=_FIXTURE_NAME,
            dsn=dsn,
        )
        bootstrapped = capsys.readouterr()
        assert rc == 0, (bootstrapped.out or bootstrapped.err)[-2000:]

        with _running_runtime_worker(
            name=_FIXTURE_NAME,
            dsn=dsn,
            tmp_path=tmp_path,
        ) as worker_log:
            main([
                "--json", "deployment", "up", _FIXTURE_NAME,
                "--target", "prod", "--dsn", dsn, "--checks", "ci",
                "--no-apply-services", "--no-sync-code", "--no-sync-deps",
                "--backup-dir", str(tmp_path / "backups"),
                "--state-file", str(tmp_path / "up" / "state.json"),
                "--reset-state",
            ])
    payload = _json_payload(capsys)
    if payload.get("ok") is not True:
        payload["_worker_log_tail"] = worker_log.read_text(
            encoding="utf-8", errors="replace",
        )[-2000:]
    detail = _upgrade_summary(payload)

    # 1. The planner ADMITTED. A refusal is what an untracked fixture hits, and
    #    it is what made this arm look untestable.
    backup = _phase(payload, "backup")
    decision = backup.get("backup_policy_decision") or {}
    assert decision.get("mode") != "refuse", detail

    # 2. The backup really executed rather than being classified and skipped.
    assert backup.get("verification_mode") == "current_full_restore", detail
    backup_path_raw = backup.get("backup_path")
    assert isinstance(backup_path_raw, str) and backup_path_raw, detail
    backup_path = Path(backup_path_raw)
    assert backup_path.exists(), (
        f"forced backup archive does not exist: {backup_path}", detail
    )
    if backup_path.is_file():
        observed_archive_size = backup_path.stat().st_size
    else:
        observed_archive_size = sum(
            member.stat().st_size
            for member in backup_path.rglob("*")
            if member.is_file() and member.name != ".okg-archive-manifest.json"
        )
    assert observed_archive_size == int(backup.get("backup_size_bytes") or 0), detail
    assert str(backup.get("backup_digest") or "").startswith("sha256:"), detail
    assert backup.get("restore_verified") is True, detail
    assert backup.get("restore_current_archive") is True, detail

    # 3. The full canonical sequence ran, in order.
    assert [p.get("phase") for p in payload["phases"]] == list(PHASES), detail

    # 4. Every phase up to and including restart_services completed for real.
    #    `readiness` is deliberately excluded: it is an OPERATIONAL gate (source
    #    freshness windows, live queue health) that a fixture deployment nobody
    #    operates cannot satisfy, and forcing it green would mean weakening a
    #    production check. It must still have RUN, so a crash before it is
    #    caught.
    driven = list(PHASES)[: list(PHASES).index("readiness")]
    not_ok = [
        {"phase": p.get("phase"), "reason": p.get("reason")}
        for p in payload["phases"]
        if p.get("phase") in driven and p.get("ok") is not True
    ]
    assert not_ok == [], (not_ok, detail)
    assert _phase(payload, "readiness"), detail

    # 5. Exactly the four phases we deliberately turned off report a skip, plus
    #    the two phases migration 0046 retired with the attribute copies
    #    (PACT lane-s-storage-drop-copies), which must skip for exactly that
    #    reason. Named explicitly so a regression that degrades a WORKING phase
    #    into an ok-but-skipped shape (backup, migrate, catalog_apply,
    #    smoke_publish) reds this instead of passing quietly.
    from okg.substrate.deployment_upgrade import _RETIRED_ATTRIBUTE_COPY_PHASE_REASON

    skipped = {
        p.get("phase") for p in payload["phases"] if p.get("skipped") is not None
    }
    retired = {"attribute_read_model", "generation_recovery"}
    assert skipped == {
        "stop_services", "sync_code", "dependency_sync", "restart_services",
        *retired,
    }, (skipped, detail)
    for name in retired:
        assert _phase(payload, name).get("reason") == (
            _RETIRED_ATTRIBUTE_COPY_PHASE_REASON
        ), (name, detail)

    # 6. The graph survived the upgrade and still serves reads. This does NOT
    #    assert the upgrade published: provision already published into this
    #    database, and smoke_publish legitimately accepts a no-source-work
    #    outcome on unchanged fixture content.
    live = _live_rows(dsn, "SELECT count(*) AS n FROM okg.graph_nodes")
    assert live[0]["n"] > 0, "the upgrade left no readable live nodes"


@pytest.mark.postgres
def test_external_effect_restore_point_full_lifecycle(
    dedicated_database: str,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys,
) -> None:
    """Disposable proof for an irreversible canary plus verified DB restore.

    The provider is a loopback HTTP stub, while the actual smoke source and
    graph publish run through the normal CLI and Postgres pipeline.  The test
    provisions every source except ``records`` so the confirmed smoke has a
    deterministic positive live-row delta.
    """
    require_planner_preconditions()
    from okg.substrate import deployment_upgrade as du
    from okg.substrate.runtime import promotion_fence

    dsn = dedicated_database
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(_FIXTURE_ROOT))
    monkeypatch.setenv("OKG_DSN", dsn)
    monkeypatch.setenv("OKG_DEPLOYMENT_SOURCE_ACTION_TIMEOUT_SECONDS", "120")
    monkeypatch.setenv("OKG_DEPLOYMENT_SOURCE_CONVERGENCE_TIMEOUT_SECONDS", "180")
    monkeypatch.setenv("OKG_DEPLOYMENT_SOURCE_POLL_SECONDS", "0.25")
    # This laptop's shared volume is intentionally below the production 6%
    # floor. The dedicated database is tiny and is dropped by the fixture, so
    # isolate that unrelated machine-wide blocker while keeping the real dump,
    # restore, digest, identity, and receipt checks load-bearing.
    disposable_headroom = {
        "ok": True,
        "refuse": False,
        "status": "disposable_test_volume",
        "threshold_authority": {"source": "test_fixture", "ok": True},
    }
    monkeypatch.setattr(
        du,
        "_backup_headroom_preflight",
        lambda *args, **kwargs: dict(disposable_headroom),
    )
    monkeypatch.setattr(
        du,
        "_backup_post_dump_headroom_preflight",
        lambda *args, **kwargs: dict(disposable_headroom),
    )
    monkeypatch.setattr(
        du,
        "_restore_headroom_preflight",
        lambda *args, **kwargs: dict(disposable_headroom),
    )
    target = _FIXTURE_ROOT / _FIXTURE_NAME
    provisioned = _provision(
        _FIXTURE_NAME,
        target,
        dsn,
        capsys,
        include=("docs", "pact"),
    )
    before_generation = provisioned["publish_proof"]["generation_id"]
    before_nodes = _live_rows(dsn, "SELECT count(*) AS n FROM okg.graph_nodes")[0]["n"]

    provider_calls: list[dict] = []

    class ProviderStub(BaseHTTPRequestHandler):
        def do_POST(self) -> None:  # noqa: N802 - stdlib handler contract
            length = int(self.headers.get("Content-Length", "0"))
            body = self.rfile.read(length)
            provider_calls.append(json.loads(body or b"{}"))
            self.send_response(202)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"accepted":true}')

        def log_message(self, format: str, *args: object) -> None:
            del format, args

    server = ThreadingHTTPServer(("127.0.0.1", 0), ProviderStub)
    server_thread = threading.Thread(target=server.serve_forever, daemon=True)
    server_thread.start()
    provider_url = f"http://127.0.0.1:{server.server_port}/publish"

    actual_smoke = du._phase_smoke_publish
    actual_run_okg = du._run_okg
    actual_release_fence = promotion_fence.release_promotion_fence
    service_calls: list[str] = []

    def disposable_release_fence(*, schedule_disposition, **kwargs):
        if schedule_disposition == "keep":
            # The bounded adapter does not launch a replacement process, so it
            # must restore the predecessor snapshot rather than falsely claim
            # that a candidate registered equivalent schedules.
            service_calls.append("restore_schedules")
            schedule_disposition = "restore"
        return actual_release_fence(
            schedule_disposition=schedule_disposition,
            **kwargs,
        )

    def disposable_service_adapter(ctx, args, **kwargs):
        if args[:2] == ["dogfood", "status"]:
            # The real `okg dogfood status --json` contract
            # (okg.dogfood.runtime_status.v1): the reader derives running/
            # stopped from runtime_processes.root_count and ignores any
            # worker_state string, so the stub must speak that schema.
            return du.CommandResult(0, json.dumps({
                "schema": du.DOGFOOD_STATUS_SCHEMA,
                "ok": True,
                "runtime_processes": {
                    "ok": True,
                    "status": "single_executor",
                    "process_count": 1,
                    "root_count": 1,
                    "split_executor": False,
                },
                "adapter": "disposable_test_service",
            }))
        if args[:2] == ["dogfood", "down"]:
            service_calls.append("down")
            return du.CommandResult(0, json.dumps({
                "ok": True,
                "status": "stopped",
                "adapter": "disposable_test_service",
            }))
        if args[:2] == ["dogfood", "restart"]:
            service_calls.append("restart")
            return du.CommandResult(0, json.dumps({
                "ok": True,
                "status": "running",
                "adapter": "disposable_test_service",
            }))
        # kwargs carries env_overrides (the renamed-flag compat child env).
        return actual_run_okg(ctx, args, **kwargs)

    def provider_smoke(ctx):
        if not ctx.options.plan_only:
            import urllib.request

            request = urllib.request.Request(
                provider_url,
                data=json.dumps({
                    "deployment": ctx.options.deployment,
                    "operation": "smoke_publish",
                }).encode(),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urllib.request.urlopen(request, timeout=5) as response:
                assert response.status == 202
        return actual_smoke(ctx)

    monkeypatch.setattr(du, "_phase_smoke_publish", provider_smoke)
    monkeypatch.setattr(du, "_run_okg", disposable_service_adapter)
    monkeypatch.setattr(
        promotion_fence,
        "release_promotion_fence",
        disposable_release_fence,
    )
    backup_dir = tmp_path / "external-backups"
    state_file = tmp_path / "external-up" / "state.json"
    # #1929. The dependency-sync phase converges a uv PROJECT ENVIRONMENT, and
    # this fixture deployment's project is this very checkout. Left implicit,
    # the phase converges the environment the suite is running in and removes
    # every distribution the deployment does not declare -- which is every
    # dev-only one, pytest and hypothesis included. UV_PROJECT_ENVIRONMENT is
    # uv's own way to say where a project's environment lives, so pointing it
    # at a scratch path keeps this arm's `--sync-deps` coverage REAL (a full
    # `uv sync --frozen` against this checkout's lock) instead of trading the
    # phase away for `--no-sync-deps`. The tree is removed below; it is
    # hard-linked from the uv cache, but it is still a full environment.
    dependency_sync_env = tmp_path / "dependency-sync-env"
    monkeypatch.setenv("UV_PROJECT_ENVIRONMENT", str(dependency_sync_env))
    # Asserted before the upgrade runs, so removing the redirect fails HERE
    # rather than by quietly stripping the interpreter this suite needs.
    assert dependency_sync_env != _REPO_ROOT / ".venv"
    assert os.environ["UV_PROJECT_ENVIRONMENT"] == str(dependency_sync_env)
    common = [
        "--json", "up", _FIXTURE_NAME,
            "--target", "prod", "--dsn", dsn, "--checks", "general",
            "--include", "records",
            "--apply-services", "--no-sync-code", "--sync-deps",
        "--backup-dir", str(backup_dir),
        "--state-file", str(state_file),
    ]
    try:
        assert main([*common, "--plan", "--reset-state"]) == 0
        planned = _json_payload(capsys)
        plan = _phase(planned, "backup")["safety_plan"]
        assert plan["external_effects"] == {
            "present": True,
            "phase": "smoke_publish",
            "sources": ["records"],
            "classes": ["provider_mutation"],
            "reversible": False,
            "guarantee_unavailable": "provider_effect_rollback",
        }
        smoke_inventory = _phase(planned, "backup")["effect_inventory"]["smoke"]
        records_inventory = next(
            row for row in smoke_inventory["entries"] if row["name"] == "records"
        )
        assert records_inventory["adapter_contract_source"] == "deployment_manifest"
        assert records_inventory["adapter_contract_digest"].startswith("sha256:")
        confirmation = plan["confirmation"]["digest"]

        with _ephemeral_mcp_login_precondition(dsn):
            assert _bootstrap_runtime_with_disposable_toolchain(
                name=_FIXTURE_NAME,
                dsn=dsn,
            ) == 0
            capsys.readouterr()
            with _running_runtime_worker(
                name=_FIXTURE_NAME,
                dsn=dsn,
                tmp_path=tmp_path,
            ):
                rc = main([
                    *common,
                    "--confirm-safety-plan", confirmation,
                    "--reset-state",
                ])
        payload = _json_payload(capsys)
        # Read the converged environment before it is removed below.
        synced_distributions = sorted(
            path.name
            for path in dependency_sync_env.glob(
                "lib/python*/site-packages/*.dist-info"
            )
        )
        # A FRESH interpreter, so this reads the filesystem rather than the
        # modules this process already imported: if the dependency sync had
        # converged the interpreter running this suite, the dev-only
        # distributions would be gone from disk and this would fail.
        interpreter_probe = subprocess.run(
            [sys.executable, "-c", "import hypothesis, pytest; print('intact')"],
            capture_output=True,
            text=True,
            check=False,
        )
    finally:
        server.shutdown()
        server.server_close()
        server_thread.join(timeout=5)
        shutil.rmtree(dependency_sync_env, ignore_errors=True)

    # #1929 first, before every other assertion, so an unrelated failure
    # later in this arm can never skip the regression guard.
    detail = _upgrade_summary(payload)
    assert interpreter_probe.returncode == 0, (
        "the dependency sync stripped the interpreter running this suite: "
        f"{interpreter_probe.stderr[-2000:]}"
    )
    assert interpreter_probe.stdout.strip() == "intact"

    # Only now the run's own outcome, so a crashed run reports its failed phase
    # rather than a missing-row error from the assertions below it.
    assert rc == 0 and payload.get("ok") is True, detail
    dependency_sync = _phase(payload, "dependency_sync")
    assert dependency_sync["ok"] is True, detail
    assert dependency_sync["project_root"] == str(_REPO_ROOT), detail
    assert dependency_sync["environment"] == str(dependency_sync_env), detail
    assert dependency_sync["environment"] != str(_REPO_ROOT / ".venv"), detail
    # The phase really converged that environment rather than reporting a path
    # nothing was written to.
    assert len(synced_distributions) > 50, (synced_distributions, detail)
    backup = _phase(payload, "backup")
    smoke = _phase(payload, "smoke_publish")
    readiness = _phase(payload, "readiness")
    assert backup["restore_verified"] is True, detail
    assert backup["verification_mode"] == "current_full_restore", detail
    assert provider_calls == [{
        "deployment": _FIXTURE_NAME,
        "operation": "smoke_publish",
    }]
    assert service_calls == ["down", "restart", "restore_schedules"]
    assert smoke["publish_proof"]["status"] == "published", detail
    assert smoke["publish_proof"]["nodes_added"] > 0, detail
    assert smoke["publish_proof"]["dlq_count"] == 0, detail
    published_generation = smoke["smoke_generation_id"]
    assert published_generation != before_generation
    after_nodes = _live_rows(dsn, "SELECT count(*) AS n FROM okg.graph_nodes")[0]["n"]
    assert after_nodes > before_nodes
    assert readiness["ok"] is True, detail
    assert readiness["readiness_generation_id"] == published_generation

    rc = main([
        "metrics", "--qa-snapshot", "--deployment", _FIXTURE_NAME,
        "--generation", str(published_generation), "--dsn", dsn, "--json",
    ])
    metrics = _json_payload(capsys)
    assert rc == 0, metrics
    assert metrics["generation_id"] == published_generation
    assert metrics["nodes_live_delta"] > 0
    assert metrics["dlq_count"] == 0
    assert isinstance(metrics["queue_depth"], dict)
    assert all(int(depth) == 0 for depth in metrics["queue_depth"].values())

    # The verified dump/restore intentionally takes longer than the fixture's
    # publisher-freshness window. Produce a real post-deploy publisher witness
    # before doctor instead of weakening that freshness threshold.
    with _running_runtime_worker(
        name=_FIXTURE_NAME,
        dsn=dsn,
        tmp_path=tmp_path,
    ):
        assert main([
            "--json", "runtime", "trigger-schedule",
            "--deployment", _FIXTURE_NAME, "--dsn", dsn,
            "okg_publisher_cycle",
        ]) == 0
        trigger = _json_payload(capsys)
        workflow_id = trigger["workflow_id"]
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            assert main([
                "--json", "runtime", "debug-workflow",
                "--deployment", _FIXTURE_NAME, "--dsn", dsn,
                workflow_id,
            ]) == 0
            publisher = _json_payload(capsys)
            if publisher.get("status") == "SUCCESS":
                break
            assert publisher.get("status") not in {"ERROR", "CANCELLED"}, publisher
            time.sleep(0.1)
        else:
            pytest.fail(f"publisher freshness witness did not finish: {publisher}")

        doctor_deadline = time.monotonic() + 30
        while time.monotonic() < doctor_deadline:
            rc = main([
                "--json", "doctor", "--deployment", _FIXTURE_NAME, "--dsn", dsn,
            ])
            doctor = _json_payload(capsys)
            if rc == 0 and doctor.get("ok") is True:
                break
            doctor_errors = doctor.get("errors") or []
            codes = {
                str(row.get("code") or "")
                for row in doctor_errors
                if isinstance(row, dict)
            }
            assert codes and codes <= {
                "publisher_workflow_no_success_yet",
                "publisher_workflow_stale",
            }, doctor
            time.sleep(0.2)
        else:
            pytest.fail(f"doctor did not converge after publisher witness: {doctor}")
    lifecycle_state = json.loads(state_file.read_text(encoding="utf-8"))
    _record_generation_evidence({
        "test": "test_external_effect_restore_point_full_lifecycle",
        "command_surface": "okg up",
        "requested_target": payload["requested_target"],
        "resolved_target": payload["resolved_target"],
        "before_generation": before_generation,
        "published_generation": published_generation,
        "before_nodes": before_nodes,
        "after_nodes": after_nodes,
        "nodes_live_delta": metrics["nodes_live_delta"],
        "edges_live_delta": metrics["edges_live_delta"],
        "provider_calls": len(provider_calls),
        "queue_depth": metrics["queue_depth"],
        "dlq_count": metrics["dlq_count"],
        "queue_dlq_bounded": (
            metrics["dlq_count"] == 0
            and all(int(depth) == 0 for depth in metrics["queue_depth"].values())
        ),
        "doctor": {
            "ok": doctor["ok"],
            "error_count": len(doctor.get("errors") or []),
            "warning_count": len(doctor.get("warnings") or []),
        },
        "readiness": {
            "ok": readiness["ok"],
            "generation_id": readiness["readiness_generation_id"],
            "profile": readiness.get("profile"),
        },
        "lifecycle": {
            "status": lifecycle_state["status"],
            "completed": lifecycle_state["completed"],
            "completed_phases": lifecycle_state["completed_phases"],
            "phase_statuses": lifecycle_state["phase_statuses"],
            "attempt_count": len(lifecycle_state["phase_attempts"]),
            "safety_plan_digest": lifecycle_state["safety_contract"]["plan"]
            ["confirmation"]["digest"],
            "database_restore_point_verified": lifecycle_state["safety_contract"]
            ["guarantees"]["database_restore_point_verified"],
            "provider_effects_reversible": lifecycle_state["safety_contract"]
            ["guarantees"]["provider_effects_reversible"],
        },
    })


@pytest.mark.postgres
def test_backup_classifies_forced_then_routine(
    dedicated_database: str,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys,
) -> None:
    """The ROUTINE half of the backup decision: on a dogfood-posture deployment
    with a fresh recovery receipt, the phase skips the physical backup entirely.

    The FORCED half is asserted by
    test_execution_mode_upgrade_drives_all_phases_over_published_graph, which
    drives the identically-configured non-dogfood sibling and gets
    current_full_restore with a real archive. Both run the same risk-admitted
    policy, so the pair shows the decision actually responding to posture rather
    than a hardcoded setting.
    """
    require_planner_preconditions()
    dsn = dedicated_database
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(_FIXTURE_ROOT))
    monkeypatch.setenv("OKG_DSN", dsn)
    monkeypatch.setenv("OKG_DEPLOYMENT_SOURCE_ACTION_TIMEOUT_SECONDS", "120")
    monkeypatch.setenv("OKG_DEPLOYMENT_SOURCE_CONVERGENCE_TIMEOUT_SECONDS", "180")
    monkeypatch.setenv("OKG_DEPLOYMENT_SOURCE_POLL_SECONDS", "0.25")
    _provision(_DOGFOOD_NAME, _FIXTURE_ROOT / _DOGFOOD_NAME, dsn, capsys)
    rc = _bootstrap_runtime_with_disposable_toolchain(
        name=_DOGFOOD_NAME,
        dsn=dsn,
    )
    capsys.readouterr()
    assert rc == 0

    # The routine path is only admitted against a fresh, independently verified
    # recovery receipt, so mint one first.
    backup_dir = tmp_path / "dr"
    rc = main([
        "--json", "deployment", "backup", "verify", _DOGFOOD_NAME,
        "--target", "prod", "--dsn", dsn, "--backup-dir", str(backup_dir),
        "--state-file", str(tmp_path / "dr" / "state.json"), "--reset-state",
    ])
    receipt_payload = _json_payload(capsys)
    assert rc == 0, json.dumps(receipt_payload, default=str)[:2500]

    # Observe the archive directory, not just the phase's self-report: the
    # routine branch hardcodes physical_backup_started=False and
    # backup_tool_child_processes_started=0, so asserting only those fields
    # would stay green if the branch ever started dumping again.
    archives_before = _archive_entries(backup_dir)
    assert archives_before, "backup verify produced no archive to reuse"

    with _running_runtime_worker(
        name=_DOGFOOD_NAME,
        dsn=dsn,
        tmp_path=tmp_path,
    ) as worker_log:
        main([
            "--json", "deployment", "up", _DOGFOOD_NAME,
            "--target", "prod", "--dsn", dsn, "--checks", "dogfood",
            "--no-apply-services", "--no-sync-code", "--no-sync-deps",
            "--backup-dir", str(backup_dir),
            "--state-file", str(tmp_path / "up" / "state.json"), "--reset-state",
        ])
    payload = _json_payload(capsys)
    if payload.get("ok") is not True:
        payload["_worker_log_tail"] = worker_log.read_text(
            encoding="utf-8", errors="replace",
        )[-2000:]
    backup = _phase(payload, "backup")
    detail = _upgrade_summary(payload)

    # Routine: the policy DECIDED to reuse the recovery proof. A phase that
    # never ran yields {} here, so this also rules out "it simply didn't happen".
    assert backup.get("verification_mode") == "no_physical_rollback_required", detail
    assert backup.get("ok") is True, detail
    assert backup.get("physical_backup_started") is False, detail
    assert int(backup.get("backup_tool_child_processes_started") or 0) == 0, detail

    # The load-bearing check: no new archive appeared on disk. This is an
    # observation of the filesystem rather than a field the branch sets itself,
    # so a routine path that regressed into dumping would red here.
    assert _archive_entries(backup_dir) == archives_before, (
        "the routine backup wrote a new archive", detail
    )

    # The routine budget held (the phase runner already fails a phase that
    # exceeds its budget; assert the number so the bound is explicit).
    assert float(backup.get("duration_seconds") or 0) <= 60.0, detail


@pytest.mark.postgres
def test_backup_verify_mints_recovery_receipt(
    dedicated_database: str,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys,
) -> None:
    """`okg deployment backup verify` creates a real archive, restores it into a
    disposable database, and mints a recovery receipt."""
    require_planner_preconditions()
    dsn = dedicated_database
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(_FIXTURE_ROOT))
    monkeypatch.setenv("OKG_DSN", dsn)
    _provision(_FIXTURE_NAME, _FIXTURE_ROOT / _FIXTURE_NAME, dsn, capsys)

    rc = main([
        "--json", "deployment", "backup", "verify", _FIXTURE_NAME,
        "--target", "prod", "--dsn", dsn,
        "--backup-dir", str(tmp_path / "verify-backups"),
        "--state-file", str(tmp_path / "verify" / "state.json"),
        "--reset-state",
    ])
    payload = _json_payload(capsys)
    backup = _phase(payload, "backup")
    detail = json.dumps(backup, indent=2, default=str)[:3000]
    assert rc == 0, detail

    # A real archive was produced, not a classification that skipped the work.
    assert backup.get("verification_mode") == "current_full_restore", detail
    assert int(backup.get("backup_size_bytes") or 0) > 0, detail
    assert "pg_dump" in str(backup.get("command") or ""), detail
    assert backup.get("restore_verified") is True, detail
    assert backup.get("restore_current_archive") is True, detail
    restore_progress = (backup.get("progress") or {}).get("pg_restore") or {}
    assert restore_progress.get("operation") == "pg_restore", detail
    identity_checks = backup.get("identity_checks") or {}
    expected_identity_checks = {
        "database_owner_equal",
        "extension_versions_equal",
        "timescale_state_equal",
        "schema_migrations_equal",
        "schema_catalog_equal",
    }
    assert set(identity_checks) == expected_identity_checks, detail
    assert all(identity_checks.values()), detail

    # The restore proof carries earned TimescaleDB evidence: the fixture
    # database is TimescaleDB-backed (okg migrate requires the extension),
    # so the block must be proven, name the protected hypertables, and
    # record that the pre/post procedure ran.
    timescale_restore = backup.get("timescale_restore") or {}
    assert timescale_restore.get("status") == "proven", detail
    assert timescale_restore.get("pre_post_restore_used") is True, detail
    assert timescale_restore.get("hypertables_protected"), detail

    # The receipt binds the archive to a recovery point and a next-required time.
    receipt = backup.get("disaster_recovery_receipt") or {}
    assert receipt.get("schema") == "okg.deployment_disaster_recovery.v1", detail
    for field in ("archive_digest", "recovery_point_at", "verified_at",
                  "next_required_backup_at"):
        assert receipt.get(field), (field, detail)
    receipt_path = Path(str(receipt.get("path") or ""))
    assert receipt_path.is_file(), (receipt_path, detail)
    receipt_document = json.loads(receipt_path.read_text(encoding="utf-8"))
    archive = receipt_document.get("archive") or {}
    assert receipt_document.get("restored_archive_digest") == archive.get("digest")
    assert archive.get("digest") == backup.get("backup_digest")
    assert receipt.get("archive_digest") == backup.get("backup_digest")
    # The minted receipt persists the earned TimescaleDB proof, so later
    # runs can reuse it without re-proving.
    receipt_timescale = receipt_document.get("timescale_restore") or {}
    assert receipt_timescale.get("status") == "proven", detail
    assert receipt_timescale.get("hypertables_protected"), detail

    # The producer itself emits the canonical target/effect/restore contract;
    # `okg up` does not invent those fields only when it later consumes state.
    state = json.loads(
        (tmp_path / "verify" / "state.json").read_text(encoding="utf-8")
    )
    contract = state.get("safety_contract") or {}
    plan = contract.get("plan") or {}
    observed = contract.get("observed_restore_point") or {}
    assert contract.get("schema") == "okg.deployment_safety_contract.v1", state
    assert plan.get("effect_digest", "").startswith("sha256:"), plan
    assert plan.get("code_revision"), plan
    assert plan.get("manifest_digest", "").startswith("sha256:"), plan
    assert (plan.get("restore_point") or {}).get("verified") is True, plan
    assert observed.get("verified") is True, observed
    assert observed.get("archive_digest") == backup.get("backup_digest"), observed
    assert (observed.get("restore_checks") or {}).get("restore_verified") is True
    assert contract.get("confirmed_plan_digest") == (
        (plan.get("confirmation") or {}).get("digest")
    )


@pytest.mark.postgres
def test_worker_boots_on_freshly_migrated_database(
    dedicated_database: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys,
) -> None:
    """The real runtime worker starts against a freshly-migrated database,
    drains its queues and exits cleanly — no skew-guard refusal, no restart loop.

    The 3886-restart crash-loop on the live deployment is exactly this path.
    Operator mode is used so the worker terminates on its own rather than the
    test having to police a long-running process.
    """
    dsn = dedicated_database
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(_FIXTURE_ROOT))
    monkeypatch.setenv("OKG_DSN", dsn)
    _provision(_FIXTURE_NAME, _FIXTURE_ROOT / _FIXTURE_NAME, dsn, capsys)
    rc = _bootstrap_runtime_with_disposable_toolchain(
        name=_FIXTURE_NAME,
        dsn=dsn,
    )
    bootstrapped = capsys.readouterr()
    assert rc == 0, (bootstrapped.out or bootstrapped.err)[-2000:]
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT to_regclass('okg.deployment_apply_runs')::text,
                   to_regclass('okg.deployment_worker_ack')::text
            """
        )
        runtime_relations = cur.fetchone()
    assert runtime_relations == (
        "okg.deployment_apply_runs",
        "okg.deployment_worker_ack",
    ), "runtime bootstrap left worker-owned schema for first launch"

    # Older deployments can already carry the DBOS bootstrap marker while
    # predating these two worker-bookkeeping relations. Recreate that exact
    # state and prove an idempotent bootstrap repairs it before worker launch.
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            DROP TABLE okg.deployment_worker_ack,
                       okg.deployment_apply_runs
            """
        )
    rc = _bootstrap_runtime_with_disposable_toolchain(
        name=_FIXTURE_NAME,
        dsn=dsn,
    )
    repaired = _json_payload(capsys)
    assert rc == 0, json.dumps(repaired, default=str)[:2000]
    assert repaired.get("already_bootstrapped") is True, repaired
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT to_regclass('okg.deployment_apply_runs')::text,
                   to_regclass('okg.deployment_worker_ack')::text
            """
        )
        repaired_relations = cur.fetchone()
    assert repaired_relations == runtime_relations

    # A subprocess, so a worker that never exits fails on the timeout instead of
    # hanging the suite, and so its real stdout/stderr can be inspected.
    result = subprocess.run(
        [sys.executable, "-m", "okg.substrate.cli.main", "--json", "runtime",
         "worker", "--deployment", _FIXTURE_NAME, "--dsn", dsn,
         "--operator", "--max-wait-seconds", "120"],
        cwd=_REPO_ROOT,
        env={**os.environ, "OKG_DEPLOYMENTS_DIR": str(_FIXTURE_ROOT),
             "OKG_DSN": dsn, "PYTHONPATH": str(_REPO_ROOT / "src")},
        # Bounded well under the lane's share of the job: a worker that never
        # exits must fail this arm, not consume the whole job's time budget.
        # Operator mode drains with its own 120s cap, so 180s is generous.
        capture_output=True, text=True, timeout=180,
    )
    output = (result.stdout or "") + (result.stderr or "")
    assert result.returncode == 0, output[-3000:]

    # It really launched DBOS and shut down under its own control.
    assert "DBOS launched" in output, output[-3000:]
    assert "successfully shut down" in output, output[-3000:]

    # No skew-guard refusal and no restart loop.
    lowered = output.lower()
    for marker in ("schema skew", "skew guard", "skew-guard", "traceback"):
        assert marker not in lowered, (marker, output[-3000:])
    assert lowered.count("dbos launched") == 1, (
        "worker launched more than once: restart loop", output[-3000:]
    )


@pytest.mark.postgres
def test_ordered_migrations_apply_to_populated_database_and_republish(
    tmp_path: Path,
    dedicated_database: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys,
) -> None:
    """Publish real rows, apply ordered migrations on top of that populated
    database, then publish again — the rows must survive and the pipeline must
    still reach a published generation."""
    dsn = dedicated_database
    name = f"e2ex{tmp_path.name.lower().replace('-', '')[-8:]}"
    target = _stage_fixture(tmp_path, dsn, capsys, name=name)
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(target.parent))

    first = _provision(name, target, dsn, capsys)
    assert first["publish_proof"]["status"] == "published", first
    assert first["publish_proof"]["nodes_added"] > 0, first

    before = _live_rows(dsn, "SELECT count(*) AS n FROM okg.graph_nodes")[0]["n"]
    assert before > 0, "fixture published no live nodes to migrate over"
    sample = _live_rows(
        dsn, "SELECT node_id FROM okg.graph_nodes ORDER BY node_id LIMIT 1"
    )[0]["node_id"]

    # The database now holds published rows. Replay the tail ordered migrations
    # against it — the real-world path a fresh-database test never covers.
    cleared = _clear_migration_ledger(dsn, _REPLAYED_MIGRATIONS)
    assert cleared == len(_REPLAYED_MIGRATIONS), (
        f"expected to clear {len(_REPLAYED_MIGRATIONS)} ledger rows, cleared {cleared}"
    )
    # Non-vacuity: the migrations are genuinely pending now. Without this the
    # post-migrate ledger assertion below would pass even if nothing replayed.
    assert _recorded_migrations(dsn) == set(), "ledger clear did not take effect"

    rc = main(["--json", "migrate", "--deployment", name, "--dsn", dsn, "--apply"])
    migrated = _json_out(capsys)
    assert rc == 0, migrated

    # The migration SQL really executed again, against populated data: the
    # runner records each migration in the same transaction that runs it, so
    # the rows can only be back if they ran.
    assert _recorded_migrations(dsn) == set(_REPLAYED_MIGRATIONS), migrated

    # The previously published rows survived the migration.
    survived = _live_rows(
        dsn,
        "SELECT count(*) AS n FROM okg.graph_nodes WHERE node_id = :nid",
        {"nid": sample},
    )
    assert survived[0]["n"] == 1, f"node {sample} did not survive the migration"

    # And the pipeline still publishes on the migrated, populated database.
    rc = main(["provision", "--deployment", name, "--dsn", dsn, "--json"])
    second = _json_out(capsys)
    assert rc == 0, second
    assert second["publish_proof"]["status"] == "published", second
    after = _live_rows(dsn, "SELECT count(*) AS n FROM okg.graph_nodes")[0]["n"]
    assert after >= before, (before, after)

    # Record the observed generations so the PACT's published_generation
    # evidence cites real ids from this run rather than a hand-written claim.
    _record_generation_evidence({
        "test": "test_ordered_migrations_apply_to_populated_database_and_republish",
        "deployment": name,
        "replayed_migrations": list(_REPLAYED_MIGRATIONS),
        "before_migration": {
            "generation_id": first["publish_proof"].get("generation_id"),
            "live_nodes": before,
        },
        "after_migration": {
            "generation_id": second["publish_proof"].get("generation_id"),
            "live_nodes": after,
        },
        "survived_node_id": sample,
    })
