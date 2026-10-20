"""Repo-wide pytest fixtures.

Per `raise-substrate-coverage`:
- `stub_llm_factory` — wraps the existing
  `okg.views.execution.llm.DeterministicStubLLM` for substrate
  tests that exercise LLM-touching code via a `client_factory`
  injection point (e.g. `InvariantsLLMRunner.__init__`).

Substrate-specific fixtures continue to live in
`tests/substrate/conftest.py`.
"""
from __future__ import annotations

import contextlib
import json
import os
import sys
import tempfile
from pathlib import Path

import pytest


# Issue #3273: okg no longer derives the built-in (public) database role
# passwords unless OKG_ALLOW_BUILTIN_ROLE_PASSWORDS=1 marks a single-user
# machine. A disposable test server is exactly that, and the suite's MCP
# fixtures log in as okg_mcp / app_rw with those passwords. Tests of the
# refusal unset the variable themselves. The role secrets file is pointed at a
# per-process path that does not exist, so a developer's real file under
# ~/.config/okg/role-secrets can never leak into, or be written by, a test.
# The directory comes from mkdtemp (0700, unpredictable name), so another
# account on a shared host cannot plant a symlink at the path.
os.environ.setdefault("OKG_ALLOW_BUILTIN_ROLE_PASSWORDS", "1")
os.environ.setdefault(
    "OKG_ROLE_SECRETS_FILE",
    str(Path(tempfile.mkdtemp(prefix="okg-test-role-secrets-")) / "role-secrets.json"),
)


SUBSTRATE_AREA_COVERAGE_FLOORS = {
    "src/okg/substrate/catalog/": 75.0,
    "src/okg/substrate/cli/main.py": 60.0,
    "src/okg/substrate/mcp/": 80.0,
    "src/okg/substrate/library/enrichers/": 70.0,
    "src/okg/substrate/library/sources/_http_cache.py": 75.0,
}


@pytest.fixture(autouse=True)
def _hermetic_runtime_worker_audit(request, monkeypatch):
    """Keep the dogfood worker-process audit off the host's process table.

    `_status_payload` resolves the runtime process audit as::

        _mapping(live.get("runtime_processes")) or _runtime_worker_process_audit(...)

    and that fallback shells out to `ps -axo`. In production the caller always
    supplies `runtime_processes` (see `deployment.py`), so the fallback is a
    safety net. In a unit test that builds a synthetic `live` payload it is a
    HOST DEPENDENCY: whether the assertion passes depends on whether an
    `okg runtime worker` happens to be running on the machine.

    That is how ten tests went red on dev at once. `codex/dogfood-runtime-capacity`
    correctly tightened trust so an unobservable worker blocks readiness
    (fail-closed — an agent must not trust a graph whose publisher may be
    dead). The production behaviour is right; the tests were simply no longer
    hermetic, and CI runners have no worker running.

    Default: report one healthy executor, so tests that say nothing about
    worker processes assert what they meant to. A test that genuinely
    exercises worker-process handling opts out with
    ``@pytest.mark.real_worker_audit`` and supplies its own `runtime_processes`
    or stub.
    """
    if request.node.get_closest_marker("real_worker_audit"):
        return
    try:
        from okg.substrate.cli import dogfood
    except Exception:  # pragma: no cover - dogfood not importable in this env
        return
    monkeypatch.setattr(
        dogfood,
        "_runtime_worker_process_audit",
        lambda deployment: {
            "ok": True,
            "status": "single_executor",
            "split_executor": False,
            "root_count": 1,
            "process_count": 1,
            "roots": [{"pid": 1, "ppid": 1, "command": f"okg runtime worker --deployment {deployment}"}],
            "processes": [{"pid": 1, "ppid": 1, "command": f"okg runtime worker --deployment {deployment}"}],
            "hermetic_stub": True,
        },
        raising=False,
    )

@pytest.fixture(autouse=True)
def _no_live_decision_ledger(monkeypatch: pytest.MonkeyPatch) -> None:
    """Tests never read the live PACT decision ledger implicitly.

    PACTSource projects ledger decisions by default; without this guard
    every source test would try the deployment DSN (nondeterministic
    watermarks locally, connection noise in CI). Tests that exercise the
    projection set OKG_PACT_DECISION_PROJECTION=on explicitly.
    """
    monkeypatch.setenv("OKG_PACT_DECISION_PROJECTION", "off")


def _module_origin_under_deployments(module: object) -> bool:
    """True if ``module`` was loaded from a ``deployments/`` tree."""
    origins: list[str] = []
    file_attr = getattr(module, "__file__", None)
    if file_attr:
        origins.append(file_attr)
    origins.extend(getattr(module, "__path__", []) or [])
    for origin in origins:
        try:
            if "deployments" in Path(origin).resolve().parts:
                return True
        except (OSError, ValueError):
            continue
    return False


def _evict_cross_deployment_module_caches() -> None:
    """Drop cached deployment-local packages from ``sys.modules``.

    The deployment-local top-level package names (``views``, ``enrichers``,
    ``sources``, ...) are SHARED across deployments, so ``sys.modules['views']``
    gets pinned to whichever deployment imported it first and a later module's
    ``import views.X`` returns that stale, foreign package. Evicting only the
    deployment-rooted ones (never anything installed/site-packages) forces the
    next module's deployment-local imports to re-resolve from its own dir.
    """
    from okg.substrate.deployments import _DEPLOYMENT_LOCAL_MODULE_PREFIXES

    stale = {
        prefix
        for prefix in _DEPLOYMENT_LOCAL_MODULE_PREFIXES
        if (module := sys.modules.get(prefix)) is not None
        and _module_origin_under_deployments(module)
    }
    if not stale:
        return
    for name in list(sys.modules):
        if name in stale or any(
            name.startswith(f"{prefix}.") for prefix in stale
        ):
            sys.modules.pop(name, None)


@pytest.fixture(autouse=True, scope="module")
def _isolate_deployment_module_caches():
    """Contain cross-deployment ``sys.modules`` pollution at module boundaries.

    Many tests under ``tests/deployments``, ``tests/okg-workspace``,
    ``tests/views``, ... put their deployment dir on ``sys.path`` (often at
    import time) and import deployment-local packages. Under xdist one worker
    collects many such modules, so a deployment-local package cached by one
    test module poisons a later, unrelated module's import of the same
    top-level name (e.g. the source-sync replay benchmarks getting a foreign
    ``views`` with no ``VIEW_REGISTRY``).

    Evicting those caches at each module's setup and teardown forces every
    module's deployment-local imports to re-resolve from its own dir. This only
    touches ``sys.modules`` — never ``sys.path`` — so already-bound names
    survive and modules that legitimately need their deployment dir keep it.
    The companion durability fix lives in
    ``deployment_wiring._module_spec_available`` (a leaked dir can no longer
    satisfy a dotted ref via tail-only ``PathFinder`` matching).
    """
    _evict_cross_deployment_module_caches()
    yield
    _evict_cross_deployment_module_caches()


# --------------------------------------------------------------------------
# Shared-module eviction guard.
# --------------------------------------------------------------------------
# Evicting an already-imported module from `sys.modules` and letting it be
# imported again leaves the process holding TWO copies of it. Aliases bound by
# the first copy no longer match the objects the second copy produces, so
# `except` clauses and `isinstance` checks elsewhere silently stop matching --
# for the rest of the session, in tests that never touched the module. The
# canonical case is `json.decoder`: `json/__init__` binds `JSONDecodeError`
# from the first copy, while the C scanner resolves it from
# `sys.modules["json.decoder"]` at raise time, so `except json.JSONDecodeError`
# stops catching malformed JSON everywhere.
#
# This hook names the test that did it, at the moment it did it, instead of
# leaving the damage to surface as unrelated failures thousands of tests later.
#
# Deployment-local packages (`views`, `enrichers`, `sources`, ...) are
# deliberately evicted at every module boundary by
# `_evict_cross_deployment_module_caches` above -- they are per-deployment by
# design, so two copies is the intended behaviour, and they are not guarded.
_GUARDED_MODULES = (
    "json",
    "json.decoder",
    "json.encoder",
    "json.scanner",
    "logging",
    "pathlib",
    "re",
    "subprocess",
    "yaml",
)
# Snapshotted LAZILY, at the first setup/teardown at which each name is
# present. A name absent at snapshot time is NOT "nothing to guard" -- it is
# "not observed yet", a distinct state, so it stays eligible. Snapshotting
# eagerly at configure time instead would have silently un-guarded every name
# first imported later in the session (`yaml` is imported on demand), leaving
# the guard reporting clean having checked nothing. Recording at SETUP as well
# as teardown is what closes the symmetric hole: baselining only at teardown
# would record a name for the first time from the very test that evicted it,
# and read the swap as the baseline.
_MODULE_IDENTITY: dict[str, object] = {}


def _record_unseen_modules() -> None:
    for name in _GUARDED_MODULES:
        if name in _MODULE_IDENTITY:
            continue
        current = sys.modules.get(name)
        if current is not None:
            _MODULE_IDENTITY[name] = current


def pytest_runtest_setup(item: pytest.Item) -> None:
    _record_unseen_modules()


def _restore_module(name: str, original: object) -> None:
    """Put `original` back in `sys.modules` AND on its parent package.

    Restoring only `sys.modules[name]` leaves `parent.leaf` pointing at the
    orphan copy, so `from json.decoder import JSONDecodeError` and
    `isinstance(exc, json.decoder.JSONDecodeError)` still see the wrong class.
    """
    sys.modules[name] = original  # type: ignore[assignment]
    parent_name, _, leaf = name.rpartition(".")
    if parent_name:
        parent = sys.modules.get(parent_name)
        if parent is not None and getattr(parent, leaf, None) is not original:
            setattr(parent, leaf, original)


class _ConnectionReservationLeakReporter:
    """Release every connection reservation a test still holds before its
    fixtures tear down, and name the test if nothing else asked for it.

    An MCP server releases its own claim when its read pool closes, and
    `run_server` releases it on shutdown (issue #2280). A test that builds a
    server in-process and never closes the pool leaves a claim that a
    heartbeat keeps live for the rest of the worker process. Two such claims
    under different names are enough to refuse the next MCP start with
    BudgetError, which is how hosted CI failed on #2359.

    Order matters. The release runs BEFORE any fixture finalizer, because a
    finalizer may drop or NOLOGIN the login the claim was made with; after
    that the release could not connect and the row would stay in the shared
    database until the 300 s stale sweep. The warning is decided AFTER the
    finalizers (in a ``finally``, so it still runs when a finalizer raises):
    a test whose own fixture asked to release that claim -- by closing the
    server's pool, calling ``release_process_reservations`` or
    ``_release_all_process_reservations`` -- is not reported.

    It warns rather than fails because ten existing test files still build
    servers without closing their pools; issue #2430 tracks them. Once they
    close their pools, this warning becomes a failure. It does nothing
    unless the test process imported db/capacity.py.
    """

    @pytest.hookimpl(wrapper=True)
    def pytest_runtest_teardown(self, item: pytest.Item, nextitem):
        capacity = sys.modules.get("okg.substrate.db.capacity")
        leaked: list[str] = []
        if capacity is not None:
            leaked = capacity._process_reservation_names()
            with capacity._RESERVATION_OWNERS_LOCK:
                capacity._RELEASE_REQUESTS.clear()
            if leaked:
                capacity._release_owned_reservations()
        try:
            return (yield)
        finally:
            if leaked:
                with capacity._RESERVATION_OWNERS_LOCK:
                    requested = set(capacity._RELEASE_REQUESTS)
                unreleased = [
                    name for name in leaked
                    if name not in requested and "*" not in requested
                ]
                if unreleased:
                    item.warn(
                        pytest.PytestWarning(
                            f"{item.nodeid} left live connection "
                            f"reservation(s) {unreleased}: close the MCP "
                            "server's read pool (server._okg_pool.close()), "
                            "call server._okg_release_reservation(), or "
                            "release the claim in the test's own teardown."
                        )
                    )


def pytest_configure(config: pytest.Config) -> None:
    config.pluginmanager.register(
        _ConnectionReservationLeakReporter(),
        "okg-connection-reservation-leak-reporter",
    )


@pytest.hookimpl(trylast=True)
def pytest_runtest_teardown(item: pytest.Item) -> None:
    swapped = [
        name
        for name, original in _MODULE_IDENTITY.items()
        if sys.modules.get(name) is not original
    ]
    _record_unseen_modules()
    if not swapped:
        return
    # Restore, so exactly one test fails instead of every later one.
    for name in swapped:
        _restore_module(name, _MODULE_IDENTITY[name])
    raise AssertionError(
        f"{item.nodeid} left {swapped} replaced in sys.modules. Evicting an "
        "already-imported module gives the process two copies of it, which "
        "silently breaks `except`/`isinstance` against its classes elsewhere. "
        "Use a throwaway module written to tmp_path as the import target "
        "instead, or restore the original object before the test returns."
    )


_OPTIONAL_DEPLOYMENT_TEST_ROOTS = {
    "memex": {
        Path("tests") / "acceptance" / "personal_okg",
        Path("tests") / "integration" / "personal_okg",
        Path("tests") / "memex",
    },
}


def pytest_ignore_collect(collection_path: Path, config: pytest.Config) -> bool:
    """Ignore deployment-specific tests when that deployment is absent."""
    root = Path(str(config.rootpath))
    path = Path(collection_path)
    try:
        rel = path.relative_to(root)
    except ValueError:
        return False

    for deployment, test_roots in _OPTIONAL_DEPLOYMENT_TEST_ROOTS.items():
        if (root / "deployments" / deployment).exists():
            continue
        if any(
            rel == test_root or rel.is_relative_to(test_root)
            for test_root in test_roots
        ):
            return True
    return False


@pytest.fixture
def stub_llm_factory():
    """Return a `client_factory(model, dsn, deployment) -> LLMClient`
    that yields `DeterministicStubLLM` instances.

    Used by tests that exercise LLM-touching code without making
    network calls. The stub's `complete(system, user, ...)` returns
    a deterministic hash of the inputs, so byte-stable tests are
    possible without committing to LLM byte-identity claims in
    production.
    """
    from okg.views.execution.llm import DeterministicStubLLM

    def _factory(model: str, dsn: str, deployment: str):
        return DeterministicStubLLM()

    return _factory


def _coverage_run_enabled(config) -> bool:
    cov_sources = getattr(config.option, "cov_source", None) or []
    return bool(cov_sources) and not getattr(config.option, "no_cov", False)


def _coverage_json_report_paths(config) -> list[Path]:
    cov_report = getattr(config.option, "cov_report", {}) or {}
    if not isinstance(cov_report, dict) or "json" not in cov_report:
        return []

    json_dest = cov_report.get("json") or "coverage.json"
    candidates: list[Path] = []
    candidates.append(Path(json_dest))

    root = Path(str(config.rootpath))
    resolved: list[Path] = []
    seen: set[Path] = set()
    for candidate in candidates:
        path = candidate if candidate.is_absolute() else root / candidate
        if path not in seen:
            seen.add(path)
            resolved.append(path)
    return resolved


def _coverage_key(path: str, root: Path) -> str:
    candidate = Path(path)
    if candidate.is_absolute():
        with contextlib.suppress(ValueError):
            candidate = candidate.relative_to(root)
    return candidate.as_posix()


def _coverage_run_requires_all_area_floors(config) -> bool:
    cov_sources = getattr(config.option, "cov_source", None) or []
    normalized = {str(source) for source in cov_sources if source is not True}
    return True in cov_sources or bool(
        normalized
        & {
            "okg",
            "okg.substrate",
            "src/okg/substrate",
            "src/okg/substrate/",
        }
    )


def _coverage_area_totals(
    coverage_json: dict,
    *,
    area: str,
    root: Path,
) -> tuple[int, int] | None:
    covered = 0
    statements = 0
    for file_path, file_data in coverage_json.get("files", {}).items():
        key = _coverage_key(file_path, root)
        if key == area or key.startswith(area):
            summary = file_data.get("summary", {})
            covered += int(summary.get("covered_lines", 0) or 0)
            statements += int(summary.get("num_statements", 0) or 0)
    if statements == 0:
        return None
    return covered, statements


def _substrate_area_coverage_failures(
    coverage_json: dict,
    *,
    root: Path,
    require_all: bool,
) -> list[str]:
    failures: list[str] = []
    for area, floor in SUBSTRATE_AREA_COVERAGE_FLOORS.items():
        totals = _coverage_area_totals(coverage_json, area=area, root=root)
        if totals is None:
            if require_all:
                failures.append(f"{area}: missing from coverage report")
            continue
        covered, statements = totals
        percent = (covered / statements) * 100
        if percent < floor:
            failures.append(
                f"{area}: {percent:.2f}% below {floor:.0f}% "
                f"({covered}/{statements} lines)"
            )
    return failures


def pytest_sessionstart(session) -> None:
    if not _coverage_run_enabled(session.config):
        return

    for path in _coverage_json_report_paths(session.config):
        path.unlink(missing_ok=True)


def pytest_sessionfinish(session, exitstatus) -> None:
    if not _coverage_run_enabled(session.config):
        return

    coverage_path = next(
        (path for path in _coverage_json_report_paths(session.config) if path.exists()),
        None,
    )
    if coverage_path is None:
        return

    coverage_json = json.loads(coverage_path.read_text())
    failures = _substrate_area_coverage_failures(
        coverage_json,
        root=Path(str(session.config.rootpath)),
        require_all=_coverage_run_requires_all_area_floors(session.config),
    )
    if not failures:
        return

    reporter = session.config.pluginmanager.getplugin("terminalreporter")
    if reporter is not None:
        reporter.write_sep("=", "substrate area coverage floors")
        for failure in failures:
            reporter.write_line(f"FAILED {failure}", red=True)
    session.exitstatus = pytest.ExitCode.TESTS_FAILED
