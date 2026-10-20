"""Fail a coverage run in which an xdist worker returned no coverage data.

PACT nightly-coverage-full-shards. pytest-cov collects each xdist worker's
coverage data when the worker shuts down. A worker that dies after its last
test (in teardown, or while pytest-cov saves) leaves no failed test and no
JUnit marker. pytest-cov records it in its controller's ``failed_workers`` and
prints "The following workers failed to return coverage data" only in its
terminal summary, which a coverage-full leg (``--cov-report=`` with no report)
never prints. The leg's data would then be partial with a green exit code.

Loaded by ``scripts/qa.py`` for each coverage-full leg (``-p
okg.pytest_cov_workers``). At the end of the session on the xdist controller:

* a worker that returned no data is named, and an exit status of 0 or 1 becomes
  ``pytest.ExitCode.INTERNAL_ERROR`` (3), which the leg's completion receipt
  (``scripts/ci/coverage_full_combine.py receipt``) counts as a run that did
  not complete. Any other exit status (2 interrupted, 3, 4, 5) already means
  the run did not complete and is kept as it is;
* when coverage was requested on an xdist controller but pytest-cov's
  controller or its ``failed_workers`` list cannot be found (pytest-cov renamed
  them in an upgrade), the check refuses with exit status 4
  (``pytest.ExitCode.USAGE_ERROR``) instead of passing silently.

Elsewhere (no ``--cov``, ``--no-cov``, no xdist, or inside a worker process) it
does nothing.
"""

from __future__ import annotations

from typing import Any

import pytest

EXIT_STATUS = pytest.ExitCode.INTERNAL_ERROR
CONTRACT_EXIT_STATUS = pytest.ExitCode.USAGE_ERROR
COMPLETED_STATUSES = (pytest.ExitCode.OK, pytest.ExitCode.TESTS_FAILED)
MESSAGE = "xdist workers returned no coverage data"


class CoverageContractError(RuntimeError):
    """pytest-cov no longer exposes what this check reads."""


def _coverage_on_xdist_controller(config: Any) -> bool:
    option = getattr(config, "option", None)
    requested = bool(getattr(option, "cov_source", None)) and not getattr(
        option, "no_cov", False
    )
    return requested and config.pluginmanager.hasplugin("dsession")


def failed_worker_names(config: Any) -> list[str]:
    """The ids of workers pytest-cov recorded as returning no data.

    Raises CoverageContractError when coverage runs on an xdist controller and
    pytest-cov's controller is not the DistMaster this check reads.
    """

    if not _coverage_on_xdist_controller(config):
        return []
    plugin = config.pluginmanager.getplugin("_cov")
    if plugin is None or getattr(plugin, "_disabled", False):
        raise CoverageContractError(
            "coverage was requested on an xdist controller, but pytest-cov's "
            "'_cov' plugin is not registered or is disabled"
        )
    try:
        from pytest_cov.engine import DistMaster
    except ImportError as error:
        raise CoverageContractError(f"pytest_cov.engine.DistMaster: {error}") from error
    controller = getattr(plugin, "cov_controller", None)
    if not isinstance(controller, DistMaster):
        raise CoverageContractError(
            f"pytest-cov's controller is {type(controller).__name__}, not DistMaster"
        )
    if not hasattr(controller, "failed_workers"):
        raise CoverageContractError("pytest-cov's DistMaster has no failed_workers")
    return [
        str(getattr(getattr(node, "gateway", None), "id", node))
        for node in controller.failed_workers
    ]


def _write(session: pytest.Session, line: str) -> None:
    reporter = session.config.pluginmanager.getplugin("terminalreporter")
    if reporter is not None:
        reporter.write_line(line, red=True)
    else:
        print(line)


@pytest.hookimpl(trylast=True)
def pytest_sessionfinish(session: pytest.Session, exitstatus: int) -> None:
    try:
        names = failed_worker_names(session.config)
    except CoverageContractError as error:
        _write(
            session,
            f"okg.pytest_cov_workers cannot check for lost coverage data: {error}",
        )
        session.exitstatus = CONTRACT_EXIT_STATUS
        return
    if not names:
        return
    _write(
        session,
        f"{MESSAGE}: {', '.join(names)}; this run's coverage data may be partial",
    )
    if session.exitstatus in COMPLETED_STATUSES:
        session.exitstatus = EXIT_STATUS
