"""Regression tests for substrate coverage-floor pytest hooks."""
from __future__ import annotations

import importlib.util
from pathlib import Path
from types import SimpleNamespace


_ROOT_CONFTEST = Path(__file__).with_name("conftest.py")
_SPEC = importlib.util.spec_from_file_location(
    "root_pytest_conftest",
    _ROOT_CONFTEST,
)
assert _SPEC is not None and _SPEC.loader is not None
root_conftest = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(root_conftest)


class _PluginManager:
    def getplugin(self, name: str):
        return None


def _config(tmp_path, *, cov_source: list[str]):
    return SimpleNamespace(
        option=SimpleNamespace(
            cov_source=cov_source,
            cov_report={"json": str(tmp_path / "coverage.json")},
            no_cov=False,
        ),
        rootpath=tmp_path,
        pluginmanager=_PluginManager(),
    )


def _session(config):
    return SimpleNamespace(config=config, exitstatus=0)


def _write_failing_coverage_json(path) -> None:
    path.write_text(
        """
        {
          "files": {
            "src/okg/substrate/cli/main.py": {
              "summary": {
                "covered_lines": 0,
                "num_statements": 10
              }
            }
          }
        }
        """
    )


def test_stale_coverage_json_is_ignored_without_cov_source(tmp_path) -> None:
    config = _config(tmp_path, cov_source=[])
    stale_json = tmp_path / "coverage.json"
    _write_failing_coverage_json(stale_json)
    session = _session(config)

    root_conftest.pytest_sessionfinish(session, exitstatus=0)

    assert stale_json.exists()
    assert session.exitstatus == 0


def test_coverage_run_unlinks_configured_json_at_session_start(tmp_path) -> None:
    config = _config(tmp_path, cov_source=["okg.substrate"])
    stale_json = tmp_path / "coverage.json"
    _write_failing_coverage_json(stale_json)

    root_conftest.pytest_sessionstart(_session(config))

    assert not stale_json.exists()
