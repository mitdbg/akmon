"""Regression test: every CLI module registers ``--dsn`` /
``--deployment`` / ``--json`` / ``--generation`` through the shared
helpers in ``okg.substrate.cli._options``, and every output goes
through ``emit_result`` from ``okg.substrate.cli._output``. The
pre-cleanup state had 44 + 37 + 30+ duplicate ``add_argument`` calls
across ``cli/main.py``, ``cli/federation.py``, ``cli/poros.py``, and
``cli/nomos.py``; this test catches any reintroduction of that
pattern.

Pinned by ``extract-cli-shared-options-and-output``. The pre-existing
read-side module ``cli/_read_side.py`` is exempt — its specialized
nested-list pagination output and its lighter-weight DSN resolver
live there by design.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[3]
CLI_ROOT = REPO_ROOT / "src/okg/substrate/cli"

# Modules that MUST go through the shared helpers.
MIGRATED_MODULES = (
    "main.py",
    "federation.py",
    "poros.py",
    "nomos.py",
)

# Shared-helper modules. Their own registration patterns are exempt
# (they are the source of truth).
SHARED_HELPER_MODULES = ("_options.py", "_output.py")


# Forbidden patterns: any of these means a CLI module re-rolled the
# canonical option registration and bypassed the helper.
FORBIDDEN_DSN = re.compile(r'\.add_argument\("--dsn"')
FORBIDDEN_DEPLOYMENT = re.compile(r'\.add_argument\("--deployment"')
FORBIDDEN_JSON_FLAG = re.compile(
    r'\.add_argument\("--json", action="store_true"'
)


@pytest.mark.parametrize("module_name", MIGRATED_MODULES)
def test_no_module_re_registers_dsn(module_name: str) -> None:
    path = CLI_ROOT / module_name
    text = path.read_text()
    offenders = [
        f"{module_name}:{i + 1}"
        for i, line in enumerate(text.splitlines())
        if FORBIDDEN_DSN.search(line)
    ]
    assert offenders == [], (
        f"`--dsn` is registered via add_dsn_arg in cli/_options.py; "
        f"the following modules re-roll the registration: {offenders}"
    )


@pytest.mark.parametrize("module_name", MIGRATED_MODULES)
def test_no_module_re_registers_deployment(module_name: str) -> None:
    path = CLI_ROOT / module_name
    text = path.read_text()
    offenders = [
        f"{module_name}:{i + 1}"
        for i, line in enumerate(text.splitlines())
        if FORBIDDEN_DEPLOYMENT.search(line)
    ]
    assert offenders == [], (
        f"`--deployment` is registered via add_deployment_arg in "
        f"cli/_options.py; the following modules re-roll the "
        f"registration: {offenders}"
    )


@pytest.mark.parametrize("module_name", MIGRATED_MODULES)
def test_no_module_re_registers_json_flag(module_name: str) -> None:
    path = CLI_ROOT / module_name
    text = path.read_text()
    offenders = [
        f"{module_name}:{i + 1}"
        for i, line in enumerate(text.splitlines())
        if FORBIDDEN_JSON_FLAG.search(line)
    ]
    assert offenders == [], (
        f"`--json` action='store_true' is registered via add_json_flag "
        f"in cli/_options.py; the following modules re-roll the "
        f"registration: {offenders}"
    )


def test_shared_helpers_are_importable() -> None:
    """A smoke test that ``_options`` and ``_output`` are loadable
    and export the documented surface.
    """
    from okg.substrate.cli import _options, _output

    assert hasattr(_options, "add_dsn_arg")
    assert hasattr(_options, "add_deployment_arg")
    assert hasattr(_options, "add_json_flag")
    assert hasattr(_options, "add_generation_arg")
    assert hasattr(_options, "resolve_dsn")
    assert hasattr(_output, "emit_result")


def test_resolve_dsn_errors_loudly_on_missing_input(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Without ``--dsn``, ``--deployment``, or ``OKG_DSN``,
    ``resolve_dsn`` MUST raise a clear error rather than silently
    returning a default. This pins the no-silent-fallback rule.
    """
    import argparse

    from okg.substrate.cli._options import resolve_dsn

    monkeypatch.delenv("OKG_DSN", raising=False)
    monkeypatch.delenv("OKG_DEPLOYMENT", raising=False)
    monkeypatch.delenv("OKG_TEST_DSN", raising=False)

    args = argparse.Namespace(dsn=None, deployment=None)
    with pytest.raises(RuntimeError, match=r"--dsn|OKG_DSN"):
        resolve_dsn(args)


def test_resolve_dsn_does_not_honor_okg_test_dsn(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Wave-1 cleanup removed the ``OKG_TEST_DSN`` silent fallback.
    Setting it MUST NOT satisfy ``resolve_dsn``.
    """
    import argparse

    from okg.substrate.cli._options import resolve_dsn

    monkeypatch.delenv("OKG_DSN", raising=False)
    monkeypatch.delenv("OKG_DEPLOYMENT", raising=False)
    monkeypatch.setenv("OKG_TEST_DSN", "postgres://should-not-leak")

    args = argparse.Namespace(dsn=None, deployment=None)
    with pytest.raises(RuntimeError, match=r"--dsn|OKG_DSN"):
        resolve_dsn(args)
