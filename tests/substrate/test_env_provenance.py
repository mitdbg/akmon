"""Interpreter/environment provenance tests (issue #639).

NOTE ON FIDELITY: the real PATH-fallthrough trap of #639 — `uv run` falling
through to the globally installed uv tool shim because the checkout venv
lacked the `okg` console script — is NOT reproducible in-repo: this repo's
pyproject has shipped the `okg` entry point since the initial commit. The
uv-tool-shim classification is therefore simulated by monkeypatching
``sys.executable`` (and ``sys.prefix`` / ``sys.base_prefix``), which is the
exact signal the classifier reads in a real shim run.

Pure unit tests: no database, no network, no subprocess.
"""
from __future__ import annotations

import importlib.metadata
import sys
from pathlib import Path

import pytest

from okg.substrate.support.env_provenance import (
    PROJECT_VENV,
    REPO_COMMAND,
    SYSTEM_INTERPRETER,
    UV_TOOL_ENVIRONMENT,
    annotate_optional_dependency_error,
    classify_interpreter,
    find_checkout_root,
    interpreter_provenance_report,
    okg_version,
    repo_command_authority,
)


# The default uv tools layout (`~/.local/share/uv/tools/<tool>/bin/python`);
# the adjacent `uv/tools` segment pair is what the classifier keys on.
_UV_TOOL_EXE = "/home/example/.local/share/uv/tools/okg/bin/python"
_VENV_EXE = "/home/example/checkout/.venv/bin/python"
_VENV_PREFIX = "/home/example/checkout/.venv"
_SYSTEM_EXE = "/usr/bin/python3"


def _expected_okg_version() -> str | None:
    try:
        return importlib.metadata.version("okg")
    except importlib.metadata.PackageNotFoundError:
        return None


def _patch_live_interpreter(
    monkeypatch: pytest.MonkeyPatch,
    executable: str,
    *,
    prefix: str,
    base_prefix: str,
) -> None:
    """Simulate a foreign interpreter for code that reads sys at call time."""
    monkeypatch.setattr(sys, "executable", executable)
    monkeypatch.setattr(sys, "prefix", prefix)
    monkeypatch.setattr(sys, "base_prefix", base_prefix)
    monkeypatch.delenv("UV_TOOL_DIR", raising=False)


# ---------------------------------------------------------------------------
# classifier
# ---------------------------------------------------------------------------


def test_classifier_uv_tools_path_is_uv_tool_environment() -> None:
    executable, kind = classify_interpreter(
        _UV_TOOL_EXE,
        prefix="/home/example/.local/share/uv/tools/okg",
        base_prefix="/usr",
        env={},
    )
    assert executable == _UV_TOOL_EXE
    assert kind == UV_TOOL_ENVIRONMENT


def test_classifier_uv_tool_dir_env_var_is_uv_tool_environment() -> None:
    # A custom UV_TOOL_DIR without any `uv/tools` path segment.
    executable, kind = classify_interpreter(
        "/opt/toolenvs/okg/bin/python",
        prefix="/opt/toolenvs/okg",
        base_prefix="/usr",
        env={"UV_TOOL_DIR": "/opt/toolenvs"},
    )
    assert kind == UV_TOOL_ENVIRONMENT


def test_classifier_venv_with_base_prefix_mismatch_is_project_venv() -> None:
    executable, kind = classify_interpreter(
        _VENV_EXE,
        prefix=_VENV_PREFIX,
        base_prefix="/usr",
        env={},
    )
    assert executable == _VENV_EXE
    assert kind == PROJECT_VENV


def test_classifier_plain_system_path_is_system_interpreter() -> None:
    executable, kind = classify_interpreter(
        _SYSTEM_EXE,
        prefix="/usr",
        base_prefix="/usr",
        env={},
    )
    assert executable == _SYSTEM_EXE
    assert kind == SYSTEM_INTERPRETER


def test_classifier_reads_monkeypatched_sys_at_call_time(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_live_interpreter(
        monkeypatch,
        _UV_TOOL_EXE,
        prefix="/home/example/.local/share/uv/tools/okg",
        base_prefix="/usr",
    )
    executable, kind = classify_interpreter()
    assert executable == _UV_TOOL_EXE
    assert kind == UV_TOOL_ENVIRONMENT


# ---------------------------------------------------------------------------
# annotate_optional_dependency_error
# ---------------------------------------------------------------------------


def test_annotate_package_form_under_uv_tool_shim(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_live_interpreter(
        monkeypatch,
        _UV_TOOL_EXE,
        prefix="/home/example/.local/share/uv/tools/okg",
        base_prefix="/usr",
    )
    message, fix_hint, data = annotate_optional_dependency_error(
        "pymupdf (fitz) is not installed",
        "Install pymupdf.",
        package="pymupdf",
    )
    assert _UV_TOOL_EXE in message
    assert UV_TOOL_ENVIRONMENT in message
    assert "uv tool install okg --with pymupdf" in fix_hint
    assert "Original hint: Install pymupdf." in fix_hint
    assert data == {
        "interpreter": _UV_TOOL_EXE,
        "environment_kind": UV_TOOL_ENVIRONMENT,
    }


def test_annotate_passes_hint_through_off_the_shim(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_live_interpreter(
        monkeypatch,
        _SYSTEM_EXE,
        prefix="/usr",
        base_prefix="/usr",
    )
    message, fix_hint, data = annotate_optional_dependency_error(
        "gliner package not installed",
        "pip install 'okg[nlp]'",
        extras=("nlp",),
    )
    assert _SYSTEM_EXE in message
    assert SYSTEM_INTERPRETER in message
    assert fix_hint == "pip install 'okg[nlp]'"
    assert data["environment_kind"] == SYSTEM_INTERPRETER


# ---------------------------------------------------------------------------
# TreeSitterNotInstalled carries the provenance (req.envdiag.interpreter-in-errors)
# ---------------------------------------------------------------------------


def _force_pack_import_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """Force `from tree_sitter_language_pack import ...` to ImportError.

    A None entry in sys.modules makes the import fail whether or not the
    package is installed, so the missing-dep branch is exercised in every
    test environment.
    """
    monkeypatch.setitem(sys.modules, "tree_sitter_language_pack", None)


def test_treesitter_error_under_uv_tool_shim_names_both_fix_forms(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.library.extraction.code.treesitter import (
        TreeSitterNotInstalled,
        TreeSitterSymbolExtractor,
    )

    _patch_live_interpreter(
        monkeypatch,
        _UV_TOOL_EXE,
        prefix="/home/example/.local/share/uv/tools/okg",
        base_prefix="/usr",
    )
    _force_pack_import_error(monkeypatch)

    with pytest.raises(TreeSitterNotInstalled) as excinfo:
        TreeSitterSymbolExtractor(language="javascript")
    err = excinfo.value

    # Interpreter path + classified kind in the message.
    assert _UV_TOOL_EXE in err.message
    assert UV_TOOL_ENVIRONMENT in err.message
    # The hint names the shim resolution...
    assert "globally installed uv tool" in err.fix_hint
    assert "not the checkout venv" in err.fix_hint
    # ...and gives BOTH fix forms.
    assert 'uv tool install "okg[code,quality]"' in err.fix_hint
    assert "uv run --extra code --extra quality okg" in err.fix_hint
    # Structured payload for the JSON error envelope.
    assert err.data["interpreter"] == _UV_TOOL_EXE
    assert err.data["environment_kind"] == UV_TOOL_ENVIRONMENT
    assert err.data["language"] == "javascript"


def test_treesitter_error_under_project_venv_has_no_shim_warning(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.library.extraction.code.treesitter import (
        TreeSitterNotInstalled,
        TreeSitterSymbolExtractor,
    )

    _patch_live_interpreter(
        monkeypatch,
        _VENV_EXE,
        prefix=_VENV_PREFIX,
        base_prefix="/usr",
    )
    _force_pack_import_error(monkeypatch)

    with pytest.raises(TreeSitterNotInstalled) as excinfo:
        TreeSitterSymbolExtractor(language="javascript")
    err = excinfo.value

    # Interpreter + kind still named.
    assert _VENV_EXE in err.message
    assert PROJECT_VENV in err.message
    # The extras hint is present...
    assert "--extra code" in err.fix_hint
    # ...without the shim warning.
    assert "uv tool install" not in err.fix_hint
    assert "globally installed" not in err.fix_hint
    assert err.data["environment_kind"] == PROJECT_VENV


# ---------------------------------------------------------------------------
# repo command authority heuristic
# ---------------------------------------------------------------------------


def _write_checkout(root: Path) -> Path:
    (root / "pact").mkdir(parents=True)
    (root / "pact" / "project.yaml").write_text(
        "command_authority:\n  repo_command: uv run okg\n",
        encoding="utf-8",
    )
    return root


def test_repo_command_authority_matches_checkout_venv(tmp_path: Path) -> None:
    checkout = _write_checkout(tmp_path / "co")
    exe = str(checkout / ".venv" / "bin" / "python")
    authority = repo_command_authority(exe, cwd=checkout)
    assert authority["expected_command"] == REPO_COMMAND
    assert authority["in_checkout"] is True
    assert authority["checkout_root"] == str(checkout)
    assert authority["matches"] is True


def test_repo_command_authority_mismatch_for_foreign_interpreter(
    tmp_path: Path,
) -> None:
    checkout = _write_checkout(tmp_path / "co")
    authority = repo_command_authority(_UV_TOOL_EXE, cwd=checkout)
    assert authority["in_checkout"] is True
    assert authority["matches"] is False


def test_repo_command_authority_outside_checkout_is_none(
    tmp_path: Path,
) -> None:
    assert find_checkout_root(tmp_path) is None
    authority = repo_command_authority(_SYSTEM_EXE, cwd=tmp_path)
    assert authority["in_checkout"] is False
    assert authority["checkout_root"] is None
    assert authority["matches"] is None


# ---------------------------------------------------------------------------
# env doctor block (req.envdiag.provenance-surface)
# ---------------------------------------------------------------------------


def _write_env_deployment(root: Path, name: str = "demo") -> Path:
    """Minimal loader-valid deployment dir (mirrors the fixture shape in
    tests/substrate/cli/test_deployment_env_doctor.py)."""
    deployment_dir = root / name
    deployment_dir.mkdir(parents=True)
    (deployment_dir / "schemas").mkdir()
    (deployment_dir / "source_registry.yaml").write_text(
        "sources: {}\n",
        encoding="utf-8",
    )
    (deployment_dir / "deployment.yaml").write_text(
        "name: demo\n"
        "postgres:\n"
        "  dsn: ${OKG_DSN}\n"
        "  database: okg_demo\n"
        "schema_dir: schemas\n"
        "source_registry: source_registry.yaml\n",
        encoding="utf-8",
    )
    return deployment_dir


def test_doctor_block(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`okg deployment env doctor` gains the interpreter-provenance block
    (simulated uv tool shim), in both JSON payload and human summary."""
    from okg.substrate.deployment_env_doctor import (
        build_env_doctor_report,
        format_env_doctor_summary,
    )

    _patch_live_interpreter(
        monkeypatch,
        _UV_TOOL_EXE,
        prefix="/home/example/.local/share/uv/tools/okg",
        base_prefix="/usr",
    )
    # Pin cwd off any real checkout so the authority verdict is
    # deterministic regardless of where pytest was launched.
    monkeypatch.chdir(tmp_path)

    deployment_dir = _write_env_deployment(tmp_path)
    report = build_env_doctor_report(
        deployment_dir,
        local_env_path=tmp_path / "local.env",
        env={"OKG_DSN": "postgres://op:pw@localhost:5432/okg_demo"},
        for_command="read",
        include_worktree_main_fallback=False,
    )

    block = report["interpreter"]
    assert block["executable"] == _UV_TOOL_EXE
    assert block["environment_kind"] == UV_TOOL_ENVIRONMENT
    assert block["okg_version"] == _expected_okg_version()
    authority = block["repo_command_authority"]
    assert authority["expected_command"] == REPO_COMMAND
    assert authority["in_checkout"] is False
    assert authority["matches"] is None

    # The block renders in the human summary too.
    text = format_env_doctor_summary(report)
    assert f"interpreter: {_UV_TOOL_EXE} ({UV_TOOL_ENVIRONMENT}" in text
    assert "repo command authority: n/a" in text

    # Appended, not restructured: the existing envelope keys are intact.
    for key in (
        "schema",
        "authority",
        "postgres",
        "findings",
        "recommended_commands",
        "command_contract",
    ):
        assert key in report


def test_doctor_block_reports_live_interpreter(tmp_path: Path) -> None:
    """Unpatched smoke run: the block describes the interpreter actually
    running the doctor (under `uv run` from the repo, a project venv)."""
    from okg.substrate.deployment_env_doctor import build_env_doctor_report

    deployment_dir = _write_env_deployment(tmp_path)
    report = build_env_doctor_report(
        deployment_dir,
        local_env_path=tmp_path / "local.env",
        env={"OKG_DSN": "postgres://op:pw@localhost:5432/okg_demo"},
        for_command="read",
        include_worktree_main_fallback=False,
    )
    block = report["interpreter"]
    assert block["executable"] == sys.executable
    assert block["environment_kind"] in {
        UV_TOOL_ENVIRONMENT,
        PROJECT_VENV,
        SYSTEM_INTERPRETER,
    }
    assert block["okg_version"] == _expected_okg_version()
    assert block["repo_command_authority"]["expected_command"] == REPO_COMMAND


def test_doctor_block_present_even_when_manifest_missing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Early-return reports (deployment not found) still carry the block —
    exactly the confused-environment case the surface exists for."""
    from okg.substrate.deployment_env_doctor import build_env_doctor_report

    monkeypatch.chdir(tmp_path)
    report = build_env_doctor_report(
        tmp_path / "no-such-deployment",
        env={},
        for_command="read",
        include_worktree_main_fallback=False,
    )
    assert report["ok"] is False
    assert report["interpreter"]["executable"] == sys.executable
    assert "environment_kind" in report["interpreter"]


def test_okg_version_matches_importlib_metadata() -> None:
    assert okg_version() == _expected_okg_version()


def test_interpreter_provenance_report_composes_injected_values(
    tmp_path: Path,
) -> None:
    checkout = _write_checkout(tmp_path / "co")
    exe = str(checkout / ".venv" / "bin" / "python")
    block = interpreter_provenance_report(
        executable=exe,
        prefix=str(checkout / ".venv"),
        base_prefix="/usr",
        env={},
        cwd=checkout,
    )
    assert block["executable"] == exe
    assert block["environment_kind"] == PROJECT_VENV
    assert block["repo_command_authority"]["matches"] is True
