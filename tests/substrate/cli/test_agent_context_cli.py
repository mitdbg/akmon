"""Non-DB coverage for retired agent-context helper internals."""
from __future__ import annotations


def test_agent_context_ranks_explicit_file_before_session_files() -> None:
    from okg.substrate.agent_context import _rank_files

    primary, secondary, related = _rank_files(
        explicit_file={
            "node_id": "file:okg:src/okg/substrate/agent_context.py",
            "attrs": {"path": "src/okg/substrate/agent_context.py"},
        },
        explicit_path="src/okg/substrate/agent_context.py",
        session_files=[
            {
                "node_id": "file:okg:deployments/okg-workspace/doctor_checks.py",
                "path": "deployments/okg-workspace/doctor_checks.py",
                "source": "related_session",
            },
        ],
        limit=10,
    )

    assert primary == [
        {
            "node_id": "file:okg:src/okg/substrate/agent_context.py",
            "path": "src/okg/substrate/agent_context.py",
            "source": "explicit_file",
            "confidence": "direct",
            "reason": "file was explicitly requested and matched graph evidence",
        }
    ]
    assert secondary[0]["path"] == "deployments/okg-workspace/doctor_checks.py"
    assert secondary[0]["confidence"] == "historical"
    assert related == [*primary, *secondary]


def test_agent_context_symbol_line_falls_back_to_node_id_suffix() -> None:
    from okg.substrate.agent_context import _symbol_line_start

    assert _symbol_line_start(
        "symbol:okg:deployments/okg-workspace/doctor_checks.py:"
        "_okg_workspace_doctor_payload:53",
        {},
    ) == 53
    assert _symbol_line_start("symbol:okg:path:name:not-a-line", {}) is None


def test_agent_context_verification_uses_concrete_known_tests() -> None:
    from okg.substrate.agent_context import _suggested_verification

    suggestions = _suggested_verification(
        change="improve-agent-context-precision",
        files=[{"path": "src/okg/substrate/agent_context.py"}],
        symbols=[],
    )

    commands = [item["command"] for item in suggestions]
    assert (
        "uv run --extra dev openspec validate "
        "improve-agent-context-precision --strict --no-interactive"
    ) in commands
    assert "git diff --check" in commands
    assert (
        "uv run --extra dev pytest "
        "tests/substrate/cli/test_agent_context_cli.py -q"
    ) in commands
    assert all(item["kind"] != "coverage_gap" for item in suggestions)


def test_agent_context_verification_reports_gap_for_unknown_files() -> None:
    from okg.substrate.agent_context import _suggested_verification

    suggestions = _suggested_verification(
        change="improve-agent-context-precision",
        files=[{"path": "unknown/surface.txt"}],
        symbols=[],
    )

    assert suggestions[-1]["kind"] == "coverage_gap"
    assert suggestions[-1]["command"] is None
