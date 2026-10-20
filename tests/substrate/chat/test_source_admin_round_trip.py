"""A chat-invoked source round trip (PACT chat-source-admin).

``task.chat-source-admin.round-trip``. Proof has to be a live round trip
against a real deployment, not a tool listing: a call surface that
answers without doing anything, read as "source admin works", is the
absence-dressed-as-agreement failure mode.

Two halves live here.

The **hermetic** half covers how the envelopes are READ, which is where
the silent failures are: a swallowed ``plan_not_found`` reads as
"nothing to do"; a count-less ingest envelope read as a zero delta
reports a failed ingest as a correct empty one; and a generation blocked
on ``narrowing_violation`` is a catalog gap, diagnosable and distinct
from a chat-surface defect.

The **live** half (``test_live_round_trip``) drives the real two-phase
flow — ``discover_sources`` → ``propose_setup`` → operator approval →
``commit_setup`` — against a write-enabled ``mcp-serve`` on the loopback
interim, captures the server's stderr warning verbatim, and reads the
graph back. It needs shared Postgres and is opt-in:

    OKG_CHAT_SOURCE_ADMIN_LIVE=1 \\
    OKG_CHAT_SOURCE_ADMIN_DEPLOYMENT=okg-workspace \\
    uv run --extra dev python -m pytest \\
      tests/substrate/chat/test_source_admin_round_trip.py -q -m 'slow'

FLAGGED FOR THE SERIAL RUN: it starts a server against a live
deployment database and commits a source, so it must not share a
database with a parallel pytest worker.
"""

from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import time
from pathlib import Path

import pytest

from okg.substrate.chat.source_admin import (
    COMMIT_PLAN_NOT_FOUND,
    ChatSourceAdminError,
    call_live_tool,
    diagnose_generation,
    digest_registry_file,
    interpret_commit_envelope,
    read_live_tools,
    resolve_admin_tool_binding,
)
from okg.substrate.mcp.operators import SOURCE_ADMIN_TOOLS


# ---------------------------------------------------------------------------
# The two-phase flow, read without guessing
# ---------------------------------------------------------------------------

def test_plan_not_found_surfaces_as_a_re_propose_prompt() -> None:
    """The 30-minute in-process TTL is a real operator path.

    Plans are held in the MCP server's memory and do not survive a
    restart, so an operator who approves five minutes after reading
    lands here. A swallowed ``plan_not_found`` reads as "nothing to do".
    """
    outcome = interpret_commit_envelope({
        "status": COMMIT_PLAN_NOT_FOUND,
        "plan_id": "1f0d-expired",
        "summary": "Plan expired (30 min TTL) or MCP server restarted.",
    })
    assert outcome.re_propose_required is True
    assert outcome.succeeded is False
    assert "propose_setup" in (outcome.prompt or "")
    assert "30 minutes" in (outcome.prompt or "")
    assert "1f0d-expired" in " ".join(outcome.findings)


def test_the_two_phase_flow_is_not_collapsed_into_add_source() -> None:
    """``add_source`` stays the documented low-level recovery hook.

    The proven path is propose/commit; this asserts the interpreter
    handles the AGGREGATE commit envelope (with its ``results`` rows),
    not just a bare single-source one.
    """
    outcome = interpret_commit_envelope({
        "status": "ok",
        "results": [
            {
                "short": "papers",
                "source_id": "openalex_papers",
                "status": "ingest_complete",
                "nodes_added": 412,
                "edges_added": 97,
            },
            {
                "short": "notes",
                "source_id": "obsidian_notes",
                "status": "ok_already_configured",
            },
        ],
    })
    assert outcome.succeeded is True
    assert outcome.counts_authoritative is True
    assert outcome.nodes_added == 412
    assert outcome.edges_added == 97
    assert len(outcome.per_source) == 2


def test_count_less_envelope_is_unknown_not_zero() -> None:
    """``add_source`` returns no counts when it cannot attribute rows.

    Reading that as zero would report a failed ingest as a correct empty
    one, and would fail the round trip for the wrong reason.
    """
    outcome = interpret_commit_envelope({
        "status": "ingest_complete",
        "source_id": "openalex_papers",
        "summary": "Wired `openalex_papers`.",
    })
    assert outcome.succeeded is True
    assert outcome.nodes_added is None
    assert outcome.edges_added is None
    assert outcome.counts_authoritative is False
    assert any("UNKNOWN (not zero)" in f for f in outcome.findings)


def test_zero_row_ingest_is_reported_as_zero_rows_not_as_unknown() -> None:
    outcome = interpret_commit_envelope({
        "status": "ingest_complete_no_rows",
        "source_id": "openalex_papers",
        "nodes_added": 0,
        "edges_added": 0,
    })
    assert outcome.zero_rows is True
    assert outcome.counts_authoritative is True
    assert outcome.nodes_added == 0


def test_a_tool_that_answers_without_doing_the_work_is_not_success() -> None:
    """``needs_connection`` is an answer, not an effect."""
    outcome = interpret_commit_envelope({
        "status": "ok",
        "results": [
            {
                "short": "gmail",
                "source_id": "gmail_api",
                "status": "needs_connection",
            },
        ],
    })
    assert outcome.succeeded is False
    assert any("answered without doing the work" in f for f in outcome.findings)


def test_a_failed_ingest_is_not_success() -> None:
    outcome = interpret_commit_envelope({
        "status": "activation_blocked",
        "results": [
            {
                "short": "papers",
                "source_id": "openalex_papers",
                "status": "activation_blocked",
                "summary": "probe findings block activation",
            },
        ],
    })
    assert outcome.succeeded is False


# ---------------------------------------------------------------------------
# The catalog hop
# ---------------------------------------------------------------------------

def test_published_generation_is_published() -> None:
    diagnosis = diagnose_generation({
        "generation_id": "gen:2026-08-04T00:00:00Z/42",
        "status": "published",
        "blocked_reason": None,
        "catalog_version_id": "cat-17",
    })
    assert diagnosis.published is True
    assert diagnosis.catalog_gap is False
    assert diagnosis.catalog_version_id == "cat-17"


def test_narrowing_violation_is_a_diagnosed_catalog_gap() -> None:
    """A first ingest of a newly enabled source can legitimately block.

    The violation class is named, and the outcome is recorded as a
    catalog gap rather than as a chat-surface failure.
    """
    diagnosis = diagnose_generation({
        "generation_id": "gen:2026-08-04T00:00:00Z/43",
        "status": "blocked",
        "blocked_reason": {
            "code": "narrowing_violation",
            "violating_triples": ["(paper, cites, dataset)"],
        },
    })
    assert diagnosis.published is False
    assert diagnosis.catalog_gap is True
    assert diagnosis.violation_class == "narrowing_violation"
    assert diagnosis.violating_triples == ("(paper, cites, dataset)",)
    assert any("catalog gap" in f for f in diagnosis.findings)


def test_a_block_for_another_reason_is_not_a_catalog_gap() -> None:
    diagnosis = diagnose_generation({
        "generation_id": "gen:2026-08-04T00:00:00Z/44",
        "status": "blocked",
        "blocked_reason": {"code": "interval_cardinality_drift"},
    })
    assert diagnosis.published is False
    assert diagnosis.catalog_gap is False
    assert diagnosis.violation_class is None


def test_a_missing_status_is_unknown_not_published() -> None:
    diagnosis = diagnose_generation({"generation_id": "gen:x"})
    assert diagnosis.published is False
    assert any("UNKNOWN, not published" in f for f in diagnosis.findings)


# ---------------------------------------------------------------------------
# The client half cannot become an escape hatch
# ---------------------------------------------------------------------------

def test_an_out_of_surface_tool_name_is_refused_client_side() -> None:
    with pytest.raises(ChatSourceAdminError) as excinfo:
        call_live_tool("http://127.0.0.1:8101/mcp", "record", {"x": 1})
    assert excinfo.value.data["reason"] == "out_of_surface"


# ---------------------------------------------------------------------------
# The live round trip
# ---------------------------------------------------------------------------

LIVE_ENV = "OKG_CHAT_SOURCE_ADMIN_LIVE"
LIVE_DEPLOYMENT_ENV = "OKG_CHAT_SOURCE_ADMIN_DEPLOYMENT"
#: The source class the round trip wires. Stated rather than discovered
#: because the catalog hop is explicit: it must be one the applied
#: narrowing catalog already covers, or `okg catalog load --apply` runs
#: first and its catalog_version_id is recorded.
LIVE_SOURCE_SHORT = os.environ.get(
    "OKG_CHAT_SOURCE_ADMIN_SOURCE", "git-files",
)


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


@pytest.mark.slow
@pytest.mark.postgres
@pytest.mark.serial_db
def test_live_round_trip(tmp_path: Path) -> None:
    """Drive the two-phase flow against a real deployment.

    Records, in one chain: the captured loopback warning, the live
    tools/list, the registry digest before and after at the
    SERVER-REPORTED path, the deliberate ``plan_not_found`` re-propose
    arm, the commit's own inline ingest envelope, ``source_status``, and
    ``okg generation describe``. No second manual ingest: the correct
    outcome of re-ingesting unchanged content is zero rows, so demanding
    a non-zero delta from it would fail the correct behavior.
    """
    if os.environ.get(LIVE_ENV) != "1":
        pytest.skip(f"set {LIVE_ENV}=1 to run the live round trip")
    deployment = os.environ.get(LIVE_DEPLOYMENT_ENV)
    if not deployment:
        pytest.skip(f"set {LIVE_DEPLOYMENT_ENV} to a live deployment name")

    port = _free_port()
    endpoint = f"http://127.0.0.1:{port}/mcp"
    stderr_path = tmp_path / "mcp-serve.stderr"
    with stderr_path.open("w", encoding="utf-8") as stderr_file:
        server = subprocess.Popen(
            [
                sys.executable, "-m", "okg.substrate.cli.main", "mcp-serve",
                "--transport", "streamable-http",
                "--deployment", deployment,
                "--host", "127.0.0.1",
                "--port", str(port),
                "--allow-writes",
            ],
            stdout=subprocess.DEVNULL,
            stderr=stderr_file,
            text=True,
            # Own session (#703): the teardown below signals this server, and
            # without a separate session that signal can reach the pytest-xdist
            # worker group. stdio is already detached from the worker.
            start_new_session=True,
        )
        try:
            live = None
            deadline = time.monotonic() + 90
            last_error: Exception | None = None
            while time.monotonic() < deadline:
                if server.poll() is not None:
                    raise AssertionError(
                        "mcp-serve exited early:\n"
                        + stderr_path.read_text(encoding="utf-8")
                    )
                try:
                    live = read_live_tools(endpoint)
                    break
                except ChatSourceAdminError as exc:
                    last_error = exc
                    time.sleep(1.0)
            assert live is not None, f"server never answered: {last_error}"

            # (0) The server's own loopback warning, captured verbatim.
            # The guard's warn tuple reaches only run_server's log, so
            # process stderr is the evidence.
            stderr_text = stderr_path.read_text(encoding="utf-8")
            assert "write-enabled MCP surface" in stderr_text
            assert "unauthenticated" in stderr_text

            bound = resolve_admin_tool_binding(live=live)
            assert set(bound) == set(SOURCE_ADMIN_TOOLS)

            # (a) The registry path the SERVER reports, digested before.
            listed = call_live_tool(endpoint, "list_sources", {
                "deployment": deployment,
            })
            assert listed.get("status") == "ok", listed
            registry_path = listed["registry_path"]
            digest_before = digest_registry_file(registry_path)

            # The deliberate plan_not_found arm, exercised once.
            expired = call_live_tool(endpoint, "commit_setup", {
                "plan_id": "00000000-0000-0000-0000-000000000000",
            })
            expired_outcome = interpret_commit_envelope(expired)
            assert expired_outcome.re_propose_required is True
            assert expired_outcome.succeeded is False

            # The NORMAL path: discover, propose, approve, commit.
            discovered = call_live_tool(endpoint, "discover_sources", {
                "deployment": deployment,
            })
            assert discovered["status"] == "ok"
            proposal = call_live_tool(endpoint, "propose_setup", {
                "deployment": deployment,
                "only": [LIVE_SOURCE_SHORT],
                "project_paths": [str(Path.cwd())],
            })
            assert proposal["status"] == "ok", proposal
            if not proposal.get("would_wire"):
                pytest.skip(
                    f"{LIVE_SOURCE_SHORT} is already wired in {deployment}; "
                    "the round trip needs an unwired catalog source"
                )
            # <- the operator approval turn happens here in chat.
            committed = call_live_tool(endpoint, "commit_setup", {
                "plan_id": proposal["plan_id"],
            })
            outcome = interpret_commit_envelope(committed)

            # (a) The registry carries the new entry.
            digest_after = digest_registry_file(registry_path)
            assert digest_after != digest_before, (
                "the commit reported an effect but the registry at the "
                f"server-reported path {registry_path} is byte-identical"
            )
            relisted = call_live_tool(endpoint, "list_sources", {
                "deployment": deployment,
            })
            assert LIVE_SOURCE_SHORT in json.dumps(relisted)

            # (b) source_status names the connector/auth and ingest state.
            status = call_live_tool(endpoint, "source_status", {
                "deployment": deployment,
                "short_name": LIVE_SOURCE_SHORT,
            })
            assert status["status"] == "ok", status
            assert "preflight_status" in status
            assert "last_ingest_at" in status

            # (c) + (d) The commit's own INLINE ingest, and the graph.
            assert outcome.succeeded, outcome.findings
            assert outcome.counts_authoritative, (
                "the inline ingest returned no run-attributed counts, so "
                "the row delta is unknown: " + str(outcome.findings)
            )
            assert (outcome.nodes_added or 0) + (outcome.edges_added or 0) > 0
        finally:
            server.terminate()
            try:
                server.wait(timeout=30)
            except subprocess.TimeoutExpired:  # pragma: no cover
                server.kill()

    # `generation describe` takes a POSITIONAL generation id; there is no
    # --latest. Resolve the newest published generation first.
    listing = subprocess.run(
        [
            sys.executable, "-m", "okg.substrate.cli.main",
            "generation", "list",
            "--deployment", deployment, "--limit", "1", "--json",
        ],
        capture_output=True, text=True, check=False, timeout=300,
    )
    assert listing.returncode == 0, listing.stderr
    rows = json.loads(listing.stdout)
    if isinstance(rows, dict):
        rows = rows.get("generations") or rows.get("rows") or []
    assert rows, f"no published generation for {deployment}"
    generation_id = str(
        rows[0].get("generation_id") or rows[0].get("id")
    )
    describe = subprocess.run(
        [
            sys.executable, "-m", "okg.substrate.cli.main",
            "generation", "describe", generation_id,
            "--deployment", deployment, "--json",
        ],
        capture_output=True, text=True, check=False, timeout=300,
    )
    assert describe.returncode == 0, describe.stderr
    diagnosis = diagnose_generation(json.loads(describe.stdout))
    if diagnosis.catalog_gap:
        pytest.fail(
            "DIAGNOSED CATALOG GAP (not a chat-surface defect): the "
            f"generation blocked on {diagnosis.violation_class} for "
            f"{list(diagnosis.violating_triples)}. Run `okg catalog load "
            f"--deployment deployments/{deployment} --apply` and record "
            "the catalog_version_id, then re-run."
        )
    assert diagnosis.published, diagnosis.findings
