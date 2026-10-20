"""Chunked transcript ingest (unblock-source-publish-admission).

A transcript larger than the per-run budget is consumed across successive
runs. The failure these tests guard against is not "slow ingest" -- it is
SILENT DATA LOSS: under `deletion_semantics: missing_from_completed_scope`,
a run that declares a complete record set retracts every record absent from
it, so a chunked source that reports a partial footprint as complete deletes
the graph nodes its earlier chunks emitted.

Each test names the specific way convergence or completeness could break.
Several are written so they FAIL against the pre-chunking behaviour; where a
guard is load-bearing that is asserted explicitly rather than assumed.
"""
from __future__ import annotations

import json
import uuid
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pygit2
import psycopg
import pytest

from okg.substrate.ingest import service as ingest_service
from okg.substrate.ingest.adapter_factory import (
    ConstructedSourceAdapter,
    bind_source_adapter_identity,
)
from okg.substrate.ingest.change_identity import (
    BOOTSTRAP_AUTHORITY_CURSOR_KEY,
    identity_fingerprint,
)
from okg.substrate.ingest.runner import (
    BOOTSTRAP_IDENTITY_LEDGER_PROGRESS_KEY,
    SourceRunner,
)
from okg.substrate.ingest.service import IngestOptions
from okg.substrate.library.sources.base import NodeFact
from okg.substrate.library.sources.change_probe import (
    CURSOR_TOKEN_KEY,
    cursor_has_outstanding_work,
)
from okg.substrate.library.sources.codex_sessions import CodexSessionsSource
from okg.substrate.sources.registry import (
    STRICT_ADMISSION_CONTRACT,
    load_source_registry,
)
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer


def _session_rows(repo: Path, session_id: str, turns: int) -> list[dict]:
    rows: list[dict] = [
        {
            "type": "session_meta",
            "timestamp": "2026-06-05T00:00:00Z",
            "payload": {
                "id": session_id,
                "cwd": str(repo),
                "model": "gpt-5",
                "timestamp": "2026-06-05T00:00:00Z",
            },
        },
    ]
    for turn in range(turns):
        rows.append({
            "type": "response_item",
            "timestamp": f"2026-06-05T00:00:{turn + 1:02d}Z",
            "payload": {
                "type": "message",
                "role": "assistant",
                # Padding makes each line comfortably larger than the byte
                # budgets used below, so a budget stops the parse mid-file.
                "content": f"turn {turn} " + ("x" * 400),
            },
        })
    return rows


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "\n".join(json.dumps(row) for row in rows) + "\n",
        encoding="utf-8",
    )


def _init_repo(repo: Path) -> None:
    """A repo with a commit, so the git half of the union probe resolves."""
    handle = pygit2.init_repository(str(repo))
    (repo / "README.md").write_text("fixture\n", encoding="utf-8")
    handle.index.add("README.md")
    handle.index.write()
    signature = pygit2.Signature("fixture", "fixture@example.com")
    handle.create_commit(
        "HEAD", signature, signature, "init", handle.index.write_tree(), []
    )


def _source(repo: Path, root: Path, **kwargs: Any) -> CodexSessionsSource:
    return CodexSessionsSource(
        repo="okg",
        repo_root=repo,
        codex_sessions_root=root,
        archived_sessions_root=None,
        cwd_filter=[repo],
        since="all",
        **kwargs,
    )


def _node_ids(facts) -> set[str]:
    return {f.node_id for f in facts if isinstance(f, NodeFact)}


def _drain_to_completion(
    source: CodexSessionsSource,
    *,
    mode: str = "cursor",
    max_runs: int = 50,
) -> tuple[list[Any], dict[str, Any], int]:
    """Run until the source reports no outstanding backlog.

    Returns the accumulated facts, the final cursor, and the run count. The
    run cap is a convergence assertion: an unbounded loop would hang rather
    than fail if catch-up livelocked.
    """
    cursor: dict[str, Any] = {}
    all_facts: list[Any] = []
    runs = 0
    while runs < max_runs:
        runs += 1
        run = source.run(f"run-{runs}", mode=mode, cursor=cursor)
        all_facts.extend(list(run.facts))
        cursor = dict(run.next_cursor or {})
        # Mirror the runner, which folds a declared record set into the cursor
        # it persists. Omitting it here would test a cursor shape the source
        # never actually sees.
        if run.record_set is not None:
            cursor["record_set"] = run.record_set.to_cursor()
        if not cursor.get("outstanding_backlog"):
            return all_facts, cursor, runs
    raise AssertionError(
        f"ingest did not converge within {max_runs} runs -- livelock"
    )


def test_oversized_transcript_is_ingested_not_dropped(tmp_path: Path) -> None:
    """The rejected alternative: skipping the file entirely.

    A long session is legitimate data. Under a budget far smaller than the
    file, every message must still reach the graph -- just across more runs.
    """
    repo = tmp_path / "repo"
    repo.mkdir()
    root = tmp_path / "codex" / "sessions"
    _write_jsonl(root / "big.jsonl", _session_rows(repo, "session-big", 40))

    full = _source(repo, root)
    full_facts = list(full.run("run-full", mode="cursor").facts)

    chunked = _source(repo, root, max_transcript_bytes=1_000)
    chunked_facts, _, runs = _drain_to_completion(chunked)

    assert runs > 1, "budget did not actually chunk the file"
    assert _node_ids(chunked_facts) == _node_ids(full_facts)


def test_chunked_parse_emits_the_same_fact_set_as_one_full_parse(
    tmp_path: Path,
) -> None:
    """Full FACT-SET equivalence, not merely equal session counters.

    Session-level counters converge across a boundary on their own, so a
    counters-only assertion would pass while the emitted facts differed.
    """
    repo = tmp_path / "repo"
    repo.mkdir()
    root = tmp_path / "codex" / "sessions"
    _write_jsonl(root / "big.jsonl", _session_rows(repo, "session-eq", 25))

    full_facts = list(_source(repo, root).run("run-full", mode="cursor").facts)
    chunked_facts, _, runs = _drain_to_completion(
        _source(repo, root, max_transcript_bytes=900)
    )

    assert runs > 1

    # Identity AND subtype: right ids under the wrong subtype is a divergence.
    # Compared as sets because chunking legitimately re-emits a node (the
    # session node advances each chunk) and emission is an upsert keyed by
    # node_id -- so the graph state, not the emission count, is what must match.
    def _identity(facts):
        return {
            (f.node_id, f.subtype) for f in facts if isinstance(f, NodeFact)
        }

    assert _identity(chunked_facts) == _identity(full_facts)

    # Last write wins, so the settled attrs of every node must match a full
    # parse. Set equality alone would miss a chunk that emitted the right node
    # carrying the wrong content.
    def _settled(facts) -> dict[str, dict[str, Any]]:
        out: dict[str, dict[str, Any]] = {}
        for fact in facts:
            if isinstance(fact, NodeFact):
                out[fact.node_id] = dict(fact.attrs)
        return out

    chunked_settled = _settled(chunked_facts)
    for node_id, attrs in _settled(full_facts).items():
        assert node_id in chunked_settled
        # The in-progress marker exists only mid-ingest and must be gone by
        # the completing run; every other attr must match exactly.
        assert "ingest_in_progress" not in chunked_settled[node_id]
        assert chunked_settled[node_id] == attrs, node_id


def test_tool_call_output_split_across_a_chunk_boundary_keeps_its_stamp(
    tmp_path: Path,
) -> None:
    """The equivalence hazard with no state to carry it.

    `function_call` and its `function_call_output` are separate lines. If a
    chunk ends between them, the is_error/exit_code stamp is lost unless the
    open call is carried in the cursor and the event re-emitted.
    """
    repo = tmp_path / "repo"
    repo.mkdir()
    root = tmp_path / "codex" / "sessions"
    rows = [
        {
            "type": "session_meta",
            "timestamp": "2026-06-05T00:00:00Z",
            "payload": {
                "id": "session-split",
                "cwd": str(repo),
                "model": "gpt-5",
                "timestamp": "2026-06-05T00:00:00Z",
            },
        },
        {
            "type": "response_item",
            "timestamp": "2026-06-05T00:00:01Z",
            "payload": {
                "type": "function_call",
                "name": "shell",
                "call_id": "call-1",
                "arguments": json.dumps({"command": "false"}),
            },
        },
        {
            "type": "response_item",
            "timestamp": "2026-06-05T00:00:02Z",
            "payload": {
                "type": "function_call_output",
                "call_id": "call-1",
                "output": "Process exited with code 1",
            },
        },
    ]
    _write_jsonl(root / "split.jsonl", rows)

    def _tool_attrs(facts) -> list[dict[str, Any]]:
        return [
            dict(f.attrs)
            for f in facts
            if isinstance(f, NodeFact) and f.node_id.startswith("tool_use:")
        ]

    full = _source(repo, root, capture_tool_use_events=["shell"])
    full_attrs = _tool_attrs(list(full.run("run-full", mode="cursor").facts))
    assert full_attrs, "fixture emitted no tool event"
    assert full_attrs[-1].get("exit_code") == 1

    # A one-byte budget forces a stop after every line, so the boundary is
    # guaranteed to fall between the call and its output.
    chunked = _source(
        repo, root, max_transcript_bytes=1, capture_tool_use_events=["shell"]
    )
    chunked_facts, _, runs = _drain_to_completion(chunked)
    assert runs > 2

    chunked_attrs = _tool_attrs(chunked_facts)
    # The stamped re-emission is what the graph upserts last.
    assert chunked_attrs[-1].get("exit_code") == 1
    assert chunked_attrs[-1].get("is_error") is True


def test_resume_works_when_a_line_exceeds_the_prefix_check_window(
    tmp_path: Path,
) -> None:
    """A long line must not make a transcript permanently unresumable.

    Resume is gated on re-hashing the last consumed line and comparing it to
    the cursor. That check read one fixed 64 KiB window and treated whatever
    followed the last newline in it as "the line" -- so a line longer than the
    window hashed a mere suffix, never matched, and forced a full re-parse.

    Real Codex transcripts embed rendered tool output, so multi-hundred-KiB
    lines are routine, and they occur in exactly the oversized transcripts
    that need chunking. Before chunking this only cost a wasted re-parse.
    Under a per-run budget the re-parse stops at the same offset every time,
    so the file makes zero progress forever -- which is how this was found: on
    the real 1.8 GB transcript, not on a fixture.
    """
    repo = tmp_path / "repo"
    repo.mkdir()
    root = tmp_path / "codex" / "sessions"
    path = root / "wide-lines.jsonl"
    rows = _session_rows(repo, "session-wide-lines", 2)
    for turn in range(4):
        rows.append({
            "type": "response_item",
            "timestamp": f"2026-06-05T00:01:{turn:02d}Z",
            # Comfortably past the 64 KiB window the check used to assume.
            "payload": {"type": "message", "role": "assistant",
                        "content": "y" * 200_000},
        })
    rows.extend(_session_rows(repo, "session-wide-lines", 2)[1:])
    _write_jsonl(path, rows)
    assert max(len(line) for line in path.read_bytes().split(b"\n")) > 65536

    source = _source(repo, root, max_transcript_bytes=50_000)
    first = source.run("run-1", mode="cursor", cursor={})
    list(first.facts)
    cursor = dict(first.next_cursor or {})
    offset_1 = int(cursor["transcripts"][str(path)]["bytes"])

    second = source.run("run-2", mode="cursor", cursor=cursor)
    list(second.facts)
    offset_2 = int(second.next_cursor["transcripts"][str(path)]["bytes"])

    # Resumed rather than restarted, and therefore advanced.
    assert source.last_run_stats["append_delta_files"] == 1
    assert source.last_run_stats["full_parse_files"] == 0
    assert offset_2 > offset_1

    _, final, runs = _drain_to_completion(
        _source(repo, root, max_transcript_bytes=50_000)
    )
    assert runs > 1
    assert int(final["transcripts"][str(path)]["bytes"]) == path.stat().st_size


def test_mcp_operator_ambiguity_survives_a_chunk_boundary(
    tmp_path: Path,
) -> None:
    """Ambiguity is a REFUSAL to attribute, and refusals must not be forgotten.

    An operator name advertised by two different MCP servers cannot be
    attributed to either, so the parse drops it from the registry and records
    it as ambiguous -- a sticky decision: `elif operator not in ambiguous`
    stops a later advertisement from re-registering it.

    That set was rebuilt per chunk. So the fixture below puts both conflicting
    advertisements in chunk 1 (making the operator ambiguous) and a single
    re-advertisement in chunk 2: without the carried set, chunk 2 starts clean,
    re-registers the operator, and confidently attributes a call that a full
    parse refuses to attribute -- a chunked parse inventing provenance.

    Note the registry carry alone does NOT cover this. An earlier version of
    this test split the two conflicting advertisements across the boundary,
    where the carried registry detects the conflict on its own -- so it passed
    with the ambiguous-set carry removed entirely.
    """
    repo = tmp_path / "repo"
    repo.mkdir()
    root = tmp_path / "codex" / "sessions"

    def _advert(server: str, timestamp: str) -> dict:
        return {
            "type": "response_item",
            "timestamp": timestamp,
            "payload": {
                "type": "tool_search_output",
                "tools": [{
                    "name": f"mcp__{server}",
                    "tools": [{"name": "query"}],
                }],
            },
        }

    rows = _session_rows(repo, "session-mcp", 1)
    # Chunk 1: two servers claim the same operator name -> ambiguous.
    rows.append(_advert("okg-workspace", "2026-06-05T00:00:10Z"))
    rows.append(_advert("okg-ci", "2026-06-05T00:00:11Z"))
    # Padding so a small budget puts the boundary here.
    rows.append({
        "type": "response_item",
        "timestamp": "2026-06-05T00:00:12Z",
        "payload": {"type": "message", "role": "assistant",
                    "content": "pad " * 300},
    })
    # Chunk 2: one server re-advertises it. The refusal must still hold.
    rows.append(_advert("okg-workspace", "2026-06-05T00:00:13Z"))
    rows.append({
        "type": "response_item",
        "timestamp": "2026-06-05T00:00:14Z",
        "payload": {
            "type": "function_call",
            "name": "query",
            "call_id": "call-mcp",
            "arguments": json.dumps({"q": "1"}),
        },
    })
    _write_jsonl(root / "mcp.jsonl", rows)

    def _operators(facts) -> list[str]:
        return [
            str(f.attrs.get("mcp_server") or "")
            for f in facts
            if isinstance(f, NodeFact) and f.node_id.startswith("tool_use:")
        ]

    full = list(
        _source(repo, root, capture_tool_use_events=["query"])
        .run("run-full", mode="cursor").facts
    )
    full_servers = _operators(full)
    assert full_servers, "fixture emitted no tool event"
    # A full parse sees both adverts and refuses to attribute.
    assert full_servers[-1] == ""

    chunked_facts, _, runs = _drain_to_completion(
        _source(
            repo, root, max_transcript_bytes=600,
            capture_tool_use_events=["query"],
        )
    )
    assert runs > 1, "budget did not chunk across the adverts"
    chunked_servers = _operators(chunked_facts)
    assert chunked_servers[-1] == "", (
        "chunked parse attributed an operator the full parse refuses to"
    )


def test_run_respects_its_byte_budget(tmp_path: Path) -> None:
    """The budget is a target, overshot by at most one line per transcript.

    Design §7 bounds a run at the budget plus one line per RESUMED transcript
    -- not "plus one line" overall. Asserting the real bound keeps the claim
    honest and catches a run that ignores the budget entirely.
    """
    repo = tmp_path / "repo"
    repo.mkdir()
    root = tmp_path / "codex" / "sessions"
    for idx in range(3):
        _write_jsonl(
            root / f"big-{idx}.jsonl",
            _session_rows(repo, f"session-{idx}", 40),
        )

    budget = 4_000
    source = _source(repo, root, max_transcript_bytes=budget)
    longest_line = max(
        len(line)
        for path in root.glob("*.jsonl")
        for line in path.read_bytes().split(b"\n")
    )

    cursor: dict[str, Any] = {}
    for run_no in range(1, 5):
        run = source.run(f"run-{run_no}", mode="cursor", cursor=cursor)
        list(run.facts)
        cursor = dict(run.next_cursor or {})
        read = int(source.last_run_stats["bytes_delta"])
        assert read <= budget + 3 * longest_line, (
            f"run {run_no} read {read} against a {budget} budget"
        )


def test_dormant_backlog_makes_the_probe_report_changed(tmp_path: Path) -> None:
    """Without this the file is never revisited on a quiet host.

    The probe digests (key, size, head_sha, tail_sha) -- consumed offset is
    not an input -- and the runner short-circuits without calling run() when
    nothing changed. Convergence would then depend on unrelated churn.
    """
    repo = tmp_path / "repo"
    repo.mkdir()
    # A real repo so the source resolves its production probe -- the UnionProbe
    # over (git scope, transcripts). The union does not delegate probe() to its
    # sub-probes, so testing only the bare AppendOffsetProbe would miss the
    # path this source actually takes.
    _init_repo(repo)
    root = tmp_path / "codex" / "sessions"
    _write_jsonl(root / "big.jsonl", _session_rows(repo, "session-dormant", 20))

    source = _source(repo, root, max_transcript_bytes=800)
    first = source.run("run-1", mode="cursor", cursor={})
    list(first.facts)
    cursor = dict(first.next_cursor or {})
    assert cursor.get("outstanding_backlog"), "fixture did not leave a backlog"

    probe = source.change_probe
    # Stamp the token the runner would have persisted, so the ONLY reason the
    # probe can report changed is the recorded backlog -- the file itself has
    # not been touched since.
    cursor[CURSOR_TOKEN_KEY] = probe.build_token()
    verdict = probe.probe(cursor=cursor, sync_scope=None)

    assert verdict.changed is True
    assert verdict.reason == "outstanding_backlog"

    # Load-bearing: with the backlog cleared and nothing else changed, the
    # same probe short-circuits. Otherwise this test would pass vacuously.
    cursor["outstanding_backlog"] = []
    assert probe.probe(cursor=cursor, sync_scope=None).changed is False


def test_outstanding_work_signal_only_forces_reads_never_suppresses_them() -> None:
    """One-directional, like every other probe rule."""
    assert cursor_has_outstanding_work(None) is False
    assert cursor_has_outstanding_work({}) is False
    assert cursor_has_outstanding_work({"outstanding_backlog": []}) is False
    assert cursor_has_outstanding_work({"outstanding_backlog": [{"p": 1}]}) is True


def test_completing_run_does_not_retract_earlier_chunks(tmp_path: Path) -> None:
    """The subtlest hazard, and the one that loses data silently.

    The per-record emission is built from THIS run's facts, and a resumed
    parse starts with an empty message list. So the run that FINISHES a
    chunked file would report a footprint covering only the last chunk --
    and because it marks the key as replaced, chunks 1..N-1 are retracted
    under missing_from_completed_scope.

    This only fires when the source can declare a record set (since: all),
    which is why the fixture uses that regime.
    """
    repo = tmp_path / "repo"
    repo.mkdir()
    root = tmp_path / "codex" / "sessions"
    _write_jsonl(root / "big.jsonl", _session_rows(repo, "session-rec", 30))

    full = _source(repo, root)
    full_run = full.run("run-full", mode="scope_complete")
    list(full_run.facts)
    assert full_run.record_set is not None
    expected = {
        node_id
        for emission in full_run.record_set.records.values()
        for node_id in emission.nodes_by_id()
    }
    assert len(expected) > 5, "fixture too small to detect a shrinking footprint"

    source = _source(repo, root, max_transcript_bytes=900)
    cursor: dict[str, Any] = {}
    runs = 0
    final_record_set = None
    while runs < 50:
        runs += 1
        run = source.run(f"run-{runs}", mode="scope_complete", cursor=cursor)
        list(run.facts)
        cursor = dict(run.next_cursor or {})
        if run.record_set is not None:
            cursor["record_set"] = run.record_set.to_cursor()
            final_record_set = run.record_set
        if not cursor.get("outstanding_backlog"):
            break
    else:
        raise AssertionError("chunked reconcile did not converge")

    assert runs > 1, "budget did not actually chunk the file"
    assert final_record_set is not None
    covered = {
        node_id
        for emission in final_record_set.records.values()
        for node_id in emission.nodes_by_id()
    }
    # The union across chunks must cover everything one full parse owns. A
    # subset here is a retraction of real graph nodes.
    assert expected - covered == set()


def test_reconcile_converges_and_drains_the_footprint_store(
    tmp_path: Path,
) -> None:
    """A budgeted reconcile must terminate, and not hoard cursor state.

    Two ways this deadlocks, both found by running it:

    a) A reconcile re-reads every transcript by design. Under a budget the
       re-read of an already-finished file gets cut short, so it is unfinished
       again -- every cycle, forever, with the budget spent entirely on data
       already in the graph.
    b) Retiring a record's accumulated footprint as soon as its transcript
       finished meant the next reconcile had no footprint for it, had to
       re-parse it from zero, and (a) applied again. The footprint is retired
       when a published record set covers it, not when the file completes.

    Accumulation is also cursor state, and cursor state has a cost -- a field
    this size is spilled to a blob table and rewritten every run -- so it must
    drain rather than grow with the corpus.
    """
    repo = tmp_path / "repo"
    repo.mkdir()
    root = tmp_path / "codex" / "sessions"
    big = root / "big.jsonl"
    _write_jsonl(big, _session_rows(repo, "session-big", 60))
    for idx in range(6):
        _write_jsonl(
            root / f"small-{idx}.jsonl",
            _session_rows(repo, f"session-small-{idx}", 1),
        )

    source = _source(repo, root, max_transcript_bytes=3_000)
    first = source.run("run-1", mode="scope_complete", cursor={})
    list(first.facts)
    assert str(big) in {
        item["path"]
        for item in (first.next_cursor.get("outstanding_backlog") or [])
    }, "fixture left no backlog"

    facts, cursor, runs = _drain_to_completion(
        _source(repo, root, max_transcript_bytes=3_000), mode="scope_complete"
    )
    assert runs > 1, "budget did not actually chunk the file"
    assert facts

    # Converged: the store is empty because the declared record set now covers
    # every transcript, and the small files did not bounce back into backlog.
    assert not (cursor.get("record_footprint") or {})
    assert not (cursor.get("outstanding_backlog") or [])

    # A steady-state reconcile does no re-reading and stays converged.
    steady = source.run("run-steady", mode="scope_complete", cursor=cursor)
    list(steady.facts)
    assert source.last_run_stats["files_scanned"] == 0
    assert not (steady.next_cursor.get("outstanding_backlog") or [])

    # A LATER catch-up (a record set already published) holds footprints only
    # for the transcripts still in flight -- not one per transcript. A store
    # that scales with the corpus is spilled to a blob table and rewritten
    # every run, on a host holding thousands of transcripts.
    _write_jsonl(
        root / "new-big.jsonl", _session_rows(repo, "session-new-big", 60)
    )
    catch_up = source.run("run-catchup", mode="scope_complete", cursor=cursor)
    list(catch_up.facts)
    footprint = catch_up.next_cursor.get("record_footprint") or {}
    backlog = {
        item["path"]
        for item in (catch_up.next_cursor.get("outstanding_backlog") or [])
    }
    assert backlog, "fixture did not create a second catch-up"
    assert set(footprint) == backlog, (
        f"footprint store holds {len(footprint)} keys for {len(backlog)} "
        "in-flight transcripts"
    )


def test_partial_run_declines_completed_scope(tmp_path: Path) -> None:
    """A run that stopped mid-transcript has not walked the whole scope."""
    repo = tmp_path / "repo"
    repo.mkdir()
    root = tmp_path / "codex" / "sessions"
    _write_jsonl(root / "big.jsonl", _session_rows(repo, "session-partial", 20))

    source = _source(repo, root, max_transcript_bytes=800)
    run = source.run("run-1", mode="scope_complete", cursor={})
    list(run.facts)

    assert run.next_cursor.get("outstanding_backlog")
    assert run.completed_scope is False
    assert run.record_set is None


def test_backlog_does_not_copy_the_whole_transcript_corpus(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    _init_repo(repo)
    root = tmp_path / "codex" / "sessions"
    _write_jsonl(
        root / "big.jsonl",
        _session_rows(repo, "session-no-corpus-copy", 24),
    )

    from okg.substrate.library.sources import _content_snapshot

    def refuse_snapshot(*_args, **_kwargs):
        raise AssertionError("whole-corpus snapshot materialization invoked")

    monkeypatch.setattr(
        _content_snapshot.CapturedContentSnapshot,
        "__init__",
        refuse_snapshot,
    )
    run = _source(repo, root, max_transcript_bytes=900).run(
        "no-corpus-copy",
        mode="scope_complete",
        cursor={},
    )

    assert run.next_cursor.get("outstanding_backlog")
    assert run.bootstrap_identity is not None
    assert run.bootstrap_identity.scope == "consumed_slices"


@pytest.mark.postgres
def test_runner_bootstraps_real_codex_backlog_then_reaches_exact_watermark(
    okg_migrated: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The production source can lawfully cross its empty-cursor boundary.

    This deliberately uses CodexSessionsSource's real union probe and reader,
    not the runner spy used by the authority-state-machine tests.  It proves
    the first bounded run persists both source progress and bootstrap
    authority, and that fresh adapter instances consume that durable state
    until the same sealed snapshot becomes an ordinary exact watermark.
    """
    repo = tmp_path / "repo"
    repo.mkdir()
    _init_repo(repo)
    root = tmp_path / "codex" / "sessions"
    transcript_path = root / "big.jsonl"
    transcript_rows = _session_rows(repo, "session-runner-bootstrap", 24)
    transcript_rows[0]["payload"]["model"] = "openai:test"
    _write_jsonl(transcript_path, transcript_rows)

    deployment_dir = (
        Path(__file__).resolve().parents[4] / "deployments" / "okg-workspace"
    )
    registry = load_source_registry(deployment_dir / "source_registry.yaml")
    codex_entry = registry["codex-sessions"]
    assert codex_entry.admission_policy is not None
    source_name = codex_entry.ownership_id
    deployment_name = (
        "bootstrap-test-"
        + uuid.uuid5(uuid.NAMESPACE_URL, str(tmp_path)).hex[:8]
    )
    runner = SourceRunner(
        okg_migrated,
        source_name=source_name,
        deployment_name=deployment_name,
        executable_profile="discovery_crawl",
        registry_name="codex-sessions",
        ownership_id=source_name,
        registry_binding_fingerprint="codex-sessions-fixture-v1",
        deletion_semantics="missing_from_completed_scope",
        write_chronos_graph=True,
        write_fact_log=True,
        defer_bootstrap_lock_release=True,
    )

    def fresh_source() -> CodexSessionsSource:
        return _source(repo, root, max_transcript_bytes=900)

    transition_index = 0

    def state_snapshot() -> tuple[object, ...]:
        with psycopg.connect(okg_migrated, autocommit=True) as conn:
            return conn.execute(
                """
                SELECT w.last_applied_rev,
                       w.replay_required,
                       (SELECT value
                          FROM okg.source_progress
                         WHERE deployment = %s
                           AND source_name = %s
                           AND key = %s),
                       (SELECT count(*)
                          FROM okg.node_facts
                         WHERE source = %s),
                       (SELECT count(*)
                          FROM okg.edge_facts
                         WHERE source = %s),
                       (SELECT count(*)
                          FROM okg.source_progress
                         WHERE deployment = %s
                           AND source_name = %s)
                  FROM okg.source_watermarks AS w
                 WHERE w.source = %s
                """,
                (
                    deployment_name,
                    source_name,
                    BOOTSTRAP_IDENTITY_LEDGER_PROGRESS_KEY,
                    source_name,
                    source_name,
                    deployment_name,
                    source_name,
                    source_name,
                ),
            ).fetchone()

    def run_once(
        *,
        observed_at: datetime | None = None,
        adapter: CodexSessionsSource | None = None,
    ):
        nonlocal transition_index
        if observed_at is None:
            transition_index += 1
            observed_at = datetime(
                2026,
                8,
                29,
                12,
                0,
                transition_index,
                tzinfo=UTC,
            )
        options = IngestOptions(
            dsn=okg_migrated,
            deployment=deployment_name,
            deployment_dir=deployment_dir,
            include={"codex-sessions"},
            include_predecessors=False,
            include_dependents=False,
            publish=False,
            admission_observed_at=observed_at,
        )
        context, workflow_ref, envelope_status = (
            ingest_service._prepare_source_admission(
                options=options,
                entry=codex_entry,
                source_registry_name="codex-sessions",
                run_mode="scope_complete",
                sync_scope=None,
            )
        )
        result = runner.run(
            adapter or fresh_source(),
            mode="scope_complete",
            run_context=context,
        )
        try:
            if result.idempotent_replay:
                assert envelope_status in {"accepted", "published", "blocked"}
            else:
                finalized = ingest_service._finalize_source_admission(
                    options=options,
                    admission_id=context.admission_id,
                    source=source_name,
                    run_id=context.run_id,
                    workflow_ref=workflow_ref,
                    existing_status=envelope_status,
                    admission_mode="fast_track",
                    observed_output_count=(
                        result.nodes_appended
                        + result.edges_appended
                        + result.retractions_node
                        + result.retractions_edge
                    ),
                    completed_scope=result.completed_scope,
                    source_health_status=(
                        result.source_health.status
                        if result.source_health is not None
                        else "ok"
                    ),
                    run_mode="scope_complete",
                    next_cursor=result.next_cursor,
                    observed_at=context.observed_at,
                    source_progress=result.terminal_progress,
                    clear_replay_required=result.clear_replay_required,
                    replay_target=result.replay_target,
                    expected_last_applied_rev=result.expected_last_applied_rev,
                    watermark_compare_and_swap=result.watermark_compare_and_swap,
                    expected_bootstrap_ledger=result.expected_bootstrap_ledger,
                    next_bootstrap_ledger=result.next_bootstrap_ledger,
                    rejected_fact_count=result.rejected_fact_count,
                )
                assert finalized.status == "accepted", finalized
        finally:
            runner.release_deferred_bootstrap_lock()
        return result, context, observed_at

    first, first_context, first_observed_at = run_once()
    first_authority = first.next_cursor[BOOTSTRAP_AUTHORITY_CURSOR_KEY]
    assert first_authority["generation"] == 1
    assert first.next_cursor.get("outstanding_backlog")
    first_offsets = {
        key: value["append_cursor"]["bytes"]
        for key, value in first.next_cursor["transcripts"].items()
    }
    assert first_offsets

    # Re-delivery uses the same real admission/run/idempotency identity.  It
    # returns the accepted cursor without invoking Codex or mutating any
    # durable source, graph, or progress state.
    accepted_state = state_snapshot()
    retry_source = fresh_source()

    def fail_if_reader_runs(*_args, **_kwargs):
        raise AssertionError("accepted Codex transition invoked the reader")

    retry_source.run = fail_if_reader_runs  # type: ignore[method-assign]
    retry, retry_context, _ = run_once(
        observed_at=first_observed_at,
        adapter=retry_source,
    )
    assert retry.idempotent_replay is True
    assert retry_context == first_context
    assert retry.next_cursor == first.next_cursor
    assert state_snapshot() == accepted_state

    # A live session may append while an older prefix is still backlogged.
    # The durable authority is for the consumed slice, so this is a normal
    # superseding transition rather than a frozen-whole-source refusal.
    with transcript_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps({
            "type": "response_item",
            "timestamp": "2026-06-05T00:10:00Z",
            "payload": {
                "type": "message",
                "role": "assistant",
                "content": "appended while bootstrap is backlogged",
            },
        }) + "\n")

    second_source = fresh_source()
    second_input_cursors: list[dict[str, Any]] = []
    original_second_run = second_source.run

    def observe_second_input(run_id: str, **kwargs: Any):
        second_input_cursors.append(dict(kwargs.get("cursor") or {}))
        kwargs.pop("since_progress", None)
        return original_second_run(run_id, **kwargs)

    second_source.run = observe_second_input  # type: ignore[method-assign]
    second, _, _ = run_once(adapter=second_source)
    second_authority = second.next_cursor[BOOTSTRAP_AUTHORITY_CURSOR_KEY]
    assert second_authority["generation"] == 2
    assert second_authority["nonce"] != first_authority["nonce"]
    second_offsets = {
        key: value["append_cursor"]["bytes"]
        for key, value in second.next_cursor["transcripts"].items()
    }
    resumed_offsets = {
        key: value["append_cursor"]["bytes"]
        for key, value in second_input_cursors[0]["transcripts"].items()
    }
    assert resumed_offsets == first_offsets
    assert any(
        second_offsets.get(key, 0) > offset
        for key, offset in first_offsets.items()
    ), "the persisted first-run cursor did not advance the real reader"

    result = second
    for _ in range(30):
        if not result.next_cursor.get("outstanding_backlog"):
            break
        result, _, _ = run_once()
    else:
        raise AssertionError("real Codex backlog did not converge")

    assert BOOTSTRAP_AUTHORITY_CURSOR_KEY not in result.next_cursor
    assert result.next_cursor[CURSOR_TOKEN_KEY]["raw_token"]["schema"] == (
        "okg.source.change_probe.token.v1"
    )
    with psycopg.connect(okg_migrated, autocommit=True) as conn:
        persisted, replay_required, node_count, ledger = conn.execute(
            """
            SELECT last_applied_rev,
                   replay_required,
                   (SELECT count(*) FROM okg.node_facts WHERE source = w.source),
                   (SELECT value
                      FROM okg.source_progress
                     WHERE deployment = %s
                       AND source_name = w.source
                       AND key = %s)
              FROM okg.source_watermarks AS w
             WHERE source = %s
            """,
            (
                deployment_name,
                BOOTSTRAP_IDENTITY_LEDGER_PROGRESS_KEY,
                source_name,
            ),
        ).fetchone()
    assert persisted == result.next_cursor
    assert replay_required == {}
    assert node_count > 1
    ledger = json.loads(ledger)
    assert ledger["schema"] == "okg.source_runner.bootstrap_identity_ledger.v2"
    assert ledger["status"] == "exact"
    assert ledger["generation"] >= 2
    assert ledger["transition_identity"] == {
        "admission_id": result.admission_id,
        "run_id": result.run_id,
        "idempotency_key": result.idempotency_key,
    }
    assert ledger["terminal_cursor_fingerprint"] == identity_fingerprint(
        persisted
    )

    # The declared dependent enters the real service runner/admission path only
    # after Codex has a durable exact cursor and no replay obligation.  This is
    # the deployment adapter and registry relationship, not a scheduler spy or
    # a discarded direct adapter call.
    monkeypatch.syspath_prepend(str(deployment_dir))
    from coordination.config_snapshot import CoordinationConfigSource
    from coordination.packet_source import CoordinationPacketSource

    coordination_entry = registry["coordination-packets"]
    predecessors = coordination_entry.raw.get("predecessor_source") or []
    assert source_name in predecessors
    assert "okg-workspace.coordination-config" in predecessors
    coordination_config_entry = replace(
        registry["coordination-config"],
        admission_contract=STRICT_ADMISSION_CONTRACT,
    )
    coordination_config_adapter = CoordinationConfigSource(
        repo="okg",
        config_path=deployment_dir / "coordination.yaml",
    )
    coordination_config_binding = ConstructedSourceAdapter(
        adapter=coordination_config_adapter,
        executable_profile=coordination_config_entry.source_class,
        registry_name="coordination-config",
        ownership_id=coordination_config_entry.ownership_id,
        graph_branch="default",
        registry_binding_fingerprint=bind_source_adapter_identity(
            coordination_config_adapter,
            coordination_config_entry,
        ),
    )
    coordination_config_options = IngestOptions(
        dsn=okg_migrated,
        deployment=deployment_name,
        deployment_dir=deployment_dir,
        include={"coordination-config"},
        include_predecessors=False,
        include_dependents=False,
        publish=False,
        admission_observed_at=datetime(
            2026, 8, 29, 12, 59, 0, tzinfo=UTC
        ),
        constructed_source_bindings={
            "coordination-config": coordination_config_binding,
        },
    )
    coordination_config = ingest_service._run_source(
        coordination_config_options,
        coordination_config_entry,
        "coordination-config",
    )
    assert coordination_config["status"] == "OK", coordination_config
    assert coordination_config["admission_status"] == "accepted"
    coordination_entry = replace(
        coordination_entry,
        admission_contract=STRICT_ADMISSION_CONTRACT,
    )
    dependent_packet_path = tmp_path / "coordination" / "proof.jsonl"
    _write_jsonl(dependent_packet_path, [{
        "format": "okg-workspace-coordination-packet/v1",
        "id": "issue-1318-dependent-proof",
        "kind": "proof_of_work",
        "coordination_scope": "fixture",
        "observed_at": "2026-08-29T13:00:00Z",
        "recorded_by": "okg-workspace-supervisor",
        "payload": {
            "proof_id": "issue-1318-dependent-proof",
            "bundle_id": "issue-1318-bootstrap-identity-authority",
            "agent_id": "okg-workspace-supervisor",
            "work_area_id": "okg-workspace",
            "command": "codex-sessions exact bootstrap proof",
            "exit_code": 0,
            "status": "passed",
            "output_digest": "sha256:" + ("1" * 64),
            "git_head": "2274cfadcf6a8c6e6e74ad589f9bbf3cc13038cd",
        },
    }])
    dependent_adapter = CoordinationPacketSource(
        repo="okg",
        repo_root=repo,
        config_path=deployment_dir / "coordination.yaml",
        packet_dirs=[],
        packet_globs=[str(dependent_packet_path)],
        emit_config_edges=True,
        emit_heartbeat_role_agent_nodes=False,
        emit_task_target_edges=False,
        emit_external_reference_edges=False,
    )
    dependent_binding = ConstructedSourceAdapter(
        adapter=dependent_adapter,
        executable_profile=coordination_entry.source_class,
        registry_name="coordination-packets",
        ownership_id=coordination_entry.ownership_id,
        graph_branch="default",
        registry_binding_fingerprint=bind_source_adapter_identity(
            dependent_adapter,
            coordination_entry,
        ),
    )
    dependent_options = IngestOptions(
        dsn=okg_migrated,
        deployment=deployment_name,
        deployment_dir=deployment_dir,
        include={"coordination-packets"},
        include_predecessors=False,
        include_dependents=False,
        publish=False,
        admission_observed_at=datetime(
            2026, 8, 29, 13, 0, 0, tzinfo=UTC
        ),
        constructed_source_bindings={
            "coordination-packets": dependent_binding,
        },
    )
    dependent = ingest_service._run_source(
        dependent_options,
        coordination_entry,
        "coordination-packets",
    )
    assert dependent["status"] == "OK", dependent
    assert dependent["admission_status"] == "accepted", dependent
    with psycopg.connect(okg_migrated, autocommit=True) as conn:
        dependent_state = conn.execute(
            """
            SELECT e.status, w.last_applied_rev
              FROM okg.admission_envelopes AS e
              JOIN okg.source_watermarks AS w ON w.source = e.source
             WHERE e.deployment_name = %s
               AND e.source = %s
             ORDER BY e.created_at DESC
             LIMIT 1
            """,
            (deployment_name, coordination_entry.ownership_id),
        ).fetchone()
    assert dependent_state is not None
    assert dependent_state[0] == "accepted"
    assert dependent_state[1]

    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.publisher import Publisher

    published = Publisher(
        okg_migrated,
        deployment_name,
        enrichers=[],
        catalog_identity=bind_test_catalog_writer(okg_migrated, deployment_name),
    ).cycle()
    assert published["status"] == "published", published
    assert isinstance(published.get("generation_id"), str)
    store = ChronosGraphStore.connect(okg_migrated, ensure_schema=False)
    try:
        generation = store.checkout_generation(published["generation_id"])
        rows = generation.query(
            """
            SELECT node_id, subtype
              FROM okg.graph_nodes
             WHERE node_id IN (
                 'session:codex:session-runner-bootstrap',
                 'coordination:proof_of_work:issue-1318-dependent-proof'
             )
             ORDER BY node_id
            """
        )
    finally:
        store.close()
    assert rows == [
        {
            "node_id": "coordination:proof_of_work:issue-1318-dependent-proof",
            "subtype": "proof_of_work",
        },
        {
            "node_id": "session:codex:session-runner-bootstrap",
            "subtype": "agent_session",
        },
    ]


def test_partially_read_session_is_marked_in_progress(tmp_path: Path) -> None:
    """Otherwise the session merely LOOKS short mid-ingest."""
    repo = tmp_path / "repo"
    repo.mkdir()
    root = tmp_path / "codex" / "sessions"
    _write_jsonl(root / "big.jsonl", _session_rows(repo, "session-marker", 20))

    source = _source(repo, root, max_transcript_bytes=800)
    first = source.run("run-1", mode="cursor", cursor={})
    sessions = [
        f for f in first.facts
        if isinstance(f, NodeFact) and f.subtype == "agent_session"
    ]
    assert sessions
    assert sessions[0].attrs.get("ingest_in_progress") is True
    assert int(sessions[0].attrs.get("ingest_consumed_bytes") or 0) > 0

    _, cursor, _ = _drain_to_completion(_source(repo, root, max_transcript_bytes=800))
    final = source.run("run-final", mode="cursor", cursor=cursor)
    final_sessions = [
        f for f in final.facts
        if isinstance(f, NodeFact) and f.subtype == "agent_session"
    ]
    # Re-emission replaces raw_attrs wholesale, so omitting the marker on the
    # completing run is what clears it.
    for node in final_sessions:
        assert "ingest_in_progress" not in node.attrs


def test_live_growing_transcript_does_not_livelock(tmp_path: Path) -> None:
    """append_safe must describe the CONSUMED PREFIX, not the file.

    An actively-appended transcript routinely ends mid-line. Evaluating
    append_safe at end-of-file therefore reports False forever, the resume
    check bails, and every run full-parses from zero and stops at the same
    budget -- zero progress, indefinitely.
    """
    repo = tmp_path / "repo"
    repo.mkdir()
    root = tmp_path / "codex" / "sessions"
    path = root / "live.jsonl"
    _write_jsonl(path, _session_rows(repo, "session-live", 20))
    # A partial trailing line: exactly what a live writer leaves behind.
    with path.open("a", encoding="utf-8") as handle:
        handle.write('{"type": "response_item", "timestamp": "2026')

    source = _source(repo, root, max_transcript_bytes=800)
    first = source.run("run-1", mode="cursor", cursor={})
    list(first.facts)
    cursor = dict(first.next_cursor or {})
    entry = cursor["transcripts"][str(path)]
    first_offset = int(entry["bytes"])
    assert first_offset > 0
    assert entry["append_cursor"]["append_safe"] is True

    # Catch all the way up to the writer, which is where this used to break:
    # a parse that reaches EOF consumes the partial trailing line too, so the
    # stop offset lands mid-line, append_safe goes false, and the NEXT append
    # restarts from byte zero. On a live 817 MB session that resets catch-up
    # on essentially every append.
    offsets = [first_offset]
    for run_no in range(2, 12):
        run = source.run(f"run-{run_no}", mode="cursor", cursor=cursor)
        list(run.facts)
        cursor = dict(run.next_cursor or {})
        offsets.append(int(cursor["transcripts"][str(path)]["bytes"]))
        if not cursor.get("outstanding_backlog"):
            break
    assert not cursor.get("outstanding_backlog"), "never caught up"

    caught_up = cursor["transcripts"][str(path)]
    # The consumed prefix stops at the last COMPLETE line, so it is still
    # resumable -- and the partial line the writer is mid-way through is left
    # for the run that sees it finished.
    assert caught_up["append_cursor"]["append_safe"] is True
    assert int(caught_up["bytes"]) < path.stat().st_size

    # Now the writer finishes that line and adds another.
    with path.open("a", encoding="utf-8") as handle:
        handle.write('-05T00:02:00Z", "payload": {"type": "message", '
                     '"role": "assistant", "content": "tail"}}\n')

    resumed = source.run("run-after-append", mode="cursor", cursor=cursor)
    list(resumed.facts)

    # Resumed, not restarted. A restart here is the sawtooth.
    assert source.last_run_stats["append_delta_files"] == 1
    assert source.last_run_stats["full_parse_files"] == 0
    assert offsets == sorted(offsets), f"offset went backwards: {offsets}"
    assert (
        int(resumed.next_cursor["transcripts"][str(path)]["bytes"])
        == path.stat().st_size
    )


def test_zero_byte_transcript_does_not_degrade_health(tmp_path: Path) -> None:
    """A just-created .jsonl is a normal transient state on a live host.

    Counting it as an enumeration failure sets health `acquire_failed`, which
    admission rejects as `source_health_not_healthy` -- failing the entire
    publish cycle for a file that is merely new.
    """
    repo = tmp_path / "repo"
    repo.mkdir()
    root = tmp_path / "codex" / "sessions"
    _write_jsonl(root / "real.jsonl", _session_rows(repo, "session-ok", 2))
    (root / "just-created.jsonl").write_text("", encoding="utf-8")

    run = _source(repo, root).run("run-1", mode="scope_complete", cursor={})
    facts = list(run.facts)

    assert run.health is None
    assert facts, "the valid transcript was not ingested"


def test_one_failing_candidate_does_not_discard_another_s_progress(
    tmp_path: Path,
) -> None:
    """Health and cursor advance are decoupled.

    Reverting the cursor wholesale on any enumeration failure turned a single
    malformed line anywhere in the backlog into permanent zero progress --
    strictly worse than no chunking, because waiting no longer fixed it.
    """
    repo = tmp_path / "repo"
    repo.mkdir()
    root = tmp_path / "codex" / "sessions"
    good = root / "good.jsonl"
    _write_jsonl(good, _session_rows(repo, "session-good", 20))
    (root / "bad.jsonl").write_text("{not-json}\n", encoding="utf-8")

    source = _source(repo, root, max_transcript_bytes=800)
    first = source.run("run-1", mode="cursor", cursor={})
    list(first.facts)
    cursor = dict(first.next_cursor or {})

    assert first.health is not None
    assert first.health.status == "acquire_failed"
    # The good file's consumed prefix survives its neighbour's failure.
    assert int(cursor["transcripts"][str(good)]["bytes"]) > 0

    second = source.run("run-2", mode="cursor", cursor=cursor)
    list(second.facts)
    assert (
        int(second.next_cursor["transcripts"][str(good)]["bytes"])
        > int(cursor["transcripts"][str(good)]["bytes"])
    ), "progress stalled behind an unrelated failure"


def test_small_transcript_is_not_held_hostage_by_a_large_one(
    tmp_path: Path,
) -> None:
    """A live session must not wait behind an 817 MB backlog file.

    Ordering is what buys this: candidates are taken by ascending unconsumed
    bytes, so a transcript that fits inside the budget completes in the same
    run that a much larger one is still being chunked through.
    """
    repo = tmp_path / "repo"
    repo.mkdir()
    root = tmp_path / "codex" / "sessions"
    huge = root / "huge.jsonl"
    small = root / "small.jsonl"
    _write_jsonl(huge, _session_rows(repo, "session-huge", 200))
    _write_jsonl(small, _session_rows(repo, "session-small", 2))

    source = _source(repo, root, max_transcript_bytes=2_000)
    run = source.run("run-1", mode="cursor", cursor={})
    list(run.facts)

    backlog = {
        item["path"] for item in (run.next_cursor.get("outstanding_backlog") or [])
    }
    assert str(huge) in backlog, "fixture did not leave the large file chunked"
    # The small session finished in the very run that only chunked the big one.
    assert str(small) not in backlog
    assert (
        int(run.next_cursor["transcripts"][str(small)]["bytes"])
        == small.stat().st_size
    )


def test_budget_is_split_so_every_backlog_file_advances_each_run(
    tmp_path: Path,
) -> None:
    """Fair share, not greedy first-come.

    Ordering alone still lets the FIRST backlog file swallow the entire
    remaining budget, leaving every other backlog file to inch forward by its
    one guaranteed line. With N oversized transcripts that serializes catch-up:
    file N makes no real progress until files 1..N-1 have finished.
    """
    repo = tmp_path / "repo"
    repo.mkdir()
    root = tmp_path / "codex" / "sessions"
    first = root / "backlog-a.jsonl"
    second = root / "backlog-b.jsonl"
    # Both far larger than the budget, so both are backlog in every run.
    _write_jsonl(first, _session_rows(repo, "session-a", 300))
    _write_jsonl(second, _session_rows(repo, "session-b", 320))

    budget = 4_000
    source = _source(repo, root, max_transcript_bytes=budget)
    run = source.run("run-1", mode="cursor", cursor={})
    list(run.facts)

    consumed = {
        path: int(run.next_cursor["transcripts"][str(path)]["bytes"])
        for path in (first, second)
    }
    # Neither finished, so this is purely about how the budget was divided.
    assert all(value < path.stat().st_size for path, value in consumed.items())
    # A greedy split gives the trailing file a single line (~450 bytes here);
    # a fair split gives each roughly half the budget.
    assert min(consumed.values()) > budget // 4, (
        f"a backlog file was starved to one line: {consumed}"
    )


def test_a_line_larger_than_the_budget_still_advances(tmp_path: Path) -> None:
    """The budget is a target, not a hard ceiling within a line.

    Without an at-least-one-line guarantee a single oversized line pins the
    offset at zero and catch-up deadlocks.
    """
    repo = tmp_path / "repo"
    repo.mkdir()
    root = tmp_path / "codex" / "sessions"
    _write_jsonl(root / "wide.jsonl", _session_rows(repo, "session-wide", 5))

    source = _source(repo, root, max_transcript_bytes=1)
    _, cursor, runs = _drain_to_completion(source)

    assert runs > 1
    entry = cursor["transcripts"][str(root / "wide.jsonl")]
    assert int(entry["bytes"]) == (root / "wide.jsonl").stat().st_size


def test_out_of_scope_transcript_is_not_re_read_every_run(
    tmp_path: Path,
) -> None:
    """`~/.codex/sessions` holds every project's sessions, not just this repo's.

    A transcript rejected for a foreign cwd is enumerated and parsed for its
    metadata, then abandoned. Leaving no cursor entry made it look new on
    every subsequent run, so it was re-read from zero forever -- on the real
    host that was half the per-run budget spent on files that emit nothing.

    Worse, an out-of-scope read that the budget cut short set the
    "incomplete" flag (suppressing completed scope, so retraction never
    propagates and the reconcile watermark never advances) while never
    appearing in the backlog -- so the stall was invisible.
    """
    repo = tmp_path / "repo"
    repo.mkdir()
    other = tmp_path / "other"
    other.mkdir()
    root = tmp_path / "codex" / "sessions"
    foreign = root / "foreign.jsonl"
    _write_jsonl(foreign, _session_rows(other, "session-foreign", 5))
    _write_jsonl(root / "mine.jsonl", _session_rows(repo, "session-mine", 2))

    source = _source(repo, root)
    first = source.run("run-1", mode="cursor", cursor={})
    list(first.facts)
    cursor = dict(first.next_cursor or {})
    assert source.last_run_stats["files_scanned"] == 2

    second = source.run("run-2", mode="cursor", cursor=cursor)
    list(second.facts)

    # Nothing changed on disk, so nothing is re-read.
    assert source.last_run_stats["files_scanned"] == 0
    assert source.last_run_stats["bytes_delta"] == 0
    # It is recorded as having nothing left to ingest -- not as backlog, and
    # not as a stall.
    assert str(foreign) in cursor["transcripts"]
    assert not (second.next_cursor.get("outstanding_backlog") or [])


def test_an_out_of_scope_backlog_cannot_stall_a_reconcile_invisibly(
    tmp_path: Path,
) -> None:
    """The worst shape of the previous bug: a stall with a clean-looking status.

    An out-of-scope transcript too big for one run set "incomplete" (which
    suppresses completed scope, so retraction never propagates and the
    reconcile watermark never advances) but was never added to the backlog.
    Every run reported completed_scope=False, record_set=None and an EMPTY
    backlog -- so nothing in the operator surface said catch-up was stuck.
    """
    repo = tmp_path / "repo"
    repo.mkdir()
    other = tmp_path / "other"
    other.mkdir()
    root = tmp_path / "codex" / "sessions"
    _write_jsonl(
        root / "foreign-big.jsonl", _session_rows(other, "session-foreign", 60)
    )
    _write_jsonl(root / "mine.jsonl", _session_rows(repo, "session-mine", 2))

    source = _source(repo, root, max_transcript_bytes=1_500)
    cursor: dict[str, Any] = {}
    for run_no in range(1, 8):
        run = source.run(f"run-{run_no}", mode="scope_complete", cursor=cursor)
        list(run.facts)
        cursor = dict(run.next_cursor or {})
        if run.record_set is not None:
            cursor["record_set"] = run.record_set.to_cursor()
        # Either the run is genuinely finished, or the backlog says why not.
        # "not finished AND nothing outstanding" is the invisible stall.
        assert run.completed_scope or cursor.get("outstanding_backlog"), (
            f"run {run_no} declined completeness with an empty backlog"
        )
        if run.completed_scope:
            return
    raise AssertionError("reconcile never completed")


def test_unreadable_candidate_records_only_the_prefix_it_read(
    tmp_path: Path,
) -> None:
    """A candidate that yields no metadata must claim only what it consumed.

    Writing the live fingerprint before the parse asserted consumption that
    never happened, so the unread remainder was skipped as `unchanged`
    forever -- the permanent data gap, reintroduced through the budget path.
    Recording nothing instead is the opposite failure: the file is re-read
    from zero every run. The honest entry is the prefix actually read.
    """
    repo = tmp_path / "repo"
    repo.mkdir()
    root = tmp_path / "codex" / "sessions"
    broken = root / "no-meta.jsonl"
    # Valid JSON lines, but no session_meta -- so no metadata is ever derived.
    root.mkdir(parents=True, exist_ok=True)
    broken.write_text(
        "\n".join(
            json.dumps({"type": "response_item", "payload": {"n": idx}})
            for idx in range(200)
        ) + "\n",
        encoding="utf-8",
    )

    source = _source(repo, root, max_transcript_bytes=500)
    run = source.run("run-1", mode="cursor", cursor={})
    list(run.facts)

    entry = (run.next_cursor.get("transcripts") or {}).get(str(broken))
    assert entry is not None, "no entry: the file would be re-read from zero"
    consumed = int(entry["bytes"])
    assert 0 < consumed < broken.stat().st_size, "claimed more than it read"
    # Unknown content is not bounded content: health still degrades.
    assert run.health is not None
    assert run.health.status == "acquire_failed"
