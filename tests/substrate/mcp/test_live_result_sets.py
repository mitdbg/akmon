"""Retained live answers: keep one large live result and read it again.

``expand(live={..., "retain": true})`` keeps the unwrapped rows of an ``ok``
live call in a sibling of the graph result-reference store; ``filter``,
``map`` and ``aggregate`` read them with ``input={"ref": id}`` without
calling the external system again. Every read re-checks the anchor node,
the binding and the live gate.

The first half is hermetic: the store, the capture rules and the dispatcher
with a fake adapter, gate, sampler and response room. The second half is
Postgres-marked and drives the registered tools on a real publish.
"""
from __future__ import annotations

import collections
import contextlib
import inspect
import json
import logging
import re
import time
import uuid
from typing import Any, Iterator

import pytest

from okg.substrate.external_backed import live_dispatch
from okg.substrate.external_backed.call_sample import digest_of
from okg.substrate.external_backed.live_dispatch import (
    GateOutcome,
    LiveAdapterError,
    LiveSampleWriteFailed,
    dispatch_live_relation,
    live_reserve_envelope,
)
from okg.substrate.external_backed.live_relations import (
    ANNOTATION_KEY,
    bind_live_relations,
    compile_args_model,
    parse_live_bindings,
    parse_live_relations,
)
from okg.substrate.mcp import continuation, live_result_sets, result_references
from okg.substrate.mcp.live_result_sets import (
    LiveResultPredicateUnsupported,
    begin_retain,
    capture,
    fence_rows,
)
from okg.substrate.mcp.response_bounds import ResponseRoom
from okg.substrate.mcp.result_references import (
    ResultReferenceCapacity,
    ResultReferenceInvalid,
    ResultReferenceStore,
)

RELATION_BODY = {
    "action": "external.opensearch.search",
    "principal_mode": "service",
    "sample_policy": "inline",
    "retention_class": "warm",
    "deadline_seconds": 5,
    "args": {
        "q": {"type": "string", "required": True, "maxLength": 200},
        "size": {"type": "integer", "default": 10, "minimum": 1, "maximum": 100},
    },
}
SERVICE = "service:okg-runtime"
BINDING = {
    "deployment": "default", "database": "db", "branch": "main",
    "catalog_version": 1, "authority": "a" * 64, "policy": None,
}


def _rows(n: int) -> list[dict[str, Any]]:
    return [
        {"name": f"row-{i:05d}", "size": i, "note": None if i % 3 else f"n{i}"}
        for i in range(n)
    ]


# ============================================================ hermetic helpers


class CountingAdapter:
    def __init__(self, rows: Any = None, raises: BaseException | None = None):
        self.calls: list[dict[str, Any]] = []
        self.rows = _rows(40) if rows is None else rows
        self.raises = raises

    def __call__(self, **kwargs: Any) -> Any:
        self.calls.append(kwargs)
        if self.raises is not None:
            raise self.raises
        return self.rows


class RecordingSampler:
    def __init__(self, raises: BaseException | None = None):
        self.rows: list[dict[str, Any]] = []
        self.raises = raises

    def __call__(self, **kwargs: Any) -> Any:
        if self.raises is not None:
            raise self.raises
        self.rows.append(kwargs)
        return object()


def _gate(allowed: bool = True):
    def gate(relation, principal, node_id):
        return GateOutcome(
            allowed=allowed, reason="allow" if allowed else "nomos.default_deny",
            principal=principal or SERVICE, action=relation.action,
        )
    return gate


def _bound():
    declared = parse_live_relations(
        {ANNOTATION_KEY: {"hits": dict(RELATION_BODY)}}, subtype="page",
    )
    bindings = parse_live_bindings(
        {"hits": {"kind": "http", "config": {"url": "http://localhost"}}},
    )
    return bind_live_relations(declared, bindings)["hits"]


def _measure(envelope: dict[str, Any]) -> int:
    return len(json.dumps(envelope, separators=(",", ":"), default=str))


def _room(budget: int) -> ResponseRoom:
    return ResponseRoom(budget_bytes=budget, served_bytes=_measure)


class NeverFits:
    """A room nothing fits in: the dispatcher must swap in its error form."""

    budget_bytes = 64

    def fits(self, envelope: dict[str, Any]) -> bool:
        return False


def _hook(store: ResultReferenceStore, bound: Any, *, reader: str = "reader-a",
          claim: dict | None = None, multi: bool = False):
    def hook(raw: Any, base: Any) -> Any:
        return begin_retain(
            raw, base=base, store=store, reader_key=reader, header_claim=claim,
            multi_user=multi, binding=BINDING, relation=bound.relation,
            node_id="node-1", subject=SERVICE, deployment_name="default",
            session_id="session-1",
        )
    return hook


def _dispatch(*, store, adapter, room=None, retain=True, gate=None, sampler=None,
              reader="reader-a", refuse=None) -> tuple[dict[str, Any], RecordingSampler]:
    """One dispatch inside a read call's draft scope, finished as the wrapper
    finishes it: the draft is committed only when the payload carries its id."""
    bound = _bound()
    sampler = sampler or RecordingSampler()
    with result_references.draft_scope() as scope:
        envelope = dispatch_live_relation(
            bound=bound,
            args_model=compile_args_model(bound.relation),
            args={"q": "x"},
            node_id="node-1",
            gate=gate or _gate(True),
            adapter=adapter,
            write_sample=sampler,
            generation_id="7",
            room=room,
            refuse_before_adapter=refuse,
            retain=_hook(store, bound, reader=reader) if retain else None,
        )
        scope.finish({"external_live": envelope})
    return envelope, sampler


@pytest.fixture
def live_store() -> Iterator[ResultReferenceStore]:
    fresh = live_result_sets.new_store()
    previous = live_result_sets.install_live_store(fresh)
    try:
        yield fresh
    finally:
        live_result_sets.install_live_store(previous)


# ============================================================ opt-in


def test_only_an_ok_retained_and_served_call_stores_one_set(live_store) -> None:
    adapter = CountingAdapter()
    envelope, sampler = _dispatch(store=live_store, adapter=adapter)
    assert envelope["state"] == "ok"
    block = envelope["result_reference"]
    assert block["retained"] is True and block["id"].startswith("l1.")
    assert set(block) == {"retained", "id", "expires_at", "rows", "digest",
                          "observed_at", "read_with"}
    assert block["rows"] == 40 and block["observed_at"] == envelope["observed_at"]
    assert live_store.count() == 1 and live_store.pending_count() == 0
    attrs = sampler.rows[0]["attrs"]
    assert attrs["retained"] is True and attrs["retained_rows"] == 40


@pytest.mark.parametrize("state,adapter,gate,expected_state", [
    ("unavailable", CountingAdapter(raises=LiveAdapterError("upstream_unavailable")),
     True, "error"),
    ("denied", CountingAdapter(), False, "denied"),
    ("deadline", CountingAdapter(raises=LiveAdapterError("timeout")), True, "timeout"),
    ("over_cap", CountingAdapter(raises=LiveAdapterError("response_over_cap")),
     True, "error"),
])
def test_no_other_live_state_stores_a_set(live_store, state, adapter, gate,
                                          expected_state) -> None:
    envelope, sampler = _dispatch(store=live_store, adapter=adapter, gate=_gate(gate))
    assert envelope["state"] == expected_state, state
    assert envelope["result_reference"] == {"retained": False, "reason": "state_not_ok"}
    assert live_store.count() == 0 and live_store.pending_count() == 0
    assert sampler.rows[0]["attrs"]["retained"] is False


def test_a_call_refused_before_its_adapter_stores_nothing(live_store) -> None:
    adapter = CountingAdapter()
    envelope, _ = _dispatch(store=live_store, adapter=adapter,
                            refuse="response_byte_budget_exceeded")
    assert adapter.calls == []
    assert envelope["result_reference"]["reason"] == "state_not_ok"
    assert live_store.count() == 0


@pytest.mark.parametrize("retain", [False, None])
def test_retain_absent_or_false_stores_nothing_and_adds_no_block(live_store, retain) -> None:
    envelope, sampler = _dispatch(store=live_store, adapter=CountingAdapter(),
                                  retain=bool(retain))
    assert "result_reference" not in envelope
    assert live_store.count() == 0
    assert "retained" not in sampler.rows[0]["attrs"]


def test_a_failed_sample_write_stores_nothing(live_store) -> None:
    with pytest.raises(LiveSampleWriteFailed):
        _dispatch(store=live_store, adapter=CountingAdapter(),
                  sampler=RecordingSampler(raises=RuntimeError("down")))
    assert live_store.count() == 0 and live_store.pending_count() == 0


def test_the_bounded_error_form_stores_nothing(live_store) -> None:
    envelope, sampler = _dispatch(store=live_store, adapter=CountingAdapter(),
                                  room=NeverFits())
    assert envelope["state"] == "error"
    assert envelope["error"]["reason"] == "response_bound_failed"
    assert "result_reference" not in envelope
    assert live_store.count() == 0 and live_store.pending_count() == 0
    assert sampler.rows[0]["attrs"]["retained"] is False


@pytest.mark.parametrize("raw,reason", [
    ({"hits": [1, 2]}, "rows_not_a_list"),
    ([{"a": 1}, 2], "rows_not_objects"),
    ([{"a": float("nan")}], "rows_not_objects"),
    ([{1: "int key"}], "rows_not_objects"),
])
def test_each_not_kept_reason_is_reported_and_the_answer_is_unchanged(
    live_store, raw, reason,
) -> None:
    plain, _ = _dispatch(store=live_store, adapter=CountingAdapter(rows=raw),
                         retain=False, room=_room(64 * 1024))
    kept, sampler = _dispatch(store=live_store, adapter=CountingAdapter(rows=raw),
                              room=_room(64 * 1024))
    assert kept["result_reference"] == {"retained": False, "reason": reason}
    assert kept.get("rows") == plain.get("rows")
    assert sampler.rows[0]["attrs"]["retained_reason"] == reason
    assert live_store.count() == 0


def test_too_large_is_reported_and_the_prefix_is_still_served(
    live_store, monkeypatch,
) -> None:
    monkeypatch.setattr(live_result_sets, "MAX_ROWS", 10)
    envelope, _ = _dispatch(store=live_store, adapter=CountingAdapter(rows=_rows(200)),
                            room=_room(4096))
    assert envelope["result_reference"] == {"retained": False, "reason": "too_large"}
    assert envelope["truncated"] is True and envelope["rows_returned"] >= 1
    assert live_store.count() == 0


def test_a_full_server_is_reported_as_capacity_and_the_prefix_is_still_served(
    live_store,
) -> None:
    # Re-pinned by PACT read-live-handles (M1): a full server evicts the
    # largest holder's oldest committed set, so only reservations of calls in
    # flight, which nothing evicts, leave no room.
    for i in range(live_result_sets.MAX_PER_SERVER):
        live_store.mint(reader_key=f"other-{i}", binding={}, members=(),
                        header_claim=None, multi_user=False, payload=b"[]",
                        payload_bytes=2, payload_rows=0)
    assert live_store.pending_count() == live_result_sets.MAX_PER_SERVER
    plain, _ = _dispatch(store=live_store, adapter=CountingAdapter(), retain=False,
                         room=_room(4096))
    envelope, sampler = _dispatch(store=live_store, adapter=CountingAdapter(),
                                  room=_room(4096))
    assert envelope["result_reference"] == {"retained": False, "reason": "capacity"}
    assert envelope["rows"] == plain["rows"][: len(envelope["rows"])]
    assert envelope["rows_returned"] >= 1
    assert sampler.rows[0]["attrs"]["retained_reason"] == "capacity"
    assert live_store.pending_count() == live_result_sets.MAX_PER_SERVER
    assert live_store.count() == 0


@pytest.mark.parametrize("budget", [3_000, 8_000, 64 * 1024])
def test_the_envelope_with_its_block_fits_the_budget(live_store, budget) -> None:
    room = _room(budget)
    envelope, _ = _dispatch(store=live_store, adapter=CountingAdapter(rows=_rows(400)),
                            room=room)
    assert envelope["state"] == "ok", envelope.get("error")
    assert "result_reference" in envelope
    assert _measure(envelope) <= budget


@pytest.mark.parametrize("fmt", ["text", "json"])
@pytest.mark.parametrize("each", [None, {"q": ["a", "b", "c"]}])
def test_the_reserve_holds_the_largest_kept_answer_and_error_form(
    live_store, fmt, each,
) -> None:
    """Every live call keeps its answer (PACT read-live-handles), so its
    largest rowless `ok` form carries the largest handle block and the
    longest next token, and an error form carries a not-kept block. The
    `ok` form has no error body, and the reserve's full-size error body
    outweighs the block, the token and their lines; the error form has no
    digest fields, which outweigh a not-kept block. So the reserve holds
    both in JSON and in text without carrying the block itself. Adding the
    block on top refused a live call at the 4 KiB floor before it ran
    (#4060)."""
    from okg.substrate.mcp.response_bounds import public_call_tool_result_bytes
    from okg.substrate.mcp.result_text import result_format_scope

    graph = {"node_id": "node-1", "direction": "out", "neighbors": []}

    def served(envelope: dict[str, Any]) -> int:
        return public_call_tool_result_bytes(
            {**graph, "external_live": envelope}, tool="expand",
        )

    bound = _bound()
    model = compile_args_model(bound.relation)
    reserve = live_reserve_envelope(
        bound=bound, args_model=model, args={} if each else {"q": "x"}, each=each,
    )
    assert "result_reference" not in reserve
    block = live_result_sets.largest_block(reserve["observed_at"])
    digest_form = {key: value for key, value in reserve.items() if key != "error"}
    longest_ok = {
        **digest_form, "result_reference": block,
        "next": live_result_sets.largest_next_token(),
        "next_from_row": live_result_sets.MAX_ROWS, "nested_cut": True,
    }
    denied, _sampler = _dispatch(store=live_store, adapter=CountingAdapter(),
                                 gate=_gate(False))
    assert denied["state"] == "denied" and denied["result_reference"]["retained"] is False
    longest_reason = max(live_result_sets.NOT_KEPT_REASONS, key=len)
    longest_error = {
        **{key: reserve[key] for key in denied if key in reserve},
        "state": "timeout", "error": reserve["error"],
        "result_reference": live_result_sets.not_kept_block(longest_reason),
    }
    with result_format_scope(fmt):
        assert served(reserve) >= served(longest_ok), (served(reserve), served(longest_ok))
        assert served(reserve) >= served(longest_error), (
            served(reserve), served(longest_error),
        )
        assert served(reserve) >= served(denied)


def test_the_largest_block_is_at_least_any_real_block() -> None:
    bound = _bound()
    model = compile_args_model(bound.relation)
    reserve = live_reserve_envelope(bound=bound, args_model=model, args={"q": "x"})
    block = live_result_sets.largest_block(reserve["observed_at"])
    real = live_result_sets.new_store().mint(
        reader_key="r", binding={}, members=(), header_claim=None, multi_user=False,
        payload=b"[]", payload_bytes=2, payload_rows=0,
    )
    assert len(block["id"]) >= len(real.id)
    for reason in live_result_sets.NOT_KEPT_REASONS:
        assert _measure(live_result_sets.not_kept_block(reason)) < _measure(block)


def test_the_published_live_call_schema_adds_only_retain_and_each() -> None:
    from okg.substrate.mcp.operator_models import LiveCall

    schema = LiveCall.model_json_schema()
    assert schema["properties"] == {
        "relation": {"type": "string"},
        "args": {"type": "object"},
        "each": {"additionalProperties": {"anyOf": [
            {"type": "array"},
            {"additionalProperties": {"type": "string"}, "type": "object"},
        ]}, "type": "object"},
        "retain": {"type": "boolean"},
    }
    assert schema["required"] == ["relation"]


def _deep(levels: int) -> Any:
    node: Any = "leaf"
    for _ in range(levels):
        node = [node]
    return node


def _comparable(row: dict[str, Any]) -> dict[str, Any]:
    attrs = {k: v for k, v in row["attrs"].items()
             if k not in {"retained", "retained_reason", "retained_rows"}}
    return {"status": row["status"], "decision": row["decision"],
            "payload": row["payload"], "attrs": attrs}


@pytest.mark.parametrize("levels", [live_dispatch.MAX_RESULT_DEPTH + 2, 20_000])
def test_a_depth_bomb_is_result_unusable_with_and_without_retain(live_store, levels) -> None:
    rows = [{"a": _deep(levels)}]
    plain, plain_sampler = _dispatch(store=live_store, adapter=CountingAdapter(rows=rows),
                                     retain=False)
    kept, kept_sampler = _dispatch(store=live_store, adapter=CountingAdapter(rows=rows))
    for envelope in (plain, kept):
        assert envelope["state"] == "error"
        assert envelope["error"]["failure"] == "result_unusable"
    assert kept["result_reference"] == {"retained": False, "reason": "state_not_ok"}
    assert _comparable(plain_sampler.rows[0]) == _comparable(kept_sampler.rows[0])
    assert live_store.count() == 0 and live_store.pending_count() == 0


class _RaisingCommit:
    """A store whose commit raises (a full store found at commit time)."""

    def __init__(self) -> None:
        self.inner = live_result_sets.new_store()

    def __getattr__(self, name: str) -> Any:
        return getattr(self.inner, name)

    def commit(self, draft: Any) -> Any:
        raise ResultReferenceCapacity("full", data={"reason": "server_references"})


def _reserved(store: Any, reader: str) -> Any:
    return store.mint(reader_key=reader, binding={}, members=(), header_claim=None,
                      multi_user=False, payload=b"[]", payload_bytes=2, payload_rows=0)


def test_a_raising_commit_does_not_strand_the_other_drafts() -> None:
    failing, other = _RaisingCommit(), live_result_sets.new_store()
    first, second = _reserved(failing, "a"), _reserved(other, "b")
    payload = {"result_reference": {"id": first.id},
               "external_live": {"result_reference": {"id": second.id}}}
    with result_references.draft_scope() as scope:
        scope.adopt(failing, first)
        scope.adopt(other, second)
        with pytest.raises(ResultReferenceCapacity):
            scope.finish(payload)
    assert failing.inner.pending_count() == 0 and other.pending_count() == 0


def test_a_raising_commit_at_the_served_point_releases_the_rest() -> None:
    from types import SimpleNamespace

    failing, other = _RaisingCommit(), live_result_sets.new_store()
    first, second = _reserved(failing, "a"), _reserved(other, "b")
    served = SimpleNamespace(
        isError=False, structuredContent=None,
        content=[SimpleNamespace(text=json.dumps([first.id, second.id]))],
    )
    with result_references.serve_scope() as scope:
        scope.adopt(failing, first)
        scope.adopt(other, second)
        with pytest.raises(ResultReferenceCapacity):
            scope.commit_served(served)
    assert failing.inner.pending_count() == 0 and other.pending_count() == 0


def test_live_read_refusals_use_live_words() -> None:
    for reason in ("binding_mismatch", "generation_conflict"):
        error = live_result_sets.invalid(reason, field="anchor")
        assert error.code == "result_reference_invalid"
        assert error.data["reason"] == reason
        assert "retained live answer" in str(error)
        assert error.fix_hint == live_result_sets.FIX_HINT


def test_trace_rows_redact_live_ids_and_keep_graph_ids() -> None:
    from okg.substrate.mcp import tracing

    live_id = _commit(live_result_sets.new_store(), "r")
    graph_id = ResultReferenceStore().mint(
        reader_key="r", binding={}, members=[], header_claim=None, multi_user=False).id
    stored = tracing._stored_json({"input": {"ref": live_id}, "other": graph_id})
    assert live_id not in stored and tracing.LIVE_ID_STAND_IN_PREFIX in stored
    assert graph_id in stored


def test_the_shared_pagination_hint_keeps_a_capped_cursor_null() -> None:
    from okg.substrate.mcp.server import add_pagination_hint

    page = {"truncated": True, "offset": 10_000, "limit": 500,
            **live_result_sets.page_cursor(10_000, 500, True)}
    assert add_pagination_hint(page, "map")["next_offset"] is None


def test_page_cursor_stops_at_the_offset_cap() -> None:
    from okg.substrate.mcp.operator_models import MAX_OFFSET

    assert live_result_sets.page_cursor(0, 50, False) == {"next_offset": None}
    assert live_result_sets.page_cursor(9_000, 1_000, True) == {"next_offset": MAX_OFFSET}
    capped = live_result_sets.page_cursor(9_500, 1_000, True)
    assert capped["next_offset"] is None
    assert capped["paging_limit"]["remaining_from"] == 10_500


def test_projection_reads_upstream_and_served_names_of_rewritten_keys() -> None:
    raw = [{"a\nb": "x", "a b": "y", "ok": 1}]
    served = fence_rows(raw, "hits")
    assert list(served[0]) == ["a b", "a b~2", "ok"]
    items = live_result_sets.project(raw, served, [
        ("raw", "attrs.a\nb"), ("plain", "attrs.a b"),
        ("served", "attrs.a b~2"), ("ok", "attrs.ok"),
    ])
    assert _unfence(items[0]["raw"]) == "x"
    assert _unfence(items[0]["plain"]) == "y"
    assert _unfence(items[0]["served"]) == "y"
    assert items[0]["ok"] == 1


# ============================================================ no argument lookup


def test_two_identical_retained_calls_make_two_adapter_calls_and_two_ids(
    live_store,
) -> None:
    adapter = CountingAdapter()
    first, _ = _dispatch(store=live_store, adapter=adapter)
    second, _ = _dispatch(store=live_store, adapter=adapter)
    assert len(adapter.calls) == 2
    assert first["result_reference"]["id"] != second["result_reference"]["id"]
    assert live_store.count() == 2


def test_the_live_store_api_takes_no_relation_node_or_arguments() -> None:
    forbidden = {"relation", "relation_name", "node", "node_id", "args",
                 "arguments", "query", "args_fingerprint"}
    store = live_result_sets.new_store()
    for name, member in inspect.getmembers(store, callable):
        if name.startswith("_"):
            continue
        params = set(inspect.signature(member).parameters)
        assert not params & forbidden, (name, params & forbidden)


def test_the_id_is_never_in_a_sample_row_or_a_log_line(live_store, caplog) -> None:
    caplog.set_level(logging.DEBUG)
    envelope, sampler = _dispatch(store=live_store, adapter=CountingAdapter())
    set_id = envelope["result_reference"]["id"]
    assert set_id not in json.dumps(sampler.rows, default=str)
    assert all(set_id not in record.getMessage() for record in caplog.records)


# ============================================================ reader binding (store)


def _commit(store: ResultReferenceStore, reader: str, *, claim=None, multi=False,
            rows: int = 3) -> str:
    bound = _bound()
    with result_references.draft_scope() as scope:
        handle = begin_retain(
            _rows(rows), base={"args_fingerprint": "f", "observed_at": "t", "source": "http"},
            store=store, reader_key=reader, header_claim=claim, multi_user=multi,
            binding=BINDING, relation=bound.relation, node_id="node-1", subject=SERVICE,
            deployment_name="default", session_id="s",
        )
        assert handle.kept, handle.block
        handle.finalize(digest="sha256:" + "1" * 64, rows_returned=1, refetch=None)
        scope.finish({"external_live": {"result_reference": handle.block}})
    return handle.block["id"]


def _reason(store: ResultReferenceStore, set_id: str, reader: str) -> str:
    with pytest.raises(ResultReferenceInvalid) as caught:
        store.lookup(set_id, reader_key=reader)
    return caught.value.data["reason"]


def test_another_reader_and_a_never_issued_id_are_both_unknown() -> None:
    store = live_result_sets.new_store()
    theirs = _commit(store, "reader-b")
    never = store.mint(reader_key="nobody", binding={}, members=(),
                       header_claim=None, multi_user=False,
                       payload=b"[]", payload_bytes=2, payload_rows=0)
    store.release(never.id)
    assert _reason(store, theirs, "reader-a") == "unknown"
    assert _reason(store, never.id, "reader-a") == "unknown"


def test_forged_expired_and_lost_ids_refuse_by_name() -> None:
    clock = [time.time()]
    store = live_result_sets.new_store(clock=lambda: clock[0])
    set_id = _commit(store, "reader-a")
    forged = set_id[:-4] + ("AAAA" if not set_id.endswith("AAAA") else "BBBB")
    assert _reason(store, forged, "reader-a") == "signature_invalid"
    assert _reason(live_result_sets.new_store(), set_id, "reader-a") == "store_lost"
    clock[0] += live_result_sets.LIFETIME_SECONDS + 1
    assert _reason(store, set_id, "reader-a") == "expired"
    with pytest.raises(ResultReferenceInvalid) as caught:
        store.lookup(set_id, reader_key="reader-a")
    assert caught.value.fix_hint == live_result_sets.FIX_HINT


def test_a_graph_id_and_a_live_id_never_validate_in_each_others_store() -> None:
    graph = ResultReferenceStore()
    live = live_result_sets.new_store()
    live.epoch = graph.epoch  # the same process epoch: only the label differs
    graph_id = graph.mint(reader_key="r", binding={}, members=[], header_claim=None,
                          multi_user=False).id
    live_id = _commit(live, "r")
    assert live_id.startswith("l1.") and graph_id.startswith("r1.")
    assert _reason(live, graph_id, "r") == "malformed"
    assert _reason(graph, live_id, "r") == "malformed"
    # The same body under the other version: the MAC label differs.
    assert _reason(live, "l1." + graph_id[3:], "r") == "signature_invalid"
    assert _reason(graph, "r1." + live_id[3:], "r") == "signature_invalid"


def test_a_role_removed_from_the_claim_makes_the_id_unknown() -> None:
    claim = {"provider_id": "sso", "issuer": "https://idp/", "external_subject": "bob",
             "roles": ["reader", "analyst"]}
    narrower = {**claim, "roles": ["reader"]}
    store = live_result_sets.new_store()
    reader = continuation.reader_key(None, claim, True)
    set_id = _commit(store, reader, claim=claim, multi=True)
    assert store.lookup(set_id, reader_key=reader).id == set_id
    assert _reason(store, set_id, continuation.reader_key(None, narrower, True)) == "unknown"


# ============================================================ limits (store)


def test_a_reader_keeps_64_sets_and_its_oldest_is_evicted() -> None:
    # Re-pinned by PACT read-live-handles (M1), then read-multi-handle-input
    # (Jason board d275): 64 per reader and session.
    store = live_result_sets.new_store()
    first, *rest = (_commit(store, "reader-a") for _ in range(65))
    assert _reason(store, first, "reader-a") == "evicted"
    assert {store.lookup(i, reader_key="reader-a").id for i in rest} == set(rest)


def test_a_principal_keeps_128_sets_across_its_reader_keys() -> None:
    # Re-pinned by PACT read-live-handles (M1), then read-multi-handle-input:
    # 128 per principal.
    claim = {"provider_id": "sso", "issuer": "https://idp/", "external_subject": "bob"}
    store = live_result_sets.new_store()
    keys = [continuation.reader_key({"note": str(i)}, claim, True) for i in range(129)]
    ids = [_commit(store, key, claim=claim, multi=True) for key in keys]
    assert store.principal_count(continuation.principal_key(claim, True)) == 128
    assert _reason(store, ids[0], keys[0]) == "evicted"


def test_a_full_server_evicts_the_largest_holders_oldest_set() -> None:
    # Re-pinned by PACT read-live-handles (M1): a full server no longer
    # refuses; it evicts the oldest set of the reader holding the most
    # (here every reader holds one: the first by key order), never a
    # reservation.
    store = live_result_sets.new_store()
    held = [_commit(store, f"reader-{i:02d}") for i in range(live_result_sets.MAX_PER_SERVER)]
    late = store.commit(store.mint(reader_key="late", binding={}, members=(), header_claim=None,
                                   multi_user=False, payload=b"[]", payload_bytes=2,
                                   payload_rows=0))
    assert store.count() == 1024 and store.lookup(late.id, reader_key="late").id == late.id
    assert _reason(store, held[0], "reader-00") == "evicted"
    for i, set_id in enumerate(held[1:], 1):
        assert store.lookup(set_id, reader_key=f"reader-{i:02d}").id == set_id


def test_server_bytes_are_capped_at_128_mib() -> None:
    # Re-pinned by PACT read-live-handles (M1): 128 MiB per server.
    store = live_result_sets.new_store()
    big = live_result_sets.MAX_SET_BYTES
    drafts = [
        store.mint(reader_key=f"r{i}", binding={}, members=(), header_claim=None,
                   multi_user=False, payload=b"x", payload_bytes=big, payload_rows=1)
        for i in range(live_result_sets.MAX_TOTAL_BYTES // big)
    ]
    assert len(drafts) == 16
    store.release(drafts[0].id)
    store.mint(reader_key="r-next", binding={}, members=(), header_claim=None,
               multi_user=False, payload=b"x", payload_bytes=1, payload_rows=1)


def test_reservations_count_until_released() -> None:
    store = live_result_sets.new_store()
    drafts = [
        store.mint(reader_key=f"r{i}", binding={}, members=(), header_claim=None,
                   multi_user=False, payload=b"[]", payload_bytes=2, payload_rows=0)
        for i in range(live_result_sets.MAX_PER_SERVER)
    ]
    assert store.pending_count() == 1024 and store.count() == 0
    with pytest.raises(ResultReferenceCapacity):
        store.mint(reader_key="r9", binding={}, members=(), header_claim=None,
                   multi_user=False, payload=b"[]", payload_bytes=2, payload_rows=0)
    store.release(drafts[0].id)
    store.mint(reader_key="r9", binding={}, members=(), header_claim=None,
               multi_user=False, payload=b"[]", payload_bytes=2, payload_rows=0)


def test_the_row_cap_is_50000_rows() -> None:
    rows = [{} for _ in range(live_result_sets.MAX_ROWS)]
    blob, count = capture(rows)
    assert count == 50_000
    with pytest.raises(live_result_sets.NotRetained) as caught:
        capture(rows + [{}])
    assert caught.value.reason == "too_large"


def test_the_byte_cap_counts_unwrapped_rows() -> None:
    # Many short strings: wrapping multiplies this set several times over,
    # so it is kept only because the cap counts the unwrapped bytes.
    rows = [{"a": "x", "b": "y", "c": "z", "d": "w"} for _ in range(30_000)]
    blob, _ = capture(rows)
    wrapped = len(json.dumps(fence_rows(rows, "hits"), separators=(",", ":")).encode())
    assert len(blob) <= live_result_sets.MAX_SET_BYTES < wrapped
    store = live_result_sets.new_store()
    bound = _bound()
    with result_references.draft_scope():
        handle = begin_retain(
            rows, base={}, store=store, reader_key="r", header_claim=None,
            multi_user=False, binding=BINDING, relation=bound.relation,
            node_id="n", subject=SERVICE, deployment_name="d", session_id="s",
        )
        assert handle.kept
        assert handle.draft.canonical_bytes == len(blob)
    too_big = [{"k": "v" * 1000} for _ in range(8_500)]
    with pytest.raises(live_result_sets.NotRetained) as caught:
        capture(too_big)
    assert caught.value.reason == "too_large"


def test_a_set_lives_600_seconds_and_reads_never_extend_it() -> None:
    # Re-pinned by PACT read-live-handles (3.5): 10 minutes from mint.
    clock = [1_000_000.0]
    store = live_result_sets.new_store(clock=lambda: clock[0])
    set_id = _commit(store, "r")
    expires = store.lookup(set_id, reader_key="r").expires_at
    assert expires - clock[0] <= live_result_sets.LIFETIME_SECONDS + 1
    for _ in range(5):
        clock[0] += 50
        assert store.lookup(set_id, reader_key="r").expires_at == expires
    clock[0] = expires
    assert _reason(store, set_id, "r") == "expired"


def test_a_lookup_returns_the_stored_object_without_copying() -> None:
    store = live_result_sets.new_store()
    set_id = _commit(store, "r")
    first = store.lookup(set_id, reader_key="r")
    second = store.lookup(set_id, reader_key="r")
    assert first is second
    assert first.payload.rows_json is second.payload.rows_json
    assert first is store._entries[set_id]  # noqa: SLF001 - identity is the claim


def test_live_and_graph_capacity_are_separate() -> None:
    # A full live store leaves the graph store working ...
    graph = ResultReferenceStore()
    live = live_result_sets.new_store()
    for i in range(live_result_sets.MAX_PER_SERVER):
        _commit(live, f"live-{i}")
    made = graph.commit(graph.mint(reader_key="g", binding={}, members=[],
                                   header_claim=None, multi_user=False))
    assert graph.lookup(made.id, reader_key="g").id == made.id
    assert live.count() == live_result_sets.MAX_PER_SERVER and graph.count() == 1
    # ... and a full graph store leaves the live store working.
    full_graph = ResultReferenceStore(
        limits=result_references.StoreLimits(max_per_server=1),
    )
    full_graph.commit(full_graph.mint(reader_key="g", binding={}, members=[],
                                      header_claim=None, multi_user=False))
    with pytest.raises(ResultReferenceCapacity):
        full_graph.commit(full_graph.mint(reader_key="h", binding={}, members=[],
                                          header_claim=None, multi_user=False))
    assert _commit(live_result_sets.new_store(), "after-graph").startswith("l1.")


# ============================================================ digest consistency


@pytest.mark.parametrize("budget", [2_500, 6_000, None])
def test_retained_rows_wrap_to_the_reported_digest_and_prefix(live_store, budget) -> None:
    rows = _rows(200) + [{"a\nb": "x", "a b": "y", "nested": {"k": ["v", 1]}}]
    room = _room(budget) if budget else None
    envelope, _ = _dispatch(store=live_store, adapter=CountingAdapter(rows=rows),
                            room=room)
    block = envelope["result_reference"]
    entry = live_store.lookup(block["id"], reader_key="reader-a")
    stored = live_result_sets.decode_rows(entry.payload.rows_json)
    rewrapped = fence_rows(stored, "hits")
    assert "sha256:" + digest_of(rewrapped)[0] == block["digest"]
    if envelope.get("truncated"):
        assert envelope["digest"] == block["digest"]
        served = envelope.get("rows", [])
        assert rewrapped[: envelope["rows_returned"]] == served
        assert entry.payload.rows_returned == envelope["rows_returned"]
    else:
        assert rewrapped == envelope["rows"]


# ============================================================ predicate checks


@pytest.mark.parametrize("predicate,argument", [
    ({"op": "eq", "field": "name", "value": "x"}, "predicate"),
    ({"op": "eq", "field": "attrs.a.b", "value": "x"}, "predicate"),
    ({"op": "contains", "field": "attrs", "value": {"a": 1}}, "predicate"),
    ({"op": "and", "clauses": [{"op": "eq", "field": "path", "value": "x"}]},
     "predicate"),
    # contains is text inside one field, never containment on the whole attrs.
    ({"op": "contains", "field": "attrs.a", "value": {"a": 1}}, "predicate"),
])
def test_bare_nested_and_contains_predicates_refuse_by_name(predicate, argument) -> None:
    with pytest.raises(LiveResultPredicateUnsupported) as caught:
        live_result_sets.check_predicate(predicate)
    assert caught.value.code == "live_result_predicate_unsupported"
    assert caught.value.data["argument"] == argument


def test_group_counts_equal_a_counter_largest_first() -> None:
    rows = _rows(1000)
    for row in rows:
        row["bucket"] = row["size"] % 7
    groups = live_result_sets.group_counts(rows, ["attrs.bucket", "attrs.note"])
    expected = collections.Counter((row["bucket"], row["note"]) for row in rows)
    assert {key: count for key, count in groups} == dict(expected)
    counts = [count for _, count in groups]
    assert counts == sorted(counts, reverse=True)


# ============================================================ Postgres: the tools


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


def _publish(dsn: str) -> str:
    import psycopg

    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.library.sources.base import EdgeFact, NodeFact
    from okg.substrate.publisher import Publisher
    from tests.pytest.catalog_fixture_builders import (
        SubtypeSpec,
        bind_test_catalog_writer,
        seed_catalog,
    )

    sfx = uuid.uuid4().hex[:8]
    hub, peer = f"n:hub-{sfx}", f"n:peer-{sfx}"
    with psycopg.connect(dsn, autocommit=True) as conn:
        seed_catalog(conn, subtypes=[SubtypeSpec("page")],
                     narrowings=[("page", "cites", "page")])
        ChronosGraphStore.from_connection(conn).apply_facts([
            NodeFact(hub, "page", {"text": "hub"}, {"id": hub}, {"v": 1}),
            NodeFact(peer, "page", {"text": "peer"}, {"id": peer}, {"v": 1}),
            EdgeFact(src=peer, dst=hub, edge_type="cites", attrs={},
                     source_record_id={"src": peer, "dst": hub},
                     source_revision={"v": 1}),
        ])
    published = Publisher(
        dsn, "default", catalog_identity=bind_test_catalog_writer(dsn, "default"),
    ).cycle()
    assert published["status"] == "published", published
    _declare(dsn)
    return hub


#: Two relations on `page`: one samples inline, one samples nothing.
RELATIONS = {
    "lrs_rows": dict(RELATION_BODY),
    "lrs_quiet": {**RELATION_BODY, "sample_policy": "none"},
}


def _declare(dsn: str) -> None:
    """Stamp both relations onto `page` in every catalog version, adding the
    `page` row where a fixture's catalog version has none."""
    import psycopg

    declared = parse_live_relations({ANNOTATION_KEY: RELATIONS}, subtype="page")
    stored = json.dumps({"live_relations": {
        name: rel.to_metadata() for name, rel in declared.items()}})
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(
            "UPDATE okg.subtypes SET metadata = COALESCE(metadata, '{}'::jsonb) "
            "|| %s::jsonb WHERE name = 'page'",
            (stored,),
        )
        conn.execute(
            "INSERT INTO okg.subtypes (name, metadata, catalog_version_id) "
            "SELECT 'page', %s::jsonb, v.catalog_version_id FROM okg.catalog_versions v "
            "WHERE NOT EXISTS (SELECT 1 FROM okg.subtypes s WHERE s.name = 'page' "
            "AND s.catalog_version_id = v.catalog_version_id)",
            (stored,),
        )


class _World(dict):
    def __repr__(self) -> str:  # never print the DSN in a failure report
        return f"<world hub={self.get('hub')!r}>"


@contextlib.contextmanager
def _built(dsn: str, adapter: Any, tmp_path, monkeypatch, **launch: Any) -> Iterator[Any]:
    from okg.substrate.mcp.server import build_server
    from tests.substrate.external_backed.test_live_relation_call_tool import (
        _deployment_with_bindings,
    )

    bindings = {name: {"kind": "http", "config": {"url": "http://localhost"}}
                for name in RELATIONS}
    _deployment_with_bindings(tmp_path, bindings)
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(tmp_path))
    monkeypatch.setenv("OKG_LIVEBIND_DSN", dsn)
    launch.setdefault("deployment_name", "default")
    mcp = build_server(dsn=dsn, live_adapters={"http": adapter},
                       auto_advance="always", **launch)
    try:
        yield mcp
    finally:
        for attr in ("_okg_pool", "_okg_write_pool"):
            pool = getattr(mcp, attr, None)
            if pool is not None:
                pool.close()


class CountingGate:
    def __init__(self, allowed: bool = True):
        self.allowed = allowed
        self.calls: list[tuple[str, str, str]] = []

    def __call__(self, relation, principal, node_id):
        self.calls.append((relation.name, principal, node_id))
        return _gate(self.allowed)(relation, principal, node_id)


@pytest.fixture
def world(clean_runtime: str, tmp_path, monkeypatch, live_store) -> Iterator[_World]:
    hub = _publish(clean_runtime)
    adapter = CountingAdapter(rows=_rows(2_000))
    with _built(clean_runtime, adapter, tmp_path, monkeypatch) as mcp:
        gate = CountingGate()
        mcp._okg_live_gate = gate  # type: ignore[attr-defined]
        yield _World(dsn=clean_runtime, hub=hub, mcp=mcp, adapter=adapter,
                     gate=gate, store=live_store, tmp_path=tmp_path)


def _serve(mcp: Any, tool: str, arguments: dict[str, Any], fmt: str = "json") -> Any:
    import anyio

    from okg.substrate.mcp.result_text import result_format_scope

    async def drive() -> Any:
        with result_format_scope(fmt):
            return await mcp.call_tool(tool, arguments)

    return anyio.run(drive)


def _ok(mcp: Any, tool: str, arguments: dict[str, Any]) -> dict[str, Any]:
    from tests.substrate.mcp._result_payload import payload

    result = _serve(mcp, tool, arguments)
    body = payload(result)
    assert result.isError is False, body
    return body


def _refused(mcp: Any, tool: str, arguments: dict[str, Any]) -> dict[str, Any]:
    from tests.substrate.mcp._result_payload import payload

    result = _serve(mcp, tool, arguments)
    body = payload(result)
    assert result.isError is True, body
    return body["error"]


def _retain(world: _World, *, relation: str = "lrs_rows", max_bytes: int = 16_384,
            **extra: Any) -> dict[str, Any]:
    body = _ok(world["mcp"], "expand", {
        "input": {"node_id": world["hub"]}, "direction": "in",
        "live": {"relation": relation, "args": {"q": "x"}, "retain": True},
        "max_bytes": max_bytes, **extra,
    })
    return body["external_live"]


def _samples(dsn: str, node_id: str, relation: str = "lrs_rows") -> list[dict[str, Any]]:
    import psycopg

    with psycopg.connect(dsn, autocommit=True) as conn:
        rows = conn.execute(
            "SELECT tool_name, status, decision, payload_sha256, attrs "
            "FROM okg.call_sample_warm WHERE node_id = %s AND relation = %s "
            "ORDER BY observed_at, sample_id",
            (node_id, relation),
        ).fetchall()
    return [
        {"tool": r[0], "status": r[1], "decision": r[2], "sha": r[3], "attrs": r[4]}
        for r in rows
    ]


def _unfence(value: Any) -> Any:
    if isinstance(value, str) and "\n" in value:
        return value.split("\n")[1]
    return value


#: A listing `/*/Run1*Beta-v1/*` of 1,420 items, and
#: one size call per item totals their size.
FAN_OUT_VALUES = [f"/Alpha{i:04d}/Run1-Beta-v1/KIND" for i in range(1_420)]
#: One row per call, sized like the real per-dataset totals (tens of PB).
FAN_OUT_SIZE = 10_150_662_000_000


@pytest.mark.postgres
@pytest.mark.parametrize("fmt", ["json", "text"])
def test_a_fan_out_over_1420_values_runs_keeps_every_row_and_sums_exactly(
    world, fmt,
) -> None:
    """The reserve was sized from the envelope's `query`, which listed every
    fanned-out value (about 67 B each): 1,420 values reserved 95,860 B and
    the call was refused before it ran under a 60,000 B budget, although the
    reply shows at most 20 rows. The reply now names the first values and
    the count; the kept answer keeps every row, and the total over it is
    exact."""
    from tests.substrate.mcp._result_payload import payload, result_text

    world["adapter"].rows = [{"name": "summary", "size": FAN_OUT_SIZE}]
    budget = 60_000
    result = _serve(world["mcp"], "expand", {
        "input": {"node_id": world["hub"]}, "direction": "in",
        "live": {"relation": "lrs_rows", "each": {"q": FAN_OUT_VALUES}},
        "max_bytes": budget,
    }, fmt)
    text = result_text(result) if fmt == "text" else result.content[0].text
    assert result.isError is False, text[:500]
    assert len(text.encode("utf-8")) <= budget
    assert "response_byte_budget_exceeded" not in text
    assert len(world["adapter"].calls) == 1_420
    (entry,) = world["store"]._entries.values()  # noqa: SLF001
    assert entry.payload.row_count == 1_420
    # The kept answer still knows every value it was asked for.
    assert len(entry.payload.query["q"]) == 1_420
    total = _ok(world["mcp"], "aggregate", {
        "input": {"ref": entry.id}, "by": [], "metrics": ["count", "sum(attrs.size)"],
    })
    (group,) = total["groups"]
    assert group["count"] == 1_420
    assert group["sum(attrs.size)"] == 1_420 * FAN_OUT_SIZE
    assert total["totals_cover"].startswith(
        "1420 rows of lrs_rows(q=<1420 values>, size=10), one call per q (1420 values)",
    ), total["totals_cover"]
    if fmt == "json":
        live = payload(result)["external_live"]
        assert live["query"]["q"] == {"values": 1_420, "first": FAN_OUT_VALUES[:3]}
        assert live["result_reference"]["rows"] == 1_420


@pytest.mark.postgres
@pytest.mark.parametrize("fmt", ["json", "text"])
def test_a_retained_expand_serves_its_prefix_and_its_reference(world, fmt) -> None:
    from tests.substrate.mcp._result_payload import payload, result_text

    budget = 16_384
    result = _serve(world["mcp"], "expand", {
        "input": {"node_id": world["hub"]}, "direction": "in",
        "live": {"relation": "lrs_rows", "args": {"q": "x"}, "retain": True},
        "max_bytes": budget,
    }, fmt)
    assert result.isError is False
    text = result_text(result) if fmt == "text" else result.content[0].text
    assert len(text.encode("utf-8")) <= budget
    assert world["store"].count() == 1
    (entry,) = world["store"]._entries.values()  # noqa: SLF001
    assert entry.id in text
    retained = entry.payload
    rewrapped = fence_rows(live_result_sets.decode_rows(retained.rows_json), "lrs_rows")
    assert "sha256:" + digest_of(rewrapped)[0] == retained.digest
    if fmt == "json":
        # The text form names the set by its id and prints no digest (#3666:
        # no internals on a live reply).
        assert retained.digest in text
        live = payload(result)["external_live"]
        assert live["truncated"] is True
        assert live["result_reference"]["digest"] == live["digest"] == retained.digest
        assert rewrapped[: live["rows_returned"]] == live["rows"]
        assert live["result_reference"]["rows"] == 2_000


@pytest.mark.postgres
def test_two_identical_retained_expands_call_twice_and_never_record_the_id(
    world, caplog,
) -> None:
    import psycopg

    caplog.set_level(logging.DEBUG)
    first = _retain(world)["result_reference"]["id"]
    second = _retain(world)["result_reference"]["id"]
    assert len(world["adapter"].calls) == 2 and first != second
    for set_id in (first, second):
        _ok(world["mcp"], "map", {"input": {"ref": set_id}, "limit": 5})
    with psycopg.connect(world["dsn"], autocommit=True) as conn:
        for table in ("hot", "warm", "cold"):
            for set_id in (first, second):
                found = conn.execute(
                    f"SELECT count(*) FROM okg.call_sample_{table} t "  # noqa: S608
                    "WHERE t::text LIKE %s",
                    (f"%{set_id}%",),
                ).fetchone()[0]
                assert found == 0, table
    for record in caplog.records:
        message = record.getMessage()
        assert first not in message and second not in message


@pytest.mark.postgres
def test_store_refusals_through_the_tools(world) -> None:
    mcp = world["mcp"]
    set_id = _retain(world)["result_reference"]["id"]
    forged = set_id[:-4] + ("AAAA" if not set_id.endswith("AAAA") else "BBBB")
    assert _refused(mcp, "map", {"input": {"ref": forged}})["data"]["reason"] == (
        "signature_invalid")
    other = _refused(mcp, "map", {"input": {"ref": set_id}, "authority": {"note": "b"}})
    assert other["data"]["reason"] == "unknown"
    # A live id under the graph prefix goes to the graph store and refuses.
    as_graph = _refused(mcp, "map", {"input": {"ref": "r1." + set_id[3:]}})
    assert as_graph["code"] == "result_reference_invalid"
    assert as_graph["data"]["reason"] in {"store_lost", "signature_invalid"}
    live_result_sets.install_live_store(live_result_sets.new_store())
    lost = _refused(mcp, "map", {"input": {"ref": set_id}})
    assert lost["data"]["reason"] == "store_lost"
    # Re-pinned by PACT read-live-handles: every read is kept, so the hint
    # no longer names retain=true.
    assert lost["fix_hint"] == live_result_sets.FIX_HINT
    assert len(world["adapter"].calls) == 1


@pytest.mark.postgres
def test_an_expired_set_refuses_through_the_tools(world) -> None:
    clock = [time.time()]
    live_result_sets.install_live_store(live_result_sets.new_store(clock=lambda: clock[0]))
    set_id = _retain(world)["result_reference"]["id"]
    clock[0] += live_result_sets.LIFETIME_SECONDS + 1
    expired = _refused(world["mcp"], "map", {"input": {"ref": set_id}})
    assert expired["data"]["reason"] == "expired"


# ---------------------------------------------------------------- regate


def _assert_dropped(world: _World, set_id: str, calls_before: int) -> None:
    assert world["store"].count() == 0
    error = _refused(world["mcp"], "map", {"input": {"ref": set_id}})
    assert error["data"]["reason"] == "unknown"
    assert len(world["adapter"].calls) == calls_before
    refused = [s for s in _samples(world["dsn"], world["hub"])
               if s["attrs"].get("read_refused")]
    assert refused and refused[-1]["status"] == "denied"
    assert refused[-1]["attrs"]["served_from"] == "retained_answer"


@pytest.mark.postgres
def test_regate_a_the_anchor_no_longer_visible(world) -> None:
    import psycopg

    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.library.sources.base import EdgeFact, NodeFact
    from okg.substrate.publisher import Publisher
    from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer

    set_id = _retain(world)["result_reference"]["id"]
    second_authority = _refused(world["mcp"], "map", {
        "input": {"ref": set_id}, "authority": {"note": "another reader"}})
    assert second_authority["data"]["reason"] == "unknown"
    hub = world["hub"]
    peer = hub.replace("hub", "peer")
    with psycopg.connect(world["dsn"], autocommit=True) as conn:
        ChronosGraphStore.from_connection(conn).apply_facts([
            EdgeFact(src=peer, dst=hub, edge_type="cites", attrs={},
                     source_record_id={"src": peer, "dst": hub},
                     source_revision={"v": 2}, op="R"),
            NodeFact(hub, "page", {}, {"id": hub}, {"v": 2}, op="R"),
        ])
    published = Publisher(
        world["dsn"], "default",
        catalog_identity=bind_test_catalog_writer(world["dsn"], "default"),
    ).cycle()
    assert published["status"] == "published", published
    calls = len(world["adapter"].calls)
    error = _refused(world["mcp"], "map", {"input": {"ref": set_id}})
    assert error["code"] == "result_reference_invalid"
    assert error["data"] == {"reason": "binding_mismatch", "field": "anchor"}
    assert "retained live answer" in error["message"]
    assert error["fix_hint"] == live_result_sets.FIX_HINT
    _assert_dropped(world, set_id, calls)


@pytest.mark.postgres
def test_regate_b_a_catalog_that_drops_the_relation(world) -> None:
    import psycopg

    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.library.sources.base import NodeFact
    from okg.substrate.publisher import Publisher
    from tests.pytest.catalog_fixture_builders import (
        SubtypeSpec,
        bind_test_catalog_writer,
        seed_catalog,
    )

    set_id = _retain(world)["result_reference"]["id"]
    with psycopg.connect(world["dsn"], autocommit=True) as conn:
        seed_catalog(conn, subtypes=[SubtypeSpec("page")],
                     narrowings=[("page", "cites", "page")])
        extra = f"n:extra-{uuid.uuid4().hex[:6]}"
        ChronosGraphStore.from_connection(conn).apply_facts([
            NodeFact(extra, "page", {"text": "x"}, {"id": extra}, {"v": 1}),
        ])
    published = Publisher(
        world["dsn"], "default",
        catalog_identity=bind_test_catalog_writer(world["dsn"], "default"),
    ).cycle()
    assert published["status"] == "published", published
    calls = len(world["adapter"].calls)
    error = _refused(world["mcp"], "map", {"input": {"ref": set_id}})
    assert error["data"] == {"reason": "binding_mismatch", "field": "catalog"}
    _assert_dropped(world, set_id, calls)


@pytest.mark.postgres
def test_regate_b_a_changed_policy_pin(world) -> None:
    import psycopg

    from okg.substrate.nomos import activate_policy_bundle, compile_policy_graph

    set_id = _retain(world)["result_reference"]["id"]
    bundle = compile_policy_graph({
        "policy_bundle_id": f"nomos:test:lrs:{uuid.uuid4().hex[:8]}",
        "policy_version": "v1",
        "default_effect": "allow",
        "rules": [{"rule_id": "allow_read", "effect": "allow", "priority": 100,
                   "subjects": ["*"], "actions": ["graph.read.mcp"]}],
    })
    with psycopg.connect(world["dsn"], autocommit=True) as conn:
        activate_policy_bundle(conn, deployment_name="default", bundle=bundle,
                               scope="deployment:default",
                               activated_by="test:operator", bootstrap=True)
    calls = len(world["adapter"].calls)
    error = _refused(world["mcp"], "map", {"input": {"ref": set_id}})
    assert error["data"] == {"reason": "binding_mismatch", "field": "authority"}
    _assert_dropped(world, set_id, calls)


@pytest.mark.postgres
def test_regate_c_the_gate_denies(world) -> None:
    set_id = _retain(world)["result_reference"]["id"]
    gate = world["gate"]
    asked = len(gate.calls)
    gate.allowed = False
    calls = len(world["adapter"].calls)
    error = _refused(world["mcp"], "filter", {"input": {"ref": set_id}, "predicate": {}})
    assert error["code"] == "live_result_denied"
    assert error["data"]["reason"] == "nomos.default_deny"
    assert gate.calls[asked:] == [("lrs_rows", SERVICE, world["hub"])]
    _assert_dropped(world, set_id, calls)


@pytest.mark.postgres
def test_regate_c_the_real_gate_raising_inside_evaluate_denies(world, monkeypatch) -> None:
    from okg.substrate.nomos import core

    class Allow:
        effect = "allow"
        reason_code = "allow"
        policy_fingerprint = None

    real = live_dispatch.build_live_gate("default", deployments_root=world["tmp_path"])
    world["mcp"]._okg_live_gate = real
    monkeypatch.setattr(core, "evaluate", lambda *a, **k: Allow())
    set_id = _retain(world)["result_reference"]["id"]

    def boom(*args: Any, **kwargs: Any) -> Any:
        raise RuntimeError("bundle exploded")

    monkeypatch.setattr(core, "evaluate", boom)
    calls = len(world["adapter"].calls)
    error = _refused(world["mcp"], "aggregate", {
        "input": {"ref": set_id}, "by": ["attrs.note"], "metrics": ["count"]})
    assert error["code"] == "live_result_denied"
    assert error["data"]["reason"] == live_dispatch.GATE_ERROR_REASON
    _assert_dropped(world, set_id, calls)


# ---------------------------------------------------------------- reads


WIDE_ROWS = 12_000


def _wide_rows(n: int) -> list[dict[str, Any]]:
    kinds = ["alpha", "beta", "gamma", None]
    return [
        {
            "name": f"name-{i:05d}",
            "kind": kinds[i % 4],
            "size": None if i % 10 == 0 else (i * 7) % 501,
        }
        for i in range(n)
    ]


@pytest.fixture
def wide(world) -> _World:
    # 12,000 rows of three fields is 48,001 values, inside the real shaping
    # cap (50,000), and past what plain paging reaches (offset 10,000 + 1,000).
    world["rows"] = _wide_rows(WIDE_ROWS)
    world["adapter"].rows = world["rows"]
    world["set"] = _retain(world, max_bytes=32_768)["result_reference"]["id"]
    return world


def _reference(rows: list[dict[str, Any]], predicate: dict[str, Any]) -> list[str]:
    def value(row, field):
        return row.get(field.split(".", 1)[1])

    def match(row, node) -> bool:
        op = node["op"]
        if op == "and":
            return all(match(row, c) for c in node["clauses"])
        if op == "or":
            return any(match(row, c) for c in node["clauses"])
        if op == "not":
            return not match(row, node["clause"])
        v = value(row, node["field"])
        if op == "eq":
            return v == node["value"]
        if op == "in":
            return v in node["values"]
        if op == "exists":
            return node["field"].split(".", 1)[1] in row
        if op == "prefix":
            return isinstance(v, str) and v.startswith(node["value"])
        if op == "suffix":
            return isinstance(v, str) and v.endswith(node["value"])
        if op in ("lt", "gt"):
            if not isinstance(v, (int, float)):
                return False
            return v < node["value"] if op == "lt" else v > node["value"]
        if op == "between":
            low, high = node["values"]
            return isinstance(v, (int, float)) and low <= v <= high
        raise AssertionError(op)

    return [row["name"] for row in rows if match(row, predicate)]


PREDICATES = [
    {"op": "eq", "field": "attrs.kind", "value": "beta"},
    {"op": "eq", "field": "attrs.kind", "value": None},
    {"op": "in", "field": "attrs.kind", "values": ["alpha", "gamma"]},
    {"op": "exists", "field": "attrs.size"},
    {"op": "lt", "field": "attrs.size", "value": 40},
    {"op": "gt", "field": "attrs.size", "value": 480},
    {"op": "between", "field": "attrs.size", "values": [100, 110]},
    {"op": "prefix", "field": "attrs.name", "value": "name-011"},
    {"op": "suffix", "field": "attrs.name", "value": "7"},
    {"op": "not", "clause": {"op": "eq", "field": "attrs.kind", "value": "alpha"}},
    {"op": "and", "clauses": [
        {"op": "eq", "field": "attrs.kind", "value": "gamma"},
        {"op": "or", "clauses": [
            {"op": "prefix", "field": "attrs.name", "value": "name-00"},
            {"op": "gt", "field": "attrs.size", "value": 400},
        ]},
    ]},
]


@pytest.mark.postgres
def test_paging_returns_upstream_order_and_is_stable(wide) -> None:
    from okg.substrate.mcp.operator_models import MAX_OFFSET

    # Follow next_offset until it is null: every cursor handed out can be
    # followed, and past the published offset cap the page says so instead.
    names: list[str] = []
    offset: int | None = 0
    pages: list[dict[str, Any]] = []
    while offset is not None:
        page = _ok(wide["mcp"], "map", {
            "input": {"ref": wide["set"]}, "projection": ["attrs.name"],
            "limit": 1000, "offset": offset})
        assert page["served_from"] == "retained_answer"
        assert page["original_call"]["rows"] == WIDE_ROWS
        assert page["original_call"]["digest"].startswith("sha256:")
        names += [_unfence(item["attrs_name"]) for item in page["items"]]
        offset = page["next_offset"]
        assert offset is None or offset <= MAX_OFFSET
        pages.append(page)
        assert len(pages) < 400
    last = pages[-1]
    assert last["truncated"] is True
    assert last["paging_limit"]["reason"] == "offset_cap"
    assert last["paging_limit"]["max_offset"] == MAX_OFFSET
    assert "attrs.<field>" in last["paging_limit"]["remedy"]
    assert MAX_OFFSET < len(names) < WIDE_ROWS
    assert names == [row["name"] for row in wide["rows"]][: len(names)]
    tail = _ok(wide["mcp"], "filter", {
        "input": {"ref": wide["set"]}, "select": ["attrs.name"], "limit": 5,
        "predicate": {"op": "prefix", "field": "attrs.name", "value": "name-1199"}})
    assert [_unfence(i["attrs_name"]) for i in tail["items"]] == [
        row["name"] for row in wide["rows"][11_990:11_995]]
    again = _ok(wide["mcp"], "map", {"input": {"ref": wide["set"]},
                                     "projection": ["attrs.name"], "limit": 50,
                                     "offset": 777})
    assert [_unfence(i["attrs_name"]) for i in again["items"]] == names[777:827]
    assert len(wide["adapter"].calls) == 1


@pytest.mark.postgres
@pytest.mark.parametrize("predicate", PREDICATES, ids=lambda p: json.dumps(p)[:60])
def test_every_predicate_matches_a_python_reference(wide, predicate) -> None:
    expected = _reference(wide["rows"], predicate)
    page = _ok(wide["mcp"], "filter", {
        "input": {"ref": wide["set"]}, "predicate": predicate,
        "select": ["attrs.name"], "limit": 100})
    assert page["total_count"] == len(expected)
    assert page["total_count_basis"] == "exact"
    shown = [_unfence(item["attrs_name"]) for item in page["items"]]
    assert shown == expected[: len(shown)] and shown


@pytest.mark.postgres
@pytest.mark.parametrize("by", [["attrs.kind"], ["attrs.kind", "attrs.size"]])
def test_grouped_counts_equal_a_counter_and_count_groups_exactly(wide, by) -> None:
    keys = [field.split(".", 1)[1] for field in by]
    expected = collections.Counter(tuple(row.get(k) for k in keys) for row in wide["rows"])
    body = _ok(wide["mcp"], "aggregate", {
        "input": {"ref": wide["set"]}, "by": by, "metrics": ["count"]})
    assert body["groups_total"] == len(expected) == body["total_count"]
    assert len(body["groups"]) == min(50, len(expected))
    for group in body["groups"]:
        key = tuple(_unfence(group[field]) for field in by)
        assert group["count"] == expected[key]
    counts = [group["count"] for group in body["groups"]]
    assert counts == sorted(counts, reverse=True)
    assert counts[0] == max(expected.values())


@pytest.mark.postgres
def test_served_values_are_wrapped_and_a_page_over_budget_is_cut(wide) -> None:
    from okg.substrate.mcp.response_bounds import BYTE_TRUNCATION_REASON

    page = _ok(wide["mcp"], "map", {"input": {"ref": wide["set"]}, "limit": 1000,
                                    "max_bytes": 8_192})
    assert page["truncated"] is True
    assert page["truncation_reason"] == BYTE_TRUNCATION_REASON
    assert 0 < len(page["items"]) < 1000
    first = page["items"][0]["attrs"]
    assert "UNTRUSTED_DATA" in first["name"] and _unfence(first["name"]) == "name-00000"
    assert first["kind"] is not None and "UNTRUSTED_DATA" in first["kind"]
    assert page["original_call"]["observed_at"]
    assert page["served_from"] == "retained_answer"


@pytest.mark.postgres
@pytest.mark.parametrize("tool,arguments,argument", [
    ("filter", {"predicate": {"op": "eq", "field": "kind", "value": "beta"}}, "predicate"),
    ("filter", {"predicate": {"op": "eq", "field": "attrs.kind.x", "value": "b"}},
     "predicate"),
    ("filter", {"predicate": {"op": "contains", "field": "attrs", "value": {"kind": "b"}}},
     "predicate"),
    # order_by is served now (live_reader.order_rows); a nested path is not.
    ("filter", {"predicate": {}, "order_by": "attrs.size.x"}, "order_by"),
    ("filter", {"predicate": {}, "retain": True}, "retain"),
    ("map", {"projection": ["node_id"]}, "projection"),
    ("map", {"projection": {"n": "attrs.a.b"}}, "projection"),
    ("aggregate", {"by": ["kind"], "metrics": ["count"]}, "by"),
])
def test_each_unsupported_argument_refuses_by_name(wide, tool, arguments, argument) -> None:
    error = _refused(wide["mcp"], tool, {"input": {"ref": wide["set"]}, **arguments})
    assert error["code"] == "live_result_predicate_unsupported"
    assert error["data"]["argument"] == argument
    assert wide["store"].count() == 1


@pytest.mark.postgres
def test_a_metric_other_than_count_refuses_by_name(wide) -> None:
    fn = wide["mcp"]._tool_manager._tools["aggregate"].fn  # noqa: SLF001
    with pytest.raises(LiveResultPredicateUnsupported) as caught:
        fn(input={"ref": wide["set"]}, by=["attrs.kind"], metrics=["sum"])
    assert caught.value.data["argument"] == "metrics"


@pytest.mark.postgres
def test_reads_render_in_the_text_format(world) -> None:
    from tests.substrate.mcp._result_payload import result_text

    set_id = _retain(world)["result_reference"]["id"]
    for tool, arguments in (
        ("map", {"limit": 3}),
        ("filter", {"predicate": {"op": "exists", "field": "attrs.name"}, "limit": 3}),
        ("aggregate", {"by": ["attrs.note"], "metrics": ["count"]}),
    ):
        result = _serve(world["mcp"], tool, {"input": {"ref": set_id}, **arguments}, "text")
        text = result_text(result)
        assert result.isError is False, text
        if tool != "aggregate":
            assert "row-00000" in text and "UNTRUSTED" in text
        else:
            # 2,000 rows: `note` is null on the 1,333 whose index is not a
            # multiple of 3, the largest group, served first.
            assert "1333" in text and "attrs.note" in text


# ---------------------------------------------------------------- audit


@pytest.mark.postgres
def test_the_original_row_and_one_row_per_read_are_written(world) -> None:
    from okg.substrate.external_backed.call_sample import live_call_counts
    import psycopg

    set_id = _retain(world)["result_reference"]["id"]
    pages = [
        _ok(world["mcp"], "map", {"input": {"ref": set_id}, "limit": 7}),
        _ok(world["mcp"], "filter", {
            "input": {"ref": set_id},
            "predicate": {"op": "prefix", "field": "attrs.name", "value": "row-001"},
            "limit": 5}),
    ]
    samples = _samples(world["dsn"], world["hub"])
    assert [s["tool"] for s in samples] == ["expand", "map", "filter"]
    original, *reads = samples
    assert original["attrs"]["retained"] is True
    assert original["attrs"]["retained_rows"] == 2_000
    for sample, page in zip(reads, pages):
        sha = "sha256:" + digest_of(page["items"])[0]
        assert sample["attrs"]["served_from"] == "retained_answer"
        assert sample["attrs"]["served_rows_sha256"] == sha
        assert "sha256:" + sample["sha"] == sha
        assert sample["attrs"]["args_fingerprint"] == original["attrs"]["args_fingerprint"]
        assert sample["status"] == "ok" and sample["decision"] == "allow"
    from datetime import datetime, timedelta, timezone

    from okg.substrate.external_backed.call_sample import read_sample_window

    now = datetime.now(timezone.utc)
    with psycopg.connect(world["dsn"], autocommit=True) as conn:
        counts = live_call_counts(conn, deployment_name="default", relation="lrs_rows",
                                  retention_class="warm")
        window = read_sample_window(
            conn, deployment_name="default", node_id=world["hub"], relation="lrs_rows",
            retention_class="warm", start=now - timedelta(hours=1),
            end=now + timedelta(hours=1),
        )
    assert counts == {"ok": 1, "total": 1}
    assert len(window["rows"]) == 1 and window["rows"][0]["status"] == "ok"


@pytest.mark.postgres
def test_a_none_policy_relation_writes_no_read_rows(world) -> None:
    import psycopg

    set_id = _retain(world, relation="lrs_quiet")["result_reference"]["id"]
    _ok(world["mcp"], "map", {"input": {"ref": set_id}, "limit": 3})
    with psycopg.connect(world["dsn"], autocommit=True) as conn:
        for table in ("hot", "warm", "cold"):
            found = conn.execute(
                f"SELECT count(*) FROM okg.call_sample_{table} "  # noqa: S608
                "WHERE relation = 'lrs_quiet'",
            ).fetchone()[0]
            assert found == 0, table


@pytest.mark.postgres
def test_a_failed_read_sample_write_fails_the_read(world, monkeypatch) -> None:
    from okg.substrate.external_backed import call_sample

    set_id = _retain(world)["result_reference"]["id"]
    original = call_sample.record_live_relation_call_sample

    def failing(conn, **kwargs):
        if kwargs.get("tool_name") == "map":
            raise RuntimeError("sample table unavailable")
        return original(conn, **kwargs)

    monkeypatch.setattr(call_sample, "record_live_relation_call_sample", failing)
    error = _refused(world["mcp"], "map", {"input": {"ref": set_id}, "limit": 3})
    assert error["code"] == "live_sample_write_failed"


@pytest.mark.postgres
def test_no_trace_or_call_log_row_holds_a_live_id(world) -> None:
    import psycopg

    set_id = _retain(world)["result_reference"]["id"]
    _ok(world["mcp"], "map", {"input": {"ref": set_id}, "limit": 3})
    with psycopg.connect(world["dsn"], autocommit=True) as conn:
        for table in ("okg.mcp_trace_events", "okg.mcp_call_log"):
            held = conn.execute(
                f"SELECT count(*) FROM {table} t WHERE t::text LIKE %s",  # noqa: S608
                (f"%{set_id}%",),
            ).fetchone()[0]
            assert held == 0, table
        redacted = conn.execute(
            "SELECT count(*) FROM okg.mcp_trace_events t WHERE t::text LIKE %s",
            ("%l1.withheld.%",),
        ).fetchone()[0]
    assert redacted >= 2  # the expand's response and the read's request


@pytest.mark.postgres
def test_a_read_projects_rewritten_keys_and_reports_them(world) -> None:
    world["adapter"].rows = [{"a\nb": f"x{i}", "a b": f"y{i}", "ok": i} for i in range(30)]
    live = _retain(world)
    set_id = live["result_reference"]["id"]
    page = _ok(world["mcp"], "map", {"input": {"ref": set_id}, "limit": 2, "projection": {
        "raw": "attrs.a\nb", "plain": "attrs.a b", "served": "attrs.a b~2"}})
    first = page["items"][0]
    assert _unfence(first["raw"]) == "x0"
    assert _unfence(first["plain"]) == "y0" == _unfence(first["served"])
    assert page["neutralised_keys"] == live["neutralised_keys"] == ["a\nb"]


# ---------------------------------------------------------------- multi-user


@pytest.mark.postgres
def test_two_principals_a_live_expand_is_refused_and_keeps_nothing(
    clean_runtime, okg_mcp_can_log_in, tmp_path, monkeypatch, live_store,
) -> None:
    """Per-user principals need access rules on, and a live call is refused
    while they are on (issue #3923), so no principal can keep a live answer
    here and there is no answer for another principal to ask for. That one
    principal cannot read another's kept answer is proved on the store in
    the hermetic store tests above, for example
    `test_another_reader_and_a_never_issued_id_are_both_unknown`, and with
    the rules off, by `authority`, in
    `test_a_kept_set_of_one_reader_answers_as_unknown_to_another`."""
    from tests.substrate.mcp.test_principal_mapping import (
        MAPPING,
        PRINCIPAL_A,
        PRINCIPAL_B,
        PRINCIPAL_HEADER,
        SUBJECT_A,
        SUBJECT_B,
        _activate_two_principal_policy,
        _configure_header_identity,
        _publish_two_source_graph,
        request_headers,
    )

    _publish_two_source_graph(clean_runtime)
    _declare(clean_runtime)
    _configure_header_identity(
        clean_runtime, principals={PRINCIPAL_A: SUBJECT_A, PRINCIPAL_B: SUBJECT_B},
    )
    _activate_two_principal_policy(clean_runtime)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", SUBJECT_A)
    adapter = CountingAdapter()
    with _built(clean_runtime, adapter, tmp_path, monkeypatch,
                multi_user_principals=True, principal_mapping=MAPPING) as mcp:
        mcp._okg_live_gate = CountingGate()
        expand = {"input": {"node_id": "node:public-docs"}, "direction": "out",
                  "live": {"relation": "lrs_rows", "args": {"q": "x"}, "retain": True}}
        refusals = []
        for header in (PRINCIPAL_B, PRINCIPAL_A):
            with request_headers({PRINCIPAL_HEADER: header}):
                refusals.append(_refused(mcp, "expand", expand))
    assert [r["code"] for r in refusals] == ["live_refused_under_access_rules"] * 2
    assert [r["data"]["reason"] for r in refusals] == ["access_rules_on"] * 2
    assert live_store.count() == 0
    assert len(adapter.calls) == 0


@pytest.mark.postgres
def test_a_kept_set_of_one_reader_answers_as_unknown_to_another(world) -> None:
    """With access rules off, readers differ by their `authority` argument
    (`continuation.reader_key`). Another reader's id answers exactly as an id
    that was never minted does."""

    from tests.substrate.mcp._result_payload import result_text

    expand = {"input": {"node_id": world["hub"]}, "direction": "in",
              "live": {"relation": "lrs_rows", "args": {"q": "x"}, "retain": True}}
    set_id = _ok(world["mcp"], "expand", {**expand, "authority": {"note": "a"}})[
        "external_live"]["result_reference"]["id"]
    own = _ok(world["mcp"], "map", {"input": {"ref": set_id},
                                    "authority": {"note": "a"}, "limit": 2})
    never = world["store"].mint(reader_key="nobody", binding={}, members=(),
                                header_claim=None, multi_user=False,
                                payload=b"[]", payload_bytes=2, payload_rows=0)
    world["store"].release(never.id)
    stranger = _serve(world["mcp"], "map", {"input": {"ref": set_id},
                                            "authority": {"note": "b"}})
    unknown = _serve(world["mcp"], "map", {"input": {"ref": never.id},
                                           "authority": {"note": "b"}})
    assert own["served_from"] == "retained_answer"
    assert stranger.isError and unknown.isError
    assert result_text(stranger).replace(set_id, "ID") == result_text(unknown).replace(
        never.id, "ID")
    assert len(world["adapter"].calls) == 1


# ---------------------------------------------------------------- next (#3666, Live build 3)
#
# A cut live answer is kept without `retain`, and the reply names ONE call,
# `expand(next="<token>")`, that serves the following rows from the kept
# answer, every field whole, with no second adapter call.

#: The one call a cut reply names, as the text form prints it.
_NEXT_CALL = re.compile(r'next: expand\(next="([^"]+)"\) returns rows ([0-9,]+) onward')


def _cut_expand(world: _World, fmt: str = "json", max_bytes: int = 16_384) -> Any:
    return _serve(world["mcp"], "expand", {
        "input": {"node_id": world["hub"]}, "direction": "in",
        "live": {"relation": "lrs_rows", "args": {"q": "x"}},
        "max_bytes": max_bytes,
    }, fmt)


def _published_expand_arguments(mcp: Any) -> set[str]:
    import anyio

    tools = anyio.run(mcp.list_tools)
    (expand,) = [tool for tool in tools if tool.name == "expand"]
    return set(expand.inputSchema.get("properties") or {})


@pytest.mark.postgres
def test_a_cut_expand_goes_on_by_next_with_no_second_adapter_call(world) -> None:
    from tests.substrate.mcp._result_payload import payload

    first = payload(_cut_expand(world))["external_live"]
    # Re-pinned by PACT read-live-handles (4.1.1): every live answer is kept,
    # and its own handle backs the next token.
    assert first["truncated"] is True and first["result_reference"]["retained"] is True
    assert live_result_sets.next_set_id(first["next"]) == first["result_reference"]["id"]
    kept = first["rows_returned"]
    assert first["next_from_row"] == kept + 1
    assert world["store"].count() == 1, "the cut answer is kept for its token"
    assert len(world["adapter"].calls) == 1

    page = _ok(world["mcp"], "expand", {"next": first["next"]})

    assert len(world["adapter"].calls) == 1, "page 2 asks the live system nothing"
    assert page["result_kind"] == "live_page"
    live = page["external_live"]
    count = live["rows_returned"]
    assert live["offset"] == kept and count > 0
    assert live["served_from"] == "retained_answer"
    assert "pinned_generation_id" not in live
    upstream = _rows(2_000)
    assert [
        {key: _unfence(value) for key, value in row.items()} for row in live["rows"]
    ] == upstream[kept: kept + count]
    if kept + count < 2_000:
        assert live["next_from_row"] == kept + count + 1
    # The original call's sample and one read sample: the read says it was
    # served from the kept answer and that no adapter ran.
    samples = _samples(world["dsn"], world["hub"])
    assert len(samples) == 2
    assert samples[0]["attrs"]["next_from_row"] == kept + 1
    read = samples[1]["attrs"]
    assert read["served_from"] == "retained_answer" and read["adapter_ran"] is False
    assert read["rows_served"] == count


@pytest.mark.postgres
def test_the_text_reply_names_one_published_call_and_its_page_follows(world) -> None:
    from tests.substrate.mcp._result_payload import result_text

    text = result_text(_cut_expand(world, "text"))
    # The live cut line (a session's own one-time warnings also start `! `).
    bangs = [line for line in text.splitlines() if line.startswith(("! rows ", "! no rows "))]
    assert len(bangs) == 1, bangs
    assert len([line for line in text.splitlines() if "next:" in line]) == 1
    found = _NEXT_CALL.search(bangs[0])
    assert found is not None, bangs[0]
    token, start = found.group(1), int(found.group(2).replace(",", ""))
    assert "retain" not in text and "filter(" not in text
    # Gate 1: the call names only arguments the served expand publishes.
    assert "next" in _published_expand_arguments(world["mcp"])

    result = _serve(world["mcp"], "expand", {"next": token}, "text")
    page = result_text(result)
    assert result.isError is False, page
    head = page.splitlines()[0]
    assert head.startswith(f"expand ok · live lrs_rows · rows {start}-"), head
    assert head.endswith("of 2,000 · from the kept answer"), head
    numbers = [int(m.group(1)) for line in page.splitlines()
               if (m := re.match(r"^\[(\d+)\] ", line))]
    assert numbers[0] == start and numbers == list(range(start, start + len(numbers)))
    assert f"row-{start - 1:05d}" in page
    assert "generation" not in page and "stale" not in page
    assert len(world["adapter"].calls) == 1


@pytest.mark.postgres
def test_an_expired_or_lost_next_token_says_so_and_names_the_call_that_asks_again(
    world,
) -> None:
    from tests.substrate.mcp._result_payload import payload

    clock = [time.time()]
    live_result_sets.install_live_store(live_result_sets.new_store(clock=lambda: clock[0]))
    token = payload(_cut_expand(world))["external_live"]["next"]
    clock[0] += live_result_sets.LIFETIME_SECONDS + 1

    expired = _refused(world["mcp"], "expand", {"next": token})
    assert expired["code"] == "continuation_token_invalid"
    assert expired["data"]["reason"] == "expired"
    assert "expired" in expired["message"] and "600 seconds" in expired["message"]
    redo = f'call expand(input="{world["hub"]}", live={{"relation":"lrs_rows","args":{{'
    assert expired["fix_hint"].startswith(redo), expired["fix_hint"]
    assert '"q":"x"' in expired["fix_hint"]
    retry = expired["data"]["retry_call"]
    assert retry["tool"] == "expand"
    assert set(retry["arguments"]) <= _published_expand_arguments(world["mcp"])

    live_result_sets.install_live_store(live_result_sets.new_store())
    lost = _refused(world["mcp"], "expand", {"next": token})
    assert lost["data"]["reason"] == "store_lost"
    assert "restarted" in lost["message"]
    assert len(world["adapter"].calls) == 1


@pytest.mark.postgres
@pytest.mark.parametrize("mode,state", [("audit", True), ("enforce", True), ("unknown", None)])
def test_next_on_a_kept_live_answer_is_refused_under_access_rules(world, mode, state) -> None:
    from tests.substrate.mcp._result_payload import payload

    token = payload(_cut_expand(world))["external_live"]["next"]
    assert len(world["adapter"].calls) == 1
    before = len(_samples(world["dsn"], world["hub"]))
    world["mcp"]._okg_access_rules_state = lambda: state
    try:
        error = _refused(world["mcp"], "expand", {"next": token})
    finally:
        del world["mcp"]._okg_access_rules_state
    assert error["code"] == "live_refused_under_access_rules"
    assert error["data"]["reason"] == ("access_rules_on" if state else "access_rules_unknown")
    assert len(world["adapter"].calls) == 1, "no second adapter call"
    assert len(_samples(world["dsn"], world["hub"])) == before, "no new sample"


#: One read of a kept live answer by its id, per tool that reads one.
KEPT_READS: dict[str, dict[str, Any]] = {
    "filter": {"predicate": {"op": "prefix", "field": "attrs.name", "value": "row-0"},
               "limit": 5},
    "map": {"limit": 5},
    "aggregate": {"by": [], "metrics": ["count"]},
}


@pytest.mark.postgres
@pytest.mark.parametrize("tool", sorted(KEPT_READS))
@pytest.mark.parametrize("mode,state", [("audit", True), ("enforce", True), ("unknown", None)])
def test_a_kept_live_answer_is_not_read_once_access_rules_are_on(world, tool, mode, state) -> None:
    """#3666 review M2. A set kept while the access rules were off is not
    served by filter, map or aggregate once they are on (or cannot be told),
    as `expand(next)` is not: refused by name, no row, no sample, no call."""
    set_id = _retain(world)["result_reference"]["id"]
    arguments = {"input": {"ref": set_id}, **KEPT_READS[tool]}
    # Control: with the rules off, the same read is served from the set.
    served = _ok(world["mcp"], tool, arguments)
    assert served.get("items") or served.get("groups"), served
    before = len(_samples(world["dsn"], world["hub"]))
    world["mcp"]._okg_access_rules_state = lambda: state
    try:
        result = _serve(world["mcp"], tool, arguments)
    finally:
        del world["mcp"]._okg_access_rules_state
    from tests.substrate.mcp._result_payload import payload

    body = payload(result)
    assert result.isError is True, str(body)[:600]
    error = body["error"]
    assert error["code"] == "live_refused_under_access_rules", error
    assert error["data"]["reason"] == ("access_rules_on" if state else "access_rules_unknown")
    assert error["data"]["tool_called"] == tool
    assert not body.get("items") and not body.get("groups"), "no row served"
    assert "row-0" not in json.dumps(body), "no kept row in the refusal"
    assert len(world["adapter"].calls) == 1, "no adapter call"
    assert len(_samples(world["dsn"], world["hub"])) == before, "no new sample"
    # Rules off again: the set was not dropped and reads as before.
    again = _ok(world["mcp"], tool, arguments)
    assert again.get("items") == served.get("items")
    assert again.get("groups") == served.get("groups")


@pytest.mark.postgres
def test_an_unknown_or_forged_next_token_is_refused_by_name(world) -> None:
    from tests.substrate.mcp._result_payload import payload

    token = payload(_cut_expand(world))["external_live"]["next"]
    parts = token.split(".")
    parts[5] = str(int(parts[5]) + 7)
    forged = _refused(world["mcp"], "expand", {"next": ".".join(parts)})
    assert forged["data"]["reason"] == "signature_invalid"
    # Another reader's token answers like one never issued.
    other = _refused(world["mcp"], "expand", {"next": token, "authority": {"note": "b"}})
    assert other["data"]["reason"] == "unknown"
    assert "holds no kept live answer" in other["message"]
    mixed = _refused(world["mcp"], "expand", {"next": token, "limit": 5})
    assert mixed["code"] == "next_with_read_arguments"
    assert len(world["adapter"].calls) == 1


# ============================================================ a kept raw answer asks expand's check again


def _deny_expand_only(tool: str) -> None:
    """SIMULATED: the dispatch gate's verdict when the principal may call
    filter, map and aggregate but not expand. The real gate decides per tool
    name; this stands in for that decision."""
    if tool == "expand":
        from okg.substrate.errors import SubstrateError

        err = SubstrateError("denied")
        err.code = "nomos_identity_denied"
        raise err


@pytest.mark.postgres
def test_a_kept_raw_answer_is_not_read_after_the_gate_denies_expand(world) -> None:
    """The set was minted by expand, so a later read asks expand's decision:
    no row once the dispatch gate denies expand, none from `expand(next=)`,
    no adapter call, and the set reads again when the gate allows."""
    mcp = world["mcp"]
    from tests.substrate.mcp._result_payload import payload

    set_id = _retain(world)["result_reference"]["id"]
    token = payload(_cut_expand(world))["external_live"]["next"]
    reads = [(tool, {"input": {"ref": set_id}, **KEPT_READS[tool]}) for tool in KEPT_READS]
    reads.append(("aggregate", {"input": {"ref": set_id}, "by": [], "metrics": ["count"]}))
    served = {tool: _ok(mcp, tool, arguments) for tool, arguments in reads}
    calls = len(world["adapter"].calls)
    mcp._okg_dispatch_principal_gate = _deny_expand_only
    try:
        for tool, arguments in reads + [("expand", {"next": token})]:
            error = _refused(mcp, tool, arguments)
            assert error["code"] == "nomos_identity_denied", (tool, error)
    finally:
        del mcp._okg_dispatch_principal_gate
    assert len(world["adapter"].calls) == calls, "no adapter call"
    for tool, arguments in reads:
        again = _ok(mcp, tool, arguments)
        assert again.get("items") == served[tool].get("items")
        assert again.get("groups") == served[tool].get("groups")


@pytest.mark.postgres
def test_a_kept_raw_answer_is_not_read_after_expand_leaves_the_surface(world) -> None:
    """SIMULATED: a real session's tool list only grows, so this replaces
    `okg_visible_tools` to show the kept raw read asks whether expand is served."""
    mcp = world["mcp"]
    set_id = _retain(world)["result_reference"]["id"]
    served = mcp.okg_visible_tools
    mcp.okg_visible_tools = lambda: frozenset({"filter", "map", "aggregate", "inspect"})
    try:
        for tool in KEPT_READS:
            error = _refused(mcp, tool, {"input": {"ref": set_id}, **KEPT_READS[tool]})
            assert error["data"]["reason"] == "expand_not_served", (tool, error)
    finally:
        mcp.okg_visible_tools = served
    assert _ok(mcp, "aggregate", {"input": {"ref": set_id}, "by": [], "metrics": ["count"]})
