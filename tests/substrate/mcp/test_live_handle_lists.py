"""A hand chain's kept live answers as one set, without a database (PACT
read-multi-handle-input).

The hint memory's chain (`live_run_hint.note_live_call`), the combine rules
on in-memory sets (`live_handle_lists.combine`), and the store paths the
chain handle needs: `held_for`, replace-at-commit with the `replaced`
answer, pinned sources and the raised caps. The reads through the tools are
in `test_live_handle_lists_db.py`.

MF marks a test that fails on the base (c429dfda44, PR #4060 dev-merged) for
the reason its docstring gives. New names are read inside each test, so on
the base each test fails on its own, not the whole file on import.
"""
from __future__ import annotations

import itertools
from dataclasses import dataclass
from typing import Any

import pytest

from okg.substrate.mcp import live_result_sets
from okg.substrate.mcp.result_references import ResultReferenceInvalid

READER = "reader-a"
QUOTA = f"{READER}\x00session-a"
_SESSIONS = itertools.count()


def _lists() -> Any:
    from okg.substrate.mcp import live_handle_lists

    return live_handle_lists


def _hint() -> Any:
    from okg.substrate.mcp import live_run_hint

    return live_run_hint


def _session() -> str:
    return f"mhi-{next(_SESSIONS)}-{id(object())}"


# ============================================================ 1: the hint memory's chain


def _note(session: str, q: Any, *, handle: Any = "auto", ok: bool = True,
          held: Any = None, size: int = 10, exact: Any = ("q",)) -> Any:
    handle = f"l1.id-{q}-{next(_SESSIONS)}" if handle == "auto" else handle
    return _hint().note_live_call(
        session, "n:hub", "lrs_rows", {"q": q, "size": size}, {"q": q},
        handle=handle if ok else None, ok=ok, exact=set(exact),
        held=held or (lambda _id: "committed"),
    )


def test_the_current_call_counts_as_kept_without_a_lookup() -> None:
    """MF 1a: the third kept answer names a chain of 3; the store is asked
    about the two earlier ids only. Base: no `note_live_call`."""
    session = _session()
    asked: list[str] = []

    def held(set_id: str) -> str:
        asked.append(set_id)
        return "committed"

    assert _note(session, "a", held=held)[1] is None
    assert _note(session, "b", held=held)[1] is None
    _run, chain = _note(session, "c", handle="l1.current", held=held)
    assert chain is not None and chain.argument == "q"
    assert chain.values == ["a", "b", "c"] and chain.source_ids[-1] == "l1.current"
    assert "l1.current" not in asked
    assert chain.shared == {"size": 10} and chain.shared_sent == {}


def test_a_failed_call_is_left_out_and_named() -> None:
    """MF 1b: a failed call has no answer; the chain holds the others and
    names it. Base: no chain."""
    session = _session()
    _note(session, "a")
    _note(session, "b", ok=False)
    _note(session, "c")
    _run, chain = _note(session, "d")
    assert chain.values == ["a", "c", "d"] and chain.failed == ["b"]


def test_a_repeated_value_keeps_the_latest_call() -> None:
    """MF 1c: the same value asked twice joins once, as its latest call."""
    session = _session()
    _note(session, "a", handle="l1.first-a")
    _note(session, "b")
    _note(session, "a", handle="l1.second-a")
    _run, chain = _note(session, "c")
    assert chain.values == ["b", "a", "c"]
    assert "l1.second-a" in chain.source_ids and "l1.first-a" not in chain.source_ids


def test_an_answer_no_longer_held_is_named_missing() -> None:
    session = _session()
    _note(session, "a", handle="l1.gone")
    _note(session, "b")
    _note(session, "c")

    def held(set_id: str) -> Any:
        return None if set_id == "l1.gone" else "pending"

    _run, chain = _note(session, "d", held=held)
    assert chain.values == ["b", "c", "d"] and chain.missing == ["a"]


@pytest.mark.parametrize("values", [["/A*", "/B", "/C"], ["a?", "b", "c"], ["a%", "b", "c"]])
def test_a_wildcard_value_mints_no_chain(values) -> None:
    """MF 1d (H2): a value with a wildcard may overlap the others."""
    session = _session()
    results = [_note(session, v) for v in values]
    assert all(chain is None for _run, chain in results)


def test_an_argument_that_is_not_an_exact_identifier_mints_no_chain() -> None:
    """MF 1d (8.2): only a declared string with no enum forms a chain; an
    integer threshold, an enum or a list-typed argument never does."""
    session = _session()
    for size in (10, 20, 30):
        _run, chain = _hint().note_live_call(
            session, "n:hub", "lrs_rows", {"q": "x", "size": size}, {"size": size},
            handle=f"l1.s{size}", ok=True, exact={"q"}, held=lambda _id: "committed",
        )
        assert chain is None
    other = _session()
    for value in (["a"], ["b"], ["c"]):
        _run, chain = _hint().note_live_call(
            other, "n:hub", "lrs_rows", {"q": value}, {"q": value},
            handle="l1.x", ok=True, exact={"q"}, held=lambda _id: "committed",
        )
        assert chain is None


def test_the_run_line_is_unchanged_at_sixteen_calls_and_the_chain_reaches_sixty() -> None:
    """MF 1e: the `run` line still looks back 16 calls (17 values at the
    20th call), while the chain holds all 20 and at most the newest 60."""
    session = _session()
    for i in range(19):
        _note(session, f"v{i:02d}")
    run, chain = _note(session, "v19")
    assert run["values"] == 17 and run["argument"] == "q"
    assert len(chain.values) == 20
    big = _session()
    for i in range(64):
        run, chain = _note(big, f"w{i:02d}")
    assert len(chain.values) == 60 and chain.values[0] == "w04"
    # Review L1: the four older kept answers left out are named.
    assert chain.dropped == ["w00", "w01", "w02", "w03"]
    assert _hint().CALLS_KEPT == 64 and _hint().RUN_CALLS == 16


def test_note_call_keeps_its_old_contract() -> None:
    session = _session()
    note = _hint().note_call
    assert note(session, "n:hub", "r", {"q": "a"}, {"q": "a"}) is None
    assert note(session, "n:hub", "r", {"q": "b"}, {"q": "b"}) is None
    hint = note(session, "n:hub", "r", {"q": "c"}, {"q": "c"})
    assert set(hint) == {"argument", "values", "call"}


def test_each_chain_carries_a_higher_sequence_number() -> None:
    session = _session()
    _note(session, "a")
    _note(session, "b")
    _r, third = _note(session, "c")
    _r, fourth = _note(session, "d")
    assert fourth.seq > third.seq


# ============================================================ 2: combine rules


@dataclass(frozen=True)
class Rel:
    name: str
    action: str = "external.fixture.read"


def _set(q: Any, rows: list[dict[str, Any]], *, relation: str = "lrs_rows",
         node: str = "n:hub", size: int = 10, each: Any = None, **extra: Any) -> Any:
    query = {"q": q, "size": size}
    return live_result_sets.RetainedSet(
        rows_json=live_result_sets.encode_rows(rows), row_count=len(rows),
        relation=Rel(relation), node_id=node, subject="service:okg",
        args_fingerprint="f", observed_at=f"2026-10-25T09:40:{len(str(q)):02d}+00:00",
        source="http", deployment_name="default", session_id="s",
        query=query, each=each, **extra,
    )


def _chain(values: list[Any]) -> Any:
    lists = _lists()
    return lists.LiveChainSet(
        chain_key="chain:k", seq=1, source_ids=tuple(f"l1.s{i}" for i in range(len(values))),
        relation="lrs_rows", node_id="n:hub", varying="q", values=tuple(values),
        shared={"size": 10},
    )


def _opened(sets: list[Any]) -> list[tuple[int, str, Any, list[Any]]]:
    return [
        (i, f"l1.s{i - 1}", s,
         live_result_sets.decode_rows(s.rows_json) if hasattr(s, "rows_json") else [])
        for i, s in enumerate(sets, 1)
    ]


def _refused(chain: Any, sets: list[Any], gone: Any = ()) -> dict[str, Any]:
    lists = _lists()
    with pytest.raises(lists.LiveHandlesNotCombinable) as caught:
        lists.combine(chain, "l1.chain", _opened(sets), list(gone))
    assert caught.value.code == "live_handles_not_combinable"
    data = caught.value.data
    assert set(data) >= {"reason", "index", "handles"}
    return data


def test_combine_equals_the_fan_out_rows_and_counts_overlap() -> None:
    """MF 2: rows in chain order, the varying key first on rows lacking it,
    the fan-out's query, and rows found in two answers counted."""
    sets = [
        _set("a", [{"name": "x", "size": 1}]),
        _set("b", [{"name": "y", "size": 2}, {"name": "x", "size": 1}]),
        _set("c", [{"q": "c", "name": "x", "size": 1}]),
    ]
    combined = _lists().combine(_chain(["a", "b", "c"]), "l1.chain", _opened(sets), [])
    assert combined.rows == [
        {"q": "a", "name": "x", "size": 1},
        {"q": "b", "name": "y", "size": 2},
        {"q": "b", "name": "x", "size": 1},
        {"q": "c", "name": "x", "size": 1},
    ]
    # The upstream echo of `q` in the third answer does not hide the overlap.
    assert combined.block["rows_in_several_answers"] == 1
    assert combined.retained.query == {"q": ["a", "b", "c"], "size": 10}
    assert combined.retained.each == "q"
    assert combined.block["answers"] == 3 and combined.block["of"] == 3
    assert live_result_sets.totals_cover(combined.retained, 4).startswith(
        "4 rows of lrs_rows(q=<3 values>, size=10), one call per q (3 values)",
    )


def test_combine_answers_over_gone_sources_and_names_them() -> None:
    chain = _chain(["a", "b", "c"])
    combined = _lists().combine(
        chain, "l1.chain", _opened([_set("a", [{"n": 1}]), _set("c", [{"n": 3}])]),
        [(2, "b", "evicted")],
    )
    assert combined.block["answers"] == 2 and combined.block["of"] == 3
    (gone,) = combined.block["not_kept"]
    assert gone["index"] == 2 and gone["value"] == "b" and gone["reason"] == "evicted"
    assert gone["retry"]["arguments"]["live"] == {
        "relation": "lrs_rows", "args": {"size": 10, "q": "b"},
    }


@pytest.mark.parametrize("case,expected", [
    ("typed_set", ("typed_set", 2)),
    ("groups_handle", ("groups_handle", 2)),
    ("derived_handle", ("derived_handle", 2)),
    ("relation_differs", ("relation_differs", 2)),
    ("anchor_differs", ("anchor_differs", 2)),
    ("args_differ", ("args_differ", 2)),
    ("value_twice", ("value_twice", 2)),
    ("same_call", ("same_call", 2)),
])
def test_combine_refuses_each_rule_with_the_source_position(case, expected) -> None:
    """MF 2: every 1c reason, with the 1-based source position."""
    first = _set("a", [{"n": 1}])
    second = {
        "typed_set": object(),
        "groups_handle": live_result_sets.groups_set(
            [], columns=["count"], source=first, from_id="l1.s0", order="count desc",
        ),
        "derived_handle": _set("b", [{"n": 2}], derived_from="l1.s0"),
        "relation_differs": _set("b", [{"n": 2}], relation="lrs_other"),
        "anchor_differs": _set("b", [{"n": 2}], node="n:peer"),
        "args_differ": _set("b", [{"n": 2}], size=20),
        "value_twice": _set(["a", "z"], [{"n": 2}], each="q"),
        "same_call": _set("a", [{"n": 2}]),
    }[case]
    data = _refused(_chain(["a", "b"]), [first, second])
    assert (data["reason"], data["index"]) == expected
    pair = case in ("same_call", "value_twice")
    assert data["handles"] == (["l1.s0", "l1.s1"] if pair else ["l1.s1"])


def test_combine_refuses_too_many_values_and_too_large_for_the_whole_chain(monkeypatch) -> None:
    lists = _lists()
    many = _set([f"v{i}" for i in range(1_500)], [{"n": 1}], each="q")
    data = _refused(_chain(["a", "fan"]), [_set("a", [{"n": 0}]), many])
    assert (data["reason"], data["index"]) == ("too_many_values", 0)
    monkeypatch.setattr(live_result_sets, "MAX_ROWS", 2)
    data = _refused(_chain(["a", "b"]), [_set("a", [{"n": 1}, {"n": 2}]), _set("b", [{"n": 3}])])
    assert (data["reason"], data["index"]) == ("too_large", 0)
    monkeypatch.setattr(live_result_sets, "MAX_ROWS", 50_000)
    monkeypatch.setattr(live_result_sets, "MAX_SET_BYTES", 40)
    data = _refused(_chain(["a", "b"]), [_set("a", [{"n": "x" * 30}]), _set("b", [{"n": 3}])])
    assert data["reason"] == "too_large"
    assert lists.MAX_VALUES == 1_500


def test_with_no_source_kept_the_read_is_refused_no_source_kept() -> None:
    data = _refused(_chain(["a", "b"]), [], gone=[(1, "a", "expired"), (2, "b", "evicted")])
    assert (data["reason"], data["index"]) == ("no_source_kept", 0)


def test_the_read_window_prints_first_and_last_read() -> None:
    lists = _lists()
    assert lists.read_window(
        ["2026-10-25T09:52:19.5+00:00", "2026-10-25T09:40:12+00:00"],
    ) == "09:40:12-09:52:19Z"


# ============================================================ store paths


def _store(clock: Any = None) -> Any:
    return live_result_sets.new_store(**({"clock": clock} if clock else {}))


def _mint(store: Any, quota: str = QUOTA, *, reader: str = READER, nbytes: int = 2,
          **chain: Any) -> Any:
    return store.mint(
        reader_key=reader, binding={}, members=(), header_claim=None, multi_user=False,
        payload=b"[]", payload_bytes=nbytes, payload_rows=0, quota_key=quota, **chain,
    )


def _commit(store: Any, quota: str = QUOTA, **kwargs: Any) -> str:
    return store.commit(_mint(store, quota, **kwargs)).id


def _reason(store: Any, set_id: str, quota: str = QUOTA, reader: str = READER) -> dict[str, Any]:
    with pytest.raises(ResultReferenceInvalid) as caught:
        store.lookup(set_id, reader_key=reader, quota_key=quota)
    return caught.value.data


def test_held_for_names_committed_pending_and_gone() -> None:
    """MF 8.3: a sibling call's draft counts as kept. Base: no `held_for`."""
    store = _store()
    committed = _commit(store)
    pending = _mint(store)
    assert store.held_for(committed, reader_key=READER, quota_key=QUOTA) == "committed"
    assert store.held_for(pending.id, reader_key=READER, quota_key=QUOTA) == "pending"
    other = f"{READER}\x00session-b"
    assert store.held_for(committed, reader_key=READER, quota_key=other) is None
    assert store.held_for(committed, reader_key="reader-b", quota_key="reader-b") is None
    store.release(pending.id)
    assert store.held_for(pending.id, reader_key=READER, quota_key=QUOTA) is None


def test_a_higher_chain_handle_replaces_the_lower_and_the_old_id_says_replaced() -> None:
    """MF 5 (store half, N2): the replaced id answers `replaced` with the
    newer id to its owner, `unknown` to another session; one chain holds
    one slot. Base: no replacement path."""
    store = _store()
    sources = [_commit(store) for _ in range(4)]
    old = _commit(store, replaces_key="chain:k", seq=5, pins=sources[:3])
    new = _commit(store, replaces_key="chain:k", seq=6, pins=sources)
    data = _reason(store, old)
    assert data["reason"] == "replaced" and data["replaced_by"] == new
    with pytest.raises(ResultReferenceInvalid) as caught:
        store.lookup(old, reader_key=READER, quota_key=QUOTA)
    assert "holds these answers and 1 more" in str(caught.value)
    assert _reason(store, old, quota=f"{READER}\x00session-b")["reason"] == "unknown"
    assert _reason(store, old, quota="reader-b", reader="reader-b")["reason"] == "unknown"
    assert store.quota_count(QUOTA) == 5


def test_a_lower_chain_handle_committing_late_is_committed_as_already_replaced() -> None:
    """MF 5b (store half, N1): whichever commits last, the higher number is live."""
    store = _store()
    lower = _mint(store, replaces_key="chain:k", seq=7)
    higher = _mint(store, replaces_key="chain:k", seq=8)
    store.commit(higher)
    store.commit(lower)
    assert store.lookup(higher.id, reader_key=READER, quota_key=QUOTA).id == higher.id
    data = _reason(store, lower.id)
    assert data["reason"] == "replaced" and data["replaced_by"] == higher.id
    assert store.quota_count(QUOTA) == 1 and store.pending_count() == 0


def test_caps_are_64_per_session_128_per_principal_1024_per_server() -> None:
    """MF 12 (M6, Jason board d275): four sessions each hold 40 handles and
    none is evicted; one session's 65th evicts its own oldest. Base:
    16/32/64, so the 17th evicts."""
    assert (
        live_result_sets.MAX_PER_READER, live_result_sets.MAX_PER_PRINCIPAL,
        live_result_sets.MAX_PER_SERVER,
    ) == (64, 128, 1024)
    assert live_result_sets.MAX_PER_QUOTA_BYTES == 32 * 1024 * 1024
    assert live_result_sets.MAX_TOTAL_BYTES == 128 * 1024 * 1024
    store = _store()
    held = {s: [_commit(store, f"{READER}\x00s{s}") for _ in range(40)] for s in range(4)}
    for s, ids in held.items():
        for set_id in ids:
            assert store.lookup(set_id, reader_key=READER, quota_key=f"{READER}\x00s{s}").id == set_id
    first = [_commit(store) for _ in range(64)]
    _commit(store)
    assert _reason(store, first[0])["reason"] == "evicted"
    assert store.quota_count(QUOTA) == 64


def test_pinned_sources_survive_their_owners_count_and_byte_eviction() -> None:
    """MF 12b (store half, N3): at the count cap the oldest UNPINNED entry
    goes, and at the byte cap too."""
    store = _store()
    sources = [_commit(store) for _ in range(60)]
    _commit(store, replaces_key="chain:k", seq=1, pins=sources)
    loose = [_commit(store) for _ in range(3)]
    assert store.quota_count(QUOTA) == 64
    _commit(store)
    assert _reason(store, loose[0])["reason"] == "evicted"
    for set_id in sources:
        assert store.held_for(set_id, reader_key=READER, quota_key=QUOTA) == "committed"
    big = _store()
    eight = live_result_sets.MAX_SET_BYTES - 1024
    pinned = [_commit(big, nbytes=eight) for _ in range(2)]
    free = [_commit(big, nbytes=eight) for _ in range(2)]
    _commit(big, replaces_key="chain:b", seq=1, pins=pinned)
    _commit(big, nbytes=eight)
    assert _reason(big, free[0])["reason"] == "evicted"
    assert all(big.held_for(i, reader_key=READER, quota_key=QUOTA) for i in pinned)


def test_a_pin_never_causes_a_refusal() -> None:
    """MF 12c (store half, P1): with only pinned entries left, the next
    draft evicts the oldest pinned one instead of refusing `capacity`; the
    same at the byte cap and at the per-principal cap. Base: no pins (the
    count case passes there by evicting; the principal case is pinned)."""
    store = _store()
    sources = [_commit(store) for _ in range(63)]
    _commit(store, replaces_key="chain:k", seq=1, pins=sources)
    late = _commit(store)
    assert _reason(store, sources[0])["reason"] == "evicted"
    assert store.held_for(late, reader_key=READER, quota_key=QUOTA) == "committed"
    big = _store()
    eight = live_result_sets.MAX_SET_BYTES - 1024
    pinned = [_commit(big, nbytes=eight) for _ in range(4)]
    _commit(big, replaces_key="chain:b", seq=1, pins=pinned)
    _commit(big, nbytes=eight)
    assert _reason(big, pinned[0])["reason"] == "evicted"
    from okg.substrate.mcp import continuation

    claim = {"provider_id": "sso", "issuer": "https://idp/", "external_subject": "bob"}
    users = _store()
    keys = [continuation.reader_key({"note": str(i)}, claim, True) for i in range(128)]
    ids = [
        users.commit(users.mint(
            reader_key=key, binding={}, members=(), header_claim=claim, multi_user=True,
            payload=b"[]", payload_bytes=2, payload_rows=0, quota_key=key,
        )).id
        for key in keys
    ]
    chain = users.mint(
        reader_key=keys[0], binding={}, members=(), header_claim=claim, multi_user=True,
        payload=b"[]", payload_bytes=2, payload_rows=0, quota_key=keys[0],
        replaces_key="chain:p", seq=1, pins=ids,
    )
    users.commit(chain)
    assert _reason(users, ids[0], quota=keys[0], reader=keys[0])["reason"] == "evicted"


def test_a_source_pending_at_chain_mint_is_protected_once_it_commits() -> None:
    """MF 12c (pins by id at eviction time)."""
    store = _store()
    early = [_commit(store) for _ in range(2)]
    pending = _mint(store)
    _commit(store, replaces_key="chain:k", seq=1, pins=[*early, pending.id])
    store.commit(pending)
    loose = [_commit(store) for _ in range(60)]
    assert store.quota_count(QUOTA) == 64
    _commit(store)
    assert _reason(store, loose[0])["reason"] == "evicted"
    assert store.held_for(pending.id, reader_key=READER, quota_key=QUOTA) == "committed"


def test_the_replaced_reason_is_documented_and_the_new_code_is_registered() -> None:
    import pathlib

    import yaml

    root = pathlib.Path(__file__).resolve().parents[3]
    registry = yaml.safe_load((root / "docs/reference/error_codes.yaml").read_text())
    entry = next(c for c in registry["codes"] if c["code"] == "live_handles_not_combinable")
    assert entry["python_exception"] == "LiveHandlesNotCombinable"
    assert set(entry["data_shape"]) == {"reason", "index", "handles"}
    assert _lists().LiveHandlesNotCombinable.code == "live_handles_not_combinable"
    invalid = next(c for c in registry["codes"] if c["code"] == "result_reference_invalid")
    assert "replaced_by" in invalid["data_shape"]


# ============================================================ 14: large whole floats


def test_a_whole_float_of_2_to_the_53_or_more_is_flagged_not_exact() -> None:
    """MF 14 (L7): a whole-number float at or past 2^53 is added as an int
    but flagged; ints of any size are not. Base: not flagged."""
    rows = [{"size": float(2 ** 53)}, {"size": 3}]
    report = live_result_sets.group_report(rows, [], [("sum", "size")])
    assert report.groups[0][1]["sum(attrs.size)"] == 2 ** 53 + 3
    assert live_result_sets.not_exact_labels(report) == ["sum(attrs.size)"]
    assert report.large_floats == ["sum(attrs.size)"] and report.inexact == []
    exact = live_result_sets.group_report([{"size": 2 ** 60}, {"size": 1}], [], [("sum", "size")])
    assert live_result_sets.not_exact_labels(exact) == []


def test_a_cut_source_is_said_in_the_combined_cover_line() -> None:
    """MF (live-kept-answer-cut merged in): the external system's cut of
    any source is counted over the chain, in upstream calls."""
    cut = _set("b", [{"n": 2}], upstream_cut={"rows": 1, "calls_cut": 1, "calls": 1})
    combined = _lists().combine(
        _chain(["a", "b", "c"]), "l1.chain",
        _opened([_set("a", [{"n": 1}]), cut, _set("c", [{"n": 3}])]), [],
    )
    assert combined.block["upstream_calls_cut"] == 1
    assert combined.retained.upstream_cut == {"rows": 3, "calls_cut": 1, "calls": 3}
    cover = live_result_sets.totals_cover(
        combined.retained, 3, metrics=[("count", None), ("sum", "n")],
    )
    assert "the external system cut 1 of 3 calls" in cover
    # The cut PACT's wording and JSON: a count is at least, a sum over the
    # rows received only; the reply carries `upstream_cut`.
    assert "count is at least the value shown" in cover
    assert "sum(attrs.n) is over the rows received only" in cover
    assert live_result_sets.totals_short(combined.retained) == {
        "upstream_cut": {"rows": 3, "calls_cut": 1, "calls": 3},
    }
    whole = _lists().combine(_chain(["a", "c"]), "l1.chain",
                             _opened([_set("a", [{"n": 1}]), _set("c", [{"n": 3}])]), [])
    assert whole.retained.upstream_cut is None and whole.block["upstream_calls_cut"] == 0


def test_a_higher_chain_number_always_holds_the_lower_chains_answers() -> None:
    """MF review M1: two calls in flight together, the one that drew its
    number first reaching the memory lock second. The higher number must
    still be the chain that holds both answers (base: the number was drawn
    before the lock, so the higher-numbered chain lacked the other answer)."""
    import threading

    hint = _hint()
    session = _session()
    _note(session, "a")
    _note(session, "b")

    class Gate:
        def __init__(self) -> None:
            self.lock = threading.Lock()
            self.second_done = threading.Event()
            self.first_waiting = threading.Event()

        def __enter__(self) -> None:
            if threading.current_thread().name == "first":
                self.first_waiting.set()
                assert self.second_done.wait(10), "the second call never ran"
            self.lock.acquire()

        def __exit__(self, *exc: Any) -> None:
            self.lock.release()
            if threading.current_thread().name == "second":
                self.second_done.set()

    gate = Gate()
    real = hint._LOCK
    hint._LOCK = gate
    found: dict[str, Any] = {}
    try:
        first = threading.Thread(
            name="first", target=lambda: found.__setitem__("first", _note(session, "c")[1]),
        )
        second = threading.Thread(
            name="second", target=lambda: found.__setitem__("second", _note(session, "d")[1]),
        )
        first.start()
        assert gate.first_waiting.wait(10)
        second.start()
        second.join(10)
        first.join(10)
    finally:
        hint._LOCK = real
    low, high = sorted(found.values(), key=lambda chain: chain.seq)
    assert set(low.values) <= set(high.values), (low.values, high.values)
    assert set(high.values) == {"a", "b", "c", "d"}


def test_answers_the_chain_never_held_are_counted_and_named_in_the_read() -> None:
    """MF review M4 and L1: a value gone before the chain was minted, and a
    kept answer older than its 60, are in `not_kept` and in `of`."""
    lists = _lists()
    chain = lists.LiveChainSet(
        chain_key="chain:k", seq=1, source_ids=("l1.s0", "l1.s1"), relation="lrs_rows",
        node_id="n:hub", varying="q", values=("a", "b"), shared={"size": 10},
        missing=("gone",), dropped=("old",),
    )
    combined = lists.combine(chain, "l1.chain", _opened([_set("a", [{"n": 1}]), _set("b", [{"n": 2}])]), [])
    assert combined.block["answers"] == 2 and combined.block["of"] == 4
    reasons = {item["value"]: item["reason"] for item in combined.block["not_kept"]}
    assert reasons == {"gone": "gone_before_chain", "old": "not_in_chain"}
    from okg.substrate.mcp import result_text

    lines = result_text._combined_lines({
        "combined": combined.block, "original_call": {"relation": "lrs_rows"},
    })
    assert lines[0] == (
        "! over 2 of 4 answers; 1 no longer kept: q=gone; "
        "1 older answers not in this handle: q=old"
    )
