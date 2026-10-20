"""The result-reference store and its final-fit commit (#3222 slice 1).

PACT ``read-result-references-3222-store``. Pure unit tests: no database.
The store keeps a short-lived, reader-bound list of result members under a
compact signed id. A producer drafts a reference inside one read call; the
byte-budget wrapper commits the draft only when the final fitted response
that carries its id is returned, and releases it otherwise.
"""

from __future__ import annotations

import json
import threading
import time
from typing import Any

import pytest

from okg.substrate.mcp import continuation, result_references
from okg.substrate.mcp.response_bounds import bound_read_tool
from okg.substrate.mcp.result_references import (
    LIFETIME_SECONDS,
    MAX_MEMBERS,
    MAX_PER_READER,
    MAX_PER_SERVER,
    MAX_REFERENCE_BYTES,
    MAX_TOTAL_BYTES,
    ResultReferenceCapacity,
    ResultReferenceInternal,
    ResultReferenceInvalid,
    ResultReferenceStore,
    canonical_bytes,
)

BINDING: dict[str, Any] = {"query": "q", "generation": "gen:7"}
#: A single-user server's draft: no claim, no principal cap. Producers must
#: name both, so none can skip the principal cap by omission.
SINGLE_USER: dict[str, Any] = {"header_claim": None, "multi_user": False}


class FrozenClock:
    """A wall clock that moves only when a test moves it."""

    def __init__(self, now: float = 1_800_000_000.0) -> None:
        self.now = now

    def __call__(self) -> float:
        return self.now


@pytest.fixture
def clock() -> FrozenClock:
    return FrozenClock()


@pytest.fixture
def store(clock: FrozenClock):
    """A fresh store installed as the process store for this test."""
    fresh = ResultReferenceStore(clock=clock)
    previous = result_references.install_store(fresh)
    try:
        yield fresh
    finally:
        result_references.install_store(previous)


def _members(count: int, *, prefix: str = "n") -> list[dict[str, Any]]:
    return [
        {"node_id": f"{prefix}:{i}", "rank": i + 1, "location": None}
        for i in range(count)
    ]


def _members_of_bytes(target: int, binding: dict[str, Any] = BINDING) -> list[dict[str, Any]]:
    """One member padded so the canonical bytes are exactly ``target``."""
    members = _members(1)
    pad = target - canonical_bytes(binding, members)
    assert pad >= 0
    members[0]["node_id"] += "x" * pad
    assert canonical_bytes(binding, members) == target
    return members


def _commit(store: ResultReferenceStore, reader: str, members: list[dict[str, Any]] | None = None, *, parent: str | None = None):
    draft = store.mint(
        **SINGLE_USER,
        reader_key=reader,
        binding=BINDING,
        members=_members(1) if members is None else members,
        parent=parent,
    )
    return store.commit(draft)


def _reason(excinfo: pytest.ExceptionInfo[BaseException]) -> str:
    return excinfo.value.data["reason"]  # type: ignore[attr-defined]


def _payload(exc: BaseException) -> str:
    return json.dumps(exc.to_json(), sort_keys=True)  # type: ignore[attr-defined]


# --- the id ----------------------------------------------------------------


def test_id_is_compact_signed_and_round_trips(store: ResultReferenceStore) -> None:
    ref = _commit(store, "reader-a")
    parts = ref.id.split(".")
    assert parts[0] == "r1" and len(parts) == 5
    assert parts[1] == store.epoch
    assert int(parts[2]) == int(ref.expires_at)
    assert 56 <= len(ref.id) <= 72
    found = store.lookup(ref.id, reader_key="reader-a")
    assert found.id == ref.id
    assert found.members == ({"node_id": "n:0", "rank": 1, "location": None},)
    assert found.binding == BINDING


def test_reference_mac_key_differs_from_continuation_key() -> None:
    assert result_references._mac_key() != continuation._signing_key()


@pytest.mark.parametrize(
    "bad",
    [None, 7, "", "r1", "r2.aaaaaaaa.1.aaaaaaaaaaaaaaaa.aaaaaaaaaaaaaaaaaaaaaa", "r1.a.b.c.d", "x" * 5000],
)
def test_malformed_id_refuses_malformed(store: ResultReferenceStore, bad: Any) -> None:
    with pytest.raises(ResultReferenceInvalid) as excinfo:
        store.lookup(bad, reader_key="reader-a")
    assert _reason(excinfo) == "malformed"
    assert excinfo.value.code == "result_reference_invalid"


def test_new_store_epoch_refuses_store_lost(clock: FrozenClock) -> None:
    old = ResultReferenceStore(clock=clock)
    ref = _commit(old, "reader-a")
    restarted = ResultReferenceStore(clock=clock)
    assert restarted.epoch != old.epoch
    with pytest.raises(ResultReferenceInvalid) as excinfo:
        restarted.lookup(ref.id, reader_key="reader-a")
    assert _reason(excinfo) == "store_lost"


def test_forged_mac_refuses_signature_invalid(store: ResultReferenceStore) -> None:
    ref = _commit(store, "reader-a")
    prefix, mac = ref.id.rsplit(".", 1)
    forged = f"{prefix}.{('A' if mac[0] != 'A' else 'B')}{mac[1:]}"
    with pytest.raises(ResultReferenceInvalid) as excinfo:
        store.lookup(forged, reader_key="reader-a")
    assert _reason(excinfo) == "signature_invalid"
    # A changed expiry under the old MAC is a forgery too, never an extension.
    parts = ref.id.split(".")
    parts[2] = str(int(parts[2]) + 3600)
    with pytest.raises(ResultReferenceInvalid) as excinfo:
        store.lookup(".".join(parts), reader_key="reader-a")
    assert _reason(excinfo) == "signature_invalid"


def test_frozen_clock_expiry(store: ResultReferenceStore, clock: FrozenClock) -> None:
    ref = _commit(store, "reader-a")
    assert ref.expires_at == clock.now + LIFETIME_SECONDS
    clock.now += LIFETIME_SECONDS - 1
    assert store.lookup(ref.id, reader_key="reader-a").id == ref.id
    clock.now += 1
    with pytest.raises(ResultReferenceInvalid) as excinfo:
        store.lookup(ref.id, reader_key="reader-a")
    assert _reason(excinfo) == "expired"
    # Expired references stop counting at the next commit.
    _commit(store, "reader-b")
    assert store.count() == 1


def test_other_readers_id_answers_exactly_like_an_unknown_id(store: ResultReferenceStore) -> None:
    owned = _commit(store, "reader-a")
    released = store.mint(**SINGLE_USER, reader_key="reader-a", binding=BINDING, members=_members(3))
    with pytest.raises(ResultReferenceInvalid) as other:
        store.lookup(owned.id, reader_key="reader-b")
    with pytest.raises(ResultReferenceInvalid) as never_issued:
        store.lookup(released.id, reader_key="reader-b")
    with pytest.raises(ResultReferenceInvalid) as own_never_issued:
        store.lookup(released.id, reader_key="reader-a")
    assert _reason(other) == "unknown"
    assert _payload(other.value) == _payload(never_issued.value)
    assert _payload(other.value) == _payload(own_never_issued.value)
    assert owned.id not in _payload(other.value)


# --- per-reference limits ----------------------------------------------------


@pytest.mark.parametrize("count", [MAX_MEMBERS - 1, MAX_MEMBERS])
def test_member_cap_accepts_up_to_the_limit(store: ResultReferenceStore, count: int) -> None:
    ref = _commit(store, "reader-a", _members(count))
    assert len(ref.members) == count


def test_member_cap_refuses_one_over_at_draft(store: ResultReferenceStore) -> None:
    with pytest.raises(ResultReferenceCapacity) as excinfo:
        store.mint(**SINGLE_USER, reader_key="reader-a", binding=BINDING, members=_members(MAX_MEMBERS + 1))
    assert excinfo.value.code == "result_reference_capacity"
    assert _reason(excinfo) == "reference_members"
    assert excinfo.value.data["members"] == MAX_MEMBERS + 1
    assert excinfo.value.data["limits"] == result_references.limits()
    assert store.count() == 0


def test_byte_cap_at_256_kib(store: ResultReferenceStore) -> None:
    exact = _commit(store, "reader-a", _members_of_bytes(MAX_REFERENCE_BYTES))
    assert exact.canonical_bytes == MAX_REFERENCE_BYTES == 256 * 1024
    with pytest.raises(ResultReferenceCapacity) as excinfo:
        store.mint(**SINGLE_USER, reader_key="reader-a", binding=BINDING, members=_members_of_bytes(MAX_REFERENCE_BYTES + 1))
    assert _reason(excinfo) == "reference_bytes"
    assert excinfo.value.data["bytes"] == MAX_REFERENCE_BYTES + 1
    assert store.count() == 1


def test_unicode_node_ids_count_as_utf8_bytes(store: ResultReferenceStore) -> None:
    members = _members(1)
    base = canonical_bytes(BINDING, members)
    small = [{"node_id": "n:\u00e9\u4e2d", "rank": 1, "location": {"chunk": "\u00e9"}}]
    ref = _commit(store, "reader-a", small)
    expected = len(json.dumps(
        {"binding": BINDING, "members": small},
        ensure_ascii=False, sort_keys=True, separators=(",", ":"),
    ).encode("utf-8"))
    assert ref.canonical_bytes == expected
    assert store.total_bytes() == expected
    # Fits by code points, not by UTF-8 bytes: refused.
    chars = MAX_REFERENCE_BYTES - base
    wide = _members(1)
    wide[0]["node_id"] += "\u00e9" * chars
    text = json.dumps(
        {"binding": BINDING, "members": wide},
        ensure_ascii=False, sort_keys=True, separators=(",", ":"),
    )
    assert len(text) == MAX_REFERENCE_BYTES
    with pytest.raises(ResultReferenceCapacity) as excinfo:
        store.mint(**SINGLE_USER, reader_key="reader-a", binding=BINDING, members=wide)
    assert _reason(excinfo) == "reference_bytes"
    assert excinfo.value.data["bytes"] == base + 2 * chars


def test_member_shape_is_checked(store: ResultReferenceStore) -> None:
    with pytest.raises(ResultReferenceInternal):
        store.mint(**SINGLE_USER, reader_key="reader-a", binding=BINDING, members=[{"node_id": "", "rank": 1}])
    with pytest.raises(ResultReferenceInternal):
        store.mint(**SINGLE_USER, reader_key="reader-a", binding=BINDING, members=[{"node_id": "n:1", "rank": True}])
    with pytest.raises(ResultReferenceInternal):
        store.mint(**SINGLE_USER, reader_key="reader-a", binding=BINDING, members=[{"node_id": "n:1", "rank": 1, "text": "body"}])


# --- per-reader cap and own-oldest eviction ---------------------------------


def test_reader_cap_127_128_then_129_evicts_own_oldest(store: ResultReferenceStore) -> None:
    other = _commit(store, "reader-b")
    refs = [_commit(store, "reader-a") for _ in range(MAX_PER_READER - 1)]
    assert store.usage("reader-a")["references"] == MAX_PER_READER - 1
    refs.append(_commit(store, "reader-a"))
    assert store.usage("reader-a")["references"] == MAX_PER_READER
    store.lookup(refs[0].id, reader_key="reader-a")

    newest = _commit(store, "reader-a")
    assert store.usage("reader-a")["references"] == MAX_PER_READER
    assert store.count() == MAX_PER_READER + 1
    with pytest.raises(ResultReferenceInvalid) as excinfo:
        store.lookup(refs[0].id, reader_key="reader-a")
    assert _reason(excinfo) == "evicted"
    assert store.lookup(refs[1].id, reader_key="reader-a").id == refs[1].id
    assert store.lookup(newest.id, reader_key="reader-a").id == newest.id
    # Another reader's reference is untouched, and another reader asking for
    # the evicted id learns nothing more than for an unknown id.
    assert store.lookup(other.id, reader_key="reader-b").id == other.id
    with pytest.raises(ResultReferenceInvalid) as stranger:
        store.lookup(refs[0].id, reader_key="reader-b")
    with pytest.raises(ResultReferenceInvalid) as unknown:
        store.lookup(other.id, reader_key="reader-a")
    assert _reason(stranger) == "unknown"
    assert _payload(stranger.value) == _payload(unknown.value)


def test_evicted_record_is_dropped_at_expiry(store: ResultReferenceStore, clock: FrozenClock) -> None:
    first = _commit(store, "reader-a")
    for _ in range(MAX_PER_READER):
        _commit(store, "reader-a")
    with pytest.raises(ResultReferenceInvalid) as excinfo:
        store.lookup(first.id, reader_key="reader-a")
    assert _reason(excinfo) == "evicted"
    assert store.evicted_records() == 1
    clock.now += LIFETIME_SECONDS
    _commit(store, "reader-b")
    assert store.evicted_records() == 0


def test_evicted_record_is_bounded(store: ResultReferenceStore) -> None:
    for _ in range(MAX_PER_READER + result_references.MAX_EVICTED_RECORDS + 5):
        _commit(store, "reader-a")
    assert store.evicted_records() == result_references.MAX_EVICTED_RECORDS


# --- server and total-byte caps ----------------------------------------------


def _fill(store: ResultReferenceStore, total: int) -> None:
    """Commit ``total`` one-member references, 128 per reader."""
    reader = 0
    for i in range(total):
        if i and i % MAX_PER_READER == 0:
            reader += 1
        _commit(store, f"filler-{reader}")


def test_server_cap_1023_1024_1025_never_evicts_another_reader(store: ResultReferenceStore) -> None:
    _fill(store, MAX_PER_SERVER - 1)
    assert store.count() == MAX_PER_SERVER - 1
    last_reader = f"filler-{(MAX_PER_SERVER - 1) // MAX_PER_READER}"
    assert store.usage(last_reader)["references"] == MAX_PER_READER - 1
    _commit(store, last_reader)
    assert store.count() == MAX_PER_SERVER

    with pytest.raises(ResultReferenceCapacity) as excinfo:
        _commit(store, "newcomer")
    assert _reason(excinfo) == "server_references"
    assert set(excinfo.value.data) == {"reason", "reader_references", "reader_bytes", "limits"}
    assert excinfo.value.data["reader_references"] == 0
    assert store.count() == MAX_PER_SERVER
    for reader in range(MAX_PER_SERVER // MAX_PER_READER):
        assert store.usage(f"filler-{reader}")["references"] == MAX_PER_READER

    # A reader at its own cap still commits: it evicts its own oldest.
    _commit(store, last_reader)
    assert store.count() == MAX_PER_SERVER


def test_total_byte_cap_32_mib(store: ResultReferenceStore) -> None:
    assert MAX_TOTAL_BYTES == 32 * 1024 * 1024 == MAX_PER_READER * MAX_REFERENCE_BYTES
    full = _members_of_bytes(MAX_REFERENCE_BYTES)
    for _ in range(MAX_PER_READER - 1):
        _commit(store, "big", full)
    small = _commit(store, "small", _members(1))
    room = MAX_TOTAL_BYTES - store.total_bytes()
    assert room == MAX_REFERENCE_BYTES - small.canonical_bytes

    with pytest.raises(ResultReferenceCapacity) as excinfo:
        _commit(store, "third", _members_of_bytes(room + 1))
    assert _reason(excinfo) == "total_bytes"
    assert excinfo.value.data["reader_references"] == 0
    assert excinfo.value.data["reader_bytes"] == 0
    assert store.usage("big")["references"] == MAX_PER_READER - 1

    _commit(store, "third", _members_of_bytes(room))
    assert store.total_bytes() == MAX_TOTAL_BYTES
    with pytest.raises(ResultReferenceCapacity):
        _commit(store, "small", _members(1))
    # Below its own cap, even the largest reader is refused, not evicted.
    with pytest.raises(ResultReferenceCapacity) as excinfo:
        _commit(store, "big", full)
    assert excinfo.value.data["reader_references"] == MAX_PER_READER - 1
    assert store.usage("big")["references"] == MAX_PER_READER - 1
    assert store.total_bytes() == MAX_TOTAL_BYTES


def test_thirty_two_threads_racing_for_the_last_slot_one_wins(
    store: ResultReferenceStore, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _fill(store, MAX_PER_SERVER - 1)
    drafts = [
        store.mint(**SINGLE_USER, reader_key=f"racer-{i}", binding=BINDING, members=_members(1))
        for i in range(32)
    ]
    barrier = threading.Barrier(32)
    outcomes: list[str] = []
    lock = threading.Lock()
    # Widen the window between the capacity check and the insert: without
    # the store lock several racers pass the check before any inserts.
    monkeypatch.setattr(store, "_after_capacity_check", lambda: time.sleep(0.02))

    def race(draft: Any) -> None:
        barrier.wait()
        try:
            store.commit(draft)
            outcome = "won"
        except ResultReferenceCapacity:
            outcome = "refused"
        with lock:
            outcomes.append(outcome)

    threads = [threading.Thread(target=race, args=(d,)) for d in drafts]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert sorted(outcomes).count("won") == 1
    assert outcomes.count("refused") == 31
    assert store.count() == MAX_PER_SERVER


# --- children ----------------------------------------------------------------


def test_child_expires_with_its_parent(store: ResultReferenceStore, clock: FrozenClock) -> None:
    parent = _commit(store, "reader-a", _members(5))
    clock.now += 300
    child = _commit(store, "reader-a", _members(2), parent=parent.id)
    assert child.parent == parent.id
    assert child.expires_at == parent.expires_at
    clock.now = parent.expires_at
    for ref in (parent, child):
        with pytest.raises(ResultReferenceInvalid) as excinfo:
            store.lookup(ref.id, reader_key="reader-a")
        assert _reason(excinfo) == "expired"


def test_child_of_evicted_parent_is_evicted_too(store: ResultReferenceStore) -> None:
    parent = _commit(store, "reader-a")
    child = _commit(store, "reader-a", parent=parent.id)
    pending = store.mint(**SINGLE_USER, reader_key="reader-a", binding=BINDING, members=_members(1), parent=parent.id)
    for _ in range(MAX_PER_READER - 2):
        _commit(store, "reader-a")
    _commit(store, "reader-a")
    assert store.usage("reader-a")["references"] == MAX_PER_READER - 1
    for ref_id in (parent.id, child.id):
        with pytest.raises(ResultReferenceInvalid) as excinfo:
            store.lookup(ref_id, reader_key="reader-a")
        assert _reason(excinfo) == "evicted"
    with pytest.raises(ResultReferenceInvalid) as excinfo:
        store.commit(pending)
    assert _reason(excinfo) == "evicted"


def test_child_of_oldest_at_reader_cap_evicts_the_second_oldest(store: ResultReferenceStore) -> None:
    oldest = _commit(store, "reader-a")
    second = _commit(store, "reader-a")
    for _ in range(MAX_PER_READER - 2):
        _commit(store, "reader-a")
    assert store.usage("reader-a")["references"] == MAX_PER_READER
    child = _commit(store, "reader-a", parent=oldest.id)
    # The oldest is the new child's parent, so it stays; the next oldest goes.
    assert store.lookup(oldest.id, reader_key="reader-a").id == oldest.id
    assert store.lookup(child.id, reader_key="reader-a").parent == oldest.id
    with pytest.raises(ResultReferenceInvalid) as excinfo:
        store.lookup(second.id, reader_key="reader-a")
    assert _reason(excinfo) == "evicted"
    assert store.usage("reader-a")["references"] == MAX_PER_READER


def test_child_needs_its_readers_live_parent(store: ResultReferenceStore) -> None:
    parent = _commit(store, "reader-a")
    with pytest.raises(ResultReferenceInvalid) as excinfo:
        store.mint(**SINGLE_USER, reader_key="reader-b", binding=BINDING, members=_members(1), parent=parent.id)
    assert _reason(excinfo) == "unknown"


# --- reader key ----------------------------------------------------------------


def test_reader_key_ignores_request_stamps_and_follows_the_claim() -> None:
    claim = {"sub": "alice", "iss": "idp", "verified_at": "2026-10-04T10:00:00Z", "auth_time": 1}
    later = {**claim, "verified_at": "2026-10-04T10:05:00Z", "auth_time": 2}
    base = continuation.reader_key({"classification": "internal"}, claim, True)
    assert continuation.reader_key({"classification": "internal"}, later, True) == base
    assert continuation.reader_key({"classification": "internal"}, {**claim, "sub": "bob"}, True) != base
    assert continuation.reader_key({"classification": "internal"}, claim, False) != base
    assert continuation.reader_key({"classification": "secret"}, claim, True) != base
    assert len(base) == 64


def test_reader_key_treats_no_authority_as_empty_and_drops_server_owned_keys() -> None:
    nobody = continuation.reader_key(None, None, False)
    assert continuation.reader_key({}, None, False) == nobody
    assert continuation.reader_key({"_nomos_decision": {"allow": True}}, None, False) == nobody
    planted = {"classification": "internal", "_nomos_identity": {"sub": "root"}}
    assert continuation.reader_key(planted, None, False) == continuation.reader_key(
        {"classification": "internal"}, None, False,
    )
    # The rest of the authority is hashed whole: a different classification
    # is a different reader, never merged.
    assert continuation.reader_key({"classification": "internal"}, None, False) != nobody


# --- final-fit commit through bound_read_tool --------------------------------


def _producer(
    drafted: list[str],
    *,
    total: int = 40,
    row_chars: int = 600,
    refuse_below: int | None = None,
    place_id: bool = True,
    rerun_padding: int = 0,
):
    """A search-like read that drafts a reference of its page on every run."""

    def search(
        query: str,
        limit: int = 20,
        offset: int = 0,
        generation_id: str | None = None,
        max_bytes: int | None = None,
    ) -> dict[str, Any]:
        pin = generation_id or "gen:7"
        rows = [
            {"node_id": f"doc:{i:03d}", "excerpt": "\u00e9" * row_chars}
            for i in range(offset, min(offset + limit, total))
        ]
        ref_id = result_references.draft(
            **SINGLE_USER,
            reader_key="reader-a",
            binding={"query": query, "generation": pin},
            members=[
                {"node_id": row["node_id"], "rank": rank, "location": None}
                for rank, row in enumerate(rows, start=offset + 1)
            ],
        )
        drafted.append(ref_id)
        if refuse_below is not None and limit < refuse_below:
            return {
                "status": "refused",
                "operator_name": "search",
                "error": {"code": "search_time_bound", "message": "too slow"},
            }
        truncated = offset + limit < total
        page: dict[str, Any] = {
            "query": query,
            "results": rows,
            "limit": limit,
            "offset": offset,
            "next_offset": offset + limit if truncated else None,
            "truncated": truncated,
            "pinned_generation_id": pin,
        }
        if rerun_padding and limit < 20:
            # Reruns weigh more than the first page's rows: the prediction
            # misses and the wrapper searches limits by rerun. A warning
            # notice: a search reply in text prints it, and no longer
            # prints unplaced page fields such as the old `notes` (#3666).
            page["warning"] = {"code": "padding", "message": "p" * rerun_padding}
        if place_id:
            page["result_reference"] = {"id": ref_id}
        return page

    return search


def test_call_that_fits_commits_its_draft(store: ResultReferenceStore) -> None:
    drafted: list[str] = []
    out = bound_read_tool("search", _producer(drafted, total=3, row_chars=10))(
        query="q", limit=20, max_bytes=65_536,
    )
    assert len(drafted) == 1
    assert out["result_reference"]["id"] == drafted[0]
    assert store.count() == 1
    assert len(store.lookup(drafted[0], reader_key="reader-a").members) == 3


def test_fit_with_several_reruns_commits_only_the_served_draft(store: ResultReferenceStore) -> None:
    drafted: list[str] = []
    out = bound_read_tool("search", _producer(drafted, rerun_padding=2_000))(
        query="q", limit=20, max_bytes=8_192,
    )
    assert out["truncated"] is True
    assert len(drafted) >= 3
    assert store.count() == 1
    served = out["result_reference"]["id"]
    assert served in drafted[1:]
    # The binary search's last rerun did not fit: the served page came from
    # an earlier rerun, and its draft is the one committed.
    assert served != drafted[-1]
    served_ref = store.lookup(served, reader_key="reader-a")
    assert [m["node_id"] for m in served_ref.members] == [r["node_id"] for r in out["results"]]
    for earlier in drafted:
        if earlier == served:
            continue
        with pytest.raises(ResultReferenceInvalid) as excinfo:
            store.lookup(earlier, reader_key="reader-a")
        assert _reason(excinfo) == "unknown"


def test_an_oversize_row_is_cut_and_commits_only_the_served_draft(
    store: ResultReferenceStore,
) -> None:
    """#3666 step 3b: a row too large for any page is served cut, on the
    limit-1 rerun's page; that page's draft is the one committed, and every
    other run's draft is released."""
    drafted: list[str] = []
    out = bound_read_tool("search", _producer(drafted, row_chars=20_000))(
        query="q", limit=20, max_bytes=4_096,
    )
    assert out.get("status") != "refused", out
    (row,) = out["results"]
    assert row["values_cut"]["excerpt"]["chars"] == 20_000
    assert len(drafted) >= 2
    assert store.count() == 1
    assert out["result_reference"]["id"] in drafted[1:]


def test_returned_refusal_on_a_rerun_commits_nothing(store: ResultReferenceStore) -> None:
    drafted: list[str] = []
    out = bound_read_tool("search", _producer(drafted, refuse_below=20))(
        query="q", limit=20, max_bytes=8_192,
    )
    assert out["status"] == "refused"
    assert len(drafted) >= 2
    assert store.count() == 0


def test_returned_refusal_that_fits_commits_nothing(store: ResultReferenceStore) -> None:
    drafted: list[str] = []
    out = bound_read_tool("search", _producer(drafted, refuse_below=100))(
        query="q", limit=20, max_bytes=65_536,
    )
    assert out["status"] == "refused"
    assert len(drafted) == 1
    assert store.count() == 0


def test_draft_whose_id_is_not_served_is_released(store: ResultReferenceStore) -> None:
    drafted: list[str] = []
    out = bound_read_tool("search", _producer(drafted, total=3, row_chars=10, place_id=False))(
        query="q", limit=20, max_bytes=65_536,
    )
    assert "result_reference" not in out
    assert len(drafted) == 1
    assert store.count() == 0


class _ExplodingLive:
    """A prepared ``expand(live=...)`` step that fails after the graph fit."""

    generation_id = "gen:live"

    def reserve_envelope(self, graph: dict[str, Any]) -> dict[str, Any]:
        return {"relation": "status", "rows": [], "reserve": "r" * 400}

    def refuse(self, reason: str) -> None:
        raise AssertionError(f"unexpected refusal {reason}")

    def run(self, graph: dict[str, Any], room: Any) -> dict[str, Any]:
        raise RuntimeError("adapter exploded after fitting")


def test_exception_after_fitting_commits_nothing(store: ResultReferenceStore) -> None:
    drafted: list[str] = []

    def expand(
        input: Any,
        limit: int = 20,
        offset: int = 0,
        generation_id: str | None = None,
        depth: int = 1,
        path: list[str] | None = None,
        live: Any = None,
        max_bytes: int | None = None,
    ) -> dict[str, Any]:
        neighbors = [
            {"edge_id": f"edge:{i}", "attrs": {"text": "\u00e9" * 600}}
            for i in range(offset, offset + limit)
        ]
        ref_id = result_references.draft(
            **SINGLE_USER,
            reader_key="reader-a",
            binding={"start": "n:start"},
            members=[
                {"node_id": n["edge_id"], "rank": rank, "location": None}
                for rank, n in enumerate(neighbors, start=1)
            ],
        )
        drafted.append(ref_id)
        return {
            "neighbors": neighbors,
            "limit": limit,
            "offset": offset,
            "next_offset": offset + limit,
            "truncated": True,
            "pinned_generation_id": generation_id,
            "result_reference": {"id": ref_id},
        }

    with pytest.raises(RuntimeError, match="adapter exploded"):
        bound_read_tool("expand", expand, live_step=lambda _args: _ExplodingLive())(
            input={"node_id": "n:start"},
            limit=20,
            live={"relation": "status", "args": {}},
            max_bytes=8_192,
        )
    assert len(drafted) >= 2
    assert store.count() == 0


def test_second_draft_in_one_attempt_is_an_internal_refusal(store: ResultReferenceStore) -> None:
    def twice(query: str, limit: int = 20, max_bytes: int | None = None) -> dict[str, Any]:
        result_references.draft(**SINGLE_USER, reader_key="reader-a", binding=BINDING, members=_members(1))
        result_references.draft(**SINGLE_USER, reader_key="reader-a", binding=BINDING, members=_members(1))
        return {"results": []}

    with pytest.raises(ResultReferenceInternal) as excinfo:
        bound_read_tool("search", twice)(query="q", max_bytes=65_536)
    assert excinfo.value.code == "result_reference_internal"
    assert store.count() == 0


def test_draft_outside_a_read_call_is_an_internal_refusal(store: ResultReferenceStore) -> None:
    with pytest.raises(ResultReferenceInternal):
        result_references.draft(**SINGLE_USER, reader_key="reader-a", binding=BINDING, members=_members(1))


def test_capacity_refusal_at_commit_is_raised_from_the_call(store: ResultReferenceStore) -> None:
    _fill(store, MAX_PER_SERVER)
    drafted: list[str] = []
    with pytest.raises(ResultReferenceCapacity):
        bound_read_tool("search", _producer(drafted, total=3, row_chars=10))(
            query="q", limit=20, max_bytes=65_536,
        )
    assert store.count() == MAX_PER_SERVER


# --- commit at the served point, through the real FastMCP call path ---------


def _serve(
    producer: Any,
    arguments: dict[str, Any],
    result_format: str,
) -> Any:
    """One in-process MCP call to ``search`` registered over ``producer``."""
    import anyio

    from okg.substrate.mcp.response_bounds import ByteBoundedFastMCP
    from okg.substrate.mcp.result_text import result_format_scope

    server = ByteBoundedFastMCP("result-reference-serve-test")
    server.tool(name="search", structured_output=False)(bound_read_tool("search", producer))

    async def drive() -> Any:
        with result_format_scope(result_format):
            return await server.call_tool("search", arguments)

    return anyio.run(drive)


def _served_text(result: Any) -> str:
    assert result.structuredContent is None
    assert len(result.content) == 1
    return result.content[0].text


def _serve_producer(drafted: list[str], **overrides: Any):
    """A fitting search page that drafts one reference; ``overrides`` replace
    page fields after the draft (an id elsewhere, an invalid envelope)."""

    def search(
        query: str,
        limit: int = 20,
        offset: int = 0,
        generation_id: str | None = None,
        max_bytes: int | None = None,
    ) -> dict[str, Any]:
        rows = [{"node_id": f"doc:{i}"} for i in range(3)]
        ref_id = result_references.draft(
            **SINGLE_USER,
            reader_key="reader-a",
            binding={"query": query},
            members=[{"node_id": r["node_id"], "rank": i + 1} for i, r in enumerate(rows)],
        )
        drafted.append(ref_id)
        page: dict[str, Any] = {
            "query": query,
            "results": rows,
            "limit": limit,
            "offset": offset,
            "next_offset": None,
            "truncated": False,
            "result_reference": {"id": ref_id},
        }
        for key, value in overrides.items():
            if value is _DROP:
                page.pop(key, None)
            else:
                page[key] = value(ref_id) if callable(value) else value
        return page

    return search


_DROP = object()


@pytest.mark.parametrize("result_format", ["text", "json"])
def test_served_id_is_committed_at_the_served_point(store: ResultReferenceStore, result_format: str) -> None:
    drafted: list[str] = []
    result = _serve(_serve_producer(drafted), {"query": "q"}, result_format)
    assert result.isError is False
    assert len(drafted) == 1
    assert drafted[0] in _served_text(result)
    assert store.count() == 1
    assert len(store.lookup(drafted[0], reader_key="reader-a").members) == 3


@pytest.mark.parametrize("result_format", ["text", "json"])
def test_validation_error_after_fitting_commits_nothing(store: ResultReferenceStore, result_format: str) -> None:
    drafted: list[str] = []
    # `truncated` must be a boolean: OperatorResult validation fails after
    # the wrapper has fitted and returned the page carrying the id.
    result = _serve(_serve_producer(drafted, truncated="maybe"), {"query": "q"}, result_format)
    assert result.isError is True
    assert "operator_execution_failed" in _served_text(result)
    assert len(drafted) == 1
    assert store.count() == 0


@pytest.mark.parametrize("result_format", ["text", "json"])
def test_served_refusal_commits_nothing(store: ResultReferenceStore, result_format: str) -> None:
    drafted: list[str] = []
    refused = {"code": "search_time_bound", "message": "too slow"}
    result = _serve(
        _serve_producer(drafted, status="refused", error=refused),
        {"query": "q"},
        result_format,
    )
    assert result.isError is True
    assert len(drafted) == 1
    assert store.count() == 0


@pytest.mark.parametrize("result_format", ["text", "json"])
def test_id_missing_from_served_content_commits_nothing(
    store: ResultReferenceStore, result_format: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The wrapper hands the draft up (its returned payload carries the id),
    # but the served content does not: a reference is committed only when
    # the caller can see its id.
    from okg.substrate.mcp.response_bounds import ByteBoundedFastMCP

    original = ByteBoundedFastMCP._okg_serve_result

    def serve_without_id(self: Any, name: str, arguments: Any, result: Any) -> Any:
        hidden = {k: v for k, v in result.items() if k != "result_reference"}
        return original(self, name, arguments, hidden)

    monkeypatch.setattr(ByteBoundedFastMCP, "_okg_serve_result", serve_without_id)
    drafted: list[str] = []
    result = _serve(_serve_producer(drafted), {"query": "q"}, result_format)
    assert result.isError is False
    assert len(drafted) == 1
    assert drafted[0] not in _served_text(result)
    assert store.count() == 0


def test_capacity_at_the_served_point_is_served_as_a_refusal(store: ResultReferenceStore) -> None:
    _fill(store, MAX_PER_SERVER)
    drafted: list[str] = []
    result = _serve(_serve_producer(drafted), {"query": "q"}, "json")
    assert result.isError is True
    assert "result_reference_capacity" in _served_text(result)
    assert drafted[0] not in _served_text(result)
    assert store.count() == MAX_PER_SERVER


def test_draft_from_a_worker_thread_refuses_internal(store: ResultReferenceStore) -> None:
    errors: list[str] = []

    def search(query: str, limit: int = 20, max_bytes: int | None = None) -> dict[str, Any]:
        def work() -> None:
            try:
                result_references.draft(**SINGLE_USER, reader_key="reader-a", binding=BINDING, members=_members(1))
            except ResultReferenceInternal as exc:
                errors.append(exc.code)

        worker = threading.Thread(target=work)
        worker.start()
        worker.join()
        return {"results": []}

    bound_read_tool("search", search)(query="q", max_bytes=65_536)
    assert errors == ["result_reference_internal"]
    assert store.count() == 0


# --- per-principal cap on multi-user servers (rrcap A) ------------------------

#: The advertised per-principal cap (read from the module in its own test).
MAX_PER_PRINCIPAL = 128


def _claim(subject: str, **extra: Any) -> dict[str, Any]:
    """A multi-user identity claim in the shape the server builds."""
    return {
        "provider_id": "okg-idp",
        "issuer": "https://idp.example",
        "audiences": ["okg"],
        "external_subject": subject,
        "claims": {"sub": subject},
        "verification_status": "valid",
        "verified_by": "transport-a",
        "verified_at": "2026-10-05T10:00:00Z",
        "auth_time": "2026-10-05T09:00:00Z",
        **extra,
    }


ALICE = _claim("alice")
BOB = _claim("bob")


def _reader(claim: dict[str, Any] | None, note: int, multi_user: bool = True) -> str:
    """One of a principal's reader keys: the authority argument varies."""
    return continuation.reader_key({"note": note}, claim, multi_user)


def _pcommit(
    store: ResultReferenceStore,
    reader: str,
    claim: dict[str, Any] | None,
    *,
    multi_user: bool = True,
    parent: str | None = None,
):
    draft = store.mint(
        reader_key=reader,
        binding=BINDING,
        members=_members(1),
        parent=parent,
        header_claim=claim,
        multi_user=multi_user,
    )
    return store.commit(draft)


def test_principal_key_is_provider_issuer_and_subject_on_multi_user_only() -> None:
    alice = continuation.principal_key(ALICE, True)
    assert alice is not None and len(alice) == 64
    renewed = _claim(
        "alice",
        verified_at="2026-10-05T11:00:00Z",
        auth_time="2026-10-05T10:59:00Z",
        expires_at="2026-10-05T12:00:00Z",
        verified_by="transport-b",
        claims={"sub": "alice", "roles": ["b", "a"], "amr": ["pwd"]},
    )
    assert continuation.principal_key(renewed, True) == alice
    assert continuation.principal_key(BOB, True) != alice
    assert continuation.principal_key({**ALICE, "issuer": "https://other"}, True) != alice
    assert continuation.principal_key({**ALICE, "provider_id": "other"}, True) != alice
    assert continuation.principal_key(ALICE, False) is None
    assert continuation.principal_key(None, True) is None


@pytest.mark.parametrize("missing", ["provider_id", "issuer", "external_subject"])
def test_multi_user_claim_without_a_principal_field_refuses(
    store: ResultReferenceStore, missing: str,
) -> None:
    for broken in ({k: v for k, v in ALICE.items() if k != missing}, {**ALICE, missing: ""}):
        with pytest.raises(ResultReferenceInternal):
            continuation.principal_key(broken, True)
        with pytest.raises(ResultReferenceInternal):
            store.mint(
                reader_key=_reader(broken, 0), binding=BINDING, members=_members(1),
                header_claim=broken, multi_user=True,
            )
    assert store.count() == 0


def _oidc_config() -> Any:
    from okg.substrate.mcp import oidc_auth

    return oidc_auth.OidcAuthConfig(
        provider_id="okg-idp", issuer="https://idp.example", audience="okg",
        subject_claim="sub", roles_claim="roles", principal_kind="person",
    )


def _oidc_claim(subject: str, *, exp: int, roles: tuple[str, ...] = ("a", "b"), amr: list[str] | None = None) -> dict[str, Any]:
    """The claim the real OIDC request path builds for one verified token."""
    from types import SimpleNamespace

    from okg.substrate.mcp import oidc_auth

    token: dict[str, Any] = {
        "iss": "https://idp.example", "subject": subject, "exp": exp,
        "roles": list(roles), "auth_time": 1_800_000_000,
    }
    if amr:
        token["amr"] = amr
    return oidc_auth.oidc_claim_for_request(_oidc_config(), access_token=SimpleNamespace(claims=token))


def test_one_persons_oidc_and_header_claims_give_one_principal() -> None:
    first = _oidc_claim("alice", exp=2_000_000_000)
    variants = [
        _oidc_claim("alice", exp=2_000_000_600),
        _oidc_claim("alice", exp=2_000_000_000, roles=("b", "a")),
        _oidc_claim("alice", exp=2_000_000_000, amr=["pwd"]),
    ]
    assert all(v != first for v in variants)
    alice = continuation.principal_key(first, True)
    assert {continuation.principal_key(v, True) for v in variants} == {alice}
    assert continuation.principal_key(_oidc_claim("bob", exp=2_000_000_000), True) != alice

    from okg.substrate.mcp.principal_mapping import PrincipalMapping, header_claim_for_request

    mapping = PrincipalMapping(
        provider_id="okg-hdr", issuer="https://hdr.example", audience="okg",
        subject_claim="sub", subject_header="X-Okg-Principal", principal_kind="person",
    )
    headers = {"X-Okg-Principal": "alice"}
    one = header_claim_for_request(mapping, headers, transport_identity="gateway-1")
    two = header_claim_for_request(mapping, headers, transport_identity="gateway-2")
    assert one["verified_by"] != two["verified_by"]
    assert continuation.principal_key(one, True) == continuation.principal_key(two, True)


def test_eight_refreshed_tokens_hold_128_and_a_second_person_commits(
    store: ResultReferenceStore,
) -> None:
    alice = None
    readers = set()
    for refresh in range(8):
        claim = _oidc_claim("alice", exp=2_000_000_000 + 600 * refresh)
        alice = continuation.principal_key(claim, True)
        reader = continuation.reader_key(None, claim, True)
        readers.add(reader)
        for _ in range(MAX_PER_READER):
            _pcommit(store, reader, claim)
    # A refresh is a new reader key (that is acceptable) but not a new person.
    assert len(readers) == 8
    assert store.principal_count(alice) == MAX_PER_PRINCIPAL
    assert store.count() == MAX_PER_PRINCIPAL
    bob = _oidc_claim("bob", exp=2_000_000_000)
    _pcommit(store, continuation.reader_key(None, bob, True), bob)
    assert store.principal_count(continuation.principal_key(bob, True)) == 1


def test_limits_list_the_principal_cap() -> None:
    assert result_references.limits()["references_per_principal"] == 128
    assert result_references.MAX_PER_PRINCIPAL == MAX_PER_PRINCIPAL


def test_one_principal_with_nine_reader_keys_holds_128_and_another_still_commits(
    store: ResultReferenceStore,
) -> None:
    readers = [_reader(ALICE, note) for note in range(9)]
    assert len(set(readers)) == 9
    first = _pcommit(store, readers[0], ALICE)
    for reader in readers:
        for _ in range(MAX_PER_READER if reader != readers[0] else MAX_PER_READER - 1):
            _pcommit(store, reader, ALICE)
    alice = continuation.principal_key(ALICE, True)
    assert store.principal_count(alice) == MAX_PER_PRINCIPAL
    assert store.count() == MAX_PER_PRINCIPAL
    # The evicted id refuses `evicted` to its own reader, `unknown` to the
    # principal's other readers.
    with pytest.raises(ResultReferenceInvalid) as own:
        store.lookup(first.id, reader_key=readers[0])
    assert _reason(own) == "evicted"
    with pytest.raises(ResultReferenceInvalid) as sibling:
        store.lookup(first.id, reader_key=readers[1])
    assert _reason(sibling) == "unknown"
    # A second principal is not blocked by the first one's volume.
    for note in range(3):
        _pcommit(store, _reader(BOB, note), BOB)
    assert store.principal_count(continuation.principal_key(BOB, True)) == 3
    assert store.principal_count(alice) == MAX_PER_PRINCIPAL


def test_principal_cap_is_exact_at_127_128_129(store: ResultReferenceStore) -> None:
    alice = continuation.principal_key(ALICE, True)
    left, right = _reader(ALICE, 0), _reader(ALICE, 1)
    refs = []
    for i in range(MAX_PER_PRINCIPAL - 1):
        refs.append(_pcommit(store, left if i % 2 == 0 else right, ALICE))
    assert store.principal_count(alice) == MAX_PER_PRINCIPAL - 1
    refs.append(_pcommit(store, right, ALICE))
    assert store.principal_count(alice) == MAX_PER_PRINCIPAL
    assert store.lookup(refs[0].id, reader_key=left).id == refs[0].id
    # Neither reader is at its own cap; the principal is.
    assert store.usage(left)["references"] < MAX_PER_READER
    assert store.usage(right)["references"] < MAX_PER_READER
    _pcommit(store, right, ALICE)
    assert store.principal_count(alice) == MAX_PER_PRINCIPAL
    with pytest.raises(ResultReferenceInvalid) as excinfo:
        store.lookup(refs[0].id, reader_key=left)
    assert _reason(excinfo) == "evicted"
    assert store.lookup(refs[1].id, reader_key=right).id == refs[1].id


def test_principal_eviction_never_takes_an_ancestor_of_the_new_child(
    store: ResultReferenceStore,
) -> None:
    alice = continuation.principal_key(ALICE, True)
    left, right = _reader(ALICE, 0), _reader(ALICE, 1)
    oldest = _pcommit(store, left, ALICE)
    second = _pcommit(store, right, ALICE)
    for i in range(MAX_PER_PRINCIPAL - 2):
        _pcommit(store, left if i % 2 == 0 else right, ALICE)
    assert store.principal_count(alice) == MAX_PER_PRINCIPAL
    child = _pcommit(store, left, ALICE, parent=oldest.id)
    assert store.lookup(oldest.id, reader_key=left).id == oldest.id
    assert store.lookup(child.id, reader_key=left).parent == oldest.id
    with pytest.raises(ResultReferenceInvalid) as excinfo:
        store.lookup(second.id, reader_key=right)
    assert _reason(excinfo) == "evicted"
    assert store.principal_count(alice) == MAX_PER_PRINCIPAL


def test_principal_eviction_removes_the_evicted_references_children(
    store: ResultReferenceStore,
) -> None:
    alice = continuation.principal_key(ALICE, True)
    left, right = _reader(ALICE, 0), _reader(ALICE, 1)
    oldest = _pcommit(store, left, ALICE)
    child = _pcommit(store, left, ALICE, parent=oldest.id)
    for i in range(MAX_PER_PRINCIPAL - 2):
        _pcommit(store, left if i % 2 == 0 else right, ALICE)
    assert store.principal_count(alice) == MAX_PER_PRINCIPAL
    assert store.usage(left)["references"] < MAX_PER_READER
    _pcommit(store, right, ALICE)
    assert store.principal_count(alice) == MAX_PER_PRINCIPAL - 1
    for ref in (oldest, child):
        with pytest.raises(ResultReferenceInvalid) as excinfo:
            store.lookup(ref.id, reader_key=left)
        assert _reason(excinfo) == "evicted"


def test_single_user_server_has_no_principal_cap(store: ResultReferenceStore) -> None:
    readers = [_reader(ALICE, note, multi_user=False) for note in range(9)]
    for reader in readers[:8]:
        for _ in range(MAX_PER_READER):
            _pcommit(store, reader, ALICE, multi_user=False)
    # No principal eviction: every reader keeps its 128, the server is full.
    assert store.count() == MAX_PER_SERVER
    assert all(store.usage(r)["references"] == MAX_PER_READER for r in readers[:8])
    with pytest.raises(ResultReferenceCapacity) as excinfo:
        _pcommit(store, readers[8], ALICE, multi_user=False)
    assert _reason(excinfo) == "server_references"
    assert "principal_references" not in excinfo.value.data


def test_principal_cap_refusal_never_evicts(store: ResultReferenceStore) -> None:
    """A commit refused on the byte cap leaves the principal's references."""
    alice = continuation.principal_key(ALICE, True)
    full = _members_of_bytes(MAX_REFERENCE_BYTES)
    for _ in range(MAX_PER_READER - 1):
        _commit(store, "hog", full)
    left = _reader(ALICE, 0)
    for _ in range(MAX_PER_PRINCIPAL):
        _pcommit(store, left, ALICE)
    before = store.principal_count(alice)
    left_before = store.usage(left)["references"]
    big = store.mint(
        reader_key=_reader(ALICE, 1), binding=BINDING, members=full,
        header_claim=ALICE, multi_user=True,
    )
    with pytest.raises(ResultReferenceCapacity) as excinfo:
        store.commit(big)
    assert _reason(excinfo) == "total_bytes"
    assert excinfo.value.data["principal_references"] == MAX_PER_PRINCIPAL
    assert store.principal_count(alice) == before
    assert store.usage(left)["references"] == left_before
    assert store.evicted_records() == 0


def test_call_tool_commit_applies_the_principal_cap_with_the_producers_claim(
    store: ResultReferenceStore,
) -> None:
    """The producer passes the claim itself here; wiring the server's
    resolved principal into a real producer is slice 2's job."""
    alice = continuation.principal_key(ALICE, True)
    other_reader = _reader(ALICE, 7)
    oldest = _pcommit(store, other_reader, ALICE)
    for _ in range(MAX_PER_PRINCIPAL - 1):
        _pcommit(store, _reader(ALICE, 8), ALICE)
    drafted: list[str] = []
    calling_reader = _reader(ALICE, 0)

    def search(
        query: str,
        limit: int = 20,
        offset: int = 0,
        generation_id: str | None = None,
        max_bytes: int | None = None,
    ) -> dict[str, Any]:
        ref_id = result_references.draft(
            reader_key=calling_reader,
            binding={"query": query},
            members=[{"node_id": "doc:0", "rank": 1}],
            header_claim=ALICE,
            multi_user=True,
        )
        drafted.append(ref_id)
        return {
            "query": query,
            "results": [{"node_id": "doc:0"}],
            "limit": limit,
            "offset": offset,
            "next_offset": None,
            "truncated": False,
            "result_reference": {"id": ref_id},
        }

    result = _serve(search, {"query": "q"}, "json")
    assert result.isError is False
    assert store.lookup(drafted[0], reader_key=calling_reader).id == drafted[0]
    assert store.principal_count(alice) == MAX_PER_PRINCIPAL
    with pytest.raises(ResultReferenceInvalid) as excinfo:
        store.lookup(oldest.id, reader_key=other_reader)
    assert _reason(excinfo) == "evicted"


def test_header_claim_and_multi_user_are_required_at_every_draft(store: ResultReferenceStore) -> None:
    import inspect

    for function in (store.mint, result_references.DraftScope.draft, result_references.draft):
        parameters = inspect.signature(function).parameters
        for name in ("header_claim", "multi_user"):
            assert parameters[name].kind is inspect.Parameter.KEYWORD_ONLY
            assert parameters[name].default is inspect.Parameter.empty, (function, name)
    with pytest.raises(TypeError):
        store.mint(reader_key="reader-a", binding=BINDING, members=_members(1))  # type: ignore[call-arg]


# --- slice 2: pure producer and consumer checks ------------------------------

from okg.substrate.mcp.result_references import (  # noqa: E402
    ResultReferenceUnsupported,
    binding_mismatch,
    producer_refusal,
)

_PRODUCER = {"method": "lexical", "rank_by": None, "view": None, "offset": 0,
             "select": None, "max_chars": None, "limit": 10}


@pytest.mark.parametrize("change,reason", [
    ({"method": "semantic"}, "method"),
    ({"method": "alias"}, "method"),
    ({"method": "identifier_mentions"}, "method"),
    ({"method": "attrs"}, "method"),
    ({"method": "grep"}, "method"),
    ({"view": "windows"}, "method"),
    ({"rank_by": "self"}, "rank_by"),
    ({"offset": 1}, "argument"),
])
def test_producer_matrix_names_its_reason(change: dict[str, Any], reason: str) -> None:
    refusal = producer_refusal(**{**_PRODUCER, **change})
    assert isinstance(refusal, ResultReferenceUnsupported)
    assert refusal.code == "result_reference_unsupported"
    assert refusal.data["reason"] == reason


@pytest.mark.parametrize("change", [{}, {"rank_by": "records"}, {"rank_by": "passages"},
                                    {"view": "list"}, {"limit": MAX_MEMBERS},
                                    # Slice 3: metadata is a producer.
                                    {"view": "metadata"},
                                    {"view": "metadata", "rank_by": "passages"}])
def test_producer_matrix_accepts_record_searches(change: dict[str, Any]) -> None:
    assert producer_refusal(**{**_PRODUCER, **change}) is None


# --- slice 3: metadata, its summary, and the summary fit ----------------------

from okg.substrate.mcp.envelope import with_read_coverage  # noqa: E402
from okg.substrate.mcp.result_references import (  # noqa: E402
    SUMMARY_MAX_GROUPS,
    fit_summary,
    summary_block,
    summary_refusal,
)


@pytest.mark.parametrize("change,argument", [
    ({"select": ["node_id"]}, "select"),
    ({"select": []}, "select"),
    ({"max_chars": 200}, "max_chars"),
])
def test_metadata_with_a_delivery_argument_refuses_naming_it(
    change: dict[str, Any], argument: str,
) -> None:
    refusal = producer_refusal(**{**_PRODUCER, "view": "metadata", **change})
    assert isinstance(refusal, ResultReferenceUnsupported)
    assert refusal.data == {"reason": "argument", "argument": argument}
    # The same argument with a cards producer is not a reference question.
    assert producer_refusal(**{**_PRODUCER, **change}) is None


def test_metadata_still_refuses_what_no_reference_can_hold() -> None:
    for change, reason in (({"method": "grep"}, "method"), ({"rank_by": "self"}, "rank_by"),
                           ({"offset": 3}, "argument")):
        refusal = producer_refusal(**{**_PRODUCER, "view": "metadata", **change})
        assert isinstance(refusal, ResultReferenceUnsupported), change
        assert refusal.data["reason"] == reason
    over = producer_refusal(**{**_PRODUCER, "view": "metadata", "limit": MAX_MEMBERS + 1})
    assert isinstance(over, ResultReferenceCapacity)


@pytest.mark.parametrize("view", [None, "cards", "list", "windows"])
def test_summary_without_metadata_refuses_by_name(view: Any) -> None:
    refusal = summary_refusal(view=view, summary=["subtype"])
    assert isinstance(refusal, ResultReferenceUnsupported)
    assert refusal.data == {"reason": "argument", "argument": "summary"}


@pytest.mark.parametrize("summary", [
    [], ["a", "b", "c", "d", "e"], ["subtype", 3], [""], "subtype",
    # Identity fields would deliver the records a metadata page withholds;
    # a repeated field or one named `count` would collapse the group keys.
    ["node_id"], ["handle"], ["attrs.title"], ["subtype", "node_id"],
    ["subtype", "subtype"], ["count"], ["attrs.a", "count"],
])
def test_summary_of_the_wrong_shape_refuses_by_name(summary: Any) -> None:
    refusal = summary_refusal(view="metadata", summary=summary)
    assert isinstance(refusal, ResultReferenceUnsupported)
    assert refusal.data == {"reason": "argument", "argument": "summary"}


@pytest.mark.parametrize("summary", [None, ["subtype"], ["subtype", "attrs.a", "attrs.b", "attrs.c"]])
def test_summary_with_metadata_is_accepted(summary: Any) -> None:
    assert summary_refusal(view="metadata", summary=summary) is None
    assert summary_refusal(view=None, summary=None) is None


def _ordered(n: int) -> list[tuple[tuple[Any, ...], int]]:
    # Largest first, as the reference-aggregate function orders them.
    return [((f"v{i:03d}", i % 2 == 0), n - i) for i in range(n)]


def test_a_summary_of_up_to_50_groups_keeps_them_all() -> None:
    block = summary_block(_ordered(SUMMARY_MAX_GROUPS), by=["attrs.a", "attrs.b"],
                          reference_id="r1.x")
    assert block["groups_total"] == len(block["groups"]) == 50
    assert block["groups"][0] == {"attrs.a": "v000", "attrs.b": True, "count": 50}
    assert "bounds" not in block and "next" not in block
    assert summary_block([], by=["subtype"], reference_id="r1.x") == {
        "by": ["subtype"], "groups": [], "groups_total": 0,
    }


def test_a_summary_over_50_groups_keeps_the_50_largest_and_names_the_rest() -> None:
    ordered = _ordered(73)
    block = summary_block(ordered, by=["attrs.a", "attrs.b"], reference_id="r1.x")
    assert len(block["groups"]) == 50
    assert block["groups_total"] == 73
    assert [g["count"] for g in block["groups"]] == [count for _, count in ordered[:50]]
    assert block["bounds"] == ["summary_groups"]
    assert block["next"] == {"tool": "aggregate", "arguments": {
        "input": {"ref": "r1.x"}, "by": ["attrs.a", "attrs.b"], "metrics": ["count"],
        "offset": 50,
    }}


def _size(page: dict[str, Any]) -> int:
    return len(json.dumps(page))


def test_fit_summary_cuts_the_smallest_groups_and_names_the_cut() -> None:
    page = {"view": "metadata", "summary": summary_block(
        _ordered(40), by=["attrs.a"], reference_id="r1.x")}
    budget = _size(page) - 200
    fitted = fit_summary(page, budget=budget, measure=_size, reference_id="r1.x")
    kept = fitted["summary"]["groups"]
    assert _size(fitted) <= budget
    assert 0 < len(kept) < 40
    assert kept == page["summary"]["groups"][: len(kept)]
    assert fitted["summary"]["groups_total"] == 40
    assert fitted["summary"]["bounds"] == ["output_bytes"]
    assert fitted["summary"]["next"]["arguments"]["offset"] == len(kept)
    # The largest prefix that fits: one more group would not.
    longer = {**fitted, "summary": {**fitted["summary"],
                                    "groups": page["summary"]["groups"][: len(kept) + 1]}}
    assert _size(longer) > budget
    # The input page is not changed.
    assert len(page["summary"]["groups"]) == 40


def test_fit_summary_keeps_the_group_cap_bound_beside_the_byte_cut() -> None:
    page = {"summary": summary_block(_ordered(60), by=["attrs.a"], reference_id="r1.x")}
    fitted = fit_summary(page, budget=_size(page) - 300, measure=_size, reference_id="r1.x")
    assert fitted["summary"]["bounds"] == ["summary_groups", "output_bytes"]


def test_fit_summary_leaves_a_fitting_page_and_an_empty_summary_alone() -> None:
    page = {"summary": summary_block(_ordered(3), by=["attrs.a"], reference_id="r1.x")}
    assert fit_summary(page, budget=10_000, measure=_size, reference_id="r1.x") == page
    empty = {"summary": summary_block([], by=["attrs.a"], reference_id="r1.x")}
    assert fit_summary(empty, budget=1, measure=_size, reference_id="r1.x") == empty
    # Not even an empty summary fits: no groups, the cut named; the
    # byte-budget wrapper refuses such a page as it refuses any that cannot fit.
    tiny = fit_summary(page, budget=1, measure=_size, reference_id="r1.x")
    assert tiny["summary"]["groups"] == []
    assert tiny["summary"]["bounds"] == ["output_bytes"]


def _metadata_page(**extra: Any) -> dict[str, Any]:
    return {
        "operator_name": "search", "method": "lexical", "rank_by": "records",
        "view": "metadata", "total_count": None, "total_count_basis": "unknown",
        "limit": 10, "offset": 0, "next_offset": None,
        "result_reference": {"id": "r1.x", "parent": None, "order": "rank",
                             "expires_at": "2026-10-05T12:00:00Z"},
        "coverage": {
            "retrieval": {"status": "bounded", "basis": "bm25_index"},
            "selection": {"unit": "record", "basis": "retrieved_candidates",
                          "size": 7, "limit": 10},
        },
        **extra,
    }


def test_metadata_delivery_is_complete_with_zero_rows_while_retrieval_keeps_its_status() -> None:
    out = with_read_coverage(_metadata_page())
    assert "results" not in out
    assert out["coverage"]["delivery"] == {"complete": True, "view": "metadata", "rows": 0}
    assert out["coverage"]["retrieval"] == {"status": "bounded", "basis": "bm25_index"}
    assert out["coverage"]["selection"]["size"] == 7
    # A second pass (after byte fitting) gives the same answer.
    assert with_read_coverage(out)["coverage"] == out["coverage"]


def test_metadata_delivery_names_a_summary_bound() -> None:
    block = summary_block(_ordered(51), by=["attrs.a"], reference_id="r1.x")
    out = with_read_coverage(_metadata_page(summary=block))
    assert out["coverage"]["delivery"] == {
        "complete": True, "view": "metadata", "rows": 0, "bounds": ["summary_groups"],
    }


def test_a_search_page_with_no_rows_and_no_metadata_view_is_left_alone() -> None:
    page = {k: v for k, v in _metadata_page().items() if k != "view"}
    assert with_read_coverage(page) is page


def _reference_read_page(tool: str, **extra: Any) -> dict[str, Any]:
    """The page `filter` or `map` builds over a reference that holds 3 rows,
    2 of them delivered (issue #3666)."""
    shape = (
        {"predicate": {"op": "eq", "field": "attrs.status", "value": "open"}}
        if tool == "filter" else {"projection": ["node_id", "attrs.status"]}
    )
    return {
        "items": [{"node_id": "ticket:1", "attrs.status": "open"},
                  {"node_id": "ticket:2", "attrs.status": "open"}],
        "total_count": 2, "total_count_basis": "exact",
        "selection_basis": "reference_subset",
        "truncated": False, "limit": 50, "offset": 0, "next_offset": None,
        "pinned_generation_id": "g1", "generation_id": "g1",
        "operator_name": tool, "input": {"ref": "r1.x"},
        **shape, **extra,
    }


@pytest.mark.parametrize("tool", ["filter", "map"])
def test_a_filter_or_map_over_a_reference_is_bounded_by_it_never_exhaustive(tool: str) -> None:
    from okg.substrate.mcp.result_text import render

    out = with_read_coverage(_reference_read_page(
        tool, retrieval_basis="result_reference_members", reference_members=3,
    ))
    assert out["coverage"]["retrieval"] == {
        "status": "bounded", "basis": "result_reference_members",
        "bounds": ["result_reference"],
    }
    assert out["reference_members"] == 3
    assert out["coverage"]["selection"]["basis"] == "reference_subset"
    assert "exhaustive" not in json.dumps(out)
    # A second pass (after byte fitting) keeps the bound.
    assert with_read_coverage(out)["coverage"] == out["coverage"]
    text = render(out, tool=tool)
    status = text.split("\n", 1)[0]
    assert "over the reference's 3 row(s) only" in status, text
    # #3666: the bound's name (`bounds result_reference`) left the status
    # line as an internal; the reference fact above and the notice below
    # say the same limit in words.
    assert "exhaustive" not in text
    assert ("! covers only the 3 record(s) held by the result reference; "
            "the search that made it may match more") in text


@pytest.mark.parametrize("tool", ["filter", "map"])
def test_the_page_these_reads_sent_before_said_exhaustive(tool: str) -> None:
    """The defect: with the old producer facts the same page reads as a
    complete answer, in JSON and in text."""
    from okg.substrate.mcp.result_text import render

    out = with_read_coverage(_reference_read_page(tool, retrieval_exhaustive=True))
    assert out["coverage"]["retrieval"] == {"status": "exhaustive", "basis": "producer_scan"}
    text = render(out, tool=tool)
    # #3666: exhaustive retrieval is the normal state and no longer prints
    # (a bounded or approximate one still does); the page says complete and
    # nothing about a reference.
    status = text.split("\n", 1)[0]
    # D8 (read-passage-rows-3666): the ending is the last fact.
    assert "retrieval" not in status and status.split(" · ")[-1] == "complete", text
    assert "held by the result reference" not in text


def test_producer_over_the_member_cap_refuses_capacity() -> None:
    refusal = producer_refusal(**{**_PRODUCER, "limit": MAX_MEMBERS + 1})
    assert isinstance(refusal, ResultReferenceCapacity)
    assert refusal.data["reason"] == "reference_members"


_BOUND = {"deployment": "d", "database": "db", "branch": "b", "generation": "g",
          "catalog_version": 3, "authority": "a", "policy": "p", "attributes": "x"}


@pytest.mark.parametrize("field,named", [
    ("authority", "authority"), ("policy", "authority"), ("catalog_version", "catalog"),
    ("branch", "branch"), ("database", "database"), ("deployment", "deployment"),
])
def test_binding_mismatch_names_the_field(field: str, named: str) -> None:
    assert binding_mismatch(_BOUND, {**_BOUND, field: "changed"}) == named


def test_binding_mismatch_checks_authority_first_and_ignores_attributes() -> None:
    changed = {**_BOUND, "deployment": "x", "authority": "y", "attributes": "z"}
    assert binding_mismatch(_BOUND, changed) == "authority"
    assert binding_mismatch(_BOUND, {**_BOUND, "attributes": "z"}) is None


def test_consumer_reasons_carry_only_their_data() -> None:
    missing = result_references.invalid("member_missing", count=2)
    assert missing.code == "result_reference_invalid"
    assert missing.data == {"reason": "member_missing", "count": 2}
    mismatch = result_references.invalid("binding_mismatch", field="attributes")
    assert mismatch.data == {"reason": "binding_mismatch", "field": "attributes"}


def test_call_memo_is_shared_by_the_attempts_of_one_call_only(store: ResultReferenceStore) -> None:
    seen: list[Any] = []

    def read(limit: int = 3) -> dict[str, Any]:
        memo = result_references.call_memo()
        assert memo is not None
        seen.append(memo.setdefault("members", limit))
        return {"limit": limit}

    wrapped = result_references.attempt(read)
    with result_references.draft_scope():
        wrapped(limit=3)
        wrapped(limit=1)
    with result_references.draft_scope():
        wrapped(limit=2)
    assert seen == [3, 3, 2]
    assert result_references.call_memo() is None
