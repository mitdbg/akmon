"""Shared scanner contracts: scope identity, source proof, bounds, cursors.

PACT shared-read-scanners-3199, requirements scope, source-proof, bounds and
extension. These need no database; the shared-scanner section of ``test_search_where_db.py`` binds the
same contracts to a real published generation and reader authority.
"""
from __future__ import annotations

import base64
import os
import zlib

import pytest

from okg.substrate.mcp import search_where as sw
from okg.substrate.mcp.continuation import (
    ContinuationTokenInvalid,
    issue_continuation,
    signing_key_id,
)
from okg.substrate.mcp.read_scanner import (
    MAX_SCAN_BODY_BYTES,
    MAX_SCAN_TOKEN_CHARS,
    EnumerationEvidence,
    FinishedCandidate,
    ScanBatch,
    ScanContinuationUnavailable,
    ScanInputInvalid,
    consume_scan_continuation,
    finish_scan_batch,
    issue_scan_continuation,
    make_scan_context,
    scan_token_body_bytes,
)

COLUMNS = {"id": "node_id", "type": "subtype", "source": "source"}
EQ_TICKET = {"op": "eq", "field": "type", "value": "ticket"}


def _binding(**changes):
    binding = {
        "kind": "search", "deployment": "analyst-search", "database": "db-fp",
        "branch": "main", "generation": "g1", "catalog_version": 7, "subtype": None,
        "path": None, "sort": "bm25", "projection": {"ids": ["a"], "max_bytes": 4096},
        "authority": "auth-1", "policy": "policy-1", "key_id": "key-1",
    }
    binding.update(changes)
    return binding


def _context(binding=None, **changes):
    kwargs = dict(query="zirconium", scope={"records": None}, compiled=sw.compile_where(EQ_TICKET, columns=COLUMNS),
                  source="lexical", result_unit="record", membership_target="parent_record",
                  max_bytes=8192, ordering_identity="keyset:node_id")
    kwargs.update(changes)
    return make_scan_context(binding or _binding(), **kwargs)


def _measure(token: str) -> int:
    return 1024 + len(token)


# ── scope identity ────────────────────────────────────────────────────


@pytest.mark.parametrize("change", [
    {"generation": "g2"}, {"catalog_version": 8}, {"deployment": "other"},
    {"authority": "auth-2"}, {"policy": "policy-2"}, {"key_id": "key-2"},
    {"database": "db-other"}, {"branch": "feature"},
])
def test_pin_and_authority_changes_change_scope_identity(change) -> None:
    base, moved = _context(), _context(_binding(**change))
    assert base.scope_identity != moved.scope_identity
    token = issue_scan_continuation(base, {"after": "a"}, measure=_measure)
    with pytest.raises(ContinuationTokenInvalid) as error:
        consume_scan_continuation(moved, token)
    assert error.value.data["reason"] == "binding_mismatch"


@pytest.mark.parametrize("change", [
    {"query": "zirconium cladding"}, {"scope": {"records": ["t:1"]}},
    {"compiled": sw.compile_where({"op": "not", "clause": EQ_TICKET}, columns=COLUMNS)},
    {"compiled": sw.compile_where(EQ_TICKET, columns={**COLUMNS, "type": "v.subtype"})},
    {"source": "grep"}, {"result_unit": "passage"}, {"membership_target": "self"},
    {"ordering_identity": "keyset:other"},
])
def test_query_scope_compiler_and_membership_changes_change_scope_identity(change) -> None:
    assert _context().scope_identity != _context(**change).scope_identity
    assert _context().scope_identity == _context().scope_identity


def test_folded_parent_and_passage_membership_never_share_a_cursor() -> None:
    parent = _context(result_unit="record", membership_target="parent_record")
    passage = _context(result_unit="passage", membership_target="self")
    token = issue_scan_continuation(parent, {"after": "ticket:1"}, measure=_measure)
    assert consume_scan_continuation(parent, token) == {"after": "ticket:1"}
    with pytest.raises(ContinuationTokenInvalid) as error:
        consume_scan_continuation(passage, token)
    assert error.value.data["reason"] == "binding_mismatch"


def test_context_copies_the_binding_and_keeps_every_authority_part() -> None:
    binding = _binding()
    context = _context(binding)
    binding["generation"] = "mutated"
    binding["projection"]["ids"].append("b")
    kept = context.binding
    assert kept["generation"] == "g1" and kept["projection"]["ids"] == ["a"]
    assert kept["projection"]["scanner"]["identity"] == context.scope_identity
    assert kept["projection"]["max_bytes"] == 8192
    for key in ("authority", "policy", "key_id", "catalog_version", "deployment", "database"):
        assert kept[key] == _binding()[key]
    kept["generation"] = "again"
    assert context.binding["generation"] == "g1"


@pytest.mark.parametrize("bad", [
    {"drop": "authority"}, {"drop": "policy"}, {"drop": "projection"},
    {"projection": {"scanner": {}}}, {"projection": ["not", "an", "object"]},
])
def test_incomplete_or_reserved_bindings_are_refused(bad) -> None:
    binding = _binding()
    if "drop" in bad:
        binding.pop(bad["drop"])
    else:
        binding.update(bad)
    with pytest.raises(ValueError):
        _context(binding)


@pytest.mark.parametrize("change", [
    {"max_bytes": 4095}, {"max_bytes": True}, {"source": ""}, {"result_unit": None},
    {"membership_target": ""}, {"ordering_identity": ""}, {"compiled": {"sql": "TRUE"}},
])
def test_invalid_scanner_inputs_are_refused(change) -> None:
    with pytest.raises(ValueError):
        _context(**change)


# ── source proof: enumeration, counts, finishing ──────────────────────


def _evidence(context, **changes):
    return EnumerationEvidence(**{"domain": "prefilter_candidates", "scope_identity": context.scope_identity,
                                  "method": "whole_sql", **changes})


@pytest.mark.parametrize("batch_kwargs", [
    {"terminal": False},
    {"terminal": True, "work_bounds": ("row_cap",)},
    {"terminal": True, "work_bounds": ("timeout",)},
    {"terminal": True, "approximate": True},
])
def test_bounded_approximate_or_nonterminal_batches_cannot_carry_exhaustion(batch_kwargs) -> None:
    context = _context()
    with pytest.raises(ValueError):
        ScanBatch(rows=({"node_id": "a"},), enumeration=_evidence(context), **batch_kwargs)


@pytest.mark.parametrize("method", ["native_bm25_stop", "short_page", "estimate", ""])
def test_only_actual_terminal_sql_traversal_is_enumeration(method) -> None:
    with pytest.raises(ValueError):
        EnumerationEvidence(domain="d", scope_identity="s", method=method)


def test_a_source_batch_cannot_claim_finished_match_exhaustion() -> None:
    context = _context()
    with pytest.raises(ValueError):
        ScanBatch(rows=(), terminal=True, enumeration=_evidence(context, finished=True))


def test_exact_candidate_count_is_never_promoted_to_exhaustion() -> None:
    context = _context()
    rows = ({"node_id": "a"}, {"node_id": "b"})
    batch = ScanBatch(rows=rows, count_basis="exact", total_count=2, terminal=False)
    keep = finish_scan_batch(context, batch, lambda row: FinishedCandidate(rows=(row,)),
                             finished_domain="visible_matches")
    assert keep.enumeration is None and len(keep.rows) == 2
    terminal_no_proof = ScanBatch(rows=rows, count_basis="exact", total_count=2, terminal=True)
    assert finish_scan_batch(context, terminal_no_proof, lambda row: FinishedCandidate(rows=(row,)),
                             finished_domain="visible_matches").enumeration is None


def test_prefilter_exhaustion_needs_named_finishing_to_prove_matches() -> None:
    context = _context()
    batch = ScanBatch(rows=({"node_id": "a"}, {"node_id": "b"}), terminal=True,
                      count_basis="exact", total_count=2, enumeration=_evidence(context))
    unnamed = finish_scan_batch(context, batch, lambda row: FinishedCandidate(rows=(row,)))
    assert unnamed.enumeration is None and unnamed.batch.enumeration.finished is False
    hidden_b = finish_scan_batch(
        context, batch,
        lambda row: None if row["node_id"] == "b" else FinishedCandidate(rows=(row,), auxiliary=({"parent": "p"},)),
        finished_domain="visible_matches")
    assert hidden_b.enumeration.finished and hidden_b.enumeration.domain == "visible_matches"
    assert hidden_b.candidates_finished == 2 and [r["node_id"] for r in hidden_b.rows] == ["a"]
    assert hidden_b.auxiliary == ({"parent": "p"},)


def test_proof_from_another_scope_is_refused() -> None:
    context, other = _context(), _context(query="other")
    batch = ScanBatch(rows=(), terminal=True, enumeration=_evidence(other))
    with pytest.raises(ValueError):
        finish_scan_batch(context, batch, lambda row: None, finished_domain="visible_matches")


def test_a_timeout_in_finishing_never_becomes_empty_exact_output() -> None:
    context = _context()
    batch = ScanBatch(rows=({"node_id": "a"},), terminal=True, count_basis="exact", total_count=1,
                      enumeration=_evidence(context))

    def finish(row):
        raise TimeoutError("statement timeout")

    with pytest.raises(TimeoutError):
        finish_scan_batch(context, batch, finish, finished_domain="visible_matches")


@pytest.mark.parametrize("kwargs", [
    {"count_basis": "exact"}, {"count_basis": "lower_bound"},
    {"count_basis": "unknown", "total_count": 0}, {"count_basis": "estimate", "total_count": 3},
    {"count_basis": "exact", "total_count": 0, "work_bounds": ("timeout",)},
    {"count_basis": "exact", "total_count": 3, "approximate": True},
    {"count_basis": "lower_bound", "total_count": -1}, {"count_basis": "exact", "total_count": True},
    {"work_bounds": "timeout"}, {"count_unit": ""},
])
def test_count_qualification_is_explicit_and_honest(kwargs) -> None:
    """A stated basis needs a stated total; a timeout or work stop may
    report a lower bound, never an exact (possibly empty) total."""
    with pytest.raises(ValueError):
        ScanBatch(rows=(), **kwargs)


def test_a_bounded_batch_reports_what_it_saw_as_a_lower_bound() -> None:
    batch = ScanBatch(rows=[{"node_id": "a"}], count_basis="lower_bound", total_count=1,
                      work_bounds=["timeout"])
    assert batch.rows == ({"node_id": "a"},) and batch.work_bounds == ("timeout",)
    assert batch.enumeration is None


def test_finishing_never_mutates_source_candidates() -> None:
    context = _context()
    rows = ({"node_id": "a", "attrs": {"k": 1}},)
    batch = ScanBatch(rows=rows)

    def finish(row):
        row["attrs"]["k"] = 2
        return FinishedCandidate(rows=(row,))

    finished = finish_scan_batch(context, batch, finish)
    assert batch.rows[0]["attrs"]["k"] == 1 and finished.rows[0]["attrs"]["k"] == 2
    with pytest.raises(ValueError):
        finish_scan_batch(context, batch, lambda row: {"rows": ()})


# ── bounds: continuation state and wire limits ────────────────────────


def test_unproven_ordering_withholds_continuation() -> None:
    context = _context(ordering_identity=None)
    with pytest.raises(ScanContinuationUnavailable) as error:
        issue_scan_continuation(context, {"offset": 10}, measure=_measure)
    assert error.value.reason == "ordering_unproven"


def test_an_accepted_token_fits_both_limits_and_round_trips() -> None:
    context = _context()
    cursor = {"after": "ticket:1", "noise": os.urandom(2600).hex()}  # incompressible, near the cap
    token = issue_scan_continuation(context, cursor, measure=_measure)
    assert scan_token_body_bytes(token) <= MAX_SCAN_BODY_BYTES
    assert len(token) <= MAX_SCAN_TOKEN_CHARS and token.isascii()
    assert consume_scan_continuation(context, token) == cursor


def test_state_over_the_body_bound_is_withheld_not_truncated() -> None:
    context = _context()
    with pytest.raises(ScanContinuationUnavailable) as error:
        issue_scan_continuation(context, {"text": "a" * MAX_SCAN_BODY_BYTES}, measure=_measure)
    assert error.value.reason == "state_bound"


def test_a_token_that_does_not_fit_the_response_budget_is_withheld() -> None:
    context = _context()
    with pytest.raises(ScanContinuationUnavailable) as error:
        issue_scan_continuation(context, {"after": "a"}, measure=lambda token: context.max_bytes + 1)
    assert error.value.reason == "output_bound"


def test_the_receiver_enforces_the_state_bound_too() -> None:
    """A token signed with the right key and binding but an oversize body
    (another issuer, or a pre-limit build) is refused on receipt."""
    context = _context()
    token = issue_continuation(context.binding, cursor={"text": "a" * MAX_SCAN_BODY_BYTES})
    assert scan_token_body_bytes(token) > MAX_SCAN_BODY_BYTES
    with pytest.raises(ContinuationTokenInvalid) as error:
        consume_scan_continuation(context, token)
    assert error.value.data["reason"] == "state_bound"


def test_the_receiver_refuses_resume_without_ordering_proof() -> None:
    proven = _context()
    token = issue_continuation(proven.binding, cursor={"offset": 3})
    unproven = make_scan_context(_binding(), query="zirconium", scope={"records": None},
                                 compiled=sw.compile_where(EQ_TICKET, columns=COLUMNS), source="lexical",
                                 result_unit="record", membership_target="parent_record", max_bytes=8192)
    with pytest.raises(ContinuationTokenInvalid):
        consume_scan_continuation(unproven, token)


def test_altered_tokens_are_refused() -> None:
    # Bind the live key id, as a served read does: a forged body that names
    # another key is refused as signing_key_changed instead.
    context = _context(_binding(key_id=signing_key_id()))
    token = issue_scan_continuation(context, {"after": "a"}, measure=_measure)
    encoded, signature = token.split(".")
    body = zlib.decompress(base64.urlsafe_b64decode(encoded + "=" * (-len(encoded) % 4)))
    forged_body = body.replace(b'"keyset:node_id"', b'"keyset:forged"')
    assert forged_body != body
    forged = base64.urlsafe_b64encode(zlib.compress(forged_body)).rstrip(b"=").decode() + "." + signature
    with pytest.raises(ContinuationTokenInvalid) as error:
        consume_scan_continuation(context, forged)
    assert error.value.data["reason"] == "signature_invalid"
    reordered = _context(_binding(key_id=signing_key_id()), ordering_identity="keyset:forged")
    with pytest.raises(ContinuationTokenInvalid) as mismatch:
        consume_scan_continuation(reordered, token)
    assert mismatch.value.data["reason"] == "binding_mismatch"
    with pytest.raises(ContinuationTokenInvalid):
        consume_scan_continuation(context, "x" * (MAX_SCAN_TOKEN_CHARS + 1))



# ── binding values and forged finished evidence ───────────────────────


@pytest.mark.parametrize("key", ["kind", "deployment", "database", "branch", "generation",
                                 "catalog_version", "authority", "key_id", "projection", "sort"])
def test_a_null_required_binding_part_is_refused(key) -> None:
    with pytest.raises(ValueError, match="must not be null"):
        _context(_binding(**{key: None}))


def test_a_null_policy_is_a_real_state_and_stays_bound() -> None:
    """No active Nomos bundle gives policy None; it must still change identity."""
    unpoliced = _context(_binding(policy=None))
    assert unpoliced.binding["policy"] is None
    assert unpoliced.scope_identity != _context().scope_identity


@pytest.mark.parametrize("changes", [
    {"finished": True},
    {"finished": 1},
    {"finished": "yes"},
])
def test_finished_evidence_cannot_be_constructed_outside_finishing(changes) -> None:
    context = _context()
    with pytest.raises(ValueError, match="only finish_scan_batch"):
        _evidence(context, **changes)


def test_finished_evidence_survives_copy_and_pickle() -> None:
    import copy
    import pickle

    context = _context()
    batch = ScanBatch(rows=({"node_id": "a"},), terminal=True, count_basis="exact",
                      total_count=1, enumeration=_evidence(context))
    proof = finish_scan_batch(context, batch, lambda row: FinishedCandidate(rows=(row,)),
                              finished_domain="visible_matches").enumeration
    assert proof.finished is True
    for clone in (copy.copy(proof), copy.deepcopy(proof), pickle.loads(pickle.dumps(proof))):
        assert clone == proof and clone.finished is True


def _finished_proof():
    context = _context()
    batch = ScanBatch(rows=({"node_id": "a"},), terminal=True, count_basis="exact",
                      total_count=1, enumeration=_evidence(context))
    proof = finish_scan_batch(context, batch, lambda row: FinishedCandidate(rows=(row,)),
                              finished_domain="visible_matches").enumeration
    assert proof.finished is True
    return proof


def test_the_seal_is_not_a_constructor_argument() -> None:
    from okg.substrate.mcp import read_scanner

    context = _context()
    for seal in (object(), read_scanner._FINISH_SEAL):
        with pytest.raises(TypeError):
            _evidence(context, finished=True, _seal=seal)


@pytest.mark.parametrize("change", [
    {"domain": "other_domain"},
    {"scope_identity": "another-scope"},
    {"method": "terminal_keyset"},
])
def test_replace_cannot_move_a_finished_proof(change) -> None:
    """replace() rebuilds through __init__, so the seal never travels with it."""
    import copy
    import dataclasses
    import pickle

    proof = _finished_proof()
    for clone in (proof, copy.copy(proof), copy.deepcopy(proof),
                  pickle.loads(pickle.dumps(proof))):
        with pytest.raises(ValueError, match="only finish_scan_batch"):
            dataclasses.replace(clone, **change)


def test_replace_may_only_produce_unfinished_evidence() -> None:
    import dataclasses

    moved = dataclasses.replace(_finished_proof(), domain="other", finished=False)
    assert moved.finished is False and moved._seal is None


# ── scanner identity values with no canonical JSON form ───────────────


@pytest.mark.parametrize(("part", "value"), [
    ("query", "lone \ud800 surrogate"),
    ("query", float("nan")),
    ("query", float("inf")),
    ("query", object()),
    ("scope", {"records": ["\udfff"]}),
    ("scope", {"\ud800": None}),
    ("scope", {"limit": float("-inf")}),
])
def test_a_query_or_scope_without_canonical_json_is_refused_by_name(part, value) -> None:
    with pytest.raises(ScanInputInvalid) as caught:
        _context(**{part: value})
    assert isinstance(caught.value, ValueError)
    assert not isinstance(caught.value, UnicodeError)
    assert caught.value.reason == "invalid_scan_value"
    assert caught.value.data == {"reason": "invalid_scan_value", "part": part}
    assert part in str(caught.value)


def test_a_paired_surrogate_query_is_valid_text() -> None:
    """Astral characters are valid Unicode and fingerprint normally."""
    assert _context(query="zirconium \U0001F600").scope_identity != _context().scope_identity
