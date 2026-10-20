"""Step-7 gate for console-count-rollup (#444 slice 2): the maintained-derivation
marginal-cost boundary in the differential delta proof is genuinely WIRED + populated
-- not a permanently-``unavailable`` stub like native_view_scope / derived_relation_scope
(which are in BOUNDARY_KEYS but never passed at the commit-replay call site).

These lock the pure boundary-building logic (no replay needed): a maintained cycle's
checkpoint-metadata summary becomes an ``available`` rows-touched marginal-cost record
(the O(changed) ``dirty_bucket_count``); an absent summary is a LOUD all-None
``unavailable`` record, never a zero-like default.
"""
from __future__ import annotations


import okg.bench.differential_replay_proof as proof


def _row(**scope_kwargs):
    return proof.build_delta_proof_row(
        i=0,
        commit_sha="abc1234567",
        previous_commit_sha="def8901234",
        subject="s",
        changed_paths=["a.py"],
        line_delta={"files_changed": 1, "lines_added": 1, "lines_deleted": 0},
        previous_generation_id=None,
        generation_id="okg:gen:1",
        **scope_kwargs,
    )


def test_boundary_key_registered():
    assert "maintained_derivation_scope" in proof.BOUNDARY_KEYS


def test_available_when_maintained_cycle_summary_present():
    # The publisher's persisted checkpoint_metadata["console_count_rollup"] summary.
    summary = {
        "mode": "incremental",
        "forcing_reason": None,
        "dirty_bucket_count": 3,
        "rows_written": 2,
        "rows_deleted": 1,
    }
    scope = proof.maintained_derivation_boundary(summary)
    row = _row(maintained_derivation_scope=scope)
    b = row["boundaries"]["maintained_derivation_scope"]
    assert b["status"] == "available", b
    assert b["rows_touched"] == 3            # O(changed): dirty_bucket_count
    assert b["mode"] == "incremental"


def test_absent_summary_is_loud_all_none_never_zero():
    scope = proof.maintained_derivation_boundary(None)
    assert scope["status"] == "unavailable"
    # Loud-on-missing: every field is None, NEVER 0 (a 0 would read as "free").
    for field in ("rows_touched", "mode", "forcing_reason"):
        assert scope[field] is None, (field, scope[field])


def test_not_a_dead_boundary_unlike_native_view_scope():
    """The whole point: passing the scope makes it `available`, whereas the
    never-passed boundaries (native_view_scope) stay `unavailable` on the same row."""
    summary = {"mode": "full_recount", "forcing_reason": "first_publish",
               "dirty_bucket_count": None}
    row = _row(maintained_derivation_scope=proof.maintained_derivation_boundary(summary))
    b = row["boundaries"]
    assert b["maintained_derivation_scope"]["status"] == "available"
    # native_view_scope is in BOUNDARY_KEYS but never passed -> permanently unavailable.
    assert b["native_view_scope"]["status"] == "unavailable"


def test_default_row_without_scope_is_unavailable_not_absent():
    # When the harness does not pass the scope (timeout / skipped row), the boundary
    # is a loud unavailable record -- present in the schema, never a silent omission.
    row = _row()
    b = row["boundaries"]["maintained_derivation_scope"]
    assert b["status"] == "unavailable"
    assert b["rows_touched"] is None
