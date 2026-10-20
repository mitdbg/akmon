"""The segment-delta classifier partitions dirty rows into added / retracted /
updated. Presence of a previous row is NOT the same as that row having been LIVE.

``_chronos_segment_table_delta`` feeds the interval-tracked running counter
(``previous_count + nodes_added - nodes_retracted``). It compared the dirty row
against whatever row was visible at the previous branch point. A tombstone is a
visible row, so:

* a node retracted in generation N and re-inserted in N+1 matched
  ``p.graph_key IS NOT NULL`` and was booked as an UPDATE, contributing zero --
  the counter never recovered it;
* re-retracting an ALREADY-tombstoned node matched the retracted predicate a
  second time and decremented twice for one node.

The two faces are one defect in one predicate. These tests drive the SQL directly
over a hand-built interval table, because the end-to-end pipeline does not reach
the second face: the projector deduplicates a repeated retraction, so no dirty
tombstone is ever produced for it.

The INTERVAL SHAPE below matches a real publish: an original live interval
closed at the retraction, a tombstone interval spanning the previous branch
point, and the new row opening in the current segment.

The tombstone PAYLOAD is a separate axis, and both values of it are modelled
here because they land in different pre-fix buckets. A chronos delete of a live
row makes the tombstone INHERIT that row's data columns verbatim -- see
``inherited_cols = old_cols if deleted`` and ``if row is None and deleted:
replacement = dict(old_row)`` in ``chronos_core.branching._interval_backend``.
Only a delete of a key with no live row yields NULL/empty columns. So:

* tombstone payload DIFFERENT from the reinserted payload (the shape a source
  that restores an item with new content produces): pre-fix the row matched the
  `updated` bucket, contributing zero to the counter -- a MISCLASSIFICATION;
* tombstone payload IDENTICAL to the reinserted payload (a source that drops an
  item and restores it byte-identically, the ordinary inherit-then-restore
  shape): pre-fix `payload_changed` was FALSE as well, so the row matched NO
  bucket at all -- a total OMISSION, strictly worse than the miscount.

The FIRST face (resurrection) is a reproduced production defect: it aborts a real
publish. The SECOND (double decrement) is proven only at the SQL level; see the
note on its test for the two production paths that were measured and did NOT
reach it.
"""
from __future__ import annotations

import json
import uuid

import psycopg
import pytest

from okg.substrate.publisher import Publisher

pytestmark = pytest.mark.postgres


# Interval geometry shared by every case below.
#
#   previous segment P: branch_point = 500   (what "was live before" means)
#   current  segment C: [500, MAX)           (what counts as dirty this cycle)
#
# A row is LIVE at the previous branch point when live_lo <= 500 < live_hi.
# A row is DIRTY this cycle when 500 <= live_lo and live_hi <= MAX.
MAX = 1_000_000
PREV_BRANCH_POINT = 500
CUR_LO = 500


def _build_fixture(conn: psycopg.Connection, rows: list[tuple]) -> tuple[str, str, str]:
    """Create an interval table + branch/segment metadata and return
    (physical_table, branch_id, previous_segment_id)."""
    sfx = uuid.uuid4().hex[:8]
    table = f"delta_probe_{sfx}"
    branch_id = f"delta-probe-branch-{sfx}"
    with conn.cursor() as cur:
        cur.execute("SELECT COALESCE(max(segment_id), 0) FROM "
                    "_chronos_branch_interval_segments")
        base = int(cur.fetchone()[0])
        prev_seg, cur_seg = base + 1, base + 2
        cur.execute(
            f'CREATE TABLE "{table}" ('
            "  node_id text NOT NULL,"
            "  subtype text,"
            "  attrs jsonb,"
            "  deleted boolean,"
            "  live_lo numeric(32,0) NOT NULL,"
            "  live_hi numeric(32,0) NOT NULL,"
            # the identity-sampling query selects its dirty candidates by writer
            # segment rather than by interval containment
            "  writer_segment_id integer)"
        )
        cur.executemany(
            "INSERT INTO _chronos_branch_interval_segments "
            "(segment_id, live_lo, live_hi, branch_point, created_at, metadata) "
            "VALUES (%s, %s, %s, %s, '', '{}')",
            [
                (prev_seg, 0, MAX, PREV_BRANCH_POINT),
                (cur_seg, CUR_LO, MAX, MAX - 1),
            ],
        )
        cur.execute(
            "INSERT INTO _chronos_branch_interval_branches "
            "(branch_id, current_segment_id, created_at, metadata) "
            "VALUES (%s, %s, '', '{}')",
            (branch_id, cur_seg),
        )
        cur.executemany(
            f'INSERT INTO "{table}" '
            "(node_id, subtype, attrs, deleted, live_lo, live_hi, "
            " writer_segment_id) "
            "VALUES (%s, %s, %s::jsonb, %s, %s, %s, %s)",
            [
                (nid, subtype, json.dumps(attrs), deleted, lo, hi,
                 cur_seg if lo >= CUR_LO else prev_seg)
                for nid, subtype, attrs, deleted, lo, hi in rows
            ],
        )
    return table, branch_id, str(prev_seg)


def _drop_fixture(conn: psycopg.Connection, table: str, branch_id: str) -> None:
    """The chronos reset fixture only truncates public._chronos%, so these probe
    tables and branch rows would otherwise accumulate in the shared test DB."""
    with conn.cursor() as cur:
        cur.execute(f'DROP TABLE IF EXISTS "{table}"')
        cur.execute(
            "DELETE FROM _chronos_branch_interval_branches WHERE branch_id = %s",
            (branch_id,),
        )


def _classify(conn: psycopg.Connection, rows: list[tuple]) -> tuple[int, int, int]:
    table, branch_id, prev_seg = _build_fixture(conn, rows)
    try:
        return Publisher._chronos_segment_table_delta(
            conn,
            physical_table=table,
            key_column="node_id",
            compare_columns=("subtype", "attrs"),
            branch_name=branch_id,
            previous_segment_id=prev_seg,
        )
    finally:
        _drop_fixture(conn, table, branch_id)


def test_resurrected_row_classifies_as_added_not_updated(
    clean_runtime: str,
) -> None:
    """A node whose latest previous state was a TOMBSTONE and which now exists is
    ADDED. Pre-fix this returned (0, 0, 1) -- an update, contributing zero to the
    running counter, so the node never came back."""
    rows = [
        # original live interval, already closed before the branch point
        ("resurrected", "case", {"v": 1}, False, 1, 300),
        # the retraction: the row that IS live at branch point 500
        ("resurrected", "case", {}, True, 300, 600),
        # the resurrection, written in the current segment
        ("resurrected", "case", {"v": 2}, False, 600, MAX),
    ]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        assert _classify(conn, rows) == (1, 0, 0)


def test_resurrection_with_an_unchanged_payload_is_added_not_omitted(
    clean_runtime: str,
) -> None:
    """The harder production shape, and the one the fixtures above do NOT model.

    A chronos delete of a live row does not blank the tombstone: it INHERITS the
    retracted row's data columns (``replacement = dict(old_row)`` /
    ``inherited_cols = old_cols if deleted`` in
    ``chronos_core.branching._interval_backend``). So a source that drops an item
    and later restores it BYTE-IDENTICALLY produces a tombstone and a reinsert
    whose subtype and attrs are equal, and ``payload_changed`` is FALSE.

    Pre-fix that row satisfied NONE of the three predicates:

    * added     -- `p.graph_key IS NULL` is false, the tombstone is a row;
    * retracted -- `d.deleted` is false, the dirty row is the reinsert;
    * updated   -- `payload_changed` is false, the payload is identical.

    It was therefore OMITTED from the delta entirely, which is worse than the
    misclassification the other resurrection case shows: an update at least
    appears in the receipt as a change. Here the resurrection is invisible, and
    the running counter is short by one with nothing in the delta to explain it.
    """
    rows = [
        ("restored", "case", {"v": 1}, False, 1, 300),
        # the tombstone carries the PRE-RETRACTION attrs, as chronos writes it
        ("restored", "case", {"v": 1}, True, 300, 600),
        # ... and the source restores the item byte-identically
        ("restored", "case", {"v": 1}, False, 600, MAX),
    ]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        assert _classify(conn, rows) == (1, 0, 0)


def test_re_retracting_a_tombstoned_row_does_not_decrement_again(
    clean_runtime: str,
) -> None:
    """The symmetric face. A retraction counts only against a row that was LIVE.
    Pre-fix this returned (0, 1, 0) -- a second decrement for a node already
    subtracted, driving the counter below the true live count.

    REACHABILITY IS UNESTABLISHED, and this test is the only witness. Two
    candidate production paths were measured and NEITHER reaches this shape:
    repeating a retraction across cycles writes no new row at all (the projector
    deduplicates it), and resurrect-then-re-retract within one cycle collapses to
    a single tombstone whose previous state is not the older tombstone -- that
    path reports (0, 0, 0) on the pre-fix classifier too. So this is a predicate
    hardened against a defect that is real in the SQL but not currently known to
    be triggerable end to end. It is kept because the predicate must partition
    correctly regardless, and because the upstream dedup that hides it is not a
    guarantee anyone has written down.
    """
    rows = [
        ("double_retracted", "case", {"v": 1}, False, 1, 300),
        ("double_retracted", "case", {}, True, 300, 600),
        # a SECOND tombstone landing in the current segment
        ("double_retracted", "case", {}, True, 600, MAX),
    ]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        assert _classify(conn, rows) == (0, 0, 0)


def test_plain_add_retract_and_update_are_unchanged(clean_runtime: str) -> None:
    """The three cases that already worked keep working. The predicates partition
    one space, so a fix that only repairs resurrection would move the error here
    rather than remove it."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        # A node that never existed before.
        assert _classify(conn, [
            ("fresh", "case", {"v": 1}, False, 600, MAX),
        ]) == (1, 0, 0)
        # A live node retracted for the first time.
        assert _classify(conn, [
            ("retract_me", "case", {"v": 1}, False, 1, 600),
            ("retract_me", "case", {}, True, 600, MAX),
        ]) == (0, 1, 0)
        # A live node whose attrs changed.
        assert _classify(conn, [
            ("update_me", "case", {"v": 1}, False, 1, 600),
            ("update_me", "case", {"v": 2}, False, 600, MAX),
        ]) == (0, 0, 1)
        # A live node rewritten identically is not a change at all.
        assert _classify(conn, [
            ("touch_me", "case", {"v": 1}, False, 1, 600),
            ("touch_me", "case", {"v": 1}, False, 600, MAX),
        ]) == (0, 0, 0)


def test_all_cases_partition_one_space(clean_runtime: str) -> None:
    """Every dirty key lands in at most one bucket, and the buckets sum the way
    the running counter assumes: live_after = live_before + added - retracted."""
    rows = [
        ("fresh", "case", {"v": 1}, False, 600, MAX),
        ("retract_me", "case", {"v": 1}, False, 1, 600),
        ("retract_me", "case", {}, True, 600, MAX),
        ("update_me", "case", {"v": 1}, False, 1, 600),
        ("update_me", "case", {"v": 2}, False, 600, MAX),
        ("resurrected", "case", {"v": 1}, False, 1, 300),
        ("resurrected", "case", {}, True, 300, 600),
        ("resurrected", "case", {"v": 2}, False, 600, MAX),
        ("double_retracted", "case", {"v": 1}, False, 1, 300),
        ("double_retracted", "case", {}, True, 300, 600),
        ("double_retracted", "case", {}, True, 600, MAX),
        # untouched live node: live at the branch point, not dirty
        ("untouched", "case", {"v": 1}, False, 1, MAX),
    ]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        added, retracted, updated = _classify(conn, rows)

    # live before the cycle: retract_me, update_me, untouched  (resurrected and
    # double_retracted were both tombstoned)  -> 3
    # live after the cycle:  fresh, update_me, resurrected, untouched          -> 4
    live_before, live_after = 3, 4
    assert (added, retracted, updated) == (2, 1, 1)
    assert live_before + added - retracted == live_after, (
        f"running counter {live_before} + {added} - {retracted} "
        f"= {live_before + added - retracted}, but the graph holds {live_after} "
        "live rows"
    )


def _effect_labels(conn: psycopg.Connection, rows: list[tuple]) -> dict[str, str]:
    table, branch_id, prev_seg = _build_fixture(conn, rows)
    query = Publisher._chronos_graph_effect_identity_query(
        physical_table=table,
        key_column="node_id",
        type_column="subtype",
        is_edge=False,
    )
    try:
        with conn.cursor() as cur:
            cur.execute(query, (branch_id, 2000, prev_seg, 100))
            return {str(key): str(kind) for key, kind, _type, _src, _dst in cur}
    finally:
        _drop_fixture(conn, table, branch_id)


def test_effect_manifest_labels_agree_with_the_count_classifier(
    clean_runtime: str,
) -> None:
    """The bounded graph-effect manifest is a second query over the dirty rows,
    and it is stamped into the publish receipt beside the delta counts. If the
    two use different senses of 'was present before', one receipt contradicts the
    other: the counter calls a resurrection an add while the manifest calls it an
    update.

    Pre-fix the manifest labelled a resurrection 'updated' and emitted a spurious
    'retracted' item for an already-tombstoned row.

    SCOPE: this checks the LABELS agree, not that the two queries see the same
    rows -- they do not. The manifest selects candidates by writer_segment_id and
    truncates to candidate_limit; the classifier selects by interval containment
    and does not truncate. This fixture happens to make those sets coincide, so
    it cannot and does not assert set equality.
    """
    rows = [
        ("fresh", "case", {"v": 1}, False, 600, MAX),
        ("retract_me", "case", {"v": 1}, False, 1, 600),
        ("retract_me", "case", {}, True, 600, MAX),
        ("update_me", "case", {"v": 1}, False, 1, 600),
        ("update_me", "case", {"v": 2}, False, 600, MAX),
        ("resurrected", "case", {"v": 1}, False, 1, 300),
        ("resurrected", "case", {}, True, 300, 600),
        ("resurrected", "case", {"v": 2}, False, 600, MAX),
        ("double_retracted", "case", {"v": 1}, False, 1, 300),
        ("double_retracted", "case", {}, True, 300, 600),
        ("double_retracted", "case", {}, True, 600, MAX),
        ("untouched", "case", {"v": 1}, False, 1, MAX),
    ]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        labels = _effect_labels(conn, rows)
        added, retracted, updated = _classify(conn, rows)

    assert labels == {
        "fresh": "added",
        "resurrected": "added",
        "retract_me": "retracted",
        "update_me": "updated",
    }, labels
    # Every key the manifest reports is a key the classifier also counted as a
    # change -- the labels partition the same way. (Not a set-equality claim; see
    # the docstring on why the two dirty sets are not the same set in general.)
    assert len(labels) == added + retracted + updated
