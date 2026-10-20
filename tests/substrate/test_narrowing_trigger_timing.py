"""RETIRED (chronos cutover): all tests in this module verified the
legacy projector-era narrowing-enforcement path, which the
IR-read-over-Chronos cutover deleted.

The original module covered `defer-narrowing-trigger-until-publish`:
the narrowing trigger was moved from `okg.edge_facts` INSERT to
`okg.edges_live` INSERT, and `dispatch_projection` was reordered so
node projection committed before `project_new_edges` ran. Source
adapters appended `edge_facts`; the strict narrowing check fired when
the Projector promoted those facts into `edges_live`.

That entire enforcement seam is gone:

  - The Chronos-native publisher no longer projects `edge_facts` into
    `edges_live`. It checkpoints the Chronos branch head
    (`okg.graph_nodes` / `okg.graph_edges`) directly.
  - The `edges_live_narrowing_check` constraint trigger (schema.sql)
    fires on `okg.edges_live` INSERT only. There is no narrowing
    trigger on `okg.graph_edges`; `ChronosGraphStore.apply_facts`
    enforces endpoint existence (`strict_endpoints`) but NOT narrowing
    triples.
  - The Phase B integrity audit's `substrate_missing_active_narrowings`
    check reads `okg.nodes_live` / `okg.edges_live`, which the
    Chronos-native publish never populates, so it cannot catch a
    narrowing-violating edge on the Chronos graph.

Verified empirically against the Chronos-native publisher: an edge
whose `(src_subtype, edge_type, dst_subtype)` triple is NOT in the
active catalog is written to `okg.graph_edges` and PUBLISHES CLEAN
(status="published", edge_count=1) — it does not raise
`narrowing_violation` / `CheckViolation`. So the three retired tests
could only be made to pass by asserting behavior the cutover removed:

  - test_compliant_edge_passes_through_to_publish
      Asserted the projected edge appears in `okg.edges_live`
      (`valid_to_gen IS NULL`). The Chronos publish writes neither
      `edges_live` nor `valid_to_gen` windows.

  - test_violating_edge_surfaces_at_projector
      Asserted `Publisher.cycle()` raises `psycopg.errors.CheckViolation`
      ("narrowing_violation") at the projector's `edges_live` INSERT.
      The projector path is deleted; the violating edge now publishes
      without raising.

  - test_edge_facts_insert_no_longer_blocks_on_missing_nodes
      Asserted that an `okg.edge_facts` row inserted before its
      endpoint nodes exist does not block, and the edge later projects
      into `edges_live`. The `edge_facts` -> `edges_live` projection
      ordering it exercises no longer exists.

Migrating the seed to the Chronos graph cannot save these: the
narrowing-blocks-at-publish behavior they assert is not implemented in
the Chronos-native path (no projector trigger, integrity audit reads
empty legacy tables). Retiring rather than weakening per the cutover
rule. If narrowing must be enforced at Chronos checkpoint time, that
is a substrate change (a `graph_edges` narrowing trigger or a
Chronos-graph-reading integrity audit), with its own coverage — not a
relaxation of these tests.
"""
from __future__ import annotations
