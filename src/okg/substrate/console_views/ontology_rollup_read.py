"""Read-side companion for the ontology graph rollup.

The operator Graph view's ontology mode used to recompute the realised
``(src_subtype, edge_type, dst_subtype)`` edge aggregation by double-joining the
~2M reconstructed Chronos edges on every request (~15-80s on okg-workspace). The
rollup is now a **Chronos-registered table** (``okg.ontology_rollup``) maintained
by a publish-stage running sum (``okg.substrate.publisher.ontology_rollup``);
this module reads it AS-OF the pinned generation's checkpoint, so the read is a
compact scan of a few hundred rows instead of a graph-wide double-join.

That Graph view, and the ``compose_graph(source="ontology")`` composer that
consumed this read, were removed in issue #1924. The rollup is still maintained
at publish time and Health still probes its availability, but it has no data
reader today. Retiring or re-pointing the maintenance is separate work.

Being a Chronos table, the rollup gets branching / forking / as-of-generation
reconstruction for free: a checkpoint is a metadata marker, and a read at
generation N sees exactly the rows live at N via the interval predicate (a
carry-forward cycle that wrote no rollup rows still reads the prior rows — no
generation-keyed relation, no carry-forward fragility).

The producer dimension is what makes enrichers surface: the rollup's
``_enricher:<linker>`` / source-connector / ``_publisher_derived:unattributed``
rows let the read report honest ``total_sources`` / ``total_enrichers`` counts
that the old (producer-blind) double-join could never produce.

Fail-loud contract: ``read_ontology_rollup`` raises ``OntologyRollupUnavailable``
when the pinned generation has no maintained rollup — the checkpoint is absent,
the table is unregistered, or no maintenance SENTINEL is present for the
generation (i.e. the publish-stage hook never ran for it). Callers MUST surface
that rather than silently falling back to the O(graph) scan.
"""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

import psycopg

from okg.substrate.derived_artifacts.maintained_derivation import (
    MaintainedDerivationUnavailable,
    SentinelSpec,
    read_maintained_rows,
)
from okg.substrate.derived_artifacts.maintained_derivation_constants import (
    ENRICHER_PREFIX,
    UNATTRIBUTED_DERIVED_PRODUCER,
)

# The Chronos-registered table this read reconstructs as-of a pinned generation, plus
# its sentinel schema. The read delegates the as-of-generation read, the three
# loud-fail reasons, and the sentinel separation to the maintained-derivation kit
# (read_maintained_rows); the kit detects the sentinel by the shared SENTINEL_TOKEN on
# every PK column. The producer constants come from the import-light single source so
# read and write share ONE copy. Importing the kit pulls no graph_store at module
# level, so this stays an import-light read.
ONTOLOGY_ROLLUP_TABLE = "okg.ontology_rollup"
_ENRICHER_PREFIX = ENRICHER_PREFIX
_ROLLUP_SENTINEL = SentinelSpec(
    pk_columns=("src_subtype", "edge_type", "dst_subtype", "producer"),
    cadence_column="edge_count",
)


class OntologyRollupUnavailable(MaintainedDerivationUnavailable):
    """Raised when no maintained ontology rollup exists for the pinned generation.

    A typed subclass of the kit's ``MaintainedDerivationUnavailable``: generic
    maintained-derivation handlers catch it too, and callers that catch
    ``OntologyRollupUnavailable`` specifically (today, Health's critical-read
    prerequisite probe) keep working. A loud failure on purpose -- the read must
    never silently fall back to the O(graph) double-join scan when the rollup is
    missing for the pinned generation.
    """


@dataclass(frozen=True)
class OntologyBucketRow:
    """One ``(src_subtype, edge_type, dst_subtype, producer)`` rollup row."""

    src_subtype: str
    edge_type: str
    dst_subtype: str
    producer: str
    edge_count: int


@dataclass
class OntologyRollup:
    """The realised subtype-pair / producer edge counts at a generation."""

    generation_id: int | str
    rows: list[OntologyBucketRow] = field(default_factory=list)

    def pair_counts(self) -> dict[tuple[str, str, str], int]:
        """``(src_subtype, edge_type, dst_subtype) -> total edges`` (summed over
        producers). This is exactly the old double-join's grouped count, so the
        link payload is byte-identical to the pre-rollup read."""
        out: dict[tuple[str, str, str], int] = {}
        for row in self.rows:
            key = (row.src_subtype, row.edge_type, row.dst_subtype)
            out[key] = out.get(key, 0) + int(row.edge_count)
        return out

    def producers(self) -> dict[str, int]:
        """``producer -> total edges`` across all buckets."""
        out: dict[str, int] = {}
        for row in self.rows:
            out[row.producer] = out.get(row.producer, 0) + int(row.edge_count)
        return out

    def producer_kind_counts(self) -> dict[str, int]:
        """Distinct producer counts split into ``source`` / ``enricher`` /
        ``unattributed_derived``.

        ``_enricher:<linker>`` rows are enrichers; the explicit
        ``_publisher_derived:unattributed`` bucket is its own count (SHOWN, never
        folded into a fake enricher); everything else is a source connector.
        """
        sources: set[str] = set()
        enrichers: set[str] = set()
        unattributed = 0
        for producer in self.producers():
            if producer == UNATTRIBUTED_DERIVED_PRODUCER:
                unattributed = 1
            elif producer.startswith(_ENRICHER_PREFIX):
                enrichers.add(producer)
            else:
                sources.add(producer)
        return {
            "total_sources": len(sources),
            "total_enrichers": len(enrichers),
            "unattributed_derived": unattributed,
        }


def _to_bucket_row(row: Mapping[str, Any]) -> OntologyBucketRow:
    return OntologyBucketRow(
        src_subtype=str(row["src_subtype"]),
        edge_type=str(row["edge_type"]),
        dst_subtype=str(row["dst_subtype"]),
        producer=str(row["producer"]),
        edge_count=int(row["edge_count"] or 0),
    )


def read_ontology_rollup(
    conn: psycopg.Connection[Any],
    *,
    deployment_name: str,
    generation_id: int | str,
) -> OntologyRollup:
    """Return the maintained rollup rows AS-OF the pinned generation.

    ``generation_id`` is the generation CHECKPOINT id (e.g. ``generation:1234``)
    OR the integer ``target_generation_id`` -- both are accepted and normalised to
    the Chronos checkpoint the rollup table is reconstructed against.

    Raises ``OntologyRollupUnavailable`` when the generation has no maintained
    rollup (checkpoint absent / table unregistered / no maintenance sentinel) --
    the caller decides how to surface it (never a silent scan). The as-of-generation
    read, the three loud-fail reasons, and the sentinel separation are delegated to
    the maintained-derivation kit (``read_maintained_rows``); the kit's generic
    ``MaintainedDerivationUnavailable`` is translated to the rollup-specific subclass
    so existing callers keep catching ``OntologyRollupUnavailable``.
    """
    del deployment_name  # one-db-per-deployment: the checkpoint is on this db
    try:
        rows = read_maintained_rows(
            conn,
            table=ONTOLOGY_ROLLUP_TABLE,
            generation_id=generation_id,
            select_columns=(
                "src_subtype",
                "edge_type",
                "dst_subtype",
                "producer",
                "edge_count",
            ),
            sentinel_spec=_ROLLUP_SENTINEL,
            row_mapper=_to_bucket_row,
        )
    except MaintainedDerivationUnavailable as exc:
        raise OntologyRollupUnavailable(
            exc.generation_id, exc.reason, exc.detail
        ) from exc
    return OntologyRollup(generation_id=generation_id, rows=rows)


__all__ = [
    "OntologyBucketRow",
    "OntologyRollup",
    "OntologyRollupUnavailable",
    "UNATTRIBUTED_DERIVED_PRODUCER",
    "read_ontology_rollup",
]
