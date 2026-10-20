"""Rollup source: node_series -> sparse graph facts, through admission.

A ``RollupSource`` is a scheduled ``SourceAdapter`` that reads windowed
aggregates from ``okg.node_series`` and emits ONE sparse ``series_rollup`` node
per (node, metric) target - never the raw samples. It carries a single trust
class; its admission_mode (fast_track | review) routes its facts to a branch
(default | quarantine) per the existing per-producer admission machinery.

Because admission routing is per-PRODUCER, the source's scope MUST be
homogeneous in external trust: a target whose external trust differs from the
source's class is rejected at construction (per-fact trust stamping is out of
scope - per-producer admission cannot express it). This prevents laundering an
untrusted-external rollup onto the canonical default branch.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterator, Optional

from okg.substrate.library.sources.base import NodeFact, SourceRun
from okg.substrate.db.connection import connect as db_connect

SNAPSHOT_SUBTYPE = "series_rollup"
TRUST_CLASSES = frozenset({"trusted", "untrusted"})
# admission_mode per trust class -> graph_branch_for_admission_mode routes it.
_ADMISSION_MODE = {"trusted": "fast_track", "untrusted": "review"}


class RollupTrustError(ValueError):
    """A rollup source's scope is not homogeneous in external trust."""


@dataclass(frozen=True)
class RollupTarget:
    node_id: str
    metric: str
    external_trust: str  # "trusted" | "untrusted"


@dataclass
class RollupSource:
    deployment_name: str
    dsn: str
    trust_class: str
    targets: list[RollupTarget]
    window_seconds: int = 3600
    name: str = "external_rollup"
    profile: str = "append_feed"

    def __post_init__(self) -> None:
        if self.trust_class not in TRUST_CLASSES:
            raise RollupTrustError(
                f"trust_class {self.trust_class!r} not in {sorted(TRUST_CLASSES)}"
            )
        mismatched = [
            t for t in self.targets if t.external_trust != self.trust_class
        ]
        if mismatched:
            raise RollupTrustError(
                "rollup source scope is not trust-homogeneous: targets "
                f"{[t.node_id for t in mismatched]} have external_trust != "
                f"{self.trust_class!r}. Split into one rollup producer per trust "
                "class (per-producer admission cannot stamp per-fact trust)."
            )

    @property
    def admission_mode(self) -> str:
        return _ADMISSION_MODE[self.trust_class]

    def run(
        self,
        run_id: str,
        *,
        mode: str = "cursor",
        cursor: Optional[dict[str, Any]] = None,
        sync_scope: Optional[dict[str, Any]] = None,
    ) -> SourceRun:
        revision = {"run_id": run_id}
        rollups: list[dict[str, Any]] = []
        with db_connect(self.dsn, autocommit=True) as conn:
            with conn.cursor() as cur:
                for t in self.targets:
                    cur.execute(
                        """
                        SELECT count(*), max(value), min(value),
                               (array_agg(value ORDER BY observed_at DESC))[1] AS last_value
                          FROM okg.node_series
                         WHERE deployment_name=%s AND node_id=%s AND metric=%s
                           AND observed_at >= now() - make_interval(secs => %s)
                        """,
                        (self.deployment_name, t.node_id, t.metric, self.window_seconds),
                    )
                    n, mx, mn, last = cur.fetchone()
                    if not n:
                        continue
                    rollups.append({
                        "target": t,
                        "sample_count": int(n),
                        "max_value": mx,
                        "min_value": mn,
                        "last_value": last,
                    })

        def _facts() -> Iterator[NodeFact]:
            for r in rollups:
                t = r["target"]
                node_id = f"series_rollup:{t.node_id}:{t.metric}"
                yield NodeFact(
                    node_id=node_id,
                    subtype=SNAPSHOT_SUBTYPE,
                    attrs={
                        "label": f"rollup {t.metric}@{t.node_id}",
                        "deployment_name": self.deployment_name,
                        "source_node_id": t.node_id,
                        "metric": t.metric,
                        "window_seconds": self.window_seconds,
                        "sample_count": r["sample_count"],
                        "max_value": r["max_value"],
                        "min_value": r["min_value"],
                        "last_value": r["last_value"],
                        "external_trust": t.external_trust,
                    },
                    source_record_id={
                        "deployment": self.deployment_name,
                        "node_id": t.node_id,
                        "metric": t.metric,
                    },
                    source_revision=revision,
                    op="I",
                )

        return SourceRun(
            facts=_facts(),
            completed_scope=mode in {"scope_complete", "reconcile"},
            next_cursor=dict(cursor or {}),
            run_mode=mode,
        )


__all__ = ["RollupSource", "RollupTarget", "RollupTrustError", "SNAPSHOT_SUBTYPE"]
