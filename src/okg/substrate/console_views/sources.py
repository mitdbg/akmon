"""Per-source live-contribution composer for the console Sources view.

Surfaces, per source, live projected counts from the maintained console count
rollup:

  - ``records_in_gen`` — distinct nodes in the pinned Chronos checkout whose
                         source matches the source.
  - ``edges_in_gen``   — distinct edges in the pinned Chronos checkout whose
                         source matches the source.

The connector inventory is the deployment's real ``source_registry.yaml`` — not
a hardcoded list — so declared-but-empty sources still appear (records_in_gen 0,
never omitted). Sources present in the maintained live rollup or source-progress
state but absent from the registry (derived ``_cascade:`` / ``_enricher:`` /
``catalog:`` producers) are listed and flagged ``in_registry: false`` — no silent
omission.

The legacy ``total_facts`` / ``total_edge_facts`` fields remain in the payload
for compatibility, but they are explicit ``null`` values with an unavailable
reason. Computing them by grouping ``okg.node_facts`` / ``okg.edge_facts`` on the
request path is a production-scale scan, so the user-facing console no longer
does it.
"""

from __future__ import annotations

import logging
from typing import Any, TYPE_CHECKING

from okg.substrate.console_views.console_count_rollup_read import (
    require_console_count_rollup_enabled,
)
from okg.substrate.console_views.producer_identity import (
    is_unlisted_alias_key,
    registry_identities,
    registry_source_names,
    runtime_key_index,
)
from okg.substrate.console_views.today_workspace import _humanize_age
from okg.substrate.mcp.session import Session

if TYPE_CHECKING:  # pragma: no cover - typing only
    from okg.substrate.console_views.console_count_rollup_read import (
        ConsoleCountRollupCache,
    )


_LAST_SYNC_SQL = """
SELECT source_name, max(updated_at)
  FROM okg.source_progress
 WHERE deployment = %(d)s
 GROUP BY source_name
"""

_FACT_LOG_COUNTS_UNAVAILABLE_REASON = "raw_fact_log_counts_unbounded"
logger = logging.getLogger(__name__)


def _registry_source_names(deployment_name: str) -> list[str]:
    """The connector ids declared in the deployment's source_registry.yaml.

    Kept as the name other console composers import. The identity rule itself
    lives in one place, `console_views.producer_identity`.
    """
    return registry_source_names(deployment_name)


def compose_sources(
    conn: Any,
    *,
    deployment_name: str,
    session: Session,
    count_rollup: ConsoleCountRollupCache | None = None,
) -> dict[str, Any]:
    """Per-source ingested-vs-projected counts over the published generation."""
    live: dict[str, int] = {}
    live_edges: dict[str, int] = {}
    last_sync: dict[str, Any] = {}
    generation_id = session.generation_id

    # Live per source must come from the maintained rollup. The old flag-off raw
    # GROUP BY over okg.graph_nodes/edges is intentionally gone.
    require_console_count_rollup_enabled(generation_id=generation_id)
    from okg.substrate.console_views.console_count_rollup_read import (
        COUNT_KIND_EDGE_SOURCE,
        COUNT_KIND_SOURCE,
        rollup_reader,
    )

    # Through the request's cache when a composed read supplied one, so
    # `/console/producers` takes ONE generation read lease instead of one per
    # composer (issue #2202); a fresh cache when this composer is its own
    # route, which reads exactly as it always did.
    rolled = rollup_reader(
        count_rollup, conn, generation_id=generation_id
    ).read({COUNT_KIND_SOURCE, COUNT_KIND_EDGE_SOURCE})
    for (kind, name), count in rolled.items():
        if kind == COUNT_KIND_SOURCE:
            live[name] = count
        else:
            live_edges[name] = count
    # source_progress is bounded by deployment/source/key and carries freshness
    # signal. Fact-log totals are intentionally not read here: grouping
    # okg.node_facts/edge_facts is an unbounded request-time scan.
    with conn.cursor() as cur:
        cur.execute(_LAST_SYNC_SQL, {"d": deployment_name})
        for source_name, updated_at in cur.fetchall():
            last_sync[str(source_name)] = updated_at

    prefix = f"{deployment_name}."

    # The registry's declared ownership_id is the key the runtime attributes
    # counts and progress to. Joining on a key built from the document name
    # instead is what listed one producer twice.
    identities = registry_identities(deployment_name)
    by_key = runtime_key_index(identities)

    def _row(
        name: str,
        source_key: str,
        in_registry: bool,
        progress_names: set[str],
    ) -> dict[str, Any]:
        ages = [
            last_sync[progress_name]
            for progress_name in sorted(progress_names)
            if last_sync.get(progress_name) is not None
        ]
        return {
            "name": name,
            "source_key": source_key,
            "in_registry": in_registry,
            "total_facts": None,
            "total_edge_facts": None,
            "fact_counts_available": False,
            "fact_counts_unavailable_reason": _FACT_LOG_COUNTS_UNAVAILABLE_REASON,
            "records_in_gen": live.get(source_key, 0),
            "edges_in_gen": live_edges.get(source_key, 0),
            "last_sync_age": _humanize_age(max(ages)) if ages else None,
        }

    sources: list[dict[str, Any]] = []
    claimed: set[str] = set()

    for identity in identities:
        # Both names, because okg.source_progress.source_name carries the
        # document key on rows written before the ownership id was recorded.
        progress_names = {identity.ownership_id, identity.name}
        sources.append(
            _row(identity.name, identity.ownership_id, True, progress_names)
        )
        claimed.update(progress_names)

    observed = set(live) | set(live_edges) | set(last_sync)
    # The alias guard, re-aimed rather than removed. Its old form skipped a
    # doubly prefixed key the composer had manufactured itself by prefixing
    # ``source_progress`` names; this read joins on recorded keys only, so
    # that spelling can no longer be built. What survives is the harm the
    # guard existed to stop: ``okg.source_progress.source_name`` carries the
    # deployment prefix on rows written before the ownership id was recorded,
    # and where a registry declares the bare key as its ``ownership_id`` that
    # historical row matches neither name. Listed, it becomes a second,
    # zero-count producer under the SAME displayed name as the configured
    # one, and a client keyed by name shows the real source's counts as zero.
    # Such a key is skipped, and named in ``alias_keys_not_listed`` so the
    # omission is on the record. Its progress stamp is not folded into the
    # configured row: attributing work by a prefix-stripped name is the
    # name-derived rule this module exists to stop.
    alias_keys: list[str] = []
    for key in sorted(observed - claimed):
        if key in by_key:
            continue
        display = key[len(prefix):] if key.startswith(prefix) else key
        if is_unlisted_alias_key(
            key,
            deployment_name=deployment_name,
            by_key=by_key,
            has_counts=lambda key=key: (
                live.get(key, 0) != 0 or live_edges.get(key, 0) != 0
            ),
        ):
            alias_keys.append(key)
            claimed.add(key)
            continue
        # Nothing configured claims this key: a derived ``_cascade:`` /
        # ``_enricher:`` / ``catalog:`` producer, or a source that ran and was
        # later removed from the registry. Listed, never silently dropped.
        sources.append(_row(display, key, False, {key}))
        claimed.add(key)

    # Registry connectors first, then by live desc, then by facts desc.
    sources.sort(
        key=lambda s: (
            not s["in_registry"],
            -s["records_in_gen"],
            -s["edges_in_gen"],
            s["name"],
        )
    )

    return {
        "deployment": deployment_name,
        "generation_id": generation_id,
        "source_count": len(sources),
        "sources": sources,
        # Recorded keys that are only a prefixed spelling of a configured
        # producer's name and carry no records and no edges. Named, not
        # silently dropped.
        "alias_keys_not_listed": alias_keys,
    }
