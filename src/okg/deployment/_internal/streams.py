"""Stream wrappers for inter-workflow derived-edge passing (Phase 2 §5.2).

DBOS streams are append-only FIFO queues durably tied to a producer
workflow. Two workflows in the same deployment can cooperate via a
stream: producer ``write_derived_edges_stream`` writes; consumer
``consume_derived_edges_stream`` reads (blocks until done).

The SDK wrappers namespace the stream key with the deployment name so
two deployments cannot accidentally cross-consume each other's
streams: ``deployment:<name>:edges:<stream_key>``.

The :class:`DerivedEdgeSpec` dataclass moves into this module's
public API; the legacy import path through ``stubs`` continues to
work for backward compatibility.
"""
from __future__ import annotations

import logging
from typing import Iterator

from .context import current_context
from .stubs import DerivedEdgeSpec


__all__ = [
    "DerivedEdgeSpec",
    "consume_derived_edges_stream",
    "write_derived_edges_stream",
]


log = logging.getLogger(__name__)


def _stream_key(*, deployment: str, stream_key: str) -> str:
    return f"deployment:{deployment}:edges:{stream_key}"


def write_derived_edges_stream(
    *,
    stream_key: str,
    edges: list[DerivedEdgeSpec] | Iterator[DerivedEdgeSpec],
) -> None:
    """Write derived-edge specs to a DBOS stream.

    Producer workflow runs in its own workflow context; the stream is
    keyed by the producer's workflow id (DBOS contract). Consumer
    workflows that want this stream pass the producer's workflow id
    plus the same ``stream_key`` to :func:`consume_derived_edges_stream`.
    """
    try:
        from dbos import DBOS
    except ImportError as e:
        raise RuntimeError(
            "write_derived_edges_stream requires the dbos package"
        ) from e

    ctx = current_context()
    full_key = _stream_key(
        deployment=ctx.deployment, stream_key=stream_key,
    )

    count = 0
    for edge in edges:
        # Serialize the dataclass to a plain dict for the stream.
        # DBOS streams use JSON serialization internally.
        payload = {
            "src": edge.src,
            "dst": edge.dst,
            "edge_type": edge.edge_type,
            "attrs": edge.attrs,
            "dedupe_key": edge.dedupe_key,
            "confidence": edge.confidence,
            "provenance": edge.provenance,
        }
        DBOS.write_stream(full_key, payload)
        count += 1

    # Close the stream so the consumer's iterator terminates.
    DBOS.close_stream(full_key)

    log.info(
        "write_derived_edges_stream: deployment=%s key=%s count=%d",
        ctx.deployment, stream_key, count,
    )


def consume_derived_edges_stream(
    *,
    producer_workflow_id: str,
    stream_key: str,
) -> Iterator[DerivedEdgeSpec]:
    """Yield derived-edge specs from a producer's stream.

    Blocks until the producer's ``close_stream`` arrives; then the
    iterator terminates.
    """
    try:
        from dbos import DBOS
    except ImportError as e:
        raise RuntimeError(
            "consume_derived_edges_stream requires the dbos package"
        ) from e

    ctx = current_context()
    full_key = _stream_key(
        deployment=ctx.deployment, stream_key=stream_key,
    )
    for payload in DBOS.read_stream(producer_workflow_id, full_key):
        yield DerivedEdgeSpec(
            src=payload["src"],
            dst=payload["dst"],
            edge_type=payload["edge_type"],
            attrs=payload.get("attrs", {}),
            dedupe_key=payload.get("dedupe_key", ""),
            confidence=payload.get("confidence", 1.0),
            provenance=payload.get("provenance", "derived_deterministic"),
        )
