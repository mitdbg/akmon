"""Shared helpers for the CTI snapshot sources (nvd_feeds, cwe_catalog,
attack_stix).

The three CTI adapters ingest operator-frozen local dumps only — NO live
network calls, ever. Every arm of the CTIBench A/B shares one dated
snapshot, so a partial or silently-degraded parse would corrupt the
benchmark's fair-fight premise. These helpers therefore fail loudly
(SubstrateError with an actionable message) on unreadable files, invalid
payloads, and unparseable timestamps; they never substitute defaults.

Cross-source id contract: `has_weakness` edges emitted by nvd_feeds must
land on the exact node ids that cwe_catalog emits, so the CWE node-id
shape lives here — in ONE place — instead of being duplicated (and
drifting) across the two adapters.

Per `okg-bench-ctibench` (2026-07-27).
"""
from __future__ import annotations

import gzip
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence

from okg.substrate.errors import SubstrateError
from okg.substrate.library.sources.base import (
    EdgeFact,
    EdgeKey,
    NodeFact,
    RecordEmission,
    RecordNode,
    RecordSet,
)


__all__ = [
    "cwe_node_id",
    "load_json",
    "normalize_timestamp",
    "read_snapshot_bytes",
    "record_set_from_facts",
]


def cwe_node_id(cwe_id: str) -> str:
    """Node id for a CWE weakness, e.g. `cwe:mitre:CWE-79`.

    CWE ids are globally unique, so — like the github adapter's
    `issue:github:<owner>/<repo>:#<n>` ids — the natural key alone
    namespaces the node; no per-deployment prefix is needed.
    """
    return f"cwe:mitre:{cwe_id}"


def read_snapshot_bytes(path: Path, *, adapter: str) -> bytes:
    """Read one snapshot file, or raise with the path in the message.

    A missing/unreadable snapshot file is an operator error (the freeze
    manifest names every file), never something to skip over.
    """
    try:
        return path.read_bytes()
    except FileNotFoundError as exc:
        raise SubstrateError(
            f"source {adapter!r}: snapshot file {path} not found",
            data={"adapter": adapter, "path": str(path)},
            fix_hint="re-run the snapshot download / check the glob",
        ) from exc
    except OSError as exc:
        raise SubstrateError(
            f"source {adapter!r}: snapshot file {path} could not be read",
            data={"adapter": adapter, "path": str(path)},
            fix_hint="make the snapshot file readable",
        ) from exc


def load_json(raw: bytes, *, path: Path, adapter: str) -> Any:
    """Decode a snapshot payload as JSON, gunzipping `.gz` files first.

    Compression is decided by the file suffix, not by sniffing: a `.gz`
    file that fails to gunzip is a corrupt snapshot and raises rather
    than being retried as plain JSON (which would mask the corruption).
    """
    if path.name.endswith(".gz"):
        try:
            raw = gzip.decompress(raw)
        except (OSError, EOFError) as exc:
            raise SubstrateError(
                f"source {adapter!r}: {path} has a .gz suffix but is not "
                f"valid gzip",
                data={"adapter": adapter, "path": str(path)},
                fix_hint="re-download the snapshot file",
            ) from exc
    try:
        return json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SubstrateError(
            f"source {adapter!r}: {path} is not valid JSON",
            data={"adapter": adapter, "path": str(path)},
            fix_hint="re-download the snapshot file",
        ) from exc


def normalize_timestamp(
    value: Any,
    *,
    field: str,
    context: str,
    adapter: str,
) -> str:
    """Normalize a source timestamp to an ISO-8601 UTC string.

    The NVD 1.1 and 2.0 feed shapes stamp the SAME instant differently
    (`2021-12-10T10:15Z` vs `2021-12-10T10:15:00.000`, naive-UTC), and
    STIX uses `Z`-suffixed timestamps. Normalizing here is what makes
    "the same CVE parses identically from either feed shape" a real
    invariant instead of a fixture coincidence. Naive timestamps are
    UTC by each corpus's documented convention. Unparseable or missing
    values raise — a fact with an unknown time is a malformed entry,
    not a fact with a default time.
    """
    if not isinstance(value, str) or not value.strip():
        raise SubstrateError(
            f"source {adapter!r}: {context} is missing timestamp field "
            f"{field!r}",
            data={"adapter": adapter, "context": context, "field": field},
            fix_hint="the snapshot entry is malformed; re-download or "
                     "exclude the file",
        )
    try:
        parsed = datetime.fromisoformat(value.strip())
    except ValueError as exc:
        raise SubstrateError(
            f"source {adapter!r}: {context} has unparseable {field!r} "
            f"timestamp {value!r}",
            data={
                "adapter": adapter,
                "context": context,
                "field": field,
                "value": value,
            },
            fix_hint="expected an ISO-8601 timestamp",
        ) from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).isoformat()


def record_set_from_facts(
    facts: Sequence[NodeFact | EdgeFact],
) -> RecordSet:
    """Declare the full logical-record footprint of a snapshot run.

    Same construction as the github adapter's: one record per emitted
    node, with each edge attached to its dst record when the dst is
    emitted by this run (else the src record). This is what lets the
    SourceRunner synthesize retractions when a record disappears from
    the next snapshot.
    """
    records: dict[str, RecordEmission] = {}
    nodes: dict[str, NodeFact] = {
        fact.node_id: fact
        for fact in facts
        if isinstance(fact, NodeFact)
    }
    for node in nodes.values():
        records[node.node_id] = RecordEmission(
            primary=RecordNode(
                node_id=node.node_id,
                subtype=node.subtype,
                source_record_id=node.source_record_id,
                source_revision=node.source_revision,
            )
        )
    for edge in (fact for fact in facts if isinstance(fact, EdgeFact)):
        record_key = edge.dst if edge.dst in nodes else edge.src
        records.setdefault(record_key, RecordEmission()).edges.append(
            EdgeKey(
                src=edge.src,
                dst=edge.dst,
                edge_type=edge.edge_type,
                attrs=edge.attrs,
                provenance=edge.provenance,
                confidence=edge.confidence,
                source_record_id=edge.source_record_id,
                source_revision=edge.source_revision,
            )
        )
    return RecordSet(records=records)
