"""Read one bounded JSON snapshot, retaining authority only after completion."""

import hashlib
import json
from pathlib import Path

from jsonschema import Draft202012Validator

from okg.deployment import (
    Checkpoint, ConnectorAdapter, ConnectorHealth, ConnectorRegistration,
    ConnectorRun, ContentHashProbe, EdgeFact, EdgeKey, NodeFact, RecordEmission,
    RecordNode, RecordSet, RejectedItem, SdkVersionRange,
)

CONFIG_SCHEMA = {
    "type": "object",
    "properties": {"path": {"type": "string", "minLength": 1}},
    "required": ["path"],
    "additionalProperties": False,
}

# The snapshot itself is a node, and every record it carries hangs off it by a
# `contains` edge. That is the honest shape, not a framework requirement: a
# complete scan is an assertion about a whole collection. An install needs
# records; it needs relationships only when a source declares them in its
# admission policy, so a records-only connector completes `okg install` with no
# edges at all. This one declares the edge below, so it must publish it.
COLLECTION_KEY = "example-collection"
COLLECTION_NODE_ID = "example-collection:snapshot"
CONTAINS = "contains"
# An integer because this edge is not a guess: containment is read straight
# from the snapshot. It carries no serialization requirement. The conformance
# kit once refused floats, which made that choice load-bearing; since #1970 it
# digests facts with the publish path's own encoder and accepts any finite
# float, so a fractional confidence is fine when a connector means one.
CERTAIN = 1

REGISTRATION = ConnectorRegistration(
    connector_type="example_records",
    version="0.1.0",
    module="okg_example_records.reader",
    class_name="RecordSource",
    ownership_id="example.records",
    # A bounded snapshot re-read in full, whose completeness authorizes
    # disappearance, is a discovery crawl. The runner maps retraction safety by
    # (profile, run_mode) (okg.substrate.ingest.runner._POLICY_MODE): a
    # reference_catalog publishes release_new/release_unchanged, while
    # scope_complete/scope_incomplete -- the modes this reader returns -- belong
    # to discovery_crawl. Declaring the other profile leaves the runner unable
    # to evaluate a retraction, so a deletion refuses instead of applying.
    source_class="discovery_crawl",
    trust_label="curated",
    sdk_range=SdkVersionRange("1.0.0", "2.0.0"),
    config_schema=CONFIG_SCHEMA,
    auth_modes=("none",),
    capabilities=("cancellation", "checkpoint_resume", "partial_health", "record_set_authority"),
    output_signature={
        "nodes": [{"subtype": "example_collection"}, {"subtype": "example_record"}],
        "edges": [
            {
                "src_subtype": "example_collection",
                "edge_type": CONTAINS,
                "dst_subtype": "example_record",
            },
        ],
    },
    # `scoped_locator`, not `domain_key`: the id identifies a record inside the
    # snapshot this reader walks, which is what a discovery crawl's identity is.
    record_identity_kind="scoped_locator",
    record_identity_fields=("id",),
    source_revision_kind="content_hash",
    deletion_semantics="missing_from_completed_scope",
    publication_mode="published_generation",
    description="Bounded JSON records; complete scans authorize disappearance.",
)


def _digest(payload) -> str:
    return hashlib.sha256(json.dumps(
        payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
    ).encode()).hexdigest()


class RecordReader:
    """IDs are immutable upstream IDs; revisions hash canonical record content."""

    name = "example_records"
    profile = "discovery_crawl"

    def run(self, ctx):
        if ctx.cancelled():
            return ConnectorRun(
                facts=[], next_checkpoint=ctx.checkpoint,
                health=ConnectorHealth(status="partial", reason="cancelled before reading"),
                run_mode="scope_incomplete",
            )
        Draft202012Validator(CONFIG_SCHEMA).validate(dict(ctx.config))
        payload = json.loads(Path(ctx.config["path"]).read_text(encoding="utf-8"))
        if not isinstance(payload, dict) or type(payload.get("complete")) is not bool:
            raise ValueError("snapshot requires an explicit boolean complete field")
        rows = payload.get("records")
        if not isinstance(rows, list):
            raise ValueError("snapshot records must be a list")
        # Validate all identities before emitting: duplicates or bad IDs cannot
        # silently replace a prior record or give a partial scan delete authority.
        ids = [row.get("id") if isinstance(row, dict) else None for row in rows]
        if any(not isinstance(key, str) or not key for key in ids):
            raise ValueError("every record requires a nonempty string id")
        if len(set(ids)) != len(ids):
            raise ValueError("duplicate upstream record id")
        # The record set is keyed by upstream id and also carries the snapshot
        # node, so one reserved key is withheld rather than silently shadowed.
        if COLLECTION_KEY in ids:
            raise ValueError("record id " + COLLECTION_KEY + " is reserved for the snapshot")
        cursor = ctx.checkpoint.next_cursor or {}
        previous = cursor.get("revisions", {})
        previous_collection = cursor.get("collection")
        revisions, records, facts, rejected = {}, {}, [], []
        complete = payload["complete"]
        for row in sorted(rows, key=lambda item: item["id"]):
            if ctx.cancelled():
                complete = False
                break
            key = row["id"]
            if row.get("kind") != "record":
                rejected.append(RejectedItem(key, "kind outside example_record signature"))
                # Invalid input may have replaced a previously admitted record.
                # Reject visibly and preserve the entire prior scope on this run.
                if key in previous:
                    complete = False
                continue
            if not isinstance(row.get("text"), str):
                raise ValueError("record text must be a string")
            digest = _digest(row)
            node_id = "example-record:" + key
            identity, revision = {"id": key}, {"sha256": digest}
            # The containment edge is part of this record's footprint, so a
            # record that disappears from a complete scan takes its edge with it.
            records[key] = RecordEmission(
                primary=RecordNode(node_id, "example_record", identity, revision),
                edges=(EdgeKey(
                    src=COLLECTION_NODE_ID, dst=node_id, edge_type=CONTAINS,
                    source_record_id=identity, source_revision=revision,
                    confidence=CERTAIN,
                ),),
            )
            revisions[key] = digest
            if previous.get(key) != digest:
                facts.append(NodeFact(
                    node_id, "example_record", {"text": row["text"]}, identity, revision,
                ))
                facts.append(EdgeFact(
                    src=COLLECTION_NODE_ID, dst=node_id, edge_type=CONTAINS,
                    source_record_id=identity, source_revision=revision,
                    confidence=CERTAIN,
                ))
        # An empty snapshot never enters the loop. Recheck before granting
        # deletion authority, including cancellation/deadline during the read.
        complete = complete and not ctx.cancelled()
        if complete:
            # The snapshot's revision covers the whole membership, so adding,
            # editing or removing any record changes it exactly once.
            collection_revision = _digest(sorted(revisions.items()))
            collection_identity = {"id": COLLECTION_KEY}
            collection_source_revision = {"sha256": collection_revision}
            records[COLLECTION_KEY] = RecordEmission(
                primary=RecordNode(
                    COLLECTION_NODE_ID, "example_collection",
                    collection_identity, collection_source_revision,
                ),
            )
            if previous_collection != collection_revision:
                facts.append(NodeFact(
                    COLLECTION_NODE_ID, "example_collection",
                    {"text": "example record snapshot"},
                    collection_identity, collection_source_revision,
                ))
        ctx.metrics.increment("records.observed", len(records))
        ctx.metrics.increment("records.changed", len(facts))
        return ConnectorRun(
            facts=facts,
            completed_scope=complete,
            next_checkpoint=Checkpoint({
                "revisions": revisions, "collection": collection_revision,
            }) if complete else ctx.checkpoint,
            record_set=RecordSet(records) if complete else None,
            health=ConnectorHealth(status="ok" if complete else "partial",
                                   reason=None if complete else "snapshot incomplete or rejected"),
            run_mode="scope_complete" if complete else "scope_incomplete",
            rejected=tuple(rejected),
        )


class RecordSource(ConnectorAdapter):
    """The normal registry constructor creates this installed public adapter."""

    profile = "discovery_crawl"
    # `okg deployment add-source` refuses a reader that cannot say cheaply
    # whether its input changed. Hashing the one input file is enough here;
    # the emitting functions are hashed too, so a code change also counts.
    change_probe_kind = "content_hash"

    def __init__(self, path: str):
        config = {"path": path}
        Draft202012Validator(CONFIG_SCHEMA).validate(config)
        super().__init__(RecordReader(), config=config)
        self.change_probe = ContentHashProbe(
            content_items=lambda: [(path, Path(path))], config=config,
            emit_targets=(RecordReader.run, _digest),
        )
