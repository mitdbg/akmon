"""PACT chat-upload-source — req.chat-upload-source.published-proof.

FLAGGED FOR THE SERIAL RUN. This is the live half of the PACT: it needs a
real Postgres runtime, it publishes a generation, and it must not share a
database with a parallel worker pool. Select it with::

    uv run --extra dev python -m pytest \
      tests/substrate/library/sources/test_upload_source_publish_live.py \
      -q -m "postgres and serial_db"

What it proves, in the order the requirement states it:

1. An upload arrives through the CHAT AFFORDANCE (`upload_to_staging`,
   the same function the MCP `upload` tool calls), not by constructing
   the adapter with hand-placed files. If the affordance and the source
   disagree about where staging lives, this fails.
2. Ingest and one publish cycle run.
3. The uploaded document's rows are read back through a
   GENERATION-PINNED CHRONOS CHECKOUT over ``okg.graph_nodes`` /
   ``okg.graph_edges``. The legacy ``nodes_live`` / ``edges_live``
   relations are dropped on chronos-native deployments and their bare
   passthroughs read empty, so a zero from them would be a failed read
   path masquerading as an empty upstream — this suite never touches
   them.
4. ``describe_generation`` reports ``published``, not ``blocked``.
5. The delta is attributed BY NODE ID: the document node id is computed
   from the upload's own deterministic identity and asserted present
   under the new generation.

Zero new rows after an upload is a FAILURE here, not an acceptable
outcome, and the two ways of reaching zero are distinguished: the
upstream count (uploads actually staged) is asserted independently of the
graph read, so "nothing was uploaded" cannot be reported as "the upload
published nothing".
"""

from __future__ import annotations

import base64
import json
from pathlib import Path
from typing import Any

import psycopg
import pytest
import yaml

from okg.substrate.exchange.harness import database_doctor, describe_generation
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.ingest import SourceRunner
from okg.substrate.library.sources.upload import (
    UploadSource,
    resolve_upload_registration,
    upload_source_registry_entry,
)
from okg.substrate.mcp import upload_admin
from okg.substrate.publisher import Publisher
from tests.substrate._db_helpers import seed_catalog_version
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402


pytestmark = [
    pytest.mark.postgres,
    pytest.mark.serial_db,
    pytest.mark.slow,
]


DEPLOYMENT = "upload-publish-proof"
OWNERSHIP_ID = f"{DEPLOYMENT}.upload"

_UPLOAD_FILENAME = "beam-calibration.md"
_UPLOAD_BODY = (
    "# Beam calibration record\n"
    "\n"
    "## Summary\n"
    "\n"
    "The calorimeter gain drifted 1.8 percent after the July shutdown.\n"
    "\n"
    "## Action\n"
    "\n"
    "Re-run the calibration sequence before the next fill.\n"
)

_EXTRACTORS = """\
parser:
  - format: markdown
    class: okg.substrate.library.extraction.parse.markdown.MarkdownParser
  - format: text
    class: okg.substrate.library.extraction.parse.text.TextParser
chunker:
  - class: okg.substrate.library.extraction.chunk.window.WindowChunker
    config: {max_chars: 400, overlap: 0}
ner: []
"""

_POSTURE = {
    "deployment_class": "dogfood_workspace",
    "read_principals": ["human_operator", "agent"],
    "write_principals": ["human_operator", "source_adapter"],
    "mcp_write_default": "review",
    "export_policy": "internal_only",
    "secret_handling": "secret_ref_required",
    "live_call_default": "review",
    "allowed_source_sensitivities": ["local_private"],
    "allowed_data_classifications": ["private"],
    "allowed_exportability": ["no_export"],
    "privacy_obligations": ["audit", "no_export"],
    "readiness_profiles": ["dogfood"],
}


def _write_deployment(root: Path) -> Path:
    dep = root / DEPLOYMENT
    dep.mkdir(parents=True)
    (dep / "source_registry.yaml").write_text(
        yaml.safe_dump(
            {
                "sources": {
                    "upload": upload_source_registry_entry(
                        ownership_id=OWNERSHIP_ID,
                    ),
                },
            },
            sort_keys=True,
        )
    )
    (dep / "deployment.yaml").write_text(
        yaml.safe_dump(
            {"name": DEPLOYMENT, "nomos": {"deployment_posture": _POSTURE}},
            sort_keys=True,
        )
    )
    (dep / "extractors.yaml").write_text(_EXTRACTORS)
    return dep


def _counts(store: Any) -> dict[str, int]:
    """Row counts through a chronos checkout — never nodes_live."""
    nodes = store.query("SELECT count(*) AS n FROM okg.graph_nodes")[0]["n"]
    edges = store.query("SELECT count(*) AS n FROM okg.graph_edges")[0]["n"]
    return {"nodes": int(nodes or 0), "edges": int(edges or 0)}


def test_upload_reaches_a_published_generation(
    clean_runtime: str, tmp_path: Path,
) -> None:
    dep_dir = _write_deployment(tmp_path / "deployments")

    def _resolve(name: str) -> Path:
        candidate = tmp_path / "deployments" / name
        if not candidate.is_dir():
            raise FileNotFoundError(name)
        return candidate

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)

    store = ChronosGraphStore.connect(clean_runtime, ensure_schema=False)
    try:
        before = _counts(store)
    finally:
        store.close()

    # 1 — upload through the chat affordance.
    staged_result = upload_admin.upload_to_staging(
        DEPLOYMENT,
        uploaded_by="operator@example.org",
        filename=_UPLOAD_FILENAME,
        content_base64=base64.b64encode(_UPLOAD_BODY.encode("utf-8")).decode(
            "ascii"
        ),
        deployment_dir_resolver=_resolve,
    )
    assert staged_result["status"] == "staged", staged_result

    # The resolver parses the registry itself (through load_source_registry,
    # so placeholders expand exactly as they do for the adapter). Passing a
    # pre-parsed registry would be the second, looser reading it exists to
    # prevent, which is why it takes the PATH.
    manifest = yaml.safe_load((dep_dir / "deployment.yaml").read_text())
    registration = resolve_upload_registration(
        DEPLOYMENT,
        manifest=manifest,
        registry_path=dep_dir / "source_registry.yaml",
    )
    staging = registration.staging()
    # Independent upstream count, taken from the staging area rather than
    # from the graph read below. A zero row delta is only ever an honest
    # empty ingest if THIS number is also zero.
    upstream_uploads = staging.list_uploads()
    assert len(upstream_uploads) == 1, upstream_uploads
    staged = upstream_uploads[0]
    expected_doc_id = f"doc:upload:{staged.staged_name}"

    # 2 — ingest + publish.
    SourceRunner(clean_runtime, source_name=OWNERSHIP_ID).run(
        UploadSource(
            staging_dir=registration.staging_dir,
            extractors_dir=registration.extractors_dir,
            source_name=OWNERSHIP_ID,
        ),
        mode="scope_complete",
    )
    published = Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()
    assert published["status"] == "published", published
    generation_id = published["generation_id"]

    # 4 — generation describe reports published, not blocked.
    described = describe_generation(clean_runtime, generation_id)
    assert described["status"] == "published", described

    # 3 + 5 — generation-pinned chronos read, attributed by node id.
    store = ChronosGraphStore.connect(clean_runtime, ensure_schema=False)
    try:
        generation = store.checkout_generation(generation_id)
        after = _counts(generation)
        document_rows = generation.query(
            "SELECT node_id, subtype, attrs FROM okg.graph_nodes "
            "WHERE node_id = :node_id",
            {"node_id": expected_doc_id},
        )
        chunk_rows = generation.query(
            "SELECT count(*) AS n FROM okg.graph_nodes "
            "WHERE source = :source AND subtype = 'document_chunk'",
            {"source": OWNERSHIP_ID},
        )
        source_node_rows = generation.query(
            "SELECT subtype, count(*)::int AS n FROM okg.graph_nodes "
            "WHERE source = :source GROUP BY subtype ORDER BY subtype",
            {"source": OWNERSHIP_ID},
        )
    finally:
        store.close()

    assert document_rows, (
        f"upload {staged.origin!r} produced no document row under generation "
        f"{generation_id}; {len(upstream_uploads)} upload(s) were staged, so "
        f"this is a zero-output failure, not an empty upstream"
    )
    assert document_rows[0]["subtype"] == "document"
    attrs = document_rows[0]["attrs"] or {}
    assert attrs.get("citation_label") == _UPLOAD_FILENAME
    assert int(chunk_rows[0]["n"]) > 0, (
        "the upload published a document with no chunks, so its content is "
        "not retrievable"
    )
    delta = {
        "nodes": after["nodes"] - before["nodes"],
        "edges": after["edges"] - before["edges"],
    }
    assert delta["nodes"] > 0 and delta["edges"] > 0, delta

    with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM okg.projection_pending")
        projection_queue = int(cur.fetchone()[0])
        cur.execute(
            "SELECT coalesce(sum(dlq_count), 0)::int "
            "FROM okg.v_runtime_queue_health"
        )
        dlq_count = int(cur.fetchone()[0])
    assert projection_queue == 0
    assert dlq_count == 0
    doctor = database_doctor(clean_runtime)
    assert doctor["ok"] is True, doctor

    report = {
        "pact": "chat-upload-source",
        "requirement": "req.chat-upload-source.published-proof",
        "deployment": f"isolated-test:{DEPLOYMENT}",
        "source": OWNERSHIP_ID,
        "classification": registration.declared_policy(),
        "upstream_staged_uploads": len(upstream_uploads),
        "upload_id": staged.upload_id,
        "document_node_id": expected_doc_id,
        "generation_id": generation_id,
        "generation_status": described["status"],
        "read_surface": (
            "generation-pinned chronos checkout over okg.graph_nodes / "
            "okg.graph_edges (never nodes_live)"
        ),
        "before": before,
        "after": after,
        "delta": delta,
        "nodes_by_subtype": {
            str(row["subtype"]): int(row["n"]) for row in source_node_rows
        },
        "projection_queue": projection_queue,
        "dlq_count": dlq_count,
        "doctor": doctor,
    }
    artifact = Path(".scratch/chat-upload-source-publish-proof.json")
    artifact.parent.mkdir(exist_ok=True)
    artifact.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
