"""The record layers against a real published graph (PACT
operator-console-record-layers).

These are the cases only a database can answer: what the raw read serves after
the policy filter, that a denied record and an absent one produce the SAME
BYTES, that a relationship tally counts only what the reader may see, that
chunks come back in the ordinal the source stored, and that retained bytes are
served -- or not -- through the same gate as the record they belong to.

Both payload states are covered. `not_retained` is the state of every default
deployment, because the indexed-v2 source authority that retains a record's own
bytes is opt-in and has no wired producer; the retained state is proved by
seeding that authority's own tables and reading back through the same code
path.
"""
from __future__ import annotations

import hashlib
import json
import uuid
from contextlib import contextmanager
from types import SimpleNamespace
from typing import Any, Iterator

import psycopg
import pytest

pytest.importorskip("fastapi")
from fastapi import APIRouter, FastAPI  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

from okg.substrate.console_views import records  # noqa: E402
from okg.substrate.graph_store import ChronosGraphStore  # noqa: E402
from okg.substrate.library.captioning import crop_store  # noqa: E402
from okg.substrate.library.sources.base import EdgeFact, NodeFact  # noqa: E402
from okg.substrate.nomos import activate_policy_bundle, compile_policy_graph  # noqa: E402
from okg.substrate.nomos.read_masking import MASK_TOKEN  # noqa: E402
from okg.substrate.publisher import Publisher  # noqa: E402
from okg.viz.record_endpoints import register_operator_record_routes  # noqa: E402
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402
from tests.substrate._db_helpers import seed_catalog_version  # noqa: E402

pytestmark = pytest.mark.postgres

DEPLOYMENT = "default"
SUBJECT = "subject:operator"
RESTRICTED = {"classification": "restricted"}
CROP = b"\x89PNG\r\n\x1a\n" + b"fixture crop bytes" * 4
CROP_SHA = hashlib.sha256(CROP).hexdigest()


class _Pool:
    def __init__(self, dsn: str) -> None:
        self._dsn = dsn

    def connection(self) -> psycopg.Connection:
        return psycopg.connect(self._dsn)


class _Ctx:
    def __init__(self, dsn: str, *, branch_name: str = "default") -> None:
        self.dsn = dsn
        self.deployment_name = DEPLOYMENT
        self.session = SimpleNamespace(branch_name=branch_name)
        self.pool = _Pool(dsn)

    @contextmanager
    def conn(self) -> Iterator[psycopg.Connection]:
        with psycopg.connect(self.dsn, autocommit=True) as conn:
            yield conn


def _client(dsn: str) -> TestClient:
    app = FastAPI()
    router = APIRouter(prefix="/viz/api")
    ctx = _Ctx(dsn)
    register_operator_record_routes(router, ctx_provider=lambda: ctx)
    app.include_router(router)
    return TestClient(app)


def _node(
    node_id: str, subtype: str, record_id: Any = None, **attrs: Any,
) -> NodeFact:
    """A fixture node. ``record_id`` overrides the source-record identity.

    The default builds every node's identity from its node id, which is never
    path-shaped -- so an assertion that no read publishes a path could not see
    an unmasked source_record_id at all. Nodes that carry a path-valued
    identity pass it here, in the flat and the nested object form.
    """
    return NodeFact(
        node_id=node_id,
        subtype=subtype,
        attrs=attrs,
        source_record_id={"id": node_id} if record_id is None else record_id,
        source_revision={"v": 1},
    )


def _publish(dsn: str, nodes: list[NodeFact], edges: list[EdgeFact] | None = None) -> None:
    with psycopg.connect(dsn, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts([*nodes, *(edges or [])], source="fixture")
    result = Publisher(
        dsn, DEPLOYMENT, catalog_identity=bind_test_catalog_writer(dsn, DEPLOYMENT),
    ).cycle()
    assert result["status"] == "published"


def _rule(rule_id: str, *, match: dict[str, Any]) -> dict[str, Any]:
    return {
        "rule_id": rule_id,
        "effect": "allow",
        "actions": ["graph.read.local_operator"],
        "subjects": [SUBJECT],
        "match": match,
        "priority": 100,
        "reason_code": f"nomos.test.{rule_id}",
    }


BOUNDARY_RULE = _rule("allow_ui_boundary", match={"resource_kind": "local_operator_boundary"})
ROW_RULE = _rule("allow_ui_rows", match={})
DENY_RESTRICTED_RULE = {
    **_rule("deny_restricted", match={"classification": "restricted"}),
    "effect": "deny",
    "priority": 200,
}


def _activate(dsn: str, rules: list[dict[str, Any]]) -> None:
    bundle = compile_policy_graph({
        "policy_bundle_id": "nomos:test:operator-console-record-layers",
        "policy_version": "v1",
        "default_effect": "deny",
        "rules": rules,
    })
    with psycopg.connect(dsn) as conn:
        activate_policy_bundle(
            conn,
            deployment_name=DEPLOYMENT,
            bundle=bundle,
            scope=f"deployment:{DEPLOYMENT}",
            activated_by="test:operator",
        )
        conn.commit()


def _enforce(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", SUBJECT)


@pytest.fixture
def layers(clean_runtime: str) -> str:
    """One version holding a document with chunks, a figure, and hidden rows."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)
        crop_store.ensure_table(conn)
        crop_store.put_crop(conn, sha256=CROP_SHA, data=CROP, mime_type="image/png")
    _publish(
        clean_runtime,
        [
            _node(
                "doc:guide", "document",
                title="Guide", path="/Users/example/corpus/guide.md",
                format="markdown", byte_count=120, content_sha256="a" * 64,
            ),
            _node("chunk:2", "document_chunk", text="second passage", page_number=2),
            _node("chunk:0", "document_chunk", text="first passage", page_number=1),
            _node("chunk:1", "document_chunk", text="middle passage"),
            _node("chunk:hidden", "document_chunk", text="withheld", **RESTRICTED),
            _node("chunk:unordered", "document_chunk", text="no ordinal stored"),
            # Numbers that are not non-negative integers. SQL used to order by
            # them -- a negative sorts FIRST and ate a slot at the head of the
            # page -- and then Python dropped them and the count missed them.
            _node("chunk:negative", "document_chunk", text="negative ordinal"),
            _node("chunk:fractional", "document_chunk", text="fractional ordinal"),
            # A document whose chunks record WHERE they start but carry no
            # ordinal on their membership edge. This is the shape issue #2178
            # found on the Lane D graph, and the offsets are stored BOTH as
            # JSON numbers and as digit strings, which is what real chunkers
            # write (tests/substrate/db/test_canonical_read_path.py).
            _node("doc:offsets", "document", title="Offsets only", format="markdown"),
            _node("chunk:off:tail", "document_chunk", text="offset tail",
                  char_offset="120"),
            _node("chunk:off:mid", "document_chunk", text="offset mid",
                  char_offset=40),
            _node("chunk:off:dup", "document_chunk", text="offset dup",
                  char_offset="40"),
            _node("chunk:off:head", "document_chunk", text="offset head",
                  char_offset=0, page_number=1),
            # A document that reaches its chunks the OTHER way: an outgoing
            # `contains` edge, document to chunk, which is what the twiki-eos
            # reader records and what cern-r3final is built of. The read
            # matched only incoming `member_of`, so on that graph a document
            # had no chunks at all.
            _node("doc:contains", "document", title="Contains only", format="markdown"),
            _node("chunk:con:0", "document_chunk", text="contains first"),
            _node("chunk:con:1", "document_chunk", text="contains second"),
            # And one recorded BOTH ways, which must be listed once.
            _node("doc:bothways", "document", title="Both ways", format="markdown"),
            _node("chunk:bw:0", "document_chunk", text="both ways first"),
            # Both bases recorded, and they DISAGREE: the stored ordinal wins,
            # so a fallback can never quietly reorder a document that already
            # says what its order is.
            _node("doc:both", "document", title="Both bases", format="markdown"),
            _node("chunk:both:0", "document_chunk", text="both first",
                  char_offset=900),
            _node("chunk:both:1", "document_chunk", text="both second",
                  char_offset=10),
            # Neither basis covers EVERY chunk: one offset, one nothing. No
            # order covers the set, so the read refuses by name rather than
            # listing half a document in a convincing order.
            _node("doc:partial", "document", title="Partial", format="markdown"),
            _node("chunk:part:off", "document_chunk", text="part offset",
                  char_offset=5),
            _node("chunk:part:none", "document_chunk", text="part nothing"),
            # A document and its chunks as a real reader records them: the
            # SOURCE's own identity -- the file and the position in it --
            # which is what a related row has to be titled by. A chunk's own
            # label is its hash, so a document's Related tab listed its chunks
            # as "Document Chunk - 5fe6bef91614aa4d" and named nothing.
            # `doc:rel` reaches its chunks the `contains` way.
            _node(
                "doc:rel", "document",
                record_id={"path": "HcalIsoTrackValidationSim.txt"},
                title="Hcal validation", format="markdown",
            ),
            _node(
                "chunk:rel:0", "document_chunk",
                record_id={"path": "HcalIsoTrackValidationSim.txt", "chunk_index": 0},
                text="rel first passage",
            ),
            # ...and one whose source recorded a heading as well.
            _node(
                "chunk:rel:1", "document_chunk",
                record_id={
                    "path": "HcalIsoTrackValidationSim.txt",
                    "chunk_index": 1,
                    "heading": "Setup",
                },
                text="rel second passage",
            ),
            # A neighbour whose source recorded NO identity. `source_record_id`
            # is JSONB NOT NULL DEFAULT '{}', so "records none" is the empty
            # object, and such a row must carry nothing rather than an empty
            # container a client would have to interpret.
            _node("rel:anon", "release", record_id={}, name="Anonymous"),
            # The other edge shape: a chunk that is `member_of` its document.
            _node(
                "doc:rel2", "document",
                record_id={"path": "Reco.txt"}, title="Reco", format="markdown",
            ),
            _node(
                "chunk:rel2:0", "document_chunk",
                record_id={"path": "Reco.txt", "chunk_index": 0},
                text="reco first passage",
            ),
            _node(
                "figure:1", "visual_artifact",
                crop_blob_ref=CROP_SHA, format="pdf_figure",
                source_pdf_path="/Users/example/papers/inbox/attention.pdf",
            ),
            _node(
                "figure:denied", "visual_artifact",
                crop_blob_ref=CROP_SHA, format="pdf_figure", **RESTRICTED,
            ),
            _node("paper:1", "paper", title="A paper",
                  source_pdf_path="/Users/example/papers/inbox/attention.pdf"),
            _node("rel:visible", "release", name="Visible"),
            _node("rel:hidden", "release", name="Hidden", **RESTRICTED),
            _node("doc:masked", "document", title="Masked", owner=MASK_TOKEN),
            # A record whose TITLE is the path the source read. Its label is
            # therefore a path, and the label is the one field search, listing
            # and related rows all carry -- so without masking it leaks on
            # reads that never return attrs at all.
            # A path-valued source_record_id, FLAT form, on a record every
            # read can reach: list, search (text and exact hit), detail, raw
            # and the raw edge rows.
            #
            # The bare-STRING form is not here because it cannot be published:
            # ChronosGraphStore does `dict(fact.source_record_id)`
            # (graph_store_internal/writes.py:173), so the writer refuses a
            # string identity outright. `record_identity` still passes a string
            # through, so `mask_paths` handles that shape, and the unit tests
            # cover it -- but no ingest can produce one.
            _node(
                "doc:strid", "document",
                record_id={"path": "/Users/example/corpus/strid.md"},
                title="String identity", text="strid notes",
            ),
            # ...and in the NESTED OBJECT form, which is what doc_corpus
            # actually writes.
            _node(
                "doc:objid", "document",
                record_id={"repo": "corpus", "path": "/Users/example/corpus/objid.md"},
                title="Object identity", text="objid notes",
            ),
            _node(
                "doc:titled", "document",
                title="/Users/example/corpus/titled.md",
                # Deliberately free of the words the assertion forbids: the
                # point is whether the PATH leaks, not whether a body happens
                # to contain the same word as a directory.
                text="titled notes",
            ),
        ],
        [
            EdgeFact(src="chunk:0", dst="doc:guide", edge_type="member_of",
                     attrs={"chunk_ordinal": 0, "heading_path": ["Intro"]}),
            EdgeFact(src="chunk:1", dst="doc:guide", edge_type="member_of",
                     attrs={"chunk_ordinal": 1, "heading_path": ["Design", "Reads"]}),
            EdgeFact(src="chunk:2", dst="doc:guide", edge_type="member_of",
                     attrs={"chunk_ordinal": 2}),
            EdgeFact(src="chunk:hidden", dst="doc:guide", edge_type="member_of",
                     attrs={"chunk_ordinal": 3}),
            # No chunk_ordinal: nothing retained to order it by.
            EdgeFact(src="chunk:unordered", dst="doc:guide", edge_type="member_of",
                     attrs={}),
            EdgeFact(src="chunk:negative", dst="doc:guide", edge_type="member_of",
                     attrs={"chunk_ordinal": -1}),
            EdgeFact(src="chunk:fractional", dst="doc:guide", edge_type="member_of",
                     attrs={"chunk_ordinal": 1.5}),
            # An EDGE whose own source-record identity is a path: the raw
            # read emits one per edge, and it went out unmasked too.
            EdgeFact(
                src="doc:guide", dst="doc:strid", edge_type="cites", attrs={},
                source_record_id={"path": "/Users/example/corpus/edge.md"},
            ),
            EdgeFact(src="doc:guide", dst="rel:visible", edge_type="cites"),
            EdgeFact(src="doc:guide", dst="rel:hidden", edge_type="cites"),
            EdgeFact(src="doc:guide", dst="figure:1", edge_type="contains"),
            # doc:offsets -- no ordinal anywhere, an offset on every chunk.
            EdgeFact(src="chunk:off:tail", dst="doc:offsets",
                     edge_type="member_of", attrs={}),
            EdgeFact(src="chunk:off:mid", dst="doc:offsets",
                     edge_type="member_of", attrs={}),
            EdgeFact(src="chunk:off:dup", dst="doc:offsets",
                     edge_type="member_of", attrs={}),
            EdgeFact(src="chunk:off:head", dst="doc:offsets",
                     edge_type="member_of", attrs={"heading_path": ["Top"]}),
            # doc:both -- the ordinal contradicts the offset order.
            EdgeFact(src="chunk:both:0", dst="doc:both",
                     edge_type="member_of", attrs={"chunk_ordinal": 0}),
            EdgeFact(src="chunk:both:1", dst="doc:both",
                     edge_type="member_of", attrs={"chunk_ordinal": 1}),
            # doc:contains -- outgoing, document to chunk.
            EdgeFact(src="doc:contains", dst="chunk:con:0",
                     edge_type="contains", attrs={"chunk_ordinal": 0}),
            EdgeFact(src="doc:contains", dst="chunk:con:1",
                     edge_type="contains", attrs={"chunk_ordinal": 1}),
            # doc:bothways -- the same pair recorded in both directions.
            EdgeFact(src="doc:bothways", dst="chunk:bw:0",
                     edge_type="contains", attrs={"chunk_ordinal": 7}),
            EdgeFact(src="chunk:bw:0", dst="doc:bothways",
                     edge_type="member_of", attrs={"chunk_ordinal": 0}),
            # doc:partial -- one chunk has an offset, the other has neither.
            EdgeFact(src="chunk:part:off", dst="doc:partial",
                     edge_type="member_of", attrs={}),
            EdgeFact(src="chunk:part:none", dst="doc:partial",
                     edge_type="member_of", attrs={}),
            # doc:rel -- chunks reached the `contains` way, each recording the
            # file it came out of and where in it, plus one neighbour that
            # records no identity at all.
            EdgeFact(src="doc:rel", dst="chunk:rel:0",
                     edge_type="contains", attrs={"chunk_ordinal": 0}),
            EdgeFact(src="doc:rel", dst="chunk:rel:1",
                     edge_type="contains", attrs={"chunk_ordinal": 1}),
            EdgeFact(src="doc:rel", dst="rel:anon", edge_type="cites"),
            # doc:rel2 -- reached the `member_of` way, from the chunk.
            EdgeFact(src="chunk:rel2:0", dst="doc:rel2",
                     edge_type="member_of", attrs={"chunk_ordinal": 0}),
        ],
    )
    with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT id FROM okg.v_generations WHERE deployment_name = %s "
            "AND status = 'published' ORDER BY published_at DESC LIMIT 1",
            (DEPLOYMENT,),
        )
        return str(cur.fetchone()[0])


def _get(dsn: str, path: str, version: str, record_id: str):
    return _client(dsn).get(path, params={"version": version, "id": record_id})


# --- the raw read -----------------------------------------------------------


def test_raw_serves_the_stored_row_and_its_edges(
    layers: str, clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _activate(clean_runtime, [BOUNDARY_RULE, ROW_RULE])
    _enforce(monkeypatch)

    body = _get(
        clean_runtime, "/viz/api/console/records/raw", layers, "doc:guide",
    ).json()["payload"]

    assert body["row"]["node_id"] == "doc:guide"
    assert body["row"]["subtype"] == "document"
    assert body["row"]["attrs"]["title"] == "Guide"
    assert body["edge_cap"] == records.RAW_EDGE_CAP
    assert body["edge_total"] >= 1
    relationships = {edge["relationship"] for edge in body["edges"]}
    assert "member_of" in relationships
    for edge in body["edges"]:
        assert {"src", "dst", "relationship"} <= set(edge)


def test_raw_reports_no_retained_payload_and_says_why(
    layers: str, clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The state of every default deployment, named rather than left blank."""
    _activate(clean_runtime, [BOUNDARY_RULE, ROW_RULE])
    _enforce(monkeypatch)

    body = _get(
        clean_runtime, "/viz/api/console/records/raw", layers, "doc:guide",
    ).json()["payload"]

    assert body["payload"] is None
    assert body["refusal"]["reason"] == "not_retained"
    assert "store records individually" in body["refusal"]["detail"]


def test_a_deployment_without_the_authority_tables_refuses_by_name(
    layers: str, clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No `source_record_authority_refs`, no 500.

    okg-workspace has no authority tables: this read named one, psycopg raised
    `UndefinedTable`, and the whole read answered 500 `record_raw_failed` --
    a database error where a named refusal was already the right answer. The
    table is dropped here rather than mocked, because the failure was the
    database's and a mock of it would prove nothing.
    """
    _activate(clean_runtime, [BOUNDARY_RULE, ROW_RULE])
    _enforce(monkeypatch)
    # Renamed, not dropped, and put back in a finally: this fixture's database
    # is shared with every other case in this file, and a CASCADE drop took
    # the generation view with it and made the next test read a 404.
    def rename(from_: str, to: str) -> None:
        with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
            cur.execute(f"ALTER TABLE okg.{from_} RENAME TO {to}")
            conn.commit()

    rename("source_record_authority_refs", "authority_refs__absent_for_test")
    try:
        response = _get(
            clean_runtime, "/viz/api/console/records/raw", layers, "doc:guide",
        )
    finally:
        rename("authority_refs__absent_for_test", "source_record_authority_refs")

    assert response.status_code == 200
    body = response.json()["payload"]
    assert body["payload"] is None
    assert body["refusal"]["reason"] == "not_retained"
    assert body["refusal"]["detail"] == (
        "this deployment records no source-record authority "
        "(table not present; migrate to enable)"
    )


def test_raw_never_answers_with_neither_a_payload_nor_a_refusal(
    layers: str, clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The console throws on that shape, so the server must never send it."""
    _activate(clean_runtime, [BOUNDARY_RULE, ROW_RULE])
    _enforce(monkeypatch)

    for record_id in ("doc:guide", "figure:1", "paper:1", "nope:1"):
        body = _get(
            clean_runtime, "/viz/api/console/records/raw", layers, record_id,
        ).json()["payload"]
        assert (body["payload"] is None) != (body["refusal"] is None), record_id


def test_raw_serves_a_retained_payload_when_an_authority_holds_one(
    layers: str, clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Seeded through the authority's own tables: no producer wires it yet.

    The indexed-v2 record authority is opt-in and its runner refuses to write
    one without a consumer, so the retained state is proved against the schema
    that stores it rather than through an ingest that cannot reach it.
    """
    _activate(clean_runtime, [BOUNDARY_RULE, ROW_RULE])
    _enforce(monkeypatch)
    payload = json.dumps({"title": "Guide", "body": "the source's own record"})
    _seed_authority(clean_runtime, record_id={"id": "doc:guide"}, payload=payload)

    body = _get(
        clean_runtime, "/viz/api/console/records/raw", layers, "doc:guide",
    ).json()["payload"]

    assert body["refusal"] is None
    assert body["payload"]["byte_count"] == len(payload.encode("utf-8"))
    assert body["payload"]["inline"] == payload
    assert body["payload"]["sha256"] == hashlib.sha256(
        payload.encode("utf-8"),
    ).hexdigest()
    # Addressed by record, never by bare digest.
    assert "id=doc:guide" in body["payload"]["url"]
    assert "sha256=" not in body["payload"]["url"]


def test_a_retained_payload_is_refused_for_a_record_the_policy_redacted(
    layers: str, clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The payload IS the values the redaction removed, so it is not fetched."""
    _activate(clean_runtime, [BOUNDARY_RULE, ROW_RULE])
    _enforce(monkeypatch)
    _seed_authority(
        clean_runtime, record_id={"id": "doc:masked"}, payload='{"owner":"real"}',
    )

    body = _get(
        clean_runtime, "/viz/api/console/records/raw", layers, "doc:masked",
    ).json()["payload"]

    assert body["payload"] is None
    assert body["refusal"]["reason"] == "policy_refused"
    assert "real" not in json.dumps(body)


def test_a_payload_bound_after_the_version_is_refused_by_name(
    layers: str, clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The binding is keyed by source alone, so it can be a LATER ingest's.

    Answering with it would show the reader one generation's bytes under
    another generation's heading and call it the record.
    """
    _activate(clean_runtime, [BOUNDARY_RULE, ROW_RULE])
    _enforce(monkeypatch)
    _seed_authority(
        clean_runtime,
        record_id={"id": "doc:guide"},
        payload='{"title": "a later ingest"}',
        bound_before_publish=False,
    )

    body = _get(
        clean_runtime, "/viz/api/console/records/raw", layers, "doc:guide",
    ).json()["payload"]

    assert body["payload"] is None
    assert body["refusal"]["reason"] == "payload_not_pinned_to_version"
    assert "later ingest" not in json.dumps(body)


def test_a_component_with_several_fields_is_refused_not_guessed(
    layers: str, clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Serving the alphabetically first field would call one field the record."""
    _activate(clean_runtime, [BOUNDARY_RULE, ROW_RULE])
    _enforce(monkeypatch)
    _seed_authority(
        clean_runtime,
        record_id={"id": "doc:guide"},
        payload='{"title": "Guide"}',
        extra_field="body",
    )

    body = _get(
        clean_runtime, "/viz/api/console/records/raw", layers, "doc:guide",
    ).json()["payload"]

    assert body["payload"] is None
    assert body["refusal"]["reason"] == "payload_has_several_fields"
    assert "body" in body["refusal"]["detail"]
    assert "record" in body["refusal"]["detail"]


def test_a_payload_that_is_not_json_is_not_served_as_json(
    layers: str, clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The column says json; bytes that are not get a named refusal."""
    _activate(clean_runtime, [BOUNDARY_RULE, ROW_RULE])
    _enforce(monkeypatch)
    _seed_authority(
        clean_runtime,
        record_id={"id": "doc:guide"},
        payload="this is not json at all",
    )

    body = _get(
        clean_runtime, "/viz/api/console/records/raw", layers, "doc:guide",
    ).json()["payload"]
    served = _get(
        clean_runtime, "/viz/api/console/records/payload", layers, "doc:guide",
    )

    assert body["payload"] is None
    assert body["refusal"]["reason"] == "not_retained"
    assert "not valid UTF-8 JSON" in body["refusal"]["detail"]
    assert served.status_code == 404


def test_a_payload_over_the_inline_limit_is_served_as_head_and_counts(
    layers: str, clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Never a truncated body presented as the whole one."""
    _activate(clean_runtime, [BOUNDARY_RULE, ROW_RULE])
    _enforce(monkeypatch)
    lines = 400
    filler = "y" * 800
    big = json.dumps([f"{index}-{filler}" for index in range(lines)], indent=1)
    assert len(big.encode("utf-8")) > records.RAW_INLINE_PAYLOAD_MAX_BYTES
    _seed_authority(
        clean_runtime, record_id={"id": "doc:guide"}, payload=big,
    )

    body = _get(
        clean_runtime, "/viz/api/console/records/raw", layers, "doc:guide",
    ).json()["payload"]

    assert body["refusal"] is None
    assert body["payload"]["inline"] is None
    assert body["payload"]["byte_count"] == len(big.encode("utf-8"))
    assert body["payload"]["line_count"] == len(big.splitlines())
    assert body["payload"]["head"].count("\n") == records.RAW_HEAD_LINES - 1
    assert len(body["payload"]["head"]) < len(big)


def test_a_payload_larger_than_its_recorded_size_is_refused(
    layers: str, clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The chunk rows are the authority on size; the column is a claim."""
    _activate(clean_runtime, [BOUNDARY_RULE, ROW_RULE])
    _enforce(monkeypatch)
    monkeypatch.setattr(records, "RAW_PAYLOAD_MAX_BYTES", 64)
    _seed_authority(
        clean_runtime,
        record_id={"id": "doc:guide"},
        payload=json.dumps({"filler": "z" * 500}),
        declared_bytes=8,
    )

    body = _get(
        clean_runtime, "/viz/api/console/records/raw", layers, "doc:guide",
    ).json()["payload"]

    assert body["payload"] is None
    assert body["refusal"]["reason"] == "not_retained"
    assert "larger than its recorded" in body["refusal"]["detail"]


def test_a_record_with_more_edges_than_the_cap_reports_a_capped_page(
    layers: str, clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _activate(clean_runtime, [BOUNDARY_RULE, ROW_RULE])
    _enforce(monkeypatch)
    monkeypatch.setattr(records, "RAW_EDGE_CAP", 2)

    body = _get(
        clean_runtime, "/viz/api/console/records/raw", layers, "doc:guide",
    ).json()["payload"]

    assert body["edge_cap"] == 2
    assert len(body["edges"]) <= 2
    # The read fetches one row past the cap to learn the page was cut. That
    # probe is not a count: serving it said "2 of 3 shown" for a record with
    # far more edges (Archiv3: "100 of 101" on a person with 313).
    assert body["edges_truncated"] is True
    assert body["edge_total"] is None
    assert body["edge_total_unavailable_reason"]


def test_a_crop_whose_bytes_are_not_the_declared_type_is_not_served(
    layers: str, clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A row says png; the bytes are a script-bearing document."""
    _activate(clean_runtime, [BOUNDARY_RULE, ROW_RULE])
    _enforce(monkeypatch)
    with psycopg.connect(clean_runtime, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "UPDATE okg.visual_crop_blobs SET bytes = %s WHERE sha256 = %s",
            (b"<html><script>alert(1)</script></html>", CROP_SHA),
        )

    served = _get(
        clean_runtime, "/viz/api/console/records/artifact/bytes", layers, "figure:1",
    )

    assert served.status_code == 404
    assert b"<script>" not in served.content


def test_a_payload_past_the_ceiling_is_refused_with_its_size(
    layers: str, clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _activate(clean_runtime, [BOUNDARY_RULE, ROW_RULE])
    _enforce(monkeypatch)
    oversized = records.RAW_PAYLOAD_MAX_BYTES + 1
    _seed_authority(
        clean_runtime,
        record_id={"id": "doc:guide"},
        payload="x",
        declared_bytes=oversized,
    )

    body = _get(
        clean_runtime, "/viz/api/console/records/raw", layers, "doc:guide",
    ).json()["payload"]

    assert body["payload"] is None
    assert body["refusal"]["reason"] == "not_retained"
    assert str(oversized) in body["refusal"]["detail"]
    assert str(records.RAW_PAYLOAD_MAX_BYTES) in body["refusal"]["detail"]


# --- denied is absent -------------------------------------------------------


@pytest.mark.parametrize(
    "path",
    ["/viz/api/console/records/raw", "/viz/api/console/records/artifact"],
)
def test_a_denied_record_and_an_absent_one_are_the_same_bytes(
    layers: str, clean_runtime: str, monkeypatch: pytest.MonkeyPatch, path: str,
) -> None:
    _activate(clean_runtime, [BOUNDARY_RULE, ROW_RULE, DENY_RESTRICTED_RULE])
    _enforce(monkeypatch)

    denied = _get(clean_runtime, path, layers, "figure:denied")
    absent = _get(clean_runtime, path, layers, "no:such:record")

    assert denied.status_code == absent.status_code
    assert denied.json() == absent.json()


def test_denied_bytes_and_absent_bytes_are_the_same_refusal(
    layers: str, clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _activate(clean_runtime, [BOUNDARY_RULE, ROW_RULE, DENY_RESTRICTED_RULE])
    _enforce(monkeypatch)

    denied = _get(
        clean_runtime, "/viz/api/console/records/artifact/bytes", layers,
        "figure:denied",
    )
    absent = _get(
        clean_runtime, "/viz/api/console/records/artifact/bytes", layers,
        "no:such:record",
    )

    assert denied.status_code == 404
    assert denied.json() == absent.json()


# --- the artifact read and its bytes ----------------------------------------


def test_a_retained_figure_reports_its_bytes_and_serves_them_inert(
    layers: str, clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _activate(clean_runtime, [BOUNDARY_RULE, ROW_RULE])
    _enforce(monkeypatch)

    described = _get(
        clean_runtime, "/viz/api/console/records/artifact", layers, "figure:1",
    ).json()["payload"]

    assert described["exists"] is True
    assert described["media_type"] == "image/png"
    assert described["byte_count"] == len(CROP)
    assert described["sha256"] == CROP_SHA
    # The recorded path is present as a BASENAME and nothing more.
    assert described["path"] == "attention.pdf"

    served = _get(
        clean_runtime, "/viz/api/console/records/artifact/bytes", layers, "figure:1",
    )

    assert served.status_code == 200
    assert served.content == CROP
    assert served.headers["content-type"].startswith("image/png")
    assert served.headers["x-content-type-options"] == "nosniff"
    assert served.headers["content-security-policy"] == "default-src 'none'; sandbox"
    assert served.headers["cross-origin-resource-policy"] == "same-origin"
    assert served.headers["content-disposition"] == "inline"


def test_a_record_with_only_a_recorded_path_reports_it_as_not_retained(
    layers: str, clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """And no absolute path or account name reaches the response."""
    _activate(clean_runtime, [BOUNDARY_RULE, ROW_RULE])
    _enforce(monkeypatch)

    response = _get(
        clean_runtime, "/viz/api/console/records/artifact", layers, "paper:1",
    )
    described = response.json()["payload"]

    assert described["exists"] is False
    assert described["reason"] == "not_retained"
    assert described["path"] == "attention.pdf"
    assert "/Users" not in response.text
    assert "example" not in response.text
    assert "inbox" not in response.text


def test_a_named_crop_that_is_gone_is_reported_missing_not_invented(
    layers: str, clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _activate(clean_runtime, [BOUNDARY_RULE, ROW_RULE])
    _enforce(monkeypatch)
    with psycopg.connect(clean_runtime, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute("DELETE FROM okg.visual_crop_blobs WHERE sha256 = %s", (CROP_SHA,))

    described = _get(
        clean_runtime, "/viz/api/console/records/artifact", layers, "figure:1",
    ).json()["payload"]

    assert described["exists"] is False
    assert described["reason"] == "file_missing"


def test_a_crop_whose_stored_type_is_not_servable_is_refused(
    layers: str, clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """SVG is an image to a reader and a scriptable document to a browser."""
    _activate(clean_runtime, [BOUNDARY_RULE, ROW_RULE])
    _enforce(monkeypatch)
    with psycopg.connect(clean_runtime, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "UPDATE okg.visual_crop_blobs SET mime_type = %s WHERE sha256 = %s",
            ("image/svg+xml", CROP_SHA),
        )

    described = _get(
        clean_runtime, "/viz/api/console/records/artifact", layers, "figure:1",
    ).json()["payload"]
    served = _get(
        clean_runtime, "/viz/api/console/records/artifact/bytes", layers, "figure:1",
    )

    assert described["exists"] is False
    assert described["reason"] == "not_retained"
    assert "image/svg+xml" in described["detail"]
    assert served.status_code == 404


# --- the detail read's new keys ---------------------------------------------


def _cites_out(related: dict[str, Any]) -> int:
    rows = [
        row for row in related["by_relationship"]
        if row["relationship"] == "cites" and row["direction"] == "outgoing"
    ]
    return rows[0]["count"] if rows else 0


def test_relationship_totals_count_both_neighbours_when_both_are_visible(
    layers: str, clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _activate(clean_runtime, [BOUNDARY_RULE, ROW_RULE])
    _enforce(monkeypatch)

    related = _get(
        clean_runtime, "/viz/api/console/records/detail", layers, "doc:guide",
    ).json()["payload"]["related"]

    # doc:guide cites rel:visible, rel:hidden and doc:strid.
    assert _cites_out(related) == 3
    assert related["outgoing_total"] is not None
    assert related["outgoing_total_capped"] is False
    assert related["tally_ceiling"] == records.RELATED_TALLY_CEILING


def test_a_hidden_neighbour_leaves_the_relationship_tally(
    layers: str, clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The tally must not tell the reader how many neighbours it hid.

    Activated in a separate test rather than re-activated inside one: a policy
    activation is itself a gated surface, so a second one after enforcement is
    on is denied `nomos.default_deny`.
    """
    _activate(clean_runtime, [BOUNDARY_RULE, ROW_RULE, DENY_RESTRICTED_RULE])
    _enforce(monkeypatch)

    related = _get(
        clean_runtime, "/viz/api/console/records/detail", layers, "doc:guide",
    ).json()["payload"]["related"]

    # rel:hidden drops out; rel:visible and doc:strid remain.
    assert _cites_out(related) == 2
    assert "rel:hidden" not in json.dumps(related)


def test_a_total_past_the_ceiling_is_null_and_says_so(
    layers: str, clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A guessed total is worse than an absent one."""
    _activate(clean_runtime, [BOUNDARY_RULE, ROW_RULE])
    _enforce(monkeypatch)
    monkeypatch.setattr(records, "RELATED_TALLY_CEILING", 1)

    related = _get(
        clean_runtime, "/viz/api/console/records/detail", layers, "doc:guide",
    ).json()["payload"]["related"]

    assert related["outgoing_total"] is None
    assert related["outgoing_total_capped"] is True
    assert related["by_relationship"] == []


def _related_row(related: dict[str, Any], direction: str, node_id: str) -> dict[str, Any]:
    return next(row for row in related[direction] if row["node_id"] == node_id)


def test_a_documents_related_chunks_carry_the_identity_their_source_recorded(
    layers: str, clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A related row serves the SAME identity a search hit serves.

    Without it the Related tab of a document lists its chunks by the only
    thing the row carried -- the chunk's own label, which is its hash -- so
    the operator reads "Document Chunk - 5fe6bef91614aa4d" twice and cannot
    tell which part of the file each row is. The identity is what the client
    titles the row by, and it comes off the neighbour row the existing join
    already selects: no second statement, no per-neighbour read.
    """
    _activate(clean_runtime, [BOUNDARY_RULE, ROW_RULE])
    _enforce(monkeypatch)

    related = _get(
        clean_runtime, "/viz/api/console/records/detail", layers, "doc:rel",
    ).json()["payload"]["related"]

    first = _related_row(related, "outgoing", "chunk:rel:0")
    assert first["relationship"] == "contains"
    assert first["source_record_id"] == {
        "path": "HcalIsoTrackValidationSim.txt", "chunk_index": 0,
    }
    # The heading the source recorded travels too: it is the other thing a
    # row can be named by when the file is not name enough.
    assert _related_row(related, "outgoing", "chunk:rel:1")["source_record_id"] == {
        "path": "HcalIsoTrackValidationSim.txt", "chunk_index": 1, "heading": "Setup",
    }


def test_a_related_neighbour_that_records_no_identity_carries_no_key(
    layers: str, clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Nothing, not an empty object: `{}` is a value-shaped thing a client
    would have to interpret, and the record reads normalize it to absent
    everywhere else."""
    _activate(clean_runtime, [BOUNDARY_RULE, ROW_RULE])
    _enforce(monkeypatch)

    related = _get(
        clean_runtime, "/viz/api/console/records/detail", layers, "doc:rel",
    ).json()["payload"]["related"]

    assert "source_record_id" not in _related_row(related, "outgoing", "rel:anon")


def test_a_chunks_related_document_carries_its_recorded_path(
    layers: str, clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The other direction and the other edge shape: read FROM the chunk, the
    one row is the document it is `member_of`, and it names the file."""
    _activate(clean_runtime, [BOUNDARY_RULE, ROW_RULE])
    _enforce(monkeypatch)

    related = _get(
        clean_runtime, "/viz/api/console/records/detail", layers, "chunk:rel2:0",
    ).json()["payload"]["related"]

    row = _related_row(related, "outgoing", "doc:rel2")
    assert row["relationship"] == "member_of"
    assert row["source_record_id"] == {"path": "Reco.txt"}


def test_a_related_neighbours_absolute_path_is_masked_and_named(
    layers: str, clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The identity is the fifth place a stored path can leave the server.

    doc:guide cites doc:strid, whose source recorded
    `/Users/<name>/corpus/strid.md`. Serving it on the related row raw would
    put the operator's home directory on the Related tab, and the detail
    read's own `masked_paths` must name the key it rewrote rather than
    quietly rewriting it.
    """
    _activate(clean_runtime, [BOUNDARY_RULE, ROW_RULE])
    _enforce(monkeypatch)

    body = _get(
        clean_runtime, "/viz/api/console/records/detail", layers, "doc:guide",
    ).json()["payload"]

    row = _related_row(body["related"], "outgoing", "doc:strid")
    assert row["source_record_id"] == {"path": "strid.md"}
    assert any(
        key.startswith("related.outgoing[") and key.endswith(".source_record_id.path")
        for key in body["masked_paths"]
    ), body["masked_paths"]


def test_chunks_come_back_in_the_ordinal_the_source_stored(
    layers: str, clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _activate(clean_runtime, [BOUNDARY_RULE, ROW_RULE])
    _enforce(monkeypatch)

    body = _get(
        clean_runtime, "/viz/api/console/records/detail", layers, "doc:guide",
    ).json()["payload"]

    # No deny rule is active here, so the restricted chunk (ordinal 3) is a
    # visible chunk like any other: what this case proves is the ORDER, which
    # is the stored ordinal and not the order the join returned them in.
    assert [chunk["ordinal"] for chunk in body["chunks"]] == [0, 1, 2, 3]
    assert [chunk["node_id"] for chunk in body["chunks"]] == [
        "chunk:0", "chunk:1", "chunk:2", "chunk:hidden",
    ]
    assert body["chunks"][0]["text"] == "first passage"
    assert body["chunks"][0]["page"] == 1
    assert body["chunks"][1]["heading_path"] == "Design / Reads"
    assert body["chunks"][2]["page"] == 2
    assert body["chunk_cap"] == records.CHUNK_CAP
    assert body["chunks_capped"] is False
    assert body["chunk_order_basis"] == "chunk_ordinal"
    # The chunk whose edge stored no ordinal is left out rather than given its
    # position in this list.
    assert "chunk:unordered" not in [chunk["node_id"] for chunk in body["chunks"]]


def test_a_document_that_contains_its_chunks_lists_them(
    layers: str, clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The other edge shape: document -contains-> chunk.

    Before this, the chunk query matched `e.dst = :id AND edge_type =
    'member_of'` only, so this document answered with no `chunks` key at all
    and no `chunk_order_basis` -- on cern-r3final, which is built this way,
    every document's Content tab had nothing to list.
    """
    _activate(clean_runtime, [BOUNDARY_RULE, ROW_RULE])
    _enforce(monkeypatch)

    body = _get(
        clean_runtime, "/viz/api/console/records/detail", layers, "doc:contains",
    ).json()["payload"]

    assert [chunk["node_id"] for chunk in body["chunks"]] == ["chunk:con:0", "chunk:con:1"]
    assert [chunk["ordinal"] for chunk in body["chunks"]] == [0, 1]
    assert body["chunks"][0]["text"] == "contains first"
    assert body["chunk_order_basis"] == "chunk_ordinal"
    assert body["chunk_edge"] == "contains"
    assert body["chunks_without_ordinal"] == 0


def test_a_chunk_recorded_both_ways_is_listed_once(
    layers: str, clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """One chunk, two edges, one row -- and the ordinal is `member_of`'s.

    A graph that records both directions would otherwise list the chunk twice.
    `member_of` wins the tie, so a graph that already had it reads exactly as
    it did before: here the two edges store DIFFERENT ordinals (0 and 7) and
    the one that comes back is 0.
    """
    _activate(clean_runtime, [BOUNDARY_RULE, ROW_RULE])
    _enforce(monkeypatch)

    body = _get(
        clean_runtime, "/viz/api/console/records/detail", layers, "doc:bothways",
    ).json()["payload"]

    assert [chunk["node_id"] for chunk in body["chunks"]] == ["chunk:bw:0"]
    assert body["chunks"][0]["ordinal"] == 0
    assert body["chunk_edge"] == "member_of"


def test_an_outgoing_contains_to_a_non_chunk_is_not_a_chunk(
    layers: str, clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """doc:guide contains figure:1, which is not a document_chunk.

    Widening the query to outgoing `contains` must not turn every contained
    thing into a chunk of the document.
    """
    _activate(clean_runtime, [BOUNDARY_RULE, ROW_RULE])
    _enforce(monkeypatch)

    body = _get(
        clean_runtime, "/viz/api/console/records/detail", layers, "doc:guide",
    ).json()["payload"]

    assert "figure:1" not in [chunk["node_id"] for chunk in body["chunks"]]
    assert body["chunk_edge"] == "member_of"


def test_a_chunk_the_policy_hides_is_absent_from_the_list(
    layers: str, clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _activate(clean_runtime, [BOUNDARY_RULE, ROW_RULE, DENY_RESTRICTED_RULE])
    _enforce(monkeypatch)

    body = _get(
        clean_runtime, "/viz/api/console/records/detail", layers, "doc:guide",
    ).json()["payload"]

    ids = [chunk["node_id"] for chunk in body["chunks"]]
    assert "chunk:hidden" not in ids
    assert "withheld" not in json.dumps(body)


def test_a_record_with_no_chunks_omits_the_key(
    layers: str, clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Absent, not empty: an empty list reads as "this has no content"."""
    _activate(clean_runtime, [BOUNDARY_RULE, ROW_RULE])
    _enforce(monkeypatch)

    body = _get(
        clean_runtime, "/viz/api/console/records/detail", layers, "rel:visible",
    ).json()["payload"]

    assert "chunks" not in body


@pytest.mark.parametrize(
    ("path", "params"),
    [
        ("/viz/api/console/records/raw", {"id": "doc:guide"}),
        ("/viz/api/console/records/detail", {"id": "doc:guide"}),
        ("/viz/api/console/records/list", {"type": "document"}),
        ("/viz/api/console/records/search", {"q": "titled"}),
        ("/viz/api/console/records/detail", {"id": "doc:titled"}),
    ],
)
def test_no_read_publishes_an_absolute_path(
    layers: str,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    path: str,
    params: dict[str, str],
) -> None:
    """The fixture publishes /Users/example/corpus/guide.md on a document.

    Masking used to cover the artifact read alone, so the same stored value
    went out verbatim on every other read.
    """
    _activate(clean_runtime, [BOUNDARY_RULE, ROW_RULE])
    _enforce(monkeypatch)

    response = _client(clean_runtime).get(
        path, params={"version": layers, **params},
    )

    assert response.status_code == 200
    assert "/Users" not in response.text
    assert "example" not in response.text
    assert "corpus" not in response.text


@pytest.mark.parametrize(
    ("path", "params"),
    [
        # Text hit and exact-ID hit are different code paths in the search
        # composer, and BOTH emitted the identity raw.
        ("/viz/api/console/records/search", {"q": "strid"}),
        ("/viz/api/console/records/search", {"q": "doc:strid"}),
        ("/viz/api/console/records/search", {"q": "objid"}),
        ("/viz/api/console/records/list", {"type": "document"}),
        ("/viz/api/console/records/detail", {"id": "doc:strid"}),
        ("/viz/api/console/records/detail", {"id": "doc:objid"}),
        ("/viz/api/console/records/raw", {"id": "doc:strid"}),
        ("/viz/api/console/records/raw", {"id": "doc:objid"}),
        # The raw read's EDGE rows carry their own identity.
        ("/viz/api/console/records/raw", {"id": "doc:guide"}),
    ],
)
def test_no_read_publishes_a_path_valued_source_record_id(
    layers: str,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    path: str,
    params: dict[str, str],
) -> None:
    """The identity is emitted from five places; two of them did not mask it."""
    _activate(clean_runtime, [BOUNDARY_RULE, ROW_RULE])
    _enforce(monkeypatch)

    response = _client(clean_runtime).get(
        path, params={"version": layers, **params},
    )

    assert response.status_code == 200
    assert "/Users" not in response.text
    assert "example" not in response.text


def test_the_search_exact_hit_still_carries_its_masked_identity(
    layers: str, clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Masked, not dropped: the reader still sees WHICH record it came from."""
    _activate(clean_runtime, [BOUNDARY_RULE, ROW_RULE])
    _enforce(monkeypatch)

    body = _client(clean_runtime).get(
        "/viz/api/console/records/search",
        params={"version": layers, "q": "doc:strid"},
    ).json()["payload"]

    hit = next(row for row in body["results"] if row["node_id"] == "doc:strid")
    assert hit["match"] == "record_id"
    assert hit["source_record_id"] == {"path": "strid.md"}


def test_a_nested_identity_keeps_its_shape_when_masked(
    layers: str, clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _activate(clean_runtime, [BOUNDARY_RULE, ROW_RULE])
    _enforce(monkeypatch)

    body = _get(
        clean_runtime, "/viz/api/console/records/raw", layers, "doc:objid",
    ).json()["payload"]

    assert body["row"]["source_record_id"] == {
        "repo": "corpus", "path": "objid.md",
    }


def test_an_ordinal_that_is_not_a_count_is_counted_not_dropped(
    layers: str, clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """-1 and 1.5 are numbers, so SQL ordered by them and Python dropped them.

    A negative also sorted to the FRONT, so it ate a slot at the head of the
    page and then vanished from the result.
    """
    _activate(clean_runtime, [BOUNDARY_RULE, ROW_RULE])
    _enforce(monkeypatch)

    body = _get(
        clean_runtime, "/viz/api/console/records/detail", layers, "doc:guide",
    ).json()["payload"]

    ids = [chunk["node_id"] for chunk in body["chunks"]]
    assert "chunk:negative" not in ids
    assert "chunk:fractional" not in ids
    assert [chunk["ordinal"] for chunk in body["chunks"]][0] == 0
    # unordered + negative + fractional, all three counted.
    assert body["chunks_without_ordinal"] == 3


def test_the_chunk_counts_are_taken_after_the_policy_filter(
    layers: str, clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A count taken before it tells the reader how many rows policy hid."""
    _activate(clean_runtime, [BOUNDARY_RULE, ROW_RULE, DENY_RESTRICTED_RULE])
    _enforce(monkeypatch)

    body = _get(
        clean_runtime, "/viz/api/console/records/detail", layers, "doc:guide",
    ).json()["payload"]

    # chunk:hidden is restricted, so it is neither listed nor counted.
    assert "chunk:hidden" not in [chunk["node_id"] for chunk in body["chunks"]]
    assert body["chunks_without_ordinal"] == 3
    assert body["chunks_capped"] is False


def test_the_raw_read_names_the_keys_it_masked(
    layers: str, clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A masked row must not be presented as a verbatim one."""
    _activate(clean_runtime, [BOUNDARY_RULE, ROW_RULE])
    _enforce(monkeypatch)

    body = _get(
        clean_runtime, "/viz/api/console/records/raw", layers, "doc:guide",
    ).json()["payload"]

    assert "row.attrs.path" in body["masked_paths"]
    assert body["row"]["attrs"]["path"] == "guide.md"
    assert "free text" in body["masked_paths_rule"]


def test_a_document_past_the_chunk_cap_reports_its_beginning(
    layers: str, clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Ordered in SQL, so the capped page is the first chunks, not a hash slice."""
    _activate(clean_runtime, [BOUNDARY_RULE, ROW_RULE])
    _enforce(monkeypatch)
    monkeypatch.setattr(records, "CHUNK_CAP", 2)

    body = _get(
        clean_runtime, "/viz/api/console/records/detail", layers, "doc:guide",
    ).json()["payload"]

    assert [chunk["ordinal"] for chunk in body["chunks"]] == [0, 1]
    assert body["chunks_capped"] is True
    assert body["chunk_cap"] == 2


def test_chunks_without_a_stored_ordinal_are_counted_not_hidden(
    layers: str, clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Left out because nothing orders them -- and the reader is told so."""
    _activate(clean_runtime, [BOUNDARY_RULE, ROW_RULE])
    _enforce(monkeypatch)

    body = _get(
        clean_runtime, "/viz/api/console/records/detail", layers, "doc:guide",
    ).json()["payload"]

    # Three: the edge with no ordinal key, plus the -1 and the 1.5, which are
    # numbers but not ordinals.
    assert body["chunks_without_ordinal"] == 3
    assert body["chunks_without_ordinal_capped"] is False
    assert "chunk:unordered" not in json.dumps(body["chunks"])
    # None of the three carries a char_offset either, so no second basis
    # covers them and the listed chunks stay on the stored ordinal.
    assert body["chunk_order_basis"] == "chunk_ordinal"


def test_chunks_fall_back_to_the_stored_char_offset(
    layers: str, clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Issue #2178: an offset on every chunk IS a recorded order.

    Ordered numerically, not lexically -- the offsets are stored both as JSON
    numbers and as digit strings, and "120" sorts before "40" as text.
    """
    _activate(clean_runtime, [BOUNDARY_RULE, ROW_RULE])
    _enforce(monkeypatch)

    body = _get(
        clean_runtime, "/viz/api/console/records/detail", layers, "doc:offsets",
    ).json()["payload"]

    assert body["chunk_order_basis"] == "char_offset"
    assert [chunk["node_id"] for chunk in body["chunks"]] == [
        "chunk:off:head", "chunk:off:dup", "chunk:off:mid", "chunk:off:tail",
    ]
    assert [chunk["char_offset"] for chunk in body["chunks"]] == [0, 40, 40, 120]
    # No ordinal was stored, so none is invented for the reader.
    assert [chunk["ordinal"] for chunk in body["chunks"]] == [None] * 4
    assert body["chunks"][0]["heading_path"] == "Top"
    assert body["chunks"][0]["page"] == 1
    assert body["chunks_without_ordinal"] == 0
    assert body["chunks_capped"] is False


def test_a_stored_chunk_ordinal_beats_a_stored_char_offset(
    layers: str, clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The offsets disagree with the ordinals here; the ordinals decide."""
    _activate(clean_runtime, [BOUNDARY_RULE, ROW_RULE])
    _enforce(monkeypatch)

    body = _get(
        clean_runtime, "/viz/api/console/records/detail", layers, "doc:both",
    ).json()["payload"]

    assert body["chunk_order_basis"] == "chunk_ordinal"
    assert [chunk["node_id"] for chunk in body["chunks"]] == [
        "chunk:both:0", "chunk:both:1",
    ]
    assert [chunk["ordinal"] for chunk in body["chunks"]] == [0, 1]
    assert body["chunks_without_ordinal"] == 0


def test_chunks_no_single_basis_covers_are_refused_not_half_ordered(
    layers: str, clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """One chunk has an offset, the other has nothing.

    Listing the one that can be placed would show the reader a document whose
    beginning is missing, with nothing in the response saying so. The named
    refusal covers both chunks instead.
    """
    _activate(clean_runtime, [BOUNDARY_RULE, ROW_RULE])
    _enforce(monkeypatch)

    body = _get(
        clean_runtime, "/viz/api/console/records/detail", layers, "doc:partial",
    ).json()["payload"]

    assert body["chunks"] == []
    assert body["chunk_order_basis"] is None
    assert body["chunks_without_ordinal"] == 2
    assert body["chunks_without_ordinal_capped"] is False
    assert "part offset" not in json.dumps(body["chunks"])


def test_no_response_carries_a_hidden_policy_name(
    layers: str, clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A cut: the filter's redaction has no field-to-rule mapping to report."""
    _activate(clean_runtime, [BOUNDARY_RULE, ROW_RULE, DENY_RESTRICTED_RULE])
    _enforce(monkeypatch)

    for record_id in ("doc:guide", "figure:1", "paper:1"):
        for path in (
            "/viz/api/console/records/detail",
            "/viz/api/console/records/raw",
            "/viz/api/console/records/artifact",
        ):
            assert "hidden_policy" not in _get(
                clean_runtime, path, layers, record_id,
            ).text


# --- helpers ----------------------------------------------------------------


def _latest(dsn: str) -> str:
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT id FROM okg.v_generations WHERE deployment_name = %s "
            "AND status = 'published' ORDER BY published_at DESC LIMIT 1",
            (DEPLOYMENT,),
        )
        return str(cur.fetchone()[0])


def _seed_authority(
    dsn: str,
    *,
    record_id: dict[str, Any],
    payload: str,
    declared_bytes: int | None = None,
    bound_before_publish: bool = True,
    extra_field: str | None = None,
) -> None:
    """Bind one sealed indexed-v2 authority holding one record's payload.

    Written straight into the authority's tables on purpose: the runner will
    not produce one (`allow_unconsumed_indexed_authority` is reachable from no
    registry and no environment variable), so the schema IS the contract this
    read has to satisfy.
    """
    body = payload.encode("utf-8")
    digest = hashlib.sha256(body).hexdigest()
    key = json.dumps(record_id, sort_keys=True, separators=(",", ":"))
    authority_id = str(uuid.uuid4())
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.source_record_authority_payloads
              (sha256, payload_kind, byte_count, chunk_count)
            VALUES (%s, 'json', %s, 1)
            ON CONFLICT (sha256) DO NOTHING
            """,
            (digest, declared_bytes if declared_bytes is not None else len(body)),
        )
        cur.execute(
            """
            INSERT INTO okg.source_record_authority_payload_chunks
              (sha256, chunk_ordinal, payload)
            VALUES (%s, 0, %s) ON CONFLICT DO NOTHING
            """,
            (digest, body),
        )
        cur.execute(
            """
            INSERT INTO okg.source_record_authorities
              (authority_id, source, incarnation, attempt, state)
            VALUES (%s, 'fixture', 'inc-1', 'attempt-1', 'staging')
            """,
            (authority_id,),
        )
        cur.execute(
            """
            INSERT INTO okg.source_record_authority_operations
              (authority_id, ordinal, record_id, operation, operation_sha256,
               component_count)
            VALUES (%s, 0, %s, 'upsert', %s, 1)
            """,
            (authority_id, key, digest),
        )
        cur.execute(
            """
            INSERT INTO okg.source_record_authority_records
              (authority_id, record_id, operation_ordinal) VALUES (%s, %s, 0)
            """,
            (authority_id, key),
        )
        cur.execute(
            """
            INSERT INTO okg.source_record_authority_components
              (authority_id, operation_ordinal, component_kind, component_ordinal)
            VALUES (%s, 0, 'primary', 0)
            """,
            (authority_id,),
        )
        cur.execute(
            """
            INSERT INTO okg.source_record_authority_component_fields
              (authority_id, operation_ordinal, component_kind,
               component_ordinal, field_name, payload_sha256)
            VALUES (%s, 0, 'primary', 0, 'record', %s)
            """,
            (authority_id, digest),
        )
        if extra_field is not None:
            cur.execute(
                """
                INSERT INTO okg.source_record_authority_component_fields
                  (authority_id, operation_ordinal, component_kind,
                   component_ordinal, field_name, payload_sha256)
                VALUES (%s, 0, 'primary', 0, %s, %s)
                """,
                (authority_id, extra_field, digest),
            )
        # Sealing LAST: `guard_authority_child_mutation` refuses a child row
        # once the authority is sealed, which is the schema saying a sealed
        # authority is immutable. The seed has to build it the way the writer
        # would -- staging, then sealed -- rather than declaring it sealed and
        # filling it in afterwards.
        cur.execute(
            """
            UPDATE okg.source_record_authorities
               SET state = 'sealed', sealed_at = now(), logical_sha256 = %s,
                   record_count = 1, operation_count = 1, component_count = 1,
                   logical_bytes = %s
             WHERE authority_id = %s
            """,
            (digest, len(body), authority_id),
        )
        # `bound_at` is what ties a payload to a version: the binding is keyed
        # by source alone, so only a binding OLDER than the generation is known
        # to be the one that generation was built from. The fixture publishes
        # before it seeds, so a binding meant to be readable is backdated.
        cur.execute(
            """
            INSERT INTO okg.source_record_authority_refs
              (source, authority_id, incarnation, logical_sha256, record_count,
               bound_at)
            VALUES ('fixture', %s, 'inc-1', %s, 1,
                    now() - %s::interval)
            """,
            (authority_id, digest, "1 hour" if bound_before_publish else "0 seconds"),
        )


# --- PACT console-archiv3-read-layer-fixes: the raw read's revisions --------
#
# Every Archiv3 twiki record's `source_revision` holds the EOS root it was read
# from, an absolute path under the operator's home directory, and the raw read
# served it verbatim on the row and on each of the page's 46 edges: 47 copies
# per page, beside a Provenance line counting only the paths it HAD masked.
# The revision below is that captured shape, with the path replaced.

TWIKI_REVISION = {
    "run_id": "96161946-2a08-53b5-bb32-843b55eb1776",
    "eos_root": "/Users/operator/archiv3/eos/twiki",
    "n_records": 980,
    "content_hash": "1371267ec1776e3e9c37232b72d9a358626ca95377506c1a132244f92bf7c636",
}
PLAIN_REVISION = {"run_id": "r-plain", "content_hash": "b" * 64}


@pytest.fixture
def twiki_revisions(clean_runtime: str) -> str:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)

    def node(node_id: str, subtype: str, revision: dict[str, Any], **attrs: Any) -> NodeFact:
        return NodeFact(
            node_id=node_id, subtype=subtype, attrs=attrs,
            source_record_id={"page": node_id}, source_revision=revision,
        )

    _publish(
        clean_runtime,
        [
            node("twiki:CMS:Deleted_3_1_X", "documentation_page", TWIKI_REVISION,
                 title="Deleted_3_1_X", text="Deleted release page"),
            node("chunk:twiki:0", "document_chunk", TWIKI_REVISION, text="first"),
            node("doc:plain", "document", PLAIN_REVISION, title="Plain"),
            node("chunk:plain:0", "document_chunk", PLAIN_REVISION, text="plain"),
        ],
        [
            EdgeFact(src="twiki:CMS:Deleted_3_1_X", dst="chunk:twiki:0",
                     edge_type="contains", source_revision=TWIKI_REVISION),
            EdgeFact(src="doc:plain", dst="chunk:plain:0",
                     edge_type="contains", source_revision=PLAIN_REVISION),
        ],
    )
    return _latest(clean_runtime)


def test_the_raw_read_masks_a_path_inside_a_source_revision(
    twiki_revisions: str, clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _activate(clean_runtime, [BOUNDARY_RULE, ROW_RULE])
    _enforce(monkeypatch)

    response = _get(
        clean_runtime, "/viz/api/console/records/raw", twiki_revisions,
        "twiki:CMS:Deleted_3_1_X",
    )
    body = response.json()["payload"]

    assert "/Users" not in response.text
    assert body["row"]["source_revision"] == {**TWIKI_REVISION, "eos_root": "twiki"}
    assert body["edges"][0]["source_revision"] == {
        **TWIKI_REVISION, "eos_root": "twiki",
    }
    assert "row.source_revision.eos_root" in body["masked_paths"]
    assert "edges[0].source_revision.eos_root" in body["masked_paths"]


def test_a_revision_with_no_path_is_served_as_stored_and_not_named(
    twiki_revisions: str, clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Masking nothing that is not there: the count must stay true."""
    _activate(clean_runtime, [BOUNDARY_RULE, ROW_RULE])
    _enforce(monkeypatch)

    body = _get(
        clean_runtime, "/viz/api/console/records/raw", twiki_revisions, "doc:plain",
    ).json()["payload"]

    assert body["row"]["source_revision"] == PLAIN_REVISION
    assert body["edges"][0]["source_revision"] == PLAIN_REVISION
    assert not [key for key in body["masked_paths"] if "source_revision" in key]


def test_an_uncapped_edge_page_reports_its_exact_total(
    twiki_revisions: str, clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _activate(clean_runtime, [BOUNDARY_RULE, ROW_RULE])
    _enforce(monkeypatch)

    body = _get(
        clean_runtime, "/viz/api/console/records/raw", twiki_revisions, "doc:plain",
    ).json()["payload"]

    assert body["edges_truncated"] is False
    assert body["edge_total"] == 1
    assert body["edge_total_unavailable_reason"] is None
