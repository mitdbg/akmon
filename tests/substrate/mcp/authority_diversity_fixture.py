"""The adversarial mixed-authority corpus for issue #1444 (part 2).

Synthetic and benchmark-blind. Every fact here is invented for this fixture;
nothing is copied from a sealed or benchmark corpus.

The shape is chosen so that the controls under test cannot pass by accident:

  * Four kinds of material share the query term — code, TEST code, prose
    documentation, and an operational record.
  * LEXICAL RANK FAVOURS THE WRONG ONE. The test file repeats the term most, so
    an unscoped search ranks it first and the documentation answer below it.
    Every authority assertion is therefore paired with the unscoped control
    that shows the corpus really was adversarial.
  * The test file and the code file share ONE producer (`source:code`), exactly
    as a git source emits both. So a family filter alone CANNOT separate them;
    only `exclude_path_globs` can, which is what makes that control's test
    non-vacuous.
  * The operational record carries NO `path` attribute at all, so it proves a
    path exclusion never removes a row it has not named.
  * `doc:code` owns four chunks and `doc:guide` owns two, so a per-parent cap
    has something to suppress and something to leave alone.
"""

from __future__ import annotations

from typing import Any

from okg.substrate.library.sources.base import EdgeFact, NodeFact
from tests.pytest.catalog_fixture_builders import SubtypeSpec


#: The one word every kind of material shares.
TERM = "provenance"

DOC = SubtypeSpec(
    "doc",
    metadata={
        "searchable_text_fields": ["path"],
        "searchable_text_path_fields": ["path"],
        "passage_rollup": {"edge_type": "contains", "child_subtype": "chunk"},
    },
)
CHUNK = SubtypeSpec("chunk", metadata={"searchable_text_fields": ["text"]})
SUBTYPES = [DOC, CHUNK]
NARROWINGS = [("doc", "contains", "chunk")]

#: Producer ids, and the families a deployment declares over them.
CODE_PRODUCER = "source:code"
DOCS_PRODUCER = "source:docs"
OPS_PRODUCER = "source:ops"

SEARCH_BLOCK: dict[str, Any] = {
    "source_families": {
        "code": [CODE_PRODUCER],
        "documentation": [DOCS_PRODUCER],
        "operations": [OPS_PRODUCER],
    },
    "profiles": {
        # Documentation only. The narrowest authority statement.
        "authority": {
            "subtypes": ["chunk"],
            "source_families": ["documentation"],
            "exclude_path_globs": ["tests/**"],
        },
        # Code AND documentation, minus tests. This is the "exclude tests and
        # fixtures from code searches" case: `tests` is NOT a separate family
        # here, so only the path exclusion can remove those rows.
        "engineering": {
            "subtypes": ["chunk"],
            "source_families": ["code", "documentation"],
            "exclude_path_globs": ["tests/**", "**/conftest.py"],
        },
        # No authority keys at all: proves the three controls are OFF unless a
        # profile declares them.
        "plain": {"subtypes": ["chunk"]},
        # A profile with a standing diversity default.
        "diverse": {"subtypes": ["chunk"], "max_per_parent": 1},
    },
}


def _node(node_id: str, subtype: str, attrs: dict[str, Any]) -> NodeFact:
    return NodeFact(
        node_id=node_id,
        subtype=subtype,
        attrs=attrs,
        source_record_id={"id": node_id},
        source_revision={"v": 1},
    )


def _edge(src: str, dst: str) -> EdgeFact:
    return EdgeFact(
        src=src,
        dst=dst,
        edge_type="contains",
        source_record_id={"id": f"{src}|contains|{dst}"},
        source_revision={"v": 1},
    )


def _body(repeats: int, tail: str) -> str:
    return f"{' '.join([TERM] * repeats)} {tail}"


#: (node_id, path, term repeats, tail). Repeats decide the ranking; the TEST
#: file deliberately wins it.
CODE_CHUNKS = [
    ("chunk:test:core", "tests/substrate/test_provenance.py", 9,
     "asserts the provenance block round-trips"),
    ("chunk:test:helper", "tests/substrate/conftest.py", 6,
     "fixture helper for provenance rows"),
    ("chunk:src:reader", "src/okg/substrate/reader.py", 4,
     "reads the provenance column"),
    ("chunk:src:writer", "src/okg/substrate/writer.py", 3,
     "writes the provenance column"),
]
DOCS_CHUNKS = [
    ("chunk:doc:guide", "docs/reference/provenance.md", 2,
     "explains what provenance means for a reader"),
    ("chunk:doc:policy", "docs/policy/trust.md", 1,
     "states the provenance trust policy"),
]
#: No `path` key at all.
OPS_CHUNKS = [
    ("chunk:ops:incident", None, 5,
     "incident record naming the provenance backfill"),
]
#: Published with NO `contains` edge, so the declared passage relation cannot
#: attach it to any parent. A per-parent cap must never suppress it.
ORPHAN_CHUNKS = [
    ("chunk:ops:orphan", None, 7,
     "loose note about provenance with no parent record"),
]

DOC_CODE = "doc:code"
DOC_GUIDE = "doc:guide"
DOC_OPS = "doc:ops"


def _group(doc_id: str, doc_path: str, rows: list[tuple[str, Any, int, str]]):
    facts: list[Any] = [_node(doc_id, "doc", {"path": doc_path})]
    for node_id, path, repeats, tail in rows:
        attrs: dict[str, Any] = {"text": _body(repeats, tail)}
        if path is not None:
            attrs["path"] = path
        facts.append(_node(node_id, "chunk", attrs))
        facts.append(_edge(doc_id, node_id))
    return facts


def node_groups() -> list[tuple[str, list[Any]]]:
    """(producer id, facts) batches. `source` is a property of the batch."""
    ops = _group(DOC_OPS, "repo/ops", OPS_CHUNKS)
    for node_id, _path, repeats, tail in ORPHAN_CHUNKS:
        ops.append(_node(node_id, "chunk", {"text": _body(repeats, tail)}))
    return [
        (CODE_PRODUCER, _group(DOC_CODE, "repo/okg", CODE_CHUNKS)),
        (DOCS_PRODUCER, _group(DOC_GUIDE, "repo/docs", DOCS_CHUNKS)),
        (OPS_PRODUCER, ops),
    ]


#: Expected memberships, so a test names what it wants rather than recomputing
#: it from the same data the code under test used.
CODE_IDS = [row[0] for row in CODE_CHUNKS]
DOCS_IDS = [row[0] for row in DOCS_CHUNKS]
OPS_IDS = [row[0] for row in OPS_CHUNKS] + [row[0] for row in ORPHAN_CHUNKS]
ORPHAN_IDS = [row[0] for row in ORPHAN_CHUNKS]
TEST_PATH_IDS = ["chunk:test:core", "chunk:test:helper"]
ALL_CHUNK_IDS = CODE_IDS + DOCS_IDS + OPS_IDS

#: Chunks per declared parent, for the diversity assertions.
CHUNKS_BY_PARENT = {
    DOC_CODE: CODE_IDS,
    DOC_GUIDE: DOCS_IDS,
    DOC_OPS: [row[0] for row in OPS_CHUNKS],
}


def register_producer_profiles(dsn: str, deployment_name: str,
                               producers: list[str]) -> None:
    """Register producer profiles so family validation has an authority.

    Written directly because the fixture publishes through `apply_facts`, which
    stamps a producer id without requiring a registered profile; the registry
    is deployment policy state, and this test needs it POPULATED to exercise
    the validated branch of `validate_family_sources`.
    """
    import psycopg

    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        for producer in producers:
            cur.execute(
                """
                INSERT INTO okg.producer_profiles (
                    deployment_name, producer_id, producer_kind, trust_label,
                    admission_mode, profile_fingerprint, status
                ) VALUES (
                    %(deployment)s, %(producer)s, 'source', 'trusted',
                    'direct', %(fingerprint)s, 'active'
                )
                ON CONFLICT DO NOTHING
                """,
                {
                    "deployment": deployment_name,
                    "producer": producer,
                    "fingerprint": f"test:{producer}",
                },
            )
