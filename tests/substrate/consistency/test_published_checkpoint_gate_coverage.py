"""Every fact in a published checkpoint must be covered by a structural gate.

Two P0 holes let structurally invalid facts reach a published generation:

* #1425 -- the gate's baseline is the last *published* generation, but its
  scope is the current continuation segment. A blocked checkpoint advances
  that segment, so an unchanged retry sees an empty scope, zero counts, and
  publishes the violation the previous cycle refused.
* #1426 -- native views, lenses, and their derived facts are applied *after*
  the transform consistency gate and before checkpoint creation, so view
  output reaches a published generation with no structural check.
"""

from __future__ import annotations

import json
import uuid

import pytest

psycopg = pytest.importorskip("psycopg")

from okg.substrate.consistency import TransformConsistencyGate  # noqa: E402
from okg.substrate.derived_artifacts import stable_hash  # noqa: E402
from okg.substrate.graph_store import ChronosGraphStore  # noqa: E402
from okg.substrate.library.sources.base import EdgeFact, NodeFact  # noqa: E402
from okg.substrate.publisher import Publisher  # noqa: E402
from okg.views.assembly import assemble as default_assemble  # noqa: E402
from okg.views.planners.base import QuoteSpec, ViewDefinition  # noqa: E402
from okg.views.types import Anchor  # noqa: E402
from tests.helpers.governed_view_materializer import (  # noqa: E402
    ViewMaterializer,
)
from tests.pytest.catalog_fixture_builders import (  # noqa: E402
    bind_test_catalog_writer,
    seed_catalog,
)

pytestmark = pytest.mark.postgres


def _publisher(dsn: str, **kwargs) -> Publisher:
    return Publisher(
        dsn,
        "default",
        consistency_gates=[TransformConsistencyGate()],
        catalog_identity=bind_test_catalog_writer(dsn, "default"),
        **kwargs,
    )


def _edge_live_in_generation(
    dsn: str,
    generation_id: str,
    *,
    src: str,
    dst: str,
    edge_type: str,
) -> bool:
    """Read a chronos GENERATION, not the live branch head.

    ``okg.graph_edges`` is a chronos view: querying it outside a generation
    context answers about the mutable branch, not about what a reader of the
    published generation can see.
    """
    with psycopg.connect(dsn, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        session = store.checkout_generation(str(generation_id))
        rows = session.query(
            "SELECT 1 FROM okg.graph_edges "
            "WHERE src = %(src)s AND dst = %(dst)s AND edge_type = %(t)s",
            {"src": src, "dst": dst, "t": edge_type},
        )
        return bool(list(rows))


def _published_generation_ids(dsn: str) -> list[str]:
    with psycopg.connect(dsn, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        return [
            str(checkpoint.checkpoint_id)
            for checkpoint in store.list_generations(
                deployment_name="default",
                branch="default",
                status="published",
            )
        ]


def test_blocked_violation_survives_an_unchanged_retry(clean_runtime: str) -> None:
    """#1425: a blocked checkpoint must not launder the violation it blocked."""
    sfx = uuid.uuid4().hex[:8]
    doc, other, ghost = f"d:{sfx}", f"o:{sfx}", f"ghost:{sfx}"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(
            conn,
            subtypes=["document"],
            narrowings=[("document", "related", "document")],
        )
        ChronosGraphStore.from_connection(conn).apply_facts(
            [
                NodeFact(doc, "document", {}, {"id": doc}, {"v": 1}),
                NodeFact(other, "document", {}, {"id": other}, {"v": 1}),
                EdgeFact(
                    doc,
                    other,
                    "related",
                    source_record_id={"id": "e1"},
                    source_revision={"v": 1},
                ),
            ],
            source="default",
            strict_endpoints=True,
        )

    clean = _publisher(clean_runtime).cycle()
    assert clean["status"] == "published", clean

    # A dangling edge: `ghost` is not a live node.
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        ChronosGraphStore.from_connection(conn).apply_facts(
            [
                EdgeFact(
                    doc,
                    ghost,
                    "related",
                    source_record_id={"id": "e2"},
                    source_revision={"v": 1},
                ),
            ],
            source="default",
            strict_endpoints=False,
        )

    blocked = _publisher(clean_runtime).cycle()
    assert blocked["status"] == "blocked", blocked

    # Nothing is written between the two cycles. The violation is unrepaired,
    # so the retry must still refuse.
    retry = _publisher(clean_runtime).cycle()
    assert retry["status"] == "blocked", retry

    # Coverage, not presence: prove no published generation shows the edge.
    published = _published_generation_ids(clean_runtime)
    assert published, "expected at least the first clean published generation"
    for generation_id in published:
        assert not _edge_live_in_generation(
            clean_runtime,
            generation_id,
            src=doc,
            dst=ghost,
            edge_type="related",
        ), f"dangling edge visible in published generation {generation_id}"


# --- #1426: post-gate view output --------------------------------------------


def _emitting_materializer(
    emit: list[dict[str, str] | None],
) -> tuple[str, ViewMaterializer]:
    """A view that writes one edge fact the way `_project_view_cites` does.

    The write happens inside `build_corpus`, i.e. inside the native-view stage,
    which the publisher runs AFTER the transform consistency gate and BEFORE
    checkpoint creation. `emit[0]` selects the edge (or ``None`` for a
    well-behaved cycle).
    """
    suffix = uuid.uuid4().hex[:12]
    view_id = f"test.post_gate_view.{suffix}"

    def planner(corpus, view_def):
        return [
            QuoteSpec(
                id="summary-fragment",
                anchor_id="summary",
                display_label="Summary",
            )
        ]

    def assembler(view) -> str:
        return "# Post-gate view\n\n" + default_assemble(view)

    def view_def_factory() -> ViewDefinition:
        return ViewDefinition(
            view_id=view_id,
            planner=planner,
            assembler=assembler,
            params={"audience": "operator"},
            view_def_hash=stable_hash({"view_id": view_id}),
            display_name="Post-gate view",
        )

    def build_corpus(conn, *, generation_id: int) -> dict:
        spec = emit[0]
        if spec is not None:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO okg.edge_facts
                      (edge_id, src, dst, edge_type, attrs,
                       provenance, confidence, source,
                       source_record_id, source_revision,
                       observed_at, dedupe_key, run_id, op)
                    VALUES
                      (%(edge_id)s, %(src)s, %(dst)s, %(edge_type)s, '{}'::jsonb,
                       'derived_deterministic', 1.0, %(source)s,
                       %(record)s::jsonb, %(revision)s::jsonb,
                       now(), %(dedupe_key)s, gen_random_uuid(), 'I')
                    ON CONFLICT (dedupe_key, observed_at) DO NOTHING
                    """,
                    {
                        "edge_id": (
                            uuid.uuid5(
                                uuid.NAMESPACE_URL,
                                f"{spec['src']}|{spec['edge_type']}|{spec['dst']}",
                            ).int
                            & ((1 << 63) - 1)
                        ),
                        "src": spec["src"],
                        "dst": spec["dst"],
                        "edge_type": spec["edge_type"],
                        "source": f"derived_artifact:{view_id}",
                        "record": json.dumps(
                            {"view_id": view_id, "dst": spec["dst"]}
                        ),
                        "revision": json.dumps(
                            {"generation_id": int(generation_id)}
                        ),
                        "dedupe_key": (
                            f"post-gate:{view_id}:{spec['src']}:"
                            f"{spec['edge_type']}:{spec['dst']}:{generation_id}"
                        ),
                    },
                )
        return {
            "summary": Anchor.from_content(
                "summary", "post-gate view coverage fixture"
            )
        }

    return view_id, ViewMaterializer(
        view_def_factory=view_def_factory,
        build_corpus=build_corpus,
        name=view_id,
    )


def _seed_two_documents(dsn: str) -> tuple[str, str, int]:
    sfx = uuid.uuid4().hex[:8]
    left, right = f"l:{sfx}", f"r:{sfx}"
    with psycopg.connect(dsn, autocommit=True) as conn:
        catalog_version_id = seed_catalog(
            conn,
            subtypes=["document", "view"],
            narrowings=[("document", "related", "document")],
        )
        # The empty-narrowing-catalog trap: with no narrowings at this catalog
        # version the narrowing family never activates and every negative arm
        # below would pass vacuously.
        with conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM okg.narrowings "
                "WHERE catalog_version_id = %s",
                (catalog_version_id,),
            )
            assert int(cur.fetchone()[0]) > 0, "narrowing catalog is empty"
        ChronosGraphStore.from_connection(conn).apply_facts(
            [
                NodeFact(left, "document", {}, {"id": left}, {"v": 1}),
                NodeFact(right, "document", {}, {"id": right}, {"v": 1}),
                EdgeFact(
                    left,
                    right,
                    "related",
                    source_record_id={"id": "e1"},
                    source_revision={"v": 1},
                ),
            ],
            source="default",
            strict_endpoints=True,
        )
    return left, right, catalog_version_id


@pytest.mark.parametrize(
    ("case", "edge_type", "dst_is_live"),
    [("dangling", "related", False), ("narrowing", "cites", True)],
)
def test_view_output_cannot_bypass_the_structural_gate(
    clean_runtime: str,
    case: str,
    edge_type: str,
    dst_is_live: bool,
) -> None:
    """#1426: view facts land after the gate and before checkpoint creation."""
    left, right, _cv = _seed_two_documents(clean_runtime)
    dst = right if dst_is_live else f"ghost:{uuid.uuid4().hex[:8]}"
    emit: list[dict[str, str] | None] = [None]
    view_id, materializer = _emitting_materializer(emit)

    clean = _publisher(clean_runtime, materializers=[materializer]).cycle()
    assert clean["status"] == "published", clean

    # A zero-delta cycle reuses the previous generation and never runs the
    # view stage at all, so the emitting view needs a real cycle to run in.
    emit[0] = {"src": left, "dst": dst, "edge_type": edge_type}
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        extra = f"x:{uuid.uuid4().hex[:8]}"
        ChronosGraphStore.from_connection(conn).apply_facts(
            [NodeFact(extra, "document", {}, {"id": extra}, {"v": 1})],
            source="default",
            strict_endpoints=True,
        )
    outcome = _publisher(clean_runtime, materializers=[materializer]).cycle()

    # Non-vacuity: the view fact really was loaded and applied post-gate. A
    # cycle that never picked the fact up would satisfy every assertion below
    # without proving anything about gate coverage.
    assert outcome["derived_fact_summary"]["view_loaded_count"] >= 1, outcome

    # The gate ran over the view output, and it is the POST-VIEW pass -- the
    # pre-view pass cannot have seen a fact written after it.
    post_view = [
        summary
        for summary in outcome["consistency_gates"]
        if (summary.get("details") or {}).get("gate_pass") == "post_view"
    ]
    assert post_view, outcome["consistency_gates"]

    # The invariant: no published generation exposes the invalid view fact.
    # It holds either by blocking the checkpoint (a narrowing violation) or by
    # the gate retracting the offending derived edge before the checkpoint (an
    # orphaned derived edge). Both are structural coverage; neither is silent.
    handled = outcome["status"] == "blocked" or any(
        int(
            (summary.get("details") or {}).get(
                "orphan_retracted_derived_edge_count", 0
            )
            or 0
        )
        + int(
            (summary.get("details") or {}).get(
                "pruned_dangling_edge_count", 0
            )
            or 0
        )
        for summary in post_view
    )
    assert handled, (case, outcome["status"], post_view)

    for generation_id in _published_generation_ids(clean_runtime):
        assert not _edge_live_in_generation(
            clean_runtime,
            generation_id,
            src=left,
            dst=dst,
            edge_type=edge_type,
        ), f"{case} view edge visible in published generation {generation_id}"
