"""G1 (generation identity): the publisher workflow's admission-feedback step must
accept the chronos_native string generation_id.

chronos_native cycle() returns an opaque Chronos checkpoint STRING as generation_id
(plus an int compat_generation_id). The DBOS publisher workflow passes generation_id to
apply_generation_feedback; an earlier `int(generation_id)` coercion crashed on the string.
apply_generation_feedback / _load_generation_feedback already handle the string (they read
the Chronos checkpoint metadata), so the workflow must pass it through unchanged.
"""
from __future__ import annotations

import pytest

psycopg = pytest.importorskip("psycopg")

from okg.substrate.admission import apply_generation_feedback  # noqa: E402
from okg.substrate.graph_store import ChronosGraphStore  # noqa: E402
from okg.substrate.library.sources.base import NodeFact  # noqa: E402
from okg.substrate.publisher import Publisher  # noqa: E402
from tests.pytest.catalog_fixture_builders import seed_catalog  # noqa: E402
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402

pytestmark = pytest.mark.postgres


def test_apply_generation_feedback_accepts_chronos_string_gen(clean_runtime: str) -> None:
    seed_catalog(
        psycopg.connect(clean_runtime, autocommit=True),
        subtypes=["page"],
        narrowings=[],
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        ChronosGraphStore.from_connection(conn).apply_facts(
            [NodeFact("n:a", "page", {"t": "a"}, {"id": "n:a"}, {"v": 1})],
            source="default", strict_endpoints=True,
        )

    out = Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()
    assert out["status"] == "published", out
    gen = out["generation_id"]
    # chronos_native returns an opaque checkpoint STRING (not an int).
    assert isinstance(gen, str) and gen.startswith("okg:"), gen

    # The workflow passes this string straight through; it must NOT be int()-coerced
    # (that was the crash). apply_generation_feedback resolves it via the checkpoint
    # metadata.
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.transaction():
            feedback = apply_generation_feedback(
                conn, deployment_name="default", generation_id=gen,
            )
    assert feedback["status"] == "published"


def test_publisher_receipt_and_checkpoint_bind_exact_predecessor(
    clean_runtime: str,
) -> None:
    """The producer side of generation-bound incremental proof is durable."""
    seed_catalog(
        psycopg.connect(clean_runtime, autocommit=True),
        subtypes=["page"],
        narrowings=[],
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [NodeFact("n:first", "page", {"t": "first"}, {"id": "n:first"}, {})],
            source="default",
            strict_endpoints=True,
        )

    first = Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()
    assert first["status"] == "published", first
    assert first["predecessor_generation_id"] is None

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn)
        first_checkpoint = store.get_generation(first["generation_id"])
        assert first_checkpoint.metadata["predecessor_generation_id"] is None
        store.apply_facts(
            [
                NodeFact(
                    "n:second",
                    "page",
                    {"t": "second"},
                    {"id": "n:second"},
                    {},
                )
            ],
            source="default",
            strict_endpoints=True,
        )

    second = Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()
    assert second["status"] == "published", second
    assert second["predecessor_generation_id"] == first["generation_id"]

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn)
        second_checkpoint = store.get_generation(second["generation_id"])
        assert second_checkpoint.metadata["predecessor_generation_id"] == (
            first["generation_id"]
        )
