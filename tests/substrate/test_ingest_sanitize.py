"""PACT ``sanitize-fact-text-at-admission`` — NUL (U+0000) admission tests.

GitHub issue #638: the code_structure source captured a vendored file
whose text contains NUL; Postgres text/jsonb cannot store NUL, so the
chronos splice COPY aborted with ``UntranslatableCharacter`` and took
down the ENTIRE source run and its publish layer.

The fix is a SHARED normalizer (``okg.substrate.ingest.sanitize``)
applied at BOTH emit paths:

- the source-runner admission pass (``SourceRunner._sanitize_facts``,
  step 0 of ``_flush_chunk``): attr values are scrubbed (each NUL →
  U+FFFD, counted); a NUL in an identity-bearing field rejects that
  fact loudly while the rest of the run continues and publishes;
- the enricher fact emitters (``_fact_emitters.emit_node_fact`` /
  ``emit_edge_fact``), which bypass the runner entirely.

This module covers: normalizer unit behavior, the identity-rejection
classifier, both end-to-end paths through a real publish with the
scrubbed value queried back, the per-source report plumbing, and a
regression pin that the original ``UntranslatableCharacter`` abort is
dead.
"""
from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone

import psycopg
import pytest
from psycopg.types.json import Jsonb

from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.ingest import SourceRunner
from okg.substrate.ingest.sanitize import (
    REPLACEMENT_CHAR,
    identity_nul_fields,
    identity_nul_fields_from,
    sanitize_fact,
    sanitize_record_set,
    sanitize_value,
    scrub_nul,
    value_contains_nul,
)
from okg.substrate.ingest.service import _sanitize_report_fields
from okg.substrate.library.enrichers._fact_emitters import (
    emit_edge_fact,
    emit_node_fact,
)
from okg.substrate.library.enrichers.code_graph.markdown_structural import (
    MarkdownStructuralEnricher,
)
from okg.substrate.library.sources.base import (
    EdgeFact,
    EdgeKey,
    NodeFact,
    RecordEmission,
    RecordNode,
    RecordSet,
    SourceRun,
)
from okg.substrate.publisher import Publisher
from tests.pytest.catalog_fixture_builders import seed_catalog
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402


NUL = "\x00"
RC = REPLACEMENT_CHAR


# ---------------------------------------------------------------------------
# Unit: the shared normalizer (no DB).
# ---------------------------------------------------------------------------


def test_scrub_nul_replaces_each_occurrence_with_replacement_char():
    assert RC == "�"
    assert scrub_nul(f"a{NUL}b{NUL}{NUL}c") == (f"a{RC}b{RC}{RC}c", 3)


def test_scrub_nul_clean_string_returned_by_reference():
    text = "no nul here"
    out, count = scrub_nul(text)
    assert out is text
    assert count == 0


def test_sanitize_value_nested_dict_list_tuple():
    value = {
        "outer": {
            "text": f"doc{NUL}string",
            "items": [f"a{NUL}", {"deep": (f"b{NUL}{NUL}",)}],
        },
        "count": 7,
    }
    out, count = sanitize_value(value)
    assert count == 4
    assert out == {
        "outer": {
            "text": f"doc{RC}string",
            "items": [f"a{RC}", {"deep": (f"b{RC}{RC}",)}],
        },
        "count": 7,
    }
    assert not value_contains_nul(out)


def test_sanitize_value_scrubs_nul_in_dict_keys():
    out, count = sanitize_value({f"k{NUL}ey": "v", "ok": f"v{NUL}"})
    assert count == 2
    assert out == {f"k{RC}ey": "v", "ok": f"v{RC}"}


def test_sanitize_value_non_string_leaves_pass_through_untouched():
    value = {"i": 3, "f": 1.5, "b": True, "n": None, "raw": b"\x00"}
    out, count = sanitize_value(value)
    assert count == 0
    assert out is value  # untouched object returned by reference
    assert out["raw"] == b"\x00"  # bytes are not strings; not scrubbed


def test_sanitize_value_idempotent():
    once, n_once = sanitize_value({"t": f"x{NUL}y", "k": [f"{NUL}"]})
    twice, n_twice = sanitize_value(once)
    assert n_once == 2
    assert n_twice == 0
    assert twice is once


def test_sanitize_value_clean_structure_returned_by_reference():
    value = {"a": ["b", {"c": ("d",)}]}
    out, count = sanitize_value(value)
    assert out is value
    assert count == 0


def test_value_contains_nul_detects_keys_values_and_nesting():
    assert value_contains_nul(f"x{NUL}")
    assert value_contains_nul({f"k{NUL}": "v"})
    assert value_contains_nul({"k": [{"deep": (f"v{NUL}",)}]})
    assert not value_contains_nul({"k": ["v", 1, None, b"\x00"]})


# ---------------------------------------------------------------------------
# Unit: the identity-field rejection classifier (no DB).
# ---------------------------------------------------------------------------


def _node(**overrides) -> NodeFact:
    base = dict(
        node_id="n:1",
        subtype="concept",
        attrs={"text": "clean"},
        source_record_id={"id": "n:1"},
        source_revision={"v": 1},
    )
    base.update(overrides)
    return NodeFact(**base)


def _edge(**overrides) -> EdgeFact:
    base = dict(
        src="n:1",
        dst="n:2",
        edge_type="links_to",
        attrs={"note": "clean"},
        source_record_id={"id": "e:1"},
        source_revision={"v": 1},
    )
    base.update(overrides)
    return EdgeFact(**base)


@pytest.mark.parametrize(
    ("overrides", "expected_fields"),
    [
        ({"node_id": f"n:{NUL}1"}, ("node_id",)),
        ({"subtype": f"conc{NUL}ept"}, ("subtype",)),
        ({"source_record_id": {"id": f"x{NUL}y"}}, ("source_record_id",)),
        ({"source_record_id": {f"k{NUL}": "v"}}, ("source_record_id",)),
        ({"source_revision": {"v": [f"r{NUL}"]}}, ("source_revision",)),
    ],
)
def test_node_identity_nul_fields_classified(overrides, expected_fields):
    fact = _node(**overrides)
    assert identity_nul_fields(fact) == expected_fields
    result = sanitize_fact(fact)
    assert result.rejected
    assert result.fact is None
    assert result.rejected_fields == expected_fields


@pytest.mark.parametrize(
    ("overrides", "expected_fields"),
    [
        ({"src": f"n:{NUL}"}, ("src",)),
        ({"dst": f"n:{NUL}"}, ("dst",)),
        ({"edge_type": f"links{NUL}to"}, ("edge_type",)),
        ({"source_record_id": {"id": f"e{NUL}"}}, ("source_record_id",)),
        ({"source_revision": {"rev": f"r{NUL}"}}, ("source_revision",)),
        # provenance: closed vocabulary (edge_facts_provenance_check)
        # — a scrubbed value could never be valid, so NUL rejects.
        ({"provenance": f"expli{NUL}cit"}, ("provenance",)),
    ],
)
def test_edge_identity_nul_fields_classified(overrides, expected_fields):
    fact = _edge(**overrides)
    assert identity_nul_fields(fact) == expected_fields
    assert sanitize_fact(fact).rejected


def test_sanitize_fact_scrubs_attr_values_but_never_identity():
    fact = _node(attrs={"text": f"doc{NUL}string", "n": 1})
    result = sanitize_fact(fact)
    assert not result.rejected
    assert result.sanitized_value_count == 1
    assert result.fact.attrs == {"text": f"doc{RC}string", "n": 1}
    # Identity fields are byte-identical to the input.
    assert result.fact.node_id == fact.node_id
    assert result.fact.source_record_id == fact.source_record_id


def test_sanitize_fact_clean_fact_returned_by_reference():
    fact = _node()
    result = sanitize_fact(fact)
    assert not result.rejected
    assert result.fact is fact
    assert result.sanitized_value_count == 0


def test_sanitize_record_set_drops_rejected_identities_and_scrubs_edge_attrs():
    clean_node = RecordNode("n:clean", "concept", {"id": "clean"}, {"v": 1})
    bad_node = RecordNode(
        "n:bad", "concept", {"name": f"bad{NUL}"}, {"v": 1},
    )
    clean_edge = EdgeKey(
        "n:clean", "n:other", "links_to", attrs={"note": f"x{NUL}y"},
    )
    bad_edge = EdgeKey(
        "n:clean", "n:other", "links_to",
        source_record_id={"name": f"bad{NUL}"},
    )
    record_set = RecordSet(records={
        "clean.py": RecordEmission(
            descendants=[clean_node, bad_node], edges=[clean_edge, bad_edge],
        ),
        f"bad{NUL}.py": RecordEmission(descendants=[clean_node]),
    })

    result = sanitize_record_set(record_set)

    assert result.changed is True
    assert result.dropped_record_count == 1
    assert result.dropped_node_count == 2
    assert result.dropped_edge_count == 1
    assert result.sanitized_value_count == 1
    assert set(result.record_set.records) == {"clean.py"}
    emission = result.record_set.records["clean.py"]
    assert [node.node_id for node in emission.descendants] == ["n:clean"]
    assert len(emission.edges) == 1
    assert emission.edges[0].attrs == {"note": f"x{RC}y"}
    assert not value_contains_nul(result.record_set.to_cursor())


def test_identity_nul_fields_from_kwargs_names_offenders():
    assert identity_nul_fields_from(
        node_id="ok",
        subtype="ok",
        dedupe_key=f"d{NUL}",
        source_record_id={"id": f"x{NUL}"},
        source_revision=None,
    ) == ("dedupe_key", "source_record_id")
    assert identity_nul_fields_from(node_id="clean") == ()


def test_sanitize_report_fields_surfaces_only_nonzero_counts():
    class _Zero:
        sanitized_value_count = 0
        rejected_fact_count = 0

    class _Hot:
        sanitized_value_count = 3
        rejected_fact_count = 2
        sanitize_first_rejected_fact = "node:concept:n\\x00"

    assert _sanitize_report_fields(_Zero()) == {}
    assert _sanitize_report_fields(object()) == {}
    assert _sanitize_report_fields(_Hot()) == {
        "sanitized_value_count": 3,
        "rejected_fact_count": 2,
        "sanitize_first_rejected_fact": "node:concept:n\\x00",
    }


# ---------------------------------------------------------------------------
# End-to-end: runner admission path (real Postgres + publish).
# ---------------------------------------------------------------------------


class _NulAttrsAdapter:
    """code_structure-style stream: one fact carries NUL inside a
    docstring-style attrs string (the #638 shape); the rest are
    clean. Also one edge with NUL in an attr value."""

    name = "nul-attrs"
    profile = "discovery_crawl"

    def run(self, run_id: str, *, mode: str = "cursor") -> SourceRun:
        del run_id
        facts = [
            NodeFact(
                node_id="n:clean",
                subtype="concept",
                attrs={"text": "def clean(): ..."},
                source_record_id={"id": "n:clean"},
                source_revision={"v": 1},
            ),
            NodeFact(
                node_id="n:nul-doc",
                subtype="concept",
                attrs={
                    "text": f"def f():\n    '''doc{NUL}string'''",
                    "docstring": f"embedded{NUL}nul{NUL}twice",
                },
                source_record_id={"id": "n:nul-doc"},
                source_revision={"v": 1},
            ),
            EdgeFact(
                src="n:clean",
                dst="n:nul-doc",
                edge_type="links_to",
                attrs={"note": f"edge{NUL}attr"},
                source_record_id={"id": "e:1"},
                source_revision={"v": 1},
            ),
        ]
        return SourceRun(
            facts=facts, completed_scope=True,
            next_cursor=None, run_mode=mode,
        )


class _NulIdentityAdapter:
    """Stream mixing clean facts with facts whose identity-bearing
    fields carry NUL. The NUL-identity facts must be rejected
    (skipped + counted) while the clean ones land and publish."""

    name = "nul-identity"
    profile = "discovery_crawl"

    def run(self, run_id: str, *, mode: str = "cursor") -> SourceRun:
        del run_id
        facts = [
            NodeFact(
                node_id="n:a",
                subtype="concept",
                attrs={"text": "a"},
                source_record_id={"id": "n:a"},
                source_revision={"v": 1},
            ),
            # NUL in source_record_id — identity, never rewritten.
            NodeFact(
                node_id="n:bad-record",
                subtype="concept",
                attrs={"text": "b"},
                source_record_id={"id": f"x{NUL}y"},
                source_revision={"v": 1},
            ),
            # NUL in node_id — identity, never rewritten.
            NodeFact(
                node_id=f"n:{NUL}bad-id",
                subtype="concept",
                attrs={"text": "c"},
                source_record_id={"id": "n:bad-id"},
                source_revision={"v": 1},
            ),
            # NUL in dst — edge identity, never rewritten.
            EdgeFact(
                src="n:a",
                dst=f"n:{NUL}bad-id",
                edge_type="links_to",
                attrs={},
                source_record_id={"id": "e:bad"},
                source_revision={"v": 1},
            ),
            NodeFact(
                node_id="n:d",
                subtype="concept",
                attrs={"text": "d"},
                source_record_id={"id": "n:d"},
                source_revision={"v": 1},
            ),
        ]
        return SourceRun(
            facts=facts, completed_scope=True,
            next_cursor=None, run_mode=mode,
        )


class _NulRecordSetAdapter:
    """Record-set authority mirrors one clean and one rejected fact."""

    name = "nul-record-set"
    profile = "discovery_crawl"

    def run(self, run_id: str, *, mode: str = "cursor") -> SourceRun:
        del run_id
        clean_record = {"path": "fixture.py", "name": "clean"}
        bad_record = {"path": "fixture.py", "name": f"bad{NUL}"}
        clean = NodeFact(
            node_id="n:record-set-clean",
            subtype="concept",
            attrs={"text": "clean"},
            source_record_id=clean_record,
            source_revision={"v": 1},
        )
        bad = NodeFact(
            node_id="n:record-set-bad",
            subtype="concept",
            attrs={"text": "bad identity"},
            source_record_id=bad_record,
            source_revision={"v": 1},
        )
        record_set = RecordSet(records={
            "fixture.py": RecordEmission(descendants=[
                RecordNode(
                    clean.node_id, clean.subtype,
                    clean.source_record_id, clean.source_revision,
                ),
                RecordNode(
                    bad.node_id, bad.subtype,
                    bad.source_record_id, bad.source_revision,
                ),
            ]),
        })
        return SourceRun(
            facts=[clean, bad],
            completed_scope=True,
            next_cursor={"files": {"fixture.py": {"sha": "1"}}},
            run_mode=mode,
            record_set=record_set,
        )


def _checkout(conn: psycopg.Connection, generation_id: str):
    """Checkout the PUBLISHED chronos checkpoint for a cycle().

    ``Publisher.cycle()`` returns the opaque chronos checkpoint
    string as ``generation_id`` (``okg:...``); that string — not the
    int ``compat_generation_id`` — names the committed checkpoint.
    """
    return ChronosGraphStore.from_connection(
        conn, ensure_schema=False,
    ).checkout_generation(generation_id)


@pytest.mark.postgres
def test_nul_file_publishes(clean_runtime: str) -> None:
    """req.sanitize.source-survives (runner path): a NUL-bearing
    docstring stream runs through REAL runner admission, the
    generation publishes with node_count > 0, the stored value
    carries U+FFFD (queried back), and the per-source report shows
    sanitized_value_count >= 1."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(
            conn,
            subtypes=["concept"],
            narrowings=[("concept", "links_to", "concept")],
        )

    result = SourceRunner(
        clean_runtime, source_name="test.nul-attrs",
    ).run(_NulAttrsAdapter(), mode="scope_complete")

    assert result.nodes_appended == 2, result.as_dict()
    assert result.edges_appended == 1
    # 1 NUL in text + 2 in docstring + 1 in the edge attr.
    assert result.sanitized_value_count == 4
    assert result.rejected_fact_count == 0

    # Per-source report plumbing: RunResult.as_dict and the ingest
    # service report fields both surface the nonzero count.
    assert result.as_dict()["sanitized_value_count"] == 4
    assert _sanitize_report_fields(result) == {
        "sanitized_value_count": 4,
    }

    out = Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()
    assert out["status"] == "published", out
    assert out["compat_generation_id"] > 0
    generation = out["generation_id"]

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        graph = _checkout(conn, generation)
        node_count = graph.query(
            "SELECT count(*) AS n FROM okg.graph_nodes "
            "WHERE source = 'test.nul-attrs'"
        )[0]["n"]
        rows = graph.query(
            "SELECT attrs FROM okg.graph_nodes "
            "WHERE node_id = :node_id",
            {"node_id": "n:nul-doc"},
        )
    assert node_count == 2
    text = rows[0]["attrs"]["text"]
    docstring = rows[0]["attrs"]["docstring"]
    assert NUL not in text and NUL not in docstring
    assert text == f"def f():\n    '''doc{RC}string'''"
    assert docstring == f"embedded{RC}nul{RC}twice"


@pytest.mark.postgres
def test_nul_identity_rejected_rest_of_run_publishes(
    clean_runtime: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """req.sanitize.nul-scrub (identity rule): facts with NUL in
    identity-bearing fields are rejected loudly (counted, named in
    the report, warned with the source name) while the remaining
    facts land and publish."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(
            conn,
            subtypes=["concept"],
            narrowings=[("concept", "links_to", "concept")],
        )

    with caplog.at_level(logging.WARNING, logger="okg.substrate.ingest.runner"):
        result = SourceRunner(
            clean_runtime, source_name="test.nul-identity",
        ).run(_NulIdentityAdapter(), mode="scope_complete")

    assert result.nodes_appended == 2, result.as_dict()
    assert result.edges_appended == 0
    assert result.rejected_fact_count == 3
    assert result.sanitized_value_count == 0
    assert result.sanitize_first_rejected_fact is not None
    assert "source_record_id" in " ".join(
        record.getMessage() for record in caplog.records
    )
    warning_text = " ".join(
        record.getMessage()
        for record in caplog.records
        if record.levelno >= logging.WARNING
    )
    assert "test.nul-identity" in warning_text
    assert "rejecting fact with NUL" in warning_text
    assert "rejected_fact_count=3" in warning_text

    # Report plumbing: nonzero rejected count is surfaced and named.
    report = _sanitize_report_fields(result)
    assert report["rejected_fact_count"] == 3
    assert "sanitize_first_rejected_fact" in report
    assert result.as_dict()["rejected_fact_count"] == 3

    out = Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()
    assert out["status"] == "published", out

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        graph = _checkout(conn, out["generation_id"])
        rows = graph.query(
            "SELECT node_id FROM okg.graph_nodes "
            "WHERE source = 'test.nul-identity' ORDER BY node_id"
        )
    assert [row["node_id"] for row in rows] == ["n:a", "n:d"]


@pytest.mark.postgres
def test_nul_identity_is_removed_from_record_set_before_cursor_persistence(
    clean_runtime: str,
) -> None:
    """A rejected fact cannot re-enter JSONB through record-set authority."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["concept"], narrowings=[])

    result = SourceRunner(
        clean_runtime, source_name="test.nul-record-set",
    ).run(_NulRecordSetAdapter(), mode="scope_complete")

    assert result.nodes_appended == 1
    assert result.rejected_fact_count == 1
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        cursor = conn.execute(
            "SELECT last_applied_rev FROM okg.source_watermarks "
            "WHERE source = %s",
            ("test.nul-record-set",),
        ).fetchone()[0]
    assert not value_contains_nul(cursor)
    record_set = RecordSet.from_cursor(cursor["record_set"])
    nodes = record_set.records["fixture.py"].nodes_by_id()
    assert set(nodes) == {"n:record-set-clean"}


@pytest.mark.postgres
def test_regression_original_untranslatable_character_is_dead(
    clean_runtime: str,
) -> None:
    """Regression pin for issue #638: the raw write layer still
    cannot store NUL (proving the test would fail without the
    admission scrub), and the SAME pre-fix fact stream through
    runner admission no longer raises UntranslatableCharacter."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(
            conn,
            subtypes=["concept"],
            narrowings=[("concept", "links_to", "concept")],
        )

    # The disease is still real at the persistence layer: a NUL-
    # bearing jsonb write aborts (pre-fix, admission forwarded the
    # value verbatim into exactly this failure).
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with pytest.raises(psycopg.DataError):
            conn.execute(
                "SELECT %s::jsonb",
                (Jsonb({"text": f"doc{NUL}string"}),),
            )

    # Post-fix: the same stream through REAL admission completes.
    result = SourceRunner(
        clean_runtime, source_name="test.nul-attrs-regression",
    ).run(_NulAttrsAdapter(), mode="scope_complete")
    assert result.nodes_appended == 2
    assert result.sanitized_value_count == 4


# ---------------------------------------------------------------------------
# End-to-end: enricher emit path (real Postgres + publish).
# ---------------------------------------------------------------------------


class _MarkdownEnricherWithAuthority(MarkdownStructuralEnricher):
    """The REAL markdown structural enricher plus a declared
    producer policy (what a deployment manifest's
    ``derived_producers`` section declares in production).

    Needed because the AUTO-derived profile (from
    ``requires_narrowings``) resolves edge endpoint subtypes from
    the chronos graph; a first-emission heading node is not in the
    graph yet, so its ``contains`` edge would be rejected
    (``producer_edge_triple_not_allowed``) and the run deny-listed
    from projection. ``dst_subtype_optional`` is the substrate's
    documented discoverability relaxation for producers whose edge
    endpoints are same-run new nodes — the narrowing trigger at
    projection still enforces the catalog. Unrelated to the NUL
    sanitize under test.
    """

    producer_policy = {
        "trust_label": "derived-deterministic-trusted",
        "admission_mode": "fast_track",
        "authority_scope": {
            "source_family": "_enricher:markdown_structural",
        },
        "allowed_record_kinds": ["node", "edge"],
        "output_signature": {
            "nodes": [{"subtype": "markdown_heading"}],
            "edges": [
                {
                    "src_subtype": "source_file",
                    "edge_type": "contains",
                    "dst_subtype": "markdown_heading",
                    "dst_subtype_optional": True,
                },
                {
                    "src_subtype": "markdown_heading",
                    "edge_type": "contains",
                    "dst_subtype": "markdown_heading",
                    "src_subtype_optional": True,
                    "dst_subtype_optional": True,
                },
            ],
        },
    }


class _SourceFileAdapter:
    """Emits one source_file node pointing at a real on-disk file."""

    name = "files"
    profile = "discovery_crawl"

    def __init__(self, *, repo: str, path: str) -> None:
        self.repo = repo
        self.path = path

    def run(self, run_id: str, *, mode: str = "cursor") -> SourceRun:
        del run_id
        facts = [
            NodeFact(
                node_id=f"file:{self.repo}:{self.path}",
                subtype="source_file",
                attrs={"repo": self.repo, "path": self.path},
                source_record_id={"id": f"file:{self.repo}:{self.path}"},
                source_revision={"v": 1},
            ),
        ]
        return SourceRun(
            facts=facts, completed_scope=True,
            next_cursor=None, run_mode=mode,
        )


@pytest.mark.postgres
def test_enricher_markdown_nul_heading_publishes(
    clean_runtime: str, tmp_path,
) -> None:
    """req.sanitize.source-survives (enricher path): a NUL-bearing
    markdown heading flows through the structural enricher's emit
    path (which bypasses the runner) to a published generation with
    the scrubbed value queryable. The enricher runs INSIDE
    Publisher.cycle() — the production wiring — so its emitted
    facts are projected into the same generation's graph."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(
            conn,
            subtypes=["source_file", "markdown_heading"],
            narrowings=[
                ("source_file", "contains", "markdown_heading"),
                ("markdown_heading", "contains", "markdown_heading"),
            ],
        )

    md_path = tmp_path / "guide.md"
    md_path.write_text(
        f"# Top{NUL}Heading\n\nbody text\n", encoding="utf-8",
    )

    result = SourceRunner(
        clean_runtime, source_name="test.files",
    ).run(
        _SourceFileAdapter(repo="r", path="guide.md"),
        mode="scope_complete",
    )
    assert result.nodes_appended == 1

    # Input-trust fixture for the trust lattice (same pattern as
    # test_derived_producer_authority): the enricher's derived
    # output must not exceed its input source's trust, and a source
    # without a materialized profile counts as untrusted.
    from okg.substrate.producer_authority import (
        materialize_producer_profile,
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        materialize_producer_profile(
            conn,
            deployment_name="default",
            producer_id="test.files",
            producer_kind="source",
            trust_label="repo-local-trusted",
            admission_mode="fast_track",
            authority_scope={"source_family": "test.files"},
            allowed_record_kinds=(),
            output_signature={},
            policy_clauses=(),
            output_scope_summary={"summary": "input trust fixture"},
            source_ref={"fixture": "test_ingest_sanitize"},
            catalog_version_id=None,
        )

    out = Publisher(
        clean_runtime,
        "default",
        enrichers=[_MarkdownEnricherWithAuthority(repo_root=tmp_path)],
        catalog_identity=bind_test_catalog_writer(clean_runtime, "default"),
    ).cycle()
    assert out["status"] == "published", out

    # The emitted heading fact landed in node_facts with the NUL
    # scrubbed (pre-fix this INSERT raised UntranslatableCharacter
    # and sank the publish).
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        row = conn.execute(
            "SELECT attrs->>'text' FROM okg.node_facts "
            "WHERE source = '_enricher:markdown_structural' "
            "  AND subtype = 'markdown_heading'"
        ).fetchone()
    assert row is not None
    assert row[0] == f"Top{RC}Heading"
    assert NUL not in row[0]

    # And the published generation serves the scrubbed heading.
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        graph = _checkout(conn, out["generation_id"])
        rows = graph.query(
            "SELECT attrs FROM okg.graph_nodes "
            "WHERE subtype = 'markdown_heading'"
        )
    assert len(rows) == 1
    assert rows[0]["attrs"]["text"] == f"Top{RC}Heading"


@pytest.mark.postgres
def test_emit_node_fact_scrubs_attrs_at_exact_write_path(
    clean_runtime: str,
) -> None:
    """Direct _fact_emitters-level coverage: the exact node_facts
    write path stores the scrubbed value (pre-fix this call raised
    UntranslatableCharacter on the jsonb attrs)."""
    run_id = str(uuid.uuid4())
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        outcome = emit_node_fact(
            conn,
            enricher_name="sanitize-test",
            node_id="mdheading:abc",
            subtype="markdown_heading",
            attrs={"text": f"Bad{NUL}Heading", "depth": 1},
            dedupe_key="sanitize-test:node:1",
            source_record_id={"enricher": "sanitize-test", "id": "1"},
            source_revision={"run_id": run_id},
            run_id=run_id,
            observed_at=datetime.now(timezone.utc),
            derived_from=["input:1"],
            support_policy="all_members",
        )
        assert outcome.emitted is True
        assert outcome.sanitized_value_count == 1
        row = conn.execute(
            "SELECT attrs->>'text' FROM okg.node_facts "
            "WHERE node_id = 'mdheading:abc'"
        ).fetchone()
    assert row[0] == f"Bad{RC}Heading"


@pytest.mark.postgres
def test_emitters_reject_nul_identity_and_insert_nothing(
    clean_runtime: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Enricher-path identity rule: NUL in an identity-bearing field
    rejects the emit loudly — no row is inserted, the outcome names
    the offending fields, and a warning names the enricher."""
    run_id = str(uuid.uuid4())
    observed_at = datetime.now(timezone.utc)
    logger_name = "okg.substrate.library.enrichers._fact_emitters"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with caplog.at_level(logging.WARNING, logger=logger_name):
            node_outcome = emit_node_fact(
                conn,
                enricher_name="sanitize-test",
                node_id=f"mdheading:{NUL}bad",
                subtype="markdown_heading",
                attrs={"text": "irrelevant"},
                dedupe_key="sanitize-test:node:bad",
                source_record_id={"id": "bad"},
                source_revision={"run_id": run_id},
                run_id=run_id,
                observed_at=observed_at,
                derived_from=["input:1"],
                support_policy="all_members",
            )
            edge_outcome = emit_edge_fact(
                conn,
                enricher_name="sanitize-test",
                src="n:ok",
                dst=f"n:{NUL}bad",
                edge_type="contains",
                attrs={},
                provenance="derived_deterministic",
                confidence=1.0,
                dedupe_key="sanitize-test:edge:bad",
                source_record_id={"id": "bad-edge"},
                source_revision={"run_id": run_id},
                run_id=run_id,
                observed_at=observed_at,
            )
        node_rows = conn.execute(
            "SELECT count(*) FROM okg.node_facts "
            "WHERE source = '_enricher:sanitize-test'"
        ).fetchone()[0]
        edge_rows = conn.execute(
            "SELECT count(*) FROM okg.edge_facts "
            "WHERE source = '_enricher:sanitize-test'"
        ).fetchone()[0]

    assert node_outcome.emitted is False
    assert node_outcome.rejected_fields == ("node_id",)
    assert edge_outcome.emitted is False
    assert edge_outcome.rejected_fields == ("dst",)
    assert node_rows == 0
    assert edge_rows == 0
    warning_text = " ".join(
        record.getMessage() for record in caplog.records
    )
    assert "sanitize-test" in warning_text
    assert "NUL (U+0000) in identity field(s)" in warning_text


# ---------------------------------------------------------------------------
# Bypass-path follow-up (adversarial review of the PACT): op is
# identity; the runner warning budget is per RUN; ProgressMarkers get
# the rejection rule.
# ---------------------------------------------------------------------------


def test_op_field_is_identity_rejected():
    """`op` flips insert/retract semantics — rewriting it would alias
    two different fact meanings, so it gets the rejection rule."""
    node = _node(op=f"I{NUL}")
    assert identity_nul_fields(node) == ("op",)
    assert sanitize_fact(node).rejected
    edge = _edge(op=f"R{NUL}")
    assert identity_nul_fields(edge) == ("op",)
    assert sanitize_fact(edge).rejected


@pytest.mark.postgres
def test_emitters_reject_nul_op(clean_runtime: str) -> None:
    run_id = str(uuid.uuid4())
    observed_at = datetime.now(timezone.utc)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        outcome = emit_node_fact(
            conn,
            enricher_name="sanitize-test",
            node_id="mdheading:op-bad",
            subtype="markdown_heading",
            attrs={"text": "clean"},
            dedupe_key="sanitize-test:node:op-bad",
            source_record_id={"id": "op-bad"},
            source_revision={"run_id": run_id},
            run_id=run_id,
            observed_at=observed_at,
            op=f"I{NUL}",
            support_policy="all_members",
        )
        edge_outcome = emit_edge_fact(
            conn,
            enricher_name="sanitize-test",
            src="n:op-ok",
            dst="n:op-ok2",
            edge_type="contains",
            attrs={},
            provenance="derived_deterministic",
            confidence=1.0,
            dedupe_key="sanitize-test:edge:op-bad",
            source_record_id={"id": "op-bad-edge"},
            source_revision={"run_id": run_id},
            run_id=run_id,
            observed_at=observed_at,
            op=f"R{NUL}",
        )
        rows = conn.execute(
            "SELECT count(*) FROM okg.node_facts "
            "WHERE node_id = 'mdheading:op-bad'"
        ).fetchone()[0]
        edge_rows = conn.execute(
            "SELECT count(*) FROM okg.edge_facts "
            "WHERE dedupe_key = 'sanitize-test:edge:op-bad'"
        ).fetchone()[0]
    assert outcome.emitted is False
    assert outcome.rejected_fields == ("op",)
    assert rows == 0
    assert edge_outcome.emitted is False
    assert edge_outcome.rejected_fields == ("op",)
    assert edge_rows == 0


def test_sanitize_warn_budget_is_per_run_not_per_chunk(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Pre-fix, the 5-line rejection-detail cap reset every chunk, so
    a pathological many-chunk stream still flooded the log. One
    budget object now spans the run: 5 detail lines + 1 suppression
    notice TOTAL, however many chunks flush."""
    from okg.substrate.ingest.runner import (
        SourceRunner,
        _SanitizeWarnBudget,
    )

    runner = SourceRunner(
        "postgresql://unused:unused@127.0.0.1:1/unused",
        source_name="test.budget",
    )
    budget = _SanitizeWarnBudget()

    def _bad_facts(count: int, start: int) -> list[NodeFact]:
        return [
            NodeFact(
                node_id=f"n:{NUL}bad-{start + i}",
                subtype="concept",
                attrs={},
                source_record_id={"id": str(start + i)},
                source_revision={"v": 1},
            )
            for i in range(count)
        ]

    logger_name = "okg.substrate.ingest.runner"
    with caplog.at_level(logging.WARNING, logger=logger_name):
        # Chunk 1: 7 rejects → 5 detail lines + 1 one-time notice.
        out1, summary1 = runner._sanitize_facts(
            _bad_facts(7, 0), warn_budget=budget,
        )
        # Chunk 2 of the SAME run: 3 more rejects → no new detail,
        # no second notice.
        out2, summary2 = runner._sanitize_facts(
            _bad_facts(3, 100), warn_budget=budget,
        )

    assert out1 == [] and out2 == []
    assert summary1.rejected_fact_count == 7
    assert summary2.rejected_fact_count == 3
    detail_lines = [
        r for r in caplog.records
        if "rejecting fact with NUL" in r.getMessage()
    ]
    notice_lines = [
        r for r in caplog.records
        if "detail suppressed for this run" in r.getMessage()
    ]
    assert len(detail_lines) == 5
    assert len(notice_lines) == 1

    # A fresh call WITHOUT a threaded budget (standalone use) still
    # gets its own per-call budget.
    caplog.clear()
    with caplog.at_level(logging.WARNING, logger=logger_name):
        runner._sanitize_facts(_bad_facts(1, 200))
    assert any(
        "rejecting fact with NUL" in r.getMessage()
        for r in caplog.records
    )


class _NulMarkerAdapter:
    """Stream with ProgressMarkers: one clean, one with NUL in
    record_id — progress keys are resume identity, so the NUL one
    must be rejected (skipped + counted) while the run completes and
    the clean marker commits."""

    name = "nul-markers"
    profile = "discovery_crawl"

    def run(self, run_id: str, *, mode: str = "cursor") -> SourceRun:
        del run_id
        from okg.substrate.library.sources.base import ProgressMarker

        facts = [
            NodeFact(
                node_id="n:m1",
                subtype="concept",
                attrs={"text": "clean"},
                source_record_id={"id": "n:m1"},
                source_revision={"v": 1},
            ),
            ProgressMarker(record_id="r-ok", fingerprint="fp-ok"),
            ProgressMarker(record_id=f"r{NUL}bad", fingerprint="fp-bad"),
        ]
        return SourceRun(
            facts=facts, completed_scope=True,
            next_cursor=None, run_mode=mode,
        )


@pytest.mark.postgres
def test_progress_marker_nul_rejected_run_completes(
    clean_runtime: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["concept"], narrowings=[])

    with caplog.at_level(
        logging.WARNING, logger="okg.substrate.ingest.runner",
    ):
        result = SourceRunner(
            clean_runtime,
            source_name="test.nul-markers",
            deployment_name="default",
        ).run(_NulMarkerAdapter(), mode="scope_complete")

    assert result.nodes_appended == 1, result.as_dict()
    assert result.rejected_marker_count == 1
    assert result.sanitize_first_rejected_marker is not None
    assert result.as_dict()["rejected_marker_count"] == 1
    assert _sanitize_report_fields(result)["rejected_marker_count"] == 1
    assert any(
        "rejected 1 progress marker" in r.getMessage()
        for r in caplog.records
    )

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        rows = conn.execute(
            "SELECT key, value FROM okg.source_progress "
            "WHERE source_name = 'test.nul-markers'"
        ).fetchall()
    keys = {row[0] for row in rows}
    assert "record:r-ok" in keys
    assert all(NUL not in key for key in keys)
