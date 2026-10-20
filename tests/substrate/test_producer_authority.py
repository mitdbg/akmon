from __future__ import annotations

import json
import uuid
from types import MappingProxyType

import psycopg
import pytest

from okg.substrate.producer_authority import (
    ProducerPolicyViolation,
    ProducerProfile,
    compare_trust_labels,
    evaluate_edge_fact_range_signature,
    evaluate_payload_signature,
    evaluate_source_run_signature,
    least_trusted_label,
    load_profile_for_producer,
    materialize_producer_profile,
    normalize_trust_label,
    policy_from_authority,
    trust_label_allows_output,
    validate_signature_catalog,
)


def test_signature_evaluator_allows_matching_node() -> None:
    evidence = evaluate_payload_signature(
        profile=_profile({
            "nodes": [
                {
                    "subtype": "source_file",
                    "id_prefix": "file:repo:",
                    "attrs": {"repo": "repo"},
                    "clause_id": "allow_source_file",
                },
            ],
        }),
        nodes=[
            {
                "node_id": "file:repo:README.md",
                "subtype": "source_file",
                "attrs": {"repo": "repo", "path": "README.md"},
            },
        ],
        edges=[],
    )

    assert evidence["producer_policy"]["status"] == "matched"
    assert evidence["producer_policy"]["matched_clause_ids"] == [
        "allow_source_file",
    ]


def test_trust_label_lattice_orders_and_rejects_unknown_labels() -> None:
    assert normalize_trust_label(" Trusted ") == "trusted"
    assert compare_trust_labels("untrusted", "trusted") < 0
    assert compare_trust_labels("derived-compat", "review-trusted") < 0
    assert compare_trust_labels(
        "derived-deterministic-trusted", "repo-local-trusted"
    ) == 0

    assert least_trusted_label([
        "trusted",
        "review-trusted",
        "untrusted",
    ]) == "untrusted"
    assert trust_label_allows_output(
        output_label="untrusted",
        input_labels=["repo-local-trusted", "untrusted"],
    )
    assert not trust_label_allows_output(
        output_label="trusted",
        input_labels=["repo-local-trusted", "less-trusted"],
    )

    with pytest.raises(ValueError, match="trust_label must be one of"):
        normalize_trust_label("mystery-trusted")


def test_signature_evaluator_rejects_node_subtype() -> None:
    with pytest.raises(ProducerPolicyViolation) as exc:
        evaluate_payload_signature(
            profile=_profile({
                "nodes": [{"subtype": "source_file"}],
            }),
            nodes=[{
                "node_id": "doc:1",
                "subtype": "document",
                "attrs": {},
            }],
            edges=[],
        )

    assert exc.value.code == "producer_node_subtype_not_allowed"


def test_signature_evaluator_rejects_node_id_prefix() -> None:
    with pytest.raises(ProducerPolicyViolation) as exc:
        evaluate_payload_signature(
            profile=_profile({
                "nodes": [
                    {"subtype": "source_file", "id_prefix": "file:repo:"},
                ],
            }),
            nodes=[{
                "node_id": "file:other:README.md",
                "subtype": "source_file",
                "attrs": {},
            }],
            edges=[],
        )

    assert exc.value.code == "producer_node_subtype_not_allowed"


def test_signature_evaluator_rejects_attrs_containment_mismatch() -> None:
    with pytest.raises(ProducerPolicyViolation) as exc:
        evaluate_payload_signature(
            profile=_profile({
                "nodes": [
                    {
                        "subtype": "source_file",
                        "attrs": {"repo": "repo"},
                    },
                ],
            }),
            nodes=[{
                "node_id": "file:repo:README.md",
                "subtype": "source_file",
                "attrs": {"repo": "other"},
            }],
            edges=[],
        )

    assert exc.value.code == "producer_node_subtype_not_allowed"


def test_signature_evaluator_exact_attrs_rejects_additional_node_attribute() -> None:
    profile = _profile({
        "nodes": [
            {
                "subtype": "source_file",
                "attrs": {"repo": "repo"},
                "attrs_exact": True,
                "attr_keys": ["repo"],
            },
        ],
    })
    assert evaluate_payload_signature(
        profile=profile,
        nodes=[{
            "node_id": "file:repo:README.md",
            "subtype": "source_file",
            "attrs": {"repo": "repo"},
        }],
        edges=[],
    )["producer_policy"]["status"] == "matched"

    with pytest.raises(ProducerPolicyViolation) as exc:
        evaluate_payload_signature(
            profile=profile,
            nodes=[{
                "node_id": "file:repo:README.md",
                "subtype": "source_file",
                "attrs": {"repo": "repo", "unexpected": "not-declared"},
            }],
            edges=[],
        )
    assert exc.value.code == "producer_node_subtype_not_allowed"


def test_signature_evaluator_exact_attrs_rejects_additional_edge_attribute() -> None:
    profile = _profile(
        {
            "nodes": [
                {"subtype": "document"},
                {"subtype": "source_file"},
            ],
            "edges": [
                {
                    "src_subtype": "document",
                    "edge_type": "references",
                    "dst_subtype": "source_file",
                    "attrs": {"role": "authority_reference"},
                    "attrs_exact": True,
                    "attr_keys": ["role", "target_id"],
                },
            ],
        },
        allowed_record_kinds=("node", "edge"),
    )
    nodes = [
        {"node_id": "doc:1", "subtype": "document", "attrs": {}},
        {"node_id": "file:1", "subtype": "source_file", "attrs": {}},
    ]
    assert evaluate_payload_signature(
        profile=profile,
        nodes=nodes,
        edges=[{
            "src": "doc:1",
            "edge_type": "references",
            "dst": "file:1",
            "attrs": {"role": "authority_reference", "target_id": "dynamic-1"},
        }],
    )["producer_policy"]["status"] == "matched"

    invalid_attrs = (
        {"role": "authority_reference"},
        {"role": "changed", "target_id": "dynamic-1"},
        {
            "role": "authority_reference",
            "target_id": "dynamic-1",
            "identity_link": True,
        },
    )
    for attrs in invalid_attrs:
        with pytest.raises(ProducerPolicyViolation) as exc:
            evaluate_payload_signature(
                profile=profile,
                nodes=nodes,
                edges=[{
                    "src": "doc:1",
                    "edge_type": "references",
                    "dst": "file:1",
                    "attrs": attrs,
                }],
            )
        assert exc.value.code == "producer_edge_triple_not_allowed"


@pytest.mark.parametrize("malformed", ["true", 1, ["true"]])
def test_signature_evaluator_rejects_malformed_exact_attrs_flag(
    malformed: object,
) -> None:
    with pytest.raises(ValueError, match="attrs_exact must be a bool"):
        policy_from_authority(
            {
                "producer_policy": {
                    "output_signature": {
                        "nodes": [
                            {
                                "subtype": "source_file",
                                "attrs": {"repo": "repo"},
                                "attrs_exact": malformed,
                            },
                        ],
                    },
                },
            },
            producer_id="review.fixture",
            producer_kind="human",
        )


def test_signature_evaluator_allows_matching_edge_triple() -> None:
    evidence = evaluate_payload_signature(
        profile=_profile(
            {
                "nodes": [
                    {"subtype": "document"},
                    {"subtype": "source_file"},
                ],
                "edges": [
                    {
                        "src_subtype": "document",
                        "edge_type": "references",
                        "dst_subtype": "source_file",
                        "clause_id": "allow_doc_ref_file",
                    },
                ],
            },
            allowed_record_kinds=("node", "edge"),
        ),
        nodes=[
            {"node_id": "doc:1", "subtype": "document", "attrs": {}},
            {
                "node_id": "file:repo:a.py",
                "subtype": "source_file",
                "attrs": {},
            },
        ],
        edges=[
            {
                "src": "doc:1",
                "edge_type": "references",
                "dst": "file:repo:a.py",
                "attrs": {},
            },
        ],
    )

    assert "allow_doc_ref_file" in (
        evidence["producer_policy"]["matched_clause_ids"]
    )
    assert evidence["catalog"]["narrowings"] == [
        {
            "src_subtype": "document",
            "edge_type": "references",
            "dst_subtype": "source_file",
            "status": "matched",
        },
    ]


def test_signature_evaluator_rejects_edge_triple() -> None:
    with pytest.raises(ProducerPolicyViolation) as exc:
        evaluate_payload_signature(
            profile=_profile(
                {
                    "nodes": [
                        {"subtype": "document"},
                        {"subtype": "source_file"},
                    ],
                    "edges": [
                        {
                            "src_subtype": "document",
                            "edge_type": "references",
                            "dst_subtype": "source_file",
                        },
                    ],
                },
                allowed_record_kinds=("node", "edge"),
            ),
            nodes=[
                {
                    "node_id": "file:repo:a.py",
                    "subtype": "source_file",
                    "attrs": {},
                },
                {"node_id": "doc:1", "subtype": "document", "attrs": {}},
            ],
            edges=[
                {
                    "src": "file:repo:a.py",
                    "edge_type": "references",
                    "dst": "doc:1",
                    "attrs": {},
                },
            ],
        )

    assert exc.value.code == "producer_edge_triple_not_allowed"


def test_signature_evaluator_admits_optional_dst_when_resolvable() -> None:
    """dst_subtype_optional=True is permissive about discoverability but
    still strict about subtype identity when the dst IS resolvable."""
    evidence = evaluate_payload_signature(
        profile=_profile(
            {
                "nodes": [
                    {"subtype": "git_commit"},
                    {"subtype": "source_file"},
                ],
                "edges": [
                    {
                        "src_subtype": "git_commit",
                        "edge_type": "affects",
                        "dst_subtype": "source_file",
                        "dst_subtype_optional": True,
                        "clause_id": "commit_affects_file",
                    },
                ],
            },
            allowed_record_kinds=("node", "edge"),
        ),
        nodes=[
            {
                "node_id": "commit:repo:abc",
                "subtype": "git_commit",
                "attrs": {},
            },
            {
                "node_id": "file:repo:a.py",
                "subtype": "source_file",
                "attrs": {},
            },
        ],
        edges=[
            {
                "src": "commit:repo:abc",
                "edge_type": "affects",
                "dst": "file:repo:a.py",
                "attrs": {},
            },
        ],
    )

    assert "commit_affects_file" in (
        evidence["producer_policy"]["matched_clause_ids"]
    )


def test_signature_evaluator_rejects_optional_dst_when_resolved_mismatch() -> None:
    """dst_subtype_optional=True does not weaken subtype identity: a
    dst that resolves to a different subtype than declared still
    rejects. dst_subtype is supplied directly on the edge to focus on
    matcher behavior without tripping node admission.
    """
    with pytest.raises(ProducerPolicyViolation) as exc:
        evaluate_payload_signature(
            profile=_profile(
                {
                    "nodes": [{"subtype": "git_commit"}],
                    "edges": [
                        {
                            "src_subtype": "git_commit",
                            "edge_type": "affects",
                            "dst_subtype": "source_file",
                            "dst_subtype_optional": True,
                            "clause_id": "commit_affects_file",
                        },
                    ],
                },
                allowed_record_kinds=("node", "edge"),
            ),
            nodes=[
                {
                    "node_id": "commit:repo:abc",
                    "subtype": "git_commit",
                    "attrs": {},
                },
            ],
            edges=[
                {
                    "src": "commit:repo:abc",
                    "src_subtype": "git_commit",
                    "edge_type": "affects",
                    "dst": "symbol:repo:foo",
                    "dst_subtype": "code_symbol",
                    "attrs": {},
                },
            ],
        )

    assert exc.value.code == "producer_edge_triple_not_allowed"


def test_signature_evaluator_admits_optional_dst_when_unresolvable() -> None:
    """An edge whose dst is not in the nodes list (so dst_subtype
    resolves to None) admits if the rule sets dst_subtype_optional=True.
    """
    evidence = evaluate_payload_signature(
        profile=_profile(
            {
                "nodes": [{"subtype": "git_commit"}],
                "edges": [
                    {
                        "src_subtype": "git_commit",
                        "edge_type": "affects",
                        "dst_subtype": "source_file",
                        "dst_subtype_optional": True,
                        "clause_id": "commit_affects_file",
                    },
                ],
            },
            allowed_record_kinds=("node", "edge"),
        ),
        nodes=[
            {
                "node_id": "commit:repo:abc",
                "subtype": "git_commit",
                "attrs": {},
            },
        ],
        edges=[
            {
                "src": "commit:repo:abc",
                "edge_type": "affects",
                "dst": "file:repo:renamed-into-archive.md",
                "attrs": {},
            },
        ],
    )

    assert "commit_affects_file" in (
        evidence["producer_policy"]["matched_clause_ids"]
    )


def test_signature_evaluator_rejects_unresolvable_dst_without_optional_flag() -> None:
    """Existing strict behavior: an unresolvable dst rejects when the
    rule does not opt into dst_subtype_optional. Regression coverage."""
    with pytest.raises(ProducerPolicyViolation) as exc:
        evaluate_payload_signature(
            profile=_profile(
                {
                    "nodes": [{"subtype": "git_commit"}],
                    "edges": [
                        {
                            "src_subtype": "git_commit",
                            "edge_type": "affects",
                            "dst_subtype": "source_file",
                            "clause_id": "commit_affects_file",
                        },
                    ],
                },
                allowed_record_kinds=("node", "edge"),
            ),
            nodes=[
                {
                    "node_id": "commit:repo:abc",
                    "subtype": "git_commit",
                    "attrs": {},
                },
            ],
            edges=[
                {
                    "src": "commit:repo:abc",
                    "edge_type": "affects",
                    "dst": "file:repo:never-tracked.bin",
                    "attrs": {},
                },
            ],
        )

    assert exc.value.code == "producer_edge_endpoint_missing"
    assert "dst node" in exc.value.message
    assert "file:repo:never-tracked.bin" in exc.value.message


def test_signature_evaluator_admits_optional_src_when_unresolvable() -> None:
    """Mirror of the dst case: an edge whose src is not in the nodes list (so
    src_subtype resolves to None) admits if the rule sets src_subtype_optional."""
    evidence = evaluate_payload_signature(
        profile=_profile(
            {
                "nodes": [{"subtype": "code_symbol"}],
                "edges": [
                    {
                        "src_subtype": "code_reference",
                        "edge_type": "references",
                        "dst_subtype": "code_symbol",
                        "src_subtype_optional": True,
                        "clause_id": "ref_references_symbol",
                    },
                ],
            },
            allowed_record_kinds=("node", "edge"),
        ),
        nodes=[
            {"node_id": "symbol:r:f.py:Foo:1", "subtype": "code_symbol", "attrs": {}},
        ],
        edges=[
            {
                "src": "reference:unresolved",
                "edge_type": "references",
                "dst": "symbol:r:f.py:Foo:1",
                "attrs": {},
            },
        ],
    )
    assert "ref_references_symbol" in (
        evidence["producer_policy"]["matched_clause_ids"]
    )


def test_signature_evaluator_rejects_unresolvable_src_without_optional_flag() -> None:
    """Strict default preserved: an unresolvable src rejects without the flag
    (symmetric to the dst regression coverage)."""
    with pytest.raises(ProducerPolicyViolation) as exc:
        evaluate_payload_signature(
            profile=_profile(
                {
                    "nodes": [{"subtype": "code_symbol"}],
                    "edges": [
                        {
                            "src_subtype": "code_reference",
                            "edge_type": "references",
                            "dst_subtype": "code_symbol",
                            "clause_id": "ref_references_symbol",
                        },
                    ],
                },
                allowed_record_kinds=("node", "edge"),
            ),
            nodes=[
                {"node_id": "symbol:r:f.py:Foo:1", "subtype": "code_symbol",
                 "attrs": {}},
            ],
            edges=[
                {
                    "src": "reference:unresolved",
                    "edge_type": "references",
                    "dst": "symbol:r:f.py:Foo:1",
                    "attrs": {},
                },
            ],
        )
    assert exc.value.code == "producer_edge_endpoint_missing"
    assert "src node" in exc.value.message
    assert "reference:unresolved" in exc.value.message


def test_signature_evaluator_admits_edge_only_producer_both_unresolvable() -> None:
    """Regression for the code-lsp resolution admission race. A pure edge
    producer (allowed_record_kinds=edge, no nodes) whose BOTH endpoints are
    cross-source nodes — so both subtypes resolve to None at admission, e.g. a
    reference edge emitted in the same cycle the predecessor created the
    endpoints — admits when both endpoints opt into optional discoverability.
    The narrowing trigger at projection still enforces the catalog."""
    evidence = evaluate_payload_signature(
        profile=_profile(
            {
                "nodes": [],
                "edges": [
                    {
                        "src_subtype": "code_reference",
                        "edge_type": "references",
                        "dst_subtype": "code_symbol",
                        "src_subtype_optional": True,
                        "dst_subtype_optional": True,
                        "clause_id": "ref_references_symbol",
                    },
                ],
            },
            allowed_record_kinds=("edge",),
        ),
        nodes=[],
        edges=[
            {
                "src": "reference:cross-source-unresolved",
                "edge_type": "references",
                "dst": "symbol:r:f.py:Foo:1",
                "attrs": {},
            },
        ],
    )
    assert "ref_references_symbol" in (
        evidence["producer_policy"]["matched_clause_ids"]
    )


@pytest.mark.postgres
def test_materialize_profile_validates_edge_signature_catalog(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO okg.catalog_versions (status, applied_at)
                VALUES ('applied', now())
                RETURNING catalog_version_id
                """
            )
            catalog_version_id = int(cur.fetchone()[0])
            cur.executemany(
                """
                INSERT INTO okg.subtypes (name, catalog_version_id)
                VALUES (%s, %s)
                """,
                [
                    ("document", catalog_version_id),
                    ("source_file", catalog_version_id),
                ],
            )
            cur.executemany(
                """
                INSERT INTO okg.subtype_ancestors
                  (catalog_version_id, subtype_name, ancestor_name, depth)
                VALUES (%s, %s, %s, 0)
                ON CONFLICT DO NOTHING
                """,
                [
                    (catalog_version_id, "document", "document"),
                    (catalog_version_id, "source_file", "source_file"),
                ],
            )

        with pytest.raises(ProducerPolicyViolation) as exc:
            materialize_producer_profile(
                conn,
                deployment_name="fixture",
                producer_id="fixture.source",
                producer_kind="source",
                trust_label="trusted",
                admission_mode="fast_track",
                authority_scope={},
                allowed_record_kinds=("edge",),
                output_signature={
                    "edges": [
                        {
                            "src_subtype": "document",
                            "edge_type": "references",
                            "dst_subtype": "source_file",
                        },
                    ],
                },
                policy_clauses=(),
                output_scope_summary={},
                source_ref={},
                catalog_version_id=catalog_version_id,
            )

    assert exc.value.code == "producer_catalog_narrowing_missing"


def test_validate_signature_catalog_skips_absent_cross_module_subtype() -> None:
    class FakeCursor:
        def execute(self, query, params=None) -> None:
            assert "FROM okg.subtypes" in query
            assert params == (1,)

        def fetchall(self):
            return [("source_file",)]

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

    class FakeConnection:
        def cursor(self):
            return FakeCursor()

    evidence = validate_signature_catalog(
        FakeConnection(),
        catalog_version_id=1,
        output_signature={
            "edges": [
                {
                    "src_subtype": "subtype_not_in_this_catalog",
                    "edge_type": "references",
                    "dst_subtype": "source_file",
                },
            ],
        },
    )

    assert evidence["narrowings"] == [
        {
            "src_subtype": "subtype_not_in_this_catalog",
            "edge_type": "references",
            "dst_subtype": "source_file",
            "status": "skipped_missing_subtype",
            "missing_subtypes": ["subtype_not_in_this_catalog"],
        }
    ]


def test_materialize_profile_thaws_nested_strict_policy_values() -> None:
    captured: dict[str, object] = {}

    class FakeCursor:
        def execute(self, _query, params=None) -> None:
            captured.update(params)

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

    class FakeConnection:
        def cursor(self):
            return FakeCursor()

    profile = materialize_producer_profile(
        FakeConnection(),
        deployment_name="strict",
        producer_id="strict.source",
        producer_kind="source",
        trust_label="review-trusted",
        admission_mode="review",
        authority_scope=MappingProxyType({
            "path_globs": ("docs/**/*",),
        }),
        allowed_record_kinds=("node",),
        output_signature=MappingProxyType({
            "nodes": (
                MappingProxyType({
                    "subtype": "document",
                    "attrs": MappingProxyType({"source": "proof"}),
                }),
            ),
        }),
        policy_clauses=(
            MappingProxyType({
                "id": "proof.document",
                "record_kind": "node",
                "subtype": "document",
            }),
        ),
        output_scope_summary=MappingProxyType({
            "nodes": ("document",),
        }),
        source_ref=MappingProxyType({"source_registry_name": "strict"}),
        catalog_version_id=None,
    )

    serialized = {
        key: getattr(value, "obj", value)
        for key, value in captured.items()
    }
    json.dumps(serialized)
    json.dumps(profile.authority_scope)
    assert profile.authority_scope == {"path_globs": ["docs/**/*"]}
    assert profile.output_signature["nodes"][0]["attrs"] == {
        "source": "proof"
    }
    assert profile.policy_clauses[0]["id"] == "proof.document"


@pytest.mark.postgres
def test_materialize_profile_exposes_view_with_stable_fingerprint(
    clean_runtime: str,
) -> None:
    deployment_name = f"fixture-{uuid.uuid4().hex[:8]}"
    producer_id = f"{deployment_name}.source"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT max(catalog_version_id)
                  FROM okg.catalog_versions
                 WHERE status = 'applied'
                """
            )
            catalog_version_id = int(cur.fetchone()[0])
        profile = materialize_producer_profile(
            conn,
            deployment_name=deployment_name,
            producer_id=producer_id,
            producer_kind="source",
            trust_label="trusted",
            admission_mode="fast_track",
            authority_scope={"source_family": "fixture"},
            allowed_record_kinds=("node",),
            output_signature={
                "nodes": [{"subtype": "document"}],
            },
            policy_clauses=(),
            output_scope_summary={"summary": "fixture"},
            source_ref={"source_registry_name": "fixture"},
            catalog_version_id=catalog_version_id,
        )
        again = materialize_producer_profile(
            conn,
            deployment_name=deployment_name,
            producer_id=producer_id,
            producer_kind="source",
            trust_label="trusted",
            admission_mode="fast_track",
            authority_scope={"source_family": "fixture"},
            allowed_record_kinds=("node",),
            output_signature={
                "nodes": [{"subtype": "document"}],
            },
            policy_clauses=(),
            output_scope_summary={"summary": "fixture"},
            source_ref={"source_registry_name": "fixture"},
            catalog_version_id=catalog_version_id,
        )
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT status, profile_fingerprint
                  FROM okg.v_producer_profiles
                 WHERE deployment_name = %(deployment_name)s
                   AND producer_id = %(producer_id)s
                   AND profile_fingerprint = %(profile_fingerprint)s
                """,
                {
                    "deployment_name": deployment_name,
                    "producer_id": producer_id,
                    "profile_fingerprint": profile.profile_fingerprint,
                },
            )
            rows = cur.fetchall()

    assert profile.profile_fingerprint == again.profile_fingerprint
    assert rows == [("active", profile.profile_fingerprint)]


@pytest.mark.postgres
def test_source_run_signature_resolves_cross_source_endpoint(
    clean_runtime: str,
) -> None:
    """A cross-source dst (created by another source's pending run, not
    yet in nodes_live) should resolve via the node_facts fallback so the
    edge admits with a matched signature instead of a spurious
    `dst_subtype is None` rejection.
    """
    import uuid
    from datetime import datetime, timezone

    now = datetime.now(timezone.utc)
    src_source = "test.cross-source.doc"
    dst_source = "test.cross-source.file"
    src_run_id = str(uuid.uuid4())
    dst_run_id = str(uuid.uuid4())
    profile = ProducerProfile(
        deployment_name="fixture",
        producer_id=src_source,
        producer_kind="source",
        trust_label="trusted",
        admission_mode="fast_track",
        authority_scope={},
        allowed_record_kinds=("node", "edge"),
        output_signature={
            "nodes": [
                {"subtype": "document", "clause_id": "doc_node"},
            ],
            "edges": [
                {
                    "src_subtype": "document",
                    "edge_type": "derived_from",
                    "dst_subtype": "source_file",
                    "clause_id": "doc_from_file",
                },
            ],
        },
        policy_clauses=(),
        output_scope_summary={},
        source_ref={},
        catalog_version_id=None,
        profile_fingerprint="sha256:test-cross-source",
        status="active",
    )

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            # Dst source_file I-fact from a different source's run. It is
            # NOT projected into nodes_live — it lives only in node_facts.
            cur.execute(
                """
                INSERT INTO okg.node_facts
                  (node_id, subtype, attrs, source, source_record_id,
                   source_revision, observed_at, dedupe_key, run_id, op)
                VALUES (%s, 'source_file', '{}'::jsonb, %s,
                        '{}'::jsonb, '{}'::jsonb, %s, %s, %s::uuid, 'I')
                """,
                (
                    "file:repo:foo.txt",
                    dst_source,
                    now,
                    f"dedupe:{dst_source}:foo",
                    dst_run_id,
                ),
            )
            # Src document I-fact (this source's run).
            cur.execute(
                """
                INSERT INTO okg.node_facts
                  (node_id, subtype, attrs, source, source_record_id,
                   source_revision, observed_at, dedupe_key, run_id, op)
                VALUES (%s, 'document', '{}'::jsonb, %s,
                        '{}'::jsonb, '{}'::jsonb, %s, %s, %s::uuid, 'I')
                """,
                (
                    "doc:repo:foo.txt",
                    src_source,
                    now,
                    f"dedupe:{src_source}:doc:foo",
                    src_run_id,
                ),
            )
            # Cross-source edge: src in this run, dst in the other run.
            cur.execute(
                """
                INSERT INTO okg.edge_facts
                  (edge_id, src, dst, edge_type, attrs, provenance,
                   confidence, source, source_record_id, source_revision,
                   observed_at, dedupe_key, run_id, op)
                VALUES (%s, %s, %s, 'derived_from', '{}'::jsonb, 'explicit',
                        1.0, %s, '{}'::jsonb, '{}'::jsonb, %s, %s,
                        %s::uuid, 'I')
                """,
                (
                    1,
                    "doc:repo:foo.txt",
                    "file:repo:foo.txt",
                    src_source,
                    now,
                    f"dedupe:{src_source}:edge:foo",
                    src_run_id,
                ),
            )

        evidence = evaluate_source_run_signature(
            conn,
            profile=profile,
            source=src_source,
            run_id=src_run_id,
        )

    assert evidence["producer_policy"]["status"] == "matched"
    assert set(evidence["producer_policy"]["matched_clause_ids"]) == {
        "doc_node",
        "doc_from_file",
    }


def _pending_node_edge_profile(source: str) -> ProducerProfile:
    """Strict derived signature shared by pending-node range tests."""
    return ProducerProfile(
        deployment_name="fixture",
        producer_id=source,
        producer_kind="enricher",
        trust_label="derived-deterministic-trusted",
        admission_mode="fast_track",
        authority_scope={},
        allowed_record_kinds=("node", "edge"),
        output_signature={
            "nodes": [{"subtype": "generic_claim"}],
            "edges": [
                {
                    "src_subtype": "paragraph",
                    "edge_type": "contains",
                    "dst_subtype": "generic_claim",
                    "clause_id": "paragraph_contains_claim",
                },
            ],
        },
        policy_clauses=(),
        output_scope_summary={},
        source_ref={},
        catalog_version_id=None,
        profile_fingerprint="sha256:test-pending-node",
        status="active",
    )


@pytest.mark.postgres
def test_derived_edge_range_resolves_same_run_pending_node(
    clean_runtime: str,
) -> None:
    """Derived edge admission sees the latest live node in its fact window."""
    from datetime import datetime, timezone

    now = datetime.now(timezone.utc)
    source = "_enricher:pending-node"
    run_id = str(uuid.uuid4())
    profile = _pending_node_edge_profile(source)

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO okg.node_facts
                  (node_id, subtype, attrs, source, source_record_id,
                   source_revision, observed_at, dedupe_key, run_id, op)
                VALUES ('paragraph:existing', 'paragraph', '{}'::jsonb,
                        '_source:fixture', '{}'::jsonb, '{}'::jsonb, %s, %s,
                        %s::uuid, 'I')
                RETURNING fact_id
                """,
                (now, "dedupe:fixture:paragraph", str(uuid.uuid4())),
            )
            node_fact_id_min = int(cur.fetchone()[0])
            # Rank all operations before accepting only a latest insertion:
            # an in-window R followed by I is live and authoritative.
            cur.execute(
                """
                INSERT INTO okg.node_facts
                  (node_id, subtype, attrs, source, source_record_id,
                   source_revision, observed_at, dedupe_key, run_id, op)
                VALUES ('claim:pending', 'generic_claim', '{}'::jsonb, %s,
                        '{}'::jsonb, '{}'::jsonb, %s, %s, %s::uuid, 'R')
                """,
                (source, now, f"dedupe:{source}:node:R", run_id),
            )
            cur.execute(
                """
                INSERT INTO okg.node_facts
                  (node_id, subtype, attrs, source, source_record_id,
                   source_revision, observed_at, dedupe_key, run_id, op)
                VALUES ('claim:pending', 'generic_claim', '{}'::jsonb, %s,
                        '{}'::jsonb, '{}'::jsonb, %s, %s, %s::uuid, 'I')
                RETURNING fact_id
                """,
                (source, now, f"dedupe:{source}:node:I", run_id),
            )
            node_fact_id_max = int(cur.fetchone()[0])
            cur.execute(
                """
                INSERT INTO okg.edge_facts
                  (edge_id, src, dst, edge_type, attrs, provenance,
                   confidence, source, source_record_id, source_revision,
                   observed_at, dedupe_key, run_id, op)
                VALUES (1, 'paragraph:existing', 'claim:pending', 'contains',
                        '{}'::jsonb, 'extracted_model', 1.0, %s,
                        '{}'::jsonb, '{}'::jsonb, %s, %s, %s::uuid, 'I')
                RETURNING fact_id
                """,
                (source, now, f"dedupe:{source}:edge", run_id),
            )
            fact_id = int(cur.fetchone()[0])
        evidence = evaluate_edge_fact_range_signature(
            conn,
            profile=profile,
            source=source,
            fact_id_min=fact_id,
            fact_id_max=fact_id,
            node_fact_id_min=node_fact_id_min,
            node_fact_id_max=node_fact_id_max,
        )

    assert evidence["producer_policy"]["status"] == "matched"
    assert evidence["producer_policy"]["matched_clause_ids"] == [
        "paragraph_contains_claim"
    ]


@pytest.mark.postgres
def test_derived_edge_range_rejects_insertion_before_node_window(
    clean_runtime: str,
) -> None:
    """An unretracted historical I-fact cannot authorize a current edge."""
    from datetime import datetime, timezone

    now = datetime.now(timezone.utc)
    source = "_enricher:pending-node"
    run_id = str(uuid.uuid4())
    profile = _pending_node_edge_profile(source)

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO okg.node_facts
                  (node_id, subtype, attrs, source, source_record_id,
                   source_revision, observed_at, dedupe_key, run_id, op)
                VALUES ('claim:pending', 'generic_claim', '{}'::jsonb, %s,
                        '{}'::jsonb, '{}'::jsonb, %s, %s, %s::uuid, 'I')
                """,
                (source, now, f"dedupe:{source}:historic:I", run_id),
            )
            cur.execute(
                """
                INSERT INTO okg.node_facts
                  (node_id, subtype, attrs, source, source_record_id,
                   source_revision, observed_at, dedupe_key, run_id, op)
                VALUES ('paragraph:existing', 'paragraph', '{}'::jsonb,
                        '_source:fixture', '{}'::jsonb, '{}'::jsonb, %s, %s,
                        %s::uuid, 'I')
                RETURNING fact_id
                """,
                (now, "dedupe:fixture:paragraph", str(uuid.uuid4())),
            )
            node_fact_id = int(cur.fetchone()[0])
            cur.execute(
                """
                INSERT INTO okg.edge_facts
                  (edge_id, src, dst, edge_type, attrs, provenance,
                   confidence, source, source_record_id, source_revision,
                   observed_at, dedupe_key, run_id, op)
                VALUES (1, 'paragraph:existing', 'claim:pending', 'contains',
                        '{}'::jsonb, 'extracted_model', 1.0, %s,
                        '{}'::jsonb, '{}'::jsonb, %s, %s, %s::uuid, 'I')
                RETURNING fact_id
                """,
                (source, now, f"dedupe:{source}:edge", run_id),
            )
            edge_fact_id = int(cur.fetchone()[0])

        with pytest.raises(ProducerPolicyViolation) as exc:
            evaluate_edge_fact_range_signature(
                conn,
                profile=profile,
                source=source,
                fact_id_min=edge_fact_id,
                fact_id_max=edge_fact_id,
                node_fact_id_min=node_fact_id,
                node_fact_id_max=node_fact_id,
            )

    assert exc.value.code == "producer_edge_endpoint_missing"
    # The message names the scope that was searched, never a bare claim that
    # the node does not exist: this path saw the branch graph and the cycle
    # node-fact window, and can speak for those only.
    assert "not resolvable in" in exc.value.message
    assert "node-fact window" in exc.value.message


@pytest.mark.postgres
def test_derived_edge_range_rejects_in_window_retraction(
    clean_runtime: str,
) -> None:
    """An in-window I followed by R leaves the endpoint unresolved."""
    from datetime import datetime, timezone

    now = datetime.now(timezone.utc)
    source = "_enricher:pending-node"
    run_id = str(uuid.uuid4())
    profile = _pending_node_edge_profile(source)

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO okg.node_facts
                  (node_id, subtype, attrs, source, source_record_id,
                   source_revision, observed_at, dedupe_key, run_id, op)
                VALUES ('paragraph:existing', 'paragraph', '{}'::jsonb,
                        '_source:fixture', '{}'::jsonb, '{}'::jsonb, %s, %s,
                        %s::uuid, 'I')
                RETURNING fact_id
                """,
                (now, "dedupe:fixture:paragraph", str(uuid.uuid4())),
            )
            node_fact_id_min = int(cur.fetchone()[0])
            cur.execute(
                """
                INSERT INTO okg.node_facts
                  (node_id, subtype, attrs, source, source_record_id,
                   source_revision, observed_at, dedupe_key, run_id, op)
                VALUES ('claim:pending', 'generic_claim', '{}'::jsonb, %s,
                        '{}'::jsonb, '{}'::jsonb, %s, %s, %s::uuid, 'I')
                """,
                (source, now, f"dedupe:{source}:node:I", run_id),
            )
            cur.execute(
                """
                INSERT INTO okg.node_facts
                  (node_id, subtype, attrs, source, source_record_id,
                   source_revision, observed_at, dedupe_key, run_id, op)
                VALUES ('claim:pending', 'generic_claim', '{}'::jsonb, %s,
                        '{}'::jsonb, '{}'::jsonb, %s, %s, %s::uuid, 'R')
                RETURNING fact_id
                """,
                (source, now, f"dedupe:{source}:node:R", run_id),
            )
            node_fact_id_max = int(cur.fetchone()[0])
            cur.execute(
                """
                INSERT INTO okg.edge_facts
                  (edge_id, src, dst, edge_type, attrs, provenance,
                   confidence, source, source_record_id, source_revision,
                   observed_at, dedupe_key, run_id, op)
                VALUES (1, 'paragraph:existing', 'claim:pending', 'contains',
                        '{}'::jsonb, 'extracted_model', 1.0, %s,
                        '{}'::jsonb, '{}'::jsonb, %s, %s, %s::uuid, 'I')
                RETURNING fact_id
                """,
                (source, now, f"dedupe:{source}:edge", run_id),
            )
            edge_fact_id = int(cur.fetchone()[0])

        with pytest.raises(ProducerPolicyViolation) as exc:
            evaluate_edge_fact_range_signature(
                conn,
                profile=profile,
                source=source,
                fact_id_min=edge_fact_id,
                fact_id_max=edge_fact_id,
                node_fact_id_min=node_fact_id_min,
                node_fact_id_max=node_fact_id_max,
            )

    assert exc.value.code == "producer_edge_endpoint_missing"
    # The message names the scope that was searched, never a bare claim that
    # the node does not exist: this path saw the branch graph and the cycle
    # node-fact window, and can speak for those only.
    assert "not resolvable in" in exc.value.message
    assert "node-fact window" in exc.value.message


@pytest.mark.postgres
def test_source_run_signature_rejects_dangling_cross_source_endpoint(
    clean_runtime: str,
) -> None:
    """If a cross-source dst has neither a live row nor an I-fact
    anywhere, the lookup still misses and the strict signature must
    reject — admission is permissive about discoverability, not about
    truly dangling endpoints.
    """
    import uuid
    from datetime import datetime, timezone

    now = datetime.now(timezone.utc)
    src_source = "test.cross-source.doc"
    src_run_id = str(uuid.uuid4())
    profile = ProducerProfile(
        deployment_name="fixture",
        producer_id=src_source,
        producer_kind="source",
        trust_label="trusted",
        admission_mode="fast_track",
        authority_scope={},
        allowed_record_kinds=("node", "edge"),
        output_signature={
            "nodes": [
                {"subtype": "document", "clause_id": "doc_node"},
            ],
            "edges": [
                {
                    "src_subtype": "document",
                    "edge_type": "derived_from",
                    "dst_subtype": "source_file",
                    "clause_id": "doc_from_file",
                },
            ],
        },
        policy_clauses=(),
        output_scope_summary={},
        source_ref={},
        catalog_version_id=None,
        profile_fingerprint="sha256:test-dangling",
        status="active",
    )

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO okg.node_facts
                  (node_id, subtype, attrs, source, source_record_id,
                   source_revision, observed_at, dedupe_key, run_id, op)
                VALUES (%s, 'document', '{}'::jsonb, %s,
                        '{}'::jsonb, '{}'::jsonb, %s, %s, %s::uuid, 'I')
                """,
                (
                    "doc:repo:dangling.txt",
                    src_source,
                    now,
                    f"dedupe:{src_source}:doc:dangling",
                    src_run_id,
                ),
            )
            cur.execute(
                """
                INSERT INTO okg.edge_facts
                  (edge_id, src, dst, edge_type, attrs, provenance,
                   confidence, source, source_record_id, source_revision,
                   observed_at, dedupe_key, run_id, op)
                VALUES (%s, %s, %s, 'derived_from', '{}'::jsonb, 'explicit',
                        1.0, %s, '{}'::jsonb, '{}'::jsonb, %s, %s,
                        %s::uuid, 'I')
                """,
                (
                    1,
                    "doc:repo:dangling.txt",
                    "file:repo:never-existed.txt",
                    src_source,
                    now,
                    f"dedupe:{src_source}:edge:dangling",
                    src_run_id,
                ),
            )

        with pytest.raises(ProducerPolicyViolation) as exc:
            evaluate_source_run_signature(
                conn,
                profile=profile,
                source=src_source,
                run_id=src_run_id,
            )

    assert exc.value.code == "producer_edge_endpoint_missing"
    assert "file:repo:never-existed.txt" in exc.value.message


def _profile(
    output_signature: dict,
    *,
    allowed_record_kinds: tuple[str, ...] = ("node",),
) -> ProducerProfile:
    return ProducerProfile(
        deployment_name="fixture",
        producer_id="fixture.source",
        producer_kind="source",
        trust_label="trusted",
        admission_mode="fast_track",
        authority_scope={},
        allowed_record_kinds=allowed_record_kinds,
        output_signature=output_signature,
        policy_clauses=(),
        output_scope_summary={},
        source_ref={},
        catalog_version_id=1,
        profile_fingerprint="sha256:test",
        status="active",
    )


def test_profile_fingerprint_stable_across_catalog_versions() -> None:
    """Per `fix-akmon-design-mistakes` task 2: the producer
    fingerprint is computed from the producer's authority
    signature only; catalog version is NOT in the hash. The same
    producer materialized against catalog v1 vs v2 must produce
    the same fingerprint so the unique index on
    `(deployment_name, producer_id, profile_fingerprint)` makes
    re-materialization an idempotent no-op.
    """
    from okg.substrate.producer_authority import profile_fingerprint

    args = dict(
        deployment_name="test",
        producer_id="test.source",
        producer_kind="source",
        trust_label="trusted",
        admission_mode="fast_track",
        authority_scope={"deployment": "test"},
        allowed_record_kinds=("node",),
        output_signature={
            "nodes": [{"subtype": "doc", "clause_id": "doc_only"}],
        },
        policy_clauses=[
            {"id": "doc_only", "effect": "allow",
             "record_kind": "node", "subtype": "doc"},
        ],
    )
    fp_v1 = profile_fingerprint(**args, catalog_version_id=1)
    fp_v2 = profile_fingerprint(**args, catalog_version_id=2)
    fp_none = profile_fingerprint(**args, catalog_version_id=None)
    assert fp_v1 == fp_v2 == fp_none

    # Changing a signature field DOES change the fingerprint.
    fp_diff_trust = profile_fingerprint(
        **{**args, "trust_label": "less-trusted"},
        catalog_version_id=1,
    )
    assert fp_diff_trust != fp_v1


def test_trust_policy_fields_change_profile_fingerprint() -> None:
    from okg.substrate.producer_authority import (
        TRUST_LABEL_RANKS,
        TRUST_LATTICE_POLICY_VERSION,
        profile_fingerprint,
        trust_lattice_policy_fingerprint,
    )

    args = dict(
        deployment_name="test",
        producer_id="test.source",
        producer_kind="source",
        trust_label="trusted",
        admission_mode="fast_track",
        authority_scope={"deployment": "test"},
        allowed_record_kinds=("node",),
        output_signature={
            "nodes": [{"subtype": "doc", "clause_id": "doc_only"}],
        },
        policy_clauses=[
            {"id": "doc_only", "effect": "allow",
             "record_kind": "node", "subtype": "doc"},
        ],
    )
    default_policy = {
        "version": TRUST_LATTICE_POLICY_VERSION,
        "fingerprint": trust_lattice_policy_fingerprint(),
    }
    shifted_ranks = {**TRUST_LABEL_RANKS, "less-trusted": 2}
    shifted_policy = {
        "version": TRUST_LATTICE_POLICY_VERSION,
        "fingerprint": trust_lattice_policy_fingerprint(shifted_ranks),
    }

    default_fp = profile_fingerprint(**args)
    explicit_default_fp = profile_fingerprint(
        **args,
        trust_policy=default_policy,
    )
    shifted_policy_fp = profile_fingerprint(
        **args,
        trust_policy=shifted_policy,
    )
    shifted_label_fp = profile_fingerprint(
        **{**args, "trust_label": "less-trusted"},
        trust_policy=default_policy,
    )

    assert explicit_default_fp == default_fp
    assert shifted_policy_fp != default_fp
    assert shifted_label_fp != default_fp


@pytest.mark.postgres
def test_loaded_profile_with_unknown_trust_label_fails_closed(
    clean_runtime: str,
) -> None:
    deployment = f"trust-label-{uuid.uuid4().hex[:8]}"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO okg.producer_profiles
                  (deployment_name, producer_id, producer_kind, trust_label,
                   admission_mode, profile_fingerprint, status)
                VALUES (%s, 'bad.source', 'source', 'mystery-trusted',
                        'fast_track', 'fp:bad.source', 'active')
                """,
                (deployment,),
            )
        with pytest.raises(ProducerPolicyViolation) as exc:
            load_profile_for_producer(
                conn,
                deployment_name=deployment,
                producer_id="bad.source",
            )

    assert exc.value.code == "unknown_trust_label"
    assert exc.value.verdict["producer_policy"]["trust_label"] == (
        "mystery-trusted"
    )


def test_block_consistency_passes_for_aligned_source() -> None:
    """A source whose output_signature, policy_clauses, and
    output_scope_summary declare the same set has no problems."""
    from okg.substrate.producer_authority import (
        check_admission_policy_block_consistency,
    )

    admission_policy = {
        "output_signature": {
            "nodes": [
                {"subtype": "doc", "clause_id": "c.doc"},
            ],
            "edges": [
                {
                    "src_subtype": "doc",
                    "edge_type": "cites",
                    "dst_subtype": "doc",
                    "clause_id": "c.doc_cites_doc",
                },
            ],
        },
        "policy_clauses": [
            {"id": "c.doc", "effect": "allow",
             "record_kind": "node", "subtype": "doc"},
            {"id": "c.doc_cites_doc", "effect": "allow",
             "record_kind": "edge", "triple": ["doc", "cites", "doc"]},
        ],
        "output_scope_summary": {
            "nodes": ["doc"],
            "edges": ["doc cites doc"],
        },
    }
    assert check_admission_policy_block_consistency(
        admission_policy, source_name="aligned"
    ) == []


def test_block_consistency_fails_on_missing_scope_summary_node() -> None:
    """Dropping a node from output_scope_summary that output_signature
    still declares is reported as drift."""
    from okg.substrate.producer_authority import (
        check_admission_policy_block_consistency,
    )

    admission_policy = {
        "output_signature": {
            "nodes": [
                {"subtype": "doc", "clause_id": "c.doc"},
                {"subtype": "author", "clause_id": "c.author"},
            ],
        },
        "policy_clauses": [
            {"id": "c.doc", "effect": "allow",
             "record_kind": "node", "subtype": "doc"},
            {"id": "c.author", "effect": "allow",
             "record_kind": "node", "subtype": "author"},
        ],
        "output_scope_summary": {
            "nodes": ["doc"],  # missing "author"
            "edges": [],
        },
    }
    problems = check_admission_policy_block_consistency(
        admission_policy, source_name="drifted"
    )
    assert problems
    assert any("output_scope_summary" in p for p in problems)
    assert any("author" in p for p in problems)


def test_block_consistency_fails_on_clause_id_mismatch() -> None:
    """An output_signature clause_id with no matching policy_clauses id
    (or vice versa) is reported as drift."""
    from okg.substrate.producer_authority import (
        check_admission_policy_block_consistency,
    )

    admission_policy = {
        "output_signature": {
            "nodes": [{"subtype": "doc", "clause_id": "c.doc"}],
        },
        "policy_clauses": [
            {"id": "c.OTHER", "effect": "allow",
             "record_kind": "node", "subtype": "doc"},
        ],
        "output_scope_summary": {"nodes": ["doc"], "edges": []},
    }
    problems = check_admission_policy_block_consistency(
        admission_policy, source_name="clause-drift"
    )
    assert problems
    assert any("clause_id" in p for p in problems)


def test_in_repo_sources_are_block_consistent() -> None:
    """Every in-repo source with an admission_policy passes the invariant
    (the previously-drifted okg-workspace `pact` source is reconciled)."""
    from pathlib import Path

    import yaml

    from okg.substrate.producer_authority import (
        check_admission_policy_block_consistency,
    )

    repo_root = Path(__file__).resolve().parents[2]
    problems: list[str] = []
    for registry_path in (repo_root / "deployments").glob(
        "*/source_registry.yaml"
    ):
        registry = yaml.safe_load(registry_path.read_text(encoding="utf-8"))
        for name, body in (registry.get("sources") or {}).items():
            if not isinstance(body, dict):
                continue
            admission_policy = body.get("admission_policy")
            if isinstance(admission_policy, dict):
                problems.extend(
                    f"{registry_path.parent.name}/{name}: {p}"
                    for p in check_admission_policy_block_consistency(
                        admission_policy, source_name=str(name)
                    )
                )
    assert problems == [], problems


def test_missing_endpoint_names_the_absent_node_and_its_collateral() -> None:
    """Issue #1330: a dangling edge is reported as a dangling edge.

    The refusal used to arrive as ``producer_edge_triple_not_allowed`` with
    the message "edge record does not match producer signature", which sends
    the operator to audit a signature that is correct. It also refused the
    whole emission while reporting only the offender, so the discarded
    remainder was invisible. Both are asserted here.
    """
    with pytest.raises(ProducerPolicyViolation) as exc:
        evaluate_payload_signature(
            profile=_profile(
                {
                    "nodes": [
                        {"subtype": "git_commit"},
                        {"subtype": "source_file"},
                    ],
                    "edges": [
                        {
                            "src_subtype": "git_commit",
                            "edge_type": "affects",
                            "dst_subtype": "source_file",
                            "clause_id": "commit_affects_file",
                        },
                    ],
                },
                allowed_record_kinds=("node", "edge"),
            ),
            nodes=[
                {
                    "node_id": "commit:repo:abc",
                    "subtype": "git_commit",
                    "attrs": {},
                },
                {
                    "node_id": "file:repo:a.py",
                    "subtype": "source_file",
                    "attrs": {},
                },
                {
                    "node_id": "file:repo:b.py",
                    "subtype": "source_file",
                    "attrs": {},
                },
            ],
            edges=[
                {
                    "src": "commit:repo:abc",
                    "edge_type": "affects",
                    "dst": "file:repo:a.py",
                    "attrs": {},
                },
                {
                    "src": "commit:repo:abc",
                    "edge_type": "affects",
                    "dst": "file:repo:b.py",
                    "attrs": {},
                },
                {
                    "src": "commit:repo:never-minted",
                    "edge_type": "affects",
                    "dst": "file:repo:a.py",
                    "attrs": {},
                },
            ],
        )

    assert exc.value.code == "producer_edge_endpoint_missing"
    assert "commit:repo:never-minted" in exc.value.message
    # Payload evaluation resolves endpoints from the batch alone, so the
    # message must scope its claim to the payload rather than to the graph.
    assert "not resolvable in the nodes declared in this payload" in (
        exc.value.message
    )
    verdict = exc.value.verdict
    # One offender; the other five admissible records (3 nodes + 2 edges)
    # are the collateral this whole-emission refusal discards.
    assert verdict["offending_record_count"] == 1
    assert verdict["collateral_record_count"] == 5
    policy = verdict["producer_policy"]
    assert policy["record_counts"] == {
        "node": 3, "edge": 3, "non_insert": 0,
    }
    assert policy["missing_endpoints"] == [
        {"role": "src", "node_id": "commit:repo:never-minted"},
    ]


def test_missing_endpoint_count_is_measured_not_assumed() -> None:
    """Every offending record of the raised class is counted.

    Raising on the first offender made the offending count unknowable, so a
    collateral figure derived from it would have been a guess. Two dangling
    edges must report two.
    """
    with pytest.raises(ProducerPolicyViolation) as exc:
        evaluate_payload_signature(
            profile=_profile(
                {
                    "nodes": [{"subtype": "git_commit"}],
                    "edges": [
                        {
                            "src_subtype": "git_commit",
                            "edge_type": "affects",
                            "dst_subtype": "source_file",
                        },
                    ],
                },
                allowed_record_kinds=("node", "edge"),
            ),
            nodes=[
                {
                    "node_id": "commit:repo:abc",
                    "subtype": "git_commit",
                    "attrs": {},
                },
            ],
            edges=[
                {
                    "src": "commit:repo:abc",
                    "edge_type": "affects",
                    "dst": "file:repo:gone-1.py",
                    "attrs": {},
                },
                {
                    "src": "commit:repo:abc",
                    "edge_type": "affects",
                    "dst": "file:repo:gone-2.py",
                    "attrs": {},
                },
            ],
        )

    assert exc.value.code == "producer_edge_endpoint_missing"
    assert exc.value.verdict["offending_record_count"] == 2
    assert exc.value.verdict["collateral_record_count"] == 1
    assert exc.value.verdict["producer_policy"]["missing_endpoints"] == [
        {"role": "dst", "node_id": "file:repo:gone-1.py"},
        {"role": "dst", "node_id": "file:repo:gone-2.py"},
    ]


def test_resolved_endpoints_keep_the_signature_rejection_code() -> None:
    """The new code does not swallow genuine signature mismatches.

    Both endpoints resolve here, so the producer really did emit a triple
    outside its declared signature and must still say so.
    """
    with pytest.raises(ProducerPolicyViolation) as exc:
        evaluate_payload_signature(
            profile=_profile(
                {
                    "nodes": [
                        {"subtype": "git_commit"},
                        {"subtype": "source_file"},
                    ],
                    "edges": [
                        {
                            "src_subtype": "git_commit",
                            "edge_type": "affects",
                            "dst_subtype": "source_file",
                        },
                    ],
                },
                allowed_record_kinds=("node", "edge"),
            ),
            nodes=[
                {
                    "node_id": "commit:repo:abc",
                    "subtype": "git_commit",
                    "attrs": {},
                },
                {
                    "node_id": "file:repo:a.py",
                    "subtype": "source_file",
                    "attrs": {},
                },
            ],
            edges=[
                {
                    "src": "file:repo:a.py",
                    "edge_type": "affects",
                    "dst": "commit:repo:abc",
                    "attrs": {},
                },
            ],
        )

    assert exc.value.code == "producer_edge_triple_not_allowed"
    assert exc.value.verdict["offending_record_count"] == 1
    assert exc.value.verdict["collateral_record_count"] == 2
    assert "missing_endpoints" not in exc.value.verdict["producer_policy"]


def test_collateral_excludes_records_that_failed_a_different_check() -> None:
    """Collateral is what was ADMISSIBLE, not merely what was not reported.

    The refusal reports one code, but a batch can break more than one rule.
    Counting the records that broke a second rule as discarded-but-admissible
    would overstate the cost of the refusal -- the number issue #1330 asks
    for is the number of good records thrown away.
    """
    with pytest.raises(ProducerPolicyViolation) as exc:
        evaluate_payload_signature(
            profile=_profile(
                {
                    "nodes": [{"subtype": "git_commit"}],
                    "edges": [
                        {
                            "src_subtype": "git_commit",
                            "edge_type": "affects",
                            "dst_subtype": "source_file",
                        },
                    ],
                },
                allowed_record_kinds=("node", "edge"),
            ),
            nodes=[
                {
                    "node_id": "commit:repo:abc",
                    "subtype": "git_commit",
                    "attrs": {},
                },
                # Outside the declared node signature: a second failure class.
                {
                    "node_id": "file:repo:a.py",
                    "subtype": "source_file",
                    "attrs": {},
                },
            ],
            edges=[
                {
                    "src": "commit:repo:abc",
                    "edge_type": "affects",
                    "dst": "file:repo:gone.py",
                    "attrs": {},
                },
            ],
        )

    # The node failure comes first in evaluation order, so it names the code.
    assert exc.value.code == "producer_node_subtype_not_allowed"
    verdict = exc.value.verdict
    assert verdict["offending_record_count"] == 1
    assert verdict["offending_record_counts_by_code"] == {
        "producer_node_subtype_not_allowed": 1,
        "producer_edge_endpoint_missing": 1,
    }
    # 3 records evaluated, 2 of them broke a rule: exactly one was admissible.
    assert verdict["collateral_record_count"] == 1


def test_undeclared_edge_type_is_not_blamed_on_the_missing_endpoint() -> None:
    """A missing endpoint is claimed only when it is the sole blocker.

    This edge has an unresolvable dst AND an edge_type the producer never
    declared. Reporting it as a missing endpoint would send the operator to
    hunt for a node when the signature is genuinely wrong -- the mirror image
    of the defect in issue #1330.
    """
    with pytest.raises(ProducerPolicyViolation) as exc:
        evaluate_payload_signature(
            profile=_profile(
                {
                    "nodes": [{"subtype": "git_commit"}],
                    "edges": [
                        {
                            "src_subtype": "git_commit",
                            "edge_type": "affects",
                            "dst_subtype": "source_file",
                        },
                    ],
                },
                allowed_record_kinds=("node", "edge"),
            ),
            nodes=[
                {
                    "node_id": "commit:repo:abc",
                    "subtype": "git_commit",
                    "attrs": {},
                },
            ],
            edges=[
                {
                    "src": "commit:repo:abc",
                    "edge_type": "never_declared",
                    "dst": "file:repo:gone.py",
                    "attrs": {},
                },
            ],
        )

    assert exc.value.code == "producer_edge_triple_not_allowed"
    assert "missing_endpoints" not in exc.value.verdict["producer_policy"]


def test_offending_record_sample_is_marked_as_a_sample() -> None:
    """More offenders than the sample limit must not read as five offenders."""
    with pytest.raises(ProducerPolicyViolation) as exc:
        evaluate_payload_signature(
            profile=_profile(
                {
                    "nodes": [{"subtype": "git_commit"}],
                    "edges": [
                        {
                            "src_subtype": "git_commit",
                            "edge_type": "affects",
                            "dst_subtype": "source_file",
                        },
                    ],
                },
                allowed_record_kinds=("node", "edge"),
            ),
            nodes=[
                {
                    "node_id": "commit:repo:abc",
                    "subtype": "git_commit",
                    "attrs": {},
                },
            ],
            edges=[
                {
                    "src": "commit:repo:abc",
                    "edge_type": "affects",
                    "dst": f"file:repo:gone-{index}.py",
                    "attrs": {},
                }
                for index in range(9)
            ],
        )

    verdict = exc.value.verdict
    assert verdict["offending_record_count"] == 9
    policy = verdict["producer_policy"]
    assert len(policy["offending_records"]) == 5
    assert policy["offending_records_truncated"] is True
    assert policy["offending_record_sample_limit"] == 5


# ---------------------------------------------------------------------------
# PACT lane-h-admission-readback: the source-run signature read is grouped.
#
# The chronos branch of ``evaluate_source_run_signature`` used to load every
# node and edge of the run, with attrs, into Python inside the admission
# transaction. It now reads groups of records that every check treats alike.
# ``tests/substrate/_legacy_source_run_signature.py`` keeps the removed code
# verbatim as the oracle these tests compare against.
# ---------------------------------------------------------------------------

import random  # noqa: E402
import tracemalloc  # noqa: E402

from okg.substrate import producer_authority as _pa  # noqa: E402
from okg.substrate.errors import SubstrateError  # noqa: E402
from tests.substrate._legacy_source_run_signature import (  # noqa: E402
    legacy_evaluate_records,
    legacy_evaluate_source_run_from_chronos,
    legacy_source_run_records_from_chronos,
)

_SUBTYPES = ("source_file", "document", "code_symbol")
_ATTR_VALUES = ("py", "md", 1, 1.0, True, None, "x", [1], {"a": 1})


def _outcome(call) -> tuple:
    try:
        return ("ok", call())
    except ProducerPolicyViolation as exc:
        return ("violation", exc.code, exc.message, exc.verdict)


def _random_attrs(rng: random.Random) -> dict:
    return {
        key: rng.choice(_ATTR_VALUES)
        for key in ("kind", "lang", "extra")
        if rng.random() < 0.6
    }


def _random_profile(rng: random.Random) -> ProducerProfile:
    node_rules = []
    for subtype in rng.sample(_SUBTYPES, rng.randint(0, 3)):
        rule: dict = {"subtype": subtype}
        if rng.random() < 0.3:
            rule["id_prefix"] = rng.choice(("a:", "a:1", "b:"))
        if rng.random() < 0.3:
            rule["attrs"] = {"kind": rng.choice(_ATTR_VALUES)}
        if rng.random() < 0.2:
            rule["attrs_exact"] = True
            rule["attr_keys"] = rng.sample(["kind", "lang", "extra"], 2)
        if rng.random() < 0.5:
            rule["clause_id"] = f"node_{subtype}"
        node_rules.append(rule)
    edge_rules = []
    for _ in range(rng.randint(0, 3)):
        rule = {
            "src_subtype": rng.choice(_SUBTYPES),
            "edge_type": rng.choice(("links", "defines")),
            "dst_subtype": rng.choice(_SUBTYPES),
        }
        for flag in ("src_subtype_optional", "dst_subtype_optional"):
            if rng.random() < 0.3:
                rule[flag] = True
        if rng.random() < 0.3:
            rule[rng.choice(("src_id_prefix", "dst_id_prefix"))] = "a:"
        if rng.random() < 0.3:
            rule["attrs"] = {"lang": rng.choice(_ATTR_VALUES)}
        edge_rules.append(rule)
    envelope: dict = {}
    if rng.random() < 0.3:
        envelope["unbounded"] = rng.sample(["nodes", "edges", "values"], 1)
    if rng.random() < 0.4:
        envelope["value_vocabularies"] = {"lang": ["py", "md"]}
    signature = {"nodes": node_rules, "edges": edge_rules}
    return ProducerProfile(
        deployment_name="fixture",
        producer_id="fixture.source",
        producer_kind="source",
        trust_label="trusted",
        admission_mode="fast_track",
        authority_scope={},
        allowed_record_kinds=tuple(
            rng.choice([("node", "edge"), ("node",), ("edge",)])
        ),
        output_signature=(
            signature if (node_rules or edge_rules or rng.random() < 0.5)
            else {}
        ),
        policy_clauses=(
            {"id": "clause_doc", "record_kind": "node", "subtype": "document"},
            {
                "id": "clause_links",
                "record_kind": "edge",
                "triple": ["source_file", "links", "document"],
            },
        ),
        output_scope_summary=(
            {"write_envelope": envelope} if envelope else {}
        ),
        source_ref={},
        catalog_version_id=1,
        profile_fingerprint="sha256:random",
        status="compat" if rng.random() < 0.05 else "active",
    )


def test_grouped_evaluator_matches_legacy_record_by_record_evaluator() -> None:
    """The refactored evaluator returns what the old one did, byte for byte.

    Every in-memory path (payloads, fact ranges, node-fact source runs) now
    runs through singleton groups; 600 random emissions, with retractions,
    unresolved endpoints, vocabularies, unbounded dimensions, exact attrs
    and refinement closures, must give identical verdicts.
    """
    rng = random.Random(20260928)
    outcomes: dict[str, int] = {}
    for _ in range(600):
        profile = _random_profile(rng)
        nodes = [
            {
                "fact_id": index,
                "node_id": f"{rng.choice(('a:1', 'a:2', 'b:1'))}/{index}",
                "subtype": rng.choice(_SUBTYPES),
                "attrs": _random_attrs(rng),
                "op": "I" if rng.random() < 0.9 else "R",
            }
            for index in range(rng.randint(0, 12))
        ]
        edges = [
            {
                "fact_id": index,
                "src": f"{rng.choice(('a:', 'b:'))}{index}",
                "src_subtype": rng.choice((*_SUBTYPES, None)),
                "edge_type": rng.choice(("links", "defines")),
                "dst": f"{rng.choice(('a:', 'b:'))}d{index}",
                "dst_subtype": rng.choice((*_SUBTYPES, None)),
                "attrs": _random_attrs(rng),
                "op": "I" if rng.random() < 0.9 else "R",
            }
            for index in range(rng.randint(0, 12))
        ]
        ancestors = rng.choice((
            None,
            {"code_symbol": {"code_symbol", "document"}},
        ))
        kwargs = {
            "profile": profile,
            "node_records": nodes,
            "edge_records": edges,
            "endpoint_scope": "the test payload",
            "ancestors": ancestors,
        }
        legacy = _outcome(lambda: legacy_evaluate_records(**kwargs))
        grouped = _outcome(lambda: _pa._evaluate_records(**kwargs))
        assert grouped == legacy
        assert json.dumps(grouped, default=str) == json.dumps(
            legacy, default=str,
        )
        key = legacy[0] if legacy[0] == "ok" else legacy[1]
        outcomes[key] = outcomes.get(key, 0) + 1
    # The random cases must actually reach the interesting branches.
    for expected in (
        "ok",
        "producer_node_subtype_not_allowed",
        "producer_edge_triple_not_allowed",
        "producer_edge_endpoint_missing",
        "envelope_vocabulary_violation",
        "producer_record_kind_not_allowed",
        "producer_signature_missing",
    ):
        assert outcomes.get(expected, 0) > 0, (expected, outcomes)


def _chronos_run_store(conn: psycopg.Connection):
    from chronos_core.branching._common import clear_all_registry_caches

    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.graph_store_internal.common import (
        clear_schema_ensured_cache,
    )

    clear_all_registry_caches()
    clear_schema_ensured_cache()
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO okg.catalog_versions "
            "(status, applied_at, deployment_name) "
            "VALUES ('applied', now(), 'readback-fixture')",
        )
    store = ChronosGraphStore.from_connection(conn)
    store.context.ensure_metadata()
    store.ensure_schema()
    store.ensure_branch("default", from_branch="main")
    return store


def _write_run(
    store,
    *,
    source: str,
    run_id: str,
    node_count: int,
    edge_count: int,
    blob_bytes: int = 0,
) -> None:
    """One source run: three subtypes, mixed id prefixes and attr values.

    Some edges point at a node another source owns, and some at a node that
    does not exist, so both endpoint lookups of the old reader are exercised.
    """
    from okg.substrate.library.sources.base import EdgeFact, NodeFact

    langs = ("py", "md", "rs", 1, 1.0)
    nodes = []
    for index in range(node_count):
        subtype = _SUBTYPES[index % 3]
        prefix = ("a:1", "a:2", "b:1")[index % 3 if index % 7 else 2]
        attrs: dict = {"lang": langs[index % len(langs)]}
        if index % 4 == 0:
            attrs["kind"] = "module"
        if blob_bytes:
            attrs["blob"] = f"{index:08d}" + "x" * blob_bytes
        nodes.append(NodeFact(
            f"{prefix}/{index:05d}", subtype, attrs, {"i": index}, {"v": 1},
        ))
    store.apply_facts(
        nodes, branch="default", source=source, run_id=run_id,
    )
    edges = []
    for index in range(edge_count):
        src = nodes[index % node_count].node_id
        if index % 11 == 0:
            dst = "other:shared-node"
        elif index % 13 == 0:
            dst = f"missing:{index}"
        else:
            dst = nodes[(index * 5 + 1) % node_count].node_id
        attrs = {"lang": langs[index % len(langs)]}
        # Vary the edges' attribute key sets so exact-key rules split them.
        if index % 3 == 0:
            attrs["kind"] = "import"
        if index % 5 == 0:
            attrs["extra"] = index
        if blob_bytes:
            attrs["blob"] = "y" * blob_bytes
        edges.append(EdgeFact(
            src, dst, ("links", "defines")[index % 2], attrs,
        ))
    store.apply_facts(
        edges,
        branch="default",
        source=source,
        run_id=run_id,
        strict_endpoints=False,
    )


def _db_profiles() -> list[ProducerProfile]:
    base = {
        "nodes": [
            {"subtype": "source_file", "clause_id": "file"},
            {"subtype": "document", "id_prefix": "a:", "clause_id": "doc"},
            {"subtype": "document", "clause_id": "doc_elsewhere"},
            {"subtype": "code_symbol"},
        ],
        "edges": [
            {
                "src_subtype": src,
                "edge_type": edge_type,
                "dst_subtype": dst,
                "dst_subtype_optional": True,
            }
            for src in _SUBTYPES
            for dst in _SUBTYPES
            for edge_type in ("links", "defines")
        ],
    }
    strict_edges = {
        **base,
        "edges": [
            {key: value for key, value in rule.items()
             if key != "dst_subtype_optional"}
            for rule in base["edges"]
        ],
    }
    narrow_nodes = {**base, "nodes": base["nodes"][:2]}
    exact = {
        **base,
        "nodes": [
            {"subtype": subtype, "attrs_exact": True,
             "attr_keys": ["lang", "kind"]}
            for subtype in _SUBTYPES
        ],
    }
    attr_rule = {
        **base,
        "nodes": [
            {"subtype": subtype, "attrs": {"lang": 1}} for subtype in _SUBTYPES
        ] + [{"subtype": "source_file"}],
    }
    few_edges = {**base, "edges": base["edges"][:3]}

    def edges_before_base(extra: dict, clause: str) -> dict:
        # A narrower rule listed before the catch-all rules: every edge still
        # matches, but WHICH clause it matches depends on the property the
        # narrower rule reads, so a grouping that ignores that property
        # reports the wrong matched clauses (the admit path).
        return {
            **base,
            "edges": [
                {
                    "src_subtype": src,
                    "edge_type": edge_type,
                    "dst_subtype": dst,
                    "dst_subtype_optional": True,
                    **extra,
                    "clause_id": clause,
                }
                for src in _SUBTYPES
                for dst in _SUBTYPES
                for edge_type in ("links", "defines")
            ] + base["edges"],
        }

    edge_src_prefix = edges_before_base({"src_id_prefix": "a:1"}, "src_a1")
    edge_dst_prefix = edges_before_base({"dst_id_prefix": "a:2"}, "dst_a2")
    edge_attrs = edges_before_base({"attrs": {"lang": 1}}, "lang_1")
    edge_exact = edges_before_base(
        {"attrs_exact": True, "attr_keys": ["lang", "nomos_source_lineage"]},
        "lang_only",
    )
    edge_src_prefix_only = {
        **base,
        "edges": [
            {**rule, "src_id_prefix": "a:"} for rule in base["edges"]
        ],
    }

    def profile(signature, *, kinds=("node", "edge"), envelope=None,
                status="active") -> ProducerProfile:
        return ProducerProfile(
            deployment_name="fixture",
            producer_id="fixture.code",
            producer_kind="source",
            trust_label="trusted",
            admission_mode="fast_track",
            authority_scope={},
            allowed_record_kinds=kinds,
            output_signature=signature,
            policy_clauses=(
                {"id": "c_doc", "record_kind": "node", "subtype": "document"},
            ),
            output_scope_summary=(
                {"write_envelope": envelope} if envelope else {}
            ),
            source_ref={},
            catalog_version_id=None,
            profile_fingerprint="sha256:readback",
            status=status,
        )

    return [
        profile(base),
        profile(strict_edges),
        profile(narrow_nodes),
        profile(exact),
        profile(attr_rule),
        profile(few_edges),
        profile(edge_src_prefix),
        profile(edge_dst_prefix),
        profile(edge_attrs),
        profile(edge_exact),
        profile(edge_src_prefix_only),
        profile(base, kinds=("node",)),
        profile(base, kinds=("edge",)),
        profile(base, envelope={"value_vocabularies": {"lang": ["py", "1"]}}),
        profile(narrow_nodes, envelope={"unbounded": ["nodes"]}),
        profile(base, status="compat"),
        profile({}),
    ]


@pytest.mark.postgres
def test_grouped_source_run_read_matches_the_legacy_full_read(
    clean_runtime: str,
) -> None:
    """Same verdict as the old full read, for matches and every refusal."""
    from okg.substrate.library.sources.base import NodeFact

    run_id = str(uuid.uuid4())
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = _chronos_run_store(conn)
        store.apply_facts(
            [NodeFact("other:shared-node", "document", {}, {"id": "shared"}, {"v": 1})],
            branch="default", source="other.src", run_id=str(uuid.uuid4()),
        )
        _write_run(
            store, source="fixture.code", run_id=run_id,
            node_count=60, edge_count=90,
        )
        seen: set[str] = set()
        for profile in _db_profiles():
            ancestors = _pa._load_subtype_ancestors(
                conn, profile.catalog_version_id,
            )
            legacy = _outcome(lambda: legacy_evaluate_source_run_from_chronos(
                conn, profile=profile, source="fixture.code", run_id=run_id,
                ancestors=ancestors,
            ))
            assert legacy != ("ok", None), "legacy read found no rows"
            grouped = _outcome(lambda: evaluate_source_run_signature(
                conn, profile=profile, source="fixture.code", run_id=run_id,
            ))
            assert grouped == legacy, profile.output_signature
            assert json.dumps(grouped, default=str) == json.dumps(
                legacy, default=str,
            )
            seen.add(legacy[0] if legacy[0] == "ok" else legacy[1])
    assert {
        "ok",
        "producer_node_subtype_not_allowed",
        "producer_edge_endpoint_missing",
        "producer_edge_triple_not_allowed",
        "envelope_vocabulary_violation",
        "producer_record_kind_not_allowed",
        "producer_signature_missing",
    } <= seen, seen


@pytest.mark.postgres
def test_grouped_source_run_read_keeps_memory_and_row_reads_bounded(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A large run is read as a few groups, not as rows.

    No statement returns more than two rows per record group, and the
    Python allocation peak stays a small fraction of what the old full read
    held for the same run. Between two statements Python therefore holds at
    most one chunk of group rows, so the admission transaction is never left
    idle behind a load proportional to the run.
    """
    run_id = str(uuid.uuid4())
    created: list = []
    real_sources = _pa._source_run_record_sources

    def spy(*args, **kwargs):
        pair = real_sources(*args, **kwargs)
        created.extend(pair)
        return pair

    monkeypatch.setattr(_pa, "_source_run_record_sources", spy)
    profile = _db_profiles()[0]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = _chronos_run_store(conn)
        _write_run(
            store, source="fixture.code", run_id=run_id,
            node_count=3000, edge_count=3000, blob_bytes=4000,
        )

        tracemalloc.start()
        try:
            legacy_records = legacy_source_run_records_from_chronos(
                conn, source="fixture.code", run_id=run_id,
            )
            _, legacy_peak = tracemalloc.get_traced_memory()
            assert legacy_records is not None
            legacy_count = len(legacy_records[0]) + len(legacy_records[1])
            del legacy_records
            tracemalloc.reset_peak()
            before, _ = tracemalloc.get_traced_memory()
            evidence = evaluate_source_run_signature(
                conn, profile=profile, source="fixture.code", run_id=run_id,
            )
            _, grouped_peak = tracemalloc.get_traced_memory()
        finally:
            tracemalloc.stop()

    assert legacy_count == 6000
    assert evidence["producer_policy"]["record_counts"] == {
        "node": 3000, "edge": 3000,
    }
    node_source, edge_source = created
    group_count = len(node_source._groups) + len(edge_source._groups)
    assert group_count < 200, group_count
    row_counts = (
        node_source.statement_row_counts + edge_source.statement_row_counts
    )
    # Each record read returns a group's first row and, for the admit-path
    # guard, its last row: at most two rows per group.
    assert max(row_counts) <= 2 * max(
        len(node_source._groups), len(edge_source._groups),
    )
    assert len(row_counts) <= 4, row_counts
    # The old read held every row with its ~4 KB of attrs (tens of MB here).
    assert legacy_peak > 20_000_000, legacy_peak
    assert grouped_peak - before < legacy_peak / 20, (
        grouped_peak - before, legacy_peak,
    )


@pytest.mark.postgres
@pytest.mark.parametrize("stage", ["checkout", "grouping"])
def test_source_run_read_failure_is_raised_not_admitted(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    stage: str,
) -> None:
    """A read failure refuses the run with its cause; it used to admit it.

    The old reader caught every exception and returned None, and the caller
    then evaluated an empty run: a failed read produced a ``matched``
    verdict with zero records.
    """
    from okg.substrate.graph_store import ChronosGraphStore

    run_id = str(uuid.uuid4())
    fault = psycopg.OperationalError("server closed the connection unexpectedly")
    real_from_connection = ChronosGraphStore.from_connection

    class _FailingCheckout:
        def query(self, *query_args, **query_kwargs):
            raise fault

    class _FailingStore:
        def checkout(self, branch):
            return _FailingCheckout()

    def failing_from_connection(*args, **kwargs):
        if stage == "checkout":
            raise fault
        real_from_connection(*args, **kwargs)
        return _FailingStore()

    profile = _db_profiles()[0]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = _chronos_run_store(conn)
        _write_run(
            store, source="fixture.code", run_id=run_id,
            node_count=6, edge_count=6,
        )
        monkeypatch.setattr(
            ChronosGraphStore, "from_connection", failing_from_connection,
        )
        # The removed behaviour, for the record: the same fault read as "no
        # rows", which the caller evaluated as an empty, matching run.
        assert legacy_source_run_records_from_chronos(
            conn, source="fixture.code", run_id=run_id,
        ) is None
        with pytest.raises(SubstrateError) as exc:
            evaluate_source_run_signature(
                conn, profile=profile, source="fixture.code", run_id=run_id,
            )

    assert exc.value.__cause__ is fault
    assert "fixture.code" in exc.value.message
    assert run_id in exc.value.message
    assert "OperationalError" in exc.value.message
    assert exc.value.data["run_id"] == run_id
    assert exc.value.data["cause"] == "OperationalError"


def _group_source(records: list[dict], check: dict | None):
    class _Source:
        total = len(records)
        insert_count = len(records)

        def groups(self):
            yield _pa._RecordGroup(
                record=records[0],
                count=len(records),
                head=tuple(enumerate(records)),
                check_record=check,
            )

        def first(self, limit):
            return records[:limit]

        def resolve(self, head):
            return [record for _, record in head]

    return _Source()


def test_admit_path_guard_refuses_a_group_whose_records_disagree() -> None:
    """A grouping defect cannot silently admit records it did not check.

    The guard re-evaluates a second record of every multi-record group; a
    different verdict refuses the run with ``signature_grouping_inconsistent``
    instead of applying the first record's verdict to the whole group.
    """
    profile = _profile(
        {"nodes": [{"subtype": "source_file", "id_prefix": "file:"}]},
    )
    good = {"node_id": "file:a", "subtype": "source_file", "attrs": {},
            "op": "I"}
    same = {**good, "node_id": "file:b"}
    wrong = {**good, "node_id": "dir:b"}
    empty = _pa._ListRecordSource([])

    evidence = _pa._evaluate_record_sources(
        profile=profile, nodes=_group_source([good, same], same),
        edges=empty, endpoint_scope="test",
    )
    assert evidence["producer_policy"]["record_counts"] == {
        "node": 2, "edge": 0,
    }
    with pytest.raises(SubstrateError) as exc:
        _pa._evaluate_record_sources(
            profile=profile, nodes=_group_source([good, wrong], wrong),
            edges=empty, endpoint_scope="test",
        )
    assert exc.value.code == "signature_grouping_inconsistent"
    assert exc.value.data["stage"] == "group guard (first and last record)"
    assert [r["node_id"] for r in exc.value.data["records"]] == [
        "file:a", "dir:b",
    ]


@pytest.mark.postgres
def test_grouped_read_treats_json_null_attrs_as_empty_under_exact_rules(
    clean_runtime: str,
) -> None:
    """JSON-null attrs read as {} under an exact-keys rule, as before.

    ``JSONB_OBJECT_KEYS`` refuses a JSON null; the grouping lists keys only
    for objects, so the run is judged, not failed.
    """
    from psycopg import sql

    run_id = str(uuid.uuid4())
    exact = {
        "nodes": [
            {"subtype": subtype, "attrs_exact": True, "attr_keys": []}
            for subtype in _SUBTYPES
        ] + [{"subtype": subtype} for subtype in _SUBTYPES],
        "edges": _db_profiles()[0].output_signature["edges"],
    }
    profile = ProducerProfile(
        **{**_db_profiles()[0].__dict__, "output_signature": exact},
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = _chronos_run_store(conn)
        _write_run(
            store, source="fixture.code", run_id=run_id,
            node_count=9, edge_count=0,
        )
        with conn.cursor() as cur:
            cur.execute(
                "SELECT physical_table FROM _chronos_branch_tables "
                "WHERE table_name = 'okg.graph_nodes'"
            )
            physical = cur.fetchone()[0]
            cur.execute(
                sql.SQL(
                    "UPDATE {} SET attrs = 'null'::jsonb "
                    "WHERE node_id IN ('a:2/00001', 'b:1/00007')"
                ).format(sql.Identifier(physical)),
            )
            assert cur.rowcount >= 2
        legacy = _outcome(lambda: legacy_evaluate_source_run_from_chronos(
            conn, profile=profile, source="fixture.code", run_id=run_id,
            ancestors=None,
        ))
        grouped = _outcome(lambda: evaluate_source_run_signature(
            conn, profile=profile, source="fixture.code", run_id=run_id,
        ))
    assert legacy[0] == "ok"
    assert grouped == legacy


@pytest.mark.postgres
def test_grouped_read_refuses_attrs_that_are_not_an_object(
    clean_runtime: str,
) -> None:
    """A row whose attrs is a JSON array or scalar fails the check loudly.

    Such a row is kept in its own group (the attrs-shape key), so its record
    is read and refused with the run named. Without that key the row would
    share a group with object rows and never be read.
    """
    from psycopg import sql

    run_id = str(uuid.uuid4())
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = _chronos_run_store(conn)
        _write_run(
            store, source="fixture.code", run_id=run_id,
            # Enough rows that the bad one is neither the first nor the
            # last of its subtype and prefix, so only the shape key
            # puts it in a group whose record is read.
            node_count=30, edge_count=0,
        )
        with conn.cursor() as cur:
            cur.execute(
                "SELECT physical_table FROM _chronos_branch_tables "
                "WHERE table_name = 'okg.graph_nodes'"
            )
            physical = cur.fetchone()[0]
            cur.execute(
                sql.SQL(
                    "UPDATE {} SET attrs = '[\"x\"]'::jsonb "
                    "WHERE node_id = 'a:2/00004'"
                ).format(sql.Identifier(physical)),
            )
            assert cur.rowcount >= 1
        # The old reader failed too, but with a bare ValueError from
        # ``dict(attrs)`` that named neither the run nor the row.
        with pytest.raises(ValueError):
            legacy_source_run_records_from_chronos(
                conn, source="fixture.code", run_id=run_id,
            )
        with pytest.raises(SubstrateError) as exc:
            evaluate_source_run_signature(
                conn, profile=_db_profiles()[0], source="fixture.code",
                run_id=run_id,
            )
    assert "not an object" in exc.value.message
    assert run_id in exc.value.message


# How each rule key, record field and envelope key the signature check reads
# is covered by the grouping in ``_signature_group_plan`` /
# ``_ChronosRunRecords``. A key read by the check but missing here fails
# ``test_group_plan_covers_every_rule_and_envelope_key``: add a grouping key
# for it (and a row here) before the grouped check may rely on it.
_GROUPED_RULE_KEYS = {
    "subtype": "grouped by the node's subtype",
    "src_subtype": "grouped by the resolved source-endpoint subtype",
    "dst_subtype": "grouped by the resolved target-endpoint subtype",
    "edge_type": "grouped by the edge type",
    "id_prefix": "a starts_with flag per rule prefix",
    "src_id_prefix": "a starts_with flag per rule prefix, on src",
    "dst_id_prefix": "a starts_with flag per rule prefix, on dst",
    "attrs": "the JSON text of each attribute key a rule names",
    "attrs_exact": "the sorted key set of object attrs",
    "attr_keys": "the sorted key set of object attrs",
    "src_subtype_optional": "rule flag; reads only the grouped src subtype",
    "dst_subtype_optional": "rule flag; reads only the grouped dst subtype",
    "clause_id": "names the rule; reads no record property",
    "id": "names the rule; reads no record property",
}
_GROUPED_RECORD_FIELDS = {
    "subtype", "edge_type", "src_subtype", "dst_subtype", "attrs",
    # Read only through rule prefixes (starts_with flags) and endpoint
    # messages; the id itself is never compared whole.
    "node_id", "src", "dst",
}
_GROUPED_ENVELOPE_KEYS = {
    "unbounded": "a per-profile switch; reads no record property",
    "value_vocabularies": "each field's JSON text is a grouping key",
}


def test_group_plan_covers_every_rule_and_envelope_key() -> None:
    """Pin the grouping to everything the signature check can read.

    The admit-path guard compares only a group's first and last records, so
    a record property the grouping misses could still admit a middle record
    wrongly. This test is the real guard: a new rule key, record field or
    envelope key read by the check fails CI until the grouping covers it.
    """
    import inspect
    import re

    declared = _pa.NODE_SIGNATURE_KEYS | _pa.EDGE_SIGNATURE_KEYS
    assert declared <= set(_GROUPED_RULE_KEYS), declared - set(_GROUPED_RULE_KEYS)

    matcher_source = "".join(
        inspect.getsource(function)
        for function in (
            _pa._match_node_rule, _pa._match_edge_rule, _pa._rule_clause_id,
        )
    )
    rule_reads = set(re.findall(r'rule(?:\.get\(|\[)"(\w+)"', matcher_source))
    assert rule_reads, "the rule-read scan found nothing; update the pattern"
    assert rule_reads <= set(_GROUPED_RULE_KEYS), (
        rule_reads - set(_GROUPED_RULE_KEYS)
    )

    evaluator_source = inspect.getsource(_pa._evaluate_record_sources) + "".join(
        inspect.getsource(function)
        for function in (
            _pa._match_node_rule, _pa._match_edge_rule, _pa._attrs_match,
            _pa._missing_endpoints,
        )
    )
    record_reads = set(re.findall(
        r'record(?:\.get\(|\[)(?:f?)"(\w+)"', evaluator_source,
    ))
    assert record_reads, "the record-read scan found nothing"
    assert record_reads <= _GROUPED_RECORD_FIELDS, (
        record_reads - _GROUPED_RECORD_FIELDS
    )
    envelope_reads = set(re.findall(r'envelope\.get\("(\w+)"', evaluator_source))
    assert envelope_reads == set(_GROUPED_ENVELOPE_KEYS), envelope_reads

    # And the plan really keys on each data-reading rule key.
    def plan(signature: dict, envelope: dict | None = None):
        return _pa._signature_group_plan(ProducerProfile(**{
            **_profile(signature).__dict__,
            "output_scope_summary": (
                {"write_envelope": envelope} if envelope else {}
            ),
        }))

    assert plan({"nodes": [{"subtype": "s", "id_prefix": "n:"}]})[0] == ["n:"]
    for key in ("src_id_prefix", "dst_id_prefix"):
        edge = {"src_subtype": "s", "edge_type": "e", "dst_subtype": "d",
                key: "e:"}
        assert plan({"edges": [edge]})[0] == ["e:"], key
    assert plan({"nodes": [{"subtype": "s", "attrs": {"k": 1}}]})[1] == ["k"]
    assert plan({"edges": [{"src_subtype": "s", "edge_type": "e",
                            "dst_subtype": "d", "attrs": {"j": 1}}]})[1] == ["j"]
    assert plan({"nodes": [{"subtype": "s", "attrs_exact": True,
                            "attr_keys": ["k"]}]})[2] is True
    assert plan({"nodes": [{"subtype": "s"}]}, {
        "value_vocabularies": {"lang": ["py"]},
    })[1] == ["lang"]


def test_offending_sample_recheck_refuses_a_sample_that_disagrees() -> None:
    """Each offending sample is re-evaluated; a disagreement refuses the run.

    The error names the disagreeing record once, not twice.
    """
    from okg.substrate.errors import SignatureGroupingInconsistent

    profile = _profile({"nodes": [{"subtype": "source_file"}]})
    failing = {"node_id": "file:a", "subtype": "directory", "attrs": {},
               "op": "I"}
    passing = {**failing, "subtype": "source_file"}

    class _Source:
        total = 1
        insert_count = 1

        def groups(self):
            yield _pa._RecordGroup(
                record=failing, count=1, head=((0, "ref"),),
            )

        def first(self, limit):
            return [failing]

        def resolve(self, head):
            # The stored row does not say what its group said.
            return [passing]

    with pytest.raises(SignatureGroupingInconsistent) as exc:
        _pa._evaluate_record_sources(
            profile=profile, nodes=_Source(),
            edges=_pa._ListRecordSource([]), endpoint_scope="test",
        )
    assert exc.value.data["stage"] == "offending sample check"
    assert len(exc.value.data["records"]) == 1


@pytest.mark.postgres
def test_grouped_read_tells_a_json_null_value_from_an_absent_key(
    clean_runtime: str,
) -> None:
    """``{"lang": null}`` and ``{}`` get different verdicts, as before.

    A rule expecting ``lang: null`` matches the first and not the second;
    the grouping keeps them apart (JSON text ``null`` vs SQL NULL).
    """
    from okg.substrate.library.sources.base import NodeFact

    run_id = str(uuid.uuid4())
    profile = ProducerProfile(**{
        **_db_profiles()[0].__dict__,
        "output_signature": {
            "nodes": [
                {"subtype": "source_file", "attrs": {"lang": None},
                 "clause_id": "lang_null"},
                {"subtype": "source_file", "clause_id": "any_file"},
            ],
            "edges": [],
        },
    })
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = _chronos_run_store(conn)
        store.apply_facts(
            [
                NodeFact(
                    f"a:{index:03d}", "source_file",
                    {"lang": None} if index % 2 else {},
                    {"i": index}, {"v": 1},
                )
                for index in range(9)
            ],
            branch="default", source="fixture.code", run_id=run_id,
        )
        legacy = _outcome(lambda: legacy_evaluate_source_run_from_chronos(
            conn, profile=profile, source="fixture.code", run_id=run_id,
            ancestors=None,
        ))
        grouped = _outcome(lambda: evaluate_source_run_signature(
            conn, profile=profile, source="fixture.code", run_id=run_id,
        ))
    assert grouped == legacy
    assert grouped[0] == "ok"
    assert grouped[1]["producer_policy"]["matched_clause_ids"] == [
        "any_file", "lang_null",
    ]


@pytest.mark.postgres
@pytest.mark.parametrize(
    ("caller_limit", "admission_limit_ms", "expected_ms", "expected_source"),
    [
        (None, 300, 300, "admission"),     # caller has no limit
        ("30s", 300, 300, "admission"),    # caller's limit is longer
        ("1s", 600_000, 1000, "caller"),   # caller's limit is shorter: kept
    ],
)
def test_grouping_statement_has_a_time_limit_and_restores_the_setting(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    caller_limit: str | None,
    admission_limit_ms: int,
    expected_ms: int,
    expected_source: str,
) -> None:
    """The stricter of the caller's and admission's limits cancels the check.

    A running statement is not bounded by the idle-in-transaction timeout.
    The slow statement runs through the real Chronos checkout of the same
    connection, which shows the limit reaches ``graph.query``. After a check
    that finishes, the caller's own statement_timeout is back.
    """
    from okg.substrate.errors import SourceRunGroupingTimeout
    from okg.substrate.graph_store import ChronosGraphStore

    run_id = str(uuid.uuid4())
    profile = _db_profiles()[0]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = _chronos_run_store(conn)
        _write_run(
            store, source="fixture.code", run_id=run_id,
            node_count=6, edge_count=6,
        )
        with conn.transaction():
            conn.execute("SET LOCAL statement_timeout = '7s'")
            evaluate_source_run_signature(
                conn, profile=profile, source="fixture.code", run_id=run_id,
            )
            assert conn.execute(
                "SELECT current_setting('statement_timeout')"
            ).fetchone()[0] == "7s"

        real_from_connection = ChronosGraphStore.from_connection

        class _SlowStore:
            def __init__(self, real_store):
                self._real = real_store

            def checkout(self, branch):
                real_checkout = self._real.checkout(branch)

                class _SlowCheckout:
                    def query(self, *args, **kwargs):
                        # Through the real Chronos query path and adapter.
                        return real_checkout.query(
                            "SELECT pg_sleep(3) AS slept", {},
                        )

                return _SlowCheckout()

        def slow_from_connection(*args, **kwargs):
            return _SlowStore(real_from_connection(*args, **kwargs))

        monkeypatch.setattr(
            ChronosGraphStore, "from_connection", slow_from_connection,
        )
        monkeypatch.setattr(
            _pa, "_SOURCE_RUN_GROUPING_TIMEOUT_MS", admission_limit_ms,
        )
        with pytest.raises(SourceRunGroupingTimeout) as exc:
            with conn.transaction():
                if caller_limit is not None:
                    conn.execute(
                        f"SET LOCAL statement_timeout = '{caller_limit}'",
                    )
                evaluate_source_run_signature(
                    conn, profile=profile, source="fixture.code",
                    run_id=run_id,
                )
    assert exc.value.code == "source_run_grouping_timeout"
    assert exc.value.data["timeout_ms"] == expected_ms
    assert exc.value.data["limit_source"] == expected_source
    assert exc.value.data["run_id"] == run_id
    assert exc.value.data["elapsed_ms"] < 2900


class _SettingCursor:
    """Cursor that answers the branch-setting read, or fails it."""

    def __init__(self, error: Exception | None) -> None:
        self._error = error

    def execute(self, query, params=None) -> None:
        assert "okg.chronos_branch" in query
        if self._error is not None:
            raise self._error

    def fetchone(self):
        return (None,)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None


class _SettingConnection:
    def __init__(self, error: Exception | None = None) -> None:
        self._error = error

    def cursor(self):
        return _SettingCursor(self._error)


def test_endpoint_subtype_lookup_skips_the_database_for_no_ids() -> None:
    from okg.substrate.producer_authority import _chronos_endpoint_subtypes

    class NoConnection:
        def cursor(self):
            raise AssertionError("an empty id list must not query")

    assert _chronos_endpoint_subtypes(NoConnection(), []) == {}


def test_endpoint_subtype_lookup_raises_when_the_branch_read_fails() -> None:
    """A failed read propagates instead of reading as "no endpoint known"."""
    from okg.substrate.producer_authority import _chronos_endpoint_subtypes

    with pytest.raises(psycopg.errors.UndefinedTable):
        _chronos_endpoint_subtypes(
            _SettingConnection(
                psycopg.errors.UndefinedTable("relation missing"),
            ),
            ["auth:src"],
        )


def test_endpoint_subtype_lookup_raises_when_the_graph_query_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate import graph_store
    from okg.substrate.producer_authority import _chronos_endpoint_subtypes

    seen: dict[str, object] = {}

    class FailingCheckout:
        def query(self, sql, params=None):
            seen["params"] = params
            raise psycopg.OperationalError("graph read failed")

    class FakeStore:
        def checkout(self, branch):
            seen["branch"] = branch
            return FailingCheckout()

    def fake_from_connection(conn, **kwargs):
        seen["kwargs"] = kwargs
        return FakeStore()

    monkeypatch.setattr(
        graph_store.ChronosGraphStore,
        "from_connection",
        staticmethod(fake_from_connection),
    )

    with pytest.raises(psycopg.OperationalError, match="graph read failed"):
        _chronos_endpoint_subtypes(_SettingConnection(), ["auth:src"])
    assert seen["branch"] == graph_store.DEFAULT_BRANCH
    assert seen["params"] == {"node_ids": ["auth:src"]}
    assert seen["kwargs"] == {"ensure_schema": False, "autocommit": False}


def test_endpoint_subtype_lookup_omits_ids_absent_from_the_graph(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An id the graph lacks is left out; it is not an error."""
    from okg.substrate import graph_store
    from okg.substrate.producer_authority import _chronos_endpoint_subtypes

    class Checkout:
        def query(self, sql, params=None):
            return [{"node_id": "auth:src", "subtype": "auth_src"}]

    class FakeStore:
        def checkout(self, branch):
            return Checkout()

    monkeypatch.setattr(
        graph_store.ChronosGraphStore,
        "from_connection",
        staticmethod(lambda conn, **kwargs: FakeStore()),
    )

    assert _chronos_endpoint_subtypes(
        _SettingConnection(), ["auth:src", "auth:not_yet_projected"],
    ) == {"auth:src": "auth_src"}
