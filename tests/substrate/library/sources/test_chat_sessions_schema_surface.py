"""PACT chat-transcript-source — req.chat-transcript-source.schema-surface.

An edge whose ``(parent_subtype, edge_type, child_subtype)`` triple is absent
from the active narrowing catalog is rejected at insert as a
``narrowing_violation``, which leaves a correct adapter committing zero rows
while every command reports success. These tests close that hole by driving
the REAL adapter over a fixture instance and cross-checking every triple it
emits against the COMPOSED catalog and the deployment's registry signature.

Both tests assert a non-zero emitted count first: an empty emitted set would
make the cross-check vacuously true.
"""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
import yaml

from okg.substrate.catalog.modules_compose import compose_catalog, load_modules_root
from okg.substrate.library.sources.base import EdgeFact, NodeFact
from okg.substrate.library.sources.chat_sessions import (
    CITED_SUBTYPE_ALLOWLIST,
    ChatSessionsSource,
    chat_sessions_source_registry_entry,
    emitted_edge_triples,
    emitted_node_subtypes,
)
from tests.mirror_scope import private_path
from tests.substrate.library.sources import _chat_sessions_fixture as fx


INSTANCE = "test-instance"
REGISTRY = Path("deployments/okg-workspace/source_registry.yaml")
DEFINITION = Path("deployments/okg-workspace/definition.yaml")
DEPLOYMENT_MANIFEST = Path("deployments/okg-workspace/deployment.yaml")
OWNERSHIP_ID = "okg-workspace.chat-sessions"


def _composed() -> Any:
    """Compose the catalog for the REAL okg-workspace deployment directory.

    Composing from the deployment rather than from a hand-listed module set is
    what makes this check answer the question that matters: whether the
    triples the adapter emits are admitted by the catalog this deployment
    actually applies.
    """
    return compose_catalog(
        DEPLOYMENT_MANIFEST.parent, modules_root=load_modules_root(),
    )


def _registry_entry() -> dict[str, Any]:
    return yaml.safe_load(REGISTRY.read_text())["sources"]["chat-sessions"]


#: The library modules a workspace-graph deployment enrols; chat_sessions
#: cites subtypes from most of them.
_WORKSPACE_GRAPH_MODULES = [
    "person", "git_graph", "openspec", "agent_sessions", "compute_env",
    "code_graph", "extraction", "forge", "ci", "quality_evidence",
    "agent_memory", "reasoning", "coordination", "views", "chat_sessions",
    "scholarly", "dataset",
]

#: A posture that admits local, private chat text, and a discovery_crawl class
#: default that does NOT ask for redaction, so the entry's own policy has to win.
_SYNTHETIC_NOMOS = {
    "deployment_posture": {
        "deployment_class": "dogfood_workspace",
        "read_principals": ["human_operator", "agent", "okg_service", "mcp_client"],
        "write_principals": ["human_operator", "okg_service", "source_adapter"],
        "mcp_write_default": "review",
        "export_policy": "internal_only",
        "secret_handling": "secret_ref_required",
        "live_call_default": "review",
        "allowed_source_sensitivities": ["public", "internal", "local_private", "private"],
        "allowed_data_classifications": ["public", "internal", "private"],
        "allowed_exportability": ["internal", "no_export"],
        "privacy_obligations": ["audit", "review", "secret_ref_required"],
        "readiness_profiles": ["dogfood"],
    },
    "source_policy_defaults": {
        "by_source_class": {
            "discovery_crawl": {
                "credential_ref_policy": "none",
                "data_classification": "private",
                "exportability": "no_export",
                "live_call_allowed": False,
                "privacy_obligations": ["audit", "no_export"],
                "provenance": "local_filesystem",
                "retention": "source_defined",
                "sensitivity": "local_private",
                "store_raw": True,
            }
        }
    },
}


@pytest.fixture(params=["synthetic", "okg-workspace"])
def deployment(request: pytest.FixtureRequest, tmp_path: Path) -> SimpleNamespace:
    """The deployment the chat_sessions entry is checked against.

    ``synthetic`` is a chat-demo deployment built here from the shipped code
    helper and library modules, so the public copy runs these checks too.
    ``okg-workspace`` is this repository's real deployment; the public copy
    skips it (tests/mirror_scope.py).
    """
    if request.param == "okg-workspace":
        manifest_path = private_path(str(DEPLOYMENT_MANIFEST))
        return SimpleNamespace(
            entry=_registry_entry(),
            manifest=yaml.safe_load(manifest_path.read_text()),
            directory=manifest_path.parent,
        )
    directory = tmp_path / "chat-demo"
    (directory / "schemas").mkdir(parents=True)
    manifest = {
        "name": "chat-demo",
        "schema_dir": "schemas",
        "modules": _WORKSPACE_GRAPH_MODULES,
        "nomos": _SYNTHETIC_NOMOS,
    }
    (directory / "deployment.yaml").write_text(yaml.safe_dump(manifest))
    entry = chat_sessions_source_registry_entry(
        ownership_id="chat-demo.chat-sessions", preset="workspace",
    )
    return SimpleNamespace(entry=entry, manifest=manifest, directory=directory)


def _emitted(tmp_path: Path) -> list[Any]:
    fx.build_app_db(tmp_path / "chat" / "app.db")
    fx.write_audit_artifact(
        tmp_path / "state", deployment="chat-source-test", preset="workspace",
    )
    source = ChatSessionsSource(
        app_db_path=tmp_path / "chat" / "app.db",
        instance=INSTANCE,
        deployment="chat-source-test",
        preset="workspace",
        audit_state_home=tmp_path / "state",
    )
    return list(source.run("run-1", mode="scope_complete").facts)


def _node_subtype_of(node_id: str, facts: list[Any]) -> str:
    """The subtype the adapter itself gave a node it owns."""
    for fact in facts:
        if isinstance(fact, NodeFact) and fact.node_id == node_id:
            return fact.subtype
    # A cited node belongs to another source; the allowlist declares its class.
    for prefix, subtype in CITED_SUBTYPE_ALLOWLIST:
        if node_id.startswith(prefix):
            return subtype
    raise AssertionError(f"no subtype known for emitted endpoint {node_id!r}")


def test_every_emitted_edge_triple_is_admitted(
    tmp_path: Path, deployment: SimpleNamespace,
) -> None:
    facts = _emitted(tmp_path)
    edges = [fact for fact in facts if isinstance(fact, EdgeFact)]
    observed = {
        (
            _node_subtype_of(edge.src, facts),
            edge.edge_type,
            _node_subtype_of(edge.dst, facts),
        )
        for edge in edges
    }
    assert len(observed) >= 3, (
        f"the adapter emitted only {len(observed)} distinct triple(s); an "
        "empty or near-empty set makes this check vacuous"
    )

    admitted = compose_catalog(
        deployment.directory, modules_root=load_modules_root(),
    ).narrowing_triples()
    unadmitted = sorted(observed - admitted)
    assert not unadmitted, (
        "the adapter emits triples the composed catalog does not admit; each "
        f"would be rejected at insert as a narrowing_violation: {unadmitted}"
    )

    # The DECLARED emission set (which includes every allowlisted citation
    # class, not only the ones this fixture happens to cite) must ALSO be
    # fully admitted -- otherwise a real conversation citing a class the
    # fixture skipped would blow up in production.
    declared = set(emitted_edge_triples())
    assert observed <= declared, sorted(observed - declared)
    assert not sorted(declared - admitted), sorted(declared - admitted)


def test_every_emitted_subtype_is_in_output_signature(
    tmp_path: Path, deployment: SimpleNamespace,
) -> None:
    facts = _emitted(tmp_path)
    nodes = [fact for fact in facts if isinstance(fact, NodeFact)]
    observed = {node.subtype for node in nodes}
    assert len(observed) >= 3, (
        f"the adapter emitted only {len(observed)} node subtype(s); an empty "
        "set makes this check vacuous"
    )

    entry = deployment.entry
    signature = entry["admission_policy"]["output_signature"]
    declared = {row["subtype"]: row for row in signature["nodes"]}
    missing = sorted(observed - set(declared))
    assert not missing, (
        f"emitted node subtype(s) absent from the registry output_signature, "
        f"so admission would reject the whole payload: {missing}"
    )
    for node in nodes:
        row = declared[node.subtype]
        assert row["id_prefix"], row
        assert row["clause_id"], row
        assert node.node_id.startswith(row["id_prefix"]), (
            node.node_id, row["id_prefix"],
        )

    # Every declared node subtype is also a policy clause, and every declared
    # edge triple is one too: a signature row with no clause is admitted by
    # nothing.
    clauses = entry["admission_policy"]["policy_clauses"]
    clause_ids = {clause["id"] for clause in clauses}
    for row in signature["nodes"]:
        assert row["clause_id"] in clause_ids, row
    for row in signature["edges"]:
        assert row["clause_id"] in clause_ids, row

    edge_triples = {
        tuple(clause["triple"])
        for clause in clauses if clause["record_kind"] == "edge"
    }
    assert set(emitted_edge_triples()) == edge_triples
    assert set(emitted_node_subtypes()) == set(declared)


def test_module_is_enrolled_in_both_deployment_module_lists() -> None:
    """An unenrolled module contributes ZERO narrowings.

    Both lists matter: `definition.yaml` and `deployment.yaml` are read by
    different code paths, and a module present in one is silently absent from
    whichever path reads the other.
    """
    for path in (DEFINITION, DEPLOYMENT_MANIFEST):
        modules = yaml.safe_load(private_path(str(path)).read_text())["modules"]
        assert "chat_sessions" in modules, path

    composed = _composed()
    origins = {row.origin for row in composed.narrowings}
    assert "chat_sessions" in origins
    chat_rows = [
        row for row in composed.narrowings if row.origin == "chat_sessions"
    ]
    # 4 intra-module + one per allowlisted cited class.
    assert len(chat_rows) == 4 + len(CITED_SUBTYPE_ALLOWLIST), len(chat_rows)


def test_registry_entry_matches_the_code_helper() -> None:
    """The deployment YAML and the code helper cannot drift apart.

    The entry in `source_registry.yaml` was GENERATED from
    `chat_sessions_source_registry_entry`. If someone edits one of them, this
    fails rather than leaving the adapter emitting one shape and the
    deployment admitting another.
    """
    private_path(str(REGISTRY))
    entry = _registry_entry()
    expected = chat_sessions_source_registry_entry(
        ownership_id=OWNERSHIP_ID,
        app_db_path=entry["params"]["app_db_path"],
        instance=entry["params"]["instance"],
        preset=entry["params"]["preset"],
        excerpt_chars=entry["params"]["excerpt_chars"],
    )
    assert entry == expected


def test_profile_tuple_is_a_named_row_of_the_profiles_matrix(
    deployment: SimpleNamespace,
) -> None:
    from okg.substrate.sources.profiles import validate_profile_tuple

    entry = deployment.entry
    validate_profile_tuple(
        source_class=entry["source_class"],
        record_identity_kind=entry["record_identity_kind"],
        source_revision_kind=entry["source_revision_kind"],
        deletion_semantics=entry["deletion_semantics"],
        publication_mode=entry["publication_mode"],
    )
    assert entry["source_class"] == "discovery_crawl"
    assert entry["record_identity_kind"] == "scoped_locator"
    assert entry["source_revision_kind"] == "content_hash"
    assert entry["deletion_semantics"] == "missing_from_completed_scope"
    assert entry["publication_mode"] == "published_generation"
    assert entry["record_identity_fields"] == ["instance", "chat_id"]


def test_nomos_source_policy_resolves_from_the_entry_not_a_default(
    deployment: SimpleNamespace,
) -> None:
    """The declared policy WINS over the deployment's class default.

    Reading the resolved triple rather than the written one is the point: the
    requirement is that raw chat text is never stored under an unreviewed
    default, and only the resolver can say which policy actually applied.
    """
    from okg.substrate.nomos.deployment_audit import audit_source_privacy_policy

    manifest = deployment.manifest
    report = audit_source_privacy_policy(
        "chat-sessions", deployment.entry, manifest=manifest,
    )
    assert report["ok"], report["findings"]
    policy = report["source_policy"]
    assert policy["policy_origin"] == "source_registry.source_policy"
    assert policy["store_raw"] is True
    assert policy["sensitivity"] == "local_private"
    assert policy["data_classification"] == "private"
    assert policy["exportability"] == "no_export"
    assert "redaction" in policy["privacy_obligations"]
    # The class default this entry OVERRIDES, quoted so the difference is
    # visible in the test rather than only in a review document.
    class_default = manifest["nomos"]["source_policy_defaults"][
        "by_source_class"
    ]["discovery_crawl"]
    assert "redaction" not in class_default["privacy_obligations"]


def test_declared_pii_classes_are_honored_by_the_real_transform(
    tmp_path: Path, deployment: SimpleNamespace,
) -> None:
    """The posture APPLIES; it is not merely declared.

    The same chokepoint the SourceRunner uses is run over a real emitted fact
    under a not-store-raw posture built from THIS entry's own ``pii_classes``.
    A `free_text` excerpt must come out with its detected spans masked and a
    `non_pii` attribute must come out intact -- if the declared map were
    ignored, the fail-safe classifier would mask both and the second assertion
    fails.

    The deployment ships `store_raw: true`, so this posture is counterfactual
    for okg-workspace today. What it proves is that the declared map is the
    one the real transform reads, which is the part that would otherwise be
    unfalsifiable.
    """
    from okg.substrate.ingest.pii_posture import resolve_source_pii_posture
    from okg.substrate.ingest.pii_sanitize import sanitize_fact_pii

    facts = _emitted(tmp_path)
    message = next(
        fact for fact in facts
        if isinstance(fact, NodeFact) and fact.subtype == "chat_message"
        and fact.attrs.get("excerpt")
    )
    policy = dict(deployment.entry["source_policy"])
    policy["store_raw"] = False
    from okg.substrate.alias.pseudonymize import PSEUDONYM_KEY_ENV

    class _WholeStringDetector:
        """Stands in for Presidio: every free-text value is one span."""

        @staticmethod
        def detect(value: str) -> list[Any]:
            from types import SimpleNamespace

            return [SimpleNamespace(start=0, end=len(value), entity_type="X")]

    posture = resolve_source_pii_posture(
        source_policy=policy,
        env={PSEUDONYM_KEY_ENV: "k" * 64},
        detector=_WholeStringDetector(),
        build_detector=False,
    )
    assert posture.active is True
    assert posture.classifier("excerpt") == "free_text"
    assert posture.classifier("role") == "non_pii"
    assert posture.classifier("an_attribute_nobody_declared") is None, (
        "an undeclared attribute must fall through to the fail-safe class, "
        "not to a declared one"
    )

    result = sanitize_fact_pii(
        message,
        classifier=posture.classifier,
        store_raw=posture.store_raw,
        pseudonym_service=posture.pseudonym_service,
        detector=posture.detector,
    )
    assert result.fact is not None, (
        f"the fact was HELD ({result.held_reason}); the posture cannot be "
        "checked"
    )
    assert result.transformed is True
    assert result.fact.attrs["excerpt"] != message.attrs["excerpt"], (
        "a sensitive_attribute excerpt survived the transform unchanged"
    )
    assert result.fact.attrs["role"] == message.attrs["role"], (
        "a non_pii attribute was masked, so the declared pii_classes map was "
        "ignored and the fail-safe classifier ran instead"
    )
