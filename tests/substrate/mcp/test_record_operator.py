from __future__ import annotations

import pytest
import yaml

from okg.substrate.mcp.record import (
    RECORD_KINDS,
    RecordPayloadError,
    lower_record_payload,
    record_prefixes_payload,
    record_templates_payload,
)


def test_record_templates_cover_builtin_kinds() -> None:
    templates = record_templates_payload()

    assert templates["record_kinds"] == list(RECORD_KINDS)
    assert {row["kind"] for row in templates["templates"]} == set(RECORD_KINDS)
    assert all(row["required_fields"] for row in templates["templates"])
    assert all(row["example_payload"] for row in templates["templates"])
    assert templates["reference_prefixes"]


@pytest.mark.parametrize(
    ("kind", "payload", "target_subtype"),
    [
        ("artifact", {"summary": "Artifact was produced."}, "agent_observation"),
        ("decision", {"summary": "Use correction v3."}, "agent_decision"),
        ("evidence", {"summary": "The table exists."}, "agent_observation"),
        ("experiment_run", {"summary": "Ran closure v3."}, "agent_experiment"),
        ("finding", {"statement": "Closure improved."}, "agent_claim"),
        ("hypothesis", {"statement": "Closure is stable."}, "agent_claim"),
        (
            "observation",
            {"summary": "Closure agrees within tolerance."},
            "agent_observation",
        ),
        (
            "project",
            {"project_id": "cooper", "title": "Cooper"},
            "project",
        ),
        (
            "study",
            {"study_id": "abcd-closure", "title": "ABCD closure"},
            "study",
        ),
        (
            "domain_entity",
            {
                "domain_entity_id": "SR",
                "domain_kind": "region",
                "label": "Signal region",
            },
            "domain_entity",
        ),
        (
            "verification",
            {"summary": "Rerun matched the table.", "verdict": "verified"},
            "verification_result",
        ),
    ],
)
def test_lower_builtin_record_kinds(
    kind: str,
    payload: dict[str, str],
    target_subtype: str,
) -> None:
    lowered = lower_record_payload(
        kind=kind,
        payload=payload,
        authority={"producer_id": "agent:test"},
    )

    assert lowered["record_kind"] == kind
    assert lowered["template"]["target_subtype"] == target_subtype
    assert lowered["admission_payload"]["nodes"][0]["subtype"] == target_subtype
    assert lowered["idempotency_key"].startswith("record:")
    assert lowered["check_summary"]["record"]["record_id"] == lowered["record_id"]


def test_record_prefix_payload_exposes_context_policies() -> None:
    prefixes = record_prefixes_payload()

    by_prefix = {entry["prefix"]: entry for entry in prefixes["prefixes"]}
    assert by_prefix["study:"]["target_subtype"] == "study"
    assert by_prefix["proj:"]["target_subtype"] == "project"
    assert by_prefix["project:"]["target_subtype"] == "project"
    assert by_prefix["region:"]["target_subtype"] == "domain_entity"
    assert by_prefix["study:"]["resolution_methods"] == [
        "exact_node_id",
        "alias",
    ]
    # study: is an agent-authored knowledge namespace -> the record tool may mint
    # under it (creation_surface=record_template). Ingest-owned namespaces
    # (doc:/commit:/file:/issue:/pr:/chunk:/mention:/session:) remain "forbidden"
    # so an agent cannot collide with curated node ids (harden-mcp-write-boundaries).
    assert by_prefix["study:"]["creation_surface"] == "record_template"
    assert by_prefix["doc:"]["creation_surface"] == "forbidden"
    assert by_prefix["commit:"]["creation_surface"] == "forbidden"


def test_project_record_creates_bookkeeping_project_node() -> None:
    lowered = lower_record_payload(
        kind="project",
        payload={
            "project_id": "cooper",
            "title": "Cooper",
            "purpose": "Coordinate analysis memory.",
        },
        authority={"producer_id": "agent:test"},
    )

    assert lowered["record_id"] == "proj:cooper"
    node = lowered["admission_payload"]["nodes"][0]
    assert node["subtype"] == "project"
    assert node["attrs"]["project_id"] == "proj:cooper"
    assert node["attrs"]["slug"] == "cooper"
    assert node["attrs"]["title"] == "Cooper"
    assert node["attrs"]["purpose"] == "Coordinate analysis memory."


def test_project_record_preserves_project_prefix_when_explicit() -> None:
    lowered = lower_record_payload(
        kind="project",
        payload={
            "project_id": "project:cooper",
            "slug": "cooper",
            "title": "Cooper",
        },
        authority={"producer_id": "agent:test"},
    )

    assert lowered["record_id"] == "project:cooper"
    node = lowered["admission_payload"]["nodes"][0]
    assert node["attrs"]["project_id"] == "project:cooper"
    assert node["attrs"]["slug"] == "cooper"


def test_study_record_creates_primary_node_not_hidden_ref_target() -> None:
    lowered = lower_record_payload(
        kind="study",
        payload={
            "study_id": "abcd-closure",
            "title": "ABCD closure",
            "domain_refs": ["region:SR"],
        },
        authority={"producer_id": "agent:test"},
    )

    assert lowered["record_id"] == "study:abcd-closure"
    node = lowered["admission_payload"]["nodes"][0]
    assert node["subtype"] == "study"
    assert node["attrs"]["study_id"] == "abcd-closure"
    assert lowered["admission_payload"]["edges"][0]["dst"] == "region:SR"
    assert lowered["admission_payload"]["edges"][0]["dst_subtype"] == (
        "domain_entity"
    )


def test_study_record_emits_project_and_domain_edges(tmp_path) -> None:
    """#947: a study's advertised reference fields emit under a real catalog.

    The pre-existing study lowering test passes `narrowing_triples=None`, which
    SKIPS the route check entirely -- that is why the missing narrowing never
    showed up in a unit test.

    The constraints here are DERIVED from `compose_catalog` over the real library
    modules, deliberately not a literal triple set like the other tests in this
    file use. A literal would prove `_route_allowed` works and nothing about the
    shipped ontology -- it would pass with the narrowing declarations reverted.
    """
    from okg.substrate.catalog.modules_compose import (
        compose_catalog,
        load_modules_root,
    )

    modules_root = load_modules_root()
    modules = sorted(
        path.name for path in modules_root.iterdir()
        if (path / "schema.yaml").is_file()
    )
    deployment = tmp_path / "all-modules"
    deployment.mkdir()
    (deployment / "deployment.yaml").write_text(
        yaml.safe_dump({"name": "all-modules", "modules": modules}),
        encoding="utf-8",
    )
    (deployment / "schemas").mkdir()
    composed = compose_catalog(deployment)
    triples = composed.narrowing_triples()

    # The library's own declarations, so reverting either one fails here.
    assert ("study", "references", "project") in triples
    assert ("study", "references", "domain_entity") in triples

    lowered = lower_record_payload(
        kind="study",
        payload={
            "study_id": "cooper-fitness-readsurfaces",
            "title": "Read-surface fitness",
            "project_ref": "proj:cooper-fitness",
            "domain_refs": ["region:SR"],
        },
        authority={"producer_id": "agent:test"},
        active_subtypes=composed.subtypes,
        narrowing_triples=triples,
    )

    assert lowered["reference_diagnostics"] == []
    assert lowered["warnings"] == []
    assert lowered["suppressed_edge_count"] == 0
    assert lowered["emitted_edge_count"] == 2
    edges = {
        (edge["dst"], edge["dst_subtype"], edge["edge_type"])
        for edge in lowered["admission_payload"]["edges"]
    }
    assert edges == {
        ("proj:cooper-fitness", "project", "references"),
        ("region:SR", "domain_entity", "references"),
    }


def test_study_record_project_edge_is_suppressed_without_the_narrowing() -> None:
    """The reported #947 symptom, pinned: node created, edge silently dropped.

    Kept as a regression witness for the LOWERING semantics the archived
    `harden-record-reference-compatibility` change specified (diagnose, do not
    emit). #947 was fixed by declaring the route in the catalog, not by changing
    this behavior, so this asserts what a deployment whose catalog lacks the
    route still does.
    """
    lowered = lower_record_payload(
        kind="study",
        payload={
            "study_id": "cooper-fitness-readsurfaces",
            "title": "Read-surface fitness",
            "project_ref": "proj:cooper-fitness",
        },
        authority={"producer_id": "agent:test"},
        active_subtypes={"study", "project"},
        narrowing_triples=set(),
    )

    assert lowered["emitted_edge_count"] == 0
    assert lowered["suppressed_edge_count"] == 1
    assert lowered["admission_payload"]["nodes"][0]["subtype"] == "study"
    diagnostic = lowered["reference_diagnostics"][0]
    assert diagnostic["reason"] == "missing_narrowing"
    assert diagnostic["field"] == "project_ref"
    assert diagnostic["src_subtype"] == "study"
    assert diagnostic["dst_subtype"] == "project"


def test_domain_entity_record_mints_kind_prefixed_primary_node() -> None:
    lowered = lower_record_payload(
        kind="domain_entity",
        payload={
            "domain_entity_id": "SR",
            "domain_kind": "region",
            "label": "Signal region",
        },
        authority={"producer_id": "agent:test"},
    )

    assert lowered["record_id"] == "region:SR"
    node = lowered["admission_payload"]["nodes"][0]
    assert node["subtype"] == "domain_entity"
    assert node["attrs"]["domain_entity_id"] == "SR"
    assert node["attrs"]["domain_kind"] == "region"


def test_deployment_owned_prefixes_can_extend_lowering() -> None:
    lowered = lower_record_payload(
        kind="hypothesis",
        payload={
            "statement": "Fit closure is stable.",
            "domain_refs": ["fit:abcd-v1"],
        },
        authority={"producer_id": "agent:test"},
        prefixes=[{"prefix": "fit:", "target_subtype": "hep_fit"}],
    )

    assert lowered["warnings"] == []
    edge = lowered["admission_payload"]["edges"][0]
    assert edge["dst"] == "fit:abcd-v1"
    assert edge["dst_subtype"] == "hep_fit"


def test_catalog_constraints_mark_missing_prefix_inactive() -> None:
    prefixes = record_prefixes_payload(
        active_subtypes={"agent_claim", "agent_observation", "study"},
        narrowing_triples={
            ("agent_claim", "references", "study"),
            ("agent_observation", "references", "study"),
        },
    )

    by_prefix = {entry["prefix"]: entry for entry in prefixes["prefixes"]}
    assert by_prefix["study:"]["active"] is True
    assert by_prefix["artifact:"]["active"] is False
    assert by_prefix["artifact:"]["inactive_reason"] == (
        "target_subtype_missing"
    )


def test_builtin_prefix_targets_declared_or_deferred(tmp_path) -> None:
    from okg.substrate.catalog.modules_compose import (
        compose_catalog,
        load_modules_root,
    )

    modules_root = load_modules_root()
    modules = sorted(
        path.name for path in modules_root.iterdir()
        if (path / "schema.yaml").is_file()
    )
    deployment = tmp_path / "all-modules"
    deployment.mkdir()
    (deployment / "deployment.yaml").write_text(
        yaml.safe_dump({"name": "all-modules", "modules": modules}),
        encoding="utf-8",
    )
    (deployment / "schemas").mkdir()
    composed = compose_catalog(deployment)

    prefixes = record_prefixes_payload(
        active_subtypes=composed.subtypes,
        narrowing_triples=composed.narrowing_triples(),
    )
    missing_targets = {
        row["prefix"]: row["target_subtype"]
        for row in prefixes["prefixes"]
        if row["inactive_reason"] == "target_subtype_missing"
    }

    assert "artifact:" not in missing_targets
    assert "plot:" not in missing_targets
    assert missing_targets == {
        "self-play-finding:": "self_play_finding",
        "self-play-run:": "self_play_run",
        "self-play-scenario:": "self_play_scenario",
    }


#: Template x advertised-reference-field pairs that the all-modules catalog
#: cannot route. Compared with `==`, not `<=`: a NEW unroutable field must fail
#: this test, and closing one of these must fail it too until the entry is
#: deleted. A subset comparison would let a new gap hide behind an existing one.
#:
#: The 8 `actor_ref` entries that #947 recorded here are GONE: the field is now
#: wired to the `agent` / `person` / `git_author` prefixes and narrowings. The
#: comparison is `==`, so leaving them would fail on the `now routable` side --
#: closing a gap forces its entry's removal, which is the point.
#:
#: `domain_entity.reference_refs` remains, and is a different defect: that
#: template advertises `reference_refs` and NOTHING else, and the field reaches
#: zero targets, so a domain_entity record cannot link to anything -- #947's
#: shape one subtype over. Target-agnostic fields are normally exempt, but the
#: exemption is void when a template has no target-naming field to carry the
#: weight: zero IS information. Not fixed here either, because what a
#: domain_entity should reference is an ontology question rather than a missing
#: enumeration. Adding an entry here admits a template promises something the
#: catalog cannot deliver -- only with a stated reason and a follow-up.
_EXPECTED_UNROUTABLE_TEMPLATE_FIELDS: dict[tuple[str, str], str] = {
    ("domain_entity", "reference_refs"): "missing_narrowing:sink_only_source",
}

#: The id shapes the ingest sources actually MINT for each actor subtype, with
#: the emitter that mints them. `actor_ref` was dead partly because no prefix
#: resolved any of these; a prefix that matched no real shape would have changed
#: the diagnostic reason while leaving every link broken, so these pairs -- not
#: the presence of a prefix -- are what the test asserts.
_ACTOR_ID_SHAPES: tuple[tuple[str, str], ...] = (
    # src/okg/substrate/library/sources/_agent_sessions_emit.py: f"agent:{model}:{harness}"
    ("agent:claude-opus-5:claude-code", "agent"),
    # src/okg/substrate/library/sources/scholarly/dblp.py: f"person:{pid}"
    ("person:123/4567", "person"),
    # src/okg/substrate/library/sources/_git_graph_history_emit.py: f"author:{email}"
    # This exact id is pinned by
    # tests/substrate/library/sources/test_git_graph_history.py:643.
    ("author:shared@example.test", "git_author"),
)


def test_actor_prefixes_match_emitted_id_shapes() -> None:
    """`actor_ref` resolves the ids the substrate really mints.

    Guards the failure mode this fix could have shipped: inventing a plausible
    prefix (`git-author:`) that no emitter produces, which would move the
    reported reason from `target_subtype_missing` to `unrecognized_node_id`
    without making a single link work.
    """
    from okg.substrate.library.agent_memory_prefixes import (
        BUILTIN_RECORD_PREFIXES,
        prefix_for_id,
        subtype_for_id,
    )

    for node_id, expected_subtype in _ACTOR_ID_SHAPES:
        assert subtype_for_id(node_id, BUILTIN_RECORD_PREFIXES) == expected_subtype, node_id
        entry = prefix_for_id(node_id, BUILTIN_RECORD_PREFIXES)
        assert entry is not None, node_id
        # Actor namespaces are ingest-owned: referencing is allowed, minting is
        # not (harden-mcp-write-boundaries).
        assert entry.creation_surface == "forbidden", entry

    # The invented-name negative control: `git-author:` must NOT be how a git
    # author is addressed, or this test would pass on a decorative prefix.
    assert subtype_for_id(
        "git-author:shared@example.test", BUILTIN_RECORD_PREFIXES,
    ) != "git_author"

    # academic_researcher already owns this more-specific namespace as `advisee`.
    # Longest-prefix resolution must not stamp it as `person`, which would make
    # actor_ref emit a mis-typed edge and block publication.
    assert subtype_for_id(
        "person:scraped:group-hash:0", BUILTIN_RECORD_PREFIXES,
    ) == "advisee"


def test_actor_target_subtypes_are_declared(tmp_path) -> None:
    """Every actor target the field advertises is a real subtype.

    `operator` was advertised and declared by no module, so a caller following
    the advertised target list could only ever get `target_subtype_missing`.
    """
    from okg.substrate.catalog.modules_compose import (
        compose_catalog,
        load_modules_root,
    )
    from okg.substrate.mcp.record import _ACTOR_TARGET_SUBTYPES

    modules_root = load_modules_root()
    modules = sorted(
        path.name for path in modules_root.iterdir()
        if (path / "schema.yaml").is_file()
    )
    deployment = tmp_path / "all-modules"
    deployment.mkdir()
    (deployment / "deployment.yaml").write_text(
        yaml.safe_dump({"name": "all-modules", "modules": modules}),
        encoding="utf-8",
    )
    (deployment / "schemas").mkdir()
    composed = compose_catalog(deployment)

    assert _ACTOR_TARGET_SUBTYPES, "the field must advertise at least one target"
    undeclared = [
        subtype for subtype in _ACTOR_TARGET_SUBTYPES
        if subtype not in composed.subtypes
    ]
    assert undeclared == [], (
        "actor_ref advertises target subtypes no module declares: "
        f"{undeclared}. Declare them or drop them from "
        "_ACTOR_TARGET_SUBTYPES -- an unsatisfiable target only produces "
        "target_subtype_missing diagnostics for callers who trust the list."
    )


def test_actor_ref_emits_under_composed_catalog(tmp_path) -> None:
    """Every actor_ref template routes every supported actor id shape."""
    from okg.substrate.catalog.modules_compose import (
        compose_catalog,
        load_modules_root,
    )

    modules_root = load_modules_root()
    modules = sorted(
        path.name for path in modules_root.iterdir()
        if (path / "schema.yaml").is_file()
    )
    deployment = tmp_path / "all-modules"
    deployment.mkdir()
    (deployment / "deployment.yaml").write_text(
        yaml.safe_dump({"name": "all-modules", "modules": modules}),
        encoding="utf-8",
    )
    (deployment / "schemas").mkdir()
    composed = compose_catalog(deployment)
    triples = composed.narrowing_triples()

    templates = record_templates_payload(
        active_subtypes=composed.subtypes,
        narrowing_triples=triples,
    )["templates"]
    actor_templates = [
        row for row in templates if "actor_ref" in row["reference_fields"]
    ]
    assert len(actor_templates) == 8

    for template in actor_templates:
        actor_field = next(
            field for field in template["reference_field_compatibility"]
            if field["field"] == "actor_ref"
        )
        assert actor_field["allowed_target_subtypes"] == [
            "agent", "git_author", "person",
        ], template["kind"]
        reference_fields = set(template["reference_fields"])
        base_payload = {
            key: value
            for key, value in template["example_payload"].items()
            if key not in reference_fields
        }
        for actor_id, expected_subtype in _ACTOR_ID_SHAPES:
            lowered = lower_record_payload(
                kind=template["kind"],
                payload={**base_payload, "actor_ref": actor_id},
                authority={"producer_id": "agent:test"},
                active_subtypes=composed.subtypes,
                narrowing_triples=triples,
            )
            assert lowered["reference_diagnostics"] == [], (
                template["kind"], actor_id, lowered["reference_diagnostics"],
            )
            assert lowered["warnings"] == [], (template["kind"], actor_id)
            assert lowered["emitted_edge_count"] == 1
            edge = lowered["admission_payload"]["edges"][0]
            assert edge["dst"] == actor_id
            assert edge["dst_subtype"] == expected_subtype
            assert edge["attrs"]["role"] == "actor"


def test_actor_ref_rejects_scraped_advisee_namespace(tmp_path) -> None:
    """A real `person:scraped:` advisee id is never mislabeled as person."""
    from okg.substrate.catalog.modules_compose import (
        compose_catalog,
        load_modules_root,
    )

    modules_root = load_modules_root()
    modules = sorted(
        path.name for path in modules_root.iterdir()
        if (path / "schema.yaml").is_file()
    )
    deployment = tmp_path / "all-modules"
    deployment.mkdir()
    (deployment / "deployment.yaml").write_text(
        yaml.safe_dump({"name": "all-modules", "modules": modules}),
        encoding="utf-8",
    )
    (deployment / "schemas").mkdir()
    composed = compose_catalog(deployment)

    lowered = lower_record_payload(
        kind="observation",
        payload={
            "summary": "The scraped roster was inspected.",
            "actor_ref": "person:scraped:group-hash:0",
        },
        authority={"producer_id": "agent:test"},
        active_subtypes=composed.subtypes,
        narrowing_triples=composed.narrowing_triples(),
    )

    assert lowered["emitted_edge_count"] == 0
    assert lowered["reference_diagnostics"] == [{
        "code": "unsupported_record_reference",
        "reason": "field_target_not_allowed",
        "field": "actor_ref",
        "node_id": "person:scraped:group-hash:0",
        "edge_type": "references",
        "role": "actor",
        "src_subtype": "agent_observation",
        "dst_subtype": "advisee",
        "prefix": "person:scraped:",
    }]


def test_record_template_reference_fields_have_routes(tmp_path) -> None:
    """Every target-naming reference field must be able to emit an edge.

    A template that advertises a reference field the catalog declares no
    narrowing for is a promise the substrate cannot keep: `record()` creates the
    primary node, drops the edge with `reason: missing_narrowing`, and returns a
    warning. That is how issue #947 shipped -- the `study` template advertised
    `project_ref` and `domain_refs` while `study` was a narrowing DESTINATION
    only and never a source. Both the cross-link rules and their tests
    hand-enumerated the five other record subtypes, so nothing mechanical
    noticed. This is that mechanical check.

    The evaluation itself lives in `unroutable_record_reference_fields` so the
    per-deployment lint (`deployment.record.unroutable_reference_field`) applies
    the same rule to a deployment's own catalog. This test supplies the
    all-modules composition; the lint supplies the deployment's.
    """
    from okg.substrate.catalog.modules_compose import (
        compose_catalog,
        load_modules_root,
    )
    from okg.substrate.mcp.record import (
        exempt_reference_fields,
        unroutable_record_reference_fields,
    )

    modules_root = load_modules_root()
    modules = sorted(
        path.name for path in modules_root.iterdir()
        if (path / "schema.yaml").is_file()
    )
    deployment = tmp_path / "all-modules"
    deployment.mkdir()
    (deployment / "deployment.yaml").write_text(
        yaml.safe_dump({"name": "all-modules", "modules": modules}),
        encoding="utf-8",
    )
    (deployment / "schemas").mkdir()
    composed = compose_catalog(deployment)

    templates = record_templates_payload(
        active_subtypes=composed.subtypes,
        narrowing_triples=composed.narrowing_triples(),
    )

    unroutable = set(unroutable_record_reference_fields(
        active_subtypes=composed.subtypes,
        narrowing_triples=composed.narrowing_triples(),
    ))

    # Count what the shared evaluator actually examined, using the same
    # exemption rule, so the vacuity bounds below track the real surface.
    checked_fields = 0
    checked_templates = 0
    for row in templates["templates"]:
        assert row["available"] is True, row["kind"]
        field_rows = row["reference_field_compatibility"]
        exempt = exempt_reference_fields(r["field"] for r in field_rows)
        if field_rows:
            checked_templates += 1
        checked_fields += sum(
            1 for r in field_rows if r["field"] not in exempt
        )

    # Guard against a vacuous pass: a shrunken field set would make the equality
    # below trivially true. Measured on the 38-module catalog: 77 checked fields
    # across the 10 templates that advertise any reference field (`project`
    # advertises none). Bound just under those so an accidental drop of a whole
    # template or field group fails here rather than passing quietly.
    assert checked_fields >= 70, checked_fields
    assert checked_templates >= 10, checked_templates

    assert unroutable == set(_EXPECTED_UNROUTABLE_TEMPLATE_FIELDS), (
        "record template reference fields with no catalog route changed; "
        "declare the missing narrowing, or add the pair to "
        "_EXPECTED_UNROUTABLE_TEMPLATE_FIELDS with its reason. "
        f"newly unroutable={sorted(unroutable - set(_EXPECTED_UNROUTABLE_TEMPLATE_FIELDS))} "
        f"now routable={sorted(set(_EXPECTED_UNROUTABLE_TEMPLATE_FIELDS) - unroutable)}"
    )


def test_catalog_constraints_suppress_missing_narrowing() -> None:
    lowered = lower_record_payload(
        kind="observation",
        payload={
            "summary": "File was inspected.",
            "code_refs": ["file:demo:src/app.py"],
        },
        authority={"producer_id": "agent:test"},
        active_subtypes={
            "agent_observation",
            "source_file",
        },
        narrowing_triples=set(),
    )

    assert lowered["edge_count"] == 0
    assert lowered["suppressed_edge_count"] == 1
    assert (
        "unsupported_record_reference:code_refs:file:demo:src/app.py"
        in lowered["warnings"]
    )
    assert lowered["reference_diagnostics"][0]["reason"] == "missing_narrowing"


def test_domain_refs_reject_code_prefix_even_when_route_exists() -> None:
    lowered = lower_record_payload(
        kind="observation",
        payload={
            "summary": "Commit was inspected.",
            "domain_refs": ["commit:demo:abc123"],
        },
        authority={"producer_id": "agent:test"},
        active_subtypes={
            "agent_observation",
            "git_commit",
        },
        narrowing_triples={
            ("agent_observation", "references", "git_commit"),
        },
    )

    assert lowered["edge_count"] == 0
    assert lowered["reference_diagnostics"][0]["reason"] == (
        "field_target_not_allowed"
    )


def test_domain_refs_reject_deployment_owned_operator_prefix() -> None:
    """Removing operator from actor_ref must not authorize it as a domain."""
    lowered = lower_record_payload(
        kind="observation",
        payload={
            "summary": "An operator-owned resource was inspected.",
            "domain_refs": ["operator:alice"],
        },
        authority={"producer_id": "agent:test"},
        prefixes=[{
            "prefix": "operator:",
            "target_subtype": "operator",
            "owner": "deployment",
        }],
        active_subtypes={"agent_observation", "operator"},
        narrowing_triples={
            ("agent_observation", "references", "operator"),
        },
    )

    assert lowered["emitted_edge_count"] == 0
    assert lowered["reference_diagnostics"][0]["reason"] == (
        "field_target_not_allowed"
    )


def test_template_payload_reports_field_compatibility() -> None:
    templates = record_templates_payload(
        "hypothesis",
        active_subtypes={"agent_claim", "agent_observation", "study"},
        narrowing_triples={
            ("agent_claim", "references", "study"),
            ("agent_claim", "supported_by", "agent_observation"),
        },
    )

    fields = {
        row["field"]: row
        for row in templates["templates"][0]["reference_field_compatibility"]
    }
    assert fields["study_ref"]["allowed_target_subtypes"] == ["study"]
    assert fields["evidence_refs"]["edge_type"] == "supported_by"
    assert fields["evidence_refs"]["allowed_target_subtypes"] == [
        "agent_observation"
    ]


def test_unadvertised_reference_field_does_not_emit_edge() -> None:
    lowered = lower_record_payload(
        kind="project",
        payload={
            "project_id": "cooper",
            "title": "Cooper",
            "artifact_refs": ["artifact:closure-v3/table.json"],
        },
        authority={"producer_id": "agent:test"},
        active_subtypes={"project", "artifact"},
        narrowing_triples={
            ("agent_observation", "references", "artifact"),
        },
    )

    assert lowered["edge_count"] == 0
    assert lowered["suppressed_edge_count"] == 1
    assert "unsupported_record_field:artifact_refs" in lowered["warnings"]
    assert lowered["reference_diagnostics"][0]["reason"] == (
        "field_not_advertised"
    )


def test_lower_rejects_malformed_record_payload() -> None:
    with pytest.raises(RecordPayloadError) as exc:
        lower_record_payload(
            kind="observation",
            payload={},
            authority={"producer_id": "agent:test"},
        )

    assert exc.value.code == "invalid_record_payload"
    assert exc.value.field == "summary"


def test_supported_claim_without_evidence_is_downgraded_to_candidate() -> None:
    lowered = lower_record_payload(
        kind="hypothesis",
        payload={
            "statement": "Closure is supported.",
            "status": "supported",
        },
        authority={"producer_id": "agent:test"},
    )

    attrs = lowered["admission_payload"]["nodes"][0]["attrs"]
    assert "supported_claim_without_known_evidence" in lowered["warnings"]
    assert attrs["status"] == "candidate"
    assert (
        "supported_claim_without_known_evidence"
        in attrs["validation_warnings"]
    )


def test_supported_claim_with_suppressed_evidence_is_downgraded() -> None:
    lowered = lower_record_payload(
        kind="hypothesis",
        payload={
            "statement": "Closure is supported.",
            "status": "supported",
            "evidence_refs": ["test-report:demo"],
        },
        authority={"producer_id": "agent:test"},
        active_subtypes={"agent_claim", "test_report"},
        narrowing_triples=set(),
    )

    attrs = lowered["admission_payload"]["nodes"][0]["attrs"]
    assert lowered["edge_count"] == 0
    assert "supported_claim_without_known_evidence" in lowered["warnings"]
    assert attrs["status"] == "candidate"
