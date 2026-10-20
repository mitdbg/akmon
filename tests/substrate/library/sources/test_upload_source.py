"""PACT chat-upload-source — req.chat-upload-source.governed-source.

Both arms of the requirement live in this one suite on purpose. The
refusal arm ("a deployment without the registration has no upload path")
is only meaningful next to a positive arm proving the same code DOES
produce an upload path when the registration is present: an absence
assertion that would also hold if the feature never worked is vacuous.
So every negative test here has a named partner:

  registration resolves            <-> registration is refused by name
  tool present on the surface      <-> tool absent from the surface
  guarded fetch succeeds           <-> private/loopback target refused
  forbidden-store scan finds none  <-> the same scanner finds a control

The suite is hermetic: no Postgres, no network, no MCP package import.
The adapter is exercised end to end against a temporary deployment
directory, and the tool-surface projection is applied to a stub tool
manager, which is the same dict `apply_admitted_tool_surface` mutates on
a real FastMCP instance.
"""

from __future__ import annotations

import base64
import inspect
from pathlib import Path
from typing import Any

import pytest
import yaml

from okg.substrate.library.sources import upload as upload_mod
from okg.substrate.library.sources.base import EdgeFact, NodeFact
from okg.substrate.library.sources.change_probe import SOUND_PROBE_KINDS
from okg.substrate.library.sources.upload import (
    UPLOAD_SOURCE_CLASS,
    UPLOAD_SOURCE_MODULE,
    UnsafeFetchURL,
    UploadRejected,
    UploadSource,
    UploadSourceNotRegistered,
    UploadSourceUnclassified,
    UploadStaging,
    resolve_upload_registration,
    upload_source_registry_entry,
)
from okg.substrate.mcp import upload_admin
from okg.substrate.mcp.operators import (
    CONDITIONAL_ADMIN_TOOLS,
    OPERATOR_TOOLS,
    WRITE_ADMIN_TOOLS,
)
from okg.substrate.mcp.tool_registration import apply_admitted_tool_surface


DEPLOYMENT = "upload-test"
OWNERSHIP_ID = f"{DEPLOYMENT}.upload"

_MARKDOWN = (
    "# Reactor safety notes\n"
    "\n"
    "## Overview\n"
    "\n"
    "The calorimeter calibration drifted after the July shutdown.\n"
    "\n"
    "## Action\n"
    "\n"
    "Re-run the calibration before the next fill.\n"
)


# ------------------------------------------------------------ fixtures


def _posture() -> dict:
    """A deployment posture wide enough to admit operator-uploaded content."""
    return {
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


def _extractors_yaml() -> dict:
    """Minimal parser + chunker registration; no NER, no vision, no network."""
    return {
        "parser": [
            {
                "format": "markdown",
                "class": (
                    "okg.substrate.library.extraction.parse.markdown."
                    "MarkdownParser"
                ),
            },
            {
                "format": "text",
                "class": "okg.substrate.library.extraction.parse.text.TextParser",
            },
            {
                "format": "html",
                "class": (
                    "okg.substrate.library.extraction.parse.html_fast."
                    "HtmlFastParser"
                ),
            },
        ],
        "chunker": [
            {
                "class": (
                    "okg.substrate.library.extraction.chunk.window.WindowChunker"
                ),
                "config": {"max_chars": 200, "overlap": 0},
            }
        ],
    }


def _write_deployment(
    root: Path,
    *,
    name: str = DEPLOYMENT,
    register_upload: bool = True,
    declare_policy: bool = True,
    inline_policy: bool = True,
) -> Path:
    """Materialize a deployment directory and return it.

    ``register_upload`` toggles the registry entry; ``declare_policy``
    toggles whether any Nomos classification is reachable at all. The two
    knobs are separate because they are separate refusals.
    """
    dep_dir = root / name
    dep_dir.mkdir(parents=True, exist_ok=True)

    sources: dict[str, Any] = {
        # A second, unrelated source so "no upload entry" is distinguishable
        # from "no sources at all" — an empty registry would satisfy the
        # absence assertion vacuously.
        "notes-corpus": {
            "module": "okg.substrate.library.sources.notes_dir",
            "class": "NotesDirSource",
            "ownership_id": f"{name}.notes-corpus",
        },
    }
    if register_upload:
        entry = upload_source_registry_entry(ownership_id=f"{name}.upload")
        if not (declare_policy and inline_policy):
            entry.pop("source_policy", None)
        sources["upload"] = entry
    (dep_dir / "source_registry.yaml").write_text(
        yaml.safe_dump({"sources": sources}, sort_keys=True)
    )

    nomos: dict[str, Any] = {"deployment_posture": _posture()}
    if declare_policy and not inline_policy:
        nomos["source_policy_defaults"] = {
            "by_source_class": {
                "discovery_crawl": {
                    "sensitivity": "local_private",
                    "data_classification": "private",
                    "store_raw": True,
                    "credential_ref_policy": "none",
                    "live_call_allowed": False,
                    "exportability": "no_export",
                    "retention": "source_defined",
                    "provenance": "operator_declared",
                    "privacy_obligations": ["audit", "no_export"],
                },
            },
        }
    (dep_dir / "deployment.yaml").write_text(
        yaml.safe_dump({"name": name, "nomos": nomos}, sort_keys=True)
    )
    (dep_dir / "extractors.yaml").write_text(yaml.safe_dump(_extractors_yaml()))
    return dep_dir


@pytest.fixture
def deployment_dir(tmp_path: Path) -> Path:
    return _write_deployment(tmp_path / "deployments")


@pytest.fixture
def resolver(tmp_path: Path):
    """A deployment-dir resolver bound to this test's temporary root."""

    def _resolve(name: str) -> Path:
        candidate = tmp_path / "deployments" / name
        if not candidate.is_dir():
            raise FileNotFoundError(f"no deployment directory for {name!r}")
        return candidate

    return _resolve


def _registration(dep_dir: Path, *, deployment: str = DEPLOYMENT):
    manifest = yaml.safe_load((dep_dir / "deployment.yaml").read_text())
    return resolve_upload_registration(
        deployment,
        registry_path=dep_dir / "source_registry.yaml",
        manifest=manifest,
    )


def _adapter(dep_dir: Path) -> UploadSource:
    reg = _registration(dep_dir)
    return UploadSource(
        staging_dir=reg.staging_dir,
        extractors_dir=reg.extractors_dir,
        source_name=reg.ownership_id,
    )


class _StubToolManager:
    def __init__(self, names: list[str]) -> None:
        self._tools = {name: object() for name in names}


class _StubServer:
    """Stands in for FastMCP: `apply_admitted_tool_surface` only ever
    touches `_tool_manager._tools` and writes readback attributes."""

    def __init__(self, names: list[str]) -> None:
        self._tool_manager = _StubToolManager(names)


class _FakeResponse:
    def __init__(self, body: bytes, content_type: str) -> None:
        self._body = body
        self.headers = {"Content-Type": content_type}
        self.closed = False

    def read(self, n: int = -1) -> bytes:
        return self._body if n < 0 else self._body[:n]

    def close(self) -> None:
        self.closed = True


# ------------------------------- an upload becomes governed graph content


def test_staged_file_becomes_document_and_chunk_facts(
    deployment_dir: Path,
) -> None:
    """scn.chat-upload-source.upload-becomes-source-content, positive arm."""
    reg = _registration(deployment_dir)
    reg.staging().stage_file(
        filename="reactor-notes.md",
        content=_MARKDOWN.encode("utf-8"),
        uploaded_by="operator@example.org",
    )

    run = _adapter(deployment_dir).run("run-1", mode="reconcile")
    facts = list(run.facts)
    nodes = [f for f in facts if isinstance(f, NodeFact)]
    edges = [f for f in facts if isinstance(f, EdgeFact)]

    documents = [n for n in nodes if n.subtype == "document"]
    chunks = [n for n in nodes if n.subtype == "document_chunk"]
    assert len(documents) == 1, [n.subtype for n in nodes]
    assert chunks, "an upload that produces no chunks is not retrievable"
    assert any(e.edge_type == "contains" for e in edges)

    doc = documents[0]
    # The operator-facing identity is the file they picked, not the
    # staged blob name the staging area chose.
    assert doc.attrs["title"] == "reactor-notes.md"
    assert doc.attrs["citation_label"] == "reactor-notes.md"
    # A local upload has no browser view; a guessed link is worse than none.
    assert "citation_url" not in doc.attrs
    assert doc.attrs["content_sha256"]
    assert doc.source_record_id["repo"] == "upload"


def test_url_upload_is_fetched_through_the_egress_guard(
    deployment_dir: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The URL path routes through `open_validated_url`, and the fetched
    URL becomes the document's truthful citation."""
    seen: list[str] = []
    response = _FakeResponse(b"<html><body><p>Fetched body.</p></body></html>", "text/html")

    def _fake_open(url: str, **_kw: Any) -> _FakeResponse:
        seen.append(url)
        return response

    monkeypatch.setattr(upload_mod, "open_validated_url", _fake_open)

    reg = _registration(deployment_dir)
    staged = reg.staging().stage_url(
        url="https://example.org/docs/handbook.html",
        uploaded_by="operator@example.org",
    )
    assert seen == ["https://example.org/docs/handbook.html"], (
        "stage_url must fetch through the library egress guard"
    )
    assert response.closed is True
    assert staged.origin_kind == "url"

    documents = [
        f
        for f in _adapter(deployment_dir).run("run-1", mode="reconcile").facts
        if isinstance(f, NodeFact) and f.subtype == "document"
    ]
    assert len(documents) == 1
    assert documents[0].attrs["citation_url"] == (
        "https://example.org/docs/handbook.html"
    )
    assert documents[0].attrs["citation_label"] == (
        "https://example.org/docs/handbook.html"
    )


def test_registration_declares_the_nomos_classification(
    deployment_dir: Path,
) -> None:
    reg = _registration(deployment_dir)
    assert reg.ownership_id == OWNERSHIP_ID
    assert reg.declared_policy() == {
        "sensitivity": "local_private",
        "data_classification": "private",
        "store_raw": True,
        "policy_origin": "source_registry.source_policy",
    }


def test_classification_can_come_from_deployment_defaults(
    tmp_path: Path,
) -> None:
    """The registry entry need not inline the policy; the deployment's
    `nomos.source_policy_defaults` is the other declared origin, and the
    resolver reports which one it used rather than flattening both."""
    dep_dir = _write_deployment(
        tmp_path / "deployments", name="defaults", inline_policy=False,
    )
    reg = _registration(dep_dir, deployment="defaults")
    assert reg.sensitivity == "local_private"
    assert reg.data_classification == "private"
    assert reg.policy_origin == (
        "nomos.source_policy_defaults.by_source_class"
    )


def test_registry_entry_shape_loads_and_passes_the_nomos_audit(
    tmp_path: Path, deployment_dir: Path,
) -> None:
    """The canonical entry is exercised by the real registry loader and
    the real source-privacy audit, not asserted as prose."""
    from okg.substrate.nomos import audit_source_privacy_policy
    from okg.substrate.sources.registry import load_source_registry

    entries = load_source_registry(deployment_dir / "source_registry.yaml")
    entry = entries["upload"]
    assert entry.raw["module"] == UPLOAD_SOURCE_MODULE
    assert entry.raw["class"] == UPLOAD_SOURCE_CLASS
    assert entry.ownership_id == OWNERSHIP_ID
    assert entry.admission_policy is not None
    assert entry.admission_policy.trust_label == "operator-uploaded"

    manifest = yaml.safe_load((deployment_dir / "deployment.yaml").read_text())
    report = audit_source_privacy_policy(
        "upload",
        upload_source_registry_entry(ownership_id=OWNERSHIP_ID),
        manifest=manifest,
    )
    assert report["ok"] is True, report["findings"]
    assert report["source_policy"]["data_classification"] == "private"


def test_emitted_facts_all_match_the_shipped_admission_signature(
    tmp_path: Path,
) -> None:
    """The registration must survive its own admission gate.

    Admission rejects the WHOLE payload on the first record matching no
    rule, so an `output_signature` that omits any emitted subtype or edge
    triple makes the first upload on a real deployment fail entirely.
    This runs the real evaluator over the adapter's real fact stream, with
    an extractor bundle that includes NER — because the canonical entry
    points `extractors_dir` at the deployment's own extractors.yaml, where
    an `ner` block is ordinary.
    """
    from okg.substrate.producer_authority import (
        ProducerProfile,
        _evaluate_records,
    )

    dep_dir = _write_deployment(tmp_path / "deployments", name="sig")
    # Add an NER backend so entity_mention nodes and their edges are in the
    # stream; without it this test would pass vacuously on the subset.
    extractors = _extractors_yaml()
    extractors["ner"] = [
        {
            "class": (
                "okg.substrate.library.extraction.ner.regex_backend."
                "RegexEntityExtractor"
            ),
            "config": {"rules": [["doi", r"10\.[0-9]{4,9}/[-._;()/:A-Za-z0-9]+"]]},
        }
    ]
    (dep_dir / "extractors.yaml").write_text(yaml.safe_dump(extractors))

    reg = _registration(dep_dir, deployment="sig")
    reg.staging().stage_file(
        filename="paper-notes.md",
        content=(
            _MARKDOWN + "\n## Reference\n\nSee DOI 10.1234/abc.5678 for detail.\n"
        ).encode("utf-8"),
        uploaded_by="operator@example.org",
    )
    source = UploadSource(
        staging_dir=reg.staging_dir,
        extractors_dir=reg.extractors_dir,
        source_name=reg.ownership_id,
    )
    facts = list(source.run("run-1", mode="reconcile").facts)

    node_records = [
        {
            "fact_id": i,
            "node_id": f.node_id,
            "subtype": f.subtype,
            "attrs": dict(f.attrs),
            "op": f.op,
        }
        for i, f in enumerate(facts)
        if isinstance(f, NodeFact)
    ]
    subtype_by_id = {r["node_id"]: r["subtype"] for r in node_records}
    edge_records = [
        {
            "fact_id": 10_000 + i,
            "src": f.src,
            "dst": f.dst,
            "edge_type": f.edge_type,
            "src_subtype": subtype_by_id.get(f.src, ""),
            "dst_subtype": subtype_by_id.get(f.dst, ""),
            "attrs": dict(f.attrs),
            "op": f.op,
        }
        for i, f in enumerate(facts)
        if isinstance(f, EdgeFact)
    ]
    # Non-vacuity: the stream must actually contain the record classes the
    # signature is being checked against.
    assert {r["subtype"] for r in node_records} >= {
        "document",
        "document_chunk",
        "entity_mention",
    }
    assert {r["edge_type"] for r in edge_records} >= {"contains", "member_of"}

    entry = upload_source_registry_entry(ownership_id="sig.upload")
    policy = entry["admission_policy"]
    profile = ProducerProfile(
        deployment_name="sig",
        producer_id=policy["producer_id"],
        producer_kind=policy["producer_kind"],
        trust_label=policy["trust_label"],
        admission_mode=policy["admission_mode"],
        authority_scope={},
        allowed_record_kinds=tuple(policy["allowed_record_kinds"]),
        output_signature=policy["output_signature"],
        policy_clauses=tuple(policy["policy_clauses"]),
        output_scope_summary={},
        source_ref={},
        catalog_version_id=None,
        profile_fingerprint="upload-signature-fixture",
        status="active",
    )
    # Raises ProducerPolicyViolation on the first unmatched record.
    _evaluate_records(
        profile=profile,
        node_records=node_records,
        edge_records=edge_records,
        endpoint_scope="the records supplied to this evaluation",
    )


def test_an_undeclared_subtype_is_still_rejected_by_the_same_evaluator(
    tmp_path: Path,
) -> None:
    """Non-vacuity partner for the signature check: the evaluator this
    suite trusts does reject something."""
    from okg.substrate.producer_authority import (
        ProducerPolicyViolation,
        ProducerProfile,
        _evaluate_records,
    )

    entry = upload_source_registry_entry(ownership_id="sig.upload")
    policy = entry["admission_policy"]
    profile = ProducerProfile(
        deployment_name="sig",
        producer_id=policy["producer_id"],
        producer_kind=policy["producer_kind"],
        trust_label=policy["trust_label"],
        admission_mode=policy["admission_mode"],
        authority_scope={},
        allowed_record_kinds=tuple(policy["allowed_record_kinds"]),
        output_signature=policy["output_signature"],
        policy_clauses=tuple(policy["policy_clauses"]),
        output_scope_summary={},
        source_ref={},
        catalog_version_id=None,
        profile_fingerprint="upload-signature-fixture",
        status="active",
    )
    with pytest.raises(ProducerPolicyViolation):
        _evaluate_records(
            profile=profile,
            endpoint_scope="the records supplied to this evaluation",
            node_records=[
                {
                    "fact_id": 1,
                    "node_id": "person:1",
                    "subtype": "person",
                    "attrs": {},
                    "op": "I",
                }
            ],
            edge_records=[],
        )


def test_the_declared_classification_is_what_the_ingest_path_resolves(
    deployment_dir: Path,
) -> None:
    """The registration's classification is not a decorative echo: it is
    the same posture `okg.substrate.ingest.pii_posture` derives when the
    facts are written."""
    from okg.substrate.ingest.pii_posture import (
        resolve_deployment_source_pii_posture,
    )
    from okg.substrate.sources.registry import load_source_registry

    reg = _registration(deployment_dir)
    manifest = yaml.safe_load((deployment_dir / "deployment.yaml").read_text())
    entry = load_source_registry(deployment_dir / "source_registry.yaml")["upload"]
    posture = resolve_deployment_source_pii_posture(
        manifest=manifest,
        source_name="upload",
        source_entry=entry.raw,
        build_detector=False,
    )
    assert posture.store_raw is reg.store_raw
    assert reg.sensitivity == "local_private"
    assert reg.data_classification == "private"


def test_an_unexpanded_placeholder_in_staging_dir_is_refused(
    tmp_path: Path,
) -> None:
    """A placeholder the registry loader does not expand must not become a
    literal directory: the staging write would succeed there while the
    adapter read a different path — a success report over a silent
    divergence."""
    dep_dir = _write_deployment(tmp_path / "deployments", name="ph")
    registry = yaml.safe_load((dep_dir / "source_registry.yaml").read_text())
    registry["sources"]["upload"]["params"]["staging_dir"] = (
        "${not_a_registry_placeholder}/uploads"
    )
    (dep_dir / "source_registry.yaml").write_text(yaml.safe_dump(registry))

    with pytest.raises(UploadSourceUnclassified) as excinfo:
        _registration(dep_dir, deployment="ph")
    assert "${not_a_registry_placeholder}" in str(excinfo.value)


def test_registry_placeholders_resolve_to_the_same_path_the_loader_gives(
    tmp_path: Path,
) -> None:
    """Non-vacuity partner: a placeholder the loader DOES expand resolves
    here to exactly the loader's answer, so the affordance and the adapter
    cannot read different directories."""
    from okg.substrate.sources.registry import load_source_registry

    dep_dir = _write_deployment(tmp_path / "deployments", name="expand")
    registry = yaml.safe_load((dep_dir / "source_registry.yaml").read_text())
    registry["sources"]["upload"]["params"]["staging_dir"] = (
        "${source_registry_dir}/state/uploads"
    )
    (dep_dir / "source_registry.yaml").write_text(yaml.safe_dump(registry))

    reg = _registration(dep_dir, deployment="expand")
    loaded = load_source_registry(dep_dir / "source_registry.yaml")["upload"]
    assert Path(loaded.raw["params"]["staging_dir"]) == reg.staging_dir
    assert reg.staging_dir == dep_dir / "state" / "uploads"


# --------------------------------- no registration means no upload path


def test_unregistered_deployment_refuses_and_names_the_registration(
    tmp_path: Path,
) -> None:
    """scn.chat-upload-source.unregistered-deployment-refuses."""
    dep_dir = _write_deployment(
        tmp_path / "deployments", name="bare", register_upload=False,
    )
    with pytest.raises(UploadSourceNotRegistered) as excinfo:
        _registration(dep_dir, deployment="bare")

    err = excinfo.value
    assert "bare" in str(err)
    assert err.data["required_module"] == UPLOAD_SOURCE_MODULE
    assert err.data["required_class"] == UPLOAD_SOURCE_CLASS
    assert "source_registry.yaml" in str(err.data["source_registry"])


def test_registered_without_classification_refuses(tmp_path: Path) -> None:
    """A registration with no reachable Nomos policy is not a registration.

    Distinct from the not-registered arm: the entry exists, so a check
    that only looked for the entry would pass here and let unclassified
    content through.
    """
    dep_dir = _write_deployment(
        tmp_path / "deployments",
        name="unclassified",
        declare_policy=False,
        inline_policy=False,
    )
    with pytest.raises(UploadSourceUnclassified) as excinfo:
        _registration(dep_dir, deployment="unclassified")
    assert "unclassified" in str(excinfo.value)
    assert excinfo.value.data["source_id"] == "upload"


def test_direct_upload_attempt_on_unregistered_deployment_is_refused(
    tmp_path: Path, resolver,
) -> None:
    _write_deployment(
        tmp_path / "deployments", name="bare", register_upload=False,
    )
    result = upload_admin.upload_to_staging(
        "bare",
        uploaded_by="operator@example.org",
        filename="notes.md",
        content_base64=base64.b64encode(b"# hi\n").decode("ascii"),
        deployment_dir_resolver=resolver,
    )
    assert result["status"] == "upload_source_not_registered"
    assert result["registration"]["required_module"] == UPLOAD_SOURCE_MODULE
    assert result["registration"]["required_class"] == UPLOAD_SOURCE_CLASS
    assert "bare" in result["summary"]
    # Nothing was written anywhere: a refusal must not leave the bytes
    # sitting in some other location.
    written = sorted(
        str(p.relative_to(tmp_path))
        for p in (tmp_path / "deployments" / "bare").rglob("*")
        if p.is_file()
    )
    assert written == [
        "deployments/bare/deployment.yaml",
        "deployments/bare/extractors.yaml",
        "deployments/bare/source_registry.yaml",
    ]


def test_direct_upload_attempt_on_registered_deployment_stages(
    deployment_dir: Path, resolver,
) -> None:
    """Non-vacuity partner: the same call path DOES stage where the
    registration exists, so the refusal above is about the registration
    and not about a permanently broken handler."""
    result = upload_admin.upload_to_staging(
        DEPLOYMENT,
        uploaded_by="operator@example.org",
        filename="notes.md",
        content_base64=base64.b64encode(_MARKDOWN.encode("utf-8")).decode("ascii"),
        deployment_dir_resolver=resolver,
    )
    assert result["status"] == "staged", result
    assert result["source"] == OWNERSHIP_ID
    assert result["classification"]["data_classification"] == "private"
    assert result["classification"]["sensitivity"] == "local_private"

    staged = _registration(deployment_dir).staging().list_uploads()
    assert [u.origin for u in staged] == ["notes.md"]
    assert staged[0].uploaded_by == "operator@example.org"


def test_upload_rejects_a_request_naming_both_a_file_and_a_url(
    deployment_dir: Path, resolver,
) -> None:
    result = upload_admin.upload_to_staging(
        DEPLOYMENT,
        uploaded_by="operator@example.org",
        filename="notes.md",
        content_base64=base64.b64encode(b"x").decode("ascii"),
        url="https://example.org/a",
        deployment_dir_resolver=resolver,
    )
    assert result["status"] == "invalid_request"


# ------------------------------------------ the projected chat tool surface


def test_upload_tool_is_absent_without_the_registration(
    tmp_path: Path, resolver,
) -> None:
    _write_deployment(
        tmp_path / "deployments", name="bare", register_upload=False,
    )
    provisioned = upload_admin.provisioned_conditional_tools(
        "bare", deployment_dir_resolver=resolver,
    )
    assert provisioned == frozenset()

    server = _StubServer([*OPERATOR_TOOLS, *WRITE_ADMIN_TOOLS])
    apply_admitted_tool_surface(
        server,
        allow_writes=True,
        provisioned_conditional_tools=provisioned,
    )
    assert "upload" not in server._tool_manager._tools
    assert server._okg_unprovisioned_conditional_tools == ("upload",)
    # An unprovisioned conditional tool is NOT a failed registration; the
    # defect signal must stay clean or it stops being readable.
    assert server._okg_missing_admitted_tools == ()


def test_upload_tool_is_present_with_the_registration(
    deployment_dir: Path, resolver,
) -> None:
    """Non-vacuity partner for the absence assertion above."""
    provisioned = upload_admin.provisioned_conditional_tools(
        DEPLOYMENT, deployment_dir_resolver=resolver,
    )
    assert provisioned == frozenset({"upload"})

    server = _StubServer([*OPERATOR_TOOLS, *WRITE_ADMIN_TOOLS, "upload"])
    apply_admitted_tool_surface(
        server,
        allow_writes=True,
        provisioned_conditional_tools=provisioned,
    )
    assert "upload" in server._tool_manager._tools
    assert server._okg_unprovisioned_conditional_tools == ()
    assert server._okg_boundary_pruned_tools == ()


def test_upload_tool_is_absent_on_a_read_only_server(
    deployment_dir: Path, resolver,
) -> None:
    """Provisioning does not override the write/admin gate: the upload
    affordance rides the write surface, so a read-only launch has none."""
    provisioned = upload_admin.provisioned_conditional_tools(
        DEPLOYMENT, deployment_dir_resolver=resolver,
    )
    server = _StubServer([*OPERATOR_TOOLS, "upload"])
    apply_admitted_tool_surface(
        server,
        allow_writes=False,
        provisioned_conditional_tools=provisioned,
    )
    assert "upload" not in server._tool_manager._tools
    assert server._okg_boundary_pruned_tools == ("upload",)


def test_upload_is_a_conditional_admin_tool_not_a_default_one() -> None:
    assert "upload" in CONDITIONAL_ADMIN_TOOLS
    assert "upload" not in OPERATOR_TOOLS


# ------------------------------------------------- adapter contract checks


def test_adapter_declares_no_dsn_parameter() -> None:
    """`source_adapter_init_params` injects `dsn` only into constructors
    that declare it, so an absent parameter is a structural guarantee
    rather than a convention."""
    params = inspect.signature(UploadSource.__init__).parameters
    assert "dsn" not in params


def test_adapter_opens_no_graph_database_connection(
    deployment_dir: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    import psycopg

    from okg.substrate.db import pool as pool_mod

    def _explode(*_a: Any, **_kw: Any):
        raise AssertionError("the upload adapter must not reach the graph database")

    monkeypatch.setattr(psycopg, "connect", _explode)
    monkeypatch.setattr(pool_mod, "get_pool", _explode)

    reg = _registration(deployment_dir)
    reg.staging().stage_file(
        filename="reactor-notes.md",
        content=_MARKDOWN.encode("utf-8"),
        uploaded_by="operator@example.org",
    )
    facts = list(_adapter(deployment_dir).run("run-1", mode="reconcile").facts)
    assert any(isinstance(f, NodeFact) and f.subtype == "document" for f in facts)


def test_chunks_carry_the_operator_facing_citation_too(
    deployment_dir: Path,
) -> None:
    """Chunks are the unit search returns. Restamping documents alone
    would leave the unrecognizable staged blob name on exactly the rows a
    reader sees."""
    reg = _registration(deployment_dir)
    reg.staging().stage_file(
        filename="reactor-notes.md",
        content=_MARKDOWN.encode("utf-8"),
        uploaded_by="operator@example.org",
    )
    chunks = [
        f
        for f in _adapter(deployment_dir).run("run-1", mode="reconcile").facts
        if isinstance(f, NodeFact) and f.subtype == "document_chunk"
    ]
    assert chunks
    labels = {str(c.attrs.get("citation_label") or "") for c in chunks}
    assert labels == {"reactor-notes.md"}, labels
    assert not any("upload/" in label for label in labels)


def test_empty_staging_area_emits_nothing_and_retracts(
    deployment_dir: Path,
) -> None:
    """An empty staging area on a completed-scope run carries an EMPTY
    record set, not None.

    The runner gates all retraction on `record_set is not None`, so
    returning None after the last upload is deleted would leave its rows
    published forever while the cursor still claimed them.
    """
    source = _adapter(deployment_dir)
    run = source.run("run-1", mode="reconcile")
    assert list(run.facts) == []
    assert source.last_run_stats["scope_status"] == "empty_staging_area"
    assert source.last_run_stats["staged_uploads"] == 0
    assert run.completed_scope is True
    assert run.record_set is not None
    assert run.record_set.records == {}


def test_an_incomplete_scope_run_claims_no_completed_scope(
    deployment_dir: Path,
) -> None:
    """Partner reading: an fs-event run is not a reconcile, so it must not
    assert a completed scope (which would license retraction of everything
    it did not look at)."""
    run = _adapter(deployment_dir).run("run-1", mode="scope_incomplete")
    assert run.completed_scope is False
    assert run.record_set is None


def test_a_blob_without_a_sidecar_is_never_published(
    deployment_dir: Path,
) -> None:
    """An unattributable blob must not become a document named after the
    staging area's own bookkeeping."""
    reg = _registration(deployment_dir)
    staging = reg.staging()
    staging.stage_file(
        filename="reactor-notes.md",
        content=_MARKDOWN.encode("utf-8"),
        uploaded_by="operator@example.org",
    )
    (staging.content_dir / "smuggled.md").write_text("# Not an upload\n")
    (staging.content_dir / ".leftover.md.partial").write_text("# Partial\n")

    source = _adapter(deployment_dir)
    documents = [
        f
        for f in source.run("run-1", mode="reconcile").facts
        if isinstance(f, NodeFact) and f.subtype == "document"
    ]
    paths = {str(d.attrs.get("path") or "") for d in documents}
    assert len(documents) == 1
    assert not any("smuggled" in p or "partial" in p for p in paths)
    # Counted, not merely skipped: an unattributable blob is an anomaly an
    # operator should be able to see.
    assert source.last_run_stats["orphan_blobs"] == 1
    assert source.last_run_stats["restamp_misses"] == 0


def test_atomic_write_scratch_lives_outside_the_corpus_root(
    deployment_dir: Path,
) -> None:
    """A partial write left by a crash inside content/ would be walked as
    a document forever."""
    staging = _registration(deployment_dir).staging()
    staging.stage_file(
        filename="notes.md", content=b"# hi\n", uploaded_by="op",
    )
    assert staging.tmp_dir.is_dir()
    assert staging.tmp_dir not in staging.content_dir.parents
    assert staging.content_dir not in staging.tmp_dir.parents


def test_change_probe_is_sound_and_moves_with_the_staging_area(
    deployment_dir: Path,
) -> None:
    assert UploadSource.change_probe_kind in SOUND_PROBE_KINDS
    source = _adapter(deployment_dir)
    assert source.change_probe is not None

    before = source.change_probe.build_token()
    assert source.change_probe.build_token() == before, "probe must be stable"

    _registration(deployment_dir).staging().stage_file(
        filename="reactor-notes.md",
        content=_MARKDOWN.encode("utf-8"),
        uploaded_by="operator@example.org",
    )
    after = source.change_probe.build_token()
    assert after != before

    # And a deletion is observed too — a probe that only grows would let a
    # retracted upload look unchanged forever.
    staged = _registration(deployment_dir).staging()
    (staged.content_dir / staged.list_uploads()[0].staged_name).unlink()
    assert source.change_probe.build_token() != after


# ------------------------------------------------------- staging refusals


def test_url_fetch_refuses_a_loopback_target(deployment_dir: Path) -> None:
    """The real guard runs — no monkeypatch, no network: the address is
    rejected before any socket is opened."""
    staging = _registration(deployment_dir).staging()
    with pytest.raises(UnsafeFetchURL):
        staging.stage_url(
            url="http://127.0.0.1:1/secrets", uploaded_by="operator@example.org",
        )
    assert staging.list_uploads() == []
    assert not staging.content_dir.exists()


def test_url_fetch_refuses_a_non_http_scheme(deployment_dir: Path) -> None:
    staging = _registration(deployment_dir).staging()
    with pytest.raises(UnsafeFetchURL):
        staging.stage_url(
            url="file:///etc/passwd", uploaded_by="operator@example.org",
        )
    assert staging.list_uploads() == []


def test_upload_tool_surfaces_the_unsafe_url_refusal(
    deployment_dir: Path, resolver,
) -> None:
    result = upload_admin.upload_to_staging(
        DEPLOYMENT,
        uploaded_by="operator@example.org",
        url="http://169.254.169.254/latest/meta-data/",
        deployment_dir_resolver=resolver,
    )
    assert result["status"] == "refused_unsafe_url"


def test_a_url_carrying_credentials_is_refused(deployment_dir: Path) -> None:
    """The URL becomes the document's title and citation, reaches the BM25
    searchable-text stamp, and is echoed in the tool response. A refusal
    tells the operator the credential was seen; silent stripping would
    not."""
    staging = _registration(deployment_dir).staging()
    with pytest.raises(UploadRejected) as excinfo:
        staging.stage_url(
            url="https://user:s3cret@example.org/private.md",
            uploaded_by="operator@example.org",
        )
    assert "s3cret" not in str(excinfo.value)
    assert staging.list_uploads() == []


def test_a_content_encoded_response_is_refused(
    deployment_dir: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """urllib does not decode content encodings; storing the compressed
    bytes would publish a document of garbage with no error anywhere."""
    response = _FakeResponse(b"\x1f\x8b\x08garbage", "text/plain")
    response.headers["Content-Encoding"] = "gzip"
    monkeypatch.setattr(
        upload_mod, "open_validated_url", lambda _url, **_kw: response,
    )
    staging = _registration(deployment_dir).staging()
    with pytest.raises(UploadRejected) as excinfo:
        staging.stage_url(
            url="https://example.org/doc", uploaded_by="operator@example.org",
        )
    assert "gzip" in str(excinfo.value)
    assert staging.list_uploads() == []


def test_the_response_media_type_selects_the_parser_lane_not_the_url_path(
    deployment_dir: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The extension picks the parser. A URL path is chosen by whoever
    wrote the link, so a `.pdf` path serving HTML must not route into the
    PDF lane (which, on a captioning deployment, means model calls)."""
    response = _FakeResponse(b"<html><body><p>Actually HTML.</p></body></html>", "text/html")
    monkeypatch.setattr(
        upload_mod, "open_validated_url", lambda _url, **_kw: response,
    )
    staged = _registration(deployment_dir).staging().stage_url(
        url="https://example.org/looks-like-a.pdf",
        uploaded_by="operator@example.org",
    )
    assert staged.staged_name.endswith(".html"), staged.staged_name
    assert not staged.staged_name.endswith(".pdf")


def test_an_upload_no_parser_would_read_is_refused_and_unstaged(
    deployment_dir: Path, resolver,
) -> None:
    """A format the deployment cannot parse publishes a document with zero
    chunks — content that is not retrievable, reported as success. Refuse
    at the door instead, and leave nothing behind for the next ingest."""
    result = upload_admin.upload_to_staging(
        DEPLOYMENT,
        uploaded_by="operator@example.org",
        filename="scan.tiff",
        content_base64=base64.b64encode(b"II*\x00 not really a tiff").decode("ascii"),
        deployment_dir_resolver=resolver,
    )
    assert result["status"] == "refused_unparseable", result
    assert _registration(deployment_dir).staging().list_uploads() == []


def test_a_parseable_upload_is_not_refused(
    deployment_dir: Path, resolver,
) -> None:
    """Non-vacuity partner: the pre-flight admits what the deployment can
    actually parse."""
    result = upload_admin.upload_to_staging(
        DEPLOYMENT,
        uploaded_by="operator@example.org",
        filename="notes.md",
        content_base64=base64.b64encode(_MARKDOWN.encode("utf-8")).decode("ascii"),
        deployment_dir_resolver=resolver,
    )
    assert result["status"] == "staged", result


@pytest.mark.parametrize("bad", ["../../etc/passwd", "a/b.md", "..", ""])
def test_a_path_is_refused_rather_than_reduced_to_a_basename(
    deployment_dir: Path, bad: str,
) -> None:
    staging = _registration(deployment_dir).staging()
    with pytest.raises(UploadRejected):
        staging.stage_file(
            filename=bad, content=b"x", uploaded_by="operator@example.org",
        )
    assert staging.list_uploads() == []


def test_oversize_and_empty_uploads_are_refused(tmp_path: Path) -> None:
    staging = UploadStaging(tmp_path / "staging", max_upload_bytes=16)
    with pytest.raises(UploadRejected):
        staging.stage_file(
            filename="big.txt", content=b"x" * 17, uploaded_by="op",
        )
    with pytest.raises(UploadRejected):
        staging.stage_file(filename="empty.txt", content=b"", uploaded_by="op")
    assert staging.list_uploads() == []


def test_restaging_identical_bytes_is_idempotent(tmp_path: Path) -> None:
    staging = UploadStaging(tmp_path / "staging")
    first = staging.stage_file(
        filename="notes.md", content=b"# hi\n", uploaded_by="op",
    )
    second = staging.stage_file(
        filename="notes.md", content=b"# hi\n", uploaded_by="op",
    )
    assert first.upload_id == second.upload_id
    assert len(staging.list_uploads()) == 1
    assert len(staging.content_paths()) == 1


def test_a_sidecar_without_its_blob_is_not_ingested(tmp_path: Path) -> None:
    """A half-written pair must not become a document with no bytes."""
    staging = UploadStaging(tmp_path / "staging")
    staged = staging.stage_file(
        filename="notes.md", content=b"# hi\n", uploaded_by="op",
    )
    (staging.content_dir / staged.staged_name).unlink()
    assert staging.list_uploads() == []


# ------------------------------- no Open WebUI knowledge / file / vector store


#: Every file the upload path runs through: the adapter and staging area,
#: the affordance handler, AND the server wiring where the tool is
#: registered — the wiring is where a second store would most plausibly be
#: reached from, so scanning only the first two would leave the obvious
#: place unscanned.
_MCP_DIR = Path(upload_admin.__file__).parent
_UPLOAD_IMPLEMENTATION_FILES = (
    Path(upload_mod.__file__),
    Path(upload_admin.__file__),
    _MCP_DIR / "tool_registration.py",
    _MCP_DIR / "server.py",
)

#: Names of the Open WebUI storage surfaces the program's knowledge stance
#: (design.md section 2) keeps off, plus the vector backends a SECOND,
#: competing store would need. Matched case-insensitively.
#:
#: `pgvector` is deliberately NOT here. The substrate's own semantic index
#: lives in the deployment's graph database and IS the governed store —
#: banning its name would ban the thing the stance is protecting, which is
#: how a taboo list stops tracking the property it claims to check.
_FORBIDDEN_STORE_TOKENS = (
    "/api/v1/knowledge",
    "/api/v1/files",
    "/api/v1/memories",
    "knowledge_base",
    "vector_db",
    "vectordb",
    "chromadb",
    "qdrant",
    "milvus",
    "faiss",
    "open_webui",
    "openwebui",
)


def test_no_openwebui_knowledge_file_or_vector_store_participates() -> None:
    """The integration acceptance's negative assertion, at code level.

    An upload's only destinations are this source's staging area and,
    after ingest, the graph. Nothing on the upload path — adapter,
    staging, affordance handler, or the server wiring that registers the
    tool — reaches an Open WebUI knowledge base, file store, or memory
    endpoint, and no vector store is written.

    SCOPE, stated because the requirement's wording is broader than any
    grep can settle: this proves the OKG-side code opens no such store.
    Whether the Open WebUI INSTANCE has its knowledge/RAG/memory features
    disabled is a deployment-configuration property, verified by readback
    in the sibling `chat-sync-projection` child (design.md section 7,
    Plane 5), not here.
    """
    assert _UPLOAD_IMPLEMENTATION_FILES, "the scan must have something to scan"
    hits: list[tuple[str, str]] = []
    read_ok = 0
    for path in _UPLOAD_IMPLEMENTATION_FILES:
        text = path.read_text(encoding="utf-8").lower()
        # Control: a token that IS present in any Python module, proving
        # the scanner reads the bytes it claims to. Without it, a broken
        # read would report "no forbidden tokens" for every file on earth.
        if "def " in text:
            read_ok += 1
        for token in _FORBIDDEN_STORE_TOKENS:
            if token in text:
                hits.append((path.name, token))
    assert read_ok == len(_UPLOAD_IMPLEMENTATION_FILES), (
        "control token missing; the forbidden-token scan is not reading "
        "the implementation"
    )
    # Second control, semantic rather than syntactic: the scanner does
    # find the thing the upload path IS built on.
    assert "staging" in Path(upload_mod.__file__).read_text().lower()
    assert hits == [], f"an ungoverned store leaked into the upload path: {hits}"


def test_staging_holds_exactly_one_copy_outside_the_graph(
    deployment_dir: Path,
) -> None:
    """The blob plus its sidecar are the whole footprint: no index, no
    embedding cache, no second retrieval store appears alongside."""
    reg = _registration(deployment_dir)
    staging = reg.staging()
    staging.stage_file(
        filename="reactor-notes.md",
        content=_MARKDOWN.encode("utf-8"),
        uploaded_by="operator@example.org",
    )
    list(_adapter(deployment_dir).run("run-1", mode="reconcile").facts)

    written = sorted(
        str(p.relative_to(staging.staging_dir))
        for p in staging.staging_dir.rglob("*")
        if p.is_file()
    )
    staged = staging.list_uploads()[0]
    assert written == sorted(
        [
            f"content/{staged.staged_name}",
            f"manifest/{staged.upload_id}.json",
        ]
    )
