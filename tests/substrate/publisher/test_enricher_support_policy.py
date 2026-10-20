"""Issue #1256: the derived-node support policy is a boot-time contract.

Before this change ``support_policy`` was declared ``str | None = None`` on
both the fact emitter and the extractor base class, while
``canonical_derived_support`` refused ``None`` unconditionally — per emitted
candidate, inside ``okg_publisher_cycle``. A downstream enricher that took
the signature at its word took the deployment's publishing down and held the
publish lock.

These tests pin the two halves of the fix:

- the signature tells the truth (required parameter, Literal-typed), and
- a node-writing enricher with no valid declaration is refused at enricher
  registration and at worker boot, naming the enricher.
"""
from __future__ import annotations

import inspect
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from okg.substrate.derived_node_liveness import DERIVED_SUPPORT_POLICIES
from okg.substrate.library.enrichers import DerivedSupportPolicy
from okg.substrate.library.enrichers._fact_emitters import emit_node_fact
from okg.substrate.library.enrichers.extraction.base import LLMExtractor
from okg.substrate.publisher.enricher_loader import (
    EnricherSupportPolicyInvalid,
    load_enrichers,
    validate_enricher_envelopes,
    validate_enricher_support_policies,
)

_ENRICHER_SOURCE = '''
from okg.substrate.library.enrichers import DerivedSupportPolicy


class _Base:
    reads_subtypes = ("paper",)
    emits_typed_subtype = "region"
    declares_node_provenance = True
    nondeterministic = True

    def enrich(self, conn, *, generation_id, incremental=None):
        raise AssertionError("fixture enricher is never run")


class OmitsPolicy(_Base):
    name = "omits_policy"


class InvalidPolicy(_Base):
    name = "invalid_policy"
    support_policy = "sometimes"


class DeclaresPolicy(_Base):
    name = "declares_policy"
    support_policy: DerivedSupportPolicy = "any_member"


class EdgeOnly:
    """Writes edges only — no node liveness declaration to make."""

    name = "edge_only"
    nondeterministic = False
    requires_narrowings = (("paper", "mentions", "region"),)

    def enrich(self, conn, *, generation_id, incremental=None):
        raise AssertionError("fixture enricher is never run")
'''


def _deployment_with(tmp_path: Path, class_name: str) -> Path:
    """A deployment manifest registering one deployment-local enricher."""
    dep = tmp_path / f"dep_{class_name.lower()}"
    package = dep / "enrichers"
    package.mkdir(parents=True)
    (package / "__init__.py").write_text("")
    (package / "policy_fixture.py").write_text(_ENRICHER_SOURCE)
    (dep / "deployment.yaml").write_text(yaml.safe_dump({
        "name": f"policy-fixture-{class_name.lower()}",
        "enrichers": [
            {"class": f"enrichers.policy_fixture.{class_name}"},
        ],
    }))
    return dep


@pytest.fixture(autouse=True)
def _evict_fixture_package():
    """The fixture package name is reused across tmp deployments."""
    saved_path = list(sys.path)
    yield
    sys.path[:] = saved_path
    for name in list(sys.modules):
        if name == "enrichers" or name.startswith("enrichers."):
            sys.modules.pop(name, None)


# --------------------------------------------------------------------- #
# The signatures tell the truth
# --------------------------------------------------------------------- #


def test_emit_node_fact_requires_support_policy() -> None:
    parameter = inspect.signature(emit_node_fact).parameters["support_policy"]
    assert parameter.default is inspect.Parameter.empty, (
        "support_policy is mandatory at runtime; a None default advertises "
        "a value that always raises"
    )
    assert parameter.kind is inspect.Parameter.KEYWORD_ONLY


def test_emit_node_fact_rejects_a_call_that_omits_the_policy() -> None:
    with pytest.raises(TypeError, match="support_policy"):
        emit_node_fact(  # type: ignore[call-arg]
            None,
            enricher_name="fixture",
            node_id="n:1",
            subtype="region",
            attrs={},
            dedupe_key="d",
            source_record_id={},
            source_revision={},
            run_id="r",
            observed_at=None,
        )


def test_extractor_base_declares_no_default_policy() -> None:
    """The base class annotates the policy; it never supplies a value."""
    assert "support_policy" not in vars(LLMExtractor)
    assert vars(LLMExtractor)["__annotations__"]["support_policy"] in (
        DerivedSupportPolicy,
        "DerivedSupportPolicy",
    )


def test_undeclared_extractor_policy_raises_naming_the_class() -> None:
    class _NoPolicy(LLMExtractor):
        name = "no_policy_fixture"
        emits_typed_subtype = "region"

    extractor = _NoPolicy.__new__(_NoPolicy)
    with pytest.raises(ValueError, match="_NoPolicy"):
        extractor.declared_support_policy()


def test_declared_extractor_policy_resolves() -> None:
    class _Policy(LLMExtractor):
        name = "policy_fixture"
        emits_typed_subtype = "region"
        support_policy: DerivedSupportPolicy = "any_member"

    extractor = _Policy.__new__(_Policy)
    assert extractor.declared_support_policy() == "any_member"


# --------------------------------------------------------------------- #
# The boot pass refuses, before any candidate is emitted
# --------------------------------------------------------------------- #


@pytest.mark.parametrize("class_name", ["OmitsPolicy", "InvalidPolicy"])
def test_worker_preflight_refuses_a_node_writer_without_policy(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    class_name: str,
) -> None:
    """End to end: a real deployment directory refuses the worker boot."""
    import okg.substrate.deployments as deployments
    from okg.substrate.runtime.worker import _preflight_enricher_contracts

    dep = _deployment_with(tmp_path, class_name)
    monkeypatch.setattr(
        deployments, "find_deployment_dir", lambda deployment: dep,
    )
    with pytest.raises(EnricherSupportPolicyInvalid) as exc_info:
        _preflight_enricher_contracts(
            deployment="policy-fixture", dsn="postgresql://not-consulted",
        )
    err = exc_info.value
    assert err.code == "enricher_support_policy_invalid"
    assert err.data["enricher"] in {"omits_policy", "invalid_policy"}
    assert err.data["producer_id"].endswith(err.data["enricher"])
    assert err.data["allowed"] == sorted(DERIVED_SUPPORT_POLICIES)
    assert err.data["enricher"] in str(err)
    assert "DerivedSupportPolicy" in (err.fix_hint or "")


def test_worker_preflight_accepts_a_declared_policy(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    import okg.substrate.deployments as deployments
    from okg.substrate.runtime.worker import _preflight_enricher_contracts

    dep = _deployment_with(tmp_path, "DeclaresPolicy")
    monkeypatch.setattr(
        deployments, "find_deployment_dir", lambda deployment: dep,
    )
    _preflight_enricher_contracts(
        deployment="policy-fixture", dsn="postgresql://not-consulted",
    )


def test_worker_preflight_accepts_an_edge_only_enricher(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """The check is scoped to node writers, not every registered enricher."""
    import okg.substrate.deployments as deployments
    from okg.substrate.runtime.worker import _preflight_enricher_contracts

    dep = _deployment_with(tmp_path, "EdgeOnly")
    monkeypatch.setattr(
        deployments, "find_deployment_dir", lambda deployment: dep,
    )
    _preflight_enricher_contracts(
        deployment="policy-fixture", dsn="postgresql://not-consulted",
    )


@pytest.mark.parametrize("class_name", ["OmitsPolicy", "DeclaresPolicy"])
def test_load_enrichers_stays_a_pure_loader(
    tmp_path: Path,
    class_name: str,
) -> None:
    """The loader is also the doctor and upgrade inventory path.

    Refusing there would empty the derived-producer inventory those callers
    build, turning a named contract error into misleading downstream
    warnings. The refusal belongs to the envelope pass, which both the
    worker preflight and Publisher construction run.
    """
    expected = {
        "OmitsPolicy": "omits_policy",
        "DeclaresPolicy": "declares_policy",
    }[class_name]
    dep = _deployment_with(tmp_path, class_name)
    loaded = load_enrichers(dep)
    assert [e.name for e in loaded] == [expected]


def test_support_policy_pass_refuses_a_missing_policy() -> None:
    class _OmitsPolicy:
        name = "envelope_omits_policy"
        emits_typed_subtype = "region"
        declares_node_provenance = True
        nondeterministic = True

    with pytest.raises(EnricherSupportPolicyInvalid) as exc_info:
        validate_enricher_support_policies([_OmitsPolicy()])
    assert "envelope_omits_policy" in str(exc_info.value)


def test_support_policy_pass_covers_unbounded_node_writers() -> None:
    """An unbounded node surface has no clauses; it is still a node writer."""
    class _Unbounded:
        name = "unbounded_omits_policy"
        declares_node_provenance = True
        unbounded_write_dimensions = ("nodes",)

    with pytest.raises(EnricherSupportPolicyInvalid) as exc_info:
        validate_enricher_support_policies([_Unbounded()])
    assert "unbounded_omits_policy" in str(exc_info.value)


def test_hand_built_publisher_validation_is_unchanged() -> None:
    """The policy pass is NOT bolted onto every Publisher construction.

    ``validate_enricher_envelopes`` runs for in-process publishers that never
    came from a deployment manifest, including a deployment's own code and
    this repository's test fixtures. Scoping the new refusal to the two
    deployment-loading paths keeps that surface exactly as it was; the
    per-candidate refusal remains the backstop there.
    """
    class _OmitsPolicy:
        name = "hand_built_omits_policy"
        emits_typed_subtype = "region"
        declares_node_provenance = True
        nondeterministic = True

    envelopes = validate_enricher_envelopes([_OmitsPolicy()])
    assert "enricher:hand_built_omits_policy" in envelopes


def test_the_publisher_stack_validates_what_it_loads(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """The other deployment-loading path: building the publisher stack."""
    import okg.substrate.deployments as deployments
    from okg.substrate.publisher import deployment_stack

    dep = _deployment_with(tmp_path, "OmitsPolicy")
    monkeypatch.setattr(
        deployments, "find_deployment_dir", lambda name: dep,
    )
    with pytest.raises(EnricherSupportPolicyInvalid):
        deployment_stack._build_deployment_publisher_stack_uncached(
            dsn="postgresql://not-consulted",
            deployment_name="policy-fixture",
            deployment_dir=dep,
        )


def test_an_inherited_policy_is_accepted() -> None:
    """A subclass of a shipped extractor inherits its derivation semantics.

    Refusing an inherited declaration would break legitimate reuse; what is
    refused is the absence of any declaration in the MRO.
    """
    from okg.substrate.library.enrichers.extraction.claim_extractor import (
        ClaimExtractor,
    )

    class _InheritingExtractor(ClaimExtractor):
        name = "inheriting_extractor"

    inheriting = _InheritingExtractor.__new__(_InheritingExtractor)
    assert inheriting.declared_support_policy() == "all_members"
    validate_enricher_support_policies([inheriting])


def test_an_enricher_with_no_write_surface_still_fails_on_its_envelope() -> None:
    """The policy check must not pre-empt the envelope-or-refuse contract."""
    from okg.substrate.publisher.enricher_loader import EnricherEnvelopeMissing

    class _NoSurface:
        name = "no_write_surface"

    validate_enricher_support_policies([_NoSurface()])
    with pytest.raises(EnricherEnvelopeMissing) as exc_info:
        validate_enricher_envelopes([_NoSurface()])
    assert exc_info.value.code == "enricher_envelope_missing"


# --------------------------------------------------------------------- #
# Every shipped node writer declares one
# --------------------------------------------------------------------- #


def _shipped_node_writer_classes() -> list[type]:
    """Every class under library/enrichers that declares a node surface.

    Walked, not listed: a hand-maintained census silently stops covering the
    next node writer someone adds.
    """
    import importlib
    import pkgutil

    import okg.substrate.library.enrichers as package

    found: dict[str, type] = {}
    for module_info in pkgutil.walk_packages(
        package.__path__, prefix=f"{package.__name__}.",
    ):
        module = importlib.import_module(module_info.name)
        for attribute in vars(module).values():
            if not isinstance(attribute, type):
                continue
            if attribute.__module__ != module_info.name:
                continue
            writes_nodes = (
                bool(getattr(attribute, "emits_typed_subtype", ""))
                or bool(getattr(attribute, "emits_node_subtypes", ()))
                or "nodes" in tuple(
                    getattr(attribute, "unbounded_write_dimensions", ()) or ()
                )
            )
            if not writes_nodes:
                continue
            # The base classes annotate the policy without a value on
            # purpose; concrete enrichers are the ones that must declare it.
            if "support_policy" in getattr(
                attribute, "__annotations__", {},
            ) and "support_policy" not in vars(attribute):
                continue
            found[f"{module_info.name}.{attribute.__name__}"] = attribute
    return sorted(found.items())


def test_shipped_node_writing_enrichers_declare_a_valid_policy() -> None:
    shipped = _shipped_node_writer_classes()
    names = {name.rsplit(".", 1)[-1] for name, _ in shipped}
    # Non-vacuous: an empty or shrunken walk would pass silently.
    assert len(shipped) >= 10, sorted(names)
    assert {
        "ClaimExtractor",
        "ConceptExtractor",
        "EvidenceExtractor",
        "HypothesisExtractor",
        "LatexStructuralEnricher",
        "MarkdownStructuralEnricher",
        "PdfStructuralEnricher",
        "VisualCaptionEnricher",
        "YamlStructuralEnricher",
    } <= names, sorted(names)
    undeclared = [
        name
        for name, cls in shipped
        if getattr(cls, "support_policy", None) not in DERIVED_SUPPORT_POLICIES
    ]
    assert not undeclared, (
        f"shipped node-writing enrichers with no valid support_policy: "
        f"{undeclared}"
    )


def test_pdf_structural_declares_the_nodes_it_writes() -> None:
    """Issue #1522: nodes written through a shared helper are declared too.

    ``PdfStructuralEnricher`` commits paper anatomy nodes via
    ``_commit_anatomy`` and an ``extraction_error`` observation via
    ``_emit_extraction_error``, but used to declare only ``emits_edge_types``
    and ``requires_narrowings``. The derived envelope therefore carried an
    empty node surface, and BOTH boot checks — node provenance and this
    policy — returned early on it, so its node facts reached the publisher
    unchecked. The node subtypes come from the pluggable parser backend, so
    the declaration is an unbounded node dimension, matching the LaTeX path
    that shares the helper.
    """
    from okg.substrate.publisher.enricher_loader import (
        EnricherEnvelopeMissing,
        derive_write_envelope,
    )
    from okg.substrate.library.enrichers.scholarly.pdf_structural import (
        PdfStructuralEnricher,
    )

    enricher = PdfStructuralEnricher()
    envelope = derive_write_envelope(
        enricher, producer_id="enricher:pdf_structural",
    )
    assert "nodes" in tuple(getattr(envelope, "unbounded", ()) or ()), (
        "the anatomy nodes must be a declared write dimension, or the "
        "boot-time node checks skip this producer"
    )
    assert PdfStructuralEnricher.support_policy in DERIVED_SUPPORT_POLICIES
    assert PdfStructuralEnricher.declares_node_provenance is True
    validate_enricher_envelopes([enricher])
    validate_enricher_support_policies([enricher])

    # Non-vacuous: both checks must REACH this producer, not merely pass on
    # it. Each refusal below is exactly the one an edge-only declaration
    # silenced.
    class _WithoutProvenance(PdfStructuralEnricher):
        name = "pdf_structural_without_provenance"
        declares_node_provenance = False

    with pytest.raises(EnricherEnvelopeMissing) as provenance_error:
        validate_enricher_envelopes([_WithoutProvenance()])
    assert "pdf_structural_without_provenance" in str(provenance_error.value)

    class _WithoutPolicy(PdfStructuralEnricher):
        name = "pdf_structural_without_policy"
        support_policy = None  # type: ignore[assignment]

    with pytest.raises(EnricherSupportPolicyInvalid) as policy_error:
        validate_enricher_support_policies([_WithoutPolicy()])
    assert "pdf_structural_without_policy" in str(policy_error.value)


_NODE_EMITTING_HELPERS = (
    "emit_node_fact",
    "_commit_anatomy",
    "_emit_extraction_error",
)


def _enrichers_reaching_a_node_emitter() -> list[tuple[str, type]]:
    """Shipped enricher classes that reach a node-writing helper.

    Issue #1522: the declaration walk above starts from the declaration, so a
    producer that writes nodes through a shared helper while declaring none is
    invisible to it — the exact gap that let the PDF path through. This walk
    starts from the write instead.

    Reaching is resolved through the whole MRO, not just the class own module:
    the code-graph structural enrichers write their nodes in a base class in
    another module, so a single-module match would miss precisely the
    inherited form of the same shared-helper pattern.
    """
    import importlib
    import inspect
    import pkgutil

    import okg.substrate.library.enrichers as package

    emitting_modules: set[str] = set()
    modules: dict[str, object] = {}
    for module_info in pkgutil.walk_packages(
        package.__path__, prefix=f"{package.__name__}.",
    ):
        module = importlib.import_module(module_info.name)
        modules[module_info.name] = module
        try:
            source = inspect.getsource(module)
        except OSError:  # pragma: no cover - source always available in repo
            continue
        if any(helper in source for helper in _NODE_EMITTING_HELPERS):
            emitting_modules.add(module_info.name)

    found: dict[str, type] = {}
    for module_name, module in modules.items():
        for attribute in vars(module).values():
            if not isinstance(attribute, type):
                continue
            if attribute.__module__ != module_name:
                continue
            if not callable(getattr(attribute, "enrich", None)):
                continue
            if not any(
                ancestor.__module__ in emitting_modules
                for ancestor in attribute.__mro__
            ):
                continue
            found[f"{module_name}.{attribute.__name__}"] = attribute
    return sorted(found.items())


# Every shipped enricher that reaches a node-writing helper, pinned by name.
# A floor alone would let the walk shrink silently onto a smaller set; this
# set has to be edited deliberately when a node writer is added or removed.
_ENRICHERS_REACHING_A_NODE_EMITTER = {
    "ClaimExtractor",
    "ConceptExtractor",
    "EvidenceExtractor",
    "HypothesisExtractor",
    "JsonStructuralEnricher",
    "LLMExtractor",
    "LatexStructuralEnricher",
    "MarkdownStructuralEnricher",
    "NonPythonStructuralEnricher",
    "PdfStructuralEnricher",
    "TomlStructuralEnricher",
    "VisualCaptionEnricher",
    "YamlStructuralEnricher",
}


def test_every_enricher_that_reaches_a_node_emitter_declares_nodes() -> None:
    reaching = _enrichers_reaching_a_node_emitter()
    names = {name.rsplit(".", 1)[-1] for name, _ in reaching}
    # Non-vacuous: a shrunken or slid walk fails here rather than passing.
    assert names == _ENRICHERS_REACHING_A_NODE_EMITTER, sorted(names)
    undeclared = [
        name
        for name, cls in reaching
        if not (
            bool(getattr(cls, "emits_typed_subtype", ""))
            or bool(getattr(cls, "emits_node_subtypes", ()))
            or "nodes" in tuple(
                getattr(cls, "unbounded_write_dimensions", ()) or ()
            )
        )
    ]
    assert not undeclared, (
        f"enrichers that reach a node emitter but declare no node write "
        f"surface, so the boot-time node checks skip them: {undeclared}"
    )


# --------------------------------------------------------------------- #
# Worker boot refuses before DBOS is constructed
# --------------------------------------------------------------------- #


def test_worker_preflight_loads_the_active_enricher_set(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    import okg.substrate.deployments as deployments
    import okg.substrate.publisher.enricher_loader as loader
    from okg.substrate.runtime.worker import _preflight_enricher_contracts

    calls: list[dict[str, object]] = []
    monkeypatch.setattr(
        deployments, "find_deployment_dir", lambda deployment: tmp_path,
    )
    monkeypatch.setattr(
        loader, "active_enricher_profiles_from_env", lambda: {"active"},
    )

    def _load_enrichers(path, active_profiles, *, runtime_dsn, deployment):
        calls.append({
            "path": path,
            "active_profiles": active_profiles,
            "runtime_dsn": runtime_dsn,
            "deployment": deployment,
        })
        return []

    monkeypatch.setattr(loader, "load_enrichers", _load_enrichers)
    _preflight_enricher_contracts(
        deployment="fixture", dsn="postgresql://runtime-authority",
    )
    assert calls == [{
        "path": tmp_path,
        "active_profiles": {"active"},
        "runtime_dsn": "postgresql://runtime-authority",
        "deployment": "fixture",
    }]


def test_worker_refuses_to_boot_on_an_invalid_policy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The refusal lands before DBOS is constructed, not mid-cycle."""
    import okg.substrate.deployment_authority as authority
    import okg.substrate.deployments as deployments
    import okg.substrate.deployments.sync_root as sync_root
    import okg.substrate.runtime.code_identity as code_identity
    import okg.substrate.runtime.release_authority as release_authority
    import okg.substrate.runtime.worker as worker
    from okg.substrate.runtime.config import RuntimeConfig

    calls: list[str] = []

    def _called(name: str):
        def _trap(*_args, **_kwargs):
            calls.append(name)
        return _trap

    class _TrapDBOS:
        def __init__(self, *_args, **_kwargs) -> None:
            calls.append("DBOS")

        @classmethod
        def launch(cls) -> None:
            calls.append("DBOS.launch")

    monkeypatch.setitem(sys.modules, "dbos", SimpleNamespace(DBOS=_TrapDBOS))
    monkeypatch.setitem(
        sys.modules,
        "okg.substrate.runtime.partition_queue",
        SimpleNamespace(
            register_deployment_partition_queue=_called("partition_queue"),
            register_source_sync_partition_queue=_called("source_sync_queue"),
        ),
    )
    monkeypatch.setitem(
        sys.modules,
        "okg.substrate.runtime.publisher_workflow",
        SimpleNamespace(
            register_publisher_workflow=_called("publisher_workflow"),
        ),
    )
    monkeypatch.setattr(
        release_authority,
        "verify_worker_release_authority",
        lambda **_kwargs: None,
    )
    monkeypatch.setattr(
        authority,
        "check_publish_provenance",
        lambda deployment, dsn: {"allowed": True, "mode": "sync_root"},
    )
    monkeypatch.setattr(
        code_identity, "record_worker_code_identity", lambda **_kwargs: None,
    )
    monkeypatch.setattr(
        deployments,
        "resolve_deployment",
        lambda name: SimpleNamespace(sync_root=None),
    )
    monkeypatch.setattr(
        sync_root, "require_resolved_sync_root", lambda *_a, **_k: None,
    )
    monkeypatch.setattr(
        sync_root,
        "require_usable_source_configuration_authority",
        lambda *_a, **_k: None,
    )
    monkeypatch.setattr(
        worker, "_worker_code_watch_roots", lambda _deployment: [],
    )
    monkeypatch.setattr(
        worker, "register_substrate_schedules", _called("schedules"),
    )

    def _refuse(**_kwargs):
        raise EnricherSupportPolicyInvalid(
            "enricher 'fixture' writes nodes but declares no valid "
            "derived-node support_policy",
        )

    monkeypatch.setattr(worker, "_preflight_enricher_contracts", _refuse)

    result = worker.run_worker.__wrapped__(
        deployment="fixture",
        dsn="postgresql://not-consulted",
        runtime=RuntimeConfig(enabled=True),
        _embedding_plan=object(),
    )
    assert result == 2
    assert calls == []
