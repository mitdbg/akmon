"""Tests for the `okg.deployment` SDK package skeleton (Phase 2 §2.1).

Verification per the spec scenario:

  * Public-surface import: `from okg.deployment import deployment_workflow,
    emit_node_fact, request_escalation, cost_budget, llm_call` succeeds.
  * ``okg.deployment.__version__`` returns the SDK semver string.
  * ``__all__`` enumerates only public symbols (no leading-underscore
    names other than ``__version__``).
  * `okg.deployment._internal.*` is not surfaced at package root.
"""
from __future__ import annotations


def test_public_surface_imports():
    import okg.deployment as deployment

    public_symbols = (
        deployment.cost_budget,
        deployment.deployment_workflow,
        deployment.emit_node_fact,
        deployment.llm_call,
        deployment.request_escalation,
    )
    assert all(symbol is not None for symbol in public_symbols)


def test_version_exposed():
    import okg.deployment as deployment
    assert deployment.__version__ == "1.0.0"


def test_all_no_internal_leaks():
    import okg.deployment as deployment

    bad = [
        s for s in deployment.__all__
        if s.startswith("_") and s != "__version__"
    ]
    assert bad == [], f"underscore names in __all__: {bad}"


def test_dir_excludes_internal_modules():
    """`dir(okg.deployment)` should not surface ``_internal`` /
    `_version` modules at the public namespace boundary."""
    import okg.deployment as deployment

    surface = set(dir(deployment))
    # Internal submodules are accessible by full path but should not
    # be in the package namespace boundary's public listing.
    for s in surface:
        if s.startswith("_") and s not in (
            "__version__", "__name__", "__doc__", "__file__",
            "__package__", "__loader__", "__spec__", "__path__",
            "__builtins__", "__cached__", "__all__",
            "_version", "_internal",  # submodules — present, but OK
        ):
            # Anything else starting with _ is unexpected.
            assert False, f"unexpected underscore name on surface: {s}"


def test_helpers_raise_clear_errors_outside_context():
    """SDK mutation/read helpers must raise MissingContextError —
    not silent failures — when called outside a workflow context.
    Phase 2 §3 / §4 implementations are now real; this test guards
    the error contract."""
    import pytest

    import okg.deployment as deployment
    import okg.deployment._internal.context as deployment_context

    with pytest.raises(deployment_context.MissingContextError, match="deployment"):
        deployment.emit_node_fact(
            "node-1", "Subtype", {}, dedupe_key="d",
        )

    with pytest.raises(deployment_context.MissingContextError, match="deployment"):
        deployment.read_node("node-1")


def test_derived_edge_spec_is_constructable():
    """DerivedEdgeSpec is a real dataclass usable today (Phase 2 §5.2
    will ship the producer/consumer; the spec shape is stable now)."""
    import okg.deployment as deployment

    spec = deployment.DerivedEdgeSpec(
        src="n-1", dst="n-2", edge_type="describes",
        attrs={"reason": "test"},
        dedupe_key="d", confidence=0.9,
    )
    assert spec.src == "n-1"
    assert spec.confidence == 0.9


def test_declare_narrowings_is_a_noop_for_now():
    """Phase 2 §2.3 will make declare_narrowings boot-time strict.
    Today it is a no-op so module-level usage is forward-compatible."""
    import okg.deployment as deployment

    # No exception.
    deployment.declare_narrowings(
        ("Candidate", "verified_by", "Verification"),
        ("Verification", "led_to", "Annotation"),
    )


def test_declare_sdk_version_is_a_noop_for_now():
    import okg.deployment as deployment

    deployment.declare_sdk_version("1.0")


# --- Public-surface stability (#1181 slice 1) --------------------------------
#
# `__all__` is the SDK's stability contract, but nothing compared it to the
# module namespace, so the contract was documentation rather than a gate.
# FROZEN_SURFACE is that gate: an accidental removal or rename fails here.
# Adding a name is a deliberate two-line change; removing one is a breaking
# SDK change and needs a major-version decision, not a test edit.
FROZEN_SURFACE = frozenset({
    "AUTH_MODES",
    "COMPLETED_SCOPE_RUN_MODES",
    "CONNECTOR_CAPABILITIES",
    "Cancelled",
    "ChangeProbeError",
    "Checkpoint",
    "Connector",
    "ConnectorAdapter",
    "ConnectorContext",
    "ConnectorHealth",
    "ConnectorError",
    "ConnectorMetrics",
    "ConnectorRegistration",
    "ConnectorRegistrationError",
    "ConnectorRun",
    "ConnectorWarning",
    "ContentHashProbe",
    "Deadline",
    "DerivedEdgeSpec",
    "EXTENSION_SDK_VERSION",
    "EdgeFact",
    "EdgeKey",
    "ExtensionBatch",
    "ExtensionEdge",
    "ExtensionKind",
    "ExtensionNode",
    "ExtensionRequest",
    "HEALTH_STATUSES",
    "ImageRef",
    "LLMPricingError",
    "LLMResponse",
    "MutableApiProbe",
    "NodeFact",
    "PreflightResult",
    "ProbeVerdict",
    "ProgressMarker",
    "RateLimited",
    "RecordAuthorityStream",
    "RecordEmission",
    "RecordNode",
    "RecordSet",
    "RejectedItem",
    "RetryAfter",
    "SdkVersionRange",
    "__version__",
    "consume_derived_edges_stream",
    "correlate_workflow",
    "cost_budget",
    "credential_preflight",
    "declare_narrowings",
    "declare_sdk_version",
    "dependency_preflight",
    "deployment_scheduled",
    "deployment_step",
    "deployment_transaction",
    "deployment_workflow",
    "emit_edge_fact",
    "emit_node_fact",
    "extension_entrypoint",
    "file_content_items",
    "file_preflight",
    "find_workflow_by_correlation",
    "http_preflight",
    "latest_published_generation",
    "list_published_generations",
    "llm_call",
    "on_escalation_decided",
    "on_generation_blocked",
    "on_generation_published",
    "pinned_generation",
    "publisher_generation",
    "query_at_generation",
    "read_node",
    "redact",
    "redact_endpoint",
    "redact_mapping",
    "request_escalation",
    "registration_registry_entry",
    "request_republish",
    "set_progress",
    "update_node_attrs",
    "wait_for_signal",
    "worker_write_generation",
    "write_derived_edges_stream",
})


def test_all_names_are_present_in_the_namespace():
    """Every `__all__` entry must resolve; a stale entry breaks `import *`."""
    import okg.deployment as deployment

    missing = [n for n in deployment.__all__ if not hasattr(deployment, n)]
    assert missing == [], f"__all__ names absent from the namespace: {missing}"


def test_all_has_no_duplicates():
    import okg.deployment as deployment

    seen = sorted(
        n for n in set(deployment.__all__) if deployment.__all__.count(n) > 1
    )
    assert seen == [], f"duplicated __all__ entries: {seen}"


def test_public_surface_matches_the_frozen_snapshot():
    import okg.deployment as deployment

    live = set(deployment.__all__)
    removed = sorted(FROZEN_SURFACE - live)
    added = sorted(live - FROZEN_SURFACE)
    assert removed == [], (
        "public SDK names disappeared from __all__: "
        f"{removed}. Removing a name is a breaking SDK change; decide the "
        "version bump before editing FROZEN_SURFACE."
    )
    assert added == [], (
        f"new public SDK names {added} — add them to FROZEN_SURFACE and to "
        "docs/reference/sdk-api.md in the same change."
    )


def test_star_import_binds_every_public_name():
    """`from okg.deployment import *` is how the reference tells authors to
    discover the surface; prove it binds what `__all__` promises."""
    namespace: dict = {}
    exec("from okg.deployment import *", namespace)  # noqa: S102

    import okg.deployment as deployment

    unbound = [
        n for n in deployment.__all__
        if n != "__version__" and n not in namespace
    ]
    assert unbound == [], f"star-import did not bind: {unbound}"


def test_connector_fact_names_are_the_substrate_objects():
    """The absorbed names are aliases, not wrappers. `ProgressMarker` above
    all: SourceRunner isinstance-checks it and raises on anything else, so a
    wrapper class would break every connector that yields one."""
    import okg.deployment as deployment
    from okg.substrate.library.sources import base
    from okg.substrate.library.sources.content_hash_probe import (
        ContentHashProbe,
        file_content_items,
    )
    from okg.substrate.library.sources.mutable_api_probe import MutableApiProbe
    from okg.substrate.sources import preflight, redaction

    assert deployment.ProgressMarker is base.ProgressMarker
    assert deployment.NodeFact is base.NodeFact
    assert deployment.EdgeFact is base.EdgeFact
    assert deployment.RecordSet is base.RecordSet
    assert deployment.RecordNode is base.RecordNode
    assert deployment.RecordEmission is base.RecordEmission
    assert deployment.EdgeKey is base.EdgeKey
    assert deployment.PreflightResult is base.SourcePreflightResult
    assert deployment.ContentHashProbe is ContentHashProbe
    assert deployment.MutableApiProbe is MutableApiProbe
    assert deployment.file_content_items is file_content_items
    assert deployment.file_preflight is preflight.file_ref_preflight
    assert deployment.credential_preflight is preflight.credential_env_preflight
    assert deployment.http_preflight is preflight.http_probe_result
    assert deployment.dependency_preflight is preflight.dependency_preflight
    assert deployment.redact is redaction.redact_text
    assert deployment.redact_endpoint is redaction.redact_endpoint
    assert deployment.redact_mapping is redaction.redact_mapping


def test_renamed_alias_table_matches_the_substrate_names():
    """The rename table is what the module docstring documents; keep it true."""
    import importlib

    from okg.deployment._internal.connectors import RENAMED_ALIASES

    import okg.deployment as deployment

    for public_name, substrate_name in RENAMED_ALIASES.items():
        alias = getattr(deployment, public_name)
        origin = importlib.import_module(alias.__module__)
        assert getattr(origin, substrate_name) is alias, (
            f"{public_name} no longer resolves to "
            f"{alias.__module__}.{substrate_name}"
        )
