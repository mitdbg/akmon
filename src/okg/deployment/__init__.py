"""Public API for the `okg.deployment` SDK.

Deployment authors import everything from this module. The
``__all__`` enumeration is the complete public surface — anything
not in ``__all__`` (including ``_internal`` submodules) is
substrate-internal and excluded from the stability contract.

See `sdk-design.md` §2 for the full surface description. Phase
status:

  * §2.1 Decorators (this module + ``_internal/decorators.py``) — LANDED.
  * §2.2 Mutation helpers — stubs land in §2.1, real impls in §3.
  * §2.3 Read helpers — stubs land in §2.1, real impls in §4.
  * §2.4 External integration — stubs land in §2.1, real impls in §5.
  * §2.5 Module-level declarations — no-op stubs; strict checks in §2.3.

Connector surface (§6 slice 1 of issue #1181): the fact, identity,
preflight and redaction names are **aliases** — the substrate objects
themselves, re-exported under this import path, so ``__module__`` stays
inside ``okg.substrate`` and the boundary is import-path-level rather
than type-level. ``ProgressMarker`` in particular is re-exported as-is
because the source runner ``isinstance``-checks it. Five aliases are
renamed for the public surface: ``PreflightResult``
(``SourcePreflightResult``), ``file_preflight`` (``file_ref_preflight``),
``credential_preflight`` (``credential_env_preflight``),
``http_preflight`` (``http_probe_result``) and ``redact``
(``redact_text``). Every other name in ``__all__`` is either exported
under its substrate name or a new SDK-owned object.

Connector protocol (§6 slice 3 of issue #1181): ``Connector``,
``ConnectorContext``, ``ConnectorRun``, ``Checkpoint``, ``ConnectorAdapter``
and the five signals are **SDK-owned**, not aliases — no substrate equivalent
exists. ``ConnectorAdapter`` satisfies ``SourceAdapter`` exactly, so the
runner drives a ``Connector`` unchanged. The whole connector surface now
resolves lazily (see ``_LAZY_MODULES`` below), because importing it eagerly
cost every SDK consumer the ``jsonschema`` chain.

Connector registration (§6 slice 4 of issue #1181): ``ConnectorRegistration``
and ``registration_registry_entry`` are SDK-owned data. A registration is
declared as a package-v2 ``connector`` asset and mapped to the strict
source-registry entry the landed admission accepts; there is no entry point
and no import-based discovery. ``ConnectorRegistrationError`` and its coded
subclasses live in ``okg.substrate.errors`` so the error registry stays the
single list, and are re-exported here so a connector author catches one
without importing the substrate.

Image values (#3046): ``ImageRef`` is an alias of
``okg.substrate.images.values.ImageRef``. ``ImageRef.file(path)`` and
``ImageRef.url(url)`` return the plain JSON image values a connector puts in
a fact's attrs; okg's ingest renders file values against
``ConnectorRun.image_root``.

The boundary lint (``okg lint deployments``, Phase 2 §6.3) treats
imports of ``okg.substrate.*`` from deployment workflow modules as
violations; deployment-author code uses only this module's public
surface.
"""
from __future__ import annotations

from typing import TYPE_CHECKING

from ._version import __version__

# Decorators — Phase 2 §2.2 LANDED.
from ._internal.decorators import (
    deployment_scheduled,
    deployment_step,
    deployment_transaction,
    deployment_workflow,
    on_escalation_decided,
    on_generation_blocked,
    on_generation_published,
)

# Mutation helpers — Phase 2 §3 LANDED.
from ._internal.mutations import (
    emit_edge_fact,
    emit_node_fact,
    set_progress,
    update_node_attrs,
)
from ._internal.cost_budget import cost_budget
from ._internal.escalation import request_escalation

# Read helpers — Phase 2 §4 LANDED.
from ._internal.reads import (
    latest_published_generation,
    list_published_generations,
    pinned_generation,
    publisher_generation,
    query_at_generation,
    read_node,
    worker_write_generation,
)

# External integration — Phase 2 §5 LANDED.
from ._internal.external import (
    LLMPricingError,
    LLMResponse,
    correlate_workflow,
    find_workflow_by_correlation,
    llm_call,
    request_republish,
    wait_for_signal,
)
from ._internal.streams import (
    DerivedEdgeSpec,
    consume_derived_edges_stream,
    write_derived_edges_stream,
)
from ._internal.extensions import (
    EXTENSION_SDK_VERSION,
    ExtensionBatch,
    ExtensionEdge,
    ExtensionKind,
    ExtensionNode,
    ExtensionRequest,
    extension_entrypoint,
)

# Module-level declarations remain no-op stubs (Phase 2 §2.3); the
# strict variants land alongside the catalog narrowing wildcard.
from ._internal.stubs import (
    declare_narrowings,
    declare_sdk_version,
)


# --- Lazy connector surface (#1181 slice 3) ---------------------------------
#
# The connector names were eagerly imported in slice 1. That pulled
# `okg.substrate.sources.preflight` -> `registry` -> `jsonschema` into every
# `import okg.deployment`, costing every SDK consumer ~58 ms whether or not it
# ever touched a connector. They load on first attribute access instead; the
# resolved object is cached in this module's globals, so repeat access is a
# plain dict hit and `is`-identity with the substrate object still holds.
#
# The TYPE_CHECKING block below imports nothing at run time. It exists so that
# static analysis — type checkers, and CodeQL's `py/undefined-export`, which
# cannot see PEP 562 `__getattr__` — still finds every `__all__` name defined
# in this module. Same shape as `okg.substrate.views`.
if TYPE_CHECKING:
    from ._internal.connectors import (
        COMPLETED_SCOPE_RUN_MODES,
        HEALTH_STATUSES,
        ChangeProbeError,
        ConnectorHealth,
        ContentHashProbe,
        EdgeFact,
        EdgeKey,
        MutableApiProbe,
        NodeFact,
        PreflightResult,
        ProbeVerdict,
        ProgressMarker,
        RecordAuthorityStream,
        RecordEmission,
        RecordNode,
        RecordSet,
        credential_preflight,
        dependency_preflight,
        file_content_items,
        file_preflight,
        http_preflight,
        redact,
        redact_endpoint,
        redact_mapping,
    )
    from ._internal.images import ImageRef
    from ._internal.protocol import (
        Cancelled,
        Checkpoint,
        Connector,
        ConnectorAdapter,
        ConnectorContext,
        ConnectorError,
        ConnectorMetrics,
        ConnectorRun,
        ConnectorWarning,
        Deadline,
        RateLimited,
        RejectedItem,
        RetryAfter,
    )
    from ._internal.registration import (
        AUTH_MODES,
        CONNECTOR_CAPABILITIES,
        ConnectorRegistration,
        ConnectorRegistrationError,
        SdkVersionRange,
        registration_registry_entry,
    )

_LAZY_MODULES = {
    "._internal.connectors": (
        "COMPLETED_SCOPE_RUN_MODES",
        "ChangeProbeError",
        "ConnectorHealth",
        "ContentHashProbe",
        "HEALTH_STATUSES",
        "EdgeFact",
        "EdgeKey",
        "MutableApiProbe",
        "NodeFact",
        "PreflightResult",
        "ProbeVerdict",
        "ProgressMarker",
        "RecordAuthorityStream",
        "RecordEmission",
        "RecordNode",
        "RecordSet",
        "credential_preflight",
        "dependency_preflight",
        "file_content_items",
        "file_preflight",
        "http_preflight",
        "redact",
        "redact_endpoint",
        "redact_mapping",
    ),
    "._internal.images": (
        "ImageRef",
    ),
    "._internal.protocol": (
        "Cancelled",
        "Checkpoint",
        "Connector",
        "ConnectorAdapter",
        "ConnectorContext",
        "ConnectorError",
        "ConnectorMetrics",
        "ConnectorRun",
        "ConnectorWarning",
        "Deadline",
        "RateLimited",
        "RejectedItem",
        "RetryAfter",
    ),
    "._internal.registration": (
        "AUTH_MODES",
        "CONNECTOR_CAPABILITIES",
        "ConnectorRegistration",
        "ConnectorRegistrationError",
        "SdkVersionRange",
        "registration_registry_entry",
    ),
}

_LAZY_NAMES = {
    name: module
    for module, names in _LAZY_MODULES.items()
    for name in names
}


def __getattr__(name: str):
    """Resolve a connector-surface name on first access (PEP 562)."""
    module = _LAZY_NAMES.get(name)
    if module is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    import importlib

    value = getattr(importlib.import_module(module, __name__), name)
    globals()[name] = value  # cache: later access skips __getattr__ entirely
    return value


def __dir__() -> list[str]:
    """List the public surface plus the two submodules, nothing private."""
    return sorted(set(__all__) | {"_internal", "_version"})


__all__ = [
    # Version
    "__version__",
    # Decorators (§2.1)
    "deployment_scheduled",
    "deployment_step",
    "deployment_transaction",
    "deployment_workflow",
    "on_escalation_decided",
    "on_generation_blocked",
    "on_generation_published",
    # Mutation helpers (§2.2)
    "cost_budget",
    "emit_edge_fact",
    "emit_node_fact",
    "request_escalation",
    "set_progress",
    "update_node_attrs",
    # Read helpers (§2.3)
    "latest_published_generation",
    "list_published_generations",
    "pinned_generation",
    "publisher_generation",
    "query_at_generation",
    "read_node",
    "worker_write_generation",
    # External integration (§2.4)
    "DerivedEdgeSpec",
    "EXTENSION_SDK_VERSION",
    "ExtensionBatch",
    "ExtensionEdge",
    "ExtensionKind",
    "ExtensionNode",
    "ExtensionRequest",
    "LLMPricingError",
    "LLMResponse",
    "consume_derived_edges_stream",
    "correlate_workflow",
    "find_workflow_by_correlation",
    "extension_entrypoint",
    "llm_call",
    "request_republish",
    "wait_for_signal",
    "write_derived_edges_stream",
    # Connector facts, identity, preflight and redaction (#1181 slice 1)
    "ChangeProbeError",
    "ContentHashProbe",
    "EdgeFact",
    "EdgeKey",
    "MutableApiProbe",
    "NodeFact",
    "PreflightResult",
    "ProbeVerdict",
    "ProgressMarker",
    "RecordEmission",
    "RecordNode",
    "RecordSet",
    "credential_preflight",
    "dependency_preflight",
    "file_content_items",
    "file_preflight",
    "http_preflight",
    "redact",
    "redact_endpoint",
    "redact_mapping",
    # Image values (#3046)
    "ImageRef",
    # Connector protocol, context and signals (#1181 slice 3)
    "Cancelled",
    "Checkpoint",
    "Connector",
    "ConnectorAdapter",
    "ConnectorContext",
    "ConnectorError",
    "ConnectorMetrics",
    "ConnectorRun",
    "ConnectorWarning",
    "Deadline",
    "RateLimited",
    "RejectedItem",
    "RetryAfter",
    # Connector health and record authority (#1181 slice 3)
    "ConnectorHealth",
    "HEALTH_STATUSES",
    "RecordAuthorityStream",
    # Run modes that complete a source's scope, for any profile
    "COMPLETED_SCOPE_RUN_MODES",
    # Connector registration metadata (#1181 slice 4)
    "AUTH_MODES",
    "CONNECTOR_CAPABILITIES",
    "ConnectorRegistration",
    "ConnectorRegistrationError",
    "SdkVersionRange",
    "registration_registry_entry",
    # Module-level declarations (§2.5)
    "declare_narrowings",
    "declare_sdk_version",
]
