"""Connector fact, identity, preflight and redaction surface (#1181 slice 1).

Every name here is an **alias**: it is the substrate object itself, re-exported
under the public import path. ``__module__`` stays inside ``okg.substrate``,
so the boundary this slice draws is import-path-level, not type-level. That is
deliberate for two of them:

* ``ProgressMarker`` is re-exported as-is because ``SourceRunner``
  ``isinstance``-checks it and raises on anything else; a wrapper class would
  break every connector that yields one.
* ``RecordSet``/``RecordNode``/``RecordEmission``/``EdgeKey`` carry deletion
  authority and are consumed by the runner's retraction diff, so they must be
  the same types the runner loads from the prior cursor.

``ProbeVerdict`` and ``ChangeProbeError`` are here for the same reason: the
runner hands a connector a ``ProbeVerdict`` through ``change_probe_verdict``,
so a connector that reads it would otherwise need an ``okg.substrate`` import
to name its type. Slice 1 exported neither.

``ConnectorHealth``, ``HEALTH_STATUSES`` and ``RecordAuthorityStream`` are
here for the third time the same gap appeared: ``ConnectorRun.health`` and
``ConnectorRun.record_authority_stream`` are *typed* by the first and third,
so a connector could name the fields but not construct a value for either
without the ``okg.substrate`` import this surface exists to remove. Without
them an external connector cannot report degraded health or hand back indexed
record authority at all. ``HEALTH_STATUSES`` is the accepted set, ``partial``
included, so a connector can check a status rather than guess one.

``COMPLETED_SCOPE_RUN_MODES`` is the set of run modes whose run covers a
source's whole scope, for every source profile. The runner hands a connector
its profile's own whole-scope mode (``release_new`` for a
``reference_catalog`` source, ``reconcile`` for ``mutable_api``), not the
generic ``scope_complete`` request, so a connector that claims
``completed_scope`` must test ``ctx.mode`` against this set rather than a
hard-coded list. It is the substrate set itself, so the two cannot drift.

Some aliases are renamed for the public surface; the substrate name each one
resolves to is given below. Renaming here does not rename the substrate
symbol, which stays guarded by
``tests/substrate/contracts/test_external_import_surface.py``.
"""
from __future__ import annotations

from okg.substrate.library.sources.base import (
    SOURCE_HEALTH_STATUSES as HEALTH_STATUSES,
)
from okg.substrate.library.sources.base import (
    EdgeFact,
    EdgeKey,
    NodeFact,
    ProgressMarker,
    RecordAuthorityStream,
    RecordEmission,
    RecordNode,
    RecordSet,
)
from okg.substrate.library.sources.base import (
    SourceHealth as ConnectorHealth,
)
from okg.substrate.library.sources.base import (
    SourcePreflightResult as PreflightResult,
)
from okg.substrate.library.sources.change_probe import (
    ChangeProbeError,
    ProbeVerdict,
)
from okg.substrate.library.sources.content_hash_probe import (
    ContentHashProbe,
    file_content_items,
)
from okg.substrate.library.sources.mutable_api_probe import MutableApiProbe
from okg.substrate.source_run_modes import COMPLETED_SCOPE_RUN_MODES
from okg.substrate.sources.preflight import dependency_preflight
from okg.substrate.sources.preflight import (
    credential_env_preflight as credential_preflight,
)
from okg.substrate.sources.preflight import file_ref_preflight as file_preflight
from okg.substrate.sources.preflight import http_probe_result as http_preflight
from okg.substrate.sources.redaction import redact_endpoint, redact_mapping
from okg.substrate.sources.redaction import redact_text as redact


#: Public name -> substrate name, for the renamed aliases only. The stability
#: test reads this so a substrate rename cannot silently drift the SDK.
#:
#: ``HEALTH_STATUSES`` is renamed too but cannot be listed here: the test
#: resolves each entry through ``alias.__module__``, and a frozenset reports
#: ``builtins``. Its identity with ``SOURCE_HEALTH_STATUSES`` is pinned by
#: ``test_health_and_authority_names_are_the_substrate_objects`` instead.
RENAMED_ALIASES = {
    "ConnectorHealth": "SourceHealth",
    "PreflightResult": "SourcePreflightResult",
    "credential_preflight": "credential_env_preflight",
    "file_preflight": "file_ref_preflight",
    "http_preflight": "http_probe_result",
    "redact": "redact_text",
}


__all__ = [
    "COMPLETED_SCOPE_RUN_MODES",
    "HEALTH_STATUSES",
    "ChangeProbeError",
    "ConnectorHealth",
    "ContentHashProbe",
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
    "RENAMED_ALIASES",
    "credential_preflight",
    "dependency_preflight",
    "file_content_items",
    "file_preflight",
    "http_preflight",
    "redact",
    "redact_endpoint",
    "redact_mapping",
]
