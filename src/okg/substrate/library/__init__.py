"""Built-in library (M5b / tasks.md §6).

Namespaced resources that deployments reference by name:

  okg.invariants.<name>   → invariant YAML (one or more)
  okg.linker_routes.<name>→ linker route fragment
  okg.modules.<name>      → source adapter Python module
  okg.templates.<name>    → deployment template directory

The resolver locates each kind by name and returns either a
Path (YAML fragments, template dirs) or an import target
(adapter modules). Unknown names raise
`LibraryRefMissing` with the list of available alternatives.

Discovery: all resources live under
`src/okg/substrate/library/<kind>/`.

Per `unify-ontology-systems` (2026-04-26): the legacy
`okg.ontology.<name>` and `okg.narrowings.<name>` kinds
were removed. All ontology declaration now lives in
`library/ontologies/<name>/schema.yaml` (full LinkML) and
is loaded via the deployment manifest's `modules:` list,
not via dotted refs.
"""
from okg.substrate.library.resolver import (
    LibraryRefMissing,
    available_refs,
    resolve,
)


__all__ = [
    "LibraryRefMissing",
    "available_refs",
    "resolve",
]
