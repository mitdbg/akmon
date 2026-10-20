"""Code graph sources for okg-workspace coding-agent context.

These adapters intentionally start with deterministic, high-confidence
facts: Python AST symbols/imports/calls, manifest-declared dependencies,
obvious interface endpoints, and coarse data/config touchpoints. They
avoid model inference and leave unresolved targets as explicit reference
nodes.
"""
from __future__ import annotations


# code_graph.py is now a pure re-export hub. Each source class
# lives in its own sibling module; the 16 shared utility helpers
# + 3 constants live in `_code_graph_common.py`. Imports below
# re-export everything the rest of the codebase (YAML manifests,
# tests, external repo factory) reaches for under the original
# `okg.substrate.library.sources.code_graph.*` paths.

__all__ = [
    "CODE_LANGUAGE_BY_EXT",
    "CodeQLDataFlowSource",
    "CodeDataFlowSource",
    "CodeLspResolutionSource",
    "CodeStructureSource",
    "DEFAULT_CODE_EXTS",
    "DEFAULT_EXCLUDES",
    "InterfaceSource",
    "PackageManifestSource",
    "SemgrepDataFlowSource",
    "_PythonAnalyzer",
    "_Symbol",
    "_analyze_python",
    "_call_name",
    "_dedupe_facts",
    "_ensure_tree_sitter_registered",
    "_file_id",
    "_glob_match",
    "_language_for_path",
    "_literal_str",
    "_module_qname",
    "_read_text",
    "_rel",
    "_sha",
    "_symbol_id",
    "_walk_files",
]


# PY_EXTS moved to `code_graph_interface.py` by Wave D-29 (its
# only consumer was InterfaceSource). CODE_LANGUAGE_BY_EXT /
# DEFAULT_CODE_EXTS / DEFAULT_EXCLUDES moved to
# `_code_graph_common.py` by Wave D-27. The re-export marker
# below adds them back to this module's namespace so existing
# `from .code_graph import CODE_LANGUAGE_BY_EXT` style imports
# (and YAML manifests using their stable names) keep working.



# ------------------------------------------------------
# Shared utility helpers + Python AST analyzer moved to
# `_code_graph_common.py` by Wave D-27 (the first slice
# of `extract-code-graph-dataflow`). Re-export at module-
# import time so the remaining classes in this file +
# the test suite + any external callers keep finding
# them under the original `code_graph.*` paths.
# ------------------------------------------------------
from okg.substrate.library.sources._code_graph_common import (  # noqa: F401, E402
    CODE_LANGUAGE_BY_EXT,
    DEFAULT_CODE_EXTS,
    DEFAULT_EXCLUDES,
    _PythonAnalyzer,
    _Symbol,
    _analyze_python,
    _call_name,
    _dedupe_facts,
    _ensure_tree_sitter_registered,
    _file_id,
    _glob_match,
    _language_for_path,
    _literal_str,
    _module_qname,
    _read_text,
    _rel,
    _sha,
    _symbol_id,
    _walk_files,
)


# ------------------------------------------------------
# CodeStructureSource moved to `code_graph_structure.py`
# by Wave D-30 (`extract-code-graph-structure-and-manifest`).
# Re-export at module-import time so YAML manifests with
# `module: okg.substrate.library.sources.code_graph;
# class: CodeStructureSource` keep resolving.
# ------------------------------------------------------
from okg.substrate.library.sources.code_graph_structure import (  # noqa: F401, E402
    CodeStructureSource,
)

from okg.substrate.library.sources.code_graph_lsp import (  # noqa: F401, E402
    CodeLspResolutionSource,
)


# ------------------------------------------------------
# PackageManifestSource moved to `code_graph_manifest.py`
# by Wave D-30 (`extract-code-graph-structure-and-manifest`).
# Re-export at module-import time so YAML manifests with
# `module: okg.substrate.library.sources.code_graph;
# class: PackageManifestSource` keep resolving.
# ------------------------------------------------------
from okg.substrate.library.sources.code_graph_manifest import (  # noqa: F401, E402
    PackageManifestSource,
)



# ------------------------------------------------------
# InterfaceSource moved to `code_graph_interface.py`
# by Wave D-29 (`extract-code-graph-interface`).
# Re-export at module-import time so YAML manifests
# with `module: okg.substrate.library.sources.code_graph;
# class: InterfaceSource` keep resolving.
# ------------------------------------------------------
from okg.substrate.library.sources.code_graph_interface import (  # noqa: F401, E402
    InterfaceSource,
)


# ------------------------------------------------------
# CodeDataFlowSource moved to `code_graph_dataflow.py`
# by Wave D-27 (`extract-code-graph-dataflow`). Re-export
# at module-import time so YAML manifests with
# `module: okg.substrate.library.sources.code_graph;
# class: CodeDataFlowSource` keep resolving.
# ------------------------------------------------------
from okg.substrate.library.sources.code_graph_dataflow import (  # noqa: F401, E402
    CodeDataFlowSource,
)




# ------------------------------------------------------
# SARIF-based data-flow sources moved to
# `code_graph_sarif.py` by Wave D-28
# (`extract-code-graph-sarif`). Re-export at module-
# import time so YAML manifests with
# `module: okg.substrate.library.sources.code_graph;
# class: SemgrepDataFlowSource` (or CodeQLDataFlowSource)
# keep resolving.
# ------------------------------------------------------
from okg.substrate.library.sources.code_graph_sarif import (  # noqa: F401, E402
    CodeQLDataFlowSource,
    SemgrepDataFlowSource,
)
