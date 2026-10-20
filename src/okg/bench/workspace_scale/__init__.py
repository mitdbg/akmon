"""Synthetic production-scale okg-workspace graph for console performance.

``python -m okg.bench.workspace_scale --dsn <url> --scale 1.0`` builds, in an
empty ``okg_workspace_*`` database, a graph shaped like the live okg-workspace
deployment (``targets.yaml``), published through the deployment's real
publisher with its Nomos policy active, so the console's production budgets
(``okg.viz.console_perf_budgets``) can be measured against it with
``OKG_CONSOLE_PERF_DSN``.
"""
from okg.bench.workspace_scale.build import (
    BuildOptions,
    BuildRefused,
    census,
)
from okg.bench.workspace_scale.targets import load_targets, scaled_plan

# `build` the function is not re-exported here: the name would shadow the
# `okg.bench.workspace_scale.build` module. Import it from that module.
__all__ = [
    "BuildOptions",
    "BuildRefused",
    "census",
    "load_targets",
    "scaled_plan",
]
