"""Every in-repo deployment schedule is censused for the #1338 defect.

``@deployment_scheduled`` defaults to ``guard_body=True``, which wraps the
workflow body in the deployment writer permit. That permit is a
``pg_advisory_xact_lock``, so a guarded body holds an OPEN Postgres
transaction for its entire duration. PostgreSQL terminates a session left idle
in a transaction past the deployment bound, and until it does, every
connection that needs the same lock queues behind it.

``okg_workspace_self_play`` failed on every run it ever had for exactly this
reason (SQLSTATE 25P03, issue #1338), and ``okg_workspace_resource_monitor``
carried the same shape around a body of ``ssh`` probes that the SDK's own
overlap comment records averaging about 22 minutes on a five-minute cron.

Per-schedule tests already pin the two okg-workspace schedules that were fixed
first. This census is deliberately broader: it fails on a schedule nobody
thought to write a test for, which is how both live instances got shipped. A
schedule that genuinely needs the permit held across its body must be added to
``_GUARDED_BY_DESIGN`` with a stated reason, so the choice is visible in
review rather than inherited from a default.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[3]
DEPLOYMENTS_DIR = REPO_ROOT / "deployments"

#: schedule_id -> why holding the writer permit across the body is correct.
#: Empty on purpose. Adding an entry is a claim that the body is a bounded
#: database operation, not a wait on a subprocess, a network peer, or a model.
_GUARDED_BY_DESIGN: dict[str, str] = {}


def _declared_workflow_modules() -> list[tuple[str, Path]]:
    """Return (deployment name, module file) for every DECLARED workflow module.

    Driven by each `deployment.yaml`'s `runtime.workflows`, not by a glob:
    that list is what `worker.py` actually imports, so a module the manifest
    declares under some other package (`tasks.foo`) would register schedules a
    glob over `workflows/` never sees -- and refusing the next guarded
    schedule is this file's entire purpose. Undeclared files are swept too, so
    a module that registers a schedule before anyone wires it up is not
    invisible either.
    """
    import yaml

    found: dict[tuple[str, Path], None] = {}
    for manifest in sorted(DEPLOYMENTS_DIR.glob("*/deployment.yaml")):
        deployment_dir = manifest.parent
        raw = yaml.safe_load(manifest.read_text(encoding="utf-8")) or {}
        runtime = raw.get("runtime")
        declared = (runtime or {}).get("workflows") or []
        for dotted in declared:
            path = deployment_dir.joinpath(*str(dotted).split(".")).with_suffix(
                ".py"
            )
            if path.is_file():
                found[(deployment_dir.name, path)] = None
    for module_path in sorted(DEPLOYMENTS_DIR.glob("*/workflows/*.py")):
        if module_path.name == "__init__.py":
            continue
        found[(module_path.parent.parent.name, module_path)] = None
    return list(found)


def test_the_census_actually_finds_deployment_workflow_modules() -> None:
    """Guard the guard: an empty sweep would pass this file vacuously."""
    modules = _declared_workflow_modules()
    assert len(modules) >= 4, (
        f"the deployment workflow-module sweep found {len(modules)} modules "
        f"under {DEPLOYMENTS_DIR}; the census below would pass without "
        "checking anything"
    )
    assert len({name for name, _ in modules}) >= 2, (
        f"the sweep covers only {sorted({n for n, _ in modules})}; the census "
        "must span every deployment that declares workflows"
    )


@pytest.fixture(scope="module")
def registered_schedules():
    """Import every deployment workflow module under a UNIQUE module name.

    Deliberately NOT the worker's `sys.path` + `workflows.<name>` shape.
    okg-workspace's `workflows/` is a regular package and okg-ci's is a
    namespace portion, and Python's finder prefers a regular package found
    anywhere on `sys.path` -- so an okg-workspace entry left behind by any
    earlier `resolve_deployment` in the same process (`inject_deployment_path`
    inserts and never removes) shadows okg-ci even when okg-ci is at index 0.
    Importing each file by path under its own name removes that ordering
    hazard and the `sys.modules` eviction hazard together.

    The registry is restored on the way out: clearing it for the whole process
    would break any suite that populated it first, order-dependently.
    """
    import importlib.util

    from okg.deployment._internal import decorators
    from okg.deployment._internal.decorators import iter_deployment_scheduled

    saved_registry = dict(decorators._DEPLOYMENT_SCHEDULED)
    saved_guard = decorators._DEPLOYMENT_SCHEDULED_GUARD
    decorators._DEPLOYMENT_SCHEDULED.clear()
    try:
        for deployment_name, module_path in _declared_workflow_modules():
            unique = (
                f"okg_schedule_census_{deployment_name}_{module_path.stem}"
            ).replace("-", "_")
            spec = importlib.util.spec_from_file_location(unique, module_path)
            assert spec is not None and spec.loader is not None, module_path
            module = importlib.util.module_from_spec(spec)
            sys.modules[unique] = module
            try:
                spec.loader.exec_module(module)
            finally:
                sys.modules.pop(unique, None)
        yield list(iter_deployment_scheduled())
    finally:
        decorators._DEPLOYMENT_SCHEDULED.clear()
        decorators._DEPLOYMENT_SCHEDULED.update(saved_registry)
        decorators._DEPLOYMENT_SCHEDULED_GUARD = saved_guard


def test_every_deployment_schedule_is_registered(
    registered_schedules,
) -> None:
    """A registry that came back empty would make the next test vacuous."""
    ids = sorted(entry["schedule_id"] for entry in registered_schedules)
    assert len(ids) >= 4, (
        f"importing the deployment workflow modules registered {len(ids)} "
        "schedules; expected at least one per module. The guard-body census "
        "below cannot prove anything otherwise"
    )
    assert len(set(ids)) == len(ids), (
        f"duplicate schedule ids in the census: {ids}. Two deployments both "
        "define `workflows.resource_monitor`, so a stale `sys.modules` entry "
        "would make the sweep import one of them twice and check the other "
        "not at all"
    )
    # Both deployments must actually be represented, by their id prefixes.
    assert any(name.startswith("okg_ci_") for name in ids), ids
    assert any(name.startswith("okg_workspace_") for name in ids), ids


def test_no_deployment_schedule_guards_a_long_body(
    registered_schedules,
) -> None:
    guarded = sorted(
        entry["schedule_id"]
        for entry in registered_schedules
        if entry["guard_body"] and entry["schedule_id"] not in _GUARDED_BY_DESIGN
    )
    assert guarded == [], (
        f"these deployment schedules hold the writer permit across their "
        f"whole body: {guarded}. The permit is a pg_advisory_xact_lock, so "
        f"the body runs inside an open Postgres transaction and every "
        f"connection needing that lock queues behind it until PostgreSQL "
        f"terminates the session (SQLSTATE 25P03) -- the defect in issue "
        f"#1338. Pass guard_body=False and take the admission decision "
        f"explicitly at the top of the body (or enqueue a child that takes "
        f"the permit around its own enqueue), or record the schedule in "
        f"_GUARDED_BY_DESIGN with the reason its body is a bounded database "
        f"operation"
    )
