"""Harness for an explicitly installed OKG plus external fixture wheel.

This file is the authoring copy. The arm never executes it from here: it is
copied to a directory outside the checkout and run by absolute path under
`-I`, which keeps even that directory off `sys.path`. The only import roots
are the isolated interpreter's own stdlib and site-packages, so OKG can
resolve nowhere else. The `sys.path` and OKG origin recorded below are what
the caller checks that against.
"""

from __future__ import annotations

import importlib
import json
import os
import sys
import uuid
from pathlib import Path

import psycopg

import okg
from okg.deployment import ConnectorContext, NodeFact
from okg.substrate.ingest import SourceRunner
from okg.substrate.ingest.adapter_factory import (
    construct_source_adapter_from_plan,
    plan_source_registry_bindings,
)
from okg.substrate.sources.registry import admit_source_registry_document


def main() -> None:
    registry_path, result_path = map(Path, sys.argv[1:3])
    deployment = sys.argv[3]
    dsn = os.environ["LAB_TEST_DSN"]
    assert "PYTHONPATH" not in os.environ
    okg_module_path = Path(next(iter(okg.__path__))).resolve()
    assert okg_module_path.is_relative_to(Path(sys.prefix).resolve())
    assert "site-packages" in okg_module_path.parts
    admission = admit_source_registry_document(
        json.loads(registry_path.read_text()), registry_path=registry_path
    )
    assert admission.strict
    bindings = plan_source_registry_bindings(admission, dsn=dsn, deployment=deployment)
    (name,) = bindings.plans
    plan = bindings.plans[name]
    assert (
        not plan.init_params
    )  # The public fixture receives no DSN or private authority.
    entry = plan.entry
    module = importlib.import_module(entry.raw["module"])
    module_file = Path(module.__file__).resolve()
    assert module_file.is_relative_to(Path(sys.prefix).resolve())
    assert "site-packages" in module_file.parts
    # Check the installed reader's emitted identity against its admitted
    # declaration before exercising the same reader through SourceRunner.
    facts = tuple(
        module.LabUnitsConnector().run(ConnectorContext(run_id=str(uuid.uuid4()))).facts
    )
    assert len(facts) == 8
    assert tuple(entry.record_identity_fields) == ("id",)
    for fact in facts:
        expected_id = fact.node_id if isinstance(fact, NodeFact) else fact.dst
        assert fact.source_record_id == {"id": expected_id}

    def checkpoint() -> dict:
        with psycopg.connect(dsn) as conn:
            (cursor,) = conn.execute(
                "SELECT last_applied_rev FROM okg.source_watermarks WHERE source = %s",
                (entry.ownership_id,),
            ).fetchone()
        # The reader's own frontier; runner bookkeeping is not its checkpoint.
        assert cursor["frontier"] == "2026-01-01T00:00:00Z"
        return {"frontier": cursor["frontier"]}

    def run():
        constructed = construct_source_adapter_from_plan(plan)
        assert type(constructed.adapter).__name__ == entry.raw["class"]
        assert constructed.adapter.profile == entry.source_class
        result = SourceRunner(
            dsn,
            source_name=constructed.ownership_id,
            deployment_name=deployment,
            graph_branch=constructed.graph_branch,
            executable_profile=constructed.executable_profile,
            registry_name=constructed.registry_name,
            ownership_id=constructed.ownership_id,
            registry_binding_fingerprint=constructed.registry_binding_fingerprint,
            deletion_semantics=entry.deletion_semantics,
            record_authority_format=entry.record_authority_format,
        ).run(constructed.adapter, mode="cursor")
        return result, constructed

    first, _ = run()
    before = checkpoint()
    second, constructed = run()
    after = checkpoint()
    result_path.write_text(
        json.dumps(
            {
                "module_file": str(module_file),
                "okg_module_path": str(okg_module_path),
                "sys_path": list(sys.path),
                "ownership_id": constructed.ownership_id,
                "source_class": constructed.executable_profile,
                "registry_binding_fingerprint": constructed.registry_binding_fingerprint,
                "record_identity_fields": list(entry.record_identity_fields),
                "identity_facts_checked": len(facts),
                "first_nodes": first.nodes_appended,
                "first_edges": first.edges_appended,
                "second_nodes": second.nodes_appended,
                "second_edges": second.edges_appended,
                "checkpoint_before": before,
                "checkpoint_after": after,
                "caught_up": constructed.adapter.last_run_metrics.get(
                    "lab_units.caught_up"
                ),
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
