"""archi-test lab connector: a checkpointed public-SDK connector."""

from okg.deployment import (
    Checkpoint,
    ConnectorAdapter,
    ConnectorRun,
    EdgeFact,
    NodeFact,
)

NAME = "lab-hermetic.lab_units"
PROFILE = "mutable_api"
FRONTIER = "2026-01-01T00:00:00Z"

UNITS = (
    ("lab_unit:alpha", "Alpha bench", ("lab_run:alpha-1", "lab_run:alpha-2")),
    ("lab_unit:beta", "Beta bench", ("lab_run:beta-1",)),
)


class LabUnitsConnector:
    """Emits a fixed dataset once, then reports its frontier as caught up."""

    name = NAME
    profile = PROFILE

    def run(self, ctx):
        cursor = ctx.checkpoint.next_cursor or {}
        checkpoint = Checkpoint(next_cursor={"frontier": FRONTIER})
        if cursor.get("frontier") == FRONTIER:
            ctx.metrics.increment("lab_units.caught_up")
            return ConnectorRun(
                facts=(),
                next_checkpoint=checkpoint,
                run_mode=ctx.mode,
            )
        facts = []
        for unit_id, label, runs in UNITS:
            revision = {"frontier": FRONTIER}
            facts.append(
                NodeFact(
                    node_id=unit_id,
                    subtype="lab_unit",
                    attrs={"lab_unit_id": unit_id, "label": label},
                    source_record_id={"id": unit_id},
                    source_revision=revision,
                )
            )
            for run_id in runs:
                facts.append(
                    NodeFact(
                        node_id=run_id,
                        subtype="lab_run",
                        attrs={"lab_run_id": run_id, "label": run_id},
                        source_record_id={"id": run_id},
                        source_revision=revision,
                    )
                )
                facts.append(
                    EdgeFact(
                        src=unit_id,
                        dst=run_id,
                        edge_type="contains",
                        source_record_id={"id": run_id},
                        source_revision=revision,
                    )
                )
        ctx.metrics.increment("lab_units.records", len(UNITS))
        return ConnectorRun(
            facts=tuple(facts),
            next_checkpoint=checkpoint,
            run_mode=ctx.mode,
        )


class LabUnitsSource(ConnectorAdapter):
    """Explicitly installed, registry-constructible public adapter."""

    profile = PROFILE

    def __init__(self):
        super().__init__(LabUnitsConnector())
