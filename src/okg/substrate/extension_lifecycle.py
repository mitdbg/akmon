"""Production bridge from admitted extension batches to the existing source lifecycle."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from .contracts.deployment_product.connections import (
    ConnectionOperation,
    execute_connection_operation,
)
from .contracts.deployment_product.models import (
    ConnectionBindingDefinition,
    GovernedToolBindingDefinition,
    ProviderConnectionDefinition,
    SourceBindingDefinition,
)
from .extension_admission import (
    ExtensionAdmissionPlan,
    ExtensionAdmissionReceipt,
    ExtensionReservationLedger,
    execute_admitted_extension,
)
from .extension_broker import ExtensionPackagePlan
from .library.sources.base import EdgeFact, NodeFact, SourceRun


@dataclass(frozen=True, slots=True)
class ExtensionInvocation:
    extension_id: str
    binding: ConnectionBindingDefinition | SourceBindingDefinition | GovernedToolBindingDefinition
    capabilities: tuple[str, ...]
    payload: Mapping[str, Any]


class BrokeredExtensionSourceAdapter:
    """SourceAdapter that accepts only exact, actively admitted broker invocations."""

    profile = "discovery_crawl"

    def __init__(
        self,
        *,
        source_name: str,
        connection: ProviderConnectionDefinition,
        package: ExtensionPackagePlan,
        admission_plan: ExtensionAdmissionPlan,
        admission_receipt: ExtensionAdmissionReceipt,
        reservation_ledger: ExtensionReservationLedger,
        invocations: Sequence[ExtensionInvocation],
    ) -> None:
        if not source_name.strip():
            raise ValueError("brokered extension source name must be non-empty")
        if not invocations:
            raise ValueError("brokered extension source requires at least one invocation")
        self.name = source_name
        self.connection = connection
        self.package = package
        self.admission_plan = admission_plan
        self.admission_receipt = admission_receipt
        self.reservation_ledger = reservation_ledger
        self.invocations = tuple(invocations)
        if self.admission_receipt.phase != "discovery":
            raise ValueError("brokered source discovery requires a discovery admission receipt")
        for item in self.invocations:
            self._authorize_binding(item, lambda: None)

    def run(self, run_id: str, *, mode: str = "cursor") -> SourceRun:
        batches = []
        for item in self.invocations:
            result = self._authorize_binding(
                item,
                lambda item=item: execute_admitted_extension(
                    self.package,
                    self.admission_plan,
                    self.admission_receipt,
                    self.reservation_ledger,
                    extension_id=item.extension_id,
                    binding_id=item.binding.binding_id,
                    capabilities=item.capabilities,
                    payload=item.payload,
                ),
            )
            batches.append(result.batch)
        nodes = [node for batch in batches for node in batch.nodes]
        edges = [edge for batch in batches for edge in batch.edges]
        facts: list[Any] = [
            NodeFact(
                node_id=node.node_id,
                subtype=node.subtype,
                attrs=dict(node.attrs),
                source_record_id={"node_id": node.node_id},
                source_revision={"run_id": run_id},
                op="I",
            )
            for node in nodes
        ]
        facts.extend(
            EdgeFact(
                src=edge.src,
                dst=edge.dst,
                edge_type=edge.edge_type,
                attrs=dict(edge.attrs),
                source_record_id={
                    "src": edge.src,
                    "dst": edge.dst,
                    "edge_type": edge.edge_type,
                },
                source_revision={"run_id": run_id},
                op="I",
            )
            for edge in edges
        )
        return SourceRun(facts=facts, completed_scope=True, run_mode=mode)

    def _authorize_binding(self, item: ExtensionInvocation, provider_call: Any) -> Any:
        return execute_connection_operation(
            self.connection,
            item.binding,
            ConnectionOperation(
                operation_id=f"extension.{item.extension_id}",
                phase=self.admission_receipt.phase,
                effect="observe",
            ),
            provider_call,
        )


__all__ = ["BrokeredExtensionSourceAdapter", "ExtensionInvocation"]
