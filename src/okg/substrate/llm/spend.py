"""Spend admission for the substrate's own model calls.

The substrate had no economic bound on model calls at all. `record_llm_call`
captures `cost_usd` AFTER the fact as telemetry, with no cap attached, so the
only thing limiting burn rate was that every call site was serial — and the
fan-out removed exactly that. Width bounds occupancy; it does nothing to bound
cost. At width 8 a runaway burns the budget eight times faster.

**One ledger, not two.** This wraps the existing `okg.deployment.cost_budget`
primitive rather than reimplementing it, so substrate spend and deployment-SDK
spend accumulate in the same `okg.deployment_cost_caps` /
`okg.deployment_cost_ledger` tables and one query answers "what did this
deployment spend". The primitive also brings idempotency the substrate would
otherwise have to re-derive: its ledger row is keyed on
``(scope, consumption_id)``, so a DBOS step retry re-inserting the same
consumption is a no-op instead of double-charging.

**Reservation, not post-hoc settlement.** `cost_budget.consume` checks the cap
before inserting and documents its own race: two concurrent consumes can both
pass the check and both insert. Serially that is a rounding error; at width N it
is structural, and the overshoot scales with the most expensive call in flight.
So phase 1 RESERVES an estimate before issuing the call and phase 2 settles the
actual against it. The overshoot bound is then the reservation, not whatever
happened to be in flight — see :meth:`ModelCallSpend.overshoot_bound_usd`.

**The estimate is the operator's, not a guess.** A per-call estimate that the
substrate invented would be a silent fallback: wrong by an unknown factor, and
invisible. The deployment declares `estimated_usd_per_call` for the lane; an
absent declaration means spend admission is OFF for that lane and says so, which
is honest, rather than defaulting to a number nobody chose.
"""
from __future__ import annotations

import logging
import threading
import uuid
from dataclasses import dataclass
from typing import Optional

__all__ = ["ModelCallSpend", "SpendAdmission"]

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class SpendAdmission:
    """The spend half of an admission decision."""

    admitted: bool
    reason: str = ""
    reserved_usd: float = 0.0
    consumption_id: str = ""


class ModelCallSpend:
    """Reserve-then-settle spend admission over the existing cost ledger.

    Construct with the deployment name, the cost_budget SCOPE the lane charges
    to, the per-call estimate, and the cap. `estimated_usd_per_call=None`
    disables spend admission for the lane — reported, never silently defaulted.
    """

    def __init__(
        self,
        *,
        deployment: str,
        scope: str,
        cap_usd: Optional[float],
        estimated_usd_per_call: Optional[float],
    ) -> None:
        self.deployment = deployment
        self.scope = scope
        self.cap_usd = cap_usd
        self.estimate = estimated_usd_per_call
        self._reserved_usd = 0.0
        self._settled_usd = 0.0
        # `reserve` is called from the fan-out's WORKER threads, so its
        # read-check-write on the running total must be atomic. Without this
        # lock, concurrent reservations lose updates and over-admit — the exact
        # both-passed-a-stale-check race this class exists to close, just moved
        # from the ledger into process memory.
        self._lock = threading.Lock()

    @property
    def enabled(self) -> bool:
        """False when the deployment declared no estimate or no cap.

        A disabled lane is UNBOUNDED, which callers must be able to see and
        report; it is not a quietly-permissive default.
        """
        return self.cap_usd is not None and self.estimate is not None

    def overshoot_bound_usd(self, width: int) -> float:
        """The maximum this lane can exceed its cap by, as a number.

        With reservation-before-call, the cap is checked against reservations
        rather than settlements, so concurrent calls cannot each slip past on a
        stale read of the ledger. What remains is per-call estimate error: a
        call that costs more than its reservation overshoots by the difference,
        and at most `width` calls are in flight. So the bound is
        ``width x (actual - estimate)`` per in-flight call, which is zero when
        the estimate is an upper bound and is why the estimate SHOULD be one.

        Reported so an operator can decide whether the estimate is tight
        enough, instead of discovering the overshoot from a bill.
        """
        if not self.enabled:
            return float("inf")
        return float(width) * float(self.estimate or 0.0)

    def reserve(self, conn) -> SpendAdmission:
        """Reserve one call's estimated cost, or refuse.

        Runs on the calling thread (phase 1 of the fan-out never touches a
        connection, so the CALLER performs reservation before dispatch).
        """
        if not self.enabled:
            return SpendAdmission(
                admitted=True,
                reason="spend admission disabled: no cap or no per-call estimate declared",
            )

        estimate = float(self.estimate or 0.0)
        with self._lock:
            if self._reserved_usd + estimate > float(self.cap_usd or 0.0):
                return SpendAdmission(
                    admitted=False,
                    reason=(
                        f"spend cap exhausted for scope {self.scope!r}: reserved "
                        f"${self._reserved_usd:.4f} of "
                        f"${float(self.cap_usd):.4f}, this call estimates "
                        f"${estimate:.4f}"
                    ),
                )
            self._reserved_usd += estimate
        return SpendAdmission(
            admitted=True,
            reserved_usd=estimate,
            consumption_id=f"{self.scope}:{uuid.uuid4().hex[:16]}",
        )

    def release(self, admission: Optional[SpendAdmission]) -> None:
        """Return a reservation whose call was never issued.

        The rate limiter can refuse AFTER a successful reserve; settling
        that reservation would write a ledger row for a call that never
        happened, and dropping it silently would burn per-run headroom on
        nothing. Release is in-memory only — there is nothing to record.
        """
        if admission is None or not self.enabled or not admission.admitted:
            return
        with self._lock:
            self._reserved_usd -= float(admission.reserved_usd)

    def settle(
        self, conn, admission: Optional[SpendAdmission],
        actual_usd: Optional[float],
    ) -> None:
        """Record the cost of an ISSUED call against the shared ledger.

        Uses the real `cost_budget` primitive so the row lands in
        `okg.deployment_cost_ledger` keyed on `(scope, consumption_id)` — the
        idempotency that makes a DBOS step retry a no-op rather than a double
        charge.

        `actual_usd` is the provider-reported cost; None means the provider
        answered but reported no usage, so the reservation (which SHOULD be an
        upper bound) is charged. A caller whose call was issued but billed
        nothing knowable passes 0.0 explicitly. A caller whose call was never
        issued must `release`, not settle.

        Settlement is ACCOUNTING, never enforcement: the money is already
        spent, so the ledger write bypasses the lifetime cap check
        (`enforce_cap=False`). Enforcing the cap here silenced settlement
        permanently once a scope's ledger sum crossed the cap while per-run
        admission kept issuing calls — the ledger under-stated real spend
        forever. The cap is enforced at RESERVATION time, per run.

        A settlement failure never propagates: the provider call has already
        been paid for, and turning a bookkeeping error into a failed enrichment
        would lose data to protect an accounting row. It IS logged loudly,
        because a lost settlement under-reports spend and the cap drifts.
        """
        if admission is None or not self.enabled or not admission.admitted:
            return
        charge = (
            float(actual_usd) if actual_usd is not None
            else float(admission.reserved_usd)
        )
        with self._lock:
            self._settled_usd += charge
        try:
            from okg.deployment._internal.context import bind_sdk_context
            from okg.deployment._internal.cost_budget import cost_budget

            # `bind_sdk_context` is the one seam for supplying the SDK context
            # (deployment + connection) outside a DBOS workflow, and the
            # alternative — a second ledger accessor — is forbidden by the
            # one-ledger requirement. No workflow_id is passed: resolution
            # falls to DBOS when a workflow is active, so the ledger row's
            # workflow_id column carries real provenance, not a placeholder.
            with bind_sdk_context(deployment=self.deployment, conn=conn):
                with cost_budget(usd=float(self.cap_usd or 0.0), scope=self.scope) as budget:
                    budget.consume(
                        charge,
                        consumption_id=admission.consumption_id,
                        enforce_cap=False,
                    )
        except Exception as exc:  # noqa: BLE001 — see docstring
            log.error(
                "model-call spend settlement FAILED for scope %s "
                "(consumption %s, $%.4f): the call was made and paid for, but "
                "the ledger does not record it, so reported spend now "
                "UNDER-states actual and the cap will drift. Underlying: %s",
                self.scope, admission.consumption_id, charge, exc,
            )

    @property
    def reserved_usd(self) -> float:
        return self._reserved_usd

    @property
    def settled_usd(self) -> float:
        return self._settled_usd
