"""Durable cost-budget primitive (Phase 2 §3.4).

The :class:`cost_budget` context manager backs the SDK's
``with cost_budget(usd=N, scope=...) as budget:`` idiom. It owns:

  * An entry-time UPSERT against ``okg.deployment_cost_caps`` to
    record the cap.
  * A per-:meth:`consume` INSERT against ``okg.deployment_cost_ledger``
    keyed on ``(scope, step_id)`` so the consume() is idempotent
    under DBOS step retry (Phase 0 C2).
  * Live aggregates (``cap``, ``consumed``, ``available``, ``exhausted``)
    computed from one database snapshot.

The ledger row's primary key is ``(scope, consumption_id)`` where
``consumption_id`` is the DBOS step_id of the consuming step. A retry
of the same step collides on the PK and the cached step output is
returned — so cumulative cost stays correct.
"""
from __future__ import annotations

import logging
import uuid
from decimal import Decimal


from .context import current_context


__all__ = ["BudgetExhaustedError", "cost_budget"]


log = logging.getLogger(__name__)


class BudgetExhaustedError(RuntimeError):
    """Raised by :meth:`cost_budget.consume` when the consumption would
    exceed the cap.

    The exception's ``available_usd`` and ``requested_usd`` attributes
    let callers downgrade (e.g. switch to a cheaper LLM) rather than
    fail outright.
    """

    def __init__(
        self,
        *,
        scope: str,
        cap_usd: float,
        consumed_usd: float,
        requested_usd: float,
    ) -> None:
        self.scope = scope
        self.cap_usd = cap_usd
        self.consumed_usd = consumed_usd
        self.available_usd = cap_usd - consumed_usd
        self.requested_usd = requested_usd
        super().__init__(
            f"cost_budget exhausted in scope {scope!r}: "
            f"requested ${requested_usd:.4f}, available "
            f"${self.available_usd:.4f} (cap ${cap_usd:.4f}, "
            f"consumed ${consumed_usd:.4f})"
        )


class cost_budget:  # noqa: N801 — public name per spec
    """Context manager for durable cost tracking.

    Usage::

        with cost_budget(usd=10.0, scope="af_pubs.verifier.2026Q2") as budget:
            for candidate in candidates:
                if budget.available < 0.05:
                    break  # not enough headroom for another LLM call
                response = llm_call(...)
                budget.consume(response.cost_usd)

    The cap is upserted on context entry: re-entering the same scope
    with a different cap **replaces** the cap (per design). To extend
    the cap at runtime, use :meth:`extend` which atomically adds to
    the current cap value.

    All state lives in Postgres (``okg.deployment_cost_caps`` +
    ``okg.deployment_cost_ledger``). Worker crash mid-consume() is
    safe: the ledger insert collides on PK if the step retries,
    returning the cached output.
    """

    def __init__(self, *, usd: float, scope: str) -> None:
        if usd < 0:
            raise ValueError(f"cost_budget usd cap must be >= 0, got {usd}")
        if not scope:
            raise ValueError("cost_budget scope is required")
        self._cap_usd: float = float(usd)
        self._scope = scope

    def __enter__(self) -> "cost_budget":
        ctx = current_context()
        self._ctx = ctx
        with ctx.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO okg.deployment_cost_caps
                  (scope, cap_usd, updated_at)
                VALUES (%s, %s, now())
                ON CONFLICT (scope)
                  DO UPDATE SET cap_usd = EXCLUDED.cap_usd,
                                updated_at = now()
                """,
                (self._scope, self._cap_usd),
            )
        log.info(
            "cost_budget enter: scope=%s cap=$%.4f",
            self._scope, self._cap_usd,
        )
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        # Nothing to roll back — the cap and ledger live in Postgres
        # and survive workflow recovery. Surface the final accounting
        # for logs / operator visibility.
        cap_usd, consumed_usd = self._live_state()
        log.info(
            "cost_budget exit: scope=%s consumed=$%.4f cap=$%.4f%s",
            self._scope, consumed_usd, cap_usd,
            f" (exception: {exc_type.__name__})" if exc_type else "",
        )

    # -------------------------------------------------------
    # Public properties
    # -------------------------------------------------------

    def _live_state(self) -> tuple[float, float]:
        """Return the authoritative cap and ledger total in one snapshot."""
        ctx = current_context()
        with ctx.conn.cursor() as cur:
            cur.execute(
                """
                SELECT caps.cap_usd,
                       COALESCE(sum(ledger.usd_consumed), 0)
                  FROM okg.deployment_cost_caps AS caps
             LEFT JOIN okg.deployment_cost_ledger AS ledger
                    ON ledger.scope = caps.scope
                 WHERE caps.scope = %s
              GROUP BY caps.cap_usd
                """,
                (self._scope,),
            )
            row = cur.fetchone()
        if row is None:
            raise RuntimeError(
                f"cost_budget scope {self._scope!r} has no authoritative cap"
            )
        return float(row[0]), float(row[1])

    @property
    def cap(self) -> float:
        """The authoritative live cap in USD."""
        return self._live_state()[0]

    @property
    def consumed(self) -> float:
        """Sum of ledger rows for this scope (live read)."""
        return self._live_state()[1]

    @property
    def available(self) -> float:
        cap, consumed = self._live_state()
        return max(cap - consumed, 0.0)

    @property
    def exhausted(self) -> bool:
        cap, consumed = self._live_state()
        return consumed >= cap

    # -------------------------------------------------------
    # Mutations
    # -------------------------------------------------------

    def consume(
        self,
        usd: float,
        *,
        consumption_id: str | None = None,
        enforce_cap: bool = True,
    ) -> float:
        """Record a consumption of `usd` against this budget.

        Idempotent under DBOS step retry: the ``(scope,
        consumption_id)`` primary key on the ledger collides on
        re-insert, the cached step output is returned, and totals stay
        correct.

        ``enforce_cap=False`` records without the cap check — the
        accounting path for callers whose admission control happens
        BEFORE the spend (reserve-before-call). See the comment at the
        check below for why settlement must never be refused.

        Parameters
        ----------
        usd : float
            Amount to consume; must be >= 0.
        consumption_id : str | None
            Explicit idempotency key. When None, the SDK derives one
            from the active DBOS step_id (preferred); if step_id is
            unavailable (test contexts), a random UUID is used.

        Returns
        -------
        float
            The new ``consumed`` total after this consume.

        Raises
        ------
        BudgetExhaustedError
            If consumed + usd would exceed the cap (and
            ``enforce_cap`` is True). The ledger row is NOT written
            in this case — the cap is the contract.
        """
        if usd < 0:
            raise ValueError(f"consume usd must be >= 0, got {usd}")

        ctx = current_context()
        if consumption_id is None:
            consumption_id = (
                ctx.step_id
                or f"local-{uuid.uuid4().hex[:12]}"
            )

        # Lock the shared cap row before checking or inserting. This
        # serializes consumers with cap changes and with one another.
        #
        # `enforce_cap=False` is the SETTLEMENT path: the money is
        # already spent, so refusing to record it cannot protect the
        # cap — it can only make the ledger lie. Substrate model-call
        # admission enforces its cap at RESERVATION time (per run);
        # enforcing a lifetime cap here as well silenced settlement
        # permanently once a scope's ledger sum ever crossed the cap,
        # while admitted calls kept spending every run.
        if enforce_cap:
            with ctx.conn.cursor() as cur:
                cur.execute(
                    "SELECT cap_usd FROM okg.deployment_cost_caps "
                    "WHERE scope = %s FOR UPDATE",
                    (self._scope,),
                )
                cap_row = cur.fetchone()
                if cap_row is None:
                    raise RuntimeError(
                        f"cost_budget scope {self._scope!r} has no "
                        "authoritative cap"
                    )
                cur.execute(
                    "SELECT 1 FROM okg.deployment_cost_ledger "
                    "WHERE scope = %s AND consumption_id = %s",
                    (self._scope, consumption_id),
                )
                if cur.fetchone() is not None:
                    return self.consumed
                cur.execute(
                    "SELECT COALESCE(sum(usd_consumed), 0) "
                    "FROM okg.deployment_cost_ledger WHERE scope = %s",
                    (self._scope,),
                )
                current_consumed = float(cur.fetchone()[0])
                live_cap = float(cap_row[0])
            if current_consumed + usd > live_cap:
                raise BudgetExhaustedError(
                    scope=self._scope,
                    cap_usd=live_cap,
                    consumed_usd=current_consumed,
                    requested_usd=usd,
                )

        with ctx.conn.cursor() as cur:
            # ON CONFLICT DO NOTHING is the idempotency boundary: a
            # retry with the same (scope, consumption_id) is a no-op
            # rather than a UniqueViolation that would abort the
            # surrounding transaction. RETURNING xmax distinguishes
            # the inserted row from the conflicted no-op (Postgres
            # docs).
            cur.execute(
                """
                INSERT INTO okg.deployment_cost_ledger
                  (scope, consumption_id, usd_consumed, workflow_id)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (scope, consumption_id) DO NOTHING
                """,
                (
                    self._scope, consumption_id,
                    Decimal(str(usd)), ctx.workflow_id or None,
                ),
            )
            if cur.rowcount == 0:
                log.debug(
                    "consume idempotent retry: scope=%s "
                    "consumption_id=%s",
                    self._scope, consumption_id,
                )

        return self.consumed

    def extend(self, extra_usd: float) -> float:
        """Atomically raise the cap.

        Returns the new cap value. Implements an UPDATE with the new
        sum so two concurrent extends() compose without loss.
        """
        if extra_usd < 0:
            raise ValueError("extend extra_usd must be >= 0")

        ctx = current_context()
        with ctx.conn.cursor() as cur:
            cur.execute(
                """
                UPDATE okg.deployment_cost_caps
                   SET cap_usd = cap_usd + %s,
                       updated_at = now()
                 WHERE scope = %s
                RETURNING cap_usd
                """,
                (Decimal(str(extra_usd)), self._scope),
            )
            row = cur.fetchone()
        if row is None:
            raise RuntimeError(
                f"cost_budget scope {self._scope!r} has no authoritative cap"
            )
        new_cap = float(row[0])
        self._cap_usd = new_cap
        log.info(
            "cost_budget extend: scope=%s new_cap=$%.4f (added $%.4f)",
            self._scope, new_cap, extra_usd,
        )
        return new_cap
