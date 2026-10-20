"""Mutation-operator contract.

Operators own site selection and the rewrite; the injector owns
localization and every gold-integrity gate (byte pre-image, identity
stability, b0 overlap, span overlap). Registration follows the substrate's
YAML ``module`` + ``class`` + ``params`` pattern — the loader lands with the
operator-library task (task.bench.operators); this module pins only the
Protocol the core engine drives.
"""

from __future__ import annotations

from typing import Iterable, Protocol, runtime_checkable

from okg.bench.injection.prf import KeyedPRF
from okg.bench.injection.snapshot import ClaimSnapshot, CorpusSnapshot
from okg.bench.injection.types import MutationPlan, Site, Span


@runtime_checkable
class MutationOperator(Protocol):
    """One mutation family (design §6).

    ``operator_id`` and ``version`` are part of every mutation id, so a
    semantic change to an operator must bump ``version``.
    """

    operator_id: str
    version: str

    def sites(self, corpus: CorpusSnapshot, claims: ClaimSnapshot) -> Iterable[Site]:
        """Enumerate candidate sites (deterministic order not required; the
        injector canonicalizes)."""
        pass

    def plan(self, site: Site, doc_text: str, span: Span, prf: KeyedPRF) -> MutationPlan:
        """Plan the rewrite for a localized site.

        ``span`` is the injector-localized span for graph-conditioned sites
        (the site's own span for independent sites); the plan may address a
        different span (e.g. an insertion point) — the injector re-checks
        the byte pre-image of whatever span the plan carries.
        """
        pass
