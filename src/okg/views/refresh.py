"""refresh() — the load-bearing incremental-maintenance loop.

The umbrella's Property A ("Refresh byte-reuse") is provable from
this code's control flow:

  For every spec the planner emits, if a cached fragment with the
  same id exists AND every anchor in its sources has the same
  content_hash now as the fragment recorded, the cached fragment
  is reused byte-identically (same Python object reference). Only
  when the predicate fails do we re-synthesize — and even then,
  only that one fragment.

The loop is deliberately small (~30 lines of substantive logic).
Property A must remain provable from inspection; future
optimizations (parallel synthesize, persistent caches, etc.) MUST
preserve the byte-reuse semantics on the stable branch.

Spans:

* ``view.refresh`` (top level) — attributes ``view_id``,
  ``fragments_total``, ``reused``, ``refreshed``, ``added``,
  ``dropped``, ``llm_calls``.
* ``view.refresh.fragment`` (per fragment) — attributes
  ``fragment_id``, ``reused`` (bool).

Per-fragment spans are cheap (microseconds) at the framework's
own boundary; if they prove costly under high fan-out (hundreds
of fragments) we can switch to span events under a single
top-level span.
"""
from __future__ import annotations

import logging
import inspect
from dataclasses import dataclass

from opentelemetry import trace

from okg.views.execution.llm import LLMClient
from okg.views.planners.base import (
    FragmentSpec,
    GeneratedSpec,
    ViewDefinition,
)
from okg.views.restricted_corpus import RestrictedCorpus
from okg.views.synthesis import synthesize as default_synthesize
from okg.views.types import Anchor, Fragment, View, prompt_hash

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)


# ---------------------------------------------------------------------------
# RefreshStats — what the loop reports back
# ---------------------------------------------------------------------------


@dataclass
class RefreshStats:
    """Summary of one refresh invocation's per-fragment outcomes."""

    fragments_total: int = 0
    reused: int = 0
    refreshed: int = 0
    added: int = 0
    dropped: int = 0
    llm_calls: int = 0

    def as_dict(self) -> dict[str, int]:
        return {
            "fragments_total": self.fragments_total,
            "reused": self.reused,
            "refreshed": self.refreshed,
            "added": self.added,
            "dropped": self.dropped,
            "llm_calls": self.llm_calls,
        }


# ---------------------------------------------------------------------------
# is_stable — the umbrella's Property-A predicate
# ---------------------------------------------------------------------------


def is_stable(fragment: Fragment, corpus: dict[str, Anchor]) -> bool:
    """Return True iff every anchor in ``fragment.sources`` is
    present in ``corpus`` AND has the same ``content_hash`` as
    ``fragment.source_hashes`` recorded at generation time.

    Implementation note: the predicate is implemented strictly by
    iterating ``fragment.sources`` (the order-significant tuple),
    not by iterating ``fragment.source_hashes`` (which is a dict).
    This makes the per-fragment dependency check independent of
    Python's dict iteration order.
    """
    for aid in fragment.sources:
        anchor = corpus.get(aid)
        if anchor is None:
            return False
        if anchor.content_hash != fragment.source_hashes.get(aid):
            return False
    return True


def _prompt_unchanged(fragment: Fragment, spec: FragmentSpec) -> bool:
    """Return True iff ``spec``'s prompt closure matches ``fragment``'s.

    Source-hash stability (:func:`is_stable`) is necessary but not
    sufficient to reuse a *generated* fragment: its content also depends
    on the planner's ``system_prompt`` + ``task_prompt``. A cached
    fragment whose recorded ``prompt_hash`` differs from the current
    spec's prompt was produced under a stale prompt and must NOT be
    reused (#693). ``QuoteSpec`` fragments carry no prompt, so this is a
    no-op for them.
    """
    if not isinstance(spec, GeneratedSpec):
        return True
    return fragment.prompt_hash == prompt_hash(
        spec.system_prompt, spec.task_prompt,
    )


# ---------------------------------------------------------------------------
# refresh() — the loop
# ---------------------------------------------------------------------------


def refresh(
    cached_view: View | None,
    new_corpus: dict[str, Anchor],
    view_def: ViewDefinition,
    *,
    llm_client: LLMClient | None,
    generation_id: int | str | None = None,
    synthesize=default_synthesize,
) -> tuple[View, RefreshStats]:
    """Refresh ``cached_view`` against ``new_corpus``.

    Parameters
    ----------
    cached_view :
        The prior :class:`View` (loaded from a manifest), or
        ``None`` for a fresh-from-scratch generation.
    new_corpus :
        The current corpus — a plain dict keyed by anchor id.
    view_def :
        The runtime :class:`ViewDefinition` whose planner is
        invoked to produce the spec list.
    llm_client :
        Passed through to ``synthesize`` for any
        ``GeneratedSpec``. Required if the planner emits any
        generated specs (umbrella's "no silent fallbacks" rule);
        a missing client raises before any anchor is read.
    synthesize :
        Synthesizer callable; defaults to
        ``okg.views.synthesis.synthesize``. Callers who want to
        record per-fragment latency / cost can pass a wrapper.

    Returns
    -------
    (View, RefreshStats)
        The refreshed view and the per-fragment outcome counts.
    """
    cached_by_id: dict[str, Fragment] = (
        {f.id: f for f in cached_view.fragments}
        if cached_view is not None
        else {}
    )

    new_specs: list[FragmentSpec] = view_def.planner(new_corpus, view_def)
    spec_ids = {s.id for s in new_specs}

    new_fragments: list[Fragment] = []
    stats = RefreshStats(fragments_total=len(new_specs))

    with tracer.start_as_current_span("view.refresh") as run_span:
        run_span.set_attribute("view_id", view_def.view_id)
        run_span.set_attribute("fragments_total", stats.fragments_total)

        for spec in new_specs:
            cached = cached_by_id.get(spec.id)
            with tracer.start_as_current_span(
                "view.refresh.fragment"
            ) as frag_span:
                frag_span.set_attribute("fragment_id", spec.id)
                if (
                    cached is not None
                    and is_stable(cached, new_corpus)
                    and _prompt_unchanged(cached, spec)
                ):
                    # Property A's reuse branch — byte-identical
                    # fragment, no synthesize, no LLM call.
                    new_fragments.append(cached)
                    stats.reused += 1
                    frag_span.set_attribute("reused", True)
                    continue

                # Synthesize. Wrap the corpus in RestrictedCorpus
                # scoped to spec.sources so any out-of-declaration
                # read raises HermeticityViolation. spec.sources is
                # uniform over QuoteSpec / GeneratedSpec via the
                # `sources` property defined on each.
                restricted = RestrictedCorpus(
                    new_corpus, allowed_ids=spec.sources,
                )
                synthesize_kwargs = {"caller": f"views:{view_def.view_id}"}
                if generation_id is not None:
                    try:
                        signature = inspect.signature(synthesize)
                        accepts_generation = (
                            "generation_id" in signature.parameters
                            or any(
                                parameter.kind is inspect.Parameter.VAR_KEYWORD
                                for parameter in signature.parameters.values()
                            )
                        )
                    except (TypeError, ValueError):
                        accepts_generation = False
                    if accepts_generation:
                        synthesize_kwargs["generation_id"] = generation_id
                fragment = synthesize(
                    spec, restricted, llm_client, **synthesize_kwargs,
                )
                new_fragments.append(fragment)
                frag_span.set_attribute("reused", False)
                if cached is None:
                    stats.added += 1
                else:
                    stats.refreshed += 1
                if fragment.kind == "generated":
                    stats.llm_calls += 1

        # Count fragments that were in the cache but no longer
        # appear in the spec list — they're "dropped".
        for cached_id in cached_by_id:
            if cached_id not in spec_ids:
                stats.dropped += 1

        run_span.set_attribute("reused", stats.reused)
        run_span.set_attribute("refreshed", stats.refreshed)
        run_span.set_attribute("added", stats.added)
        run_span.set_attribute("dropped", stats.dropped)
        run_span.set_attribute("llm_calls", stats.llm_calls)

    new_view = View(
        view_id=view_def.view_id,
        view_def_hash=view_def.view_def_hash,
        manifest_version=cached_view.manifest_version if cached_view else 1,
        fragments=new_fragments,
        generation_id=cached_view.generation_id if cached_view else None,
    )
    return new_view, stats


def fresh(
    new_corpus: dict[str, Anchor],
    view_def: ViewDefinition,
    *,
    llm_client: LLMClient | None,
    generation_id: int | None = None,
    synthesize=default_synthesize,
) -> tuple[View, RefreshStats]:
    """Render a brand-new view (no cached predecessor).

    Implemented as ``refresh(None, ...)`` per the framework spec —
    this is a thin convenience for callers that want symmetric
    fresh / refresh signatures without having to build an empty
    cached View by hand.
    """
    view, stats = refresh(
        cached_view=None,
        new_corpus=new_corpus,
        view_def=view_def,
        llm_client=llm_client,
        generation_id=generation_id,
        synthesize=synthesize,
    )
    if generation_id is not None:
        view.generation_id = generation_id
    return view, stats
