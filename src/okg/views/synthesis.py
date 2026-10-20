"""synthesize() — produce one Fragment from one FragmentSpec.

The synthesizer dispatches on spec kind:

* :class:`QuoteSpec` — copy the declared anchor's content
  byte-literal. **No method on llm_client is invoked under any
  circumstances.** This is the umbrella's "Quote fragments never
  invoke an LLMClient" invariant; we honor it by keying on
  ``isinstance`` here.

* :class:`GeneratedSpec` — build a prompt from
  ``system_prompt`` + ``task_prompt`` (after rendering declared
  sources into the latter), dispatch via
  ``llm_client.complete``. Capture all source content hashes at
  call time.

The caller is responsible for wrapping the corpus in a
:class:`RestrictedCorpus` scoped to ``spec.sources`` before
calling synthesize. This module assumes the wrapping has already
happened — defense in depth: synthesize asserts it via the
wrapper's __getitem__ raising ``HermeticityViolation`` if the
planner's prompt-build secretly reads an undeclared anchor.

The umbrella's "no silent fallbacks" rule: if a GeneratedSpec
arrives without an ``llm_client``, we raise immediately with a
message that names the spec id. No env-var fallback, no implicit
selection.
"""
from __future__ import annotations

from okg.views.execution.llm import LLMClient
from okg.views.planners.base import (
    FragmentSpec,
    GeneratedSpec,
    QuoteSpec,
)
from okg.views.restricted_corpus import RestrictedCorpus
from okg.views.types import Fragment, prompt_hash


# ---------------------------------------------------------------------------
# Prompt rendering
# ---------------------------------------------------------------------------


def _render_sources_block(
    spec_sources: tuple[str, ...],
    corpus: RestrictedCorpus,
) -> str:
    """Render the declared anchors as a labeled block for the LLM.

    Format::

        [src:<anchor_id>]
        <anchor.content>

        [src:<next_anchor_id>]
        ...

    Each anchor read goes through ``corpus[anchor_id]``, which
    means a planner that declared a source it never actually uses
    is fine, but a planner that reads outside ``spec.sources``
    raises ``HermeticityViolation``.

    Source order matches ``spec.sources`` order (significant per
    the framework spec).
    """
    parts: list[str] = []
    for aid in spec_sources:
        anchor = corpus[aid]
        parts.append(f"[src:{aid}]\n{anchor.content}")
    return "\n\n".join(parts)


# ---------------------------------------------------------------------------
# synthesize
# ---------------------------------------------------------------------------


def synthesize(
    spec: FragmentSpec,
    corpus: RestrictedCorpus,
    llm_client: LLMClient | None,
    *,
    caller: str = "",
    generation_id: int | str | None = None,
) -> Fragment:
    """Produce one :class:`Fragment` from one ``FragmentSpec``.

    Parameters
    ----------
    spec :
        The planner-produced spec. ``QuoteSpec`` or
        ``GeneratedSpec``.
    corpus :
        The hermetic wrapper, scoped to ``spec.sources``. Caller
        constructs this; synthesize does not double-wrap.
    llm_client :
        Required when ``spec`` is a ``GeneratedSpec``. May be
        ``None`` for ``QuoteSpec`` — but a single mixed-batch
        caller (e.g. the refresh loop) typically passes the same
        client for every spec, since stale ``llm_client=None`` for
        a generated spec raises immediately and loudly.

    Returns
    -------
    Fragment
        With ``id``, ``kind``, ``sources``, ``source_hashes``, and
        ``content`` populated. Quote fragments record only their
        single source's hash; generated fragments record every
        declared source's hash at call time so refresh can detect
        any of them changing.
    """
    if isinstance(spec, QuoteSpec):
        return _synthesize_quote(spec, corpus)
    if isinstance(spec, GeneratedSpec):
        return _synthesize_generated(
            spec,
            corpus,
            llm_client,
            caller=caller,
            generation_id=generation_id,
        )
    raise TypeError(
        f"synthesize: unsupported spec type {type(spec).__name__!r}"
    )


def _synthesize_quote(
    spec: QuoteSpec,
    corpus: RestrictedCorpus,
) -> Fragment:
    anchor = corpus[spec.anchor_id]
    return Fragment(
        id=spec.id,
        kind="quote",
        sources=(spec.anchor_id,),
        source_hashes={spec.anchor_id: anchor.content_hash},
        content=anchor.content,
        display_label=spec.display_label,
    )


def _synthesize_generated(
    spec: GeneratedSpec,
    corpus: RestrictedCorpus,
    llm_client: LLMClient | None,
    *,
    caller: str = "",
    generation_id: int | str | None = None,
) -> Fragment:
    if llm_client is None:
        # Umbrella's "no silent fallbacks" rule. Fail BEFORE reading
        # any anchor or building any prompt, so the caller's stack
        # trace points exactly here.
        raise ValueError(
            f"synthesize: GeneratedSpec(id={spec.id!r}) requires an "
            f"llm_client, but None was passed. Pass a LiteLLMClient "
            f"or DeterministicStubLLM explicitly."
        )

    # Capture the source-anchor hashes BEFORE invoking the LLM, so
    # the recorded hashes match exactly what the LLM saw. Reading
    # the anchors through the wrapper is also what enforces
    # hermeticity — an undeclared read would raise here.
    source_hashes: dict[str, str] = {}
    for aid in spec.sources:
        source_hashes[aid] = corpus[aid].content_hash

    sources_block = _render_sources_block(spec.sources, corpus)
    user_prompt = (
        f"{spec.task_prompt}\n\n"
        f"# Source anchors\n\n"
        f"{sources_block}"
    )

    content = llm_client.complete(
        system=spec.system_prompt,
        user=user_prompt,
        fragment_id=spec.id,
        caller=caller,
        generation_id=generation_id,
    )

    return Fragment(
        id=spec.id,
        kind="generated",
        sources=spec.sources,
        source_hashes=source_hashes,
        content=content,
        display_label=spec.display_label,
        # Fold the prompt closure into the fragment's identity so a
        # prompt edit (same sources, same id) busts reuse (#693).
        prompt_hash=prompt_hash(spec.system_prompt, spec.task_prompt),
    )
