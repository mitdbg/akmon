"""Regression: editing a planner's prompt must bust fragment reuse.

Issue #693b. A generated fragment's content depends on the planner's
``system_prompt`` + ``task_prompt``, but the refresh loop's reuse predicate
only checked source-anchor stability. So editing a prompt while the source
anchors were unchanged served the stale completion forever: ``is_stable``
returned True and the cached fragment was reused without re-synthesizing.

The fix folds the prompt closure into the fragment's identity
(``Fragment.prompt_hash``) and compares it on reuse, so a prompt edit
re-synthesizes the fragment.
"""
from __future__ import annotations

from okg.views.execution.llm import DeterministicStubLLM
from okg.views.planners.base import GeneratedSpec, ViewDefinition
from okg.views.refresh import refresh
from okg.views.types import Anchor, prompt_hash


def _corpus() -> dict[str, Anchor]:
    return {"a1": Anchor.from_content("a1", "source body unchanged")}


def _view_def(system_prompt: str) -> ViewDefinition:
    def planner(corpus, view_def):
        del corpus, view_def
        return [
            GeneratedSpec(
                id="frag-1",
                sources=("a1",),
                system_prompt=system_prompt,
                task_prompt="summarize the source",
            )
        ]

    return ViewDefinition(
        view_id="v.prompt-key",
        planner=planner,
        assembler=lambda view: "\n".join(f.content for f in view.fragments),
        view_def_hash="hash-unchanged",
    )


def test_prompt_edit_busts_reuse_even_when_sources_unchanged() -> None:
    corpus = _corpus()
    client = DeterministicStubLLM()

    first, first_stats = refresh(
        cached_view=None,
        new_corpus=corpus,
        view_def=_view_def("SYSTEM PROMPT VERSION 1"),
        llm_client=client,
    )
    assert first_stats.added == 1
    original_content = first.fragments[0].content

    # Edit the prompt; sources and fragment id are identical.
    second, second_stats = refresh(
        cached_view=first,
        new_corpus=corpus,
        view_def=_view_def("SYSTEM PROMPT VERSION 2 (edited)"),
        llm_client=client,
    )

    # The stale fragment must NOT be reused; it re-synthesizes.
    assert second_stats.reused == 0, second_stats.as_dict()
    assert second_stats.refreshed == 1, second_stats.as_dict()
    assert second.fragments[0].content != original_content
    assert second.fragments[0].prompt_hash == prompt_hash(
        "SYSTEM PROMPT VERSION 2 (edited)", "summarize the source",
    )


def test_unchanged_prompt_and_sources_still_reuses() -> None:
    corpus = _corpus()
    client = DeterministicStubLLM()

    first, _ = refresh(
        cached_view=None,
        new_corpus=corpus,
        view_def=_view_def("STABLE PROMPT"),
        llm_client=client,
    )

    second, second_stats = refresh(
        cached_view=first,
        new_corpus=corpus,
        view_def=_view_def("STABLE PROMPT"),
        llm_client=client,
    )

    # Property A byte-reuse still holds when nothing changed.
    assert second_stats.reused == 1, second_stats.as_dict()
    assert second.fragments[0] is first.fragments[0]
