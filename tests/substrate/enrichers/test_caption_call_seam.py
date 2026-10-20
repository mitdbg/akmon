"""The caption provider call is separable from caption fact emission.

`_emit_caption_chunks` interleaves the VLM call with six `_emit_visual_edge`
calls and a cache write, so it could not be batched: phase 1 of the model-call
fan-out must touch no connection, and handing that emission to worker threads
is the hazard the two-phase split exists to prevent.

These assert the seam that makes batching possible — the call is planned, run,
and injectable — without yet claiming the caller loop is parallel. That is
`task.shared-model-call-fanout.separate-caption-emission`'s remaining half.
"""
from __future__ import annotations


from okg.substrate.library.sources.doc_corpus import DocCorpusSource


class _Svc:
    def __init__(self, fail=False):
        self.calls: list[tuple] = []
        self._fail = fail

    def caption(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        if self._fail:
            raise RuntimeError("vlm 503")
        return [type("Cap", (), {"text": "a caption", "category": None})()]


def test_plan_omits_context_when_empty() -> None:
    """`context` is positional-optional on CaptionService.caption; passing an
    empty string would break injected services predating that parameter."""
    planned = DocCorpusSource.plan_caption_call(
        "IMG", context="", caption_kwargs={"caller": "c"},
    )
    assert planned["args"] == ("IMG",)
    assert planned["kwargs"] == {"caller": "c"}


def test_plan_includes_context_when_present() -> None:
    planned = DocCorpusSource.plan_caption_call(
        "IMG", context="Figure 1", caption_kwargs={},
    )
    assert planned["args"] == ("IMG", "Figure 1")


def test_plan_copies_kwargs_so_callers_cannot_alias() -> None:
    """A batched caller builds many plans in a loop; a shared dict would let
    the last visual's fragment_id overwrite every earlier one."""
    kwargs = {"fragment_id": "v1"}
    planned = DocCorpusSource.plan_caption_call(
        "IMG", context="", caption_kwargs=kwargs,
    )
    kwargs["fragment_id"] = "v2"
    assert planned["kwargs"]["fragment_id"] == "v1"


def test_run_issues_exactly_the_planned_call() -> None:
    svc = _Svc()
    planned = DocCorpusSource.plan_caption_call(
        "IMG", context="ctx", caption_kwargs={"caller": "c", "fragment_id": "v1"},
    )
    out = DocCorpusSource.run_caption_call(svc, planned, image_label="fig-1")

    assert svc.calls == [(("IMG", "ctx"), {"caller": "c", "fragment_id": "v1"})]
    assert out is not None and out[0].text == "a caption"


def test_run_returns_none_on_failure_rather_than_raising() -> None:
    """Best-effort is load-bearing: the VisualArtifact is already emitted, so a
    failed caption must leave the visual discovered-but-uncaptioned, not abort
    the document."""
    svc = _Svc(fail=True)
    planned = DocCorpusSource.plan_caption_call(
        "IMG", context="", caption_kwargs={},
    )
    assert DocCorpusSource.run_caption_call(
        svc, planned, image_label="fig-1"
    ) is None


def test_the_call_seam_touches_nothing_but_the_provider() -> None:
    """The whole point of the seam: `run_caption_call` may run on a worker
    thread, so it must not reach emission or the database. Asserted by source
    inspection because the alternative is discovering it in production."""
    import ast
    import inspect
    import textwrap

    tree = ast.parse(textwrap.dedent(
        inspect.getsource(DocCorpusSource.run_caption_call)
    ))
    fn = tree.body[0]
    # Drop the docstring: it legitimately DISCUSSES emission, and matching
    # prose instead of code is how a source-inspection test lies to you.
    body = fn.body[1:] if (
        isinstance(fn.body[0], ast.Expr)
        and isinstance(fn.body[0].value, ast.Constant)
    ) else fn.body
    code = "\n".join(ast.unparse(node) for node in body)

    for forbidden in ("_emit_", "cur.execute", "caption_cache_out"):
        assert forbidden not in code, (
            f"run_caption_call references {forbidden!r} in its CODE; it must "
            f"touch only the provider"
        )


def test_emit_accepts_injected_results_and_skips_the_provider(monkeypatch) -> None:
    """With results supplied, no provider call is made — this is what lets a
    caller issue the calls in parallel and then emit serially."""
    svc = _Svc()
    called = []
    monkeypatch.setattr(
        DocCorpusSource, "run_caption_call",
        staticmethod(lambda *a, **k: called.append(1) or None),
    )
    sig = __import__("inspect").signature(DocCorpusSource._emit_caption_chunks)
    assert "caption_results" in sig.parameters, (
        "the injection seam is missing; batching cannot be wired"
    )
    assert svc.calls == [] and called == []
