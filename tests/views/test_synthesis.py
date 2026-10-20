"""Unit tests for okg.views.synthesis — Stage 1.6.

Covers the framework spec's "Quote fragments never invoke an
LLMClient" requirement and synthesize's behavior on
GeneratedSpec — including the "no silent fallbacks" rule.
"""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from okg.views.execution.errors import LLMCallFailed
from okg.views.execution.llm import DeterministicStubLLM
from okg.views.planners.base import GeneratedSpec, QuoteSpec
from okg.views.restricted_corpus import HermeticityViolation, RestrictedCorpus
from okg.views.synthesis import synthesize
from okg.views.types import Anchor


@pytest.fixture
def corpus():
    return {
        "a:catalog": Anchor.from_content("a:catalog", "status: current"),
        "a:body": Anchor.from_content("a:body", "purpose: improvise"),
        "b:catalog": Anchor.from_content("b:catalog", "status: rescinded"),
    }


# ---------------------------------------------------------------------------
# QuoteSpec
# ---------------------------------------------------------------------------


class TestQuoteSpec:
    def test_quote_copies_content_byte_literal(self, corpus):
        spec = QuoteSpec(id="row:a", anchor_id="a:catalog")
        rc = RestrictedCorpus(corpus, allowed_ids=("a:catalog",))
        # Use a recording mock as the llm_client; it MUST NOT be
        # called for a quote.
        recording_client = MagicMock()
        f = synthesize(spec, rc, recording_client)
        assert f.kind == "quote"
        assert f.content == "status: current"
        assert f.sources == ("a:catalog",)
        assert f.source_hashes == {"a:catalog": corpus["a:catalog"].content_hash}

    def test_quote_does_not_invoke_any_method_on_llm_client(self, corpus):
        spec = QuoteSpec(id="row:a", anchor_id="a:catalog")
        rc = RestrictedCorpus(corpus, allowed_ids=("a:catalog",))
        recording_client = MagicMock()
        synthesize(spec, rc, recording_client)
        # No method should have been touched. MagicMock.method_calls
        # is empty unless something was invoked.
        assert recording_client.method_calls == []
        assert not recording_client.complete.called

    def test_quote_tolerates_none_llm_client(self, corpus):
        # Quote synthesis should not require an LLMClient — it's
        # logically free of LLM dispatch. None is acceptable.
        spec = QuoteSpec(id="row:a", anchor_id="a:catalog")
        rc = RestrictedCorpus(corpus, allowed_ids=("a:catalog",))
        f = synthesize(spec, rc, llm_client=None)
        assert f.content == "status: current"

    def test_quote_undeclared_anchor_raises_hermeticity(self, corpus):
        # The planner declared spec.anchor_id but constructed the
        # RestrictedCorpus with a DIFFERENT allow-list. Catches a
        # planner-bug class where the spec and the wrapper drift.
        spec = QuoteSpec(id="row:a", anchor_id="a:catalog")
        rc = RestrictedCorpus(corpus, allowed_ids=("b:catalog",))
        with pytest.raises(HermeticityViolation):
            synthesize(spec, rc, llm_client=None)

    def test_quote_display_label_propagates(self, corpus):
        spec = QuoteSpec(
            id="row:a", anchor_id="a:catalog", display_label="Pub A row"
        )
        rc = RestrictedCorpus(corpus, allowed_ids=("a:catalog",))
        f = synthesize(spec, rc, None)
        assert f.display_label == "Pub A row"


# ---------------------------------------------------------------------------
# GeneratedSpec
# ---------------------------------------------------------------------------


class TestGeneratedSpec:
    def test_generated_dispatches_via_llm_client_complete(self, corpus):
        spec = GeneratedSpec(
            id="scope",
            sources=("a:catalog", "b:catalog"),
            system_prompt="You are a careful summarizer.",
            task_prompt="Summarize the scope:",
        )
        rc = RestrictedCorpus(
            corpus, allowed_ids=("a:catalog", "b:catalog"),
        )
        client = MagicMock()
        client.complete.return_value = "scope summary text"
        f = synthesize(spec, rc, client)

        assert f.kind == "generated"
        assert f.content == "scope summary text"
        # source_hashes captured at call time for every declared source.
        assert f.source_hashes == {
            "a:catalog": corpus["a:catalog"].content_hash,
            "b:catalog": corpus["b:catalog"].content_hash,
        }
        assert client.complete.call_count == 1
        # The complete call passed system, user, and fragment_id="scope".
        kwargs = client.complete.call_args.kwargs
        assert "system" in kwargs and "user" in kwargs
        assert kwargs["system"] == "You are a careful summarizer."
        assert kwargs["fragment_id"] == "scope"
        # The user prompt should embed the source labels.
        assert "[src:a:catalog]" in kwargs["user"]
        assert "[src:b:catalog]" in kwargs["user"]

    def test_generated_with_none_client_raises_before_anchor_read(self, corpus):
        # The "no silent fallbacks" rule: missing client must raise
        # BEFORE any anchor is read or any prompt is built. Use a
        # corpus that would error on anchor read (empty inner dict)
        # to detect order-of-operations.
        spec = GeneratedSpec(
            id="scope",
            sources=("a:catalog",),
            system_prompt="...",
            task_prompt="...",
        )
        empty_inner: dict[str, Anchor] = {}
        rc = RestrictedCorpus(empty_inner, allowed_ids=("a:catalog",))
        with pytest.raises(ValueError) as excinfo:
            synthesize(spec, rc, llm_client=None)
        # Error message names the offending spec id.
        assert "scope" in str(excinfo.value)

    def test_generated_with_stub_is_byte_deterministic(self, corpus):
        # DeterministicStubLLM is byte-stable for tests — same
        # inputs, same output. (NOT a substantive claim about
        # production fresh regen; this is testing the framework's
        # plumbing.)
        spec = GeneratedSpec(
            id="scope",
            sources=("a:catalog",),
            system_prompt="sys",
            task_prompt="task",
        )
        rc = RestrictedCorpus(corpus, allowed_ids=("a:catalog",))
        stub = DeterministicStubLLM()
        f1 = synthesize(spec, rc, stub)
        f2 = synthesize(spec, rc, stub)
        assert f1.content == f2.content
        assert f1.source_hashes == f2.source_hashes

    def test_generated_undeclared_read_raises_hermeticity(self, corpus):
        # The planner declared sources=(a:catalog,) but the
        # RestrictedCorpus's allow-list was wider. We then invoke
        # synthesize with the wider RC; synthesize itself only
        # reads spec.sources, so it should be fine. But if the spec
        # secretly listed extra sources beyond what the wrapper
        # allows, the read fails.
        spec = GeneratedSpec(
            id="scope",
            sources=("a:catalog", "out_of_scope"),  # second is not allowed
            system_prompt="sys",
            task_prompt="task",
        )
        rc = RestrictedCorpus(corpus, allowed_ids=("a:catalog",))
        stub = DeterministicStubLLM()
        with pytest.raises(HermeticityViolation):
            synthesize(spec, rc, stub)

    def test_generated_llm_failure_propagates(self, corpus):
        spec = GeneratedSpec(
            id="scope",
            sources=("a:catalog",),
            system_prompt="sys",
            task_prompt="task",
        )
        rc = RestrictedCorpus(corpus, allowed_ids=("a:catalog",))
        client = MagicMock()
        client.complete.side_effect = LLMCallFailed("boom")
        with pytest.raises(LLMCallFailed):
            synthesize(spec, rc, client)


# ---------------------------------------------------------------------------
# Unsupported spec types
# ---------------------------------------------------------------------------


class TestUnsupportedSpec:
    def test_unknown_spec_type_raises_typeerror(self, corpus):
        class Unknown:
            id = "x"
            sources = ()

        rc = RestrictedCorpus(corpus, allowed_ids=())
        with pytest.raises(TypeError, match="Unknown"):
            synthesize(Unknown(), rc, None)  # type: ignore[arg-type]
