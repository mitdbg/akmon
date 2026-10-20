"""Exception classes used by the views execution layer.

Kept separate from implementation modules so callers can
``except LLMCallFailed`` without importing ``litellm`` /
``tenacity`` themselves.

``HermeticityViolation`` lives in ``okg.views.restricted_corpus``
because it's structurally raised by the wrapper, not by the
execution layer; it is re-exported there.
"""
from __future__ import annotations


class LLMCallFailed(Exception):
    """Raised by an :class:`LLMClient` after the retry budget is
    exhausted (or after a non-retryable error).

    The exception's ``__cause__`` is the underlying provider
    exception (``litellm.RateLimitError``, ``litellm.APIError``,
    etc.) so the caller can diagnose without unwrapping by hand.
    The exception message names the fragment id (when supplied)
    so logs point at the offending fragment immediately.
    """


class LLMResponseTruncated(LLMCallFailed):
    """Raised when the provider stopped on ``finish_reason="length"``
    and left no content body.

    A subclass of :class:`LLMCallFailed` on purpose: callers that
    already hold a watermark on ``except LLMCallFailed`` keep working
    unchanged, while callers that want to react specifically — raise
    the output cap, or turn a reasoning trace off — can catch this.

    The distinction matters because a reasoning model can spend its
    entire output budget on the reasoning trace and return a
    zero-length content body. That is indistinguishable from a
    content-filter trip unless ``finish_reason`` is read, and
    diagnosing it as a content filter sends the operator looking in
    the wrong place (issue #907).
    """
