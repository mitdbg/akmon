"""An assistant turn whose reasoning is encrypted must not claim to be text.

Claude Code emits ``{"type": "thinking", "thinking": "", "signature": ...}`` when
a turn's reasoning is encrypted. The plaintext is absent and unrecoverable, so
the adapter stores no body -- but ``_message_kind`` classified the turn purely on
the absence of tool calls, labelling it ``assistant_text``. On okg-workspace that
produced 956 nodes asserting they were text messages while carrying no body and
no ``text_hash``, making them invisible to lexical search and indistinguishable
from genuine data loss (#1326).
"""

from __future__ import annotations

from okg.substrate.library.sources._agent_sessions_extract import _message_kind


def _assistant(blocks: list[dict]) -> dict:
    return {"type": "assistant", "message": {"content": blocks}}


ENCRYPTED = {"type": "thinking", "thinking": "", "signature": "CAISkAMKiAEIEBgC"}


def test_encrypted_thinking_turn_is_not_labelled_assistant_text() -> None:
    kind = _message_kind("assistant", _assistant([ENCRYPTED]), 0)
    assert kind == "assistant_redacted_thinking", kind


def test_several_encrypted_blocks_still_classify_as_redacted() -> None:
    ev = _assistant([ENCRYPTED, dict(ENCRYPTED, signature="CAISothersig")])
    assert _message_kind("assistant", ev, 0) == "assistant_redacted_thinking"


def test_an_empty_text_block_alongside_encrypted_thinking_is_tolerated() -> None:
    ev = _assistant([{"type": "text", "text": ""}, ENCRYPTED])
    assert _message_kind("assistant", ev, 0) == "assistant_redacted_thinking"


def test_readable_text_is_still_assistant_text() -> None:
    ev = _assistant([ENCRYPTED, {"type": "text", "text": "here is the answer"}])
    assert _message_kind("assistant", ev, 0) == "assistant_text"


def test_readable_thinking_is_still_assistant_text() -> None:
    ev = _assistant([{"type": "thinking", "thinking": "weighing it up", "signature": "x"}])
    assert _message_kind("assistant", ev, 0) == "assistant_text"


def test_empty_thinking_without_a_signature_is_not_the_encrypted_shape() -> None:
    """Guard against swallowing a genuine body loss.

    An empty thinking block with no signature is not the encrypted shape; it is
    an unexplained absence and must keep reporting as ``assistant_text`` so it
    stays visible rather than being reclassified into the benign bucket.
    """
    ev = _assistant([{"type": "thinking", "thinking": ""}])
    assert _message_kind("assistant", ev, 0) == "assistant_text"


def test_an_unknown_block_type_is_not_reclassified() -> None:
    ev = _assistant([ENCRYPTED, {"type": "some_future_block"}])
    assert _message_kind("assistant", ev, 0) == "assistant_text"


def test_tool_calls_still_win() -> None:
    ev = _assistant([ENCRYPTED, {"type": "tool_use", "name": "Bash", "input": {}}])
    assert _message_kind("assistant", ev, 1) == "assistant_tool_request"


def test_non_assistant_events_are_untouched() -> None:
    assert _message_kind("user", {"type": "user", "message": {"content": "hi"}}, 0) == "user_prompt"
    assert _message_kind("system", {"type": "system", "subtype": "compact_boundary"}, 0) == (
        "system:compact_boundary"
    )
