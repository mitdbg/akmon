"""Module-scoped smoke tests for the communications schema."""
from __future__ import annotations

from pathlib import Path

from okg.substrate import fastyaml

SCHEMA_PATH = Path(__file__).resolve().parent.parent / "schema.yaml"


def _load() -> dict:
    with SCHEMA_PATH.open() as f:
        return fastyaml.safe_load(f) or {}


def test_schema_yaml_parses() -> None:
    schema = _load()
    assert schema.get("name") == "communications"
    assert "classes" in schema


def test_four_subtypes_present() -> None:
    classes = _load().get("classes") or {}
    for name in ("Email", "EmailThread", "ChatChannel", "ChatMessage"):
        assert name in classes, f"missing subtype {name!r}"


def test_intra_narrowings_present() -> None:
    en = (_load().get("classes") or {}).get("EdgeNarrowings") or {}
    attrs = en.get("attributes") or {}
    for slot in (
        "email_thread_contains_email",
        "chat_channel_contains_chat_message",
        "chat_message_replies_to_chat_message",
    ):
        assert slot in attrs, f"missing intra narrowing {slot}"
