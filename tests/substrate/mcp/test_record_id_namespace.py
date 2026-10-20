"""Record-id namespace restriction (harden-mcp-write-boundaries).

A caller-supplied node_id is only honored when it falls within the active
record-prefix namespace; a foreign prefix (e.g. doc:) is ignored in favor of a
server-minted id, so an agent cannot write to / collide with curated node ids.
"""
from __future__ import annotations

import pytest

from okg.substrate.mcp.record import (
    _TEMPLATES,
    _normalize_prefixes,
    _record_id,
)

_PREFIXES = _normalize_prefixes(None)  # substrate builtin record prefixes
_TMPL = _TEMPLATES["observation"]
_DOMAIN_TMPL = _TEMPLATES["domain_entity"]
_SHA = "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6"


def test_foreign_prefix_node_id_is_regenerated():
    # A curated graph node id (publication:/person:/...) is NOT an agent
    # record-prefix; an agent must not be able to write to it.
    rid = _record_id(
        "observation", {"node_id": "publication:curated-paper"}, _SHA, _TMPL,
        _PREFIXES,
    )
    assert rid != "publication:curated-paper"
    assert rid.startswith("observation:record:")


def test_no_active_prefixes_does_not_help_attacker_when_namespace_set():
    rid = _record_id(
        "observation", {"node_id": "person:riley"}, _SHA, _TMPL, _PREFIXES,
    )
    assert rid != "person:riley"


def test_in_namespace_node_id_is_honored():
    # observation: is an agent-writable (creation_surface=record_template) prefix.
    rid = _record_id(
        "observation", {"node_id": "observation:mine"}, _SHA, _TMPL, _PREFIXES,
    )
    assert rid == "observation:mine"


@pytest.mark.parametrize("curated", [
    "doc:curated-thing",   # the exact case the PACT names
    "commit:abc123",
    "file:src/x.py",
    "issue:42",
    "pr:7",
    "chunk:doc-1#0",
    "mention:m1",
    "session:s1",
])
def test_curated_builtin_prefix_node_id_is_refused(curated):
    # These prefixes ARE in the active builtin set, but carry
    # creation_surface="forbidden" (ingest-owned). An agent must not be able to
    # mint/collide with them via a caller-supplied node_id.
    rid = _record_id("observation", {"node_id": curated}, _SHA, _TMPL, _PREFIXES)
    assert rid != curated
    assert rid.startswith("observation:record:")


def test_absent_node_id_is_server_minted():
    rid = _record_id("observation", {}, _SHA, _TMPL, _PREFIXES)
    assert rid == f"observation:record:{_SHA[:20]}"


def test_no_active_prefixes_fails_closed():
    # With no namespace configured, fail closed: do NOT honor a caller id (which
    # could collide with a curated node), server-mint instead.
    rid = _record_id("observation", {"node_id": "doc:x"}, _SHA, _TMPL, ())
    assert rid != "doc:x"
    assert rid.startswith("observation:record:")


# ---- domain_entity id_attr path (the second mint route — must also be gated) ----


@pytest.mark.parametrize("payload", [
    {"domain_entity_id": "doc:victim"},          # explicit curated prefix
    {"domain_entity_id": "commit:deadbeef"},
    {"domain_entity_id": "file:/etc/passwd"},
    {"domain_entity_id": "session:s1"},
    {"domain_entity_id": "x", "domain_kind": "doc"},   # curated prefix via domain_kind
    {"domain_entity_id": "y", "domain_kind": "commit"},
])
def test_domain_entity_cannot_mint_curated_namespace(payload):
    # The domain_entity id_attr path honored `doc:victim` / domain_kind="doc"
    # verbatim, bypassing the node_id creation_surface gate. It must server-mint
    # instead of minting in a curated/ingest-owned namespace.
    rid = _record_id("domain_entity", payload, _SHA, _DOMAIN_TMPL, _PREFIXES)
    assert not rid.startswith(("doc:", "commit:", "file:", "session:")), rid
    assert rid == f"domain:record:{_SHA[:20]}"


def test_domain_entity_legit_namespace_still_honored():
    # An agent-writable domain namespace (region:/method:/dataset:) is honored,
    # whether supplied as an explicit prefix or via domain_kind.
    assert _record_id(
        "domain_entity", {"domain_entity_id": "SR", "domain_kind": "region"},
        _SHA, _DOMAIN_TMPL, _PREFIXES,
    ) == "region:SR"
    assert _record_id(
        "domain_entity", {"domain_entity_id": "region:SR"},
        _SHA, _DOMAIN_TMPL, _PREFIXES,
    ) == "region:SR"
    # A NOVEL domain namespace (not a builtin at all, not curated) is allowed —
    # domain entities are extensible.
    assert _record_id(
        "domain_entity", {"domain_entity_id": "protein:p53"},
        _SHA, _DOMAIN_TMPL, _PREFIXES,
    ) == "protein:p53"
