from __future__ import annotations

import hashlib

from okg.substrate.library.sources.doc_corpus import _chunk_node_id


def test_existing_paragraph_chunk_id_remains_content_sha_prefix() -> None:
    text = "legacy paragraph identity"
    expected = f"chunk:{hashlib.sha256(text.encode()).hexdigest()[:16]}"
    assert _chunk_node_id(text, "paragraph") == expected


def test_new_nonparagraph_identity_transition_is_kind_sensitive() -> None:
    text = "print('same bytes')"
    assert _chunk_node_id(text, "code_block") != _chunk_node_id(text, "paragraph")
    assert _chunk_node_id(text, "code_block") == _chunk_node_id(text, "code_block")
