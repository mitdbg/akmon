"""Hardened XML parsing (constrain-graph-file-and-parser-ingestion).

Source/network XML must be parsed with DTD/entity expansion disabled so
billion-laughs and XXE inputs raise rather than expand, while benign payloads
still parse into the expected records.
"""
from __future__ import annotations

import pytest

from okg.substrate.library.sources._safe_xml import (
    safe_lxml_fromstring,
    safe_xml_fromstring,
)
from okg.substrate.library.sources.scholarly.arxiv import parse_arxiv_payload

_BILLION_LAUGHS = """<?xml version="1.0"?>
<!DOCTYPE lolz [
  <!ENTITY lol "lol">
  <!ENTITY lol2 "&lol;&lol;&lol;&lol;&lol;">
]>
<root>&lol2;</root>"""

_XXE = """<?xml version="1.0"?>
<!DOCTYPE foo [ <!ENTITY xxe SYSTEM "file:///etc/passwd"> ]>
<root>&xxe;</root>"""

_BENIGN_ATOM = """<?xml version="1.0"?>
<feed xmlns="http://www.w3.org/2005/Atom" xmlns:arxiv="http://arxiv.org/schemas/atom">
  <entry>
    <id>http://arxiv.org/abs/2401.00001v1</id>
    <title>A Benign Paper</title>
    <summary>Hello.</summary>
    <published>2024-01-01T00:00:00Z</published>
    <author><name>Ada Lovelace</name></author>
  </entry>
</feed>"""


# ---- shared helpers ----


def test_safe_xml_fromstring_rejects_entity_bomb():
    with pytest.raises(Exception):  # defusedxml EntitiesForbidden
        safe_xml_fromstring(_BILLION_LAUGHS)


def test_safe_xml_fromstring_rejects_xxe():
    with pytest.raises(Exception):  # defusedxml entity/external forbidden
        safe_xml_fromstring(_XXE)


def test_safe_xml_fromstring_parses_benign():
    root = safe_xml_fromstring(_BENIGN_ATOM)
    assert root.tag.endswith("feed")


def test_safe_lxml_fromstring_does_not_expand_entities():
    # Hardened lxml: entities are not resolved (no expansion); attacker content
    # is not materialized into the tree.
    root = safe_lxml_fromstring(_BILLION_LAUGHS.encode("utf-8"))
    text = "".join(root.itertext())
    assert "lollol" not in text  # the bomb did not expand


def test_safe_lxml_fromstring_parses_benign():
    root = safe_lxml_fromstring(b"<r><a>1</a><a>2</a></r>")
    assert [e.text for e in root.findall("a")] == ["1", "2"]


# ---- arxiv.parse_arxiv_payload routes through the hardened parser ----


def test_arxiv_payload_rejects_entity_bomb():
    with pytest.raises(Exception):
        parse_arxiv_payload(_BILLION_LAUGHS)


def test_arxiv_payload_parses_benign_entry():
    rows = parse_arxiv_payload(_BENIGN_ATOM)
    assert len(rows) == 1
    assert rows[0]["title"] == "A Benign Paper"


# ---- dblp_by_orcid._stream routes through the hardened lxml parser ----


def test_dblp_by_orcid_stream_does_not_expand_entities():
    from okg.substrate.library.sources.scholarly.dblp_by_orcid import (
        DblpByOrcidSource,
    )
    src = DblpByOrcidSource(orcid="0000-0001-0000-0000")
    # The bomb must not expand; _stream yields no records (and does not hang).
    rows = list(src._stream(_BILLION_LAUGHS.encode("utf-8")))
    assert rows == []
