"""Hardened XML parsing for source- or network-fetched content.

Scholarly/RSS adapters parse XML fetched from remote (or deployment-overridable)
endpoints. Parsing such content with the stdlib / default-lxml parser leaves
DTD processing and entity expansion enabled — the billion-laughs (entity-bomb)
and XXE (external-entity / SSRF) surfaces. This module wraps `defusedxml` (for
the ElementTree sites) and a hardened `lxml.etree.XMLParser` (for the lxml site)
so those inputs raise rather than expand (per
`constrain-graph-file-and-parser-ingestion`).
"""
from __future__ import annotations

from typing import Any


def safe_xml_fromstring(text: str | bytes) -> Any:
    """Parse XML with defusedxml: internal/external entity expansion and
    external resource resolution are forbidden, so billion-laughs / XXE inputs
    raise (EntitiesForbidden / ExternalReferenceForbidden) rather than expand.
    Returns a standard `xml.etree.ElementTree.Element`."""
    from defusedxml.ElementTree import fromstring
    return fromstring(text)


def safe_lxml_fromstring(data: str | bytes) -> Any:
    """Parse XML with a hardened lxml parser: no entity resolution, no network
    access, no huge-tree, no DTD load. Returns an `lxml.etree._Element`."""
    from lxml import etree
    if isinstance(data, str):
        data = data.encode("utf-8")
    parser = etree.XMLParser(
        resolve_entities=False,
        no_network=True,
        huge_tree=False,
        load_dtd=False,
        dtd_validation=False,
    )
    return etree.fromstring(data, parser=parser)
