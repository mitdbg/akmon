"""PaperPdfSource — substrate adapter for an inbox of PDF papers.

Reads every `*.pdf` under the deployment-configured `inbox_path`
(recursively) and emits one `Paper` NodeFact per file with
`attrs.kind = research`, `attrs.tier = 1`. The Paper's
`paper_id` is content-hash-stable across runs so the source is
idempotent: a PDF whose bytes have not changed produces the
same `paper_id`.

Identity convention:

  `paper:<content_hash_first_16_chars>-<filename_slug>`

Per umbrella D11 + S-A1-R5: the source accepts `inbox_path` as
a parameter. The substrate hardcodes no operator-specific path;
the deployment's `source_registry.yaml` carries the value.

After emission, the PaperPdfSource pairs with the
`PdfStructuralEnricher` (registered in the deployment's
`enrichers:` block). The enricher picks up the new Paper on the
next publish cycle, parses the PDF, and emits the structural
anatomy (or an `extraction_error` Observation per the
confidence-floor logic).
"""

from __future__ import annotations

import hashlib
import logging
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator, Optional

from okg.substrate.library.sources.base import (
    NodeFact,
    RecordEmission,
    RecordNode,
    RecordSet,
    SourceRun,
)

__all__ = ["PaperPdfSource", "compute_paper_id"]

logger = logging.getLogger(__name__)


_SLUG_RE = re.compile(r"[^a-z0-9]+")
_DOI_RE = re.compile(r"(?:https?://(?:dx\.)?doi\.org/|doi:\s*)?(10\.\d{4,9}/[-._;()/:a-z0-9]+)", re.IGNORECASE)
_FILENAME_BIB_RE = re.compile(
    r"^\s*(?P<authors>.+?)\s+-\s+"
    r"(?P<year>(?:18|19|20|21)\d{2})\s+-\s+"
    r"(?P<title>.+?)\s*$"
)
_YEAR_RE = re.compile(r"\b((?:18|19|20|21)\d{2})\b")


def _slugify(value: str) -> str:
    slug = _SLUG_RE.sub("-", value.lower()).strip("-")
    return slug[:40] or "untitled"


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    return " ".join(str(value).replace("\x00", " ").split()).strip()


def _first_doi(*values: Any) -> str:
    for value in values:
        text = _clean_text(value)
        if not text:
            continue
        match = _DOI_RE.search(text)
        if match:
            return match.group(1).strip().rstrip(".,;")
    return ""


def _first_year(*values: Any) -> str:
    for value in values:
        text = _clean_text(value)
        if not text:
            continue
        match = _YEAR_RE.search(text)
        if match:
            return match.group(1)
    return ""


def _filename_bibliography(pdf_path: Path) -> dict[str, str]:
    stem = _clean_text(pdf_path.stem)
    parsed = _FILENAME_BIB_RE.match(stem)
    if not parsed:
        year = _first_year(stem)
        return {
            "filename_stem": stem,
            "filename_title": stem,
            **({"year": year} if year else {}),
        }
    return {
        "filename_stem": stem,
        "display_authors": _clean_text(parsed.group("authors")),
        "year": parsed.group("year"),
        "filename_title": _clean_text(parsed.group("title")),
    }


def _pdf_bibliography(pdf_path: Path) -> tuple[dict[str, str], str | None]:
    try:
        import pypdf

        with pdf_path.open("rb") as f:
            reader = pypdf.PdfReader(f, strict=False)
            metadata = reader.metadata or {}
            title = _clean_text(metadata.get("/Title"))
            author = _clean_text(metadata.get("/Author"))
            subject = _clean_text(metadata.get("/Subject"))
            creation_date = _clean_text(metadata.get("/CreationDate"))
            mod_date = _clean_text(metadata.get("/ModDate"))
            first_page_text = ""
            if reader.pages:
                first_page_text = _clean_text(reader.pages[0].extract_text() or "")
    except Exception as exc:  # pragma: no cover - exception shape is backend-specific
        return {}, f"{type(exc).__name__}: {_clean_text(exc)}"

    attrs: dict[str, str] = {}
    if title:
        attrs["pdf_metadata_title"] = title
    if author:
        attrs["pdf_metadata_author"] = author
    if subject:
        attrs["pdf_metadata_subject"] = subject
    doi = _first_doi(title, subject, first_page_text)
    if doi:
        attrs["doi"] = doi
    year = _first_year(creation_date, mod_date, first_page_text)
    if year:
        attrs["pdf_metadata_year"] = year
    return attrs, None


def _bibliographic_attrs(pdf_path: Path) -> dict[str, Any]:
    filename_attrs = _filename_bibliography(pdf_path)
    pdf_attrs, pdf_error = _pdf_bibliography(pdf_path)
    sources: list[str] = []
    title = _clean_text(pdf_attrs.get("pdf_metadata_title"))
    if title:
        sources.append("pdf_metadata_title")
    else:
        title = _clean_text(filename_attrs.get("filename_title") or pdf_path.stem)
        if filename_attrs.get("filename_title"):
            sources.append("filename")

    year = _clean_text(filename_attrs.get("year") or pdf_attrs.get("pdf_metadata_year"))
    doi = _clean_text(pdf_attrs.get("doi"))

    attrs: dict[str, Any] = {
        "title": title,
        "filename_stem": filename_attrs["filename_stem"],
        "bibliographic_metadata_sources": sources or ["filename_stem"],
    }
    if title != filename_attrs["filename_stem"]:
        attrs["source_title"] = filename_attrs["filename_stem"]
    if year:
        attrs["year"] = year
    if doi:
        attrs["doi"] = doi
    if filename_attrs.get("display_authors"):
        attrs["display_authors"] = filename_attrs["display_authors"]
    for key in ("pdf_metadata_title", "pdf_metadata_author", "pdf_metadata_subject"):
        if pdf_attrs.get(key):
            attrs[key] = pdf_attrs[key]
    if pdf_error:
        attrs["pdf_metadata_error"] = pdf_error
        attrs["bibliographic_metadata_status"] = "pdf_parse_failed"
    else:
        attrs["bibliographic_metadata_status"] = "ok"
    return attrs


def compute_paper_id(pdf_path: Path) -> tuple[str, str]:
    """Return (paper_id, content_hash) for a PDF file.

    Content-hash-stable: same bytes → same id. The id format is
    `paper:<hash16>-<filename_slug>`, which keeps re-runs
    idempotent even when the filename changes (the hash anchors
    identity).
    """
    h = hashlib.sha256()
    with pdf_path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    content_hash = h.hexdigest()
    slug = _slugify(pdf_path.stem)
    return f"paper:{content_hash[:16]}-{slug}", content_hash


@dataclass
class PaperPdfSource:
    """Substrate source for an operator-configured PDF inbox.

    Constructor:
      `inbox_path: str` — deployment-configured path. The source
      walks this directory recursively for `*.pdf` files. No
      default; the deployment SHALL supply a value.

    The optional `kind`, `tier`, and `extra_attrs` overrides let
    a deployment tag the emitted Papers (e.g. tier=2 for a
    secondary inbox).
    """

    inbox_path: str
    kind: str = "research"
    tier: int = 1
    extra_attrs: Optional[dict[str, Any]] = None

    name: str = "paper_pdf_source"
    profile: str = "discovery_crawl"

    # Incremental-source contract (issue #323): the inbox is a plain local
    # directory of PDF bytes (no git, no API watermark), so the probe is the
    # content-hash floor over the SAME `*.pdf` enumeration `run()` walks. The
    # token is a membership-sensitive digest of (rel_path, sha256(bytes)) over
    # the current scope, so it is mtime-churn-immune yet busts on an edit (sha
    # shifts), an add (new entry + count), or a delete (entry/count drops) —
    # every emission-determining input. `kind` / `tier` / `extra_attrs` reach
    # emitted Paper attrs, so they fold into config. A missing inbox enumerates
    # to zero files (consistent with `run()`); an unreadable PDF reads as
    # "absent" identity, which still shifts the token (never false-unchanged).
    change_probe_kind = "content_hash"

    def __post_init__(self) -> None:
        if not isinstance(self.inbox_path, str) or not self.inbox_path:
            raise ValueError(
                "PaperPdfSource: `inbox_path` must be a non-empty string"
            )
        self._inbox = Path(self.inbox_path).expanduser()
        self.change_probe = self._build_change_probe()

    def _build_change_probe(self) -> Any:
        import sys

        from okg.substrate.library.sources.content_hash_probe import (
            ContentHashProbe,
            file_content_items,
        )

        def _items() -> list[tuple[str, Path]]:
            return file_content_items(self._inbox, include_globs=("*.pdf",))

        return ContentHashProbe(
            content_items=_items,
            config={
                "inbox_path": self.inbox_path,
                "kind": self.kind,
                "tier": self.tier,
                "extra_attrs": self.extra_attrs,
            },
            emit_targets=[sys.modules[__name__]],
        )

    def run(
        self,
        run_id: str,
        *,
        mode: str = "cursor",
        cursor: Optional[dict[str, Any]] = None,
        sync_scope: Optional[dict[str, Any]] = None,
    ) -> SourceRun:
        revision = {"run_id": run_id}
        discovered: list[tuple[Path, str, str]] = []
        if self._inbox.is_dir():
            for pdf_path in sorted(self._inbox.rglob("*.pdf")):
                node_id, content_hash = compute_paper_id(pdf_path)
                discovered.append((pdf_path, node_id, content_hash))
        else:
            logger.info(
                "%s: inbox_path %s does not exist; emitting zero papers",
                self.name, self._inbox,
            )

        next_cursor = {
            "last_run_id": run_id,
            "n_papers_seen": len(discovered),
        }
        completed_scope = mode in {"reconcile", "scope_complete"}
        record_set = (
            self._record_set(discovered, revision=revision)
            if completed_scope else None
        )

        def _facts() -> Iterator[NodeFact]:
            for pdf_path, node_id, content_hash in discovered:
                attrs: dict[str, Any] = {
                    "paper_id": node_id,
                    "kind": self.kind,
                    "tier": self.tier,
                    "source_pdf_path": str(pdf_path),
                    "content_hash": content_hash,
                }
                attrs.update(_bibliographic_attrs(pdf_path))
                if self.extra_attrs:
                    attrs.update(self.extra_attrs)
                yield NodeFact(
                    node_id=node_id,
                    subtype="paper",
                    attrs=attrs,
                    source_record_id={
                        "inbox": str(self._inbox),
                        "pdf_path": str(pdf_path),
                        "content_hash": content_hash,
                    },
                    source_revision=revision,
                    op="I",
                )

        return SourceRun(
            facts=_facts(),
            completed_scope=completed_scope,
            next_cursor=next_cursor,
            run_mode=mode,
            record_set=record_set,
        )

    def _record_set(
        self,
        discovered: list[tuple[Path, str, str]],
        *,
        revision: dict[str, Any],
    ) -> RecordSet:
        records: dict[str, RecordEmission] = {}
        for pdf_path, node_id, content_hash in discovered:
            record_key = node_id  # one record per Paper
            records[record_key] = RecordEmission(
                primary=RecordNode(
                    node_id=node_id, subtype="paper",
                    source_record_id={
                        "inbox": str(self._inbox),
                        "pdf_path": str(pdf_path),
                        "content_hash": content_hash,
                    },
                    source_revision=revision,
                ),
                descendants=[],
                edges=[],
            )
        return RecordSet(records=records)
