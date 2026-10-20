"""DocCorpusSource — substrate-library adapter that walks a
filesystem corpus, dispatches each file to a registered parser
+ chunker + NER backend, and emits Document / DocumentChunk /
EntityMention nodes plus the edges connecting them.

First deployment-driven consumer of the extraction library
(`add-doc-corpus-extraction-adapter`, 2026-04-25).

State translation:
  - cursor: per-path `last_content_sha256` so unchanged files
    skip parsing on re-run. Empty cursor on first run.
  - watermark advance: standard SourceRunner contract via
    `next_cursor`.

Profile: `discovery_crawl`. Files absent from a fresh walk are
retraction candidates (caller sets `mode='reconcile'` to opt
into op='R' emission).

Dated-snapshot mode:
  With `snapshot_date='YYYY-MM-DD'` the corpus is one dated
  OBSERVATION of a document set rather than a standing index.
  Every document and chunk then carries `snapshot_date`, and
  every chunk additionally carries the positional key
  (`doc_path`, `chunk_ordinal`) that relates the same passage
  across two snapshots. Node identity stays content-addressed,
  so a changed passage keeps its key and gets a new node id.
  Left unset (the default) emission is unchanged.

Candidate files (issue #2357):
  In a git work tree the candidate set is what git tracks
  (`git ls-files --cached`), plus untracked-but-not-ignored files
  when `include_untracked=True`, intersected with the
  include/exclude globs. A git-ignored file is never read unless
  it matches `include_ignored_paths`, a narrow explicit opt-in.
  A root that git does not see (not a work tree, or itself
  ignored) is refused unless `non_git_walk=True`, which keeps the
  historical filesystem walk. `project_manifest` corpora keep the
  manifest's own inventory.

Oversize files (issues #2245, #1692):
  A file larger than `max_bytes_per_file` is recorded as a Document
  with `parse_warnings: ["oversize"]` and no chunks (an image or
  PDF becomes a visual artifact whose caption is skipped as
  `source_too_large`). Its bytes are never loaded; it is logged and
  counted in `last_run_stats`, and the run continues.

Document-to-file link:
  `doc:<repo>:<path> derived_from file:<repo>:<path>` is asserted
  exactly when the `source_file` node exists in the graph (issue
  #2688), whichever source wrote it and in whichever order the
  sources ran. The cursor records which paths carry the link, so
  an unchanged corpus still re-checks it: a file node that appears
  or disappears re-emits only the affected documents. Without a
  `dsn` the source cannot see file nodes and asserts no link.

Predecessor invariant:
  When constructed with `predecessor_source='<name>'`, the
  adapter refuses to start if the named source hasn't reconciled
  in the current cycle. This makes the dependency on the
  file-walking predecessor (typically `git-files`) explicit, so
  the first run already sees its `source_file` nodes.
"""

from __future__ import annotations

import fnmatch
import hashlib
import io
import json
import logging
import mimetypes
import os
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, ClassVar, Iterator, Mapping, Optional, Sequence

import psycopg

from okg.substrate.errors import SourceRootMissing, SubstrateError
from okg.substrate.library.extraction._protocols import DEFAULT_CHUNK_KIND
from okg.substrate.library.extraction.chunk.latex_structural import (
    INTERNAL_FIGURE_ENVIRONMENT_SPAN,
)
from okg.substrate.library.extraction.pdf_table_vlm import (
    MemoryTableResponseCache,
    PdfTableTranscriptionConfig,
    PdfTableTranscriptionService,
    TableCropRenderer,
    TableRegionLocator,
    TableResponseCache,
    TableTranscriptionProvider,
    build_table_transcription_provider,
)
from okg.substrate.library.extraction.parse.dispatch import (
    sniff_format,
)
from okg.substrate.library.sources._code_graph_common import _scoped_symbol_name
from okg.substrate.library.sources._file_walk import FileContainmentError
from okg.substrate.library.extraction.registry import load_extractors
from okg.substrate.library.extraction.table_grid import table_occurrence_id
from okg.substrate.llm.tracking import resolve_tracking_dsn
from okg.substrate.library.sources.base import (
    EdgeFact,
    EdgeKey,
    NodeFact,
    RecordEmission,
    RecordNode,
    RecordSet,
    SourceRun,
    cursor_has_record_set_authority,
    preserve_cursor_record_set_authority,
    source_state_without_runner_keys,
)


log = logging.getLogger(__name__)


def _glob_to_regex(pat: str) -> "re.Pattern[str]":
    """Convert a gitignore/glob pattern to a regex.

    Supported syntax:
      `**` matches any number of path segments (including zero).
      `*` matches any chars except `/` within a segment.
      `?` matches a single non-`/` char.
      Other chars match literally.

    `fnmatch.fnmatch` doesn't recurse on `**`, so we can't use
    it for the `**/<dir>/**` exclude patterns this proposal
    relies on. This builds the equivalent regex once per
    pattern; matches against the full repo-relative path.
    """
    out = ["^"]
    i = 0
    while i < len(pat):
        c = pat[i]
        if c == "*":
            if pat[i : i + 2] == "**":
                # Skip an optional trailing /
                if pat[i + 2 : i + 3] == "/":
                    out.append(r"(?:.*/)?")
                    i += 3
                    continue
                out.append(r".*")
                i += 2
                continue
            out.append(r"[^/]*")
            i += 1
            continue
        if c == "?":
            out.append(r"[^/]")
            i += 1
            continue
        out.append(re.escape(c))
        i += 1
    out.append("$")
    return re.compile("".join(out))


_GLOB_CACHE: dict[str, "re.Pattern[str]"] = {}


def _match_any(rel: Path, patterns: Sequence[str]) -> bool:
    """Match `rel` against any of the patterns. Recursive `**`
    is supported via `_glob_to_regex`."""
    rel_str = str(rel).replace("\\", "/")
    for pat in patterns:
        if pat in ("**/*", "**", "*"):
            return True
        rx = _GLOB_CACHE.get(pat)
        if rx is None:
            rx = _glob_to_regex(pat)
            _GLOB_CACHE[pat] = rx
        if rx.match(rel_str):
            return True
    return False


def _normalize_scope_key(key: str) -> str | None:
    """Repo-relative posix form of a scope key, or None when it leaves the root.

    `./docs/a.md`, `docs//a.md` and `/docs/a.md` all name `docs/a.md`, the
    form git and the filesystem walk report, so a scoped run finds the file.
    """
    import posixpath

    rel = posixpath.normpath(str(key).replace("\\", "/").lstrip("/"))
    if rel in ("", ".", "..") or rel.startswith("../"):
        return None
    return rel


__all__ = [
    "DocCorpusNotGitWorkTree",
    "DocCorpusOversize",
    "DocCorpusSource",
    "PredecessorNotRun",
    "chunk_node_id",
]


class DocCorpusNotGitWorkTree(SubstrateError):
    """`repo_root` is not visible to git and the source did not opt in to
    the filesystem walk. Without git the source cannot tell a secret-bearing
    ignored file from a corpus member, so it refuses rather than guess."""

    code = "doc_corpus_not_git_work_tree"


class DocCorpusOversize(SubstrateError):
    """File-discovery exceeded the configured `max_files` budget.
    Loud refusal so operators tighten `exclude_globs` rather
    than silently truncating the corpus."""

    code = "doc_corpus_oversize"


class PredecessorNotRun(SubstrateError):
    """The configured `predecessor_source` has no reconcile
    timestamp; refuse to start so the adapter doesn't emit
    `derived_from` edges to nonexistent endpoints."""

    code = "predecessor_not_run"


# Extension → format-key dispatch. Deliberately conservative:
# only the formats this proposal's `extractors.yaml` registers
# by default. Unknown extensions fall through to `sniff_format`
# (magic-byte) and then to a `no_parser` warning if still
# unresolved. Per Stride A.1 (2026-04-26), deployments can
# override / extend this map via the
# `DocCorpusSource(extension_to_format=...)` constructor
# kwarg without forking the adapter.
DEFAULT_EXTENSION_TO_FORMAT: dict[str, str] = {
    ".md": "markdown",
    ".markdown": "markdown",
    ".txt": "text",
    ".rst": "text",
    ".json": "json",
    ".yaml": "yaml",
    ".yml": "yaml",
    ".toml": "toml",
    ".html": "html",
    ".htm": "html",
    ".pdf": "pdf",
    ".py": "code",
    ".csv": "csv",
    ".tsv": "tsv",
    ".xlsx": "xlsx",
    ".docx": "docx",
    ".eml": "eml",
    ".email": "email",
    ".ipynb": "ipynb",
    ".tex": "latex",
}

# Opt-in project routing only. Keeping these outside the global default is a
# compatibility contract: existing file-only corpora containing TeX support or
# BibTeX files must retain their prior unknown/no-format behavior.
PROJECT_LATEX_EXTENSION_TO_FORMAT: dict[str, str] = {
    ".sty": "latex",
    ".cls": "latex",
    ".def": "latex",
    ".bst": "latex",
    ".bbx": "latex",
    ".cbx": "latex",
    ".dbx": "latex",
    ".lbx": "latex",
    ".bib": "text",
}

# Increment whenever unchanged source bytes must be re-emitted because the graph
# footprint or cursor-cache shape changed. A missing/older value defeats both
# corpus-level short-circuits and per-file record-set reuse exactly once, then the
# current value is persisted in the next cursor. Version 2 introduced required
# ``chunk_kind`` plus kind-sensitive caption identities. Version 3 moves table
# chunks to source-occurrence identity and persists structured table outcomes.
# Version 4 rasterizes standalone PDF figures and persists caption outcomes.
# Version 5 links author-written LaTeX figure captions to their selected visuals.
# Version 6 writes `chunk_ordinal` and `chunk_total` on every chunk's member_of
# edge (issue #2668), so an unchanged document re-emits once to gain them.
# Version 7 declares content-addressed chunks, visual and mention nodes
# `substitute` (issue #3382): every corpus re-emits once, so no reused record
# still says `authoritative` beside a new one that says `substitute`, and every
# source records its claim on the shared nodes it emits.
DOC_CORPUS_EMISSION_SCHEMA_VERSION = 7
#: Ownership of a node whose id is derived from content, so another source can
#: emit the same node: a content-addressed chunk (every kind but a table
#: occurrence), a `visual:` node (image bytes) and a `mention:` node (seeded on
#: its chunk). Issue #3382: two repositories carrying the same text share one
#: node. `substitute` is the shared-node contract of issue #1901: the stored
#: row keeps its owner, every emitting source records a claim in
#: `okg.graph_node_declarers`, coverage accepts the node under any owner, and
#: a retraction or compensation by the owner hands the row to a surviving
#: claimant instead of deleting it.
SHARED_NODE_OWNERSHIP_MODE = "substitute"
#: Cursor key recording the max_bytes_per_file a run used (see
#: `max_bytes_unchanged` in `DocCorpusSource._run_with_reader`).
_MAX_BYTES_CURSOR_KEY = "doc_corpus_max_bytes_per_file"
#: Written after a scoped run, or a run that did not complete its scope,
#: under a changed limit: matches no real limit,
#: so the next run re-parses the whole corpus.
_MAX_BYTES_MIXED = -1

# Increment when some files' emission changes but no record's identity or
# revision does, so a corpus must re-parse its unchanged files once without
# rewriting every row's revision (the emission schema version above is part of
# every record's revision). A missing/older value in the cursor forces one full
# re-parse; files whose emission is unchanged produce the same facts, so the
# runner writes nothing for them. Epoch 1: YAML files also emit their original
# text as `code_block` chunks (#2671). Epoch 2: a chunk's stored `text` is no
# longer whitespace-stripped under a `text`-only searchable declaration
# (#2972); the re-parse lets the runner's dedupe rewrite the stripped rows.
# Epoch 3: `MarkdownHeadingChunker` chunks keep the blank lines between
# sections and carry their real span, so they tile the document (#2997); the
# re-parse replaces the old Markdown chunks, which re-embeds them once.
# Epoch 4: a chunk with no real span stores no position (it was offset 0), PDF,
# Word, spreadsheet, notebook and semantic-chunker chunks store their real
# span, and each text chunk's member_of edge gains `doc_char_length` and its
# stored separators. Chunk texts, and so chunk ids and embeddings, do not
# change; the re-parse rewrites only chunk attributes and member_of edge
# attributes. Epoch 5: a YAML file's re-emission is written with its
# characters and line breaks as written instead of unicode escapes, folded
# lines and quoted line breaks (#2179); the re-parse replaces those chunks.
DOC_CORPUS_REPARSE_EPOCH = 5

# Cursor key: sorted corpus paths whose document carries the
# `derived_from source_file` link (issue #2688).
SOURCE_FILE_LINKS_KEY = "doc_corpus_source_file_links"

# `ParsedDocument.metadata` key a parser sets when `text` is a re-emission
# (YAML's sorted, comment-free dump) and the file as written differs: the
# source chunks it as well, as `code_block`, so comments and the author's key
# order reach search.
ORIGINAL_TEXT_METADATA_KEY = "original_text"
ORIGINAL_TEXT_CHUNK_KIND = "code_block"
# What a document's positions put between the re-emission and the file as
# written (so an original chunk's offset is past both).
_ORIGINAL_TEXT_JOIN = "\n\n"

_INTERNAL_CAPTION_VISUAL_IDS = "_okg_caption_visual_ids"
# Occurrence-only attributes a text chunk's member_of edge carries and its
# content-addressed node must not (see `_occurrence_layout`).
_INTERNAL_OCCURRENCE_ATTRS = "_okg_occurrence_attrs"
#: Longest whitespace run stored as a chunk's `separator_before` /
#: `separator_after`. A longer run stays a gap, and a whole read says partial.
MAX_STORED_SEPARATOR_CHARS = 256


_TEXT_SNIFF_BYTES = 8192
# `#!/usr/bin/python3`, `#!/usr/bin/env python`, `#!/usr/bin/env -S python3 -u`;
# not `#!/opt/python3/bin/bash`.
_PYTHON_SHEBANG = re.compile(
    rb"#!\s*(?:\S*/)?(?:env\s+(?:-\S*\s+)*)?python[0-9.]*(?:\s|$)"
)


def _looks_like_text(content: bytes) -> bool:
    """True when a file reads as UTF-8 text: no NUL byte anywhere, and its
    first `_TEXT_SNIFF_BYTES` decode (a multi-byte character cut by that
    limit is allowed; one cut at the real end of the file is not)."""
    if not content or b"\x00" in content:
        return False
    head = content[:_TEXT_SNIFF_BYTES]
    try:
        head.decode("utf-8")
        return True
    except UnicodeDecodeError as exc:
        return len(content) > len(head) and exc.reason == "unexpected end of data"


def _python_shebang(content: bytes) -> bool:
    """True when the first line is a shebang that runs python."""
    return _PYTHON_SHEBANG.match(content.split(b"\n", 1)[0]) is not None


def _chunk_identity_sha(text: str, chunk_kind: str = DEFAULT_CHUNK_KIND) -> str:
    """Return the sha256 of the exact seed a chunk node id is hashed from.

    `_chunk_node_id` truncates this digest; a content-addressed chunk's
    `source_revision` carries it whole (see
    `DocCorpusSource._record_revision`). Both must read the same seed, so
    there is one definition of "this chunk's own content".
    """
    seed = text if chunk_kind == DEFAULT_CHUNK_KIND else f"{chunk_kind}\0{text}"
    return hashlib.sha256(seed.encode("utf-8")).hexdigest()


def _chunk_node_id(text: str, chunk_kind: str = DEFAULT_CHUNK_KIND) -> str:
    """Return the collision-safe pre-canonical chunk node id.

    Paragraphs deliberately retain the historical content-only identity. Typed
    non-paragraph chunks include their kind so identical text used as, for example,
    prose and an equation cannot race through last-write-wins node dedup.
    """
    return f"chunk:{_chunk_identity_sha(text, chunk_kind)[:16]}"


def chunk_node_id(
    text: str,
    chunk_kind: str = DEFAULT_CHUNK_KIND,
    *,
    doc_id: str | None = None,
    extras: Mapping[str, Any] | None = None,
) -> str:
    """Return the pre-canonical `document_chunk` node id for one extracted chunk.

    This is the ONE definition of chunk identity. Every producer that emits a
    `document_chunk` node OR points an edge at one must call it, so the two
    never drift: `DocCorpusSource` creates the nodes, while
    `LatexProjectSource` points `paper_section --contains-->` and
    `citation --references-->` edges at chunks it re-derives from the same
    `LatexStructuralChunker` output. When the two disagreed, admission rejected
    the edge because its destination did not exist.

    Table chunks carry source-occurrence identity (per
    ``DOC_CORPUS_EMISSION_SCHEMA_VERSION`` 3) so two documents whose tables
    render to identical searchable text stay two nodes; every other kind keeps
    content-addressed identity. `doc_id` and the chunk's `source_locator`
    extra are therefore required for a table; without them there is no
    occurrence to name and the caller gets the content-addressed id, which is
    what pre-v3 callers already stored.
    """
    locator = (extras or {}).get("source_locator")
    if chunk_kind == "table" and doc_id and isinstance(locator, str):
        return table_occurrence_id(doc_id, locator)
    return _chunk_node_id(text, chunk_kind)


IMAGE_EXTENSIONS: frozenset[str] = frozenset(
    {
        ".png",
        ".jpg",
        ".jpeg",
        ".gif",
        ".webp",
        ".tiff",
    }
)

# Maps a PIL image `format` string to the mime_type persisted on a
# VisualArtifact node. Per `first-class-visual-artifacts` (Task 2).
_PIL_FORMAT_TO_MIME: dict[str, str] = {
    "PNG": "image/png",
    "JPEG": "image/jpeg",
    "GIF": "image/gif",
    "WEBP": "image/webp",
    "TIFF": "image/tiff",
    "BMP": "image/bmp",
}

_DEFAULT_MIN_IMAGE_PX: int = 10_000  # 100×100 pixels
_DEFAULT_PDF_RENDER_DPI: int = 100
_STANDALONE_PDF_PAGE_NUMBER: int = 1
_STANDALONE_PDF_RENDERER: str = "pdfium"


# The PDF helpers moved to `okg.substrate.imaging` so the MCP image read
# (issue #3046) renders with the same code; the private names stay bound
# here because this module's callers and tests use them.
from okg.substrate.imaging import (  # noqa: E402
    has_pdf_magic as _has_pdf_magic,
    pdfium_renderer_version as _pdfium_renderer_version,
    render_pdf_first_page as _render_pdf_first_page,
)


@dataclass
class CaptioningConfig:
    """Parsed `captioning:` block from extractors.yaml."""

    enabled: bool = False
    mode: str = "coarse"
    # No default: a deployment that enables captioning must name the VLM
    # explicitly (enforced by `_load_captioning_config`). The empty string
    # only ever survives on a disabled config, where it is never used.
    model: str = ""
    min_image_px: int = _DEFAULT_MIN_IMAGE_PX
    max_images_per_doc: Optional[int] = None
    # Standalone PDF figures are rasterized before the ordinary Pillow/VLM
    # path. 100 dpi was measured as the cost/legibility knee on CMS plots.
    pdf_render_dpi: int = _DEFAULT_PDF_RENDER_DPI
    # Deployment-wide coarse prompt override (applies to every source).
    coarse_prompt: str = ""
    # Per-datasource coarse prompt overrides, keyed by source `name`.
    prompts: dict[str, str] = field(default_factory=dict)
    # Optional Postgres DSN for VLM call tracking (per
    # `add-substrate-llm-call-tracking`). None (the default) means
    # tracking is off — VLMClient does not require a `caller`. Set this
    # to wire vision calls into `okg.llm_calls`; once set, this source
    # supplies a stable caller + per-visual fragment_id on every call.
    tracking_dsn: Optional[str] = field(default=None, repr=False)
    # ``None`` preserves the historical contract for an explicit
    # tracking_dsn; a concrete mode is required to inherit the worker DSN.
    tracking_mode: Optional[str] = None
    # Optional deployment name stamped on tracked VLM calls, so
    # `okg metrics --llm --deployment <name>` finds them. Sources don't
    # know their deployment, so the captioning block names it explicitly.
    deployment: Optional[str] = None
    # Inline figure-region detection. When true, PDF visual discovery runs
    # DocLayout-YOLO over each page AT INGEST (see
    # okg.substrate.library.captioning.figure_detector.FigureDetector) and
    # crops each detected `figure` box via `page.get_pixmap(clip=...)` with the
    # anchored `figure_caption` text as VLM context — replacing the
    # embedded-raster-by-xref + `get_drawings() >= 5` vector-render heuristics.
    # Requires the `vision` extra (doclayout-yolo); enabling this without it
    # fails loud at config load. Table boxes are inert provenance (never
    # cropped). Off by default so non-vision deployments are unaffected.
    figure_detection: bool = False
    # Optional path to DocLayout-YOLO weights. When None, the detector resolves
    # the DocStructBench model from the HF hub (cached after first download).
    figure_detection_weights: Optional[str] = None

    # Resolved once per source run. Kept out of config serialization and repr
    # so the credential cannot enter fingerprints, graph attrs, or logs.
    resolved_tracking_dsn: Optional[str] = field(default=None, init=False, repr=False)

    def resolve_tracking(self, runtime_dsn: str | None) -> str | None:
        self.resolved_tracking_dsn = resolve_tracking_dsn(
            tracking_mode=self.tracking_mode,
            explicit_tracking_dsn=self.tracking_dsn,
            runtime_dsn=runtime_dsn,
            component="DocCorpusSource captioning",
        )
        return self.resolved_tracking_dsn

    def prompt_for(self, source_name: str) -> str:
        """Resolve the coarse prompt for a source.

        Precedence: per-source override → deployment-wide override →
        empty string (caller passes ``None`` so the engine default is used).
        """
        return self.prompts.get(source_name) or self.coarse_prompt or ""


@dataclass(frozen=True)
class _CaptionCallOutcome:
    """Safe in-memory result of one caption provider call."""

    results: Optional[Sequence[Any]] = None
    failure_kind: Optional[str] = None
    rate_limit_hits: int = 0


@dataclass(frozen=True)
class _PendingCaptionCall:
    """One planned standalone-PDF call and its serial emission continuation."""

    caption_svc: Any
    planned: dict[str, Any]
    image_label: str
    finish: Callable[[_CaptionCallOutcome], None]


def _load_captioning_config(extractors_dir: Path) -> CaptioningConfig:
    """Read the optional `captioning:` block from extractors.yaml.

    An absent file or an absent/empty/``false`` ``captioning:`` block
    yields a disabled default. A *malformed* block (e.g. a non-integer
    ``min_image_px`` or a non-mapping ``prompts``) raises rather than
    silently disabling captioning — a silent disable would be a P0
    default-value-as-fallback (see CLAUDE.md).

    Captioning has **no default model**: an enabled block that omits
    ``model`` raises, so a deployment must name the VLM it wants (and
    will be billed for) rather than inheriting an unnamed default.
    """
    import yaml as _yaml

    yaml_path = Path(extractors_dir) / "extractors.yaml"
    try:
        with yaml_path.open() as f:
            doc = _yaml.safe_load(f) or {}
    except FileNotFoundError:
        return CaptioningConfig()
    cfg = doc.get("captioning")
    if not isinstance(cfg, dict):
        # Absent, empty, or `captioning: false` → captioning disabled.
        return CaptioningConfig()
    enabled = bool(cfg.get("enabled", False))
    raw_model = cfg.get("model")
    model = str(raw_model).strip() if raw_model is not None else ""
    if enabled and not model:
        # Fail loud rather than picking a model the operator never named.
        raise ValueError(
            f"captioning.model in {yaml_path} is required when "
            f"captioning.enabled is true; set it to a litellm model string "
            f"(e.g. 'openai/gpt-4o'). Captioning has no default model."
        )
    raw_prompts = cfg.get("prompts") or {}
    if not isinstance(raw_prompts, dict):
        raise ValueError(
            f"captioning.prompts in {yaml_path} must be a mapping, got "
            f"{type(raw_prompts).__name__}"
        )
    prompts = {str(k): str(v) for k, v in raw_prompts.items()}
    raw_tracking_dsn = cfg.get("tracking_dsn")
    if raw_tracking_dsn is not None and not isinstance(raw_tracking_dsn, str):
        raise ValueError(
            f"captioning.tracking_dsn in {yaml_path} must be a string, got "
            f"{type(raw_tracking_dsn).__name__}"
        )
    raw_tracking_mode = cfg.get("tracking_mode")
    if raw_tracking_mode is not None and raw_tracking_mode not in {
        "disabled", "optional", "required",
    }:
        raise ValueError(
            f"captioning.tracking_mode in {yaml_path} must be disabled, "
            "optional, or required"
        )
    figure_detection = bool(cfg.get("figure_detection", False))
    raw_weights = cfg.get("figure_detection_weights")
    if raw_weights is not None and not isinstance(raw_weights, str):
        raise ValueError(
            f"captioning.figure_detection_weights in {yaml_path} must be a "
            f"string, got {type(raw_weights).__name__}"
        )
    figure_detection_weights: Optional[str] = (
        os.path.expandvars(raw_weights) if raw_weights else None
    )
    try:
        min_image_px = int(cfg.get("min_image_px", _DEFAULT_MIN_IMAGE_PX))
        max_images_per_doc = (
            int(cfg["max_images_per_doc"])
            if cfg.get("max_images_per_doc") is not None
            else None
        )
        pdf_render_dpi = int(
            cfg.get("pdf_render_dpi", _DEFAULT_PDF_RENDER_DPI),
        )
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f"captioning: numeric field in {yaml_path} is not an integer ({exc})"
        ) from exc
    if not 36 <= pdf_render_dpi <= 600:
        raise ValueError(
            f"captioning.pdf_render_dpi in {yaml_path} must be between 36 and "
            f"600, got {pdf_render_dpi}"
        )
    return CaptioningConfig(
        enabled=enabled,
        mode=str(cfg.get("mode", "coarse")),
        model=model,
        min_image_px=min_image_px,
        max_images_per_doc=max_images_per_doc,
        pdf_render_dpi=pdf_render_dpi,
        coarse_prompt=str(cfg.get("coarse_prompt", "") or ""),
        prompts=prompts,
        tracking_dsn=raw_tracking_dsn,
        tracking_mode=raw_tracking_mode,
        deployment=(str(cfg["deployment"]) if cfg.get("deployment") else None),
        figure_detection=figure_detection,
        figure_detection_weights=figure_detection_weights,
    )


def _load_table_transcription_config(
    extractors_dir: Path,
) -> PdfTableTranscriptionConfig:
    """Read and strictly validate the optional ``table_transcription`` block."""

    import yaml as _yaml

    yaml_path = Path(extractors_dir) / "extractors.yaml"
    try:
        with yaml_path.open() as handle:
            document = _yaml.safe_load(handle) or {}
    except FileNotFoundError:
        return PdfTableTranscriptionConfig()
    raw = document.get("table_transcription")
    if raw in (None, False):
        return PdfTableTranscriptionConfig()
    if not isinstance(raw, Mapping):
        raise ValueError(
            f"table_transcription in {yaml_path} must be a mapping or false"
        )
    return PdfTableTranscriptionConfig.from_mapping(raw)


_EXTRACTORS_ABSENT_FINGERPRINT = "extractors.yaml:absent"


def _extractors_config_fingerprint(extractors_dir: Path) -> str:
    """Fingerprint the parsed extractors.yaml that decides emission.

    extractors.yaml picks each format's parser and chunker and the NER
    backends, so editing it changes the chunks an unchanged file would get.
    The per-file reuse and both no-change short-circuits key on file bytes,
    so without this fingerprint in the cursor an extractor edit (registering
    `PythonSymbolChunker`, say) never reaches files that did not change.
    The parsed document is hashed, so comments and key order do not count.
    An absent file hashes to a fixed value, as the captioning and table
    loaders treat it as the default config; a malformed file raises here as
    it does in those loaders.
    """
    import yaml as _yaml

    yaml_path = Path(extractors_dir) / "extractors.yaml"
    try:
        with yaml_path.open() as handle:
            document = _yaml.safe_load(handle)
    except FileNotFoundError:
        return _EXTRACTORS_ABSENT_FINGERPRINT
    return hashlib.sha256(
        json.dumps(
            document,
            sort_keys=True,
            ensure_ascii=False,
            separators=(",", ":"),
            default=str,
        ).encode("utf-8")
    ).hexdigest()


@dataclass
class DocCorpusSource:
    """Walks `repo_root`, parses files via the deployment's
    `extractors.yaml`, emits Document / DocumentChunk /
    EntityMention nodes + edges."""

    repo: str
    repo_root: Path | str
    extractors_dir: Path | str
    include_globs: Sequence[str] = ("**/*",)
    exclude_globs: Sequence[str] = ()
    # Runaway-corpus guard, counted after git filtering.
    # 25,000 is about twice the 11,615 tracked files this repository has under
    # the codebase-index template's widest include globs (measured 2026-09-23);
    # the old 5,000 refused ordinary repositories once code globs were added.
    # Overflow still fails the run: a corpus this large is a config mistake.
    max_files: int = 25_000
    # A file over this size is recorded as an `oversize` Document, not fatal.
    max_bytes_per_file: int = 50 * 1024 * 1024
    # Git candidate selection (issue #2357). Default: tracked files only.
    # `include_untracked` adds untracked files git does not ignore, the same
    # set `git ls-files --others --exclude-standard` reports (git-files'
    # `include_untracked` semantics). `include_ignored_paths` names the only
    # ignored files that may be read, as git glob pathspecs relative to
    # `repo_root`. `non_git_walk` permits the historical filesystem walk when
    # git cannot see `repo_root`; without it such a root is refused.
    include_untracked: bool = False
    include_ignored_paths: Sequence[str] = ()
    non_git_walk: bool = False
    # Files whose format stays unresolved after the extension map and magic
    # bytes are skipped with a `no_format` warning. With `text_fallback`, such
    # a file is read as plain text when it looks like text (no NUL byte, valid
    # UTF-8 head) — or as Python `code` when its first line is a python shebang
    # (extension-less scripts). Binary files keep the no_format behaviour.
    # It also covers structured parse failures (templated YAML, Python 2): the
    # file is then read and chunked as text, with a `text_fallback:<format>`
    # warning. Changing it re-reads every file on the next full run, even
    # when no file changed (see `text_fallback_unchanged`).
    text_fallback: bool = False
    # `predecessor_source` accepts a single name or a sequence;
    # all named predecessors must have a non-NULL
    # `last_reconcile_at` in `okg.source_watermarks` for run()
    # to start.
    predecessor_source: Optional[str | Sequence[str]] = None
    dsn: Optional[str] = None  # used by predecessor check
    # Worker-owned deployment identity for LLM telemetry.  The source
    # registry may not redirect this stamp through captioning config.
    deployment: Optional[str] = None
    # Per Stride A.1: deployments can override or extend the
    # extension → format map. The supplied dict is merged on
    # top of `DEFAULT_EXTENSION_TO_FORMAT` (deployment values
    # win on key collision).
    extension_to_format: Optional[dict[str, str]] = None
    # Per Stride A.1: route Document / DocumentChunk /
    # EntityMention node_ids through this canonicalizer chain
    # before they hit the durable log. Same Protocol the
    # SourceRunner uses; cross-source collapse on canonical
    # identity.
    canonicalizers: Optional[Sequence[Any]] = None
    # Per `add-code-symbol-extraction` (Stride B): map file
    # suffix -> code-extractor language. Defaults to
    # `{".py": "python"}`; deployments override or extend
    # to add tree-sitter languages.
    code_extension_to_language: Optional[dict[str, str]] = None
    # Bounds fuzzy-alias `mentions` emissions. When set, aliases resolving to
    # graph nodes outside this subtype set stay unresolved for this source run.
    mention_target_subtypes: Optional[Sequence[str]] = None
    # For git-backed corpora, allow unscoped completed-scope no-change runs to
    # short-circuit on HEAD + included dirty-path metadata instead of statting
    # every document path.
    git_frontier_skip: bool = False
    # Injectable CaptionService for testing. When None, the service is
    # built from the captioning: block in extractors.yaml at run() time.
    # Has no effect when captioning.enabled=false in extractors.yaml.
    caption_service: Optional[Any] = None
    # Source-supplied citations (PACT source-supplied-search-citations):
    # when the corpus is a git checkout with a known remote, emit
    # citation_label / citation_url attrs on document + document_chunk
    # nodes. In a code_repos expansion the registry injects the repo's
    # url/tracked_branch here; standalone corpora may set them explicitly.
    # Absent url -> label-only or no citation, never a guessed link.
    url: Optional[str] = None
    tracked_branch: Optional[str] = None
    # Dated-snapshot mode (PACT okg-corpus-doc-corpus). When set, this corpus
    # is ONE OBSERVATION of a document set at a stated date, not a standing
    # index, and every emitted document + chunk carries:
    #   snapshot_date  — this exact string, verbatim
    #   doc_path       — repo-relative path of the parent document  } the
    #   chunk_ordinal  — 0-based position within that document      } key
    # The key is what makes two snapshots comparable; node identity stays
    # content-addressed so a CHANGED passage keeps the key and gets a new id.
    # The date is a PARAMETER on purpose: deriving it from a file mtime or
    # the wall clock would make the corpus a fact about the machine that
    # built it, and two such snapshots could not be ordered.
    # Left None, emission is byte-identical to before this field existed, so
    # standing corpora (okg-workspace) neither re-emit nor gain attributes.
    snapshot_date: Optional[str] = None
    # PDF table boundaries are independently injectable so tests and replay
    # fixtures never make live model calls. When transcription is enabled and
    # these are absent, the pdfplumber-backed locator/renderer are selected.
    table_region_locator: Optional[TableRegionLocator] = None
    table_crop_renderer: Optional[TableCropRenderer] = None
    table_transcription_provider: Optional[TableTranscriptionProvider] = None
    table_response_cache: Optional[TableResponseCache] = None
    # Opt-in LaTeX project selection. Accepts either an immutable manifest or
    # the deployment-friendly builder mapping (repository_authority, projects,
    # and optional budgets). File-only users retain the historical walk.
    project_manifest: Any | Mapping[str, Any] | None = None
    # Injectable figure detector for testing. Any object with a
    # `detect(doc) -> list[box]` method. When None, a real DocLayout-YOLO
    # FigureDetector is built lazily from captioning.figure_detection_weights.
    # Has no effect unless captioning.figure_detection=true.
    figure_detector: Optional[Any] = None
    # Injectable figure-crop store for testing (a `crop_store.CropStoreWriter`,
    # i.e. any object with `put(sha256, data)`). When None, a real
    # `crop_store.DsnCropStore` bound to `self.dsn` is built lazily. Discovery
    # stages each figure crop here so the VisualCaptionEnricher (Phase B) can
    # caption it without re-opening the PDF. Has no effect unless
    # captioning.figure_detection=true.
    figure_crop_store: Optional[Any] = None

    name: str = "doc_corpus"
    profile: str = "discovery_crawl"
    last_run_stats: dict[str, Any] = field(default_factory=dict, init=False)

    # Incremental-source contract (issue #323): doc_corpus reads the git
    # candidate set (tracked files, plus untracked-not-ignored or explicitly
    # named ignored paths when configured; see issue #2357), or the
    # filesystem walk under `non_git_walk`, filtered by include/exclude globs
    # plus the deployment's extractors.yaml. It falls to the
    # O(content) floor over BOTH via the source's OWN _walk_files()
    # enumeration -- the probe and the read call the same method, so the probe
    # scope equals the read scope, and a deleted or newly ignored
    # doc drops from the next enumeration. The cross-source
    # `derived_from source_file` edge depends on which file nodes the graph
    # holds; completed-scope runs re-check that against the cursor's recorded
    # link set (`_source_file_links_current`), not this cursor-mode probe.
    change_probe_kind = "content_hash"

    def __post_init__(self) -> None:
        self.repo_root = Path(self.repo_root).expanduser().resolve()
        self.extractors_dir = Path(self.extractors_dir).expanduser().resolve()
        self._contained_reader = None
        if isinstance(self.include_ignored_paths, str):
            self.include_ignored_paths = (self.include_ignored_paths,)
        self.include_ignored_paths = tuple(
            str(pattern) for pattern in (self.include_ignored_paths or ())
        )
        # Filled by `run`: repo-relative path -> byte size of each file
        # recorded as `oversize` (over `max_bytes_per_file`) this run.
        self._oversize_recorded: dict[str, int] = {}
        self._file_enumeration: str | None = None
        self._project_manifest_config: dict[str, Any] | None = None
        if isinstance(self.project_manifest, Mapping):
            self._project_manifest_config = dict(self.project_manifest)
            self.project_manifest = self._build_project_manifest()
        if (
            self.project_manifest is not None
            and self.project_manifest.repository_authority != self.repo
        ):
            raise SubstrateError(
                "DocCorpusSource repo must equal project manifest repository_authority",
                data={
                    "repo": self.repo,
                    "repository_authority": self.project_manifest.repository_authority,
                },
            )
        if self.snapshot_date is not None:
            self.snapshot_date = str(self.snapshot_date).strip()
            try:
                datetime.strptime(self.snapshot_date, "%Y-%m-%d")
            except ValueError as e:
                raise SubstrateError(
                    "DocCorpusSource snapshot_date must be an ISO calendar "
                    f"date (YYYY-MM-DD); got {self.snapshot_date!r}",
                    data={"snapshot_date": self.snapshot_date},
                    fix_hint=(
                        "pass the operator-stated freeze date, e.g. "
                        "snapshot_date: '2026-07-28'; never derive it from a "
                        "file mtime or the wall clock"
                    ),
                ) from e
        merged = dict(DEFAULT_EXTENSION_TO_FORMAT)
        if self.project_manifest is not None:
            merged.update(PROJECT_LATEX_EXTENSION_TO_FORMAT)
        if self.extension_to_format:
            merged.update(self.extension_to_format)
        # A key that starts with "." is a suffix. Any other key is an
        # fnmatch pattern on the file NAME (`Dockerfile`, `Dockerfile.*`,
        # `Makefile`): files without a telling suffix need a route too.
        # Name patterns are checked first, in map order; the first match wins.
        name_routes: list[tuple[str, str]] = []
        for key, fmt in merged.items():
            if key.startswith("."):
                continue
            if "/" in key or not key:
                raise ValueError(
                    f"extension_to_format key {key!r} is neither a suffix "
                    "(starting with '.') nor a file-name pattern (no '/'); "
                    "it could never match: use the file name alone, e.g. "
                    "'Dockerfile' or 'Dockerfile.*'"
                )
            name_routes.append((key, fmt))
        self._extension_map: dict[str, str] = {
            key: fmt for key, fmt in merged.items() if key.startswith(".")
        }
        self._name_routes: tuple[tuple[str, str], ...] = tuple(name_routes)
        if self.predecessor_source is None:
            self._predecessors: tuple[str, ...] = ()
        elif isinstance(self.predecessor_source, str):
            self._predecessors = (self.predecessor_source,)
        else:
            self._predecessors = tuple(self.predecessor_source)
        self._canonicalizers: tuple[Any, ...] = tuple(
            self.canonicalizers or (),
        )
        merged_code_lang = dict(
            self._DEFAULT_CODE_EXTENSION_TO_LANGUAGE,
        )
        if self.code_extension_to_language:
            merged_code_lang.update(self.code_extension_to_language)
        self._code_extension_to_language: dict[str, str] = merged_code_lang
        self._mention_target_subtypes = frozenset(
            str(subtype) for subtype in (self.mention_target_subtypes or ())
        )
        self.change_probe = self._build_change_probe()

    def _build_project_manifest(self) -> Any:
        assert self._project_manifest_config is not None
        from okg.substrate.library.sources.scholarly.latex_project_manifest import (
            LatexProjectManifestBuilder,
        )

        kwargs = dict(self._project_manifest_config)
        if self._contained_reader is not None:
            kwargs["contained_reader"] = self._contained_reader
        return LatexProjectManifestBuilder(
            source_root=self.repo_root,
            **kwargs,
        ).build()

    def _refresh_project_manifest(self) -> None:
        if self._project_manifest_config is not None:
            self.project_manifest = self._build_project_manifest()

    def _build_change_probe(self) -> Any:
        import sys

        from okg.substrate.library.sources.content_hash_probe import (
            ContentHashProbe,
        )

        def _items() -> list[tuple[str, Any]]:
            if self._contained_reader is None:
                from okg.substrate.library.sources._file_walk import (
                    ContainedFileReader,
                )

                with ContainedFileReader(self.repo_root) as reader:
                    self._contained_reader = reader
                    try:
                        return _items()
                    finally:
                        self._contained_reader = None
            # Automatic project roots and dependency closure are live inputs,
            # not constructor-time state. Rebuild before enumerating so a new
            # root/member changes the token and cannot be skipped forever.
            self._refresh_project_manifest()
            items = [
                (str(path), self._contained_probe_bytes(path))
                for path in self._walk_files()
            ]
            if self.project_manifest is not None:
                # The handoff cursor carries the exact canonical digest, which
                # can change through warnings/closure semantics even when the
                # currently selected file-byte set does not. Make that cursor
                # obligation part of the dynamic probe token itself.
                items.append(
                    (
                        "manifest::digest",
                        self.project_manifest.digest.encode("utf-8"),
                    )
                )
            # The deployment's extractors.yaml selects parsers/chunker/NER, so an
            # extractor-config edit changes emission -> fold it into the digest.
            extractors_yaml = Path(self.extractors_dir) / "extractors.yaml"
            if extractors_yaml.is_file():
                items.append((f"extractors::{extractors_yaml}", extractors_yaml))
            # Inline figure detection is driven entirely by the captioning:
            # block in extractors.yaml (already folded into the digest above),
            # so no separate sidecar files contribute to the change probe.
            return items

        return ContentHashProbe(
            content_items=_items,
            config={
                "repo": self.repo,
                "repo_root": str(self.repo_root),
                "extractors_dir": str(self.extractors_dir),
                "include_globs": list(self.include_globs),
                "exclude_globs": list(self.exclude_globs),
                "include_untracked": self.include_untracked,
                "include_ignored_paths": list(self.include_ignored_paths),
                "non_git_walk": self.non_git_walk,
                "max_bytes_per_file": self.max_bytes_per_file,
                "mention_target_subtypes": sorted(self._mention_target_subtypes),
                # Emission depends on the citation config
                # (source-supplied-search-citations) — fold it into the
                # probe digest so a config change re-emits the corpus.
                "url": self.url or "",
                # A dated snapshot's date is stamped on every node, so
                # re-dating the same bytes is a real emission change. Without
                # it in the probe config the corpus-level no-change
                # short-circuit would keep the OLD date forever.
                "snapshot_date": self.snapshot_date or "",
                # Only when on, so turning it on changes the digest while
                # existing (off) deployments keep theirs.
                **({"text_fallback": True} if self.text_fallback else {}),
                "emission_schema_version": DOC_CORPUS_EMISSION_SCHEMA_VERSION,
                "latex_project_manifest_digest": getattr(
                    self.project_manifest,
                    "digest",
                    None,
                ),
            },
            emit_targets=[sys.modules[__name__]],
            max_bytes_per_item=self.max_bytes_per_file,
        )

    def _citation_fields(self, rel_str: str) -> dict[str, str]:
        """Source-supplied citation attrs for a repo-relative path.

        Always includes ``citation_label`` (``<repo>/<path>``);
        includes ``citation_url`` only when it can truthfully link the
        commit whose tree was read — a configured remote on a supported
        host, a known HEAD sha, and a file that is clean vs HEAD.
        Everything else degrades to a label-only citation, never a
        guessed link (source-supplied-search-citations). HEAD sha and
        dirty-paths are resolved once per source instance.
        """
        from okg.substrate.library.sources._source_urls import (
            citation_label,
            dirty_paths,
            head_commit_sha,
            repo_file_url,
        )

        label = citation_label(self.repo, rel_str)
        if not label:
            return {}
        fields = {"citation_label": label}
        if not self.url:
            return fields
        if not hasattr(self, "_citation_head_sha"):
            self._citation_head_sha = head_commit_sha(self.repo_root)
            self._citation_dirty = dirty_paths(self.repo_root)
        if (
            self._citation_head_sha
            and self._citation_dirty is not None
            and rel_str not in self._citation_dirty
        ):
            url = repo_file_url(
                self.url,
                self._citation_head_sha,
                rel_str,
            )
            if url:
                fields["citation_url"] = url
        return fields

    @property
    def probe_short_circuit_safe(self) -> bool:
        """Whether the runner may short-circuit this source on a changed=false
        change-probe verdict.

        False whenever this source can emit cross-source `mentions` edges,
        whose target currency the content-hash probe (which hashes only our own
        files) cannot observe — a target retracted by another source between
        runs leaves a stale edge, so the runner must let `run()` reach its own
        freshness gate (`_prior_record_set_external_targets_current`) instead of
        skipping. Alias resolution is the only path that emits those edges and
        runs exactly when `dsn` is set (see `_resolve_aliases_batch`);
        `mention_target_subtypes` merely NARROWS which resolved aliases are kept
        (empty = keep ALL), so it is NOT the right trigger. With no `dsn` the
        probe fully covers this source and the #323 short-circuit is preserved.
        """
        # Source-owned cursor and git-frontier fast paths remain available,
        # but this adapter does not yet bind emission to one immutable
        # captured byte snapshot for runner-level acknowledgement.
        return False

    # ---------- file walk ----------

    def _contained_read_bytes(self, path: Path) -> bytes:
        if self._contained_reader is None:
            raise FileContainmentError("DocCorpus secure reader is not active")
        _stat, content, _rel = self._contained_reader.read_bytes(
            path, max_bytes=self.max_bytes_per_file,
        )
        return content

    def _contained_probe_bytes(self, path: Path) -> bytes:
        """The change-probe item for one file.

        A file over ``max_bytes_per_file`` is not loaded: its item is a token
        naming its size and streamed SHA-256, so the probe still sees any
        change to it and the run is not stopped by the read limit.
        """
        if self._contained_reader is None:
            raise FileContainmentError("DocCorpus secure reader is not active")
        st = self._contained_reader.stat(path)
        if st.st_size <= self.max_bytes_per_file:
            return self._contained_read_bytes(path)
        st, digest, _rel = self._contained_reader.sha256(path)
        return f"okg-doc-corpus-oversize:{st.st_size}:{digest}".encode("ascii")

    def _contained_is_file(self, path: Path) -> bool:
        if self._contained_reader is None:
            raise FileContainmentError("DocCorpus secure reader is not active")
        try:
            self._contained_reader.stat(path)
        except FileNotFoundError:
            return False
        return True

    def _walk_files(
        self,
        scope_keys: Sequence[str] | None = None,
    ) -> list[Path]:
        """Honor include/exclude globs. Refuse loudly above
        `max_files`. Patterns use `pathlib.PurePath.match`
        semantics — `**` is recursive; bare `*` is single-
        segment.

        Per `shrink-source-walk-cost`: the unscoped path uses
        `os.walk` to enumerate filenames without calling
        `is_file()` per entry. The prior `rglob('*')` shape
        stat'd every entry (file or dir) in the tree just to
        filter — measured 105+ ms on palimpzest scale across
        the source threads.
        """
        import os

        out: list[Path] = []
        root = self.repo_root.resolve()
        root_str = str(root)
        if self.project_manifest is not None:
            selected = {item.path for item in self.project_manifest.source_inventory}
            selected.update(
                item.path
                for project in self.project_manifest.projects
                for item in project.files
            )
            if scope_keys:
                scoped_keys = {
                    str(key).replace("\\", "/").lstrip("/") for key in scope_keys
                }
                selected.intersection_update(scoped_keys)
            paths = [root / rel for rel in sorted(selected)]
            out = [path for path in paths if self._contained_is_file(path)]
            if len(out) > self.max_files:
                raise self._oversize(len(out))
            return out
        enumeration = self._resolve_file_enumeration()
        git_candidates = (
            self._git_candidate_paths() if enumeration == "git" else None
        )
        if scope_keys:
            # Scoped path: small set of candidates, per-path
            # resolve is acceptable.
            candidates: list[Path] = []
            for key in scope_keys:
                rel_key = _normalize_scope_key(key)
                # A key that climbs out of repo_root names no corpus member;
                # it is out of scope, not a containment breach to fail on.
                if rel_key is None:
                    continue
                # A scope key never widens the candidate set: an ignored or
                # untracked path named by an event is still out of scope.
                if git_candidates is not None and rel_key not in git_candidates:
                    continue
                candidate = root / rel_key
                if not self._contained_is_file(candidate):
                    continue
                candidates.append(candidate)
            for path in candidates:
                rel = path.relative_to(root)
                if not _match_any(rel, self.include_globs):
                    continue
                if _match_any(rel, self.exclude_globs):
                    continue
                out.append(path)
                if len(out) > self.max_files:
                    raise self._oversize(len(out))
            return out

        if git_candidates is not None:
            for rel_str in sorted(git_candidates):
                rel = Path(rel_str)
                if not _match_any(rel, self.include_globs):
                    continue
                if _match_any(rel, self.exclude_globs):
                    continue
                full = root / rel_str
                # Git lists index entries whose worktree file is gone
                # (deleted but not staged) and gitlinks (directories); neither
                # is a readable document, exactly as the filesystem walk would
                # never have yielded them.
                if not os.path.isfile(full):
                    continue
                out.append(full)
                if len(out) > self.max_files:
                    raise self._oversize(len(out))
            return out

        # `non_git_walk` (explicit opt-in, git cannot see the root):
        # enumerate via os.walk so we don't `is_file()`-stat every entry.
        def inventory_error(error: OSError) -> None:
            raise error

        for dirpath, dirnames, filenames in os.walk(root_str, onerror=inventory_error):
            for fname in sorted(filenames):
                full = os.path.join(dirpath, fname)
                rel_str = os.path.relpath(full, root_str).replace("\\", "/")
                rel = Path(rel_str)
                if not _match_any(rel, self.include_globs):
                    continue
                if _match_any(rel, self.exclude_globs):
                    continue
                out.append(Path(full))
                if len(out) > self.max_files:
                    raise self._oversize(len(out))
        out.sort()
        return out

    def _resolve_file_enumeration(self) -> str:
        """Return ``"git"`` or ``"filesystem"``; refuse a root git cannot see.

        Git "sees" `repo_root` when it is inside a work tree and is not itself
        ignored. An ignored root (say a staging directory under an ignored
        path) would list nothing from `git ls-files`, which would silently
        retract the whole corpus, so it is treated like a non-git root.
        """
        from okg.substrate.library.sources._git_helpers import (
            GitInvocationError,
            run_git,
            run_git_stdin,
        )

        root = self.repo_root
        reason: str
        try:
            inside = run_git(root, ["rev-parse", "--is-inside-work-tree"]).strip()
        except GitInvocationError:
            inside = "false"
        if inside != "true":
            reason = "repo_root is not inside a git work tree"
        else:
            returncode, _out, stderr = run_git_stdin(
                root, ["check-ignore", "-q", "--", "."], stdin=b"", check=False,
            )
            if returncode == 1:
                self._file_enumeration = "git"
                return "git"
            if returncode != 0:
                raise GitInvocationError(
                    ["check-ignore", "-q", "--", "."], returncode, stderr,
                )
            reason = "repo_root is ignored by its enclosing git work tree"
        if self.non_git_walk:
            self._file_enumeration = "filesystem"
            return "filesystem"
        raise DocCorpusNotGitWorkTree(
            f"DocCorpusSource: {reason}; refusing to walk it without "
            "`non_git_walk: true`",
            data={"repo_root": str(root), "reason": reason},
            fix_hint=(
                "point repo_root at a git checkout, or set `non_git_walk: true` "
                "in the source params to read every file the include/exclude "
                "globs match (no ignore rules apply)"
            ),
        )

    def _git_candidate_paths(self) -> set[str]:
        """Repo-root-relative paths git offers as corpus candidates.

        Paths come from `-z` output as bytes and are decoded with
        `os.fsdecode`, the same decoding `os.walk` uses, so a name that is
        not valid UTF-8 still opens.
        """
        from okg.substrate.library.sources._git_helpers import run_git_bytes

        root = self.repo_root

        def listed(args: list[str]) -> set[str]:
            raw = run_git_bytes(root, args)
            return {os.fsdecode(item) for item in raw.split(b"\0") if item}

        # `-C repo_root` makes ls-files report paths relative to repo_root even
        # when it is a subdirectory of the work tree, and limit output to it.
        args = ["ls-files", "-z", "--cached"]
        if self.include_untracked:
            args += ["--others", "--exclude-standard"]
        paths = listed(args)
        if self.include_ignored_paths:
            paths.update(listed([
                "ls-files", "-z", "--others", "--ignored", "--exclude-standard",
                "--",
                *(f":(glob){pattern}" for pattern in self.include_ignored_paths),
            ]))
        return {path.replace("\\", "/") for path in paths}

    def _note_oversize(self) -> None:
        recorded = self._oversize_recorded
        if recorded:
            log.warning(
                "doc_corpus %s: recorded %d file(s) larger than "
                "max_bytes_per_file=%d as oversize documents without "
                "reading them: %s",
                self.repo,
                len(recorded),
                self.max_bytes_per_file,
                ", ".join(sorted(recorded)[:10]),
            )

    def _enumeration_stats(self) -> dict[str, Any]:
        # Called once at the end of every run that walked the files, so the
        # oversize warning is logged exactly once per run.
        self._note_oversize()
        recorded = self._oversize_recorded
        return {
            "file_enumeration": (
                "project_manifest"
                if self.project_manifest is not None
                else self._file_enumeration
            ),
            "files_recorded_oversize": len(recorded),
            "files_recorded_oversize_sample": sorted(recorded)[:20],
        }

    def _oversize(self, discovered: int) -> "DocCorpusOversize":
        return DocCorpusOversize(
            f"DocCorpusSource: corpus exceeds max_files={self.max_files}",
            data={
                "discovered_so_far": discovered,
                "max_files": self.max_files,
                "repo_root": str(self.repo_root),
            },
            fix_hint=(
                "tighten `exclude_globs` or raise "
                "`max_files` if the corpus genuinely "
                "needs to grow"
            ),
        )

    def _format_for(
        self,
        path: Path,
        content: bytes,
    ) -> Optional[str]:
        """Resolve format by extension, then magic bytes, then (opt-in)
        shebang / plain-text fallback."""
        for pattern, fmt in self._name_routes:
            if fnmatch.fnmatchcase(path.name, pattern):
                return fmt
        ext = path.suffix.lower()
        if ext in self._extension_map:
            return self._extension_map[ext]
        sniffed = sniff_format(content[:32])
        if sniffed is not None or not self.text_fallback:
            return sniffed
        if not _looks_like_text(content):
            return None
        return "code" if _python_shebang(content) else "text"

    def _canonicalize(self, raw_id: str) -> str:
        """Run a candidate node_id through the registered
        canonicalizer chain. No-op when no canonicalizers are
        registered."""
        if not self._canonicalizers:
            return raw_id
        from okg.substrate.canonicalize import canonicalize_id

        return canonicalize_id(raw_id, self._canonicalizers)

    # ---------- alias resolver ----------

    def _resolve_aliases_batch(
        self,
        values: set[str],
        *,
        threshold: float = 0.85,
    ) -> dict[str, Optional[str]]:
        """Per-run alias lookup. Returns a dict mapping each
        `entity_value` to its canonical form (None if no match
        above the similarity threshold). Issues one bulk
        SELECT against `okg.fuzzy_resolve_alias`; misses on a
        no-active-cv or empty-aliases deployment return all
        Nones silently.

        Per Stride A.1 (2026-04-26): wires the W5 gap. The
        `mentions` cross-module bridge in
        `extraction_to_git_files.yaml` is dormant until this
        emit runs.
        """
        if not values or not self.dsn:
            return {v: None for v in values}
        out: dict[str, Optional[str]] = {v: None for v in values}
        # Postgres fuzzystrmatch Levenshtein functions reject
        # arguments longer than 255 chars. Very long extracted
        # values are usually noisy spans; leave them unresolved
        # so one bad mention cannot poison the whole batch.
        resolver_values = {v for v in values if len(v) <= 255}
        if not resolver_values:
            return out
        try:
            # Use the process-shared pool to avoid a fresh ~10 ms
            # TCP+TLS handshake on every alias-resolve call.
            from okg.substrate.db.pool import GENERAL, pooled_connection

            with pooled_connection(self.dsn, purpose=GENERAL) as conn, \
                    conn.cursor() as cur:
                cur.execute(
                    "SELECT max(catalog_version_id) FROM okg.catalog_versions "
                    " WHERE status = 'applied'"
                )
                row = cur.fetchone()
                if row is None or row[0] is None:
                    return out
                cv = row[0]
                # One round-trip per value via LATERAL.
                cur.execute(
                    """
                    SELECT v.value, r.canonical
                      FROM unnest(%s::text[]) AS v(value)
                      LEFT JOIN LATERAL (
                        SELECT canonical
                          FROM okg.fuzzy_resolve_alias(
                            v.value, %s::bigint, %s::real, 1
                          )
                         ORDER BY similarity DESC
                         LIMIT 1
                      ) r ON true
                    """,
                    (sorted(resolver_values), cv, float(threshold)),
                )
                resolved = {
                    str(value): str(canonical)
                    for value, canonical in cur.fetchall()
                    if canonical
                }
                subtype_by_id = self._canonical_subtypes(
                    conn,
                    sorted(set(resolved.values())),
                )
                resolved = {
                    value: canonical
                    for value, canonical in resolved.items()
                    if subtype_by_id.get(canonical)
                    and (
                        not self._mention_target_subtypes
                        or subtype_by_id[canonical] in self._mention_target_subtypes
                    )
                }
                out.update(resolved)
        except psycopg.Error as e:
            log.warning(
                "DocCorpusSource: alias resolver query failed "
                "(%s); emitting no `mentions` edges for this "
                "run",
                e,
            )
        return out

    @staticmethod
    def _canonical_subtypes(
        conn: psycopg.Connection,
        canonical_ids: Sequence[str],
    ) -> dict[str, str]:
        if not canonical_ids:
            return {}
        from okg.substrate.graph_store import ChronosGraphStore

        graph = ChronosGraphStore.from_connection(
            conn,
            ensure_schema=False,
            autocommit=False,
        ).checkout()
        out: dict[str, str] = {}
        chunk_size = 1000
        for idx in range(0, len(canonical_ids), chunk_size):
            chunk = list(canonical_ids[idx : idx + chunk_size])
            rows = graph.query(
                """
                SELECT node_id, subtype
                  FROM okg.graph_nodes
                 WHERE node_id = ANY(:node_ids)
                """,
                {"node_ids": chunk},
            )
            out.update({str(row["node_id"]): str(row["subtype"]) for row in rows})
        return out

    def _available_source_file_ids(
        self,
        source_file_ids: Sequence[str],
    ) -> set[str]:
        """Return the `source_file` endpoints the graph holds right now.

        Chronos writes keep exactly what the source emits, so doc-corpus
        asserts `document derived_from source_file` only toward an endpoint
        that exists; a dangling edge blocks the transform-consistency gate.
        Which source wrote the file node does not matter (issue #2688), so
        this reads the graph whether or not `predecessor_source` is set.
        Without a dsn no endpoint is visible and no edge is asserted.
        """
        if not self.dsn:
            return set()
        ids = sorted(set(source_file_ids))
        if not ids:
            return set()
        from okg.substrate.db.pool import GENERAL, pooled_connection
        from okg.substrate.graph_store import ChronosGraphStore

        available: set[str] = set()
        with pooled_connection(self.dsn, purpose=GENERAL) as conn:
            graph = ChronosGraphStore.from_connection(
                conn,
                ensure_schema=False,
                autocommit=False,
            ).checkout()
            chunk_size = 1000
            for idx in range(0, len(ids), chunk_size):
                chunk = ids[idx : idx + chunk_size]
                rows = graph.query(
                    """
                    SELECT node_id
                      FROM okg.graph_nodes
                     WHERE subtype = 'source_file'
                       AND node_id = ANY(:node_ids)
                    """,
                    {"node_ids": chunk},
                )
                available.update(str(row["node_id"]) for row in rows)
        return available

    def _linked_paths(self, rel_paths: Sequence[str]) -> set[str]:
        """The corpus paths whose `file:` node exists, so whose document
        carries the `derived_from` link."""
        ids = {
            rel: self._canonicalize(f"file:{self.repo}:{rel}") for rel in rel_paths
        }
        available = self._available_source_file_ids(tuple(ids.values()))
        return {rel for rel, file_id in ids.items() if file_id in available}

    def _source_file_links_current(
        self,
        cursor: Optional[dict[str, Any]],
        rel_paths: Sequence[str],
    ) -> bool:
        """Whether the prior run's links still match the graph's file nodes.

        The unchanged-corpus short-circuits reuse the prior record set, whose
        links were decided against the file nodes of that run. A file node
        that appeared or disappeared since then changes no corpus byte, so
        compare the cursor's recorded link set with the graph's current one:
        one indexed lookup per 1,000 paths and no parse. An unknown prior set
        (see `_prior_source_file_links`) declines the short-circuit once.
        """
        prior = self._prior_source_file_links(cursor)
        return prior is not None and prior == self._linked_paths(rel_paths)

    def _prior_source_file_links(
        self,
        cursor: Optional[dict[str, Any]],
    ) -> set[str] | None:
        """The paths the prior run linked, or None when that is unknown.

        A cursor written before #2688 has no record. Without a predecessor
        that code never wrote the link, so the prior set is empty; with one it
        may have linked any path, so the set is unknown.
        """
        prior = cursor.get(SOURCE_FILE_LINKS_KEY) if isinstance(cursor, dict) else None
        if isinstance(prior, list):
            return set(prior)
        return None if self._predecessors else set()

    @staticmethod
    def _record_has_derived_from(
        record: RecordEmission,
        *,
        doc_id: str,
        source_file_id: str,
    ) -> bool:
        for edge in record.edges_by_identity().values():
            if (
                edge.src == doc_id
                and edge.edge_type == "derived_from"
                and edge.dst == source_file_id
            ):
                return True
        return False

    def _current_node_subtypes(
        self,
        node_ids: Sequence[str],
    ) -> dict[str, str] | None:
        unique = sorted({str(node_id) for node_id in node_ids if node_id})
        if not unique:
            return {}
        if not self.dsn:
            return None
        try:
            from okg.substrate.db.pool import GENERAL, pooled_connection

            with pooled_connection(self.dsn, purpose=GENERAL) as conn:
                return self._canonical_subtypes(conn, unique)
        except psycopg.Error as e:
            log.warning(
                "DocCorpusSource: current-node lookup failed "
                "(%s); reprocessing unchanged records with external "
                "edges instead of copying prior record-set entries",
                e,
            )
            return None

    def _record_external_targets_current(
        self,
        record: RecordEmission,
        *,
        known_doc_ids: set[str],
        mention_target_subtypes_by_id: dict[str, str] | None,
    ) -> bool:
        """Return whether copied cross-record/external edge targets are current.

        Source-owned document/chunk/mention edges are covered by the record-set
        diff and coverage checks. Edges to external graph nodes or other corpus
        records need a source-side freshness check before we can safely reuse an
        unchanged prior record.
        """
        for edge in record.edges_by_identity().values():
            if edge.edge_type == "references":
                if edge.dst not in known_doc_ids:
                    return False
                continue
            if edge.edge_type != "mentions":
                continue
            if edge.dst in known_doc_ids:
                if (
                    self._mention_target_subtypes
                    and "document" not in self._mention_target_subtypes
                ):
                    return False
                continue
            if mention_target_subtypes_by_id is None:
                return False
            subtype = mention_target_subtypes_by_id.get(edge.dst)
            if not subtype:
                return False
            if (
                self._mention_target_subtypes
                and subtype not in self._mention_target_subtypes
            ):
                return False
        return True

    def _prior_record_set_external_targets_current(
        self,
        cursor: Optional[dict[str, Any]],
        files: Sequence[Path],
    ) -> bool:
        """Return whether reusing the prior inline record_set verbatim is safe.

        The `full_unchanged_noop` short-circuit reuses the prior record_set
        without re-running the parse/extract pipeline. That is only safe when
        every cross-record / external edge target the prior record_set copied
        is still current. A retracted `mentions` target (or a now-missing
        `references` doc) on a file whose CONTENT is unchanged is invisible to
        the content-hash probe, so we re-check those targets here. Returns
        False only when we can positively determine a copied external target
        is stale — i.e. we must fall through to full reprocessing so the stale
        edge gets retracted. When the prior record_set is external-by-reference
        (not materialized inline) or empty, the caller's existing non-short-
        circuit path handles freshness, so this returns True.
        """
        if not isinstance(cursor, dict):
            return True
        prior_rs_raw = cursor.get("record_set")
        if prior_rs_raw is None:
            # External-by-reference (or absent) inline record_set: nothing to
            # re-check here; the non-short-circuit path owns ref-mode freshness.
            return True
        try:
            prior_rs_records = RecordSet.from_cursor(prior_rs_raw).records
        except (ValueError, TypeError):
            # Corrupted prior record_set: don't take the fast path; let the
            # full path rebuild a clean record_set.
            return False
        if not prior_rs_records:
            return True

        known_doc_paths: set[str] = {
            str(p.relative_to(self.repo_root)).replace("\\", "/") for p in files
        }
        known_doc_ids: set[str] = {
            self._canonicalize(f"doc:{self.repo}:{rel_str}")
            for rel_str in known_doc_paths
        }
        prior_mention_targets = sorted(
            {
                edge.dst
                for record in prior_rs_records.values()
                for edge in record.edges_by_identity().values()
                if edge.edge_type == "mentions" and edge.dst not in known_doc_ids
            }
        )
        prior_mention_target_subtypes = self._current_node_subtypes(
            prior_mention_targets,
        )
        for record in prior_rs_records.values():
            if not self._record_external_targets_current(
                record,
                known_doc_ids=known_doc_ids,
                mention_target_subtypes_by_id=prior_mention_target_subtypes,
            ):
                return False
        return True

    # ---------- predecessor check ----------

    def _assert_predecessor_ran(self) -> None:
        if not self._predecessors:
            return
        if not self.dsn:
            raise SubstrateError(
                "DocCorpusSource needs `dsn` when "
                "`predecessor_source` is configured (the "
                "check queries okg.source_watermarks)",
                data={"predecessors": list(self._predecessors)},
            )
        missing: list[str] = []
        # Use the process-shared ConnectionPool instead of
        # `psycopg.connect()`. Saves ~10 ms TCP+TLS handshake per
        # DocCorpusSource invocation (observed on the per-source
        # flamegraph: `_assert_predecessor_ran` was 17 ms total,
        # of which 10 ms was `Connection.connect`). One indexed
        # SELECT per predecessor; usually 1-2 predecessors.
        from okg.substrate.db.pool import GENERAL, pooled_connection

        with pooled_connection(self.dsn, purpose=GENERAL) as conn, conn.cursor() as cur:
            for predecessor in self._predecessors:
                cur.execute(
                    "SELECT last_reconcile_at FROM okg.source_watermarks "
                    " WHERE source = %s",
                    (predecessor,),
                )
                row = cur.fetchone()
                if row is None or row[0] is None:
                    missing.append(predecessor)
        if missing:
            raise PredecessorNotRun(
                f"DocCorpusSource: predecessor(s) "
                f"{missing!r} have not reconciled; refusing "
                f"to emit `derived_from` edges to nonexistent "
                f"source_file nodes",
                data={
                    "predecessors": list(self._predecessors),
                    "missing": missing,
                },
                fix_hint=(
                    "run the missing predecessor(s) via "
                    "SourceRunner before this adapter, OR clear "
                    "`predecessor_source` if the dependency "
                    "isn't real for this deployment"
                ),
            )

    # ---------- main run ----------

    def run(
        self,
        run_id: str,
        *,
        mode: str = "cursor",
        cursor: Optional[dict[str, Any]] = None,
        sync_scope: Optional[dict[str, Any]] = None,
    ) -> SourceRun:
        from okg.substrate.library.sources._file_walk import ContainedFileReader

        if not Path(self.repo_root).is_dir():
            raise SourceRootMissing(
                f"doc_corpus source {self.name!r}: repo_root "
                f"{str(self.repo_root)!r} does not exist or is not a directory",
                data={
                    "source": self.name,
                    "setting": "repo_root",
                    "path": str(self.repo_root),
                },
                fix_hint=SourceRootMissing.FIX_HINT,
            )
        with ContainedFileReader(self.repo_root) as reader:
            self._contained_reader = reader
            try:
                return self._run_with_reader(
                    run_id, mode=mode, cursor=cursor, sync_scope=sync_scope,
                )
            finally:
                self._contained_reader = None

    def _run_with_reader(
        self,
        run_id: str,
        *,
        mode: str = "cursor",
        cursor: Optional[dict[str, Any]] = None,
        sync_scope: Optional[dict[str, Any]] = None,
    ) -> SourceRun:
        self._refresh_project_manifest()
        self._oversize_recorded = {}
        self._assert_predecessor_ran()

        # Per `first-class-visual-artifacts` (Task 3): loaded up front —
        # BEFORE the unchanged-corpus short-circuits below — so a
        # model/prompt/config change can be detected and defeat those
        # short-circuits. Reading it after the short-circuits (as Task
        # 2 did) would let an unchanged file skip re-emit its stale
        # caption forever, even though the vision_signature it should
        # produce has changed. See `_vision_gate_fingerprint`.
        captioning_cfg = _load_captioning_config(self.extractors_dir)
        caption_tracking_dsn = None
        if (
            captioning_cfg.enabled
            or captioning_cfg.tracking_dsn is not None
            or captioning_cfg.tracking_mode is not None
        ):
            caption_tracking_dsn = captioning_cfg.resolve_tracking(self.dsn)
        # Fail loud at ingest start (not first-PDF) if figure detection is
        # enabled but the vision extra is absent — per acc.misconfig-loud, so a
        # misconfigured deployment cannot start successfully and then either
        # fail deep in the walk or never surface the misconfiguration when no
        # PDF is processed. Skipped when a detector is injected (tests supply a
        # stub); find_spec does not import torch.
        if captioning_cfg.figure_detection and self.figure_detector is None:
            from okg.substrate.library.captioning.figure_detector import (
                require_doclayout_yolo,
            )

            require_doclayout_yolo()
        # Fail loud at ingest start if figure detection is enabled but crops
        # cannot be staged: the source is now discovery-only (it emits no
        # captions), so the VisualCaptionEnricher (Phase B) reads each crop
        # from okg.visual_crop_blobs to caption it. Without a dsn to write that
        # store (and no injected crop store), every published figure would be
        # permanently uncaptionable — a silent-failure P0. This is orthogonal
        # to the doclayout guard above (a stubbed detector still needs a place
        # to stage crops), so it is NOT skipped when a detector is injected.
        if (
            captioning_cfg.figure_detection
            and self.figure_crop_store is None
            and not self.dsn
        ):
            raise SubstrateError(
                "DocCorpusSource: captioning.figure_detection is enabled but no "
                "source dsn is configured, so figure crops cannot be staged into "
                "okg.visual_crop_blobs for the caption enricher.",
                fix_hint="Configure the source dsn (the same DSN used for VLM "
                "call tracking), or inject a figure_crop_store; else set "
                "captioning.figure_detection=false.",
            )
        table_transcription_cfg = _load_table_transcription_config(self.extractors_dir)
        vision_fingerprint = (
            self._vision_gate_fingerprint(captioning_cfg)
            if captioning_cfg.enabled
            else None
        )
        prior_vision_fingerprint = (
            cursor.get("vision_config_fingerprint")
            if isinstance(cursor, dict)
            else None
        )
        # True when it is safe to trust a file-identity-only short-circuit:
        # either captioning is off, or the config that would produce a
        # caption hasn't changed since the cursor was written.
        vision_config_unchanged = (
            not captioning_cfg.enabled or vision_fingerprint == prior_vision_fingerprint
        )
        table_config_fingerprint = hashlib.sha256(
            json.dumps(
                table_transcription_cfg.public_mapping(),
                sort_keys=True,
                ensure_ascii=False,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()
        prior_table_config_fingerprint = (
            cursor.get("table_transcription_config_fingerprint")
            if isinstance(cursor, dict)
            else None
        )
        table_config_unchanged = (
            table_config_fingerprint == prior_table_config_fingerprint
        )
        emission_schema_unchanged = (
            isinstance(cursor, dict)
            and cursor.get("doc_corpus_emission_schema_version")
            == DOC_CORPUS_EMISSION_SCHEMA_VERSION
            and cursor.get("doc_corpus_reparse_epoch") == DOC_CORPUS_REPARSE_EPOCH
        )
        current_project_manifest_digest = getattr(
            self.project_manifest,
            "digest",
            None,
        )
        project_manifest_unchanged = (
            isinstance(cursor, dict)
            and cursor.get("latex_project_manifest_digest")
            == current_project_manifest_digest
        )
        # Re-dating a snapshot changes what every node says while changing no
        # file byte. Both no-change short-circuits and the per-file
        # record-set reuse below key on bytes, so without this the corpus
        # would keep asserting the PREVIOUS date — a silent fallback to a
        # stale observation, which is exactly the failure this vertical
        # exists to measure. A cursor written before this field existed
        # carries no key; `.get` returns None, which compares equal only for
        # an undated corpus, so an undated corpus is unaffected.
        snapshot_date_unchanged = (
            isinstance(cursor, dict)
            and cursor.get("doc_corpus_snapshot_date") == self.snapshot_date
        )
        # A file over the byte limit is recorded with an `oversize` warning
        # and no chunks. Raising (or lowering) the limit changes that outcome
        # while changing no file byte, so a changed limit defeats both
        # no-change short-circuits and the per-file record-set reuse. A cursor
        # written before this key existed counts as unchanged: runs then
        # stopped on any oversize file, so no oversize record can exist.
        max_bytes_unchanged = (
            not isinstance(cursor, dict)
            or cursor.get(_MAX_BYTES_CURSOR_KEY, self.max_bytes_per_file)
            == self.max_bytes_per_file
        )
        # Same for `text_fallback`: turning it on changes which files have
        # text while changing no file byte. A cursor written before this
        # field existed carries no key and means off.
        text_fallback_unchanged = (
            isinstance(cursor, dict)
            and bool(cursor.get("doc_corpus_text_fallback", False)) == self.text_fallback
        )
        # And for extractors.yaml: a new chunker or parser changes the chunks
        # of every file whose bytes did not change. A cursor written before
        # this field existed carries no fingerprint, which counts as changed:
        # there is no way to tell which config produced its chunks.
        extractors_fingerprint = _extractors_config_fingerprint(self.extractors_dir)
        extractors_config_unchanged = (
            isinstance(cursor, dict)
            and cursor.get("doc_corpus_extractors_fingerprint")
            == extractors_fingerprint
        )

        scope_keys = tuple(
            str(key).replace("\\", "/").lstrip("/")
            for key in (sync_scope or {}).get("scope_keys", ())
            if key
        )
        scoped = mode in {"scope_incomplete", "scope_complete"} and bool(
            scope_keys,
        )
        completed_scope = mode in {"reconcile", "scope_complete"}
        scoped_manifest_refresh_pending = (
            scoped
            and self.project_manifest is not None
            and not project_manifest_unchanged
        )
        scope_widened_for_project_manifest = (
            scoped_manifest_refresh_pending and completed_scope
        )
        if scope_widened_for_project_manifest:
            # A selected visual's content hash is part of the canonical visual
            # node id referenced by otherwise-unchanged LaTeX captions. A
            # completed-scope event for only the image therefore has global
            # manifest consequences: widen it to the full selected corpus so
            # the caption record is rebuilt and its stale EdgeKey retracts in
            # this run. Incomplete slices stay narrow and deliberately retain
            # the prior digest below, latching a required full reconcile.
            scoped = False
        current_git_frontier: dict[str, Any] | None = None
        if self.git_frontier_skip and completed_scope and not scoped:
            current_git_frontier = self._git_frontier()
            prior_git_frontier = (
                cursor.get("git_frontier") if isinstance(cursor, dict) else None
            )
            if (
                cursor_has_record_set_authority(cursor)
                and current_git_frontier is not None
                and current_git_frontier == prior_git_frontier
                and vision_config_unchanged
                and table_config_unchanged
                and emission_schema_unchanged
                and project_manifest_unchanged
                and snapshot_date_unchanged
                and max_bytes_unchanged
                and text_fallback_unchanged
                and extractors_config_unchanged
                # An unchanged git frontier proves OUR files didn't change, but
                # not that a cross-source `mentions` target is still current —
                # one retracted elsewhere leaves a stale edge this fast path
                # would otherwise strand. Re-check target currency (same gate
                # `full_unchanged_noop` uses) against the prior cursor's known
                # file set; a stale target declines the short-circuit and falls
                # through to the walk + reprocess path. Cheap when there are no
                # cross-source targets (returns True without a graph read).
                and self._prior_record_set_external_targets_current(
                    cursor,
                    [self.repo_root / rel for rel in (cursor.get("files") or {})],
                )
                # Same for the document-to-file link: a file node that
                # appeared or disappeared changes no corpus byte.
                and self._source_file_links_current(
                    cursor, list(cursor.get("files") or {}),
                )
            ):
                next_cursor = source_state_without_runner_keys(cursor)
                next_cursor["git_frontier"] = current_git_frontier
                next_cursor["table_transcription_config_fingerprint"] = (
                    table_config_fingerprint
                )
                if self.project_manifest is not None:
                    next_cursor["latex_project_manifest_digest"] = (
                        self.project_manifest.digest
                    )
                file_count = len(next_cursor.get("files") or {})
                self.last_run_stats = {
                    "scope_status": "git_frontier_no_change",
                    "run_mode": mode,
                    "scope_keys": [],
                    "files_seen": 0,
                    "files_changed": 0,
                    "files_skipped_unchanged": file_count,
                    "files_reused_prior_record_set": file_count,
                    "emitted_nodes": 0,
                    "emitted_edges": 0,
                    "record_set_authority": "preserved",
                    "tables_seen": 0,
                    "tables_parsed_by_lane": {},
                    "table_cache_hits": 0,
                    "table_cache_misses": 0,
                    "table_provider_invocations": 0,
                    "table_external_network_attempts": 0,
                    "table_failures_by_code": {},
                    "git_frontier_dirty_paths": len(
                        current_git_frontier.get("dirty_paths") or (),
                    ),
                    # No enumeration ran on this path, so there is no
                    # oversize count; None says "not measured", not zero.
                    "file_enumeration": None,
                    "files_recorded_oversize": None,
                    "files_recorded_oversize_sample": [],
                }
                return SourceRun(
                    facts=iter(()),
                    completed_scope=True,
                    next_cursor=preserve_cursor_record_set_authority(
                        next_cursor,
                        cursor,
                    ),
                    run_mode=mode,
                    record_set=None,
                    # The cursor above carries the prior footprint forward.
                    # Without this flag the runner infers a footprint from
                    # this run's empty stream and retracts the whole corpus
                    # (issue #2122).
                    preserves_prior_record_set=True,
                )
        files = self._walk_files(scope_keys if scoped else None)
        if completed_scope and not scoped and cursor_has_record_set_authority(cursor):
            from okg.substrate.library.sources._file_walk import FileWalk

            probe = FileWalk(
                prior_cursor=cursor,
                repo_root=self.repo_root,
                read_as="bytes",
                strict_io=True,
                contained_io=True,
                contained_reader=self._contained_reader,
                max_bytes=self.max_bytes_per_file,
                oversize="record",
            )
            probed = 0
            changed = False
            for event in probe.iter(files):
                probed += 1
                if event.oversize_bytes is not None:
                    self._oversize_recorded[event.rel_path] = event.oversize_bytes
                if event.is_changed:
                    changed = True
                    break
            if (
                not changed
                and probed == len(files)
                and set(probe.next_files) == set(probe.prior_files)
                and vision_config_unchanged
                and table_config_unchanged
                and emission_schema_unchanged
                and project_manifest_unchanged
                and snapshot_date_unchanged
                and max_bytes_unchanged
                and text_fallback_unchanged
                and extractors_config_unchanged
                and self._prior_record_set_external_targets_current(
                    cursor,
                    files,
                )
                and self._source_file_links_current(cursor, list(probe.next_files))
            ):
                next_cursor = probe.next_cursor(run_id=run_id)
                next_cursor[SOURCE_FILE_LINKS_KEY] = sorted(
                    self._prior_source_file_links(cursor) or ()
                )
                if current_git_frontier is not None:
                    next_cursor["git_frontier"] = current_git_frontier
                next_cursor["vision_config_fingerprint"] = vision_fingerprint
                next_cursor["table_transcription_config_fingerprint"] = (
                    table_config_fingerprint
                )
                next_cursor["doc_corpus_snapshot_date"] = self.snapshot_date
                next_cursor[_MAX_BYTES_CURSOR_KEY] = self.max_bytes_per_file
                next_cursor["doc_corpus_text_fallback"] = self.text_fallback
                next_cursor["doc_corpus_extractors_fingerprint"] = (
                    extractors_fingerprint
                )
                # Every key the reuse gates read must survive this branch, or
                # the run after a no-change run finds it missing and re-parses
                # every file (the schema version used to be dropped here).
                next_cursor["doc_corpus_emission_schema_version"] = (
                    DOC_CORPUS_EMISSION_SCHEMA_VERSION
                )
                next_cursor["doc_corpus_reparse_epoch"] = DOC_CORPUS_REPARSE_EPOCH
                if self.project_manifest is not None:
                    next_cursor["latex_project_manifest_digest"] = (
                        self.project_manifest.digest
                    )
                if isinstance(cursor, dict) and "visual_captions" in cursor:
                    next_cursor["visual_captions"] = cursor["visual_captions"]
                if isinstance(cursor, dict) and "table_transcription_cache" in cursor:
                    next_cursor["table_transcription_cache"] = cursor[
                        "table_transcription_cache"
                    ]
                self.last_run_stats = {
                    "scope_status": "full_unchanged_noop",
                    "run_mode": mode,
                    "scope_keys": [],
                    "files_seen": probed,
                    "files_changed": 0,
                    "files_skipped_unchanged": probed,
                    "files_reused_prior_record_set": probed,
                    "emitted_nodes": 0,
                    "emitted_edges": 0,
                    "record_set_authority": "preserved",
                    "tables_seen": 0,
                    "tables_parsed_by_lane": {},
                    "table_cache_hits": 0,
                    "table_cache_misses": 0,
                    "table_provider_invocations": 0,
                    "table_external_network_attempts": 0,
                    "table_failures_by_code": {},
                    **self._enumeration_stats(),
                }
                return SourceRun(
                    facts=iter(()),
                    completed_scope=True,
                    next_cursor=preserve_cursor_record_set_authority(
                        next_cursor,
                        cursor,
                    ),
                    run_mode=mode,
                    record_set=None,
                    # The cursor above carries the prior footprint forward.
                    # Without this flag the runner infers a footprint from
                    # this run's empty stream and retracts the whole corpus
                    # (issue #2122).
                    preserves_prior_record_set=True,
                )

        bundle = load_extractors(self.extractors_dir)
        # captioning_cfg / vision_fingerprint were loaded up front, above
        # the unchanged-corpus short-circuits.
        _caption_svc = self.caption_service
        if captioning_cfg.enabled and _caption_svc is None:
            from okg.substrate.library.captioning.service import CaptionService
            from okg.views.execution.llm import VLMClient

            _caption_svc = CaptionService(
                mode=captioning_cfg.mode,  # type: ignore[arg-type]
                vlm=VLMClient(
                    model=captioning_cfg.model,
                    tracking_dsn=caption_tracking_dsn,
                    tracking_mode=(
                        "optional" if caption_tracking_dsn else "disabled"
                    ),
                    runtime_dsn=None,
                    deployment=self.deployment or self.repo,
                ),
                model=captioning_cfg.model,
                coarse_prompt=captioning_cfg.prompt_for(self.name) or None,
            )
        prior_table_cache: dict[str, str] = {}
        if isinstance(cursor, dict):
            raw_table_cache = cursor.get("table_transcription_cache")
            if isinstance(raw_table_cache, dict):
                prior_table_cache = {
                    str(key): str(value)
                    for key, value in raw_table_cache.items()
                    if isinstance(key, str) and isinstance(value, str)
                }
        _table_cache = self.table_response_cache or MemoryTableResponseCache(
            values=dict(prior_table_cache)
        )
        _table_service: PdfTableTranscriptionService | None = None
        if table_transcription_cfg.enabled:
            locator = self.table_region_locator
            renderer = self.table_crop_renderer
            if locator is None or renderer is None:
                from okg.substrate.library.extraction.parse.pdf_tables import (
                    PdfPlumberTableCropRenderer,
                    PdfPlumberTableRegionLocator,
                )

                locator = locator or PdfPlumberTableRegionLocator()
                renderer = renderer or PdfPlumberTableCropRenderer()
            provider = build_table_transcription_provider(
                table_transcription_cfg,
                injected_provider=self.table_transcription_provider,
                tracking_dsn=self.dsn,
                deployment=self.deployment or self.repo,
            )
            _table_service = PdfTableTranscriptionService(
                config=table_transcription_cfg,
                locator=locator,
                renderer=renderer,
                provider=provider,
                cache=_table_cache,
                # Same identity/DSN the provider stamps on tracked llm_calls
                # rows, so ledger settlements and call telemetry agree on which
                # deployment spent the money.
                deployment=self.deployment or self.repo,
                settlement_dsn=self.dsn,
            )
        all_files = self._walk_files(None) if scoped else files
        selected_figure_paths = {
            item.path
            for project in (
                self.project_manifest.projects
                if self.project_manifest is not None
                else ()
            )
            for item in project.files
            if "figure" in item.roles
        }
        figure_graphics_by_source = self._figure_graphics_by_source()
        # Per `add-markdown-link-extraction` (2026-04-27):
        # pre-collect the set of repo-relative paths that will
        # become `document` nodes this run. The link resolver
        # uses this set to decide whether a markdown link
        # resolves to a known doc — links to paths NOT in this
        # set are skipped (no edge emitted).
        known_doc_paths: set[str] = {
            str(p.relative_to(self.repo_root)).replace("\\", "/") for p in all_files
        }
        known_doc_ids: set[str] = {
            self._canonicalize(f"doc:{self.repo}:{rel_str}")
            for rel_str in known_doc_paths
        }
        source_file_ids_by_rel: dict[str, str] = {
            rel_str: self._canonicalize(f"file:{self.repo}:{rel_str}")
            for rel_str in known_doc_paths
        }
        available_source_file_ids = self._available_source_file_ids(
            tuple(source_file_ids_by_rel.values()),
        )
        # Paths linked by the prior run; None when unknown.
        prior_links = self._prior_source_file_links(cursor)
        # Paths whose record this run asserts, and which of those it links.
        asserted_paths: set[str] = set()
        linked_paths: set[str] = set()
        # `source_revision` is a CONTENT revision, as the source registry
        # declares (`source_revision_kind: content_hash`). It must NOT carry a
        # run token. The Chronos writer's semantic no-op filter compares
        # `source_revision` along with the payload, so a per-run value makes
        # every rerun rewrite every doc-corpus row even when the corpus is
        # byte-identical.
        #
        # This base holds only inputs that genuinely change WHAT is emitted:
        # the emission schema, the vision and table extraction configs, and
        # the operator-pinned snapshot date (a config string, never derived
        # from the clock). `_record_revision` layers each record's own file
        # content sha on top, so editing one file changes the revision of
        # that file's records and no others.
        base_revision = {
            "doc_corpus_emission_schema_version": (DOC_CORPUS_EMISSION_SCHEMA_VERSION),
            "vision_config_fingerprint": vision_fingerprint,
            "table_transcription_config_fingerprint": (table_config_fingerprint),
            "snapshot_date": self.snapshot_date,
        }
        # Deliberately absent: the LaTeX project manifest digest. Manifest-
        # derived state reaches doc-corpus facts through IDENTITY only -- the
        # `visual_id` a `caption_of` edge points at -- and a changed identity
        # is a different fact, which no revision comparison can suppress. Add
        # the digest here the moment a manifest-derived ATTRIBUTE is emitted
        # on a `.tex`-owned fact, or that attribute's update will be dropped
        # as a no-op when the file bytes have not changed.
        #
        # Per-path content revision for every path this walk visits, reused by
        # the deferred mention pass and the record_set builder below.
        revision_by_path: dict[str, dict[str, Any]] = {}

        # Per `add-delta-proportional-source-runs`: read the prior
        # record_set from the cursor so unchanged-and-skipped files
        # can be re-asserted in the current record_set. Without
        # this, the runner's record_set diff would synthesize a
        # spurious retraction for every unchanged file (its
        # prior record was in prior_rs but not in current_rs).
        prior_rs_records: dict[str, RecordEmission] = {}
        if isinstance(cursor, dict):
            prior_rs_raw = cursor.get("record_set")
            if prior_rs_raw is not None:
                try:
                    prior_rs_records = dict(
                        RecordSet.from_cursor(prior_rs_raw).records,
                    )
                except (ValueError, TypeError):
                    # Corrupted prior record_set: degrade to full
                    # processing (no short-circuit). The next run
                    # writes a clean record_set.
                    prior_rs_records = {}
        external_record_set_authority = (
            isinstance(cursor, dict)
            and cursor.get("record_set_ref") is not None
            and cursor.get("record_set") is None
        )
        prior_mention_targets = sorted(
            {
                edge.dst
                for record in prior_rs_records.values()
                for edge in record.edges_by_identity().values()
                if edge.edge_type == "mentions" and edge.dst not in known_doc_ids
            }
        )
        prior_mention_target_subtypes = self._current_node_subtypes(
            prior_mention_targets,
        )

        # Per `first-class-visual-artifacts` (Task 3): per-visual caption
        # reuse cache. Keyed by `visual_id`, each entry is
        # `{"vision_signature": str, "chunks": [{"chunk_id", "text",
        # "attrs"}, ...]}` — the exact caption(s) last produced for that
        # visual at that signature. `_emit_caption_chunks` skips the VLM
        # call when the visual's freshly-computed signature matches the
        # cached one, re-emitting the cached chunk(s) instead. Carried
        # across runs via `cursor["visual_captions"]`; stale entries for
        # visuals not seen this run are harmless (never read) and are
        # left for a future retraction pass to prune, not this task.
        prior_visual_captions: dict[str, dict[str, Any]] = {}
        if isinstance(cursor, dict):
            raw_visual_captions = cursor.get("visual_captions")
            if isinstance(raw_visual_captions, dict):
                prior_visual_captions = raw_visual_captions
        new_visual_captions: dict[str, dict[str, Any]] = {}

        # Per `first-class-visual-artifacts` (Task 3): when captioning is
        # enabled and the vision config (model/prompt/mode/limits) changed
        # since the cursor was written, an unchanged file must NOT take
        # the per-file "unchanged, skip" branch below — its visuals need
        # their signatures recomputed against the new config, which can
        # only happen by re-running `_emit_caption_chunks`. Content
        # identity is unaffected, so this does not change non-visual
        # chunks; it only defeats the skip.
        force_full_reprocess = (
            not emission_schema_unchanged
            or not project_manifest_unchanged
            or (captioning_cfg.enabled and not vision_config_unchanged)
            or not table_config_unchanged
            # Same file bytes, new stated date: the per-file skip would copy
            # the prior record set forward and the graph would keep the old
            # snapshot_date. Re-parse instead.
            or not snapshot_date_unchanged
            # A changed byte limit changes which files are recorded as
            # oversize; re-parse so none keeps the other limit's outcome.
            or not max_bytes_unchanged
            # Files skipped (no_format) or left empty (parse_failed) under
            # the old setting have unchanged bytes; re-read them.
            or not text_fallback_unchanged
            # A new chunker or parser: unchanged bytes, different chunks.
            or not extractors_config_unchanged
        )

        # Per `add-source-record-set-reconciliation`: this source
        # still tracks the per-path emission shape locally, but
        # it now returns a `RecordSet` and lets SourceRunner diff
        # it against the prior cursor. This keeps retraction
        # policy, provenance, dedupe, and COPY in one substrate
        # layer instead of keeping bespoke source-side diff logic.
        next_emissions: dict[str, dict[str, Any]] = {}

        # Materialize all NodeFacts/EdgeFacts in memory so the
        # SourceRunner's dedup pass collapses identical chunks
        # across files (same `chunk:<sha256>` lands once).
        emitted_facts: list[Any] = []
        # (rel_path, [(mention_id, chunk_id, etype, evalue,
        #              attrs), ...]) per file. Mentions are
        # emitted post-walk so the alias resolver can run one
        # batched query (Stride A.1 W5).
        all_mentions: list[tuple[str, list[tuple[str, str, str, str, dict]]]] = []

        def _emit(fact: Any) -> None:
            emitted_facts.append(fact)

        observed_at = datetime.now(timezone.utc)

        # Per `add-delta-proportional-source-runs`: delegate the
        # walk + read + hash + skip-on-match decision to the
        # substrate's shared `FileWalk` helper. Per-file parsing
        # + chunking + extraction now only runs for changed files.
        from okg.substrate.library.sources._file_walk import FileWalk

        walk = FileWalk(
            prior_cursor=cursor,
            repo_root=self.repo_root,
            read_as="bytes",
            strict_io=True,
            contained_io=True,
            contained_reader=self._contained_reader,
            max_bytes=self.max_bytes_per_file,
            # A file over max_bytes_per_file is recorded as a Document with
            # an `oversize` warning and the run continues (operator decision
            # 2026-09-22; fuller design in #2323). Its bytes are never loaded.
            oversize="record",
        )
        # Paths that were unchanged AND for which we have a prior
        # record_set entry to copy. Anything else (unchanged but
        # no prior entry, or changed) goes through next_emissions
        # / current_rs the normal way.
        unchanged_with_prior_rs: set[str] = set()
        walked_scope_keys: set[str] = set()
        changed_scope_keys: set[str] = set()
        changed_record_keys: set[str] = set()
        files_seen = 0
        files_changed = 0
        files_skipped_unchanged = 0
        files_reused_prior_rs = 0
        files_processed_unchanged = 0
        table_stats: dict[str, Any] = {
            "tables_seen": 0,
            "tables_parsed_by_lane": {},
            "table_cache_hits": 0,
            "table_cache_misses": 0,
            "table_provider_invocations": 0,
            "table_external_network_attempts": 0,
            "table_failures_by_code": {},
        }
        pending_caption_calls: list[_PendingCaptionCall] = []
        caption_stats: dict[str, Any] = {
            "calls": 0,
            "rate_limit_hits": 0,
            "failures_by_kind": {},
            "outcomes": {},
            "defer_emit": True,
        }

        def _record_table_outcome(attrs: Mapping[str, Any]) -> None:
            status = attrs.get("table_grid_status")
            if not isinstance(status, str):
                return
            table_stats["tables_seen"] += 1
            if status == "parsed":
                lane = str(attrs.get("table_extraction_lane") or "unknown")
                parsed_by_lane = table_stats["tables_parsed_by_lane"]
                parsed_by_lane[lane] = parsed_by_lane.get(lane, 0) + 1
                return
            error_code = str(attrs.get("table_grid_error_code") or "unknown")
            failures = table_stats["table_failures_by_code"]
            failures[error_code] = failures.get(error_code, 0) + 1

        caption_incomplete = False
        caption_outcomes_by_rel: dict[str, _CaptionCallOutcome] = {}
        if captioning_cfg.enabled and _caption_svc is not None:
            # Read only files that the normal walk will process. The pre-pass
            # plans/renders standalone PDFs without emitting facts, so the
            # publish loop below can still emit each file's chunks immediately
            # after its deterministic result is available.
            planning_walk = FileWalk(
                prior_cursor=cursor,
                repo_root=self.repo_root,
                read_as="bytes",
                strict_io=True,
                contained_io=True,
                contained_reader=self._contained_reader,
                max_bytes=self.max_bytes_per_file,
                oversize="record",
            )
            for event in planning_walk.iter(files):
                if not event.is_changed and not force_full_reprocess:
                    continue
                if event.oversize_bytes is not None:
                    # Never rendered or captioned; the main loop records it
                    # with caption_skip_reason `source_too_large`.
                    continue
                rel_str = event.rel_path
                if rel_str not in selected_figure_paths:
                    continue
                content = event.content
                if not isinstance(content, bytes):
                    content = self._contained_read_bytes(self.repo_root / rel_str)
                if not _has_pdf_magic(content):
                    continue
                self._emit_image_caption_chunks(
                    content=content,
                    doc_id=self._canonicalize(f"doc:{self.repo}:{rel_str}"),
                    rel_str=rel_str,
                    revision=self._record_revision(
                        base_revision, event.content_hash,
                    ),
                    emit=lambda _fact: None,
                    caption_svc=_caption_svc,
                    captioning_cfg=captioning_cfg,
                    chunks_this_doc=[],
                    visual_descendants_this_doc=[],
                    visual_edges_this_doc=[],
                    source_file_id=None,
                    caption_cache=prior_visual_captions,
                    caption_cache_out={},
                    caption_batch=pending_caption_calls,
                    caption_stats=caption_stats,
                )
            if pending_caption_calls:
                from okg.substrate.llm.fanout import bounded_map

                outcomes = bounded_map(
                    pending_caption_calls,
                    lambda pending: DocCorpusSource.run_caption_call_outcome(
                        pending.caption_svc,
                        pending.planned,
                        image_label=pending.image_label,
                    ),
                )
                for pending, result in zip(pending_caption_calls, outcomes):
                    outcome = (
                        result.value
                        if result.error is None
                        and isinstance(result.value, _CaptionCallOutcome)
                        else _CaptionCallOutcome(failure_kind="provider_error")
                    )
                    if outcome.failure_kind in {
                        "truncated",
                        "rate_limited",
                        "provider_error",
                        "empty_result",
                    }:
                        caption_incomplete = True
                    pending.finish(outcome)
                caption_outcomes_by_rel = dict(caption_stats["outcomes"])
                caption_stats["defer_emit"] = False

        for event in walk.iter(files):
            files_seen += 1
            rel_str = event.rel_path
            if scoped:
                walked_scope_keys.add(rel_str)
            # `event.content` is None when the mtime fast-path
            # fired (per `shrink-source-walk-cost`). For unchanged
            # files we never use `content` / `byte_count` — the
            # skip branches below `continue` before they would be
            # read. Only the changed branch needs the bytes.
            content_sha = event.content_hash
            # This file's revision: the base plus the file's own content sha.
            # It is the revision for every fact emitted below whose identity
            # names `rel_str` -- the document node, every edge, and the
            # mention / table-occurrence / code-symbol / code-import /
            # code-reference nodes -- so revision and record identity agree.
            #
            # The two content-addressed node kinds are the exception and
            # override it with a sha of their own content: non-table
            # `document_chunk` nodes (see the chunk loop below) and
            # `visual_artifact` nodes (see `_emit_caption_chunks`). Their ids
            # do not name `rel_str`, so keying them on this sha would stage
            # one shared paragraph or image once per owning file.
            revision = self._record_revision(base_revision, content_sha)
            revision_by_path[rel_str] = revision
            path = self.repo_root / rel_str

            # Two paths for skipping unchanged files:
            #
            # 1. `scope_incomplete` / `cursor` mode: record_set
            #    is NOT emitted (completed_scope is False). The
            #    runner doesn't synthesize retractions from
            #    "missing from emit set," so unconditionally
            #    skipping an unchanged file is correct.
            #
            # 2. `scope_complete` / `reconcile` mode: record_set
            #    IS emitted. Skipping a file means we must copy
            #    the prior record_set's entry for that path into
            #    the current record_set; otherwise the runner's
            #    diff would synthesize a spurious retraction.
            #    If the prior record_set lacks this path's
            #    entry (e.g. first scope_complete run after a
            #    deploy), we can't skip — fall through and
            #    process so the entry gets built.
            oversize_bytes = event.oversize_bytes
            if not event.is_changed and not force_full_reprocess:
                completed_scope_run = mode in {"reconcile", "scope_complete"}
                if not completed_scope_run:
                    files_skipped_unchanged += 1
                    continue
                should_have_derived_from = (
                    source_file_ids_by_rel[rel_str] in available_source_file_ids
                )
                if rel_str in prior_rs_records:
                    doc_id = self._canonicalize(f"doc:{self.repo}:{rel_str}")
                    prior_has_derived_from = self._record_has_derived_from(
                        prior_rs_records[rel_str],
                        doc_id=doc_id,
                        source_file_id=source_file_ids_by_rel[rel_str],
                    )
                    external_targets_current = self._record_external_targets_current(
                        prior_rs_records[rel_str],
                        known_doc_ids=known_doc_ids,
                        mention_target_subtypes_by_id=(prior_mention_target_subtypes),
                    )
                    if (
                        should_have_derived_from == prior_has_derived_from
                        and external_targets_current
                    ):
                        files_reused_prior_rs += 1
                        unchanged_with_prior_rs.add(rel_str)
                        asserted_paths.add(rel_str)
                        if should_have_derived_from:
                            linked_paths.add(rel_str)
                        continue
                if external_record_set_authority:
                    if prior_links is not None and (
                        (rel_str in prior_links) == should_have_derived_from
                    ):
                        files_reused_prior_rs += 1
                        continue
                    # The link changed (or its prior state is unknown): the
                    # record kept by reference is stale, so rebuild it and
                    # name it as a replacement key.
                    changed_record_keys.add(rel_str)
                    if scoped:
                        changed_scope_keys.add(rel_str)
                # Unchanged path whose prior record_set entry is missing or
                # stale. Materialize the bytes so the parse/extract pipeline
                # can build the entry for the current record_set. Read on
                # demand since the fast-path didn't.
                content = (
                    event.head or b""
                    if oversize_bytes is not None
                    else self._contained_read_bytes(path)
                )
                files_processed_unchanged += 1
            elif not event.is_changed:
                # `force_full_reprocess`: bytes are unchanged (this is
                # still an unchanged-content event) but the vision config
                # changed, so this file's visuals need their signatures
                # recomputed against it. Re-read on demand since the
                # fast-path skipped reading unchanged content.
                content = (
                    event.head or b""
                    if oversize_bytes is not None
                    else self._contained_read_bytes(path)
                )
                files_changed += 1
                changed_record_keys.add(rel_str)
                if scoped:
                    changed_scope_keys.add(rel_str)
            else:
                files_changed += 1
                changed_record_keys.add(rel_str)
                if scoped:
                    changed_scope_keys.add(rel_str)
                content = (
                    event.head or b""
                    if oversize_bytes is not None
                    else event.content
                )
                # Defensive: `content` MUST be bytes for the parse
                # pipeline below. If the helper fast-paths in
                # text-read mode we'd have a type mismatch; but
                # this source constructs FileWalk with
                # read_as="bytes", so event.content is bytes for
                # any changed event.
                assert isinstance(content, bytes), (
                    "DocCorpusSource expects bytes content from the changed-event path"
                )
            # An oversize file's `content` is only its first bytes (enough to
            # sniff the format); its real size gates every branch below.
            byte_count = len(content) if oversize_bytes is None else oversize_bytes

            doc_id = self._canonicalize(f"doc:{self.repo}:{rel_str}")
            source_file_id = source_file_ids_by_rel[rel_str]
            # Link the document to its file node only when that node exists
            # now; the graph keeps whatever the source emits, so an edge to a
            # missing node would dangle and block the consistency gate.
            emit_source_file_edge = source_file_id in available_source_file_ids
            asserted_paths.add(rel_str)
            if emit_source_file_edge:
                linked_paths.add(rel_str)
            warnings: list[str] = []
            if oversize_bytes is not None:
                self._oversize_recorded[rel_str] = oversize_bytes
                # Every oversize file says so, whatever branch handles it
                # below (unknown format, standalone image or PDF, text).
                warnings.append("oversize")

            chunks_this_doc: list[tuple[str, str, dict]] = []
            mentions_this_doc: list[tuple[str, str, str, str, dict]] = []
            # Per `add-code-symbol-extraction`: the code-symbol
            # extractor emits code_symbol / code_import /
            # code_reference NodeFacts + their edges via `_emit`.
            # Those graph rows are source-owned, so they MUST land
            # in this record's footprint too — otherwise the
            # runner's record_set coverage check flags every
            # code-symbol row as stale and refuses to advance the
            # watermark. `_maybe_emit_code_symbols` appends the
            # emitted descendants/edges here.
            code_descendants_this_doc: list[RecordNode] = []
            code_edges_this_doc: list[EdgeKey] = []
            # Per `first-class-visual-artifacts` (Task 2): VisualArtifact
            # nodes + their contains/member_of/derived_from edges are
            # source-owned descendants of this document, exactly like
            # code_symbol above — they must land in this record's
            # footprint so the record_set coverage check doesn't flag
            # them as stale and block the watermark advance.
            visual_descendants_this_doc: list[RecordNode] = []
            visual_edges_this_doc: list[EdgeKey] = []
            caption_edges_this_doc: list[EdgeKey] = []

            fmt = self._format_for(path, content)

            # Vision captioning — standalone image files.
            # When captioning is enabled and the file is an image extension,
            # skip the normal text-parser path and produce caption chunks.
            # Discovery (the VisualArtifact node + contains/member_of edges)
            # happens unconditionally for a qualifying image, BEFORE any
            # VLM call and independent of whether one is ultimately made
            # (oversize-for-caption / per-image failures still leave the
            # visual discoverable). Per req.visual-artifacts.incremental.
            _is_image_ext = path.suffix.lower() in IMAGE_EXTENSIONS
            if rel_str in selected_figure_paths or (
                captioning_cfg.enabled and _caption_svc is not None and _is_image_ext
            ):
                self._emit_image_caption_chunks(
                    content=content,
                    doc_id=doc_id,
                    rel_str=rel_str,
                    revision=revision,
                    emit=_emit,
                    oversize=(
                        (oversize_bytes, content_sha)
                        if oversize_bytes is not None
                        else None
                    ),
                    caption_svc=_caption_svc,
                    captioning_cfg=captioning_cfg,
                    chunks_this_doc=chunks_this_doc,
                    visual_descendants_this_doc=visual_descendants_this_doc,
                    visual_edges_this_doc=visual_edges_this_doc,
                    source_file_id=(source_file_id if emit_source_file_edge else None),
                    caption_cache=prior_visual_captions,
                    caption_cache_out=new_visual_captions,
                    caption_outcome=(
                        outcome
                        if (outcome := caption_outcomes_by_rel.get(rel_str)) is not None
                        and outcome.failure_kind != "below_min_pixels"
                        else None
                    ),
                )
                fmt = fmt or "image"  # ensure Document.format is meaningful
            elif fmt is None:
                warnings.append("no_format")
            elif byte_count > self.max_bytes_per_file:
                warnings.append("oversize")
            else:
                # Look up the parser in the DEPLOYMENT's
                # bundle, not the global registry. Otherwise
                # extractors.yaml's parser selection is
                # meaningless (every globally-registered
                # parser would be reachable). Fixed in
                # Stride A.1 (2026-04-26) alongside W9 test.
                parser = bundle.parsers.get(fmt)
                if parser is None:
                    warnings.append("no_parser")
                # The format the content is chunked as; differs from `fmt`
                # (the document's format) only after a text fallback.
                content_fmt = fmt
                if parser is not None:
                    try:
                        parsed = parser.parse(content, path=str(path))
                    except Exception as e:
                        warnings.append(f"parse_failed:{type(e).__name__}")
                        parsed = None
                        # `text_fallback`: a structured parse that fails
                        # (templated YAML, Python 2 source, ...) still
                        # contributes the file's text; the document keeps both
                        # the parse_failed and a text_fallback warning, so the
                        # fallback is recorded, never silent. The text is then
                        # chunked as `text`: no code symbols, no code chunker.
                        text_parser = bundle.parsers.get("text")
                        if (
                            self.text_fallback
                            and fmt != "text"
                            and text_parser is not None
                            and _looks_like_text(content)
                        ):
                            parsed = text_parser.parse(content, path=str(path))
                            warnings.append(f"text_fallback:{fmt}")
                            content_fmt = "text"
                    if parsed is not None:
                        warnings.extend(
                            str(warning)
                            for warning in parsed.metadata.get("parse_warnings", [])
                        )
                        # Per `add-code-symbol-extraction`
                        # (2026-04-26): when format == 'code'
                        # AND a code-symbol extractor is
                        # registered for the file's language,
                        # parse the source for symbols /
                        # imports / references and emit the
                        # corresponding NodeFacts + EdgeFacts
                        # alongside the chunks. A failing
                        # code extractor logs but does NOT
                        # skip the document.
                        if content_fmt == "code":
                            self._maybe_emit_code_symbols(
                                doc_id=doc_id,
                                rel_str=rel_str,
                                source_text=getattr(
                                    parsed,
                                    "text",
                                    "",
                                )
                                or content.decode(
                                    "utf-8",
                                    errors="replace",
                                ),
                                path=path,
                                revision=revision,
                                emit=_emit,
                                footprint_descendants=(code_descendants_this_doc),
                                footprint_edges=code_edges_this_doc,
                            )
                        # Per `refine-doc-corpus-extraction`
                        # (2026-04-26): pick the chunker
                        # registered for this file's format
                        # (falls back to `default`); pick every
                        # NER backend whose route matches the
                        # format (or `*`).
                        format_chunker = bundle.chunker_for_format(content_fmt)
                        format_ners = bundle.ner_for_format(content_fmt)
                        chunk_texts = self._chunks_from(parsed, format_chunker)
                        original_chunks = self._original_text_chunks(
                            parsed, format_chunker
                        )
                        chunk_texts = chunk_texts + original_chunks
                        warnings.extend(
                            str(warning)
                            for warning in getattr(format_chunker, "last_warnings", [])
                        )
                        # The string every chunk offset indexes: the parsed
                        # text, then (when the file as written is chunked
                        # too) a blank line and that text.
                        positioned_text = getattr(parsed, "text", "") or ""
                        if original_chunks:
                            positioned_text += _ORIGINAL_TEXT_JOIN + (
                                self._original_text(parsed) or ""
                            )
                        occurrence_layout = self._occurrence_layout(
                            [(o, e, t) for o, e, t, _h, _k, _x in chunk_texts],
                            positioned_text,
                        )
                        for (
                            offset,
                            end,
                            ctext,
                            heading_path,
                            chunk_kind,
                            extras,
                        ), occurrence_attrs in zip(chunk_texts, occurrence_layout):
                            extras = dict(extras)
                            figure_environment_span = extras.pop(
                                INTERNAL_FIGURE_ENVIRONMENT_SPAN,
                                None,
                            )
                            chunk_sha = hashlib.sha256(
                                ctext.encode("utf-8"),
                            ).hexdigest()
                            raw_chunk_id = chunk_node_id(
                                ctext,
                                chunk_kind,
                                doc_id=doc_id,
                                extras=extras,
                            )
                            chunk_id = self._canonicalize(raw_chunk_id)
                            # A chunk with no real span stores no position
                            # at all: never offset 0 standing in for one.
                            chunk_attrs = (
                                {
                                    "char_offset": offset,
                                    "char_end": end,
                                    "char_length": end - offset,
                                }
                                if offset is not None and end is not None
                                else {}
                            ) | {
                                "heading_path": (
                                    " / ".join(heading_path) if heading_path else ""
                                ),
                                "chunk_kind": chunk_kind,
                                "content_sha256": chunk_sha,
                                "chunker_name": (
                                    type(format_chunker).__name__
                                    if format_chunker
                                    else ("structural_inline")
                                ),
                            }
                            # Per `add-heading-aware-chunker`
                            # (2026-04-26): chunkers can carry
                            # additive attributes (e.g.
                            # `chunk_index_within_section` for
                            # split sections) via `extras`.
                            # Splice them in without
                            # overwriting the canonical attrs
                            # above.
                            if extras:
                                for k, v in extras.items():
                                    chunk_attrs.setdefault(k, v)
                            if (
                                chunk_kind == "figure_caption"
                                and isinstance(figure_environment_span, tuple)
                                and len(figure_environment_span) == 2
                            ):
                                env_start, env_end = figure_environment_span
                                caption_visual_ids = sorted(
                                    {
                                        visual_id
                                        for (
                                            occurrence_start,
                                            occurrence_end,
                                            visual_id,
                                        ) in figure_graphics_by_source.get(
                                            rel_str,
                                            (),
                                        )
                                        if env_start <= occurrence_start
                                        and occurrence_end <= env_end
                                    }
                                )
                                if caption_visual_ids:
                                    chunk_attrs[_INTERNAL_CAPTION_VISUAL_IDS] = (
                                        caption_visual_ids
                                    )
                            if occurrence_attrs:
                                chunk_attrs[_INTERNAL_OCCURRENCE_ATTRS] = (
                                    occurrence_attrs
                                )
                            _record_table_outcome(chunk_attrs)
                            chunks_this_doc.append(
                                (
                                    chunk_id,
                                    ctext,
                                    chunk_attrs,
                                )
                            )
                            for ner_backend in format_ners:
                                try:
                                    entities = ner_backend.extract(ctext)
                                except Exception:  # noqa: BLE001 — NER backend failed on one chunk; log full traceback and skip just this chunk.
                                    log.exception(
                                        "DocCorpusSource: NER "
                                        "%s raised on chunk in "
                                        "%s; continuing",
                                        type(ner_backend).__name__,
                                        path,
                                    )
                                    entities = []
                                for ent in entities:
                                    span_start = getattr(ent, "start", 0)
                                    span_end = getattr(ent, "end", 0)
                                    mention_seed = (
                                        f"{chunk_id}|"
                                        f"{ent.type}|{ent.value}|"
                                        f"{span_start}"
                                    )
                                    mention_sha = hashlib.sha256(
                                        mention_seed.encode("utf-8"),
                                    ).hexdigest()
                                    mention_id = self._canonicalize(
                                        f"mention:{mention_sha[:16]}",
                                    )
                                    mentions_this_doc.append(
                                        (
                                            mention_id,
                                            chunk_id,
                                            ent.type,
                                            ent.value,
                                            {
                                                "span_start": span_start,
                                                "span_end": span_end,
                                                "extractor_name": (
                                                    type(
                                                        ner_backend,
                                                    ).__name__
                                                ),
                                                "confidence": float(
                                                    getattr(
                                                        ent,
                                                        "confidence",
                                                        1.0,
                                                    ),
                                                ),
                                            },
                                        )
                                    )

                        # PDF embedded image captioning.
                        # After all text chunks are processed, extract embedded
                        # images from the PDF bytes and caption each qualifying
                        # image. Failures per-image are caught and logged.
                        if (
                            fmt == "pdf"
                            and captioning_cfg.enabled
                            and _caption_svc is not None
                        ):
                            self._emit_pdf_embedded_captions(
                                pdf_bytes=content,
                                pdf_content_sha256=content_sha,
                                doc_id=doc_id,
                                rel_str=rel_str,
                                revision=revision,
                                emit=_emit,
                                caption_svc=_caption_svc,
                                captioning_cfg=captioning_cfg,
                                chunks_this_doc=chunks_this_doc,
                                visual_descendants_this_doc=(
                                    visual_descendants_this_doc
                                ),
                                visual_edges_this_doc=visual_edges_this_doc,
                                source_file_id=(
                                    source_file_id if emit_source_file_edge else None
                                ),
                                caption_cache=prior_visual_captions,
                                caption_cache_out=new_visual_captions,
                            )

            if (
                fmt == "pdf"
                and _table_service is not None
                and byte_count <= self.max_bytes_per_file
            ):
                table_result = _table_service.extract_document(content)
                warnings.extend(table_result.warnings)
                table_stats["table_cache_hits"] += table_result.cache_hits
                table_stats["table_cache_misses"] += table_result.cache_misses
                table_stats["table_provider_invocations"] += (
                    table_result.provider_invocations
                )
                table_stats["table_external_network_attempts"] += (
                    table_result.external_network_attempts
                )
                for occurrence in table_result.occurrences:
                    extras = occurrence.to_chunk_extras()
                    grid = occurrence.result.grid
                    chunk_text = grid.searchable_text() if grid is not None else ""
                    chunk_id = self._canonicalize(
                        table_occurrence_id(doc_id, occurrence.result.source_locator)
                    )
                    chunk_attrs = {
                        "chunk_kind": "table",
                        "content_sha256": hashlib.sha256(
                            chunk_text.encode("utf-8")
                        ).hexdigest(),
                        "chunker_name": "PdfTableTranscriptionService",
                        **extras,
                    }
                    _record_table_outcome(chunk_attrs)
                    if occurrence.result.error_code is not None:
                        warnings.append(
                            f"pdf_table_{occurrence.result.error_code}:"
                            f"{occurrence.result.source_locator}"
                        )
                    chunks_this_doc.append((chunk_id, chunk_text, chunk_attrs))

            # Emit Document NodeFact.
            doc_attrs = {
                "doc_id": doc_id,
                "repo": self.repo,
                "path": rel_str,
                "format": fmt or "unknown",
                "byte_count": byte_count,
                "content_sha256": content_sha,
                "parsed_at": observed_at.isoformat(),
                # Per `refine-doc-corpus-extraction` (W8): emit
                # parse_warnings as a JSON array (not a comma-
                # joined string). The projector's
                # `okg._stamp_searchable_text` skips array-typed
                # fields, so warnings stay out of the BM25 index.
                "parse_warnings": list(dict.fromkeys(warnings)),
                "title": rel_str,  # placeholder; richer
                # extractors override later
            }
            # Dated-snapshot stamp (PACT okg-corpus-doc-corpus /
            # req.fresh.passage): the date must be QUERYABLE graph state, not
            # only a line in the freeze manifest, because the corpus is one
            # observation among several and a query has to be able to say
            # which one it is reading.
            if self.snapshot_date is not None:
                doc_attrs["snapshot_date"] = self.snapshot_date
            # Source-supplied citation (source-supplied-search-citations).
            doc_citation = self._citation_fields(rel_str)
            doc_attrs.update(doc_citation)
            _emit(
                NodeFact(
                    node_id=doc_id,
                    subtype="document",
                    attrs=doc_attrs,
                    source_record_id={
                        "repo": self.repo,
                        "path": rel_str,
                    },
                    source_revision=revision,
                    op="I",
                )
            )
            # derived_from source_file edge. Only assert it when the
            # predecessor graph has the endpoint; Chronos-native graph writes
            # intentionally preserve source output instead of dropping
            # orphan edges in projection.
            if emit_source_file_edge:
                _emit(
                    EdgeFact(
                        src=doc_id,
                        dst=source_file_id,
                        edge_type="derived_from",
                        attrs={},
                        source_record_id={
                            "repo": self.repo,
                            "path": rel_str,
                        },
                        source_revision=revision,
                        op="I",
                    )
                )

            # Chunks + member_of edges.
            # Each chunk node's own revision (content-addressed for every
            # kind but a table occurrence), so the record_set descendant
            # below carries the same revision as the emitted NodeFact.
            chunk_revision_by_id: dict[str, dict[str, Any]] = {}
            # Content-addressed chunks are shared nodes (issue #3382); a
            # table occurrence belongs to its document alone.
            chunk_mode_by_id: dict[str, str] = {}
            for chunk_ordinal, (chunk_id, ctext, attrs) in enumerate(chunks_this_doc):
                attrs = dict(attrs)
                caption_visual_ids = tuple(attrs.pop(_INTERNAL_CAPTION_VISUAL_IDS, ()))
                # `doc_char_length` and the stored separators describe this
                # occurrence, so they go on the member_of edge only.
                occurrence_attrs = dict(
                    attrs.pop(_INTERNAL_OCCURRENCE_ATTRS, None) or {}
                )
                # Source-supplied citation (source-supplied-search-citations):
                # a chunk cites its parent document's record.
                if doc_citation:
                    attrs = {**attrs, **doc_citation}
                # The snapshot-stable POSITIONAL KEY (req.fresh.comparable).
                # `chunk_ordinal` is this chunk's index in the document's
                # emission order — text chunks in chunker order, then table
                # occurrences, then image captions — which is deterministic
                # for fixed bytes and a fixed extractor config.
                #
                # Node identity is content-addressed and stays that way, so
                # when a passage's text changes between snapshots its key is
                # unchanged and its node id differs: the key says WHICH
                # passage, the id says WHETHER IT CHANGED. Positional identity
                # would have collapsed those two answers into one and lost the
                # second.
                #
                # These land on the node AND (below) on the member_of edge.
                # The edge is the occurrence-authoritative carrier: chunk
                # nodes deduplicate by content, so one text appearing in two
                # documents is ONE node and the node-level pair names only one
                # of its occurrences.
                if self.snapshot_date is not None:
                    attrs = {
                        **attrs,
                        "snapshot_date": self.snapshot_date,
                        "doc_path": rel_str,
                        "chunk_ordinal": chunk_ordinal,
                    }
                # A chunk node id is content-addressed for every kind but a
                # table occurrence, so its revision must name the chunk's own
                # seed rather than the owning file: the same paragraph in two
                # files is ONE node and must stage once, with one revision.
                # Deciding by comparing against the content-addressed id (not
                # by testing `chunk_kind == "table"`) keeps this exact when
                # `chunk_node_id` falls back to content identity for a table
                # with no `source_locator`.
                chunk_kind = str(attrs.get("chunk_kind") or DEFAULT_CHUNK_KIND)
                chunk_identity_sha = _chunk_identity_sha(ctext, chunk_kind)
                if chunk_id == self._canonicalize(
                    f"chunk:{chunk_identity_sha[:16]}"
                ):
                    chunk_revision = self._record_revision(
                        revision, chunk_identity_sha,
                    )
                    chunk_mode = SHARED_NODE_OWNERSHIP_MODE
                else:
                    chunk_revision = revision
                    chunk_mode = "authoritative"
                chunk_revision_by_id[chunk_id] = chunk_revision
                chunk_mode_by_id[chunk_id] = chunk_mode
                _emit(
                    NodeFact(
                        node_id=chunk_id,
                        subtype="document_chunk",
                        attrs={
                            "chunk_id": chunk_id,
                            "text": ctext,
                            **attrs,
                        },
                        source_record_id={
                            "repo": self.repo,
                            "path": rel_str,
                            "chunk_id": chunk_id,
                        },
                        source_revision=chunk_revision,
                        op="I",
                        ownership_mode=chunk_mode,
                    )
                )
                _emit(
                    EdgeFact(
                        src=chunk_id,
                        dst=doc_id,
                        edge_type="member_of",
                        # These fields describe this occurrence in this document,
                        # not the content-deduplicated chunk node. The SQL view reads
                        # them before legacy node mirrors.
                        attrs={
                            key: attrs[key]
                            for key in (
                                "char_offset",
                                "char_end",
                                "char_length",
                                "doc_path",
                                "chunk_ordinal",
                                "snapshot_date",
                                "heading_path",
                                "chunker_name",
                                "table_grid_status",
                                "table_extraction_lane",
                                "table_grid_error_code",
                                "source_locator",
                                "source_content_sha256",
                                "source_page_index",
                                "source_bbox",
                                "cache_signature",
                                # code chunks (PythonSymbolChunker)
                                "start_line",
                                "end_line",
                                "symbol",
                            )
                            if key in attrs
                        }
                        # Issue #2668: every occurrence carries its position in
                        # the document's emission order and the document's
                        # chunk count, for every corpus (not only a dated
                        # snapshot). A chunk whose producer reports no span
                        # (HTML paragraphs, Markdown structural chunks) stores
                        # no `char_offset`; the ordinal still orders it. The
                        # count lets a reader see that an occurrence is
                        # missing: a paragraph that repeats inside one
                        # document is ONE content-addressed node with ONE
                        # member_of edge, so one of its ordinals never reaches
                        # the graph. `doc_char_length` and the separators let a
                        # whole read prove it covers the document exactly
                        # (`_occurrence_layout`).
                        | occurrence_attrs
                        | {
                            "chunk_ordinal": chunk_ordinal,
                            "chunk_total": len(chunks_this_doc),
                        },
                        source_record_id={
                            "repo": self.repo,
                            "path": rel_str,
                            "chunk_id": chunk_id,
                        },
                        source_revision=revision,
                        op="I",
                    )
                )
                for visual_id in caption_visual_ids:
                    self._emit_caption_of_edge(
                        _emit,
                        caption_edges_this_doc,
                        chunk_id=chunk_id,
                        visual_id=visual_id,
                        rel_str=rel_str,
                        revision=revision,
                    )
                _emit(
                    EdgeFact(
                        src=doc_id,
                        dst=chunk_id,
                        edge_type="contains",
                        attrs={},
                        source_record_id={
                            "repo": self.repo,
                            "path": rel_str,
                            "chunk_id": chunk_id,
                        },
                        source_revision=revision,
                        op="I",
                    )
                )

            # Per `add-markdown-link-extraction` (2026-04-27):
            # for markdown docs, extract inline links from each
            # chunk and emit `references` edges to the linked
            # docs. Links resolve relative to this doc's path;
            # external URLs / anchor-only / unresolvable hrefs
            # are silently skipped at the resolver. Within one
            # chunk, multiple links to the SAME target dedup
            # to a single edge whose `anchors` attr aggregates
            # every distinct fragment.
            references_this_doc: list[dict[str, Any]] = []
            if fmt == "markdown" and chunks_this_doc:
                from okg.substrate.library.extraction.parse.markdown_links import (  # noqa: E501
                    extract_links,
                    resolve_link_target,
                )

                for chunk_id, ctext, _attrs in chunks_this_doc:
                    # Aggregate per (chunk_id, target_doc_id):
                    # keep the first link_text seen, accumulate
                    # all distinct anchors.
                    by_target: dict[
                        str,
                        dict[str, Any],
                    ] = {}
                    for link in extract_links(ctext):
                        resolved, anchor = resolve_link_target(
                            link.href,
                            rel_str,
                        )
                        if resolved is None:
                            continue
                        if resolved not in known_doc_paths:
                            continue
                        target_doc_id = self._canonicalize(
                            f"doc:{self.repo}:{resolved}",
                        )
                        agg = by_target.get(target_doc_id)
                        if agg is None:
                            by_target[target_doc_id] = {
                                "link_text": link.text,
                                "anchors": ([anchor] if anchor else []),
                            }
                        else:
                            if anchor and anchor not in agg["anchors"]:
                                agg["anchors"].append(anchor)
                    for target_doc_id, agg in by_target.items():
                        ref_attrs: dict[str, Any] = {
                            "link_text": agg["link_text"],
                        }
                        if agg["anchors"]:
                            ref_attrs["anchors"] = agg["anchors"]
                        ref_src_id = {
                            "repo": self.repo,
                            "path": rel_str,
                            "chunk_id": chunk_id,
                            "target_doc_id": target_doc_id,
                        }
                        _emit(
                            EdgeFact(
                                src=chunk_id,
                                dst=target_doc_id,
                                edge_type="references",
                                attrs=ref_attrs,
                                source_record_id=ref_src_id,
                                source_revision=revision,
                                op="I",
                            )
                        )
                        references_this_doc.append(
                            {
                                "chunk_id": chunk_id,
                                "target_doc_id": target_doc_id,
                                "link_text": agg["link_text"],
                                "anchors": agg["anchors"],
                            }
                        )

            # Track this path's emission for cursor-state
            # diff at end of run. Mentions get filled in after
            # the alias-resolver pass below.
            next_emissions[rel_str] = {
                "doc_id": doc_id,
                "source_file_id": (source_file_id if emit_source_file_edge else None),
                "chunks": [chunk_id for chunk_id, _t, _a in chunks_this_doc],
                "chunk_revisions": chunk_revision_by_id,
                "chunk_modes": chunk_mode_by_id,
                "mentions": [],  # filled post-alias-resolution
                # Per `add-markdown-link-extraction` (2026-04-27):
                # references retract per chunk via reconciliation
                # — the diff between prior and current ref-keys
                # drives op='R' edges so removed links disappear
                # from the graph cleanly.
                "references": references_this_doc,
                # Per `add-code-symbol-extraction`: the
                # code_symbol / code_import / code_reference nodes
                # and their edges are part of this document's
                # footprint so the record_set coverage check
                # accounts for them.
                "code_descendants": code_descendants_this_doc,
                "code_edges": code_edges_this_doc,
                # Per `first-class-visual-artifacts` (Task 2): VisualArtifact
                # nodes and their edges are source-owned descendants of this
                # document, so they must be in the footprint for retraction.
                "visual_descendants": visual_descendants_this_doc,
                "visual_edges": visual_edges_this_doc,
                "caption_edges": caption_edges_this_doc,
            }

            # Defer mention emission to after the corpus walk
            # so the alias resolver can run in one batched
            # query (Stride A.1, W5).
            all_mentions.append((rel_str, mentions_this_doc))

        if caption_incomplete:
            completed_scope = False

        # Stride A.1 (2026-04-26) — W5: resolve every distinct
        # entity_value in one bulk query, then emit mention
        # NodeFacts + their `member_of` edge to the chunk +
        # (when an alias resolves) a `mentions <canonical>`
        # edge. Misses emit only the member_of edge; the
        # mention stays "dangling" until a future alias gets
        # registered or a post-projection linker fires.
        unique_values = {
            evalue
            for (_path, mentions) in all_mentions
            for (_mid, _cid, _etype, evalue, _attrs) in mentions
        }
        alias_map = self._resolve_aliases_batch(unique_values)

        for rel_str, mentions in all_mentions:
            # Mentions are emitted after the walk so the alias resolver can
            # batch, but they belong to `rel_str`'s record: reuse that file's
            # content revision rather than a walk-final one.
            revision = revision_by_path[rel_str]
            for mention_id, chunk_id, etype, evalue, attrs in mentions:
                # Track for cursor diff. The mention's chunk_id
                # and resolved canonical (filled below) are
                # both needed to retract the matching edges.
                if rel_str in next_emissions:
                    next_emissions[rel_str]["mentions"].append(
                        {
                            "mention_id": mention_id,
                            "chunk_id": chunk_id,
                            "canonical": alias_map.get(evalue),
                        }
                    )
                _emit(
                    NodeFact(
                        node_id=mention_id,
                        subtype="entity_mention",
                        attrs={
                            "mention_id": mention_id,
                            "entity_type": etype,
                            "entity_value": evalue,
                            **attrs,
                        },
                        source_record_id={
                            "repo": self.repo,
                            "path": rel_str,
                            "mention_id": mention_id,
                        },
                        source_revision=revision,
                        op="I",
                        # Seeded on its chunk id: shared whenever the chunk is.
                        ownership_mode=SHARED_NODE_OWNERSHIP_MODE,
                    )
                )
                _emit(
                    EdgeFact(
                        src=mention_id,
                        dst=chunk_id,
                        edge_type="member_of",
                        attrs={},
                        source_record_id={
                            "repo": self.repo,
                            "path": rel_str,
                            "mention_id": mention_id,
                        },
                        source_revision=revision,
                        op="I",
                    )
                )
                canonical = alias_map.get(evalue)
                if canonical:
                    _emit(
                        EdgeFact(
                            src=mention_id,
                            dst=canonical,
                            edge_type="mentions",
                            attrs={"resolver": "fuzzy_alias"},
                            source_record_id={
                                "repo": self.repo,
                                "path": rel_str,
                                "mention_id": mention_id,
                                "canonical": canonical,
                            },
                            source_revision=revision,
                            op="I",
                        )
                    )

        def _facts() -> Iterator[Any]:
            for f in emitted_facts:
                yield f

        record_set = None
        record_set_replacement_keys: set[str] | None = None
        prior_emission_schema_current = (
            isinstance(cursor, dict)
            and cursor.get("doc_corpus_emission_schema_version")
            == DOC_CORPUS_EMISSION_SCHEMA_VERSION
        )
        if completed_scope:
            # Built from this run's processed (changed) files.
            current_rs = self._record_set_from_emissions(
                next_emissions,
                revision_by_path=revision_by_path,
            )
            # Copy unchanged-but-asserted entries from prior so
            # the runner's record_set diff doesn't synthesize
            # spurious retractions for files we skipped. Per
            # `add-delta-proportional-source-runs`.
            for rel_str in unchanged_with_prior_rs:
                current_rs.records[rel_str] = prior_rs_records[rel_str]
            if scoped:
                scope_record_keys = self._scope_record_keys(scope_keys)
                deleted_scope_keys = scope_record_keys.difference(
                    walked_scope_keys,
                )
                replacement_keys = changed_scope_keys | deleted_scope_keys
                if not prior_emission_schema_current:
                    # The records outside the scope were emitted under an
                    # older schema (issue #3382: schema 6 declared shared
                    # chunks `authoritative`, schema 7 `substitute`).
                    # Merging them with this run's records would mix
                    # declarations the runner refuses, so the run asserts
                    # no record set and the next full run re-emits them all.
                    completed_scope = False
                    current_rs = None
                elif prior_rs_records:
                    current_rs = self._scoped_record_set(
                        cursor,
                        current=current_rs,
                        replacement_keys=replacement_keys,
                    )
                    completed_scope = current_rs is not None
                elif external_record_set_authority:
                    record_set_replacement_keys = replacement_keys
                else:
                    completed_scope = False
            elif external_record_set_authority:
                deleted_record_keys = set(walk.prior_files).difference(
                    set(walk.next_files),
                )
                record_set_replacement_keys = changed_record_keys | deleted_record_keys
            record_set = current_rs

        # Build the next cursor's `files` map. For scoped runs
        # (narrow walk), merge with the prior `files` map so paths
        # outside the narrow set retain their hashes. For
        # full-walk runs, walk.next_cursor() already contains the
        # complete current state.
        if scoped:
            prior_files = walk.prior_files
            next_files = walk.next_files
            deleted_scope_keys = self._scope_record_keys(scope_keys).difference(
                walked_scope_keys,
            )
            merged_files = {
                key: value
                for key, value in prior_files.items()
                if key not in deleted_scope_keys
            }
            merged_files.update(next_files)
            cursor_extras = {"files": merged_files}
        else:
            cursor_extras = walk.next_cursor()
        if current_git_frontier is not None:
            cursor_extras["git_frontier"] = current_git_frontier
        # Record which documents now carry the file link. A completed scope
        # replaced every asserted record, so those paths take this run's
        # answer; otherwise nothing was retracted and prior links stand.
        # With the prior set unknown, only a complete full run knows every
        # path; a scoped or incomplete run leaves the record absent so the
        # next full run still rebuilds what it cannot vouch for.
        if prior_links is not None or (completed_scope and not scoped):
            next_links = set(prior_links or ())
            if completed_scope:
                next_links -= asserted_paths
            next_links |= linked_paths
            cursor_extras[SOURCE_FILE_LINKS_KEY] = sorted(
                next_links.intersection(cursor_extras.get("files") or {})
            )
        # Per `first-class-visual-artifacts` (Task 3): carry the per-visual
        # caption-reuse cache forward. Visuals not touched this run keep
        # their prior entry (harmless — never read unless that visual is
        # seen again); visuals processed this run (reused or freshly
        # captioned) get `new_visual_captions`' entry, which wins on
        # overlap.
        cursor_extras["visual_captions"] = {
            **prior_visual_captions,
            **new_visual_captions,
        }
        cursor_extras["vision_config_fingerprint"] = vision_fingerprint
        cursor_extras["table_transcription_config_fingerprint"] = (
            table_config_fingerprint
        )
        if table_transcription_cfg.enabled and isinstance(
            _table_cache, MemoryTableResponseCache
        ):
            cache_items = sorted(_table_cache.values.items())[-1000:]
            cursor_extras["table_transcription_cache"] = dict(cache_items)
        else:
            cursor_extras["table_transcription_cache"] = {}
        # A run that did not rebuild the whole record set (a scoped run, or one
        # that did not complete its scope) keeps the prior emission schema
        # version and epoch, so the next complete run still re-parses the
        # files it did not (issue #3382).
        cursor_extras["doc_corpus_emission_schema_version"] = (
            cursor.get("doc_corpus_emission_schema_version")
            if (scoped or not completed_scope) and isinstance(cursor, dict)
            else DOC_CORPUS_EMISSION_SCHEMA_VERSION
        )
        cursor_extras["doc_corpus_reparse_epoch"] = (
            cursor.get("doc_corpus_reparse_epoch")
            if (scoped or not completed_scope) and isinstance(cursor, dict)
            else DOC_CORPUS_REPARSE_EPOCH
        )
        cursor_extras["doc_corpus_snapshot_date"] = self.snapshot_date
        # A scoped run re-parses only its slice, and a run that did not
        # complete its scope emits no record set, so nothing it replaced was
        # retracted (issue #2872). Either way, after a limit change the corpus
        # holds records made under two limits. Record a value no real limit
        # equals: the next run re-parses everything, whichever limit it uses
        # (including a revert to the old one).
        cursor_extras[_MAX_BYTES_CURSOR_KEY] = (
            _MAX_BYTES_MIXED
            if (scoped or not completed_scope) and not max_bytes_unchanged
            else self.max_bytes_per_file
        )
        # A run that did not rebuild the whole record set -- a scoped run
        # re-read only its slice; a run that did not complete its scope emits
        # no record set, so nothing it replaced was retracted -- keeps the
        # prior setting, so the next complete run still re-reads every file.
        keep_prior_config = (scoped or not completed_scope) and isinstance(
            cursor, dict
        )
        cursor_extras["doc_corpus_text_fallback"] = (
            bool(cursor.get("doc_corpus_text_fallback", False))
            if keep_prior_config
            else self.text_fallback
        )
        cursor_extras["doc_corpus_extractors_fingerprint"] = (
            cursor.get("doc_corpus_extractors_fingerprint")
            if keep_prior_config
            else extractors_fingerprint
        )
        if self.project_manifest is not None:
            if scoped_manifest_refresh_pending and not completed_scope:
                prior_manifest_digest = (
                    cursor.get("latex_project_manifest_digest")
                    if isinstance(cursor, dict)
                    else None
                )
                if prior_manifest_digest is not None:
                    cursor_extras["latex_project_manifest_digest"] = (
                        prior_manifest_digest
                    )
            else:
                cursor_extras["latex_project_manifest_digest"] = (
                    self.project_manifest.digest
                )

        emitted_nodes = sum(isinstance(fact, NodeFact) for fact in emitted_facts)
        emitted_edges = sum(isinstance(fact, EdgeFact) for fact in emitted_facts)
        if scoped and not files:
            scope_status = "scoped_no_matching_files"
        elif scoped:
            scope_status = "scoped"
        elif completed_scope:
            scope_status = "full"
        else:
            scope_status = "cursor"
        self.last_run_stats = {
            "scope_status": scope_status,
            "run_mode": mode,
            "scope_keys": list(scope_keys),
            "scope_widened_for_project_manifest": (scope_widened_for_project_manifest),
            "files_seen": files_seen,
            "files_changed": files_changed,
            "files_skipped_unchanged": files_skipped_unchanged,
            "files_reused_prior_record_set": files_reused_prior_rs,
            "files_processed_unchanged": files_processed_unchanged,
            "emitted_nodes": emitted_nodes,
            "emitted_edges": emitted_edges,
            "record_set_authority": (
                "replacement_slice"
                if record_set_replacement_keys is not None
                else "rebuilt"
                if record_set is not None
                else "none"
            ),
            "record_set_replacement_keys": len(
                record_set_replacement_keys or (),
            ),
            "caption_calls": caption_stats["calls"],
            "caption_rate_limit_hits": caption_stats["rate_limit_hits"],
            "caption_failures_by_kind": dict(caption_stats["failures_by_kind"]),
            "caption_incomplete": caption_incomplete,
            **table_stats,
            **self._enumeration_stats(),
        }

        return SourceRun(
            facts=_facts(),
            completed_scope=completed_scope,
            next_cursor={
                **cursor_extras,
            },
            run_mode=mode,
            record_set=record_set,
            record_set_replacement_keys=record_set_replacement_keys,
            # The manifest widening above abandoned the requested narrow scope
            # and walked the complete selected corpus, so `record_set` is a
            # whole-source footprint. Declare that to the runner: without it
            # the run still looks path-scoped, deletion authority is refused,
            # and the caption's edge to the superseded visual survives
            # (issue #1454).
            effective_scope_complete=(
                scope_widened_for_project_manifest
                and completed_scope
                and record_set is not None
                and record_set_replacement_keys is None
            ),
        )

    def _git_frontier(self) -> dict[str, Any] | None:
        """Return a cheap git frontier covering included doc-corpus paths."""
        try:
            from ._git_helpers import in_proc_head_sha, in_proc_status_porcelain

            head_sha = in_proc_head_sha(self.repo_root)
            dirty_paths = sorted(
                path
                for path in in_proc_status_porcelain(self.repo_root)
                if self._frontier_path_included(path)
            )
        except Exception:
            return None

        dirty_meta: list[dict[str, Any]] = []
        for rel_path in dirty_paths:
            path = self.repo_root / rel_path
            try:
                fd, _ = self._contained_reader.open(path)
                try:
                    stat_result = os.fstat(fd)
                finally:
                    os.close(fd)
            except FileContainmentError:
                raise
            except OSError:
                dirty_meta.append(
                    {
                        "path": rel_path,
                        "exists": False,
                        "mtime_ns": None,
                        "size": None,
                    }
                )
                continue
            dirty_meta.append(
                {
                    "path": rel_path,
                    "exists": True,
                    "mtime_ns": stat_result.st_mtime_ns,
                    "size": stat_result.st_size,
                }
            )
        return {
            "schema": "okg.doc_corpus.git_frontier.v1",
            "head": head_sha,
            "dirty_paths": dirty_meta,
            "include_globs": list(self.include_globs),
            "exclude_globs": list(self.exclude_globs),
            # The candidate set decides which files are read, so a change
            # must defeat the unchanged-frontier skip. (A changed byte limit
            # is handled by the `doc_corpus_max_bytes_per_file` cursor key.)
            "include_untracked": self.include_untracked,
            "include_ignored_paths": list(self.include_ignored_paths),
        }

    def _frontier_path_included(self, rel_path: str) -> bool:
        rel = Path(str(rel_path).replace("\\", "/").lstrip("/"))
        if not rel.as_posix() or rel.as_posix().startswith("../"):
            return False
        if not _match_any(rel, self.include_globs):
            return False
        return not _match_any(rel, self.exclude_globs)

    def _scoped_record_set(
        self,
        cursor: Optional[dict[str, Any]],
        *,
        current: RecordSet,
        replacement_keys: set[str],
    ) -> Optional[RecordSet]:
        """Return a full-source record-set for a scoped complete run."""
        if not isinstance(cursor, dict):
            return None
        prior_raw = cursor.get("record_set")
        if prior_raw is None:
            return None
        prior = RecordSet.from_cursor(prior_raw)
        records = {
            key: value
            for key, value in prior.records.items()
            if key not in replacement_keys
        }
        records.update(current.records)
        return RecordSet(records=records)

    def _figure_graphics_by_source(
        self,
    ) -> dict[str, tuple[tuple[int, int, str], ...]]:
        """Index resolved figure occurrences by their owning LaTeX source.

        The project manifest is the authority for path resolution and selected
        file content.  Keeping the raw source offsets here lets the chunk loop
        match an occurrence to one exact figure environment without persisting
        occurrence state on a content-deduplicated chunk node.
        """
        if self.project_manifest is None:
            return {}

        visual_ids_by_path: dict[str, set[str]] = {}
        for project in self.project_manifest.projects:
            for item in project.files:
                if "figure" not in item.roles:
                    continue
                visual_ids_by_path.setdefault(item.path, set()).add(
                    self._canonicalize(
                        f"visual:{item.content_sha256[:16]}",
                    )
                )

        indexed: dict[str, set[tuple[int, int, str]]] = {}
        for project in self.project_manifest.projects:
            for occurrence in project.dependencies:
                if (
                    occurrence.role != "figure"
                    or occurrence.status != "resolved"
                    or occurrence.resolved_path is None
                ):
                    continue
                for visual_id in visual_ids_by_path.get(
                    occurrence.resolved_path,
                    (),
                ):
                    indexed.setdefault(occurrence.source_path, set()).add(
                        (occurrence.start, occurrence.end, visual_id)
                    )

        return {
            source_path: tuple(sorted(occurrences))
            for source_path, occurrences in indexed.items()
        }

    def _scope_record_keys(self, scope_keys: Sequence[str]) -> set[str]:
        keys: set[str] = set()
        for key in scope_keys:
            try:
                rel = Path(key)
                if rel.is_absolute():
                    rel = rel.relative_to(self.repo_root)
            except ValueError:
                continue
            rel_str = rel.as_posix()
            if rel_str.startswith("../") or rel_str == "..":
                continue
            keys.add(rel_str)
        return keys

    @staticmethod
    def _record_revision(
        base_revision: Mapping[str, Any],
        content_sha: str,
    ) -> dict[str, Any]:
        """A content revision: the extraction-config base plus one sha.

        Combined with the base it answers "would this payload differ?"
        without reference to which run produced it -- the property the
        Chronos no-op filter and the runner's COPY dedupe both need.

        WHICH sha depends on how the fact's node identity is derived, and
        the two must agree or one node acquires several revisions per run:

        * File-identified facts -- the `document` node, every edge, and the
          `entity_mention` / `code_symbol` / `code_import` /
          `code_reference` / table-occurrence nodes whose ids all name
          `rel_str` -- take `FileWalkEvent.content_hash`, the owning file's
          sha (the value the cursor stores as `last_content_sha256`).
        * Content-addressed facts -- non-table `document_chunk` nodes
          (`_chunk_node_id`) and `visual_artifact` nodes
          (`visual:<content_sha[:16]>`) -- take a sha of their OWN content:
          `_chunk_identity_sha(text, chunk_kind)` and the image bytes'
          sha256 respectively. One paragraph or image shared by two files
          is ONE node, so keying its revision on the owning file would
          stage it once per file and add `okg.node_facts` rows for a node
          the graph writes once.

        `base_revision` may itself be a file revision; the caller's
        `content_sha` replaces its `content_sha256`.
        """
        return {**dict(base_revision), "content_sha256": content_sha}

    def _record_set_from_emissions(
        self,
        emissions: dict[str, dict[str, Any]],
        *,
        revision_by_path: Mapping[str, dict[str, Any]],
    ) -> RecordSet:
        records: dict[str, RecordEmission] = {}
        for rel_str, emitted in emissions.items():
            revision = revision_by_path[rel_str]
            doc_id = emitted.get("doc_id")
            primary = None
            descendants: list[RecordNode] = []
            edges: list[EdgeKey] = []

            if doc_id:
                primary = RecordNode(
                    node_id=doc_id,
                    subtype="document",
                    source_record_id={
                        "repo": self.repo,
                        "path": rel_str,
                    },
                    source_revision=revision,
                )
                source_file_id = emitted.get("source_file_id")
                if source_file_id:
                    edges.append(
                        EdgeKey(
                            src=doc_id,
                            dst=str(source_file_id),
                            edge_type="derived_from",
                            source_record_id={
                                "repo": self.repo,
                                "path": rel_str,
                            },
                            source_revision=revision,
                        )
                    )

            chunk_revisions = emitted.get("chunk_revisions") or {}
            chunk_modes = emitted["chunk_modes"]
            for chunk_id in emitted.get("chunks", []):
                descendants.append(
                    RecordNode(
                        node_id=chunk_id,
                        subtype="document_chunk",
                        source_record_id={
                            "repo": self.repo,
                            "path": rel_str,
                            "chunk_id": chunk_id,
                        },
                        # The chunk node's own revision, matching the
                        # NodeFact; content-addressed for every kind but a
                        # table occurrence.
                        source_revision=chunk_revisions.get(
                            chunk_id, revision,
                        ),
                        # The same mode as the NodeFact (issue #3382).
                        ownership_mode=chunk_modes[chunk_id],
                    )
                )
                if doc_id:
                    edge_source_record = {
                        "repo": self.repo,
                        "path": rel_str,
                        "chunk_id": chunk_id,
                    }
                    edges.append(
                        EdgeKey(
                            src=chunk_id,
                            dst=doc_id,
                            edge_type="member_of",
                            source_record_id=edge_source_record,
                            source_revision=revision,
                        )
                    )
                    edges.append(
                        EdgeKey(
                            src=doc_id,
                            dst=chunk_id,
                            edge_type="contains",
                            source_record_id=edge_source_record,
                            source_revision=revision,
                        )
                    )

            for mention in emitted.get("mentions", []):
                mention_id = mention["mention_id"]
                chunk_id = mention.get("chunk_id")
                canonical = mention.get("canonical")
                descendants.append(
                    RecordNode(
                        node_id=mention_id,
                        subtype="entity_mention",
                        source_record_id={
                            "repo": self.repo,
                            "path": rel_str,
                            "mention_id": mention_id,
                        },
                        source_revision=revision,
                        ownership_mode=SHARED_NODE_OWNERSHIP_MODE,
                    )
                )
                if chunk_id:
                    edges.append(
                        EdgeKey(
                            src=mention_id,
                            dst=chunk_id,
                            edge_type="member_of",
                            source_record_id={
                                "repo": self.repo,
                                "path": rel_str,
                                "mention_id": mention_id,
                            },
                            source_revision=revision,
                        )
                    )
                if canonical:
                    edges.append(
                        EdgeKey(
                            src=mention_id,
                            dst=canonical,
                            edge_type="mentions",
                            attrs={"resolver": "fuzzy_alias"},
                            source_record_id={
                                "repo": self.repo,
                                "path": rel_str,
                                "mention_id": mention_id,
                                "canonical": canonical,
                            },
                            source_revision=revision,
                        )
                    )

            for ref in emitted.get("references", []):
                edges.append(
                    EdgeKey(
                        src=ref["chunk_id"],
                        dst=ref["target_doc_id"],
                        edge_type="references",
                        source_record_id={
                            "repo": self.repo,
                            "path": rel_str,
                            "chunk_id": ref["chunk_id"],
                            "target_doc_id": ref["target_doc_id"],
                        },
                        source_revision=revision,
                    )
                )

            # Per `add-code-symbol-extraction`: fold the
            # code_symbol / code_import / code_reference nodes and
            # their edges into the footprint. These RecordNode /
            # EdgeKey objects were captured at emit time so the
            # coverage check sees exactly the source-owned rows.
            descendants.extend(emitted.get("code_descendants", []))
            edges.extend(emitted.get("code_edges", []))
            # Per `first-class-visual-artifacts` (Task 2): fold VisualArtifact
            # nodes and their contains/member_of/derived_from edges into the
            # footprint so RecordSet retraction covers them like code_symbols.
            descendants.extend(emitted.get("visual_descendants", []))
            edges.extend(emitted.get("visual_edges", []))
            edges.extend(emitted.get("caption_edges", []))

            records[rel_str] = RecordEmission(
                primary=primary,
                descendants=descendants,
                edges=edges,
            )
        return RecordSet(records=records)

    # ---------- chunking helper ----------

    # ---------- code-symbol extraction (Stride B) ----------

    # Path-suffix → CodeSymbolExtractor language. Empty by
    # default so existing deployments don't unexpectedly start
    # emitting code_symbol nodes. Deployments that want
    # code-symbol extraction pass the explicit map (e.g.
    # `{".py": "python"}`).
    _DEFAULT_CODE_EXTENSION_TO_LANGUAGE: ClassVar[dict[str, str]] = {}

    def _maybe_emit_code_symbols(
        self,
        *,
        doc_id: str,
        rel_str: str,
        source_text: str,
        path: Path,
        revision: dict[str, Any],
        emit,
        footprint_descendants: list[RecordNode] | None = None,
        footprint_edges: list[EdgeKey] | None = None,
    ) -> None:
        """Per `add-code-symbol-extraction` (2026-04-26): if a
        CodeSymbolExtractor is registered for the file's
        language, run it and emit code_symbol / code_import /
        code_reference NodeFacts + edges. A failing extractor
        logs but doesn't skip the doc.

        `footprint_descendants` / `footprint_edges`, when
        provided, accumulate a RecordNode / EdgeKey for every
        emitted node / edge so the caller can fold the
        code-symbol rows into the document's record-set
        footprint (the coverage check would otherwise flag them
        as stale source-owned rows).
        """
        descendants = footprint_descendants if footprint_descendants is not None else []
        edges = footprint_edges if footprint_edges is not None else []

        def _emit_node(fact: NodeFact) -> None:
            emit(fact)
            descendants.append(
                RecordNode(
                    node_id=fact.node_id,
                    subtype=fact.subtype,
                    source_record_id=fact.source_record_id,
                    source_revision=fact.source_revision,
                )
            )

        def _emit_edge(fact: EdgeFact) -> None:
            emit(fact)
            edges.append(
                EdgeKey(
                    src=fact.src,
                    dst=fact.dst,
                    edge_type=fact.edge_type,
                    attrs=fact.attrs,
                    source_record_id=fact.source_record_id,
                    source_revision=fact.source_revision,
                )
            )

        from okg.substrate.library.extraction.code import (
            CodeExtractorNotFound,
            code_extractor_for,
        )

        lang_map = self._code_extension_to_language
        language = lang_map.get(path.suffix.lower())
        # Extension-less python scripts (text_fallback -> `code`) get code chunks
        # but no code symbols: their document is not canonicalized to the
        # source_file node, and the catalog only allows code symbols / imports
        # under a source_file (emitting them here was a narrowing violation).
        if language is None:
            return
        try:
            extractor = code_extractor_for(language)
        except CodeExtractorNotFound:
            return
        try:
            report = extractor.extract(
                source_text,
                path=str(path),
            )
        except Exception:  # noqa: BLE001 — code-extractor failed on this file; log traceback and continue without code symbols for it.
            log.exception(
                "DocCorpusSource: code extractor for %s raised "
                "on %s; continuing without code symbols",
                language,
                path,
            )
            return

        # Symbols.
        for sym in report.symbols:
            symbol_id = self._canonicalize(
                f"symbol:{self.repo}:{rel_str}:{sym.qualified_name}",
            )
            attrs = {
                "symbol_id": symbol_id,
                "language": report.language,
                "kind": sym.kind,
                "name": sym.name,
                "qualified_name": sym.qualified_name,
                # The CodeSymbol alias key (issue #1747).
                "scoped_qualified_name": _scoped_symbol_name(
                    self.repo, rel_str, sym.qualified_name,
                ),
                "signature": sym.signature,
                "docstring": sym.docstring,
                "span_start": sym.span_start,
                "span_end": sym.span_end,
            }
            _emit_node(
                NodeFact(
                    node_id=symbol_id,
                    subtype="code_symbol",
                    attrs=attrs,
                    source_record_id={
                        "repo": self.repo,
                        "path": rel_str,
                        "qualified_name": sym.qualified_name,
                    },
                    source_revision=revision,
                    op="I",
                )
            )
            # `document contains code_symbol`.
            _emit_edge(
                EdgeFact(
                    src=doc_id,
                    dst=symbol_id,
                    edge_type="contains",
                    attrs={},
                    source_record_id={
                        "repo": self.repo,
                        "path": rel_str,
                        "symbol_id": symbol_id,
                    },
                    source_revision=revision,
                    op="I",
                )
            )
            # `code_symbol member_of <parent_symbol>` when the
            # qname has a parent (e.g. C.m → C).
            if "." in sym.qualified_name:
                parent_qname = sym.qualified_name.rsplit(".", 1)[0]
                parent_id = self._canonicalize(
                    f"symbol:{self.repo}:{rel_str}:{parent_qname}",
                )
                _emit_edge(
                    EdgeFact(
                        src=symbol_id,
                        dst=parent_id,
                        edge_type="member_of",
                        attrs={},
                        source_record_id={
                            "repo": self.repo,
                            "path": rel_str,
                            "symbol_id": symbol_id,
                        },
                        source_revision=revision,
                        op="I",
                    )
                )

        # Imports.
        for imp in report.imports:
            seed = (
                f"{self.repo}|{rel_str}|{imp.module}|"
                f"{imp.name}|{imp.alias}|{imp.kind}|"
                f"{imp.span_start}"
            )
            sha = hashlib.sha256(seed.encode("utf-8")).hexdigest()
            import_id = self._canonicalize(f"import:{sha[:16]}")
            _emit_node(
                NodeFact(
                    node_id=import_id,
                    subtype="code_import",
                    attrs={
                        "import_id": import_id,
                        "module": imp.module,
                        "name": imp.name,
                        "alias": imp.alias,
                        "kind": imp.kind,
                        "language": report.language,
                        "span_start": imp.span_start,
                        "span_end": imp.span_end,
                    },
                    source_record_id={
                        "repo": self.repo,
                        "path": rel_str,
                        "import_id": import_id,
                    },
                    source_revision=revision,
                    op="I",
                )
            )
            # `document declares code_import`.
            _emit_edge(
                EdgeFact(
                    src=doc_id,
                    dst=import_id,
                    edge_type="declares",
                    attrs={},
                    source_record_id={
                        "repo": self.repo,
                        "path": rel_str,
                        "import_id": import_id,
                    },
                    source_revision=revision,
                    op="I",
                )
            )

        # References.
        for ref in report.references:
            seed = (
                f"{self.repo}|{rel_str}|{ref.caller_qname}|"
                f"{ref.callee_name}|{ref.span_start}"
            )
            sha = hashlib.sha256(seed.encode("utf-8")).hexdigest()
            reference_id = self._canonicalize(
                f"reference:{sha[:16]}",
            )
            _emit_node(
                NodeFact(
                    node_id=reference_id,
                    subtype="code_reference",
                    attrs={
                        "reference_id": reference_id,
                        "caller_qname": ref.caller_qname,
                        "callee_name": ref.callee_name,
                        "resolution_kind": ref.resolution_kind,
                        "span_start": ref.span_start,
                        "span_end": ref.span_end,
                        "language": report.language,
                    },
                    source_record_id={
                        "repo": self.repo,
                        "path": rel_str,
                        "reference_id": reference_id,
                    },
                    source_revision=revision,
                    op="I",
                )
            )
            # `code_symbol calls code_symbol` when the caller
            # is a known qname AND the callee resolves
            # within-file (we can mint a same-file callee id);
            # otherwise the reference stays a standalone node.
            if ref.caller_qname and ref.resolution_kind == "within_file":
                caller_id = self._canonicalize(
                    f"symbol:{self.repo}:{rel_str}:{ref.caller_qname}",
                )
                callee_id = self._canonicalize(
                    f"symbol:{self.repo}:{rel_str}:{ref.callee_name}",
                )
                _emit_edge(
                    EdgeFact(
                        src=caller_id,
                        dst=callee_id,
                        edge_type="calls",
                        attrs={
                            "callee_token": ref.callee_name,
                            "resolution_kind": ref.resolution_kind,
                        },
                        source_record_id={
                            "repo": self.repo,
                            "path": rel_str,
                            "reference_id": reference_id,
                        },
                        source_revision=revision,
                        op="I",
                    )
                )

    @staticmethod
    def _original_text_chunks(
        parsed,
        chunker,
    ) -> list[tuple[Optional[int], Optional[int], str, list[str], str, dict]]:
        """Chunk the file as written when the parsed text is a re-emission.

        A parser that re-emits its input (YAML: sorted keys, no comments)
        puts the file as written under `ORIGINAL_TEXT_METADATA_KEY`. Those
        chunks come after the re-emission's, as `code_block`; adding them
        does not change the re-emission's chunks or their ids.

        Offsets: the document reads as the re-emission, a blank line, then
        the original text, so an original chunk's offset is its offset in
        the original text plus `len(parsed.text) + 2`. Readers that order a
        document's chunks by offset (`okg.v_chunk_sequence`, the console
        record view) then show the two forms one after the other instead of
        interleaved.

        Nothing is added when the original text is blank or equals the
        re-emission (line endings aside).
        """
        original = DocCorpusSource._original_text(parsed)
        if original is None:
            return []
        canonical = getattr(parsed, "text", "") or ""
        base = len(canonical) + len(_ORIGINAL_TEXT_JOIN)
        pieces = (
            [(c.span[0], c.span[1], c.text) if c.span else (None, None, c.text)
             for c in chunker.chunk(original)]
            if chunker is not None
            else [(0, len(original), original)]
        )
        return [
            (
                base + start if start is not None else None,
                base + end if end is not None else None,
                text,
                [],
                ORIGINAL_TEXT_CHUNK_KIND,
                {},
            )
            for start, end, text in pieces
            if text.strip()
        ]

    @staticmethod
    def _original_text(parsed) -> Optional[str]:
        """The file as written, when it is chunked after the re-emission."""
        metadata = getattr(parsed, "metadata", None) or {}
        original = metadata.get(ORIGINAL_TEXT_METADATA_KEY)
        if not isinstance(original, str) or not original.strip():
            return None
        canonical = getattr(parsed, "text", "") or ""
        if original.replace("\r\n", "\n").strip() == canonical.strip():
            return None
        return original

    @staticmethod
    def _occurrence_layout(
        pieces: list[tuple[Optional[int], Optional[int], str]],
        text: str,
    ) -> list[dict[str, Any]]:
        """Occurrence attributes that let a whole read rejoin `text` exactly.

        `pieces` are one document's text chunks in emission order as
        `(char_offset, char_end, chunk text)`; `text` is the string those
        offsets index. Every placed piece whose text IS `text[start:end]`
        gets `doc_char_length` (the length of `text`), so a reader can tell a
        read that reaches the end from one that stops short; a piece whose
        span names other text (a LaTeX table: source span, rendered text)
        gets nothing, so a reader can never take it for document text. A
        matching piece that starts after the text already covered gets
        `separator_before`, the characters in between, when they are
        whitespace of at most
        `MAX_STORED_SEPARATOR_CHARS` (a PDF's blank line between pages); the
        piece that reaches furthest gets `separator_after` for trailing
        whitespace the same way. Anything else between two pieces stays a gap,
        and the read reports it as partial. An unplaced piece gets nothing.
        """

        out: list[dict[str, Any]] = [{} for _ in pieces]
        covered = 0
        furthest: Optional[int] = None
        for index, (start, end, piece) in enumerate(pieces):
            if start is None or end is None or text[start:end] != piece:
                continue
            out[index]["doc_char_length"] = len(text)
            if not piece:
                continue
            gap = text[covered:start]
            if (
                gap
                and len(gap) <= MAX_STORED_SEPARATOR_CHARS
                and gap.isspace()
            ):
                out[index]["separator_before"] = gap
            if end > covered or furthest is None:
                covered = max(covered, end)
                furthest = index
        if furthest is not None:
            tail = text[covered:]
            if tail and len(tail) <= MAX_STORED_SEPARATOR_CHARS and tail.isspace():
                out[furthest]["separator_after"] = tail
        return out

    @staticmethod
    def _chunks_from(
        parsed,
        chunker,
    ) -> list[tuple[Optional[int], Optional[int], str, list[str], str, dict]]:
        """Resolve chunks from the parsed document.

        Priority:
        1. If the chunker advertises `prefers_raw_text = True`
           (e.g. `MarkdownHeadingChunker` per
           `add-heading-aware-chunker` 2026-04-26), bypass
           parser-emitted structural chunks and run the chunker
           on `parsed.text` directly. Heading-aware chunkers
           reconstruct structure from the AST themselves; the
           parser's pre-split paragraph chunks would defeat the
           size-normalization pass.
        2. Else, if the parser already produced structural
           chunks (e.g. `MarkdownParser` fills `parsed.chunks`),
           use them — they carry heading paths the body chunker
           can't reconstruct. Structural chunks that exceed the
           registered chunker's `max_chars` (when known) are
           re-chunked via the chunker so very large sections
           don't escape the size budget. heading_path is
           preserved across the re-chunk.
        3. Else, if a chunker is registered, run it on
           `parsed.text`.
        4. Else, return one whole-document chunk.

        Returns a list of (char_offset, char_end, text, heading_path,
        chunk_kind, extras) tuples. The offsets are the chunk's span in
        `parsed.text`, or `(None, None)` when its producer reports none (a
        piece cut from an unplaced chunk has none either).
        `extras` is the chunk's additive attribute bag (empty
        dict when unused), spliced into the resulting
        `document_chunk` node's attrs by the caller.
        """
        prefers_raw = bool(getattr(chunker, "prefers_raw_text", False))

        def _bounds(
            c, *, base: Optional[int] = 0
        ) -> tuple[Optional[int], Optional[int]]:
            # A chunk with no span has no position, and neither does a piece
            # cut from a chunk with none: offsets relative to an unplaced
            # parent are not document offsets. `None` stores no position
            # rather than a made-up one (before, `0` and `len(text)`).
            span = getattr(c, "span", None)
            if span is None or base is None:
                return None, None
            start = base + int(span[0])
            end = base + int(span[1])
            if end < start:
                raise ValueError(f"chunk span end {end} precedes start {start}")
            return start, end

        def _kind(c) -> str:
            return str(
                getattr(c, "chunk_kind", DEFAULT_CHUNK_KIND) or DEFAULT_CHUNK_KIND
            )

        # Stride A.1 (2026-04-26): infer the chunker's
        # max_chars budget so oversize structural chunks get
        # re-windowed instead of slipping through unbounded.
        max_chars: Optional[int] = (
            int(getattr(chunker, "max_chars"))
            if chunker is not None and getattr(chunker, "max_chars", None)
            else None
        )

        # Path 1: heading-aware chunker that wants the raw text
        # (skip parsed.chunks entirely).
        if prefers_raw and chunker is not None:
            text = getattr(parsed, "text", "") or ""
            if not text:
                return []
            out0: list[
                tuple[Optional[int], Optional[int], str, list[str], str, dict]
            ] = []
            for c in chunker.chunk(text):
                start, end = _bounds(c)
                out0.append(
                    (
                        start,
                        end,
                        c.text,
                        list(getattr(c, "heading_path", []) or []),
                        _kind(c),
                        dict(getattr(c, "extras", None) or {}),
                    )
                )
            return out0

        if getattr(parsed, "chunks", None):
            out: list[
                tuple[Optional[int], Optional[int], str, list[str], str, dict]
            ] = []
            for c in parsed.chunks:
                start, end = _bounds(c)
                heading = list(getattr(c, "heading_path", []) or [])
                parent_kind = _kind(c)
                base_extras = dict(getattr(c, "extras", None) or {})
                if (
                    max_chars is not None
                    and chunker is not None
                    and len(c.text) > max_chars
                ):
                    for sub in chunker.chunk(c.text):
                        sub_start, sub_end = _bounds(sub, base=start)
                        sub_kind = _kind(sub)
                        resolved_kind = (
                            parent_kind
                            if parent_kind != DEFAULT_CHUNK_KIND
                            and sub_kind == DEFAULT_CHUNK_KIND
                            else sub_kind
                        )
                        sub_extras = dict(getattr(sub, "extras", None) or {})
                        merged_extras = {**base_extras, **sub_extras}
                        out.append(
                            (
                                sub_start,
                                sub_end,
                                sub.text,
                                list(heading),
                                resolved_kind,
                                merged_extras,
                            )
                        )
                else:
                    out.append(
                        (
                            start,
                            end,
                            c.text,
                            heading,
                            parent_kind,
                            base_extras,
                        )
                    )
            return out
        text = getattr(parsed, "text", "") or ""
        if chunker is None:
            return (
                [
                    (
                        0,
                        len(text),
                        text,
                        [],
                        DEFAULT_CHUNK_KIND,
                        {},
                    )
                ]
                if text
                else []
            )
        chunked = chunker.chunk(text)
        out2: list[
            tuple[Optional[int], Optional[int], str, list[str], str, dict]
        ] = []
        for c in chunked:
            start, end = _bounds(c)
            out2.append(
                (
                    start,
                    end,
                    c.text,
                    list(getattr(c, "heading_path", []) or []),
                    _kind(c),
                    dict(getattr(c, "extras", None) or {}),
                )
            )
        return out2

    @staticmethod
    def _vision_config_digest(captioning_cfg: "CaptioningConfig") -> str:
        """Deterministic digest of the captioning config fields that affect
        a caption's content — a component of every caption's vision_signature.

        Thin wrapper over the shared
        :func:`captioning.vision_signature.vision_config_digest` so the source
        and the ``VisualCaptionEnricher`` share ONE definition; see that module
        for why only `mode`/`model` (not the limit knobs) belong here.
        """
        from okg.substrate.library.captioning.vision_signature import (
            vision_config_digest,
        )

        return vision_config_digest(
            mode=captioning_cfg.mode,
            model=captioning_cfg.model,
        )

    def _compute_vision_signature(
        self,
        *,
        content_sha256: str,
        render_sha256: Optional[str],
        caption_model: str,
        caption_provider: str,
        prompt: str,
        captioning_cfg: "CaptioningConfig",
        context: str = "",
    ) -> str:
        """Recaption-reuse signature for a caption chunk.

        Deterministic over the visual's content/render hash plus the
        model/provider/prompt/config that produced the caption. The
        ``VisualCaptionEnricher`` skips the VLM call when a chunk's stored
        signature is unchanged; the source predicts and stores it here.

        Delegates to the shared
        :func:`captioning.vision_signature.compute_vision_signature` — the ONE
        formula both sites use, so the source's predicted signature and the
        enricher's recomputed one can never drift (which would silently
        re-caption the whole corpus). ``context`` is folded only when non-empty
        so context-less deployments stay byte-identical to pre-context runs.
        """
        from okg.substrate.library.captioning.vision_signature import (
            compute_vision_signature,
        )

        return compute_vision_signature(
            content_sha256=content_sha256,
            render_sha256=render_sha256,
            model=caption_model,
            provider=caption_provider,
            prompt=prompt,
            mode=captioning_cfg.mode,
            context=context,
        )

    @staticmethod
    def _provider_from_model(model: str) -> str:
        """Extract the litellm provider prefix from a model string.

        Mirrors `CaptionService._provider_from_model` so a signature can be
        predicted here (before any VLM call) from `captioning_cfg.model`
        alone, matching what `CaptionResult.caption_provider` will read
        once `CaptionService` is actually invoked.
        """
        if "/" in model:
            return model.split("/", 1)[0]
        return model

    def _vision_gate_fingerprint(
        self,
        captioning_cfg: "CaptioningConfig",
    ) -> str:
        """Cheap fingerprint of everything that changes a visual's
        vision_signature, used to gate the unchanged-corpus short-circuits.

        Distinct from `_vision_config_digest`: that digest is a component
        baked into every stored `vision_signature` (so a stored signature
        stays comparable across runs); this fingerprint additionally folds
        in the resolved per-source prompt so a prompt-only change (no
        model/mode/limit change) still defeats the file-identity-only
        short-circuits below and forces a re-walk. Per
        `req.visual-artifacts.incremental`.
        """
        prompt_digest = hashlib.sha256(
            (captioning_cfg.prompt_for(self.name) or "").encode("utf-8"),
        ).hexdigest()
        config_digest = self._vision_config_digest(captioning_cfg)
        # Inline figure detection changes which crops exist AND the context
        # each caption is grounded in, without touching the PDF bytes — so the
        # detection config must defeat the file-identity-only short-circuits
        # too. Detection is deterministic given the PDF bytes (already in file
        # identity) + the detector config, so fold the enable flag and a
        # content-addressed weights identity here. The identity is the file's
        # sha256 (or the pinned default repo@revision#digest), NOT just the
        # configured path — so replacing the weights in place, or re-pinning the
        # default model, invalidates unchanged-file cursors. Importing
        # figure_detector does not import torch/doclayout.
        detection_digest = ""
        if captioning_cfg.figure_detection:
            from okg.substrate.library.captioning.figure_detector import (
                weights_identity,
            )

            detection_digest = hashlib.sha256(
                json.dumps(
                    {
                        "figure_detection": True,
                        "weights_identity": weights_identity(
                            captioning_cfg.figure_detection_weights,
                        ),
                    },
                    sort_keys=True,
                ).encode("utf-8"),
            ).hexdigest()
        pdf_render_digest = hashlib.sha256(
            json.dumps(
                {
                    "renderer_name": _STANDALONE_PDF_RENDERER,
                    "renderer_version": _pdfium_renderer_version(),
                    "page_number": _STANDALONE_PDF_PAGE_NUMBER,
                    "render_dpi": captioning_cfg.pdf_render_dpi,
                },
                sort_keys=True,
            ).encode("utf-8"),
        ).hexdigest()
        seed = f"{config_digest}|{prompt_digest}"
        if detection_digest:
            seed = f"{seed}|{detection_digest}"
        seed = f"{seed}|{pdf_render_digest}"
        return hashlib.sha256(seed.encode("utf-8")).hexdigest()

    def _emit_visual_edge(
        self,
        emit: Any,
        visual_edges_this_doc: list,
        *,
        src: str,
        dst: str,
        edge_type: str,
        rel_str: str,
        visual_id: str,
        revision: dict,
        extra_src_id: Optional[dict] = None,
    ) -> None:
        """Emit a VisualArtifact edge as both an EdgeFact (graph write) and an
        EdgeKey (record-set footprint), so source-sync retracts it cleanly.

        Mirrors the code_symbol footprint pattern already used in this file.
        """
        src_id = {"repo": self.repo, "path": rel_str, "visual_id": visual_id}
        if extra_src_id:
            src_id = {**src_id, **extra_src_id}
        emit(
            EdgeFact(
                src=src,
                dst=dst,
                edge_type=edge_type,
                attrs={},
                source_record_id=src_id,
                source_revision=revision,
                op="I",
            )
        )
        visual_edges_this_doc.append(
            EdgeKey(
                src=src,
                dst=dst,
                edge_type=edge_type,
                attrs={},
                source_record_id=src_id,
                source_revision=revision,
            )
        )

    def _emit_caption_of_edge(
        self,
        emit: Any,
        caption_edges_this_doc: list[EdgeKey],
        *,
        chunk_id: str,
        visual_id: str,
        rel_str: str,
        revision: dict[str, Any],
    ) -> None:
        """Emit and reconcile one author-written caption relationship."""
        source_record_id = {
            "repo": self.repo,
            "path": rel_str,
            "chunk_id": chunk_id,
            "visual_id": visual_id,
        }
        emit(
            EdgeFact(
                src=chunk_id,
                dst=visual_id,
                edge_type="caption_of",
                attrs={},
                source_record_id=source_record_id,
                source_revision=revision,
                op="I",
            )
        )
        caption_edges_this_doc.append(
            EdgeKey(
                src=chunk_id,
                dst=visual_id,
                edge_type="caption_of",
                attrs={},
                source_record_id=source_record_id,
                source_revision=revision,
            )
        )

    @staticmethod
    def plan_caption_call(
        img: Any,
        *,
        context: str,
        caption_kwargs: dict[str, Any],
    ) -> dict[str, Any]:
        """Freeze the arguments of one VLM caption call.

        Separated so the call can be issued away from this class's emission
        code. `context` is positional-optional on `CaptionService.caption`, so
        it is omitted when empty — injected caption services predating the
        context parameter keep working unchanged.
        """
        args: tuple = (img, context) if context else (img,)
        return {"args": args, "kwargs": dict(caption_kwargs)}

    @staticmethod
    def run_caption_call(
        caption_svc: Any,
        planned: dict[str, Any],
        *,
        image_label: str,
    ) -> Optional[Sequence[Any]]:
        """Issue one planned caption call. Returns None on failure.

        THIS is the only part of the captioning path that may run on a worker
        thread: it touches the provider and nothing else. Every caller of
        `_emit_caption_chunks` emits facts on the publish connection, so the
        emission must stay on the owning thread — handing it to workers is the
        hazard the two-phase model-call split exists to prevent.

        Failure is best-effort by design and unchanged: the VisualArtifact is
        already emitted, so a failed caption leaves the visual discovered but
        uncaptioned rather than losing the whole document.
        """
        try:
            return caption_svc.caption(*planned["args"], **planned["kwargs"])
        except Exception as exc:  # noqa: BLE001 - preserve legacy best effort
            log.warning(
                "DocCorpusSource: captioning failed for %s (kind=%s); "
                "VisualArtifact still emitted",
                image_label,
                DocCorpusSource._caption_failure_kind(exc),
            )
            return None

    @staticmethod
    def run_caption_call_outcome(
        caption_svc: Any,
        planned: dict[str, Any],
        *,
        image_label: str,
    ) -> _CaptionCallOutcome:
        """Issue one call and return only safe failure metadata.

        Provider messages and exception reprs never enter a durable source
        record. The shared VLM client already owns bounded retry/backoff; this
        wrapper only classifies its terminal outcome and any rate-limit hits it
        reports on the thread-local call stats.
        """
        try:
            results = caption_svc.caption(*planned["args"], **planned["kwargs"])
        except Exception as exc:  # noqa: BLE001 - classify one provider call
            kind = DocCorpusSource._caption_failure_kind(exc)
            rate_limit_hits = DocCorpusSource._caption_rate_limit_hits(caption_svc)
            log.warning(
                "DocCorpusSource: captioning failed for %s (kind=%s); "
                "VisualArtifact still emitted",
                image_label,
                kind,
            )
            return _CaptionCallOutcome(
                failure_kind=kind,
                rate_limit_hits=rate_limit_hits,
            )
        rate_limit_hits = DocCorpusSource._caption_rate_limit_hits(caption_svc)
        if results is None:
            return _CaptionCallOutcome(
                failure_kind="provider_error",
                rate_limit_hits=rate_limit_hits,
            )
        if any(bool(getattr(item, "truncated", False)) for item in results):
            return _CaptionCallOutcome(
                failure_kind="truncated",
                rate_limit_hits=rate_limit_hits,
            )
        if not results or not any(
            str(getattr(item, "text", "") or "").strip() for item in results
        ):
            return _CaptionCallOutcome(
                failure_kind="empty_result",
                rate_limit_hits=rate_limit_hits,
            )
        return _CaptionCallOutcome(
            results=results,
            rate_limit_hits=rate_limit_hits,
        )

    @staticmethod
    def _caption_failure_kind(exc: BaseException) -> str:
        """Map provider failures to a small, non-sensitive operator taxonomy."""
        seen: set[int] = set()
        current: Optional[BaseException] = exc
        while current is not None and id(current) not in seen:
            seen.add(id(current))
            name = type(current).__name__.lower()
            status = getattr(current, "status_code", None)
            if (
                "truncat" in name
                or "length" in name
                or "truncat" in str(current).lower()
            ):
                return "truncated"
            if "ratelimit" in name or "rate_limit" in name or status == 429:
                return "rate_limited"
            current = current.__cause__ or current.__context__
        return "provider_error"

    @staticmethod
    def _caption_rate_limit_hits(caption_svc: Any) -> int:
        """Read retry count from the service/client without retaining errors."""
        owners = (caption_svc, getattr(caption_svc, "_vlm", None))
        for owner in owners:
            stats = getattr(owner, "last_call_stats", None)
            extras = stats.get("extras", {}) if isinstance(stats, dict) else {}
            hits = extras.get("rate_limit_hits") if isinstance(extras, dict) else None
            if isinstance(hits, int) and hits > 0:
                return hits
        return 0

    def _emit_caption_chunks(
        self,
        image_bytes: bytes,
        doc_id: str,
        rel_str: str,
        image_label: str,
        revision: dict,
        emit: Any,
        caption_svc: Any,
        captioning_cfg: "CaptioningConfig",
        chunks_this_doc: list,
        *,
        source_kind: str,
        visual_descendants_this_doc: list,
        visual_edges_this_doc: list,
        source_file_id: Optional[str] = None,
        page_number: Optional[int] = None,
        image_index: Optional[int] = None,
        content_sha256_override: Optional[str] = None,
        render_sha256: Optional[str] = None,
        renderer_name: Optional[str] = None,
        renderer_version: Optional[str] = None,
        render_dpi: Optional[int] = None,
        source_byte_count_override: Optional[int] = None,
        mime_type_override: Optional[str] = None,
        pre_caption_failure_reason: Optional[str] = None,
        probe_image: bool = True,
        caption_cache: Optional[dict[str, dict[str, Any]]] = None,
        caption_cache_out: Optional[dict[str, dict[str, Any]]] = None,
        context: str = "",
        extra_visual_attrs: Optional[dict[str, Any]] = None,
        ocr_text: Optional[str] = None,
        discovery_only: bool = False,
        crop_blob_ref: Optional[str] = None,
        caption_results: Optional[Sequence[Any]] = None,
        caption_batch: Optional[list[_PendingCaptionCall]] = None,
        caption_failure_kind: Optional[str] = None,
    ) -> None:
        """Discover a VisualArtifact, then best-effort caption it.

        ``discovery_only`` (the PDF figure path, since the caption-enricher
        split) emits ONLY the VisualArtifact discovery node + edges and returns
        before any VLM call: captioning is owned by the VisualCaptionEnricher
        (Phase B), not the source. In that mode the node additionally carries
        ``crop_blob_ref`` (the crop's render_sha256 key into
        okg.visual_crop_blobs) and ``caption_context`` (the exact anchored
        "Figure N" text / page-text fallback the VLM should see), so the
        enricher reproduces both the crop bytes and the vision_signature without
        re-opening the PDF.

        ``context`` (detected-figure path) is document text injected into
        the VLM prompt — the figure's anchored caption when detection found
        one, else the page text. It is folded into the vision_signature so a
        context change defeats the recaption cache.
        ``extra_visual_attrs`` are spliced into the VisualArtifact attrs
        (e.g. `bbox`, `confidence` for detector-cropped figures).
        ``ocr_text`` short-circuits the VLM entirely: discovery still runs,
        then the given text is emitted as a single `ocr_text` chunk
        derived_from the visual (offline-OCR'd embedded rasters).

        Discovery — the VisualArtifact node plus its contains / member_of /
        derived_from edges — runs unconditionally and BEFORE any size gate or
        VLM call, so an oversize image, an undecodable image, or a captioning
        failure still leaves the visual first-class in the graph (per
        `req.visual-artifacts.incremental`). Caption failures are caught and
        logged — never propagate.

        Per `first-class-visual-artifacts` (Task 3): `caption_cache` is the
        prior run's `{visual_id: {"vision_signature", "chunks"}}` map (from
        `cursor["visual_captions"]`). When this visual's freshly-computed
        `vision_signature` matches the cached one, the cached chunk(s) are
        re-emitted verbatim and the VLM call is skipped entirely —
        `req.visual-artifacts.incremental`. `caption_cache_out` collects the
        signature this run settled on (reused or freshly captioned) so the
        caller can persist it into the next cursor.
        """
        # --- Content identity + provenance -------------------------------
        content_sha256 = (
            content_sha256_override
            or hashlib.sha256(
                image_bytes,
            ).hexdigest()
        )
        source_byte_count = (
            source_byte_count_override
            if source_byte_count_override is not None
            else len(image_bytes)
        )
        visual_id = self._canonicalize(f"visual:{content_sha256[:16]}")
        # The VisualArtifact node id is content-addressed, so one image
        # embedded in two documents is ONE node. Its revision names the image
        # bytes -- the very bytes the id hashes -- not the owning file, so the
        # node stages once instead of once per document. The edges below keep
        # the file revision: they carry file-owned payload and their identity
        # already names the document.
        visual_revision = self._record_revision(revision, content_sha256)

        # Probe the bytes that will reach the VLM. For a standalone PDF these
        # are rendered PNG bytes, while source identity remains the raw PDF.
        width = height = None
        mime_type = "image/png" if source_kind == "pdf_page_render" else ""
        img = None
        if probe_image:
            try:
                from PIL import Image as _PILImage

                img = _PILImage.open(io.BytesIO(image_bytes))
                width, height = img.size
                if source_kind != "pdf_page_render":
                    mime_type = _PIL_FORMAT_TO_MIME.get(img.format or "", "")
            except Exception:
                log.warning(
                    "DocCorpusSource: could not decode image %s; emitting "
                    "VisualArtifact without dimensions",
                    image_label,
                    exc_info=True,
                )

        visual_attrs: dict[str, Any] = {
            "visual_id": visual_id,
            "source_kind": source_kind,
            "path": rel_str,
            "parent_document_ref": doc_id,
            "byte_count": source_byte_count,
            "content_sha256": content_sha256,
        }
        # `mime_type` is `required: true` on the `VisualArtifact` subtype
        # (schema.yaml), so it must ALWAYS be set — even when PIL couldn't
        # decode the image, or decoded a format outside
        # `_PIL_FORMAT_TO_MIME` (e.g. JPEG2000/JBIG2/CCITT, common in real
        # PDFs). Default to a generic binary mime rather than omitting the
        # attribute (a required-attr contract violation).
        visual_attrs["mime_type"] = (
            mime_type_override or mime_type or "application/octet-stream"
        )
        if width is not None and height is not None:
            visual_attrs["width"] = width
            visual_attrs["height"] = height
        if page_number is not None:
            visual_attrs["page_number"] = page_number
        if image_index is not None:
            visual_attrs["image_index"] = image_index
        if render_sha256:
            visual_attrs["render_sha256"] = render_sha256
        if renderer_name:
            visual_attrs["renderer_name"] = renderer_name
        if renderer_version:
            visual_attrs["renderer_version"] = renderer_version
        if render_dpi is not None:
            visual_attrs["render_dpi"] = render_dpi
        if source_file_id is not None:
            visual_attrs["source_file_ref"] = source_file_id
        if extra_visual_attrs:
            # Additive only: detector-provided attrs (bbox, confidence)
            # must not overwrite the canonical identity/provenance attrs.
            for k, v in extra_visual_attrs.items():
                visual_attrs.setdefault(k, v)
        if discovery_only:
            # Persist the enricher's inputs on the discovery node: the crop
            # bytes' content-address (key into okg.visual_crop_blobs) and the
            # exact VLM context (anchored caption / page-text fallback) the
            # source-time path would have used. Persisting the context verbatim
            # keeps the enricher's vision_signature identical, so reuse holds.
            if crop_blob_ref:
                visual_attrs["crop_blob_ref"] = crop_blob_ref
            visual_attrs["caption_context"] = context

        track_caption_outcome = captioning_cfg.enabled and source_kind in {
            "standalone_image",
            "standalone_pdf",
        }

        def _settle_caption_outcome(
            status: str,
            reason: Optional[str] = None,
        ) -> None:
            if not track_caption_outcome:
                return
            visual_attrs["caption_status"] = status
            if reason is None:
                visual_attrs.pop("caption_skip_reason", None)
            else:
                visual_attrs["caption_skip_reason"] = reason

        # Fail closed until one of the explicit branches below settles a more
        # specific result. A successfully returned source run never publishes a
        # caption-enabled standalone visual without an outcome.
        _settle_caption_outcome(
            "failed",
            pre_caption_failure_reason or "provider_error",
        )

        visual_src_id = {
            "repo": self.repo,
            "path": rel_str,
            "visual_id": visual_id,
        }

        # --- Discovery: emit the VisualArtifact node + edges -------------
        emit(
            NodeFact(
                node_id=visual_id,
                subtype="visual_artifact",
                attrs=visual_attrs,
                source_record_id=visual_src_id,
                source_revision=visual_revision,
                op="I",
                # Keyed by the image bytes: the same image in two
                # repositories is one node (issue #3382).
                ownership_mode=SHARED_NODE_OWNERSHIP_MODE,
            )
        )
        visual_descendants_this_doc.append(
            RecordNode(
                node_id=visual_id,
                subtype="visual_artifact",
                source_record_id=visual_src_id,
                source_revision=visual_revision,
                ownership_mode=SHARED_NODE_OWNERSHIP_MODE,
            )
        )
        self._emit_visual_edge(
            emit,
            visual_edges_this_doc,
            src=doc_id,
            dst=visual_id,
            edge_type="contains",
            rel_str=rel_str,
            visual_id=visual_id,
            revision=revision,
        )
        self._emit_visual_edge(
            emit,
            visual_edges_this_doc,
            src=visual_id,
            dst=doc_id,
            edge_type="member_of",
            rel_str=rel_str,
            visual_id=visual_id,
            revision=revision,
        )
        if source_file_id is not None:
            self._emit_visual_edge(
                emit,
                visual_edges_this_doc,
                src=visual_id,
                dst=source_file_id,
                edge_type="derived_from",
                rel_str=rel_str,
                visual_id=visual_id,
                revision=revision,
            )

        if discovery_only:
            # Discovery is complete; captioning is the enricher's job. The crop
            # bytes were staged by the caller and their key + context ride the
            # node attrs above.
            return

        # --- Offline-OCR short path (no VLM) -----------------------------
        if ocr_text is not None:
            # Precomputed sidecar OCR: the raster's baked-in text was read
            # offline (rapidocr); emit it as a chunk derived_from the
            # visual and skip captioning entirely — this tier is OCR-only
            # by design (see mmlongbench papers deployment plan).
            text = ocr_text.strip()
            if text:
                chunk_sha = hashlib.sha256(text.encode("utf-8")).hexdigest()
                chunk_id = self._canonicalize(f"chunk:{chunk_sha[:16]}")
                chunk_attrs = {
                    "chunk_id": chunk_id,
                    "content_sha256": chunk_sha,
                    "chunker_name": "ocr_text",
                    "chunk_source": "ocr_text",
                    "visual_ref": visual_id,
                }
                if page_number is not None:
                    chunk_attrs["heading_path"] = f"page {page_number}"
                chunks_this_doc.append((chunk_id, text, chunk_attrs))
                self._emit_visual_edge(
                    emit,
                    visual_edges_this_doc,
                    src=chunk_id,
                    dst=visual_id,
                    edge_type="derived_from",
                    rel_str=rel_str,
                    visual_id=visual_id,
                    revision=revision,
                    extra_src_id={"chunk_id": chunk_id},
                )
            return

        # --- Caption enrichment (best-effort; may skip) -----------------
        if not captioning_cfg.enabled or caption_svc is None:
            return
        if source_byte_count > self.max_bytes_per_file:
            # Standalone images short-circuit into this caption path before
            # `run()`'s normal `byte_count > max_bytes_per_file` oversize
            # guard, so it never fires for them. Mirror it here: skip the
            # VLM call but keep the VisualArtifact node (discovery already
            # emitted above), same as the `min_image_px` undersize case.
            log.warning(
                "DocCorpusSource: skipping caption for %s (%d bytes > max "
                "%d); VisualArtifact still emitted",
                image_label,
                source_byte_count,
                self.max_bytes_per_file,
            )
            _settle_caption_outcome("skipped", "source_too_large")
            return
        if pre_caption_failure_reason is not None:
            return
        if img is None:
            _settle_caption_outcome("failed", "decode_failed")
            return
        assert width is not None and height is not None
        if width * height < captioning_cfg.min_image_px:
            log.warning(
                "DocCorpusSource: skipping caption for %s (%dx%d px < min %d); "
                "VisualArtifact still emitted",
                image_label,
                width,
                height,
                captioning_cfg.min_image_px,
            )
            _settle_caption_outcome("skipped", "below_min_pixels")
            return
        prompt = captioning_cfg.prompt_for(self.name)

        # --- Signature-based reuse gate -----------------------------------
        # The signature is computed from the CONFIGURED model/provider (not
        # a value read off the VLM response), so it can be predicted before
        # ever calling the VLM. `CaptionResult.caption_model` /
        # `.caption_provider` are always the config's model/provider too
        # (`CaptionService` stamps them from its own construction args,
        # never from the model's reply), so this signature is identical to
        # the one a fresh call would produce.
        expected_model = captioning_cfg.model
        expected_provider = self._provider_from_model(expected_model)
        expected_signature = self._compute_vision_signature(
            content_sha256=content_sha256,
            render_sha256=render_sha256,
            caption_model=expected_model,
            caption_provider=expected_provider,
            prompt=prompt,
            captioning_cfg=captioning_cfg,
            context=context,
        )
        cached = (caption_cache or {}).get(visual_id)
        if cached is not None and cached.get("vision_signature") == expected_signature:
            cached_chunks = [
                chunk
                for chunk in cached.get("chunks", ())
                if str(chunk.get("text") or "").strip()
            ]
            if not cached_chunks:
                _settle_caption_outcome("failed", "empty_result")
                return
            upgraded_chunks: list[dict[str, Any]] = []
            for cached_chunk in cached_chunks:
                cap_text = cached_chunk["text"]
                chunk_id = self._canonicalize(_chunk_node_id(cap_text, "image_caption"))
                chunk_attrs = dict(cached_chunk["attrs"])
                # Lazy cache-format migration: never trust a pre-chunk-kind
                # content-only id, and never pay for another VLM call merely to
                # rewrite deterministic metadata.
                chunk_attrs["chunk_id"] = chunk_id
                chunk_attrs["chunk_kind"] = "image_caption"
                chunks_this_doc.append((chunk_id, cap_text, chunk_attrs))
                upgraded_chunks.append(
                    {
                        "chunk_id": chunk_id,
                        "text": cap_text,
                        "attrs": dict(chunk_attrs),
                    }
                )
                self._emit_visual_edge(
                    emit,
                    visual_edges_this_doc,
                    src=chunk_id,
                    dst=visual_id,
                    edge_type="derived_from",
                    rel_str=rel_str,
                    visual_id=visual_id,
                    revision=revision,
                    extra_src_id={"chunk_id": chunk_id},
                )
            if caption_cache_out is not None:
                caption_cache_out[visual_id] = {
                    "vision_signature": expected_signature,
                    "chunks": upgraded_chunks,
                }
            _settle_caption_outcome("reused")
            return

        caption_kwargs: dict[str, Any] = {}
        if captioning_cfg.resolved_tracking_dsn:
            # Per `req.visual-artifacts.fail-loud-config`: when VLM call
            # tracking is configured, supply a stable caller and a
            # per-visual fragment_id so `okg.llm_calls` rows resolve back
            # to this source and this exact visual, instead of raising
            # (VLMClient requires `caller` once `tracking_dsn` is set).
            caption_kwargs["caller"] = f"sources:{self.name}.caption"
            caption_kwargs["fragment_id"] = visual_id
        # The provider call is reached through a substitutable seam so it can be
        # issued OUTSIDE this function — see `caption_results` and
        # `plan_caption_call` below. Everything around it in this function emits
        # facts on the caller's connection, so the call is the only part that
        # may ever move to a worker thread.
        if caption_failure_kind is not None:
            _settle_caption_outcome(
                "failed",
                "empty_result"
                if caption_failure_kind == "empty_result"
                else "provider_error",
            )
            visual_attrs["caption_failure_kind"] = caption_failure_kind
            return
        if caption_results is not None:
            results = caption_results
        else:
            planned = self.plan_caption_call(
                img,
                context=context,
                caption_kwargs=caption_kwargs,
            )
            results = self.run_caption_call(
                caption_svc,
                planned,
                image_label=image_label,
            )
        if results is None:
            # The call failed and was already logged. The VisualArtifact stays
            # emitted and carries a stable, countable failure reason.
            _settle_caption_outcome("failed", "provider_error")
            return

        cached_chunks: list[dict[str, Any]] = []
        for cap in results:
            cap_text = cap.text or ""
            # A VLM reply can contain lone UTF-16 surrogates (bad token
            # decode on unusual glyphs). They are unencodable as UTF-8,
            # so the hash below — and the eventual Postgres write — would
            # crash the whole source run over one caption. Replace them.
            cap_text = cap_text.encode("utf-8", "replace").decode("utf-8")
            if not cap_text.strip():
                continue
            chunk_sha = hashlib.sha256(cap_text.encode("utf-8")).hexdigest()
            chunk_id = self._canonicalize(_chunk_node_id(cap_text, "image_caption"))
            chunk_attrs: dict[str, Any] = {
                "chunk_id": chunk_id,
                "chunk_kind": "image_caption",
                "content_sha256": chunk_sha,
                "chunker_name": "vision_caption",
                "chunk_source": cap.chunk_source,
                "caption_model": cap.caption_model,
                "caption_provider": cap.caption_provider,
                # Link the caption back to the exact visual it describes, so a
                # search hit on repeated caption text still resolves to its
                # own image; and stamp the recaption-reuse signature.
                "visual_ref": visual_id,
                "vision_signature": self._compute_vision_signature(
                    content_sha256=content_sha256,
                    render_sha256=render_sha256,
                    caption_model=cap.caption_model,
                    caption_provider=cap.caption_provider,
                    prompt=prompt,
                    captioning_cfg=captioning_cfg,
                    context=context,
                ),
            }
            # Persist the model's category (from a JSON captioning prompt);
            # deployments that use the plain-prose default get "other".
            # Same surrogate guard as cap_text — same VLM reply.
            if cap.category:
                chunk_attrs["category"] = cap.category.encode(
                    "utf-8", "replace"
                ).decode("utf-8")
            # Page attribution: prefer the explicit page_number (PDF page
            # renders); fall back to the ":page<N>" label form.
            if page_number is not None:
                chunk_attrs["heading_path"] = f"page {page_number}"
            else:
                _page_m = re.search(r":page(\d+)$", image_label)
                if _page_m:
                    chunk_attrs["heading_path"] = f"page {_page_m.group(1)}"
            chunks_this_doc.append((chunk_id, cap_text, chunk_attrs))
            cached_chunks.append(
                {
                    "chunk_id": chunk_id,
                    "text": cap_text,
                    "attrs": dict(chunk_attrs),
                }
            )
            self._emit_visual_edge(
                emit,
                visual_edges_this_doc,
                src=chunk_id,
                dst=visual_id,
                edge_type="derived_from",
                rel_str=rel_str,
                visual_id=visual_id,
                revision=revision,
                extra_src_id={"chunk_id": chunk_id},
            )
        if caption_cache_out is not None and cached_chunks:
            caption_cache_out[visual_id] = {
                "vision_signature": expected_signature,
                "chunks": cached_chunks,
            }
        if cached_chunks:
            _settle_caption_outcome("captioned")
        else:
            _settle_caption_outcome("failed", "empty_result")

    def _emit_image_caption_chunks(
        self,
        content: bytes,
        doc_id: str,
        rel_str: str,
        revision: dict,
        emit: Any,
        caption_svc: Any,
        captioning_cfg: "CaptioningConfig",
        chunks_this_doc: list,
        *,
        visual_descendants_this_doc: list,
        visual_edges_this_doc: list,
        source_file_id: Optional[str] = None,
        caption_cache: Optional[dict[str, dict[str, Any]]] = None,
        caption_cache_out: Optional[dict[str, dict[str, Any]]] = None,
        caption_batch: Optional[list[_PendingCaptionCall]] = None,
        caption_stats: Optional[dict[str, Any]] = None,
        caption_outcome: Optional[_CaptionCallOutcome] = None,
        oversize: Optional[tuple[int, str]] = None,
    ) -> None:
        """Handle a standalone image file: discover a VisualArtifact and
        populate chunks_this_doc with its caption.

        ``oversize`` is ``(byte_count, content_sha256)`` for a file over
        ``max_bytes_per_file``. Its bytes were never loaded, so ``content``
        holds only its first bytes: the visual is discovered from that size
        and hash, and captioning is skipped as ``source_too_large``.
        """
        if oversize is not None:
            source_byte_count, oversize_sha256 = oversize
            is_pdf = _has_pdf_magic(content)
            self._emit_caption_chunks(
                image_bytes=b"",
                doc_id=doc_id,
                rel_str=rel_str,
                image_label=rel_str,
                revision=revision,
                emit=emit,
                caption_svc=caption_svc,
                captioning_cfg=captioning_cfg,
                chunks_this_doc=chunks_this_doc,
                source_kind="standalone_pdf" if is_pdf else "standalone_image",
                visual_descendants_this_doc=visual_descendants_this_doc,
                visual_edges_this_doc=visual_edges_this_doc,
                source_file_id=source_file_id,
                page_number=_STANDALONE_PDF_PAGE_NUMBER if is_pdf else None,
                render_dpi=captioning_cfg.pdf_render_dpi if is_pdf else None,
                content_sha256_override=oversize_sha256,
                source_byte_count_override=source_byte_count,
                mime_type_override=(
                    "application/pdf"
                    if is_pdf
                    else mimetypes.guess_type(rel_str)[0]
                ),
                probe_image=False,
                caption_cache=caption_cache,
                caption_cache_out=caption_cache_out,
            )
            return
        if _has_pdf_magic(content):
            content_sha256 = hashlib.sha256(content).hexdigest()
            common_pdf_kwargs: dict[str, Any] = {
                "doc_id": doc_id,
                "rel_str": rel_str,
                "image_label": rel_str,
                "revision": revision,
                "emit": emit,
                "caption_svc": caption_svc,
                "captioning_cfg": captioning_cfg,
                "chunks_this_doc": chunks_this_doc,
                "source_kind": "standalone_pdf",
                "visual_descendants_this_doc": visual_descendants_this_doc,
                "visual_edges_this_doc": visual_edges_this_doc,
                "source_file_id": source_file_id,
                "page_number": _STANDALONE_PDF_PAGE_NUMBER,
                "content_sha256_override": content_sha256,
                "source_byte_count_override": len(content),
                "mime_type_override": "application/pdf",
                "render_dpi": captioning_cfg.pdf_render_dpi,
                "caption_cache": caption_cache,
                "caption_cache_out": caption_cache_out,
            }
            if (
                not captioning_cfg.enabled
                or caption_svc is None
                or len(content) > self.max_bytes_per_file
            ):
                # Disabled paths need discovery only; oversize source PDFs are
                # classified by `_emit_caption_chunks` before any decode gate.
                self._emit_caption_chunks(
                    image_bytes=content,
                    probe_image=False,
                    **common_pdf_kwargs,
                )
                return

            png_bytes, renderer_version, failure_reason = _render_pdf_first_page(
                content,
                dpi=captioning_cfg.pdf_render_dpi,
                image_label=rel_str,
            )
            if png_bytes is None:
                self._emit_caption_chunks(
                    image_bytes=content,
                    renderer_name=_STANDALONE_PDF_RENDERER,
                    renderer_version=renderer_version,
                    pre_caption_failure_reason=failure_reason,
                    probe_image=False,
                    **common_pdf_kwargs,
                )
                return

            render_sha256 = hashlib.sha256(png_bytes).hexdigest()
            if caption_outcome is not None:
                self._emit_caption_chunks(
                    image_bytes=png_bytes,
                    render_sha256=render_sha256,
                    renderer_name=_STANDALONE_PDF_RENDERER,
                    renderer_version=renderer_version,
                    caption_results=caption_outcome.results,
                    caption_failure_kind=caption_outcome.failure_kind,
                    **common_pdf_kwargs,
                )
                return
            if caption_batch is None:
                self._emit_caption_chunks(
                    image_bytes=png_bytes,
                    render_sha256=render_sha256,
                    renderer_name=_STANDALONE_PDF_RENDERER,
                    renderer_version=renderer_version,
                    **common_pdf_kwargs,
                )
                return

            # Predict the same signature used by `_emit_caption_chunks` so a
            # cache hit never consumes a provider call merely because this run
            # is batched.
            visual_id = self._canonicalize(f"visual:{content_sha256[:16]}")
            expected_signature = self._compute_vision_signature(
                content_sha256=content_sha256,
                render_sha256=render_sha256,
                caption_model=captioning_cfg.model,
                caption_provider=self._provider_from_model(captioning_cfg.model),
                prompt=captioning_cfg.prompt_for(self.name),
                captioning_cfg=captioning_cfg,
                context="",
            )
            cached = (caption_cache or {}).get(visual_id)
            cached_chunks = (
                cached.get("chunks", ())
                if isinstance(cached, dict)
                and cached.get("vision_signature") == expected_signature
                else ()
            )
            if cached_chunks:
                self._emit_caption_chunks(
                    image_bytes=png_bytes,
                    render_sha256=render_sha256,
                    renderer_name=_STANDALONE_PDF_RENDERER,
                    renderer_version=renderer_version,
                    **common_pdf_kwargs,
                )
                return

            from PIL import Image as _PILImage

            image = _PILImage.open(io.BytesIO(png_bytes))
            image.load()
            if image.width * image.height < captioning_cfg.min_image_px:
                if caption_stats is not None:
                    caption_stats["outcomes"][rel_str] = _CaptionCallOutcome(
                        failure_kind="below_min_pixels",
                    )
                return
            caption_kwargs: dict[str, Any] = {}
            if captioning_cfg.resolved_tracking_dsn:
                caption_kwargs["caller"] = f"sources:{self.name}.caption"
                caption_kwargs["fragment_id"] = visual_id
            planned = self.plan_caption_call(
                image,
                context="",
                caption_kwargs=caption_kwargs,
            )

            def _finish(outcome: _CaptionCallOutcome) -> None:
                if caption_stats is not None:
                    caption_stats["calls"] += 1
                    if outcome.rate_limit_hits:
                        caption_stats["rate_limit_hits"] += outcome.rate_limit_hits
                    if outcome.failure_kind is not None:
                        failures = caption_stats["failures_by_kind"]
                        failures[outcome.failure_kind] = (
                            failures.get(outcome.failure_kind, 0) + 1
                        )
                if caption_stats is not None and caption_stats.get("defer_emit"):
                    caption_stats["outcomes"][rel_str] = outcome
                    return
                self._emit_caption_chunks(
                    image_bytes=png_bytes,
                    render_sha256=render_sha256,
                    renderer_name=_STANDALONE_PDF_RENDERER,
                    renderer_version=renderer_version,
                    caption_results=outcome.results,
                    caption_failure_kind=outcome.failure_kind,
                    **common_pdf_kwargs,
                )

            caption_batch.append(
                _PendingCaptionCall(
                    caption_svc=caption_svc,
                    planned=planned,
                    image_label=rel_str,
                    finish=_finish,
                )
            )
            return

        self._emit_caption_chunks(
            image_bytes=content,
            doc_id=doc_id,
            rel_str=rel_str,
            image_label=rel_str,
            revision=revision,
            emit=emit,
            caption_svc=caption_svc,
            captioning_cfg=captioning_cfg,
            chunks_this_doc=chunks_this_doc,
            source_kind="standalone_image",
            visual_descendants_this_doc=visual_descendants_this_doc,
            visual_edges_this_doc=visual_edges_this_doc,
            source_file_id=source_file_id,
            caption_cache=caption_cache,
            caption_cache_out=caption_cache_out,
        )

    def _emit_pdf_embedded_captions(
        self,
        pdf_bytes: bytes,
        pdf_content_sha256: str,
        doc_id: str,
        rel_str: str,
        revision: dict,
        emit: Any,
        caption_svc: Any,
        captioning_cfg: "CaptioningConfig",
        chunks_this_doc: list,
        *,
        visual_descendants_this_doc: list,
        visual_edges_this_doc: list,
        source_file_id: Optional[str] = None,
        caption_cache: Optional[dict[str, dict[str, Any]]] = None,
        caption_cache_out: Optional[dict[str, dict[str, Any]]] = None,
    ) -> None:
        """Discover a PDF's visual material as VisualArtifacts.

        Inline DocLayout-YOLO figure detection is the sole PDF visual-discovery
        path: each kept figure box is cropped to a `pdf_figure` VisualArtifact
        (with bbox + confidence) and captioned. The old `get_drawings() >= 5`
        page-render heuristic and standalone embedded-raster extraction were
        removed in favor of the detector (see `figure_detector.FigureDetector`
        and `_emit_detected_box_captions`). Consequently a captioning-enabled
        deployment with PDFs MUST set `captioning.figure_detection=true`; with
        it false this raises rather than silently discovering zero figures.
        Detected `table` boxes are inert provenance — never cropped or
        captioned (their text rides the PDF text layer).
        """
        try:
            import fitz  # pymupdf
        except ImportError as exc:
            # Fail loud: captioning is enabled and this is a PDF, so silently
            # skipping embedded-image extraction would disable an enabled
            # vision mode (a default-value-as-fallback). Per
            # `req.visual-artifacts.fail-loud-config`. Issue #639: name the
            # interpreter that lacked the package so a uv-tool-shim run is
            # distinguishable from a missing install.
            from okg.substrate.support.env_provenance import (
                annotate_optional_dependency_error,
            )

            message, fix_hint, provenance = annotate_optional_dependency_error(
                f"DocCorpusSource: captioning is enabled and {rel_str} is a "
                "PDF, but pymupdf (fitz) is not installed, so PDF visual "
                "extraction cannot run.",
                "Install pymupdf (e.g. `uv pip install pymupdf`), or "
                "set captioning.enabled=false for this deployment.",
                package="pymupdf",
            )
            raise SubstrateError(
                message,
                data=provenance,
                fix_hint=fix_hint,
            ) from exc

        _renderer_version = (
            getattr(fitz, "VersionBind", "")
            or getattr(fitz, "__version__", "")
            or "unknown"
        )

        try:
            doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        except Exception:
            log.warning(
                "DocCorpusSource: fitz.open failed for %s; "
                "skipping embedded image extraction",
                rel_str,
                exc_info=True,
            )
            return

        # Inline figure detection (the only PDF visual-discovery path). A
        # captioning deployment with PDFs MUST enable figure_detection; the old
        # `get_drawings() >= 5` + embedded-raster heuristics were removed in
        # favor of DocLayout-YOLO (see figure_detector.FigureDetector). Failing
        # loud here beats silently discovering zero figures.
        if not captioning_cfg.figure_detection:
            raise SubstrateError(
                f"DocCorpusSource: captioning is enabled and {rel_str} is a "
                "PDF, but captioning.figure_detection is false, so there is no "
                "figure-discovery path.",
                fix_hint="Set captioning.figure_detection=true (requires the "
                "vision extra), or set captioning.enabled=false.",
            )
        detector = self._get_figure_detector(captioning_cfg)
        try:
            doc_boxes = detector.detect(doc)
        except SubstrateError:
            raise
        except Exception as exc:
            # Fail loud: a detector crash is a real failure, not "this doc has
            # no figures" (which is an empty list, not an exception). Silently
            # dropping every figure of a document would be a default-value-as-
            # fallback P0 (see CLAUDE.md).
            raise SubstrateError(
                f"DocCorpusSource: inline figure detection failed for {rel_str}.",
                fix_hint="Check the DocLayout-YOLO weights/model and the PDF; "
                "re-run once the detector loads cleanly.",
            ) from exc
        self._emit_detected_box_captions(
            doc,
            doc_boxes,
            pdf_content_sha256=pdf_content_sha256,
            doc_id=doc_id,
            rel_str=rel_str,
            revision=revision,
            emit=emit,
            caption_svc=caption_svc,
            captioning_cfg=captioning_cfg,
            chunks_this_doc=chunks_this_doc,
            visual_descendants_this_doc=visual_descendants_this_doc,
            visual_edges_this_doc=visual_edges_this_doc,
            renderer_version=_renderer_version,
            source_file_id=source_file_id,
            caption_cache=caption_cache,
            caption_cache_out=caption_cache_out,
        )

    def _get_figure_detector(self, captioning_cfg: "CaptioningConfig") -> Any:
        """Build (and memoize) the inline DocLayout-YOLO FigureDetector.

        One instance per source — the model loads lazily on the first
        `detect()` and is reused across every PDF in the corpus.
        """
        if self.figure_detector is not None:
            return self.figure_detector
        detector = getattr(self, "_built_figure_detector", None)
        if detector is None:
            from okg.substrate.library.captioning.figure_detector import (
                FigureDetector,
                require_doclayout_yolo,
            )

            # Fail loud at ingest start if the vision extra is absent, rather
            # than silently discovering zero figures (a default-value-as-
            # fallback P0 — see CLAUDE.md). Skipped when a detector is injected
            # (tests supply a stub). find_spec does not import torch.
            require_doclayout_yolo()
            detector = FigureDetector(
                weights=captioning_cfg.figure_detection_weights,
            )
            self._built_figure_detector = detector
        return detector

    def _get_crop_store(self) -> Any:
        """Return the figure-crop store (injected, or a DsnCropStore on the
        source dsn). Memoized so one DsnCropStore's ensure-once flag and pooled
        connection are reused across every PDF in the corpus.

        A missing dsn is a loud construction error inside DsnCropStore — the
        run-start guard already refuses figure_detection without a dsn or an
        injected store, so this is the belt-and-suspenders half of that
        contract (no silent no-op crop staging).
        """
        if self.figure_crop_store is not None:
            return self.figure_crop_store
        store = getattr(self, "_built_crop_store", None)
        if store is None:
            from okg.substrate.library.captioning.crop_store import DsnCropStore

            store = DsnCropStore(self.dsn)
            self._built_crop_store = store
        return store

    def _emit_detected_box_captions(
        self,
        doc: Any,
        doc_boxes: Sequence[dict[str, Any]],
        *,
        pdf_content_sha256: str,
        doc_id: str,
        rel_str: str,
        revision: dict,
        emit: Any,
        caption_svc: Any,
        captioning_cfg: "CaptioningConfig",
        chunks_this_doc: list,
        visual_descendants_this_doc: list,
        visual_edges_this_doc: list,
        renderer_version: str,
        source_file_id: Optional[str] = None,
        caption_cache: Optional[dict[str, dict[str, Any]]] = None,
        caption_cache_out: Optional[dict[str, dict[str, Any]]] = None,
        doc_ocr: Sequence[dict[str, Any]] = (),
    ) -> None:
        """Emit VisualArtifacts + captions for a detected box list.

        `doc_boxes` is the FigureDetector output (same schema the old
        figures.json sidecar used):

        - `kind: "figure"` boxes → crop via
          `page.get_pixmap(clip=fitz.Rect(bbox))` → VLM caption with document
          context (the box's anchored `caption_text`, else the page text). The
          bbox is part of the content identity so two same-page crops don't
          collapse onto one visual.
        - `kind: "table"` boxes are inert provenance — their text rides the PDF
          text layer (PdfTextParser), so they are neither cropped nor captioned.

        `doc_ocr` (optional embedded-raster OCR entries) → each becomes a
        VisualArtifact whose OCR'd text is emitted as an `ocr_text` chunk
        `derived_from` it, with no VLM call. Inline detection passes none;
        the tier is retained for callers that supply OCR out of band.
        """
        import fitz  # pymupdf — caller already proved it importable

        # Phase 1: crop each detected figure box and stage its emit args.
        #
        # There is NO max_images_per_doc cap here anymore (the caption-enricher
        # split, PACT B). Discovery emits a visual_artifact for EVERY kept box;
        # the cap now gates the enricher's VLM call, not discovery — so a
        # capped deployment no longer silently drops kept boxes from the graph
        # (PR #860 finding F3).
        figure_tasks: list[dict[str, Any]] = []
        for box in doc_boxes:
            if box.get("kind", "figure") != "figure":
                continue  # tables et al.: inert provenance only
            # Validate the detector output before cropping. A kept figure box
            # that is malformed or cannot be cropped must fail loud, not
            # silently vanish from the graph: req.inline-detector wants one
            # visual_artifact per kept box, and a swallowed crop is the same
            # default-value-as-fallback P0 (CLAUDE.md) as a swallowed detector
            # crash — which is already loud above.
            try:
                page_num = int(box["page"])  # 1-based
                bbox = [float(v) for v in box["bbox"]]
            except (KeyError, TypeError, ValueError) as exc:
                raise SubstrateError(
                    f"DocCorpusSource: inline figure detection returned a "
                    f"malformed box for {rel_str}: {box!r}.",
                    fix_hint="A kept figure box needs an int `page` (1-based) "
                    "and a 4-float `bbox`; check the FigureDetector "
                    "output schema.",
                ) from exc
            if not 1 <= page_num <= len(doc) or len(bbox) != 4:
                raise SubstrateError(
                    f"DocCorpusSource: inline figure detection returned an "
                    f"out-of-range box for {rel_str}: page={page_num}, "
                    f"bbox={bbox} (doc has {len(doc)} pages).",
                    fix_hint="A kept figure box must reference a real page and "
                    "a 4-coordinate bbox; check the detector filter.",
                )
            try:
                page = doc[page_num - 1]
                # 2x matrix ≈ 144 dpi — matches the page-render path and
                # keeps small figure crops above min_image_px.
                pix = page.get_pixmap(
                    matrix=fitz.Matrix(2.0, 2.0),
                    clip=fitz.Rect(bbox),
                )
                img_bytes = pix.tobytes("png")
            except Exception as exc:
                raise SubstrateError(
                    f"DocCorpusSource: failed to crop kept figure box {box!r} "
                    f"of {rel_str}.",
                    fix_hint="A kept figure box that cannot be cropped is a "
                    "real failure, not zero figures; check the bbox "
                    "and the PDF page.",
                ) from exc
            context = str(box.get("caption_text") or "").strip()
            if not context:
                # No anchored caption — ground the VLM in the page text.
                try:
                    context = (page.get_text() or "").strip()
                except Exception:
                    context = ""
            # PyMuPDF page text can carry lone UTF-16 surrogates (broken
            # ToUnicode CMaps — e.g. the 1883-era scan in mmlongbench).
            # They are unencodable as UTF-8 and would crash the signature
            # hash / VLM request; replace rather than fail the whole run.
            context = context.encode("utf-8", "replace").decode("utf-8")
            bbox_key = ",".join(f"{v:.2f}" for v in bbox)
            # bbox belongs to the content identity: two crops from the
            # same page must be distinct visuals even if their rendered
            # bytes were ever to collide.
            crop_content_sha256 = hashlib.sha256(
                f"{pdf_content_sha256}:page{page_num}:bbox[{bbox_key}]".encode("utf-8"),
            ).hexdigest()
            visual_id = self._canonicalize(f"visual:{crop_content_sha256[:16]}")
            figure_tasks.append(
                {
                    "box": box,
                    "img_bytes": img_bytes,
                    "context": context,
                    "page_num": page_num,
                    "bbox": bbox,
                    "bbox_key": bbox_key,
                    "crop_content_sha256": crop_content_sha256,
                    "render_sha256": hashlib.sha256(img_bytes).hexdigest(),
                    "visual_id": visual_id,
                }
            )

        # Phase 2 (discovery only): stage each crop into okg.visual_crop_blobs
        # and emit its visual_artifact discovery node — NO VLM call. Captioning
        # is owned by the VisualCaptionEnricher (Phase B), which reads the crop
        # back by its render_sha256 key and captions it with parallel fan-out.
        # The source no longer captions figures at all, so caption_svc /
        # caption_cache are unused on this path.
        crop_store = self._get_crop_store()
        for t in figure_tasks:
            # Stage the crop bytes BEFORE emitting the node, so a published
            # visual never references a blob that was not persisted.
            crop_store.put(t["render_sha256"], t["img_bytes"])
            self._emit_caption_chunks(
                image_bytes=t["img_bytes"],
                doc_id=doc_id,
                rel_str=rel_str,
                image_label=(f"{rel_str}:page{t['page_num']}:bbox[{t['bbox_key']}]"),
                revision=revision,
                emit=emit,
                caption_svc=None,
                captioning_cfg=captioning_cfg,
                chunks_this_doc=chunks_this_doc,
                source_kind="pdf_figure",
                visual_descendants_this_doc=visual_descendants_this_doc,
                visual_edges_this_doc=visual_edges_this_doc,
                source_file_id=source_file_id,
                page_number=t["page_num"],
                content_sha256_override=t["crop_content_sha256"],
                render_sha256=t["render_sha256"],
                renderer_name="pymupdf",
                renderer_version=renderer_version,
                context=t["context"],
                extra_visual_attrs={
                    "bbox": json.dumps(t["bbox"]),
                    "confidence": float(t["box"].get("conf", 0.0)),
                },
                discovery_only=True,
                crop_blob_ref=t["render_sha256"],
            )

        # OCR tier: one VisualArtifact + ocr_text chunk per unique raster.
        # ocr.json is keyed per placement; the same xref placed on several
        # pages carries identical text, so dedupe here.
        seen_xrefs: set[int] = set()
        for entry in doc_ocr:
            text = str(entry.get("text") or "").strip()
            # Same surrogate guard as `context` above: json.loads will
            # happily materialize lone surrogates from \udXXX escapes.
            text = text.encode("utf-8", "replace").decode("utf-8")
            if not text:
                continue
            try:
                xref = int(entry["xref"])
                page_num = int(entry["page"])
            except (KeyError, TypeError, ValueError):
                log.warning(
                    "DocCorpusSource: malformed ocr.json entry %s for %s",
                    entry,
                    rel_str,
                )
                continue
            if xref in seen_xrefs:
                continue
            seen_xrefs.add(xref)
            try:
                img_bytes = doc.extract_image(xref)["image"]
            except Exception:
                log.warning(
                    "DocCorpusSource: failed to extract OCR raster xref=%d of %s",
                    xref,
                    rel_str,
                    exc_info=True,
                )
                continue
            extra_attrs: dict[str, Any] = {}
            if entry.get("bbox"):
                extra_attrs["bbox"] = json.dumps(
                    [float(v) for v in entry["bbox"]],
                )
            self._emit_caption_chunks(
                image_bytes=img_bytes,
                doc_id=doc_id,
                rel_str=rel_str,
                image_label=f"{rel_str}:page{page_num}:xref{xref}",
                revision=revision,
                emit=emit,
                caption_svc=caption_svc,
                captioning_cfg=captioning_cfg,
                chunks_this_doc=chunks_this_doc,
                source_kind="pdf_embedded_image",
                visual_descendants_this_doc=visual_descendants_this_doc,
                visual_edges_this_doc=visual_edges_this_doc,
                source_file_id=source_file_id,
                page_number=page_num,
                ocr_text=text,
                extra_visual_attrs=extra_attrs or None,
            )
