"""TalksSource — substrate-library source for talk transcripts.

Reads an operator-curated talks YAML file and emits `Paper`
(kind=talk, tier=1), `Person` (speaker), and `Document` +
`DocumentChunk` NodeFacts for the chunked transcript text. The
YouTube transcript is fetched via `youtube_transcript_api` (optional
dep) and cached at `transcript_cache_root/<youtube_id>.json` so
subsequent runs do not re-fetch.

Per `add-substrate-bibliographic-sources` (Phase 3 of impl-A4).

Talks YAML shape (a list of entries):

    - title: "What Comes After the Relational Model"
      speaker: "Morgan Vale"
      venue: "VLDB Keynote"
      year: 2023
      youtube_url: "https://www.youtube.com/watch?v=ABCDEFGHIJK"
      annote: "tier: 1, auto_extract: true"

Substrate-library purity: no memex paths. Deployments configure
`talks_yaml_path` and `transcript_cache_root` in source_registry.yaml.
Transcript fetch failures fail loudly (no silent partial emission).
"""
from __future__ import annotations
from okg.substrate import fastyaml

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Iterator, Optional

from okg.substrate.errors import SubstrateError

from ._common import slugify
from .bib_references import _parse_annote
from okg.substrate.library.sources.base import (
    EdgeFact,
    NodeFact,
    SourceRun,
)


__all__ = ["TalksSource", "TranscriptFetchError"]


_YOUTUBE_ID_RE = re.compile(
    r"(?:youtu\.be/|v=|/embed/|/shorts/)([A-Za-z0-9_-]{11})"
)


class TranscriptFetchError(SubstrateError):
    """Raised when a YouTube transcript cannot be fetched.

    Per A4 fail-loud convention: TalksSource does NOT silently emit
    a Paper without its transcript chunks. A failed fetch aborts
    the entry.
    """

    code = "transcript_fetch_error"


def _extract_youtube_id(url: str) -> Optional[str]:
    m = _YOUTUBE_ID_RE.search(url)
    return m.group(1) if m else None


def _talk_node_id(entry: dict[str, Any]) -> str:
    youtube_id = _extract_youtube_id(entry.get("youtube_url", "") or "")
    if youtube_id:
        return f"paper:talk:youtube:{youtube_id}"
    slug = slugify(entry.get("title", "untitled-talk"))
    return f"paper:talk:{slug}"


def _person_node_id(name: str) -> str:
    return f"person:{slugify(name)}"


def _document_node_id(paper_id: str) -> str:
    return f"doc:{paper_id}"


def _chunk_node_id(paper_id: str, chunk_index: int) -> str:
    return f"chunk:{paper_id}:{chunk_index}"


def _chunk_text(text: str, *, window: int = 4000, overlap: int = 200) -> list[tuple[int, int, str]]:
    """Return list of (chunk_index, char_offset, chunk_text) tuples."""
    out: list[tuple[int, int, str]] = []
    step = max(1, window - overlap)
    idx = 0
    pos = 0
    while pos < len(text):
        out.append((idx, pos, text[pos:pos + window]))
        idx += 1
        pos += step
    return out


def _default_transcript_fetcher(youtube_id: str) -> list[dict[str, Any]]:
    """Fetch transcript segments via `youtube_transcript_api`.

    Returns the raw segment list (each segment has `text`, `start`,
    `duration` keys). Raises TranscriptFetchError on any failure.
    """
    try:
        from youtube_transcript_api import (  # type: ignore[import-not-found]
            YouTubeTranscriptApi,
        )
    except ImportError as exc:
        # Issue #639: name the interpreter that lacked the package so a
        # uv-tool-shim run is distinguishable from a missing install.
        from okg.substrate.support.env_provenance import (
            annotate_optional_dependency_error,
        )
        message, fix_hint, provenance = annotate_optional_dependency_error(
            "TalksSource: youtube_transcript_api is required to fetch "
            "transcripts but is not installed",
            (
                "Install with `uv pip install youtube-transcript-api` "
                "or add to pyproject.toml. Alternatively, inject a "
                "custom fetcher via TalksSource(transcript_fetcher=...)."
            ),
            package="youtube-transcript-api",
        )
        raise SubstrateError(
            message,
            data=provenance,
            fix_hint=fix_hint,
        ) from exc
    try:
        return YouTubeTranscriptApi.get_transcript(youtube_id)
    except Exception as exc:
        raise TranscriptFetchError(
            f"TalksSource: failed to fetch transcript for {youtube_id!r}",
            fix_hint=(
                "Verify the YouTube URL is correct and the video has "
                "captions. Check network access."
            ),
            data={"youtube_id": youtube_id, "underlying_error": str(exc)},
        ) from exc


@dataclass
class TalksSource:
    talks_yaml_path: str | Path | None = None
    transcript_cache_root: str | Path | None = None
    chunk_window: int = 4000
    chunk_overlap: int = 200
    transcript_fetcher: Optional[Callable[[str], list[dict[str, Any]]]] = None
    target_subtypes: list[str] | tuple[str, ...] = (
        "paper",
        "person",
        "document",
        "document_chunk",
    )
    status: str = "enabled"
    last_run_stats: dict[str, Any] = field(default_factory=dict, init=False)

    name: str = "talks"
    profile: str = "discovery_crawl"

    # Incremental-source contract (issue #323): TalksSource reads a local talks
    # YAML file and emits Paper/Person facts from it, plus Document/DocumentChunk
    # facts whose text is the YouTube transcript. Transcripts are *cached* at
    # `transcript_cache_root/<youtube_id>.json` and, once cached, are read from
    # disk and never re-fetched — so with caching enabled the emitted facts are a
    # pure function of two LOCAL byte sources: the talks YAML and the cached
    # transcript JSON files. The probe is therefore the content-hash floor over
    # BOTH, a membership-sensitive (rel_key, sha256(bytes)) digest that is
    # mtime-churn-immune yet busts on every emission-determining mutation: a YAML
    # edit (entry text shifts), an added/removed talk entry (YAML bytes + count),
    # and any cached-transcript add / edit / delete. A brand-new talk forces a
    # YAML change, which busts the token, so its first transcript fetch is never
    # missed. `chunk_window` / `chunk_overlap` reach the emitted chunk boundaries
    # and fold into config. No `now()`-derived value reaches an emitted attr
    # (verified: no datetime/now use in this module or its emit dependencies), so
    # the content floor is time-sound. RESIDUAL GAP (reported as a concern, not
    # silently shipped): when `transcript_cache_root` is None the source
    # re-fetches transcripts live every run, and a remote re-captioning that does
    # not change the YAML is invisible to a content probe — but it is *also*
    # invisible to the source under caching (the cache is never invalidated), so
    # the probe matches the source's own observable behavior. The ungated
    # reconcile pass remains the catch-all for the no-cache live-mutation case.
    change_probe_kind = "content_hash"

    def __post_init__(self) -> None:
        if not self.talks_yaml_path:
            raise SubstrateError(
                "TalksSource: talks_yaml_path is required",
                fix_hint=(
                    "Set `talks_yaml_path: <path>` in the deployment's "
                    "source_registry.yaml entry."
                ),
            )
        self.change_probe = self._build_change_probe()

    def _build_change_probe(self) -> Any:
        import sys

        from okg.substrate.library.sources.content_hash_probe import (
            ContentHashProbe,
            file_content_items,
        )

        def _items() -> list[tuple[str, Path]]:
            # Reuse the source's OWN read scope so the probe hashes exactly the
            # bytes `run()` consumes: the talks YAML file plus every cached
            # transcript JSON. A vanished YAML or a deleted cache file is simply
            # absent here, which the membership digest reads as a delete (never
            # swallowed to false-unchanged).
            items: list[tuple[str, Path]] = []
            if self.talks_yaml_path:
                yaml_path = Path(self.talks_yaml_path).expanduser()
                items.append((f"yaml:{yaml_path}", yaml_path))
            if self.transcript_cache_root:
                cache_root = Path(self.transcript_cache_root).expanduser()
                for key, path in file_content_items(
                    cache_root, include_globs=("*.json",)
                ):
                    items.append((f"cache:{key}", path))
            return items

        from okg.substrate.library.sources.scholarly import _common

        return ContentHashProbe(
            content_items=_items,
            config={
                "talks_yaml_path": str(self.talks_yaml_path),
                "transcript_cache_root": (
                    str(self.transcript_cache_root)
                    if self.transcript_cache_root
                    else None
                ),
                "chunk_window": self.chunk_window,
                "chunk_overlap": self.chunk_overlap,
            },
            # `_common.slugify` shapes the emitted node ids (talk:/person:), so a
            # change to its normalization is an emission-logic change; fold the
            # `_common` module into emit_version so it busts the token without a
            # manual bump (else a slugify change with unchanged YAML is a
            # false-unchanged).
            emit_targets=[sys.modules[__name__], _common],
        )

    def _load_yaml(self) -> list[dict[str, Any]]:
        import yaml

        path = Path(self.talks_yaml_path).expanduser()
        if not path.is_file():
            raise SubstrateError(
                f"TalksSource: talks_yaml_path {str(path)!r} does not "
                f"resolve to a file",
                fix_hint="Verify the path; supports `~` expansion.",
                data={"talks_yaml_path": str(path)},
            )
        try:
            data = fastyaml.safe_load(path.read_text(encoding="utf-8"))
        except yaml.YAMLError as exc:
            raise SubstrateError(
                f"TalksSource: malformed YAML at {str(path)!r}: {exc}",
                fix_hint="Lint the YAML file before retrying.",
                data={"talks_yaml_path": str(path), "yaml_error": str(exc)},
            ) from exc
        if not isinstance(data, list):
            raise SubstrateError(
                f"TalksSource: talks YAML at {str(path)!r} must be a "
                f"list of talk entries, got {type(data).__name__}",
                fix_hint="Wrap entries in a top-level YAML list (`- ...`).",
            )
        return data

    def _fetch_or_cache_transcript(
        self, youtube_id: str, cache_root: Optional[Path],
    ) -> list[dict[str, Any]]:
        cache_path: Optional[Path] = None
        if cache_root is not None:
            cache_root.mkdir(parents=True, exist_ok=True)
            cache_path = cache_root / f"{youtube_id}.json"
            if cache_path.is_file():
                return json.loads(cache_path.read_text(encoding="utf-8"))
        fetcher = self.transcript_fetcher or _default_transcript_fetcher
        segments = fetcher(youtube_id)
        if cache_path is not None:
            cache_path.write_text(
                json.dumps(segments, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        return segments

    def run(
        self,
        run_id: str,
        *,
        mode: str = "cursor",
        cursor: Optional[dict[str, Any]] = None,
        sync_scope: Optional[dict[str, Any]] = None,
    ) -> SourceRun:
        stats: dict[str, Any] = {
            "talks": 0,
            "transcripts_fetched": 0,
            "transcripts_cached": 0,
            "chunks": 0,
        }
        entries = self._load_yaml()
        cache_root = (
            Path(self.transcript_cache_root).expanduser()
            if self.transcript_cache_root
            else None
        )

        def _facts() -> Iterator[Any]:
            seen_persons: set[str] = set()
            for entry in entries:
                paper_id = _talk_node_id(entry)
                annote = _parse_annote(entry.get("annote"))
                tier = int(annote.get("tier", 1))
                auto_extract = bool(annote.get("auto_extract", False))
                title = (entry.get("title") or "").strip()
                speaker = (entry.get("speaker") or "").strip()
                venue = (entry.get("venue") or "").strip()
                year = str(entry.get("year") or "").strip()
                youtube_url = (entry.get("youtube_url") or "").strip()
                youtube_id = _extract_youtube_id(youtube_url)

                paper_attrs: dict[str, Any] = {
                    "title": title,
                    "kind": "talk",
                    "tier": tier,
                }
                if auto_extract:
                    paper_attrs["auto_extract"] = True
                if venue:
                    paper_attrs["venue"] = venue
                if year:
                    paper_attrs["year"] = year
                if youtube_url:
                    paper_attrs["youtube_url"] = youtube_url
                if youtube_id:
                    paper_attrs["youtube_id"] = youtube_id

                yield NodeFact(
                    node_id=paper_id,
                    subtype="paper",
                    attrs=paper_attrs,
                    source_record_id={
                        "talks_yaml_path": str(self.talks_yaml_path),
                        "paper_id": paper_id,
                    },
                    source_revision={"run_id": run_id},
                    op="I",
                )
                stats["talks"] += 1

                if speaker:
                    pid = _person_node_id(speaker)
                    if pid not in seen_persons:
                        seen_persons.add(pid)
                        yield NodeFact(
                            node_id=pid,
                            subtype="person",
                            attrs={"name": speaker},
                            source_record_id={"speaker": speaker},
                            source_revision={"run_id": run_id},
                            op="I",
                        )
                    yield EdgeFact(
                        src=paper_id,
                        dst=pid,
                        edge_type="authored_by",
                        source_record_id={"paper_id": paper_id, "speaker": speaker},
                        source_revision={"run_id": run_id},
                    )

                if not youtube_id:
                    # No transcript path for this entry; emit Paper only.
                    continue

                # Determine cache hit / miss BEFORE fetch, for stats.
                cache_hit = (
                    cache_root is not None
                    and (cache_root / f"{youtube_id}.json").is_file()
                )
                segments = self._fetch_or_cache_transcript(youtube_id, cache_root)
                if cache_hit:
                    stats["transcripts_cached"] += 1
                else:
                    stats["transcripts_fetched"] += 1
                transcript_text = " ".join(
                    str(seg.get("text") or "").strip() for seg in segments
                ).strip()

                doc_id = _document_node_id(paper_id)
                yield NodeFact(
                    node_id=doc_id,
                    subtype="document",
                    attrs={
                        "title": title,
                        "source_kind": "talk_transcript",
                        "youtube_id": youtube_id,
                    },
                    source_record_id={"paper_id": paper_id, "youtube_id": youtube_id},
                    source_revision={"run_id": run_id},
                    op="I",
                )
                yield EdgeFact(
                    src=paper_id,
                    dst=doc_id,
                    edge_type="contains",
                    source_record_id={"paper_id": paper_id, "youtube_id": youtube_id},
                    source_revision={"run_id": run_id},
                )

                for chunk_index, char_offset, chunk_text in _chunk_text(
                    transcript_text,
                    window=self.chunk_window,
                    overlap=self.chunk_overlap,
                ):
                    chunk_id = _chunk_node_id(paper_id, chunk_index)
                    yield NodeFact(
                        node_id=chunk_id,
                        subtype="document_chunk",
                        attrs={
                            "text": chunk_text,
                            "char_offset": char_offset,
                            "chunk_index": chunk_index,
                            "chunk_kind": "transcript",
                        },
                        source_record_id={
                            "paper_id": paper_id,
                            "youtube_id": youtube_id,
                            "chunk_index": chunk_index,
                        },
                        source_revision={"run_id": run_id},
                        op="I",
                    )
                    yield EdgeFact(
                        src=doc_id,
                        dst=chunk_id,
                        edge_type="contains",
                        source_record_id={
                            "paper_id": paper_id,
                            "youtube_id": youtube_id,
                            "chunk_index": chunk_index,
                        },
                        source_revision={"run_id": run_id},
                    )
                    stats["chunks"] += 1

        self.last_run_stats = stats
        return SourceRun(
            facts=_facts(),
            completed_scope=(mode in {"scope_complete", "reconcile"}),
            run_mode=mode,
        )
