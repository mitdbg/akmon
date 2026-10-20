"""ForgeReferenceLinker — `#N` / "fixes #N" / full-URL references from
commit messages, issue bodies and PR bodies to the issue or PR they name.

Why this is not a `DeclarativeLinker` rule. The declarative
`derive_from_path` matcher can extract `#(\\d+)` and look the capture up
against `number`, and it would emit the same edges. What it cannot do is
COUNT what it failed to resolve: it drops a non-matching capture silently,
so a corpus whose reference targets all fell outside the cutoff would look
exactly like a corpus with no references at all. The umbrella
(`okg-corpus-design`) fixes the opposite contract — unresolvable references
are SKIPPED AND COUNTED, never a stub, never a dangling edge, and zero
resolved references in an input large enough to contain them FAILS. That
requires a linker that keeps the books, which is this one.

What it emits:

  issue|pull_request -references-> issue|pull_request
  git_commit         -references-> issue|pull_request

Resolution is closed over the published generation: a capture resolves only
if a node with that `number` is live in this graph. A reference to an
excluded or post-cutoff item resolves to nothing and is counted under
`unresolved_out_of_corpus` — emitting a stub node for it would put a gold
identifier into the retrieval candidate pool the benchmark scores, and
emitting the edge anyway would dangle and block the publish.

Per PACT `okg-corpus-repo-history` / req.repo.linkage.
"""
from __future__ import annotations

import re
import time
from typing import Optional

import psycopg

from okg.substrate.enrichers.base import EnrichResult, IncrementalContext
from okg.substrate.enrichers.derived_edges import (
    DerivedEdgeCandidate,
    insert_deterministic_edges,
    stable_edge_dedupe_key,
)
from okg.substrate.errors import SubstrateError
from okg.substrate.library.linkers import _chronos


__all__ = [
    "ForgeReferenceLinker",
    "ForgeMergeAttributionLinker",
    "ForgeReferencesUnresolved",
    "extract_refs",
    "MERGE_PR_SUBJECT",
]


class ForgeReferencesUnresolved(SubstrateError):
    """Raised when candidate references exist but NONE resolve.

    "No matches" must not be able to masquerade as "none exist": a body
    corpus full of `#N` tokens that resolves to zero live nodes means the
    number index is wrong (wrong subtype, wrong attr, wrong generation),
    not that the maintainers never cross-referenced anything.
    """

    code = "forge_references_unresolved"


#: Bare `#123`, and `fixes #123` / `closes #123` (the verb is not required —
#: it only changes the SEMANTICS of the reference, not whether one exists).
#: `(?<![\w#])` keeps `abc#12`, `##12` and `#12` inside a longer token out.
_HASH_REF = re.compile(r"(?<![\w#/])#(\d+)\b")

#: A same-repo web or API URL: `https://github.com/psf/requests/issues/123`,
#: `.../pull/123`, `.../pulls/123`. The repo slug is bound at match time so a
#: link to ANOTHER repository's #123 cannot resolve to this corpus's #123 —
#: silently linking across repositories would fabricate a relationship the
#: source never asserted.
_URL_REF_TEMPLATE = (
    r"https?://(?:www\.)?(?:api\.)?[\w.-]*{host}[\w.-]*/(?:repos/)?"
    r"{slug}/(?:issues|pull|pulls)/(\d+)\b"
)

#: `GH-123` — the other conventional same-repo form in commit subjects.
_GH_REF = re.compile(r"(?<![\w-])GH-(\d+)\b", re.IGNORECASE)


def extract_refs(text: str, *, url_pattern: re.Pattern[str] | None) -> set[int]:
    """Every same-repo issue/PR number `text` names. Pure; unit-testable
    without a database."""
    if not text:
        return set()
    out = {int(n) for n in _HASH_REF.findall(text)}
    out |= {int(n) for n in _GH_REF.findall(text)}
    if url_pattern is not None:
        out |= {int(n) for n in url_pattern.findall(text)}
    return out


class ForgeReferenceLinker:
    name: str = "forge_references"
    reads_subtypes: tuple[str, ...] = (
        "issue", "pull_request", "git_commit",
    )
    reads_edge_types: tuple[str, ...] = ()
    emits_edge_types: tuple[str, ...] = ("references",)
    requires_narrowings: tuple[tuple[str, str, str], ...] = (
        ("git_commit", "references", "issue"),
        ("git_commit", "references", "pull_request"),
        ("issue", "references", "issue"),
        ("issue", "references", "pull_request"),
        ("pull_request", "references", "issue"),
        ("pull_request", "references", "pull_request"),
    )
    dormant_ok: bool = False
    nondeterministic: bool = False

    #: (subtype, attrs to scan). `message_subject` is what GitHistorySource
    #: puts on a commit; `title`/`body` are the forge item text.
    _SCAN: tuple[tuple[str, tuple[str, ...]], ...] = (
        ("git_commit", ("message_subject",)),
        ("issue", ("title", "body")),
        ("pull_request", ("title", "body")),
    )

    def __init__(
        self,
        *,
        name: Optional[str] = None,
        repo_slug: Optional[str] = None,
        forge_host: str = "github",
        fail_on_zero_resolved: bool = True,
    ) -> None:
        """`repo_slug` (``owner/name``) scopes full-URL references to THIS
        repository. Without it, URL forms are not matched at all — the
        conservative choice, since an unscoped URL regex would happily
        resolve another project's `#123` onto this corpus's node."""
        if name is not None:
            self.name = name
        self.repo_slug = repo_slug
        self.forge_host = forge_host
        self.fail_on_zero_resolved = bool(fail_on_zero_resolved)
        self._url_pattern: re.Pattern[str] | None = None
        if repo_slug:
            self._url_pattern = re.compile(
                _URL_REF_TEMPLATE.format(
                    host=re.escape(forge_host),
                    slug=re.escape(repo_slug.strip("/")),
                ),
                re.IGNORECASE,
            )

    def can_skip_clean(
        self, conn: psycopg.Connection, ctx: IncrementalContext,
    ) -> bool:
        del conn, ctx
        return True

    def enrich(
        self,
        conn: psycopg.Connection,
        *,
        generation_id: int,
        incremental: IncrementalContext | None = None,
    ) -> EnrichResult:
        started = time.perf_counter()
        timings_ms: dict[str, float] = {}
        with conn.cursor() as cur:
            targets = self._number_index(cur)
            candidates: list[DerivedEdgeCandidate] = []
            stats = {
                "citing_nodes_scanned": 0,
                "citing_nodes_with_refs": 0,
                "refs_seen": 0,
                "resolved": 0,
                "unresolved_out_of_corpus": 0,
                "self_reference_dropped": 0,
            }
            unresolved_sample: set[int] = set()
            for subtype, fields in self._SCAN:
                for node_id, text in self._texts(cur, subtype, fields):
                    stats["citing_nodes_scanned"] += 1
                    refs = extract_refs(text, url_pattern=self._url_pattern)
                    if not refs:
                        continue
                    stats["citing_nodes_with_refs"] += 1
                    for number in sorted(refs):
                        stats["refs_seen"] += 1
                        dst = targets.get(number)
                        if dst is None:
                            # Excluded, post-cutoff, or simply never
                            # existed. Counted, never stubbed.
                            stats["unresolved_out_of_corpus"] += 1
                            if len(unresolved_sample) < 20:
                                unresolved_sample.add(number)
                            continue
                        if dst == node_id:
                            stats["self_reference_dropped"] += 1
                            continue
                        stats["resolved"] += 1
                        candidates.append(
                            DerivedEdgeCandidate(
                                src=node_id,
                                edge_type="references",
                                dst=dst,
                                attrs={"reference_number": number},
                                source_record_id={
                                    "src_node_id": node_id,
                                    "dst_node_id": dst,
                                    "linker": self.name,
                                    "reference_number": number,
                                },
                                source_revision={
                                    "generation_id": generation_id,
                                },
                                dedupe_key=stable_edge_dedupe_key(
                                    source=f"_enricher:{self.name}",
                                    src=node_id,
                                    edge_type="references",
                                    dst=dst,
                                    attrs={"reference_number": number},
                                ),
                            )
                        )
            _record(timings_ms, "scan", started)

            if (
                self.fail_on_zero_resolved
                and stats["refs_seen"]
                and not stats["resolved"]
            ):
                raise ForgeReferencesUnresolved(
                    f"{self.name}: scanned {stats['citing_nodes_scanned']} "
                    f"node(s), found {stats['refs_seen']} reference token(s) "
                    f"across {stats['citing_nodes_with_refs']} node(s), and "
                    f"resolved NONE of them against "
                    f"{len(targets)} numbered forge item(s) in this "
                    f"generation",
                    data={
                        **stats,
                        "numbered_targets": len(targets),
                        "unresolved_sample": sorted(unresolved_sample),
                    },
                    fix_hint=(
                        "check that issue/pull_request nodes are published "
                        "with an integer `number` attr in this generation; a "
                        "graph with commits but no forge items resolves "
                        "nothing by construction"
                    ),
                )

            if not candidates:
                return EnrichResult(
                    enricher_name=self.name,
                    n_edges_emitted=0,
                    timings_ms=timings_ms,
                    output_lineage={"reference_counts": stats},
                )
            started = time.perf_counter()
            result = insert_deterministic_edges(
                cur, source=f"_enricher:{self.name}", candidates=candidates,
            )
            _record(timings_ms, "insert", started)
        return EnrichResult(
            enricher_name=self.name,
            n_edges_emitted=result.inserted,
            n_edges_skipped=result.skipped,
            timings_ms=timings_ms,
            sanitized_value_count=result.sanitized_value_count,
            output_lineage={
                "reference_counts": stats,
                "unresolved_sample": sorted(unresolved_sample),
            },
        )

    # -- graph reads ----------------------------------------------------

    @staticmethod
    def _number_index(cur) -> dict[int, str]:
        """`number` -> node_id over live issue + pull_request nodes.

        GitHub numbers issues and PRs from ONE sequence, so a number
        identifies at most one of them and the flat map is sound. A
        collision would mean two live nodes claim one number, which is a
        corrupt graph, not a resolution ambiguity — so it raises.
        """
        out: dict[int, str] = {}
        for row in _chronos.query(
            cur,
            """
            SELECT node_id, subtype, attrs
              FROM okg.graph_nodes
             WHERE subtype = ANY(:subtypes)
            """,
            {"subtypes": ["issue", "pull_request"]},
        ):
            raw = (row["attrs"] or {}).get("number")
            if raw is None:
                continue
            try:
                number = int(raw)
            except (TypeError, ValueError):
                continue
            existing = out.get(number)
            node_id = str(row["node_id"])
            if existing is not None and existing != node_id:
                raise ForgeReferencesUnresolved(
                    f"two live forge nodes claim number {number}: "
                    f"{existing!r} and {node_id!r} — issue and PR numbers "
                    f"share one sequence per repository, so this graph mixes "
                    f"two repositories under one forge namespace",
                    data={"number": number, "nodes": [existing, node_id]},
                )
            out[number] = node_id
        return out

    @staticmethod
    def _texts(cur, subtype: str, fields: tuple[str, ...]):
        for row in _chronos.query(
            cur,
            """
            SELECT node_id, attrs
              FROM okg.graph_nodes
             WHERE subtype = :subtype
            """,
            {"subtype": subtype},
        ):
            attrs = row["attrs"] or {}
            parts = [
                attrs[f] for f in fields
                if isinstance(attrs.get(f), str) and attrs[f]
            ]
            if parts:
                yield str(row["node_id"]), "\n".join(parts)


#: git's own record of which pull request a merge commit landed. GitHub
#: writes this subject when a PR is merged with a merge commit.
MERGE_PR_SUBJECT = re.compile(r"^Merge pull request #(\d+)\b")


class ForgeMergeAttributionLinker:
    """`pull_request -merged_as-> git_commit` from the MERGE COMMIT'S OWN
    SUBJECT (`Merge pull request #N from ...`).

    Why this exists alongside the source-carried path. The forge payload
    carries `merge_commit_sha`, and `GitHubSource` emits `merged_as` from
    it — that is the source-carried join and it runs first, per the
    umbrella's ordering rule. But that field does not always name a commit
    in the repository's own history: for psf/requests at the 2317 cutoff,
    ALL 714 pre-cutoff merged PRs report a sha that is GitHub's ephemeral
    TEST-MERGE commit (`Merge <head> into <base>`), not the merge that
    landed (`Merge pull request #N from ...`). Those shas resolve to
    nothing in the corpus, so the builder drops them with a count rather
    than emitting 714 dangling edges.

    This linker is not a substitute for a field the corpus failed to read
    — the field was read, measured, and found to point outside the corpus.
    It reads a DIFFERENT field that git itself carries: the merge commit's
    subject line. Structure the publisher wrote down, per the umbrella's
    "ingesting is not presupposing" ruling.

    A subject naming a PR outside the corpus is dropped WITH A COUNT.
    """

    name: str = "forge_merge_attribution"
    reads_subtypes: tuple[str, ...] = ("git_commit", "pull_request")
    reads_edge_types: tuple[str, ...] = ()
    emits_edge_types: tuple[str, ...] = ("merged_as",)
    requires_narrowings: tuple[tuple[str, str, str], ...] = (
        ("pull_request", "merged_as", "git_commit"),
    )
    dormant_ok: bool = False
    nondeterministic: bool = False

    def __init__(
        self,
        *,
        name: Optional[str] = None,
        fail_on_zero_resolved: bool = True,
    ) -> None:
        if name is not None:
            self.name = name
        self.fail_on_zero_resolved = bool(fail_on_zero_resolved)

    def can_skip_clean(
        self, conn: psycopg.Connection, ctx: IncrementalContext,
    ) -> bool:
        del conn, ctx
        return True

    def enrich(
        self,
        conn: psycopg.Connection,
        *,
        generation_id: int,
        incremental: IncrementalContext | None = None,
    ) -> EnrichResult:
        started = time.perf_counter()
        timings_ms: dict[str, float] = {}
        stats = {
            "merge_commits_scanned": 0,
            "subjects_naming_a_pr": 0,
            "resolved": 0,
            "unresolved_out_of_corpus": 0,
        }
        unresolved_sample: set[int] = set()
        with conn.cursor() as cur:
            targets = _pull_request_numbers(cur)
            candidates: list[DerivedEdgeCandidate] = []
            for node_id, subject in ForgeReferenceLinker._texts(
                cur, "git_commit", ("message_subject",),
            ):
                stats["merge_commits_scanned"] += 1
                m = MERGE_PR_SUBJECT.match(subject.strip())
                if not m:
                    continue
                stats["subjects_naming_a_pr"] += 1
                number = int(m.group(1))
                pr_id = targets.get(number)
                if pr_id is None:
                    stats["unresolved_out_of_corpus"] += 1
                    if len(unresolved_sample) < 20:
                        unresolved_sample.add(number)
                    continue
                stats["resolved"] += 1
                attrs = {
                    "merge_commit_subject_number": number,
                    "attribution": "merge_commit_subject",
                }
                candidates.append(
                    DerivedEdgeCandidate(
                        src=pr_id,
                        edge_type="merged_as",
                        dst=node_id,
                        attrs=attrs,
                        source_record_id={
                            "src_node_id": pr_id,
                            "dst_node_id": node_id,
                            "linker": self.name,
                            "pr_number": number,
                        },
                        source_revision={"generation_id": generation_id},
                        dedupe_key=stable_edge_dedupe_key(
                            source=f"_enricher:{self.name}",
                            src=pr_id,
                            edge_type="merged_as",
                            dst=node_id,
                            attrs=attrs,
                        ),
                    )
                )
            _record(timings_ms, "scan", started)

            if (
                self.fail_on_zero_resolved
                and stats["subjects_naming_a_pr"]
                and not stats["resolved"]
            ):
                raise ForgeReferencesUnresolved(
                    f"{self.name}: {stats['subjects_naming_a_pr']} merge "
                    f"commit subject(s) name a pull request and NONE resolved "
                    f"against {len(targets)} pull_request node(s) in this "
                    f"generation — the forge and git halves of this graph are "
                    f"not the same repository",
                    data={
                        **stats,
                        "pull_request_targets": len(targets),
                        "unresolved_sample": sorted(unresolved_sample),
                    },
                    fix_hint=(
                        "confirm the forge source and the git checkout point "
                        "at the same repository, and that pull_request nodes "
                        "publish an integer `number` attr"
                    ),
                )
            if not candidates:
                return EnrichResult(
                    enricher_name=self.name,
                    n_edges_emitted=0,
                    timings_ms=timings_ms,
                    output_lineage={"merge_attribution_counts": stats},
                )
            started = time.perf_counter()
            result = insert_deterministic_edges(
                cur, source=f"_enricher:{self.name}", candidates=candidates,
            )
            _record(timings_ms, "insert", started)
        return EnrichResult(
            enricher_name=self.name,
            n_edges_emitted=result.inserted,
            n_edges_skipped=result.skipped,
            timings_ms=timings_ms,
            sanitized_value_count=result.sanitized_value_count,
            output_lineage={
                "merge_attribution_counts": stats,
                "unresolved_sample": sorted(unresolved_sample),
            },
        )


def _pull_request_numbers(cur) -> dict[int, str]:
    """`number` -> node_id over live pull_request nodes only."""
    out: dict[int, str] = {}
    for row in _chronos.query(
        cur,
        """
        SELECT node_id, attrs
          FROM okg.graph_nodes
         WHERE subtype = :subtype
        """,
        {"subtype": "pull_request"},
    ):
        raw = (row["attrs"] or {}).get("number")
        if raw is None:
            continue
        try:
            out[int(raw)] = str(row["node_id"])
        except (TypeError, ValueError):
            continue
    return out


def _record(timings_ms: dict[str, float], name: str, started: float) -> None:
    timings_ms[name] = round(
        timings_ms.get(name, 0.0) + max(0.0, (time.perf_counter() - started) * 1000.0),
        3,
    )
