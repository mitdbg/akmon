"""GitHub source adapter — issues, pull requests + their discussion
via the `gh` CLI.

Profile: `mutable_api`.

Auth, pagination, and rate-limit handling are delegated to the
`gh` (GitHub CLI) binary. Operators install + auth `gh` once
(`gh auth login`), and the adapter shells out per cycle. This
trades a subprocess invocation cost (~50-200ms) for zero
HTTP-client / token-management code in the substrate.

Two modes:
- `fixture_path`: read a local JSON snapshot. Tests + CI use
  this so `gh` doesn't need to be installed.
- `repo`: live ingest via
  `gh api --paginate /repos/<owner>/<name>/issues?state=all`.

Emits:
- `repo` node (one per configured repo).
- `issue` and `pull_request` nodes (PRs are issues with
  `pull_request` field set; we split into the two subtypes).
- `repo contains issue` / `repo contains pull_request` edges.
- `forge_comment` nodes with `issue|pull_request -has_comment->
  forge_comment` edges, when the payload carries a discussion
  block (per `okg-corpus-repo-history`, 2026-07-28).
- `pull_request -merged_as-> git_commit` edges from the payload's
  merge commit sha, when the payload carries a `pulls` block AND
  `commit_repo` names the git_graph repo token the commit node ids
  are built from.

SOURCE INVENTORY — what the `gh` payload carries versus what this
adapter reads (umbrella req.design.inventory):

  /repos/{repo}/issues  (the item stream)
    READ   number, title, body, state, user.login, html_url,
           created_at, updated_at, closed_at, labels[].name,
           pull_request (presence -> subtype split; merged_at)
    UNREAD comments (an INTEGER COUNT, not bodies — the bodies come
           from the discussion endpoints below, so the count would
           be a second, drift-prone copy of a number the graph can
           compute by counting has_comment edges)
    UNREAD id, node_id, url, repository_url, labels_url,
           comments_url, events_url, timeline_url — API-internal
           locators superseded by `html_url` and the node id
    UNREAD assignee/assignees, milestone, reactions, locked,
           active_lock_reason, author_association, draft,
           performed_via_github_app, state_reason, closed_by,
           type, sub_issues_summary, issue_dependencies_summary,
           issue_field_values, pinned_comment — no question shape
           in this vertical's benchmark reaches them; each is a
           node/attribute expansion a later PACT can justify

  /repos/{repo}/issues/comments  (issue + PR conversation)
    READ   id, issue_url (-> parent number), user.login, body,
           created_at, updated_at, html_url, author_association
    UNREAD node_id, url (API locators), reactions, minimized, pin,
           performed_via_github_app

  /repos/{repo}/pulls/comments  (PR review comments)
    READ   id, pull_request_url (-> parent number), user.login,
           body, created_at, updated_at, html_url,
           author_association, path, commit_id, in_reply_to_id
    UNREAD diff_hunk, line/original_line, position/
           original_position, start_line/start_side, side,
           subject_type, pull_request_review_id, original_commit_id,
           _links, reactions, node_id — diff-anchor geometry that
           only matters for rendering the review UI

  /repos/{repo}/pulls  (merge shas; the issues endpoint has none)
    READ   number, merge_commit_sha, merged_at
    UNREAD head/base refs, requested_reviewers, auto_merge,
           statuses_url and the rest: the issues endpoint is
           already the authority for every PR attribute this
           adapter emits, and reading a second copy here would
           create two sources for one field

Per `wire-issues-feed-to-okg-workspace` (2026-04-26);
discussion + linkage per `okg-corpus-repo-history` (2026-07-28).
"""
from __future__ import annotations

import hashlib
import json
import re
import shutil
import subprocess  # nosec B404
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator, Optional

from okg.substrate.errors import SubstrateError
from okg.substrate.library.sources.base import (
    EdgeFact,
    EdgeKey,
    NodeFact,
    RecordEmission,
    RecordNode,
    RecordSet,
    SourceRun,
)
from okg.substrate.support import redact_text


__all__ = ["GitHubSource", "GhCliMissing", "ForgeDiscussionError"]

#: `.../issues/2311` or `.../pulls/2311` -> 2311.
_PARENT_NUMBER = re.compile(r"/(?:issues|pulls)/(\d+)\s*$")

_ISSUE_COMMENT = "issue_comment"
_REVIEW_COMMENT = "review_comment"


class GhCliMissing(SubstrateError):
    """Raised when the `gh` binary isn't on PATH and no
    fixture_path was provided."""

    code = "gh_cli_missing"


class ForgeDiscussionError(SubstrateError):
    """Raised when a payload carries discussion or merge structure that
    normalizes to nothing.

    Absence must never be read as agreement: a `comments` array that
    yields zero `forge_comment` nodes, or a `pulls` array of merged PRs
    that yields zero `merged_as` edges, means the parse is broken — not
    that the repository is quiet. Every message names the counts.
    """

    code = "forge_discussion_empty"


@dataclass
class GitHubSource:
    """GitHub source adapter.

    Pass one of:
    - `repo='owner/name'` to fetch live via `gh api`.
    - `fixture_path=Path(...)` to read a local JSON snapshot
      (used by tests and air-gapped envs).

    `since` is an ISO-8601 timestamp; only issues updated after
    it are fetched. Defaults to None (full pull).
    """

    name: str = "github"
    profile: str = "mutable_api"
    repo: Optional[str] = None
    fixture_path: Optional[Path] = None
    since: Optional[str] = None
    forge_host: str = "github"
    #: git_graph repo token used to build `commit:<token>:<sha>` node ids.
    #: Required to emit `pull_request -merged_as-> git_commit`; when it is
    #: None a payload carrying merge shas RAISES rather than silently
    #: dropping the join (the whole point of the field).
    commit_repo: Optional[str] = None
    #: Live mode only. Fetching every comment in a repository is a large
    #: crawl, so it is opt-in; an existing deployment keeps its current
    #: single-endpoint cost until it asks for the discussion. Fixture mode
    #: needs no flag: the presence of a `comments` block in the payload is
    #: the request, and the corpus builder is what writes it.
    include_discussion: bool = False

    # Incremental-source contract (issue #323): a live repo is a mutable upstream
    # API, so the probe is a cheap high-water `updated_at` over the issues feed
    # (one `gh api ...?sort=updated&per_page=1` call instead of the full
    # paginated fetch); every create / edit / close bumps the timestamp. A
    # fixture-backed source is local bytes -> the content-hash floor. A `gh`
    # failure / missing repo RAISES -> run_probe forces a full read.
    change_probe_kind = "mutable_api"

    def __post_init__(self) -> None:
        self.change_probe = self._build_change_probe()
        # A local, unfiltered fixture is a complete immutable byte snapshot.
        # The live API watermark and `since`-filtered reads do not identify the
        # exact complete payload consumed, so they conservatively always read.
        self.probe_short_circuit_safe = (
            self.fixture_path is not None and self.since is None
        )

    def _build_change_probe(self) -> Any:
        import sys

        from okg.substrate.library.sources.emission_identity import (
            emission_identity_targets,
        )

        emit_targets = emission_identity_targets(
            self.name,
            sys.modules[__name__],
        )
        if self.fixture_path is not None:
            from okg.substrate.library.sources.content_hash_probe import (
                ContentHashProbe,
            )

            fixture = Path(self.fixture_path)

            def _items() -> list[tuple[str, Path]]:
                return [(str(fixture), fixture)] if fixture.is_file() else []

            return ContentHashProbe(
                content_items=_items,
                config={"fixture_path": str(self.fixture_path), "since": self.since},
                emit_targets=emit_targets,
            )

        from okg.substrate.library.sources.mutable_api_probe import MutableApiProbe

        return MutableApiProbe(
            version_fn=self._issues_updated_watermark,
            config={
                "repo": self.repo,
                "since": self.since,
                "forge_host": self.forge_host,
            },
            emit_targets=emit_targets,
            content_kind="updated_at",
        )

    def _issues_updated_watermark(self) -> str:
        """Cheap high-water `updated_at` over the issues feed.

        One non-paginated `gh api` call (most-recently-updated issue) instead of
        the full crawl. Raises (no repo / `gh` missing / API error) so the
        run_probe boundary forces a full read rather than a silent skip.
        """
        if self.repo is None:
            raise SubstrateError(
                f"source {self.name!r}: change-probe needs repo or fixture_path",
                fix_hint="repo='owner/name' for live, fixture_path=Path(...) for tests",
            )
        gh = shutil.which("gh")
        if gh is None:
            raise GhCliMissing(
                "gh binary not on PATH",
                data={"adapter": self.name, "repo": self.repo},
                fix_hint="install GitHub CLI + `gh auth login`",
            )
        path = (
            f"/repos/{self.repo}/issues"
            "?state=all&sort=updated&direction=desc&per_page=1"
        )
        if self.since:
            path += f"&since={self.since}"
        proc = subprocess.run(
            [gh, "api", path],
            capture_output=True, text=True,
        )  # nosec B603
        if proc.returncode != 0:
            raise SubstrateError(
                f"`gh api` watermark failed for {self.repo!r}: "
                f"{proc.stderr.strip()[:300]}",
                data={"adapter": self.name, "repo": self.repo},
                fix_hint="check `gh auth status`; verify the repo slug",
            )
        items = json.loads(proc.stdout.strip() or "[]")
        if not isinstance(items, list) or not items:
            return "empty"
        top = items[0]
        # `updated_at` alone catches create/edit/close; pair it with the issue's
        # number so a backdated-update edge case still shifts the token.
        return f"{top.get('updated_at')}:{top.get('number')}"

    @staticmethod
    def _redact_text(value: str | None) -> str:
        if value is None:
            return ""
        redacted, _markers = redact_text(str(value))
        return redacted

    def run(
        self,
        run_id: str,
        *,
        mode: str = "scope_complete",
        cursor: Optional[dict[str, Any]] = None,
    ) -> SourceRun:
        applied_raw_token: dict[str, Any] | None = None
        if self.fixture_path is not None:
            raw_bytes = self._read_fixture_bytes()
            payload = self._load_fixture(raw_bytes=raw_bytes)
            # Legacy fixture shape: {"nodes": [...], "edges": [...]}
            # — already in NodeFact/EdgeFact form, no normalization
            # needed. Used by the devops template and existing
            # callers; new fixtures use the gh-API shape (a JSON
            # array of issue objects) which goes through
            # `_normalize`.
            if isinstance(payload, dict) and (
                "nodes" in payload or "edges" in payload
            ):
                nodes_edges = list(self._direct_facts(payload))
            else:
                nodes_edges = list(self._normalize(payload))
                self._assert_discussion_parsed(payload, nodes_edges)
            if self.since is None:
                applied_raw_token = self.change_probe.token_from_item_hashes({
                    str(Path(self.fixture_path)): hashlib.sha256(
                        raw_bytes
                    ).hexdigest(),
                })
        elif self.repo is not None:
            payload = self._fetch_via_gh()
            nodes_edges = list(self._normalize(payload))
            self._assert_discussion_parsed(payload, nodes_edges)
        else:
            raise SubstrateError(
                f"source {self.name!r}: pass fixture_path or repo",
                fix_hint="repo='owner/name' for live, "
                         "fixture_path=Path(...) for tests",
            )

        completed_scope = (
            mode in {"reconcile", "scope_complete", "release_new"}
        )
        record_set = (
            self._record_set_from_facts(nodes_edges)
            if completed_scope and self.since is None else None
        )

        def _stream() -> Iterator[NodeFact | EdgeFact]:
            yield from nodes_edges

        return SourceRun(
            facts=_stream(),
            completed_scope=completed_scope,
            applied_raw_token=applied_raw_token,
            applied_token_scope=(
                "global" if applied_raw_token is not None else "unavailable"
            ),
            applied_token_authority=(
                "exact" if applied_raw_token is not None else "unavailable"
            ),
            run_mode=mode,
            record_set=record_set,
        )

    @staticmethod
    def _record_set_from_facts(
        facts: list[NodeFact | EdgeFact],
    ) -> RecordSet:
        records: dict[str, RecordEmission] = {}
        nodes: dict[str, NodeFact] = {
            fact.node_id: fact
            for fact in facts
            if isinstance(fact, NodeFact)
        }
        for node in nodes.values():
            records[node.node_id] = RecordEmission(
                primary=RecordNode(
                    node_id=node.node_id,
                    subtype=node.subtype,
                    source_record_id=node.source_record_id,
                    source_revision=node.source_revision,
                    ownership_mode=node.ownership_mode,
                )
            )
        for edge in (fact for fact in facts if isinstance(fact, EdgeFact)):
            record_key = edge.dst if edge.dst in nodes else edge.src
            records.setdefault(record_key, RecordEmission()).edges.append(
                EdgeKey(
                    src=edge.src,
                    dst=edge.dst,
                    edge_type=edge.edge_type,
                    attrs=edge.attrs,
                    provenance=edge.provenance,
                    confidence=edge.confidence,
                    source_record_id=edge.source_record_id,
                    source_revision=edge.source_revision,
                )
            )
        return RecordSet(records=records)

    def _direct_facts(
        self, doc: dict,
    ) -> Iterator[NodeFact | EdgeFact]:
        """Emit pre-normalized facts from the legacy
        `{"nodes": [...], "edges": [...]}` fixture shape."""
        for n in doc.get("nodes", []) or []:
            yield NodeFact(
                node_id=n["node_id"], subtype=n["subtype"],
                attrs=n.get("attrs", {}),
                source_record_id=n.get(
                    "source_record_id", {"id": n["node_id"]},
                ),
                source_revision=n.get(
                    "source_revision", {"v": 1},
                ),
                op=n.get("op", "I"),
                # A node several sources emit (a person, say) declares
                # "substitute": one row, a claim per source, and coverage
                # accepts it under any owner (pipeline.md, shared nodes).
                ownership_mode=n.get("ownership_mode", "authoritative"),
            )
        for e in doc.get("edges", []) or []:
            yield EdgeFact(
                src=e["src"], dst=e["dst"],
                edge_type=e["edge_type"],
                attrs=e.get("attrs", {}),
                provenance=e.get("provenance", "explicit"),
                confidence=float(e.get("confidence", 1.0)),
                source_record_id=e.get(
                    "source_record_id",
                    {"edge": f"{e['src']}|{e['edge_type']}|"
                             f"{e['dst']}"},
                ),
                source_revision=e.get(
                    "source_revision", {"v": 1},
                ),
                op=e.get("op", "I"),
            )

    # ---------- live fetch ----------

    def _fetch_via_gh(self) -> list[dict] | dict[str, list[dict]]:
        """Shell out to `gh api --paginate /repos/<repo>/issues`.

        `gh` handles auth, pagination, and rate-limit retry.
        Issues and PRs come from the same endpoint; PRs carry a
        `pull_request` field that we use to split subtypes in
        normalization.

        With `include_discussion`, three more endpoints are crawled and
        the result becomes the dict payload shape: the issue-comment and
        review-comment streams (the conversation) and the pulls stream
        (the only endpoint that carries `merge_commit_sha`).
        """
        path = f"/repos/{self.repo}/issues?state=all&per_page=100"
        if self.since:
            path += f"&since={self.since}"
        items = self._gh_paginated(path)
        if not self.include_discussion:
            return items
        return {
            "items": items,
            "comments": self._gh_paginated(
                f"/repos/{self.repo}/issues/comments?per_page=100"
            ),
            "review_comments": self._gh_paginated(
                f"/repos/{self.repo}/pulls/comments?per_page=100"
            ),
            "pulls": self._gh_paginated(
                f"/repos/{self.repo}/pulls?state=all&per_page=100"
            ),
        }

    def _gh_paginated(self, path: str) -> list[dict]:
        gh = shutil.which("gh")
        if gh is None:
            raise GhCliMissing(
                "gh binary not on PATH",
                data={"adapter": self.name, "repo": self.repo},
                fix_hint=(
                    "install via `brew install gh` (macOS) or "
                    "see https://cli.github.com/; "
                    "then run `gh auth login`"
                ),
            )
        proc = subprocess.run(
            [gh, "api", "--paginate", path],
            capture_output=True, text=True,
        )  # nosec B603
        if proc.returncode != 0:
            raise SubstrateError(
                f"`gh api` failed for {self.repo!r}: "
                f"{proc.stderr.strip()[:300]}",
                data={
                    "adapter": self.name,
                    "repo": self.repo,
                    "path": path,
                    "returncode": proc.returncode,
                },
                fix_hint=(
                    "check `gh auth status`; verify the repo "
                    "slug; if rate-limited, wait + retry"
                ),
            )
        # `gh api --paginate` joins JSON arrays — modern gh
        # returns one merged array, older gh returns
        # `]\n[...]\n[...]`. Try the merged-array parse first.
        out = proc.stdout.strip()
        try:
            return json.loads(out)
        except json.JSONDecodeError:
            items: list[dict] = []
            buf = []
            depth = 0
            for ch in out:
                buf.append(ch)
                if ch == "[":
                    depth += 1
                elif ch == "]":
                    depth -= 1
                    if depth == 0:
                        try:
                            parsed = json.loads("".join(buf))
                            if isinstance(parsed, list):
                                items.extend(parsed)
                        except json.JSONDecodeError:
                            # Ignore malformed embedded JSON blocks in exported issue text.
                            pass
                        buf = []
            return items

    # ---------- fixture ----------

    def _read_fixture_bytes(self) -> bytes:
        p = Path(self.fixture_path)
        try:
            return p.read_bytes()
        except FileNotFoundError as exc:
            raise SubstrateError(
                f"source {self.name!r}: fixture_path {p} not "
                f"found",
                data={"adapter": self.name, "fixture_path": str(p)},
            ) from exc
        except OSError as exc:
            raise SubstrateError(
                f"source {self.name!r}: fixture_path could not be read",
                data={"adapter": self.name, "fixture_path": str(p)},
                fix_hint="make fixture_path readable or configure repo",
            ) from exc

    def _load_fixture(self, *, raw_bytes: bytes):
        p = Path(self.fixture_path)
        try:
            doc = json.loads(raw_bytes)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise SubstrateError(
                f"source {self.name!r}: fixture_path is not valid JSON",
                data={"adapter": self.name, "fixture_path": str(p)},
                fix_hint="replace fixture_path with a valid JSON fixture",
            ) from exc
        # Three accepted shapes:
        #   1. JSON array of issue objects (new, gh-API shape).
        #   2. {"items": [...], "comments": [...],
        #       "review_comments": [...], "pulls": [...]} — the
        #      discussion-carrying variant; every key but `items` is
        #      optional, and `{"items": [...]}` alone is the historical
        #      list-wrapped form.
        #   3. {"nodes": [...], "edges": [...]} — legacy
        #      pre-normalized shape (devops template).
        if isinstance(doc, list):
            return doc
        if isinstance(doc, dict):
            if "items" in doc:
                return doc
            if "nodes" in doc or "edges" in doc:
                return doc  # consumer dispatches via _direct_facts.
        raise SubstrateError(
            f"source {self.name!r}: fixture_path {p} must "
            f"contain a JSON array of issues, or "
            f"{{\"items\": [...]}}, or "
            f"{{\"nodes\": [...], \"edges\": [...]}}",
            data={"adapter": self.name},
        )

    # ---------- payload accessors ----------

    @staticmethod
    def _payload_block(payload: Any, key: str) -> list[dict]:
        """The `key` array of a dict payload, or `[]`. A present-but-
        wrong-typed block is an error, never an empty read."""
        if not isinstance(payload, dict):
            return []
        if key not in payload:
            return []
        block = payload[key]
        if block is None:
            return []
        if not isinstance(block, list):
            raise ForgeDiscussionError(
                f"payload key {key!r} must be a JSON array; got "
                f"{type(block).__name__}",
                data={"key": key, "type": type(block).__name__},
            )
        return [row for row in block if isinstance(row, dict)]

    @classmethod
    def _payload_items(cls, payload: Any) -> list[dict]:
        if isinstance(payload, list):
            return [row for row in payload if isinstance(row, dict)]
        return cls._payload_block(payload, "items")

    # ---------- normalization ----------

    def _normalize(
        self, payload: Any,
    ) -> Iterator[NodeFact | EdgeFact]:
        items = self._payload_items(payload)
        repo = self.repo or "unknown/unknown"
        repo_id = f"repo:{self.forge_host}:{repo}"
        revision = {"forge": self.forge_host, "repo": repo}
        #: number -> parent node id, for comment + merge attachment. Built as
        #: items are emitted so a comment can only attach to an item that is
        #: actually IN this payload — the corpus builder's cutoff filter
        #: therefore bounds the comments too, without the source re-deriving it.
        parents: dict[int, tuple[str, str]] = {}

        yield NodeFact(
            node_id=repo_id, subtype="repo",
            attrs={
                "repo_id": repo_id,
                "forge": self.forge_host,
                "slug": repo,
                "url": f"https://{self.forge_host}.com/{repo}",
            },
            source_record_id={"repo": repo},
            source_revision=revision,
            op="I",
        )

        for item in items:
            number = item.get("number")
            title = self._redact_text(item.get("title", ""))
            body = self._redact_text(item.get("body") or "")
            state = item.get("state", "open")
            user = (item.get("user") or {}).get("login")
            url = item.get("html_url")
            created_at = item.get("created_at")
            updated_at = item.get("updated_at")
            closed_at = item.get("closed_at")
            labels_list = item.get("labels") or []
            labels = ",".join(
                lbl.get("name", "") if isinstance(lbl, dict)
                else str(lbl) for lbl in labels_list
            )
            is_pr = "pull_request" in item

            if is_pr:
                pr_id = f"pr:{self.forge_host}:{repo}:#{number}"
                # The issues endpoint nests the merge timestamp under
                # `pull_request.merged_at`; there is NO top-level
                # `merged_at` on this stream. Reading only the top level
                # (the shape before okg-corpus-repo-history) made
                # `merged` False and `state` "closed" for every merged PR
                # in the graph — 723/1039 of them in the psf/requests
                # corpus. Prefer the top level when a caller supplies it,
                # fall back to the nested value the API actually returns.
                merged_at = item.get("merged_at") or (
                    item["pull_request"].get("merged_at")
                    if isinstance(item.get("pull_request"), dict) else None
                )
                yield NodeFact(
                    node_id=pr_id, subtype="pull_request",
                    attrs={
                        "pr_id": pr_id,
                        "number": number,
                        "title": title,
                        "state": "merged" if merged_at else state,
                        "author_login": user,
                        "body": body,
                        "merged": bool(merged_at),
                        "created_at": created_at,
                        "updated_at": updated_at,
                        "merged_at": merged_at,
                        "url": url,
                    },
                    source_record_id={
                        "repo": repo, "number": number,
                    },
                    source_revision=revision,
                    op="I",
                )
                yield EdgeFact(
                    src=repo_id, dst=pr_id,
                    edge_type="contains",
                    attrs={},
                    source_record_id={
                        "repo": repo, "number": number,
                    },
                    source_revision=revision,
                    op="I",
                )
                if isinstance(number, int):
                    parents[number] = (pr_id, "pull_request")
            else:
                issue_id = (
                    f"issue:{self.forge_host}:{repo}:#{number}"
                )
                yield NodeFact(
                    node_id=issue_id, subtype="issue",
                    attrs={
                        "issue_id": issue_id,
                        "number": number,
                        "title": title,
                        "state": state,
                        "author_login": user,
                        "body": body,
                        "labels": labels,
                        "created_at": created_at,
                        "updated_at": updated_at,
                        "closed_at": closed_at,
                        "url": url,
                    },
                    source_record_id={
                        "repo": repo, "number": number,
                    },
                    source_revision=revision,
                    op="I",
                )
                yield EdgeFact(
                    src=repo_id, dst=issue_id,
                    edge_type="contains",
                    attrs={},
                    source_record_id={
                        "repo": repo, "number": number,
                    },
                    source_revision=revision,
                    op="I",
                )
                if isinstance(number, int):
                    parents[number] = (issue_id, "issue")

        yield from self._normalize_comments(
            payload, repo=repo, revision=revision, parents=parents,
        )
        yield from self._normalize_merges(
            payload, repo=repo, revision=revision, parents=parents,
        )

    # ---------- discussion ----------

    @staticmethod
    def _parent_number(row: dict) -> int | None:
        """The issue/PR number a comment belongs to, from whichever
        parent locator the endpoint supplied."""
        for key in ("issue_url", "pull_request_url"):
            value = row.get(key)
            if isinstance(value, str):
                m = _PARENT_NUMBER.search(value)
                if m:
                    return int(m.group(1))
        # `html_url` is `.../issues/12#issuecomment-770770` — strip the
        # fragment before matching so it can serve as the fallback.
        html = row.get("html_url")
        if isinstance(html, str):
            m = _PARENT_NUMBER.search(html.split("#", 1)[0])
            if m:
                return int(m.group(1))
        return None

    def _normalize_comments(
        self,
        payload: Any,
        *,
        repo: str,
        revision: dict[str, Any],
        parents: dict[int, tuple[str, str]],
    ) -> Iterator[NodeFact | EdgeFact]:
        blocks = (
            (_ISSUE_COMMENT, self._payload_block(payload, "comments")),
            (_REVIEW_COMMENT, self._payload_block(payload, "review_comments")),
        )
        seen: set[str] = set()
        for kind, rows in blocks:
            for row in rows:
                remote_id = row.get("id")
                if remote_id is None:
                    raise ForgeDiscussionError(
                        f"{kind} row without an `id`: keys={sorted(row)}",
                        data={"adapter": self.name, "repo": repo, "kind": kind},
                        fix_hint="comment identity is the remote id; refetch the payload",
                    )
                number = self._parent_number(row)
                if number is None or number not in parents:
                    # The parent is not in this payload — outside the
                    # caller's scope (for a benchmark corpus: excluded or
                    # post-cutoff). Skipped, never stubbed; the builder
                    # counts these before they reach the source.
                    continue
                parent_id, _parent_subtype = parents[number]
                comment_id = (
                    f"comment:{self.forge_host}:{repo}:"
                    f"{'ic' if kind == _ISSUE_COMMENT else 'rc'}:{remote_id}"
                )
                if comment_id in seen:
                    continue
                seen.add(comment_id)
                record_id = {
                    "repo": repo, "number": number, "comment_id": comment_id,
                }
                attrs: dict[str, Any] = {
                    "comment_id": comment_id,
                    "comment_kind": kind,
                    "parent_number": number,
                    "author_login": (row.get("user") or {}).get("login"),
                    "body": self._redact_text(row.get("body") or ""),
                    "author_association": row.get("author_association"),
                    "created_at": row.get("created_at"),
                    "updated_at": row.get("updated_at"),
                    "url": row.get("html_url"),
                }
                if kind == _REVIEW_COMMENT:
                    attrs["path"] = row.get("path")
                    attrs["review_commit_sha"] = row.get("commit_id")
                    in_reply_to = row.get("in_reply_to_id")
                    if in_reply_to is not None:
                        attrs["in_reply_to"] = str(in_reply_to)
                yield NodeFact(
                    node_id=comment_id, subtype="forge_comment",
                    attrs=attrs,
                    source_record_id=record_id,
                    source_revision=revision,
                    op="I",
                )
                yield EdgeFact(
                    src=parent_id, dst=comment_id,
                    edge_type="has_comment",
                    attrs={"comment_kind": kind},
                    source_record_id=record_id,
                    source_revision=revision,
                    op="I",
                )

    def _normalize_merges(
        self,
        payload: Any,
        *,
        repo: str,
        revision: dict[str, Any],
        parents: dict[int, tuple[str, str]],
    ) -> Iterator[EdgeFact]:
        """`pull_request -merged_as-> git_commit` from the payload's merge
        sha. SOURCE-CARRIED, so it is emitted here rather than derived —
        the umbrella's ordering rule."""
        rows = self._payload_block(payload, "pulls")
        merged_rows = [
            row for row in rows
            if row.get("merged_at") and row.get("merge_commit_sha")
        ]
        if not merged_rows:
            return
        if not self.commit_repo:
            raise ForgeDiscussionError(
                f"payload carries {len(merged_rows)} merged pull request(s) "
                f"with a merge sha, but commit_repo is not configured — the "
                f"git_commit node ids cannot be built, so every "
                f"pull_request -merged_as-> git_commit edge would be silently "
                f"dropped",
                data={
                    "adapter": self.name, "repo": repo,
                    "merged_rows": len(merged_rows),
                },
                fix_hint=(
                    "set commit_repo to the git_graph repo token (the token in "
                    "`commit:<token>:<sha>` node ids — usually the deployment "
                    "or checkout name)"
                ),
            )
        for row in merged_rows:
            number = row.get("number")
            if not isinstance(number, int) or number not in parents:
                continue
            parent_id, parent_subtype = parents[number]
            if parent_subtype != "pull_request":
                continue
            sha = str(row["merge_commit_sha"])
            yield EdgeFact(
                src=parent_id,
                dst=f"commit:{self.commit_repo}:{sha}",
                edge_type="merged_as",
                attrs={"merge_commit_sha": sha, "merged_at": row["merged_at"]},
                source_record_id={
                    "repo": repo, "number": number, "merge_commit_sha": sha,
                },
                source_revision=revision,
                op="I",
            )

    def _assert_discussion_parsed(
        self, payload: Any, facts: list[NodeFact | EdgeFact],
    ) -> None:
        """Absence is never agreement. A payload that carries discussion or
        merge structure and normalizes to nothing means the parse broke."""
        comments_in = len(self._payload_block(payload, "comments")) + len(
            self._payload_block(payload, "review_comments")
        )
        comments_out = sum(
            1 for f in facts
            if isinstance(f, NodeFact) and f.subtype == "forge_comment"
        )
        if comments_in and not comments_out:
            raise ForgeDiscussionError(
                f"payload carried {comments_in} comment row(s) but normalized "
                f"to 0 forge_comment nodes — every row failed to attach to a "
                f"parent in this payload",
                data={
                    "adapter": self.name, "repo": self.repo,
                    "comments_in": comments_in, "comments_out": 0,
                    "items": len(self._payload_items(payload)),
                },
                fix_hint=(
                    "check that the comment rows' issue_url / "
                    "pull_request_url parent numbers exist among the payload's "
                    "items — a comments block fetched for a different item set "
                    "attaches to nothing"
                ),
            )
        merged_in = sum(
            1 for row in self._payload_block(payload, "pulls")
            if row.get("merged_at") and row.get("merge_commit_sha")
        )
        merged_out = sum(
            1 for f in facts
            if isinstance(f, EdgeFact) and f.edge_type == "merged_as"
        )
        if merged_in and not merged_out:
            raise ForgeDiscussionError(
                f"payload carried {merged_in} merged pull request(s) with a "
                f"merge sha but normalized to 0 merged_as edges",
                data={
                    "adapter": self.name, "repo": self.repo,
                    "merged_in": merged_in, "merged_out": 0,
                },
                fix_hint=(
                    "check that the `pulls` rows' numbers appear among the "
                    "payload's items as pull_request subtypes"
                ),
            )
