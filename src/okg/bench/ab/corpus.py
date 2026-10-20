"""Corpus builder: (repo, base_commit, cutoff, gold PR) -> leakage-audited
OKG deployment + MCP server config.

Orchestrates existing substrate machinery — never reimplements ingestion:

1. ``checkouts.surgical_checkout`` — the checkout's ref surface IS the git
   cutoff (GitHistorySource walks every ref unconditionally).
2. ``gh api`` fetch of issues/PRs, filtered to ``created_at < cutoff`` with
   the gold PR excluded, written as a fixture for ``GitHubSource`` (its
   ``since`` cannot express an upper bound).
3. workspace-graph profile scaffold via
   ``okg.substrate.deployment_bootstrap.profile_init``.
4. In-process migrate + catalog + ingest + publish, mirroring
   ``cli/provision.py`` — the github source is registered BEFORE first
   publish because the profile seeds the nomos baseline from the registry at
   install time.
5. ``leakage.audit`` — per-instance, fail-closed: a corpus that cannot pass
   the audit is torn down, never silently published.

The result is a ``corpus.json`` the okg arm consumes (deployment name, DSN,
MCP config path, published generation, cutoff facts).
"""

from __future__ import annotations

import json
import re
import subprocess
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from . import checkouts
from .errors import CorpusBuildError
from .manifest import now_utc
from .model import dump_json
from okg.substrate.mcp.operators import OPERATOR_TOOLS


#: What `OKG_MCP_TOOL_SURFACE="operator"` was meant to select. That variable
#: was read by nothing, so these launches silently got the server's default
#: surface instead; `OKG_MCP_TOOL_ALLOWLIST` is the name the runtime honors.
_OPERATOR_SURFACE = ",".join(OPERATOR_TOOLS)

GITHUB_SOURCE_ID = "github-issues"
SESSIONS_SOURCE_ID = "agent-sessions"

# The corpus keeps only repo-derived sources. The workspace-graph scaffold
# also wires operator-environment sources (codex_sessions -> ~/.codex,
# pact, openspec, compute_env_local, test_reports, ...) which would ingest
# the OPERATOR's data into the benchmark corpus — an integrity violation the
# leakage audit would flag as post-cutoff facts. They are pruned before the
# nomos baseline is seeded.
CORPUS_SOURCE_IDS = frozenset(
    {"git_history", "git_files", "code_structure", "package_manifests"}
)


@dataclass(frozen=True)
class CorpusSpec:
    repo: str  # owner/name
    base_commit: str
    cutoff: str  # ISO 8601; facts must be created strictly before this
    gold_pr_number: int | None
    deployment_name: str
    dsn: str
    corpus_root: Path  # holds checkout/, fixtures/, deployments/, corpus.json
    mcp_port: int | None = None
    #: Where the INSTANCE DEFINITION came from — the released benchmark
    #: dataset, or a local `--offline-fixture` authored for tests and pilots.
    #: The forge payload and the checkout are always live (see
    #: `_declare_provenance`), but a corpus built to a locally-written spec
    #: has not been built from released artifacts and must not claim it.
    #:
    #: Defaults to EMPTY, not to "dataset". Defaulting to the released value
    #: would mean any caller that forgets the field gets a `real` verdict for
    #: free — the same defaulting defect this field exists to remove, one
    #: level down, and it reached the audited corpus because a direct-
    #: constructor build script never set it. An unstated origin is not
    #: evidence of a released one, so `_declare_provenance` REFUSES on empty
    #: rather than assuming either way.
    instance_source: str = ""


def build(spec: CorpusSpec, *, data_root: Path) -> Path:
    """Build the corpus end-to-end. Returns the corpus.json path.

    Every stage is fail-closed; a leakage-audit failure after publish tears
    the deployment down with the violation in the error.
    """
    spec.corpus_root.mkdir(parents=True, exist_ok=True)

    checkout = spec.corpus_root / "checkout"
    if checkout.exists():
        raise CorpusBuildError(
            f"corpus checkout already exists: {checkout}; corpora are immutable — "
            "build into a fresh corpus_root"
        )
    checkouts.surgical_checkout(
        spec.repo, spec.base_commit, checkout, data_root=data_root
    )

    fixture_path = spec.corpus_root / "fixtures" / "issues.json"
    counts = fetch_forge_fixture(
        repo=spec.repo,
        cutoff=spec.cutoff,
        gold_pr_number=spec.gold_pr_number,
        out_path=fixture_path,
        # The checkout IS the git cutoff, so its commit set is the exact
        # universe a merged_as edge may point into. Resolving here keeps a
        # dangling edge out of the fixture rather than out of the publish.
        known_shas=checkout_commit_shas(checkout),
    )

    deployment_dir = _scaffold_deployment(spec, checkout)
    pruned = _prune_registry_to_corpus_sources(deployment_dir)
    _patch_manifest_for_corpus(deployment_dir, spec)
    _register_github_source(deployment_dir, spec, fixture_path)
    sessions_root = spec.corpus_root / "sessions"
    sessions_root.mkdir(parents=True, exist_ok=True)
    _register_sessions_source(deployment_dir, spec, checkout, sessions_root)
    generation_id = _provision_and_publish(spec, deployment_dir)

    from .leakage import audit

    audit(
        dsn=spec.dsn,
        generation_id=generation_id,
        cutoff=spec.cutoff,
        gold_pr_number=spec.gold_pr_number,
        checkout=checkout,
        base_commit=spec.base_commit,
    )

    mcp_config_path = _write_mcp_config(spec, deployment_dir)
    corpus_json = spec.corpus_root / "corpus.json"
    dump_json(
        corpus_json,
        {
            **asdict_paths(spec),
            "generation_id": generation_id,
            "deployment_dir": str(deployment_dir),
            "mcp_config_path": str(mcp_config_path),
            "sessions_root": str(sessions_root),
            "forge_fixture": {"path": str(fixture_path), **counts},
            "pruned_sources": sorted(pruned),
            "built_at": now_utc(),
            "leakage_audit": "pass",
            **_declare_provenance(spec),
        },
    )
    return corpus_json


def _declare_provenance(spec: CorpusSpec) -> dict[str, Any]:
    """State this corpus's provenance per artifact, DERIVED, never asserted.

    The per-instance builder assembles no snapshot dir, so the domain
    builder's provenance.json does not apply here — but the claim still has
    to be written down, or the audit reads the corpus as `undeclared` and
    fails it. Writing a flat `"real"` instead would reintroduce exactly the
    defect that made every domain corpus claim released artifacts regardless
    of its inputs: a provenance that cannot come out any other way is not
    evidence of anything.

    Two of the three artifacts are unconditionally live: `fetch_forge_fixture`
    always shells to `gh api` (there is no forge-payload fixture path), and
    the checkout is a real clone at base_commit. The third is not — the
    instance DEFINITION can come from `--offline-fixture`, and a corpus built
    to a locally-authored spec has not been built from released artifacts.
    One stand-in artifact taints the corpus, matching `declared_provenance`.
    """
    if not spec.instance_source:
        raise CorpusBuildError(
            "CorpusSpec.instance_source is unset, so this corpus cannot state "
            "where its instance definition came from. Pass 'dataset' for the "
            "released benchmark dataset or 'offline-fixture:<path>' for a "
            "locally authored one. Absence is not a claim of released "
            "artifacts — the audit refuses an undeclared corpus, and guessing "
            "here would put the guess behind a `real` verdict."
        )
    artifacts = {
        "github_api": "real",       # fetch_forge_fixture has no fixture path
        "checkout": "real",         # a real clone at spec.base_commit
        "instance_definition": (
            "real" if spec.instance_source == "dataset" else "stand-in"
        ),
    }
    stand_ins = sorted(k for k, v in artifacts.items() if v != "real")
    return {
        "provenance": "stand-in" if stand_ins else "real",
        "provenance_by_artifact": artifacts,
        "provenance_note": (
            f"instance definition from {spec.instance_source}"
            if stand_ins else "released dataset, live API, real checkout"
        ),
    }


def asdict_paths(spec: CorpusSpec) -> dict[str, Any]:
    d = asdict(spec)
    d["corpus_root"] = str(spec.corpus_root)
    return d


# -- forge fixture ------------------------------------------------------


def fetch_forge_fixture(
    *,
    repo: str,
    cutoff: str,
    gold_pr_number: int | None,
    out_path: Path,
    known_shas: set[str] | None = None,
) -> dict[str, int]:
    """Fetch issues+PRs+discussion via ``gh``, filter to created_at <
    cutoff, exclude the gold PR, SCRUB post-cutoff mutable fields, and
    write the GitHubSource fixture.

    The GitHub issues API returns PRs as issues with a ``pull_request`` key,
    which is exactly the payload shape GitHubSource normalizes.

    Scrub rules (the API returns CURRENT field values; the leakage audit
    catches them as post-cutoff facts otherwise): an item closed/merged at
    or after the cutoff is restored to open/unmerged; a post-cutoff
    ``updated_at`` is clamped to ``created_at`` (the true pre-cutoff update
    time is not recoverable from this endpoint). Residual risk, documented
    in the runbook: body text edited after the cutoff is not detectable via
    the REST issues list; titles/bodies are used as-is.

    DISCUSSION (PACT okg-corpus-repo-history). Three more endpoints are
    crawled and filtered by the SAME rule applied PER COMMENT, not per
    parent: a comment created at or after the cutoff is dropped even when
    its parent issue predates it, because the parent's age says nothing
    about when a given reply was written. The gold PR's whole thread goes
    with the gold PR, since a comment can only attach to a kept item.
    """
    raw = _gh_json(
        ["api", "--paginate", f"/repos/{repo}/issues?state=all&per_page=100"]
    )
    if not isinstance(raw, list):
        raise CorpusBuildError(f"gh api returned non-list for {repo}: {type(raw)}")
    kept: list[dict[str, Any]] = []
    dropped_post_cutoff = 0
    dropped_gold = 0
    scrubbed = 0
    for item in raw:
        created = item.get("created_at")
        if not created:
            raise CorpusBuildError(
                f"issue item without created_at from {repo}: keys={sorted(item)}"
            )
        if not created < cutoff:  # ISO strings compare lexicographically in UTC
            dropped_post_cutoff += 1
            continue
        if gold_pr_number is not None and item.get("number") == gold_pr_number:
            dropped_gold += 1
            continue
        item, changed = _scrub_post_cutoff_fields(item, cutoff)
        scrubbed += int(changed)
        kept.append(item)
    if gold_pr_number is not None and dropped_gold == 0:
        # The gold PR postdates the cutoff in SWE-bench (PR created at
        # instance creation); its absence among pre-cutoff items is expected.
        # It must not be present in the kept set either way:
        if any(i.get("number") == gold_pr_number for i in kept):
            raise CorpusBuildError(
                f"gold PR #{gold_pr_number} survived the fixture filter"
            )

    kept_numbers = {
        i.get("number") for i in kept if isinstance(i.get("number"), int)
    }
    comments, comment_counts = fetch_forge_discussion(
        repo=repo,
        cutoff=cutoff,
        gold_pr_number=gold_pr_number,
        kept_numbers=kept_numbers,
    )
    pulls, pull_counts = fetch_forge_merges(
        repo=repo,
        cutoff=cutoff,
        gold_pr_number=gold_pr_number,
        kept_numbers=kept_numbers,
        known_shas=known_shas,
    )
    payload = {
        "items": kept,
        "comments": comments["issue_comments"],
        "review_comments": comments["review_comments"],
        "pulls": pulls,
    }
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")
    return {
        "kept": len(kept),
        "dropped_post_cutoff": dropped_post_cutoff,
        "dropped_gold": dropped_gold,
        "scrubbed_post_cutoff_fields": scrubbed,
        **comment_counts,
        **pull_counts,
    }


#: `.../issues/2311` / `.../pulls/2311` -> 2311 (fragment already stripped).
_PARENT_NUMBER_RE = re.compile(r"/(?:issues|pulls)/(\d+)\s*$")

_DISCUSSION_ENDPOINTS = (
    ("issue_comments", "issues/comments"),
    ("review_comments", "pulls/comments"),
)


def _comment_parent_number(row: dict[str, Any]) -> int | None:
    for key in ("issue_url", "pull_request_url"):
        value = row.get(key)
        if isinstance(value, str):
            m = _PARENT_NUMBER_RE.search(value)
            if m:
                return int(m.group(1))
    html = row.get("html_url")
    if isinstance(html, str):
        m = _PARENT_NUMBER_RE.search(html.split("#", 1)[0])
        if m:
            return int(m.group(1))
    return None


def fetch_forge_discussion(
    *,
    repo: str,
    cutoff: str,
    gold_pr_number: int | None,
    kept_numbers: set[int],
) -> tuple[dict[str, list[dict[str, Any]]], dict[str, int]]:
    """Issue-comment + review-comment streams, filtered PER COMMENT.

    Both endpoints are crawled ``sort=created&direction=asc`` so the
    pre-cutoff prefix comes first; the crawl is a full ``--paginate`` and
    the filter is applied here rather than trusted to a server-side bound
    (the API has ``since`` but no ``until``).

    Four independent drop reasons, each counted, so a zero can never be
    read as "the repository was quiet":

      - ``dropped_comments_post_cutoff`` — created at/after the cutoff.
        This is the per-comment rule: a 2019 reply on a 2012 issue is
        dropped even though its parent is squarely inside the corpus.
      - ``dropped_comments_gold`` — on the gold PR's thread.
      - ``dropped_comments_orphan`` — parent not among the kept items
        (itself post-cutoff, or unreadable).
      - ``dropped_comments_unparented`` — no parent locator at all.
    """
    counts: dict[str, int] = {
        "kept_comments": 0,
        "kept_review_comments": 0,
        "dropped_comments_post_cutoff": 0,
        "dropped_comments_gold": 0,
        "dropped_comments_orphan": 0,
        "dropped_comments_unparented": 0,
        "scrubbed_comment_updated_at": 0,
    }
    out: dict[str, list[dict[str, Any]]] = {
        "issue_comments": [],
        "review_comments": [],
    }
    for key, endpoint in _DISCUSSION_ENDPOINTS:
        raw = _gh_json([
            "api", "--paginate",
            f"/repos/{repo}/{endpoint}"
            "?sort=created&direction=asc&per_page=100",
        ])
        if not isinstance(raw, list):
            raise CorpusBuildError(
                f"gh api returned non-list for {repo} {endpoint}: {type(raw)}"
            )
        for row in raw:
            if not isinstance(row, dict):
                continue
            created = row.get("created_at")
            if not created:
                raise CorpusBuildError(
                    f"{endpoint} row without created_at from {repo}: "
                    f"keys={sorted(row)}"
                )
            # THE per-comment cutoff. Deliberately evaluated before the
            # parent lookup so it applies to comments on kept parents too.
            if not str(created) < cutoff:
                counts["dropped_comments_post_cutoff"] += 1
                continue
            number = _comment_parent_number(row)
            if number is None:
                counts["dropped_comments_unparented"] += 1
                continue
            if gold_pr_number is not None and number == gold_pr_number:
                counts["dropped_comments_gold"] += 1
                continue
            if number not in kept_numbers:
                counts["dropped_comments_orphan"] += 1
                continue
            row, changed = _scrub_comment_updated_at(row, cutoff)
            counts["scrubbed_comment_updated_at"] += int(changed)
            out[key].append(row)
        counts[
            "kept_comments" if key == "issue_comments"
            else "kept_review_comments"
        ] = len(out[key])
    total_kept = counts["kept_comments"] + counts["kept_review_comments"]
    total_seen = total_kept + sum(
        counts[k] for k in (
            "dropped_comments_post_cutoff", "dropped_comments_gold",
            "dropped_comments_orphan", "dropped_comments_unparented",
        )
    )
    if total_seen and not total_kept:
        raise CorpusBuildError(
            f"{repo}: {total_seen} comment(s) fetched but 0 survived the "
            f"cutoff filter — a corpus with issues but no discussion is a "
            f"failed fetch, not a quiet repository. Counts: {counts}"
        )
    return out, counts


def fetch_forge_merges(
    *,
    repo: str,
    cutoff: str,
    gold_pr_number: int | None,
    kept_numbers: set[int],
    known_shas: set[str] | None = None,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    """The pulls stream, reduced to (number, merge_commit_sha, merged_at)
    for PRs merged strictly before the cutoff whose merge commit is
    actually IN the corpus checkout's history.

    The issues endpoint carries no ``merge_commit_sha`` at all — this is
    the only stream that does, which is why the join to git history did not
    exist. A PR merged at or after the cutoff was NOT merged as of the
    cutoff, so it contributes no edge (and its ``merged_at`` is already
    scrubbed off the item by ``_scrub_post_cutoff_fields``).

    ``known_shas`` is the checkout's commit set, and filtering against it
    is not defensive padding — MEASURED on psf/requests at the 2317 cutoff,
    all 714 pre-cutoff merged PRs report a ``merge_commit_sha`` that names
    GitHub's ephemeral TEST-MERGE commit (``Merge <head> into <base>``)
    rather than the merge that landed (``Merge pull request #N from ...``),
    and NONE of the 714 exist in the repository's history. Emitting them
    would be 714 dangling edges and a blocked publish. They are dropped
    with a count under ``dropped_merges_absent_from_history``; the
    surviving join comes from git's own merge-commit subjects via
    ``ForgeMergeAttributionLinker``.
    """
    counts = {
        "kept_merges": 0,
        "dropped_merges_post_cutoff": 0,
        "dropped_merges_unmerged": 0,
        "dropped_merges_gold": 0,
        "dropped_merges_orphan": 0,
        "dropped_merges_absent_from_history": 0,
    }
    raw = _gh_json([
        "api", "--paginate",
        f"/repos/{repo}/pulls?state=all&sort=created&direction=asc&per_page=100",
    ])
    if not isinstance(raw, list):
        raise CorpusBuildError(f"gh api returned non-list for {repo} pulls")
    out: list[dict[str, Any]] = []
    for row in raw:
        if not isinstance(row, dict):
            continue
        number = row.get("number")
        merged_at = row.get("merged_at")
        sha = row.get("merge_commit_sha")
        if not merged_at or not sha:
            # GitHub sets merge_commit_sha on UNMERGED PRs too (the last
            # test-merge). Only merged_at proves a real merge commit.
            counts["dropped_merges_unmerged"] += 1
            continue
        if not str(merged_at) < cutoff:
            counts["dropped_merges_post_cutoff"] += 1
            continue
        if gold_pr_number is not None and number == gold_pr_number:
            counts["dropped_merges_gold"] += 1
            continue
        if number not in kept_numbers:
            counts["dropped_merges_orphan"] += 1
            continue
        if known_shas is not None and str(sha) not in known_shas:
            # The API's merge sha is not a commit in this checkout. Skipped
            # and counted — never a dangling edge, never a stub.
            counts["dropped_merges_absent_from_history"] += 1
            continue
        out.append({
            "number": number,
            "merge_commit_sha": str(sha),
            "merged_at": merged_at,
        })
    counts["kept_merges"] = len(out)
    seen_merged = counts["kept_merges"] + counts["dropped_merges_post_cutoff"] \
        + counts["dropped_merges_gold"] + counts["dropped_merges_orphan"] \
        + counts["dropped_merges_absent_from_history"]
    if not seen_merged:
        raise CorpusBuildError(
            f"{repo}: the pulls stream reported NO merged pull request with a "
            f"merge sha at all — a repository with pull requests but no merge "
            f"record is a failed fetch, not a project that never merged "
            f"anything. Counts: {counts}"
        )
    return out, counts


def checkout_commit_shas(checkout: Path) -> set[str]:
    """Every commit reachable in the corpus checkout — the same surface
    ``GitHistorySource`` walks, and therefore the exact set of
    ``git_commit`` nodes the published generation will contain."""
    proc = subprocess.run(
        ["git", "-C", str(checkout), "rev-list", "--all"],
        capture_output=True, text=True, check=False,
    )
    if proc.returncode != 0:
        raise CorpusBuildError(
            f"git rev-list failed in {checkout}: {proc.stderr.strip()[:300]}"
        )
    shas = set(proc.stdout.split())
    if not shas:
        raise CorpusBuildError(
            f"{checkout} has no reachable commits; the surgical checkout "
            "did not produce a history"
        )
    return shas


def _scrub_comment_updated_at(
    row: dict[str, Any], cutoff: str
) -> tuple[dict[str, Any], bool]:
    """Clamp a post-cutoff ``updated_at`` to ``created_at``.

    Same rule and same residual risk as the item scrub: the API returns
    the CURRENT body, so a comment edited after the cutoff contributes
    post-cutoff TEXT that no field exposes. The timestamp is clamped so
    the leakage audit's temporal assertion is honest about what is
    checkable; the text risk is disclosed, not papered over.
    """
    updated = row.get("updated_at")
    if updated and str(updated) >= cutoff:
        out = dict(row)
        out["updated_at"] = out.get("created_at")
        return out, True
    return row, False


def _scrub_post_cutoff_fields(
    item: dict[str, Any], cutoff: str
) -> tuple[dict[str, Any], bool]:
    """Return a copy of ``item`` with post-cutoff mutable fields restored to
    their as-of-cutoff values, plus whether anything changed."""
    out = dict(item)
    changed = False
    closed = out.get("closed_at")
    if closed and str(closed) >= cutoff:
        out["closed_at"] = None
        out["state"] = "open"
        out.pop("closed_by", None)
        out.pop("state_reason", None)
        changed = True
    pr = out.get("pull_request")
    if isinstance(pr, dict):
        merged = pr.get("merged_at")
        if merged and str(merged) >= cutoff:
            pr = dict(pr)
            pr["merged_at"] = None
            out["pull_request"] = pr
            changed = True
    updated = out.get("updated_at")
    if updated and str(updated) >= cutoff:
        out["updated_at"] = out.get("created_at")
        changed = True
    return out, changed


def _gh_json(args: list[str]) -> Any:
    proc = subprocess.run(
        ["gh", *args], capture_output=True, text=True, check=False
    )
    if proc.returncode != 0:
        raise CorpusBuildError(
            f"gh {' '.join(args[:2])} failed: {proc.stderr.strip()[:500]}"
        )
    return parse_gh_paginated(proc.stdout)


def parse_gh_paginated(text: str) -> Any:
    """``gh api --paginate`` emits page arrays back to back; merge them."""
    text = text.strip()
    if not text:
        raise CorpusBuildError("gh returned empty output")
    if text.startswith("["):
        merged: list[Any] = []
        decoder = json.JSONDecoder()
        idx = 0
        while idx < len(text):
            chunk, end = decoder.raw_decode(text, idx)
            if isinstance(chunk, list):
                merged.extend(chunk)
            else:
                merged.append(chunk)
            idx = end
            while idx < len(text) and text[idx] in " \n\r\t":
                idx += 1
        return merged
    return json.loads(text)


# -- deployment ---------------------------------------------------------


def _scaffold_deployment(spec: CorpusSpec, checkout: Path) -> Path:
    from okg.substrate.deployment_bootstrap.profile_init import (
        build_plan,
        discover_profile,
        scaffold_deployment,
    )

    deployments_root = spec.corpus_root / "deployments"
    profile = discover_profile("workspace-graph")
    answers: dict[str, Any] = {
        "deployment_name": spec.deployment_name,
        "postgres_dsn": spec.dsn,
        "repo_root": str(checkout),
        "since": "all",
    }
    if spec.mcp_port is not None:
        answers["mcp_port"] = spec.mcp_port
    plan = build_plan(
        profile=profile,
        deployment_name=spec.deployment_name,
        deployments_root=deployments_root,
        raw_answers=answers,
    )
    result = scaffold_deployment(plan, force=False)
    deployment_dir = Path(result.deployment_dir)
    if not (deployment_dir / "deployment.yaml").exists():
        raise CorpusBuildError(
            f"scaffold produced no deployment.yaml under {deployment_dir}"
        )
    return deployment_dir


def _load_registry(deployment_dir: Path) -> tuple[Path, dict[str, Any], dict[str, Any]]:
    """Load source_registry.yaml; ``sources`` is a MAPPING keyed by id."""
    import yaml

    registry_path = deployment_dir / "source_registry.yaml"
    registry = yaml.safe_load(registry_path.read_text(encoding="utf-8"))
    sources = registry.get("sources") if isinstance(registry, dict) else None
    if not isinstance(sources, dict):
        raise CorpusBuildError(
            f"{registry_path} has no sources mapping (got {type(sources).__name__})"
        )
    return registry_path, registry, sources


def _write_registry(registry_path: Path, registry: dict[str, Any]) -> None:
    import yaml

    registry_path.write_text(
        yaml.safe_dump(registry, sort_keys=False), encoding="utf-8"
    )


def _prune_registry_to_corpus_sources(deployment_dir: Path) -> set[str]:
    """Drop every scaffolded source outside CORPUS_SOURCE_IDS (operator
    environment sources must never feed a benchmark corpus). Returns the
    pruned ids."""
    registry_path, registry, sources = _load_registry(deployment_dir)
    missing = CORPUS_SOURCE_IDS - set(sources)
    if missing:
        raise CorpusBuildError(
            f"scaffold is missing expected corpus sources {sorted(missing)}; "
            f"present: {sorted(sources)}"
        )
    pruned = {sid for sid in sources if sid not in CORPUS_SOURCE_IDS}
    for sid in pruned:
        del sources[sid]
    # Drop dangling predecessor references onto pruned sources.
    kept_ownership = {
        str(spec.get("ownership_id", "")) for spec in sources.values()
    }
    for spec in sources.values():
        pred = spec.get("predecessor_source")
        if pred and str(pred) not in kept_ownership:
            spec.pop("predecessor_source", None)
    _write_registry(registry_path, registry)
    return pruned


def _register_github_source(
    deployment_dir: Path, spec: CorpusSpec, fixture_path: Path
) -> None:
    """Add the fixture-backed GitHubSource — BEFORE first publish, so the
    nomos baseline covers it. Block shape mirrors the canonical
    deployments/okg-workspace github-issues entry (admission policy
    included: workspace-graph runs nomos runtime_enforcement: enforce)."""
    registry_path, registry, sources = _load_registry(deployment_dir)
    if GITHUB_SOURCE_ID in sources:
        raise CorpusBuildError(f"{registry_path} already has {GITHUB_SOURCE_ID}")
    dep = spec.deployment_name
    ns = "okg_bench_corpus"
    clause = f"{ns}.github_issues"
    sources[GITHUB_SOURCE_ID] = {
        "module": "okg.substrate.library.sources.github",
        "class": "GitHubSource",
        "ownership_id": f"{dep}.{GITHUB_SOURCE_ID}",
        "params": {
            "repo": spec.repo,
            "fixture_path": str(fixture_path),
            "forge_host": "github",
            # The git_graph repo token: git history node ids are
            # `commit:<deployment_name>:<sha>` in this scaffold, and that
            # token is what `pull_request -merged_as-> git_commit` must
            # point at. Unset, the source RAISES on a payload carrying
            # merge shas rather than dropping the join silently.
            "commit_repo": spec.deployment_name,
        },
        "source_class": "mutable_api",
        "record_identity_kind": "remote_id",
        "record_identity_fields": ["repo", "number", "comment_id"],
        "source_revision_kind": "version_token",
        "deletion_semantics": "missing_from_completed_scope",
        "publication_mode": "published_generation",
        "admission_policy": {
            "producer_id": f"{dep}.{GITHUB_SOURCE_ID}",
            "producer_kind": "source",
            "trust_label": "external-source-trusted",
            "admission_mode": "fast_track",
            "authority_scope": {"source_family": ns},
            "allowed_record_kinds": ["node", "edge"],
            "output_signature": {
                "nodes": [
                    {"subtype": "issue", "id_prefix": "issue:github:",
                     "clause_id": f"{clause}.issue"},
                    {"subtype": "pull_request", "id_prefix": "pr:github:",
                     "clause_id": f"{clause}.pull_request"},
                    {"subtype": "repo", "id_prefix": "repo:github:",
                     "clause_id": f"{clause}.repo"},
                    {"subtype": "forge_comment", "id_prefix": "comment:github:",
                     "clause_id": f"{clause}.forge_comment"},
                ],
                "edges": [
                    {"src_subtype": "repo", "edge_type": "contains",
                     "dst_subtype": "issue",
                     "clause_id": f"{clause}.repo_contains_issue"},
                    {"src_subtype": "repo", "edge_type": "contains",
                     "dst_subtype": "pull_request",
                     "clause_id": f"{clause}.repo_contains_pull_request"},
                    {"src_subtype": "issue", "edge_type": "has_comment",
                     "dst_subtype": "forge_comment",
                     "clause_id": f"{clause}.issue_has_comment"},
                    {"src_subtype": "pull_request", "edge_type": "has_comment",
                     "dst_subtype": "forge_comment",
                     "clause_id": f"{clause}.pull_request_has_comment"},
                    {"src_subtype": "pull_request", "edge_type": "merged_as",
                     "dst_subtype": "git_commit",
                     "clause_id": f"{clause}.pull_request_merged_as"},
                ],
            },
            "policy_clauses": [
                {"id": f"{clause}.issue", "effect": "allow",
                 "record_kind": "node", "subtype": "issue"},
                {"id": f"{clause}.pull_request", "effect": "allow",
                 "record_kind": "node", "subtype": "pull_request"},
                {"id": f"{clause}.repo", "effect": "allow",
                 "record_kind": "node", "subtype": "repo"},
                {"id": f"{clause}.forge_comment", "effect": "allow",
                 "record_kind": "node", "subtype": "forge_comment"},
                {"id": f"{clause}.repo_contains_issue", "effect": "allow",
                 "record_kind": "edge",
                 "triple": ["repo", "contains", "issue"]},
                {"id": f"{clause}.repo_contains_pull_request", "effect": "allow",
                 "record_kind": "edge",
                 "triple": ["repo", "contains", "pull_request"]},
                {"id": f"{clause}.issue_has_comment", "effect": "allow",
                 "record_kind": "edge",
                 "triple": ["issue", "has_comment", "forge_comment"]},
                {"id": f"{clause}.pull_request_has_comment", "effect": "allow",
                 "record_kind": "edge",
                 "triple": ["pull_request", "has_comment", "forge_comment"]},
                {"id": f"{clause}.pull_request_merged_as", "effect": "allow",
                 "record_kind": "edge",
                 "triple": ["pull_request", "merged_as", "git_commit"]},
            ],
        },
    }
    _write_registry(registry_path, registry)


def _register_sessions_source(
    deployment_dir: Path, spec: CorpusSpec, checkout: Path, sessions_root: Path
) -> None:
    """Register the agent-sessions source (Claude Code transcripts) pointed
    at the corpus's OWN sessions root (initially empty). The okg arm's
    between-task ingest points it at the run's isolated CLAUDE_CONFIG_DIR
    projects dir; registering now keeps it under the nomos baseline."""
    registry_path, registry, sources = _load_registry(deployment_dir)
    if SESSIONS_SOURCE_ID in sources:
        raise CorpusBuildError(f"{registry_path} already has {SESSIONS_SOURCE_ID}")
    sources[SESSIONS_SOURCE_ID] = {
        "module": "okg.substrate.library.sources.agent_sessions",
        "class": "AgentSessionsSource",
        "ownership_id": f"{spec.deployment_name}.{SESSIONS_SOURCE_ID}",
        "params": {
            "repo": spec.deployment_name,
            "claude_projects_root": str(sessions_root),
            "repo_root": str(checkout),
            # Map EVERY session cwd to this corpus repo: the emitter derives
            # file-stub node ids from the cwd basename, and a basename that
            # differs from `repo` leaves orphan nodes outside the source's
            # record_set — the verified-delta guard then refuses the run
            # ("record_set omitted a node"). "/" is a universal prefix.
            "cwd_to_repo": {"/": spec.deployment_name},
            "since": "all",
        },
        # Classification mirrors deployments/okg-workspace agent-sessions;
        # discovery_crawl is covered by the profile's source_policy_defaults.
        "source_class": "discovery_crawl",
        "record_identity_kind": "scoped_locator",
        "record_identity_fields": ["repo", "node_id"],
        "source_revision_kind": "content_hash",
        "deletion_semantics": "missing_from_completed_scope",
        "publication_mode": "published_generation",
    }
    _write_registry(registry_path, registry)


def _patch_manifest_for_corpus(deployment_dir: Path, spec: CorpusSpec) -> None:
    """Three scaffold gaps the corpus needs, the first two mirrored from
    deployments/okg-workspace:

    - the ``forge`` ontology module (issue/pull_request/repo/forge_comment
      subtypes and their narrowings) is not in the workspace-graph
      profile's module list — without it every repo-contains-issue edge is
      a narrowing violation and the publish BLOCKS (observed: 2290/2290);
    - the nomos source_policy_defaults cover only discovery_crawl and
      reference_catalog; the github source is mutable_api;
    - ForgeReferenceLinker is not in any profile: it is this vertical's
      derived-edge stage (PACT okg-corpus-repo-history / req.repo.linkage).
    """
    import yaml

    manifest_path = deployment_dir / "deployment.yaml"
    manifest = yaml.safe_load(manifest_path.read_text(encoding="utf-8"))
    modules = manifest.get("modules")
    if not isinstance(modules, list):
        raise CorpusBuildError(
            f"{manifest_path} has no modules list; workspace-graph scaffold changed?"
        )
    if "forge" not in modules:
        modules.append("forge")
    nomos = manifest.get("nomos")
    if not isinstance(nomos, dict):
        raise CorpusBuildError(
            f"{manifest_path} has no nomos block; workspace-graph scaffold changed?"
        )
    _register_reference_enricher(manifest, spec)
    by_class = nomos.setdefault("source_policy_defaults", {}).setdefault(
        "by_source_class", {}
    )
    by_class.setdefault("mutable_api", {
        "store_raw": True,
        "sensitivity": "credentialed_external",
        "data_classification": "internal",
        "credential_ref_policy": "optional",
        "live_call_allowed": False,  # fixture-backed; the corpus never calls out
        "exportability": "internal",
        "retention": "normal",
        "provenance": "remote_api",
        "privacy_obligations": ["audit", "review"],
    })
    manifest_path.write_text(yaml.safe_dump(manifest, sort_keys=False), encoding="utf-8")


_LINKER_MODULE = "okg.substrate.library.linkers.forge_references"
REFERENCE_ENRICHER_CLASS = f"{_LINKER_MODULE}.ForgeReferenceLinker"
MERGE_ENRICHER_CLASS = f"{_LINKER_MODULE}.ForgeMergeAttributionLinker"


def _register_reference_enricher(
    manifest: dict[str, Any], spec: CorpusSpec
) -> None:
    """Wire the forge linkage enrichers into the corpus deployment.

    ``repo_slug`` scopes full-URL references to THIS repository: an
    unscoped URL regex would resolve another project's ``#123`` onto this
    corpus's node, fabricating a relationship the source never asserted.

    The merge-attribution linker is separate because it emits a different
    edge type from a different field, and a single class emitting both
    would hide one of them going silent behind the other's liveness.
    """
    enrichers = manifest.setdefault("enrichers", [])
    if not isinstance(enrichers, list):
        raise CorpusBuildError(
            "deployment.yaml `enrichers` is not a list; scaffold changed?"
        )
    present = {
        e.get("class") for e in enrichers if isinstance(e, dict)
    }
    if REFERENCE_ENRICHER_CLASS not in present:
        enrichers.append({
            "class": REFERENCE_ENRICHER_CLASS,
            "config": {"repo_slug": spec.repo, "forge_host": "github"},
        })
    if MERGE_ENRICHER_CLASS not in present:
        enrichers.append({"class": MERGE_ENRICHER_CLASS})


def _provision_and_publish(spec: CorpusSpec, deployment_dir: Path) -> str:
    """Migrate, apply catalog, seed nomos, ingest, publish — in process,
    mirroring cli/provision.py. Returns the published generation id."""
    from okg.substrate.cli.provision import (
        _apply_catalog,
        _load_main_helpers,
        _seed_nomos_baseline,
    )
    from okg.substrate.db.migrate import ensure_required_extensions, migrate
    from okg.substrate.ingest.service import IngestOptions, _publish, run_ingest

    _load_main_helpers()  # provision's helpers lazy-bind SubstrateError

    ensure_required_extensions(spec.dsn)
    migrate(spec.dsn, deployment_name=spec.deployment_name)
    _apply_catalog(deployment_dir, dsn=spec.dsn)
    _seed_nomos_baseline(spec.deployment_name, deployment_dir, dsn=spec.dsn)
    options = IngestOptions(
        dsn=spec.dsn,
        deployment=spec.deployment_name,
        deployment_dir=deployment_dir,
        run_mode="scope_complete",
        publish=False,
        command="okg-bench corpus build",
    )
    run_ingest(options)
    return _published_generation(_publish(options, "final"))


def _published_generation(publish_result: dict[str, Any] | None) -> str:
    """Extract the published generation id in the form the substrate's read
    paths accept: the chronos-native ``generation_id`` (``okg:...:gen:...``)
    — ``ChronosGraphStore.checkout_generation`` uses ids with an ``okg:``
    prefix verbatim as checkpoint ids, while the numeric
    ``compat_generation_id`` has no committed ``generation:<n>`` checkpoint
    under chronos-native storage. A blocked or failed publish is fatal."""
    out = publish_result or {}
    status = out.get("status")
    if status != "published":
        raise CorpusBuildError(
            f"publish did not publish (status={status!r}): "
            f"{ {k: out.get(k) for k in ('label', 'status', 'code', 'error', 'generation_id')} }"
        )
    generation = out.get("generation_id") or out.get("compat_generation_id")
    if generation is None:
        raise CorpusBuildError(f"publish returned no generation id: {out!r}")
    return str(generation)


def _write_mcp_config(spec: CorpusSpec, deployment_dir: Path) -> Path:
    """The ``--mcp-config`` file the okg arm mounts.

    ``--project`` pins uv to the okg checkout that built the corpus: the
    server subprocess is spawned with the TASK WORKDIR as cwd (an external
    repo with no pyproject), where a bare ``uv run`` would fail.
    """
    okg_repo_root = Path(__file__).resolve().parents[4]
    if not (okg_repo_root / "pyproject.toml").exists():
        raise CorpusBuildError(
            f"cannot locate the okg repo root from {__file__} "
            f"(tried {okg_repo_root}); corpus MCP config needs it"
        )
    mcp_config = {
        "mcpServers": {
            "okg": {
                "command": "uv",
                "args": [
                    "run", "--project", str(okg_repo_root), "--extra", "mcp",
                    "okg", "mcp-serve", "--deployment", spec.deployment_name,
                ],
                "env": {
                    "OKG_DSN": spec.dsn,
                    "OKG_DEPLOYMENTS_DIR": str(deployment_dir.parent),
                    "OKG_MCP_TOOL_ALLOWLIST": _OPERATOR_SURFACE,
                },
            }
        }
    }
    path = spec.corpus_root / "mcp-config.json"
    dump_json(path, mcp_config)
    return path


# -- between-task ingest (okg arm) --------------------------------------


def ingest_sessions_and_publish(
    *, corpus_cfg: dict[str, Any], sessions_root: Path
) -> str:
    """Ingest the completed task's session transcripts into the corpus
    deployment and publish a new generation (the okg arm's between-task
    accumulation step). Fails loudly; the next task must read a real graph.

    ``sessions_root`` is the RUN's CLAUDE_CONFIG_DIR ``projects`` dir; its
    transcripts are copied into the corpus's registered sessions root
    (``corpus_cfg["sessions_root"]``) rather than retargeting the registry —
    editing source_registry.yaml after ``catalog load --apply`` trips the
    substrate's fail-closed catalog identity guard
    (``catalog_ownership_mismatch``) and the publish refuses."""
    from okg.substrate.ingest.service import IngestOptions, _publish, run_ingest

    deployment_dir = Path(corpus_cfg["deployment_dir"])
    if not deployment_dir.exists():
        raise CorpusBuildError(f"corpus deployment dir missing: {deployment_dir}")
    _assert_registry_matches_claim(corpus_cfg, deployment_dir)
    _sync_run_sessions(corpus_cfg, sessions_root)
    options = IngestOptions(
        dsn=corpus_cfg["dsn"],
        deployment=corpus_cfg["deployment_name"],
        deployment_dir=deployment_dir,
        run_mode="scope_complete",
        publish=False,
        command="okg-bench between-tasks",
    )
    result = run_ingest(options)
    _assert_ingest_clean(result)
    return _published_generation(_publish(options, "final"))


def _assert_ingest_clean(result: Any) -> None:
    """``run_ingest`` reports per-source status without raising; a FAILed
    source (especially agent-sessions) would otherwise become a silent
    partial ingest — the okg arm would keep publishing generations that
    never accumulate session knowledge (P0 silent fallback)."""
    report = getattr(result, "report", None) or {}
    failed = [
        {"name": row.get("name"), "stage": row.get("stage"),
         "error": row.get("error")}
        for row in report.get("results", [])
        if str(row.get("status", "")).upper() not in ("OK", "SKIP", "SKIPPED")
    ]
    if failed:
        raise CorpusBuildError(
            f"between-task ingest had failing sources: {failed}"
        )


def _assert_registry_matches_claim(
    corpus_cfg: dict[str, Any], deployment_dir: Path
) -> None:
    """A corpus whose agent-sessions source points anywhere but the corpus's
    own sessions root has drifted from its catalog-claim state (e.g. an older
    builder retargeted it per run) — the substrate would refuse the publish
    with catalog_ownership_mismatch. Diagnose it here with the fix."""
    _, _, sources = _load_registry(deployment_dir)
    source = sources.get(SESSIONS_SOURCE_ID) or {}
    configured = str((source.get("params") or {}).get("claude_projects_root", ""))
    expected = str(corpus_cfg.get("sessions_root", ""))
    if configured != expected:
        raise CorpusBuildError(
            f"corpus registry drifted: {SESSIONS_SOURCE_ID}.claude_projects_root "
            f"is {configured!r}, expected the corpus sessions root {expected!r}. "
            "Restore that value (the state recorded by `okg catalog load "
            "--apply` at build time) or rebuild the corpus."
        )


def _sync_run_sessions(corpus_cfg: dict[str, Any], run_projects_root: Path) -> None:
    """Copy the run's session transcripts into the corpus's registered
    sessions root. Copy, not symlink: AgentSessionsSource walks real files,
    and the corpus stays self-contained after the run dir is pruned."""
    import shutil

    raw_dest = corpus_cfg.get("sessions_root")
    if not raw_dest:
        raise CorpusBuildError(
            "corpus.json has no sessions_root; rebuild the corpus with the "
            "current builder"
        )
    dest_root = Path(raw_dest)
    if not run_projects_root.exists():
        raise CorpusBuildError(
            f"run sessions dir missing: {run_projects_root} — the task session "
            "left no transcripts to ingest"
        )
    dest_root.mkdir(parents=True, exist_ok=True)
    shutil.copytree(run_projects_root, dest_root, dirs_exist_ok=True)
