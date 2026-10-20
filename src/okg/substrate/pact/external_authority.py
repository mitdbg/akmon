"""The merge-boundary authority surface for a PACT owned by another repository.

Issue #1424. A PACT can live in a repository that is not the one whose sync
root feeds the graph — a separate reference-deployments repository is the
reported case. Its
decision ledger is reachable (the ledger is a database, not a path), so the
operator approval records fine. What does NOT exist for it is a *projection*:
the registered sync root indexes only the core repository's `pact/` tree, so
the downstream manifest has no graph node and no event-plane intent, and
`gate pre-merge` refuses on `graph_not_ready` / `graph_not_fresh` /
`generation_pin_mismatch`. Publishing the downstream checkout to fix that is
correctly forbidden — an unmerged foreign checkout must never become canonical
state — which left the change with no admission path at all.

This module supplies the missing third surface, and nothing else:

  * It replaces the PROJECTION, never the DECISION LEDGER. The merge gate
    still demands a ledger approval binding the current intent digest, and an
    unreachable ledger still refuses. Preserving that is the whole point of
    calling this authority-preserving rather than an exemption.
  * Presence of the declaration is the selector, malformed included — the same
    fail-closed rule `published_generation_authority` uses. A surface that
    silently fell back to the graph on a typo would be an exemption wearing an
    authority's clothes.
  * The receipt binds EXACT identity: repository, head commit, tree, the
    current intent digest, and the core generation the approval was taken
    against, under one self-digest. Any drift in any of them refuses under its
    own named blocker, so a receipt cannot outlive the manifest, the commit, or
    the tree it was recorded for.
  * The receipt is a RUNTIME artifact of the checkout, never committed
    content. A committed receipt would sit inside the tree whose hash it
    binds — writing it changes that tree, committing it changes that head —
    so it could never name the tree that would actually merge. `git`-tracked
    receipts are refused, and only the receipt path itself is excluded from
    the worktree-cleanliness answer.
  * The authority repository is pinned by TRACKED policy
    (`pact/project.yaml` `external_authority.authority_repository`), not by
    the declaration. Comparing the declaration's own two strings to each other
    checks nothing an author cannot arrange, and the surface is refused both
    when the declaration names the pinned repository as its own and when the
    live origin remote IS the pinned repository. The surface exists because a
    foreign repository has no projection; used at home it would be a way to
    skip a graph that IS there.
  * The `receipt` path is a WRITE TARGET the manifest chooses, so it is
    validated before anything is written: repository-relative, no traversal,
    never under `.git/`, and refused when it or its parent resolves outside
    the checkout. Without that, a manifest could clobber repository internals
    or follow a committed symlink anywhere on the machine.

What a verified receipt does NOT establish, reported on every result in
`unverified` so no reader has to infer it:

  * that the core graph actually holds a projection of this manifest;
  * that the pinned generation was computed over this manifest;
  * that the external repository's own checks passed;
  * that the receipt was produced by a recorded decision at all. The self
    digest detects EDITING a receipt, not AUTHORING one — anyone who can write
    the checkout can compute a well-formed digest. What makes a receipt
    trustworthy is the ledger approval the merge gate separately demands.

It is an ATTESTATION with named provenance and exact identity binding, not a
re-derivation of the core graph. The distinction is the reason the provenance
block is emitted on accepted results too.
"""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
import re
import subprocess
from typing import Any

from okg.substrate.pact import envelope


# The `change.method_metadata` key whose PRESENCE selects this surface.
DECLARATION_KEY = "external_authority"
# The declaration's `kind`. A future second external surface gets its own kind
# rather than widening this one.
AUTHORITY_KIND = "external_repository_projection"
# What `workflow_authority` and every payload call this arm.
AUTHORITY_SCOPE = "external_repository_receipt"
# The `pact/project.yaml` block that pins which repository owns the graph.
SETTINGS_KEY = "external_authority"

_DECLARATION_KEYS = frozenset({
    "kind",
    "repository",
    "authority_repository",
    "receipt",
})
# Every field the receipt must carry, and — apart from `receipt_digest` itself
# — every field the self-digest covers. Unknown fields are refused rather than
# ignored: a field outside the digest is a field an editor could add without
# breaking the seal.
_RECEIPT_KEYS = frozenset({
    "change_id",
    "repository",
    "authority_repository",
    "head_sha",
    "tree_sha",
    "intent_digest",
    "generation_id",
    "deployment",
    "recorded_by",
    "recorded_at",
    "receipt_digest",
})
_BOUND_KEYS = _RECEIPT_KEYS - {"receipt_digest"}

# Facts the receipt asserts and this verification cannot check locally. Stated
# on EVERY result, accepted ones included.
UNVERIFIED_BY_RECEIPT = (
    "core_graph_holds_a_projection_of_this_manifest",
    "pinned_generation_was_computed_over_this_manifest",
    "external_repository_check_results",
    # The self digest detects EDITING a receipt, not AUTHORING one. Anyone who
    # can write the checkout can compute a well-formed digest, so what makes a
    # receipt trustworthy is the ledger approval the merge gate separately
    # demands, never the receipt on its own.
    "receipt_was_produced_by_a_recorded_decision",
)

_REPO_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*/[A-Za-z0-9][A-Za-z0-9._-]*$")
_SHA_RE = re.compile(r"^[0-9a-f]{40}$|^[0-9a-f]{64}$")
_DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
# Repository-relative, and a leading dot is allowed because the receipt is a
# runtime artifact that belongs under an ignored directory such as `.okg/`.
# Traversal is refused separately, on the parsed parts.
_RECEIPT_PATH_RE = re.compile(r"^[A-Za-z0-9.][A-Za-z0-9._/-]*$")


@dataclass(frozen=True)
class RepositoryIdentity:
    """What the live checkout is, as far as git will say."""

    head_sha: str | None = None
    tree_sha: str | None = None
    remote: str | None = None
    clean: bool | None = None


def change_declares_external_authority(manifest: Any) -> bool:
    """Presence is the opt-in boundary, malformed declarations included."""
    metadata = getattr(getattr(manifest, "change", None), "method_metadata", None)
    return isinstance(metadata, dict) and DECLARATION_KEY in metadata


def receipt_digest(payload: dict[str, Any]) -> str:
    """The self-digest over the receipt's bound fields.

    Canonical JSON with sorted keys and no insignificant whitespace, so the
    producer and the verifier compute the same bytes. `receipt_digest` is
    excluded — a digest cannot cover itself.
    """
    bound = {
        key: payload[key]
        for key in sorted(_BOUND_KEYS)
        if key in payload
    }
    encoded = json.dumps(bound, sort_keys=True, separators=(",", ":")).encode()
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def repository_identity(
    repo_root: Path,
    *,
    receipt_path: str | None = None,
) -> RepositoryIdentity:
    """Head, tree, origin remote and cleanliness of the live checkout.

    Every field degrades to None rather than raising: an unreadable field is
    reported as unknown and refused by the caller, never guessed.

    `receipt_path` is excluded from the cleanliness answer, and only that one
    path. A receipt binds the head and tree it was recorded against, so a
    receipt that were itself part of the tree could never be satisfied:
    writing it changes the tree it names, committing it changes the head it
    names. It is therefore a runtime artifact of the checkout, not repository
    content — `receipt_is_tracked` refuses the committed case outright.
    """

    def run(*args: str) -> str | None:
        try:
            done = subprocess.run(
                ["git", "-C", str(repo_root), *args],
                capture_output=True,
                text=True,
                timeout=5,
                check=False,
            )
        except (OSError, subprocess.SubprocessError):
            return None
        return done.stdout.strip() if done.returncode == 0 else None

    # `--untracked-files=all` is load-bearing: the default `normal` collapses
    # an untracked directory to one `?? .okg/` line, so excluding the receipt
    # PATH would never match and the accepting branch would be unreachable
    # unless the directory happened to be ignored.
    status = run("status", "--porcelain=v1", "--untracked-files=all")
    clean: bool | None = None
    if status is not None:
        entries = [line for line in status.splitlines() if line.strip()]
        if receipt_path:
            entries = [
                line for line in entries if line[3:].strip().strip('"') != receipt_path
            ]
        clean = not entries
    return RepositoryIdentity(
        head_sha=run("rev-parse", "HEAD"),
        tree_sha=run("rev-parse", "HEAD^{tree}"),
        remote=_remote_slug(run("config", "--get", "remote.origin.url")),
        clean=clean,
    )


def receipt_is_tracked(repo_root: Path, receipt_path: str) -> bool:
    """Is the receipt committed to the repository?

    A tracked receipt is refused: it would be inside the tree whose hash it
    binds, so the artifact could never name the tree that would actually
    merge.
    """
    try:
        done = subprocess.run(
            ["git", "-C", str(repo_root), "ls-files", "--error-unmatch", receipt_path],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return False
    return done.returncode == 0


def _remote_slug(url: str | None) -> str | None:
    """`owner/name` from an origin URL, or None when it does not look like one."""
    if not url:
        return None
    text = url.strip().removesuffix(".git")
    if text.startswith("git@") and ":" in text:
        text = text.split(":", 1)[1]
    else:
        text = re.sub(r"^[a-z+]+://[^/]+/", "", text)
    parts = [part for part in text.split("/") if part]
    if len(parts) < 2:
        return None
    slug = "/".join(parts[-2:])
    return slug if _REPO_RE.match(slug) else None


def configured_authority_repository(repo_root: Path) -> str | None:
    """The authority repository pinned in `pact/project.yaml`.

    The pin has to come from tracked repository policy, not from the
    manifest. Comparing the declaration's two own strings to each other
    checks nothing an author cannot arrange: `repository: <home>` beside
    `authority_repository: <other>` differs, parses clean, and would
    have skipped the projection inside the very repository that owns it.
    """
    from okg.substrate.pact.project_settings import load_project_settings

    try:
        settings = load_project_settings(repo_root)
    except Exception:  # noqa: BLE001 - an unreadable policy is a refusal
        return None
    block = settings.get(SETTINGS_KEY)
    if not isinstance(block, dict):
        return None
    value = block.get("authority_repository")
    if isinstance(value, str) and _REPO_RE.match(value):
        return value
    return None


def _receipt_path_is_safe(receipt: str) -> bool:
    """Repository-relative, no traversal, and never inside `.git/`.

    The regex admits a leading dot so the receipt can live under an ignored
    directory such as `.okg/`. That also admits `.git/description`, which a
    write would clobber, so the first path part is checked by name.
    """
    if not _RECEIPT_PATH_RE.match(receipt):
        return False
    path = Path(receipt)
    parts = path.parts
    return bool(parts) and not path.is_absolute() and ".." not in parts and parts[0] != ".git"


def receipt_target(repo_root: Path, receipt: str) -> Path | None:
    """The absolute receipt path, or None when it leaves the checkout.

    Checked on the PARENT as well as the file: a committed symlink at the
    receipt path resolves outside the repository, and `resolve()` on a
    not-yet-existing file would not have caught a symlinked directory above
    it. This is what stops a manifest from steering a write anywhere on the
    machine.
    """
    root = repo_root.resolve()
    candidate = root / receipt
    for probe in (candidate, candidate.parent):
        try:
            resolved = probe.resolve()
            resolved.relative_to(root)
        except (OSError, ValueError):
            return None
    return candidate.resolve()


def parse_declaration(
    manifest: Any,
    *,
    repo_root: Path | None = None,
) -> tuple[dict[str, str] | None, list[str]]:
    """Parse the declaration without touching the ledger.

    `repo_root` is read only for the tracked authority-repository pin. It is
    optional so the parse can be exercised alone, and its ABSENCE is a
    refusal, not a pass: an unpinned authority is the bypass this guard
    exists to close.
    """
    metadata = getattr(getattr(manifest, "change", None), "method_metadata", None)
    if not isinstance(metadata, dict) or DECLARATION_KEY not in metadata:
        return None, []
    raw = metadata.get(DECLARATION_KEY)
    if not isinstance(raw, dict):
        return None, ["external_declaration_invalid"]
    blockers: list[str] = []
    missing = sorted(_DECLARATION_KEYS - set(raw))
    unknown = sorted(set(raw) - _DECLARATION_KEYS)
    if missing:
        blockers.append("external_declaration_incomplete")
    if unknown:
        blockers.append("external_declaration_unknown_field")
    if raw.get("kind") != AUTHORITY_KIND:
        blockers.append("external_declaration_unknown_kind")
    repository = raw.get("repository")
    authority = raw.get("authority_repository")
    for value in (repository, authority):
        if not isinstance(value, str) or not _REPO_RE.match(value):
            blockers.append("external_declaration_invalid_repository")
            break
    receipt = raw.get("receipt")
    if not isinstance(receipt, str) or not _receipt_path_is_safe(receipt):
        blockers.append("external_declaration_invalid_receipt_path")
    configured = (
        configured_authority_repository(repo_root) if repo_root is not None else None
    )
    if configured is None:
        blockers.append("external_authority_repository_unconfigured")
    else:
        # The declaration may not choose its own authority. Naming a
        # different one is the bypass spelling: it makes the two self-declared
        # strings differ while the checkout still IS the authority repository.
        if authority != configured:
            blockers.append("external_declaration_authority_repository_not_pinned")
        # The surface exists because a FOREIGN repository has no projection.
        # Used in the repository that owns the graph it would be a way to skip
        # a projection that is actually available.
        if repository == configured:
            blockers.append("external_authority_is_the_authority_repository")
    if blockers:
        return None, sorted(set(blockers))
    return {
        "kind": str(raw["kind"]),
        "repository": str(repository),
        "authority_repository": str(authority),
        "receipt": str(receipt),
    }, []


def _load_receipt(path: Path) -> tuple[dict[str, Any] | None, str | None]:
    if not path.is_file():
        return None, "external_receipt_missing"
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None, "external_receipt_unreadable"
    if not isinstance(payload, dict):
        return None, "external_receipt_unreadable"
    return payload, None


def verify_external_authority(
    repo_root: Path,
    manifest: Any,
    *,
    intent_digest: str,
    identity: RepositoryIdentity | None = None,
) -> dict[str, Any]:
    """Fail-closed verdict for the external merge-authority surface.

    Returns an envelope component shaped like the graph-projection status the
    merge boundary already consumes (`status`, `stale`, `authority_scope`,
    `projected_generation_id`) plus the provenance and limits this surface
    must always publish.
    """
    declaration, blockers = parse_declaration(manifest, repo_root=repo_root)
    identity = identity or repository_identity(
        repo_root,
        receipt_path=declaration["receipt"] if declaration else None,
    )
    provenance: dict[str, Any] = {
        "declaration": declaration,
        "receipt_path": declaration["receipt"] if declaration else None,
        "checkout": {
            "head_sha": identity.head_sha,
            "tree_sha": identity.tree_sha,
            "remote": identity.remote,
            "clean": identity.clean,
        },
    }
    receipt: dict[str, Any] | None = None
    if declaration is not None:
        # The checkout may not BE the authority repository, whatever the
        # declaration says about itself. The origin remote is the fact; it is
        # consulted only when git resolved one.
        configured = configured_authority_repository(repo_root)
        if configured is not None and identity.remote == configured:
            blockers.append("external_authority_is_the_authority_repository")
        target = receipt_target(repo_root, declaration["receipt"])
        if target is None:
            blockers.append("external_receipt_path_escapes_repository")
        if receipt_is_tracked(repo_root, declaration["receipt"]):
            blockers.append("external_receipt_is_tracked")
        if target is not None:
            receipt, load_blocker = _load_receipt(target)
            if load_blocker:
                blockers.append(load_blocker)
            else:
                blockers.extend(
                    _receipt_blockers(
                        receipt or {},
                        declaration,
                        manifest=manifest,
                        intent_digest=intent_digest,
                        identity=identity,
                    )
                )
    if receipt is not None:
        provenance["receipt"] = {
            key: receipt.get(key)
            for key in sorted(_RECEIPT_KEYS)
            if key in receipt
        }
    blockers = sorted(set(blockers))
    ok = not blockers
    return envelope.component(
        ok=ok,
        authority_scope=AUTHORITY_SCOPE,
        # Never "fresh": that word belongs to a graph projection this arm did
        # not read, and reporting it here would let a reader believe the
        # manifest was projected. The verdict is carried by `ok` and
        # `blockers`; `status` says what happened to the projection, which is
        # nothing.
        status="not_projected",
        stale=None,
        # The pin this surface reports in place of a projected generation. It
        # is the generation the approval was taken against, not one this
        # checkout observed — hence its place in `unverified` as well.
        projected_generation_id=(receipt or {}).get("generation_id") if ok else None,
        blockers=blockers,
        provenance=provenance,
        attested=True,
        verified_against_graph=False,
        unverified=list(UNVERIFIED_BY_RECEIPT),
        reason=(
            None
            if ok
            else "external repository approval receipt does not bind this checkout"
        ),
        next_action="none" if ok else "record_external_approval_receipt",
        # No command is published here on purpose. The receipt is written by
        # `okg pact decide --verdict approve` running in this checkout, which
        # is the same call that records the ledger approval — a separate
        # "regenerate the receipt" verb would let a checkout mint its own
        # authority without a decision behind it.
        recommended_commands=[],
    )


def _receipt_blockers(
    receipt: dict[str, Any],
    declaration: dict[str, str],
    *,
    manifest: Any,
    intent_digest: str,
    identity: RepositoryIdentity,
) -> list[str]:
    blockers: list[str] = []
    missing = sorted(_RECEIPT_KEYS - set(receipt))
    unknown = sorted(set(receipt) - _RECEIPT_KEYS)
    if missing:
        blockers.append("external_receipt_incomplete")
    if unknown:
        blockers.append("external_receipt_unknown_field")
    if missing or unknown:
        # The self-digest is only meaningful over the exact declared field
        # set; checking it against a wrong-shaped document would report a
        # second, misleading fault for one cause.
        return blockers
    if not _DIGEST_RE.match(str(receipt.get("receipt_digest") or "")):
        blockers.append("external_receipt_digest_malformed")
    elif receipt["receipt_digest"] != receipt_digest(receipt):
        blockers.append("external_receipt_digest_mismatch")
    change_id = getattr(getattr(manifest, "change", None), "id", None)
    if receipt.get("change_id") != change_id:
        blockers.append("external_receipt_change_mismatch")
    if receipt.get("repository") != declaration["repository"]:
        blockers.append("external_receipt_repository_mismatch")
    if receipt.get("authority_repository") != declaration["authority_repository"]:
        blockers.append("external_receipt_authority_repository_mismatch")
    # The origin remote is checked only when git could resolve one: an absent
    # remote is not evidence of a different repository, and the head and tree
    # bindings below already pin the checkout.
    if identity.remote is not None and identity.remote != declaration["repository"]:
        blockers.append("external_receipt_repository_mismatch")
    if receipt.get("intent_digest") != intent_digest:
        blockers.append("external_receipt_intent_digest_mismatch")
    if not _SHA_RE.match(str(receipt.get("head_sha") or "")):
        blockers.append("external_receipt_head_malformed")
    elif identity.head_sha is None:
        blockers.append("external_receipt_head_unreadable")
    elif receipt["head_sha"] != identity.head_sha:
        blockers.append("external_receipt_head_mismatch")
    if not _SHA_RE.match(str(receipt.get("tree_sha") or "")):
        blockers.append("external_receipt_tree_malformed")
    elif identity.tree_sha is None:
        blockers.append("external_receipt_tree_unreadable")
    elif receipt["tree_sha"] != identity.tree_sha:
        blockers.append("external_receipt_tree_mismatch")
    if not str(receipt.get("generation_id") or "").strip():
        blockers.append("external_receipt_generation_unpinned")
    for field in ("deployment", "recorded_by", "recorded_at"):
        if not str(receipt.get(field) or "").strip():
            blockers.append("external_receipt_provenance_incomplete")
            break
    # A dirty worktree means the tree the receipt names is not the tree that
    # would merge, even when HEAD still matches.
    if identity.clean is None:
        blockers.append("external_receipt_worktree_unreadable")
    elif not identity.clean:
        blockers.append("external_receipt_worktree_not_clean")
    return blockers


def build_receipt(
    manifest: Any,
    declaration: dict[str, str],
    *,
    intent_digest: str,
    generation_id: str,
    deployment: str,
    recorded_by: str,
    recorded_at: str,
    identity: RepositoryIdentity,
) -> dict[str, Any]:
    """The receipt a recorded approval in this checkout produces.

    Every bound field is read from the live checkout or the caller's ledger
    result — none of them is defaulted, because a defaulted identity field is
    an unbound receipt that still passes its own digest check.
    """
    payload = {
        "change_id": str(getattr(manifest.change, "id")),
        "repository": declaration["repository"],
        "authority_repository": declaration["authority_repository"],
        "head_sha": identity.head_sha or "",
        "tree_sha": identity.tree_sha or "",
        "intent_digest": intent_digest,
        "generation_id": generation_id,
        "deployment": deployment,
        "recorded_by": recorded_by,
        "recorded_at": recorded_at,
    }
    payload["receipt_digest"] = receipt_digest(payload)
    return payload


def write_receipt(
    repo_root: Path,
    manifest: Any,
    *,
    intent_digest: str,
    generation_id: str | None,
    deployment: str,
    recorded_by: str,
    recorded_at: str,
    identity: RepositoryIdentity | None = None,
) -> dict[str, Any]:
    """Write the receipt for a recorded approval, or say why it was not written.

    Never raises and never writes a partial receipt: a missing generation pin,
    an unreadable checkout or a malformed declaration all return
    `written: false` with a reason, so the decide payload reports the gap
    instead of leaving an approval that the merge gate will silently refuse.
    """
    declaration, blockers = parse_declaration(manifest, repo_root=repo_root)
    identity = identity or repository_identity(
        repo_root,
        receipt_path=declaration["receipt"] if declaration else None,
    )
    if declaration is None:
        return {
            "written": False,
            "reason": "external authority declaration is not usable",
            "blockers": blockers,
        }
    # Every reason to refuse is checked BEFORE the write. A manifest is
    # authored content: a `receipt` path is a write target it chooses, so an
    # unchecked write here would let a manifest clobber `.git/description`, or
    # follow a committed symlink out of the checkout entirely.
    if receipt_target(repo_root, declaration["receipt"]) is None:
        return {
            "written": False,
            "reason": (
                "the declared receipt path resolves outside the repository; "
                "refusing to write"
            ),
            "blockers": ["external_receipt_path_escapes_repository"],
        }
    if receipt_is_tracked(repo_root, declaration["receipt"]):
        return {
            "written": False,
            "reason": (
                "the declared receipt path is tracked by git; the receipt is a "
                "runtime artifact and must be untracked"
            ),
            "blockers": ["external_receipt_is_tracked"],
        }
    if not str(generation_id or "").strip():
        return {
            "written": False,
            "reason": (
                "no core generation pin: set OKG_PACT_EXTERNAL_GENERATION_ID to "
                "the generation the approval was taken against"
            ),
            "blockers": ["external_receipt_generation_unpinned"],
        }
    if not identity.head_sha or not identity.tree_sha:
        return {
            "written": False,
            "reason": "git could not resolve the head or tree of this checkout",
            "blockers": ["external_receipt_head_unreadable"],
        }
    payload = build_receipt(
        manifest,
        declaration,
        intent_digest=intent_digest,
        generation_id=str(generation_id),
        deployment=deployment,
        recorded_by=recorded_by,
        recorded_at=recorded_at,
        identity=identity,
    )
    path = receipt_target(repo_root, declaration["receipt"]) or (
        repo_root / declaration["receipt"]
    )
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(payload, sort_keys=True, indent=2) + "\n",
            encoding="utf-8",
        )
    except OSError as exc:
        return {
            "written": False,
            "reason": f"could not write the receipt: {exc}",
            "blockers": ["external_receipt_missing"],
        }
    return {
        "written": True,
        "path": declaration["receipt"],
        "receipt_digest": payload["receipt_digest"],
        # Writing the receipt does not make its claims true. The same limits
        # the gate reports are stated at production time.
        "unverified": list(UNVERIFIED_BY_RECEIPT),
    }
