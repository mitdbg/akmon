#!/usr/bin/env python3
"""The steps of akmon's release workflow (.github/workflows/release.yml).

akmon publishes okg releases (decisions akmonrel A, akmongate A2). The
private development repository prepares a candidate: the public-mirror
export as one commit on top of akmon's main, the release notes, this
release's contract and release.json. After a required reviewer approves,
this workflow takes that candidate, pushes it, builds and proves the wheel,
both images and the chart from akmon's own tree, and publishes them.

Order is the point. Every check that can refuse runs before the commit and
the tag are pushed; the images are pushed by digest only and proved; only
then does ``publish`` give the proven bytes their release tags, push the
chart, and publish the GitHub release.

Each subcommand is one step and refuses, naming what is wrong:

``fetch-candidate``   fetches the exact hand-over commit the dispatch names from
                      the development repository's ``refs/akmon-candidates/<tag>``
                      (Contents read only), refuses unless the ref is at that
                      SHA, the commit has no parent, its tree is exactly
                      ``candidate/`` with the four files and
                      ``candidate.bundle``, the bundle's sha256 is the one its
                      message records and it verifies against main, the
                      candidate commit it holds is the one release.json names,
                      and the okg commit release.json names is on the
                      development repository's dev; then writes the files.
``verify-candidate``  the downloaded candidate is one commit whose parent is
                      akmon's main, whose tree and version are what
                      release.json says, for a tag akmon does not have yet,
                      whose release.json names the hand-written notes
                      as notes_source, and whose notes.md and
                      release-body.md are those notes, byte for byte.
``tree-digest`` / ``same-tree``  a digest over a directory's file names and
                      bytes, and the check that a downloaded artifact still
                      holds exactly what the job that uploaded it digested.
``history``           earlier akmon releases from v0.2.0 on, each carrying its
                      contract asset; any gap refuses.
``stage-contracts``   downloads those contracts into build/release-contracts,
                      gzipped with mtime 0, and prints each file's SHA-256.
``check-wheel``       the built wheel is this version and carries exactly the
                      staged contracts, byte for byte.
``same-contract``     the contract built here equals the candidate's.
``wheel-source``      every file of the wheel is a byte-for-byte copy of the
                      release tree (``src/...``, ``schemas/``), the staged
                      contracts, or its build record naming this commit and
                      tree, clean; nothing else.
``absent`` / ``reference-state`` / ``same-chart`` / ``tag``  release
                      references are written once: refuse an existing one,
                      accept a rerun holding the same bytes, and tag a proven
                      digest byte for byte.
``chart``             the published chart: renamed ``akmon``, version X.Y.Z,
                      appVersion vX.Y.Z, released annotation ``"true"``, both
                      image repositories set to akmon's packages.
``verify-installed``  runs inside the runtime image: okg there is the clean
                      wheel of this commit and tree, carrying exactly the
                      staged contracts.
``notes``             appends the Install section with the three digests to a
                      release-body file. The hand-written notes plus this
                      section are the whole release text; no notes file is
                      attached.
``assets``            which local files a GitHub release (draft or not) does
                      not yet hold byte for byte, by each asset's sha256
                      digest; ``--check`` refuses unless it holds them all.
``public``            pulls the three manifests anonymously; the release stays a
                      draft until this passes.

Exit status: 0 done, 1 refused or failed, 2 usage error.
"""

from __future__ import annotations

import argparse
import base64
import gzip
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tomllib
import urllib.error
import urllib.parse
import urllib.request
import zipfile
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

REPOSITORY = "mitdbg/akmon"
ORGANIZATION = REPOSITORY.split("/")[0]
API_ROOT = "https://api.github.com"
REGISTRY = "ghcr.io"
#: Decision "chart akmon": the release packages are named for akmon.
RUNTIME_IMAGE = f"{REGISTRY}/mitdbg/akmon"
POSTGRES_IMAGE = f"{REGISTRY}/mitdbg/akmon-postgres"
CHART_REPOSITORY = f"oci://{REGISTRY}/mitdbg/charts"
#: The chart in the source tree is named okg; the published chart is akmon,
#: so its OCI path ends in /akmon.
SOURCE_CHART_NAME = "okg"
CHART_NAME = "akmon"
CHART_REFERENCE = f"{CHART_REPOSITORY}/{CHART_NAME}"
RELEASED_ANNOTATION = "okg.mitdbg.io/released"
PACKAGE = "okg"
TAG_PATTERN = re.compile(r"v0\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)")
#: Release contracts start here (decision 312).
CONTRACT_HISTORY_STARTS = (0, 2, 0)
CONTRACT_BUNDLE_DIR = Path("build") / "release-contracts"
RELEASE_RECORD = "release.json"
#: Where the development repository publishes a candidate (decision
#: candidatehandoff A): one sidecar commit at refs/akmon-candidates/<tag>. The
#: repository itself is passed in by the workflow, the one file that names it.
CANDIDATE_REF_PREFIX = "refs/akmon-candidates/"
CANDIDATE_DIR = "candidate"
#: The candidate commit, bundled against akmon's main, as a file of the
#: hand-over commit (so okg never holds akmon's workflow files as commits).
CANDIDATE_BUNDLE = "candidate.bundle"
CANDIDATE_BUNDLE_SHA = re.compile(r"^candidate bundle sha256 ([0-9a-f]{64})$", re.MULTILINE)
#: The development branch the okg commit a candidate names must be on.
CANDIDATE_BRANCH = "dev"
#: GitHub refuses a release body longer than this many characters.
RELEASE_BODY_LIMIT = 125_000
DIGEST = re.compile(r"sha256:[0-9a-f]{64}")
GIT_ID = re.compile(r"[0-9a-f]{40}")
NOTES_START = "<!-- okg-release-packages:start -->"
NOTES_END = "<!-- okg-release-packages:end -->"
MANIFEST_ACCEPT = ", ".join((
    "application/vnd.oci.image.index.v1+json",
    "application/vnd.oci.image.manifest.v1+json",
    "application/vnd.docker.distribution.manifest.list.v2+json",
    "application/vnd.docker.distribution.manifest.v2+json",
))

#: (method, url, headers, body) -> (status, body). Never raises for an HTTP
#: status; raises OSError when the server cannot be reached.
Http = Callable[[str, str, Mapping[str, str], bytes | None], tuple[int, bytes]]
Runner = Callable[[Sequence[str], Path], "subprocess.CompletedProcess[str]"]


class Refused(RuntimeError):
    """A release step that must not go on; the message says why."""


def parse_tag(tag: str) -> tuple[int, int, int] | None:
    match = TAG_PATTERN.fullmatch(tag)
    return None if match is None else (0, int(match.group(1)), int(match.group(2)))


def notes_source(tag: str) -> str:
    """What release.json names as the notes' source: the hand-written file.
    Kept equal to the development repository's cut_release.notes_source."""
    return f"release/notes/{tag}.md (hand-written)"


def contract_asset_name(tag: str) -> str:
    return f"okg-contract-{tag}.json"


def http_request(method: str, url: str, headers: Mapping[str, str], body: bytes | None) -> tuple[int, bytes]:
    request = urllib.request.Request(url, data=body, method=method, headers=dict(headers))
    try:
        with urllib.request.urlopen(request, timeout=60) as response:  # noqa: S310 - fixed https hosts
            return response.status, response.read()
    except urllib.error.HTTPError as error:
        return error.code, error.read()


def run_command(argv: Sequence[str], cwd: Path) -> "subprocess.CompletedProcess[str]":
    return subprocess.run(list(argv), cwd=cwd, capture_output=True, text=True, check=False)


def _token() -> str:
    token = os.environ.get("GITHUB_TOKEN", "")
    if not token:
        raise Refused("GITHUB_TOKEN is not set")
    return token


def _api_headers(token: str) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }


def _json(status: int, body: bytes, what: str) -> Any:
    if status not in (200, 201):
        raise Refused(f"{what}: HTTP {status}: {body[:200].decode('utf-8', 'replace')}")
    try:
        return json.loads(body)
    except ValueError as error:
        raise Refused(f"{what}: not JSON: {error}") from error


def write_outputs(values: Mapping[str, str]) -> None:
    """Append ``key=value`` lines to $GITHUB_OUTPUT, or print them."""
    lines = []
    for key, value in values.items():
        if "\n" in value:
            raise Refused(f"output {key} has a newline")
        lines.append(f"{key}={value}")
    path = os.environ.get("GITHUB_OUTPUT")
    if path:
        with open(path, "a", encoding="utf-8") as handle:
            handle.write("\n".join(lines) + "\n")
    print("\n".join(lines))



# --- releases ------------------------------------------------------------------


def _git(run: Runner, repo: Path, *args: str) -> str:
    result = run(["git", *args], repo)
    if result.returncode != 0:
        raise Refused(f"git {' '.join(args)} failed: {(result.stderr or result.stdout).strip()[:300]}")
    return result.stdout.strip()


def release_view(tag: str, http: Http, token: str) -> dict[str, Any] | None:
    """The GitHub release of ``tag``, or None when there is none."""
    status, body = http("GET", f"{API_ROOT}/repos/{REPOSITORY}/releases/tags/{tag}", _api_headers(token), None)
    if status == 404:
        return None
    data = _json(status, body, f"reading the release {tag}")
    if not isinstance(data, dict):
        raise Refused(f"reading the release {tag}: unexpected answer")
    return data


def _require_release(tag: str, http: Http, token: str) -> dict[str, Any]:
    release = release_view(tag, http, token)
    if release is None:
        raise Refused(f"no GitHub release has the tag {tag}")
    return release


def find_release(tag: str, http: Http, token: str) -> dict[str, Any] | None:
    """The release of ``tag``, drafts included (the tags endpoint skips a
    draft), or None."""
    for page in range(1, 11):
        status, body = http(
            "GET", f"{API_ROOT}/repos/{REPOSITORY}/releases?per_page=100&page={page}", _api_headers(token), None,
        )
        releases = _json(status, body, "listing akmon's releases")
        if not isinstance(releases, list):
            raise Refused("listing akmon's releases: unexpected answer")
        for release in releases:
            if isinstance(release, dict) and release.get("tag_name") == tag:
                return release
        if len(releases) < 100:
            return None
    raise Refused("more than 1000 akmon releases; refusing to guess")


def stale_assets(tag: str, files: Sequence[Path], http: Http, token: str) -> list[Path]:
    """The files the release of ``tag`` does not hold byte for byte: no asset
    of that name, one that did not finish uploading, or one whose sha256
    digest differs. An asset GitHub reports with no digest refuses: its bytes
    cannot be compared."""
    release = find_release(tag, http, token)
    if release is None:
        raise Refused(f"no GitHub release, draft or published, has the tag {tag}")
    by_name = {str(a.get("name")): a for a in release.get("assets") or [] if isinstance(a, dict)}
    stale = []
    for path in files:
        asset = by_name.get(path.name)
        if asset is None or asset.get("state") != "uploaded":
            stale.append(path)
            continue
        digest = asset.get("digest")
        if not isinstance(digest, str) or not DIGEST.fullmatch(digest):
            raise Refused(f"the release asset {path.name} reports no sha256 digest ({digest!r}); cannot compare it")
        try:
            local = "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()
        except OSError as error:
            raise Refused(f"cannot read {path}: {error}") from error
        if digest != local:
            stale.append(path)
    return stale


def _asset_names(release: Mapping[str, Any]) -> set[str]:
    return {str(a.get("name")) for a in release.get("assets") or [] if isinstance(a, dict)}


# --- the candidate ------------------------------------------------------------


def candidate_files(tag: str) -> tuple[str, ...]:
    return (RELEASE_RECORD, "notes.md", "release-body.md", contract_asset_name(tag))


def _git_auth_env(token: str) -> dict[str, str]:
    """The environment for git against GitHub with ``token``: the header goes
    in through GIT_CONFIG_* variables, never the command line or a URL."""
    env = {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}
    env["GIT_TERMINAL_PROMPT"] = "0"
    if token:
        basic = base64.b64encode(f"x-access-token:{token}".encode()).decode()
        env.update({
            "GIT_CONFIG_COUNT": "1",
            "GIT_CONFIG_KEY_0": "http.https://github.com/.extraheader",
            "GIT_CONFIG_VALUE_0": f"AUTHORIZATION: basic {basic}",
        })
    return env


def _git_bytes(repo: Path, env: Mapping[str, str], *args: str) -> bytes:
    result = subprocess.run(["git", *args], cwd=repo, env=dict(env), capture_output=True, check=False)
    if result.returncode != 0:
        detail = (result.stderr or result.stdout).decode("utf-8", "replace").strip()[:300]
        raise Refused(f"git {args[0]} failed: {detail}")
    return result.stdout


def on_development_branch(repository: str, commit: str, http: Http, token: str) -> None:
    """``commit`` is an ancestor of (or is) the development repository's dev,
    per the compare API, which needs only Contents read."""
    status, body = http(
        "GET", f"{API_ROOT}/repos/{repository}/compare/{commit}...{CANDIDATE_BRANCH}", _api_headers(token), None,
    )
    answer = _json(status, body, f"comparing okg commit {commit[:12]} with {CANDIDATE_BRANCH}")
    if not isinstance(answer, dict):
        raise Refused(f"comparing okg commit {commit[:12]} with {CANDIDATE_BRANCH}: unexpected answer")
    base = (answer.get("merge_base_commit") or {}).get("sha")
    if answer.get("status") not in ("ahead", "identical") or base != commit:
        raise Refused(
            f"the candidate names okg commit {commit[:12]}, which is not on {repository} {CANDIDATE_BRANCH} "
            f"(compare status {answer.get('status')!r}, merge base {str(base)[:12]})"
        )


def fetch_candidate(
    tag: str, sha: str, remote: str, repository: str, directory: Path, repo: Path, http: Http, token: str,
) -> dict[str, str]:
    """The candidate the development repository published for ``tag``, by the
    exact hand-over SHA the dispatch names.

    ``repo`` is a checkout of akmon's main. The hand-over commit is fetched
    into ``refs/candidate-meta/<tag>``; its four files are written to
    ``directory`` byte for byte, and its ``candidate.bundle`` as
    ``akmon-<tag>.bundle``, for ``verify-candidate`` and the later jobs.

    This proves the SHA, the ref holding it and every byte against the
    candidate commit. It cannot prove which okg run wrote the ref: anyone who
    can push to the development repository is trusted, and the reviewer
    checks the SHA against the one the prepare run printed."""
    if parse_tag(tag) is None:
        raise Refused(f"{tag!r} is not v0.MINOR.PATCH")
    if not GIT_ID.fullmatch(sha):
        raise Refused(f"the candidate {sha!r} is not a full commit id")
    if not re.fullmatch(r"[A-Za-z0-9-]+/[A-Za-z0-9._-]+", repository):
        raise Refused(f"{repository!r} is not owner/name")
    if directory.exists() and any(directory.iterdir()):
        raise Refused(f"{directory} is not empty")
    env = _git_auth_env(token)
    ref = f"{CANDIDATE_REF_PREFIX}{tag}"
    listed = [line.split("\t") for line in _git_bytes(repo, env, "ls-remote", remote, ref).decode().splitlines()]
    if listed != [[sha, ref]]:
        held = listed[0][0][:12] if listed else "absent"
        raise Refused(f"{repository} {ref} is {held}, not {sha[:12]}; dispatch with the SHA the prepare run printed")
    local = f"refs/candidate-meta/{tag}"
    _git_bytes(repo, env, "fetch", "--no-tags", remote, f"+{sha}:{local}")
    if _git_bytes(repo, env, "rev-parse", f"{local}^{{commit}}").decode().strip() != sha:
        raise Refused(f"the fetched hand-over is not {sha[:12]}")
    parents = _git_bytes(repo, env, "rev-list", "--parents", "-n", "1", sha).decode().split()[1:]
    if parents:
        raise Refused(f"the hand-over {sha[:12]} has {len(parents)} parents; it must have none")

    names = [*candidate_files(tag), CANDIDATE_BUNDLE]
    expected = {f"{CANDIDATE_DIR}/{name}": name for name in names}
    entries: dict[str, str] = {}
    for line in _git_bytes(repo, env, "ls-tree", "-r", "--full-tree", sha).decode().splitlines():
        meta, path = line.split("\t", 1)
        mode, kind, oid = meta.split()
        if (mode, kind) != ("100644", "blob"):
            raise Refused(f"the hand-over's {path} is {mode} {kind}, not a plain file")
        entries[path] = oid
    if sorted(entries) != sorted(expected):
        raise Refused(f"the hand-over holds {sorted(entries)}, not exactly {sorted(expected)}")
    directory.mkdir(parents=True, exist_ok=True)
    bundle = directory / f"akmon-{tag}.bundle"
    for path, name in expected.items():
        target = bundle if name == CANDIDATE_BUNDLE else directory / name
        target.write_bytes(_git_bytes(repo, env, "cat-file", "blob", entries[path]))

    message = _git_bytes(repo, env, "log", "-1", "--format=%B", sha).decode()
    recorded = CANDIDATE_BUNDLE_SHA.findall(message)
    held_sha = hashlib.sha256(bundle.read_bytes()).hexdigest()
    if recorded != [held_sha]:
        raise Refused(f"the hand-over records candidate bundle sha256 {recorded}, but its bundle is {held_sha}")
    _git_bytes(repo, env, "bundle", "verify", str(bundle))
    fetched = f"refs/candidate-fetch/{tag}"
    _git_bytes(repo, env, "fetch", "--no-tags", str(bundle), f"+refs/heads/release:{fetched}")
    commit = _git_bytes(repo, env, "rev-parse", f"{fetched}^{{commit}}").decode().strip()
    _git_bytes(repo, env, "update-ref", "-d", fetched)

    record = _read_record(directory)
    tree = _git_bytes(repo, env, "rev-parse", f"{commit}^{{tree}}").decode().strip()
    if record.get("commit") != commit or record.get("tree") != tree:
        raise Refused(
            f"the hand-over's {RELEASE_RECORD} names candidate {record.get('commit')!r} tree {record.get('tree')!r}, "
            f"but its bundle holds {commit} with tree {tree}"
        )
    okg_commit = str(record.get("okg_commit", ""))
    if not GIT_ID.fullmatch(okg_commit):
        raise Refused(f"{RELEASE_RECORD} okg_commit is {okg_commit!r}, not a full commit id")
    on_development_branch(repository, okg_commit, http, token)
    return {"meta": sha, "commit": commit, "okg-commit": okg_commit}


def _read_record(directory: Path) -> dict[str, Any]:
    try:
        record = json.loads((directory / RELEASE_RECORD).read_text(encoding="utf-8"))
    except (OSError, ValueError) as error:
        raise Refused(f"{RELEASE_RECORD}: {error}") from error
    if not isinstance(record, dict):
        raise Refused(f"{RELEASE_RECORD} is not an object")
    return record


def verify_candidate(tag: str, directory: Path, repo: Path, run: Runner) -> dict[str, str]:
    """The candidate the private repository prepared, checked against akmon.

    ``repo`` is a checkout of akmon's main. The candidate's commit is fetched
    from its bundle into ``refs/candidate/<tag>``; nothing is pushed here."""
    if parse_tag(tag) is None:
        raise Refused(f"{tag!r} is not v0.MINOR.PATCH")
    record = _read_record(directory)
    version = tag[1:]
    if record.get("tag") != tag or record.get("version") != version:
        raise Refused(f"{RELEASE_RECORD} is for {record.get('tag')} {record.get('version')}, not {tag}")
    for key in ("okg_commit", "commit", "tree", "parent"):
        if not isinstance(record.get(key), str) or not GIT_ID.fullmatch(record[key]):
            raise Refused(f"{RELEASE_RECORD} {key} is {record.get(key)!r}, not a full commit id")
    if record.get("notes_source") != notes_source(tag):
        raise Refused(
            f"{RELEASE_RECORD} notes_source is {record.get('notes_source')!r}, not "
            f"{notes_source(tag)!r}; the release notes must be the hand-written file, so prepare "
            "the candidate again with tooling that writes it"
        )
    for name in ("notes.md", "release-body.md", contract_asset_name(tag)):
        if not (directory / name).is_file():
            raise Refused(f"the candidate has no {name}")
    # The release notes are the hand-written file, published as the release
    # body; no generated list is attached beside it.
    if (directory / "notes.md").read_bytes() != (directory / "release-body.md").read_bytes():
        raise Refused(
            "the candidate's notes.md and release-body.md differ; the release notes must be the "
            "hand-written file, and nothing generated is attached"
        )
    try:
        contract = json.loads((directory / contract_asset_name(tag)).read_text(encoding="utf-8"))
    except ValueError as error:
        raise Refused(f"{contract_asset_name(tag)} is not JSON: {error}") from error
    if not isinstance(contract, dict) or contract.get("okg_version") != version:
        raise Refused(f"{contract_asset_name(tag)} does not name okg {version}")
    bundle = directory / f"akmon-{tag}.bundle"
    if not bundle.is_file():
        raise Refused(f"the candidate has no {bundle.name}")

    main = _git(run, repo, "rev-parse", "HEAD")
    if record["parent"] != main:
        raise Refused(
            f"the candidate's parent is {record['parent'][:12]}, but akmon's main is {main[:12]}; "
            "prepare the release again on the current main"
        )
    _git(run, repo, "bundle", "verify", str(bundle))
    _git(run, repo, "fetch", "--no-tags", str(bundle), f"+refs/heads/release:refs/candidate/{tag}")
    commit = _git(run, repo, "rev-parse", f"refs/candidate/{tag}^{{commit}}")
    if commit != record["commit"]:
        raise Refused(f"the bundle holds {commit[:12]}, {RELEASE_RECORD} says {record['commit'][:12]}")
    parents = _git(run, repo, "rev-list", "--parents", "-n", "1", commit).split()[1:]
    if parents != [main]:
        raise Refused(f"the candidate commit's parents are {parents}, not akmon's main {main[:12]} alone")
    tree = _git(run, repo, "rev-parse", f"{commit}^{{tree}}")
    if tree != record["tree"]:
        raise Refused(f"the candidate's tree is {tree[:12]}, {RELEASE_RECORD} says {record['tree'][:12]}")
    try:
        declared = tomllib.loads(_git(run, repo, "show", f"{commit}:pyproject.toml"))["project"]["version"]
    except (KeyError, tomllib.TOMLDecodeError) as error:
        raise Refused(f"pyproject.toml in the candidate: {error}") from error
    if declared != version:
        raise Refused(f"the candidate's pyproject.toml declares {declared}, not {version}")
    if _git(run, repo, "ls-remote", "--tags", "origin", f"refs/tags/{tag}"):
        raise Refused(f"{tag} already exists on {REPOSITORY}")
    return {"commit": commit, "tree": tree, "version": version}


def history(tag: str, http: Http, token: str) -> list[str]:
    """Earlier akmon releases from v0.2.0 on; each must carry its contract."""
    target = parse_tag(tag)
    if target is None:
        raise Refused(f"{tag!r} is not v0.MINOR.PATCH")
    names: list[str] = []
    for page in range(1, 11):
        status, body = http(
            "GET", f"{API_ROOT}/repos/{REPOSITORY}/releases?per_page=100&page={page}", _api_headers(token), None,
        )
        releases = _json(status, body, "listing akmon's releases")
        if not isinstance(releases, list):
            raise Refused("listing akmon's releases: unexpected answer")
        for release in releases:
            if not isinstance(release, dict) or release.get("draft"):
                continue
            name = str(release.get("tag_name", ""))
            parsed = parse_tag(name)
            if parsed is None or parsed < CONTRACT_HISTORY_STARTS or parsed >= target:
                continue
            if contract_asset_name(name) not in _asset_names(release):
                raise Refused(f"release {name} has no {contract_asset_name(name)}; a gap would ship")
            names.append(name)
        if len(releases) < 100:
            return sorted(names, key=lambda n: parse_tag(n) or (0, 0, 0))
    raise Refused("more than 1000 akmon releases; refusing to guess")


def check_wheel(wheel: Path, version: str, contracts: Mapping[str, str]) -> None:
    """The wheel is this version and carries exactly the staged contracts."""
    try:
        with zipfile.ZipFile(wheel) as archive:
            metadata = [n for n in archive.namelist() if n.endswith(".dist-info/METADATA")]
            if len(metadata) != 1:
                raise Refused(f"{wheel.name} has {len(metadata)} METADATA files")
            declared = next(
                (line.split(":", 1)[1].strip() for line in archive.read(metadata[0]).decode().splitlines()
                 if line.startswith("Version:")),
                None,
            )
            carried = {
                n.rsplit("/", 1)[1]: hashlib.sha256(archive.read(n)).hexdigest()
                for n in archive.namelist()
                if n.startswith(f"{PACKAGE}/_release_contracts/") and n.endswith(".json.gz")
            }
    except (OSError, zipfile.BadZipFile) as error:
        raise Refused(f"cannot read {wheel.name}: {error}") from error
    if declared != version:
        raise Refused(f"{wheel.name} is version {declared}, not {version}")
    if carried != dict(contracts):
        raise Refused(
            f"{wheel.name} bundles {format_contracts(carried) or 'no contracts'}, expected "
            f"{format_contracts(contracts) or 'none'}"
        )


#: The only files a wheel's .dist-info may hold (``licenses/`` aside).
DIST_INFO_FILES = frozenset({"METADATA", "WHEEL", "RECORD", "entry_points.txt", "top_level.txt"})


def _unsafe_member(name: str) -> str | None:
    """Why a wheel member name could land anywhere but where it says."""
    if "\\" in name:
        return "has a backslash"
    if name.startswith("/") or re.match(r"[A-Za-z]:", name):
        return "is absolute"
    parts = name.rstrip("/").split("/")
    if any(part in ("", ".", "..") for part in parts):
        return "has an empty, '.' or '..' segment"
    return None


def _declared_entry_points(tree: Path) -> dict[str, dict[str, str]]:
    """The entry points the release tree's pyproject.toml declares."""
    try:
        project = tomllib.loads((tree / "pyproject.toml").read_text(encoding="utf-8"))["project"]
    except (OSError, KeyError, tomllib.TOMLDecodeError) as error:
        raise Refused(f"pyproject.toml in the release tree: {error}") from error
    groups = {group: dict(points) for group, points in (project.get("entry-points") or {}).items()}
    if project.get("scripts"):
        groups["console_scripts"] = dict(project["scripts"])
    if project.get("gui-scripts"):
        groups["gui_scripts"] = dict(project["gui-scripts"])
    return groups


def _wheel_entry_points(data: bytes) -> dict[str, dict[str, str]]:
    import configparser

    parser = configparser.ConfigParser(delimiters=("=",), interpolation=None)
    parser.optionxform = str  # type: ignore[assignment,method-assign]
    parser.read_string(data.decode("utf-8"))
    return {group: {key: value.strip() for key, value in parser.items(group)} for group in parser.sections()}


def wheel_source_problems(
    wheel: Path, tree: Path, revision: str, tree_id: str, version: str, contracts: Mapping[str, str],
) -> list[str]:
    """Why the wheel holds anything but copies of the release tree.

    The wheel is built by the release's own setup.py, so this check reads it
    as data. Every member name must be a plain relative path, and appear
    once. Each package file must equal ``src/<its path>`` in ``tree`` (every
    package lives under src/; ``okg/_schemas/X`` equals ``schemas/X``), byte
    for byte. ``okg/_release_contracts/`` holds exactly the staged
    ``contracts`` (name -> sha256); ``okg/_build_info.json`` names this
    commit and tree, clean. The one ``okg-<version>.dist-info/`` holds only
    the standard metadata files, its entry points are the ones pyproject.toml
    declares, and its ``licenses/`` are copies of the tree's root files.
    Anything else is a problem."""
    problems: list[str] = []
    root = tree.resolve()
    dist_info = f"{PACKAGE}-{version}.dist-info/"
    contract_dir = f"{PACKAGE}/_release_contracts/"
    held_contracts: dict[str, str] = {}
    held_entry_points: dict[str, dict[str, str]] = {}
    declared_entry_points = _declared_entry_points(tree)
    try:
        with zipfile.ZipFile(wheel) as archive:
            members = archive.infolist()
            names = [info.filename for info in members]
            for name in sorted({n for n in names if names.count(n) > 1}):
                problems.append(f"{name} appears {names.count(name)} times")
            for name in names:
                unsafe = _unsafe_member(name)
                if unsafe:
                    problems.append(f"{name!r} {unsafe}")
            if problems:
                return problems
            tops = {n.split("/", 1)[0] for n in names}
            other_info = sorted(t for t in tops if t.endswith(".dist-info") and t != dist_info.rstrip("/"))
            if other_info or dist_info.rstrip("/") not in tops:
                problems.append(f"the wheel's .dist-info is {sorted(t for t in tops if t.endswith('.dist-info'))}, "
                                f"not exactly {dist_info.rstrip('/')}")
            for info in members:
                name = info.filename
                if info.is_dir():
                    continue
                data = archive.read(info)
                if name.startswith(dist_info):
                    rest = name[len(dist_info):]
                    if rest.startswith("licenses/") and rest.count("/") == 1:
                        source = tree / rest.split("/", 1)[1]
                    elif rest in DIST_INFO_FILES:
                        if rest == "entry_points.txt":
                            try:
                                held_entry_points = _wheel_entry_points(data)
                            except (ValueError, UnicodeDecodeError) as error:
                                problems.append(f"{name} is unreadable: {error}")
                        continue
                    else:
                        problems.append(f"{name} is not a standard .dist-info file")
                        continue
                elif name.split("/", 1)[0].endswith(".dist-info"):
                    continue  # another .dist-info: reported above
                elif name == f"{PACKAGE}/_build_info.json":
                    try:
                        record = json.loads(data)
                    except ValueError:
                        problems.append(f"{name} is not JSON")
                        continue
                    claimed = (record.get("source_revision"), record.get("source_tree"), record.get("source_dirty"))
                    if claimed != (revision, tree_id, False):
                        problems.append(f"{name} records {claimed}, not ({revision}, {tree_id}, False)")
                    continue
                elif name.startswith(contract_dir):
                    held_contracts[name[len(contract_dir):]] = hashlib.sha256(data).hexdigest()
                    continue
                elif name.startswith(f"{PACKAGE}/_schemas/") and name.count("/") == 2:
                    source = tree / "schemas" / name.rsplit("/", 1)[1]
                else:
                    source = tree / "src" / name
                where = source.relative_to(tree)
                if source.is_symlink() or not source.is_file() or not source.resolve().is_relative_to(root):
                    problems.append(f"{name} has no file {where} in the release tree")
                elif source.read_bytes() != data:
                    problems.append(f"{name} differs from {where}")
    except (OSError, zipfile.BadZipFile) as error:
        raise Refused(f"cannot read {wheel.name}: {error}") from error
    if held_entry_points != declared_entry_points:
        problems.append(
            f"{dist_info}entry_points.txt declares {held_entry_points or 'nothing'}, "
            f"pyproject.toml {declared_entry_points or 'nothing'}"
        )
    if held_contracts != dict(contracts):
        problems.append(
            f"{contract_dir} holds {format_contracts(held_contracts) or 'nothing'}, "
            f"expected {format_contracts(contracts) or 'nothing'}"
        )
    return problems


def same_contract(built: Path, candidate: Path) -> None:
    try:
        left = json.loads(built.read_text(encoding="utf-8"))
        right = json.loads(candidate.read_text(encoding="utf-8"))
    except (OSError, ValueError) as error:
        raise Refused(f"cannot read a contract: {error}") from error
    if left != right:
        raise Refused(
            "the contract built from akmon's tree differs from the one the candidate was prepared "
            "with; the published surface would not match its notes"
        )


# --- stage-contracts ---------------------------------------------------------


def stage_contracts(history: Sequence[str], directory: Path, http: Http, token: str) -> dict[str, str]:
    """Each earlier contract, gzipped with mtime 0 exactly as the cut writes
    it. Returns file name -> SHA-256 of the staged bytes."""
    if directory.exists() and any(directory.iterdir()):
        raise Refused(f"{directory} is not empty; a stale contract would ship")
    directory.mkdir(parents=True, exist_ok=True)
    staged: dict[str, str] = {}
    for name in history:
        asset = contract_asset_name(name)
        release = _require_release(name, http, token)
        matches = [a for a in release.get("assets") or [] if isinstance(a, dict) and a.get("name") == asset]
        if len(matches) != 1 or not matches[0].get("url"):
            raise Refused(f"release {name} has no single {asset} asset")
        headers = {**_api_headers(token), "Accept": "application/octet-stream"}
        status, body = http("GET", str(matches[0]["url"]), headers, None)
        if status != 200:
            raise Refused(f"downloading {asset}: HTTP {status}")
        try:
            contract = json.loads(body)
        except ValueError as error:
            raise Refused(f"{asset} is not JSON: {error}") from error
        if not isinstance(contract, dict):
            raise Refused(f"{asset} is not a contract object")
        data = gzip.compress(body, mtime=0)
        (directory / f"{asset}.gz").write_bytes(data)
        staged[f"{asset}.gz"] = hashlib.sha256(data).hexdigest()
    return staged


def format_contracts(staged: Mapping[str, str]) -> str:
    return ",".join(f"{name}={digest}" for name, digest in sorted(staged.items()))


def parse_contracts(text: str) -> dict[str, str]:
    parsed: dict[str, str] = {}
    for item in filter(None, text.split(",")):
        name, sep, digest = item.partition("=")
        if not sep or not re.fullmatch(r"okg-contract-v[0-9.]+\.json\.gz", name) or not re.fullmatch(r"[0-9a-f]{64}", digest):
            raise Refused(f"--contracts entry {item!r} is not <okg-contract-vX.Y.Z.json.gz>=<sha256>")
        parsed[name] = digest
    return parsed


# --- registry ----------------------------------------------------------------


def _split_reference(reference: str) -> tuple[str, str]:
    """``ghcr.io/owner/name:tag`` or ``...@sha256:...`` -> (``owner/name``, tag or digest)."""
    bare = reference.removeprefix("oci://")
    if not bare.startswith(f"{REGISTRY}/"):
        raise Refused(f"{reference!r} is not a {REGISTRY} reference")
    rest = bare[len(REGISTRY) + 1:]
    if "@" in rest:
        name, digest = rest.split("@", 1)
        if not DIGEST.fullmatch(digest):
            raise Refused(f"{reference!r} has a malformed digest")
        return name, digest
    if ":" not in rest.rsplit("/", 1)[-1]:
        raise Refused(f"{reference!r} names no tag")
    name, tag = rest.rsplit(":", 1)
    return name, tag


def _basic(token: str) -> str:
    return "Basic " + base64.b64encode(f"okg-release:{token}".encode()).decode()


def registry_token(name: str, http: Http, token: str | None, actions: str = "pull") -> str | None:
    """A registry token for ``name``, or None when the registry refuses one.
    Anonymous when ``token`` is None."""
    headers = {} if token is None else {"Authorization": _basic(token)}
    url = f"https://{REGISTRY}/token?" + urllib.parse.urlencode({"scope": f"repository:{name}:{actions}"})
    status, body = http("GET", url, headers, None)
    if status != 200:
        return None
    try:
        return str(json.loads(body)["token"])
    except (ValueError, KeyError, TypeError) as error:
        raise Refused(f"{REGISTRY} token answer for {name} is unreadable: {error}") from error


def get_manifest(reference: str, http: Http, token: str | None) -> tuple[int, bytes]:
    name, ref = _split_reference(reference)
    bearer = registry_token(name, http, token)
    if bearer is None:
        return -1, b""
    return http(
        "GET", f"https://{REGISTRY}/v2/{name}/manifests/{ref}",
        {"Authorization": f"Bearer {bearer}", "Accept": MANIFEST_ACCEPT}, None,
    )


def package_exists(name: str, http: Http, token: str) -> bool:
    """Whether the organization has a container package ``name`` (packages API)."""
    encoded = urllib.parse.quote(name.removeprefix(f"{ORGANIZATION}/"), safe="")
    status, body = http(
        "GET", f"{API_ROOT}/orgs/{ORGANIZATION}/packages/container/{encoded}", _api_headers(token), None,
    )
    if status == 404:
        return False
    if status == 200:
        return True
    raise Refused(f"cannot tell whether the package {name} exists: packages API HTTP {status}")


def reference_state(reference: str, http: Http, token: str) -> str:
    """``absent``, or the digest ``reference`` holds. Refuses when unreadable.

    Absent is a manifest answer of 404, or a package the registry and the
    packages API both say does not exist yet (the first chart).
    """
    name, _tag = _split_reference(reference)
    status, body = get_manifest(reference, http, token)
    if status == 404:
        return "absent"
    if status == -1:
        if not package_exists(name, http, token):
            return "absent"
        raise Refused(f"the registry refused a token for {reference}; cannot tell whether it exists")
    if status == 200:
        return "sha256:" + hashlib.sha256(body).hexdigest()
    raise Refused(f"cannot tell whether {reference} exists: HTTP {status}")


def require_absent(reference: str, http: Http, token: str, same_as: str = "") -> str:
    """Release references are immutable: refuse to write one that exists.
    ``same_as`` accepts a reference already holding exactly that digest: a
    rerun of the step that wrote it. Returns "absent" or "same"."""
    state = reference_state(reference, http, token)
    if state == "absent":
        return "absent"
    if same_as and state == same_as:
        return "same"
    raise Refused(f"{reference} already exists ({state}); release references are immutable")


def chart_archive_digest(archive: Path) -> str:
    """SHA-256 over a packaged chart's member paths and bytes, ignoring the
    archive's timestamps, so two packagings of one prepared chart compare
    equal and any changed byte does not."""
    import tarfile

    digest = hashlib.sha256()
    try:
        with tarfile.open(archive, "r:gz") as tar:
            members = sorted((m for m in tar.getmembers() if m.isfile()), key=lambda m: m.name)
            for member in members:
                handle = tar.extractfile(member)
                assert handle is not None
                digest.update(member.name.encode() + b"\0")
                digest.update(hashlib.sha256(handle.read()).digest())
    except (OSError, tarfile.TarError) as error:
        raise Refused(f"cannot read the chart archive {archive}: {error}") from error
    return "sha256:" + digest.hexdigest()


def require_same_chart(published: Path, prepared: Path) -> None:
    """A chart version that already exists may stand only when it is the
    chart this release prepared: a rerun after its push succeeded."""
    if chart_archive_digest(published) != chart_archive_digest(prepared):
        raise Refused(
            f"the published chart {published.name} differs from the one this release prepared; "
            "release references are immutable"
        )


def tag_manifest(source_ref: str, target_ref: str, http: Http, token: str) -> str:
    """Put the exact manifest bytes at ``source_ref`` (a digest) under the
    release tag ``target_ref``. Returns the digest, which is unchanged."""
    name, digest = _split_reference(source_ref)
    target_name, tag = _split_reference(target_ref)
    if name != target_name or not DIGEST.fullmatch(digest) or DIGEST.fullmatch(tag):
        raise Refused(f"tag needs <image>@<digest> and the same <image>:<tag>, got {source_ref} {target_ref}")
    if require_absent(target_ref, http, token, same_as=digest) == "same":
        return digest
    status, body = get_manifest(source_ref, http, token)
    if status != 200 or "sha256:" + hashlib.sha256(body).hexdigest() != digest:
        raise Refused(f"cannot read the proven manifest {source_ref}: HTTP {status}")
    media_type = json.loads(body).get("mediaType")
    if not media_type:
        raise Refused(f"{source_ref} names no mediaType")
    bearer = registry_token(name, http, token, "pull,push")
    if bearer is None:
        raise Refused(f"the registry refused a push token for {name}")
    status, answer = http(
        "PUT", f"https://{REGISTRY}/v2/{name}/manifests/{tag}",
        {"Authorization": f"Bearer {bearer}", "Content-Type": str(media_type)}, body,
    )
    if status not in (200, 201):
        raise Refused(f"tagging {target_ref}: HTTP {status}: {answer[:200].decode('utf-8', 'replace')}")
    return digest


def require_public(references: Sequence[str], http: Http) -> list[str]:
    """Every reference answers an anonymous pull. Returns the problems."""
    problems = []
    for reference in references:
        status, _body = get_manifest(reference, http, None)
        if status == -1:
            problems.append(f"{reference}: {REGISTRY} refused an anonymous token")
        elif status != 200:
            problems.append(f"{reference}: anonymous manifest read answered HTTP {status}")
    return problems


def release_references(tag: str) -> list[str]:
    version = tag.removeprefix("v")
    return [
        f"{RUNTIME_IMAGE}:{tag}",
        f"{POSTGRES_IMAGE}:{tag}",
        f"{CHART_REFERENCE.removeprefix('oci://')}:{version}",
    ]


# --- chart -------------------------------------------------------------------


def _replace_line(text: str, pattern: str, replacement: str, what: str) -> str:
    updated, count = re.subn(pattern, replacement, text, flags=re.MULTILINE)
    if count != 1:
        raise Refused(f"expected exactly one {what} line in the chart, found {count}")
    return updated


def _nested_line(values: str, top: str, middle: str, key: str) -> tuple[int, int]:
    """The span of the ``key:`` line under top-level ``top:`` -> ``middle:``."""
    offset = 0
    section = inner = False
    for line in values.splitlines(keepends=True):
        stripped = line.rstrip("\n")
        if re.match(r"^\S", stripped):
            section = stripped.startswith(f"{top}:")
            inner = False
        elif section and re.match(r"^  \S", stripped):
            inner = stripped.startswith(f"  {middle}:")
        elif section and inner and re.match(rf"^    {key}:", stripped):
            return offset, offset + len(stripped)
        offset += len(line)
    raise Refused(f"values.yaml has no {top}.{middle}.{key} line")


def _set_nested(values: str, top: str, middle: str, key: str, value: str) -> str:
    start, end = _nested_line(values, top, middle, key)
    return values[:start] + f"    {key}: {value}" + values[end:]


def tree_digest(root: Path) -> str:
    """SHA-256 over every file's relative path and bytes, in path order."""
    digest = hashlib.sha256()
    for path in sorted(p for p in root.rglob("*") if p.is_file()):
        digest.update(path.relative_to(root).as_posix().encode() + b"\0")
        digest.update(hashlib.sha256(path.read_bytes()).digest())
    return "sha256:" + digest.hexdigest()


def require_same_tree(directory: Path, expected: str) -> str:
    """A downloaded artifact holds exactly the files the uploading job
    digested: no file swapped, added or removed since."""
    if not DIGEST.fullmatch(expected):
        raise Refused(f"the expected digest {expected!r} is not sha256:<64 hex>; the uploading job named none")
    if not directory.is_dir():
        raise Refused(f"{directory} is not a directory")
    held = tree_digest(directory)
    if held != expected:
        raise Refused(
            f"{directory} holds {held}, but the job that uploaded it digested {expected}; "
            "the artifact changed after upload"
        )
    return held


def prepare_chart(tag: str, source_dir: Path, out_dir: Path) -> Path:
    """Copy the chart and set what a released chart says about itself."""
    import yaml

    version = tag.removeprefix("v")
    chart_file = source_dir / "Chart.yaml"
    try:
        chart = yaml.safe_load(chart_file.read_text(encoding="utf-8")) or {}
    except (OSError, yaml.YAMLError) as error:
        raise Refused(f"{chart_file}: {error}") from error
    annotations = chart.get("annotations") or {}
    if annotations.get(RELEASED_ANNOTATION) != "false":
        raise Refused(
            f"{chart_file} has no {RELEASED_ANNOTATION}: \"false\" annotation, so its image "
            "helper cannot default to the released image; merge the chart change that adds it first"
        )
    if chart.get("name") != SOURCE_CHART_NAME:
        raise Refused(f"{chart_file} names chart {chart.get('name')!r}, not {SOURCE_CHART_NAME!r}")

    target = out_dir / CHART_NAME
    if target.exists():
        raise Refused(f"{target} already exists")
    shutil.copytree(source_dir, target)

    text = (target / "Chart.yaml").read_text(encoding="utf-8")
    # The published chart is named akmon, so `helm push` writes it at
    # oci://ghcr.io/mitdbg/charts/akmon; the source tree keeps `okg`.
    text = _replace_line(text, r"^name:.*$", f"name: {CHART_NAME}", "name")
    text = _replace_line(text, r"^version:.*$", f"version: {version}", "version")
    text = _replace_line(text, r"^appVersion:.*$", f'appVersion: "{tag}"', "appVersion")
    text = _replace_line(
        text, rf'^(\s+){re.escape(RELEASED_ANNOTATION)}:\s*"false"\s*$', rf'\g<1>{RELEASED_ANNOTATION}: "true"',
        RELEASED_ANNOTATION,
    )
    (target / "Chart.yaml").write_text(text, encoding="utf-8")

    # The published chart, and only it, points at the release packages; the
    # in-repo values keep the dev packages.
    values = (target / "values.yaml").read_text(encoding="utf-8")
    values = _set_nested(values, "images", "okg", "repository", RUNTIME_IMAGE)
    values = _set_nested(values, "postgresql", "image", "repository", POSTGRES_IMAGE)
    values = _set_nested(values, "postgresql", "image", "tag", f'"{tag}"')
    (target / "values.yaml").write_text(values, encoding="utf-8")

    released = yaml.safe_load((target / "Chart.yaml").read_text(encoding="utf-8"))
    released_values = yaml.safe_load((target / "values.yaml").read_text(encoding="utf-8"))
    checks = {
        "name": released.get("name") == CHART_NAME,
        "version": released.get("version") == version,
        "appVersion": released.get("appVersion") == tag,
        RELEASED_ANNOTATION: (released.get("annotations") or {}).get(RELEASED_ANNOTATION) == "true",
        "postgresql.image.tag": released_values["postgresql"]["image"].get("tag") == tag,
        "images.okg.tag empty": not (released_values.get("images", {}).get("okg", {}).get("tag")),
        "images.okg.repository": released_values.get("images", {}).get("okg", {}).get("repository") == RUNTIME_IMAGE,
        "postgresql.image.repository": released_values["postgresql"]["image"].get("repository") == POSTGRES_IMAGE,
    }
    wrong = sorted(key for key, ok in checks.items() if not ok)
    if wrong:
        raise Refused(f"the prepared chart is wrong on {', '.join(wrong)}")
    return target


# --- verify-installed --------------------------------------------------------


def installed_problems(
    installed: Any,
    bundled: Mapping[str, bytes],
    code_revision: str | None,
    revision: str,
    tree: str,
    contracts: Mapping[str, str],
) -> list[str]:
    """Why an image's installed okg is not the release build of this commit.
    ``bundled`` maps each file under okg/_release_contracts to its bytes;
    ``contracts`` maps each staged file to its SHA-256."""
    if installed is None:
        return ["okg is not installed from a verified wheel"]
    problems = []
    if installed.source_revision != revision:
        problems.append(f"source_revision is {installed.source_revision}, expected {revision}")
    if installed.source_tree != tree:
        problems.append(f"source_tree is {installed.source_tree}, expected {tree}")
    if installed.source_dirty is not False:
        problems.append(f"source_dirty is {installed.source_dirty}, expected false")
    held = {name: hashlib.sha256(data).hexdigest() for name, data in bundled.items()}
    if held != dict(contracts):
        problems.append(f"bundled contracts are {format_contracts(held) or 'none'}, expected "
                        f"{format_contracts(contracts) or 'none'}")
    if code_revision != revision:
        problems.append(f"OKG_CODE_REVISION is {code_revision}, expected {revision}")
    return problems


def _installed_contract_bytes() -> dict[str, bytes]:
    from importlib import resources

    root = resources.files("okg") / "_release_contracts"
    if not root.is_dir():
        return {}
    return {entry.name: entry.read_bytes() for entry in root.iterdir() if entry.name.endswith(".json.gz")}


# --- notes -------------------------------------------------------------------


def packages_section(tag: str, runtime: str, postgres: str, chart: str) -> str:
    for label, digest in (("runtime", runtime), ("postgres", postgres), ("chart", chart)):
        if not DIGEST.fullmatch(digest):
            raise Refused(f"the {label} digest {digest!r} is not sha256:<64 hex>")
    version = tag.removeprefix("v")
    return "\n".join([
        NOTES_START,
        "## Install",
        "",
        f"Everything below was built from the commit this tag names. The tags `{tag}` "
        f"(images) and `{version}` (chart) are never moved or overwritten; pin a digest to "
        "be certain.",
        "",
        "| Artifact | Reference | Digest |",
        "| --- | --- | --- |",
        f"| Runtime image | `{RUNTIME_IMAGE}:{tag}` | `{runtime}` |",
        f"| PostgreSQL image | `{POSTGRES_IMAGE}:{tag}` | `{postgres}` |",
        f"| Helm chart | `{CHART_REFERENCE}` version `{version}` | `{chart}` |",
        "",
        "```",
        f"helm install okg {CHART_REFERENCE} --version {version} -f values.yaml",
        "```",
        "",
        f"The chart's `appVersion` is `{tag}`: with `images.okg.tag` and `digest` empty it runs "
        f"`{RUNTIME_IMAGE}:{tag}`, and its PostgreSQL image is `{POSTGRES_IMAGE}:{tag}`.",
        NOTES_END,
    ])


def merge_notes(body: str, section: str) -> str:
    """The release body with the Install section appended, or replaced in place."""
    start, end = body.find(NOTES_START), body.find(NOTES_END)
    if (start == -1) != (end == -1) or (start != -1 and end < start):
        raise Refused("the release body has a broken Install section marker; fix it by hand")
    if start == -1:
        merged = body.rstrip("\n") + "\n\n" + section + "\n"
    else:
        merged = body[:start] + section + body[end + len(NOTES_END):]
    if len(merged) > RELEASE_BODY_LIMIT:
        raise Refused(f"the release body would be {len(merged)} characters, over GitHub's limit")
    return merged


# --- command line ------------------------------------------------------------


def main(argv: Sequence[str] | None = None, *, http: Http = http_request, run: Runner = run_command) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    commands = parser.add_subparsers(dest="command", required=True)
    sub = commands.add_parser("fetch-candidate")
    sub.add_argument("--repository", required=True)
    sub.add_argument("--tag", required=True)
    sub.add_argument("--sha", required=True)
    sub.add_argument("--dir", type=Path, required=True)
    sub.add_argument("--repo", type=Path, default=Path.cwd())
    sub = commands.add_parser("verify-candidate")
    sub.add_argument("--tag", required=True)
    sub.add_argument("--dir", type=Path, required=True)
    sub.add_argument("--repo", type=Path, default=Path.cwd())
    sub = commands.add_parser("tree-digest")
    sub.add_argument("directory", type=Path)
    sub = commands.add_parser("same-tree")
    sub.add_argument("directory", type=Path)
    sub.add_argument("--expected", required=True)
    sub = commands.add_parser("history")
    sub.add_argument("--tag", required=True)
    sub = commands.add_parser("stage-contracts")
    sub.add_argument("--history", default="")
    sub.add_argument("--dir", type=Path, default=CONTRACT_BUNDLE_DIR)
    sub = commands.add_parser("check-wheel")
    sub.add_argument("wheel", type=Path)
    sub.add_argument("--version", required=True)
    sub.add_argument("--contracts", default="")
    sub = commands.add_parser("wheel-source")
    sub.add_argument("wheel", type=Path)
    sub.add_argument("--tree", type=Path, required=True)
    sub.add_argument("--revision", required=True)
    sub.add_argument("--tree-id", required=True)
    sub.add_argument("--version", required=True)
    sub.add_argument("--contracts", default="")
    sub = commands.add_parser("same-contract")
    sub.add_argument("built", type=Path)
    sub.add_argument("candidate", type=Path)
    sub = commands.add_parser("absent")
    sub.add_argument("reference")
    sub.add_argument("--same-as", default="")
    sub = commands.add_parser("reference-state")
    sub.add_argument("reference")
    sub = commands.add_parser("same-chart")
    sub.add_argument("published", type=Path)
    sub.add_argument("prepared", type=Path)
    sub = commands.add_parser("tag")
    sub.add_argument("source")
    sub.add_argument("target")
    sub = commands.add_parser("chart")
    sub.add_argument("--tag", required=True)
    sub.add_argument("--source", type=Path, required=True)
    sub.add_argument("--out", type=Path, required=True)
    sub = commands.add_parser("verify-installed")
    sub.add_argument("--revision", required=True)
    sub.add_argument("--tree", required=True)
    sub.add_argument("--contracts", default="")
    sub = commands.add_parser("notes")
    sub.add_argument("--tag", required=True)
    sub.add_argument("--body", type=Path, required=True)
    for flag in ("--runtime-digest", "--postgres-digest", "--chart-digest"):
        sub.add_argument(flag, required=True)
    sub = commands.add_parser("assets")
    sub.add_argument("--tag", required=True)
    sub.add_argument("--check", action="store_true")
    sub.add_argument("files", type=Path, nargs="+")
    sub = commands.add_parser("public")
    sub.add_argument("--tag", required=True)
    args = parser.parse_args(argv)

    try:
        if args.command == "fetch-candidate":
            write_outputs(fetch_candidate(
                args.tag, args.sha, f"https://github.com/{args.repository}.git", args.repository,
                args.dir.resolve(), args.repo.resolve(), http, _token(),
            ))
        elif args.command == "verify-candidate":
            write_outputs(verify_candidate(args.tag, args.dir.resolve(), args.repo.resolve(), run))
        elif args.command == "tree-digest":
            if not args.directory.is_dir():
                raise Refused(f"{args.directory} is not a directory")
            write_outputs({"digest": tree_digest(args.directory)})
        elif args.command == "same-tree":
            print(f"{args.directory} is unchanged: {require_same_tree(args.directory, args.expected)}")
        elif args.command == "history":
            write_outputs({"history": ",".join(history(args.tag, http, _token()))})
        elif args.command == "stage-contracts":
            names = [t for t in args.history.split(",") if t]
            write_outputs({"contracts": format_contracts(stage_contracts(names, args.dir, http, _token()))})
        elif args.command == "check-wheel":
            check_wheel(args.wheel, args.version, parse_contracts(args.contracts))
            print(f"{args.wheel.name} is {args.version} and carries exactly: {args.contracts or 'no contracts'}")
        elif args.command == "wheel-source":
            if not GIT_ID.fullmatch(args.revision) or not GIT_ID.fullmatch(args.tree_id):
                raise Refused("--revision and --tree-id must be full Git object ids")
            problems = wheel_source_problems(
                args.wheel, args.tree, args.revision, args.tree_id, args.version, parse_contracts(args.contracts),
            )
            if problems:
                raise Refused(
                    f"{args.wheel.name} holds what the release tree does not ({len(problems)}): "
                    + "; ".join(problems[:20])
                )
            print(f"{args.wheel.name}: every file is a copy of {args.tree} at {args.revision}")
        elif args.command == "same-contract":
            same_contract(args.built, args.candidate)
            print("the contract equals the candidate's")
        elif args.command == "absent":
            outcome = require_absent(args.reference, http, _token(), args.same_as)
            print(f"{args.reference}: {'already holds ' + args.same_as if outcome == 'same' else 'does not exist yet'}")
        elif args.command == "reference-state":
            print(reference_state(args.reference, http, _token()))
        elif args.command == "same-chart":
            require_same_chart(args.published, args.prepared)
            print(f"{args.published.name} is the chart this release prepared")
        elif args.command == "tag":
            digest = tag_manifest(args.source, args.target, http, _token())
            print(f"{args.target} -> {digest}")
            write_outputs({"digest": digest})
        elif args.command == "chart":
            prepared = prepare_chart(args.tag, args.source, args.out)
            print(f"prepared {prepared}")
            write_outputs({"content": tree_digest(prepared)})
        elif args.command == "verify-installed":
            from okg.substrate.distributions.installed_framework import installed_framework_distribution

            if not GIT_ID.fullmatch(args.revision) or not GIT_ID.fullmatch(args.tree):
                raise Refused("--revision and --tree must be full Git object ids")
            problems = installed_problems(
                installed_framework_distribution(), _installed_contract_bytes(),
                os.environ.get("OKG_CODE_REVISION"), args.revision, args.tree, parse_contracts(args.contracts),
            )
            if problems:
                raise Refused("the image is not the release build: " + "; ".join(problems))
            print(f"okg in this image is the clean wheel of {args.revision}; contracts: {args.contracts or 'none'}")
        elif args.command == "notes":
            section = packages_section(args.tag, args.runtime_digest, args.postgres_digest, args.chart_digest)
            body = args.body.read_text(encoding="utf-8")
            args.body.write_text(merge_notes(body, section), encoding="utf-8")
            print(section)
        elif args.command == "assets":
            stale = stale_assets(args.tag, args.files, http, _token())
            if args.check and stale:
                raise Refused(
                    f"the release {args.tag} does not hold "
                    + ", ".join(p.name for p in stale) + " byte for byte"
                )
            for path in stale:
                print(path)
        elif args.command == "public":
            problems = require_public(release_references(args.tag), http)
            if problems:
                raise Refused(
                    "not publicly pullable: " + "; ".join(problems) + ". The GitHub release "
                    f"{args.tag} stays a draft. Set each package's visibility to public in its "
                    "GitHub package settings (a first release's packages start private), then "
                    "re-run the failed jobs"
                )
            print("\n".join(f"{ref}: public" for ref in release_references(args.tag)))
    except Refused as error:
        print(f"REFUSED: {error}", file=sys.stderr)
        return 1
    except OSError as error:
        print(f"FAILED: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
