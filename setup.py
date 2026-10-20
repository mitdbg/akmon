"""Stage the canonical authored-file schemas and the build identity into built
OKG packages."""

import hashlib
import json
import os
import re
import subprocess
import sys
from pathlib import Path
from shutil import rmtree

from setuptools import setup
from setuptools.command.build_py import build_py
from setuptools.command.sdist import sdist

ROOT = Path(__file__).resolve().parent
#: Written into an sdist so a wheel built from it still names its source.
SDIST_BUILD_INFO = "okg-build-info.json"
#: What a build with no .git to read is made from, for example the container
#: image (Dockerfile `ARG VCS_REF`, `OKG_BUILD_SOURCE_TREE`,
#: `OKG_BUILD_SOURCE_DIRTY`). Whoever builds says it, because the build cannot
#: see it; a field nobody supplies is recorded as unknown (null), never as
#: clean. The release workflow (.github/workflows/release.yml) computes all
#: three from its own checkout and passes them; container-images.yml passes
#: only VCS_REF, so its images record the tree and dirty state as unknown.
BUILD_REVISION_ENV = "OKG_BUILD_SOURCE_REVISION"
BUILD_TREE_ENV = "OKG_BUILD_SOURCE_TREE"
BUILD_DIRTY_ENV = "OKG_BUILD_SOURCE_DIRTY"
#: Values that mean "not supplied". `unknown` is the Dockerfile's default.
UNKNOWN_VALUES = frozenset({"", "unknown"})
_GIT_ID = re.compile(r"[0-9a-f]{40}|[0-9a-f]{64}")


def _git(*args):
    try:
        out = subprocess.run(
            ["git", "-C", str(ROOT), *args],
            capture_output=True, text=True, check=True, timeout=30,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    return out.stdout.strip()


def _in_own_repository():
    """Whether ROOT is itself a Git work tree's top level.

    An sdist unpacked inside some other repository must not record that
    repository's HEAD.
    """
    top = _git("rev-parse", "--show-toplevel")
    return top is not None and Path(top).resolve() == ROOT


def _built_files_untracked(build_lib):
    """Files the build is about to ship under okg/ that HEAD does not track.

    setuptools ships whatever matches package discovery and the package-data
    globs, tracked or not, and reuses a stale build/ directory, so a clean
    `git status` alone does not make a clean wheel.
    """
    tracked = _git("ls-files", "-z", "--", "src/okg")
    if tracked is None:
        return None
    tracked_set = {path for path in tracked.split("\0") if path}
    generated = {"okg/_build_info.json"}
    untracked = []
    package = Path(build_lib) / "okg"
    for path in sorted(package.rglob("*")):
        if not path.is_file() or path.suffix == ".pyc":
            continue
        relative = path.relative_to(build_lib).as_posix()
        # Not source files: the schemas are copied from tracked schemas/, and
        # the earlier releases' contracts are downloaded release assets that
        # the release cut and the release image build stage in
        # build/release-contracts. _build_info.json names each bundled
        # contract with its SHA-256, so they are recorded, not hidden.
        if relative in generated or relative.startswith(("okg/_schemas/", "okg/_release_contracts/")):
            continue
        if f"src/{relative}" not in tracked_set:
            untracked.append(relative)
    return untracked


def _claimed_git_id(env):
    raw = os.environ.get(env)
    if raw is None or raw.strip() in UNKNOWN_VALUES:
        return None
    value = raw.strip()
    if _GIT_ID.fullmatch(value) is None:
        raise ValueError(
            f"{env} must be a full lowercase Git object id or 'unknown', got {raw!r}"
        )
    return value


def _claimed_build():
    """The supplied build facts; a key is present only when it was supplied.

    Anything other than a full lowercase Git object id (revision, tree),
    `true`/`false` (dirty) or `unknown` stops the build: a short or mistyped
    value would otherwise become a recorded claim no reader can match, or
    silently become "unknown". A tree or dirty state with no commit names
    nothing, and stops the build too.
    """
    claims = {}
    revision = _claimed_git_id(BUILD_REVISION_ENV)
    if revision is not None:
        claims["source_revision"] = revision
    tree = _claimed_git_id(BUILD_TREE_ENV)
    if tree is not None:
        claims["source_tree"] = tree
    raw_dirty = os.environ.get(BUILD_DIRTY_ENV)
    if raw_dirty is not None and raw_dirty.strip() not in UNKNOWN_VALUES:
        value = raw_dirty.strip()
        if value not in ("true", "false"):
            raise ValueError(
                f"{BUILD_DIRTY_ENV} must be 'true', 'false' or 'unknown', "
                f"got {raw_dirty!r}"
            )
        claims["source_dirty"] = value == "true"
    if claims and "source_revision" not in claims:
        raise ValueError(
            f"{BUILD_TREE_ENV} and {BUILD_DIRTY_ENV} describe a commit; "
            f"set {BUILD_REVISION_ENV} beside them"
        )
    return claims


def _build_info(build_lib=None):
    """The commit this tree is, as the builder sees it: a claim, not a proof.

    Read by okg.substrate.distributions.installed_framework. `source_dirty`
    is true when the work tree has any change, including untracked files, or
    when the build is about to ship a file HEAD does not track. A build
    outside Git (an sdist) reuses the info the sdist carries. With neither,
    each field is the one OKG_BUILD_SOURCE_REVISION / _TREE / _DIRTY supplies,
    or null. Inside Git or an sdist, a supplied field must agree.
    """
    claims = _claimed_build()
    revision = _git("rev-parse", "HEAD") if _in_own_repository() else None
    if revision:
        status = _git("status", "--porcelain", "--untracked-files=all")
        untracked = _built_files_untracked(build_lib) if build_lib else []
        dirty = None if status is None or untracked is None else bool(status or untracked)
        info = {
            "source_revision": revision,
            "source_tree": _git("rev-parse", "HEAD^{tree}"),
            "source_dirty": dirty,
        }
        _refuse_conflicting_claims(claims, info, "this checkout")
        return info
    carried = ROOT / SDIST_BUILD_INFO
    if carried.is_file():
        info = json.loads(carried.read_text(encoding="utf-8"))
        _refuse_conflicting_claims(claims, info, f"the sdist's {SDIST_BUILD_INFO}")
        return info
    # No Git and no sdist: only what the builder supplied is known; the rest
    # stays null.
    return {
        "source_revision": claims.get("source_revision"),
        "source_tree": claims.get("source_tree"),
        "source_dirty": claims.get("source_dirty"),
    }


def _refuse_conflicting_claims(claims, observed, where):
    env = {
        "source_revision": BUILD_REVISION_ENV,
        "source_tree": BUILD_TREE_ENV,
        "source_dirty": BUILD_DIRTY_ENV,
    }
    for key, claimed in claims.items():
        if observed.get(key) != claimed:
            raise ValueError(
                f"{env[key]}={claimed} disagrees with {where} "
                f"({key}={observed.get(key)}); a wheel names one source"
            )


class BuildPyWithSchemas(build_py):
    """Keep schemas/ authored once; wheels read their own packaged copy."""

    def run(self):
        super().run()
        source = ROOT / "schemas"
        schemas = sorted(source.glob("*.json"))
        if not schemas:
            raise FileNotFoundError(f"canonical OKG schemas missing: {source}")
        destination = Path(self.build_lib) / "okg" / "_schemas"
        # Reused build directories must not retain deleted canonical schemas.
        if destination.exists():
            rmtree(destination)
        destination.mkdir(parents=True)
        for schema in schemas:
            self.copy_file(str(schema), str(destination / schema.name))
        # Earlier releases' contracts, which the release cut downloads into
        # build/release-contracts before it builds (decision 312), so
        # `okg deployment compat` can list changes since `release.tested_with`
        # offline. A build outside the cut finds none and bundles none.
        contracts = sorted(
            (Path(__file__).resolve().parent / "build" / "release-contracts").glob(
                "okg-contract-v*.json.gz"
            )
        )
        bundled = Path(self.build_lib) / "okg" / "_release_contracts"
        if bundled.exists():
            rmtree(bundled)
        if contracts:
            bundled.mkdir(parents=True)
            for contract in contracts:
                self.copy_file(str(contract), str(bundled / contract.name))
            # A stale build/release-contracts would otherwise ship silently in
            # an ordinary wheel: say so, and record exactly what was bundled.
            print(
                "bundling earlier release contracts from build/release-contracts: "
                + ", ".join(contract.name for contract in contracts),
                file=sys.stderr,
            )
        info = _build_info(self.build_lib)
        if contracts:
            info["release_contracts"] = {
                contract.name: hashlib.sha256(contract.read_bytes()).hexdigest()
                for contract in contracts
            }
        build_info = Path(self.build_lib) / "okg" / "_build_info.json"
        build_info.write_text(
            json.dumps(info, indent=1, sort_keys=True) + "\n",
            encoding="utf-8",
        )


class SdistWithBuildInfo(sdist):
    """Carry the source commit into the sdist, which has no .git."""

    def make_release_tree(self, base_dir, files):
        super().make_release_tree(base_dir, files)
        (Path(base_dir) / SDIST_BUILD_INFO).write_text(
            json.dumps(_build_info(), indent=1, sort_keys=True) + "\n",
            encoding="utf-8",
        )


setup(cmdclass={"build_py": BuildPyWithSchemas, "sdist": SdistWithBuildInfo})
