"""okg_self_micro fixture loader.

Materializes the static fixture corpus into a tmp dir for the
thesis test. Public surface:

    materialize(dest: Path) -> FixturePaths
        Unpacks repo.tar.gz, copies openspec/ + encoded Claude sessions/ +
        aliases.json under `dest`, returns FixturePaths with
        absolute paths.

The fixture pins specific commit SHAs / session ids that the
thesis test references via its CRITICAL_NODE_IDS hand-list
(below). Re-running materialize() is idempotent.
"""

from __future__ import annotations

import shutil
import tarfile
from dataclasses import dataclass
from pathlib import Path


_FIXTURE_DIR = Path(__file__).parent


@dataclass(frozen=True)
class FixturePaths:
    repo_root: Path
    openspec_root: Path
    sessions_root: Path
    aliases_path: Path


def materialize(dest: Path) -> FixturePaths:
    """Unpack the fixture corpus under `dest`. Idempotent."""
    dest = Path(dest)
    dest.mkdir(parents=True, exist_ok=True)

    # Unpack the git repo.
    repo_target = dest / "okg_self_micro_repo"
    if repo_target.exists():
        shutil.rmtree(repo_target)
    with tarfile.open(_FIXTURE_DIR / "repo.tar.gz", "r:gz") as tar:
        tar.extractall(dest)

    # Copy openspec dir.
    openspec_target = dest / "openspec"
    if openspec_target.exists():
        shutil.rmtree(openspec_target)
    shutil.copytree(_FIXTURE_DIR / "openspec", openspec_target)

    # Copy sessions under Claude's lossy encoding of the fixture cwd
    # `/repo/okg_self_micro_repo`.
    sessions_target = dest / "-repo-okg-self-micro-repo"
    if sessions_target.exists():
        shutil.rmtree(sessions_target)
    shutil.copytree(_FIXTURE_DIR / "sessions", sessions_target)

    # Copy aliases.json.
    aliases_target = dest / "aliases.json"
    shutil.copy2(_FIXTURE_DIR / "aliases.json", aliases_target)

    return FixturePaths(
        repo_root=repo_target,
        openspec_root=openspec_target,
        sessions_root=sessions_target,
        aliases_path=aliases_target,
    )


# Hand-curated id list used by the thesis test's
# critical-node-id parity check. Pinned by the fixture
# tarball + JSON files.
CRITICAL_NODE_IDS: tuple[str, ...] = (
    # Specific commit SHAs from the 5-commit fixture repo.
    # Computed when the repo was built; pinned bytes-for-bytes
    # in repo.tar.gz. Emit shape is
    # `commit:<repo>:<full-sha>` per git_graph schema.
    "commit:okg:0583708ffa8f3825194106a6fdc5347a6c265f91",  # scaffold okg_micro
    "commit:okg:db530199795d4ace436484b15ffc1146f811794e",  # docs: add usage (mentions add-greeting-feature)
    "commit:okg:67941957fba88d5967d15fe19d2d80c59fe0f3b9",  # refactor: rename core to greetings
    "commit:okg:4b6c6f13da8558f0aaa47029bac96f24471c240c",  # feat: optional formal mode
    "commit:okg:ead8d35f8429ca40362a6ea70f96f933c82747f3",  # docs: document formal mode (mentions add-formal-greeting)
    # Specific source files.
    "file:okg:src/okg_micro/__init__.py",
    "file:okg:src/okg_micro/greetings.py",
    "file:okg:docs/usage.md",
    # Specific OpenSpec changes — emit shape `change:<slug>` per
    # openspec module schema.
    "change:add-greeting-feature",
    "change:add-formal-greeting",
    # Specific agent sessions — emit shape `session:<sessionId>`
    # per agent_sessions module schema.
    "session:alpha-001",
    "session:beta-001",
)


__all__ = ["CRITICAL_NODE_IDS", "FixturePaths", "materialize"]
