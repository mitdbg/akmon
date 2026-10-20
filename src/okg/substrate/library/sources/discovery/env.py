"""`DiscoveryEnv` — the controlled IO surface source
discoverers see.

Per the proposal: sources' `discover()` classmethods SHALL
NOT use `os.listdir` / `pathlib.Path.glob` directly. They
receive a `DiscoveryEnv` whose methods are the only IO
surface. The MCP tool builds the env at the start of a
discovery walk and audits the IO calls.

Bounds enforced by the env:
  - `list_files(pattern, max_depth=2, max_files=1000)` —
    bounded directory walk.
  - `file_exists(path)` — single stat call.
  - `read_install_answer(key)` — reads
    `deployment.yaml.profile.init_answers.<key>`.
  - `env_var(name)` — reads `os.environ[name]`.
  - `platform_is(name)` — checks `sys.platform`.

Sources don't get raw filesystem access. The env tracks
which paths were inspected so the MCP response can list
them ("here's everywhere I looked"); auditable, less spooky
for the operator.
"""
from __future__ import annotations
from okg.substrate import fastyaml

import os
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional


# Default scan bounds. Each per-source `discover` SHALL respect
# them. The env enforces; sources don't need to police themselves.
DEFAULT_MAX_DEPTH = 2
DEFAULT_MAX_FILES = 1000
DEFAULT_SCAN_TIMEOUT_S = 5.0


@dataclass
class DiscoveryEnv:
    """Controlled filesystem + env view exposed to source
    discovery code.

    Construct via `DiscoveryEnv.for_deployment(deployment_dir)`
    or `DiscoveryEnv.from_machine(install_answers=..., home=...)`.

    Attributes:
      - `home_dir`: operator's home directory (Path).
      - `install_answers`: dict from
        `deployment.yaml.profile.init_answers`.
      - `env_vars`: snapshot of `os.environ` at construction time.
      - `platform`: `sys.platform` snapshot.
      - `inspected_paths`: cumulative list of paths the source
        asked about — useful for the MCP audit trail.
    """

    home_dir: Path
    install_answers: dict[str, Any]
    env_vars: dict[str, str]
    platform: str
    inspected_paths: list[str] = field(default_factory=list)
    _scan_deadline: Optional[float] = None

    @classmethod
    def from_machine(
        cls,
        *,
        install_answers: Optional[dict[str, Any]] = None,
        home: Optional[Path] = None,
    ) -> "DiscoveryEnv":
        """Snapshot the operator's machine at this moment."""
        return cls(
            home_dir=home or Path.home(),
            install_answers=dict(install_answers or {}),
            env_vars=dict(os.environ),
            platform=sys.platform,
        )

    @classmethod
    def for_deployment(
        cls,
        deployment_dir: Path,
        *,
        home: Optional[Path] = None,
    ) -> "DiscoveryEnv":
        """Construct an env scoped to a deployment, reading
        its `init_answers` from `deployment.yaml` if present.
        """
        import yaml
        answers: dict[str, Any] = {}
        dy = deployment_dir / "deployment.yaml"
        if dy.is_file():
            try:
                raw = fastyaml.safe_load(dy.read_text()) or {}
                answers = (raw.get("profile") or {}).get(
                    "init_answers"
                ) or {}
            except yaml.YAMLError:
                # Ignore malformed optional profile answers and use machine defaults.
                pass
        return cls.from_machine(
            install_answers=answers, home=home,
        )

    # ---------- IO methods ----------

    def file_exists(self, path: Path | str) -> bool:
        """Returns True iff the path is a file the operator
        can read. Records the inspection."""
        p = Path(path).expanduser()
        self.inspected_paths.append(str(p))
        return p.is_file()

    def dir_exists(self, path: Path | str) -> bool:
        """Returns True iff the path is a directory."""
        p = Path(path).expanduser()
        self.inspected_paths.append(str(p))
        return p.is_dir()

    def list_files(
        self,
        root: Path | str,
        *,
        suffixes: Optional[tuple[str, ...]] = None,
        max_depth: int = DEFAULT_MAX_DEPTH,
        max_files: int = DEFAULT_MAX_FILES,
    ) -> list[Path]:
        """Walk `root` up to `max_depth` levels, returning up
        to `max_files` paths (optionally filtered by suffix).

        Records the inspection. Stops at the limit; the
        caller learns about truncation by checking
        `len(result) == max_files`.
        """
        root_p = Path(root).expanduser()
        self.inspected_paths.append(str(root_p))
        if not root_p.is_dir():
            return []
        if self._scan_deadline is not None and time.monotonic() > self._scan_deadline:
            return []
        out: list[Path] = []
        for cur, depth in self._walk_with_depth(root_p, max_depth):
            try:
                for child in cur.iterdir():
                    if child.is_file():
                        if suffixes is None or child.suffix.lower() in suffixes:
                            out.append(child)
                            if len(out) >= max_files:
                                return out
            except (OSError, PermissionError):
                continue
        return out

    def _walk_with_depth(self, root: Path, max_depth: int):
        """Yield (dir, depth) for dirs up to max_depth."""
        from collections import deque
        q: deque[tuple[Path, int]] = deque([(root, 0)])
        while q:
            cur, depth = q.popleft()
            yield cur, depth
            if depth >= max_depth:
                continue
            try:
                for child in cur.iterdir():
                    if child.is_dir() and not child.name.startswith("."):
                        # Skip hidden dirs (`.cache`, `.git`) by
                        # default; sources that want them
                        # (`~/.obsidian/`) call `file_exists`
                        # directly.
                        q.append((child, depth + 1))
            except (OSError, PermissionError):
                continue

    def read_install_answer(
        self, key: str, default: Any = None,
    ) -> Any:
        """Pull a value from the deployment's installation
        answers dict (or default if absent / empty-string).
        """
        v = self.install_answers.get(key)
        if v in (None, ""):
            return default
        return v

    def env_var(self, name: str, default: str = "") -> str:
        """Read `os.environ[name]` from the snapshot."""
        return self.env_vars.get(name, default)

    def platform_is(self, name: str) -> bool:
        """`name` is one of `darwin | linux | windows`."""
        if name == "darwin":
            return self.platform == "darwin"
        if name == "linux":
            return self.platform.startswith("linux")
        if name == "windows":
            return self.platform.startswith("win")
        return False

    # ---------- scan budget ----------

    def start_scan(self, timeout_s: float = DEFAULT_SCAN_TIMEOUT_S) -> None:
        """Arm a per-source scan deadline. The env's
        `list_files` short-circuits once the deadline
        passes. Source discoverers MAY call this at the top
        of their `discover` method to enforce the 5-second
        bound recommended by the proposal."""
        self._scan_deadline = time.monotonic() + timeout_s

    def end_scan(self) -> None:
        self._scan_deadline = None
