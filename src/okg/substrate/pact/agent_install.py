"""`okg pact install` — place the agent-drivable PACT/OKG skills bundle + a
concise instruction stub for a coding-agent target (Claude Code, Codex, or
Gemini CLI).

Thin + idempotent (per `pact-agent-install`, install = thin script +
agent-drivable skill): it copies the in-package skills bundle
(``src/okg/substrate/pact/skills/``) and writes a single marker-delimited
PACT/OKG section into the target's instruction file. It never touches content
outside its marker, and a re-run refreshes the block + bundle with no
duplication. No tailored prose is generated — the deep, conversational setup is
driven by an agent running the installed skills.

Per `ship-harness-integration-pack`: the same install optionally registers
the deployment's OKG MCP server via a thin per-harness manifest — a
`.mcp.json` mcpServers entry (Claude Code), a `config.toml` snippet under
`.agents/` (Codex; pasted into `~/.codex/config.toml`), or a
`gemini-extension.json` (Gemini CLI). Manifests point at the server and the
single skills bundle; they never embed skill bodies. OpenClaw and Hermes
consume the same SKILL.md format and standard MCP client config — they get a
documented recipe (the instruction section), not a maintained package.
"""
from __future__ import annotations

import hashlib
import json
import re
import shutil
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

from okg.substrate.pact import envelope
from okg.substrate.library.agent_runtimes import atomic_write_text

GH_PR_ALIAS = "prd"
GH_PR_ALIAS_EXPANSION = "pr create --base dev"

_SKILLS_DIR = Path(__file__).parent / "skills"
_MARKER_BEGIN = "<!-- BEGIN okg-pact-install -->"
_MARKER_END = "<!-- END okg-pact-install -->"
_SYNC_MANIFEST = ".okg-skill-sync.json"

AGENT_TARGETS = ("claude-code", "codex", "gemini")

#: The path whose presence marks the okg repository itself. The full
#: instruction block points agents at this tree ("installed copies must match
#: them"), at ``docs/runbooks/agents/`` and at ``scripts/ci/``; all of those
#: exist only in a checkout of okg, so any other destination is a foreign
#: repository that gets the repository-neutral block and the ``pact`` skill.
OKG_REPOSITORY_MARKER = Path("src") / "okg" / "substrate" / "pact" / "skills"
REPOSITORY_KINDS = ("okg", "generic")
#: The one skill a foreign repository needs: it covers the PACT verbs only.
GENERIC_SKILLS = ("pact",)
#: Local files PACT writes in a foreign repository that must not be committed:
#: the usage log under ``.okg/`` and the generated review views.
GENERIC_GITIGNORE_LINES = (
    ".okg/",
    "pact/changes/*/pact.md",
    "pact/changes/*/pact.yaml.lock",
)
_GITIGNORE_HEADER = "# okg pact: local usage log and generated review views"

_CREDENTIAL_ENV_NAME = re.compile(
    r"(?:DSN|PASSWORD|PASSWD|TOKEN|SECRET|API_KEY|PRIVATE_KEY)$",
    re.IGNORECASE,
)
_ENV_REFERENCE = re.compile(r"\$\{[A-Z][A-Z0-9_]*\}")
_ABSOLUTE_PATH = re.compile(r"(?:^|[=:@\s'\"])/(?!\$\{)[^\s'\"]+")
_WINDOWS_ABSOLUTE_PATH = re.compile(r"(?:^|[=:@\s'\"])[A-Za-z]:[\\/]")
_IPV4 = re.compile(r"(?<![\d.])(?:\d{1,3}\.){3}\d{1,3}(?![\d.])")
_CREDENTIAL_ARGUMENT = re.compile(
    r"(?:postgres(?:ql)?://|(?:password|passwd|token|secret|api[_-]?key|dsn)\s*[=:]|"
    r"authorization:\s*bearer)",
    re.IGNORECASE,
)
_PRIVATE_KEY_ARGUMENT = re.compile(
    r"(?:^|/)(?:\.ssh/|id_(?:rsa|dsa|ecdsa|ed25519)(?:$|[./])|"
    r"[^/]+\.(?:key|pem|p12|pfx)$)",
    re.IGNORECASE,
)
_HOST_FLAGS = {"--host", "--hostname", "--server", "--url", "-h"}
_CREDENTIAL_FLAGS = {
    "--api-key",
    "--api_key",
    "--authorization",
    "--dsn",
    "--passwd",
    "--password",
    "--secret",
    "--token",
}
_FIXED_HOST = re.compile(
    r"^(?:localhost|(?:[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?\.)+"
    r"[A-Za-z][A-Za-z0-9-]{1,62})$",
    re.IGNORECASE,
)
_NON_HOST_SUFFIXES = {
    "css",
    "html",
    "ini",
    "js",
    "json",
    "md",
    "py",
    "sh",
    "toml",
    "ts",
    "txt",
    "whl",
    "xml",
    "yaml",
    "yml",
}


@dataclass(frozen=True)
class McpServerSpec:
    """The OKG MCP server a harness manifest should register."""

    name: str
    command: str
    args: list[str] = field(default_factory=list)
    env: dict[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Refuse authority that would be copied into a tracked manifest."""
        literal_credentials = [
            key
            for key, value in self.env.items()
            if _CREDENTIAL_ENV_NAME.search(key) and not _ENV_REFERENCE.fullmatch(value)
        ]
        if literal_credentials:
            names = ", ".join(sorted(literal_credentials))
            raise ValueError(
                f"MCP credential settings must use environment references: {names}"
            )

        command_values = [self.command, *self.args]
        if any(_CREDENTIAL_ARGUMENT.search(value) for value in command_values):
            raise ValueError(
                "MCP manifests cannot contain credential values in commands; "
                "use an environment reference"
            )
        for index, value in enumerate(self.args[:-1]):
            if value.lower() in _CREDENTIAL_FLAGS and not _ENV_REFERENCE.fullmatch(
                self.args[index + 1]
            ):
                raise ValueError(
                    "MCP credential settings must use environment references"
                )

        values = [self.command, *self.args, *self.env.values()]
        if any(
            _ABSOLUTE_PATH.search(value) or _WINDOWS_ABSOLUTE_PATH.search(value)
            for value in values
        ):
            raise ValueError(
                "MCP manifests cannot contain absolute paths; "
                "supply machine settings through runtime configuration"
            )
        command_name = Path(self.command).name.lower()
        if command_name in {"ssh", "scp", "sftp"} or "-i" in self.args:
            raise ValueError(
                "MCP manifests cannot contain SSH or private-key commands; "
                "supply remote authority through the authenticated runtime"
            )
        if any(_PRIVATE_KEY_ARGUMENT.search(value) for value in values):
            raise ValueError(
                "MCP manifests cannot contain private-key paths; "
                "supply remote authority through the authenticated runtime"
            )
        for index, value in enumerate(self.args[:-1]):
            if value.lower() in _HOST_FLAGS and not _ENV_REFERENCE.fullmatch(
                self.args[index + 1]
            ):
                raise ValueError(
                    "MCP host settings must use environment references"
                )
        if any(_IPV4.search(value) for value in command_values):
            raise ValueError(
                "MCP manifests cannot contain a fixed host address; "
                "supply machine settings through runtime configuration"
            )
        for value in self.args:
            suffix = value.rsplit(".", 1)[-1].lower() if "." in value else ""
            if _FIXED_HOST.fullmatch(value) and suffix not in _NON_HOST_SUFFIXES:
                raise ValueError(
                    "MCP manifests cannot contain a fixed hostname; "
                    "use an environment reference"
                )
        if any(
            re.fullmatch(r"[^@\s]+@[^@\s]+", value)
            and not _ENV_REFERENCE.fullmatch(value)
            for value in self.args
        ):
            raise ValueError(
                "MCP host settings must use environment references"
            )


def bundled_skills(skills_dir: Path | None = None) -> list[Path]:
    """The skill directories shipped in-package (each has a SKILL.md)."""
    skills_dir = _SKILLS_DIR if skills_dir is None else skills_dir
    if not skills_dir.is_dir():
        return []
    return sorted(
        p for p in skills_dir.iterdir()
        if p.is_dir() and (p / "SKILL.md").is_file()
    )


def is_okg_repository(dest: Path) -> bool:
    """True when ``dest`` is a checkout of the okg repository itself."""
    return (dest / OKG_REPOSITORY_MARKER).is_dir()


def generic_instruction_section() -> str:
    """Return the repository-neutral block installed into a foreign repo."""
    return "\n".join(
        [
            _MARKER_BEGIN,
            "## PACT change records",
            "",
            "PACT keeps a short, reviewable record of each nontrivial change in",
            "`pact/changes/<id>/pact.yaml`: why it is needed, which code it touches,",
            "what must hold, and the check that proves it. The operator approves it.",
            "",
            "- Create one: `okg pact new <id> --title \"<title>\"`, then edit",
            "  `pact/changes/<id>/pact.yaml` directly.",
            "- Show the review view: `okg pact view <id> --format markdown`.",
            "- Check it: `okg pact check <id>`. Work on a branch: the check",
            "  judges the branch's diff against the default branch.",
            "- When the operator says \"approve\": commit the manifest, then run",
            "  `okg pact approve <id>`. It records the approval as an empty git commit.",
            "",
            "Run `okg` directly. The `pact` skill has the details. Commit `pact.yaml`;",
            "`.okg/`, `pact.md` and `pact.yaml.lock` are local files kept out of git",
            "by `.gitignore`.",
            _MARKER_END,
        ]
    )


def ensure_gitignore(dest: Path) -> list[str]:
    """Add the PACT local-file patterns to ``dest/.gitignore`` once.

    Returns the lines it added. A pattern already present in an equivalent
    spelling (``.okg`` for ``.okg/``, a leading ``/``) is not added again, a
    pattern the file negates (``!pact/changes/*/pact.md``) is left to the
    owner's choice, the file's line ending (LF or CRLF) is kept, and other
    content is preserved byte for byte.
    """
    path = dest / ".gitignore"
    # Bytes in and out keep "\r\n" intact (text mode would translate it).
    text = path.read_bytes().decode("utf-8") if path.exists() else ""
    newline = "\r\n" if "\r\n" in text else "\n"

    def _norm(pattern: str) -> str:
        return pattern.strip().strip("/")

    present: set[str] = set()
    negated: set[str] = set()
    for raw in text.splitlines():
        line = raw.strip()
        if line.startswith("!"):
            negated.add(_norm(line[1:]))
        elif line and not line.startswith("#"):
            present.add(_norm(line))
    missing = [
        line for line in GENERIC_GITIGNORE_LINES
        if _norm(line) not in present and _norm(line) not in negated
    ]
    if not missing:
        return []
    has_header = any(raw.strip() == _GITIGNORE_HEADER for raw in text.splitlines())
    block = ([] if has_header else [_GITIGNORE_HEADER]) + missing
    sep = "" if not text or text.endswith("\n") else newline
    path.write_bytes((text + sep + newline.join(block) + newline).encode("utf-8"))
    return missing


#: Lines of the okg-repository block that name a file of this development
#: checkout, keyed by that file and tagged with where they go ("opening-a" and
#: "opening-b" around a fixed sentence of the first paragraph, "bullets" in the
#: list). A checkout of okg without the file (the public copy) gets the block
#: without the line, so the installed text never points at something that is
#: not there. The keys are paths in the INSTALL DESTINATION, not reads of this
#: repository.
_OKG_BLOCK_FILE_LINES: tuple[tuple[str, str, tuple[str, ...]], ...] = (
    ("history/openspec", "opening-a", (
        "The `history/openspec/` tree is read-only history.",
    )),
    ("pact/AGENTS.md", "opening-a", (
        "The full PACT contract and command reference live in",
        "`pact/AGENTS.md`; repo defaults live in `pact/project.yaml`.",
    )),
    ("pact/project.md", "opening-b", (
        "`pact/project.md` is human context.",
    )),
    ("docs/runbooks/agents/reporting.md", "bullets", (
        "- Write human updates and pull requests by",
        "  `docs/runbooks/agents/reporting.md`: lead with the result, translate",
        "  internal terms, and include only the proof or recovery detail that",
        "  changes the reader's decision.",
    )),
    ("docs/runbooks/agents/coordination.md", "bullets", (
        "- Use `docs/runbooks/agents/coordination.md` only when work is actually",
        "  delegated or split across worktrees.",
    )),
    ("docs/runbooks/agents/okg-usage.md", "bullets", (
        "- For graph context, follow `docs/runbooks/agents/okg-usage.md`. Treat",
        "  the graph as context and live Git, files, tests, and publish checks as",
        "  current truth.",
    )),
    ("docs/runbooks/agents/ui.md", "bullets", (
        "- For UI work, follow `docs/runbooks/agents/ui.md` and inspect the",
        "  rendered result.",
    )),
    ("scripts/ci/agent_preflight.py", "bullets", (
        "- Before push, run `python scripts/ci/agent_preflight.py --pact-id <id>",
        "  --pr-body <body-file> --run`. Pull requests target `dev`.",
    )),
)


#: The development-checkout files the okg block can name.
OKG_BLOCK_FILES = tuple(key for key, _section, _lines in _OKG_BLOCK_FILE_LINES)


def _okg_block_present(key: str, dest: Path | None) -> bool:
    """False only when ``dest`` is a checkout of okg that lacks ``key``.

    A destination that is not an okg checkout at all keeps every line: the
    caller asked for the okg block explicitly.
    """
    return not (
        dest is not None and is_okg_repository(dest) and not (dest / key).exists()
    )


def _okg_block_section(section: str, dest: Path | None) -> list[str]:
    """The lines tagged ``section`` whose files ``dest`` has, in table order."""
    return [
        line
        for key, tag, lines in _OKG_BLOCK_FILE_LINES
        if tag == section and _okg_block_present(key, dest)
        for line in lines
    ]


def instruction_section(
    skills_dir: Path | None = None, *, dest: Path | None = None
) -> str:
    """Return the short router installed into an agent instruction file.

    With ``dest`` a checkout of okg, a line that names a file of the
    development checkout is kept only when ``dest`` has that file.
    """
    names = ", ".join(f"`{p.name}`" for p in bundled_skills(skills_dir))
    if all(
        _okg_block_present(key, dest)
        for key, tag, _lines in _OKG_BLOCK_FILE_LINES
        if tag.startswith("opening")
    ):
        # The development checkout: the paragraph exactly as it always read.
        paragraph = [
            "PACT is the only active change workflow. The `history/openspec/` tree",
            "is read-only history. The full PACT contract and command reference live in",
            "`pact/AGENTS.md`; repo defaults live in `pact/project.yaml`. Edit",
            "`pact.yaml` directly.",
            "`pact/project.md` is human context. Use `uv run okg`. Generated `pact.md`",
            "and `pact.yaml.lock` are worktree-local and must not be committed.",
        ]
    else:
        # One sentence (or source line) per line, keeping only the sentences
        # whose files exist, never split inside a code span.
        paragraph = [
            "PACT is the only active change workflow.",
            *_okg_block_section("opening-a", dest),
            "Edit `pact.yaml` directly.",
            *_okg_block_section("opening-b", dest),
            "Use `uv run okg`. Generated `pact.md` and `pact.yaml.lock` are",
            "worktree-local and must not be committed.",
        ]
    return "\n".join(
        [
            _MARKER_BEGIN,
            "## PACT and agent workflow",
            "",
            *paragraph,
            "",
            "- Start or resume nontrivial work with the `agent-workflow` skill and",
            "  `uv run okg pact view <id> --format=bootstrap --json`.",
            "- Use `dev-process` when the operator asks for the standing issue-to-PR",
            "  workflow.",
            *_okg_block_section("bullets", dest),
            "- Run an adversarial review before saying a pull request is ready.",
            "",
            f"Installed skills: {names}. Canonical sources live under",
            "`src/okg/substrate/pact/skills/`; installed copies must match them.",
            _MARKER_END,
        ]
    )


def _upsert_section(path: Path, section: str) -> str:
    """Write/refresh the marker-delimited section in ``path``.

    Returns ``'created'`` | ``'refreshed'`` | ``'appended'``. Content outside
    the marker block is preserved verbatim; a second call replaces the block
    rather than appending a duplicate.
    """
    if not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(section + "\n", encoding="utf-8")
        return "created"
    text = path.read_text(encoding="utf-8")
    if _MARKER_BEGIN in text and _MARKER_END in text:
        before = text[: text.index(_MARKER_BEGIN)]
        after = text[text.index(_MARKER_END) + len(_MARKER_END):]
        path.write_text(before + section + after, encoding="utf-8")
        return "refreshed"
    sep = "" if text.endswith("\n") else "\n"
    path.write_text(text + sep + "\n" + section + "\n", encoding="utf-8")
    return "appended"


def is_installed(dest: Path) -> bool:
    """True if either agent target's instruction file already carries the
    install marker (used by the first-run nudge)."""
    for name in ("CLAUDE.md", "AGENTS.md"):
        p = dest / name
        try:
            if p.is_file() and _MARKER_BEGIN in p.read_text(encoding="utf-8"):
                return True
        except OSError:
            continue
    return any(
        bool(agent_skill_freshness(dest, targets=(target,)).get("ok"))
        for target in ("claude-code", "codex")
    )


def nudge_message(dest: Path) -> str | None:
    """The one-line install tip for a repo whose bundle marker is absent, or
    None once installed. Pure read — it never writes."""
    if is_installed(dest):
        return None
    return (
        "tip: `okg pact install --agent claude-code` (or --agent codex) "
        "installs PACT/OKG agent instructions + skills. "
        "Set OKG_PACT_QUIET_NUDGE=1 to suppress."
    )


@dataclass
class InstallResult:
    agent: str
    dest: str
    instruction_file: str
    instruction_action: str
    skills_written: list[str] = field(default_factory=list)
    skills_added: list[str] = field(default_factory=list)
    skills_updated: list[str] = field(default_factory=list)
    skills_unchanged: list[str] = field(default_factory=list)
    skills_pruned: list[str] = field(default_factory=list)
    skills_orphaned: list[str] = field(default_factory=list)
    # Bundled skills not installed because a folder of the same name that
    # okg's sync manifest does not record is already there (never replaced).
    skills_clashed: list[str] = field(default_factory=list)
    skills_root: Optional[str] = None
    sync_manifest: Optional[str] = None
    mcp_manifest: Optional[str] = None
    repository: str = "okg"
    gitignore_added: list[str] = field(default_factory=list)

    @property
    def skill_count(self) -> int:
        return len(self.skills_written)

    def to_json(self) -> dict:
        text = (
            f"install {self.agent}: {len(self.skills_written)} skill(s) "
            f"written to {self.dest}"
        )
        if self.skills_clashed:
            text += (
                "; not installed, because a folder okg did not install already "
                "has the name: " + ", ".join(self.skills_clashed)
                + f" (in {self.skills_root}; move it aside and re-run to "
                "install okg's copy)"
            )
        return envelope.recorded(
            text=text,
            agent=self.agent,
            dest=self.dest,
            instruction_file=self.instruction_file,
            instruction_action=self.instruction_action,
            skills_written=self.skills_written,
            skills_added=self.skills_added,
            skills_updated=self.skills_updated,
            skills_unchanged=self.skills_unchanged,
            skills_pruned=self.skills_pruned,
            skills_orphaned=self.skills_orphaned,
            skills_clashed=self.skills_clashed,
            sync_manifest=self.sync_manifest,
            diff_summary={
                "added": len(self.skills_added),
                "updated": len(self.skills_updated),
                "unchanged": len(self.skills_unchanged),
                "pruned": len(self.skills_pruned),
                "orphaned": len(self.skills_orphaned),
            },
            skill_count=len(self.skills_written),
            mcp_manifest=self.mcp_manifest,
            repository=self.repository,
            gitignore_added=self.gitignore_added,
        )


def install_agent_setup(
    agent: str,
    dest: Path,
    *,
    mcp: Optional[McpServerSpec] = None,
    skills_dir: Path | None = None,
    repository: str = "okg",
) -> InstallResult:
    """Place the bundle + instruction stub for ``agent`` into ``dest``.

    With ``mcp``, also write the per-harness manifest registering the
    OKG MCP server (per ship-harness-integration-pack).

    ``repository="generic"`` is for a repository that is not okg (the CLI
    decides with :func:`is_okg_repository`): it writes the neutral block,
    installs only the ``pact`` skill and gitignores PACT's local files.
    """
    if agent not in AGENT_TARGETS:
        raise ValueError(
            f"unknown --agent {agent!r}; choose from {', '.join(AGENT_TARGETS)}"
        )
    if repository not in REPOSITORY_KINDS:
        raise ValueError(
            f"unknown repository kind {repository!r}; "
            f"choose from {', '.join(REPOSITORY_KINDS)}"
        )
    dest = dest.resolve()
    skills_dir = _SKILLS_DIR if skills_dir is None else skills_dir
    generic = repository == "generic"
    section = (
        generic_instruction_section()
        if generic
        else instruction_section(skills_dir, dest=dest)
    )

    # Every target gets the skill FILES — Claude Code reads
    # .claude/skills/; Codex and Gemini read the cross-tool
    # .agents/skills/ (the agentskills.io convention) — plus a
    # marker-delimited instruction section in the target's
    # instruction file.
    if agent == "claude-code":
        skills_root = dest / ".claude" / "skills"
        instruction = dest / "CLAUDE.md"
    elif agent == "gemini":
        skills_root = dest / ".agents" / "skills"
        instruction = dest / "GEMINI.md"
    else:  # codex
        skills_root = dest / ".agents" / "skills"
        instruction = dest / "AGENTS.md"
    sync = sync_agent_skills(
        dest,
        skills_root=skills_root,
        skills_dir=skills_dir,
        only=GENERIC_SKILLS if generic else None,
    )

    action = _upsert_section(instruction, section)
    gitignore_added = ensure_gitignore(dest) if generic else []
    manifest_path: Optional[str] = None
    if mcp is not None:
        manifest = _write_mcp_manifest(agent, dest, mcp)
        manifest_path = str(manifest.relative_to(dest))
    return InstallResult(
        agent=agent,
        dest=str(dest),
        instruction_file=instruction.name,
        instruction_action=action,
        skills_written=list(sync["skills_written"]),
        skills_added=list(sync["added"]),
        skills_updated=list(sync["updated"]),
        skills_unchanged=list(sync["unchanged"]),
        skills_pruned=list(sync["pruned"]),
        skills_orphaned=list(sync["orphaned"]),
        skills_clashed=list(sync["clashed"]),
        skills_root=str(sync["skills_root"]),
        sync_manifest=str((Path(sync["manifest"])).relative_to(dest)),
        mcp_manifest=manifest_path,
        repository=repository,
        gitignore_added=gitignore_added,
    )


def ensure_gh_pr_alias() -> dict[str, Any]:
    """Set the `gh prd` = `pr create --base dev` alias so PRs default to dev.

    Idempotent and non-clobbering: if `prd` already maps to the dev-base
    expansion it is left alone; if it maps to something else it is preserved
    (status ``conflict``) rather than overwritten. A side effect on the user's
    GLOBAL gh config — call it from the CLI install path, never from the pure
    file-writing install (so tests do not mutate the machine). gh-optional.
    """
    gh = shutil.which("gh")
    if gh is None:
        return envelope.component(
            ok=True, status="gh_unavailable", alias=GH_PR_ALIAS
        )
    try:
        listed = subprocess.run(
            [gh, "alias", "list"],
            capture_output=True, text=True, timeout=10, check=False,
        )
    except Exception:  # noqa: BLE001 - gh present but unrunnable; do not block install.
        return envelope.component(
            ok=True, status="gh_error", alias=GH_PR_ALIAS
        )
    existing: str | None = None
    for line in listed.stdout.splitlines():
        # gh prints "name: expansion" (older) or "name<TAB>expansion".
        for sep in (":", "\t"):
            if line.startswith(f"{GH_PR_ALIAS}{sep}"):
                existing = line.split(sep, 1)[1].strip()
                break
        if existing is not None:
            break
    if existing is not None:
        if existing.replace("'", "").strip() == GH_PR_ALIAS_EXPANSION:
            return envelope.component(
                ok=True, status="unchanged",
                alias=GH_PR_ALIAS, expansion=existing,
            )
        return envelope.component(
            ok=True, status="conflict", alias=GH_PR_ALIAS,
            expansion=existing,
            note=(f"`gh {GH_PR_ALIAS}` already maps to {existing!r}; left "
                  f"as-is. Run `gh alias set {GH_PR_ALIAS} "
                  f"'{GH_PR_ALIAS_EXPANSION}'` to adopt the dev-base default."),
        )
    try:
        done = subprocess.run(
            [gh, "alias", "set", GH_PR_ALIAS, GH_PR_ALIAS_EXPANSION],
            capture_output=True, text=True, timeout=10, check=False,
        )
    except Exception:  # noqa: BLE001
        return envelope.component(
            ok=True, status="gh_error", alias=GH_PR_ALIAS
        )
    return envelope.component(
        ok=done.returncode == 0,
        status="created" if done.returncode == 0 else "set_failed",
        alias=GH_PR_ALIAS,
        expansion=GH_PR_ALIAS_EXPANSION,
    )


def sync_agent_skills(
    dest: Path,
    *,
    skills_root: Path,
    skills_dir: Path | None = None,
    only: tuple[str, ...] | None = None,
    prune: bool = True,
) -> dict[str, Any]:
    """Synchronize one installed skills root from the bundled canon.

    Only skills recorded in this root's sync manifest are ever replaced or
    removed. A skill folder the manifest does not record is someone else's:
    when its name matches a bundled skill it is reported in ``clashed`` and
    left byte-identical (PACT agent-connect-3412), and any other unrecorded
    folder is reported as orphaned and preserved. ``only`` limits the managed
    set to the named bundled skills. ``prune=False`` keeps a previously
    managed skill that ``only`` leaves out, still managed, instead of removing
    it (``okg agent connect`` without ``--writes`` must not remove the
    ``okg-record`` skill an earlier ``--writes`` connect installed).
    """
    dest = dest.resolve()
    skills_root = skills_root.resolve()
    skills_dir = _SKILLS_DIR if skills_dir is None else skills_dir
    source_skills = {
        skill.name: skill
        for skill in bundled_skills(skills_dir)
        if only is None or skill.name in only
    }
    source_hashes = {
        name: _skill_digest(skill)
        for name, skill in source_skills.items()
    }
    manifest = _read_sync_manifest(skills_root)
    previously_managed = set(manifest.get("managed_skills") or [])
    previous_hashes = dict(manifest.get("hashes") or {})

    added: list[str] = []
    updated: list[str] = []
    unchanged: list[str] = []
    pruned: list[str] = []
    clashed: list[str] = []
    kept: dict[str, str] = {}

    for name in sorted(previously_managed - set(source_skills)):
        target = skills_root / name
        if not prune:
            if target.exists():
                kept[name] = str(previous_hashes.get(name) or _skill_digest(target))
            continue
        if target.exists():
            _remove_installed_skill(target)
        pruned.append(name)

    for name, source in sorted(source_skills.items()):
        target = skills_root / name
        if not target.exists() and not target.is_symlink():
            _copy_skill_dir(source, target)
            added.append(name)
            continue
        if name not in previously_managed:
            # Not ours: never overwrite or adopt it.
            clashed.append(name)
            continue
        if _skill_digest(target) != source_hashes[name]:
            _remove_installed_skill(target)
            _copy_skill_dir(source, target)
            updated.append(name)
        else:
            unchanged.append(name)

    installed_dirs = {
        p.name
        for p in skills_root.iterdir()
        if p.is_dir() and (p / "SKILL.md").is_file()
    } if skills_root.is_dir() else set()
    managed = {
        name: digest
        for name, digest in source_hashes.items()
        if name not in clashed
    }
    managed.update(kept)
    orphaned = sorted(installed_dirs - set(source_skills) - set(kept))
    manifest_path = _write_sync_manifest(
        skills_root,
        managed_skills=sorted(managed),
        hashes=dict(sorted(managed.items())),
    )
    skills_written = [
        str((skills_root / name / "SKILL.md").relative_to(dest))
        for name in sorted(source_skills)
        if name not in clashed
    ]
    return envelope.component(
        ok=True,
        skills_root=str(skills_root),
        manifest=str(manifest_path),
        skills_written=skills_written,
        added=added,
        updated=updated,
        unchanged=unchanged,
        pruned=pruned,
        orphaned=orphaned,
        clashed=clashed,
    )


def remove_managed_skills(skills_root: Path) -> dict[str, Any]:
    """Remove the skills this root's sync manifest records, then the manifest.

    The undo of :func:`sync_agent_skills`. A folder the manifest does not
    record is never touched. A recorded skill whose files no longer match the
    digest recorded at install time was edited by someone, so it is kept and
    reported in ``modified`` rather than deleted.
    """
    skills_root = skills_root.resolve()
    manifest = _read_sync_manifest(skills_root)
    hashes = dict(manifest.get("hashes") or {})
    removed: list[str] = []
    modified: list[str] = []
    missing: list[str] = []
    for name in sorted(manifest.get("managed_skills") or []):
        target = skills_root / name
        if not target.exists():
            missing.append(name)
            continue
        recorded = hashes.get(name)
        if recorded and _skill_digest(target) != recorded:
            modified.append(name)
            continue
        _remove_installed_skill(target)
        removed.append(name)
    manifest_path = skills_root / _SYNC_MANIFEST
    if manifest_path.is_file():
        if modified:
            _write_sync_manifest(
                skills_root,
                managed_skills=modified,
                hashes={name: hashes[name] for name in modified if name in hashes},
            )
        else:
            manifest_path.unlink()
    return envelope.component(
        ok=True,
        skills_root=str(skills_root),
        removed=removed,
        modified=modified,
        missing=missing,
    )


def managed_skill_names(skills_root: Path) -> list[str]:
    """The skills this root's sync manifest records (empty when none)."""
    return sorted(_read_sync_manifest(skills_root).get("managed_skills") or [])


def agent_skill_freshness(
    root: Path,
    *,
    skills_dir: Path | None = None,
    targets: tuple[str, ...] = ("claude-code", "codex"),
) -> dict[str, Any]:
    """Compare installed repo-local skills against the bundled canon."""
    root = root.resolve()
    skills_dir = _SKILLS_DIR if skills_dir is None else skills_dir
    source_skills = {skill.name: skill for skill in bundled_skills(skills_dir)}
    source_hashes = {
        name: _skill_digest(skill)
        for name, skill in source_skills.items()
    }
    target_reports: dict[str, Any] = {}
    totals = {
        "current": 0,
        "stale": 0,
        "missing": 0,
        "orphaned": 0,
    }
    clashes: list[str] = []
    for target in targets:
        skills_root = _skills_root_for_agent(target, root)
        managed = set(_read_sync_manifest(skills_root).get("managed_skills") or [])
        skills: dict[str, Any] = {}
        for name in sorted(source_skills):
            installed = skills_root / name
            if not installed.is_dir():
                status = "missing"
                installed_hash = None
            else:
                installed_hash = _skill_digest(installed)
                status = (
                    "current"
                    if installed_hash == source_hashes[name]
                    else "stale"
                )
            totals[status] += 1
            unmanaged = (
                status == "stale"
                and (skills_root / name).exists()
                and name not in managed
            )
            if unmanaged:
                clashes.append(str(skills_root / name))
            skills[name] = {
                "status": status,
                "expected_hash": source_hashes[name],
                "installed_hash": installed_hash,
                "unmanaged": unmanaged,
            }
        installed_dirs = {
            p.name
            for p in skills_root.iterdir()
            if p.is_dir() and (p / "SKILL.md").is_file()
        } if skills_root.is_dir() else set()
        orphaned = sorted(installed_dirs - set(source_skills))
        totals["orphaned"] += len(orphaned)
        target_reports[target] = envelope.component(
            ok=all(
                row["status"] == "current"
                for row in skills.values()
            ),
            skills_root=str(skills_root.relative_to(root)),
            **{
            "current_count": sum(
                1 for row in skills.values()
                if row["status"] == "current"
            ),
            "stale_count": sum(
                1 for row in skills.values()
                if row["status"] == "stale"
            ),
            "missing_count": sum(
                1 for row in skills.values()
                if row["status"] == "missing"
            ),
            "orphaned_count": len(orphaned),
            "orphaned": orphaned,
            "skills": skills,
            },
        )
    ok = totals["stale"] == 0 and totals["missing"] == 0
    fix_hint = (
        f"uv run okg pact install --agent codex --dest {root} && "
        f"uv run okg pact install --agent claude-code --dest {root}"
    )
    if clashes:
        # `okg pact install` never replaces a folder its sync manifest does
        # not record, so re-running it alone would report the same state.
        fix_hint = (
            "move aside the skill folders okg did not install ("
            + ", ".join(clashes)
            + "), then run "
            + fix_hint
        )
    return envelope.component(
        ok=ok,
        status="current" if ok else "blocked",
        code="agent_skills_current" if ok else "agent_skills_stale_or_missing",
        message=(
            "installed agent skills match bundled canon"
            if ok else
            "installed agent skills are missing or stale"
        ),
        root=str(root),
        required_targets=list(targets),
        current_count=totals["current"],
        stale_count=totals["stale"],
        missing_count=totals["missing"],
        orphaned_count=totals["orphaned"],
        fix_hint=fix_hint,
        clashed=clashes,
        targets=target_reports,
    )


def _skills_root_for_agent(agent: str, root: Path) -> Path:
    if agent == "claude-code":
        return root / ".claude" / "skills"
    if agent in {"codex", "gemini"}:
        return root / ".agents" / "skills"
    raise ValueError(
        f"unknown agent target {agent!r}; choose from {', '.join(AGENT_TARGETS)}"
    )


def _copy_skill_dir(source: Path, target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(source, target)


def _remove_installed_skill(path: Path) -> None:
    if path.is_dir() and not path.is_symlink():
        shutil.rmtree(path)
        return
    path.unlink()


def _skill_digest(skill_dir: Path) -> str:
    h = hashlib.sha256()
    if not skill_dir.is_dir():
        return "sha256:missing"
    for path in sorted(p for p in skill_dir.rglob("*") if p.is_file()):
        rel = path.relative_to(skill_dir).as_posix()
        h.update(rel.encode("utf-8"))
        h.update(b"\0")
        h.update(path.read_bytes())
        h.update(b"\0")
    return f"sha256:{h.hexdigest()}"


def _read_sync_manifest(skills_root: Path) -> dict[str, Any]:
    path = skills_root / _SYNC_MANIFEST
    if not path.is_file():
        return {}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return raw if isinstance(raw, dict) else {}


def _write_sync_manifest(
    skills_root: Path,
    *,
    managed_skills: list[str],
    hashes: dict[str, str],
) -> Path:
    skills_root.mkdir(parents=True, exist_ok=True)
    path = skills_root / _SYNC_MANIFEST
    path.write_text(
        json.dumps(
            {
                "schema": "okg.agent_skills.sync.v1",
                "source": "src/okg/substrate/pact/skills",
                "managed_skills": managed_skills,
                "hashes": hashes,
            },
            indent=2,
            sort_keys=True,
        ) + "\n",
        encoding="utf-8",
    )
    return path


def _write_mcp_manifest(
    agent: str, dest: Path, mcp: McpServerSpec,
) -> Path:
    """Write the thin per-harness MCP manifest. Pointers only —
    skill bodies are never embedded; a re-run upserts in place."""
    server_entry = {
        "command": mcp.command,
        "args": list(mcp.args),
        **({"env": dict(mcp.env)} if mcp.env else {}),
    }
    if agent == "claude-code":
        # Project-scope .mcp.json: merge-preserve other servers.
        path = dest / ".mcp.json"
        existing: dict = {}
        if path.is_file():
            try:
                existing = json.loads(path.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                raise ValueError("existing Claude MCP config is not valid JSON")
            if not isinstance(existing, dict):
                raise ValueError("existing Claude MCP config must be a JSON object")
        servers = existing.setdefault("mcpServers", {})
        if not isinstance(servers, dict):
            raise ValueError("existing Claude MCP config mcpServers must be an object")
        servers[mcp.name] = server_entry
        atomic_write_text(
            path,
            json.dumps(existing, indent=2) + "\n", encoding="utf-8",
        )
        # MCP manifests carry credential references and are local runtime
        # configuration, never shared authority.  Keep newly written or
        # refreshed manifests private to the invoking user.
        path.chmod(0o600)
        return path
    if agent == "gemini":
        # Gemini CLI extension: gemini-extension.json bundling the
        # MCP server + the context file; skills stay in the shared
        # .agents/skills/ directory (referenced, not copied).
        ext_dir = dest / ".gemini" / "extensions" / "okg"
        ext_dir.mkdir(parents=True, exist_ok=True)
        path = ext_dir / "gemini-extension.json"
        existing_extension: dict[str, Any] = {}
        if path.is_file():
            try:
                decoded = json.loads(path.read_text(encoding="utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise ValueError("existing Gemini MCP config is not valid JSON") from exc
            if not isinstance(decoded, dict):
                raise ValueError("existing Gemini MCP config must be a JSON object")
            existing_extension = decoded
        existing_servers = existing_extension.setdefault("mcpServers", {})
        if not isinstance(existing_servers, dict):
            raise ValueError("existing Gemini MCP config mcpServers must be an object")
        existing_servers[mcp.name] = server_entry
        existing_extension.update(
            {
                "name": "okg",
                "version": "1.0.0",
                "contextFileName": "GEMINI.md",
            }
        )
        atomic_write_text(
            path,
            json.dumps(existing_extension, indent=2) + "\n",
            encoding="utf-8",
        )
        atomic_write_text(
            ext_dir / "GEMINI.md",
            instruction_section(dest=dest) + "\n", encoding="utf-8",
        )
        path.chmod(0o600)
        return path
    # codex: a config.toml snippet the operator (or agent) appends to
    # ~/.codex/config.toml — Codex has no project-scope MCP config.
    path = dest / ".agents" / "okg-mcp.config.toml"
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# OKG MCP server — append to ~/.codex/config.toml",
        f"[mcp_servers.{mcp.name}]",
        f'command = "{mcp.command}"',
        "args = [" + ", ".join(f'"{a}"' for a in mcp.args) + "]",
    ]
    if mcp.env:
        lines.append(f"[mcp_servers.{mcp.name}.env]")
        lines.extend(f'{k} = "{v}"' for k, v in sorted(mcp.env.items()))
    atomic_write_text(path, "\n".join(lines) + "\n", encoding="utf-8")
    path.chmod(0o600)
    return path
