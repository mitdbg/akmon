#!/usr/bin/env python3
"""Lint documented CLI references and exhaustive generated CLI inventories.

Every ``okg <verb> ...`` reference in docs / agents.md / source must point at a
real command path. Each adjacent token is checked against the live argparse
subparser tree for as long as that tree still expects a subcommand, so a
fictional third- or fourth-level choice is reported even when its prefix names
real command groups. In ``--surface`` mode, every verb in the argparse tree must have
matching rows in the generated audit and smoke manifest. The operator-facing
``docs/reference/cli.md`` remains a curated hot-path reference; its
commands are validated by the normal reference scan, but it is not an
exhaustive inventory.

Source strings are read with the Python parser, not line by line. A string
constant (including the literal parts of an f-string) is checked only where
it is written as a command: inside a backtick span, after a shell prompt, or
at the start of a ``fix_hint`` value. After "run", "use", "try" and similar
verbs, or after a colon, it counts only when its first word is a real
top-level verb or a ``--flag`` follows in the same sentence ("Error: okg
failed to connect" is English). Everywhere else ``okg`` is the project name
("the okg arm", "okg telemetry requires ..."), and the string is left alone.
Docstrings are developer notes and are not scanned. ``okg pact`` verbs that
are dispatched before argparse runs count as real commands.

Modes:
  (default)   — scan docs / source for fictional `okg <verb>`.
  --surface   — argparse ↔ audit/reference/manifest cross-check.
  --all       — both.

Walks `*.md` under `docs/`, plus `README.md` and `src/okg/AGENTS.md`,
and the string constants of every `*.py` under `src/`; finds `okg <verb>`
patterns in fenced code blocks, inline prose and source command strings;
validates each verb against the substrate's argparse tree.

Reports offenders to stdout and exits non-zero. Honors the spec
invariant in `openspec/changes/align-substrate-cli-docs-and-runbooks/
specs/cli-surface/spec.md`.

Per `align-substrate-cli-docs-and-runbooks` Phase 5 + extended by
`audit-test-and-document-cli-surface` Phase 5.
"""
from __future__ import annotations

import argparse
import ast
import re
import sys
from pathlib import Path


# Pattern to match command-token runs in docs/code. Capture every adjacent
# lowercase token after `okg` so nested argparse choices can be checked; the
# scanner stops walking once it reaches a leaf parser or a non-command token.
# The negative lookahead keeps a later `okg` invocation on the same prose line
# available to the next regex match. Negative-lookbehind
# excludes path-like contexts (`/okg`, `-okg`, `.okg`) where the
# `okg` token is a directory or repo name, and shell env-var
# assignment (`PGPASSWORD=okg`, `KEY=okg`) where `okg` is a value
# being assigned, not a CLI invocation, and a `<okg ...>` placeholder
# (`uv tool install <okg wheel or source>`), which names no command.
_CLI_PATTERN = re.compile(
    r"(?<![a-zA-Z0-9_/.\-=<])okg\s+"
    r"([a-z][a-z0-9_-]*"
    r"(?:\s+(?!okg\b)[a-z][a-z0-9_-]*)*)"
)


_FENCE_PATTERN = re.compile(r"^\s{0,3}(`{3,}|~{3,})")
_INLINE_CODE_PATTERN = re.compile(r"(`+)(.*?)\1")


# Tokens that follow `okg` but are NOT a verb — typically a flag
# or a placeholder. Exclude these to keep noise down.
_NOT_VERB_TOKENS = {
    "<",  # `okg <verb>`
    "--",  # `okg --help`
    "&&", "||", ";",
    "#",  # `okg # comment`
}


# Paths the lint scans.
_SCAN_GLOBS = (
    "README.md",
    "PROFESSOR_INSTALL.md",
    "docs/**/*.md",
    "src/okg/AGENTS.md",
)


# Python sources whose string constants are scanned for command references.
_SRC_GLOB = "src/**/*.py"


# Source strings that name a command which does not exist on purpose. Keyed by
# (repo-relative path, reported reference); each entry says why it is allowed.
# An entry that no longer matches anything is itself reported, so the list
# cannot silently outlive the string it excuses.
_SOURCE_ALLOWLIST: dict[tuple[str, str], str] = {
    ("src/okg/substrate/cli/parser_groups.py", "okg bootstrap"): (
        "`okg snapshot-load` help text naming the verb's former name"
    ),
}


# Keyword arguments and dict keys whose string value is operator guidance. A
# value that starts with `okg ` is a command, not a sentence about okg.
_HINT_FIELDS = frozenset({"fix_hint"})


# A shell prompt directly before `okg` ("$ okg ...") makes the rest a command.
_SOURCE_PROMPT_PREFIX = re.compile(r"\$\s*$")


# An imperative verb ("run okg ...") or a colon ("recreate it: okg ...")
# directly before `okg` usually introduces a command, but English uses both
# too: "Error: okg failed to connect", "then okg publishes the generation".
# After one of these, a run is checked only when its first word is a real
# top-level verb, or when a `--flag` follows it in the same sentence.
_SOURCE_WEAK_PREFIX = re.compile(
    r"(?:(?:^|[\s(])(?:re-?run|run|try|use|execute|invoke|then)\s+"
    r"|:\s+)$",
    re.IGNORECASE,
)


# A `--flag` later in the same sentence: the mark of a command line.
_FLAG_IN_SENTENCE = re.compile(r"[^.;\n]*?\s--[a-z]")


# `okg` as a word in source text, for mapping a match back to its line.
_OKG_WORD = re.compile(r"(?<![a-zA-Z0-9_/.\-=<])okg(?![a-zA-Z0-9_])")


# Stands in for an f-string's `{...}` field. It starts with `<`, so the token
# walk stops there as it does for a documented `<placeholder>`.
_FSTRING_FIELD = "<value>"


# Paths explicitly excluded — archived proposals, working notes,
# abandoned-deployment docs (per MEMORY.md's active-deployments list:
# cms/memex/intersystems/example are abandoned).
_EXCLUDE_PATTERNS = (
    "openspec/changes/archive/",
    "docs/working-notes/",
    ".venv/",
    "node_modules/",
)


# Grammatical connectives. These are never argparse choices at any depth
# (asserted by test_lint_connectives_are_never_command_names), so a run of
# command tokens ends at one wherever it appears — including inside a fenced
# block that holds prose rather than commands.
_PROSE_CONNECTIVES = frozenset({
    "as", "to", "for", "is", "by", "in", "the", "that", "when",
    "while", "if", "because", "and", "or", "of", "on", "are",
    "with", "from", "via", "into",
})


# Words that plausibly name a command but usually follow `okg` as the project
# name in prose. Suppressed only in the first position, where `okg` is being
# used as a noun; deeper down they are checked like any other command token.
_PROSE_FOLLOWERS = _PROSE_CONNECTIVES | frozenset({
    "uses", "use", "becomes", "carries", "tracks", "stores", "exposes",
    "deployment", "deployments", "instance", "instances",
    "session", "sessions", "repo", "submodule",
    "tree", "core", "team", "project", "package",
})


def _in_inline_code(line: str, offset: int) -> bool:
    """Return whether offset falls inside a Markdown backtick span."""
    return any(
        span.start(2) <= offset < span.end(2)
        for span in _INLINE_CODE_PATTERN.finditer(line)
    )


def _is_explicit_command_context(
    line: str,
    offset: int,
    *,
    in_fence: bool,
) -> bool:
    """Distinguish command syntax from prose that names a command group."""
    if in_fence or _in_inline_code(line, offset):
        return True
    prefix = line[:offset]
    # A `.py` string that opens with `okg ...` is just as often an English
    # error message ("okg telemetry requires --deployment") as a command
    # hint, so quoted source strings stay prose here.
    return re.fullmatch(
        r"\s*(?:"
        r"(?:[-+*]\s+(?:\[[ xX]\]\s+)?)"
        r"|(?:\d+[.)]\s+)"
        r"|(?:command:\s+)"
        r")?"
        r"(?:[$>%]\s*)?"
        r"(?:[A-Za-z_][A-Za-z0-9_]*=\S+\s+)*",
        prefix,
        re.IGNORECASE,
    ) is not None


_CommandTree = dict[str, "_CommandTree"]


def _parser_command_tree(parser: argparse.ArgumentParser) -> _CommandTree:
    """Return every argparse subparser choice as a recursive command tree."""
    commands: _CommandTree = {}
    for action in parser._actions:
        if not isinstance(action, argparse._SubParsersAction):
            continue
        for name, sub_parser in action.choices.items():
            commands[name] = _parser_command_tree(sub_parser)
    return commands


def _build_parser_tree() -> _CommandTree:
    """Return the argparse tree alone: what the smoke manifest must cover."""
    from okg.substrate.cli.main import _build_parser

    return _parser_command_tree(_build_parser())


def _build_command_tree() -> _CommandTree:
    """Build the documented-command authority from the live CLI parser.

    `okg pact <verb>` for the v5 verbs is dispatched before argparse runs
    (`_PACT_V5_VERBS` in cli/main.py), so some of those verbs never reach the
    parser; they are added from the same set the dispatcher reads.
    """
    from okg.substrate.cli.main import _PACT_V5_VERBS

    tree = _build_parser_tree()
    pact = tree.setdefault("pact", {})
    for verb in _PACT_V5_VERBS:
        pact.setdefault(verb, {})
    return tree


def _build_known_verbs() -> tuple[set[str], dict[str, set[str]]]:
    """Return the two-level projection used by the smoke manifest."""
    command_tree = _build_parser_tree()
    top_level = set(command_tree)
    subverbs = {
        verb: set(children)
        for verb, children in command_tree.items()
        if children
    }
    return top_level, subverbs


def _scan_file(
    path: Path,
    command_tree: _CommandTree,
) -> list[tuple[int, str, str]]:
    """Return (lineno, matched_text, reason) for each offender in path."""
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return []
    offenders: list[tuple[int, str, str]] = []
    fence: tuple[str, int] | None = None
    for line_no, line in enumerate(text.splitlines(), start=1):
        fence_match = _FENCE_PATTERN.match(line)
        if fence is not None:
            if (
                fence_match
                and fence_match.group(1)[0] == fence[0]
                and len(fence_match.group(1)) >= fence[1]
                # CommonMark forbids an info string on a closing fence, so a
                # nested ```bash inside a ```markdown block opens, never
                # closes. Without this the fence parity inverts for the rest
                # of the file.
                and not line[fence_match.end():].strip()
            ):
                fence = None
                continue
            in_fence = True
        else:
            if fence_match:
                marker = fence_match.group(1)
                fence = (marker[0], len(marker))
                continue
            in_fence = False
        for match in _CLI_PATTERN.finditer(line):
            explicit_command = _is_explicit_command_context(
                line,
                match.start(),
                in_fence=in_fence,
            )
            finding = _check_command_tokens(
                match.group(1).split(),
                command_tree,
                explicit_command=explicit_command,
            )
            if finding is not None:
                offenders.append((line_no, *finding))
    return offenders


def _check_command_tokens(
    tokens: list[str],
    command_tree: _CommandTree,
    *,
    explicit_command: bool,
) -> tuple[str, str] | None:
    """Walk tokens down the parser tree; return (reference, reason) if fictional."""
    choices = command_tree
    command_path: list[str] = []
    for token in tokens:
        if token not in choices:
            if token in _NOT_VERB_TOKENS or token.startswith("<"):
                return None
            if token in _PROSE_CONNECTIVES:
                # No command at any depth is named `is`, `are`, `the`
                # ... so this run of tokens is an English sentence.
                return None
            if not command_path and token in _PROSE_FOLLOWERS:
                # `okg` is also the project name, so an English word
                # right after it is prose wherever it appears --
                # including inline code spans such as
                # "the `okg sessions` corpus" and bullet lines.
                return None
            if not explicit_command and command_path:
                # In prose, a real command-group name can be followed
                # by ordinary nouns: "the okg pact workflow" or
                # "the okg pact evidence model". Explicit command
                # syntax never receives this suppression.
                return None
            attempted_path = " ".join((*command_path, token))
            if command_path:
                reason = f"unknown subcommand `{attempted_path}`"
            else:
                reason = f"unknown top-level verb `{token}`"
            return f"okg {attempted_path}", reason
        command_path.append(token)
        choices = choices[token]
        if not choices:
            # Remaining words are options, positional values, or prose
            # belonging to the leaf command rather than more commands.
            return None
    return None


_BACKTICK_SPAN = re.compile(r"(`+)(.*?)\1", re.DOTALL)


def _source_command_context(
    text: str,
    offset: int,
    *,
    hint_value: bool,
) -> str | None:
    """Classify the `okg` at offset in a source string.

    "strong": certainly a command (backticks, a `$` prompt, the start of a
    `fix_hint`). "weak": after a verb or colon, a command only if it looks
    like one. None: English about okg.
    """
    if any(
        span.start(2) <= offset < span.end(2)
        for span in _BACKTICK_SPAN.finditer(text)
    ):
        return "strong"
    prefix = text[:offset]
    if hint_value and not prefix.strip():
        return "strong"
    if _SOURCE_PROMPT_PREFIX.search(prefix) is not None:
        return "strong"
    if _SOURCE_WEAK_PREFIX.search(prefix) is not None:
        return "weak"
    return None


def _okg_source_lines(node: ast.expr, source_lines: list[str]) -> list[int]:
    """Return the source line of each `okg` word inside a string node.

    A value's line cannot be derived from the value itself: an escaped `\\n`
    adds a newline that is not a source line, and implicitly joined pieces
    add source lines that are not in the value. Reading the node's own source
    segment gives the real line of each occurrence, in order.
    """
    first, last = node.lineno, node.end_lineno or node.lineno
    lines: list[int] = []
    for line_no in range(first, last + 1):
        raw = source_lines[line_no - 1].encode("utf-8")
        end = node.end_col_offset if line_no == last else len(raw)
        start = node.col_offset if line_no == first else 0
        segment = raw[start:end].decode("utf-8", errors="replace")
        lines.extend(line_no for _ in _OKG_WORD.finditer(segment))
    return lines


def _source_strings(module: ast.Module) -> list[tuple[ast.expr, str, bool]]:
    """Return (node, text, is_hint_value) for each non-docstring string.

    Implicitly concatenated literals arrive as one constant. An f-string is
    read as one string with each `{...}` field replaced by a placeholder.
    """
    docstrings: set[int] = set()
    hint_nodes: set[int] = set()
    fstring_parts: set[int] = set()
    for node in ast.walk(module):
        if isinstance(
            node,
            (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef),
        ):
            body = node.body
            if (
                body
                and isinstance(body[0], ast.Expr)
                and isinstance(body[0].value, ast.Constant)
                and isinstance(body[0].value.value, str)
            ):
                docstrings.add(id(body[0].value))
        hint_values: list[ast.expr] = []
        if isinstance(node, ast.keyword) and node.arg in _HINT_FIELDS:
            hint_values.append(node.value)
        elif isinstance(node, ast.Dict):
            hint_values.extend(
                value
                for key, value in zip(node.keys, node.values)
                if isinstance(key, ast.Constant) and key.value in _HINT_FIELDS
            )
        for value in hint_values:
            hint_nodes.update(id(child) for child in ast.walk(value))
    strings: list[tuple[ast.expr, str, bool]] = []
    # ast.walk is breadth-first, so an f-string is seen before its parts.
    for node in ast.walk(module):
        if isinstance(node, ast.JoinedStr):
            fstring_parts.update(id(part) for part in node.values)
            text = "".join(
                part.value
                if isinstance(part, ast.Constant) and isinstance(part.value, str)
                else _FSTRING_FIELD
                for part in node.values
            )
            strings.append((node, text, id(node) in hint_nodes))
        elif (
            isinstance(node, ast.Constant)
            and isinstance(node.value, str)
            and id(node) not in docstrings
            and id(node) not in fstring_parts
        ):
            strings.append((node, node.value, id(node) in hint_nodes))
    return strings


def _scan_python_file(
    path: Path,
    command_tree: _CommandTree,
) -> list[tuple[int, str, str]]:
    """Return (lineno, matched_text, reason) for each fictional source command."""
    text = path.read_text(encoding="utf-8")
    if _CLI_PATTERN.search(text) is None:
        return []
    try:
        module = ast.parse(text, filename=str(path))
    except SyntaxError as err:
        return [(err.lineno or 0, "<unparsed>", f"cannot parse: {err.msg}")]
    source_lines = text.splitlines()
    offenders: list[tuple[int, str, str]] = []
    for node, value, hint_value in _source_strings(module):
        okg_offsets = [m.start() for m in _OKG_WORD.finditer(value)]
        okg_lines: list[int] | None = None
        for match in _CLI_PATTERN.finditer(value):
            context = _source_command_context(
                value,
                match.start(),
                hint_value=hint_value,
            )
            if context is None:
                continue
            tokens = match.group(1).split()
            if (
                context == "weak"
                and tokens[0] not in command_tree
                and _FLAG_IN_SENTENCE.match(value, match.end()) is None
            ):
                # "Error: okg failed to connect": English, not a command.
                continue
            finding = _check_command_tokens(
                tokens,
                command_tree,
                explicit_command=True,
            )
            if finding is None:
                continue
            if okg_lines is None:
                okg_lines = _okg_source_lines(node, source_lines)
            index = okg_offsets.index(match.start())
            reference, reason = finding
            if len(okg_lines) == len(okg_offsets):
                line_no = okg_lines[index]
            else:
                # An `okg` inside an f-string field's code makes the counts
                # differ; say so rather than guess a line.
                line_no = node.lineno
                reason = f"{reason} (line {line_no} is where the string starts)"
            offenders.append((line_no, reference, reason))
    return offenders


def _should_skip(path: Path) -> bool:
    s = str(path)
    return any(pat in s for pat in _EXCLUDE_PATTERNS)


# ─── Phase 5 surface check: argparse ↔ docs/manifest ────────────


def _all_verbs(
    top_level: set[str], subverbs: dict[str, set[str]]
) -> list[str]:
    """Flatten top-level + nested into the manifest's `top sub` form."""
    out: list[str] = list(top_level)
    for top, subs in subverbs.items():
        for sub in subs:
            out.append(f"{top} {sub}")
    return out


def _check_smoke_manifest(
    repo_root: Path, verbs: list[str]
) -> list[str]:
    """Every verb has a row in CLI_SMOKE_INDEX.yaml."""
    manifest_path = (
        repo_root / "tests/substrate/cli/CLI_SMOKE_INDEX.yaml"
    )
    if not manifest_path.is_file():
        return [
            "missing: tests/substrate/cli/CLI_SMOKE_INDEX.yaml"
        ]
    # Parse the YAML to get the verb keys. Use yaml-free regex so the
    # lint can run without pyyaml installed.
    text = manifest_path.read_text()
    # Match indented YAML keys of the form `  <verb>:` or `  <verb sub>:`
    # (not `    test_file:` etc.).
    keys = set()
    for m in re.finditer(
        r"^  ([a-z][a-z0-9][a-z0-9 -]*):\s*$", text, re.M,
    ):
        keys.add(m.group(1).strip())
    missing = sorted(set(verbs) - keys)
    return [
        f"missing manifest row: `okg {v}` "
        f"(add to tests/substrate/cli/CLI_SMOKE_INDEX.yaml)"
        for v in missing
    ]


def _surface_check(repo_root: Path) -> int:
    """Run the argparse ↔ executable smoke-manifest cross-check.

    Returns 0 if every argparse verb has a row in the executable artifact;
    1 otherwise, with offenders printed to stdout.
    """
    top_level, subverbs = _build_known_verbs()
    verbs = _all_verbs(top_level, subverbs)
    offenders: list[str] = []
    offenders.extend(_check_smoke_manifest(repo_root, verbs))
    for line in offenders:
        print(line)
    if offenders:
        print(
            f"\n{len(offenders)} surface offender(s) across "
            f"{len(verbs)} argparse verbs.",
            file=sys.stderr,
        )
        return 1
    print(
        f"surface clean: all {len(verbs)} argparse verbs have "
        f"smoke-manifest entries.",
        file=sys.stderr,
    )
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Lint okg CLI references in docs + source",
    )
    parser.add_argument(
        "--quiet", action="store_true",
        help="Only print offending lines (skip the run summary).",
    )
    parser.add_argument(
        "--surface", action="store_true",
        help="Skip the docs/source scan; just run the argparse "
             "↔ exhaustive audit/manifest cross-check.",
    )
    parser.add_argument(
        "--all", action="store_true",
        help="Run both the docs scan AND the surface cross-check; "
             "exits non-zero if either reports offenders.",
    )
    parser.add_argument(
        "--extra-file", action="append", type=Path, default=[],
        help="Additionally scan this file (repeatable). Lets tests lint "
             "synthetic fragments from a temp dir instead of writing into "
             "the shared docs/ tree (which races concurrent docs scanners).",
    )
    parser.add_argument(
        "--extra-only", action="store_true",
        help="Scan only the --extra-file paths, not the repository tree. "
             "For tests that lint one synthetic fragment.",
    )
    args = parser.parse_args(argv)

    repo_root = Path(__file__).resolve().parent.parent
    if args.surface:
        return _surface_check(repo_root)
    command_tree = _build_command_tree()

    paths_to_scan: list[Path] = []
    for glob in () if args.extra_only else (*_SCAN_GLOBS, _SRC_GLOB):
        paths_to_scan.extend(repo_root.glob(glob))
    paths_to_scan = [p for p in paths_to_scan if not _should_skip(p)]
    paths_to_scan.extend(p.resolve() for p in args.extra_file)

    total_offenders = 0
    allowlist_used: set[tuple[str, str]] = set()
    scanned: set[str] = set()
    for path in sorted(paths_to_scan):
        if path.suffix == ".py":
            offenders = _scan_python_file(path, command_tree)
        else:
            offenders = _scan_file(path, command_tree)
        try:
            rel = path.relative_to(repo_root).as_posix()
        except ValueError:
            rel = str(path)  # --extra-file may live outside the repo
        scanned.add(rel)
        for line_no, matched, reason in offenders:
            if (rel, matched) in _SOURCE_ALLOWLIST:
                allowlist_used.add((rel, matched))
                continue
            print(f"{rel}:{line_no}: {matched}  -- {reason}")
            total_offenders += 1
    # A full scan owns every entry, so an entry for a deleted file is stale
    # too; a fragment scan answers only for the files it read.
    unused_allowlist = {
        entry
        for entry in set(_SOURCE_ALLOWLIST) - allowlist_used
        if not args.extra_only or entry[0] in scanned
    }
    for rel, matched in sorted(unused_allowlist):
        print(
            f"{rel}: {matched}  -- allowlist entry matches nothing; "
            f"remove it from _SOURCE_ALLOWLIST"
        )
        total_offenders += 1

    if not args.quiet:
        if total_offenders == 0:
            print(
                f"clean: scanned {len(paths_to_scan)} files; no "
                f"fictional `okg <verb>` references found.",
                file=sys.stderr,
            )
        else:
            print(
                f"\n{total_offenders} offender(s) across "
                f"{len(paths_to_scan)} scanned files.",
                file=sys.stderr,
            )
    rc = 0 if total_offenders == 0 else 1
    if args.all:
        rc = max(rc, _surface_check(repo_root))
    return rc


if __name__ == "__main__":
    sys.exit(main())
