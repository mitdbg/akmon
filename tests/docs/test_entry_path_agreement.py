"""First-contact docs may only name bring-up commands that exist and parse.

Per PACT `define-newcomer-entry-path` (req.entry.docs-name-only-real-commands).

Three canonical entry sequences had drifted apart, and one of them was
unrunnable: `okg-deployments/README.md` tells operators to run
``okg install --profile cms``, which exits ``unrecognized arguments:
--profile cms``. The advertised install command for the entire reference
gallery had never worked, and nothing noticed, because no check ever fed a
documented command back through the parser that has to accept it.

Two assertions, in increasing strength:

  1. A bring-up command in a first-contact document names the declared
     ``newcomer_bring_up`` verb or a declared ``alias`` — prose cannot
     introduce a door the CLI surface has not declared.
  2. Every extracted command PARSES against the live argparse tree — prose
     cannot name a flag the verb does not accept.

Second-level verbs (``okg deployment bootstrap``) are parsed too, so a
documented subcommand invocation is held to the same bar.
"""
from __future__ import annotations

import argparse
import contextlib
import io
import re
import shlex
from pathlib import Path

import pytest

from okg.substrate.cli import entry_roles
from okg.substrate.cli.main import _build_parser
from tests.mirror_scope import shipped


REPO_ROOT = Path(__file__).resolve().parents[2]

#: Surfaces a newcomer meets before they understand the system. Kept
#: explicit rather than globbed: the point is that someone adding a new
#: front-door page has to opt it in, and think about the vocabulary and
#: command contract while doing so.
#: Surfaces held to BOTH assertions. The parse assertion needs each
#: extracted string to be one command, so this list is limited to pages
#: whose code blocks hold one invocation each — see
#: `DOOR_NAMING_ONLY_DOCS` for the rest.
FIRST_CONTACT_DOCS: tuple[str, ...] = (
    "README.md",
    "docs/README.md",
    "install.sh",
    "docs/runbooks/workspace-graph/setup.md",
)

#: Surfaces held to the DOOR-NAMING assertion only. Which verb a page tells
#: a reader to run is decided by the first token, so a block holding
#: several commands is still checkable — but feeding that merged string to
#: argparse reports a phantom "unrecognized arguments" for the second
#: command onward, and pages using concrete values (rather than
#: placeholders) additionally need the profile-answer flag injection the
#: parse check does not perform. Both are extractor limits, recorded in the
#: PACT rather than papered over.
#:
#: The reference-deployment READMEs the requirement names live in the
#: separate reference-deployments repository and are unreachable from
#: here; the ones in this checkout are listed.
DOOR_NAMING_ONLY_DOCS: tuple[str, ...] = (
    "docs/runbooks/troubleshooting/first-run.md",
    "deployments/README.md",
    "deployments/okg-workspace/README.md",
)

#: `okg <verb> ...` within an already-normalized code region. Stops at a
#: shell operator; `<...>` placeholders are preserved because a doc's
#: angle brackets are far more often a placeholder than a redirect.
_OKG_CMD = re.compile(
    r"(?:^|\s)(?:uv\s+run\s+(?:--extra\s+\S+\s+)*)?okg\s+([a-z][^|&;\n]*)"
)

#: Fenced code blocks and inline code spans. Commands are extracted only
#: from these, then whitespace-normalized — a documented command wrapped
#: across lines inside one span is still one command, and matching
#: line-by-line would truncate it mid-flag and report a phantom error.
_FENCED = re.compile(r"```[a-zA-Z0-9]*\n(.*?)```", re.S)
_INLINE = re.compile(r"`([^`]+)`", re.S)

#: Placeholder forms a doc or script legitimately uses; they are not real
#: values and must not be fed to the parser as if they were. `$@` and `$1`
#: matter because install.sh forwards its own argv.
_PLACEHOLDER = re.compile(r"<[^>]+>|\$\{?[A-Z_]+\}?|\$[@*0-9]|\.\.\.")

#: A shell line continuation. Documented commands are routinely wrapped
#: across lines, and matching per-line would truncate them mid-flag —
#: reporting "expected one argument" for a command that is in fact fine.
_CONTINUATION = re.compile(r"\\\n\s*")


def _code_regions(text: str, *, is_markdown: bool) -> list[str]:
    """Text spans that can contain a runnable command.

    A shell script is code end to end. Markdown prose is not — only its
    fenced blocks and inline spans are, and a command mentioned in a
    sentence without backticks is not a documented invocation.
    """
    if not is_markdown:
        return [text]
    regions = _FENCED.findall(text)
    without_fences = _FENCED.sub("\n", text)
    regions.extend(_INLINE.findall(without_fences))
    return regions


def _iter_okg_commands(text: str, *, is_markdown: bool) -> list[str]:
    """Extract `okg ...` invocations from code regions, unwrapped."""
    out: list[str] = []
    for region in _code_regions(text, is_markdown=is_markdown):
        # Join shell continuations, then collapse the newlines a span may
        # carry from wrapping, so one command is one string.
        normalized = _CONTINUATION.sub(" ", region)
        normalized = re.sub(r"\s*\n\s*", " ", normalized)
        for m in _OKG_CMD.finditer(normalized):
            raw = m.group(1).strip().rstrip("\\").strip()
            if not raw or raw.startswith("-"):
                continue
            out.append(re.sub(r"\s{2,}", " ", raw))
    return out


def _top_verb(command: str) -> str | None:
    try:
        parts = shlex.split(command)
    except ValueError:
        return None
    return parts[0] if parts else None


def _bring_up_verbs() -> set[str]:
    """Verbs whose appearance in a doc constitutes 'how to start'."""
    return {
        v for v, r in entry_roles.ENTRY_ROLES.items()
        if r in {
            entry_roles.NEWCOMER_BRING_UP,
            entry_roles.SUPERSEDED_ENTRY,
            entry_roles.ALIAS,
            entry_roles.FROZEN,
        }
    }


def _documented_doors() -> set[str]:
    """What a first-contact doc is allowed to tell a newcomer to run.

    The door, and only the door. An alias exists so that EXISTING scripts
    and runbooks keep working — that is not the same as being a path a
    first-contact page may hand a newcomer. Admitting aliases here would
    make this check unfailable the moment the last superseded door became
    one: `allowed` would equal the whole bring-up set and no document
    could offend.
    """
    return {entry_roles.newcomer_door()}


def _existing_docs(
    surfaces: tuple[str, ...] = FIRST_CONTACT_DOCS,
) -> list[Path]:
    return [
        REPO_ROOT / rel
        for rel in surfaces
        if (REPO_ROOT / rel).is_file()
    ]


def test_first_contact_docs_exist() -> None:
    """The registry may not silently point at deleted pages."""
    # deployments/okg-workspace/README.md is private; in the public copy the
    # registry's shipped pages are checked (tests/mirror_scope.py).
    missing = [
        rel for rel in shipped(FIRST_CONTACT_DOCS + DOOR_NAMING_ONLY_DOCS)
        if not (REPO_ROOT / rel).is_file()
    ]
    assert not missing, f"first-contact surfaces not found: {missing}"


def test_first_contact_docs_name_only_the_declared_door() -> None:
    """A doc may only tell a newcomer to run the declared door or an alias."""
    allowed = _documented_doors()
    bring_up = _bring_up_verbs()
    offenders: list[str] = []
    for path in _existing_docs(FIRST_CONTACT_DOCS + DOOR_NAMING_ONLY_DOCS):
        for command in _iter_okg_commands(
            path.read_text(errors="replace"),
            is_markdown=path.suffix == ".md",
        ):
            verb = _top_verb(command)
            if verb in bring_up and verb not in allowed:
                rel = path.relative_to(REPO_ROOT)
                offenders.append(f"{rel}: `okg {command}` (verb {verb!r})")
    if offenders:
        pytest.fail(
            "first-contact docs name a bring-up path that is not the "
            f"declared door {sorted(allowed)}:\n  - "
            + "\n  - ".join(offenders)
        )


def test_the_door_naming_check_can_still_fail() -> None:
    """Non-vacuity guard for the check above.

    While a superseded door existed, `allowed` and the bring-up set were
    different sets and the check had teeth. The moment the last superseded
    door became an alias, admitting aliases into `allowed` would have made
    the two sets equal — and a check whose allow-list equals its candidate
    set can never fail. This seeds the offender the real check looks for.
    """
    allowed = _documented_doors()
    bring_up = _bring_up_verbs()
    assert bring_up - allowed, (
        "every bring-up verb is allowed, so no document can offend: the "
        "door-naming check has become unfailable"
    )
    seeded_verb = sorted(bring_up - allowed)[0]
    seeded_doc = f"```bash\nokg {seeded_verb} --profile workspace-graph\n```"
    offending = {
        _top_verb(command)
        for command in _iter_okg_commands(seeded_doc, is_markdown=True)
    } & bring_up - allowed
    assert offending == {seeded_verb}, (
        f"a document naming {seeded_verb!r} would not be reported"
    )


def test_documented_okg_commands_parse() -> None:
    """Every documented `okg ...` invocation parses against the live tree.

    This is the assertion that catches ``okg install --profile cms``: a
    command can name a real verb and still be unrunnable because the verb
    does not accept the flag the doc passes it.
    """
    parser = _build_parser()
    failures: list[str] = []
    for path in _existing_docs():
        rel = path.relative_to(REPO_ROOT)
        for command in _iter_okg_commands(
            path.read_text(errors="replace"),
            is_markdown=path.suffix == ".md",
        ):
            if _PLACEHOLDER.search(command):
                continue
            try:
                argv = shlex.split(command)
            except ValueError:
                continue
            if not argv or argv[0] not in _top_level_names(parser):
                continue
            err = _parse_error(parser, argv)
            if err:
                failures.append(f"{rel}: `okg {command}` -> {err}")
    if failures:
        pytest.fail(
            "documented commands that do not parse against the live CLI:"
            "\n  - " + "\n  - ".join(failures)
        )


def _top_level_names(parser: argparse.ArgumentParser) -> set[str]:
    for action in parser._actions:
        if isinstance(action, argparse._SubParsersAction):
            return set(action.choices)
    return set()


def _parse_error(
    parser: argparse.ArgumentParser, argv: list[str]
) -> str | None:
    """Return argparse's message if `argv` does not parse, else None.

    argparse writes to stderr and raises SystemExit; both are captured so a
    documented command cannot take the test process down with it.
    """
    buf = io.StringIO()
    try:
        with contextlib.redirect_stderr(buf), contextlib.redirect_stdout(buf):
            parser.parse_args(argv)
    except SystemExit as exc:
        if exc.code in (0, None):
            return None  # --help and friends exit 0
        message = buf.getvalue().strip().splitlines()
        return message[-1] if message else f"exit {exc.code}"
    except Exception as exc:  # pragma: no cover - defensive
        return f"{type(exc).__name__}: {exc}"
    return None
