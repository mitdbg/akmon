"""okg commands built from the CLI's own parser. Issue #2575.

Hints, next-step lines and child processes used to spell ``okg`` commands by
hand, so nothing tied them to the parser that would run them: an install
hint named ``okg doctor sources`` (the parser rejects ``sources``, #2560) and
the academic-researcher dry run printed ``--orcid None`` (#2277).

:func:`okg_command` builds a command from a verb path and keyword flags and
checks it against the live argparse tree when it is built. It refuses, with
a :class:`ValueError` naming the problem, an unknown verb or subcommand, a
path that stops at a command group, an option the command does not define, a
value of the wrong shape or outside the option's choices, a missing required
option or a clash in a mutually exclusive group, the wrong number of
positionals, and a credential option (last word ``dsn``, ``password``,
``token`` or ``secret``) whose value is not wrapped in :class:`Masked`.
Options are matched exactly, never by argparse's prefix abbreviation.
``None`` flags are omitted, as is ``False`` for an on/off flag, so an unset
answer never prints as ``None``; ``False`` for a ``--x/--no-x`` flag prints
``--no-x``.

* :meth:`OkgCommand.argv` is the child-process argv: ``running_okg_argv()``
  (this interpreter, ``-P -m okg.substrate.cli.main``) plus the tokens, with
  every masked value in the clear.
* :meth:`OkgCommand.display` is the hint text: ``okg`` plus the shell-quoted
  tokens, with every masked value in its masked form.

``tests/substrate/cli/test_command_reference.py`` also reads every
``okg_command(...)`` call under ``src/`` without running it and checks its
literal verb path and flags against the parser, so a wrong reference fails
the test suite even on a code path no test executes.

Not covered yet (follow-ups on #2575): ``okg pact`` commands, which the CLI
hands to a second parser (#2165) and which :func:`okg_command` therefore
refuses; flags a profile adds only when ``--profile`` is on the command
line (they are refused as unknown); and the display prefix for a wheel
install (#2276). The parser is built once per process, in the mode the
process runs in: inside a packaged install (``active_bootstrap()``) the
install verb lacks the profile installer's own flags, as it does for the
real CLI in that mode.
"""
from __future__ import annotations

import argparse
import functools
import shlex
from collections.abc import Iterable, Mapping
from dataclasses import dataclass

from okg.substrate.cli.child_argv import running_okg_argv

#: The last word of an option whose value is a credential an operator must
#: not see pasted into a hint (``--postgres-dsn``, ``--database-password``,
#: ``--auth-token``). Such an option takes a :class:`Masked` value. Only the
#: last word counts: ``--dsn-env`` and ``--token-env`` name a variable,
#: ``--install-dsn-ref`` a reference, ``--run-key`` an identifier.
CREDENTIAL_LAST_WORDS = frozenset({"dsn", "password", "token", "secret"})


@dataclass(frozen=True)
class Masked:
    """A flag value with a separate form for display.

    ``value`` goes into :meth:`OkgCommand.argv`; ``shown`` goes into
    :meth:`OkgCommand.display` (for a DSN, the DSN with its password
    redacted).
    """

    value: str
    shown: str

    def __repr__(self) -> str:  # never print the real value by accident
        return f"Masked(shown={self.shown!r})"


@functools.lru_cache(maxsize=1)
def _parser_tree() -> argparse.ArgumentParser:
    # Imported here: cli/main.py imports most of the package, and several of
    # the modules that build commands are imported while main.py loads.
    from okg.substrate.cli.main import _build_parser

    return _build_parser()


def _subparsers(parser: argparse.ArgumentParser) -> argparse._SubParsersAction | None:
    for action in parser._actions:
        if isinstance(action, argparse._SubParsersAction):
            return action
    return None


def _resolve(verb_path: tuple[str, ...]) -> argparse.ArgumentParser:
    """Walk ``verb_path`` down the parser tree and return the leaf parser."""
    if not verb_path:
        raise ValueError("okg_command needs a verb path, e.g. 'doctor'")
    if verb_path[0] == "pact":
        raise ValueError(
            "okg_command cannot build `okg pact ...`: those commands are "
            "handed to a second parser before argparse runs"
        )
    parser = _parser_tree()
    walked: list[str] = []
    for word in verb_path:
        sub = _subparsers(parser)
        if sub is None:
            raise ValueError(
                f"`okg {' '.join(walked)}` takes no subcommand; "
                f"`{word}` is not a command"
            )
        if word not in sub.choices:
            where = f"okg {' '.join(walked)}" if walked else "okg"
            raise ValueError(
                f"`{where}` has no subcommand `{word}`; "
                f"choices: {', '.join(sorted(sub.choices))}"
            )
        walked.append(word)
        parser = sub.choices[word]
    if _subparsers(parser) is not None and _subparsers(parser).required:
        raise ValueError(
            f"`okg {' '.join(walked)}` is a command group; name a subcommand: "
            f"{', '.join(sorted(_subparsers(parser).choices))}"
        )
    return parser


def _option_for(
    parser: argparse.ArgumentParser, name: str
) -> tuple[str, argparse.Action]:
    """Return (option string, action) for keyword ``name`` (``foo_bar`` -> ``--foo-bar``).

    A keyword that names a renamed flag's deprecated spelling (decision 310,
    e.g. ``profile=`` on ``deployment ready``) resolves to the NEW spelling
    (``--checks``), so a built command never prints the deprecated name.
    """
    base = name[:-1] if name.endswith("_") else name
    wanted = ("--" + base.replace("_", "-"), "--" + base)
    for action in parser._actions:
        for option in action.option_strings:
            if option in wanted:
                renamed_to = getattr(action, "okg_renamed_to", None)
                if renamed_to:
                    return _current_option(parser, renamed_to)
                return option, action
    raise ValueError(
        f"`{parser.prog}` has no option {wanted[0]}"
    )


def _current_option(
    parser: argparse.ArgumentParser, option: str
) -> tuple[str, argparse.Action]:
    for action in parser._actions:
        if option in action.option_strings:
            return option, action
    raise ValueError(f"`{parser.prog}` has no option {option}")  # pragma: no cover


def _renamed_from(parser: argparse.ArgumentParser, name: str) -> str | None:
    """The new spelling when keyword ``name`` names a deprecated option."""
    base = name[:-1] if name.endswith("_") else name
    wanted = ("--" + base.replace("_", "-"), "--" + base)
    for action in parser._actions:
        renamed_to = getattr(action, "okg_renamed_to", None)
        if renamed_to and any(o in wanted for o in action.option_strings):
            return renamed_to
    return None


def is_credential_option(option: str) -> bool:
    """Whether ``option`` carries a credential and so needs a :class:`Masked` value."""
    return option.lstrip("-").split("-")[-1] in CREDENTIAL_LAST_WORDS


def _check_value(
    prog: str, option: str, action: argparse.Action, value: object
) -> None:
    if isinstance(value, bool):
        raise ValueError(f"`{prog} {option}` takes a value, not {value}")
    # The credential rule runs first, so no later error message can echo an
    # unmasked secret.
    if is_credential_option(option) and not isinstance(value, Masked):
        raise ValueError(
            f"`{prog} {option}` carries a credential; wrap the value in "
            "Masked(value, shown) so a hint never prints it"
        )
    if action.type in (int, float) and not isinstance(value, Masked):
        try:
            action.type(_plain(value))
        except ValueError:
            raise ValueError(
                f"`{prog} {option}` takes a {action.type.__name__}, "
                f"not {value!r}"
            ) from None
    if action.choices is not None:
        plain = value.value if isinstance(value, Masked) else value
        if plain not in action.choices and str(plain) not in {
            str(choice) for choice in action.choices
        }:
            raise ValueError(
                f"`{prog} {option}` does not accept {value!r}; "
                f"choices: {', '.join(str(c) for c in action.choices)}"
            )


def _flag_tokens(
    parser: argparse.ArgumentParser, name: str, value: object
) -> tuple[str, list[tuple[str, object]]]:
    """Validate one keyword flag; return its option and its (token, value) pairs.

    A pair's value is ``None`` for a bare option token.
    """
    option, action = _option_for(parser, name)
    prog = parser.prog
    if option != action.option_strings[0] and isinstance(
        action, argparse.BooleanOptionalAction
    ):
        raise ValueError(
            f"`{prog} {option}`: name the flag as "
            f"{action.option_strings[0]} and pass False"
        )
    if isinstance(
        action, (argparse._HelpAction, argparse._VersionAction)
    ):
        raise ValueError(f"`{prog} {option}` is not a command flag")
    if isinstance(action, argparse.BooleanOptionalAction):
        if not isinstance(value, bool):
            raise ValueError(f"`{prog} {option}` is on/off; pass True or False")
        long = next(o for o in action.option_strings if o.startswith("--"))
        token = long if value else "--no-" + long[2:]
        return option, [(token, None)]
    if action.nargs == 0:
        if value is True:
            return option, [(option, None)]
        if value is False:
            return option, []
        raise ValueError(
            f"`{prog} {option}` is an on/off flag; pass True or False, "
            f"not {value!r}"
        )
    if value is True and action.nargs == "?":
        return option, [(option, None)]  # the option's `const`
    many = isinstance(value, (list, tuple))
    if many:
        if not isinstance(action, argparse._AppendAction) and action.nargs not in (
            "*", "+",
        ):
            raise ValueError(f"`{prog} {option}` takes one value, not a list")
        if not value:
            return option, []
        for item in value:
            _check_value(prog, option, action, item)
        if isinstance(action, argparse._AppendAction):
            return option, [(option, item) for item in value]
        if action.nargs in ("*", "+"):
            for item in value:
                if _plain(item).startswith("-"):
                    raise ValueError(
                        f"`{prog} {option}` value {item!r} starts with '-'"
                    )
            return option, [(option, None), *((None, item) for item in value)]
    _check_value(prog, option, action, value)
    return option, [(option, value)]


def _plain(value: object) -> str:
    return str(value.value if isinstance(value, Masked) else value)


def _check_exclusive_groups(
    parser: argparse.ArgumentParser, given: set[str]
) -> None:
    for group in parser._mutually_exclusive_groups:
        options = [
            option
            for action in group._group_actions
            for option in action.option_strings
            if option in given
        ]
        if len(options) > 1:
            raise ValueError(
                f"`{parser.prog}` options {' and '.join(options)} "
                "cannot be used together"
            )
        if group.required and not options:
            names = [
                action.option_strings[0] if action.option_strings else action.dest
                for action in group._group_actions
                if not getattr(action, "okg_renamed_to", None)
            ]
            raise ValueError(
                f"`{parser.prog}` requires one of {', '.join(names)}"
            )


def _refuse_remainder(parser: argparse.ArgumentParser) -> None:
    """A positional that takes everything after it would swallow the options
    this builder writes after it, so such commands are not built yet."""
    for action in parser._actions:
        if not action.option_strings and action.nargs == argparse.REMAINDER:
            raise ValueError(
                f"`{parser.prog}` ends in a pass-through argument "
                f"({action.dest}); okg_command cannot build it yet"
            )


def _positional_bounds(parser: argparse.ArgumentParser) -> tuple[int, int | None]:
    low, high = 0, 0
    for action in parser._actions:
        if action.option_strings or isinstance(action, argparse._SubParsersAction):
            continue
        nargs = action.nargs
        if nargs is None:
            low += 1
            high = None if high is None else high + 1
        elif nargs == "?":
            high = None if high is None else high + 1
        elif nargs == "*" or nargs == argparse.REMAINDER:
            high = None
        elif nargs == "+":
            low += 1
            high = None
        elif isinstance(nargs, int):
            low += nargs
            high = None if high is None else high + nargs
    return low, high


def _check_positional_count(parser: argparse.ArgumentParser, count: int) -> None:
    low, high = _positional_bounds(parser)
    if count < low or (high is not None and count > high):
        want = (
            str(low) if high == low
            else f"{low} to {high if high is not None else 'any number of'}"
        )
        raise ValueError(
            f"`{parser.prog}` takes {want} positional argument(s), got {count}"
        )


def _check_positional_choices(
    parser: argparse.ArgumentParser, positionals: tuple[object, ...]
) -> None:
    """Check each positional against its action's choices, in order.

    Stops at the first positional that takes a variable number of values,
    where the pairing of values to actions is no longer one to one.
    """
    actions = [
        action for action in parser._actions
        if not action.option_strings
        and not isinstance(action, argparse._SubParsersAction)
    ]
    for action, item in zip(actions, positionals):
        if action.nargs not in (None, "?"):
            return
        plain = item.value if isinstance(item, Masked) else item
        if action.choices is not None and str(plain) not in {
            str(choice) for choice in action.choices
        }:
            raise ValueError(
                f"`{parser.prog}` {action.dest} does not accept {item!r}; "
                f"choices: {', '.join(str(c) for c in action.choices)}"
            )


@dataclass(frozen=True)
class OkgCommand:
    """A validated okg command. Build it with :func:`okg_command`."""

    verb_path: tuple[str, ...]
    #: (token, value) pairs; a ``None`` token is a bare value, a ``None``
    #: value is a bare option.
    parts: tuple[tuple[str | None, object], ...]

    def _tokens(self, *, masked: bool) -> list[str]:
        tokens = list(self.verb_path)
        for token, value in self.parts:
            if token is not None:
                tokens.append(token)
            if value is None:
                continue
            text = (
                value.shown if masked and isinstance(value, Masked)
                else _plain(value)
            )
            if token is not None and text.startswith("-"):
                # `--name -x` would read -x as an option; `--name=-x` cannot.
                tokens[-1] = f"{token}={text}"
            else:
                tokens.append(text)
        return tokens

    def argv(self) -> list[str]:
        """Child-process argv that starts THIS okg, masked values in the clear."""
        return [*running_okg_argv(), *self._tokens(masked=False)]

    def display(self, prefix: str = "okg") -> str:
        """Shell-quoted hint text; masked values show their masked form."""
        return " ".join([prefix, *(shlex.quote(t) for t in self._tokens(masked=True))])

    def __str__(self) -> str:
        return self.display()


def okg_command(
    verb_path: str, *positionals: object, **flags: object
) -> OkgCommand:
    """Build an okg command, checked against the live parser.

    ``verb_path`` is the space-separated command path (``"doctor"``,
    ``"deployment apply"``). Each keyword is a long option: ``foo_bar=x``
    is ``--foo-bar x``; a trailing underscore escapes a Python keyword
    (``from_=``). ``True`` sets an on/off flag, ``None`` and ``False`` leave
    one out, a list repeats an append option or fills a multi-value one.
    Positionals follow the options in the order given.
    """
    path = tuple(verb_path.split())
    parser = _resolve(path)
    _refuse_remainder(parser)
    _check_positional_count(parser, len(positionals))
    _check_positional_choices(parser, positionals)
    for item in positionals:
        # A leading `-` would be read as an option; no caller needs `--`
        # yet, so refuse rather than guess.
        if _plain(item).startswith("-"):
            raise ValueError(f"`{parser.prog}` positional {item!r} starts with '-'")
    # Positionals go first: after a multi-value option argparse would read
    # them as more values of that option.
    parts: list[tuple[str | None, object]] = [(None, item) for item in positionals]
    given: set[str] = set()
    keyword_for: dict[str, str] = {}
    for name, value in flags.items():
        # A deprecated keyword and its new name resolve to one option
        # (decision 310); passing both would emit the flag twice.
        option, _ = _option_for(parser, name)
        if option in keyword_for:
            raise ValueError(
                f"`{parser.prog}` keywords {keyword_for[option]!r} and "
                f"{name!r} both name {option}; pass only one"
            )
        keyword_for[option] = name
        if value is None:
            continue
        option, pairs = _flag_tokens(parser, name, value)
        if pairs:
            given.add(option)
        parts.extend(pairs)
    _check_exclusive_groups(parser, given)
    for action in parser._actions:
        if action.required and action.option_strings and not (
            given & set(action.option_strings)
        ):
            raise ValueError(
                f"`{parser.prog}` requires {'/'.join(action.option_strings)}"
            )
    return OkgCommand(verb_path=path, parts=tuple(parts))


def check_reference(
    verb_path: str,
    flag_names: Iterable[str],
    literal_flags: Mapping[str, object],
    positional_count: int | None = None,
) -> None:
    """Static form of :func:`okg_command` for the source scan.

    Checks the verb path, that every flag name is an option of the leaf
    command, the literal values in ``literal_flags`` and, when the call's
    positionals can be counted, their number. Values the scan cannot read
    (variables) are checked by :func:`okg_command` when the call runs.
    """
    parser = _resolve(tuple(verb_path.split()))
    _refuse_remainder(parser)
    for name in flag_names:
        renamed_to = _renamed_from(parser, name)
        if renamed_to is not None:
            raise ValueError(
                f"`{parser.prog}` keyword {name!r} names a deprecated "
                f"spelling; use {renamed_to.lstrip('-').replace('-', '_')}= "
                f"({renamed_to})"
            )
        _option_for(parser, name)
        value = literal_flags.get(name)
        if value is not None:
            _flag_tokens(parser, name, value)
    if positional_count is not None:
        _check_positional_count(parser, positional_count)
