"""Coverage gate for the substrate CLI surface.

Per `audit-test-and-document-cli-surface` Phase 2: every verb in the
substrate's argparse tree SHALL have an entry in
[CLI_SMOKE_INDEX.yaml](CLI_SMOKE_INDEX.yaml) mapping it to a test
file. Adding a verb without updating the manifest fails these tests.

Three assertions:
  1. Every argparse verb / sub-verb has a manifest entry.
  2. Every manifest entry's `test_file:` points at a file that exists.
  3. Each referenced test file either parametrizes over the manifest
     (i.e. references `CLI_SMOKE_INDEX.yaml`) OR contains the verb /
     sub-verb name as a string literal — so renaming a verb without
     touching the dedicated test surfaces.

Per PACT `define-newcomer-entry-path` (req.entry.role-in-existing-registry)
the manifest additionally carries a `role:` on every TOP-LEVEL verb, and
this module gates it:

  4. Every top-level verb is classified — an unclassified verb fails
     rather than defaulting to any role.
  5. Exactly one verb is the `newcomer_bring_up` door.
  6. `alias` names an `alias_of:` that is that door; `frozen` names a
     `replaced_by:` that exists.
  7. `superseded_entry` is a ratchet: the count may fall as doors are
     migrated, never rise.

These live here rather than in a new file because the PACT requires
extending the registry that already gates this surface, not minting a
fourth parallel artifact.
"""
from __future__ import annotations

import argparse
import re
from pathlib import Path

import pytest
import yaml

from tests.mirror_scope import is_private, is_public_copy

from okg.substrate.cli.main import _build_parser


REPO_ROOT = Path(__file__).resolve().parents[3]
MANIFEST_PATH = (
    REPO_ROOT / "tests" / "substrate" / "cli" / "CLI_SMOKE_INDEX.yaml"
)


def _walk_argparse_verbs(parser: argparse.ArgumentParser) -> list[str]:
    """Return every verb name (two levels deep) in the parser tree.

    Names look like "doctor" or "db status" or "runtime worker".
    """
    out: list[str] = []
    for action in parser._actions:
        if not isinstance(action, argparse._SubParsersAction):
            continue
        for name, sub_parser in action.choices.items():
            out.append(name)
            for inner in sub_parser._actions:
                if not isinstance(inner, argparse._SubParsersAction):
                    continue
                for sub_name in inner.choices:
                    out.append(f"{name} {sub_name}")
    return out


def _load_manifest() -> dict[str, dict[str, str]]:
    data = yaml.safe_load(MANIFEST_PATH.read_text())
    return data.get("verbs", {}) or {}


# ── entry-surface roles (PACT define-newcomer-entry-path) ──────────────
VALID_ROLES = frozenset({
    "newcomer_bring_up",
    "superseded_entry",
    "alias",
    "frozen",
    "lifecycle",
    "not_entry",
})

# The migration ratchet. `req.entry.migrate-the-other-doors` drove this to
# zero: the one superseded door, `init`, is now an `alias` of `install`.
# It may fall; a rise means a new front door appeared, which is the
# regression this whole PACT exists to make impossible.
MAX_SUPERSEDED_ENTRIES = 0


def _top_level_verbs(parser: argparse.ArgumentParser) -> list[str]:
    """Top-level verb names only — roles are a top-level property."""
    for action in parser._actions:
        if isinstance(action, argparse._SubParsersAction):
            return list(action.choices)
    return []


def _roles() -> dict[str, str]:
    """Map top-level verb -> declared role (absent verbs omitted)."""
    out: dict[str, str] = {}
    for verb, entry in _load_manifest().items():
        if " " in verb or not isinstance(entry, dict):
            continue
        role = entry.get("role")
        if role is not None:
            out[verb] = role
    return out


def test_every_top_level_verb_declares_a_role() -> None:
    """An unclassified top-level verb fails — never defaults.

    Silently defaulting is how the entry surface grew several front
    doors with nothing observing it.
    """
    argparse_top = set(_top_level_verbs(_build_parser()))
    declared = _roles()
    missing = sorted(argparse_top - set(declared))
    if missing:
        pytest.fail(
            "top-level verbs with no `role:` in the manifest: "
            f"{missing}\n"
            f"classify each explicitly as one of {sorted(VALID_ROLES)}; "
            "a verb must not default into a role.\n"
            f"manifest: {MANIFEST_PATH.relative_to(REPO_ROOT)}"
        )


def test_declared_roles_are_valid() -> None:
    """A typo'd or invented role value fails loudly."""
    bad = {
        verb: role
        for verb, role in _roles().items()
        if role not in VALID_ROLES
    }
    if bad:
        pytest.fail(
            f"invalid role values: {bad}\n"
            f"valid roles: {sorted(VALID_ROLES)}"
        )


def test_exactly_one_newcomer_bring_up_door() -> None:
    """There is one documented way in, and it is declared."""
    doors = sorted(
        verb for verb, role in _roles().items()
        if role == "newcomer_bring_up"
    )
    if len(doors) != 1:
        pytest.fail(
            f"expected exactly one `newcomer_bring_up` verb, found "
            f"{len(doors)}: {doors}\n"
            "a second door must instead be `alias` (delegating to the "
            "documented one) or `frozen` (naming its replacement)."
        )


def test_alias_and_frozen_entries_name_their_target() -> None:
    """`alias` delegates somewhere real; `frozen` names a replacement."""
    manifest = _load_manifest()
    roles = _roles()
    doors = {v for v, r in roles.items() if r == "newcomer_bring_up"}
    bad: list[str] = []
    for verb, role in sorted(roles.items()):
        entry = manifest[verb]
        if role == "alias":
            target = entry.get("alias_of")
            if not target:
                bad.append(f"{verb!r}: role `alias` with no `alias_of:`")
            elif target not in doors:
                bad.append(
                    f"{verb!r}: alias_of={target!r} is not the "
                    f"newcomer door {sorted(doors)}"
                )
        elif role == "frozen":
            replacement = entry.get("replaced_by")
            if not replacement:
                bad.append(
                    f"{verb!r}: role `frozen` with no `replaced_by:`"
                )
            elif replacement not in roles:
                bad.append(
                    f"{verb!r}: replaced_by={replacement!r} is not a verb"
                )
    if bad:
        pytest.fail(
            "alias/frozen entries missing or misnaming their target:"
            "\n  - " + "\n  - ".join(bad)
        )


def test_source_role_map_and_manifest_agree() -> None:
    """The runtime map and the gated mirror must not drift.

    `entry_roles.ENTRY_ROLES` is canonical because the CLI needs roles at
    runtime, where `tests/` does not ship. This manifest is the gated
    mirror. Either one changing alone is the drift that would let the
    help say one thing while the gate enforces another.
    """
    from okg.substrate.cli import entry_roles

    manifest_roles = _roles()
    disagree: list[str] = []
    for verb, manifest_role in sorted(manifest_roles.items()):
        source_role = entry_roles.role_of(verb)
        if source_role != manifest_role:
            disagree.append(
                f"{verb!r}: manifest={manifest_role!r} "
                f"source={source_role!r}"
            )
    if disagree:
        pytest.fail(
            "CLI_SMOKE_INDEX.yaml and "
            "src/okg/substrate/cli/entry_roles.py disagree:\n  - "
            + "\n  - ".join(disagree)
            + "\nupdate ENTRY_ROLES and the manifest together."
        )


def test_source_role_map_names_exactly_one_door() -> None:
    """`newcomer_door()` raises rather than guessing when ambiguous."""
    from okg.substrate.cli import entry_roles

    assert entry_roles.newcomer_door() in _top_level_verbs(_build_parser())


def test_help_leads_with_the_newcomer_door() -> None:
    """`okg --help` puts the door first and marks it.

    The point of the role field is that a newcomer scanning 58 verbs can
    find the one that starts the system; that only holds if the help
    actually reflects it.
    """
    from okg.substrate.cli import entry_roles

    parser = _build_parser()
    door = entry_roles.newcomer_door()
    for action in parser._actions:
        if not isinstance(action, argparse._SubParsersAction):
            continue
        listed = [a.dest for a in getattr(action, "_choices_actions", [])]
        assert listed, "no verbs listed in help"
        assert listed[0] == door, (
            f"help lists {listed[0]!r} first; expected the newcomer door "
            f"{door!r}"
        )
        marker = {
            a.dest: (a.help or "")
            for a in getattr(action, "_choices_actions", [])
        }
        assert marker[door].startswith("[START HERE]"), (
            f"the door {door!r} is not marked in help: {marker[door]!r}"
        )
        # Retired doors sink below ordinary verbs.
        ranks = {
            v: entry_roles.display_rank(v)
            for v in listed
        }
        superseded = [v for v in listed if ranks[v] == entry_roles
                      .ROLE_DISPLAY_ORDER.index("superseded_entry")]
        if superseded:
            assert listed[-len(superseded):] == superseded, (
                "superseded doors are not listed last"
            )
        return
    pytest.fail("no subparsers action found")


def test_help_ordering_does_not_change_dispatch() -> None:
    """Reordering help must never change what a command resolves to.

    Guards the one real risk in sorting `_choices_actions`: argparse
    dispatch reads `action.choices`, and if the sort ever touched that
    mapping a verb could silently resolve to the wrong parser.
    """
    parser = _build_parser()
    for action in parser._actions:
        if not isinstance(action, argparse._SubParsersAction):
            continue
        for name, sub_parser in action.choices.items():
            assert sub_parser.prog.endswith(name), (
                f"verb {name!r} resolves to parser {sub_parser.prog!r}"
            )
        return


def test_superseded_entry_count_does_not_rise() -> None:
    """The migration ratchet: doors may be retired, never added."""
    superseded = sorted(
        verb for verb, role in _roles().items()
        if role == "superseded_entry"
    )
    if len(superseded) > MAX_SUPERSEDED_ENTRIES:
        pytest.fail(
            f"{len(superseded)} `superseded_entry` verbs "
            f"({superseded}) exceeds the ratchet of "
            f"{MAX_SUPERSEDED_ENTRIES}.\n"
            "A new bring-up door was added. Make it an `alias` of the "
            "documented door, or `frozen` with a replacement."
        )


def test_manifest_yaml_parses() -> None:
    """CLI_SMOKE_INDEX.yaml is well-formed and non-empty."""
    verbs = _load_manifest()
    assert isinstance(verbs, dict)
    assert len(verbs) > 0, "manifest is empty"


def test_every_argparse_verb_has_manifest_entry() -> None:
    """Adding an argparse verb without a manifest entry fails here.

    Also fails if a manifest entry references a verb argparse no
    longer ships (i.e. someone deleted a verb without removing the
    manifest row).
    """
    parser = _build_parser()
    argparse_verbs = set(_walk_argparse_verbs(parser))
    manifest_verbs = set(_load_manifest().keys())
    missing = argparse_verbs - manifest_verbs
    extra = manifest_verbs - argparse_verbs
    parts: list[str] = []
    if missing:
        parts.append(
            "argparse verbs not in manifest "
            f"(add rows): {sorted(missing)}"
        )
    if extra:
        parts.append(
            "manifest verbs not in argparse "
            f"(remove rows): {sorted(extra)}"
        )
    if parts:
        rel = MANIFEST_PATH.relative_to(REPO_ROOT)
        parts.append(f"manifest: {rel}")
        pytest.fail("\n".join(parts))


def test_every_manifest_entry_names_an_existing_test_file() -> None:
    """`test_file:` must resolve to a real file under the repo."""
    bad: list[str] = []
    for verb, entry in _load_manifest().items():
        path_str = entry.get("test_file") if isinstance(entry, dict) else None
        if not path_str:
            bad.append(f"{verb!r}: missing `test_file:`")
            continue
        target = REPO_ROOT / path_str
        if is_public_copy() and is_private(path_str):
            # The verb's test is this repository's own (tests/mirror_scope.py).
            continue
        if not target.is_file():
            bad.append(
                f"{verb!r}: test_file {path_str!r} does not exist"
            )
    if bad:
        pytest.fail(
            "manifest references invalid test files:\n  - "
            + "\n  - ".join(bad)
        )


def test_every_manifest_test_file_references_the_verb() -> None:
    """A test file pointed-at by a verb must actually mention it.

    A file referencing `CLI_SMOKE_INDEX.yaml` (i.e. parametrized
    over the manifest itself) is exempt — the parametrize pulls
    the verb in via the manifest key.

    Otherwise the test file's source SHALL contain the verb's name
    (top-level for non-nested verbs; the sub-name for nested
    verbs) as a quoted string OR as `okg <verb>` somewhere in the
    file. This guards against silent stale references after a
    rename.
    """
    bad: list[str] = []
    for verb, entry in _load_manifest().items():
        path_str = entry["test_file"]
        if is_public_copy() and is_private(path_str):
            continue
        target = REPO_ROOT / path_str
        text = target.read_text()
        if "CLI_SMOKE_INDEX.yaml" in text:
            continue
        top, _, sub = verb.partition(" ")
        anchor = sub or top
        # Non-alphanumeric boundary so `_cmd_doctor`, `doctor_checks`,
        # `"doctor"`, `okg doctor`, `_cmd_seed_from_subtype` all
        # match while `address` (for verb `add`) does not.
        boundary = rf"(?<![A-Za-z0-9]){re.escape(anchor)}(?![A-Za-z0-9])"
        if not re.search(boundary, text):
            bad.append(
                f"{verb!r}: {path_str} contains no occurrence of "
                f"{anchor!r}"
            )
    if bad:
        pytest.fail(
            "manifest verbs missing from their referenced test "
            "files:\n  - " + "\n  - ".join(bad)
        )


# ── default-help visibility (okg#3109 slice 0, decision 1b) ─────────────
#: The exact verbs decision 1b hides. Pinned here so a ninth hidden verb is
#: a deliberate test edit, not a quiet registry change.
EXPECTED_HIDDEN = frozenset({
    "agent-ssh-run",
    "agent-ssh-client",
    "agent-runtime-launch",
    "dogfood",
    "pact",
    "bench",
    "environment",
    "external-action",
})

#: Retired verbs (issue #2479): out of the listing and out of the epilog.
EXPECTED_RETIRED = frozenset({"new-source"})

#: The top-level verbs the federation and Poros modules register. Issue
#: #3416: federation is not supported in the public release, so every one of
#: them is unlisted.
EXPECTED_FEDERATION = frozenset({
    "connect",
    "send",
    "pull",
    "inbox",
    "proof",
    "federation",
    "poros",
})

#: Unlisted verbs (issue #3416): out of the listing, the usage choice list
#: and the epilog, yet they parse and run exactly as before.
EXPECTED_UNLISTED = EXPECTED_FEDERATION


def _top_subparsers(
    parser: argparse.ArgumentParser,
) -> argparse._SubParsersAction:
    for action in parser._actions:
        if isinstance(action, argparse._SubParsersAction):
            return action
    pytest.fail("no subparsers action found")


def _manifest_visibility() -> dict[str, str]:
    return {
        verb: entry["visibility"]
        for verb, entry in _load_manifest().items()
        if isinstance(entry, dict) and "visibility" in entry
    }


def test_visibility_values_are_valid_and_top_level_only() -> None:
    from okg.substrate.cli import entry_roles

    bad = [
        f"{verb!r}: visibility={value!r}"
        for verb, value in sorted(_manifest_visibility().items())
        if value not in entry_roles.VALID_VISIBILITIES or " " in verb
    ]
    assert not bad, (
        "manifest `visibility:` must be `shown`, `hidden`, `retired` or "
        "`unlisted` and "
        "only on "
        "top-level verbs:\n  - " + "\n  - ".join(bad)
    )


def test_source_visibility_map_and_manifest_agree() -> None:
    """`entry_roles.HIDDEN_FROM_DEFAULT_HELP` and the manifest mirror match."""
    from okg.substrate.cli import entry_roles

    manifest = {
        v for v, value in _manifest_visibility().items() if value == "hidden"
    }
    assert manifest == set(entry_roles.HIDDEN_FROM_DEFAULT_HELP), (
        f"manifest hidden={sorted(manifest)} "
        f"source hidden={sorted(entry_roles.HIDDEN_FROM_DEFAULT_HELP)}"
    )
    assert set(entry_roles.HIDDEN_FROM_DEFAULT_HELP) == EXPECTED_HIDDEN
    retired = {
        v for v, value in _manifest_visibility().items() if value == "retired"
    }
    assert retired == set(entry_roles.RETIRED_VERBS) == EXPECTED_RETIRED
    unlisted = {
        v for v, value in _manifest_visibility().items() if value == "unlisted"
    }
    assert unlisted == set(entry_roles.UNLISTED_VERBS) == EXPECTED_UNLISTED


def test_every_federation_verb_is_unlisted() -> None:
    """A new federation or Poros verb cannot reach public help by accident.

    The registrars run against a bare parser, so whatever top-level verb
    they add is caught here, whatever its name.
    """
    from okg.substrate.cli import entry_roles
    from okg.substrate.cli import federation as federation_cli
    from okg.substrate.cli import poros as poros_cli

    parser = argparse.ArgumentParser(prog="okg")
    sub = parser.add_subparsers(dest="command")

    def add_json_flag(p: argparse.ArgumentParser) -> None:
        p.add_argument("--json", action="store_true")

    federation_cli.register_subparsers(sub, add_json_flag=add_json_flag)
    poros_cli.register_subparsers(sub, add_json_flag=add_json_flag)
    registered = set(sub.choices)
    assert registered == EXPECTED_FEDERATION, (
        "federation/Poros registered a verb this test does not pin: "
        f"{sorted(registered ^ EXPECTED_FEDERATION)}; unlist it in "
        "entry_roles.UNLISTED_VERBS and pin it here"
    )
    not_unlisted = sorted(
        v for v in registered
        if entry_roles.visibility_of(v) != entry_roles.UNLISTED
    )
    assert not not_unlisted, not_unlisted
    for verb in registered:
        assert entry_roles.UNLISTED_VERBS[verb].strip(), verb


def test_default_help_omits_exactly_the_hidden_verbs() -> None:
    """Default `okg --help` drops the eight internal verbs and only them.

    Renamed verbs (decision 310, `entry_roles.RENAMED_VERBS`) also leave
    the listing, but are not internal: the epilog does not name them.
    """
    from okg.substrate.cli import entry_roles

    parser = _build_parser()
    sub = _top_subparsers(parser)
    every = list(sub.choices)
    listed = [a.dest for a in sub._choices_actions]
    renamed = set(entry_roles.RENAMED_VERBS)
    assert EXPECTED_HIDDEN <= set(every), (
        "hidden verbs that are not real verbs: "
        f"{sorted(EXPECTED_HIDDEN - set(every))}"
    )
    assert renamed <= set(every)
    assert EXPECTED_UNLISTED <= set(every), (
        "unlisted verbs that are not real verbs: "
        f"{sorted(EXPECTED_UNLISTED - set(every))}"
    )
    dropped = EXPECTED_HIDDEN | EXPECTED_RETIRED | EXPECTED_UNLISTED | renamed
    assert set(listed) == set(every) - dropped
    assert len(listed) == len(every) - len(dropped)

    text = parser.format_help()
    usage_braces = re.search(r"\{([^}]*)\}", text)
    assert usage_braces, text[:400]
    assert sorted(usage_braces.group(1).split(",")) == sorted(listed)
    listing = [
        line.split()[0] for line in text.splitlines()
        if re.match(r"^    \S", line)
    ]
    assert not dropped & set(listing), listing
    # One epilog line keeps them discoverable.
    for verb in EXPECTED_HIDDEN:
        assert verb in (parser.epilog or ""), (verb, parser.epilog)
    # A retired verb only refuses, so the epilog does not advertise it.
    for verb in EXPECTED_RETIRED:
        assert verb not in (parser.epilog or ""), (verb, parser.epilog)
    # An unlisted verb is unsupported in this release: the note naming the
    # internal verbs lists exactly the hidden ones, none of the unlisted.
    note = re.search(r"not\s+listed\s+above:\s+(.*?)\.\s", parser.epilog or "", re.S)
    assert note, parser.epilog
    named = {v.strip() for v in re.split(r",\s*", note.group(1))}
    assert named == set(EXPECTED_HIDDEN), named
    assert not named & EXPECTED_UNLISTED, named


@pytest.mark.parametrize("verb", sorted(EXPECTED_HIDDEN | EXPECTED_UNLISTED))
def test_hidden_verb_still_parses_and_answers_help(verb, capsys) -> None:
    parser = _build_parser()
    assert verb in _top_subparsers(parser).choices
    with pytest.raises(SystemExit) as exc:
        parser.parse_args([verb, "--help"])
    assert exc.value.code == 0
    assert f"okg {verb}" in capsys.readouterr().out


def test_unknown_verb_error_still_names_the_command_argument(capsys) -> None:
    """Hiding must not rename the argument in argparse's error text.

    Pinning `sub.metavar` would make every typo read
    `argument {migrate,doctor,...}: invalid choice`; the help-only formatter
    keeps `argument command:`.
    """
    parser = _build_parser()
    with pytest.raises(SystemExit) as exc:
        parser.parse_args(["no-such-verb"])
    assert exc.value.code == 2
    err = capsys.readouterr().err
    assert "argument command: invalid choice: 'no-such-verb'" in err, err
    usage = parser.format_usage()
    for verb in EXPECTED_HIDDEN | EXPECTED_RETIRED | EXPECTED_UNLISTED:
        assert re.search(rf"[{{,]{re.escape(verb)}[,}}]", usage) is None, verb


def test_hiding_filters_help_but_not_dispatch(monkeypatch, capsys) -> None:
    """Failure case: a hidden verb vanishes from help yet still dispatches.

    A toy parser keeps the check independent of the real verb set, and the
    second half shows an unhidden parser keeps argparse's own rendering and
    gets no epilog.
    """
    from okg.substrate.cli import entry_roles
    from okg.substrate.cli.main import _apply_entry_roles_to_help

    def toy() -> tuple[argparse.ArgumentParser, argparse._SubParsersAction]:
        p = argparse.ArgumentParser(prog="okg")
        s = p.add_subparsers(dest="command")
        s.add_parser("alpha", help="a")
        s.add_parser("secret", help="s")
        return p, s

    monkeypatch.setattr(
        entry_roles, "HIDDEN_FROM_DEFAULT_HELP", frozenset({"secret"}),
    )
    p, s = toy()
    _apply_entry_roles_to_help(s, p)
    assert [a.dest for a in s._choices_actions] == ["alpha"]
    assert s.metavar is None
    assert "{alpha}" in p.format_usage()
    assert "secret" not in p.format_usage()
    assert not re.search(r"^    secret\b", p.format_help(), re.MULTILINE)
    assert "secret" in p.epilog
    assert p.parse_args(["secret"]).command == "secret"
    with pytest.raises(SystemExit):
        p.parse_args(["nope"])
    assert "argument command: invalid choice" in capsys.readouterr().err

    monkeypatch.setattr(entry_roles, "HIDDEN_FROM_DEFAULT_HELP", frozenset())
    p, s = toy()
    _apply_entry_roles_to_help(s, p)
    assert [a.dest for a in s._choices_actions] == ["alpha", "secret"]
    assert "{alpha,secret}" in p.format_usage()
    assert p.epilog is None


def test_unlisting_hides_without_naming_and_keeps_dispatch(
    monkeypatch, capsys,
) -> None:
    """Failure case: an unlisted verb leaves the listing, the usage choice
    list and the epilog, yet still parses and answers its own `--help`.

    Then the same verb with an empty unlisted map is listed again, so the
    check proves the map drives the hiding.
    """
    from okg.substrate.cli import entry_roles
    from okg.substrate.cli.main import _apply_entry_roles_to_help

    def toy() -> tuple[argparse.ArgumentParser, argparse._SubParsersAction]:
        p = argparse.ArgumentParser(prog="okg")
        s = p.add_subparsers(dest="command")
        s.add_parser("alpha", help="a")
        s.add_parser("internal", help="i")
        s.add_parser("exchange", help="x")
        return p, s

    monkeypatch.setattr(
        entry_roles, "HIDDEN_FROM_DEFAULT_HELP", frozenset({"internal"}),
    )
    monkeypatch.setattr(
        entry_roles, "UNLISTED_VERBS", {"exchange": "not supported"},
    )
    p, s = toy()
    _apply_entry_roles_to_help(s, p)
    assert [a.dest for a in s._choices_actions] == ["alpha"]
    assert "{alpha}" in p.format_usage()
    help_text = p.format_help()
    assert "exchange" not in help_text, help_text
    assert "internal" in (p.epilog or "")
    assert p.parse_args(["exchange"]).command == "exchange"
    with pytest.raises(SystemExit) as exc:
        p.parse_args(["exchange", "--help"])
    assert exc.value.code == 0
    assert "okg exchange" in capsys.readouterr().out

    # Only unlisted verbs dropped: no epilog line is written for them.
    monkeypatch.setattr(entry_roles, "HIDDEN_FROM_DEFAULT_HELP", frozenset())
    p, s = toy()
    _apply_entry_roles_to_help(s, p)
    assert [a.dest for a in s._choices_actions] == ["alpha", "internal"]
    assert p.epilog is None
    assert "exchange" not in p.format_help()

    monkeypatch.setattr(entry_roles, "UNLISTED_VERBS", {})
    p, s = toy()
    _apply_entry_roles_to_help(s, p)
    assert [a.dest for a in s._choices_actions] == [
        "alpha", "internal", "exchange",
    ]


def test_did_you_mean_skips_unlisted_verbs(capsys) -> None:
    """A typo near an unlisted verb gets no suggestion naming it.

    Failure case: `conect` used to answer "did you mean `connect`?". A
    subcommand of the same name in another group (`okg poros send`) is a
    different command and is still suggested there.
    """
    parser = _build_parser()
    with pytest.raises(SystemExit) as exc:
        parser.parse_args(["conect"])
    assert exc.value.code == 2
    err = capsys.readouterr().err
    assert "invalid choice: 'conect'" in err, err
    assert "did you mean `connect`" not in err, err
    for verb in EXPECTED_UNLISTED:
        assert f"did you mean `{verb}`" not in err, (verb, err)

    with pytest.raises(SystemExit):
        parser.parse_args(["statu"])
    assert "did you mean `status`" in capsys.readouterr().err

    with pytest.raises(SystemExit):
        parser.parse_args(["poros", "sen"])
    assert "did you mean `send`" in capsys.readouterr().err


def test_invalid_choice_lists_only_public_verbs(capsys) -> None:
    """The usage line hid internal verbs, but argparse's own
    `(choose from ...)` list named every one of them, twice."""
    import argparse

    parser = _build_parser()
    hidden = next(
        action._okg_hidden_from_help for action in parser._actions
        if isinstance(action, argparse._SubParsersAction)
    )
    assert hidden, "fixture: the top-level command list hides some verbs"
    with pytest.raises(SystemExit) as exc:
        parser.parse_args(["runtime worker"])
    assert exc.value.code == 2
    err = capsys.readouterr().err
    lists = re.findall(r"\(choose from ([^)]*)\)", err)
    assert len(lists) == 2, err
    for listed in lists:
        named = {token.strip().strip("'") for token in listed.split(",")}
        assert "status" in named
        assert not named & hidden, sorted(named & hidden)

    # A hidden verb the help epilog names is still a suggestion; an unlisted
    # one is named nowhere.
    with pytest.raises(SystemExit):
        parser.parse_args(["pac"])
    assert "did you mean `pact`" in capsys.readouterr().err
    with pytest.raises(SystemExit):
        parser.parse_args(["federatio"])
    assert "federation" not in capsys.readouterr().err
