"""`okg install` is substrate-owned and routes to installable profiles.

Per PACT `define-newcomer-entry-path`
(task.entry.promote-install-to-substrate).

`install` is the one documented way in, so the substrate has to own the
verb. But `retire-pi-prefix-commands` deliberately removed every
academic-researcher reference from the substrate dispatcher, and the obvious
implementation — import the profile and call it — would put that
reference straight back. So profiles opt in by exporting
``INSTALL_EXTENSION``, and the substrate merges their arguments onto one
parser and routes ``--profile``.

These tests hold that arrangement in place: the verb is substrate-owned,
the default is a profile the library ships (the ``minimal`` starter since
issue #3414), an unknown profile explains itself instead of dying with
``unrecognized arguments``, and the substrate never names a profile.
"""
from __future__ import annotations

import argparse
import re
from pathlib import Path

from okg.substrate.cli import main as cli_main
from okg.substrate.cli.profile_cli_loader import (
    discover_install_extensions,
    discover_profile_clis,
)


REPO_ROOT = Path(__file__).resolve().parents[3]
MAIN_PY = REPO_ROOT / "src" / "okg" / "substrate" / "cli" / "main.py"


def _install_parser() -> argparse.ArgumentParser:
    parser = cli_main._build_parser()
    for action in parser._actions:
        if isinstance(action, argparse._SubParsersAction):
            return action.choices["install"]
    raise AssertionError("no subparsers action on the root parser")


def test_install_is_substrate_owned_not_profile_owned() -> None:
    """No profile may register `install`; the substrate ships it."""
    for name, mod in discover_profile_clis().items():
        assert "install" not in getattr(mod, "DISPATCH", {}), (
            f"profile {name!r} registers `install`; the substrate owns it, "
            "and the profile loader refuses a profile that claims a "
            "substrate verb"
        )


def test_at_least_one_profile_is_installable() -> None:
    """The door must lead somewhere."""
    assert discover_install_extensions(), (
        "no profile exports INSTALL_EXTENSION, so `okg install` has "
        "nothing to install"
    )


def test_install_accepts_a_profile_flag() -> None:
    """The flag whose absence made the gallery's install command a lie."""
    parser = _install_parser()
    flags = {
        opt
        for action in parser._actions
        for opt in getattr(action, "option_strings", [])
    }
    assert "--profile" in flags, (
        "`okg install` has no --profile; the reference gallery advertises "
        "`okg install --profile cms`"
    )


def test_install_profile_cms_parses() -> None:
    """`okg install --profile cms` must PARSE.

    It may then fail with a real explanation, but it must no longer die at
    the parser with `unrecognized arguments: --profile cms` — which is how
    the command okg-deployments/README.md advertises has always failed.
    """
    parser = _install_parser()
    args = parser.parse_args(["--profile", "cms"])
    assert args.profile == "cms"


def test_default_profile_is_a_shipped_library_profile() -> None:
    """A zero-argument install builds a profile the library ships."""
    from okg.substrate.deployment_bootstrap.profile_init import (
        installable_library_profiles,
    )

    shipped = {profile.name for profile in installable_library_profiles()}
    assert cli_main.DEFAULT_INSTALL_PROFILE in shipped, (
        f"default install profile {cli_main.DEFAULT_INSTALL_PROFILE!r} is "
        f"not a shipped library profile; shipped: {sorted(shipped)}"
    )
    parser = _install_parser()
    args = parser.parse_args([])
    assert getattr(args, "profile", None) is None, (
        "--profile must default to None so the handler applies the "
        "default"
    )


def test_installer_profile_keeps_its_flag_surface() -> None:
    """Promotion must not drop the academic-researcher installer's flags."""
    parser = _install_parser()
    flags = {
        opt
        for action in parser._actions
        for opt in getattr(action, "option_strings", [])
    }
    for expected in ("--orcid", "--pi-name", "--name", "--dry-run", "--json"):
        assert expected in flags, (
            f"{expected} disappeared from `okg install` during promotion"
        )


def test_unknown_profile_explains_itself(capsys) -> None:
    """An unknown profile names the alternatives and the next command."""
    args = argparse.Namespace(profile="definitely-not-a-profile")
    rc = cli_main._cmd_install(args, as_json=False)
    assert rc != 0
    out = capsys.readouterr()
    combined = out.out + out.err
    assert "definitely-not-a-profile" in combined
    assert "fetch-deployments" in combined, (
        "the error should tell the reader how to get a reference deployment"
    )


def test_install_accepts_the_scaffold_flags_init_takes() -> None:
    """`install` offers every scaffold flag `init` does.

    `init` is to become an alias of `install`, and an alias cannot accept
    options the door refuses. `--profile` and `--json` are excluded: each
    parser owns them, and `init` binds `--profile` to a different dest.
    """
    parser = cli_main._build_parser()
    subs = next(
        a for a in parser._actions
        if isinstance(a, argparse._SubParsersAction)
    )

    def flags(verb: str) -> set[str]:
        return {
            opt
            for action in subs.choices[verb]._actions
            for opt in getattr(action, "option_strings", [])
            if opt.startswith("--")
        }

    missing = flags("init") - flags("install") - {"--profile", "--json"}
    assert not missing, (
        f"`okg install` is missing scaffold flags `okg init` accepts: "
        f"{sorted(missing)} — an alias cannot take options the door refuses"
    )


def test_profile_answer_flags_reach_both_verbs() -> None:
    """A profile's init questions become flags on `install`, not just `init`.

    Profile answers (`--postgres-dsn`, ...) are injected by scanning argv
    for `--profile`. That injection was hardcoded to the `init` subparser,
    so `okg install --profile workspace-graph --postgres-dsn ...` died with
    `unrecognized arguments` while the identical `init` call worked.
    """
    assert "install" in cli_main._PROFILE_ANSWER_VERBS
    assert "init" in cli_main._PROFILE_ANSWER_VERBS

    parser = cli_main._build_parser()
    injected = cli_main._maybe_inject_profile_flags(
        parser, ["install", "--profile", "workspace-graph"]
    )
    assert injected, (
        "no profile answer flags were injected for `install`; the "
        "workspace-graph profile declares init questions"
    )


def test_substrate_dispatcher_never_names_a_profile() -> None:
    """The boundary `retire-pi-prefix-commands` established still holds.

    Routing by discovery rather than by import is the whole point; a
    literal profile name in the substrate dispatcher would undo it.
    """
    source = MAIN_PY.read_text()
    # The declared default is data, not a reference to profile code, and is
    # the one legitimate occurrence.
    source = source.replace(
        'DEFAULT_INSTALL_PROFILE = "minimal"', "DEFAULT_INSTALL_PROFILE"
    )
    offenders = [
        m.group(0)
        for m in re.finditer(r"academic[-_]pi", source, re.I)
    ]
    assert not offenders, (
        f"src/okg/substrate/cli/main.py names a profile {offenders[:3]}; "
        "route through discover_install_extensions() instead"
    )
