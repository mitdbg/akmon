"""Tests for the `okg add <source>` wizard module + dispatch.

Per `streamline-academic-pi-install` Phase 5: per-source wizards
collect config, append to source_registry.yaml, run a scoped
publish, print delta.
"""
from __future__ import annotations

import argparse
from pathlib import Path
from unittest import mock

import yaml

from okg.substrate.cli.profile_cli_loader import get_profile_cli

_pi_cli_pkg = get_profile_cli("academic-researcher")
pi_cli = _pi_cli_pkg.install
wiz = _pi_cli_pkg.wizards


# --- wizard catalog ---


def test_wizards_cover_expected_short_names() -> None:
    """Every operator-facing short-name SHALL be in the catalog.

    Per `unify-source-admin-catalog`: the 10 original wizards
    plus the 6 filesystem-templated sources newly cataloged so
    the conversational setup agent can drive them via
    `add_source(...)`.
    """
    expected = {
        # Credentialed connector sources (5) — outlook pair added by
        # add-msgraph-sources (one shared msgraph Entra app).
        "gmail", "google-cal", "slack",
        "outlook-mail", "outlook-cal",
        # Public identity sources wired after setup approval —
        # dblp-venues added by source-health-matrix (the venue
        # scraper was source-defaults-only, not okg-add-able).
        "publications", "orcid-publications", "nsf-awards", "doe-osti",
        "dblp-venues",
        # Path / URL prompts — canvas added by
        # add-academic-corpora-sources.
        "apple-cal", "cal-ics", "canvas",
        "notes", "papers",
        "group-page", "lab-talks",
        # LLM (1)
        "cv",
        # Filesystem-templated, no-op wizard (6) —
        # `unify-source-admin-catalog`.
        "talks", "bib-references", "mit-courses",
        "author-aliases", "concept-aliases", "external-repos",
        "latex-project-git-files", "latex-project-doc-corpus",
        "latex-project",
        # Scholarly identity-alias index (PDF publication aliases),
        # cataloged by `Improve memex run control and PDF publication
        # aliases`.
        "identity-aliases",
        # Broad Workspace Graph sources, opt-in after setup approval.
        "git-files", "git-history", "code-structure",
        "openspec", "agent-sessions",
    }
    assert set(wiz.WIZARDS.keys()) == expected


def test_each_wizard_maps_to_a_known_source_id() -> None:
    """The middle of each WIZARDS triple SHALL be a source-id
    that appears in the academic-researcher profile's source-defaults."""
    profile_sd = Path(
        "src/okg/substrate/library/profiles/academic-researcher/source-defaults"
    )
    available = {p.stem for p in profile_sd.glob("*.yaml")}
    for short, (source_id, _kind, _fn) in wiz.WIZARDS.items():
        assert source_id in available, (
            f"wizard `{short}` maps to source-id `{source_id}` "
            f"which is not in {profile_sd}"
        )


def test_each_source_default_is_reachable_from_wizards() -> None:
    """Every profile source-default SHALL have a WIZARDS entry —
    the inverse of `test_each_wizard_maps_to_a_known_source_id`.

    WIZARDS feeds `SOURCE_CATALOG`, the only name table
    `add_source` resolves through; a source-default without a
    wizard entry is proposable by `propose_setup` but un-wireable
    (`unknown_source`). This drift shipped twice: the 6 names
    fixed by `unify-source-admin-catalog`, then
    `orcid_publications` (the day-1 publication source, caught
    live 2026-06-09 by the advisor-demo de-risk).
    """
    # Known-unmapped source-defaults, each with a reason. Shrink
    # this set; never grow it without a written reason.
    allowed_unmapped = {
        # `dblp` is the name-search fallback variant of
        # `dblp_by_orcid`; setup proposes the ORCID variant and
        # no wizard drives plain-DBLP yet.
        "dblp",
    }
    profile_sd = Path(
        "src/okg/substrate/library/profiles/academic-researcher/source-defaults"
    )
    available = {p.stem for p in profile_sd.glob("*.yaml")}
    mapped = {sid for (sid, _kind, _fn) in wiz.WIZARDS.values()}
    unmapped = available - mapped - allowed_unmapped
    assert not unmapped, (
        f"source-defaults with no WIZARDS entry (un-wireable via "
        f"add_source/commit_setup): {sorted(unmapped)}"
    )


def test_each_wizard_kind_is_in_valid_kinds() -> None:
    """The kind SHALL be drawn from `wiz.VALID_KINDS` so future
    kinds added without updating source_admin's envelope branches
    fail this test."""
    for short, (_sid, kind, _fn) in wiz.WIZARDS.items():
        assert kind in wiz.VALID_KINDS, (
            f"wizard `{short}` has kind={kind!r} which is not in "
            f"{sorted(wiz.VALID_KINDS)}"
        )


def test_supported_short_names_is_sorted() -> None:
    assert wiz.supported_short_names() == sorted(wiz.WIZARDS.keys())


def test_recovery_short_names_exclude_connectors() -> None:
    recovery = set(wiz.supported_recovery_short_names())
    assert {"gmail", "google-cal", "slack"}.isdisjoint(recovery)
    assert {"notes", "cal-ics", "papers"}.issubset(recovery)


# --- path wizard ---


def test_path_wizard_returns_resolved_path(tmp_path: Path) -> None:
    """`_prompt_path` SHALL return the resolved absolute path of
    an existing directory.
    """
    real_path = tmp_path / "notes"
    real_path.mkdir()
    with mock.patch(
        "builtins.input", side_effect=[str(real_path)],
    ):
        got = wiz._prompt_path("Notes dir")
    assert got == str(real_path.resolve())


def test_path_wizard_rejects_nonexistent_path(tmp_path: Path) -> None:
    """`_prompt_path` SHALL re-prompt if the supplied path doesn't
    exist (we test by supplying a bad path first, then a real one).
    """
    real_path = tmp_path / "real"
    real_path.mkdir()
    bad_path = tmp_path / "nonexistent"
    with mock.patch(
        "builtins.input",
        side_effect=[str(bad_path), str(real_path)],
    ):
        got = wiz._prompt_path("Notes dir")
    assert got == str(real_path.resolve())


def test_wizard_notes_returns_notes_dir_config(tmp_path: Path) -> None:
    real_path = tmp_path / "my-notes"
    real_path.mkdir()
    with mock.patch(
        "builtins.input", side_effect=[str(real_path)],
    ):
        config = wiz._wizard_notes("test_pi")
    assert config == {"notes_dir": str(real_path.resolve())}


# --- add / remove dispatch ---


def test_add_rejects_unknown_source(tmp_path: Path) -> None:
    """`_cmd_add` SHALL fail cleanly when given an unknown source
    short-name. The registry SHALL NOT be modified.
    """
    # Build a fake deployment dir to satisfy the existence check.
    repo_root = tmp_path
    dep_dir = repo_root / "deployments/test_pi"
    dep_dir.mkdir(parents=True)
    (dep_dir / "source_registry.yaml").write_text(
        yaml.safe_dump({"sources": {}}),
    )

    args = argparse.Namespace(
        source="gmial",  # typo
        deployment="test_pi",
    )
    with mock.patch.object(
        pi_cli, "_find_repo_root",
        return_value=repo_root,
    ):
        rc = pi_cli._cmd_add(args, as_json=True)
    assert rc == 1
    # Registry untouched.
    raw = yaml.safe_load(
        (dep_dir / "source_registry.yaml").read_text(),
    )
    assert raw == {"sources": {}}


def test_add_appends_to_registry_and_runs_scoped_publish(
    tmp_path: Path,
) -> None:
    """`_cmd_add notes <path>` SHALL run the notes wizard, append
    to source_registry.yaml, and invoke a scoped publish via
    subprocess.
    """
    repo_root = tmp_path
    dep_dir = repo_root / "deployments/test_pi"
    dep_dir.mkdir(parents=True)
    (dep_dir / "sound_notes.py").write_text(
        "class SoundNotesSource:\n"
        "    change_probe_kind = 'content_hash'\n"
        # The notes wizard writes `params.notes_dir`; the stub takes it as
        # the real NotesDirSource does, or activation refuses the key.
        "    def __init__(self, notes_dir=None):\n"
        "        self.change_probe = object()\n",
    )
    (dep_dir / "source_registry.yaml").write_text(
        yaml.safe_dump({
            "sources": {
                "notes_dir": {
                    "module": "sound_notes",
                    "class": "SoundNotesSource",
                    "ownership_id": "test_pi.notes_dir",
                    "source_class": "discovery_crawl",
                    "record_identity_kind": "scoped_locator",
                    "record_identity_fields": ["path"],
                    "source_revision_kind": "content_hash",
                    "deletion_semantics": "missing_from_completed_scope",
                    "publication_mode": "published_generation",
                    "enabled_by_default": False,
                },
            },
        }),
    )
    notes_dir = tmp_path / "my-notes"
    notes_dir.mkdir()
    (notes_dir / "x.md").write_text("# hello\n")

    args = argparse.Namespace(
        source="notes",
        deployment="test_pi",
    )
    with (
        mock.patch.object(
            pi_cli, "_find_repo_root",
            return_value=repo_root,
        ),
        mock.patch(
            "builtins.input", side_effect=[str(notes_dir)],
        ),
        mock.patch(
            "subprocess.run",
            return_value=mock.Mock(returncode=0),
        ) as fake_run,
        mock.patch.object(
            pi_cli, "_try_aggregate_counts",
            return_value={"document": 1},
        ),
    ):
        rc = pi_cli._cmd_add(args, as_json=True)

    assert rc == 0
    # Scoped ingest invoked with --include notes_dir --inline.
    fake_run.assert_called_once()
    call_args, _kwargs = fake_run.call_args
    cmd = call_args[0]
    assert "ingest" in cmd and "--inline" in cmd
    assert "--include" in cmd
    idx = cmd.index("--include")
    assert cmd[idx + 1] == "notes_dir"
    # Registry now has the notes_dir entry with enabled_by_default=true.
    raw = yaml.safe_load(
        (dep_dir / "source_registry.yaml").read_text(),
    )
    entry = raw["sources"]["notes_dir"]
    assert entry["enabled_by_default"] is True
    assert entry["params"]["notes_dir"] == str(notes_dir.resolve())


def test_remove_drops_entry_without_touching_connector_tokens(
    tmp_path: Path,
) -> None:
    """`_cmd_remove gmail` SHALL drop the gmail_api registry entry.
    """
    repo_root = tmp_path
    dep_dir = repo_root / "deployments/test_pi"
    dep_dir.mkdir(parents=True)
    (dep_dir / "source_registry.yaml").write_text(
        yaml.safe_dump({
            "sources": {
                "gmail_api": {
                    "enabled_by_default": True,
                    "params": {"gmail_labels": ["research"]},
                },
                "dblp": {"enabled_by_default": True},
            },
        }),
    )

    args = argparse.Namespace(source="gmail", deployment="test_pi")
    with mock.patch.object(
        pi_cli, "_find_repo_root",
        return_value=repo_root,
    ):
        rc = pi_cli._cmd_remove(args, as_json=True)
    assert rc == 0
    raw = yaml.safe_load(
        (dep_dir / "source_registry.yaml").read_text(),
    )
    assert "gmail_api" not in raw["sources"]
    assert "dblp" in raw["sources"]  # other entries intact


def test_remove_nonexistent_source_returns_error(
    tmp_path: Path,
) -> None:
    repo_root = tmp_path
    dep_dir = repo_root / "deployments/test_pi"
    dep_dir.mkdir(parents=True)
    (dep_dir / "source_registry.yaml").write_text(
        yaml.safe_dump({"sources": {}}),
    )
    args = argparse.Namespace(source="gmail", deployment="test_pi")
    with mock.patch.object(
        pi_cli, "_find_repo_root",
        return_value=repo_root,
    ):
        rc = pi_cli._cmd_remove(args, as_json=True)
    assert rc == 1


# --- reset dispatch ---


def test_reset_dispatches_to_internal_teardown_helper(
    tmp_path: Path,
) -> None:
    """`_cmd_reset` SHALL dispatch to the internal teardown helper."""
    args = argparse.Namespace(
        deployment="test_pi", yes=True,
    )
    repo_root = tmp_path
    dep_dir = repo_root / "deployments/test_pi"
    dep_dir.mkdir(parents=True)
    with (
        mock.patch.object(
            pi_cli, "_find_repo_root",
            return_value=repo_root,
        ),
        mock.patch.object(
            pi_cli, "_provision_postgres_and_scaffold_reset",
            return_value=0,
        ) as fake_reset,
    ):
        rc = pi_cli._cmd_reset(args, as_json=True)
    assert rc == 0
    fake_reset.assert_called_once()
    _args, kwargs = fake_reset.call_args
    assert kwargs["deployment_name"] == "test_pi"
    assert kwargs["repo_root"] == repo_root


# --- dispatch wiring ---


def test_new_subcommands_in_dispatch() -> None:
    """The profile-owned phase-5 verbs SHALL be in academic-researcher's DISPATCH.

    `install` is deliberately excluded: per PACT
    `define-newcomer-entry-path` it is the one documented way in, so the
    substrate owns the verb and reaches this profile through
    `INSTALL_EXTENSION`. Leaving it in DISPATCH would also be refused by
    the profile loader's substrate-verb collision guard.
    """
    for verb in ("add", "remove", "reset"):
        assert verb in pi_cli.DISPATCH, (
            f"`okg {verb}` missing from pi.DISPATCH"
        )
    assert "install" not in pi_cli.DISPATCH
