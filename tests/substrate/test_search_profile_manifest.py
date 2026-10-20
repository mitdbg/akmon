"""Manifest-side contract for deployment search profiles (issue #797).

No database: `parse_search_config` is pure validation over the `search:` block,
so these run in the `fast` lane.

The point of every test here is that a malformed block FAILS, loudly, at load.
The alternative — tolerating it — leaves a deployment believing it scoped its
search when it did not, which is the same class of defect as the unscoped
ranking the profile exists to fix.
"""
from __future__ import annotations

import pytest

from okg.substrate.mcp.search_profiles import (
    RESERVED_ALL,
    DiversityBoundInvalid,
    PathGlobInvalid,
    SearchProfileConfigInvalid,
    SourceFamilyConfigInvalid,
    UnknownSearchProfile,
    UnknownSourceFamily,
    excluded_by_path_globs,
    glob_to_like,
    parse_search_config,
    path_matches_glob,
)


def test_absent_block_parses_to_none_and_changes_nothing() -> None:
    assert parse_search_config(None) is None


def test_a_well_formed_block_parses() -> None:
    config = parse_search_config({
        "default_profile": "analyst",
        "profiles": {
            "analyst": {"subtypes": ["chunk", "entity"],
                        "description": "content"},
            "ops": {"subtypes": ["pact_task"]},
        },
    })
    assert config is not None
    assert config.default_profile == "analyst"
    assert config.profiles["analyst"].subtypes == ("chunk", "entity")
    assert config.declared_names == ("analyst", "ops")


def test_strict_nested_keys_empty_profile_and_reserved_all_are_rejected() -> None:
    """The three ways a `search:` block can be wrong, each refused at load.

    The nested-key assertions check that the message names the ACCEPTED KEYS
    INSIDE the block. That matters: `search` is also policed by the top-level
    unknown-key census in `deployments/__init__.py`, and a test asserting only
    "load raises" would be satisfied by that path without ever exercising
    nested strictness. Only the in-block message can distinguish them.
    """
    # 1. A misspelled key directly under `search:`.
    with pytest.raises(SearchProfileConfigInvalid) as exc:
        parse_search_config({"default_profle": "analyst", "profiles": {}})
    message = str(exc.value)
    assert "default_profle" in message
    assert "'default_profile'" in message and "'profiles'" in message
    assert exc.value.data["accepted_keys"] == [
        "concept_groups", "default_profile", "profiles", "source_families",
    ]

    # 2. A misspelled key inside a profile.
    with pytest.raises(SearchProfileConfigInvalid) as exc:
        parse_search_config(
            {"profiles": {"analyst": {"subtyps": ["chunk"]}}}
        )
    message = str(exc.value)
    assert "subtyps" in message
    assert "'subtypes'" in message
    assert exc.value.data["profile"] == "analyst"

    # 3. An empty subtype list: an empty scope returns an empty page that
    #    cannot be told apart from "nothing matched".
    with pytest.raises(SearchProfileConfigInvalid) as exc:
        parse_search_config({"profiles": {"analyst": {"subtypes": []}}})
    assert "non-empty" in str(exc.value)

    # 4. `all` is reserved, so a deployment default can never trap a caller.
    with pytest.raises(SearchProfileConfigInvalid) as exc:
        parse_search_config(
            {"profiles": {RESERVED_ALL: {"subtypes": ["chunk"]}}}
        )
    assert "reserved" in str(exc.value)


def test_default_profile_must_name_a_declared_profile() -> None:
    with pytest.raises(UnknownSearchProfile) as exc:
        parse_search_config({
            "default_profile": "anaylst",
            "profiles": {"analyst": {"subtypes": ["chunk"]}},
        })
    assert "anaylst" in str(exc.value)
    assert exc.value.data["declared"] == ["analyst"]


def test_default_profile_may_be_the_reserved_all() -> None:
    config = parse_search_config({
        "default_profile": RESERVED_ALL,
        "profiles": {"analyst": {"subtypes": ["chunk"]}},
    })
    assert config is not None
    assert config.default_profile == RESERVED_ALL


def test_manifest_load_rejects_a_bad_block(tmp_path) -> None:
    """The validation runs at manifest load, not lazily at first search.

    A deployment whose `search:` block is wrong must fail where the operator
    can see it, rather than silently serving unscoped results forever.
    """
    from okg.substrate.deployments import DeploymentManifest

    deployment = tmp_path / "bad-search"
    (deployment / "schemas").mkdir(parents=True)
    (deployment / "source_registry.yaml").write_text("{}\n", encoding="utf-8")
    (deployment / "deployment.yaml").write_text(
        "name: bad-search\n"
        "description: fixture\n"
        "schema_dir: schemas\n"
        "source_registry: source_registry.yaml\n"
        "search:\n"
        "  profiles:\n"
        "    analyst:\n"
        "      subtyps: [chunk]\n",
        encoding="utf-8",
    )
    with pytest.raises(SearchProfileConfigInvalid) as exc:
        DeploymentManifest.from_yaml(deployment / "deployment.yaml")
    assert "subtyps" in str(exc.value)


def test_manifest_load_accepts_and_captures_a_good_block(tmp_path) -> None:
    from okg.substrate.deployments import DeploymentManifest

    deployment = tmp_path / "good-search"
    (deployment / "schemas").mkdir(parents=True)
    (deployment / "source_registry.yaml").write_text("{}\n", encoding="utf-8")
    (deployment / "deployment.yaml").write_text(
        "name: good-search\n"
        "description: fixture\n"
        "schema_dir: schemas\n"
        "source_registry: source_registry.yaml\n"
        "search:\n"
        "  default_profile: analyst\n"
        "  profiles:\n"
        "    analyst:\n"
        "      subtypes: [chunk]\n",
        encoding="utf-8",
    )
    manifest = DeploymentManifest.from_yaml(deployment / "deployment.yaml")
    assert manifest.search == {
        "default_profile": "analyst",
        "profiles": {"analyst": {"subtypes": ["chunk"]}},
    }


# ---------- mcp-authority-diversity-budgets (issue #1444, part 2) ----------
#
# Same rule as above: a malformed authority declaration FAILS at load. A
# deployment that believes it declared an authority scope, and did not, is the
# same class of defect as the unscoped ranking the profile exists to fix.

def test_a_well_formed_authority_block_parses() -> None:
    config = parse_search_config({
        "source_families": {
            "documentation": ["source:docs", "source:handbook"],
            "code": ["source:git"],
        },
        "profiles": {
            "authority": {
                "subtypes": ["chunk"],
                "source_families": ["documentation"],
                "exclude_path_globs": ["tests/**"],
                "max_per_parent": 2,
            },
        },
    })
    assert config is not None
    assert config.declared_family_names == ("code", "documentation")
    assert config.source_families["documentation"].sources == (
        "source:docs", "source:handbook",
    )
    profile = config.profiles["authority"]
    assert profile.source_families == ("documentation",)
    assert profile.exclude_path_globs == ("tests/**",)
    assert profile.max_per_parent == 2
    # Every declared producer maps to exactly one family, for the row label.
    assert config.family_by_source() == (
        ("source:docs", "documentation"),
        ("source:git", "code"),
        ("source:handbook", "documentation"),
    )


def test_absent_authority_keys_leave_every_control_off() -> None:
    """The controls must be OFF unless a profile declares them."""
    config = parse_search_config({"profiles": {"p": {"subtypes": ["chunk"]}}})
    assert config is not None
    assert config.source_families == {}
    profile = config.profiles["p"]
    assert profile.source_families == ()
    assert profile.exclude_path_globs == ()
    assert profile.max_per_parent is None


@pytest.mark.parametrize("families", [
    [],                                   # not a mapping
    {"docs": []},                         # a family naming no source
    {"docs": "source:docs"},              # not a list
    {"docs": ["  "]},                     # a blank producer id
    {RESERVED_ALL: ["source:docs"]},      # the reserved family name
])
def test_a_malformed_source_family_block_is_refused(families) -> None:
    with pytest.raises(SourceFamilyConfigInvalid):
        parse_search_config({"source_families": families, "profiles": {}})


def test_one_producer_may_not_belong_to_two_families() -> None:
    """A row would then have no single `source_family` to report."""
    with pytest.raises(SourceFamilyConfigInvalid) as exc:
        parse_search_config({
            "source_families": {"a": ["source:x"], "b": ["source:x"]},
            "profiles": {},
        })
    assert "source:x" in str(exc.value)
    assert exc.value.data["families"] == ["a", "b"]


def test_a_profile_naming_an_undeclared_family_is_refused_at_load() -> None:
    with pytest.raises(UnknownSourceFamily) as exc:
        parse_search_config({
            "source_families": {"docs": ["source:docs"]},
            "profiles": {"p": {"subtypes": ["chunk"],
                               "source_families": ["dcos"]}},
        })
    message = str(exc.value)
    assert "dcos" in message and "docs" in message


@pytest.mark.parametrize("body", [
    {"subtypes": ["chunk"], "source_families": []},
    {"subtypes": ["chunk"], "source_families": "docs"},
    {"subtypes": ["chunk"], "exclude_path_globs": []},
    {"subtypes": ["chunk"], "exclude_path_globs": [""]},
])
def test_an_empty_or_mistyped_profile_authority_key_is_refused(body) -> None:
    with pytest.raises((SearchProfileConfigInvalid, PathGlobInvalid)):
        parse_search_config({
            "source_families": {"docs": ["source:docs"]},
            "profiles": {"p": body},
        })


def test_an_untranslatable_glob_is_refused_at_load_not_matched_literally() -> None:
    """Accepting it would exclude nothing while the envelope claimed it did."""
    with pytest.raises(PathGlobInvalid) as exc:
        parse_search_config({
            "profiles": {"p": {"subtypes": ["chunk"],
                               "exclude_path_globs": ["tests/[ab]/**"]}},
        })
    assert "tests/[ab]/**" in str(exc.value)


def test_an_invalid_profile_diversity_default_is_refused() -> None:
    for bad in (0, -1, "2", 1.5):
        with pytest.raises(DiversityBoundInvalid):
            parse_search_config({
                "profiles": {"p": {"subtypes": ["chunk"],
                                   "max_per_parent": bad}},
            })


def test_the_glob_translation_is_one_walk_used_by_both_forms() -> None:
    """SQL LIKE and the Python residual come from ONE translation.

    Two translations would drift, and the drift would be invisible: the SQL
    predicate decides what the rank cap is spent on, the Python residual
    decides what is returned.
    """
    assert glob_to_like("tests/**") == "tests/%"
    assert glob_to_like("**/conftest.py") == "%/conftest.py"
    assert glob_to_like("a?c") == "a_c"
    # LIKE metacharacters in the literal part are escaped, so they cannot
    # become wildcards.
    assert glob_to_like("a_b%c") == "a\\_b\\%c"

    assert path_matches_glob("tests/substrate/x.py", "tests/**")
    assert not path_matches_glob("src/tests/x.py", "tests/**")
    assert path_matches_glob("src/pkg/conftest.py", "**/conftest.py")
    assert path_matches_glob("a_b%c", "a_b%c")
    assert not path_matches_glob("aXbYc", "a_b%c")

    # A row with no `path` is never excluded: an exclusion names paths.
    assert not excluded_by_path_globs({"text": "x"}, ["tests/**"])
    assert not excluded_by_path_globs(None, ["tests/**"])
    assert excluded_by_path_globs({"path": "tests/a.py"}, ["tests/**"])
