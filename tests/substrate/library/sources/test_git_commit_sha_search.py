"""Commit SHAs are searchable: `GitCommit` puts `commit_sha` and the short forms
people type (`commit_sha_prefixes`: every 7- to 12-char prefix) into its search
text, because the BM25 `english` index matches whole lexemes, never prefixes."""

from __future__ import annotations

import sys
from pathlib import Path

import psycopg
import pytest
import yaml

_FIXTURES_DIR = Path(__file__).resolve().parents[2] / "fixtures"
if str(_FIXTURES_DIR) not in sys.path:
    sys.path.insert(0, str(_FIXTURES_DIR))

_SCHEMA = (
    Path(__file__).resolve().parents[4]
    / "src/okg/substrate/library/ontologies/git_graph/schema.yaml"
)
_PREFIX_LENGTHS = range(7, 13)


def _git_commit_class() -> dict:
    return yaml.safe_load(_SCHEMA.read_text())["classes"]["GitCommit"]


def test_git_commit_search_text_declares_sha_fields() -> None:
    cls = _git_commit_class()
    fields = cls["annotations"]["okg_searchable_text_fields"].split(",")
    assert "commit_sha" in fields and "commit_sha_prefixes" in fields
    assert "message_subject" in fields and "message_body" in fields  # unchanged
    assert "commit_sha_prefixes" in cls["attributes"]


def test_commit_nodes_carry_sha_prefixes(tmp_path) -> None:
    from okg_self_micro import materialize  # type: ignore
    from okg.substrate.library.sources.git_graph_history import GitHistorySource

    paths = materialize(tmp_path)
    run = GitHistorySource(repo="okg", repo_root=paths.repo_root).run("run-1", mode="cursor")
    commits = [f for f in run.facts if getattr(f, "subtype", None) == "git_commit"]
    assert commits
    for fact in commits:
        sha = fact.attrs["commit_sha"]
        assert fact.attrs["commit_sha_prefixes"] == " ".join(sha[:n] for n in _PREFIX_LENGTHS)


@pytest.mark.postgres
@pytest.mark.parametrize(
    "sha",
    [
        "b9c2ed0841ce64cf7ce785d0cf90f622a00a2446",  # letters first
        "0087c26064b492b8e8d0a1f2c3b4d5e6f7a8b9c0",  # leading zeros
        "1234567890abcdef1234567890abcdef12345678",  # all-digit short forms
    ],
)
def test_short_and_full_sha_match_the_english_index(clean_runtime: str, sha: str) -> None:
    prefixes = " ".join(sha[:n] for n in _PREFIX_LENGTHS)
    text = f"Fix typo in rse attr\n\n{sha}\n\n{prefixes}"
    with psycopg.connect(clean_runtime, autocommit=True) as conn, conn.cursor() as cur:
        for query in (*(sha[:n] for n in _PREFIX_LENGTHS), sha):
            cur.execute(
                "SELECT to_tsvector('english', %s) @@ plainto_tsquery('english', %s)",
                (text, query),
            )
            assert cur.fetchone()[0] is True, query
        # without the prefixes the short form would not match (the reason for the field)
        cur.execute(
            "SELECT to_tsvector('english', %s) @@ plainto_tsquery('english', %s)",
            (f"Fix typo in rse attr\n\n{sha}", sha[:7]),
        )
        assert cur.fetchone()[0] is False


@pytest.mark.postgres
def test_body_only_word_matches_the_english_index(clean_runtime: str, tmp_path) -> None:
    """A word only in the commit body is in the node's search text (#2671 A)."""
    import json
    import os
    import subprocess

    from okg.substrate.library.sources.git_graph_history import GitHistorySource
    from tests.pytest.catalog_fixture_builders import SubtypeSpec, seed_catalog

    repo = tmp_path / "repo"
    repo.mkdir()
    env = {
        **os.environ,
        "GIT_AUTHOR_NAME": "Dev",
        "GIT_AUTHOR_EMAIL": "dev@example.test",
        "GIT_COMMITTER_NAME": "Dev",
        "GIT_COMMITTER_EMAIL": "dev@example.test",
    }
    subprocess.run(["git", "init", "-q", "-b", "main"], cwd=repo, env=env, check=True)
    (repo / "config.yaml").write_text("retries: 5\n")
    subprocess.run(["git", "add", "config.yaml"], cwd=repo, env=env, check=True)
    subprocess.run(
        [
            "git",
            "commit",
            "-q",
            "-m",
            "Raise retries to 5",
            "-m",
            "The zanzibarquokka mirror drops one request in forty.",
        ],
        cwd=repo,
        env=env,
        check=True,
    )
    run = GitHistorySource(repo="r", repo_root=repo).run("run-1", mode="cursor")
    attrs = next(
        f.attrs for f in run.facts if getattr(f, "subtype", None) == "git_commit"
    )
    fields = _git_commit_class()["annotations"]["okg_searchable_text_fields"].split(",")
    # The write path's own stamper (okg._stamp_searchable_text) builds the
    # search text from the subtype's declared fields in the catalog.
    match = (
        "SELECT to_tsvector('english', "
        "okg._stamp_searchable_text('git_commit', %s::jsonb, %s)->>'text') "
        "@@ plainto_tsquery('english', %s)"
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        cv = seed_catalog(
            conn,
            subtypes=[
                SubtypeSpec("git_commit", metadata={"searchable_text_fields": fields})
            ],
        )
        with conn.cursor() as cur:
            cur.execute(match, (json.dumps(attrs), cv, "zanzibarquokka"))
            assert cur.fetchone()[0] is True
            # Subject-only attrs (what the source stored before) do not match.
            subject_only = {k: v for k, v in attrs.items() if k != "message_body"}
            cur.execute(match, (json.dumps(subject_only), cv, "zanzibarquokka"))
            assert cur.fetchone()[0] is False


@pytest.mark.postgres
def test_old_value_in_a_diff_matches_the_english_index(
    clean_runtime: str, tmp_path
) -> None:
    """An old config value is in its diff node's search text (#2671 B)."""
    import json
    import os
    import subprocess

    from okg.substrate.library.sources.git_graph_history import GitHistorySource
    from tests.pytest.catalog_fixture_builders import SubtypeSpec, seed_catalog

    repo = tmp_path / "repo"
    repo.mkdir()
    env = {
        **os.environ,
        "GIT_AUTHOR_NAME": "Dev",
        "GIT_AUTHOR_EMAIL": "dev@example.test",
        "GIT_COMMITTER_NAME": "Dev",
        "GIT_COMMITTER_EMAIL": "dev@example.test",
    }
    subprocess.run(["git", "init", "-q", "-b", "main"], cwd=repo, env=env, check=True)
    for message, value in (("Add config", "bluefalcon"), ("New pool", "greenheron")):
        (repo / "config.yaml").write_text(f"pool_name: {value}\n")
        subprocess.run(["git", "add", "config.yaml"], cwd=repo, env=env, check=True)
        subprocess.run(["git", "commit", "-q", "-m", message], cwd=repo, env=env, check=True)
    head = subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=repo, env=env, text=True
    ).strip()
    run = GitHistorySource(repo="r", repo_root=repo, store_diffs=True).run("run-1")
    attrs = next(
        f.attrs for f in run.facts if getattr(f, "node_id", "") == f"diff:r:{head}:config.yaml"
    )
    cls = yaml.safe_load(_SCHEMA.read_text())["classes"]["GitFileDiff"]["annotations"]
    metadata = {
        "searchable_text_fields": cls["okg_searchable_text_fields"].split(","),
        "searchable_text_path_fields": cls["okg_searchable_text_path_fields"].split(","),
    }
    match = (
        "SELECT to_tsvector('english', "
        "okg._stamp_searchable_text('git_file_diff', %s::jsonb, %s)->>'text') "
        "@@ plainto_tsquery('english', %s)"
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        cv = seed_catalog(conn, subtypes=[SubtypeSpec("git_file_diff", metadata=metadata)])
        with conn.cursor() as cur:
            for query in ("bluefalcon", "greenheron", "pool_name", "config"):
                cur.execute(match, (json.dumps(attrs), cv, query))
                assert cur.fetchone()[0] is True, query
