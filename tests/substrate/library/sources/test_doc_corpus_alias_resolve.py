"""Stride A.1 (2026-04-26) — W5: DocCorpusSource emits a
`mentions <canonical>` edge when a regex/NER hit's
`entity_value` resolves via `okg.aliases`.

Sets up a deployment with one alias row mapping
`add-greeting-feature` (a synthetic change slug) → an
`open_spec_change` node. The doc-corpus adapter parses a
markdown file containing that slug, NER picks it up, the
alias resolver finds the canonical, and the runner emits
both the `member_of` edge AND the `mentions` edge.
"""
from __future__ import annotations

import textwrap
from pathlib import Path

import psycopg
import pytest
import yaml

from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.ingest import SourceRunner
from okg.substrate.library.sources.base import NodeFact, RecordSet
from okg.substrate.library.sources.doc_corpus import DocCorpusSource


pytestmark = pytest.mark.postgres


def _seed_cv_with_alias(
    conn: psycopg.Connection,
    *,
    alias: str,
    canonical: str,
) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO okg.catalog_versions (status, applied_at) "
            "VALUES ('applied', now()) RETURNING catalog_version_id"
        )
        cv = cur.fetchone()[0]
        cur.execute(
            "INSERT INTO okg.aliases "
            "  (alias, canonical, scope, catalog_version_id) "
            "VALUES (%s, %s, NULL, %s)",
            (alias, canonical, cv),
        )
    return cv


def _seed_predecessor_watermark(
    conn: psycopg.Connection, *, source: str,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO okg.source_watermarks "
            "  (source, last_reconcile_at) VALUES (%s, now())",
            (source,),
        )


def _seed_graph_node(
    conn: psycopg.Connection,
    *,
    node_id: str,
    subtype: str,
) -> None:
    ChronosGraphStore.from_connection(conn).apply_facts(
        [
            NodeFact(
                node_id=node_id,
                subtype=subtype,
                attrs={"title": node_id},
                source_record_id={"id": node_id},
                source_revision={"rev": "test"},
            ),
        ],
        source="test.seed",
    )


def _retract_graph_node(
    conn: psycopg.Connection,
    *,
    node_id: str,
    subtype: str,
) -> None:
    ChronosGraphStore.from_connection(conn).apply_facts(
        [
            NodeFact(
                node_id=node_id,
                subtype=subtype,
                attrs={},
                source_record_id={"id": node_id},
                source_revision={"rev": "retracted"},
                op="R",
            ),
        ],
        source="test.seed",
    )


def test_alias_match_emits_mentions_edge(
    clean_runtime: str, tmp_path: Path,
) -> None:
    canonical = "change:add-greeting-feature"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_cv_with_alias(
            conn, alias="add-greeting-feature", canonical=canonical,
        )
        _seed_graph_node(
            conn,
            node_id=canonical,
            subtype="open_spec_change",
        )
        _seed_predecessor_watermark(
            conn, source="test.predecessor",
        )

    corpus = tmp_path / "corpus"
    corpus.mkdir()
    (corpus / "note.md").write_text(textwrap.dedent("""\
        # Note

        See add-greeting-feature for context.
    """))

    extr = tmp_path / "extractors"
    extr.mkdir()
    (extr / "extractors.yaml").write_text(yaml.safe_dump({
        "parser": [{
            "format": "markdown",
            "class": "okg.substrate.library.extraction.parse.markdown.MarkdownParser",
        }],
        "chunker": [{
            "class": "okg.substrate.library.extraction.chunk.window.WindowChunker",
            "config": {"max_chars": 1500, "overlap": 100},
        }],
        "ner": [{
            "class": "okg.substrate.library.extraction.ner.regex_backend.RegexEntityExtractor",
            "config": {"rules": [
                ["change_slug", "(?:add|fix)-[a-z0-9-]+"],
            ]},
        }],
    }))

    SourceRunner(
        clean_runtime, source_name="test.alias",
    ).run(
        DocCorpusSource(
            repo="test", repo_root=corpus, non_git_walk=True,
            extractors_dir=extr,
            include_globs=("**/*.md",),
            predecessor_source="test.predecessor",
            dsn=clean_runtime,
        ),
        mode="scope_complete",
    )

    n_mentions_edges = _graph_count(
        clean_runtime,
        table="okg.graph_edges",
        where="source = :source AND edge_type = 'mentions' AND dst = :dst",
        params={"source": "test.alias", "dst": canonical},
    )
    n_mention_nodes = _graph_count(
        clean_runtime,
        table="okg.graph_nodes",
        where="source = :source AND subtype = 'entity_mention'",
        params={"source": "test.alias"},
    )
    assert n_mention_nodes >= 1, n_mention_nodes
    assert n_mentions_edges >= 1, n_mentions_edges


def test_alias_target_filter_blocks_out_of_scope_mentions(
    clean_runtime: str, tmp_path: Path,
) -> None:
    canonical = "pact-change:fix-source-sync-liveness"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_cv_with_alias(
            conn,
            alias="fix-source-sync-liveness",
            canonical=canonical,
        )
        _seed_graph_node(
            conn,
            node_id=canonical,
            subtype="pact_change",
        )
        _seed_predecessor_watermark(
            conn, source="test.predecessor.filtered",
        )

    corpus = tmp_path / "corpus_filtered"
    corpus.mkdir()
    (corpus / "note.md").write_text(textwrap.dedent("""\
        # Note

        See fix-source-sync-liveness for context.
    """))

    extr = tmp_path / "extractors_filtered"
    extr.mkdir()
    (extr / "extractors.yaml").write_text(yaml.safe_dump({
        "parser": [{
            "format": "markdown",
            "class": "okg.substrate.library.extraction.parse.markdown.MarkdownParser",
        }],
        "chunker": [{
            "class": "okg.substrate.library.extraction.chunk.window.WindowChunker",
            "config": {"max_chars": 1500, "overlap": 100},
        }],
        "ner": [{
            "class": "okg.substrate.library.extraction.ner.regex_backend.RegexEntityExtractor",
            "config": {"rules": [
                ["change_slug", "(?:add|fix)-[a-z0-9-]+"],
            ]},
        }],
    }))

    SourceRunner(
        clean_runtime, source_name="test.alias.filtered",
    ).run(
        DocCorpusSource(
            repo="test",
            repo_root=corpus, non_git_walk=True,
            extractors_dir=extr,
            include_globs=("**/*.md",),
            predecessor_source="test.predecessor.filtered",
            dsn=clean_runtime,
            mention_target_subtypes=("open_spec_change",),
        ),
        mode="scope_complete",
    )

    n_mention_nodes = _graph_count(
        clean_runtime,
        table="okg.graph_nodes",
        where="source = :source AND subtype = 'entity_mention'",
        params={"source": "test.alias.filtered"},
    )
    n_mentions_edges = _graph_count(
        clean_runtime,
        table="okg.graph_edges",
        where="source = :source AND edge_type = 'mentions' AND dst = :dst",
        params={"source": "test.alias.filtered", "dst": canonical},
    )
    assert n_mention_nodes >= 1, n_mention_nodes
    assert n_mentions_edges == 0, n_mentions_edges


def test_unchanged_record_set_reprocesses_stale_mentions_target(
    clean_runtime: str, tmp_path: Path,
) -> None:
    canonical = "change:retired-feature"
    source_name = "test.alias.stale-target"
    predecessor = "test.predecessor.stale-target"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_cv_with_alias(
            conn,
            alias="retired-feature",
            canonical=canonical,
        )
        _seed_graph_node(
            conn,
            node_id=canonical,
            subtype="open_spec_change",
        )
        _seed_predecessor_watermark(conn, source=predecessor)

    corpus = tmp_path / "corpus_stale_target"
    corpus.mkdir()
    (corpus / "note.md").write_text(textwrap.dedent("""\
        # Note

        See retired-feature for context.
    """))

    extr = tmp_path / "extractors_stale_target"
    extr.mkdir()
    (extr / "extractors.yaml").write_text(yaml.safe_dump({
        "parser": [{
            "format": "markdown",
            "class": "okg.substrate.library.extraction.parse.markdown.MarkdownParser",
        }],
        "chunker": [{
            "class": "okg.substrate.library.extraction.chunk.window.WindowChunker",
            "config": {"max_chars": 1500, "overlap": 100},
        }],
        "ner": [{
            "class": "okg.substrate.library.extraction.ner.regex_backend.RegexEntityExtractor",
            "config": {"rules": [
                ["change_slug", "[a-z]+-[a-z]+"],
            ]},
        }],
    }))

    runner = SourceRunner(clean_runtime, source_name=source_name)
    runner.run(
        DocCorpusSource(
            repo="test",
            repo_root=corpus, non_git_walk=True,
            extractors_dir=extr,
            include_globs=("**/*.md",),
            predecessor_source=predecessor,
            dsn=clean_runtime,
            mention_target_subtypes=("open_spec_change",),
        ),
        mode="scope_complete",
    )
    assert _graph_count(
        clean_runtime,
        table="okg.graph_edges",
        where="source = :source AND edge_type = 'mentions' AND dst = :dst",
        params={"source": source_name, "dst": canonical},
    ) >= 1

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _retract_graph_node(
            conn,
            node_id=canonical,
            subtype="open_spec_change",
        )

    runner.run(
        DocCorpusSource(
            repo="test",
            repo_root=corpus, non_git_walk=True,
            extractors_dir=extr,
            include_globs=("**/*.md",),
            predecessor_source=predecessor,
            dsn=clean_runtime,
            mention_target_subtypes=("open_spec_change",),
        ),
        mode="scope_complete",
    )

    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        cur.execute(
            "SELECT last_applied_rev FROM okg.source_watermarks "
            " WHERE source = %s",
            (source_name,),
        )
        cursor = cur.fetchone()[0]

    record_set = RecordSet.from_cursor(cursor["record_set"])
    copied_mentions = [
        edge
        for record in record_set.records.values()
        for edge in record.edges
        if edge.edge_type == "mentions" and edge.dst == canonical
    ]
    assert copied_mentions == []
    assert _graph_count(
        clean_runtime,
        table="okg.graph_edges",
        where="source = :source AND edge_type = 'mentions' AND dst = :dst",
        params={"source": source_name, "dst": canonical},
    ) == 0


def test_unchanged_record_set_with_current_target_takes_noop_fast_path(
    clean_runtime: str, tmp_path: Path,
) -> None:
    """overDeleteCheck (Wave B): the freshness fall-through must
    NOT make every unchanged file reprocess. When the `mentions`
    target is still present (current), a second `scope_complete`
    run over an unchanged corpus MUST still take the
    `full_unchanged_noop` short-circuit instead of re-running the
    parse/extract pipeline.
    """
    canonical = "change:still-here-feature"
    source_name = "test.alias.current-target"
    predecessor = "test.predecessor.current-target"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_cv_with_alias(
            conn,
            alias="still-here-feature",
            canonical=canonical,
        )
        _seed_graph_node(
            conn,
            node_id=canonical,
            subtype="open_spec_change",
        )
        _seed_predecessor_watermark(conn, source=predecessor)

    corpus = tmp_path / "corpus_current_target"
    corpus.mkdir()
    (corpus / "note.md").write_text(textwrap.dedent("""\
        # Note

        See still-here-feature for context.
    """))

    extr = tmp_path / "extractors_current_target"
    extr.mkdir()
    (extr / "extractors.yaml").write_text(yaml.safe_dump({
        "parser": [{
            "format": "markdown",
            "class": "okg.substrate.library.extraction.parse.markdown.MarkdownParser",
        }],
        "chunker": [{
            "class": "okg.substrate.library.extraction.chunk.window.WindowChunker",
            "config": {"max_chars": 1500, "overlap": 100},
        }],
        "ner": [{
            "class": "okg.substrate.library.extraction.ner.regex_backend.RegexEntityExtractor",
            "config": {"rules": [
                ["change_slug", "[a-z]+-[a-z]+-[a-z]+"],
            ]},
        }],
    }))

    def _make_source() -> DocCorpusSource:
        return DocCorpusSource(
            repo="test",
            repo_root=corpus, non_git_walk=True,
            extractors_dir=extr,
            include_globs=("**/*.md",),
            predecessor_source=predecessor,
            dsn=clean_runtime,
            mention_target_subtypes=("open_spec_change",),
        )

    runner = SourceRunner(clean_runtime, source_name=source_name)
    runner.run(_make_source(), mode="scope_complete")
    assert _graph_count(
        clean_runtime,
        table="okg.graph_edges",
        where="source = :source AND edge_type = 'mentions' AND dst = :dst",
        params={"source": source_name, "dst": canonical},
    ) >= 1

    # Second run over an UNCHANGED corpus with a STILL-CURRENT
    # target: the fast no-op short-circuit must fire.
    second = _make_source()
    runner.run(second, mode="scope_complete")
    assert (
        second.last_run_stats.get("scope_status") == "full_unchanged_noop"
    ), second.last_run_stats
    # And the mentions edge is preserved (nothing retracted).
    assert _graph_count(
        clean_runtime,
        table="okg.graph_edges",
        where="source = :source AND edge_type = 'mentions' AND dst = :dst",
        params={"source": source_name, "dst": canonical},
    ) >= 1


def test_no_alias_match_no_mentions_edge_only_member_of(
    clean_runtime: str, tmp_path: Path,
) -> None:
    """When no alias resolves, the mention NodeFact and its
    `member_of` edge still land — but NO `mentions` edge.
    The mention stays "dangling" until a future linker
    enriches it."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        # Apply a CV but populate NO aliases.
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO okg.catalog_versions (status, applied_at) "
                "VALUES ('applied', now())"
            )
        _seed_predecessor_watermark(
            conn, source="test.predecessor.bare",
        )

    corpus = tmp_path / "corpus2"
    corpus.mkdir()
    (corpus / "x.md").write_text("hello world\n\nadd-mystery-feature lives here.\n")

    extr = tmp_path / "extractors2"
    extr.mkdir()
    (extr / "extractors.yaml").write_text(yaml.safe_dump({
        "parser": [{
            "format": "markdown",
            "class": "okg.substrate.library.extraction.parse.markdown.MarkdownParser",
        }],
        "chunker": [{
            "class": "okg.substrate.library.extraction.chunk.window.WindowChunker",
            "config": {"max_chars": 1500, "overlap": 100},
        }],
        "ner": [{
            "class": "okg.substrate.library.extraction.ner.regex_backend.RegexEntityExtractor",
            "config": {"rules": [
                ["change_slug", "(?:add|fix)-[a-z0-9-]+"],
            ]},
        }],
    }))

    SourceRunner(
        clean_runtime, source_name="test.alias.bare",
    ).run(
        DocCorpusSource(
            repo="test", repo_root=corpus, non_git_walk=True,
            extractors_dir=extr,
            include_globs=("**/*.md",),
            predecessor_source="test.predecessor.bare",
            dsn=clean_runtime,
        ),
        mode="scope_complete",
    )

    n_mention_nodes = _graph_count(
        clean_runtime,
        table="okg.graph_nodes",
        where="source = :source AND subtype = 'entity_mention'",
        params={"source": "test.alias.bare"},
    )
    n_mentions_edges = _graph_count(
        clean_runtime,
        table="okg.graph_edges",
        where="source = :source AND edge_type = 'mentions'",
        params={"source": "test.alias.bare"},
    )
    assert n_mention_nodes >= 1, n_mention_nodes
    assert n_mentions_edges == 0, n_mentions_edges


def test_runner_reconciles_stale_target_without_mention_target_subtypes(
    clean_runtime: str, tmp_path: Path,
) -> None:
    """An empty `mention_target_subtypes` keeps ALL resolved aliases, so the
    source STILL emits cross-source `mentions` edges. `probe_short_circuit_safe`
    must therefore key off alias resolution being active (dsn set), not off the
    subtype set — otherwise the runner change-probe strands a retracted target
    here exactly as it would with the subtype set configured."""
    canonical = "change:no-subtype-feature"
    source_name = "test.alias.no-subtype"
    predecessor = "test.predecessor.no-subtype"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_cv_with_alias(conn, alias="no-subtype-feature", canonical=canonical)
        _seed_graph_node(conn, node_id=canonical, subtype="open_spec_change")
        _seed_predecessor_watermark(conn, source=predecessor)

    corpus = tmp_path / "corpus_no_subtype"
    corpus.mkdir()
    (corpus / "note.md").write_text(textwrap.dedent("""\
        # Note

        See no-subtype-feature for context.
    """))
    _write_mentions_extractors(tmp_path / "extractors_no_subtype")

    def _make_source() -> DocCorpusSource:
        # NOTE: mention_target_subtypes is deliberately NOT set.
        return DocCorpusSource(
            repo="test",
            repo_root=corpus, non_git_walk=True,
            extractors_dir=tmp_path / "extractors_no_subtype",
            include_globs=("**/*.md",),
            predecessor_source=predecessor,
            dsn=clean_runtime,
        )

    runner = SourceRunner(clean_runtime, source_name=source_name)
    runner.run(_make_source(), mode="scope_complete")
    assert _graph_count(
        clean_runtime,
        table="okg.graph_edges",
        where="source = :source AND edge_type = 'mentions' AND dst = :dst",
        params={"source": source_name, "dst": canonical},
    ) >= 1

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _retract_graph_node(conn, node_id=canonical, subtype="open_spec_change")

    second = _make_source()
    runner.run(second, mode="scope_complete")
    assert second.last_run_stats.get("scope_status") != "probe_short_circuit", (
        second.last_run_stats
    )
    assert _graph_count(
        clean_runtime,
        table="okg.graph_edges",
        where="source = :source AND edge_type = 'mentions' AND dst = :dst",
        params={"source": source_name, "dst": canonical},
    ) == 0


def _git_init_commit(repo: Path) -> None:
    """Make `repo` a git repo with one commit, so `_git_frontier()` returns a
    real frontier and `git_frontier_skip` can engage."""
    import os
    import subprocess

    env = {
        **os.environ,
        "GIT_AUTHOR_NAME": "t", "GIT_AUTHOR_EMAIL": "t@example.com",
        "GIT_COMMITTER_NAME": "t", "GIT_COMMITTER_EMAIL": "t@example.com",
    }
    subprocess.run(["git", "init", "-q"], cwd=repo, check=True, env=env)
    subprocess.run(["git", "add", "-A"], cwd=repo, check=True, env=env)
    subprocess.run(
        ["git", "commit", "-q", "-m", "init"], cwd=repo, check=True, env=env,
    )


def _write_mentions_extractors(extr: Path) -> None:
    extr.mkdir()
    (extr / "extractors.yaml").write_text(yaml.safe_dump({
        "parser": [{
            "format": "markdown",
            "class": "okg.substrate.library.extraction.parse.markdown.MarkdownParser",
        }],
        "chunker": [{
            "class": "okg.substrate.library.extraction.chunk.window.WindowChunker",
            "config": {"max_chars": 1500, "overlap": 100},
        }],
        "ner": [{
            "class": "okg.substrate.library.extraction.ner.regex_backend.RegexEntityExtractor",
            "config": {"rules": [["change_slug", "[a-z]+-[a-z]+-[a-z]+"]]},
        }],
    }))


def test_git_frontier_no_change_reconciles_stale_mentions_target(
    clean_runtime: str, tmp_path: Path,
) -> None:
    """The `git_frontier_no_change` fast path must NOT strand a stale
    cross-source `mentions` edge. An unchanged git frontier proves our own
    files didn't change, but a mention target retracted by another source is
    invisible to it — so the short-circuit must be declined and the stale edge
    reconciled away."""
    canonical = "change:git-retired-feature"
    source_name = "test.alias.git-stale-target"
    predecessor = "test.predecessor.git-stale-target"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_cv_with_alias(conn, alias="git-retired-feature", canonical=canonical)
        _seed_graph_node(conn, node_id=canonical, subtype="open_spec_change")
        _seed_predecessor_watermark(conn, source=predecessor)

    corpus = tmp_path / "corpus_git_stale"
    corpus.mkdir()
    (corpus / "note.md").write_text(textwrap.dedent("""\
        # Note

        See git-retired-feature for context.
    """))
    _write_mentions_extractors(tmp_path / "extractors_git_stale")
    _git_init_commit(corpus)

    def _make_source() -> DocCorpusSource:
        return DocCorpusSource(
            repo="test",
            repo_root=corpus, non_git_walk=True,
            extractors_dir=tmp_path / "extractors_git_stale",
            include_globs=("**/*.md",),
            predecessor_source=predecessor,
            dsn=clean_runtime,
            mention_target_subtypes=("open_spec_change",),
            git_frontier_skip=True,
        )

    runner = SourceRunner(clean_runtime, source_name=source_name)
    runner.run(_make_source(), mode="scope_complete")
    assert _graph_count(
        clean_runtime,
        table="okg.graph_edges",
        where="source = :source AND edge_type = 'mentions' AND dst = :dst",
        params={"source": source_name, "dst": canonical},
    ) >= 1

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _retract_graph_node(conn, node_id=canonical, subtype="open_spec_change")

    # Second run: git frontier unchanged (no new commit) but the target is
    # gone. The short-circuit must be declined and the stale edge retracted.
    second = _make_source()
    runner.run(second, mode="scope_complete")
    assert second.last_run_stats.get("scope_status") != "git_frontier_no_change", (
        second.last_run_stats
    )
    assert _graph_count(
        clean_runtime,
        table="okg.graph_edges",
        where="source = :source AND edge_type = 'mentions' AND dst = :dst",
        params={"source": source_name, "dst": canonical},
    ) == 0


def test_git_frontier_no_change_short_circuits_when_target_current(
    clean_runtime: str, tmp_path: Path,
) -> None:
    """When the mention target is still current, an unchanged git frontier MUST
    still take the `git_frontier_no_change` short-circuit — the freshness
    re-check declines only on a genuinely stale target, not every run."""
    canonical = "change:git-current-feature"
    source_name = "test.alias.git-current-target"
    predecessor = "test.predecessor.git-current-target"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_cv_with_alias(conn, alias="git-current-feature", canonical=canonical)
        _seed_graph_node(conn, node_id=canonical, subtype="open_spec_change")
        _seed_predecessor_watermark(conn, source=predecessor)

    corpus = tmp_path / "corpus_git_current"
    corpus.mkdir()
    (corpus / "note.md").write_text(textwrap.dedent("""\
        # Note

        See git-current-feature for context.
    """))
    _write_mentions_extractors(tmp_path / "extractors_git_current")
    _git_init_commit(corpus)

    def _make_source() -> DocCorpusSource:
        return DocCorpusSource(
            repo="test",
            repo_root=corpus, non_git_walk=True,
            extractors_dir=tmp_path / "extractors_git_current",
            include_globs=("**/*.md",),
            predecessor_source=predecessor,
            dsn=clean_runtime,
            mention_target_subtypes=("open_spec_change",),
            git_frontier_skip=True,
        )

    runner = SourceRunner(clean_runtime, source_name=source_name)
    runner.run(_make_source(), mode="scope_complete")

    second = _make_source()
    runner.run(second, mode="scope_complete")
    assert second.last_run_stats.get("scope_status") == "git_frontier_no_change", (
        second.last_run_stats
    )
    # Target still current: the mentions edge is preserved.
    assert _graph_count(
        clean_runtime,
        table="okg.graph_edges",
        where="source = :source AND edge_type = 'mentions' AND dst = :dst",
        params={"source": source_name, "dst": canonical},
    ) >= 1


def _graph_count(
    dsn: str,
    *,
    table: str,
    where: str,
    params: dict[str, object],
) -> int:
    from okg.substrate.graph_store import ChronosGraphStore

    with psycopg.connect(dsn, autocommit=True) as conn:
        rows = ChronosGraphStore.from_connection(
            conn,
            ensure_schema=False,
        ).query(
            f"SELECT count(*)::bigint AS n FROM {table} WHERE {where}",
            params,
        )
    return int(rows[0]["n"]) if rows else 0
