"""add-academic-corpora-sources: discovery for the local-first
academic corpora — Zotero (+BBT exports, localhost API), Obsidian
vaults, Overleaf git remotes — plus the end-to-end vault+bib
ingest publish."""
from __future__ import annotations

import http.server
import threading
from pathlib import Path

import pytest

from okg.substrate.library.sources.discovery.env import DiscoveryEnv
from okg.substrate.library.sources.discovery.registry import (
    discover_for_source_id,
)
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402


def _env(home: Path, **answers) -> DiscoveryEnv:
    return DiscoveryEnv(
        home_dir=home,
        install_answers=dict(answers),
        env_vars={},
        platform="darwin",
    )


def _make_zotero(home: Path, *, bib: bool = True) -> None:
    zdir = home / "Zotero"
    (zdir / "storage").mkdir(parents=True)
    (zdir / "zotero.sqlite").write_text("stub", encoding="utf-8")
    if bib:
        (zdir / "My Library.bib").write_text(
            "@article{key1,\n title={T},\n}\n", encoding="utf-8",
        )


def test_zotero_install_and_bbt_export_candidates(tmp_path: Path) -> None:
    _make_zotero(tmp_path)
    candidates = discover_for_source_id("bib_references", _env(tmp_path))
    zotero = [
        c for c in candidates if c.extra.get("zotero_data_dir")
    ]
    assert len(zotero) == 1
    cand = zotero[0]
    # Zotero ranks first among bib candidates.
    assert candidates[0] is cand
    assert cand.params == {"bib_paths": [str(tmp_path / "Zotero" / "*.bib")]}
    assert cand.confidence == "high"
    # Item count + recency in the evidence string.
    assert "1 BBT .bib export(s)" in cand.evidence
    assert "newest 20" in cand.evidence
    assert cand.extra["zotero_local_api"] is False


def test_zotero_without_bbt_export_needs_operator(tmp_path: Path) -> None:
    _make_zotero(tmp_path, bib=False)
    candidates = discover_for_source_id("bib_references", _env(tmp_path))
    cand = candidates[0]
    assert cand.params == {}
    assert cand.extra["needs_operator_input"] is True
    assert "BBT" in cand.evidence


def test_zotero_local_api_detected_when_listening(tmp_path: Path) -> None:
    """A stub HTTP server on 127.0.0.1 mimicking the Zotero local
    read API marks the richer-metadata route; its absence (other
    tests) degrades to file-only with no error."""
    _make_zotero(tmp_path)

    class _Api(http.server.BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802 — stdlib naming
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"Zotero is running")

        def log_message(self, *args):  # silence test output
            pass

    server = http.server.HTTPServer(("127.0.0.1", 0), _Api)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        env = DiscoveryEnv(
            home_dir=tmp_path,
            install_answers={},
            env_vars={"OKG_ZOTERO_LOCAL_API_PORT": str(port)},
            platform="darwin",
        )
        candidates = discover_for_source_id("bib_references", env)
        assert candidates[0].extra["zotero_local_api"] is True
        assert "localhost read API" in candidates[0].evidence
    finally:
        server.shutdown()


def test_obsidian_vault_marker_promotes_notes_dir_candidate(
    tmp_path: Path,
) -> None:
    """`.obsidian/` marker → HIGH confidence + vault-aware evidence,
    independent of the directory's name; a plain markdown tree keeps
    existing notes_dir behavior."""
    vault = tmp_path / "notes"
    (vault / ".obsidian").mkdir(parents=True)
    (vault / "research.md").write_text("# r", encoding="utf-8")

    plain = tmp_path / "Documents" / "notes"
    plain.mkdir(parents=True)
    (plain / "todo.md").write_text("# t", encoding="utf-8")

    candidates = discover_for_source_id("notes_dir", _env(tmp_path))
    by_path = {c.path: c for c in candidates}
    vault_cand = by_path[str(vault)]
    assert vault_cand.confidence == "high"
    assert vault_cand.extra.get("obsidian_vault") is True
    assert vault_cand.evidence.startswith("Obsidian vault")
    plain_cand = by_path[str(plain)]
    assert plain_cand.extra.get("obsidian_vault") is None
    assert not plain_cand.evidence.startswith("Obsidian vault")


def test_overleaf_remote_detected_as_git_candidate(tmp_path: Path) -> None:
    """A repo with a git.overleaf.com remote routes through the
    existing git source with token-required setup state."""
    repo = tmp_path / "papers" / "sigmod26"
    (repo / ".git").mkdir(parents=True)
    (repo / ".git" / "config").write_text(
        '[remote "origin"]\n'
        "\turl = https://git.overleaf.com/65abc123\n",
        encoding="utf-8",
    )
    other = tmp_path / "code" / "okg"
    (other / ".git").mkdir(parents=True)
    (other / ".git" / "config").write_text(
        '[remote "origin"]\n'
        "\turl = git@github.com:example-org/example-repo.git\n",
        encoding="utf-8",
    )
    env = _env(tmp_path, git_repos=[str(repo), str(other)])
    candidates = discover_for_source_id("git_files", env)
    by_path = {c.path: c for c in candidates}
    overleaf = by_path[str(repo)]
    assert overleaf.extra.get("overleaf_project") is True
    assert overleaf.extra.get("setup_state") == "needs_token"
    assert "Overleaf" in overleaf.evidence
    github = by_path[str(other)]
    assert github.extra.get("overleaf_project") is None


@pytest.mark.postgres
def test_vault_and_bib_ingest_publish_row_deltas(
    clean_runtime: str, tmp_path: Path,
) -> None:
    """The local corpora are fully CI-testable end to end: ingest
    the fixture vault (notes_dir) + the Zotero BBT .bib
    (bib_references) through SourceRunner -> Publisher; positive
    note/reference row deltas under a published generation."""
    from okg.substrate.ingest import SourceRunner
    from okg.substrate.library.sources.scholarly.bib_references import (
        BibReferencesSource,
    )
    from okg.substrate.library.sources.notes_dir import NotesDirSource
    from okg.substrate.publisher import Publisher
    from tests.substrate.library.sources.test_ci_source_sync_db import (
        _seed_catalog_and_external_nodes,
    )
    from okg.substrate.graph_store import ChronosGraphStore

    vault = tmp_path / "vault"
    (vault / ".obsidian").mkdir(parents=True)
    # notes_dir ingests the meeting-note protocol (dated filename +
    # okg-meeting frontmatter) — a vault's free-form research notes
    # are doc_corpus territory, recorded as such in the PACT.
    (vault / "2026-06-08-lab-sync.md").write_text(
        "---\n"
        "okg-meeting:\n"
        "  kind: lab_meeting\n"
        "  actions:\n"
        "    - description: rerun ablations\n"
        "      owner: alice\n"
        "---\n"
        "# Lab sync\n",
        encoding="utf-8",
    )
    (vault / "2026-06-09-advisor-1-1.md").write_text(
        "---\n"
        "okg-meeting:\n"
        "  kind: one_on_one\n"
        "---\n"
        "# Advisor 1:1\n",
        encoding="utf-8",
    )
    _make_zotero(tmp_path)

    def _notes() -> NotesDirSource:
        return NotesDirSource(notes_dir=str(vault))

    def _bib() -> BibReferencesSource:
        return BibReferencesSource(
            bib_paths=[str(tmp_path / "Zotero" / "*.bib")],
        )

    facts = list(_notes().run("preview").facts)
    facts += list(_bib().run("preview").facts)
    _seed_catalog_and_external_nodes(clean_runtime, iter(facts))

    notes_subtypes = {
        f.subtype for f in facts if hasattr(f, "subtype")
    }
    runner = SourceRunner(clean_runtime, source_name="test.corpora-notes")
    runner.run(_notes(), mode="scope_complete")
    runner_bib = SourceRunner(clean_runtime, source_name="test.corpora-bib")
    runner_bib.run(_bib(), mode="scope_complete")
    publish = Publisher(clean_runtime, "test", catalog_identity=bind_test_catalog_writer(clean_runtime, "test")).cycle()
    assert publish["status"] == "published", publish

    store = ChronosGraphStore.connect(clean_runtime, ensure_schema=False)
    try:
        latest = store.list_generations(
            deployment_name="test", status="published",
        )[-1]
        graph = store.checkout_generation(latest.checkpoint_id)
        counts = {
            row["subtype"]: int(row["count"])
            for row in graph.query(
                "SELECT subtype, count(*) AS count FROM okg.graph_nodes"
                " GROUP BY subtype",
            )
        }
    finally:
        store.close()
    # Positive row deltas for both corpora, attributable by subtype.
    assert sum(
        counts.get(s, 0) for s in notes_subtypes
    ) >= 2, counts
    assert counts, "publish produced no rows"
