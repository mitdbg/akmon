"""The workspace-graph `code_text` source stores function text, and only that.

These tests install the real profile over a throwaway git repository, build
the source exactly as the registry describes it (same parameters, same shipped
`extractors.yaml`), and read the repository. No database is needed, so they
check what is stored, not how it is published.

What they hold:

- a word that appears only inside a function body is findable, in a chunk that
  names its symbol and its lines (the point of the source);
- credential files, vendored and generated code, and test code are never read;
- a language with no dedicated parser is stored as text, not as a document with
  no text;
- a file over the per-file cap is recorded, with no text;
- test code comes back when an operator deletes the two `tests` excludes.
"""
from __future__ import annotations

import subprocess
from pathlib import Path

import pytest
import yaml

from okg.substrate.catalog.profile import load_profile
from okg.substrate.deployment_bootstrap.profile_init import (
    build_plan,
    scaffold_deployment,
)
from okg.substrate.ingest.adapter_factory import source_adapter_init_params
from okg.substrate.library.sources.doc_corpus import (
    DocCorpusOversize,
    DocCorpusSource,
)
from okg.substrate.sources.registry import load_source_registry

PROFILE_DIR = (
    Path(__file__).resolve().parents[4]
    / "src/okg/substrate/library/profiles/workspace-graph"
)

BODY_ONLY_WORD = "zanzibarquux"
LONG_BODY_WORD = "tailmarkerxyzzy"


def _git(root: Path, *args: str) -> None:
    subprocess.run(
        [
            "git",
            "-c", "user.email=fixture@example.test",
            "-c", "user.name=OKG Fixture",
            "-c", "commit.gpgsign=false",
            *args,
        ],
        cwd=root,
        check=True,
        capture_output=True,
    )


def _write(root: Path, rel: str, text: str) -> None:
    path = root / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _fixture_repo(tmp_path: Path) -> Path:
    root = tmp_path / "workspace"
    root.mkdir()
    _write(
        root, "src/app.py",
        "import os\n"
        "\n"
        "\n"
        "def greet(name):\n"
        '    """Say hello."""\n'
        "    return 'hello ' + name\n"
        "\n"
        "\n"
        "def compute_total(values):\n"
        '    """Add the values."""\n'
        f"    marker = '{BODY_ONLY_WORD}'\n"
        "    return sum(values), marker\n",
    )
    # One function far longer than the 4,000-character window.
    long_lines = "".join(f"    step_{i} = {i} * 2\n" for i in range(600))
    _write(
        root, "src/long_function.py",
        "def very_long():\n" + long_lines + f"    return '{LONG_BODY_WORD}'\n",
    )
    _write(root, "web/app.ts", "export function render() { return 'tsbody'; }\n")
    # Never read: credentials, vendored and generated code, test code.
    _write(root, ".env", "TOKEN=hunter2secret\n")
    _write(root, "secrets/token.py", "TOKEN = 'secretsdirbody'\n")
    _write(root, "config/credentials.py", "KEY = 'credentialsbody'\n")
    _write(root, "vendor/lib.py", "VENDORED = 'vendoredbody'\n")
    _write(root, "proto/messages_pb2.py", "GENERATED = 'pb2body'\n")
    _write(root, "static/app.min.js", "var minified='minjsbody';\n")
    _write(root, "tests/test_app.py", "def test_greet():\n    assert 'testbody'\n")
    _write(root, "pkg/tests/test_inner.py", "def test_inner():\n    'innertestbody'\n")
    # Over the 1,000,000-byte per-file cap.
    _write(root, "src/huge.py", "X = 1\n" + "# pad\n" * 200_000)
    _git(root, "init", "-q")
    _git(root, "add", ".")
    _git(root, "commit", "-q", "-m", "fixture")
    return root


def _code_text_source(
    tmp_path: Path, root: Path, *, drop_excludes: tuple[str, ...] = (), **overrides,
) -> DocCorpusSource:
    profile = load_profile(PROFILE_DIR / "profile.yaml")
    plan = build_plan(
        profile=profile,
        deployment_name="demo_workspace",
        deployments_root=tmp_path / "deployments",
        raw_answers={
            "deployment_name": "demo_workspace",
            "postgres_dsn": "postgres://postgres@localhost:5433/demo_workspace",
            "repo_root": str(root),
            "since": "all",
            "mcp_port": "18888",
            "codex_sessions_root": str(root / ".codex" / "sessions"),
            "codex_archived_sessions_root": str(root / ".codex" / "archived"),
        },
    )
    deployment_dir = scaffold_deployment(plan).deployment_dir
    registry_path = deployment_dir / "source_registry.yaml"
    if drop_excludes:
        registry = yaml.safe_load(registry_path.read_text(encoding="utf-8"))
        params = registry["sources"]["code_text"]["params"]
        params["exclude_globs"] = [
            glob for glob in params["exclude_globs"] if glob not in drop_excludes
        ]
        registry_path.write_text(
            yaml.safe_dump(registry, sort_keys=False), encoding="utf-8",
        )
    entry = load_source_registry(registry_path)["code_text"]
    params = source_adapter_init_params(
        entry, dsn="unused", deployment="demo_workspace",
    )
    # No database here: nothing to check a predecessor against or to link to.
    params["dsn"] = None
    params["predecessor_source"] = None
    params.update(overrides)
    return DocCorpusSource(**params)


def _read(source: DocCorpusSource):
    # `run.facts` is a one-shot stream: read it once.
    facts = list(source.run("run-1", mode="scope_complete").facts)
    documents = {
        fact.attrs["path"]: fact
        for fact in facts
        if getattr(fact, "subtype", None) == "document"
    }
    chunks = [
        fact for fact in facts
        if getattr(fact, "subtype", None) == "document_chunk"
    ]
    return documents, chunks


def _chunks_for(chunks, path: str):
    # A chunk names its file through its citation label, `<repo>/<path>`.
    return [
        c for c in chunks
        if c.attrs.get("citation_label") == f"demo_workspace/{path}"
    ]


def _all_text(chunks) -> str:
    return "\n".join(str(c.attrs.get("text") or "") for c in chunks)


def test_a_word_found_only_inside_a_function_body_is_stored_with_its_symbol(
    tmp_path: Path,
) -> None:
    documents, chunks = _read(_code_text_source(tmp_path, _fixture_repo(tmp_path)))

    hits = [c for c in chunks if BODY_ONLY_WORD in str(c.attrs.get("text"))]

    assert len(hits) == 1
    hit = hits[0]
    assert hit.attrs["symbol"] == "compute_total"
    # The function is lines 9 to 12 of src/app.py. The chunk may also carry
    # the blank lines before the `def` until chunk starts are trimmed to the
    # `def` (a separate change), so only the end is exact.
    assert hit.attrs["start_line"] <= 9
    assert hit.attrs["end_line"] == 12
    assert hit.attrs["citation_label"] == "demo_workspace/src/app.py"
    assert "src/app.py" in documents


def test_a_function_longer_than_one_window_is_stored_whole_in_named_windows(
    tmp_path: Path,
) -> None:
    """No per-function cap: the tail of a long function is still stored."""
    _documents, chunks = _read(_code_text_source(tmp_path, _fixture_repo(tmp_path)))

    long_chunks = [c for c in chunks if c.attrs.get("symbol") == "very_long"]
    stored = _all_text(long_chunks)

    assert len(long_chunks) > 1
    assert all(len(str(c.attrs["text"])) <= 4_000 for c in long_chunks)
    assert LONG_BODY_WORD in stored
    assert "step_599 = 599 * 2" in stored


def test_a_language_without_a_dedicated_parser_is_stored_as_text(
    tmp_path: Path,
) -> None:
    documents, chunks = _read(_code_text_source(tmp_path, _fixture_repo(tmp_path)))

    assert "web/app.ts" in documents
    assert "tsbody" in _all_text(chunks)


@pytest.mark.parametrize(
    ("hidden", "path"),
    [
        ("hunter2secret", ".env"),
        ("secretsdirbody", "secrets/token.py"),
        ("credentialsbody", "config/credentials.py"),
        ("vendoredbody", "vendor/lib.py"),
        ("pb2body", "proto/messages_pb2.py"),
        ("minjsbody", "static/app.min.js"),
        ("testbody", "tests/test_app.py"),
        ("innertestbody", "pkg/tests/test_inner.py"),
    ],
)
def test_credentials_vendored_generated_and_test_code_are_not_read(
    tmp_path: Path, hidden: str, path: str,
) -> None:
    documents, chunks = _read(_code_text_source(tmp_path, _fixture_repo(tmp_path)))

    # The source reads the rest of the repository; only this path is left out.
    assert "src/app.py" in documents and chunks
    assert path not in documents
    assert hidden not in _all_text(chunks)


def test_a_file_over_the_per_file_cap_is_recorded_without_text(
    tmp_path: Path,
) -> None:
    source = _code_text_source(tmp_path, _fixture_repo(tmp_path))

    documents, chunks = _read(source)

    assert "src/huge.py" in documents
    assert "oversize" in documents["src/huge.py"].attrs["parse_warnings"]
    assert not _chunks_for(chunks, "src/huge.py")
    assert "src/huge.py" in source.last_run_stats["files_recorded_oversize_sample"]


def test_test_code_comes_back_when_the_two_tests_excludes_are_deleted(
    tmp_path: Path,
) -> None:
    """The documented way to store test code too."""
    source = _code_text_source(
        tmp_path, _fixture_repo(tmp_path),
        drop_excludes=("tests/**", "**/tests/**"),
    )

    documents, chunks = _read(source)

    assert {"tests/test_app.py", "pkg/tests/test_inner.py"} <= set(documents)
    assert "testbody" in _all_text(chunks)
    # Only the two lines were deleted: the other excludes still hold.
    assert ".env" not in documents
    assert "vendor/lib.py" not in documents


def test_the_shipped_cap_refuses_a_corpus_over_64_mib(tmp_path: Path) -> None:
    """The profile's own `max_total_bytes` is wired to the reader."""
    root = _fixture_repo(tmp_path)
    source = _code_text_source(tmp_path, root)
    assert source.max_total_bytes == 64 * 1024 * 1024

    over = _code_text_source(tmp_path / "again", root, max_total_bytes=500)
    with pytest.raises(DocCorpusOversize) as raised:
        over.run("run-1", mode="scope_complete")

    assert raised.value.data["max_total_bytes"] == 500
    assert raised.value.data["largest_directories"][0]["path"] == "src"
