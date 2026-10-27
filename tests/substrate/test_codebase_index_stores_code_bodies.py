"""Every code file the codebase-index template matches carries its source.

An operator opened a record on a live deployment and asked why they could see
only metadata. They were right: `git-files` stores a file's path, size and
language and none of its bytes, and the template's document source matched only
`**/*.py` among source extensions. Searching `bash`, `FROM`, `apt` or `golang`
returned zero on a graph whose Dockerfile, shell script and go.mod it listed.

The trap this file exists to hold shut: widening the globs is NOT sufficient.
Format dispatch knows `.py` and nothing else among source extensions, and the
tree-sitter parsers need per-language grammars that are an optional install. A
matched file with no parser becomes a `document` node with zero chunks -- the
graph gains a record that stores nothing, which is worse than not matching it.
Measured before the fix: a shell script and a Go file produced 2 documents and
0 chunks between them.
"""
from __future__ import annotations

import re
import subprocess
from pathlib import Path

import psycopg
import pytest

from okg.substrate.ingest import SourceRunner
from okg.substrate.library.sources.doc_corpus import (
    DEFAULT_EXTENSION_TO_FORMAT,
    DocCorpusSource,
    _chunk_node_id,
)
from okg.substrate.mcp import open_session, search
from okg.substrate.publisher import Publisher
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer
from tests.substrate._db_helpers import seed_catalog_version

TEMPLATE = (
    Path(__file__).resolve().parents[2]
    / "src/okg/substrate/library/templates/codebase-index"
)
REGISTRY = TEMPLATE / "source_registry.yaml"


def _doc_corpus_block() -> str:
    text = REGISTRY.read_text(encoding="utf-8")
    return text.split("  doc-corpus:", 1)[1].split("\n  notes-doc-corpus:", 1)[0]


def _glob_list(key: str) -> list[str]:
    """The YAML list under `key`, read without a Jinja-aware YAML loader.

    The template carries `{{ }}` placeholders, so it is not loadable YAML.
    """
    body = _doc_corpus_block().split(f"{key}:", 1)[1]
    out: list[str] = []
    for line in body.splitlines()[1:]:
        stripped = line.strip()
        if stripped.startswith("- "):
            out.append(stripped[2:].strip().strip('"'))
        elif stripped and not stripped.startswith("#"):
            break
    return out


def _extension_to_format() -> dict[str, str]:
    body = _doc_corpus_block().split("extension_to_format:", 1)[1]
    out: dict[str, str] = {}
    for line in body.splitlines()[1:]:
        stripped = line.strip()
        # A suffix (".go") or a file-name pattern ("Dockerfile.*").
        match = re.match(r'^"([^"]+)"\s*:\s*"([^"]+)"$', stripped)
        if match:
            out[match.group(1)] = match.group(2)
        elif stripped and not stripped.startswith("#"):
            break
    return out


def _code_extensions() -> list[str]:
    """Source extensions the template's globs match."""
    prose_and_config = {".md", ".txt", ".rst", ".json", ".yaml", ".yml", ".toml"}
    out = []
    for glob in _glob_list("include_globs"):
        match = re.fullmatch(r"\*\*/\*(\.[A-Za-z0-9]+)", glob)
        if match and match.group(1) not in prose_and_config:
            out.append(match.group(1))
    return out


def test_the_template_matches_source_beyond_python() -> None:
    extensions = set(_code_extensions())

    assert ".py" in extensions
    # The languages a polyglot repository actually contains.
    for ext in (".go", ".sh", ".ts", ".rs", ".java", ".sql"):
        assert ext in extensions, f"{ext} is not matched by the template"


def test_every_matched_code_extension_can_actually_be_parsed() -> None:
    """The regression: a glob that matches a file no parser handles.

    Such a file becomes a document with no chunks -- a record that stores
    nothing. Each matched extension must reach a parser, either through the
    built-in dispatch or through the template's own override.
    """
    overrides = _extension_to_format()
    unroutable = [
        ext for ext in _code_extensions()
        if ext not in DEFAULT_EXTENSION_TO_FORMAT and ext not in overrides
    ]

    assert unroutable == [], (
        "these extensions are matched but reach no parser, so they would "
        f"produce documents with no body: {unroutable}"
    )


def test_python_keeps_its_own_parser() -> None:
    # `.py` routes to the Python AST parser, which needs no optional grammar
    # and extracts symbols. Overriding it to plain text would lose that.
    assert DEFAULT_EXTENSION_TO_FORMAT[".py"] == "code"
    assert ".py" not in _extension_to_format()


def test_paths_whose_contents_are_credentials_are_excluded() -> None:
    excludes = set(_glob_list("exclude_globs"))

    for glob in (
        "**/.env", "**/*.pem", "**/*.key", "**/secrets/**",
        "**/id_rsa*", "**/.netrc", "**/credentials/**",
    ):
        assert glob in excludes, f"{glob} is not excluded from stored bodies"


def test_body_exclusions_are_stricter_than_identity_exclusions() -> None:
    """The body source excludes more than the source that lists the file.

    A credential path stays a `source_file` node -- the graph may say the file
    exists -- while its bytes are never stored and never indexed.
    """
    body_excludes = set(_glob_list("exclude_globs"))
    identity_block = REGISTRY.read_text(encoding="utf-8").split(
        "exclude_globs: &repo_excludes", 1,
    )[1]
    identity_excludes = set()
    for line in identity_block.splitlines()[1:]:
        stripped = line.strip()
        if stripped.startswith("- "):
            identity_excludes.add(stripped[2:].strip().strip('"'))
        elif stripped and not stripped.startswith("#"):
            break

    assert identity_excludes < body_excludes
    assert "**/.env" not in identity_excludes


@pytest.mark.parametrize(
    ("name", "body", "needle"),
    [
        ("run.sh", "#!/bin/bash\nset -euo pipefail\necho hello\n", "bash"),
        ("main.go", 'package main\n\nimport "fmt"\n', "package"),
        ("schema.sql", "SELECT relname FROM pg_class;\n", "relname"),
    ],
)
def test_a_matched_code_file_carries_its_source(
    tmp_path: Path, name: str, body: str, needle: str,
) -> None:
    """End to end over the real connector: bytes in, bytes out."""
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / name).write_text(body, encoding="utf-8")

    run = DocCorpusSource(
        repo="t",
        repo_root=repo, non_git_walk=True,
        extractors_dir=TEMPLATE,
        include_globs=tuple(_glob_list("include_globs")),
        exclude_globs=tuple(_glob_list("exclude_globs")),
        extension_to_format=_extension_to_format(),
        max_files=10,
        max_bytes_per_file=1_000_000,
    ).run("probe", mode="scope_complete")

    chunks = [
        fact for fact in run.facts
        if getattr(fact, "subtype", None) == "document_chunk"
    ]
    stored = "\n".join(str((c.attrs or {}).get("text") or "") for c in chunks)

    assert chunks, f"{name} produced no chunk: its source was not stored"
    assert needle in stored


@pytest.mark.postgres
def test_published_code_bodies_are_lexically_searchable(
    clean_runtime: str,
    tmp_path: Path,
) -> None:
    """The template's bodies survive a real publish and reach search."""
    repo = tmp_path / "repo"
    repo.mkdir()
    files = {
        "run.sh": "#!/bin/bash\necho shellbodymarker\n",
        "main.go": 'package main\n\nconst GoBodyMarker = "gobodymarker"\n',
        "schema.sql": "SELECT 'sqlbodymarker';\n",
    }
    for name, body in files.items():
        (repo / name).write_text(body, encoding="utf-8")
    secrets = repo / "secrets"
    secrets.mkdir()
    (secrets / "hidden.sh").write_text(
        "#!/bin/bash\necho excludedbodymarker\n",
        encoding="utf-8",
    )

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)

    SourceRunner(clean_runtime, source_name="test.code-bodies").run(
        DocCorpusSource(
            repo="code-bodies",
            repo_root=repo, non_git_walk=True,
            extractors_dir=TEMPLATE,
            include_globs=tuple(_glob_list("include_globs")),
            exclude_globs=tuple(_glob_list("exclude_globs")),
            extension_to_format=_extension_to_format(),
            max_files=10,
            max_bytes_per_file=1_000_000,
        ),
        mode="scope_complete",
    )
    published = Publisher(
        clean_runtime,
        "default",
        catalog_identity=bind_test_catalog_writer(clean_runtime, "default"),
    ).cycle()
    assert published["status"] == "published", published

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name="default")
        for name, needle in (
            ("run.sh", "shellbodymarker"),
            ("main.go", "gobodymarker"),
            ("schema.sql", "sqlbodymarker"),
        ):
            found = search(
                conn,
                session,
                needle,
                method="lexical",
                filter={"subtype": "document_chunk"},
                limit=10,
            )
            assert found["results"], (needle, found)
            assert _chunk_node_id(files[name]) in {
                row["node_id"] for row in found["results"]
            }, (needle, found)

        excluded = search(
            conn,
            session,
            "excludedbodymarker",
            method="lexical",
            filter={"subtype": "document_chunk"},
            limit=10,
        )
        assert excluded["results"] == [], excluded


# -- Python bodies and unparseable files (#2671) ------------------------------
#
# The template once kept only the Python parser's docstring chunks, so an
# identifier used inside a function body was unsearchable, and a file whose
# parse failed (Python 2, templated YAML) was stored with no text at all.

QUOTA_MODULE = (
    '"""Quota helpers."""\n\n\n'
    "def compute_quota(site):\n"
    '    """Return the quota for a site."""\n'
    "    limit = body_only_quota_marker(site)\n"
    "    return limit\n"
)
# Too large for one chunk, so it is split into its header and one chunk per
# method.
LARGE_CLASS = "class Scheduler:\n    \"\"\"Places jobs.\"\"\"\n\n" + "".join(
    f"    def step_{i}(self):\n"
    + "".join(f"        value_{i}_{j} = {j}\n" for j in range(60))
    + f"        return value_{i}_0\n\n"
    for i in range(4)
)
PYTHON2 = "print 'python2_only_marker'\n"


def _template_text_fallback() -> bool:
    match = re.search(r"^\s+text_fallback:\s*(\S+)\s*$", _doc_corpus_block(), re.M)
    assert match, "the template's doc-corpus block does not set text_fallback"
    return match.group(1) == "true"


def _template_source(repo: Path, *, text_fallback: bool) -> DocCorpusSource:
    return DocCorpusSource(
        repo="t",
        repo_root=repo, non_git_walk=True,
        extractors_dir=TEMPLATE,
        include_globs=tuple(_glob_list("include_globs")),
        exclude_globs=tuple(_glob_list("exclude_globs")),
        extension_to_format=_extension_to_format(),
        max_files=10,
        max_bytes_per_file=1_000_000,
        text_fallback=text_fallback,
    )


def _python_repo(tmp_path: Path) -> Path:
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "quota.py").write_text(QUOTA_MODULE, encoding="utf-8")
    (repo / "scheduler.py").write_text(LARGE_CLASS, encoding="utf-8")
    (repo / "legacy.py").write_text(PYTHON2, encoding="utf-8")
    return repo


def test_the_template_turns_on_text_fallback() -> None:
    assert _template_text_fallback() is True


def test_the_template_chunks_python_by_symbol() -> None:
    from okg.substrate.library.extraction.chunk.python_symbols import (
        PythonSymbolChunker,
    )
    from okg.substrate.library.extraction.registry import load_extractors

    chunker = load_extractors(TEMPLATE).chunker_for_format("code")
    assert isinstance(chunker, PythonSymbolChunker)


def test_the_template_stores_python_bodies_and_python2_text(tmp_path: Path) -> None:
    repo = _python_repo(tmp_path)
    run = _template_source(
        repo, text_fallback=_template_text_fallback(),
    ).run("probe", mode="scope_complete")
    facts = list(run.facts)
    chunks = [f for f in facts if getattr(f, "subtype", None) == "document_chunk"]
    texts = [str((c.attrs or {}).get("text") or "") for c in chunks]

    # A function body is stored, not only its docstring.
    assert any("body_only_quota_marker" in t for t in texts), texts
    # A large class is split per method.
    headings = {
        str((c.attrs or {}).get("heading_path") or "") for c in chunks
    }
    for i in range(4):
        assert f"Scheduler / step_{i}" in headings, headings
    # A Python 2 file does not parse; it is stored as text, and says so.
    assert any("python2_only_marker" in t for t in texts), texts
    legacy = [
        f for f in facts
        if getattr(f, "subtype", None) == "document"
        and (f.attrs or {}).get("path") == "legacy.py"
    ]
    assert legacy, "no document for legacy.py"
    warnings = (legacy[0].attrs or {}).get("parse_warnings") or []
    assert "text_fallback:code" in warnings, warnings


def test_without_text_fallback_a_python2_file_has_no_text(tmp_path: Path) -> None:
    """The failure the template default removes."""
    repo = _python_repo(tmp_path)
    run = _template_source(repo, text_fallback=False).run(
        "probe", mode="scope_complete",
    )
    texts = [
        str((f.attrs or {}).get("text") or "") for f in run.facts
        if getattr(f, "subtype", None) == "document_chunk"
    ]
    assert not any("python2_only_marker" in t for t in texts), texts
    assert any("body_only_quota_marker" in t for t in texts), texts


@pytest.mark.postgres
def test_published_python_bodies_are_lexically_searchable(
    clean_runtime: str,
    tmp_path: Path,
) -> None:
    """A body-only identifier and a Python 2 file survive a real publish."""
    repo = _python_repo(tmp_path)

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)

    SourceRunner(clean_runtime, source_name="test.python-bodies").run(
        _template_source(repo, text_fallback=_template_text_fallback()),
        mode="scope_complete",
    )
    published = Publisher(
        clean_runtime,
        "default",
        catalog_identity=bind_test_catalog_writer(clean_runtime, "default"),
    ).cycle()
    assert published["status"] == "published", published

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name="default")
        for needle in ("body_only_quota_marker", "python2_only_marker"):
            found = search(
                conn,
                session,
                needle,
                method="lexical",
                filter={"subtype": "document_chunk"},
                limit=10,
            )
            assert found["results"], (needle, found)


# -- A symbol's stored lines read the symbol's source (#3982) ----------------

_BIG_BODY = "".join(f"    v{j} = {j}  # pad pad pad pad pad\n" for j in range(130))
SYMBOL_LINES_MODULE = (
    '"""Module doc."""\n'
    "import os\n"
    "\n\n\n\n"
    "# comment above first\n"
    "@staticmethod\n"
    "@other(1,\n"
    "       2)\n"
    "def first(x):\n"
    "    return x\n"
    "\n\n\n"
    "X = 1\n"
    "\n"
    "class Klass:\n"
    '    """Doc."""\n'
    "\n"
    "    @property\n"
    "    def prop(self):\n"
    "        return 1\n"
    "\n"
    "    # note\n"
    "    def plain(self):\n"
    "        return 2\n"
    "\n\n"
    "async def last():\n"
    "    pass\n"
    "\n\n"
    "def big():\n"
    + _BIG_BODY
    + "    return 1\n"
    "\n\n" + "\n" * 300
    + "def after_long_gap():\n"
    "    return 9\n"
)


@pytest.mark.postgres
def test_a_symbols_stored_lines_read_exactly_its_source(
    clean_runtime: str,
    tmp_path: Path,
) -> None:
    """The range the text pointer names (`_symbol_line_range`, from the
    start and end line a `code_structure` symbol stores: the same `ast`
    line numbers `_analyze_python` returns) reads exactly that slice of the
    source from a real `PythonSymbolChunker` document of the codebase-index
    template. Covers a decorated function (the range starts at its `def`
    line, the decorators are not in it), a class method, a function split
    across chunks, and a definition after a long blank gap."""
    from okg.substrate.library.sources._code_graph_common import _analyze_python
    from okg.substrate.mcp.record_text import _symbol_line_range, read_record_text

    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "m.py").write_text(SYMBOL_LINES_MODULE, encoding="utf-8")
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)
    SourceRunner(clean_runtime, source_name="test.symbol-lines").run(
        _template_source(repo, text_fallback=_template_text_fallback()),
        mode="scope_complete",
    )
    published = Publisher(
        clean_runtime, "default",
        catalog_identity=bind_test_catalog_writer(clean_runtime, "default"),
    ).cycle()
    assert published["status"] == "published", published

    source_lines = SYMBOL_LINES_MODULE.split("\n")
    fence = re.compile(r"\u27e6UNTRUSTED_DATA[^\u27e7]*\u27e7\n|\n?\u27e6/UNTRUSTED_DATA\u27e7")
    symbols = {
        symbol.qname: symbol
        for symbol in _analyze_python("t", "m.py", SYMBOL_LINES_MODULE).symbols
    }
    for name in ("first", "Klass", "Klass.prop", "Klass.plain", "last", "big",
                 "after_long_gap"):
        assert name in symbols, sorted(symbols)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name="default")
        pieces = {}
        for name, symbol in symbols.items():
            record = {"subtype": "code_symbol", "attrs": {
                "start_line": symbol.start_line, "end_line": symbol.end_line}}
            lines = _symbol_line_range(record)
            assert lines == [[symbol.start_line, symbol.end_line]], name
            page = read_record_text(conn, session, "doc:t:m.py", lines=lines)
            assert page["status"] == "ok", (name, page)
            assert page["lines"]["basis"] == "source", (name, page["lines"])
            pieces[name] = len(page["items"])
            got = fence.sub("", "".join(item["text"] for item in page["items"]))
            want = "\n".join(source_lines[symbol.start_line - 1:symbol.end_line])
            assert got.rstrip("\n") == want.rstrip("\n"), name
    # The decorated function's range starts at the `def` line.
    first = symbols["first"]
    assert source_lines[first.start_line - 1].startswith("def first")
    assert source_lines[first.start_line - 2].strip() == "2)"
    assert source_lines[symbols["Klass.prop"].start_line - 1].strip() == "def prop(self):"
    # The long function is served from several chunks; a method from one.
    assert pieces["big"] > 1, pieces
    assert pieces["Klass.prop"] == 1, pieces
    assert symbols["big"].end_line - symbols["big"].start_line > 120


# -- Build and deploy files without a telling extension (#2671) --------------

BUILD_FILES = {
    "Dockerfile": "FROM python:3.12\nRUN pip install dockerfile_body_marker\n",
    "deploy/Dockerfile.prod": "FROM alpine\nRUN apk add prod_dockerfile_marker\n",
    "Makefile": "build:\n\tcc -o makefile_body_marker main.c\n",
    "rules.mk": "CFLAGS += -Dmk_include_marker\n",
    "Jenkinsfile": "pipeline { stages { stage('jenkins_body_marker') {} } }\n",
    "chart/values.yaml.j2": "replicas: {{ jinja_template_marker }}\n",
}


def _name_globs() -> list[str]:
    """Include globs whose last part is a file name, not `*.ext`."""
    return [
        glob for glob in _glob_list("include_globs")
        if not re.fullmatch(r"\*\*/\*\.[A-Za-z0-9]+", glob)
    ]


def test_the_template_matches_build_and_deploy_files() -> None:
    globs = set(_glob_list("include_globs"))
    for glob in (
        "**/*.j2", "**/*.tpl", "**/*.tmpl", "**/Dockerfile", "**/Dockerfile.*",
        "**/Makefile", "**/*.mk", "**/Jenkinsfile",
    ):
        assert glob in globs, f"{glob} is not matched by the template"


def test_every_matched_file_name_has_a_route() -> None:
    """A name glob with no route stores a document with no text."""
    routes = _extension_to_format()
    unroutable = [
        glob for glob in _name_globs()
        if glob.rsplit("/", 1)[-1] not in routes
    ]
    assert unroutable == [], unroutable


def _build_repo(tmp_path: Path) -> Path:
    repo = tmp_path / "repo"
    for name, body in BUILD_FILES.items():
        path = repo / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(body, encoding="utf-8")
    return repo


def _stored_text_by_path(repo: Path, routes: dict[str, str]) -> dict[str, str]:
    run = DocCorpusSource(
        repo="t",
        repo_root=repo, non_git_walk=True,
        extractors_dir=TEMPLATE,
        include_globs=tuple(_glob_list("include_globs")),
        exclude_globs=tuple(_glob_list("exclude_globs")),
        extension_to_format=routes,
        max_files=20,
        max_bytes_per_file=1_000_000,
        text_fallback=False,  # the routes alone must carry these files
    ).run("probe", mode="scope_complete")
    facts = list(run.facts)
    doc_path = {
        f.node_id: (f.attrs or {}).get("path")
        for f in facts if getattr(f, "subtype", None) == "document"
    }
    chunk_text = {
        f.node_id: str((f.attrs or {}).get("text") or "")
        for f in facts if getattr(f, "subtype", None) == "document_chunk"
    }
    out: dict[str, str] = {path: "" for path in doc_path.values()}
    for edge in facts:
        if getattr(edge, "edge_type", None) == "member_of" and edge.dst in doc_path:
            out[doc_path[edge.dst]] += chunk_text.get(edge.src, "")
    return out


def test_build_and_deploy_files_are_stored_as_text(tmp_path: Path) -> None:
    stored = _stored_text_by_path(_build_repo(tmp_path), _extension_to_format())
    for name, body in BUILD_FILES.items():
        marker = re.search(r"[a-z_]+_marker", body).group(0)
        assert marker in stored.get(name, ""), (name, stored.get(name))


def test_without_name_routes_a_dockerfile_stores_no_text(tmp_path: Path) -> None:
    """The failure the name routes remove."""
    suffix_only = {
        key: fmt for key, fmt in _extension_to_format().items() if key.startswith(".")
    }
    stored = _stored_text_by_path(_build_repo(tmp_path), suffix_only)
    assert "dockerfile_body_marker" not in stored.get("Dockerfile", "")
    # Suffix routes still work.
    assert "mk_include_marker" in stored["rules.mk"]


def test_a_route_key_with_a_slash_is_refused(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="could never match"):
        DocCorpusSource(
            repo="t",
            repo_root=tmp_path,
            extractors_dir=TEMPLATE,
            extension_to_format={"docker/Dockerfile": "text"},
        )


# -- Operator note for deployments made before a template change -------------

README = TEMPLATE / "README.md"


def test_the_readme_shows_how_to_add_the_python_chunker(tmp_path: Path) -> None:
    from okg.substrate.library.extraction.chunk.python_symbols import (
        PythonSymbolChunker,
    )
    from okg.substrate.library.extraction.registry import load_extractors

    text = README.read_text(encoding="utf-8")
    section = text.split("## Updating a deployment made before a template change", 1)[1]
    assert "extractors.yaml" in section and "--reset-cursor" in section
    snippet = section.split("```yaml\n", 1)[1].split("```", 1)[0]

    (tmp_path / "extractors.yaml").write_text(
        "parser:\n"
        "  - format: code\n"
        "    class: okg.substrate.library.extraction.parse.code_python.PythonAstParser\n"
        "chunker:\n" + snippet,
        encoding="utf-8",
    )
    chunker = load_extractors(tmp_path).chunker_for_format("code")
    assert isinstance(chunker, PythonSymbolChunker)
    assert (chunker.max_chars, chunker.overlap) == (4000, 400)


# -- A fresh deployment ingests with no hand edit (#2694) --------------------


@pytest.mark.postgres
def test_a_fresh_template_deployment_ingests_and_publishes(
    schema_destructive_runtime: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The issue's reproduction: init with no publish, then a manual ingest.

    Before the template shipped a ``nomos:`` block, ``okg ingest`` refused
    git-files at stage ``run`` (``nomos.audit.deployment_posture_missing``)
    and skipped doc-corpus, so nothing published. ``okg init --no-publish``
    seeds no Nomos policy bundle. Since issue #3414 the template ships
    ``runtime_enforcement: off``, so the first ingest runs without one and no
    runtime decision is evaluated or recorded.
    """
    import json

    import okg.substrate.cli.main as cli
    from okg.substrate.nomos.runtime_enforcement import (
        _clear_nomos_enforcement_mode_cache,
    )

    dsn = schema_destructive_runtime
    name = "w2694"
    monkeypatch.delenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", raising=False)
    monkeypatch.setenv("OKG_DSN", dsn)
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "a.py").write_text("def f():\n    return 1\n", encoding="utf-8")
    (repo / "README.md").write_text("# freshdeploymentmarker\n", encoding="utf-8")
    git = ["git", "-C", str(repo), "-c", "user.name=t", "-c", "user.email=t@t"]
    subprocess.run([*git, "init", "-q"], check=True)
    subprocess.run([*git, "add", "-A"], check=True)
    subprocess.run([*git, "commit", "-q", "-m", "init"], check=True)
    # The operator's own repository for deployment folders: a publish records
    # the deployment's Git revision, so the rendered folder is committed.
    ops = tmp_path / "ops"
    ops.mkdir()
    ops_git = ["git", "-C", str(ops), "-c", "user.name=t", "-c", "user.email=t@t"]
    subprocess.run([*ops_git, "init", "-q"], check=True)
    target = ops / "deployments"

    assert cli.main([
        "init",
        "--template", "codebase-index",
        "--target", str(target),
        "--repo-root", str(repo),
        "--deployment-name", name,
        "--no-publish",
        "--non-interactive",
        "--yes",
    ]) == 0
    # No hand edit: the rendered file is committed exactly as init wrote it.
    subprocess.run([*ops_git, "add", "-A"], check=True)
    subprocess.run([*ops_git, "commit", "-q", "-m", "okg init"], check=True)
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(target))
    _clear_nomos_enforcement_mode_cache()
    assert cli.main([
        "catalog", "load", "--deployment", str(target / name), "--apply",
    ]) == 0
    capsys.readouterr()

    rc = cli.main([
        "ingest", "--deployment", name, "--include", f"{name}.doc-corpus",
        "--json",
    ])
    out = capsys.readouterr().out
    report = json.loads(out)
    completeness = report["completeness"]
    failed = {r["name"]: r.get("error") for r in report["results"] if r["status"] != "OK"}

    assert failed == {}, failed
    assert completeness["status"] == "complete", completeness
    assert set(completeness["completed_sources"]) == {"git-files", "doc-corpus"}
    assert [p["status"] for p in report["publishes"]] == ["published"], (
        report["publishes"]
    )
    published = report["publishes"][0]["generation_id"]
    assert published, report["publishes"]
    assert rc == 0

    with psycopg.connect(dsn, autocommit=True) as conn:
        bundles = conn.execute(
            "SELECT count(*) FROM okg.nomos_policy_activations",
        ).fetchone()[0]
        decisions = conn.execute(
            "SELECT count(*) FROM okg.nomos_decisions "
            "WHERE deployment_name = %s",
            (name,),
        ).fetchone()[0]
        session = open_session(conn, deployment_name=name)
        found = search(
            conn,
            session,
            "freshdeploymentmarker",
            method="lexical",
            filter={"subtype": "document_chunk"},
            limit=5,
        )

    # Reads pin to the generation this ingest published (a Chronos checkpoint).
    assert session.generation_id == published
    # Nothing seeded a bundle on this path, and with runtime enforcement off
    # (issue #3414) no runtime decision was evaluated or recorded.
    assert bundles == 0
    assert decisions == 0
    # The default lexical page has no count (PACT lexical-index-scan-cap-3162).
    assert found["results"], found
