"""Executed-test half of task.stamper-path-tokens.

Covers req.path-fields-tokenized from
pact/changes/index-document-source-file-search/pact.yaml: a subtype may
declare `okg_searchable_text_path_fields`, and the searchable-text rollup
then also carries the component-word form of those values, so a word
inside a path is matchable under the BM25 index's `english` config (which
otherwise lexes a whole path as a SINGLE token).

The unit tests here carry NO postgres mark on purpose — a file-level
`pytestmark = pytest.mark.postgres` hides every test in the file from the
per-PR CI lane. The stamping tests are marked individually.
"""
from __future__ import annotations

import json
import re
import uuid
from pathlib import Path

import psycopg
import pytest
import yaml

from okg.substrate.catalog.modules_compose import (
    ComposedCatalog,
    _extract_subtypes,
)
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import NodeFact
from okg.substrate.publisher import Publisher
from okg.substrate.searchable_text import split_camel_value, split_path_value
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402


PATH_VALUE = "tests/fixtures/cooper/refs/AN-23-183/07-Background_Estimation.tex"
PATH_SPLIT = (
    "tests fixtures cooper refs AN 23 183 07 Background Estimation tex"
)


def test_split_path_value() -> None:
    """Separator runs collapse to single spaces; the result is a plain
    word sequence."""
    assert split_path_value(PATH_VALUE) == PATH_SPLIT
    assert split_path_value("src/app.py") == "src app py"
    assert split_path_value("a//b__c--d") == "a b c d"
    assert split_path_value("chunk:doc:0012") == "chunk doc 0012"
    assert split_path_value("already words") == "already words"


#: CamelCase and letter-digit identifiers (archi-physics/cms-kb#6). TWiki
#: page titles are WikiWords, which the `english` parser lexes as ONE
#: token each, so a word inside one was unfindable.
CAMEL_CASES = (
    ("SWGuideCrab", "SW Guide Crab"),
    ("HLTConfig", "HLT Config"),
    # An all-caps run with no lowercase after it has no marked end.
    ("EcalPFGFAQ", "Ecal PFGFAQ"),
    ("CMST0TransferSystem", "CMST0 Transfer System"),
    ("OfflineOperationsMeeting28092011", "Offline Operations Meeting 28092011"),
    ("CMSSW_14_0", "CMSSW 14 0"),
    ("Run3Analysis", "Run 3 Analysis"),
    ("formal/admission/AdmissionSmoke.cfg", "formal admission Admission Smoke cfg"),
    ("oauth2-proxy.cfg", "oauth 2 proxy cfg"),
    # No boundary to mark: all lowercase, all caps, single letters
    # between digits, caps then digits.
    ("readme", "readme"),
    ("README.md", "README md"),
    ("a1b2c3", "a1b2c3"),
    ("PFG2024", "PFG2024"),
    # Hash-like components (hex only, 7+ long) stay whole; a shorter or
    # non-hex neighbour is still split.
    ("fdd423463766", "fdd423463766"),
    ("build/fdd4234/Run3Analysis.log", "build fdd4234 Run 3 Analysis log"),
    ("abc1234", "abc1234"),
    ("abc123", "abc 123"),
    ("xyz1234567", "xyz 1234567"),
)


@pytest.mark.parametrize(("value", "expected"), CAMEL_CASES)
def test_split_camel_value(value: str, expected: str) -> None:
    """CamelCase, acronym ends, and letter-digit joins become word
    boundaries; acronyms and hash-like components stay whole."""
    assert split_camel_value(value) == expected


@pytest.mark.parametrize(("value", "_expected"), CAMEL_CASES)
def test_separator_split_keeps_whole_components(
    value: str, _expected: str,
) -> None:
    """The separator split is unchanged by the CamelCase work: every
    component keeps its own spelling, so `AdmissionSmoke` and `oauth2`
    stay matchable as whole words."""
    assert split_path_value(value) == " ".join(
        part for part in re.split(r"[/\\_.:\-]+", value) if part
    )


def test_split_path_value_without_word_content() -> None:
    """A value that is only separators has no component words."""
    assert split_path_value("///") == ""
    assert split_path_value("   ") == ""


def _compose_path_fields(
    tmp_path: Path, searchable: str | None, path: str | None,
) -> list[str]:
    """Run one subtype's annotations through the REAL compose step —
    `_extract_subtypes` over a schema.yaml — not a re-implementation of
    it. A guard proven only against a stand-in proves nothing about the
    code that loads a deployment's catalog."""
    annotations: dict[str, str] = {"archetype": "artifact"}
    if searchable is not None:
        annotations["okg_searchable_text_fields"] = searchable
    if path is not None:
        annotations["okg_searchable_text_path_fields"] = path

    schema_path = tmp_path / "schema.yaml"
    schema_path.write_text(
        yaml.safe_dump({"classes": {"Memo": {"annotations": annotations}}})
    )
    composed = ComposedCatalog(deployment_name="test")
    _extract_subtypes(schema_path, "test", composed)
    return (
        composed.subtype_metadata
        .get("memo", {})
        .get("searchable_text_path_fields", [])
    )


def test_path_field_must_also_be_a_searchable_field(tmp_path: Path) -> None:
    """A path field that is not also a plain searchable field is rejected
    at compose time. On its own it contributes nothing, so accepting it
    would ship a catalog claiming the field is indexed when nothing
    stamps it — the silent no-op this guard exists to prevent."""
    assert _compose_path_fields(
        tmp_path, "title,path", "path",
    ) == ["path"]

    with pytest.raises(ValueError, match="not also declared"):
        _compose_path_fields(tmp_path, "title", "path")
    with pytest.raises(ValueError, match="not also declared"):
        _compose_path_fields(tmp_path, None, "path")


def test_text_cannot_be_a_path_field(tmp_path: Path) -> None:
    """`attrs.text` is the rollup this stamping WRITES, not a source
    field. Both stampers skip it in the path loop, so declaring it would
    be another silently-inert declaration."""
    with pytest.raises(ValueError, match="cannot be an"):
        _compose_path_fields(tmp_path, "text,path", "text,path")


def _seed_cv_with_subtype_metadata(
    conn: psycopg.Connection,
    *,
    subtype: str,
    text_fields: list[str] | None,
    path_fields: list[str] | None = None,
) -> int:
    metadata: dict[str, list[str]] = {}
    if text_fields:
        metadata["searchable_text_fields"] = text_fields
    if path_fields:
        metadata["searchable_text_path_fields"] = path_fields
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO okg.catalog_versions (status, applied_at) "
            "VALUES ('applied', now()) RETURNING catalog_version_id"
        )
        cv = cur.fetchone()[0]
        cur.execute(
            "INSERT INTO okg.subtypes (name, metadata, catalog_version_id) "
            "VALUES (%s, %s::jsonb, %s)",
            (subtype, json.dumps(metadata), cv),
        )
    return cv


def _seed_node_fact(
    conn: psycopg.Connection,
    *,
    node_id: str,
    subtype: str,
    attrs: dict,
) -> None:
    run_id = str(uuid.uuid4())
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.node_facts
              (node_id, subtype, attrs, source, source_record_id,
               source_revision, observed_at, dedupe_key, run_id, op)
            VALUES (%s, %s, %s::jsonb, 'fixture',
                    %s::jsonb, %s::jsonb, now(), %s, %s, 'I')
            """,
            (
                node_id, subtype, json.dumps(attrs),
                json.dumps({"id": node_id}),
                json.dumps({"v": 1, "rev": run_id}),
                f"{run_id}:{node_id}", run_id,
            ),
        )
    ChronosGraphStore.from_connection(conn).apply_facts(
        [
            NodeFact(
                node_id=node_id,
                subtype=subtype,
                attrs=dict(attrs),
                source_record_id={"id": node_id},
                source_revision={"v": 1},
            )
        ],
        source="fixture",
    )


def _graph_text_at(dsn: str, generation_id: str, node_id: str) -> str | None:
    store = ChronosGraphStore.connect(dsn)
    try:
        rows = store.checkout_generation(generation_id).query(
            "SELECT attrs->>'text' AS text FROM okg.graph_nodes "
            "WHERE node_id = %(node_id)s",
            {"node_id": node_id},
        )
        return rows[0]["text"] if rows else None
    finally:
        store.close()


@pytest.mark.postgres
def test_projector_stamps_path_tokens(clean_runtime: str) -> None:
    """scn.path-fields-tokenized.split: the rollup carries the raw value
    AND its component words, and `english` lexes the words separately."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_cv_with_subtype_metadata(
            conn, subtype="pathmemo",
            text_fields=["p"], path_fields=["p"],
        )
        _seed_node_fact(
            conn, node_id="n:path:1", subtype="pathmemo",
            attrs={"p": PATH_VALUE},
        )

    out = Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()
    assert out["status"] == "published", out

    text = _graph_text_at(clean_runtime, out["generation_id"], "n:path:1")
    assert text == f"{PATH_VALUE}\n\n{PATH_SPLIT}", repr(text)

    # The point of the split: `english` must yield a lexeme for an
    # interior word. Without it the whole path is one `file` token.
    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
            conn.cursor() as cur:
        cur.execute(
            "SELECT to_tsvector('english', %s) @@ "
            "       plainto_tsquery('english', 'background')",
            (text,),
        )
        assert cur.fetchone()[0] is True
        cur.execute(
            "SELECT to_tsvector('english', %s) @@ "
            "       plainto_tsquery('english', 'background')",
            (PATH_VALUE,),
        )
        assert cur.fetchone()[0] is False, (
            "pre-change behavior changed: a raw path now lexes into words, "
            "so this test no longer proves the split is what fixed #900"
        )


@pytest.mark.postgres
def test_plain_declaration_unchanged(clean_runtime: str) -> None:
    """scn.path-fields-tokenized.no-op: a subtype that declares no path
    fields stamps byte-identical text to the pre-change function."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_cv_with_subtype_metadata(
            conn, subtype="plainmemo",
            text_fields=["a", "b"], path_fields=None,
        )
        _seed_node_fact(
            conn, node_id="n:plain:1", subtype="plainmemo",
            attrs={"a": "src/app.py", "b": "world"},
        )

    out = Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()
    assert out["status"] == "published", out

    text = _graph_text_at(clean_runtime, out["generation_id"], "n:plain:1")
    assert text == "src/app.py\n\nworld", repr(text)


@pytest.mark.postgres
def test_duplicate_field_values_collapse(clean_runtime: str) -> None:
    """Two declared fields holding the same value contribute it once —
    `document.title` is seeded from `document.path`."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_cv_with_subtype_metadata(
            conn, subtype="dupmemo",
            text_fields=["title", "path"], path_fields=["title", "path"],
        )
        _seed_node_fact(
            conn, node_id="n:dup:1", subtype="dupmemo",
            attrs={"title": "src/app.py", "path": "src/app.py"},
        )

    out = Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()
    assert out["status"] == "published", out

    text = _graph_text_at(clean_runtime, out["generation_id"], "n:dup:1")
    assert text == "src/app.py\n\nsrc app py", repr(text)


@pytest.mark.postgres
def test_sql_and_python_stampers_agree(clean_runtime: str) -> None:
    """scn.path-fields-tokenized.mirrors-agree: `okg._stamp_searchable_text`
    and `ChronosGraphWriteMixin._stamp_searchable_text` produce the same
    rollup for the same declaration and attrs."""
    cases = [
        {"p": PATH_VALUE, "q": "plain words"},
        {"p": "src/app.py", "q": "src/app.py"},
        {"p": "///", "q": "only separators above"},
        {"q": "no path field present"},
        {"p": None, "q": "path field is not a string"},
        {"p": 17, "q": "path field is a number"},
        {"p": ["a/b"], "q": "path field is an array"},
        {"p": "", "q": "path field is empty"},
        # Whitespace is where the SQL and Python trims diverge if the two
        # are not deliberately held to the same character set. Postgres'
        # `btrim`/`\s` cover ASCII whitespace; Python's `.strip()`/`\s`
        # also cover U+00A0 and friends. With a dedupe in play, a padding
        # difference decides whether a part is DROPPED, not just how it is
        # spelled — so these must agree exactly.
        {"q": "x", "p": "\tx"},
        {"q": "plain", "p": "a\u00a0b/c"},
        {"q": " x\n", "p": "src/app.py"},
        {"q": " y ", "p": " y "},
        {"q": "z", "p": "  \t src//app.py \n "},
        {"q": "dup", "p": "dup"},
        # Explicit Unicode-space cases (the literals above can hide which
        # space character they carry).
        {"q": "\u00a0nbsp\u00a0", "p": "src/app.py"},
        {"q": "\u3000ideographic\u3000", "p": "a/b"},
        # CamelCase splitting (archi-physics/cms-kb#6): both mirrors run
        # the same substitutions in the same order.
        *({"q": "camel", "p": value} for value, _ in CAMEL_CASES),
        {"q": "camel", "p": "src/okg/substrate/graph_store_internal/writes.py"},
        {"q": "camel", "p": "formal/admission/AdmissionSmoke.cfg"},
        {"q": "camel", "p": "ops/oauth2-proxy.cfg"},
        {"q": "camel", "p": "runs/fdd423463766/DEADBEEF12/Gen13Sample.root"},
        {"q": "camel", "p": "twiki/CMSPublic/SWGuideCrab_v2.txt"},
        {"q": "camel", "p": "ÉcoleNormale\u00a0ÜberGuide"},
        {"q": "SWGuideCrab", "p": "SWGuideCrab"},
    ]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        cv = _seed_cv_with_subtype_metadata(
            conn, subtype="mirrormemo",
            text_fields=["q", "p"], path_fields=["p"],
        )
        store = ChronosGraphStore.from_connection(conn)
        for attrs in cases:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT okg._stamp_searchable_text("
                    "  'mirrormemo', %s::jsonb, %s)->>'text'",
                    (json.dumps(attrs), cv),
                )
                sql_text = cur.fetchone()[0]
            python_text = store._stamp_searchable_text(
                "mirrormemo", dict(attrs), cache={},
            ).get("text")
            assert sql_text == python_text, (attrs, sql_text, python_text)


#: Whitespace that ONLY the `text` fallback branch can expose. The cases
#: above all declare `q`/`p`, so `parts` is never empty and the fallback
#: never runs — a differential blind to the branch that
#: `bookkeeping.ProjectGoal` (`okg_searchable_text_fields: text`) takes on
#: EVERY node. Tabs and newlines are ordinary in a text blob; the Unicode
#: entries are here because Python's `.strip()` covers them and Postgres'
#: `btrim` does not.
_FALLBACK_WHITESPACE = (
    "\t", "\n", "\r", "\f", "\v", " ", " ", " ", "　",
)


@pytest.mark.postgres
def test_sql_and_python_stampers_agree_on_the_text_fallback(
    clean_runtime: str,
) -> None:
    """The `text`-only declaration takes the `cardinality(parts) = 0`
    fallback. The two mirrors must agree there too."""
    cases = [{"text": f"{ws}hello{ws}"} for ws in _FALLBACK_WHITESPACE]
    cases += [
        {"text": "  \t mixed \n "},
        {"text": " only-nbsp "},
        {"text": "no padding"},
        {"text": "   "},
        {"text": ""},
        {"text": 17},
        {"other": "text attr absent"},
    ]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        cv = _seed_cv_with_subtype_metadata(
            conn, subtype="fallbackmemo", text_fields=["text"],
        )
        store = ChronosGraphStore.from_connection(conn)
        for attrs in cases:
            with conn.cursor() as cur:
                # Compare the JSON value, not `->>'text'`: the text
                # operator stringifies a non-string, which would report a
                # divergence where both sides in fact leave attrs alone.
                cur.execute(
                    "SELECT okg._stamp_searchable_text("
                    "  'fallbackmemo', %s::jsonb, %s)->'text'",
                    (json.dumps(attrs), cv),
                )
                sql_text = cur.fetchone()[0]
            python_text = store._stamp_searchable_text(
                "fallbackmemo", dict(attrs), cache={},
            ).get("text")
            assert sql_text == python_text, (attrs, sql_text, python_text)
            # Issue #2972: the fallback keeps the stored body byte for byte;
            # it never trims a non-blank value.
            if isinstance(attrs.get("text"), str):
                assert python_text == attrs["text"], (attrs, python_text)


def _lexical_ids(dsn: str, query: str) -> list[str]:
    from okg.substrate.mcp import open_session, search

    with psycopg.connect(dsn, autocommit=True) as conn:
        session = open_session(conn, deployment_name="default")
        hits = search(conn, session, query, method="lexical", limit=20)
    return [row["node_id"] for row in hits["results"]]


@pytest.mark.postgres
def test_camel_case_title_found_by_its_words(clean_runtime: str) -> None:
    """archi-physics/cms-kb#6: a documentation page titled `SWGuideCrab`
    is found by "crab guide" once its title is a path field, an exact
    `SWGuideCrab` query still ranks it first, and a subtype that does not
    declare the field keeps the old one-token text (the control row, which
    is how the page behaved before this change)."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        cv = _seed_cv_with_subtype_metadata(
            conn, subtype="docpage",
            text_fields=["title", "url"], path_fields=["title"],
        )
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO okg.subtypes (name, metadata, catalog_version_id) "
                "VALUES ('plainpage', %s::jsonb, %s)",
                (json.dumps({"searchable_text_fields": ["title", "url"]}), cv),
            )
        pages = (
            ("n:doc:crab", "docpage", "SWGuideCrab"),
            ("n:doc:crabconfig", "docpage", "SWGuideCrabConfig"),
            ("n:doc:ecal", "docpage", "EcalPFGFAQ"),
            ("n:doc:plain", "plainpage", "SWGuideCrabLegacy"),
        )
        for node_id, subtype, title in pages:
            _seed_node_fact(
                conn, node_id=node_id, subtype=subtype,
                attrs={
                    "title": title,
                    "url": f"https://twiki.example.org/view/{title}",
                },
            )

    out = Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()
    assert out["status"] == "published", out

    text = _graph_text_at(clean_runtime, out["generation_id"], "n:doc:crab")
    assert text == (
        "SWGuideCrab\n\nhttps://twiki.example.org/view/SWGuideCrab"
        "\n\nSW Guide Crab"
    ), repr(text)

    words = _lexical_ids(clean_runtime, "crab guide")
    assert set(words) == {"n:doc:crab", "n:doc:crabconfig"}, words

    exact = _lexical_ids(clean_runtime, "SWGuideCrab")
    assert exact and exact[0] == "n:doc:crab", exact

    assert _lexical_ids(clean_runtime, "ecal") == ["n:doc:ecal"]


@pytest.mark.postgres
def test_path_components_and_camel_words_both_found(clean_runtime: str) -> None:
    """The CamelCase form is added BESIDE the separator split, never in its
    place. A slash-path is one `file` lexeme, so a whole file-name
    component is findable only through the separator split: replacing it
    (as 625b1d4f03 did) lost `AdmissionSmoke`, `oauth2` and an exact
    `SWGuideCrab` inside a path."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_cv_with_subtype_metadata(
            conn, subtype="repofile", text_fields=["path"], path_fields=["path"],
        )
        paths = (
            ("n:file:smoke", "formal/admission/AdmissionSmoke.cfg"),
            ("n:file:oauth", "ops/oauth2-proxy.cfg"),
            ("n:file:crab", "twiki/SWGuideCrab.txt"),
            ("n:file:other", "src/app.py"),
        )
        for node_id, path in paths:
            _seed_node_fact(
                conn, node_id=node_id, subtype="repofile", attrs={"path": path},
            )

    out = Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()
    assert out["status"] == "published", out

    # One dict, so a regression reports every query at once. At
    # 625b1d4f03 the first three came back empty; "crab guide" is the
    # CamelCase gain that must survive the fix.
    found = {
        query: _lexical_ids(clean_runtime, query)
        for query in ("AdmissionSmoke", "oauth2", "SWGuideCrab", "crab guide")
    }
    assert found == {
        "AdmissionSmoke": ["n:file:smoke"],
        "oauth2": ["n:file:oauth"],
        "SWGuideCrab": ["n:file:crab"],
        "crab guide": ["n:file:crab"],
    }, found
    assert _lexical_ids(clean_runtime, "admission smoke")[0] == "n:file:smoke"

    text = _graph_text_at(clean_runtime, out["generation_id"], "n:file:smoke")
    assert text == (
        "formal/admission/AdmissionSmoke.cfg"
        "\n\nformal admission AdmissionSmoke cfg"
        "\n\nformal admission Admission Smoke cfg"
    ), repr(text)


# --- issue #2991: a producer's own `text` body ----------------------------

_BODY = "Events pass the HLT trigger before the zeppelin selection."

#: Inputs for the mirror check under the declaration `title,text,p` with
#: path field `p`. The first group is what producers write; the stamper's
#: own outputs are added in the test, so restamps are compared too.
PRODUCER_BODY_CASES = [
    {"title": "T", "text": _BODY},
    {"title": "T", "text": "  padded body \n"},
    {"title": "T", "text": "T"},
    {"title": "T", "text": " T\t"},
    {"title": "T", "text": "T\n\nX"},
    {"title": "   ", "text": _BODY},
    {"text": _BODY},
    {"title": "T", "p": "notes/SWGuideCrab.md", "text": _BODY},
    {"title": "T", "p": "a/b", "text": "T\n\na/b\n\na b"},
    {"title": "T", "p": "a/b"},
    {"title": "T", "text": "   "},
    {"title": "T", "text": 17},
    {"title": "T", "text": None},
    {"title": "T", "text": ["a"]},
    {"title": "T", "text": " nbsp body "},
    # Reserved-copy inputs: a stored stamp, a stale copy, a blank copy, a
    # non-string copy.
    {"title": "New", "text": f"T\n\n{_BODY}", "okg_producer_text": _BODY},
    {"title": "T", "text": "a new body", "okg_producer_text": _BODY},
    {"title": "T", "text": "apply the fix", "okg_producer_text": "the fix"},
    # Review round 2: a supplied text that ends with the stale copy, and
    # copies of bodies that repeat a part.
    {"title": "T", "text": "A\n\nC", "okg_producer_text": "C"},
    {"title": "T", "text": "T", "okg_producer_text": "T"},
    {"title": "T", "text": "T", "okg_producer_text": " T\n"},
    {"title": "T", "p": "a/b", "text": "T\n\na/b\n\na b", "okg_producer_text": "a/b"},
    {"title": "T", "text": _BODY, "okg_producer_text": _BODY},
    {"title": "T", "text": "T", "okg_producer_text": "  "},
    {"title": "T", "text": _BODY, "okg_producer_text": 5},
    {"title": "T", "okg_producer_text": _BODY},
    {"text": f"x\n\n{_BODY}", "okg_producer_text": _BODY},
]


@pytest.mark.postgres
def test_sql_and_python_stampers_agree_on_the_producer_body(
    clean_runtime: str,
) -> None:
    """Issue #2991: the two mirrors agree on the WHOLE stamped attrs --
    `text` and the reserved copy -- for producer bodies, restamps of their
    own output, and stale or malformed copies; and stamping twice equals
    stamping once on both."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        cv = _seed_cv_with_subtype_metadata(
            conn, subtype="bodymemo",
            text_fields=["title", "text", "p"], path_fields=["p"],
        )
        store = ChronosGraphStore.from_connection(conn)

        def sql_stamp(attrs: dict) -> dict:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT okg._stamp_searchable_text("
                    "  'bodymemo', %s::jsonb, %s)",
                    (json.dumps(attrs), cv),
                )
                return cur.fetchone()[0]

        def py_stamp(attrs: dict) -> dict:
            return store._stamp_searchable_text(
                "bodymemo", dict(attrs), cache={},
            )

        cases = list(PRODUCER_BODY_CASES)
        cases += [py_stamp(attrs) for attrs in PRODUCER_BODY_CASES]
        for attrs in cases:
            sql_out = sql_stamp(attrs)
            py_out = py_stamp(attrs)
            assert sql_out == py_out, (attrs, sql_out, py_out)
            assert sql_stamp(sql_out) == sql_out, (attrs, sql_out)
            assert py_stamp(py_out) == py_out, (attrs, py_out)

        body = py_stamp({"title": "T", "text": _BODY})
        assert body["text"] == f"T\n\n{_BODY}"
        assert body["okg_producer_text"] == _BODY


# --- issue #3469: a declared value the body holds is not repeated --------

_JIRA_BODY = "Fix JIRA-4411: the SSO bug blocks C++ (v1.2) renewal."

#: Inputs for the mirror check under the declaration `title,key,text,p`
#: with path field `p`: parts the body holds as whole tokens, parts only
#: inside a longer word, regex metacharacters, several parts, every part
#: held, path and CamelCase splits (always kept), non-ASCII neighbours, and
#: a part longer than the SQL regex limit (the `strpos` walk).
DEDUPE_CASES = [
    {"title": "SSO bug", "key": "JIRA-4411", "text": _JIRA_BODY},
    {"title": "Renewal", "key": "JIRA-4411", "text": _JIRA_BODY},
    {"title": "C++ (v1.2)", "text": _JIRA_BODY},
    {"title": "C++ (v1.2)", "text": "C++ (v1.2)x"},
    {"title": "HLT", "text": "the HLTConfig module"},
    {"title": "HLT", "text": "HLT_v2 and HLTConfig"},
    {"title": "HLT", "text": "ÉHLT"},
    {"title": "Été", "text": "xÉté"},
    {"title": "Été", "text": "Été day"},
    {"title": "JIRA-4411", "text": "JIRA-44110 then JIRA-4411"},
    {"title": "a.b*c", "key": "x|y", "text": "aXbbbc x|y"},
    {"title": "[z]", "key": "(?i)t", "text": "[z] T"},
    {"title": "\\d", "key": "***=x", "text": "7 \\d ***=x"},
    {"title": "[:alpha:]", "key": "a{2}", "text": "aa [:alpha:]"},
    {"title": "it's", "key": "100%", "text": "it's at 100%."},
    {"title": "SSO", "key": "sso", "text": "the sso fix"},
    {"title": "T", "text": "T\n\nX"},
    {"title": "Same", "text": " Same\n"},
    {"p": "notes/SWGuideCrab.md", "text": "see notes/SWGuideCrab.md"},
    {"p": "a/b", "text": "a b"},
    {"title": "notes SWGuideCrab md", "p": "notes/SWGuideCrab.md",
     "text": "notes SWGuideCrab md"},
    {"title": "x" * 1500, "text": "y" + "x" * 1500 + " " + "x" * 1500 + "!"},
    {"title": "x" * 1500, "text": "y" + "x" * 1501},
    {"title": "x" * 1001, "text": "(" + "x" * 1001 + ")"},
]


def _legacy_stamp(attrs: dict, fields: list[str], path_fields: list[str]) -> dict:
    """What the #2991 rule stored for a stamp with a body: every part, then
    the body, with the copy."""
    from okg.substrate.searchable_text import ASCII_WHITESPACE, searchable_parts

    parts = searchable_parts(attrs, fields, path_fields)
    body = attrs["okg_producer_text"]
    text = "\n\n".join(parts)
    if body.strip(ASCII_WHITESPACE) not in parts:
        text = f"{text}\n\n{body}"
    return {**attrs, "text": text}


@pytest.mark.postgres
def test_sql_and_python_stampers_agree_on_the_dedupe(clean_runtime: str) -> None:
    """Issue #3469: both mirrors leave out the same parts, write the same
    copy, restamp a node stored under the #2991 rule to the same new form,
    and return their own output unchanged."""
    fields, path_fields = ["title", "key", "text", "p"], ["p"]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        cv = _seed_cv_with_subtype_metadata(
            conn, subtype="dedupememo",
            text_fields=fields, path_fields=path_fields,
        )
        store = ChronosGraphStore.from_connection(conn)

        def sql_stamp(attrs: dict) -> dict:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT okg._stamp_searchable_text("
                    "  'dedupememo', %s::jsonb, %s)",
                    (json.dumps(attrs), cv),
                )
                return cur.fetchone()[0]

        def py_stamp(attrs: dict) -> dict:
            return store._stamp_searchable_text(
                "dedupememo", dict(attrs), cache={},
            )

        for attrs in DEDUPE_CASES:
            py_out = py_stamp(attrs)
            sql_out = sql_stamp(attrs)
            assert sql_out == py_out, (attrs, sql_out, py_out)
            assert sql_stamp(sql_out) == sql_out, attrs
            assert py_stamp(py_out) == py_out, attrs
            if "okg_producer_text" in py_out:
                legacy = _legacy_stamp(py_out, fields, path_fields)
                assert sql_stamp(legacy) == py_out, (attrs, legacy)
                assert py_stamp(legacy) == py_out, (attrs, legacy)

        held = py_stamp(DEDUPE_CASES[0])
        assert held["text"] == _JIRA_BODY
        assert held["okg_producer_text"] == _JIRA_BODY
        partial = py_stamp(DEDUPE_CASES[1])
        assert partial["text"] == f"Renewal\n\n{_JIRA_BODY}"
        inner = py_stamp({"title": "HLT", "text": "the HLTConfig module"})
        assert inner["text"] == "HLT\n\nthe HLTConfig module"
        split = py_stamp(DEDUPE_CASES[18])
        assert split["text"] == (
            "notes SWGuideCrab md\n\nnotes SW Guide Crab md"
            "\n\nsee notes/SWGuideCrab.md"
        )


@pytest.mark.postgres
def test_old_form_node_publishes_in_the_new_form(clean_runtime: str) -> None:
    """Issue #3469 end to end: a node fact carrying text stored under the
    #2991 rule (every part, then the body, with the copy) publishes as the
    body alone when the body holds every declared value, and stays
    findable by a word from its key."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_cv_with_subtype_metadata(
            conn, subtype="jiramemo", text_fields=["key", "summary", "text"],
        )
        _seed_node_fact(
            conn, node_id="n:jira:legacy", subtype="jiramemo",
            attrs={
                "key": "JIRA-4411", "summary": "the SSO bug",
                "text": f"JIRA-4411\n\nthe SSO bug\n\n{_JIRA_BODY}",
                "okg_producer_text": _JIRA_BODY,
            },
        )

    out = Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()
    assert out["status"] == "published", out
    text = _graph_text_at(clean_runtime, out["generation_id"], "n:jira:legacy")
    assert text == _JIRA_BODY, repr(text)
    assert _graph_attrs(clean_runtime, "n:jira:legacy")["okg_producer_text"] == _JIRA_BODY
    assert _lexical_ids(clean_runtime, "renewal") == ["n:jira:legacy"]


@pytest.mark.postgres
def test_body_only_word_found_after_publish(clean_runtime: str) -> None:
    """Issue #2991 end to end: a node whose subtype declares `stem,text`
    (example-kb `Section`) is found by a word that appears only in its body,
    and the published `text` holds the stem and the body."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_cv_with_subtype_metadata(
            conn, subtype="notesection", text_fields=["stem", "text"],
        )
        _seed_node_fact(
            conn, node_id="n:section:1", subtype="notesection",
            attrs={"stem": "03-Selection", "text": _BODY},
        )

    out = Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()
    assert out["status"] == "published", out

    text = _graph_text_at(clean_runtime, out["generation_id"], "n:section:1")
    assert text == f"03-Selection\n\n{_BODY}", repr(text)
    assert _lexical_ids(clean_runtime, "zeppelin") == ["n:section:1"]


def _graph_attrs(dsn: str, node_id: str) -> dict:
    store = ChronosGraphStore.connect(dsn)
    try:
        rows = store.checkout().query(
            "SELECT attrs, text_hash FROM okg.graph_nodes "
            "WHERE node_id = :node_id",
            {"node_id": node_id},
        )
        return {**rows[0]["attrs"], "_text_hash": rows[0]["text_hash"]}
    finally:
        store.close()


@pytest.mark.postgres
def test_catalog_change_restamp_keeps_the_body(clean_runtime: str) -> None:
    """Issue #2991: the catalog-change restamp re-emits stored nodes under a
    new declaration. A producer body survives it, a node with no body gets
    the plain new rollup (its old rollup is not read as a body), and a
    second restamp changes nothing."""
    from okg.substrate.catalog.modules_apply import _restamp_changed_subtypes

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_cv_with_subtype_metadata(
            conn, subtype="restampmemo", text_fields=["title", "text"],
        )
        _seed_node_fact(
            conn, node_id="n:restamp:body", subtype="restampmemo",
            attrs={"title": "T", "summary": "S", "text": _BODY},
        )
        _seed_node_fact(
            conn, node_id="n:restamp:plain", subtype="restampmemo",
            attrs={"title": "T", "summary": "S"},
        )
    before = _graph_attrs(clean_runtime, "n:restamp:body")
    assert before["text"] == f"T\n\n{_BODY}"
    assert before["okg_producer_text"] == _BODY

    with psycopg.connect(clean_runtime) as conn:
        cv2 = _seed_cv_with_subtype_metadata(
            conn, subtype="restampmemo",
            text_fields=["title", "summary", "text"],
        )
        _restamp_changed_subtypes(
            conn, ["restampmemo"], deployment_name="default",
            catalog_version_id=cv2,
            prior_declarations={"restampmemo": (["title", "text"], [])},
        )
        conn.commit()
    body = _graph_attrs(clean_runtime, "n:restamp:body")
    plain = _graph_attrs(clean_runtime, "n:restamp:plain")
    assert body["text"] == f"T\n\nS\n\n{_BODY}", body
    assert body["okg_producer_text"] == _BODY
    assert plain["text"] == "T\n\nS", plain
    assert "okg_producer_text" not in plain

    with psycopg.connect(clean_runtime) as conn:
        _restamp_changed_subtypes(
            conn, ["restampmemo"], deployment_name="default",
            catalog_version_id=cv2,
            prior_declarations={"restampmemo": (["title", "summary", "text"], [])},
        )
        conn.commit()
    assert _graph_attrs(clean_runtime, "n:restamp:body") == body
    assert _graph_attrs(clean_runtime, "n:restamp:plain") == plain

    # A field is removed again: the plain node's two-part old rollup must
    # not come back as a body (`T\n\nT\n\nS`).
    with psycopg.connect(clean_runtime) as conn:
        cv3 = _seed_cv_with_subtype_metadata(
            conn, subtype="restampmemo", text_fields=["title", "text"],
        )
        _restamp_changed_subtypes(
            conn, ["restampmemo"], deployment_name="default",
            catalog_version_id=cv3,
            prior_declarations={"restampmemo": (["title", "summary", "text"], [])},
        )
        conn.commit()
    body = _graph_attrs(clean_runtime, "n:restamp:body")
    plain = _graph_attrs(clean_runtime, "n:restamp:plain")
    assert body["text"] == f"T\n\n{_BODY}", body
    assert body["okg_producer_text"] == _BODY
    assert plain["text"] == "T", plain
    assert "okg_producer_text" not in plain

    # The declaration goes away (review round 2): the stamper then leaves
    # attrs alone, so the body is the whole text and the plain node keeps
    # its old rollup, as dev did, instead of losing `text` altogether.
    with psycopg.connect(clean_runtime) as conn:
        cv4 = _seed_cv_with_subtype_metadata(
            conn, subtype="restampmemo", text_fields=None,
        )
        _restamp_changed_subtypes(
            conn, ["restampmemo"], deployment_name="default",
            catalog_version_id=cv4,
            prior_declarations={"restampmemo": (["title", "text"], [])},
        )
        conn.commit()
    body = _graph_attrs(clean_runtime, "n:restamp:body")
    plain = _graph_attrs(clean_runtime, "n:restamp:plain")
    assert body["text"] == _BODY, body
    assert "okg_producer_text" not in body
    assert plain["text"] == "T", plain
    assert plain["_text_hash"] is not None


# --- Issue #3318: a restamp must not take a row from its source ------------
#
# The catalog-change restamp used to re-apply every node of a changed subtype
# under the system source `_catalog:restamp`. Each rewritten row then carried
# that source, an empty source revision and the restamp's own run id, so the
# source that wrote it could no longer retract it: completed-scope retraction
# deletes only rows the retracting source owns.

_RESTAMP_SOURCE = "test.restamp-owner"
_RESTAMP_SUBTYPE = "restampjob"


def _provenance(dsn: str, node_id: str) -> dict | None:
    store = ChronosGraphStore.connect(dsn)
    try:
        rows = store.checkout().query(
            "SELECT source, source_record_id, source_revision, run_id, "
            "       observed_at, attrs, text_hash "
            "  FROM okg.graph_nodes WHERE node_id = :node_id",
            {"node_id": node_id},
        )
        return dict(rows[0]) if rows else None
    finally:
        store.close()


def _job_node(node_id: str) -> NodeFact:
    return NodeFact(
        node_id=node_id,
        subtype=_RESTAMP_SUBTYPE,
        attrs={"title": f"Job {node_id}", "label": f"partition-{node_id}"},
        source_record_id={"job": node_id},
        source_revision={"run": "r1", "rev": 7},
    )


def _job_record_set(node_ids: list[str]):
    from okg.substrate.library.sources.base import (
        RecordEmission,
        RecordNode,
        RecordSet,
    )

    return RecordSet(records={
        f"jobs/{node_id}": RecordEmission(
            primary=RecordNode(node_id, _RESTAMP_SUBTYPE),
            descendants=[],
            edges=[],
        )
        for node_id in node_ids
    })


class _JobAdapter:
    """A whole-scope source: each run declares its complete record set."""

    name = "restamp-jobs"
    profile = "discovery_crawl"
    probe_short_circuit_safe = True

    def __init__(self, runs: list[list[str]]) -> None:
        self._runs = list(runs)

    def run(self, run_id, *, mode="cursor", cursor=None, since_progress=None):
        from okg.substrate.library.sources.base import SourceRun

        del run_id, cursor, since_progress
        node_ids = self._runs.pop(0)
        return SourceRun(
            facts=[_job_node(node_id) for node_id in node_ids],
            completed_scope=True,
            next_cursor={},
            run_mode=mode,
            record_set=_job_record_set(node_ids),
        )


def _job_runner(dsn: str):
    from okg.substrate.ingest import SourceRunner

    return SourceRunner(dsn, source_name=_RESTAMP_SOURCE)


def _restamp_declaration_change(dsn: str) -> None:
    """The catalog adds `label` to the subtype's searchable fields and the
    apply restamps every stored node of it (the code under test)."""
    from okg.substrate.catalog.modules_apply import _restamp_changed_subtypes

    with psycopg.connect(dsn) as conn:
        cv = _seed_cv_with_subtype_metadata(
            conn, subtype=_RESTAMP_SUBTYPE, text_fields=["title", "label"],
        )
        _restamp_changed_subtypes(
            conn, [_RESTAMP_SUBTYPE], deployment_name="default",
            catalog_version_id=cv,
            prior_declarations={_RESTAMP_SUBTYPE: (["title"], [])},
        )
        conn.commit()


def _pre_fix_restamp(dsn: str) -> None:
    """The restamp exactly as dev wrote it before #3318, kept here so the
    repair is proven against rows the OLD code produced: every node of the
    subtype re-applied under producer and source `_catalog:restamp`, with
    `{"node_id": ...}` as its record id and an empty revision."""
    from okg.substrate.fact_application import apply_bounded_fact_batch
    from okg.substrate.searchable_text import unstamp_searchable_text

    with psycopg.connect(dsn) as conn:
        cv = _seed_cv_with_subtype_metadata(
            conn, subtype=_RESTAMP_SUBTYPE, text_fields=["title", "label"],
        )
        store = ChronosGraphStore.from_connection(
            conn, ensure_schema=False, autocommit=False,
            catalog_version_id=cv,
        )
        rows = store.checkout().query(
            "SELECT node_id, attrs FROM okg.graph_nodes "
            " WHERE subtype = :subtype",
            {"subtype": _RESTAMP_SUBTYPE},
        )
        facts = [
            NodeFact(
                row["node_id"],
                _RESTAMP_SUBTYPE,
                unstamp_searchable_text(dict(row["attrs"] or {}), ["title"], []),
                {"node_id": row["node_id"]},
                {},
            )
            for row in rows
        ]
        apply_bounded_fact_batch(
            conn,
            store,
            facts,
            deployment_name="default",
            producer_id="_catalog:restamp",
            producer_kind="system",
            intent="catalog_searchable_text_restamp",
            catalog_version_id=cv,
            authority_scope={
                "catalog_version_id": cv,
                "subtype": _RESTAMP_SUBTYPE,
            },
            source="_catalog:restamp",
            strict_endpoints=True,
        )
        conn.commit()


def _seed_job_subtype(dsn: str) -> None:
    with psycopg.connect(dsn, autocommit=True) as conn:
        _seed_cv_with_subtype_metadata(
            conn, subtype=_RESTAMP_SUBTYPE, text_fields=["title"],
        )


@pytest.mark.postgres
def test_restamp_keeps_each_row_source_and_provenance(
    clean_runtime: str,
) -> None:
    """Issue #3318 (a) and (d): a restamp rewrites only the searchable text.
    Source, record id, revision, run id, observed time and lineage stay the
    row's own; the text and its hash do change."""
    from datetime import datetime, timezone

    _seed_job_subtype(clean_runtime)
    observed = datetime(2026, 10, 1, 12, 0, tzinfo=timezone.utc)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        ChronosGraphStore.from_connection(conn).apply_facts(
            [_job_node("job:1")],
            source=_RESTAMP_SOURCE,
            run_id="run-original",
            observed_at=observed,
        )
    before = _provenance(clean_runtime, "job:1")
    assert before["attrs"]["text"] == "Job job:1"

    _restamp_declaration_change(clean_runtime)

    after = _provenance(clean_runtime, "job:1")
    assert after["attrs"]["text"] == "Job job:1\n\npartition-job:1", after
    assert after["text_hash"] != before["text_hash"]
    assert after["source"] == _RESTAMP_SOURCE, after["source"]
    assert after["source_record_id"] == {"job": "job:1"}
    assert after["source_revision"] == {"run": "r1", "rev": 7}
    assert after["run_id"] == "run-original"
    assert after["observed_at"] == observed
    assert (
        after["attrs"]["nomos_source_lineage"]
        == before["attrs"]["nomos_source_lineage"]
    )

    # A second restamp under the same declaration writes nothing new.
    with psycopg.connect(clean_runtime) as conn:
        from okg.substrate.catalog.modules_apply import (
            _restamp_changed_subtypes,
        )

        summary = _restamp_changed_subtypes(
            conn, [_RESTAMP_SUBTYPE], deployment_name="default",
            catalog_version_id=_latest_cv(conn),
            prior_declarations={_RESTAMP_SUBTYPE: (["title", "label"], [])},
        )
        conn.commit()
    assert summary == {}, summary
    assert _provenance(clean_runtime, "job:1") == after


def _latest_cv(conn: psycopg.Connection) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT max(catalog_version_id) FROM okg.catalog_versions "
            " WHERE status = 'applied'"
        )
        return int(cur.fetchone()[0])


def _visible_job_ids(dsn: str) -> list[str]:
    store = ChronosGraphStore.connect(dsn)
    try:
        rows = store.checkout().query(
            "SELECT node_id FROM okg.graph_nodes WHERE subtype = :subtype "
            " ORDER BY node_id",
            {"subtype": _RESTAMP_SUBTYPE},
        )
        return [str(row["node_id"]) for row in rows]
    finally:
        store.close()


@pytest.mark.postgres
def test_restamped_node_is_retracted_by_its_source_next_completed_scope(
    clean_runtime: str,
) -> None:
    """Issue #3318 (b) and (c), through the real source runner: after a
    restamp the source still owns its nodes. Its next completed scope that
    omits one retracts it, and a node it emits again is an unchanged
    re-emission: no write and no change of owner."""
    _seed_job_subtype(clean_runtime)
    adapter = _JobAdapter([
        ["job:a", "job:b", "job:c"],
        ["job:a", "job:c"],
    ])
    runner = _job_runner(clean_runtime)
    first = runner.run(adapter, mode="scope_complete")
    assert first.nodes_appended == 3

    _restamp_declaration_change(clean_runtime)
    restamped = _provenance(clean_runtime, "job:a")
    assert restamped["attrs"]["text"] == "Job job:a\n\npartition-job:a"

    second = runner.run(adapter, mode="scope_complete")
    assert second.retractions_node == 1
    assert _visible_job_ids(clean_runtime) == ["job:a", "job:c"]
    assert restamped["source"] == _RESTAMP_SOURCE, restamped["source"]
    # The re-emitted nodes match what the restamp stored, so nothing is
    # written and the rows are the restamped ones, still the source's own.
    assert second.nodes_appended == 0, second
    assert _provenance(clean_runtime, "job:a") == restamped


def _publish_checkpoint(dsn: str, generation_id: int) -> None:
    """A published generation between the source's write and the old
    restamp, as on a live deployment: the restamp then writes a new row
    version and the source's version stays in the branch history."""
    from tests.substrate._db_helpers import ensure_published_generation

    with psycopg.connect(dsn, autocommit=True) as conn:
        ensure_published_generation(
            conn, generation_id, deployment_name="default",
        )


def _repair(dsn: str, *, apply: bool) -> dict:
    from okg.substrate.catalog.restamp_repair import repair_restamp_ownership

    with psycopg.connect(dsn) as conn:
        report = repair_restamp_ownership(
            conn, deployment_name="default", apply=apply,
        )
        conn.commit()
    return report


@pytest.mark.postgres
def test_repair_returns_old_restamp_rows_to_their_source(
    clean_runtime: str,
) -> None:
    """Issue #3318 repair: rows the OLD restamp took over go back to the
    source that wrote them, from the row version the restamp replaced. The
    report is read-only by default, counts rows per original source, and a
    second apply finds nothing. The source's next completed scope then
    retracts the row it no longer emits."""
    _seed_job_subtype(clean_runtime)
    adapter = _JobAdapter([
        ["job:a", "job:b", "job:c"],
        ["job:a", "job:c"],
    ])
    runner = _job_runner(clean_runtime)
    runner.run(adapter, mode="scope_complete")
    originals = {
        node_id: _provenance(clean_runtime, node_id)
        for node_id in ("job:a", "job:b", "job:c")
    }
    _publish_checkpoint(clean_runtime, 1)
    _pre_fix_restamp(clean_runtime)
    taken = _provenance(clean_runtime, "job:b")
    assert taken["source"] == "_catalog:restamp"
    assert taken["source_revision"] == {}
    restamped_text = taken["attrs"]["text"]

    dry = _repair(clean_runtime, apply=False)
    assert dry["mode"] == "dry_run"
    assert dry["restamp_owned"] == 3
    assert dry["resolved_by_source"] == {_RESTAMP_SOURCE: 3}
    assert dry["unresolved"] == 0
    assert dry["repaired"] == 0
    assert _provenance(clean_runtime, "job:b") == taken

    applied = _repair(clean_runtime, apply=True)
    assert applied["mode"] == "apply"
    assert applied["repaired"] == 3
    assert applied["repaired_by_source"] == {_RESTAMP_SOURCE: 3}
    for node_id, original in originals.items():
        repaired = _provenance(clean_runtime, node_id)
        assert repaired["source"] == _RESTAMP_SOURCE, repaired
        assert repaired["source_record_id"] == original["source_record_id"]
        assert repaired["source_revision"] == original["source_revision"]
        assert repaired["run_id"] == original["run_id"]
        assert (
            repaired["attrs"]["nomos_source_lineage"]
            == original["attrs"]["nomos_source_lineage"]
        )
    assert _provenance(clean_runtime, "job:b")["attrs"]["text"] == (
        restamped_text
    )

    again = _repair(clean_runtime, apply=True)
    assert again["restamp_owned"] == 0
    assert again["repaired"] == 0

    second = runner.run(adapter, mode="scope_complete")
    assert second.retractions_node == 1
    assert _visible_job_ids(clean_runtime) == ["job:a", "job:c"]


@pytest.mark.postgres
def test_repair_refuses_a_row_whose_source_it_cannot_find(
    clean_runtime: str, capsys,
) -> None:
    """A row the old restamp overwrote in the same branch segment has no
    earlier version left to name its source. The repair counts it as
    unresolved, leaves it untouched even with --apply, and the command
    exits non-zero so the operator sees it."""
    import json as _json

    from okg.substrate.cli.main import main

    _seed_job_subtype(clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        ChronosGraphStore.from_connection(conn).apply_facts(
            [_job_node("job:lost")], source=_RESTAMP_SOURCE,
        )
    _pre_fix_restamp(clean_runtime)
    taken = _provenance(clean_runtime, "job:lost")
    assert taken["source"] == "_catalog:restamp"

    exit_code = main([
        "--json", "catalog", "repair-restamp-ownership",
        "--deployment", "default", "--dsn", clean_runtime, "--apply",
    ])
    report = _json.loads(capsys.readouterr().out)
    assert exit_code != 0
    assert report["restamp_owned"] == 1
    assert report["unresolved"] == 1
    assert report["unresolved_by_reason"] == {"no_earlier_version": 1}
    assert report["unresolved_samples"][0]["node_id"] == "job:lost"
    assert report["repaired"] == 0
    assert _provenance(clean_runtime, "job:lost") == taken


def _kept(source: str | None = _RESTAMP_SOURCE, **extra) -> dict:
    return {
        "source": source, "run_id": None, "observed_at": None,
        "expected_source": source, **extra,
    }


@pytest.mark.parametrize(
    ("facts", "provenance", "kwargs", "message"),
    [
        pytest.param(
            [_job_node("job:1")], {"job:1": _kept()},
            {"source": "_catalog:restamp"}, "no batch source",
            id="batch-source",
        ),
        pytest.param(
            [NodeFact("job:1", _RESTAMP_SUBTYPE, {}, {}, {}, op="R")],
            {"job:1": _kept()}, {}, "authoritative node inserts",
            id="retraction",
        ),
        pytest.param(
            [NodeFact(
                "job:1", _RESTAMP_SUBTYPE, {}, {}, {},
                ownership_mode="substitute",
            )],
            {"job:1": _kept()}, {}, "authoritative node inserts",
            id="substitute",
        ),
        pytest.param(
            [_job_node("job:1")], {}, {}, "no entry",
            id="missing-entry",
        ),
        pytest.param(
            [_job_node("job:1")], {"job:1": {"source": "s"}}, {},
            "malformed", id="malformed-entry",
        ),
    ],
)
def test_row_provenance_refuses_a_write_that_could_move_ownership(
    facts, provenance, kwargs, message,
) -> None:
    """Issue #3318: the provenance-preserving rewrite accepts only
    authoritative inserts of nodes that each name their kept provenance,
    under no batch source. Anything else is refused before any read."""
    from okg.substrate.graph_store_internal.writes import (
        _validate_row_provenance,
    )

    with pytest.raises(ValueError, match=message):
        _validate_row_provenance(
            facts, provenance,
            source=kwargs.get("source"), derived_reapplication=False,
        )


@pytest.mark.postgres
def test_row_provenance_refuses_a_row_another_writer_moved(
    clean_runtime: str,
) -> None:
    """Issue #3318: the rewrite re-reads each row under the branch write
    lock. A row whose source is no longer the one the caller read is
    refused, and nothing is written."""
    from okg.substrate.errors import SubstrateError

    _seed_job_subtype(clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        ChronosGraphStore.from_connection(conn).apply_facts(
            [_job_node("job:1")], source=_RESTAMP_SOURCE,
        )
    before = _provenance(clean_runtime, "job:1")
    changed = _job_node("job:1")
    changed.attrs["title"] = "Moved"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with pytest.raises(SubstrateError) as refused:
            ChronosGraphStore.from_connection(conn).apply_facts(
                [changed],
                row_provenance={
                    "job:1": _kept("other.source", expected_source="other.source"),
                },
            )
    assert refused.value.data["reason"] == "row_provenance_rows_moved"
    assert refused.value.data["moved"][0]["current_source"] == _RESTAMP_SOURCE
    assert _provenance(clean_runtime, "job:1") == before


@pytest.mark.postgres
def test_repair_names_nodes_the_source_dropped_and_a_reset_cursor_run_clears_them(
    clean_runtime: str, capsys,
) -> None:
    """Review B1 of #3318, the live SubMIT case: the source dropped the
    node while the old restamp owned it, so its stored record set no longer
    names it. Repairing ownership alone leaves it published: a normal run
    diffs only against that record set. The repair names such nodes per
    source, and the follow-up it prints, a reset-cursor run, retracts them
    and nothing else."""
    from okg.substrate.cli.main import main

    _seed_job_subtype(clean_runtime)
    adapter = _JobAdapter([
        ["job:a", "job:b", "job:c"],
        ["job:a", "job:c"],
        ["job:a", "job:c"],
        ["job:a", "job:c"],
    ])
    runner = _job_runner(clean_runtime)
    runner.run(adapter, mode="scope_complete")
    _publish_checkpoint(clean_runtime, 1)
    _pre_fix_restamp(clean_runtime)

    dropped = runner.run(adapter, mode="scope_complete")
    assert dropped.retractions_node == 1
    assert _visible_job_ids(clean_runtime) == ["job:a", "job:b", "job:c"], (
        "the old restamp's takeover must still hide job:b from its source"
    )

    exit_code = main([
        "catalog", "repair-restamp-ownership",
        "--deployment", "default", "--dsn", clean_runtime,
    ])
    text = capsys.readouterr().out
    assert exit_code == 0
    assert "job:b" in text
    assert (
        f"okg ingest --deployment default --include {_RESTAMP_SOURCE} "
        "--no-predecessors --inline --reset-cursor"
    ) in text, text

    # The source's re-emission already took job:a and job:c back; only the
    # dropped node is still owned by the restamp, as on SubMIT.
    applied = _repair(clean_runtime, apply=True)
    assert applied["restamp_owned"] == 1
    assert applied["repaired"] == 1
    assert applied["pending_retraction"] == {
        _RESTAMP_SOURCE: {"count": 1, "node_ids": ["job:b"]},
    }
    assert applied["pending_retraction_unchecked"] == {}
    assert _provenance(clean_runtime, "job:b")["source"] == _RESTAMP_SOURCE

    normal = runner.run(adapter, mode="scope_complete")
    assert normal.retractions_node == 0
    assert _visible_job_ids(clean_runtime) == ["job:a", "job:b", "job:c"], (
        "a normal run diffs only against the stored record set"
    )

    follow_up = runner.run(adapter, mode="scope_complete", reset_cursor=True)
    assert follow_up.retractions_node == 1
    assert _visible_job_ids(clean_runtime) == ["job:a", "job:c"]
    assert _provenance(clean_runtime, "job:a")["source"] == _RESTAMP_SOURCE
    after = _repair(clean_runtime, apply=False)
    assert after["restamp_owned"] == 0
    assert after["pending_retraction"] == {}


@pytest.mark.postgres
def test_restamp_holds_the_branch_write_lock_from_read_to_write(
    clean_runtime: str, monkeypatch,
) -> None:
    """Review S1 of #3318: the restamp reads each row's provenance and text
    and writes them back. A graph writer that updated the row in between
    would be overwritten with the stale copy. The restamp takes the branch
    write lock before it reads, so a same-source update waits until the
    restamp commits and then lands on top of it."""
    import okg.substrate.fact_application as fact_application
    from okg.substrate.graph_store_internal.writes import _is_lock_timeout

    _seed_job_subtype(clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        ChronosGraphStore.from_connection(conn).apply_facts(
            [_job_node("job:1")], source=_RESTAMP_SOURCE,
        )
    real = fact_application.apply_bounded_fact_batch
    blocked: list[bool] = []

    def update_between_read_and_write(*args, **kwargs):
        update = _job_node("job:1")
        update.attrs["title"] = "Updated between read and write"
        with psycopg.connect(clean_runtime, autocommit=True) as other:
            other.execute("SET lock_timeout = '300ms'")
            try:
                ChronosGraphStore.from_connection(other).apply_facts(
                    [update], source=_RESTAMP_SOURCE,
                )
            except Exception as exc:  # noqa: BLE001 - classified below.
                blocked.append(_is_lock_timeout(exc))
            else:
                blocked.append(False)
        return real(*args, **kwargs)

    monkeypatch.setattr(
        fact_application, "apply_bounded_fact_batch",
        update_between_read_and_write,
    )
    _restamp_declaration_change(clean_runtime)
    monkeypatch.setattr(fact_application, "apply_bounded_fact_batch", real)

    assert blocked == [True], (
        "a same-source update must wait for the restamp's branch lock"
    )
    assert _provenance(clean_runtime, "job:1")["attrs"]["text"] == (
        "Job job:1\n\npartition-job:1"
    )


@pytest.mark.postgres
def test_catalog_refusal_names_the_nodes_that_moved(
    clean_runtime: str, monkeypatch,
) -> None:
    """Review S2 of #3318: when the provenance-preserving rewrite refuses
    moved rows, the catalog refusal carries the reason and the node ids and
    tells the operator what to do."""
    import okg.substrate.catalog.modules_apply as modules_apply
    from okg.substrate.catalog.modules_compose import ComposedCatalog
    from okg.substrate.errors import CatalogMigrationRefused, SubstrateError

    def refuse(*args, **kwargs):
        raise SubstrateError(
            "a node changed while its provenance was being preserved",
            data={
                "reason": "row_provenance_rows_moved",
                "moved": [{"node_id": "job:1"}],
                "moved_count": 1,
            },
        )

    monkeypatch.setattr(modules_apply, "_restamp_changed_subtypes", refuse)
    composed = ComposedCatalog(
        deployment_name="restamp-moved",
        modules=["test"],
        subtypes={"concept"},
        subtype_origin={"concept": "test"},
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with pytest.raises(CatalogMigrationRefused) as refused:
            modules_apply.apply_composed_catalog(conn, composed, identity=None)
    data = refused.value.data
    assert data["stage"] == "searchable_text_restamp"
    assert data["reason"] == "row_provenance_rows_moved"
    assert data["moved_node_ids"] == ["job:1"]
    assert "pause the worker" in refused.value.fix_hint


@pytest.mark.postgres
def test_repair_refuses_a_version_without_lineage_and_says_what_it_wrote(
    clean_runtime: str, capsys, monkeypatch,
) -> None:
    """Review N1 and S3 of #3318: an earlier version that carries no
    lineage is reported, not given a lineage the repair made up. With
    --apply the rows it could resolve are written anyway, the exit is 1 for
    the rest, and the output says both."""
    import okg.substrate.graph_store_internal.writes as writes
    from okg.substrate.cli.main import main

    _seed_job_subtype(clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts([_job_node("job:ok")], source=_RESTAMP_SOURCE)
        with monkeypatch.context() as patcher:
            patcher.setattr(
                writes, "stamp_attrs_with_source_lineage",
                lambda attrs, **_: dict(attrs or {}),
            )
            store.apply_facts([_job_node("job:bare")], source=_RESTAMP_SOURCE)
    assert "nomos_source_lineage" not in (
        _provenance(clean_runtime, "job:bare")["attrs"]
    )
    _publish_checkpoint(clean_runtime, 1)
    _pre_fix_restamp(clean_runtime)
    taken = _provenance(clean_runtime, "job:bare")

    exit_code = main([
        "catalog", "repair-restamp-ownership",
        "--deployment", "default", "--dsn", clean_runtime, "--apply",
    ])
    text = capsys.readouterr().out
    assert exit_code == 1
    assert "earlier_version_no_lineage: 1" in text, text
    assert "repaired 1" in text, text
    assert "were written" in text, text
    assert _provenance(clean_runtime, "job:ok")["source"] == _RESTAMP_SOURCE
    assert _provenance(clean_runtime, "job:bare") == taken


def test_follow_up_command_selects_only_the_source() -> None:
    """Review S4 of #3318: `--include` alone pulls in the source's
    predecessors, and `--reset-cursor` would reset every source selected.
    The printed follow-up parses as an inline reset-cursor ingest of that
    one source with predecessor expansion off."""
    import shlex

    from okg.substrate.catalog.restamp_repair import follow_up_command
    from okg.substrate.cli.main import _build_parser

    command = follow_up_command("dep", "slurm.jobs")
    argv = shlex.split(command)
    assert argv[0] == "okg"
    args = _build_parser().parse_args(argv[1:])
    assert args.command == "ingest"
    assert args.deployment == "dep"
    assert args.include == "slurm.jobs"
    assert args.no_predecessors is True
    assert args.inline is True
    assert args.reset_cursor is True


@pytest.mark.postgres
def test_repair_dry_run_takes_no_lock_and_runs_no_ddl(
    clean_runtime: str,
) -> None:
    """Review S5 of #3318: a dry run must not hold the branch write lock or
    take a table lock. While its transaction is still open, a graph writer
    on the branch goes through, and the dry run's session holds no
    advisory or exclusive lock."""
    from okg.substrate.catalog.restamp_repair import repair_restamp_ownership

    _seed_job_subtype(clean_runtime)
    adapter = _JobAdapter([["job:a", "job:b"], ["job:a"]])
    runner = _job_runner(clean_runtime)
    runner.run(adapter, mode="scope_complete")
    _publish_checkpoint(clean_runtime, 1)
    _pre_fix_restamp(clean_runtime)
    runner.run(adapter, mode="scope_complete")

    with psycopg.connect(clean_runtime) as dry:
        report = repair_restamp_ownership(
            dry, deployment_name="default", apply=False,
        )
        assert report["pending_retraction"] == {
            _RESTAMP_SOURCE: {"count": 1, "node_ids": ["job:b"]},
        }
        pid = dry.info.backend_pid
        with psycopg.connect(clean_runtime, autocommit=True) as other:
            held = other.execute(
                "SELECT locktype, mode FROM pg_locks WHERE pid = %s "
                "AND (locktype = 'advisory' OR mode = 'AccessExclusiveLock')",
                (pid,),
            ).fetchall()
            assert held == [], held
            other.execute("SET lock_timeout = '300ms'")
            ChronosGraphStore.from_connection(other).apply_facts(
                [_job_node("job:z")], source=_RESTAMP_SOURCE,
            )
        dry.rollback()
    assert _provenance(clean_runtime, "job:z")["source"] == _RESTAMP_SOURCE


@pytest.mark.postgres
def test_repair_marks_a_source_without_a_record_set_for_inspection(
    clean_runtime: str, capsys,
) -> None:
    """Review nit of #3318: a source with no stored record set gets no
    reset-cursor command, which would not retract anything for it; the
    output tells the operator to inspect it instead."""
    from okg.substrate.cli.main import main

    _seed_job_subtype(clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        ChronosGraphStore.from_connection(conn).apply_facts(
            [_job_node("job:feed")], source="test.append-feed",
        )
    _publish_checkpoint(clean_runtime, 1)
    _pre_fix_restamp(clean_runtime)

    exit_code = main([
        "catalog", "repair-restamp-ownership",
        "--deployment", "default", "--dsn", clean_runtime,
    ])
    text = capsys.readouterr().out
    assert exit_code == 0
    assert (
        "inspect: test.append-feed has no stored record set; "
        "a reset-cursor run will not retract"
    ) in text, text
    assert "okg ingest" not in text, text
