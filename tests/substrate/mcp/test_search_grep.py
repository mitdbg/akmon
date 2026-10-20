"""`search(method="grep")` on a published fixture graph (#3067, PACT mcp-grep-3067).

PostgreSQL tests: the trigram match against the published segment, folding
into records, path scoping, the cap and its lower bound, the timeout, the
refusals that need the database, and Nomos under `enforce` (a hidden row is
never counted or served; a masked row is counted and served only on its
masked text). Synthetic data only.
"""
from __future__ import annotations

import uuid

import psycopg
from psycopg.types.json import Jsonb
import pytest

from okg.substrate.library.sources.base import EdgeFact, NodeFact
from okg.substrate.mcp import grep_search, open_session, search
from okg.substrate.mcp.grep_query import GrepQueryInvalid, GrepRegexUnindexable, parse_grep_query
from okg.substrate.mcp.grep_search import GrepTimeout
from okg.substrate.mcp.record_fold import SearchFoldReadCheckMismatch
from tests.pytest.catalog_fixture_builders import SubtypeSpec
from tests.substrate.mcp.search_fixture import make_search_deployment, publish_nodes

pytestmark = pytest.mark.postgres

OPEN = "connector:open_docs"
SECRET = "connector:secret_docs"
MASKED = "connector:masked_docs"
MAIN = "system:analyst_search_fixture"
READER = "subject:grep-reader"

DOC = SubtypeSpec(
    "doc",
    metadata={
        "searchable_text_fields": ["text"],
        "passage_rollup": {"edge_type": "contains", "child_subtype": "chunk"},
    },
)
CHUNK = SubtypeSpec("chunk", metadata={"searchable_text_fields": ["text"]})
NOTE = SubtypeSpec("note", metadata={"searchable_text_fields": ["text"]})
SUBTYPES = [DOC, CHUNK, NOTE]
NARROWINGS = [("doc", "contains", "chunk")]


def _node(node_id: str, subtype: str, text: str, **attrs) -> NodeFact:
    return NodeFact(
        node_id=node_id, subtype=subtype, attrs={"text": text, **attrs},
        source_record_id={"id": node_id}, source_revision={"v": 1},
    )


def _edge(src: str, dst: str) -> EdgeFact:
    return EdgeFact(
        src=src, dst=dst, edge_type="contains",
        source_record_id={"id": f"{src}|contains|{dst}"}, source_revision={"v": 1},
    )


def _publish(tmp_path, dsn: str, main: list, groups: dict | None = None) -> str:
    deployment = make_search_deployment(tmp_path, dsn=dsn)
    publish_nodes(
        dsn, deployment, subtypes=SUBTYPES, narrowings=NARROWINGS,
        nodes=[f for f in main if isinstance(f, NodeFact)],
        edges=[f for f in main if isinstance(f, EdgeFact)],
        node_groups=list((groups or {}).items()),
    )
    return deployment


def _corpus() -> list:
    return [
        _node("doc:a", "doc", "Doc A calls make_uuid4() at line two.\nSecond line here.",
              path="app/pkg/a.py"),
        _node("chunk:a:0", "chunk", "a chunk mentioning nothing special", chunk_ordinal=0),
        _node("chunk:a:1", "chunk", "Chunk one: UUID4 appears\nand Zirconium too.",
              chunk_ordinal=1),
        _edge("doc:a", "chunk:a:0"),
        _edge("doc:a", "chunk:a:1"),
        _node("doc:b", "doc", "Doc B: plain words only.", path="manual/guide.md"),
        _node("chunk:b:0", "chunk", "The generation   publish   step, uuid4 here.",
              chunk_ordinal=0),
        _edge("doc:b", "chunk:b:0"),
        _node("doc:c", "doc", "zirconium cladding report; uuid absent", path="app/pkg/c.py"),
        _node("note:1", "note", "standalone uuid4 note"),
        _node("note:2", "note", "order 555-1234 and 777-9999"),
    ]


def _grep(dsn: str, deployment: str, query: str, **kwargs) -> dict:
    with psycopg.connect(dsn, autocommit=True) as conn:
        session = open_session(
            conn, deployment_name=deployment,
            **({"nomos_subject": kwargs.pop("subject")} if "subject" in kwargs else {}),
        )
        return search(conn, session, query, method="grep", **kwargs)


def _plain(value):
    """The source text inside an untrusted fence (the fixture producer is
    untrusted, so every excerpt arrives fenced)."""
    if isinstance(value, str) and value.startswith("\u27e6UNTRUSTED_DATA"):
        lines = value.split("\n")
        assert lines[-1] == "\u27e6/UNTRUSTED_DATA\u27e7"
        return "\n".join(lines[1:-1])
    return value


def _excerpts(row: dict) -> list:
    return [_plain(w["excerpt"]) for w in row["match_windows"]]


def _ids(page: dict) -> list[str]:
    return [row["node_id"] for row in page["results"]]


def _by_id(page: dict) -> dict:
    return {row["node_id"]: row for row in page["results"]}


# ---------- matching ----------

def test_a_substring_inside_a_word_matches_case_insensitively_and_folds(
    clean_runtime, tmp_path,
):
    deployment = _publish(tmp_path, clean_runtime, _corpus())
    page = _grep(clean_runtime, deployment, "uuid4")
    # Passages folded into their record; records ranked by BM25 on the
    # literal word (PACT mcp-grep-rank, test_search_grep_rank.py holds the
    # order), so this test checks the set.
    assert sorted(_ids(page)) == ["doc:a", "doc:b", "note:1"]
    # The count is of the records the page lists (PACT mcp-grep-page).
    assert page["total_count"] == 3 and page["count_unit"] == "record"
    assert page["total_count_basis"] == "exact"
    assert page["result_unit"] == "record" and page["and_scope"] == "text_unit"
    assert page["rank_by"] == "records" and page["method"] == "grep"
    rows = _by_id(page)
    # doc:a matched on its own text ("make_uuid4") and through chunk:a:1.
    assert rows["doc:a"]["matching_passages"] == 1
    windows = rows["doc:a"]["match_windows"]
    # #3666: neither line is a line `inspect` text reads back (the record
    # has no `document_chunk` text), so each says what it counts.
    assert {k: v for k, v in windows[0].items() if k != "excerpt"} == {
        "char_start": 17, "char_end": 22, "line": 1, "line_basis": "own_text",
    }
    assert windows[1]["line_basis"] == "passage"
    assert _excerpts(rows["doc:a"]) == [
        "Doc A calls make_uuid4() at line two.", "Chunk one: UUID4 appears",
    ]
    assert windows[1]["passage_id"] == "chunk:a:1"
    assert windows[1]["line"] == 1
    assert "make_uuid4" in rows["doc:a"]["excerpt"]
    # doc:b matched only through its chunk: no own excerpt, one window.
    assert rows["doc:b"]["excerpt"] is None
    assert [w["passage_id"] for w in rows["doc:b"]["match_windows"]] == ["chunk:b:0"]


def test_and_applies_within_one_text_unit_not_across_passages(clean_runtime, tmp_path):
    deployment = _publish(tmp_path, clean_runtime, _corpus())
    # doc:a holds "make_uuid4" in its own text and "zirconium" in a chunk that
    # also says UUID4: the chunk matches both; doc:c matches zirconium only.
    page = _grep(clean_runtime, deployment, "uuid4 zirconium")
    assert _ids(page) == ["doc:a"]
    assert [w.get("passage_id") for w in page["results"][0]["match_windows"]] == [
        "chunk:a:1", "chunk:a:1",
    ]
    # "two" is in doc:a's own text, "zirconium" only in its chunk: no match.
    assert _grep(clean_runtime, deployment, "two zirconium")["total_count"] == 0


def test_phrase_near_exclusion_and_regex(clean_runtime, tmp_path):
    deployment = _publish(tmp_path, clean_runtime, _corpus())
    assert _ids(_grep(clean_runtime, deployment, '"generation publish step"')) == ["doc:b"]
    assert _ids(_grep(clean_runtime, deployment, "cladding NEAR/20 zirconium")) == ["doc:c"]
    assert _ids(_grep(clean_runtime, deployment, "zirconium NEAR/3 report")) == []
    # PACT grep-near-operands-3251: the operand prefilter keeps the exact
    # distance (the gap is " cladding ", 10 characters) and the case folding.
    assert _ids(_grep(clean_runtime, deployment, "zirconium NEAR/10 report")) == ["doc:c"]
    assert _grep(clean_runtime, deployment, "zirconium NEAR/9 report")["total_count"] == 0
    assert _ids(_grep(clean_runtime, deployment, "CLADDING NEAR/20 Zirconium")) == ["doc:c"]
    assert sorted(_ids(_grep(clean_runtime, deployment, "uuid4 -standalone"))) == [
        "doc:a", "doc:b",
    ]
    page = _grep(clean_runtime, deployment, r"re:/make_uuid\d\(\)/")
    assert _ids(page) == ["doc:a"]
    assert page["grep_terms"] == [{
        "kind": "regex", "term": r"make_uuid\d\(\)", "negated": False, "indexable": True,
    }]


def test_an_unindexable_regex_is_refused_unscoped_and_runs_scoped(clean_runtime, tmp_path):
    deployment = _publish(tmp_path, clean_runtime, _corpus())
    with pytest.raises(GrepRegexUnindexable):
        _grep(clean_runtime, deployment, r"re:/\d{3}-\d{4}/")
    page = _grep(clean_runtime, deployment, r"re:/\d{3}-\d{4}/", filter={"subtype": "note"})
    assert _ids(page) == ["note:2"]
    assert page["result_unit"] == "record"
    assert _excerpts(page["results"][0]) == [
        "order 555-1234 and 777-9999",
    ]


def test_a_pattern_postgresql_rejects_is_refused_before_any_row_is_read(
    clean_runtime, tmp_path,
):
    deployment = _publish(tmp_path, clean_runtime, _corpus())
    # Python reads `[[:nope:]]` as a class; PostgreSQL refuses the class name.
    with pytest.raises(GrepQueryInvalid) as raised:
        _grep(clean_runtime, deployment, "re:/abc[[:nope:]]/")
    assert "PostgreSQL rejected" in str(raised.value)


def test_rank_by_other_than_records_is_refused():
    from okg.substrate.mcp.passage_rollup import RankByNotApplicable

    with pytest.raises(RankByNotApplicable, match="ranks records only") as raised:
        search(None, None, "uuid", method="grep", rank_by="self")  # type: ignore[arg-type]
    assert raised.value.code == "rank_by_not_applicable"
    with pytest.raises(ValueError, match="path_prefix applies only"):
        search(None, None, "uuid", method="lexical", path_prefix="app/")  # type: ignore[arg-type]


# ---------- scoping ----------

def test_path_prefix_scopes_records_and_their_passages(clean_runtime, tmp_path):
    deployment = _publish(tmp_path, clean_runtime, _corpus())
    page = _grep(clean_runtime, deployment, "uuid4", path_prefix="app/")
    # doc:a by its own path (and its chunk through it); doc:b is under manual/.
    assert _ids(page) == ["doc:a"]
    assert page["path_prefix"] == "app/"
    assert page["results"][0]["matching_passages"] == 1
    # A chunk carries no path: it is in scope only through its record.
    assert _ids(_grep(clean_runtime, deployment, "generation", path_prefix="manual/")) == [
        "doc:b",
    ]
    # LIKE metacharacters in the prefix are literal.
    assert _grep(clean_runtime, deployment, "uuid4", path_prefix="app%")["total_count"] == 0


def test_a_subtype_filter_naming_a_record_type_folds_its_passages(clean_runtime, tmp_path):
    deployment = _publish(tmp_path, clean_runtime, _corpus())
    page = _grep(clean_runtime, deployment, "uuid4", filter={"subtype": "doc"})
    assert sorted(_ids(page)) == ["doc:a", "doc:b"]
    passages = _grep(clean_runtime, deployment, "uuid4", filter={"subtype": "chunk"})
    assert passages["result_unit"] == "passage"
    assert sorted(_ids(passages)) == ["chunk:a:1", "chunk:b:0"]


# ---------- cap, paging, timeout ----------

def test_past_the_cap_the_count_is_a_lower_bound(clean_runtime, tmp_path, monkeypatch):
    facts = [_node(f"note:{i:03d}", "note", f"common token {i}") for i in range(12)]
    deployment = _publish(tmp_path, clean_runtime, facts)
    exact = _grep(clean_runtime, deployment, "common", limit=5)
    assert exact["total_count"] == 12 and exact["total_count_basis"] == "exact"
    assert exact["next_offset"] == 5 and "scan_cap" not in exact
    monkeypatch.setattr(grep_search, "COUNT_SCAN_BOUND", 10)
    capped = _grep(clean_runtime, deployment, "common", limit=5, offset=5)
    # Never below the records the page found (all twelve, in one batch).
    assert capped["total_count"] == 12
    assert capped["total_count_basis"] == "lower_bound"
    assert capped["scan_cap"] == {"rows": 10}
    assert capped["count_basis_reasons"] == ["scan_cap"]
    # The count's bound never limits the page (PACT mcp-grep-page): equal
    # scores, so the page walks node-id order.
    assert _ids(capped) == [f"note:{i:03d}" for i in range(5, 10)]
    assert capped["next_offset"] == 10


def test_a_statement_past_the_timeout_is_refused_not_answered_empty(
    clean_runtime, tmp_path, monkeypatch,
):
    facts = [_node(f"note:{i:04d}", "note", "x" * 2000 + f" row {i}") for i in range(3000)]
    deployment = _publish(tmp_path, clean_runtime, facts)
    monkeypatch.setattr(grep_search, "STATEMENT_TIMEOUT_MS", 1)
    with psycopg.connect(clean_runtime) as conn:
        session = open_session(conn, deployment_name=deployment)
        conn.execute("SET statement_timeout = '7s'")
        with pytest.raises(GrepTimeout) as raised:
            search(conn, session, r"re:/\d{9}/", method="grep", filter={"subtype": "note"})
        assert raised.value.code == "grep_timeout"
        assert "path_prefix" in raised.value.fix_hint
        # The grep timeout never outlives its statement.
        assert conn.execute("SHOW statement_timeout").fetchone()[0] == "7s"


def test_the_index_is_built_on_the_live_segment(clean_runtime, tmp_path):
    deployment = _publish(tmp_path, clean_runtime, _corpus())
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        defs = [
            row[0] for row in conn.execute(
                "SELECT indexdef FROM pg_indexes WHERE indexname ~ "
                "'(node_text_trgm_visible|node_path_prefix_visible)$'"
            )
        ]
        assert any("gin_trgm_ops" in d for d in defs)
        assert any("text_pattern_ops" in d for d in defs)
        session = open_session(conn, deployment_name=deployment)
        assert grep_search.resolve_trgm_segment(conn)
        del session


# ---------- Nomos under enforce ----------

def _activate(dsn: str, deployment: str, rules: list[dict]) -> None:
    from okg.substrate.nomos import activate_policy_bundle, compile_policy_graph

    bundle = compile_policy_graph({
        "policy_bundle_id": f"nomos:test:grep:{uuid.uuid4().hex[:8]}",
        "policy_version": "v1",
        "default_effect": "deny",
        "rules": rules,
    })
    with psycopg.connect(dsn) as conn:
        activate_policy_bundle(
            conn, deployment_name=deployment, bundle=bundle,
            scope=f"deployment:{deployment}", activated_by="test:operator",
        )
        conn.commit()


def _allow(source: str, priority: int = 100, **match) -> dict:
    return {
        "rule_id": f"allow_{source}_{priority}".replace(":", "_"),
        "effect": "allow", "actions": ["graph.read.mcp"], "subjects": [READER],
        "match": {"source_id": source, **match}, "priority": priority,
    }


def _nomos_deployment(tmp_path, dsn: str, rules: list[dict]) -> str:
    groups = {
        OPEN: [
            _node("doc:open", "doc", "Open doc.", path="app/open.py"),
            _node("chunk:open:0", "chunk", "visible quasar finding"),
            _node("note:open", "note", "a quasar note anyone may read"),
        ],
        SECRET: [
            _node("doc:secret", "doc", "Secret doc with quasar inside.", path="app/secret.py"),
            _node("chunk:secret:0", "chunk", "secret quasar finding"),
            _node("note:secret", "note", "a quasar note nobody may read"),
        ],
        MASKED: [_node("note:masked", "note", "quasar words that are masked")],
    }
    main = [
        _edge("doc:open", "chunk:open:0"),
        _edge("doc:open", "chunk:secret:0"),
        _edge("doc:secret", "chunk:open:0"),
    ]
    deployment = _publish(tmp_path, dsn, main, groups)
    _activate(dsn, deployment, rules)
    return deployment


_VISIBLE = [_allow(OPEN), _allow(MAIN), _allow(MASKED)]


def test_under_enforce_a_hidden_row_is_never_counted_or_served(
    clean_runtime, tmp_path, monkeypatch,
):
    deployment = _nomos_deployment(tmp_path, clean_runtime, _VISIBLE)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    page = _grep(clean_runtime, deployment, "quasar", subject=READER)
    ids = _ids(page)
    assert "doc:secret" not in ids and "note:secret" not in ids
    assert "chunk:secret:0" not in str(page) and "secret quasar" not in str(page)
    assert "nobody may read" not in str(page)
    # The count is over visible rows only: doc:open (via its readable chunk),
    # note:masked and note:open; chunk:open:0 has two parents, one hidden,
    # and folds into the visible one.
    assert ids == ["doc:open", "note:masked", "note:open"]
    assert page["total_count"] == 3 and page["total_count_basis"] == "exact"
    assert _by_id(page)["doc:open"]["matching_passages"] == 1
    # A hidden record cannot lend its path to a passage under it.
    scoped = _grep(clean_runtime, deployment, "quasar", subject=READER,
                   path_prefix="app/secret")
    assert scoped["total_count"] == 0 and scoped["results"] == []


def test_under_enforce_a_redacting_policy_counts_only_masked_matches(
    clean_runtime, tmp_path, monkeypatch,
):
    rules = [_allow(OPEN), _allow(MAIN), {
        "rule_id": "redact_masked", "effect": "redact", "actions": ["graph.read.mcp"],
        "subjects": [READER], "match": {"source_id": MASKED}, "priority": 100,
        "obligations": [{"type": "redaction"}, {"type": "audit"}],
    }]
    deployment = _nomos_deployment(tmp_path, clean_runtime, rules)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        conn.execute(
            "UPDATE okg.subtypes SET metadata = coalesce(metadata, '{}'::jsonb)"
            " || '{\"pii_classes\": {\"text\": \"free_text\"}}'::jsonb"
            " WHERE name = 'note'"
        )
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    page = _grep(clean_runtime, deployment, "quasar", subject=READER)
    # The masked note's raw text matches; its masked text does not, so it is
    # neither counted nor served, and the count says it is a lower bound.
    assert "note:masked" not in _ids(page)
    assert "masked" not in str([r.get("match_windows") for r in page["results"]])
    assert page["total_count_basis"] == "lower_bound"
    assert "redaction" in page["count_basis_reasons"]


def test_a_policy_that_counts_what_it_will_not_read_fails_the_call(
    clean_runtime, tmp_path, monkeypatch,
):
    """The scan site (a `count`) admits the secret note; the serve site (a
    `read`) hides it; the call fails rather than serve around it."""
    rules = [*_VISIBLE, _allow(SECRET, priority=400, nomos_read_purpose="count")]
    deployment = _nomos_deployment(tmp_path, clean_runtime, rules)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    with pytest.raises(SearchFoldReadCheckMismatch):
        _grep(clean_runtime, deployment, "quasar", subject=READER)


# ---------- review B1/B2: no oracle on what a redacting policy hides ----------

def _redacting_deployment(tmp_path, dsn: str, pii: dict[str, str]) -> str:
    groups = {
        OPEN: [
            _node("note:open", "note", "an open launch memo", path="open/memo.md",
                  owner="carol"),
            _node("chunk:open:lent", "chunk", "launch checklist under a masked doc"),
        ],
        MASKED: [
            _node("note:masked", "note", "quasar7 launch notes", path="secret/alice/x.md",
                  owner="alice"),
            _node("doc:masked", "doc", "Masked doc.", path="secret/alice/doc.md",
                  owner="alice"),
        ],
    }
    main = [_edge("doc:masked", "chunk:open:lent")]
    rules = [_allow(OPEN), _allow(MAIN), {
        "rule_id": "redact_masked", "effect": "redact", "actions": ["graph.read.mcp"],
        "subjects": [READER], "match": {"source_id": MASKED}, "priority": 100,
        "obligations": [{"type": "redaction"}, {"type": "audit"}],
    }]
    deployment = _publish(tmp_path, dsn, main, groups)
    _activate(dsn, deployment, rules)
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(
            "UPDATE okg.subtypes SET metadata = coalesce(metadata, '{}'::jsonb)"
            " || jsonb_build_object('pii_classes', %s::jsonb)"
            " WHERE name IN ('note', 'doc')",
            (Jsonb(pii),),
        )
    return deployment


def _same(secret: dict, control: dict) -> None:
    """A secret query and its control: indistinguishable to the reader."""
    for key in (
        "total_count", "total_count_basis", "results", "count_basis_reasons",
        "scan_cap", "page_basis", "page_scan_bound_hit", "next_offset",
        "next_offset_reason", "truncated", "matching_passages_basis", "rank_scan",
    ):
        assert secret.get(key) == control.get(key), key


@pytest.mark.parametrize("pii,secret,control,kwargs", [
    pytest.param(
        {"text": "free_text"}, r"re:/quasar7|^$/", r"re:/zzqqxx|^$/",
        {"filter": {"subtype": "note"}}, id="dropped-text",
    ),
    pytest.param(
        {"text": "quasi_identifier"}, "re:/quasar7|nomos:masked/",
        "re:/zzqqxx|nomos:masked/", {}, id="masked-text",
    ),
])
def test_a_regex_is_no_oracle_on_redacted_text(
    clean_runtime, tmp_path, monkeypatch, pii, secret, control, kwargs,
):
    deployment = _redacting_deployment(tmp_path, clean_runtime, pii)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    page = _grep(clean_runtime, deployment, secret, subject=READER, **kwargs)
    _same(page, _grep(clean_runtime, deployment, control, subject=READER, **kwargs))
    assert page["total_count"] == 0 and page["total_count_basis"] == "lower_bound"


ATTR_PII = {"text": "non_pii", "path": "quasi_identifier", "owner": "quasi_identifier"}


def test_path_prefix_is_no_oracle_on_a_masked_path(clean_runtime, tmp_path, monkeypatch):
    deployment = _redacting_deployment(tmp_path, clean_runtime, ATTR_PII)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    # note:masked by its own masked path; chunk:open:lent through its masked
    # lending record.
    secret = _grep(clean_runtime, deployment, "launch", subject=READER,
                   path_prefix="secret/al")
    control = _grep(clean_runtime, deployment, "launch", subject=READER,
                    path_prefix="secret/zz")
    _same(secret, control)
    assert secret["total_count"] == 0
    # The open row's readable path still scopes.
    assert _ids(_grep(clean_runtime, deployment, "launch", subject=READER,
                      path_prefix="open/")) == ["note:open"]


def test_attr_filters_are_no_oracle_on_a_masked_attribute(
    clean_runtime, tmp_path, monkeypatch,
):
    deployment = _redacting_deployment(tmp_path, clean_runtime, ATTR_PII)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    secret = _grep(clean_runtime, deployment, "launch", subject=READER,
                   filter={"subtype": "note", "attr_filters": {"owner": "alice"}})
    control = _grep(clean_runtime, deployment, "launch", subject=READER,
                    filter={"subtype": "note", "attr_filters": {"owner": "bob"}})
    _same(secret, control)
    assert secret["total_count"] == 0
    assert _ids(_grep(clean_runtime, deployment, "launch", subject=READER,
                      filter={"subtype": "note", "attr_filters": {"owner": "carol"}})) == [
        "note:open",
    ]


# ---------- review B3: a path past the btree row limit ----------

def test_a_path_past_the_btree_row_limit_publishes_and_scopes(clean_runtime, tmp_path):
    import random

    rng = random.Random(3067)
    alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
    long_path = "deep/" + "".join(rng.choice(alphabet) for _ in range(6400))
    facts = [
        _node("note:long", "note", "a uuid4 under a very long path", path=long_path),
        _node("note:short", "note", "a uuid4 under a short path", path="deep/x"),
    ]
    deployment = _publish(tmp_path, clean_runtime, facts)
    # A prefix longer than the 512-character index bound is rechecked in full.
    page = _grep(clean_runtime, deployment, "uuid4", path_prefix=long_path[:600])
    assert _ids(page) == ["note:long"]
    wrong = long_path[:550] + ("X" if long_path[550] != "X" else "Y")
    assert _grep(clean_runtime, deployment, "uuid4", path_prefix=wrong)["total_count"] == 0


def test_both_path_branches_can_use_the_path_index(clean_runtime, tmp_path, monkeypatch):
    # Enough records outside the prefix that a subtype scan is unselective.
    bulk: list = []
    for i in range(3000):
        bulk += [
            _node(f"doc:bulk:{i:04d}", "doc", f"bulk record {i}", path=f"bulk/{i}"),
            _node(f"chunk:bulk:{i:04d}", "chunk", f"bulk passage {i}"),
            _edge(f"doc:bulk:{i:04d}", f"chunk:bulk:{i:04d}"),
        ]
    deployment = _publish(tmp_path, clean_runtime, _corpus() + bulk)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        conn.execute(f"ANALYZE {grep_search.resolve_trgm_segment(conn)}")
        from okg.substrate.mcp.passage_rollup import resolve_edge_segment

        conn.execute(f"ANALYZE {resolve_edge_segment(conn)}")
    seen: list[tuple[str, dict]] = []
    real = grep_search._bounded

    def capture(conn, sql, params):
        seen.append((sql, dict(params)))
        return real(conn, sql, params)

    monkeypatch.setattr(grep_search, "_bounded", capture)
    _grep(clean_runtime, deployment, r"re:/\d+/", path_prefix="app/")
    sql, params = next((s, p) for s, p in seen if "UNION ALL" in s)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        conn.execute("SET enable_seqscan = off")
        plan = "\n".join(
            row[0] for row in conn.execute("EXPLAIN " + sql, params, prepare=False)
        )
    assert plan.count("node_path_prefix_visible") >= 2, plan


def test_a_repeated_storage_upgrade_keeps_the_path_index(clean_runtime, tmp_path):
    """Review of b1429a7d90: the bounded-build check misread PostgreSQL's
    stored `"left"(...)` form, so every migrate dropped and rebuilt the index."""
    from okg.substrate.graph_store import ChronosGraphStore

    _publish(tmp_path, clean_runtime, _corpus())

    def oid() -> int:
        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            rows = conn.execute(
                "SELECT c.oid FROM pg_class c JOIN pg_indexes i ON i.indexname = c.relname "
                "WHERE i.indexname ~ 'node_path_prefix_visible$'"
            ).fetchall()
        assert len(rows) == 1
        return int(rows[0][0])

    before = oid()
    store = ChronosGraphStore.connect(clean_runtime)
    try:
        store.ensure_interval_storage_upgrade()
        store.ensure_interval_storage_upgrade()
    finally:
        store.close()
    assert oid() == before


# ---------- PACT grep-near-operands-3251: NEAR operands narrow the scan ----------

_NEAR_FILLER = " ".join(f"w{i}x" for i in range(40))
_NEAR_ROWS = [
    *(f"row {i} {_NEAR_FILLER}" for i in range(2000)),
    *(f"kestrel {i} {_NEAR_FILLER}" for i in range(1500)),
    *(f"lantern {i} {_NEAR_FILLER}" for i in range(1500)),
    # Both words, farther apart than 80 characters: never a NEAR/80 match.
    *(f"kestrel {_NEAR_FILLER} lantern" for _ in range(30)),
    *(f"KESTREL near Lantern {i}" for i in range(4)),
    "lantern, then kestrel",
    "the dusk   watch\n\tlantern",
    "the dusk watchlantern",
]
_NEAR_QUERIES = [
    "kestrel NEAR/80 lantern",
    "Lantern NEAR/3 KESTREL",
    "lantern NEAR/9 kestrel",
    '"dusk watch" NEAR/5 lantern',
    "kestrel NEAR/80 kestrel",
]


def _near_plan(conn, clause: str, params: dict) -> tuple[int, int]:
    """(rows the trigram index proposed, rows that matched) for `clause`."""
    (plan,) = conn.execute(
        f"EXPLAIN (ANALYZE, FORMAT JSON) SELECT count(*) FROM near_rows WHERE {clause}",
        params, prepare=False,
    ).fetchone()
    nodes, stack = {}, [plan[0]["Plan"]]
    while stack:
        node = stack.pop()
        nodes[node["Node Type"]] = node
        stack.extend(node.get("Plans", []))
    return nodes["Bitmap Index Scan"]["Actual Rows"], nodes["Bitmap Heap Scan"]["Actual Rows"]


def test_near_operands_keep_the_match_set_and_shrink_the_trigram_candidates(clean_runtime):
    """req.exact and req.plan: the prefilter returns exactly the old NEAR matches, and the
    trigram index proposes rows holding both operands, not either one."""
    params: dict = {}
    with psycopg.connect(clean_runtime) as conn:
        conn.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")
        conn.execute("CREATE TEMP TABLE near_rows (id int PRIMARY KEY, txt text)")
        with conn.cursor() as cur:
            cur.executemany(
                "INSERT INTO near_rows VALUES (%s, %s)", list(enumerate(_NEAR_ROWS)),
            )
        conn.execute("CREATE INDEX ON near_rows USING gin (txt gin_trgm_ops)")
        conn.execute("ANALYZE near_rows")
        conn.execute("SET LOCAL enable_seqscan = off")
        for query in _NEAR_QUERIES:
            parsed = parse_grep_query(query)
            params = {}
            (new,) = grep_search._term_sql(parsed, params, column="txt")
            old = "(txt ~* %(grep_t0)s)"
            assert new != old
            matched = {}
            for label, clause in (("old", old), ("new", new)):
                matched[label] = [row[0] for row in conn.execute(
                    f"SELECT id FROM near_rows WHERE {clause} ORDER BY id",
                    params, prepare=False,
                )]
            assert matched["new"] == matched["old"], query
        parsed = parse_grep_query("kestrel NEAR/80 lantern")
        params = {}
        (new,) = grep_search._term_sql(parsed, params, column="txt")
        old_candidates, old_rows = _near_plan(conn, "txt ~* %(grep_t0)s", params)
        new_candidates, new_rows = _near_plan(conn, new, params)
        # 4 upper-case rows and "lantern, then kestrel" are the only matches.
        assert old_rows == new_rows == 5
        # Without the operands pg_trgm proposes every row with EITHER word...
        assert old_candidates >= 3000
        # ...with them, only the 35 rows holding BOTH.
        assert new_candidates == 35
