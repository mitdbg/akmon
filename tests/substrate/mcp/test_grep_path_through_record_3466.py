"""Grep's path `where` reaches a passage through its record outside the profile.

Issue #3466, grep follow-up (PACT grep-path-through-record-3466). PACT
path-through-record-3466 fixed lexical search; on the C4 comp-ops copy grep
with the same `where` (path prefix `apps/production/`, default content
profile without `document`) still returned 0 rows while `path_prefix` with
the same prefix returned 10 chunks: grep's fold dropped every chunk whose
document the profile leaves out. Each requirement with its failure case, on
the corpus of `test_search_path_through_record_3466`:

* under a profile without the record type, a chunk whose record passes the
  path `where` stays its own row and is counted, and one whose record fails
  it does not;
* a record the profile excludes by path, a record the reader may not see,
  and a readable record whose path is masked still drop their passages;
* without a profile the record itself answers, as before.
"""
from __future__ import annotations

import psycopg
import pytest

from okg.substrate.mcp import open_session, search
from okg.substrate.mcp.search_profiles import ResolvedScope
from tests.substrate.mcp.test_search_path_through_record_3466 import (  # noqa: F401
    CONTENT,
    PREFIX,
    QUERY,
    READER,
    VISIBLE,
    corpus,
)
from tests.substrate.mcp.test_search_records import _ids

pytestmark = pytest.mark.postgres


def _grep(corpus, *, scope=CONTENT, where=PREFIX, **kwargs) -> dict:  # noqa: F811
    dsn, deployment = corpus
    with psycopg.connect(dsn, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment, nomos_subject=READER)
        return search(conn, session, QUERY, method="grep", limit=50,
                      scope=scope, where=where, **kwargs)


def test_a_chunk_matches_through_its_record_outside_the_profile(corpus, monkeypatch):  # noqa: F811
    """Failure case before the fix: only `note:in` came back, counted 1."""
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "off")
    page = _grep(corpus)
    assert sorted(_ids(page)) == ["chunk:hid:0", "chunk:in:0", "chunk:mask:0", "note:in"]
    assert page["total_count"] == 4
    assert page["total_count_basis"] == "exact"
    # A prefix no record carries admits nothing.
    empty = _grep(corpus, where={"op": "prefix", "field": "path", "value": "nothere/"})
    assert _ids(empty) == [] and empty["total_count"] == 0
    # Control: without the `where` every matching chunk answers.
    assert {"chunk:in:0", "chunk:out:0"} <= set(_ids(_grep(corpus, where=None)))


def test_an_exact_where_counts_what_the_page_lists(corpus, monkeypatch):  # noqa: F811
    """A `where` SQL decides whole (`source`) is counted by the SQL count,
    which judges a parent with the profile's subtypes: the kept chunks must
    be counted too (review finding)."""
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "off")
    page = _grep(corpus, where={"op": "eq", "field": "source", "value": VISIBLE})
    assert sorted(_ids(page)) == ["chunk:in:0", "chunk:out:0", "note:in"]
    assert page["total_count"] == 3


def test_a_chunk_grep_judges_the_chunk_alone(corpus, monkeypatch):  # noqa: F811
    """A passage-subtype grep is unchanged: no chunk declares a path, so the
    path `where` is refused by name rather than read through the document."""
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "off")
    with pytest.raises(Exception, match="no scoped subtype declares 'path'"):
        _grep(corpus, scope=None, filter={"subtype": "chunk"})


def test_a_record_excluded_by_path_still_drops_its_passages(corpus, monkeypatch):  # noqa: F811
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "off")
    scope = ResolvedScope(profile="content", subtypes=("chunk", "note"),
                          excluded_path_globs=("apps/prod/a.yaml",))
    assert "chunk:in:0" not in _ids(_grep(corpus, scope=scope))


def test_a_hidden_record_does_not_lend_its_path(corpus, monkeypatch):  # noqa: F811
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    page = _grep(corpus)
    assert "chunk:hid:0" not in _ids(page)
    assert "chunk:in:0" in _ids(page)


def test_a_masked_record_path_does_not_admit_its_passage(corpus, monkeypatch):  # noqa: F811
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    ids = _ids(_grep(corpus))
    assert "chunk:mask:0" not in ids
    assert "chunk:in:0" in ids
    assert "chunk:mask:0" in _ids(_grep(corpus, where=None))


def test_without_a_profile_the_record_answers(corpus, monkeypatch):  # noqa: F811
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "off")
    ids = set(_ids(_grep(corpus, scope=None)))
    assert {"doc:in", "doc:hid", "doc:mask", "note:in"} == ids


def test_under_audit_a_chunk_kept_through_its_record_is_counted_as_listed(
    corpus, monkeypatch,  # noqa: F811
):
    """#3466 and #3384 together: under `audit` a chunk kept through its
    record outside the profile is listed and counted by the finished count,
    which keeps #3466's finish bound (`COUNT_FINISH_ROWS`), not the audit
    SQL count's 10,000 rows, also when the `where` compiles exactly."""
    from okg.substrate.mcp import grep_search

    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "audit")
    seen: list[dict] = []
    real = grep_search.count_grep_matches

    def spy(*args, **kwargs):
        seen.append(kwargs)
        return real(*args, **kwargs)

    monkeypatch.setattr(grep_search, "count_grep_matches", spy)
    page = _grep(corpus)
    ids = _ids(page)
    assert {"chunk:in:0", "note:in"} <= set(ids)
    assert "chunk:out:0" not in ids
    assert page["total_count"] == len(ids)
    # The redact rule makes every count a lower bound ("redaction"); the
    # count itself reached no cap.
    assert page["count_basis_reasons"] == ["redaction"]
    assert seen[-1]["finish_sql"] is not None
    assert seen[-1]["finish_rows"] == grep_search.COUNT_FINISH_ROWS
    # Failure case for the merge: an exact `where` alone would give the audit
    # count the 10,000-row bound; a kept passage keeps the 2,000-row one.
    exact = _grep(corpus, where={"op": "eq", "field": "source", "value": VISIBLE})
    exact_ids = _ids(exact)
    assert {"chunk:in:0", "chunk:out:0", "note:in"} <= set(exact_ids)
    assert exact["total_count"] == len(exact_ids)
    assert seen[-1]["finish_sql"] is not None
    assert seen[-1]["finish_rows"] == grep_search.COUNT_FINISH_ROWS
