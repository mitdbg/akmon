"""A registry-active source refused at admission must not read as silence.

Issue #1323: `okg-workspace.latex_project` was registry-active, ran three
times, and was rejected at admission every time. A rejected admission writes
only an `okg.admission_envelopes` row -- no source-run row, no watermark, no
`node_facts`/`edge_facts` rows -- so every per-source health surface reported
it as an ordinary enabled source with zero counts and no error.

These tests are DB-free: they drive the real `list_sources` / `source_status`
code paths over a psycopg-shaped stub, the same seam
`tests/substrate/mcp/test_source_admin.py` uses.
"""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

import psycopg
import pytest
import yaml


_REJECTED_AT = datetime(2026, 8, 24, 5, 5, tzinfo=timezone.utc)

#: The verdict okg-workspace recorded for all three latex_project runs.
_LATEX_VERDICT = {
    "code": "producer_edge_triple_not_allowed",
    "message": "edge record does not match producer signature",
    "producer_policy": {
        "offending_records": [
            {
                "src_subtype": "citation",
                "edge_type": "references",
                "dst_subtype": None,
                "dst": "chunk:fa66c14ace9ad1e7",
            },
        ],
    },
}


class _StubCursor:
    """Cursor stub. `responses` maps a query substring to a row (or to an
    exception instance, which is raised instead)."""

    def __init__(self, responses: dict[str, Any], calls: list) -> None:
        self._responses = responses
        self._calls = calls
        self._row: Any = None

    def __enter__(self) -> "_StubCursor":
        return self

    def __exit__(self, *exc: Any) -> None:
        return None

    def execute(self, query: str, params: Any = None) -> None:
        self._calls.append((query, params))
        self._row = None
        for key, value in self._responses.items():
            if key in query:
                if isinstance(value, Exception):
                    raise value
                self._row = value
                break

    def fetchone(self) -> Any:
        return self._row


class _StubConn:
    def __init__(self, responses: dict[str, Any] | None = None) -> None:
        self.responses = responses or {}
        self.calls: list = []

    def cursor(self) -> _StubCursor:
        return _StubCursor(self.responses, self.calls)


def _admission_row(
    status: str,
    *,
    envelope_count: int = 3,
    adverse_count: int = 3,
    verdict: Any = None,
    producer_id: str = "demo.latex_project",
) -> tuple:
    """One `okg.admission_envelopes` row in the column order the query selects:
    status, created_at, verdict, producer_id, envelope_count, adverse_count."""
    return (
        status,
        _REJECTED_AT,
        _LATEX_VERDICT if verdict is None else verdict,
        producer_id,
        envelope_count,
        adverse_count,
    )


@pytest.fixture
def repo(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Path]:
    (tmp_path / "pyproject.toml").write_text("[project]\nname = 'x'\n")
    dep = tmp_path / "deployments" / "demo"
    dep.mkdir(parents=True)
    (dep / "deployment.yaml").write_text("profile:\n  name: test\n")
    (dep / "source_registry.yaml").write_text(
        yaml.safe_dump(
            {
                "sources": {
                    # No `enabled_by_default`, exactly as `latex-project` is
                    # written in deployments/okg-workspace/source_registry.yaml
                    # -- 29 of its 30 sources omit the flag. A fixture that
                    # wrote `true` here would let a label rule that keys on the
                    # flag pass while going vacuous in production.
                    "latex-project": {
                        "ownership_id": "demo.latex_project",
                    },
                },
            },
            sort_keys=False,
        )
    )
    monkeypatch.setattr(
        "okg.substrate.mcp.source_admin._repo_root_from_cwd",
        lambda: tmp_path,
    )
    monkeypatch.setenv("OKG_USER_HOME", str(tmp_path / "user-state"))
    yield tmp_path


def _only_source(result: dict) -> dict:
    assert result["status"] == "ok", result
    assert len(result["sources"]) == 1, result["sources"]
    return result["sources"][0]


# ---------- list_sources ----------


def test_rejected_admission_is_reported_as_a_failure_not_as_active(
    repo: Path,
) -> None:
    """The #1323 case: three rejected admissions, nothing else written."""
    from okg.substrate.mcp.source_admin import list_sources

    conn = _StubConn({"okg.admission_envelopes": _admission_row("rejected")})
    row = _only_source(list_sources(conn, deployment="demo"))

    assert row["status"] == "admission_rejected"
    assert row["last_admission"]["status"] == "rejected"
    assert (
        row["last_admission"]["reason_code"]
        == "producer_edge_triple_not_allowed"
    )
    assert row["last_admission"]["adverse_count"] == 3
    assert row["last_admission"]["created_at"] == _REJECTED_AT.isoformat()

    error = row["recent_error"]
    assert error is not None
    assert error["surface"] == "admission"
    assert error["reason_code"] == "producer_edge_triple_not_allowed"
    assert "v_source_run_diagnostics" in error["agent_hint"]
    # The source DID run; a surface that says otherwise repeats the false
    # negative that hid this for the source's whole enrolment.
    assert row["runtime_observed"] is True


def test_failed_admission_is_a_refusal_like_rejected(repo: Path) -> None:
    from okg.substrate.mcp.source_admin import list_sources

    conn = _StubConn({"okg.admission_envelopes": _admission_row("failed")})
    row = _only_source(list_sources(conn, deployment="demo"))

    assert row["status"] == "admission_rejected"
    assert row["recent_error"]["status"] == "failed"
    assert row["recent_error"]["refused"] is True


@pytest.mark.parametrize(
    ("status", "label"),
    [("blocked", "publish_blocked"), ("quarantined", "quarantined")],
)
def test_blocked_and_quarantined_are_not_reported_as_refusals(
    repo: Path, status: str, label: str,
) -> None:
    """`admission._mark_generation_blocked` stamps `blocked` on EVERY
    pending/accepted envelope in a deployment when a publish generation
    blocks. Those payloads were admitted and their facts exist. Calling them
    refused would flip every actively-ingesting source in the deployment to a
    refusal it never had, and tell the operator it wrote no rows -- the same
    misdirection this change removes from the edge message. `quarantined`
    admissions likewise carry isolated or compensated facts."""
    from okg.substrate.mcp.source_admin import list_sources

    conn = _StubConn({"okg.admission_envelopes": _admission_row(status)})
    row = _only_source(list_sources(conn, deployment="demo"))

    assert row["status"] == label
    error = row["recent_error"]
    assert error is not None and error["status"] == status
    assert error["refused"] is False
    assert "refused at admission" not in error["agent_hint"]
    assert "wrote no source-run row" not in error["agent_hint"]


def test_blocked_verdict_without_a_code_still_names_a_reason(
    repo: Path,
) -> None:
    """A blocked generation stamps `blocked_reason`, not a refusal `code`;
    reporting `reason: None` would say "adverse, reason unknown"."""
    from okg.substrate.mcp.source_admin import list_sources

    conn = _StubConn(
        {
            "okg.admission_envelopes": _admission_row(
                "blocked",
                verdict={
                    "generation_id": 7,
                    "generation_status": "blocked",
                    "blocked_reason": "invariant_violation",
                },
            ),
        },
    )
    row = _only_source(list_sources(conn, deployment="demo"))
    assert row["recent_error"]["reason_code"] == "invariant_violation"


@pytest.mark.parametrize("status", ["pending", "accepted"])
def test_an_undecided_envelope_alone_is_not_ordinary_health(
    repo: Path, status: str,
) -> None:
    """Counting an envelope that was never decided or never published as
    runtime health would make a stalled source look better than the silence
    this change replaces."""
    from okg.substrate.mcp.source_admin import list_sources

    conn = _StubConn(
        {
            "okg.admission_envelopes": _admission_row(
                status, envelope_count=1, adverse_count=0, verdict={},
            ),
        },
    )
    row = _only_source(list_sources(conn, deployment="demo"))

    assert row["status"] == "admission_pending"
    assert row["recent_error"] is None


def test_a_pending_envelope_alongside_real_progress_stays_active(
    repo: Path,
) -> None:
    """Non-vacuity: a source mid-run must not be labelled stalled."""
    from okg.substrate.mcp.source_admin import list_sources

    conn = _StubConn(
        {
            "okg.admission_envelopes": _admission_row(
                "pending", envelope_count=1, adverse_count=0, verdict={},
            ),
            "okg.source_watermarks": (_REJECTED_AT, "rev-3"),
        },
    )
    row = _only_source(list_sources(conn, deployment="demo"))
    # `observed`, not an admission label: this fixture omits
    # `enabled_by_default`, so `_preflight_status` says `disabled` and an
    # observed source has read `observed` since long before this change. What
    # matters is that no admission label fired.
    assert row["status"] == "observed"


def test_a_producing_source_with_one_refusal_is_not_called_silent(
    repo: Path,
) -> None:
    """A source ingesting for months that had one transient refusal last night
    must not be told it "wrote no source-run row and contributed no rows" --
    its own row says runtime_observed, so that hint would contradict it."""
    from okg.substrate.mcp.source_admin import list_sources

    conn = _StubConn(
        {
            "okg.admission_envelopes": _admission_row("rejected"),
            "okg.source_watermarks": (_REJECTED_AT, "rev-42"),
        },
    )
    row = _only_source(list_sources(conn, deployment="demo"))

    assert row["status"] == "admission_rejected_recent"
    error = row["recent_error"]
    assert error["refused"] is True
    assert error["produced_before"] is True
    # "that attempt wrote no source-run row" is true and useful; "contributed
    # no rows", said of a source with a watermark, is not.
    assert "contributed no rows" not in error["agent_hint"]
    assert "has produced before" in error["agent_hint"]


def test_a_setup_state_with_an_action_survives_an_admission_outcome(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`needs_connection` names the concrete operator action. A refusal must
    not replace it, or a gmail source that is both unconnected and once-refused
    loses the only signal that says what to do. One blocked publish stamps
    `blocked` across the whole deployment, so the same rule keeps it from
    relabelling every source in the census."""
    from okg.substrate.mcp.source_admin import list_sources

    (tmp_path / "pyproject.toml").write_text("[project]\nname = 'x'\n")
    dep = tmp_path / "deployments" / "demo"
    dep.mkdir(parents=True)
    (dep / "deployment.yaml").write_text("profile:\n  name: test\n")
    (dep / "source_registry.yaml").write_text(
        yaml.safe_dump(
            {
                "sources": {
                    "gmail_api": {
                        "ownership_id": "demo.gmail_api",
                        "enabled_by_default": True,
                    },
                },
            },
            sort_keys=False,
        )
    )
    monkeypatch.setattr(
        "okg.substrate.mcp.source_admin._repo_root_from_cwd",
        lambda: tmp_path,
    )
    monkeypatch.setenv("OKG_USER_HOME", str(tmp_path / "user-state"))

    conn = _StubConn({"okg.admission_envelopes": _admission_row("rejected")})
    row = _only_source(list_sources(conn, deployment="demo"))

    assert row["status"] == "needs_connection"
    # The refusal is still reported, beside the setup state rather than
    # instead of it.
    assert row["recent_error"]["refused"] is True


def test_active_source_with_no_observation_at_all_is_flagged(
    repo: Path,
) -> None:
    """The original #1323 premise (never scheduled) must not be silent either."""
    from okg.substrate.mcp.source_admin import list_sources

    row = _only_source(list_sources(_StubConn(), deployment="demo"))

    assert row["status"] == "never_observed"
    assert row["runtime_observed"] is False
    assert row["last_admission"]["authority"] == "admission_envelopes"
    assert row["last_admission"]["envelope_count"] == 0
    # Nothing was refused, so there is no admission error to report.
    assert row["recent_error"] is None


def test_accepted_admission_leaves_the_source_active(repo: Path) -> None:
    """The signal must not fire on a source that is actually working."""
    from okg.substrate.mcp.source_admin import list_sources

    conn = _StubConn(
        {
            "okg.admission_envelopes": _admission_row(
                "published", envelope_count=9, adverse_count=0, verdict={},
            ),
        },
    )
    row = _only_source(list_sources(conn, deployment="demo"))

    # `observed` rather than an admission label: this fixture omits
    # `enabled_by_default`, the way the real registry does, so
    # `_preflight_status` reports `disabled` and an observed source has read
    # `observed` since long before this change.
    assert row["status"] == "observed"
    assert row["recent_error"] is None
    assert row["runtime_observed"] is True


def test_unreadable_admission_surface_is_not_reported_as_healthy(
    repo: Path,
) -> None:
    """Absence of a readable rejection is not evidence there was none."""
    from okg.substrate.mcp.source_admin import list_sources

    conn = _StubConn(
        {"okg.admission_envelopes": psycopg.errors.UndefinedTable("nope")},
    )
    row = _only_source(list_sources(conn, deployment="demo"))

    assert row["status"] == "admission_unknown"
    assert row["last_admission"]["authority"] == "unavailable"
    assert row["last_admission"]["envelope_count"] is None


def test_list_sources_health_authority_names_the_admission_join(
    repo: Path,
) -> None:
    from okg.substrate.mcp.source_admin import list_sources

    result = list_sources(_StubConn(), deployment="demo")
    assert "admission_envelopes" in result["health_authority"]
    assert "v_source_run_diagnostics" in result["health_authority"]


def test_admission_query_is_scoped_to_this_deployment_and_producer(
    repo: Path,
) -> None:
    """Guards against a census that reads another deployment's envelopes."""
    from okg.substrate.mcp.source_admin import list_sources

    conn = _StubConn({"okg.admission_envelopes": _admission_row("rejected")})
    list_sources(conn, deployment="demo")

    admission_calls = [
        (query, params)
        for query, params in conn.calls
        if "okg.admission_envelopes" in query
    ]
    assert len(admission_calls) == 1
    _query, params = admission_calls[0]
    assert params[1] == "demo"
    assert params[2] == "demo.latex_project"
    assert params[3] == "demo.latex_project"


def test_envelopes_are_found_when_producer_id_differs_from_ownership_id(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A registry entry may declare its own `admission_policy.producer_id`;
    nine sources in one production registry do. Keying the envelope read on `ownership_id`
    alone returns zero rows for all of them and reports them as never
    observed -- the exact false negative this change exists to close."""
    from okg.substrate.mcp.source_admin import list_sources

    (tmp_path / "pyproject.toml").write_text("[project]\nname = 'x'\n")
    dep = tmp_path / "deployments" / "demo"
    dep.mkdir(parents=True)
    (dep / "deployment.yaml").write_text("profile:\n  name: test\n")
    (dep / "source_registry.yaml").write_text(
        yaml.safe_dump(
            {
                "sources": {
                    "github": {
                        "ownership_id": "demo.github",
                        "admission_policy": {
                            "producer_id": "demo.github.canonical-snapshot",
                        },
                    },
                },
            },
            sort_keys=False,
        )
    )
    monkeypatch.setattr(
        "okg.substrate.mcp.source_admin._repo_root_from_cwd",
        lambda: tmp_path,
    )
    monkeypatch.setenv("OKG_USER_HOME", str(tmp_path / "user-state"))

    conn = _StubConn(
        {
            "okg.admission_envelopes": _admission_row(
                "rejected", producer_id="demo.github.canonical-snapshot",
            ),
        },
    )
    row = _only_source(list_sources(conn, deployment="demo"))

    # The producer id reported is the one the ROW carries, never the one the
    # query asked for, so a verdict cannot be attributed to a producer that
    # did not receive it.
    assert row["last_admission"]["producer_id"] == (
        "demo.github.canonical-snapshot"
    )
    assert row["status"] == "admission_rejected"
    params = next(
        params
        for query, params in conn.calls
        if "okg.admission_envelopes" in query
    )
    # Matched on the declared producer id OR the ownership id, because ingest
    # stamps `producer_id` from the policy and `source` from the ownership id.
    # The `source` arm is scoped to the source's own producer kind.
    assert params[2] == "demo.github.canonical-snapshot"
    assert params[3] == "demo.github"
    assert params[4] == "source"


def test_a_system_producers_envelope_is_not_read_as_the_sources_refusal(
    repo: Path,
) -> None:
    """`entity_merge`, `source_retirement` and `source_lineage_repair` stamp a
    registry source's ownership id in `source` while owning
    `producer_kind: "system"`. Without the kind guard on the `source` arm, a
    failed merge batch becomes the source's newest refusal and the operator is
    told the source wrote no rows -- the misdirection this change removes.

    This pins the QUERY SHAPE only; the stub cursor does not evaluate
    predicates. The behavior itself is proved against a real database by
    `tests/substrate/test_doctor_sources.py::
    test_a_system_producers_envelope_is_not_attributed_to_the_source`."""
    from okg.substrate.mcp.source_admin import list_sources

    conn = _StubConn({"okg.admission_envelopes": _admission_row("rejected")})
    list_sources(conn, deployment="demo")

    query, params = next(
        (query, params)
        for query, params in conn.calls
        if "okg.admission_envelopes" in query
    )
    assert "producer_kind = %s" in query
    assert params[4] == "source"


class _PoisoningCursor(_StubCursor):
    """Cursor that models Postgres transaction abort semantics.

    After a failed statement, psycopg raises `InFailedSqlTransaction` on every
    later statement until the transaction (or savepoint) is rolled back. A
    read that swallows `UndefinedTable` without a savepoint therefore blanks
    every later read in the same census, not just its own.
    """

    def __init__(self, conn: "_PoisoningConn") -> None:
        super().__init__(conn.responses, conn.calls)
        self._conn = conn

    def execute(self, query: str, params: Any = None) -> None:
        if self._conn.aborted:
            raise psycopg.errors.InFailedSqlTransaction(
                "current transaction is aborted"
            )
        try:
            super().execute(query, params)
        except Exception:
            if self._conn.savepoint_depth == 0:
                self._conn.aborted = True
            raise


class _PoisoningConn(_StubConn):
    def __init__(self, responses: dict[str, Any]) -> None:
        super().__init__(responses)
        self.aborted = False
        self.savepoint_depth = 0

    def cursor(self) -> _PoisoningCursor:
        return _PoisoningCursor(self)

    def transaction(self):
        import contextlib

        @contextlib.contextmanager
        def _savepoint():
            self.savepoint_depth += 1
            try:
                yield
            finally:
                self.savepoint_depth -= 1

        return _savepoint()


def test_unreadable_admission_surface_does_not_blank_the_rest_of_the_census(
    repo: Path, tmp_path: Path,
) -> None:
    """An unreadable envelope table must abort only its own savepoint."""
    from okg.substrate.mcp.source_admin import list_sources

    registry = tmp_path / "deployments" / "demo" / "source_registry.yaml"
    registry.write_text(
        yaml.safe_dump(
            {
                "sources": {
                    "a-latex-project": {
                        "ownership_id": "demo.latex_project",
                        "enabled_by_default": True,
                    },
                    "b-healthy": {
                        "ownership_id": "demo.healthy",
                        "enabled_by_default": True,
                    },
                },
            },
            sort_keys=False,
        )
    )
    conn = _PoisoningConn(
        {
            "okg.admission_envelopes": psycopg.errors.UndefinedTable("nope"),
            "okg.source_watermarks": (_REJECTED_AT, "rev-9"),
        },
    )
    result = list_sources(conn, deployment="demo")

    assert result["status"] == "ok", result
    rows = {row["name"]: row for row in result["sources"]}
    assert set(rows) == {"a-latex-project", "b-healthy"}
    # The source enumerated AFTER the failed admission read still resolves its
    # watermark. Before the savepoint guard this raised InFailedSqlTransaction.
    assert rows["b-healthy"]["last_ingest_at"] == _REJECTED_AT.isoformat()
    assert rows["b-healthy"]["status"] == "admission_unknown"
    assert conn.aborted is False


# ---------- source_status ----------


def test_source_status_reports_the_rejection_for_one_source(
    repo: Path,
) -> None:
    from okg.substrate.mcp.source_admin import source_status

    conn = _StubConn({"okg.admission_envelopes": _admission_row("rejected")})
    result = source_status(conn, deployment="demo", short_name="latex-project")

    assert result["status"] == "ok", result
    assert result["health_status"] == "admission_rejected"
    assert (
        result["last_admission"]["reason_code"]
        == "producer_edge_triple_not_allowed"
    )
    assert result["recent_error"]["surface"] == "admission"


# ---------- reason extraction ----------


def test_reason_code_is_read_from_a_violations_list_too() -> None:
    from okg.substrate.mcp.source_admin import _admission_reason

    code, message = _admission_reason(
        {"violations": [{"code": "producer_edge_triple_not_allowed"}]},
    )
    assert code == "producer_edge_triple_not_allowed"
    assert message is None


def test_reason_extraction_survives_a_json_encoded_verdict() -> None:
    import json

    from okg.substrate.mcp.source_admin import _admission_reason

    code, message = _admission_reason(json.dumps(_LATEX_VERDICT))
    assert code == "producer_edge_triple_not_allowed"
    assert message == "edge record does not match producer signature"


def test_reason_extraction_returns_nothing_for_an_unparseable_verdict() -> None:
    from okg.substrate.mcp.source_admin import _admission_reason

    assert _admission_reason("not json") == (None, None)
    assert _admission_reason(None) == (None, None)


def test_an_explicitly_dormant_source_keeps_its_setup_label(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Only an EXPLICIT `enabled_by_default: false` suppresses the admission
    label. The registry deliberately turned this source off, so `disabled` is
    the true state; the refusal is still carried in `recent_error`."""
    from okg.substrate.mcp.source_admin import list_sources

    (tmp_path / "pyproject.toml").write_text("[project]\nname = 'x'\n")
    dep = tmp_path / "deployments" / "demo"
    dep.mkdir(parents=True)
    (dep / "deployment.yaml").write_text("profile:\n  name: test\n")
    (dep / "source_registry.yaml").write_text(
        yaml.safe_dump(
            {
                "sources": {
                    "code-lsp-resolution": {
                        "ownership_id": "demo.code_lsp",
                        "enabled_by_default": False,
                    },
                },
            },
            sort_keys=False,
        )
    )
    monkeypatch.setattr(
        "okg.substrate.mcp.source_admin._repo_root_from_cwd",
        lambda: tmp_path,
    )
    monkeypatch.setenv("OKG_USER_HOME", str(tmp_path / "user-state"))

    conn = _StubConn(
        {
            "okg.admission_envelopes": _admission_row(
                "rejected", producer_id="demo.code_lsp",
            ),
        },
    )
    row = _only_source(list_sources(conn, deployment="demo"))

    assert row["status"] == "observed"
    assert row["recent_error"]["refused"] is True


# ---------- reason codes this surface must not hard-code ----------

#: The refusal code okg-workspace recorded in 2026-08, and the code PR #1526
#: (issue #1330) introduces for the missing-endpoint case specifically. These
#: surfaces report whatever code the verdict carries, so both must work and
#: neither may be special-cased. Pinned as a pair so that whichever order the
#: two changes merge in, this suite still proves the signal survives -- and so
#: a future surface that starts branching on one code reds here rather than
#: going quietly blind to refusals under the other.
_REFUSAL_CODES = (
    "producer_edge_triple_not_allowed",
    "producer_edge_endpoint_missing",
)


@pytest.mark.parametrize("code", _REFUSAL_CODES)
def test_the_refusal_signal_does_not_depend_on_the_reason_code(
    repo: Path, code: str,
) -> None:
    from okg.substrate.mcp.source_admin import list_sources

    conn = _StubConn(
        {
            "okg.admission_envelopes": _admission_row(
                "rejected",
                verdict={
                    "code": code,
                    "message": "refused",
                    "producer_policy": {"offending_records": []},
                },
            ),
        },
    )
    row = _only_source(list_sources(conn, deployment="demo"))

    assert row["status"] == "admission_rejected"
    assert row["recent_error"]["reason_code"] == code
    assert row["recent_error"]["refused"] is True


def test_the_reason_code_is_reported_verbatim_not_from_a_known_set() -> None:
    """A code this surface has never heard of must still be reported. A
    surface that recognised only today's codes would go silent exactly when
    admission grows a new refusal reason."""
    from okg.substrate.mcp.source_admin import _admission_reason

    code, _message = _admission_reason({"code": "some_future_refusal_code"})
    assert code == "some_future_refusal_code"
