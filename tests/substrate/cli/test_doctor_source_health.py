"""source-health-matrix: the health view's contract — clean on the
real library, loud on import failures and registration drift, live
gates reflect env — and the runbook stays consistent with it."""
from __future__ import annotations


from okg.substrate.cli.source_health import (
    assemble_source_health,
    parse_runbook_matrix,
    runbook_env_vars,
    summarize,
    _repo_root,
)


def test_current_library_reports_clean() -> None:
    """The contract that would have caught the outlook
    _CREDENTIAL_SOURCE_IDS gap: every source-defaults entry imports,
    is wizard-cataloged, and connector sources are in the
    credentialed set."""
    rows = assemble_source_health()
    failing = {r.source_id: r.failures for r in rows if r.failures}
    assert failing == {}, failing
    summary = summarize(rows)
    assert summary["ok"] is True
    assert summary["sources"] >= 29
    assert summary["import_failures"] == 0
    assert summary["registration_drift"] == 0
    # The connector family all carries live gates + live tests.
    by_id = {r.source_id: r for r in rows}
    for sid in ("gmail_api", "outlook_mail_api", "canvas_api"):
        assert by_id[sid].live_gate_env, sid
        assert by_id[sid].live_test_file, sid


def test_unresolvable_class_fails_with_import_error() -> None:
    rows = assemble_source_health(source_defaults={
        "ghost_source": {
            "module": "okg.substrate.library.sources.no_such_module",
            "class": "GhostSource",
        },
    })
    assert len(rows) == 1
    row = rows[0]
    assert row.import_ok is False
    assert any(f.startswith("import:") for f in row.failures)
    assert "no_such_module" in (row.import_error or "")
    assert summarize(rows)["ok"] is False


def test_connector_missing_from_credentialed_set_fails() -> None:
    """Re-create the outlook gap synthetically: a connector-kind
    source absent from the credentialed set fails with the facet
    named."""
    rows = assemble_source_health(
        source_defaults={
            "gmail_api": {
                "module": "okg.substrate.library.sources.gmail_api",
                "class": "GmailApiSource",
            },
        },
        credentialed_ids=frozenset(),
    )
    row = rows[0]
    assert row.credentialed_connector is True
    assert row.in_credentialed_set is False
    assert any("_CREDENTIAL_SOURCE_IDS" in f for f in row.failures)


def test_live_tier_gates_reflect_env() -> None:
    defaults = {
        "canvas_api": {
            "module": "okg.substrate.library.sources.canvas_api",
            "class": "CanvasApiSource",
        },
    }
    blocked = assemble_source_health(source_defaults=defaults, env={})
    assert blocked[0].live_ready is False
    assert "OKG_CANVAS_TOKEN" in blocked[0].live_gate_env

    ready = assemble_source_health(
        source_defaults=defaults,
        env={"OKG_LIVE_CANVAS": "1", "OKG_CANVAS_TOKEN": "tok"},
    )
    assert ready[0].live_ready is True


def test_matrix_runbook_commands_are_real() -> None:
    """Runbook <-> health view consistency: every test path the
    matrix names exists on disk, and every env var in a live-gate
    cell is one the health view reports for that source."""
    root = _repo_root()
    text = (root / "docs" / "runbooks" / "quality" / "source-test-matrix.md").read_text(
        encoding="utf-8",
    )
    matrix = parse_runbook_matrix(text)
    assert len(matrix) >= 29, "runbook matrix lost rows"

    rows = {r.source_id: r for r in assemble_source_health()}
    assert set(m["source"] for m in matrix) == set(rows), (
        "runbook rows out of sync with source-defaults"
    )
    for entry in matrix:
        row = rows[entry["source"]]
        for cell in (entry["test"], entry["live_test"]):
            for token in cell.split():
                if token.startswith("tests/") and token.endswith(".py"):
                    assert (root / token).is_file(), (
                        f"{entry['source']}: {token} does not exist"
                    )
        gate_vars = runbook_env_vars(entry["live_gate"])
        view_vars = set(row.live_gate_env)
        # The runbook may add live-test convenience vars (labels,
        # base URLs) but must include every gate the view reports.
        assert view_vars <= gate_vars, (
            f"{entry['source']}: runbook missing {view_vars - gate_vars}"
        )
