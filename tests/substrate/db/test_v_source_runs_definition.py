"""Text-level pin for `okg.v_source_runs` (PACT
fix-v-source-runs-null-placeholder).

The view once ended with a `UNION ALL` fallback arm that projected
`okg.source_watermarks` rows shaped like runs — `NULL::text AS run_id`
and NULL counts — whenever a source had no per-run progress evidence.
A bare SourceRunner (no deployment name) can never write the
`sync.run.<run_id>` payload (`okg.source_progress`'s primary key
requires a deployment), so freshly ingested sources surfaced as rows of
NULLs instead of "no runs recorded". `scripts/check_view_reads.py`
deliberately does not flag this shape because the same columns are
non-NULL in the evidenced arms, so this test is the gate.

Deliberately runs without postgres, so it stays in per-PR CI. The
behavioral pin lives in
tests/substrate/mcp/test_v_source_runs_readability.py::
test_v_source_runs_returns_nothing_for_watermark_only_source.

This test originally pinned TWO copies of the view text: `views.sql`,
and the chronos-native `catalog_replacement` variant in
`management.py` that `okg migrate` used to install over it. The fix
this test guards had to be written into both, which is the hazard
`schema-canonical-read-path` removed: `management.py` no longer
rewrites the view layer, `okg migrate` executes `views.sql` as
written, and there is exactly one definition to regress. Rather than
drop to a hardcoded single path, the scan below still walks the db
package and pins every copy it finds — so if a second one is ever
reintroduced, this test covers it without being edited.
"""

from __future__ import annotations

from pathlib import Path

import okg.substrate.db.management as management

_DECLARATION = "CREATE OR REPLACE VIEW okg.v_source_runs AS"


def test_v_source_runs_definitions_do_not_project_watermarks() -> None:
    db_dir = Path(management.__file__).parent
    checked: list[str] = []
    for path in sorted(db_dir.glob("*.sql")) + sorted(db_dir.glob("*.py")):
        text = path.read_text()
        start = text.find(_DECLARATION)
        if start < 0:
            continue
        terminator = text.index(
            ";", text.index("FULL OUTER JOIN fact_agg f", start),
        )
        block = text[start:terminator]
        checked.append(path.name)
        assert "source_watermarks" not in block, (
            f"{path.name}: okg.v_source_runs regained a watermark "
            "placeholder arm; a source with no per-run evidence must "
            "project zero rows, not a row of NULLs"
        )

    # Absence is not agreement: a scan that found no definition would pass
    # while pinning nothing at all.
    assert "views.sql" in checked, (
        "okg.v_source_runs was not found in views.sql, which is the "
        f"canonical definition `okg migrate` installs; scanned: {checked}"
    )
