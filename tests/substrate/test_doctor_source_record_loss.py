"""Plain `okg doctor` warns when a source loses its records (issue #3783).

Bug #3776 moved almost every file record of two git-files sources to the
git-history source: each fell to about one record, the graph total held, and
doctor read ok. These tests publish real generations and run the probe on
them.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import psycopg
import pytest

from okg.substrate.cli import doctor as doctor_cli
from okg.substrate.console_views.console_count_rollup_read import COUNT_ROLLUP_FLAG
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import NodeFact
from okg.substrate.publisher import Publisher
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer, seed_catalog
from tests.substrate.mcp.search_fixture import make_search_deployment

pytestmark = pytest.mark.postgres

FILES = "dep.git-files"
HISTORY = "dep.git-history"
NOTES = "dep.notes"


def _node(node_id: str, op: str = "I") -> NodeFact:
    return NodeFact(
        node_id=node_id,
        subtype="thing",
        attrs={} if op == "R" else {"text": node_id},
        source_record_id={"id": node_id},
        source_revision={"v": 1},
        op=op,
    )


def _ids(prefix: str, count: int, start: int = 0) -> list[str]:
    return [f"{prefix}:{i}" for i in range(start, start + count)]


def _add(source: str, prefix: str, count: int) -> tuple[str, list[NodeFact]]:
    return source, [_node(n) for n in _ids(prefix, count)]


def _drop(
    source: str, prefix: str, count: int, start: int = 0,
) -> tuple[str, list[NodeFact]]:
    return source, [_node(n, op="R") for n in _ids(prefix, count, start)]


def _publish(
    dsn: str, deployment: str, batches: list[tuple[str, list[NodeFact]]],
) -> str:
    """Apply each (source, facts) batch in order, then publish once."""
    with psycopg.connect(dsn, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn)
        for source, facts in batches:
            store.apply_facts(facts, source=source, strict_endpoints=True)
    result = Publisher(
        dsn, deployment, catalog_identity=bind_test_catalog_writer(dsn, deployment)
    ).cycle()
    assert result["status"] == "published", result
    return str(result["generation_id"])


def _start(
    dsn: str, tmp_path: Path, batches: list[tuple[str, list[NodeFact]]],
) -> str:
    deployment = make_search_deployment(tmp_path, dsn=dsn)
    with psycopg.connect(dsn, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["thing"])
    _publish(dsn, deployment, batches)
    return deployment


def _first_generation(dsn: str, tmp_path: Path) -> str:
    """30 file records, 5 history records, 3 notes."""
    return _start(
        dsn,
        tmp_path,
        [_add(FILES, "file", 30), _add(HISTORY, "commit", 5), _add(NOTES, "note", 3)],
    )


def _published(dsn: str, deployment: str) -> list[str]:
    """Published checkpoint ids of the deployment, oldest first."""
    with psycopg.connect(dsn, autocommit=True) as conn:
        rows = conn.execute(
            """
            SELECT checkpoint_id FROM public._chronos_branch_interval_checkpoints
             WHERE metadata::jsonb->>'deployment_name' = %s
               AND metadata::jsonb->>'status' = 'published'
               AND checkpoint_id NOT LIKE 'okg-run-base:%%'
             ORDER BY created_at::timestamptz
            """,
            (deployment,),
        ).fetchall()
    return [str(row[0]) for row in rows]


def _age(dsn: str, checkpoint_id: str, days: int) -> None:
    """Move a checkpoint's creation time back by ``days`` days."""
    with psycopg.connect(dsn, autocommit=True) as conn:
        (data_type,) = conn.execute(
            """
            SELECT data_type FROM information_schema.columns
             WHERE table_schema = 'public'
               AND table_name = '_chronos_branch_interval_checkpoints'
               AND column_name = 'created_at'
            """
        ).fetchone()
        value = "(now() - make_interval(days => %s))"
        if data_type == "text":
            value += "::text"
        conn.execute(
            "UPDATE public._chronos_branch_interval_checkpoints "
            f"SET created_at = {value} WHERE checkpoint_id = %s",
            (days, checkpoint_id),
        )


def _probe_module():  # noqa: ANN202
    # Imported here, not at module level, so the plain-doctor test still runs
    # (and fails on its behaviour) on a tree without the probe.
    from okg.substrate import source_record_loss

    return source_record_loss


def _probe(dsn: str, deployment: str) -> tuple[dict, dict]:
    report: dict = {"ok": True, "errors": []}
    result = doctor_cli._default_doctor_source_record_loss(dsn, deployment, report)
    return result, report


def _losses(result: dict) -> list[tuple[str, int, int]]:
    return [
        (x["source"], x["records_before"], x["records_now"]) for x in result["losses"]
    ]


def test_ownership_handoff_that_leaves_one_record_warns(
    clean_runtime: str, tmp_path: Path,
) -> None:
    """The #3776 shape: 29 of 30 file records move to the history source."""
    deployment = _first_generation(clean_runtime, tmp_path)
    _publish(
        clean_runtime,
        deployment,
        [_drop(FILES, "file", 29, start=1), (HISTORY, [_node(n) for n in _ids("file", 29, 1)])],
    )

    result, report = _probe(clean_runtime, deployment)

    assert result["ok"] is False
    assert result["sources_checked"] == 3
    (loss,) = result["losses"]
    assert loss["source"] == FILES
    assert loss["records_before"] == 30
    assert loss["records_now"] == 1
    assert loss["lost_share"] == pytest.approx(29 / 30, abs=1e-4)
    (warning,) = report["warnings"]
    assert warning["code"] == "source_records_lost"
    assert warning["source"] == FILES
    assert f"source {FILES} has 1 records" in warning["message"]
    assert "down from 30" in warning["message"]
    assert "deliberate removal" in warning["fix_hint"]
    assert "more than 7 days old" in warning["fix_hint"]
    assert "retire-source" not in warning["fix_hint"]
    # A warning, not an error.
    assert report["ok"] is True and report["errors"] == []


def test_small_source_that_falls_to_zero_warns(
    clean_runtime: str, tmp_path: Path,
) -> None:
    """A fall to zero warns even below the share rule's baseline."""
    deployment = _first_generation(clean_runtime, tmp_path)
    _publish(clean_runtime, deployment, [_drop(NOTES, "note", 3)])

    result, report = _probe(clean_runtime, deployment)

    assert _losses(result) == [(NOTES, 3, 0)]
    assert [w["code"] for w in report["warnings"]] == ["source_records_lost"]


def test_a_ten_percent_drop_stays_ok(clean_runtime: str, tmp_path: Path) -> None:
    deployment = _first_generation(clean_runtime, tmp_path)
    _publish(clean_runtime, deployment, [_drop(FILES, "file", 3)])

    result, report = _probe(clean_runtime, deployment)

    assert result["ok"] is True
    assert result["losses"] == []
    assert len(result["compared_generations"]) == 1
    assert report.get("warnings", []) == []


def test_a_normal_publish_stays_ok(clean_runtime: str, tmp_path: Path) -> None:
    deployment = _first_generation(clean_runtime, tmp_path)
    _publish(clean_runtime, deployment, [_add(HISTORY, "commit-new", 4)])

    result, report = _probe(clean_runtime, deployment)

    assert result["ok"] is True
    assert result["losses"] == []
    assert result["sources_checked"] == 3
    assert report.get("warnings", []) == []


def test_threshold_boundaries(clean_runtime: str, tmp_path: Path) -> None:
    """Exactly half lost stays ok; a baseline of 19 needs a fall to zero."""
    deployment = _start(
        clean_runtime,
        tmp_path,
        [_add("dep.half", "h", 20), _add("dep.small", "s", 19), _add("dep.over", "o", 20)],
    )
    _publish(
        clean_runtime,
        deployment,
        [_drop("dep.half", "h", 10), _drop("dep.small", "s", 18), _drop("dep.over", "o", 11)],
    )

    result, report = _probe(clean_runtime, deployment)

    assert _losses(result) == [("dep.over", 20, 9)]
    assert result["losses"][0]["lost_share"] == 0.55
    assert [w["source"] for w in report["warnings"]] == ["dep.over"]


def test_peak_in_an_older_generation_counts_until_it_leaves_the_window(
    clean_runtime: str, tmp_path: Path,
) -> None:
    """A slow leak: 40 -> 30 -> 18 never loses half between two publishes,
    but loses 55% against the peak two publishes back. Once that peak is
    older than the window, the check no longer sees it."""
    deployment = _start(clean_runtime, tmp_path, [_add(FILES, "file", 40)])
    _publish(clean_runtime, deployment, [_drop(FILES, "file", 10)])
    _publish(clean_runtime, deployment, [_drop(FILES, "file", 12, start=10)])
    first, second, third = _published(clean_runtime, deployment)

    result, report = _probe(clean_runtime, deployment)

    assert result["generation"] == third
    assert result["compared_generations"] == [second, first]
    (loss,) = result["losses"]
    assert (loss["source"], loss["records_before"], loss["records_now"]) == (FILES, 40, 18)
    assert loss["baseline_generation"] == first
    assert len(report["warnings"]) == 1

    _age(clean_runtime, first, _probe_module().WINDOW_DAYS + 1)
    result, report = _probe(clean_runtime, deployment)

    assert result["compared_generations"] == [second]
    assert result["ok"] is True and result["losses"] == []
    assert report.get("warnings", []) == []


def test_an_older_generation_without_counts_is_named_not_read_as_zero(
    clean_runtime: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The first generation is published with the count rollup switched off.
    monkeypatch.setenv(COUNT_ROLLUP_FLAG, "0")
    deployment = _start(clean_runtime, tmp_path, [_add(FILES, "file", 30)])
    monkeypatch.setenv(COUNT_ROLLUP_FLAG, "1")
    _publish(clean_runtime, deployment, [_add(HISTORY, "commit", 5)])
    _publish(clean_runtime, deployment, [_add(HISTORY, "commit-new", 5)])
    first, second, third = _published(clean_runtime, deployment)

    result, report = _probe(clean_runtime, deployment)

    assert result["ok"] is True
    assert result["compared_generations"] == [second]
    assert result["unreadable_generations"] == [
        {"generation": first, "reason": "ConsoleCountRollupUnavailable"}
    ]
    assert report.get("warnings", []) == []


def test_newest_generation_without_counts_skips_and_says_so(
    clean_runtime: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Both generations are published with the count rollup switched off, so
    # no generation has maintained counts. (Switching it off after a
    # maintained publish leaves the older counts visible as of later
    # generations, which is the rollup's own behaviour, not this check's.)
    monkeypatch.setenv(COUNT_ROLLUP_FLAG, "0")
    deployment = _first_generation(clean_runtime, tmp_path)
    _publish(clean_runtime, deployment, [_drop(FILES, "file", 30)])
    monkeypatch.setenv(COUNT_ROLLUP_FLAG, "1")

    result, report = _probe(clean_runtime, deployment)

    assert result["skipped"] is True
    assert result["code"] == "source_record_loss_skipped_no_counts"
    assert "no_sentinel" in result["reason"]
    assert report.get("warnings", []) == []


def test_counts_disabled_skips_and_says_so(
    clean_runtime: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    deployment = _first_generation(clean_runtime, tmp_path)
    _publish(clean_runtime, deployment, [_drop(NOTES, "note", 3)])
    monkeypatch.setenv(COUNT_ROLLUP_FLAG, "0")

    result, report = _probe(clean_runtime, deployment)

    assert result["skipped"] is True
    assert result["code"] == "source_record_loss_skipped_counts_disabled"
    assert report.get("warnings", []) == []


def test_internal_producer_names_are_not_sources(
    clean_runtime: str, tmp_path: Path,
) -> None:
    deployment = _start(
        clean_runtime, tmp_path,
        [_add("_fixture_internal", "internal", 30), _add(FILES, "file", 30)],
    )
    _publish(clean_runtime, deployment, [_drop("_fixture_internal", "internal", 30)])

    result, report = _probe(clean_runtime, deployment)

    assert result["ok"] is True
    assert result["sources_checked"] == 1
    assert report.get("warnings", []) == []


def test_retirement_skips_only_an_active_default_branch_retirement_after_the_peak(
    clean_runtime: str, tmp_path: Path,
) -> None:
    deployment = _first_generation(clean_runtime, tmp_path)
    _publish(clean_runtime, deployment, [_drop(NOTES, "note", 3)])

    def retire(*, status: str, branch: str, days_ago: int) -> tuple[dict, dict]:
        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            conn.execute(
                "DELETE FROM okg.source_retirement_receipts WHERE deployment_name = %s",
                (deployment,),
            )
            conn.execute(
                """
                INSERT INTO okg.source_retirement_receipts
                  (deployment_name, branch_name, registry_source_id, ownership_id,
                   source_subject_ids, registry_digest, pinned_checkpoint,
                   catalog_version_id, branch_mutation_token, footprint_digest,
                   plan_digest, plan, actor, status, applied_at, published_at)
                VALUES (%s, %s, 'notes', %s, '[]'::jsonb, 'r', 'c', 1,
                        't', 'f', 'p', '{}'::jsonb, 'test', %s,
                        now() - make_interval(days => %s),
                        now() - make_interval(days => %s))
                """,
                (deployment, branch, NOTES, status, days_ago, days_ago),
            )
        return _probe(clean_runtime, deployment)

    # Retired on the default branch after the records were seen: skipped.
    result, report = retire(status="published", branch="default", days_ago=0)
    assert result["ok"] is True
    assert result["retired_sources_skipped"] == [NOTES]
    assert report.get("warnings", []) == []

    # A planned retirement removed nothing yet: still a loss.
    result, _ = retire(status="planned", branch="default", days_ago=0)
    assert _losses(result) == [(NOTES, 3, 0)]

    # A retirement on another branch says nothing about the default one.
    result, _ = retire(status="published", branch="experiment", days_ago=0)
    assert _losses(result) == [(NOTES, 3, 0)]

    # A retirement older than the peak: the source came back and lost again.
    result, _ = retire(status="published", branch="default", days_ago=2)
    assert _losses(result) == [(NOTES, 3, 0)]
    assert result["retired_sources_skipped"] == []


def test_one_published_generation_skips_and_says_so(
    clean_runtime: str, tmp_path: Path,
) -> None:
    deployment = _first_generation(clean_runtime, tmp_path)

    result, report = _probe(clean_runtime, deployment)

    assert result["ok"] is True
    assert result["skipped"] is True
    assert result["code"] == "source_record_loss_skipped_no_previous_generation"
    assert result["published_generations"] == 1
    assert result["newest_generation_in_window"] is True
    assert result["previous_generations_in_window"] == 0
    assert report.get("warnings", []) == []


def test_generations_older_than_the_window_are_reported_as_found(
    clean_runtime: str, tmp_path: Path,
) -> None:
    """A graph not published for over a week: two generations exist, the
    newest is outside the window, and none before it is inside."""
    deployment = _first_generation(clean_runtime, tmp_path)
    _publish(clean_runtime, deployment, [_drop(NOTES, "note", 3)])
    first, second = _published(clean_runtime, deployment)
    _age(clean_runtime, first, 10)
    _age(clean_runtime, second, 9)

    result, report = _probe(clean_runtime, deployment)

    assert result["code"] == "source_record_loss_skipped_no_previous_generation"
    assert result["published_generations"] == 2
    assert result["newest_generation"] == second
    assert result["newest_generation_in_window"] is False
    assert result["previous_generations_in_window"] == 0
    assert report.get("warnings", []) == []


def test_more_generations_than_the_cap_are_sampled_across_the_window(
    clean_runtime: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The cap is lowered to 3 so the test publishes 6 generations, not 52;
    the sampling code is the same. The peak is in the oldest generation,
    outside the newest 3, and still counts."""
    monkeypatch.setattr(_probe_module(), "MAX_BASELINE_GENERATIONS", 3)
    deployment = _start(clean_runtime, tmp_path, [_add(FILES, "file", 40)])
    _publish(clean_runtime, deployment, [_drop(FILES, "file", 10)])
    for i in range(3):
        _publish(clean_runtime, deployment, [_add(HISTORY, f"commit{i}", 1)])
    _publish(clean_runtime, deployment, [_drop(FILES, "file", 12, start=10)])
    g1, g2, g3, g4, g5, g6 = _published(clean_runtime, deployment)

    result, report = _probe(clean_runtime, deployment)

    assert result["generation"] == g6
    assert result["previous_generations_in_window"] == 5
    # Newest previous and oldest always, the rest spread between them.
    assert result["compared_generations"] == [g5, g3, g1]
    assert _losses(result) == [(FILES, 40, 18)]
    assert result["losses"][0]["baseline_generation"] == g1
    assert [w["code"] for w in report["warnings"]] == ["source_records_lost"]


def test_a_timed_out_count_read_is_a_failed_probe_never_a_skip(
    clean_runtime: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A real database failure inside the count read: another session holds
    an exclusive lock on the rollup's tables, and the read hits the probe's
    statement timeout. The maintained-derivation read reports that as
    "unavailable"; the probe must still report a failure."""
    deployment = _first_generation(clean_runtime, tmp_path)
    _publish(clean_runtime, deployment, [_drop(NOTES, "note", 3)])
    monkeypatch.setattr(_probe_module(), "TIMEOUT_MS", 300)

    holder = psycopg.connect(clean_runtime)
    try:
        names = [
            f'"{schema}"."{name}"'
            for schema, name in holder.execute(
                """
                SELECT n.nspname, c.relname FROM pg_class c
                  JOIN pg_namespace n ON n.oid = c.relnamespace
                 WHERE c.relname LIKE '%%console_count_rollup%%'
                   AND c.relkind IN ('r', 'p', 'v')
                """
            ).fetchall()
        ]
        assert names
        holder.execute(f"LOCK TABLE {', '.join(names)} IN ACCESS EXCLUSIVE MODE")

        result, report = _probe(clean_runtime, deployment)
    finally:
        holder.rollback()
        holder.close()

    assert result["ok"] is False
    assert result["code"] == "source_record_loss_probe_failed"
    assert "skipped" not in result
    # The failure came through the maintained-derivation conversion, the
    # path that used to turn it into a skip.
    assert result["message"].startswith("ConsoleCountRollupUnavailable"), result
    assert "statement timeout" in result["message"], result
    assert [w["code"] for w in report["warnings"]] == [
        "source_record_loss_probe_failed"
    ]


def test_plain_doctor_reports_it_and_keeps_its_exit_status(
    clean_runtime: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The wiring: plain doctor runs the check for a deployment."""
    deployment = _first_generation(clean_runtime, tmp_path)
    _publish(clean_runtime, deployment, [_drop(NOTES, "note", 3)])
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(Path(deployment).parent))
    monkeypatch.setenv("OKG_DSN", clean_runtime)
    args = argparse.Namespace(
        deployment=deployment, dsn=clean_runtime, check=None, json=True
    )

    exit_with = doctor_cli._cmd_doctor(args, as_json=True)
    report = json.loads(capsys.readouterr().out)

    assert report["source_record_loss"]["losses"][0]["source"] == NOTES
    codes = [w["code"] for w in report.get("warnings", [])]
    assert codes.count("source_records_lost") == 1

    # Without the finding the verdict is the same: it never turns doctor red.
    monkeypatch.setattr(
        doctor_cli,
        "_default_doctor_source_record_loss",
        lambda dsn, deployment, report: {"ok": True, "losses": []},
    )
    exit_without = doctor_cli._cmd_doctor(args, as_json=True)
    without = json.loads(capsys.readouterr().out)
    assert without["ok"] == report["ok"]
    assert exit_without == exit_with
