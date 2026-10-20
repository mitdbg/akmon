"""End-to-end: an unchanged `reference_catalog` connector rerun is admitted.

The CRAB and comp-ops `cmssw_releases` sources are `reference_catalog`
sources read by archi connectors built on `okg.deployment`. `okg ingest` and
`okg provision` hand such a source `release_new`, and the readers claimed a
complete scope only for `scope_complete` or `reconcile`. Every unchanged
rerun then appended nothing, admission rejected it as `partial`, and
`okg provision --publish-once` skipped its publish.

Each test runs the real ingest service on a source registry that names a
connector class from
`tests/substrate/runtime/test_connector_completed_scope_modes.py`. The first
run appends the catalog and publishes it for real, as
`okg provision --publish-once` does; later runs read the same catalog.

What is covered, connector by connector:

* `SdkSetCatalogSource` and `ProbedSdkSetCatalogSource` are the archi shape
  after the fix: no record set of their own (the runner infers it from the
  facts), and the second has the content-hash change probe `cmssw` declares.
  Neither declares `probe_short_circuit_safe` (no archi reader does), so the
  runner reads both runs; the unchanged rerun is admitted as
  `authoritative_replay`. This is the CRAB path. The `github` shape (two
  nodes and an edge per record, run id in the revision) is covered too.
* `ExplicitRecordSetCatalogSource` returns its own record set; same verdict.
* `HardCodedCatalogSource` is the reader before the fix. Once its first run
  has published, okg counts that published admission as history, so its
  unchanged reruns are accepted `incomplete_scope_noop`; it still never
  retracts and never records a reconcile time.
* `CheckpointCatalogSource` is the shape chosen for archi's DBS and CondDB
  readers: it never claims a complete scope, so it never deletes, and it
  returns a content-identifying checkpoint. An unchanged rerun is accepted as
  `incomplete_scope_noop` and a changed input appends without retracting.
  These sources stay non-deleting because okg has no guard on how much one
  run may retract (okg#3307).
* A brand-new source with an empty first run, a source whose only run was
  never published, and a source whose only run failed are still refused
  `partial`.

The "upgraded" tests seed what a deployment upgraded from the earlier reader
holds: one accepted, published `release_new` run that kept no cursor, so the
watermark row has an empty cursor, an empty published revision, projection
cursor 0 and no reconcile time. A complete-scope reader's first rerun there
replays the existing rows and retracts none; a checkpoint reader's rerun is
an accepted no-op. Before okg counted a published admission as history, the
checkpoint reader's rerun was rejected `partial`.

Not covered: the runner skipping the second read on an unchanged probe
(`no_change`, admitted `unchanged`). That needs the adapter to declare
`probe_short_circuit_safe`, which in turn needs an exact applied token that
`ConnectorRun` does not carry; a connector that declares it has its first run
refused (`applied_token_not_exact_global`). No archi reader declares it.

Uses a migrated disposable test database (``clean_runtime``).
"""
from __future__ import annotations

from pathlib import Path

import psycopg
import pytest
import yaml

from okg.substrate.ingest import IngestOptions, run_ingest
from tests.pytest.catalog_fixture_builders import seed_catalog
from tests.substrate.runtime import test_connector_completed_scope_modes as modes
from tests.substrate.test_ingest_service import (
    _claim_layered_fixture_catalog,
    _write_layered_fixture_deployment,
)

pytestmark = pytest.mark.postgres


#: Lets the `github` shape's `references` edge between two documents publish.
_DOCUMENT_REFERENCES_BRIDGE = """\
id: https://example.invalid/okg/fixture-bridges
name: ingest_fixture_bridges
classes:
  EdgeNarrowings:
    annotations:
      okg_edge_narrowings: 'true'
    attributes:
      document_references_document:
        annotations:
          edge_archetype: references
          src_subtypes: Document
          dst_subtypes: Document
"""


def _nodes_per_release(shape: str) -> int:
    return 2 if shape == "github" else 1


def _deployment(
    tmp_path: Path,
    dsn: str,
    source_class_name: str,
    *,
    shape: str = "cmssw",
) -> tuple[Path, str]:
    with psycopg.connect(dsn, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["document"])
    dep = _write_layered_fixture_deployment(tmp_path)
    bridges = dep / "schemas" / "bridges"
    bridges.mkdir()
    (bridges / "document_bridges.yaml").write_text(_DOCUMENT_REFERENCES_BRIDGE)
    catalog = str(modes.write_catalog(tmp_path / "releases.txt"))
    registry_path = dep / "source_registry.yaml"
    registry = yaml.safe_load(registry_path.read_text())
    first = registry["sources"]["first"]
    body = modes.reference_catalog_entry_body(first["ownership_id"])
    body["class"] = source_class_name
    body["params"] = {"path": catalog, "shape": shape}
    # Keep the fixture's admission and privacy policy; replace the reader,
    # its profile and its sync block with the reference-catalog connector.
    first.pop("sync", None)
    registry["sources"]["first"] = {**first, **body}
    registry_path.write_text(yaml.safe_dump(registry))
    _claim_layered_fixture_catalog(dsn, dep)
    return dep, catalog


def _ingest(dsn: str, dep: Path, *, publish: bool) -> tuple[dict, dict]:
    outcome = run_ingest(
        IngestOptions(
            dsn=dsn,
            deployment="svc",
            deployment_dir=dep,
            include={"first"},
            include_predecessors=False,
            include_dependents=False,
            publish=publish,
        )
    )
    (record,) = outcome.report["results"]
    return {"any_failed": outcome.any_failed, **record}, outcome.report


def _switch_reader(dsn: str, dep: Path, source_class_name: str) -> None:
    """Replace the registry's reader class, as an upgrade of the reader does."""
    registry_path = dep / "source_registry.yaml"
    registry = yaml.safe_load(registry_path.read_text())
    registry["sources"]["first"]["class"] = source_class_name
    registry_path.write_text(yaml.safe_dump(registry))
    _claim_layered_fixture_catalog(dsn, dep)


def _first_run_publishes(dsn: str, dep: Path, *, shape: str = "cmssw") -> dict:
    first, report = _ingest(dsn, dep, publish=True)
    assert first["any_failed"] is False, first
    assert first["admission_status"] in {"accepted", "published"}, first
    assert first["nodes_appended"] == (
        len(modes.RELEASES) * _nodes_per_release(shape)
    ), first
    publish = report["publishes"][0]
    assert publish["status"] == "published", publish
    with psycopg.connect(dsn, autocommit=True) as conn:
        status = conn.execute(
            "SELECT status FROM okg.admission_envelopes"
            " WHERE admission_id = %s::uuid",
            (first["admission_id"],),
        ).fetchone()
    assert status == ("published",), "the publish did not sweep the admission"
    return first


def _watermark(dsn: str) -> tuple | None:
    with psycopg.connect(dsn, autocommit=True) as conn:
        return conn.execute(
            "SELECT last_applied_rev, last_reconcile_at"
            " FROM okg.source_watermarks WHERE source = 'svc.first'"
        ).fetchone()


def _watermark_row(dsn: str) -> tuple | None:
    with psycopg.connect(dsn, autocommit=True) as conn:
        return conn.execute(
            "SELECT last_applied_rev, last_published_rev, projection_cursor,"
            " last_reconcile_at"
            " FROM okg.source_watermarks WHERE source = 'svc.first'"
        ).fetchone()


#: What a deployment upgraded from the earlier reader holds for the source:
#: one accepted, published `release_new` run that claimed no complete scope
#: and kept no cursor, and nothing since.
_UPGRADED_STATE = ({}, {}, 0, None)


def _seed_upgraded_state(
    tmp_path: Path, dsn: str, *, shape: str = "cmssw",
) -> tuple[Path, str]:
    dep, catalog = _deployment(
        tmp_path, dsn, "HardCodedCatalogSource", shape=shape,
    )
    _first_run_publishes(dsn, dep, shape=shape)
    assert _watermark_row(dsn) == _UPGRADED_STATE
    return dep, catalog


def _policy(record: dict) -> dict:
    return record["admission_verdict"]["producer_policy"]


@pytest.mark.parametrize(
    ("source_class_name", "second_run_reads", "run_mode", "reason"),
    [
        ("SdkSetCatalogSource", True, "release_new", "authoritative_replay"),
        ("ProbedSdkSetCatalogSource", True, "release_new", "authoritative_replay"),
        ("ExplicitRecordSetCatalogSource", True, "release_new", "authoritative_replay"),
    ],
)
def test_an_unchanged_rerun_of_a_connector_using_the_sdk_set_is_admitted(
    tmp_path: Path,
    clean_runtime: str,
    source_class_name: str,
    second_run_reads: bool,
    run_mode: str,
    reason: str,
) -> None:
    dep, catalog = _deployment(tmp_path, clean_runtime, source_class_name)

    _first_run_publishes(clean_runtime, dep)
    assert modes.read_count(catalog) == 1
    watermark = _watermark(clean_runtime)
    assert watermark is not None and watermark[1] is not None, (
        "a completed scope records its reconcile time"
    )

    second, _report = _ingest(clean_runtime, dep, publish=False)
    assert modes.read_count(catalog) == (2 if second_run_reads else 1)
    assert second["any_failed"] is False, second
    assert second["nodes_appended"] == 0, second
    assert second["admission_status"] == "accepted", second
    policy = _policy(second)
    assert policy["run_mode"] == run_mode
    assert policy["completed_scope"] is True
    assert policy["reason"] == reason


def test_a_hard_coded_reader_reruns_as_a_noop_but_never_completes_its_scope(
    tmp_path: Path,
    clean_runtime: str,
) -> None:
    """What the hard-coded list still costs once the source has published.

    Before okg counted a published admission as history, every one of these
    reruns was rejected `partial` and `okg provision --publish-once` stopped
    publishing. Now they are accepted no-ops, but the reader still never
    claims its whole scope: nothing it drops is ever retracted, and the
    source never records a reconcile time.
    """
    dep, catalog = _deployment(tmp_path, clean_runtime, "HardCodedCatalogSource")

    _first_run_publishes(clean_runtime, dep)
    assert _watermark_row(clean_runtime) == _UPGRADED_STATE

    for attempt in (2, 3):
        rerun, _report = _ingest(clean_runtime, dep, publish=False)
        assert modes.read_count(catalog) == attempt
        assert rerun["any_failed"] is False, rerun
        assert rerun["nodes_appended"] == 0, rerun
        assert (rerun["retract_n"], rerun["retract_e"]) == (0, 0), rerun
        assert rerun["admission_status"] == "accepted", rerun
        policy = _policy(rerun)
        assert policy["run_mode"] == "release_new"
        assert policy["completed_scope"] is False
        assert policy["source_has_published_watermark"] is True
        assert policy["source_history"] == "published_source_admission"
        assert policy["reason"] == "incomplete_scope_noop"
        # The no-op saves no source state: still no cursor, no reconcile.
        assert _watermark_row(clean_runtime) == _UPGRADED_STATE


def _envelope_status(dsn: str, admission_id: str) -> str | None:
    with psycopg.connect(dsn, autocommit=True) as conn:
        row = conn.execute(
            "SELECT status FROM okg.admission_envelopes"
            " WHERE admission_id = %s::uuid",
            (admission_id,),
        ).fetchone()
    return None if row is None else row[0]


def test_a_non_deleting_reader_with_a_checkpoint_reruns_as_an_accepted_noop(
    tmp_path: Path,
    clean_runtime: str,
) -> None:
    dep, catalog = _deployment(tmp_path, clean_runtime, "CheckpointCatalogSource")

    _first_run_publishes(clean_runtime, dep)
    watermark = _watermark(clean_runtime)
    assert watermark is not None, "the accepted run stored no watermark"
    assert watermark[0].get("content_sha256"), (
        "the connector's checkpoint was not stored as the cursor"
    )
    assert watermark[1] is None, "an incomplete scope recorded a reconcile time"
    first_cursor = dict(watermark[0])

    # Unchanged input: nothing appended, accepted as a no-op, publish runs.
    second, report = _ingest(clean_runtime, dep, publish=True)
    assert modes.read_count(catalog) == 2
    assert second["any_failed"] is False, second
    assert second["nodes_appended"] == 0, second
    assert (second["retract_n"], second["retract_e"]) == (0, 0), second
    assert second["admission_status"] == "accepted", second
    policy = _policy(second)
    assert policy["run_mode"] == "release_new"
    assert policy["completed_scope"] is False
    assert policy["source_has_published_watermark"] is True
    assert policy["source_history"] == "cursor"
    assert policy["reason"] == "incomplete_scope_noop"
    # Nothing new to publish: the publish step is skipped, not blocked.
    (publish,) = report["publishes"]
    assert publish["status"] == "skipped", publish
    assert publish["reason"] == "no_source_work", publish
    assert dict(_watermark(clean_runtime)[0]) == first_cursor

    # Changed input: one release added, one dropped. The new one is
    # appended; the dropped one is not retracted, because no run of this
    # reader claims the whole scope.
    modes.write_catalog(
        Path(catalog), (modes.RELEASES[1], "CMSSW_14_1_0"),
    )
    third, report = _ingest(clean_runtime, dep, publish=True)
    assert third["any_failed"] is False, third
    assert third["nodes_appended"] == 1, third
    assert (third["retract_n"], third["retract_e"]) == (0, 0), third
    assert third["admission_status"] in {"accepted", "published"}, third
    assert report["publishes"][0]["status"] == "published", report["publishes"]
    assert _envelope_status(clean_runtime, third["admission_id"]) == "published"
    assert dict(_watermark(clean_runtime)[0]) != first_cursor


def _assert_refused_partial(record: dict) -> None:
    assert record["any_failed"] is True, record
    assert record["nodes_appended"] == 0, record
    assert record["admission_status"] == "rejected", record
    policy = _policy(record)
    assert policy["completed_scope"] is False
    assert policy["source_has_published_watermark"] is False
    assert policy["source_history"] is None
    assert policy["reason"] == "partial"


def test_a_brand_new_source_whose_first_run_is_empty_is_refused(
    tmp_path: Path,
    clean_runtime: str,
) -> None:
    """No envelope at all: an empty incomplete first run hides a reader
    that reads nothing, so it stays refused."""
    dep, catalog = _deployment(tmp_path, clean_runtime, "CheckpointCatalogSource")
    modes.write_catalog(Path(catalog), ())

    first, _report = _ingest(clean_runtime, dep, publish=False)
    assert modes.read_count(catalog) == 1
    _assert_refused_partial(first)


def test_a_source_whose_only_run_was_never_published_is_refused(
    tmp_path: Path,
    clean_runtime: str,
) -> None:
    dep, catalog = _deployment(tmp_path, clean_runtime, "NoCheckpointCatalogSource")

    first, _report = _ingest(clean_runtime, dep, publish=False)
    assert first["any_failed"] is False, first
    assert first["nodes_appended"] == len(modes.RELEASES), first
    assert _envelope_status(clean_runtime, first["admission_id"]) == "accepted"

    second, _report = _ingest(clean_runtime, dep, publish=False)
    assert modes.read_count(catalog) == 2
    _assert_refused_partial(second)


def test_a_source_whose_only_run_failed_is_refused(
    tmp_path: Path,
    clean_runtime: str,
) -> None:
    dep, catalog = _deployment(tmp_path, clean_runtime, "NoCheckpointCatalogSource")
    Path(catalog).unlink()

    first, _report = _ingest(clean_runtime, dep, publish=False)
    assert first["any_failed"] is True, first
    assert first["status"] == "FAIL", first
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        statuses = [
            row[0]
            for row in conn.execute(
                "SELECT status FROM okg.admission_envelopes"
                " WHERE source = 'svc.first' AND intent = 'source_sync'"
            ).fetchall()
        ]
    assert statuses and "published" not in statuses, statuses

    modes.write_catalog(Path(catalog), ())
    second, _report = _ingest(clean_runtime, dep, publish=False)
    _assert_refused_partial(second)


# --- A deployment upgraded from the earlier reader ---------------------------


@pytest.mark.parametrize(
    ("source_class_name", "shape"),
    [
        ("SdkSetCatalogSource", "cmssw"),
        ("ProbedSdkSetCatalogSource", "cmssw"),
        ("SdkSetCatalogSource", "github"),
    ],
)
def test_upgraded_state_complete_scope_reader_rerun_is_admitted_without_retraction(
    tmp_path: Path,
    clean_runtime: str,
    source_class_name: str,
    shape: str,
) -> None:
    """The first complete run has no prior record set to diff against, so
    it must replay the existing rows and retract none of them."""
    dep, catalog = _seed_upgraded_state(tmp_path, clean_runtime, shape=shape)
    _switch_reader(clean_runtime, dep, source_class_name)

    rerun, _report = _ingest(clean_runtime, dep, publish=False)
    assert modes.read_count(catalog) == 2
    assert rerun["any_failed"] is False, rerun
    assert (rerun["nodes_appended"], rerun["edges_appended"]) == (0, 0), rerun
    assert (rerun["retract_n"], rerun["retract_e"]) == (0, 0), rerun
    assert rerun["admission_status"] == "accepted", rerun
    policy = _policy(rerun)
    assert policy["run_mode"] == "release_new"
    assert policy["completed_scope"] is True
    assert policy["reason"] == "authoritative_replay"
    watermark = _watermark_row(clean_runtime)
    assert watermark[3] is not None, "the complete scope recorded no reconcile time"


def test_upgraded_state_checkpoint_reader_rerun_is_an_accepted_noop(
    tmp_path: Path,
    clean_runtime: str,
) -> None:
    """The DBS/CondDB shape on an upgraded deployment.

    The published first run left no cursor, so before okg counted a
    published admission as history this rerun was rejected `partial`.
    """
    dep, catalog = _seed_upgraded_state(tmp_path, clean_runtime)
    _switch_reader(clean_runtime, dep, "CheckpointCatalogSource")

    rerun, report = _ingest(clean_runtime, dep, publish=True)
    assert modes.read_count(catalog) == 2
    assert rerun["any_failed"] is False, rerun
    assert rerun["nodes_appended"] == 0, rerun
    assert (rerun["retract_n"], rerun["retract_e"]) == (0, 0), rerun
    assert rerun["admission_status"] == "accepted", rerun
    policy = _policy(rerun)
    assert policy["run_mode"] == "release_new"
    assert policy["completed_scope"] is False
    assert policy["source_has_published_watermark"] is True
    assert policy["source_history"] == "published_source_admission"
    assert policy["reason"] == "incomplete_scope_noop"
    # The publish is not blocked. It may still publish: switching the reader
    # re-applied the deployment catalog, which this test does not control.
    (publish,) = report["publishes"]
    assert publish["status"] in {"published", "skipped"}, publish
    # A no-op saves no source state, so the proposed checkpoint is not kept.
    assert _watermark_row(clean_runtime) == _UPGRADED_STATE


def _seed_published_envelope(
    dsn: str, *, deployment: str, producer_kind: str, source: str,
) -> None:
    """A published admission envelope, written as the admission writer does."""
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(
            """
            INSERT INTO okg.admission_envelopes
              (deployment_name, producer_id, producer_kind, intent,
               admission_mode, status, source, run_id, idempotency_key,
               published_at)
            VALUES (%s, %s, %s, 'append_facts', 'fast_track', 'published',
                    %s, gen_random_uuid(), gen_random_uuid()::text, now())
            """,
            (deployment, source, producer_kind, source),
        )


@pytest.mark.parametrize(
    ("deployment", "producer_kind", "source", "counts"),
    [
        # Positive control: the same seeding does count when it matches.
        ("svc", "source", "svc.first", True),
        ("other-deployment", "source", "svc.first", False),
        ("svc", "enricher", "svc.first", False),
        ("svc", "source", "svc.other", False),
    ],
    ids=["matching", "other-deployment", "other-producer-kind", "other-source"],
)
def test_only_this_sources_published_admission_counts_as_history(
    tmp_path: Path,
    clean_runtime: str,
    deployment: str,
    producer_kind: str,
    source: str,
    counts: bool,
) -> None:
    """A source with no runs of its own, beside one published envelope."""
    dep, catalog = _deployment(tmp_path, clean_runtime, "CheckpointCatalogSource")
    modes.write_catalog(Path(catalog), ())
    _seed_published_envelope(
        clean_runtime,
        deployment=deployment,
        producer_kind=producer_kind,
        source=source,
    )

    first, _report = _ingest(clean_runtime, dep, publish=False)
    assert modes.read_count(catalog) == 1
    if not counts:
        _assert_refused_partial(first)
        assert _policy(first)["source_history"] is None
        return
    assert first["any_failed"] is False, first
    assert first["admission_status"] == "accepted", first
    policy = _policy(first)
    assert policy["reason"] == "incomplete_scope_noop"
    assert policy["source_history"] == "published_source_admission"
