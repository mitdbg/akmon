"""Unit tests for `okg doctor --check sources`.

Per `doctor-sources-check` (2026-04-26).
"""
from __future__ import annotations

import json
import textwrap
from pathlib import Path
from uuid import uuid4

import psycopg
import pytest


def _run_doctor(
    deployment_name: str,
    *,
    dsn: str | None = None,
) -> tuple[int, dict]:
    """Invoke the doctor sources check via the CLI main()
    entrypoint and parse the JSON report."""
    import argparse
    from okg.substrate.cli.doctor import _doctor_sources

    args = argparse.Namespace(
        deployment=deployment_name,
        dsn=dsn,
    )
    # Capture stdout via the JSON path
    import io
    import contextlib
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        rc = _doctor_sources(args, as_json=True)
    payload = json.loads(buf.getvalue())
    return rc, payload


def _write_deployment(tmp_path: Path, *, name: str, registry_yaml: str) -> Path:
    import yaml
    d = tmp_path / name
    d.mkdir()
    (d / "deployment.yaml").write_text(
        yaml.safe_dump({"name": name, "modules": []})
    )
    (d / "source_registry.yaml").write_text(registry_yaml)
    return d


@pytest.fixture
def patched_finder(tmp_path, monkeypatch):
    """Make `find_deployment_dir` look up by an in-memory map."""
    fake = {}

    def _find(name):
        return fake.get(name)

    monkeypatch.setattr(
        "okg.substrate.deployments.find_deployment_dir",
        _find,
    )
    return fake


def test_typoed_class_path_is_FAIL(tmp_path, patched_finder):
    registry = textwrap.dedent("""\
        sources:
          bad-source:
            module: okg.substrate.library.sources.git_graph_files
            class: ThisClassDoesNotExist
            ownership_id: t.bad
            params: {}
            source_class: discovery_crawl
            record_identity_kind: scoped_locator
            record_identity_fields: [path]
            source_revision_kind: content_hash
            deletion_semantics: missing_from_completed_scope
            publication_mode: published_generation
    """)
    d = _write_deployment(
        tmp_path, name="t", registry_yaml=registry,
    )
    patched_finder["t"] = d

    rc, payload = _run_doctor("t")
    assert rc != 0
    assert payload["ok"] is False
    assert payload["sources"][0]["status"] == "FAIL"
    assert any(
        e["code"] == "source_class_not_resolvable"
        for e in payload["errors"]
    )


def test_unknown_module_is_FAIL(tmp_path, patched_finder):
    registry = textwrap.dedent("""\
        sources:
          bad-mod:
            module: okg.does.not.exist
            class: Whatever
            ownership_id: t.badmod
            params: {}
            source_class: discovery_crawl
            record_identity_kind: scoped_locator
            record_identity_fields: [path]
            source_revision_kind: content_hash
            deletion_semantics: missing_from_completed_scope
            publication_mode: published_generation
    """)
    d = _write_deployment(
        tmp_path, name="t2", registry_yaml=registry,
    )
    patched_finder["t2"] = d

    rc, payload = _run_doctor("t2")
    assert rc != 0
    assert payload["sources"][0]["status"] == "FAIL"


def test_valid_class_returns_OK_or_WARN(tmp_path, patched_finder):
    """A real, valid adapter should resolve and either
    construct (OK) or fail at construction (WARN). Never
    FAIL."""
    registry = textwrap.dedent("""\
        sources:
          good-source:
            module: okg.substrate.library.sources.git_graph_files
            class: GitFilesSource
            ownership_id: t.good
            params:
              repo: testrepo
              repo_root: /tmp
            source_class: discovery_crawl
            record_identity_kind: scoped_locator
            record_identity_fields: [repo, path]
            source_revision_kind: content_hash
            deletion_semantics: missing_from_completed_scope
            publication_mode: published_generation
    """)
    d = _write_deployment(
        tmp_path, name="t3", registry_yaml=registry,
    )
    patched_finder["t3"] = d

    rc, payload = _run_doctor("t3")
    status = payload["sources"][0]["status"]
    assert status in ("OK", "WARN"), status
    if status == "OK":
        assert rc == 0
        assert payload["ok"] is True


def test_no_registry_skips_cleanly(tmp_path, patched_finder, monkeypatch):
    import yaml
    d = tmp_path / "norreg"
    d.mkdir()
    (d / "deployment.yaml").write_text(
        yaml.safe_dump({"name": "norreg", "modules": []})
    )
    patched_finder["norreg"] = d

    rc, payload = _run_doctor("norreg")
    assert rc == 0
    assert payload["ok"] is True
    assert "skipped" in payload


@pytest.mark.postgres
def test_source_coverage_reports_predecessor_and_live_counts(
    tmp_path, patched_finder, clean_runtime,
) -> None:
    registry = textwrap.dedent("""\
        sources:
          upstream:
            module: okg.substrate.library.sources.git_graph_files
            class: GitFilesSource
            ownership_id: t.upstream
            params:
              repo: testrepo
              repo_root: /tmp
            source_class: discovery_crawl
            record_identity_kind: scoped_locator
            record_identity_fields: [repo, path]
            source_revision_kind: content_hash
            deletion_semantics: missing_from_completed_scope
            publication_mode: published_generation
          downstream:
            module: okg.substrate.library.sources.git_graph_files
            class: GitFilesSource
            ownership_id: t.downstream
            predecessor_source: t.upstream
            params:
              repo: testrepo
              repo_root: /tmp
            source_class: discovery_crawl
            record_identity_kind: scoped_locator
            record_identity_fields: [repo, path]
            source_revision_kind: content_hash
            deletion_semantics: missing_from_completed_scope
            publication_mode: published_generation
    """)
    d = _write_deployment(
        tmp_path, name="tcov", registry_yaml=registry,
    )
    patched_finder["tcov"] = d

    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.graph_generations
              (generation_id, deployment_name, status,
               catalog_version_id, correlation_id)
            VALUES (1, 'tcov', 'pending', 1, 'tcov-1')
            """
        )
        observed_at = "2026-01-01T00:00:00Z"
        cur.execute(
            """
            INSERT INTO okg.node_facts
              (node_id, subtype, attrs, source, source_record_id,
               source_revision, observed_at, dedupe_key, run_id, op)
            VALUES (
              'node:one', 'source_file', '{"path":"a.md"}'::jsonb,
              't.upstream', '{}'::jsonb, '{}'::jsonb,
              %s::timestamptz, 'dedupe-one', %s, 'I'
            )
            RETURNING fact_id
            """,
            (observed_at, str(uuid4())),
        )
        fact_id = cur.fetchone()[0]
        # Chronos cutover: okg.nodes_live was dropped. The "live" node set the
        # coverage doctor counts is the default-branch graph_nodes, read
        # through a Chronos checkout. Seed the live node via apply_facts so its
        # (source, subtype) lands in graph_nodes with the version columns a
        # branch query needs.
        from okg.substrate.graph_store import ChronosGraphStore
        from okg.substrate.library.sources.base import NodeFact

        ChronosGraphStore.from_connection(conn, ensure_schema=False).apply_facts(
            [
                NodeFact(
                    "node:one",
                    "source_file",
                    {"path": "a.md"},
                    {},
                    {},
                )
            ],
            branch="default",
            source="t.upstream",
            strict_endpoints=False,
        )
        cur.execute(
            """
            INSERT INTO okg.source_watermarks
              (source, projection_cursor, last_reconcile_at,
               last_applied_rev)
            VALUES ('t.upstream', %s, now(), %s::jsonb)
            """,
            (
                fact_id,
                json.dumps({
                    f"cursor-{i:02d}": "x" * 100
                    for i in range(30)
                }),
            ),
        )
        cur.execute(
            """
            INSERT INTO okg.source_progress
              (deployment, source_name, key, value)
            VALUES ('tcov', 't.upstream', 'record:node:one', 'ok')
            ON CONFLICT (deployment, source_name, key)
            DO UPDATE SET value = EXCLUDED.value,
                          updated_at = now()
            """
        )

    rc, payload = _run_doctor("tcov", dsn=clean_runtime)
    assert rc == 0
    assert payload["runtime_available"] is True

    by_name = {s["name"]: s for s in payload["sources"]}
    upstream = by_name["upstream"]
    assert upstream["coverage"]["status"] == "OK"
    assert upstream["coverage"]["watermark"]["projection_cursor"] == fact_id
    assert upstream["coverage"]["node_facts"]["max_fact_id"] == fact_id
    assert upstream["coverage"]["node_projection"] == {
        "cursor": fact_id,
        "max_fact_id": fact_id,
        "lag": 0,
    }
    assert upstream["coverage"]["watermark"]["last_applied_rev"] == {
        "type": "object",
        "keys": 30,
        "sample_keys": [
            "cursor-00",
            "cursor-01",
            "cursor-02",
            "cursor-03",
            "cursor-04",
            "cursor-05",
            "cursor-06",
            "cursor-07",
            "cursor-08",
            "cursor-09",
            "cursor-10",
            "cursor-11",
        ],
        "truncated": True,
    }
    assert upstream["coverage"]["progress"]["rows"] == 1
    assert upstream["coverage"]["live_nodes_by_subtype"] == {
        "source_file": 1,
    }

    downstream = by_name["downstream"]
    assert downstream["coverage"]["ready_to_run"] is True
    assert downstream["predecessors"] == [{
        "declared": "t.upstream",
        "ownership_id": "t.upstream",
        "ready": True,
        "last_reconcile_at": downstream["predecessors"][0][
            "last_reconcile_at"
        ],
        "projection_cursor": fact_id,
    }]


@pytest.mark.postgres
def test_source_coverage_warns_on_projection_lag(
    tmp_path, patched_finder, clean_runtime,
) -> None:
    registry = textwrap.dedent("""\
        sources:
          laggy:
            module: okg.substrate.library.sources.git_graph_files
            class: GitFilesSource
            ownership_id: t.laggy
            params:
              repo: testrepo
              repo_root: /tmp
            source_class: discovery_crawl
            record_identity_kind: scoped_locator
            record_identity_fields: [repo, path]
            source_revision_kind: content_hash
            deletion_semantics: missing_from_completed_scope
            publication_mode: published_generation
    """)
    d = _write_deployment(
        tmp_path, name="tlag", registry_yaml=registry,
    )
    patched_finder["tlag"] = d

    run_id = str(uuid4())
    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.node_facts
              (node_id, subtype, attrs, source, source_record_id,
               source_revision, observed_at, dedupe_key, run_id, op)
            VALUES
              (
                'node:old', 'source_file', '{}'::jsonb,
                't.laggy', '{}'::jsonb, '{}'::jsonb,
                '2026-01-01T00:00:00Z'::timestamptz,
                'lag-old', %s, 'I'
              ),
              (
                'node:new', 'source_file', '{}'::jsonb,
                't.laggy', '{}'::jsonb, '{}'::jsonb,
                '2026-01-01T00:00:01Z'::timestamptz,
                'lag-new', %s, 'I'
              )
            RETURNING fact_id
            """,
            (run_id, run_id),
        )
        fact_ids = [row[0] for row in cur.fetchall()]
        cur.execute(
            """
            INSERT INTO okg.source_watermarks
              (source, projection_cursor)
            VALUES ('t.laggy', %s)
            """,
            (min(fact_ids),),
        )

    rc, payload = _run_doctor("tlag", dsn=clean_runtime)
    assert rc == 0
    source = payload["sources"][0]
    assert source["coverage"]["status"] == "WARN"
    assert source["coverage"]["node_projection"] == {
        "cursor": min(fact_ids),
        "max_fact_id": max(fact_ids),
        "lag": max(fact_ids) - min(fact_ids),
    }
    assert source["coverage"]["warnings"] == [
        f"projection_lag:{max(fact_ids) - min(fact_ids)}",
    ]


def _admission_registry(*, dormant: bool = False) -> str:
    """A one-source registry shaped like the real one.

    `dormant=False` OMITS `enabled_by_default` entirely, which is how
    `latex-project` and 28 of okg-workspace's other 29 sources are written.
    Only an explicit `enabled_by_default: false` is dormant -- the rule
    `source_sync_entry_is_explicitly_dormant` applies and the source-sync
    scheduler uses. Writing the flag in on the non-dormant side would let a
    gate that keyed on the flag pass here while going vacuous in production.
    """
    flag = "\n            enabled_by_default: false" if dormant else ""
    return textwrap.dedent(f"""\
        sources:
          latex-project:
            module: okg.substrate.library.sources.git_graph_files
            class: GitFilesSource
            ownership_id: t.latex_project{flag}
            params:
              repo: testrepo
              repo_root: /tmp
            source_class: discovery_crawl
            record_identity_kind: scoped_locator
            record_identity_fields: [repo, path]
            source_revision_kind: content_hash
            deletion_semantics: missing_from_completed_scope
            publication_mode: published_generation
    """)


def _insert_envelope(cur, deployment: str, status: str, verdict: str) -> None:
    cur.execute(
        """
        INSERT INTO okg.admission_envelopes
          (deployment_name, producer_id, producer_kind, intent,
           admission_mode, status, idempotency_key, source, verdict)
        VALUES (%s, 't.latex_project', 'source', 'source_run',
                'fast_track', %s, %s, 't.latex_project', %s::jsonb)
        """,
        (deployment, status, str(uuid4()), verdict),
    )


#: The refusal code okg-workspace recorded in 2026-08, and the code PR #1526
#: (issue #1330) introduces for the missing-endpoint case. This doctor check
#: reports whichever code the verdict carries and recognises neither by name,
#: so it is proved against both and survives either merge order.
_REFUSAL_CODES = (
    "producer_edge_triple_not_allowed",
    "producer_edge_endpoint_missing",
)


@pytest.mark.postgres
@pytest.mark.parametrize("reason_code", _REFUSAL_CODES)
def test_source_refused_at_admission_is_a_doctor_error(
    tmp_path, patched_finder, clean_runtime, reason_code,
) -> None:
    """Issue #1323. A source refused at admission writes no watermark, no
    source-progress row and no facts, so before this the doctor reported it
    as an ordinary registered source with `source_has_no_runtime_rows` --
    indistinguishable from one that had simply not run yet."""
    name = f"tadm{_REFUSAL_CODES.index(reason_code)}"
    d = _write_deployment(
        tmp_path, name=name, registry_yaml=_admission_registry(),
    )
    patched_finder[name] = d

    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        _insert_envelope(
            cur,
            name,
            "rejected",
            json.dumps({"code": reason_code, "message": "refused"}),
        )

    rc, payload = _run_doctor(name, dsn=clean_runtime)

    assert rc == 1
    errors = [
        e for e in payload["errors"]
        if e["code"] == "source_refused_at_admission"
    ]
    assert len(errors) == 1, payload["errors"]
    assert errors[0]["reason_code"] == reason_code
    assert errors[0]["status"] == "rejected"
    source = payload["sources"][0]
    assert source["coverage"]["admission"]["status"] == "rejected"
    assert source["coverage"]["admission_authority"] == "admission_envelopes"
    assert (
        f"admission_refused:{reason_code}"
        in source["coverage"]["warnings"]
    )


@pytest.mark.postgres
def test_blocked_admission_is_not_reported_as_a_refusal(
    tmp_path, patched_finder, clean_runtime,
) -> None:
    """A blocked publish generation stamps `blocked` on every pending
    envelope in the deployment. Those payloads were admitted, so reporting
    them as refused would fail the doctor for every ingesting source."""
    d = _write_deployment(
        tmp_path, name="tblk", registry_yaml=_admission_registry(),
    )
    patched_finder["tblk"] = d

    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        _insert_envelope(cur, "tblk", "blocked", json.dumps({}))

    rc, payload = _run_doctor("tblk", dsn=clean_runtime)

    assert rc == 0
    assert [
        e for e in payload["errors"]
        if e["code"] == "source_refused_at_admission"
    ] == []
    warnings = payload["sources"][0]["coverage"]["warnings"]
    assert "source_has_no_runtime_rows" in warnings
    assert not any(w.startswith("admission_refused:") for w in warnings)
    # Reported, not silent: the admission data is in hand, so "no runtime
    # rows" alone would be the pre-change silence with the answer available.
    assert "admission_not_refused:blocked" in warnings


@pytest.mark.postgres
def test_source_with_no_envelope_at_all_says_never_admitted(
    tmp_path, patched_finder, clean_runtime,
) -> None:
    """The original #1323 premise. Absence must be named, not left silent."""
    d = _write_deployment(
        tmp_path, name="tnone", registry_yaml=_admission_registry(),
    )
    patched_finder["tnone"] = d

    rc, payload = _run_doctor("tnone", dsn=clean_runtime)

    assert rc == 0
    warnings = payload["sources"][0]["coverage"]["warnings"]
    assert "source_never_admitted" in warnings
    assert payload["sources"][0]["coverage"]["admission"] is None


@pytest.mark.postgres
def test_a_dormant_source_refusal_warns_rather_than_failing_the_doctor(
    tmp_path, patched_finder, clean_runtime,
) -> None:
    """Retiring a source is the standard remediation for a refusal, and the
    old envelope outlives the change. Failing the doctor forever for a source
    the operator deliberately turned off leaves no action but deleting
    envelope rows. Dormant means an EXPLICIT `enabled_by_default: false`; the
    test above proves the absent flag is still a finding."""
    d = _write_deployment(
        tmp_path, name="tdis", registry_yaml=_admission_registry(dormant=True),
    )
    patched_finder["tdis"] = d

    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        _insert_envelope(
            cur,
            "tdis",
            "rejected",
            json.dumps({"code": "producer_edge_triple_not_allowed"}),
        )

    rc, payload = _run_doctor("tdis", dsn=clean_runtime)

    assert rc == 0
    assert [
        e for e in payload["errors"]
        if e["code"] == "source_refused_at_admission"
    ] == []
    warnings = payload["sources"][0]["coverage"]["warnings"]
    # Still reported -- warned, not silent.
    assert (
        "admission_refused:producer_edge_triple_not_allowed" in warnings
    )
    assert "admission_refused_while_dormant" in warnings


@pytest.mark.postgres
def test_a_system_producers_envelope_is_not_attributed_to_the_source(
    tmp_path, patched_finder, clean_runtime,
) -> None:
    """`entity_merge` and `source_retirement` stamp a registry source's
    ownership id in `source` while owning `producer_kind: "system"`. Reading
    those as the source's own refusal would fail the doctor for every source a
    failed merge batch touched."""
    d = _write_deployment(
        tmp_path, name="tsys", registry_yaml=_admission_registry(),
    )
    patched_finder["tsys"] = d

    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.admission_envelopes
              (deployment_name, producer_id, producer_kind, intent,
               admission_mode, status, idempotency_key, source, verdict)
            VALUES ('tsys', 'system:entity_merge', 'system', 'source_run',
                    'fast_track', 'rejected', %s, 't.latex_project',
                    '{}'::jsonb)
            """,
            (str(uuid4()),),
        )

    rc, payload = _run_doctor("tsys", dsn=clean_runtime)

    assert rc == 0
    assert [
        e for e in payload["errors"]
        if e["code"] == "source_refused_at_admission"
    ] == []
    warnings = payload["sources"][0]["coverage"]["warnings"]
    assert not any(w.startswith("admission_refused") for w in warnings)
    assert "source_never_admitted" in warnings


@pytest.mark.parametrize("contract", [None, "strict_v1"])
def test_sources_check_injects_runtime_deployment_identity(
    tmp_path, patched_finder, contract,
):
    """`--check sources` constructs adapters with the worker's params.

    The registry authors no ``deployment`` (the runtime owns it since
    #2150). Before the fix doctor passed raw registry params and the
    adapter reported a missing ``deployment`` argument.
    """
    import yaml
    document: dict = {
        "sources": {
            "profile-measurements": {
                "module": (
                    "okg.substrate.library.sources.profile_measurements"
                ),
                "class": "ProfileMeasurementSource",
                "ownership_id": "tident.profile-measurements",
                "source_class": "discovery_crawl",
                "record_identity_kind": "scoped_locator",
                "record_identity_fields": ["path"],
                "source_revision_kind": "content_hash",
                "deletion_semantics": "missing_from_completed_scope",
                "publication_mode": "published_generation",
                "params": {"repo": "tident", "repo_root": str(tmp_path)},
            },
        },
    }
    if contract is not None:
        document["admission_contract"] = contract
    d = _write_deployment(
        tmp_path, name="tident", registry_yaml=yaml.safe_dump(document),
    )
    patched_finder["tident"] = d

    rc, payload = _run_doctor("tident")
    (source,) = payload["sources"]
    assert source["status"] == "OK", source
    assert rc == 0, payload


def _doc_corpus_registry(params: dict, *, strict: bool) -> str:
    import yaml
    document: dict = {
        "sources": {
            "repo_docs": {
                "module": "okg.substrate.library.sources.doc_corpus",
                "class": "DocCorpusSource",
                "ownership_id": "tparams.repo_docs",
                "source_class": "discovery_crawl",
                "record_identity_kind": "scoped_locator",
                "record_identity_fields": ["repo", "path"],
                "source_revision_kind": "content_hash",
                "deletion_semantics": "missing_from_completed_scope",
                "publication_mode": "published_generation",
                "params": params,
            },
        },
    }
    if strict:
        document["admission_contract"] = "strict_v1"
    return yaml.safe_dump(document)


_DOC_PARAMS = {"repo": "tparams", "repo_root": "/tmp", "extractors_dir": "/tmp"}


@pytest.mark.parametrize("strict", [False, True])
@pytest.mark.parametrize(
    ("params", "code", "key"),
    [
        (
            {**_DOC_PARAMS, "exclude_glob": ["x"]},
            "source_param_unconsumed",
            "exclude_glob",
        ),
        (
            {k: v for k, v in _DOC_PARAMS.items() if k != "repo_root"},
            "source_param_missing",
            "repo_root",
        ),
    ],
)
def test_a_bad_params_key_fails_the_sources_check(
    tmp_path, patched_finder, strict, params, code, key,
):
    """#2484 rows 1 and 2: doctor used to answer ok (a constructor
    TypeError was a WARN). It now runs the comparison ingest runs."""
    name = f"tparams{int(strict)}{key}"
    patched_finder[name] = _write_deployment(
        tmp_path, name=name,
        registry_yaml=_doc_corpus_registry(params, strict=strict),
    )

    rc, payload = _run_doctor(name)

    assert rc == 1
    assert payload["ok"] is False
    (source,) = payload["sources"]
    assert source["status"] == "FAIL"
    assert source["error"].startswith(
        f"{code}: source 'repo_docs' at /sources/repo_docs/params/{key}: "
    )
    (error,) = payload["errors"]
    assert (error["name"], error["code"], error["key"]) == (
        "repo_docs", code, key,
    )


def test_a_legacy_reserved_key_warns_and_a_strict_one_fails(
    tmp_path, patched_finder,
):
    """Legacy ingest passes a reserved key through, so doctor warns; strict
    ingest refuses it, so doctor fails."""
    params = {**_DOC_PARAMS, "source_name": "tparams.repo_docs"}
    for strict in (False, True):
        name = f"treserved{int(strict)}"
        patched_finder[name] = _write_deployment(
            tmp_path, name=name,
            registry_yaml=_doc_corpus_registry(params, strict=strict),
        )
        rc, payload = _run_doctor(name)
        (source,) = payload["sources"]
        if strict:
            assert (rc, source["status"]) == (1, "FAIL"), source
        else:
            assert (rc, source["status"]) == (0, "WARN"), source
            assert "source_param_reserved" in source["warning"]
            assert "add-source" in source["warning"]


def _custom_reader_deployment(
    tmp_path, patched_finder, *, name: str, reader: str, params: dict,
    strict: bool,
):
    import yaml
    d = _write_deployment(tmp_path, name=name, registry_yaml="sources: {}\n")
    (d / f"{name}_reader.py").write_text(textwrap.dedent(reader))
    document: dict = {"sources": {"r": {
        "module": f"{name}_reader",
        "class": "Reader",
        "ownership_id": f"{name}.r",
        "source_class": "discovery_crawl",
        "record_identity_kind": "scoped_locator",
        "record_identity_fields": ["path"],
        "source_revision_kind": "content_hash",
        "deletion_semantics": "missing_from_completed_scope",
        "publication_mode": "published_generation",
        "params": params,
    }}}
    if strict:
        document["admission_contract"] = "strict_v1"
    (d / "source_registry.yaml").write_text(yaml.safe_dump(document))
    patched_finder[name] = d
    return d


def test_a_legacy_kwargs_key_warns(tmp_path, patched_finder):
    _custom_reader_deployment(
        tmp_path, patched_finder, name="tdockw", strict=False,
        params={"seed": 7},
        reader="""
            class Reader:
                profile = "discovery_crawl"
                def __init__(self, **params):
                    self.params = params
        """,
    )
    rc, payload = _run_doctor("tdockw")
    (source,) = payload["sources"]
    assert (rc, source["status"]) == (0, "WARN"), source
    assert source["warning"].startswith(
        "source_param_unconsumed: source 'r' at /sources/r/params/seed"
    )


def test_a_strict_registry_whose_arguments_do_not_bind_fails(
    tmp_path, patched_finder,
):
    """A reader requiring a substrate-owned name the substrate does not
    inject: the shared check leaves it to the bind check, which strict
    ingest refuses, so doctor fails."""
    _custom_reader_deployment(
        tmp_path, patched_finder, name="tdocbind", strict=True, params={},
        reader="""
            class Reader:
                profile = "discovery_crawl"
                def __init__(self, *, run_id):
                    self.run_id = run_id
        """,
    )
    rc, payload = _run_doctor("tdocbind")
    (source,) = payload["sources"]
    assert (rc, source["status"]) == (1, "FAIL"), source
    (error,) = payload["errors"]
    assert error["code"] == "source_constructor_arguments_refused"
    assert "source_constructor_binding_invalid" in error["message"]


def test_legacy_refused_arguments_warn(tmp_path, patched_finder):
    """Legacy `source_adapter_init_params` refuses a mismatched deployment;
    doctor used to raise NameError on that path (params unbound)."""
    _custom_reader_deployment(
        tmp_path, patched_finder, name="tdocdep", strict=False,
        params={"deployment": "someone-else"},
        reader="""
            class Reader:
                profile = "discovery_crawl"
                def __init__(self, *, deployment):
                    self.deployment = deployment
        """,
    )
    rc, payload = _run_doctor("tdocdep")
    (source,) = payload["sources"]
    assert (rc, source["status"]) == (0, "WARN"), source
    assert "worker-owned" in source["error"]


def test_an_absent_params_block_still_names_missing_keys(
    tmp_path, patched_finder,
):
    _custom_reader_deployment(
        tmp_path, patched_finder, name="tdocbare", strict=False, params={},
        reader="""
            class Reader:
                profile = "discovery_crawl"
                def __init__(self, *, repo_root):
                    self.repo_root = repo_root
        """,
    )
    import yaml
    registry = patched_finder["tdocbare"] / "source_registry.yaml"
    document = yaml.safe_load(registry.read_text())
    del document["sources"]["r"]["params"]
    registry.write_text(yaml.safe_dump(document))
    rc, payload = _run_doctor("tdocbare")
    (source,) = payload["sources"]
    assert (rc, source["status"]) == (1, "FAIL"), source
    assert payload["errors"][0]["code"] == "source_param_missing"


def test_doctor_hold_reports_classes_and_whether_retirement_applies(monkeypatch):
    """`okg doctor --check sources` passes the registry to the hold read and
    reports the per-source classification for a retirable and a
    non-retirable contract, without running an ingest."""
    from types import SimpleNamespace

    from okg.substrate.cli import doctor
    from okg.substrate.ingest import incomplete_recovery as ir

    base_hold = {
        "checkpoint_id": "ckpt-1",
        "blocked_at": None,
        "legacy_receipt": False,
        "deployment": "d",
        "required_selection": ["widget-snapshot", "widget-keeper"],
        "failed_sources": ["widget-snapshot"],
        "sources": [],
        "not_admitted_since_block": [],
        "escape_hatch_command": "okg runtime supersede-checkpoint --deployment d",
    }
    gone = [
        {"source": "widget-snapshot", "class": "renamed",
         "renamed_to": "widget-inventory"},
        {"source": "widget-keeper", "class": "present"},
    ]
    rebound = [
        {"source": "widget-snapshot", "class": "rebound",
         "changed_fields": ["catalog_fingerprint"]},
        {"source": "widget-keeper", "class": "present"},
    ]
    seen = {}

    def run(classes, applies):
        def hold(**kwargs):
            seen.update(kwargs)
            return {
                **base_hold,
                "source_classes": classes,
                "automatic_retirement_applies": applies,
            }

        monkeypatch.setattr(ir, "source_sync_hold", hold)
        entry = SimpleNamespace(ownership_id="d.keeper", legacy=False)
        report = {"ok": True, "errors": [], "runtime_available": True}
        doctor._add_source_sync_hold(
            report,
            dsn="postgresql://x/y",
            deployment="d",
            registry={"widget-keeper": entry},
        )
        [error] = report["errors"]
        return error

    retirable = run(gone, True)
    assert seen["registry"] == {"widget-keeper": seen["registry"]["widget-keeper"]}
    assert retirable["code"] == "source_sync_held"
    assert retirable["automatic_retirement_applies"] is True
    assert [row["class"] for row in retirable["source_classes"]] == [
        "renamed", "present",
    ]
    assert "widget-snapshot was renamed to widget-inventory" in retirable["error"]
    assert "migrate --deployment d --apply" in retirable["error"]

    kept = run(rebound, False)
    assert kept["automatic_retirement_applies"] is False
    assert "catalog_fingerprint changed" in kept["error"]
    assert "migrate --deployment" not in kept["error"]
