"""Connector conformance kit (#1181 slice 7).

The kit is a checker, so the load-bearing test is not "the good connector
passes" — it is that each check *fails* on a connector broken in exactly
the way that check exists to catch. `test_each_check_fails_on_its_own_break`
is that table: one deliberately broken variant per check, asserted to fail
that check while the neutral fixture passes it.

The fixture connector is in-tree and domain-neutral, written against the
public `okg.deployment` surface only, in the same shape as the packaged
connector #1773's hermetic arm drives through the real `SourceRunner`.
"""
from __future__ import annotations

import dataclasses
import json
import os
import pathlib
import re
import subprocess
import sys

import pytest

import okg.deployment as sdk
import okg.deployment.conformance as conformance
from okg.substrate.errors import ConnectorConformanceCaseInvalid
from tests.pytest.child_pythonpath import child_pythonpath


@pytest.fixture
def runnable_reader():
    """Authoring tests; the separate installed CLI receipt proves packaging.

    Load the concrete reader without changing the interpreter import path.
    This deliberately makes no installed-wheel or publication claim.
    """
    import importlib.util

    path = pathlib.Path(__file__).resolve().parents[3] / (
        "examples/json-record-connector/src/okg_example_records/reader.py"
    )
    spec = importlib.util.spec_from_file_location("runnable_reader_under_test", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _node_ids(facts):
    """The node facts' ids. The example also emits containment edge facts."""
    return [fact.node_id for fact in facts if hasattr(fact, "node_id")]


def _example_input(tmp_path, records, *, complete=True):
    path = tmp_path / "records.json"
    path.write_text(json.dumps({"complete": complete, "records": records}))
    return path


def test_runnable_reader_edits_retries_and_complete_scope(runnable_reader, tmp_path):
    first = {"id": "A", "kind": "record", "text": "original"}
    second = {"id": "B", "kind": "record", "text": "retained"}
    path = _example_input(tmp_path, [first, second])
    adapter = runnable_reader.RecordSource(str(path))
    initial = adapter.run("first")
    assert set(_node_ids(initial.facts)) == {
        "example-record:A", "example-record:B", "example-collection:snapshot",
    }
    assert {(edge.src, edge.dst) for edge in initial.facts if hasattr(edge, "edge_type")} == {
        ("example-collection:snapshot", "example-record:A"),
        ("example-collection:snapshot", "example-record:B"),
    }
    assert set(initial.next_cursor["revisions"]) == {"A", "B"}
    retry = adapter.run("retry", cursor=initial.next_cursor)
    assert retry.facts == []
    assert retry.record_set == initial.record_set  # No-change never means empty authority.
    edited = dict(first, text="edited")
    _example_input(tmp_path, [edited, second])
    changed = adapter.run("edit", cursor=initial.next_cursor)
    # The edited record, its containment edge, and the snapshot whose
    # membership revision the edit moved.
    assert _node_ids(changed.facts) == [
        "example-record:A", "example-collection:snapshot",
    ]
    assert changed.facts[0].source_revision != initial.facts[0].source_revision
    _example_input(tmp_path, [edited])
    deleted = adapter.run("delete", cursor=changed.next_cursor)
    # No record fact: the runner synthesizes B's retraction from the full
    # footprint. The snapshot's own membership revision did move, so it is
    # restated -- that is what makes the removal visible in the graph.
    assert _node_ids(deleted.facts) == ["example-collection:snapshot"]
    assert deleted.completed_scope
    assert set(deleted.record_set.records) == {"A", "example-collection"}
    assert set(deleted.next_cursor["revisions"]) == {"A"}


def test_runnable_reader_partial_retains_checkpoint_and_retries(runnable_reader, tmp_path):
    first = {"id": "A", "kind": "record", "text": "original"}
    second = {"id": "B", "kind": "record", "text": "retained"}
    path = _example_input(tmp_path, [first, second])
    adapter = runnable_reader.RecordSource(str(path))
    initial = adapter.run("first")
    edited = dict(first, text="edited")
    _example_input(tmp_path, [edited], complete=False)
    partial = adapter.run("partial", cursor=initial.next_cursor)
    assert partial.health.status == "partial"
    assert not partial.completed_scope
    assert partial.record_set is None
    assert partial.next_cursor == initial.next_cursor
    _example_input(tmp_path, [edited, second])
    recovered = adapter.run("recovered", cursor=partial.next_cursor)
    assert recovered.completed_scope
    assert set(recovered.record_set.records) == {"A", "B", "example-collection"}
    assert _node_ids(recovered.facts) == [
        "example-record:A", "example-collection:snapshot",
    ]
    assert adapter.run("retry", cursor=recovered.next_cursor).facts == []


def test_runnable_reader_rejecting_an_owned_id_withholds_deletion(runnable_reader, tmp_path):
    row = {"id": "A", "kind": "record", "text": "original"}
    path = _example_input(tmp_path, [row])
    adapter = runnable_reader.RecordSource(str(path))
    initial = adapter.run("first")
    _example_input(tmp_path, [dict(row, kind="outside")])
    rejected = adapter.run("rejected", cursor=initial.next_cursor)
    assert rejected.health.status == "partial"
    assert rejected.record_set is None
    assert rejected.next_cursor == initial.next_cursor
    assert adapter.last_run_metrics["connector.rejected"] == 1


def test_runnable_reader_cancellation_keeps_prior_authority(runnable_reader, tmp_path):
    path = _example_input(tmp_path, [{"id": "A", "kind": "record", "text": "original"}])
    initial = runnable_reader.RecordSource(str(path)).run("first")
    stopped = sdk.ConnectorAdapter(
        runnable_reader.RecordReader(), config={"path": str(path)}, cancel=lambda: True,
    ).run("cancelled", cursor=initial.next_cursor)
    assert stopped.next_cursor == initial.next_cursor
    assert stopped.health.status == "partial"
    assert not stopped.completed_scope
    assert stopped.record_set is None
    assert stopped.facts == []


@pytest.mark.parametrize("cancel_at_poll", [1, 2])
def test_runnable_reader_cancelled_empty_snapshot_never_deletes(
    runnable_reader, tmp_path, cancel_at_poll,
):
    """A cancelled empty complete scan must not retract the prior populated scope."""
    path = _example_input(tmp_path, [{"id": "A", "kind": "record", "text": "keep"}])
    initial = runnable_reader.RecordSource(str(path)).run("first")
    _example_input(tmp_path, [])
    polls = 0

    def cancelled():
        nonlocal polls
        polls += 1
        return polls >= cancel_at_poll

    stopped = sdk.ConnectorAdapter(
        runnable_reader.RecordReader(), config={"path": str(path)}, cancel=cancelled,
    ).run("cancelled-empty", cursor=initial.next_cursor)
    assert stopped.next_cursor == initial.next_cursor
    assert stopped.health.status == "partial"
    assert not stopped.completed_scope
    assert stopped.record_set is None
    # The same empty snapshot without cancellation is valid deletion authority.
    completed = runnable_reader.RecordSource(str(path)).run("empty", cursor=initial.next_cursor)
    assert completed.completed_scope
    # An empty snapshot still speaks for itself: the collection remains, and
    # every record is absent from it, which is the deletion authority.
    assert set(completed.record_set.records) == {"example-collection"}
    assert completed.next_cursor["revisions"] == {}


#: Each case is a whole snapshot payload, written verbatim, paired with the
#: refusal it must produce. The payload is raw rather than built from rows
#: because the `complete` flag is itself under test: a snapshot that never
#: claims completeness must not reach the reader's deletion authority, and a
#: parametrisation that always supplies a real boolean cannot see that.
#: Asserting on the message, not on bare ValueError, keeps one refusal from
#: standing in for another when the validation order changes.
@pytest.mark.parametrize("payload, message", [
    ({"complete": True,
      "records": [{"id": "A", "kind": "record", "text": "one"},
                  {"id": "A", "kind": "record", "text": "two"}]},
     "duplicate upstream record id"),
    ({"complete": True, "records": [{"id": "", "kind": "record", "text": "invalid"}]},
     "every record requires a nonempty string id"),
    ({"complete": True, "records": [{"id": 3, "kind": "record", "text": "invalid"}]},
     "every record requires a nonempty string id"),
    ({"complete": True, "records": [{"id": "A", "kind": "record", "text": 3}]},
     "record text must be a string"),
    # No `complete` key at all. Defaulting it to True would hand a snapshot
    # that never claimed completeness the authority to retract records.
    ({"records": [{"id": "A", "kind": "record", "text": "one"}]},
     "snapshot requires an explicit boolean complete field"),
    # Truthy but not a boolean: a JSON string is not the completeness contract.
    ({"complete": "true", "records": [{"id": "A", "kind": "record", "text": "one"}]},
     "snapshot requires an explicit boolean complete field"),
    ({"complete": True, "records": {"A": "not a list"}},
     "snapshot records must be a list"),
])
def test_runnable_reader_refuses_ambiguous_snapshot(runnable_reader, tmp_path, payload, message):
    path = tmp_path / "records.json"
    path.write_text(json.dumps(payload))
    with pytest.raises(ValueError, match=re.escape(message)):
        runnable_reader.RecordSource(str(path)).run("invalid")


def test_runnable_reader_change_probe_follows_the_input_file(runnable_reader, tmp_path):
    """`okg deployment add-source` refuses a reader without a change probe."""
    assert runnable_reader.RecordSource.change_probe_kind == "content_hash"
    path = _example_input(tmp_path, [{"id": "A", "kind": "record", "text": "one"}])
    adapter = runnable_reader.RecordSource(str(path))
    first = adapter.change_probe.probe(cursor=None, sync_scope=None)
    same = adapter.change_probe.probe(cursor={"change_probe_token": first.token}, sync_scope=None)
    assert first.changed
    assert first.token == same.token
    _example_input(tmp_path, [{"id": "A", "kind": "record", "text": "edited"}])
    edited = adapter.change_probe.probe(cursor={"change_probe_token": first.token}, sync_scope=None)
    assert edited.changed
    assert edited.token != first.token


def test_runnable_reader_passes_the_public_kit(runnable_reader, tmp_path, monkeypatch):
    rows = [{"id": "A", "kind": "record", "text": "one"},
            {"id": "B", "kind": "record", "text": "two"},
            {"id": "excluded", "kind": "outside", "text": "rejected"}]
    path = _example_input(tmp_path, rows)
    # The `change_probe` check imports the registered adapter class. The
    # example is loaded from its file, not installed, so name that module.
    monkeypatch.setitem(sys.modules, runnable_reader.__name__, runnable_reader)
    registration = dataclasses.replace(
        runnable_reader.REGISTRATION, module=runnable_reader.__name__,
    )
    case = conformance.ConnectorCase(
        connector=runnable_reader.RecordReader(),
        registration=registration,
        dataset={row["id"]: row for row in rows}, config={"path": str(path)},
        expected_rejections=("excluded",),
    )
    drives = conformance.execute(case)
    assert {name: reason for name in conformance.CHECKS
            if (reason := conformance.run_check(case, drives, name)) is not None} == {}


#: Three deterministic records. `skip-1` lies outside the declared
#: output_signature, so a conforming connector must reject it visibly.
DATASET = {"doc-1": "alpha", "doc-2": "beta", "skip-1": "outside"}

CREDENTIAL_REF = "FIXTURE_CONNECTOR_REF"
#: The resolved credential the kit hunts for. Opaque on purpose: it must
#: never reach a fact, a health reason, a warning or a rejected item.
CREDENTIAL_VALUE = "opaque-fixture-string-0001"


class _LooseHealth:
    """A health object the SDK did not build.

    `SourceHealth` refuses an unknown status at construction and
    `ConnectorRun` refuses `partial` beside `completed_scope`, so the only
    way a connector reaches the runner with an out-of-vocabulary status is
    to hand back an object of its own. `ConnectorRun.health` is untyped, so
    this is reachable — and it is the only break the health check can
    actually catch.
    """

    def __init__(self, status: str) -> None:
        self.status = status
        self.reason = None


class FixtureConnector:
    """A conforming connector, plus one flag per deliberate break."""

    name = "conformance-fixture"
    profile = "reference_catalog"

    def __init__(
        self,
        *,
        nondeterministic: bool = False,
        ignore_checkpoint: bool = False,
        ignore_cancellation: bool = False,
        bad_health: bool = False,
        leak_secret: bool = False,
        undeclared_subtype: bool = False,
        skip_credential: bool = False,
        read_since_progress: bool = False,
        drop_silently: bool = False,
    ) -> None:
        self.flags = dict(
            nondeterministic=nondeterministic,
            ignore_checkpoint=ignore_checkpoint,
            ignore_cancellation=ignore_cancellation,
            bad_health=bad_health,
            leak_secret=leak_secret,
            undeclared_subtype=undeclared_subtype,
            skip_credential=skip_credential,
            read_since_progress=read_since_progress,
            drop_silently=drop_silently,
        )
        self.calls = 0

    def run(self, ctx):
        flags = self.flags
        self.calls += 1
        if not flags["skip_credential"]:
            ctx.secret_ref("token")
        if flags["read_since_progress"]:
            _ = ctx.since_progress
        done = set()
        if not flags["ignore_checkpoint"]:
            done = set((ctx.checkpoint.next_cursor or {}).get("done") or ())
        facts = []
        rejected = []
        warnings = []
        for key in sorted(DATASET):
            if not flags["ignore_cancellation"] and ctx.cancelled():
                raise sdk.Cancelled("stopped on request")
            if key in done:
                continue
            done.add(key)
            if key.startswith("skip-"):
                if not flags["drop_silently"]:
                    rejected.append(
                        sdk.RejectedItem(
                            record_id=key,
                            reason="outside the declared output_signature",
                        )
                    )
                continue
            attrs = {"body": DATASET[key]}
            if flags["nondeterministic"]:
                attrs["attempt"] = self.calls
            facts.append(
                sdk.NodeFact(
                    node_id=key,
                    subtype="Undeclared" if flags["undeclared_subtype"] else "Doc",
                    attrs=attrs,
                    source_record_id={"id": key},
                    source_revision={"rev": "1"},
                )
            )
        if flags["leak_secret"]:
            warnings.append(
                sdk.ConnectorWarning(
                    code="credential_echo",
                    message=f"resolved {CREDENTIAL_VALUE}",
                )
            )
        health = None
        if flags["bad_health"]:
            health = _LooseHealth("totally-fine")
        return sdk.ConnectorRun(
            facts=facts,
            completed_scope=True,
            next_checkpoint=sdk.Checkpoint(next_cursor={"done": sorted(done)}),
            health=health,
            warnings=tuple(warnings),
            rejected=tuple(rejected),
        )


class FixtureSource(sdk.ConnectorAdapter):
    """The adapter the fixture registration names, with a change probe.

    `okg deployment add-source` constructs the registered class and refuses
    one without a sound `change_probe_kind` and a runtime
    `self.change_probe`; the kit's `change_probe` check holds a connector to
    the same rule (#2478).
    """

    profile = "reference_catalog"
    change_probe_kind = "content_hash"

    def __init__(self):
        super().__init__(FixtureConnector(), config={})
        self.change_probe = sdk.ContentHashProbe(
            content_items=lambda: [
                ("dataset", json.dumps(DATASET, sort_keys=True).encode()),
            ],
            config={}, emit_targets=(FixtureConnector.run,),
        )


class DeclaredOnlyProbeSource(FixtureSource):
    """Declares a probe kind but never sets the runtime probe."""

    def __init__(self):
        super().__init__()
        self.change_probe = None


class KwargsProbeSource(sdk.ConnectorAdapter):
    """A `**kwargs` constructor: it must receive the whole case config."""

    profile = "reference_catalog"
    change_probe_kind = "content_hash"

    def __init__(self, **config):
        if config != {"path": "/fixture/input.json"}:
            raise TypeError(f"expected the whole config, got {config!r}")
        super().__init__(FixtureConnector(), config=config)
        self.change_probe = sdk.ContentHashProbe(
            content_items=lambda: [("config", b"fixture")],
            config=config, emit_targets=(FixtureConnector.run,),
        )


class UnsoundProbeSource(FixtureSource):
    """Declares a stat-digest probe, which add-source refuses as unsound."""

    change_probe_kind = "mtime"


def _registration(**overrides):
    kwargs = dict(
        connector_type="conformance_fixture",
        version="1.0.0",
        module="tests.substrate.runtime.test_connector_conformance",
        class_name="FixtureSource",
        ownership_id="okg.conformance_fixture",
        source_class="reference_catalog",
        trust_label="operator_curated",
        sdk_range=sdk.SdkVersionRange("1.0.0", "2.0.0"),
        auth_modes=("env_credential",),
        capabilities=("cancellation", "checkpoint_resume"),
        output_signature={"nodes": [{"subtype": "Doc"}]},
        credential_refs=(CREDENTIAL_REF,),
        description="in-tree conformance fixture",
    )
    kwargs.update(overrides)
    return sdk.ConnectorRegistration(**kwargs)


def _case(connector=None, **overrides):
    kwargs = dict(
        connector=connector or FixtureConnector(),
        registration=_registration(),
        dataset=DATASET,
        secret_refs={"token": CREDENTIAL_REF},
        secret_values=(CREDENTIAL_VALUE,),
        expected_rejections=("skip-1",),
    )
    kwargs.update(overrides)
    return conformance.ConnectorCase(**kwargs)


class ExplodingConnector(FixtureConnector):
    """Raises on every run, so the shared drives fixture errors and not one
    check records a result. The receipt must still read `fail`."""

    def run(self, ctx):
        raise RuntimeError("upstream exploded")


def _case_raises():
    return _case(connector=ExplodingConnector())


@pytest.fixture(scope="module")
def good_case():
    return _case()


@pytest.fixture(scope="module")
def good_drives(good_case):
    return conformance.execute(good_case)


# --- (a) the neutral fixture conforms ----------------------------------------


def test_every_check_passes_on_the_fixture_connector(good_case, good_drives):
    failures = {
        name: conformance.run_check(good_case, good_drives, name)
        for name in conformance.CHECKS
    }
    assert {k: v for k, v in failures.items() if v is not None} == {}


def test_the_kit_runs_one_check_per_declared_area():
    """Every check names an area, and the nine-area map stays complete."""
    assert len(conformance.CHECKS) == 11
    areas = {area for area, _ in conformance.CHECKS.values()}
    assert "sdk surface" in areas
    assert len(conformance.COVERAGE) == 9
    marks = {mark for _, mark, _ in conformance.COVERAGE}
    assert marks <= {"exercised", "partly exercised", "documented only", "out of scope"}


# --- (b) each check fails on its own break -----------------------------------
#
# A check that cannot fail proves nothing. Each row breaks the fixture in
# exactly one way and names the check that must catch it.

BREAKS = [
    ("determinism", dict(nondeterministic=True), {}),
    ("checkpoint_resume", dict(ignore_checkpoint=True), {}),
    ("cancellation", dict(ignore_cancellation=True), {}),
    ("health", dict(bad_health=True), {}),
    ("redaction", dict(leak_secret=True), {}),
    ("signature_consistency", dict(undeclared_subtype=True), {}),
    ("credential_failure", dict(skip_credential=True), {}),
    ("capability_declaration", dict(read_since_progress=True), {}),
    ("narrowing_rejection", dict(drop_silently=True), {}),
    # #2478: a registered class with no change probe is what add-source
    # refuses; the plain connector class declares none.
    (
        "change_probe", {},
        {"registration": _registration(class_name="FixtureConnector")},
    ),
    (
        "change_probe", {},
        {"registration": _registration(class_name="DeclaredOnlyProbeSource")},
    ),
    (
        "change_probe", {},
        {"registration": _registration(class_name="UnsoundProbeSource")},
    ),
]


@pytest.mark.parametrize("check,flags,overrides", BREAKS)
def test_each_check_fails_on_its_own_break(check, flags, overrides):
    case = _case(connector=FixtureConnector(**flags), **overrides)
    reason = conformance.run_check(case, conformance.execute(case), check)
    assert reason is not None, f"{check} passed a connector broken for it"


def test_a_declared_capability_that_is_never_exercised_fails(good_drives):
    """The design note's rule: declaring an unimplemented optional
    capability must FAIL, not skip."""
    case = _case(
        registration=_registration(
            capabilities=("cancellation", "checkpoint_resume", "record_set_authority"),
        ),
    )
    reason = conformance.run_check(case, good_drives, "capability_declaration")
    assert reason is not None and "record_set_authority" in reason


def test_a_running_sdk_outside_the_declared_range_fails(good_drives):
    case = _case(registration=_registration(sdk_range=sdk.SdkVersionRange("0.1.0", "0.2.0")))
    assert conformance.run_check(case, good_drives, "sdk_version") is not None


def test_a_case_with_nothing_to_prove_fails_rather_than_skips(good_drives):
    """No secret values, no out-of-signature item: the kit must refuse to
    report a pass it did not earn."""
    empty = _case(secret_values=(), expected_rejections=())
    assert conformance.run_check(empty, good_drives, "redaction") is not None
    assert conformance.run_check(empty, good_drives, "narrowing_rejection") is not None


def test_a_raising_connector_is_reported_not_propagated(good_case):
    class Exploding(FixtureConnector):
        def run(self, ctx):
            raise RuntimeError("upstream exploded")

    case = _case(connector=Exploding())
    drives_error = None
    try:
        conformance.execute(case)
    except RuntimeError as exc:
        drives_error = exc
    assert drives_error is not None
    reason = conformance.run_check(good_case, _BrokenDrives(), "health")
    assert reason is not None and "raised" in reason


class _BrokenDrives:
    """Stands in for drives whose accessors raise, to prove `run_check`
    converts an exception into a named failure instead of aborting."""

    def runs(self):
        raise ValueError("no runs")


# --- (c) the receipt ---------------------------------------------------------


def test_the_receipt_names_the_subject_the_checks_and_the_dataset(
    good_case, good_drives,
):
    results = {
        name: conformance.run_check(good_case, good_drives, name)
        for name in conformance.CHECKS
    }
    document = conformance.receipt(good_case, results)

    assert document["schema_version"] == conformance.RECEIPT_SCHEMA
    assert document["verdict"] == "pass"
    assert document["harness"] == {
        "harness_protocol": conformance.HARNESS_PROTOCOL,
        "package_version": sdk.__version__,
    }
    assert document["subject"]["connector_type"] == "conformance_fixture"
    assert document["subject"]["connector_version"] == "1.0.0"
    assert document["subject"]["sdk_version"] == sdk.__version__
    assert document["inputs"]["item_count"] == len(DATASET)
    assert set(document["inputs"]["item_digests"]) == set(DATASET)
    assert document["inputs"]["dataset_digest"].startswith("sha256:")
    assert document["receipt_digest"].startswith("sha256:")
    assert len(document["checks"]) == len(conformance.CHECKS)
    assert all(entry["status"] == "pass" for entry in document["checks"])
    assert all(entry["diagnostic_codes"] == [] for entry in document["checks"])
    assert len(document["coverage"]) == 9
    lineage = [c for c in document["coverage"] if c["area"] == "lineage"][0]
    assert lineage["coverage"] == "documented only"
    assert "publisher" in lineage["note"]


def test_a_failing_check_reaches_the_receipt_verdict(good_case, good_drives):
    results = {name: None for name in conformance.CHECKS}
    results["determinism"] = "diverged"
    document = conformance.receipt(good_case, results)
    assert document["verdict"] == "fail"
    entry = [c for c in document["checks"] if c["check"] == "determinism"][0]
    assert entry["status"] == "fail"
    assert entry["diagnostic_codes"] == ["connector_conformance_determinism_failed"]
    assert entry["detail"] == "diverged"


def test_a_check_that_never_ran_is_failed_not_omitted(good_case):
    """The load-bearing one. A connector that raises on every run records
    no result at all; building the receipt from what was recorded made that
    read `{"verdict": "pass", "checks": []}`."""
    document = conformance.receipt(good_case, {}, session_failed=True)
    assert document["verdict"] == "fail"
    assert len(document["checks"]) == len(conformance.CHECKS) == 11
    assert all(entry["status"] == "fail" for entry in document["checks"])
    assert all(
        entry["detail"] == conformance.DID_NOT_RUN for entry in document["checks"]
    )
    assert {c["check"] for c in document["checks"]} == set(conformance.CHECKS)


def test_a_nonzero_exit_fails_a_receipt_whose_checks_all_passed(good_case):
    """Every check that ran passed, but the session did not. Collection
    errors and fixture errors both land here."""
    passing = {name: None for name in conformance.CHECKS}
    assert conformance.receipt(good_case, passing)["verdict"] == "pass"
    failed = conformance.receipt(good_case, passing, session_failed=True)
    assert failed["verdict"] == "fail"


def test_the_receipt_digest_covers_the_document(good_case):
    passing = {name: None for name in conformance.CHECKS}
    first = conformance.receipt(good_case, passing)
    second = conformance.receipt(good_case, {**passing, "determinism": "diverged"})
    assert first["receipt_digest"] != second["receipt_digest"]
    assert conformance.receipt(good_case, passing) == first


# --- (d) the harness surface -------------------------------------------------

CASE_REFERENCE = "tests.substrate.runtime.test_connector_conformance:_case"


def test_load_case_resolves_a_factory_and_a_value():
    assert isinstance(conformance.load_case(CASE_REFERENCE), conformance.ConnectorCase)


@pytest.mark.parametrize(
    "reference,reason",
    [
        ("no-colon-here", "missing ':'"),
        ("okg.deployment:NotAName", "does not resolve"),
        ("okg.deployment:__version__", "is not a ConnectorCase"),
    ],
)
def test_load_case_refuses_a_bad_reference(reference, reason):
    with pytest.raises(ConnectorConformanceCaseInvalid) as caught:
        conformance.load_case(reference)
    assert caught.value.code == "connector_conformance_case_invalid"
    assert set(caught.value.data) == {"reference", "reason"}


def test_an_unknown_check_is_refused_with_the_registered_code(good_case, good_drives):
    with pytest.raises(ConnectorConformanceCaseInvalid):
        conformance.run_check(good_case, good_drives, "not-a-check")


def test_the_plugin_declares_both_options():
    seen = {}

    class _Group:
        def addoption(self, name, **kwargs):
            seen[name] = kwargs

    class _Parser:
        def getgroup(self, name):
            assert name == "okg-conformance"
            return _Group()

    conformance.pytest_addoption(_Parser())
    assert set(seen) == {"--okg-connector", "--okg-conformance-receipt"}


def test_one_test_per_check_registered_exactly_once():
    """The parametrization is a decorator on the test, not a
    `pytest_generate_tests` hook. Under the documented command the module is
    BOTH the registered plugin and the collected test module, so a hook runs
    twice and pytest refuses the session with
    `duplicate parametrization of 'okg_conformance_check'`. A decorator is
    attached to the function once, however many times the module loads."""
    assert not hasattr(conformance, "pytest_generate_tests")
    marks = [
        mark
        for mark in conformance.test_connector_conformance.pytestmark
        if mark.name == "parametrize"
    ]
    assert len(marks) == 1
    assert marks[0].args == ("okg_conformance_check", sorted(conformance.CHECKS))


def test_the_session_hook_writes_the_receipt(tmp_path, good_case, monkeypatch):
    target = tmp_path / "receipt.json"

    class _Config:
        def getoption(self, name, default=None):
            return str(target) if name == "--okg-conformance-receipt" else default

    class _Session:
        config = _Config()

    passing = {name: None for name in conformance.CHECKS}
    monkeypatch.setitem(conformance._SESSION, "case", good_case)
    monkeypatch.setitem(conformance._SESSION, "results", passing)
    conformance.pytest_sessionfinish(_Session(), 0)
    written = json.loads(target.read_text(encoding="utf-8"))
    assert written["schema_version"] == conformance.RECEIPT_SCHEMA
    assert written["verdict"] == "pass"

    monkeypatch.setitem(conformance._SESSION, "results", {})
    conformance.pytest_sessionfinish(_Session(), 1)
    written = json.loads(target.read_text(encoding="utf-8"))
    assert written["verdict"] == "fail"
    assert len(written["checks"]) == len(conformance.CHECKS)


def test_no_receipt_is_written_without_a_path(tmp_path, monkeypatch):
    class _Config:
        def getoption(self, name, default=None):
            return default

    class _Session:
        config = _Config()

    monkeypatch.setitem(conformance._SESSION, "case", None)
    conformance.pytest_sessionfinish(_Session(), 0)
    assert list(tmp_path.iterdir()) == []


# --- (d2) the documented command, end to end ---------------------------------
#
# The two defects review found both lived here: the module is registered as a
# plugin AND collected as the test module, and the receipt was built from what
# happened to be recorded. Calling the hooks directly could not see either.
# These two runs are the only tests that execute what an external repository is
# actually told to type.

RAISING_CASE_REFERENCE = "tests.substrate.runtime.test_connector_conformance:_case_raises"


def _documented_command(tmp_path, reference):
    """Run the published invocation in a subprocess, from an empty directory.

    `cwd` is a scratch directory so this repository's own pytest configuration
    does not apply, exactly as it would not in an external repository. The
    child runs this same interpreter, so it resolves the same installed
    modules; `PYTHONPATH` adds only the repo roots (see
    `tests.pytest.child_pythonpath` for why it never copies `sys.path`).
    """
    receipt_path = tmp_path / "receipt.json"
    environment = dict(os.environ)
    environment["PYTHONPATH"] = child_pythonpath()
    completed = subprocess.run(
        [
            sys.executable, "-m", "pytest",
            "-p", "okg.deployment.conformance",
            "--pyargs", "okg.deployment.conformance",
            "--okg-connector", reference,
            "--okg-conformance-receipt", str(receipt_path),
            "-p", "no:cacheprovider",
            "-q",
        ],
        cwd=str(tmp_path),
        env=environment,
        capture_output=True,
        text=True,
        timeout=600,
    )
    return completed, receipt_path


def test_the_documented_command_certifies_the_fixture_connector(tmp_path):
    completed, receipt_path = _documented_command(tmp_path, CASE_REFERENCE)
    output = completed.stdout + completed.stderr

    assert "duplicate parametrization" not in output, output
    assert completed.returncode == 0, output
    assert "11 passed" in output, output

    document = json.loads(receipt_path.read_text(encoding="utf-8"))
    assert document["verdict"] == "pass"
    assert len(document["checks"]) == len(conformance.CHECKS)
    assert all(entry["status"] == "pass" for entry in document["checks"])

    in_process = conformance.receipt(
        _case(), {name: None for name in conformance.CHECKS},
    )
    assert document["receipt_digest"] == in_process["receipt_digest"]
    assert document == in_process


def test_the_case_fixture_cannot_be_overridden_from_outside(tmp_path):
    """`--okg-connector` is the only way in, and the refusal says so.

    A fixture defined in the collected module shadows one from a
    `conftest.py` or a `-p` plugin, so the alternative this hint used to
    offer could not work under the documented command.
    """
    plugin = tmp_path / "case_override_plugin.py"
    plugin.write_text(
        "import pytest\n"
        "import tests.substrate.runtime.test_connector_conformance as fixture\n"
        "@pytest.fixture(scope='session')\n"
        "def okg_connector_case():\n"
        "    return fixture._case()\n",
        encoding="utf-8",
    )
    environment = dict(os.environ)
    environment["PYTHONPATH"] = child_pythonpath(tmp_path)
    completed = subprocess.run(
        [
            sys.executable, "-m", "pytest",
            "-p", "okg.deployment.conformance",
            "-p", "case_override_plugin",
            "--pyargs", "okg.deployment.conformance",
            "-p", "no:cacheprovider",
            "-q",
        ],
        cwd=str(tmp_path),
        env=environment,
        capture_output=True,
        text=True,
        timeout=600,
    )
    output = completed.stdout + completed.stderr
    assert completed.returncode != 0, output
    assert "11 errors" in output, output
    assert "no connector to certify" in output, output


def test_the_documented_command_fails_a_connector_that_never_runs(tmp_path):
    completed, receipt_path = _documented_command(tmp_path, RAISING_CASE_REFERENCE)
    output = completed.stdout + completed.stderr

    assert completed.returncode != 0, output
    assert "11 errors" in output, output

    document = json.loads(receipt_path.read_text(encoding="utf-8"))
    assert document["verdict"] == "fail"
    assert len(document["checks"]) == len(conformance.CHECKS)
    assert all(entry["status"] == "fail" for entry in document["checks"])
    assert all(
        entry["detail"] == conformance.DID_NOT_RUN for entry in document["checks"]
    )


# --- (e) the boundary --------------------------------------------------------


def test_the_kit_needs_no_substrate_import_from_the_author():
    """A case is declared out of `okg.deployment` names only. The kit's own
    substrate imports are the SDK's, on this side of the boundary."""
    source = pathlib.Path(conformance.__file__).read_text(encoding="utf-8")
    assert "okg.substrate" in source  # the kit itself may
    author_names = {"ConnectorCase", "ConnectorRegistration", "SdkVersionRange"}
    assert author_names <= set(dir(sdk)) | set(conformance.__all__)


# --- (f) issue #1922: the shipped EdgeFact default must survive the kit ------


def test_default_edge_fact_digests_under_the_canonical_serializer():
    """A default-constructed `EdgeFact` must be digestible by the kit.

    `conformance._digests` canonicalizes every emitted fact. It once did so
    with an encoder that refused floating point, and `EdgeFact.confidence`
    shipped as `1.0`, so any connector that emitted a default-confidence
    edge failed `determinism` and `checkpoint_resume` for a reason that had
    nothing to do with the connector. #1964 changed the default to `1`;
    #1970 then moved the kit onto the publish path's own encoder, which
    accepts any finite float. Both halves are still pinned: this test holds
    the default, and `test_the_kit_digests_float_confidence_and_float_attrs`
    holds the encoder.
    """
    from okg.substrate.library.sources.base import EdgeFact

    edge = EdgeFact(src="a", dst="b", edge_type="mentions")

    # The load-bearing line: before #1964 this raised on the default value.
    digests = conformance._digests([edge])
    assert len(digests) == 1
    assert conformance._digests([edge]) == digests

    assert edge.confidence == 1
    assert not isinstance(edge.confidence, float)


# --- (g) issue #1970: the kit must digest exactly what publish would --------
#
# The kit's whole value is that its digest is the digest of the fact a
# publisher would store. It used to carry the deployment-product *lock*
# encoder instead, which refuses any float, while the publish path accepts
# every finite one. These three tests pin the two sides together: what the
# kit accepts, what it refuses, and the bytes it produces.


def _float_fact(**kwargs):
    from okg.substrate.library.sources.base import EdgeFact

    return EdgeFact(src="a", dst="b", edge_type="mentions", **kwargs)


FLOAT_BEARING_FACTS = [
    ("integral float confidence", {"confidence": 1.0}),
    ("fractional confidence", {"confidence": 0.9}),
    ("fractional attribute", {"attrs": {"score": 0.5}}),
    ("integral attribute", {"attrs": {"score": 2}}),
    ("default confidence", {}),
]


@pytest.mark.parametrize(
    "label,kwargs", FLOAT_BEARING_FACTS, ids=[row[0] for row in FLOAT_BEARING_FACTS]
)
def test_the_kit_digests_float_confidence_and_float_attrs(label, kwargs):
    """A float a connector really emits must not fail the kit.

    `git_graph_files`, `stark_prime`, `_fixture` and `agent_sessions` all
    pass a float confidence, and `attrs` is wider still: a connector may put
    a score or a ratio there. Publish stores all of them, so the kit has no
    business refusing any of them.
    """
    digests = conformance._digests([_float_fact(**kwargs)])
    assert len(digests) == 1
    assert digests[0].startswith("sha256:")
    assert conformance._digests([_float_fact(**kwargs)]) == digests


def _lowered_digest(item):
    """The digest the real publisher-side lowerer gives this one item.

    `SourceRunFactBatchLowerer` is what `SourceRunner` drives on the way to a
    publish. Pinning the kit against *it*, rather than against the same
    `FactChunk.create` call the kit makes, is what makes this a comparison
    and not a restatement: the lowerer decides for itself whether an item is
    a fact or a progress marker, and the kit has to reach the same answer.
    """
    import uuid

    from okg.substrate.fact_batch import FactBatchHeader
    from okg.substrate.library.sources.base import SourceRun
    from okg.substrate.source_fact_batch import SourceRunFactBatchLowerer

    header = FactBatchHeader(
        schema_version=1,
        run_id=uuid.UUID(conformance._DIGEST_RUN_ID),
        admission_id=uuid.uuid4(),
        producer_profile_fingerprint="sha256:" + "a" * 64,
        deployment="test",
        target_branch="candidate",
        catalog_version_id=1,
        intent="source_sync",
        idempotency_key="conformance-equivalence",
    )
    lowerer = SourceRunFactBatchLowerer(
        header=header,
        source_id="conformance-equivalence",
        source_run=SourceRun(
            facts=[item], completed_scope=True, next_cursor={"cursor": "1"}
        ),
    )
    chunks = list(lowerer.iter_chunks())
    assert len(chunks) == 1
    return chunks[0].digest


@pytest.mark.parametrize(
    "label,kwargs", FLOAT_BEARING_FACTS, ids=[row[0] for row in FLOAT_BEARING_FACTS]
)
def test_the_kit_digest_equals_the_publish_path_digest(label, kwargs):
    """The kit's digest is the lowerer's digest, byte for byte.

    Not "an equivalent canonicalization" — the same one, reached by driving
    the publisher's own lowerer over a one-item run and comparing.
    """
    fact = _float_fact(**kwargs)
    assert conformance._digests([fact]) == [_lowered_digest(fact)]


def test_the_kit_digests_a_progress_marker_as_the_lowerer_does():
    """A `ProgressMarker` is a marker to the lowerer, not a fact.

    `SourceRun.facts` is one mixed stream — `ConnectorAdapter.run` flat-extends
    whatever the connector returned — and the lowerer routes each item by type.
    A kit that hands every item over as a fact refuses the marker outright.
    """
    from okg.substrate.library.sources.base import ProgressMarker

    marker = ProgressMarker("record-1", "sha256:" + "b" * 64)
    assert conformance._digests([marker]) == [_lowered_digest(marker)]


NON_FINITE_FACTS = [
    ("nan confidence", {"confidence": float("nan")}),
    ("infinite confidence", {"confidence": float("inf")}),
    ("nan attribute", {"attrs": {"score": float("nan")}}),
    # Not a float at all. `_freeze_fact` calls `float(fact.confidence)`
    # before it can reach a FactBatch refusal, so these escape as a bare
    # ValueError and TypeError from the standard library.
    ("non-numeric confidence", {"confidence": "zzz"}),
    ("null confidence", {"confidence": None}),
]


@pytest.mark.parametrize(
    "label,kwargs", NON_FINITE_FACTS, ids=[row[0] for row in NON_FINITE_FACTS]
)
def test_the_kit_refuses_exactly_what_the_publish_path_refuses(label, kwargs):
    """A value publish refuses must be refused here, in publish's words.

    The kit must never certify an item publish would reject, and its refusal
    must carry the publisher's own sentence rather than an encoder's wording.
    It adds one label in front, because a validation refusal arriving under
    the `determinism` check name otherwise reads as though the connector had
    been caught being unstable.

    The refusal must also arrive as one type. The publisher's own refusals
    reach here as `FactBatchFieldError`, but `_freeze_fact` coerces with
    `float(fact.confidence)`, which raises a bare `ValueError` or `TypeError`
    for a non-numeric value before any FactBatch check runs. Those escaped
    the label, so `determinism` reported a raw `ValueError` at the operator.
    """
    from okg.substrate.fact_batch import FactBatchFieldError

    fact = _float_fact(**kwargs)
    with pytest.raises((ValueError, TypeError)) as published:
        _lowered_digest(fact)
    with pytest.raises(FactBatchFieldError) as digested:
        conformance._digests([fact])
    assert str(digested.value) == (
        f"this item would be refused at publish: {published.value}"
    )


class MarkerConnector(FixtureConnector):
    """A conforming connector that also yields one marker per emitted node.

    Sixteen shipped sources do this, and `ProgressMarker` is in
    `okg.deployment.__all__` for exactly this purpose. The adapter flat-extends
    the connector's output into one `SourceRun.facts` stream, so the kit sees
    facts and markers interleaved and must split them the way the lowerer does.
    """

    def run(self, ctx):
        outcome = super().run(ctx)
        interleaved = []
        for fact in outcome.facts:
            interleaved.append(fact)
            interleaved.append(
                sdk.ProgressMarker(fact.node_id, f"sha256:{'c' * 64}")
            )
        return sdk.ConnectorRun(
            facts=interleaved,
            completed_scope=outcome.completed_scope,
            next_checkpoint=outcome.next_checkpoint,
            health=outcome.health,
            warnings=outcome.warnings,
            rejected=outcome.rejected,
        )


@pytest.mark.parametrize("check", ["determinism", "checkpoint_resume"])
def test_a_marker_yielding_connector_passes_the_digest_checks(check):
    """A marker in the stream must not fail a check about the connector.

    Both checks compare digests, so both break the moment the kit refuses an
    item the publisher accepts.
    """
    case = _case(connector=MarkerConnector())
    drives = conformance.execute(case)
    assert conformance.run_check(case, drives, check) is None


class RunLevelMarkerConnector(FixtureConnector):
    """A connector that ends every run with one stable run-level marker.

    This is the `calendar_ics.py` shape: one
    `ProgressMarker(record_id=feed_id, fingerprint=<hash of the feed>)` per
    run, not per record. The fingerprint is stable while the feed is
    unchanged, so the resumed run re-emits the identical marker -- which is
    what a stable feed fingerprint is supposed to do (#1994).
    """

    FEED_FINGERPRINT = f"sha256:{'d' * 64}"

    def run(self, ctx):
        outcome = super().run(ctx)
        return sdk.ConnectorRun(
            facts=[
                *outcome.facts,
                sdk.ProgressMarker("feed-1", self.FEED_FINGERPRINT),
            ],
            completed_scope=outcome.completed_scope,
            next_checkpoint=outcome.next_checkpoint,
            health=outcome.health,
            warnings=outcome.warnings,
            rejected=outcome.rejected,
        )


@pytest.mark.parametrize("check", ["determinism", "checkpoint_resume"])
def test_a_stable_run_level_marker_is_not_a_repeated_fact(check):
    """Re-emitting one unchanged run-level marker is correct, not a break.

    A `ProgressMarker` is a commit boundary, not a record. The resumed run
    emits no fact the first run produced; it re-states the same feed
    fingerprint, and `checkpoint_resume` used to read that overlap as a
    duplicated fact (#1994).
    """
    case = _case(connector=RunLevelMarkerConnector())
    drives = conformance.execute(case)
    assert conformance.run_check(case, drives, check) is None


def test_a_resumed_run_repeating_a_real_fact_still_fails_and_says_fact():
    """The fix must not blind the check.

    `ignore_checkpoint` makes the resumed run re-emit every fact of the
    first. That must still fail, and the reason must name facts -- the
    message is only accurate now that markers are out of the comparison.
    """
    case = _case(connector=FixtureConnector(ignore_checkpoint=True))
    reason = conformance.run_check(case, conformance.execute(case), "checkpoint_resume")
    assert reason is not None
    assert "fact(s) the first run had already produced" in reason
    assert reason.startswith("resuming from next_checkpoint re-emitted 2 fact(s)")


def test_a_marker_yielding_connector_that_repeats_a_fact_still_fails():
    """Markers must not mask a real duplicate carried alongside them."""
    case = _case(connector=MarkerConnector(ignore_checkpoint=True))
    reason = conformance.run_check(case, conformance.execute(case), "checkpoint_resume")
    assert reason is not None and "fact(s)" in reason


class ResumeOnlyBadMarkerConnector(FixtureConnector):
    """Emits a marker the publisher would refuse, and only on a resume.

    The empty fingerprint is refused by `_freeze_marker`. Nothing else in
    the kit looks at it: `_determinism` digests the baseline run and a fresh
    one, never the resumed run, so `_content_digests` digesting a marker
    before dropping it is the only place this is ever seen (#1994).
    """

    def run(self, ctx):
        outcome = super().run(ctx)
        if not (ctx.checkpoint.next_cursor or {}):
            return outcome
        return sdk.ConnectorRun(
            facts=[*outcome.facts, sdk.ProgressMarker("feed-1", "")],
            completed_scope=outcome.completed_scope,
            next_checkpoint=outcome.next_checkpoint,
            health=outcome.health,
            warnings=outcome.warnings,
            rejected=outcome.rejected,
        )


def test_a_marker_only_a_resume_emits_is_still_held_to_publish():
    """Dropping markers must not mean skipping them.

    Filtering the stream before digesting it -- the obvious simplification
    of `_content_digests` -- passes this connector silently, because no
    other check digests the resumed run at all.
    """
    case = _case(connector=ResumeOnlyBadMarkerConnector())
    reason = conformance.run_check(case, conformance.execute(case), "checkpoint_resume")
    assert reason is not None
    assert "would be refused at publish" in reason
    assert "progress_marker.fingerprint must be a non-empty string" in reason


def test_change_probe_passes_a_kwargs_constructor_the_whole_config(good_drives):
    case = _case(
        registration=_registration(class_name="KwargsProbeSource"),
        config={"path": "/fixture/input.json"},
    )
    assert conformance.run_check(case, good_drives, "change_probe") is None
