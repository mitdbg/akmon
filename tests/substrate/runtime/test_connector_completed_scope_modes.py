"""A connector claims a complete scope for every scope-completing run mode.

Since the run-mode policy moved into one table (`source_run_modes`), the
runner hands a `reference_catalog` source its own whole-scope mode,
`release_new`, when a caller asks for `scope_complete` without an explicit
`--mode`. External readers built on the connector SDK tested
`mode in {"scope_complete", "reconcile"}`, so they never claimed a complete
scope for such a source. Admission then rejected every unchanged rerun as
`partial`, and `okg provision --publish-once` did not publish.

`okg.deployment` now exports `COMPLETED_SCOPE_RUN_MODES`, the substrate set
itself. These tests drive archi-style connectors through the service's own
mode choice and the runner's own kwarg selection into the real
`ConnectorAdapter`, and pin the invariant that makes the one set safe for
every profile. The Postgres twin,
`tests/substrate/test_connector_completed_scope_modes_e2e.py`, runs the same
connectors twice through `run_ingest` with a real publish between the runs
and reads the admission verdict back.

The connector and source classes below are module-level so that the twin's
source registry can name them by module path. The substrate reads
`change_probe_kind` from the class body's source text, so each probed class
declares it as a literal.
"""
from __future__ import annotations

import hashlib
from collections.abc import Callable
from pathlib import Path

import pytest

import okg.deployment as sdk
from okg.substrate import source_run_modes
from okg.substrate.ingest.runner import _POLICY_MODE, _adapter_run_kwargs
from okg.substrate.ingest.service import IngestOptions, _run_mode_for_source
from okg.substrate.ingest.zero_output_admission import (
    _source_history_kwargs,
    _zero_output_admission_verdict,
)
from okg.substrate.sources.registry import admit_source_registry_document

#: The modes the archi readers tested before the SDK exported the set.
HARD_CODED_MODES = frozenset({"scope_complete", "reconcile"})

#: Two catalog releases; enough for a record set with more than one key.
RELEASES = ("CMSSW_14_0_0", "CMSSW_14_0_1")


def claims_sdk_set(mode: str) -> bool:
    return mode in sdk.COMPLETED_SCOPE_RUN_MODES


def claims_hard_coded_set(mode: str) -> bool:
    return mode in HARD_CODED_MODES


def never_claims(mode: str) -> bool:
    """A non-deleting reader: no run claims the whole scope."""
    del mode
    return False


def write_catalog(path: Path, releases: tuple[str, ...] = RELEASES) -> Path:
    """Write the catalog input: one release label per line."""
    path.write_text("".join(f"{label}\n" for label in releases), encoding="utf-8")
    return path


def read_count(path: str | Path) -> int:
    """How many times a connector read the catalog at ``path``."""
    reads = Path(f"{path}.reads")
    return len(reads.read_text().splitlines()) if reads.exists() else 0


def _cmssw_facts(labels: list[str], run_id: str):
    del run_id
    facts = []
    records = {}
    for label in labels:
        node_id = f"svc:release-{label.lower()}"
        identity = {"id": label}
        revision = {"sha256": label.lower()}
        facts.append(
            sdk.NodeFact(node_id, "document", {"text": label}, identity, revision)
        )
        records[label] = sdk.RecordEmission(
            primary=sdk.RecordNode(node_id, "document", identity, revision)
        )
    return facts, records


def _github_facts(labels: list[str], run_id: str):
    revision = {"run_id": run_id, "n_records": len(labels)}
    records = {}

    def _facts():
        for label in labels:
            identity = {"id": label}
            software = f"svc:software-{label.lower()}"
            repo = f"svc:repo-{label.lower()}"
            yield sdk.NodeFact(software, "document", {"text": label}, identity, revision)
            yield sdk.NodeFact(repo, "document", {"text": label}, identity, revision)
            yield sdk.EdgeFact(
                src=software,
                dst=repo,
                edge_type="references",
                source_record_id=identity,
                source_revision=revision,
            )

    for label in labels:
        identity = {"id": label}
        records[label] = sdk.RecordEmission(
            primary=sdk.RecordNode(
                f"svc:software-{label.lower()}", "document", identity, revision,
            ),
        )
    return _facts(), records


_SHAPE_FACTS = {"cmssw": _cmssw_facts, "github": _github_facts}
SHAPES = tuple(_SHAPE_FACTS)


class CatalogConnector:
    """An archi-style reference-catalog reader.

    It always reads its whole input file, so whether it claims a complete
    scope depends only on how it reads `ctx.mode`. Like the archi readers it
    returns no checkpoint and reports the mode it was handed. By default it
    also returns no record set, as the archi `cmssw` reader does, so the
    runner infers the record set from the emitted facts.
    ``explicit_record_set`` makes it return one instead.

    ``checkpoint`` makes it return a content-identifying checkpoint (the
    SHA-256 of its input), the shape archi's DBS and CondDB readers use: they
    never claim a complete scope, so they never delete, but the stored
    checkpoint lets admission accept an unchanged rerun as a no-op.

    ``shape`` picks the facts: ``"cmssw"`` is one node per release;
    ``"github"`` is archi's `github_repos` shape -- two nodes and a
    `references` edge per record, streamed lazily, with the run id inside the
    source revision.
    """

    name = "archi-style-catalog"
    profile = "reference_catalog"

    def __init__(
        self,
        claims_complete: Callable[[str], bool],
        *,
        path: str,
        explicit_record_set: bool = False,
        checkpoint: bool = False,
        shape: str = "cmssw",
    ) -> None:
        if shape not in SHAPES:
            raise ValueError(f"unknown connector shape {shape!r}")
        self._shape = shape
        self._claims_complete = claims_complete
        self._path = Path(path)
        self._explicit_record_set = explicit_record_set
        self._checkpoint = checkpoint

    def run(self, ctx: sdk.ConnectorContext) -> sdk.ConnectorRun:
        with Path(f"{self._path}.reads").open("a", encoding="utf-8") as log:
            log.write(f"{ctx.mode}\n")
        content = self._path.read_bytes()
        labels = content.decode("utf-8").split()
        complete = self._claims_complete(ctx.mode) and bool(labels)
        facts, records = _SHAPE_FACTS[self._shape](labels, ctx.run_id)
        record_set = (
            sdk.RecordSet(records) if complete and self._explicit_record_set else None
        )
        return sdk.ConnectorRun(
            facts=facts,
            completed_scope=complete,
            record_set=record_set,
            next_checkpoint=(
                sdk.Checkpoint({"content_sha256": hashlib.sha256(content).hexdigest()})
                if self._checkpoint
                else None
            ),
            run_mode=ctx.mode,
            health=sdk.ConnectorHealth(status="ok"),
        )


def _content_probe(path: str) -> sdk.ContentHashProbe:
    """The probe archi's `content_hash_change_probe` builds: a content hash
    over the input file, the reader's configuration and its emit code."""
    return sdk.ContentHashProbe(
        content_items=lambda: [("catalog", Path(path))],
        config={"path": path},
        emit_targets=CatalogConnector,
    )


class ExplicitRecordSetCatalogSource(sdk.ConnectorAdapter):
    """Uses the SDK set and returns its own record set."""

    profile = "reference_catalog"

    def __init__(self, *, path: str, shape: str = "cmssw") -> None:
        super().__init__(
            CatalogConnector(
                claims_sdk_set, path=path, explicit_record_set=True, shape=shape,
            )
        )


class SdkSetCatalogSource(sdk.ConnectorAdapter):
    """Uses the SDK set and, like archi, returns no record set."""

    profile = "reference_catalog"

    def __init__(self, *, path: str, shape: str = "cmssw") -> None:
        super().__init__(
            CatalogConnector(claims_sdk_set, path=path, shape=shape)
        )


class ProbedSdkSetCatalogSource(sdk.ConnectorAdapter):
    """The archi `CMSSWReleaseAdapter` shape after the fix.

    A content-hash change probe, no record set, and no
    `probe_short_circuit_safe` declaration, so the runner still reads every
    run. A connector cannot declare it: the runner then requires an exact
    applied token, which `ConnectorRun` does not carry, and refuses the run
    (`applied_token_not_exact_global`).
    """

    profile = "reference_catalog"

    change_probe_kind = "content_hash"

    def __init__(self, *, path: str, shape: str = "cmssw") -> None:
        super().__init__(
            CatalogConnector(claims_sdk_set, path=path, shape=shape)
        )
        self.change_probe = _content_probe(path)


class HardCodedCatalogSource(sdk.ConnectorAdapter):
    """The archi reader before the fix: the old list, no record set."""

    profile = "reference_catalog"

    def __init__(self, *, path: str, shape: str = "cmssw") -> None:
        super().__init__(
            CatalogConnector(claims_hard_coded_set, path=path, shape=shape)
        )


class CheckpointCatalogSource(sdk.ConnectorAdapter):
    """Never claims a complete scope; returns a content checkpoint."""

    profile = "reference_catalog"

    def __init__(self, *, path: str, shape: str = "cmssw") -> None:
        super().__init__(
            CatalogConnector(never_claims, path=path, checkpoint=True, shape=shape)
        )


class NoCheckpointCatalogSource(sdk.ConnectorAdapter):
    """The same non-deleting reader without a checkpoint."""

    profile = "reference_catalog"

    def __init__(self, *, path: str, shape: str = "cmssw") -> None:
        super().__init__(
            CatalogConnector(never_claims, path=path, shape=shape)
        )


def reference_catalog_entry_body(ownership_id: str) -> dict:
    """A legacy-contract `reference_catalog` entry, as CRAB and comp-ops ship."""
    return {
        "module": __name__,
        "class": "SdkSetCatalogSource",
        "ownership_id": ownership_id,
        "source_class": "reference_catalog",
        "record_identity_kind": "domain_key",
        "record_identity_fields": ["id"],
        "source_revision_kind": "content_hash",
        "deletion_semantics": "missing_from_completed_scope",
        "publication_mode": "published_generation",
    }


def _entry():
    admission = admit_source_registry_document(
        {"sources": {"catalog": reference_catalog_entry_body("svc.catalog")}},
        registry_path=Path("/nonexistent/source_registry.yaml"),
    )
    return admission.entries["catalog"]


def _options(**overrides) -> IngestOptions:
    base = dict(
        dsn="postgresql://fake/connector-modes",
        deployment="svc",
        deployment_dir=Path("/nonexistent"),
        run_mode="scope_complete",
    )
    base.update(overrides)
    return IngestOptions(**base)


def _mode(options: IngestOptions) -> str:
    entry = _entry()
    assert entry.source_class == "reference_catalog"
    return _run_mode_for_source(options, entry, entry.name, (), None)


def _whole_scope_mode() -> str:
    """The mode `okg ingest` and `okg provision` hand the source."""
    options = _options()
    assert options.run_mode_explicit is False
    return _mode(options)


def _drive(adapter: sdk.ConnectorAdapter, mode: str):
    selected = _adapter_run_kwargs(
        adapter,
        mode=mode,
        cursor=None,
        since_progress=None,
        sync_scope=None,
    )
    return adapter.run("run-1", **selected)


@pytest.fixture
def catalog(tmp_path: Path) -> str:
    return str(write_catalog(tmp_path / "releases.txt"))


def test_the_sdk_exports_the_substrate_completed_scope_set():
    assert sdk.COMPLETED_SCOPE_RUN_MODES is source_run_modes.COMPLETED_SCOPE_RUN_MODES
    assert "COMPLETED_SCOPE_RUN_MODES" in sdk.__all__
    assert {"release_new", "release_unchanged", "reconcile", "scope_complete"} <= (
        sdk.COMPLETED_SCOPE_RUN_MODES
    )
    assert not HARD_CODED_MODES >= sdk.COMPLETED_SCOPE_RUN_MODES


@pytest.mark.parametrize("profile", sorted(source_run_modes.PROFILE_RUN_MODES))
def test_the_one_set_names_exactly_each_profiles_completing_modes(profile):
    """A connector sees only `ctx.mode`, never its policy row, so the union
    is safe only while no profile accepts a mode that completes another
    profile's scope without completing its own. A row that broke this would
    make every SDK connector of that profile claim a complete scope -- the
    deletion authority -- for a run that did not cover it."""
    modes = source_run_modes.PROFILE_RUN_MODES[profile]
    assert modes.accepted & sdk.COMPLETED_SCOPE_RUN_MODES == modes.completes_scope


def test_a_reference_catalog_whole_scope_request_reaches_the_connector_as_release_new(
    catalog,
):
    mode = _whole_scope_mode()
    assert mode == "release_new"
    assert ("reference_catalog", mode) in _POLICY_MODE

    _drive(SdkSetCatalogSource(path=catalog), mode)
    assert Path(f"{catalog}.reads").read_text().split() == ["release_new"]


def test_an_explicit_mode_is_not_rewritten():
    """`release_new` arrives only when no explicit `--mode` was passed."""
    explicit = _options(run_mode="scope_complete", run_mode_explicit=True)
    assert _mode(explicit) == "scope_complete"


@pytest.mark.parametrize(
    "source_class",
    [SdkSetCatalogSource, ExplicitRecordSetCatalogSource, ProbedSdkSetCatalogSource],
)
def test_a_connector_using_the_sdk_set_completes_a_reference_catalog_scope(
    catalog, source_class,
):
    run = _drive(source_class(path=catalog), _whole_scope_mode())

    assert run.run_mode == "release_new"
    assert run.completed_scope is True
    if source_class is ExplicitRecordSetCatalogSource:
        assert sorted(run.record_set.records) == sorted(RELEASES)
    else:
        # The archi shape: the runner infers the record set from the facts.
        assert run.record_set is None


def test_a_connector_with_the_hard_coded_list_never_completes_it(catalog):
    """The failure mode: the same whole read, reported as incomplete."""
    run = _drive(HardCodedCatalogSource(path=catalog), _whole_scope_mode())

    assert run.run_mode == "release_new"
    assert run.completed_scope is False
    assert run.record_set is None


def test_the_probed_source_declares_the_cmssw_probe_shape(catalog):
    source = ProbedSdkSetCatalogSource(path=catalog)
    assert source.change_probe_kind == "content_hash"
    assert getattr(source, "probe_short_circuit_safe", False) is False
    assert isinstance(source.change_probe, sdk.ContentHashProbe)


def test_a_non_deleting_reader_returns_a_content_checkpoint(catalog):
    run = _drive(CheckpointCatalogSource(path=catalog), _whole_scope_mode())
    assert run.run_mode == "release_new"
    assert run.completed_scope is False
    assert run.record_set is None
    digest = hashlib.sha256(Path(catalog).read_bytes()).hexdigest()
    assert run.next_cursor == {"content_sha256": digest}

    bare = _drive(NoCheckpointCatalogSource(path=catalog), _whole_scope_mode())
    assert bare.completed_scope is False
    assert bare.next_cursor is None


def test_the_github_shape_streams_two_nodes_and_an_edge_per_record(catalog):
    run = _drive(SdkSetCatalogSource(path=catalog, shape="github"), _whole_scope_mode())
    assert run.completed_scope is True
    kinds = [type(fact).__name__ for fact in run.facts]
    assert kinds == ["NodeFact", "NodeFact", "EdgeFact"] * len(RELEASES)
    assert {fact.source_revision["run_id"] for fact in run.facts} == {"run-1"}


def test_a_completed_scope_reads_no_history():
    """No database is touched: `conn` is never used for a completed scope."""
    assert _source_history_kwargs(
        None, source="svc.first", deployment="svc", completed_scope=True,
    ) == {"source_has_published_watermark": False, "source_history": None}


@pytest.mark.parametrize(
    ("history", "reason"),
    [
        ("cursor", "incomplete_scope_noop"),
        ("reconcile_time", "incomplete_scope_noop"),
        ("published_source_admission", "incomplete_scope_noop"),
        (None, "partial"),
    ],
)
def test_the_verdict_names_the_history_signal_that_matched(history, reason):
    verdict = _zero_output_admission_verdict(
        source="svc.first",
        run_id="run-2",
        admission_mode="fast_track",
        completed_scope=False,
        source_health_status="ok",
        run_mode="release_new",
        next_cursor=None,
        source_has_published_watermark=history is not None,
        source_history=history,
    )
    policy = verdict["producer_policy"]
    assert policy["reason"] == reason
    assert policy["source_history"] == history
    assert policy["source_has_published_watermark"] is (history is not None)
