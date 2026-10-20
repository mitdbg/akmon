"""ExperimentRecordsSource — operator-authored experiment YAML files.

Reads ``*.yaml`` files matching the configured glob. Each file is one
experiment authoring document. The source emits one ``Experiment``
node per file plus mention/cross-link edges read from frontmatter:

- ``tests_hypothesis: hyp:<slug>`` → ``Experiment tests Hypothesis``
- ``uses_dataset: ds:<slug>`` → ``Experiment uses Dataset``
- ``concerns_concept: concept:<slug>`` → ``Experiment concerns Concept``
- ``ran_on_compute_job: { cluster, job_id }`` → ``Experiment ran_on ComputeJob``

The ``success_condition`` block, when present, is parsed via the
narrow predicate DSL and stored on the Experiment's ``attrs`` so the
``success_condition_eval`` matcher can read it later. Malformed
predicates raise :class:`PredicateParseError` at source-parse time —
no silent fallback.

Profile: ``reference_catalog`` — the operator's experiment tree is a
small, hand-maintained catalog; absence implies retraction.

Per `extend-substrate-experimental-with-compute-job-and-sources` Phase 3.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, ClassVar, Iterator, Optional

import yaml
from okg.substrate import fastyaml

from okg.substrate.errors import SubstrateError
from okg.substrate.library.sources.base import (
    EdgeFact,
    EdgeKey,
    NodeFact,
    RecordEmission,
    RecordNode,
    RecordSet,
    SourceRun,
)
from okg.substrate.library.sources.experimental.predicates import (
    Predicate,
    PredicateParseError,
)


__all__ = [
    "ExperimentRecordsSource",
    "ExperimentRecordParseError",
]


class ExperimentRecordParseError(SubstrateError):
    """Raised when an experiment YAML file is malformed in a way the
    source cannot reasonably ignore (missing required field, unparsable
    success_condition, etc.)."""

    code: ClassVar[str] = "experiment_record_parse_error"


_REQUIRED_FIELDS: tuple[str, ...] = ("name",)


@dataclass
class ExperimentRecordsSource:
    """Substrate adapter for operator-authored experiment YAML files."""

    records_path: str
    """Glob pattern relative to ``base_dir`` (or absolute). All
    matching files are read as experiment YAML records."""

    base_dir: Path | str = Path(".")
    """Working directory that ``records_path`` resolves against. The
    deployment sets this from its config; tests pass ``tmp_path``."""

    name: str = "experimental.experiment_records"
    profile: str = "reference_catalog"

    # Incremental-source contract (issue #323): the scope is a set of local
    # operator-authored experiment YAML files matched by ``records_path`` under
    # ``base_dir``. There is no git head, append offset, or upstream API — just
    # bytes on disk — so the sound floor is the O(content) ContentHashProbe over
    # the source's own file enumeration. It is membership-sensitive, so an
    # add / edit / delete of any matched file busts the token, and content-based,
    # so a worktree-checkout mtime bump does NOT. Emitted node/edge attrs derive
    # purely from each file's parsed YAML (no ``now()``/clock value reaches an
    # emitted attr — only ``run_id`` lands on ``source_revision`` lineage), so a
    # content hash is a complete superset of the emission-determining content.
    change_probe_kind = "content_hash"

    def __post_init__(self) -> None:
        if isinstance(self.base_dir, str):
            self.base_dir = Path(self.base_dir).expanduser()
        self.change_probe = self._build_change_probe()

    def _build_change_probe(self) -> Any:
        import sys

        from okg.substrate.library.sources.content_hash_probe import (
            ContentHashProbe,
        )
        from okg.substrate.library.sources.experimental import predicates

        def _items() -> list[tuple[str, Path]]:
            # Re-enumerate live scope each call (delete = dropped entry) using
            # the source's OWN glob resolution, which handles the absolute and
            # relative ``records_path`` forms ``file_content_items`` does not.
            return [(str(p), p) for p in sorted(self._iter_paths())]

        return ContentHashProbe(
            content_items=_items,
            config={
                "records_path": self.records_path,
                "base_dir": str(self.base_dir),
                "name": self.name,
                "profile": self.profile,
            },
            # `predicates.Predicate.parse` GATES emission (a record whose
            # success_condition predicate fails to parse is rejected), so the
            # predicate DSL is part of the emission logic. Fold the predicates
            # module into emit_version too: a tightening of the DSL that flips a
            # previously-accepted record to rejected must bust the token even
            # when the record bytes are unchanged (else a content-only token is a
            # false-unchanged after that code change).
            emit_targets=[sys.modules[__name__], predicates],
        )

    # ---------- public API ----------

    def run(
        self,
        run_id: str,
        *,
        mode: str = "cursor",
        cursor: Optional[dict[str, Any]] = None,
        sync_scope: Optional[dict[str, Any]] = None,
    ) -> SourceRun:
        records = list(self._read_records())
        revision = {"run_id": run_id}
        completed_scope = mode != "cursor"

        record_set = self._build_record_set(records, revision)

        def _facts() -> Iterator[Any]:
            for rec in records:
                yield from rec.to_facts(
                    source_name=self.name,
                    revision=revision,
                )

        return SourceRun(
            facts=_facts(),
            completed_scope=completed_scope,
            next_cursor=cursor or {},
            record_set=record_set,
        )

    # ---------- impl ----------

    def _read_records(self) -> Iterator["_ExperimentRecord"]:
        for path in sorted(self._iter_paths()):
            yield self._parse_one(path)

    def _iter_paths(self) -> Iterator[Path]:
        base = Path(self.base_dir)
        pattern = self.records_path
        if Path(pattern).is_absolute():
            # Anchor on filesystem root; treat as absolute glob.
            for p in sorted(Path("/").glob(pattern.lstrip("/"))):
                if p.is_file():
                    yield p
        else:
            for p in sorted(base.glob(pattern)):
                if p.is_file():
                    yield p

    def _parse_one(self, path: Path) -> "_ExperimentRecord":
        try:
            text = path.read_text()
        except OSError as exc:
            raise ExperimentRecordParseError(
                f"could not read experiment record at {path}: {exc}",
                data={"path": str(path)},
            ) from exc

        try:
            doc = fastyaml.safe_load(text) or {}
        except yaml.YAMLError as exc:
            raise ExperimentRecordParseError(
                f"invalid YAML in experiment record at {path}: {exc}",
                data={"path": str(path)},
            ) from exc

        if not isinstance(doc, dict):
            raise ExperimentRecordParseError(
                f"experiment record at {path} must be a mapping",
                data={"path": str(path)},
            )

        for field_name in _REQUIRED_FIELDS:
            if not doc.get(field_name):
                raise ExperimentRecordParseError(
                    f"experiment record at {path} missing required "
                    f"field {field_name!r}",
                    data={"path": str(path), "field": field_name},
                )

        slug: str = str(doc["name"]).strip()
        experiment_id = doc.get("experiment_id") or f"exp:{slug}"

        # Validate the predicate now so failures are loud and local.
        # The parsed Predicate is stored as JSON on the node for
        # downstream matchers; we keep the raw success_condition block
        # too so operators can audit verbatim.
        success_condition = doc.get("success_condition")
        success_condition_json: Optional[str] = None
        if success_condition is not None:
            if not isinstance(success_condition, dict):
                raise ExperimentRecordParseError(
                    f"success_condition in {path} must be a mapping "
                    f"with `metric` and `predicate` keys",
                    data={"path": str(path)},
                )
            metric = success_condition.get("metric")
            predicate_text = success_condition.get("predicate")
            if not metric or not isinstance(metric, str):
                raise ExperimentRecordParseError(
                    f"success_condition.metric in {path} must be a "
                    f"non-empty string",
                    data={"path": str(path)},
                )
            if not isinstance(predicate_text, str):
                raise ExperimentRecordParseError(
                    f"success_condition.predicate in {path} must be a "
                    f"string",
                    data={"path": str(path)},
                )
            # Parse now: raises PredicateParseError on unsupported DSL.
            try:
                Predicate.parse(predicate_text)
            except PredicateParseError as exc:
                # Re-raise with the source path for operator clarity.
                raise PredicateParseError(
                    f"in experiment {path}: {exc.message}",
                    data={**exc.data, "path": str(path)},
                    fix_hint=exc.fix_hint,
                ) from exc
            success_condition_json = json.dumps(
                {"metric": metric, "predicate": predicate_text},
                sort_keys=True,
            )

        # ran_on_compute_job is a mapping with cluster + job_id; the
        # source's emit derives a deterministic ComputeJob id but does
        # NOT emit the ComputeJob node — ClusterJobsSource owns that.
        # We only emit the cross-module mention edge.
        ran_on = doc.get("ran_on_compute_job")
        ran_on_compute_job_id: Optional[str] = None
        if ran_on is not None:
            if not isinstance(ran_on, dict):
                raise ExperimentRecordParseError(
                    f"ran_on_compute_job in {path} must be a mapping "
                    f"with `cluster` and `job_id` keys",
                    data={"path": str(path)},
                )
            cluster = ran_on.get("cluster")
            job_id = ran_on.get("job_id")
            if not cluster or not isinstance(cluster, str):
                raise ExperimentRecordParseError(
                    f"ran_on_compute_job.cluster in {path} must be a "
                    f"non-empty string",
                    data={"path": str(path)},
                )
            if job_id is None:
                raise ExperimentRecordParseError(
                    f"ran_on_compute_job.job_id in {path} is required",
                    data={"path": str(path)},
                )
            ran_on_compute_job_id = f"job:{cluster}:{job_id}"

        # Frontmatter slugs for cross-module edges.
        tests_hypothesis = _coerce_slug(doc.get("tests_hypothesis"))
        uses_dataset = _coerce_slug(doc.get("uses_dataset"))
        concerns_concept = _coerce_slug(doc.get("concerns_concept"))

        attrs: dict[str, Any] = {
            "experiment_id": experiment_id,
            "slug": slug,
            "title": doc.get("title") or slug,
        }
        for opt in ("design", "kind", "status", "hypothesis_ref"):
            if doc.get(opt) is not None:
                attrs[opt] = doc[opt]
        if success_condition_json is not None:
            attrs["success_condition"] = success_condition_json

        return _ExperimentRecord(
            path=path,
            experiment_id=experiment_id,
            slug=slug,
            attrs=attrs,
            tests_hypothesis=tests_hypothesis,
            uses_dataset=uses_dataset,
            concerns_concept=concerns_concept,
            ran_on_compute_job_id=ran_on_compute_job_id,
        )

    def _build_record_set(
        self,
        records: list["_ExperimentRecord"],
        revision: dict[str, Any],
    ) -> RecordSet:
        emissions: dict[str, RecordEmission] = {}
        for rec in records:
            emissions[rec.record_key()] = rec.to_record_emission(revision)
        return RecordSet(records=emissions)


def _coerce_slug(raw: Any) -> Optional[str]:
    if raw is None:
        return None
    if not isinstance(raw, str):
        return None
    s = raw.strip()
    return s or None


@dataclass
class _ExperimentRecord:
    """One parsed experiment YAML file."""

    path: Path
    experiment_id: str
    slug: str
    attrs: dict[str, Any]
    tests_hypothesis: Optional[str] = None
    uses_dataset: Optional[str] = None
    concerns_concept: Optional[str] = None
    ran_on_compute_job_id: Optional[str] = None

    def record_key(self) -> str:
        return self.experiment_id

    def source_record_id(self) -> dict[str, Any]:
        return {"path": str(self.path)}

    def content_fingerprint(self) -> str:
        # Stable hash over the visible attrs for record-set diffing.
        payload = json.dumps(
            {
                "attrs": self.attrs,
                "tests_hypothesis": self.tests_hypothesis,
                "uses_dataset": self.uses_dataset,
                "concerns_concept": self.concerns_concept,
                "ran_on_compute_job_id": self.ran_on_compute_job_id,
            },
            sort_keys=True,
        )
        return hashlib.sha256(payload.encode()).hexdigest()

    def to_facts(
        self,
        *,
        source_name: str,
        revision: dict[str, Any],
    ) -> Iterator[Any]:
        sri = self.source_record_id()

        yield NodeFact(
            node_id=self.experiment_id,
            subtype="experiment",
            attrs=dict(self.attrs),
            source_record_id=sri,
            source_revision=revision,
            op="I",
        )

        if self.tests_hypothesis:
            yield EdgeFact(
                src=self.experiment_id,
                dst=self.tests_hypothesis,
                edge_type="tests",
                attrs={},
                provenance="experiment_records",
                confidence=1.0,
                source_record_id=sri,
                source_revision=revision,
                op="I",
            )
        if self.uses_dataset:
            yield EdgeFact(
                src=self.experiment_id,
                dst=self.uses_dataset,
                edge_type="uses",
                attrs={},
                provenance="experiment_records",
                confidence=1.0,
                source_record_id=sri,
                source_revision=revision,
                op="I",
            )
        if self.concerns_concept:
            yield EdgeFact(
                src=self.experiment_id,
                dst=self.concerns_concept,
                edge_type="concerns",
                attrs={},
                provenance="experiment_records",
                confidence=1.0,
                source_record_id=sri,
                source_revision=revision,
                op="I",
            )
        if self.ran_on_compute_job_id:
            yield EdgeFact(
                src=self.experiment_id,
                dst=self.ran_on_compute_job_id,
                edge_type="ran_on",
                attrs={},
                provenance="experiment_records",
                confidence=1.0,
                source_record_id=sri,
                source_revision=revision,
                op="I",
            )

    def to_record_emission(
        self, revision: dict[str, Any]
    ) -> RecordEmission:
        sri = self.source_record_id()
        primary = RecordNode(
            node_id=self.experiment_id,
            subtype="experiment",
            source_record_id=sri,
            source_revision=revision,
        )
        edges: list[EdgeKey] = []
        for dst, edge_type in (
            (self.tests_hypothesis, "tests"),
            (self.uses_dataset, "uses"),
            (self.concerns_concept, "concerns"),
            (self.ran_on_compute_job_id, "ran_on"),
        ):
            if not dst:
                continue
            edges.append(
                EdgeKey(
                    src=self.experiment_id,
                    dst=dst,
                    edge_type=edge_type,
                    source_record_id=sri,
                    source_revision=revision,
                )
            )
        return RecordEmission(primary=primary, edges=edges)
