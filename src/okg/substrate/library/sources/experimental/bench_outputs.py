"""BenchOutputsSource — emits Result nodes from benchmark output files.

Reads JSON, YAML, and CSV files matching the configured glob. For
each row / metric entry, emits one ``Result`` node with the parent
``Experiment`` reference + the schema-mapped attributes (metric name,
value, unit, optional step). Also emits a ``Result derives_from
Trial`` mention edge using either the row's explicit ``trial_ref``
column or a synthetic default-trial id derived from the parent
Experiment slug.

Profile: ``reference_catalog`` — benchmark outputs are a reference
set that the operator regenerates by re-running the bench.

Per `extend-substrate-experimental-with-compute-job-and-sources` Phase 4.
"""

from __future__ import annotations

import csv
import hashlib
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, ClassVar, Iterable, Iterator, Optional

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


__all__ = [
    "BenchOutputsSource",
    "ResultSchemaError",
]


class ResultSchemaError(SubstrateError):
    """Raised when the operator-supplied ``result_schema`` is
    incomplete or maps fields the input file does not contain."""

    code: ClassVar[str] = "result_schema_error"


_REQUIRED_SCHEMA_KEYS: tuple[str, ...] = (
    "experiment_ref",
    "metric_name",
    "metric_value",
)
_OPTIONAL_SCHEMA_KEYS: tuple[str, ...] = (
    "metric_unit",
    "metric_step",
    "trial_ref",
)


@dataclass
class BenchOutputsSource:
    """Substrate adapter for benchmark output files (JSON / YAML / CSV)."""

    results_glob: str
    result_schema: dict[str, str] = field(default_factory=dict)
    """Mapping from `Result` attribute names to input field names.

    Required keys: ``experiment_ref``, ``metric_name``,
    ``metric_value``. Optional: ``metric_unit``, ``metric_step``,
    ``trial_ref``. Unknown keys raise :class:`ResultSchemaError`."""

    base_dir: Path | str = Path(".")

    name: str = "experimental.bench_outputs"
    profile: str = "reference_catalog"

    # Incremental-source contract (issue #323): benchmark outputs are local
    # JSON / YAML / CSV files on disk (no git scope, no upstream API), so the
    # probe is the O(content) floor — read+hash the CURRENT in-scope files, skip
    # parse/extraction on no-change. The digest reuses the source's own
    # `_iter_paths()` enumeration so the probe and `run()` read EXACTLY the same
    # files; membership is sensitive (an added/deleted output file busts the
    # token, a delete simply drops its entry). The `result_schema` mapping drives
    # every emitted attr, so it folds into the config component (a schema edit
    # re-emits even when the bytes are unchanged). A vanished/unreadable file
    # reads as the "absent" identity inside the probe, so it still shifts the
    # token rather than silently no-changing.
    change_probe_kind = "content_hash"

    def __post_init__(self) -> None:
        if isinstance(self.base_dir, str):
            self.base_dir = Path(self.base_dir).expanduser()
        self._validate_schema()
        self.change_probe = self._build_change_probe()

    def _build_change_probe(self) -> Any:
        import sys

        from okg.substrate.library.sources.content_hash_probe import (
            ContentHashProbe,
        )

        def _items() -> list[tuple[str, Path]]:
            # Re-enumerate live state every call so an added/deleted bench output
            # file is observed (delete = a dropped (key, sha) entry). Reuse the
            # source's own path iteration so the probe sees the same files run()
            # would read.
            return [(str(p), p) for p in self._iter_paths()]

        return ContentHashProbe(
            content_items=_items,
            config={
                "results_glob": self.results_glob,
                "base_dir": str(self.base_dir),
                # The schema mapping changes which input fields become which
                # emitted Result attrs, so a schema edit must bust the token even
                # under byte-identical files.
                "result_schema": dict(sorted(self.result_schema.items())),
                "name": self.name,
                "profile": self.profile,
            },
            emit_targets=[sys.modules[__name__]],
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
        rows = list(self._read_rows())
        revision = {"run_id": run_id}
        completed_scope = mode != "cursor"
        record_set = self._build_record_set(rows, revision)

        def _facts() -> Iterator[Any]:
            for row in rows:
                yield from row.to_facts(
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

    def _validate_schema(self) -> None:
        if not isinstance(self.result_schema, dict):
            raise ResultSchemaError(
                "result_schema must be a mapping",
            )
        missing = [
            k for k in _REQUIRED_SCHEMA_KEYS
            if not self.result_schema.get(k)
        ]
        if missing:
            raise ResultSchemaError(
                f"result_schema missing required keys: {missing}",
                fix_hint=(
                    "result_schema must name the input fields for "
                    f"{list(_REQUIRED_SCHEMA_KEYS)}; optional fields: "
                    f"{list(_OPTIONAL_SCHEMA_KEYS)}"
                ),
            )
        unknown = [
            k for k in self.result_schema
            if k not in _REQUIRED_SCHEMA_KEYS + _OPTIONAL_SCHEMA_KEYS
        ]
        if unknown:
            raise ResultSchemaError(
                f"result_schema has unknown keys: {unknown}",
                fix_hint=(
                    f"recognised keys: "
                    f"{list(_REQUIRED_SCHEMA_KEYS + _OPTIONAL_SCHEMA_KEYS)}"
                ),
            )

    def _read_rows(self) -> Iterator["_BenchRow"]:
        for path in sorted(self._iter_paths()):
            ext = path.suffix.lower()
            if ext == ".csv":
                rows = list(_read_csv_rows(path))
            elif ext in (".json",):
                rows = list(_read_json_rows(path))
            elif ext in (".yaml", ".yml"):
                rows = list(_read_yaml_rows(path))
            else:
                # Unknown extension; skip with no silent fallback — the
                # source's run summary should surface this via metrics.
                # Today we simply ignore the file.
                continue
            for idx, raw in enumerate(rows):
                yield self._row_from_raw(path, idx, raw)

    def _iter_paths(self) -> Iterator[Path]:
        base = Path(self.base_dir)
        pattern = self.results_glob
        if Path(pattern).is_absolute():
            for p in sorted(Path("/").glob(pattern.lstrip("/"))):
                if p.is_file():
                    yield p
        else:
            for p in sorted(base.glob(pattern)):
                if p.is_file():
                    yield p

    def _row_from_raw(
        self, path: Path, idx: int, raw: dict[str, Any],
    ) -> "_BenchRow":
        def _get(key: str) -> Optional[Any]:
            src_field = self.result_schema.get(key)
            if not src_field:
                return None
            return raw.get(src_field)

        experiment_ref = _get("experiment_ref")
        metric_name = _get("metric_name")
        metric_value_raw = _get("metric_value")

        for key, value in (
            ("experiment_ref", experiment_ref),
            ("metric_name", metric_name),
            ("metric_value", metric_value_raw),
        ):
            if value is None or value == "":
                raise ResultSchemaError(
                    f"row {idx} in {path} missing required field "
                    f"mapped from result_schema[{key!r}]",
                    data={"path": str(path), "row_index": idx},
                )

        try:
            metric_value = float(metric_value_raw)
        except (TypeError, ValueError) as exc:
            raise ResultSchemaError(
                f"row {idx} in {path}: metric_value "
                f"{metric_value_raw!r} is not numeric",
                data={"path": str(path), "row_index": idx},
            ) from exc

        unit = _get("metric_unit")
        step_raw = _get("metric_step")
        step: Optional[int] = None
        if step_raw is not None and step_raw != "":
            try:
                step = int(step_raw)
            except (TypeError, ValueError) as exc:
                raise ResultSchemaError(
                    f"row {idx} in {path}: metric_step "
                    f"{step_raw!r} is not an integer",
                    data={"path": str(path), "row_index": idx},
                ) from exc

        trial_ref = _get("trial_ref")
        if not trial_ref:
            # Default trial for experiments emitting unparameterised
            # results: one synthetic Trial per Experiment.
            trial_ref = f"trial:{str(experiment_ref).removeprefix('exp:')}:default"

        # Deterministic result_id so re-runs against the same file
        # produce stable identity.
        digest_input = f"{path}|{idx}|{experiment_ref}|{metric_name}|{step}"
        result_id = "result:" + hashlib.sha256(
            digest_input.encode()
        ).hexdigest()[:16]

        return _BenchRow(
            path=path,
            row_index=idx,
            result_id=result_id,
            experiment_ref=str(experiment_ref),
            trial_ref=str(trial_ref),
            metric_name=str(metric_name),
            metric_value=metric_value,
            metric_unit=str(unit) if unit else None,
            metric_step=step,
        )

    def _build_record_set(
        self,
        rows: list["_BenchRow"],
        revision: dict[str, Any],
    ) -> RecordSet:
        emissions: dict[str, RecordEmission] = {}
        for row in rows:
            emissions[row.result_id] = row.to_record_emission(revision)
        return RecordSet(records=emissions)


@dataclass
class _BenchRow:
    """One parsed benchmark row → one Result node + one derives_from
    edge to the parent Trial."""

    path: Path
    row_index: int
    result_id: str
    experiment_ref: str
    trial_ref: str
    metric_name: str
    metric_value: float
    metric_unit: Optional[str]
    metric_step: Optional[int]

    def source_record_id(self) -> dict[str, Any]:
        return {"path": str(self.path), "row_index": self.row_index}

    def to_facts(
        self, *, source_name: str, revision: dict[str, Any],
    ) -> Iterator[Any]:
        sri = self.source_record_id()
        attrs: dict[str, Any] = {
            "result_id": self.result_id,
            "trial_id": self.trial_ref,
            "metric_name": self.metric_name,
            "metric_value": self.metric_value,
            "experiment_ref": self.experiment_ref,
        }
        if self.metric_unit:
            attrs["metric_unit"] = self.metric_unit
        if self.metric_step is not None:
            attrs["metric_step"] = self.metric_step

        yield NodeFact(
            node_id=self.result_id,
            subtype="result",
            attrs=attrs,
            source_record_id=sri,
            source_revision=revision,
            op="I",
        )

        # Trial → Result containment (the substrate's existing
        # `trial_produces_result` narrowing). Re-emission of the Trial
        # node itself is not this source's job; it materialises via
        # the existing experimental schema's `experiment_produces_result`
        # alias and operator-curated Trial records, if needed.
        yield EdgeFact(
            src=self.trial_ref,
            dst=self.result_id,
            edge_type="produces",
            attrs={},
            provenance="bench_outputs",
            confidence=1.0,
            source_record_id=sri,
            source_revision=revision,
            op="I",
        )

    def to_record_emission(
        self, revision: dict[str, Any]
    ) -> RecordEmission:
        sri = self.source_record_id()
        return RecordEmission(
            primary=RecordNode(
                node_id=self.result_id,
                subtype="result",
                source_record_id=sri,
                source_revision=revision,
            ),
            edges=[
                EdgeKey(
                    src=self.trial_ref,
                    dst=self.result_id,
                    edge_type="produces",
                    source_record_id=sri,
                    source_revision=revision,
                ),
            ],
        )


# ---------- file readers ----------


def _read_csv_rows(path: Path) -> Iterable[dict[str, Any]]:
    with path.open(newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            yield dict(row)


def _read_json_rows(path: Path) -> Iterable[dict[str, Any]]:
    doc = json.loads(path.read_text() or "null")
    yield from _flatten_doc(doc, path)


def _read_yaml_rows(path: Path) -> Iterable[dict[str, Any]]:
    doc = fastyaml.safe_load(path.read_text() or "")
    yield from _flatten_doc(doc, path)


def _flatten_doc(doc: Any, path: Path) -> Iterable[dict[str, Any]]:
    """Coerce a JSON/YAML doc into an iterable of row mappings.

    Accepted shapes:
    - list of mappings  (most common)
    - mapping with a `results: [...]` key
    - single mapping    (one-row file)
    """
    if doc is None:
        return
    if isinstance(doc, list):
        for item in doc:
            if isinstance(item, dict):
                yield item
        return
    if isinstance(doc, dict):
        if isinstance(doc.get("results"), list):
            for item in doc["results"]:
                if isinstance(item, dict):
                    yield item
            return
        # Single-row mapping.
        yield doc
