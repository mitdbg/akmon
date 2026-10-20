"""Planner-side types: FragmentSpec, PlanFn, ViewDefinition.

A planner is a pure function ``(corpus, view_def) -> list[FragmentSpec]``.
Each FragmentSpec is either a :class:`QuoteSpec` (verbatim copy of
a single anchor's content) or a :class:`GeneratedSpec` (LLM-rendered
prose grounded in declared sources).

The umbrella's "Planner determinism across interpreters" invariant
lives in the planner itself — the spec types here are passive
data carriers. Determinism is enforced by tests in Stage 5.

``ViewDefinition`` is an in-memory object that names the planner +
assembler + parameters for one registered view. The substrate
catalog also has a ``view_definition`` graph node subtype (owned
by ``viz-materialized-view-nodes``); this Python ``ViewDefinition``
is the runtime representation, distinct from the substrate node.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Mapping, Protocol, Union, runtime_checkable

from okg.views.types import Anchor


# ---------------------------------------------------------------------------
# FragmentSpec — the planner's output type
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class QuoteSpec:
    """Spec for a quote fragment: copy ``anchor_id``'s content verbatim.

    The synthesizer reads the anchor (through ``RestrictedCorpus``)
    and emits a Fragment whose ``content`` is the anchor's content
    byte-literal.
    """

    id: str
    anchor_id: str
    display_label: str = ""

    @property
    def kind(self) -> str:
        return "quote"

    @property
    def sources(self) -> tuple[str, ...]:
        # Quote specs always have exactly one source — but we expose
        # the same ``sources`` attribute as GeneratedSpec so
        # downstream code (RestrictedCorpus construction, refresh's
        # is_stable predicate) treats both kinds uniformly.
        return (self.anchor_id,)


@dataclass(frozen=True)
class GeneratedSpec:
    """Spec for an LLM-generated fragment grounded in declared sources.

    The synthesizer constructs an ``LLMClient`` prompt from
    ``system_prompt`` + ``task_prompt`` (the latter SHOULD render
    the declared sources via the standard helper, but the framework
    does not enforce a particular template — planners own their
    prompts).

    ``sources`` is the hermetic input set; the synthesizer wraps
    the corpus with ``RestrictedCorpus(allowed_ids=sources)`` before
    invoking the planner-supplied prompt build, so declarations and
    actual reads stay in sync.
    """

    id: str
    sources: tuple[str, ...]
    system_prompt: str
    task_prompt: str
    display_label: str = ""

    @property
    def kind(self) -> str:
        return "generated"


FragmentSpec = Union[QuoteSpec, GeneratedSpec]
"""The discriminated union of fragment specs. ``isinstance`` on
:class:`QuoteSpec` / :class:`GeneratedSpec` is the canonical
dispatch."""


# ---------------------------------------------------------------------------
# Relation output template contracts
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ViewRelationColumn:
    """Declared column in a relation-backed view output."""

    name: str
    label: str = ""
    data_type: str = ""
    required: bool = True

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError("ViewRelationColumn requires a non-empty name")

    def to_contract(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "label": self.label or self.name,
            "data_type": self.data_type,
            "required": bool(self.required),
        }


def _relation_column_contract(
    column: str | ViewRelationColumn,
) -> dict[str, Any]:
    if isinstance(column, ViewRelationColumn):
        return column.to_contract()
    column_name = str(column)
    if not column_name:
        raise ValueError("sql_relation columns must be non-empty")
    return {
        "name": column_name,
        "label": column_name,
        "data_type": "",
        "required": True,
    }


# ---------------------------------------------------------------------------
# Planner protocol
# ---------------------------------------------------------------------------


@runtime_checkable
class PlanFn(Protocol):
    """Callable signature for a view planner.

    Planners take a corpus (plain dict of anchors) and a
    :class:`ViewDefinition` and return an ordered list of
    FragmentSpecs. The order is the order fragments will appear in
    the assembled view.

    Determinism rule (umbrella invariant): for a given corpus +
    view_def, two runs of the same planner across independent
    Python interpreters MUST produce identical FragmentSpec lists.
    Stage 5 of ``add-incremental-views`` enforces this with a
    cross-interpreter test.
    """

    def __call__(
        self,
        corpus: dict[str, Anchor],
        view_def: "ViewDefinition",
    ) -> list[FragmentSpec]:  # pragma: no cover - Protocol stub
        raise NotImplementedError


# ---------------------------------------------------------------------------
# Assembler protocol
# ---------------------------------------------------------------------------


# Importing View at type-check time only would let us use a
# Protocol here too, but a plain Callable[[View], str] is simpler
# and aligns with the existing demo's style.

# Actual signature: (View) -> str. We don't import View here to
# avoid a circular dependency (View has no dependency on this
# module, and we want to keep it that way). Callers annotate with
# the concrete type.

# (No further code needed — assembly.py owns the default assembler
# implementation; ViewDefinition just stores a Callable reference.)


# ---------------------------------------------------------------------------
# ViewDefinition
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ViewOutputTemplate:
    """Declarative output slot requested by a view definition.

    The views layer declares *what* slots a view wants and where the
    payload should come from; the substrate publisher decides how to
    render and persist the slot. This keeps modality growth out of the
    publisher's ad hoc conditionals while preserving the old default
    markdown + JSON manifest behavior.
    """

    slot_name: str
    source: str
    media_type: str
    artifact_type: str
    renderer_ref: str
    output_kind: str
    required: bool = True
    metadata: Mapping[str, Any] = field(default_factory=dict)
    config: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.slot_name:
            raise ValueError("ViewOutputTemplate requires a slot_name")
        if not self.source:
            raise ValueError(
                f"ViewOutputTemplate({self.slot_name!r}) requires a source"
            )
        if not self.media_type:
            raise ValueError(
                f"ViewOutputTemplate({self.slot_name!r}) requires a media_type"
            )
        if not self.artifact_type:
            raise ValueError(
                f"ViewOutputTemplate({self.slot_name!r}) requires an "
                "artifact_type"
            )
        if not self.renderer_ref:
            raise ValueError(
                f"ViewOutputTemplate({self.slot_name!r}) requires a "
                "renderer_ref"
            )
        if not self.output_kind:
            raise ValueError(
                f"ViewOutputTemplate({self.slot_name!r}) requires an "
                "output_kind"
            )

    @classmethod
    def markdown(cls) -> "ViewOutputTemplate":
        return cls(
            slot_name="markdown",
            source="assembled_markdown",
            media_type="text/markdown",
            artifact_type="okg/view-markdown-v1",
            renderer_ref="okg.renderer.markdown.v1",
            output_kind="markdown",
        )

    @classmethod
    def native_json(cls) -> "ViewOutputTemplate":
        return cls(
            slot_name="json",
            source="native_view_manifest",
            media_type="application/json",
            artifact_type="okg/view-json-v1",
            renderer_ref="okg.renderer.json.v1",
            output_kind="json",
        )

    @classmethod
    def view_graph_json(cls) -> "ViewOutputTemplate":
        return cls(
            slot_name="view_graph",
            source="view_graph_json",
            media_type="application/vnd.okg.view-graph+json",
            artifact_type="okg/view-graph-json-v1",
            renderer_ref="okg.renderer.view_graph.json.v1",
            output_kind="view_graph",
            metadata={"modality": "view_graph"},
        )

    @classmethod
    def fragment_json_tables(
        cls,
        *,
        required: bool = False,
    ) -> "ViewOutputTemplate":
        return cls(
            slot_name="table",
            source="fragment_json_tables",
            media_type="application/vnd.okg.table+json",
            artifact_type="okg/view-table-json-v1",
            renderer_ref="okg.renderer.table.json.v1",
            output_kind="table",
            required=required,
            metadata={"modality": "table"},
        )

    @classmethod
    def sql_relation(
        cls,
        *,
        logical_relation_id: str,
        sql: str,
        slot_name: str = "relation",
        title: str = "",
        columns: tuple[str | ViewRelationColumn, ...] = (),
        scope_key: str = "",
        scope_key_column: str = "",
        row_key_column: str = "row_key",
        row_key_columns: tuple[str, ...] = (),
        allow_ordinal_row_key: bool = False,
        sql_params: Mapping[str, Any] | None = None,
        required: bool = True,
        timeout_ms: int = 5000,
        direct_rows_key: str = "",
    ) -> "ViewOutputTemplate":
        if not logical_relation_id:
            raise ValueError("sql_relation template requires logical_relation_id")
        if not sql.strip():
            raise ValueError("sql_relation template requires non-empty sql")
        if timeout_ms <= 0:
            raise ValueError("sql_relation template requires positive timeout_ms")
        if scope_key and scope_key_column:
            raise ValueError(
                "sql_relation template accepts either scope_key or "
                "scope_key_column, not both"
            )
        if row_key_column and row_key_columns:
            raise ValueError(
                "sql_relation template accepts either row_key_column or "
                "row_key_columns, not both"
            )
        if (
            not row_key_column
            and not row_key_columns
            and not allow_ordinal_row_key
        ):
            raise ValueError(
                "sql_relation template requires row_key_column, "
                "row_key_columns, or allow_ordinal_row_key=True"
            )
        column_contracts = [
            _relation_column_contract(column)
            for column in columns
        ]
        column_names = [contract["name"] for contract in column_contracts]
        if scope_key_column:
            scope = {"policy": "column", "column": scope_key_column}
        elif scope_key:
            scope = {"policy": "constant", "scope_key": scope_key}
        else:
            scope = {"policy": "view_slot"}
        if row_key_column:
            row_identity = {
                "policy": "column",
                "column": row_key_column,
            }
        elif row_key_columns:
            cleaned_columns = [str(column) for column in row_key_columns]
            if any(not column for column in cleaned_columns):
                raise ValueError("row_key_columns must be non-empty")
            row_identity = {
                "policy": "columns_hash",
                "columns": cleaned_columns,
            }
        else:
            row_identity = {"policy": "ordinal"}
        return cls(
            slot_name=slot_name,
            source="sql_relation",
            media_type="application/vnd.okg.relation+json",
            artifact_type="okg/view-relation-reference-v1",
            renderer_ref="okg.renderer.sql_relation.table.v1",
            output_kind="relation",
            required=required,
            metadata={"modality": "relation", "title": title},
            config={
                "logical_relation_id": logical_relation_id,
                "sql": sql,
                "columns": column_names,
                "column_contracts": column_contracts,
                "scope": scope,
                "scope_key": scope_key,
                "scope_key_column": scope_key_column,
                "row_identity": row_identity,
                "row_key_column": row_key_column,
                "row_key_columns": list(row_key_columns),
                "allow_ordinal_row_key": bool(allow_ordinal_row_key),
                "sql_params": dict(sql_params or {}),
                "timeout_ms": int(timeout_ms),
                "direct_rows_key": str(direct_rows_key or ""),
            },
        )

    def to_contract(self) -> dict[str, Any]:
        return {
            "slot_name": self.slot_name,
            "source": self.source,
            "media_type": self.media_type,
            "artifact_type": self.artifact_type,
            "renderer_ref": self.renderer_ref,
            "output_kind": self.output_kind,
            "required": bool(self.required),
            "metadata": dict(self.metadata),
            "config": dict(self.config),
        }


def default_output_templates() -> tuple[ViewOutputTemplate, ...]:
    """Return the default native view outputs: markdown + JSON manifest."""

    return (
        ViewOutputTemplate.markdown(),
        ViewOutputTemplate.native_json(),
    )


@dataclass
class ViewDefinition:
    """Runtime descriptor for one registered view.

    A view is identified by ``view_id``; its planner produces the
    fragment spec list, and its assembler renders the view to
    markdown (or other text). ``params`` carries planner-specific
    arguments (``opr``, ``limit``, etc.) — the planner reads them
    via ``view_def.params``.

    Not frozen: ``params`` is a dict and we allow callers to update
    it (e.g. via the CLI's `-o key=value` flags). Once constructed
    for a refresh run, treat it as immutable.

    Notes:

    * ``planner`` and ``assembler`` are callable references —
      Python objects, not import strings. The substrate-side
      ``view_definition`` graph node carries an import string
      (``planner_ref``) which is resolved to a callable when the
      runtime ViewDefinition is constructed.

    * ``view_def_hash`` lets refresh detect when a planner's
      identity / parameters changed across runs (a different
      planner producing the same fragment ids would otherwise
      silently reuse cached fragments). The hash discipline is
      planner-author responsibility — typically
      ``hash((planner.__qualname__, sorted(params.items())))`` or
      similar — and the framework treats it as opaque.

    * ``output_templates`` declares the typed slots the substrate
      publisher should render. The default is the native view pair:
      markdown for display and JSON for the canonical view manifest.
    """

    view_id: str
    planner: PlanFn
    assembler: Callable[[Any], str]
    params: dict[str, Any] = field(default_factory=dict)
    view_def_hash: str = ""
    display_name: str = ""
    output_templates: tuple[ViewOutputTemplate, ...] = field(
        default_factory=default_output_templates
    )

    def __post_init__(self) -> None:
        if not self.view_id:
            raise ValueError("ViewDefinition requires a non-empty view_id")
        if self.planner is None:
            raise ValueError(
                f"ViewDefinition({self.view_id!r}) requires a planner"
            )
        if self.assembler is None:
            raise ValueError(
                f"ViewDefinition({self.view_id!r}) requires an assembler"
            )
        templates = tuple(self.output_templates)
        if not templates:
            raise ValueError(
                f"ViewDefinition({self.view_id!r}) requires at least one "
                "output template"
            )
        slot_names = [template.slot_name for template in templates]
        duplicate_slots = sorted({
            slot_name
            for slot_name in slot_names
            if slot_names.count(slot_name) > 1
        })
        if duplicate_slots:
            raise ValueError(
                f"ViewDefinition({self.view_id!r}) has duplicate output "
                f"template slots: {duplicate_slots}"
            )
        self.output_templates = templates


__all__ = [
    "QuoteSpec",
    "GeneratedSpec",
    "FragmentSpec",
    "PlanFn",
    "ViewRelationColumn",
    "ViewOutputTemplate",
    "ViewDefinition",
    "default_output_templates",
]
