"""Declarative page-graph primitives for native OKG views."""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Mapping

from okg.substrate import fastyaml

from okg.views.assembly import MarkeredAssembly
from okg.views.planners.base import QuoteSpec, ViewDefinition
from okg.views.types import Anchor, View


VIEW_GRAPH_MANIFEST_FRAGMENT_ID = "__okg_view_graph_manifest__"
VIEW_GRAPH_MANIFEST_DISPLAY_LABEL = "okg_view_graph_manifest"
VIEW_GRAPH_MEDIA_TYPE = "application/vnd.okg.view-graph+json"
VIEW_GRAPH_ARTIFACT_TYPE = "okg/view-graph-json-v1"


@dataclass(frozen=True)
class FlatPageSpec:
    page_id: str
    title: str
    route: str
    collection: str
    title_field: str
    detail_fields: tuple[str, ...] = ()
    limit: int | None = None


@dataclass(frozen=True)
class NeighborhoodPageSpec:
    collection: str
    route_prefix: str
    title_field: str
    slug_field: str
    include_edge_types: tuple[str, ...] = ("references", "mentions", "derived_from", "contains")
    page_id_prefix: str = ""


@dataclass(frozen=True)
class ViewGraphSpec:
    view_id: str
    title: str
    export_mode: str
    artifact_policy: str
    flat_pages: tuple[FlatPageSpec, ...] = ()
    neighborhood_pages: tuple[NeighborhoodPageSpec, ...] = ()
    version: int = 1

    def stable_hash(self) -> str:
        return stable_hash(self.to_dict())

    def to_dict(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "view_id": self.view_id,
            "title": self.title,
            "export_mode": self.export_mode,
            "artifact_policy": self.artifact_policy,
            "flat_pages": [
                {
                    "page_id": page.page_id,
                    "title": page.title,
                    "route": page.route,
                    "collection": page.collection,
                    "title_field": page.title_field,
                    "detail_fields": list(page.detail_fields),
                    "limit": page.limit,
                }
                for page in self.flat_pages
            ],
            "neighborhood_pages": [
                {
                    "collection": page.collection,
                    "route_prefix": page.route_prefix,
                    "title_field": page.title_field,
                    "slug_field": page.slug_field,
                    "include_edge_types": list(page.include_edge_types),
                    "page_id_prefix": page.page_id_prefix,
                }
                for page in self.neighborhood_pages
            ],
        }


@dataclass(frozen=True)
class ViewGraphNode:
    node_id: str
    node_kind: str
    title: str
    route: str = ""
    source_node_id: str = ""
    attrs: Mapping[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "node_id": self.node_id,
            "node_kind": self.node_kind,
            "title": self.title,
            "route": self.route,
            "source_node_id": self.source_node_id,
            "attrs": dict(self.attrs),
        }


@dataclass(frozen=True)
class ViewGraphEdge:
    src: str
    dst: str
    edge_type: str
    attrs: Mapping[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "src": self.src,
            "dst": self.dst,
            "edge_type": self.edge_type,
            "attrs": dict(self.attrs),
        }


@dataclass(frozen=True)
class ViewGraph:
    view_id: str
    title: str
    generation_id: int | None
    export_mode: str
    artifact_policy: str
    nodes: tuple[ViewGraphNode, ...]
    edges: tuple[ViewGraphEdge, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "view_id": self.view_id,
            "title": self.title,
            "generation_id": self.generation_id,
            "export_mode": self.export_mode,
            "artifact_policy": self.artifact_policy,
            "nodes": [node.to_dict() for node in self.nodes],
            "edges": [edge.to_dict() for edge in self.edges],
        }


def stable_hash(value: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()[:16]


def load_view_graph_spec(path: Path | str) -> ViewGraphSpec:
    spec_path = Path(path).expanduser().resolve()
    payload = fastyaml.safe_load(spec_path.read_text(encoding="utf-8")) or {}
    if not isinstance(payload, Mapping):
        raise ValueError(f"view graph spec {spec_path} must be a mapping")
    version = int(payload.get("version") or 1)
    if version != 1:
        raise ValueError(f"view graph spec {spec_path} version {version!r} is not supported")
    view_id = _required_str(payload, "view_id", spec_path)
    title = _required_str(payload, "title", spec_path)
    export_mode = _required_str(payload, "export_mode", spec_path)
    artifact_policy = _required_str(payload, "artifact_policy", spec_path)
    flat_pages = tuple(
        _flat_page_from_raw(item, spec_path=spec_path, index=index)
        for index, item in enumerate(payload.get("flat_pages") or [])
    )
    neighborhood_pages = tuple(
        _neighborhood_page_from_raw(item, spec_path=spec_path, index=index)
        for index, item in enumerate(payload.get("neighborhood_pages") or [])
    )
    page_ids = [page.page_id for page in flat_pages]
    duplicate_ids = sorted({page_id for page_id in page_ids if page_ids.count(page_id) > 1})
    if duplicate_ids:
        raise ValueError(f"view graph spec {spec_path} duplicate flat page ids: {duplicate_ids}")
    if not flat_pages and not neighborhood_pages:
        raise ValueError(f"view graph spec {spec_path} must declare at least one page")
    return ViewGraphSpec(
        version=version,
        view_id=view_id,
        title=title,
        export_mode=export_mode,
        artifact_policy=artifact_policy,
        flat_pages=flat_pages,
        neighborhood_pages=neighborhood_pages,
    )


def build_collection_view_graph(
    *,
    spec: ViewGraphSpec,
    generation_id: int | None,
    collections: Mapping[str, Iterable[Mapping[str, Any]]],
    edges: Iterable[Mapping[str, Any]],
) -> ViewGraph:
    edge_rows = [dict(edge) for edge in edges]
    node_by_id = _node_index(collections)
    adjacency = _edge_index(edge_rows)
    graph_nodes: dict[str, ViewGraphNode] = {
        f"view:{spec.view_id}": ViewGraphNode(
            node_id=f"view:{spec.view_id}",
            node_kind="view",
            title=spec.title,
            attrs={"export_mode": spec.export_mode, "artifact_policy": spec.artifact_policy},
        )
    }
    graph_edges: list[ViewGraphEdge] = []

    for page in spec.flat_pages:
        page_node_id = f"page:{spec.view_id}:{page.page_id}"
        rows = [dict(row) for row in collections.get(page.collection, [])]
        if page.limit is not None:
            rows = rows[:page.limit]
        graph_nodes[page_node_id] = ViewGraphNode(
            node_id=page_node_id,
            node_kind="page",
            title=page.title,
            route=page.route,
            attrs={
                "collection": page.collection,
                "row_count": len(rows),
                "page_kind": "collection",
            },
        )
        graph_edges.append(ViewGraphEdge(
            src=f"view:{spec.view_id}",
            dst=page_node_id,
            edge_type="contains",
        ))
        for row in rows:
            entity_id = _entity_view_node_id(spec.view_id, row)
            graph_nodes.setdefault(entity_id, _entity_node(spec.view_id, row))
            graph_edges.append(ViewGraphEdge(
                src=page_node_id,
                dst=entity_id,
                edge_type="contains",
                attrs={"collection": page.collection},
            ))

    for page_spec in spec.neighborhood_pages:
        for row in collections.get(page_spec.collection, []):
            root = dict(row)
            slug = str(root.get(page_spec.slug_field) or root.get("node_id") or "")
            if not slug:
                continue
            page_id_prefix = page_spec.page_id_prefix or page_spec.collection.rstrip("s")
            page_id = f"{page_id_prefix}:{slug}"
            route = f"{page_spec.route_prefix.strip('/')}/{slug}"
            page_node_id = f"page:{spec.view_id}:{page_id}"
            root_node_id = str(root.get("node_id") or "")
            neighborhood_edges = [
                edge
                for edge in adjacency.get(root_node_id, [])
                if str(edge.get("edge_type") or "") in page_spec.include_edge_types
            ]
            neighbor_ids = {
                str(edge.get("src") or "")
                for edge in neighborhood_edges
                if str(edge.get("src") or "") != root_node_id
            } | {
                str(edge.get("dst") or "")
                for edge in neighborhood_edges
                if str(edge.get("dst") or "") != root_node_id
            }
            rows = [
                node_by_id[node_id]
                for node_id in sorted(neighbor_ids)
                if node_id in node_by_id
            ]
            graph_nodes[page_node_id] = ViewGraphNode(
                node_id=page_node_id,
                node_kind="page",
                title=str(root.get(page_spec.title_field) or root.get("node_id") or slug),
                route=route,
                source_node_id=root_node_id,
                attrs={
                    "collection": page_spec.collection,
                    "row_count": len(rows),
                    "page_kind": "neighborhood",
                },
            )
            graph_edges.append(ViewGraphEdge(
                src=f"view:{spec.view_id}",
                dst=page_node_id,
                edge_type="contains",
            ))
            root_entity_id = _entity_view_node_id(spec.view_id, root)
            graph_nodes.setdefault(root_entity_id, _entity_node(spec.view_id, root))
            graph_edges.append(ViewGraphEdge(
                src=page_node_id,
                dst=root_entity_id,
                edge_type="focuses_on",
                attrs={"collection": page_spec.collection},
            ))
            for neighbor in rows:
                entity_id = _entity_view_node_id(spec.view_id, neighbor)
                graph_nodes.setdefault(entity_id, _entity_node(spec.view_id, neighbor))
                graph_edges.append(ViewGraphEdge(
                    src=page_node_id,
                    dst=entity_id,
                    edge_type="references",
                    attrs={"source": "graph_neighborhood"},
                ))
            for edge in neighborhood_edges:
                src_id = _entity_view_node_id_for_source(spec.view_id, str(edge.get("src") or ""))
                dst_id = _entity_view_node_id_for_source(spec.view_id, str(edge.get("dst") or ""))
                if src_id in graph_nodes and dst_id in graph_nodes:
                    graph_edges.append(ViewGraphEdge(
                        src=src_id,
                        dst=dst_id,
                        edge_type=str(edge.get("edge_type") or "references"),
                        attrs={
                            key: value
                            for key, value in dict(edge).items()
                            if key not in {"src", "dst", "edge_type"}
                        },
                    ))

    nodes = tuple(sorted(graph_nodes.values(), key=lambda node: node.node_id))
    deduped_edges = _dedupe_edges(graph_edges)
    return ViewGraph(
        view_id=spec.view_id,
        title=spec.title,
        generation_id=generation_id,
        export_mode=spec.export_mode,
        artifact_policy=spec.artifact_policy,
        nodes=nodes,
        edges=deduped_edges,
    )


def view_graph_to_corpus(graph: ViewGraph) -> dict[str, Anchor]:
    payload = json.dumps(graph.to_dict(), indent=2, sort_keys=True)
    anchors = {
        _manifest_anchor_id(graph.view_id): Anchor.from_content(
            _manifest_anchor_id(graph.view_id),
            payload,
        )
    }
    for node in graph.nodes:
        if node.node_kind != "page":
            continue
        anchors[_page_anchor_id(graph.view_id, node.node_id)] = Anchor.from_content(
            _page_anchor_id(graph.view_id, node.node_id),
            render_page_markdown(graph, node),
        )
    return anchors


def view_graph_planner(
    corpus: dict[str, Anchor],
    view_def: ViewDefinition,
) -> list[QuoteSpec]:
    view_id = view_def.view_id
    specs = [
        QuoteSpec(
            id=VIEW_GRAPH_MANIFEST_FRAGMENT_ID,
            anchor_id=_manifest_anchor_id(view_id),
            display_label=VIEW_GRAPH_MANIFEST_DISPLAY_LABEL,
        )
    ]
    for anchor_id in sorted(corpus):
        if anchor_id == _manifest_anchor_id(view_id):
            continue
        specs.append(QuoteSpec(
            id=f"page:{anchor_id}",
            anchor_id=anchor_id,
            display_label="view_graph_page",
        ))
    return specs


def view_graph_assembler(view: View):
    builder = MarkeredAssembly()
    first = True
    for fragment in view.fragments:
        if fragment.display_label == VIEW_GRAPH_MANIFEST_DISPLAY_LABEL:
            continue
        if not first:
            builder.write("\n\n---\n\n")
        first = False
        builder.write_fragment(fragment.id, fragment.content)
    return builder.build()


def view_graph_payload_from_view(view: View) -> dict[str, Any]:
    for fragment in view.fragments:
        if fragment.display_label == VIEW_GRAPH_MANIFEST_DISPLAY_LABEL:
            payload = json.loads(fragment.content)
            if not isinstance(payload, dict):
                raise ValueError("view graph manifest fragment must contain a JSON object")
            return payload
    raise ValueError(f"view {view.view_id!r} has no view graph manifest fragment")


def render_page_markdown(graph: ViewGraph, page: ViewGraphNode) -> str:
    lines = [
        f"# {page.title}",
        "",
        f"- View: `{graph.view_id}`",
        f"- Generation: `{graph.generation_id if graph.generation_id is not None else 'unpublished'}`",
        f"- Export mode: `{graph.export_mode}`",
        f"- Artifact policy: `{graph.artifact_policy}`",
        "",
    ]
    linked_nodes = [
        node
        for node in _page_linked_nodes(graph, page.node_id)
        if node.node_kind == "entity"
    ]
    lines.append("## Linked Nodes")
    if not linked_nodes:
        lines.append("- None.")
    for node in linked_nodes[:250]:
        source = f"; source={node.source_node_id}" if node.source_node_id else ""
        lines.append(f"- {node.title} ({node.attrs.get('subtype', 'entity')}{source})")
    return "\n".join(lines) + "\n"


def _page_linked_nodes(graph: ViewGraph, page_node_id: str) -> list[ViewGraphNode]:
    node_by_id = {node.node_id: node for node in graph.nodes}
    rows = []
    for edge in graph.edges:
        if edge.src == page_node_id and edge.dst in node_by_id:
            rows.append(node_by_id[edge.dst])
    return sorted(rows, key=lambda node: (node.node_kind, node.title, node.node_id))


def _manifest_anchor_id(view_id: str) -> str:
    return f"view-graph:{view_id}:manifest"


def _page_anchor_id(view_id: str, page_node_id: str) -> str:
    return f"view-graph:{view_id}:{page_node_id}"


def _entity_view_node_id(view_id: str, row: Mapping[str, Any]) -> str:
    return _entity_view_node_id_for_source(view_id, str(row.get("node_id") or ""))


def _entity_view_node_id_for_source(view_id: str, source_node_id: str) -> str:
    return f"entity:{view_id}:{source_node_id}"


def _entity_node(view_id: str, row: Mapping[str, Any]) -> ViewGraphNode:
    title = _row_title(row)
    return ViewGraphNode(
        node_id=_entity_view_node_id(view_id, row),
        node_kind="entity",
        title=title,
        source_node_id=str(row.get("node_id") or ""),
        attrs={
            key: value
            for key, value in dict(row).items()
            if key not in {"node_id"}
        },
    )


def _node_index(
    collections: Mapping[str, Iterable[Mapping[str, Any]]],
) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for collection_name, rows in collections.items():
        subtype = _subtype_for_collection(collection_name)
        for row in rows:
            item = dict(row)
            item.setdefault("subtype", subtype)
            node_id = str(item.get("node_id") or "")
            if node_id:
                out[node_id] = item
    return out


def _edge_index(edges: Iterable[Mapping[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    index: dict[str, list[dict[str, Any]]] = {}
    for edge in edges:
        item = dict(edge)
        src = str(item.get("src") or "")
        dst = str(item.get("dst") or "")
        if src:
            index.setdefault(src, []).append(item)
        if dst:
            index.setdefault(dst, []).append(item)
    return index


def _dedupe_edges(edges: Iterable[ViewGraphEdge]) -> tuple[ViewGraphEdge, ...]:
    seen: set[tuple[str, str, str, str]] = set()
    out: list[ViewGraphEdge] = []
    for edge in edges:
        key = (
            edge.src,
            edge.dst,
            edge.edge_type,
            json.dumps(dict(edge.attrs), sort_keys=True, default=str),
        )
        if key in seen:
            continue
        seen.add(key)
        out.append(edge)
    return tuple(sorted(out, key=lambda edge: (edge.src, edge.edge_type, edge.dst)))


def _row_title(row: Mapping[str, Any]) -> str:
    for field_name in ("title", "name", "display_name", "source_family", "node_id"):
        value = row.get(field_name)
        if value:
            return str(value)
    return ""


def _subtype_for_collection(collection_name: str) -> str:
    return {
        "organizations": "organization",
        "people": "person",
        "research_areas": "research_area",
        "projects": "lab_project",
        "publications": "paper",
        "software": "software",
        "resources": "document",
        "benchmarks": "benchmark",
        "datasets": "dataset",
        "courses": "course",
        "project_coverage": "project_coverage",
        "quality_findings": "graph_quality_finding",
        "news": "document",
        "source_observations": "source_observation",
        "source_registry_entries": "lab_source_registry_entry",
    }.get(collection_name, collection_name.rstrip("s"))


def _required_str(payload: Mapping[str, Any], field_name: str, path: Path) -> str:
    value = payload.get(field_name)
    if not isinstance(value, str) or not value:
        raise ValueError(f"view graph spec {path} requires non-empty {field_name!r}")
    return value


def _flat_page_from_raw(raw: Any, *, spec_path: Path, index: int) -> FlatPageSpec:
    if not isinstance(raw, Mapping):
        raise ValueError(f"view graph spec {spec_path} flat_pages[{index}] must be a mapping")
    return FlatPageSpec(
        page_id=_required_str(raw, "page_id", spec_path),
        title=_required_str(raw, "title", spec_path),
        route=_required_str(raw, "route", spec_path),
        collection=_required_str(raw, "collection", spec_path),
        title_field=_required_str(raw, "title_field", spec_path),
        detail_fields=tuple(str(item) for item in raw.get("detail_fields") or ()),
        limit=int(raw["limit"]) if raw.get("limit") is not None else None,
    )


def _neighborhood_page_from_raw(
    raw: Any,
    *,
    spec_path: Path,
    index: int,
) -> NeighborhoodPageSpec:
    if not isinstance(raw, Mapping):
        raise ValueError(f"view graph spec {spec_path} neighborhood_pages[{index}] must be a mapping")
    return NeighborhoodPageSpec(
        collection=_required_str(raw, "collection", spec_path),
        route_prefix=_required_str(raw, "route_prefix", spec_path),
        title_field=_required_str(raw, "title_field", spec_path),
        slug_field=_required_str(raw, "slug_field", spec_path),
        include_edge_types=tuple(
            str(item)
            for item in raw.get("include_edge_types") or (
                "references",
                "mentions",
                "derived_from",
                "contains",
            )
        ),
        page_id_prefix=str(raw.get("page_id_prefix") or ""),
    )


__all__ = [
    "FlatPageSpec",
    "NeighborhoodPageSpec",
    "VIEW_GRAPH_ARTIFACT_TYPE",
    "VIEW_GRAPH_MANIFEST_DISPLAY_LABEL",
    "VIEW_GRAPH_MANIFEST_FRAGMENT_ID",
    "VIEW_GRAPH_MEDIA_TYPE",
    "ViewGraph",
    "ViewGraphEdge",
    "ViewGraphNode",
    "ViewGraphSpec",
    "build_collection_view_graph",
    "load_view_graph_spec",
    "stable_hash",
    "view_graph_assembler",
    "view_graph_payload_from_view",
    "view_graph_planner",
    "view_graph_to_corpus",
]
