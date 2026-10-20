"""Schema refusals that carry the legal vocabulary, sourced from the model.

Raw pydantic serialization names the offending PATH and the rule it broke
("Input should be 'blocks', 'requires', ... [type=literal_error]" collapses to
`literal_error` on `dependencies[14].kind` once it reaches the CLI's error
block) and never the legal set an author has to choose from. Every audited
session answered that by grepping `pact.schema.yaml` or copying a sibling
PACT — 3 to 16 calls per manifest.

The vocabulary here is READ OFF THE PYDANTIC MODEL, never restated: a new
`Literal` member or a renamed field shows up in the refusal the moment it
lands, so this cannot rot into a second, stale schema.

Per `pact-self-repairing-refusals` req.refusal.complete.
"""

from __future__ import annotations

from typing import Any, Literal, Union, get_args, get_origin

from okg.substrate.pact.models import PactManifest


def _unwrap_optional(annotation: Any) -> Any:
    origin = get_origin(annotation)
    if origin is Union or str(origin) == "types.UnionType":
        args = [arg for arg in get_args(annotation) if arg is not type(None)]
        if len(args) == 1:
            return args[0]
    return annotation


def _fields(model: Any) -> dict[str, Any]:
    return getattr(model, "model_fields", {}) or {}


def _annotation_at(model: Any, name: str) -> Any:
    field = _fields(model).get(name)
    return None if field is None else _unwrap_optional(field.annotation)


def _element_type(annotation: Any) -> Any:
    args = get_args(annotation)
    return _unwrap_optional(args[0]) if args else None


def resolve_model(location: tuple[Any, ...], root: Any = PactManifest) -> Any | None:
    """The model class that owns ``location``, walking the real annotations."""
    current: Any = root
    for part in location:
        if current is None:
            return None
        if isinstance(part, int):
            current = _element_type(current)
            continue
        if _fields(current):
            current = _annotation_at(current, str(part))
            continue
        return None
    return current


def _literal_members(annotation: Any) -> list[str]:
    if get_origin(annotation) is Literal:
        return [str(member) for member in get_args(annotation)]
    return []


def _shape_of(annotation: Any) -> str | None:
    if annotation is None:
        return None
    if _fields(annotation):
        return f"mapping with keys: {', '.join(sorted(_fields(annotation)))}"
    origin = get_origin(annotation)
    if origin in (list, tuple, set):
        element = _element_type(annotation)
        name = getattr(element, "__name__", str(element)) if element else "value"
        return f"list of {name}"
    if origin is dict:
        return "mapping"
    return getattr(annotation, "__name__", str(annotation))


def explain(errors: list[dict[str, Any]], *, root: Any = PactManifest) -> list[dict[str, Any]]:
    """Add the offending value and the legal set to each pydantic error."""
    explained: list[dict[str, Any]] = []
    for error in errors or []:
        location = tuple(error.get("loc") or ())
        kind = str(error.get("type") or "")
        row: dict[str, Any] = {
            "code": kind,
            "path": ".".join(str(part) for part in location),
            "message": str(error.get("msg") or ""),
        }
        if "input" in error:
            row["offending_value"] = error["input"]
        owner = resolve_model(location[:-1], root=root)
        leaf = str(location[-1]) if location else None
        if kind == "extra_forbidden":
            row["offending_key"] = leaf
            legal = sorted(_fields(owner)) if owner is not None else []
            row["legal_values"] = legal
            if legal:
                row["message"] = (
                    f"unknown key {leaf!r}; this block accepts: {', '.join(legal)}"
                )
        elif kind.startswith("literal_error"):
            annotation = (
                _annotation_at(owner, leaf) if owner is not None and leaf else None
            )
            members = _literal_members(annotation)
            if not members:
                # Pydantic already computed the set; parse it rather than
                # report nothing.
                expected = (error.get("ctx") or {}).get("expected")
                if isinstance(expected, str):
                    members = [
                        part.strip().strip("'\"")
                        for part in expected.replace(" or ", ", ").split(",")
                        if part.strip()
                    ]
            row["legal_values"] = members
            if members:
                row["message"] = (
                    f"{row['path']}: {error.get('input')!r} is not a legal value; "
                    f"legal values: {', '.join(members)}"
                )
        elif kind == "missing":
            row["required_field"] = leaf
            shape = _shape_of(_annotation_at(owner, leaf) if owner and leaf else None)
            row["expected_shape"] = shape
            row["message"] = f"required key {leaf!r} is absent" + (
                f" (expected {shape})" if shape else ""
            )
        else:
            annotation = (
                _annotation_at(owner, leaf)
                if owner is not None and leaf and not leaf.isdigit()
                else resolve_model(location, root=root)
            )
            shape = _shape_of(annotation)
            if shape:
                row["expected_shape"] = shape
                row["message"] = (
                    f"{row['path']}: wrong shape — expected {shape}, got "
                    f"{type(error.get('input')).__name__}"
                )
        explained.append(row)
    return explained


def summary(explained: list[dict[str, Any]], *, limit: int = 3) -> str:
    """One line an agent can act on without a second call."""
    if not explained:
        return "manifest failed schema validation"
    parts = [row["message"] for row in explained[:limit] if row.get("message")]
    if len(explained) > limit:
        parts.append(f"(+{len(explained) - limit} more)")
    return "; ".join(parts)
