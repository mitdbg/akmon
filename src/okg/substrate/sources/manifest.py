"""Source manifest: compile and map JSON, JSONL, CSV and YAML records.

Phase 1 reads local files; phase 2 adds JSON APIs (`read:` with a
`connection:` or `url:`), several top-level types each with its own read,
`"*"` object records, `changes_when` and `max_deleted_fraction`. The HTTP
client itself lives in `okg.substrate.sources.manifest_http`.

A source manifest (`okg.source/v1`) is one YAML file that says where a
source's records are and what each kind of record looks like. This module is
pure -- no database, no network -- and is shared by the generic reader
(`okg.substrate.library.sources.manifest_records`) and the
`okg deployment add-source --manifest` preview, so the two cannot disagree.

- `load_manifest` / `parse_manifest` validate the file and return a typed
  `Manifest`; every rule violation raises `ManifestError` naming the code, the
  offending manifest path, and a fix.
- `compile_manifest` turns a manifest into the strict source-registry
  registration for the generic reader, the generated LinkML schema and
  bridges files, and the output signature (exactly the declared types, links
  and passage types).
- `read_input_files` and `map_records` turn the matched files into mapped
  records, links and passages, with every problem the read found.

Design: pact/changes/no-code-source-manifest/design.md and
pact/changes/no-code-source-manifest-http/design.md.
"""
from __future__ import annotations

import csv
import functools
import glob as globlib
import hashlib
import io
import json
import math
import re
import unicodedata
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable, Mapping, Optional, Sequence
from urllib.parse import parse_qsl, quote, urljoin, urlsplit

import yaml

MANIFEST_VERSION = "okg.source/v1"
READER_MODULE = "okg.substrate.library.sources.manifest_records"
READER_CLASS = "ManifestRecordSource"
CONNECTOR_TYPE = "source-manifest"

DELETION_MODES = ("missing_from_full_read", "never")
DEFAULT_DELETION = "missing_from_full_read"
FORMATS = ("json", "jsonl", "csv", "yaml")
_EXTENSION_FORMATS = {
    ".json": "json", ".jsonl": "jsonl", ".ndjson": "jsonl", ".csv": "csv",
    ".yaml": "yaml", ".yml": "yaml",
}
#: Formats whose file holds one document that `read.records` can point into.
_DOCUMENT_FORMATS = ("json", "yaml")

FIELD_TYPES = ("string", "integer", "number", "boolean", "date", "datetime", "list")
_LINKML_RANGES = {
    "string": "string", "integer": "integer", "number": "float",
    "boolean": "boolean", "date": "date", "datetime": "datetime", "list": "string",
}
#: Field type changes a re-apply may make: the old values stay valid.
_WIDENINGS = {("integer", "number")} | {
    (kind, "string") for kind in FIELD_TYPES if kind not in ("string", "list")
}

#: `dates:` keys and the record attribute and catalog annotation each fills.
DATE_ATTRS = {
    "created": ("created_at", "okg_created_at_from"),
    "modified": ("last_modified_at", "okg_last_modified_at_from"),
    "published": ("published_at", "okg_published_at_from"),
    "resolved": ("resolved_at", "okg_resolved_at_from"),
    "effective": ("effective_at", "okg_effective_at_from"),
}
TITLE_ATTR = "title"
#: `text:` is stored as `body`: the searchable-text stamper owns `text`
#: (`attrs.text` is its rollup) and `okg_producer_text`.
BODY_ATTR = "body"
RESERVED_FIELD_NAMES = frozenset({
    TITLE_ATTR, BODY_ATTR, "text", "okg_producer_text",
    *(attr for attr, _ in DATE_ATTRS.values()),
})

#: Link names that mean containment (`catalog/relation_kind.py`).
CONTAINMENT_LINKS = frozenset({"contains", "has_part", "parent_of", "defines", "owns", "wraps"})

#: Text longer than this is split into passages with the window chunker at
#: the packaged default settings (`sources/guided_defaults/extractors.yaml`).
PASSAGE_THRESHOLD = 2000
CHUNK_MAX_CHARS = 1500
CHUNK_OVERLAP = 200
CHUNKER_IDENTITY = (
    "okg.substrate.library.extraction.chunk.window.WindowChunker"
    f"(max_chars={CHUNK_MAX_CHARS},overlap={CHUNK_OVERLAP})"
)
PASSAGE_SUFFIX = "_passage"
PASSAGE_EDGE = "contains"

#: `records: "*"` reads each value of a JSON object as one record; the
#: value's name is available to the mapping as `{_key}`.
OBJECT_RECORDS = "*"
OBJECT_KEY_FIELD = "_key"

#: Default share of the earlier records one run may remove, when any read is
#: HTTP. File-only sources have no default guard.
DEFAULT_HTTP_MAX_DELETED_FRACTION = 0.5
# A record with no key value is skipped; a read where more than this share of
# one type's records have none is refused, since the key path is likely wrong.
MAX_KEYLESS_FRACTION = 0.1
DEFAULT_MAX_PAGES = 1000

AUTH_MODES = ("none", "bearer", "header", "basic", "client_certificate")
#: Each auth mode's keys that name an environment variable (never a value).
AUTH_ENV_ROLES: Mapping[str, tuple[str, ...]] = {
    "none": (),
    "bearer": ("token",),
    "header": ("value",),
    "basic": ("user", "password"),
    "client_certificate": ("cert", "key", "ca_dir"),
}
PAGING_MODES = ("none", "offset", "page", "cursor", "link_header")
_ENV_NAME_RE = re.compile(r"^[A-Z_][A-Z0-9_]*$")
#: Query parameter names that carry a secret; a URL naming one is refused.
_SECRET_QUERY_NAMES = frozenset({
    "token", "access_token", "auth_token", "id_token", "refresh_token",
    "api_key", "apikey", "api-key", "key", "secret", "client_secret",
    "password", "passwd", "pwd", "sig", "signature", "auth", "authorization",
    "session", "sessionid", "session_id", "x-api-key",
})
_SECRET_QUERY_SUFFIXES = ("_token", "_secret", "_apikey", "_api_key", "_password", "-token", "-key")

GENERATED_MARK = "okg-generated: source-manifest"
_NAME_RE = re.compile(r"^[a-z][a-z0-9_]*$")
_PLACEHOLDER_RE = re.compile(r"\{([^{}]*)\}")
_SAMPLE_LIMIT = 10


# --------------------------------------------------------------------------
# Problems
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class ManifestProblem:
    """One thing wrong with a manifest or with the records it maps.

    `path` is the manifest path (`types.ticket.fields.votes`); `file` and
    `record` say where in the input the problem was found. A `blocking`
    problem stops the import; the others are reported and the import goes on.
    """

    code: str
    message: str
    fix: str
    path: Optional[str] = None
    file: Optional[str] = None
    record: Optional[str] = None
    blocking: bool = True

    def as_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {"code": self.code, "message": self.message, "fix": self.fix}
        for key in ("path", "file", "record"):
            value = getattr(self, key)
            if value is not None:
                out[key] = value
        out["blocking"] = self.blocking
        return out


class ManifestError(ValueError):
    """A manifest refused: `code`, the offending `path`, and a `fix`.

    `problems` holds every refusal found; the attributes describe the first.
    """

    def __init__(self, problems: Sequence[ManifestProblem]) -> None:
        if not problems:
            raise ValueError("ManifestError needs at least one problem")
        self.problems = list(problems)
        first = self.problems[0]
        self.code = first.code
        self.path = first.path
        self.fix = first.fix
        where = f" at {first.path}" if first.path else ""
        more = f" (and {len(self.problems) - 1} more)" if len(self.problems) > 1 else ""
        super().__init__(f"{first.message}{where}{more}")

    def as_dict(self) -> dict[str, Any]:
        return {"code": self.code, "path": self.path, "message": str(self),
                "fix": self.fix, "problems": [p.as_dict() for p in self.problems]}


def _refuse(code: str, message: str, fix: str, path: Optional[str] = None) -> ManifestError:
    return ManifestError([ManifestProblem(code, message, fix, path=path)])


# --------------------------------------------------------------------------
# Paths and templates
# --------------------------------------------------------------------------


_MISSING = object()


def resolve_path(record: Any, path: str) -> Any:
    """The value at a dotted `path`, or `_MISSING`. `.` is the record itself."""
    if path == ".":
        return record
    current = record
    for segment in path.split("."):
        if isinstance(current, Mapping):
            if segment not in current:
                return _MISSING
            current = current[segment]
        elif isinstance(current, list) and segment.isdigit():
            index = int(segment)
            if index >= len(current):
                return _MISSING
            current = current[index]
        else:
            return _MISSING
        if current is None:
            return _MISSING
    return current


def _valid_path(path: str) -> bool:
    if path == ".":
        return True
    return bool(path) and all(segment.strip() == segment and segment for segment in path.split("."))


@dataclass(frozen=True)
class Template:
    text: str
    placeholders: tuple[str, ...]

    def render(self, record: Any) -> Optional[str]:
        """The rendered key, or None when a placeholder has no usable value."""
        out: list[str] = []
        last = 0
        for match in _PLACEHOLDER_RE.finditer(self.text):
            out.append(self.text[last:match.start()])
            value = resolve_path(record, match.group(1).strip())
            rendered = _key_part(value)
            if rendered is None:
                return None
            out.append(rendered)
            last = match.end()
        out.append(self.text[last:])
        key = "".join(out)
        return key if key.strip() else None


def key_problem(key: str) -> Optional[str]:
    """Why a rendered key cannot be an identity, or None."""
    if any(unicodedata.category(ch) in ("Cc", "Zl", "Zp") for ch in key):
        return "it contains a control character or line break"
    if any(unicodedata.category(ch) == "Cf" for ch in key):
        return "it contains an invisible formatting character (zero-width or direction override)"
    return None


def _key_part(value: Any) -> Optional[str]:
    if value is _MISSING or isinstance(value, (Mapping, list)):
        return None
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, float):
        if not math.isfinite(value):
            return None
        return str(int(value)) if value.is_integer() else repr(value)
    text = str(value)
    return text if text.strip() else None


def _parse_template(text: str, where: str) -> Template:
    stripped = _PLACEHOLDER_RE.sub("", text)
    if "{" in stripped or "}" in stripped:
        raise _refuse(
            "key_template_invalid",
            f"key template {text!r} has an unmatched brace",
            "write each placeholder as {path}, for example \"{id}\"", where,
        )
    placeholders = tuple(m.group(1).strip() for m in _PLACEHOLDER_RE.finditer(text))
    if not placeholders:
        raise _refuse(
            "key_template_invalid",
            f"key template {text!r} names no {{path}} placeholder, so every record would get the same key",
            "name the field that identifies a record, for example key: \"{id}\"", where,
        )
    bad = [p for p in placeholders if not _valid_path(p)]
    if bad:
        raise _refuse(
            "key_template_invalid",
            f"key template {text!r} has an empty or malformed placeholder {bad[0]!r}",
            "use a dotted path inside each placeholder, for example {fields.id} or {.}", where,
        )
    return Template(text=text, placeholders=placeholders)


# --------------------------------------------------------------------------
# The typed manifest
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class FieldSpec:
    name: str
    path: str
    kind: str


@dataclass(frozen=True)
class LinkSpec:
    name: str
    to: str
    key: Template
    each: Optional[str] = None


@dataclass(frozen=True)
class AuthSpec:
    """How an HTTP read authenticates. `env` maps a role (`token`, `cert`,
    ...) to the NAME of the environment variable that holds its value."""

    mode: str = "none"
    env: Mapping[str, str] = field(default_factory=dict)
    header_name: Optional[str] = None

    def describe(self) -> str:
        """Plain words for the preview; names variables, never values."""
        if self.mode == "none":
            return "no authentication"
        named = ", ".join(f"{role} from ${name}" for role, name in self.env.items())
        if self.mode == "header":
            return f"header {self.header_name} ({named})"
        return f"{self.mode.replace('_', ' ')} ({named})"


@dataclass(frozen=True)
class PagingSpec:
    mode: str = "none"
    params: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ReadSpec:
    """One read: local files (`files`) or a JSON API (`url`)."""

    files: Optional[str] = None
    format: Optional[str] = None
    records: Optional[str] = None
    url: Optional[str] = None
    connection: Optional[str] = None
    auth: AuthSpec = field(default_factory=AuthSpec)
    paging: PagingSpec = field(default_factory=PagingSpec)
    max_pages: int = DEFAULT_MAX_PAGES
    #: Where the read is declared: `read` or `types.<name>.read`.
    where: str = "read"

    @property
    def kind(self) -> str:
        return "http" if self.url is not None else "files"

    @property
    def read_id(self) -> str:
        return self.where

    def format_for(self, path: str) -> Optional[str]:
        if self.kind == "http":
            return "json"
        return self.format or _EXTENSION_FORMATS.get(Path(path).suffix.lower())

    def describe(self) -> str:
        if self.kind == "files":
            return f"files {self.files}"
        paging = "" if self.paging.mode == "none" else f", {self.paging.mode} paging"
        return f"GET {self.url} ({self.auth.describe()}{paging})"


@dataclass(frozen=True)
class TypeSpec:
    name: str
    key: Template
    from_path: Optional[str] = None
    title: Optional[str] = None
    text: Optional[str] = None
    dates: Mapping[str, str] = field(default_factory=dict)  # attr -> path
    fields: Mapping[str, FieldSpec] = field(default_factory=dict)
    links: Mapping[str, LinkSpec] = field(default_factory=dict)
    read: Optional[ReadSpec] = None
    parent: Optional[str] = None
    changes_when: Optional[str] = None

    @property
    def class_name(self) -> str:
        return class_name_for(self.name)

    @property
    def passage_type(self) -> Optional[str]:
        return f"{self.name}{PASSAGE_SUFFIX}" if self.text else None

    def attribute_ranges(self) -> dict[str, str]:
        """Record attribute -> field type, as the generated schema declares them."""
        ranges: dict[str, str] = {}
        if self.title:
            ranges[TITLE_ATTR] = "string"
        if self.text:
            ranges[BODY_ATTR] = "string"
        for attr in self.dates:
            ranges[attr] = "datetime"
        for spec in self.fields.values():
            ranges[spec.name] = spec.kind
        return ranges


@dataclass(frozen=True)
class Manifest:
    raw: Mapping[str, Any]
    #: The manifest's top-level `read:`, or None when every top-level type
    #: carries its own.
    read: Optional[ReadSpec]
    types: Mapping[str, TypeSpec]
    deletion: str
    description: Optional[str]
    digest: str
    #: Top-level type name -> the read its records come from.
    reads: Mapping[str, ReadSpec] = field(default_factory=dict)
    #: The share of the earlier records one run may remove; None is no guard.
    max_deleted_fraction: Optional[float] = None
    #: Digest of what maps a record (types without their reads): a record's
    #: revision uses it, so changing a read or the guard rewrites nothing.
    mapping_digest: str = ""

    @property
    def top_types(self) -> list[TypeSpec]:
        return [t for t in self.types.values() if t.from_path is None]

    @property
    def top_type(self) -> TypeSpec:
        """The type the top-level `read:` feeds (or the first top-level type)."""
        tops = self.top_types
        if self.read is not None:
            for spec in tops:
                if spec.read is None:
                    return spec
        return tops[0]

    @property
    def nested_types(self) -> list[TypeSpec]:
        return [t for t in self.types.values() if t.from_path is not None]

    def nested_types_of(self, parent: str) -> list[TypeSpec]:
        single = len(self.top_types) == 1
        return [
            t for t in self.nested_types
            if t.parent == parent or (t.parent is None and single)
        ]

    @property
    def has_http(self) -> bool:
        return any(read.kind == "http" for read in self.reads.values())

    def all_reads(self) -> list[tuple[str, ReadSpec]]:
        """(top-level type, read) in declaration order."""
        return [(name, self.reads[name]) for name in self.types if name in self.reads]


def class_name_for(type_name: str) -> str:
    return "".join(part[:1].upper() + part[1:] for part in type_name.split("_"))


def pascal_to_snake(name: str) -> str:
    """The catalog's own conversion (`catalog/modules_compose.py`)."""
    from okg.substrate.catalog.modules_compose import _pascal_to_snake

    return _pascal_to_snake(name)


def type_name_round_trips(type_name: str) -> bool:
    return pascal_to_snake(class_name_for(type_name)) == type_name


def manifest_digest(raw: Mapping[str, Any]) -> str:
    payload = json.dumps(raw, sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


@functools.lru_cache(maxsize=1)
def _manifest_json_schema() -> dict[str, Any]:
    from okg.substrate.schema_resources import schemas_dir

    return json.loads(schemas_dir().joinpath("source_manifest.schema.json").read_text(encoding="utf-8"))


def _schema_problem(error: Any) -> ManifestProblem:
    where = ".".join(str(p) for p in error.absolute_path) or "<manifest>"
    validator = error.validator
    in_fields = ".fields." in f".{where}."
    if validator in ("oneOf", "anyOf") and error.context and not in_fields:
        from jsonschema.exceptions import best_match

        # A read, auth or paging block: report the closest alternative.
        return _schema_problem(best_match(error.context))
    if validator == "additionalProperties":
        allowed = sorted((error.schema.get("properties") or {}).keys())
        fix = f"remove the unknown key; allowed keys here: {', '.join(allowed) or 'none'}"
    elif validator == "required":
        fix = "add the missing key"
    elif validator == "const":
        fix = f"set it to {error.validator_value!r}"
    elif validator == "enum":
        fix = f"use one of {', '.join(map(str, error.validator_value))}"
    elif validator == "oneOf":
        fix = "write either a path (text) or {from: <path>, type: <type>}"
    else:
        fix = "correct the value as the message says"
    return ManifestProblem("manifest_invalid", error.message, fix, path=where)


def load_manifest(path: str | Path) -> Manifest:
    """Read and validate a manifest file; resolve its glob against its folder."""
    manifest_path = Path(path).expanduser()
    try:
        text = manifest_path.read_bytes().decode("utf-8")
    except UnicodeDecodeError as exc:
        raise _refuse(
            "manifest_unreadable", f"manifest {manifest_path} is not UTF-8 text",
            "save the manifest as UTF-8",
        ) from exc
    except OSError as exc:
        raise _refuse(
            "manifest_unreadable", f"cannot read manifest {manifest_path}: {exc.strerror or exc}",
            "pass the path of an existing, readable manifest file",
        ) from exc
    return parse_manifest(text, base_dir=manifest_path.resolve().parent, origin=str(manifest_path))


def parse_manifest(text: str, *, base_dir: Path, origin: str = "<manifest>") -> Manifest:
    from jsonschema import Draft202012Validator

    try:
        raw = yaml.safe_load(text)
    except yaml.YAMLError as exc:
        raise _refuse(
            "manifest_unreadable", f"{origin} is not valid YAML: {exc}",
            "fix the YAML syntax at the position named",
        ) from exc
    if not isinstance(raw, dict):
        raise _refuse(
            "manifest_invalid", f"{origin} must be a mapping with manifest, read and types",
            "start the file with `manifest: okg.source/v1`",
        )
    errors = sorted(
        Draft202012Validator(_manifest_json_schema()).iter_errors(raw),
        key=lambda e: [str(p) for p in e.absolute_path],
    )
    if errors:
        raise ManifestError([_schema_problem(e) for e in errors])

    normalized = json.loads(json.dumps(raw))
    problems: list[ManifestProblem] = []
    connections: dict[str, dict[str, Any]] = {}
    for conn_name, conn in (normalized.get("connections") or {}).items():
        cwhere = f"connections.{conn_name}"
        base_url = str(conn["base_url"])
        problems.extend(_url_problems(base_url, f"{cwhere}.base_url"))
        auth = _guard(problems, lambda: _parse_auth(conn.get("auth"), f"{cwhere}.auth"))
        connections[conn_name] = {"base_url": base_url, "auth": auth or AuthSpec()}

    top_read: Optional[ReadSpec] = None
    if normalized.get("read") is not None:
        top_read = _guard(problems, lambda: _parse_read(
            normalized["read"], "read", base_dir=base_dir, connections=connections,
        ))

    types: dict[str, TypeSpec] = {}
    for name, body in normalized["types"].items():
        try:
            types[name] = _parse_type(name, body, base_dir=base_dir, connections=connections)
        except ManifestError as exc:
            problems.extend(exc.problems)
    if problems:
        raise ManifestError(problems)
    deletion = str(normalized.get("deletion") or DEFAULT_DELETION)
    reads = _assign_reads(types, top_read)
    fraction = normalized.get("max_deleted_fraction")
    if fraction is None and any(r.kind == "http" for r in reads.values()):
        fraction = DEFAULT_HTTP_MAX_DELETED_FRACTION
    manifest = Manifest(
        raw=normalized, read=top_read, types=types, deletion=deletion,
        description=normalized.get("description"),
        digest=manifest_digest(normalized),
        reads=reads,
        max_deleted_fraction=None if fraction is None else float(fraction),
        mapping_digest=mapping_digest(normalized),
    )
    _check_structure(manifest)
    return manifest


def mapping_digest(raw: Mapping[str, Any]) -> str:
    """Digest of what maps one record: the types, without their reads."""
    types = {
        name: {k: v for k, v in (body or {}).items() if k != "read"}
        for name, body in (raw.get("types") or {}).items()
    }
    return manifest_digest({"manifest": raw.get("manifest"), "types": types})


def _assign_reads(types: Mapping[str, TypeSpec], top_read: Optional[ReadSpec]) -> dict[str, ReadSpec]:
    """Top-level type -> its read. Structure problems are reported later."""
    reads: dict[str, ReadSpec] = {}
    for spec in types.values():
        if spec.from_path is not None:
            continue
        if spec.read is not None:
            reads[spec.name] = spec.read
        elif top_read is not None and not any(r is top_read for r in reads.values()):
            reads[spec.name] = top_read
    return reads


def _env_name_problem(value: Any, where: str, role: str) -> Optional[ManifestProblem]:
    if isinstance(value, str) and _ENV_NAME_RE.match(value):
        return None
    return ManifestProblem(
        "auth_literal_secret",
        f"{where}.{role} must name an environment variable (upper-case letters, digits "
        "and underscores); a manifest never holds a secret or a path itself",
        f"put the value in an environment variable, for example ITEMS_{role.upper()}, "
        f"and write {role}: ITEMS_{role.upper()}",
        path=f"{where}.{role}",
    )


def _parse_auth(raw: Any, where: str) -> AuthSpec:
    if raw is None or raw == "none":
        return AuthSpec()
    if not isinstance(raw, Mapping) or len(raw) != 1:
        raise _refuse(
            "auth_invalid", f"{where} must be none or one mode",
            f"use one of {', '.join(AUTH_MODES)}", where,
        )
    mode, body = next(iter(raw.items()))
    body = body or {}
    problems: list[ManifestProblem] = []
    env: dict[str, str] = {}
    for role in AUTH_ENV_ROLES[mode]:
        if role not in body:
            continue  # the schema requires the needed roles; ca_dir is optional
        problem = _env_name_problem(body[role], f"{where}.{mode}", role)
        if problem is not None:
            problems.append(problem)
        else:
            env[role] = str(body[role])
    if problems:
        raise ManifestError(problems)
    return AuthSpec(mode=mode, env=env, header_name=body.get("name") if mode == "header" else None)


def normalize_url(url: str) -> str:
    """Percent-encode what a request line cannot carry (a space, for
    example), leaving characters a URL may hold and existing escapes alone."""
    return quote(url, safe=":/?#[]@!$&'()*+,;=%~")


def _url_problems(url: str, where: str) -> list[ManifestProblem]:
    problems: list[ManifestProblem] = []
    parts = urlsplit(url)
    if parts.scheme not in ("http", "https") or not parts.netloc:
        problems.append(ManifestProblem(
            "url_invalid", f"{url!r} is not an http or https URL",
            "write a full URL such as https://example.org/api/", path=where,
        ))
        return problems
    if parts.username or parts.password:
        problems.append(ManifestProblem(
            "auth_literal_secret", f"{where} carries a user name or password in the URL",
            "remove it from the URL and use auth: basic with environment-variable names",
            path=where,
        ))
    for name, _value in parse_qsl(parts.query, keep_blank_values=True):
        lowered = name.lower()
        if lowered in _SECRET_QUERY_NAMES or lowered.endswith(_SECRET_QUERY_SUFFIXES):
            problems.append(ManifestProblem(
                "auth_literal_secret",
                f"{where} has a query parameter {name!r}, which looks like it carries a secret",
                "remove it from the URL; send the secret with auth: header or bearer, "
                "naming an environment variable",
                path=where,
            ))
    return problems


def _parse_paging(raw: Any, where: str) -> PagingSpec:
    if raw is None or raw == "none":
        return PagingSpec()
    if raw == "link_header":
        return PagingSpec("link_header")
    mode, body = next(iter(raw.items()))
    params = dict(body or {})
    if mode == "cursor" and not _valid_path(str(params.get("next", ""))):
        raise _refuse(
            "path_invalid", f"{where}.cursor.next {params.get('next')!r} is not a dotted path",
            "write the path to the next cursor in each response, for example meta.next_cursor",
            f"{where}.cursor.next",
        )
    if mode == "page":
        params.setdefault("first", 1)
    return PagingSpec(mode=str(mode), params=params)


def _parse_read(
    body: dict[str, Any], where: str, *, base_dir: Path,
    connections: Mapping[str, Mapping[str, Any]],
) -> ReadSpec:
    """Validate one read; a files glob is saved as an absolute path."""
    problems: list[ManifestProblem] = []
    records = body.get("records")
    if records is not None and records != OBJECT_RECORDS and not _valid_path(str(records)):
        problems.append(ManifestProblem(
            "path_invalid", f"{where}.records {records!r} is not a dotted path",
            "write a path such as issues or data.items, or \"*\" for each value of an object",
            path=f"{where}.records",
        ))
    if "files" in body:
        files = str(body["files"])
        glob_path = Path(files).expanduser()
        if not glob_path.is_absolute():
            glob_path = base_dir / glob_path
        body["files"] = str(glob_path)
        read = ReadSpec(files=str(glob_path), format=body.get("format"), records=records, where=where)
        if read.format is None and _EXTENSION_FORMATS.get(Path(read.files or "").suffix.lower()) is None:
            problems.append(ManifestProblem(
                "format_unknown",
                f"cannot tell the file format from {files!r}",
                "set read.format to json, jsonl, csv or yaml, or end the glob in .json, .jsonl, .csv, "
                ".yaml or .yml",
                path=f"{where}.format",
            ))
        if records is not None and (read.format or read.format_for(read.files or "")) not in (None, *_DOCUMENT_FORMATS):
            problems.append(ManifestProblem(
                "records_path_not_json",
                "read.records applies only to JSON and YAML files: each JSONL line and each CSV row "
                "is one record",
                "remove read.records", path=f"{where}.records",
            ))
        if problems:
            raise ManifestError(problems)
        return read

    conn_name = body.get("connection")
    if conn_name is not None:
        connection = connections.get(conn_name)
        if connection is None:
            raise _refuse(
                "connection_unknown", f"{where}.connection names {conn_name!r}, which connections does not declare",
                f"declare connections.{conn_name} with a base_url, or name one of "
                f"{', '.join(sorted(connections)) or 'none'}",
                f"{where}.connection",
            )
        if "path" not in body:
            raise _refuse(
                "read_invalid", f"{where} names a connection but no path",
                "add path: the part of the URL after the connection's base_url", where,
            )
        url = urljoin(str(connection["base_url"]), str(body["path"]))
        default_auth = connection["auth"]
    else:
        if "url" not in body:
            raise _refuse(
                "read_invalid", f"{where} needs files:, url:, or connection: with path:",
                "say where the records are", where,
            )
        if "path" in body:
            raise _refuse(
                "read_invalid", f"{where} has path: but no connection:",
                "add connection:, or write the whole address as url:", f"{where}.path",
            )
        url = str(body["url"])
        default_auth = AuthSpec()
    url = normalize_url(url)
    problems.extend(_url_problems(url, f"{where}.{'url' if conn_name is None else 'path'}"))
    auth = default_auth
    if "auth" in body:
        auth = _guard(problems, lambda: _parse_auth(body["auth"], f"{where}.auth")) or AuthSpec()
    paging = _guard(problems, lambda: _parse_paging(body.get("paging"), f"{where}.paging")) or PagingSpec()
    if problems:
        raise ManifestError(problems)
    return ReadSpec(
        url=url, records=records, connection=conn_name, auth=auth, paging=paging,
        max_pages=int(body.get("max_pages") or DEFAULT_MAX_PAGES), where=where,
    )


def _parse_type(
    name: str, body: dict[str, Any], *, base_dir: Path = Path("."),
    connections: Mapping[str, Mapping[str, Any]] | None = None,
) -> TypeSpec:
    where = f"types.{name}"
    problems: list[ManifestProblem] = []
    if not _NAME_RE.match(name):
        problems.append(ManifestProblem(
            "type_name_invalid", f"type name {name!r} is not snake_case",
            f"use lower-case letters, digits and underscores, starting with a letter, for example {_snake_hint(name)!r}",
            path=where,
        ))
    elif not type_name_round_trips(name):
        suggestion = pascal_to_snake(class_name_for(name))
        problems.append(ManifestProblem(
            "type_name_round_trip",
            f"type name {name!r} does not survive the catalog's conversion to a class name "
            f"({class_name_for(name)}) and back ({suggestion!r})",
            f"rename the type to {suggestion!r}", path=where,
        ))
    key = _guard(problems, lambda: _parse_template(str(body["key"]), f"{where}.key"))
    read = None
    if body.get("read") is not None:
        read = _guard(problems, lambda: _parse_read(
            body["read"], f"{where}.read", base_dir=base_dir, connections=connections or {},
        ))
    for label in ("from", "title", "text", "changes_when"):
        value = body.get(label)
        if value is not None and not _valid_path(str(value)):
            problems.append(ManifestProblem(
                "path_invalid", f"{label} {value!r} is not a dotted path",
                "write a path such as fields.summary, or . for the record itself", path=f"{where}.{label}",
            ))
    dates = {}
    for date_key, path in (body.get("dates") or {}).items():
        dates[DATE_ATTRS[date_key][0]] = str(path)
    fields: dict[str, FieldSpec] = {}
    for field_name, spec in (body.get("fields") or {}).items():
        fwhere = f"{where}.fields.{field_name}"
        if not _NAME_RE.match(field_name):
            problems.append(ManifestProblem(
                "field_name_invalid", f"field name {field_name!r} is not snake_case",
                f"rename it, for example to {_snake_hint(field_name)!r}", path=fwhere,
            ))
            continue
        if field_name in RESERVED_FIELD_NAMES:
            problems.append(ManifestProblem(
                "field_name_reserved",
                f"field name {field_name!r} is reserved for title, text and dates",
                f"rename it, for example to {name}_{field_name}; map titles with title:, text with text:, dates with dates:",
                path=fwhere,
            ))
            continue
        if isinstance(spec, str):
            fields[field_name] = FieldSpec(field_name, spec, "string")
        else:
            fields[field_name] = FieldSpec(field_name, str(spec["from"]), str(spec.get("type") or "string"))
    links: dict[str, LinkSpec] = {}
    for link_name, spec in (body.get("links") or {}).items():
        lwhere = f"{where}.links.{link_name}"
        if not _NAME_RE.match(link_name):
            problems.append(ManifestProblem(
                "link_name_invalid", f"link name {link_name!r} is not snake_case",
                f"rename it, for example to {_snake_hint(link_name)!r}", path=lwhere,
            ))
            continue
        link_key = _guard(problems, lambda: _parse_template(str(spec["key"]), f"{lwhere}.key"))
        if link_key is not None:
            links[link_name] = LinkSpec(link_name, str(spec["to"]), link_key, spec.get("each"))
    if problems:
        raise ManifestError(problems)
    assert key is not None
    return TypeSpec(
        name=name, key=key, from_path=body.get("from"), title=body.get("title"),
        text=body.get("text"), dates=dates, fields=fields, links=links,
        read=read, parent=body.get("parent"), changes_when=body.get("changes_when"),
    )


def _guard(problems: list[ManifestProblem], fn: Any) -> Any:
    try:
        return fn()
    except ManifestError as exc:
        problems.extend(exc.problems)
        return None


def _snake_hint(name: str) -> str:
    hint = re.sub(r"[^a-z0-9]+", "_", pascal_to_snake(str(name)).lower()).strip("_")
    if not hint or not hint[0].isalpha():
        hint = f"t_{hint}".rstrip("_")
    return hint


def _check_structure(manifest: Manifest) -> None:
    problems: list[ManifestProblem] = []
    top = [t.name for t in manifest.types.values() if t.from_path is None]
    shared = [name for name in top if manifest.types[name].read is None]
    if not top:
        problems.append(ManifestProblem(
            "top_level_type", "no type reads the records themselves",
            "give one type no from:, and give every other type a from: path to "
            "the records nested inside it", path="types",
        ))
    elif manifest.read is None and shared:
        problems.append(ManifestProblem(
            "read_missing",
            f"type(s) {', '.join(shared)} have no from: and no read:, and the manifest has no read:",
            "give each top-level type its own read:, or add a top-level read: for one of them",
            path=f"types.{shared[0]}",
        ))
    elif manifest.read is not None and len(shared) != 1:
        problems.append(ManifestProblem(
            "top_level_type",
            (
                "the top-level read: feeds no type: every top-level type has its own read:"
                if not shared else
                f"types {', '.join(shared)} all read the records of the top-level read:; exactly one may"
            ),
            "give exactly one type no from: and no read: for the top-level read:, give "
            "other top-level types their own read:, and give nested types a from: path",
            path="types",
        ))
    names = set(manifest.types)
    for spec in manifest.types.values():
        where = f"types.{spec.name}"
        if spec.from_path is not None and spec.read is not None:
            problems.append(ManifestProblem(
                "read_on_nested", f"type {spec.name!r} has both from: and read:",
                "a nested type reads from its parent's records; remove read: or from:",
                path=f"{where}.read",
            ))
        if spec.parent is not None and (spec.from_path is None or spec.parent not in top):
            problems.append(ManifestProblem(
                "parent_invalid",
                f"type {spec.name!r} names parent {spec.parent!r}, "
                + ("but has no from:" if spec.from_path is None else "which is not a top-level type"),
                f"set parent: to one of {', '.join(top) or 'the top-level types'}, and give the type a from: path",
                path=f"{where}.parent",
            ))
        if spec.from_path is not None and spec.parent is None and len(top) > 1:
            problems.append(ManifestProblem(
                "parent_ambiguous",
                f"type {spec.name!r} reads nested records, and the manifest has several "
                f"top-level types ({', '.join(top)})",
                f"add parent: <one of {', '.join(top)}> to say whose records hold it",
                path=f"{where}.from",
            ))
    for spec in manifest.types.values():
        passage = spec.passage_type
        if passage and passage in names:
            problems.append(ManifestProblem(
                "type_name_taken",
                f"type {passage!r} clashes with the passage type generated for {spec.name!r}'s text",
                f"rename type {passage!r}", path=f"types.{passage}",
            ))
        for link in spec.links.values():
            if link.to not in names:
                problems.append(ManifestProblem(
                    "link_target_unknown",
                    f"link {link.name!r} points to type {link.to!r}, which this manifest does not declare",
                    f"set to: to one of {', '.join(sorted(names))}; links to other sources wait for phase 3",
                    path=f"types.{spec.name}.links.{link.name}.to",
                ))
    cycle = _containment_cycle(manifest)
    if cycle:
        problems.append(ManifestProblem(
            "containment_cycle",
            f"types {', '.join(cycle)} would contain each other through containment links "
            f"({', '.join(sorted(CONTAINMENT_LINKS))})",
            "rename one of those links to a reference name such as related_to, or remove it",
            path="types",
        ))
    if problems:
        raise ManifestError(problems)


def _containment_cycle(manifest: Manifest) -> list[str]:
    """Members of the first containment cycle of two or more types, or []."""
    graph: dict[str, set[str]] = {name: set() for name in manifest.types}
    for spec in manifest.types.values():
        for link in spec.links.values():
            if link.name in CONTAINMENT_LINKS and link.to in graph and link.to != spec.name:
                graph[spec.name].add(link.to)
    index: dict[str, int] = {}
    low: dict[str, int] = {}
    stack: list[str] = []
    on_stack: set[str] = set()
    found: list[list[str]] = []

    def visit(node: str) -> None:
        index[node] = low[node] = len(index)
        stack.append(node)
        on_stack.add(node)
        for nxt in sorted(graph[node]):
            if nxt not in index:
                visit(nxt)
                low[node] = min(low[node], low[nxt])
            elif nxt in on_stack:
                low[node] = min(low[node], index[nxt])
        if low[node] == index[node]:
            component = []
            while True:
                member = stack.pop()
                on_stack.discard(member)
                component.append(member)
                if member == node:
                    break
            if len(component) > 1:
                found.append(sorted(component))

    for node in sorted(graph):
        if node not in index:
            visit(node)
    return found[0] if found else []


# --------------------------------------------------------------------------
# Compile
# --------------------------------------------------------------------------


@functools.lru_cache(maxsize=1)
def library_type_names() -> frozenset[str]:
    """Every subtype any built-in library ontology declares, loaded or not."""
    root = Path(__file__).resolve().parents[1] / "library" / "ontologies"
    names: set[str] = set()
    for path in sorted(root.rglob("*.yaml")):
        names.update(schema_file_subtypes(path))
    return frozenset(names)


def schema_file_subtypes(path: Path) -> set[str]:
    """Subtypes one LinkML schema file declares (archetype-annotated classes)."""
    try:
        document = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except (OSError, yaml.YAMLError, UnicodeDecodeError):
        return set()
    classes = document.get("classes") if isinstance(document, Mapping) else None
    if not isinstance(classes, Mapping):
        return set()
    out: set[str] = set()
    for cls_name, cls_def in classes.items():
        if cls_name == "EdgeNarrowings" or not isinstance(cls_def, Mapping):
            continue
        annotations = cls_def.get("annotations") or {}
        if isinstance(annotations, Mapping) and "archetype" in annotations:
            out.add(pascal_to_snake(str(cls_name)))
    return out


@dataclass(frozen=True)
class CompiledSource:
    """Everything a manifest generates for one source."""

    manifest: Manifest
    source: str
    registration: Any  # okg.deployment.ConnectorRegistration
    params: dict[str, Any]
    output_signature: dict[str, Any]
    schema_text: str
    bridges_text: Optional[str]
    manifest_copy_text: str

    @property
    def manifest_relpath(self) -> str:
        return manifest_copy_relpath(self.source)

    @property
    def schema_relpath(self) -> str:
        return f"{self.source}.yaml"

    @property
    def bridges_relpath(self) -> str:
        return f"bridges/{self.source}_links.yaml"

    @property
    def node_types(self) -> list[str]:
        return [str(node["subtype"]) for node in self.output_signature["nodes"]]

    @property
    def edge_triples(self) -> list[tuple[str, str, str]]:
        return [
            (str(e["src_subtype"]), str(e["edge_type"]), str(e["dst_subtype"]))
            for e in self.output_signature["edges"]
        ]


def manifest_copy_relpath(source: str) -> str:
    """Where the saved manifest copy lives, relative to the deployment."""
    return f"sources/{source}.source.yaml"


def check_type_names(
    manifest: Manifest,
    *,
    source: str,
    taken: Mapping[str, str] | None = None,
) -> None:
    """Refuse library type names and names another source already declares.

    `taken` maps a type name to who declares it (`source tracker`); the
    library ontology names are always reserved.
    """
    reserved = library_type_names()
    problems: list[ManifestProblem] = []
    prefix = re.sub(r"[^a-z0-9]+", "_", source.lower()).strip("_") or "src"
    for spec in manifest.types.values():
        for name in filter(None, (spec.name, spec.passage_type)):
            owner = None
            if name in reserved:
                owner = "a built-in library type"
            elif taken and name in taken:
                owner = taken[name]
            if owner is None:
                continue
            suggestion = f"{prefix}_{spec.name}"
            if not _NAME_RE.match(suggestion) or not type_name_round_trips(suggestion):
                suggestion = f"{_snake_hint(prefix)}_{spec.name}"
            problems.append(ManifestProblem(
                "type_name_reserved" if owner == "a built-in library type" else "type_name_taken",
                f"type name {name!r} is already used by {owner}",
                f"rename type {spec.name!r}, for example to {suggestion!r}",
                path=f"types.{spec.name}",
            ))
    if problems:
        raise ManifestError(problems)


def compile_manifest(
    manifest: Manifest,
    *,
    source: str,
    taken: Mapping[str, str] | None = None,
) -> CompiledSource:
    """Compile a validated manifest for source `source`. Pure."""
    from okg.deployment import ConnectorRegistration, SdkVersionRange

    check_type_names(manifest, source=source, taken=taken)
    nodes: list[dict[str, str]] = []
    edges: list[dict[str, str]] = []
    for spec in manifest.types.values():
        nodes.append({"subtype": spec.name})
        if spec.passage_type:
            nodes.append({"subtype": spec.passage_type})
    for spec in manifest.types.values():
        for link in spec.links.values():
            triple = {"src_subtype": spec.name, "edge_type": link.name, "dst_subtype": link.to}
            if triple not in edges:
                edges.append(triple)
        if spec.passage_type:
            edges.append({"src_subtype": spec.name, "edge_type": PASSAGE_EDGE, "dst_subtype": spec.passage_type})
    signature = {"nodes": nodes, "edges": edges}
    params = {
        "manifest": "${deployment_dir}/" + manifest_copy_relpath(source),
        "source": source,
    }
    registration = ConnectorRegistration(
        connector_type=CONNECTOR_TYPE,
        version="1.0.0",
        module=READER_MODULE,
        class_name=READER_CLASS,
        ownership_id=source,
        source_class="discovery_crawl",
        trust_label="curated",
        sdk_range=SdkVersionRange(min_version="1.0", max_version_exclusive="2.0"),
        config_schema={
            "type": "object",
            "additionalProperties": False,
            "required": ["manifest", "source"],
            "properties": {
                "manifest": {"type": "string", "description": "The saved manifest copy."},
                "source": {"type": "string", "description": "The source name in node ids."},
            },
        },
        auth_modes=("none",),
        capabilities=("change_probe", "record_set_authority"),
        output_signature=signature,
        sync={
            "triggers": ["manual", "reconcile"],
            "default_event_mode": "scope_complete",
            "reconcile_mode": "scope_complete",
            "deletes_require_reconcile": True,
            "cheap_probe": "stat_tree",
        },
        record_identity_kind="scoped_locator",
        record_identity_fields=("type", "key"),
        source_revision_kind="content_hash",
        deletion_semantics="missing_from_completed_scope",
        publication_mode="published_generation",
        admission_mode="fast_track",
        description=manifest.description or f"Records read by the source manifest for {source}",
    )
    schema_body = _schema_document(manifest, source)
    bridges_body = _bridges_document(manifest, source)
    copy_body = yaml.safe_dump(dict(manifest.raw), sort_keys=False, allow_unicode=True)
    return CompiledSource(
        manifest=manifest,
        source=source,
        registration=registration,
        params=params,
        output_signature=signature,
        schema_text=with_generated_header(schema_body, source=source, digest=manifest.digest),
        bridges_text=(
            with_generated_header(bridges_body, source=source, digest=manifest.digest)
            if bridges_body is not None else None
        ),
        manifest_copy_text=with_generated_header(copy_body, source=source, digest=manifest.digest),
    )


def _schema_document(manifest: Manifest, source: str) -> str:
    snake = re.sub(r"[^a-z0-9]+", "_", source.lower()).strip("_")
    classes: dict[str, Any] = {}
    for spec in manifest.types.values():
        attributes: dict[str, Any] = {}
        for attr, kind in spec.attribute_ranges().items():
            body: dict[str, Any] = {"range": _LINKML_RANGES[kind]}
            if kind == "list":
                body["multivalued"] = True
            attributes[attr] = body
        annotations: dict[str, Any] = {"archetype": "entity", "transient": "false"}
        if spec.title:
            annotations["okg_title_from"] = TITLE_ATTR
        searchable = [a for a in (TITLE_ATTR if spec.title else None, BODY_ATTR if spec.text else None) if a]
        if searchable:
            annotations["okg_searchable_text_fields"] = ",".join(searchable)
        for date_key, (attr, annotation) in DATE_ATTRS.items():
            if attr in spec.dates:
                annotations[annotation] = attr
        if spec.passage_type:
            annotations["okg_passage_rollup"] = {
                "edge_type": PASSAGE_EDGE, "child_subtype": class_name_for(spec.passage_type),
            }
        cls: dict[str, Any] = {
            "description": f"A {spec.name} record read by source {source}.",
            "annotations": annotations,
        }
        if attributes:
            cls["attributes"] = attributes
        classes[spec.class_name] = cls
        if spec.passage_type:
            classes[class_name_for(spec.passage_type)] = {
                "description": f"A passage of a long {spec.name} text, read by source {source}.",
                "annotations": {
                    "archetype": "artifact", "transient": "false",
                    "okg_searchable_text_fields": "text",
                },
                "attributes": {
                    "text": {"range": "string", "required": True},
                    "ordinal": {"range": "integer"},
                    "char_offset": {"range": "integer"},
                    "char_end": {"range": "integer"},
                },
            }
    document = {
        "id": f"https://okg.invalid/sources/{snake}",
        "name": f"okg_source_{snake}",
        "description": f"Record types generated from the source manifest for {source}.",
        "imports": ["linkml:types"],
        "prefixes": {"linkml": "https://w3id.org/linkml/"},
        "default_range": "string",
        "classes": classes,
    }
    return yaml.safe_dump(document, sort_keys=False, allow_unicode=True)


def narrowing_name(source: str, src: str, edge: str, dst: str) -> str:
    snake = re.sub(r"[^a-z0-9]+", "_", source.lower()).strip("_")
    return f"{snake}_{src}_{edge}_{dst}"


def _bridges_document(manifest: Manifest, source: str) -> Optional[str]:
    snake = re.sub(r"[^a-z0-9]+", "_", source.lower()).strip("_")
    attributes: dict[str, Any] = {}
    for spec in manifest.types.values():
        for link in spec.links.values():
            attributes[narrowing_name(source, spec.name, link.name, link.to)] = {
                "annotations": {
                    "edge_archetype": link.name,
                    "src_subtypes": spec.class_name,
                    "dst_subtypes": class_name_for(link.to),
                },
            }
        if spec.passage_type:
            attributes[narrowing_name(source, spec.name, PASSAGE_EDGE, spec.passage_type)] = {
                "annotations": {
                    "edge_archetype": PASSAGE_EDGE,
                    "src_subtypes": spec.class_name,
                    "dst_subtypes": class_name_for(spec.passage_type),
                },
            }
    if not attributes:
        return None
    document = {
        "id": f"https://okg.invalid/sources/{snake}/links",
        "name": f"okg_source_{snake}_links",
        "imports": ["linkml:types"],
        "prefixes": {"linkml": "https://w3id.org/linkml/"},
        "default_range": "string",
        "classes": {
            "EdgeNarrowings": {
                "description": f"Links generated from the source manifest for {source}.",
                "annotations": {"okg_edge_narrowings": "true"},
                "attributes": attributes,
            },
        },
    }
    return yaml.safe_dump(document, sort_keys=False, allow_unicode=True)


# --------------------------------------------------------------------------
# Generated-file header
# --------------------------------------------------------------------------


def with_generated_header(body: str, *, source: str, digest: str) -> str:
    body_digest = hashlib.sha256(body.encode("utf-8")).hexdigest()
    return (
        f"# Generated by okg from the source manifest of source {source!r}. Do not edit:\n"
        "# `okg deployment add-source --manifest` rewrites this file.\n"
        f"# {GENERATED_MARK} name={source} manifest-sha256={digest} body-sha256={body_digest}\n"
        + body
    )


@dataclass(frozen=True)
class GeneratedHeader:
    source: str
    manifest_sha256: str
    body_intact: bool


_HEADER_RE = re.compile(
    rf"^# {re.escape(GENERATED_MARK)} name=(?P<name>\S+) "
    r"manifest-sha256=(?P<manifest>[0-9a-f]{64}) body-sha256=(?P<body>[0-9a-f]{64})$"
)


def read_generated_header(text: str) -> Optional[GeneratedHeader]:
    """The header AKMON wrote, or None when the file does not carry one."""
    lines = text.split("\n", 3)
    if len(lines) < 4:
        return None
    match = _HEADER_RE.match(lines[2])
    if match is None:
        return None
    body = lines[3]
    intact = hashlib.sha256(body.encode("utf-8")).hexdigest() == match.group("body")
    return GeneratedHeader(match.group("name"), match.group("manifest"), intact)


# --------------------------------------------------------------------------
# Re-apply: additive changes only
# --------------------------------------------------------------------------


def additive_change_problems(old: Manifest, new: Manifest, *, manual_path: str) -> list[ManifestProblem]:
    """Every change from `old` to `new` that is not additive."""
    problems: list[ManifestProblem] = []

    def refuse(code: str, message: str, path: str) -> None:
        problems.append(ManifestProblem(code, message, manual_path, path=path))

    if old.deletion == "never" and new.deletion != "never":
        refuse(
            "deletion_changed",
            "deletion changed from never to missing_from_full_read, which would remove "
            "every kept record that is no longer in the files",
            "deletion",
        )
    for name, old_read in old.reads.items():
        new_read = new.reads.get(name)
        if new_read is None:
            continue  # the type was removed or nested: reported below
        prefix = "read" if new_read.where == "read" and old_read.where == "read" else f"types.{name}.read"
        if old_read.kind != new_read.kind:
            refuse(
                "read_changed",
                f"type {name!r} changed from reading {old_read.kind} to reading {new_read.kind}",
                prefix,
            )
            continue
        attrs = ("files", "format", "records") if old_read.kind == "files" else ("records",)
        # An HTTP read may change its address, auth and paging: the
        # mass-deletion guard (max_deleted_fraction) bounds what that removes.
        for attr in attrs:
            before, after = getattr(old_read, attr), getattr(new_read, attr)
            if attr == "format":
                # Compare what is read, so stating the implied format is no change.
                before = old_read.format_for(old_read.files or "")
                after = new_read.format_for(new_read.files or "")
            if before != after:
                refuse(
                    "read_changed",
                    f"{prefix}.{attr} changed from {before!r} to {after!r}; a different input can "
                    "re-identify or remove every record",
                    f"{prefix}.{attr}",
                )
    if (
        old.read is not None and new.read is not None
        and old.top_type.name != new.top_type.name and new.top_type.name in old.types
    ):
        refuse(
            "top_level_changed",
            f"the top-level type changed from {old.top_type.name!r} to {new.top_type.name!r}",
            "types",
        )
    for name, old_spec in old.types.items():
        new_spec = new.types.get(name)
        if new_spec is not None and new_spec.parent != old_spec.parent and old_spec.parent is not None:
            refuse(
                "parent_changed",
                f"type {name!r} changed parent from {old_spec.parent!r} to {new_spec.parent!r}, "
                "which changes which records exist",
                f"types.{name}.parent",
            )
    for name, old_spec in old.types.items():
        new_spec = new.types.get(name)
        if new_spec is not None and new_spec.from_path != old_spec.from_path:
            refuse(
                "from_changed",
                f"type {name!r} changed from: {old_spec.from_path!r} to {new_spec.from_path!r}, "
                "which changes which records exist",
                f"types.{name}.from",
            )

    for name, old_spec in old.types.items():
        new_spec = new.types.get(name)
        where = f"types.{name}"
        if new_spec is None:
            refuse("type_removed", f"type {name!r} was removed or renamed; a re-apply may only add", where)
            continue
        if new_spec.key.text != old_spec.key.text:
            refuse(
                "key_changed",
                f"type {name!r} changed its key from {old_spec.key.text!r} to {new_spec.key.text!r}, "
                "which re-identifies every record",
                f"{where}.key",
            )
        if old_spec.passage_type and not new_spec.passage_type:
            refuse("type_removed", f"removing {name!r}'s text: removes its passage type", f"{where}.text")
        old_ranges = old_spec.attribute_ranges()
        new_ranges = new_spec.attribute_ranges()
        for attr, kind in old_ranges.items():
            if attr not in new_ranges:
                refuse("field_removed", f"type {name!r} no longer maps {attr!r}; a re-apply may only add", where)
            elif new_ranges[attr] != kind and (kind, new_ranges[attr]) not in _WIDENINGS:
                refuse(
                    "field_type_narrowed",
                    f"field {attr!r} of {name!r} changed type from {kind} to {new_ranges[attr]}",
                    f"{where}.fields.{attr}",
                )
        for link_name, link in old_spec.links.items():
            new_link = new_spec.links.get(link_name)
            if new_link is None or new_link.to != link.to:
                refuse(
                    "link_removed", f"link {link_name!r} of {name!r} to {link.to!r} was removed or retargeted",
                    f"{where}.links.{link_name}",
                )
    return problems


# --------------------------------------------------------------------------
# Reading files
# --------------------------------------------------------------------------


def _glob(read: ReadSpec) -> list[str]:
    return sorted(
        path for path in globlib.glob(read.files or "", recursive=True)
        if Path(path).is_file() and not _is_source_manifest(path)
    )


def _is_source_manifest(path: str) -> bool:
    """A source manifest saved beside its YAML data matches a `*.yaml` glob;
    it is never a data file, so the glob leaves it out."""
    if Path(path).suffix.lower() not in (".yaml", ".yml"):
        return False
    try:
        text = Path(path).read_text(encoding="utf-8-sig")
        if "okg.source/" not in text:
            return False
        head = yaml.load(text, Loader=_YAML_LOADER)
    except (OSError, UnicodeDecodeError, yaml.YAMLError, RecursionError):
        return False
    return isinstance(head, dict) and str(head.get("manifest", "")).startswith("okg.source/")


def match_files(manifest: Manifest) -> list[str]:
    """The files every files read matches now, in sorted path order."""
    out: list[str] = []
    for _name, read in manifest.all_reads():
        if read.kind == "files":
            out.extend(p for p in _glob(read) if p not in out)
    return out


class FileReadError(ValueError):
    """One input file (or HTTP response) could not be read or parsed."""


def extract_records(payload: Any, records: Optional[str], origin: str) -> list[Any]:
    """The records in one parsed JSON document.

    `records` is a dotted path to a list, `"*"` (each value of an object,
    with its name as `_key`), or None (the document itself).
    """
    if records == OBJECT_RECORDS:
        if not isinstance(payload, Mapping):
            raise FileReadError(f'{origin} is not a JSON object, so records: "*" has nothing to read')
        out: list[Any] = []
        for name, value in payload.items():
            if isinstance(value, Mapping):
                value = {**value, OBJECT_KEY_FIELD: name}
            out.append(value)
        return out
    if records is not None:
        found = resolve_path(payload, records)
        if found is _MISSING or not isinstance(found, list):
            raise FileReadError(f"{origin} has no list at records {records!r}")
        return found
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        return [payload]
    raise FileReadError(f"{origin} holds neither a list of records nor one record")


def read_file_records(
    manifest: Optional[Manifest], path: str, data: Optional[bytes] = None, *,
    read: Optional[ReadSpec] = None,
) -> list[Any]:
    """Parse one input file with `read` (default: the manifest's first files
    read); `data` is its bytes when already read."""
    spec = read
    if spec is None and manifest is not None:
        spec = manifest.read or next(r for _n, r in manifest.all_reads() if r.kind == "files")
    if spec is None:
        raise ValueError("read_file_records needs a manifest or a read")
    fmt = spec.format_for(path)
    if fmt is None:
        raise FileReadError(f"cannot tell the format of {path}; set read.format")
    try:
        raw = Path(path).read_bytes() if data is None else data
        text = raw.decode("utf-8-sig")
    except (OSError, UnicodeDecodeError) as exc:
        raise FileReadError(f"cannot read {path}: {exc}") from exc
    if fmt == "csv":
        reader = csv.DictReader(io.StringIO(text))
        headers = [h for h in (reader.fieldnames or []) if h is not None]
        # Blank names (trailing empty spreadsheet columns) are not mapped.
        named = [h for h in headers if h.strip()]
        repeated = sorted({h for h in named if named.count(h) > 1})
        if repeated:
            raise FileReadError(
                f"{path} repeats the CSV column name(s) {', '.join(map(repr, repeated))}; "
                "a repeated column would silently keep only one value"
            )
        return [{k: v for k, v in row.items() if k and k.strip() and v not in (None, "")} for row in reader]
    if fmt == "jsonl":
        out = []
        for number, line in enumerate(text.splitlines(), start=1):
            if not line.strip():
                continue
            try:
                out.append(json.loads(line))
            except json.JSONDecodeError as exc:
                raise FileReadError(f"{path} line {number} is not JSON: {exc.msg}") from exc
        return out
    if fmt == "yaml":
        return extract_records(parse_yaml_document(text, path), spec.records, path)
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        raise FileReadError(f"{path} is not JSON: {exc.msg} (line {exc.lineno})") from exc
    return extract_records(payload, spec.records, path)


_YAML_LOADER = yaml.CSafeLoader if getattr(yaml, "__with_libyaml__", False) else yaml.SafeLoader


class _DataLoader(_YAML_LOADER):  # type: ignore[valid-type,misc]
    """Python merges keys it holds equal (`1`, `1.0`, `true`) before any
    later check could see both; refuse two such keys written differently.
    A key written twice the same way keeps its last value, as in JSON."""

    def construct_mapping(self, node: Any, deep: bool = False) -> Any:
        if isinstance(node, yaml.MappingNode):
            self.flatten_mapping(node)
            seen: dict[Any, Any] = {}
            for key_node, _value in node.value:
                key = self.construct_object(key_node, deep=deep)
                try:
                    prior = seen.setdefault(key, key_node)
                except TypeError:
                    continue  # unhashable: the base constructor refuses it
                if (prior.tag, prior.value) != (key_node.tag, key_node.value):
                    raise yaml.constructor.ConstructorError(
                        None, None,
                        f"keys {prior.value!r} and {key_node.value!r} read as the same key",
                        key_node.start_mark,
                    )
        return super().construct_mapping(node, deep=deep)


def parse_yaml_document(text: str, path: str) -> Any:
    """One YAML data file, as the same data in JSON would read."""
    try:
        if "*" in text and any(
            isinstance(event, yaml.AliasEvent)
            for event in yaml.parse(text, Loader=_YAML_LOADER)
        ):
            # JSON has no aliases; an alias can refer to its own container
            # (endless recursion) or repeat a subtree without limit.
            raise FileReadError(
                f"{path} uses a YAML alias (*name); write the data out in full"
            )
        return _json_like(yaml.load(text, Loader=_DataLoader), path)
    except yaml.YAMLError as exc:
        raise FileReadError(f"{path} is not YAML: {exc}") from exc
    except RecursionError as exc:
        raise FileReadError(f"{path} nests too deeply to read") from exc


def _json_key(key: Any, path: str) -> str:
    """A mapping key as JSON would write it (`json.dumps` key rules)."""
    if isinstance(key, str):
        return key
    if isinstance(key, (datetime, date)):
        return key.isoformat()
    if key is None or isinstance(key, (bool, int, float)):
        return json.dumps(key)
    raise FileReadError(f"{path} has a mapping key {key!r} that is not text, a number or a date")


def _json_like(value: Any, path: str) -> Any:
    """A YAML document as the same data in JSON would read: an unquoted date
    becomes its ISO text, a set a sorted list; binary data is refused."""
    if isinstance(value, Mapping):
        out: dict[str, Any] = {}
        for k, v in value.items():
            key = _json_key(k, path)
            if key in out:
                raise FileReadError(f"{path} has two mapping keys that both read as {key!r}")
            out[key] = _json_like(v, path)
        return out
    if isinstance(value, list):
        return [_json_like(v, path) for v in value]
    if isinstance(value, (set, frozenset)):
        return sorted((_json_like(v, path) for v in value), key=str)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, (bytes, bytearray)):
        raise FileReadError(f"{path} holds binary data, which a record cannot carry")
    return value


@dataclass
class InputRead:
    files: list[str]
    #: (label, records): a file path, or `GET <url>` for one HTTP page.
    records: list[tuple[str, list[Any]]]
    failed: list[ManifestProblem]
    #: sha256 of the exact bytes parsed, keyed `file:<path>` as the change
    #: probe keys its content items, and `http:<read>:<page>` for HTTP pages.
    digests: dict[str, str] = field(default_factory=dict)
    #: The top-level type of each entry of `records` (parallel list).
    record_types: list[str] = field(default_factory=list)
    #: Per HTTP read: the pages fetched, with their validators, for the
    #: conditional-GET skip.
    http_pages: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    #: What each read did, for the preview and the run's metrics.
    reads: list[dict[str, Any]] = field(default_factory=list)

    def extend(self, other: "InputRead") -> None:
        self.files.extend(other.files)
        self.records.extend(other.records)
        self.failed.extend(other.failed)
        self.digests.update(other.digests)
        self.record_types.extend(other.record_types)
        self.http_pages.update(other.http_pages)
        self.reads.extend(other.reads)


def read_one_files_read(type_name: str, read: ReadSpec) -> InputRead:
    """Glob and parse one files read. A glob with no match is refused."""
    files = _glob(read)
    if not files:
        raise _refuse(
            "glob_no_match", f"{read.where}.files {read.files!r} matches no file",
            "check the path and pattern; an empty match is never read as an empty source",
            path=f"{read.where}.files",
        )
    out = InputRead(files=files, records=[], failed=[])
    count = 0
    for path in files:
        try:
            try:
                data = Path(path).read_bytes()
            except OSError as exc:
                raise FileReadError(f"cannot read {path}: {exc.strerror or exc}") from exc
            out.digests[f"file:{path}"] = hashlib.sha256(data).hexdigest()
            rows = read_file_records_for(read, path, data)
            out.records.append((path, rows))
            out.record_types.append(type_name)
            count += len(rows)
        except FileReadError as exc:
            out.failed.append(ManifestProblem(
                "file_unreadable", str(exc),
                "fix or remove the file; nothing is applied or removed until it reads",
                file=path,
            ))
    out.reads.append({
        "read": read.where, "type": type_name, "kind": "files", "what": read.describe(),
        "files": len(files), "records": count, "complete": not out.failed,
    })
    return out


def read_file_records_for(read: ReadSpec, path: str, data: bytes) -> list[Any]:
    return read_file_records(None, path, data, read=read)


def read_input_files(manifest: Manifest) -> InputRead:
    """Glob and parse every files read (no network). A glob with no match is refused."""
    out = InputRead(files=[], records=[], failed=[])
    for type_name, read in manifest.all_reads():
        if read.kind == "files":
            out.extend(read_one_files_read(type_name, read))
    return out


# --------------------------------------------------------------------------
# Mapping
# --------------------------------------------------------------------------


class _Unconvertible(ValueError):
    pass


def convert_value(value: Any, kind: str) -> Any:
    """Convert one value to a declared field type, or raise `_Unconvertible`."""
    if kind == "list":
        if not isinstance(value, list) or any(isinstance(v, (Mapping, list)) for v in value):
            raise _Unconvertible("expected a list of plain values")
        return [_scalar_text(v) for v in value if v is not None]
    if isinstance(value, (Mapping, list)):
        raise _Unconvertible(f"expected a {kind}, got a {'list' if isinstance(value, list) else 'nested object'}")
    if kind == "string":
        return _scalar_text(value)
    if kind == "integer":
        if isinstance(value, bool):
            raise _Unconvertible("expected an integer, got a boolean")
        if isinstance(value, int):
            return value
        if isinstance(value, float) and value.is_integer():
            return int(value)
        if isinstance(value, str) and re.fullmatch(r"\s*[+-]?\d+\s*", value):
            return int(value)
        raise _Unconvertible(f"{value!r} is not an integer")
    if kind == "number":
        if isinstance(value, bool):
            raise _Unconvertible("expected a number, got a boolean")
        try:
            number = float(value)
        except (TypeError, ValueError):
            raise _Unconvertible(f"{value!r} is not a number") from None
        if not math.isfinite(number):
            raise _Unconvertible(f"{value!r} is not a finite number")
        return number
    if kind == "boolean":
        if isinstance(value, bool):
            return value
        if isinstance(value, str) and value.strip().lower() in ("true", "false"):
            return value.strip().lower() == "true"
        raise _Unconvertible(f"{value!r} is not true or false")
    if kind == "date":
        if isinstance(value, str):
            try:
                return date.fromisoformat(value.strip()).isoformat()
            except ValueError:
                pass
        raise _Unconvertible(f"{value!r} is not a date (YYYY-MM-DD)")
    if kind == "datetime":
        if isinstance(value, str):
            text = value.strip()
            if text.endswith(("Z", "z")):
                text = text[:-1] + "+00:00"
            try:
                return datetime.fromisoformat(text).isoformat()
            except ValueError:
                pass
        raise _Unconvertible(f"{value!r} is not an ISO 8601 date-time")
    raise _Unconvertible(f"unknown field type {kind!r}")


def _scalar_text(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value)


@dataclass(frozen=True)
class MappedLink:
    edge_type: str
    dst_type: str
    dst_key: str


@dataclass
class MappedRecord:
    type: str
    key: str
    attrs: dict[str, Any]
    candidate_links: list[MappedLink]
    file: str
    links: list[MappedLink] = field(default_factory=list)
    passages: list[dict[str, Any]] = field(default_factory=list)
    revision: str = ""
    #: The `changes_when` value, when the type declares one and it is present.
    marker: Optional[str] = None

    @property
    def record_key(self) -> str:
        return f"{self.type}:{self.key}"


@dataclass
class RejectedRecord:
    record_key: Optional[str]
    file: str
    record: str
    reason: str
    # A record with no key value is skipped and counted, not a problem that
    # withholds deletion, unless too many records of its type have no key.
    missing_key: bool = False


@dataclass
class MappedRead:
    records: dict[str, MappedRecord]
    problems: list[ManifestProblem]
    rejected: list[RejectedRecord]
    dropped_links: int
    duplicate_keys_in_file: list[ManifestProblem]
    conflicts: int
    markers_missing: int = 0
    # Per type: objects read, and those among them with no key value.
    items_by_type: dict[str, int] = field(default_factory=dict)
    keyless_by_type: dict[str, int] = field(default_factory=dict)

    @property
    def fatal(self) -> bool:
        return bool(self.duplicate_keys_in_file)

    def counts_by_type(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for record in self.records.values():
            counts[record.type] = counts.get(record.type, 0) + 1
        return counts


def keyless_over_share(mapped: MappedRead) -> dict[str, tuple[int, int]]:
    """Types where more than MAX_KEYLESS_FRACTION of the items read have no
    key value: {type: (keyless, read)}. The key path is then probably wrong."""
    return {
        name: (count, mapped.items_by_type[name])
        for name, count in sorted(mapped.keyless_by_type.items())
        if count / mapped.items_by_type[name] > MAX_KEYLESS_FRACTION
    }


def keyless_share_message(over: Mapping[str, tuple[int, int]]) -> str:
    return "; ".join(
        f"{count} of {total} {name} records ({count / total:.0%}) have no key value"
        for name, (count, total) in over.items()
    ) + f", more than {MAX_KEYLESS_FRACTION:.0%}"


def node_id_for(source: str, type_name: str, key: str) -> str:
    return f"{source}:{type_name}:{key}"


def _change_marker(spec: TypeSpec, raw: Any) -> Optional[str]:
    if spec.changes_when is None:
        return None
    value = resolve_path(raw, spec.changes_when)
    if value is _MISSING or isinstance(value, (Mapping, list)):
        return None
    text = _scalar_text(value)
    return text if text.strip() else None


def _map_one(spec: TypeSpec, raw: Any) -> tuple[Optional[str], dict[str, Any], list[MappedLink], list[str]]:
    """(key, attrs, candidate links, conversion errors) for one raw record."""
    key = spec.key.render(raw)
    attrs: dict[str, Any] = {}
    errors: list[str] = []
    mappings: list[tuple[str, str, str]] = []
    if spec.title:
        mappings.append((TITLE_ATTR, spec.title, "string"))
    if spec.text:
        mappings.append((BODY_ATTR, spec.text, "string"))
    mappings += [(attr, path, "datetime") for attr, path in spec.dates.items()]
    mappings += [(f.name, f.path, f.kind) for f in spec.fields.values()]
    for attr, path, kind in mappings:
        value = resolve_path(raw, path)
        if value is _MISSING:
            continue
        try:
            attrs[attr] = convert_value(value, kind)
        except _Unconvertible as exc:
            errors.append(f"{attr} ({path}): {exc}")
    links: list[MappedLink] = []
    for link in spec.links.values():
        contexts: list[Any]
        if link.each is None:
            contexts = [raw]
        else:
            items = resolve_path(raw, link.each)
            contexts = [] if items is _MISSING else (items if isinstance(items, list) else [items])
        for context in contexts:
            dst = link.key.render(context)
            if dst is not None:
                candidate = MappedLink(link.name, link.to, dst)
                if candidate not in links:
                    links.append(candidate)
    return key, attrs, links, errors


def map_records(
    manifest: Manifest,
    *,
    source: str,
    inputs: Sequence[tuple[str, Sequence[Any]]],
    known_keys: frozenset[str] = frozenset(),
    input_types: Optional[Sequence[str]] = None,
) -> MappedRead:
    """Map the parsed inputs to records, links and passages. Pure.

    Inputs are taken in the order given (sorted path order, then page order).
    `input_types` names the top-level type of each input (default: the type
    the top-level `read:` feeds). A link is written only when its target
    (`<type>:<key>`) is in this read, from any of its reads, or in
    `known_keys`: earlier records the caller keeps in the record set.
    """
    default_top = manifest.top_type.name
    problems: list[ManifestProblem] = []
    rejected: list[RejectedRecord] = []
    items_by_type: dict[str, int] = {}
    keyless_by_type: dict[str, int] = {}
    duplicates_in_file: list[ManifestProblem] = []
    # Pass 1: top-level records; a later file's record replaces an earlier one.
    chosen: dict[tuple[str, str], tuple[str, int, Any]] = {}
    # Top-level records with no key value: skipped, but the items nested in
    # them are still read, so they are counted rather than silently lost.
    keyless_parents: list[tuple[str, str, int, Any]] = []
    for position, (path, rows) in enumerate(inputs):
        top = manifest.types[input_types[position] if input_types is not None else default_top]
        seen_here: dict[str, int] = {}
        for index, raw in enumerate(rows, start=1):
            where = f"record {index}"
            if not isinstance(raw, Mapping):
                rejected.append(RejectedRecord(None, path, where, "the record is not an object"))
                problems.append(ManifestProblem(
                    "record_not_object", f"{where} is not an object", "export each record as a JSON object",
                    file=path, record=where, blocking=False,
                ))
                continue
            items_by_type[top.name] = items_by_type.get(top.name, 0) + 1
            key = top.key.render(raw)
            if key is None:
                keyless_by_type[top.name] = keyless_by_type.get(top.name, 0) + 1
                keyless_parents.append((top.name, path, index, raw))
                rejected.append(RejectedRecord(
                    None, path, where, f"key {top.key.text!r} is missing or empty", missing_key=True,
                ))
                problems.append(ManifestProblem(
                    "key_missing", f"{where} has no value for key {top.key.text!r}",
                    "give every record a value at the key path, or correct types."
                    f"{top.name}.key", path=f"types.{top.name}.key", file=path, record=where, blocking=False,
                ))
                continue
            bad = key_problem(key)
            if bad is not None:
                rejected.append(RejectedRecord(None, path, where, f"key {key!r} is refused: {bad}"))
                problems.append(ManifestProblem(
                    "key_invalid", f"{where} has key {key!r}, which is refused: {bad}",
                    "remove control characters and line breaks from the key values",
                    path=f"types.{top.name}.key", file=path, record=where, blocking=False,
                ))
                continue
            if key in seen_here:
                duplicates_in_file.append(ManifestProblem(
                    "duplicate_key_in_file",
                    f"key {key!r} appears twice in one file (records {seen_here[key]} and {index}); "
                    "nothing is imported and nothing is removed",
                    "make the key unique within the file, or choose a key template that is",
                    path=f"types.{top.name}.key", file=path, record=where,
                ))
                continue
            seen_here[key] = index
            slot = (top.name, key)
            if slot in chosen and chosen[slot][0] != path:
                problems.append(ManifestProblem(
                    "duplicate_key_across_files",
                    f"key {key!r} is in {chosen[slot][0]} and {path}; the record from the later file is kept",
                    "remove the older copy if the overlap is not intended",
                    file=path, record=where, blocking=False,
                ))
            chosen[slot] = (path, index, raw)

    records: dict[str, MappedRecord] = {}
    conflicts = 0
    walk: list[tuple[str, Optional[str], str, int, Any]] = [
        (top_name, key, path, index, raw) for (top_name, key), (path, index, raw) in chosen.items()
    ] + [(top_name, None, path, index, raw) for top_name, path, index, raw in keyless_parents]
    for top_name, key, path, index, raw in walk:
        top = manifest.types[top_name]
        where = f"record {index}"
        if key is not None:
            record_key = f"{top.name}:{key}"
            _key, attrs, links, errors = _map_one(top, raw)
            if errors:
                _reject_invalid(rejected, problems, record_key, path, where, top, errors)
            else:
                records[record_key] = MappedRecord(
                    top.name, key, attrs, links, path, marker=_change_marker(top, raw),
                )
        for nested in manifest.nested_types_of(top.name):
            value = resolve_path(raw, nested.from_path or ".")
            if value is _MISSING:
                continue
            items = value if isinstance(value, list) else [value]
            for item_no, item in enumerate(items, start=1):
                nwhere = f"{where} {nested.from_path}[{item_no}]"
                if not isinstance(item, Mapping):
                    problems.append(ManifestProblem(
                        "record_not_object", f"{nwhere} is not an object",
                        f"point types.{nested.name}.from at objects", file=path, record=nwhere, blocking=False,
                    ))
                    continue
                items_by_type[nested.name] = items_by_type.get(nested.name, 0) + 1
                nkey, nattrs, nlinks, nerrors = _map_one(nested, item)
                if nkey is None:
                    keyless_by_type[nested.name] = keyless_by_type.get(nested.name, 0) + 1
                    rejected.append(RejectedRecord(
                        None, path, nwhere, f"key {nested.key.text!r} is missing", missing_key=True,
                    ))
                    problems.append(ManifestProblem(
                        "key_missing", f"{nwhere} has no value for key {nested.key.text!r}",
                        f"correct types.{nested.name}.key", path=f"types.{nested.name}.key",
                        file=path, record=nwhere, blocking=False,
                    ))
                    continue
                nbad = key_problem(nkey)
                if nbad is not None:
                    rejected.append(RejectedRecord(None, path, nwhere, f"key {nkey!r} is refused: {nbad}"))
                    problems.append(ManifestProblem(
                        "key_invalid", f"{nwhere} has key {nkey!r}, which is refused: {nbad}",
                        "remove control characters and line breaks from the key values",
                        path=f"types.{nested.name}.key", file=path, record=nwhere, blocking=False,
                    ))
                    continue
                nrecord_key = f"{nested.name}:{nkey}"
                if nerrors:
                    _reject_invalid(rejected, problems, nrecord_key, path, nwhere, nested, nerrors)
                    continue
                existing = records.get(nrecord_key)
                if existing is None:
                    records[nrecord_key] = MappedRecord(
                        nested.name, nkey, nattrs, nlinks, path, marker=_change_marker(nested, item),
                    )
                    continue
                changed = sorted(k for k, v in nattrs.items() if k in existing.attrs and existing.attrs[k] != v)
                if changed:
                    conflicts += 1
                    problems.append(ManifestProblem(
                        "nested_conflict",
                        f"{nested.name} {nkey!r} has different {', '.join(changed)} under different parents; "
                        "the later value is kept",
                        "make the nested copies agree in the export, or ignore this if the latest value is right",
                        file=path, record=nwhere, blocking=False,
                    ))
                existing.attrs.update(nattrs)
                existing.file = path
                marker = _change_marker(nested, item)
                if marker is not None:
                    existing.marker = marker
                for link in nlinks:
                    if link not in existing.candidate_links:
                        existing.candidate_links.append(link)

    # Links are written only when the target is in this read.
    dropped = 0
    samples = 0
    for record in records.values():
        for link in record.candidate_links:
            target = f"{link.dst_type}:{link.dst_key}"
            if target in records or target in known_keys:
                record.links.append(link)
                continue
            dropped += 1
            if samples < _SAMPLE_LIMIT:
                samples += 1
                problems.append(ManifestProblem(
                    "link_dropped",
                    f"{record.type} {record.key!r} links {link.edge_type} to {link.dst_type} "
                    f"{link.dst_key!r}, which is not in this read; the link is not written",
                    "add the target record to the export, or ignore this if it is expected",
                    path=f"types.{record.type}.links.{link.edge_type}", file=record.file,
                    record=record.key, blocking=False,
                ))
    if dropped > samples:
        problems.append(ManifestProblem(
            "link_dropped", f"{dropped - samples} more links point to records not in this read",
            "see the samples above", blocking=False,
        ))
    markers_missing = 0
    for record in records.values():
        _finish(record, manifest, source)
        spec = manifest.types[record.type]
        if spec.changes_when is not None and record.marker is None:
            markers_missing += 1
            if markers_missing <= _SAMPLE_LIMIT:
                problems.append(ManifestProblem(
                    "changes_when_missing",
                    f"{record.type} {record.key!r} has no value at changes_when {spec.changes_when!r}; "
                    "its revision is a hash of its whole content instead",
                    f"give every record a value at {spec.changes_when}, or remove changes_when",
                    path=f"types.{record.type}.changes_when", file=record.file, record=record.key,
                    blocking=False,
                ))
    return MappedRead(
        records=records, problems=problems, rejected=rejected, dropped_links=dropped,
        duplicate_keys_in_file=duplicates_in_file, conflicts=conflicts,
        markers_missing=markers_missing, items_by_type=items_by_type,
        keyless_by_type=keyless_by_type,
    )


def _reject_invalid(
    rejected: list[RejectedRecord], problems: list[ManifestProblem], record_key: str,
    path: str, where: str, spec: TypeSpec, errors: list[str],
) -> None:
    rejected.append(RejectedRecord(record_key, path, where, "; ".join(errors)))
    problems.append(ManifestProblem(
        "invalid_value",
        f"{spec.name} {record_key.split(':', 1)[1]!r}: {'; '.join(errors)}; the record is rejected",
        f"correct the value in the file, or change the field's type in types.{spec.name}.fields",
        path=f"types.{spec.name}", file=path, record=where, blocking=False,
    ))


def _finish(record: MappedRecord, manifest: Manifest, source: str) -> None:
    """Split long text into passages and compute the record's revision."""
    body = record.attrs.get(BODY_ATTR)
    spec = manifest.types[record.type]
    if spec.passage_type and isinstance(body, str) and len(body) > PASSAGE_THRESHOLD:
        from okg.substrate.library.extraction.chunk.window import WindowChunker

        del record.attrs[BODY_ATTR]
        chunks = WindowChunker(CHUNK_MAX_CHARS, CHUNK_OVERLAP).chunk(body)
        record.passages = [
            {
                "node_id": f"{node_id_for(source, spec.passage_type, record.key)}#{ordinal}",
                "subtype": spec.passage_type,
                "attrs": {
                    "text": chunk.text, "ordinal": ordinal,
                    "char_offset": chunk.span[0], "char_end": chunk.span[1],
                },
            }
            for ordinal, chunk in enumerate(chunks)
        ]
    if spec.changes_when is not None and record.marker is not None:
        # `changes_when`: the revision is that field's value plus the mapping.
        payload: dict[str, Any] = {
            "changes_when": spec.changes_when, "value": record.marker,
            "manifest": manifest.mapping_digest,
        }
    else:
        payload = {
            "attrs": record.attrs,
            "links": sorted((link.edge_type, link.dst_type, link.dst_key) for link in record.links),
            "passages": [p["attrs"] for p in record.passages],
            "manifest": manifest.mapping_digest,
            "chunker": CHUNKER_IDENTITY,
        }
    record.revision = hashlib.sha256(json.dumps(
        payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=str,
    ).encode("utf-8")).hexdigest()


# --------------------------------------------------------------------------
# Facts
# --------------------------------------------------------------------------


def record_identity(record: MappedRecord) -> dict[str, str]:
    return {"type": record.type, "key": record.key}


def record_emission(record: MappedRecord, *, source: str) -> Any:
    """The record's whole graph footprint, for the run's record set."""
    from okg.substrate.library.sources.base import EdgeKey, RecordEmission, RecordNode

    identity = record_identity(record)
    revision = {"sha256": record.revision}
    node_id = node_id_for(source, record.type, record.key)
    edges = [
        EdgeKey(
            src=node_id, dst=node_id_for(source, link.dst_type, link.dst_key), edge_type=link.edge_type,
            source_record_id=identity, source_revision=revision,
        )
        for link in record.links
    ] + [
        EdgeKey(
            src=node_id, dst=p["node_id"], edge_type=PASSAGE_EDGE,
            source_record_id=identity, source_revision=revision,
        )
        for p in record.passages
    ]
    return RecordEmission(
        primary=RecordNode(node_id, record.type, identity, revision),
        descendants=[RecordNode(p["node_id"], p["subtype"], identity, revision) for p in record.passages],
        edges=edges,
    )


def record_facts(record: MappedRecord, *, source: str) -> list[Any]:
    """Node and edge facts for one (changed) record."""
    from okg.substrate.library.sources.base import EdgeFact, NodeFact

    identity = record_identity(record)
    revision = {"sha256": record.revision}
    node_id = node_id_for(source, record.type, record.key)
    facts: list[Any] = [NodeFact(node_id, record.type, dict(record.attrs), identity, revision)]
    for passage in record.passages:
        facts.append(NodeFact(passage["node_id"], passage["subtype"], dict(passage["attrs"]), identity, revision))
    for link in record.links:
        facts.append(EdgeFact(
            src=node_id, dst=node_id_for(source, link.dst_type, link.dst_key), edge_type=link.edge_type,
            source_record_id=identity, source_revision=revision,
        ))
    for passage in record.passages:
        facts.append(EdgeFact(
            src=node_id, dst=passage["node_id"], edge_type=PASSAGE_EDGE,
            source_record_id=identity, source_revision=revision,
        ))
    return facts


def admission_problems(compiled: CompiledSource, facts: Iterable[Any], composed: Any = None) -> list[ManifestProblem]:
    """Would admission accept these facts? Checked without a database.

    Every node subtype and edge triple must be in the generated output
    signature; with a composed catalog, every signature subtype and triple
    must also be composed.
    """
    from okg.substrate.library.sources.base import EdgeFact, NodeFact

    nodes = set(compiled.node_types)
    triples = set(compiled.edge_triples)
    subtype_of: dict[str, str] = {}
    problems: list[ManifestProblem] = []
    fact_list = list(facts)
    for fact in fact_list:
        if isinstance(fact, NodeFact):
            subtype_of[fact.node_id] = fact.subtype
            if fact.subtype not in nodes:
                problems.append(ManifestProblem(
                    "admission_subtype", f"record type {fact.subtype!r} is not in the output signature",
                    "report this: the manifest compiler and the mapper disagree",
                ))
    for fact in fact_list:
        if isinstance(fact, EdgeFact):
            triple = (subtype_of.get(fact.src), fact.edge_type, subtype_of.get(fact.dst))
            if triple not in triples:
                problems.append(ManifestProblem(
                    "admission_edge", f"link {triple} is not in the output signature",
                    "report this: the manifest compiler and the mapper disagree",
                ))
    if composed is not None:
        composed_triples = set(composed.narrowing_triples())
        for subtype in sorted(nodes - set(composed.subtypes)):
            problems.append(ManifestProblem(
                "catalog_subtype", f"type {subtype!r} is not in the composed catalog",
                "check the generated schema file", path="types",
            ))
        for triple in sorted(triples - composed_triples):
            problems.append(ManifestProblem(
                "catalog_link", f"link {' '.join(triple)} is not a composed narrowing",
                "check the generated bridges file", path="types",
            ))
    return problems


__all__ = [
    "CompiledSource",
    "GENERATED_MARK",
    "AuthSpec",
    "InputRead",
    "Manifest",
    "PagingSpec",
    "ReadSpec",
    "extract_records",
    "match_files",
    "mapping_digest",
    "ManifestError",
    "ManifestProblem",
    "MappedRead",
    "MappedRecord",
    "additive_change_problems",
    "admission_problems",
    "check_type_names",
    "compile_manifest",
    "convert_value",
    "library_type_names",
    "load_manifest",
    "manifest_copy_relpath",
    "map_records",
    "parse_manifest",
    "parse_yaml_document",
    "read_generated_header",
    "read_input_files",
    "record_emission",
    "record_facts",
    "schema_file_subtypes",
    "type_name_round_trips",
]
