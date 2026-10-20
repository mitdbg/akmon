"""`okg source draft`: write a first source manifest from sample data.

The draft reads up to 200 sample records from local files or one GET of a
JSON API, profiles their fields with the same profiler `okg deployment
bootstrap inspect-source` uses (`deployment_bootstrap.workflow._summarize_records`),
and prints a commented `okg.source/v1` manifest:

- where the record list sits (`records:` path, or `"*"` for an object of
  objects) and, for a URL, the paging the response suggests;
- each field's type, and dates where every value parses as an ISO date;
- nested objects and lists of objects as nested types, with a link;
- a candidate key (unique and present in every sample, preferring `id`,
  `key`, `uuid` and `name`), a title and a text field.

The key and deletion lines say they were guessed. The draft never adds the
source: the user reviews the file, then runs `okg deployment add-source
--manifest`, whose preview is the real check.

Design: pact/changes/no-code-source-manifest-http/design.md.
"""
from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence
from urllib.parse import parse_qsl, urlsplit

from okg.substrate.sources.manifest import (
    CONTAINMENT_LINKS,
    OBJECT_KEY_FIELD,
    OBJECT_RECORDS,
    RESERVED_FIELD_NAMES,
    AuthSpec,
    FileReadError,
    ManifestError,
    ReadSpec,
    library_type_names,
    parse_manifest,
    parse_yaml_document,
    read_file_records,
    type_name_round_trips,
)

SAMPLE_LIMIT = 200
MAX_NESTED_TYPES = 3
_PREFERRED_KEYS = ("id", "key", "uuid", "name")
_TITLE_FIELDS = ("title", "name", "summary", "display_name", "subject", "label")
_TEXT_MIN_AVERAGE = 40
#: Field names the draft does not use as-is: graph-level names.
_RENAMED_FIELDS = frozenset({"id", "type", "subtype", "node_id", "source", "kind", "key"})
_NEXT_PATHS = ("meta.next_cursor", "next_cursor", "nextCursor", "meta.next", "next_page_token", "nextPageToken")
_DATE_SLOTS = (
    ("created", ("created", "creation")),
    ("modified", ("updated", "modified", "changed")),
    ("published", ("published", "publication", "issued")),
    ("resolved", ("resolved", "closed")),
)


class DraftError(ValueError):
    """The samples could not be read or contain no records."""

    def __init__(self, message: str, fix: str) -> None:
        super().__init__(message)
        self.fix = fix


# --------------------------------------------------------------------------
# Samples
# --------------------------------------------------------------------------


@dataclass
class Samples:
    records: list[dict[str, Any]]
    records_path: Optional[str]
    origin: str
    url: Optional[str] = None
    files_glob: Optional[str] = None
    file_format: Optional[str] = None
    paging: Optional[str] = None  # a YAML flow mapping, or "link_header"
    paging_note: Optional[str] = None
    auth: Optional[AuthSpec] = None
    notes: list[str] = field(default_factory=list)


def parse_auth_option(text: Optional[str]) -> AuthSpec:
    """`bearer:ENV`, `header:Name:ENV`, `basic:USER_ENV:PASSWORD_ENV` or
    `client_certificate:CERT_ENV:KEY_ENV[:CA_DIR_ENV]`."""
    if not text or text == "none":
        return AuthSpec()
    mode, _, rest = text.partition(":")
    parts = rest.split(":") if rest else []
    env_re = re.compile(r"^[A-Z_][A-Z0-9_]*$")
    shapes = {
        "bearer": (("token",), 1),
        "header": (("name", "value"), 2),
        "basic": (("user", "password"), 2),
        "client_certificate": (("cert", "key", "ca_dir"), 2),
    }
    if mode not in shapes:
        raise DraftError(
            f"--auth {text!r} names no known mode",
            "use bearer:ENV, header:Name:ENV, basic:USER_ENV:PASSWORD_ENV or "
            "client_certificate:CERT_ENV:KEY_ENV[:CA_DIR_ENV]",
        )
    roles, needed = shapes[mode]
    if not (needed <= len(parts) <= len(roles)):
        raise DraftError(f"--auth {text!r} has the wrong number of parts", f"write {mode}:" + ":".join(r.upper() for r in roles))
    named = dict(zip(roles, parts))
    header_name = named.pop("name", None)
    for role, value in named.items():
        if not env_re.match(value):
            raise DraftError(
                f"--auth {mode} {role} must name an environment variable, not hold a value",
                f"put the value in an environment variable and pass its name, for example {mode}:ITEMS_TOKEN",
            )
    return AuthSpec(mode=mode, env=named, header_name=header_name)


def _find_records(payload: Any) -> tuple[Optional[str], list[Any]]:
    """(records path, records) for one JSON document."""
    if isinstance(payload, list):
        return None, payload
    if not isinstance(payload, Mapping):
        raise DraftError("the sample is neither a JSON object nor a list", "point the draft at JSON records")
    best: tuple[Optional[str], list[Any]] = (None, [])
    for key, value in payload.items():
        candidates = [(str(key), value)]
        if isinstance(value, Mapping):
            candidates += [(f"{key}.{sub}", inner) for sub, inner in value.items()]
        for path, inner in candidates:
            if (
                isinstance(inner, list) and inner and all(isinstance(i, Mapping) for i in inner)
                and len(inner) > len(best[1])
            ):
                best = (path, inner)
    if best[1]:
        return best
    values = list(payload.values())
    if len(values) >= 2 and all(isinstance(v, Mapping) for v in values):
        return OBJECT_RECORDS, [{**v, OBJECT_KEY_FIELD: k} for k, v in payload.items()]
    return None, [payload]


def _resolve(payload: Any, path: str) -> Any:
    current = payload
    for segment in path.split("."):
        if not isinstance(current, Mapping) or segment not in current:
            return None
        current = current[segment]
    return current


def _paging_for_url(url: str, payload: Any, headers: Mapping[str, str]) -> tuple[Optional[str], Optional[str]]:
    query = dict(parse_qsl(urlsplit(url).query, keep_blank_values=True))
    link = headers.get("Link") or headers.get("link") or ""
    if 'rel="next"' in link.replace(" ", "") or "rel=next" in link.replace(" ", ""):
        return "link_header", "the response has a Link header with rel=next"
    if isinstance(payload, Mapping):
        for path in _NEXT_PATHS:
            value = _resolve(payload, path)
            if isinstance(value, (str, int)) and str(value) and not str(value).startswith("http"):
                param = next((k for k, v in query.items() if v == "*" or "cursor" in k.lower()), "cursor")
                return (
                    f"{{ cursor: {{ param: {param}, next: {path} }} }}",
                    f"the response carries a next cursor at {path}"
                    + ("" if param in query else f"; add {param}=* (or the API's first value) to the URL if it needs one"),
                )
    lowered = {k.lower(): k for k in query}
    for offset_name in ("offset", "skip", "start"):
        for limit_name in ("limit", "take", "count", "rows"):
            if offset_name in lowered and limit_name in lowered:
                size = query[lowered[limit_name]]
                return (
                    f"{{ offset: {{ offset_param: {lowered[offset_name]}, limit_param: "
                    f"{lowered[limit_name]}, size: {size if size.isdigit() else 100} }} }}",
                    "the URL has offset and limit parameters",
                )
    for page_name in ("page", "p"):
        for size_name in ("per_page", "per-page", "page_size", "pagesize", "size", "limit"):
            if page_name in lowered and size_name in lowered:
                size = query[lowered[size_name]]
                return (
                    f"{{ page: {{ page_param: {lowered[page_name]}, size_param: {lowered[size_name]}, "
                    f"size: {size if size.isdigit() else 100}, first: {query[lowered[page_name]] or 1} }} }}",
                    "the URL has page and page-size parameters",
                )
    return None, None


def load_url_samples(url: str, auth: AuthSpec, *, env: Optional[Mapping[str, str]] = None) -> Samples:
    from okg.substrate.sources.manifest_http import HttpReadFailed, _Client, _parse_json

    from okg.substrate.sources.manifest import normalize_url

    url = normalize_url(url)
    read = ReadSpec(url=url, auth=auth, where="draft")
    try:
        client = _Client(read, os.environ if env is None else env, sleep=None, timeout=30.0)
        response = client.get(url)
        payload = _parse_json(response, client)
    except HttpReadFailed as exc:
        if exc.status == 429:
            raise DraftError(
                str(exc),
                "the server rate-limited this request; retry after the wait it names, "
                "or pass an API key with --auth (for example --auth bearer:API_TOKEN)",
            ) from exc
        raise DraftError(str(exc), "check the URL and the --auth variable, then try again") from exc
    path, records = _find_records(payload)
    paging, note = _paging_for_url(url, payload, response.headers)
    return Samples(
        records=[r for r in records[:SAMPLE_LIMIT] if isinstance(r, Mapping)],
        records_path=path, origin=url, url=url, paging=paging, paging_note=note, auth=auth,
    )


def load_file_samples(paths: Sequence[str]) -> Samples:
    files = [str(Path(p).expanduser().resolve()) for p in paths]
    if not files:
        raise DraftError("no sample file given", "pass a file, a few files, or a URL")
    suffixes = {Path(f).suffix.lower() for f in files}
    parents = {str(Path(f).parent) for f in files}
    if len(files) == 1:
        glob = files[0]
    elif len(parents) == 1 and len(suffixes) == 1:
        glob = str(Path(next(iter(parents))) / f"*{next(iter(suffixes))}")
    else:
        glob = files[0]
    records: list[Any] = []
    records_path: Optional[str] = None
    fmt: Optional[str] = None
    for path in files:
        suffix = Path(path).suffix.lower()
        fmt = {
            ".json": "json", ".jsonl": "jsonl", ".ndjson": "jsonl", ".csv": "csv",
            ".yaml": "yaml", ".yml": "yaml",
        }.get(suffix)
        if fmt is None:
            raise DraftError(
                f"cannot tell the format of {path}", "draft from .json, .jsonl, .csv, .yaml or .yml files",
            )
        try:
            if fmt in ("json", "yaml"):
                if fmt == "json":
                    payload = json.loads(Path(path).read_text(encoding="utf-8-sig"))
                else:
                    payload = parse_yaml_document(Path(path).read_text(encoding="utf-8-sig"), path)
                found_path, found = _find_records(payload)
                records_path = records_path or found_path
                records.extend(found)
            else:
                records.extend(read_file_records(None, path, read=ReadSpec(files=path, format=fmt)))
        except (OSError, json.JSONDecodeError, FileReadError) as exc:
            raise DraftError(f"cannot read {path}: {exc}", "fix or remove the file") from exc
        if len(records) >= SAMPLE_LIMIT:
            break
    notes = []
    if len(files) > 1 and glob == files[0]:
        notes.append("the files are in different folders or formats; the draft reads only the first")
    return Samples(
        records=[r for r in records[:SAMPLE_LIMIT] if isinstance(r, Mapping)],
        records_path=records_path, origin=glob, files_glob=glob, file_format=fmt, notes=notes,
    )


# --------------------------------------------------------------------------
# Profiling
# --------------------------------------------------------------------------


def _snake(name: str) -> str:
    text = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", str(name))
    text = re.sub(r"[^A-Za-z0-9]+", "_", text).strip("_").lower()
    if not text or not text[0].isalpha():
        text = f"f_{text}".rstrip("_")
    return text


def _singular(name: str) -> str:
    if name.endswith("ies") and len(name) > 4:
        return name[:-3] + "y"
    if name.endswith("sses"):
        return name[:-2]
    if name.endswith("s") and not name.endswith("ss") and len(name) > 3:
        return name[:-1]
    return name


def _is_date(text: str) -> Optional[str]:
    """'date' or 'datetime' when `text` parses as ISO 8601."""
    value = text.strip()
    if len(value) < 10:
        return None
    try:
        date.fromisoformat(value)
        return "date"
    except ValueError:
        pass
    try:
        datetime.fromisoformat(value[:-1] + "+00:00" if value.endswith(("Z", "z")) else value)
        return "datetime"
    except ValueError:
        return None


@dataclass
class FieldProfile:
    name: str
    kinds: set[str]
    count: int
    unique: int
    example: Any
    avg_len: float = 0.0
    date_kind: Optional[str] = None


def profile(records: Sequence[Mapping[str, Any]]) -> list[FieldProfile]:
    """Per field: types, presence and uniqueness, from the bootstrap profiler."""
    from okg.substrate.deployment_bootstrap.workflow import _summarize_records

    fields, _ids, _texts = _summarize_records([dict(r) for r in records])
    out: list[FieldProfile] = []
    order = {}
    for record in records:
        for key in record:
            order.setdefault(key, len(order))
    for summary in sorted(fields, key=lambda f: order.get(f["name"], 0)):
        name = summary["name"]
        values = [r[name] for r in records if r.get(name) is not None]
        strings = [v for v in values if isinstance(v, str)]
        kinds = {k for k in summary["types"] if k != "null"}
        date_kind = None
        if strings and len(strings) == len(values):
            parsed = {_is_date(s) for s in strings}
            if None not in parsed:
                date_kind = "datetime" if "datetime" in parsed else "date"
        out.append(FieldProfile(
            name=name, kinds=kinds, count=summary["count"], unique=summary["unique_count"],
            example=summary["example"],
            avg_len=(sum(len(s) for s in strings) / len(strings)) if strings else 0.0,
            date_kind=date_kind,
        ))
    return out


def _scalar_kind(kinds: set[str]) -> Optional[str]:
    if kinds == {"integer"}:
        return "integer"
    if kinds and kinds <= {"integer", "number"}:
        return "number"
    if kinds == {"boolean"}:
        return "boolean"
    if kinds == {"string"}:
        return "string"
    if kinds and kinds <= {"string", "integer", "number", "boolean"}:
        return "string"
    return None


@dataclass
class KeyChoice:
    path: Optional[str]
    alternatives: list[str]
    reason: str


def choose_key(profiles: Sequence[FieldProfile], total: int, *, unique_required: bool = True) -> KeyChoice:
    """A field present in every sample (and unique, for top-level records)."""
    candidates = [
        p.name for p in profiles
        if p.count == total and _scalar_kind(p.kinds) in ("string", "integer")
        and (not unique_required or p.unique == total)
    ]
    if not candidates:
        return KeyChoice(None, [], "no field is present in every sample" + (" and unique" if unique_required else ""))
    for preferred in (*_PREFERRED_KEYS, OBJECT_KEY_FIELD):
        if preferred in candidates:
            return KeyChoice(preferred, [c for c in candidates if c != preferred][:5], f"{preferred!r} is unique and always present")
    ranked = sorted(candidates, key=lambda c: (not re.search(r"(_id$|uuid|key|code|slug)", c.lower()), candidates.index(c)))
    return KeyChoice(
        ranked[0], ranked[1:6],
        "AMBIGUOUS: no field is named id, key, uuid or name; several fields are unique",
    )


def _nested_key(items: Sequence[Mapping[str, Any]]) -> tuple[Optional[str], Optional[str], bool]:
    """(key path, title path, strong) for nested items: the key is present in
    every item; `strong` when it is named id, key, uuid or name."""
    total = len(items)
    profiles = profile(items)
    choice = choose_key(profiles, total, unique_required=False)
    title = next((n for n in _TITLE_FIELDS if any(p.name == n for p in profiles)), None)
    if choice.path is not None and choice.path in _PREFERRED_KEYS:
        return choice.path, title, True
    # One level deeper: authorships[].author.id
    for p in profiles:
        if p.kinds == {"object"} and p.count == total:
            inner = [i[p.name] for i in items if isinstance(i.get(p.name), Mapping)]
            inner_profiles = profile(inner)
            for preferred in _PREFERRED_KEYS:
                if any(ip.name == preferred and ip.count == len(inner) for ip in inner_profiles):
                    inner_title = next(
                        (f"{p.name}.{n}" for n in _TITLE_FIELDS if any(ip.name == n for ip in inner_profiles)), None,
                    )
                    return f"{p.name}.{preferred}", title or inner_title, True
    return choice.path, title, False


# --------------------------------------------------------------------------
# Writing the manifest
# --------------------------------------------------------------------------


def _q(text: str) -> str:
    return json.dumps(text)


def _type_name(samples: Samples, explicit: Optional[str]) -> tuple[str, str]:
    """(type name, prefix for nested types)."""
    if explicit:
        name = _snake(explicit)
        return name, name.split("_")[0] if "_" in name else name
    if samples.url:
        parts = urlsplit(samples.url)
        labels = [label for label in parts.hostname.split(".") if label not in ("www", "api")] if parts.hostname else []
        prefix = _snake(labels[-2] if len(labels) >= 2 else (labels[0] if labels else "api"))
        segments = [s for s in parts.path.split("/") if s and not re.fullmatch(r"v\d+", s)]
        stem = _singular(_snake(segments[-1])) if segments else "record"
    else:
        prefix = _snake(Path(samples.origin).parent.name or "src")
        stem = _singular(_snake(Path(samples.origin).stem.replace("*", "") or "record"))
    name = stem if stem.startswith(prefix) else f"{prefix}_{stem}"
    return name, prefix


def _safe_type_name(name: str, prefix: str) -> str:
    reserved = library_type_names()
    candidate = name
    if candidate in reserved or not type_name_round_trips(candidate):
        candidate = f"{prefix}_{name}" if not name.startswith(prefix) else f"src_{name}"
    if not type_name_round_trips(candidate):
        candidate = re.sub(r"_(\d)", r"\1", candidate)
    return candidate


def _field_name(raw: str, stem: str, used: set[str]) -> Optional[str]:
    name = _snake(raw)
    if name in RESERVED_FIELD_NAMES or name in _RENAMED_FIELDS:
        name = f"{stem}_{name}"
    if name in used:
        return None
    used.add(name)
    return name


def _date_slot(name: str, taken: set[str]) -> Optional[str]:
    lowered = name.lower()
    for slot, words in _DATE_SLOTS:
        if slot not in taken and any(w in lowered for w in words):
            return slot
    return None


def _type_block(
    records: Sequence[Mapping[str, Any]],
    *,
    stem: str,
    key_path: Optional[str],
    key_comment: str,
    title_hint: Optional[str],
    exclude: set[str],
) -> tuple[list[str], list[tuple[str, str, str, Optional[str], bool]], list[str]]:
    """(lines under the type, nested candidates, notes). A nested candidate is
    (field, kind 'list'|'object', key path, title path)."""
    profiles = profile(records)
    total = len(records)
    lines: list[str] = []
    notes: list[str] = []
    key_line = f'    key: "{{{key_path}}}"' if key_path else '    key: "{CHOOSE_A_FIELD}"'
    lines.append(f"{key_line}   # GUESSED: {key_comment}. Check it identifies one record and never changes.")
    by_name = {p.name: p for p in profiles}
    title = title_hint or next((n for n in _TITLE_FIELDS if n in by_name and _scalar_kind(by_name[n].kinds) == "string"), None)
    if title:
        lines.append(f"    title: {title}")
    strings = [
        p for p in profiles
        if _scalar_kind(p.kinds) == "string" and p.name not in (title, key_path) and p.date_kind is None
        and p.name not in _TITLE_FIELDS
        and p.avg_len >= _TEXT_MIN_AVERAGE
    ]
    text = max(strings, key=lambda p: p.avg_len).name if strings else None
    if text:
        lines.append(f"    text: {text}   # the longest text field (average {int(by_name[text].avg_len)} characters)")
    dates: list[str] = []
    fields: list[str] = []
    nested: list[tuple[str, str, str, Optional[str], bool]] = []
    used: set[str] = set()
    taken_slots: set[str] = set()
    for p in profiles:
        if p.name in (key_path, title, text) or p.name in exclude or p.name == OBJECT_KEY_FIELD:
            continue
        if "." in p.name or not p.name.strip():
            notes.append(f"field {p.name!r} has a dot or no name and cannot be addressed by a path; skipped")
            continue
        if p.date_kind is not None:
            slot = _date_slot(p.name, taken_slots)
            if slot is not None:
                taken_slots.add(slot)
                dates.append(f"      {slot}: {p.name}")
                continue
            fname = _field_name(p.name, stem, used)
            if fname:
                fields.append(f"      {fname}: {{ from: {p.name}, type: {p.date_kind} }}")
            continue
        scalar = _scalar_kind(p.kinds)
        if scalar is not None:
            fname = _field_name(p.name, stem, used)
            if fname is None:
                continue
            if scalar == "string" and fname == p.name:
                fields.append(f"      {fname}: {p.name}")
            else:
                fields.append(f"      {fname}: {{ from: {p.name}, type: {scalar} }}")
            continue
        if p.kinds == {"array"}:
            values = [r[p.name] for r in records if isinstance(r.get(p.name), list)]
            items = [i for v in values for i in v]
            if items and all(isinstance(i, Mapping) for i in items):
                nkey, ntitle, strong = _nested_key(items)
                if nkey:
                    nested.append((p.name, "list", nkey, ntitle, strong))
                else:
                    notes.append(f"list {p.name!r} holds objects with no field present in every item; not mapped")
                continue
            if all(not isinstance(i, (Mapping, list)) for i in items):
                fname = _field_name(p.name, stem, used)
                if fname:
                    fields.append(f"      {fname}: {{ from: {p.name}, type: list }}")
                continue
        if p.kinds == {"object"}:
            items = [r[p.name] for r in records if isinstance(r.get(p.name), Mapping)]
            nkey, ntitle, strong = _nested_key(items)
            if nkey and strong and p.count == total:
                nested.append((p.name, "object", nkey, ntitle, strong))
            else:
                subs = sorted({k for i in items for k, v in i.items() if not isinstance(v, (Mapping, list))})[:3]
                hint = f" (for example {p.name}.{subs[0]})" if subs else ""
                notes.append(f"object {p.name!r} has no key; map its parts with dotted paths{hint}")
            continue
        notes.append(f"field {p.name!r} mixes types ({', '.join(sorted(p.kinds))}); not mapped")
    if dates:
        lines.append("    dates:")
        lines.extend(dates)
    if fields:
        lines.append("    fields:")
        lines.extend(fields)
    return lines, nested, notes


def draft_manifest(samples: Samples, *, type_name: Optional[str] = None) -> str:
    """The commented manifest text for these samples."""
    records = samples.records
    if not records:
        raise DraftError(
            f"{samples.origin} has no records to sample",
            "point the draft at a file or URL that returns at least one record",
        )
    name, prefix = _type_name(samples, type_name)
    name = _safe_type_name(name, prefix)
    stem = name.split("_")[-1]
    top_profiles = profile(records)
    choice = choose_key(top_profiles, len(records))
    if samples.records_path == OBJECT_RECORDS:
        choice = KeyChoice(OBJECT_KEY_FIELD, [], "each record is a value of a JSON object; _key is its name")
    key_comment = choice.reason + (f"; other candidates: {', '.join(choice.alternatives)}" if choice.alternatives else "")
    body, nested, notes = _type_block(
        records, stem=stem, key_path=choice.path, key_comment=key_comment,
        title_hint=None, exclude=set(),
    )
    out: list[str] = [
        "# Drafted by `okg source draft` from " + samples.origin + f" ({len(records)} sample records).",
        "# Review every line. Lines marked GUESSED must be checked before you add the source:",
        "#   okg deployment add-source <deployment> <name> --manifest <this file>",
        "# shows a preview without writing anything.",
        "manifest: okg.source/v1",
        f"description: {_q('Records from ' + samples.origin)}",
    ]
    out.append("types:")
    out.append(f"  {name}:")
    out.append("    read:")
    if samples.url:
        out.append(f"      url: {_q(samples.url)}")
        if samples.auth is not None and samples.auth.mode != "none":
            auth = samples.auth
            if auth.mode == "header":
                out.append(f"      auth: {{ header: {{ name: {auth.header_name}, value: {auth.env['value']} }} }}")
            else:
                inner = ", ".join(f"{role}: {env}" for role, env in auth.env.items())
                out.append(f"      auth: {{ {auth.mode}: {{ {inner} }} }}")
        if samples.paging:
            out.append(f"      paging: {samples.paging}   # GUESSED: {samples.paging_note}")
        else:
            out.append("      # paging: none found. If the API pages, add offset, page, cursor or link_header paging.")
    else:
        out.append(f"      files: {_q(samples.files_glob or samples.origin)}")
        if samples.file_format:
            out.append(f"      format: {samples.file_format}")
    if samples.records_path:
        out.append(f"      records: {_q(samples.records_path)}")
    nested_blocks: list[str] = []
    links: list[str] = []
    used_names = {name}
    # Lists of objects with an id-like key first, then such objects, then
    # lists keyed by any always-present field.
    nested = sorted(nested, key=lambda n: (not n[4], n[1] != "list"))
    for field_name, kind, nkey, ntitle, _strong in nested[:MAX_NESTED_TYPES]:
        child = _safe_type_name(f"{prefix}_{_singular(_snake(field_name))}", prefix)
        if child in used_names:
            continue
        used_names.add(child)
        link_name = _snake(field_name)
        if link_name in CONTAINMENT_LINKS:
            link_name = f"has_{link_name}"
        each = f", each: {field_name}" if kind == "list" else ""
        key_ref = nkey if kind == "list" else f"{field_name}.{nkey}"
        links.append(f'      {link_name}: {{ to: {child}{each}, key: "{{{key_ref}}}" }}')
        nested_blocks.append(f"  {child}:")
        nested_blocks.append(f"    from: {field_name}")
        nested_blocks.append(f'    key: "{{{nkey}}}"   # GUESSED: present in every {field_name} item')
        if ntitle:
            nested_blocks.append(f"    title: {ntitle}")
    for field_name, *_rest in nested[MAX_NESTED_TYPES:]:
        notes.append(f"nested {field_name!r} could be another nested type (the draft proposes at most {MAX_NESTED_TYPES})")
    out.extend(body)
    if links:
        out.append("    links:")
        out.extend(links)
    out.extend(nested_blocks)
    out.append(
        "deletion: missing_from_full_read   # GUESSED: a record missing from a complete read is "
        "removed; write `never` to keep every record ever read."
    )
    if samples.url:
        out.append(
            "# max_deleted_fraction: 0.5   # the default for an API: a run that would remove more "
            "than half the records removes nothing."
        )
    for note in [*samples.notes, *notes]:
        out.append(f"# Note: {note}")
    text = "\n".join(out) + "\n"
    try:
        parse_manifest(text, base_dir=Path.cwd(), origin="the draft")
    except ManifestError as exc:  # pragma: no cover - a draft bug, never user input
        if choice.path is not None:
            raise RuntimeError(f"okg source draft wrote a manifest it cannot read: {exc}\n{text}") from exc
    return text


def draft(
    targets: Sequence[str],
    *,
    auth: Optional[str] = None,
    type_name: Optional[str] = None,
    env: Optional[Mapping[str, str]] = None,
) -> str:
    """Draft a manifest from one URL or from local sample files."""
    if len(targets) == 1 and urlsplit(targets[0]).scheme in ("http", "https"):
        samples = load_url_samples(targets[0], parse_auth_option(auth), env=env)
    else:
        if auth:
            raise DraftError("--auth applies to a URL, not to files", "remove --auth")
        samples = load_file_samples(targets)
    return draft_manifest(samples, type_name=type_name)


__all__ = ["DraftError", "SAMPLE_LIMIT", "draft", "draft_manifest", "load_file_samples", "load_url_samples", "parse_auth_option"]
