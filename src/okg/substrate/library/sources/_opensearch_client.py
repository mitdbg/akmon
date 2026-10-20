"""Cluster-metadata reader for OpenSearch, and its recorded twin.

Two providers, one parser. `HttpMetadataProvider` talks to a real cluster
over its REST API; `RecordedMetadataProvider` replays response bodies
captured from one. Both hand the same raw bodies to `parse_snapshot`, so a
test exercises the production parsing path and a fixture that drifts from
the real cluster shows up as a parse difference rather than as an untested
branch.

Why no OpenSearch client library (PACT `opensearch-reference-integration`,
issue #2014): the six endpoints below are plain JSON over HTTP and
`requests` is already a core dependency. `opensearch-py` would add a
transitive surface for read-only metadata calls that fit in one small
module.

Six requests, independent of how many indices the cluster holds:

    GET /                 cluster name, uuid, version
    GET /_cluster/health  status, node count
    GET /_cat/indices     per-index uuid, doc count, store bytes
    GET /_mapping         per-index field mappings
    GET /_settings        per-index settings, incl. the ISM policy
    GET /_alias           per-index aliases

`GET /_settings` is deliberately cluster-wide rather than per index
(operator decision D2, 2026-09-20): the response is keyed by index, so one
request carries every index's settings.
"""
from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass, field
from fnmatch import fnmatch
from pathlib import Path
from typing import Any, Mapping, Optional, Protocol
from urllib.parse import urlsplit, urlunsplit

from okg.substrate.errors import SubstrateError


#: Field-summary bounds. A mapping can hold thousands of fields and
#: megabytes of JSON; an index node must stay cheap to return from a
#: graph read. These are an order of magnitude below the 16 KiB inline
#: sample cap that Sprint 20's charter sets for live payloads.
MAX_SUMMARY_FIELDS = 40
MAX_SUMMARY_DEPTH = 4
MAX_FIELD_NAME_CHARS = 128
MAX_SUMMARY_BYTES = 8192

#: Index names beginning with one of these prefixes are out of scope.
#: OpenSearch ships its own bookkeeping indices (`.opensearch-*`,
#: `.plugins-ml-config`, the ISM config index) whose presence depends on
#: which plugins booted, so ingesting them would make the node count a
#: function of the image rather than of the operator's data.
SCOPE_EXCLUDED_PREFIXES: tuple[str, ...] = (".",)

#: Additional out-of-scope names, matched as shell globs.
#:
#: The dot prefix is a convention, not a rule the cluster enforces. The
#: 2.19.1 reference image creates `top_queries-<date>-<n>` from the
#: query-insights plugin with no leading dot, and `index.hidden` does not
#: separate the two groups either: `.opensearch-observability` and
#: `.plugins-ml-config` are both NOT hidden while `.opendistro-ism-config`
#: is. So there is no derivable rule, and the honest answer is a declared
#: one -- a registry parameter the operator sets, with what it dropped
#: reported on the cluster node rather than vanishing.
DEFAULT_EXCLUDED_GLOBS: tuple[str, ...] = ()

DEFAULT_TIMEOUT_SECONDS = 10.0


class OpenSearchMetadataError(SubstrateError):
    """A metadata read failed, or the cluster answered something unusable.

    Never raised to mean "nothing there". A read that cannot be completed
    must not degrade into an empty scope, because an empty completed scope
    retracts every node the source owns.
    """

    code = "opensearch_metadata_unavailable"


@dataclass(frozen=True)
class MappingField:
    """One flattened mapping field: a dotted path and its type."""

    name: str
    type: str

    def as_dict(self) -> dict[str, str]:
        return {"name": self.name, "type": self.type}


@dataclass(frozen=True)
class FieldSummary:
    """The bounded rendering of one index's field mapping."""

    fields: tuple[MappingField, ...]
    total: int
    truncated: bool

    def as_list(self) -> list[dict[str, str]]:
        return [f.as_dict() for f in self.fields]


@dataclass(frozen=True)
class IndexMetadata:
    """One index, as the graph will model it.

    `uuid` is the identity. A rename keeps it, a rollover mints a new one,
    and a delete-then-recreate under the same name produces a different
    one -- all three of which keying on the name would get wrong.
    """

    uuid: str
    name: str
    aliases: tuple[str, ...]
    summary: FieldSummary
    mapping_read: bool
    retention: Optional[str]
    retention_read: bool
    doc_count: Optional[int]
    store_bytes: Optional[int]

    def structural_payload(self) -> dict[str, Any]:
        """What the content hash covers.

        Volatile metrics are NOT here. `doc_count` and `store_bytes` move
        on every write to a live cluster; hashing them would mint a fresh
        `source_revision` for every node on every sync and churn the graph
        with no structural change. They are still recorded as attributes,
        so a read returns current figures -- they just do not define a
        revision.
        """
        return {
            "uuid": self.uuid,
            "name": self.name,
            "aliases": list(self.aliases),
            "fields": self.summary.as_list(),
            "field_count": self.summary.total,
            "field_summary_truncated": self.summary.truncated,
            "mapping_read": self.mapping_read,
            "retention": self.retention,
            "retention_read": self.retention_read,
        }


@dataclass(frozen=True)
class ClusterMetadata:
    """The cluster itself. `health` and `node_count` are volatile."""

    uuid: str
    name: str
    version: Optional[str]
    health: Optional[str]
    node_count: Optional[int]

    def structural_payload(self) -> dict[str, Any]:
        return {"uuid": self.uuid, "name": self.name, "version": self.version}


@dataclass(frozen=True)
class ReadAnomalies:
    """Everything the read declined to use, counted.

    Each of these was a silent `continue` or a swallowed coercion. A
    dropped row that nobody counts is a graph quietly disagreeing with
    its cluster, so they are carried out of the parser and onto the
    cluster node.
    """

    #: `_cat/indices` rows that were not JSON objects at all.
    malformed_index_rows: int = 0
    #: Rows whose `docs.count` or `store.size` would not parse as an
    #: integer. The index is still modelled; the metric reads unknown.
    unparsable_metrics: tuple[str, ...] = ()
    #: Mapping properties whose body was not an object, so no type could
    #: be read from them.
    malformed_mapping_fields: int = 0
    #: Field paths cut at MAX_FIELD_NAME_CHARS. Two different fields can
    #: truncate to the same name, which would silently merge them.
    truncated_field_names: int = 0
    #: Distinct truncated names that collided with another truncated
    #: name. Not a guess: counted while flattening.
    truncated_name_collisions: int = 0

    def total(self) -> int:
        return (
            self.malformed_index_rows
            + len(self.unparsable_metrics)
            + self.malformed_mapping_fields
            + self.truncated_field_names
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "malformed_index_rows": self.malformed_index_rows,
            "unparsable_metrics": list(self.unparsable_metrics),
            "malformed_mapping_fields": self.malformed_mapping_fields,
            "truncated_field_names": self.truncated_field_names,
            "truncated_name_collisions": self.truncated_name_collisions,
        }


@dataclass(frozen=True)
class ClusterSnapshot:
    """One complete metadata read."""

    cluster: ClusterMetadata
    indices: tuple[IndexMetadata, ...]
    excluded: tuple[str, ...] = ()
    excluded_globs: tuple[str, ...] = ()
    anomalies: ReadAnomalies = ReadAnomalies()

    def by_name(self) -> dict[str, IndexMetadata]:
        return {index.name: index for index in self.indices}


def content_revision(payload: Mapping[str, Any]) -> str:
    """Stable sha256 over a structural payload.

    `sort_keys` and a separator-tight encoding make the digest independent
    of dict ordering, so two reads of an unchanged index agree.
    """
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _flatten_properties(
    properties: Mapping[str, Any],
    *,
    prefix: str = "",
    depth: int = 1,
    counters: Optional[dict[str, int]] = None,
) -> list[MappingField]:
    """Flatten a mapping's `properties` tree to dotted paths.

    Descends at most `MAX_SUMMARY_DEPTH` levels. A subtree deeper than
    that contributes its depth-limit ancestor once, typed `object`, rather
    than being dropped without trace.

    `multi_fields` -- an OpenSearch field's `fields` sub-mapping, such as
    `title.keyword` beside a `text` title -- are deliberately NOT
    traversed: they are alternate analyses of the same field, not
    separate fields, and counting them would overstate `field_count`.
    The decision is recorded here rather than left to be inferred from a
    missing branch.

    Anything genuinely dropped is counted into `counters`.
    """
    counts = {} if counters is None else counters
    out: list[MappingField] = []
    for key in sorted(properties):
        raw = properties[key]
        if not isinstance(raw, Mapping):
            # A property whose body is not an object carries no type.
            counts["malformed_mapping_fields"] = counts.get(
                "malformed_mapping_fields", 0) + 1
            continue
        full = f"{prefix}{key}"
        path = full[:MAX_FIELD_NAME_CHARS]
        if path != full:
            counts["truncated_field_names"] = counts.get(
                "truncated_field_names", 0) + 1
        children = raw.get("properties")
        if isinstance(children, Mapping) and children:
            if depth >= MAX_SUMMARY_DEPTH:
                out.append(MappingField(name=path, type="object"))
                continue
            out.extend(
                _flatten_properties(
                    children, prefix=f"{path}.", depth=depth + 1,
                    counters=counts,
                )
            )
            continue
        declared = raw.get("type")
        out.append(
            MappingField(
                name=path,
                type=str(declared) if declared is not None else "object",
            )
        )
    return out


def summarize_mapping(
    mapping_body: Mapping[str, Any],
    counters: Optional[dict[str, int]] = None,
) -> FieldSummary:
    """Bound one index's mapping to something a graph read can carry.

    Bounds, all of them exact: at most `MAX_SUMMARY_FIELDS` entries, sorted
    by name; at most `MAX_SUMMARY_DEPTH` levels of nesting; field names cut
    at `MAX_FIELD_NAME_CHARS`; the serialized list capped at
    `MAX_SUMMARY_BYTES`. `total` is the true field count BEFORE truncation,
    so a reader can see what was left out.
    """
    properties = mapping_body.get("mappings", {})
    if isinstance(properties, Mapping):
        properties = properties.get("properties", {})
    if not isinstance(properties, Mapping):
        properties = {}

    counts = {} if counters is None else counters
    flattened = sorted(
        _flatten_properties(properties, counters=counts),
        key=lambda f: f.name,
    )
    # Two different fields can truncate to the same 128-character name,
    # which would silently merge them. Counted, not guessed at.
    seen: set[str] = set()
    for field_ in flattened:
        if field_.name in seen:
            counts["truncated_name_collisions"] = counts.get(
                "truncated_name_collisions", 0) + 1
        seen.add(field_.name)
    total = len(flattened)
    kept = list(flattened[:MAX_SUMMARY_FIELDS])
    truncated = total > len(kept)

    # Byte cap. Drop from the end until the rendered list fits; a name long
    # enough to blow the cap on its own would otherwise loop forever, so
    # the empty list is the floor.
    while kept:
        rendered = json.dumps(
            [f.as_dict() for f in kept], separators=(",", ":"),
        )
        if len(rendered.encode("utf-8")) <= MAX_SUMMARY_BYTES:
            break
        kept.pop()
        truncated = True

    return FieldSummary(fields=tuple(kept), total=total, truncated=truncated)


def _retention_of(settings_body: Mapping[str, Any]) -> Optional[str]:
    """The lifecycle policy attached to an index, or None if there is none.

    None here means READ AND ABSENT. "Not read at all" is carried
    separately by `retention_read`, because rendering an unread setting as
    None would assert "no retention policy" on no evidence.
    """
    settings = settings_body.get("settings")
    if not isinstance(settings, Mapping):
        return None
    index = settings.get("index")
    if not isinstance(index, Mapping):
        return None
    ism = index.get("plugins")
    if isinstance(ism, Mapping):
        state = ism.get("index_state_management")
        if isinstance(state, Mapping):
            policy = state.get("policy_id")
            if policy:
                return str(policy)
    lifecycle = index.get("lifecycle")
    if isinstance(lifecycle, Mapping):
        name = lifecycle.get("name")
        if name:
            return str(name)
    return None


def _int_or_none(value: Any) -> Optional[int]:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _in_scope(
    name: str,
    excluded_prefixes: tuple[str, ...],
    excluded_globs: tuple[str, ...] = (),
) -> bool:
    if any(name.startswith(prefix) for prefix in excluded_prefixes):
        return False
    return not any(fnmatch(name, pattern) for pattern in excluded_globs)


def parse_snapshot(
    *,
    root: Mapping[str, Any],
    health: Mapping[str, Any],
    cat_indices: Any,
    mappings: Mapping[str, Any],
    settings: Optional[Mapping[str, Any]],
    aliases: Mapping[str, Any],
    excluded_prefixes: tuple[str, ...] = SCOPE_EXCLUDED_PREFIXES,
    excluded_globs: tuple[str, ...] = DEFAULT_EXCLUDED_GLOBS,
) -> ClusterSnapshot:
    """Turn six raw response bodies into one snapshot.

    `settings` is `None` when the settings request itself failed. That is
    not the same as a cluster with no lifecycle policies, and the
    distinction survives into every index's `retention_read` flag.
    """
    cluster_uuid = root.get("cluster_uuid")
    cluster_name = root.get("cluster_name")
    if not cluster_uuid or not cluster_name:
        raise OpenSearchMetadataError(
            "cluster root response carries no cluster_uuid/cluster_name; "
            f"got keys {sorted(root)}",
            data={"keys": sorted(root)},
            fix_hint="point the source at an OpenSearch cluster root endpoint",
        )
    version = root.get("version")
    version_number = (
        str(version.get("number"))
        if isinstance(version, Mapping) and version.get("number") is not None
        else None
    )
    cluster = ClusterMetadata(
        uuid=str(cluster_uuid),
        name=str(cluster_name),
        version=version_number,
        health=(
            str(health.get("status")) if health.get("status") is not None
            else None
        ),
        node_count=_int_or_none(health.get("number_of_nodes")),
    )

    if not isinstance(cat_indices, list):
        raise OpenSearchMetadataError(
            "_cat/indices did not return a list; refusing to treat an "
            "unreadable index listing as an empty cluster",
            data={"type": type(cat_indices).__name__},
            fix_hint="request _cat/indices with format=json",
        )
    if not cat_indices:
        # A running OpenSearch always holds at least one index of its own,
        # so a zero-row listing means the read was filtered or the cluster
        # is mid-restore -- never "this cluster has no indices". Accepting
        # it would complete the scope with nothing in it and retract every
        # index node the source owns.
        raise OpenSearchMetadataError(
            "_cat/indices returned zero rows; a live cluster always holds "
            "its own system indices, so this is a filtered or incomplete "
            "read, not an empty cluster",
            data={"rows": 0},
            fix_hint=(
                "check the credential's index permissions and that the "
                "cluster has finished recovering"
            ),
        )

    indices: list[IndexMetadata] = []
    excluded: list[str] = []
    counts: dict[str, int] = {}
    unparsable: list[str] = []
    retention_read = settings is not None
    rows = [r for r in cat_indices if isinstance(r, Mapping)]
    counts["malformed_index_rows"] = len(cat_indices) - len(rows)
    for row in sorted(rows, key=lambda r: str(r.get("index", ""))):
        name = str(row.get("index", ""))
        uuid = row.get("uuid")
        if not name or not uuid:
            raise OpenSearchMetadataError(
                "_cat/indices row is missing index or uuid; the uuid is "
                f"this source's identity key. Row: {dict(row)!r}",
                data={"row": dict(row)},
                fix_hint="request _cat/indices with h=index,uuid,docs.count,store.size",
            )
        if not _in_scope(name, excluded_prefixes, excluded_globs):
            excluded.append(name)
            continue
        alias_body = aliases.get(name)
        alias_names: tuple[str, ...] = ()
        if isinstance(alias_body, Mapping):
            found = alias_body.get("aliases")
            if isinstance(found, Mapping):
                alias_names = tuple(sorted(str(a) for a in found))
        # A 200 that simply omits an in-scope index is NOT an index with
        # no fields. Summarising the absence would report field_count 0
        # and mint a new content hash claiming the mapping changed, which
        # is a graph that lies about a cluster nobody touched.
        mapping_body = mappings.get(name)
        mapping_read = isinstance(mapping_body, Mapping)
        summary = summarize_mapping(
            mapping_body if mapping_read else {}, counts,
        )
        settings_body = (
            settings.get(name) if isinstance(settings, Mapping) else None
        )
        doc_count = _int_or_none(row.get("docs.count"))
        store_bytes = _int_or_none(row.get("store.size"))
        if (row.get("docs.count") is not None and doc_count is None) or (
            row.get("store.size") is not None and store_bytes is None
        ):
            # The index is still modelled; the metric reads unknown and
            # the fact that it could not be parsed is carried, not lost.
            unparsable.append(name)
        indices.append(
            IndexMetadata(
                uuid=str(uuid),
                name=name,
                aliases=alias_names,
                summary=summary,
                mapping_read=mapping_read,
                retention=(
                    _retention_of(settings_body)
                    if isinstance(settings_body, Mapping) else None
                ),
                retention_read=retention_read,
                doc_count=_int_or_none(row.get("docs.count")),
                store_bytes=_int_or_none(row.get("store.size")),
            )
        )

    return ClusterSnapshot(
        cluster=cluster,
        indices=tuple(indices),
        excluded=tuple(sorted(excluded)),
        excluded_globs=tuple(excluded_globs),
        anomalies=ReadAnomalies(
            malformed_index_rows=counts.get("malformed_index_rows", 0),
            unparsable_metrics=tuple(sorted(set(unparsable))),
            malformed_mapping_fields=counts.get(
                "malformed_mapping_fields", 0),
            truncated_field_names=counts.get("truncated_field_names", 0),
            truncated_name_collisions=counts.get(
                "truncated_name_collisions", 0),
        ),
    )


class MetadataProvider(Protocol):
    """Yields one cluster metadata snapshot."""

    def fetch(self) -> ClusterSnapshot:
        raise NotImplementedError


#: Largest metadata response this source will read. A mapping for a
#: cluster with thousands of indices is the big one; 32 MiB is far above
#: any legitimate reference cluster and far below "read whatever the
#: other end sends until memory runs out".
MAX_RESPONSE_BYTES = 32 * 1024 * 1024

#: Wall-clock bound across ALL six requests. The per-socket timeout only
#: bounds one quiet period; a server dribbling a byte at a time resets it
#: forever, so the deadline is tracked separately.
DEFAULT_TOTAL_BUDGET_SECONDS = 60.0


def redact_url(url: str) -> str:
    """Drop any userinfo before a URL reaches a message, log or payload.

    `https://user:pass@host:9200/_mapping` carries a credential in the
    part before the `@`. Error text is the one place a credential leaks
    without anyone deciding to write it down.
    """
    try:
        parts = urlsplit(url)
    except ValueError:
        return "<unparsable url>"
    if not parts.hostname:
        return url
    netloc = parts.hostname
    if parts.port:
        netloc = f"{netloc}:{parts.port}"
    if parts.username or parts.password:
        netloc = f"<redacted>@{netloc}"
    return urlunsplit((parts.scheme, netloc, parts.path, "", ""))


@dataclass
class HttpMetadataProvider:
    """Read a live cluster over its REST API.

    `session` is any `requests.Session`-shaped object, so a caller may
    supply one carrying a credential for a real cluster. The reference
    deployment points at a local unauthenticated dev container and passes
    none -- there is no credential in this repository.

    Four bounds, because a metadata reader talks to something it does not
    control: the scheme is http or https only, redirects are never
    followed (a redirect is how a credentialed request gets sent
    somewhere else), the body is read in chunks against a byte cap, and a
    total wall-clock budget spans all six requests.
    """

    base_url: str
    session: Any = None
    timeout: float = DEFAULT_TIMEOUT_SECONDS
    total_budget_seconds: float = DEFAULT_TOTAL_BUDGET_SECONDS
    max_response_bytes: int = MAX_RESPONSE_BYTES
    excluded_prefixes: tuple[str, ...] = SCOPE_EXCLUDED_PREFIXES
    excluded_globs: tuple[str, ...] = DEFAULT_EXCLUDED_GLOBS

    def __post_init__(self) -> None:
        scheme = urlsplit(str(self.base_url)).scheme.lower()
        if scheme not in ("http", "https"):
            raise OpenSearchMetadataError(
                f"base_url scheme {scheme!r} is not http or https",
                data={"scheme": scheme},
                fix_hint="point base_url at an http(s) OpenSearch endpoint",
            )
        self._deadline: Optional[float] = None

    def _session(self) -> Any:
        if self.session is None:
            import requests  # imported lazily: only a live read needs it

            self.session = requests.Session()
        return self.session

    def _remaining(self) -> float:
        if self._deadline is None:
            return self.timeout
        left = self._deadline - time.monotonic()
        if left <= 0:
            raise OpenSearchMetadataError(
                "metadata read exceeded its total time budget of "
                f"{self.total_budget_seconds}s",
                data={"budget_seconds": self.total_budget_seconds},
                fix_hint="raise total_budget_seconds, or check the cluster",
            )
        return min(self.timeout, left)

    def _body(self, response: Any, safe: str) -> Any:
        """Read a bounded body.

        `iter_content` is used when the session offers it so the cap is
        enforced while reading rather than after; a stub session without
        it falls back to `.json()`, and the cap is then advisory.
        """
        iter_content = getattr(response, "iter_content", None)
        if not callable(iter_content):
            return response.json()
        chunks: list[bytes] = []
        total = 0
        for chunk in iter_content(chunk_size=65536):
            if not chunk:
                continue
            total += len(chunk)
            if total > self.max_response_bytes:
                raise OpenSearchMetadataError(
                    f"GET {safe} exceeded {self.max_response_bytes} bytes",
                    data={"url": safe, "cap": self.max_response_bytes},
                    fix_hint="narrow the scope, or raise max_response_bytes",
                )
            chunks.append(chunk)
        return json.loads(b"".join(chunks).decode("utf-8"))

    def _get(self, path: str, *, required: bool = True) -> Any:
        url = f"{self.base_url.rstrip('/')}{path}"
        safe = redact_url(url)
        try:
            response = self._session().get(
                url,
                timeout=self._remaining(),
                allow_redirects=False,
                stream=True,
            )
        except OpenSearchMetadataError:
            raise
        except Exception as exc:  # network layer, any shape
            if required:
                raise OpenSearchMetadataError(
                    f"GET {safe} failed: {type(exc).__name__}",
                    data={"url": safe},
                    fix_hint="check the cluster is up and the URL is right",
                ) from None
            return None
        status = getattr(response, "status_code", 200)
        if status >= 300:
            # 3xx included on purpose: redirects are not followed, so a
            # redirect is an unread response, not a successful one.
            if required:
                raise OpenSearchMetadataError(
                    f"GET {safe} returned HTTP {status}",
                    data={"url": safe, "status": status},
                    fix_hint="check the cluster is up and the URL is right",
                )
            return None
        try:
            return self._body(response, safe)
        except OpenSearchMetadataError:
            raise
        except Exception as exc:
            if required:
                raise OpenSearchMetadataError(
                    f"GET {safe} returned an unreadable body: "
                    f"{type(exc).__name__}",
                    data={"url": safe},
                    fix_hint="check the endpoint returns JSON",
                ) from None
            return None

    def fetch(self) -> ClusterSnapshot:
        self._deadline = time.monotonic() + self.total_budget_seconds
        try:
            return parse_snapshot(
                root=self._get("/"),
                health=self._get("/_cluster/health"),
                cat_indices=self._get(
                    "/_cat/indices?format=json&bytes=b"
                    "&h=index,uuid,docs.count,store.size",
                ),
                mappings=self._get("/_mapping"),
                # Not required: a cluster where the settings call fails is
                # still readable, and the failure is carried honestly as
                # `retention_read: false` rather than as "no policy".
                settings=self._get("/_settings", required=False),
                aliases=self._get("/_alias"),
                excluded_prefixes=self.excluded_prefixes,
                excluded_globs=self.excluded_globs,
            )
        finally:
            self._deadline = None


@dataclass
class RecordedMetadataProvider:
    """Replay response bodies captured from a real cluster.

    The fixture directory holds one JSON file per endpoint, under the
    names below. Both providers feed `parse_snapshot`, so this exercises
    the production parser rather than a test-only path.

    A missing `settings.json` is the recorded form of a failed settings
    call, which is how the `retention_read: false` branch is tested.
    """

    root_dir: Path
    excluded_prefixes: tuple[str, ...] = SCOPE_EXCLUDED_PREFIXES
    excluded_globs: tuple[str, ...] = DEFAULT_EXCLUDED_GLOBS
    overrides: dict[str, Any] = field(default_factory=dict)

    _REQUIRED = ("root.json", "health.json", "cat_indices.json",
                 "mapping.json", "alias.json")

    def _load(self, filename: str, *, required: bool) -> Any:
        key = filename.removesuffix(".json")
        if key in self.overrides:
            return self.overrides[key]
        path = Path(self.root_dir) / filename
        if not path.is_file():
            if required:
                raise OpenSearchMetadataError(
                    f"recorded response {path} is missing",
                    data={"path": str(path)},
                    fix_hint="capture the fixture from the pinned image",
                )
            return None
        return json.loads(path.read_text(encoding="utf-8"))

    def fetch(self) -> ClusterSnapshot:
        return parse_snapshot(
            root=self._load("root.json", required=True),
            health=self._load("health.json", required=True),
            cat_indices=self._load("cat_indices.json", required=True),
            mappings=self._load("mapping.json", required=True),
            settings=self._load("settings.json", required=False),
            aliases=self._load("alias.json", required=True),
            excluded_prefixes=self.excluded_prefixes,
            excluded_globs=self.excluded_globs,
        )
