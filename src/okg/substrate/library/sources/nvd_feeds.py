"""NVD CVE feed source adapter — frozen local JSON feed files.

Profile: `reference_catalog`.

Ingests operator-downloaded NVD CVE dumps only; there is NO live
network path in this adapter (the CTIBench freeze contract: every
arm reads the same dated snapshot, digests recorded up front). Both
published feed shapes parse to the identical node/edge output:

- legacy 1.1 year feeds: `{"CVE_Items": [...]}` (the bulk
  `nvdcve-1.1-<year>.json[.gz]` artifacts);
- 2.0 shape: `{"vulnerabilities": [{"cve": {...}}, ...]}` (saved
  pages of the 2.0 API, the current bulk path).

Any other top-level shape raises — a wrong file must never be
half-ingested and presented as a corpus.

Emits:
- `cve` nodes (cve_id, english description, preferred CVSS
  version/vector/score, published/modified timestamps).
- `cve has_weakness cwe` edges for each `CWE-\\d+` referenced in the
  entry's weakness data. Edge dst ids use the shared CWE node-id
  shape so they land on the nodes the cwe_catalog source emits.
- `weakness_mapping_status` on a CVE that references NO CWE, saying
  WHICH of NVD's three statements applies: `nvd_cwe_noinfo` (NVD has
  no information), `nvd_cwe_other` (an analyst placed the weakness
  outside CWE — a judgment, not an absence), or `not_stated` (the
  entry carries no weakness block at all). OMITTED whenever NVD did
  state a mapping, so absence of the key means "an edge was declared
  here" and an isolated CVE without it lost that edge rather than
  never having had one. An isolation probe can therefore demand a
  STATED reason instead of accepting an aggregate fraction. Stamped
  from the parsed feed, never from whether an edge was emitted, which
  would be circular. A non-CWE value outside the known marker
  vocabulary RAISES rather than minting an unread status.

Per `okg-corpus-cti` (req.cti2.weakness) the preferred CVSS vector is
also DECOMPOSED into its eight base metrics — cti-vsp is graded metric
by metric, so a single opaque vector string is not the answer unit:
`cvss_attack_vector`, `cvss_attack_complexity`,
`cvss_privileges_required`, `cvss_user_interaction`, `cvss_scope`,
`cvss_confidentiality_impact`, `cvss_integrity_impact`,
`cvss_availability_impact`, plus `cvss_base_severity`.

Two properties make the decomposition honest:

  * it happens ONLY for CVSS v3.1 and v3.0, the versions cti-vsp is
    graded against. For a v2-only vector, or a CVE NVD has not scored,
    the nine attributes are ABSENT from the node — not null-valued, not
    defaulted. Absent is what `attrs ? 'cvss_attack_vector'` can
    actually distinguish, so a v2-only CVE cannot answer a v3 probe by
    carrying an empty key;
  * it is proven lossless per CVE: the vector reassembled from the
    stored metric values MUST equal the released `cvss_vector` string,
    and a mismatch raises with the CVE id named.

Per `okg-bench-ctibench` (2026-07-27) and `okg-corpus-cti` (2026-07-28).
"""
from __future__ import annotations

import glob
import hashlib
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterator, Optional

from okg.substrate.errors import SubstrateError
from okg.substrate.library.sources._cti_snapshot import (
    cwe_node_id,
    load_json,
    normalize_timestamp,
    read_snapshot_bytes,
    record_set_from_facts,
)
from okg.substrate.library.sources.base import (
    EdgeFact,
    NodeFact,
    SourceRun,
)


__all__ = ["NvdFeedsSource"]


_CWE_REF_RE = re.compile(r"CWE-\d+")

#: NVD's no-mapping vocabulary -> the `weakness_mapping_status` each one
#: means. Kept as separate values because they are separate claims:
#: `noinfo` is "we have no information", `Other` is "an analyst placed
#: this weakness outside CWE". Any OTHER non-CWE value raises — see
#: `_weakness_mapping`.
_NO_MAPPING_MARKERS: dict[str, str] = {
    "NVD-CWE-noinfo": "nvd_cwe_noinfo",
    "NVD-CWE-Other": "nvd_cwe_other",
}

#: The CVSS versions cti-vsp is graded against, and the ONLY ones the
#: vector is decomposed for.
_DECOMPOSED_CVSS_VERSIONS = ("3.1", "3.0")

#: The eight base metrics, in the canonical vector order, as
#: (vector abbreviation, attribute name, {code: released value}). The
#: value vocabulary is CVSS v3.1 section 2 / NVD's own JSON encoding
#: (`ADJACENT_NETWORK` for AV:A), so a stored value reads the way NVD
#: publishes it and still round-trips to the abbreviation.
_CVSS_BASE_METRICS: tuple[tuple[str, str, dict[str, str]], ...] = (
    ("AV", "cvss_attack_vector", {
        "N": "NETWORK", "A": "ADJACENT_NETWORK", "L": "LOCAL",
        "P": "PHYSICAL",
    }),
    ("AC", "cvss_attack_complexity", {"L": "LOW", "H": "HIGH"}),
    ("PR", "cvss_privileges_required", {
        "N": "NONE", "L": "LOW", "H": "HIGH",
    }),
    ("UI", "cvss_user_interaction", {"N": "NONE", "R": "REQUIRED"}),
    ("S", "cvss_scope", {"U": "UNCHANGED", "C": "CHANGED"}),
    ("C", "cvss_confidentiality_impact", {
        "H": "HIGH", "L": "LOW", "N": "NONE",
    }),
    ("I", "cvss_integrity_impact", {"H": "HIGH", "L": "LOW", "N": "NONE"}),
    ("A", "cvss_availability_impact", {"H": "HIGH", "L": "LOW", "N": "NONE"}),
)

#: (version, vector, base score, base severity) of the preferred metric.
#: Every element is None when NVD has not scored the CVE.
_Cvss = tuple[Optional[str], Optional[str], Optional[float], Optional[str]]


@dataclass
class NvdFeedsSource:
    """NVD CVE feed adapter over frozen local snapshot files.

    `snapshot_glob` matches the downloaded feed files (`*.json` and
    `*.json.gz`), e.g. `/snapshots/nvd/nvdcve-*.json.gz`. There is no
    live mode: the operator downloads the feeds once, freezes them,
    and every run re-reads the same bytes.
    """

    name: str = "nvd_feeds"
    profile: str = "reference_catalog"
    snapshot_glob: Optional[str] = None

    #: Counts from the most recent run: CVEs, has_weakness edges, and
    #: the CVSS decomposition census (how many CVEs got the eight
    #: metrics, how many are v2-only, how many are unscored). A corpus
    #: with no queryable metrics therefore says why.
    last_run_stats: dict[str, Any] = field(default_factory=dict, init=False)

    # Incremental-source contract (issue #323): frozen local feed files
    # are plain bytes on disk, so the probe is the content-hash floor
    # over the CURRENT glob expansion — an edit, an added feed file, or
    # a deleted feed file all bust the token.
    change_probe_kind = "content_hash"

    def __post_init__(self) -> None:
        self.change_probe = self._build_change_probe()
        # The run consumes exactly the bytes the probe hashes (complete
        # immutable local files), so a matching token proves the emitted
        # facts would be identical — short-circuit is safe.
        self.probe_short_circuit_safe = True

    def _build_change_probe(self) -> Any:
        import sys

        from okg.substrate.library.sources.content_hash_probe import (
            ContentHashProbe,
        )
        from okg.substrate.library.sources.emission_identity import (
            emission_identity_targets,
        )

        def _items() -> list[tuple[str, Path]]:
            return [(str(p), p) for p in self._matched_files()]

        return ContentHashProbe(
            content_items=_items,
            config={"snapshot_glob": self.snapshot_glob},
            emit_targets=emission_identity_targets(
                self.name,
                sys.modules[__name__],
            ),
        )

    def _matched_files(self) -> list[Path]:
        if self.snapshot_glob is None:
            return []
        return sorted(
            Path(match)
            for match in glob.glob(self.snapshot_glob, recursive=True)
            if Path(match).is_file()
        )

    def run(
        self,
        run_id: str,
        *,
        mode: str = "scope_complete",
        cursor: Optional[dict[str, Any]] = None,
    ) -> SourceRun:
        if self.snapshot_glob is None:
            raise SubstrateError(
                f"source {self.name!r}: pass snapshot_glob",
                fix_hint=(
                    "snapshot_glob='/path/to/nvd/*.json*' matching the "
                    "frozen feed files"
                ),
            )
        files = self._matched_files()
        if not files:
            raise SubstrateError(
                f"source {self.name!r}: snapshot_glob "
                f"{self.snapshot_glob!r} matched no files",
                data={"adapter": self.name, "snapshot_glob": self.snapshot_glob},
                fix_hint="download the NVD feed files into the snapshot dir",
            )

        item_hashes: dict[str, str] = {}
        facts: list[NodeFact | EdgeFact] = []
        cve_count = 0
        self.last_run_stats = {}
        for path in files:
            raw = read_snapshot_bytes(path, adapter=self.name)
            item_hashes[str(path)] = hashlib.sha256(raw).hexdigest()
            doc = load_json(raw, path=path, adapter=self.name)
            for fact in self._normalize_feed(doc, path=path):
                if isinstance(fact, NodeFact):
                    cve_count += 1
                facts.append(fact)
        # An all-empty snapshot is an operator error (wrong glob, empty
        # download), not an empty-but-healthy corpus: refuse to publish
        # a CVE corpus with zero CVEs.
        if cve_count == 0:
            raise SubstrateError(
                f"source {self.name!r}: {len(files)} matched file(s) "
                f"contained zero CVE entries",
                data={
                    "adapter": self.name,
                    "snapshot_glob": self.snapshot_glob,
                    "files": [str(p) for p in files],
                },
                fix_hint="check that the feed files are complete downloads",
            )

        completed_scope = mode in {"reconcile", "scope_complete", "release_new"}
        record_set = (
            record_set_from_facts(facts) if completed_scope else None
        )
        applied_raw_token = self.change_probe.token_from_item_hashes(
            item_hashes,
        )
        self.last_run_stats.update({
            "files_scanned": len(files),
            "cves_emitted": cve_count,
            "has_weakness_edges": sum(
                1 for fact in facts if isinstance(fact, EdgeFact)
            ),
        })

        def _stream() -> Iterator[NodeFact | EdgeFact]:
            yield from facts

        return SourceRun(
            facts=_stream(),
            completed_scope=completed_scope,
            applied_raw_token=applied_raw_token,
            applied_token_scope="global",
            applied_token_authority="exact",
            run_mode=mode,
            record_set=record_set,
        )

    # ---------- feed-shape dispatch ----------

    def _normalize_feed(
        self, doc: Any, *, path: Path,
    ) -> Iterator[NodeFact | EdgeFact]:
        """Dispatch on the feed's top-level shape; refuse anything else.

        Both shapes normalize into the same `(cve_id, description,
        cvss, timestamps, cwe_refs)` tuple, so downstream emission is
        shared and the two shapes provably produce identical facts.
        """
        if isinstance(doc, dict) and isinstance(doc.get("CVE_Items"), list):
            for item in doc["CVE_Items"]:
                yield from self._emit(
                    *self._parse_item_11(item, path=path), path=path,
                )
            return
        if isinstance(doc, dict) and isinstance(
            doc.get("vulnerabilities"), list
        ):
            for wrapper in doc["vulnerabilities"]:
                yield from self._emit(
                    *self._parse_item_20(wrapper, path=path), path=path,
                )
            return
        raise SubstrateError(
            f"source {self.name!r}: {path} is neither an NVD 1.1 feed "
            f"({{'CVE_Items': [...]}}) nor a 2.0 feed "
            f"({{'vulnerabilities': [...]}})",
            data={"adapter": self.name, "path": str(path)},
            fix_hint="point snapshot_glob at NVD CVE feed downloads only",
        )

    # ---------- per-shape parsing ----------

    def _parse_item_11(
        self, item: Any, *, path: Path,
    ) -> tuple[str, str, _Cvss, str, str, list[str], Optional[str]]:
        if not isinstance(item, dict) or not isinstance(item.get("cve"), dict):
            raise self._malformed(path, "1.1 CVE_Items entry missing 'cve'")
        cve = item["cve"]
        cve_id = ((cve.get("CVE_data_meta") or {}).get("ID"))
        if not isinstance(cve_id, str) or not cve_id:
            raise self._malformed(path, "1.1 entry missing CVE_data_meta.ID")
        description = self._english_description(
            (cve.get("description") or {}).get("description_data"),
            cve_id=cve_id, path=path,
        )
        cvss = self._select_cvss_11(item.get("impact") or {}, cve_id=cve_id, path=path)
        published = normalize_timestamp(
            item.get("publishedDate"),
            field="publishedDate", context=cve_id, adapter=self.name,
        )
        modified = normalize_timestamp(
            item.get("lastModifiedDate"),
            field="lastModifiedDate", context=cve_id, adapter=self.name,
        )
        cwe_refs, mapping_status = self._weakness_mapping(
            desc_lists=[
                entry.get("description")
                for entry in (
                    (cve.get("problemtype") or {}).get("problemtype_data")
                    or []
                )
                if isinstance(entry, dict)
            ],
            cve_id=cve_id,
            path=path,
        )
        return (
            cve_id, description, cvss, published, modified, cwe_refs,
            mapping_status,
        )

    def _parse_item_20(
        self, wrapper: Any, *, path: Path,
    ) -> tuple[str, str, _Cvss, str, str, list[str], Optional[str]]:
        if not isinstance(wrapper, dict) or not isinstance(
            wrapper.get("cve"), dict
        ):
            raise self._malformed(
                path, "2.0 vulnerabilities entry missing 'cve'",
            )
        cve = wrapper["cve"]
        cve_id = cve.get("id")
        if not isinstance(cve_id, str) or not cve_id:
            raise self._malformed(path, "2.0 entry missing cve.id")
        description = self._english_description(
            cve.get("descriptions"), cve_id=cve_id, path=path,
        )
        cvss = self._select_cvss_20(
            cve.get("metrics") or {}, cve_id=cve_id, path=path,
        )
        published = normalize_timestamp(
            cve.get("published"),
            field="published", context=cve_id, adapter=self.name,
        )
        modified = normalize_timestamp(
            cve.get("lastModified"),
            field="lastModified", context=cve_id, adapter=self.name,
        )
        cwe_refs, mapping_status = self._weakness_mapping(
            desc_lists=[
                weakness.get("description")
                for weakness in (cve.get("weaknesses") or [])
                if isinstance(weakness, dict)
            ],
            cve_id=cve_id,
            path=path,
        )
        return (
            cve_id, description, cvss, published, modified, cwe_refs,
            mapping_status,
        )

    # ---------- shared field extraction ----------

    def _english_description(
        self, entries: Any, *, cve_id: str, path: Path,
    ) -> str:
        """First english description text; raise when there is none.

        Every published CVE carries an english description (rejected
        CVEs carry the `** REJECT **` text), so absence means the entry
        is truncated/malformed — raise rather than emit a fact with no
        content.
        """
        for entry in entries or []:
            if (
                isinstance(entry, dict)
                and entry.get("lang") == "en"
                and isinstance(entry.get("value"), str)
            ):
                return entry["value"]
        raise self._malformed(
            path, f"{cve_id} has no english description",
        )

    def _select_cvss_11(
        self, impact: Any, *, cve_id: str, path: Path,
    ) -> _Cvss:
        """Preferred CVSS from the 1.1 `impact` block (v3.x, then v2).

        The 1.1 feed carries at most one v3 metric (`baseMetricV3`,
        version 3.1 or 3.0 inside), so preferring v3.1 over v3.0
        reduces to preferring the v3 block over the v2 block.
        """
        if not isinstance(impact, dict):
            raise self._malformed(path, f"{cve_id} impact is not an object")
        for key, inner in (("baseMetricV3", "cvssV3"), ("baseMetricV2", "cvssV2")):
            block = impact.get(key)
            if not isinstance(block, dict):
                continue
            return self._cvss_from_data(
                block.get(inner), cve_id=cve_id, path=path,
            )
        # No CVSS at all is a legitimate NVD state (e.g. "Awaiting
        # Analysis" / rejected CVEs), not a malformed entry: emit an
        # honest absence, never a default score.
        return None, None, None, None

    def _select_cvss_20(
        self, metrics: Any, *, cve_id: str, path: Path,
    ) -> _Cvss:
        """Preferred CVSS from the 2.0 `metrics` block: v3.1 > v3.0 > v2."""
        if not isinstance(metrics, dict):
            raise self._malformed(path, f"{cve_id} metrics is not an object")
        for key in ("cvssMetricV31", "cvssMetricV30", "cvssMetricV2"):
            entries = metrics.get(key)
            if not isinstance(entries, list) or not entries:
                continue
            first = entries[0]
            if not isinstance(first, dict):
                raise self._malformed(
                    path, f"{cve_id} {key} entry is not an object",
                )
            return self._cvss_from_data(
                first.get("cvssData"), cve_id=cve_id, path=path,
            )
        return None, None, None, None

    def _cvss_from_data(
        self, data: Any, *, cve_id: str, path: Path,
    ) -> _Cvss:
        """Extract (version, vector, score, severity); a
        present-but-incomplete CVSS block is malformed and raises."""
        if not isinstance(data, dict):
            raise self._malformed(
                path, f"{cve_id} CVSS block missing cvss data object",
            )
        version = data.get("version")
        vector = data.get("vectorString")
        score = data.get("baseScore")
        if (
            not isinstance(version, str)
            or not isinstance(vector, str)
            or not isinstance(score, (int, float))
            or isinstance(score, bool)
        ):
            raise self._malformed(
                path,
                f"{cve_id} CVSS block missing version/vectorString/baseScore",
            )
        # baseSeverity is a released field, not a derivable one: when
        # the block omits it the attribute stays absent rather than
        # being inferred from the score.
        severity = data.get("baseSeverity")
        return (
            version,
            vector,
            float(score),
            severity if isinstance(severity, str) and severity else None,
        )

    def _decompose_cvss(
        self,
        version: Optional[str],
        vector: Optional[str],
        *,
        cve_id: str,
        path: Path,
    ) -> Optional[dict[str, str]]:
        """The eight base metrics of a v3.x vector, or None.

        None means "this CVE has no v3 vector" — the caller then emits
        NO metric attributes at all, because an absent key is the only
        encoding a `attrs ? 'cvss_attack_vector'` probe can tell apart
        from a real answer. The parse is strict: prefix, metric order,
        metric set and value vocabulary must all be the released v3
        shape, and the reassembled string must equal the released
        vector or the ingest fails with the CVE named.
        """
        if version not in _DECOMPOSED_CVSS_VERSIONS or vector is None:
            return None
        parts = vector.split("/")
        expected_prefix = f"CVSS:{version}"
        if len(parts) != len(_CVSS_BASE_METRICS) + 1:
            raise self._malformed(
                path,
                f"{cve_id} CVSS v{version} vector {vector!r} does not carry "
                f"exactly the {len(_CVSS_BASE_METRICS)} base metrics",
            )
        if parts[0] != expected_prefix:
            raise self._malformed(
                path,
                f"{cve_id} CVSS vector {vector!r} is not prefixed "
                f"{expected_prefix!r}",
            )
        decomposed: dict[str, str] = {}
        for (abbrev, attr, values), component in zip(
            _CVSS_BASE_METRICS, parts[1:],
        ):
            key, _, code = component.partition(":")
            if key != abbrev or code not in values:
                raise self._malformed(
                    path,
                    f"{cve_id} CVSS vector {vector!r} component "
                    f"{component!r} is not the expected {abbrev} metric",
                )
            decomposed[attr] = values[code]

        # Lossless or nothing: reassemble FROM THE STORED VALUES.
        reassembled = "/".join(
            [expected_prefix]
            + [
                f"{abbrev}:{self._metric_code(values, decomposed[attr])}"
                for abbrev, attr, values in _CVSS_BASE_METRICS
            ]
        )
        if reassembled != vector:
            raise SubstrateError(
                f"source {self.name!r}: {cve_id} CVSS decomposition is "
                f"lossy: released {vector!r} != reassembled "
                f"{reassembled!r}",
                data={
                    "adapter": self.name,
                    "path": str(path),
                    "cve": cve_id,
                    "released_vector": vector,
                    "reassembled_vector": reassembled,
                },
                fix_hint="the feed carries a non-canonical CVSS vector; "
                         "re-download the feed file",
            )
        return decomposed

    @staticmethod
    def _metric_code(values: dict[str, str], value: str) -> str:
        for code, released in values.items():
            if released == value:
                return code
        # Unreachable: `value` was produced from `values` above. Kept
        # explicit so the round-trip can never fall back to the raw
        # string and quietly "pass".
        raise KeyError(value)

    def _weakness_mapping(
        self, *, desc_lists: list[Any], cve_id: str, path: Path,
    ) -> tuple[list[str], Optional[str]]:
        """`(cwe_refs, weakness_mapping_status)` for one CVE.

        `cwe_refs` are the ordered, de-duplicated `CWE-\\d+` ids the feed
        cites. The status is the reason NVD gives for citing NONE, and
        is `None` whenever at least one is cited — so absence of the
        attribute means "NVD stated a mapping", and an isolated CVE with
        no status lost its edge after the feed declared it.

        The three upstream statements stay DISTINCT because they are not
        the same claim: `NVD-CWE-noinfo` is an absence of information,
        `NVD-CWE-Other` is an analyst judgment that the weakness is real
        but outside CWE, and an entry with no weakness block at all is
        NVD saying nothing either way. A consumer may treat them
        differently, so collapsing them here would destroy the
        distinction before anyone can.

        A non-CWE value outside the known marker vocabulary RAISES: the
        stamp is only meaningful if it always names a KNOWN upstream
        statement, and minting one for an unrecognized marker would
        forgive an isolated CVE for a reason nobody has read.
        """
        seen: dict[str, None] = {}
        markers: set[str] = set()
        for entries in desc_lists:
            for entry in entries or []:
                if not isinstance(entry, dict):
                    continue
                value = entry.get("value")
                if not isinstance(value, str) or not value:
                    continue
                # NVD-CWE-noinfo contains no CWE-\d+ substring; a plain
                # fullmatch-style value like "CWE-79" does.
                refs = _CWE_REF_RE.findall(value)
                if refs:
                    for ref in refs:
                        seen.setdefault(ref)
                    continue
                if value not in _NO_MAPPING_MARKERS:
                    raise SubstrateError(
                        f"source {self.name!r}: {cve_id} in {path} cites "
                        f"weakness value {value!r}, which is neither a "
                        f"CWE id nor a known NVD no-mapping marker "
                        f"({', '.join(sorted(_NO_MAPPING_MARKERS))})",
                        data={
                            "adapter": self.name,
                            "path": str(path),
                            "cve": cve_id,
                            "value": value,
                            "known_markers": sorted(_NO_MAPPING_MARKERS),
                        },
                        fix_hint="add the marker to _NO_MAPPING_MARKERS "
                                 "with the weakness_mapping_status it "
                                 "means, rather than letting an unread "
                                 "statement pass as no mapping",
                    )
                markers.add(value)
        if seen:
            # A mapping exists; any marker alongside it (14 CVEs in the
            # 2026-07-27 recent feed cite NVD-CWE-noinfo next to a real
            # id) does not make the CVE unmapped.
            if markers:
                self._count("cves_with_both_cwe_ref_and_marker")
            return list(seen), None
        if not markers:
            return [], "not_stated"
        if len(markers) > 1:
            # Never observed; counted rather than silently resolved.
            self._count("cves_with_multiple_no_mapping_markers")
        # `Other` is the stronger statement — an analyst looked and
        # placed the weakness outside CWE — so it wins a tie.
        for marker in ("NVD-CWE-Other", "NVD-CWE-noinfo"):
            if marker in markers:
                return [], _NO_MAPPING_MARKERS[marker]
        raise AssertionError("unreachable: markers checked above")

    def _count(self, key: str, delta: int = 1) -> None:
        self.last_run_stats[key] = self.last_run_stats.get(key, 0) + delta

    def _malformed(self, path: Path, detail: str) -> SubstrateError:
        return SubstrateError(
            f"source {self.name!r}: malformed feed entry in {path}: "
            f"{detail}",
            data={"adapter": self.name, "path": str(path), "detail": detail},
            fix_hint="re-download the feed file; NVD entries are never "
                     "shaped like this",
        )

    # ---------- emission ----------

    def _emit(
        self,
        cve_id: str,
        description: str,
        cvss: _Cvss,
        published_at: str,
        modified_at: str,
        cwe_refs: list[str],
        mapping_status: Optional[str],
        *,
        path: Path,
    ) -> Iterator[NodeFact | EdgeFact]:
        cvss_version, cvss_vector, cvss_base_score, cvss_base_severity = cvss
        node_id = f"cve:nvd:{cve_id}"
        source_record_id = {"cve": cve_id}
        source_revision = {"catalog": "nvd", "modified_at": modified_at}
        attrs: dict[str, Any] = {
            "cve_id": cve_id,
            "description": description,
            "cvss_version": cvss_version,
            "cvss_vector": cvss_vector,
            "cvss_base_score": cvss_base_score,
            "published_at": published_at,
            "modified_at": modified_at,
            # created_at is the semantic creation time of the fact:
            # the CVE's publication instant.
            "created_at": published_at,
        }
        if mapping_status is not None:
            # WHY this CVE cites no weakness, as NVD states it. Stamped
            # from the parsed feed, never from whether an edge came out:
            # deriving it from emission would forgive precisely the
            # regression it exists to expose. OMITTED when NVD stated a
            # mapping, so an isolated CVE with no status is a lost edge.
            attrs["weakness_mapping_status"] = mapping_status
            self._count(f"cves_{mapping_status}")
        decomposed = self._decompose_cvss(
            cvss_version, cvss_vector, cve_id=cve_id, path=path,
        )
        if decomposed is None:
            # v2-only or unscored: the nine v3 attributes are ABSENT.
            # Not None-valued — a null key would answer `attrs ?
            # 'cvss_attack_vector'` with "yes" and carry no metric.
            self._count(
                "cves_without_v3_metrics" if cvss_version
                else "cves_unscored",
            )
        else:
            attrs.update(decomposed)
            if cvss_base_severity is not None:
                attrs["cvss_base_severity"] = cvss_base_severity
            else:
                self._count("cves_v3_without_base_severity")
            self._count(f"cves_decomposed_v{cvss_version}")
        yield NodeFact(
            node_id=node_id,
            subtype="cve",
            attrs=attrs,
            source_record_id=source_record_id,
            source_revision=source_revision,
            op="I",
        )
        for cwe_ref in cwe_refs:
            yield EdgeFact(
                src=node_id,
                dst=cwe_node_id(cwe_ref),
                edge_type="has_weakness",
                attrs={},
                source_record_id={"cve": cve_id, "cwe": cwe_ref},
                source_revision=source_revision,
                op="I",
            )
