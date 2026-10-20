"""Deterministic synthetic facts for the okg-workspace scale graph.

Every record and relationship is a pure function of the scaled plan, the
deployment's source registry, and an index, so two builds at the same scale
write the same graph and a re-write (version history) changes only the
attributes it means to change.

The registry is the authority for shape: a record type the targets give a
source must be one that source declares in its ``output_signature``, with the
declared id prefix and fixed attributes, and a relationship must be one of
its declared triples. A target the registry does not declare is refused, so
the generator cannot write a fact the real admission path would reject.
"""
from __future__ import annotations

import hashlib
import math
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import yaml

from okg.bench.workspace_scale.targets import EdgeTarget, ScaledPlan
from okg.substrate.library.sources.base import EdgeFact, NodeFact

_EPOCH = datetime(2025, 10, 1, tzinfo=timezone.utc)
_FILLER = (
    "synthetic okg-workspace record generated for console performance "
    "measurement; the words carry no meaning beyond their length and shape "
)


class RegistryMismatch(ValueError):
    """A target names output the source registry does not declare."""


@dataclass(frozen=True)
class NodeSpec:
    source: str
    subtype: str
    count: int
    id_prefix: str
    fixed_attrs: Mapping[str, Any]


@dataclass(frozen=True)
class EdgeSpec:
    target: EdgeTarget
    src_prefix: str | None
    dst_prefix: str | None


def load_registry_signatures(deployment_dir: Path) -> dict[str, dict[str, Any]]:
    """Map each source's ownership id to its declared admission policy."""
    raw = yaml.safe_load(
        (deployment_dir / "source_registry.yaml").read_text(encoding="utf-8")
    )
    out: dict[str, dict[str, Any]] = {}
    for name, entry in (raw.get("sources") or {}).items():
        ownership = str(entry.get("ownership_id") or name)
        out[ownership] = {
            "registry_name": str(name),
            "admission_policy": dict(entry.get("admission_policy") or {}),
            "predecessors": entry.get("predecessor_source"),
        }
    return out


def _signature(registry: Mapping[str, Any], source: str) -> Mapping[str, Any]:
    if source not in registry:
        raise RegistryMismatch(
            f"targets name source {source!r}, which the okg-workspace source "
            "registry does not declare"
        )
    policy = registry[source]["admission_policy"]
    signature = policy.get("output_signature")
    if not isinstance(signature, Mapping):
        raise RegistryMismatch(f"source {source!r} declares no output_signature")
    return signature


def _permutation_prime(total: int) -> int:
    for prime in (7919, 7927, 7933, 7937, 7949, 7951, 104729):
        if total and math.gcd(prime, total) == 1:
            return prime
    return 1


class WorkspaceFacts:
    """The scaled plan resolved against the registry, ready to emit facts."""

    def __init__(
        self,
        plan: ScaledPlan,
        *,
        deployment_dir: Path,
        search_term: str,
        text_chars: int,
        enricher_plants: Mapping[str, int],
    ) -> None:
        self.plan = plan
        self.search_term = search_term
        self.text_chars = text_chars
        self.registry = load_registry_signatures(deployment_dir)
        self.node_specs: dict[tuple[str, str], NodeSpec] = {}
        for source, by_subtype in plan.nodes.items():
            signature = _signature(self.registry, source)
            declared = {
                str(n["subtype"]): n for n in (signature.get("nodes") or [])
            }
            for subtype, count in by_subtype.items():
                if subtype not in declared:
                    raise RegistryMismatch(
                        f"targets give {source} {count} {subtype} records, but "
                        f"its output_signature declares only {sorted(declared)}"
                    )
                node = declared[subtype]
                prefix = node.get("id_prefix") or f"{subtype}:"
                self.node_specs[(source, subtype)] = NodeSpec(
                    source=source,
                    subtype=subtype,
                    count=count,
                    id_prefix=str(prefix),
                    fixed_attrs=dict(node.get("attrs") or {}),
                )
        self.edge_specs: list[EdgeSpec] = []
        for target in plan.edges:
            signature = _signature(self.registry, target.source)
            match = [
                e
                for e in (signature.get("edges") or [])
                if (e.get("src_subtype"), e.get("edge_type"), e.get("dst_subtype"))
                == target.triple
            ]
            if not match:
                raise RegistryMismatch(
                    f"targets give {target.source} {target.count} {target.triple} "
                    "relationships, which its output_signature does not declare"
                )
            self.edge_specs.append(
                EdgeSpec(
                    target=target,
                    src_prefix=match[0].get("src_id_prefix"),
                    dst_prefix=match[0].get("dst_id_prefix"),
                )
            )
        # Global record order: the index every attribute choice keys on.
        self._order: list[tuple[str, str]] = sorted(self.node_specs)
        self._offsets: dict[tuple[str, str], int] = {}
        offset = 0
        for key in self._order:
            self._offsets[key] = offset
            offset += self.node_specs[key].count
        self.total_records = offset
        self._prime = _permutation_prime(self.total_records)
        self._ids: dict[tuple[str, str], list[str]] = {
            key: [self._make_id(spec, i) for i in range(spec.count)]
            for key, spec in self.node_specs.items()
        }
        self._pairs: dict[int, list[tuple[str, str]]] = {}
        # Below scale 1.0 a rounded-down record count can leave fewer
        # distinct pairs than the rounded relationship count (80 sections
        # become 1, which cannot contain another section). Lower those counts
        # to what exists and SAY so in `clamped`; at full scale a shortfall
        # is a targets error and is refused.
        self.clamped: list[dict[str, Any]] = []
        for index, spec in enumerate(self.edge_specs):
            capacity = self.edge_capacity(spec)
            if spec.target.count <= capacity:
                continue
            if plan.scale >= 1.0:
                raise RegistryMismatch(
                    f"{spec.target.source} {spec.target.triple}: "
                    f"{spec.target.count} relationships requested but only "
                    f"{capacity} distinct pairs exist"
                )
            self.clamped.append({
                "source": spec.target.source,
                "triple": list(spec.target.triple),
                "requested": spec.target.count,
                "written": capacity,
            })
            t = spec.target
            self.edge_specs[index] = EdgeSpec(
                target=EdgeTarget(t.source, t.src_subtype, t.edge_type, t.dst_subtype, capacity),
                src_prefix=spec.src_prefix,
                dst_prefix=spec.dst_prefix,
            )
        self.plants = dict(enricher_plants)
        self._plant_index = self._resolve_plants()

    # ------------------------------------------------------------------ ids
    @staticmethod
    def _make_id(spec: NodeSpec, i: int) -> str:
        if spec.subtype == "git_commit":
            digest = hashlib.sha1(f"{spec.source}:{i}".encode()).hexdigest()
            return f"{spec.id_prefix}{digest}"
        if spec.subtype == "source_file":
            short = spec.source.rsplit(".", 1)[-1]
            return f"{spec.id_prefix}{short}/pkg{i // 40:04d}/module_{i:06d}.py"
        # Two sources can declare one prefix (git_branch under `branch:okg:`
        # for both git sources): the source name keeps their records apart,
        # so each record has the one owner the census counted it under.
        sep = "" if spec.id_prefix.endswith((":", "-", "/")) else "-"
        short = spec.source.rsplit(".", 1)[-1]
        return f"{spec.id_prefix}{sep}{short}-{spec.subtype}-{i:06d}"

    def ids(self, source: str, subtype: str) -> list[str]:
        return self._ids[(source, subtype)]

    # --------------------------------------------------------------- plants
    def _resolve_plants(self) -> dict[str, Any]:
        """Choose which records carry the attributes the enrichers match."""
        plants: dict[str, Any] = {}

        def owned(subtype: str, source: str) -> list[str]:
            return self._ids.get((source, subtype), [])

        changes = owned("open_spec_change", "okg-workspace.openspec")
        papers = owned("paper", "okg-workspace.bib_references")
        citations = owned("citation", "okg-workspace.latex_project")
        commits = owned("git_commit", "okg-workspace.git-history")
        forge = owned("issue", "okg-workspace.github-issues") + owned(
            "pull_request", "okg-workspace.github-issues"
        )
        criteria = owned("acceptance_criterion", "okg-workspace.openspec")
        tests = owned("test_case", "okg-workspace.test-reports")
        findings = owned("security_finding", "okg-workspace.security-analysis") + owned(
            "secret_finding", "okg-workspace.security-analysis"
        )
        files = owned("source_file", "okg-workspace.git-files")

        cites = self.plants.get("_enricher:declarative:citation_cites_paper_by_paper_id", 0)
        plants["citation_paper"] = {
            citations[i]: f"paperkey-{i % max(1, len(papers)):06d}"
            for i in range(min(cites, len(citations)))
            if papers
        }
        # The commit and forge rules are bidirectional: one match writes a
        # relationship each way, so half as many records carry the plant.
        commit_refs = self.plants.get(
            "_enricher:declarative:commit_message_subject_starts_with_change_slug", 0
        )
        plants["commit_change"] = {
            commits[i]: self.change_id(i % len(changes))
            for i in range(min(max(1, commit_refs // 2), len(commits)))
            if changes and commit_refs
        }
        forge_refs = self.plants.get(
            "_enricher:declarative:forge_item_word_boundary_mentions_change_slug", 0
        )
        plants["forge_change"] = {
            forge[i]: self.change_id(i % len(changes))
            for i in range(min(max(1, forge_refs // 2), len(forge)))
            if changes and forge_refs
        }
        verified = self.plants.get("_enricher:acceptance_criteria", 0)
        plants["test_criterion"] = {
            tests[i]: criteria[i % len(criteria)]
            for i in range(min(verified, len(tests)))
            if criteria
        }
        affects = self.plants.get("_enricher:security_finding", 0)
        plants["finding_path"] = {
            findings[i]: self.path_of(files[i % len(files)])
            for i in range(min(affects, len(findings)))
            if files
        }
        return plants

    @staticmethod
    def change_id(i: int) -> str:
        return f"synthetic-change-{i:05d}"

    @staticmethod
    def path_of(file_id: str) -> str:
        return file_id.split(":", 2)[-1]

    # ---------------------------------------------------------------- nodes
    def carries_search_term(self, global_index: int) -> bool:
        return (global_index * self._prime) % self.total_records < self.plan.search_records

    def _text(self, title: str, global_index: int, revision: int) -> str:
        head = f"{title}. "
        if self.carries_search_term(global_index):
            head += f"Touches the {self.search_term} path. "
        if revision:
            head += f"Revision {revision}. "
        body = (_FILLER * (1 + self.text_chars // len(_FILLER)))
        return (head + body)[: max(self.text_chars, len(head))]

    def node_attrs(
        self, source: str, subtype: str, i: int, *, revision: int = 0
    ) -> dict[str, Any]:
        spec = self.node_specs[(source, subtype)]
        node_id = self._ids[(source, subtype)][i]
        g = self._offsets[(source, subtype)] + i
        stamp = (_EPOCH + timedelta(minutes=17 * g + revision)).isoformat()
        title = f"{subtype.replace('_', ' ').capitalize()} {i}"
        attrs: dict[str, Any] = {
            **spec.fixed_attrs,
            "title": title,
            "status": ("done", "active", "blocked", "draft")[g % 4],
            "created_at": stamp,
            "updated_at": stamp,
            "last_modified_at": stamp,
            "revision": revision,
        }
        if subtype == "git_commit":
            subject = f"Synthetic commit {i}"
            planted = self._plant_index["commit_change"].get(node_id)
            if planted:
                subject = f"{planted}: synthetic commit {i}"
            attrs.update(
                sha=node_id.rsplit(":", 1)[-1],
                message_subject=subject,
                message=subject,
                committed_at=stamp,
                author_date=stamp,
            )
        elif subtype == "source_file":
            attrs.update(path=self.path_of(node_id), language="python")
        elif subtype in {"open_spec_change", "pact_change"}:
            attrs.update(change_id=self.change_id(i) if subtype == "open_spec_change"
                         else f"pact-synthetic-{i:05d}")
            if subtype == "pact_change":
                attrs.update(
                    child_change_ids=[],
                    next_task_ids=[],
                    evidence_gaps=[],
                    blocked_task_count=g % 3,
                )
        elif subtype == "design_decision":
            attrs.update(
                decision_number=i + 1,
                rationale="Synthetic rationale for a design decision.",
                change_id=self.change_id(i % 50),
            )
        elif subtype == "paper":
            attrs.update(paper_id=f"paperkey-{i:06d}" if source.endswith("bib_references")
                         else f"latexpaper-{i:06d}")
        elif subtype == "citation":
            ref = self._plant_index["citation_paper"].get(node_id)
            attrs.update(cited_paper_ref=ref or f"unresolved-ref-{i:06d}")
        elif subtype in {"issue", "pull_request"}:
            planted = self._plant_index["forge_change"].get(node_id)
            if planted:
                attrs["title"] = f"Follow up on {planted} rollout"
            attrs.update(body="Synthetic forge item body.", number=i + 1)
        elif subtype == "test_case":
            planted = self._plant_index["test_criterion"].get(node_id)
            name = f"test_synthetic_{i}"
            if planted:
                name = f"test_{i} covers {planted}"
            attrs.update(name=name, classname="tests.synthetic", message=name)
        elif subtype in {"security_finding", "secret_finding"}:
            path = self._plant_index["finding_path"].get(node_id)
            if path:
                attrs.update(path=path, start_line=1)
        attrs["text"] = self._text(title, g, revision)
        return attrs

    def node_fact(self, source: str, subtype: str, i: int, *, revision: int = 0) -> NodeFact:
        node_id = self._ids[(source, subtype)][i]
        return NodeFact(
            node_id=node_id,
            subtype=subtype,
            attrs=self.node_attrs(source, subtype, i, revision=revision),
            source_record_id={"id": node_id},
            source_revision={"revision": revision},
        )

    def iter_node_facts(self, source: str) -> Iterator[NodeFact]:
        for (spec_source, subtype), spec in sorted(self.node_specs.items()):
            if spec_source != source:
                continue
            for i in range(spec.count):
                yield self.node_fact(source, subtype, i)

    # ---------------------------------------------------------------- edges
    def _pool(self, source: str, subtype: str, prefix: str | None) -> list[str]:
        own = self._ids.get((source, subtype))
        if own:
            candidates = own
        else:
            candidates = [
                node_id
                for (s, st), ids in sorted(self._ids.items())
                if st == subtype
                for node_id in ids
            ]
        if prefix:
            candidates = [c for c in candidates if c.startswith(prefix)]
        return candidates

    def edge_capacity(self, spec: EdgeSpec) -> int:
        t = spec.target
        src = self._pool(t.source, t.src_subtype, spec.src_prefix)
        dst = self._pool(t.source, t.dst_subtype, spec.dst_prefix)
        same = t.src_subtype == t.dst_subtype
        return len(src) * len(dst) - (min(len(src), len(dst)) if same else 0)

    def edge_pairs(self, index: int) -> list[tuple[str, str]]:
        """Distinct (src, dst) pairs for edge target ``index``, no self-loops.

        Each destination is used about evenly; a source cycles over the
        destinations so a one-parent tree target (count == children) gives
        each child one parent.
        """
        cached = self._pairs.get(index)
        if cached is not None:
            return cached
        spec = self.edge_specs[index]
        t = spec.target
        src = self._pool(t.source, t.src_subtype, spec.src_prefix)
        dst = self._pool(t.source, t.dst_subtype, spec.dst_prefix)
        if not src or not dst:
            raise RegistryMismatch(
                f"{t.source} {t.triple}: no {'source' if not src else 'target'} "
                "records exist for this relationship at this scale"
            )
        capacity = self.edge_capacity(spec)
        if t.count > capacity:
            raise RegistryMismatch(
                f"{t.source} {t.triple}: {t.count} relationships requested but "
                f"only {capacity} distinct pairs exist at this scale"
            )
        s, d = len(src), len(dst)
        seen: set[int] = set()
        pairs: list[tuple[str, str]] = []
        for k in range(t.count):
            j0 = k % d
            i0 = (k + (k // d) * 7) % s
            chosen: tuple[int, int] | None = None
            for dj in range(d):
                j = (j0 + dj) % d
                for di in range(s):
                    i = (i0 + di) % s
                    if i * d + j not in seen and src[i] != dst[j]:
                        chosen = (i, j)
                        break
                if chosen is not None:
                    break
            if chosen is None:  # unreachable: count <= capacity was checked
                raise RegistryMismatch(f"{t.source} {t.triple}: ran out of pairs")
            i, j = chosen
            seen.add(i * d + j)
            pairs.append((src[i], dst[j]))
        self._pairs[index] = pairs
        return pairs

    @staticmethod
    def edge_fact(edge_type: str, src: str, dst: str, *, revision: int = 0) -> EdgeFact:
        return EdgeFact(
            src=src,
            dst=dst,
            edge_type=edge_type,
            attrs={"revision": revision} if revision else {},
            source_record_id={"id": f"{src}|{edge_type}|{dst}"},
            source_revision={"revision": revision},
        )

    def iter_edge_facts(self, source: str) -> Iterator[EdgeFact]:
        for index, spec in enumerate(self.edge_specs):
            if spec.target.source != source:
                continue
            for src, dst in self.edge_pairs(index):
                yield self.edge_fact(spec.target.edge_type, src, dst)

    # ------------------------------------------------------------ ordering
    def producer_order(self) -> list[str]:
        """Producers in an order where every relationship's endpoints exist.

        A producer whose relationships point at another producer's records
        runs after it. A cycle is refused rather than broken arbitrarily.
        """
        producers = sorted(self.plan.nodes)
        owner_of: dict[str, set[str]] = {}
        for source, subtype in self.node_specs:
            owner_of.setdefault(subtype, set()).add(source)
        after: dict[str, set[str]] = {p: set() for p in producers}
        for spec in self.edge_specs:
            t = spec.target
            for subtype in (t.src_subtype, t.dst_subtype):
                if (t.source, subtype) in self.node_specs:
                    continue
                after.setdefault(t.source, set()).update(owner_of.get(subtype, set()))
        order: list[str] = []
        done: set[str] = set()
        pending = set(after)
        while pending:
            ready = sorted(p for p in pending if after[p] <= done)
            if not ready:
                raise RegistryMismatch(
                    f"relationship endpoints form a producer cycle: {sorted(pending)}"
                )
            order.extend(ready)
            done.update(ready)
            pending.difference_update(ready)
        return order

    def edges_by_type(self) -> dict[str, int]:
        out: dict[str, int] = {}
        for spec in self.edge_specs:
            out[spec.target.edge_type] = out.get(spec.target.edge_type, 0) + spec.target.count
        return out

    def edges_by_source(self) -> dict[str, int]:
        out: dict[str, int] = {}
        for spec in self.edge_specs:
            out[spec.target.source] = out.get(spec.target.source, 0) + spec.target.count
        return out

    def records_of(self, source: str) -> Sequence[tuple[str, int]]:
        return [
            (subtype, spec.count)
            for (s, subtype), spec in sorted(self.node_specs.items())
            if s == source
        ]


__all__ = [
    "EdgeSpec",
    "NodeSpec",
    "RegistryMismatch",
    "WorkspaceFacts",
    "load_registry_signatures",
]
