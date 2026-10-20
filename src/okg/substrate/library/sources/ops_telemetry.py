"""Ops-telemetry source adapter — ITBench scenarios, application library,
alert definitions, and recorded telemetry, from frozen local trees.

Profile: `reference_catalog`.

WHAT CHANGED AND WHY (PACT okg-corpus-ops, designs/real-upstream-findings.md
plus this module's own re-measurement of all 66 upstream scenario indexes
at pinned upstream ref 7c68548, 2026-07-28). The previous version of this adapter was written against
scenarios 1-3 and emitted a directory inventory: one node per scenario index,
one node per recorded-run DIRECTORY whose only evidentiary attribute was
`file_count`. Two premises behind it are FALSE against the full upstream:

- `environment.applications` is NOT a service topology. All 66 scenarios
  carry exactly one application, and only two distinct ids exist corpus-wide
  (`opentelemetry-demo` x61, `book-info` x5). Each entry is `{"id": "<name>"}`
  — a foreign key into an application library, with no components and no
  relations. Reading it as a topology yields two nodes for the whole corpus.
- The scenario-index alert path is NOT dead code. 30 of 66 scenarios carry a
  non-empty `alerts` array (33 entries, 7 distinct names, every entry a bare
  string). Scenarios 1-3 ship `alerts: []` and are unrepresentative.

So the environment block is resolved through the application library, and the
alert names are resolved against the Prometheus rule DEFINITIONS that ship in
the same Apache-2.0 tree.

Inputs — three frozen local trees, NO live network calls:

- `scenarios_root`: ITBench scenario index JSONs (Apache-2.0 clone of
  github.com/itbench-hub/ITBench), `<clone>/scenarios/sre/library/indexes/
  scenarios/`, files like ``1.json``.
- `library_root`: the same clone's ``<clone>/scenarios/sre`` directory. The
  adapter reads, and only reads:
  ``library/indexes/applications/*.json`` (application definitions),
  ``project/roles/applications/defaults/main/managers.yaml`` (namespace,
  chart/git pin), ``project/roles/applications/tasks/install_*.yaml`` (which
  templates each application installs, and its deployed component labels),
  and the ``templates/helm/*/values.j2`` + ``templates/kubernetes/*/
  prometheusrules.j2`` those tasks name.
- `trajectories_root` (OPTIONAL): recorded telemetry, the CC-BY-NC-4.0
  ``ibm-research/ITBench-Trajectories`` layout model -> domain -> scenario ->
  run; operator-supplied local copy, never vendored. When absent, the
  topology and alert families still build in full — they need only the
  Apache-2.0 clone.

Emitted families:

- `ops_application` — one per application the library defines, with its
  namespace, platform, upstream repository, and chart/git pin.
- `ops_service` — one per component the application's own deployment
  artifacts name (helm values `components:` keys, `app.kubernetes.io/
  component` and `app` match-labels in the install task, and literal
  service-identifying label matchers inside the application's Prometheus
  rules). Each service records WHICH artifact named it (`origin`).
  `ops_application -runs-> ops_service`.
- `ops_alert` — a DEFINITION node per Prometheus alerting rule: PromQL
  `expr`, `for` window, `severity`, description annotation, rule group,
  template path. Keyed by (application, group, alert name) because an alert
  NAME is not unique within an application: otel-demo defines
  HighRequestLatency and HighRequestErrorRate twice (frontend and backend
  golden-signal groups) and book-info defines HighRequestErrorRate and
  NoRequestsReceived twice (productpage and gateway groups).
  `ops_application -defines-> ops_alert`.
- `ops_scenario` — one per scenario index, ALWAYS (not only when recorded).
  `ops_scenario -deploys-> ops_application`, and
  `ops_scenario -raises-> ops_alert` for every definition in the scenario's
  application whose name the index fires.
- `ops_alert -targets-> ops_service` — resolved from the rule's OWN label
  matchers, positive matchers only. Every alert states whether its target
  resolved (`targets_resolved`) and why not (`target_selector_kind`), so an
  unresolved rule is a reported, queryable fact, never a silent drop.
  `targets_resolved` is emission-derived, so it is published ALONGSIDE the
  declaration it should be read against: `declared_target_services` (the
  positive service matchers the rule's own PromQL states) and
  `unresolved_target_services` (those the topology does not define). An
  `ops_alert` always carries its `defines` edge and so is never isolated —
  no isolation probe keys on `targets_resolved`.
- `telemetry_run` / `telemetry_signal` — the recorded-telemetry index (see
  below), when `trajectories_root` is supplied.
- `telemetry_signal -about-> ops_service`, when the indexed artifact names a
  service the run's application defines. A signal that gets no `about` edge
  is the only tolerated isolate in this module, and it carries
  `no_subject_reason` saying WHICH upstream state produced it — read from the
  parsed artifact, never from whether the edge came out. See STAMPED, NOT
  INFERRED below.

STAMPED, NOT INFERRED. `no_subject_reason` is the reason the artifact names
no service of the application, and it is OMITTED whenever the artifact named
one that resolves. Absence of the attribute therefore means "the input DID
declare a resolvable subject", so a regression that drops the `about` edge
leaves an isolated signal carrying no stamp and an isolation probe fires.

The predecessor of this attribute, `subject_resolved`, was
`bool(signal.subject in service_names)` — the SAME expression as the `about`
edge's own gate. An isolated signal therefore carried `subject_resolved=false`
by construction and was exempted by construction: measured on the 2026-07-28
ops generation, 2 of 2 signals carried `false` and the complement was EMPTY.
It forgave precisely the subject-parsing regression its probe existed to
catch. It is gone; nothing in this adapter derives a stamp from an emitted
edge.

The three upstream states stay DISTINCT because they are three different
claims about the input, and a consumer may treat them differently:

- `artifact_empty` — the artifact holds zero records. The run recorded
  nothing, so it says nothing about any service.
- `no_subject_declared` — records exist and NONE carries any of
  `_SIGNAL_SUBJECT_KEYS`. The run observed something and did not say what
  it was about.
- `declared_subject_unknown` — the artifact NAMED a subject that the run's
  application does not define. `declared_subject_service` records which one.
  This is a FINDING, not a shrug: either the topology is incomplete or the
  telemetry is from another application, and an isolation probe is entitled
  to refuse it rather than exempt it.

GOLD EXCLUSION. Two different disciplines, because two different things are
at stake.

1. The application catalog (`ops_application`, `ops_service`, `ops_alert` and
   the edges among them) is built by `_build_catalog`, a pure function of the
   LIBRARY files that never receives a scenario document. It therefore cannot
   leak per-scenario gold by construction, and it MUST contain the component
   vocabulary the answers are graded against — the umbrella's rule is
   "nothing distinguishes the injected component from its peers", not "the
   answer's token is absent". All 61 `opentelemetry-demo` scenarios receive a
   byte-identical catalog.
2. Per-scenario facts are scanned by `_assert_gold_excluded` against every
   string of at least `_GOLD_FRAGMENT_MIN_CHARS` inside ``solutions``,
   ``disruptions`` AND ``description``. `description` is included because it
   is a per-scenario field that names the faulty component and the fault kind
   in prose on 59 of 66 scenarios ("... the OpenTelemetry Demo's `cart`
   service failing") while living OUTSIDE ``disruptions``/``solutions`` — so
   a stripped-gold rebuild that deletes only those two keys would not catch
   a description leak. `description` is never read for emission.

TRAJECTORY BOUNDARY. `TRAJECTORY_DENY_LIST` — `agent_output.json`,
`session.jsonl`, `judge_output.json` — is a hard deny-list, not a convention.
Those files are NEVER opened for content, supply no node and no attribute,
and are excluded from the snapshot the agent reads. `judge_output.json` is
graded against the gold; the other two are a prior agent's answer, which for
fault localization IS the answer unit and need match no gold string, so no
substring assertion could catch that leak. They keep exactly one role: their
PRESENCE marks a directory as a recorded run — a filename test, never a
content read. The constant is imported by the okg-bench itbench adapter's
snapshot-mount path too, so the corpus-side and prompt-side boundaries
cannot drift apart.

FAIL LOUDLY. Absence is never read as agreement — that specific defect is
what produced the 4-node corpus. Each of these raises, naming the counts:
zero scenario files; a scenario whose `environment.applications` is absent,
empty, or declares more than one application; a scenario naming an
application the library does not define; an application whose install task
names no template; a rule template that parses to zero alerting rules; an
application resolving to zero components; a rule template placeholder the
adapter does not know how to resolve; a recorded run with no classifiable
observability artifact. A scenario naming an alert the library does not
define, and an alert rule that resolves no service, are REPORTED — on the
node itself and in `last_run_report` — never stubbed and never dropped.

Per PACT `okg-corpus-ops` (2026-07-28); supersedes the
`okg-bench-domain-adapters` shape of 2026-07-27.
"""
from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from pathlib import Path, PurePath
from typing import Any, Iterator, Optional

import yaml

from okg.substrate.errors import SubstrateError
from okg.substrate.library.sources._cti_snapshot import (
    read_snapshot_bytes,
    record_set_from_facts,
)
from okg.substrate.library.sources.base import (
    EdgeFact,
    NodeFact,
    SourceRun,
)


__all__ = [
    "OpsTelemetrySource",
    "TRAJECTORY_DENY_LIST",
    "GROUND_TRUTH_DENY_LIST",
    "SNAPSHOT_DENY_LIST",
]


#: HARD DENY-LIST. These three filenames are never opened for content, never
#: supply a node or an attribute, and never enter the snapshot the agent
#: mounts. Their single permitted role is a FILENAME test marking a directory
#: as one recorded run. Imported by `okg.bench.ab.adapters.itbench` so the
#: corpus-side and prompt-side boundaries are one constant, not two.
TRAJECTORY_DENY_LIST = frozenset({
    "agent_output.json",
    "session.jsonl",
    "judge_output.json",
})

#: HARD DENY-LIST, second family: the ANSWER KEY that ships INSIDE the
#: published telemetry.
#:
#: `ibm-research/ITBench-Lite` bundles `ground_truth.yaml` in every scenario
#: directory alongside the alerts and the K8s dumps. Measured on the release
#: (2026-07-29): 50 files — 35 under `snapshots/sre/` and 15 under
#: `snapshots/finops/`. A deny-list scoped to the SRE tree would leave the
#: finops 15 mounted, so this is matched by FILENAME anywhere beneath a root,
#: never by a per-domain path prefix.
#:
#: Each SRE file carries `fault` (the injected entity), `alerts` (the labelled
#: alert set), `groups`, `aliases`, `propagations` and `recommended_actions` —
#: the fault entity, the answer's alert labels, and the propagation graph.
#: That is the complete answer unit for BOTH halves of the itbench score
#: (fault localization and remediation overlap), so no substring assertion
#: over emitted attrs could catch a leak of it: the grader does not require
#: the answer to match any particular gold string.
#:
#: IBM's own harness stripped these before mounting; the HuggingFace release
#: re-includes them. Treat their presence as the default, not the exception.
GROUND_TRUTH_DENY_LIST = frozenset({
    "ground_truth.yaml",
})

#: The single deny-list both the source and the snapshot-mount path consume.
#: One constant so the corpus-side and prompt-side boundaries cannot drift.
SNAPSHOT_DENY_LIST = TRAJECTORY_DENY_LIST | GROUND_TRUTH_DENY_LIST

#: text truncation bound (chars) for any ingested free text or exemplar
_TEXT_MAX_CHARS = 4000

#: minimum length of a gold text fragment enforced by the exclusion
#: assertion — full solution/injection/description string values at or above
#: this length must never appear inside any emitted per-scenario attr.
_GOLD_FRAGMENT_MIN_CHARS = 8

#: scenario-index keys this adapter is permitted to read for emission.
#: `description`, `disruptions` and `solutions` are absent ON PURPOSE and the
#: emitter never receives them.
_SCENARIO_EMIT_KEYS = frozenset({
    "alerts", "category", "complexity", "environment", "platforms",
})

#: scenario-index keys treated as gold for the exclusion scan. `description`
#: is here because it names the faulty component in prose on 59 of 66
#: scenarios while living outside disruptions/solutions.
_SCENARIO_GOLD_KEYS = ("solutions", "disruptions", "description")

#: per-scenario attrs whose values are IDENTIFIERS drawn from a
#: source-carried vocabulary rather than free text. These are proven by
#: MEMBERSHIP, not by a substring scan.
#:
#: A substring scan is the wrong instrument for them, and measurably so: the
#: application id `book-info` also appears inside scenario 36's `disruptions`
#: as a target namespace, so scanning it would refuse the corpus for naming
#: an application that all 5 book-info scenarios share and that therefore
#: distinguishes nothing. The umbrella's rule is "nothing distinguishes the
#: injected item from its peers", not "the token is absent". Membership is
#: the stronger check: it proves the value came from the library catalog or
#: from the index's own non-gold `alerts` array, rather than merely that it
#: does not happen to collide with a gold string.
_SCENARIO_IDENTIFIER_ATTRS = frozenset({
    "application_id",
    "declared_alert_names",
    "unresolved_alert_names",
    "subject_service",
})

#: PromQL label names that identify a SERVICE. Used both to harvest the
#: component vocabulary a rule mentions and to resolve `targets`.
_SERVICE_LABELS = (
    "service_name",
    "source_canonical_service",
    "destination_canonical_service",
    "destination_app",
)

#: one PromQL label matcher, e.g. service_name=~"frontend|frontend-proxy"
_LABEL_MATCHER = re.compile(
    r"(?P<label>[A-Za-z_][A-Za-z0-9_]*)\s*(?P<op>=~|!~|!=|=)\s*"
    r"\"(?P<value>[^\"]*)\""
)

#: an ansible template placeholder in the ITBench rule templates
_PLACEHOLDER = re.compile(r"\[\[\s*(?P<expr>.+?)\s*\]\]")

#: a template path referenced from an install task
_TEMPLATE_REF = re.compile(r"templates/[A-Za-z0-9_./-]+\.j2")

#: an `applications_managers.<key>` reference in an install task
_MANAGER_REF = re.compile(r"applications_managers\.([A-Za-z0-9_]+)")

#: signal-kind classification by artifact filename
_SIGNAL_KINDS = (
    ("log", "log"),
    ("metric", "metric"),
    ("trace", "trace"),
    ("span", "trace"),
    ("alert", "event"),
    ("event", "event"),
)

#: record keys that may name the service a telemetry signal is about
_SIGNAL_SUBJECT_KEYS = (
    "service_name", "service", "component", "app", "workload",
    "destination_app", "destination_canonical_service",
)

#: At or above this many signals NAMING a subject inside one recorded run,
#: resolving ZERO of them against the run's application FAILS the run. The
#: umbrella's rule ("zero resolved references in an input large enough to
#: contain them fails the build") applied to subject resolution: it is the
#: guard that keeps `declared_subject_unknown` from becoming a blanket
#: excuse when a topology regression empties the service vocabulary.
_MIN_DECLARED_SUBJECTS_FOR_FLOOR = 5

#: record keys that may carry a signal's timestamp
_SIGNAL_TIME_KEYS = (
    "timestamp", "time", "ts", "startsAt", "starts_at", "@timestamp",
)


# ------------------------------------------------- published telemetry
#
# `ibm-research/ITBench-Lite` (Apache-2.0, ungated). Layout:
#   snapshots/<domain>/<version>/Scenario-<N>/
#     alerts/alerts_at_<ISO>.json                  a LIST of alert objects
#     alerts/alerts_in_alerting_state_<ISO>.json   {"data": {"alerts": [...]}}
#     k8s_objects_raw.tsv / k8s_events_raw.tsv     17-column OTel export
#     ground_truth.yaml                            DENY-LISTED, never opened
#
# Measured on the release (2026-07-29, complete 1.576 GB slice):
# 35 SRE scenario snapshots; 34 name a scenario the 66-index tree defines;
# 1,839 `alerts_at_*` files / 16,037 entries; 33 `alerts_in_alerting_state_*`
# files / 91 entries; the two patterns agree on every distinct alertname.
# Scenario-11 has no `alerts/` directory at all — upstream absence.

#: a published scenario-snapshot directory
_SCENARIO_DIR = re.compile(r"^Scenario-(?P<number>\d+)$")

#: both published alert-file patterns. Globbing only the first drops 33 files
#: / 91 entries and ZERO distinct alertnames — a quantitative drop, but a drop.
_ALERT_FILE_PREFIXES = ("alerts_at_", "alerts_in_alerting_state_")

#: the exact 17-column header BOTH k8s TSVs carry, verified against the
#: release before this parser was written. `_raw` means `transform=False`
#: upstream, so the Kubernetes object is JSON inside `Body` rather than
#: exploded into `object_kind` / `object_name` / `namespace` columns.
#: `Body` is CSV-QUOTED inside the TSV, so it needs a csv reader, not a split.
_K8S_TSV_HEADER = (
    "Timestamp", "TimestampTime", "TraceId", "SpanId", "TraceFlags",
    "SeverityText", "SeverityNumber", "ServiceName", "Body",
    "ResourceSchemaUrl", "ResourceAttributes", "ScopeSchemaUrl", "ScopeName",
    "ScopeVersion", "ScopeAttributes", "LogAttributes", "EventName",
)

#: Kubernetes kinds that constitute the SERVICE ROSTER. Workloads, NOT
#: `Service` objects: measured, `Service` yields 19 for otel-demo and DROPS
#: `accounting` and `fraud-detection`, which are Kafka consumers with no
#: inbound Service. `accounting` is the sole positive target of the
#: NoDatabaseOperations rule, so a Service-based roster would silently break
#: the one `targets` edge that resolves.
_WORKLOAD_KINDS = frozenset({"Deployment", "StatefulSet", "DaemonSet"})

#: labels on a firing that may name the service it is about
_FIRING_SERVICE_LABELS = ("service_name", "service")


# ---------------------------------------------------------------- catalog


@dataclass(frozen=True)
class _AlertRule:
    """One Prometheus alerting rule as the library DEFINES it."""

    application_id: str
    group: str
    alert_name: str
    expr: str
    for_window: str
    severity: str
    description: str
    template_path: str
    #: service names the rule's POSITIVE matchers select
    positive_services: tuple[str, ...]
    #: service names the rule mentions in any matcher (positive or negative);
    #: vocabulary evidence, not a target
    mentioned_services: tuple[str, ...]
    #: 'positive' | 'negative' | 'none' — why targets did or did not resolve
    selector_kind: str


@dataclass(frozen=True)
class _Application:
    application_id: str
    name: str
    index: int
    description: str
    repository: str
    platforms: tuple[str, ...]
    authors: tuple[str, ...]
    namespace: str
    deploy_kind: str
    deploy_ref: str
    deploy_version: str
    #: (service name, the artifact that named it)
    services: tuple[tuple[str, str], ...]
    rules: tuple[_AlertRule, ...]


@dataclass
class _Catalog:
    applications: dict[str, _Application] = field(default_factory=dict)
    #: (application_id, alert_name) -> rules defining that name
    rules_by_name: dict[tuple[str, str], list[_AlertRule]] = field(
        default_factory=dict
    )


@dataclass
class OpsTelemetrySource:
    """ITBench scenario + application-library + recorded-telemetry adapter
    over frozen local trees. There is no live mode.

    `repo` names the upstream scenario repo (recorded in node identity and
    revision); `scenarios_root` holds the scenario index JSONs;
    `library_root` is the clone's ``scenarios/sre`` directory;
    `trajectories_root` holds the recorded runs and is OPTIONAL — the
    topology and alert families build from the Apache-2.0 clone alone.
    """

    name: str = "ops_telemetry"
    profile: str = "reference_catalog"
    repo: str = "itbench-hub/ITBench"
    scenarios_root: Optional[Path | str] = None
    library_root: Optional[Path | str] = None
    trajectories_root: Optional[Path | str] = None
    #: published Apache-2.0 telemetry (`ibm-research/ITBench-Lite`), laid out
    #: as `snapshots/<domain>/<version>/Scenario-<N>/`. OPTIONAL: the topology
    #: and alert-definition families build from the clone alone. When
    #: supplied, its k8s workload roster REPLACES the helm reconstruction for
    #: every application whose namespace it covers.
    telemetry_root: Optional[Path | str] = None

    #: populated by run(): counts, plus the NAMES behind every reported (not
    #: dropped) resolution failure.
    last_run_report: dict[str, Any] = field(default_factory=dict)

    # Incremental-source contract (issue #323): frozen local trees
    # -> the content-hash floor over every file's bytes.
    change_probe_kind = "content_hash"

    def __post_init__(self) -> None:
        self.change_probe = self._build_change_probe()
        # The applied token is computed over the SAME item enumeration the
        # probe hashes (_probe_items: every file under every configured
        # root), so a matching token proves identical emission. A narrower
        # run-side set (only parsed files) would make the tokens structurally
        # unequal and silently void the short-circuit forever.
        self.probe_short_circuit_safe = True

    # ---------- change probe ----------

    def _roots(self) -> list[Path]:
        return [
            Path(root)
            for root in (
                self.scenarios_root, self.library_root,
                self.trajectories_root, self.telemetry_root,
            )
            if root is not None
        ]

    def _probe_items(self) -> list[tuple[str, "Path"]]:
        items: list[tuple[str, Path]] = []
        for root_path in self._roots():
            if not root_path.is_dir():
                continue
            items.extend(
                (str(path), path)
                for path in sorted(root_path.rglob("*"))
                if path.is_file()
            )
        return items

    def _build_change_probe(self) -> Any:
        import sys

        from okg.substrate.library.sources.content_hash_probe import (
            ContentHashProbe,
        )
        from okg.substrate.library.sources.emission_identity import (
            emission_identity_targets,
        )

        return ContentHashProbe(
            content_items=self._probe_items,
            config={
                "repo": self.repo,
                "scenarios_root": _as_str(self.scenarios_root),
                "library_root": _as_str(self.library_root),
                "trajectories_root": _as_str(self.trajectories_root),
                "telemetry_root": _as_str(self.telemetry_root),
            },
            emit_targets=emission_identity_targets(
                self.name,
                sys.modules[__name__],
            ),
        )

    # ---------- run ----------

    def run(
        self,
        run_id: str,
        *,
        mode: str = "scope_complete",
        cursor: Optional[dict[str, Any]] = None,
    ) -> SourceRun:
        if self.scenarios_root is None or self.library_root is None:
            raise SubstrateError(
                f"source {self.name!r}: pass scenarios_root and library_root",
                fix_hint=(
                    "scenarios_root=<clone>/scenarios/sre/library/indexes/"
                    "scenarios, library_root=<clone>/scenarios/sre; "
                    "trajectories_root is optional (the topology and alert "
                    "families build from the Apache-2.0 clone alone)"
                ),
            )
        scenarios_root = Path(self.scenarios_root)
        library_root = Path(self.library_root)
        trajectories_root = (
            Path(self.trajectories_root)
            if self.trajectories_root is not None else None
        )
        telemetry_root = (
            Path(self.telemetry_root)
            if self.telemetry_root is not None else None
        )
        roots = [("scenarios_root", scenarios_root),
                 ("library_root", library_root)]
        if trajectories_root is not None:
            roots.append(("trajectories_root", trajectories_root))
        if telemetry_root is not None:
            roots.append(("telemetry_root", telemetry_root))
        for label, root in roots:
            if not root.is_dir():
                raise SubstrateError(
                    f"source {self.name!r}: {label} {root} is not a directory",
                    data={"adapter": self.name, label: str(root)},
                    fix_hint="point it at the frozen local copy",
                )

        facts = list(self._normalize(
            scenarios_root, library_root, trajectories_root, telemetry_root,
        ))

        completed_scope = mode in {"reconcile", "scope_complete", "release_new"}
        record_set = (
            record_set_from_facts(facts) if completed_scope else None
        )
        # Token scope MUST equal the probe's: hash every file under every
        # root, not just the files this run parsed — otherwise the fresh
        # probe token can never match and the #323 short-circuit is
        # permanently (and silently) disabled.
        applied_raw_token = self.change_probe.token_from_item_hashes(
            {
                key: hashlib.sha256(path.read_bytes()).hexdigest()
                for key, path in self._probe_items()
            }
        )

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

    # ---------- normalization ----------

    def _normalize(
        self,
        scenarios_root: Path,
        library_root: Path,
        trajectories_root: Optional[Path],
        telemetry_root: Optional[Path] = None,
    ) -> Iterator[NodeFact | EdgeFact]:
        source_revision = {"repo": self.repo, "corpus": "itbench_sre"}
        report: dict[str, Any] = {
            "scenarios": 0,
            "applications": 0,
            "services": 0,
            "alert_definitions": 0,
            "deploys_edges": 0,
            "raises_edges": 0,
            "targets_edges": 0,
            "telemetry_runs": 0,
            "telemetry_signals": 0,
            "unresolved_alert_names": {},
            "alerts_without_target": [],
            "signals_without_subject": 0,
            # `no_subject_reason` value -> count, and the unresolvable
            # subject names in full. A stamp nobody can count is a stamp
            # nobody can audit for discrimination.
            "no_subject_reasons": {},
            "unresolved_subject_services": [],
            "unrecorded_scenarios": 0,
            # published telemetry
            "telemetry_snapshots": 0,
            "alert_firings": 0,
            "firing_targets_edges": 0,
            "no_target_reasons": {},
            "firing_services_outside_topology": {},
            # scenarios the telemetry publishes that the 66-index tree does
            # NOT define. REPORTED, never silently subtracted from a floor:
            # a count that quietly excludes a case is where a real regression
            # later hides. Measured on the release: [9].
            "telemetry_scenarios_without_index": [],
            "roster_source": {},
            "roster_deviations": {},
        }
        self.last_run_report = report

        # 1. The application catalog. A pure function of the LIBRARY tree —
        #    it never sees a scenario document, so it cannot carry
        #    per-scenario gold.
        catalog = _build_catalog(library_root, adapter=self.name)

        # 1b. The published telemetry's k8s workload roster REPLACES the helm
        #     reconstruction wherever it covers the application's namespace.
        #     The roster is the UNION over every scenario, which is what keeps
        #     it from encoding any one scenario's fault — see _workload_roster.
        snapshots: dict[int, _ScenarioSnapshot] = {}
        if telemetry_root is not None:
            snapshots = _find_snapshots(telemetry_root, adapter=self.name)
            if not snapshots:
                raise SubstrateError(
                    f"source {self.name!r}: telemetry_root {telemetry_root} "
                    f"holds zero Scenario-<N> snapshot directories; an empty "
                    f"telemetry layer must never be read as 'this release "
                    f"records nothing'",
                    data={"adapter": self.name,
                          "telemetry_root": str(telemetry_root)},
                    fix_hint="point telemetry_root at the release root "
                             "containing snapshots/<domain>/<version>/",
                )
            report["telemetry_snapshots"] = len(snapshots)
            roster, deviations, _counts = _workload_roster(
                snapshots, adapter=self.name,
            )
            report["roster_deviations"] = deviations
            catalog = _apply_workload_roster(
                catalog, roster, report=report, adapter=self.name,
            )

        yield from self._catalog_facts(
            catalog, source_revision=source_revision, report=report,
        )

        # 2. Recorded telemetry, when the operator supplied it.
        recordings: dict[int, list[_Recording]] = {}
        if trajectories_root is not None:
            recordings = _find_recordings(trajectories_root, adapter=self.name)

        # 3. Scenarios.
        scenario_paths = sorted(scenarios_root.glob("*.json"))
        if not scenario_paths:
            raise SubstrateError(
                f"source {self.name!r}: {scenarios_root} contains zero "
                f"scenario index JSONs",
                data={"adapter": self.name,
                      "scenarios_root": str(scenarios_root)},
                fix_hint="point scenarios_root at the scenario index "
                         "directory (files like 1.json)",
            )
        for path in scenario_paths:
            raw = read_snapshot_bytes(path, adapter=self.name)
            number, scenario = self._parse_scenario(path, raw)
            runs = recordings.get(number, [])
            if not runs:
                report["unrecorded_scenarios"] += 1
            facts = list(self._scenario_facts(
                number, scenario, catalog, runs,
                source_revision=source_revision, report=report,
            ))
            snap = snapshots.pop(number, None)
            if snap is not None:
                application_id = _scenario_application_id(
                    scenario, path, adapter=self.name,
                )
                facts.extend(self._snapshot_facts(
                    snap,
                    f"ops_scenario:{self.repo}:{number}",
                    catalog.applications.get(application_id),
                    source_revision=source_revision,
                    report=report,
                ))
            self._assert_gold_excluded(path, scenario, facts, catalog)
            report["scenarios"] += 1
            yield from facts

        # Telemetry the 66-index tree does not define. REPORTED and EMITTED —
        # not dropped, and not quietly subtracted from the recorded_by floor.
        # Measured on the release: Scenario-9 is published with no index. A
        # floor that silently excluded it is exactly where a later regression
        # that lost a real scenario would hide.
        for number in sorted(snapshots):
            snap = snapshots[number]
            # Resolve the application from the TELEMETRY's own namespace
            # labels rather than passing None. Passing None would empty
            # `service_names` and stamp every firing
            # `service_outside_topology` — a claim that the service is not in
            # the topology, when the truth is that we never looked it up.
            # That is absence dressed as agreement, and it measurably fired:
            # before this, `frontend-proxy` (a real member of the 21-workload
            # roster) appeared 4 times in the outside-topology census.
            yield from self._snapshot_facts(
                snap, None,
                _application_for_namespaces(catalog, snap, adapter=self.name),
                source_revision=source_revision, report=report,
            )

    # ---------- catalog emission ----------

    def _catalog_facts(
        self,
        catalog: _Catalog,
        *,
        source_revision: dict[str, Any],
        report: dict[str, Any],
    ) -> Iterator[NodeFact | EdgeFact]:
        for app in catalog.applications.values():
            app_node_id = self._application_node_id(app.application_id)
            app_record_id = {
                "repo": self.repo, "application": app.application_id,
            }
            yield NodeFact(
                node_id=app_node_id,
                subtype="ops_application",
                attrs={
                    "application_id": app.application_id,
                    "name": app.name,
                    "library_index": app.index,
                    "summary": app.description[:_TEXT_MAX_CHARS],
                    "upstream_repository": app.repository,
                    "platforms": ", ".join(app.platforms),
                    "authors": ", ".join(app.authors),
                    "namespace": app.namespace,
                    "deploy_kind": app.deploy_kind,
                    "deploy_ref": app.deploy_ref,
                    "deploy_version": app.deploy_version,
                    "service_count": len(app.services),
                    "alert_definition_count": len(app.rules),
                },
                source_record_id=app_record_id,
                source_revision=source_revision,
                op="I",
            )
            report["applications"] += 1

            for service_name, origin in app.services:
                svc_node_id = self._service_node_id(
                    app.application_id, service_name,
                )
                yield NodeFact(
                    node_id=svc_node_id,
                    subtype="ops_service",
                    attrs={
                        "service_name": service_name,
                        "application_id": app.application_id,
                        "namespace": app.namespace,
                        "origin": origin,
                    },
                    source_record_id={
                        "repo": self.repo,
                        "application": app.application_id,
                        "service": service_name,
                    },
                    source_revision=source_revision,
                    op="I",
                )
                yield EdgeFact(
                    src=app_node_id,
                    dst=svc_node_id,
                    edge_type="runs",
                    source_record_id=app_record_id,
                    source_revision=source_revision,
                )
                report["services"] += 1

            service_names = {name for name, _ in app.services}
            for rule in app.rules:
                alert_node_id = self._alert_node_id(rule)
                resolved = [
                    svc for svc in rule.positive_services
                    if svc in service_names
                ]
                yield NodeFact(
                    node_id=alert_node_id,
                    subtype="ops_alert",
                    attrs={
                        "alert_name": rule.alert_name,
                        "application_id": rule.application_id,
                        "rule_group": rule.group,
                        "expr": rule.expr[:_TEXT_MAX_CHARS],
                        "expr_truncated": len(rule.expr) > _TEXT_MAX_CHARS,
                        "for_window": rule.for_window,
                        "severity": rule.severity,
                        "description": rule.description[:_TEXT_MAX_CHARS],
                        "template_path": rule.template_path,
                        "target_selector_kind": rule.selector_kind,
                        "targets_resolved": bool(resolved),
                        # `targets_resolved` alone is derived from the
                        # edges emitted just below, so it cannot tell a
                        # rule that named no service apart from one whose
                        # `targets` edge was lost. These two carry the
                        # DECLARATION — the positive service matchers the
                        # rule's own PromQL states, and which of them the
                        # topology does not define — so the pair is
                        # readable against each other. `ops_alert` is
                        # never isolated (it always has its `defines`
                        # edge), so no isolation probe keys on this; the
                        # symmetry is for the reader and for a future one.
                        "declared_target_services": ", ".join(
                            rule.positive_services
                        ),
                        "unresolved_target_services": ", ".join(
                            svc for svc in rule.positive_services
                            if svc not in service_names
                        ),
                        "mentioned_services": ", ".join(
                            rule.mentioned_services
                        ),
                    },
                    source_record_id={
                        "repo": self.repo,
                        "application": rule.application_id,
                        "rule_group": rule.group,
                        "alert": rule.alert_name,
                    },
                    source_revision=source_revision,
                    op="I",
                )
                yield EdgeFact(
                    src=app_node_id,
                    dst=alert_node_id,
                    edge_type="defines",
                    source_record_id=app_record_id,
                    source_revision=source_revision,
                )
                report["alert_definitions"] += 1
                if not resolved:
                    # REPORTED by name, never dropped. The node itself
                    # carries targets_resolved=false and the reason.
                    report["alerts_without_target"].append(
                        f"{rule.application_id}/{rule.group}/{rule.alert_name}"
                        f" ({rule.selector_kind})"
                    )
                for svc in resolved:
                    yield EdgeFact(
                        src=alert_node_id,
                        dst=self._service_node_id(rule.application_id, svc),
                        edge_type="targets",
                        source_record_id={
                            "repo": self.repo,
                            "application": rule.application_id,
                            "rule_group": rule.group,
                            "alert": rule.alert_name,
                            "service": svc,
                        },
                        source_revision=source_revision,
                    )
                    report["targets_edges"] += 1

    # ---------- scenario emission ----------

    def _parse_scenario(
        self, path: Path, raw: bytes,
    ) -> tuple[int, dict[str, Any]]:
        if not re.fullmatch(r"\d+", path.stem):
            raise SubstrateError(
                f"source {self.name!r}: scenario file {path} is not "
                f"named <scenario_number>.json",
                data={"adapter": self.name, "path": str(path)},
                fix_hint="point scenarios_root at the scenario index "
                         "directory",
            )
        try:
            scenario = json.loads(raw)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise SubstrateError(
                f"source {self.name!r}: {path} is not valid JSON: {exc}",
                data={"adapter": self.name, "path": str(path)},
                fix_hint="re-clone the scenario repo",
            ) from exc
        if not isinstance(scenario, dict):
            raise SubstrateError(
                f"source {self.name!r}: {path} must be a JSON object, "
                f"got {type(scenario).__name__}",
                data={"adapter": self.name, "path": str(path)},
                fix_hint="re-clone the scenario repo",
            )
        alerts = scenario.get("alerts")
        # Measured over all 66 upstream indexes (pinned 7c68548): 30 carry
        # a NON-EMPTY alerts array — 33 entries, 7 distinct names, every entry
        # a bare string. The `alerts: []` of scenarios 1-3 is the minority
        # case, not the rule. An empty list is legal; a non-list is not, and
        # a non-string entry is not (there is no upstream precedent for one,
        # so accepting it would be inventing a shape).
        if not isinstance(alerts, list):
            raise SubstrateError(
                f"source {self.name!r}: scenario {path} alerts must be a list",
                data={"adapter": self.name, "path": str(path)},
                fix_hint="re-clone the scenario repo; the index schema "
                         "declares alerts as an array",
            )
        for idx, entry in enumerate(alerts):
            if not isinstance(entry, str) or not entry.strip():
                raise SubstrateError(
                    f"source {self.name!r}: scenario {path} alerts[{idx}] "
                    f"must be a non-empty alert NAME string, got "
                    f"{type(entry).__name__}",
                    data={"adapter": self.name, "path": str(path),
                          "index": idx},
                    fix_hint="all 33 alert entries across the 66 upstream "
                             "indexes are bare strings; a new shape needs an "
                             "explicit adapter change, not a silent coercion",
                )
        for key in ("category", "complexity"):
            if not isinstance(scenario.get(key), str) or not scenario[key]:
                raise SubstrateError(
                    f"source {self.name!r}: scenario {path} lacks a "
                    f"{key} string",
                    data={"adapter": self.name, "path": str(path),
                          "field": key},
                    fix_hint="re-clone the scenario repo",
                )
        if "platforms" not in scenario:
            raise SubstrateError(
                f"source {self.name!r}: scenario {path} lacks platforms",
                data={"adapter": self.name, "path": str(path)},
                fix_hint="re-clone the scenario repo",
            )
        _scenario_application_id(scenario, path, adapter=self.name)
        return int(path.stem), scenario

    def _scenario_facts(
        self,
        number: int,
        scenario: dict[str, Any],
        catalog: _Catalog,
        runs: list["_Recording"],
        *,
        source_revision: dict[str, Any],
        report: dict[str, Any],
    ) -> Iterator[NodeFact | EdgeFact]:
        # Only the allow-listed keys reach the emitter. `description`,
        # `disruptions` and `solutions` are structurally unreachable here.
        emit = {k: v for k, v in scenario.items() if k in _SCENARIO_EMIT_KEYS}
        application_id = _scenario_application_id(
            emit, Path(f"{number}.json"), adapter=self.name,
        )
        app = catalog.applications.get(application_id)
        if app is None:
            raise SubstrateError(
                f"source {self.name!r}: scenario {number} deploys application "
                f"{application_id!r}, which the library does not define "
                f"({len(catalog.applications)} defined: "
                f"{sorted(catalog.applications)})",
                data={"adapter": self.name, "scenario": number,
                      "application": application_id},
                fix_hint="point library_root at <clone>/scenarios/sre so "
                         "library/indexes/applications/*.json resolves",
            )

        scenario_node_id = f"ops_scenario:{self.repo}:{number}"
        scenario_record_id = {"repo": self.repo, "scenario": number}
        platforms = emit["platforms"]
        platforms_value = (
            ", ".join(str(p) for p in platforms)
            if isinstance(platforms, list) else str(platforms)
        )
        declared = [str(a) for a in emit["alerts"]]
        unresolved = [
            name for name in declared
            if not catalog.rules_by_name.get((application_id, name))
        ]
        for name in unresolved:
            counts = report["unresolved_alert_names"]
            counts[name] = counts.get(name, 0) + 1

        yield NodeFact(
            node_id=scenario_node_id,
            subtype="ops_scenario",
            attrs={
                "scenario_number": number,
                "category": emit["category"],
                "complexity": emit["complexity"],
                "platforms": platforms_value,
                "application_id": application_id,
                "declared_alert_names": ", ".join(declared),
                "declared_alert_count": len(declared),
                # REPORTED, never stubbed: the library defines no rule of
                # this name for this application. 20 of the 33 upstream
                # alert entries are in this state — KubePodNotReady,
                # KubeContainerWaiting, KubePodCrashLooping and
                # KubeHpaMaxedOut come from the kube-prometheus-stack chart,
                # which is not in the ITBench tree.
                "unresolved_alert_names": ", ".join(unresolved),
                "unresolved_alert_count": len(unresolved),
                "recorded_run_count": len(runs),
            },
            source_record_id=scenario_record_id,
            source_revision=source_revision,
            op="I",
        )
        yield EdgeFact(
            src=scenario_node_id,
            dst=self._application_node_id(application_id),
            edge_type="deploys",
            source_record_id=scenario_record_id,
            source_revision=source_revision,
        )
        report["deploys_edges"] += 1
        for name in declared:
            for rule in catalog.rules_by_name.get((application_id, name), []):
                yield EdgeFact(
                    src=scenario_node_id,
                    dst=self._alert_node_id(rule),
                    edge_type="raises",
                    source_record_id=scenario_record_id,
                    source_revision=source_revision,
                )
                report["raises_edges"] += 1

        for recording in runs:
            yield from self._recording_facts(
                number, scenario_node_id, scenario_record_id, recording,
                app, source_revision=source_revision, report=report,
            )

    # ---------- recorded telemetry ----------

    def _recording_facts(
        self,
        number: int,
        scenario_node_id: str,
        scenario_record_id: dict[str, Any],
        recording: "_Recording",
        app: _Application,
        *,
        source_revision: dict[str, Any],
        report: dict[str, Any],
    ) -> Iterator[NodeFact | EdgeFact]:
        run_node_id = f"telemetry_run:{self.repo}:{recording.rel_path}"
        run_record_id = {
            "repo": self.repo, "scenario": number, "run": recording.rel_path,
        }
        signals = _read_signals(recording, adapter=self.name)
        yield NodeFact(
            node_id=run_node_id,
            subtype="telemetry_run",
            attrs={
                "model": recording.model,
                "run_path": recording.rel_path,
                "file_count": recording.file_count,
                "signal_count": len(signals),
            },
            source_record_id=run_record_id,
            source_revision=source_revision,
            op="I",
        )
        yield EdgeFact(
            src=scenario_node_id,
            dst=run_node_id,
            edge_type="recorded_by",
            source_record_id=scenario_record_id,
            source_revision=source_revision,
        )
        report["telemetry_runs"] += 1

        service_names = {name for name, _ in app.services}
        observed: set[str] = set()
        declared_subjects = 0
        resolved_subjects = 0
        for signal in signals:
            subject = signal.subject if signal.subject in service_names else ""
            signal_node_id = (
                f"telemetry_signal:{self.repo}:{recording.rel_path}:"
                f"{signal.kind}:{subject or '_'}"
            )
            attrs: dict[str, Any] = {
                "kind": signal.kind,
                "artifact_path": signal.artifact_path,
                "record_count": signal.record_count,
                "subject_service": subject,
                "exemplar": signal.exemplar[:_TEXT_MAX_CHARS],
                "exemplar_truncated": len(signal.exemplar) > _TEXT_MAX_CHARS,
            }
            # WHY this signal names no service of the application, read
            # from the parsed artifact. OMITTED when the artifact named one
            # that resolves — so an isolated signal with no stamp is an
            # `about` edge that was lost, not an artifact that never had
            # one. Never `bool(subject)`: that is the edge gate itself.
            reason = _no_subject_reason(signal, service_names)
            if reason is None:
                resolved_subjects += 1
            else:
                attrs["no_subject_reason"] = reason
                report["no_subject_reasons"][reason] = (
                    report["no_subject_reasons"].get(reason, 0) + 1
                )
            if signal.subject:
                declared_subjects += 1
                # WHICH name the artifact used. Kept on the node for the
                # unresolvable case because an unresolvable subject is a
                # finding an operator has to be able to read, and dropping
                # it would leave `declared_subject_unknown` unactionable.
                attrs["declared_subject_service"] = signal.subject
                if reason == "declared_subject_unknown":
                    report["unresolved_subject_services"].append(
                        f"{recording.rel_path}:{signal.artifact_path}:"
                        f"{signal.subject}"
                    )
            if signal.window_start:
                attrs["window_start"] = signal.window_start
            if signal.window_end:
                attrs["window_end"] = signal.window_end
            yield NodeFact(
                node_id=signal_node_id,
                subtype="telemetry_signal",
                attrs=attrs,
                source_record_id={
                    **run_record_id, "signal": signal.artifact_path,
                },
                source_revision=source_revision,
                op="I",
            )
            report["telemetry_signals"] += 1
            if subject:
                yield EdgeFact(
                    src=signal_node_id,
                    dst=self._service_node_id(app.application_id, subject),
                    edge_type="about",
                    source_record_id=run_record_id,
                    source_revision=source_revision,
                )
            else:
                report["signals_without_subject"] += 1
            observed |= signal.alert_names

        if (
            declared_subjects >= _MIN_DECLARED_SUBJECTS_FOR_FLOOR
            and resolved_subjects == 0
        ):
            # The umbrella's rule, applied to subject resolution: zero
            # resolved references in an input large enough to contain them
            # FAILS the build, so "no matches" cannot masquerade as "none
            # exist". Without this, a topology regression that emptied
            # `service_names` would restamp every signal
            # `declared_subject_unknown` and go on producing a corpus.
            raise SubstrateError(
                f"source {self.name!r}: recorded run {recording.rel_path} "
                f"has {declared_subjects} signal(s) naming a subject "
                f"service and resolved ZERO of them against application "
                f"{app.application_id!r} "
                f"({len(app.services)} service(s) defined)",
                data={
                    "adapter": self.name,
                    "run": recording.rel_path,
                    "application": app.application_id,
                    "declared_subjects": declared_subjects,
                    "services": sorted(service_names),
                },
                fix_hint="the application topology and the recorded "
                         "telemetry disagree about service naming; check "
                         "the library components before ingesting",
            )

        for rule in app.rules:
            if rule.alert_name not in observed:
                continue
            yield EdgeFact(
                src=run_node_id,
                dst=self._alert_node_id(rule),
                edge_type="observed",
                source_record_id=run_record_id,
                source_revision=source_revision,
            )

    # ---------- published telemetry emission ----------

    def _snapshot_facts(
        self,
        snap: _ScenarioSnapshot,
        scenario_node_id: Optional[str],
        app: Optional[_Application],
        *,
        source_revision: dict[str, Any],
        report: dict[str, Any],
    ) -> Iterator[NodeFact | EdgeFact]:
        """One published telemetry snapshot: the run, and its firings.

        NO `observed -> ops_alert` edge is emitted, deliberately. The
        recorded and defined alert vocabularies are DISJOINT (measured: 16
        observed names, 17 definitions, empty intersection) and no revision
        of the ITBench tree closes it, so any such edge would assert a
        correspondence neither artifact states."""
        run_node_id = f"telemetry_run:{self.repo}:{snap.number}"
        run_record_id = {
            "repo": self.repo, "scenario": snap.number, "run": snap.rel_path,
        }
        firings = _read_firings(snap, adapter=self.name)
        service_names = {name for name, _ in app.services} if app else set()

        yield NodeFact(
            node_id=run_node_id,
            subtype="telemetry_run",
            attrs={
                "model": "",
                "run_path": snap.rel_path,
                "file_count": snap.file_count,
                "signal_count": 0,
                "alert_firing_count": len(firings),
                "has_object_dump": snap.objects_tsv is not None,
                "has_event_dump": snap.events_tsv is not None,
                # REPORTED on the node, not merely subtracted from a floor.
                "scenario_indexed": scenario_node_id is not None,
            },
            source_record_id=run_record_id,
            source_revision=source_revision,
            op="I",
        )
        report["telemetry_runs"] += 1
        if scenario_node_id is not None:
            yield EdgeFact(
                src=scenario_node_id,
                dst=run_node_id,
                edge_type="recorded_by",
                source_record_id={"repo": self.repo, "scenario": snap.number},
                source_revision=source_revision,
            )
        else:
            report["telemetry_scenarios_without_index"].append(snap.number)

        for firing in firings:
            firing_node_id = self._firing_node_id(firing)
            attrs: dict[str, Any] = {
                "alert_name": firing.alert_name,
                "scenario_number": firing.scenario,
                "active_at": firing.active_at,
                "severity": firing.severity,
                "namespace": firing.namespace,
                "state": firing.state,
                "description": firing.description[:_TEXT_MAX_CHARS],
                "artifact_path": firing.artifact_path,
                "declared_service_label": firing.service_label,
            }
            reason = _no_target_reason(firing, service_names)
            if reason is not None:
                attrs["no_target_reason"] = reason
                report["no_target_reasons"][reason] = (
                    report["no_target_reasons"].get(reason, 0) + 1
                )
                if reason == "service_outside_topology":
                    counts = report["firing_services_outside_topology"]
                    counts[firing.service_label] = (
                        counts.get(firing.service_label, 0) + 1
                    )
            yield NodeFact(
                node_id=firing_node_id,
                subtype="alert_firing",
                attrs=attrs,
                source_record_id={
                    **run_record_id,
                    "alert": firing.alert_name,
                    "active_at": firing.active_at,
                },
                source_revision=source_revision,
                op="I",
            )
            report["alert_firings"] += 1
            yield EdgeFact(
                src=run_node_id,
                dst=firing_node_id,
                edge_type="observed",
                source_record_id=run_record_id,
                source_revision=source_revision,
            )
            if reason is None and app is not None:
                yield EdgeFact(
                    src=firing_node_id,
                    dst=self._service_node_id(
                        app.application_id, firing.service_label,
                    ),
                    edge_type="targets",
                    source_record_id=run_record_id,
                    source_revision=source_revision,
                )
                report["firing_targets_edges"] += 1

    # ---------- node identity ----------

    def _firing_node_id(self, firing: _Firing) -> str:
        return (
            f"alert_firing:{self.repo}:{firing.scenario}:"
            f"{firing.alert_name}:{firing.active_at}"
        )

    def _application_node_id(self, application_id: str) -> str:
        return f"ops_application:{self.repo}:{application_id}"

    def _service_node_id(self, application_id: str, service: str) -> str:
        return f"ops_service:{self.repo}:{application_id}:{service}"

    def _alert_node_id(self, rule: _AlertRule) -> str:
        return (
            f"ops_alert:{self.repo}:{rule.application_id}:"
            f"{rule.group}:{rule.alert_name}"
        )

    # ---------- gold exclusion contract ----------

    def _assert_gold_excluded(
        self,
        path: Path,
        scenario: dict[str, Any],
        facts: list[NodeFact | EdgeFact],
        catalog: _Catalog,
    ) -> None:
        """Raise if any PER-SCENARIO emitted attr carries gold.

        Two instruments, because the emitted attrs are of two kinds.

        FREE TEXT is substring-scanned. Gold fragments are every string
        value inside ``solutions``, ``disruptions`` (injections included)
        AND ``description`` of at least ``_GOLD_FRAGMENT_MIN_CHARS`` chars;
        none may appear, even as a substring.

        IDENTIFIERS (`_SCENARIO_IDENTIFIER_ATTRS`) are proven by MEMBERSHIP
        in a source-carried vocabulary — the library's application ids and
        the application's service names, plus the alert names the index's
        own non-gold ``alerts`` array declares. A value outside that
        vocabulary raises: it could only have come from somewhere this
        adapter is not allowed to read.

        The application catalog is out of scope for both: it is built from
        the library alone, is byte-identical across every scenario sharing
        an application, and MUST hold the component vocabulary an answer is
        graded against. Scanning it would refuse the corpus for containing
        the very names the umbrella requires it to contain.
        """
        vocabulary = set(catalog.applications)
        for app in catalog.applications.values():
            vocabulary |= {svc for svc, _ in app.services}
        vocabulary |= {
            str(a) for a in (scenario.get("alerts") or []) if isinstance(a, str)
        }
        fragments = {
            value.strip().lower()
            for value in _string_values(
                [scenario.get(key) for key in _SCENARIO_GOLD_KEYS]
            )
            if len(value.strip()) >= _GOLD_FRAGMENT_MIN_CHARS
        }
        for fact in facts:
            if getattr(fact, "subtype", None) in {
                "ops_application", "ops_service", "ops_alert",
                # TELEMETRY-DERIVED, and out of scope for the SUBSTRING scan
                # for the same structural reason the catalog is: these facts
                # are a pure function of the published telemetry artifact and
                # never receive a scenario document, so they cannot carry
                # per-scenario gold however much their text coincides with it.
                #
                # The coincidence is real and is why the exemption has to be
                # structural rather than a widened fragment filter. A firing's
                # rendered description ("Latency in service ad in namespace
                # otel-demo is above 1500ms") shares long substrings with the
                # index's own prose description ("... the `ad` service
                # failing"), and the namespace `otel-demo` appears verbatim
                # inside scenario 1's disruptions as a target. Scanning them
                # refuses the corpus for holding the OBSERVATION the agent is
                # supposed to reason from — the umbrella's rule is "nothing
                # distinguishes the injected component from its peers", not
                # "the token is absent". Every firing in a scenario carries
                # the same attribute shape, so none distinguishes anything.
                #
                # THE EXEMPTION IS ONLY SOUND BECAUSE STRIPPED-GOLD
                # EQUIVALENCE COVERS IT: a rebuild with `disruptions`,
                # `solutions` and `description` deleted from every scenario
                # JSON must emit a byte-identical fact stream, which proves
                # non-derivation directly rather than inferring it from the
                # absence of a substring. Do not widen this set without
                # extending that test — the scan is the weaker instrument and
                # this is the one carrying the proof.
                "alert_firing", "telemetry_run",
            }:
                continue
            for attr_name, attr_value in fact.attrs.items():
                if not isinstance(attr_value, str) or not attr_value:
                    continue
                if attr_name in _SCENARIO_IDENTIFIER_ATTRS:
                    unknown = [
                        token for token in
                        (t.strip() for t in attr_value.split(","))
                        if token and token not in vocabulary
                    ]
                    if unknown:
                        raise SubstrateError(
                            f"source {self.name!r}: scenario {path} emitted "
                            f"identifier attr {attr_name!r} holding "
                            f"{unknown!r}, absent from the source-carried "
                            f"vocabulary (library application ids, their "
                            f"service names, and the index's own alerts "
                            f"array). An identifier from anywhere else could "
                            f"only have come from a field this adapter must "
                            f"not read",
                            data={"adapter": self.name, "path": str(path),
                                  "attr": attr_name, "unknown": unknown},
                            fix_hint="emit identifiers only by resolving "
                                     "them through the catalog",
                        )
                    continue
                low = attr_value.lower()
                for fragment in fragments:
                    if fragment in low:
                        raise SubstrateError(
                            f"source {self.name!r}: gold-exclusion "
                            f"violation for scenario {path}: emitted attr "
                            f"{attr_name!r} contains solution/injection/"
                            f"description text {fragment!r}",
                            data={
                                "adapter": self.name,
                                "path": str(path),
                                "attr": attr_name,
                            },
                            fix_hint="a per-scenario attr embeds gold "
                                     "content; this scenario cannot be "
                                     "ingested without leaking the answer",
                        )


# ------------------------------------------------------------- library IO


def _build_catalog(library_root: Path, *, adapter: str) -> _Catalog:
    """Parse the application library into applications, services and alert
    rule definitions.

    A PURE function of the library tree — it never receives a scenario
    document, which is what makes stripped-gold equivalence hold by
    construction rather than by inspection."""
    index_dir = library_root / "library" / "indexes" / "applications"
    role_dir = library_root / "project" / "roles" / "applications"
    for label, path in (("application index", index_dir), ("role", role_dir)):
        if not path.is_dir():
            raise SubstrateError(
                f"source {adapter!r}: library_root {library_root} has no "
                f"{label} directory at {path}",
                data={"adapter": adapter, "library_root": str(library_root)},
                fix_hint="point library_root at <clone>/scenarios/sre",
            )
    index_paths = sorted(index_dir.glob("*.json"))
    if not index_paths:
        raise SubstrateError(
            f"source {adapter!r}: {index_dir} defines zero applications",
            data={"adapter": adapter, "index_dir": str(index_dir)},
            fix_hint="re-clone the scenario repo",
        )
    managers = _load_managers(role_dir, adapter=adapter)
    install_tasks = _load_install_tasks(role_dir, adapter=adapter)

    catalog = _Catalog()
    for path in index_paths:
        doc = _load_json(path, adapter=adapter)
        application_id = str(doc.get("id") or "").strip()
        if not application_id:
            raise SubstrateError(
                f"source {adapter!r}: application index {path} has no id",
                data={"adapter": adapter, "path": str(path)},
                fix_hint="re-clone the scenario repo",
            )
        install = install_tasks.get(application_id)
        if install is None:
            raise SubstrateError(
                f"source {adapter!r}: application {application_id!r} has no "
                f"install task under {role_dir / 'tasks'} "
                f"({sorted(install_tasks)} found). The install task is the "
                f"only source-carried statement of which templates an "
                f"application deploys",
                data={"adapter": adapter, "application": application_id},
                fix_hint="re-clone the scenario repo",
            )
        manager = managers.get(install.manager_key)
        if manager is None:
            raise SubstrateError(
                f"source {adapter!r}: application {application_id!r} install "
                f"task references applications_managers."
                f"{install.manager_key}, absent from managers.yaml "
                f"({sorted(managers)})",
                data={"adapter": adapter, "application": application_id},
                fix_hint="re-clone the scenario repo",
            )
        namespace = str(
            ((manager.get("kubernetes") or {}).get("namespace")) or ""
        )
        if not namespace:
            raise SubstrateError(
                f"source {adapter!r}: application {application_id!r} manager "
                f"{install.manager_key!r} declares no kubernetes.namespace",
                data={"adapter": adapter, "application": application_id},
                fix_hint="re-clone the scenario repo",
            )
        deploy_kind, deploy_ref, deploy_version = _deploy_pin(manager)

        services = _application_services(
            role_dir, install, adapter=adapter, application_id=application_id,
        )
        rules = _application_rules(
            role_dir, install, application_id=application_id,
            namespace=namespace, adapter=adapter,
        )
        # Rules mention services the deployment artifacts do not list: the
        # otel-demo helm values override only 9 of the chart's components,
        # while the golden-signal rules name frontend, frontend-proxy and
        # flagd. A mentioned service IS source-carried evidence the component
        # exists, so it joins the vocabulary with its own origin marker.
        known = {name for name, _ in services}
        for rule in rules:
            for mentioned in rule.mentioned_services:
                if mentioned not in known:
                    known.add(mentioned)
                    services.append((mentioned, "alert_selector"))
        if not services:
            raise SubstrateError(
                f"source {adapter!r}: application {application_id!r} resolved "
                f"to ZERO components from {len(install.template_refs)} "
                f"template(s) and its install task. A componentless "
                f"application makes the benchmark's answer unit unreachable "
                f"by construction",
                data={"adapter": adapter, "application": application_id,
                      "templates": sorted(install.template_refs)},
                fix_hint="check that library_root points at "
                         "<clone>/scenarios/sre and that the install task's "
                         "templates/ references resolve under "
                         "project/roles/applications/",
            )
        services.sort()

        app = _Application(
            application_id=application_id,
            name=str(doc.get("name") or application_id),
            index=int(doc.get("index") or 0),
            description=str(doc.get("description") or ""),
            repository=str(doc.get("repository") or ""),
            platforms=tuple(str(p) for p in (doc.get("platforms") or [])),
            authors=tuple(str(a) for a in (doc.get("authors") or [])),
            namespace=namespace,
            deploy_kind=deploy_kind,
            deploy_ref=deploy_ref,
            deploy_version=deploy_version,
            services=tuple(services),
            rules=tuple(rules),
        )
        catalog.applications[application_id] = app
        for rule in rules:
            catalog.rules_by_name.setdefault(
                (application_id, rule.alert_name), []
            ).append(rule)
    return catalog


@dataclass(frozen=True)
class _InstallTask:
    application_id: str
    path: Path
    manager_key: str
    template_refs: tuple[str, ...]
    group_name: str
    doc: Any


def _load_install_tasks(
    role_dir: Path, *, adapter: str,
) -> dict[str, _InstallTask]:
    """application id -> its install task.

    The application id is not derivable from any directory name: the
    `opentelemetry-demo` application installs from `templates/helm/otel_demo`
    under `applications_managers.opentelemetry_demo`. The one correspondence
    the tree actually asserts is manager key <-> application id under
    `_` <-> `-`; anything else would be a guess."""
    tasks_dir = role_dir / "tasks"
    out: dict[str, _InstallTask] = {}
    for path in sorted(tasks_dir.glob("install_*.yaml")):
        text = path.read_text(encoding="utf-8", errors="replace")
        manager_keys = sorted(set(_MANAGER_REF.findall(text)))
        if len(manager_keys) != 1:
            # install.yaml is the dispatcher, not a per-application task.
            if not manager_keys:
                continue
            raise SubstrateError(
                f"source {adapter!r}: install task {path} references "
                f"{len(manager_keys)} application managers {manager_keys}; "
                f"one install task must describe exactly one application",
                data={"adapter": adapter, "path": str(path)},
                fix_hint="re-clone the scenario repo",
            )
        manager_key = manager_keys[0]
        refs = tuple(sorted(set(_TEMPLATE_REF.findall(text))))
        if not refs:
            raise SubstrateError(
                f"source {adapter!r}: install task {path} names no "
                f"templates/... artifact, so the application's alert "
                f"definitions and component set cannot be resolved",
                data={"adapter": adapter, "path": str(path)},
                fix_hint="re-clone the scenario repo",
            )
        try:
            doc = yaml.safe_load(text)
        except yaml.YAMLError as exc:
            raise SubstrateError(
                f"source {adapter!r}: install task {path} is not valid "
                f"YAML: {exc}",
                data={"adapter": adapter, "path": str(path)},
                fix_hint="re-clone the scenario repo",
            ) from exc
        application_id = manager_key.replace("_", "-")
        out[application_id] = _InstallTask(
            application_id=application_id,
            path=path,
            manager_key=manager_key,
            template_refs=refs,
            group_name=_common_group_name(doc),
            doc=doc,
        )
    return out


def _common_group_name(doc: Any) -> str:
    """`group_name` the install task passes to the shared common template."""
    for task in doc if isinstance(doc, list) else []:
        if not isinstance(task, dict):
            continue
        for value in task.values():
            if (isinstance(value, dict)
                    and isinstance(value.get("template"), dict)
                    and str(value["template"].get("path", "")).endswith(
                        "common/prometheusrules.j2")):
                return str((task.get("vars") or {}).get("group_name") or "")
    return ""


def _load_managers(role_dir: Path, *, adapter: str) -> dict[str, Any]:
    path = role_dir / "defaults" / "main" / "managers.yaml"
    if not path.is_file():
        raise SubstrateError(
            f"source {adapter!r}: no application managers file at {path}; "
            f"namespaces and chart pins cannot be resolved",
            data={"adapter": adapter, "path": str(path)},
            fix_hint="point library_root at <clone>/scenarios/sre",
        )
    try:
        doc = yaml.safe_load(path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        raise SubstrateError(
            f"source {adapter!r}: {path} is not valid YAML: {exc}",
            data={"adapter": adapter, "path": str(path)},
            fix_hint="re-clone the scenario repo",
        ) from exc
    managers = (doc or {}).get("applications_managers")
    if not isinstance(managers, dict) or not managers:
        raise SubstrateError(
            f"source {adapter!r}: {path} declares no applications_managers",
            data={"adapter": adapter, "path": str(path)},
            fix_hint="re-clone the scenario repo",
        )
    return managers


def _deploy_pin(manager: dict[str, Any]) -> tuple[str, str, str]:
    helm = manager.get("helm")
    if isinstance(helm, dict):
        chart = helm.get("chart") or {}
        return ("helm", str(chart.get("reference") or ""),
                str(chart.get("version") or ""))
    git = manager.get("git")
    if isinstance(git, dict):
        return ("git", str(git.get("repository") or ""),
                str(git.get("tag") or ""))
    return ("", "", "")


def _application_services(
    role_dir: Path,
    install: _InstallTask,
    *,
    adapter: str,
    application_id: str,
) -> list[tuple[str, str]]:
    """The component names the application's own deployment artifacts state.

    Two source-carried extractors, each stamped as the service's `origin`:
    `helm_values` — keys under the top-level `components:` map of the helm
    values template the install task passes to `kubernetes.core.helm`;
    `install_manifest` — `app.kubernetes.io/component` label values and `app`
    match-label values in the Kubernetes objects the install task creates.
    A third, `alert_selector`, is added by the caller from the rules."""
    found: dict[str, str] = {}
    for ref in install.template_refs:
        if not ref.startswith("templates/helm/"):
            continue
        path = role_dir / ref
        if not path.is_file():
            raise SubstrateError(
                f"source {adapter!r}: application {application_id!r} install "
                f"task names {ref}, absent at {path}",
                data={"adapter": adapter, "application": application_id,
                      "template": ref},
                fix_hint="point library_root at <clone>/scenarios/sre",
            )
        components = _helm_components(path)
        if not components:
            raise SubstrateError(
                f"source {adapter!r}: helm values template {path} declares "
                f"zero components for application {application_id!r}",
                data={"adapter": adapter, "application": application_id,
                      "template": ref},
                fix_hint="the top-level `components:` map is the "
                         "source-carried component set; an empty one means "
                         "the template changed shape",
            )
        for component in components:
            found.setdefault(component, "helm_values")
    for component in _install_manifest_components(install.doc):
        found.setdefault(component, "install_manifest")
    return sorted(found.items())


def _helm_components(path: Path) -> list[str]:
    """Keys of the top-level `components:` map in a helm values template.

    A line scan, not a YAML parse: the template interleaves `{% if %}` blocks
    with the mapping, so it is not valid YAML until Jinja renders it."""
    out: list[str] = []
    inside = False
    for line in path.read_text(encoding="utf-8").splitlines():
        if re.match(r"^components:\s*$", line):
            inside = True
            continue
        if inside:
            if re.match(r"^\S", line):
                break
            match = re.match(r"^  ([A-Za-z0-9][A-Za-z0-9._-]*):\s*$", line)
            if match:
                out.append(match.group(1))
    return out


def _install_manifest_components(doc: Any) -> list[str]:
    """`app.kubernetes.io/component` and `app` match-label values inside the
    Kubernetes objects an install task creates."""
    out: list[str] = []

    def _walk(node: Any) -> None:
        if isinstance(node, dict):
            for key, value in node.items():
                if (key in ("app.kubernetes.io/component", "app")
                        and isinstance(value, str) and value.strip()
                        and "{{" not in value):
                    out.append(value.strip())
                else:
                    _walk(value)
        elif isinstance(node, list):
            for item in node:
                _walk(item)

    _walk(doc)
    return sorted(set(out))


def _application_rules(
    role_dir: Path,
    install: _InstallTask,
    *,
    application_id: str,
    namespace: str,
    adapter: str,
) -> list[_AlertRule]:
    """Every Prometheus alerting rule the application installs.

    Keyed by (group, alert name), NOT by alert name: otel-demo defines
    HighRequestLatency and HighRequestErrorRate twice (frontend and backend
    golden-signal groups) and book-info defines HighRequestErrorRate and
    NoRequestsReceived twice (productpage and gateway groups). Collapsing
    them onto the name would silently discard half the definitions.

    The shared `common/prometheusrules.j2` is rendered per application, so
    the same key can arrive from two templates; identical rule bodies merge,
    and DIFFERING bodies raise rather than letting one silently win."""
    by_key: dict[tuple[str, str], _AlertRule] = {}
    seen_templates: dict[tuple[str, str], list[str]] = {}
    for ref in install.template_refs:
        if not ref.endswith("prometheusrules.j2"):
            continue
        path = role_dir / ref
        if not path.is_file():
            raise SubstrateError(
                f"source {adapter!r}: application {application_id!r} install "
                f"task names {ref}, absent at {path}",
                data={"adapter": adapter, "application": application_id,
                      "template": ref},
                fix_hint="point library_root at <clone>/scenarios/sre",
            )
        rules = _parse_rule_template(
            path, ref, application_id=application_id,
            manager_key=install.manager_key, namespace=namespace,
            group_name=install.group_name, adapter=adapter,
        )
        if not rules:
            raise SubstrateError(
                f"source {adapter!r}: rule template {path} parsed to ZERO "
                f"alerting rules for application {application_id!r}",
                data={"adapter": adapter, "application": application_id,
                      "template": ref},
                fix_hint="the template's spec.groups[].rules[] shape "
                         "changed; a zero-rule parse must never be read as "
                         "'this application defines no alerts'",
            )
        for rule in rules:
            key = (rule.group, rule.alert_name)
            seen_templates.setdefault(key, []).append(ref)
            existing = by_key.get(key)
            if existing is None:
                by_key[key] = rule
                continue
            if (existing.expr, existing.for_window, existing.severity,
                    existing.description) != (
                        rule.expr, rule.for_window, rule.severity,
                        rule.description):
                raise SubstrateError(
                    f"source {adapter!r}: application {application_id!r} "
                    f"defines alert {rule.alert_name!r} in group "
                    f"{rule.group!r} twice with DIFFERENT bodies "
                    f"({seen_templates[key]}); one definition would silently "
                    f"win",
                    data={"adapter": adapter, "application": application_id,
                          "group": rule.group, "alert": rule.alert_name},
                    fix_hint="disambiguate the rule groups upstream, or "
                             "extend the alert key",
                )
    return sorted(by_key.values(), key=lambda r: (r.group, r.alert_name))


def _parse_rule_template(
    path: Path,
    ref: str,
    *,
    application_id: str,
    manager_key: str,
    namespace: str,
    group_name: str,
    adapter: str,
) -> list[_AlertRule]:
    text = "\n".join(
        line for line in path.read_text(encoding="utf-8").splitlines()
        if not line.lstrip().startswith("{#")
    )
    known = {
        "application_namespace": namespace,
        f"applications_managers.{manager_key}.kubernetes.namespace": namespace,
        "group_name": group_name,
    }

    def _resolve(match: "re.Match[str]") -> str:
        expr = match.group("expr")
        if expr not in known:
            raise SubstrateError(
                f"source {adapter!r}: rule template {path} uses placeholder "
                f"[[ {expr} ]], which this adapter cannot resolve "
                f"(known: {sorted(known)})",
                data={"adapter": adapter, "template": ref,
                      "placeholder": expr},
                fix_hint="a new placeholder needs an explicit resolution "
                         "rule; substituting a blank would silently corrupt "
                         "the rule body",
            )
        value = known[expr]
        if not value:
            raise SubstrateError(
                f"source {adapter!r}: rule template {path} needs "
                f"[[ {expr} ]] for application {application_id!r}, but the "
                f"install task supplies no value for it",
                data={"adapter": adapter, "template": ref,
                      "placeholder": expr},
                fix_hint="the install task's vars: block is the only "
                         "source-carried binding for the shared template",
            )
        return value

    rendered = _PLACEHOLDER.sub(_resolve, text)
    try:
        doc = yaml.safe_load(rendered)
    except yaml.YAMLError as exc:
        raise SubstrateError(
            f"source {adapter!r}: rule template {path} is not valid YAML "
            f"after placeholder resolution: {exc}",
            data={"adapter": adapter, "template": ref},
            fix_hint="re-clone the scenario repo",
        ) from exc
    groups = ((doc or {}).get("spec") or {}).get("groups")
    if not isinstance(groups, list):
        raise SubstrateError(
            f"source {adapter!r}: rule template {path} has no "
            f"spec.groups list",
            data={"adapter": adapter, "template": ref},
            fix_hint="re-clone the scenario repo",
        )
    out: list[_AlertRule] = []
    for group in groups:
        if not isinstance(group, dict):
            continue
        group_label = str(group.get("name") or "")
        for rule in group.get("rules") or []:
            if not isinstance(rule, dict) or not rule.get("alert"):
                continue
            expr = str(rule.get("expr") or "").strip()
            positive, mentioned, kind = _rule_services(expr)
            out.append(_AlertRule(
                application_id=application_id,
                group=group_label,
                alert_name=str(rule["alert"]),
                expr=_squash(expr),
                for_window=str(rule.get("for") or ""),
                severity=str((rule.get("labels") or {}).get("severity") or ""),
                description=str(
                    (rule.get("annotations") or {}).get("description") or ""
                ),
                template_path=ref,
                positive_services=positive,
                mentioned_services=mentioned,
                selector_kind=kind,
            ))
    return out


def _rule_services(expr: str) -> tuple[tuple[str, ...], tuple[str, ...], str]:
    """(positive targets, all mentioned services, selector kind) for a rule.

    Only POSITIVE matchers (`=` and `=~`) resolve a `targets` edge — a rule
    that fires on everything EXCEPT a list of services targets no particular
    service, and saying otherwise would invert its meaning. Negative matchers
    still contribute vocabulary: naming a service to exclude is evidence the
    service exists."""
    positive: list[str] = []
    mentioned: list[str] = []
    has_negative = False
    for match in _LABEL_MATCHER.finditer(expr):
        if match.group("label") not in _SERVICE_LABELS:
            continue
        names = [
            token.strip().strip("*")
            for token in match.group("value").split("|")
        ]
        names = [n for n in names if n and re.fullmatch(r"[A-Za-z0-9._-]+", n)]
        if not names:
            continue
        mentioned.extend(names)
        if match.group("op") in {"=", "=~"}:
            positive.extend(names)
        else:
            has_negative = True
    kind = "positive" if positive else ("negative" if has_negative else "none")
    return tuple(sorted(set(positive))), tuple(sorted(set(mentioned))), kind


def _squash(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def _load_json(path: Path, *, adapter: str) -> dict[str, Any]:
    try:
        doc = json.loads(path.read_text(encoding="utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SubstrateError(
            f"source {adapter!r}: {path} is not valid JSON: {exc}",
            data={"adapter": adapter, "path": str(path)},
            fix_hint="re-clone the scenario repo",
        ) from exc
    if not isinstance(doc, dict):
        raise SubstrateError(
            f"source {adapter!r}: {path} must be a JSON object, got "
            f"{type(doc).__name__}",
            data={"adapter": adapter, "path": str(path)},
            fix_hint="re-clone the scenario repo",
        )
    return doc


def _scenario_application_id(
    scenario: dict[str, Any], path: Path, *, adapter: str,
) -> str:
    """The one application id a scenario's environment block points at.

    Measured over all 66 upstream indexes: `environment` carries exactly one
    key (`applications`), holding exactly one entry, of shape `{"id": ...}`.
    An absent, empty or multi-entry block raises — a scenario with no
    application has no topology, and a multi-application scenario is a shape
    this adapter has never seen and must not silently pick a winner from."""
    env = scenario.get("environment")
    if not isinstance(env, dict):
        raise SubstrateError(
            f"source {adapter!r}: scenario {path} has no environment object",
            data={"adapter": adapter, "path": str(path)},
            fix_hint="re-clone the scenario repo",
        )
    apps = env.get("applications")
    if not isinstance(apps, list) or not apps:
        raise SubstrateError(
            f"source {adapter!r}: scenario {path} environment.applications is "
            f"absent or empty; the scenario cannot become a componentless "
            f"node because the answer unit would be unreachable",
            data={"adapter": adapter, "path": str(path)},
            fix_hint="re-clone the scenario repo",
        )
    if len(apps) != 1:
        raise SubstrateError(
            f"source {adapter!r}: scenario {path} declares {len(apps)} "
            f"applications; all 66 upstream indexes declare exactly one, so "
            f"a multi-application scenario needs an explicit adapter change",
            data={"adapter": adapter, "path": str(path),
                  "applications": len(apps)},
            fix_hint="extend the adapter rather than picking one silently",
        )
    entry = apps[0]
    application_id = (
        str(entry.get("id") or "").strip() if isinstance(entry, dict) else ""
    )
    if not application_id:
        raise SubstrateError(
            f"source {adapter!r}: scenario {path} environment.applications[0] "
            f"carries no id",
            data={"adapter": adapter, "path": str(path)},
            fix_hint="re-clone the scenario repo",
        )
    return application_id


# --------------------------------------------------------------- telemetry


@dataclass(frozen=True)
class _Recording:
    rel_path: str
    model: str
    file_count: int
    #: artifact files, deny-listed names ALREADY removed
    artifacts: tuple[Path, ...]
    directory: Path


@dataclass(frozen=True)
class _Signal:
    kind: str
    artifact_path: str
    record_count: int
    subject: str
    exemplar: str
    window_start: str
    window_end: str
    alert_names: frozenset[str]


def _find_recordings(
    trajectories_root: Path, *, adapter: str,
) -> dict[int, list[_Recording]]:
    """Scenario number -> recorded runs.

    A directory is one recorded run when it holds a TRAJECTORY marker file or
    any observability artifact. The deny-listed files are counted into
    `file_count` and then DROPPED from `artifacts`: their presence is the
    only signal they are ever permitted to carry.

    `ground_truth.yaml` is deliberately NOT a run marker even though it is
    deny-listed. Letting the answer key decide which directories become
    `telemetry_run` nodes would make the corpus's STRUCTURE a function of the
    gold — a subtler leak than an attribute, and one no attribute scan could
    see. It is dropped from `artifacts` and contributes only to
    `file_count`."""
    recordings: dict[int, list[_Recording]] = {}
    for run_dir in sorted(
        p for p in trajectories_root.rglob("*") if p.is_dir()
    ):
        files = sorted(c for c in run_dir.iterdir() if c.is_file())
        if not any(
            c.name in TRAJECTORY_DENY_LIST
            or "observability" in c.name.lower()
            for c in files
        ):
            continue
        rel = run_dir.relative_to(trajectories_root)
        number = _scenario_number_for(rel)
        if number is None:
            raise SubstrateError(
                f"source {adapter!r}: recording {run_dir} has no scenario "
                f"number in its path (relative {rel})",
                data={"adapter": adapter, "path": str(run_dir)},
                fix_hint="lay trajectories_root out as "
                         "model/domain/scenario/run",
            )
        artifacts = tuple(
            f for f in files if f.name not in SNAPSHOT_DENY_LIST
        )
        # FAIL CLOSED. The filter above is the only thing standing between
        # the answer key and a content read, so assert the postcondition
        # rather than trusting the comprehension. A future edit that widens
        # the filter trips here instead of silently ingesting gold.
        leaked = sorted(f.name for f in artifacts if f.name in SNAPSHOT_DENY_LIST)
        if leaked:
            raise SubstrateError(
                f"source {adapter!r}: deny-listed file(s) {leaked} survived "
                f"the artifact filter in {run_dir}",
                data={"adapter": adapter, "run": str(run_dir),
                      "leaked": leaked},
                fix_hint="SNAPSHOT_DENY_LIST members are never opened for "
                         "content; they may only be counted",
            )
        recordings.setdefault(number, []).append(_Recording(
            rel_path=rel.as_posix(),
            model=rel.parts[0] if len(rel.parts) > 1 else "",
            file_count=len(files),
            artifacts=artifacts,
            directory=run_dir,
        ))
    return recordings


def _read_signals(recording: _Recording, *, adapter: str) -> list[_Signal]:
    """A bounded INDEX of one run's observability artifacts.

    Gigabytes of logs, metrics and spans are not ingested: each artifact
    yields one node carrying its kind, its relative path, a record count, the
    observed window, and one truncated exemplar. The bytes stay on disk under
    the read-only snapshot the agent already mounts."""
    signals: list[_Signal] = []
    for path in recording.artifacts:
        kind = _signal_kind(path.name)
        if kind is None:
            continue
        records = _load_records(path)
        subject = ""
        exemplar = ""
        times: list[str] = []
        alert_names: set[str] = set()
        for record in records:
            if isinstance(record, dict):
                if not subject:
                    subject = _first_str(record, _SIGNAL_SUBJECT_KEYS)
                for key in _SIGNAL_TIME_KEYS:
                    value = record.get(key)
                    if isinstance(value, str) and value:
                        times.append(value)
                name = _first_str(record, ("alertname", "alert", "name"))
                if name:
                    alert_names.add(name)
            if not exemplar:
                exemplar = json.dumps(record, sort_keys=True, default=str)
        signals.append(_Signal(
            kind=kind,
            artifact_path=path.name,
            record_count=len(records),
            subject=subject,
            exemplar=exemplar,
            window_start=min(times) if times else "",
            window_end=max(times) if times else "",
            alert_names=frozenset(alert_names),
        ))
    if not signals:
        raise SubstrateError(
            f"source {adapter!r}: recorded run {recording.rel_path} holds no "
            f"classifiable observability artifact "
            f"({[p.name for p in recording.artifacts]}); a run indexed as "
            f"zero signals would be a file count wearing a new name",
            data={"adapter": adapter, "run": recording.rel_path},
            fix_hint="artifact filenames must name their signal kind "
                     "(log / metric / trace / span / alert / event)",
        )
    return signals


def _no_subject_reason(
    signal: _Signal, service_names: set[str],
) -> Optional[str]:
    """Why this signal names no service of the application, or `None`.

    `None` means the ARTIFACT declared a subject that the application's
    topology defines — so if such a signal still lands isolated, the
    `about` edge was lost between declaration and emission, and no stamp
    forgives it. This is the whole point: the stamp is read from the
    parsed artifact, never from `bool(subject)`, which is the edge gate
    itself and would make every isolated signal exempt by construction.

    Three reasons, kept distinct because they are three different claims
    about the input:

    * `artifact_empty` — zero records in the artifact. The run recorded
      nothing here, so it says nothing about any service.
    * `no_subject_declared` — records exist and none carries any of
      `_SIGNAL_SUBJECT_KEYS`. The run observed something and did not say
      what it was about.
    * `declared_subject_unknown` — the artifact NAMED a subject the
      application does not define. A finding, not a shrug: the name is
      kept on the node as `declared_subject_service`, the run reports it,
      and a run that resolves none of a floor's worth of declared
      subjects RAISES (`_MIN_DECLARED_SUBJECTS_FOR_FLOOR`).
    """
    if signal.subject and signal.subject in service_names:
        return None
    if not signal.record_count:
        return "artifact_empty"
    if not signal.subject:
        return "no_subject_declared"
    return "declared_subject_unknown"


def _signal_kind(filename: str) -> Optional[str]:
    low = filename.lower()
    for token, kind in _SIGNAL_KINDS:
        if token in low:
            return kind
    return None


def _load_records(path: Path) -> list[Any]:
    text = path.read_text(encoding="utf-8", errors="replace")
    if path.suffix == ".jsonl":
        out: list[Any] = []
        for line in text.splitlines():
            stripped = line.strip()
            if not stripped:
                continue
            try:
                out.append(json.loads(stripped))
            except json.JSONDecodeError:
                out.append(stripped)
        return out
    try:
        doc = json.loads(text)
    except json.JSONDecodeError:
        return [line for line in text.splitlines() if line.strip()]
    if isinstance(doc, list):
        return doc
    return [doc]


def _first_str(record: dict[str, Any], keys: tuple[str, ...]) -> str:
    labels = record.get("labels")
    for key in keys:
        value = record.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
        if isinstance(labels, dict):
            nested = labels.get(key)
            if isinstance(nested, str) and nested.strip():
                return nested.strip()
    return ""


def _scenario_number_for(rel: PurePath) -> Optional[int]:
    parts = rel.parts
    candidates: list[str] = []
    if len(parts) >= 2:
        candidates.append(parts[-2])  # canonical scenario position
    candidates.append(parts[-1])  # markers directly under the scenario dir
    candidates.extend(reversed(parts[:-2]))
    for part in candidates:
        match = re.search(r"(\d+)", part)
        if match:
            return int(match.group(1))
    return None


# -------------------------------------------- published-telemetry parsing


@dataclass(frozen=True)
class _Firing:
    """One recorded alert firing — an OBSERVATION, not a rule definition.

    Keyed by (scenario, alert_name, active_at). Deliberately carries NO
    `expr` and no link to an `ops_alert`: the recorded vocabulary and the
    defined vocabulary are DISJOINT (measured: 16 observed names, 17
    definitions, empty intersection), and no revision of the ITBench tree
    closes the gap. Asserting a correspondence the data does not state
    would be inventing structure.
    """

    scenario: int
    alert_name: str
    active_at: str
    severity: str
    namespace: str
    state: str
    #: the service label the firing carried, resolvable or not
    service_label: str
    description: str
    artifact_path: str


@dataclass(frozen=True)
class _ScenarioSnapshot:
    """One published telemetry snapshot directory."""

    number: int
    rel_path: str
    directory: Path
    alert_files: tuple[Path, ...]
    objects_tsv: Optional[Path]
    events_tsv: Optional[Path]
    file_count: int


def _find_snapshots(
    telemetry_root: Path, *, adapter: str,
) -> dict[int, _ScenarioSnapshot]:
    """Scenario number -> its published telemetry snapshot.

    A directory named `Scenario-<N>` anywhere beneath the root. Deny-listed
    files are counted and never opened; `ground_truth.yaml` in particular is
    NOT a marker, so the answer key cannot decide which directories become
    nodes."""
    out: dict[int, _ScenarioSnapshot] = {}
    for directory in sorted(
        p for p in telemetry_root.rglob("Scenario-*") if p.is_dir()
    ):
        match = _SCENARIO_DIR.match(directory.name)
        if match is None:
            continue
        number = int(match.group("number"))
        if number in out:
            raise SubstrateError(
                f"source {adapter!r}: scenario {number} has two telemetry "
                f"snapshots ({out[number].directory} and {directory}); the "
                f"published layout is one snapshot per scenario, so a second "
                f"one would silently double every telemetry count",
                data={"adapter": adapter, "scenario": number},
                fix_hint="point telemetry_root at a single release version",
            )
        files = [p for p in directory.rglob("*") if p.is_file()]
        alerts = sorted(
            p for p in files
            if p.parent.name == "alerts"
            and p.suffix == ".json"
            and p.name.startswith(_ALERT_FILE_PREFIXES)
        )
        denied = [p for p in files if p.name in SNAPSHOT_DENY_LIST]
        objects = directory / "k8s_objects_raw.tsv"
        events = directory / "k8s_events_raw.tsv"
        if not alerts and not objects.is_file() and not events.is_file():
            continue
        # FAIL CLOSED: nothing deny-listed may reach the artifact set.
        leaked = sorted(p.name for p in alerts if p.name in SNAPSHOT_DENY_LIST)
        if leaked:
            raise SubstrateError(
                f"source {adapter!r}: deny-listed file(s) {leaked} entered "
                f"the alert artifact set for scenario {number}",
                data={"adapter": adapter, "scenario": number},
                fix_hint="SNAPSHOT_DENY_LIST members are never opened",
            )
        out[number] = _ScenarioSnapshot(
            number=number,
            rel_path=directory.relative_to(telemetry_root).as_posix(),
            directory=directory,
            alert_files=tuple(alerts),
            objects_tsv=objects if objects.is_file() else None,
            events_tsv=events if events.is_file() else None,
            file_count=len(files) - len(denied),
        )
    return out


def _iter_k8s_objects(path: Path, *, adapter: str) -> Iterator[dict[str, Any]]:
    """Yield the Kubernetes object inside each row's `Body` column.

    The published TSVs are UNTRANSFORMED (`_raw` == `transform=False`
    upstream): 17 OTel log columns with the object as JSON inside `Body`,
    CSV-quoted within the tab-delimited file. A `split('\\t')` corrupts every
    row whose object contains a tab or a quote, so this uses a csv reader and
    VERIFIES the header rather than trusting a described column set."""
    import csv

    csv.field_size_limit(_CSV_FIELD_LIMIT)
    with path.open(newline="", encoding="utf-8", errors="replace") as handle:
        reader = csv.DictReader(handle, delimiter="\t", quotechar='"')
        header = tuple(reader.fieldnames or ())
        if header != _K8S_TSV_HEADER:
            raise SubstrateError(
                f"source {adapter!r}: {path} has header {header!r}, not the "
                f"17-column OTel export this parser was written against "
                f"({_K8S_TSV_HEADER!r}). A changed export shape must be an "
                f"explicit adapter change, never a silent mis-parse",
                data={"adapter": adapter, "path": str(path),
                      "header": list(header)},
                fix_hint="re-download the release, or extend the parser for "
                         "the new column set",
            )
        for row in reader:
            body = row.get("Body") or ""
            if not body:
                continue
            try:
                obj = json.loads(body)
            except json.JSONDecodeError:
                continue
            if not isinstance(obj, dict):
                continue
            # k8s_events_raw.tsv nests one level deeper.
            if "object" in obj and "kind" not in obj:
                inner = obj.get("object")
                if isinstance(inner, dict):
                    obj = inner
            yield obj


#: csv field cap for the object dumps (a single Body can exceed the default)
_CSV_FIELD_LIMIT = 1 << 30


def _workload_roster(
    snapshots: dict[int, _ScenarioSnapshot], *, adapter: str,
) -> tuple[dict[str, set[str]], dict[str, list[str]], dict[str, int]]:
    """(namespace -> workload names, per-scenario deviations, object counts).

    THE ROSTER IS THE UNION ACROSS EVERY SCENARIO, and that is a gold-boundary
    requirement, not an optimisation. A roster taken per-scenario would encode
    that scenario's fault: a scenario whose injected Deployment was deleted
    would carry a SMALLER roster than its peers, and the umbrella's rule for
    this vertical is 'nothing distinguishes the injected component from its
    peers'. Taking the union makes every `ops_service` node byte-identical no
    matter which scenario is being read.

    Measured on the release: all 35 snapshots yield exactly the same 21
    workloads for `otel-demo`, so the union costs nothing today — but a future
    release where they differ must be REPORTED, not silently unioned away.
    Deviations are returned and land in `last_run_report`."""
    roster: dict[str, set[str]] = {}
    per_scenario: dict[int, dict[str, set[str]]] = {}
    counts: dict[str, int] = {}
    for number, snap in sorted(snapshots.items()):
        if snap.objects_tsv is None:
            continue
        local: dict[str, set[str]] = {}
        for obj in _iter_k8s_objects(snap.objects_tsv, adapter=adapter):
            if obj.get("kind") not in _WORKLOAD_KINDS:
                continue
            meta = obj.get("metadata") or {}
            namespace = str(meta.get("namespace") or "")
            name = str(meta.get("name") or "")
            if not namespace or not name:
                continue
            local.setdefault(namespace, set()).add(name)
            counts[namespace] = counts.get(namespace, 0) + 1
        per_scenario[number] = local
        for namespace, names in local.items():
            roster.setdefault(namespace, set()).update(names)

    deviations: dict[str, list[str]] = {}
    for number, local in per_scenario.items():
        for namespace, names in roster.items():
            if namespace not in local:
                continue
            missing = sorted(names - local[namespace])
            if missing:
                deviations[f"{number}:{namespace}"] = missing
    return roster, deviations, counts


def _read_firings(
    snap: _ScenarioSnapshot, *, adapter: str,
) -> list[_Firing]:
    """Every distinct recorded firing in one snapshot.

    Both published filename patterns are read. Deduplicated on
    (alert_name, active_at) — the alert directory is a ~1/min poll, so one
    firing appears in many files and counting files would inflate the
    corpus by two orders of magnitude."""
    seen: dict[tuple[str, str], _Firing] = {}
    for path in snap.alert_files:
        try:
            doc = json.loads(path.read_text(encoding="utf-8", errors="replace"))
        except json.JSONDecodeError as exc:
            raise SubstrateError(
                f"source {adapter!r}: alert artifact {path} is not valid "
                f"JSON: {exc}",
                data={"adapter": adapter, "path": str(path)},
                fix_hint="re-download the release",
            ) from exc
        if isinstance(doc, list):
            entries = doc
        elif isinstance(doc, dict):
            entries = ((doc.get("data") or {}).get("alerts")) or []
        else:
            raise SubstrateError(
                f"source {adapter!r}: alert artifact {path} is a "
                f"{type(doc).__name__}; the two published shapes are a LIST "
                f"and {{'data': {{'alerts': [...]}}}}",
                data={"adapter": adapter, "path": str(path)},
                fix_hint="a third shape needs an explicit adapter change",
            )
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            labels = entry.get("labels") or {}
            annotations = entry.get("annotations") or {}
            name = str(labels.get("alertname") or "").strip()
            if not name:
                continue
            active_at = str(entry.get("activeAt") or "")
            key = (name, active_at)
            if key in seen:
                continue
            seen[key] = _Firing(
                scenario=snap.number,
                alert_name=name,
                active_at=active_at,
                severity=str(labels.get("severity") or ""),
                namespace=str(labels.get("namespace") or ""),
                state=str(entry.get("state") or ""),
                service_label=next(
                    (
                        str(labels[k]).strip()
                        for k in _FIRING_SERVICE_LABELS
                        if str(labels.get(k) or "").strip()
                    ),
                    "",
                ),
                description=str(annotations.get("description") or ""),
                artifact_path=path.name,
            )
    return sorted(seen.values(), key=lambda f: (f.alert_name, f.active_at))


def _apply_workload_roster(
    catalog: _Catalog,
    roster: dict[str, set[str]],
    *,
    report: dict[str, Any],
    adapter: str,
) -> _Catalog:
    """Replace the helm-values reconstruction with the observed roster.

    WHY THE RECONSTRUCTION IS NOT SIMPLY DELETED. For every application whose
    namespace the telemetry covers, the k8s workload roster is strictly
    better and wins outright: measured, `otel-demo` yields 21 workloads
    against the reconstruction's 12, adding `cart`, `checkout`, `currency`,
    `email`, `otel-collector`, `payment`, `postgresql`, `quote` and
    `shipping` — which closes the gap this PACT recorded as known and
    unclosed (the helm values template overrides only 9 of the chart's
    components and the chart is an OCI artifact outside the clone).

    But `book-info` has NO published telemetry: ITBench-Lite's 35 SRE
    snapshots are all `opentelemetry-demo`, and 5 upstream scenarios deploy
    `book-info`. For those the reconstruction is the ONLY source-carried
    statement of the component set, and deleting it would take a real
    application's topology to zero — which the source already refuses. So the
    reconstruction is RETAINED as the fallback and is superseded per
    application, never globally. Each service records which one named it via
    `origin`, so the provenance stays auditable per node."""
    if not roster:
        return catalog
    for app_id, app in list(catalog.applications.items()):
        observed = roster.get(app.namespace)
        if not observed:
            report["roster_source"][app_id] = "helm_reconstruction"
            continue
        # The alert rules' own positive matchers are source-carried evidence
        # too; a rule targeting a service the dump does not show is a finding
        # worth keeping rather than dropping.
        from_rules = {
            svc for rule in app.rules for svc in rule.mentioned_services
        }
        services = sorted(
            {(name, "k8s_workload") for name in observed}
            | {(name, "alert_selector") for name in from_rules - observed}
        )
        merged: dict[str, str] = {}
        for name, origin in services:
            merged.setdefault(name, origin)
        catalog.applications[app_id] = _Application(
            **{
                **{f: getattr(app, f) for f in app.__dataclass_fields__},
                "services": tuple(sorted(merged.items())),
            }
        )
        report["roster_source"][app_id] = "k8s_workload"
    return catalog


def _application_for_namespaces(
    catalog: _Catalog, snap: _ScenarioSnapshot, *, adapter: str,
) -> Optional[_Application]:
    """The application a snapshot's own telemetry names, by namespace.

    Used for telemetry the scenario index does not define (measured on the
    release: Scenario-9). Resolving from the firing's `namespace` label keeps
    `no_target_reason` truthful for those snapshots; the alternative — no
    application at all — would make every firing read as
    `service_outside_topology` regardless of the topology."""
    namespaces = {
        firing.namespace
        for firing in _read_firings(snap, adapter=adapter)
        if firing.namespace
    }
    matches = [
        app for app in catalog.applications.values()
        if app.namespace in namespaces
    ]
    if len(matches) == 1:
        return matches[0]
    # Ambiguous or absent: return None rather than guessing. The firings then
    # carry `application_unresolved`, which says what is actually true.
    return None


def _no_target_reason(
    firing: _Firing, service_names: set[str],
) -> Optional[str]:
    """Why this firing has no `targets` edge, or `None`.

    Read from the PARSED FIRING, never from whether the edge came out.
    `None` means the firing named a service the topology defines — so an
    untargeted firing carrying no stamp is a lost edge, and the probe fires.
    Same discipline as `no_subject_reason`; the predecessor defect this
    avoids is a stamp computed as `bool(service_label)`, which is the edge
    gate itself and would exempt every untargeted firing by construction.

    Two reasons, kept distinct because they are two different upstream
    statements (measured: 272 and 484 of 1,146 firings respectively):

    * `no_service_label` — the firing carries no `service_name`/`service`
      label at all. Cluster-scoped alerts (Watchdog, KubeSchedulerDown) are
      about no service by construction.
    * `service_outside_topology` — it NAMED a service that is not a workload
      of the application. Overwhelmingly monitoring infrastructure
      (`prometheus-kube-prometheus-kubelet` and friends), which is real and
      is simply not part of the application under test.
    """
    if firing.service_label and firing.service_label in service_names:
        return None
    if not firing.service_label:
        return "no_service_label"
    if not service_names:
        # THIRD reason, and it exists because conflating it with the second
        # was a real defect. With no application resolved there IS no
        # topology to be outside of, so `service_outside_topology` would be
        # a claim about the topology made without consulting one.
        return "application_unresolved"
    return "service_outside_topology"


def _as_str(value: Any) -> Optional[str]:
    return str(value) if value is not None else None


def _string_values(obj: Any) -> list[str]:
    out: list[str] = []
    if isinstance(obj, str):
        out.append(obj)
    elif isinstance(obj, dict):
        for key, value in obj.items():
            out.append(str(key))
            out.extend(_string_values(value))
    elif isinstance(obj, (list, tuple)):
        for item in obj:
            out.extend(_string_values(item))
    return out
