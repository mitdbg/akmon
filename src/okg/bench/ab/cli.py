"""okg-bench — one CLI for the Claude Code +/- OKG A/B experiment.

Verbs:
    list                     benchmarks, arms, and (with --benchmark) subsets
    fetch    --benchmark B   download + pin a dataset (network; run once)
    run      --benchmark B --arm A [--pilot] [--resume RUN_ID] ...
    report   --run RUN_ID [--charts RUN_ID...]
    compare  --baseline RUN_ID --against RUN_ID   (paired; refuses non-zero)
    corpus   build --instance ID ... | corpus audit --corpus corpus.json

Default state roots (outside any git tree — task workdirs are full external
checkouts):
    data:  ~/.local/share/okg-bench/data    (datasets, mirrors, pins)
    runs:  ~/.local/share/okg-bench/runs
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from .adapters import adapter_names, get_adapter
from .arms import arm_names
from .compare import (
    DEFAULT_CONFIDENCE,
    DEFAULT_MIN_COVERAGE,
    DEFAULT_MIN_PAIRS,
    DEFAULT_RESAMPLES,
    DEFAULT_SEED,
)
from .errors import AbBenchError


def _default_root(kind: str) -> Path:
    base = os.environ.get("XDG_DATA_HOME", str(Path.home() / ".local" / "share"))
    return Path(base) / "okg-bench" / kind


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="okg-bench",
        description="Claude Code +/- OKG A/B benchmarking framework",
    )
    parser.add_argument(
        "--data-root", type=Path, default=_default_root("data"),
        help="datasets/mirrors/pins root (default: %(default)s)",
    )
    parser.add_argument(
        "--runs-root", type=Path, default=_default_root("runs"),
        help="run directories root (default: %(default)s)",
    )
    sub = parser.add_subparsers(dest="verb", required=True)

    p_list = sub.add_parser("list", help="list benchmarks, arms, subsets")
    p_list.add_argument(
        "--corpora", action="store_true",
        help="print the benchmark -> OKG deployment definition mapping",
    )
    p_list.add_argument("--benchmark", help="show subset counts for one benchmark")
    p_list.add_argument("--subset", help="report one subset's instance count")
    p_list.add_argument(
        "--offline-fixture", type=Path,
        help="load instances from a local fixture instead of the cache",
    )

    p_fetch = sub.add_parser("fetch", help="download + pin a dataset")
    p_fetch.add_argument("--benchmark", required=True, choices=adapter_names())

    p_run = sub.add_parser("run", help="run one (benchmark, arm)")
    p_run.add_argument("--benchmark", required=True, choices=adapter_names())
    p_run.add_argument("--arm", required=True, choices=arm_names())
    p_run.add_argument("--model", default="sonnet",
                       help="model for claude arms (default: %(default)s)")
    p_run.add_argument("--pilot", action="store_true",
                       help="run the adapter's pilot subset")
    p_run.add_argument("--limit", type=int, help="cap the task count")
    p_run.add_argument("--subset", help="named subset (e.g. regression, repo:django/django)")
    p_run.add_argument("--resume", metavar="RUN_ID",
                       help="resume an existing run (manifest must match)")
    p_run.add_argument("--offline-fixture", type=Path,
                       help="load instances from a local fixture (tests/pilots)")
    p_run.add_argument("--task-timeout", type=float, default=1800.0,
                       help="per-task wall-clock budget in seconds (default: %(default)s)")
    p_run.add_argument("--permission-mode", default="bypassPermissions",
                       help="claude permission mode (default: %(default)s)")
    p_run.add_argument("--native-memory-mode", default="shared-workdir",
                       choices=("shared-workdir", "carry-forward"))
    p_run.add_argument("--corpus-config", type=Path,
                       help="corpus.json from `okg-bench corpus build` (okg arm)")
    p_run.add_argument("--no-workdir", action="store_true",
                       help="skip repo checkout provisioning (echo-arm testing)")
    p_run.add_argument("--no-score", action="store_true",
                       help="skip scoring (score later with `report`)")

    p_report = sub.add_parser("report", help="summarize a run / render charts")
    p_report.add_argument("--run", required=True, metavar="RUN_ID")
    p_report.add_argument("--charts", nargs="*", metavar="RUN_ID",
                          help="additional runs to overlay in the money charts")

    p_cmp = sub.add_parser(
        "compare",
        help="paired comparison of two runs (refuses when incomparable)",
        description=(
            "Compare two arms on the same instances. The unit is the "
            "per-instance difference; the delta carries a seeded bootstrap "
            "interval; an interval containing zero is INSUFFICIENT EVIDENCE. "
            "An incomparable pair of runs is REFUSED naming the field, and a "
            "refusal exits non-zero so a script cannot read it as a result."
        ),
    )
    p_cmp.add_argument("--baseline", required=True, metavar="RUN_ID")
    p_cmp.add_argument("--against", required=True, metavar="RUN_ID")
    p_cmp.add_argument(
        "--declared-corpus", type=Path, action="append", metavar="CORPUS_JSON",
        help=(
            "repeatable; corpus.json holding the generation currently DECLARED "
            "for this benchmark. An okg-arm run that read a different "
            "generation is refused as stale. REQUIRED for every okg-arm run in "
            "the comparison: there is no default, because the run's own "
            "corpus.json is where it recorded its generation from and reading "
            "it back would compare a value to itself."
        ),
    )
    p_cmp.add_argument(
        "--min-pairs", type=int, default=DEFAULT_MIN_PAIRS,
        help="refuse below this many paired instances (default: %(default)s)",
    )
    p_cmp.add_argument(
        "--min-coverage", type=float, default=DEFAULT_MIN_COVERAGE,
        help=(
            "refuse below this FRACTION of the ordered instances (default: "
            "%(default)s). --min-pairs is absolute and cannot see this: 10 of "
            "10 and 10 of 500 both clear it, and only the first is the "
            "comparison that was ordered"
        ),
    )
    p_cmp.add_argument(
        "--seed", type=int, default=DEFAULT_SEED,
        help="bootstrap seed, recorded with the verdict (default: %(default)s)",
    )
    p_cmp.add_argument(
        "--resamples", type=int, default=DEFAULT_RESAMPLES,
        help="bootstrap resamples (default: %(default)s)",
    )
    p_cmp.add_argument(
        "--confidence", type=float, default=DEFAULT_CONFIDENCE,
        help="interval coverage (default: %(default)s)",
    )
    p_cmp.add_argument(
        "--chart", type=Path, metavar="PNG",
        help="write the sorted per-instance paired-delta chart here",
    )
    p_cmp.add_argument(
        "--json", action="store_true", help="print the comparison as JSON",
    )

    p_corpus = sub.add_parser("corpus", help="corpus builder (okg arm input)")
    corpus_sub = p_corpus.add_subparsers(dest="corpus_verb", required=True)
    p_cb = corpus_sub.add_parser("build", help="build one instance's corpus")
    p_cb.add_argument("--benchmark", default="swebench-verified",
                      choices=adapter_names())
    p_cb.add_argument("--instance", required=True, metavar="INSTANCE_ID")
    p_cb.add_argument("--dsn", required=True,
                      help="Postgres DSN for the corpus deployment (dedicated DB)")
    p_cb.add_argument("--corpus-root", type=Path, required=True,
                      help="fresh directory for checkout/fixtures/deployment")
    p_cb.add_argument("--deployment-name",
                      help="default: bench-<slugged instance id>")
    p_cb.add_argument("--mcp-port", type=int)
    p_cb.add_argument("--offline-fixture", type=Path,
                      help="instance source fixture (else the cached dataset)")
    p_ca = corpus_sub.add_parser("audit", help="re-run the leakage audit")
    p_ca.add_argument("--corpus", type=Path, required=True, metavar="CORPUS_JSON")
    p_health = corpus_sub.add_parser(
        "audit-health",
        help="report what a corpus contains and whether it meets its contract",
    )
    p_health.add_argument("--corpus", type=Path, action="append", required=True,
                          metavar="CORPUS_JSON",
                          help="repeatable; audits each corpus in turn")
    p_cti = corpus_sub.add_parser(
        "build-cti", help="build the CTI snapshot corpus (ctibench)"
    )
    p_cti.add_argument("--snapshot-dir", type=Path, required=True,
                       help="frozen NVD/CWE/ATT&CK dump directory")
    p_cti.add_argument("--snapshot-date", required=True,
                       help="operator-stated freeze date (ISO), recorded verbatim")
    p_cti.add_argument("--dsn", required=True)
    p_cti.add_argument("--corpus-root", type=Path, required=True)
    p_cti.add_argument("--deployment-name", default="bench-cti")
    p_dom = corpus_sub.add_parser(
        # Names the verticals this verb actually builds, read from the
        # registry rather than hand-listed: the hand-listed form still
        # advertised doc-corpus (which no benchmark maps to) and omitted
        # four that do.
        "build-domain",
        help="build one domain corpus (see `okg-bench list --corpora`)",
    )
    p_dom.add_argument("--vertical", required=True,
                       help="vertical name, or a benchmark name to resolve it")
    p_dom.add_argument("--snapshot-dir", type=Path, required=True)
    p_dom.add_argument("--snapshot-date", required=True,
                       help="operator-stated freeze date (ISO), recorded verbatim")
    p_dom.add_argument("--dsn", required=True)
    p_dom.add_argument("--corpus-root", type=Path, required=True)
    p_dom.add_argument("--deployment-name", default=None,
                       help="default: bench-<vertical>")
    p_dom.add_argument(
        "--supersedes", type=Path, default=None,
        help=(
            "corpus.json of the dated snapshot this build supersedes "
            "(doc-corpus). The build FAILS if it cannot relate itself to "
            "that predecessor."
        ),
    )
    p_dom.add_argument(
        "--extra-params", type=Path, default=None,
        help=(
            "JSON file of {source_name: {param: value}} merged into each "
            "source's params. For inputs a source needs but which must NOT "
            "live in the snapshot: phantomwiki's KB path and generation "
            "triple, for instance, because the snapshot is MOUNTED READ-ONLY "
            "INTO THE AGENT WORKSPACE and a Prolog fact dump sitting there "
            "would hand every arm the structure the OKG arm is supposed to "
            "supply. The source refuses rather than defaulting when a "
            "required param is absent."
        ),
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return _dispatch(args)
    except AbBenchError as exc:
        print(f"okg-bench: error: {exc}", file=sys.stderr)
        return 2


def _dispatch(args: argparse.Namespace) -> int:
    if args.verb == "list":
        return _cmd_list(args)
    if args.verb == "fetch":
        adapter = get_adapter(args.benchmark)
        ref = adapter.fetch(data_root=args.data_root)
        print(f"fetched {ref}")
        return 0
    if args.verb == "run":
        return _cmd_run(args)
    if args.verb == "report":
        return _cmd_report(args)
    if args.verb == "compare":
        return _cmd_compare(args)
    if args.verb == "corpus":
        return _cmd_corpus(args)
    raise AbBenchError(f"unknown verb {args.verb!r}")


def _cmd_list(args: argparse.Namespace) -> int:
    if getattr(args, "corpora", False):
        from .corpus_domain import BENCHMARK_CORPORA, VERTICALS, NoCorpus

        print("benchmark -> okg-arm corpus definition:")
        for name in adapter_names():
            corpus = BENCHMARK_CORPORA.get(name)
            if corpus is None:
                print(f"  {name}: UNRECORDED (add it to BENCHMARK_CORPORA)")
            elif isinstance(corpus, NoCorpus):
                print(f"  {name}: none — {corpus.reason}")
            else:
                spec = VERTICALS.get(corpus)
                sources = (
                    ", ".join(s.name for s in spec.sources) if spec
                    else "git_history, git_files, github-issues, agent-sessions"
                )
                modules = ", ".join(spec.modules) if spec else "workspace-graph + forge"
                print(f"  {name}: {corpus} [modules: {modules}] [sources: {sources}]")
                if spec and spec.notes:
                    print(f"      note: {spec.notes}")
        return 0
    if not args.benchmark:
        print("benchmarks:")
        for name in adapter_names():
            print(f"  {name}")
        print("arms:")
        for name in arm_names():
            print(f"  {name}")
        return 0
    adapter = get_adapter(args.benchmark)
    instances = adapter.load_instances(
        data_root=args.data_root, offline_fixture=args.offline_fixture
    )
    if args.subset:
        picked = adapter.subset(instances, args.subset)
        print(
            f"{args.benchmark} subset {args.subset!r}: {len(picked)} of "
            f"{len(instances)} instances"
        )
        return 0
    print(f"{args.benchmark}: {len(instances)} instances")
    repos: dict[str, int] = {}
    for inst in instances:
        repos[inst.repo or "<none>"] = repos.get(inst.repo or "<none>", 0) + 1
    for repo, count in sorted(repos.items(), key=lambda kv: -kv[1]):
        print(f"  {repo}: {count}")
    return 0


def _cmd_run(args: argparse.Namespace) -> int:
    from .runner import RunConfig, execute

    cfg = RunConfig(
        benchmark=args.benchmark,
        arm=args.arm,
        model=args.model,
        runs_root=args.runs_root,
        data_root=args.data_root,
        pilot=args.pilot,
        limit=args.limit,
        subset=args.subset,
        resume_run_id=args.resume,
        offline_fixture=args.offline_fixture,
        task_timeout_s=args.task_timeout,
        permission_mode=args.permission_mode,
        native_memory_mode=args.native_memory_mode,
        corpus_config=args.corpus_config,
        no_workdir=args.no_workdir,
        score=not args.no_score,
    )
    run_dir = execute(cfg)
    from .report import summarize

    summary = summarize(run_dir)
    print(f"summary: {summary}")
    return 0


def _cmd_report(args: argparse.Namespace) -> int:
    from .report import render_charts, summarize

    run_dir = args.runs_root / args.run
    summary = summarize(run_dir)
    for key, value in summary.items():
        print(f"{key}: {value}")
    if args.charts is not None:
        dirs = [run_dir] + [args.runs_root / r for r in args.charts]
        written = render_charts(dirs, run_dir / "charts")
        for path in written:
            print(f"chart: {path}")
    return 0


def _cmd_compare(args: argparse.Namespace) -> int:
    """Print the paired comparison, or refuse.

    A refusal propagates as ComparisonRefused and `main` returns 2. That
    non-zero exit is load-bearing: a caller that pipes this into a decision
    must not be able to read "these runs are not comparable" as "no
    difference".
    """
    import json as _json

    from .compare import compare, render_paired_delta_chart

    comparison = compare(
        args.runs_root / args.baseline,
        args.runs_root / args.against,
        declared_corpora=args.declared_corpus,
        min_pairs=args.min_pairs,
        min_coverage=args.min_coverage,
        seed=args.seed,
        resamples=args.resamples,
        confidence=args.confidence,
    )
    if args.json:
        print(_json.dumps(comparison.as_dict(), indent=2, sort_keys=True))
    else:
        print(comparison.render())
    if args.chart is not None:
        path = render_paired_delta_chart(comparison, args.chart)
        print(f"chart: {path}")
    return 0


def _cmd_corpus(args: argparse.Namespace) -> int:
    if args.corpus_verb == "build":
        return _cmd_corpus_build(args)
    if args.corpus_verb == "audit":
        return _cmd_corpus_audit(args)
    if args.corpus_verb == "build-cti":
        return _cmd_corpus_build_cti(args)
    if args.corpus_verb == "build-domain":
        return _cmd_corpus_build_domain(args)
    if args.corpus_verb == "audit-health":
        return _cmd_corpus_audit_health(args)
    raise AbBenchError(f"unknown corpus verb {args.corpus_verb!r}")


def _resolve_vertical(cfg: dict) -> tuple[str | None, object | None]:
    """Resolve a corpus.json to its vertical spec.

    Corpora built before the spec-driven builder carry no `vertical` key, so
    fall back to the deployment name (`bench-cti` -> `cti`). An unresolvable
    corpus returns (None, None) and the caller must treat that as a
    FAILURE, never as an exemption."""
    from .corpus_domain import VERTICALS

    name = cfg.get("vertical")
    if name and name in VERTICALS:
        return name, VERTICALS[name]
    deployment = str(cfg.get("deployment_name") or "")
    inferred = deployment[len("bench-"):] if deployment.startswith("bench-") else ""
    if inferred in VERTICALS:
        return inferred, VERTICALS[inferred]
    # repo-history is per-INSTANCE, not per-vertical: `okg-bench corpus build`
    # names its deployments after the SWE-bench instance, so there is no fixed
    # suffix to match and no `vertical` key is written. Discriminate on the
    # SHAPE the per-instance builder produces instead of on a name prefix —
    # base_commit + gold_pr_number + cutoff are written by that path and by no
    # other. Without this every SWE-bench instance audits as UNCONTRACTED,
    # which is exactly how the psf__requests-2317 corpus went ungated.
    if {"base_commit", "gold_pr_number", "cutoff"} <= set(cfg):
        return "repo-history", VERTICALS["repo-history"]
    return (name or deployment or None), None


def _cmd_corpus_audit_health(args: argparse.Namespace) -> int:
    """The single command that answers 'is this corpus real?'."""
    from .contract import SATISFYING_PROVENANCE
    from .contract import evaluate as evaluate_contract
    from .model import load_json

    from .corpus_domain import VERTICALS

    failures = 0
    covered: set[str] = set()
    for path in args.corpus:
        cfg = load_json(path)
        vertical_name, vertical = _resolve_vertical(cfg)
        if vertical is not None:
            covered.add(vertical_name)
        label = vertical_name or cfg.get("deployment_name") or str(path)
        print("=" * 72)
        print(f"{label}  ({cfg.get('generation_id')})")
        if vertical is None:
            # UNCONTRACTED, and therefore FAILING. A corpus whose contract
            # cannot be resolved must never be skipped: the first version of
            # this verb quietly excluded such corpora from the failure count,
            # so a CTI corpus already proven to fail four clauses was
            # reported as merely "no contract declared". Absence is not
            # agreement — it is the loudest kind of unknown.
            from .contract import CorpusContract

            report = evaluate_contract(
                dsn=cfg["dsn"], generation_id=cfg["generation_id"],
                contract=CorpusContract(),
                # "undeclared", never a default of "real": a corpus.json that
                # did not record its provenance has not earned the
                # released-artifact claim. (This branch fails regardless --
                # it has no contract to meet -- so the value is reported, not
                # relied on for the verdict.)
                provenance=cfg.get("provenance", "undeclared"),
            )
            print(
                f"nodes={report.total_nodes} edges={report.total_edges} "
                f"connected={report.connected_nodes} "
                f"({100.0 * report.connected_fraction():.1f}%)"
            )
            print(
                "  [FAIL] contract: UNCONTRACTED — no vertical contract "
                f"resolves for this corpus (vertical={cfg.get('vertical')!r}, "
                f"deployment={cfg.get('deployment_name')!r}); it cannot be "
                "reported as healthy"
            )
            failures += 1
            continue
        report = evaluate_contract(
            dsn=cfg["dsn"], generation_id=cfg["generation_id"],
            contract=vertical.contract,
            provenance=cfg.get("provenance", "undeclared"),
        )
        print(report.render())
        # SATISFYING_PROVENANCE, not a local `!= "real"`. The open-coded form
        # counted phantomwiki -- every clause [ok], provenance `generated` by
        # design -- as an unmet contract, and it did so SILENTLY: the total
        # said 2 while exactly one [FAIL] line was printed. A count the
        # printed lines do not explain is worse than a wrong count.
        if report.provenance not in SATISFYING_PROVENANCE:
            print(
                f"  [FAIL] provenance: {report.provenance!r} — a corpus is "
                "reported as meeting its contract only when it was built over "
                f"{' or '.join(SATISFYING_PROVENANCE)} inputs"
            )
            failures += 1
        elif not report.ok:
            failures += 1
    # A vertical with no corpus.json is INVISIBLE to a path-driven audit, and
    # invisibility reads as health. That is not hypothetical: a build whose
    # contract is unmet raises before corpus.json is written, so the corpora
    # most worth reporting are exactly the ones that leave nothing to audit.
    # Enumerate the registry and name what was not covered.
    uncovered = sorted(set(VERTICALS) - covered)
    if uncovered:
        print("=" * 72)
        for name in uncovered:
            print(
                f"{name}\n  [FAIL] NOT BUILT — no corpus.json was audited for "
                "this vertical. Either it was never built, or its build was "
                "REFUSED before writing one (an unmet contract raises first). "
                "It cannot be reported as healthy on the strength of its "
                "absence."
            )
        failures += len(uncovered)
    print("=" * 72)
    if failures:
        print(f"{failures} corpus/corpora do not meet their contract")
    else:
        print(f"all {len(VERTICALS)} verticals meet their contract")
    return 1 if failures else 0


def _cmd_corpus_build_domain(args: argparse.Namespace) -> int:
    from .corpus_domain import (
        VERTICALS,
        DomainCorpusSpec,
        NoCorpus,
        build,
        corpus_for_benchmark,
    )

    vertical = args.vertical
    if vertical not in VERTICALS:
        # Accept a benchmark name and resolve it, so operators can say
        # `--vertical finauditing` without knowing the vertical's name.
        resolved = corpus_for_benchmark(vertical)
        if isinstance(resolved, NoCorpus):
            raise AbBenchError(
                f"benchmark {vertical!r} has no okg-arm corpus: {resolved.reason}"
            )
        vertical = resolved
    if vertical not in VERTICALS:
        raise AbBenchError(
            f"{args.vertical!r} resolves to {vertical!r}, which is built by "
            "`okg-bench corpus build` (repo history), not build-domain"
        )
    corpus_json = build(
        DomainCorpusSpec(
            vertical=vertical,
            snapshot_dir=args.snapshot_dir.resolve(),
            snapshot_date=args.snapshot_date,
            deployment_name=args.deployment_name or f"bench-{vertical}",
            dsn=args.dsn,
            corpus_root=args.corpus_root.resolve(),
            supersedes=(
                args.supersedes.resolve()
                if getattr(args, "supersedes", None) is not None
                else None
            ),
            extra_params=_load_extra_params(
                getattr(args, "extra_params", None)
            ),
        )
    )
    print(f"{vertical} corpus built: {corpus_json}")
    return 0


def _load_extra_params(path: Path | None) -> dict[str, dict[str, object]]:
    """Read `--extra-params`, refusing a shape that would silently do nothing.

    A file whose top level is not {source: {param: value}} would merge as an
    empty dict and the source would then fail with "missing required
    parameter" — pointing at the source rather than at the malformed file the
    operator actually wrote. Refuse here, where the cause is visible.
    """
    if path is None:
        return {}
    from .model import load_json

    raw = load_json(path)
    if not isinstance(raw, dict) or not all(
        isinstance(v, dict) for v in raw.values()
    ):
        raise AbBenchError(
            f"--extra-params {path} must be an object of "
            "{source_name: {param: value}}; got "
            f"{type(raw).__name__} with values "
            f"{sorted({type(v).__name__ for v in (raw.values() if isinstance(raw, dict) else [])})}"
        )
    return raw


def _cmd_corpus_build_cti(args: argparse.Namespace) -> int:
    from .corpus_cti import CtiCorpusSpec, build

    corpus_json = build(
        CtiCorpusSpec(
            snapshot_dir=args.snapshot_dir.resolve(),
            snapshot_date=args.snapshot_date,
            deployment_name=args.deployment_name,
            dsn=args.dsn,
            corpus_root=args.corpus_root.resolve(),
        )
    )
    print(f"cti corpus built: {corpus_json}")
    return 0


def _cmd_corpus_build(args: argparse.Namespace) -> int:
    from .corpus import CorpusSpec, build

    adapter = get_adapter(args.benchmark)
    instances = adapter.load_instances(
        data_root=args.data_root, offline_fixture=args.offline_fixture
    )
    matches = [i for i in instances if i.instance_id == args.instance]
    if not matches:
        raise AbBenchError(
            f"instance {args.instance!r} not found in {args.benchmark}"
        )
    inst = matches[0]
    if not inst.repo or not inst.base_commit or not inst.cutoff:
        raise AbBenchError(
            f"instance {inst.instance_id} lacks repo/base_commit/cutoff — "
            "corpus building needs all three"
        )
    spec = CorpusSpec(
        repo=inst.repo,
        base_commit=inst.base_commit,
        cutoff=inst.cutoff,
        gold_pr_number=inst.extra.get("gold_pr_number"),
        deployment_name=args.deployment_name
        or f"bench-{inst.instance_id.replace('/', '-')}",
        dsn=args.dsn,
        corpus_root=args.corpus_root.resolve(),
        mcp_port=args.mcp_port,
        instance_source=(
            f"offline-fixture:{args.offline_fixture}"
            if args.offline_fixture else "dataset"
        ),
    )
    corpus_json = build(spec, data_root=args.data_root)
    print(f"corpus built: {corpus_json}")
    return 0


def _cmd_corpus_audit(args: argparse.Namespace) -> int:
    from .leakage import audit
    from .model import load_json

    cfg = load_json(args.corpus)
    report = audit(
        dsn=cfg["dsn"],
        generation_id=cfg["generation_id"],
        cutoff=cfg["cutoff"],
        gold_pr_number=cfg.get("gold_pr_number"),
        checkout=Path(cfg["corpus_root"]) / "checkout",
        base_commit=cfg["base_commit"],
    )
    print(f"leakage audit PASS: {report}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
