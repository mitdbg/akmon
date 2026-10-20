"""req.corp.*: spec-driven domain corpus builder — one OKG per benchmark."""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest
import yaml

from okg.bench.ab import corpus_domain as cd
from okg.bench.ab.adapters import adapter_names
from okg.bench.ab.errors import CorpusBuildError


def _spec(tmp_path: Path, vertical: str, snapshot: Path) -> cd.DomainCorpusSpec:
    return cd.DomainCorpusSpec(
        vertical=vertical,
        snapshot_dir=snapshot,
        snapshot_date="2026-07-27",
        deployment_name=f"bench-{vertical}",
        dsn="postgres://x/y",
        corpus_root=tmp_path / f"corpus-{vertical}",
    )


def _snapshot(tmp_path: Path, vertical: str) -> Path:
    snap = tmp_path / f"snap-{vertical}"
    snap.mkdir()
    # Every snapshot must declare where its artifacts came from; a snapshot
    # that declares nothing is refused rather than assumed to hold released
    # data. Fixtures are no exception — exempting them is how the production
    # default of "real" went unnoticed in the first place.
    (snap / cd.PROVENANCE_FILENAME).write_text(
        '{"artifacts": {"fixture": "stand-in"},'
        ' "note": "synthetic test fixture"}'
    )
    if vertical == "cti":
        (snap / "nvdcve-2.0-2024.json").write_text('{"vulnerabilities": []}')
        (snap / "cwec_v4.14.xml").write_text("<Weakness_Catalog/>")
        (snap / "enterprise-attack.json").write_text('{"type": "bundle"}')
    elif vertical == "xbrl":
        docset = snap / "docsets" / "filing-1"
        docset.mkdir(parents=True)
        (docset / "instance.xml").write_text("<xbrl/>")
    elif vertical == "literature":
        (snap / "corpus.jsonl").write_text(
            '{"doc_id": 1, "title": "t", "abstract": ["s"], "structured": false}\n'
        )
    elif vertical == "legal":
        (snap / "statutes.tsv").write_text(
            "idx\tcitation\tpath\tstate\ttext\n1\tc\tp\tAK\tbody\n"
        )
    elif vertical == "ops":
        (snap / "scenarios").mkdir()
        (snap / "scenarios" / "1.json").write_text("{}")
        # The application library the scenario index points at. Without it a
        # scenario's `environment.applications` id and its `alerts` names both
        # dangle, so the vertical requires it as an artifact.
        apps = snap / "library" / "library" / "indexes" / "applications"
        apps.mkdir(parents=True)
        (apps / "1.json").write_text("{}")
        # The published ITBench-Lite telemetry. It replaced the synthetic
        # trajectories stand-in: the recorded telemetry turned out to be
        # published Apache-2.0, so the corpus reads the release rather than a
        # substitute, and `alert_firing` is its observation family.
        scen = (snap / "telemetry" / "snapshots" / "sre" / "v0.2"
                / "Scenario-1")
        (scen / "alerts").mkdir(parents=True)
        (scen / "alerts" / "alerts_at_2025-12-15T17:00:00.json").write_text(
            "[]"
        )
    elif vertical == "stark-prime":
        # The four SKB artifacts the source reads. Subtypes and edge types are
        # derived from node_type_dict / edge_type_dict rather than a
        # hand-written list, so the release determines the composition — which
        # is why all four are required artifacts rather than optional.
        skb = snap / "skb" / "prime" / "processed"
        skb.mkdir(parents=True)
        for name in ("node_info.pkl", "node_type_dict.pkl", "edge_type_dict.pkl",
                     "edge_index.pt", "node_types.pt", "edge_types.pt"):
            (skb / name).write_bytes(b"")
    elif vertical == "phantomwiki":
        # One generated article. The typed half is parsed from PROSE, not from
        # the Prolog KB — in PhantomWiki's KB the kinship predicates
        # (mother, father, sister, ...) are RULE CLAUSES, and the only ground
        # kinship fact is `parent/2`. Ingesting "ground facts only" would have
        # handed the OKG arm question vocabulary the articles never state.
        arts = snap / "articles"
        arts.mkdir()
        (arts / "Aida Wang.txt").write_text(
            "# Aida Wang\n\n## Family\nThe mother of Aida Wang is Bea Wang.\n"
            "\n## Friends\nThe friend of Aida Wang is Cy Sinclair.\n"
            "\n## Attributes\nThe date of birth of Aida Wang is 0altered.\n"
        )
    else:  # doc-corpus
        (snap / "page.md").write_text("# frozen doc\n")
    return snap


def test_every_benchmark_names_one_corpus():
    """The mapping is exhaustive over registered adapters and every entry is
    either a known vertical, the repo-history builder, or a stated NoCorpus."""
    for name in adapter_names():
        corpus = cd.corpus_for_benchmark(name)
        if isinstance(corpus, cd.NoCorpus):
            assert corpus.reason.strip(), f"{name} NoCorpus needs a reason"
            continue
        # repo-history is now registered in VERTICALS like every other
        # vertical (it was a special case only while it had no contract), so
        # one membership check covers all seven.
        assert corpus in cd.VERTICALS, (
            f"{name} -> {corpus!r} is not a known vertical"
        )
    # No stale entries either: everything mapped is a real adapter.
    assert set(cd.BENCHMARK_CORPORA) == set(adapter_names())
    with pytest.raises(CorpusBuildError, match="no recorded corpus"):
        cd.corpus_for_benchmark("not-a-benchmark")


@pytest.mark.parametrize("vertical", sorted(cd.VERTICALS))
def test_spec_driven_build_shapes(tmp_path, vertical, monkeypatch):
    """Each vertical scaffolds a deployment whose manifest, registry, and
    nomos posture match its spec, and whose sources carry resolved paths."""
    snap = _snapshot(tmp_path, vertical)
    spec = _spec(tmp_path, vertical, snap)
    vspec = cd.VERTICALS[vertical]
    if vspec.git_init_snapshot:
        cd._ensure_git_snapshot(snap)
    deployment_dir = cd.write_deployment(spec, vspec)
    manifest = yaml.safe_load((deployment_dir / "deployment.yaml").read_text())
    assert manifest["modules"] == vspec.modules
    posture = manifest["nomos"]["deployment_posture"]
    # The class table forces these; a drift here blocks every publish.
    assert posture["deployment_class"] == "public_reference_demo"
    assert posture["export_policy"] == "public_allowed"
    assert posture["secret_handling"] == "no_secrets"
    assert posture["live_call_default"] == "deny"
    registry = yaml.safe_load((deployment_dir / "source_registry.yaml").read_text())
    assert set(registry["sources"]) == {s.name for s in vspec.sources}
    for source in vspec.sources:
        entry = registry["sources"][source.name]
        assert entry["class"] == source.cls
        assert entry["source_class"] == source.source_class
        assert entry["record_identity_kind"] == source.record_identity_kind
        assert entry["params"], f"{source.name} has no params"
        defaults = manifest["nomos"]["source_policy_defaults"]["by_source_class"]
        assert source.source_class in defaults


def test_unknown_vertical_and_immutability(tmp_path):
    snap = _snapshot(tmp_path, "literature")
    with pytest.raises(CorpusBuildError, match="unknown vertical"):
        cd.build(_spec(tmp_path, "not-a-vertical", snap))
    spec = _spec(tmp_path, "literature", snap)
    spec.corpus_root.mkdir(parents=True)
    (spec.corpus_root / "corpus.json").write_text("{}")
    with pytest.raises(CorpusBuildError, match="immutable"):
        cd.build(spec)


@pytest.mark.parametrize("vertical", sorted(cd.VERTICALS))
def test_zero_family_refusal_per_vertical(tmp_path, vertical, monkeypatch):
    """A hollow corpus never ships: if any expected family publishes zero
    rows the build fails, for every vertical."""
    snap = _snapshot(tmp_path, vertical)
    spec = _spec(tmp_path, vertical, snap)
    monkeypatch.setattr(
        cd, "provision_and_publish",
        lambda *a, **k: ("gen-1", {}),
    )
    with pytest.raises(CorpusBuildError, match="ZERO nodes for families"):
        cd.build(spec)


def test_missing_required_artifact_fails_loudly(tmp_path):
    snap = tmp_path / "bare"
    snap.mkdir()
    (snap / "unrelated.txt").write_text("x")
    with pytest.raises(CorpusBuildError, match="lacks xbrl artifacts"):
        cd.inventory(snap, cd.XBRL)
    empty = tmp_path / "empty"
    empty.mkdir()
    with pytest.raises(CorpusBuildError, match="is empty"):
        cd.inventory(empty, cd.LITERATURE)


def test_snapshot_nested_in_a_checkout_is_refused(tmp_path):
    """`rev-parse --show-toplevel` succeeds from ANY directory inside a
    checkout, so "it succeeded" is not "this dir is a git snapshot".

    Measured consequence before this guard: a 7-file doc-corpus snapshot
    placed under .scratch/ published 8,393 source_file nodes — the entire okg
    repo — with `document -derived_from-> source_file` at ZERO, because the
    file source walked the enclosing repo while the document source keyed
    paths against the snapshot. expected_families and every subtype clause
    passed on that corpus; only a triple clause caught it.
    """
    outer = tmp_path / "checkout"
    (outer / "docs").mkdir(parents=True)
    subprocess.run(["git", "-C", str(outer), "init", "-q", "-b", "main"],
                   check=True, capture_output=True)
    nested = outer / "docs"
    (nested / "page.md").write_text("# frozen doc\n")

    with pytest.raises(CorpusBuildError, match="NESTED inside the git checkout"):
        cd._ensure_git_snapshot(nested)

    # A dir that is its own repository root is accepted...
    standalone = tmp_path / "standalone"
    standalone.mkdir()
    (standalone / "page.md").write_text("# frozen doc\n")
    cd._ensure_git_snapshot(standalone)      # initialises it
    cd._ensure_git_snapshot(standalone)      # ...and is idempotent


def test_freeze_manifest_digests_content_not_git(tmp_path):
    snap = _snapshot(tmp_path, "doc-corpus")
    cd._ensure_git_snapshot(snap)
    inv = cd.inventory(snap, cd.DOC_CORPUS)
    assert "page.md" in inv["files"]
    assert not [k for k in inv["files"] if k.startswith(".git/")], (
        "git metadata is not corpus content"
    )
    assert len(inv["files"]["page.md"]["sha256"]) == 64


def test_doc_corpus_is_retained_but_unconsumed(tmp_path):
    """FreshQA was cut; doc-corpus was its only consumer.

    The spec, contract and snapshot machinery are RETAINED — the
    dated-passage shape and the positional comparability key are general
    document-snapshot work a replacement benchmark can adopt — but the
    vertical is out of VERTICALS, because auditing a vertical no benchmark
    maps to would report a permanent NOT BUILT that no work could clear.
    """
    assert "freshqa" not in cd.BENCHMARK_CORPORA
    assert "doc-corpus" not in cd.VERTICALS
    assert "doc-corpus" in cd.UNCONSUMED_VERTICALS
    assert not any(v == "doc-corpus" for v in cd.BENCHMARK_CORPORA.values()), (
        "a vertical in UNCONSUMED_VERTICALS must have no benchmark mapping"
    )

    # The machinery still works, so adopting it later is a mapping change.
    snap = _snapshot(tmp_path, "doc-corpus")
    assert not (snap / ".git").exists()
    cd._ensure_git_snapshot(snap)
    top = subprocess.run(
        ["git", "-C", str(snap), "rev-parse", "--show-toplevel"],
        capture_output=True, text=True, check=True,
    ).stdout.strip()
    assert Path(top).resolve() == snap.resolve()
    # Idempotent: a second call on an initialised snapshot is a no-op.
    cd._ensure_git_snapshot(snap)
    assert cd.DOC_CORPUS.git_init_snapshot is True
    assert cd.DOC_CORPUS.contract.is_binding()


def test_cti_delegates_to_the_shared_path(tmp_path, monkeypatch):
    """corpus_cti keeps its CLI contract but must not carry a second
    provisioning implementation."""
    from okg.bench.ab import corpus_cti

    source = Path(corpus_cti.__file__).read_text()
    for gone in ("_provision_and_publish", "run_ingest", "ensure_required_extensions"):
        assert gone not in source, f"corpus_cti still implements {gone}"
    assert corpus_cti.EXPECTED_FAMILIES == tuple(cd.CTI.expected_families)

    snap = _snapshot(tmp_path, "cti")
    captured: dict = {}

    def fake_build(spec):
        captured["spec"] = spec
        return tmp_path / "corpus.json"

    monkeypatch.setattr(corpus_cti, "_build_vertical", fake_build)
    corpus_cti.build(corpus_cti.CtiCorpusSpec(
        snapshot_dir=snap, snapshot_date="2026-07-27",
        deployment_name="bench-cti", dsn="postgres://x/y",
        corpus_root=tmp_path / "c",
    ))
    assert captured["spec"].vertical == "cti"


def test_okg_arm_refuses_corpus_less_benchmarks(tmp_path):
    """gitgoodbench/regminer have no okg corpus; asking for the okg arm
    reports the recorded reason rather than a missing-deployment error."""
    from okg.bench.ab.errors import AbBenchError
    from okg.bench.ab.runner import _load_corpus_config

    class _Cfg:
        arm = "okg"
        corpus_config = None

    class _Adapter:
        name = "gitgoodbench"
        requires_corpus_config = False

    with pytest.raises(AbBenchError, match="no okg-arm corpus definition"):
        _load_corpus_config(_Cfg(), _Adapter())


def test_extra_params_override_reaches_the_registry(tmp_path):
    """Operator overrides (e.g. statute state_filter) land in the source."""
    snap = _snapshot(tmp_path, "legal")
    spec = cd.DomainCorpusSpec(
        vertical="legal", snapshot_dir=snap, snapshot_date="2026-07-27",
        deployment_name="bench-legal", dsn="postgres://x/y",
        corpus_root=tmp_path / "c",
        extra_params={"statute-corpus": {"state_filter": "AK"}},
    )
    deployment_dir = cd.write_deployment(spec, cd.LEGAL)
    registry = yaml.safe_load((deployment_dir / "source_registry.yaml").read_text())
    assert registry["sources"]["statute-corpus"]["params"]["state_filter"] == "AK"
