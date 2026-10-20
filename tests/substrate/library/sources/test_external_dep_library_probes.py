"""Library sources used by external deployments declare sound change-probes (#323).

These library sources (concept_aliases, scholarly/*, experimental/*,
external_repos/factory, bootstrap_fixture) are used by the memex / statgen-analysis
deployments but no in-tree deployment, so they were missed by the first migration
pass. Each must declare a SOUND change_probe_kind (the contract deployment_lint
enforces) and the content-hash floors must be edit/delete-sensitive.
"""
from __future__ import annotations

import json
import os

import pytest

from okg.substrate.library.sources.change_probe import (
    SOUND_PROBE_KINDS,
    tokens_equal,
    validate_token_composition,
)
from okg.substrate.library.sources.content_hash_probe import ContentHashProbe
from okg.substrate.library.sources.bootstrap_fixture import BootstrapFixtureSource
from okg.substrate.library.sources.concept_aliases import ConceptAliasesSource
from okg.substrate.library.sources.external_repos.factory import ExternalRepoFactory
from okg.substrate.library.sources.scholarly.author_aliases import AuthorAliasesSource
from okg.substrate.library.sources.scholarly.bib_references import BibReferencesSource
from okg.substrate.library.sources.scholarly.dblp import DblpSource
from okg.substrate.library.sources.scholarly.our_drafts_source import OurDraftsSource
from okg.substrate.library.sources.scholarly.paper_pdf_source import PaperPdfSource
from okg.substrate.library.sources.scholarly.talks import TalksSource
from okg.substrate.library.sources.experimental.bench_outputs import BenchOutputsSource
from okg.substrate.library.sources.experimental.cluster_jobs import ClusterJobsSource
from okg.substrate.library.sources.experimental.experiment_records import (
    ExperimentRecordsSource,
)
from okg.substrate.library.sources.engram_runs.agent_summary import (
    EngramAgentSummarySource,
)
from okg.substrate.library.sources.engram_runs.candidate_solution import (
    EngramCandidateSolutionSource,
)
from okg.substrate.library.sources.engram_runs.experiment_result import (
    EngramExperimentResultSource,
)
from okg.substrate.library.sources.engram_runs.final_result import (
    EngramFinalResultSource,
)
from okg.substrate.library.sources.engram_runs.research_digest import (
    EngramResearchDigestSource,
)
from okg.substrate.library.sources.upload import UploadSource


_CLASSES = [
    ConceptAliasesSource, AuthorAliasesSource, BibReferencesSource, DblpSource,
    OurDraftsSource, PaperPdfSource, TalksSource, BenchOutputsSource,
    ClusterJobsSource, ExperimentRecordsSource, ExternalRepoFactory,
    BootstrapFixtureSource,
    EngramAgentSummarySource, EngramCandidateSolutionSource,
    EngramExperimentResultSource, EngramFinalResultSource,
    EngramResearchDigestSource,
    # PACT chat-upload-source: the upload source ships in the library and
    # is registered per deployment (there is no in-tree deployment entry),
    # so deployment_lint does not reach it and this list is where its probe
    # declaration is enforced.
    UploadSource,
]


@pytest.mark.parametrize("cls", _CLASSES, ids=lambda c: c.__name__)
def test_declares_sound_probe_kind(cls):
    kind = getattr(cls, "change_probe_kind", None)
    assert kind in SOUND_PROBE_KINDS, f"{cls.__name__}: {kind!r} not a sound kind"


def test_bootstrap_fixture_content_hash_edit_and_delete(tmp_path):
    fx = tmp_path / "facts.json"
    fx.write_text(json.dumps({"nodes": [], "edges": []}))
    s = BootstrapFixtureSource(fixture_path=str(fx))
    assert isinstance(s.change_probe, ContentHashProbe)
    t0 = s.change_probe.build_token()
    validate_token_composition(t0)
    os.utime(fx, (2_000_000_000, 2_000_000_000))
    assert tokens_equal(t0, s.change_probe.build_token())  # mtime churn-immune
    fx.write_text(json.dumps({"nodes": [{"node_id": "x"}], "edges": []}))
    assert not tokens_equal(t0, s.change_probe.build_token())  # edit busts
    t1 = s.change_probe.build_token()
    fx.unlink()
    assert not tokens_equal(t1, s.change_probe.build_token())  # delete busts


def test_dblp_fixture_mode_is_content_hash(tmp_path):
    fx = tmp_path / "dblp.json"
    fx.write_text(json.dumps({"result": {"hits": {"hit": []}}}))
    s = DblpSource(fixture_path=str(fx))
    assert isinstance(s.change_probe, ContentHashProbe)
    t0 = s.change_probe.build_token()
    fx.write_text(json.dumps({"result": {"hits": {"hit": [{"info": {}}]}}}))
    assert not tokens_equal(t0, s.change_probe.build_token())


def test_experiment_records_emit_version_covers_predicate_dsl():
    # The predicates module is folded into emit_version, so a source built today
    # carries an emit_version that fingerprints predicates.py — guard against a
    # regression that drops it back to module-only (a false-unchanged after a
    # predicate-DSL change).
    from okg.substrate.library.sources.change_probe import emit_version_for
    from okg.substrate.library.sources.experimental import (
        experiment_records as er,
        predicates,
    )

    with_predicates = emit_version_for(er, predicates)
    module_only = emit_version_for(er)
    assert with_predicates != module_only
