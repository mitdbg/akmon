"""The producer name pairs that actually shipped, held against regression.

No database. Each case is a registry document and a set of recorded runtime
keys; whether the console counts one producer or two is decided by the join
alone.

Three of these registries are in this repository today
(`deployments/okg-workspace`, `deployments/okg-ci`); the Archiv3 three are the
pairs from the defect report, where the Health view showed eight sources with
three needing review on a deployment that configured five, all healthy.
"""
from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any

import pytest

from okg.substrate.console_views import health as health_mod
from okg.substrate.console_views import sources as sources_mod


# (document key, declared ownership_id) -- the two names of one producer.
SHIPPED_PAIRS: list[tuple[str, str]] = [
    # deployments/okg-workspace/source_registry.yaml
    ("bib-references", "okg-workspace.bib_references"),
    ("latex-project", "okg-workspace.latex_project"),
]

# deployments/okg-ci/source_registry.yaml -- no separator rule derives this.
OKG_CI_PAIR: tuple[str, str] = ("ci-triage-verdicts", "agent:okg-ci-triage")

# The Archiv3 deployment from the defect report: five configured sources, of
# which three carried a runtime key spelled with underscores.
ARCHIV3_PAIRS: list[tuple[str, str]] = [
    ("cmssw-releases", "archiv3.cmssw_releases"),
    ("github-repo", "archiv3.github_repo"),
    ("gitlab-repo", "archiv3.gitlab_repo"),
    ("papers", "archiv3.papers"),
    ("twiki", "archiv3.twiki"),
]


def _registry_text(pairs: list[tuple[str, str]]) -> str:
    lines = ["sources:"]
    for name, ownership_id in pairs:
        lines.append(f"  {name}:")
        lines.append(f"    ownership_id: {ownership_id}")
    return "\n".join(lines) + "\n"


def _write_registry(tmp_path, monkeypatch, deployment: str, pairs) -> None:
    dep_dir = tmp_path / deployment
    dep_dir.mkdir()
    (dep_dir / "source_registry.yaml").write_text(_registry_text(pairs))
    monkeypatch.setattr(
        "okg.substrate.deployments.find_deployment_dir",
        lambda name: dep_dir if name == deployment else None,
    )


class _Cursor:
    def __init__(self, rows: list[tuple[str, Any]]) -> None:
        self._rows = rows

    def __enter__(self) -> "_Cursor":
        return self

    def __exit__(self, *_: Any) -> None:
        return None

    def execute(self, *_: Any, **__: Any) -> None:
        return None

    def fetchall(self) -> list[tuple[str, Any]]:
        return list(self._rows)


class _Conn:
    def __init__(self, rows: list[tuple[str, Any]]) -> None:
        self._rows = rows

    def cursor(self) -> _Cursor:
        return _Cursor(self._rows)


@pytest.fixture
def rolled(monkeypatch):
    """Install a rollup keyed by whatever ownership ids a case declares."""

    def install(counts: dict[str, int]) -> None:
        monkeypatch.setattr(
            sources_mod,
            "require_console_count_rollup_enabled",
            lambda **_: None,
        )
        import okg.substrate.console_views.console_count_rollup_read as rollup

        monkeypatch.setattr(
            rollup,
            "read_console_count_rollup",
            lambda conn, **_: {
                (rollup.COUNT_KIND_SOURCE, key): value
                for key, value in counts.items()
            },
        )

    return install


def _compose_sources(conn, deployment: str) -> dict[str, Any]:
    return sources_mod.compose_sources(
        conn,
        deployment_name=deployment,
        session=SimpleNamespace(generation_id="gen:1", branch_name="default"),
    )


@pytest.mark.parametrize(("name", "ownership_id"), SHIPPED_PAIRS + [OKG_CI_PAIR])
def test_a_shipped_name_pair_is_one_producer(
    tmp_path, monkeypatch, rolled, name, ownership_id
):
    """Each pair this repository ships today resolves to a single row."""
    deployment = "shipped-case"
    _write_registry(tmp_path, monkeypatch, deployment, [(name, ownership_id)])
    rolled({ownership_id: 12})

    payload = _compose_sources(_Conn([(ownership_id, None)]), deployment)

    rows = payload["sources"]
    assert payload["source_count"] == 1, [row["name"] for row in rows]
    assert rows[0]["name"] == name
    assert rows[0]["in_registry"] is True
    assert rows[0]["records_in_gen"] == 12


def test_archiv3_five_sources_are_five_rows(tmp_path, monkeypatch, rolled):
    """The reported case: five configured sources, not eight rows."""
    deployment = "archiv3"
    _write_registry(tmp_path, monkeypatch, deployment, ARCHIV3_PAIRS)
    ownership_ids = [ownership_id for _, ownership_id in ARCHIV3_PAIRS]
    rolled({ownership_id: 5 for ownership_id in ownership_ids})

    progress = [(ownership_id, None) for ownership_id in ownership_ids]
    payload = _compose_sources(_Conn(progress), deployment)

    names = sorted(row["name"] for row in payload["sources"])
    assert payload["source_count"] == 5, names
    assert names == sorted(name for name, _ in ARCHIV3_PAIRS)
    assert all(row["in_registry"] for row in payload["sources"])


def test_archiv3_health_counts_five_healthy_not_eight(
    tmp_path, monkeypatch, rolled
):
    """The Health summary that read "8 sources, 3 needing review"."""
    deployment = "archiv3"
    _write_registry(tmp_path, monkeypatch, deployment, ARCHIV3_PAIRS)
    # Every source has run: source_progress records the ownership id, and the
    # row's updated_at is what makes the source count as having run.
    ran_at = datetime(2026, 9, 21, 9, 0, tzinfo=timezone.utc)
    progress = [(ownership_id, ran_at) for _, ownership_id in ARCHIV3_PAIRS]

    breakdown = health_mod._health_source_breakdown(
        _Conn(progress),
        deployment_name=deployment,
        excluded_sources=[],
    )

    summary = breakdown["summary"]
    assert breakdown["source_count"] == 5, [
        row["name"] for row in breakdown["sources"]
    ]
    assert summary["total"] == 5
    # Five rows, one per configured source, each with its progress row. A
    # progress row is not a health signal (issue #2205), so all five read
    # unknown -- the identity claim here is the count, not the verdict.
    assert summary["unknown"] == 5
    assert summary["healthy"] == 0
    assert summary["needs_review"] == 5


def test_an_unconfigured_runtime_key_is_still_listed(
    tmp_path, monkeypatch, rolled
):
    """Collapsing must not hide a producer nothing configures."""
    deployment = "archiv3"
    _write_registry(tmp_path, monkeypatch, deployment, ARCHIV3_PAIRS)
    rolled({"archiv3._enricher:affiliations": 3})

    payload = _compose_sources(_Conn([]), deployment)

    unconfigured = [
        row for row in payload["sources"] if not row["in_registry"]
    ]
    assert [row["name"] for row in unconfigured] == ["_enricher:affiliations"]
    assert unconfigured[0]["records_in_gen"] == 3


def test_a_prefixed_progress_row_is_not_listed_as_a_second_producer(
    tmp_path, monkeypatch, rolled
):
    """The alias guard, re-aimed at the declared identity.

    `okg.source_progress.source_name` carries the deployment prefix on rows
    written before the ownership id was recorded. Where the registry declares
    the bare key as its `ownership_id`, that historical row matches neither
    name, so the composer listed it as a second, zero-count producer whose
    displayed name is the configured one's. A client keyed by name then shows
    the real source's counts as zero -- the defect the deleted
    doubly-prefixed guard existed to stop.

    The key is skipped only when it carries no records and no edges. A key
    with counts of its own is a producer and is still listed.
    """
    deployment = "okg-workspace"
    _write_registry(tmp_path, monkeypatch, deployment, [("git-files", "git-files")])
    rolled({"git-files": 42})

    payload = _compose_sources(
        _Conn([("okg-workspace.git-files", None)]), deployment
    )

    rows = payload["sources"]
    names = [row["name"] for row in rows]
    assert names == ["git-files"], names
    assert payload["source_count"] == 1, rows
    assert rows[0]["in_registry"] is True
    assert rows[0]["records_in_gen"] == 42
    assert payload["alias_keys_not_listed"] == ["okg-workspace.git-files"]


def test_a_prefixed_key_with_counts_of_its_own_is_still_listed(
    tmp_path, monkeypatch, rolled
):
    """The guard drops an empty alias, never a producer that holds records."""
    deployment = "okg-workspace"
    _write_registry(tmp_path, monkeypatch, deployment, [("git-files", "git-files")])
    rolled({"git-files": 42, "okg-workspace.git-files": 9})

    payload = _compose_sources(_Conn([]), deployment)

    unconfigured = [row for row in payload["sources"] if not row["in_registry"]]
    assert [row["source_key"] for row in unconfigured] == [
        "okg-workspace.git-files"
    ]
    assert unconfigured[0]["records_in_gen"] == 9
    assert payload["alias_keys_not_listed"] == []


def test_the_read_never_builds_a_doubly_prefixed_key(
    tmp_path, monkeypatch, rolled
):
    """No key the read emits is `<deployment>.<deployment>.<name>`.

    The old composer built its join key as `f"{prefix}{name}"` over
    `source_progress` names that already carried the prefix, which is what
    manufactured `okg-workspace.okg-workspace.git-files`. This read joins on
    recorded keys only, and this test holds that: every emitted `source_key`
    is either a declared ownership_id or a key some read recorded.
    """
    deployment = "okg-workspace"
    _write_registry(
        tmp_path, monkeypatch, deployment,
        [("git-files", "okg-workspace.git-files")],
    )
    rolled({"okg-workspace.git-files": 3})

    payload = _compose_sources(
        _Conn([
            ("git-files", None),
            ("okg-workspace.git-files", None),
        ]),
        deployment,
    )

    recorded = {"okg-workspace.git-files", "git-files"}
    for row in payload["sources"]:
        assert row["source_key"] in recorded, row
        assert not row["source_key"].startswith("okg-workspace.okg-workspace."), row
    assert payload["source_count"] == 1, [r["name"] for r in payload["sources"]]


# --- what "excluded" matches, and what the publisher actually writes --------


def test_an_exclusion_matches_the_spelling_the_publisher_writes(
    tmp_path, monkeypatch
):
    """The registry NAME, on a source whose ownership_id is a different string.

    `generation.metadata.excluded_sources` is
    `sorted(phase_b.excluded_sources.keys())` (`publisher/publisher.py:10887`).
    Those keys come from two places, and both are the registry name: the
    invariant's own `source` column (`publisher/gates.py:511`), which
    `invariants.yaml` writes as a bare source name, and Phase A's
    `per_source_status` (`publisher/gates.py:433`), which the live publish
    cycle always passes empty. Nothing in that path writes an ownership_id.
    """
    deployment = "archiv3"
    _write_registry(tmp_path, monkeypatch, deployment, ARCHIV3_PAIRS)

    breakdown = health_mod._health_source_breakdown(
        _Conn([]),
        deployment_name=deployment,
        excluded_sources=["papers"],
    )

    by_name = {row["name"]: row for row in breakdown["sources"]}
    assert by_name["papers"]["status"] == "excluded", by_name["papers"]
    assert by_name["papers"]["included"] is False
    # Its declared runtime key is `archiv3.papers`, which is NOT what matched.
    assert by_name["papers"]["source_key"] == "archiv3.papers"
    assert by_name["twiki"]["status"] != "excluded"


#: The configured `cmssw-releases` row is identified by its declared key.
_CMSSW = "archiv3.cmssw_releases"


@pytest.mark.parametrize(
    ("spelling", "excluded_keys"),
    [
        # What the publisher writes: the registry name, which resolves to the
        # configured row.
        ("cmssw-releases", [_CMSSW]),
        # The key the registry DECLARES for it, which `okg.source_progress`
        # and the count rollup record its work under. Same row.
        ("archiv3.cmssw_releases", [_CMSSW]),
        # The key this composer used to BUILD for it and match. No read
        # records it and no publisher writes it, so it resolves to nothing
        # configured and is listed as its own excluded row -- which is what
        # every unrecognised excluded key has always become. The configured
        # producer is untouched. Constructing a runtime key from a producer
        # name is the rule this change ends; the `why` names this.
        ("archiv3.cmssw-releases", ["archiv3.cmssw-releases"]),
        # Another producer's declared key excludes that producer and no other.
        ("archiv3.twiki", ["archiv3.twiki"]),
        # A string nothing answers to: its own row, and nobody else's.
        ("cmssw-releases-mirror", ["cmssw-releases-mirror"]),
    ],
)
def test_the_spellings_an_exclusion_matches_are_named(
    tmp_path, monkeypatch, spelling, excluded_keys
):
    """Deterministic, and one named set rather than one key that changed."""
    deployment = "archiv3"
    _write_registry(tmp_path, monkeypatch, deployment, ARCHIV3_PAIRS)

    breakdown = health_mod._health_source_breakdown(
        _Conn([]),
        deployment_name=deployment,
        excluded_sources=[spelling],
    )

    assert sorted(
        row["source_key"] for row in breakdown["sources"]
        if row["status"] == "excluded"
    ) == excluded_keys, breakdown["sources"]


def test_a_constructed_key_no_longer_excludes_the_configured_row(
    tmp_path, monkeypatch
):
    """The behaviour change, stated on its own so it cannot pass unnoticed.

    `<deployment>.<name>` used to be the row's key, so a generation excluding
    `archiv3.cmssw-releases` marked the configured producer excluded. That
    string is not a key any read records for it -- the registry declares
    `archiv3.cmssw_releases` -- and this composer no longer builds one. The
    configured row stands; the unrecognised key is listed as its own excluded
    row rather than dropped.
    """
    deployment = "archiv3"
    _write_registry(tmp_path, monkeypatch, deployment, ARCHIV3_PAIRS)

    breakdown = health_mod._health_source_breakdown(
        _Conn([]),
        deployment_name=deployment,
        excluded_sources=["archiv3.cmssw-releases"],
    )

    by_key = {row["source_key"]: row for row in breakdown["sources"]}
    assert by_key[_CMSSW]["status"] != "excluded"
    assert by_key[_CMSSW]["included"] is True
    assert by_key["archiv3.cmssw-releases"]["status"] == "excluded"
    # Nothing is silently dropped: six rows for five configured producers plus
    # the excluded key nothing configures.
    assert breakdown["source_count"] == 6, breakdown["sources"]


def test_an_unconfigured_excluded_key_is_matched_by_that_key(
    tmp_path, monkeypatch
):
    """A producer no registry names has no name but the key it was recorded by.

    `test_compose_health_includes_excluded_source_rows` seeds exactly this:
    `excluded_sources = ["test.legacy-docs"]` on a deployment whose registry
    does not name it. The row's displayed name is the shortened key, so
    matching the name alone would have lost it.
    """
    deployment = "archiv3"
    _write_registry(tmp_path, monkeypatch, deployment, ARCHIV3_PAIRS)

    breakdown = health_mod._health_source_breakdown(
        _Conn([]),
        deployment_name=deployment,
        excluded_sources=["archiv3.legacy-docs"],
    )

    row = next(r for r in breakdown["sources"] if r["name"] == "legacy-docs")
    assert row["status"] == "excluded"
    assert row["source_key"] == "archiv3.legacy-docs"


def test_two_distinct_unconfigured_keys_are_two_rows(tmp_path, monkeypatch):
    """Health must list what `compose_sources` lists, one row per recorded key.

    `_resolve` falls back to `_source_display_name`, which strips the
    deployment prefix. Two recorded keys nothing configures -- `archiv3.ghost`
    and `ghost` -- therefore resolved to the same display name and were folded
    into one row whose stamp is the newer of the two. `compose_sources`
    refuses that fold by design: attributing one producer's work to another by
    a prefix-stripped name is the name-derived rule these modules exist to
    end, and the two composers must not disagree about how many producers a
    deployment has.
    """
    deployment = "archiv3"
    _write_registry(tmp_path, monkeypatch, deployment, ARCHIV3_PAIRS)
    ran_at = datetime(2026, 9, 21, 9, 0, tzinfo=timezone.utc)

    breakdown = health_mod._health_source_breakdown(
        _Conn([("archiv3.ghost", ran_at), ("ghost", None)]),
        deployment_name=deployment,
        excluded_sources=[],
    )

    ghosts = [row for row in breakdown["sources"] if row["name"] == "ghost"]
    assert len(ghosts) == 2, breakdown["sources"]
    assert sorted(row["source_key"] for row in ghosts) == [
        "archiv3.ghost", "ghost",
    ]
    # And the run stamp stays with the key that recorded it.
    by_key = {row["source_key"]: row for row in ghosts}
    assert by_key["archiv3.ghost"]["last_update"] is not None
    assert by_key["ghost"]["last_update"] is None
    assert breakdown["source_count"] == 7, breakdown["summary"]
