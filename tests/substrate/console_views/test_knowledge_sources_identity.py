"""The knowledge read's Sources section names a producer once, or refuses.

Issue #2147. `console_views.knowledge._fetch_sources` parsed
`source_registry.yaml` itself, keyed its configured sources by the document
name, and joined them against `okg.source_progress.source_name`, which carries
the registry's `ownership_id`. A source whose name differs from its
`ownership_id` (`git_docs` with `ownership_id: repo-git`) was therefore listed
twice. A registry that did not parse was swallowed and served as
`{"items": []}`, which reads as a deployment that configures nothing.

No database: the join is decidable from a registry file and progress rows.
"""
from __future__ import annotations

import json
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Iterator

import pytest

import yaml

import okg.substrate.console_views.console_count_rollup_read as rollup
from okg.substrate.console_views import knowledge
from okg.substrate.console_views.console_count_rollup_read import (
    ConsoleCountRollupUnavailable,
)
from okg.substrate.console_views.knowledge import (
    ConsoleSourceRegistryUnreadable,
    _fetch_sources,
)

#: The real count read, kept before `counts_not_read` replaces it per test.
REAL_LIVE_SOURCE_COUNTS = knowledge._live_source_counts
ERROR_CODES_YAML = Path(__file__).resolve().parents[3] / "docs/reference/error_codes.yaml"

DEPLOYMENT = "lane-fixture"

REGISTRY = """
sources:
  git_docs:
    ownership_id: repo-git
"""

#: Not YAML: an unclosed flow mapping.
MALFORMED_REGISTRY = "sources: {git_docs: [\n"


def _poll(last_run_at: datetime, *, enqueue: int = 0, errors: int = 0) -> str:
    return json.dumps({
        "last_run_at": last_run_at.isoformat(),
        "enqueue_count": enqueue,
        "error_count": errors,
    })


class _Cursor:
    def __init__(self, rows: list[tuple[str, str, Any, Any]]) -> None:
        self._rows = rows

    def __enter__(self) -> "_Cursor":
        return self

    def __exit__(self, *_: Any) -> None:
        return None

    def execute(self, *_: Any, **__: Any) -> None:
        return None

    def fetchall(self) -> list[tuple[str, str, Any, Any]]:
        return list(self._rows)


class _Conn:
    """`okg.source_progress` rows as (source_name, key, value, updated_at)."""

    def __init__(self, rows: list[tuple[str, str, Any, Any]]) -> None:
        self._rows = rows

    def cursor(self) -> _Cursor:
        return _Cursor(self._rows)


@pytest.fixture
def deployment(tmp_path, monkeypatch):
    """A deployment directory `find_deployment_dir` resolves by name."""
    dep_dir = tmp_path / DEPLOYMENT
    dep_dir.mkdir()
    monkeypatch.setattr(
        "okg.substrate.deployments.find_deployment_dir",
        lambda name, **_: dep_dir if name == DEPLOYMENT else None,
    )
    return dep_dir


def _write(dep_dir: Path, text: str) -> None:
    (dep_dir / "source_registry.yaml").write_text(text)


GENERATION = "okg:lane-fixture:branch:default:gen:1:abc"


def _fetch(conn: Any) -> dict[str, Any]:
    return _fetch_sources(conn, DEPLOYMENT, generation_id=GENERATION)


@pytest.fixture(autouse=True)
def counts_not_read(monkeypatch):
    """The live counts are read only to decide a legacy alias key.

    A test with no such key must not reach them; one that has one replaces
    this with `_counts`.
    """
    def _refuse(conn: Any, *, generation_id: Any) -> dict[str, int]:
        raise AssertionError("live counts read without an alias candidate")

    monkeypatch.setattr(knowledge, "_live_source_counts", _refuse)


def _counts(monkeypatch, counts: dict[str, int]) -> list[Any]:
    seen: list[Any] = []

    def _read(conn: Any, *, generation_id: Any) -> dict[str, int]:
        seen.append(generation_id)
        return counts

    monkeypatch.setattr(knowledge, "_live_source_counts", _read)
    return seen


def test_a_source_named_apart_from_its_ownership_id_is_one_row(deployment):
    """`git_docs` recorded as `repo-git` is one configured row, not two."""
    _write(deployment, REGISTRY)
    now = datetime.now(timezone.utc)
    conn = _Conn([("repo-git", "sync.producer.poll", _poll(now, enqueue=3), now)])

    items = _fetch(conn)["items"]

    assert [item["name"] for item in items] == ["git_docs"], items
    [row] = items
    assert row["source_key"] == "repo-git"
    assert row["in_registry"] is True
    assert row["enqueue_count_24h"] == 3
    assert row["status"] == "healthy"
    assert row["last_run_age"]


def test_a_configured_source_that_never_ran_is_idle(deployment):
    _write(deployment, REGISTRY)

    items = _fetch(_Conn([]))["items"]

    assert items == [{
        "name": "git_docs",
        "source_key": "repo-git",
        "in_registry": True,
        "enqueue_count_24h": 0,
        "error_count_24h": 0,
        "status": "idle",
    }]


def test_progress_under_both_names_is_one_row_with_the_latest_run(deployment):
    """Older rows carry the document key; the latest run is the state served."""
    _write(deployment, REGISTRY)
    now = datetime.now(timezone.utc)
    old = now - timedelta(days=3)
    conn = _Conn([
        ("git_docs", "sync.producer.poll", _poll(old, errors=5), old),
        ("repo-git", "sync.producer.poll", _poll(now, enqueue=2), now),
    ])

    items = _fetch(conn)["items"]

    assert [item["name"] for item in items] == ["git_docs"], items
    assert items[0]["enqueue_count_24h"] == 2
    assert items[0]["error_count_24h"] == 0
    assert items[0]["status"] == "healthy"


def test_a_recorded_key_nothing_configures_is_listed_as_unconfigured(deployment):
    _write(deployment, REGISTRY)
    now = datetime.now(timezone.utc)
    conn = _Conn([("_cascade:x", "sync.producer.poll", _poll(now), now)])

    rows = {item["name"]: item for item in _fetch(conn)["items"]}

    assert set(rows) == {"git_docs", "_cascade:x"}
    assert rows["_cascade:x"]["in_registry"] is False
    assert rows["_cascade:x"]["source_key"] == "_cascade:x"


def test_an_unparseable_registry_is_a_named_refusal(deployment):
    """Pre-fix this returned `{"items": []}` with no error."""
    _write(deployment, MALFORMED_REGISTRY)

    with pytest.raises(ConsoleSourceRegistryUnreadable) as caught:
        _fetch(_Conn([]))

    assert caught.value.code == "console_source_registry_unreadable"
    assert caught.value.data["deployment"] == DEPLOYMENT
    assert caught.value.data["stage"] == "load"
    # The underlying class, not the wrapper's and not the text.
    cause = caught.value.data["cause_type"]
    assert cause and cause != "ConsoleSourceRegistryUnreadable"
    assert cause == type(caught.value.__cause__).__name__


#: `b` is one source's name and another's ownership_id: a recorded `b` would
#: belong to two producers. The loader admits it (ownership ids are unique);
#: `runtime_key_index` refuses it.
OVERLAPPING_REGISTRY = """
sources:
  a:
    ownership_id: b
  b:
    ownership_id: lane-fixture.b
"""


def test_an_identity_overlap_is_the_same_named_refusal(deployment):
    """Sources refuses this registry too (`runtime_key_index` raises)."""
    _write(deployment, OVERLAPPING_REGISTRY)

    with pytest.raises(ConsoleSourceRegistryUnreadable) as caught:
        _fetch(_Conn([]))

    assert caught.value.code == "console_source_registry_unreadable"
    assert caught.value.data == {
        "deployment": DEPLOYMENT,
        "stage": "identity_overlap",
        "cause_type": "ValueError",
    }
    assert "ownership_id" in caught.value.fix_hint


#: okg-ci's shape: the declared ownership_id is not `<deployment>.<name>`, and
#: a row written before it was recorded carries `<deployment>.<name>`.
ALIAS_REGISTRY = """
sources:
  ci-triage-verdicts:
    ownership_id: agent:okg-ci-triage
"""
LEGACY_KEY = f"{DEPLOYMENT}.ci-triage-verdicts"


def test_a_legacy_prefixed_row_with_no_records_is_named_not_listed(
    deployment, monkeypatch,
):
    """One source, as on Sources; the legacy key is named, not a second row."""
    _write(deployment, ALIAS_REGISTRY)
    seen = _counts(monkeypatch, {"agent:okg-ci-triage": 12})
    now = datetime.now(timezone.utc)
    old = now - timedelta(days=30)
    conn = _Conn([
        (LEGACY_KEY, "sync.producer.poll", _poll(old), old),
        ("agent:okg-ci-triage", "sync.producer.poll", _poll(now), now),
    ])

    payload = _fetch(conn)

    assert [item["name"] for item in payload["items"]] == ["ci-triage-verdicts"]
    assert payload["alias_keys_not_listed"] == [LEGACY_KEY]
    assert seen == [GENERATION]


def test_a_legacy_prefixed_row_with_records_of_its_own_is_listed(
    deployment, monkeypatch,
):
    """Counts of its own make it a producer: listed, as Sources lists it."""
    _write(deployment, ALIAS_REGISTRY)
    _counts(monkeypatch, {LEGACY_KEY: 4})
    now = datetime.now(timezone.utc)
    conn = _Conn([(LEGACY_KEY, "sync.producer.poll", _poll(now), now)])

    payload = _fetch(conn)

    rows = {item["name"]: item for item in payload["items"]}
    assert set(rows) == {"ci-triage-verdicts", LEGACY_KEY}
    assert rows[LEGACY_KEY]["in_registry"] is False
    assert payload["alias_keys_not_listed"] == []


def test_a_legacy_key_with_count_tracking_off_refuses(deployment, monkeypatch):
    """The real count read refuses rather than call the key empty.

    Were it to return `{}` with tracking off, the legacy key would be
    omitted as an alias and this read would succeed.
    """
    _write(deployment, ALIAS_REGISTRY)
    monkeypatch.setattr(knowledge, "_live_source_counts", REAL_LIVE_SOURCE_COUNTS)
    monkeypatch.setenv(rollup.COUNT_ROLLUP_FLAG, "0")
    now = datetime.now(timezone.utc)
    conn = _Conn([(LEGACY_KEY, "sync.producer.poll", _poll(now), now)])

    with pytest.raises(ConsoleCountRollupUnavailable):
        _fetch(conn)


def test_a_legacy_key_with_edges_and_no_records_stays_listed(deployment, monkeypatch):
    """Edges count as the key's own work, through the real count read."""
    _write(deployment, ALIAS_REGISTRY)
    monkeypatch.setattr(knowledge, "_live_source_counts", REAL_LIVE_SOURCE_COUNTS)
    monkeypatch.setenv(rollup.COUNT_ROLLUP_FLAG, "1")
    reads: list[Any] = []

    def _rolled(conn: Any, *, generation_id: Any, kinds: Any = None) -> dict:
        reads.append(generation_id)
        return {(rollup.COUNT_KIND_EDGE_SOURCE, LEGACY_KEY): 3}

    monkeypatch.setattr(rollup, "read_console_count_rollup", _rolled)
    now = datetime.now(timezone.utc)
    conn = _Conn([(LEGACY_KEY, "sync.producer.poll", _poll(now), now)])

    payload = _fetch(conn)

    rows = {item["name"]: item for item in payload["items"]}
    assert set(rows) == {"ci-triage-verdicts", LEGACY_KEY}
    assert rows[LEGACY_KEY]["in_registry"] is False
    assert payload["alias_keys_not_listed"] == []
    assert reads == [GENERATION]


def test_the_registered_fix_hint_is_the_load_hint():
    """The registry row states the hint the code serves for `stage: load`."""
    codes = yaml.safe_load(ERROR_CODES_YAML.read_text())["codes"]
    [row] = [c for c in codes if c["code"] == "console_source_registry_unreadable"]
    assert row["fix_hint_template"] == knowledge._REGISTRY_FIX_HINTS["load"]
    assert row["python_exception"] == ConsoleSourceRegistryUnreadable.__name__


def test_the_sources_read_omits_the_same_legacy_key(deployment, monkeypatch):
    """The two reads apply one rule, so they agree on this deployment."""
    from okg.substrate.console_views import sources as sources_mod
    import okg.substrate.console_views.console_count_rollup_read as rollup

    _write(deployment, ALIAS_REGISTRY)
    monkeypatch.setattr(
        sources_mod, "require_console_count_rollup_enabled", lambda **_: None,
    )
    monkeypatch.setattr(
        rollup, "read_console_count_rollup",
        lambda conn, **_: {(rollup.COUNT_KIND_SOURCE, "agent:okg-ci-triage"): 12},
    )

    class _SyncConn:
        def cursor(self) -> Any:
            now = datetime.now(timezone.utc)
            return _Cursor([(LEGACY_KEY, now), ("agent:okg-ci-triage", now)])

    payload = sources_mod.compose_sources(
        _SyncConn(),
        deployment_name=DEPLOYMENT,
        session=SimpleNamespace(generation_id=GENERATION, branch_name="default"),
    )

    assert [row["name"] for row in payload["sources"]] == ["ci-triage-verdicts"]
    assert payload["alias_keys_not_listed"] == [LEGACY_KEY]


def test_a_deployment_without_a_registry_lists_only_recorded_keys(deployment):
    """No registry file is not a failure: nothing is configured."""
    now = datetime.now(timezone.utc)
    conn = _Conn([("repo-git", "sync.producer.poll", _poll(now), now)])

    items = _fetch(conn)["items"]

    assert [(item["name"], item["in_registry"]) for item in items] == [
        ("repo-git", False),
    ]


# --------------------------------------------------------------------------- #
# the route serves the refusal by its registered code                         #
# --------------------------------------------------------------------------- #
class _Ctx:
    def __init__(self) -> None:
        self.deployment_name = DEPLOYMENT
        self.manifest: dict[str, Any] = {}
        self.config: dict[str, Any] = {}
        self.session = SimpleNamespace(
            generation_id="okg:lane-fixture:branch:default:gen:1:abc",
            branch_name="default",
            deployment_name=DEPLOYMENT,
            compat_generation_id=None,
        )

    @contextmanager
    def conn(self) -> Iterator[Any]:
        yield _Conn([])


def test_the_sources_subview_serves_the_registered_refusal(deployment, monkeypatch):
    pytest.importorskip("fastapi")
    from fastapi.testclient import TestClient

    from okg.viz.app import build_app

    _write(deployment, MALFORMED_REGISTRY)
    monkeypatch.setattr(knowledge, "checkout", lambda conn, session: None)
    monkeypatch.setattr(
        "okg.substrate.console_views._chronos.generation_published_at",
        lambda conn, session: None,
    )
    client = TestClient(
        build_app(ctx_provider=_Ctx, is_ready=lambda: True),
        raise_server_exceptions=False,
    )

    response = client.get(
        "/viz/api/console/view", params={"id": "okg_workspace.sources"},
    )

    assert response.status_code == 500, response.text
    detail = response.json()["detail"]
    assert detail["reason"] == "console_source_registry_unreadable"
    assert detail["error_type"] == "ConsoleSourceRegistryUnreadable"
    assert detail["deployment"] == DEPLOYMENT
    assert detail["view_def_id"] == "okg_workspace.sources"
    assert detail["stage"] == "load"
    assert detail["fix_hint"] == knowledge._REGISTRY_FIX_HINTS["load"]
    # The underlying class name is served; its text is not.
    assert detail["cause_type"] not in {"", None, "ConsoleSourceRegistryUnreadable"}
    assert "did not find expected" not in response.text
    assert str(deployment) not in response.text
