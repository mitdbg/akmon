from __future__ import annotations

import ast
import importlib
import sys
import threading
import time
from pathlib import Path
from types import ModuleType
from typing import Any

import psycopg
import pytest

from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import NodeFact
from okg.substrate.publisher.checkpoint_attestations import (
    CheckpointAttestationConfigurationError,
    CheckpointAttestationFailure,
    CheckpointAttestationRefused,
    CheckpointAttestationResult,
    LoadedCheckpointAttestor,
    canonicalize_json,
    checkpoint_attestation_context,
    inspect_checkpoint_attestor_declarations,
    load_checkpoint_attestors,
    payload_digest,
    run_checkpoint_attestors,
    validate_loaded_checkpoint_attestors,
)
from okg.substrate.publisher.publisher import Publisher
from tests.pytest.catalog_fixture_builders import seed_catalog
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402


class _PagedCandidateSession:
    def __init__(
        self,
        *,
        nodes: list[dict[str, Any]],
        edges: list[dict[str, Any]],
    ) -> None:
        self.nodes = nodes
        self.edges = edges

    def query(
        self,
        statement: str,
        params: dict[str, Any],
    ) -> list[dict[str, Any]]:
        if "FROM okg.graph_nodes" in statement:
            after = params.get("after_node_id")
            return [
                row for row in self.nodes
                if after is None or row["node_id"] > after
            ]
        if "FROM okg.graph_edges" in statement:
            after = params.get("after_edge_id")
            return [
                row for row in self.edges
                if after is None or row["edge_id"] > after
            ]
        raise AssertionError(f"unexpected candidate query: {statement}")


# Upper bounds for the positive waits in the concurrent-writer test. Each
# returns as soon as its event fires (the test itself takes a few seconds on
# an idle host), so the bound only decides how long a genuinely stuck publisher takes
# to fail. 10 s was too tight for a loaded CI runner: the publisher had not
# reached the attestor yet (lane M trains, 2026-10-07).
_LOADED_RUNNER_WAIT_SECONDS = 60


_FENCE_WRITER_APP = "okg-attestation-fence-writer"


def _wait_until_writer_blocks_on_fence(dsn: str, writer_done: threading.Event) -> None:
    """Return once the fence writer's session waits on an advisory lock.

    Fails if the writer finishes first (it escaped the fence) or never blocks
    within _LOADED_RUNNER_WAIT_SECONDS.
    """
    deadline = time.monotonic() + _LOADED_RUNNER_WAIT_SECONDS
    with psycopg.connect(dsn, autocommit=True) as conn:
        while True:
            blocked = conn.execute(
                "SELECT count(*) FROM pg_stat_activity "
                "WHERE application_name = %s "
                "AND wait_event_type = 'Lock' AND wait_event = 'advisory'",
                (_FENCE_WRITER_APP,),
            ).fetchone()[0]
            if blocked:
                return
            assert not writer_done.is_set(), (
                "branch writer escaped the attested checkpoint fence"
            )
            assert time.monotonic() < deadline, (
                "branch writer never blocked on the checkpoint fence"
            )
            time.sleep(0.05)


class _ContextAttestor:
    attestor_id = "tests.candidate"
    config_schema = {
        "type": "object",
        "properties": {},
        "required": [],
        "additionalProperties": False,
    }

    def __init__(
        self,
        *,
        entered: threading.Event | None = None,
        release: threading.Event | None = None,
    ) -> None:
        self.calls = 0
        self.contexts: list[Any] = []
        self.entered = entered
        self.release = release

    def attest(self, context: Any) -> CheckpointAttestationResult:
        self.calls += 1
        self.contexts.append(context)
        if self.entered is not None:
            self.entered.set()
        if self.release is not None and not self.release.wait(
            timeout=_LOADED_RUNNER_WAIT_SECONDS
        ):
            raise RuntimeError("test release was not signaled")
        payload = {
            "branch": context.branch_name,
            "candidate_counts": context.candidate_counts(),
            "native_generation_id": context.native_generation_id,
            "node_ids": [row["node_id"] for row in context.candidate_nodes()],
            "target_generation_token": context.target_generation_token,
        }
        return CheckpointAttestationResult(
            attestor_id=self.attestor_id,
            schema="tests.candidate.v1",
            status="valid",
            payload=payload,
            payload_digest=payload_digest(payload),
        )


class _RefusingAttestor:
    attestor_id = "tests.refusing"
    config_schema = {
        "type": "object",
        "properties": {},
        "required": [],
        "additionalProperties": False,
    }

    def __init__(self, code: str = "attestation_invalid") -> None:
        self.code = code
        self.calls = 0

    def attest(self, context: Any) -> CheckpointAttestationResult:
        self.calls += 1
        raise CheckpointAttestationRefused(self.code, "receipt set is incomplete")


class _ExplodingAttestor:
    attestor_id = "tests.exploding"
    config_schema = {
        "type": "object",
        "properties": {},
        "required": [],
        "additionalProperties": False,
    }

    def attest(self, context: Any) -> CheckpointAttestationResult:
        raise RuntimeError("secret database password must not escape")


class _ResultAttestor:
    attestor_id = "tests.result"
    config_schema = {
        "type": "object",
        "properties": {},
        "required": [],
        "additionalProperties": False,
    }

    def __init__(self, result: Any) -> None:
        self.result = result

    def attest(self, context: Any) -> CheckpointAttestationResult:
        return self.result


class _LargeAttestor:
    config_schema = {
        "type": "object",
        "properties": {},
        "required": [],
        "additionalProperties": False,
    }

    def __init__(self, index: int) -> None:
        self.attestor_id = f"tests.large{index}"

    def attest(self, context: Any) -> CheckpointAttestationResult:
        payload = {"a": "a" * 16_000, "b": "b" * 16_000, "c": "c" * 16_000}
        return CheckpointAttestationResult(
            attestor_id=self.attestor_id,
            schema="tests.large.v1",
            status="valid",
            payload=payload,
            payload_digest=payload_digest(payload),
        )


def _loaded(instance: Any) -> LoadedCheckpointAttestor:
    return LoadedCheckpointAttestor(
        manifest_id=instance.attestor_id,
        class_name=f"{type(instance).__module__}.{type(instance).__qualname__}",
        attestor=instance,
    )


def _seed_candidate(dsn: str, *, node_id: str = "attested:one") -> None:
    seed_catalog(
        psycopg.connect(dsn, autocommit=True),
        subtypes=["page"],
        narrowings=[],
    )
    with psycopg.connect(dsn, autocommit=True) as conn:
        ChronosGraphStore.from_connection(conn).apply_facts(
            [NodeFact(node_id, "page", {"text": node_id}, {"id": node_id}, {})],
            source="attestation-test",
            strict_endpoints=True,
        )


def test_candidate_pages_cover_a_candidate_larger_than_the_complete_read_cap() -> None:
    nodes = [
        {
            "node_id": f"node-{index:05d}",
            "subtype": "page",
            "attrs": {"index": index},
            "source": "source",
            "source_record_id": {"id": index},
            "source_revision": {"revision": index},
        }
        for index in range(10_005)
    ]
    edges = [
        {
            "edge_id": f"edge-{index:02d}",
            "src": "node-00000",
            "dst": f"node-{index + 1:05d}",
            "edge_type": "links",
            "attrs": {"index": index},
            "source": "source",
            "source_record_id": {"id": index},
            "source_revision": {"revision": index},
        }
        for index in range(3)
    ]
    session = _PagedCandidateSession(nodes=nodes, edges=edges)

    with checkpoint_attestation_context(
        conn=object(),  # type: ignore[arg-type]
        candidate_session=session,
        deployment_name="paged-test",
        branch_name="default",
        native_generation_id="candidate-generation",
        target_generation_token=1,
        catalog_version_id=1,
        node_count=len(nodes),
        edge_count=len(edges),
    ) as context:
        with pytest.raises(
            CheckpointAttestationRefused,
            match="attestation_unavailable",
        ) as bounded_read:
            context.candidate_nodes()
        assert bounded_read.value.detail == (
            "candidate nodes exceed the requested complete-read limit"
        )

        first = context.candidate_nodes_page()
        assert len(first.rows) == 10_000
        assert first.complete is False
        assert first.next_cursor == "node-09999"

        second = context.candidate_nodes_page(after_node_id=first.next_cursor)
        assert [row["node_id"] for row in second.rows] == [
            f"node-{index:05d}" for index in range(10_000, 10_005)
        ]
        assert second.complete is True
        assert second.next_cursor is None

        first_edges = context.candidate_edges_page(limit=2)
        second_edges = context.candidate_edges_page(
            after_edge_id=first_edges.next_cursor,
            limit=2,
        )
        assert [row["edge_id"] for row in first_edges.rows] == [
            "edge-00",
            "edge-01",
        ]
        assert first_edges.complete is False
        assert [row["edge_id"] for row in second_edges.rows] == ["edge-02"]
        assert second_edges.complete is True

        with pytest.raises(
            CheckpointAttestationRefused,
            match="attestation_invalid",
        ) as invalid_cursor:
            context.candidate_nodes_page(after_node_id="")
        assert invalid_cursor.value.detail == (
            "after_node_id must be a non-empty string"
        )

    with pytest.raises(
        CheckpointAttestationRefused,
        match="attestation_unavailable",
    ) as expired_context:
        context.candidate_nodes_page()
    assert expired_context.value.detail == (
        "candidate read context is no longer active"
    )


def test_jcs_integer_only_subset_is_canonical_and_utf16_sorted() -> None:
    payload = {"\U0001f600": 2, "\ufffd": 1, "a": [True, None, "line\n"]}

    assert canonicalize_json(payload) == (
        b'{"a":[true,null,"line\\n"],"\xf0\x9f\x98\x80":2,'
        b'"\xef\xbf\xbd":1}'
    )
    assert payload_digest(payload).startswith("sha256:")


@pytest.mark.parametrize(
    "payload",
    [
        {"float": 1.0},
        {"wide": 2**63},
        {"not_nfc": "e\u0301"},
        {1: "non-string key"},
        {"tuple": (1, 2)},
        {"surrogate": "\ud800"},
    ],
)
def test_canonical_payload_rejects_values_outside_strict_ijson(payload: Any) -> None:
    with pytest.raises(CheckpointAttestationFailure) as caught:
        canonicalize_json(payload)

    assert caught.value.code in {
        "attestation_invalid",
        "attestation_limit_exceeded",
    }


def test_canonical_payload_enforces_exact_collection_depth_limit() -> None:
    at_limit: dict[str, Any] = {}
    for _ in range(15):
        at_limit = {"nested": at_limit}

    canonicalize_json(at_limit)

    beyond_limit = {"nested": at_limit}
    with pytest.raises(CheckpointAttestationFailure) as caught:
        canonicalize_json(beyond_limit)
    assert caught.value.code == "attestation_limit_exceeded"


@pytest.mark.parametrize(
    "result, expected_code",
    [
        ({"status": "valid"}, "attestation_invalid"),
        (
            CheckpointAttestationResult(
                "tests.other", "tests.result.v1", "valid", {}, payload_digest({})
            ),
            "attestation_invalid",
        ),
        (
            CheckpointAttestationResult(
                "tests.result", "INVALID", "valid", {}, payload_digest({})
            ),
            "attestation_invalid",
        ),
        (
            CheckpointAttestationResult(
                "tests.result", "tests.result.v1", "failed", {}, payload_digest({})
            ),
            "attestation_invalid",
        ),
        (
            CheckpointAttestationResult(
                "tests.result", "tests.result.v1", "valid", {}, "SHA256:bad"
            ),
            "attestation_invalid",
        ),
        (
            CheckpointAttestationResult(
                "tests.result", "tests.result.v1", "valid", {}, "sha256:" + "0" * 64
            ),
            "attestation_digest_mismatch",
        ),
        (
            CheckpointAttestationResult(
                "tests.result",
                "tests.result.v1",
                "valid",
                {"oversized": "x" * 16_385},
                "sha256:" + "0" * 64,
            ),
            "attestation_limit_exceeded",
        ),
    ],
)
def test_result_validation_maps_every_malformed_class_to_stable_code(
    result: Any,
    expected_code: str,
) -> None:
    attestor = _ResultAttestor(result)

    with pytest.raises(CheckpointAttestationFailure) as caught:
        run_checkpoint_attestors([_loaded(attestor)], context=None)  # type: ignore[arg-type]

    assert caught.value.attestor_id == "tests.result"
    assert caught.value.code == expected_code


def test_result_validation_rejects_extra_runtime_fields() -> None:
    result = CheckpointAttestationResult(
        "tests.result",
        "tests.result.v1",
        "valid",
        {},
        payload_digest({}),
    )
    object.__setattr__(result, "extra", "not part of the protocol")

    with pytest.raises(CheckpointAttestationFailure) as caught:
        run_checkpoint_attestors(
            [_loaded(_ResultAttestor(result))],
            context=None,  # type: ignore[arg-type]
        )
    assert caught.value.code == "attestation_invalid"


def test_attestor_count_duplicate_and_aggregate_limits_are_enforced() -> None:
    loaded = [_loaded(_LargeAttestor(index)) for index in range(8)]

    with pytest.raises(CheckpointAttestationFailure) as aggregate:
        run_checkpoint_attestors(loaded, context=None)  # type: ignore[arg-type]
    assert aggregate.value.code == "attestation_limit_exceeded"

    with pytest.raises(CheckpointAttestationConfigurationError) as count:
        validate_loaded_checkpoint_attestors([
            *_loaded_many(9),
        ])
    assert count.value.code == "attestation_limit_exceeded"

    duplicate = _loaded(_LargeAttestor(0))
    with pytest.raises(CheckpointAttestationConfigurationError) as dup:
        validate_loaded_checkpoint_attestors([duplicate, duplicate])
    assert dup.value.code == "attestation_duplicate_id"

    with pytest.raises(CheckpointAttestationConfigurationError, match="sequence"):
        validate_loaded_checkpoint_attestors({})  # type: ignore[arg-type]


def _loaded_many(count: int) -> list[LoadedCheckpointAttestor]:
    return [_loaded(_LargeAttestor(index)) for index in range(count)]


def test_loader_uses_raw_closed_config_and_publisher_owned_root(tmp_path: Path) -> None:
    deployment = tmp_path / "sample"
    package = deployment / "attestors"
    package.mkdir(parents=True)
    (package / "__init__.py").write_text("", encoding="utf-8")
    (package / "sample.py").write_text(
        "class SampleAttestor:\n"
        "    attestor_id = 'sample.receipt'\n"
        "    config_schema = {\n"
        "        'type': 'object',\n"
        "        'properties': {'label': {'type': 'string'}},\n"
        "        'required': ['label'],\n"
        "        'additionalProperties': False,\n"
        "    }\n"
        "    def __init__(self, *, deployment_root, label):\n"
        "        self.deployment_root = deployment_root\n"
        "        self.label = label\n"
        "    def attest(self, context):\n"
        "        raise NotImplementedError\n",
        encoding="utf-8",
    )
    (deployment / "deployment.yaml").write_text(
        "name: sample\n"
        "name: ignored-duplicate-outside-attestor-subtree\n"
        "checkpoint_attestors:\n"
        "  - id: sample.receipt\n"
        "    class: attestors.sample.SampleAttestor\n"
        "    config:\n"
        "      label: pilot\n",
        encoding="utf-8",
    )

    loaded = load_checkpoint_attestors(deployment, deployment_name="sample")

    assert [item.manifest_id for item in loaded] == ["sample.receipt"]
    assert loaded[0].class_name == "attestors.sample.SampleAttestor"
    assert loaded[0].attestor.deployment_root == deployment.resolve()
    assert loaded[0].attestor.label == "pilot"


def test_manifest_compatibility_ignores_unrelated_duplicate_keys(
    tmp_path: Path,
) -> None:
    deployment = tmp_path / "sample"
    deployment.mkdir()
    (deployment / "deployment.yaml").write_text(
        "name: sample\nname: duplicate\nruntime: {}\n",
        encoding="utf-8",
    )

    assert inspect_checkpoint_attestor_declarations(
        deployment,
        deployment_name="sample",
    ) == []


def test_loader_evicts_shared_attestor_package_between_deployments(
    tmp_path: Path,
) -> None:
    deployments: list[Path] = []
    for name in ("alpha", "beta"):
        deployment = tmp_path / name
        package = deployment / "attestors"
        package.mkdir(parents=True)
        (package / "__init__.py").write_text("", encoding="utf-8")
        (package / "shared.py").write_text(
            "class SharedAttestor:\n"
            f"    attestor_id = '{name}.receipt'\n"
            "    config_schema = {\n"
            "        'type': 'object',\n"
            "        'properties': {},\n"
            "        'required': [],\n"
            "        'additionalProperties': False,\n"
            "    }\n"
            "    def __init__(self, *, deployment_root):\n"
            "        self.deployment_root = deployment_root\n"
            "    def attest(self, context): raise NotImplementedError\n",
            encoding="utf-8",
        )
        (deployment / "deployment.yaml").write_text(
            f"name: {name}\n"
            "checkpoint_attestors:\n"
            f"  - id: {name}.receipt\n"
            "    class: attestors.shared.SharedAttestor\n"
            "    config: {}\n",
            encoding="utf-8",
        )
        deployments.append(deployment)

    alpha = load_checkpoint_attestors(deployments[0], deployment_name="alpha")
    beta = load_checkpoint_attestors(deployments[1], deployment_name="beta")

    assert alpha[0].manifest_id == "alpha.receipt"
    assert beta[0].manifest_id == "beta.receipt"
    assert alpha[0].attestor.deployment_root == deployments[0].resolve()
    assert beta[0].attestor.deployment_root == deployments[1].resolve()


def test_loader_reloads_deployment_local_attestor_support_between_roots(
    tmp_path: Path,
) -> None:
    roots: list[Path] = []
    for directory, label in (("release-a", "release-a"), ("release-b", "release-b")):
        root = tmp_path / directory
        attestors = root / "checkpoint_attestors"
        support = root / "sealed_attestor_support"
        namespace_support = root / "sealed_attestor_namespace"
        attestors.mkdir(parents=True)
        support.mkdir()
        namespace_support.mkdir()
        (attestors / "__init__.py").write_text("", encoding="utf-8")
        (support / "__init__.py").write_text(
            "from pathlib import Path\n"
            "from sealed_attestor_namespace.eager import EAGER_LABEL\n"
            "def resource_label():\n"
            "    return Path(__file__).with_name('label.txt').read_text(encoding='utf-8').strip()\n"
            "def lazy_label():\n"
            "    from sealed_attestor_namespace.release import RELEASE_LABEL\n"
            "    return RELEASE_LABEL\n",
            encoding="utf-8",
        )
        (support / "label.txt").write_text(label, encoding="utf-8")
        (namespace_support / "eager.py").write_text(
            f"EAGER_LABEL = {label!r}\n",
            encoding="utf-8",
        )
        (namespace_support / "release.py").write_text(
            f"RELEASE_LABEL = {label!r}\n",
            encoding="utf-8",
        )
        (attestors / "shared.py").write_text(
            "from sealed_attestor_support import EAGER_LABEL, lazy_label, resource_label\n"
            "class SharedAttestor:\n"
            "    attestor_id = 'sample.receipt'\n"
            "    config_schema = {'type': 'object', 'properties': {}, "
            "'required': [], 'additionalProperties': False}\n"
            "    def __init__(self, *, deployment_root):\n"
            "        self.deployment_root = deployment_root\n"
            "        self.resource_label = resource_label()\n"
            "        self.eager_label = EAGER_LABEL\n"
            "    def lazy_release_label(self): return lazy_label()\n"
            "    def attest(self, context): raise NotImplementedError\n",
            encoding="utf-8",
        )
        roots.append(root)

    declaration = [
        {
            "id": "sample.receipt",
            "class": "checkpoint_attestors.shared.SharedAttestor",
            "config": {},
        }
    ]
    try:
        first = load_checkpoint_attestors(
            roots[0],
            deployment_name="sample",
            declarations=declaration,
            implementation_root=roots[0],
        )
        assert "sealed_attestor_namespace.release" not in sys.modules
        second = load_checkpoint_attestors(
            roots[1],
            deployment_name="sample",
            declarations=declaration,
            implementation_root=roots[1],
        )

        assert first[0].attestor.resource_label == "release-a"
        assert second[0].attestor.resource_label == "release-b"
        assert first[0].attestor.eager_label == "release-a"
        assert second[0].attestor.eager_label == "release-b"
        assert second[0].attestor.lazy_release_label() == "release-b"
        support_package = sys.modules["sealed_attestor_support"]
        namespace_package = sys.modules["sealed_attestor_namespace"]
        support_module = sys.modules["sealed_attestor_namespace.release"]
        assert Path(support_package.__file__).resolve().is_relative_to(
            roots[1].resolve()
        )
        assert all(
            Path(path).resolve().is_relative_to(roots[1].resolve())
            for path in namespace_package.__path__
        )
        assert Path(support_module.__file__).resolve().is_relative_to(
            roots[1].resolve()
        )
    finally:
        for name in tuple(sys.modules):
            if name in {
                "sealed_attestor_support",
                "sealed_attestor_namespace",
            } or name.startswith(
                ("sealed_attestor_support.", "sealed_attestor_namespace.")
            ):
                sys.modules.pop(name, None)


def test_loader_removes_a_discovered_unregistered_implementation_root(
    tmp_path: Path,
) -> None:
    prior = tmp_path / "unregistered-release"
    selected = tmp_path / "selected-release"
    for root in (prior, selected):
        (root / "checkpoint_attestors").mkdir(parents=True)
        (root / "checkpoint_attestors/__init__.py").write_text("", encoding="utf-8")
        (root / "_sealed_attestor_unregistered").mkdir()
        (root / "_sealed_attestor_unregistered/__init__.py").write_text(
            f"LABEL = {root.name!r}\n",
            encoding="utf-8",
        )
    (prior / "attestor_unbound_extra.py").write_text(
        "VALUE = 'outside-selected-root'\n",
        encoding="utf-8",
    )
    (selected / "checkpoint_attestors/shared.py").write_text(
        "from _sealed_attestor_unregistered import LABEL\n"
        "class SharedAttestor:\n"
        "    attestor_id = 'sample.receipt'\n"
        "    config_schema = {'type': 'object', 'properties': {}, "
        "'required': [], 'additionalProperties': False}\n"
        "    def __init__(self, *, deployment_root): self.label = LABEL\n"
        "    def attest(self, context): raise NotImplementedError\n",
        encoding="utf-8",
    )
    declaration = [
        {
            "id": "sample.receipt",
            "class": "checkpoint_attestors.shared.SharedAttestor",
            "config": {},
        }
    ]
    sys.path.insert(0, str(prior.resolve()))
    try:
        support = importlib.import_module("_sealed_attestor_unregistered")
        assert Path(support.__file__).resolve().is_relative_to(prior.resolve())

        loaded = load_checkpoint_attestors(
            selected,
            deployment_name="sample",
            declarations=declaration,
            implementation_root=selected,
        )

        assert loaded[0].attestor.label == "selected-release"
        assert all(Path(entry or ".").resolve() != prior.resolve() for entry in sys.path)
        with pytest.raises(ModuleNotFoundError):
            importlib.import_module("attestor_unbound_extra")
    finally:
        sys.path[:] = [
            entry
            for entry in sys.path
            if Path(entry or ".").resolve() not in {prior.resolve(), selected.resolve()}
        ]
        for name in tuple(sys.modules):
            if name in {
                "attestor_unbound_extra",
                "_sealed_attestor_unregistered",
            } or name.startswith("_sealed_attestor_unregistered."):
                sys.modules.pop(name, None)


def test_attestation_refuses_a_late_mixed_namespace_package(tmp_path: Path) -> None:
    selected = tmp_path / "selected-release"
    outsider = tmp_path / "outside-release"
    attestors = selected / "checkpoint_attestors"
    namespace = selected / "sealed_attestor_lazy"
    outsider_namespace = outsider / "sealed_attestor_lazy"
    attestors.mkdir(parents=True)
    namespace.mkdir()
    outsider_namespace.mkdir(parents=True)
    (attestors / "__init__.py").write_text("", encoding="utf-8")
    for root, label in ((namespace, "selected"), (outsider_namespace, "outside")):
        (root / "release.py").write_text(
            f"RELEASE_LABEL = {label!r}\n",
            encoding="utf-8",
        )
    (attestors / "shared.py").write_text(
        "class SharedAttestor:\n"
        "    attestor_id = 'sample.receipt'\n"
        "    config_schema = {'type': 'object', 'properties': {}, "
        "'required': [], 'additionalProperties': False}\n"
        "    def __init__(self, *, deployment_root): pass\n"
        "    def attest(self, context):\n"
        "        from sealed_attestor_lazy.release import RELEASE_LABEL\n"
        "        raise RuntimeError(RELEASE_LABEL)\n",
        encoding="utf-8",
    )
    try:
        loaded = load_checkpoint_attestors(
            selected,
            deployment_name="sample",
            declarations=[
                {
                    "id": "sample.receipt",
                    "class": "checkpoint_attestors.shared.SharedAttestor",
                    "config": {},
                }
            ],
            implementation_root=selected,
        )
        assert tuple(Path(path).resolve() for path in sys.modules[
            "sealed_attestor_lazy"
        ].__path__) == (namespace.resolve(),)

        sys.path.insert(0, str(outsider.resolve()))
        with pytest.raises(CheckpointAttestationFailure) as caught:
            run_checkpoint_attestors(loaded, context=None)  # type: ignore[arg-type]

        assert caught.value.code == "attestation_invalid"
        assert "sealed_attestor_lazy.release" not in sys.modules
    finally:
        sys.path[:] = [
            entry
            for entry in sys.path
            if Path(entry or ".").resolve()
            not in {selected.resolve(), outsider.resolve()}
        ]
        for name in tuple(sys.modules):
            if name == "sealed_attestor_lazy" or name.startswith(
                "sealed_attestor_lazy."
            ):
                sys.modules.pop(name, None)


def test_loader_rejects_colliding_deployment_package_names(tmp_path: Path) -> None:
    for index, name in enumerate(("foo-bar", "foo_bar")):
        deployment = tmp_path / name
        package = deployment / "foo_bar"
        package.mkdir(parents=True)
        (package / "__init__.py").write_text("", encoding="utf-8")
        (package / "sample.py").write_text(
            "class SampleAttestor:\n"
            f"    attestor_id = 'tests.collision{index}'\n"
            "    config_schema = {'type': 'object', 'properties': {}, "
            "'required': [], 'additionalProperties': False}\n"
            "    def __init__(self, *, deployment_root):\n"
            "        self.deployment_root = deployment_root\n"
            "    def attest(self, context): raise NotImplementedError\n",
            encoding="utf-8",
        )
        (deployment / "deployment.yaml").write_text(
            f"name: {name}\n"
            "checkpoint_attestors:\n"
            f"  - id: tests.collision{index}\n"
            "    class: foo_bar.sample.SampleAttestor\n"
            "    config: {}\n",
            encoding="utf-8",
        )
        with pytest.raises(
            CheckpointAttestationConfigurationError,
            match="deployment-local attestors",
        ):
            load_checkpoint_attestors(deployment, deployment_name=name)


def test_loader_never_evicts_framework_package_for_colliding_deployment(
    tmp_path: Path,
) -> None:
    import okg

    deployment = tmp_path / "okg"
    deployment.mkdir()
    (deployment / "deployment.yaml").write_text(
        "name: okg\n"
        "checkpoint_attestors:\n"
        "  - id: tests.framework-collision\n"
        "    class: okg.sample.SampleAttestor\n"
        "    config: {}\n",
        encoding="utf-8",
    )
    framework_package = sys.modules["okg"]

    with pytest.raises(
        CheckpointAttestationConfigurationError,
        match="deployment-local attestors",
    ):
        load_checkpoint_attestors(deployment, deployment_name="okg")

    assert sys.modules["okg"] is framework_package is okg


def test_attestor_reload_preserves_other_current_deployment_modules(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    deployment = tmp_path / "sample"
    package = deployment / "attestors"
    enricher_package = deployment / "enrichers"
    package.mkdir(parents=True)
    enricher_package.mkdir()
    (package / "__init__.py").write_text("", encoding="utf-8")
    (package / "sample.py").write_text(
        "class SampleAttestor:\n"
        "    attestor_id = 'sample.receipt'\n"
        "    config_schema = {'type': 'object', 'properties': {}, "
        "'required': [], 'additionalProperties': False}\n"
        "    def __init__(self, *, deployment_root): pass\n"
        "    def attest(self, context): raise NotImplementedError\n",
        encoding="utf-8",
    )
    (deployment / "deployment.yaml").write_text(
        "name: sample\n"
        "checkpoint_attestors:\n"
        "  - id: sample.receipt\n"
        "    class: attestors.sample.SampleAttestor\n"
        "    config: {}\n",
        encoding="utf-8",
    )
    sentinel = ModuleType("enrichers")
    sentinel.__file__ = str(enricher_package / "__init__.py")
    monkeypatch.setitem(sys.modules, "enrichers", sentinel)

    load_checkpoint_attestors(deployment, deployment_name="sample")

    assert sys.modules["enrichers"] is sentinel


@pytest.mark.parametrize(
    "config_yaml, message",
    [
        ("      label: ${ATTESTATION_LABEL}\n", "environment substitutions"),
        ("      label: pilot\n      dsn: secret\n", "publisher authority"),
        ("      label: pilot\n      label: duplicate\n", "duplicate key"),
        ("      unknown: value\n", "config rejected"),
    ],
)
def test_loader_rejects_placeholder_authority_duplicate_and_unknown_config(
    tmp_path: Path,
    config_yaml: str,
    message: str,
) -> None:
    deployment = tmp_path / "sample"
    package = deployment / "attestors"
    package.mkdir(parents=True)
    (package / "__init__.py").write_text("", encoding="utf-8")
    (package / "sample.py").write_text(
        "class SampleAttestor:\n"
        "    attestor_id = 'sample.receipt'\n"
        "    config_schema = {\n"
        "        'type': 'object',\n"
        "        'properties': {'label': {'type': 'string'}},\n"
        "        'required': ['label'],\n"
        "        'additionalProperties': False,\n"
        "    }\n"
        "    def __init__(self, *, deployment_root, label): pass\n"
        "    def attest(self, context): raise NotImplementedError\n",
        encoding="utf-8",
    )
    (deployment / "deployment.yaml").write_text(
        "name: sample\n"
        "checkpoint_attestors:\n"
        "  - id: sample.receipt\n"
        "    class: attestors.sample.SampleAttestor\n"
        "    config:\n"
        + config_yaml,
        encoding="utf-8",
    )

    with pytest.raises(CheckpointAttestationConfigurationError, match=message):
        load_checkpoint_attestors(deployment, deployment_name="sample")


@pytest.mark.parametrize("block", ["null", "{}"])
def test_loader_rejects_explicit_non_list_attestor_block(
    tmp_path: Path,
    block: str,
) -> None:
    deployment = tmp_path / "sample"
    deployment.mkdir()
    (deployment / "deployment.yaml").write_text(
        f"name: sample\ncheckpoint_attestors: {block}\n",
        encoding="utf-8",
    )

    with pytest.raises(
        CheckpointAttestationConfigurationError,
        match="checkpoint_attestors must be a list",
    ):
        load_checkpoint_attestors(deployment, deployment_name="sample")


def test_loader_rejects_non_mapping_manifest_document(tmp_path: Path) -> None:
    deployment = tmp_path / "sample"
    deployment.mkdir()
    (deployment / "deployment.yaml").write_text("[]\n", encoding="utf-8")

    with pytest.raises(
        CheckpointAttestationConfigurationError,
        match="deployment.yaml must be a mapping",
    ):
        load_checkpoint_attestors(deployment, deployment_name="sample")


def test_loader_rejects_recursive_alias_in_attestor_config(tmp_path: Path) -> None:
    deployment = tmp_path / "sample"
    deployment.mkdir()
    (deployment / "deployment.yaml").write_text(
        "name: sample\n"
        "checkpoint_attestors:\n"
        "  - id: sample.receipt\n"
        "    class: attestors.sample.SampleAttestor\n"
        "    config: &recursive\n"
        "      nested: *recursive\n",
        encoding="utf-8",
    )

    with pytest.raises(
        CheckpointAttestationConfigurationError,
        match="recursive YAML alias",
    ):
        load_checkpoint_attestors(deployment, deployment_name="sample")


@pytest.mark.parametrize(
    ("schema_body", "message"),
    [
        (
            "'type': 'object', 'properties': {}, 'additionalProperties': False",
            "must declare a unique required list",
        ),
        (
            "'type': 'object', 'properties': {'label': {}}, "
            "'required': [], 'additionalProperties': False",
            "properties must declare types",
        ),
        (
            "'type': 'object', 'properties': {}, 'required': [], "
            "'patternProperties': {'.*': {'type': 'string'}}, "
            "'additionalProperties': False",
            "must declare fixed keys",
        ),
        (
            "'type': 'object', 'properties': {'options': {'type': 'object'}}, "
            "'required': [], 'additionalProperties': False",
            "properties must be a mapping",
        ),
    ],
)
def test_loader_requires_fixed_typed_config_schema(
    tmp_path: Path,
    schema_body: str,
    message: str,
) -> None:
    deployment = tmp_path / "sample"
    package = deployment / "attestors"
    package.mkdir(parents=True)
    (package / "__init__.py").write_text("", encoding="utf-8")
    (package / "sample.py").write_text(
        "class SampleAttestor:\n"
        "    attestor_id = 'sample.receipt'\n"
        f"    config_schema = {{{schema_body}}}\n"
        "    def __init__(self, *, deployment_root): pass\n"
        "    def attest(self, context): raise NotImplementedError\n",
        encoding="utf-8",
    )
    (deployment / "deployment.yaml").write_text(
        "name: sample\n"
        "checkpoint_attestors:\n"
        "  - id: sample.receipt\n"
        "    class: attestors.sample.SampleAttestor\n"
        "    config: {}\n",
        encoding="utf-8",
    )

    with pytest.raises(CheckpointAttestationConfigurationError, match=message):
        load_checkpoint_attestors(deployment, deployment_name="sample")


def test_loader_accepts_recursively_closed_config_schema(tmp_path: Path) -> None:
    deployment = tmp_path / "sample"
    package = deployment / "attestors"
    package.mkdir(parents=True)
    (package / "__init__.py").write_text("", encoding="utf-8")
    (package / "sample.py").write_text(
        "class SampleAttestor:\n"
        "    attestor_id = 'sample.receipt'\n"
        "    config_schema = {\n"
        "        'type': 'object',\n"
        "        'properties': {'options': {\n"
        "            'type': 'object',\n"
        "            'properties': {'label': {'type': 'string'}},\n"
        "            'required': ['label'],\n"
        "            'additionalProperties': False,\n"
        "        }},\n"
        "        'required': [],\n"
        "        'additionalProperties': False,\n"
        "    }\n"
        "    def __init__(self, *, deployment_root): pass\n"
        "    def attest(self, context): raise NotImplementedError\n",
        encoding="utf-8",
    )
    (deployment / "deployment.yaml").write_text(
        "name: sample\n"
        "checkpoint_attestors:\n"
        "  - id: sample.receipt\n"
        "    class: attestors.sample.SampleAttestor\n"
        "    config: {}\n",
        encoding="utf-8",
    )

    loaded = load_checkpoint_attestors(deployment, deployment_name="sample")

    assert [item.manifest_id for item in loaded] == ["sample.receipt"]


def wiring_parity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.deployment_wiring import resolve_deployment_wiring_plan
    from okg.substrate.publisher import factory as publisher_factory
    from okg.substrate.publisher import publisher as publisher_module
    from okg.substrate.publisher.deployment_stack import (
        _clear_deployment_stack_cache,
        build_deployment_publisher_stack,
    )

    deployment = tmp_path / "sample"
    package = deployment / "attestors"
    package.mkdir(parents=True)
    (package / "__init__.py").write_text("", encoding="utf-8")
    (package / "sample.py").write_text(
        "class SampleAttestor:\n"
        "    attestor_id = 'sample.receipt'\n"
        "    config_schema = {\n"
        "        'type': 'object',\n"
        "        'properties': {'label': {'type': 'string'}},\n"
        "        'required': ['label'],\n"
        "        'additionalProperties': False,\n"
        "    }\n"
        "    def __init__(self, *, deployment_root, label):\n"
        "        self.deployment_root = deployment_root\n"
        "        self.label = label\n"
        "    def attest(self, context): raise NotImplementedError\n",
        encoding="utf-8",
    )
    (deployment / "deployment.yaml").write_text(
        "name: sample\n"
        "checkpoint_attestors:\n"
        "  - id: sample.receipt\n"
        "    class: attestors.sample.SampleAttestor\n"
        "    config:\n"
        "      label: pilot\n",
        encoding="utf-8",
    )
    _clear_deployment_stack_cache()
    stack = build_deployment_publisher_stack(
        dsn="postgresql://unused",
        deployment_name="sample",
        deployment_dir=deployment,
    )
    authority = object()
    authority_calls: list[dict[str, Any]] = []

    def _authority(**kwargs):
        authority_calls.append(kwargs)
        return authority

    class _Publisher:
        def __init__(self, _dsn, _deployment_name, **kwargs):
            self.checkpoint_attestors = tuple(kwargs["checkpoint_attestors"])
            self.catalog_identity = kwargs["catalog_identity"]

    monkeypatch.setattr(
        publisher_factory._deployment_stack,
        "build_runtime_catalog_identity",
        _authority,
    )
    monkeypatch.setattr(publisher_module, "Publisher", _Publisher)
    monkeypatch.setattr(
        "okg.substrate.runtime.release_authority.verify_worker_release_authority",
        lambda **_kwargs: None,
    )
    monkeypatch.setattr(
        "okg.substrate.publisher.repository_provenance.tracking_repository_provenance",
        lambda _path: __import__(
            "okg.substrate.publisher.repository_provenance", fromlist=["RepositoryProvenance"]
        ).RepositoryProvenance.exact(
            core_revision="1" * 40,
            deployment_revision="2" * 40,
            deployment_tree_sha="3" * 40,
        ),
    )
    publisher = publisher_factory.build_deployment_publisher(
        dsn="postgresql://unused",
        deployment_name="sample",
        deployment_dir=deployment,
        command="test wiring parity",
        stack=stack,
    )
    plan = resolve_deployment_wiring_plan(deployment, check_imports=True)

    assert [item.manifest_id for item in stack.checkpoint_attestors] == [
        "sample.receipt"
    ]
    assert [item.manifest_id for item in publisher.checkpoint_attestors] == [
        "sample.receipt"
    ]
    assert publisher.catalog_identity is authority
    assert authority_calls == [
        {
            "dsn": "postgresql://unused",
            "deployment_name": "sample",
            "deployment_dir": deployment,
            "command": "test wiring parity",
        }
    ]
    assert [item.component_id for item in plan.checkpoint_attestors] == [
        "sample.receipt"
    ]
    assert plan.checkpoint_attestors[0].import_ref == (
        "attestors.sample:SampleAttestor"
    )
    with pytest.raises(ValueError, match="cannot replace deployment authority"):
        publisher_factory.build_deployment_publisher(
            dsn="postgresql://unused",
            deployment_name="sample",
            deployment_dir=deployment,
            command="test forbidden override",
            stack=stack,
            publisher_overrides={"checkpoint_attestors": []},
        )
    with pytest.raises(ValueError, match="deployment name does not match"):
        publisher_factory.build_deployment_publisher(
            dsn="postgresql://unused",
            deployment_name="other",
            deployment_dir=deployment,
            command="test cross-deployment stack rejection",
            stack=stack,
        )
    with pytest.raises(ValueError, match="cannot replace deployment authority"):
        publisher_factory.build_deployment_publisher(
            dsn="postgresql://unused",
            deployment_name="sample",
            deployment_dir=deployment,
            command="test branch override rejection",
            stack=stack,
            publisher_overrides={"branch_name": "other"},
        )

    repository_root = Path(__file__).resolve().parents[3]
    product_publish_paths = (
        "src/okg/substrate/cli/main.py",
        "src/okg/substrate/cli/branch.py",
        "src/okg/substrate/ingest/service.py",
        "src/okg/substrate/runtime/publisher_workflow.py",
        "src/okg/substrate/deployment_bootstrap/workflow.py",
        "src/okg/substrate/db/bootstrap.py",
        "src/okg/substrate/enrichers/backfill.py",
        "src/okg/substrate/merge_back.py",
        "src/okg/substrate/mcp/connector_ingest.py",
        "src/okg/substrate/exchange/receiver.py",
        "src/okg/substrate/exchange/harness.py",
        "src/okg/substrate/exchange/hardening_harness.py",
    )
    for relative_path in product_publish_paths:
        source_path = repository_root / relative_path
        tree = ast.parse(source_path.read_text(encoding="utf-8"))
        direct_calls = [
            node.lineno
            for node in ast.walk(tree)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "Publisher"
        ]
        assert direct_calls == [], (
            f"{relative_path} bypasses the canonical publisher factory at "
            f"lines {direct_calls}"
        )


wiring_parity.__test__ = True


@pytest.mark.postgres
def atomic_postgres(
    clean_runtime: str,
) -> None:
    _seed_candidate(clean_runtime)
    attestor = _ContextAttestor()

    result = Publisher(
        clean_runtime,
        "default",
        checkpoint_attestors=[_loaded(attestor)],
        catalog_identity=bind_test_catalog_writer(clean_runtime, "default"),
    ).cycle()

    assert result["status"] == "published", result
    assert attestor.calls == 1
    envelope = result["deployment_attestations"]
    assert envelope["schema"] == "okg.deployment_attestations.v1"
    assert envelope["attestations"][0]["id"] == "tests.candidate"
    assert envelope["attestations"][0]["payload"]["native_generation_id"] == (
        result["generation_id"]
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        checkpoint = ChronosGraphStore.from_connection(
            conn, ensure_schema=False
        ).get_generation(result["generation_id"])
    assert checkpoint.metadata["deployment_attestations"] == envelope
    context = attestor.contexts[0]
    assert not hasattr(context, "__dict__")
    assert not hasattr(context, "_candidate_reader")
    assert all(
        not isinstance(getattr(context, slot), psycopg.Connection)
        for slot in context.__slots__
    )
    with pytest.raises(
        CheckpointAttestationRefused,
        match="attestation_unavailable",
    ):
        context.candidate_nodes()


@pytest.mark.postgres
def failure_and_retry(
    clean_runtime: str,
) -> None:
    _seed_candidate(clean_runtime)
    first = Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()
    assert first["status"] == "published", first

    refusing = _RefusingAttestor("attestation_unavailable")
    refused = Publisher(
        clean_runtime,
        "default",
        checkpoint_attestors=[_loaded(refusing)],
        catalog_identity=bind_test_catalog_writer(clean_runtime, "default"),
    ).cycle()
    assert refused["status"] == "blocked"
    assert refused["blocked_reason"] == {
        "reason": "checkpoint_attestation_failed",
        "attestor_id": "tests.refusing",
        "code": "attestation_unavailable",
        "detail": "receipt set is incomplete",
    }
    assert refused["deployment_attestations"] is None
    assert refusing.calls == 1

    exploded = Publisher(
        clean_runtime,
        "default",
        checkpoint_attestors=[_loaded(_ExplodingAttestor())],
        catalog_identity=bind_test_catalog_writer(clean_runtime, "default"),
    ).cycle()
    assert exploded["status"] == "blocked"
    assert exploded["blocked_reason"] == {
        "reason": "checkpoint_attestation_failed",
        "attestor_id": "tests.exploding",
        "code": "attestor_exception",
    }
    assert "password" not in str(exploded)

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        assert store.latest_generation(
            deployment_name="default", status="published"
        ).checkpoint_id == first["generation_id"]
        assert store.latest_generation(
            deployment_name="default", status=None
        ).checkpoint_id == exploded["generation_id"]


@pytest.mark.postgres
def metadata_only_reuse(
    clean_runtime: str,
) -> None:
    _seed_candidate(clean_runtime)
    attestor = _ContextAttestor()
    publisher = Publisher(
        clean_runtime,
        "default",
        checkpoint_attestors=[_loaded(attestor)],
        catalog_identity=bind_test_catalog_writer(clean_runtime, "default"),
    )

    first = publisher.cycle()
    second = publisher.cycle()

    assert first["status"] == second["status"] == "published"
    assert first["generation_id"] != second["generation_id"]
    assert second["noop"] is True
    assert second["noop_reuse_invalidation_reason"] == (
        "checkpoint_attestors_configured"
    )
    assert attestor.calls == 2


metadata_only_reuse.__test__ = True


@pytest.mark.postgres
def test_unattested_noop_preserves_reuse_and_metadata_absence(
    clean_runtime: str,
) -> None:
    _seed_candidate(clean_runtime)
    publisher = Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default"))

    first = publisher.cycle()
    second = publisher.cycle()

    assert first["status"] == "published", first
    assert second["reused_generation"] is True
    assert second["generation_id"] == first["generation_id"]
    assert "deployment_attestations" not in second
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        checkpoint = ChronosGraphStore.from_connection(
            conn,
            ensure_schema=False,
        ).get_generation(first["generation_id"])
    assert "deployment_attestations" not in checkpoint.metadata


@pytest.mark.postgres
def test_checkpoint_fault_exposes_no_published_successor(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _seed_candidate(clean_runtime)
    attestor = _ContextAttestor()

    def fail_checkpoint(*args: Any, **kwargs: Any) -> str:
        raise RuntimeError("checkpoint failpoint")

    monkeypatch.setattr(ChronosGraphStore, "checkpoint_generation", fail_checkpoint)
    with pytest.raises(RuntimeError, match="checkpoint failpoint"):
        Publisher(
            clean_runtime,
            "default",
            checkpoint_attestors=[_loaded(attestor)],
            catalog_identity=bind_test_catalog_writer(clean_runtime, "default"),
        ).cycle()

    assert attestor.calls == 1
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        assert store.list_generations(
            deployment_name="default", status="published"
        ) == []


@pytest.mark.postgres
def test_failed_attestation_preserves_typed_code_if_blocked_checkpoint_fails(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _seed_candidate(clean_runtime)
    refusing = _RefusingAttestor("attestation_invalid")

    def fail_checkpoint(*args: Any, **kwargs: Any) -> str:
        raise RuntimeError("blocked checkpoint failpoint")

    monkeypatch.setattr(ChronosGraphStore, "checkpoint_generation", fail_checkpoint)
    with pytest.raises(CheckpointAttestationFailure) as caught:
        Publisher(
            clean_runtime,
            "default",
            checkpoint_attestors=[_loaded(refusing)],
            catalog_identity=bind_test_catalog_writer(clean_runtime, "default"),
        ).cycle()

    assert caught.value.attestor_id == "tests.refusing"
    assert caught.value.code == "attestation_invalid"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        assert store.list_generations(deployment_name="default", status=None) == []


@pytest.mark.postgres
def test_attestor_does_not_run_after_caller_has_blocked_candidate(
    clean_runtime: str,
) -> None:
    _seed_candidate(clean_runtime)
    attestor = _ContextAttestor()
    from okg.substrate.publisher.repository_provenance import RepositoryProvenance
    provenance = RepositoryProvenance.exact(
        core_revision="a" * 40,
        deployment_revision="b" * 40,
        deployment_tree_sha="c" * 40,
    )

    result = Publisher(
        clean_runtime,
        "default",
        checkpoint_attestors=[_loaded(attestor)],
        catalog_identity=bind_test_catalog_writer(clean_runtime, "default"),
        repository_provenance=provenance,
    ).cycle(publish_blocker={"reason": "source_scope_incomplete"})

    assert result["status"] == "blocked"
    assert result["blocked_reason"] == {"reason": "source_scope_incomplete"}
    assert attestor.calls == 0
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        checkpoint = ChronosGraphStore.from_connection(
            conn, ensure_schema=False,
        ).get_generation(result["generation_id"])
    assert checkpoint.metadata["repository_provenance"] == provenance.to_dict()


@pytest.mark.postgres
def test_attestor_refusal_leaves_no_published_generation_ordinal(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Issue #1428: a blocked checkpoint must not occupy a published ordinal.

    The interval-backed publish path mints the branch-scoped generation ordinal
    while it syncs the effective-interval view. Running the attestors AFTER that
    left a refused checkpoint holding an ordinal nothing removed, so the next
    successful publish skipped that slot.
    """
    # The interval arm is the default, so this only defends against an ambient
    # OKG_IVM_INTERVAL_VIEW=0 in the runner environment. It matters because on
    # the NON-interval path ChronosGraphStore._maybe_mint_generation_ordinal
    # already declines to mint for a blocked status, so every assertion below
    # would pass on UNFIXED code and this test would cover nothing.
    monkeypatch.setenv("OKG_IVM_INTERVAL_VIEW", "1")

    def _publish(attestors: list[Any] | None = None) -> dict[str, Any]:
        return Publisher(
            clean_runtime,
            "default",
            checkpoint_attestors=attestors,
            catalog_identity=bind_test_catalog_writer(clean_runtime, "default"),
        ).cycle()

    def _ordinals() -> dict[str, int]:
        # Scoped to the branch under test: the exact-equality assertions below
        # must not start failing the day a cycle mints an ordinal on a second
        # branch (a quarantine fork, say) that has nothing to do with this test.
        with psycopg.connect(clean_runtime, autocommit=True) as conn, \
             conn.cursor() as cur:
            cur.execute(
                "SELECT checkpoint_id, ordinal FROM okg.generation_ordinals "
                "WHERE branch_id = %s ORDER BY ordinal",
                ("default",),
            )
            return {str(row[0]): int(row[1]) for row in cur.fetchall()}

    _seed_candidate(clean_runtime, node_id="attested:one")
    first = _publish()
    assert first["status"] == "published", first
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        first_checkpoint = ChronosGraphStore.from_connection(
            conn, ensure_schema=False
        ).get_generation(str(first["generation_id"]))
    # Discriminator, not decoration: only the interval-sync block writes
    # `effective_interval_view_prune`, so its presence proves this deployment
    # really takes the arm that mints the ordinal under test. Without it a
    # missing ordinal below could just mean the interval branch never ran.
    assert "effective_interval_view_prune" in first_checkpoint.metadata, (
        first_checkpoint.metadata.keys()
    )
    after_first = _ordinals()
    assert after_first == {str(first["generation_id"]): 0}, after_first

    _seed_candidate(clean_runtime, node_id="attested:two")
    refusing = _RefusingAttestor("attestation_invalid")
    blocked = _publish([_loaded(refusing)])
    assert blocked["status"] == "blocked", blocked
    assert refusing.calls == 1
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        assert store.latest_generation(
            deployment_name="default", status=None
        ).checkpoint_id == blocked["generation_id"]

    after_blocked = _ordinals()
    assert str(blocked["generation_id"]) not in after_blocked, after_blocked
    assert after_blocked == after_first, after_blocked

    _seed_candidate(clean_runtime, node_id="attested:three")
    second = _publish()
    assert second["status"] == "published", second
    after_second = _ordinals()
    assert after_second.get(str(second["generation_id"])) == 1, after_second
    assert sorted(after_second.values()) == [0, 1], after_second


@pytest.mark.postgres
def protocol(
    clean_runtime: str,
) -> None:
    _seed_candidate(clean_runtime)
    entered = threading.Event()
    release = threading.Event()
    writer_started = threading.Event()
    writer_done = threading.Event()
    attestor = _ContextAttestor(entered=entered, release=release)
    publish_result: dict[str, Any] = {}
    publish_error: list[BaseException] = []

    def publish() -> None:
        try:
            publish_result.update(Publisher(
                clean_runtime,
                "default",
                checkpoint_attestors=[_loaded(attestor)],
                catalog_identity=bind_test_catalog_writer(clean_runtime, "default"),
            ).cycle())
        except BaseException as exc:  # noqa: BLE001 - returned to test thread.
            publish_error.append(exc)

    def write() -> None:
        # Signalled only once connected, under a name the fence check below
        # can find in pg_stat_activity.
        with psycopg.connect(
            clean_runtime, autocommit=True, application_name=_FENCE_WRITER_APP
        ) as conn:
            writer_started.set()
            ChronosGraphStore.from_connection(conn).apply_facts(
                [NodeFact(
                    "attested:concurrent",
                    "page",
                    {"text": "concurrent"},
                    {"id": "attested:concurrent"},
                    {},
                )],
                source="attestation-test",
                strict_endpoints=True,
            )
        writer_done.set()

    publisher_thread = threading.Thread(target=publish, daemon=True)
    publisher_thread.start()
    assert entered.wait(timeout=_LOADED_RUNNER_WAIT_SECONDS), (
        "publisher never reached attestor"
    )
    writer_thread = threading.Thread(target=write, daemon=True)
    writer_thread.start()
    assert writer_started.wait(timeout=_LOADED_RUNNER_WAIT_SECONDS), (
        "branch writer thread never started"
    )
    # Prove the writer is actually waiting on the fence (the branch write
    # lock, a transaction-scoped advisory lock the publisher holds through the
    # attested checkpoint) before checking it has not finished: a writer that
    # has not yet sent its SQL would pass a bare "not done" check vacuously.
    _wait_until_writer_blocks_on_fence(clean_runtime, writer_done)
    assert not writer_done.wait(timeout=0.25), (
        "branch writer escaped the attested checkpoint fence"
    )
    release.set()
    publisher_thread.join(timeout=_LOADED_RUNNER_WAIT_SECONDS)
    writer_thread.join(timeout=_LOADED_RUNNER_WAIT_SECONDS)
    assert not publisher_thread.is_alive(), "publisher did not finish after release"
    assert not writer_thread.is_alive(), "branch writer did not finish after release"

    assert not publish_error
    assert publish_result["status"] == "published"
    assert writer_done.is_set()
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        pinned = store.checkout_generation(publish_result["generation_id"])
        assert pinned.query(
            "SELECT node_id FROM okg.graph_nodes "
            "WHERE node_id = 'attested:concurrent'"
        ) == []
        assert store.checkout().query(
            "SELECT node_id FROM okg.graph_nodes "
            "WHERE node_id = 'attested:concurrent'"
        ) == [{"node_id": "attested:concurrent"}]


# PACT binds these behavior-rich cases by their semantic JUnit names. Explicit
# ``__test__`` markers let pytest collect them without a redundant ``test_``
# prefix, so the executed report and the reviewed proof identities are exact.
atomic_postgres.__test__ = True
failure_and_retry.__test__ = True
protocol.__test__ = True


def test_attestor_module_origins_skip_placeholder_path_attributes(tmp_path: Path) -> None:
    """A module that answers every attribute with a placeholder has no origin.

    torch's ``torch.classes`` returns a truthy, non-iterable ``_ClassNamespace``
    for ``__file__`` and ``__path__``; scanning ``sys.modules`` after torch is
    imported used to raise ``TypeError`` here.
    """
    from okg.substrate.publisher import checkpoint_attestations as attestations

    class _Placeholder:
        def __bool__(self) -> bool:
            return True

    class _AnswersEverything(ModuleType):
        def __getattr__(self, name: str) -> _Placeholder:
            return _Placeholder()

    assert attestations._attestor_module_origins(_AnswersEverything("fake.classes")) == ()

    package = ModuleType("fake_pkg")
    package.__file__ = str(tmp_path / "fake_pkg" / "__init__.py")
    package.__path__ = [str(tmp_path / "fake_pkg")]
    assert attestations._attestor_module_origins(package) == (
        (tmp_path / "fake_pkg" / "__init__.py").resolve(),
        (tmp_path / "fake_pkg").resolve(),
    )
