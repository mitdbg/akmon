"""OpenRouter embedding backend and its HNSW index spec (PACT openrouter-embeddings-hnsw).

No database and no network: requests go to an ``httpx.MockTransport``, so the
`fast` lane (`not postgres`) runs this file with no lane registration. The
backend is exercised through its real request, validation, retry and
accounting code; only the HTTP peer is a stub.
"""
from __future__ import annotations

import json
import math

import httpx
import pytest

from okg.substrate.embeddings import EmbeddingConfig, build_embedding_execution_plan
from okg.substrate.embeddings.identity import (
    EMBEDDING_DIMENSION,
    build_embedder,
    embedder_model_id,
    model_id_for_spec,
    native_dimension_for_model_id,
    parse_openrouter_spec,
)
from okg.substrate.embeddings.openrouter import (
    API_KEY_ENV,
    MAX_INPUT_CHARS,
    OpenRouterEmbedder,
)
from okg.substrate.errors import SubstrateError
from okg.substrate.semantic_ann import ann_spec_for_model_id
from tests.substrate.openrouter_dns import openrouter_dns  # noqa: F401

pytestmark = pytest.mark.usefixtures("openrouter_dns")


MODEL = "google/gemini-embedding-2"
SPEC = f"openrouter:{MODEL}@768"
SECRET = "sk-or-test-secret-value"
#: The definition PostgreSQL 17 + pgvector 0.8.6 stores for the index this
#: change builds (read back with pg_get_indexdef on 2026-10-09).
STORED_INDEXDEF = (
    'CREATE INDEX idx_okg_chronos_x_node_embedding_hnsw ON public."Phys_T" '
    "USING hnsw (((subvector(embedding, 1, 768))::halfvec(768)) "
    "halfvec_cosine_ops) WITH (m='16', ef_construction='64') WHERE "
    "((deleted = false) AND (embedding_model = "
    "'openrouter:google/gemini-embedding-2@768'::text))"
)


def _vector(seed: int, width: int = 768) -> list[float]:
    raw = [math.sin(seed * 31 + i) for i in range(width)]
    norm = math.sqrt(sum(v * v for v in raw))
    return [v / norm for v in raw]


class Peer:
    """A scripted OpenRouter: one queued reply per request, every request kept."""

    def __init__(self, *replies) -> None:
        self.replies = list(replies)
        self.requests: list[httpx.Request] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        reply = self.replies.pop(0) if self.replies else "ok"
        if isinstance(reply, Exception):
            raise reply
        if isinstance(reply, httpx.Response):
            return reply
        body = json.loads(request.content)
        inputs = body["input"]
        data = [
            {"object": "embedding", "index": i, "embedding": _vector(len(t) + i)}
            for i, t in enumerate(inputs)
        ]
        if reply == "reversed":
            data.reverse()
        return httpx.Response(200, json={
            "object": "list",
            "model": "gemini-embedding-2",
            "provider": "Google AI Studio",
            "data": data,
            "usage": {
                "prompt_tokens": 10 * len(inputs),
                "total_tokens": 10 * len(inputs),
                "cost": 2e-6 * len(inputs),
            },
        })

    def payloads(self) -> list[dict]:
        return [json.loads(r.content) for r in self.requests]


def _embedder(peer: Peer, **kwargs) -> OpenRouterEmbedder:
    sleeps: list[float] = []
    embedder = OpenRouterEmbedder(
        model=MODEL,
        native_dimension=768,
        api_key=SECRET,
        transport=httpx.MockTransport(peer),
        sleep=sleeps.append,
        **kwargs,
    )
    embedder.sleeps = sleeps  # type: ignore[attr-defined]
    return embedder


# ── spec and identity ─────────────────────────────────────────────────


def test_spec_identity_encodes_model_and_width() -> None:
    assert model_id_for_spec(SPEC) == SPEC
    assert model_id_for_spec(f" openrouter:{MODEL} @ 0768 ") == SPEC
    assert parse_openrouter_spec(SPEC) == (MODEL, 768)
    ids = {
        model_id_for_spec(s) for s in (
            SPEC, f"openrouter:{MODEL}@1536", "openrouter:openai/text-embedding-3-small@768",
            "hash", "sentence-transformers:all-mpnet-base-v2",
        )
    }
    assert len(ids) == 5, "two models or two widths must never share an identity"
    assert native_dimension_for_model_id(SPEC) == 768
    assert native_dimension_for_model_id("hash:2560") is None


@pytest.mark.parametrize("spec", [
    f"openrouter:{MODEL}",          # no width
    f"openrouter:{MODEL}@",         # empty width
    "openrouter:@768",              # no model
    f"openrouter:{MODEL}@0",        # below 1
    f"openrouter:{MODEL}@2561",     # wider than the column
    f"openrouter:{MODEL}@seven",    # not an integer
    "openrouter:goo'gle/x@768",     # a character an index literal would need to escape
])
def test_malformed_openrouter_specs_are_refused(spec: str) -> None:
    with pytest.raises(SubstrateError) as exc:
        model_id_for_spec(spec)
    assert exc.value.code == "embedder_spec_invalid"


def test_built_embedder_reports_the_spec_identity(monkeypatch) -> None:
    monkeypatch.setenv(API_KEY_ENV, SECRET)
    embedder = build_embedder(SPEC)
    try:
        assert embedder_model_id(embedder) == model_id_for_spec(SPEC)
        assert embedder.dimension == EMBEDDING_DIMENSION
        assert SECRET not in repr(embedder)
    finally:
        embedder.close()


def test_missing_key_is_refused_without_a_request(monkeypatch) -> None:
    monkeypatch.delenv(API_KEY_ENV, raising=False)
    with pytest.raises(SubstrateError) as exc:
        build_embedder(SPEC)
    assert exc.value.code == "embedder_api_key_missing"
    assert exc.value.data["env_var"] == API_KEY_ENV


# ── requests and responses ────────────────────────────────────────────


def test_batches_split_by_count_and_characters_and_keep_input_order() -> None:
    peer = Peer("reversed", "ok", "reversed", "ok")
    embedder = _embedder(peer, batch_size=3, max_batch_chars=50, concurrency=3)
    texts = ["a" * 5, "b" * 6, "c" * 7, "d" * 8, "e" * 40, "f" * 9]
    vectors = embedder.embed_batch(texts)
    sizes = sorted(len(p["input"]) for p in peer.payloads())
    # [a,b,c] by count; [d,e] (48 chars); "f" would pass 50, so alone.
    assert sizes == [1, 2, 3]
    for payload in peer.payloads():
        assert payload["model"] == MODEL
        assert payload["dimensions"] == 768
    for request in peer.requests:
        assert request.headers["authorization"] == f"Bearer {SECRET}"
    assert len(vectors) == len(texts)
    for text, vector in zip(texts, vectors):
        # The vector for each text, whatever order the peer listed them in.
        payload = next(p for p in peer.payloads() if text in p["input"])
        i = payload["input"].index(text)
        expected = _vector(len(text) + i)
        assert vector[:768] == pytest.approx(expected, abs=1e-12)
        assert vector[768:] == [0.0] * (EMBEDDING_DIMENSION - 768)
        assert len(vector) == EMBEDDING_DIMENSION
        assert math.isclose(math.sqrt(sum(v * v for v in vector)), 1.0, rel_tol=1e-9)


def test_padding_leaves_cosine_unchanged() -> None:
    embedder = _embedder(Peer())
    a, b = embedder.embed_batch(["first text", "second, longer text"])
    native_a, native_b = a[:768], b[:768]
    assert sum(x * y for x, y in zip(a, b)) == pytest.approx(
        sum(x * y for x, y in zip(native_a, native_b)), abs=1e-12,
    )


def test_long_text_is_truncated_and_empty_text_refused() -> None:
    peer = Peer()
    embedder = _embedder(peer)
    embedder.embed("x" * (MAX_INPUT_CHARS + 500))
    assert len(peer.payloads()[0]["input"][0]) == MAX_INPUT_CHARS
    with pytest.raises(ValueError):
        embedder.embed("   ")
    assert len(peer.requests) == 1, "an empty text must never be sent"


@pytest.mark.parametrize("body, needle", [
    ({"model": "gemini-embedding-2", "data": []}, "0 vectors for 1 inputs"),
    ({"model": "gemini-embedding-2", "data": [{"index": 0, "embedding": [0.1] * 3072}]}, "width 3072"),
    ({"model": "some-other-model", "data": [{"index": 0, "embedding": [0.1] * 768}]}, "some-other-model"),
    ({"model": "gemini-embedding-2", "data": [{"index": 3, "embedding": [0.1] * 768}]}, "index 3"),
    ({"model": "gemini-embedding-2", "data": [{"index": 0, "embedding": [0.0] * 768}]}, "all-zero"),
])
def test_invalid_responses_raise_and_are_not_retried(body, needle) -> None:
    peer = Peer(httpx.Response(200, json=body))
    embedder = _embedder(peer)
    with pytest.raises(SubstrateError) as exc:
        embedder.embed("text")
    assert exc.value.code == "embedder_output_invalid"
    assert needle in str(exc.value)
    assert len(peer.requests) == 1


def test_non_finite_values_are_refused() -> None:
    content = (
        '{"model": "gemini-embedding-2", "data": [{"index": 0, "embedding": ['
        + ",".join(["NaN"] + ["0.1"] * 767) + "]}]}"
    ).encode()
    peer = Peer(httpx.Response(200, content=content))
    with pytest.raises(SubstrateError) as exc:
        _embedder(peer).embed("text")
    assert exc.value.code == "embedder_output_invalid"


def test_rate_limit_is_retried_honoring_retry_after() -> None:
    peer = Peer(
        httpx.Response(429, headers={"retry-after": "7"}, json={"error": {"message": "slow down"}}),
        httpx.ConnectError("reset"),
        "ok",
    )
    embedder = _embedder(peer)
    vector = embedder.embed("text")
    assert len(vector) == EMBEDDING_DIMENSION
    assert len(peer.requests) == 3
    assert embedder.sleeps[0] >= 7.0  # type: ignore[attr-defined]
    assert len(embedder.sleeps) == 2  # type: ignore[attr-defined]


def test_persistent_server_error_fails_loudly_after_the_attempt_budget() -> None:
    peer = Peer(*[httpx.Response(503, text="upstream down")] * 3)
    calls = []
    embedder = _embedder(peer, max_attempts=3, call_observer=calls.append)
    with pytest.raises(SubstrateError) as exc:
        embedder.embed("text")
    assert exc.value.code == "embedder_request_failed"
    assert exc.value.data["attempts"] == 3
    assert exc.value.data["status"] == 503
    assert len(peer.requests) == 3
    assert [c.success for c in calls] == [False]
    assert SECRET not in str(exc.value) and SECRET not in json.dumps(exc.value.data)


def test_client_error_is_not_retried() -> None:
    peer = Peer(httpx.Response(400, json={"error": {"code": 400, "message": "input too long"}}))
    embedder = _embedder(peer)
    with pytest.raises(SubstrateError) as exc:
        embedder.embed("text")
    assert exc.value.data["status"] == 400
    assert "input too long" in str(exc.value)
    assert len(peer.requests) == 1


def test_an_error_object_inside_a_200_is_an_error() -> None:
    peer = Peer(
        httpx.Response(200, json={"error": {"code": 502, "message": "provider hiccup"}}),
        "ok",
    )
    embedder = _embedder(peer)
    assert len(embedder.embed("text")) == EMBEDDING_DIMENSION
    assert len(peer.requests) == 2


def test_cost_and_tokens_reach_the_observer() -> None:
    calls = []
    embedder = _embedder(Peer(), call_observer=calls.append, batch_size=2)
    embedder.embed_batch(["a", "b", "c"])
    # The batches run on a thread pool, so the observer sees them in
    # completion order, not submission order.
    assert sorted(c.inputs for c in calls) == [1, 2]
    assert all(c.success and c.provider == "Google AI Studio" for c in calls)
    assert embedder.spent_usd == pytest.approx(6e-6)
    assert embedder.prompt_tokens == 30
    assert embedder.requests == 2


def test_execution_plan_batches_through_the_backend() -> None:
    """The plan's embed_batch makes the backend's batched calls, not one per text."""
    peer = Peer()
    plan = build_embedding_execution_plan(
        EmbeddingConfig(enabled=True, backend=SPEC),
        builder=lambda spec: _embedder(peer, batch_size=64),
    )
    probe_requests = len(peer.requests)
    vectors = plan.embed_batch([f"text {i}" for i in range(10)])
    assert len(vectors) == 10
    assert len(peer.requests) - probe_requests == 1
    assert plan.model_id == SPEC


# ── the HNSW index spec ───────────────────────────────────────────────


def test_only_narrow_models_get_an_ann_index() -> None:
    assert ann_spec_for_model_id("hash:2560") is None
    assert ann_spec_for_model_id("sentence-transformers:all-mpnet-base-v2") is None
    assert ann_spec_for_model_id(None) is None
    spec = ann_spec_for_model_id(SPEC)
    assert spec is not None and spec.native_dimension == 768
    assert "subvector(embedding, 1, 768)::halfvec(768)" in spec.index_body()
    assert "halfvec_cosine_ops" in spec.index_body()
    assert f"embedding_model = '{SPEC}'" in spec.index_body()


def test_index_definition_match_reads_the_stored_form() -> None:
    spec = ann_spec_for_model_id(SPEC)
    assert spec is not None
    assert spec.matches_definition(STORED_INDEXDEF)
    other_model = ann_spec_for_model_id(f"openrouter:{MODEL}@1536")
    assert other_model is not None and not other_model.matches_definition(STORED_INDEXDEF)
    assert not spec.matches_definition(STORED_INDEXDEF.replace("hnsw", "ivfflat"))


# ── review follow-ups ─────────────────────────────────────────────────


def test_a_charged_invalid_response_is_observed_with_its_cost() -> None:
    """The cost is settled before validation, so the cap counts it."""
    body = {
        "model": "gemini-embedding-2",
        "data": [],
        "usage": {"prompt_tokens": 9, "total_tokens": 9, "cost": 0.25},
    }
    calls = []
    embedder = _embedder(Peer(httpx.Response(200, json=body)), call_observer=calls.append)
    with pytest.raises(SubstrateError) as exc:
        embedder.embed("text")
    assert exc.value.code == "embedder_output_invalid"
    assert [(c.success, c.cost_usd, c.error_kind) for c in calls] == [
        (False, 0.25, "embedder_output_invalid"),
    ]
    assert embedder.spent_usd == pytest.approx(0.25)


def test_the_truncation_bound_is_part_of_the_identity() -> None:
    """`openrouter:<model>@<dims>` means texts cut at 24,000 characters.

    Changing the bound changes every long text's vector, so it must come
    with a new identity grammar; this pin makes that a deliberate edit.
    """
    assert MAX_INPUT_CHARS == 24_000


def test_only_the_openrouter_family_is_paid() -> None:
    from okg.substrate.embeddings.identity import is_paid_model_id

    assert is_paid_model_id(SPEC)
    assert not is_paid_model_id("hash:2560")
    assert not is_paid_model_id("sentence-transformers:all-mpnet-base-v2")
    assert not is_paid_model_id(None)


@pytest.mark.parametrize("version, supported", [
    (None, False), ("0.7.4", False), ("0.8.0", True), ("0.8.6", True), ("1.0", True),
])
def test_hnsw_needs_pgvector_0_8(version, supported) -> None:
    from okg.substrate.semantic_ann import _version_supported

    assert _version_supported(version) is supported


# ── round-3 review: paid backends outside the backfill ────────────────


def test_the_worker_does_not_register_a_paid_backend() -> None:
    from types import SimpleNamespace

    from okg.substrate.runtime.services import should_register_embed

    def manifest(**block):
        return SimpleNamespace(embedding_config=EmbeddingConfig(**block))

    assert should_register_embed(manifest(enabled=True, backend="hash")) is True
    assert should_register_embed(manifest(enabled=True, backend=SPEC)) is False
    assert should_register_embed(manifest(enabled=False)) is False


def test_an_unprobed_plan_makes_no_paid_call_and_cannot_embed() -> None:
    built = []
    plan = build_embedding_execution_plan(
        EmbeddingConfig(enabled=True, backend=SPEC),
        builder=lambda spec: built.append(spec),
        paid_backend="absent",
    )
    assert built == [], "no backend may be built, so no probe call is made"
    assert plan.enabled and not plan.can_embed
    assert plan.model_id == SPEC
    with pytest.raises(Exception, match="embed-backfill"):
        plan.embed("text")
    # A free backend is still built and probed.
    hash_plan = build_embedding_execution_plan(
        EmbeddingConfig(enabled=True, backend="hash"), paid_backend="absent",
    )
    assert hash_plan.can_embed


def test_alias_resolution_skips_a_paid_backend(tmp_path) -> None:
    import json as _json
    from pathlib import Path

    from okg.substrate.alias.loader import load_alias_resolver
    from tests.substrate.mcp.search_fixture import make_search_deployment

    deployment = Path(make_search_deployment(tmp_path, dsn="postgres://u@localhost/x"))
    manifest = _json.loads((deployment / "deployment.yaml").read_text())
    manifest["embeddings"] = {"enabled": True, "backend": SPEC}
    (deployment / "deployment.yaml").write_text(_json.dumps(manifest))
    plan = build_embedding_execution_plan(
        EmbeddingConfig(enabled=True, backend=SPEC), paid_backend="absent",
    )
    resolver = load_alias_resolver(deployment, conn=object(), embedding_plan=plan)
    assert [type(b).__name__ for b in resolver._backends] == ["SqlCascadeBackend"]


def test_only_the_dev_shm_resize_error_triggers_the_serial_rebuild() -> None:
    from okg.substrate.semantic_ann import _is_shared_memory_error

    assert _is_shared_memory_error(RuntimeError(
        'could not resize shared memory segment "/PostgreSQL.1" to 1073741824 bytes'
    ))
    assert not _is_shared_memory_error(RuntimeError("out of shared memory"))


def test_an_invalid_answer_counts_as_answered() -> None:
    body = {"model": "gemini-embedding-2", "data": []}  # no usage, no cost
    calls = []
    embedder = _embedder(Peer(httpx.Response(200, json=body)), call_observer=calls.append)
    with pytest.raises(SubstrateError):
        embedder.embed("text")
    assert [(c.answered, c.success, c.cost_usd) for c in calls] == [(True, False, None)]


# ── round-3 re-review: which calls spend ──────────────────────────────


def test_an_absent_paid_plan_names_why_it_has_no_dimension() -> None:
    plan = build_embedding_execution_plan(
        EmbeddingConfig(enabled=True, backend=SPEC), paid_backend="absent",
    )
    with pytest.raises(Exception, match="paid backend") as exc:
        plan.dimension
    assert "disabled" not in str(exc.value)


def test_a_no_probe_plan_spends_only_on_the_query() -> None:
    peer = Peer()
    plan = build_embedding_execution_plan(
        EmbeddingConfig(enabled=True, backend=SPEC),
        builder=lambda _spec: _embedder(peer),
        paid_backend="no_probe",
    )
    assert peer.requests == [], "building the plan makes no paid call"
    assert plan.can_embed and plan.dimension == EMBEDDING_DIMENSION
    assert len(plan.embed("the query")) == EMBEDDING_DIMENSION
    assert len(peer.requests) == 1


def test_the_backfill_plan_captures_its_probe_call(monkeypatch) -> None:
    import okg.substrate.embeddings.identity as identity
    from okg.substrate.embeddings.backfill import build_set_plan
    from okg.substrate.embeddings.config import VectorSetConfig

    peer = Peer()
    # A set's backend is unpadded (PACT vector-set-pgvector-widths).
    monkeypatch.setattr(
        identity, "build_embedder",
        lambda _spec, **_kwargs: _embedder(peer, pad_to_column=False),
    )
    plan, probe_calls = build_set_plan(VectorSetConfig(name="main", backend=SPEC))
    assert plan.observed_dimension == 768
    assert len(peer.requests) == 1
    assert [(c.success, c.cost_usd) for c in probe_calls] == [(True, pytest.approx(2e-6))]
    assert plan.embedder.call_observer is None


class _Recorded(Exception):
    pass


@pytest.fixture
def plan_kwargs(monkeypatch):
    """Replace the plan builder with one that records how it was called."""
    seen: list[dict] = []

    def record(config, **kwargs):
        seen.append({"config": config, **kwargs})
        raise _Recorded

    monkeypatch.setattr("okg.substrate.embeddings.build_embedding_execution_plan", record)
    return seen


def _paid_deployment(tmp_path):
    import json as _json
    from pathlib import Path as _Path

    from tests.substrate.mcp.search_fixture import make_search_deployment

    deployment = _Path(make_search_deployment(tmp_path, dsn="postgres://u@localhost/x"))
    manifest = _json.loads((deployment / "deployment.yaml").read_text())
    manifest["embeddings"] = {"enabled": True, "backend": SPEC}
    (deployment / "deployment.yaml").write_text(_json.dumps(manifest))
    return deployment


def test_the_worker_and_the_publisher_build_no_paid_backend(tmp_path, plan_kwargs) -> None:
    from okg.substrate.publisher.deployment_stack import build_deployment_publisher_stack
    from okg.substrate.runtime.worker import _preflight_embedding_plan

    deployment = _paid_deployment(tmp_path)
    with pytest.raises(_Recorded):
        _preflight_embedding_plan(str(deployment))
    with pytest.raises(_Recorded):
        build_deployment_publisher_stack(
            dsn="postgres://u@localhost/x", deployment_name=str(deployment),
            deployment_dir=deployment,
        )
    assert [k.get("paid_backend") for k in plan_kwargs] == ["absent", "absent"]


def test_the_mcp_server_builds_its_query_backend_without_a_probe(
    tmp_path, plan_kwargs,
) -> None:
    from okg.substrate.deployments import resolve_deployment
    from okg.substrate.mcp.deployment_entry import _verified_embedder_for_manifest

    manifest = resolve_deployment(str(_paid_deployment(tmp_path)))
    with pytest.raises(_Recorded):
        _verified_embedder_for_manifest(manifest)
    assert [k.get("paid_backend") for k in plan_kwargs] == ["no_probe"]


# ── egress: every request passes the central guard ────────────────────


@pytest.mark.parametrize("base_url, reason", [
    ("http://127.0.0.1:9/api/v1", "private"),  # loopback, not on the allowlist
    ("file:///etc/passwd", "non-http"),
    ("https://api.other.example/api/v1", "origin"),  # public, but not OpenRouter
])
def test_the_egress_policy_refuses_the_call_before_any_request(base_url, reason) -> None:
    peer = Peer()
    embedder = _embedder(peer, base_url=base_url)
    with pytest.raises(SubstrateError) as exc:
        embedder.embed("text")
    assert exc.value.code == "embedder_egress_refused", reason
    assert peer.requests == [], "the key must not be sent anywhere"
    assert SECRET not in str(exc.value)


def test_the_operator_egress_allowlist_is_honored(monkeypatch) -> None:
    """The posture is the central guard's: an operator exception for a
    private host lets the call through (and is logged), nothing else does."""
    monkeypatch.setenv("OKG_EGRESS_ALLOW_HOSTS", "127.0.0.1")
    peer = Peer()
    embedder = _embedder(peer, base_url="http://127.0.0.1:9/api/v1")
    assert len(embedder.embed("text")) == EMBEDDING_DIMENSION
    assert len(peer.requests) == 1


def test_a_redirect_is_not_followed() -> None:
    peer = Peer(httpx.Response(307, headers={"location": "http://169.254.169.254/"}))
    embedder = _embedder(peer)
    with pytest.raises(SubstrateError) as exc:
        embedder.embed("text")
    assert exc.value.code == "embedder_request_failed"
    assert exc.value.data["status"] == 307
    assert len(peer.requests) == 1


# ── semantic search over a vector set: the query embedding (no DB) ────


def test_a_hung_query_embedding_is_a_clear_refusal_within_two_timeouts(
    monkeypatch,
) -> None:
    """The production wiring end to end: the deployment block's paid search
    set, built by `build_embedding_execution_plan(..., paid_backend=
    "no_probe")` with its default builder, gets the search-path client (5 s,
    2 attempts, the set's provider pin). The network transport is replaced
    by one that HANGS for the read timeout the client itself put on the
    request, then times out, as a peer that never answers does. The search
    refuses with `semantic_query_embedding_failed` after two attempts, in
    under 12 s, and ranks nothing. The database steps are stubbed; the
    refusal comes before any of them."""
    import importlib
    import threading
    import time
    from types import SimpleNamespace

    import okg.substrate.embeddings.openrouter as openrouter
    import okg.substrate.semantic_ann as semantic_ann
    # `okg.substrate.mcp.search` the attribute is the function; the module:
    search_module = importlib.import_module("okg.substrate.mcp.search")
    from okg.substrate.embeddings.config import parse_embedding_config

    hung: list[float] = []

    def hang(self, request):
        read_timeout = float(request.extensions["timeout"]["read"])
        hung.append(read_timeout)
        threading.Event().wait(read_timeout)
        raise httpx.ReadTimeout("no answer", request=request)

    monkeypatch.setenv("OPENROUTER_API_KEY", SECRET)
    monkeypatch.setattr(httpx.HTTPTransport, "handle_request", hang)
    # Only the jittered pause between the two attempts is shortened.
    monkeypatch.setattr(openrouter, "_BACKOFF_BASE_SECONDS", 0.05)
    block = {"enabled": True, "sets": {"main": {
        "backend": SPEC, "provider": "google-vertex",
    }}}
    plan = build_embedding_execution_plan(
        parse_embedding_config(plural=block), paid_backend="no_probe",
    )
    assert plan.embedder.client_policy == {
        "timeout_seconds": 5.0, "max_attempts": 2, "provider": "google-vertex",
    }
    vector_set = plan.config.search_vector_set
    route = search_module._VectorSetRoute(vector_set, vector_set, "active", ())
    monkeypatch.setattr(
        semantic_ann, "prepare_set_read",
        lambda *a, **k: SimpleNamespace(embedded_nodes=None),
    )
    monkeypatch.setattr(search_module, "_reader_predicate_sql", lambda conn, s: None)

    def no_ranking(*_a, **_k):
        raise AssertionError("ranked without a query vector")

    monkeypatch.setattr(semantic_ann, "rank_vector_set", no_ranking)
    started = time.monotonic()
    with pytest.raises(SubstrateError) as caught:
        search_module._search_semantic_vector_set(
            None, SimpleNamespace(generation_id="g"), "the query",
            route=route, embedder=plan, embedder_spec=None, embedder_source=None,
            filter={}, limit=5, offset=0, select=None, scope=None,
            scoped_subtypes=None, allow_unverified_provenance=False,
            max_chars=200,
        )
    elapsed = time.monotonic() - started
    assert caught.value.code == "semantic_query_embedding_failed"
    assert caught.value.data["attempts"] == 2
    assert hung == [5.0, 5.0], "each attempt waited out the client's own 5 s timeout"
    assert 10.0 <= elapsed < 12.0, elapsed
    assert SECRET not in str(caught.value)


def test_the_query_vector_is_cut_to_the_set_width_and_its_padding_checked() -> None:
    from okg.substrate.embeddings.config import VectorSetConfig
    from okg.substrate.semantic_ann import query_vector_for_set

    vector_set = VectorSetConfig(name="main", backend=SPEC)
    assert vector_set.width == 768
    native = _vector(3)
    assert query_vector_for_set(vector_set, native + [0.0] * 1792) == pytest.approx(native)
    with pytest.raises(SubstrateError) as caught:
        query_vector_for_set(vector_set, native + [0.5] + [0.0] * 1791)
    assert caught.value.code == "embedder_dim_mismatch"
    with pytest.raises(SubstrateError):
        query_vector_for_set(vector_set, native[:100])
