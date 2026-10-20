"""Embedding pipeline after legacy okg.jobs retirement."""
from __future__ import annotations

import json
import uuid
from types import SimpleNamespace

import psycopg
import pytest

from okg.substrate.embeddings import HashEmbedder, embedder_model_id
from okg.substrate.runtime.embed_workflow import (
    _embed_and_write_one,
    _run_one_embed_batch,
)


from tests.substrate.openrouter_dns import openrouter_dns  # noqa: E402,F401

pytestmark = [pytest.mark.postgres, pytest.mark.usefixtures("openrouter_dns")]


def _insert_node_facts(
    conn: psycopg.Connection,
    nodes: list[tuple[str, str, str]],
) -> None:
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.library.sources.base import NodeFact

    run_id = str(uuid.uuid4())
    with conn.cursor() as cur:
        for nid, subtype, text in nodes:
            cur.execute(
                """
                INSERT INTO okg.node_facts
                  (node_id, subtype, attrs, source, source_record_id,
                   source_revision, observed_at, dedupe_key, run_id, op)
                VALUES (%s, %s, %s::jsonb, 'fixture', %s::jsonb,
                        %s::jsonb, now(), %s, %s, 'I')
                """,
                (
                    nid, subtype, json.dumps({"text": text}),
                    json.dumps({"id": nid}),
                    json.dumps({"v": 1}),
                    f"{run_id}:{nid}:{uuid.uuid4().hex[:8]}",
                    run_id,
                ),
            )
    store = ChronosGraphStore.from_connection(conn)
    store.apply_facts(
        [
            NodeFact(
                node_id=nid,
                subtype=subtype,
                attrs={"text": text},
                source_record_id={"id": nid},
                source_revision={"v": 1},
            )
            for nid, subtype, text in nodes
        ],
        source="fixture",
        run_id=run_id,
    )


def _text_hash(dsn: str, node_id: str) -> str:
    from okg.substrate.graph_store import ChronosGraphStore

    store = ChronosGraphStore.connect(dsn, ensure_schema=False)
    try:
        row = store.checkout().query(
            """
            SELECT text_hash
              FROM okg.graph_nodes
             WHERE node_id = :node_id
            """,
            {"node_id": node_id},
        )[0]
        return str(row["text_hash"])
    finally:
        store.close()


def test_embed_step_rejects_dimension_mismatch() -> None:
    from okg.substrate.embeddings import (
        EmbeddingConfig,
        build_embedding_execution_plan,
    )

    with pytest.raises(Exception) as exc:
        build_embedding_execution_plan(
            EmbeddingConfig(enabled=True, backend="hash"),
            builder=lambda _spec: HashEmbedder(dimension=128),
        )

    assert getattr(exc.value, "code", None) == "embedder_dim_mismatch"




def test_embed_step_writes_embedding(clean_runtime: str) -> None:
    from okg.substrate.embeddings import (
        EmbeddingConfig,
        build_embedding_execution_plan,
    )
    plan = build_embedding_execution_plan(
        EmbeddingConfig(enabled=True, backend="hash")
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _insert_node_facts(conn, [("n:a", "page", "hello world")])
    text_hash = _text_hash(clean_runtime, "n:a")

    outcome = _embed_and_write_one(
        dsn=clean_runtime,
        node_id="n:a",
        text="hello world",
        text_hash=text_hash,
        embedding_plan=plan,
    )

    assert outcome["cache_hit"] is False
    from okg.substrate.graph_store import ChronosGraphStore

    store = ChronosGraphStore.connect(clean_runtime, ensure_schema=False)
    try:
        row = store.checkout().query(
            """
            SELECT embedding IS NOT NULL AS has_embedding,
                   embedding_model
              FROM okg.graph_nodes
             WHERE node_id = 'n:a'
            """
        )[0]
    finally:
        store.close()
    assert row["has_embedding"] is True
    # Derived from the embedder, not spelled out: the recorded identity carries
    # the MODEL (`hash:2560`), not the class name. The old class-derived form
    # (`HashEmbedder:2560`) was not injective — every sentence-transformer at
    # the substrate width collapsed to one string — so semantic search could
    # not tell two models apart (PACT reachable-semantic-search).
    assert row["embedding_model"] == embedder_model_id(HashEmbedder())


def test_embed_batch_scans_graph_without_legacy_queue(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.embeddings import (
        EmbeddingConfig,
        build_embedding_execution_plan,
    )
    plan = build_embedding_execution_plan(
        EmbeddingConfig(enabled=True, backend="hash")
    )
    manifest = SimpleNamespace(embedding_config=plan.config)
    monkeypatch.setattr(
        "okg.substrate.deployments.resolve_deployment",
        lambda _deployment: manifest,
    )
    monkeypatch.setattr(
        "okg.substrate.deployments.require_postgres_dsn",
        lambda _manifest, *, deployment: clean_runtime,
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _insert_node_facts(conn, [
            ("n:a", "page", "alpha"),
            ("n:b", "page", "beta"),
        ])

    result = _run_one_embed_batch(
        deployment="default",
        batch_size=10,
        embed_one=lambda node_id, text, text_hash, _fingerprint: (
            _embed_and_write_one(
                dsn=clean_runtime,
                node_id=node_id,
                text=text,
                text_hash=text_hash,
                embedding_plan=plan,
            )
        ),
        embedding_plan=plan,
    )

    assert result["processed"] == 2
    assert result["errors"] == 0


# ── vector-set backfill with a paid backend (PACT openrouter-embeddings-hnsw) ──
# The real backfill and the real OpenRouter backend; only the HTTP peer is a
# stub (httpx.MockTransport), so no network call and no spend. The free-backend
# backfill cases (documents, links, lock, dry run, embed-show) are in
# tests/substrate/test_embedding_vector_sets_db.py.

OR_SPEC = "openrouter:google/gemini-embedding-2@768"
OR_SET_BLOCK = {
    "enabled": True,
    "sets": {
        "main": {
            "backend": OR_SPEC,
            "subtypes": ["chunk"],
            "document": {"default": {"header": [], "body": "text"}},
            "index": {"kind": "none"},
        }
    },
}


class _PricedPeer:
    """Answers every embeddings request, charging `usd_per_input` per text
    plus `usd_per_char` per input character."""

    def __init__(self, usd_per_input: float = 0.0, usd_per_char: float = 0.0) -> None:
        self.usd_per_input = usd_per_input
        self.usd_per_char = usd_per_char
        self.requests = 0
        self.charged: list[float] = []

    def __call__(self, request):
        import math

        import httpx

        self.requests += 1
        inputs = json.loads(request.content)["input"]
        data = []
        for i, text in enumerate(inputs):
            raw = [math.sin(len(text) * 3.1 + j * 0.7 + i) for j in range(768)]
            data.append({"index": i, "embedding": raw})
        cost = (
            self.usd_per_input * len(inputs)
            + self.usd_per_char * sum(len(text) for text in inputs)
        )
        self.charged.append(cost)
        return httpx.Response(200, json={
            "model": "gemini-embedding-2",
            "provider": "stub",
            "data": data,
            "usage": {
                "prompt_tokens": 5 * len(inputs),
                "total_tokens": 5 * len(inputs),
                "cost": cost,
            },
        })


def _or_embedder(peer, *, pad_to_column: bool = False):
    """A vector set's OpenRouter backend (unpadded, PACT
    vector-set-pgvector-widths) unless ``pad_to_column``: the node column's."""
    import httpx

    from okg.substrate.embeddings.openrouter import OpenRouterEmbedder

    return OpenRouterEmbedder(
        model="google/gemini-embedding-2", native_dimension=768,
        api_key="sk-test", transport=httpx.MockTransport(peer),
        sleep=lambda _s: None, pad_to_column=pad_to_column,
    )


def _or_plan(peer):
    from okg.substrate.embeddings import (
        EmbeddingConfig,
        build_embedding_execution_plan,
    )

    return build_embedding_execution_plan(
        EmbeddingConfig(enabled=True, backend=OR_SPEC),
        builder=lambda _spec: _or_embedder(peer, pad_to_column=True),
    )


def _never_built(_spec):
    raise AssertionError("the backend was built, so a paid call was possible")


def _paid_set(dsn: str, tmp_path, *, count: int = 3):
    """A published graph of `count` distinct chunks under a registered paid
    set. Returns (deployment, embedding config, vector set)."""
    from pathlib import Path

    from okg.substrate.embeddings.config import parse_embedding_config
    from okg.substrate.embeddings.store import register_sets
    from tests.substrate.mcp.semantic_fixture import (
        make_search_deployment,
        node,
        publish_with_vectors,
    )

    deployment = make_search_deployment(tmp_path, dsn=dsn)
    path = Path(deployment) / "deployment.yaml"
    manifest = json.loads(path.read_text())
    manifest["embeddings"] = OR_SET_BLOCK
    path.write_text(json.dumps(manifest))
    config = parse_embedding_config(plural=OR_SET_BLOCK)
    with psycopg.connect(dsn) as conn:
        register_sets(conn, list(config.vector_sets))
        conn.commit()
    nodes = [node(f"chunk:{i}", "chunk", f"distinct passage {i}") for i in range(count)]
    publish_with_vectors(
        dsn, deployment, subtypes=["chunk"], nodes=nodes, vectors={}, model_id=None,
    )
    return deployment, config, config.search_vector_set


def _ledger(dsn: str) -> list[tuple]:
    from okg.substrate.embeddings.backfill import BACKFILL_CALLER

    with psycopg.connect(dsn, autocommit=True) as conn:
        return conn.execute(
            "SELECT success, cost_usd, error_kind, extras->>'set_id' "
            "FROM okg.llm_calls WHERE caller = %s ORDER BY id",
            (BACKFILL_CALLER,),
        ).fetchall()


def _vectors(dsn: str, set_id: str) -> int:
    with psycopg.connect(dsn, autocommit=True) as conn:
        return int(conn.execute(
            "SELECT count(*) FROM okg.text_embeddings WHERE set_id = %s", (set_id,),
        ).fetchone()[0])


def _ledger_row(dsn: str, deployment: str, set_id: str, *, success: bool,
                cost, error_kind=None) -> None:
    from okg.substrate.embeddings.backfill import BACKFILL_CALLER

    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(
            "INSERT INTO okg.llm_calls (deployment_name, caller, model, success, "
            "error_kind, cost_usd, extras) VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb)",
            (deployment, BACKFILL_CALLER, OR_SPEC, success, error_kind, cost,
             json.dumps({"set_id": set_id})),
        )


def test_backfill_caps_spend_per_set_and_refuses_before_any_paid_call(
    clean_runtime: str, tmp_path,
) -> None:
    from okg.substrate.embeddings.backfill import run_set_backfill

    from okg.substrate.embeddings.config import EMBEDDING_PROBE_TEXT

    deployment, config, vector_set = _paid_set(clean_runtime, tmp_path, count=6)
    # Priced per character, as a token-priced provider roughly is: the probe
    # costs 0.33 and each 18-character document 0.18.
    peer = _PricedPeer(usd_per_char=0.01)
    doc_chars = len("distinct passage 0")
    assert len(EMBEDDING_PROBE_TEXT) == 33

    def run(max_usd, builder):
        return run_set_backfill(
            clean_runtime, deployment=deployment, config=config, set_name="main",
            max_usd=max_usd, max_window_chars=2 * doc_chars, builder=builder,
        )

    first = run(1.0, lambda _spec: _or_embedder(peer))
    # The probe (0.33), a batch of two documents (0.36), then a batch cut to
    # the one document that still fits (0.18): 0.87. A fourth document would
    # bring it to 1.05, so the run stops before sending it. Must-fail: the
    # earlier rule (stop once spend >= cap, batches never cut) spent the
    # probe plus two full batches, 1.05.
    assert first["status"] == "cap_reached"
    assert peer.charged == pytest.approx([0.33, 0.36, 0.18])
    assert first["documents_embedded"] == 3
    assert first["run_cost_usd"] == pytest.approx(0.87)
    assert first["prior_cost_usd"] + first["run_cost_usd"] <= 1.0
    assert first["node_rows_written"] == 0
    assert _vectors(clean_runtime, vector_set.set_id) == 3
    rows = _ledger(clean_runtime)
    assert len(rows) == 3 and {r[3] for r in rows} == {vector_set.set_id}

    # The earlier spend counts, and so does the price this database measured:
    # under the same cap the probe alone (0.33 on top of 0.87) would pass it,
    # so the run refuses before the backend is even built.
    second = run(1.0, _never_built)
    assert second["status"] == "cap_reached"
    assert second["prior_cost_usd"] == pytest.approx(0.87)
    assert second["documents_embedded"] == 0 and second["requests"] == 0

    # The ledger is keyed by set: another set's spend does not count here.
    _ledger_row(clean_runtime, deployment, "es_" + "0" * 24, success=True, cost=100.0)
    third = run(5.0, lambda _spec: _or_embedder(peer))
    assert third["status"] == "completed"
    assert third["prior_cost_usd"] == pytest.approx(0.87)
    assert third["documents_embedded"] == 3
    assert _vectors(clean_runtime, vector_set.set_id) == 6


def test_must_fail_the_cap_estimate_uses_the_highest_measured_price_per_character(
    clean_runtime: str, tmp_path,
) -> None:
    """Two earlier priced calls of the model: 1,000 characters for $0.01 and
    100 characters for $0.10. Their average is $0.0001 a character, the
    highest $0.001. With $0.02 of the cap left, the 33-character probe fits
    at the average ($0.0033) but not at the highest ($0.033), so the run
    refuses before building the backend. The earlier estimate used the
    average and made the probe call."""
    from okg.substrate.embeddings.backfill import (
        BACKFILL_CALLER,
        _measured_rates,
        run_set_backfill,
    )

    deployment, config, vector_set = _paid_set(clean_runtime, tmp_path, count=2)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        for cost, chars in ((0.01, 1000), (0.10, 100)):
            conn.execute(
                "INSERT INTO okg.llm_calls (deployment_name, caller, model, success, "
                "prompt_tokens, cost_usd, extras) "
                "VALUES (%s, %s, %s, TRUE, %s, %s, %s::jsonb)",
                (deployment, BACKFILL_CALLER, vector_set.model_id, chars // 4, cost,
                 json.dumps({"set_id": vector_set.set_id, "input_chars": chars})),
            )
        rates = _measured_rates(conn, vector_set.model_id)
    assert rates["max_usd_per_char"] == pytest.approx(0.001)
    assert rates["usd_per_token"] / rates["chars_per_token"] == pytest.approx(0.0001)
    report = run_set_backfill(
        clean_runtime, deployment=deployment, config=config, set_name="main",
        max_usd=0.13, builder=_never_built,
    )
    assert report["status"] == "cap_reached"
    assert report["prior_cost_usd"] == pytest.approx(0.11)
    assert report["requests"] == 0 and report["documents_embedded"] == 0


def test_backfill_refuses_a_paid_set_without_a_cap(clean_runtime: str, tmp_path) -> None:
    from okg.substrate.embeddings.backfill import run_set_backfill
    from okg.substrate.errors import SubstrateError

    deployment, config, _vs = _paid_set(clean_runtime, tmp_path, count=1)
    with pytest.raises(SubstrateError) as caught:
        run_set_backfill(
            clean_runtime, deployment=deployment, config=config, set_name="main",
            max_usd=None, builder=_never_built,
        )
    assert caught.value.code == "embedding_spend_cap_required"


def test_backfill_refuses_an_uncosted_earlier_charge_of_the_set(
    clean_runtime: str, tmp_path,
) -> None:
    """An answered earlier call with no cost makes the set's spend unknown:
    the run refuses before any call. A call never answered was not charged."""
    from okg.substrate.embeddings.backfill import run_set_backfill
    from okg.substrate.errors import SubstrateError

    deployment, config, vector_set = _paid_set(clean_runtime, tmp_path, count=1)
    _ledger_row(clean_runtime, deployment, vector_set.set_id, success=False,
                cost=None, error_kind="http_503")
    peer = _PricedPeer(usd_per_input=1e-6)
    ok = run_set_backfill(
        clean_runtime, deployment=deployment, config=config, set_name="main",
        max_usd=1.0, builder=lambda _spec: _or_embedder(peer),
    )
    assert ok["prior_cost_basis"] == "ledger" and ok["documents_embedded"] == 1

    _ledger_row(clean_runtime, deployment, vector_set.set_id, success=True, cost=None)
    with pytest.raises(SubstrateError) as caught:
        run_set_backfill(
            clean_runtime, deployment=deployment, config=config, set_name="main",
            max_usd=1.0, builder=_never_built,
        )
    assert caught.value.code == "embedding_cost_unreported"
    assert caught.value.data["answered_uncosted_calls"] == 1


def test_backfill_records_a_charged_invalid_response_and_stops(
    clean_runtime: str, tmp_path,
) -> None:
    import httpx

    from okg.substrate.embeddings.backfill import run_set_backfill
    from okg.substrate.errors import SubstrateError

    deployment, config, vector_set = _paid_set(clean_runtime, tmp_path)
    good = _PricedPeer(usd_per_input=0.01)
    calls = {"n": 0}

    def peer(request):
        calls["n"] += 1
        if calls["n"] == 1:  # the capability probe succeeds
            return good(request)
        return httpx.Response(200, json={
            "model": "gemini-embedding-2", "data": [],
            "usage": {"prompt_tokens": 7, "total_tokens": 7, "cost": 0.05},
        })

    with pytest.raises(SubstrateError) as caught:
        run_set_backfill(
            clean_runtime, deployment=deployment, config=config, set_name="main",
            max_usd=1.0, builder=lambda _spec: _or_embedder(peer),
        )
    assert caught.value.code == "embedder_output_invalid"
    rows = _ledger(clean_runtime)
    assert [(r[0], float(r[1]), r[2]) for r in rows] == [
        (True, 0.01, None), (False, 0.05, "embedder_output_invalid"),
    ]
    assert _vectors(clean_runtime, vector_set.set_id) == 0


def test_backfill_fails_when_its_spend_cannot_be_recorded(
    clean_runtime: str, tmp_path,
) -> None:
    """The ledger insert fails after a paid request answered (the probe):
    the run fails loudly and writes no vector."""
    from okg.substrate.embeddings.backfill import BACKFILL_CALLER, run_set_backfill

    deployment, config, vector_set = _paid_set(clean_runtime, tmp_path)
    peer = _PricedPeer(usd_per_input=1e-6)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        conn.execute(
            """
            CREATE FUNCTION okg.test_refuse_backfill_ledger() RETURNS trigger
            LANGUAGE plpgsql AS $$
            BEGIN
              IF NEW.caller = 'embeddings.backfill' THEN
                RAISE EXCEPTION 'ledger write refused by test';
              END IF;
              RETURN NEW;
            END $$;
            CREATE TRIGGER test_refuse_backfill_ledger
              BEFORE INSERT ON okg.llm_calls
              FOR EACH ROW EXECUTE FUNCTION okg.test_refuse_backfill_ledger();
            """
        )
    with pytest.raises(psycopg.Error, match="ledger write refused"):
        run_set_backfill(
            clean_runtime, deployment=deployment, config=config, set_name="main",
            max_usd=1.0, builder=lambda _spec: _or_embedder(peer),
        )
    assert peer.requests >= 1, "the failure came after a paid request"
    assert _vectors(clean_runtime, vector_set.set_id) == 0
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        count = conn.execute(
            "SELECT count(*) FROM okg.llm_calls WHERE caller = %s", (BACKFILL_CALLER,),
        ).fetchone()[0]
    assert count == 0


def test_dry_run_of_a_paid_set_estimates_without_a_call_or_a_write(
    clean_runtime: str, tmp_path, monkeypatch,
) -> None:
    import okg.substrate.embeddings.identity as identity
    from okg.substrate.embeddings.backfill import BACKFILL_CALLER, dry_run_backfill

    deployment, config, vector_set = _paid_set(clean_runtime, tmp_path, count=3)
    monkeypatch.setattr(identity, "build_embedder", _never_built)

    def state() -> tuple:
        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            return conn.execute(
                "SELECT (SELECT count(*) FROM okg.text_embeddings), "
                "(SELECT count(*) FROM okg.node_documents), "
                "(SELECT count(*) FROM okg.llm_calls), "
                "(SELECT next_seq FROM okg.embedding_sets WHERE set_id = %s)",
                (vector_set.set_id,),
            ).fetchone()

    before = state()
    (report,) = dry_run_backfill(
        clean_runtime, deployment=deployment, config=config, set_name=None,
        max_usd=2.0,
    )["sets"]
    assert state() == before
    chars = 3 * len("distinct passage 0")
    assert report["documents_missing"] == 3 and report["links_missing"] == 0
    assert report["characters_missing"] == chars
    assert report["by_subtype"]["chunk"]["documents_missing"] == 3
    assert report["estimated_tokens"] == round(chars / 4)
    assert report["token_basis"].startswith("assumed 4 characters per token")
    # Review P10 F1: the basis says no rate was measured, not that no call
    # of the model exists (a call without counts or a cost is not priced).
    assert "no measured rate (this database has no priced backfill call" in (
        report["token_basis"]
    )
    # No price table in the code and no priced call recorded: said, not guessed.
    assert report["estimated_usd"] is None
    assert report["price_source"].startswith("none:")
    assert report["max_usd"] == 2.0 and report["cap_reached"] is False

    # Review P9 (dry run): the ledger's measured rates for the model are
    # used, dollars per input character. The C4 fill measured 2.38
    # characters per token, so pricing estimated tokens at 4 characters
    # each was 1.7x low. Must-fail: dollars were the estimated tokens times
    # the measured price per token, and the basis did not name the calls.
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        for _ in range(2):
            conn.execute(
                "INSERT INTO okg.llm_calls (deployment_name, caller, model, "
                "success, prompt_tokens, total_tokens, cost_usd, extras) "
                "VALUES (%s, %s, %s, TRUE, 100, 100, 0.001, %s::jsonb)",
                (deployment, BACKFILL_CALLER, OR_SPEC,
                 json.dumps({"set_id": vector_set.set_id, "input_chars": 238})),
            )
    (measured,) = dry_run_backfill(
        clean_runtime, deployment=deployment, config=config, set_name="main",
    )["sets"]
    assert measured["estimated_tokens"] == round(chars / 2.38)
    assert measured["estimated_usd"] == pytest.approx(round(chars * 0.001 / 238, 6))
    assert measured["token_basis"].startswith("measured from 2 call(s): 2.38")
    assert measured["price_source"].startswith("measured from 2 call(s)")
    assert measured["prior_cost_usd"] == pytest.approx(0.002)
    assert "the USD is unknown" in report["price_source"]


def test_semantic_index_and_embed_backfill_cli(
    clean_runtime: str, tmp_path, capsys: pytest.CaptureFixture[str],
) -> None:
    """`okg db semantic-index` reports then builds; `okg db embed-backfill`
    refuses a deployment whose embeddings are off before any model call."""
    from pathlib import Path

    from okg.substrate.cli.main import main as okg_main
    from tests.substrate.mcp.semantic_fixture import (
        make_search_deployment,
        node,
        publish_with_vectors,
    )

    deployment = make_search_deployment(tmp_path, dsn=clean_runtime)
    publish_with_vectors(
        clean_runtime, deployment, subtypes=["chunk"],
        nodes=[node("chunk:a", "chunk", "alpha")], vectors={}, model_id=None,
    )
    manifest_path = Path(deployment) / "deployment.yaml"
    manifest = json.loads(manifest_path.read_text())

    # Embeddings off: both verbs refuse, and no model is contacted.
    assert okg_main([
        "db", "embed-backfill", "--deployment", deployment,
        "--dsn", clean_runtime, "--max-usd", "1", "--json",
    ]) == 1
    assert "semantic_embedder_not_configured" in capsys.readouterr().err

    manifest["embeddings"] = {"enabled": True, "backend": OR_SPEC}
    manifest_path.write_text(json.dumps(manifest))
    base = [
        "db", "semantic-index", "--deployment", deployment,
        "--dsn", clean_runtime, "--json",
    ]
    assert okg_main(base) == 0
    dry = json.loads(capsys.readouterr().out)
    dry = dry.get("data", dry)
    assert dry["status"] == "missing" and dry["action"] == "would_build"
    assert okg_main(base + ["--apply", "--maintenance-work-mem", "64MB"]) == 0
    built = json.loads(capsys.readouterr().out)
    built = built.get("data", built)
    assert built["status"] == "present" and built["action"] == "build"


def _backfilled_deployment(dsn: str, tmp_path, *, count: int = 3):
    from tests.substrate.mcp.semantic_fixture import (
        make_search_deployment,
        node,
        publish_with_vectors,
    )

    deployment = make_search_deployment(tmp_path, dsn=dsn)
    nodes = [node(f"chunk:{i}", "chunk", f"distinct passage {i}") for i in range(count)]
    publish_with_vectors(
        dsn, deployment, subtypes=["chunk"], nodes=nodes, vectors={}, model_id=None,
    )
    return deployment


def _plain_cycle(dsn: str, deployment: str) -> dict:
    from okg.substrate.publisher import Publisher
    from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer

    result = Publisher(
        dsn, deployment, catalog_identity=bind_test_catalog_writer(dsn, deployment),
    ).cycle()
    assert result["status"] == "published", result
    return result


def test_a_cleared_vector_does_not_force_a_new_generation(
    clean_runtime: str, tmp_path,
) -> None:
    """Only a WRITTEN old-column vector refuses no-op reuse; a cleared one
    (what the fact write path does on every node upsert) must not cut a
    generation that would only drop vectors."""
    from okg.substrate.graph_store import ChronosGraphStore

    deployment = _backfilled_deployment(clean_runtime, tmp_path)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        graph = ChronosGraphStore.from_connection(conn).checkout()
        for i in range(3):
            graph.execute(
                "UPDATE okg.graph_nodes SET embedding = :embedding::vector, "
                "embedding_model = :model_id WHERE node_id = :node_id",
                {
                    "embedding": "[" + ",".join(["0.01"] * 2560) + "]",
                    "model_id": OR_SPEC,
                    "node_id": f"chunk:{i}",
                },
            )
    published = _plain_cycle(clean_runtime, deployment)
    assert not published.get("reused_generation")
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        ChronosGraphStore.from_connection(conn).checkout().execute(
            "UPDATE okg.graph_nodes SET embedding = NULL, embedding_model = NULL "
            "WHERE node_id = :node_id",
            {"node_id": "chunk:1"},
        )
    after = _plain_cycle(clean_runtime, deployment)
    assert after["reused_generation"] is True
    assert after["generation_id"] == published["generation_id"]
    assert after["timings_ms"]["embedding_writes_probe.nodes"] == 0.0


def test_the_embed_worker_refuses_a_paid_backend(
    clean_runtime: str, tmp_path,
) -> None:
    """The DBOS worker has no cap and records no cost, so it never makes
    paid calls; `okg db embed-backfill` is the only path that does."""
    from pathlib import Path

    deployment = _backfilled_deployment(clean_runtime, tmp_path)
    manifest_path = Path(deployment) / "deployment.yaml"
    manifest = json.loads(manifest_path.read_text())
    manifest["embeddings"] = {"enabled": True, "backend": OR_SPEC}
    manifest_path.write_text(json.dumps(manifest))
    peer = _PricedPeer(usd_per_input=1e-6)
    plan = _or_plan(peer)
    probe_requests = peer.requests

    def must_not_embed(*_args, **_kwargs):
        raise AssertionError("the worker embedded with a paid backend")

    from okg.substrate.runtime.embed_workflow import _embed_operational_outcome

    outcome = _run_one_embed_batch(
        deployment=deployment, batch_size=10,
        embed_one=must_not_embed, embedding_plan=plan,
    )
    assert outcome["skipped"] is True
    assert outcome["reason"] == "paid_backend_requires_embed_backfill"
    assert peer.requests == probe_requests
    # The summary passes the workflow's operational-outcome contract, so even
    # a stray scheduled run is recorded as skipped instead of failing.
    assert _embed_operational_outcome(outcome)["outcome"] == "skipped"


def test_the_worker_fills_a_free_set_and_refuses_a_paid_one(
    clean_runtime: str, tmp_path,
) -> None:
    """Jason, lane chat 2026-10-14, "ehfill yes": after a publish the worker
    fills a set with a free backend through the backfill's own path (its
    lock, its completion record), once per generation, and refuses a paid
    set without a call. The completed fill then routes search to the set."""
    from pathlib import Path

    from okg.substrate.db.migrate import _ensure_vector_sets
    from okg.substrate.deployments import resolve_deployment
    from okg.substrate.embeddings import build_embedding_execution_plan
    from okg.substrate.mcp import open_session, search
    from okg.substrate.runtime.embed_workflow import fill_vector_sets

    deployment = _backfilled_deployment(clean_runtime, tmp_path)
    manifest_path = Path(deployment) / "deployment.yaml"
    manifest = json.loads(manifest_path.read_text())
    manifest["embeddings"] = {
        "enabled": True,
        "search_set": "main",
        "sets": {"main": {"backend": "hash"}, "paid": {"backend": OR_SPEC}},
    }
    manifest_path.write_text(json.dumps(manifest))
    _ensure_vector_sets(clean_runtime, deployment)
    plan = build_embedding_execution_plan(resolve_deployment(deployment).embedding_config)

    reports = {r["set"]: r for r in fill_vector_sets(deployment=deployment, embedding_plan=plan)}
    assert reports["paid"]["status"] == "refused_paid"
    assert "embed-backfill" in reports["paid"]["reason"]
    main = reports["main"]
    assert main["status"] == "completed" and main["fill_completed"] is True
    assert main["documents_embedded"] == 3
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        paid_vectors = conn.execute(
            "SELECT count(*) FROM okg.text_embeddings WHERE set_id = %s",
            (reports["paid"]["set_id"],),
        ).fetchone()[0]
    assert paid_vectors == 0
    again = {r["set"]: r for r in fill_vector_sets(deployment=deployment, embedding_plan=plan)}
    assert again["main"]["status"] == "up_to_date"

    _plain_cycle(clean_runtime, deployment)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        page = search(conn, session, "distinct passage 1", method="semantic",
                      embedder=plan, limit=3)
    assert page["vector_source"] == "vector_set"
    assert page["vector_set"]["status"] == "active"


def test_must_fail_the_worker_fill_reads_no_graph_when_nothing_changed(
    clean_runtime: str, tmp_path, monkeypatch,
) -> None:
    """Review P7 S3 (PACT openrouter-embeddings-hnsw). After a completed
    fill, a publish that wrote no link, re-registered nothing and kept the
    catalog gets its completion carried forward without the full graph
    pass (`unchanged`). `okg migrate` again with the same document rules
    moves nothing. A graph write makes the next fill run the full pass.
    One set's failure is that set's report; the other sets are still
    handled. Must-fail: every new generation ran the full pass, and a
    failure raised out of the fill."""
    from pathlib import Path

    import okg.substrate.embeddings.backfill as backfill
    from okg.substrate.db.migrate import _ensure_vector_sets
    from okg.substrate.deployments import resolve_deployment
    from okg.substrate.embeddings import build_embedding_execution_plan
    from okg.substrate.mcp import open_session, search
    from okg.substrate.runtime.embed_workflow import fill_vector_sets

    deployment = _backfilled_deployment(clean_runtime, tmp_path)
    manifest_path = Path(deployment) / "deployment.yaml"
    manifest = json.loads(manifest_path.read_text())
    manifest["embeddings"] = {
        "enabled": True,
        "search_set": "main",
        "sets": {"main": {"backend": "hash"}, "paid": {"backend": OR_SPEC}},
    }
    manifest_path.write_text(json.dumps(manifest))
    _ensure_vector_sets(clean_runtime, deployment)
    plan = build_embedding_execution_plan(resolve_deployment(deployment).embedding_config)

    def fill() -> dict:
        return {
            r["set"]: r
            for r in fill_vector_sets(deployment=deployment, embedding_plan=plan)
        }

    first = fill()["main"]
    assert first["status"] == "completed" and first["documents_embedded"] == 3
    # The completion forces the next publish to a new generation, which has
    # no completion of its own.
    g2 = _plain_cycle(clean_runtime, deployment)["generation_id"]
    assert g2 != first["generation_id"]

    real_backfill = backfill.run_set_backfill
    full_passes: list[str] = []

    def no_full_pass(*_args, **_kwargs):
        full_passes.append("ran")
        raise AssertionError("the worker read the whole graph with nothing changed")

    monkeypatch.setattr(backfill, "run_set_backfill", no_full_pass)
    reports = fill()
    assert full_passes == []
    assert reports["main"]["status"] == "unchanged", reports["main"]
    assert reports["main"]["generation_id"] == g2
    assert reports["paid"]["status"] == "refused_paid"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        assert conn.execute(
            "SELECT count(*) FROM okg.embedding_fill_completions "
            "WHERE set_id = %s AND generation_id = %s",
            (reports["main"]["set_id"], g2),
        ).fetchone()[0] == 1
    assert fill()["main"]["status"] == "up_to_date"

    # `okg migrate` again with the same document rules changes nothing: the
    # link counter does not move, so it cannot force a publish or a pass.
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        counter_before = conn.execute(
            "SELECT next_seq FROM okg.embedding_sets WHERE set_id = %s",
            (reports["main"]["set_id"],),
        ).fetchone()[0]
    _ensure_vector_sets(clean_runtime, deployment)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        assert conn.execute(
            "SELECT next_seq FROM okg.embedding_sets WHERE set_id = %s",
            (reports["main"]["set_id"],),
        ).fetchone()[0] == counter_before

    # A graph write takes a link: the full pass runs. A failure in it is
    # the set's report.
    from okg.substrate.graph_store import ChronosGraphStore
    from tests.substrate.mcp.semantic_fixture import node

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        ChronosGraphStore.from_connection(conn).apply_facts(
            [node("chunk:new", "chunk", "a passage written after the fill")],
            source="system:analyst_search_fixture", strict_endpoints=True,
        )
    g3 = _plain_cycle(clean_runtime, deployment)["generation_id"]
    assert g3 != g2
    failed = fill()
    assert full_passes == ["ran"]
    assert failed["main"]["status"] == "error"
    assert failed["main"]["error"] == "AssertionError"
    assert failed["paid"]["status"] == "refused_paid"

    monkeypatch.setattr(backfill, "run_set_backfill", real_backfill)
    full = fill()["main"]
    assert full["status"] == "completed" and full["generation_id"] == g3
    assert full["documents_embedded"] == 1
    _plain_cycle(clean_runtime, deployment)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        page = search(conn, session, "distinct passage 1", method="semantic",
                      embedder=plan, limit=3)
    assert page["vector_source"] == "vector_set"


def _worker_fill_fixture(dsn: str, tmp_path, block: dict | None = None):
    """A published graph of three chunks with the free set ``main``
    registered; returns ``(deployment, fill)``, where ``fill()`` runs the
    worker fill and returns its reports by set name."""
    from pathlib import Path

    from okg.substrate.db.migrate import _ensure_vector_sets
    from okg.substrate.deployments import resolve_deployment
    from okg.substrate.embeddings import build_embedding_execution_plan
    from okg.substrate.runtime.embed_workflow import fill_vector_sets

    deployment = _backfilled_deployment(dsn, tmp_path)
    manifest_path = Path(deployment) / "deployment.yaml"
    manifest = json.loads(manifest_path.read_text())
    manifest["embeddings"] = block or {
        "enabled": True, "sets": {"main": {"backend": "hash"}},
    }
    manifest_path.write_text(json.dumps(manifest))
    _ensure_vector_sets(dsn, deployment)
    plan = build_embedding_execution_plan(resolve_deployment(deployment).embedding_config)

    def fill() -> dict:
        return {
            r["set"]: r
            for r in fill_vector_sets(deployment=deployment, embedding_plan=plan)
        }

    return deployment, fill


def test_must_fail_a_publish_during_a_fill_is_filled_by_the_next_pass(
    clean_runtime: str, tmp_path, monkeypatch,
) -> None:
    """Review P8 B1 (PACT openrouter-embeddings-hnsw). A sync writes a
    node and publishes while the worker's fill runs at the older pin. The
    fill never reads that node (not visible at its pin), so its completion
    must not be carried to the new generation: the next worker run does
    the full pass and embeds the node. Must-fail: the completion recorded
    the link counter at the end of the fill, the new node's link included,
    so the next run carried it forward (`unchanged`) and the node stayed
    without a vector."""
    import okg.substrate.embeddings.backfill as backfill
    from okg.substrate.graph_store import ChronosGraphStore
    from tests.substrate.mcp.semantic_fixture import node

    deployment, fill = _worker_fill_fixture(clean_runtime, tmp_path)
    real_windows = backfill._visible_windows
    published: list[str] = []

    def windows_then_publish(*args, **kwargs):
        yield from real_windows(*args, **kwargs)
        # After the fill's last window, before it records its completion.
        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            ChronosGraphStore.from_connection(conn).apply_facts(
                [node("chunk:mid", "chunk", "a passage published mid-fill")],
                source="system:analyst_search_fixture", strict_endpoints=True,
            )
        published.append(_plain_cycle(clean_runtime, deployment)["generation_id"])

    monkeypatch.setattr(backfill, "_visible_windows", windows_then_publish)
    first = fill()["main"]
    monkeypatch.setattr(backfill, "_visible_windows", real_windows)
    assert first["status"] == "completed" and first["documents_embedded"] == 3
    (g2,) = published
    assert g2 != first["generation_id"]

    second = fill()["main"]
    assert second["status"] == "completed", second
    assert second["generation_id"] == g2
    assert second["documents_embedded"] == 1
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        assert conn.execute(
            "SELECT count(*) FROM okg.text_embeddings WHERE set_id = %s",
            (first["set_id"],),
        ).fetchone()[0] == 4
        # The first fill's completion says it cannot be carried
        # (store.UNCARRIABLE_FILL_SEQ).
        assert conn.execute(
            "SELECT max_seq FROM okg.embedding_fill_completions "
            "WHERE set_id = %s AND generation_id = %s",
            (first["set_id"], first["generation_id"]),
        ).fetchone()[0] == -1


def test_must_fail_content_set_back_to_a_version_never_embedded_is_filled(
    clean_runtime: str, tmp_path,
) -> None:
    """Review P9 SF1 (PACT openrouter-embeddings-hnsw). Node X is written
    with content A and published, then with content B and published before
    any fill; the worker fills only the newer generation, so B is embedded
    and A never is. A later write sets X back to A: its link already exists.
    The write must still move the link counter, so the next generation's
    fill runs the full pass and embeds A. Must-fail: a write whose links
    were all stored took no number, the counter still equalled the last
    completion's, the worker carried that completion forward (`unchanged`)
    and X stayed without a vector."""
    from okg.substrate.graph_store import ChronosGraphStore
    from tests.substrate.mcp.semantic_fixture import node

    deployment, fill = _worker_fill_fixture(clean_runtime, tmp_path)

    def write(text: str) -> None:
        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            ChronosGraphStore.from_connection(conn).apply_facts(
                [node("chunk:x", "chunk", text)],
                source="system:analyst_search_fixture", strict_endpoints=True,
            )

    def counter(set_id: str) -> int:
        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            return int(conn.execute(
                "SELECT next_seq FROM okg.embedding_sets WHERE set_id = %s",
                (set_id,),
            ).fetchone()[0])

    write("version A of the passage")
    _plain_cycle(clean_runtime, deployment)
    write("version B of the passage")
    g2 = _plain_cycle(clean_runtime, deployment)["generation_id"]
    first = fill()["main"]
    assert first["status"] == "completed" and first["generation_id"] == g2
    assert first["documents_embedded"] == 4  # three chunks and B, never A
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        assert conn.execute(
            "SELECT max_seq FROM okg.embedding_fill_completions "
            "WHERE set_id = %s AND generation_id = %s",
            (first["set_id"], g2),
        ).fetchone()[0] == counter(first["set_id"]) - 1

    before = counter(first["set_id"])
    write("version A of the passage")
    g3 = _plain_cycle(clean_runtime, deployment)["generation_id"]
    assert g3 != g2
    second = fill()["main"]
    assert second["status"] == "completed", second
    assert second["generation_id"] == g3
    assert second["links_written"] == 0 and second["documents_embedded"] == 1
    # The write of A took one number with no link (its link was stored).
    assert counter(first["set_id"]) == before + 1


def _link_counter(dsn: str, set_id: str) -> int:
    with psycopg.connect(dsn, autocommit=True) as conn:
        return int(conn.execute(
            "SELECT next_seq FROM okg.embedding_sets WHERE set_id = %s",
            (set_id,),
        ).fetchone()[0])


def _x_documents(dsn: str, set_id: str) -> tuple[int, int]:
    """``(documents linked, documents with a vector)`` of chunk:x."""
    with psycopg.connect(dsn, autocommit=True) as conn:
        linked, embedded = conn.execute(
            "SELECT count(DISTINCT d.doc_hash), count(DISTINCT e.doc_hash) "
            "  FROM okg.node_documents AS d "
            "  LEFT JOIN okg.text_embeddings AS e "
            "    ON e.set_id = d.set_id AND e.doc_hash = d.doc_hash "
            " WHERE d.set_id = %s AND d.node_id = 'chunk:x'",
            (set_id,),
        ).fetchone()
    return int(linked), int(embedded)


def test_must_fail_a_run_undo_that_restores_unembedded_content_is_filled(
    clean_runtime: str, tmp_path,
) -> None:
    """Lane EH P11 (PACT openrouter-embeddings-hnsw). Node X has content A
    at the run base of a source run; the run writes X = B and its own
    run-end publish shows B; the worker fills that generation, so B is
    embedded and A never is. The run is then rejected and undone to its run
    base (X = A again, written by the undo, not the graph writer). The undo
    must move the set's link counter, so the next generation's fill runs
    the full pass and embeds A. Must-fail: the undo took no link number, the
    counter still equalled the last completion's, the worker carried that
    completion forward (`unchanged`) and X stayed without a vector."""
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.ingest.compensation import retract_chronos_run
    from okg.substrate.ingest.run_base import (
        hold_run_liveness,
        run_base_checkpoint_id,
        take_run_base,
    )
    from okg.substrate.publisher import Publisher
    from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer
    from tests.substrate.mcp.semantic_fixture import node

    source, bad_run = "system:analyst_search_fixture", "run-x-bad"
    deployment, fill = _worker_fill_fixture(clean_runtime, tmp_path)
    store = ChronosGraphStore.connect(clean_runtime)
    liveness = None
    try:
        store.apply_facts(
            [node("chunk:x", "chunk", "version A of the passage")],
            source=source, run_id="run-x-good", strict_endpoints=True,
        )
        _plain_cycle(clean_runtime, deployment)  # A published, never filled
        publisher = Publisher(
            clean_runtime, deployment,
            catalog_identity=bind_test_catalog_writer(clean_runtime, deployment),
            own_source_run_ids=[bad_run],
        )
        checkpoint_id = run_base_checkpoint_id("default", bad_run)
        liveness = hold_run_liveness(clean_runtime, checkpoint_id)
        assert take_run_base(
            store, deployment=publisher.deployment_name, branch="default",
            source=source, run_id=bad_run,
        ) is not None
        store.apply_facts(
            [node("chunk:x", "chunk", "version B of the passage")],
            source=source, run_id=bad_run, strict_endpoints=True,
        )
        run_end = publisher.cycle()
        assert run_end["status"] == "published", run_end
        first = fill()["main"]
        set_id = first["set_id"]
        assert first["status"] == "completed", first
        assert first["generation_id"] == run_end["generation_id"]
        assert first["documents_embedded"] == 4  # three chunks and B, never A
        # A and B are both linked; only B has a vector.
        assert _x_documents(clean_runtime, set_id) == (2, 1)

        before = _link_counter(clean_runtime, set_id)
        stats = retract_chronos_run(
            clean_runtime, admission_id=None, source=source, run_id=bad_run,
        )
        assert stats["mode"] == "undo_to_run_base", stats
        assert stats["rows_restored"] >= 1, stats
        liveness.close()
        liveness = None
        undone = _link_counter(clean_runtime, set_id)
        after_undo = _plain_cycle(clean_runtime, deployment)["generation_id"]
        assert after_undo != run_end["generation_id"]

        second = fill()["main"]
        assert second["status"] == "completed", second
        assert second["generation_id"] == after_undo
        assert second["links_written"] == 0 and second["documents_embedded"] == 1
        assert _x_documents(clean_runtime, set_id) == (2, 2)
        # The undo took exactly one number (one active set, one operation).
        assert undone == before + 1
    finally:
        if liveness is not None:
            liveness.close()
        store.close()


def test_must_fail_a_branch_restore_to_unembedded_content_is_filled(
    clean_runtime: str, tmp_path,
) -> None:
    """Lane EH P11 (PACT openrouter-embeddings-hnsw). Node X is published
    with content A (G1), then with content B (G2) before any fill; the
    worker fills G2 only, so A is never embedded. The branch is then
    restored to G1 (X = A again, written by the restore, not the graph
    writer). The restore must move the set's link counter, so the next
    generation's fill runs the full pass and embeds A. Must-fail: the
    restore took no link number, the worker carried the G2 completion
    forward (`unchanged`) and X stayed without a vector."""
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.publisher import Publisher
    from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer
    from tests.substrate.mcp.semantic_fixture import node

    deployment, fill = _worker_fill_fixture(clean_runtime, tmp_path)
    name = Publisher(
        clean_runtime, deployment,
        catalog_identity=bind_test_catalog_writer(clean_runtime, deployment),
    ).deployment_name

    def write(text: str) -> None:
        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            ChronosGraphStore.from_connection(conn).apply_facts(
                [node("chunk:x", "chunk", text)],
                source="system:analyst_search_fixture", strict_endpoints=True,
            )

    write("version A of the passage")
    g1 = _plain_cycle(clean_runtime, deployment)["generation_id"]
    write("version B of the passage")
    g2 = _plain_cycle(clean_runtime, deployment)["generation_id"]
    first = fill()["main"]
    set_id = first["set_id"]
    assert first["status"] == "completed" and first["generation_id"] == g2
    assert first["documents_embedded"] == 4  # three chunks and B, never A
    # A and B are both linked; only B has a vector.
    assert _x_documents(clean_runtime, set_id) == (2, 1)

    before = _link_counter(clean_runtime, set_id)
    store = ChronosGraphStore.connect(clean_runtime)
    try:
        store.restore_branch_to_generation(g1, deployment_name=name)
    finally:
        store.close()
    restored = _link_counter(clean_runtime, set_id)
    g3 = _plain_cycle(clean_runtime, deployment)["generation_id"]
    assert g3 not in (g1, g2)

    second = fill()["main"]
    assert second["status"] == "completed", second
    assert second["generation_id"] == g3
    assert second["links_written"] == 0 and second["documents_embedded"] == 1
    assert _x_documents(clean_runtime, set_id) == (2, 2)
    # The restore took exactly one number (one active set, one operation).
    assert restored == before + 1


def test_must_fail_a_merge_landing_unembedded_content_is_filled(
    clean_runtime: str, tmp_path,
) -> None:
    """Review P11 S1 (PACT openrouter-embeddings-hnsw). A fork writes node
    X = C through the graph writer: C is linked, but visible only on the
    fork. Default publishes G, whose watermark covers C's link number, and
    the worker fills G (C is not visible there, so it is not embedded).
    Merge-back's LAND then merges the fork onto default (rows copied by
    Chronos's merge, not the graph writer) and default publishes again.
    The merge must move the link counter, so the next fill runs the full
    pass and embeds C. Must-fail: the merge took no number, the worker
    carried G's completion forward (`unchanged`) and X stayed without a
    vector."""
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.merge_guard import MergeDecision
    from tests.substrate.mcp.semantic_fixture import node

    deployment, fill = _worker_fill_fixture(clean_runtime, tmp_path)
    base = fill()["main"]
    assert base["status"] == "completed", base
    set_id, fork = base["set_id"], "experiment:eh-merge"
    store = ChronosGraphStore.connect(clean_runtime)
    try:
        store.create_branch(fork, from_branch="default", metadata={
            "base_branch": "default", "base_generation_id": base["generation_id"],
        })
        store.apply_facts(
            [node("chunk:x", "chunk", "version C written on the fork")],
            source="system:analyst_search_fixture", strict_endpoints=True,
            branch=fork,
        )
        g = _plain_cycle(clean_runtime, deployment)["generation_id"]
        first = fill()["main"]
        assert first["status"] == "completed" and first["generation_id"] == g
        assert first["documents_embedded"] == 0  # C is not visible at G
        assert _x_documents(clean_runtime, set_id) == (1, 0)

        before = _link_counter(clean_runtime, set_id)
        store.merge(fork, "default", decision=MergeDecision(
            decided_by="jason@example.org", decision="accept", fast_track=False,
        ))
    finally:
        store.close()
    merged = _link_counter(clean_runtime, set_id)
    landed = _plain_cycle(clean_runtime, deployment)["generation_id"]
    assert landed != g

    second = fill()["main"]
    assert second["status"] == "completed", second
    assert second["generation_id"] == landed
    assert second["links_written"] == 0 and second["documents_embedded"] == 1
    assert _x_documents(clean_runtime, set_id) == (1, 1)
    # The merge took exactly one number (one active set, one operation).
    assert merged == before + 1


def test_a_write_that_changes_nothing_keeps_the_carry(
    clean_runtime: str, tmp_path,
) -> None:
    """Review P10 F2 (PACT openrouter-embeddings-hnsw). Re-applying the
    same fact after a fill takes no link number, so the next worker fill
    still carries its completion (`unchanged`). Guards the graph writer's
    no-op filter: if it let an identical rewrite through, every re-sync
    would tick and every fill would read the whole graph again."""
    from okg.substrate.graph_store import ChronosGraphStore
    from tests.substrate.mcp.semantic_fixture import node

    deployment, fill = _worker_fill_fixture(clean_runtime, tmp_path)
    first = fill()["main"]
    assert first["status"] == "completed" and first["documents_embedded"] == 3
    set_id = first["set_id"]
    before = _link_counter(clean_runtime, set_id)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        ChronosGraphStore.from_connection(conn).apply_facts(
            [node("chunk:0", "chunk", "distinct passage 0")],
            source="system:analyst_search_fixture", strict_endpoints=True,
        )
    assert _link_counter(clean_runtime, set_id) == before
    g2 = _plain_cycle(clean_runtime, deployment)["generation_id"]
    second = fill()["main"]
    assert second["status"] == "unchanged", second
    assert second["generation_id"] == g2


def test_a_restore_takes_no_link_number_without_vector_sets(
    clean_runtime: str,
) -> None:
    """Lane EH P11 (review P11 S3): a database without the vector-set
    tables, one with no active set, and a store that is not PostgreSQL are
    all unaffected by the restore tick."""
    from okg.substrate.embeddings.store import mark_node_rows_restored

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        assert conn.execute(
            "SELECT count(*) FROM okg.embedding_sets WHERE active"
        ).fetchone()[0] == 0
        assert mark_node_rows_restored(conn) == 0
    # The tables absent (a deployment without embeddings): hidden inside a
    # transaction that is rolled back.
    with psycopg.connect(clean_runtime) as conn:
        conn.execute(
            "ALTER TABLE okg.embedding_sets RENAME TO embedding_sets_hidden"
        )
        assert conn.execute(
            "SELECT to_regclass('okg.embedding_sets') IS NULL"
        ).fetchone()[0]
        assert mark_node_rows_restored(conn) == 0
        conn.rollback()
    assert mark_node_rows_restored(SimpleNamespace(dialect="sqlite")) == 0


def test_must_fail_a_registration_that_lands_after_a_completion_is_not_carried(
    clean_runtime: str, tmp_path,
) -> None:
    """Review P8 S4 (PACT openrouter-embeddings-hnsw). `okg migrate` starts
    its transaction before a fill completes and commits a template change
    after it. The change takes a link number, so the next publish cuts a
    generation and the worker runs the full pass with the new template.
    Must-fail: the carry compared the row's `updated_at` (the migrate
    transaction's start) with the completion's `completed_at`; the change
    looked older than the completion, nothing moved the counter, the
    publish reused the generation and the new documents were never
    embedded."""
    from okg.substrate.embeddings.config import parse_embedding_config
    from okg.substrate.embeddings.store import register_sets

    block = {"enabled": True, "sets": {"main": {"backend": "hash"}}}
    changed = json.loads(json.dumps(block))
    # No header: every document changes, so each needs a new vector.
    changed["sets"]["main"]["document"] = {
        "default": {"header": [], "body": "text"},
    }
    (changed_set,) = parse_embedding_config(plural=changed).vector_sets
    deployment, fill = _worker_fill_fixture(clean_runtime, tmp_path, block)
    first = fill()["main"]
    assert first["status"] == "completed"
    assert changed_set.set_id == first["set_id"]
    g2 = _plain_cycle(clean_runtime, deployment)["generation_id"]

    migrate = psycopg.connect(clean_runtime)
    try:
        # The migrate transaction starts (its now() is fixed) ...
        migrate.execute("SELECT now()")
        # ... the fill completes at g2 after it ...
        carried = fill()["main"]
        assert carried["status"] == "unchanged" and carried["generation_id"] == g2
        # ... and the template change commits after the completion.
        register_sets(migrate, [changed_set])
        migrate.commit()
    finally:
        migrate.close()

    g3 = _plain_cycle(clean_runtime, deployment)["generation_id"]
    assert g3 != g2, "the template change did not force a new generation"
    full = fill()["main"]
    assert full["status"] == "completed", full
    assert full["generation_id"] == g3
    assert full["links_written"] == 3 and full["documents_embedded"] == 3


def test_the_set_plan_captures_its_probe_call_and_carries_the_provider_pin(
    monkeypatch,
) -> None:
    import okg.substrate.embeddings.identity as identity
    from okg.substrate.embeddings.backfill import build_set_plan
    from okg.substrate.embeddings.config import parse_embedding_config

    block = json.loads(json.dumps(OR_SET_BLOCK))
    block["sets"]["main"]["provider"] = "google-vertex"
    vector_set = parse_embedding_config(plural=block).search_vector_set
    peer = _PricedPeer(usd_per_input=0.001)
    seen: list[dict] = []

    def build(spec, **kwargs):
        seen.append({"spec": spec, **kwargs})
        return _or_embedder(peer)

    monkeypatch.setattr(identity, "build_embedder", build)
    plan, probe_calls = build_set_plan(vector_set)
    # The set's own backend: unpadded, at the set's width.
    assert seen == [{"spec": OR_SPEC, "provider": "google-vertex", "vector_set": True}]
    assert plan.observed_dimension == 768 and plan.node_column is False
    assert peer.requests == 1 and len(probe_calls) == 1
    assert probe_calls[0].cost_usd == pytest.approx(0.001)
    assert plan.embedder.call_observer is None
