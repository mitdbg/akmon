"""PACT bind-at-rest-anonymization -- req.chokepoint-binding.

Proves the at-rest transform is bound at BOTH ingest write chokepoints under a
not-store-raw posture, that a store_raw source is untouched, that a detector-
unavailable free-text fact is HELD (never stored raw), and -- via a frozen writer
inventory -- that no NEW fact-writer silently appears outside a documented
disposition.

Two chokepoints, not one (the design review's central finding): the source-runner
admission COPY (``SourceRunner._apply_pii_posture``) and the enricher-emitter
INSERT (``_fact_emitters.emit_node_fact`` / ``emit_edge_fact``). Across the whole
``src/okg`` tree there are 26 fact-writer modules; the inventory below classifies
each -- most are DERIVED (they read already-admitted, already-transformed facts ->
transitive safety), PROJECTION (graph-internal), agent WRITE-BACK (own Nomos
admission, including the deployment SDK mutation path), or test HARNESSES. Binding
those two chokepoints is what stops raw *source* PII at rest; posture plumbing
through the Publisher's enricher loop is the named residual.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

from okg.substrate.alias.pseudonymize import load_pseudonym_service
from okg.substrate.ingest.pii_posture import (
    PiiFactsHeld,
    PiiPosture,
    declared_pii_classifier,
    store_raw_posture,
)
from okg.substrate.ingest.runner import SourceRunner
from okg.substrate.library.enrichers._fact_emitters import (
    emit_edge_fact,
    emit_node_fact,
)
from okg.substrate.library.sources.base import EdgeFact, NodeFact

_KEY = "0123456789abcdef0123456789abcdef"
_CLASSES = {"email": "direct_identifier", "kind": "non_pii", "note": "free_text"}


def _active_posture(*, detector=None) -> PiiPosture:
    return PiiPosture(
        store_raw=False,
        classifier=declared_pii_classifier(_CLASSES),
        pseudonym_service=load_pseudonym_service(env={"OKG_PSEUDONYM_HMAC_KEY": _KEY}),
        detector=detector,
    )


def _runner(posture: PiiPosture | None) -> SourceRunner:
    # __init__ does not connect (the pool is acquired lazily), so a placeholder
    # dsn is fine for the pure _apply_pii_posture transform.
    return SourceRunner(
        "postgresql://unused/unused", source_name="fixture", pii_posture=posture
    )


class _Span:
    def __init__(self, start: int, end: int) -> None:
        self.start = start
        self.end = end


class _SubstringDetector:
    """A stand-in free-text detector: masks every occurrence of ``needle``."""

    def __init__(self, needle: str) -> None:
        self._needle = needle

    def detect(self, text: str):
        return [
            _Span(m.start(), m.end())
            for m in re.finditer(re.escape(self._needle), text)
        ]


# --- Chokepoint 1: source-runner admission (SourceRunner._apply_pii_posture) ---


def test_source_runner_store_raw_is_untouched() -> None:
    fact = NodeFact("n1", "person", {"email": "alice@example.com"}, {"id": "1"}, {"rev": "1"})
    # No posture and an explicit store_raw posture both leave facts by identity.
    assert _runner(None)._apply_pii_posture([fact]) == [fact]
    assert _runner(store_raw_posture())._apply_pii_posture([fact]) == [fact]


def test_source_runner_not_store_raw_transforms_and_joins() -> None:
    posture = _active_posture()
    n1 = NodeFact(
        "n1", "person",
        {"email": "alice@example.com", "kind": "person", "secret": "raw-secret"},
        {"id": "alice@example.com"}, {"rev": "alice@example.com"},
    )
    n2 = NodeFact("n2", "person", {"email": "alice@example.com"}, {"id": "2"}, {"rev": "1"})
    e1 = EdgeFact("n1", "n3", "reviews", {"email": "alice@example.com"})
    out = _runner(posture)._apply_pii_posture([n1, n2, e1])
    a1, a2, ae = out[0].attrs, out[1].attrs, out[2].attrs
    # direct_identifier -> pseudonym (raw gone); non_pii revealed; undeclared masked.
    assert a1["email"] != "alice@example.com" and a1["email"].startswith("directidentifier_")
    assert a1["kind"] == "person"
    assert a1["secret"] == "[nomos:masked]"
    # join-consistent: same raw identifier -> same pseudonym across node + edge.
    assert a1["email"] == a2["email"] == ae["email"]
    # no raw value survives anywhere in the transformed attrs.
    assert "alice@example.com" not in repr(out)
    assert out[0].source_record_id["nomos_pseudonym"].startswith(
        "directidentifier_"
    )
    assert out[0].source_revision["nomos_pseudonym"].startswith(
        "directidentifier_"
    )


def test_source_runner_holds_free_text_without_detector(monkeypatch) -> None:
    # Force the registry empty so the absent-detector path is deterministic.
    monkeypatch.setattr(
        "okg.substrate.ingest.pii_sanitize.free_text_detector", lambda: None
    )
    fact = NodeFact("n1", "note", {"note": "call Alice at 555-1234"}, {"id": "1"}, {"rev": "1"})
    with pytest.raises(PiiFactsHeld) as excinfo:
        _runner(_active_posture(detector=None))._apply_pii_posture([fact])
    assert excinfo.value.held == 1
    assert "no raw PII was stored" in str(excinfo.value)


def test_source_runner_masks_free_text_spans_with_detector() -> None:
    posture = _active_posture(detector=_SubstringDetector("Alice"))
    fact = NodeFact("n1", "note", {"note": "call Alice today"}, {"id": "1"}, {"rev": "1"})
    (out,) = _runner(posture)._apply_pii_posture([fact])
    assert "Alice" not in out.attrs["note"]
    assert out.attrs["note"].startswith("call ")


# --- Chokepoint 2: enricher emitters (_fact_emitters.emit_node/edge_fact) -----


class _Cur:
    # `rowcount` is a standard DBAPI cursor attribute, and the emitters
    # read it to report `FactEmitOutcome.inserted` (PACT
    # add-emit-once-dedupe-scope, issue #901). This double captures
    # params instead of writing, so every captured statement stands for
    # exactly one inserted row.
    rowcount = 1

    def __init__(self, sink: list) -> None:
        self._sink = sink

    def __enter__(self) -> "_Cur":
        return self

    def __exit__(self, *_a) -> bool:
        return False

    def execute(self, _sql: str, params=None) -> None:
        self._sink.append(params)


class _Conn:
    def __init__(self) -> None:
        self.captured: list = []

    def cursor(self) -> _Cur:
        return _Cur(self.captured)


def _captured_attrs(params) -> dict:
    jsonb = params["attrs"]
    return getattr(jsonb, "obj", jsonb)


def _captured_json(params, key: str) -> dict:
    jsonb = params[key]
    return getattr(jsonb, "obj", jsonb)


def test_enricher_emit_node_transforms_before_insert() -> None:
    conn = _Conn()
    outcome = emit_node_fact(
        conn,
        enricher_name="probe",
        node_id="n1",
        subtype="person",
        attrs={"email": "alice@example.com", "kind": "person", "secret": "raw"},
        dedupe_key="dk1",
        source_record_id={"id": "alice@example.com"},
        source_revision={"rev": "alice@example.com"},
        run_id="run1",
        observed_at=__import__("datetime").datetime(2026, 7, 12),
        pii_posture=_active_posture(),
        derived_from=["input:1"],
        support_policy="all_members",
    )
    assert outcome.emitted is True and outcome.held is False
    attrs = _captured_attrs(conn.captured[0])
    assert attrs["email"].startswith("directidentifier_") and attrs["email"] != "alice@example.com"
    assert attrs["kind"] == "person"
    assert attrs["secret"] == "[nomos:masked]"
    assert "alice@example.com" not in repr(conn.captured[0])
    assert _captured_json(conn.captured[0], "srcrec")["nomos_pseudonym"].startswith(
        "directidentifier_"
    )


def test_enricher_emit_edge_transforms_before_insert() -> None:
    conn = _Conn()
    outcome = emit_edge_fact(
        conn,
        enricher_name="probe",
        src="n1",
        dst="n2",
        edge_type="reviews",
        attrs={"email": "alice@example.com"},
        provenance="explicit",
        confidence=1.0,
        dedupe_key="dk1",
        source_record_id={"id": "alice@example.com"},
        source_revision={"rev": "alice@example.com"},
        run_id="run1",
        observed_at=__import__("datetime").datetime(2026, 7, 12),
        pii_posture=_active_posture(),
    )
    assert outcome.emitted is True
    assert _captured_attrs(conn.captured[0])["email"].startswith("directidentifier_")
    assert "alice@example.com" not in repr(conn.captured[0])


def test_enricher_emit_holds_free_text_without_detector_no_insert(monkeypatch) -> None:
    monkeypatch.setattr(
        "okg.substrate.ingest.pii_sanitize.free_text_detector", lambda: None
    )
    conn = _Conn()
    outcome = emit_node_fact(
        conn,
        enricher_name="probe",
        node_id="n1",
        subtype="note",
        attrs={"note": "call Alice at 555-1234"},
        dedupe_key="dk1",
        source_record_id={"id": "1"},
        source_revision={"rev": "1"},
        run_id="run1",
        observed_at=__import__("datetime").datetime(2026, 7, 12),
        pii_posture=_active_posture(detector=None),
        derived_from=["input:1"],
        support_policy="all_members",
    )
    assert outcome.emitted is False and outcome.held is True
    assert conn.captured == []  # HELD short-circuits before any INSERT.


def test_enricher_emit_store_raw_is_unchanged() -> None:
    conn = _Conn()
    emit_node_fact(
        conn,
        enricher_name="probe",
        node_id="n1",
        subtype="person",
        attrs={"email": "alice@example.com"},
        dedupe_key="dk1",
        source_record_id={"id": "1"},
        source_revision={"rev": "1"},
        run_id="run1",
        observed_at=__import__("datetime").datetime(2026, 7, 12),
        derived_from=["input:1"],
        support_policy="all_members",
        # no pii_posture ⇒ existing behavior
    )
    assert _captured_attrs(conn.captured[0])["email"] == "alice@example.com"


# --- Residual-writer sweep: a frozen, classified inventory -------------------

# Every module under src/okg writing okg.node_facts / okg.edge_facts, keyed
# relative to src/okg, with its PII disposition. The at-rest transform binds the
# two ADMISSION chokepoints; the rest read already-admitted facts (DERIVED),
# project the stored graph (PROJECTION), run their own Nomos admission
# (WRITE_BACK, including the deployment SDK mutation path), or are test HARNESSES.
# A NEW writer breaks this test until it is classified -- so a raw-source path can
# never be added silently outside the transform.
_WRITER_INVENTORY: dict[str, str] = {
    # source admission -- raw source PII entry, TRANSFORMED at rest
    "substrate/ingest/runner.py": "source-admission-chokepoint",
    # enricher emit chokepoint -- TRANSFORM-CAPABLE (posture-gated)
    "substrate/library/enrichers/_fact_emitters.py": "enricher-emit-chokepoint",
    # derived: read already-admitted (already-transformed) facts
    "substrate/enrichers/derived_edges.py": "derived",
    "substrate/library/enrichers/semantic_concept_linker.py": "derived",
    "substrate/lenses/promote.py": "derived",
    "substrate/library/linkers/agent_memory_quality.py": "derived",
    "substrate/library/linkers/code_reference.py": "derived",
    "substrate/library/linkers/coverage_symbol.py": "derived",
    "substrate/library/linkers/data_asset.py": "derived",
    "substrate/library/linkers/data_flow_evidence.py": "derived",
    "substrate/library/linkers/interface_handler.py": "derived",
    "substrate/library/linkers/profile_hotspot.py": "derived",
    "substrate/library/linkers/security_finding.py": "derived",
    "substrate/library/linkers/success_condition.py": "derived",
    "substrate/library/linkers/test_symbol.py": "derived",
    # projection: graph-internal, projects the stored graph
    "substrate/projection/projector.py": "projection",
    "substrate/publisher/publisher.py": "projection",
    "substrate/publisher/native_view_stage.py": "projection",
    # agent write-back: separate Nomos-governed admission
    "substrate/runtime/admission_workflows.py": "write-back",
    "substrate/mcp/fan_out.py": "write-back",
    "substrate/mcp/review_as.py": "write-back",
    "substrate/mcp/connector_ingest.py": "write-back",
    "deployment/_internal/mutations.py": "write-back",
    # test / bench harnesses -- not production ingest
    "substrate/exchange/harness.py": "harness",
    "substrate/exchange/hardening_harness.py": "harness",
}

# Case-insensitive + quote/whitespace tolerant so a lowercase, double-spaced, or
# "okg"."node_facts"-quoted write cannot slip a new writer past the tripwire.
_WRITE_RE = re.compile(
    r'(INSERT\s+INTO|COPY)\s+"?okg"?\.\s*"?(node_facts|edge_facts)"?',
    re.IGNORECASE,
)


def test_fact_writer_inventory_is_frozen() -> None:
    root = Path(__file__).resolve()
    # Walk the WHOLE src/okg tree (not just substrate): fact-writers also live
    # under src/okg/deployment (the SDK mutation path).
    repo = next(p for p in root.parents if (p / "src" / "okg").is_dir())
    okg = repo / "src" / "okg"
    discovered = {
        str(path.relative_to(okg)).replace("\\", "/")
        for path in okg.rglob("*.py")
        if _WRITE_RE.search(path.read_text(encoding="utf-8"))
    }
    documented = set(_WRITER_INVENTORY)
    new_writers = discovered - documented
    assert not new_writers, (
        "New okg.node_facts/edge_facts writer(s) appeared and are unclassified "
        f"for at-rest PII: {sorted(new_writers)}. Classify each in "
        "_WRITER_INVENTORY (source-admission / enricher-emit / derived / "
        "projection / write-back / harness) and route any raw-source path "
        "through the at-rest transform."
    )
    # And nothing documented was removed without updating the inventory.
    assert not (documented - discovered), (
        "Documented fact-writer(s) no longer write facts; update "
        f"_WRITER_INVENTORY: {sorted(documented - discovered)}"
    )
