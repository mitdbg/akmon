"""STaRK-Prime source: the gold path is absent, and the release decides
the composition.

PACT okg-bench-graph-benchmarks / req.stark.corpus, task.stark.source.

The fixtures here write REAL `torch.save` containers (zip + pickled
rebuild call + raw storage), so `_stark_pt.read_int_tensor` is exercised
against the format it claims to read rather than against a mock. The
writer is checked against the released files in
`test_reader_matches_the_real_release`, which is skipped when the release
is not on this machine — a skip that is reported, never a silent pass.
"""

from __future__ import annotations

import os
import pickle
import struct
import zipfile
from pathlib import Path

import pytest

from okg.substrate.library.sources._stark_pt import (
    StarkTensorError,
    read_int_tensor,
)
from okg.substrate.library.sources.base import EdgeFact, NodeFact
from okg.substrate.library.sources.stark_prime import (
    GOLD_ARTIFACT_NAMES,
    PINNED_PRIME_RELEASE,
    PRIME_EDGE_TYPES,
    PRIME_RELEASE_TRIPLES,
    PRIME_SUBTYPES,
    ReleaseExpectation,
    StarkPrimeError,
    StarkPrimeSource,
    node_id_for,
)

# --------------------------------------------------------------------------
# a torch.save container, written by hand
# --------------------------------------------------------------------------


#: Pickle protocol-2 opcodes, spelled out. The pickle is assembled from
#: opcodes rather than by `pickle.Pickler`, because pickling a call to
#: `torch._utils._rebuild_tensor_v2` would require torch — or a fake
#: `torch` injected into `sys.modules`, which is exactly the module-eviction
#: hazard this repo has been bitten by. Assembling the bytes needs neither,
#: and states the container format the reader parses.
_PROTO2 = b"\x80\x02"
_MARK = b"("
_TUPLE = b"t"
_BINPERSID = b"Q"
_REDUCE = b"R"
_STOP = b"."


def _global(module: str, name: str) -> bytes:
    return b"c" + module.encode() + b"\n" + name.encode() + b"\n"


def _unicode(text: str) -> bytes:
    raw = text.encode("utf-8")
    return b"X" + struct.pack("<i", len(raw)) + raw


def _int(value: int) -> bytes:
    return b"J" + struct.pack("<i", value)


def _int_tuple(values: tuple[int, ...]) -> bytes:
    return _MARK + b"".join(_int(v) for v in values) + _TUPLE


def write_int_tensor(
    path: Path, shape: tuple[int, ...], values: list[int],
    storage: str = "LongStorage",
) -> None:
    """Write `values` as a torch>=1.6 zip-format int tensor."""
    stem = path.stem
    code = {"LongStorage": "q", "IntStorage": "i"}[storage]
    raw = struct.pack(f"<{len(values)}{code}", *values)
    stride: list[int] = []
    running = 1
    for dim in reversed(shape):
        stride.append(running)
        running *= dim
    stride.reverse()

    data_pkl = (
        _PROTO2
        + _global("torch._utils", "_rebuild_tensor_v2")
        + _MARK
        # the persistent storage id: ('storage', torch.<Storage>, key,
        # location, numel)
        + _MARK
        + _unicode("storage")
        + _global("torch", storage)
        + _unicode("0")
        + _unicode("cpu")
        + _int(len(values))
        + _TUPLE
        + _BINPERSID
        + _int(0)                       # storage offset
        + _int_tuple(tuple(shape))
        + _int_tuple(tuple(stride))
        + _TUPLE
        + _REDUCE
        + _STOP
    )
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr(f"{stem}/data.pkl", data_pkl)
        archive.writestr(f"{stem}/data/0", raw)
        archive.writestr(f"{stem}/version", "3\n")


# --------------------------------------------------------------------------
# a miniature Prime release
# --------------------------------------------------------------------------

#: A four-entity, two-relation toy release. Entities 0/1 are genes, 2 is a
#: drug, 3 a disease. Edges mirror the release's own symmetry.
FIXTURE_NODE_TYPES = {0: "gene/protein", 1: "drug", 2: "disease"}
FIXTURE_EDGE_TYPES = {0: "ppi", 1: "target", 2: "indication"}
FIXTURE_ENTITY_TYPE = [0, 0, 1, 2]
FIXTURE_EDGES = [
    (0, 0, 1), (1, 0, 0),          # gene 0 <-ppi-> gene 1
    (2, 1, 0), (0, 1, 2),          # drug 2 <-target-> gene 0
    (2, 2, 3), (3, 2, 2),          # drug 2 <-indication-> disease 3
]
FIXTURE_NAMES = ["PAK1", "NCK2", "aspirin", "migraine"]

FIXTURE_RELEASE = ReleaseExpectation(
    label="fixture release (tests/substrate/test_stark_prime_source.py)",
    entity_count=4,
    edge_row_count=6,
    subtypes=frozenset({
        "stark_prime_gene_protein", "stark_prime_drug", "stark_prime_disease",
    }),
    edge_types=frozenset({"ppi", "target", "indication"}),
    triples=frozenset({
        ("stark_prime_gene_protein", "ppi", "stark_prime_gene_protein"),
        ("stark_prime_drug", "target", "stark_prime_gene_protein"),
        ("stark_prime_gene_protein", "target", "stark_prime_drug"),
        ("stark_prime_drug", "indication", "stark_prime_disease"),
        ("stark_prime_disease", "indication", "stark_prime_drug"),
    }),
)


def build_release(
    root: Path,
    *,
    node_types: dict[int, str] | None = None,
    edge_types: dict[int, str] | None = None,
    entity_type: list[int] | None = None,
    edges: list[tuple[int, int, int]] | None = None,
    names: list[str] | None = None,
    details: dict[int, dict] | None = None,
) -> Path:
    """Write a Prime-shaped `skb/prime/processed/` under `root`."""
    node_types = FIXTURE_NODE_TYPES if node_types is None else node_types
    edge_types = FIXTURE_EDGE_TYPES if edge_types is None else edge_types
    entity_type = FIXTURE_ENTITY_TYPE if entity_type is None else entity_type
    edges = FIXTURE_EDGES if edges is None else edges
    names = FIXTURE_NAMES if names is None else names
    processed = root / "skb" / "prime" / "processed"
    processed.mkdir(parents=True, exist_ok=True)
    (processed / "node_type_dict.pkl").write_bytes(pickle.dumps(node_types))
    (processed / "edge_type_dict.pkl").write_bytes(pickle.dumps(edge_types))
    node_info = {
        index: {
            "id": f"src-{index}",
            "type": node_types[entity_type[index]],
            "name": names[index],
            "source": "FIXTURE",
            **({"details": details[index]} if details and index in details
               else {}),
        }
        for index in range(len(entity_type))
    }
    (processed / "node_info.pkl").write_bytes(pickle.dumps(node_info))
    write_int_tensor(
        processed / "node_types.pt", (len(entity_type),), list(entity_type))
    write_int_tensor(
        processed / "edge_types.pt", (len(edges),), [e[1] for e in edges])
    write_int_tensor(
        processed / "edge_index.pt", (2, len(edges)),
        [e[0] for e in edges] + [e[2] for e in edges])
    return processed


def run_fixture(root: Path, **kwargs):
    """Run the default fixture release, building it if the caller has not
    already written a variant."""
    if not (root / "skb" / "prime" / "processed").is_dir():
        build_release(root)
    source = StarkPrimeSource(
        processed_dir=str(root / "skb" / "prime" / "processed"),
        snapshot_root=str(root),
        expect=FIXTURE_RELEASE,
        **kwargs,
    )
    run = source.run("test-run")
    return source, run, list(run.facts)


# --------------------------------------------------------------------------
# the .pt reader
# --------------------------------------------------------------------------


def test_reader_round_trips_a_2d_tensor(tmp_path: Path) -> None:
    write_int_tensor(tmp_path / "edge_index.pt", (2, 3), [1, 2, 3, 4, 5, 6])
    assert read_int_tensor(tmp_path / "edge_index.pt") == (
        (2, 3), [1, 2, 3, 4, 5, 6]
    )


def test_reader_refuses_a_non_zip_file(tmp_path: Path) -> None:
    (tmp_path / "bad.pt").write_bytes(b"\x80\x02legacy pickle container")
    with pytest.raises(StarkTensorError, match="not a torch>=1.6 zip-format"):
        read_int_tensor(tmp_path / "bad.pt")


def test_reader_refuses_a_truncated_storage(tmp_path: Path) -> None:
    path = tmp_path / "t.pt"
    write_int_tensor(path, (4,), [1, 2, 3, 4])
    with zipfile.ZipFile(path) as archive:
        entries = {n: archive.read(n) for n in archive.namelist()}
    with zipfile.ZipFile(path, "w") as archive:
        for name, data in entries.items():
            archive.writestr(
                name, data[:-8] if name.endswith("data/0") else data)
    with pytest.raises(StarkTensorError, match="declares 4 elements"):
        read_int_tensor(path)


def test_reader_refuses_a_missing_file(tmp_path: Path) -> None:
    with pytest.raises(StarkTensorError, match="is missing"):
        read_int_tensor(tmp_path / "nope.pt")


@pytest.mark.skipif(
    not os.environ.get("OKG_STARK_PRIME_PROCESSED"),
    reason="set OKG_STARK_PRIME_PROCESSED to the unpacked release to check "
           "the reader against the real skb/prime/processed files",
)
def test_reader_matches_the_real_release() -> None:
    processed = Path(os.environ["OKG_STARK_PRIME_PROCESSED"])
    shape, values = read_int_tensor(processed / "node_types.pt")
    assert shape == (PINNED_PRIME_RELEASE.entity_count,)
    assert len(values) == PINNED_PRIME_RELEASE.entity_count
    shape, _ = read_int_tensor(processed / "edge_index.pt")
    assert shape == (2, PINNED_PRIME_RELEASE.edge_row_count)


# --------------------------------------------------------------------------
# THE GOLD PATH
# --------------------------------------------------------------------------


def _gold_shaped(fact: NodeFact | EdgeFact) -> bool:
    """True when a fact looks like the promoted-from adapter's gold
    emission: a query node, a `targets` edge, or anything carrying the
    `stark.gold_answer` stamp or an `answer_ids`-derived attribute.

    This is the probe. `test_the_gold_probe_can_fail` runs it against the
    old shape to show it is not vacuous."""
    if isinstance(fact, EdgeFact):
        if fact.edge_type == "targets":
            return True
        payload = {**fact.attrs, **fact.source_record_id}
    else:
        if "query" in fact.subtype:
            return True
        payload = {**fact.attrs, **fact.source_record_id}
    for key, value in payload.items():
        blob = f"{key}={value}".lower()
        if "gold" in blob or "answer_id" in blob or "stark.gold" in blob:
            return True
    return False


def test_gold_answers_never_enter_the_graph(tmp_path: Path) -> None:
    """req.stark.corpus: no node or edge derives from `answer_ids`.

    Three assertions, because "no gold-shaped fact" alone would pass on an
    adapter that emitted the gold under different names: the fact stream
    is exactly the release's own entities and edge rows, COUNTED, and its
    edge triples are the release's triples."""
    _, run, facts = run_fixture(tmp_path)
    nodes = [f for f in facts if isinstance(f, NodeFact)]
    edges = [f for f in facts if isinstance(f, EdgeFact)]

    assert [f for f in facts if _gold_shaped(f)] == []
    assert len(nodes) == FIXTURE_RELEASE.entity_count
    assert len(edges) == FIXTURE_RELEASE.edge_row_count
    assert len(facts) == len(nodes) + len(edges)
    assert {f.subtype for f in nodes} <= FIXTURE_RELEASE.subtypes
    assert {f.edge_type for f in edges} <= FIXTURE_RELEASE.edge_types
    assert run.completed_scope is True


def test_the_gold_probe_can_fail(tmp_path: Path) -> None:
    """The probe above must be able to go red, or it proves nothing.

    Reconstructs the emission the promoted-from source produced by default
    (`include_queries=True`): a query node plus `query -targets-> entity`
    edges stamped `source_relation: stark.gold_answer`."""
    old_shape = [
        NodeFact(
            node_id="stark:prime:query:42",
            subtype="stark_prime_query",
            attrs={"query_id": "42", "query": "which drug treats X?"},
            source_record_id={"dataset": "prime", "record_type": "query"},
            source_revision={"content_hash": "x"},
        ),
        EdgeFact(
            src="stark:prime:query:42",
            dst=node_id_for(3),
            edge_type="targets",
            attrs={"source_relation": "stark.gold_answer", "split": "test"},
        ),
    ]
    assert [f for f in old_shape if _gold_shaped(f)] == old_shape

    # ...and the same probe over what this source actually emits is green,
    # so the two runs distinguish the shapes rather than agreeing on both.
    _, _, facts = run_fixture(tmp_path)
    assert [f for f in facts if _gold_shaped(f)] == []


def test_the_module_has_no_reader_for_the_qa_table() -> None:
    """The gold path is ABSENT, not defaulted off.

    A flag can be flipped back by a caller, an env var or a copy-paste; a
    codepath that does not exist cannot. Checked over the module's
    IDENTIFIERS and IMPORTS via `ast`, not over its text — the docstring
    describes the removed path at length, and a text scan would either
    trip on the prose or be weakened until it caught nothing."""
    import ast

    from okg.substrate.library.sources import stark_prime

    tree = ast.parse(
        Path(stark_prime.__file__).read_text(encoding="utf-8")
    )
    names: set[str] = set()
    attributes: set[str] = set()
    imports: set[str] = set()
    strings: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Name):
            names.add(node.id)
        elif isinstance(node, ast.Attribute):
            attributes.add(node.attr)
        elif isinstance(node, ast.arg):
            names.add(node.arg)
        elif isinstance(node, ast.keyword) and node.arg:
            names.add(node.arg)
        elif isinstance(node, ast.Import):
            imports.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            imports.add(node.module or "")
        elif isinstance(node, ast.Constant) and isinstance(node.value, str):
            strings.append(node.value)

    identifiers = names | attributes
    for banned in ("answer_ids", "include_queries", "gold_answer", "query",
                   "queries", "split"):
        assert banned not in identifiers, (
            f"stark_prime.py binds or reads {banned!r}; the QA half must "
            "not be reachable from this module"
        )
    # No CSV reader at all: `stark_qa.csv` is the only CSV in the release.
    assert "csv" not in imports and "csv" not in identifiers

    # The only QA FILENAMES the module names are the refusal list. Prose
    # (docstrings, error messages) discusses them at length and is not a
    # filename, so the check is over bare-filename literals.
    filenames = {
        s for s in strings
        if " " not in s and (s.endswith(".csv") or s.endswith(".index"))
    }
    assert filenames == set(GOLD_ARTIFACT_NAMES)


def test_a_snapshot_holding_the_qa_table_is_refused(tmp_path: Path) -> None:
    build_release(tmp_path)
    qa = tmp_path / "qa" / "prime" / "stark_qa"
    qa.mkdir(parents=True)
    (qa / "stark_qa.csv").write_text("id,query,answer_ids\n0,q,[1]\n")
    source = StarkPrimeSource(
        processed_dir=str(tmp_path / "skb" / "prime" / "processed"),
        snapshot_root=str(tmp_path),
        expect=FIXTURE_RELEASE,
    )
    with pytest.raises(StarkPrimeError, match="gold-bearing artifacts"):
        source.run("test-run")


@pytest.mark.parametrize("name", GOLD_ARTIFACT_NAMES)
def test_every_gold_artifact_name_is_refused(tmp_path: Path, name: str) -> None:
    """Each name in the refusal list actually refuses. Without this the
    list could gain a typo'd entry that refuses nothing."""
    build_release(tmp_path)
    (tmp_path / "elsewhere").mkdir()
    (tmp_path / "elsewhere" / name).write_text("x")
    source = StarkPrimeSource(
        processed_dir=str(tmp_path / "skb" / "prime" / "processed"),
        snapshot_root=str(tmp_path),
        expect=FIXTURE_RELEASE,
    )
    with pytest.raises(StarkPrimeError, match="gold-bearing artifacts"):
        source.run("test-run")


def test_a_clean_snapshot_is_not_refused(tmp_path: Path) -> None:
    """The complement of the refusal: a snapshot holding only the SKB
    runs. Without this the refusal could be unconditional and every test
    above would still be green."""
    _, _, facts = run_fixture(tmp_path)
    assert facts


# --------------------------------------------------------------------------
# composition is determined by the release
# --------------------------------------------------------------------------


def test_subtypes_and_edge_types_come_from_the_release_dicts(
    tmp_path: Path,
) -> None:
    """Renaming a type in `node_type_dict.pkl` moves the emitted subtype.

    That is what "derived from the SKB's own dicts" means operationally:
    the mapping is not a table in this module keyed on names it hopes to
    see."""
    build_release(
        tmp_path,
        node_types={0: "gene/protein", 1: "drug", 2: "malady"},
    )
    source = StarkPrimeSource(
        processed_dir=str(tmp_path / "skb" / "prime" / "processed"),
        snapshot_root=str(tmp_path),
        expect=ReleaseExpectation(
            label="renamed fixture",
            entity_count=4,
            edge_row_count=6,
            subtypes=frozenset({
                "stark_prime_gene_protein", "stark_prime_drug",
                "stark_prime_malady",
            }),
            edge_types=FIXTURE_RELEASE.edge_types,
            triples=frozenset({
                ("stark_prime_gene_protein", "ppi",
                 "stark_prime_gene_protein"),
                ("stark_prime_drug", "target", "stark_prime_gene_protein"),
                ("stark_prime_gene_protein", "target", "stark_prime_drug"),
                ("stark_prime_drug", "indication", "stark_prime_malady"),
                ("stark_prime_malady", "indication", "stark_prime_drug"),
            }),
        ),
    )
    subtypes = {
        f.subtype for f in source.run("r").facts if isinstance(f, NodeFact)
    }
    assert "stark_prime_malady" in subtypes
    assert "stark_prime_disease" not in subtypes


def test_an_undeclared_type_is_refused_not_bucketed(tmp_path: Path) -> None:
    """The promoted-from source bucketed an unknown type into a generic
    `stark_prime_entity`. That is a silent fallback; this raises."""
    build_release(
        tmp_path, node_types={0: "gene/protein", 1: "drug", 2: "malady"})
    source = StarkPrimeSource(
        processed_dir=str(tmp_path / "skb" / "prime" / "processed"),
        snapshot_root=str(tmp_path),
        expect=FIXTURE_RELEASE,
    )
    with pytest.raises(StarkPrimeError) as excinfo:
        source.run("r")
    message = str(excinfo.value)
    assert "entity subtype" in message
    assert "stark_prime_malady" in message
    assert "generic bucket" in message


def test_an_undeclared_triple_is_refused(tmp_path: Path) -> None:
    """A release realising a triple the ontology does not narrow must stop
    the build, not be rejected one edge at a time at insert."""
    build_release(
        tmp_path,
        edges=[*FIXTURE_EDGES, (3, 0, 3)],  # disease -ppi-> disease
    )
    source = StarkPrimeSource(
        processed_dir=str(tmp_path / "skb" / "prime" / "processed"),
        snapshot_root=str(tmp_path),
        expect=ReleaseExpectation(
            label="fixture with an extra triple",
            entity_count=4,
            edge_row_count=7,
            subtypes=FIXTURE_RELEASE.subtypes,
            edge_types=FIXTURE_RELEASE.edge_types,
            triples=FIXTURE_RELEASE.triples,
        ),
    )
    with pytest.raises(StarkPrimeError, match="does not declare"):
        source.run("r")


def test_a_release_of_the_wrong_size_is_refused(tmp_path: Path) -> None:
    """Same vocabulary, different size: the contract floors, the triple
    fingerprint and the adapter's answer-id bound are all keyed to the
    pinned counts, so a size change stops the build."""
    build_release(tmp_path)
    source = StarkPrimeSource(
        processed_dir=str(tmp_path / "skb" / "prime" / "processed"),
        snapshot_root=str(tmp_path),
        expect=ReleaseExpectation(
            label="an expectation with the wrong counts",
            entity_count=4000,
            edge_row_count=6000,
            subtypes=FIXTURE_RELEASE.subtypes,
            edge_types=FIXTURE_RELEASE.edge_types,
            triples=FIXTURE_RELEASE.triples,
        ),
    )
    with pytest.raises(StarkPrimeError, match="declares 4000 / 6000"):
        source.run("r")


def test_the_pinned_expectation_refuses_a_fixture_release(
    tmp_path: Path,
) -> None:
    """The DEFAULT expectation is the real release, so pointing the source
    at anything else without saying so fails. Without this the fixtures
    above could be passing because the guards are inert."""
    build_release(tmp_path)
    source = StarkPrimeSource(
        processed_dir=str(tmp_path / "skb" / "prime" / "processed"),
        snapshot_root=str(tmp_path),
    )
    with pytest.raises(StarkPrimeError, match="entity subtype"):
        source.run("r")


def test_the_release_disagreeing_with_itself_is_refused(
    tmp_path: Path,
) -> None:
    """`node_info['type']` and `node_types.pt` both name a type. They must
    agree; the promoted-from source read only the first."""
    processed = build_release(tmp_path)
    node_info = pickle.loads((processed / "node_info.pkl").read_bytes())
    node_info[3]["type"] = "drug"           # node_types.pt says disease
    (processed / "node_info.pkl").write_bytes(pickle.dumps(node_info))
    source = StarkPrimeSource(
        processed_dir=str(processed), snapshot_root=str(tmp_path),
        expect=FIXTURE_RELEASE,
    )
    with pytest.raises(StarkPrimeError, match="disagrees with itself"):
        list(source.run("r").facts)


# --------------------------------------------------------------------------
# identity, attributes, isolation
# --------------------------------------------------------------------------


def test_node_ids_are_derivable_from_a_gold_answer_id(tmp_path: Path) -> None:
    """STaRK gold is a set of entity INDICES. A node id an arm cannot
    derive from one without a lookup is a corpus the benchmark cannot be
    scored against."""
    _, _, facts = run_fixture(tmp_path)
    ids = {f.node_id for f in facts if isinstance(f, NodeFact)}
    assert ids == {f"stark_prime:entity:{i}" for i in range(4)}
    assert node_id_for(3) == "stark_prime:entity:3"
    assert all(
        f.attrs["entity_index"] == int(f.node_id.rsplit(":", 1)[1])
        for f in facts if isinstance(f, NodeFact)
    )


def test_entity_text_uses_a_declared_key_order(tmp_path: Path) -> None:
    """A release that adds a `details` field must not silently rewrite
    every entity's retrieval text under a running benchmark."""
    build_release(
        tmp_path,
        details={
            2: {"description": "an analgesic", "indication": "pain",
                "a_brand_new_field": "SHOULD NOT APPEAR"},
        },
    )
    _, _, facts = run_fixture(tmp_path)
    drug = next(
        f for f in facts
        if isinstance(f, NodeFact) and f.attrs["name"] == "aspirin"
    )
    assert drug.attrs["text"] == (
        "aspirin\ndescription: an analgesic\nindication: pain"
    )
    assert "SHOULD NOT APPEAR" not in drug.attrs["text"]
    assert drug.attrs["text_truncated"] is False


def test_a_full_build_refuses_an_isolated_entity(tmp_path: Path) -> None:
    """The release declares minimum degree 2 and zero isolates, so an
    isolate on a complete build means edge rows were lost. No stamp
    forgives it: a stamp derived from the same edge_index the emission
    walks could not tell a lost row from an upstream gap."""
    build_release(
        tmp_path,
        entity_type=[*FIXTURE_ENTITY_TYPE, 1],   # a fifth, unreferenced
        names=[*FIXTURE_NAMES, "orphan drug"],
    )
    source = StarkPrimeSource(
        processed_dir=str(tmp_path / "skb" / "prime" / "processed"),
        snapshot_root=str(tmp_path),
        expect=ReleaseExpectation(
            label="fixture with an isolate",
            entity_count=5,
            edge_row_count=6,
            subtypes=FIXTURE_RELEASE.subtypes,
            edge_types=FIXTURE_RELEASE.edge_types,
            triples=FIXTURE_RELEASE.triples,
        ),
    )
    with pytest.raises(StarkPrimeError, match="no incident edge"):
        source.run("r")


def test_a_full_build_stamps_nothing_and_strands_nothing(
    tmp_path: Path,
) -> None:
    """The COMPLEMENT of the stranding rule, measured: on a complete build
    every entity is connected and NONE carries `no_relations_reason`. A
    stamp present on all or on none would forgive everything or nothing."""
    source, _, facts = run_fixture(tmp_path)
    nodes = [f for f in facts if isinstance(f, NodeFact)]
    assert [f for f in nodes if "no_relations_reason" in f.attrs] == []
    assert source.last_run_stats["stranded_entities_by_reason"] == {}
    connected = {f.src for f in facts if isinstance(f, EdgeFact)} | {
        f.dst for f in facts if isinstance(f, EdgeFact)
    }
    assert connected == {f.node_id for f in nodes}


def test_a_bounded_build_stamps_what_the_bound_dropped(
    tmp_path: Path,
) -> None:
    """Under `max_entities` an entity CAN lose every incident edge. The
    stamp names the bound as the cause and records the entity's degree in
    the WHOLE release, so the loss is quantified rather than excused."""
    source, run, facts = run_fixture(tmp_path, max_entities=3)
    nodes = [f for f in facts if isinstance(f, NodeFact)]
    edges = [f for f in facts if isinstance(f, EdgeFact)]
    assert len(nodes) == 3
    # entity 2 (aspirin) keeps its target edges to gene 0; entity 3
    # (migraine) is outside the bound entirely.
    assert {(f.src, f.edge_type, f.dst) for f in edges} == {
        (node_id_for(0), "ppi", node_id_for(1)),
        (node_id_for(1), "ppi", node_id_for(0)),
        (node_id_for(2), "target", node_id_for(0)),
        (node_id_for(0), "target", node_id_for(2)),
    }
    assert run.completed_scope is False
    assert "NOT the full release" in (run.health.reason or "")
    assert run.next_cursor["mode"] == "bounded_release"
    assert [f for f in nodes if "no_relations_reason" in f.attrs] == []

    # ...and a tighter bound DOES strand one, with a stated reason.
    source, _, facts = run_fixture(tmp_path, max_entities=2)
    nodes = [f for f in facts if isinstance(f, NodeFact)]
    assert len(nodes) == 2
    assert source.last_run_stats["stranded_entities_by_reason"] == {}
    source, _, facts = run_fixture(tmp_path, max_entities=1)
    stranded = [
        f for f in facts
        if isinstance(f, NodeFact) and "no_relations_reason" in f.attrs
    ]
    assert len(stranded) == 1
    assert stranded[0].attrs["no_relations_reason"] == (
        "all_declared_edges_leave_the_bounded_subgraph"
    )
    # gene 0's degree in the WHOLE fixture release: two ppi rows plus two
    # target rows. The bound kept none of them, and the stamp says so.
    assert stranded[0].attrs["declared_degree"] == 4


def test_the_census_reports_what_it_emitted(tmp_path: Path) -> None:
    source, _, facts = run_fixture(tmp_path)
    stats = source.last_run_stats
    assert stats["entities_emitted"] == 4
    assert stats["edge_rows_emitted"] == 6
    assert stats["distinct_triples"] == 5
    assert stats["entities_by_subtype"] == {
        "stark_prime_disease": 1,
        "stark_prime_drug": 1,
        "stark_prime_gene_protein": 2,
    }
    emitted_edges = [f for f in facts if isinstance(f, EdgeFact)]
    assert sum(stats["edge_rows_by_triple"].values()) == len(emitted_edges)


# --------------------------------------------------------------------------
# the pinned release fingerprint agrees with the ontology module
# --------------------------------------------------------------------------


def test_the_change_probe_sees_a_byte_change_in_the_edge_tensor(
    tmp_path: Path,
) -> None:
    """No `max_bytes_per_item` cap: the edge tensors are exactly where a
    truncated read would permit a false-unchanged verdict."""
    processed = build_release(tmp_path)
    source = StarkPrimeSource(
        processed_dir=str(processed), snapshot_root=str(tmp_path),
        expect=FIXTURE_RELEASE,
    )
    probe = source._build_change_probe()
    before = probe.build_token()

    # same probe, same bytes -> same token
    assert StarkPrimeSource(
        processed_dir=str(processed), snapshot_root=str(tmp_path),
        expect=FIXTURE_RELEASE,
    )._build_change_probe().build_token() == before

    # one relabelled edge row -> a different token
    write_int_tensor(
        processed / "edge_types.pt", (len(FIXTURE_EDGES),),
        [2] + [e[1] for e in FIXTURE_EDGES[1:]])
    after = StarkPrimeSource(
        processed_dir=str(processed), snapshot_root=str(tmp_path),
        expect=FIXTURE_RELEASE,
    )._build_change_probe().build_token()
    assert after != before

    # and the BOUND is in the token: the same bytes under a different
    # bound produce different facts, so an unchanged token would be a lie
    bounded = StarkPrimeSource(
        processed_dir=str(processed), snapshot_root=str(tmp_path),
        expect=FIXTURE_RELEASE, max_entities=2,
    )._build_change_probe().build_token()
    assert bounded != after


def test_the_ontology_module_declares_exactly_the_release_triples() -> None:
    """The narrowing catalog and the source's fingerprint must state the
    same 56 triples. If they drift, the source emits edges the catalog
    rejects at insert — one at a time, with no statement of why."""
    import yaml

    from okg.substrate.library.ontologies import __file__ as ontologies_init

    schema = yaml.safe_load(
        (Path(ontologies_init).parent / "stark_prime" / "schema.yaml")
        .read_text(encoding="utf-8")
    )
    classes = schema["classes"]
    declared_subtypes = {
        _pascal_to_snake(name) for name in classes if name != "EdgeNarrowings"
    }
    assert declared_subtypes == set(PRIME_SUBTYPES)

    narrowed = set()
    for attr in classes["EdgeNarrowings"]["attributes"].values():
        annotations = attr["annotations"]
        narrowed.add((
            _pascal_to_snake(annotations["src_subtypes"]),
            annotations["edge_archetype"],
            _pascal_to_snake(annotations["dst_subtypes"]),
        ))
    assert narrowed == set(PRIME_RELEASE_TRIPLES)
    assert len(narrowed) == 56
    assert {t[1] for t in narrowed} == set(PRIME_EDGE_TYPES)


def test_the_release_is_symmetric_in_the_declared_triples() -> None:
    """Every declared triple's mirror is declared too, with the same row
    count — the shape a perfectly symmetric release has. Measured on the
    release: 0 self-loops and 0 rows without their reverse, so a
    one-directional triple here would mean the fingerprint was edited by
    hand rather than generated."""
    for (src, rel, dst), rows in PRIME_RELEASE_TRIPLES.items():
        assert (dst, rel, src) in PRIME_RELEASE_TRIPLES, (src, rel, dst)
        assert PRIME_RELEASE_TRIPLES[(dst, rel, src)] == rows
    assert sum(PRIME_RELEASE_TRIPLES.values()) == (
        PINNED_PRIME_RELEASE.edge_row_count
    )


def _pascal_to_snake(name: str) -> str:
    import re

    return re.sub(r"(?<!^)(?=[A-Z])", "_", name).lower()


def test_no_non_finite_float_reaches_an_attr() -> None:
    """NaN AND +/-inf are both stripped, because the attrs blob has to be
    valid JSON and `json.dumps(float("inf"))` emits a bare `Infinity`, which
    no conforming parser accepts.

    The guard originally tested only NaN, so an inf in the release would have
    written an unparseable attrs blob silently, at ingest time, far from
    wherever it eventually failed to read.
    """
    import json

    from okg.substrate.library.sources.stark_prime import _jsonable_scalar

    for bad in (float("nan"), float("inf"), float("-inf")):
        assert _jsonable_scalar(bad) is None, bad

    # ...and finite values are untouched, so the guard discriminates rather
    # than nulling every float it sees.
    for good in (0.0, -1.5, 3.25):
        assert _jsonable_scalar(good) == good

    # The point of stripping: what survives must round-trip through STRICT
    # json, which is what a downstream reader will use.
    blob = {"a": _jsonable_scalar(float("inf")), "b": _jsonable_scalar(2.5)}
    assert json.loads(json.dumps(blob, allow_nan=False)) == {"a": None, "b": 2.5}
