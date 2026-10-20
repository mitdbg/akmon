"""PACT ``concord-reuse-defect-isolation`` — req.rdi.affordable.

A history replay re-reads the WHOLE corpus at every update. For the 22-update
prose history that is 3,806 model calls, 6.3M input tokens and about four
dollars — when one document changed and the other 167 produce identical claims
paid for 22 times. Cost is the reason every realistic result in this program is
a single sample, the divergence finding included.

The reuse is keyed on the document's CONTENT DIGEST. These witnesses pin the
property that makes that safe, and the two that make it honest.
"""
from __future__ import annotations

import hashlib
from pathlib import Path


from okg.bench.synth.llm_extract import ExtractionCost
from okg.bench.synth.runner import LlmLane


class _CountingExtractor:
    """Stands in for the model. Counts what it was asked to read."""

    def __init__(self) -> None:
        self.reads: list[str] = []
        self.cost = ExtractionCost()
        self.labels: tuple[str, ...] = ()

    def extract_document(self, relpath: str, text: str, digest: str):
        self.reads.append(relpath)
        return [], f"response for {relpath}", 1, []


def _lane(**kw) -> LlmLane:
    lane = LlmLane(extractor=_CountingExtractor(), **kw)  # type: ignore[arg-type]

    def _extract_doc(relpath, text, digest):
        return lane.extractor.extract_document(relpath, text, digest)

    object.__setattr__(lane, "_extract_doc", _extract_doc)
    return lane


def _corpus(tmp_path: Path, docs: dict[str, str]) -> Path:
    root = tmp_path / "corpus"
    root.mkdir(exist_ok=True)
    for name, text in docs.items():
        (root / name).write_text(text, encoding="utf-8")
    return root


def test_off_by_default_so_no_existing_result_moves() -> None:
    assert LlmLane.__dataclass_fields__["reuse_unchanged"].default is False


def test_an_unchanged_document_is_not_read_twice(tmp_path: Path) -> None:
    """THE saving."""
    lane = _lane(reuse_unchanged=True)
    root = _corpus(tmp_path, {"a.md": "# A\n\n## S\n\nx was 1.\n"})
    lane.extract(root, (), phase="p0")
    lane.extract(root, (), phase="p1")
    assert lane.extractor.reads == ["a.md"], (
        f"an unchanged document was read {len(lane.extractor.reads)} times"
    )


def test_a_changed_document_IS_read_again_under_the_same_name(tmp_path: Path) -> None:
    """THE safety property. A name-keyed cache would serve a stale reading for a
    document that changed, corrupting every downstream measurement in a way no
    existing check catches."""
    lane = _lane(reuse_unchanged=True)
    root = _corpus(tmp_path, {"a.md": "# A\n\n## S\n\nx was 1.\n"})
    lane.extract(root, (), phase="p0")
    (root / "a.md").write_text("# A\n\n## S\n\nx was 2.\n", encoding="utf-8")
    lane.extract(root, (), phase="p1")
    assert lane.extractor.reads == ["a.md", "a.md"], (
        "the document's bytes changed and it was NOT re-read — the cache is "
        "keyed on the name rather than the content digest"
    )


def test_reverting_to_earlier_bytes_reuses_that_reading(tmp_path: Path) -> None:
    """The key is the digest, so identical bytes hit whenever they recur."""
    lane = _lane(reuse_unchanged=True)
    root = _corpus(tmp_path, {"a.md": "# A\n\n## S\n\nx was 1.\n"})
    lane.extract(root, (), phase="p0")
    (root / "a.md").write_text("# A\n\n## S\n\nx was 2.\n", encoding="utf-8")
    lane.extract(root, (), phase="p1")
    (root / "a.md").write_text("# A\n\n## S\n\nx was 1.\n", encoding="utf-8")
    lane.extract(root, (), phase="p2")
    assert len(lane.extractor.reads) == 2, lane.extractor.reads


def test_with_reuse_off_every_pass_reads_again(tmp_path: Path) -> None:
    """NON-VACUITY: the saving above is the flag's doing, not the harness."""
    lane = _lane(reuse_unchanged=False)
    root = _corpus(tmp_path, {"a.md": "# A\n\n## S\n\nx was 1.\n"})
    lane.extract(root, (), phase="p0")
    lane.extract(root, (), phase="p1")
    assert lane.extractor.reads == ["a.md", "a.md"]


def test_only_one_of_several_documents_is_re_read(tmp_path: Path) -> None:
    """The realistic shape: one document changes, the rest are free."""
    lane = _lane(reuse_unchanged=True)
    docs = {f"d{i}.md": f"# D{i}\n\n## S\n\nx was {i}.\n" for i in range(5)}
    root = _corpus(tmp_path, docs)
    lane.extract(root, (), phase="p0")
    assert len(lane.extractor.reads) == 5
    (root / "d2.md").write_text("# D2\n\n## S\n\nx was 99.\n", encoding="utf-8")
    lane.extract(root, (), phase="p1")
    assert lane.extractor.reads[5:] == ["d2.md"], lane.extractor.reads[5:]


def test_the_digest_is_over_content_not_path(tmp_path: Path) -> None:
    """Two names, identical bytes, are still two documents: a claim carries its
    source document, so collapsing them would lose a rendering."""
    lane = _lane(reuse_unchanged=True)
    same = "# A\n\n## S\n\nx was 1.\n"
    root = _corpus(tmp_path, {"a.md": same, "b.md": same})
    lane.extract(root, (), phase="p0")
    assert sorted(lane.extractor.reads) == ["a.md", "b.md"], (
        "identical bytes under two names must both be read — the key is "
        "(path, digest), not the digest alone"
    )
    assert hashlib.sha256(same.encode()).hexdigest()
