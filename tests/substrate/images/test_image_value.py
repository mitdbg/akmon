"""The image value and its rules (PACT first-class-images, req value).

`okg.substrate.images.values` is the one definition of an image value. These
tests hold its accept and refuse cases, prove the SDK builder's output passes
the real fact pipeline's JSON checks unchanged, and pin the write-path check
(`check_incoming`) and the read-side classifier (`classify`).
"""
from __future__ import annotations

import uuid
from types import MappingProxyType

import pytest

from okg.substrate.errors import SubstrateError
from okg.substrate.fact_batch import FactChunk, _thaw_json
from okg.substrate.images import values as v
from okg.substrate.library.sources.base import NodeFact

SHA_A = "a" * 64
SHA_B = "b" * 64


def _png(**overrides):
    png = {
        "id": SHA_A, "sha256": SHA_B, "mime_type": "image/png",
        "width": 640, "height": 480, "byte_count": 12345,
    }
    png.update(overrides)
    return png


# ── paths ──────────────────────────────────────────────────────────────

@pytest.mark.parametrize("path", [
    "f.png",
    "figs/f1.pdf",
    "a/b/c/d.jpeg",
    "with space/f.png",
    "dots..in.name.png",
    ".hidden/f.png",
    "unicodé/图.png",
    "c:/not-a-drive-on-posix.png",
])
def test_relative_posix_paths_are_accepted(path):
    assert v.path_problem(path) is None
    assert v.file_value(path) == {"okg_image": "file", "path": path}


@pytest.mark.parametrize(("path", "needle"), [
    ("", "empty"),
    ("/etc/passwd", "absolute"),
    ("/abs/f.png", "absolute"),
    ("../f.png", "'..'"),
    ("a/../../f.png", "'..'"),
    ("a/..", "'..'"),
    ("./f.png", "'.'"),
    ("a/./f.png", "'.'"),
    ("a//f.png", "empty segment"),
    ("a/", "empty segment"),
    ("a\\f.png", "backslash"),
    ("..\\f.png", "backslash"),
    ("a\x00.png", "NUL"),
    (None, "string"),
    (7, "string"),
    (b"f.png", "string"),
])
def test_bad_paths_are_refused_with_image_ref_invalid(path, needle):
    problem = v.path_problem(path)
    assert problem is not None and needle in problem
    with pytest.raises(v.ImageRefInvalid) as caught:
        v.file_value(path)
    assert caught.value.code == "image_ref_invalid"
    assert caught.value.data["field"] == "path"
    assert isinstance(caught.value, SubstrateError)


# ── URLs ───────────────────────────────────────────────────────────────

@pytest.mark.parametrize("url", [
    "https://cds.cern.ch/record/1/files/f.png",
    "https://example.org",
    "https://example.org:8443/a.png?x=1#frag",
    "HTTPS://Example.org/a.png",
    "https://[2001:db8::1]/a.png",
])
def test_https_urls_with_a_host_are_accepted(url):
    assert v.url_problem(url) is None
    assert v.url_value(url) == {"okg_image": "url", "url": url}


@pytest.mark.parametrize(("url", "needle"), [
    ("", "empty"),
    ("http://example.org/a.png", "https"),
    ("ftp://example.org/a.png", "https"),
    ("file:///etc/passwd", "https"),
    ("example.org/a.png", "https"),
    ("https:///a.png", "no host"),
    ("https://", "no host"),
    ("https://user:pw@example.org/a.png", "credentials"),
    ("https://user@example.org/a.png", "credentials"),
    ("https://@example.org/a.png", "credentials"),
    ("https://example.org:notaport/a.png", "parse"),
    ("https://example.org/a b.png", "whitespace"),
    ("https://example.org/a.png\n", "whitespace"),
    ("https://example.org/\x00.png", "NUL"),
    (None, "string"),
    (["https://example.org"], "string"),
])
def test_bad_urls_are_refused_with_image_ref_invalid(url, needle):
    problem = v.url_problem(url)
    assert problem is not None and needle in problem
    with pytest.raises(v.ImageRefInvalid) as caught:
        v.url_value(url)
    assert caught.value.code == "image_ref_invalid"
    assert caught.value.data["field"] == "url"


# ── the SDK builder ────────────────────────────────────────────────────

def test_sdk_image_ref_builds_plain_json_dicts():
    import okg.deployment as sdk

    assert sdk.ImageRef is v.ImageRef
    file_ref = sdk.ImageRef.file("figs/f1.pdf")
    url_ref = sdk.ImageRef.url("https://example.org/a.png")
    assert type(file_ref) is dict and type(url_ref) is dict
    assert file_ref == {"okg_image": "file", "path": "figs/f1.pdf"}
    assert url_ref == {"okg_image": "url", "url": "https://example.org/a.png"}
    with pytest.raises(v.ImageRefInvalid):
        sdk.ImageRef.file("../escape.png")
    with pytest.raises(v.ImageRefInvalid):
        sdk.ImageRef.url("http://example.org/a.png")


def test_image_ref_is_not_instantiated():
    with pytest.raises(TypeError, match="plain dicts"):
        v.ImageRef()


def test_image_values_pass_the_real_fact_freeze_unchanged():
    """The values go through `FactChunk.create`, which runs
    `_freeze_safe_json` on every NodeFact's attrs, and come back equal."""
    import okg.deployment as sdk

    attrs = {
        "figure": sdk.ImageRef.file("figs/f1.pdf"),
        "panels": [sdk.ImageRef.file("figs/a.png"), sdk.ImageRef.url("https://example.org/b.jpeg")],
        "caption": "Figure 1",
    }
    fact = NodeFact(
        node_id="fig-1", subtype="Figure", attrs=attrs,
        source_record_id={"id": "fig-1"}, source_revision={"rev": "1"},
    )
    chunk = FactChunk.create(run_id=uuid.uuid4(), ordinal=0, facts=[fact])
    (frozen,) = chunk.facts
    assert _thaw_json(frozen.attrs) == attrs
    # The frozen attrs are still found and still pass the write-path check.
    sites = list(v.iter_image_values(frozen.attrs))
    assert [(s.attr, s.index) for s in sites] == [("figure", None), ("panels", 0), ("panels", 1)]
    v.check_incoming(frozen.attrs)


def test_a_completed_value_passes_the_fact_freeze_too():
    """Ingest writes completed values through the same pipeline."""
    done = v.with_png(v.file_value("f.png"), **_png())
    fact = NodeFact(
        node_id="fig-1", subtype="Figure", attrs={"figure": done},
        source_record_id={"id": "fig-1"}, source_revision={"rev": "1"},
    )
    (frozen,) = FactChunk.create(run_id=uuid.uuid4(), ordinal=0, facts=[fact]).facts
    assert _thaw_json(frozen.attrs) == {"figure": done}


# ── iter_image_values ──────────────────────────────────────────────────

def test_iter_finds_attr_values_and_list_elements_in_attr_order():
    attrs = {
        "z": v.file_value("z.png"),
        "a": [v.url_value("https://e.org/1.png"), "text", {"okg_image": "file", "path": "x"}],
        "nested": {"inner": v.file_value("deep.png")},
        "list_of_lists": [[v.file_value("deeper.png")]],
        "plain": "x",
        "tuple": (v.file_value("t.png"),),
        "bad": {"okg_image": "mystery"},
    }
    sites = [(s.attr, s.index, s.value) for s in v.iter_image_values(attrs)]
    assert sites == [
        ("a", 0, {"okg_image": "url", "url": "https://e.org/1.png"}),
        ("a", 2, {"okg_image": "file", "path": "x"}),
        ("bad", None, {"okg_image": "mystery"}),
        ("tuple", 0, {"okg_image": "file", "path": "t.png"}),
        ("z", None, {"okg_image": "file", "path": "z.png"}),
    ]


@pytest.mark.parametrize("attrs", [None, [], "x", 3])
def test_iter_over_a_non_mapping_yields_nothing(attrs):
    assert list(v.iter_image_values(attrs)) == []


def test_any_mapping_tagged_okg_image_counts_including_a_proxy():
    proxy = MappingProxyType({"okg_image": "file", "path": "f.png"})
    assert v.is_image_value(proxy)
    assert v.is_image_value({"okg_image": None})
    assert not v.is_image_value({"image": "file"})
    assert not v.is_image_value("okg_image")


# ── check_incoming ─────────────────────────────────────────────────────

def test_check_incoming_accepts_references_and_unrelated_attrs():
    v.check_incoming({
        "figure": v.file_value("f.png"),
        "panels": [v.url_value("https://e.org/a.png")],
        "title": "t",
        "meta": {"path": "/abs/is/fine/here", "error": "not an image value"},
    })
    v.check_incoming({})
    v.check_incoming(None)


@pytest.mark.parametrize("value", [
    {"okg_image": "file", "path": "f.png", "png": _png()},
    {"okg_image": "file", "path": "f.png", "error": "image_file_missing"},
    {"okg_image": "file", "path": "f.png", "png": None},
    {"okg_image": "url", "url": "https://e.org/a.png", "png": _png()},
    {"okg_image": "url", "url": "https://e.org/a.png", "error": "x"},
    # Forgery is decided before the kind: a forged value of an unknown kind
    # is still a forgery.
    {"okg_image": "mystery", "error": "image_file_missing"},
])
@pytest.mark.parametrize("position", ["attr", "list"])
def test_check_incoming_refuses_png_or_error_as_forged(value, position):
    attrs = {"figure": value} if position == "attr" else {"figure": ["x", value]}
    with pytest.raises(v.ImageValueForged) as caught:
        v.check_incoming(attrs)
    err = caught.value
    assert err.code == "image_value_forged"
    assert err.data["attr"] == "figure"
    assert err.data["index"] == (None if position == "attr" else 1)
    assert set(err.data["keys"]) <= {"png", "error"} and err.data["keys"]


@pytest.mark.parametrize(("value", "field"), [
    ({"okg_image": "stored", "sha256": SHA_A}, "okg_image"),
    ({"okg_image": "unavailable", "reason": "x"}, "okg_image"),
    ({"okg_image": None, "path": "f.png"}, "okg_image"),
    ({"okg_image": 1, "path": "f.png"}, "okg_image"),
    ({"okg_image": "FILE", "path": "f.png"}, "okg_image"),
    ({"okg_image": "file"}, "path"),
    ({"okg_image": "file", "path": "../f.png"}, "path"),
    ({"okg_image": "file", "path": "/abs.png"}, "path"),
    ({"okg_image": "file", "path": "a\\b.png"}, "path"),
    ({"okg_image": "file", "path": "a\x00.png"}, "path"),
    ({"okg_image": "file", "path": "f.png", "sha256": SHA_A}, "keys"),
    ({"okg_image": "file", "path": "f.png", "url": "https://e.org"}, "keys"),
    ({"okg_image": "url"}, "url"),
    ({"okg_image": "url", "url": "http://e.org/a.png"}, "url"),
    ({"okg_image": "url", "url": "https://u:p@e.org/a.png"}, "url"),
    ({"okg_image": "url", "url": "https://e.org/a.png", "path": "f.png"}, "keys"),
])
def test_check_incoming_refuses_an_invalid_reference(value, field):
    with pytest.raises(v.ImageRefInvalid) as caught:
        v.check_incoming({"figure": value})
    assert caught.value.code == "image_ref_invalid"
    assert caught.value.data["field"] == field
    assert caught.value.data["attr"] == "figure"


def test_check_incoming_reports_the_first_offender_in_attr_order():
    attrs = {
        "b": {"okg_image": "file", "path": "f.png", "png": _png()},
        "a": {"okg_image": "file", "path": "../f.png"},
    }
    with pytest.raises(v.ImageRefInvalid) as caught:
        v.check_incoming(attrs)
    assert caught.value.data["attr"] == "a"


def test_a_value_nested_deeper_is_ordinary_data():
    """Only the attr value and list elements are image values; a tagged
    mapping deeper down is not read as an image by anyone, so it is data."""
    v.check_incoming({"meta": {"fig": {"okg_image": "file", "path": "f.png", "png": _png()}}})
    v.check_incoming({"meta": [[{"okg_image": "file", "error": "x"}]]})


# ── with_png / with_error ──────────────────────────────────────────────

def test_with_png_and_with_error_return_new_completed_dicts():
    ref = v.file_value("figs/f.png")
    done = v.with_png(ref, **_png())
    assert done == {"okg_image": "file", "path": "figs/f.png", "png": _png()}
    assert ref == {"okg_image": "file", "path": "figs/f.png"}  # not mutated
    failed = v.with_error(ref, "pdf_renderer_unavailable")
    assert failed == {"okg_image": "file", "path": "figs/f.png", "error": "pdf_renderer_unavailable"}
    assert v.classify(done).state == "png"
    assert v.classify(failed).state == "error"


def test_every_reason_is_in_exactly_one_closed_set():
    assert v.PRE_HASH_REASONS == {
        "image_root_undeclared", "image_path_refused", "image_file_missing", "image_too_large",
    }
    assert v.POST_HASH_REASONS == {
        "image_format_unsupported", "pdf_renderer_unavailable", "pdf_render_failed", "image_decode_failed",
        "image_render_limit_exceeded",
    }
    assert not v.PRE_HASH_REASONS & v.POST_HASH_REASONS
    for reason in v.IMAGE_ERROR_REASONS:
        assert v.classify(v.with_error(v.file_value("f.png"), reason)).reason == reason


def test_with_error_refuses_an_unknown_reason():
    with pytest.raises(ValueError, match="not one of"):
        v.with_error(v.file_value("f.png"), "image_unavailable")


@pytest.mark.parametrize("override", [
    {"id": "not-hex"},
    {"sha256": "A" * 64},
    {"sha256": "a" * 63},
    {"mime_type": "image/jpeg"},
    {"width": 0},
    {"height": -1},
    {"byte_count": True},
    {"width": 1.5},
])
def test_with_png_refuses_a_malformed_png(override):
    with pytest.raises(ValueError, match="cannot complete"):
        v.with_png(v.file_value("f.png"), **_png(**override))


@pytest.mark.parametrize(("value", "error"), [
    (v.url_value("https://e.org/a.png"), v.ImageRefInvalid),
    ({"okg_image": "file", "path": "f.png", "error": "image_file_missing"}, v.ImageValueForged),
    ({"okg_image": "file", "path": "../f.png"}, v.ImageRefInvalid),
    ("f.png", v.ImageRefInvalid),
])
def test_only_an_uncompleted_valid_file_value_can_be_completed(value, error):
    with pytest.raises(error):
        v.with_png(value, **_png())
    with pytest.raises(error):
        v.with_error(value, "image_file_missing")


# ── classify ───────────────────────────────────────────────────────────

def test_classify_reads_each_valid_state():
    png = v.classify(v.with_png(v.file_value("a/f.png"), **_png()))
    assert png.state == "png" and png.path == "a/f.png"
    assert png.png == v.StoredPng(**_png())
    assert png.reason is None and png.problem is None

    err = v.classify(v.with_error(v.file_value("a/f.png"), "image_too_large"))
    assert (err.state, err.path, err.reason, err.png) == ("error", "a/f.png", "image_too_large", None)

    raw = v.classify(v.file_value("a/f.png"))
    assert (raw.state, raw.path) == ("not_ingested", "a/f.png")

    url = v.classify(v.url_value("https://e.org/a.png"))
    assert (url.state, url.url, url.path) == ("url", "https://e.org/a.png", None)


def test_classify_reads_a_frozen_value():
    frozen = MappingProxyType({
        "okg_image": "file", "path": "f.png", "png": MappingProxyType(_png()),
    })
    assert v.classify(frozen).state == "png"


@pytest.mark.parametrize("value", [
    "f.png",
    None,
    {"path": "f.png"},
    {"okg_image": "stored", "sha256": SHA_A},
    {"okg_image": "file"},
    {"okg_image": "file", "path": "/abs.png"},
    {"okg_image": "file", "path": "f.png", "png": _png(), "error": "image_file_missing"},
    {"okg_image": "file", "path": "f.png", "png": _png(mime_type="image/jpeg")},
    {"okg_image": "file", "path": "f.png", "png": {**_png(), "extra": 1}},
    {"okg_image": "file", "path": "f.png", "png": "x"},
    {"okg_image": "file", "path": "f.png", "error": "made_up_reason"},
    {"okg_image": "file", "path": "f.png", "error": None},
    {"okg_image": "file", "path": "f.png", "sha256": SHA_A},
    {"okg_image": "url", "url": "http://e.org/a.png"},
    {"okg_image": "url", "url": "https://e.org/a.png", "png": _png()},
])
def test_classify_reports_a_malformed_value_as_invalid_without_raising(value):
    got = v.classify(value)
    assert got.state == "invalid"
    assert got.problem
    assert got.png is None


# ── mappings whose views disagree (adversarial review, finding 5) ───────

class _HidesPngFromIteration(dict):
    """Iteration and keys() hide ``png``; the stored entries and items()
    still carry it, so a writer that serializes through items() would
    store it."""

    def __iter__(self):
        return iter([k for k in dict.keys(self) if k != "png"])

    def keys(self):
        return [k for k in dict.keys(self) if k != "png"]

    def __contains__(self, key):
        return key != "png" and dict.__contains__(self, key)


class _InventsPngInItems(dict):
    """Stored entries are a bare reference; items() adds ``png``."""

    def items(self):
        return [*dict.items(self), ("png", _png())]


class _HidesTagFromContains(dict):
    """``okg_image in value`` is False, but the entries carry the tag."""

    def __contains__(self, key):
        return key != "okg_image" and dict.__contains__(self, key)


class _ListHidesElement(list):
    def __iter__(self):
        return iter([item for item in list.__iter__(self) if not isinstance(item, dict)])


class _SneakyKey(str):
    """A key that is ``"png"`` to a serializer but hides from a set."""

    def __hash__(self):
        return 0

    def __eq__(self, other):
        return False


def _forged_bare(**extra):
    return {"okg_image": "file", "path": "f.png", **extra}


@pytest.mark.parametrize(
    "attrs",
    [
        {"figure": _HidesPngFromIteration(_forged_bare(png=_png()))},
        {"figure": [_HidesPngFromIteration(_forged_bare(png=_png()))]},
        {"figure": _InventsPngInItems(_forged_bare())},
        {"figure": _HidesTagFromContains(_forged_bare(png=_png()))},
        {"figure": _ListHidesElement(["text", _forged_bare(png=_png())])},
        _HidesPngFromIteration({"figure": "x", "png": _forged_bare(png=_png())}),
    ],
    ids=["hides-png", "hides-png-in-list", "items-invents-png", "hides-tag",
         "list-hides-element", "attrs-hide-an-attr"],
)
def test_check_incoming_reads_real_entries_not_overridden_views(attrs):
    """A dict or list subclass cannot hide ``png`` from the check while a
    writer still serializes it: the check reads every view and refuses
    when they disagree."""
    with pytest.raises((v.ImageRefInvalid, v.ImageValueForged)) as raised:
        v.check_incoming(attrs)
    assert raised.value.code in {"image_ref_invalid", "image_value_forged"}
    with pytest.raises((v.ImageRefInvalid, v.ImageValueForged)):
        v.check_record_write(attrs)


def test_a_str_subclass_key_cannot_hide_png():
    value = {"okg_image": "file", "path": "f.png", _SneakyKey("png"): _png()}
    with pytest.raises(v.ImageValueForged):
        v.check_incoming({"figure": value})


def test_consistent_mapping_types_still_pass():
    """A proxy or a dict subclass whose views agree is read normally."""

    class Plain(dict):
        pass

    v.check_incoming(MappingProxyType({
        "figure": MappingProxyType(v.file_value("f.png")),
        "panels": (v.url_value("https://example.org/a.png"),),
    }))
    v.check_incoming({"figure": Plain(v.file_value("f.png"))})
    # A non-image mapping attr whose views disagree is not this check's
    # concern: no view carries the tag.
    v.check_incoming({"meta": _HidesPngFromIteration({"a": 1, "png": 2})})


def test_classify_reads_a_disagreeing_mapping_as_invalid():
    state = v.classify(_HidesPngFromIteration(_forged_bare(png=_png())))
    assert state.state == v.STATE_INVALID
    assert "different entries" in state.problem


# ── agent record writes refuse file references (decision b) ───────────

def test_check_record_write_refuses_a_file_reference_and_accepts_a_url():
    for attrs, where in (
        ({"figure": v.file_value("figs/f1.png")}, "attr 'figure'"),
        ({"panels": [v.url_value("https://example.org/a.png"),
                     v.file_value("figs/f1.png")]}, "attr 'panels'[1]"),
    ):
        with pytest.raises(v.ImageRefInvalid) as raised:
            v.check_record_write(attrs)
        assert raised.value.code == "image_ref_invalid"
        assert raised.value.data["problem"] == "file_ref_outside_ingest"
        assert raised.value.data["kind"] == "file"
        assert where in raised.value.message
        assert "come only from a connector's ingest run" in raised.value.message
        # The general check accepts the same bare file reference.
        v.check_incoming(attrs)
    v.check_record_write({"figure": v.url_value("https://example.org/a.png"), "n": 1})
    v.check_record_write(None)
    # Forged and invalid values keep their own codes.
    with pytest.raises(v.ImageValueForged):
        v.check_record_write({"figure": _forged_bare(png=_png())})
    with pytest.raises(v.ImageRefInvalid) as raised:
        v.check_record_write({"figure": {"okg_image": "url", "url": "http://x.org/a"}})
    assert raised.value.data["field"] == "url"


# ── reducing values to references (decision c) ─────────────────────────

def test_to_reference_and_strip_ingest_fields_drop_png_and_error():
    completed = v.with_png(v.file_value("a.png"), id=SHA_A, sha256=SHA_B,
                           mime_type="image/png", width=1, height=1, byte_count=1)
    failed = v.with_error(v.file_value("b.png"), "image_file_missing")
    url = v.url_value("https://example.org/c.png")
    assert v.to_reference(completed) == v.file_value("a.png")
    assert v.to_reference(failed) == v.file_value("b.png")
    assert v.to_reference(url) == url
    assert v.to_reference("text") == "text"
    attrs = {
        "figure": completed,
        "panels": ["text", failed, url],
        "nested": {"inner": completed},
        "n": 3,
    }
    reduced = v.strip_ingest_fields(attrs)
    assert reduced == {
        "figure": v.file_value("a.png"),
        "panels": ["text", v.file_value("b.png"), url],
        # Deeper than an image position: ordinary data, left alone.
        "nested": {"inner": completed},
        "n": 3,
    }
    v.check_incoming(reduced)
    # The input is not mutated, and attrs with nothing to reduce come back as is.
    assert attrs["figure"]["png"]["id"] == SHA_A
    untouched = {"figure": url, "n": 1}
    assert v.strip_ingest_fields(untouched) is untouched
    assert v.strip_ingest_fields(None) is None


# ── okg snapshot load reduces image values (finding 6) ─────────────────

def test_snapshot_load_reduces_node_and_edge_image_values(tmp_path):
    """A snapshot taken from another okg names that okg's blob rows; the
    loader turns each node and edge into a fact carrying the bare
    references. (The loader's publish step is quarantined on dev by #2628,
    so this reads the facts it streams rather than a published graph.)"""
    import json

    from okg.substrate.db.bootstrap import _edge_facts, _node_facts

    completed = {"okg_image": "file", "path": "figs/a.pdf", "png": _png()}
    failed = {"okg_image": "file", "path": "figs/b.pdf", "error": "image_file_missing"}
    url = v.url_value("https://example.org/c.png")
    snapshot = tmp_path / "snapshot.json"
    snapshot.write_text(json.dumps({
        "nodes": [
            {"node_id": "snap:a", "subtype": "document",
             "attrs": {"figure": completed, "panels": [failed, url]}},
            {"node_id": "snap:b", "subtype": "document", "attrs": {"text": "b"}},
        ],
        "edges": [
            {"src": "snap:a", "dst": "snap:b", "edge_type": "links_to",
             "attrs": {"figure": completed}},
        ],
    }), encoding="utf-8")
    nodes = {fact.node_id: _thaw_json(fact.attrs) for fact in _node_facts(snapshot)}
    edges = [_thaw_json(fact.attrs) for fact in _edge_facts(snapshot)]
    assert nodes["snap:a"] == {
        "figure": v.file_value("figs/a.pdf"),
        "panels": [v.file_value("figs/b.pdf"), url],
    }
    assert nodes["snap:b"] == {"text": "b"}
    assert edges == [{"figure": v.file_value("figs/a.pdf")}]
    assert SHA_A not in json.dumps([nodes, edges])
