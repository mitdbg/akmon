"""task.synth.gold witnesses: sealed manifest, high-entropy leakage keys,
comparability refusal."""

from __future__ import annotations

import json

import pytest

from okg.bench.synth.config import load_config, load_preset
from okg.bench.synth.corruption import Planner
from okg.bench.synth.goldseal import (
    SealError,
    assert_comparable,
    build_manifest,
    leakage_tokens,
    load_manifest,
    write_manifest,
)
from okg.bench.synth.prf import SynthPrf
from okg.bench.synth.render import render_corpus


@pytest.fixture(scope="module")
def sealed(world, smoke_config):
    prf = SynthPrf(smoke_config.seed)
    planner = Planner(world, smoke_config, prf)
    plan = planner.plan()
    rendered = render_corpus(world, smoke_config, planner, plan, prf)
    manifest = build_manifest(
        config=smoke_config, world=world, plan=plan, rendered=rendered
    )
    return manifest, rendered


def test_manifest_embeds_all_digests_and_trace(sealed, smoke_config):
    manifest, rendered = sealed
    assert manifest["schema"] == "okg.synth.manifest.v1"
    assert manifest["config_digest"] == smoke_config.config_digest()
    for key in ("world_digest", "pack_digest", "manifest_digest"):
        # shape only — the digests are proven CONTENT-ADDRESSED (they move when
        # their input moves) in test_digests_are_content_addressed below, which
        # a `_sha256` stubbed to a constant cannot survive.
        assert len(manifest[key]) == 64
        assert set(manifest[key]) <= set("0123456789abcdef")
    assert set(manifest["clean_corpus"]) == set(rendered.clean_docs)
    assert set(manifest["corrupted_corpus"]) == set(rendered.corrupted_docs)
    assert len(manifest["trace"]) == len(rendered.trace)
    assert manifest["gold"]["planned"] == len(manifest["gold"]["corruptions"])
    for row in manifest["gold"]["corruptions"]:
        assert row["partition"] in {
            "cross_doc",
            "internal_only",
            "world_knowledge",
            "undetectable_by_construction",
        }
    assert "clone_rate" in manifest and "rate" in manifest["clone_rate"]


def test_digests_are_content_addressed(sealed, world, smoke_config):
    """The sealed digests are FUNCTIONS OF THEIR INPUT, not decoration.

    ``len(digest) == 64`` passes with ``_sha256`` stubbed to a constant, and
    checking ``config_digest`` by calling ``config_digest()`` again verifies a
    value with the function that produced it. The falsifiable statement is
    differential: three different inputs give three different digests, and a
    smaller world gives a different world digest and a different manifest
    digest.
    """
    from okg.bench.synth.runner import _subset_world

    manifest, _ = sealed
    assert len({
        manifest["world_digest"], manifest["pack_digest"], manifest["manifest_digest"]
    }) == 3, "distinct inputs must not share a digest"

    smaller = _subset_world(world, max(1, len(world.patients) - 1))
    assert len(smaller.patients) < len(world.patients)
    prf = SynthPrf(smoke_config.seed)
    planner = Planner(smaller, smoke_config, prf)
    plan = planner.plan()
    rendered = render_corpus(smaller, smoke_config, planner, plan, prf)
    other = build_manifest(
        config=smoke_config, world=smaller, plan=plan, rendered=rendered
    )

    assert other["world_digest"] != manifest["world_digest"], (
        "a different world sealed to the same world_digest"
    )
    assert other["manifest_digest"] != manifest["manifest_digest"]
    # the pack did not change, so its digest must NOT move — a digest that
    # changes with unrelated input is as broken as one that never changes
    assert other["pack_digest"] == manifest["pack_digest"]
    assert other["config_digest"] == manifest["config_digest"]


def test_canaries_never_appear_in_corpus(sealed):
    manifest, rendered = sealed
    all_text = "".join(rendered.clean_docs.values()) + "".join(
        rendered.corrupted_docs.values()
    )
    for canary in manifest["canary_tokens"]:
        assert canary.startswith("okgsynthcanary_")
        assert canary not in all_text, "canaries live in the manifest ONLY"


def test_leakage_tokens_are_high_entropy_only(world):
    tokens = leakage_tokens(world)
    assert tokens
    for token in tokens:
        assert any(c.isdigit() for c in token), (
            f"low-entropy token {token!r} must not be a leakage key"
        )


def test_leakage_scan_fails_closed(sealed):
    """A leakage token appearing outside the sanctioned surface fails."""
    manifest, _ = sealed
    token = manifest["leakage_tokens"][0]
    unsanctioned = f"debug log line mentioning {token} outside corpus"
    from okg.bench.synth.goldseal import scan_unsanctioned_surface

    violations = scan_unsanctioned_surface(manifest, {"agent.log": unsanctioned})
    assert violations, "scanner must flag a token on an unsanctioned surface"
    clean = scan_unsanctioned_surface(manifest, {"agent.log": "nothing to see"})
    assert clean == []


def test_manifest_roundtrip(sealed, tmp_path):
    manifest, _ = sealed
    path = tmp_path / "benchmark-manifest.json"
    write_manifest(manifest, path)
    again = load_manifest(path)
    assert again["manifest_digest"] == manifest["manifest_digest"]


def test_comparability_refusal(sealed):
    manifest, _ = sealed
    other = dict(manifest)
    other["config_digest"] = "0" * 64
    with pytest.raises(SealError, match="refusing to compare"):
        assert_comparable(manifest, other)
    # explicit recorded reason overrides
    assert_comparable(manifest, other, override_reason="sweep axis comparison")
    # same digest never refuses
    assert_comparable(manifest, manifest)


def test_manifest_deterministic_across_builds(world, smoke_config):
    def _build():
        prf = SynthPrf(smoke_config.seed)
        planner = Planner(world, smoke_config, prf)
        plan = planner.plan()
        rendered = render_corpus(world, smoke_config, planner, plan, prf)
        return build_manifest(
            config=smoke_config, world=world, plan=plan, rendered=rendered
        )

    a, b = _build(), _build()
    assert a["manifest_digest"] == b["manifest_digest"]


def test_config_digest_differs_across_presets(world):
    digests = {load_preset(n).config_digest() for n in ("smoke", "default", "easy", "hard")}
    assert len(digests) == 4


def test_manifest_json_is_canonicalizable(sealed):
    """The manifest survives a JSON round-trip UNCHANGED, and its canonical dump
    is independent of dict insertion order.

    Both halves matter because ``manifest_digest`` is taken over exactly this
    dump. A value that does not survive a reader's re-serialization (a NaN, a
    non-string mapping key whose sort order changes once it becomes a string)
    would make the digest unverifiable by any other JSON reader, and an
    order-dependent dump would make it unreproducible.

    Note the asserted property is FIXPOINT under reload, not identity with the
    in-memory manifest: spans are sealed as tuples and come back as lists, so
    ``json.loads(payload) == manifest`` is false. That asymmetry is harmless for
    the digest (both dump identically) but real, and asserting it would fail.

    The previous form — ``json.loads(payload) == json.loads(json.dumps(manifest,
    sort_keys=True))`` where ``payload`` IS that expression — compared a value
    with itself and held for any manifest whatsoever.
    """
    manifest, _ = sealed
    payload = json.dumps(manifest, sort_keys=True, allow_nan=False)
    reloaded = json.loads(payload)
    assert json.dumps(reloaded, sort_keys=True, allow_nan=False) == payload, (
        "the canonical dump is not a fixpoint under reload, so manifest_digest "
        "is not verifiable outside this process"
    )
    permuted = dict(reversed(list(manifest.items())))
    assert json.dumps(permuted, sort_keys=True, allow_nan=False) == payload


def test_load_config_rejects_non_mapping(tmp_path):
    from okg.bench.synth.config import ConfigError

    path = tmp_path / "bad.yaml"
    path.write_text("- just\n- a\n- list\n")
    with pytest.raises(ConfigError, match="must be a mapping"):
        load_config(path)
