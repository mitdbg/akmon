"""Connector protocol, context and signals (#1181 slice 3).

Slice 1 gave an external connector the fact *values*. This slice gives it the
shape it runs in, and these tests hold four promises:

* a `Connector` reaches the runner unchanged. The conformance test drives the
  shim through `_adapter_run_kwargs` — the runner's own kwarg selection, at
  `ingest/runner.py` line 3282 — rather than a hand-built call, so a runner
  that stops passing a kwarg fails here instead of in production.
* the five signals are the only sanctioned early exits, and none of them
  advances the checkpoint.
* `ConnectorRun` refuses `partial` health with `completed_scope`, the same
  refusal `SourceRun` makes, at construction.
* the lazy `okg.deployment` still binds every `__all__` name.
"""
from __future__ import annotations

import pytest

from okg.deployment._internal import protocol
from okg.substrate.ingest.runner import _adapter_run_kwargs
from okg.substrate.library.sources import base
from okg.substrate.library.sources.change_probe import ProbeVerdict


# --- helpers -----------------------------------------------------------------


class _FixtureConnector:
    """A minimal `Connector`: a name, a profile, and a `run(ctx)`."""

    profile = "reference_catalog"

    def __init__(self, run, *, name="fixture-connector"):
        self.name = name
        self._run = run

    def run(self, ctx):
        return self._run(ctx)


def _connector(run):
    return _FixtureConnector(run)


def _node_fact(node_id):
    return base.NodeFact(
        node_id=node_id,
        subtype="Doc",
        attrs={},
        source_record_id={"id": node_id},
        source_revision={"rev": "1"},
    )


def _drive(adapter, **overrides):
    """Invoke `adapter` exactly as SourceRunner does.

    `_adapter_run_kwargs` is the runner's signature-inspection selector; using
    it here means the test exercises the real selection rather than asserting
    a signature by hand.
    """
    kwargs = dict(
        mode="cursor",
        cursor=None,
        since_progress=None,
        sync_scope=None,
        change_probe_verdict=None,
        authoritative_replay=False,
    )
    kwargs.update(overrides)
    selected = _adapter_run_kwargs(adapter, **kwargs)
    return selected, adapter.run("run-1", **selected)


# --- (a) protocol conformance through the runner's own call path -------------


def test_the_runner_selects_every_kwarg_the_shim_offers():
    """The shim must expose the full six-kwarg surface, or the runner will
    silently drop context (it passes only what the signature names)."""
    import okg.deployment as sdk

    seen: dict = {}

    def run(ctx):
        seen["ctx"] = ctx
        return sdk.ConnectorRun(facts=[], completed_scope=True)

    adapter = sdk.ConnectorAdapter(_connector(run))
    verdict = ProbeVerdict(changed=True, token={"h": "a" * 32})
    selected, source_run = _drive(
        adapter,
        mode="reconcile",
        cursor={"page": 7},
        since_progress={"record:1": "fp-1"},
        sync_scope={"repo": "okg"},
        change_probe_verdict=verdict,
        authoritative_replay=True,
    )

    assert set(selected) == {
        "mode",
        "cursor",
        "since_progress",
        "sync_scope",
        "change_probe_verdict",
        "authoritative_replay",
    }

    ctx = seen["ctx"]
    assert ctx.run_id == "run-1"
    assert ctx.mode == "reconcile"
    assert ctx.checkpoint.next_cursor == {"page": 7}
    assert ctx.since_progress == {"record:1": "fp-1"}
    assert ctx.sync_scope == {"repo": "okg"}
    assert ctx.change_probe_verdict is verdict
    assert ctx.authoritative_replay is True
    assert isinstance(source_run, base.SourceRun)


def test_the_shim_returns_a_real_source_run_with_record_set_authority():
    """Record-set authority must survive the narrowing; losing a field here
    silently costs the connector its deletion authority."""
    import okg.deployment as sdk

    record_set = base.RecordSet(
        records={
            "r-1": base.RecordEmission(
                primary=base.RecordNode(node_id="n-1", subtype="Doc"),
            ),
        }
    )

    def run(ctx):
        return sdk.ConnectorRun(
            facts=[_node_fact("n-1")],
            completed_scope=True,
            next_checkpoint=sdk.Checkpoint(next_cursor={"page": 8}),
            record_set=record_set,
            run_mode="reconcile",
            record_set_replacement_keys=["r-1"],
        )

    _selected, source_run = _drive(sdk.ConnectorAdapter(_connector(run)))

    assert source_run.completed_scope is True
    assert source_run.next_cursor == {"page": 8}
    assert source_run.record_set is record_set
    assert source_run.run_mode == "reconcile"
    assert list(source_run.record_set_replacement_keys) == ["r-1"]
    assert len(list(source_run.facts)) == 1


# --- (d) signals advance no checkpoint ---------------------------------------


@pytest.mark.parametrize("signal_name", [
    "Cancelled", "Deadline", "RetryAfter", "RateLimited", "ConnectorError",
])
def test_a_signal_advances_no_checkpoint(signal_name):
    """Every sanctioned early exit hands the runner back the cursor it was
    given. The runner snapshots `next_cursor` before it walks the facts, so a
    shim that advanced first could not take it back."""
    import okg.deployment as sdk

    prior = {"page": 3}

    def run(ctx):
        raise getattr(sdk, signal_name)("stop")

    adapter = sdk.ConnectorAdapter(_connector(run))
    _selected, source_run = _drive(adapter, cursor=prior)

    assert source_run.next_cursor == prior, "the checkpoint advanced"
    assert adapter.last_run_metrics.get("connector.signal") == 1.0
    assert source_run.completed_scope is False
    assert source_run.health is not None
    assert source_run.health.status == base.SOURCE_RUN_PARTIAL_STATUS
    assert signal_name in source_run.health.reason
    # A partial walk proves nothing about absence, so it carries no
    # record-set deletion authority.
    assert source_run.record_set is None


def test_a_signal_raised_mid_stream_keeps_the_facts_and_the_checkpoint():
    """Facts emitted before the signal are still admissible; the checkpoint
    stays put so the next run re-reads the same frontier."""
    import okg.deployment as sdk

    def run(ctx):
        def facts():
            yield _node_fact("n-1")
            raise sdk.RateLimited("slow down", seconds=30, scope="api-token")

        return sdk.ConnectorRun(
            facts=facts(),
            next_checkpoint=sdk.Checkpoint(next_cursor={"page": 99}),
        )

    _selected, source_run = _drive(
        sdk.ConnectorAdapter(_connector(run)), cursor={"page": 3},
    )

    assert source_run.next_cursor == {"page": 3}, "advanced past a partial run"
    assert len(list(source_run.facts)) == 1
    assert "30" in source_run.health.reason
    assert "api-token" in source_run.health.reason


def test_an_unsanctioned_exception_is_not_swallowed():
    """Only the sanctioned signals become a partial run. Anything else must
    reach the runner, or a connector bug would look like a clean partial."""
    import okg.deployment as sdk

    def run(ctx):
        raise ValueError("a real bug")

    with pytest.raises(ValueError, match="a real bug"):
        _drive(sdk.ConnectorAdapter(_connector(run)))


def test_secret_ref_yields_a_reference_and_an_undeclared_one_fails_visibly():
    """`secret_ref` hands back the environment-variable *name*, never a value,
    and an undeclared secret raises `KeyError` — a configuration defect, not a
    signal the shim would turn into a partial run."""
    import okg.deployment as sdk

    seen: dict = {}

    def run(ctx):
        seen["ctx"] = ctx
        return sdk.ConnectorRun(facts=[])

    adapter = sdk.ConnectorAdapter(
        _connector(run), secrets={"token": "EXAMPLE_API_TOKEN"},
    )
    _drive(adapter)
    ctx = seen["ctx"]

    assert ctx.secret_ref("token") == "EXAMPLE_API_TOKEN"
    assert not issubclass(KeyError, sdk.ConnectorError)
    with pytest.raises(KeyError, match="not declared"):
        ctx.secret_ref("absent")
    assert adapter.name == "fixture-connector"
    assert adapter.profile == "reference_catalog"


def test_cancelled_reports_true_past_the_deadline():
    import okg.deployment as sdk

    seen: dict = {}

    def run(ctx):
        seen["cancelled"] = ctx.cancelled()
        return sdk.ConnectorRun(facts=[])

    _drive(sdk.ConnectorAdapter(_connector(run), deadline=0.0))
    assert seen["cancelled"] is True


# --- (b) ConnectorRun refuses partial + completed_scope ----------------------


def test_connector_run_refuses_partial_with_completed_scope():
    """The same refusal `SourceRun` makes, at construction, for the same
    reason: absence in a partial run is not evidence of deletion."""
    import okg.deployment as sdk

    partial = base.SourceHealth(status=base.SOURCE_RUN_PARTIAL_STATUS)

    with pytest.raises(ValueError, match="not evidence of deletion"):
        sdk.ConnectorRun(facts=[], completed_scope=True, health=partial)

    with pytest.raises(ValueError, match="not evidence of deletion"):
        base.SourceRun(facts=[], completed_scope=True, health=partial)

    # Either alone is fine.
    assert sdk.ConnectorRun(facts=[], health=partial).completed_scope is False
    assert sdk.ConnectorRun(facts=[], completed_scope=True).health is None


# --- (c) since_progress round-trips a ProgressMarker identity ----------------


def test_since_progress_round_trips_a_progress_marker_identity():
    """`SourceRunner` isinstance-checks `ProgressMarker` and raises on anything
    else, so the marker a connector yields must be the substrate class itself
    — not an SDK wrapper — after the round trip through the shim."""
    import okg.deployment as sdk

    def run(ctx):
        resumed = ctx.since_progress["record:r-1"]
        return sdk.ConnectorRun(
            facts=[sdk.ProgressMarker(record_id="r-1", fingerprint=resumed)],
            next_checkpoint=sdk.Checkpoint(next_cursor={"page": 2}),
        )

    _selected, source_run = _drive(
        sdk.ConnectorAdapter(_connector(run)),
        since_progress={"record:r-1": "fp-abc"},
    )

    (marker,) = list(source_run.facts)
    assert type(marker) is base.ProgressMarker
    assert isinstance(marker, base.ProgressMarker), "runner isinstance would fail"
    assert marker.record_id == "r-1"
    assert marker.fingerprint == "fp-abc"
    assert sdk.ProgressMarker is base.ProgressMarker


# --- (e) the slice-1 export gap ----------------------------------------------


def test_probe_verdict_and_change_probe_error_are_the_substrate_objects():
    """The runner hands a connector a `ProbeVerdict` through
    `change_probe_verdict`; slice 1 exported neither name."""
    import okg.deployment as sdk
    from okg.substrate.library.sources import change_probe

    assert sdk.ProbeVerdict is change_probe.ProbeVerdict
    assert sdk.ChangeProbeError is change_probe.ChangeProbeError


# --- (f) the lazy import -----------------------------------------------------


def test_importing_the_sdk_does_not_load_preflight_or_jsonschema():
    """The eager slice-1 import cost every consumer the
    preflight -> registry -> jsonschema chain. Run in a subprocess so this
    holds for a fresh interpreter, not one warmed by the other tests."""
    import subprocess
    import sys

    probe = (
        "import sys, okg.deployment;"
        "eager=[m for m in "
        "('okg.substrate.sources.preflight','okg.substrate.sources.registry',"
        "'jsonschema') if m in sys.modules];"
        "print(eager)"
    )
    import os

    from tests.pytest.child_pythonpath import child_pythonpath

    env = dict(os.environ, PYTHONPATH=child_pythonpath())
    out = subprocess.run(
        [sys.executable, "-c", probe],
        capture_output=True, text=True, check=True, env=env,
    )
    assert out.stdout.strip() == "[]", f"eagerly imported: {out.stdout.strip()}"


def test_every_public_name_still_resolves_and_star_import_binds_it():
    """Laziness must not cost the stability contract."""
    import okg.deployment as sdk

    missing = [n for n in sdk.__all__ if not hasattr(sdk, n)]
    assert missing == [], f"__all__ names absent from the namespace: {missing}"

    namespace: dict = {}
    exec("from okg.deployment import *", namespace)  # noqa: S102
    unbound = [
        n for n in sdk.__all__ if n != "__version__" and n not in namespace
    ]
    assert unbound == [], f"star-import did not bind: {unbound}"


def test_a_lazy_name_caches_and_keeps_substrate_identity():
    """First access resolves through `__getattr__` and caches; the second is a
    plain globals hit. Import the package fresh so an earlier test's access
    does not pre-warm the cache."""
    import importlib
    import sys

    sys.modules.pop("okg.deployment", None)
    sdk = importlib.import_module("okg.deployment")

    assert "redact" not in vars(sdk), "expected redact to start unresolved"
    first = sdk.redact
    assert "redact" in vars(sdk), "resolved name was not cached in globals"
    assert first is sdk.redact

    from okg.substrate.sources import redaction

    assert first is redaction.redact_text


def test_an_unknown_attribute_still_raises_attribute_error():
    import okg.deployment as sdk

    with pytest.raises(AttributeError, match="no attribute 'not_a_real_name'"):
        sdk.not_a_real_name


# --- (g) health and record authority are constructible from the SDK ----------


def test_health_and_authority_names_are_the_substrate_objects():
    """`ConnectorRun.health` and `.record_authority_stream` are *typed* by
    these, so without them a connector could name the fields but not build a
    value for either — the `okg.substrate` import this surface exists to
    remove."""
    import okg.deployment as sdk

    assert sdk.ConnectorHealth is base.SourceHealth
    assert sdk.RecordAuthorityStream is base.RecordAuthorityStream
    assert sdk.HEALTH_STATUSES is base.SOURCE_HEALTH_STATUSES
    # `partial` is what a signalled run reports, so a connector that wants to
    # report degraded health itself must be able to see the status.
    assert base.SOURCE_RUN_PARTIAL_STATUS in sdk.HEALTH_STATUSES


def test_a_connector_reports_degraded_health_without_importing_substrate():
    """The end the exports exist for: health set by the connector survives
    the shim onto the `SourceRun`."""
    import okg.deployment as sdk

    def run(ctx):
        return sdk.ConnectorRun(
            facts=[_node_fact("n-1")],
            completed_scope=False,
            health=sdk.ConnectorHealth(
                status="acquire_stale", reason="upstream 503",
            ),
        )

    _selected, source_run = _drive(sdk.ConnectorAdapter(_connector(run)))

    assert source_run.health.status == "acquire_stale"
    assert source_run.health.reason == "upstream 503"


# --- (h) RetryAfter.seconds is bounded ---------------------------------------


def _construct(signal_cls, **kwargs):
    """Construct a signal and return it.

    The bound is enforced in `__init__`, so the *construction* is what must
    raise. Written inline, `RetryAfter("later", seconds=-1)` is a bare
    exception-instantiation statement, which CodeQL's
    `py/unused-exception-object` reads as a forgotten `raise`. Going through
    a call says the constructor is the unit under test, and returning the
    value keeps ruff from seeing an unused local.
    """
    return signal_cls("later", **kwargs)


@pytest.mark.parametrize("seconds", [
    -1,
    -0.001,
    float("nan"),
    float("inf"),
    float("-inf"),
    protocol.MAX_RETRY_AFTER_SECONDS + 1,
    1_757_000_000,  # an epoch timestamp copied out of a Retry-After header
    "30",  # the header value, unparsed
    True,  # bool is an int subclass; a flag is not a duration
    object(),
])
def test_retry_after_refuses_an_out_of_range_hint(seconds):
    """A negative or NaN wait is unsatisfiable and an absurd one reads as a
    deliberate day-long backoff, so both are refused at construction rather
    than carried into run health."""
    import okg.deployment as sdk

    with pytest.raises(ValueError, match="RetryAfter"):
        _construct(sdk.RetryAfter, seconds=seconds)


@pytest.mark.parametrize("seconds", [0, 0.5, 30, protocol.MAX_RETRY_AFTER_SECONDS])
def test_retry_after_accepts_a_hint_inside_the_bound(seconds):
    import okg.deployment as sdk

    assert sdk.RetryAfter("later", seconds=seconds).seconds == float(seconds)


def test_retry_after_seconds_stays_optional():
    import okg.deployment as sdk

    assert sdk.RetryAfter("later").seconds is None


def test_rate_limited_inherits_the_bound():
    """`RateLimited` forwards `seconds` to `RetryAfter`, so it must not be a
    way around the check."""
    import okg.deployment as sdk

    with pytest.raises(ValueError, match="RetryAfter"):
        _construct(sdk.RateLimited, seconds=-5, scope="api-token")


# --- (i) a connector that returns None is a defect, not a partial run --------


def test_a_none_outcome_is_refused_rather_than_dressed_up_as_a_partial():
    """A connector that falls off the end of `run` returns None. Reporting
    that as a partial run would make a broken connector indistinguishable
    from a real signal, and the runner would resume it forever."""
    import okg.deployment as sdk

    def run(ctx):
        return None

    adapter = sdk.ConnectorAdapter(_connector(run))
    with pytest.raises(TypeError, match="returned None from run"):
        _drive(adapter)


def test_a_mid_stream_signal_keeps_the_run_mode_the_connector_declared():
    """The other arm of the same branch: `run` returned, so its declared
    `run_mode` is known and must beat the shim's default even though the
    fact stream then signalled."""
    import okg.deployment as sdk

    def run(ctx):
        def facts():
            yield _node_fact("n-1")
            raise sdk.Deadline("out of time")

        return sdk.ConnectorRun(facts=facts(), run_mode="reconcile")

    _selected, source_run = _drive(
        sdk.ConnectorAdapter(_connector(run)), mode="cursor",
    )

    assert source_run.run_mode == "reconcile"
    assert source_run.health.status == base.SOURCE_RUN_PARTIAL_STATUS


# --- (i) image_root reaches the runner (#3046) --------------------------------


def test_image_root_is_carried_from_connector_run_to_source_run(tmp_path):
    """The root file image values resolve against reaches `SourceRun` on a
    completed run, and the fact carries only the relative path."""
    import okg.deployment as sdk

    fact = base.NodeFact(
        node_id="n-1",
        subtype="Doc",
        attrs={"figure": sdk.ImageRef.file("figs/f1.png")},
        source_record_id={"id": "n-1"},
        source_revision={"rev": "1"},
    )

    def run(ctx):
        return sdk.ConnectorRun(
            facts=[fact], completed_scope=True, image_root=str(tmp_path),
        )

    _selected, source_run = _drive(sdk.ConnectorAdapter(_connector(run)))

    assert source_run.image_root == str(tmp_path)
    (emitted,) = source_run.facts
    assert emitted.attrs == {"figure": {"okg_image": "file", "path": "figs/f1.png"}}
    assert str(tmp_path) not in repr(emitted)


def test_image_root_defaults_to_none_on_both_sides():
    import okg.deployment as sdk

    assert sdk.ConnectorRun(facts=[]).image_root is None
    assert base.SourceRun(facts=[]).image_root is None

    def run(ctx):
        return sdk.ConnectorRun(facts=[], completed_scope=True)

    _selected, source_run = _drive(sdk.ConnectorAdapter(_connector(run)))
    assert source_run.image_root is None


def test_image_root_survives_a_signal_raised_mid_stream(tmp_path):
    """A signal raised part-way through a lazy fact stream keeps the facts
    collected so far; their image values still need the declared root."""
    import okg.deployment as sdk

    def run(ctx):
        def facts():
            yield _node_fact("n-1")
            raise sdk.Deadline("out of time")

        return sdk.ConnectorRun(facts=facts(), image_root=str(tmp_path))

    _selected, source_run = _drive(sdk.ConnectorAdapter(_connector(run)))

    assert source_run.health.status == base.SOURCE_RUN_PARTIAL_STATUS
    assert [f.node_id for f in source_run.facts] == ["n-1"]
    assert source_run.image_root == str(tmp_path)


def test_a_signal_before_any_connector_run_carries_no_image_root():
    """No `ConnectorRun` was returned, so no root was declared: the early
    exit says so rather than inventing one."""
    import okg.deployment as sdk

    def run(ctx):
        raise sdk.Cancelled("operator stop")

    _selected, source_run = _drive(sdk.ConnectorAdapter(_connector(run)))

    assert source_run.health.status == base.SOURCE_RUN_PARTIAL_STATUS
    assert source_run.image_root is None
