"""Per-source at-rest PII posture resolution.

Resolves a source's ``store_raw`` posture from its ``source_policy`` and
assembles the at-rest transform's collaborators -- the ``pii_class`` classifier,
the pseudonym vault, and the free-text detector -- into an immutable
:class:`PiiPosture` that the :class:`SourceRunner` applies at the admission
chokepoint (and that the enricher emitters may apply too). An explicitly declared
``store_raw`` source resolves to a cheap no-op posture. An implicit raw-storage
default is accepted only during a bounded, canonically valid deferred rollout.

Fail-safe by construction:

* a not-store-raw source with a MISSING or SHORT vault key raises
  :class:`~okg.substrate.alias.pseudonymize.PseudonymKeyUnavailable` -- it never
  silently falls back to storing raw;
* a not-store-raw source's classifier maps an UNDECLARED attribute to ``None``,
  which :func:`resolve_pii_class` resolves to the fail-safe
  ``sensitive_attribute`` (mask) -- never the fail-open ``non_pii``, so an
  attribute the operator forgot to mark is masked, not revealed.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping, Optional

from okg.substrate.errors import SubstrateError
from okg.substrate.nomos.read_masking import AttrClassifier

__all__ = [
    "PiiFactsHeld",
    "PiiPosture",
    "declared_pii_classifier",
    "resolve_deployment_source_pii_posture",
    "resolve_source_pii_posture",
    "store_raw_posture",
]


class PiiFactsHeld(SubstrateError):
    """Raised when a not-store-raw run must HOLD rather than store raw PII.

    Carries the count of held facts. The runner raises it so the run fails and
    the source watermark stays unchanged (held facts are re-processed on a later
    run) -- never a silent skip or a raw store. The remedy is to install the
    free-text PII detector (the ``privacy`` extra) or to correct the source's
    posture.
    """

    code = "pii_facts_held"

    def __init__(self, message: str, *, held: int, reason: str) -> None:
        super().__init__(
            message,
            data={"held": held, "reason": reason},
            fix_hint=(
                "install the 'privacy' extra so free-text PII can be detected "
                "and masked, or correct the source's store_raw posture; the run "
                "held the watermark and stored no raw PII"
            ),
        )
        self.held = held
        self.reason = reason


def _failsafe_classifier(_key: str) -> Optional[str]:
    """Classify every attribute as unknown -> fail-safe mask (default not-store-raw)."""
    return None


@dataclass(frozen=True)
class PiiPosture:
    """Resolved at-rest privacy posture for one source.

    ``store_raw`` True (the default) means the transform is a no-op -- the source
    has authority to store raw identifiers, exactly as before this binding. When
    False the runner applies :func:`sanitize_fact_pii` to each fact using
    ``classifier`` + ``pseudonym_service`` + ``detector``.
    """

    store_raw: bool = True
    # Default to the FAIL-SAFE classifier (mask everything), not the fail-open
    # legacy one: a not-store-raw posture constructed without an explicit
    # classifier must mask undeclared attrs, never reveal them. store_raw
    # postures never consult it, so the default only matters if a caller
    # mis-constructs an active posture -- and then it fails safe.
    classifier: AttrClassifier = field(default=_failsafe_classifier)
    pseudonym_service: Optional[Any] = None
    detector: Optional[Any] = None

    @property
    def active(self) -> bool:
        """True when this posture actually transforms facts (not-store-raw)."""
        return not self.store_raw


def store_raw_posture() -> PiiPosture:
    """The backward-compatible no-op posture (``store_raw=True``)."""
    return PiiPosture(store_raw=True)


def declared_pii_classifier(pii_classes: Mapping[str, Any]) -> AttrClassifier:
    """A fail-safe classifier from a source's declared ``attr -> pii_class`` map.

    A declared key returns its class; an UNDECLARED key returns ``None``, which
    :func:`resolve_pii_class` maps to the fail-safe ``sensitive_attribute`` (mask).
    So a not-store-raw source masks anything it did not explicitly mark (including
    an empty map -> mask everything), never fail-open revealing it. To reveal a
    non-PII attribute the operator declares it ``non_pii``; to keep joins on an
    identifier they declare it ``direct_identifier`` / ``quasi_identifier`` (it is
    pseudonymized via the vault).
    """
    declared = {str(key): str(value) for key, value in pii_classes.items()}

    def classify(key: str) -> Optional[str]:
        return declared.get(key)

    return classify


def _coerce_store_raw(value: Any) -> bool:
    """Coerce a ``store_raw`` value to bool, failing LOUD on a malformed one.

    A privacy posture must never silently pick a direction. Only an actual bool
    is accepted; YAML/JSON strings such as ``"true"`` are malformed and raise,
    matching the Nomos deployment audit exactly.
    """
    if isinstance(value, bool):
        return value
    raise ValueError(
        f"source_policy.store_raw must be a boolean; got {value!r} "
        "(a malformed privacy posture must not silently store raw)"
    )


def _valid_deferred_rollout(
    manifest: Mapping[str, Any],
    *,
    enforce_dsn_keys: Optional[Mapping[str, str]],
) -> tuple[bool, list[dict[str, Any]]]:
    """Return whether ``manifest`` carries a canonically valid raw-compat deferral."""
    nomos = manifest.get("nomos")
    rollout = nomos.get("rollout") if isinstance(nomos, Mapping) else None
    if not isinstance(rollout, Mapping) or rollout.get("status") != "deferred":
        return False, []

    from okg.substrate.nomos.rollout import (
        audit_deferred_rollout,
        deployment_dsn_key,
    )

    dsn_key = deployment_dsn_key(manifest)
    authority_findings: list[dict[str, Any]] = []
    if enforce_dsn_keys is None or dsn_key is None:
        authority_findings.append({
            "code": "nomos.rollout.deferred_dsn_authority_missing",
            "reason_code": "nomos.rollout.deferred_dsn_authority_missing",
            "severity": "error",
            "message": (
                "deferred raw compatibility requires deployment DSN identity "
                "and enforce-mode portfolio authority"
            ),
            "path": "postgres.dsn",
        })

    findings = audit_deferred_rollout(
        rollout,
        deployment_name=str(manifest.get("name") or "unknown"),
        deployment_dsn_key=dsn_key,
        enforce_dsn_keys=enforce_dsn_keys,
    )
    findings.extend(authority_findings)
    return not findings, findings


def resolve_deployment_source_pii_posture(
    *,
    manifest: Mapping[str, Any],
    source_name: str,
    source_entry: Mapping[str, Any],
    env: Optional[Mapping[str, str]] = None,
    detector: Optional[Any] = None,
    build_detector: bool = True,
    enforce_dsn_keys: Optional[Mapping[str, str]] = None,
) -> PiiPosture:
    """Resolve the exact source policy accepted by the Nomos deployment audit.

    Inline policy and deployment defaults by source name or source class share
    one precedence and validation path. Any audit error stops ingestion instead
    of falling back to raw storage.
    """
    from okg.substrate.nomos.deployment_audit import audit_source_privacy_policy

    deferred_rollout, deferred_findings = _valid_deferred_rollout(
        manifest,
        enforce_dsn_keys=enforce_dsn_keys,
    )
    report = audit_source_privacy_policy(
        source_name,
        source_entry,
        manifest=manifest,
    )
    if not report.get("ok"):
        error_codes = {
            str(finding.get("reason_code") or finding.get("code") or "")
            for finding in report.get("findings", [])
            if finding.get("severity") == "error"
        }
        compatibility_errors = {
            "nomos.audit.source_policy_missing",
            "nomos.audit.deployment_posture_missing",
        }
        if (
            deferred_rollout
            and error_codes
            and error_codes.issubset(compatibility_errors)
        ):
            return store_raw_posture()
        raise SubstrateError(
            f"source privacy policy for {source_name!r} failed Nomos audit",
            data={
                "source_name": source_name,
                "findings": report.get("findings", []),
                "deferred_rollout_findings": deferred_findings,
            },
            fix_hint=(
                "declare source_policy.store_raw (inline or in deployment "
                "nomos.source_policy_defaults), or fix the bounded deferred rollout"
            ),
        )
    return resolve_source_pii_posture(
        source_policy=report.get("source_policy"),
        env=env,
        detector=detector,
        build_detector=build_detector,
        allow_implicit_store_raw=deferred_rollout,
    )


def resolve_source_pii_posture(
    *,
    source_policy: Optional[Mapping[str, Any]],
    env: Optional[Mapping[str, str]] = None,
    detector: Optional[Any] = None,
    build_detector: bool = True,
    allow_implicit_store_raw: bool = False,
) -> PiiPosture:
    """Resolve the at-rest posture from a source's ``source_policy``.

    ``store_raw`` must be explicit unless the deployment resolver has already
    proved a bounded deferred rollout and passes ``allow_implicit_store_raw``.
    For a not-store-raw source the vault is loaded LOUD (a missing/short key
    raises, never a silent store-raw fallback), the classifier is built from the
    declared ``pii_classes`` map (fail-safe: undeclared attributes mask), and the
    Presidio detector is built best-effort unless one is injected (absent -> the
    runner HOLDS free-text facts rather than storing them raw).
    """
    policy = source_policy or {}
    if "store_raw" not in policy:
        if allow_implicit_store_raw:
            return store_raw_posture()
        raise SubstrateError(
            "source privacy policy must explicitly declare store_raw",
            data={"source_policy_present": source_policy is not None},
            fix_hint=(
                "set source_policy.store_raw to true or false; implicit raw storage "
                "is allowed only under a valid bounded deferred rollout"
            ),
        )
    store_raw = _coerce_store_raw(policy["store_raw"])
    if store_raw:
        return store_raw_posture()

    # Not-store-raw: load the vault LOUD. A missing key must fail the run, not
    # degrade to storing raw identifiers.
    from okg.substrate.alias.pseudonymize import load_pseudonym_service

    pseudonym_service = load_pseudonym_service(env=env)

    pii_classes = policy.get("pii_classes")
    classifier = (
        declared_pii_classifier(pii_classes)
        if isinstance(pii_classes, Mapping)
        else _failsafe_classifier
    )

    resolved_detector = detector
    if resolved_detector is None and build_detector:
        from okg.substrate.ingest.presidio_detector import (
            maybe_build_presidio_detector,
        )

        resolved_detector = maybe_build_presidio_detector()

    return PiiPosture(
        store_raw=False,
        classifier=classifier,
        pseudonym_service=pseudonym_service,
        detector=resolved_detector,
    )
