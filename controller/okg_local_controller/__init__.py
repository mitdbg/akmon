"""Stable lifecycle authority that never imports an OKG release."""

from .controller import LocalLifecycleController, TransitionResult
from .ledger import ControllerLedger, LedgerError
from .probe import ProbeError, TrustedProbeLauncher
from .protocol import (
    ActivationReceipt,
    EffectSpec,
    ProbeAttestation,
    ReleaseDescriptor,
    TransitionPlan,
)

__all__ = [
    "ActivationReceipt",
    "ControllerLedger",
    "EffectSpec",
    "LedgerError",
    "LocalLifecycleController",
    "ProbeAttestation",
    "ProbeError",
    "ReleaseDescriptor",
    "TransitionPlan",
    "TransitionResult",
    "TrustedProbeLauncher",
]
