"""Source-registry loading and profile validation (M3.1)."""
from okg.substrate.sources.profiles import (
    PROFILES,
    PROFILE_NAMES,
    REGISTRY_ONLY,
    ProfileSpec,
    profile_spec,
    validate_profile_tuple,
)
from okg.substrate.sources.registry import (
    SourceAdmissionPolicy,
    SourceEntry,
    load_source_registry,
    validate_source_registry,
)


__all__ = [
    "PROFILES",
    "PROFILE_NAMES",
    "REGISTRY_ONLY",
    "ProfileSpec",
    "profile_spec",
    "validate_profile_tuple",
    "SourceAdmissionPolicy",
    "SourceEntry",
    "load_source_registry",
    "validate_source_registry",
]
