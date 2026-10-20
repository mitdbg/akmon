"""SDK version. The single source of truth that ``__init__`` re-exports.

Per `sdk-design.md` §6 the SDK ships semver. Deployment manifests
declare ``runtime.sdk_version`` and the substrate refuses to load a
deployment whose declared major version is incompatible with the
installed SDK.
"""

__version__ = "1.0.0"
