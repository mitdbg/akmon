"""Per-machine resolution of a deployment's declared ``sync_root``.

WHY THIS EXISTS (2026-08-03, issue #1014). ``sync_root`` names the checkout a
deployment is allowed to project file-derived truth from, so its value is an
absolute path that is correct on exactly one machine. The only place to declare
it is ``deployment.yaml``, which is tracked in git and, for downstream
consumers, mirrored into a shipped package. A deployment shared by two
developers therefore had no way to declare it at all: a literal path is
wrong-but-plausible for everyone but its author, and omitting it leaves a
permanent ``deployment.code_drift`` blocker.

The capability already existed one layer up. An environment descriptor
(``ops/environments/*/environment.yaml``) declares ``sync_root: ${VAR}`` and
``environment._expand_template`` resolves it, refusing secret-like variable
names and refusing any fallback when a variable is unset. This module gives a
plain deployment the same resolution, and gives every consumer ONE
implementation of it -- previously the typed loader, the Tier W publish guard,
the code-drift doctor probe, and the operator-UI readiness script each read the
raw string independently, so they could disagree about what the deployment
declared.

THREE STATES, NOT TWO. ``undeclared`` and ``unresolved`` have opposite safety
meanings and must never collapse into each other:

* ``undeclared`` -- the manifest names no sync root. An operator choice; the
  Tier W guard tolerates it with a warning.
* ``declared`` -- resolved to an absolute path.
* ``unresolved`` -- the manifest DOES name one but this machine cannot resolve
  it (unset variable, or a secret-like variable name). Nobody knows which
  checkout is authoritative, so every consumer must fail closed. Reporting this
  as ``undeclared`` would turn a missing signal into a satisfied one.

SOURCE IS REPORTED, NOT JUST THE VALUE. A value resolved from an untracked
``.okg/local.env`` does not move ``manifest_fingerprint`` and does not show in
``git status``, unlike editing the tracked manifest. ``source`` therefore
travels with the result so the publish-provenance payload, the drift verdict,
and the worker log can say where a Tier W authority value came from.
"""
from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Any, Mapping

#: ``${VAR}`` reference. Same shape as the manifest loader's ``_ENV_VAR_RE``
#: and the environment descriptor's ``_TEMPLATE_RE`` so all three agree on
#: exactly what counts as a reference.
_REF_RE = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")

#: Substring/suffix markers for variables that carry credentials. Mirrors
#: ``deployments/environment.py:_expand_template`` -- a path field must never
#: interpolate a secret, because status reports echo resolved paths.
_SECRET_MARKERS = ("_CREDENTIAL", "_DSN", "_PASSWORD", "_SECRET", "_TOKEN")

STATE_UNDECLARED = "undeclared"
STATE_DECLARED = "declared"
STATE_UNRESOLVED = "unresolved"

SOURCE_MANIFEST_LITERAL = "manifest_literal"
SOURCE_PROCESS_ENV = "process_env"
SOURCE_LOCAL_PROFILE = "local_profile"


@dataclass(frozen=True)
class SyncRootResolution:
    """What one manifest declares as its sync root, on THIS machine."""

    state: str
    path: Path | None = None
    declared_value: str | None = None
    source: str | None = None
    missing: tuple[str, ...] = ()
    detail: str | None = None

    @property
    def is_undeclared(self) -> bool:
        return self.state == STATE_UNDECLARED

    @property
    def is_unresolved(self) -> bool:
        return self.state == STATE_UNRESOLVED

    @property
    def path_str(self) -> str | None:
        return str(self.path) if self.path is not None else None


class SyncRootResolverError(RuntimeError):
    """The resolver was asked to resolve without the context it needs.

    Distinct from an unresolvable declaration: this is a CALLER defect, not a
    deployment misconfiguration, and it must not be answered with a guessed
    directory.
    """


def manifest_path_anchors(deployment_dir: Path) -> dict[str, str]:
    """The stable path anchors every manifest string may contain.

    Lives here rather than in the loader so that consumers which read the
    manifest WITHOUT the loader (``deployment_authority._read_manifest`` does a
    bare ``safe_load``) expand anchors the same way. Without this, a today-legal
    ``sync_root: ${repo_root}/x`` would reach the env step and be reported as a
    missing environment variable named ``repo_root``.
    """
    manifest_dir_str = str(deployment_dir.resolve())
    if deployment_dir.parent.name == "deployments":
        repo_root = deployment_dir.parent.parent.resolve()
    else:
        repo_root = deployment_dir.parent.resolve()
    return {
        "${manifest_dir}": manifest_dir_str,
        "${deployment_dir}": manifest_dir_str,
        "${repo_root}": str(repo_root),
    }


def _substitute_anchors(value: str, deployment_dir: Path) -> str:
    out = value
    for needle, replacement in manifest_path_anchors(deployment_dir).items():
        out = out.replace(needle, replacement)
    return out


# --------------------------------------------------------------------------
# Local profile (`.okg/local.env`) as a value source
# --------------------------------------------------------------------------

#: (path, mtime_ns) -> (filtered values, parse error or None).
#: ``_env_signature_matches`` runs on every manifest-cache hit on the
#: publisher's per-cycle path, so the profile must not be re-parsed per resolve.
#: Keyed by mtime so an edit is picked up; a missing file is cached as an empty
#: mapping under a sentinel mtime so repeated misses do not stat-thrash while
#: still noticing the file being created. Bounded so a profile rewritten in a
#: loop cannot grow it without limit.
_PROFILE_CACHE: dict[
    tuple[str, int, int], tuple[Mapping[str, str], str | None]
] = {}

#: Max distinct (path, mtime) entries retained. Every real process reads one or
#: two profiles; the bound exists so a rewrite loop evicts instead of leaking.
_PROFILE_CACHE_MAX = 32

_EMPTY_PROFILE: Mapping[str, str] = MappingProxyType({})

#: mtime sentinel recorded for "file does not exist".
_NO_FILE = -1


def _profile_enabled() -> bool:
    """Whether the local profile may contribute values.

    Mirrors ``local_env.load_local_env_for_cli``: a developer machine's
    ``.okg/local.env`` must not change unit-test outcomes unless the test opts
    in by naming the file explicitly.
    """
    if not os.environ.get("PYTEST_CURRENT_TEST"):
        return True
    if os.environ.get("OKG_LOCAL_ENV"):
        return True
    return os.environ.get("OKG_ENABLE_LOCAL_ENV_FOR_TESTS") == "1"


def _profile_admits(key: str, value: str) -> bool:
    """``load_local_env``'s ingestion rules, so a local profile contributes
    the same key set to sync-root resolution as it does to everything else.
    One rule, not two."""
    from okg.substrate.local_env import (
        _ALLOWED_NON_OKG_KEYS,
        _DISALLOWED_KEYS,
    )

    if key in _DISALLOWED_KEYS:
        return False
    if not key.startswith("OKG_") and key not in _ALLOWED_NON_OKG_KEYS:
        return False
    return bool(value)


#: Resolved profile PATH per start directory. `configured_local_env_path`
#: walks parent directories looking for a repo sentinel; that walk runs on every
#: manifest-cache hit on the publisher's per-cycle path, so the path lookup is
#: cached separately from the file contents.
_PATH_CACHE: dict[tuple[str | None, str | None], Path] = {}


def _profile_path(start: Path | None):
    from okg.substrate.local_env import configured_local_env_path

    if start is None:
        # `configured_local_env_path(None)` walks up from `Path.cwd()`, and a
        # relative OKG_LOCAL_ENV also resolves against it. Caching a
        # cwd-derived answer would freeze it for the process, so this case is
        # simply not cached.
        return configured_local_env_path(None)
    key = (os.environ.get("OKG_LOCAL_ENV"), str(start))
    cached = _PATH_CACHE.get(key)
    if cached is not None:
        return cached
    path = configured_local_env_path(start)
    if len(_PATH_CACHE) >= _PROFILE_CACHE_MAX:
        _PATH_CACHE.clear()
    _PATH_CACHE[key] = path
    return path


def local_profile_values(start: Path | None = None) -> Mapping[str, str]:
    """Values the local ``.okg/local.env`` may contribute.

    ``start`` is ALWAYS the deployment's own directory (see
    :func:`resolve_declared_sync_root`). Anchoring on the deployment rather
    than on the process's working directory is what makes every consumer read
    the same file: a worktree running a command about a deployment whose
    manifest lives in another checkout must not pick up its own profile.

    For an in-repo deployment (``<checkout>/deployments/<name>``) that walk
    lands on ``<checkout>/.okg/local.env`` -- the file that already carries
    ``OKG_DSN``, and the one a supervised worker's checkout has. Never mutates
    ``os.environ``.
    """
    return _profile_load(start)[0]


def local_profile_error(start: Path | None = None) -> str | None:
    """Why the local profile contributed nothing, if it failed to parse.

    A malformed profile must not be reported to the operator as "the variable
    is unset" -- that sends them to add a line that is already there, and
    destroys the file-and-line the parser knew. The resolver still refuses; it
    just refuses with the real reason.
    """
    return _profile_load(start)[1]


def _profile_load(
    start: Path | None,
) -> tuple[Mapping[str, str], str | None]:
    if not _profile_enabled():
        return _EMPTY_PROFILE, None
    from okg.substrate.local_env import parse_local_env

    try:
        path = _profile_path(start)
    except Exception:  # noqa: BLE001 - an undiscoverable profile is no profile.
        return _EMPTY_PROFILE, None
    try:
        stat = path.stat()
        # Size joins mtime in the key: a rewrite that lands on the same
        # timestamp (coarse filesystem granularity, or a tool that preserves
        # mtime) would otherwise keep serving the previous parse -- including a
        # previous PARSE ERROR, which would name a line the file no longer has.
        mtime, size = stat.st_mtime_ns, stat.st_size
    except OSError:
        mtime, size = _NO_FILE, 0
    key = (str(path), mtime, size)
    cached = _PROFILE_CACHE.get(key)
    if cached is not None:
        return cached
    values: dict[str, str] = {}
    error: str | None = None
    if mtime != _NO_FILE:
        try:
            for name, value in parse_local_env(path).items():
                if _profile_admits(name, value):
                    values[name] = value
        except (OSError, ValueError) as exc:
            # A malformed profile contributes nothing, but the reason is
            # carried so the refusal can name the file and line instead of
            # claiming the variable was never set.
            values = {}
            error = f"{type(exc).__name__}: {exc}"
    # A read-only view: the cached mapping is shared by every later resolve in
    # this process, so a caller must not be able to mutate it.
    entry = (MappingProxyType(values), error)
    if len(_PROFILE_CACHE) >= _PROFILE_CACHE_MAX:
        _PROFILE_CACHE.clear()
    _PROFILE_CACHE[key] = entry
    return entry


def env_and_profile_values(
    name: str, *, start: Path | None = None,
) -> tuple[str | None, str | None]:
    """``(process_env_value, local_profile_value)`` for one variable.

    Both are reported separately rather than merged, because the manifest
    cache's env signature must notice a change in EITHER source. Merging them
    into one value would let a variable move from the profile to the process
    environment (or back) without invalidating a cached manifest whose fields
    read only one of the two.
    """
    process = os.environ.get(name) or None
    profile = local_profile_values(start).get(name) or None
    return process, profile


def _lookup(name: str, start: Path | None) -> tuple[str | None, str | None]:
    """Process environment first, local profile second.

    Process-first is the generic deployment-command precedence
    (``cli/deployment.py`` gives the local profile priority only for
    dogfood-scoped commands) and it is what lets a supervisor deliberately
    inject a value that a stale file at the sync root cannot override.

    ATTRIBUTION is not the same question as precedence. Most CLI entry points
    call ``load_local_env_for_cli`` before dispatch, which copies the profile
    INTO ``os.environ`` -- so by the time this runs, a value that originated in
    an untracked file is indistinguishable from a shell export unless we look.
    When the profile carries the same value, it is reported as the source: the
    point of the field is to name the LEAST-tracked thing that could have
    supplied this Tier W authority, and reporting `process_env` for a value
    sitting in a gitignored file would be the more flattering answer, not the
    truer one.
    """
    process, profile = env_and_profile_values(name, start=start)
    if process:
        return process, (
            SOURCE_LOCAL_PROFILE if profile == process else SOURCE_PROCESS_ENV
        )
    if profile:
        return profile, SOURCE_LOCAL_PROFILE
    return None, None


# --------------------------------------------------------------------------
# Resolution
# --------------------------------------------------------------------------


def require_resolved_sync_root(manifest: Any, *, command: str) -> None:
    """Refuse a write/publish path whose declared sync root did not resolve.

    The typed loader CARRIES an unresolvable declaration rather than raising,
    so read-only verbs keep working on a machine that has never set the
    variable. Every surface that then USES the value as authority -- as a
    source-registry ``repo_root_override``, as a worker working directory, as
    the Tier W comparison -- must call this first. Otherwise ``sync_root``
    silently reads as ``None`` and the surface falls back to a default root:
    the deployment projects file-derived truth from the wrong tree and says
    nothing.

    Reads the attribute DIRECTLY rather than through ``getattr(..., default)``:
    a manifest-like object that does not carry the field cannot be told apart
    from one with nothing unresolved, and a default would silently turn all
    three refusals into no-ops. An `AttributeError` here is the right failure --
    loud, and at the caller that passed the wrong object.
    """
    unresolved = manifest.sync_root_unresolved
    if not unresolved:
        return
    from okg.substrate.errors import SubstrateError

    raise SubstrateError(
        f"refusing {command}: this deployment declares a sync_root that "
        f"cannot be resolved on this machine, so the checkout its "
        f"file-derived sources come from is unknown",
        data={"unresolved_variables": list(unresolved)},
        fix_hint=(
            "set the variable(s) in the environment, or in the deployment "
            "checkout's .okg/local.env (OKG_-prefixed keys only)"
        ),
    )


def require_usable_source_configuration_authority(
    value: Path | str | None,
    *,
    command: str,
) -> Path | None:
    """Return one canonical source root or refuse before source admission.

    ``None`` retains the existing undeclared-root behavior. A present value is
    authority, not a hint: it must resolve strictly to a directory and can
    never fall back to the current checkout. Callers pass the returned path to
    every registry load/reload in their execution segment so a symlink spelling
    cannot become a second cache or source-configuration identity.
    """
    if value is None:
        return None

    from okg.substrate.errors import SourceConfigurationAuthorityInvalid

    configured = Path(value)
    try:
        configured = configured.expanduser()
    except (OSError, RuntimeError, ValueError) as exc:
        raise SourceConfigurationAuthorityInvalid(
            f"refusing {command}: configured source root is not usable on "
            f"this host: {configured}",
            data={
                "configured_path": str(configured),
                "reason": f"{type(exc).__name__}: {exc}",
            },
            fix_hint=_source_configuration_authority_fix_hint(),
        ) from None
    if not configured.is_absolute():
        raise SourceConfigurationAuthorityInvalid(
            f"refusing {command}: configured source root is relative and "
            f"therefore has no host-stable authority: {configured}",
            data={
                "configured_path": str(configured),
                "reason": "relative_path_not_authoritative",
            },
            fix_hint=_source_configuration_authority_fix_hint(),
        )
    try:
        canonical = configured.resolve(strict=True)
    except (OSError, RuntimeError, ValueError) as exc:
        raise SourceConfigurationAuthorityInvalid(
            f"refusing {command}: configured source root is not usable on "
            f"this host: {configured}",
            data={
                "configured_path": str(configured),
                "reason": f"{type(exc).__name__}: {exc}",
            },
            fix_hint=_source_configuration_authority_fix_hint(),
        ) from None
    if not canonical.is_dir():
        raise SourceConfigurationAuthorityInvalid(
            f"refusing {command}: configured source root is not a "
            f"directory: {configured}",
            data={
                "configured_path": str(configured),
                "canonical_path": str(canonical),
                "reason": "not_directory",
            },
            fix_hint=_source_configuration_authority_fix_hint(),
        )
    if not os.access(canonical, os.R_OK | os.X_OK):
        raise SourceConfigurationAuthorityInvalid(
            f"refusing {command}: configured source root cannot be read and "
            f"searched by this process: {configured}",
            data={
                "configured_path": str(configured),
                "canonical_path": str(canonical),
                "reason": "directory_not_readable_or_searchable",
            },
            fix_hint=_source_configuration_authority_fix_hint(),
        )
    try:
        # Exercise directory enumeration before any source adapter does.  The
        # access-mode check above gives a precise refusal for ordinary POSIX
        # permissions; scandir also catches ACL, mount, and filesystem errors.
        with os.scandir(canonical):
            pass
    except OSError as exc:
        raise SourceConfigurationAuthorityInvalid(
            f"refusing {command}: configured source root cannot be "
            f"enumerated by this process: {configured}",
            data={
                "configured_path": str(configured),
                "canonical_path": str(canonical),
                "reason": f"{type(exc).__name__}: {exc}",
            },
            fix_hint=_source_configuration_authority_fix_hint(),
        ) from None
    return canonical


def _source_configuration_authority_fix_hint() -> str:
    if os.environ.get("OKG_RELEASE_ARTIFACT_ID"):
        return (
            "repair the sealed environment descriptor or launcher so its "
            "declared sync_root names an existing authoritative source "
            "directory"
        )
    return (
        "repair the deployment's declared sync_root authority; for a "
        "per-machine checkout, declare a non-secret ${OKG_*_SYNC_ROOT} value "
        "and point it at the existing authoritative source directory; restart "
        "a long-lived source-sync worker after changing .okg/local.env"
    )


def normalize_sync_root(value: str, *, deployment_dir: Path) -> Path:
    """Expand ``~``, anchor a relative value at the deployment directory, and
    resolve without requiring existence -- the normalization the typed loader
    has always applied to this field.

    An exceptional filesystem spelling is preserved for the strict execution
    guard instead of escaping from this read-compatible loader.  Read-only
    manifest consumers must remain able to report a bad declaration, while a
    source execution boundary must classify it as
    ``source_configuration_authority_invalid`` before registry admission.

    Parity across consumers is load-bearing, not cosmetic: ``check_publish_provenance``
    compares the resolved string against the sealed release's recorded sync
    root (which is ``str(manifest.sync_root)``, i.e. this normalization), and
    ``deployment_apply`` folds it into ``desired_state_digest``, which the
    acking worker must reproduce byte for byte.
    """
    path = Path(value)
    try:
        path = path.expanduser()
    except (OSError, RuntimeError, ValueError):
        # ``require_usable_source_configuration_authority`` repeats this
        # operation at the write boundary and owns the typed refusal. Keeping
        # the original spelling here prevents the loader from turning a
        # deployment misconfiguration into an untyped traceback.
        return path
    if not path.is_absolute():
        path = deployment_dir / path
    try:
        return path.resolve(strict=False)
    except (OSError, RuntimeError, ValueError):
        # As above, defer classification to the strict authority guard. The
        # anchored path remains the exact value every consumer observes.
        return path


def resolve_declared_sync_root(
    raw: Mapping[str, Any] | None,
    *,
    deployment_dir: Path | None,
) -> SyncRootResolution:
    """Resolve a manifest's ``sync_root`` for this machine.

    ``raw`` is the parsed manifest mapping (anchor-substituted or not -- this
    function substitutes anchors itself, and doing so twice is a no-op).

    ``deployment_dir`` is the ONLY context this resolver takes, and it is the
    single rule that makes every consumer agree: path anchors, relative values,
    and the ``.okg/local.env`` lookup are all anchored on it, never on the
    process's working directory. A consumer that resolved against its own cwd
    would compute a different sync root than one that resolved against the
    deployment -- two Tier W authority values inside one process, which is the
    exact disagreement this module exists to remove.

    Never raises for an expected condition: an unresolvable declaration is
    returned as :data:`STATE_UNRESOLVED`, never as absence. A missing
    ``deployment_dir`` for a manifest that DOES declare a sync root raises
    :class:`SyncRootResolverError` -- guessing a directory would silently
    redefine ``${repo_root}`` as "wherever this process happens to run".
    """
    candidate = raw.get("sync_root") if isinstance(raw, Mapping) else None
    if not isinstance(candidate, str) or not candidate.strip():
        return SyncRootResolution(state=STATE_UNDECLARED)

    if deployment_dir is None:
        raise SyncRootResolverError(
            "cannot resolve a declared sync_root without the deployment "
            "directory it is declared in"
        )

    declared = candidate.strip()
    profile_start = deployment_dir
    anchored = _substitute_anchors(declared, deployment_dir)

    names = sorted(set(_REF_RE.findall(anchored)))
    if not names:
        return SyncRootResolution(
            state=STATE_DECLARED,
            path=normalize_sync_root(anchored, deployment_dir=deployment_dir),
            declared_value=declared,
            source=SOURCE_MANIFEST_LITERAL,
        )

    secret_like = sorted(
        name for name in names
        if any(marker in name for marker in _SECRET_MARKERS)
        or name == "API_KEY"
        or name.endswith("_API_KEY")
    )
    if secret_like:
        return SyncRootResolution(
            state=STATE_UNRESOLVED,
            declared_value=declared,
            missing=tuple(secret_like),
            detail=(
                f"sync_root references secret-like environment variable(s) "
                f"{secret_like}; a sync root is a path that status reports "
                f"echo, so it must not interpolate a credential"
            ),
        )

    resolved: dict[str, str] = {}
    sources: set[str] = set()
    missing: list[str] = []
    for name in names:
        value, source = _lookup(name, profile_start)
        if value is None or source is None:
            missing.append(name)
            continue
        resolved[name] = value
        sources.add(source)
    if missing:
        # A profile that failed to PARSE contributed nothing, which would
        # otherwise be reported as "the variable is unset" -- sending the
        # operator to add a line that is already there. Name the real cause.
        profile_error = local_profile_error(profile_start)
        if profile_error:
            detail = (
                f"sync_root declares {declared!r} and the local profile could "
                f"not be parsed ({profile_error}), so it contributed no "
                f"values for {list(missing)}. Fix the profile, or set the "
                f"variable(s) in the shell."
            )
        else:
            detail = (
                f"sync_root declares {declared!r} but environment "
                f"variable(s) {list(missing)} are unset. Set them in the "
                f"shell, or add them to the deployment checkout's "
                f".okg/local.env (which admits OKG_-prefixed keys only)."
            )
        return SyncRootResolution(
            state=STATE_UNRESOLVED,
            declared_value=declared,
            missing=tuple(missing),
            detail=detail,
        )

    expanded = _REF_RE.sub(lambda m: resolved[m.group(1)], anchored)
    # Report the LEAST-tracked contributing source: an untracked local profile
    # is the one a reviewer cannot see in the manifest or in git status.
    source = (
        SOURCE_LOCAL_PROFILE
        if SOURCE_LOCAL_PROFILE in sources
        else SOURCE_PROCESS_ENV
    )
    return SyncRootResolution(
        state=STATE_DECLARED,
        path=normalize_sync_root(expanded, deployment_dir=deployment_dir),
        declared_value=declared,
        source=source,
    )
