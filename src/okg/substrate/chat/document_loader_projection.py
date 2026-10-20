"""Project the external document loader — all THREE gate conditions, by readback.

Per PACT `chat-document-loader`, requirement
``req.chat-document-loader.loader-engaged``. The vendor engages the
external loader only when THREE things hold together
(`retrieval/loaders/main.py:542-544` at v0.11.4; :433-435 at v0.11.0)::

Line numbers below are v0.11.4 (revision ``8bd8b4fa``), re-read
2026-09-29; the behavior each one cites is unchanged from v0.11.0.

    self.engine == 'external'
    and self.kwargs.get('EXTERNAL_DOCUMENT_LOADER_URL')
    and self.kwargs.get('EXTERNAL_DOCUMENT_LOADER_API_KEY')

Driving only the last two is the silent failure this module exists to
prevent: with ``rag.content_extraction_engine`` at its default empty
string (`config.py:868`), the hook NEVER fires, every upload takes the
local extraction path into the vendor's own corpus, and a sync that
wrote and read back the two keys it knew about reports GREEN. A PACT
written to prevent an ungoverned corpus would have created one and
called it success.

So this module drives three keys plus the embedding bypass, and treats
anything short of all four reading back as a REFUSAL:

===============================  ============================  =========
Config key                       Form field on the update      Gate?
                                 route (`retrieval.py:962`)
===============================  ============================  =========
rag.content_extraction_engine    CONTENT_EXTRACTION_ENGINE     yes
rag.external_document_loader_url EXTERNAL_DOCUMENT_LOADER_URL  yes
..._loader_api_key               EXTERNAL_DOCUMENT_LOADER_API  yes
rag.bypass_embedding_and_...     BYPASS_EMBEDDING_AND_RETRI..  no
===============================  ============================  =========

Four properties are load-bearing.

**Readback from the instance, never from the request body.** Every
verified value is read from ``GET /api/v1/retrieval/config``
(`retrieval.py:629`, engine at :647, bypass at :638, loader keys at
:662-663) after the write. A write that the vendor coerced, ignored, or
lower-cased differently is caught here. Note the asymmetry that makes
this necessary: the env read at `config.py:868` applies ``.lower()`` and
the update route at `retrieval.py:1036-1040` does NOT, so an engine
written as ``"External"`` round-trips happily and never matches the
gate's ``== 'external'``.

**Refusal, never degradation.** EVERY key that does not read back as
projected refuses, and the refusal names each failing key with the value
read for it: the engine as anything other than exactly ``external``
(including empty and absent), the URL as another host, the API key as a
different value (named, never printed), the bypass as anything but the
boolean asked for. Naming only the engine was its own defect -- a run
whose engine was fine and whose bypass did not stick refused by quoting
the one condition that WORKED. And because leaving the URL and API
key configured while extraction still runs locally is precisely the
half-configured state the requirement forbids, a failed verification
ROLLS BACK the keys it wrote and says whether the rollback was
confirmed.

**Readback is not proof the hook fired.** It proves the keys were
written. `okg.substrate.chat.document_loader.LoaderRecorder` -- a record
of an actual ``PUT /process`` -- is the only evidence that the vendor
called us, and `loader_projection_report` says so in its own output so
an operator cannot read a green projection as a reached endpoint.

**The loader token is a request BODY field, so an error body can echo
it.** ``EXTERNAL_DOCUMENT_LOADER_API_KEY`` is POSTed in the update body;
a 422 from the instance echoes the submitted value back under
``detail[].input``, and a scrub that knows only the ADMIN token plus runs
of 20+ credential-shaped characters lets a short operator-chosen loader
token through into the printed envelope. So every call this module makes
DECLARES the secrets in play to the transport (``secrets=``), and,
independently, every ``ChatSyncError`` leaving this module is rebuilt
with those secrets removed from the message, the data, and the fix hint,
with the causal chain suppressed so a printed traceback cannot carry what
the envelope no longer does. Two owners on purpose: what the transport
renders is the transport's property, and what this module raises is
this module's. See :func:`_call`.

This module deliberately does not edit ``chat/sync.py``: it exposes the
projection as functions over an :class:`~okg.substrate.chat.sync.InstanceClient`
so the sync verb can adopt it without this PACT restructuring a file
another change owns.
"""

from __future__ import annotations

import hmac
import inspect
import logging
from typing import Any, Mapping, Optional

from okg.substrate.chat.document_loader import (
    MODE_INLINE,
    MODE_POINTER,
    VENDOR_CONTRACT,
    DocumentLoaderPlan,
)
from okg.substrate.chat.sync import ChatSyncError


log = logging.getLogger(__name__)

__all__ = [
    "BYPASS_FORM_KEY",
    "ENGINE_EXTERNAL",
    "ENGINE_FORM_KEY",
    "GATE_FORM_KEYS",
    "LOADER_CONFIG_ROUTE",
    "LOADER_CONFIG_UPDATE_ROUTE",
    "RESIDUAL_VECTOR_WRITE_PATHS",
    "loader_projection_report",
    "loader_update_payload",
    "project_document_loader",
    "verify_loader_readback",
]


#: `main.py:852` mounts the retrieval router at ``/api/v1/retrieval``;
#: `retrieval.py:629` and `:962` are the two routes on it this uses.
LOADER_CONFIG_ROUTE = "/api/v1/retrieval/config"
LOADER_CONFIG_UPDATE_ROUTE = "/api/v1/retrieval/config/update"

#: The exact engine string the gate compares against. Lower case, and
#: compared exactly: the update route does not normalize it.
ENGINE_EXTERNAL = "external"

ENGINE_FORM_KEY = "CONTENT_EXTRACTION_ENGINE"
URL_FORM_KEY = "EXTERNAL_DOCUMENT_LOADER_URL"
API_KEY_FORM_KEY = "EXTERNAL_DOCUMENT_LOADER_API_KEY"
HEADERS_FORM_KEY = "EXTERNAL_DOCUMENT_LOADER_HEADERS"
BYPASS_FORM_KEY = "BYPASS_EMBEDDING_AND_RETRIEVAL"

#: The three the vendor's gate reads. The bypass is projected alongside
#: them but is NOT a gate condition, and the two sets are kept apart so a
#: report cannot imply the bypass had anything to do with the hook firing.
GATE_FORM_KEYS = (ENGINE_FORM_KEY, URL_FORM_KEY, API_KEY_FORM_KEY)

#: Config-table keys, for reports that name what was actually changed.
FORM_KEY_TO_CONFIG_KEY = {
    ENGINE_FORM_KEY: "rag.content_extraction_engine",
    URL_FORM_KEY: "rag.external_document_loader_url",
    API_KEY_FORM_KEY: "rag.external_document_loader_api_key",
    HEADERS_FORM_KEY: "rag.external_document_loader_headers",
    BYPASS_FORM_KEY: "rag.bypass_embedding_and_retrieval",
}

#: Vector-store writes the bypass does NOT cover, measured at v0.11.0 and
#: re-read at v0.11.4.
#: Named in every report: an operator reading "zero vector rows" has to be
#: told which doors that number does not cover, and a number without that
#: list is a claim about the whole instance that was measured on part of
#: it.
RESIDUAL_VECTOR_WRITE_PATHS = (
    {
        "path": "routers/retrieval.py:2223",
        "function": "process_text",
        "route": "POST /api/v1/retrieval/process/text",
        "note": (
            "calls save_docs_to_vector_db UNCONDITIONALLY on raw pasted "
            "content; there is no bypass check anywhere on this path"
        ),
    },
    {
        "path": "routers/retrieval.py:3427",
        "function": "process_files_batch",
        "route": "POST /api/v1/retrieval/process/files/batch",
        "note": (
            "reached by the knowledge-base MULTI-FILE add "
            "(routers/knowledge.py:2117); no bypass check on this path, so a "
            "multi-file admin add WRITES VECTOR ROWS with the bypass on"
        ),
    },
)

#: Knowledge ingest sites that DO inherit the bypass, because they route
#: through process_file (`retrieval.py:2071`). Listed so the coverage
#: claim is a measured partition of the four sites, not three named
#: examples.
BYPASS_COVERED_INGEST_SITES = (
    "routers/knowledge.py:432",
    "routers/knowledge.py:1482",
    "routers/knowledge.py:1576",
)


def loader_update_payload(
    plan: DocumentLoaderPlan,
    token: str,
    *,
    bypass_embedding: bool = True,
) -> dict[str, Any]:
    """The body for ``POST /api/v1/retrieval/config/update``.

    Only the keys this projection owns. The route leaves every field it
    is not sent at its current value (`retrieval.py:962` onwards reads
    ``if form_data.X is not None else config.X``), so a narrow body is
    the correct way to avoid clobbering unrelated retrieval settings.
    """
    payload: dict[str, Any] = {
        ENGINE_FORM_KEY: ENGINE_EXTERNAL,
        URL_FORM_KEY: plan.base_url,
        API_KEY_FORM_KEY: token,
        BYPASS_FORM_KEY: bool(bypass_embedding),
    }
    if plan.filename_headers:
        payload[HEADERS_FORM_KEY] = dict(plan.filename_headers)
    return payload


def verify_loader_readback(
    readback: Mapping[str, Any],
    plan: DocumentLoaderPlan,
    token: str,
    *,
    bypass_embedding: bool = True,
) -> dict[str, Any]:
    """Check an instance's own config against the plan. Never mutates.

    Returns ``{"ok": bool, "verified": [...], "problems": [...]}``. Each
    problem NAMES the value that was read -- except for the API key,
    where naming the read value would print the secret, so the problem
    says only that it does not match.
    """
    verified: list[dict[str, Any]] = []
    problems: list[dict[str, Any]] = []

    def _check(form_key: str, ok: bool, read_display: Any, gate: bool) -> None:
        entry = {
            "form_key": form_key,
            "config_key": FORM_KEY_TO_CONFIG_KEY[form_key],
            "gate_condition": gate,
            "read": read_display,
        }
        (verified if ok else problems).append(entry)

    if ENGINE_FORM_KEY not in readback:
        problems.append({
            "form_key": ENGINE_FORM_KEY,
            "config_key": FORM_KEY_TO_CONFIG_KEY[ENGINE_FORM_KEY],
            "gate_condition": True,
            "read": None,
            "detail": (
                f"the instance's {LOADER_CONFIG_ROUTE} response has no "
                f"{ENGINE_FORM_KEY} field, so the extraction engine cannot be "
                f"confirmed at all"
            ),
        })
    else:
        engine = readback.get(ENGINE_FORM_KEY)
        _check(
            ENGINE_FORM_KEY,
            isinstance(engine, str) and engine == ENGINE_EXTERNAL,
            engine,
            True,
        )

    if URL_FORM_KEY not in readback:
        problems.append({
            "form_key": URL_FORM_KEY,
            "config_key": FORM_KEY_TO_CONFIG_KEY[URL_FORM_KEY],
            "gate_condition": True,
            "read": None,
            "detail": f"no {URL_FORM_KEY} field in the config readback",
        })
    else:
        url = readback.get(URL_FORM_KEY)
        _check(URL_FORM_KEY, url == plan.base_url, url, True)

    if API_KEY_FORM_KEY not in readback:
        problems.append({
            "form_key": API_KEY_FORM_KEY,
            "config_key": FORM_KEY_TO_CONFIG_KEY[API_KEY_FORM_KEY],
            "gate_condition": True,
            # Never the value, present or absent.
            "read": "<absent>",
            "detail": f"no {API_KEY_FORM_KEY} field in the config readback",
        })
    else:
        read_key = readback.get(API_KEY_FORM_KEY)
        matches = isinstance(read_key, str) and hmac.compare_digest(read_key, token)
        _check(
            API_KEY_FORM_KEY,
            matches,
            # The secret is never printed. "set and matching" vs "set and
            # different" is the whole operator-relevant distinction.
            "<matches the configured token>" if matches else "<does not match>",
            True,
        )

    if BYPASS_FORM_KEY not in readback:
        problems.append({
            "form_key": BYPASS_FORM_KEY,
            "config_key": FORM_KEY_TO_CONFIG_KEY[BYPASS_FORM_KEY],
            "gate_condition": False,
            "read": None,
            "detail": f"no {BYPASS_FORM_KEY} field in the config readback",
        })
    else:
        bypass = readback.get(BYPASS_FORM_KEY)
        _check(BYPASS_FORM_KEY, bypass is bool(bypass_embedding), bypass, False)

    if plan.filename_headers:
        read_headers = readback.get(HEADERS_FORM_KEY)
        _check(
            HEADERS_FORM_KEY,
            isinstance(read_headers, Mapping)
            and dict(read_headers) == dict(plan.filename_headers),
            read_headers,
            False,
        )

    return {"ok": not problems, "verified": verified, "problems": problems}


#: What a scrubbed secret is replaced by, matching ``sync.redact``.
REDACTED = "<redacted>"


def _client_accepts_secrets(client: Any) -> bool:
    """Whether ``client.call`` takes the ``secrets`` keyword.

    Only an EXPLICIT parameter counts. A client that swallows ``**kwargs``
    would accept the keyword and drop it, which is the shape of an
    argument silently going nowhere.
    """
    try:
        parameters = inspect.signature(client.call).parameters
    except (TypeError, ValueError):  # pragma: no cover - exotic callables
        return False
    return "secrets" in parameters


def _scrub(value: Any, secrets: tuple[str, ...]) -> Any:
    """``value`` with every non-empty secret replaced, recursively."""
    if isinstance(value, str):
        out = value
        for secret in secrets:
            if secret:
                out = out.replace(secret, REDACTED)
        return out
    if isinstance(value, Mapping):
        return {
            _scrub(key, secrets): _scrub(item, secrets) for key, item in value.items()
        }
    if isinstance(value, (list, tuple)):
        return [_scrub(item, secrets) for item in value]
    return value


def _call(
    client: Any,
    method: str,
    path: str,
    payload: Any = None,
    *,
    secrets: tuple[str, ...],
) -> Any:
    """One instance call whose error envelope cannot carry ``secrets``.

    Two layers, because either alone leaves a hole:

    * The secrets are DECLARED to the transport (``client.call(...,
      secrets=...)``), which is where an error body is read and turned
      into ``data['detail']``. That is the fix at the source.
    * Whatever the transport did, any ``ChatSyncError`` leaving here is
      rebuilt with the secrets removed from its message, its data, and
      its fix hint. The projection owns this property for its own calls
      and does not inherit it.

    ``raise ... from None`` is deliberate: a chained ``__cause__`` or
    ``__context__`` holds the ORIGINAL exception, and a printed traceback
    would render its unscrubbed text under "the above exception". The
    scrubbed envelope keeps the method, path, status, and reason, so
    nothing diagnostic is lost with it.

    A transport without the ``secrets`` parameter is reported at WARNING
    rather than passed over: the envelope is still safe, but the instance
    log on the other side of that call is not this module's to scrub.
    """
    try:
        if _client_accepts_secrets(client):
            return client.call(method, path, payload, secrets=secrets)
        log.warning(
            "%s.call() has no 'secrets' parameter, so the loader token is not "
            "declared to the transport; %s %s error envelopes are scrubbed by "
            "the projection instead",
            type(client).__name__,
            method,
            path,
        )
        return client.call(method, path, payload)
    except ChatSyncError as exc:
        scrubbed = ChatSyncError(
            _scrub(getattr(exc, "message", str(exc)), secrets),
            data=_scrub(getattr(exc, "data", None) or {}, secrets),
            fix_hint=_scrub(getattr(exc, "fix_hint", None) or "", secrets) or None,
        )
        raise scrubbed from None


def _readback(client: Any, *, secrets: tuple[str, ...] = ()) -> Mapping[str, Any]:
    """Read the instance's retrieval config.

    ``secrets`` is not about the request -- a GET carries no body -- but
    about the RESPONSE: this route returns
    ``EXTERNAL_DOCUMENT_LOADER_API_KEY``, so a non-2xx whose body is that
    same document would put the token in the error envelope.
    """
    body = _call(client, "GET", LOADER_CONFIG_ROUTE, secrets=secrets)
    if not isinstance(body, Mapping):
        raise ChatSyncError(
            f"{LOADER_CONFIG_ROUTE} did not return an object, so the loader "
            f"configuration cannot be confirmed",
            data={"reason": "unreadable_retrieval_config", "route": LOADER_CONFIG_ROUTE},
            fix_hint="check the instance version against the pinned 0.11.4",
        )
    return body


def project_document_loader(
    client: Any,
    plan: DocumentLoaderPlan,
    *,
    token: str,
    bypass_embedding: bool = True,
) -> dict[str, Any]:
    """Drive the three gate conditions plus the bypass, and verify by readback.

    Order matters, and each step exists because of a failure it prevents:

    1. **Read first.** The pre-image is what a rollback restores, and an
       instance whose config route does not expose the engine field is
       refused BEFORE anything is written -- writing the loader keys to
       an instance that cannot be switched to external extraction is
       exactly the half-configured state the requirement forbids.
    2. **Write once**, all four keys in one call.
    3. **Read back** from the instance's own route and verify.
    4. **Roll back on failure**, then refuse naming the value read. The
       report says whether the rollback was CONFIRMED by a second read,
       because an unconfirmed rollback is not a restored instance.

    ``token`` is passed in rather than resolved here so the caller owns
    the one read of the environment variable. It is never logged, never
    returned, never placed in the report, and -- because it is a request
    BODY field an error can echo -- never left in an error envelope
    either (:func:`_call`).
    """
    secrets = (token,)
    pre = _readback(client, secrets=secrets)
    if ENGINE_FORM_KEY not in pre:
        raise ChatSyncError(
            f"this instance's {LOADER_CONFIG_ROUTE} exposes no "
            f"{ENGINE_FORM_KEY}, so the external-loader gate "
            f"({VENDOR_CONTRACT.gate_source}) cannot be satisfied; refusing "
            f"before writing the loader URL or API key",
            data={
                "reason": "engine_key_absent",
                "deployment": plan.deployment,
                "route": LOADER_CONFIG_ROUTE,
                "expected_form_key": ENGINE_FORM_KEY,
            },
            fix_hint=(
                f"run against Open WebUI {VENDOR_CONTRACT.openwebui_version} "
                f"({VENDOR_CONTRACT.image_ref})"
            ),
        )

    _call(
        client,
        "POST",
        LOADER_CONFIG_UPDATE_ROUTE,
        loader_update_payload(plan, token, bypass_embedding=bypass_embedding),
        secrets=secrets,
    )
    post = _readback(client, secrets=secrets)
    verdict = verify_loader_readback(
        post, plan, token, bypass_embedding=bypass_embedding,
    )
    if verdict["ok"]:
        return loader_projection_report(plan, verdict, rolled_back=None)

    rollback = _roll_back(client, pre, plan, secrets=secrets)
    engine_read = post.get(ENGINE_FORM_KEY, "<absent>")
    raise ChatSyncError(
        f"the external document loader was NOT engaged for deployment "
        f"{plan.deployment!r}: "
        + "; ".join(_problem_phrase(problem) for problem in verdict["problems"])
        + f". All three gate conditions must hold together "
        f"({VENDOR_CONTRACT.gate_source}); two of three leaves every upload "
        f"on the vendor's local extraction path, and a bypass that did not "
        f"stick leaves the vendor's own vector store collecting rows.",
        data={
            "reason": "loader_gate_not_verified",
            "deployment": plan.deployment,
            "engine_read": engine_read,
            "problems": verdict["problems"],
            "verified": verdict["verified"],
            "rolled_back": rollback,
        },
        fix_hint=(
            f"the loader keys were rolled back to their pre-write values "
            f"({'confirmed' if rollback.get('confirmed') else 'NOT CONFIRMED'}); "
            f"check that the instance accepts "
            + ", ".join(
                str(problem.get("form_key")) for problem in verdict["problems"]
            )
            + f" on {LOADER_CONFIG_UPDATE_ROUTE}"
        ),
    )


def _problem_phrase(problem: Mapping[str, Any]) -> str:
    """One failing key, named, with the value read -- never a secret.

    The refusal used to describe the engine and only the engine, so a run
    whose engine was fine and whose LOADER URL or EMBEDDING BYPASS did not
    stick refused with "the extraction engine read back as 'external',
    not 'external'" -- a message that names the one thing that WORKED.
    """
    form_key = problem.get("form_key")
    if problem.get("detail"):
        return f"{form_key}: {problem['detail']}"
    if form_key == API_KEY_FORM_KEY:
        return (
            f"{form_key} does not match the token this endpoint is served with"
        )
    return f"{form_key} read back as {problem.get('read')!r}"


def _roll_back(
    client: Any,
    pre: Mapping[str, Any],
    plan: DocumentLoaderPlan,
    *,
    secrets: tuple[str, ...] = (),
) -> dict[str, Any]:
    """Restore the pre-write values of the keys this projection writes.

    A failed verification must not leave the loader URL and API key
    configured while extraction still runs locally. Returns what was
    attempted and whether a second read CONFIRMED it -- an unconfirmed
    rollback is reported as unconfirmed, never assumed.

    The restore body carries the instance's PREVIOUS API key, which is a
    second secret this call can echo back out of a 422. It is added to
    ``secrets`` rather than assumed harmless.
    """
    restore = {
        key: pre[key]
        for key in (ENGINE_FORM_KEY, URL_FORM_KEY, API_KEY_FORM_KEY, BYPASS_FORM_KEY)
        if key in pre
    }
    attempted = {
        key: ("<previous value>" if key == API_KEY_FORM_KEY else value)
        for key, value in restore.items()
    }
    previous_key = restore.get(API_KEY_FORM_KEY)
    if isinstance(previous_key, str) and previous_key:
        secrets = (*secrets, previous_key)
    if not restore:
        return {"attempted": attempted, "confirmed": False, "detail": "nothing to restore"}
    try:
        _call(client, "POST", LOADER_CONFIG_UPDATE_ROUTE, restore, secrets=secrets)
        after = _readback(client, secrets=secrets)
    except ChatSyncError as exc:
        log.warning(
            "document loader rollback for deployment %r did not complete: %s",
            plan.deployment,
            exc,
        )
        return {"attempted": attempted, "confirmed": False, "detail": str(exc)}
    confirmed = all(
        after.get(key) == value for key, value in restore.items()
    )
    return {"attempted": attempted, "confirmed": bool(confirmed)}


def loader_projection_report(
    plan: DocumentLoaderPlan,
    verdict: Mapping[str, Any],
    *,
    rolled_back: Optional[Mapping[str, Any]] = None,
    generation: Optional[str] = None,
) -> dict[str, Any]:
    """The operator-facing result. Contains no secret, by construction.

    Three things it always says, because each is a claim an operator
    would otherwise make wrongly from a green run:

    * whether a VENDOR-SIDE COPY of every uploaded document exists (it
      does, in inline mode, and that is stated rather than inferred);
    * that a verified readback is NOT evidence the hook fired;
    * which vector-store write paths the bypass does not cover.
    """
    vendor_copy = plan.mode == MODE_INLINE
    return {
        "deployment": plan.deployment,
        "loader_url": plan.base_url,
        "process_url": plan.process_url,
        # The NAME of the variable, never its value.
        "token_env": plan.token_env,
        "mode": plan.mode,
        "vendor_side_copy": vendor_copy,
        "mode_summary": (
            "inline mode was DECLARED: the full document text is returned to "
            "Open WebUI and stored on its file row, so a vendor-side copy of "
            "every uploaded document exists for this deployment"
            if vendor_copy
            else "pointer mode (the default): Open WebUI keeps only a "
            "breadcrumb naming the graph node; the document text is in the "
            "graph under the upload source"
        ),
        "gate_conditions": list(VENDOR_CONTRACT.gate_conditions),
        "gate_source": VENDOR_CONTRACT.gate_source,
        "verified": list(verdict.get("verified") or []),
        "problems": list(verdict.get("problems") or []),
        "ok": bool(verdict.get("ok")),
        "rolled_back": dict(rolled_back) if rolled_back else None,
        "generation": generation or "",
        "readback_is_not_reach_proof": (
            "these values were read back from the instance's own "
            f"{LOADER_CONFIG_ROUTE}, which proves the keys were WRITTEN. It "
            "does not prove Open WebUI called the endpoint. The only evidence "
            "for that is a recorded PUT /process in okg's own handler log, "
            "correlated by X-Filename."
        ),
        "bypass_covered_ingest_sites": list(BYPASS_COVERED_INGEST_SITES),
        "residual_vector_write_paths": [dict(p) for p in RESIDUAL_VECTOR_WRITE_PATHS],
        "residual_write_path_warning": (
            "rag.bypass_embedding_and_retrieval stops the embedding path only "
            "where the vendor checks it. A measured zero vector-row count "
            "after a chat upload says nothing about "
            + ", ".join(p["path"] for p in RESIDUAL_VECTOR_WRITE_PATHS)
        ),
    }


def unprojected_report(plan_deployment: str) -> dict[str, Any]:
    """What a deployment that declares NO loader gets: nothing touched.

    An explicit report rather than a silent no-op, so "this deployment
    has no loader" and "the projection ran and did nothing" are
    distinguishable in a sync run's output.
    """
    return {
        "deployment": plan_deployment,
        "ok": True,
        "mode": MODE_POINTER,
        "declared": False,
        "summary": (
            "this deployment declares no okg document loader; Open WebUI's "
            "extraction configuration was left exactly as found"
        ),
        "gate_conditions": list(VENDOR_CONTRACT.gate_conditions),
    }
