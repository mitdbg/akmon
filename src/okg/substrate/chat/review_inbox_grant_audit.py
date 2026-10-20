"""Audit the review inbox's model-listing grant at Open WebUI v0.11.0.

This is the repo-side half of `task.chat-review-queue-pipe.bounded-exception`.
It is deliberately NOT part of `review_queue_pipe`: the Pipe ships into
the Open WebUI process and must stay self-contained, while this module
runs on the operator's side and audits that instance from outside.

Why a module rather than a `curl | jq` one-liner
------------------------------------------------

The previous form of this check printed nothing and was read as "no
public grant found" -- a pass by absence. Three MEASURED vendor facts at
v0.11.0 make that outcome reachable while everything looks fine, and each
one is encoded here as a refusal rather than left to a reader:

**The route.** ``GET /api/v1/models`` is the OpenAI-compat route and
returns ``{"data": [...]}`` (``main.py:839-840``, ``:887``). The
WORKSPACE listing is ``GET /api/v1/models/list`` and returns
``{"items": [...], "total": N}`` (``routers/models.py:130-135``,
``ModelAccessListResponse`` at ``models/models.py:167-169``). A `jq
'.[]'` or `.data[]` selector against the wrong one of those two matches
nothing and prints nothing. A payload that is not ``{items, total}`` is
refused here, naming the route mistake.

**The field.** ``access_control`` DOES NOT EXIST at v0.11.0; reading it
yields null on every row. Access is ``access_grants``, a list of
``AccessGrantModel`` carrying resource_type / resource_id /
principal_type / principal_id / permission
(``models/access_grants.py:48-57``), surfaced on every model row via
``models/models.py:139``. A row whose ``access_grants`` key is absent
entirely is refused as UNREADABLE rather than counted as ungranted.

**The pagination.** ``/list`` pages at ``PAGE_ITEM_COUNT = 30``
(``routers/models.py:134``), and its ``?query=`` filter matches
``Model.name`` / ``Model.base_model_id`` / the joined user's name, email
and username -- NOT ``Model.id`` (``models/models.py:349-357``). So the
preset cannot be found by filtering on its id, and a naive page-one read
silently omits it once the workspace holds more than 30 models. The
pages are therefore WALKED until the collected count reaches the
reported ``total``, and a walk that cannot reach it refuses.

Anti-vacuity
------------

An empty grants array is only interpretable next to a POSITIVE CONTROL
proving the same query can see a grant at all. `run_grant_check` takes a
control model id, and refuses the whole report -- rather than reporting
"no grant" -- when the control's grants array comes back empty. A zero
selector match is likewise a FAILED verification, never a clean result.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

__all__ = [
    "MANIFOLD_ID_SEPARATOR",
    "NO_GROUP_GRANT_CONDITION",
    "OPENAI_COMPAT_MODELS_PATH",
    "PAGE_ITEM_COUNT",
    "PUBLIC_PRINCIPALS",
    "WORKSPACE_MODELS_LIST_PATH",
    "GrantAudit",
    "GrantCheckReport",
    "ListingRefusal",
    "PageWalk",
    "audit_model_grants",
    "live_manifold_model_id",
    "run_grant_check",
    "walk_model_pages",
]

# routers/models.py:131-135 -- the WORKSPACE listing.
WORKSPACE_MODELS_LIST_PATH = "/api/v1/models/list"
# main.py:839-840 -- the OpenAI-compat route. Named so the wrong-route
# refusal can say which one was probably reached.
OPENAI_COMPAT_MODELS_PATH = "/api/v1/models"
# routers/models.py:134
PAGE_ITEM_COUNT = 30

# models/access_grants.py:14-17. `anyone/*` is the public grant; the
# legacy `user/*` wildcard is treated as public by the vendor's own
# reader (models/access_grants.py:301), so it is public here too.
PUBLIC_PRINCIPALS: frozenset[tuple[str, str]] = frozenset(
    {("anyone", "*"), ("user", "*")}
)

# A walk of a workspace larger than this is refused rather than looped
# forever. 30 items per page, so this is 3000 models.
_MAX_PAGES = 100

# MEASURED, read-only, in the operator's running instance (container
# `open-webui`, image ghcr.io/open-webui/open-webui:v0.11.0, `GET
# /api/version` -> {"version":"0.11.0"}): a manifold Pipe's model id is
# composed at `functions.py:106` as
#
#     sub_pipe_id = f'{pipe.id}.{p["id"]}'
#
# where `pipe.id` is the FUNCTION id the module is installed under and
# `p["id"]` is the entry the module's own `pipes()` returns. The bare pipe
# id therefore NEVER appears as a row id in the workspace listing, so
# auditing it matches ZERO rows -- which reads as "no grant found" instead
# of "you audited an id that cannot exist". That is the pass-by-absence
# this module exists to convert into a named refusal, so the bare id is
# refused rather than walked.
MANIFOLD_ID_SEPARATOR = "."

# Passed as `admin_group_id` when an audit deliberately asserts NO
# group-grant condition -- the positive control, whose only job is to
# prove the query can see a grant at all. It is a NAMED constant rather
# than "" or None precisely because those are the values a FORGOTTEN
# input arrives as: opting out of a check must be a deliberate choice
# that reads as one, never the default a missing Valve selects.
NO_GROUP_GRANT_CONDITION = "\x00no-group-grant-condition-asserted"


def live_manifold_model_id(function_id: str, pipe_id: str | None = None) -> str:
    """The id Open WebUI registers this manifold Pipe under.

    `function_id` is the id the function file is installed as; it is an
    INSTALL-TIME choice, so it is a required argument rather than a
    constant guessed here. `pipe_id` defaults to the Pipe's own
    `pipes()` id, read from the module rather than re-typed.
    """
    if pipe_id is None:
        from okg.substrate.chat.review_queue_pipe import MODEL_ID

        pipe_id = MODEL_ID
    if not function_id.strip():
        raise ValueError(
            "the function id is unset; a manifold model id is "
            f"`{{function_id}}{MANIFOLD_ID_SEPARATOR}{{pipe_id}}` "
            "(functions.py:106 at v0.11.0) and cannot be composed without it"
        )
    return f"{function_id}{MANIFOLD_ID_SEPARATOR}{pipe_id}"


class ListingRefusal(Exception):
    """The listing could not be read in a way that supports any claim."""


@dataclass(frozen=True)
class PageWalk:
    """Every row of the workspace listing, and the proof it is every row."""

    rows: tuple[dict[str, Any], ...]
    total: int
    pages: int

    @property
    def complete(self) -> bool:
        return len(self.rows) == self.total


def _page_payload(payload: Any, page: int) -> tuple[list[Any], int]:
    if not isinstance(payload, Mapping):
        raise ListingRefusal(
            f"page {page} of `{WORKSPACE_MODELS_LIST_PATH}` was not a JSON "
            f"object; `{OPENAI_COMPAT_MODELS_PATH}` (the OpenAI-compat route) "
            "returns a different shape and is the usual cause"
        )
    if "items" not in payload or "total" not in payload:
        keys = ", ".join(sorted(str(key) for key in payload)) or "none"
        hint = (
            f" -- the payload carries `data`, which is the "
            f"`{OPENAI_COMPAT_MODELS_PATH}` OpenAI-compat shape, not the "
            f"`{WORKSPACE_MODELS_LIST_PATH}` workspace listing"
            if "data" in payload
            else ""
        )
        raise ListingRefusal(
            f"page {page} of `{WORKSPACE_MODELS_LIST_PATH}` carried no "
            f"`items`/`total` (keys: {keys}){hint}"
        )
    items = payload["items"]
    total = payload["total"]
    if not isinstance(items, list):
        raise ListingRefusal(f"page {page} `items` was not a list")
    if not isinstance(total, int) or total < 0:
        raise ListingRefusal(f"page {page} `total` was not a count: {total!r}")
    return items, total


def walk_model_pages(
    fetch_page: Callable[[int], Any], *, max_pages: int = _MAX_PAGES
) -> PageWalk:
    """Walk `?page=N` until the collected count reaches the reported total.

    `fetch_page(n)` returns the decoded JSON body of
    ``GET {base}{WORKSPACE_MODELS_LIST_PATH}?page={n}``.

    Refuses rather than returning a short list. The failure this guards
    is the quiet one: page 1 parses, has 30 rows, and the preset is on
    page 2, so a page-one read reports "not found" with no error.
    """
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    total = 0
    page = 0
    while True:
        page += 1
        if page > max_pages:
            raise ListingRefusal(
                f"the listing walk exceeded {max_pages} pages without "
                f"reaching the reported total of {total}"
            )
        items, total = _page_payload(fetch_page(page), page)
        if not items:
            if len(rows) < total:
                raise ListingRefusal(
                    f"page {page} returned no items while only {len(rows)} of "
                    f"{total} rows had been collected; the walk stalled and "
                    "the listing is NOT known to be complete"
                )
            break
        for item in items:
            if not isinstance(item, Mapping):
                raise ListingRefusal(f"page {page} carried a non-object row")
            identifier = item.get("id")
            if not isinstance(identifier, str) or not identifier:
                raise ListingRefusal(f"page {page} carried a row with no `id`")
            if identifier in seen:
                # A repeated id means the walk double-counted, so
                # `len(rows) >= total` could be reached without ever
                # seeing every row. Reaching the total that way is the
                # vacuous pass this refusal exists to exclude.
                raise ListingRefusal(
                    f"row `{identifier}` appeared twice across pages; the "
                    "listing shifted mid-walk and the collected set is not "
                    "a faithful enumeration"
                )
            seen.add(identifier)
            rows.append(dict(item))
        if len(rows) >= total:
            break
    if len(rows) != total:
        raise ListingRefusal(
            f"collected {len(rows)} rows against a reported total of {total}"
        )
    return PageWalk(rows=tuple(rows), total=total, pages=page)


@dataclass(frozen=True)
class GrantAudit:
    """What the listing says about one model id's access grants."""

    model_id: str
    matched: int
    grants: tuple[dict[str, Any], ...] = ()
    group_principals: tuple[str, ...] = ()
    public_grants: tuple[dict[str, Any], ...] = ()
    refusals: tuple[str, ...] = ()

    @property
    def readable(self) -> bool:
        """True when the audit measured something, refusals aside."""
        return self.matched == 1 and not any(
            refusal.startswith("unreadable:") for refusal in self.refusals
        )


def audit_model_grants(
    rows: Sequence[Mapping[str, Any]],
    *,
    model_id: str,
    admin_group_id: str | None = None,
) -> GrantAudit:
    """Audit one model id's `access_grants` in a WALKED listing.

    `rows` must be the complete row set from `walk_model_pages`; auditing
    a partial listing is what makes a zero match uninterpretable.

    `admin_group_id` is the group the inbox must be granted to. An unset
    one -- `None`, `""`, or whitespace -- is UNMET and refuses; it does
    NOT skip the condition. Skipping was the earlier behaviour (`if
    admin_group_id and admin_group_id not in group_principals`), which
    made a forgotten input read as a satisfied check. To assert no
    group-grant condition at all, pass `NO_GROUP_GRANT_CONDITION`.
    """
    matches = [row for row in rows if row.get("id") == model_id]
    refusals: list[str] = []
    if len(matches) != 1:
        refusal = (
            f"selector matched {len(matches)} rows for `{model_id}`, not 1; a "
            "zero match is a FAILED verification, never a clean result"
        )
        if not matches:
            # Name the MEASURED cause rather than leaving an
            # uninterpretable zero. This is the fourth vendor trap on this
            # route and the one a Pipe-backed preset always hits.
            refusal += (
                f". MEASURED at v0.11.0: `{WORKSPACE_MODELS_LIST_PATH}` cannot "
                "return this row at all. `search_models` applies "
                "`stmt.filter(Model.base_model_id != None)` "
                "(models/models.py:346) BEFORE any access-control or "
                "pagination logic, so every row with a NULL `base_model_id` is "
                "excluded unconditionally -- and a Pipe-backed preset's "
                "`base_model_id` IS NULL, because the Pipe itself is the base. "
                "Proven by A/B against the pinned instance: with two rows at "
                "NULL the listing reported total 0; setting `base_model_id` on "
                "ONE of them and changing nothing else made that row appear "
                "with its grants intact while the other stayed absent. So this "
                "is NOT evidence that the preset is ungranted or uninstalled. "
                f"Read the grant from `GET /api/v1/models/model?id={model_id}`, "
                "which does return it, and treat a matched-count assertion on "
                "the workspace listing as unsatisfiable for this surface"
            )
        refusals.append(refusal)
        return GrantAudit(
            model_id=model_id, matched=len(matches), refusals=tuple(refusals)
        )

    row = matches[0]
    if "access_grants" not in row:
        # `access_control` does not exist at v0.11.0 and reads null on
        # every row, so a missing grants key means the row was not read
        # from the workspace listing at all.
        refusals.append(
            f"unreadable: row `{model_id}` carries no `access_grants` key, so "
            "its grants were not measured (v0.11.0 has no `access_control` "
            "field; reading that name yields null on every row)"
        )
        return GrantAudit(model_id=model_id, matched=1, refusals=tuple(refusals))
    raw = row["access_grants"]
    if not isinstance(raw, list):
        refusals.append(
            f"unreadable: row `{model_id}` `access_grants` is not a list"
        )
        return GrantAudit(model_id=model_id, matched=1, refusals=tuple(refusals))

    grants = tuple(dict(grant) for grant in raw if isinstance(grant, Mapping))
    if len(grants) != len(raw):
        refusals.append(
            f"unreadable: row `{model_id}` carried a non-object grant"
        )
    public = tuple(
        grant
        for grant in grants
        if (str(grant.get("principal_type")), str(grant.get("principal_id")))
        in PUBLIC_PRINCIPALS
    )
    group_principals = tuple(
        str(grant.get("principal_id"))
        for grant in grants
        if grant.get("principal_type") == "group"
    )
    if public:
        refusals.append(
            f"`{model_id}` carries a public grant "
            f"({', '.join(sorted({str(g.get('principal_type')) for g in public}))}"
            "/*); the inbox must be granted to the admin group only"
        )
    if admin_group_id == NO_GROUP_GRANT_CONDITION:
        # The positive control: it measures grant VISIBILITY only and
        # deliberately asserts nothing about which principal holds them.
        pass
    elif not (admin_group_id or "").strip():
        refusals.append(
            f"unmet: no admin group id was supplied for `{model_id}`, so the "
            "group-grant condition was never evaluated. An unset input is "
            "UNMET, never assumed satisfied; pass NO_GROUP_GRANT_CONDITION "
            "to assert no group-grant condition on purpose"
        )
    elif admin_group_id not in group_principals:
        refusals.append(
            f"`{model_id}` carries no group grant naming admin group "
            f"`{admin_group_id}` (group principals present: "
            f"{list(group_principals) or 'none'})"
        )
    return GrantAudit(
        model_id=model_id,
        matched=1,
        grants=grants,
        group_principals=group_principals,
        public_grants=public,
        refusals=tuple(refusals),
    )


def _refuse_a_bare_manifold_pipe_id(model_id: str) -> None:
    """Refuse the id that CANNOT match, instead of matching zero rows.

    Auditing `okg-review-inbox` against a live listing returns matched=0,
    which the acceptance clause requires to be 1 -- and a zero match is
    indistinguishable from "the preset was never installed". Naming the
    cause here turns a silent uninterpretable result into an instruction.
    """
    from okg.substrate.chat.review_queue_pipe import MODEL_ID

    if model_id != MODEL_ID:
        return
    raise ValueError(
        f"`{model_id}` is the Pipe's own `pipes()` id, not the id Open WebUI "
        "registers a manifold under: v0.11.0 composes "
        f"`f'{{pipe.id}}{MANIFOLD_ID_SEPARATOR}{{p[\"id\"]}}'` at "
        "functions.py:106, so the bare id matches ZERO rows in the workspace "
        "listing and reads as 'no grant found'. Pass "
        "live_manifold_model_id(<function id the module was installed as>)."
    )


@dataclass(frozen=True)
class GrantCheckReport:
    """The inbox audit, its positive control, and whether it is readable."""

    inbox: GrantAudit
    control: GrantAudit
    walk: PageWalk
    refusals: tuple[str, ...] = field(default=())

    @property
    def ok(self) -> bool:
        return not self.refusals

    def summary(self) -> str:
        state = "PASS" if self.ok else "REFUSED"
        lines = [
            f"{state}: `{self.inbox.model_id}` grant audit over "
            f"{self.walk.total} workspace models in {self.walk.pages} page(s) "
            f"of `{WORKSPACE_MODELS_LIST_PATH}`.",
            f"  matched: {self.inbox.matched}",
            f"  group principals: {list(self.inbox.group_principals) or 'none'}",
            f"  public grants: {len(self.inbox.public_grants)}",
            f"  positive control `{self.control.model_id}`: "
            f"{len(self.control.grants)} grant(s) visible",
        ]
        lines += [f"  refusal: {refusal}" for refusal in self.refusals]
        return "\n".join(lines)


def run_grant_check(
    fetch_page: Callable[[int], Any],
    *,
    model_id: str,
    admin_group_id: str | None,
    control_model_id: str,
) -> GrantCheckReport:
    """Audit the inbox preset's grant WITH a positive control.

    The control is not decoration. Without it an empty `grants` array is
    two different facts wearing one face: "this model has no grant" and
    "this query cannot see grants". The check refuses rather than
    reporting the former unless the control was observed NON-EMPTY.

    `model_id` must be the id the instance actually registers. For this
    manifold Pipe that is `live_manifold_model_id(function_id)`, not the
    bare `pipes()` id -- see MANIFOLD_ID_SEPARATOR above.
    """
    if control_model_id == model_id:
        raise ValueError(
            "the positive control must be a DIFFERENT model row; controlling "
            "a query with its own subject proves nothing"
        )
    _refuse_a_bare_manifold_pipe_id(model_id)
    walk = walk_model_pages(fetch_page)
    inbox = audit_model_grants(
        walk.rows, model_id=model_id, admin_group_id=admin_group_id
    )
    control = audit_model_grants(
        walk.rows,
        model_id=control_model_id,
        admin_group_id=NO_GROUP_GRANT_CONDITION,
    )

    refusals: list[str] = []
    if control.matched != 1:
        refusals.append(
            f"positive control `{control_model_id}` matched "
            f"{control.matched} rows, so the query is not known to work at "
            "all and the inbox result is uninterpretable"
        )
    elif not control.grants:
        refusals.append(
            f"positive control `{control_model_id}` returned an EMPTY grants "
            "array; the query cannot see a grant at all, so an empty result "
            "for the inbox is uninterpretable and MUST NOT be recorded as a "
            "pass"
        )
    refusals += list(inbox.refusals)
    if inbox.matched == 1 and not inbox.grants and not inbox.refusals:
        refusals.append(
            f"`{model_id}` carries no access grants at all, so it is visible "
            "to admins only by default rather than granted to the admin group"
        )
    return GrantCheckReport(
        inbox=inbox, control=control, walk=walk, refusals=tuple(refusals)
    )
