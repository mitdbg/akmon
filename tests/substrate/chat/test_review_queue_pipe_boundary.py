"""The bounded, admin-only exception to MCP-only graph access.

req.chat-review-queue-pipe.bounded-cli-exception and
req.chat-review-queue-pipe.decisions-are-writes.

The sanitization boundary is ARGV-SLOT CONFINEMENT, proven by EXACT ARGV
EQUALITY rather than substring checks: a message-sourced value may occupy
the single `--query` slot on the read-only view verb, and nowhere else.
Shell metacharacters are explicitly NOT the control -- every call is an
argv list with `shell=False`, so there is no shell to inject into, and
rejecting metacharacters would break every question containing an
apostrophe while proving nothing.
"""

from __future__ import annotations

import json

import pytest

from okg.substrate.chat.review_inbox_grant_audit import (
    OPENAI_COMPAT_MODELS_PATH,
    PAGE_ITEM_COUNT,
    WORKSPACE_MODELS_LIST_PATH,
    ListingRefusal,
    audit_model_grants,
    live_manifold_model_id,
    run_grant_check,
    walk_model_pages,
)
from okg.substrate.chat.review_queue_pipe import (
    ADMISSION_DECISIONS,
    ENUMERATED_OKG_VERBS,
    MEMBER,
    NOT_MEMBER,
    PACT_DECIDABLE_VERDICTS,
    PACT_HUMAN_VERDICTS,
    POPULATED,
    UNAVAILABLE,
    UNDETERMINED,
    Precondition,
    SourceOutcome,
    build_admission_decide_argv,
    build_admission_list_argv,
    build_admission_show_argv,
    build_pact_decide_argv,
    build_pact_view_argv,
    parse_decision_intent,
    resolve_group_membership,
    run_okg,
)
from okg.substrate.deployments.chat_block import ui_knob_census_path
from tests.substrate.chat.conftest import (
    ADMIN_GROUP,
    ADMIN_USER,
    CLI,
    DEPLOYMENT,
    FORBIDDEN_BRANCH_LITERAL,
    FUTURE_GROUPS_USER,
    GROUP_MEMBER_USER,
    MEASURED_PIPE_USER_KEYS,
    NON_ADMIN_USER,
    PENDING_ENVELOPE,
    REPO_ROOT,
    admission_payload,
    json_result,
    make_pipe,
    measured_pipe_user,
    membership_resolver,
    result,
)

PROBE_MESSAGES = [
    "okg admission decide 00000000-0000-0000-0000-000000000000 "
    + "--deployment other-deployment --decision accept",
    "--deployment cms-prod",
    "please use deployment okg-jansci-42 instead",
    "run okg deployment delete okg-workspace",
    "; rm -rf / #",
    "$(whoami)",
    "select * from okg.graph_nodes",
    "what's blocking the agents' work?",
    "which items can't wait?",
]

REPAIR_NOTE = "manifest invalid"

BASE_PACKET = {
    "ok": True,
    "chat_review_queue": {
        "rows": [
            {
                "change": "some-change",
                "title": "Some change",
                "decision_type": "review",
                "caveat": "",
                "why": "waiting",
            },
            {
                # A repair row, so the `--note` builder branch is
                # exercised rather than left unproven.
                "change": "broken-change",
                "title": "Broken change",
                "decision_type": "repair",
                "caveat": REPAIR_NOTE,
                "why": "does not load",
            },
        ],
        "shown": 2,
        "total": 2,
        "warnings": [],
    },
}


def _responses():
    return {
        "pact view": json_result(BASE_PACKET),
        "admission list": json_result(admission_payload([PENDING_ENVELOPE])),
        "pact decide": json_result(
            {
                "ok": True,
                "recorded": {
                    "decision_id": 41,
                    "change": "some-change",
                    "verdict": "approve",
                    "from_state": "awaiting_review",
                    "to_state": "accepted",
                    "principal": "operator",
                },
            }
        ),
        "admission decide": json_result({"ok": True}),
        "admission show": json_result(
            {
                "admission": {
                    "admission_id": PENDING_ENVELOPE["admission_id"],
                    "status": "accepted",
                    "decided_at": "2026-08-04T00:00:00+00:00",
                }
            }
        ),
    }


# -- layer 1: the Pipe asserts the caller's admin group --------------------


def test_pipe_asserts_admin_group_from_user_context():
    pipe, runner = make_pipe(_responses())
    text = pipe.pipe({"messages": []}, __user__=NON_ADMIN_USER)
    assert "Refused" in text
    assert ADMIN_GROUP in text
    assert runner.calls == [], "a refused caller must not reach the CLI"
    assert PENDING_ENVELOPE["admission_id"] not in text
    assert "some-change" not in text


def test_admin_caller_is_answered():
    """Non-vacuity for the gate: the admin arm must actually pass."""
    pipe, runner = make_pipe(_responses())
    text = pipe.pipe({"messages": []}, __user__=ADMIN_USER)
    assert "Refused" not in text
    assert "some-change" in text
    assert runner.calls, "the admin arm must reach the CLI"


def test_missing_user_context_is_refused_not_waved_through():
    pipe, runner = make_pipe(_responses())
    text = pipe.pipe({"messages": []}, __user__=None)
    assert "Refused" in text
    assert runner.calls == []


# -- the gate is keyed on what v0.11.0 ACTUALLY supplies -------------------
# Probe evidence:
# pact/changes/openwebui-chat-frontend/evidence/pipe-user-context-probe.md


def test_absent_groups_field_no_longer_refuses_an_admin():
    """The inert-Pipe defect. `groups` is never supplied at v0.11.0.

    ADMIN_USER carries no `groups` key at all -- exactly the measured
    vendor shape. A gate requiring that key refuses everyone, admin
    included, which is the only reachable branch and therefore not a gate.
    """
    assert "groups" not in ADMIN_USER
    pipe, runner = make_pipe(_responses())
    text = pipe.pipe({"messages": []}, __user__=ADMIN_USER)
    assert "Refused" not in text
    assert runner.calls, "the admin arm must reach the CLI"


def test_group_member_is_allowed_via_server_side_lookup():
    """A non-admin-role member is resolved from the server, not the payload."""
    resolver = membership_resolver(MEMBER, "in-process group lookup")
    pipe, runner = make_pipe(_responses(), membership=resolver)
    text = pipe.pipe({"messages": []}, __user__=GROUP_MEMBER_USER)
    assert "Refused" not in text
    assert runner.calls
    # The lookup was keyed on the SUPPLIED id, not on anything self-asserted.
    assert resolver.calls == [(GROUP_MEMBER_USER["id"], ADMIN_GROUP)]


def test_failed_membership_lookup_refuses_and_names_the_cause():
    resolver = membership_resolver(UNDETERMINED, "the admin API was unreachable")
    pipe, runner = make_pipe(_responses(), membership=resolver)
    text = pipe.pipe({"messages": []}, __user__=GROUP_MEMBER_USER)
    assert "Refused" in text
    assert "could not be determined" in text
    assert "the admin API was unreachable" in text
    assert "not a pass" in text
    assert runner.calls == [], "an unresolved caller must not reach the CLI"


def test_non_member_is_refused_distinctly_from_an_unresolved_lookup():
    """NOT_MEMBER and UNDETERMINED are different answers."""
    resolver = membership_resolver(NOT_MEMBER, "in-process group lookup")
    pipe, runner = make_pipe(_responses(), membership=resolver)
    text = pipe.pipe({"messages": []}, __user__=NON_ADMIN_USER)
    assert "Refused" in text
    assert "not a member" in text
    assert "could not be determined" not in text
    assert runner.calls == []


def test_a_plain_user_with_no_lookup_configured_is_refused():
    """The default resolver cannot answer, so the plain user is refused."""
    pipe, runner = make_pipe(_responses())
    text = pipe.pipe({"messages": []}, __user__=NON_ADMIN_USER)
    assert "Refused" in text
    assert runner.calls == []


def test_explicit_groups_field_remains_a_fast_path():
    """If a future version supplies `groups`, honour it without a lookup."""
    resolver = membership_resolver(UNDETERMINED, "must not be consulted")
    pipe, runner = make_pipe(_responses(), membership=resolver)
    text = pipe.pipe({"messages": []}, __user__=FUTURE_GROUPS_USER)
    assert "Refused" not in text
    assert runner.calls
    assert resolver.calls == [], "the fast path must short-circuit the lookup"


def test_a_user_without_an_id_cannot_be_looked_up_and_is_refused():
    pipe, runner = make_pipe(_responses())
    text = pipe.pipe({"messages": []}, __user__={"role": "user", "name": "x"})
    assert "Refused" in text
    assert "no usable `id`" in text
    assert runner.calls == []


class _FakeResponse:
    def __init__(self, body: bytes) -> None:
        self._body = body

    def read(self) -> bytes:
        return self._body

    def __enter__(self):
        return self

    def __exit__(self, *_: object) -> bool:
        return False


def _stub_urlopen(
    monkeypatch,
    body: object = None,
    error: Exception | None = None,
    routes: dict[str, object] | None = None,
):
    """Stub the opener, not `urlopen`.

    The lookup builds its own no-redirect opener (a followed 3xx would be
    an unvalidated hop carrying the bearer token), so patching `urlopen`
    would stub a function the code never calls and the test would exercise
    nothing.

    `routes` maps a URL SUBSTRING to the body (or `Exception`) that URL
    answers with. It exists because the lookup makes more than one hop at
    v0.11.0 -- the group listing does not carry `user_ids`, so membership
    comes from a member-bearing endpoint -- and a stub that answered every
    URL with one body let the listing masquerade as a member list.
    """
    import urllib.request

    seen: list[str] = []

    def _answer(url: str):
        if routes is not None:
            for fragment, value in routes.items():
                if fragment in url:
                    return value
            raise OSError(f"no stub route for {url}")
        if error is not None:
            raise error
        return body

    class _FakeOpener:
        def open(self, request, timeout=None):  # noqa: ARG002
            seen.append(request.full_url)
            value = _answer(request.full_url)
            if isinstance(value, Exception):
                raise value
            return _FakeResponse(json.dumps(value).encode("utf-8"))

    monkeypatch.setattr(
        urllib.request, "build_opener", lambda *handlers: _FakeOpener()
    )
    return seen


@pytest.mark.parametrize(
    "user_ids,expected",
    [
        (["u-3", "u-9"], MEMBER),
        (["u-9"], NOT_MEMBER),
    ],
)
def test_http_membership_lookup_reads_the_servers_user_ids(
    monkeypatch, user_ids, expected
):
    """The HTTP transport is what runs when the in-process API is absent."""
    seen = _stub_urlopen(
        monkeypatch,
        body=[
            {"id": "g-0", "name": "other-group", "user_ids": ["u-3"]},
            {"id": "g-1", "name": ADMIN_GROUP, "user_ids": user_ids},
        ],
    )
    state, reason = resolve_group_membership(
        "u-3", ADMIN_GROUP, api_base="http://owui.invalid", api_key="k"
    )
    assert state == expected
    assert seen == ["http://owui.invalid/api/v1/groups/"]
    assert "admin API" in reason


def test_http_membership_lookup_refuses_a_redirect() -> None:
    """A 3xx is refused, not followed.

    The Pipe runs inside the Open WebUI process and cannot call the
    substrate's egress validator, so a followed redirect is exactly where a
    validated first hop becomes an unvalidated second one — with the
    instance bearer token attached. Asserted against the real handler
    rather than a stub, so it fails if the no-redirect opener is dropped.
    """
    import urllib.error
    import urllib.request

    from okg.substrate.chat.review_queue_pipe import _NoRedirect

    handler = _NoRedirect()
    request = urllib.request.Request("http://owui.invalid/api/v1/groups/")
    with pytest.raises(urllib.error.HTTPError):
        handler.redirect_request(
            request, None, 302, "Found", {}, "http://169.254.169.254/latest/meta-data/"
        )


@pytest.mark.parametrize(
    "kwargs",
    [
        {"body": [{"id": "g-1", "name": "some-other-group", "user_ids": ["u-3"]}]},
        {
            # The MEASURED v0.11.0 listing shape (no `user_ids`) with every
            # member-bearing endpoint failing too: nothing answered, so
            # nothing may be concluded.
            "routes": {
                "/api/v1/groups/id/": OSError("member endpoint unavailable"),
                "/api/v1/groups/": [{"id": "g-1", "name": ADMIN_GROUP}],
            }
        },
        {
            # The member endpoints answer, but with nothing readable as ids.
            "routes": {
                "/api/v1/groups/id/": {"user_ids": [{"no": "id"}]},
                "/api/v1/groups/": [{"id": "g-1", "name": ADMIN_GROUP}],
            }
        },
        {"body": {"not": "a list"}},
        {"error": OSError("connection refused")},
    ],
    ids=[
        "group-absent",
        "no-user-ids-and-no-member-endpoint",
        "member-endpoint-has-no-readable-ids",
        "not-a-list",
        "unreachable",
    ],
)
def test_http_membership_lookup_never_allows_on_a_bad_answer(monkeypatch, kwargs):
    _stub_urlopen(monkeypatch, **kwargs)
    state, reason = resolve_group_membership(
        "u-3", ADMIN_GROUP, api_base="http://owui.invalid", api_key="k"
    )
    assert state == UNDETERMINED, f"a bad answer must not allow: {reason}"
    assert reason


@pytest.mark.parametrize(
    "members,expected",
    [(["u-3", "u-9"], MEMBER), (["u-9"], NOT_MEMBER)],
    ids=["member", "non-member"],
)
def test_membership_is_resolved_when_the_listing_omits_user_ids(
    monkeypatch, members, expected
):
    """The v0.11.0 shape must still resolve membership.

    MEASURED against the pinned instance: `GET /api/v1/groups/` returns
    `GroupResponse`, which carries `member_count` but NOT `user_ids`, so
    reading the ids off the listing can never answer. Before this was
    fixed BOTH transports were inert -- the in-process API is async and
    this one had no ids to read -- and the gate could admit nobody but an
    instance admin, which is the inert-Pipe defect one layer deeper.
    """
    seen = _stub_urlopen(
        monkeypatch,
        routes={
            "/api/v1/groups/id/": {"id": "g-1", "user_ids": members},
            "/api/v1/groups/": [{"id": "g-1", "name": ADMIN_GROUP}],
        },
    )
    state, reason = resolve_group_membership(
        "u-3", ADMIN_GROUP, api_base="http://owui.invalid", api_key="k"
    )
    assert state == expected, reason
    # The listing was read first, then a member-bearing endpoint.
    assert seen[0] == "http://owui.invalid/api/v1/groups/"
    assert "/api/v1/groups/id/g-1/" in seen[1]


def test_an_async_in_process_group_api_is_undetermined_not_an_exception(monkeypatch):
    """An async Groups API must refuse, not escape as a TypeError.

    MEASURED at v0.11.0: `Groups.get_groups_by_member_id` is `async def`
    (`models/groups.py:303`), so calling it from this SYNC Pipe returns a
    coroutine. Iterating that coroutine raises `'coroutine' object is not
    iterable`, and that raise used to happen outside the guard -- it
    escaped the Pipe as an unhandled error, observed live against the
    pinned instance. It must be UNDETERMINED, naming the capability.
    """
    import sys
    import types

    module = types.ModuleType("open_webui.models.groups")

    class _AsyncGroups:
        @staticmethod
        async def get_groups_by_member_id(user_id):  # noqa: ARG004
            return []

    module.Groups = _AsyncGroups
    package = types.ModuleType("open_webui")
    models_pkg = types.ModuleType("open_webui.models")
    monkeypatch.setitem(sys.modules, "open_webui", package)
    monkeypatch.setitem(sys.modules, "open_webui.models", models_pkg)
    monkeypatch.setitem(sys.modules, "open_webui.models.groups", module)

    from okg.substrate.chat.review_queue_pipe import _membership_via_open_webui

    state, reason = _membership_via_open_webui("u-3", ADMIN_GROUP)
    assert state == UNDETERMINED
    assert "asynchronous" in reason
    assert state != MEMBER


def test_a_raising_in_process_group_api_is_undetermined_not_an_exception(monkeypatch):
    """Its control: a lookup that raises is also a refusal, never a crash."""
    import sys
    import types

    module = types.ModuleType("open_webui.models.groups")

    class _BrokenGroups:
        @staticmethod
        def get_groups_by_member_id(user_id):  # noqa: ARG004
            raise RuntimeError("no database session")

    module.Groups = _BrokenGroups
    monkeypatch.setitem(sys.modules, "open_webui", types.ModuleType("open_webui"))
    monkeypatch.setitem(
        sys.modules, "open_webui.models", types.ModuleType("open_webui.models")
    )
    monkeypatch.setitem(sys.modules, "open_webui.models.groups", module)

    from okg.substrate.chat.review_queue_pipe import _membership_via_open_webui

    state, reason = _membership_via_open_webui("u-3", ADMIN_GROUP)
    assert state == UNDETERMINED
    assert "no database session" in reason


def test_membership_resolution_never_allows_on_failure():
    """No failure path in the real resolver can return MEMBER."""
    # Both transports unavailable: no Open WebUI in this process, and no
    # HTTP Valves configured.
    state, reason = resolve_group_membership("u-1", ADMIN_GROUP)
    assert state == UNDETERMINED
    assert "unavailable" in reason or "unset" in reason
    assert state != MEMBER


def test_unset_admin_group_valve_refuses_everyone():
    pipe, runner = make_pipe(_responses(), admin_group="")
    text = pipe.pipe({"messages": []}, __user__=ADMIN_USER)
    assert "Refused" in text
    assert "unset" in text
    assert runner.calls == []


# -- the gate is bound to the MEASURED payload, not to a hoped-for one -----


def test_the_gate_answers_the_exact_measured_user_payload():
    """Drive the gate with the real v0.11.0 `__user__` key set.

    The fixtures above are abbreviated dicts. This one is the measured
    22-key payload verbatim, with every key the vendor supplies present
    and `groups` absent -- the shape that made the first version of this
    Pipe inert. The admin arm must be ANSWERED.
    """
    assert len(MEASURED_PIPE_USER_KEYS) == 22
    assert "groups" not in MEASURED_PIPE_USER_KEYS
    user = measured_pipe_user(role="admin", user_id="u-1")
    assert set(user) == set(MEASURED_PIPE_USER_KEYS)

    pipe, runner = make_pipe(_responses())
    text = pipe.pipe({"messages": []}, __user__=user)
    assert "Refused" not in text
    assert runner.calls, "the measured admin payload must reach the CLI"


def test_the_measured_payload_still_refuses_a_non_admin():
    """The refusal control for the arm above.

    Same 22 keys, same absent `groups`, `role` the only difference. If the
    gate were passing everyone, the test above would prove nothing.
    """
    user = measured_pipe_user(role="user", user_id="u-2")
    pipe, runner = make_pipe(_responses())
    text = pipe.pipe({"messages": []}, __user__=user)
    assert "Refused" in text
    assert runner.calls == []


def test_the_census_records_groups_as_never_supplied():
    """Bind the gate's keying to the artifact that measured it.

    If a future edit re-keys the gate onto `groups`, the two tests above
    catch the behaviour; this one names the reason so the failure is
    diagnosable rather than mysterious.
    """
    census = json.loads(ui_knob_census_path().read_text())
    assert census["openwebui_version"] == "0.11.4"
    entries = [
        knob
        for knob in census["knobs"]
        if "__user__" in str(knob.get("knob", ""))
    ]
    assert len(entries) == 1, "the census entry this gate rests on is missing"
    detail = entries[0]["mechanism_detail"]
    assert "'groups' is NOT among them" in detail
    assert "'role' IS present" in detail


# -- the instance-side grant audit -----------------------------------------
# The readback half of this task, proven here on the LOGIC and driven live
# against the instance. The fake below mirrors the vendor's own
# `search_models` semantics at v0.11.0, so a checker that takes any of the
# three documented shortcuts fails against it rather than passing quietly.


# The id the workspace listing ACTUALLY holds. Open WebUI v0.11.0 composes
# a manifold's model id as `f'{pipe.id}.{p["id"]}'` (functions.py:106,
# measured read-only in the running `open-webui` container), so a fake
# listing keyed on the bare `pipes()` id would model a workspace that
# cannot exist -- and the audit would pass against it while matching zero
# rows live.
INSTALLED_FUNCTION_ID = "okg_review_inbox"
INBOX_MODEL_ID = live_manifold_model_id(INSTALLED_FUNCTION_ID)


def _model_row(model_id: str, *, name: str, grants: list[dict] | None = None):
    return {
        "id": model_id,
        "name": name,
        "base_model_id": "gpt-4o",
        "access_grants": list(grants or []),
    }


class FakeWorkspaceListing:
    """`GET /api/v1/models/list` as v0.11.0 actually implements it.

    - pages at PAGE_ITEM_COUNT (routers/models.py:134)
    - returns {items, total} (models/models.py:167-169)
    - `?query=` matches name / base_model_id / user fields, NOT Model.id
      (models/models.py:349-357)
    """

    def __init__(self, rows: list[dict]) -> None:
        self.rows = rows
        self.requested: list[tuple[int, str | None]] = []

    def fetch(self, page: int, *, query: str | None = None) -> dict:
        self.requested.append((page, query))
        rows = self.rows
        if query:
            rows = [
                row
                for row in rows
                if query.lower() in str(row.get("name", "")).lower()
                or query.lower() in str(row.get("base_model_id", "")).lower()
            ]
        skip = (page - 1) * PAGE_ITEM_COUNT
        return {"items": rows[skip : skip + PAGE_ITEM_COUNT], "total": len(rows)}


GROUP_GRANT = {
    "id": "ag-1",
    "resource_type": "model",
    "resource_id": INBOX_MODEL_ID,
    "principal_type": "group",
    "principal_id": "g-admins",
    "permission": "read",
}
CONTROL_GRANT = {**GROUP_GRANT, "id": "ag-2", "resource_id": "granted-control"}


def _workspace_with_inbox_on_page_two() -> FakeWorkspaceListing:
    """45 models, with the inbox preset past the 30-item page boundary."""
    rows = [
        _model_row(f"filler-{index:02d}", name=f"Filler {index:02d}")
        for index in range(43)
    ]
    rows.append(
        _model_row("granted-control", name="Control", grants=[CONTROL_GRANT])
    )
    rows.append(
        _model_row(INBOX_MODEL_ID, name="OKG Review Inbox", grants=[GROUP_GRANT])
    )
    assert len(rows) > PAGE_ITEM_COUNT, "the fixture must span more than one page"
    return FakeWorkspaceListing(rows)


def test_grant_check_walks_past_page_one_to_find_the_preset():
    listing = _workspace_with_inbox_on_page_two()
    report = run_grant_check(
        listing.fetch,
        model_id=INBOX_MODEL_ID,
        admin_group_id="g-admins",
        control_model_id="granted-control",
    )
    assert report.ok, report.summary()
    assert report.inbox.matched == 1
    assert report.inbox.group_principals == ("g-admins",)
    assert report.inbox.public_grants == ()
    assert report.walk.pages == 2, "the preset is past the first page"
    assert report.walk.total == 45


def test_a_page_one_only_read_would_have_missed_it():
    """The mutation control for the walk.

    Reading only page 1 of the same listing -- the shortcut the previous
    form of this check took -- matches ZERO rows. That zero is what was
    formerly read as "no public grant found".
    """
    listing = _workspace_with_inbox_on_page_two()
    page_one = listing.fetch(1)["items"]
    audit = audit_model_grants(page_one, model_id=INBOX_MODEL_ID)
    assert audit.matched == 0
    assert any("FAILED verification" in refusal for refusal in audit.refusals)


def test_the_query_filter_does_not_match_the_model_id():
    """MEASURED: `?query=` matches name / base_model_id / user fields only.

    A checker that narrowed the listing with `?query=okg-review-inbox`
    would get an empty page and conclude the preset does not exist.
    """
    listing = _workspace_with_inbox_on_page_two()
    by_id = listing.fetch(1, query=INBOX_MODEL_ID)
    assert by_id["items"] == [] and by_id["total"] == 0
    # Control: the same filter DOES match on the name, so the empty result
    # above is about the field, not about a broken fake.
    by_name = listing.fetch(1, query="OKG Review Inbox")
    assert by_name["total"] == 1


def test_zero_matches_is_a_refusal_never_a_clean_result():
    listing = _workspace_with_inbox_on_page_two()
    report = run_grant_check(
        listing.fetch,
        model_id="a-preset-that-was-never-installed",
        admin_group_id="g-admins",
        control_model_id="granted-control",
    )
    assert not report.ok
    assert any("matched 0 rows" in refusal for refusal in report.refusals)
    # The control still answered, so the failure is attributed correctly.
    assert report.control.grants


def test_an_empty_control_makes_the_whole_report_uninterpretable():
    """The positive control is load-bearing, not decoration."""
    rows = [
        _model_row("granted-control", name="Control"),  # NO grants
        _model_row(INBOX_MODEL_ID, name="OKG Review Inbox"),
    ]
    report = run_grant_check(
        FakeWorkspaceListing(rows).fetch,
        model_id=INBOX_MODEL_ID,
        admin_group_id="g-admins",
        control_model_id="granted-control",
    )
    assert not report.ok
    assert any("uninterpretable" in refusal for refusal in report.refusals)


def test_a_public_grant_is_refused():
    rows = [
        _model_row("granted-control", name="Control", grants=[CONTROL_GRANT]),
        _model_row(
            INBOX_MODEL_ID,
            name="OKG Review Inbox",
            grants=[
                GROUP_GRANT,
                {
                    "id": "ag-3",
                    "resource_type": "model",
                    "resource_id": INBOX_MODEL_ID,
                    "principal_type": "anyone",
                    "principal_id": "*",
                    "permission": "read",
                },
            ],
        ),
    ]
    report = run_grant_check(
        FakeWorkspaceListing(rows).fetch,
        model_id=INBOX_MODEL_ID,
        admin_group_id="g-admins",
        control_model_id="granted-control",
    )
    assert not report.ok
    assert any("public grant" in refusal for refusal in report.refusals)


def test_the_openai_compat_route_shape_is_refused_by_name():
    """`GET /api/v1/models` returns {"data": [...]}, not {items, total}.

    Against that payload a `.items[]` selector matches nothing and prints
    nothing -- the pass-by-absence this refusal exists to convert into a
    named failure.
    """
    with pytest.raises(ListingRefusal) as err:
        walk_model_pages(lambda _page: {"data": [{"id": "okg-review-inbox"}]})
    assert OPENAI_COMPAT_MODELS_PATH in str(err.value)
    assert WORKSPACE_MODELS_LIST_PATH in str(err.value)


def test_a_stalled_walk_refuses_rather_than_returning_a_short_list():
    def fetch(page: int) -> dict:
        return {"items": [], "total": 45} if page > 1 else {
            "items": [_model_row(f"m-{i}", name=f"M{i}") for i in range(30)],
            "total": 45,
        }

    with pytest.raises(ListingRefusal, match="stalled"):
        walk_model_pages(fetch)


def test_a_repeated_row_refuses_rather_than_reaching_the_total_vacuously():
    """A listing that shifts mid-walk can reach `total` without enumerating it."""

    def fetch(page: int) -> dict:
        return {
            "items": [_model_row("m-0", name="M0")] * 1,
            "total": 2,
        }

    with pytest.raises(ListingRefusal, match="appeared twice"):
        walk_model_pages(fetch)


def test_a_missing_access_grants_key_is_unreadable_not_ungranted():
    """`access_control` does not exist at v0.11.0; reading it yields null.

    A row whose grants were never read must not be counted as a row with
    no grants.
    """
    audit = audit_model_grants(
        [{"id": INBOX_MODEL_ID, "name": "OKG Review Inbox"}],
        model_id=INBOX_MODEL_ID,
    )
    assert audit.matched == 1
    assert not audit.readable
    assert any("access_control" in refusal for refusal in audit.refusals)


# -- an unset admin group is UNMET, never "condition satisfied" -----------
# The defect: `if admin_group_id and admin_group_id not in group_principals`.
# With `admin_group_id=""` -- the value a forgotten Valve arrives as -- the
# `and` short-circuits, the group-grant condition is never evaluated, and
# the report comes back ok. That is the module's own stated rule inverted:
# an unset Valve is UNMET, never assumed satisfied.


@pytest.mark.parametrize(
    "admin_group_id", ["", "   ", None], ids=["empty", "whitespace", "none"]
)
def test_an_unset_admin_group_is_unmet_not_a_satisfied_condition(admin_group_id):
    listing = _workspace_with_inbox_on_page_two()
    report = run_grant_check(
        listing.fetch,
        model_id=INBOX_MODEL_ID,
        admin_group_id=admin_group_id,
        control_model_id="granted-control",
    )
    assert not report.ok, report.summary()
    assert any("unmet:" in refusal for refusal in report.refusals), report.refusals
    assert any(
        "never assumed satisfied" in refusal for refusal in report.refusals
    )
    # The control still answered, so the refusal is attributed to the unset
    # input rather than to a query that cannot see grants.
    assert report.control.grants


def test_a_supplied_admin_group_still_passes_and_still_catches_a_mismatch():
    """Non-vacuity control for the arm above, in the SAME suite.

    Two arms, because an audit that refused every group id would satisfy
    the unset-is-unmet assertion and prove nothing: the RIGHT id passes,
    and a DIFFERENT id is still caught.
    """
    listing = _workspace_with_inbox_on_page_two()
    passing = run_grant_check(
        listing.fetch,
        model_id=INBOX_MODEL_ID,
        admin_group_id="g-admins",
        control_model_id="granted-control",
    )
    assert passing.ok, passing.summary()

    mismatched = run_grant_check(
        _workspace_with_inbox_on_page_two().fetch,
        model_id=INBOX_MODEL_ID,
        admin_group_id="g-some-other-group",
        control_model_id="granted-control",
    )
    assert not mismatched.ok
    assert any("no group grant naming" in r for r in mismatched.refusals)
    assert not any("unmet:" in r for r in mismatched.refusals)


def test_the_positive_control_opts_out_of_the_group_condition_by_name():
    """The control asserts no group grant -- and says so with a CONSTANT.

    The opt-out may not be spelled `""` or `None`, because those are the
    values a forgotten input arrives as; selecting the "assert nothing"
    arm must be a deliberate, named choice.
    """
    from okg.substrate.chat.review_inbox_grant_audit import (
        NO_GROUP_GRANT_CONDITION,
        audit_model_grants,
    )

    rows = [_model_row("granted-control", name="Control", grants=[CONTROL_GRANT])]
    control = audit_model_grants(
        rows, model_id="granted-control", admin_group_id=NO_GROUP_GRANT_CONDITION
    )
    assert control.refusals == ()
    assert control.grants
    # And the sentinel is not a value a forgotten input could produce.
    assert NO_GROUP_GRANT_CONDITION.strip()
    assert NO_GROUP_GRANT_CONDITION not in {"", None}

    # Control: the same rows audited WITHOUT the sentinel refuse as unmet.
    unset = audit_model_grants(rows, model_id="granted-control")
    assert any("unmet:" in refusal for refusal in unset.refusals)


# -- the id a manifold Pipe actually registers under ----------------------
# MEASURED, read-only, in the operator's running instance
# (container `open-webui`, ghcr.io/open-webui/open-webui:v0.11.0,
# `GET /api/version` -> 0.11.0): functions.py:106 composes a manifold's
# model id as `f'{pipe.id}.{p["id"]}'`, where `pipe.id` is the FUNCTION id
# the module is installed under and `p["id"]` is the entry this Pipe's own
# `pipes()` returns. So `okg-review-inbox` NEVER appears as a row id, and
# auditing it against a live listing matches ZERO rows -- which
# acc.chat-review-queue-pipe.bounded-exception requires to be 1.


def test_the_live_model_id_is_the_function_id_dot_the_pipe_id():
    from okg.substrate.chat.review_inbox_grant_audit import live_manifold_model_id
    from okg.substrate.chat.review_queue_pipe import MODEL_ID, Pipe

    assert live_manifold_model_id("okg_review_inbox") == (
        f"okg_review_inbox.{MODEL_ID}"
    )
    # The pipe id half is the one the vendor reads off `pipes()`, not a
    # second copy of the literal.
    assert Pipe().pipes()[0]["id"] == MODEL_ID
    assert live_manifold_model_id("fn").endswith("." + Pipe().pipes()[0]["id"])


def test_auditing_the_bare_pipe_id_refuses_instead_of_matching_zero_rows():
    """A zero match against the bare id is the pass-by-absence to kill."""
    from okg.substrate.chat.review_inbox_grant_audit import live_manifold_model_id
    from okg.substrate.chat.review_queue_pipe import MODEL_ID

    listing = _workspace_with_inbox_on_page_two()
    with pytest.raises(ValueError, match="functions.py:106"):
        run_grant_check(
            listing.fetch,
            model_id=MODEL_ID,
            admin_group_id="g-admins",
            control_model_id="granted-control",
        )

    # Control: the SAME check against a listing whose row carries the
    # composed id passes, so the refusal above is about the id shape and
    # not about a check that refuses everything.
    composed = live_manifold_model_id("okg_review_inbox")
    rows = [
        _model_row("granted-control", name="Control", grants=[CONTROL_GRANT]),
        _model_row(
            composed,
            name="OKG Review Inbox",
            grants=[{**GROUP_GRANT, "resource_id": composed}],
        ),
    ]
    report = run_grant_check(
        FakeWorkspaceListing(rows).fetch,
        model_id=composed,
        admin_group_id="g-admins",
        control_model_id="granted-control",
    )
    assert report.ok, report.summary()
    assert report.inbox.matched == 1


def test_the_control_may_not_be_its_own_subject():
    with pytest.raises(ValueError, match="DIFFERENT model row"):
        run_grant_check(
            FakeWorkspaceListing([]).fetch,
            model_id="okg-review-inbox",
            admin_group_id="g-admins",
            control_model_id="okg-review-inbox",
        )


# -- argv-slot confinement -------------------------------------------------


def _legal_argvs(message: str | None) -> list[list[str]]:
    """Every argv the Pipe is permitted to build for these fixtures.

    Assembled here from the Valves, from the rows the fixtures list, and
    from the fixed verdict vocabularies -- exactly the three sources the
    requirement allows. `message` may appear in one of them and only as
    the `--query` value.
    """
    legal = [
        build_pact_view_argv(CLI, REPO_ROOT),
        build_admission_list_argv(CLI, DEPLOYMENT),
        build_admission_show_argv(
            CLI, DEPLOYMENT, PENDING_ENVELOPE["admission_id"]
        ),
    ]
    if message is not None:
        legal.append(build_pact_view_argv(CLI, REPO_ROOT, query=message))
    for verdict in PACT_DECIDABLE_VERDICTS:
        for change in ("some-change", "broken-change"):
            legal.append(
                build_pact_decide_argv(
                    CLI, REPO_ROOT, change=change, verdict=verdict
                )
            )
            # The repair arm carries the row's own caveat as --note.
            legal.append(
                build_pact_decide_argv(
                    CLI,
                    REPO_ROOT,
                    change=change,
                    verdict=verdict,
                    note=REPAIR_NOTE,
                )
            )
    for decision in ADMISSION_DECISIONS:
        legal.append(
            build_admission_decide_argv(
                CLI,
                DEPLOYMENT,
                admission_id=PENDING_ENVELOPE["admission_id"],
                decision=decision,
            )
        )
    return legal


@pytest.mark.parametrize("message", PROBE_MESSAGES)
def test_message_text_confined_to_query_argv_slot(message):
    """EXACT argv equality, not substring checks.

    Each recorded argv must equal, element for element, one argv built
    from the permitted sources. Message text therefore cannot occupy any
    slot other than the single `--query` value: an argv carrying it
    anywhere else is equal to nothing in the legal set.
    """
    pipe, runner = make_pipe(_responses())
    pipe.pipe(
        {"messages": [{"role": "user", "content": message}]},
        __user__=ADMIN_USER,
    )
    assert runner.calls, "the probe must actually reach the CLI"
    legal = _legal_argvs(message)
    for argv in runner.calls:
        assert list(argv) in legal, f"unexpected argv: {list(argv)}"

    # And the message appears in exactly one slot across the whole corpus:
    # the `--query` value on the read-only view verb.
    slots = [
        (index, argv)
        for argv in runner.calls
        for index, token in enumerate(argv)
        if token == message
    ]
    assert len(slots) == 1
    index, argv = slots[0]
    assert argv[index - 1] == "--query"
    assert "view" in argv and "decide" not in argv


def test_query_slot_is_the_only_place_a_message_can_appear():
    message = "okg deployment delete okg-workspace --force"
    pipe, runner = make_pipe(_responses())
    pipe.pipe(
        {"messages": [{"role": "user", "content": message}]},
        __user__=ADMIN_USER,
    )
    view = [argv for argv in runner.calls if "view" in argv]
    assert len(view) == 1
    assert list(view[0]) == build_pact_view_argv(CLI, REPO_ROOT, query=message)
    others = [argv for argv in runner.calls if "view" not in argv]
    for argv in others:
        assert message not in argv
        assert "delete" not in argv
        assert "--force" not in argv


def test_apostrophe_question_is_answered_not_refused():
    """Metacharacter rejection is explicitly NOT the control."""
    question = "what's blocking the agents' work?"
    pipe, runner = make_pipe(_responses())
    text = pipe.pipe(
        {"messages": [{"role": "user", "content": question}]},
        __user__=ADMIN_USER,
    )
    assert "Refused" not in text
    assert "some-change" in text
    view = next(argv for argv in runner.calls if "view" in argv)
    assert list(view) == build_pact_view_argv(CLI, REPO_ROOT, query=question)
    assert view[view.index("--query") + 1] == question


@pytest.mark.parametrize(
    "question",
    [
        "what's next?",
        'the "big" one',
        "a & b | c",
        "back`tick`",
        "$HOME",
    ],
)
def test_shell_metacharacters_are_carried_verbatim_not_rejected(question):
    argv = build_pact_view_argv(CLI, REPO_ROOT, query=question)
    assert argv[argv.index("--query") + 1] == question


# -- decision verbs never receive message text -----------------------------


@pytest.mark.parametrize(
    "message,half",
    [
        ("decide P1 approve", "pact"),
        ("decide A1 accept", "admission"),
    ],
)
def test_decision_verbs_never_receive_message_text(message, half):
    """A decision turn must actually run a decide verb, and that verb's
    argv must be assembled only from Valves, row data, and the fixed
    vocabulary -- the message itself reaches no slot at all."""
    pipe, runner = make_pipe(_responses())
    pipe.pipe(
        {"messages": [{"role": "user", "content": message}]},
        __user__=ADMIN_USER,
    )
    decides = [argv for argv in runner.calls if "decide" in argv]
    assert decides, "the decision arm must be exercised or this proves nothing"
    legal = _legal_argvs(None)  # note: NO query argv is legal on this turn
    for argv in decides:
        assert list(argv) in legal
        # The message never reaches argv, in whole or as a token.
        assert message not in argv
        assert all(token != "P1" and token != "A1" for token in argv)
    # A decision turn does not also send the message to --query.
    assert not any("--query" in argv for argv in runner.calls)


def test_a_valid_decision_builds_argv_only_from_rows_and_valves():
    pipe, runner = make_pipe(_responses())
    text = pipe.pipe(
        {"messages": [{"role": "user", "content": "decide P1 approve"}]},
        __user__=ADMIN_USER,
    )
    decide = next(argv for argv in runner.calls if "decide" in argv)
    assert list(decide) == build_pact_decide_argv(
        CLI, REPO_ROOT, change="some-change", verdict="approve"
    )
    assert "Ledger entry `41`" in text


def test_admission_decision_reads_the_envelope_back():
    pipe, runner = make_pipe(_responses())
    text = pipe.pipe(
        {"messages": [{"role": "user", "content": "decide A1 accept"}]},
        __user__=ADMIN_USER,
    )
    decide = next(
        argv for argv in runner.calls if "decide" in argv and "admission" in argv
    )
    assert list(decide) == build_admission_decide_argv(
        CLI,
        DEPLOYMENT,
        admission_id=PENDING_ENVELOPE["admission_id"],
        decision="accept",
    )
    assert any("show" in argv for argv in runner.calls), (
        "the outcome must be read back from the CLI, never assumed"
    )
    assert "status is now `accepted`" in text


def test_repair_row_decision_carries_the_rows_own_note_not_message_text():
    """The `--note` builder branch, exercised rather than left unproven."""
    pipe, runner = make_pipe(_responses())
    pipe.pipe(
        {"messages": [{"role": "user", "content": "decide P2 revise"}]},
        __user__=ADMIN_USER,
    )
    decide = next(argv for argv in runner.calls if "decide" in argv)
    assert list(decide) == build_pact_decide_argv(
        CLI,
        REPO_ROOT,
        change="broken-change",
        verdict="revise",
        note=REPAIR_NOTE,
    )
    assert "--note" in decide
    # The note came from the ROW's caveat, which the CLI builds from its
    # own flag literals -- never from the operator's message.
    assert decide[decide.index("--note") + 1] == REPAIR_NOTE
    assert REPAIR_NOTE == BASE_PACKET["chat_review_queue"]["rows"][1]["caveat"]


def test_supersede_is_never_executed_from_chat():
    """It needs --target, which the grammar has no slot for."""
    assert parse_decision_intent("decide P1 supersede") is None
    with pytest.raises(ValueError):
        build_pact_decide_argv(
            CLI, REPO_ROOT, change="some-change", verdict="supersede"
        )
    pipe, runner = make_pipe(_responses())
    text = pipe.pipe(
        {"messages": [{"role": "user", "content": "decide P1 supersede"}]},
        __user__=ADMIN_USER,
    )
    assert not any("decide" in argv for argv in runner.calls)
    # But it IS still spelled out in the row's rendered instruction.
    assert "supersede" in text


def test_a_decide_turn_renders_the_row_it_decides():
    """The handle typed and the row decided appear in the SAME response.

    A decision message does not parse as a question, so it never reaches
    `--query` and the response is never the answer-first form -- the
    ranked list the handle indexes is on screen in the same answer.
    """
    pipe, runner = make_pipe(_responses())
    text = pipe.pipe(
        {"messages": [{"role": "user", "content": "decide P1 approve"}]},
        __user__=ADMIN_USER,
    )
    assert "**P1 `some-change`**" in text, (
        "the decided row must be listed in the same response"
    )
    assert "Read as:" not in text, "a decision turn is never answer-first"
    assert not any("--query" in argv for argv in runner.calls)
    decide = next(argv for argv in runner.calls if "decide" in argv)
    assert "some-change" in decide


def test_out_of_range_handle_refuses_and_decides_nothing():
    pipe, runner = make_pipe(_responses())
    text = pipe.pipe(
        {"messages": [{"role": "user", "content": "decide P99 approve"}]},
        __user__=ADMIN_USER,
    )
    assert "There is no row" in text
    assert not any("decide" in argv for argv in runner.calls)


def test_free_text_verdicts_are_rejected_by_the_builders():
    with pytest.raises(ValueError):
        build_pact_decide_argv(
            CLI, REPO_ROOT, change="c", verdict="approve --and-also"
        )
    with pytest.raises(ValueError):
        build_admission_decide_argv(
            CLI, DEPLOYMENT, admission_id="a", decision="accept; drop table"
        )


@pytest.mark.parametrize(
    "message",
    [
        "decide P1 obliterate",
        "decide A1 approve",  # a PACT verdict on the admission half
        "decide X1 approve",
        "decide P0 approve",
        "decide P1",
        "decide P1 approve extra",
    ],
)
def test_decision_grammar_rejects_anything_outside_the_vocabularies(message):
    assert parse_decision_intent(message) is None


def test_decision_grammar_yields_typed_values_only():
    intent = parse_decision_intent("decide P3 revise")
    assert intent is not None
    assert intent.half == "pact"
    assert isinstance(intent.index, int)
    assert intent.verdict in PACT_HUMAN_VERDICTS
    admission = parse_decision_intent("decide A2 reject")
    assert admission is not None
    assert admission.verdict in ADMISSION_DECISIONS


# -- no verb outside the enumerated set ------------------------------------


def test_non_enumerated_verbs_never_invoked():
    pipe, runner = make_pipe(_responses())
    for message in PROBE_MESSAGES + ["decide P1 approve", "decide A1 accept"]:
        pipe.pipe(
            {"messages": [{"role": "user", "content": message}]},
            __user__=ADMIN_USER,
        )
    for argv in runner.calls:
        tokens = [token for token in argv[1:] if not token.startswith("-")]
        tokens = [t for t in tokens if t not in {REPO_ROOT, DEPLOYMENT}]
        assert tuple(tokens[:2]) in ENUMERATED_OKG_VERBS, (
            f"non-enumerated verb reached argv: {' '.join(argv)}"
        )
    joined = [" ".join(argv) for argv in runner.calls]
    assert not any("pact gate" in call for call in joined)
    assert not any("evidence" in call for call in joined)
    assert not any("psql" in call or "query" == call.split()[-1] for call in joined)


def test_runner_refuses_a_verb_outside_the_enumerated_set():
    with pytest.raises(ValueError, match="outside the review inbox"):
        run_okg([CLI, "deployment", "delete", DEPLOYMENT])
    with pytest.raises(ValueError, match="outside the review inbox"):
        run_okg([CLI, "pact", "gate", "task-done", "x", "y"])


def test_no_graph_answering_path_exists():
    """No MCP operator and no SQL verb is reachable from this module."""
    for verb in ENUMERATED_OKG_VERBS:
        assert verb[0] in {"pact", "admission", "external-action"}
    assert ("query",) not in ENUMERATED_OKG_VERBS
    assert not any("search" in verb for verb in ENUMERATED_OKG_VERBS)


# -- the write-transport precondition --------------------------------------


@pytest.mark.parametrize(
    "kwargs,expected",
    [
        ({"okg_cli": ""}, "okg_cli"),
        ({"repo_root": ""}, "repo_root"),
        ({"deployment": ""}, "deployment"),
        ({"host_ok": False}, "okg_cli"),
    ],
    ids=["unset-cli", "unset-root", "unset-deployment", "probes-fail"],
)
def test_unmet_host_precondition_refuses_and_runs_nothing(kwargs, expected):
    pipe, runner = make_pipe(_responses(), **kwargs)
    text = pipe.pipe(
        {"messages": [{"role": "user", "content": "decide P1 approve"}]},
        __user__=ADMIN_USER,
    )
    assert "host preconditions unmet" in text
    assert expected in text
    assert runner.calls == [], "no subprocess may run on the unmet path"
    assert pipe.command_log == [], "the command log must be empty"
    assert "verified empty" not in text.lower()
    assert "0 " not in text.replace("0.", "")


def test_unset_valve_reads_as_unmet_even_when_probes_would_pass():
    pipe, _ = make_pipe(_responses(), okg_cli="", host_ok=True)
    text = pipe.pipe({"messages": []}, __user__=ADMIN_USER)
    assert "unmet, never assumed" in text


def test_unavailable_dsn_withholds_decision_verbs_without_deciding():
    responses = _responses()
    responses["admission list"] = result(
        "", returncode=2, stderr="connection refused"
    )
    pipe, runner = make_pipe(responses)
    text = pipe.pipe(
        {"messages": [{"role": "user", "content": "decide A1 accept"}]},
        __user__=ADMIN_USER,
    )
    assert "Decisions — withheld" in text
    # The withholding names the MEASURED reason, not a presumed cause.
    assert "admission source did not answer" in text
    assert "exited 2" in text
    assert not any("decide" in argv for argv in runner.calls), (
        "no decide subprocess may run when the precondition is unmet"
    )
    assert "nothing is queued for later" in text


def test_decision_verbs_are_offered_when_the_precondition_holds():
    """Non-vacuity for the withholding arm."""
    pipe, _ = make_pipe(_responses())
    text = pipe.pipe({"messages": []}, __user__=ADMIN_USER)
    assert "Decision verbs are available" in text
    assert "withheld" not in text


# -- the surface and the executor must share ONE predicate -----------------
# The defect this covers: the renderer withheld the verbs when EITHER
# source failed, while the executor gated on only the ADDRESSED half. With
# the PACT half down, a `decide A<n>` turn rendered "Decisions — withheld"
# AND ran `okg admission decide`, reporting `status is now accepted`. A
# write landed while the surface said it had refused, which is the exact
# thing scn.chat-review-queue-pipe.precondition-unmet forbids.


@pytest.mark.parametrize(
    "down,message,marker",
    [
        # CROSS-half: the failed source is NOT the one being decided.
        ("pact view", "decide A1 accept", "PACT queue source did not answer"),
        ("admission list", "decide P1 approve", "admission source did not answer"),
        # SAME-half, kept alongside so the cross arms are read as an
        # extension of a working rule rather than a separate one.
        ("pact view", "decide P1 approve", "PACT queue source did not answer"),
        ("admission list", "decide A1 accept", "admission source did not answer"),
    ],
    ids=[
        "pact-down-admission-decided",
        "admission-down-pact-decided",
        "pact-down-pact-decided",
        "admission-down-admission-decided",
    ],
)
def test_an_unavailable_source_withholds_and_executes_nothing(down, message, marker):
    """Whatever the surface withholds, the executor must also refuse.

    The proof is the COMMAND LOG, not the absence of an error string: a
    decide that ran and then rendered a refusal would still have mutated
    the ledger or the envelope.
    """
    responses = _responses()
    responses[down] = result("", returncode=2, stderr="connection refused")
    pipe, runner = make_pipe(responses)
    text = pipe.pipe(
        {"messages": [{"role": "user", "content": message}]},
        __user__=ADMIN_USER,
    )
    assert "Decisions — withheld" in text
    assert marker in text
    assert "Decision not executed" in text
    assert not any("decide" in argv for argv in runner.calls), (
        f"a decide subprocess ran while the surface said it refused: "
        f"{[' '.join(argv) for argv in runner.calls]}"
    )
    assert not any("decide" in argv for argv in pipe.command_log)
    # And no outcome is claimed from a decision that never ran.
    assert "status is now" not in text
    assert "Ledger entry" not in text


@pytest.mark.parametrize(
    "message,claim",
    [
        ("decide P1 approve", "Ledger entry `41`"),
        ("decide A1 accept", "status is now `accepted`"),
    ],
    ids=["pact", "admission"],
)
def test_both_sources_available_actually_executes_the_decision(message, claim):
    """Non-vacuity control for the arms above, in the SAME suite.

    Without it, an executor that refused unconditionally would satisfy
    every assertion in `test_an_unavailable_source_withholds_and_executes_nothing`.
    """
    pipe, runner = make_pipe(_responses())
    text = pipe.pipe(
        {"messages": [{"role": "user", "content": message}]},
        __user__=ADMIN_USER,
    )
    assert "Decision verbs are available" in text
    assert "Decisions — withheld" not in text
    assert any("decide" in argv for argv in runner.calls)
    assert any("decide" in argv for argv in pipe.command_log)
    assert claim in text


def test_the_surface_and_the_executor_read_the_same_predicate():
    """Bind them structurally, so the two can never drift apart again.

    The behavioural arms above cover the four (failed source, decided
    half) combinations. This one names the mechanism: both callers read
    `decision_blockers`, so a future edit that reintroduces a
    half-specific gate in only one of them fails here rather than
    silently.
    """
    from okg.substrate.chat.review_queue_pipe import (
        decision_blockers,
        render_decision_availability,
    )

    pact_down = SourceOutcome(
        source="pact", state=UNAVAILABLE, command="okg pact view", reason="exited 2"
    )
    admission_up = SourceOutcome(
        source="admission", state=POPULATED, command="okg admission list", rows=()
    )
    met = [Precondition("okg_cli", True, "ok")]

    blockers = decision_blockers(
        preconditions=met, pact=pact_down, admission=admission_up
    )
    assert blockers, "a failed PACT source must block decisions on BOTH halves"
    rendered = "\n".join(
        render_decision_availability(
            preconditions=met, pact=pact_down, admission=admission_up
        )
    )
    assert "Decisions — withheld" in rendered
    for blocker in blockers:
        assert blocker in rendered, "the renderer must print the same blockers"

    # Control: with both sources up the predicate is empty and the
    # renderer offers the verbs, so the assertion above is not passing
    # against a predicate that always blocks.
    pact_up = SourceOutcome(
        source="pact", state=POPULATED, command="okg pact view", rows=()
    )
    assert (
        decision_blockers(preconditions=met, pact=pact_up, admission=admission_up)
        == []
    )
    assert "Decision verbs are available" in "\n".join(
        render_decision_availability(
            preconditions=met, pact=pact_up, admission=admission_up
        )
    )


# -- no branch predicate reaches argv, on any turn -------------------------
# The DELETED assertion. Its former form (`--mode review` implies the
# quarantine branch, refuse if a Valve disagreed) would have refused
# against every MCP-created envelope, because those carry no branch at
# all -- a fail-closed guard converted into a denial of the one thing
# this surface exists to show.


def test_no_argv_ever_carries_a_branch_predicate():
    pipe, runner = make_pipe(_responses())
    for message in PROBE_MESSAGES + ["decide P1 approve", "decide A1 accept"]:
        pipe.pipe(
            {"messages": [{"role": "user", "content": message}]},
            __user__=ADMIN_USER,
        )
    assert runner.calls, "the probe must actually reach the CLI"
    for argv in runner.calls:
        assert "--branch" not in argv
        assert FORBIDDEN_BRANCH_LITERAL not in argv
    # Control: the flags that DO identify the pending set are on the read,
    # so the absence above is not passing against an empty argv corpus.
    listing = next(argv for argv in runner.calls if "list" in argv)
    # `--held` is every held admission mode, resolved by the CLI (#1752).
    assert "--held" in listing and "--mode" not in listing
    assert "--deployment" in listing


def test_optimistic_done_is_never_rendered():
    responses = _responses()
    responses["admission show"] = json_result(
        {"admission": {"admission_id": "x", "status": "pending"}}
    )
    pipe, _ = make_pipe(responses)
    text = pipe.pipe(
        {"messages": [{"role": "user", "content": "decide A1 accept"}]},
        __user__=ADMIN_USER,
    )
    assert "Still pending" in text
    assert "status is now `pending`" in text


def test_decide_with_no_readback_claims_nothing():
    responses = _responses()
    responses["admission show"] = result("", returncode=1, stderr="gone")
    pipe, _ = make_pipe(responses)
    text = pipe.pipe(
        {"messages": [{"role": "user", "content": "decide A1 accept"}]},
        __user__=ADMIN_USER,
    )
    assert "Outcome unknown" in text
    assert "no post-decision status is claimed" in text
