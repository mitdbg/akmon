"""Load-time refusals of a `kind: http` live binding.

Issue #2100. Hermetic: no database, no socket, no server. Every row here
drives :func:`parse_http_binding` or
:func:`validate_live_binding_configs` directly, because a config-shape
refusal is a property of the parser and does not need a composed catalog to
prove.

**That is the cheap half of the proof, not the whole of it.** A refusal that
only ever fired in a unit test would be a refusal with no production caller,
which is the defect two previous reviews of this sprint found. One
representative of every refusal CLASS below is proved again through
`build_server` in `test_live_relation_http_adapter.py`, so removing the hook
from `_build_server` reds a row there.
"""
from __future__ import annotations

import pytest

from okg.substrate.external_backed.http_adapter import (
    HTTP_CONFIG_FIELDS,
    LiveAdapterArgUnconstrained,
    LiveAdapterConfigInvalid,
    LiveAdapterHostRefused,
    LiveAdapterSecretMissing,
    parse_http_binding,
    validate_live_binding_configs,
)
from okg.substrate.external_backed.live_relations import (
    LiveBinding,
    LiveRelation,
    LiveRelationArg,
)
from tests.substrate._fake_credentials import fake_credential

#: A binding that PASSES, so every row below differs from a working one by
#: exactly the thing it is testing. A base that did not parse would let a
#: test pass for the wrong reason.
GOOD_CONFIG: dict = {
    "base_url": "http://127.0.0.1:9200",
    "allowed_hosts": ["127.0.0.1:9200"],
    "allow_non_global": True,
    "method": "POST",
    "path": "/logs-*/_search",
    "body": {
        "size": {"$arg": "limit"},
        "query": {"match": {"message": {"$arg": "query"}}},
    },
    "rows_path": "hits.hits",
    "max_rows": 200,
}


def relation(*args: LiveRelationArg, name: str = "documents") -> LiveRelation:
    return LiveRelation(
        name=name, subtype="log_document", action="external.search", args=args,
    )


def bounded_args() -> tuple[LiveRelationArg, ...]:
    return (
        LiveRelationArg(
            name="query", type_name="string", required=True,
            pattern=r"[A-Za-z0-9 ]{1,120}", max_length=120,
        ),
        LiveRelationArg(
            name="limit", type_name="integer", default=10,
            minimum=1, maximum=200,
        ),
    )


def binding(config: dict | None = None, **kwargs) -> LiveBinding:
    body = dict(GOOD_CONFIG if config is None else config)
    return LiveBinding(
        relation=kwargs.pop("relation", "documents"),
        kind=kwargs.pop("kind", "http"),
        config=body,
        **kwargs,
    )


def with_config(**overrides) -> LiveBinding:
    config = dict(GOOD_CONFIG)
    for key, value in overrides.items():
        if value is _REMOVE:
            config.pop(key, None)
        else:
            config[key] = value
    return binding(config)


class _Remove:
    pass


_REMOVE = _Remove()


def parse(bound: LiveBinding, rel: LiveRelation | None = None):
    return parse_http_binding(
        bound.relation, bound, rel or relation(*bounded_args()),
    )


# ------------------------------------------------------ the passing shape

def test_a_well_formed_binding_parses():
    config = parse(binding())
    assert config.url == "http://127.0.0.1:9200/logs-*/_search"
    assert config.rows_path == ("hits", "hits")
    assert config.max_rows == 200
    # The default, not a silent absence.
    assert config.max_response_bytes == 8 * 1024 * 1024
    assert config.auth_header is None


def test_the_parsed_config_carries_no_secret_anywhere_in_its_repr():
    """No field of the parsed config can hold a credential.

    The secret lives in a closure; this row pins that the object the
    dispatcher can reach does not have a field for one, so a future field
    named `token` reds it rather than shipping.
    """
    import os

    os.environ["OKG_TEST_HTTP_TOKEN"] = "s3cret-value-not-in-any-repr"
    try:
        bound = validate_live_binding_configs(
            {"documents": binding(
                dict(GOOD_CONFIG, auth_header="Authorization"),
                secret_refs=("env:OKG_TEST_HTTP_TOKEN",),
            )},
            {"documents": relation(*bounded_args())},
            runners={},
        )
        runner = bound["documents"]
        assert "s3cret-value-not-in-any-repr" not in repr(runner)
        assert "s3cret-value-not-in-any-repr" not in repr(
            parse(binding(
                dict(GOOD_CONFIG, auth_header="Authorization"),
                secret_refs=("env:OKG_TEST_HTTP_TOKEN",),
            )),
        )
    finally:
        os.environ.pop("OKG_TEST_HTTP_TOKEN", None)


# ------------------------------------------------------------- the slots

@pytest.mark.parametrize(
    "body, fragment",
    [
        # A slot as a KEY. An argument that could be a key could change the
        # SHAPE of the request, not just the question it asks.
        pytest.param({'{"$arg": "query"}': 1}, "KEY position", id="key"),
        # Sibling keys are a second reading of the same object creeping in.
        pytest.param(
            {"q": {"$arg": "query", "default": "x"}},
            "sibling key", id="siblings",
        ),
        # The slot value must be a plain argument name.
        pytest.param(
            {"q": {"$arg": {"name": "query"}}},
            "not an argument name", id="nested-name",
        ),
        pytest.param({"q": {"$arg": ""}}, "not an argument name", id="empty"),
        # A slot spelled inside a string is a TEMPLATE, and there is none.
        pytest.param(
            {"q": "prefix {\"$arg\": \"query\"}"},
            "no string templating", id="in-string",
        ),
    ],
)
def test_a_malformed_or_misplaced_slot_is_refused(body, fragment):
    with pytest.raises(LiveAdapterConfigInvalid) as caught:
        parse(with_config(body=body))
    assert fragment in str(caught.value)
    # The refusal names the relation and the field, so an operator knows
    # which of several bindings to open.
    assert caught.value.data["relation"] == "documents"
    assert caught.value.data["field"].startswith("body")


def test_a_slot_naming_an_undeclared_argument_is_refused():
    with pytest.raises(LiveAdapterConfigInvalid) as caught:
        parse(with_config(body={"q": {"$arg": "nonesuch"}}))
    assert "nonesuch" in str(caught.value)
    assert "does not declare" in str(caught.value)


def test_a_declared_argument_no_slot_uses_is_refused():
    """`inspect` must not advertise a parameter that changes nothing.

    The other direction from the row above, and not symmetry for its own
    sake: an unused argument is a knob an agent discovers is dead only by
    getting the same answer for every value.
    """
    with pytest.raises(LiveAdapterConfigInvalid) as caught:
        parse(with_config(body={"size": {"$arg": "limit"}}))
    assert "'query'" in str(caught.value) or "query" in str(caught.value)
    assert "advertise" in str(caught.value)


def test_substitution_is_single_pass():
    """An argument whose VALUE is a slot stays a string.

    If the body were rescanned after substitution, an agent could smuggle a
    slot in through an argument value and reach a second argument -- or an
    unbounded one. The walk never descends into a substituted value.
    """
    from okg.substrate.external_backed.http_adapter import _substitute

    config = parse(binding())
    rendered = _substitute(config.body, {"limit": 5, "query": '{"$arg": "x"}'})
    assert rendered["query"]["match"]["message"] == '{"$arg": "x"}'
    assert rendered["size"] == 5


def test_substitution_does_not_mutate_the_parsed_body():
    """A second call sees the same template as the first."""
    from okg.substrate.external_backed.http_adapter import _substitute

    config = parse(binding())
    _substitute(config.body, {"limit": 1, "query": "a"})
    again = _substitute(config.body, {"limit": 2, "query": "b"})
    assert again["size"] == 2
    assert config.body["size"] == {"$arg": "limit"}


# ------------------------------------------------------- the config shape

def test_an_unknown_config_key_is_refused_not_ignored():
    """A silently dropped key is how an operator believes a limit is set."""
    with pytest.raises(LiveAdapterConfigInvalid) as caught:
        parse(with_config(max_bytes=10))
    assert "max_bytes" in str(caught.value)
    assert "REFUSED rather than ignored" in str(caught.value)


def test_every_known_field_is_actually_accepted():
    """The allowlist and the parser cannot drift apart.

    A field added to `HTTP_CONFIG_FIELDS` and never read would be accepted
    and ignored, which is the failure the row above exists to stop -- from
    the other side.
    """
    full = dict(
        GOOD_CONFIG, content_type="application/json",
        max_response_bytes=1024 * 1024, auth_header="Authorization",
        row_fields=["pid", "name"],
    )
    assert set(full) == set(HTTP_CONFIG_FIELDS)
    import os

    os.environ["OKG_TEST_HTTP_TOKEN"] = "value"
    try:
        validate_live_binding_configs(
            {"documents": binding(full, secret_refs=("env:OKG_TEST_HTTP_TOKEN",))},
            {"documents": relation(*bounded_args())},
            runners={},
        )
    finally:
        os.environ.pop("OKG_TEST_HTTP_TOKEN", None)


@pytest.mark.parametrize(
    "overrides, fragment",
    [
        pytest.param({"path": "logs/_search"}, "must start with", id="path-rel"),
        pytest.param({"path": "/a?b=1"}, "must not contain", id="path-query"),
        pytest.param({"path": "/a#b"}, "must not contain", id="path-frag"),
        pytest.param(
            {"path": '/a/{"$arg": "query"}'}, "may never reach the URL",
            id="path-slot",
        ),
        pytest.param({"method": "DELETE"}, "must be one of", id="method"),
        pytest.param({"rows_path": "Hits.Hits"}, "dotted path", id="rows-case"),
        pytest.param({"rows_path": ""}, "dotted path", id="rows-empty"),
        pytest.param({"max_rows": 0}, "between 1", id="rows-zero"),
        pytest.param({"max_rows": 10_001}, "between 1", id="rows-huge"),
        pytest.param({"max_rows": True}, "must be an integer", id="rows-bool"),
        pytest.param(
            {"max_response_bytes": 64}, "between 1024", id="bytes-small",
        ),
        pytest.param(
            {"max_response_bytes": 64 * 1024 * 1024}, "between 1024",
            id="bytes-huge",
        ),
        pytest.param(
            {"content_type": "text/html"}, "JSON media type", id="content-type",
        ),
        pytest.param(
            {"allow_non_global": "yes"}, "must be true or false", id="opt-in",
        ),
        pytest.param({"row_fields": []}, "non-empty list", id="fields-empty"),
        pytest.param({"row_fields": "pid"}, "non-empty list", id="fields-str"),
        pytest.param(
            {"row_fields": ["pid", "pid"]}, "distinct", id="fields-dup",
        ),
        pytest.param({"row_fields": ["Pid"]}, "lower-snake", id="fields-case"),
        pytest.param({"row_fields": [1]}, "lower-snake", id="fields-int"),
    ],
)
def test_a_field_outside_its_grammar_is_refused(overrides, fragment):
    with pytest.raises(LiveAdapterConfigInvalid) as caught:
        parse(with_config(**overrides))
    assert fragment in str(caught.value)
    assert caught.value.data["relation"] == "documents"


def test_a_body_on_a_get_is_refused():
    with pytest.raises(LiveAdapterConfigInvalid) as caught:
        parse(with_config(method="GET"), relation())
    assert "carries no body" in str(caught.value)


def test_a_missing_body_on_a_post_is_refused():
    with pytest.raises(LiveAdapterConfigInvalid) as caught:
        parse(with_config(body=_REMOVE), relation())
    assert "required on a `POST`" in str(caught.value)


def test_a_get_whose_relation_declares_any_argument_is_refused():
    """A GET has nowhere to put an argument in this version.

    Accepting it would publish a relation through `inspect` whose parameters
    do nothing: the agent would vary them and get the same answer, which is
    worse than a refusal because nothing tells it so.
    """
    with pytest.raises(LiveAdapterConfigInvalid) as caught:
        parse(
            with_config(method="GET", body=_REMOVE),
            relation(*bounded_args()),
        )
    assert "nowhere to put them" in str(caught.value)
    assert "do nothing" in str(caught.value)


def test_a_get_with_no_arguments_is_accepted():
    """The rule above is about arguments, not about GET."""
    config = parse(with_config(method="GET", body=_REMOVE), relation())
    assert config.method == "GET"
    assert config.body is None


# ------------------------------------------------------- the constraints

@pytest.mark.parametrize(
    "arg",
    [
        pytest.param(
            LiveRelationArg(name="query", type_name="string", required=True),
            id="string-unbounded",
        ),
        pytest.param(
            LiveRelationArg(
                name="query", type_name="string", required=True,
                pattern=r".{1,5}",
            ),
            id="string-pattern-without-maxlength",
        ),
        pytest.param(
            LiveRelationArg(
                name="query", type_name="string", required=True,
                max_length=5,
            ),
            id="string-maxlength-without-pattern",
        ),
    ],
)
def test_an_unconstrained_slotted_string_is_refused(arg):
    with pytest.raises(LiveAdapterArgUnconstrained) as caught:
        parse(
            with_config(body={"q": {"$arg": "query"}}),
            relation(arg),
        )
    assert caught.value.data == {
        "relation": "documents", "argument": "query", "type": "string",
    }
    # The message says what the rule does NOT do, in the same words as the
    # design and the docs page. An operator who believes the check is
    # stronger than it is writes a weaker pattern than they otherwise would.
    assert "ABSENT constraint, not a weak one" in str(caught.value)


@pytest.mark.parametrize(
    "arg",
    [
        pytest.param(
            LiveRelationArg(name="limit", type_name="integer", default=1),
            id="integer-unbounded",
        ),
        pytest.param(
            LiveRelationArg(
                name="limit", type_name="integer", default=1, minimum=1,
            ),
            id="integer-minimum-only",
        ),
        pytest.param(
            LiveRelationArg(
                name="limit", type_name="number", default=1.0, maximum=9.0,
            ),
            id="number-maximum-only",
        ),
    ],
)
def test_an_unconstrained_slotted_number_is_refused(arg):
    with pytest.raises(LiveAdapterArgUnconstrained) as caught:
        parse(with_config(body={"n": {"$arg": arg.name}}), relation(arg))
    assert caught.value.data["argument"] == arg.name


@pytest.mark.parametrize(
    "arg",
    [
        pytest.param(
            LiveRelationArg(
                name="a", type_name="string", enum=("x", "y"),
            ),
            id="string-enum",
        ),
        pytest.param(
            LiveRelationArg(
                name="a", type_name="string", pattern=r".{1,5}", max_length=5,
            ),
            id="string-pattern-and-maxlength",
        ),
        pytest.param(
            LiveRelationArg(
                name="a", type_name="integer", default=1, minimum=1, maximum=9,
            ),
            id="integer-both-bounds",
        ),
        pytest.param(
            LiveRelationArg(name="a", type_name="integer", enum=(1, 2)),
            id="integer-enum",
        ),
        # The type IS the constraint: a boolean has two values.
        pytest.param(
            LiveRelationArg(name="a", type_name="boolean", default=False),
            id="boolean-needs-nothing",
        ),
    ],
)
def test_a_constrained_slotted_argument_is_accepted(arg):
    config = parse(with_config(body={"v": {"$arg": "a"}}), relation(arg))
    assert config.body == {"v": {"$arg": "a"}}


def test_an_unconstrained_argument_that_is_not_slotted_is_not_this_rule():
    """The rule is about what reaches the REQUEST, not what is declared.

    An argument no slot uses is already refused, by a different rule and
    with a different message. This row keeps the two from being confused:
    the unconstrained-argument refusal must not be what an operator sees
    when their real mistake was forgetting the slot.
    """
    with pytest.raises(LiveAdapterConfigInvalid) as caught:
        parse(
            with_config(body={"size": {"$arg": "limit"}}),
            relation(
                LiveRelationArg(name="loose", type_name="string"),
                LiveRelationArg(
                    name="limit", type_name="integer", default=1,
                    minimum=1, maximum=9,
                ),
            ),
        )
    assert not isinstance(caught.value, LiveAdapterArgUnconstrained)
    assert "advertise" in str(caught.value)


# ------------------------------------------------------------- the egress

@pytest.mark.parametrize(
    "overrides, fragment",
    [
        pytest.param(
            {"base_url": "ftp://127.0.0.1:9200"}, "scheme", id="scheme-ftp",
        ),
        pytest.param(
            {"base_url": "file:///etc/passwd"}, "scheme", id="scheme-file",
        ),
        # `base_url` carries scheme, host and port and NOTHING else, so it
        # and `path` cannot disagree about where the request goes.
        pytest.param(
            {"base_url": "http://127.0.0.1:9200/prefix"}, "url_has_path",
            id="base-url-path",
        ),
        pytest.param(
            {"base_url": "http://127.0.0.1:9200/?a=1"}, "url_has_path",
            id="base-url-query",
        ),
        pytest.param(
            {"base_url": "http://user:pw@127.0.0.1:9200"}, "userinfo",
            id="base-url-userinfo",
        ),
        # A hostname is checked before the socket connects, so DNS can answer
        # differently for the check and for the connection. IP literals only,
        # until the connection is pinned to the address that was checked.
        pytest.param(
            {"base_url": "http://opensearch.internal:9200",
             "allowed_hosts": ["opensearch.internal:9200"]},
            "host_not_literal", id="hostname",
        ),
    ],
)
def test_a_base_url_the_egress_rules_refuse_is_refused_at_load(
    overrides, fragment,
):
    with pytest.raises(LiveAdapterHostRefused) as caught:
        parse(with_config(**overrides))
    assert fragment in str(caught.value)
    assert caught.value.data["relation"] == "documents"


def test_a_refused_base_url_does_not_echo_its_own_credentials():
    """The refusal must not be the thing that prints the password."""
    with pytest.raises(LiveAdapterHostRefused) as caught:
        parse(with_config(base_url="http://user:hunter2@127.0.0.1:9200"))
    assert "hunter2" not in str(caught.value)
    assert "hunter2" not in repr(caught.value.data)


@pytest.mark.parametrize(
    "hosts",
    [
        pytest.param([], id="empty"),
        pytest.param("127.0.0.1:9200", id="not-a-list"),
    ],
)
def test_an_empty_or_malformed_allowlist_is_refused(hosts):
    with pytest.raises(LiveAdapterConfigInvalid) as caught:
        parse(with_config(allowed_hosts=hosts))
    assert "non-empty list" in str(caught.value)


@pytest.mark.parametrize(
    "entry",
    [
        pytest.param("opensearch.internal", id="hostname"),
        # `::1:9200` is BOTH a valid IPv6 literal and what an operator writes
        # meaning `::1` on port 9200. Either guess authorises a host nobody
        # asked for, so it is refused rather than resolved.
        pytest.param("::1:9200", id="ipv6-unbracketed"),
        pytest.param("::1", id="ipv6-bare-unbracketed"),
        pytest.param("127.0.0.1:not-a-port", id="bad-port"),
    ],
)
def test_an_allowlist_entry_that_is_not_an_ip_literal_is_refused(entry):
    with pytest.raises(LiveAdapterHostRefused) as caught:
        parse(with_config(allowed_hosts=[entry]))
    assert caught.value.data["host"] == entry


def test_a_base_url_outside_its_own_allowlist_is_refused():
    with pytest.raises(LiveAdapterHostRefused) as caught:
        parse(with_config(allowed_hosts=["127.0.0.2:9200"]))
    assert "host_not_allowed" in str(caught.value)


def test_a_port_the_allowlist_does_not_name_is_refused():
    """`127.0.0.1:9200` allowlisted is not `127.0.0.1:5432` allowlisted.

    An allowlist whose entries covered every port would authorise the admin
    interface next door to the one the operator meant.
    """
    with pytest.raises(LiveAdapterHostRefused) as caught:
        parse(with_config(base_url="http://127.0.0.1:5432"))
    assert "host_not_allowed" in str(caught.value)


def test_a_bare_allowlist_entry_means_the_default_port_only():
    """A bare `ip` is that address on the SCHEME's default port, not any."""
    with pytest.raises(LiveAdapterHostRefused):
        parse(with_config(allowed_hosts=["127.0.0.1"]))
    # ... and the default port itself does match.
    config = parse(with_config(
        base_url="http://127.0.0.1", allowed_hosts=["127.0.0.1"],
    ))
    assert config.port == 80


def test_a_non_global_address_without_the_opt_in_is_refused():
    with pytest.raises(LiveAdapterHostRefused) as caught:
        parse(with_config(allow_non_global=_REMOVE))
    assert "allow_non_global" in str(caught.value)
    assert caught.value.data["reason"] == "non_global"


@pytest.mark.parametrize(
    "address",
    [
        pytest.param("169.254.169.254", id="cloud-metadata"),
        pytest.param("10.0.0.1", id="private"),
        pytest.param("[::1]", id="ipv6-loopback"),
        # `is_global` is TRUE for every multicast address and for the NAT64
        # prefix, so a `not is_global` guard would have dialled these.
        pytest.param("239.255.255.250", id="multicast"),
        pytest.param("[64:ff9b::7f00:1]", id="nat64-embedding-loopback"),
    ],
)
def test_each_non_global_family_needs_the_opt_in(address):
    shown = address.strip("[]")
    entry = f"[{shown}]:9200" if ":" in shown else f"{shown}:9200"
    with pytest.raises(LiveAdapterHostRefused) as caught:
        parse(with_config(
            base_url=f"http://{address}:9200",
            allowed_hosts=[entry],
            allow_non_global=_REMOVE,
        ))
    assert caught.value.data["reason"] == "non_global"


def test_an_ipv4_mapped_ipv6_base_url_matches_its_ipv4_allowlist_entry():
    """`::ffff:127.0.0.1` IS 127.0.0.1; that is the address the socket reaches."""
    config = parse(with_config(base_url="http://[::ffff:127.0.0.1]:9200"))
    assert str(config.address) == "127.0.0.1"


# ------------------------------------------------------------ the secrets

def bind_with_secret(refs, **overrides):
    config = dict(GOOD_CONFIG)
    config.setdefault("auth_header", "Authorization")
    config.update({k: v for k, v in overrides.items() if v is not _REMOVE})
    for key, value in overrides.items():
        if value is _REMOVE:
            config.pop(key, None)
    return binding(config, secret_refs=tuple(refs))


def validate(bound):
    return validate_live_binding_configs(
        {"documents": bound},
        {"documents": relation(*bounded_args())},
        runners={},
    )


@pytest.mark.parametrize(
    "ref, error",
    [
        # `env:` is the ONLY scheme. A second scheme would be a second place
        # a credential can live, and therefore a second way to leak one.
        pytest.param("file:/run/secrets/token", LiveAdapterConfigInvalid, id="file"),
        pytest.param("vault:kv/token", LiveAdapterConfigInvalid, id="vault"),
        pytest.param("OPENSEARCH_TOKEN", LiveAdapterConfigInvalid, id="bare-name"),
        pytest.param("env:", LiveAdapterConfigInvalid, id="env-empty"),
        # The existing inline-credential heuristic, reused.
        pytest.param("TOKEN=hunter2", LiveAdapterConfigInvalid, id="inline-kv"),
    ],
)
def test_a_reference_that_is_not_env_name_is_refused(ref, error):
    with pytest.raises(error) as caught:
        validate(bind_with_secret([ref]))
    # WRAPPED, not raised straight from `handle`: raised there it names
    # `access.secret_refs`, a field a live binding does not have, and sends
    # the operator looking in the wrong block.
    assert "live_bindings.documents" in str(caught.value)
    assert "access.secret_refs" not in str(caught.value)
    assert "hunter2" not in str(caught.value)


def test_more_than_one_reference_is_refused():
    """One header, one value, one reference."""
    with pytest.raises(LiveAdapterConfigInvalid) as caught:
        validate(bind_with_secret(["env:A", "env:B"]))
    assert "exactly one reference" in str(caught.value)


def test_a_missing_environment_variable_fails_the_load_naming_the_reference():
    import os

    os.environ.pop("OKG_TEST_ABSENT_TOKEN", None)
    with pytest.raises(LiveAdapterSecretMissing) as caught:
        validate(bind_with_secret(["env:OKG_TEST_ABSENT_TOKEN"]))
    assert caught.value.data == {
        "relation": "documents", "secret_ref": "env:OKG_TEST_ABSENT_TOKEN",
    }


def test_an_auth_header_without_secret_refs_is_refused():
    with pytest.raises(LiveAdapterConfigInvalid) as caught:
        parse(with_config(auth_header="Authorization"))
    assert "no value to send" in str(caught.value)


def test_secret_refs_without_an_auth_header_is_refused():
    """A resolved credential with nowhere to go is one held for no reason."""
    with pytest.raises(LiveAdapterConfigInvalid) as caught:
        parse(binding(dict(GOOD_CONFIG), secret_refs=("env:ANY",)))
    assert "nowhere to go" in str(caught.value)


@pytest.mark.parametrize(
    "value",
    [
        pytest.param("Bearer hunter2", id="literal-credential"),
        pytest.param("Authorization: Bearer x", id="header-line"),
        pytest.param(42, id="not-a-string"),
    ],
)
def test_a_literal_credential_in_auth_header_is_refused(value):
    """`auth_header` carries a header NAME, never a value."""
    with pytest.raises(LiveAdapterConfigInvalid) as caught:
        parse(with_config(auth_header=value))
    assert "bare header NAME" in str(caught.value)
    assert "hunter2" not in str(caught.value)


@pytest.mark.parametrize(
    "name", ["Host", "Content-Length", "Content-Type", "Transfer-Encoding"],
)
def test_a_transport_owned_header_may_not_be_set(name):
    with pytest.raises(LiveAdapterConfigInvalid) as caught:
        parse(with_config(auth_header=name))
    assert "owns that header" in str(caught.value)


# ------------------------------------------------------- the load hook

def test_the_hook_returns_one_runner_per_http_binding():
    import os

    os.environ["OKG_TEST_HTTP_TOKEN"] = "value"
    try:
        bound = validate(bind_with_secret(["env:OKG_TEST_HTTP_TOKEN"]))
    finally:
        os.environ.pop("OKG_TEST_HTTP_TOKEN", None)
    assert sorted(bound) == ["documents"]
    assert callable(bound["documents"])


def test_a_caller_supplied_http_runner_is_left_alone():
    """`live_adapters` is a TEST SEAM and overrides the built-in.

    A test that passes its own fake `http` runner must not be made to
    satisfy this parser: its binding's config is whatever its fake needs.
    """
    bound = validate_live_binding_configs(
        {"documents": binding({"anything": "at all"})},
        {"documents": relation(*bounded_args())},
        runners={"http": lambda **kw: []},
    )
    assert bound == {}


def test_a_binding_of_another_kind_is_not_parsed_here():
    """`sql`, `command` and `python` have no runner and are not this hook's.

    They are still refused, by name, in
    `assert_live_bindings_are_declared` -- which is a different check with a
    different message, and this row keeps the two from merging.
    """
    bound = validate_live_binding_configs(
        {"documents": binding({"dsn": "x"}, kind="sql")},
        {"documents": relation(*bounded_args())},
        runners={},
    )
    assert bound == {}


# ---------------------------------------------------------------- the docs

def test_every_config_field_the_parser_accepts_is_documented():
    """A field added later without documenting it reds this gate.

    The operator writes this block by hand, from the reference page. A field
    the parser accepts and the page never mentions is a field nobody knows
    to set -- or worse, one whose default nobody knows either.
    """
    import pathlib

    page = pathlib.Path(__file__).resolve()
    for _ in range(4):
        page = page.parent
    text = (page / "docs" / "reference" / "live-http-bindings.md").read_text(
        encoding="utf-8",
    )
    missing = sorted(
        field for field in HTTP_CONFIG_FIELDS if f"`{field}`" not in text
    )
    assert not missing, f"undocumented config field(s): {missing}"


def test_the_docs_state_what_the_constraint_rule_does_not_do():
    """The warning is load-bearing and must not be edited away.

    An operator who believes the check is stronger than it is writes a
    weaker pattern than they otherwise would, so the page says what the rule
    misses in the same words the refusal does.
    """
    import pathlib

    page = pathlib.Path(__file__).resolve()
    for _ in range(4):
        page = page.parent
    text = (page / "docs" / "reference" / "live-http-bindings.md").read_text(
        encoding="utf-8",
    )
    assert "absent constraint, not a weak one" in text.lower()
    # And the anchoring hazard, which is the concrete way a constraint is
    # weak without looking it.
    assert "unanchored" in text.lower()


# --------------------------------------------------- the review's findings

@pytest.mark.parametrize(
    "entry",
    [
        # Token SHAPES an operator actually pastes. The first four contain
        # no `=`, so `handle._validate_secret_refs` passes them through and
        # they reach the `env:` check directly.
        pytest.param("sk-live-REALTOKEN-abc123", id="stripe-shaped"),
        pytest.param("ghp_REALTOKENabcdef0123456789", id="github-shaped"),
        pytest.param(fake_credential("xoxb-", "REALTOKEN-0123456789"), id="slack-shaped"),
        pytest.param(
            "eyJhbGciOiJIUzI1NiJ9.REALPAYLOAD.REALSIG", id="jwt-shaped",
        ),
        # These two DO contain `=`, so they take the wrapped branch, where
        # `handle`'s own message quotes the entry -- which is the whole
        # credential. That is the only path where the context suppression
        # below has anything to suppress, so without these two the
        # traceback assertion passes vacuously.
        pytest.param("TOKEN=sk-live-REALTOKEN-abc123", id="kv-shaped"),
        pytest.param(
            "eyJhbGciOiJIUzI1NiJ9.REALPAYLOAD.REALSIG==", id="jwt-padded",
        ),
    ],
)
def test_a_pasted_secret_VALUE_is_refused_without_being_echoed(entry):
    """B1. The refusal must not be the thing that publishes the credential.

    The usual cause of this refusal is an operator pasting the secret VALUE
    where its reference name belongs. The earlier version quoted the entry
    into both the message and `data["reason"]`, so a failed build wrote a
    live token into every log line and error envelope that carried it.

    THE TRACEBACK IS PART OF THE SURFACE, and scrubbing the message alone
    was not enough. On the wrapped branch the `ExternalHandleError` -- whose
    own message IS the pasted entry -- stayed on `__context__`, so Python
    printed the chained exception in full and the credential reappeared in
    any formatted traceback. Which is precisely what a failed build writes
    to the log.
    """
    import json
    import traceback

    with pytest.raises(LiveAdapterConfigInvalid) as caught:
        validate(bind_with_secret([entry]))

    exc = caught.value
    assert entry not in str(exc)
    assert entry not in json.dumps(exc.data)
    assert entry not in (exc.fix_hint or "")

    # The chain is suppressed wherever there is one to suppress. Asserting
    # `__context__ is None` would be wrong: Python still RECORDS the
    # context, it just does not print it once `__suppress_context__` is set.
    if exc.__context__ is not None:
        assert exc.__suppress_context__, (
            "the wrapped error survives as __context__ and will be printed"
        )

    rendered = "".join(
        traceback.format_exception(type(exc), exc, exc.__traceback__),
    )
    assert entry not in rendered
    # The distinctive fragment too, in case a path quotes only part of it.
    assert "REALTOKEN" not in rendered
    assert "REALPAYLOAD" not in rendered

    # It still says what is wrong and where.
    assert "env:NAME" in str(exc)


def test_a_secret_refs_refusal_names_the_field_that_exists():
    """B1. `secret_refs` is a SIBLING of `config`, not a key inside it.

    The shared `_refuse` helper writes `live_bindings.<rel>.config.<field>`,
    which sent an operator to a block that does not have the key.
    """
    with pytest.raises(LiveAdapterConfigInvalid) as caught:
        validate(bind_with_secret(["not-a-reference"]))
    assert "`live_bindings.documents.secret_refs`" in str(caught.value)
    assert "config.secret_refs" not in str(caught.value)
    assert caught.value.data["field"] == "secret_refs"


@pytest.mark.parametrize(
    "media, ok",
    [
        pytest.param("application/json", True, id="exact"),
        pytest.param("text/json", True, id="exact-text"),
        pytest.param("application/json; charset=utf-8", True, id="charset"),
        pytest.param("application/vnd.api+json", True, id="structured-suffix"),
        # N3: `endswith` over every known name accepted these. An upstream
        # chooses its own Content-Type, so this is a type it can pick to get
        # a non-JSON body read as JSON.
        pytest.param("evil/application/json", False, id="prefixed-type"),
        pytest.param("x-text/json", False, id="prefixed-text-json"),
        pytest.param("application/jsonx", False, id="suffixed"),
        pytest.param("text/html", False, id="html"),
        pytest.param("", False, id="empty"),
    ],
)
def test_only_a_real_json_media_type_is_accepted(media, ok):
    from okg.substrate.external_backed.http_adapter import _is_json_media

    assert _is_json_media(media) is ok


def test_an_upstream_excerpt_keeps_its_reason_and_loses_its_credentials():
    """N1. `redact_url` answered `<unparsable url>` for the whole excerpt.

    It parses its argument AS a URL, so an excerpt that merely CONTAINS one
    was replaced wholesale by that marker -- throwing away the upstream's
    reason, which is the only thing an excerpt is for.
    """
    from okg.substrate.external_backed.http_adapter import _excerpt_of

    raw = b"index_not_found_exception reaching http://adm:hunter2@10.0.0.9/x"
    text = _excerpt_of(raw)
    assert "index_not_found_exception" in text
    assert "hunter2" not in text
    assert "<unparsable url>" not in text


# ------------------------------------- the document root as rows_path (F1)

def test_the_root_spelling_is_accepted_and_means_the_body_itself():
    """A real API returns a top-level JSON array over GET.

    Glances 4.5.6 does it for per-CPU and process-list endpoints. Before
    this there was no way to bind one: every dotted path requires an object
    at each step, so such a response was refused at load AND at runtime.
    """
    from okg.substrate.external_backed.http_adapter import ROWS_PATH_ROOT

    config = parse(with_config(rows_path=ROWS_PATH_ROOT))
    # The empty tuple IS the root: the extraction loop does not run.
    assert config.rows_path == ()


@pytest.mark.parametrize(
    "spelling",
    [
        # `""` is what an absent or mistyped key looks like. A config
        # language must not spell "the whole document" the same way it
        # spells "I forgot to fill this in".
        pytest.param("", id="empty-string"),
        pytest.param("$", id="jsonpath-root"),
        pytest.param("..", id="double-dot"),
        pytest.param(".a", id="leading-dot"),
        pytest.param("a.", id="trailing-dot"),
        pytest.param("./.", id="dot-slash-dot"),
        pytest.param(". ", id="dot-with-space"),
    ],
)
def test_only_one_root_spelling_is_accepted(spelling):
    with pytest.raises(LiveAdapterConfigInvalid) as caught:
        parse(with_config(rows_path=spelling))
    assert "rows_path" in str(caught.value)


def test_the_root_path_returns_a_top_level_array():
    from okg.substrate.external_backed.http_adapter import (
        ROWS_PATH_ROOT,
        _extract_rows,
    )

    config = parse(with_config(rows_path=ROWS_PATH_ROOT))
    rows = [{"cpu": 0, "idle": 91.2}, {"cpu": 1, "idle": 88.0}]
    assert _extract_rows(rows, config) == rows


def test_the_root_path_refuses_a_top_level_object():
    """An endpoint that switches from an array to an object is an ERROR.

    Never an empty result: "the shape changed" must not read as "no rows".
    """
    from okg.substrate.external_backed.http_adapter import (
        ROWS_PATH_ROOT,
        _extract_rows,
    )

    config = parse(with_config(rows_path=ROWS_PATH_ROOT))
    with pytest.raises(Exception) as caught:
        _extract_rows({"cpu": 0}, config)
    assert caught.value.failure == "response_unusable"
    # The refusal names the root by the spelling the operator wrote.
    assert ROWS_PATH_ROOT in str(caught.value.excerpt)


# ------------------------------ the node cap is charged on the slice (F3)

def wide_body(noise: int = 12_000) -> dict:
    """A body shaped like a Glances `/api/4/all`: wide, with a small list.

    Over the node cap as a whole; its `processlist` is twenty short rows.
    """
    return {
        "processlist": [{"pid": i, "name": f"p{i}"} for i in range(20)],
        "noise": {f"k{i}": {"a": i, "b": [i, i, i]} for i in range(noise)},
    }


def test_the_fixture_body_really_is_over_the_node_cap():
    """Pin the premise, so the row below cannot pass vacuously.

    If a future cap change put this body under the limit, the test that
    matters would still be green while proving nothing.
    """
    from okg.substrate.external_backed.live_dispatch import (
        _assert_result_is_shapeable,
    )

    with pytest.raises(Exception):
        _assert_result_is_shapeable(wide_body())


def test_a_small_slice_of_an_over_cap_body_is_shapeable():
    """F3. The cap is charged on what the adapter RETURNS.

    Charging the whole parsed document made `max_rows` useless against a
    wide endpoint: a Glances `/api/4/all` response measured 64,612 nodes,
    so even `max_rows: 12` failed -- and whether it failed depended on how
    busy the machine was that second, which is the worst kind of
    intermittent.
    """
    from okg.substrate.external_backed.live_dispatch import (
        _assert_result_is_shapeable,
    )

    rows = wide_body()["processlist"][:12]
    _assert_result_is_shapeable(rows)


def test_a_slice_that_is_itself_over_the_cap_is_still_refused():
    """Moving the check must not remove it."""
    from okg.substrate.external_backed.live_dispatch import (
        MAX_RESULT_NODES,
        _assert_result_is_shapeable,
    )

    rows = [{"a": i, "b": [i, i, i]} for i in range(MAX_RESULT_NODES)]
    with pytest.raises(Exception):
        _assert_result_is_shapeable(rows)


def test_row_fields_parse_into_a_tuple():
    assert parse(with_config(row_fields=["pid", "name"])).row_fields == (
        "pid", "name",
    )
    assert parse(with_config()).row_fields is None
