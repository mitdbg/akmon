"""The chat stack pins the PATCHED OpenWebUI image, by digest, and only it.

req.ooc.chat-adoption. Upstream Open WebUI v0.11.4's native MCP client prefers
a result's ``content`` and drops ``structuredContent``. Every operator answer
this change makes structured-only would arrive EMPTY through it, so adopting
the patched image is what keeps the output contract observable from a chat
client at all.

These tests pull nothing: the image is not published (build it from
``ops/openwebui/Dockerfile``), so no registry is reached.
What is provable without a pull is the whole adoption decision: which
reference is pinned, that it is pinned by DIGEST rather than a tag, that it is
the default an operator gets without saying anything, and that the upstream
reference now refuses with a message naming the patched image and the reason.
The remaining half -- a structured-only result reaching the model THROUGH the
running patched container -- needs a real pull and is proved by an
operator-side live receipt, disclosed as such in the PACT manifest.
"""

from __future__ import annotations

import pytest

from okg.substrate.chat.instance import (
    PINNED_IMAGE_REF,
    PINNED_VERSION,
    TESTED_IMAGES,
    UPSTREAM_IMAGE_REF,
    ChatImageNotTested,
    resolve_instance_image,
)
from okg.substrate.chat.document_loader import VENDOR_CONTRACT
from okg.substrate.deployments.chat_block import (
    EXAMPLE_PINNED_IMAGE,
    PINNED_IMAGE_REFERENCE,
    ChatManifestError,
    _parse_image,
)
from okg.substrate.chat.instance import _repository_of

DEPLOYMENT = "adoption-probe"

#: The v0.11.4-based digest published by #2930 from dev 46ef2e92a7
#: (container-images run 36531821115, job 109286977916; tags ``dev`` and
#: ``sha-46ef2e92a7...``). Written out here rather than imported so that a
#: silent edit to the constant fails a test instead of passing one.
PUBLISHED_DIGEST = (
    "sha256:2561170716571aaf7e41417decd7e23b43c390a00395c957b362a17e80557c11"
)
COMPAT_REF = f"your-registry.invalid/okg-openwebui-compat@{PUBLISHED_DIGEST}"

#: The v0.11.0-based image this replaced (#2048, dev 6623262a24). Retired
#: from the tested set: its measurements are not the ones okg now checks.
RETIRED_REF = (
    "your-registry.invalid/okg-openwebui-compat@"
    "sha256:e0b1f475fd051febc37a62c4a4b681299035cb4578afcfa2370f77e8daa5290b"
)


class _Block:
    """The smallest thing ``resolve_instance_image`` reads: an image or None."""

    def __init__(self, image: str | None = None) -> None:
        self.image = image
        self.raw = {} if image is None else {"image": image}


def test_the_pin_is_the_published_patched_digest() -> None:
    assert PINNED_IMAGE_REF == COMPAT_REF


def test_the_tested_set_is_exactly_the_patched_image() -> None:
    """Replacement, not addition: upstream is no longer admissible."""

    assert dict(TESTED_IMAGES) == {COMPAT_REF: PINNED_VERSION}
    assert UPSTREAM_IMAGE_REF not in TESTED_IMAGES


def test_the_pin_is_a_digest_not_a_tag() -> None:
    """A tag would let the bytes move under the pin. The rule must agree."""

    assert PINNED_IMAGE_REFERENCE.match(PINNED_IMAGE_REF)
    assert "@sha256:" in PINNED_IMAGE_REF
    assert not PINNED_IMAGE_REF.endswith(":dev")


def test_the_patched_image_is_what_an_operator_gets_by_default() -> None:
    plan = resolve_instance_image(DEPLOYMENT, _Block())
    assert plan.image == COMPAT_REF
    assert plan.source == "default_pin"
    assert plan.version == PINNED_VERSION


def test_the_upstream_reference_refuses_and_says_why() -> None:
    """The refusal has to be actionable: WHICH image, and WHY not this one."""

    with pytest.raises(ChatImageNotTested) as caught:
        resolve_instance_image(DEPLOYMENT, _Block(image=UPSTREAM_IMAGE_REF))
    error = caught.value
    assert error.data["reason"] == "image_not_tested"
    assert error.data["upstream_ref"] is True
    assert "okg-openwebui-compat" in error.message
    assert "structuredContent" in error.message
    assert "okg-openwebui-compat" in error.fix_hint
    assert "structured tool results" in error.fix_hint
    # And it still names the set, so the operator can read the answer off it.
    assert error.data["tested"] == [COMPAT_REF]


def test_the_retired_0110_image_refuses_as_not_tested() -> None:
    """Fail-before: the v0.11.0-based image was the pin, so `up` started it.
    It now refuses by name: the census, the chat-sessions schema pin and the
    loader contract describe v0.11.4, not what that container runs."""
    assert PINNED_VERSION == "0.11.4"
    with pytest.raises(ChatImageNotTested) as caught:
        resolve_instance_image(DEPLOYMENT, _Block(image=RETIRED_REF))
    assert caught.value.data["reason"] == "image_not_tested"
    assert caught.value.data["image"] == RETIRED_REF
    assert caught.value.data["tested"] == [COMPAT_REF]


def test_an_unrelated_untested_image_refuses_without_the_upstream_wording() -> None:
    """The explanation is about upstream specifically, not decoration.

    A refusal that blamed structured-content loss for every unknown image
    would be telling an operator something the program did not observe.
    """

    with pytest.raises(ChatImageNotTested) as caught:
        resolve_instance_image(
            DEPLOYMENT, _Block(image="docker.io/library/nginx:v1.2.3"),
        )
    error = caught.value
    assert error.data["upstream_ref"] is False
    assert "structuredContent" not in error.message
    assert "okg-openwebui-compat" in error.fix_hint


def test_the_document_loader_contract_names_the_same_image() -> None:
    """Two pins that disagree would make one of the two receipts a fiction."""

    assert VENDOR_CONTRACT.image_ref == COMPAT_REF
    assert VENDOR_CONTRACT.openwebui_version == PINNED_VERSION


def test_the_manifest_layers_example_agrees_with_the_pin() -> None:
    """The literal is duplicated across a layer boundary; it may not drift."""

    assert EXAMPLE_PINNED_IMAGE == PINNED_IMAGE_REF


@pytest.mark.parametrize("value", ["", "your-registry.invalid/okg-openwebui-compat:dev"])
def test_no_manifest_hint_sends_an_operator_to_upstream(value: str) -> None:
    """A hint naming upstream would be worse than no hint at all.

    Upstream satisfies the pinned-reference rule, so an operator who followed
    such a hint would write a manifest that PARSES and then refuses a layer
    later as chat_image_not_tested -- the confusing failure this asserts away.
    """

    with pytest.raises(ChatManifestError) as caught:
        _parse_image(value, manifest_path=None)
    hint = caught.value.fix_hint
    assert "open-webui/open-webui" not in hint
    assert EXAMPLE_PINNED_IMAGE in hint


@pytest.mark.parametrize(
    ("reference", "repository"),
    [
        ("ghcr.io/open-webui/open-webui:v0.11.0", "ghcr.io/open-webui/open-webui"),
        (
            "registry.example.com:5000/open-webui/open-webui:v0.11.0",
            "registry.example.com:5000/open-webui/open-webui",
        ),
        (f"your-registry.invalid/okg-openwebui-compat@{PUBLISHED_DIGEST}",
         "your-registry.invalid/okg-openwebui-compat"),
    ],
)
def test_a_registry_port_is_not_mistaken_for_a_tag(
    reference: str, repository: str,
) -> None:
    """The tag separator is the LAST colon with no slash after it."""

    assert _repository_of(reference) == repository
