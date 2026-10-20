from __future__ import annotations

from unittest import mock

import pytest

from okg.substrate.errors import SubstrateError
from okg.substrate.library.sources.academic_researcher.doe_osti import DoeOstiSource
from okg.substrate.library.sources.academic_researcher.group_webpage_scrape import (
    GroupWebpageScrapeSource,
)
from okg.substrate.library.sources.academic_researcher.lab_webpage_talks import (
    LabWebpageTalksSource,
)
from okg.substrate.library.sources.academic_researcher.mit_course_catalog import (
    MitCourseCatalogSource,
)
from okg.substrate.library.sources.academic_researcher.nsf_award_search import (
    NsfAwardSearchSource,
)


_PUBLIC_DNS = [(2, 1, 6, "", ("93.184.216.34", 443))]


@pytest.mark.parametrize(
    ("source_name", "factory", "patch_target"),
    [
        (
            "group_webpage_scrape",
            lambda: GroupWebpageScrapeSource(
                group_webpage="https://lab.example/people",
            ),
            "okg.substrate.library.sources.academic_researcher.group_webpage_scrape"
            ".open_validated_url",
        ),
        (
            "lab_webpage_talks",
            lambda: LabWebpageTalksSource(
                talks_url="https://lab.example/talks",
            ),
            "okg.substrate.library.sources.academic_researcher.lab_webpage_talks"
            ".open_validated_url",
        ),
        (
            "mit_course_catalog",
            lambda: MitCourseCatalogSource(pi_name="Ada Lovelace"),
            "okg.substrate.library.sources.academic_researcher.mit_course_catalog"
            ".open_validated_url",
        ),
        (
            "doe_osti",
            lambda: DoeOstiSource(orcid="0000-0002-1825-0097"),
            "okg.substrate.library.sources.academic_researcher.doe_osti"
            ".open_validated_url",
        ),
        (
            "nsf_award_search",
            lambda: NsfAwardSearchSource(pi_name="Ada Lovelace"),
            "okg.substrate.library.sources.academic_researcher.nsf_award_search"
            ".open_validated_url",
        ),
    ],
)
def test_finding_12_academic_researcher_adapters_fail_loud(
    source_name: str,
    factory,
    patch_target: str,
) -> None:
    with (
        mock.patch(patch_target, side_effect=OSError("upstream unavailable")),
        mock.patch(
            "okg.substrate.library.sources._url_safety.socket.getaddrinfo",
            return_value=_PUBLIC_DNS,
        ),
    ):
        with pytest.raises(SubstrateError) as exc_info:
            factory().run(run_id="run-1")

    err = exc_info.value
    assert err.data["source"] == source_name
    assert err.__cause__ is not None
    assert "upstream unavailable" in str(err.__cause__)
