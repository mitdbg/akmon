"""What a failed source did to the other sources of the same ingest (#3927).

When one source fails during `okg ingest`, the failure line names that
source. A reader then asks the next question: what happened to the rest?
Three things can happen to another source in the same run:

- it finished and its records went into a new graph version (the version
  records that the failed source is missing from it);
- it finished but the run's last publish did not produce a graph version, so
  its records are staged only;
- it did not run, because it depends on the failed source (it names that
  source as its `predecessor_source` in the source registry).

`other_sources_after_failure` reads the run's own completion record and its
last publish and names all three. Only the run's own last publish (label
`commit`) counts: the earlier publishes that resume a half-finished run
(`staged-source-redrive` and the recovery promotion) say nothing about the
sources of this run. The note is built only by the in-process `okg ingest`
path, where the report covers every selected source. Pure functions of the
report, so a test can pin the text without a database.
"""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from okg.substrate.deployment_release import _plural
from okg.substrate.publish_outcome import NO_CHANGE, PUBLISHED, classify_publish

REPORT_KEY = "other_sources"
#: Label of the publish that ends a run.
FINAL_PUBLISH_LABEL = "commit"


def _names(items: Sequence[Mapping[str, Any]] | None) -> list[str]:
    return [str(item["source"]) for item in items or [] if item.get("source")]


def _listed(names: Sequence[str]) -> str:
    return ", ".join(names)


def _final_publish_made_a_version(
    publishes: Sequence[Mapping[str, Any]] | None,
) -> bool:
    """Whether this run's last publish published (or kept) a graph version."""
    finals = [
        item for item in publishes or []
        if isinstance(item, Mapping) and item.get("label") == FINAL_PUBLISH_LABEL
    ]
    if not finals:
        return False
    return classify_publish(finals[-1]).kind in {PUBLISHED, NO_CHANGE}


def text_lines(report: Any) -> list[str]:
    """The lines `okg ingest` prints after its report when a source failed."""
    others = report.get(REPORT_KEY) if isinstance(report, Mapping) else None
    if not isinstance(others, Mapping):
        return []
    return [str(others["message"]), str(others["next_step"])]


def other_sources_after_failure(
    *,
    deployment: str,
    completeness: Mapping[str, Any] | None,
    publishes: Sequence[Mapping[str, Any]] | None,
    inline_flag: bool = False,
) -> dict[str, Any] | None:
    """The other sources' fate in a run where a source failed, or None.

    None when nothing failed, or when the failed source was the only one
    selected (no other source to report on). The `message` is the text a
    person reads; the lists are for `--json`. `inline_flag` is True when the
    command ran with `--inline`: the suggested command then carries it too,
    because a deployment whose runtime is on would otherwise queue the rerun,
    and a queued run ignores `--exclude`.
    """
    if not isinstance(completeness, Mapping):
        return None
    failed = _names(completeness.get("failed_sources"))
    if not failed:
        return None
    blocked = _names(completeness.get("dependency_blocked_sources"))
    completed = [str(name) for name in completeness.get("completed_sources") or []]
    if not completed and not blocked:
        return None
    made_a_version = _final_publish_made_a_version(publishes)
    published = completed if made_a_version else []
    staged = [] if made_a_version else completed

    failed_text = _listed(failed)
    sentences: list[str] = []
    if published:
        sentences.append(
            f"{_plural(len(published), 'other source')} "
            f"{'was' if len(published) == 1 else 'were'} published without "
            f"{failed_text}: {_listed(published)}."
        )
    if staged:
        sentences.append(
            f"{_plural(len(staged), 'other source')} "
            f"{'was' if len(staged) == 1 else 'were'} ingested but not "
            f"published: {_listed(staged)}."
        )
    if blocked:
        sentences.append(
            f"{_plural(len(blocked), 'source')} that "
            f"{'depends' if len(blocked) == 1 else 'depend'} on "
            f"{failed_text} did not run: {_listed(blocked)}."
        )
    inline = " --inline" if inline_flag else ""
    rerun = f"okg ingest{inline} --deployment {deployment}"
    next_step = (
        f"To go on, fix {failed_text} and run `{rerun}` again, or leave it "
        f"out for one run with `{rerun} --exclude {','.join(failed)}` (a "
        "later run includes it again)."
    )
    return {
        "failed": failed,
        "published_without_failed": published,
        "ingested_not_published": staged,
        "not_run_depends_on_failed": blocked,
        "message": " ".join(sentences),
        "next_step": next_step,
    }
