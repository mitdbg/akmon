"""Names near one that names no record (issue 4053).

When an agent asks for a record by a name or id that no record bears, the
read tools say so and may list records whose names are close: an agent that
typed `ReportJune2013` sees `ReportMay2013` and `ReportApril2013`, and can
answer "absent" or retry with a name that exists.

Two pieces, shared by search (record names) and inspect (node ids):

* `name_stem` cuts the last one or two letter or digit runs off a name, so
  the reads can fetch names that start the same way through an index (the
  text trigram index for search, the node-id B-tree for inspect). A stem too
  short to be selective is not used: no read runs.
* `rank_near` orders the names those reads found by how close they are to
  the one asked for.

Nothing here reads the database.
"""
from __future__ import annotations

import difflib
import re
from typing import Iterable, Optional

#: Near names shown in one reply.
NEAR_NAMES_SHOWN = 5
#: A stem shorter than this, or than `_STEM_SHARE` of the name, is not used.
_STEM_MIN = 5
_STEM_SHARE = 0.4
#: The letter and digit runs of a name: an acronym before a capitalized word
#: (`ABC` in `ABCAt`), a capitalized or lower-case word, an acronym, a number.
_RUN = re.compile(r"[A-Z]+(?=[A-Z][a-z])|[A-Z]?[a-z]+|[A-Z]+|[0-9]+")
_TRAILING_SEPARATORS = "-_.:/ "


def name_stem(name: str) -> Optional[str]:
    """`name` without its last two letter or digit runs, or without its last
    one when two leave too little, trailing separators dropped; None when
    neither leaves at least `_STEM_MIN` characters and `_STEM_SHARE` of the
    name. `ReleaseNotesAgentJune2013` -> `ReleaseNotesAgent`,
    `WorkshopAtLISBON2024` -> `WorkshopAt`, `pkg_release_14_0_9` ->
    `pkg_release_14`."""
    runs = [match.start() for match in _RUN.finditer(name)]
    floor = max(_STEM_MIN, int(len(name) * _STEM_SHARE + 0.999))
    for dropped in (2, 1):
        if len(runs) <= dropped:
            continue
        stem = name[:runs[-dropped]].rstrip(_TRAILING_SEPARATORS)
        if len(stem) >= floor:
            return stem
    return None


def rank_near(
    wanted: str, found: Iterable[tuple[str, str]], *, shown: int = NEAR_NAMES_SHOWN,
) -> list[str]:
    """The ids of `found` (pairs of id and name) whose names are closest to
    `wanted`, closest first, at most `shown`. Closeness is
    `difflib.SequenceMatcher.ratio` on the case-folded names; ties go to the
    shorter name, then to the id in byte order."""
    target = wanted.casefold()
    scored = []
    seen: set[str] = set()
    for node_id, name in found:
        if node_id in seen or name == wanted:
            continue
        seen.add(node_id)
        ratio = difflib.SequenceMatcher(None, target, name.casefold()).ratio()
        scored.append((-ratio, len(name), node_id.encode("utf-8"), node_id))
    scored.sort()
    return [node_id for *_key, node_id in scored[:shown]]


def name_runs(name: str) -> list[str]:
    """The letter and digit runs of `name`, in order (`ReleaseNotes2013` ->
    `Release`, `Notes`, `2013`; `Workshop-At-Lisbon` -> `Workshop`, `At`,
    `Lisbon`)."""
    return _RUN.findall(name)
