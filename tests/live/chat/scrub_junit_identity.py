"""Remove the machine name from a junit report before committing it.

pytest stamps every ``<testsuite>`` with ``hostname``, the laptop's own name.
It is not a credential and nothing depends on it, but it is the one personal
string that would otherwise travel into this change's committed evidence, and
pytest offers no option to leave it out -- ``junit_logging`` and the property
options change what is recorded inside a test case, not this attribute. So it
is removed by this step, which runs after pytest and is named in the artifact
description so a reader knows the file was post-processed and why.

Nothing else is touched: the counts, the timing, and every test case stay
exactly as pytest wrote them, which is what the evidence is for.

    python -m tests.live.chat.scrub_junit_identity <report.xml>
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from xml.etree import ElementTree

ATTRIBUTE = "hostname"


def scrub_junit_identity(path: Path) -> list[str]:
    """Drop ``hostname`` from every element that carries it.

    Returns the values removed, so a caller can show that the step did
    something rather than assume it did.
    """
    tree = ElementTree.parse(path)
    removed = []
    for element in tree.getroot().iter():
        value = element.attrib.pop(ATTRIBUTE, None)
        if value is not None:
            removed.append(value)
    tree.write(path, encoding="utf-8", xml_declaration=True)
    return removed


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("report", type=Path)
    args = parser.parse_args()
    removed = scrub_junit_identity(args.report)
    print(f"removed {len(removed)} {ATTRIBUTE} value(s) from {args.report.name}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
