"""The E1b, E6/E14, E7 and E13 corpora of design.md (okg#3112 step 2) as
data for the admission and probe tests. Not a test module.

E13 (number values and filters) is regenerated exactly as
``pact/changes/lane-h-3112-step2/scripts/e13_num_superset.py`` builds it:
same seed, same order, 698 values and 150 filters.
"""

from __future__ import annotations

import math
import random
import sys
from decimal import Decimal, localcontext
from typing import Any

#: E1b: crafted JSON values (as JSON text) against every declared type.
E1B_VALUES = [
    "42", "42.0", "-0", "1e400", '"42"', "true", "false", '"true"', "null",
    '"abc"', '""', '"  spaced  "', '"Ünïcode ✓"', '{"a":1}', "[1,2]",
    '"2024-01-02"', '"2024-01-02T03:04:05Z"', '"2024-01-02 03:04:05+02"',
    '"2024-01-02 03:04:05"', '"01/02/2024"', '"2024-13-45"', '"not a date"',
    '"infinity"', '"-infinity"', '"epoch"', '"now"', '"today"', '" Tomorrow "',
    '"2024-01-02 10:00 Foo/Bar"', '"294277-01-01"', '"20240102"', "20240102",
]

#: E14: E6's 46 strings plus 23 forms the verification named (69).
E14_TS = [
    "2024-01-02", "2024-01-02T03:04:05Z", "2024-01-02 03:04:05+02", "2024-01-02 03:04:05",
    "01/02/2024", "2024-13-45", "not a date", "infinity", "-infinity", "epoch", "now", "today",
    " Tomorrow ", "2024-01-02 10:00 Foo/Bar", "294277-01-01", "20240102", "today 10:00",
    "10:00 today", "tomorrow 00:00", "yesterday 23:59", "today allballs", "now()", "\tnow",
    "now\n", "2024-01-02 10:00 IST", "2024-01-02 10:00 EST", "2024-01-02 10:00 Asia/Kolkata",
    "2024-01-02T10:00:00 UTC", "2024-01-02T10:00:00.123456+05:30", "2024-01-02T10:00:00+0530",
    "2024-01-02T10:00Z", "2024-01-02 10:00:00-05", "2024-02-29", "2016-12-31T23:59:60Z",
    "2024-01-02T10:00:00.1234567Z", "0001-01-01", "2023-02-29", "2024-01-02T25:00Z",
    "2024-01-02t10:00Z", "2024-1-2", "２０２４-01-02", " 2024-01-02",
    "2024-01-02 ", "J2451187", "allballs", "2024-01-02 allballs",
    "2024-01-02T03:04:05.1234567Z", "2024-01-02T03:04:05.12345678Z",
    "2024-01-02T03:04:05.123456789Z", "2024-01-02T03:04:05.123456789+05:30",
    "2024-01-02T03:04:05.123456789-07:00", "2024-01-02T03:04:05.000000001Z",
    "2024-01-02T23:59:59.9999995Z", "2024-01-02T23:59:59.9999994Z",
    "2024-01-02T03:04:05.1234567890Z", "2024-01-02t03:04:05Z", "2024-01-02T03:04:05z",
    "2024-01-02t03:04:05.123456789z", "2024-01-02t03:04:05+05:30", "2024-01-02 03:04:05z",
    "2024-01-02T10", "2024-01-02T1000", "2024-01-02T10:00:00,5Z", "2024-W01-2",
    "2024-01-02T10:00:00+05:30:15", "2024-01-02X10:00:00", "2024-01-02T10:00:00.5+05",
    "2024-01-02T10:00:00Z ", "2024002",
]

#: okg.attr_ts_v4 of every E14 value it admits, in UTC (evidence/e14_ts_v4.json).
E14_V4_ADMITS = {
    "2024-01-02": "2024-01-02T00:00:00.000000Z",
    "2024-01-02T03:04:05Z": "2024-01-02T03:04:05.000000Z",
    "2024-01-02 03:04:05+02": "2024-01-02T01:04:05.000000Z",
    "2024-01-02 03:04:05": "2024-01-02T03:04:05.000000Z",
    "2024-01-02T10:00:00.123456+05:30": "2024-01-02T04:30:00.123456Z",
    "2024-01-02T10:00:00+0530": "2024-01-02T04:30:00.000000Z",
    "2024-01-02T10:00Z": "2024-01-02T10:00:00.000000Z",
    "2024-01-02 10:00:00-05": "2024-01-02T15:00:00.000000Z",
    "2024-02-29": "2024-02-29T00:00:00.000000Z",
    "2016-12-31T23:59:60Z": "2017-01-01T00:00:00.000000Z",
    "2024-01-02T10:00:00.1234567Z": "2024-01-02T10:00:00.123457Z",
    "0001-01-01": "0001-01-01T00:00:00.000000Z",
    "2024-01-02t10:00Z": "2024-01-02T10:00:00.000000Z",
    "2024-01-02T03:04:05.1234567Z": "2024-01-02T03:04:05.123457Z",
    "2024-01-02T03:04:05.12345678Z": "2024-01-02T03:04:05.123457Z",
    "2024-01-02T03:04:05.123456789Z": "2024-01-02T03:04:05.123457Z",
    "2024-01-02T03:04:05.123456789+05:30": "2024-01-01T21:34:05.123457Z",
    "2024-01-02T03:04:05.123456789-07:00": "2024-01-02T10:04:05.123457Z",
    "2024-01-02T03:04:05.000000001Z": "2024-01-02T03:04:05.000000Z",
    "2024-01-02T23:59:59.9999995Z": "2024-01-03T00:00:00.000000Z",
    "2024-01-02T23:59:59.9999994Z": "2024-01-02T23:59:59.999999Z",
    "2024-01-02t03:04:05Z": "2024-01-02T03:04:05.000000Z",
    "2024-01-02T03:04:05z": "2024-01-02T03:04:05.000000Z",
    "2024-01-02t03:04:05.123456789z": "2024-01-02T03:04:05.123457Z",
    "2024-01-02t03:04:05+05:30": "2024-01-01T21:34:05.000000Z",
    "2024-01-02 03:04:05z": "2024-01-02T03:04:05.000000Z",
    "2024-01-02T10:00:00.5+05": "2024-01-02T05:00:00.500000Z",
}

#: The 11 bound forms Python's fromisoformat accepts and v4 refuses: never
#: pushed down (design section 2.2 rule 2).
PYTHON_ONLY_TS_BOUNDS = [
    "20240102", " 2024-01-02", "2024-01-02 ", "2024-01-02T03:04:05.1234567890Z",
    "2024-01-02T10", "2024-01-02T1000", "2024-01-02T10:00:00,5Z", "2024-W01-2",
    "2024-01-02T10:00:00+05:30:15", "2024-01-02X10:00:00", "2024-01-02T10:00:00Z ",
]

#: The four session settings of E6/E14.
TS_SESSIONS = [
    ["SET timezone = 'UTC'", "SET datestyle = 'ISO, MDY'", "SET timezone_abbreviations = 'Default'"],
    ["SET timezone = 'America/New_York'"],
    ["SET timezone = 'UTC'", "SET timezone_abbreviations = 'India'"],
    ["SET timezone_abbreviations = 'Default'", "SET datestyle = 'SQL, DMY'"],
]


def _exact_decimal(x: float) -> Decimal:
    with localcontext() as c:
        c.prec = 1200
        return Decimal(x)


def _midpoint(a: float, b: float) -> str:
    with localcontext() as c:
        c.prec = 1200
        return str((_exact_decimal(a) + _exact_decimal(b)) / 2)


def e13() -> tuple[list[str], list[dict[str, Any]]]:
    """(values as JSON number text, filters) of E13."""
    rnd = random.Random(13)
    with localcontext() as ctx:
        ctx.prec = 400
        top = Decimal(sys.float_info.max)
        ulp_top = Decimal(2) ** 971
        overflow_at = top + ulp_top / 2
        half_min_sub = Decimal(2) ** -1075
    values: list[str] = [
        "0", "-0", "0.0", "1", "-1", "0.1", "0.10000000000000001",
        "0.1000000000000000055511151231257827", "0.30000000000000004", "0.3",
        "0.29999999999999998", "0.30000000000000001", "1e300", "1.0000000000000001e300",
        "1e301", "-1e301", "1e308", "1.7976931348623157e308", str(top), str(overflow_at),
        str(overflow_at - Decimal("1e280")), "1.7976931348623158e308", "1.8e308", "1e309",
        "1e400", "-1e400", "1e-400", "-1e-400", "5e-324", "4.9406564584124654e-324",
        str(half_min_sub), "3e-324", "2.5e-324", "1e-320", "9007199254740992",
        "9007199254740993", "9007199254740993.0", "123.456789012345678901234567890123",
        "0." + "1" * 40, "1" + "0" * 400, "-" + "9" * 350,
    ]
    doubles: list[float] = [
        0.0, -0.0, 0.1, 0.3, 0.30000000000000004, 1.0, 1e300, 1e301, 1e308,
        sys.float_info.max, 5e-324, 1e-310, 2.2250738585072014e-308, 9007199254740992.0,
        123.456,
    ]
    for _ in range(120):
        kind = rnd.random()
        if kind < 0.4:
            x = rnd.uniform(-1000, 1000)
        elif kind < 0.7:
            x = math.ldexp(rnd.random(), rnd.randint(-1074, 1023))
        else:
            x = rnd.choice([-1, 1]) * 10.0 ** rnd.uniform(-320, 308)
        doubles.append(x)
    for x in doubles:
        up, down = math.nextafter(x, math.inf), math.nextafter(x, -math.inf)
        values += [repr(x), repr(up), repr(down)]
        if math.isfinite(up):
            values.append(_midpoint(x, up))
        if math.isfinite(down):
            values.append(_midpoint(x, down))
    values = [v for v in dict.fromkeys(values) if v not in ("inf", "-inf", "nan")]

    float_bounds = list(dict.fromkeys(doubles[:30] + [math.inf, -math.inf]))
    int_bounds = [0, 1, -1, 9007199254740993, 2 * 10 ** 308, 10 ** 400, -(10 ** 400)]
    predicates: list[dict[str, Any]] = []
    for x in float_bounds + int_bounds:
        if not (isinstance(x, float) and not math.isfinite(x)):
            predicates.append({"op": "eq", "value": x})
        predicates.append({"op": "lt", "value": x})
        predicates.append({"op": "gt", "value": x})
    for i in range(0, len(float_bounds) - 1, 2):
        a, b = sorted(float_bounds[i:i + 2])
        if math.isfinite(a) and math.isfinite(b):
            predicates.append({"op": "in", "value": [a, b]})
        predicates.append({"op": "between", "value": [a, b]})
    predicates += [
        {"op": "between", "value": [0, 0.1]},
        {"op": "between", "value": [0.1, 0.3]},
        {"op": "between", "value": [0.0, 0.30000000000000004]},
        {"op": "between", "value": [1e300, sys.float_info.max]},
        {"op": "between", "value": [0, 10 ** 400]},
        {"op": "between", "value": [-math.inf, math.inf]},
        {"op": "between", "value": [5e-324, 1]},
        {"op": "in", "value": [0.1, 0.30000000000000004, 9007199254740993]},
        {"op": "in", "value": [0, -0.0, 1e300]},
    ]
    return values, predicates


def md5_chain(seed: str, n: int) -> str:
    """E7's incompressible text: an md5 chain, ``n`` characters."""
    import hashlib

    out = []
    g = 1
    while sum(len(p) for p in out) < n:
        out.append(hashlib.md5(f"{seed}{g}".encode()).hexdigest())
        g += 1
    return "".join(out)[:n]


def text_corpus() -> list[str]:
    """Text values of 1 to 512 characters, and longer ones in groups that
    share a 600-character prefix."""
    values = ["a", "A", "b", "B", "a-b", "ab", "a b", "é", "e", "z", "Z", "_", "-", " ",
              "日本", "😀", "", "abc", "abd", "abc\x7f", "short-000001", "short-000002"]
    values += ["x" * n for n in (1, 2, 255, 511, 512)]
    values += [("a" * 511) + "b", ("a" * 512) + "c", "a" * 513]
    for group in range(4):
        prefix = md5_chain(f"grp{group}", 600)
        for member in range(5):
            values.append(prefix + md5_chain(f"row{group}-{member}", 200 + 50 * member))
    return values
