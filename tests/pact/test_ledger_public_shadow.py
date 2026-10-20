"""The ledger guard refuses unless the `okg` copy contains the `public` one.

Issue #1476, ask 2a. `ensure_ledger_schema` refused whenever any PACT ledger
relation still existed in `public`, whatever either copy held. On the sprint
machine that meant every `okg pact decide` refused with
`ledger_shadowed_by_public`, so an operator approval was recorded nowhere.

Containment is asked EXACTLY, with EXCEPT, and not inferred from row counts.
Counts were tried first and are not merely weaker, they are wrong: measured on
that deployment after a few writes had landed, `okg` held 37 coordination
events to `public`'s 32 and a newer newest-row, and all 32 of `public`'s rows
were still absent from it. The two copies had been written independently, so
"more rows, more recent" said contained while the truth was disjoint.

These cases drive the guard through a fake cursor: the subject is the decision
it makes about a set of measurements. What a fake cannot prove - that the SQL
parses, that a privilege error is caught, that SAVEPOINT leaves the transaction
usable - is covered by the two real-database cases in
`tests/substrate/db/test_namespace_qualification.py`.
"""

from __future__ import annotations

import pytest

from okg.substrate.errors import SubstrateError
from okg.substrate.pact import decisions as led

_DEFAULT_COLUMNS = ("id", "change_id", "verdict")


class FakeCursor:
    """The slice of a cursor `ensure_ledger_schema` actually uses.

    `tables` maps `"<schema>.<relation>"` to a row count; a missing key means
    the relation does not exist. `orphans` gives, per relation, how many
    `public` rows are absent from the `okg` copy. `columns` overrides the
    column list of a copy, so a pair with nothing in common can be modelled.
    `fail_on` raises the given exception when that relation is counted.
    """

    def __init__(
        self,
        tables: dict[str, int],
        *,
        orphans: dict[str, int] | None = None,
        columns: dict[str, tuple[str, ...]] | None = None,
        relkinds: dict[str, str] | None = None,
        fail_on: dict[str, Exception] | None = None,
        objects_present: bool = True,
        grants_present: bool = True,
    ) -> None:
        self.tables = tables
        self.orphans = orphans or {}
        self.columns = columns or {}
        self.relkinds = relkinds or {}
        self.fail_on = dict(fail_on or {})
        self.objects_present = objects_present
        self.grants_present = grants_present
        self.executed: list[str] = []
        self._result: object = None
        self._rows: list[tuple] = []

    def _columns_for(self, name: str) -> tuple[str, ...]:
        return self.columns.get(name, _DEFAULT_COLUMNS)

    def execute(self, sql: str, params: tuple | None = None) -> None:
        self.executed.append(sql)
        text = " ".join(sql.split())
        if text.startswith(("SAVEPOINT", "RELEASE", "ROLLBACK TO SAVEPOINT")):
            self._result = None
            return
        if text.startswith("SELECT c.relkind"):
            assert params is not None
            name = f"{params[0]}.{params[1]}"
            self._result = (
                (self.relkinds.get(name, "r"),) if name in self.tables else None
            )
            return
        if text.startswith("SELECT column_name FROM information_schema.columns"):
            assert params is not None
            name = f"{params[0]}.{params[1]}"
            self._rows = [(column,) for column in self._columns_for(name)]
            return
        if text.startswith("SELECT count(*) FROM (SELECT"):
            relation = text.split('public."')[1].split('"')[0]
            self._result = (self.orphans.get(relation, 0),)
            return
        if text.startswith("SELECT count(*) FROM"):
            name = text.removeprefix("SELECT count(*) FROM ").replace('"', "").strip()
            if name in self.fail_on:
                raise self.fail_on.pop(name)
            self._result = (self.tables[name],)
            return
        if "public_ledger_present" in sql or "objects_present" in sql:
            in_public = sorted(
                name.split(".", 1)[1]
                for name in self.tables
                if name.startswith("public.")
            )
            self._result = (self.objects_present, self.grants_present, in_public)
            return
        self._result = None

    def fetchone(self):
        return self._result

    def fetchall(self):
        return self._rows


def _state(**kwargs) -> led.ShadowState:
    return led.ShadowState(**kwargs)


class TestShadowVerdict:
    """The pure decision, over measurements alone."""

    def test_an_empty_public_copy_is_passed_over(self) -> None:
        verdict, detail = led._public_shadow_verdict(
            {"pact_decisions": _state(public_rows=0, okg_rows=2, orphan_rows=0)}
        )

        assert verdict == led.SHADOW_WARN
        assert "empty leftover table" in detail[0]

    def test_a_contained_public_copy_is_passed_over(self) -> None:
        verdict, detail = led._public_shadow_verdict(
            {"pact_decisions": _state(public_rows=2, okg_rows=5, orphan_rows=0)}
        )

        assert verdict == led.SHADOW_WARN
        assert "fully contained by" in detail[0]

    def test_orphan_rows_refuse(self) -> None:
        verdict, detail = led._public_shadow_verdict(
            {
                "pact_coordination_events": _state(
                    public_rows=32, okg_rows=24, orphan_rows=32
                )
            }
        )

        assert verdict == led.SHADOW_REFUSE
        assert "32 row(s) that are in no okg.pact_coordination_events row" in detail[0]

    def test_more_rows_in_okg_is_not_containment(self) -> None:
        """The measured trap, recorded as a test.

        `okg` holding more rows says nothing about whether it holds THESE
        rows. This exact shape - 37 against 32, all 32 orphaned - is what the
        live deployment looked like, and a count rule called it contained.
        """

        verdict, detail = led._public_shadow_verdict(
            {
                "pact_coordination_events": _state(
                    public_rows=32, okg_rows=37, orphan_rows=32
                )
            }
        )

        assert verdict == led.SHADOW_REFUSE
        assert "public 32, okg 37" in detail[0]

    def test_an_incomparable_pair_refuses(self) -> None:
        verdict, detail = led._public_shadow_verdict(
            {"pact_decisions": _state(public_rows=2, okg_rows=3, orphan_rows=None)}
        )

        assert verdict == led.SHADOW_REFUSE
        assert "no shared columns" in detail[0]

    def test_a_missing_okg_copy_refuses(self) -> None:
        verdict, detail = led._public_shadow_verdict(
            {"pact_decisions": _state(public_rows=7, okg_rows=-1)}
        )

        assert verdict == led.SHADOW_REFUSE
        assert "no okg.pact_decisions at all" in detail[0]

    def test_an_unreadable_copy_refuses_rather_than_reading_as_empty(self) -> None:
        verdict, detail = led._public_shadow_verdict(
            {"pact_decisions": _state(unreadable="InsufficientPrivilege: denied")}
        )

        assert verdict == led.SHADOW_REFUSE
        assert "could not be measured" in detail[0]

    def test_one_bad_relation_refuses_the_whole_ledger(self) -> None:
        verdict, detail = led._public_shadow_verdict(
            {
                "pact_decisions": _state(public_rows=0, okg_rows=2, orphan_rows=0),
                "pact_coordination_events": _state(
                    public_rows=32, okg_rows=37, orphan_rows=32
                ),
            }
        )

        assert verdict == led.SHADOW_REFUSE
        assert len(detail) == 1
        assert "pact_coordination_events" in detail[0]

    def test_the_measured_live_deployment_refuses(self) -> None:
        """Measured on `okg_workspace`, 2026-09-02, by EXCEPT.

        Recorded because it is the case this change was written for and the
        one that must NOT be waved through: the copies are disjoint and only
        an operator can reconcile them.
        """

        verdict, detail = led._public_shadow_verdict(
            {
                "pact_decisions": _state(public_rows=0, okg_rows=2, orphan_rows=0),
                "pact_intents": _state(public_rows=2, okg_rows=4, orphan_rows=2),
                "pact_coordination_events": _state(
                    public_rows=32, okg_rows=37, orphan_rows=32
                ),
            }
        )

        assert verdict == led.SHADOW_REFUSE
        assert len(detail) == 2


class TestEnsureLedgerSchema:
    def setup_method(self) -> None:
        led._WARNED_SHADOWS.clear()

    def test_a_contained_leftover_does_not_refuse(self) -> None:
        cursor = FakeCursor(
            {
                "public.pact_decisions": 0,
                "okg.pact_decisions": 2,
                "public.pact_intents": 1,
                "okg.pact_intents": 4,
            },
            orphans={"pact_intents": 0},
        )

        assert led.ensure_ledger_schema(cursor) is None

    def test_a_disjoint_leftover_refuses(self) -> None:
        cursor = FakeCursor(
            {
                "public.pact_coordination_events": 32,
                "okg.pact_coordination_events": 37,
            },
            orphans={"pact_coordination_events": 32},
        )

        with pytest.raises(SubstrateError) as excinfo:
            led.ensure_ledger_schema(cursor)

        error = excinfo.value
        assert error.data["cause"] == "ledger_shadowed_by_public"
        # The refusal carries the measurement, so an operator does not have to
        # go and take it before deciding anything.
        counts = error.data["relation_counts"]["pact_coordination_events"]
        assert counts["public_rows"] == 32
        assert counts["okg_rows"] == 37
        assert counts["orphan_rows"] == 32
        # And it must not send them to drop the copy holding the only rows.
        assert "Do NOT drop it first" in (error.fix_hint or "")
        assert "merge" in (error.fix_hint or "")
        assert "duplicate_table" in (error.fix_hint or "")

    def test_the_refusal_still_says_BOTH(self) -> None:
        """`test_namespace_qualification.py` keys on that word."""

        cursor = FakeCursor(
            {"public.pact_decisions": 7, "okg.pact_decisions": 0},
            orphans={"pact_decisions": 7},
        )

        with pytest.raises(SubstrateError) as excinfo:
            led.ensure_ledger_schema(cursor)

        assert "BOTH" in str(excinfo.value)

    def test_public_only_still_refuses_as_not_adopted(self) -> None:
        """No `okg` copy at all is the un-migrated case, and is untouched."""

        cursor = FakeCursor({"public.pact_decisions": 7}, objects_present=False)

        with pytest.raises(SubstrateError) as excinfo:
            led.ensure_ledger_schema(cursor)

        assert excinfo.value.data["cause"] == "ledger_not_adopted"

    def test_a_dropped_index_does_not_look_like_an_unadopted_ledger(self) -> None:
        """`objects_present` is False for a missing INDEX too.

        Leaning on it would send a fully adopted ledger down the "not adopted"
        path, recommending a migration that has already run.
        """

        cursor = FakeCursor(
            {"public.pact_decisions": 0, "okg.pact_decisions": 2},
            objects_present=False,
        )

        assert led.ensure_ledger_schema(cursor) == "bootstrap"

    def test_a_privilege_error_becomes_a_refusal_not_a_crash(self) -> None:
        import psycopg

        cursor = FakeCursor(
            {"public.pact_decisions": 7, "okg.pact_decisions": 9},
            fail_on={
                "public.pact_decisions": psycopg.errors.InsufficientPrivilege(
                    "permission denied for table pact_decisions"
                )
            },
        )

        with pytest.raises(SubstrateError) as excinfo:
            led.ensure_ledger_schema(cursor)

        assert excinfo.value.data["cause"] == "ledger_shadowed_by_public"
        assert "could not be measured" in "; ".join(excinfo.value.data["stranded"])
        # The transaction was left usable for the repairs below.
        assert any("ROLLBACK TO SAVEPOINT" in sql for sql in cursor.executed)

    def test_a_public_name_that_is_not_a_table_refuses(self) -> None:
        cursor = FakeCursor(
            {"public.pact_decisions": 0, "okg.pact_decisions": 2},
            relkinds={"public.pact_decisions": "i"},
        )

        with pytest.raises(SubstrateError) as excinfo:
            led.ensure_ledger_schema(cursor)

        assert "not a table" in "; ".join(excinfo.value.data["stranded"])

    def test_no_shared_columns_refuses(self) -> None:
        cursor = FakeCursor(
            {"public.pact_decisions": 3, "okg.pact_decisions": 9},
            columns={"public.pact_decisions": ("legacy_only",)},
        )

        with pytest.raises(SubstrateError) as excinfo:
            led.ensure_ledger_schema(cursor)

        assert "no shared columns" in "; ".join(excinfo.value.data["stranded"])

    def test_allowing_the_write_warns_with_both_row_counts(self, capsys) -> None:
        cursor = FakeCursor(
            {
                "public.pact_coordination_events": 3,
                "okg.pact_coordination_events": 24,
            },
            orphans={"pact_coordination_events": 0},
        )

        led.ensure_ledger_schema(cursor)

        message = capsys.readouterr().err
        assert "incomplete migration 0022" in message
        assert "3 row(s)" in message and "24 row(s)" in message
        assert "merge" in message

    def test_the_warning_does_not_repeat_as_row_counts_change(self, capsys) -> None:
        """It is keyed on relation NAMES.

        Keying it on the counts made the key change with every write, so the
        warning reprinted at each of the many ledger touches in one command.
        """

        cursor = FakeCursor(
            {"public.pact_intents": 1, "okg.pact_intents": 4},
            orphans={"pact_intents": 0},
        )
        led.ensure_ledger_schema(cursor)
        assert capsys.readouterr().err

        cursor.tables["okg.pact_intents"] = 5
        led.ensure_ledger_schema(cursor)

        assert capsys.readouterr().err == ""

    def test_a_clean_okg_only_ledger_is_untouched(self) -> None:
        cursor = FakeCursor({"okg.pact_decisions": 2})

        assert led.ensure_ledger_schema(cursor) is None
        assert not any("SAVEPOINT" in sql for sql in cursor.executed)
