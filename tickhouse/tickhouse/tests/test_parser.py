"""Unit tests for the SQL-like command parser."""
import pytest
from tickhouse.parser import (
    CreateCommand,
    InsertCommand,
    InsertRow,
    QueryCommand,
    parse,
)


# ---------------------------------------------------------------------------
# CREATE TABLE
# ---------------------------------------------------------------------------

class TestCreate:
    def test_basic(self):
        cmd = parse("CREATE TABLE my_table")
        assert isinstance(cmd, CreateCommand)
        assert cmd.table == "my_table"

    def test_case_insensitive(self):
        cmd = parse("create table foo")
        assert isinstance(cmd, CreateCommand)
        assert cmd.table == "foo"

    def test_extra_whitespace(self):
        cmd = parse("  CREATE   TABLE   stocks  ")
        assert isinstance(cmd, CreateCommand)
        assert cmd.table == "stocks"


# ---------------------------------------------------------------------------
# INSERT INTO
# ---------------------------------------------------------------------------

class TestInsert:
    """Single-row inserts are the degenerate case of a bulk insert."""

    _SQL = "INSERT INTO tbl VALUES ('2026-01-01', 'AAPL', 100.0, 110.5, 99.0, 105.0, 1000000)"

    def test_basic_returns_insert_command(self):
        cmd = parse(self._SQL)
        assert isinstance(cmd, InsertCommand)
        assert cmd.table == "tbl"
        assert len(cmd.rows) == 1

    def test_single_row_fields(self):
        cmd = parse(self._SQL)
        row = cmd.rows[0]
        assert isinstance(row, InsertRow)
        assert row.date == "2026-01-01"
        assert row.symbol == "AAPL"
        assert row.open == pytest.approx(100.0)
        assert row.high == pytest.approx(110.5)
        assert row.low == pytest.approx(99.0)
        assert row.close == pytest.approx(105.0)
        assert row.volume == 1_000_000

    def test_unquoted_values(self):
        cmd = parse("INSERT INTO tbl VALUES (2026-01-01, AAPL, 1.0, 2.0, 0.5, 1.5, 500)")
        assert isinstance(cmd, InsertCommand)
        assert cmd.rows[0].symbol == "AAPL"

    def test_wrong_value_count(self):
        with pytest.raises(ValueError, match="7 values"):
            parse("INSERT INTO tbl VALUES ('2026-01-01', 'AAPL')")

    def test_invalid_float(self):
        with pytest.raises(ValueError, match="invalid numeric"):
            parse("INSERT INTO tbl VALUES ('2026-01-01', 'AAPL', notanumber, 2.0, 0.5, 1.5, 500)")


class TestBulkInsert:
    _SQL = (
        "INSERT INTO bars VALUES "
        "(2024-01-02, AAPL, 185.20, 186.10, 184.90, 185.85, 50123400), "
        "(2024-01-03, AAPL, 185.21, 186.11, 184.91, 185.86, 50123410)"
    )

    def test_returns_insert_command(self):
        cmd = parse(self._SQL)
        assert isinstance(cmd, InsertCommand)
        assert cmd.table == "bars"

    def test_two_rows(self):
        cmd = parse(self._SQL)
        assert len(cmd.rows) == 2

    def test_row_values(self):
        cmd = parse(self._SQL)
        r0, r1 = cmd.rows

        assert r0.date == "2024-01-02"
        assert r0.symbol == "AAPL"
        assert r0.open == pytest.approx(185.20)
        assert r0.high == pytest.approx(186.10)
        assert r0.low == pytest.approx(184.90)
        assert r0.close == pytest.approx(185.85)
        assert r0.volume == 50123400

        assert r1.date == "2024-01-03"
        assert r1.volume == 50123410

    def test_quoted_values(self):
        sql = (
            "INSERT INTO bars VALUES "
            "('2024-01-02', 'AAPL', 185.20, 186.10, 184.90, 185.85, 100), "
            "('2024-01-03', 'MSFT', 300.0, 305.0, 299.0, 302.0, 200)"
        )
        cmd = parse(sql)
        assert len(cmd.rows) == 2
        assert cmd.rows[0].symbol == "AAPL"
        assert cmd.rows[1].symbol == "MSFT"

    def test_single_row_is_still_list(self):
        sql = "INSERT INTO bars VALUES (2024-01-02, AAPL, 1.0, 2.0, 0.5, 1.5, 100)"
        cmd = parse(sql)
        assert len(cmd.rows) == 1

    def test_wrong_value_count_in_second_row(self):
        sql = (
            "INSERT INTO bars VALUES "
            "(2024-01-02, AAPL, 1.0, 2.0, 0.5, 1.5, 100), "
            "(2024-01-03, AAPL, 1.0, 2.0)"
        )
        with pytest.raises(ValueError, match="Row 2"):
            parse(sql)

    def test_invalid_float_in_bulk_row(self):
        sql = (
            "INSERT INTO bars VALUES "
            "(2024-01-02, AAPL, 1.0, 2.0, 0.5, 1.5, 100), "
            "(2024-01-03, AAPL, bad, 2.0, 0.5, 1.5, 200)"
        )
        with pytest.raises(ValueError, match="invalid numeric"):
            parse(sql)


# ---------------------------------------------------------------------------
# SELECT
# ---------------------------------------------------------------------------

class TestSelect:
    def test_star(self):
        cmd = parse("SELECT * FROM tbl WHERE symbol = 'AAPL'")
        assert isinstance(cmd, QueryCommand)
        assert cmd.columns == ["*"]
        assert cmd.symbol == "AAPL"
        assert cmd.date_gte is None
        assert cmd.date_lte is None

    def test_specific_columns(self):
        cmd = parse("SELECT date, close FROM tbl WHERE symbol = 'TSLA'")
        assert cmd.columns == ["date", "close"]
        assert cmd.symbol == "TSLA"

    def test_date_gte(self):
        cmd = parse("SELECT * FROM tbl WHERE symbol = 'X' AND date >= '2026-01-01'")
        assert cmd.date_gte == "2026-01-01"
        assert cmd.date_lte is None

    def test_date_lte(self):
        cmd = parse("SELECT * FROM tbl WHERE symbol = 'X' AND date <= '2026-12-31'")
        assert cmd.date_lte == "2026-12-31"

    def test_date_range(self):
        cmd = parse(
            "SELECT * FROM tbl WHERE symbol = 'X' "
            "AND date >= '2026-01-01' AND date <= '2026-06-30'"
        )
        assert cmd.date_gte == "2026-01-01"
        assert cmd.date_lte == "2026-06-30"

    def test_missing_symbol(self):
        with pytest.raises(ValueError, match="symbol"):
            parse("SELECT * FROM tbl WHERE date >= '2026-01-01'")


# ---------------------------------------------------------------------------
# Unknown command
# ---------------------------------------------------------------------------

def test_unknown_command():
    with pytest.raises(ValueError, match="Unrecognised"):
        parse("DROP TABLE foo")
