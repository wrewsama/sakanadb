"""Unit tests for the SQL-like command parser."""
import pytest
from tickhouse.parser import (
    CreateCommand,
    InsertCommand,
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
    _SQL = "INSERT INTO tbl VALUES ('2026-01-01', 'AAPL', 100.0, 110.5, 99.0, 105.0, 1000000)"

    def test_basic(self):
        cmd = parse(self._SQL)
        assert isinstance(cmd, InsertCommand)
        assert cmd.table == "tbl"
        assert cmd.date == "2026-01-01"
        assert cmd.symbol == "AAPL"
        assert cmd.open == pytest.approx(100.0)
        assert cmd.high == pytest.approx(110.5)
        assert cmd.low == pytest.approx(99.0)
        assert cmd.close == pytest.approx(105.0)
        assert cmd.volume == 1_000_000

    def test_unquoted_values(self):
        cmd = parse("INSERT INTO tbl VALUES (2026-01-01, AAPL, 1.0, 2.0, 0.5, 1.5, 500)")
        assert isinstance(cmd, InsertCommand)
        assert cmd.symbol == "AAPL"

    def test_wrong_value_count(self):
        with pytest.raises(ValueError, match="7 values"):
            parse("INSERT INTO tbl VALUES ('2026-01-01', 'AAPL')")

    def test_invalid_float(self):
        with pytest.raises(ValueError, match="Invalid numeric"):
            parse("INSERT INTO tbl VALUES ('2026-01-01', 'AAPL', notanumber, 2.0, 0.5, 1.5, 500)")


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
