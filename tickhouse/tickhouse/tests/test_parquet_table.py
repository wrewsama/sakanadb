"""Integration tests for ParquetTable storage."""
import pytest
from pathlib import Path
from tickhouse.storage.parquet_table import ParquetTable, ALL_COLUMNS


@pytest.fixture
def table(tmp_path: Path) -> ParquetTable:
    t = ParquetTable(name="test_tbl", data_dir=tmp_path)
    t.create()
    return t


class TestCreate:
    def test_creates_directory(self, tmp_path: Path):
        t = ParquetTable(name="stocks", data_dir=tmp_path)
        t.create()
        assert (tmp_path / "stocks").is_dir()

    def test_idempotent(self, tmp_path: Path):
        t = ParquetTable(name="stocks", data_dir=tmp_path)
        t.create()
        t.create()  # should not raise


class TestInsertQuery:
    def _insert(self, t: ParquetTable, date="2026-01-01", symbol="AAPL",
                open=100.0, high=110.0, low=99.0, close=105.0, volume=1_000_000):
        t.insert(date=date, symbol=symbol, open=open, high=high,
                 low=low, close=close, volume=volume)

    def test_roundtrip(self, table: ParquetTable):
        self._insert(table)
        rows = table.query(symbol="AAPL")
        assert len(rows) == 1
        row = rows[0]
        assert row["date"] == "2026-01-01"
        assert row["symbol"] == "AAPL"
        assert row["open"] == pytest.approx(100.0)
        assert row["volume"] == 1_000_000

    def test_filters_by_symbol(self, table: ParquetTable):
        self._insert(table, symbol="AAPL")
        self._insert(table, symbol="TSLA")
        rows = table.query(symbol="AAPL")
        assert all(r["symbol"] == "AAPL" for r in rows)
        assert len(rows) == 1

    def test_date_gte_filter(self, table: ParquetTable):
        self._insert(table, date="2026-01-01", symbol="AAPL")
        self._insert(table, date="2026-06-01", symbol="AAPL")
        rows = table.query(symbol="AAPL", date_gte="2026-03-01")
        assert len(rows) == 1
        assert rows[0]["date"] == "2026-06-01"

    def test_date_lte_filter(self, table: ParquetTable):
        self._insert(table, date="2026-01-01", symbol="AAPL")
        self._insert(table, date="2026-06-01", symbol="AAPL")
        rows = table.query(symbol="AAPL", date_lte="2026-03-01")
        assert len(rows) == 1
        assert rows[0]["date"] == "2026-01-01"

    def test_date_range_filter(self, table: ParquetTable):
        for m in range(1, 7):
            self._insert(table, date=f"2026-0{m}-01", symbol="AAPL")
        rows = table.query(symbol="AAPL", date_gte="2026-02-01", date_lte="2026-04-01")
        dates = {r["date"] for r in rows}
        assert dates == {"2026-02-01", "2026-03-01", "2026-04-01"}

    def test_column_projection(self, table: ParquetTable):
        self._insert(table)
        rows = table.query(symbol="AAPL", columns=["date", "close"])
        assert set(rows[0].keys()) == {"date", "close"}

    def test_star_returns_all_columns(self, table: ParquetTable):
        self._insert(table)
        rows = table.query(symbol="AAPL", columns=["*"])
        assert set(rows[0].keys()) == set(ALL_COLUMNS)

    def test_empty_result(self, table: ParquetTable):
        rows = table.query(symbol="NONEXISTENT")
        assert rows == []

    def test_query_before_create_raises(self, tmp_path: Path):
        t = ParquetTable(name="missing", data_dir=tmp_path)
        with pytest.raises(FileNotFoundError):
            t.query(symbol="X")

    def test_invalid_column_raises(self, table: ParquetTable):
        self._insert(table)
        with pytest.raises(ValueError, match="Unknown column"):
            table.query(symbol="AAPL", columns=["bogus"])
