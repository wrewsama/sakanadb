"""Integration tests for NaiveColumnarTable storage."""
import pytest
from pathlib import Path
from tickhouse.parser import InsertRow
from tickhouse.storage.naive_columnar_table import NaiveColumnarTable, ALL_COLUMNS


@pytest.fixture
def table(tmp_path: Path) -> NaiveColumnarTable:
    t = NaiveColumnarTable(name="test_tbl", data_dir=tmp_path)
    t.create()
    return t


def _row(date="2026-01-01", symbol="AAPL",
         open=100.0, high=110.0, low=99.0, close=105.0, volume=1_000_000) -> InsertRow:
    return InsertRow(date=date, symbol=symbol, open=open, high=high,
                     low=low, close=close, volume=volume)


class TestCreate:
    def test_creates_directory(self, tmp_path: Path):
        t = NaiveColumnarTable(name="stocks", data_dir=tmp_path)
        t.create()
        assert (tmp_path / "stocks").is_dir()

    def test_creates_meta_bin(self, tmp_path: Path):
        t = NaiveColumnarTable(name="stocks", data_dir=tmp_path)
        t.create()
        assert (tmp_path / "stocks" / "meta.bin").exists()

    def test_idempotent(self, tmp_path: Path):
        t = NaiveColumnarTable(name="stocks", data_dir=tmp_path)
        t.create()
        t.create()  # should not raise


class TestInsertQuery:
    def test_roundtrip(self, table: NaiveColumnarTable):
        table.insert_many([_row()])
        rows = table.query(symbol="AAPL")
        assert len(rows) == 1
        row = rows[0]
        assert row["date"] == "2026-01-01"
        assert row["symbol"] == "AAPL"
        assert row["open"] == pytest.approx(100.0)
        assert row["high"] == pytest.approx(110.0)
        assert row["low"] == pytest.approx(99.0)
        assert row["close"] == pytest.approx(105.0)
        assert row["volume"] == 1_000_000

    def test_bulk_insert_single_part(self, table: NaiveColumnarTable):
        table.insert_many([
            _row(date="2026-01-01", symbol="AAPL"),
            _row(date="2026-01-02", symbol="AAPL"),
            _row(date="2026-01-03", symbol="AAPL"),
        ])
        parts = list(table._dir.glob("part_*"))
        assert len(parts) == 1
        rows = table.query(symbol="AAPL")
        assert len(rows) == 3

    def test_two_inserts_two_parts(self, table: NaiveColumnarTable):
        table.insert_many([_row(date="2026-01-01")])
        table.insert_many([_row(date="2026-01-02")])
        parts = list(table._dir.glob("part_*"))
        assert len(parts) == 2
        rows = table.query(symbol="AAPL")
        assert len(rows) == 2

    def test_filters_by_symbol(self, table: NaiveColumnarTable):
        table.insert_many([_row(symbol="AAPL"), _row(symbol="TSLA")])
        rows = table.query(symbol="AAPL")
        assert all(r["symbol"] == "AAPL" for r in rows)
        assert len(rows) == 1

    def test_filters_by_symbol_across_parts(self, table: NaiveColumnarTable):
        table.insert_many([_row(date="2026-01-01", symbol="AAPL")])
        table.insert_many([_row(date="2026-01-02", symbol="TSLA")])
        table.insert_many([_row(date="2026-01-03", symbol="AAPL")])
        rows = table.query(symbol="AAPL")
        assert len(rows) == 2
        assert all(r["symbol"] == "AAPL" for r in rows)

    def test_date_gte_filter(self, table: NaiveColumnarTable):
        table.insert_many([
            _row(date="2026-01-01", symbol="AAPL"),
            _row(date="2026-06-01", symbol="AAPL"),
        ])
        rows = table.query(symbol="AAPL", date_gte="2026-03-01")
        assert len(rows) == 1
        assert rows[0]["date"] == "2026-06-01"

    def test_date_lte_filter(self, table: NaiveColumnarTable):
        table.insert_many([
            _row(date="2026-01-01", symbol="AAPL"),
            _row(date="2026-06-01", symbol="AAPL"),
        ])
        rows = table.query(symbol="AAPL", date_lte="2026-03-01")
        assert len(rows) == 1
        assert rows[0]["date"] == "2026-01-01"

    def test_date_range_filter(self, table: NaiveColumnarTable):
        table.insert_many([_row(date=f"2026-0{m}-01", symbol="AAPL") for m in range(1, 7)])
        rows = table.query(symbol="AAPL", date_gte="2026-02-01", date_lte="2026-04-01")
        dates = {r["date"] for r in rows}
        assert dates == {"2026-02-01", "2026-03-01", "2026-04-01"}

    def test_date_range_across_parts(self, table: NaiveColumnarTable):
        for m in range(1, 7):
            table.insert_many([_row(date=f"2026-0{m}-01", symbol="AAPL")])
        rows = table.query(symbol="AAPL", date_gte="2026-02-01", date_lte="2026-04-01")
        dates = {r["date"] for r in rows}
        assert dates == {"2026-02-01", "2026-03-01", "2026-04-01"}

    def test_column_projection(self, table: NaiveColumnarTable):
        table.insert_many([_row()])
        rows = table.query(symbol="AAPL", columns=["date", "close"])
        assert set(rows[0].keys()) == {"date", "close"}

    def test_star_returns_all_columns(self, table: NaiveColumnarTable):
        table.insert_many([_row()])
        rows = table.query(symbol="AAPL", columns=["*"])
        assert set(rows[0].keys()) == set(ALL_COLUMNS)

    def test_empty_result(self, table: NaiveColumnarTable):
        rows = table.query(symbol="NONEXISTENT")
        assert rows == []

    def test_empty_table_returns_empty(self, table: NaiveColumnarTable):
        rows = table.query(symbol="AAPL")
        assert rows == []

    def test_query_before_create_raises(self, tmp_path: Path):
        t = NaiveColumnarTable(name="missing", data_dir=tmp_path)
        with pytest.raises(FileNotFoundError):
            t.query(symbol="X")

    def test_insert_before_create_raises(self, tmp_path: Path):
        t = NaiveColumnarTable(name="missing", data_dir=tmp_path)
        with pytest.raises(FileNotFoundError):
            t.insert_many([_row()])

    def test_invalid_column_raises(self, table: NaiveColumnarTable):
        table.insert_many([_row()])
        with pytest.raises(ValueError, match="Unknown column"):
            table.query(symbol="AAPL", columns=["bogus"])

    def test_part_column_files_are_immutable(self, table: NaiveColumnarTable):
        """Each insert creates a new part; existing part files are never modified."""
        table.insert_many([_row(date="2026-01-01")])
        part0 = table._dir / "part_00000000"
        mtime_before = (part0 / "open.bin").stat().st_mtime

        table.insert_many([_row(date="2026-01-02")])
        mtime_after = (part0 / "open.bin").stat().st_mtime

        assert mtime_before == mtime_after

    def test_part_index_is_sorted_by_symbol_date(self, table: NaiveColumnarTable):
        """Index records within a part are sorted (symbol, date) regardless of insert order."""
        import struct
        table.insert_many([
            _row(date="2026-01-03", symbol="AAPL"),
            _row(date="2026-01-01", symbol="AAPL"),
            _row(date="2026-01-02", symbol="AAPL"),
        ])
        index_path = table._dir / "part_00000000" / "index.bin"
        data = index_path.read_bytes()
        header = struct.Struct(">Q")
        record = struct.Struct(">16s8sQ")
        (count,) = header.unpack(data[:8])
        records = []
        for i in range(count):
            off = 8 + i * record.size
            sym, date, _ = record.unpack(data[off:off + record.size])
            records.append(date)
        assert records == sorted(records)
