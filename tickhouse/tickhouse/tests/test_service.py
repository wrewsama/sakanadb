"""Integration tests for TickhouseService."""
import pytest
from pathlib import Path
from tickhouse.parser import InsertCommand, InsertRow, parse
from tickhouse.service import TickhouseService


@pytest.fixture
def svc(tmp_path: Path) -> TickhouseService:
    return TickhouseService(data_dir=tmp_path)


def _cmd(table: str, *rows_kwargs) -> InsertCommand:
    """Build an InsertCommand with one or more rows from keyword-arg dicts."""
    rows = [
        InsertRow(
            date=kw.get("date", "2026-01-01"),
            symbol=kw.get("symbol", "AAPL"),
            open=kw.get("open", 100.0),
            high=kw.get("high", 110.0),
            low=kw.get("low", 99.0),
            close=kw.get("close", 105.0),
            volume=kw.get("volume", 1_000_000),
        )
        for kw in rows_kwargs
    ]
    return InsertCommand(table=table, rows=rows)


class TestCreate:
    def test_creates_table(self, svc: TickhouseService):
        resp = svc.create("stocks")
        assert resp["status"] == "ok"

    def test_duplicate_create_returns_error(self, svc: TickhouseService):
        svc.create("stocks")
        resp = svc.create("stocks")
        assert resp["status"] == "error"
        assert "already exists" in resp["message"]


class TestInsert:
    def test_insert_single_row_ok(self, svc: TickhouseService):
        svc.create("stocks")
        resp = svc.insert(_cmd("stocks", {}))
        assert resp["status"] == "ok"
        assert "1 row(s)" in resp["message"]

    def test_insert_bulk_rows_ok(self, svc: TickhouseService):
        svc.create("stocks")
        resp = svc.insert(_cmd("stocks", {}, {"date": "2026-01-02"}, {"date": "2026-01-03"}))
        assert resp["status"] == "ok"
        assert "3 row(s)" in resp["message"]

    def test_insert_missing_table_raises(self, svc: TickhouseService):
        """Calling insert() directly on a missing table raises KeyError."""
        with pytest.raises(KeyError, match="not found"):
            svc.insert(_cmd("missing", {}))

    def test_insert_missing_table_via_handle(self, svc: TickhouseService):
        """Via handle(), missing-table errors are returned as error dicts."""
        resp = svc.handle(parse("INSERT INTO missing VALUES ('2026-01-01', 'AAPL', 1, 2, 0.5, 1.5, 100)"))
        assert resp["status"] == "error"


class TestQuery:
    def test_query_returns_rows(self, svc: TickhouseService):
        svc.create("stocks")
        svc.insert(_cmd("stocks",
                        {"date": "2026-01-01"},
                        {"date": "2026-02-01"}))
        resp = svc.query("stocks", "AAPL")
        assert resp["status"] == "ok"
        assert len(resp["data"]) == 2

    def test_query_missing_table_raises(self, svc: TickhouseService):
        """Calling query() directly on a missing table raises KeyError."""
        with pytest.raises(KeyError, match="not found"):
            svc.query("missing", "AAPL")

    def test_query_missing_table_via_handle(self, svc: TickhouseService):
        """Via handle(), missing-table errors are returned as error dicts."""
        resp = svc.handle(parse("SELECT * FROM missing WHERE symbol = 'AAPL'"))
        assert resp["status"] == "error"

    def test_handle_dispatches_create(self, svc: TickhouseService):
        resp = svc.handle(parse("CREATE TABLE t"))
        assert resp["status"] == "ok"

    def test_handle_dispatches_insert_then_query(self, svc: TickhouseService):
        svc.handle(parse("CREATE TABLE t"))
        svc.handle(parse("INSERT INTO t VALUES ('2026-01-01', 'AAPL', 1, 2, 0.5, 1.5, 100)"))
        resp = svc.handle(parse("SELECT * FROM t WHERE symbol = 'AAPL'"))
        assert resp["status"] == "ok"
        assert len(resp["data"]) == 1

    def test_handle_dispatches_bulk_insert_then_query(self, svc: TickhouseService):
        svc.handle(parse("CREATE TABLE t"))
        svc.handle(parse(
            "INSERT INTO t VALUES "
            "('2026-01-01', 'AAPL', 1, 2, 0.5, 1.5, 100), "
            "('2026-01-02', 'AAPL', 2, 3, 1.0, 2.5, 200)"
        ))
        resp = svc.handle(parse("SELECT * FROM t WHERE symbol = 'AAPL'"))
        assert resp["status"] == "ok"
        assert len(resp["data"]) == 2

    def test_restore_existing_tables(self, tmp_path: Path):
        """Tables created in one service instance are visible in a new one (restart sim)."""
        svc1 = TickhouseService(data_dir=tmp_path)
        svc1.create("stocks")
        svc1.insert(_cmd("stocks", {"date": "2026-01-01"}))

        svc2 = TickhouseService(data_dir=tmp_path)
        resp = svc2.query("stocks", "AAPL")
        assert resp["status"] == "ok"
        assert len(resp["data"]) == 1
