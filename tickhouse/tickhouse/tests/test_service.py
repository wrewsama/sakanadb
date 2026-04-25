"""Integration tests for TickhouseService."""
import pytest
from pathlib import Path
from tickhouse.service import TickhouseService


@pytest.fixture
def svc(tmp_path: Path) -> TickhouseService:
    return TickhouseService(data_dir=tmp_path)


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
    def test_insert_ok(self, svc: TickhouseService):
        svc.create("stocks")
        resp = svc.insert("stocks", "2026-01-01", "AAPL", 100, 110, 99, 105, 1_000_000)
        assert resp["status"] == "ok"

    def test_insert_missing_table_raises(self, svc: TickhouseService):
        """Calling insert() directly on a missing table raises KeyError."""
        with pytest.raises(KeyError, match="not found"):
            svc.insert("missing", "2026-01-01", "AAPL", 100, 110, 99, 105, 1_000_000)

    def test_insert_missing_table_via_handle(self, svc: TickhouseService):
        """Via handle(), missing-table errors are returned as error dicts."""
        from tickhouse.parser import parse
        resp = svc.handle(parse("INSERT INTO missing VALUES ('2026-01-01', 'AAPL', 1, 2, 0.5, 1.5, 100)"))
        assert resp["status"] == "error"


class TestQuery:
    def test_query_returns_rows(self, svc: TickhouseService):
        svc.create("stocks")
        svc.insert("stocks", "2026-01-01", "AAPL", 100, 110, 99, 105, 1_000_000)
        svc.insert("stocks", "2026-02-01", "AAPL", 105, 115, 103, 112, 900_000)
        resp = svc.query("stocks", "AAPL")
        assert resp["status"] == "ok"
        assert len(resp["data"]) == 2

    def test_query_missing_table_raises(self, svc: TickhouseService):
        """Calling query() directly on a missing table raises KeyError."""
        with pytest.raises(KeyError, match="not found"):
            svc.query("missing", "AAPL")

    def test_query_missing_table_via_handle(self, svc: TickhouseService):
        """Via handle(), missing-table errors are returned as error dicts."""
        from tickhouse.parser import parse
        resp = svc.handle(parse("SELECT * FROM missing WHERE symbol = 'AAPL'"))
        assert resp["status"] == "error"

    def test_handle_dispatches_create(self, svc: TickhouseService):
        from tickhouse.parser import parse
        resp = svc.handle(parse("CREATE TABLE t"))
        assert resp["status"] == "ok"

    def test_handle_dispatches_insert_then_query(self, svc: TickhouseService):
        from tickhouse.parser import parse
        svc.handle(parse("CREATE TABLE t"))
        svc.handle(parse("INSERT INTO t VALUES ('2026-01-01', 'AAPL', 1, 2, 0.5, 1.5, 100)"))
        resp = svc.handle(parse("SELECT * FROM t WHERE symbol = 'AAPL'"))
        assert resp["status"] == "ok"
        assert len(resp["data"]) == 1

    def test_restore_existing_tables(self, tmp_path: Path):
        """Tables created in one service instance are visible in a new one (restart sim)."""
        svc1 = TickhouseService(data_dir=tmp_path)
        svc1.create("stocks")
        svc1.insert("stocks", "2026-01-01", "AAPL", 100, 110, 99, 105, 1_000_000)

        svc2 = TickhouseService(data_dir=tmp_path)
        resp = svc2.query("stocks", "AAPL")
        assert resp["status"] == "ok"
        assert len(resp["data"]) == 1
