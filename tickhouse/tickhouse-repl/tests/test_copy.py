"""Unit tests for the COPY command helpers in repl.py."""
import json
import os
import tempfile

import pytest

from tickhouse_repl.repl import _build_insert_sql, _handle_copy, _COPY_RE


# ---------------------------------------------------------------------------
# _build_insert_sql
# ---------------------------------------------------------------------------

class TestBuildInsertSql:
    def _bar(self, **kwargs):
        defaults = {
            "date": "2024-01-02",
            "symbol": "AAPL",
            "open": 185.20,
            "high": 186.10,
            "low": 184.90,
            "close": 185.85,
            "volume": 50123400,
        }
        return {**defaults, **kwargs}

    def test_single_row(self):
        sql = _build_insert_sql("bars", [self._bar()])
        assert sql.startswith("INSERT INTO bars VALUES")
        assert "('2024-01-02', 'AAPL', 185.2, 186.1, 184.9, 185.85, 50123400)" in sql

    def test_multiple_rows(self):
        rows = [
            self._bar(date="2024-01-02", symbol="AAPL"),
            self._bar(date="2024-01-03", symbol="MSFT", open=300.0, high=305.0, low=299.0, close=303.5, volume=12000000),
        ]
        sql = _build_insert_sql("bars", rows)
        assert sql.count("INSERT INTO") == 1
        assert "'AAPL'" in sql
        assert "'MSFT'" in sql
        assert "'2024-01-02'" in sql
        assert "'2024-01-03'" in sql

    def test_table_name_used(self):
        sql = _build_insert_sql("my_table", [self._bar()])
        assert "INSERT INTO my_table VALUES" in sql

    def test_string_fields_are_quoted(self):
        sql = _build_insert_sql("bars", [self._bar(date="2025-06-15", symbol="TSLA")])
        assert "'2025-06-15'" in sql
        assert "'TSLA'" in sql

    def test_numeric_fields_are_not_quoted(self):
        sql = _build_insert_sql("bars", [self._bar(open=100.5, volume=999)])
        # numerics appear bare — not wrapped in single quotes
        assert "100.5" in sql
        assert "999" in sql
        # verify no double-quoting like "'100.5'"
        assert "'100.5'" not in sql
        assert "'999'" not in sql


# ---------------------------------------------------------------------------
# _COPY_RE regex
# ---------------------------------------------------------------------------

class TestCopyRegex:
    def test_basic_with_single_quotes(self):
        m = _COPY_RE.match("COPY bars FROM '/tmp/data.json'")
        assert m is not None
        assert m.group(1) == "bars"
        assert m.group(2) == "/tmp/data.json"

    def test_basic_without_quotes(self):
        m = _COPY_RE.match("COPY bars FROM /tmp/data.json")
        assert m is not None
        assert m.group(2) == "/tmp/data.json"

    def test_case_insensitive(self):
        assert _COPY_RE.match("copy bars from '/tmp/data.json'") is not None
        assert _COPY_RE.match("Copy Bars From '/tmp/data.json'") is not None

    def test_no_match_for_regular_sql(self):
        assert _COPY_RE.match("SELECT * FROM bars") is None
        assert _COPY_RE.match("INSERT INTO bars VALUES ()") is None

    def test_double_quotes(self):
        m = _COPY_RE.match('COPY bars FROM "/tmp/data.json"')
        assert m is not None
        assert m.group(2) == "/tmp/data.json"


# ---------------------------------------------------------------------------
# _handle_copy (integration: uses a real socket pair + temp file)
# ---------------------------------------------------------------------------

import socket
import threading


def _socketpair():
    """Return a (client, server) connected socket pair."""
    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock.bind(("127.0.0.1", 0))
    server_sock.listen(1)
    port = server_sock.getsockname()[1]
    client = socket.create_connection(("127.0.0.1", port))
    server, _ = server_sock.accept()
    server_sock.close()
    return client, server


class TestHandleCopy:
    def _write_json(self, rows) -> str:
        """Write rows to a temp JSON file and return the path."""
        fh = tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False, encoding="utf-8"
        )
        json.dump(rows, fh)
        fh.close()
        return fh.name

    def _bar(self, **kwargs):
        defaults = {
            "date": "2024-01-02",
            "symbol": "AAPL",
            "open": 185.20,
            "high": 186.10,
            "low": 184.90,
            "close": 185.85,
            "volume": 50123400,
        }
        return {**defaults, **kwargs}

    def test_sends_insert_and_prints_response(self, capsys):
        """_handle_copy sends a valid INSERT and displays the server response."""
        from tickhouse_repl.protocol import recv_message, send_message

        rows = [self._bar(), self._bar(date="2024-01-03", symbol="MSFT")]
        path = self._write_json(rows)
        try:
            client, server = _socketpair()

            # Simulate server: receive INSERT, reply ok
            def fake_server():
                msg = recv_message(server)
                assert msg is not None
                assert "INSERT INTO bars" in msg
                assert "'AAPL'" in msg
                assert "'MSFT'" in msg
                reply = json.dumps({"status": "ok", "message": "inserted 2 rows"})
                send_message(server, reply)
                server.close()

            t = threading.Thread(target=fake_server, daemon=True)
            t.start()

            _handle_copy(client, f"COPY bars FROM '{path}'")
            client.close()
            t.join(timeout=3)
        finally:
            os.unlink(path)

        captured = capsys.readouterr()
        assert "inserted 2 rows" in captured.out

    def test_file_not_found_prints_error(self, capsys):
        client, server = _socketpair()
        with client, server:
            _handle_copy(client, "COPY bars FROM '/nonexistent/path/file.json'")
        captured = capsys.readouterr()
        assert "file not found" in captured.err

    def test_empty_array_no_send(self, capsys):
        """An empty JSON array should print a message and not send anything."""
        path = self._write_json([])
        try:
            client, server = _socketpair()
            with client, server:
                _handle_copy(client, f"COPY bars FROM '{path}'")
        finally:
            os.unlink(path)
        captured = capsys.readouterr()
        assert "0 rows" in captured.out

    def test_non_list_json_prints_error(self, capsys):
        path = self._write_json({"not": "a list"})
        try:
            client, server = _socketpair()
            with client, server:
                _handle_copy(client, f"COPY bars FROM '{path}'")
        finally:
            os.unlink(path)
        captured = capsys.readouterr()
        assert "array" in captured.err

    def test_invalid_json_prints_error(self, capsys):
        fh = tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False, encoding="utf-8"
        )
        fh.write("this is not json{{{")
        fh.close()
        try:
            client, server = _socketpair()
            with client, server:
                _handle_copy(client, f"COPY bars FROM '{fh.name}'")
        finally:
            os.unlink(fh.name)
        captured = capsys.readouterr()
        assert "invalid JSON" in captured.err
