"""Unit tests for the REPL binary protocol helpers."""
import socket
import threading
import pytest
from tickhouse_repl.protocol import recv_message, send_message


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


class TestRoundtrip:
    def test_simple_string(self):
        client, server = _socketpair()
        with client, server:
            send_message(client, "hello world")
            result = recv_message(server)
        assert result == "hello world"

    def test_unicode(self):
        client, server = _socketpair()
        with client, server:
            msg = "SELECT * FROM tbl WHERE symbol = 'AAPL' AND date >= '2026-01-01'"
            send_message(client, msg)
            result = recv_message(server)
        assert result == msg

    def test_empty_string(self):
        client, server = _socketpair()
        with client, server:
            send_message(client, "")
            result = recv_message(server)
        assert result == ""

    def test_multiple_messages(self):
        client, server = _socketpair()
        messages = ["CREATE TABLE t", "INSERT INTO t VALUES ()", "SELECT * FROM t WHERE symbol = 'X'"]
        with client, server:
            for m in messages:
                send_message(client, m)
            received = [recv_message(server) for _ in messages]
        assert received == messages

    def test_recv_returns_none_on_clean_close(self):
        client, server = _socketpair()
        client.close()
        result = recv_message(server)
        server.close()
        assert result is None

    def test_bidirectional(self):
        """Simulate a request-response exchange."""
        client, server = _socketpair()
        with client, server:
            send_message(client, "ping")
            recv_message(server)           # server reads request
            send_message(server, "pong")  # server writes response
            result = recv_message(client)  # client reads response
        assert result == "pong"
