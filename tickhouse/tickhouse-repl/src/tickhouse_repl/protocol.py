"""
Binary protocol helpers (REPL side — mirrors tickhouse/protocol.py).

Wire format (both directions):
    [4 bytes big-endian uint32: payload length][N bytes: UTF-8 payload]
"""
from __future__ import annotations

import socket
import struct

_LENGTH_PREFIX = struct.Struct("!I")


def send_message(sock: socket.socket, payload: str) -> None:
    data = payload.encode("utf-8")
    sock.sendall(_LENGTH_PREFIX.pack(len(data)) + data)


def recv_message(sock: socket.socket) -> str | None:
    raw_len = _recv_exact(sock, _LENGTH_PREFIX.size)
    if raw_len is None:
        return None
    (length,) = _LENGTH_PREFIX.unpack(raw_len)
    raw_body = _recv_exact(sock, length)
    if raw_body is None:
        raise ConnectionError("connection closed mid-message")
    return raw_body.decode("utf-8")


def _recv_exact(sock: socket.socket, n: int) -> bytes | None:
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            return None
        buf.extend(chunk)
    return bytes(buf)
