"""
Binary protocol helpers.

Wire format (both directions):
    [4 bytes big-endian uint32: payload length][N bytes: UTF-8 payload]
"""
import socket
import struct

_LENGTH_PREFIX = struct.Struct("!I")  # big-endian unsigned 32-bit int


def send_message(sock: socket.socket, payload: str) -> None:
    """Encode and send a length-prefixed UTF-8 message."""
    data = payload.encode("utf-8")
    sock.sendall(_LENGTH_PREFIX.pack(len(data)) + data)


def recv_message(sock: socket.socket) -> str | None:
    """
    Read one length-prefixed message from *sock*.

    Returns the decoded string, or None if the connection was closed cleanly
    before any bytes arrived.  Raises ConnectionError if the connection drops
    mid-message.
    """
    raw_len = _recv_exact(sock, _LENGTH_PREFIX.size)
    if raw_len is None:
        return None
    (length,) = _LENGTH_PREFIX.unpack(raw_len)
    raw_body = _recv_exact(sock, length)
    if raw_body is None:
        raise ConnectionError("connection closed mid-message")
    return raw_body.decode("utf-8")


def _recv_exact(sock: socket.socket, n: int) -> bytes | None:
    """Read exactly *n* bytes; return None only if 0 bytes arrive on first read."""
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            return None
        buf.extend(chunk)
    return bytes(buf)
