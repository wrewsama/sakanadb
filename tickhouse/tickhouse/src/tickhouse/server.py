"""
TCP server — accept loop and per-client handler.

Wire format (length-prefixed):
    Request:  [4-byte len][UTF-8 SQL string]
    Response: [4-byte len][UTF-8 JSON]
"""
from __future__ import annotations

import json
import socket
import threading
import logging
from pathlib import Path

from tickhouse.parser import parse
from tickhouse.protocol import recv_message, send_message
from tickhouse.service import TickhouseService

logger = logging.getLogger(__name__)


def start(
    host: str,
    port: int,
    data_dir: str | Path,
) -> None:
    """
    Start the tickhouse TCP server (blocking).

    Each client connection is handled in its own daemon thread.
    """
    service = TickhouseService(data_dir=data_dir)

    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_sock.bind((host, port))
    server_sock.listen()

    print(f"tickhouse listening on {host}:{port}  (data → {Path(data_dir).resolve()})")

    try:
        while True:
            conn, addr = server_sock.accept()
            t = threading.Thread(
                target=_handle_client,
                args=(conn, addr, service),
                daemon=True,
            )
            t.start()
    except KeyboardInterrupt:
        print("\nshutting down")
    finally:
        server_sock.close()


def _handle_client(
    conn: socket.socket,
    addr: tuple[str, int],
    service: TickhouseService,
) -> None:
    """Read → parse → execute → respond loop for a single client."""
    logger.debug("client connected: %s:%s", *addr)
    try:
        with conn:
            while True:
                raw = recv_message(conn)
                if raw is None:
                    break  # client disconnected cleanly

                try:
                    command = parse(raw)
                    response = service.handle(command)
                except ValueError as exc:
                    response = {"status": "error", "message": str(exc)}

                send_message(conn, json.dumps(response))
    except ConnectionError:
        pass  # client dropped mid-message — nothing to do
    except Exception:
        logger.exception("unhandled error for client %s:%s", *addr)
    finally:
        logger.debug("client disconnected: %s:%s", *addr)
