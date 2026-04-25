"""
Interactive REPL for tickhouse.

Connects to a tickhouse server over TCP, sends SQL-like commands entered by
the user, and pretty-prints JSON responses.

Usage
-----
    tickhouse-repl [--host HOST] [--port PORT]
"""
from __future__ import annotations

import json
import socket
import sys

from tickhouse_repl.protocol import recv_message, send_message



def start(host: str, port: int) -> None:
    """Connect to the server and run the interactive REPL loop."""
    try:
        sock = socket.create_connection((host, port))
    except ConnectionRefusedError:
        print(f"error: could not connect to tickhouse at {host}:{port}", file=sys.stderr)
        print("Is the server running?  Start it with:  uv run tickhouse", file=sys.stderr)
        sys.exit(1)

    print(f"Connected to tickhouse at {host}:{port}")
    print("Type SQL commands, or 'quit' / Ctrl-D to exit.\n")

    with sock:
        while True:
            try:
                line = input("tickhouse> ").strip()
            except (EOFError, KeyboardInterrupt):
                print()
                break

            if not line:
                continue
            if line.lower() in {"quit", "exit", "\\q"}:
                break

            try:
                send_message(sock, line)
                raw = recv_message(sock)
            except (ConnectionError, OSError) as exc:
                print(f"error: connection lost — {exc}", file=sys.stderr)
                break

            if raw is None:
                print("error: server closed the connection", file=sys.stderr)
                break

            _pretty_print(raw)


def _pretty_print(raw: str) -> None:
    """Decode and display a JSON response from the server."""
    try:
        obj = json.loads(raw)
    except json.JSONDecodeError:
        print(raw)
        return

    status = obj.get("status", "?")

    if status == "error":
        print(f"ERROR: {obj.get('message', raw)}")
        return

    if "data" in obj:
        _print_rows(obj["data"])
        return

    # Generic OK message
    if "message" in obj:
        print(obj["message"])
    else:
        print(json.dumps(obj, indent=2))

def _print_rows(rows: list[dict]):
    if not rows:
        print("(0 rows)")
        return
    cols = list(rows[0].keys())
    widths = {}
    for col_name in cols:
        max_width = len(col_name)
        for r in rows:
            max_width = max(max_width, len(str(r.get(col_name, ""))))
        widths[col_name] = max_width

    header = "  ".join(c.ljust(widths[c]) for c in cols)
    separator = "  ".join("-" * widths[c] for c in cols)
    print(header)
    print(separator)

    for row in rows:
        print("  ".join(str(row.get(c, "")).ljust(widths[c]) for c in cols))
    print(f"\n({len(rows)} row{'s' if len(rows) != 1 else ''})")

