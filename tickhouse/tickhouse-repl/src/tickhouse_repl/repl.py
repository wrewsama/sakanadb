"""
Interactive REPL for tickhouse.

Connects to a tickhouse server over TCP, sends SQL-like commands entered by
the user, and pretty-prints JSON responses.

Usage
-----
    tickhouse-repl [--host HOST] [--port PORT]

Client-side commands (not sent to server)
-----------------------------------------
    COPY <table> FROM '<path>'   Bulk-insert all rows from a JSON file.
        The file must be a JSON array of bar objects with keys:
        date, symbol, open, high, low, close, volume.
        All rows are sent in a single INSERT statement.
"""
import json
import re
import socket
import sys

from tickhouse_repl.protocol import recv_message, send_message

_COPY_RE = re.compile(
    r"^COPY\s+(\w+)\s+FROM\s+['\"]?(.+?)['\"]?\s*$",
    re.IGNORECASE,
)


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

            if _COPY_RE.match(line):
                _handle_copy(sock, line)
                continue

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


def _handle_copy(sock: socket.socket, line: str) -> None:
    """Handle a client-side COPY <table> FROM '<path>' command.

    Reads a JSON file containing a list of bar objects and sends a single
    INSERT statement to the server with all rows.
    """
    m = _COPY_RE.match(line)
    if not m:
        print("usage: COPY <table> FROM '<path>'", file=sys.stderr)
        return

    table, path = m.group(1), m.group(2).strip()

    try:
        with open(path, "r", encoding="utf-8") as fh:
            rows = json.load(fh)
    except FileNotFoundError:
        print(f"error: file not found: {path}", file=sys.stderr)
        return
    except OSError as exc:
        print(f"error: could not read file: {exc}", file=sys.stderr)
        return
    except json.JSONDecodeError as exc:
        print(f"error: invalid JSON: {exc}", file=sys.stderr)
        return

    if not isinstance(rows, list):
        print("error: JSON file must contain a top-level array of bar objects", file=sys.stderr)
        return

    if not rows:
        print("(0 rows — nothing to insert)")
        return

    sql = _build_insert_sql(table, rows)

    try:
        send_message(sock, sql)
        raw = recv_message(sock)
    except (ConnectionError, OSError) as exc:
        print(f"error: connection lost — {exc}", file=sys.stderr)
        return

    if raw is None:
        print("error: server closed the connection", file=sys.stderr)
        return

    _pretty_print(raw)


def _build_insert_sql(table: str, rows: list[dict]) -> str:
    """Convert a list of bar dicts into a single INSERT SQL statement.

    Each row is rendered as:
        ('<date>', '<symbol>', <open>, <high>, <low>, <close>, <volume>)

    String fields (date, symbol) are single-quoted; numeric fields are
    emitted as-is so the server's parser handles type coercion.
    """
    def _row_sql(row: dict) -> str:
        date   = row["date"]
        symbol = row["symbol"]
        open_  = row["open"]
        high   = row["high"]
        low    = row["low"]
        close  = row["close"]
        volume = row["volume"]
        return f"('{date}', '{symbol}', {open_}, {high}, {low}, {close}, {volume})"

    values = ",\n    ".join(_row_sql(r) for r in rows)
    return f"INSERT INTO {table} VALUES\n    {values}"


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

