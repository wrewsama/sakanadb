"""
SQL-like command parser.

Supported syntax
----------------
CREATE TABLE <name>

INSERT INTO <name> VALUES (<date>, <symbol>, <open>, <high>, <low>, <close>, <volume>)
  [, (<date>, <symbol>, <open>, <high>, <low>, <close>, <volume>)] ...

  - date:   YYYY-MM-DD  (quoted or unquoted)
  - symbol: string      (quoted or unquoted)
  - open/high/low/close: float
  - volume: integer

  A single-row insert is just the degenerate case of a bulk insert.

SELECT <col1>[, <col2>, ...] FROM <name>
    WHERE symbol = '<sym>'
    [AND date >= '<date>']
    [AND date <= '<date>']

The WHERE clause must contain at least `symbol = '<sym>'`.
Column list may be '*' to mean all columns.
"""
import re
from dataclasses import dataclass


@dataclass
class CreateCommand:
    table: str


@dataclass
class InsertRow:
    date: str
    symbol: str
    open: float
    high: float
    low: float
    close: float
    volume: int


@dataclass
class InsertCommand:
    table: str
    rows: list[InsertRow]


@dataclass
class QueryCommand:
    table: str
    columns: list[str]          # ["*"] means all columns
    symbol: str
    date_gte: str | None = None
    date_lte: str | None = None


Command = CreateCommand | InsertCommand | QueryCommand


_QUOTED = r"'([^']+)'|\"([^\"]+)\""  # captures content inside '' or ""

def _unquote(s: str) -> str:
    """Strip surrounding single or double quotes if present."""
    s = s.strip()
    if (s.startswith("'") and s.endswith("'")) or (
        s.startswith('"') and s.endswith('"')
    ):
        return s[1:-1]
    return s

_RE_CREATE = re.compile(
    r"^\s*CREATE\s+TABLE\s+(\w+)\s*$",
    re.IGNORECASE,
)

_RE_INSERT = re.compile(
    r"^\s*INSERT\s+INTO\s+(\w+)\s+VALUES\s*(.+)\s*$",
    re.IGNORECASE | re.DOTALL,
)

_RE_SELECT = re.compile(
    r"^\s*SELECT\s+(.+?)\s+FROM\s+(\w+)\s+WHERE\s+(.+?)\s*$",
    re.IGNORECASE | re.DOTALL,
)

_RE_SYMBOL_COND = re.compile(
    r"symbol\s*=\s*(" + _QUOTED + r"|\w+)",
    re.IGNORECASE,
)
_RE_DATE_GTE = re.compile(
    r"date\s*>=\s*(" + _QUOTED + r"|[\d\-]+)",
    re.IGNORECASE,
)
_RE_DATE_LTE = re.compile(
    r"date\s*<=\s*(" + _QUOTED + r"|[\d\-]+)",
    re.IGNORECASE,
)

_RE_ROW_SPLIT = re.compile(r"\(([^)]+)\)")


def _parse_row(raw: str, row_index: int) -> InsertRow:
    """
    Parse a single comma-separated row string into an InsertRow.

    *raw* is the text *inside* the parentheses, e.g.::

        '2024-01-02', AAPL, 185.20, 186.10, 184.90, 185.85, 50123400

    Raises ValueError with a descriptive message on failure.
    """
    parts = [p.strip() for p in raw.split(",")]
    if len(parts) != 7:
        raise ValueError(
            f"Row {row_index}: INSERT expects 7 values "
            f"(date, symbol, open, high, low, close, volume), "
            f"got {len(parts)}: {raw!r}"
        )
    date_val   = _unquote(parts[0])
    symbol_val = _unquote(parts[1])
    try:
        open_val   = float(parts[2])
        high_val   = float(parts[3])
        low_val    = float(parts[4])
        close_val  = float(parts[5])
        volume_val = int(parts[6])
    except ValueError as exc:
        raise ValueError(
            f"Row {row_index}: invalid numeric value in INSERT: {exc}"
        ) from exc
    return InsertRow(
        date=date_val,
        symbol=symbol_val,
        open=open_val,
        high=high_val,
        low=low_val,
        close=close_val,
        volume=volume_val,
    )


def parse(sql: str) -> Command:
    """
    Parse a SQL-like string into a Command dataclass.

    Raises ValueError with a descriptive message on parse failure.
    """
    sql = sql.strip()

    m = _RE_CREATE.match(sql)
    if m:
        return CreateCommand(table=m.group(1))

    m = _RE_INSERT.match(sql)
    if m:
        table = m.group(1)
        raw_after_values = m.group(2).strip()

        row_matches = _RE_ROW_SPLIT.findall(raw_after_values)
        if not row_matches:
            raise ValueError(
                f"INSERT VALUES clause contains no valid row groups: "
                f"{raw_after_values!r}"
            )

        rows = [_parse_row(raw, i + 1) for i, raw in enumerate(row_matches)]
        return InsertCommand(table=table, rows=rows)

    m = _RE_SELECT.match(sql)
    if m:
        col_str   = m.group(1).strip()
        table     = m.group(2)
        where_str = m.group(3)

        if col_str == "*":
            columns = ["*"]
        else:
            columns = [c.strip() for c in col_str.split(",")]

        sym_m = _RE_SYMBOL_COND.search(where_str)
        if not sym_m:
            raise ValueError("SELECT WHERE clause must contain `symbol = '<value>'`")
        symbol = _unquote(sym_m.group(1))

        date_gte = None
        gte_m = _RE_DATE_GTE.search(where_str)
        if gte_m:
            date_gte = _unquote(gte_m.group(1))

        date_lte = None
        lte_m = _RE_DATE_LTE.search(where_str)
        if lte_m:
            date_lte = _unquote(lte_m.group(1))

        return QueryCommand(
            table=table,
            columns=columns,
            symbol=symbol,
            date_gte=date_gte,
            date_lte=date_lte,
        )

    raise ValueError(f"Unrecognised command: {sql!r}")
