"""
SQL-like command parser.

Supported syntax
----------------
CREATE TABLE <name>

INSERT INTO <name> VALUES (<date>, <symbol>, <open>, <high>, <low>, <close>, <volume>)
  - date:   YYYY-MM-DD  (quoted or unquoted)
  - symbol: string      (quoted or unquoted)
  - open/high/low/close: float
  - volume: integer

SELECT <col1>[, <col2>, ...] FROM <name>
    WHERE symbol = '<sym>'
    [AND date >= '<date>']
    [AND date <= '<date>']

The WHERE clause must contain at least `symbol = '<sym>'`.
Column list may be '*' to mean all columns.
"""
from __future__ import annotations

import re
from dataclasses import dataclass

@dataclass
class CreateCommand:
    table: str


@dataclass
class InsertCommand:
    table: str
    date: str
    symbol: str
    open: float
    high: float
    low: float
    close: float
    volume: int


@dataclass
class QueryCommand:
    table: str
    columns: list[str]          # ["*"] means all columns
    symbol: str
    date_gte: str | None = None  # inclusive lower bound  (>= )
    date_lte: str | None = None  # inclusive upper bound  (<= )


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
    r"^\s*INSERT\s+INTO\s+(\w+)\s+VALUES\s*\((.+)\)\s*$",
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


def parse(sql: str) -> Command:
    """
    Parse a SQL-like string into a Command dataclass.

    Raises ValueError with a descriptive message on parse failure.
    """
    sql = sql.strip()

    # --- CREATE ---
    m = _RE_CREATE.match(sql)
    if m:
        return CreateCommand(table=m.group(1))

    # --- INSERT ---
    m = _RE_INSERT.match(sql)
    if m:
        table = m.group(1)
        raw_values = m.group(2)
        parts = [p.strip() for p in raw_values.split(",")]
        if len(parts) != 7:
            raise ValueError(
                f"INSERT expects 7 values (date, symbol, open, high, low, close, volume), "
                f"got {len(parts)}: {raw_values!r}"
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
            raise ValueError(f"Invalid numeric value in INSERT: {exc}") from exc
        return InsertCommand(
            table=table,
            date=date_val,
            symbol=symbol_val,
            open=open_val,
            high=high_val,
            low=low_val,
            close=close_val,
            volume=volume_val,
        )

    # --- SELECT ---
    m = _RE_SELECT.match(sql)
    if m:
        col_str   = m.group(1).strip()
        table     = m.group(2)
        where_str = m.group(3)

        # Columns
        if col_str == "*":
            columns = ["*"]
        else:
            columns = [c.strip() for c in col_str.split(",")]

        # WHERE: symbol is mandatory
        sym_m = _RE_SYMBOL_COND.search(where_str)
        if not sym_m:
            raise ValueError("SELECT WHERE clause must contain `symbol = '<value>'`")
        # group(1) is the full match token; groups 2/3 are the quoted sub-groups
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
