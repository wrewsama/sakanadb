"""
MVP storage implementation: naive Parquet files.

Layout
------
<data_dir>/<table_name>/
    chunk_<n>.parquet     one file per insert_many call

Query
-----
Reads all chunk files, concatenates, filters in-memory.  No indexing.
"""
import os
import time
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from tickhouse.storage.table import Table
from tickhouse.parser import InsertRow


# Schema used for every Parquet file written by this implementation.
_SCHEMA = pa.schema(
    [
        pa.field("date",   pa.string()),
        pa.field("symbol", pa.string()),
        pa.field("open",   pa.float64()),
        pa.field("high",   pa.float64()),
        pa.field("low",    pa.float64()),
        pa.field("close",  pa.float64()),
        pa.field("volume", pa.int64()),
    ]
)

ALL_COLUMNS: list[str] = [f.name for f in _SCHEMA]


class ParquetTable(Table):
    """
    Naive Parquet-backed table.

    Each :meth:`insert_many` call writes exactly one Parquet file containing
    all supplied rows.  This is intentionally simple for the MVP; later
    iterations will use a proper merge-tree layout.
    """

    def __init__(self, name: str, data_dir: str | Path = "data") -> None:
        self._name = name
        self._dir = Path(data_dir) / name

    @classmethod
    def restore(cls, name: str, data_dir: str | Path = "data") -> "ParquetTable":
        """Re-open an existing Parquet table directory without running create()."""
        return cls(name=name, data_dir=data_dir)

    def create(self) -> None:
        """Create the on-disk directory for this table (idempotent)."""
        self._dir.mkdir(parents=True, exist_ok=True)

    def insert_many(self, rows: list[InsertRow]) -> None:
        """
        Persist one or more OHLCV bars in a single Parquet file.

        A single-row insert is the degenerate case: ``rows`` has one element.
        """
        self._ensure_exists()
        table = pa.table(
            {
                "date":   [r.date   for r in rows],
                "symbol": [r.symbol for r in rows],
                "open":   [float(r.open)   for r in rows],
                "high":   [float(r.high)   for r in rows],
                "low":    [float(r.low)    for r in rows],
                "close":  [float(r.close)  for r in rows],
                "volume": [int(r.volume)   for r in rows],
            },
            schema=_SCHEMA,
        )
        # Use a timestamp + pid suffix to avoid collisions under concurrent
        # inserts (good enough for the MVP).
        filename = f"chunk_{int(time.time_ns())}_{os.getpid()}.parquet"
        pq.write_table(table, self._dir / filename)

    def query(
        self,
        symbol: str,
        date_gte: str | None = None,
        date_lte: str | None = None,
        columns: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        self._ensure_exists()

        wanted_cols = _resolve_columns(columns)

        chunks: list[pa.Table] = []
        for path in sorted(self._dir.glob("chunk_*.parquet")):
            tbl = pq.read_table(path, schema=_SCHEMA)
            chunks.append(tbl)

        if not chunks:
            return []

        combined: pa.Table = pa.concat_tables(chunks)

        # Filter: symbol (mandatory)
        mask = pa.compute.equal(combined.column("symbol"), symbol)

        # Filter: date_gte
        if date_gte is not None:
            mask = pa.compute.and_(
                mask,
                pa.compute.greater_equal(combined.column("date"), date_gte),
            )

        # Filter: date_lte
        if date_lte is not None:
            mask = pa.compute.and_(
                mask,
                pa.compute.less_equal(combined.column("date"), date_lte),
            )

        filtered = combined.filter(mask)

        # Project columns
        if wanted_cols != ALL_COLUMNS:
            filtered = filtered.select(wanted_cols)

        return filtered.to_pylist()

    def _ensure_exists(self) -> None:
        if not self._dir.exists():
            raise FileNotFoundError(
                f"Table '{self._name}' does not exist. "
                "Run CREATE TABLE first."
            )


def _resolve_columns(columns: list[str] | None) -> list[str]:
    """Normalise the column list; return ALL_COLUMNS for None / ['*']."""
    if not columns or columns == ["*"]:
        return ALL_COLUMNS
    for col in columns:
        if col not in ALL_COLUMNS:
            raise ValueError(
                f"Unknown column '{col}'. Valid columns: {ALL_COLUMNS}"
            )
    return columns
