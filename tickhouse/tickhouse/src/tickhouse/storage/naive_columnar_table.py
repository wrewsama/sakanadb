"""
Naive columnar storage implementation.

Layout
------
<data_dir>/<table_name>/
    meta.bin                  global metadata: next_part_id (8 bytes)
    part_00000000/
        index.bin             per-part primary index, sorted by (symbol, date)
        date.bin              8 bytes/row  — ASCII YYYYMMDD
        symbol.bin            16 bytes/row — null-padded UTF-8
        open.bin              8 bytes/row  — big-endian float64
        high.bin              8 bytes/row  — big-endian float64
        low.bin               8 bytes/row  — big-endian float64
        close.bin             8 bytes/row  — big-endian float64
        volume.bin            8 bytes/row  — big-endian uint64
    part_00000001/
        ...

All column files and per-part index files are immutable once written.
Only meta.bin is updated (atomically) on each insert.

Per-part index.bin format
--------------------------
Header  (8 bytes):
    [8 bytes: row_count  uint64 big-endian]

Records (32 bytes each), sorted by (symbol, date):
    [16 bytes: symbol, null-padded UTF-8]
    [ 8 bytes: date,   ASCII YYYYMMDD    ]
    [ 8 bytes: row_id  uint64 big-endian ] — row offset in column files

Query
-----
Every part is checked.  For each part its index.bin is loaded, filtered by
symbol and optional date bounds, and matched rows are fetched from the column
files by seeking to row_id * col_record_size.
"""
import struct
from pathlib import Path
from typing import Any

from tickhouse.storage.table import Table
from tickhouse.parser import InsertRow


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

ALL_COLUMNS: list[str] = ["date", "symbol", "open", "high", "low", "close", "volume"]

# Struct for meta.bin: single uint64 (next_part_id)
_META = struct.Struct(">Q")

# Struct for the index.bin header: single uint64 (row_count)
_INDEX_HEADER = struct.Struct(">Q")

# Struct for each index record: 16s (symbol) + 8s (date) + Q (row_id) = 32 bytes
_INDEX_RECORD = struct.Struct(">16s8sQ")

# Per-column struct: fixed size, big-endian where applicable
_COL_STRUCT: dict[str, struct.Struct] = {
    "date":   struct.Struct("8s"),   # ASCII YYYYMMDD
    "symbol": struct.Struct("16s"),  # null-padded UTF-8
    "open":   struct.Struct(">d"),   # float64
    "high":   struct.Struct(">d"),
    "low":    struct.Struct(">d"),
    "close":  struct.Struct(">d"),
    "volume": struct.Struct(">Q"),   # uint64
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _encode_date(date_str: str) -> bytes:
    """Convert 'YYYY-MM-DD' to 8-byte ASCII b'YYYYMMDD'."""
    compact = date_str.replace("-", "")
    return compact.encode("ascii").ljust(8, b"\x00")[:8]


def _decode_date(raw: bytes) -> str:
    """Convert 8-byte ASCII b'YYYYMMDD' back to 'YYYY-MM-DD'."""
    s = raw.rstrip(b"\x00").decode("ascii")
    return f"{s[:4]}-{s[4:6]}-{s[6:8]}"


def _encode_symbol(symbol: str) -> bytes:
    """Encode symbol as null-padded 16-byte UTF-8."""
    enc = symbol.encode("utf-8")
    return enc.ljust(16, b"\x00")[:16]


def _decode_symbol(raw: bytes) -> str:
    return raw.rstrip(b"\x00").decode("utf-8")


def _encode_col(col: str, value: Any) -> bytes:
    fmt = _COL_STRUCT[col]
    if col == "date":
        return fmt.pack(_encode_date(value))
    if col == "symbol":
        return fmt.pack(_encode_symbol(value))
    if col == "volume":
        return fmt.pack(int(value))
    return fmt.pack(float(value))


def _decode_col(col: str, raw: bytes) -> Any:
    fmt = _COL_STRUCT[col]
    (val,) = fmt.unpack(raw)
    if col == "date":
        return _decode_date(val)
    if col == "symbol":
        return _decode_symbol(val)
    return val


def _resolve_columns(columns: list[str] | None) -> list[str]:
    """Normalise column list; return ALL_COLUMNS for None / ['*']."""
    if not columns or columns == ["*"]:
        return ALL_COLUMNS
    for col in columns:
        if col not in ALL_COLUMNS:
            raise ValueError(
                f"Unknown column '{col}'. Valid columns: {ALL_COLUMNS}"
            )
    return columns


# ---------------------------------------------------------------------------
# NaiveColumnarTable
# ---------------------------------------------------------------------------

class NaiveColumnarTable(Table):
    """
    Columnar table with one binary file per column and a per-part primary index
    on (symbol, date).

    Each :meth:`insert_many` call creates a new immutable part directory.
    Queries scan every part's index and fetch matching rows from column files
    using O(1) seeks.
    """

    def __init__(self, name: str, data_dir: str | Path = "data") -> None:
        self._name = name
        self._dir = Path(data_dir) / name

    # ------------------------------------------------------------------
    # Table ABC
    # ------------------------------------------------------------------

    def create(self) -> None:
        """Create the on-disk directory for this table (idempotent)."""
        self._dir.mkdir(parents=True, exist_ok=True)
        meta = self._dir / "meta.bin"
        if not meta.exists():
            meta.write_bytes(_META.pack(0))

    def insert_many(self, rows: list[InsertRow]) -> None:
        self._ensure_exists()

        # 1. Read next_part_id from meta.bin
        (next_part_id,) = _META.unpack(self._meta_path.read_bytes())

        # 2. Create part directory
        part_dir = self._part_dir(next_part_id)
        part_dir.mkdir()

        # 3. Write column files (in insertion order)
        col_files = {col: open(part_dir / f"{col}.bin", "wb") for col in ALL_COLUMNS}
        try:
            for row in rows:
                row_dict = {
                    "date":   row.date,
                    "symbol": row.symbol,
                    "open":   row.open,
                    "high":   row.high,
                    "low":    row.low,
                    "close":  row.close,
                    "volume": row.volume,
                }
                for col in ALL_COLUMNS:
                    col_files[col].write(_encode_col(col, row_dict[col]))
        finally:
            for f in col_files.values():
                f.close()

        # 4. Build index records sorted by (symbol, date)
        index_records: list[tuple[bytes, bytes, int]] = []
        for row_id, row in enumerate(rows):
            sym_b = _encode_symbol(row.symbol)
            date_b = _encode_date(row.date)
            index_records.append((sym_b, date_b, row_id))
        index_records.sort(key=lambda r: (r[0], r[1]))

        # 5. Write immutable per-part index.bin
        index_path = part_dir / "index.bin"
        with open(index_path, "wb") as f:
            f.write(_INDEX_HEADER.pack(len(index_records)))
            for sym_b, date_b, row_id in index_records:
                f.write(_INDEX_RECORD.pack(sym_b, date_b, row_id))

        # 6. Atomically update meta.bin
        tmp = self._meta_path.with_suffix(".tmp")
        tmp.write_bytes(_META.pack(next_part_id + 1))
        tmp.replace(self._meta_path)

    def query(
        self,
        symbol: str,
        date_gte: str | None = None,
        date_lte: str | None = None,
        columns: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        self._ensure_exists()
        wanted = _resolve_columns(columns)

        # Encode bounds once for fast byte comparison
        sym_b = _encode_symbol(symbol)
        gte_b = _encode_date(date_gte) if date_gte is not None else None
        lte_b = _encode_date(date_lte) if date_lte is not None else None

        (next_part_id,) = _META.unpack(self._meta_path.read_bytes())

        results: list[dict[str, Any]] = []

        for part_id in range(next_part_id):
            part_dir = self._part_dir(part_id)
            matches = self._query_part(part_dir, sym_b, gte_b, lte_b)
            if not matches:
                continue

            # Open only the requested column files for this part
            col_fds = {col: open(part_dir / f"{col}.bin", "rb") for col in wanted}
            try:
                for row_id in matches:
                    row: dict[str, Any] = {}
                    for col in wanted:
                        col_size = _COL_STRUCT[col].size
                        col_fds[col].seek(row_id * col_size)
                        raw = col_fds[col].read(col_size)
                        row[col] = _decode_col(col, raw)
                    results.append(row)
            finally:
                for fd in col_fds.values():
                    fd.close()

        return results

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @property
    def _meta_path(self) -> Path:
        return self._dir / "meta.bin"

    def _part_dir(self, part_id: int) -> Path:
        return self._dir / f"part_{part_id:08d}"

    def _ensure_exists(self) -> None:
        if not self._dir.exists() or not self._meta_path.exists():
            raise FileNotFoundError(
                f"Table '{self._name}' does not exist. "
                "Run CREATE TABLE first."
            )

    def _query_part(
        self,
        part_dir: Path,
        sym_b: bytes,
        gte_b: bytes | None,
        lte_b: bytes | None,
    ) -> list[int]:
        """
        Load a part's index.bin and return row_ids matching the filters.
        """
        index_path = part_dir / "index.bin"
        with open(index_path, "rb") as f:
            (row_count,) = _INDEX_HEADER.unpack(f.read(_INDEX_HEADER.size))
            matched: list[int] = []
            for _ in range(row_count):
                raw = f.read(_INDEX_RECORD.size)
                rec_sym, rec_date, row_id = _INDEX_RECORD.unpack(raw)
                if rec_sym != sym_b:
                    continue
                if gte_b is not None and rec_date < gte_b:
                    continue
                if lte_b is not None and rec_date > lte_b:
                    continue
                matched.append(row_id)
        return matched
