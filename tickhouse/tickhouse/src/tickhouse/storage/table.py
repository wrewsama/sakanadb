"""
Abstract base class for all Table storage implementations.
"""
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any
from tickhouse.parser import InsertRow



class Table(ABC):
    """
    Storage abstraction for a single tickhouse table.

    Every concrete implementation must store OHLCV bar data and support
    the three operations below.
    """

    @classmethod
    @abstractmethod
    def restore(cls, name: str, data_dir: str | Path) -> "Table":
        """
        Re-open an existing on-disk table without running :meth:`create`.

        Called on service startup to re-hydrate tables that were created in a
        previous process.  Implementations should not perform any I/O that
        requires the table to be in a specific state beyond existing on disk.
        """
        ...

    @abstractmethod
    def create(self) -> None:
        """Initialise on-disk storage for this table (idempotent)."""
        ...

    @abstractmethod
    def insert_many(self, rows: list[InsertRow]) -> None:
        """
        Persist one or more OHLCV bars atomically.

        A single-row insert is the degenerate case: ``rows`` has one element.
        Implementations should write all rows in a single batch operation where
        possible (e.g. one Parquet file per call).
        """
        ...

    @abstractmethod
    def query(
        self,
        symbol: str,
        date_gte: str | None = None,
        date_lte: str | None = None,
        columns: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        """
        Return rows matching *symbol* (and optional date bounds) as a list
        of plain dicts.  *columns* is a list of column names to include;
        ``None`` or ``["*"]`` means all columns.
        """
        ...
