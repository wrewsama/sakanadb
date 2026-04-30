"""
Abstract base class for all Table storage implementations.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from tickhouse.parser import InsertRow


class Table(ABC):
    """
    Storage abstraction for a single tickhouse table.

    Every concrete implementation must store OHLCV bar data and support
    the three operations below.
    """

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
