"""
TickhouseService — domain service layer.

Receives parsed Command objects (from parser.py) and dispatches them to
the appropriate Table instance.  All storage-engine specifics live in the
Table implementations; this class only orchestrates.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from tickhouse.parser import Command, CreateCommand, InsertCommand, QueryCommand
from tickhouse.storage import ParquetTable, Table


class TickhouseService:
    """
    Manages a registry of named tables and handles create / insert / query.

    Parameters
    ----------
    data_dir:
        Root directory under which table data is stored.
        Defaults to ``./data`` (relative to the process working directory).
    """

    def __init__(self, data_dir: str | Path = "data") -> None:
        self._data_dir = Path(data_dir)
        # In-memory registry: table_name -> Table instance.
        # Populated lazily on CREATE; restored on startup by scanning data_dir.
        self._tables: dict[str, Table] = {}
        self._restore_existing_tables()


    def handle(self, command: Command) -> dict[str, Any]:
        """
        Dispatch *command* and return a response dict.

        The response always has a ``"status"`` key (``"ok"`` or ``"error"``).
        Successful queries also carry a ``"data"`` key.
        """
        try:
            if isinstance(command, CreateCommand):
                return self.create(command.table)
            if isinstance(command, InsertCommand):
                return self.insert(command)
            if isinstance(command, QueryCommand):
                return self.query(
                    command.table,
                    command.symbol,
                    command.date_gte,
                    command.date_lte,
                    command.columns,
                )
            raise ValueError(f"Unknown command type: {type(command)}")
        except Exception as exc:  # noqa: BLE001
            return {"status": "error", "message": str(exc)}


    def create(self, table_name: str) -> dict[str, Any]:
        if table_name in self._tables:
            return {"status": "error", "message": f"Table '{table_name}' already exists."}
        table = ParquetTable(name=table_name, data_dir=self._data_dir)
        table.create()
        self._tables[table_name] = table
        return {"status": "ok", "message": f"Table '{table_name}' created."}

    def insert(self, command: InsertCommand) -> dict[str, Any]:
        table = self._get_table(command.table)
        table.insert_many(command.rows)
        n = len(command.rows)
        return {"status": "ok", "message": f"{n} row(s) inserted."}

    def query(
        self,
        table_name: str,
        symbol: str,
        date_gte: str | None = None,
        date_lte: str | None = None,
        columns: list[str] | None = None,
    ) -> dict[str, Any]:
        table = self._get_table(table_name)
        rows = table.query(
            symbol=symbol,
            date_gte=date_gte,
            date_lte=date_lte,
            columns=columns,
        )
        return {"status": "ok", "data": rows}

    def _get_table(self, name: str) -> Table:
        if name not in self._tables:
            raise KeyError(f"Table '{name}' not found. Run CREATE TABLE first.")
        return self._tables[name]

    def _restore_existing_tables(self) -> None:
        """
        On startup, scan data_dir for existing table directories and register
        them so the server survives restarts without needing re-CREATE.
        """
        if not self._data_dir.exists():
            return
        for entry in self._data_dir.iterdir():
            if entry.is_dir():
                table = ParquetTable(name=entry.name, data_dir=self._data_dir)
                self._tables[entry.name] = table
