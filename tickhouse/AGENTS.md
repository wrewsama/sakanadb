# AGENTS.md — tickhouse

## What this is

A columnar time-series database for L1 market data (OHLCV bars). Two independent Python packages:

- `tickhouse/` — TCP server
- `tickhouse-repl/` — REPL client

**Status: skeleton only.** Both packages contain stub `main()` functions. No business logic, no tests, no external dependencies exist yet.

## Toolchain

- **Python 3.12** (pinned via `.python-version` in each package — system Python is 3.13 and will NOT be used)
- **uv** for package management, venv creation, and running scripts (no pip/venv/poetry)

## Package structure

The two packages are **not** in a shared uv workspace. Each has its own `pyproject.toml` and `.venv`. Run all commands from within the relevant package directory.

Both use `src`-layout: source lives at `src/<package_name>/`, not at the repo root.

## Developer commands

```bash
# Run server (from tickhouse/tickhouse/)
uv run tickhouse

# Run REPL (from tickhouse/tickhouse-repl/)
uv run tickhouse-repl

# Sync deps / create .venv (first-time or after pyproject.toml changes)
uv sync

# Add a dependency
uv add <package>

# Run tests (once pytest is added)
uv run pytest
uv run pytest tests/path/to/test_file.py::test_name  # focused
```

## Non-obvious setup facts

- `uv run` auto-creates `.venv` on first use — no manual venv step needed.
- `uv.lock` files are not committed. `uv sync` regenerates them.
- No CI exists for this project. The only CI (`/.github/workflows/go.yml`) targets `sakanakv2`.
- No linter, formatter, or type checker is configured yet. When adding tools (ruff, mypy, etc.), configure them under `[tool.*]` in `pyproject.toml`.

## Testing setup (not yet done — do it this way)

When creating tests, add to `pyproject.toml`:

```toml
[tool.pytest.ini_options]
pythonpath = ["src"]
```

This is required because of the `src`-layout — pytest won't find the package otherwise.

## Architecture (from docs/)

- Server and REPL communicate over **TCP** using a custom binary protocol.
- Server follows Clean Architecture: `TickhouseService` → `Table` (create / insert / query).
- SQL-like query syntax: `SELECT ... WHERE symbol = 'X' AND date >= 'Y'`.
- Planned storage iterations: naive Parquet → custom column-store → delta compression → XOR float compression. Each iteration benchmarks 1M record insert and sub-1s query against total file size.

See `docs/design.md`, `docs/plans.md`, and `docs/prd.md` for full specs.
