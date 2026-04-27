# Testing

## 1. Running all tests

Each package is independent. Run commands from within the relevant package directory.

**Server (`tickhouse/`):**

```bash
cd tickhouse/tickhouse
uv run pytest
```

**REPL (`tickhouse-repl/`):**

```bash
cd tickhouse/tickhouse-repl
uv run pytest
```

---

## 2. Focused runs

Run a single test file:

```bash
uv run pytest tests/test_parser.py
uv run pytest tests/test_parquet_table.py
uv run pytest tests/test_service.py
uv run pytest tests/test_protocol.py
```

Run a single test class:

```bash
uv run pytest tests/test_parser.py::TestCreate
uv run pytest tests/test_parquet_table.py::TestInsertQuery
uv run pytest tests/test_service.py::TestQuery
```

Run a single test by name:

```bash
uv run pytest tests/test_service.py::TestQuery::test_restart_restores_tables
uv run pytest tests/test_parquet_table.py::TestInsertQuery::test_date_range_filter
uv run pytest tests/test_parser.py::TestSelect::test_select_star
```

---

## 3. Useful pytest flags

| Flag | Effect |
|---|---|
| `-v` | Verbose — show each test name and pass/fail |
| `-s` | Disable output capture — print statements appear in terminal |
| `--tb=short` | Shorter traceback on failure |
| `-x` | Stop after the first failure |
| `-k <expr>` | Run only tests whose name matches the expression |

Examples:

```bash
uv run pytest -v
uv run pytest -x --tb=short
uv run pytest -k "restart"
uv run pytest -k "date" -v
```

---

## 4. Manual end-to-end testing

Start the server in one terminal (from `tickhouse/tickhouse/`):

```bash
uv run tickhouse
```

Open the REPL in a second terminal (from `tickhouse/tickhouse-repl/`):

```bash
uv run tickhouse-repl
```

Sample SQL commands to run in the REPL:

```sql
CREATE TABLE bars

INSERT INTO bars VALUES (2024-01-02, AAPL, 185.20, 186.10, 184.90, 185.85, 50123400)

INSERT INTO bars VALUES (2024-01-03, AAPL, 184.50, 185.75, 183.80, 184.25, 48201000)

INSERT INTO bars VALUES (2024-01-02, MSFT, 374.00, 376.50, 373.20, 375.80, 21034500)

SELECT * FROM bars WHERE symbol = 'AAPL'

SELECT date, close, volume FROM bars WHERE symbol = 'AAPL' AND date >= '2024-01-03'

SELECT * FROM bars WHERE symbol = 'MSFT' AND date >= '2024-01-01' AND date <= '2024-12-31'
```

Exit the REPL with `\q`, `quit`, `exit`, or Ctrl-D.
