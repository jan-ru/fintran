# fintran — Libraries

## Core Stack

| Library | Role | Rationale |
|---|---|---|
| [Polars](https://pola.rs) | Transformation engine / IR | Fast, modern DataFrame library; typed, lazy evaluation, ideal for structured financial data |
| [DuckDB](https://duckdb.org) | I/O and persistence | Reads/writes CSV, Parquet, Excel; serves as output sink for balance tables; complements Polars for SQL-style queries |
| [Cyclopts](https://github.com/BrianPugh/cyclopts) | CLI framework | Modern, type-hint-driven CLI; minimal boilerplate; clean alternative to Typer/Click |
| [uv](https://docs.astral.sh/uv/) | Package and project management | Fast, modern replacement for pip/venv; handles dependency locking and virtual environments |

## Supporting Libraries (anticipated)

| Library | Role |
|---|---|
| `openpyxl` / `fastexcel` | Excel read/write support (via Polars or DuckDB) |
| `pydantic` | Schema validation for IR and config files |
| `rich` | Terminal output formatting for CLI feedback |

## Python Version

Python 3.12+ required (leverages modern type hint syntax used by Cyclopts and Polars).

## Dependency Management

All dependencies are declared in `pyproject.toml` and locked via `uv.lock`. Use `uv run fintran` for zero-install execution in development.
