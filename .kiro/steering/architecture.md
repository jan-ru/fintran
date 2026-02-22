# fintran — Architecture

## Governance Rules for Kiro

> These rules are hard constraints. Kiro must follow them before generating or modifying any code or structure.

### ArchiMate Application Architecture

All application-level elements of fintran (application components, services, interfaces, data objects) must be defined in the ArchiMate architecture file `architecture.archimate` at the root of the repository.

**Rule:** If a task requires an application-level element that is not present in `architecture.archimate`, Kiro must **stop, report the missing element to the user, and wait for approval before proceeding**. Kiro must not invent or assume elements outside of the defined architecture.

### C4 Model

fintran is designed and documented using the **C4 model** (Context, Container, Component, Code):

| Level | Scope | Artifact |
|---|---|---|
| **C1 — Context** | fintran in relation to users and external systems | `docs/c4-context.md` |
| **C2 — Container** | Runtime units (CLI, library, DuckDB, file system) | `docs/c4-container.md` |
| **C3 — Component** | Internal modules (readers, writers, core, transforms) | `docs/c4-component.md` |
| **C4 — Code** | Class/function level, generated from code | IDE / docstrings |

**Rule:** When designing or modifying a module, Kiro must reference the appropriate C4 level. New containers or components must be reflected in the corresponding C4 document before implementation begins.

---

## Overview

`fintran` follows a **reader → IR → writer** pipeline, inspired by pandoc. Every supported format has a dedicated reader and writer module. The Intermediate Representation (IR) is a normalized Polars DataFrame with a fixed schema.

```
Input File
    │
    ▼
┌─────────┐
│ Reader  │  (format-specific parsing)
└─────────┘
    │  Polars DataFrame (IR)
    ▼
┌─────────────┐
│  Transform  │  (optional: filtering, mapping, enrichment)
└─────────────┘
    │  Polars DataFrame (IR)
    ▼
┌─────────┐
│ Writer  │  (format-specific serialization)
└─────────┘
    │
    ▼
Output File
```

## Intermediate Representation (IR)

The IR is a Polars DataFrame with a canonical schema. Every reader must produce this; every writer consumes it.

| Field | Type | Required | Notes |
|---|---|---|---|
| `date` | `Date` | ✓ | Transaction or period date |
| `account` | `Utf8` | ✓ | Account code or name from source |
| `amount` | `Decimal` | ✓ | Signed amount (negative = credit) |
| `currency` | `Utf8` | ✓ | ISO 4217 code, e.g. `EUR` |
| `description` | `Utf8` | | Narrative or memo |
| `reference` | `Utf8` | | Source document reference |

> Account codes are passed through as-is from the source. A future mapping layer may normalize them to SBR/XBRL concepts.

## Project Structure

```
fintran/
├── pyproject.toml
├── uv.lock
├── fintran/
│   ├── cli.py           # Cyclopts entry point
│   ├── core/
│   │   ├── schema.py    # IR schema definition and validation
│   │   └── pipeline.py  # Reader → Transform → Writer orchestration
│   ├── readers/
│   │   ├── csv.py
│   │   ├── hledger.py
│   │   ├── excel.py
│   │   └── twinfield.py
│   ├── writers/
│   │   ├── csv.py
│   │   ├── hledger.py
│   │   ├── excel.py
│   │   ├── twinfield.py
│   │   └── duckdb.py
│   └── transforms/      # Optional enrichment steps
│       └── account_map.py
└── tests/
```

## CLI Interface

Modelled on pandoc's `--from` / `--to` flags:

```bash
fintran --from hledger --to twinfield input.journal -o output.csv
fintran --from excel --to hledger input.xlsx -o journal.txt
fintran --from twinfield --to duckdb export.csv -o balances.duckdb
```

## DuckDB and Polars Roles

These two libraries are complementary, not interchangeable:

| Concern | Tool |
|---|---|
| Reading CSV, Excel, Parquet, JSON | DuckDB (native, zero-copy) |
| In-memory transformation | Polars (fast, typed, lazy) |
| Writing to DuckDB tables | DuckDB |
| Writing to CSV, Excel, hledger | Polars or DuckDB depending on format |

The typical flow: DuckDB ingests raw source files → hands off to Polars as a DataFrame → Polars applies transformations → writer serializes to target format (which may be DuckDB again).

## Design Principles

- **Readers are strict** — they validate and reject malformed input early.
- **Writers are explicit** — output format is always declared; no guessing from file extension.
- **IR is immutable** — transforms return new DataFrames; the original IR is never mutated.
- **Excel sheets are explicit** — the `--sheet` flag must be provided when reading Excel files with multiple sheets; fintran does not guess.
