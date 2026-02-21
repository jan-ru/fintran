# fintran — C4 Level 2: Containers

## Description

This document describes the runtime units that make up fintran and how they interact.

```
┌──────────────────────────────────────────────────────────────────────┐
│                         fintran system                               │
│                                                                      │
│  ┌─────────────────────────────────────────────────────────────┐    │
│  │  CLI Process  [Python, Cyclopts]                             │    │
│  │  Entry point. Parses --from / --to flags, resolves reader    │    │
│  │  and writer, invokes pipeline.                               │    │
│  └───────────────────────┬─────────────────────────────────────┘    │
│                           │ calls                                    │
│  ┌────────────────────────▼────────────────────────────────────┐    │
│  │  fintran Library  [Python, Polars, DuckDB]                   │    │
│  │                                                              │    │
│  │  ┌──────────┐   ┌───────────┐   ┌──────────┐               │    │
│  │  │ Readers  │──▶│    IR     │──▶│ Writers  │               │    │
│  │  └──────────┘   │ (Polars   │   └──────────┘               │    │
│  │                 │ DataFrame)│                                │    │
│  │  ┌──────────┐   └───────────┘                               │    │
│  │  │Transforms│ (optional, sits between reader and writer)    │    │
│  │  └──────────┘                                               │    │
│  └─────────────────────────────────────────────────────────────┘    │
│                                                                      │
│  ┌──────────────┐   ┌──────────────┐   ┌──────────────────────┐    │
│  │ Input files  │   │ Output files │   │  DuckDB database     │    │
│  │ (file system)│   │ (file system)│   │  (.duckdb file)      │    │
│  └──────────────┘   └──────────────┘   └──────────────────────┘    │
│                                                                      │
└──────────────────────────────────────────────────────────────────────┘
```

## Containers

### CLI Process
| Property | Value |
|---|---|
| Technology | Python 3.12, Cyclopts |
| Responsibility | Parse user input, resolve format identifiers, invoke pipeline |
| Entry point | `fintran/cli.py` |
| Invocation | `uv run fintran --from <fmt> --to <fmt> <input> -o <output>` |

### fintran Library
| Property | Value |
|---|---|
| Technology | Python 3.12, Polars, DuckDB |
| Responsibility | Core transformation logic: readers, IR, transforms, writers |
| Key modules | `core/`, `readers/`, `writers/`, `transforms/` |
| IR engine | Polars DataFrame (in-memory transformation) |
| I/O engine | DuckDB (file ingestion and DuckDB sink) |

### Input Files (file system)
| Property | Value |
|---|---|
| Technology | Local file system |
| Supported formats | `.journal` (hledger), `.xlsx` (Excel), `.csv` (generic / Twinfield) |
| Access | Read-only by fintran |

### Output Files (file system)
| Property | Value |
|---|---|
| Technology | Local file system |
| Supported formats | `.journal`, `.xlsx`, `.csv` |
| Access | Write by fintran |

### DuckDB Database
| Property | Value |
|---|---|
| Technology | DuckDB embedded database |
| Responsibility | Persistent storage of normalized balance tables |
| File | `.duckdb` file on local file system |
| Access | Write (append / upsert) by DuckDB Writer; queryable externally by DuckDB CLI, Power BI |

## Container Interactions

| From | To | Description |
|---|---|---|
| CLI Process | fintran Library | Invokes pipeline with resolved reader/writer pair |
| fintran Library (Reader) | Input Files | Reads source file via DuckDB or Polars I/O |
| fintran Library (Writer) | Output Files | Writes transformed data to target file |
| fintran Library (Writer) | DuckDB Database | Writes IR to `balances` table |

## Deployment

fintran runs as a single process on the user's local machine. No server, no network, no authentication. Optionally containerized via Docker for distribution.
