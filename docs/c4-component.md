# fintran — C4 Level 3: Components

## Description

This document describes the internal modules and components within the fintran library and how they interact to implement the reader → IR → writer pipeline.

```
┌────────────────────────────────────────────────────────────────────────┐
│                        fintran Library                                 │
│                                                                        │
│  ┌──────────────────────────────────────────────────────────────────┐ │
│  │  Core Module                                                      │ │
│  │  ┌──────────┐  ┌───────────┐  ┌────────────┐                    │ │
│  │  │ schema.py│  │protocols.py│  │exceptions.py│                   │ │
│  │  │ IR schema│  │ Reader/   │  │ Error types│                    │ │
│  │  │definition│  │ Writer    │  │            │                    │ │
│  │  │          │  │ protocols │  │            │                    │ │
│  │  └──────────┘  └───────────┘  └────────────┘                    │ │
│  └──────────────────────────────────────────────────────────────────┘ │
│                           │                                            │
│                           │ defines contracts                          │
│                           ▼                                            │
│  ┌──────────────────────────────────────────────────────────────────┐ │
│  │  Readers Module                                                   │ │
│  │  ┌────────────┐  ┌──────────┐  ┌─────────┐  ┌────────────┐     │ │
│  │  │hledger.py  │  │ excel.py │  │ csv.py  │  │twinfield.py│     │ │
│  │  │Plain-text  │  │Workbook  │  │Generic  │  │ERP export  │     │ │
│  │  │accounting  │  │reader    │  │tabular  │  │reader      │     │ │
│  │  └────────────┘  └──────────┘  └─────────┘  └────────────┘     │ │
│  │         │              │             │              │            │ │
│  └─────────┼──────────────┼─────────────┼──────────────┼────────────┘ │
│            │              │             │              │              │
│            └──────────────┴─────────────┴──────────────┘              │
│                           │ produces                                  │
│                           ▼                                           │
│  ┌──────────────────────────────────────────────────────────────────┐ │
│  │  Intermediate Representation (IR)                                 │ │
│  │  Polars DataFrame with canonical schema:                          │ │
│  │  [date, account, amount, currency, description, reference]        │ │
│  └──────────────────────────────────────────────────────────────────┘ │
│                           │                                           │
│                           │ optionally transformed by                 │
│                           ▼                                           │
│  ┌──────────────────────────────────────────────────────────────────┐ │
│  │  Transforms Module (future)                                       │ │
│  │  ┌──────────────┐                                                 │ │
│  │  │account_map.py│  Maps source accounts to target taxonomy        │ │
│  │  └──────────────┘  (e.g., SBR/XBRL concepts)                      │ │
│  └──────────────────────────────────────────────────────────────────┘ │
│                           │                                           │
│                           │ validated by                              │
│                           ▼                                           │
│  ┌──────────────────────────────────────────────────────────────────┐ │
│  │  Validation Module (in development)                               │ │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐           │ │
│  │  │protocols.py  │  │business.py   │  │quality.py    │           │ │
│  │  │Validator     │  │Business rule │  │Data quality  │           │ │
│  │  │protocol      │  │validators    │  │validators    │           │ │
│  │  └──────────────┘  └──────────────┘  └──────────────┘           │ │
│  └──────────────────────────────────────────────────────────────────┘ │
│                           │                                           │
│                           │ consumed by                               │
│                           ▼                                           │
│  ┌──────────────────────────────────────────────────────────────────┐ │
│  │  Writers Module                                                   │ │
│  │  ┌────────────┐  ┌──────────┐  ┌─────────┐  ┌────────────┐     │ │
│  │  │hledger.py  │  │ excel.py │  │ csv.py  │  │ duckdb.py  │     │ │
│  │  │Journal     │  │Workbook  │  │Generic  │  │Database    │     │ │
│  │  │writer      │  │writer    │  │tabular  │  │writer      │     │ │
│  │  └────────────┘  └──────────┘  └─────────┘  └────────────┘     │ │
│  │         │              │             │              │            │ │
│  └─────────┼──────────────┼─────────────┼──────────────┼────────────┘ │
│            │              │             │              │              │
│            └──────────────┴─────────────┴──────────────┘              │
│                           │ writes to                                 │
│                           ▼                                           │
│                    Output Files / Database                            │
│                                                                        │
└────────────────────────────────────────────────────────────────────────┘
```

## Components

### Core Module

The core module defines the fundamental abstractions and contracts for the entire system.

| Component | Responsibility | Key Elements |
|---|---|---|
| **schema.py** | IR schema definition and validation | `IR_SCHEMA` constant (Polars schema dict), schema validation functions |
| **protocols.py** | Reader and Writer protocols | `Reader` protocol (`.read()` method), `Writer` protocol (`.write()` method) |
| **exceptions.py** | Error types | `FintranError`, `ReaderError`, `WriterError`, `ValidationError` |

**Dependencies:**
- Polars (for schema types)
- Python typing (for Protocol definitions)

**Used by:** All readers, writers, and transforms

---

### Readers Module

Each reader implements the `Reader` protocol and produces an IR DataFrame from a specific source format.

| Component | Source Format | Key Responsibilities |
|---|---|---|
| **hledger.py** | Plain-text accounting (`.journal`) | Parse hledger journal syntax, extract transactions, map to IR schema |
| **excel.py** | Excel workbooks (`.xlsx`) | Read specified sheet, validate columns, convert to IR |
| **csv.py** | Generic CSV files | Infer or validate schema, map columns to IR fields |
| **twinfield.py** | Twinfield ERP exports (CSV) | Parse Twinfield-specific CSV format, handle multi-currency |

**Common pattern:**
```python
class HledgerReader:
    def read(self, path: Path) -> pl.DataFrame:
        # 1. Parse source format
        # 2. Validate required fields
        # 3. Map to IR schema
        # 4. Return Polars DataFrame
```

**Dependencies:**
- Core module (schema, protocols, exceptions)
- Polars (DataFrame construction)
- DuckDB (for CSV/Excel I/O in some readers)

---

### Intermediate Representation (IR)

The IR is not a component but a **data contract** — a Polars DataFrame with a fixed schema:

| Field | Type | Required | Notes |
|---|---|---|---|
| `date` | `Date` | ✓ | Transaction or period date |
| `account` | `Utf8` | ✓ | Account code or name from source |
| `amount` | `Decimal` | ✓ | Signed amount (negative = credit) |
| `currency` | `Utf8` | ✓ | ISO 4217 code, e.g. `EUR` |
| `description` | `Utf8` | | Narrative or memo |
| `reference` | `Utf8` | | Source document reference |

**Immutability:** Transforms return new DataFrames; the original IR is never mutated.

---

### Transforms Module (future)

Optional transformation layer that sits between readers and writers.

| Component | Responsibility | Status |
|---|---|---|
| **account_map.py** | Maps source account codes to target taxonomy (e.g., SBR/XBRL concepts) | Planned |

**Pattern:**
```python
def transform(ir: pl.DataFrame, mapping: dict) -> pl.DataFrame:
    return ir.with_columns(
        pl.col("account").map_dict(mapping).alias("account")
    )
```

---

### Validation Module (in development)

Validates IR data against business rules and quality constraints.

| Component | Responsibility | Status |
|---|---|---|
| **protocols.py** | `Validator` protocol definition | Implemented |
| **business.py** | Business rule validators (e.g., balanced transactions, valid accounts) | In progress |
| **quality.py** | Data quality validators (e.g., completeness, consistency) | In progress |

**Pattern:**
```python
class BalancedTransactionsValidator:
    def validate(self, ir: pl.DataFrame) -> list[ValidationError]:
        # Check that debits == credits per transaction
        # Return list of errors (empty if valid)
```

**Integration:** Validators run after readers produce IR, before writers consume it.

---

### Writers Module

Each writer implements the `Writer` protocol and serializes an IR DataFrame to a specific target format.

| Component | Target Format | Key Responsibilities |
|---|---|---|
| **hledger.py** | Plain-text accounting (`.journal`) | Format transactions in hledger syntax, preserve metadata |
| **excel.py** | Excel workbooks (`.xlsx`) | Write IR to specified sheet, apply formatting |
| **csv.py** | Generic CSV files | Serialize IR to CSV with configurable delimiter |
| **duckdb.py** | DuckDB database (`.duckdb`) | Write IR to `balances` table, support upsert |

**Common pattern:**
```python
class HledgerWriter:
    def write(self, ir: pl.DataFrame, path: Path) -> None:
        # 1. Validate IR schema
        # 2. Transform to target format
        # 3. Write to file/database
```

**Dependencies:**
- Core module (schema, protocols, exceptions)
- Polars (DataFrame operations)
- DuckDB (for database writer)

---

## Component Interactions

### Data Flow

1. **CLI** invokes pipeline with `--from` and `--to` flags
2. **Pipeline** resolves reader and writer from format identifiers
3. **Reader** parses source file → produces IR DataFrame
4. **Validator** (optional) checks IR against business rules
5. **Transform** (optional) enriches or maps IR
6. **Writer** serializes IR → writes to target format

### Dependency Graph

```
Core (schema, protocols, exceptions)
  ↓
Readers ──→ IR ──→ Transforms ──→ Validation ──→ Writers
  ↓                                                  ↓
Input Files                                    Output Files
```

---

## Design Principles

- **Readers are strict**: Validate and reject malformed input early
- **Writers are explicit**: Output format is always declared; no guessing
- **IR is immutable**: Transforms return new DataFrames
- **Protocols over inheritance**: Use `typing.Protocol` for loose coupling
- **Fail fast**: Raise exceptions immediately on validation errors

---

## Testing Strategy

Each component has dedicated tests:

| Component Type | Test Location | Test Focus |
|---|---|---|
| Readers | `tests/readers/test_<format>.py` | Parse valid input, reject invalid input, produce correct IR |
| Writers | `tests/writers/test_<format>.py` | Serialize IR correctly, handle edge cases |
| Transforms | `tests/transforms/test_<transform>.py` | Correct mapping, preserve IR schema |
| Validators | `tests/validation/test_<validator>.py` | Detect violations, pass valid data |

**Fixtures:** Sample input files in `tests/fixtures/<format>/`

---

## Future Components

- **API readers** (REST, GraphQL)
- **Cloud writers** (S3, Azure Blob)
- **Transformation DSL** (declarative mapping language)
- **CLI interactive mode** (guided format selection)

---

## Related Documentation

- **C4 Context**: `docs/c4-context.md` (fintran in relation to users and external systems)
- **C4 Container**: `docs/c4-container.md` (runtime units: CLI, library, file system, DuckDB)
- **Architecture**: `.kiro/steering/architecture.md` (governance rules, design principles)
