# fintran — Use Cases

## UC-01: CSV → hledger journal

**Description:** Convert a generic CSV file with financial transactions into an hledger plain-text journal.

**Input:** CSV file with columns for date, account, amount, currency, and optional description.

**Output:** hledger `.journal` file with one transaction per row.

**CLI:**
```bash
fintran --from csv --to hledger transactions.csv -o journal.journal
```

**Notes:**
- Date format must be configurable (ISO, Dutch locale, etc.)
- Double-entry balancing is not enforced in this direction; hledger will validate on import.

---

## UC-02: hledger journal → CSV

**Description:** Export an hledger journal to a flat CSV file for further processing or reporting.

**Input:** hledger `.journal` file.

**Output:** CSV with normalized IR columns.

**CLI:**
```bash
fintran --from hledger --to csv journal.journal -o transactions.csv
```

**Notes:**
- Multi-posting transactions are flattened to one row per posting.

---

## UC-03: Excel → hledger journal

**Description:** Convert a bookkeeping spreadsheet (e.g. a student or client workbook) into an hledger journal.

**Input:** `.xlsx` file with a tabular transaction list on a named sheet.

**Output:** hledger `.journal` file.

**CLI:**
```bash
fintran --from excel --to hledger --sheet "Transactions" workbook.xlsx -o journal.journal
```

**Notes:**
- Sheet name is configurable via flag.
- Column mapping may require a config file for non-standard layouts.

---

## UC-04: hledger journal → Excel

**Description:** Export hledger postings to an Excel workbook, useful for sharing with non-CLI users or for further analysis.

**Input:** hledger `.journal` file.

**Output:** `.xlsx` file with one sheet containing the normalized IR.

**CLI:**
```bash
fintran --from hledger --to excel journal.journal -o report.xlsx
```

---

## UC-05: Twinfield Export → hledger journal

**Description:** Convert a Twinfield trial balance or transaction export (CSV/XAF) into an hledger journal for local analysis.

**Input:** Twinfield CSV export (transaction list or trial balance).

**Output:** hledger `.journal` file.

**CLI:**
```bash
fintran --from twinfield --to hledger twinfield_export.csv -o journal.journal
```

**Notes:**
- Twinfield-specific fields (office code, dimension codes) are mapped to hledger account names.
- Amount sign convention (debit positive) is normalized during reading.

---

## UC-06: Twinfield Export → DuckDB balances table

**Description:** Load a Twinfield trial balance export into a DuckDB database table for SQL-based analysis or Power BI integration.

**Input:** Twinfield CSV trial balance export.

**Output:** DuckDB database file with a `balances` table in IR schema.

**CLI:**
```bash
fintran --from twinfield --to duckdb twinfield_tb.csv -o reporting.duckdb
```

**Notes:**
- Subsequent runs append or upsert by period/account key (configurable).
- The DuckDB file can be queried directly by DuckDB-WASM or Power BI.

---

## UC-07: Excel → Twinfield import

**Description:** Convert a structured Excel workbook into a Twinfield-compatible import CSV for batch journal entry upload.

**Input:** `.xlsx` file with transactions in a known layout.

**Output:** Twinfield import CSV matching the required column spec.

**CLI:**
```bash
fintran --from excel --to twinfield workbook.xlsx -o twinfield_import.csv
```

**Notes:**
- Twinfield import format requires specific column names, date formats, and office codes.
- Validation errors are reported before writing output.

---

## Future Use Cases (backlog)

- Twinfield XAF → hledger
- hledger → SBR/XBRL concept mapping
- CSV → DuckDB balances table (generic)
