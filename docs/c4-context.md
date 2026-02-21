# fintran — C4 Level 1: System Context

## Description

This document describes fintran in relation to its users and the external systems it interacts with.

```
┌─────────────────────────────────────────────────────────────────────┐
│                        System Context                               │
│                                                                     │
│   [User: Consultant / Student]                                      │
│          │                                                          │
│          │ runs CLI commands                                        │
│          ▼                                                          │
│   ┌─────────────┐                                                   │
│   │   fintran   │  Financial document transformation tool          │
│   └─────────────┘                                                   │
│     │         │         │         │         │                       │
│     ▼         ▼         ▼         ▼         ▼                       │
│  [hledger] [Excel]  [CSV files] [Twinfield] [DuckDB]               │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

## People

| Person | Role | Interaction |
|---|---|---|
| Consultant | Primary user (Jan-Ru) | Transforms financial exports between client systems and local tooling |
| Student | Secondary user | Uses fintran in coursework for financial data exercises |

## Internal System

| System | Description |
|---|---|
| **fintran** | CLI tool that reads financial documents in one format and writes them in another, via a normalized Intermediate Representation |

## External Systems

| System | Type | Relationship |
|---|---|---|
| **hledger** | Plain-text accounting tool | fintran reads and writes `.journal` files; hledger validates and reports on them |
| **Excel** | Spreadsheet application | fintran reads bookkeeping workbooks and writes result sheets |
| **CSV files** | Generic flat file format | Used as both source and target for generic tabular financial data |
| **Twinfield** | Cloud accounting ERP (Wolters Kluwer) | fintran reads Twinfield transaction/trial balance exports and writes Twinfield import files |
| **DuckDB** | Embedded analytical database | fintran writes normalized balance tables for SQL querying and Power BI integration |

## Out of Scope (C1)

- Authentication or authorization (fintran has no user accounts)
- Network connectivity (all I/O is local file system)
- SBR/XBRL mapping (planned future extension)
