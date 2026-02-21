# Context7 Library References

This file lists Context7 library IDs for the key dependencies used in this project. These can be queried via the Context7 MCP server for up-to-date documentation and code examples.

## Core Data Processing Libraries

### Polars
- **Library ID**: `/pola-rs/polars`
- **Purpose**: Fast DataFrame library for data processing (faster than pandas)
- **Usage**: Primary data manipulation, reading from ConnectorX, preparing for Parquet export
- **Key Features**: Lazy evaluation, parallel processing, Arrow-native

### PyArrow
- **Library ID**: `/apache/arrow`
- **Purpose**: Parquet file writing with embedded metadata
- **Usage**: Converting Polars DataFrames to Parquet format with custom metadata
- **Key Features**: Columnar format, compression, metadata support

### ConnectorX
- **Library ID**: `/sfu-db/connector-x`
- **Purpose**: High-performance database connector for MS SQL Server
- **Usage**: Reading tables from MS SQL Server into Polars DataFrames
- **Key Features**: Zero-copy, parallel loading, multiple database support


## Testing & Quality

### pytest
- **Library ID**: `/pytest-dev/pytest`
- **Purpose**: Testing framework
- **Usage**: Unit tests, integration tests, test fixtures
- **Key Features**: Simple syntax, powerful fixtures, plugin ecosystem

### Hypothesis
- **Library ID**: `/HypothesisWorks/hypothesis`
- **Purpose**: Property-based testing
- **Usage**: Generating test cases for edge case discovery
- **Key Features**: Automatic test case generation, shrinking, stateful testing

### Ruff
- **Library ID**: `/astral-sh/ruff`
- **Purpose**: Fast Python linter and formatter
- **Usage**: Code quality enforcement in pre-commit hooks
- **Key Features**: Fast, comprehensive rules, auto-fix

## Configuration & Environment

### python-dotenv
- **Library ID**: `/theskumar/python-dotenv`
- **Purpose**: Environment variable loading from .env files
- **Usage**: Loading database credentials and configuration
- **Key Features**: Simple API, .env file support

## How to Use These References

When working with these libraries, you can query Context7 for documentation:

1. Use the `mcp_context7_query_docs` tool with the library ID
2. Ask specific questions about API usage, best practices, or examples
3. Get up-to-date documentation that may be newer than training data

### Example Query Pattern

```
Query: "How to write Parquet files with custom metadata using PyArrow"
Library ID: /apache/arrow
```

## Version Notes

This project uses:
- Python 3.13+ (target 3.14 in Docker)
- Latest stable versions of all libraries (managed via `uv`)
- See `pyproject.toml` for exact version constraints

## Related Documentation

- Project dependencies: `pyproject.toml`
- Tech stack details: `.kiro/steering/tech.md`
- Architecture overview: `docs/architecture.md`
