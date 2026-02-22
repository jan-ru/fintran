# fintran

[![Release](https://img.shields.io/github/v/release/jan-ru/fintran)](https://github.com/jan-ru/fintran/releases)
[![Python](https://img.shields.io/badge/python-3.13+-blue.svg)](https://www.python.org/downloads/)
[![License](https://img.shields.io/github/license/jan-ru/fintran)](LICENSE)
[![Code style: ruff](https://img.shields.io/badge/code%20style-ruff-000000.svg)](https://github.com/astral-sh/ruff)
[![Type checked: mypy](https://img.shields.io/badge/type%20checked-mypy-blue.svg)](https://mypy-lang.org/)
[![Tests](https://img.shields.io/badge/tests-120%20passed-success)](tests/)
[![Coverage](https://img.shields.io/badge/coverage-99%25-brightgreen)](tests/coverage-report.md)

A high-performance financial document transformation tool built on a flexible reader → IR → writer pipeline pattern, designed using the C4 model for clear architectural documentation.

## Overview

fintran transforms financial data between different formats and storage systems using an Intermediate Representation (IR) approach. This architecture decouples data sources from destinations, making it easy to add new readers and writers without modifying existing code.

## Key Features

- **Pipeline Architecture**: Clean separation between readers, IR, and writers
- **Intermediate Representation**: Format-agnostic data model that decouples sources from destinations
- **High Performance**: Built on Polars and ConnectorX for fast data processing
- **Flexible I/O**: Support for multiple data sources (MS SQL Server) and formats (Parquet)
- **Metadata Preservation**: Embedded metadata in output files for traceability
- **Type Safety**: Full type hints with mypy validation
- **Comprehensive Testing**: Unit tests + property-based testing with Hypothesis

## Architecture

fintran follows the [C4 model](https://c4model.com) for architecture documentation. See `docs/` for detailed diagrams at each level (Context, Container, Component).

```
┌─────────────┐      ┌──────────────┐      ┌─────────────┐
│   Readers   │ ───> │      IR      │ ───> │   Writers   │
│             │      │  (Polars DF) │      │             │
│ - SQL       │      │  + Metadata  │      │ - Parquet   │
│ - CSV       │      │              │      │ - JSON      │
│ - API       │      │              │      │ - Database  │
└─────────────┘      └──────────────┘      └─────────────┘
```

The IR layer uses Polars DataFrames with associated metadata, providing a consistent interface for all transformations.

## Installation

Requires Python 3.13 or higher. We use [uv](https://github.com/astral-sh/uv) for fast dependency management.

```bash
# Install uv if you haven't already
curl -LsSf https://astral.sh/uv/install.sh | sh

# Clone the repository
git clone https://github.com/yourusername/fintran.git
cd fintran

# Install dependencies
uv sync
```

## Quick Start

```python
from fintran.readers.sql import SQLReader
from fintran.writers.parquet import ParquetWriter
from fintran.config import load_config

# Load configuration
config = load_config()

# Create reader and writer
reader = SQLReader(config.database)
writer = ParquetWriter(output_dir="./output")

# Read data into IR
ir_data = reader.read(table="transactions", query_params={"date": "2024-01-01"})

# Write IR to Parquet
writer.write(ir_data, filename="transactions_2024.parquet")
```

## Project Structure

```
fintran/
├── src/fintran/
│   ├── core/           # IR definitions and base classes
│   ├── readers/        # Data source readers (SQL, CSV, etc.)
│   ├── writers/        # Output format writers (Parquet, JSON, etc.)
│   ├── transforms/     # Data transformation utilities
│   └── config/         # Configuration management
├── tests/
│   ├── unit/          # Unit tests
│   ├── integration/   # Integration tests
│   └── property/      # Property-based tests with Hypothesis
├── docs/              # Documentation
├── pyproject.toml     # Project dependencies and configuration
└── README.md
```

## Development Setup

### Install Development Dependencies

```bash
# Install all dependencies including dev tools
uv sync --all-extras
```

### Running Tests

```bash
# Run all tests
uv run pytest

# Run with coverage
uv run pytest --cov=fintran --cov-report=html

# Run property-based tests only
uv run pytest tests/property/

# Run with verbose output
uv run pytest -v
```

### Code Quality

We use ruff for linting and formatting, and mypy for type checking:

```bash
# Format code
uv run ruff format .

# Lint code
uv run ruff check .

# Type check
uv run mypy src/fintran

# Run all quality checks
uv run ruff check . && uv run ruff format --check . && uv run mypy src/fintran
```

### Pre-commit Hooks

```bash
# Install pre-commit hooks
uv run pre-commit install

# Run hooks manually
uv run pre-commit run --all-files
```

## Technology Stack

- **[Polars](https://pola.rs/)**: Fast DataFrame library for data processing
- **[PyArrow](https://arrow.apache.org/docs/python/)**: Parquet file writing with metadata support
- **[ConnectorX](https://github.com/sfu-db/connector-x)**: High-performance database connector
- **[pytest](https://pytest.org/)**: Testing framework
- **[Hypothesis](https://hypothesis.readthedocs.io/)**: Property-based testing
- **[ruff](https://github.com/astral-sh/ruff)**: Fast Python linter and formatter
- **[mypy](https://mypy-lang.org/)**: Static type checker
- **[python-dotenv](https://github.com/theskumar/python-dotenv)**: Environment configuration

## Testing Approach

fintran uses a multi-layered testing strategy:

1. **Unit Tests**: Test individual components in isolation
2. **Integration Tests**: Test the full pipeline with real data sources
3. **Property-Based Tests**: Use Hypothesis to discover edge cases automatically

Example property-based test:

```python
from hypothesis import given
from hypothesis import strategies as st

@given(st.dataframes())
def test_ir_roundtrip(df):
    """Any DataFrame should survive IR conversion."""
    ir = IR.from_polars(df)
    result = ir.to_polars()
    assert_frame_equal(df, result)
```

## Contributing

We welcome contributions! Please follow these guidelines:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Make your changes with tests
4. Run quality checks (`ruff check`, `mypy`, `pytest`)
5. Commit your changes (`git commit -m 'Add amazing feature'`)
6. Push to the branch (`git push origin feature/amazing-feature`)
7. Open a Pull Request

### Code Standards

- Follow PEP 8 style guidelines (enforced by ruff)
- Add type hints to all functions
- Write tests for new features
- Update documentation as needed
- Keep commits atomic and well-described

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Roadmap

- [ ] Additional readers (CSV, JSON, REST APIs)
- [ ] Additional writers (Database, Cloud storage)
- [x] Data validation framework (in progress - core validators implemented)
- [ ] Transformation DSL
- [ ] CLI interface
- [ ] Web API
- [ ] Performance benchmarks
- [ ] Docker deployment

## Support

For questions, issues, or contributions, please open an issue on GitHub.
