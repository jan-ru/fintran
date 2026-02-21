"""Protocol definitions for fintran pipeline components.

This module defines the interfaces that all format-specific readers, writers,
and transforms must implement. These protocols ensure a consistent interface
across the pipeline and enable type checking.

Protocols:
    - Reader: Parses source files and produces validated IR DataFrames
    - Writer: Serializes IR DataFrames to target format files
    - Transform: Applies transformations to IR DataFrames

All implementations must:
    - Follow the IR schema defined in fintran.core.schema
    - Raise descriptive errors from fintran.core.exceptions
    - Maintain immutability (never mutate input DataFrames)
    - Produce validated IR output
"""

from pathlib import Path
from typing import Any, Protocol

import polars as pl


class Reader(Protocol):
    """Protocol for format-specific readers.

    Readers parse source files in various formats (CSV, Parquet, SQL, etc.)
    and produce validated IR DataFrames. All implementations must:

    1. Parse the input file and convert to IR schema
    2. Validate the output IR DataFrame before returning
    3. Raise ReaderError for malformed input with descriptive messages
    4. Support optional configuration parameters for format-specific options

    Requirements:
        - Requirement 3.1: Define Reader protocol with read method
        - Requirement 3.2: Support optional configuration parameters
        - Requirement 3.3: Raise descriptive errors for malformed input
        - Requirement 3.4: Produce validated IR output

    Example:
        >>> class CSVReader:
        ...     def read(self, path: Path, **config: Any) -> pl.DataFrame:
        ...         df = pl.read_csv(path, **config)
        ...         ir = convert_to_ir(df)
        ...         return validate_ir(ir)
        ...
        >>> reader = CSVReader()
        >>> ir = reader.read(Path("transactions.csv"), delimiter=",")
    """

    def read(self, path: Path, **config: Any) -> pl.DataFrame:
        """Read a file and return a validated IR DataFrame.

        Args:
            path: Path to the input file to parse
            **config: Format-specific configuration options (e.g., delimiter
                     for CSV, connection string for SQL, compression for Parquet)

        Returns:
            Validated IR DataFrame conforming to the canonical schema

        Raises:
            ReaderError: If parsing fails due to malformed input, missing file,
                        invalid format, or other reading errors. Error should
                        include context about the file path and specific failure.

        Example:
            >>> reader = CSVReader()
            >>> ir = reader.read(Path("data.csv"), delimiter="|")
            >>> ir.columns
            ['date', 'account', 'amount', 'currency', 'description', 'reference']
        """
        ...


class Writer(Protocol):
    """Protocol for format-specific writers.

    Writers serialize IR DataFrames to target format files (CSV, Parquet, SQL, etc.).
    All implementations must:

    1. Validate the input IR DataFrame before writing
    2. Serialize the IR to the target format
    3. Raise WriterError for write failures with descriptive messages
    4. Support optional configuration parameters for format-specific options

    Requirements:
        - Requirement 4.1: Define Writer protocol with write method
        - Requirement 4.2: Support optional configuration parameters
        - Requirement 4.3: Raise descriptive errors for write failures
        - Requirement 4.4: Validate IR input before writing

    Example:
        >>> class ParquetWriter:
        ...     def write(self, df: pl.DataFrame, path: Path, **config: Any) -> None:
        ...         validate_ir(df)
        ...         df.write_parquet(path, **config)
        ...
        >>> writer = ParquetWriter()
        >>> writer.write(ir, Path("output.parquet"), compression="snappy")
    """

    def write(self, df: pl.DataFrame, path: Path, **config: Any) -> None:
        """Write a validated IR DataFrame to a file.

        Args:
            df: Validated IR DataFrame to serialize
            path: Path to the output file to create
            **config: Format-specific configuration options (e.g., delimiter
                     for CSV, compression for Parquet, table name for SQL)

        Returns:
            None

        Raises:
            WriterError: If writing fails due to invalid path, permission errors,
                        disk space issues, or serialization errors. Error should
                        include context about the output path and specific failure.

        Example:
            >>> writer = ParquetWriter()
            >>> writer.write(ir, Path("output.parquet"), compression="zstd")
        """
        ...


class Transform(Protocol):
    """Protocol for IR transformations.

    Transforms apply optional enrichment or modification steps to IR DataFrames
    in the pipeline. All implementations must:

    1. Not mutate the input DataFrame (return a new DataFrame)
    2. Return validated IR output conforming to the schema
    3. Be deterministic (same input always produces same output)
    4. Raise TransformError for transformation failures

    Requirements:
        - Requirement 5.1: Define Transform protocol with transform method
        - Requirement 5.2: Must not mutate input DataFrame
        - Requirement 5.3: Must return validated IR output
        - Requirement 5.4: Must be deterministic
        - Requirement 7.1: Document IR immutability requirement
        - Requirement 7.2: Require returning new DataFrames
        - Requirement 7.3: Verify input DataFrame is not modified

    Example:
        >>> class CurrencyNormalizer:
        ...     def transform(self, df: pl.DataFrame) -> pl.DataFrame:
        ...         # Create new DataFrame, don't mutate input
        ...         normalized = df.with_columns(
        ...             pl.col("currency").str.to_uppercase()
        ...         )
        ...         return validate_ir(normalized)
        ...
        >>> transform = CurrencyNormalizer()
        >>> result = transform.transform(ir)
        >>> result is not ir  # New DataFrame, not mutated
        True
    """

    def transform(self, df: pl.DataFrame) -> pl.DataFrame:
        """Transform an IR DataFrame.

        This method must create and return a new DataFrame rather than modifying
        the input. The transformation must be deterministic: applying the same
        transform multiple times to the same input must produce equivalent results.

        Args:
            df: Input IR DataFrame to transform

        Returns:
            Transformed IR DataFrame (new instance, not mutated input)

        Raises:
            TransformError: If transformation fails due to invalid data,
                           computation errors, or other transformation issues.
                           Error should include context about the transform
                           and specific failure.

        Example:
            >>> transform = FilterPositiveAmounts()
            >>> result = transform.transform(ir)
            >>> result is not ir  # Immutability: new DataFrame returned
            True
            >>> len(result) <= len(ir)  # Transforms may reduce row count
            True
        """
        ...
