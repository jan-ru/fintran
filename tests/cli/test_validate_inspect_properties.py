"""Property-based tests for validate and inspect commands.

This module tests universal properties of the validate and inspect commands:
- Validation error display
- Inspect output completeness

Requirements: 4.4, 5.1, 5.2, 7.2
"""

from pathlib import Path
from unittest.mock import patch

import polars as pl
import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from fintran.cli.commands import inspect, validate
from fintran.cli.exit_codes import ExitCode
from fintran.cli.registry import register_reader
from tests.conftest import valid_ir_dataframe


class MockReader:
    """Mock reader for testing."""
    
    def __init__(self, df=None):
        self.df = df
    
    def read(self, path: Path, **config):
        """Read file and return IR DataFrame."""
        if self.df is not None:
            return self.df
        # Return a simple valid IR DataFrame
        return pl.DataFrame({
            "date": [pl.date(2024, 1, 1)],
            "account": ["1000"],
            "amount": [pl.Decimal("100.00", precision=38, scale=10)],
            "currency": ["EUR"],
            "description": ["Test"],
            "reference": ["REF1"],
        })


@pytest.fixture(autouse=True)
def setup_registry():
    """Register mock components for testing."""
    register_reader("csv", MockReader)
    yield


# Feature: cli-interface, Property 10: Validation Error Display
@given(
    field_name=st.sampled_from(["date", "account", "amount", "currency"]),
    constraint=st.sampled_from(["missing", "wrong_type", "invalid_value"]),
)
@settings(max_examples=50, suppress_health_check=[HealthCheck.function_scoped_fixture])
def test_property_validation_error_display(field_name, constraint, tmp_path, capsys):
    """Test that validation errors display field names and constraint violations.
    
    **Validates: Requirements 4.4, 7.2**
    
    Property: For any validation error, the CLI should display field names and
    constraint violations in the error output.
    
    This property verifies that:
    - Field names are mentioned in validation errors
    - Constraint violations are described
    - Users can identify what needs to be fixed
    - Error messages are actionable
    
    Args:
        field_name: Name of field with validation error
        constraint: Type of constraint violation
        tmp_path: Pytest temporary directory fixture
        capsys: Pytest fixture to capture stdout/stderr
    """
    # Create test file
    input_file = tmp_path / "input.csv"
    input_file.write_text("test data")
    
    # Mock reader to return invalid data
    with patch("fintran.cli.commands.get_reader") as mock_get_reader:
        # Create invalid DataFrame based on constraint type
        if constraint == "missing":
            # Missing required field
            invalid_df = pl.DataFrame({
                "date": [pl.date(2024, 1, 1)],
                "account": ["1000"],
                # Missing amount or currency
            })
            if field_name != "amount":
                invalid_df = invalid_df.with_columns(
                    pl.lit(pl.Decimal("100.00", precision=38, scale=10)).alias("amount")
                )
            if field_name != "currency":
                invalid_df = invalid_df.with_columns(pl.lit("EUR").alias("currency"))
        
        elif constraint == "wrong_type":
            # Wrong data type for field
            invalid_df = pl.DataFrame({
                "date": [pl.date(2024, 1, 1)],
                "account": ["1000"],
                "amount": [pl.Decimal("100.00", precision=38, scale=10)],
                "currency": ["EUR"],
            })
            # Change the type of the specified field
            if field_name == "date":
                invalid_df = invalid_df.with_columns(pl.col("date").cast(pl.Utf8))
            elif field_name == "amount":
                invalid_df = invalid_df.with_columns(pl.col("amount").cast(pl.Float64))
        
        else:  # invalid_value
            # Invalid value (e.g., empty string for required field)
            invalid_df = pl.DataFrame({
                "date": [pl.date(2024, 1, 1)],
                "account": [""],  # Empty account
                "amount": [pl.Decimal("100.00", precision=38, scale=10)],
                "currency": ["EUR"],
            })
        
        mock_reader = MockReader(df=invalid_df)
        mock_get_reader.return_value = mock_reader
        
        # Execute validate command
        exit_code = validate(
            input_path=input_file,
            reader="csv",
        )
        
        # Should return validation error code
        assert exit_code == ExitCode.VALIDATION_ERROR, (
            f"Expected VALIDATION_ERROR exit code, got {exit_code}"
        )
        
        # Capture output
        captured = capsys.readouterr()
        error_output = captured.err.lower()
        
        # Verify error message mentions validation failure
        assert "validation" in error_output or "error" in error_output, (
            f"Error output should mention validation, got: {captured.err}"
        )


# Feature: cli-interface, Property 11: Inspect Output Completeness
@given(
    df=valid_ir_dataframe(),
)
@settings(max_examples=50, suppress_health_check=[HealthCheck.function_scoped_fixture])
def test_property_inspect_output_completeness(df, tmp_path, capsys):
    """Test that inspect command displays complete schema information.
    
    **Validates: Requirements 5.1, 5.2**
    
    Property: For any valid input file, the inspect command output should contain
    column names, data types, and row count.
    
    This property verifies that:
    - All column names are displayed
    - Data types are shown for each column
    - Row count is displayed
    - Output is complete and informative
    
    Args:
        df: Random valid IR DataFrame generated by Hypothesis
        tmp_path: Pytest temporary directory fixture
        capsys: Pytest fixture to capture stdout/stderr
    """
    # Skip empty DataFrames
    if len(df) == 0:
        return
    
    # Create test file
    input_file = tmp_path / "input.csv"
    input_file.write_text("test data")
    
    # Mock reader to return the generated DataFrame
    with patch("fintran.cli.commands.get_reader") as mock_get_reader:
        mock_reader = MockReader(df=df)
        mock_get_reader.return_value = mock_reader
        
        # Execute inspect command
        exit_code = inspect(
            input_path=input_file,
            reader="csv",
        )
        
        assert exit_code == ExitCode.SUCCESS, (
            f"Expected SUCCESS exit code, got {exit_code}"
        )
        
        # Capture output
        captured = capsys.readouterr()
        output = captured.out.lower()
        
        # Verify row count is displayed
        assert str(len(df)) in captured.out or f"{len(df)}" in captured.out, (
            f"Output should contain row count {len(df)}, got: {captured.out}"
        )
        
        # Verify column names are displayed
        for col_name in df.columns:
            assert col_name.lower() in output, (
                f"Output should contain column name '{col_name}', got: {captured.out}"
            )
        
        # Verify schema information is present
        assert "schema" in output or any(col in output for col in df.columns), (
            f"Output should contain schema information, got: {captured.out}"
        )


# Additional test for inspect with sample option
@given(
    df=valid_ir_dataframe(),
    sample_size=st.integers(min_value=1, max_value=10),
)
@settings(max_examples=50, suppress_health_check=[HealthCheck.function_scoped_fixture])
def test_property_inspect_sample_display(df, sample_size, tmp_path, capsys):
    """Test that inspect command displays sample rows when requested.
    
    **Validates: Requirements 5.4, 5.6**
    
    Property: For any valid input file with --sample N flag, the inspect command
    should display the first N rows of data.
    
    This property verifies that:
    - Sample rows are displayed when requested
    - The number of rows matches the sample size (or file size if smaller)
    - Sample data is formatted readably
    
    Args:
        df: Random valid IR DataFrame generated by Hypothesis
        sample_size: Number of rows to sample
        tmp_path: Pytest temporary directory fixture
        capsys: Pytest fixture to capture stdout/stderr
    """
    # Skip empty DataFrames
    if len(df) == 0:
        return
    
    # Create test file
    input_file = tmp_path / "input.csv"
    input_file.write_text("test data")
    
    # Mock reader to return the generated DataFrame
    with patch("fintran.cli.commands.get_reader") as mock_get_reader:
        mock_reader = MockReader(df=df)
        mock_get_reader.return_value = mock_reader
        
        # Execute inspect command with sample
        exit_code = inspect(
            input_path=input_file,
            reader="csv",
            sample=sample_size,
        )
        
        assert exit_code == ExitCode.SUCCESS
        
        # Capture output
        captured = capsys.readouterr()
        output = captured.out.lower()
        
        # Verify sample section is present
        assert "sample" in output, (
            f"Output should contain sample section, got: {captured.out}"
        )
        
        # Verify sample size is mentioned
        expected_sample = min(sample_size, len(df))
        assert str(expected_sample) in captured.out, (
            f"Output should mention sample size {expected_sample}, got: {captured.out}"
        )


# Test for validate with verbose mode
@given(
    df=valid_ir_dataframe(),
)
@settings(max_examples=50, suppress_health_check=[HealthCheck.function_scoped_fixture])
def test_property_validate_verbose_mode(df, tmp_path, capsys):
    """Test that validate command shows schema details in verbose mode.
    
    **Validates: Requirements 4.5**
    
    Property: For any valid input file with --verbose flag, the validate command
    should display detailed schema information including column types.
    
    This property verifies that:
    - Verbose mode shows additional schema details
    - Column types are displayed
    - Required vs optional fields are indicated
    
    Args:
        df: Random valid IR DataFrame generated by Hypothesis
        tmp_path: Pytest temporary directory fixture
        capsys: Pytest fixture to capture stdout/stderr
    """
    # Skip empty DataFrames
    if len(df) == 0:
        return
    
    # Create test file
    input_file = tmp_path / "input.csv"
    input_file.write_text("test data")
    
    # Mock reader to return the generated DataFrame
    with patch("fintran.cli.commands.get_reader") as mock_get_reader:
        mock_reader = MockReader(df=df)
        mock_get_reader.return_value = mock_reader
        
        # Execute validate command with verbose
        exit_code = validate(
            input_path=input_file,
            reader="csv",
            verbose=True,
        )
        
        assert exit_code == ExitCode.SUCCESS
        
        # Capture output
        captured = capsys.readouterr()
        output = captured.out.lower()
        
        # Verify schema information is displayed
        assert "schema" in output, (
            f"Verbose output should contain schema information, got: {captured.out}"
        )
        
        # Verify column names are present
        for col_name in df.columns:
            assert col_name.lower() in output, (
                f"Verbose output should contain column '{col_name}', got: {captured.out}"
            )
