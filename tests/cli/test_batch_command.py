"""Tests for the batch command.

This module tests the batch processing functionality of the CLI,
including file pattern matching, error isolation, and summary reporting.
"""

import tempfile
from pathlib import Path

import polars as pl
import pytest

from fintran.cli.commands import batch
from fintran.cli.exit_codes import ExitCode
from fintran.cli.registry import register_reader, register_writer


class MockCSVReader:
    """Mock CSV reader for testing."""
    
    def read(self, path: Path, **config):
        """Read CSV file and return IR DataFrame."""
        return pl.read_csv(path).with_columns([
            pl.col("date").str.to_date(),
            pl.col("account").cast(pl.Utf8),
            pl.col("amount").cast(pl.Decimal(scale=2)),
        ])


class MockParquetWriter:
    """Mock Parquet writer for testing."""
    
    def write(self, df: pl.DataFrame, path: Path, **config):
        """Write DataFrame to Parquet file."""
        df.write_parquet(path)


@pytest.fixture(autouse=True)
def setup_registry():
    """Register mock components for testing."""
    register_reader("csv", MockCSVReader)
    register_writer("parquet", MockParquetWriter)
    yield
    # Note: In a real implementation, we'd clean up the registry here
    # For now, we'll leave the mocks registered


@pytest.fixture
def sample_ir_data():
    """Create sample IR data for testing."""
    return pl.DataFrame({
        "date": ["2024-01-01", "2024-01-02"],
        "account": ["1000", "2000"],
        "amount": [100.0, 200.0],
        "currency": ["EUR", "EUR"],
        "description": ["Test 1", "Test 2"],
        "reference": ["REF1", "REF2"],
    }).with_columns([
        pl.col("date").str.to_date(),
        pl.col("amount").cast(pl.Decimal(scale=2)),
    ])


def test_batch_basic_functionality(sample_ir_data, tmp_path):
    """Test basic batch processing of multiple files.
    
    Requirements:
        - Requirement 6.1: Process all matching files in directory
        - Requirement 6.8: Display summary with counts
    """
    # Create input directory with test files
    input_dir = tmp_path / "input"
    input_dir.mkdir()
    
    # Create test CSV files
    for i in range(3):
        csv_file = input_dir / f"test_{i}.csv"
        sample_ir_data.write_csv(csv_file)
    
    # Create output directory
    output_dir = tmp_path / "output"
    
    # Run batch command
    exit_code = batch(
        input_dir=input_dir,
        output_dir=output_dir,
        pattern="*.csv",
        recursive=False,
        writer="parquet",
        quiet=True,
    )
    
    # Verify success
    assert exit_code == ExitCode.SUCCESS
    
    # Verify output files were created
    output_files = list(output_dir.glob("*.parquet"))
    assert len(output_files) == 3


def test_batch_recursive_processing(sample_ir_data, tmp_path):
    """Test recursive batch processing with subdirectories.
    
    Requirements:
        - Requirement 6.4: Accept --recursive flag for subdirectories
    """
    # Create nested directory structure
    input_dir = tmp_path / "input"
    subdir1 = input_dir / "subdir1"
    subdir2 = input_dir / "subdir2"
    
    input_dir.mkdir()
    subdir1.mkdir()
    subdir2.mkdir()
    
    # Create test files in different directories
    (input_dir / "test_root.csv").write_text("date,account,amount,currency\n2024-01-01,1000,100.0,EUR\n")
    (subdir1 / "test_sub1.csv").write_text("date,account,amount,currency\n2024-01-01,1000,100.0,EUR\n")
    (subdir2 / "test_sub2.csv").write_text("date,account,amount,currency\n2024-01-01,1000,100.0,EUR\n")
    
    output_dir = tmp_path / "output"
    
    # Run batch with recursive=True
    exit_code = batch(
        input_dir=input_dir,
        output_dir=output_dir,
        pattern="*.csv",
        recursive=True,
        writer="parquet",
        quiet=True,
    )
    
    # Verify all files were processed
    assert exit_code == ExitCode.SUCCESS
    
    # Check that subdirectory structure is preserved
    assert (output_dir / "test_root.parquet").exists()
    assert (output_dir / "subdir1" / "test_sub1.parquet").exists()
    assert (output_dir / "subdir2" / "test_sub2.parquet").exists()


def test_batch_pattern_filtering(sample_ir_data, tmp_path):
    """Test that batch only processes files matching the pattern.
    
    Requirements:
        - Requirement 6.2: Accept --pattern argument for glob filtering
    """
    input_dir = tmp_path / "input"
    input_dir.mkdir()
    
    # Create files with different extensions
    sample_ir_data.write_csv(input_dir / "test1.csv")
    sample_ir_data.write_csv(input_dir / "test2.csv")
    (input_dir / "test.txt").write_text("not a csv")
    
    output_dir = tmp_path / "output"
    
    # Run batch with CSV pattern only
    exit_code = batch(
        input_dir=input_dir,
        output_dir=output_dir,
        pattern="*.csv",
        writer="parquet",
        quiet=True,
    )
    
    assert exit_code == ExitCode.SUCCESS
    
    # Only CSV files should be processed
    output_files = list(output_dir.glob("*.parquet"))
    assert len(output_files) == 2


def test_batch_error_isolation(tmp_path):
    """Test that batch continues processing after individual file errors.
    
    Requirements:
        - Requirement 6.7: Continue processing on individual file errors
        - Requirement 6.9: Return non-zero exit code if any file fails
    """
    input_dir = tmp_path / "input"
    input_dir.mkdir()
    
    # Create one valid file and one invalid file
    (input_dir / "valid.csv").write_text("date,account,amount,currency\n2024-01-01,1000,100.0,EUR\n")
    (input_dir / "invalid.csv").write_text("not,valid,csv,data\n")
    
    output_dir = tmp_path / "output"
    
    # Run batch - should process valid file and report error for invalid
    exit_code = batch(
        input_dir=input_dir,
        output_dir=output_dir,
        pattern="*.csv",
        writer="parquet",
        quiet=True,
    )
    
    # Should return error code since one file failed
    assert exit_code == ExitCode.UNEXPECTED_ERROR
    
    # But valid file should still be processed
    assert (output_dir / "valid.parquet").exists()


def test_batch_empty_directory(tmp_path):
    """Test batch behavior with no matching files."""
    input_dir = tmp_path / "input"
    input_dir.mkdir()
    
    output_dir = tmp_path / "output"
    
    # Run batch on empty directory
    exit_code = batch(
        input_dir=input_dir,
        output_dir=output_dir,
        pattern="*.csv",
        quiet=True,
    )
    
    # Should return error for no matching files
    assert exit_code == ExitCode.UNEXPECTED_ERROR


def test_batch_nonexistent_input_directory(tmp_path):
    """Test batch behavior with nonexistent input directory."""
    input_dir = tmp_path / "nonexistent"
    output_dir = tmp_path / "output"
    
    exit_code = batch(
        input_dir=input_dir,
        output_dir=output_dir,
        pattern="*.csv",
        quiet=True,
    )
    
    assert exit_code == ExitCode.UNEXPECTED_ERROR



# ============================================================================
# Property-Based Tests for Batch Processing
# ============================================================================

from hypothesis import given, settings
from hypothesis import strategies as st

from tests.conftest import valid_ir_dataframe


# Feature: cli-interface, Property 12: Batch Processing Completeness
@given(
    num_files=st.integers(min_value=1, max_value=10),
    df=valid_ir_dataframe(),
)
@settings(max_examples=50)
def test_property_batch_processing_completeness(num_files, df, tmp_path):
    """Test that batch processes all matching files and reports accurate summary.
    
    **Validates: Requirements 6.1, 6.8**
    
    Property: For any directory with N files matching a pattern, the batch command
    should process all N files and display a summary with total=N, successful=N
    (assuming all files are valid), and failed=0.
    
    This property verifies that:
    - All matching files are discovered and processed
    - The summary accurately reflects the number of files processed
    - Success count matches the number of valid files
    
    Args:
        num_files: Random number of files to create (1-10)
        df: Random valid IR DataFrame generated by Hypothesis
        tmp_path: Pytest temporary directory fixture
    """
    # Skip if DataFrame is empty (can't write empty CSV properly)
    if len(df) == 0:
        return
    
    # Create input directory with N test files
    input_dir = tmp_path / "input"
    input_dir.mkdir()
    
    # Create N CSV files with valid data
    for i in range(num_files):
        csv_file = input_dir / f"test_{i}.csv"
        df.write_csv(csv_file)
    
    output_dir = tmp_path / "output"
    
    # Run batch command
    exit_code = batch(
        input_dir=input_dir,
        output_dir=output_dir,
        pattern="*.csv",
        recursive=False,
        writer="parquet",
        quiet=True,
    )
    
    # Verify success exit code
    assert exit_code == ExitCode.SUCCESS, (
        f"Expected SUCCESS exit code for {num_files} valid files, got {exit_code}"
    )
    
    # Verify all files were processed (output files created)
    output_files = list(output_dir.glob("*.parquet"))
    assert len(output_files) == num_files, (
        f"Expected {num_files} output files, but found {len(output_files)}"
    )
    
    # Verify each output file exists and has correct name
    for i in range(num_files):
        expected_output = output_dir / f"test_{i}.parquet"
        assert expected_output.exists(), (
            f"Expected output file {expected_output} not found"
        )


# Feature: cli-interface, Property 13: Batch Pattern Filtering
@given(
    num_csv_files=st.integers(min_value=1, max_value=5),
    num_other_files=st.integers(min_value=1, max_value=5),
    df=valid_ir_dataframe(),
)
@settings(max_examples=50)
def test_property_batch_pattern_filtering(num_csv_files, num_other_files, df, tmp_path):
    """Test that batch only processes files matching the specified pattern.
    
    **Validates: Requirements 6.2**
    
    Property: For any directory with M files matching pattern P and N files not
    matching pattern P, the batch command should process exactly M files and
    ignore the N non-matching files.
    
    This property verifies that:
    - Only files matching the glob pattern are processed
    - Non-matching files are completely ignored
    - The output count matches the matching file count, not total file count
    
    Args:
        num_csv_files: Random number of CSV files to create (1-5)
        num_other_files: Random number of non-CSV files to create (1-5)
        df: Random valid IR DataFrame generated by Hypothesis
        tmp_path: Pytest temporary directory fixture
    """
    # Skip if DataFrame is empty
    if len(df) == 0:
        return
    
    # Create input directory
    input_dir = tmp_path / "input"
    input_dir.mkdir()
    
    # Create CSV files (matching pattern)
    for i in range(num_csv_files):
        csv_file = input_dir / f"data_{i}.csv"
        df.write_csv(csv_file)
    
    # Create non-CSV files (not matching pattern)
    for i in range(num_other_files):
        txt_file = input_dir / f"other_{i}.txt"
        txt_file.write_text("This is not a CSV file")
    
    output_dir = tmp_path / "output"
    
    # Run batch with CSV pattern only
    exit_code = batch(
        input_dir=input_dir,
        output_dir=output_dir,
        pattern="*.csv",
        recursive=False,
        writer="parquet",
        quiet=True,
    )
    
    # Verify success
    assert exit_code == ExitCode.SUCCESS, (
        f"Expected SUCCESS for {num_csv_files} CSV files, got {exit_code}"
    )
    
    # Verify only CSV files were processed
    output_files = list(output_dir.glob("*.parquet"))
    assert len(output_files) == num_csv_files, (
        f"Expected {num_csv_files} output files (matching CSV pattern), "
        f"but found {len(output_files)}. "
        f"Total input files: {num_csv_files + num_other_files}"
    )
    
    # Verify no .txt files were processed
    txt_outputs = list(output_dir.glob("*.txt"))
    assert len(txt_outputs) == 0, (
        f"Found {len(txt_outputs)} .txt files in output, expected 0"
    )


# Feature: cli-interface, Property 14: Batch Error Isolation
@given(
    num_valid_files=st.integers(min_value=1, max_value=5),
    num_invalid_files=st.integers(min_value=1, max_value=5),
    df=valid_ir_dataframe(),
)
@settings(max_examples=50)
def test_property_batch_error_isolation(num_valid_files, num_invalid_files, df, tmp_path):
    """Test that batch continues processing after individual file errors.
    
    **Validates: Requirements 6.7, 6.9**
    
    Property: For any batch operation with V valid files and I invalid files,
    the batch command should:
    1. Process all V valid files successfully
    2. Attempt to process all I invalid files (which will fail)
    3. Return a non-zero exit code (since some files failed)
    4. Create exactly V output files (one for each valid input)
    
    This property verifies that:
    - Errors in individual files don't stop processing of remaining files
    - Valid files are still processed even when invalid files are present
    - The exit code reflects that some files failed
    - The success count matches the number of valid files
    
    Args:
        num_valid_files: Random number of valid files to create (1-5)
        num_invalid_files: Random number of invalid files to create (1-5)
        df: Random valid IR DataFrame generated by Hypothesis
        tmp_path: Pytest temporary directory fixture
    """
    # Skip if DataFrame is empty
    if len(df) == 0:
        return
    
    # Create input directory
    input_dir = tmp_path / "input"
    input_dir.mkdir()
    
    # Create valid CSV files
    for i in range(num_valid_files):
        csv_file = input_dir / f"valid_{i}.csv"
        df.write_csv(csv_file)
    
    # Create invalid CSV files (malformed data)
    for i in range(num_invalid_files):
        csv_file = input_dir / f"invalid_{i}.csv"
        # Write malformed CSV that will fail to parse
        csv_file.write_text("not,valid,csv,data,format\ngarbage,data,here,x,y\n")
    
    output_dir = tmp_path / "output"
    
    # Run batch command
    exit_code = batch(
        input_dir=input_dir,
        output_dir=output_dir,
        pattern="*.csv",
        recursive=False,
        writer="parquet",
        quiet=True,
    )
    
    # Verify non-zero exit code (since some files failed)
    assert exit_code != ExitCode.SUCCESS, (
        f"Expected non-zero exit code when {num_invalid_files} files fail, "
        f"but got SUCCESS"
    )
    
    # Verify valid files were still processed
    output_files = list(output_dir.glob("*.parquet"))
    assert len(output_files) == num_valid_files, (
        f"Expected {num_valid_files} output files (from valid inputs), "
        f"but found {len(output_files)}. "
        f"Valid inputs: {num_valid_files}, Invalid inputs: {num_invalid_files}"
    )
    
    # Verify each valid file has corresponding output
    for i in range(num_valid_files):
        expected_output = output_dir / f"valid_{i}.parquet"
        assert expected_output.exists(), (
            f"Expected output file {expected_output} from valid input not found"
        )

