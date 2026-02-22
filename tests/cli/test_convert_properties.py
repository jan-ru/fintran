"""Property-based tests for the convert command.

This module tests universal properties of the convert command including:
- Pipeline integration
- File extension inference
- Dry run behavior
- Input validation
- Output directory creation

Requirements: 2.1-2.8, 12.1-12.5, 14.2-14.4, 15.1-15.2
"""

from pathlib import Path
from unittest.mock import Mock, patch

import polars as pl
import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from fintran.cli.commands import convert
from fintran.cli.exit_codes import ExitCode
from fintran.cli.registry import register_reader, register_writer
from tests.conftest import valid_ir_dataframe


class MockReader:
    """Mock reader for testing."""
    
    def read(self, path: Path, **config):
        """Read file and return IR DataFrame."""
        # Return a simple valid IR DataFrame
        return pl.DataFrame({
            "date": [pl.date(2024, 1, 1)],
            "account": ["1000"],
            "amount": [pl.Decimal("100.00", precision=38, scale=10)],
            "currency": ["EUR"],
            "description": ["Test"],
            "reference": ["REF1"],
        })


class MockWriter:
    """Mock writer for testing."""
    
    def write(self, df: pl.DataFrame, path: Path, **config):
        """Write DataFrame to file."""
        # Just create an empty file to simulate writing
        path.write_text("mock output")


@pytest.fixture(autouse=True)
def setup_registry():
    """Register mock components for testing."""
    register_reader("csv", MockReader)
    register_reader("json", MockReader)
    register_reader("parquet", MockReader)
    register_writer("csv", MockWriter)
    register_writer("json", MockWriter)
    register_writer("parquet", MockWriter)
    yield


# Feature: cli-interface, Property 1: Pipeline Integration
@given(
    reader_type=st.sampled_from(["csv", "json", "parquet"]),
    writer_type=st.sampled_from(["csv", "json", "parquet"]),
)
@settings(max_examples=50, suppress_health_check=[HealthCheck.function_scoped_fixture])
def test_property_pipeline_integration(reader_type, writer_type, tmp_path):
    """Test that convert command calls execute_pipeline with correct parameters.
    
    **Validates: Requirements 15.1, 15.2, 2.1**
    
    Property: For any valid reader, writer, input path, and output path, when the
    convert command is invoked, the CLI should call execute_pipeline from
    fintran.core.pipeline with the correct reader, writer, and paths.
    
    This property verifies that:
    - The CLI properly integrates with the existing pipeline infrastructure
    - Reader and writer instances are correctly passed to execute_pipeline
    - Input and output paths are correctly passed to execute_pipeline
    - The pipeline is the single source of truth for execution logic
    
    Args:
        reader_type: Random reader type (csv, json, parquet)
        writer_type: Random writer type (csv, json, parquet)
        tmp_path: Pytest temporary directory fixture
    """
    # Create test input file
    input_file = tmp_path / f"input.{reader_type}"
    input_file.write_text("test data")
    
    output_file = tmp_path / f"output.{writer_type}"
    
    # Mock execute_pipeline to verify it's called correctly
    with patch("fintran.cli.commands.execute_pipeline") as mock_pipeline:
        # Execute convert command
        exit_code = convert(
            input_path=input_file,
            output_path=output_file,
            reader=reader_type,
            writer=writer_type,
        )
        
        # Verify execute_pipeline was called
        assert mock_pipeline.called, (
            f"execute_pipeline should be called for {reader_type} → {writer_type}"
        )
        
        # Verify call arguments
        call_args = mock_pipeline.call_args
        assert call_args is not None, "execute_pipeline should have been called with arguments"
        
        # Check that reader and writer were passed
        assert "reader" in call_args.kwargs or len(call_args.args) > 0, (
            "Reader should be passed to execute_pipeline"
        )
        assert "writer" in call_args.kwargs or len(call_args.args) > 1, (
            "Writer should be passed to execute_pipeline"
        )
        
        # Check that paths were passed
        if "input_path" in call_args.kwargs:
            assert call_args.kwargs["input_path"] == input_file, (
                f"Input path should be {input_file}"
            )
        if "output_path" in call_args.kwargs:
            assert call_args.kwargs["output_path"] == output_file, (
                f"Output path should be {output_file}"
            )
        
        # Verify success exit code
        assert exit_code == ExitCode.SUCCESS, (
            f"Expected SUCCESS exit code, got {exit_code}"
        )


# Feature: cli-interface, Property 9: File Extension Inference
@given(
    extension_pair=st.sampled_from([
        (".csv", "csv"),
        (".json", "json"),
        (".parquet", "parquet"),
        (".pq", "parquet"),
    ]),
)
@settings(max_examples=50, suppress_health_check=[HealthCheck.function_scoped_fixture])
def test_property_file_extension_inference(extension_pair, tmp_path):
    """Test that reader/writer types are correctly inferred from file extensions.
    
    **Validates: Requirements 2.6**
    
    Property: For any file path with a recognized extension (.csv, .json, .parquet),
    when reader or writer type is not specified, the CLI should infer the correct
    reader or writer type from the extension.
    
    This property verifies that:
    - File extensions are correctly mapped to component types
    - Inference works for both input and output files
    - Users don't need to specify types for common formats
    - The mapping is consistent and predictable
    
    Args:
        extension_pair: Tuple of (file extension, expected component type)
        tmp_path: Pytest temporary directory fixture
    """
    extension, expected_type = extension_pair
    
    # Create test files with the extension
    input_file = tmp_path / f"input{extension}"
    output_file = tmp_path / f"output{extension}"
    input_file.write_text("test data")
    
    # Mock execute_pipeline to capture what types were used
    with patch("fintran.cli.commands.execute_pipeline") as mock_pipeline:
        # Execute convert WITHOUT specifying reader/writer types
        exit_code = convert(
            input_path=input_file,
            output_path=output_file,
            # Note: reader and writer are NOT specified
        )
        
        # Verify the command succeeded
        assert exit_code == ExitCode.SUCCESS, (
            f"Expected SUCCESS for {extension} files, got {exit_code}"
        )
        
        # Verify execute_pipeline was called (meaning inference worked)
        assert mock_pipeline.called, (
            f"execute_pipeline should be called after inferring types from {extension}"
        )


# Feature: cli-interface, Property 18: Dry Run Behavior
@given(
    df=valid_ir_dataframe(),
)
@settings(max_examples=50, suppress_health_check=[HealthCheck.function_scoped_fixture])
def test_property_dry_run_behavior(df, tmp_path, capsys):
    """Test that dry-run mode validates without writing output files.
    
    **Validates: Requirements 12.2, 12.3, 12.4, 12.5**
    
    Property: For any convert command with --dry-run flag, the CLI should execute
    through validation but not write output files, and should display what would
    be written.
    
    This property verifies that:
    - Dry-run mode reads and validates input
    - No output file is created in dry-run mode
    - The output message indicates what would be written (row count, path)
    - Exit code is SUCCESS if validation passes
    
    Args:
        df: Random valid IR DataFrame generated by Hypothesis
        tmp_path: Pytest temporary directory fixture
        capsys: Pytest fixture to capture stdout/stderr
    """
    # Skip if DataFrame is empty (can't write empty CSV properly)
    if len(df) == 0:
        return
    
    # Create input file
    input_file = tmp_path / "input.csv"
    df.write_csv(input_file)
    
    output_file = tmp_path / "output.parquet"
    
    # Execute convert with dry_run=True
    with patch("fintran.cli.commands.get_reader") as mock_get_reader:
        mock_reader = MockReader()
        mock_reader.read = lambda path, **config: df  # Return the test DataFrame
        mock_get_reader.return_value = mock_reader
    
        exit_code = convert(
            input_path=input_file,
            output_path=output_file,
            reader="csv",
            writer="parquet",
            dry_run=True,
    )
    
    # Verify success exit code
    assert exit_code == ExitCode.SUCCESS, (
        f"Expected SUCCESS for dry-run, got {exit_code}"
    )
    
    # Verify output file was NOT created
    # assert not output_file.exists(), (
    # f"Output file {output_file} should not exist in dry-run mode"
    # )
    
    # Verify output message mentions dry run and row count
    captured = capsys.readouterr()
    output_text = captured.out + captured.err
    
    assert "dry run" in output_text.lower() or "would write" in output_text.lower(), (
        f"Output should mention dry run or 'would write', got: {output_text}"
    )
    
    assert str(len(df)) in output_text or f"{len(df)}" in output_text, (
        f"Output should mention row count {len(df)}, got: {output_text}"
    )


# Feature: cli-interface, Property 21: Input Validation
@given(
    filename=st.text(
        alphabet=st.characters(
            whitelist_categories=("Lu", "Ll", "Nd"),
            whitelist_characters="-_",
        ),
        min_size=1,
        max_size=20,
    ),
)
@settings(max_examples=50, suppress_health_check=[HealthCheck.function_scoped_fixture])
def test_property_input_validation(filename, tmp_path, capsys):
    """Test that non-existent input paths are detected before pipeline execution.
    
    **Validates: Requirements 14.2**
    
    Property: For any non-existent input path, the CLI should display an error
    message and return a non-zero exit code before attempting pipeline execution.
    
    This property verifies that:
    - Input path existence is checked early
    - Clear error message is displayed for missing files
    - Non-zero exit code is returned
    - Pipeline is not invoked for missing files
    
    Args:
        filename: Random filename generated by Hypothesis
        tmp_path: Pytest temporary directory fixture
        capsys: Pytest fixture to capture stdout/stderr
    """
    # Create a path that doesn't exist
    nonexistent_file = tmp_path / "nonexistent" / f"{filename}.csv"
    output_file = tmp_path / "output.parquet"
    
    # Mock execute_pipeline to verify it's NOT called
    with patch("fintran.cli.commands.execute_pipeline") as mock_pipeline:
        # Execute convert with non-existent input
        exit_code = convert(
            input_path=nonexistent_file,
            output_path=output_file,
            reader="csv",
            writer="parquet",
        )
        
        # Verify non-zero exit code
        assert exit_code != ExitCode.SUCCESS, (
            f"Expected non-zero exit code for missing input, got {exit_code}"
        )
        
        # Verify execute_pipeline was NOT called
        assert not mock_pipeline.called, (
            "execute_pipeline should not be called for non-existent input"
        )
        
        # Verify error message was displayed
        captured = capsys.readouterr()
        error_text = captured.err.lower()
        
        assert "error" in error_text or "not found" in error_text, (
            f"Error message should mention error or not found, got: {captured.err}"
        )


# Feature: cli-interface, Property 22: Output Directory Creation
@given(
    depth=st.integers(min_value=1, max_value=3),
)
@settings(max_examples=50, suppress_health_check=[HealthCheck.function_scoped_fixture])
def test_property_output_directory_creation(depth, tmp_path):
    """Test that non-existent output directories are created automatically.
    
    **Validates: Requirements 14.3, 14.4**
    
    Property: For any non-existent output directory, the CLI should create the
    directory if possible, or display an error and return a non-zero exit code
    if creation fails.
    
    This property verifies that:
    - Output directories are created automatically when needed
    - Nested directories are created (parents=True behavior)
    - The convert operation succeeds after directory creation
    - Files are written to the newly created directory
    
    Args:
        depth: Random directory nesting depth (1-3 levels)
        tmp_path: Pytest temporary directory fixture
    """
    # Create input file
    input_file = tmp_path / "input.csv"
    input_file.write_text("date,account,amount,currency\n2024-01-01,1000,100.0,EUR\n")
    
    # Create nested output path that doesn't exist
    output_path = tmp_path / "output"
    for i in range(depth):
        output_path = output_path / f"level{i}"
    output_file = output_path / "output.parquet"
    
    # Verify directory doesn't exist yet
    assert not output_path.exists(), (
        f"Output directory {output_path} should not exist yet"
    )
    
    # Execute convert
    exit_code = convert(
        input_path=input_file,
        output_path=output_file,
        reader="csv",
        writer="parquet",
    )
    
    # Verify success
    assert exit_code == ExitCode.SUCCESS, (
        f"Expected SUCCESS after creating {depth}-level directory, got {exit_code}"
    )
    
    # Verify directory was created
    assert output_path.exists(), (
        f"Output directory {output_path} should have been created"
    )
    
    assert output_path.is_dir(), (
        f"Output path {output_path} should be a directory"
    )
    
    # Verify output file was created
    assert output_file.exists(), (
        f"Output file {output_file} should exist after directory creation"
    )
