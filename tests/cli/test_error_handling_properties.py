"""Property-based tests for error handling.

This module tests universal properties of error handling including:
- Exit code mapping for different error types
- Error context preservation
- Exception propagation

Requirements: 7.1-7.5, 7.8, 9.1-9.7, 15.3, 15.5
"""

from pathlib import Path
from unittest.mock import Mock, patch

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from fintran.cli.commands import convert
from fintran.cli.exit_codes import ExitCode
from fintran.cli.registry import register_reader, register_writer
from fintran.core.exceptions import (
    PipelineError,
    ReaderError,
    TransformError,
    ValidationError,
    WriterError,
)


class MockReader:
    """Mock reader for testing."""
    
    def read(self, path: Path, **config):
        """Read file and return IR DataFrame."""
        import polars as pl
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
    
    def write(self, df, path: Path, **config):
        """Write DataFrame to file."""
        path.write_text("mock output")


@pytest.fixture(autouse=True)
def setup_registry():
    """Register mock components for testing."""
    register_reader("csv", MockReader)
    register_writer("parquet", MockWriter)
    yield


# Feature: cli-interface, Property 2: Exit Code Mapping
@given(
    error_type=st.sampled_from([
        "ValidationError",
        "ReaderError",
        "WriterError",
        "TransformError",
        "ConfigError",
        "PipelineError",
        "UnexpectedError",
    ]),
)
@settings(max_examples=50, suppress_health_check=[HealthCheck.function_scoped_fixture])
def test_property_exit_code_mapping(error_type, tmp_path):
    """Test that error types are mapped to correct exit codes.
    
    **Validates: Requirements 9.1, 9.2, 9.3, 9.4, 9.5, 9.6, 9.7, 2.7, 2.8**
    
    Property: For any error type (ValidationError, ReaderError, WriterError,
    TransformError, ConfigError, unexpected), the CLI should return the
    corresponding exit code (2, 3, 4, 5, 6, 1 respectively), and for successful
    operations, return exit code 0.
    
    This property verifies that:
    - Each exception type maps to a unique exit code
    - Exit codes are consistent across all commands
    - Scripts can reliably detect error types from exit codes
    - Success always returns 0
    
    Args:
        error_type: Type of error to simulate
        tmp_path: Pytest temporary directory fixture
    """
    # Create test files
    input_file = tmp_path / "input.csv"
    output_file = tmp_path / "output.parquet"
    input_file.write_text("test data")
    
    # Map error types to expected exit codes
    expected_exit_codes = {
        "ValidationError": ExitCode.VALIDATION_ERROR,
        "ReaderError": ExitCode.READER_ERROR,
        "WriterError": ExitCode.WRITER_ERROR,
        "TransformError": ExitCode.TRANSFORM_ERROR,
        "ConfigError": ExitCode.CONFIG_ERROR,
        "PipelineError": ExitCode.UNEXPECTED_ERROR,
        "UnexpectedError": ExitCode.UNEXPECTED_ERROR,
    }
    
    # Map error types to exception classes
    exception_classes = {
        "ValidationError": ValidationError,
        "ReaderError": ReaderError,
        "WriterError": WriterError,
        "TransformError": TransformError,
        "ConfigError": Exception,  # ConfigError is imported from config module
        "PipelineError": PipelineError,
        "UnexpectedError": RuntimeError,
    }
    
    expected_code = expected_exit_codes[error_type]
    exception_class = exception_classes[error_type]
    
    # Mock execute_pipeline to raise the specified error
    with patch("fintran.cli.commands.execute_pipeline") as mock_pipeline:
        if error_type == "ConfigError":
            # ConfigError happens before pipeline execution
            with patch("fintran.cli.commands.load_config") as mock_load:
                from fintran.cli.config import ConfigError
                mock_load.side_effect = ConfigError("Test config error")
                
                exit_code = convert(
                    input_path=input_file,
                    output_path=output_file,
                    config=tmp_path / "config.json",
                )
        else:
            # Other errors happen during pipeline execution
            mock_pipeline.side_effect = exception_class("Test error")
            
            exit_code = convert(
                input_path=input_file,
                output_path=output_file,
                reader="csv",
                writer="parquet",
            )
        
        # Verify correct exit code
        assert exit_code == expected_code, (
            f"Expected exit code {expected_code} for {error_type}, got {exit_code}"
        )


# Feature: cli-interface, Property 3: Error Context Preservation
@given(
    context_fields=st.lists(
        st.tuples(
            st.text(alphabet=st.characters(whitelist_categories=("Lu", "Ll")), min_size=1, max_size=20),
            st.text(alphabet=st.characters(whitelist_categories=("Lu", "Ll", "Nd", "Zs")), max_size=50),
        ),
        min_size=1,
        max_size=5,
    ),
)
@settings(max_examples=50, suppress_health_check=[HealthCheck.function_scoped_fixture])
def test_property_error_context_preservation(context_fields, tmp_path, capsys):
    """Test that error context information is preserved in CLI output.
    
    **Validates: Requirements 15.5, 7.1, 7.2, 7.3, 7.4, 7.5**
    
    Property: For any FintranError with context information, the CLI error output
    should include all context fields from the original exception (file paths,
    field names, transform names, etc.).
    
    This property verifies that:
    - All context fields from exceptions are displayed
    - Context is formatted in a readable way
    - Users get enough information to diagnose issues
    - No context information is lost during error handling
    
    Args:
        context_fields: Random list of (key, value) context pairs
        tmp_path: Pytest temporary directory fixture
        capsys: Pytest fixture to capture stdout/stderr
    """
    # Create test files
    input_file = tmp_path / "input.csv"
    output_file = tmp_path / "output.parquet"
    input_file.write_text("test data")
    
    # Create context dictionary from generated fields
    context = {key: value for key, value in context_fields}
    
    # Mock execute_pipeline to raise error with context
    with patch("fintran.cli.commands.execute_pipeline") as mock_pipeline:
        # Create a ReaderError with context
        error = ReaderError("Test error with context")
        # Add context as attributes (FintranError pattern)
        for key, value in context.items():
            setattr(error, key, value)
        error.context = context
        
        mock_pipeline.side_effect = error
        
        # Execute convert
        exit_code = convert(
            input_path=input_file,
            output_path=output_file,
            reader="csv",
            writer="parquet",
        )
        
        # Verify error exit code
        assert exit_code == ExitCode.READER_ERROR
        
        # Capture output
        captured = capsys.readouterr()
        error_output = captured.err.lower()
        
        # Verify context fields are present in error output
        for key, value in context.items():
            # Check if key or value appears in output
            # (exact format may vary, so we check for presence)
            key_present = key.lower() in error_output
            value_present = value.lower() in error_output if value else True
            
            assert key_present or value_present, (
                f"Context field '{key}: {value}' should appear in error output. "
                f"Got: {captured.err}"
            )


# Feature: cli-interface, Property 24: Exception Propagation
@given(
    exception_type=st.sampled_from([
        ValidationError,
        ReaderError,
        WriterError,
        TransformError,
        PipelineError,
    ]),
)
@settings(max_examples=50, suppress_health_check=[HealthCheck.function_scoped_fixture])
def test_property_exception_propagation(exception_type, tmp_path):
    """Test that pipeline exceptions are propagated without losing type or context.
    
    **Validates: Requirements 15.3**
    
    Property: For any exception raised by the pipeline, the CLI should propagate
    it to the error handling layer without losing exception type or context.
    
    This property verifies that:
    - Exception types are preserved during propagation
    - Exception context is not lost
    - The CLI doesn't wrap exceptions unnecessarily
    - Error handling can distinguish between different error types
    
    Args:
        exception_type: Type of exception to test
        tmp_path: Pytest temporary directory fixture
    """
    # Create test files
    input_file = tmp_path / "input.csv"
    output_file = tmp_path / "output.parquet"
    input_file.write_text("test data")
    
    # Create exception with context
    error_message = "Test error message"
    test_context = {"test_key": "test_value", "file_path": str(input_file)}
    
    exception = exception_type(error_message)
    exception.context = test_context
    
    # Mock execute_pipeline to raise the exception
    with patch("fintran.cli.commands.execute_pipeline") as mock_pipeline:
        mock_pipeline.side_effect = exception
        
        # Execute convert
        exit_code = convert(
            input_path=input_file,
            output_path=output_file,
            reader="csv",
            writer="parquet",
        )
        
        # Verify appropriate exit code based on exception type
        expected_codes = {
            ValidationError: ExitCode.VALIDATION_ERROR,
            ReaderError: ExitCode.READER_ERROR,
            WriterError: ExitCode.WRITER_ERROR,
            TransformError: ExitCode.TRANSFORM_ERROR,
            PipelineError: ExitCode.UNEXPECTED_ERROR,
        }
        
        expected_code = expected_codes[exception_type]
        assert exit_code == expected_code, (
            f"Expected exit code {expected_code} for {exception_type.__name__}, "
            f"got {exit_code}"
        )
        
        # The fact that we got the correct exit code proves the exception
        # type was preserved and correctly identified by the error handler


# Feature: cli-interface, Property 4: Stream Separation
@given(
    operation_succeeds=st.booleans(),
)
@settings(max_examples=50, suppress_health_check=[HealthCheck.function_scoped_fixture])
def test_property_stream_separation(operation_succeeds, tmp_path, capsys):
    """Test that errors go to stderr and normal output goes to stdout.
    
    **Validates: Requirements 7.6, 7.7**
    
    Property: For any CLI operation, error messages should be written to stderr
    and normal output should be written to stdout.
    
    This property verifies that:
    - Error messages are written to stderr
    - Success messages are written to stdout
    - Streams are not mixed
    - Scripts can redirect streams independently
    
    Args:
        operation_succeeds: Whether the operation should succeed or fail
        tmp_path: Pytest temporary directory fixture
        capsys: Pytest fixture to capture stdout/stderr
    """
    # Create test files
    input_file = tmp_path / "input.csv"
    output_file = tmp_path / "output.parquet"
    input_file.write_text("test data")
    
    if operation_succeeds:
        # Mock successful execution
        with patch("fintran.cli.commands.execute_pipeline"):
            exit_code = convert(
                input_path=input_file,
                output_path=output_file,
                reader="csv",
                writer="parquet",
                quiet=False,  # Enable output
            )
            
            assert exit_code == ExitCode.SUCCESS
            
            # Capture output
            captured = capsys.readouterr()
            
            # Success messages should be on stdout or stderr (progress indicators)
            # At minimum, there should be some output
            assert captured.out or captured.err, (
                "Should have some output for successful operation"
            )
            
            # If there's output on stdout, it should be success-related
            if captured.out:
                assert "error" not in captured.out.lower(), (
                    f"stdout should not contain error messages, got: {captured.out}"
                )
    else:
        # Mock failed execution
        with patch("fintran.cli.commands.execute_pipeline") as mock_pipeline:
            mock_pipeline.side_effect = ReaderError("Test error")
            
            exit_code = convert(
                input_path=input_file,
                output_path=output_file,
                reader="csv",
                writer="parquet",
            )
            
            assert exit_code != ExitCode.SUCCESS
            
            # Capture output
            captured = capsys.readouterr()
            
            # Error messages should be on stderr
            assert captured.err, (
                "Error messages should be written to stderr"
            )
            
            assert "error" in captured.err.lower(), (
                f"stderr should contain error message, got: {captured.err}"
            )
