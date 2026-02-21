"""Unit tests for pipeline error handling.

This module tests error propagation and context enrichment in the pipeline
orchestration service. It verifies that errors from different pipeline steps
(Reader, Transform, Writer) are properly caught, wrapped with context, and
propagated to the caller.

Test Coverage:
- Error propagation from Reader (ReaderError, unexpected errors)
- Error propagation from Transform (TransformError, unexpected errors)
- Error propagation from Writer (WriterError, unexpected errors)
- Context enrichment (step name, file paths, transform index, etc.)
- Validation errors at different stages (after reader, after transforms)
- Immutability violation detection
- Unexpected error wrapping

Requirements:
    - Requirement 6.7: Pipeline propagates errors with context
    - Requirement 9.6: Transform_Service wraps errors with step context
"""

from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any

import polars as pl
import pytest

from fintran.core.exceptions import (
    PipelineError,
    ReaderError,
    TransformError,
    ValidationError,
    WriterError,
)
from fintran.core.pipeline import execute_pipeline
from fintran.core.protocols import Transform

# Mock implementations for testing


class MockReader:
    """Mock Reader that returns a stored DataFrame."""

    def __init__(self, df: pl.DataFrame) -> None:
        """Initialize with a DataFrame to return.

        Args:
            df: DataFrame to return from read()
        """
        self.df = df

    def read(self, path: Path, **config: Any) -> pl.DataFrame:
        """Return the stored DataFrame.

        Args:
            path: Input path (ignored)
            **config: Configuration (ignored)

        Returns:
            The stored DataFrame
        """
        return self.df


class FailingReader:
    """Mock Reader that raises an error."""

    def __init__(self, error: Exception) -> None:
        """Initialize with an error to raise.

        Args:
            error: Exception to raise from read()
        """
        self.error = error

    def read(self, path: Path, **config: Any) -> pl.DataFrame:
        """Raise the stored error.

        Args:
            path: Input path (ignored)
            **config: Configuration (ignored)

        Raises:
            The stored exception
        """
        raise self.error


class MockWriter:
    """Mock Writer that captures the written DataFrame."""

    def __init__(self) -> None:
        """Initialize with no captured DataFrame."""
        self.written_df: pl.DataFrame | None = None

    def write(self, df: pl.DataFrame, path: Path, **config: Any) -> None:
        """Capture the DataFrame being written.

        Args:
            df: DataFrame to write
            path: Output path (ignored)
            **config: Configuration (ignored)
        """
        self.written_df = df


class FailingWriter:
    """Mock Writer that raises an error."""

    def __init__(self, error: Exception) -> None:
        """Initialize with an error to raise.

        Args:
            error: Exception to raise from write()
        """
        self.error = error

    def write(self, df: pl.DataFrame, path: Path, **config: Any) -> None:
        """Raise the stored error.

        Args:
            df: DataFrame to write (ignored)
            path: Output path (ignored)
            **config: Configuration (ignored)

        Raises:
            The stored exception
        """
        raise self.error


class IdentityTransform:
    """Mock Transform that returns a copy of the input DataFrame."""

    def transform(self, df: pl.DataFrame) -> pl.DataFrame:
        """Return a copy of the input DataFrame.

        Args:
            df: Input DataFrame

        Returns:
            A copy of the input DataFrame
        """
        return df.clone()


class FailingTransform:
    """Mock Transform that raises an error."""

    def __init__(self, error: Exception) -> None:
        """Initialize with an error to raise.

        Args:
            error: Exception to raise from transform()
        """
        self.error = error

    def transform(self, df: pl.DataFrame) -> pl.DataFrame:
        """Raise the stored error.

        Args:
            df: Input DataFrame (ignored)

        Raises:
            The stored exception
        """
        raise self.error


class ImmutableViolatingTransform:
    """Mock Transform that violates immutability by returning the same instance."""

    def transform(self, df: pl.DataFrame) -> pl.DataFrame:
        """Return the same DataFrame instance (violates immutability).

        Args:
            df: Input DataFrame

        Returns:
            The same DataFrame instance (not a copy)
        """
        return df


# Test fixtures


@pytest.fixture
def sample_ir() -> pl.DataFrame:
    """Create a sample IR DataFrame for testing."""
    from datetime import date
    from decimal import Decimal

    return pl.DataFrame(
        {
            "date": [date(2024, 1, 15), date(2024, 1, 16)],
            "account": ["1001", "1002"],
            "amount": [Decimal("100.50"), Decimal("-50.25")],
            "currency": ["USD", "USD"],
            "description": ["Payment received", "Service fee"],
            "reference": ["INV-001", None],
        }
    ).with_columns(pl.col("amount").cast(pl.Decimal(precision=38, scale=10)))


@pytest.fixture
def temp_paths() -> Any:
    """Create temporary file paths for testing."""
    with (
        NamedTemporaryFile(suffix=".csv") as input_file,
        NamedTemporaryFile(suffix=".parquet") as output_file,
    ):
        yield Path(input_file.name), Path(output_file.name)


# Tests for Reader error propagation


def test_reader_error_is_propagated(sample_ir: pl.DataFrame, temp_paths: tuple[Path, Path]) -> None:
    """Test that ReaderError is re-raised without wrapping.

    Validates:
        - Requirement 6.7: Pipeline propagates errors with context
        - Requirement 9.6: Transform_Service wraps errors with step context
    """
    input_path, output_path = temp_paths

    # Create a reader that raises ReaderError
    reader_error = ReaderError(
        "Failed to parse CSV", file_path=str(input_path), reason="Invalid date format"
    )
    reader = FailingReader(reader_error)
    writer = MockWriter()

    # Execute pipeline and expect ReaderError to be re-raised
    with pytest.raises(ReaderError) as exc_info:
        execute_pipeline(
            reader=reader,
            writer=writer,
            input_path=input_path,
            output_path=output_path,
        )

    # Verify the error is the same instance (not wrapped)
    assert exc_info.value is reader_error


def test_reader_unexpected_error_is_wrapped(
    sample_ir: pl.DataFrame, temp_paths: tuple[Path, Path]
) -> None:
    """Test that unexpected errors from Reader are wrapped in PipelineError.

    Validates:
        - Requirement 6.7: Pipeline propagates errors with context
        - Requirement 9.6: Transform_Service wraps errors with step context
    """
    input_path, output_path = temp_paths

    # Create a reader that raises an unexpected error
    unexpected_error = ValueError("Unexpected parsing error")
    reader = FailingReader(unexpected_error)
    writer = MockWriter()

    # Execute pipeline and expect PipelineError
    with pytest.raises(PipelineError) as exc_info:
        execute_pipeline(
            reader=reader,
            writer=writer,
            input_path=input_path,
            output_path=output_path,
        )

    # Verify error is wrapped with context
    error = exc_info.value
    assert "Pipeline failed at read step" in str(error)
    assert error.context["step"] == "read"
    assert error.context["input_path"] == str(input_path)
    assert error.__cause__ is unexpected_error


# Tests for Transform error propagation


def test_transform_error_is_propagated(
    sample_ir: pl.DataFrame, temp_paths: tuple[Path, Path]
) -> None:
    """Test that TransformError is re-raised without wrapping.

    Validates:
        - Requirement 6.7: Pipeline propagates errors with context
        - Requirement 9.6: Transform_Service wraps errors with step context
    """
    input_path, output_path = temp_paths

    # Create a transform that raises TransformError
    transform_error = TransformError(
        "Failed to normalize currency",
        transform_name="CurrencyNormalizer",
        reason="Unknown currency code",
    )
    reader = MockReader(sample_ir)
    writer = MockWriter()
    transforms = [FailingTransform(transform_error)]

    # Execute pipeline and expect TransformError to be re-raised
    with pytest.raises(TransformError) as exc_info:
        execute_pipeline(
            reader=reader,
            writer=writer,
            input_path=input_path,
            output_path=output_path,
            transforms=transforms,
        )

    # Verify the error is the same instance (not wrapped)
    assert exc_info.value is transform_error


def test_transform_unexpected_error_is_wrapped(
    sample_ir: pl.DataFrame, temp_paths: tuple[Path, Path]
) -> None:
    """Test that unexpected errors from Transform are wrapped in PipelineError.

    Validates:
        - Requirement 6.7: Pipeline propagates errors with context
        - Requirement 9.6: Transform_Service wraps errors with step context
    """
    input_path, output_path = temp_paths

    # Create a transform that raises an unexpected error
    unexpected_error = RuntimeError("Unexpected transformation error")
    reader = MockReader(sample_ir)
    writer = MockWriter()
    transforms = [FailingTransform(unexpected_error)]

    # Execute pipeline and expect PipelineError
    with pytest.raises(PipelineError) as exc_info:
        execute_pipeline(
            reader=reader,
            writer=writer,
            input_path=input_path,
            output_path=output_path,
            transforms=transforms,
        )

    # Verify error is wrapped with context
    error = exc_info.value
    assert "Pipeline failed at transform step 0" in str(error)
    assert error.context["step"] == "transform_0"
    assert error.context["transform_index"] == 0
    assert error.context["transform_type"] == "FailingTransform"
    assert error.__cause__ is unexpected_error


def test_transform_error_context_includes_index(
    sample_ir: pl.DataFrame, temp_paths: tuple[Path, Path]
) -> None:
    """Test that transform errors include the transform index in context.

    Validates:
        - Requirement 6.7: Pipeline propagates errors with context
        - Requirement 9.6: Transform_Service wraps errors with step context
    """
    input_path, output_path = temp_paths

    # Create multiple transforms, with the second one failing
    unexpected_error = RuntimeError("Transform 2 failed")
    reader = MockReader(sample_ir)
    writer = MockWriter()
    transforms: list[Transform] = [
        IdentityTransform(),
        FailingTransform(unexpected_error),
        IdentityTransform(),
    ]

    # Execute pipeline and expect PipelineError
    with pytest.raises(PipelineError) as exc_info:
        execute_pipeline(
            reader=reader,
            writer=writer,
            input_path=input_path,
            output_path=output_path,
            transforms=transforms,
        )

    # Verify error context includes correct transform index
    error = exc_info.value
    assert error.context["step"] == "transform_1"
    assert error.context["transform_index"] == 1
    assert error.context["transform_type"] == "FailingTransform"


# Tests for Writer error propagation


def test_writer_error_is_propagated(sample_ir: pl.DataFrame, temp_paths: tuple[Path, Path]) -> None:
    """Test that WriterError is re-raised without wrapping.

    Validates:
        - Requirement 6.7: Pipeline propagates errors with context
        - Requirement 9.6: Transform_Service wraps errors with step context
    """
    input_path, output_path = temp_paths

    # Create a writer that raises WriterError
    writer_error = WriterError(
        "Failed to write Parquet file", output_path=str(output_path), reason="Disk full"
    )
    reader = MockReader(sample_ir)
    writer = FailingWriter(writer_error)

    # Execute pipeline and expect WriterError to be re-raised
    with pytest.raises(WriterError) as exc_info:
        execute_pipeline(
            reader=reader,
            writer=writer,
            input_path=input_path,
            output_path=output_path,
        )

    # Verify the error is the same instance (not wrapped)
    assert exc_info.value is writer_error


def test_writer_unexpected_error_is_wrapped(
    sample_ir: pl.DataFrame, temp_paths: tuple[Path, Path]
) -> None:
    """Test that unexpected errors from Writer are wrapped in PipelineError.

    Validates:
        - Requirement 6.7: Pipeline propagates errors with context
        - Requirement 9.6: Transform_Service wraps errors with step context
    """
    input_path, output_path = temp_paths

    # Create a writer that raises an unexpected error
    unexpected_error = OSError("Unexpected write error")
    reader = MockReader(sample_ir)
    writer = FailingWriter(unexpected_error)

    # Execute pipeline and expect PipelineError
    with pytest.raises(PipelineError) as exc_info:
        execute_pipeline(
            reader=reader,
            writer=writer,
            input_path=input_path,
            output_path=output_path,
        )

    # Verify error is wrapped with context
    error = exc_info.value
    assert "Pipeline failed at write step" in str(error)
    assert error.context["step"] == "write"
    assert error.context["output_path"] == str(output_path)
    assert error.__cause__ is unexpected_error


# Tests for validation error handling


def test_reader_validation_error_is_wrapped(temp_paths: tuple[Path, Path]) -> None:
    """Test that validation errors after Reader are wrapped with context.

    Validates:
        - Requirement 6.7: Pipeline propagates errors with context
        - Requirement 9.6: Transform_Service wraps errors with step context
    """
    input_path, output_path = temp_paths

    # Create a reader that returns invalid IR (missing required field)
    invalid_ir = pl.DataFrame(
        {
            "account": ["1001"],
            "amount": [100.50],
            "currency": ["USD"],
        }
    )
    reader = MockReader(invalid_ir)
    writer = MockWriter()

    # Execute pipeline and expect PipelineError wrapping ValidationError
    with pytest.raises(PipelineError) as exc_info:
        execute_pipeline(
            reader=reader,
            writer=writer,
            input_path=input_path,
            output_path=output_path,
        )

    # Verify error is wrapped with context
    error = exc_info.value
    assert "Reader produced invalid IR" in str(error)
    assert error.context["step"] == "validate_reader_output"
    assert error.context["input_path"] == str(input_path)
    assert isinstance(error.__cause__, ValidationError)


def test_transform_validation_error_is_wrapped(
    sample_ir: pl.DataFrame, temp_paths: tuple[Path, Path]
) -> None:
    """Test that validation errors after transforms are wrapped with context.

    Validates:
        - Requirement 6.7: Pipeline propagates errors with context
        - Requirement 9.6: Transform_Service wraps errors with step context
    """
    input_path, output_path = temp_paths

    # Create a transform that returns invalid IR (missing required field)
    class InvalidTransform:
        def transform(self, df: pl.DataFrame) -> pl.DataFrame:
            # Return DataFrame missing required field
            return pl.DataFrame(
                {
                    "account": ["1001"],
                    "amount": [100.50],
                    "currency": ["USD"],
                }
            )

    reader = MockReader(sample_ir)
    writer = MockWriter()
    transforms = [InvalidTransform()]

    # Execute pipeline and expect PipelineError wrapping ValidationError
    with pytest.raises(PipelineError) as exc_info:
        execute_pipeline(
            reader=reader,
            writer=writer,
            input_path=input_path,
            output_path=output_path,
            transforms=transforms,
        )

    # Verify error is wrapped with context
    error = exc_info.value
    assert "Final IR is invalid after transforms" in str(error)
    assert error.context["step"] == "validate_final_ir"
    assert error.context["transform_count"] == 1
    assert isinstance(error.__cause__, ValidationError)


# Tests for immutability violation


def test_immutability_violation_is_detected(
    sample_ir: pl.DataFrame, temp_paths: tuple[Path, Path]
) -> None:
    """Test that immutability violations are detected and reported.

    Validates:
        - Requirement 6.7: Pipeline propagates errors with context
        - Requirement 7.3: Verify input DataFrame is not modified by Transforms
    """
    input_path, output_path = temp_paths

    reader = MockReader(sample_ir)
    writer = MockWriter()
    transforms = [ImmutableViolatingTransform()]

    # Execute pipeline and expect PipelineError for immutability violation
    with pytest.raises(PipelineError) as exc_info:
        execute_pipeline(
            reader=reader,
            writer=writer,
            input_path=input_path,
            output_path=output_path,
            transforms=transforms,
        )

    # Verify error describes immutability violation
    error = exc_info.value
    assert "violated immutability requirement" in str(error)
    assert error.context["step"] == "transform_0"
    assert error.context["transform_index"] == 0
    assert error.context["transform_type"] == "ImmutableViolatingTransform"


# Tests for context enrichment


def test_error_context_includes_file_paths(
    sample_ir: pl.DataFrame, temp_paths: tuple[Path, Path]
) -> None:
    """Test that error context includes input and output file paths.

    Validates:
        - Requirement 6.7: Pipeline propagates errors with context
        - Requirement 9.6: Transform_Service wraps errors with step context
    """
    input_path, output_path = temp_paths

    # Test read error includes input_path
    reader = FailingReader(ValueError("Read failed"))
    writer = MockWriter()

    with pytest.raises(PipelineError) as exc_info:
        execute_pipeline(
            reader=reader,
            writer=writer,
            input_path=input_path,
            output_path=output_path,
        )

    assert exc_info.value.context["input_path"] == str(input_path)

    # Test write error includes output_path
    reader2 = MockReader(sample_ir)
    writer2 = FailingWriter(ValueError("Write failed"))

    with pytest.raises(PipelineError) as exc_info:
        execute_pipeline(
            reader=reader2,
            writer=writer2,
            input_path=input_path,
            output_path=output_path,
        )

    assert exc_info.value.context["output_path"] == str(output_path)


def test_error_context_includes_transform_details(
    sample_ir: pl.DataFrame, temp_paths: tuple[Path, Path]
) -> None:
    """Test that error context includes transform index and type.

    Validates:
        - Requirement 6.7: Pipeline propagates errors with context
        - Requirement 9.6: Transform_Service wraps errors with step context
    """
    input_path, output_path = temp_paths

    reader = MockReader(sample_ir)
    writer = MockWriter()
    transforms: list[Transform] = [
        IdentityTransform(),
        IdentityTransform(),
        FailingTransform(ValueError("Transform failed")),
    ]

    with pytest.raises(PipelineError) as exc_info:
        execute_pipeline(
            reader=reader,
            writer=writer,
            input_path=input_path,
            output_path=output_path,
            transforms=transforms,
        )

    # Verify context includes transform details
    error = exc_info.value
    assert error.context["step"] == "transform_2"
    assert error.context["transform_index"] == 2
    assert error.context["transform_type"] == "FailingTransform"


# Tests for successful pipeline execution


def test_successful_pipeline_with_no_transforms(
    sample_ir: pl.DataFrame, temp_paths: tuple[Path, Path]
) -> None:
    """Test that successful pipeline execution completes without errors.

    Validates:
        - Requirement 6.1-6.6: Pipeline executes all steps successfully
    """
    input_path, output_path = temp_paths

    reader = MockReader(sample_ir)
    writer = MockWriter()

    # Execute pipeline - should not raise any errors
    execute_pipeline(
        reader=reader,
        writer=writer,
        input_path=input_path,
        output_path=output_path,
    )

    # Verify writer received the DataFrame
    assert writer.written_df is not None
    assert sample_ir.equals(writer.written_df)


def test_successful_pipeline_with_transforms(
    sample_ir: pl.DataFrame, temp_paths: tuple[Path, Path]
) -> None:
    """Test that successful pipeline with transforms completes without errors.

    Validates:
        - Requirement 6.1-6.6: Pipeline executes all steps successfully
    """
    input_path, output_path = temp_paths

    reader = MockReader(sample_ir)
    writer = MockWriter()
    transforms = [IdentityTransform(), IdentityTransform()]

    # Execute pipeline - should not raise any errors
    execute_pipeline(
        reader=reader,
        writer=writer,
        input_path=input_path,
        output_path=output_path,
        transforms=transforms,
    )

    # Verify writer received the DataFrame
    assert writer.written_df is not None
