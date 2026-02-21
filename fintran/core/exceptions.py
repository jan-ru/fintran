"""Custom exception classes for fintran error handling.

This module defines the exception hierarchy for the fintran pipeline:
- ValidationError: Schema violations in IR DataFrames
- ReaderError: Input file parsing failures
- WriterError: Output file serialization failures
- TransformError: Transformation operation failures
- PipelineError: Pipeline orchestration failures

All exceptions inherit from FintranError for consistent error handling.
"""

from typing import Any


class FintranError(Exception):
    """Base exception for all fintran errors.

    Provides a common base class for all custom exceptions in the fintran
    pipeline, enabling catch-all error handling when needed.
    """

    def __init__(self, message: str, context: dict[str, Any] | None = None) -> None:
        """Initialize the exception with a message and optional context.

        Args:
            message: Human-readable error description
            context: Optional dictionary of contextual information (file paths,
                    field names, values, etc.)
        """
        super().__init__(message)
        self.message = message
        self.context = context or {}

    def __str__(self) -> str:
        """Return a formatted error message with context."""
        if not self.context:
            return self.message

        context_str = ", ".join(f"{k}={v!r}" for k, v in self.context.items())
        return f"{self.message} [{context_str}]"


class ValidationError(FintranError):
    """Exception raised when IR DataFrame validation fails.

    This exception is raised by the Validation_Service when a DataFrame
    does not conform to the IR schema requirements (missing required fields,
    incorrect data types, etc.).

    Context typically includes:
        - field: Name of the problematic field
        - expected_type: Expected data type for the field
        - actual_type: Actual data type found
        - missing_fields: List of missing required fields
    """

    def __init__(
        self,
        message: str,
        field: str | None = None,
        expected_type: str | None = None,
        actual_type: str | None = None,
        missing_fields: list[str] | None = None,
        **extra_context: Any,
    ) -> None:
        """Initialize validation error with schema violation details.

        Args:
            message: Human-readable error description
            field: Name of the field that failed validation
            expected_type: Expected data type for the field
            actual_type: Actual data type found in the DataFrame
            missing_fields: List of missing required field names
            **extra_context: Additional context information
        """
        context: dict[str, Any] = {}
        if field is not None:
            context["field"] = field
        if expected_type is not None:
            context["expected_type"] = expected_type
        if actual_type is not None:
            context["actual_type"] = actual_type
        if missing_fields is not None:
            context["missing_fields"] = missing_fields
        context.update(extra_context)

        super().__init__(message, context)


class ReaderError(FintranError):
    """Exception raised when reading/parsing input files fails.

    This exception is raised by Reader implementations when they cannot
    successfully parse an input file into an IR DataFrame.

    Context typically includes:
        - file_path: Path to the input file that failed to parse
        - line_number: Line number where parsing failed (if applicable)
        - format: Expected file format
        - reason: Specific reason for the parsing failure
    """

    def __init__(
        self,
        message: str,
        file_path: str | None = None,
        line_number: int | None = None,
        format: str | None = None,
        reason: str | None = None,
        **extra_context: Any,
    ) -> None:
        """Initialize reader error with input file details.

        Args:
            message: Human-readable error description
            file_path: Path to the input file that failed
            line_number: Line number where parsing failed
            format: Expected file format (e.g., "CSV", "Parquet")
            reason: Specific reason for the parsing failure
            **extra_context: Additional context information
        """
        context: dict[str, Any] = {}
        if file_path is not None:
            context["file_path"] = file_path
        if line_number is not None:
            context["line_number"] = line_number
        if format is not None:
            context["format"] = format
        if reason is not None:
            context["reason"] = reason
        context.update(extra_context)

        super().__init__(message, context)


class WriterError(FintranError):
    """Exception raised when writing/serializing output files fails.

    This exception is raised by Writer implementations when they cannot
    successfully serialize an IR DataFrame to the output format.

    Context typically includes:
        - output_path: Path where the output file should be written
        - format: Target file format
        - reason: Specific reason for the serialization failure
    """

    def __init__(
        self,
        message: str,
        output_path: str | None = None,
        format: str | None = None,
        reason: str | None = None,
        **extra_context: Any,
    ) -> None:
        """Initialize writer error with output file details.

        Args:
            message: Human-readable error description
            output_path: Path where the output file should be written
            format: Target file format (e.g., "Parquet", "CSV")
            reason: Specific reason for the serialization failure
            **extra_context: Additional context information
        """
        context: dict[str, Any] = {}
        if output_path is not None:
            context["output_path"] = output_path
        if format is not None:
            context["format"] = format
        if reason is not None:
            context["reason"] = reason
        context.update(extra_context)

        super().__init__(message, context)


class TransformError(FintranError):
    """Exception raised when a transformation operation fails.

    This exception is raised by Transform implementations when they cannot
    successfully transform an IR DataFrame.

    Context typically includes:
        - transform_name: Name of the transform that failed
        - step: Specific step within the transform that failed
        - reason: Specific reason for the transformation failure
    """

    def __init__(
        self,
        message: str,
        transform_name: str | None = None,
        step: str | None = None,
        reason: str | None = None,
        **extra_context: Any,
    ) -> None:
        """Initialize transform error with transformation details.

        Args:
            message: Human-readable error description
            transform_name: Name of the transform that failed
            step: Specific step within the transform that failed
            reason: Specific reason for the transformation failure
            **extra_context: Additional context information
        """
        context: dict[str, Any] = {}
        if transform_name is not None:
            context["transform_name"] = transform_name
        if step is not None:
            context["step"] = step
        if reason is not None:
            context["reason"] = reason
        context.update(extra_context)

        super().__init__(message, context)


class PipelineError(FintranError):
    """Exception raised when pipeline orchestration fails.

    This exception is raised by the Transform_Service when the pipeline
    execution fails at any step (read, validate, transform, write). It wraps
    the original exception and adds context about which pipeline step failed.

    Context typically includes:
        - step: Which pipeline step failed (read, validate_reader_output,
               transform_N, validate_final_ir, write)
        - input_path: Path to the input file (for read failures)
        - output_path: Path to the output file (for write failures)
        - transform_index: Index of the transform that failed
        - transform_type: Type name of the transform that failed
    """

    def __init__(
        self,
        message: str,
        step: str | None = None,
        input_path: str | None = None,
        output_path: str | None = None,
        transform_index: int | None = None,
        transform_type: str | None = None,
        transform_count: int | None = None,
        **extra_context: Any,
    ) -> None:
        """Initialize pipeline error with orchestration details.

        Args:
            message: Human-readable error description
            step: Which pipeline step failed (read, validate, transform, write)
            input_path: Path to the input file
            output_path: Path to the output file
            transform_index: Index of the transform that failed (0-based)
            transform_type: Type name of the transform that failed
            transform_count: Total number of transforms in the pipeline
            **extra_context: Additional context information
        """
        context: dict[str, Any] = {}
        if step is not None:
            context["step"] = step
        if input_path is not None:
            context["input_path"] = input_path
        if output_path is not None:
            context["output_path"] = output_path
        if transform_index is not None:
            context["transform_index"] = transform_index
        if transform_type is not None:
            context["transform_type"] = transform_type
        if transform_count is not None:
            context["transform_count"] = transform_count
        context.update(extra_context)

        super().__init__(message, context)
