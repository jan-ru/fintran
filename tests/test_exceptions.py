"""Unit tests for custom exception classes."""

import pytest

from fintran.core.exceptions import (
    FintranError,
    ReaderError,
    TransformError,
    ValidationError,
    WriterError,
)


class TestFintranError:
    """Test the base FintranError exception."""

    def test_basic_message(self) -> None:
        """Test exception with just a message."""
        error = FintranError("Something went wrong")
        assert str(error) == "Something went wrong"
        assert error.message == "Something went wrong"
        assert error.context == {}

    def test_with_context(self) -> None:
        """Test exception with context information."""
        error = FintranError("Operation failed", context={"operation": "read", "file": "data.csv"})
        assert error.message == "Operation failed"
        assert error.context == {"operation": "read", "file": "data.csv"}
        assert "operation='read'" in str(error)
        assert "file='data.csv'" in str(error)


class TestValidationError:
    """Test the ValidationError exception."""

    def test_missing_field_error(self) -> None:
        """Test validation error for missing required field."""
        error = ValidationError("Required field is missing", field="date", missing_fields=["date"])
        assert error.message == "Required field is missing"
        assert error.context["field"] == "date"
        assert error.context["missing_fields"] == ["date"]
        assert "field='date'" in str(error)

    def test_type_mismatch_error(self) -> None:
        """Test validation error for incorrect data type."""
        error = ValidationError(
            "Field has incorrect type", field="amount", expected_type="Decimal", actual_type="Utf8"
        )
        assert error.context["field"] == "amount"
        assert error.context["expected_type"] == "Decimal"
        assert error.context["actual_type"] == "Utf8"
        assert "expected_type='Decimal'" in str(error)
        assert "actual_type='Utf8'" in str(error)

    def test_multiple_missing_fields(self) -> None:
        """Test validation error with multiple missing fields."""
        error = ValidationError(
            "Multiple required fields are missing", missing_fields=["date", "account", "amount"]
        )
        assert error.context["missing_fields"] == ["date", "account", "amount"]

    def test_extra_context(self) -> None:
        """Test validation error with extra context."""
        error = ValidationError(
            "Validation failed", field="currency", row_count=100, source="test_data.csv"
        )
        assert error.context["field"] == "currency"
        assert error.context["row_count"] == 100
        assert error.context["source"] == "test_data.csv"


class TestReaderError:
    """Test the ReaderError exception."""

    def test_file_not_found(self) -> None:
        """Test reader error for missing file."""
        error = ReaderError(
            "Input file not found", file_path="/path/to/missing.csv", reason="File does not exist"
        )
        assert error.context["file_path"] == "/path/to/missing.csv"
        assert error.context["reason"] == "File does not exist"
        assert "file_path='/path/to/missing.csv'" in str(error)

    def test_parsing_failure(self) -> None:
        """Test reader error for parsing failure."""
        error = ReaderError(
            "Failed to parse CSV file",
            file_path="data.csv",
            line_number=42,
            format="CSV",
            reason="Invalid delimiter",
        )
        assert error.context["file_path"] == "data.csv"
        assert error.context["line_number"] == 42
        assert error.context["format"] == "CSV"
        assert error.context["reason"] == "Invalid delimiter"
        assert "line_number=42" in str(error)

    def test_format_error(self) -> None:
        """Test reader error for unsupported format."""
        error = ReaderError(
            "Unsupported file format",
            file_path="data.xlsx",
            format="Excel",
            reason="Excel format not supported",
        )
        assert error.context["format"] == "Excel"
        assert "format='Excel'" in str(error)


class TestWriterError:
    """Test the WriterError exception."""

    def test_write_permission_error(self) -> None:
        """Test writer error for permission denied."""
        error = WriterError(
            "Cannot write to output path",
            output_path="/protected/output.parquet",
            reason="Permission denied",
        )
        assert error.context["output_path"] == "/protected/output.parquet"
        assert error.context["reason"] == "Permission denied"
        assert "output_path='/protected/output.parquet'" in str(error)

    def test_serialization_failure(self) -> None:
        """Test writer error for serialization failure."""
        error = WriterError(
            "Failed to serialize DataFrame",
            output_path="output.parquet",
            format="Parquet",
            reason="Invalid schema for Parquet format",
        )
        assert error.context["output_path"] == "output.parquet"
        assert error.context["format"] == "Parquet"
        assert error.context["reason"] == "Invalid schema for Parquet format"
        assert "format='Parquet'" in str(error)

    def test_disk_space_error(self) -> None:
        """Test writer error for disk space issues."""
        error = WriterError(
            "Insufficient disk space",
            output_path="/data/large_output.parquet",
            reason="No space left on device",
        )
        assert error.context["reason"] == "No space left on device"


class TestTransformError:
    """Test the TransformError exception."""

    def test_transform_failure(self) -> None:
        """Test transform error for general failure."""
        error = TransformError(
            "Transform operation failed",
            transform_name="CurrencyConverter",
            reason="Invalid currency code",
        )
        assert error.context["transform_name"] == "CurrencyConverter"
        assert error.context["reason"] == "Invalid currency code"
        assert "transform_name='CurrencyConverter'" in str(error)

    def test_transform_step_failure(self) -> None:
        """Test transform error with specific step."""
        error = TransformError(
            "Transform step failed",
            transform_name="DataEnricher",
            step="lookup_account_names",
            reason="Database connection failed",
        )
        assert error.context["transform_name"] == "DataEnricher"
        assert error.context["step"] == "lookup_account_names"
        assert error.context["reason"] == "Database connection failed"
        assert "step='lookup_account_names'" in str(error)

    def test_transform_with_extra_context(self) -> None:
        """Test transform error with additional context."""
        error = TransformError(
            "Transform failed on row",
            transform_name="RowFilter",
            row_index=150,
            filter_condition="amount > 0",
        )
        assert error.context["transform_name"] == "RowFilter"
        assert error.context["row_index"] == 150
        assert error.context["filter_condition"] == "amount > 0"


class TestExceptionInheritance:
    """Test exception inheritance hierarchy."""

    def test_all_inherit_from_fintran_error(self) -> None:
        """Test that all custom exceptions inherit from FintranError."""
        assert issubclass(ValidationError, FintranError)
        assert issubclass(ReaderError, FintranError)
        assert issubclass(WriterError, FintranError)
        assert issubclass(TransformError, FintranError)

    def test_all_inherit_from_exception(self) -> None:
        """Test that all custom exceptions inherit from Exception."""
        assert issubclass(FintranError, Exception)
        assert issubclass(ValidationError, Exception)
        assert issubclass(ReaderError, Exception)
        assert issubclass(WriterError, Exception)
        assert issubclass(TransformError, Exception)

    def test_catch_all_with_fintran_error(self) -> None:
        """Test that FintranError can catch all custom exceptions."""
        exceptions = [
            ValidationError("test"),
            ReaderError("test"),
            WriterError("test"),
            TransformError("test"),
        ]

        for exc in exceptions:
            try:
                raise exc
            except FintranError as e:
                assert isinstance(e, FintranError)
            else:
                pytest.fail(f"Failed to catch {type(exc).__name__}")
