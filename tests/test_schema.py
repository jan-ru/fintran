"""Unit tests for IR schema definition and validation service."""

from datetime import date
from decimal import Decimal as PyDecimal

import polars as pl
import pytest

from fintran.core.exceptions import ValidationError
from fintran.core.schema import (
    OPTIONAL_FIELDS,
    REQUIRED_FIELDS,
    create_empty_ir,
    get_ir_schema,
    validate_ir,
)


class TestSchemaDefinition:
    """Tests for IR schema definition functions."""

    def test_create_empty_ir_returns_empty_dataframe(self) -> None:
        """Test that create_empty_ir returns an empty DataFrame."""
        df = create_empty_ir()
        assert len(df) == 0
        assert isinstance(df, pl.DataFrame)

    def test_create_empty_ir_has_correct_schema(self) -> None:
        """Test that create_empty_ir has all required and optional fields."""
        df = create_empty_ir()
        columns = set(df.columns)

        # Check all required fields are present
        for field in REQUIRED_FIELDS:
            assert field in columns

        # Check all optional fields are present
        for field in OPTIONAL_FIELDS:
            assert field in columns

    def test_create_empty_ir_has_correct_types(self) -> None:
        """Test that create_empty_ir has correct data types."""
        df = create_empty_ir()
        schema = df.schema

        assert schema["date"] == pl.Date
        assert schema["account"] == pl.Utf8
        assert schema["amount"].is_decimal()
        assert schema["currency"] == pl.Utf8
        assert schema["description"] == pl.Utf8
        assert schema["reference"] == pl.Utf8

    def test_get_ir_schema_returns_dict(self) -> None:
        """Test that get_ir_schema returns a dictionary."""
        schema = get_ir_schema()
        assert isinstance(schema, dict)

    def test_get_ir_schema_has_all_fields(self) -> None:
        """Test that get_ir_schema includes all required and optional fields."""
        schema = get_ir_schema()
        fields = set(schema.keys())

        for field in REQUIRED_FIELDS:
            assert field in fields

        for field in OPTIONAL_FIELDS:
            assert field in fields

    def test_get_ir_schema_returns_copy(self) -> None:
        """Test that get_ir_schema returns a copy, not the original."""
        schema1 = get_ir_schema()
        schema2 = get_ir_schema()

        # Modify one copy
        schema1["new_field"] = pl.Int64

        # Verify the other is unchanged
        assert "new_field" not in schema2


class TestValidation:
    """Tests for IR validation service."""

    def test_validate_empty_ir_succeeds(self) -> None:
        """Test that validating an empty IR DataFrame succeeds."""
        df = create_empty_ir()
        result = validate_ir(df)
        assert result is df  # Same reference, not modified

    def test_validate_ir_with_data_succeeds(self) -> None:
        """Test that validating an IR DataFrame with data succeeds."""
        df = pl.DataFrame(
            {
                "date": [date(2024, 1, 1)],
                "account": ["ACC001"],
                "amount": [PyDecimal("100.50")],
                "currency": ["USD"],
                "description": ["Test transaction"],
                "reference": ["REF001"],
            }
        )
        result = validate_ir(df)
        assert result is df

    def test_validate_ir_with_optional_fields_null_succeeds(self) -> None:
        """Test that validation succeeds when optional fields are null."""
        df = pl.DataFrame(
            {
                "date": [date(2024, 1, 1)],
                "account": ["ACC001"],
                "amount": [PyDecimal("100.50")],
                "currency": ["USD"],
                "description": [None],
                "reference": [None],
            }
        )
        result = validate_ir(df)
        assert result is df

    def test_validate_ir_missing_required_field_raises_error(self) -> None:
        """Test that validation fails when a required field is missing."""
        df = pl.DataFrame(
            {
                "date": [date(2024, 1, 1)],
                "account": ["ACC001"],
                "amount": [PyDecimal("100.50")],
                # Missing 'currency' field
                "description": ["Test"],
                "reference": ["REF001"],
            }
        )

        with pytest.raises(ValidationError) as exc_info:
            validate_ir(df)

        assert "currency" in str(exc_info.value)
        assert exc_info.value.context["missing_fields"] == ["currency"]

    def test_validate_ir_multiple_missing_fields_raises_error(self) -> None:
        """Test that validation reports all missing required fields."""
        df = pl.DataFrame(
            {
                "date": [date(2024, 1, 1)],
                "account": ["ACC001"],
                # Missing 'amount' and 'currency'
                "description": ["Test"],
                "reference": ["REF001"],
            }
        )

        with pytest.raises(ValidationError) as exc_info:
            validate_ir(df)

        missing = exc_info.value.context["missing_fields"]
        assert "amount" in missing
        assert "currency" in missing

    def test_validate_ir_incorrect_type_raises_error(self) -> None:
        """Test that validation fails when a field has incorrect type."""
        df = pl.DataFrame(
            {
                "date": [date(2024, 1, 1)],
                "account": ["ACC001"],
                "amount": [100],  # Int64 instead of Decimal
                "currency": ["USD"],
                "description": ["Test"],
                "reference": ["REF001"],
            }
        )

        with pytest.raises(ValidationError) as exc_info:
            validate_ir(df)

        assert "amount" in str(exc_info.value)
        assert exc_info.value.context["field"] == "amount"

    def test_validate_ir_unexpected_field_raises_error(self) -> None:
        """Test that validation fails when unexpected fields are present."""
        df = pl.DataFrame(
            {
                "date": [date(2024, 1, 1)],
                "account": ["ACC001"],
                "amount": [PyDecimal("100.50")],
                "currency": ["USD"],
                "description": ["Test"],
                "reference": ["REF001"],
                "extra_field": ["unexpected"],  # Not in IR schema
            }
        )

        with pytest.raises(ValidationError) as exc_info:
            validate_ir(df)

        assert "extra_field" in str(exc_info.value)

    def test_validate_ir_is_idempotent(self) -> None:
        """Test that validating twice produces the same result."""
        df = create_empty_ir()

        result1 = validate_ir(df)
        result2 = validate_ir(result1)

        assert result1 is df
        assert result2 is df
        assert result1 is result2

    def test_validate_ir_does_not_modify_input(self) -> None:
        """Test that validation does not modify the input DataFrame."""
        df = pl.DataFrame(
            {
                "date": [date(2024, 1, 1)],
                "account": ["ACC001"],
                "amount": [PyDecimal("100.50")],
                "currency": ["USD"],
                "description": ["Test"],
                "reference": ["REF001"],
            }
        )

        original_id = id(df)
        result = validate_ir(df)

        # Same reference
        assert id(result) == original_id
        assert result is df
