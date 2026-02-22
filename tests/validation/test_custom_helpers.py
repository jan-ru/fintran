"""Tests for custom validator helpers.

This module tests the helper functions and base class provided for creating
custom validators.
"""

from datetime import date
from decimal import Decimal

import polars as pl
import pytest

from fintran.validation.custom import (
    CustomValidatorBase,
    aggregate_by_group,
    check_required_fields,
    custom_validator,
    filter_by_patterns,
    format_group_error,
    format_violation_error,
    get_violations_with_index,
    safe_field_access,
)
from fintran.validation.exceptions import ValidatorExecutionError
from fintran.validation.result import ValidationResult


@pytest.fixture
def sample_ir_df() -> pl.DataFrame:
    """Create a sample IR DataFrame for testing."""
    return pl.DataFrame(
        {
            "date": [date(2024, 1, 1), date(2024, 1, 2), date(2024, 1, 3)],
            "account": ["4001", "4002", "5001"],
            "amount": [Decimal("100.00"), Decimal("-50.00"), Decimal("200.00")],
            "currency": ["EUR", "EUR", "USD"],
            "description": ["Revenue", "Expense", "Revenue"],
            "reference": ["REF1", "REF2", "REF3"],
        }
    )


def test_custom_validator_decorator():
    """Test @custom_validator decorator creates a valid validator."""

    @custom_validator("test_validator")
    def validate_test(df: pl.DataFrame) -> ValidationResult:
        """Test validator function."""
        return ValidationResult(is_valid=True, validator_name="test_validator")

    # Check validator_name attribute is set
    assert hasattr(validate_test, "validator_name")
    assert validate_test.validator_name == "test_validator"

    # Check validator can be called
    df = pl.DataFrame({"test": [1, 2, 3]})
    result = validate_test(df)
    assert result.is_valid
    assert result.validator_name == "test_validator"


def test_custom_validator_with_parameters():
    """Test @custom_validator decorator with configuration parameters."""

    @custom_validator("threshold_validator")
    def validate_threshold(
        df: pl.DataFrame, threshold: float = 100.0
    ) -> ValidationResult:
        """Validator with configuration parameter."""
        violations = df.filter(pl.col("amount") > threshold)

        if len(violations) > 0:
            return ValidationResult(
                is_valid=False,
                errors=[f"Found {len(violations)} amounts above threshold"],
                validator_name="threshold_validator",
            )

        return ValidationResult(is_valid=True, validator_name="threshold_validator")

    # Test with default threshold
    df = pl.DataFrame({"amount": [50.0, 150.0, 75.0]})
    result = validate_threshold(df)
    assert not result.is_valid
    assert len(result.errors) == 1

    # Test with custom threshold
    result = validate_threshold(df, threshold=200.0)
    assert result.is_valid


def test_check_required_fields_all_present(sample_ir_df):
    """Test check_required_fields when all fields are present."""
    result = check_required_fields(sample_ir_df, ["account", "amount"], "TestValidator")
    assert result is None


def test_check_required_fields_missing(sample_ir_df):
    """Test check_required_fields when fields are missing."""
    result = check_required_fields(
        sample_ir_df, ["account", "missing_field"], "TestValidator"
    )
    assert result is not None
    assert not result.is_valid
    assert "missing_field" in result.errors[0]
    assert result.validator_name == "TestValidator"


def test_filter_by_patterns(sample_ir_df):
    """Test filter_by_patterns with regex patterns."""
    # Filter to revenue accounts (4xxx)
    filtered = filter_by_patterns(sample_ir_df, "account", ["^4[0-9]{3}"])
    assert len(filtered) == 2
    assert filtered["account"].to_list() == ["4001", "4002"]

    # Filter to expense accounts (5xxx)
    filtered = filter_by_patterns(sample_ir_df, "account", ["^5[0-9]{3}"])
    assert len(filtered) == 1
    assert filtered["account"].to_list() == ["5001"]

    # Filter with multiple patterns
    filtered = filter_by_patterns(sample_ir_df, "account", ["^4001", "^5001"])
    assert len(filtered) == 2


def test_filter_by_patterns_empty():
    """Test filter_by_patterns with empty patterns list."""
    df = pl.DataFrame({"account": ["4001", "4002"]})
    filtered = filter_by_patterns(df, "account", [])
    assert len(filtered) == 0


def test_get_violations_with_index(sample_ir_df):
    """Test get_violations_with_index returns violations and indices."""
    # Find negative amounts
    violations, indices = get_violations_with_index(
        sample_ir_df, pl.col("amount") < 0
    )

    assert len(violations) == 1
    assert len(indices) == 1
    assert indices[0] == 1  # Second row (index 1)
    assert violations["account"].to_list() == ["4002"]


def test_get_violations_with_index_no_violations(sample_ir_df):
    """Test get_violations_with_index when no violations exist."""
    violations, indices = get_violations_with_index(
        sample_ir_df, pl.col("amount") > 1000
    )

    assert len(violations) == 0
    assert len(indices) == 0


def test_format_violation_error():
    """Test format_violation_error creates properly formatted messages."""
    error = format_violation_error(
        row_index=5,
        field="amount",
        value=-150.00,
        message="has negative amount",
        account="4001",
    )

    assert "Field 'amount'" in error
    assert "has negative amount" in error
    assert "-150.0" in error  # Python formats -150.00 as -150.0
    assert "row: 5" in error
    assert "account: 4001" in error


def test_format_violation_error_minimal():
    """Test format_violation_error with minimal parameters."""
    error = format_violation_error(
        row_index=10, field="currency", value="XXX", message="is invalid"
    )

    assert "Field 'currency'" in error
    assert "is invalid" in error
    assert "XXX" in error
    assert "row: 10" in error


def test_format_group_error():
    """Test format_group_error creates properly formatted messages."""
    error = format_group_error(
        group_key="1001",
        field="currency",
        values=["EUR", "USD"],
        message="has multiple currencies",
        row_indices=[5, 12],
    )

    assert "Group '1001'" in error
    assert "has multiple currencies" in error
    assert "field 'currency'" in error
    assert "EUR, USD" in error
    assert "rows: [5, 12]" in error


def test_format_group_error_tuple_key():
    """Test format_group_error with tuple group key."""
    error = format_group_error(
        group_key=("2024-01-01", "1001"),
        field="amount",
        values=[100.0, 200.0],
        message="has conflicting amounts",
    )

    assert "Group '2024-01-01, 1001'" in error
    assert "has conflicting amounts" in error


def test_format_group_error_many_indices():
    """Test format_group_error truncates long index lists."""
    indices = list(range(20))
    error = format_group_error(
        group_key="1001",
        field="currency",
        values=["EUR", "USD"],
        message="has issues",
        row_indices=indices,
    )

    assert "..." in error  # Should truncate after 10 indices


def test_aggregate_by_group(sample_ir_df):
    """Test aggregate_by_group performs group-by aggregation."""
    # Count rows per currency
    result = aggregate_by_group(
        sample_ir_df,
        group_by=["currency"],
        agg_expr=pl.col("account").count().alias("count"),
    )

    assert len(result) == 2  # EUR and USD
    assert set(result["currency"].to_list()) == {"EUR", "USD"}

    # Get counts
    eur_count = result.filter(pl.col("currency") == "EUR")["count"][0]
    usd_count = result.filter(pl.col("currency") == "USD")["count"][0]
    assert eur_count == 2
    assert usd_count == 1


def test_aggregate_by_group_multiple_keys(sample_ir_df):
    """Test aggregate_by_group with multiple group keys."""
    result = aggregate_by_group(
        sample_ir_df,
        group_by=["currency", "account"],
        agg_expr=pl.col("amount").sum().alias("total"),
    )

    assert len(result) == 3  # One row per unique (currency, account) pair


def test_safe_field_access_success(sample_ir_df):
    """Test safe_field_access returns field when it exists."""
    amounts = safe_field_access(sample_ir_df, "amount", "TestValidator")
    assert isinstance(amounts, pl.Series)
    assert len(amounts) == 3


def test_safe_field_access_missing():
    """Test safe_field_access raises error when field is missing."""
    df = pl.DataFrame({"account": ["4001", "4002"]})

    with pytest.raises(ValidatorExecutionError) as exc_info:
        safe_field_access(df, "missing_field", "TestValidator")

    assert "missing_field" in str(exc_info.value)
    assert "TestValidator" in str(exc_info.value)


def test_custom_validator_base_class():
    """Test CustomValidatorBase provides template for custom validators."""

    class TestValidator(CustomValidatorBase):
        """Test validator implementation."""

        def __init__(self, threshold: float):
            self.threshold = threshold
            self.validator_name = "test_validator"

        def validate(self, df: pl.DataFrame) -> ValidationResult:
            """Validate amounts are below threshold."""
            error = check_required_fields(df, ["amount"], self.validator_name)
            if error:
                return error

            violations, indices = get_violations_with_index(
                df, pl.col("amount") > self.threshold
            )

            if len(violations) > 0:
                errors = [
                    format_violation_error(
                        row_index=idx,
                        field="amount",
                        value=row["amount"],
                        message=f"exceeds threshold {self.threshold}",
                    )
                    for idx, row in zip(indices, violations.iter_rows(named=True))
                ]

                return ValidationResult(
                    is_valid=False,
                    errors=errors,
                    validator_name=self.validator_name,
                )

            return ValidationResult(is_valid=True, validator_name=self.validator_name)

    # Test the custom validator
    df = pl.DataFrame({"amount": [50.0, 150.0, 75.0]})
    validator = TestValidator(threshold=100.0)

    result = validator.validate(df)
    assert not result.is_valid
    assert len(result.errors) == 1
    assert "150.0" in result.errors[0]
    assert "exceeds threshold 100.0" in result.errors[0]


def test_custom_validator_base_not_implemented():
    """Test CustomValidatorBase raises error if validate not implemented."""

    class IncompleteValidator(CustomValidatorBase):
        """Validator that doesn't implement validate."""

        pass

    validator = IncompleteValidator()
    df = pl.DataFrame({"test": [1, 2, 3]})

    with pytest.raises(NotImplementedError) as exc_info:
        validator.validate(df)

    assert "must implement validate()" in str(exc_info.value)


def test_balance_check_example():
    """Test the balance check example from the docstring."""

    @custom_validator("balance_check")
    def validate_balance(df: pl.DataFrame, tolerance: float = 0.01) -> ValidationResult:
        """Check that debits equal credits within tolerance."""
        debits = df.filter(pl.col("amount") < 0)["amount"].sum()
        credits = df.filter(pl.col("amount") > 0)["amount"].sum()

        balance = abs(debits + credits)

        if balance > tolerance:
            return ValidationResult(
                is_valid=False,
                errors=[
                    f"Debits and credits don't balance: {debits:.2f} + {credits:.2f} = {balance:.2f}"
                ],
                validator_name="balance_check",
            )

        return ValidationResult(is_valid=True, validator_name="balance_check")

    # Test with balanced data
    balanced_df = pl.DataFrame(
        {"amount": [Decimal("100.00"), Decimal("-100.00"), Decimal("50.00"), Decimal("-50.00")]}
    )
    result = validate_balance(balanced_df)
    assert result.is_valid

    # Test with unbalanced data
    unbalanced_df = pl.DataFrame(
        {"amount": [Decimal("100.00"), Decimal("-50.00")]}
    )
    result = validate_balance(unbalanced_df, tolerance=0.01)
    assert not result.is_valid
    assert "don't balance" in result.errors[0]
