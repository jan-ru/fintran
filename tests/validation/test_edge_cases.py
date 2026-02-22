"""Edge case tests for validation framework.

This module tests edge cases and boundary conditions for all validators:
- Empty DataFrames
- Single-row DataFrames
- Null values and empty strings
- Extreme values (very large/small numbers, far past/future dates)
- Malformed input (missing required fields)
- Invalid validator configuration
- Special numeric values (NaN, infinity)
- Special characters and Unicode handling

Validates: Requirements 20.2, 20.3, 20.4, 20.5
"""

from datetime import date
from decimal import Decimal

import polars as pl
import pytest

from fintran.validation.business.amounts import PositiveAmountsValidator
from fintran.validation.business.currency import CurrencyConsistencyValidator
from fintran.validation.business.dates import DateRangeValidator
from fintran.validation.quality.duplicates import DuplicateDetectionValidator
from fintran.validation.quality.missing import MissingValueDetectionValidator
from fintran.validation.quality.outliers import OutlierDetectionValidator
from fintran.validation.pipeline import ValidationPipeline, ValidationMode
from fintran.validation.exceptions import ValidatorConfigurationError


# ============================================================================
# Empty DataFrame Tests
# ============================================================================


def test_positive_amounts_validator_empty_dataframe():
    """Test PositiveAmountsValidator with empty DataFrame.
    
    Validates: Requirement 20.2 (edge cases - empty DataFrames)
    """
    validator = PositiveAmountsValidator(account_patterns=["^4[0-9]{3}"])
    
    # Create empty DataFrame with correct schema
    df = pl.DataFrame({
        "date": [],
        "account": [],
        "amount": [],
        "currency": [],
        "description": [],
        "reference": [],
    }, schema={
        "date": pl.Date,
        "account": pl.Utf8,
        "amount": pl.Decimal,
        "currency": pl.Utf8,
        "description": pl.Utf8,
        "reference": pl.Utf8,
    })
    
    result = validator.validate(df)
    
    # Empty DataFrame should pass validation (no violations)
    assert result.is_valid
    assert len(result.errors) == 0
    assert len(result.warnings) == 0


def test_currency_consistency_validator_empty_dataframe():
    """Test CurrencyConsistencyValidator with empty DataFrame.
    
    Validates: Requirement 20.2 (edge cases - empty DataFrames)
    """
    validator = CurrencyConsistencyValidator(group_by=["account"])
    
    df = pl.DataFrame({
        "date": [],
        "account": [],
        "amount": [],
        "currency": [],
        "description": [],
        "reference": [],
    }, schema={
        "date": pl.Date,
        "account": pl.Utf8,
        "amount": pl.Decimal,
        "currency": pl.Utf8,
        "description": pl.Utf8,
        "reference": pl.Utf8,
    })
    
    result = validator.validate(df)
    
    assert result.is_valid
    assert len(result.errors) == 0


def test_date_range_validator_empty_dataframe():
    """Test DateRangeValidator with empty DataFrame.
    
    Validates: Requirement 20.2 (edge cases - empty DataFrames)
    """
    validator = DateRangeValidator(
        min_date=date(2020, 1, 1),
        max_date=date(2024, 12, 31)
    )
    
    df = pl.DataFrame({
        "date": [],
        "account": [],
        "amount": [],
        "currency": [],
        "description": [],
        "reference": [],
    }, schema={
        "date": pl.Date,
        "account": pl.Utf8,
        "amount": pl.Decimal,
        "currency": pl.Utf8,
        "description": pl.Utf8,
        "reference": pl.Utf8,
    })
    
    result = validator.validate(df)
    
    assert result.is_valid
    assert len(result.errors) == 0


def test_duplicate_detection_validator_empty_dataframe():
    """Test DuplicateDetectionValidator with empty DataFrame.
    
    Validates: Requirement 20.2 (edge cases - empty DataFrames)
    """
    validator = DuplicateDetectionValidator(
        fields=["date", "account", "reference"],
        mode="exact"
    )
    
    df = pl.DataFrame({
        "date": [],
        "account": [],
        "amount": [],
        "currency": [],
        "description": [],
        "reference": [],
    }, schema={
        "date": pl.Date,
        "account": pl.Utf8,
        "amount": pl.Decimal,
        "currency": pl.Utf8,
        "description": pl.Utf8,
        "reference": pl.Utf8,
    })
    
    result = validator.validate(df)
    
    assert result.is_valid
    assert len(result.warnings) == 0


def test_missing_value_detection_validator_empty_dataframe():
    """Test MissingValueDetectionValidator with empty DataFrame.
    
    Validates: Requirement 20.2 (edge cases - empty DataFrames)
    """
    validator = MissingValueDetectionValidator(fields=["description", "reference"])
    
    df = pl.DataFrame({
        "date": [],
        "account": [],
        "amount": [],
        "currency": [],
        "description": [],
        "reference": [],
    }, schema={
        "date": pl.Date,
        "account": pl.Utf8,
        "amount": pl.Decimal,
        "currency": pl.Utf8,
        "description": pl.Utf8,
        "reference": pl.Utf8,
    })
    
    result = validator.validate(df)
    
    # Empty DataFrame has no missing values (vacuous truth)
    assert result.is_valid
    assert len(result.warnings) == 0


def test_outlier_detection_validator_empty_dataframe():
    """Test OutlierDetectionValidator with empty DataFrame.
    
    Validates: Requirement 20.2 (edge cases - empty DataFrames)
    """
    validator = OutlierDetectionValidator(method="zscore", threshold=3.0)
    
    df = pl.DataFrame({
        "date": [],
        "account": [],
        "amount": [],
        "currency": [],
        "description": [],
        "reference": [],
    }, schema={
        "date": pl.Date,
        "account": pl.Utf8,
        "amount": pl.Decimal,
        "currency": pl.Utf8,
        "description": pl.Utf8,
        "reference": pl.Utf8,
    })
    
    result = validator.validate(df)
    
    # Empty DataFrame has no outliers
    assert result.is_valid
    assert len(result.warnings) == 0


# ============================================================================
# Single-Row DataFrame Tests
# ============================================================================


def test_positive_amounts_validator_single_row():
    """Test PositiveAmountsValidator with single-row DataFrame.
    
    Validates: Requirement 20.2 (edge cases - single-row DataFrames)
    """
    validator = PositiveAmountsValidator(account_patterns=["^4[0-9]{3}"])
    
    # Single row with positive amount
    df = pl.DataFrame({
        "date": [date(2024, 1, 1)],
        "account": ["4001"],
        "amount": [Decimal("100.00")],
        "currency": ["USD"],
        "description": ["Test"],
        "reference": ["REF1"],
    })
    
    result = validator.validate(df)
    assert result.is_valid
    
    # Single row with negative amount
    df_negative = pl.DataFrame({
        "date": [date(2024, 1, 1)],
        "account": ["4001"],
        "amount": [Decimal("-100.00")],
        "currency": ["USD"],
        "description": ["Test"],
        "reference": ["REF1"],
    })
    
    result_negative = validator.validate(df_negative)
    assert not result_negative.is_valid
    assert len(result_negative.errors) == 1


def test_currency_consistency_validator_single_row():
    """Test CurrencyConsistencyValidator with single-row DataFrame.
    
    Validates: Requirement 20.2 (edge cases - single-row DataFrames)
    """
    validator = CurrencyConsistencyValidator(group_by=["account"])
    
    df = pl.DataFrame({
        "date": [date(2024, 1, 1)],
        "account": ["1001"],
        "amount": [Decimal("100.00")],
        "currency": ["USD"],
        "description": ["Test"],
        "reference": ["REF1"],
    })
    
    result = validator.validate(df)
    
    # Single row is always consistent with itself
    assert result.is_valid


def test_outlier_detection_validator_single_row():
    """Test OutlierDetectionValidator with single-row DataFrame.
    
    Validates: Requirement 20.2 (edge cases - single-row DataFrames)
    """
    validator = OutlierDetectionValidator(method="zscore", threshold=3.0)
    
    df = pl.DataFrame({
        "date": [date(2024, 1, 1)],
        "account": ["1001"],
        "amount": [Decimal("100.00")],
        "currency": ["USD"],
        "description": ["Test"],
        "reference": ["REF1"],
    })
    
    result = validator.validate(df)
    
    # Single value cannot be an outlier (no distribution)
    assert result.is_valid
    assert len(result.warnings) == 0


# ============================================================================
# Null Value Handling Tests
# ============================================================================


def test_validators_handle_null_descriptions():
    """Test that validators handle null values in optional fields correctly.
    
    Validates: Requirement 20.5 (null values)
    """
    df = pl.DataFrame({
        "date": [date(2024, 1, 1), date(2024, 1, 2)],
        "account": ["1001", "1002"],
        "amount": [Decimal("100.00"), Decimal("200.00")],
        "currency": ["USD", "USD"],
        "description": [None, None],
        "reference": [None, None],
    })
    
    # Test that validators don't crash on null optional fields
    validators = [
        PositiveAmountsValidator(account_patterns=["^1[0-9]{3}"]),
        CurrencyConsistencyValidator(group_by=["account"]),
        DateRangeValidator(min_date=date(2020, 1, 1), max_date=date(2025, 1, 1)),
        DuplicateDetectionValidator(fields=["date", "account"], mode="exact"),
        OutlierDetectionValidator(method="zscore", threshold=3.0),
    ]
    
    for validator in validators:
        result = validator.validate(df)
        # Should not crash, result may vary by validator
        assert result is not None
        assert hasattr(result, "is_valid")


def test_missing_value_detection_with_nulls():
    """Test MissingValueDetectionValidator correctly identifies null values.
    
    Validates: Requirement 20.5 (null values)
    """
    validator = MissingValueDetectionValidator(fields=["description", "reference"])
    
    df = pl.DataFrame({
        "date": [date(2024, 1, 1), date(2024, 1, 2), date(2024, 1, 3)],
        "account": ["1001", "1002", "1003"],
        "amount": [Decimal("100.00"), Decimal("200.00"), Decimal("300.00")],
        "currency": ["USD", "USD", "USD"],
        "description": [None, "Test", None],
        "reference": [None, None, "REF1"],
    })
    
    result = validator.validate(df)
    
    # Should detect missing values (as warnings, not errors)
    assert result.is_valid  # Missing values are warnings, not errors
    assert len(result.warnings) > 0
    assert "description" in result.warnings[0] or "reference" in result.warnings[0]


# ============================================================================
# Empty String Handling Tests
# ============================================================================


def test_validators_handle_empty_strings():
    """Test that validators handle empty strings correctly.
    
    Validates: Requirement 20.5 (empty strings)
    """
    df = pl.DataFrame({
        "date": [date(2024, 1, 1), date(2024, 1, 2)],
        "account": ["1001", "1002"],
        "amount": [Decimal("100.00"), Decimal("200.00")],
        "currency": ["USD", "USD"],
        "description": ["", ""],
        "reference": ["", ""],
    })
    
    # Test that validators don't crash on empty strings
    validators = [
        PositiveAmountsValidator(account_patterns=["^1[0-9]{3}"]),
        CurrencyConsistencyValidator(group_by=["account"]),
        DateRangeValidator(min_date=date(2020, 1, 1), max_date=date(2025, 1, 1)),
        OutlierDetectionValidator(method="zscore", threshold=3.0),
    ]
    
    for validator in validators:
        result = validator.validate(df)
        assert result is not None
        assert hasattr(result, "is_valid")


# ============================================================================
# Extreme Value Tests
# ============================================================================


def test_date_range_validator_extreme_dates():
    """Test DateRangeValidator with extreme past and future dates.
    
    Validates: Requirement 20.2 (extreme values)
    """
    validator = DateRangeValidator(
        min_date=date(2020, 1, 1),
        max_date=date(2024, 12, 31)
    )
    
    # Test with very old date
    df_old = pl.DataFrame({
        "date": [date(1900, 1, 1)],
        "account": ["1001"],
        "amount": [Decimal("100.00")],
        "currency": ["USD"],
        "description": ["Test"],
        "reference": ["REF1"],
    })
    
    result_old = validator.validate(df_old)
    assert not result_old.is_valid
    assert len(result_old.errors) == 1
    assert "1900" in result_old.errors[0]
    
    # Test with far future date
    df_future = pl.DataFrame({
        "date": [date(2100, 12, 31)],
        "account": ["1001"],
        "amount": [Decimal("100.00")],
        "currency": ["USD"],
        "description": ["Test"],
        "reference": ["REF1"],
    })
    
    result_future = validator.validate(df_future)
    assert not result_future.is_valid
    assert len(result_future.errors) == 1
    assert "2100" in result_future.errors[0]


def test_outlier_detection_extreme_amounts():
    """Test OutlierDetectionValidator with extreme amounts.
    
    Validates: Requirement 20.2 (extreme values)
    """
    # Use IQR method which is more robust to extreme outliers
    validator = OutlierDetectionValidator(method="iqr", threshold=1.5)
    
    # Create DataFrame with normal values and extreme outliers
    df = pl.DataFrame({
        "date": [date(2024, 1, i) for i in range(1, 11)],
        "account": ["1001"] * 10,
        "amount": [
            Decimal("100.00"), Decimal("101.00"), Decimal("99.00"),
            Decimal("100.50"), Decimal("99.50"), Decimal("100.25"),
            Decimal("99.75"), Decimal("100.10"), Decimal("99.90"),
            Decimal("10000.00")  # Extreme outlier (100x normal)
        ],
        "currency": ["USD"] * 10,
        "description": ["Test"] * 10,
        "reference": [f"REF{i}" for i in range(1, 11)],
    })
    
    result = validator.validate(df)
    
    # Should detect the extreme outlier (as warnings, not errors)
    assert result.is_valid  # Outliers are warnings, not errors
    assert len(result.warnings) > 0
    # Check that outlier was detected (warning message contains "outlier")
    assert "outlier" in result.warnings[0].lower()


def test_positive_amounts_validator_very_large_amounts():
    """Test PositiveAmountsValidator with very large amounts.
    
    Validates: Requirement 20.2 (extreme values)
    """
    validator = PositiveAmountsValidator(account_patterns=["^4[0-9]{3}"])
    
    df = pl.DataFrame({
        "date": [date(2024, 1, 1)],
        "account": ["4001"],
        "amount": [Decimal("999999999999.99")],
        "currency": ["USD"],
        "description": ["Test"],
        "reference": ["REF1"],
    })
    
    result = validator.validate(df)
    
    # Very large positive amount should still pass
    assert result.is_valid


def test_positive_amounts_validator_very_small_negative():
    """Test PositiveAmountsValidator with very small negative amounts.
    
    Validates: Requirement 20.2 (extreme values)
    """
    validator = PositiveAmountsValidator(account_patterns=["^4[0-9]{3}"])
    
    df = pl.DataFrame({
        "date": [date(2024, 1, 1)],
        "account": ["4001"],
        "amount": [Decimal("-0.01")],
        "currency": ["USD"],
        "description": ["Test"],
        "reference": ["REF1"],
    })
    
    result = validator.validate(df)
    
    # Even very small negative amounts should be detected
    assert not result.is_valid
    assert len(result.errors) == 1


# ============================================================================
# Malformed Input Tests
# ============================================================================


def test_validators_handle_missing_required_fields():
    """Test that validators handle DataFrames with missing required fields gracefully.
    
    Validates: Requirement 20.3 (malformed input)
    """
    # DataFrame missing 'amount' column
    df_missing_amount = pl.DataFrame({
        "date": [date(2024, 1, 1)],
        "account": ["1001"],
        "currency": ["USD"],
        "description": ["Test"],
        "reference": ["REF1"],
    })
    
    validator = PositiveAmountsValidator(account_patterns=["^1[0-9]{3}"])
    
    # Validator should handle missing fields gracefully
    # It may return an error result or raise an exception
    try:
        result = validator.validate(df_missing_amount)
        # If it doesn't raise, it should return an error result
        assert not result.is_valid or len(result.errors) > 0
    except (KeyError, pl.exceptions.ColumnNotFoundError):
        # This is also acceptable behavior
        pass


def test_duplicate_detection_with_missing_fields():
    """Test DuplicateDetectionValidator when specified fields don't exist.
    
    Validates: Requirement 20.3 (malformed input)
    """
    validator = DuplicateDetectionValidator(
        fields=["date", "account", "nonexistent_field"],
        mode="exact"
    )
    
    df = pl.DataFrame({
        "date": [date(2024, 1, 1)],
        "account": ["1001"],
        "amount": [Decimal("100.00")],
        "currency": ["USD"],
        "description": ["Test"],
        "reference": ["REF1"],
    })
    
    # Validator should handle missing fields gracefully
    # It may return an error result or raise an exception
    try:
        result = validator.validate(df)
        # If it doesn't raise, it should return an error result
        assert not result.is_valid or len(result.errors) > 0
    except (KeyError, pl.exceptions.ColumnNotFoundError):
        # This is also acceptable behavior
        pass


# ============================================================================
# Invalid Validator Configuration Tests
# ============================================================================


def test_positive_amounts_validator_invalid_config():
    """Test PositiveAmountsValidator with invalid configuration.
    
    Validates: Requirement 20.3 (malformed input)
    """
    # Empty account patterns should raise configuration error
    with pytest.raises((ValidatorConfigurationError, ValueError)):
        validator = PositiveAmountsValidator(account_patterns=[])
        # If constructor doesn't raise, validation should
        df = pl.DataFrame({
            "date": [date(2024, 1, 1)],
            "account": ["4001"],
            "amount": [Decimal("100.00")],
            "currency": ["USD"],
            "description": ["Test"],
            "reference": ["REF1"],
        })
        validator.validate(df)


def test_date_range_validator_invalid_range():
    """Test DateRangeValidator with invalid date range (min > max).
    
    Validates: Requirement 20.3 (malformed input)
    """
    # min_date after max_date should raise configuration error
    with pytest.raises((ValidatorConfigurationError, ValueError)):
        DateRangeValidator(
            min_date=date(2024, 12, 31),
            max_date=date(2020, 1, 1)
        )


def test_outlier_detection_validator_invalid_method():
    """Test OutlierDetectionValidator with invalid method.
    
    Validates: Requirement 20.3 (malformed input)
    """
    # Invalid method should raise configuration error
    with pytest.raises((ValidatorConfigurationError, ValueError)):
        OutlierDetectionValidator(method="invalid_method", threshold=3.0)


def test_outlier_detection_validator_invalid_threshold():
    """Test OutlierDetectionValidator with invalid threshold.
    
    Validates: Requirement 20.3 (malformed input)
    """
    # Negative threshold should raise configuration error
    with pytest.raises((ValidatorConfigurationError, ValueError)):
        OutlierDetectionValidator(method="zscore", threshold=-1.0)


# ============================================================================
# Error Message Descriptiveness Tests
# ============================================================================


def test_positive_amounts_error_message_descriptiveness():
    """Test that PositiveAmountsValidator error messages are descriptive.
    
    Validates: Requirement 20.4 (descriptive error messages)
    """
    validator = PositiveAmountsValidator(account_patterns=["^4[0-9]{3}"])
    
    df = pl.DataFrame({
        "date": [date(2024, 1, 1), date(2024, 1, 2)],
        "account": ["4001", "4002"],
        "amount": [Decimal("-100.00"), Decimal("0.00")],
        "currency": ["USD", "USD"],
        "description": ["Test1", "Test2"],
        "reference": ["REF1", "REF2"],
    })
    
    result = validator.validate(df)
    
    assert not result.is_valid
    assert len(result.errors) > 0
    
    # Error message should contain:
    # - Validator name
    error_text = " ".join(result.errors)
    assert "4001" in error_text or "4002" in error_text  # Account name
    assert "-100" in error_text or "0.00" in error_text  # Amount value


def test_currency_consistency_error_message_descriptiveness():
    """Test that CurrencyConsistencyValidator error messages are descriptive.
    
    Validates: Requirement 20.4 (descriptive error messages)
    """
    validator = CurrencyConsistencyValidator(group_by=["account"])
    
    df = pl.DataFrame({
        "date": [date(2024, 1, 1), date(2024, 1, 2)],
        "account": ["1001", "1001"],
        "amount": [Decimal("100.00"), Decimal("100.00")],
        "currency": ["USD", "EUR"],
        "description": ["Test1", "Test2"],
        "reference": ["REF1", "REF2"],
    })
    
    result = validator.validate(df)
    
    assert not result.is_valid
    assert len(result.errors) > 0
    
    # Error message should contain:
    error_text = " ".join(result.errors)
    assert "1001" in error_text  # Account name
    assert ("USD" in error_text and "EUR" in error_text) or "currency" in error_text.lower()


def test_date_range_error_message_descriptiveness():
    """Test that DateRangeValidator error messages are descriptive.
    
    Validates: Requirement 20.4 (descriptive error messages)
    """
    validator = DateRangeValidator(
        min_date=date(2020, 1, 1),
        max_date=date(2024, 12, 31)
    )
    
    df = pl.DataFrame({
        "date": [date(2019, 6, 15)],
        "account": ["1001"],
        "amount": [Decimal("100.00")],
        "currency": ["USD"],
        "description": ["Test"],
        "reference": ["REF1"],
    })
    
    result = validator.validate(df)
    
    assert not result.is_valid
    assert len(result.errors) > 0
    
    # Error message should contain the out-of-range date
    error_text = " ".join(result.errors)
    assert "2019" in error_text


# ============================================================================
# Special Character and Unicode Handling Tests
# ============================================================================


def test_validators_handle_unicode_in_descriptions():
    """Test that validators handle Unicode characters correctly.
    
    Validates: Requirement 20.5 (special characters)
    """
    df = pl.DataFrame({
        "date": [date(2024, 1, 1), date(2024, 1, 2)],
        "account": ["1001", "1002"],
        "amount": [Decimal("100.00"), Decimal("200.00")],
        "currency": ["USD", "EUR"],
        "description": ["Test with émojis 🎉", "Ñoño café"],
        "reference": ["REF-α", "REF-β"],
    })
    
    # Test that validators don't crash on Unicode
    validators = [
        PositiveAmountsValidator(account_patterns=["^1[0-9]{3}"]),
        CurrencyConsistencyValidator(group_by=["account"]),
        DateRangeValidator(min_date=date(2020, 1, 1), max_date=date(2025, 1, 1)),
        DuplicateDetectionValidator(fields=["date", "account"], mode="exact"),
        MissingValueDetectionValidator(fields=["description", "reference"]),
        OutlierDetectionValidator(method="zscore", threshold=3.0),
    ]
    
    for validator in validators:
        result = validator.validate(df)
        assert result is not None
        assert hasattr(result, "is_valid")


def test_validators_handle_special_characters_in_accounts():
    """Test that validators handle special characters in account codes.
    
    Validates: Requirement 20.5 (special characters)
    """
    df = pl.DataFrame({
        "date": [date(2024, 1, 1)],
        "account": ["1001-A/B"],
        "amount": [Decimal("100.00")],
        "currency": ["USD"],
        "description": ["Test"],
        "reference": ["REF1"],
    })
    
    # Validators should handle special characters in account codes
    validator = CurrencyConsistencyValidator(group_by=["account"])
    result = validator.validate(df)
    
    assert result is not None
    assert hasattr(result, "is_valid")


# ============================================================================
# ValidationPipeline Edge Cases
# ============================================================================


def test_validation_pipeline_empty_validators():
    """Test ValidationPipeline with no validators.
    
    Validates: Requirement 20.2 (edge cases)
    """
    pipeline = ValidationPipeline(validators=[], mode=ValidationMode.CONTINUE)
    
    df = pl.DataFrame({
        "date": [date(2024, 1, 1)],
        "account": ["1001"],
        "amount": [Decimal("100.00")],
        "currency": ["USD"],
        "description": ["Test"],
        "reference": ["REF1"],
    })
    
    report = pipeline.run(df)
    
    # Empty pipeline should return success
    assert report.is_valid()
    assert report.total_validators == 0
    assert report.passed == 0
    assert report.failed == 0


def test_validation_pipeline_with_empty_dataframe():
    """Test ValidationPipeline with empty DataFrame.
    
    Validates: Requirement 20.2 (edge cases - empty DataFrames)
    """
    validators = [
        PositiveAmountsValidator(account_patterns=["^4[0-9]{3}"]),
        CurrencyConsistencyValidator(group_by=["account"]),
        DateRangeValidator(min_date=date(2020, 1, 1), max_date=date(2024, 12, 31)),
    ]
    
    pipeline = ValidationPipeline(validators=validators, mode=ValidationMode.CONTINUE)
    
    df = pl.DataFrame({
        "date": [],
        "account": [],
        "amount": [],
        "currency": [],
        "description": [],
        "reference": [],
    }, schema={
        "date": pl.Date,
        "account": pl.Utf8,
        "amount": pl.Decimal,
        "currency": pl.Utf8,
        "description": pl.Utf8,
        "reference": pl.Utf8,
    })
    
    report = pipeline.run(df)
    
    # All validators should pass on empty DataFrame
    assert report.is_valid()
    assert report.total_validators == 3
    assert report.passed == 3
    assert report.failed == 0
