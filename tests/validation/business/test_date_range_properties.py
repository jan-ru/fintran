"""Property-based tests for DateRangeValidator.

This module implements Property 5: Date Range Validation
Validates Requirements: 5.2, 5.3, 5.4, 5.5, 20.1
"""

from datetime import date, timedelta
from decimal import Decimal as PyDecimal

import polars as pl
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from fintran.validation.business.dates import DateRangeValidator
from fintran.validation.result import ValidationResult


# Hypothesis strategies for date range testing

@st.composite
def ir_with_dates_in_range(
    draw: st.DrawFn,
    min_date: date | None = None,
    max_date: date | None = None,
) -> pl.DataFrame:
    """Generate IR DataFrame with dates within specified range.
    
    Args:
        draw: Hypothesis draw function
        min_date: Minimum date (inclusive)
        max_date: Maximum date (inclusive)
        
    Returns:
        IR DataFrame where all dates are within range
    """
    # Default range if not specified
    if min_date is None:
        min_date = date(2020, 1, 1)
    if max_date is None:
        max_date = date(2024, 12, 31)
    
    # Generate number of rows (1-20)
    size = draw(st.integers(min_value=1, max_value=20))
    
    # Generate dates within range
    dates = draw(st.lists(
        st.dates(min_value=min_date, max_value=max_date),
        min_size=size,
        max_size=size
    ))
    
    accounts = draw(st.lists(
        st.text(
            alphabet=st.characters(whitelist_categories=("Lu", "Ll", "Nd")),
            min_size=1,
            max_size=10
        ),
        min_size=size,
        max_size=size
    ))
    
    amounts = draw(st.lists(
        st.decimals(
            min_value=PyDecimal("-999999.99"),
            max_value=PyDecimal("999999.99"),
            allow_nan=False,
            allow_infinity=False,
            places=2
        ),
        min_size=size,
        max_size=size
    ))
    
    currencies = draw(st.lists(
        st.sampled_from(["USD", "EUR", "GBP", "JPY"]),
        min_size=size,
        max_size=size
    ))
    
    df = pl.DataFrame({
        "date": dates,
        "account": accounts,
        "amount": amounts,
        "currency": currencies,
        "description": [None] * size,
        "reference": [None] * size,
    })
    
    df = df.with_columns(pl.col("amount").cast(pl.Decimal(precision=38, scale=10)))
    
    return df


@st.composite
def ir_with_dates_outside_range(
    draw: st.DrawFn,
    min_date: date | None = None,
    max_date: date | None = None,
) -> pl.DataFrame:
    """Generate IR DataFrame with at least one date outside specified range.
    
    Args:
        draw: Hypothesis draw function
        min_date: Minimum date (inclusive)
        max_date: Maximum date (inclusive)
        
    Returns:
        IR DataFrame where at least one date is out of range
    """
    # Default range if not specified
    if min_date is None:
        min_date = date(2020, 1, 1)
    if max_date is None:
        max_date = date(2024, 12, 31)
    
    # Generate number of rows (2-20, need at least 2 to have violations)
    size = draw(st.integers(min_value=2, max_value=20))
    
    # Generate mix of in-range and out-of-range dates
    dates = []
    
    # At least one violation
    violation_type = draw(st.sampled_from(["before_min", "after_max", "both"]))
    
    if violation_type == "before_min" or violation_type == "both":
        # Add date before min_date
        days_before = draw(st.integers(min_value=1, max_value=365))
        dates.append(min_date - timedelta(days=days_before))
    
    if violation_type == "after_max" or violation_type == "both":
        # Add date after max_date
        days_after = draw(st.integers(min_value=1, max_value=365))
        dates.append(max_date + timedelta(days=days_after))
    
    # Fill remaining with mix of in-range and potentially more out-of-range dates
    remaining = size - len(dates)
    for _ in range(remaining):
        choice = draw(st.sampled_from(["in_range", "before_min", "after_max"]))
        
        if choice == "in_range":
            dates.append(draw(st.dates(min_value=min_date, max_value=max_date)))
        elif choice == "before_min":
            days_before = draw(st.integers(min_value=1, max_value=365))
            dates.append(min_date - timedelta(days=days_before))
        else:  # after_max
            days_after = draw(st.integers(min_value=1, max_value=365))
            dates.append(max_date + timedelta(days=days_after))
    
    accounts = draw(st.lists(
        st.text(
            alphabet=st.characters(whitelist_categories=("Lu", "Ll", "Nd")),
            min_size=1,
            max_size=10
        ),
        min_size=size,
        max_size=size
    ))
    
    amounts = draw(st.lists(
        st.decimals(
            min_value=PyDecimal("-999999.99"),
            max_value=PyDecimal("999999.99"),
            allow_nan=False,
            allow_infinity=False,
            places=2
        ),
        min_size=size,
        max_size=size
    ))
    
    currencies = draw(st.lists(
        st.sampled_from(["USD", "EUR", "GBP", "JPY"]),
        min_size=size,
        max_size=size
    ))
    
    df = pl.DataFrame({
        "date": dates,
        "account": accounts,
        "amount": amounts,
        "currency": currencies,
        "description": [None] * size,
        "reference": [None] * size,
    })
    
    df = df.with_columns(pl.col("amount").cast(pl.Decimal(precision=38, scale=10)))
    
    return df


# Property-based tests

class TestDateRangeValidatorProperties:
    """Property-based tests for DateRangeValidator.
    
    Implements Property 5: Date Range Validation
    Validates Requirements: 5.2, 5.3, 5.4, 5.5, 20.1
    """
    
    @given(ir_with_dates_in_range())
    @settings(max_examples=50, deadline=None)
    def test_property_dates_in_range_pass(self, df: pl.DataFrame):
        """Property: Validator passes when all dates are within range.
        
        Validates Requirement 5.4: When all dates are within range, the validator
        shall return a ValidationResult indicating success.
        """
        # Use fixed range that matches the strategy defaults
        min_date = date(2020, 1, 1)
        max_date = date(2024, 12, 31)
        
        validator = DateRangeValidator(min_date=min_date, max_date=max_date)
        result = validator.validate(df)
        
        assert result.is_valid, f"Expected validation to pass for dates in range, but got errors: {result.errors}"
        assert not result.has_errors()
        assert result.validator_name == "DateRangeValidator"
    
    @given(ir_with_dates_outside_range())
    @settings(max_examples=50, deadline=None)
    def test_property_dates_outside_range_fail(self, df: pl.DataFrame):
        """Property: Validator detects dates outside specified range.
        
        Validates Requirement 5.3: If any date falls outside the range, the validator
        shall return a ValidationResult with errors identifying the row indices and
        out-of-range dates.
        """
        # Use fixed range that matches the strategy defaults
        min_date = date(2020, 1, 1)
        max_date = date(2024, 12, 31)
        
        validator = DateRangeValidator(min_date=min_date, max_date=max_date)
        result = validator.validate(df)
        
        assert not result.is_valid, "Expected validation to fail for dates outside range"
        assert result.has_errors()
        assert len(result.errors) > 0
        
        # Verify error messages contain date information
        error_message = " ".join(result.errors)
        assert "date" in error_message.lower() or "row" in error_message.lower()
        
        # Verify metadata contains violation details
        assert "violations" in result.metadata or "violation_count" in result.metadata
    
    @given(ir_with_dates_outside_range())
    @settings(max_examples=50, deadline=None)
    def test_property_min_date_only(self, df: pl.DataFrame):
        """Property: Validator works with only minimum date specified.
        
        Validates Requirement 5.5: The validator shall support optional boundaries
        (only min, only max, or both).
        """
        # Use a min_date that's within the strategy's range
        min_date = date(2020, 1, 1)
        
        # Validator with only min_date
        validator = DateRangeValidator(min_date=min_date)
        result = validator.validate(df)
        
        # Check if there are any dates before min_date
        dates_before_min = df.filter(pl.col("date") < min_date)
        
        if len(dates_before_min) > 0:
            assert not result.is_valid
            assert result.has_errors()
        else:
            assert result.is_valid
    
    @given(ir_with_dates_outside_range())
    @settings(max_examples=50, deadline=None)
    def test_property_max_date_only(self, df: pl.DataFrame):
        """Property: Validator works with only maximum date specified.
        
        Validates Requirement 5.5: The validator shall support optional boundaries
        (only min, only max, or both).
        """
        # Use a max_date that's within the strategy's range
        max_date = date(2024, 12, 31)
        
        # Validator with only max_date
        validator = DateRangeValidator(max_date=max_date)
        result = validator.validate(df)
        
        # Check if there are any dates after max_date
        dates_after_max = df.filter(pl.col("date") > max_date)
        
        if len(dates_after_max) > 0:
            assert not result.is_valid
            assert result.has_errors()
        else:
            assert result.is_valid
    
    @given(ir_with_dates_in_range())
    @settings(max_examples=50, deadline=None)
    def test_property_determinism(self, df: pl.DataFrame):
        """Property: Validator is deterministic (same input produces same result).
        
        Validates Requirement 1.5: For all Validator implementations, applying
        the same validator twice to the same input shall produce equivalent results.
        """
        validator = DateRangeValidator(min_date=date(2020, 1, 1), max_date=date(2024, 12, 31))
        
        result1 = validator.validate(df)
        result2 = validator.validate(df)
        
        assert result1.is_valid == result2.is_valid
        assert result1.errors == result2.errors
        assert result1.warnings == result2.warnings
    
    @given(ir_with_dates_outside_range())
    @settings(max_examples=50, deadline=None)
    def test_property_immutability(self, df: pl.DataFrame):
        """Property: Validator does not modify input DataFrame.
        
        Validates Requirement 18.1: For all validators, the test suite shall verify
        that the input DataFrame is not modified during validation.
        """
        # Create a copy to compare against
        df_copy = df.clone()
        
        validator = DateRangeValidator(min_date=date(2020, 1, 1), max_date=date(2024, 12, 31))
        _ = validator.validate(df)
        
        # Verify DataFrame was not modified
        assert df.equals(df_copy), "Validator modified the input DataFrame"
    
    @given(ir_with_dates_outside_range())
    @settings(max_examples=50, deadline=None)
    def test_property_error_message_completeness(self, df: pl.DataFrame):
        """Property: Error messages contain validator name and violation details.
        
        Validates Requirement 16.1, 16.2, 16.3: Error messages shall include
        validator name, affected row indices, and specific dates that are out of range.
        """
        validator = DateRangeValidator(min_date=date(2020, 1, 1), max_date=date(2024, 12, 31))
        result = validator.validate(df)
        
        if result.has_errors():
            # Check validator name is present
            assert result.validator_name == "DateRangeValidator"
            
            # Check error messages contain relevant information
            error_text = " ".join(result.errors).lower()
            assert "date" in error_text or "row" in error_text
            
            # Check metadata contains violation details
            assert result.metadata is not None
            assert len(result.metadata) > 0


# Edge case tests

class TestDateRangeValidatorEdgeCases:
    """Edge case tests for DateRangeValidator.
    
    Validates Requirement 20.2, 20.3, 20.4, 20.5: Validators handle edge cases correctly.
    """
    
    def test_empty_dataframe(self):
        """Test validator with empty DataFrame."""
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
            "amount": pl.Decimal(precision=38, scale=10),
            "currency": pl.Utf8,
            "description": pl.Utf8,
            "reference": pl.Utf8,
        })
        
        validator = DateRangeValidator(min_date=date(2020, 1, 1), max_date=date(2024, 12, 31))
        result = validator.validate(df)
        
        # Empty DataFrame should pass (no violations)
        assert result.is_valid
        assert not result.has_errors()
    
    def test_single_row_dataframe(self):
        """Test validator with single-row DataFrame."""
        df = pl.DataFrame({
            "date": [date(2024, 1, 1)],
            "account": ["ACC1"],
            "amount": [PyDecimal("100.00")],
            "currency": ["USD"],
            "description": [None],
            "reference": [None],
        })
        df = df.with_columns(pl.col("amount").cast(pl.Decimal(precision=38, scale=10)))
        
        validator = DateRangeValidator(min_date=date(2020, 1, 1), max_date=date(2024, 12, 31))
        result = validator.validate(df)
        
        # Single row within range should pass
        assert result.is_valid
        assert not result.has_errors()
    
    def test_missing_date_field(self):
        """Test validator returns error when date field is missing."""
        df = pl.DataFrame({
            "account": ["ACC1"],
            "amount": [PyDecimal("100.00")],
            "currency": ["USD"],
        })
        df = df.with_columns(pl.col("amount").cast(pl.Decimal(precision=38, scale=10)))
        
        validator = DateRangeValidator(min_date=date(2020, 1, 1))
        result = validator.validate(df)
        
        assert not result.is_valid
        assert result.has_errors()
        assert "date" in result.errors[0].lower()
    
    def test_date_exactly_at_boundaries(self):
        """Test validator handles dates exactly at min/max boundaries."""
        df = pl.DataFrame({
            "date": [date(2020, 1, 1), date(2024, 12, 31)],
            "account": ["ACC1", "ACC2"],
            "amount": [PyDecimal("100.00"), PyDecimal("200.00")],
            "currency": ["USD", "USD"],
            "description": [None, None],
            "reference": [None, None],
        })
        df = df.with_columns(pl.col("amount").cast(pl.Decimal(precision=38, scale=10)))
        
        validator = DateRangeValidator(min_date=date(2020, 1, 1), max_date=date(2024, 12, 31))
        result = validator.validate(df)
        
        # Dates exactly at boundaries should pass (inclusive)
        assert result.is_valid
        assert not result.has_errors()
    
    def test_date_one_day_outside_boundaries(self):
        """Test validator detects dates one day outside boundaries."""
        df = pl.DataFrame({
            "date": [date(2019, 12, 31), date(2025, 1, 1)],
            "account": ["ACC1", "ACC2"],
            "amount": [PyDecimal("100.00"), PyDecimal("200.00")],
            "currency": ["USD", "USD"],
            "description": [None, None],
            "reference": [None, None],
        })
        df = df.with_columns(pl.col("amount").cast(pl.Decimal(precision=38, scale=10)))
        
        validator = DateRangeValidator(min_date=date(2020, 1, 1), max_date=date(2024, 12, 31))
        result = validator.validate(df)
        
        # Both dates are one day outside boundaries
        assert not result.is_valid
        assert result.has_errors()
        assert len(result.errors) == 2


# Configuration tests

class TestDateRangeValidatorConfiguration:
    """Tests for DateRangeValidator configuration.
    
    Validates Requirement 9.4: Custom validators support configuration parameters.
    """
    
    def test_both_boundaries_none_raises_error(self):
        """Test that both boundaries being None raises ValueError."""
        with pytest.raises(ValueError, match="At least one of min_date or max_date must be specified"):
            DateRangeValidator(min_date=None, max_date=None)
    
    def test_min_date_after_max_date_raises_error(self):
        """Test that min_date > max_date raises ValueError."""
        with pytest.raises(ValueError, match="min_date.*must be.*max_date"):
            DateRangeValidator(min_date=date(2025, 1, 1), max_date=date(2020, 1, 1))
    
    def test_min_date_equals_max_date_allowed(self):
        """Test that min_date == max_date is allowed."""
        validator = DateRangeValidator(min_date=date(2024, 1, 1), max_date=date(2024, 1, 1))
        
        assert validator.min_date == date(2024, 1, 1)
        assert validator.max_date == date(2024, 1, 1)
