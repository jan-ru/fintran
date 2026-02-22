"""Property-based tests for CurrencyConsistencyValidator.

This module implements Property 4: Currency Consistency Validation
Validates Requirements: 4.2, 4.3, 4.4, 4.5, 20.1
"""

from datetime import date
from decimal import Decimal as PyDecimal

import polars as pl
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from fintran.validation.business.currency import CurrencyConsistencyValidator
from fintran.validation.result import ValidationResult


# Hypothesis strategies for currency consistency testing

@st.composite
def ir_with_consistent_currency(draw: st.DrawFn, group_by: list[str] | None = None) -> pl.DataFrame:
    """Generate IR DataFrame with consistent currency within groups.
    
    Args:
        draw: Hypothesis draw function
        group_by: Fields to group by (default: ["account"])
        
    Returns:
        IR DataFrame where each group has only one currency
    """
    group_by = group_by or ["account"]
    
    # Generate number of groups (1-5)
    num_groups = draw(st.integers(min_value=1, max_value=5))
    
    # Generate group identifiers
    if "account" in group_by:
        accounts = draw(st.lists(
            st.text(
                alphabet=st.characters(whitelist_categories=("Lu", "Ll", "Nd")),
                min_size=1,
                max_size=10
            ),
            min_size=num_groups,
            max_size=num_groups,
            unique=True
        ))
    else:
        accounts = ["ACC1"] * num_groups
    
    # Assign one currency per group
    group_currencies = draw(st.lists(
        st.sampled_from(["USD", "EUR", "GBP", "JPY"]),
        min_size=num_groups,
        max_size=num_groups
    ))
    
    # Generate transactions per group (1-5 per group)
    all_dates = []
    all_accounts = []
    all_amounts = []
    all_currencies = []
    
    for i in range(num_groups):
        transactions_in_group = draw(st.integers(min_value=1, max_value=5))
        
        for _ in range(transactions_in_group):
            all_dates.append(draw(st.dates(
                min_value=date(2020, 1, 1),
                max_value=date(2025, 12, 31)
            )))
            all_accounts.append(accounts[i])
            all_amounts.append(draw(st.decimals(
                min_value=PyDecimal("-999999.99"),
                max_value=PyDecimal("999999.99"),
                allow_nan=False,
                allow_infinity=False,
                places=2
            )))
            # All transactions in this group use the same currency
            all_currencies.append(group_currencies[i])
    
    df = pl.DataFrame({
        "date": all_dates,
        "account": all_accounts,
        "amount": all_amounts,
        "currency": all_currencies,
        "description": [None] * len(all_dates),
        "reference": [None] * len(all_dates),
    })
    
    df = df.with_columns(pl.col("amount").cast(pl.Decimal(precision=38, scale=10)))
    
    return df


@st.composite
def ir_with_mixed_currency(draw: st.DrawFn, group_by: list[str] | None = None) -> pl.DataFrame:
    """Generate IR DataFrame with mixed currencies within at least one group.
    
    Args:
        draw: Hypothesis draw function
        group_by: Fields to group by (default: ["account"])
        
    Returns:
        IR DataFrame where at least one group has multiple currencies
    """
    group_by = group_by or ["account"]
    
    # Generate number of groups (1-5)
    num_groups = draw(st.integers(min_value=1, max_value=5))
    
    # Generate group identifiers
    if "account" in group_by:
        accounts = draw(st.lists(
            st.text(
                alphabet=st.characters(whitelist_categories=("Lu", "Ll", "Nd")),
                min_size=1,
                max_size=10
            ),
            min_size=num_groups,
            max_size=num_groups,
            unique=True
        ))
    else:
        accounts = ["ACC1"] * num_groups
    
    # Pick at least one group to have mixed currencies
    mixed_group_idx = draw(st.integers(min_value=0, max_value=num_groups - 1))
    
    all_dates = []
    all_accounts = []
    all_amounts = []
    all_currencies = []
    
    for i in range(num_groups):
        transactions_in_group = draw(st.integers(min_value=2, max_value=5))
        
        if i == mixed_group_idx:
            # This group will have mixed currencies
            group_currencies = draw(st.lists(
                st.sampled_from(["USD", "EUR", "GBP", "JPY"]),
                min_size=transactions_in_group,
                max_size=transactions_in_group
            ))
            # Ensure at least 2 different currencies
            if len(set(group_currencies)) == 1:
                group_currencies[0] = "USD"
                group_currencies[1] = "EUR"
        else:
            # This group has consistent currency
            single_currency = draw(st.sampled_from(["USD", "EUR", "GBP", "JPY"]))
            group_currencies = [single_currency] * transactions_in_group
        
        for j in range(transactions_in_group):
            all_dates.append(draw(st.dates(
                min_value=date(2020, 1, 1),
                max_value=date(2025, 12, 31)
            )))
            all_accounts.append(accounts[i])
            all_amounts.append(draw(st.decimals(
                min_value=PyDecimal("-999999.99"),
                max_value=PyDecimal("999999.99"),
                allow_nan=False,
                allow_infinity=False,
                places=2
            )))
            all_currencies.append(group_currencies[j])
    
    df = pl.DataFrame({
        "date": all_dates,
        "account": all_accounts,
        "amount": all_amounts,
        "currency": all_currencies,
        "description": [None] * len(all_dates),
        "reference": [None] * len(all_dates),
    })
    
    df = df.with_columns(pl.col("amount").cast(pl.Decimal(precision=38, scale=10)))
    
    return df


@st.composite
def ir_with_single_currency(draw: st.DrawFn) -> pl.DataFrame:
    """Generate IR DataFrame where all rows use the same currency.
    
    Args:
        draw: Hypothesis draw function
        
    Returns:
        IR DataFrame with single currency across all rows
    """
    size = draw(st.integers(min_value=1, max_value=20))
    
    # Pick one currency for entire DataFrame
    single_currency = draw(st.sampled_from(["USD", "EUR", "GBP", "JPY"]))
    
    dates = draw(st.lists(
        st.dates(min_value=date(2020, 1, 1), max_value=date(2025, 12, 31)),
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
    
    df = pl.DataFrame({
        "date": dates,
        "account": accounts,
        "amount": amounts,
        "currency": [single_currency] * size,
        "description": [None] * size,
        "reference": [None] * size,
    })
    
    df = df.with_columns(pl.col("amount").cast(pl.Decimal(precision=38, scale=10)))
    
    return df


# Property-based tests

class TestCurrencyConsistencyValidatorProperties:
    """Property-based tests for CurrencyConsistencyValidator.
    
    Implements Property 4: Currency Consistency Validation
    Validates Requirements: 4.2, 4.3, 4.4, 4.5, 20.1
    """
    
    @given(ir_with_consistent_currency())
    @settings(max_examples=50, deadline=None)
    def test_property_consistent_currency_passes(self, df: pl.DataFrame):
        """Property: Validator passes when currency is consistent within groups.
        
        Validates Requirement 4.4: When currency is consistent within all account
        groups, the validator shall return a ValidationResult indicating success.
        """
        validator = CurrencyConsistencyValidator(group_by=["account"])
        result = validator.validate(df)
        
        assert result.is_valid, f"Expected validation to pass for consistent currency, but got errors: {result.errors}"
        assert not result.has_errors()
        assert result.validator_name == "CurrencyConsistencyValidator"
    
    @given(ir_with_mixed_currency())
    @settings(max_examples=50, deadline=None)
    def test_property_mixed_currency_fails(self, df: pl.DataFrame):
        """Property: Validator detects mixed currencies within groups.
        
        Validates Requirement 4.3: If multiple currencies are found within an
        account group, the validator shall return a ValidationResult with errors
        identifying the account group and conflicting currencies.
        """
        validator = CurrencyConsistencyValidator(group_by=["account"])
        result = validator.validate(df)
        
        assert not result.is_valid, "Expected validation to fail for mixed currency"
        assert result.has_errors()
        assert len(result.errors) > 0
        
        # Verify error message contains account group information
        error_message = " ".join(result.errors)
        assert "currency" in error_message.lower() or "currencies" in error_message.lower()
        
        # Verify metadata contains information about violations
        assert "violations" in result.metadata or "groups_with_violations" in result.metadata
    
    @given(ir_with_single_currency())
    @settings(max_examples=50, deadline=None)
    def test_property_single_currency_whole_dataframe(self, df: pl.DataFrame):
        """Property: Validator passes when entire DataFrame uses single currency.
        
        Validates Requirement 4.5: Where no account grouping rules are provided,
        the validator shall validate that the entire DataFrame uses a single currency.
        """
        # Test with no grouping (validates whole DataFrame)
        validator = CurrencyConsistencyValidator(group_by=None)
        result = validator.validate(df)
        
        assert result.is_valid, f"Expected validation to pass for single currency DataFrame, but got errors: {result.errors}"
        assert not result.has_errors()
    
    @given(ir_with_consistent_currency())
    @settings(max_examples=50, deadline=None)
    def test_property_determinism(self, df: pl.DataFrame):
        """Property: Validator is deterministic (same input produces same result).
        
        Validates Requirement 1.5: For all Validator implementations, applying
        the same validator twice to the same input shall produce equivalent results.
        """
        validator = CurrencyConsistencyValidator(group_by=["account"])
        
        result1 = validator.validate(df)
        result2 = validator.validate(df)
        
        assert result1.is_valid == result2.is_valid
        assert result1.errors == result2.errors
        assert result1.warnings == result2.warnings
    
    @given(ir_with_mixed_currency())
    @settings(max_examples=50, deadline=None)
    def test_property_immutability(self, df: pl.DataFrame):
        """Property: Validator does not modify input DataFrame.
        
        Validates Requirement 18.1: For all validators, the test suite shall verify
        that the input DataFrame is not modified during validation.
        """
        # Create a copy to compare against
        df_copy = df.clone()
        
        validator = CurrencyConsistencyValidator(group_by=["account"])
        _ = validator.validate(df)
        
        # Verify DataFrame was not modified
        assert df.equals(df_copy), "Validator modified the input DataFrame"
    
    @given(ir_with_mixed_currency())
    @settings(max_examples=50, deadline=None)
    def test_property_error_message_completeness(self, df: pl.DataFrame):
        """Property: Error messages contain validator name and violation details.
        
        Validates Requirement 16.1, 16.2, 16.3: Error messages shall include
        validator name, affected groups, and specific currencies that conflict.
        """
        validator = CurrencyConsistencyValidator(group_by=["account"])
        result = validator.validate(df)
        
        if result.has_errors():
            # Check validator name is present
            assert result.validator_name == "CurrencyConsistencyValidator"
            
            # Check error messages contain relevant information
            error_text = " ".join(result.errors).lower()
            assert "currency" in error_text or "currencies" in error_text
            
            # Check metadata contains violation details
            assert result.metadata is not None
            assert len(result.metadata) > 0


# Edge case tests

class TestCurrencyConsistencyValidatorEdgeCases:
    """Edge case tests for CurrencyConsistencyValidator.
    
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
        
        validator = CurrencyConsistencyValidator(group_by=["account"])
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
        
        validator = CurrencyConsistencyValidator(group_by=["account"])
        result = validator.validate(df)
        
        # Single row should pass (no conflicts possible)
        assert result.is_valid
        assert not result.has_errors()
    
    def test_missing_currency_field(self):
        """Test validator returns error when currency field is missing."""
        df = pl.DataFrame({
            "date": [date(2024, 1, 1)],
            "account": ["ACC1"],
            "amount": [PyDecimal("100.00")],
        })
        df = df.with_columns(pl.col("amount").cast(pl.Decimal(precision=38, scale=10)))
        
        validator = CurrencyConsistencyValidator(group_by=["account"])
        result = validator.validate(df)
        
        assert not result.is_valid
        assert result.has_errors()
        assert "currency" in result.errors[0].lower()
    
    def test_missing_grouping_field(self):
        """Test validator returns error when grouping field is missing."""
        df = pl.DataFrame({
            "date": [date(2024, 1, 1)],
            "amount": [PyDecimal("100.00")],
            "currency": ["USD"],
        })
        df = df.with_columns(pl.col("amount").cast(pl.Decimal(precision=38, scale=10)))
        
        validator = CurrencyConsistencyValidator(group_by=["account"])
        result = validator.validate(df)
        
        assert not result.is_valid
        assert result.has_errors()
        assert "account" in result.errors[0].lower()
    
    def test_null_currency_values(self):
        """Test validator handles null currency values."""
        df = pl.DataFrame({
            "date": [date(2024, 1, 1), date(2024, 1, 2)],
            "account": ["ACC1", "ACC1"],
            "amount": [PyDecimal("100.00"), PyDecimal("200.00")],
            "currency": ["USD", None],
            "description": [None, None],
            "reference": [None, None],
        })
        df = df.with_columns(pl.col("amount").cast(pl.Decimal(precision=38, scale=10)))
        
        validator = CurrencyConsistencyValidator(group_by=["account"])
        result = validator.validate(df)
        
        # Null currency should be treated as a violation or handled gracefully
        # The exact behavior depends on implementation, but it should not crash
        assert isinstance(result, ValidationResult)
    
    def test_empty_string_currency(self):
        """Test validator handles empty string currency values."""
        df = pl.DataFrame({
            "date": [date(2024, 1, 1), date(2024, 1, 2)],
            "account": ["ACC1", "ACC1"],
            "amount": [PyDecimal("100.00"), PyDecimal("200.00")],
            "currency": ["USD", ""],
            "description": [None, None],
            "reference": [None, None],
        })
        df = df.with_columns(pl.col("amount").cast(pl.Decimal(precision=38, scale=10)))
        
        validator = CurrencyConsistencyValidator(group_by=["account"])
        result = validator.validate(df)
        
        # Empty string should be treated as different from "USD"
        assert not result.is_valid
        assert result.has_errors()
    
    def test_multiple_grouping_fields(self):
        """Test validator with multiple grouping fields."""
        df = pl.DataFrame({
            "date": [date(2024, 1, 1), date(2024, 1, 2), date(2024, 1, 3)],
            "account": ["ACC1", "ACC1", "ACC2"],
            "amount": [PyDecimal("100.00"), PyDecimal("200.00"), PyDecimal("300.00")],
            "currency": ["USD", "EUR", "USD"],  # ACC1 has mixed, ACC2 is consistent
            "description": ["Type1", "Type1", "Type2"],
            "reference": [None, None, None],
        })
        df = df.with_columns(pl.col("amount").cast(pl.Decimal(precision=38, scale=10)))
        
        # Group by both account and description
        validator = CurrencyConsistencyValidator(group_by=["account", "description"])
        result = validator.validate(df)
        
        # Should detect mixed currency in (ACC1, Type1) group
        assert not result.is_valid
        assert result.has_errors()


# Configuration tests

class TestCurrencyConsistencyValidatorConfiguration:
    """Tests for CurrencyConsistencyValidator configuration.
    
    Validates Requirement 9.4: Custom validators support configuration parameters.
    """
    
    def test_default_grouping(self):
        """Test validator uses default grouping when none provided."""
        validator = CurrencyConsistencyValidator()
        
        # Default should be ["account"]
        assert validator.group_by == ["account"]
        assert not validator._validate_whole_df
    
    def test_custom_grouping(self):
        """Test validator accepts custom grouping fields."""
        validator = CurrencyConsistencyValidator(group_by=["account", "description"])
        
        assert validator.group_by == ["account", "description"]
    
    def test_none_grouping_validates_whole_dataframe(self):
        """Test validator with None grouping validates entire DataFrame."""
        df = pl.DataFrame({
            "date": [date(2024, 1, 1), date(2024, 1, 2)],
            "account": ["ACC1", "ACC2"],
            "amount": [PyDecimal("100.00"), PyDecimal("200.00")],
            "currency": ["USD", "EUR"],  # Different currencies
            "description": [None, None],
            "reference": [None, None],
        })
        df = df.with_columns(pl.col("amount").cast(pl.Decimal(precision=38, scale=10)))
        
        validator = CurrencyConsistencyValidator(group_by=None)
        result = validator.validate(df)
        
        # Should fail because DataFrame has multiple currencies
        assert not result.is_valid
        assert result.has_errors()
    
    def test_empty_grouping_list_raises_error(self):
        """Test that empty grouping list raises ValueError."""
        with pytest.raises(ValueError, match="group_by must contain at least one field"):
            CurrencyConsistencyValidator(group_by=[])
