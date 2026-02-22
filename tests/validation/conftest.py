"""Pytest configuration and fixtures for validation framework tests.

This module provides Hypothesis strategies and pytest fixtures for testing validators.
"""

from datetime import date, timedelta
from decimal import Decimal

import polars as pl
import pytest
from hypothesis import strategies as st

from hypothesis import settings

# Configure Hypothesis for validation tests
settings.register_profile("validation", max_examples=100, deadline=None)
settings.load_profile("validation")


# Hypothesis strategies for generating test data


@st.composite
def valid_ir_dataframe(draw, min_rows: int = 1, max_rows: int = 50):
    """Generate random valid IR DataFrames for testing.

    Args:
        draw: Hypothesis draw function
        min_rows: Minimum number of rows (default: 1)
        max_rows: Maximum number of rows (default: 50)

    Returns:
        A valid IR DataFrame with random data
    """
    num_rows = draw(st.integers(min_value=min_rows, max_value=max_rows))

    # Generate dates within a reasonable range
    base_date = date(2020, 1, 1)
    dates = [
        base_date + timedelta(days=draw(st.integers(min_value=0, max_value=1825)))
        for _ in range(num_rows)
    ]

    # Generate account codes
    accounts = [
        draw(st.sampled_from(["1001", "1002", "2001", "2002", "4001", "4002", "5001"]))
        for _ in range(num_rows)
    ]

    # Generate amounts (mix of positive and negative)
    # Use integers to avoid Decimal precision issues with very small floats
    amounts = [
        Decimal(str(draw(st.integers(min_value=-1000000, max_value=1000000)))) / Decimal("100")
        for _ in range(num_rows)
    ]

    # Generate currencies
    currencies = [
        draw(st.sampled_from(["USD", "EUR", "GBP"]))
        for _ in range(num_rows)
    ]

    # Generate optional fields (some None values)
    descriptions = [
        draw(st.one_of(st.none(), st.text(min_size=1, max_size=50)))
        for _ in range(num_rows)
    ]

    references = [
        draw(st.one_of(st.none(), st.text(min_size=1, max_size=20)))
        for _ in range(num_rows)
    ]

    return pl.DataFrame({
        "date": dates,
        "account": accounts,
        "amount": amounts,
        "currency": currencies,
        "description": descriptions,
        "reference": references,
    })


@st.composite
def ir_with_violations(draw, violation_type: str):
    """Generate IR DataFrames with known validation violations.

    Args:
        draw: Hypothesis draw function
        violation_type: Type of violation to generate
            - "negative_amounts": Include negative amounts in revenue accounts
            - "mixed_currency": Include mixed currencies within same account
            - "out_of_range_dates": Include dates outside expected range
            - "duplicates": Include duplicate transactions
            - "missing_values": Include missing values in optional fields
            - "outliers": Include extreme outlier amounts

    Returns:
        An IR DataFrame with the specified violation type
    """
    num_rows = draw(st.integers(min_value=5, max_value=20))

    if violation_type == "negative_amounts":
        # Generate DataFrame with negative amounts in revenue accounts (4xxx)
        dates = [date(2024, 1, 1) for _ in range(num_rows)]
        accounts = ["4001"] * num_rows
        # Mix of positive and negative amounts
        amounts = [
            Decimal(str(draw(st.integers(min_value=-100000, max_value=100000)))) / Decimal("100")
            for _ in range(num_rows)
        ]
        currencies = ["USD"] * num_rows
        descriptions = [None] * num_rows
        references = [None] * num_rows

    elif violation_type == "mixed_currency":
        # Generate DataFrame with mixed currencies for same account
        dates = [date(2024, 1, 1) for _ in range(num_rows)]
        accounts = ["1001"] * num_rows
        amounts = [Decimal("100.00")] * num_rows
        # Mix of currencies
        currencies = [draw(st.sampled_from(["USD", "EUR", "GBP"])) for _ in range(num_rows)]
        descriptions = [None] * num_rows
        references = [None] * num_rows

    elif violation_type == "out_of_range_dates":
        # Generate DataFrame with dates outside expected range
        dates = [
            date(2019, 1, 1) if i < num_rows // 2 else date(2025, 1, 1)
            for i in range(num_rows)
        ]
        accounts = ["1001"] * num_rows
        amounts = [Decimal("100.00")] * num_rows
        currencies = ["USD"] * num_rows
        descriptions = [None] * num_rows
        references = [None] * num_rows

    elif violation_type == "duplicates":
        # Generate DataFrame with duplicate transactions
        dates = [date(2024, 1, 1)] * num_rows
        accounts = ["1001"] * num_rows
        amounts = [Decimal("100.00")] * num_rows
        currencies = ["USD"] * num_rows
        descriptions = ["Test"] * num_rows
        references = ["REF1"] * num_rows

    elif violation_type == "missing_values":
        # Generate DataFrame with missing values
        dates = [date(2024, 1, 1) for _ in range(num_rows)]
        accounts = ["1001"] * num_rows
        amounts = [Decimal("100.00")] * num_rows
        currencies = ["USD"] * num_rows
        descriptions = [None if i % 2 == 0 else "Test" for i in range(num_rows)]
        references = [None] * num_rows

    elif violation_type == "outliers":
        # Generate DataFrame with extreme outliers
        dates = [date(2024, 1, 1) for _ in range(num_rows)]
        accounts = ["1001"] * num_rows
        # Most values around 100, but include extreme outliers
        amounts = [
            Decimal("100.00") if i < num_rows - 2 else Decimal("100000.00")
            for i in range(num_rows)
        ]
        currencies = ["USD"] * num_rows
        descriptions = [None] * num_rows
        references = [None] * num_rows

    else:
        raise ValueError(f"Unknown violation type: {violation_type}")

    return pl.DataFrame({
        "date": dates,
        "account": accounts,
        "amount": amounts,
        "currency": currencies,
        "description": descriptions,
        "reference": references,
    })


@st.composite
def validator_instances(draw):
    """Generate instances of all built-in validators.

    Args:
        draw: Hypothesis draw function

    Returns:
        A randomly selected validator instance
    """
    from fintran.validation.business.amounts import PositiveAmountsValidator
    from fintran.validation.business.currency import CurrencyConsistencyValidator
    from fintran.validation.business.dates import DateRangeValidator
    from fintran.validation.quality.duplicates import DuplicateDetectionValidator
    from fintran.validation.quality.missing import MissingValueDetectionValidator
    from fintran.validation.quality.outliers import OutlierDetectionValidator

    validator_type = draw(st.sampled_from([
        "positive_amounts",
        "currency_consistency",
        "date_range",
        "duplicates",
        "missing",
        "outliers",
    ]))

    if validator_type == "positive_amounts":
        return PositiveAmountsValidator(account_patterns=["^4[0-9]{3}"])
    elif validator_type == "currency_consistency":
        return CurrencyConsistencyValidator(group_by=["account"])
    elif validator_type == "date_range":
        return DateRangeValidator(
            min_date=date(2020, 1, 1),
            max_date=date(2024, 12, 31)
        )
    elif validator_type == "duplicates":
        return DuplicateDetectionValidator(
            fields=["date", "account", "reference"],
            mode="exact"
        )
    elif validator_type == "missing":
        return MissingValueDetectionValidator(fields=["description", "reference"])
    else:  # outliers
        return OutlierDetectionValidator(method="zscore", threshold=3.0)
