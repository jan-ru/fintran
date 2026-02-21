"""Shared test fixtures and Hypothesis strategies for fintran tests."""

from datetime import date
from decimal import Decimal as PyDecimal

import polars as pl
from hypothesis import strategies as st
from hypothesis.strategies import composite


@composite
def valid_ir_dataframe(draw: st.DrawFn) -> pl.DataFrame:
    """Generate random valid IR DataFrames for property-based testing.

    This strategy generates DataFrames that conform to the IR schema with:
    - Random size between 0 and 20 rows
    - date: Valid dates
    - account: Non-empty text strings (1-20 chars)
    - amount: Decimal values with 2 decimal places (no NaN/infinity)
    - currency: Sampled from common currency codes
    - description: Optional text (can be None)
    - reference: Optional text (can be None)

    Returns:
        Valid IR DataFrame with random data

    Example:
        >>> from hypothesis import given
        >>> @given(valid_ir_dataframe())
        ... def test_something(df):
        ...     assert len(df.columns) == 6
    """
    # Generate random size between 0 and 20 rows
    size = draw(st.integers(min_value=0, max_value=20))

    if size == 0:
        # Return empty IR DataFrame
        return pl.DataFrame(
            {
                "date": [],
                "account": [],
                "amount": [],
                "currency": [],
                "description": [],
                "reference": [],
            },
            schema={
                "date": pl.Date,
                "account": pl.Utf8,
                "amount": pl.Decimal(precision=38, scale=10),
                "currency": pl.Utf8,
                "description": pl.Utf8,
                "reference": pl.Utf8,
            },
        )

    # Generate dates (within a reasonable range)
    dates = draw(
        st.lists(
            st.dates(
                min_value=date(2020, 1, 1),
                max_value=date(2025, 12, 31),
            ),
            min_size=size,
            max_size=size,
        )
    )

    # Generate account strings (1-20 chars)
    accounts = draw(
        st.lists(
            st.text(
                alphabet=st.characters(
                    whitelist_categories=("Lu", "Ll", "Nd"),
                    whitelist_characters="-_",
                ),
                min_size=1,
                max_size=20,
            ),
            min_size=size,
            max_size=size,
        )
    )

    # Generate Decimal amounts (2 decimal places, no NaN/infinity)
    amounts = draw(
        st.lists(
            st.decimals(
                min_value=PyDecimal("-999999999.99"),
                max_value=PyDecimal("999999999.99"),
                allow_nan=False,
                allow_infinity=False,
                places=2,
            ),
            min_size=size,
            max_size=size,
        )
    )

    # Generate currency codes
    currencies = draw(
        st.lists(
            st.sampled_from(["USD", "EUR", "GBP", "JPY"]),
            min_size=size,
            max_size=size,
        )
    )

    # Generate optional descriptions
    descriptions = draw(
        st.lists(
            st.one_of(
                st.none(),
                st.text(
                    alphabet=st.characters(
                        whitelist_categories=("Lu", "Ll", "Nd", "Zs"),
                        whitelist_characters=".,;:-_",
                    ),
                    max_size=100,
                ),
            ),
            min_size=size,
            max_size=size,
        )
    )

    # Generate optional references
    references = draw(
        st.lists(
            st.one_of(
                st.none(),
                st.text(
                    alphabet=st.characters(
                        whitelist_categories=("Lu", "Ll", "Nd"),
                        whitelist_characters="-_",
                    ),
                    max_size=50,
                ),
            ),
            min_size=size,
            max_size=size,
        )
    )

    # Create DataFrame with proper Decimal type casting
    df = pl.DataFrame(
        {
            "date": dates,
            "account": accounts,
            "amount": amounts,
            "currency": currencies,
            "description": descriptions,
            "reference": references,
        }
    )

    # Cast amount column to Decimal type
    df = df.with_columns(pl.col("amount").cast(pl.Decimal(precision=38, scale=10)))

    return df


@composite
def invalid_ir_dataframe(draw: st.DrawFn) -> pl.DataFrame:
    """Generate random invalid IR DataFrames for error testing.

    This strategy generates DataFrames that violate the IR schema in various ways:
    - Missing required fields (date, account, amount, currency)
    - Wrong data types for fields
    - Unexpected extra fields

    Returns:
        Invalid IR DataFrame that should be rejected by validation

    Example:
        >>> from hypothesis import given
        >>> @given(invalid_ir_dataframe())
        ... def test_validation_rejects(df):
        ...     with pytest.raises(ValidationError):
        ...         validate_ir(df)
    """
    from datetime import date
    from decimal import Decimal as PyDecimal

    # Choose what kind of invalid DataFrame to generate
    invalid_type = draw(
        st.sampled_from(
            [
                "missing_date",
                "missing_account",
                "missing_amount",
                "missing_currency",
                "wrong_type_date",
                "wrong_type_amount",
                "unexpected_field",
            ]
        )
    )

    # Generate a small size for invalid DataFrames
    size = draw(st.integers(min_value=1, max_value=5))

    # Base data that we'll modify based on invalid_type
    dates = draw(
        st.lists(
            st.dates(min_value=date(2020, 1, 1), max_value=date(2025, 12, 31)),
            min_size=size,
            max_size=size,
        )
    )
    accounts = draw(
        st.lists(
            st.text(
                alphabet=st.characters(
                    whitelist_categories=("Lu", "Ll", "Nd"), whitelist_characters="-_"
                ),
                min_size=1,
                max_size=20,
            ),
            min_size=size,
            max_size=size,
        )
    )
    amounts = draw(
        st.lists(
            st.decimals(
                min_value=PyDecimal("-999999.99"),
                max_value=PyDecimal("999999.99"),
                allow_nan=False,
                allow_infinity=False,
                places=2,
            ),
            min_size=size,
            max_size=size,
        )
    )
    currencies = draw(
        st.lists(st.sampled_from(["USD", "EUR", "GBP"]), min_size=size, max_size=size)
    )

    # Create invalid DataFrame based on type
    if invalid_type == "missing_date":
        # Missing date field
        return pl.DataFrame(
            {
                "account": accounts,
                "amount": amounts,
                "currency": currencies,
            }
        ).with_columns(pl.col("amount").cast(pl.Decimal(precision=38, scale=10)))

    elif invalid_type == "missing_account":
        # Missing account field
        return pl.DataFrame(
            {
                "date": dates,
                "amount": amounts,
                "currency": currencies,
            }
        ).with_columns(pl.col("amount").cast(pl.Decimal(precision=38, scale=10)))

    elif invalid_type == "missing_amount":
        # Missing amount field
        return pl.DataFrame(
            {
                "date": dates,
                "account": accounts,
                "currency": currencies,
            }
        )

    elif invalid_type == "missing_currency":
        # Missing currency field
        return pl.DataFrame(
            {
                "date": dates,
                "account": accounts,
                "amount": amounts,
            }
        ).with_columns(pl.col("amount").cast(pl.Decimal(precision=38, scale=10)))

    elif invalid_type == "wrong_type_date":
        # Date field has wrong type (string instead of Date)
        date_strings = [d.isoformat() for d in dates]
        return pl.DataFrame(
            {
                "date": date_strings,  # String instead of Date
                "account": accounts,
                "amount": amounts,
                "currency": currencies,
            }
        ).with_columns(pl.col("amount").cast(pl.Decimal(precision=38, scale=10)))

    elif invalid_type == "wrong_type_amount":
        # Amount field has wrong type (Float64 instead of Decimal)
        float_amounts = [float(a) for a in amounts]
        return pl.DataFrame(
            {
                "date": dates,
                "account": accounts,
                "amount": float_amounts,  # Float64 instead of Decimal
                "currency": currencies,
            }
        )

    elif invalid_type == "unexpected_field":
        # Has an unexpected field not in IR schema
        return pl.DataFrame(
            {
                "date": dates,
                "account": accounts,
                "amount": amounts,
                "currency": currencies,
                "unexpected_field": ["extra"] * size,  # Not in IR schema
            }
        ).with_columns(pl.col("amount").cast(pl.Decimal(precision=38, scale=10)))

    # Fallback (should not reach here)
    return pl.DataFrame({"account": accounts})
