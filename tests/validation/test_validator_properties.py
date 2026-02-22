"""Property-based tests for validator determinism and immutability.

This module tests fundamental properties that all validators must satisfy:
- Property 1: Validator Determinism
- Property 24: Validator Immutability

These properties ensure that validators are pure functions with no side effects.
"""

import polars as pl
from hypothesis import given

from tests.validation.conftest import valid_ir_dataframe, validator_instances


@given(validator_instances(), valid_ir_dataframe())
def test_validator_determinism(validator, df: pl.DataFrame) -> None:
    """Feature: data-validation-framework, Property 1: Validator Determinism

    For any validator and any IR DataFrame, applying the same validator twice
    to the same input should produce equivalent results (same is_valid status,
    same errors, same warnings).

    This property ensures that validators are deterministic and reproducible,
    which is essential for reliable validation and testing.

    Validates: Requirements 1.5, 17.1, 17.2, 17.3, 17.4

    Args:
        validator: A randomly generated validator instance
        df: A randomly generated valid IR DataFrame
    """
    # Apply validator twice to the same DataFrame
    result1 = validator.validate(df)
    result2 = validator.validate(df)

    # Check that results are equivalent
    assert result1.is_valid == result2.is_valid, (
        f"Validator determinism violated: is_valid differs between invocations "
        f"(first={result1.is_valid}, second={result2.is_valid})"
    )

    # Check that errors are the same (order may vary for some validators)
    assert set(result1.errors) == set(result2.errors), (
        f"Validator determinism violated: errors differ between invocations "
        f"(first={sorted(result1.errors)}, second={sorted(result2.errors)})"
    )

    # Check that warnings are the same (order may vary for some validators)
    assert set(result1.warnings) == set(result2.warnings), (
        f"Validator determinism violated: warnings differ between invocations "
        f"(first={sorted(result1.warnings)}, second={sorted(result2.warnings)})"
    )

    assert result1.validator_name == result2.validator_name, (
        f"Validator determinism violated: validator_name differs between invocations "
        f"(first={result1.validator_name}, second={result2.validator_name})"
    )


@given(validator_instances(), valid_ir_dataframe())
def test_validator_immutability_reference(validator, df: pl.DataFrame) -> None:
    """Feature: data-validation-framework, Property 24: Validator Immutability (Reference)

    For any validator and any IR DataFrame, validation should not modify the
    input DataFrame. This test checks reference equality - the DataFrame object
    should remain the same instance.

    This property ensures that validators are pure functions with no side effects,
    which is essential for composability and predictability.

    Validates: Requirements 18.1, 18.2, 18.3

    Args:
        validator: A randomly generated validator instance
        df: A randomly generated valid IR DataFrame
    """
    # Store the original DataFrame reference
    original_df = df

    # Apply validator
    validator.validate(df)

    # Check that the DataFrame reference is unchanged
    assert df is original_df, (
        "Validator immutability violated: DataFrame reference changed after validation"
    )


@given(validator_instances(), valid_ir_dataframe())
def test_validator_immutability_content(validator, df: pl.DataFrame) -> None:
    """Feature: data-validation-framework, Property 24: Validator Immutability (Content)

    For any validator and any IR DataFrame, validation should not modify the
    content of the input DataFrame. This test checks content equality - all
    columns and values should remain unchanged.

    This property ensures that validators are pure functions with no side effects,
    which is essential for composability and predictability.

    Validates: Requirements 18.1, 18.2, 18.4

    Args:
        validator: A randomly generated validator instance
        df: A randomly generated valid IR DataFrame
    """
    # Create a deep copy of the DataFrame for comparison
    df_before = df.clone()

    # Apply validator
    validator.validate(df)

    # Check that DataFrame content is unchanged
    assert df.equals(df_before), (
        "Validator immutability violated: DataFrame content changed after validation"
    )

    # Also check that columns are the same
    assert df.columns == df_before.columns, (
        "Validator immutability violated: DataFrame columns changed after validation"
    )

    # Check that schema is the same
    assert df.schema == df_before.schema, (
        "Validator immutability violated: DataFrame schema changed after validation"
    )
