"""IR schema definition and validation service.

This module defines the canonical Intermediate Representation (IR) schema
used throughout the fintran pipeline. The IR is a Polars DataFrame with
a fixed schema that serves as the common format between readers and writers.

Schema:
    - date (Date): Transaction date [REQUIRED]
    - account (Utf8): Account identifier [REQUIRED]
    - amount (Decimal): Transaction amount [REQUIRED]
    - currency (Utf8): Currency code (e.g., "USD", "EUR") [REQUIRED]
    - description (Utf8): Optional transaction description [OPTIONAL]
    - reference (Utf8): Optional reference number [OPTIONAL]
"""

import polars as pl
from polars.datatypes import Decimal
from polars.datatypes.classes import DataTypeClass

from fintran.core.exceptions import ValidationError

# IR Schema Definition
IR_SCHEMA = {
    "date": pl.Date,
    "account": pl.Utf8,
    "amount": Decimal,
    "currency": pl.Utf8,
    "description": pl.Utf8,
    "reference": pl.Utf8,
}

# Required fields that must be present and non-null
REQUIRED_FIELDS = ["date", "account", "amount", "currency"]

# Optional fields that may be null
OPTIONAL_FIELDS = ["description", "reference"]


def create_empty_ir() -> pl.DataFrame:
    """Create an empty IR DataFrame with the correct schema.

    Returns:
        An empty Polars DataFrame with the IR schema.

    Example:
        >>> df = create_empty_ir()
        >>> df.schema
        Schema({'date': Date, 'account': Utf8, 'amount': Decimal, ...})
        >>> len(df)
        0
    """
    return pl.DataFrame(schema=IR_SCHEMA)


def get_ir_schema() -> dict[str, DataTypeClass]:
    """Return the IR schema definition for validation purposes.

    Returns:
        Dictionary mapping field names to Polars data types.

    Example:
        >>> schema = get_ir_schema()
        >>> schema["date"]
        <class 'polars.datatypes.classes.Date'>
    """
    return IR_SCHEMA.copy()


def validate_ir(df: pl.DataFrame) -> pl.DataFrame:
    """Validate that a DataFrame conforms to the IR schema.

    This function verifies:
    1. All required fields are present (date, account, amount, currency)
    2. All fields have correct data types
    3. No unexpected fields are present

    The validation is idempotent and does not modify the input DataFrame.

    Args:
        df: DataFrame to validate

    Returns:
        The validated DataFrame unchanged (same reference)

    Raises:
        ValidationError: If validation fails with details about the violation

    Example:
        >>> df = create_empty_ir()
        >>> validated = validate_ir(df)
        >>> validated is df  # Same reference, not modified
        True
    """
    # Check for missing required fields
    df_columns = set(df.columns)
    required_set = set(REQUIRED_FIELDS)
    missing_fields = required_set - df_columns

    if missing_fields:
        raise ValidationError(
            f"Missing required fields: {sorted(missing_fields)}",
            missing_fields=sorted(missing_fields),
        )

    # Check for unexpected fields
    expected_fields = set(IR_SCHEMA.keys())
    unexpected_fields = df_columns - expected_fields

    if unexpected_fields:
        raise ValidationError(
            f"Unexpected fields not in IR schema: {sorted(unexpected_fields)}",
            unexpected_fields=sorted(unexpected_fields),
        )

    # Check data types for all present fields
    for field_name in df_columns:
        expected_type = IR_SCHEMA[field_name]
        actual_type = df.schema[field_name]

        # Handle Decimal type comparison specially
        if expected_type == Decimal:
            # Polars Decimal types have precision/scale, so we check using is_decimal()
            if not actual_type.is_decimal():
                raise ValidationError(
                    f"Field '{field_name}' has incorrect type",
                    field=field_name,
                    expected_type=str(expected_type),
                    actual_type=str(actual_type),
                )
        else:
            # For optional fields, allow Null type (when all values are null)
            if actual_type == pl.Null and field_name in OPTIONAL_FIELDS:
                continue

            # For other types, compare base types to handle nullable variants
            if actual_type.base_type() != expected_type:
                raise ValidationError(
                    f"Field '{field_name}' has incorrect type",
                    field=field_name,
                    expected_type=str(expected_type),
                    actual_type=str(actual_type),
                )

    # Validation passed - return the DataFrame unchanged
    return df
