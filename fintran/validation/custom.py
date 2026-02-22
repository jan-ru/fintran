"""Custom validator helpers.

This module provides utilities for creating custom validators that integrate
seamlessly with the validation framework. It includes:

- @custom_validator decorator for wrapping functions as validators
- Helper functions for common validation patterns (filtering, grouping, aggregation)
- Helper functions for field access with error handling
- Helper functions for error message formatting with row indices and field names

Requirements:
    - Requirement 9.1: Provide decorator/base class for creating custom validators
    - Requirement 9.2: Developer implements validate method accepting IR DataFrame
    - Requirement 9.3: Provide helper functions for common validation patterns
    - Requirement 9.4: Support configuration parameters at initialization
    - Requirement 9.5: Integrate seamlessly with ValidationPipeline

Example:
    >>> from fintran.validation.custom import custom_validator
    >>> from fintran.validation.result import ValidationResult
    >>> import polars as pl
    >>>
    >>> @custom_validator("balance_check")
    >>> def validate_balance(df: pl.DataFrame, tolerance: float = 0.01) -> ValidationResult:
    ...     '''Check that debits equal credits within tolerance.'''
    ...     debits = df.filter(pl.col("amount") < 0)["amount"].sum()
    ...     credits = df.filter(pl.col("amount") > 0)["amount"].sum()
    ...
    ...     balance = abs(debits + credits)
    ...
    ...     if balance > tolerance:
    ...         return ValidationResult(
    ...             is_valid=False,
    ...             errors=[f"Debits and credits don't balance: {debits:.2f} + {credits:.2f} = {balance:.2f}"],
    ...             validator_name="balance_check"
    ...         )
    ...
    ...     return ValidationResult(is_valid=True, validator_name="balance_check")
    >>>
    >>> # Use as a validator
    >>> validator = validate_balance
    >>> result = validator(ir_dataframe, tolerance=0.01)
"""

from collections.abc import Callable
from functools import wraps
from typing import Any

import polars as pl

from fintran.validation.exceptions import ValidatorExecutionError
from fintran.validation.result import ValidationResult


def custom_validator(name: str) -> Callable:
    """Decorator for creating custom validators from functions.

    This decorator wraps a function to make it a validator that integrates
    with the validation framework. The decorated function should accept an
    IR DataFrame as the first argument and return a ValidationResult.

    The decorator automatically sets the validator_name attribute on the
    function, which is used by ValidationPipeline for reporting.

    Requirements:
        - Requirement 9.1: Provide decorator for creating custom validators
        - Requirement 9.2: Function accepts IR DataFrame and returns ValidationResult
        - Requirement 9.4: Support configuration parameters at initialization
        - Requirement 9.5: Integrate seamlessly with ValidationPipeline

    Args:
        name: Name of the validator (used in error messages and reports)

    Returns:
        Decorator function that wraps the validator function

    Example:
        >>> @custom_validator("balance_check")
        >>> def validate_balance(df: pl.DataFrame, tolerance: float = 0.01) -> ValidationResult:
        ...     '''Check that debits equal credits.'''
        ...     debits = df.filter(pl.col("amount") < 0)["amount"].sum()
        ...     credits = df.filter(pl.col("amount") > 0)["amount"].sum()
        ...     balance = abs(debits + credits)
        ...
        ...     if balance > tolerance:
        ...         return ValidationResult(
        ...             is_valid=False,
        ...             errors=[f"Balance mismatch: {balance:.2f}"],
        ...             validator_name="balance_check"
        ...         )
        ...     return ValidationResult(is_valid=True, validator_name="balance_check")
        >>>
        >>> # Create validator with custom tolerance
        >>> validator = lambda df: validate_balance(df, tolerance=0.001)
        >>> result = validator(ir_dataframe)
    """

    def decorator(func: Callable[..., ValidationResult]) -> Callable[..., ValidationResult]:
        """Wrap the validator function."""

        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> ValidationResult:
            """Execute the validator function."""
            return func(*args, **kwargs)

        # Set validator name attribute for ValidationPipeline
        wrapper.validator_name = name  # type: ignore[attr-defined]

        return wrapper

    return decorator


def check_required_fields(
    df: pl.DataFrame, fields: list[str], validator_name: str
) -> ValidationResult | None:
    """Check that required fields exist in the DataFrame.

    This helper function validates that all required fields are present in
    the DataFrame before performing validation logic. If any fields are missing,
    it returns a ValidationResult with an error. If all fields are present,
    it returns None to indicate validation should continue.

    Requirements:
        - Requirement 9.3: Provide helper functions for common validation patterns

    Args:
        df: IR DataFrame to check
        fields: List of required field names
        validator_name: Name of the validator (for error messages)

    Returns:
        ValidationResult with error if fields are missing, None if all present

    Example:
        >>> def validate(self, df: pl.DataFrame) -> ValidationResult:
        ...     # Check required fields
        ...     error = check_required_fields(df, ["account", "amount"], "MyValidator")
        ...     if error:
        ...         return error
        ...
        ...     # Continue with validation logic
        ...     ...
    """
    missing_fields = [f for f in fields if f not in df.columns]

    if missing_fields:
        return ValidationResult(
            is_valid=False,
            errors=[
                f"Required fields not found in DataFrame: {', '.join(missing_fields)}"
            ],
            validator_name=validator_name,
        )

    return None


def filter_by_patterns(
    df: pl.DataFrame, field: str, patterns: list[str]
) -> pl.DataFrame:
    """Filter DataFrame rows where field matches any of the regex patterns.

    This helper function creates a boolean mask that matches rows where the
    specified field matches any of the provided regex patterns. It uses Polars
    vectorized operations for performance.

    Requirements:
        - Requirement 9.3: Provide helper functions for common validation patterns

    Args:
        df: IR DataFrame to filter
        field: Field name to match against patterns
        patterns: List of regex patterns

    Returns:
        Filtered DataFrame containing only matching rows

    Example:
        >>> # Filter to revenue accounts (4xxx series)
        >>> revenue_accounts = filter_by_patterns(df, "account", ["^4[0-9]{3}"])
        >>>
        >>> # Filter to multiple account series
        >>> accounts = filter_by_patterns(df, "account", ["^4[0-9]{3}", "^5[0-9]{3}"])
    """
    if not patterns:
        return df.filter(pl.lit(False))  # Return empty DataFrame

    # Build boolean mask for accounts matching any pattern
    mask = pl.lit(False)
    for pattern in patterns:
        mask = mask | pl.col(field).str.contains(pattern)

    return df.filter(mask)


def get_violations_with_index(
    df: pl.DataFrame, condition: pl.Expr
) -> tuple[pl.DataFrame, list[int]]:
    """Get rows that violate a condition along with their original indices.

    This helper function filters the DataFrame to rows that match the violation
    condition and returns both the filtered DataFrame and a list of the original
    row indices. This is useful for error reporting.

    Requirements:
        - Requirement 9.3: Provide helper functions for common validation patterns

    Args:
        df: IR DataFrame to check
        condition: Polars expression defining the violation condition

    Returns:
        Tuple of (violations DataFrame, list of original row indices)

    Example:
        >>> # Find negative amounts
        >>> violations, indices = get_violations_with_index(
        ...     df,
        ...     pl.col("amount") < 0
        ... )
        >>> print(f"Found {len(violations)} violations at rows: {indices}")
    """
    # Add row index for tracking
    df_with_index = df.with_row_index("_row_idx")

    # Filter to violations
    violations = df_with_index.filter(condition)

    # Extract row indices
    indices = violations["_row_idx"].to_list() if len(violations) > 0 else []

    return violations, indices


def format_violation_error(
    row_index: int,
    field: str,
    value: Any,
    message: str,
    **extra_context: Any,
) -> str:
    """Format a validation error message with row index, field, and value.

    This helper function creates a consistently formatted error message that
    includes the row index, field name, value, and a descriptive message.
    Additional context can be included as keyword arguments.

    Requirements:
        - Requirement 9.3: Provide helper functions for error formatting
        - Requirement 16.1: Include validator name in error message
        - Requirement 16.2: Include affected row indices
        - Requirement 16.3: Include field names and values

    Args:
        row_index: Original row index in the DataFrame
        field: Field name that failed validation
        value: Value that failed validation
        message: Descriptive message about the violation
        **extra_context: Additional context to include (e.g., account, threshold)

    Returns:
        Formatted error message string

    Example:
        >>> error = format_violation_error(
        ...     row_index=5,
        ...     field="amount",
        ...     value=-150.00,
        ...     message="has negative amount",
        ...     account="4001"
        ... )
        >>> print(error)
        Field 'amount' has negative amount: -150.00 (row: 5, account: 4001)
    """
    # Build base message
    parts = [f"Field '{field}' {message}: {value} (row: {row_index}"]

    # Add extra context
    for key, val in extra_context.items():
        parts.append(f", {key}: {val}")

    parts.append(")")

    return "".join(parts)


def format_group_error(
    group_key: str | tuple[str, ...],
    field: str,
    values: list[Any],
    message: str,
    row_indices: list[int] | None = None,
) -> str:
    """Format an error message for group-level violations.

    This helper function creates error messages for violations detected at the
    group level (e.g., multiple currencies within an account group). It includes
    the group key, field name, conflicting values, and optionally row indices.

    Requirements:
        - Requirement 9.3: Provide helper functions for error formatting
        - Requirement 16.2: Include affected row indices
        - Requirement 16.3: Include field names and values

    Args:
        group_key: Key identifying the group (single value or tuple for multi-field groups)
        field: Field name with conflicting values
        values: List of conflicting values in the group
        message: Descriptive message about the violation
        row_indices: Optional list of row indices in the group

    Returns:
        Formatted error message string

    Example:
        >>> error = format_group_error(
        ...     group_key="1001",
        ...     field="currency",
        ...     values=["EUR", "USD"],
        ...     message="has multiple currencies",
        ...     row_indices=[5, 12]
        ... )
        >>> print(error)
        Group '1001' has multiple currencies in field 'currency': EUR, USD (rows: [5, 12])
    """
    # Format group key
    if isinstance(group_key, tuple):
        group_str = ", ".join(str(k) for k in group_key)
    else:
        group_str = str(group_key)

    # Format values
    values_str = ", ".join(str(v) for v in values)

    # Build message
    parts = [
        f"Group '{group_str}' {message} in field '{field}': {values_str}"
    ]

    # Add row indices if provided
    if row_indices:
        # Limit to first 10 indices for readability
        if len(row_indices) > 10:
            indices_str = f"{row_indices[:10]}..."
        else:
            indices_str = str(row_indices)
        parts.append(f" (rows: {indices_str})")

    return "".join(parts)


def aggregate_by_group(
    df: pl.DataFrame, group_by: list[str], agg_expr: pl.Expr
) -> pl.DataFrame:
    """Aggregate DataFrame by group with specified aggregation expression.

    This helper function performs group-by aggregation using Polars, which is
    a common pattern in validation (e.g., counting distinct currencies per account,
    summing amounts per group).

    Requirements:
        - Requirement 9.3: Provide helper functions for common validation patterns

    Args:
        df: IR DataFrame to aggregate
        group_by: List of fields to group by
        agg_expr: Polars aggregation expression

    Returns:
        Aggregated DataFrame with group keys and aggregation results

    Example:
        >>> # Count distinct currencies per account
        >>> currency_counts = aggregate_by_group(
        ...     df,
        ...     group_by=["account"],
        ...     agg_expr=pl.col("currency").n_unique().alias("currency_count")
        ... )
        >>>
        >>> # Sum amounts per account
        >>> account_totals = aggregate_by_group(
        ...     df,
        ...     group_by=["account"],
        ...     agg_expr=pl.col("amount").sum().alias("total_amount")
        ... )
    """
    return df.group_by(group_by).agg(agg_expr)


def safe_field_access(
    df: pl.DataFrame, field: str, validator_name: str
) -> pl.Series:
    """Safely access a field from DataFrame with error handling.

    This helper function attempts to access a field from the DataFrame and
    raises a ValidatorExecutionError if the field doesn't exist. This provides
    consistent error handling for field access across validators.

    Requirements:
        - Requirement 9.3: Provide helper functions for field access with error handling

    Args:
        df: IR DataFrame to access
        field: Field name to access
        validator_name: Name of the validator (for error messages)

    Returns:
        Polars Series containing the field data

    Raises:
        ValidatorExecutionError: If field doesn't exist in DataFrame

    Example:
        >>> try:
        ...     amounts = safe_field_access(df, "amount", "MyValidator")
        ...     # Use amounts for validation
        ... except ValidatorExecutionError as e:
        ...     # Handle missing field error
        ...     return ValidationResult(is_valid=False, errors=[str(e)])
    """
    if field not in df.columns:
        raise ValidatorExecutionError(
            f"Required field '{field}' not found in DataFrame",
            validator_name=validator_name,
            field=field,
            reason="Field missing from DataFrame",
        )

    return df[field]


class CustomValidatorBase:
    """Base class for creating custom validators with configuration.

    This base class provides a template for creating custom validators that
    accept configuration parameters at initialization. Subclasses should:

    1. Define __init__ to accept configuration parameters
    2. Implement validate() method to perform validation logic
    3. Use helper functions from this module for common patterns

    Requirements:
        - Requirement 9.1: Provide base class for creating custom validators
        - Requirement 9.2: Implement validate method accepting IR DataFrame
        - Requirement 9.4: Support configuration parameters at initialization
        - Requirement 9.5: Integrate seamlessly with ValidationPipeline

    Example:
        >>> class BalanceCheckValidator(CustomValidatorBase):
        ...     '''Check that debits equal credits within tolerance.'''
        ...
        ...     def __init__(self, tolerance: float = 0.01):
        ...         self.tolerance = tolerance
        ...         self.validator_name = "balance_check"
        ...
        ...     def validate(self, df: pl.DataFrame) -> ValidationResult:
        ...         # Check required fields
        ...         error = check_required_fields(df, ["amount"], self.validator_name)
        ...         if error:
        ...             return error
        ...
        ...         # Calculate balance
        ...         debits = df.filter(pl.col("amount") < 0)["amount"].sum()
        ...         credits = df.filter(pl.col("amount") > 0)["amount"].sum()
        ...         balance = abs(debits + credits)
        ...
        ...         if balance > self.tolerance:
        ...             return ValidationResult(
        ...                 is_valid=False,
        ...                 errors=[f"Balance mismatch: {balance:.2f}"],
        ...                 validator_name=self.validator_name
        ...             )
        ...
        ...         return ValidationResult(
        ...             is_valid=True,
        ...             validator_name=self.validator_name
        ...         )
        >>>
        >>> # Use the validator
        >>> validator = BalanceCheckValidator(tolerance=0.001)
        >>> result = validator.validate(ir_dataframe)
    """

    validator_name: str = "custom_validator"

    def validate(self, df: pl.DataFrame) -> ValidationResult:
        """Validate an IR DataFrame.

        Subclasses must implement this method to perform validation logic.
        The method should:

        1. Check required fields using check_required_fields()
        2. Perform validation logic using Polars vectorized operations
        3. Use helper functions for filtering, aggregation, and error formatting
        4. Return ValidationResult with appropriate errors/warnings

        Args:
            df: IR DataFrame to validate (must not be mutated)

        Returns:
            ValidationResult with validation outcome

        Raises:
            NotImplementedError: If subclass doesn't implement this method
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement validate() method"
        )
