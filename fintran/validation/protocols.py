"""Validator protocol definitions.

This module defines the Validator protocol that all validators must implement.
The protocol ensures a consistent interface across all validation implementations
and enables type checking.

Protocols:
    - Validator: Validates IR DataFrames and returns ValidationResults

All implementations must:
    - Accept an IR DataFrame as input
    - Return a ValidationResult indicating success or failure
    - Not mutate the input DataFrame (immutability requirement)
    - Be deterministic (same input produces same result)
"""

from typing import TYPE_CHECKING, Protocol

import polars as pl

if TYPE_CHECKING:
    from fintran.validation.result import ValidationResult


class Validator(Protocol):
    """Protocol for validation implementations.

    Validators check IR DataFrames against business rules, data quality
    constraints, or custom validation logic. All implementations must:

    1. Accept an IR DataFrame as input
    2. Return a ValidationResult with validation outcome
    3. Not mutate the input DataFrame (immutability)
    4. Be deterministic (same input always produces same result)

    The immutability requirement ensures that validation is a pure function
    with no side effects. The determinism requirement ensures that validation
    results are reproducible and testable.

    Requirements:
        - Requirement 1.1: Define Validator protocol with validate method
        - Requirement 1.2: Support optional configuration parameters
        - Requirement 1.3: Document immutability requirement
        - Requirement 1.4: Document determinism requirement

    Example:
        >>> class PositiveAmountsValidator:
        ...     def __init__(self, account_patterns: list[str]):
        ...         self.account_patterns = account_patterns
        ...
        ...     def validate(self, df: pl.DataFrame) -> ValidationResult:
        ...         # Check for negative amounts in matching accounts
        ...         violations = df.filter(
        ...             (pl.col("account").str.contains("|".join(self.account_patterns))) &
        ...             (pl.col("amount") <= 0)
        ...         )
        ...
        ...         if len(violations) > 0:
        ...             return ValidationResult(
        ...                 is_valid=False,
        ...                 errors=[f"Found {len(violations)} negative amounts"],
        ...                 validator_name="positive_amounts"
        ...             )
        ...
        ...         return ValidationResult(
        ...             is_valid=True,
        ...             validator_name="positive_amounts"
        ...         )
        ...
        >>> validator = PositiveAmountsValidator(account_patterns=["^4[0-9]{3}"])
        >>> result = validator.validate(ir_dataframe)
        >>> result.is_valid
        True
    """

    def validate(self, df: pl.DataFrame) -> "ValidationResult":
        """Validate an IR DataFrame.

        This method checks the DataFrame against validation rules and returns
        a ValidationResult indicating success or failure. The method must not
        modify the input DataFrame and must be deterministic.

        Immutability: The input DataFrame must not be modified. If the validator
        needs to perform transformations for analysis, it should create a new
        DataFrame or use Polars lazy evaluation.

        Determinism: Applying the same validator to the same DataFrame multiple
        times must produce equivalent ValidationResults (same is_valid status,
        same errors, same warnings).

        Args:
            df: IR DataFrame to validate (must not be mutated)

        Returns:
            ValidationResult with validation outcome including:
                - is_valid: True if validation passed (no errors)
                - errors: List of error messages for validation failures
                - warnings: List of warning messages for data quality issues
                - validator_name: Name of the validator
                - metadata: Additional context (row indices, statistics, etc.)

        Raises:
            ValidatorError: If validation logic itself fails (not data validation
                           failure). For example, if the validator tries to access
                           a non-existent field or encounters an unexpected error.

        Example:
            >>> validator = CurrencyConsistencyValidator(group_by=["account"])
            >>> result = validator.validate(ir_dataframe)
            >>> if not result.is_valid:
            ...     print(result.format())
            [CurrencyConsistencyValidator] Validation failed: Found 2 accounts with mixed currencies
              - Error: Account 1001 has multiple currencies: EUR, USD (rows: [5, 12])
        """
        ...
