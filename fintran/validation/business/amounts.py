"""Positive amounts validator.

This module provides the PositiveAmountsValidator that checks for positive
amounts in specified account patterns.
"""

import re

import polars as pl

from fintran.validation.result import ValidationResult


class PositiveAmountsValidator:
    """Validates that amounts are positive for specified accounts.

    This validator checks that all amounts are greater than zero for accounts
    matching the specified regex patterns. It's commonly used to validate that
    revenue accounts have positive amounts.

    Use case: Revenue accounts (e.g., 4xxx series) should have positive amounts
    to ensure proper accounting and detect data entry errors.

    Requirements:
        - Requirement 3.1: Provide positive_amounts validator with account patterns
        - Requirement 3.2: Check amounts are positive for matching accounts
        - Requirement 3.3: Return errors identifying row indices and account names
        - Requirement 3.4: Return success when all amounts are positive
        - Requirement 3.5: Support regex patterns for account matching

    Attributes:
        account_patterns: List of regex patterns for matching accounts.

    Example:
        >>> validator = PositiveAmountsValidator(account_patterns=["^4[0-9]{3}"])
        >>> result = validator.validate(ir_dataframe)
        >>> if not result.is_valid:
        ...     print(result.format())
        [PositiveAmountsValidator] Validation failed
        Errors:
          - Account 4001 has non-positive amount -150.00 (row: 5)
    """

    def __init__(self, account_patterns: list[str]):
        """Initialize positive amounts validator.

        Args:
            account_patterns: List of regex patterns for account matching.
                             Accounts matching any pattern will be validated.

        Raises:
            ValueError: If account_patterns is empty.

        Example:
            >>> # Match revenue accounts (4xxx series)
            >>> validator = PositiveAmountsValidator(account_patterns=["^4[0-9]{3}"])
            >>> # Match multiple account series
            >>> validator = PositiveAmountsValidator(
            ...     account_patterns=["^4[0-9]{3}", "^5[0-9]{3}"]
            ... )
        """
        if not account_patterns:
            raise ValueError("account_patterns must contain at least one pattern")

        self.account_patterns = account_patterns
        # Compile regex patterns for efficiency
        self._compiled_patterns = [re.compile(pattern) for pattern in account_patterns]

    def validate(self, df: pl.DataFrame) -> ValidationResult:
        """Check that amounts are positive for matching accounts.

        Uses Polars vectorized operations for performance:
        1. Filter rows matching account patterns
        2. Check if any amounts are <= 0
        3. Collect error details if violations found

        Args:
            df: IR DataFrame to validate (must not be mutated)

        Returns:
            ValidationResult with:
                - is_valid: False if any matching account has non-positive amount
                - errors: List of error messages identifying accounts and amounts
                - metadata: Dictionary with violation details including row indices

        Example:
            >>> df = pl.DataFrame({
            ...     "date": [date(2024, 1, 1), date(2024, 1, 2)],
            ...     "account": ["4001", "4002"],
            ...     "amount": [Decimal("100.00"), Decimal("-50.00")],
            ...     "currency": ["USD", "USD"],
            ...     "description": [None, None],
            ...     "reference": [None, None],
            ... })
            >>> validator = PositiveAmountsValidator(account_patterns=["^4[0-9]{3}"])
            >>> result = validator.validate(df)
            >>> result.is_valid
            False
            >>> "4002" in result.errors[0]
            True
        """
        # Check if required fields exist
        if "account" not in df.columns:
            return ValidationResult(
                is_valid=False,
                errors=["Required field 'account' not found in DataFrame"],
                validator_name="PositiveAmountsValidator",
            )

        if "amount" not in df.columns:
            return ValidationResult(
                is_valid=False,
                errors=["Required field 'amount' not found in DataFrame"],
                validator_name="PositiveAmountsValidator",
            )

        # Add row index for error reporting
        df_with_index = df.with_row_index("_row_idx")

        # Build boolean mask for accounts matching any pattern
        # Use Polars str.contains with regex patterns
        mask = pl.lit(False)
        for pattern in self.account_patterns:
            mask = mask | pl.col("account").str.contains(pattern)

        # Filter to matching accounts with non-positive amounts
        violations = df_with_index.filter(mask & (pl.col("amount") <= 0))

        if len(violations) == 0:
            return ValidationResult(
                is_valid=True,
                validator_name="PositiveAmountsValidator",
            )

        # Build error messages
        errors = []
        violation_details = []

        for row in violations.iter_rows(named=True):
            error_msg = (
                f"Account {row['account']} has non-positive amount {row['amount']} "
                f"(row: {row['_row_idx']})"
            )
            errors.append(error_msg)

            violation_details.append(
                {
                    "row_index": row["_row_idx"],
                    "account": row["account"],
                    "amount": row["amount"],
                }
            )

        return ValidationResult(
            is_valid=False,
            errors=errors,
            validator_name="PositiveAmountsValidator",
            metadata={
                "violations": violation_details,
                "violation_count": len(violations),
            },
        )
