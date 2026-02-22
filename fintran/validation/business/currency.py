"""Currency consistency validator.

This module provides the CurrencyConsistencyValidator that checks for currency
consistency within account groups or across the entire DataFrame.
"""

import polars as pl

from fintran.validation.result import ValidationResult

# Sentinel value to indicate default behavior (group by account)
_DEFAULT = object()


class CurrencyConsistencyValidator:
    """Validates currency consistency within account groups.

    This validator checks that all transactions within the same account group
    use the same currency. It can group by any combination of fields (default
    is by account) or validate that the entire DataFrame uses a single currency.

    Use case: All transactions for the same account should use the same currency
    to prevent mixed-currency accounting errors.

    Requirements:
        - Requirement 4.1: Provide currency_consistency validator with grouping rules
        - Requirement 4.2: Check currency consistency within groups
        - Requirement 4.3: Return errors identifying groups and conflicting currencies
        - Requirement 4.4: Return success when currency is consistent
        - Requirement 4.5: Support whole-DataFrame validation when no grouping

    Attributes:
        group_by: List of field names to group by, or None for whole-DataFrame validation.
                 Default is ["account"].

    Example:
        >>> validator = CurrencyConsistencyValidator(group_by=["account"])
        >>> result = validator.validate(ir_dataframe)
        >>> if not result.is_valid:
        ...     print(result.format())
        [CurrencyConsistencyValidator] Validation failed
        Errors:
          - Account ACC1 has multiple currencies: EUR, USD (2 distinct currencies)
    """

    def __init__(self, group_by: list[str] | None = _DEFAULT):
        """Initialize currency consistency validator.

        Args:
            group_by: List of field names to group by for currency consistency checks.
                     If None, validates that entire DataFrame uses single currency.
                     If empty list, raises ValueError.
                     If not provided (default), uses ["account"].

        Raises:
            ValueError: If group_by is an empty list.

        Example:
            >>> # Group by account (default)
            >>> validator = CurrencyConsistencyValidator()
            >>> # Group by multiple fields
            >>> validator = CurrencyConsistencyValidator(group_by=["account", "description"])
            >>> # Validate entire DataFrame
            >>> validator = CurrencyConsistencyValidator(group_by=None)
        """
        if group_by is not _DEFAULT and group_by is not None and len(group_by) == 0:
            raise ValueError("group_by must contain at least one field or be None")

        # Distinguish between default (not provided) and explicit None
        if group_by is _DEFAULT:
            self.group_by = ["account"]
            self._validate_whole_df = False
        elif group_by is None:
            self.group_by = None
            self._validate_whole_df = True
        else:
            self.group_by = group_by
            self._validate_whole_df = False

    def validate(self, df: pl.DataFrame) -> ValidationResult:
        """Check currency consistency within groups or across entire DataFrame.

        Uses Polars group_by and aggregation to efficiently count distinct currencies
        per group. Groups with more than one distinct currency are reported as errors.

        When group_by is None, validates that the entire DataFrame uses a single currency.

        Args:
            df: IR DataFrame to validate (must not be mutated)

        Returns:
            ValidationResult with:
                - is_valid: False if any group has multiple currencies
                - errors: List of error messages identifying groups and currencies
                - metadata: Dictionary with violation details including group identifiers
                           and currency lists

        Example:
            >>> df = pl.DataFrame({
            ...     "date": [date(2024, 1, 1), date(2024, 1, 2)],
            ...     "account": ["ACC1", "ACC1"],
            ...     "amount": [Decimal("100.00"), Decimal("200.00")],
            ...     "currency": ["USD", "EUR"],  # Mixed currencies
            ...     "description": [None, None],
            ...     "reference": [None, None],
            ... })
            >>> validator = CurrencyConsistencyValidator(group_by=["account"])
            >>> result = validator.validate(df)
            >>> result.is_valid
            False
            >>> "ACC1" in result.errors[0]
            True
        """
        # Check if required fields exist
        if "currency" not in df.columns:
            return ValidationResult(
                is_valid=False,
                errors=["Required field 'currency' not found in DataFrame"],
                validator_name="CurrencyConsistencyValidator",
            )

        # If explicitly set to None, validate entire DataFrame
        if self._validate_whole_df:
            return self._validate_whole_dataframe(df)

        # Check if grouping fields exist
        missing_fields = [field for field in self.group_by if field not in df.columns]
        if missing_fields:
            return ValidationResult(
                is_valid=False,
                errors=[
                    f"Required grouping field '{field}' not found in DataFrame"
                    for field in missing_fields
                ],
                validator_name="CurrencyConsistencyValidator",
            )

        # Group by specified fields and count distinct currencies
        currency_counts = df.group_by(self.group_by).agg(
            [
                pl.col("currency").n_unique().alias("currency_count"),
                pl.col("currency").unique().alias("currencies"),
            ]
        )

        # Find groups with multiple currencies
        violations = currency_counts.filter(pl.col("currency_count") > 1)

        if len(violations) == 0:
            return ValidationResult(
                is_valid=True,
                validator_name="CurrencyConsistencyValidator",
            )

        # Build error messages
        errors = []
        violation_details = []

        for row in violations.iter_rows(named=True):
            # Build group identifier
            group_parts = [f"{field}={row[field]}" for field in self.group_by]
            group_id = ", ".join(group_parts)

            # Get currencies list and handle None values
            currencies = row["currencies"]
            # Filter out None and sort
            currency_strs = [str(c) if c is not None else "NULL" for c in currencies]
            currency_list = ", ".join(sorted(currency_strs))

            error_msg = (
                f"Group ({group_id}) has multiple currencies: {currency_list} "
                f"({row['currency_count']} distinct currencies)"
            )
            errors.append(error_msg)

            violation_details.append(
                {
                    "group": {field: row[field] for field in self.group_by},
                    "currencies": currencies,
                    "currency_count": row["currency_count"],
                }
            )

        return ValidationResult(
            is_valid=False,
            errors=errors,
            validator_name="CurrencyConsistencyValidator",
            metadata={
                "violations": violation_details,
                "groups_with_violations": len(violations),
            },
        )

    def _validate_whole_dataframe(self, df: pl.DataFrame) -> ValidationResult:
        """Validate that entire DataFrame uses a single currency.

        Args:
            df: IR DataFrame to validate

        Returns:
            ValidationResult indicating whether DataFrame uses single currency
        """
        # Count distinct currencies in entire DataFrame
        unique_currencies = df["currency"].unique().to_list()
        currency_count = len(unique_currencies)

        if currency_count <= 1:
            return ValidationResult(
                is_valid=True,
                validator_name="CurrencyConsistencyValidator",
            )

        # Multiple currencies found
        currency_list = ", ".join(sorted(unique_currencies))
        error_msg = (
            f"DataFrame has multiple currencies: {currency_list} "
            f"({currency_count} distinct currencies)"
        )

        return ValidationResult(
            is_valid=False,
            errors=[error_msg],
            validator_name="CurrencyConsistencyValidator",
            metadata={
                "currencies": unique_currencies,
                "currency_count": currency_count,
            },
        )
