"""Date range validator.

This module provides the DateRangeValidator that checks transaction dates
fall within expected ranges.
"""

from datetime import date

import polars as pl

from fintran.validation.result import ValidationResult


class DateRangeValidator:
    """Validates that transaction dates fall within expected ranges.

    This validator checks that all transaction dates in the IR DataFrame
    fall within specified minimum and maximum date boundaries. Either or
    both boundaries can be specified.

    Use case: Detect data entry errors with out-of-range dates (e.g., dates
    in the future, dates before company founding, dates outside fiscal period).

    Requirements:
        - Requirement 5.1: Provide date_range validator with min/max boundaries
        - Requirement 5.2: Check all dates fall within range
        - Requirement 5.3: Return errors identifying out-of-range dates
        - Requirement 5.4: Return success when all dates are within range
        - Requirement 5.5: Support optional boundaries (min only, max only, or both)

    Attributes:
        min_date: Minimum allowed date (inclusive), or None for no minimum
        max_date: Maximum allowed date (inclusive), or None for no maximum

    Example:
        >>> validator = DateRangeValidator(
        ...     min_date=date(2020, 1, 1),
        ...     max_date=date(2024, 12, 31)
        ... )
        >>> result = validator.validate(ir_dataframe)
        >>> if not result.is_valid:
        ...     print(result.format())
        [DateRangeValidator] Validation failed
        Errors:
          - Row 5: Date 2019-12-31 is before minimum date 2020-01-01
          - Row 12: Date 2025-01-15 is after maximum date 2024-12-31
    """

    def __init__(
        self,
        min_date: date | None = None,
        max_date: date | None = None,
    ):
        """Initialize date range validator.

        Args:
            min_date: Minimum allowed date (inclusive). None means no minimum.
            max_date: Maximum allowed date (inclusive). None means no maximum.

        Raises:
            ValueError: If both min_date and max_date are None (no validation would occur)
            ValueError: If min_date is after max_date

        Example:
            >>> # Validate dates are after 2020-01-01
            >>> validator = DateRangeValidator(min_date=date(2020, 1, 1))
            >>> # Validate dates are before 2025-01-01
            >>> validator = DateRangeValidator(max_date=date(2025, 1, 1))
            >>> # Validate dates are within range
            >>> validator = DateRangeValidator(
            ...     min_date=date(2020, 1, 1),
            ...     max_date=date(2024, 12, 31)
            ... )
        """
        if min_date is None and max_date is None:
            raise ValueError("At least one of min_date or max_date must be specified")

        if min_date is not None and max_date is not None and min_date > max_date:
            raise ValueError(f"min_date ({min_date}) must be <= max_date ({max_date})")

        self.min_date = min_date
        self.max_date = max_date

    def validate(self, df: pl.DataFrame) -> ValidationResult:
        """Check that all transaction dates fall within the specified range.

        Uses Polars boolean expressions to efficiently identify out-of-range dates
        without row-by-row iteration.

        Args:
            df: IR DataFrame to validate (must not be mutated)

        Returns:
            ValidationResult with:
                - is_valid: False if any dates are out of range
                - errors: List of error messages identifying row indices and dates
                - metadata: Dictionary with violation details including row indices
                           and out-of-range dates

        Example:
            >>> df = pl.DataFrame({
            ...     "date": [date(2019, 12, 31), date(2024, 1, 1)],
            ...     "account": ["ACC1", "ACC2"],
            ...     "amount": [Decimal("100.00"), Decimal("200.00")],
            ...     "currency": ["USD", "USD"],
            ...     "description": [None, None],
            ...     "reference": [None, None],
            ... })
            >>> validator = DateRangeValidator(min_date=date(2020, 1, 1))
            >>> result = validator.validate(df)
            >>> result.is_valid
            False
            >>> "2019-12-31" in result.errors[0]
            True
        """
        # Check if required field exists
        if "date" not in df.columns:
            return ValidationResult(
                is_valid=False,
                errors=["Required field 'date' not found in DataFrame"],
                validator_name="DateRangeValidator",
            )

        # Build filter expression for out-of-range dates
        violations_mask = None

        if self.min_date is not None:
            min_mask = pl.col("date") < self.min_date
            violations_mask = min_mask if violations_mask is None else violations_mask | min_mask

        if self.max_date is not None:
            max_mask = pl.col("date") > self.max_date
            violations_mask = max_mask if violations_mask is None else violations_mask | max_mask

        # Find violations
        violations = df.with_row_index("_row_idx").filter(violations_mask)

        if len(violations) == 0:
            return ValidationResult(
                is_valid=True,
                validator_name="DateRangeValidator",
            )

        # Build error messages
        errors = []
        violation_details = []

        for row in violations.iter_rows(named=True):
            row_idx = row["_row_idx"]
            date_value = row["date"]

            # Determine which boundary was violated
            if self.min_date is not None and date_value < self.min_date:
                error_msg = (
                    f"Row {row_idx}: Date {date_value} is before minimum date {self.min_date}"
                )
            elif self.max_date is not None and date_value > self.max_date:
                error_msg = (
                    f"Row {row_idx}: Date {date_value} is after maximum date {self.max_date}"
                )
            else:
                # Should not happen, but handle gracefully
                error_msg = f"Row {row_idx}: Date {date_value} is out of range"

            errors.append(error_msg)

            violation_details.append(
                {
                    "row_index": row_idx,
                    "date": str(date_value),
                    "min_date": str(self.min_date) if self.min_date else None,
                    "max_date": str(self.max_date) if self.max_date else None,
                }
            )

        return ValidationResult(
            is_valid=False,
            errors=errors,
            validator_name="DateRangeValidator",
            metadata={
                "violations": violation_details,
                "violation_count": len(violations),
            },
        )
