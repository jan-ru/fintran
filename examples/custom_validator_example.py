"""Example: Creating Custom Validators

This example demonstrates how to create custom validators using the
fintran validation framework's helper functions and base classes.
"""

from datetime import date
from decimal import Decimal

import polars as pl

from fintran.validation import (
    CustomValidatorBase,
    ValidationResult,
    check_required_fields,
    custom_validator,
    filter_by_patterns,
    format_violation_error,
    get_violations_with_index,
)


# Example 1: Using the @custom_validator decorator
@custom_validator("balance_check")
def validate_balance(df: pl.DataFrame, tolerance: float = 0.01) -> ValidationResult:
    """Check that debits equal credits within tolerance.

    This is a simple function-based validator using the decorator pattern.
    """
    debits = df.filter(pl.col("amount") < 0)["amount"].sum()
    credits = df.filter(pl.col("amount") > 0)["amount"].sum()

    balance = abs(debits + credits)

    if balance > tolerance:
        return ValidationResult(
            is_valid=False,
            errors=[
                f"Debits and credits don't balance: "
                f"debits={debits:.2f}, credits={credits:.2f}, "
                f"difference={balance:.2f}"
            ],
            validator_name="balance_check",
            metadata={
                "debits": float(debits),
                "credits": float(credits),
                "balance": float(balance),
                "tolerance": tolerance,
            },
        )

    return ValidationResult(
        is_valid=True,
        validator_name="balance_check",
        metadata={
            "debits": float(debits),
            "credits": float(credits),
            "balance": float(balance),
        },
    )


# Example 2: Using CustomValidatorBase for more complex validators
class AccountRangeValidator(CustomValidatorBase):
    """Validate that amounts fall within expected ranges for account types.

    This validator demonstrates using the base class with configuration
    parameters and helper functions.
    """

    def __init__(self, account_ranges: dict[str, tuple[float, float]]):
        """Initialize validator with account range specifications.

        Args:
            account_ranges: Dict mapping account patterns to (min, max) tuples
        """
        self.account_ranges = account_ranges
        self.validator_name = "account_range"

    def validate(self, df: pl.DataFrame) -> ValidationResult:
        """Validate amounts are within expected ranges for each account type."""
        # Check required fields
        error = check_required_fields(df, ["account", "amount"], self.validator_name)
        if error:
            return error

        errors = []

        # Check each account pattern
        for pattern, (min_amount, max_amount) in self.account_ranges.items():
            # Filter to matching accounts
            matching_accounts = filter_by_patterns(df, "account", [pattern])

            if len(matching_accounts) == 0:
                continue

            # Find violations (amounts outside range)
            violations, indices = get_violations_with_index(
                matching_accounts,
                (pl.col("amount") < min_amount) | (pl.col("amount") > max_amount),
            )

            # Format error messages
            for idx, row in zip(indices, violations.iter_rows(named=True)):
                error_msg = format_violation_error(
                    row_index=idx,
                    field="amount",
                    value=row["amount"],
                    message=f"outside expected range [{min_amount}, {max_amount}]",
                    account=row["account"],
                    pattern=pattern,
                )
                errors.append(error_msg)

        if errors:
            return ValidationResult(
                is_valid=False,
                errors=errors,
                validator_name=self.validator_name,
                metadata={"violation_count": len(errors)},
            )

        return ValidationResult(is_valid=True, validator_name=self.validator_name)


# Example 3: Complex validator with multiple checks
class ComprehensiveTransactionValidator(CustomValidatorBase):
    """Comprehensive validator that performs multiple checks.

    This demonstrates combining multiple validation patterns in a single validator.
    """

    def __init__(
        self,
        required_fields: list[str] | None = None,
        min_amount: float = 0.01,
        max_amount: float = 1_000_000.00,
    ):
        """Initialize validator with configuration."""
        self.required_fields = required_fields or ["date", "account", "amount", "currency"]
        self.min_amount = min_amount
        self.max_amount = max_amount
        self.validator_name = "comprehensive_transaction"

    def validate(self, df: pl.DataFrame) -> ValidationResult:
        """Perform comprehensive transaction validation."""
        errors = []
        warnings = []

        # Check 1: Required fields
        field_error = check_required_fields(df, self.required_fields, self.validator_name)
        if field_error:
            return field_error

        # Check 2: Amount range
        amount_violations, amount_indices = get_violations_with_index(
            df,
            (pl.col("amount").abs() < self.min_amount)
            | (pl.col("amount").abs() > self.max_amount),
        )

        for idx, row in zip(amount_indices, amount_violations.iter_rows(named=True)):
            error_msg = format_violation_error(
                row_index=idx,
                field="amount",
                value=row["amount"],
                message=f"outside valid range [{self.min_amount}, {self.max_amount}]",
                account=row["account"],
            )
            errors.append(error_msg)

        # Check 3: Zero amounts (warning only)
        zero_violations, zero_indices = get_violations_with_index(
            df, pl.col("amount") == 0
        )

        if len(zero_violations) > 0:
            warnings.append(
                f"Found {len(zero_violations)} transactions with zero amounts "
                f"(rows: {zero_indices[:10]}{'...' if len(zero_indices) > 10 else ''})"
            )

        # Check 4: Missing descriptions (warning only)
        missing_desc = df.filter(
            pl.col("description").is_null() | (pl.col("description") == "")
        )

        if len(missing_desc) > 0:
            warnings.append(
                f"Found {len(missing_desc)} transactions with missing descriptions"
            )

        if errors:
            return ValidationResult(
                is_valid=False,
                errors=errors,
                warnings=warnings,
                validator_name=self.validator_name,
                metadata={
                    "error_count": len(errors),
                    "warning_count": len(warnings),
                },
            )

        return ValidationResult(
            is_valid=True,
            warnings=warnings,
            validator_name=self.validator_name,
            metadata={"warning_count": len(warnings)},
        )


def main():
    """Demonstrate custom validators with sample data."""
    # Create sample IR DataFrame
    sample_data = pl.DataFrame(
        {
            "date": [date(2024, 1, 1), date(2024, 1, 2), date(2024, 1, 3), date(2024, 1, 4)],
            "account": ["4001", "4002", "5001", "6001"],
            "amount": [
                Decimal("1000.00"),
                Decimal("-1000.00"),
                Decimal("50.00"),
                Decimal("0.00"),
            ],
            "currency": ["EUR", "EUR", "EUR", "EUR"],
            "description": ["Revenue", "Expense", None, "Zero amount"],
            "reference": ["REF1", "REF2", "REF3", "REF4"],
        }
    )

    print("Sample IR DataFrame:")
    print(sample_data)
    print("\n" + "=" * 80 + "\n")

    # Example 1: Balance check validator
    print("Example 1: Balance Check Validator")
    print("-" * 80)
    balance_validator = validate_balance
    result = balance_validator(sample_data, tolerance=0.01)
    print(result.format())
    print("\n")

    # Example 2: Account range validator
    print("Example 2: Account Range Validator")
    print("-" * 80)
    range_validator = AccountRangeValidator(
        account_ranges={
            "^4[0-9]{3}": (0.0, 10000.0),  # Revenue accounts: 0 to 10,000
            "^5[0-9]{3}": (0.0, 5000.0),  # Expense accounts: 0 to 5,000
        }
    )
    result = range_validator.validate(sample_data)
    print(result.format())
    print("\n")

    # Example 3: Comprehensive validator
    print("Example 3: Comprehensive Transaction Validator")
    print("-" * 80)
    comprehensive_validator = ComprehensiveTransactionValidator(
        min_amount=0.01, max_amount=100000.0
    )
    result = comprehensive_validator.validate(sample_data)
    print(result.format())
    print("\n")


if __name__ == "__main__":
    main()
