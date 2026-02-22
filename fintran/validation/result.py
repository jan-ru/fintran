"""ValidationResult data structure.

This module defines the ValidationResult class that represents the outcome of
a validation check. ValidationResult is the return type for all Validator
implementations and provides methods for checking validation status, formatting
results, and combining multiple results.
"""

from dataclasses import dataclass, field
from typing import Any


@dataclass
class ValidationResult:
    """Result of a validation check.

    This class encapsulates the outcome of applying a validator to an IR DataFrame.
    It contains information about whether validation passed, any errors or warnings
    encountered, and additional metadata for context.

    The is_valid field indicates overall success (True if no errors). Errors represent
    validation failures that should prevent further processing, while warnings indicate
    data quality issues that may not be critical.

    Requirements:
        - Requirement 2.1: Define ValidationResult with required fields
        - Requirement 2.2: Provide method to check if validation passed
        - Requirement 2.3: Provide method to check if validation has warnings
        - Requirement 2.4: Provide method to format result as human-readable string
        - Requirement 2.5: Support combining multiple ValidationResults

    Attributes:
        is_valid: True if validation passed (no errors). False if any errors occurred.
        errors: List of error messages describing validation failures. Each error
               should include context like row indices, field names, and values.
        warnings: List of warning messages describing data quality issues. Warnings
                 do not cause is_valid to be False.
        validator_name: Name of the validator that produced this result. Used for
                       identifying the source of errors in reports.
        metadata: Additional context about the validation. May include row indices,
                 field names, statistics, or other diagnostic information.

    Example:
        >>> result = ValidationResult(
        ...     is_valid=False,
        ...     errors=["Account 4001 has negative amount -150.00 (row: 5)"],
        ...     warnings=["Found 2 duplicate transactions"],
        ...     validator_name="positive_amounts",
        ...     metadata={"violation_count": 1, "row_indices": [5]}
        ... )
        >>> result.has_errors()
        True
        >>> result.has_warnings()
        True
        >>> print(result.format())
        [positive_amounts] Validation failed
        Errors:
          - Account 4001 has negative amount -150.00 (row: 5)
        Warnings:
          - Found 2 duplicate transactions
    """

    is_valid: bool
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    validator_name: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)

    def has_errors(self) -> bool:
        """Check if validation failed with errors.

        Returns:
            True if there are any error messages, False otherwise.

        Example:
            >>> result = ValidationResult(is_valid=True)
            >>> result.has_errors()
            False
            >>> result = ValidationResult(is_valid=False, errors=["Error 1"])
            >>> result.has_errors()
            True
        """
        return len(self.errors) > 0

    def has_warnings(self) -> bool:
        """Check if validation has warnings.

        Warnings indicate data quality issues that may not be critical but
        should be reviewed. A result can have warnings even if is_valid is True.

        Returns:
            True if there are any warning messages, False otherwise.

        Example:
            >>> result = ValidationResult(is_valid=True, warnings=["Warning 1"])
            >>> result.has_warnings()
            True
            >>> result.is_valid
            True
        """
        return len(self.warnings) > 0

    def format(self) -> str:
        """Format result as human-readable string.

        Produces a formatted string showing the validator name, validation status,
        and all errors and warnings. The format is designed for console output and
        log files.

        Returns:
            Formatted string with validator name, status, errors, and warnings.

        Example:
            >>> result = ValidationResult(
            ...     is_valid=False,
            ...     errors=["Error 1", "Error 2"],
            ...     warnings=["Warning 1"],
            ...     validator_name="test_validator"
            ... )
            >>> print(result.format())
            [test_validator] Validation failed
            Errors:
              - Error 1
              - Error 2
            Warnings:
              - Warning 1
        """
        lines = []

        # Header with validator name and status
        status = "passed" if self.is_valid else "failed"
        header = f"[{self.validator_name}] Validation {status}"
        lines.append(header)

        # Errors section
        if self.has_errors():
            lines.append("Errors:")
            for error in self.errors:
                lines.append(f"  - {error}")

        # Warnings section
        if self.has_warnings():
            lines.append("Warnings:")
            for warning in self.warnings:
                lines.append(f"  - {warning}")

        return "\n".join(lines)

    @staticmethod
    def combine(results: list["ValidationResult"]) -> "ValidationResult":
        """Combine multiple results into an aggregated result.

        This method aggregates multiple ValidationResults into a single result.
        The combined result:
        - is_valid is False if any individual result has errors
        - errors list contains all errors from all results
        - warnings list contains all warnings from all results
        - validator_name is "combined"
        - metadata contains the count of combined results

        This is useful for aggregating results from multiple validators or
        multiple validation runs.

        Args:
            results: List of ValidationResults to combine

        Returns:
            Aggregated ValidationResult containing all errors and warnings

        Example:
            >>> result1 = ValidationResult(
            ...     is_valid=False,
            ...     errors=["Error 1"],
            ...     validator_name="validator1"
            ... )
            >>> result2 = ValidationResult(
            ...     is_valid=True,
            ...     warnings=["Warning 1"],
            ...     validator_name="validator2"
            ... )
            >>> combined = ValidationResult.combine([result1, result2])
            >>> combined.is_valid
            False
            >>> len(combined.errors)
            1
            >>> len(combined.warnings)
            1
            >>> combined.validator_name
            'combined'
        """
        # Aggregate all errors and warnings
        all_errors: list[str] = []
        all_warnings: list[str] = []

        for result in results:
            all_errors.extend(result.errors)
            all_warnings.extend(result.warnings)

        # Combined result is valid only if all results are valid (no errors)
        is_valid = len(all_errors) == 0

        return ValidationResult(
            is_valid=is_valid,
            errors=all_errors,
            warnings=all_warnings,
            validator_name="combined",
            metadata={"combined_count": len(results)},
        )
