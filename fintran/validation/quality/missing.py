"""Missing value detection validator.

This module provides a validator for detecting missing values in optional fields.
Missing values are reported as warnings to help assess data completeness.
"""

import polars as pl

from fintran.validation.result import ValidationResult


class MissingValueDetectionValidator:
    """Detects missing values in optional fields.

    This validator identifies null or empty values in specified fields and reports
    the count and percentage of missing values. Missing values are reported as
    warnings rather than errors, as they may be acceptable for optional fields.

    Requirements:
        - Requirement 7.1: Accept fields parameter for checking
        - Requirement 7.2: Identify rows with null or empty values
        - Requirement 7.3: Return warnings with field names and counts
        - Requirement 7.4: Return success when no missing values found
        - Requirement 7.5: Report percentage of missing values per field

    Attributes:
        fields: List of field names to check for missing values

    Example:
        >>> validator = MissingValueDetectionValidator(
        ...     fields=["description", "reference"]
        ... )
        >>> result = validator.validate(ir_dataframe)
        >>> if result.has_warnings():
        ...     print(result.format())
        [missing_value_detection] Validation passed
        Warnings:
          - Field 'description' has 15 missing values (30.0% of 50 rows)
          - Field 'reference' has 5 missing values (10.0% of 50 rows)
    """

    def __init__(self, fields: list[str]):
        """Initialize missing value detection validator.

        Args:
            fields: List of field names to check for missing values. Each field
                   will be checked for null values and empty strings.

        Raises:
            ValueError: If fields list is empty
        """
        if not fields:
            msg = "fields parameter must contain at least one field name"
            raise ValueError(msg)

        self.fields = fields

    def validate(self, df: pl.DataFrame) -> ValidationResult:
        """Detect missing values and calculate percentages.

        Uses Polars null_count() to efficiently count missing values per field.
        For string fields, also checks for empty strings. Reports both the count
        and percentage of missing values for each field.

        The validator uses vectorized operations for performance:
        1. Count null values per field using null_count()
        2. For string fields, also count empty strings
        3. Calculate percentage based on total row count
        4. Generate warnings for fields with missing values

        Args:
            df: IR DataFrame to validate (must not be mutated)

        Returns:
            ValidationResult with warnings if missing values found, success otherwise.
            Metadata includes missing value counts and percentages per field.

        Example:
            >>> df = pl.DataFrame({
            ...     "date": ["2024-01-01", "2024-01-02", "2024-01-03"],
            ...     "account": ["1001", "1002", "1003"],
            ...     "amount": [100.0, 200.0, 300.0],
            ...     "currency": ["EUR", "EUR", "EUR"],
            ...     "description": ["Test", None, ""],
            ...     "reference": ["REF1", "REF2", "REF3"]
            ... })
            >>> validator = MissingValueDetectionValidator(fields=["description"])
            >>> result = validator.validate(df)
            >>> result.has_warnings()
            True
        """
        # Check that all specified fields exist in the DataFrame
        missing_fields = [f for f in self.fields if f not in df.columns]
        if missing_fields:
            return ValidationResult(
                is_valid=False,
                errors=[
                    f"Cannot check missing values: fields not found in DataFrame: "
                    f"{', '.join(missing_fields)}"
                ],
                validator_name="missing_value_detection",
            )

        total_rows = len(df)
        if total_rows == 0:
            return ValidationResult(
                is_valid=True,
                validator_name="missing_value_detection",
                metadata={"total_rows": 0},
            )

        warnings = []
        metadata = {"total_rows": total_rows, "fields_checked": {}}

        for field in self.fields:
            # Count null values
            null_count = df[field].null_count()

            # For string fields, also count empty strings
            if df[field].dtype == pl.Utf8:
                empty_count = df.filter(
                    (pl.col(field).is_not_null()) & (pl.col(field) == "")
                ).height
                missing_count = null_count + empty_count
            else:
                missing_count = null_count

            if missing_count > 0:
                percentage = (missing_count / total_rows) * 100
                warnings.append(
                    f"Field '{field}' has {missing_count} missing values "
                    f"({percentage:.1f}% of {total_rows} rows)"
                )
                metadata["fields_checked"][field] = {
                    "missing_count": missing_count,
                    "percentage": percentage,
                }
            else:
                metadata["fields_checked"][field] = {
                    "missing_count": 0,
                    "percentage": 0.0,
                }

        if warnings:
            return ValidationResult(
                is_valid=True,  # Missing values are warnings, not errors
                warnings=warnings,
                validator_name="missing_value_detection",
                metadata=metadata,
            )

        return ValidationResult(
            is_valid=True,
            validator_name="missing_value_detection",
            metadata=metadata,
        )
