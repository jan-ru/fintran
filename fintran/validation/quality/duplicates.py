"""Duplicate detection validator.

This module provides a validator for detecting duplicate transactions based on
specified fields. Duplicates are reported as warnings rather than errors, as they
may represent legitimate repeated transactions.
"""

import polars as pl

from fintran.validation.result import ValidationResult


class DuplicateDetectionValidator:
    """Detects duplicate transactions based on specified fields.

    This validator identifies rows with duplicate values across the specified
    fields. It supports both exact matching and fuzzy matching modes. Duplicates
    are reported as warnings rather than errors, as they may represent legitimate
    repeated transactions that need review.

    Requirements:
        - Requirement 6.1: Accept fields parameter for uniqueness checking
        - Requirement 6.2: Identify rows with duplicate values
        - Requirement 6.3: Return warnings with duplicate row indices
        - Requirement 6.4: Return success when no duplicates found
        - Requirement 6.5: Support exact and fuzzy match modes

    Attributes:
        fields: List of field names to check for uniqueness
        mode: Matching mode - "exact" for exact matching, "fuzzy" for fuzzy matching

    Example:
        >>> validator = DuplicateDetectionValidator(
        ...     fields=["date", "account", "reference"],
        ...     mode="exact"
        ... )
        >>> result = validator.validate(ir_dataframe)
        >>> if result.has_warnings():
        ...     print(result.format())
        [duplicate_detection] Validation passed
        Warnings:
          - Found 3 duplicate transactions (rows: [5, 12, 18])
    """

    def __init__(self, fields: list[str], mode: str = "exact"):
        """Initialize duplicate detection validator.

        Args:
            fields: List of field names to check for uniqueness. Duplicates are
                   identified when all specified fields have the same values.
            mode: Matching mode - "exact" for exact matching, "fuzzy" for fuzzy
                 matching. Fuzzy mode is not yet implemented and will fall back
                 to exact matching.

        Raises:
            ValueError: If fields list is empty or mode is invalid
        """
        if not fields:
            msg = "fields parameter must contain at least one field name"
            raise ValueError(msg)

        if mode not in ("exact", "fuzzy"):
            msg = f"mode must be 'exact' or 'fuzzy', got: {mode}"
            raise ValueError(msg)

        self.fields = fields
        self.mode = mode

    def validate(self, df: pl.DataFrame) -> ValidationResult:
        """Detect duplicate rows based on specified fields.

        Uses Polars is_duplicated() to efficiently identify duplicate rows.
        For exact mode, checks if all specified fields have identical values.
        Fuzzy mode is not yet implemented and falls back to exact matching.

        The validator uses vectorized operations for performance:
        1. Mark duplicate rows using is_duplicated()
        2. Filter to get only duplicate rows
        3. Collect row indices and sample values for reporting

        Args:
            df: IR DataFrame to validate (must not be mutated)

        Returns:
            ValidationResult with warnings if duplicates found, success otherwise.
            Metadata includes duplicate row indices and count.

        Example:
            >>> df = pl.DataFrame({
            ...     "date": ["2024-01-01", "2024-01-01", "2024-01-02"],
            ...     "account": ["1001", "1001", "1002"],
            ...     "amount": [100.0, 100.0, 200.0],
            ...     "currency": ["EUR", "EUR", "EUR"],
            ...     "description": ["Test", "Test", "Other"],
            ...     "reference": ["REF1", "REF1", "REF2"]
            ... })
            >>> validator = DuplicateDetectionValidator(fields=["date", "account", "reference"])
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
                    f"Cannot check duplicates: fields not found in DataFrame: "
                    f"{', '.join(missing_fields)}"
                ],
                validator_name="duplicate_detection",
            )

        # For exact mode, use Polars is_duplicated()
        if self.mode == "exact":
            # Mark duplicate rows (keeps all duplicates, not just subsequent ones)
            duplicate_mask = df.select(self.fields).is_duplicated()

            # Get duplicate rows
            duplicate_rows = df.filter(duplicate_mask)

            if len(duplicate_rows) == 0:
                return ValidationResult(
                    is_valid=True,
                    validator_name="duplicate_detection",
                    metadata={"duplicate_count": 0},
                )

            # Get row indices (original positions in DataFrame)
            # Since we filtered, we need to get the original indices
            duplicate_indices = df.with_row_index("__row_idx__").filter(
                duplicate_mask
            )["__row_idx__"].to_list()

            # Create warning message
            warning_msg = (
                f"Found {len(duplicate_rows)} duplicate transactions "
                f"based on fields: {', '.join(self.fields)} "
                f"(rows: {duplicate_indices[:10]}{'...' if len(duplicate_indices) > 10 else ''})"
            )

            return ValidationResult(
                is_valid=True,  # Duplicates are warnings, not errors
                warnings=[warning_msg],
                validator_name="duplicate_detection",
                metadata={
                    "duplicate_count": len(duplicate_rows),
                    "duplicate_indices": duplicate_indices,
                    "fields_checked": self.fields,
                },
            )

        # Fuzzy mode not yet implemented - fall back to exact
        return ValidationResult(
            is_valid=False,
            errors=["Fuzzy matching mode is not yet implemented"],
            validator_name="duplicate_detection",
        )
