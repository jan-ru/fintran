"""Outlier detection validator.

This module provides a validator for detecting outlier amounts using statistical
methods. Outliers are reported as warnings to flag unusual transactions for review.
"""

import polars as pl

from fintran.validation.result import ValidationResult


class OutlierDetectionValidator:
    """Detects outlier amounts using statistical methods.

    This validator identifies amounts that fall outside statistical bounds using
    one of three methods: z-score, IQR (Interquartile Range), or percentile-based.
    Outliers are reported as warnings rather than errors, as they may represent
    legitimate unusual transactions that need review.

    Requirements:
        - Requirement 8.1: Accept method and threshold parameters
        - Requirement 8.2: Identify amounts outside statistical bounds
        - Requirement 8.3: Return warnings with row indices and outlier amounts
        - Requirement 8.4: Return success when no outliers found
        - Requirement 8.5: Support multiple detection methods (z-score, IQR, percentile)

    Attributes:
        method: Outlier detection method - "zscore", "iqr", or "percentile"
        threshold: Method-specific threshold value

    Methods:
        - zscore: Detects values more than threshold standard deviations from mean
        - iqr: Detects values outside threshold * IQR from Q1/Q3
        - percentile: Detects values outside threshold percentile range

    Example:
        >>> validator = OutlierDetectionValidator(
        ...     method="zscore",
        ...     threshold=3.0
        ... )
        >>> result = validator.validate(ir_dataframe)
        >>> if result.has_warnings():
        ...     print(result.format())
        [outlier_detection] Validation passed
        Warnings:
          - Found 2 outlier amounts using zscore method (threshold=3.0)
          - Row 5: amount=10000.00 (z-score=4.5)
          - Row 12: amount=-5000.00 (z-score=-3.8)
    """

    def __init__(self, method: str = "zscore", threshold: float = 3.0):
        """Initialize outlier detection validator.

        Args:
            method: Outlier detection method - "zscore", "iqr", or "percentile"
            threshold: Method-specific threshold:
                - zscore: number of standard deviations (default: 3.0)
                - iqr: IQR multiplier (default: 1.5)
                - percentile: percentile value (default: 95.0 for 95th percentile)

        Raises:
            ValueError: If method is invalid or threshold is invalid for the method
        """
        if method not in ("zscore", "iqr", "percentile"):
            msg = f"method must be 'zscore', 'iqr', or 'percentile', got: {method}"
            raise ValueError(msg)

        if threshold <= 0:
            msg = f"threshold must be positive, got: {threshold}"
            raise ValueError(msg)

        if method == "percentile" and (threshold < 0 or threshold > 100):
            msg = f"percentile threshold must be between 0 and 100, got: {threshold}"
            raise ValueError(msg)

        self.method = method
        self.threshold = threshold

    def validate(self, df: pl.DataFrame) -> ValidationResult:
        """Detect outlier amounts using the specified method.

        Uses Polars statistical functions for efficient outlier detection:
        - zscore: mean(), std(), and boolean expressions
        - iqr: quantile() for Q1/Q3 and IQR calculation
        - percentile: quantile() for percentile bounds

        The validator uses vectorized operations for performance:
        1. Calculate statistics based on method
        2. Create boolean mask for outliers
        3. Filter to get outlier rows
        4. Collect row indices and amounts for reporting

        Args:
            df: IR DataFrame to validate (must not be mutated)

        Returns:
            ValidationResult with warnings if outliers found, success otherwise.
            Metadata includes outlier row indices, amounts, and statistics.

        Example:
            >>> df = pl.DataFrame({
            ...     "date": ["2024-01-01", "2024-01-02", "2024-01-03"],
            ...     "account": ["1001", "1002", "1003"],
            ...     "amount": [100.0, 200.0, 10000.0],
            ...     "currency": ["EUR", "EUR", "EUR"],
            ...     "description": ["Normal", "Normal", "Outlier"],
            ...     "reference": ["REF1", "REF2", "REF3"]
            ... })
            >>> validator = OutlierDetectionValidator(method="zscore", threshold=2.0)
            >>> result = validator.validate(df)
            >>> result.has_warnings()
            True
        """
        # Check that amount field exists
        if "amount" not in df.columns:
            return ValidationResult(
                is_valid=False,
                errors=["Cannot detect outliers: 'amount' field not found in DataFrame"],
                validator_name="outlier_detection",
            )

        # Need at least 2 rows for meaningful statistics
        if len(df) < 2:
            return ValidationResult(
                is_valid=True,
                validator_name="outlier_detection",
                metadata={
                    "method": self.method,
                    "threshold": self.threshold,
                    "total_rows": len(df),
                },
            )

        # Detect outliers based on method
        if self.method == "zscore":
            return self._detect_zscore(df)
        elif self.method == "iqr":
            return self._detect_iqr(df)
        else:  # percentile
            return self._detect_percentile(df)

    def _detect_zscore(self, df: pl.DataFrame) -> ValidationResult:
        """Detect outliers using z-score method.

        Identifies values more than threshold standard deviations from the mean.
        """
        mean = df["amount"].mean()
        std = df["amount"].std()

        # Handle case where std is 0 (all values are the same)
        if std == 0 or std is None:
            return ValidationResult(
                is_valid=True,
                validator_name="outlier_detection",
                metadata={
                    "method": "zscore",
                    "threshold": self.threshold,
                    "mean": float(mean) if mean is not None else 0.0,
                    "std": 0.0,
                    "outlier_count": 0,
                },
            )

        # Calculate z-scores and identify outliers
        outliers = df.with_row_index("__row_idx__").with_columns(
            ((pl.col("amount") - mean) / std).abs().alias("__zscore__")
        ).filter(pl.col("__zscore__") > self.threshold)

        if len(outliers) == 0:
            return ValidationResult(
                is_valid=True,
                validator_name="outlier_detection",
                metadata={
                    "method": "zscore",
                    "threshold": self.threshold,
                    "mean": float(mean),
                    "std": float(std),
                    "outlier_count": 0,
                },
            )

        # Generate warnings
        warnings = [
            f"Found {len(outliers)} outlier amounts using zscore method "
            f"(threshold={self.threshold})"
        ]

        # Add details for first 10 outliers
        for row in outliers.head(10).iter_rows(named=True):
            warnings.append(
                f"Row {row['__row_idx__']}: amount={row['amount']:.2f} "
                f"(z-score={row['__zscore__']:.2f})"
            )

        if len(outliers) > 10:
            warnings.append(f"... and {len(outliers) - 10} more outliers")

        return ValidationResult(
            is_valid=True,  # Outliers are warnings, not errors
            warnings=warnings,
            validator_name="outlier_detection",
            metadata={
                "method": "zscore",
                "threshold": self.threshold,
                "mean": float(mean),
                "std": float(std),
                "outlier_count": len(outliers),
                "outlier_indices": outliers["__row_idx__"].to_list(),
            },
        )

    def _detect_iqr(self, df: pl.DataFrame) -> ValidationResult:
        """Detect outliers using IQR (Interquartile Range) method.

        Identifies values outside threshold * IQR from Q1/Q3.
        """
        q1 = df["amount"].quantile(0.25)
        q3 = df["amount"].quantile(0.75)
        iqr = q3 - q1

        # Handle case where IQR is 0 (all values in middle 50% are the same)
        if iqr == 0 or iqr is None:
            return ValidationResult(
                is_valid=True,
                validator_name="outlier_detection",
                metadata={
                    "method": "iqr",
                    "threshold": self.threshold,
                    "q1": float(q1) if q1 is not None else 0.0,
                    "q3": float(q3) if q3 is not None else 0.0,
                    "iqr": 0.0,
                    "outlier_count": 0,
                },
            )

        # Calculate bounds
        lower_bound = q1 - (self.threshold * iqr)
        upper_bound = q3 + (self.threshold * iqr)

        # Identify outliers
        outliers = df.with_row_index("__row_idx__").filter(
            (pl.col("amount") < lower_bound) | (pl.col("amount") > upper_bound)
        )

        if len(outliers) == 0:
            return ValidationResult(
                is_valid=True,
                validator_name="outlier_detection",
                metadata={
                    "method": "iqr",
                    "threshold": self.threshold,
                    "q1": float(q1),
                    "q3": float(q3),
                    "iqr": float(iqr),
                    "lower_bound": float(lower_bound),
                    "upper_bound": float(upper_bound),
                    "outlier_count": 0,
                },
            )

        # Generate warnings
        warnings = [
            f"Found {len(outliers)} outlier amounts using IQR method (threshold={self.threshold})"
        ]

        # Add details for first 10 outliers
        for row in outliers.head(10).iter_rows(named=True):
            warnings.append(
                f"Row {row['__row_idx__']}: amount={row['amount']:.2f}"
            )

        if len(outliers) > 10:
            warnings.append(f"... and {len(outliers) - 10} more outliers")

        return ValidationResult(
            is_valid=True,  # Outliers are warnings, not errors
            warnings=warnings,
            validator_name="outlier_detection",
            metadata={
                "method": "iqr",
                "threshold": self.threshold,
                "q1": float(q1),
                "q3": float(q3),
                "iqr": float(iqr),
                "lower_bound": float(lower_bound),
                "upper_bound": float(upper_bound),
                "outlier_count": len(outliers),
                "outlier_indices": outliers["__row_idx__"].to_list(),
            },
        )

    def _detect_percentile(self, df: pl.DataFrame) -> ValidationResult:
        """Detect outliers using percentile-based method.

        Identifies values outside the specified percentile range.
        For example, threshold=95 means values outside the 2.5th-97.5th percentile range.
        """
        # Calculate percentile bounds (symmetric around median)
        lower_percentile = (100 - self.threshold) / 2
        upper_percentile = 100 - lower_percentile

        lower_bound = df["amount"].quantile(lower_percentile / 100)
        upper_bound = df["amount"].quantile(upper_percentile / 100)

        # Identify outliers
        outliers = df.with_row_index("__row_idx__").filter(
            (pl.col("amount") < lower_bound) | (pl.col("amount") > upper_bound)
        )

        if len(outliers) == 0:
            return ValidationResult(
                is_valid=True,
                validator_name="outlier_detection",
                metadata={
                    "method": "percentile",
                    "threshold": self.threshold,
                    "lower_percentile": lower_percentile,
                    "upper_percentile": upper_percentile,
                    "lower_bound": float(lower_bound) if lower_bound is not None else 0.0,
                    "upper_bound": float(upper_bound) if upper_bound is not None else 0.0,
                    "outlier_count": 0,
                },
            )

        # Generate warnings
        warnings = [
            f"Found {len(outliers)} outlier amounts using percentile method "
            f"(outside {lower_percentile:.1f}th-{upper_percentile:.1f}th percentile range)"
        ]

        # Add details for first 10 outliers
        for row in outliers.head(10).iter_rows(named=True):
            warnings.append(
                f"Row {row['__row_idx__']}: amount={row['amount']:.2f}"
            )

        if len(outliers) > 10:
            warnings.append(f"... and {len(outliers) - 10} more outliers")

        return ValidationResult(
            is_valid=True,  # Outliers are warnings, not errors
            warnings=warnings,
            validator_name="outlier_detection",
            metadata={
                "method": "percentile",
                "threshold": self.threshold,
                "lower_percentile": lower_percentile,
                "upper_percentile": upper_percentile,
                "lower_bound": float(lower_bound),
                "upper_bound": float(upper_bound),
                "outlier_count": len(outliers),
                "outlier_indices": outliers["__row_idx__"].to_list(),
            },
        )
