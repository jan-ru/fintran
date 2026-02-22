"""Basic tests for data quality validators.

These tests verify that the validators work correctly with simple examples.
Property-based tests will be implemented in separate test files.
"""

from datetime import date

import polars as pl
import pytest

from fintran.validation.quality import (
    DuplicateDetectionValidator,
    MissingValueDetectionValidator,
    OutlierDetectionValidator,
)


def create_test_df(data: dict) -> pl.DataFrame:
    """Helper to create test IR DataFrames."""
    return pl.DataFrame(data)


class TestDuplicateDetectionValidator:
    """Tests for DuplicateDetectionValidator."""

    def test_no_duplicates(self):
        """Test that validator passes when no duplicates exist."""
        df = create_test_df({
            "date": [date(2024, 1, 1), date(2024, 1, 2), date(2024, 1, 3)],
            "account": ["1001", "1002", "1003"],
            "amount": [100.0, 200.0, 300.0],
            "currency": ["EUR", "EUR", "EUR"],
            "description": ["Test 1", "Test 2", "Test 3"],
            "reference": ["REF1", "REF2", "REF3"],
        })

        validator = DuplicateDetectionValidator(fields=["date", "account", "reference"])
        result = validator.validate(df)

        assert result.is_valid
        assert not result.has_warnings()
        assert result.metadata["duplicate_count"] == 0

    def test_exact_duplicates(self):
        """Test that validator detects exact duplicates."""
        df = create_test_df({
            "date": [date(2024, 1, 1), date(2024, 1, 1), date(2024, 1, 2)],
            "account": ["1001", "1001", "1002"],
            "amount": [100.0, 100.0, 200.0],
            "currency": ["EUR", "EUR", "EUR"],
            "description": ["Test", "Test", "Other"],
            "reference": ["REF1", "REF1", "REF2"],
        })

        validator = DuplicateDetectionValidator(fields=["date", "account", "reference"])
        result = validator.validate(df)

        assert result.is_valid  # Duplicates are warnings, not errors
        assert result.has_warnings()
        assert result.metadata["duplicate_count"] == 2
        assert len(result.metadata["duplicate_indices"]) == 2

    def test_missing_fields(self):
        """Test that validator returns error when fields don't exist."""
        df = create_test_df({
            "date": [date(2024, 1, 1)],
            "account": ["1001"],
            "amount": [100.0],
            "currency": ["EUR"],
        })

        validator = DuplicateDetectionValidator(fields=["nonexistent_field"])
        result = validator.validate(df)

        assert not result.is_valid
        assert result.has_errors()
        assert "not found in DataFrame" in result.errors[0]

    def test_empty_fields_raises_error(self):
        """Test that empty fields list raises ValueError."""
        with pytest.raises(ValueError, match="must contain at least one field"):
            DuplicateDetectionValidator(fields=[])

    def test_invalid_mode_raises_error(self):
        """Test that invalid mode raises ValueError."""
        with pytest.raises(ValueError, match="mode must be"):
            DuplicateDetectionValidator(fields=["date"], mode="invalid")


class TestMissingValueDetectionValidator:
    """Tests for MissingValueDetectionValidator."""

    def test_no_missing_values(self):
        """Test that validator passes when no missing values exist."""
        df = create_test_df({
            "date": [date(2024, 1, 1), date(2024, 1, 2)],
            "account": ["1001", "1002"],
            "amount": [100.0, 200.0],
            "currency": ["EUR", "EUR"],
            "description": ["Test 1", "Test 2"],
            "reference": ["REF1", "REF2"],
        })

        validator = MissingValueDetectionValidator(fields=["description", "reference"])
        result = validator.validate(df)

        assert result.is_valid
        assert not result.has_warnings()
        assert result.metadata["fields_checked"]["description"]["missing_count"] == 0

    def test_null_values(self):
        """Test that validator detects null values."""
        df = create_test_df({
            "date": [date(2024, 1, 1), date(2024, 1, 2), date(2024, 1, 3)],
            "account": ["1001", "1002", "1003"],
            "amount": [100.0, 200.0, 300.0],
            "currency": ["EUR", "EUR", "EUR"],
            "description": ["Test", None, "Other"],
            "reference": ["REF1", "REF2", "REF3"],
        })

        validator = MissingValueDetectionValidator(fields=["description"])
        result = validator.validate(df)

        assert result.is_valid  # Missing values are warnings
        assert result.has_warnings()
        assert result.metadata["fields_checked"]["description"]["missing_count"] == 1
        assert result.metadata["fields_checked"]["description"]["percentage"] == pytest.approx(33.3, rel=0.1)

    def test_empty_strings(self):
        """Test that validator detects empty strings in string fields."""
        df = create_test_df({
            "date": [date(2024, 1, 1), date(2024, 1, 2)],
            "account": ["1001", "1002"],
            "amount": [100.0, 200.0],
            "currency": ["EUR", "EUR"],
            "description": ["Test", ""],
            "reference": ["REF1", "REF2"],
        })

        validator = MissingValueDetectionValidator(fields=["description"])
        result = validator.validate(df)

        assert result.is_valid
        assert result.has_warnings()
        assert result.metadata["fields_checked"]["description"]["missing_count"] == 1

    def test_missing_fields(self):
        """Test that validator returns error when fields don't exist."""
        df = create_test_df({
            "date": [date(2024, 1, 1)],
            "account": ["1001"],
            "amount": [100.0],
            "currency": ["EUR"],
        })

        validator = MissingValueDetectionValidator(fields=["nonexistent_field"])
        result = validator.validate(df)

        assert not result.is_valid
        assert result.has_errors()
        assert "not found in DataFrame" in result.errors[0]

    def test_empty_fields_raises_error(self):
        """Test that empty fields list raises ValueError."""
        with pytest.raises(ValueError, match="must contain at least one field"):
            MissingValueDetectionValidator(fields=[])


class TestOutlierDetectionValidator:
    """Tests for OutlierDetectionValidator."""

    def test_zscore_no_outliers(self):
        """Test z-score method with no outliers."""
        df = create_test_df({
            "date": [date(2024, 1, i) for i in range(1, 11)],
            "account": ["1001"] * 10,
            "amount": [100.0, 105.0, 95.0, 110.0, 90.0, 100.0, 105.0, 95.0, 100.0, 100.0],
            "currency": ["EUR"] * 10,
        })

        validator = OutlierDetectionValidator(method="zscore", threshold=3.0)
        result = validator.validate(df)

        assert result.is_valid
        assert not result.has_warnings()
        assert result.metadata["outlier_count"] == 0

    def test_zscore_with_outliers(self):
        """Test z-score method detects outliers."""
        # Create data with clear outliers: most values around 100, one extreme value
        df = create_test_df({
            "date": [date(2024, 1, i) for i in range(1, 21)],
            "account": ["1001"] * 20,
            "amount": [100.0] * 19 + [10000.0],  # 10000 is a clear outlier
            "currency": ["EUR"] * 20,
        })

        validator = OutlierDetectionValidator(method="zscore", threshold=3.0)
        result = validator.validate(df)

        assert result.is_valid  # Outliers are warnings
        assert result.has_warnings()
        assert result.metadata["outlier_count"] > 0

    def test_iqr_with_outliers(self):
        """Test IQR method detects outliers."""
        df = create_test_df({
            "date": [date(2024, 1, i) for i in range(1, 11)],
            "account": ["1001"] * 10,
            "amount": [100.0, 105.0, 95.0, 110.0, 90.0, 100.0, 105.0, 95.0, 100.0, 10000.0],
            "currency": ["EUR"] * 10,
        })

        validator = OutlierDetectionValidator(method="iqr", threshold=1.5)
        result = validator.validate(df)

        assert result.is_valid
        assert result.has_warnings()
        assert result.metadata["outlier_count"] > 0

    def test_percentile_with_outliers(self):
        """Test percentile method detects outliers."""
        # Use a tighter percentile range to catch the extreme value
        df = create_test_df({
            "date": [date(2024, 1, i) for i in range(1, 21)],
            "account": ["1001"] * 20,
            "amount": [100.0] * 19 + [10000.0],  # 10000 is a clear outlier
            "currency": ["EUR"] * 20,
        })

        validator = OutlierDetectionValidator(method="percentile", threshold=80.0)
        result = validator.validate(df)

        assert result.is_valid
        assert result.has_warnings()
        assert result.metadata["outlier_count"] > 0

    def test_missing_amount_field(self):
        """Test that validator returns error when amount field doesn't exist."""
        df = create_test_df({
            "date": [date(2024, 1, 1)],
            "account": ["1001"],
            "currency": ["EUR"],
        })

        validator = OutlierDetectionValidator(method="zscore", threshold=3.0)
        result = validator.validate(df)

        assert not result.is_valid
        assert result.has_errors()
        assert "amount" in result.errors[0]

    def test_invalid_method_raises_error(self):
        """Test that invalid method raises ValueError."""
        with pytest.raises(ValueError, match="method must be"):
            OutlierDetectionValidator(method="invalid", threshold=3.0)

    def test_invalid_threshold_raises_error(self):
        """Test that invalid threshold raises ValueError."""
        with pytest.raises(ValueError, match="threshold must be positive"):
            OutlierDetectionValidator(method="zscore", threshold=-1.0)

    def test_invalid_percentile_threshold_raises_error(self):
        """Test that invalid percentile threshold raises ValueError."""
        with pytest.raises(ValueError, match="percentile threshold must be between"):
            OutlierDetectionValidator(method="percentile", threshold=150.0)

    def test_zscore_with_zero_std(self):
        """Test z-score method when all values are the same (std=0)."""
        df = create_test_df({
            "date": [date(2024, 1, i) for i in range(1, 6)],
            "account": ["1001"] * 5,
            "amount": [100.0] * 5,  # All same values
            "currency": ["EUR"] * 5,
        })

        validator = OutlierDetectionValidator(method="zscore", threshold=3.0)
        result = validator.validate(df)

        assert result.is_valid
        assert not result.has_warnings()
        assert result.metadata["outlier_count"] == 0
        assert result.metadata["std"] == 0.0
