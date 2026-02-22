"""Property-based tests for validation performance.

This module tests performance properties of validators to ensure they scale
linearly with DataFrame size (O(n) complexity).

Property 22: Validation Performance Linearity
Validates: Requirements 15.1, 15.2, 15.3, 15.4, 15.5
"""

import time
from datetime import date, timedelta
from decimal import Decimal

import polars as pl
import pytest

from fintran.validation.business.amounts import PositiveAmountsValidator
from fintran.validation.business.currency import CurrencyConsistencyValidator
from fintran.validation.business.dates import DateRangeValidator
from fintran.validation.quality.duplicates import DuplicateDetectionValidator
from fintran.validation.quality.missing import MissingValueDetectionValidator
from fintran.validation.quality.outliers import OutlierDetectionValidator


# Helper functions


def generate_ir_dataframe(size: int) -> pl.DataFrame:
    """Generate test IR DataFrame with specified number of rows.
    
    Creates a DataFrame with variety in data to ensure realistic validation
    performance testing.
    
    Args:
        size: Number of rows to generate
        
    Returns:
        IR DataFrame with random but realistic data
    """
    base_date = date(2024, 1, 1)
    
    # Generate varied data
    dates = [base_date + timedelta(days=i % 365) for i in range(size)]
    accounts = [f"{1000 + (i % 10)}" for i in range(size)]
    amounts = [Decimal(str((i % 1000) + 100)) / Decimal("100") for i in range(size)]
    currencies = ["USD" if i % 3 == 0 else "EUR" if i % 3 == 1 else "GBP" for i in range(size)]
    descriptions = [f"Transaction {i}" if i % 5 != 0 else None for i in range(size)]
    references = [f"REF{i}" if i % 7 != 0 else None for i in range(size)]
    
    return pl.DataFrame({
        "date": dates,
        "account": accounts,
        "amount": amounts,
        "currency": currencies,
        "description": descriptions,
        "reference": references,
    })


def measure_validation_time(validator, df: pl.DataFrame, iterations: int = 3) -> float:
    """Measure median validation time over multiple iterations.
    
    Uses median to reduce measurement noise from system variability.
    
    Args:
        validator: Validator instance to test
        df: DataFrame to validate
        iterations: Number of iterations to run (default: 3)
        
    Returns:
        Median validation time in seconds
    """
    times = []
    
    for _ in range(iterations):
        start = time.perf_counter()
        validator.validate(df)
        end = time.perf_counter()
        times.append(end - start)
    
    # Return median time
    times.sort()
    return times[len(times) // 2]


# Performance tests


@pytest.mark.performance
class TestValidationPerformanceLinearity:
    """Property 22: Validation time scales linearly with DataFrame size.
    
    Validates Requirements 15.1, 15.2, 15.3, 15.4, 15.5
    
    These tests verify that all validators use Polars vectorized operations
    efficiently and scale linearly (O(n) complexity) with dataset size.
    """
    
    def test_positive_amounts_validator_linearity(self):
        """Test PositiveAmountsValidator scales linearly with DataFrame size.
        
        Validates that validation time approximately doubles when DataFrame
        size doubles, demonstrating O(n) complexity.
        """
        validator = PositiveAmountsValidator(account_patterns=["^1[0-9]{3}"])
        # Use larger sizes to get measurable times (> 1ms)
        sizes = [5000, 10000, 20000, 40000]
        times = []
        
        for size in sizes:
            df = generate_ir_dataframe(size)
            validation_time = measure_validation_time(validator, df)
            times.append(validation_time)
        
        # Check linearity: doubling size should approximately double time
        # Allow 0.5x to 3x tolerance for measurement noise and overhead
        for i in range(len(sizes) - 1):
            size_ratio = sizes[i + 1] / sizes[i]
            time_ratio = times[i + 1] / times[i]
            
            assert 0.5 * size_ratio <= time_ratio <= 3.0 * size_ratio, (
                f"PositiveAmountsValidator: Non-linear scaling detected. "
                f"Size ratio: {size_ratio:.2f}, Time ratio: {time_ratio:.2f}. "
                f"Sizes: {sizes[i]} -> {sizes[i+1]}, "
                f"Times: {times[i]:.4f}s -> {times[i+1]:.4f}s"
            )
    
    def test_currency_consistency_validator_linearity(self):
        """Test CurrencyConsistencyValidator scales linearly with DataFrame size."""
        validator = CurrencyConsistencyValidator(group_by=["account"])
        sizes = [5000, 10000, 20000, 40000]
        times = []
        
        for size in sizes:
            df = generate_ir_dataframe(size)
            validation_time = measure_validation_time(validator, df)
            times.append(validation_time)
        
        # Check linearity
        for i in range(len(sizes) - 1):
            size_ratio = sizes[i + 1] / sizes[i]
            time_ratio = times[i + 1] / times[i]
            
            # For very fast operations (< 1ms), allow wider tolerance due to measurement noise
            # If times are very small, constant overhead dominates and ratio may be < 1
            if times[i] < 0.001:
                assert 0.3 * size_ratio <= time_ratio <= 3.0 * size_ratio, (
                    f"CurrencyConsistencyValidator: Non-linear scaling detected. "
                    f"Size ratio: {size_ratio:.2f}, Time ratio: {time_ratio:.2f}. "
                    f"Sizes: {sizes[i]} -> {sizes[i+1]}, "
                    f"Times: {times[i]:.4f}s -> {times[i+1]:.4f}s"
                )
            else:
                assert 0.5 * size_ratio <= time_ratio <= 3.0 * size_ratio, (
                    f"CurrencyConsistencyValidator: Non-linear scaling detected. "
                    f"Size ratio: {size_ratio:.2f}, Time ratio: {time_ratio:.2f}. "
                    f"Sizes: {sizes[i]} -> {sizes[i+1]}, "
                    f"Times: {times[i]:.4f}s -> {times[i+1]:.4f}s"
                )
    
    def test_date_range_validator_linearity(self):
        """Test DateRangeValidator scales linearly with DataFrame size."""
        validator = DateRangeValidator(
            min_date=date(2020, 1, 1),
            max_date=date(2025, 12, 31)
        )
        sizes = [5000, 10000, 20000, 40000]
        times = []
        
        for size in sizes:
            df = generate_ir_dataframe(size)
            validation_time = measure_validation_time(validator, df)
            times.append(validation_time)
        
        # Check linearity
        for i in range(len(sizes) - 1):
            size_ratio = sizes[i + 1] / sizes[i]
            time_ratio = times[i + 1] / times[i]
            
            # For very fast operations (< 1ms), allow wider tolerance due to measurement noise
            if times[i] < 0.001:
                assert 0.3 * size_ratio <= time_ratio <= 3.0 * size_ratio, (
                    f"DateRangeValidator: Non-linear scaling detected. "
                    f"Size ratio: {size_ratio:.2f}, Time ratio: {time_ratio:.2f}. "
                    f"Sizes: {sizes[i]} -> {sizes[i+1]}, "
                    f"Times: {times[i]:.4f}s -> {times[i+1]:.4f}s"
                )
            else:
                assert 0.5 * size_ratio <= time_ratio <= 3.0 * size_ratio, (
                    f"DateRangeValidator: Non-linear scaling detected. "
                    f"Size ratio: {size_ratio:.2f}, Time ratio: {time_ratio:.2f}. "
                    f"Sizes: {sizes[i]} -> {sizes[i+1]}, "
                    f"Times: {times[i]:.4f}s -> {times[i+1]:.4f}s"
                )
    
    def test_duplicate_detection_validator_linearity(self):
        """Test DuplicateDetectionValidator scales linearly with DataFrame size."""
        validator = DuplicateDetectionValidator(
            fields=["date", "account", "reference"],
            mode="exact"
        )
        sizes = [5000, 10000, 20000, 40000]
        times = []
        
        for size in sizes:
            df = generate_ir_dataframe(size)
            validation_time = measure_validation_time(validator, df)
            times.append(validation_time)
        
        # Check linearity
        for i in range(len(sizes) - 1):
            size_ratio = sizes[i + 1] / sizes[i]
            time_ratio = times[i + 1] / times[i]
            
            # For very fast operations (< 1ms), allow wider tolerance due to measurement noise
            if times[i] < 0.001:
                assert 0.3 * size_ratio <= time_ratio <= 3.0 * size_ratio, (
                    f"DuplicateDetectionValidator: Non-linear scaling detected. "
                    f"Size ratio: {size_ratio:.2f}, Time ratio: {time_ratio:.2f}. "
                    f"Sizes: {sizes[i]} -> {sizes[i+1]}, "
                    f"Times: {times[i]:.4f}s -> {times[i+1]:.4f}s"
                )
            else:
                assert 0.5 * size_ratio <= time_ratio <= 3.0 * size_ratio, (
                    f"DuplicateDetectionValidator: Non-linear scaling detected. "
                    f"Size ratio: {size_ratio:.2f}, Time ratio: {time_ratio:.2f}. "
                    f"Sizes: {sizes[i]} -> {sizes[i+1]}, "
                    f"Times: {times[i]:.4f}s -> {times[i+1]:.4f}s"
                )
    
    def test_missing_value_detection_validator_linearity(self):
        """Test MissingValueDetectionValidator scales linearly with DataFrame size."""
        validator = MissingValueDetectionValidator(fields=["description", "reference"])
        sizes = [5000, 10000, 20000, 40000]
        times = []
        
        for size in sizes:
            df = generate_ir_dataframe(size)
            validation_time = measure_validation_time(validator, df)
            times.append(validation_time)
        
        # Check linearity
        for i in range(len(sizes) - 1):
            size_ratio = sizes[i + 1] / sizes[i]
            time_ratio = times[i + 1] / times[i]
            
            # For very fast operations (< 1ms), allow wider tolerance due to measurement noise
            if times[i] < 0.001:
                assert 0.3 * size_ratio <= time_ratio <= 3.0 * size_ratio, (
                    f"MissingValueDetectionValidator: Non-linear scaling detected. "
                    f"Size ratio: {size_ratio:.2f}, Time ratio: {time_ratio:.2f}. "
                    f"Sizes: {sizes[i]} -> {sizes[i+1]}, "
                    f"Times: {times[i]:.4f}s -> {times[i+1]:.4f}s"
                )
            else:
                assert 0.5 * size_ratio <= time_ratio <= 3.0 * size_ratio, (
                    f"MissingValueDetectionValidator: Non-linear scaling detected. "
                    f"Size ratio: {size_ratio:.2f}, Time ratio: {time_ratio:.2f}. "
                    f"Sizes: {sizes[i]} -> {sizes[i+1]}, "
                    f"Times: {times[i]:.4f}s -> {times[i+1]:.4f}s"
                )
    
    def test_outlier_detection_validator_linearity(self):
        """Test OutlierDetectionValidator scales linearly with DataFrame size."""
        validator = OutlierDetectionValidator(method="zscore", threshold=3.0)
        sizes = [5000, 10000, 20000, 40000]
        times = []
        
        for size in sizes:
            df = generate_ir_dataframe(size)
            validation_time = measure_validation_time(validator, df)
            times.append(validation_time)
        
        # Check linearity
        for i in range(len(sizes) - 1):
            size_ratio = sizes[i + 1] / sizes[i]
            time_ratio = times[i + 1] / times[i]
            
            assert 0.5 * size_ratio <= time_ratio <= 3.0 * size_ratio, (
                f"OutlierDetectionValidator: Non-linear scaling detected. "
                f"Size ratio: {size_ratio:.2f}, Time ratio: {time_ratio:.2f}. "
                f"Sizes: {sizes[i]} -> {sizes[i+1]}, "
                f"Times: {times[i]:.4f}s -> {times[i+1]:.4f}s"
            )


@pytest.mark.performance
class TestValidationReasonablePerformance:
    """Test that validators can process reasonable dataset sizes efficiently.
    
    Validates Requirement 15.5: Validators should handle production-scale
    datasets efficiently.
    """
    
    def test_all_validators_process_10k_rows_under_1_second(self):
        """Test that all validators can process 10,000 rows in under 1 second.
        
        This ensures validators are fast enough for typical production use cases.
        """
        df = generate_ir_dataframe(10000)
        
        validators = [
            ("PositiveAmountsValidator", PositiveAmountsValidator(account_patterns=["^1[0-9]{3}"])),
            ("CurrencyConsistencyValidator", CurrencyConsistencyValidator(group_by=["account"])),
            ("DateRangeValidator", DateRangeValidator(min_date=date(2020, 1, 1), max_date=date(2025, 12, 31))),
            ("DuplicateDetectionValidator", DuplicateDetectionValidator(fields=["date", "account", "reference"], mode="exact")),
            ("MissingValueDetectionValidator", MissingValueDetectionValidator(fields=["description", "reference"])),
            ("OutlierDetectionValidator", OutlierDetectionValidator(method="zscore", threshold=3.0)),
        ]
        
        for name, validator in validators:
            start = time.perf_counter()
            validator.validate(df)
            elapsed = time.perf_counter() - start
            
            assert elapsed < 1.0, (
                f"{name} took {elapsed:.3f}s to process 10,000 rows "
                f"(expected < 1.0s)"
            )


@pytest.mark.performance
class TestValidationEdgeCasePerformance:
    """Test validator performance on edge cases.
    
    Validates that validators handle edge cases efficiently without
    performance degradation.
    """
    
    def test_empty_dataframe_performance(self):
        """Test that validators handle empty DataFrames quickly (< 10ms)."""
        # Create empty DataFrame with proper schema
        df = pl.DataFrame(
            schema={
                "date": pl.Date,
                "account": pl.Utf8,
                "amount": pl.Decimal(scale=2),
                "currency": pl.Utf8,
                "description": pl.Utf8,
                "reference": pl.Utf8,
            }
        )
        
        validators = [
            PositiveAmountsValidator(account_patterns=["^1[0-9]{3}"]),
            CurrencyConsistencyValidator(group_by=["account"]),
            DateRangeValidator(min_date=date(2020, 1, 1), max_date=date(2025, 12, 31)),
            DuplicateDetectionValidator(fields=["date", "account", "reference"], mode="exact"),
            MissingValueDetectionValidator(fields=["description", "reference"]),
            OutlierDetectionValidator(method="zscore", threshold=3.0),
        ]
        
        for validator in validators:
            start = time.perf_counter()
            validator.validate(df)
            elapsed = time.perf_counter() - start
            
            assert elapsed < 0.01, (
                f"{validator.__class__.__name__} took {elapsed*1000:.2f}ms "
                f"to process empty DataFrame (expected < 10ms)"
            )
    
    def test_single_row_dataframe_performance(self):
        """Test that validators handle single-row DataFrames quickly (< 10ms)."""
        df = generate_ir_dataframe(1)
        
        validators = [
            PositiveAmountsValidator(account_patterns=["^1[0-9]{3}"]),
            CurrencyConsistencyValidator(group_by=["account"]),
            DateRangeValidator(min_date=date(2020, 1, 1), max_date=date(2025, 12, 31)),
            DuplicateDetectionValidator(fields=["date", "account", "reference"], mode="exact"),
            MissingValueDetectionValidator(fields=["description", "reference"]),
            OutlierDetectionValidator(method="zscore", threshold=3.0),
        ]
        
        for validator in validators:
            start = time.perf_counter()
            validator.validate(df)
            elapsed = time.perf_counter() - start
            
            assert elapsed < 0.01, (
                f"{validator.__class__.__name__} took {elapsed*1000:.2f}ms "
                f"to process single-row DataFrame (expected < 10ms)"
            )
    
    def test_large_dataframe_performance(self):
        """Test that validators can handle large DataFrames (100k rows) in reasonable time.
        
        This test ensures validators scale to production datasets.
        Target: < 10 seconds for 100k rows.
        """
        print("\nTesting large DataFrame performance (100k rows)...")
        df = generate_ir_dataframe(100000)
        
        validators = [
            ("PositiveAmountsValidator", PositiveAmountsValidator(account_patterns=["^1[0-9]{3}"])),
            ("CurrencyConsistencyValidator", CurrencyConsistencyValidator(group_by=["account"])),
            ("DateRangeValidator", DateRangeValidator(min_date=date(2020, 1, 1), max_date=date(2025, 12, 31))),
            ("DuplicateDetectionValidator", DuplicateDetectionValidator(fields=["date", "account", "reference"], mode="exact")),
            ("MissingValueDetectionValidator", MissingValueDetectionValidator(fields=["description", "reference"])),
            ("OutlierDetectionValidator", OutlierDetectionValidator(method="zscore", threshold=3.0)),
        ]
        
        for name, validator in validators:
            start = time.perf_counter()
            validator.validate(df)
            elapsed = time.perf_counter() - start
            
            print(f"  {name}: {elapsed:.3f}s")
            
            assert elapsed < 10.0, (
                f"{name} took {elapsed:.3f}s to process 100,000 rows "
                f"(expected < 10.0s)"
            )
