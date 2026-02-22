"""Property-based tests for ValidatingTransform.

This module implements:
- Property 18: ValidatingTransform Metadata Attachment
- Property 19: ValidatingTransform Fail-Fast Error Handling

Validates Requirements: 12.2, 12.3, 22.1, 22.3
"""

from datetime import date
from decimal import Decimal as PyDecimal

import polars as pl
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from fintran.core.exceptions import ValidationError
from fintran.validation.pipeline import ValidationMode, ValidationPipeline
from fintran.validation.result import ValidationResult
from fintran.validation.transform import (
    ValidatingTransform,
    attach_validation_report,
    get_validation_reports,
)


# Mock validators for testing

class AlwaysPassValidator:
    """Mock validator that always passes."""
    
    def __init__(self, name: str = "always_pass"):
        self.name = name
    
    def validate(self, df: pl.DataFrame) -> ValidationResult:
        """Always return success."""
        return ValidationResult(
            is_valid=True,
            validator_name=self.name,
        )


class AlwaysFailValidator:
    """Mock validator that always fails."""
    
    def __init__(self, name: str = "always_fail", error_message: str = "Validation failed"):
        self.name = name
        self.error_message = error_message
    
    def validate(self, df: pl.DataFrame) -> ValidationResult:
        """Always return failure."""
        return ValidationResult(
            is_valid=False,
            errors=[self.error_message],
            validator_name=self.name,
        )


class AlwaysWarnValidator:
    """Mock validator that always passes with warnings."""
    
    def __init__(self, name: str = "always_warn", warning_message: str = "Data quality issue"):
        self.name = name
        self.warning_message = warning_message
    
    def validate(self, df: pl.DataFrame) -> ValidationResult:
        """Always return success with warnings."""
        return ValidationResult(
            is_valid=True,
            warnings=[self.warning_message],
            validator_name=self.name,
        )


# Hypothesis strategies

@st.composite
def valid_ir_dataframe(draw: st.DrawFn) -> pl.DataFrame:
    """Generate random valid IR DataFrame for testing.
    
    Args:
        draw: Hypothesis draw function
        
    Returns:
        Valid IR DataFrame with random data
    """
    size = draw(st.integers(min_value=1, max_value=20))
    
    dates = draw(st.lists(
        st.dates(min_value=date(2020, 1, 1), max_value=date(2025, 12, 31)),
        min_size=size,
        max_size=size
    ))
    
    accounts = draw(st.lists(
        st.text(
            alphabet=st.characters(whitelist_categories=("Lu", "Ll", "Nd")),
            min_size=1,
            max_size=10
        ),
        min_size=size,
        max_size=size
    ))
    
    amounts = draw(st.lists(
        st.decimals(
            min_value=PyDecimal("-999999.99"),
            max_value=PyDecimal("999999.99"),
            allow_nan=False,
            allow_infinity=False,
            places=2
        ),
        min_size=size,
        max_size=size
    ))
    
    currencies = draw(st.lists(
        st.sampled_from(["USD", "EUR", "GBP", "JPY"]),
        min_size=size,
        max_size=size
    ))
    
    df = pl.DataFrame({
        "date": dates,
        "account": accounts,
        "amount": amounts,
        "currency": currencies,
        "description": [None] * size,
        "reference": [None] * size,
    })
    
    df = df.with_columns(pl.col("amount").cast(pl.Decimal(precision=38, scale=10)))
    
    return df


# Property-based tests

class TestValidatingTransformProperties:
    """Property-based tests for ValidatingTransform.
    
    Implements:
    - Property 18: ValidatingTransform Metadata Attachment
    - Property 19: ValidatingTransform Fail-Fast Error Handling
    
    Validates Requirements: 12.2, 12.3, 22.1, 22.3
    """
    
    @given(valid_ir_dataframe())
    @settings(max_examples=50, deadline=None)
    def test_property_18_metadata_attachment_pass(self, df: pl.DataFrame):
        """Property 18: ValidationReport is attached to IR metadata and retrievable (pass case).
        
        Validates Requirements 12.2, 22.1, 22.3: When ValidatingTransform is applied,
        the ValidationReport should be attached to the IR metadata and be retrievable.
        
        This test verifies the case where validation passes.
        """
        # Create pipeline with passing validator
        validators = [AlwaysPassValidator("test_validator")]
        pipeline = ValidationPipeline(validators, mode=ValidationMode.CONTINUE)
        transform = ValidatingTransform(pipeline, fail_on_error=False)
        
        # Apply transform
        result_df = transform.transform(df)
        
        # Verify metadata is attached
        reports = get_validation_reports(result_df)
        assert len(reports) > 0, "No validation reports found in metadata"
        
        # Verify report content
        report = reports[0]
        assert "timestamp" in report
        assert "summary" in report
        assert report["summary"]["total_validators"] == 1
        assert report["summary"]["passed"] == 1
        assert report["summary"]["failed"] == 0
        assert report["summary"]["is_valid"] is True
        
        # Verify results are present
        assert "results" in report
        assert len(report["results"]) == 1
        assert report["results"][0]["validator_name"] == "test_validator"
        assert report["results"][0]["is_valid"] is True
    
    @given(valid_ir_dataframe())
    @settings(max_examples=50, deadline=None)
    def test_property_18_metadata_attachment_fail(self, df: pl.DataFrame):
        """Property 18: ValidationReport is attached to IR metadata and retrievable (fail case).
        
        Validates Requirements 12.2, 22.1, 22.3: When ValidatingTransform is applied
        with fail_on_error=False, the ValidationReport should be attached even when
        validation fails.
        """
        # Create pipeline with failing validator
        validators = [AlwaysFailValidator("test_validator", "Test error")]
        pipeline = ValidationPipeline(validators, mode=ValidationMode.CONTINUE)
        transform = ValidatingTransform(pipeline, fail_on_error=False)
        
        # Apply transform (should not raise)
        result_df = transform.transform(df)
        
        # Verify metadata is attached
        reports = get_validation_reports(result_df)
        assert len(reports) > 0, "No validation reports found in metadata"
        
        # Verify report content
        report = reports[0]
        assert report["summary"]["total_validators"] == 1
        assert report["summary"]["passed"] == 0
        assert report["summary"]["failed"] == 1
        assert report["summary"]["is_valid"] is False
        
        # Verify error details
        assert len(report["results"]) == 1
        assert report["results"][0]["validator_name"] == "test_validator"
        assert report["results"][0]["is_valid"] is False
        assert len(report["results"][0]["errors"]) > 0
        assert "Test error" in report["results"][0]["errors"][0]
    
    @given(valid_ir_dataframe())
    @settings(max_examples=50, deadline=None)
    def test_property_18_metadata_attachment_multiple_validators(self, df: pl.DataFrame):
        """Property 18: ValidationReport contains results from all validators.
        
        Validates Requirements 12.2, 22.1: When multiple validators are used,
        all results should be present in the attached metadata.
        """
        # Create pipeline with multiple validators
        validators = [
            AlwaysPassValidator("validator1"),
            AlwaysWarnValidator("validator2", "Warning message"),
            AlwaysPassValidator("validator3"),
        ]
        pipeline = ValidationPipeline(validators, mode=ValidationMode.CONTINUE)
        transform = ValidatingTransform(pipeline, fail_on_error=False)
        
        # Apply transform
        result_df = transform.transform(df)
        
        # Verify metadata is attached
        reports = get_validation_reports(result_df)
        assert len(reports) > 0
        
        # Verify all validators are in report
        report = reports[0]
        assert report["summary"]["total_validators"] == 3
        assert len(report["results"]) == 3
        
        # Verify validator names
        validator_names = [r["validator_name"] for r in report["results"]]
        assert "validator1" in validator_names
        assert "validator2" in validator_names
        assert "validator3" in validator_names
        
        # Verify warnings are captured
        assert report["summary"]["warnings_count"] == 1
    
    @given(valid_ir_dataframe())
    @settings(max_examples=50, deadline=None)
    def test_property_19_fail_fast_raises_error(self, df: pl.DataFrame):
        """Property 19: ValidatingTransform with fail_on_error=True raises ValidationError.
        
        Validates Requirement 12.3: If validation fails in fail-fast mode,
        ValidatingTransform shall raise a ValidationError with the ValidationReport.
        """
        # Create pipeline with failing validator
        validators = [AlwaysFailValidator("test_validator", "Critical error")]
        pipeline = ValidationPipeline(validators, mode=ValidationMode.FAIL_FAST)
        transform = ValidatingTransform(pipeline, fail_on_error=True)
        
        # Apply transform should raise ValidationError
        with pytest.raises(ValidationError) as exc_info:
            transform.transform(df)
        
        # Verify exception contains validation report
        error = exc_info.value
        assert hasattr(error, "validation_report")
        assert error.validation_report is not None
        
        # Verify report content
        report = error.validation_report
        assert not report.is_valid()
        assert report.failed == 1
        assert len(report.results) == 1
        assert report.results[0].validator_name == "test_validator"
        assert "Critical error" in report.results[0].errors[0]
    
    @given(valid_ir_dataframe())
    @settings(max_examples=50, deadline=None)
    def test_property_19_fail_fast_passes_when_valid(self, df: pl.DataFrame):
        """Property 19: ValidatingTransform with fail_on_error=True passes when validation succeeds.
        
        Validates Requirement 12.3: If validation passes, ValidatingTransform should
        not raise an error even with fail_on_error=True.
        """
        # Create pipeline with passing validator
        validators = [AlwaysPassValidator("test_validator")]
        pipeline = ValidationPipeline(validators, mode=ValidationMode.FAIL_FAST)
        transform = ValidatingTransform(pipeline, fail_on_error=True)
        
        # Apply transform should not raise
        result_df = transform.transform(df)
        
        # Verify metadata is attached
        reports = get_validation_reports(result_df)
        assert len(reports) > 0
        assert reports[0]["summary"]["is_valid"] is True
    
    @given(valid_ir_dataframe())
    @settings(max_examples=50, deadline=None)
    def test_property_19_continue_mode_no_error(self, df: pl.DataFrame):
        """Property 19: ValidatingTransform with fail_on_error=False never raises.
        
        Validates Requirement 12.4: If validation fails in continue mode,
        ValidatingTransform shall attach the ValidationReport and continue processing.
        """
        # Create pipeline with failing validator
        validators = [AlwaysFailValidator("test_validator", "Non-critical error")]
        pipeline = ValidationPipeline(validators, mode=ValidationMode.CONTINUE)
        transform = ValidatingTransform(pipeline, fail_on_error=False)
        
        # Apply transform should not raise
        result_df = transform.transform(df)
        
        # Verify metadata is attached with failure
        reports = get_validation_reports(result_df)
        assert len(reports) > 0
        assert reports[0]["summary"]["is_valid"] is False
        assert reports[0]["summary"]["failed"] == 1
    
    @given(valid_ir_dataframe())
    @settings(max_examples=50, deadline=None)
    def test_property_metadata_retrieval_consistency(self, df: pl.DataFrame):
        """Property: Metadata retrieval is consistent across multiple calls.
        
        Validates Requirement 22.3: The get_validation_reports function should
        consistently retrieve the same reports from metadata.
        """
        # Create and apply transform
        validators = [AlwaysPassValidator("test_validator")]
        pipeline = ValidationPipeline(validators)
        transform = ValidatingTransform(pipeline)
        
        result_df = transform.transform(df)
        
        # Retrieve reports multiple times
        reports1 = get_validation_reports(result_df)
        reports2 = get_validation_reports(result_df)
        
        # Verify consistency
        assert len(reports1) == len(reports2)
        assert reports1[0]["timestamp"] == reports2[0]["timestamp"]
        assert reports1[0]["summary"] == reports2[0]["summary"]


# Edge case tests

class TestValidatingTransformEdgeCases:
    """Edge case tests for ValidatingTransform.
    
    Validates Requirement 20.2, 20.3: Validators handle edge cases correctly.
    """
    
    def test_empty_dataframe(self):
        """Test ValidatingTransform with empty DataFrame."""
        df = pl.DataFrame({
            "date": [],
            "account": [],
            "amount": [],
            "currency": [],
            "description": [],
            "reference": [],
        }, schema={
            "date": pl.Date,
            "account": pl.Utf8,
            "amount": pl.Decimal(precision=38, scale=10),
            "currency": pl.Utf8,
            "description": pl.Utf8,
            "reference": pl.Utf8,
        })
        
        validators = [AlwaysPassValidator()]
        pipeline = ValidationPipeline(validators)
        transform = ValidatingTransform(pipeline)
        
        result_df = transform.transform(df)
        
        # Should not crash and should attach metadata
        reports = get_validation_reports(result_df)
        assert len(reports) > 0
    
    def test_empty_pipeline(self):
        """Test ValidatingTransform with empty pipeline."""
        df = pl.DataFrame({
            "date": [date(2024, 1, 1)],
            "account": ["ACC1"],
            "amount": [PyDecimal("100.00")],
            "currency": ["USD"],
            "description": [None],
            "reference": [None],
        })
        df = df.with_columns(pl.col("amount").cast(pl.Decimal(precision=38, scale=10)))
        
        # Empty pipeline
        pipeline = ValidationPipeline([])
        transform = ValidatingTransform(pipeline)
        
        result_df = transform.transform(df)
        
        # Should pass with no validators
        reports = get_validation_reports(result_df)
        assert len(reports) > 0
        assert reports[0]["summary"]["total_validators"] == 0
        assert reports[0]["summary"]["is_valid"] is True
    
    def test_custom_metadata_key(self):
        """Test ValidatingTransform with custom metadata key."""
        df = pl.DataFrame({
            "date": [date(2024, 1, 1)],
            "account": ["ACC1"],
            "amount": [PyDecimal("100.00")],
            "currency": ["USD"],
            "description": [None],
            "reference": [None],
        })
        df = df.with_columns(pl.col("amount").cast(pl.Decimal(precision=38, scale=10)))
        
        validators = [AlwaysPassValidator()]
        pipeline = ValidationPipeline(validators)
        transform = ValidatingTransform(pipeline, metadata_key="custom_validation")
        
        result_df = transform.transform(df)
        
        # Should be retrievable with custom key
        reports = get_validation_reports(result_df, metadata_key="custom_validation")
        assert len(reports) > 0
    
    def test_multiple_validation_runs(self):
        """Test multiple validation runs append to metadata history.
        
        Validates Requirement 22.4: Support multiple validation runs with history.
        """
        df = pl.DataFrame({
            "date": [date(2024, 1, 1)],
            "account": ["ACC1"],
            "amount": [PyDecimal("100.00")],
            "currency": ["USD"],
            "description": [None],
            "reference": [None],
        })
        df = df.with_columns(pl.col("amount").cast(pl.Decimal(precision=38, scale=10)))
        
        # First validation run
        validators1 = [AlwaysPassValidator("validator1")]
        pipeline1 = ValidationPipeline(validators1)
        transform1 = ValidatingTransform(pipeline1)
        df = transform1.transform(df)
        
        # Second validation run
        validators2 = [AlwaysPassValidator("validator2")]
        pipeline2 = ValidationPipeline(validators2)
        transform2 = ValidatingTransform(pipeline2)
        df = transform2.transform(df)
        
        # Should have two reports in history
        reports = get_validation_reports(df)
        assert len(reports) == 2
        assert reports[0]["results"][0]["validator_name"] == "validator1"
        assert reports[1]["results"][0]["validator_name"] == "validator2"


# Configuration tests

class TestValidatingTransformConfiguration:
    """Tests for ValidatingTransform configuration.
    
    Validates Requirement 12.1, 12.2, 12.3, 12.4: ValidatingTransform configuration.
    """
    
    def test_default_configuration(self):
        """Test ValidatingTransform with default configuration."""
        validators = [AlwaysPassValidator()]
        pipeline = ValidationPipeline(validators)
        transform = ValidatingTransform(pipeline)
        
        assert transform.fail_on_error is False
        assert transform.metadata_key == "validation_report"
    
    def test_fail_on_error_configuration(self):
        """Test ValidatingTransform with fail_on_error=True."""
        validators = [AlwaysPassValidator()]
        pipeline = ValidationPipeline(validators)
        transform = ValidatingTransform(pipeline, fail_on_error=True)
        
        assert transform.fail_on_error is True
    
    def test_custom_metadata_key_configuration(self):
        """Test ValidatingTransform with custom metadata key."""
        validators = [AlwaysPassValidator()]
        pipeline = ValidationPipeline(validators)
        transform = ValidatingTransform(pipeline, metadata_key="custom_key")
        
        assert transform.metadata_key == "custom_key"
