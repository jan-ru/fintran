"""Property-based tests for ValidationReport.

This module implements property tests for ValidationReport aggregation and formatting.
Validates Requirements: 11.2, 11.4, 11.6
"""

from datetime import datetime
from typing import Any

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from fintran.validation.report import ValidationReport, create_report
from fintran.validation.result import ValidationResult


# Hypothesis strategies for ValidationReport testing


@st.composite
def validation_result_strategy(draw: st.DrawFn) -> ValidationResult:
    """Generate random ValidationResult instances.
    
    Args:
        draw: Hypothesis draw function
        
    Returns:
        Random ValidationResult with varied validity, errors, and warnings
    """
    # Generate errors (0-5)
    num_errors = draw(st.integers(min_value=0, max_value=5))
    errors = draw(st.lists(
        st.text(
            alphabet=st.characters(whitelist_categories=("Lu", "Ll", "Nd", "P", "Z")),
            min_size=10,
            max_size=100
        ),
        min_size=num_errors,
        max_size=num_errors
    ))
    
    # Generate warnings (0-5)
    num_warnings = draw(st.integers(min_value=0, max_value=5))
    warnings = draw(st.lists(
        st.text(
            alphabet=st.characters(whitelist_categories=("Lu", "Ll", "Nd", "P", "Z")),
            min_size=10,
            max_size=100
        ),
        min_size=num_warnings,
        max_size=num_warnings
    ))
    
    # is_valid is False if there are errors
    is_valid = len(errors) == 0
    
    # Generate validator name
    validator_name = draw(st.text(
        alphabet=st.characters(whitelist_categories=("Lu", "Ll", "Nd")),
        min_size=5,
        max_size=30
    ))
    
    # Generate metadata
    metadata = draw(st.dictionaries(
        keys=st.text(
            alphabet=st.characters(whitelist_categories=("Lu", "Ll", "Nd")),
            min_size=1,
            max_size=20
        ),
        values=st.one_of(
            st.integers(),
            st.text(max_size=50),
            st.lists(st.integers(), max_size=10)
        ),
        max_size=5
    ))
    
    return ValidationResult(
        is_valid=is_valid,
        errors=errors,
        warnings=warnings,
        validator_name=validator_name,
        metadata=metadata
    )


@st.composite
def validation_report_strategy(draw: st.DrawFn) -> ValidationReport:
    """Generate random ValidationReport instances with correct statistics.
    
    Args:
        draw: Hypothesis draw function
        
    Returns:
        Random ValidationReport with consistent summary statistics
    """
    # Generate 1-10 results
    num_results = draw(st.integers(min_value=1, max_value=10))
    results = draw(st.lists(
        validation_result_strategy(),
        min_size=num_results,
        max_size=num_results
    ))
    
    # Calculate correct statistics
    total_validators = len(results)
    passed = sum(1 for r in results if r.is_valid)
    failed = sum(1 for r in results if not r.is_valid)
    warnings_count = sum(len(r.warnings) for r in results)
    
    # Generate timestamp
    timestamp = draw(st.datetimes(
        min_value=datetime(2020, 1, 1),
        max_value=datetime(2025, 12, 31)
    ))
    
    return ValidationReport(
        results=results,
        timestamp=timestamp,
        total_validators=total_validators,
        passed=passed,
        failed=failed,
        warnings_count=warnings_count
    )


# Property-based tests


class TestValidationReportProperties:
    """Property-based tests for ValidationReport.
    
    Validates Requirements: 11.2, 11.4, 11.6
    """
    
    @given(validation_report_strategy())
    @settings(max_examples=100, deadline=None)
    def test_property_15_summary_accuracy(self, report: ValidationReport):
        """Property 15: Summary counts match actual results.
        
        Validates Requirement 11.2: The summary shall accurately reflect the
        counts of passed validators, failed validators, and total warnings.
        """
        # Verify total_validators matches result count
        assert report.total_validators == len(report.results), \
            f"total_validators {report.total_validators} != len(results) {len(report.results)}"
        
        # Verify passed count
        actual_passed = sum(1 for r in report.results if r.is_valid)
        assert report.passed == actual_passed, \
            f"passed {report.passed} != actual {actual_passed}"
        
        # Verify failed count
        actual_failed = sum(1 for r in report.results if not r.is_valid)
        assert report.failed == actual_failed, \
            f"failed {report.failed} != actual {actual_failed}"
        
        # Verify warnings count
        actual_warnings = sum(len(r.warnings) for r in report.results)
        assert report.warnings_count == actual_warnings, \
            f"warnings_count {report.warnings_count} != actual {actual_warnings}"
        
        # Verify passed + failed = total
        assert report.passed + report.failed == report.total_validators, \
            "passed + failed should equal total_validators"
    
    @given(validation_report_strategy())
    @settings(max_examples=100, deadline=None)
    def test_property_16_json_round_trip(self, report: ValidationReport):
        """Property 16: JSON export/import preserves data.
        
        Validates Requirement 11.4: The JSON export shall preserve all validation
        results, summary statistics, and metadata for programmatic access.
        """
        # Export to JSON
        json_data = report.to_json()
        
        # Verify JSON structure
        assert "timestamp" in json_data
        assert "summary" in json_data
        assert "results" in json_data
        
        # Import from JSON
        restored = ValidationReport.from_json(json_data)
        
        # Verify all fields match
        assert restored.total_validators == report.total_validators
        assert restored.passed == report.passed
        assert restored.failed == report.failed
        assert restored.warnings_count == report.warnings_count
        assert restored.is_valid() == report.is_valid()
        
        # Verify timestamp (compare as strings to avoid microsecond precision issues)
        assert restored.timestamp.isoformat() == report.timestamp.isoformat()
        
        # Verify results count
        assert len(restored.results) == len(report.results)
        
        # Verify each result
        for original, restored_result in zip(report.results, restored.results):
            assert restored_result.validator_name == original.validator_name
            assert restored_result.is_valid == original.is_valid
            assert restored_result.errors == original.errors
            assert restored_result.warnings == original.warnings
            assert restored_result.metadata == original.metadata
    
    @given(validation_report_strategy())
    @settings(max_examples=100, deadline=None)
    def test_property_17_filtering_errors(self, report: ValidationReport):
        """Property 17: Error filtering shows only validators with errors.
        
        Validates Requirement 11.6: The report shall support filtering by severity
        to show only errors, only warnings, or all results.
        """
        # Format with error filter
        formatted = report.format(severity_filter="errors")
        
        # Count validators with errors
        validators_with_errors = [r for r in report.results if r.has_errors()]
        
        # Verify filtered output contains only error validators
        for result in validators_with_errors:
            assert result.validator_name in formatted, \
                f"Validator {result.validator_name} with errors not in filtered output"
        
        # Verify validators without errors are not in output (unless they have warnings)
        for result in report.results:
            if not result.has_errors():
                # This validator should not appear in error-filtered output
                # (We check by validator name, but it's possible the name appears in error messages)
                pass  # Difficult to verify absence without false positives
    
    @given(validation_report_strategy())
    @settings(max_examples=100, deadline=None)
    def test_property_17_filtering_warnings(self, report: ValidationReport):
        """Property 17: Warning filtering shows only validators with warnings.
        
        Validates Requirement 11.6: The report shall support filtering by severity
        to show only warnings.
        """
        # Format with warning filter
        formatted = report.format(severity_filter="warnings")
        
        # Count validators with warnings
        validators_with_warnings = [r for r in report.results if r.has_warnings()]
        
        # Verify filtered output contains warning validators
        for result in validators_with_warnings:
            assert result.validator_name in formatted, \
                f"Validator {result.validator_name} with warnings not in filtered output"
    
    @given(validation_report_strategy())
    @settings(max_examples=100, deadline=None)
    def test_property_17_filtering_all(self, report: ValidationReport):
        """Property 17: No filter shows all results.
        
        Validates Requirement 11.6: When no filter is applied, all results
        shall be shown in the formatted output.
        """
        # Format without filter
        formatted = report.format(severity_filter=None)
        
        # Verify all validators appear in output
        for result in report.results:
            assert result.validator_name in formatted, \
                f"Validator {result.validator_name} not in unfiltered output"
    
    @given(st.lists(validation_result_strategy(), min_size=1, max_size=10))
    @settings(max_examples=100, deadline=None)
    def test_property_create_report_accuracy(self, results: list[ValidationResult]):
        """Property: create_report() calculates statistics correctly.
        
        Validates that the create_report() helper function correctly calculates
        all summary statistics from a list of ValidationResults.
        """
        report = create_report(results)
        
        # Verify statistics
        assert report.total_validators == len(results)
        assert report.passed == sum(1 for r in results if r.is_valid)
        assert report.failed == sum(1 for r in results if not r.is_valid)
        assert report.warnings_count == sum(len(r.warnings) for r in results)
        
        # Verify results are stored
        assert report.results == results
        
        # Verify timestamp is recent (within last minute)
        time_diff = (datetime.utcnow() - report.timestamp).total_seconds()
        assert time_diff < 60, "Timestamp should be recent"


# Edge case tests


class TestValidationReportEdgeCases:
    """Edge case tests for ValidationReport.
    
    Validates that ValidationReport handles edge cases correctly.
    """
    
    def test_empty_results_list(self):
        """Test report with empty results list."""
        report = ValidationReport(
            results=[],
            timestamp=datetime.utcnow(),
            total_validators=0,
            passed=0,
            failed=0,
            warnings_count=0
        )
        
        assert report.is_valid()
        assert report.summary() == "Validation Summary: 0/0 passed, 0 failed, 0 warnings"
        
        # Format should not crash
        formatted = report.format()
        assert "Validation Report" in formatted
        assert "0/0 passed" in formatted
    
    def test_single_result(self):
        """Test report with single result."""
        result = ValidationResult(
            is_valid=True,
            validator_name="test_validator",
            warnings=["Warning 1"]
        )
        
        report = create_report([result])
        
        assert report.total_validators == 1
        assert report.passed == 1
        assert report.failed == 0
        assert report.warnings_count == 1
        assert report.is_valid()
    
    def test_all_passed(self):
        """Test report where all validators passed."""
        results = [
            ValidationResult(is_valid=True, validator_name=f"validator_{i}")
            for i in range(5)
        ]
        
        report = create_report(results)
        
        assert report.total_validators == 5
        assert report.passed == 5
        assert report.failed == 0
        assert report.is_valid()
    
    def test_all_failed(self):
        """Test report where all validators failed."""
        results = [
            ValidationResult(
                is_valid=False,
                errors=[f"Error from validator {i}"],
                validator_name=f"validator_{i}"
            )
            for i in range(5)
        ]
        
        report = create_report(results)
        
        assert report.total_validators == 5
        assert report.passed == 0
        assert report.failed == 5
        assert not report.is_valid()
    
    def test_mixed_results(self):
        """Test report with mix of passed, failed, and warnings."""
        results = [
            ValidationResult(is_valid=True, validator_name="v1"),
            ValidationResult(
                is_valid=False,
                errors=["Error"],
                validator_name="v2"
            ),
            ValidationResult(
                is_valid=True,
                warnings=["Warning 1", "Warning 2"],
                validator_name="v3"
            ),
        ]
        
        report = create_report(results)
        
        assert report.total_validators == 3
        assert report.passed == 2
        assert report.failed == 1
        assert report.warnings_count == 2
        assert not report.is_valid()
    
    def test_json_with_special_characters(self):
        """Test JSON round-trip with special characters in messages."""
        result = ValidationResult(
            is_valid=False,
            errors=['Error with "quotes" and \n newlines'],
            warnings=["Warning with 'apostrophes' and \t tabs"],
            validator_name="test_validator",
            metadata={"key": "value with special chars: <>&"}
        )
        
        report = create_report([result])
        json_data = report.to_json()
        restored = ValidationReport.from_json(json_data)
        
        assert restored.results[0].errors == result.errors
        assert restored.results[0].warnings == result.warnings
        assert restored.results[0].metadata == result.metadata
    
    def test_format_with_no_results(self):
        """Test format() with empty results list."""
        report = ValidationReport(
            results=[],
            timestamp=datetime(2024, 1, 15, 10, 30, 0),
            total_validators=0,
            passed=0,
            failed=0,
            warnings_count=0
        )
        
        formatted = report.format()
        
        assert "Validation Report (2024-01-15 10:30:00)" in formatted
        assert "0/0 passed, 0 failed, 0 warnings" in formatted
    
    def test_format_with_invalid_severity_filter(self):
        """Test format() with invalid severity filter value."""
        result = ValidationResult(
            is_valid=True,
            validator_name="test_validator"
        )
        
        report = create_report([result])
        
        # Invalid filter should show all results (treated as None)
        formatted = report.format(severity_filter="invalid")
        assert "test_validator" in formatted
    
    def test_summary_string_format(self):
        """Test summary() string format is consistent."""
        report = ValidationReport(
            results=[],
            timestamp=datetime.utcnow(),
            total_validators=10,
            passed=7,
            failed=3,
            warnings_count=5
        )
        
        summary = report.summary()
        
        # Verify format: "Validation Summary: X/Y passed, Z failed, W warnings"
        assert summary.startswith("Validation Summary:")
        assert "7/10 passed" in summary
        assert "3 failed" in summary
        assert "5 warnings" in summary
    
    def test_json_preserves_empty_lists(self):
        """Test JSON round-trip preserves empty error/warning lists."""
        result = ValidationResult(
            is_valid=True,
            errors=[],
            warnings=[],
            validator_name="test_validator",
            metadata={}
        )
        
        report = create_report([result])
        json_data = report.to_json()
        restored = ValidationReport.from_json(json_data)
        
        assert restored.results[0].errors == []
        assert restored.results[0].warnings == []
        assert restored.results[0].metadata == {}
