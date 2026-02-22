"""ValidationReport aggregation.

This module defines the ValidationReport class that aggregates multiple ValidationResults.
"""

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from fintran.validation.result import ValidationResult


@dataclass
class ValidationReport:
    """Aggregated report of validation results.
    
    This class aggregates multiple ValidationResults from a ValidationPipeline
    execution and provides summary statistics, formatting, and filtering capabilities.
    
    Requirements:
        - Requirement 11.1: Define ValidationReport with aggregated results
        - Requirement 11.2: Provide summary showing counts
        - Requirement 11.3: Provide detailed results for each validator
        - Requirement 11.4: Support JSON export
        - Requirement 11.5: Support human-readable text formatting
        - Requirement 11.6: Support severity filtering
    
    Attributes:
        results: List of individual ValidationResults from each validator
        timestamp: When validation was performed
        total_validators: Total number of validators run
        passed: Number of validators that passed (no errors)
        failed: Number of validators that failed (has errors)
        warnings_count: Total number of warnings across all validators
    
    Example:
        >>> report = ValidationReport(
        ...     results=[result1, result2],
        ...     timestamp=datetime.now(),
        ...     total_validators=2,
        ...     passed=1,
        ...     failed=1,
        ...     warnings_count=3
        ... )
        >>> print(report.summary())
        Validation Summary: 1/2 passed, 1 failed, 3 warnings
        >>> print(report.format(severity_filter="errors"))
        # Shows only validators with errors
    """
    
    results: list[ValidationResult]
    timestamp: datetime
    total_validators: int
    passed: int
    failed: int
    warnings_count: int
    
    def is_valid(self) -> bool:
        """Check if all validations passed (no errors).
        
        Returns:
            True if no validators failed (failed count is 0), False otherwise
            
        Example:
            >>> report = ValidationReport(results=[], timestamp=datetime.now(),
            ...                          total_validators=2, passed=2, failed=0,
            ...                          warnings_count=0)
            >>> report.is_valid()
            True
        """
        return self.failed == 0
    
    def summary(self) -> str:
        """Generate summary string.
        
        Returns:
            Human-readable summary with counts of passed/failed validators and warnings
            
        Example:
            >>> report.summary()
            'Validation Summary: 4/5 passed, 1 failed, 3 warnings'
        """
        return (
            f"Validation Summary: {self.passed}/{self.total_validators} passed, "
            f"{self.failed} failed, {self.warnings_count} warnings"
        )
    
    def to_json(self) -> dict[str, Any]:
        """Export report as JSON for programmatic access.
        
        Returns:
            Dictionary representation suitable for JSON serialization
            
        Example:
            >>> json_data = report.to_json()
            >>> import json
            >>> json.dumps(json_data, indent=2)
        """
        return {
            "timestamp": self.timestamp.isoformat(),
            "summary": {
                "total_validators": self.total_validators,
                "passed": self.passed,
                "failed": self.failed,
                "warnings_count": self.warnings_count,
                "is_valid": self.is_valid(),
            },
            "results": [
                {
                    "validator_name": r.validator_name,
                    "is_valid": r.is_valid,
                    "errors": r.errors,
                    "warnings": r.warnings,
                    "metadata": r.metadata,
                }
                for r in self.results
            ],
        }
    
    def format(self, severity_filter: str | None = None) -> str:
        """Format report as human-readable text.
        
        Args:
            severity_filter: Filter by "errors", "warnings", or None for all
            
        Returns:
            Formatted string with summary and filtered results
            
        Example:
            >>> print(report.format())
            Validation Report (2024-01-15 10:30:00)
            ========================================
            Validation Summary: 1/2 passed, 1 failed, 3 warnings
            
            [validator1] Validation passed
            Warnings:
              - Warning message
            
            [validator2] Validation failed
            Errors:
              - Error message
        """
        lines = []
        
        # Header
        lines.append(f"Validation Report ({self.timestamp.strftime('%Y-%m-%d %H:%M:%S')})")
        lines.append("=" * 60)
        lines.append(self.summary())
        lines.append("")
        
        # Filter results based on severity
        filtered_results = self.results
        if severity_filter == "errors":
            filtered_results = [r for r in self.results if r.has_errors()]
        elif severity_filter == "warnings":
            filtered_results = [r for r in self.results if r.has_warnings()]
        
        # Format each result
        for result in filtered_results:
            lines.append(result.format())
            lines.append("")
        
        return "\n".join(lines)
    
    @staticmethod
    def from_json(data: dict[str, Any]) -> "ValidationReport":
        """Reconstruct ValidationReport from JSON data.
        
        Args:
            data: Dictionary representation from to_json()
            
        Returns:
            ValidationReport instance reconstructed from JSON
            
        Example:
            >>> json_data = report.to_json()
            >>> restored = ValidationReport.from_json(json_data)
            >>> restored.is_valid() == report.is_valid()
            True
        """
        # Parse timestamp
        timestamp = datetime.fromisoformat(data["timestamp"])
        
        # Handle both nested "summary" format and flat format for backward compatibility
        if "summary" in data:
            summary = data["summary"]
            total_validators = summary["total_validators"]
            passed = summary["passed"]
            failed = summary["failed"]
            warnings_count = summary["warnings_count"]
        else:
            # Flat format (backward compatibility)
            total_validators = data["total_validators"]
            passed = data["passed"]
            failed = data["failed"]
            warnings_count = data["warnings_count"]
        
        # Reconstruct ValidationResults
        results = [
            ValidationResult(
                validator_name=r["validator_name"],
                is_valid=r["is_valid"],
                errors=r["errors"],
                warnings=r["warnings"],
                metadata=r.get("metadata", {}),
            )
            for r in data["results"]
        ]
        
        return ValidationReport(
            results=results,
            timestamp=timestamp,
            total_validators=total_validators,
            passed=passed,
            failed=failed,
            warnings_count=warnings_count,
        )


def create_report(results: list[ValidationResult]) -> ValidationReport:
    """Create ValidationReport from a list of ValidationResults.
    
    This helper function calculates summary statistics from the results and
    creates a ValidationReport with the current timestamp.
    
    Args:
        results: List of ValidationResults from validators
        
    Returns:
        ValidationReport with calculated statistics
        
    Example:
        >>> result1 = ValidationResult(is_valid=True, validator_name="v1")
        >>> result2 = ValidationResult(is_valid=False, errors=["Error"], validator_name="v2")
        >>> report = create_report([result1, result2])
        >>> report.total_validators
        2
        >>> report.passed
        1
        >>> report.failed
        1
    """
    total_validators = len(results)
    passed = sum(1 for r in results if r.is_valid)
    failed = sum(1 for r in results if not r.is_valid)
    warnings_count = sum(len(r.warnings) for r in results)
    
    return ValidationReport(
        results=results,
        timestamp=datetime.utcnow(),
        total_validators=total_validators,
        passed=passed,
        failed=failed,
        warnings_count=warnings_count,
    )
