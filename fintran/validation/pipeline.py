"""ValidationPipeline orchestration.

This module defines the ValidationPipeline class that orchestrates execution of multiple validators.
"""

from collections.abc import Sequence
from datetime import datetime
from enum import Enum

import polars as pl

from fintran.validation.protocols import Validator
from fintran.validation.report import ValidationReport
from fintran.validation.result import ValidationResult


class ValidationMode(Enum):
    """Validation pipeline execution mode.
    
    Attributes:
        FAIL_FAST: Stop execution on first validation error
        CONTINUE: Run all validators regardless of failures
    """
    
    FAIL_FAST = "fail_fast"
    CONTINUE = "continue"


class ValidationPipeline:
    """Orchestrates execution of multiple validators.
    
    The pipeline applies validators in sequence and aggregates results
    into a ValidationReport. Supports fail-fast and continue modes.
    
    Requirements:
        - Requirement 10.1: Accept list of validators and execution mode
        - Requirement 10.2: Apply validators in sequence
        - Requirement 10.3: Aggregate results into ValidationReport
        - Requirement 10.4: Support fail-fast and continue modes
        - Requirement 10.6: Empty pipeline returns success
    
    Attributes:
        validators: Sequence of validators to apply
        mode: Execution mode (fail-fast or continue)
    
    Example:
        >>> from fintran.validation.business import PositiveAmountsValidator
        >>> validators = [PositiveAmountsValidator(account_patterns=["^4[0-9]{3}"])]
        >>> pipeline = ValidationPipeline(validators, mode=ValidationMode.CONTINUE)
        >>> report = pipeline.run(ir_dataframe)
        >>> if report.is_valid():
        ...     print("All validations passed!")
    """
    
    def __init__(
        self,
        validators: Sequence[Validator],
        mode: ValidationMode = ValidationMode.CONTINUE,
    ):
        """Initialize validation pipeline.
        
        Args:
            validators: Sequence of validators to apply
            mode: Execution mode (fail-fast or continue)
        """
        self.validators = validators
        self.mode = mode
    
    def run(self, df: pl.DataFrame) -> ValidationReport:
        """Run all validators and aggregate results.
        
        In FAIL_FAST mode, stops execution on the first validator that returns errors.
        In CONTINUE mode, runs all validators regardless of failures.
        
        Args:
            df: IR DataFrame to validate
            
        Returns:
            ValidationReport with aggregated results
            
        Example:
            >>> pipeline = ValidationPipeline([validator1, validator2])
            >>> report = pipeline.run(ir_dataframe)
            >>> print(f"Passed: {report.passed}/{report.total_validators}")
        """
        results: list[ValidationResult] = []
        timestamp = datetime.now()
        
        # Handle empty pipeline (identity property)
        if len(self.validators) == 0:
            return ValidationReport(
                results=[],
                timestamp=timestamp,
                total_validators=0,
                passed=0,
                failed=0,
                warnings_count=0,
            )
        
        # Execute validators
        for validator in self.validators:
            result = validator.validate(df)
            results.append(result)
            
            # In fail-fast mode, stop on first error
            if self.mode == ValidationMode.FAIL_FAST and result.has_errors():
                break
        
        # Aggregate results
        passed = sum(1 for r in results if r.is_valid)
        failed = sum(1 for r in results if not r.is_valid)
        warnings_count = sum(len(r.warnings) for r in results)
        
        return ValidationReport(
            results=results,
            timestamp=timestamp,
            total_validators=len(results),
            passed=passed,
            failed=failed,
            warnings_count=warnings_count,
        )
