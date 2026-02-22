"""Data Validation Framework for fintran.

This module provides comprehensive validation capabilities beyond basic IR schema validation,
including business rule validation, data quality checks, custom validation rules, and detailed
validation reporting.

The validation framework integrates with the existing reader → IR → writer pipeline and
maintains the property-based testing approach established in the core infrastructure.
"""

# Business rule validators
from fintran.validation.business.amounts import PositiveAmountsValidator
from fintran.validation.business.currency import CurrencyConsistencyValidator
from fintran.validation.business.dates import DateRangeValidator

# Custom validator helpers
from fintran.validation.custom import (
    CustomValidatorBase,
    aggregate_by_group,
    check_required_fields,
    custom_validator,
    filter_by_patterns,
    format_group_error,
    format_violation_error,
    get_violations_with_index,
    safe_field_access,
)

# Declarative configuration
from fintran.validation.declarative import (
    get_configuration_schema,
    load_validation_config,
    parse_config,
)

# Exceptions
from fintran.validation.exceptions import (
    ConfigurationSchemaError,
    ValidatorConfigurationError,
    ValidatorError,
    ValidatorExecutionError,
)

# Pipeline orchestration
from fintran.validation.pipeline import ValidationMode, ValidationPipeline

# Core protocols
from fintran.validation.protocols import Validator

# Data quality validators
from fintran.validation.quality.duplicates import DuplicateDetectionValidator
from fintran.validation.quality.missing import MissingValueDetectionValidator
from fintran.validation.quality.outliers import OutlierDetectionValidator

# Reporting
from fintran.validation.report import ValidationReport
from fintran.validation.result import ValidationResult

# Transform integration
from fintran.validation.transform import (
    ValidatingTransform,
    attach_validation_report,
    get_validation_reports,
)

__all__ = [
    # Core protocols and data structures
    "Validator",
    "ValidationResult",
    "ValidationReport",
    # Pipeline orchestration
    "ValidationPipeline",
    "ValidationMode",
    # Transform integration
    "ValidatingTransform",
    "attach_validation_report",
    "get_validation_reports",
    # Business rule validators
    "PositiveAmountsValidator",
    "CurrencyConsistencyValidator",
    "DateRangeValidator",
    # Data quality validators
    "DuplicateDetectionValidator",
    "MissingValueDetectionValidator",
    "OutlierDetectionValidator",
    # Declarative configuration
    "load_validation_config",
    "parse_config",
    "get_configuration_schema",
    # Custom validator helpers
    "custom_validator",
    "CustomValidatorBase",
    "check_required_fields",
    "filter_by_patterns",
    "get_violations_with_index",
    "format_violation_error",
    "format_group_error",
    "aggregate_by_group",
    "safe_field_access",
    # Exceptions
    "ValidatorError",
    "ValidatorConfigurationError",
    "ValidatorExecutionError",
    "ConfigurationSchemaError",
]
