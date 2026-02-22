"""Validation-specific exceptions.

This module defines the exception hierarchy for the validation framework.
All exceptions extend from FintranError for consistent error handling.
"""

from typing import Any

from fintran.core.exceptions import FintranError


class ValidatorError(FintranError):
    """Base exception for validator-related errors.
    
    This is the base class for all validator-specific exceptions, including
    configuration errors and execution errors.
    
    Context typically includes:
        - validator_name: Name of the validator that raised the error
        - validator_type: Type of the validator
    """
    
    def __init__(
        self,
        message: str,
        validator_name: str | None = None,
        validator_type: str | None = None,
        **extra_context: Any,
    ) -> None:
        """Initialize validator error with validator details.
        
        Args:
            message: Human-readable error description
            validator_name: Name of the validator that raised the error
            validator_type: Type of the validator
            **extra_context: Additional context information
        """
        context: dict[str, Any] = {}
        if validator_name is not None:
            context["validator_name"] = validator_name
        if validator_type is not None:
            context["validator_type"] = validator_type
        context.update(extra_context)
        
        super().__init__(message, context)


class ValidatorConfigurationError(ValidatorError):
    """Exception raised when a validator is configured with invalid parameters.
    
    This exception is raised when a validator is initialized with invalid
    configuration parameters (e.g., empty account patterns, invalid thresholds).
    
    Context typically includes:
        - validator_name: Name of the validator
        - parameter: Name of the invalid parameter
        - value: Invalid value provided
        - reason: Why the value is invalid
    
    Example:
        >>> raise ValidatorConfigurationError(
        ...     "account_patterns must contain at least one pattern",
        ...     validator_name="PositiveAmountsValidator",
        ...     parameter="account_patterns",
        ...     value=[],
        ...     reason="Empty list provided"
        ... )
    """
    
    def __init__(
        self,
        message: str,
        validator_name: str | None = None,
        parameter: str | None = None,
        value: Any = None,
        reason: str | None = None,
        **extra_context: Any,
    ) -> None:
        """Initialize validator configuration error.
        
        Args:
            message: Human-readable error description
            validator_name: Name of the validator
            parameter: Name of the invalid parameter
            value: Invalid value provided
            reason: Why the value is invalid
            **extra_context: Additional context information
        """
        context: dict[str, Any] = {}
        if parameter is not None:
            context["parameter"] = parameter
        if value is not None:
            context["value"] = value
        if reason is not None:
            context["reason"] = reason
        context.update(extra_context)
        
        super().__init__(message, validator_name=validator_name, **context)


class ValidatorExecutionError(ValidatorError):
    """Exception raised when validator logic encounters an unexpected error.
    
    This exception is raised when a validator's validation logic fails due to
    an unexpected condition (e.g., accessing a non-existent field, division by zero).
    This is distinct from validation failures (which return ValidationResult with errors).
    
    Context typically includes:
        - validator_name: Name of the validator
        - field: Field that caused the error (if applicable)
        - reason: Specific reason for the execution failure
    
    Example:
        >>> raise ValidatorExecutionError(
        ...     "Required field 'amount' not found in DataFrame",
        ...     validator_name="PositiveAmountsValidator",
        ...     field="amount",
        ...     reason="Field missing from DataFrame"
        ... )
    """
    
    def __init__(
        self,
        message: str,
        validator_name: str | None = None,
        field: str | None = None,
        reason: str | None = None,
        **extra_context: Any,
    ) -> None:
        """Initialize validator execution error.
        
        Args:
            message: Human-readable error description
            validator_name: Name of the validator
            field: Field that caused the error
            reason: Specific reason for the execution failure
            **extra_context: Additional context information
        """
        context: dict[str, Any] = {}
        if field is not None:
            context["field"] = field
        if reason is not None:
            context["reason"] = reason
        context.update(extra_context)
        
        super().__init__(message, validator_name=validator_name, **context)


class ConfigurationSchemaError(FintranError):
    """Exception raised when declarative validation configuration is invalid.
    
    This exception is raised when loading declarative validation rules (from
    Python dict or YAML) that violate the configuration schema (e.g., missing
    required fields, invalid validator names, incorrect parameter types).
    
    Context typically includes:
        - validator_index: Index of the validator in the configuration
        - validator_type: Type of validator specified
        - field: Configuration field that is invalid
        - value: Invalid value provided
        - reason: Why the configuration is invalid
    
    Example:
        >>> raise ConfigurationSchemaError(
        ...     "Unknown validator type: 'invalid_validator'",
        ...     validator_index=0,
        ...     validator_type="invalid_validator",
        ...     field="type",
        ...     reason="Validator type not found in registry"
        ... )
    """
    
    def __init__(
        self,
        message: str,
        validator_index: int | None = None,
        validator_type: str | None = None,
        field: str | None = None,
        value: Any = None,
        reason: str | None = None,
        **extra_context: Any,
    ) -> None:
        """Initialize configuration schema error.
        
        Args:
            message: Human-readable error description
            validator_index: Index of the validator in the configuration
            validator_type: Type of validator specified
            field: Configuration field that is invalid
            value: Invalid value provided
            reason: Why the configuration is invalid
            **extra_context: Additional context information
        """
        context: dict[str, Any] = {}
        if validator_index is not None:
            context["validator_index"] = validator_index
        if validator_type is not None:
            context["validator_type"] = validator_type
        if field is not None:
            context["field"] = field
        if value is not None:
            context["value"] = value
        if reason is not None:
            context["reason"] = reason
        context.update(extra_context)
        
        super().__init__(message, context)
