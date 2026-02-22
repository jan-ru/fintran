"""Declarative validation configuration support.

This module provides functionality for loading and parsing declarative validation
configurations from Python dictionaries or YAML files. It constructs ValidationPipeline
instances from configuration specifications.

The declarative format supports:
- Validator type specification (e.g., "positive_amounts", "currency_consistency")
- Parameters for each validator
- Severity level (error or warning)
- Pipeline mode (fail_fast or continue)
- Optional conditional rules

Example configuration:
    {
        "validators": [
            {
                "type": "positive_amounts",
                "params": {"account_patterns": ["^4[0-9]{3}"]},
                "severity": "error"
            },
            {
                "type": "currency_consistency",
                "params": {"group_by": ["account"]},
                "severity": "error"
            }
        ],
        "mode": "continue"
    }
"""

from pathlib import Path
from typing import Any

from fintran.validation.business.amounts import PositiveAmountsValidator
from fintran.validation.business.currency import CurrencyConsistencyValidator
from fintran.validation.business.dates import DateRangeValidator
from fintran.validation.exceptions import ConfigurationSchemaError
from fintran.validation.pipeline import ValidationMode, ValidationPipeline
from fintran.validation.protocols import Validator
from fintran.validation.quality.duplicates import DuplicateDetectionValidator
from fintran.validation.quality.missing import MissingValueDetectionValidator
from fintran.validation.quality.outliers import OutlierDetectionValidator

# Registry mapping validator type names to validator classes
VALIDATOR_REGISTRY: dict[str, type[Validator]] = {
    "positive_amounts": PositiveAmountsValidator,
    "currency_consistency": CurrencyConsistencyValidator,
    "date_range": DateRangeValidator,
    "duplicate_detection": DuplicateDetectionValidator,
    "detect_duplicates": DuplicateDetectionValidator,  # Alias
    "missing_value_detection": MissingValueDetectionValidator,
    "detect_missing": MissingValueDetectionValidator,  # Alias
    "outlier_detection": OutlierDetectionValidator,
    "detect_outliers": OutlierDetectionValidator,  # Alias
}


def load_validation_config(
    config_source: dict[str, Any] | str | Path,
) -> dict[str, Any]:
    """Load validation configuration from dict or YAML file.
    
    Accepts either a Python dictionary or a path to a YAML file containing
    the validation configuration. The configuration is validated against the
    schema before being returned.
    
    Args:
        config_source: Either a dict containing the configuration, or a string/Path
                      pointing to a YAML file
    
    Returns:
        Dictionary containing the validated configuration
    
    Raises:
        ConfigurationSchemaError: If the configuration is invalid
        FileNotFoundError: If YAML file path doesn't exist
        ImportError: If PyYAML is not installed when loading from YAML
    
    Example:
        >>> # Load from dict
        >>> config = load_validation_config({
        ...     "validators": [{"type": "positive_amounts", "params": {...}}],
        ...     "mode": "continue"
        ... })
        >>> 
        >>> # Load from YAML file
        >>> config = load_validation_config("validation_rules.yaml")
    """
    # If it's already a dict, validate and return
    if isinstance(config_source, dict):
        _validate_config_schema(config_source)
        return config_source
    
    # Otherwise, load from YAML file
    try:
        import yaml
    except ImportError as e:
        msg = (
            "PyYAML is required to load validation configuration from YAML files. "
            "Install it with: uv add pyyaml"
        )
        raise ImportError(msg) from e
    
    # Convert to Path if string
    config_path = Path(config_source) if isinstance(config_source, str) else config_source
    
    if not config_path.exists():
        msg = f"Configuration file not found: {config_path}"
        raise FileNotFoundError(msg)
    
    # Load YAML file
    with config_path.open("r") as f:
        config = yaml.safe_load(f)
    
    if not isinstance(config, dict):
        msg = f"Configuration file must contain a YAML dictionary, got: {type(config).__name__}"
        raise ConfigurationSchemaError(
            msg,
            reason="Invalid YAML structure"
        )
    
    # Validate schema
    _validate_config_schema(config)
    
    return config


def parse_config(config: dict[str, Any]) -> ValidationPipeline:
    """Parse declarative configuration and construct ValidationPipeline.
    
    Constructs a ValidationPipeline from a declarative configuration dictionary.
    The configuration specifies validator types, parameters, and execution mode.
    
    Built-in validator types:
        - positive_amounts: Validates positive amounts for specified accounts
        - currency_consistency: Validates currency consistency within groups
        - date_range: Validates dates fall within specified range
        - duplicate_detection / detect_duplicates: Detects duplicate transactions
        - missing_value_detection / detect_missing: Detects missing values
        - outlier_detection / detect_outliers: Detects statistical outliers
    
    Args:
        config: Dictionary containing validation configuration with keys:
               - validators: List of validator specifications
               - mode: Execution mode ("fail_fast" or "continue", default: "continue")
    
    Returns:
        ValidationPipeline configured according to the specification
    
    Raises:
        ConfigurationSchemaError: If configuration is invalid or validator
                                 construction fails
    
    Example:
        >>> config = {
        ...     "validators": [
        ...         {
        ...             "type": "positive_amounts",
        ...             "params": {"account_patterns": ["^4[0-9]{3}"]},
        ...             "severity": "error"
        ...         }
        ...     ],
        ...     "mode": "continue"
        ... }
        >>> pipeline = parse_config(config)
        >>> report = pipeline.run(ir_dataframe)
    """
    # Validate schema first
    _validate_config_schema(config)
    
    # Extract mode
    mode_str = config.get("mode", "continue")
    try:
        mode = ValidationMode(mode_str)
    except ValueError as e:
        msg = f"Invalid mode: {mode_str}. Must be 'fail_fast' or 'continue'"
        raise ConfigurationSchemaError(
            msg,
            field="mode",
            value=mode_str,
            reason="Invalid mode value"
        ) from e
    
    # Construct validators
    validators: list[Validator] = []
    validator_specs = config.get("validators", [])
    
    for idx, validator_spec in enumerate(validator_specs):
        try:
            validator = _construct_validator(validator_spec, idx)
            validators.append(validator)
        except Exception as e:
            # Re-raise ConfigurationSchemaError as-is
            if isinstance(e, ConfigurationSchemaError):
                raise
            
            # Wrap other exceptions
            validator_type = validator_spec.get("type", "unknown")
            msg = f"Failed to construct validator at index {idx} (type: {validator_type}): {e}"
            raise ConfigurationSchemaError(
                msg,
                validator_index=idx,
                validator_type=validator_type,
                reason=str(e)
            ) from e
    
    # Construct and return pipeline
    return ValidationPipeline(validators=validators, mode=mode)


def _validate_config_schema(config: dict[str, Any]) -> None:
    """Validate configuration against schema.
    
    Validates that the configuration dictionary conforms to the expected schema:
    - Must have "validators" key with list value
    - Each validator must have "type" key
    - Optional "params" must be a dict
    - Optional "severity" must be "error" or "warning"
    - Optional "mode" must be "fail_fast" or "continue"
    
    Args:
        config: Configuration dictionary to validate
    
    Raises:
        ConfigurationSchemaError: If configuration violates schema
    """
    # Check top-level structure
    if not isinstance(config, dict):
        msg = f"Configuration must be a dictionary, got: {type(config).__name__}"
        raise ConfigurationSchemaError(msg, reason="Invalid configuration type")
    
    # Check validators key
    if "validators" not in config:
        msg = "Configuration must contain 'validators' key"
        raise ConfigurationSchemaError(
            msg,
            field="validators",
            reason="Required field missing"
        )
    
    validators = config["validators"]
    if not isinstance(validators, list):
        msg = f"'validators' must be a list, got: {type(validators).__name__}"
        raise ConfigurationSchemaError(
            msg,
            field="validators",
            value=validators,
            reason="Invalid field type"
        )
    
    # Validate each validator specification
    for idx, validator_spec in enumerate(validators):
        _validate_validator_spec(validator_spec, idx)
    
    # Validate mode if present
    if "mode" in config:
        mode = config["mode"]
        if mode not in ("fail_fast", "continue"):
            msg = f"Invalid mode: {mode}. Must be 'fail_fast' or 'continue'"
            raise ConfigurationSchemaError(
                msg,
                field="mode",
                value=mode,
                reason="Invalid mode value"
            )


def _validate_validator_spec(spec: dict[str, Any], index: int) -> None:
    """Validate a single validator specification.
    
    Args:
        spec: Validator specification dictionary
        index: Index of validator in configuration (for error messages)
    
    Raises:
        ConfigurationSchemaError: If specification is invalid
    """
    if not isinstance(spec, dict):
        msg = f"Validator at index {index} must be a dictionary, got: {type(spec).__name__}"
        raise ConfigurationSchemaError(
            msg,
            validator_index=index,
            reason="Invalid validator specification type"
        )
    
    # Check required 'type' field
    if "type" not in spec:
        msg = f"Validator at index {index} missing required 'type' field"
        raise ConfigurationSchemaError(
            msg,
            validator_index=index,
            field="type",
            reason="Required field missing"
        )
    
    validator_type = spec["type"]
    if not isinstance(validator_type, str):
        msg = f"Validator type at index {index} must be a string, got: {type(validator_type).__name__}"
        raise ConfigurationSchemaError(
            msg,
            validator_index=index,
            validator_type=validator_type,
            field="type",
            reason="Invalid field type"
        )
    
    # Check validator type is known
    if validator_type not in VALIDATOR_REGISTRY:
        available = ", ".join(sorted(VALIDATOR_REGISTRY.keys()))
        msg = (
            f"Unknown validator type at index {index}: '{validator_type}'. "
            f"Available types: {available}"
        )
        raise ConfigurationSchemaError(
            msg,
            validator_index=index,
            validator_type=validator_type,
            field="type",
            reason="Validator type not found in registry"
        )
    
    # Validate params if present
    if "params" in spec:
        params = spec["params"]
        if not isinstance(params, dict):
            msg = (
                f"Validator params at index {index} must be a dictionary, "
                f"got: {type(params).__name__}"
            )
            raise ConfigurationSchemaError(
                msg,
                validator_index=index,
                validator_type=validator_type,
                field="params",
                value=params,
                reason="Invalid field type"
            )
    
    # Validate severity if present
    if "severity" in spec:
        severity = spec["severity"]
        if severity not in ("error", "warning"):
            msg = (
                f"Validator severity at index {index} must be 'error' or 'warning', "
                f"got: {severity}"
            )
            raise ConfigurationSchemaError(
                msg,
                validator_index=index,
                validator_type=validator_type,
                field="severity",
                value=severity,
                reason="Invalid severity value"
            )


def _construct_validator(spec: dict[str, Any], index: int) -> Validator:
    """Construct a validator instance from specification.
    
    Args:
        spec: Validator specification dictionary
        index: Index of validator in configuration (for error messages)
    
    Returns:
        Constructed validator instance
    
    Raises:
        ConfigurationSchemaError: If validator construction fails
    """
    validator_type = spec["type"]
    params = spec.get("params", {})
    
    # Get validator class from registry
    validator_class = VALIDATOR_REGISTRY[validator_type]
    
    # Construct validator with params
    try:
        validator = validator_class(**params)
    except TypeError as e:
        # Invalid parameters for validator
        msg = (
            f"Invalid parameters for validator '{validator_type}' at index {index}: {e}"
        )
        raise ConfigurationSchemaError(
            msg,
            validator_index=index,
            validator_type=validator_type,
            field="params",
            value=params,
            reason=str(e)
        ) from e
    except ValueError as e:
        # Validator rejected parameters
        msg = (
            f"Validator '{validator_type}' at index {index} rejected parameters: {e}"
        )
        raise ConfigurationSchemaError(
            msg,
            validator_index=index,
            validator_type=validator_type,
            field="params",
            value=params,
            reason=str(e)
        ) from e
    
    return validator


def get_configuration_schema() -> dict[str, Any]:
    """Export configuration schema for documentation.
    
    Returns a dictionary describing the expected structure of validation
    configuration, including field types, required fields, and available
    validator types.
    
    Returns:
        Dictionary describing the configuration schema
    
    Example:
        >>> schema = get_configuration_schema()
        >>> print(schema["validators"]["item"]["properties"]["type"]["enum"])
        ['positive_amounts', 'currency_consistency', ...]
    """
    return {
        "type": "object",
        "required": ["validators"],
        "properties": {
            "validators": {
                "type": "array",
                "description": "List of validator specifications",
                "items": {
                    "type": "object",
                    "required": ["type"],
                    "properties": {
                        "type": {
                            "type": "string",
                            "description": "Validator type identifier",
                            "enum": sorted(VALIDATOR_REGISTRY.keys()),
                        },
                        "params": {
                            "type": "object",
                            "description": "Validator-specific parameters",
                        },
                        "severity": {
                            "type": "string",
                            "description": "Validation severity level",
                            "enum": ["error", "warning"],
                            "default": "error",
                        },
                        "condition": {
                            "type": "object",
                            "description": "Optional conditional rule (not yet implemented)",
                        },
                    },
                },
            },
            "mode": {
                "type": "string",
                "description": "Pipeline execution mode",
                "enum": ["fail_fast", "continue"],
                "default": "continue",
            },
        },
    }
