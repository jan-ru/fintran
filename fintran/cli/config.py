"""Configuration file loading and validation.

This module handles loading configuration from JSON and YAML files, merging
CLI arguments with file-based configuration (with CLI taking precedence), and
validating that referenced components exist in the registry.

Configuration files can specify:
- reader: Reader type name
- reader_config: Dictionary of reader-specific configuration
- writer: Writer type name
- writer_config: Dictionary of writer-specific configuration
- transforms: List of transform type names
- pipeline_config: Dictionary of pipeline-level configuration

Requirements:
    - Requirement 3.1: Support JSON format config files
    - Requirement 3.2: Support YAML format config files
    - Requirement 3.3: Load reader, writer, and transform configurations
    - Requirement 3.4: CLI arguments override config file settings
    - Requirement 3.5: Display error for invalid config file path
    - Requirement 3.6: Display error for invalid config file syntax
    - Requirement 11.2: Validate configuration syntax and structure
    - Requirement 11.5: Verify referenced readers/writers/transforms exist
    - Requirement 11.6: Verify required configuration parameters are present
"""

import json
from pathlib import Path
from typing import Any

try:
    import yaml
except ImportError:
    yaml = None  # type: ignore

from fintran.cli.registry import get_reader, get_writer, get_transform


class ConfigError(Exception):
    """Configuration file error.
    
    Raised when configuration files cannot be loaded, parsed, or validated.
    This includes file not found errors, syntax errors in JSON/YAML, and
    validation errors for missing or invalid component references.
    """
    pass


def load_config(path: Path) -> dict[str, Any]:
    """Load configuration from JSON or YAML file.
    
    Supports both JSON and YAML formats. Format is determined by file extension
    (.json, .yaml, .yml) or auto-detected if extension is ambiguous.
    
    Args:
        path: Path to configuration file
        
    Returns:
        Configuration dictionary with keys like 'reader', 'writer', 'transforms',
        'reader_config', 'writer_config', 'pipeline_config'
        
    Raises:
        ConfigError: If file cannot be loaded, parsed, or has invalid syntax
        
    Example:
        >>> from pathlib import Path
        >>> from fintran.cli.config import load_config
        >>> 
        >>> config = load_config(Path("config.json"))
        >>> print(config["reader"])  # "csv"
        >>> print(config["writer"])  # "parquet"
        
    Requirements:
        - Requirement 3.1: Support JSON format
        - Requirement 3.2: Support YAML format
        - Requirement 3.5: Error for invalid path
        - Requirement 3.6: Error for invalid syntax
    """
    if not path.exists():
        raise ConfigError(f"Configuration file not found: {path}")
    
    try:
        content = path.read_text()
        
        # Try JSON first if extension suggests it
        if path.suffix == ".json":
            return json.loads(content)
        
        # Try YAML if extension suggests it
        elif path.suffix in (".yaml", ".yml"):
            if yaml is None:
                raise ConfigError(
                    f"YAML support not available. Install pyyaml to use YAML config files."
                )
            return yaml.safe_load(content)
        
        else:
            # Try to auto-detect format
            try:
                return json.loads(content)
            except json.JSONDecodeError:
                if yaml is None:
                    raise ConfigError(
                        f"Could not parse {path} as JSON and YAML support not available. "
                        f"Install pyyaml or use .json extension."
                    )
                return yaml.safe_load(content)
                
    except json.JSONDecodeError as e:
        raise ConfigError(f"Invalid JSON in {path}: {e}")
    except Exception as e:
        if yaml is not None and isinstance(e, yaml.YAMLError):
            raise ConfigError(f"Invalid YAML in {path}: {e}")
        raise ConfigError(f"Failed to load config from {path}: {e}")


def merge_config(
    base: dict[str, Any],
    **overrides: Any
) -> dict[str, Any]:
    """Merge CLI arguments into base configuration.
    
    CLI arguments take precedence over config file values. Only non-None
    override values are applied, allowing config file defaults to be used
    when CLI arguments are not specified.
    
    Args:
        base: Base configuration from file
        **overrides: CLI argument overrides (reader, writer, transforms, etc.)
        
    Returns:
        Merged configuration dictionary with CLI arguments taking precedence
        
    Example:
        >>> from fintran.cli.config import merge_config
        >>> 
        >>> file_config = {"reader": "csv", "writer": "json"}
        >>> merged = merge_config(file_config, writer="parquet")
        >>> print(merged)  # {"reader": "csv", "writer": "parquet"}
        
    Requirements:
        - Requirement 3.4: CLI arguments override config file settings
    """
    merged = base.copy()
    
    for key, value in overrides.items():
        if value is not None:
            # Handle list arguments (like transforms) specially
            if key == "transforms" and isinstance(value, list):
                merged[key] = value  # Allow empty list to override
            elif key != "transforms":
                merged[key] = value    
    return merged


def validate_config(config: dict[str, Any]) -> list[str]:
    """Validate configuration structure and referenced components.
    
    Checks that:
    - Referenced reader exists in registry
    - Referenced writer exists in registry
    - All referenced transforms exist in registry
    
    Args:
        config: Configuration dictionary to validate
        
    Returns:
        List of validation error messages (empty list if valid)
        
    Example:
        >>> from fintran.cli.config import validate_config
        >>> 
        >>> config = {"reader": "csv", "writer": "invalid_writer"}
        >>> errors = validate_config(config)
        >>> print(errors)  # ["Unknown writer type: invalid_writer"]
        
    Requirements:
        - Requirement 11.2: Validate configuration syntax and structure
        - Requirement 11.5: Verify referenced components exist
        - Requirement 11.6: Verify required parameters present
    """
    errors = []
    
    # Check reader exists
    if "reader" in config:
        try:
            get_reader(config["reader"])
        except KeyError as e:
            errors.append(str(e))
    
    # Check writer exists
    if "writer" in config:
        try:
            get_writer(config["writer"])
        except KeyError as e:
            errors.append(str(e))
    
    # Check transforms exist
    if "transforms" in config:
        for transform in config["transforms"]:
            try:
                get_transform(transform)
            except KeyError as e:
                errors.append(str(e))
    
    return errors

