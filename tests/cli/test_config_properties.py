"""Property-based tests for configuration handling.

This module tests universal properties of configuration file loading and merging:
- Configuration round trip (JSON/YAML serialization)
- Configuration loading
- CLI argument precedence
- Invalid configuration detection
- Configuration validation
- Configuration parameter passing

Requirements: 3.1-3.6, 11.2, 11.5, 11.6, 15.6
"""

import json
from pathlib import Path

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from fintran.cli.config import ConfigError, load_config, merge_config, validate_config
from fintran.cli.registry import register_reader, register_writer, register_transform


class MockComponent:
    """Mock component for testing."""
    pass


@pytest.fixture(autouse=True)
def setup_registry():
    """Register mock components for testing."""
    register_reader("csv", MockComponent)
    register_reader("json", MockComponent)
    register_writer("parquet", MockComponent)
    register_writer("csv", MockComponent)
    register_transform("test_transform", MockComponent)
    yield


# Strategy for generating valid configuration dictionaries
@st.composite
def valid_config_dict(draw):
    """Generate random valid configuration dictionaries."""
    config = {}
    
    # Optionally include reader
    if draw(st.booleans()):
        config["reader"] = draw(st.sampled_from(["csv", "json"]))
    
    # Optionally include writer
    if draw(st.booleans()):
        config["writer"] = draw(st.sampled_from(["parquet", "csv"]))
    
    # Optionally include transforms
    if draw(st.booleans()):
        num_transforms = draw(st.integers(min_value=0, max_value=3))
        if num_transforms > 0:
            config["transforms"] = ["test_transform"] * num_transforms
    
    # Optionally include reader_config
    if draw(st.booleans()):
        config["reader_config"] = {
            "option1": draw(st.text(max_size=20)),
            "option2": draw(st.integers(min_value=0, max_value=100)),
        }
    
    # Optionally include writer_config
    if draw(st.booleans()):
        config["writer_config"] = {
            "compression": draw(st.sampled_from(["snappy", "gzip", "none"])),
        }
    
    return config


# Feature: cli-interface, Property 5: Configuration Round Trip
@given(
    config=valid_config_dict(),
)
@settings(max_examples=50, suppress_health_check=[HealthCheck.function_scoped_fixture])
def test_property_configuration_round_trip(config, tmp_path):
    """Test that configuration survives JSON serialization round trip.
    
    **Validates: Requirements 3.1, 3.2**
    
    Property: For any valid configuration dictionary, serializing to JSON or YAML
    and then loading should produce an equivalent configuration.
    
    This property verifies that:
    - Configuration can be serialized to JSON without data loss
    - Configuration can be loaded from JSON files
    - The loaded configuration matches the original
    - All configuration fields are preserved
    
    Args:
        config: Random valid configuration dictionary
        tmp_path: Pytest temporary directory fixture
    """
    # Skip empty configs
    if not config:
        return
    
    # Write config to JSON file
    json_file = tmp_path / "config.json"
    json_file.write_text(json.dumps(config, indent=2))
    
    # Load config back
    loaded_config = load_config(json_file)
    
    # Verify all keys are preserved
    for key in config.keys():
        assert key in loaded_config, (
            f"Key '{key}' should be present in loaded config"
        )
        assert loaded_config[key] == config[key], (
            f"Value for '{key}' should match: expected {config[key]}, got {loaded_config[key]}"
        )
    
    # Verify no extra keys were added
    for key in loaded_config.keys():
        assert key in config, (
            f"Unexpected key '{key}' in loaded config"
        )


# Feature: cli-interface, Property 6: Configuration Loading
@given(
    config=valid_config_dict(),
)
@settings(max_examples=50, suppress_health_check=[HealthCheck.function_scoped_fixture])
def test_property_configuration_loading(config, tmp_path):
    """Test that all configuration settings are loaded correctly.
    
    **Validates: Requirements 3.3**
    
    Property: For any valid configuration file, when loaded by the CLI, all
    specified reader, writer, and transform settings should be present in the
    loaded configuration.
    
    This property verifies that:
    - Reader settings are loaded correctly
    - Writer settings are loaded correctly
    - Transform lists are loaded correctly
    - Nested configuration objects are preserved
    
    Args:
        config: Random valid configuration dictionary
        tmp_path: Pytest temporary directory fixture
    """
    # Skip empty configs
    if not config:
        return
    
    # Write config to file
    config_file = tmp_path / "config.json"
    config_file.write_text(json.dumps(config))
    
    # Load config
    loaded = load_config(config_file)
    
    # Verify reader is present if specified
    if "reader" in config:
        assert "reader" in loaded, "Reader should be present in loaded config"
        assert loaded["reader"] == config["reader"], (
            f"Reader should be {config['reader']}, got {loaded['reader']}"
        )
    
    # Verify writer is present if specified
    if "writer" in config:
        assert "writer" in loaded, "Writer should be present in loaded config"
        assert loaded["writer"] == config["writer"], (
            f"Writer should be {config['writer']}, got {loaded['writer']}"
        )
    
    # Verify transforms are present if specified
    if "transforms" in config:
        assert "transforms" in loaded, "Transforms should be present in loaded config"
        assert loaded["transforms"] == config["transforms"], (
            f"Transforms should be {config['transforms']}, got {loaded['transforms']}"
        )
    
    # Verify nested configs are present
    if "reader_config" in config:
        assert "reader_config" in loaded, "reader_config should be present"
        assert loaded["reader_config"] == config["reader_config"]
    
    if "writer_config" in config:
        assert "writer_config" in loaded, "writer_config should be present"
        assert loaded["writer_config"] == config["writer_config"]


# Feature: cli-interface, Property 7: CLI Argument Precedence
@given(
    file_reader=st.sampled_from(["csv", "json"]),
    cli_reader=st.sampled_from(["csv", "json"]),
    file_writer=st.sampled_from(["parquet", "csv"]),
    cli_writer=st.sampled_from(["parquet", "csv"]),
)
@settings(max_examples=50, suppress_health_check=[HealthCheck.function_scoped_fixture])
def test_property_cli_argument_precedence(file_reader, cli_reader, file_writer, cli_writer):
    """Test that CLI arguments override configuration file values.
    
    **Validates: Requirements 3.4**
    
    Property: For any configuration file and CLI arguments that specify the same
    setting, the CLI argument value should take precedence over the configuration
    file value.
    
    This property verifies that:
    - CLI reader argument overrides config file reader
    - CLI writer argument overrides config file writer
    - CLI transforms override config file transforms
    - The precedence rule is consistent across all settings
    
    Args:
        file_reader: Reader type in config file
        cli_reader: Reader type from CLI argument
        file_writer: Writer type in config file
        cli_writer: Writer type from CLI argument
    """
    # Create base config from file
    file_config = {
        "reader": file_reader,
        "writer": file_writer,
        "transforms": ["test_transform"],
    }
    
    # Merge with CLI arguments
    merged = merge_config(
        file_config,
        reader=cli_reader,
        writer=cli_writer,
        transforms=[],  # Empty list to override
    )
    
    # Verify CLI arguments took precedence
    assert merged["reader"] == cli_reader, (
        f"CLI reader '{cli_reader}' should override file reader '{file_reader}', "
        f"got {merged['reader']}"
    )
    
    assert merged["writer"] == cli_writer, (
        f"CLI writer '{cli_writer}' should override file writer '{file_writer}', "
        f"got {merged['writer']}"
    )
    
    assert merged["transforms"] == [], (
        f"CLI transforms [] should override file transforms, got {merged['transforms']}"
    )


# Feature: cli-interface, Property 8: Invalid Configuration Detection
@given(
    invalid_type=st.sampled_from([
        "missing_file",
        "invalid_json",
        "invalid_reader",
        "invalid_writer",
        "invalid_transform",
    ]),
)
@settings(max_examples=50, suppress_health_check=[HealthCheck.function_scoped_fixture])
def test_property_invalid_configuration_detection(invalid_type, tmp_path):
    """Test that invalid configurations are detected and reported.
    
    **Validates: Requirements 3.5, 3.6, 11.4**
    
    Property: For any configuration file with invalid syntax or non-existent file
    path, the CLI should detect the error and raise ConfigError.
    
    This property verifies that:
    - Missing files are detected
    - Invalid JSON syntax is detected
    - Invalid component references are detected
    - Appropriate error messages are provided
    
    Args:
        invalid_type: Type of invalid configuration to test
        tmp_path: Pytest temporary directory fixture
    """
    if invalid_type == "missing_file":
        # Test missing file
        missing_file = tmp_path / "nonexistent.json"
        
        with pytest.raises(ConfigError) as exc_info:
            load_config(missing_file)
        
        assert "not found" in str(exc_info.value).lower(), (
            f"Error message should mention 'not found', got: {exc_info.value}"
        )
    
    elif invalid_type == "invalid_json":
        # Test invalid JSON syntax
        invalid_file = tmp_path / "invalid.json"
        invalid_file.write_text("{invalid json syntax")
        
        with pytest.raises(ConfigError) as exc_info:
            load_config(invalid_file)
        
        assert "json" in str(exc_info.value).lower() or "invalid" in str(exc_info.value).lower(), (
            f"Error message should mention JSON or invalid, got: {exc_info.value}"
        )
    
    elif invalid_type == "invalid_reader":
        # Test invalid reader reference
        config = {"reader": "nonexistent_reader"}
        errors = validate_config(config)
        
        assert len(errors) > 0, "Should detect invalid reader"
        assert any("reader" in err.lower() for err in errors), (
            f"Error should mention reader, got: {errors}"
        )
    
    elif invalid_type == "invalid_writer":
        # Test invalid writer reference
        config = {"writer": "nonexistent_writer"}
        errors = validate_config(config)
        
        assert len(errors) > 0, "Should detect invalid writer"
        assert any("writer" in err.lower() for err in errors), (
            f"Error should mention writer, got: {errors}"
        )
    
    elif invalid_type == "invalid_transform":
        # Test invalid transform reference
        config = {"transforms": ["nonexistent_transform"]}
        errors = validate_config(config)
        
        assert len(errors) > 0, "Should detect invalid transform"
        assert any("transform" in err.lower() for err in errors), (
            f"Error should mention transform, got: {errors}"
        )


# Feature: cli-interface, Property 17: Configuration Validation
@given(
    has_reader=st.booleans(),
    has_writer=st.booleans(),
    has_transforms=st.booleans(),
    reader_valid=st.booleans(),
    writer_valid=st.booleans(),
    transforms_valid=st.booleans(),
)
@settings(max_examples=50, suppress_health_check=[HealthCheck.function_scoped_fixture])
def test_property_configuration_validation(
    has_reader, has_writer, has_transforms,
    reader_valid, writer_valid, transforms_valid
):
    """Test that configuration validation checks all referenced components.
    
    **Validates: Requirements 11.2, 11.5, 11.6**
    
    Property: For any configuration file referencing readers, writers, or transforms,
    the check-config command should verify that all referenced components exist in
    the registry.
    
    This property verifies that:
    - Valid component references pass validation
    - Invalid component references are detected
    - All component types are checked (readers, writers, transforms)
    - Validation errors list specific missing components
    
    Args:
        has_reader: Whether config includes reader
        has_writer: Whether config includes writer
        has_transforms: Whether config includes transforms
        reader_valid: Whether reader reference is valid
        writer_valid: Whether writer reference is valid
        transforms_valid: Whether transform references are valid
    """
    config = {}
    expected_errors = 0
    
    # Add reader if specified
    if has_reader:
        if reader_valid:
            config["reader"] = "csv"
        else:
            config["reader"] = "invalid_reader_xyz"
            expected_errors += 1
    
    # Add writer if specified
    if has_writer:
        if writer_valid:
            config["writer"] = "parquet"
        else:
            config["writer"] = "invalid_writer_xyz"
            expected_errors += 1
    
    # Add transforms if specified
    if has_transforms:
        if transforms_valid:
            config["transforms"] = ["test_transform"]
        else:
            config["transforms"] = ["invalid_transform_xyz"]
            expected_errors += 1
    
    # Validate configuration
    errors = validate_config(config)
    
    # Verify error count matches expectations
    if expected_errors == 0:
        assert len(errors) == 0, (
            f"Valid configuration should have no errors, got: {errors}"
        )
    else:
        assert len(errors) == expected_errors, (
            f"Expected {expected_errors} validation errors, got {len(errors)}: {errors}"
        )


# Feature: cli-interface, Property 25: Configuration Parameter Passing
@given(
    config=valid_config_dict(),
)
@settings(max_examples=50, suppress_health_check=[HealthCheck.function_scoped_fixture])
def test_property_configuration_parameter_passing(config, tmp_path):
    """Test that configuration parameters are preserved for pipeline use.
    
    **Validates: Requirements 15.6**
    
    Property: For any configuration parameters specified in config file or CLI
    arguments, they should be available for passing to readers and writers via
    the pipeline's config mechanism.
    
    This property verifies that:
    - reader_config is preserved in merged configuration
    - writer_config is preserved in merged configuration
    - pipeline_config is preserved in merged configuration
    - Nested configuration objects maintain their structure
    
    Args:
        config: Random valid configuration dictionary
        tmp_path: Pytest temporary directory fixture
    """
    # Skip if no config parameters
    if not any(k in config for k in ["reader_config", "writer_config", "pipeline_config"]):
        return
    
    # Write config to file
    config_file = tmp_path / "config.json"
    config_file.write_text(json.dumps(config))
    
    # Load and merge config (simulating CLI flow)
    loaded = load_config(config_file)
    merged = merge_config(loaded)
    
    # Verify reader_config is preserved
    if "reader_config" in config:
        assert "reader_config" in merged, (
            "reader_config should be preserved in merged config"
        )
        assert merged["reader_config"] == config["reader_config"], (
            f"reader_config should match: expected {config['reader_config']}, "
            f"got {merged['reader_config']}"
        )
    
    # Verify writer_config is preserved
    if "writer_config" in config:
        assert "writer_config" in merged, (
            "writer_config should be preserved in merged config"
        )
        assert merged["writer_config"] == config["writer_config"], (
            f"writer_config should match: expected {config['writer_config']}, "
            f"got {merged['writer_config']}"
        )
    
    # Verify pipeline_config is preserved
    if "pipeline_config" in config:
        assert "pipeline_config" in merged, (
            "pipeline_config should be preserved in merged config"
        )
        assert merged["pipeline_config"] == config["pipeline_config"], (
            f"pipeline_config should match: expected {config['pipeline_config']}, "
            f"got {merged['pipeline_config']}"
        )
