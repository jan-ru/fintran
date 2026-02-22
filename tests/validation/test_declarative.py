"""Tests for declarative validation configuration.

This module tests the declarative configuration parser and schema validation.
"""

from datetime import date
from decimal import Decimal
from pathlib import Path

import polars as pl
import pytest

from fintran.validation.declarative import (
    VALIDATOR_REGISTRY,
    get_configuration_schema,
    load_validation_config,
    parse_config,
)
from fintran.validation.exceptions import ConfigurationSchemaError
from fintran.validation.pipeline import ValidationMode


@pytest.fixture
def sample_ir_df() -> pl.DataFrame:
    """Create a sample IR DataFrame for testing."""
    return pl.DataFrame(
        {
            "date": [date(2024, 1, 1), date(2024, 1, 2), date(2024, 1, 3)],
            "account": ["4001", "4002", "1001"],
            "amount": [Decimal("100.00"), Decimal("200.00"), Decimal("-50.00")],
            "currency": ["USD", "USD", "USD"],
            "description": ["Revenue 1", "Revenue 2", "Expense 1"],
            "reference": ["REF001", "REF002", "REF003"],
        }
    )


class TestLoadValidationConfig:
    """Tests for load_validation_config function."""

    def test_load_from_dict(self):
        """Test loading configuration from dictionary."""
        config = {
            "validators": [
                {
                    "type": "positive_amounts",
                    "params": {"account_patterns": ["^4[0-9]{3}"]},
                    "severity": "error",
                }
            ],
            "mode": "continue",
        }

        result = load_validation_config(config)

        assert result == config
        assert "validators" in result
        assert len(result["validators"]) == 1

    def test_load_from_dict_validates_schema(self):
        """Test that loading from dict validates schema."""
        invalid_config = {
            "validators": "not a list",  # Should be a list
        }

        with pytest.raises(ConfigurationSchemaError) as exc_info:
            load_validation_config(invalid_config)

        assert "must be a list" in str(exc_info.value)

    def test_load_from_nonexistent_file(self):
        """Test loading from non-existent file raises FileNotFoundError."""
        with pytest.raises(FileNotFoundError):
            load_validation_config("nonexistent_file.yaml")

    def test_load_from_yaml_requires_pyyaml(self, tmp_path, monkeypatch):
        """Test that loading from YAML requires PyYAML."""
        # Create a temporary YAML file
        yaml_file = tmp_path / "config.yaml"
        yaml_file.write_text("validators: []\n")

        # Mock import to raise ImportError
        import builtins

        original_import = builtins.__import__

        def mock_import(name, *args, **kwargs):
            if name == "yaml":
                raise ImportError("No module named 'yaml'")
            return original_import(name, *args, **kwargs)

        monkeypatch.setattr(builtins, "__import__", mock_import)

        with pytest.raises(ImportError) as exc_info:
            load_validation_config(yaml_file)

        assert "PyYAML is required" in str(exc_info.value)


class TestParseConfig:
    """Tests for parse_config function."""

    def test_parse_simple_config(self, sample_ir_df):
        """Test parsing a simple configuration."""
        config = {
            "validators": [
                {
                    "type": "positive_amounts",
                    "params": {"account_patterns": ["^4[0-9]{3}"]},
                }
            ],
            "mode": "continue",
        }

        pipeline = parse_config(config)

        assert len(pipeline.validators) == 1
        assert pipeline.mode == ValidationMode.CONTINUE

        # Run pipeline to verify it works
        report = pipeline.run(sample_ir_df)
        assert report.total_validators == 1

    def test_parse_multiple_validators(self, sample_ir_df):
        """Test parsing configuration with multiple validators."""
        config = {
            "validators": [
                {
                    "type": "positive_amounts",
                    "params": {"account_patterns": ["^4[0-9]{3}"]},
                },
                {
                    "type": "currency_consistency",
                    "params": {"group_by": ["account"]},
                },
                {
                    "type": "duplicate_detection",
                    "params": {"fields": ["date", "account", "reference"]},
                },
            ],
            "mode": "fail_fast",
        }

        pipeline = parse_config(config)

        assert len(pipeline.validators) == 3
        assert pipeline.mode == ValidationMode.FAIL_FAST

        # Run pipeline to verify it works
        report = pipeline.run(sample_ir_df)
        assert report.total_validators <= 3  # May stop early in fail-fast mode

    def test_parse_default_mode(self, sample_ir_df):
        """Test that default mode is 'continue'."""
        config = {
            "validators": [
                {
                    "type": "positive_amounts",
                    "params": {"account_patterns": ["^4[0-9]{3}"]},
                }
            ]
        }

        pipeline = parse_config(config)

        assert pipeline.mode == ValidationMode.CONTINUE

    def test_parse_validator_aliases(self, sample_ir_df):
        """Test that validator aliases work."""
        config = {
            "validators": [
                {
                    "type": "detect_duplicates",  # Alias for duplicate_detection
                    "params": {"fields": ["date", "account"]},
                },
                {
                    "type": "detect_missing",  # Alias for missing_value_detection
                    "params": {"fields": ["description"]},
                },
            ]
        }

        pipeline = parse_config(config)

        assert len(pipeline.validators) == 2

        # Run pipeline to verify it works
        report = pipeline.run(sample_ir_df)
        assert report.total_validators == 2

    def test_parse_empty_validators_list(self):
        """Test parsing configuration with empty validators list."""
        config = {"validators": [], "mode": "continue"}

        pipeline = parse_config(config)

        assert len(pipeline.validators) == 0
        assert pipeline.mode == ValidationMode.CONTINUE


class TestConfigurationSchemaValidation:
    """Tests for configuration schema validation."""

    def test_missing_validators_key(self):
        """Test that missing 'validators' key raises error."""
        config = {"mode": "continue"}

        with pytest.raises(ConfigurationSchemaError) as exc_info:
            load_validation_config(config)

        assert "validators" in str(exc_info.value)
        assert "Required field missing" in str(exc_info.value)

    def test_validators_not_list(self):
        """Test that non-list validators raises error."""
        config = {"validators": "not a list"}

        with pytest.raises(ConfigurationSchemaError) as exc_info:
            load_validation_config(config)

        assert "must be a list" in str(exc_info.value)

    def test_validator_missing_type(self):
        """Test that validator without 'type' raises error."""
        config = {
            "validators": [
                {
                    "params": {"account_patterns": ["^4[0-9]{3}"]},
                }
            ]
        }

        with pytest.raises(ConfigurationSchemaError) as exc_info:
            load_validation_config(config)

        assert "missing required 'type' field" in str(exc_info.value)
        assert exc_info.value.context.get("validator_index") == 0

    def test_unknown_validator_type(self):
        """Test that unknown validator type raises error."""
        config = {
            "validators": [
                {
                    "type": "unknown_validator",
                    "params": {},
                }
            ]
        }

        with pytest.raises(ConfigurationSchemaError) as exc_info:
            load_validation_config(config)

        assert "Unknown validator type" in str(exc_info.value)
        assert "unknown_validator" in str(exc_info.value)
        assert exc_info.value.context.get("validator_type") == "unknown_validator"

    def test_invalid_params_type(self):
        """Test that non-dict params raises error."""
        config = {
            "validators": [
                {
                    "type": "positive_amounts",
                    "params": "not a dict",
                }
            ]
        }

        with pytest.raises(ConfigurationSchemaError) as exc_info:
            load_validation_config(config)

        assert "params" in str(exc_info.value)
        assert "must be a dictionary" in str(exc_info.value)

    def test_invalid_severity(self):
        """Test that invalid severity raises error."""
        config = {
            "validators": [
                {
                    "type": "positive_amounts",
                    "params": {"account_patterns": ["^4[0-9]{3}"]},
                    "severity": "critical",  # Invalid
                }
            ]
        }

        with pytest.raises(ConfigurationSchemaError) as exc_info:
            load_validation_config(config)

        assert "severity" in str(exc_info.value)
        assert "error" in str(exc_info.value) or "warning" in str(exc_info.value)

    def test_invalid_mode(self):
        """Test that invalid mode raises error."""
        config = {
            "validators": [
                {
                    "type": "positive_amounts",
                    "params": {"account_patterns": ["^4[0-9]{3}"]},
                }
            ],
            "mode": "invalid_mode",
        }

        with pytest.raises(ConfigurationSchemaError) as exc_info:
            parse_config(config)

        assert "Invalid mode" in str(exc_info.value)
        assert "invalid_mode" in str(exc_info.value)

    def test_invalid_validator_params(self):
        """Test that invalid validator parameters raise error."""
        config = {
            "validators": [
                {
                    "type": "positive_amounts",
                    "params": {
                        "account_patterns": [],  # Empty list is invalid
                    },
                }
            ]
        }

        with pytest.raises(ConfigurationSchemaError) as exc_info:
            parse_config(config)

        assert "positive_amounts" in str(exc_info.value)
        assert exc_info.value.context.get("validator_type") == "positive_amounts"

    def test_validator_not_dict(self):
        """Test that non-dict validator spec raises error."""
        config = {"validators": ["not a dict"]}

        with pytest.raises(ConfigurationSchemaError) as exc_info:
            load_validation_config(config)

        assert "must be a dictionary" in str(exc_info.value)


class TestGetConfigurationSchema:
    """Tests for get_configuration_schema function."""

    def test_schema_structure(self):
        """Test that schema has expected structure."""
        schema = get_configuration_schema()

        assert schema["type"] == "object"
        assert "validators" in schema["required"]
        assert "validators" in schema["properties"]
        assert "mode" in schema["properties"]

    def test_schema_validators_enum(self):
        """Test that schema includes all validator types."""
        schema = get_configuration_schema()

        validator_types = schema["properties"]["validators"]["items"]["properties"][
            "type"
        ]["enum"]

        # Check that all registry types are in schema
        for validator_type in VALIDATOR_REGISTRY.keys():
            assert validator_type in validator_types

    def test_schema_mode_enum(self):
        """Test that schema includes valid modes."""
        schema = get_configuration_schema()

        modes = schema["properties"]["mode"]["enum"]

        assert "fail_fast" in modes
        assert "continue" in modes

    def test_schema_severity_enum(self):
        """Test that schema includes valid severity levels."""
        schema = get_configuration_schema()

        severity_levels = schema["properties"]["validators"]["items"]["properties"][
            "severity"
        ]["enum"]

        assert "error" in severity_levels
        assert "warning" in severity_levels


class TestValidatorRegistry:
    """Tests for validator registry."""

    def test_registry_contains_all_validators(self):
        """Test that registry contains all expected validators."""
        expected_validators = [
            "positive_amounts",
            "currency_consistency",
            "date_range",
            "duplicate_detection",
            "detect_duplicates",
            "missing_value_detection",
            "detect_missing",
            "outlier_detection",
            "detect_outliers",
        ]

        for validator_type in expected_validators:
            assert validator_type in VALIDATOR_REGISTRY

    def test_registry_classes_are_callable(self):
        """Test that all registry entries are callable classes."""
        for validator_type, validator_class in VALIDATOR_REGISTRY.items():
            assert callable(validator_class)


class TestIntegration:
    """Integration tests for declarative configuration."""

    def test_full_pipeline_from_config(self, sample_ir_df):
        """Test complete pipeline from configuration to execution."""
        config = {
            "validators": [
                {
                    "type": "positive_amounts",
                    "params": {"account_patterns": ["^4[0-9]{3}"]},
                    "severity": "error",
                },
                {
                    "type": "currency_consistency",
                    "params": {"group_by": ["account"]},
                    "severity": "error",
                },
                {
                    "type": "duplicate_detection",
                    "params": {
                        "fields": ["date", "account", "reference"],
                        "mode": "exact",
                    },
                    "severity": "warning",
                },
            ],
            "mode": "continue",
        }

        # Load and parse configuration
        loaded_config = load_validation_config(config)
        pipeline = parse_config(loaded_config)

        # Run pipeline
        report = pipeline.run(sample_ir_df)

        # Verify results
        assert report.total_validators == 3
        assert report.is_valid()  # All validations should pass

    def test_config_with_violations(self):
        """Test configuration with data that has violations."""
        # Create DataFrame with violations
        df = pl.DataFrame(
            {
                "date": [date(2024, 1, 1), date(2024, 1, 2)],
                "account": ["4001", "4001"],
                "amount": [Decimal("100.00"), Decimal("-50.00")],  # Negative amount
                "currency": ["USD", "USD"],
                "description": [None, None],
                "reference": [None, None],
            }
        )

        config = {
            "validators": [
                {
                    "type": "positive_amounts",
                    "params": {"account_patterns": ["^4[0-9]{3}"]},
                }
            ],
            "mode": "continue",
        }

        pipeline = parse_config(config)
        report = pipeline.run(df)

        # Should detect the negative amount
        assert not report.is_valid()
        assert report.failed == 1

    def test_fail_fast_mode_stops_on_error(self):
        """Test that fail-fast mode stops on first error."""
        # Create DataFrame with violations
        df = pl.DataFrame(
            {
                "date": [date(2024, 1, 1)],
                "account": ["4001"],
                "amount": [Decimal("-50.00")],  # Negative amount
                "currency": ["USD"],
                "description": [None],
                "reference": [None],
            }
        )

        config = {
            "validators": [
                {
                    "type": "positive_amounts",
                    "params": {"account_patterns": ["^4[0-9]{3}"]},
                },
                {
                    "type": "currency_consistency",
                    "params": {"group_by": ["account"]},
                },
            ],
            "mode": "fail_fast",
        }

        pipeline = parse_config(config)
        report = pipeline.run(df)

        # Should stop after first validator fails
        assert report.total_validators == 1  # Only first validator ran
        assert not report.is_valid()
