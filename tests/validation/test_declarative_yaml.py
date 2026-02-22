"""Tests for YAML configuration loading.

This module tests loading validation configuration from YAML files.
Requires PyYAML to be installed.
"""

from datetime import date
from decimal import Decimal
from pathlib import Path

import polars as pl
import pytest

# Check if PyYAML is available
pytest.importorskip("yaml", reason="PyYAML not installed")

from fintran.validation.declarative import load_validation_config, parse_config
from fintran.validation.exceptions import ConfigurationSchemaError


@pytest.fixture
def sample_ir_df() -> pl.DataFrame:
    """Create a sample IR DataFrame for testing."""
    return pl.DataFrame(
        {
            "date": [date(2024, 1, 1), date(2024, 1, 2)],
            "account": ["4001", "4002"],
            "amount": [Decimal("100.00"), Decimal("200.00")],
            "currency": ["USD", "USD"],
            "description": ["Revenue 1", "Revenue 2"],
            "reference": ["REF001", "REF002"],
        }
    )


class TestYAMLLoading:
    """Tests for loading configuration from YAML files."""

    def test_load_from_yaml_file(self, tmp_path, sample_ir_df):
        """Test loading configuration from YAML file."""
        yaml_content = """
validators:
  - type: positive_amounts
    params:
      account_patterns:
        - "^4[0-9]{3}"
    severity: error
  
  - type: currency_consistency
    params:
      group_by:
        - account
    severity: error

mode: continue
"""
        yaml_file = tmp_path / "validation_config.yaml"
        yaml_file.write_text(yaml_content)

        # Load configuration
        config = load_validation_config(yaml_file)

        assert "validators" in config
        assert len(config["validators"]) == 2
        assert config["mode"] == "continue"

        # Parse and run pipeline
        pipeline = parse_config(config)
        report = pipeline.run(sample_ir_df)

        assert report.total_validators == 2
        assert report.is_valid()

    def test_load_from_yaml_string_path(self, tmp_path, sample_ir_df):
        """Test loading from YAML file using string path."""
        yaml_content = """
validators:
  - type: duplicate_detection
    params:
      fields:
        - date
        - account
        - reference
      mode: exact

mode: continue
"""
        yaml_file = tmp_path / "config.yaml"
        yaml_file.write_text(yaml_content)

        # Load using string path
        config = load_validation_config(str(yaml_file))

        assert "validators" in config
        assert len(config["validators"]) == 1

        # Parse and run pipeline
        pipeline = parse_config(config)
        report = pipeline.run(sample_ir_df)

        assert report.total_validators == 1

    def test_load_invalid_yaml_structure(self, tmp_path):
        """Test that invalid YAML structure raises error."""
        yaml_content = """
- this is a list
- not a dictionary
"""
        yaml_file = tmp_path / "invalid.yaml"
        yaml_file.write_text(yaml_content)

        with pytest.raises(ConfigurationSchemaError) as exc_info:
            load_validation_config(yaml_file)

        assert "must contain a YAML dictionary" in str(exc_info.value)

    def test_load_yaml_with_schema_errors(self, tmp_path):
        """Test that YAML with schema errors raises ConfigurationSchemaError."""
        yaml_content = """
validators:
  - type: unknown_validator
    params: {}
"""
        yaml_file = tmp_path / "invalid_schema.yaml"
        yaml_file.write_text(yaml_content)

        with pytest.raises(ConfigurationSchemaError) as exc_info:
            load_validation_config(yaml_file)

        assert "Unknown validator type" in str(exc_info.value)

    def test_yaml_with_multiple_validators(self, tmp_path, sample_ir_df):
        """Test YAML with multiple validators of different types."""
        yaml_content = """
validators:
  - type: positive_amounts
    params:
      account_patterns:
        - "^4[0-9]{3}"
  
  - type: currency_consistency
    params:
      group_by:
        - account
  
  - type: duplicate_detection
    params:
      fields:
        - date
        - account
      mode: exact
  
  - type: missing_value_detection
    params:
      fields:
        - description
  
  - type: outlier_detection
    params:
      method: zscore
      threshold: 3.0

mode: continue
"""
        yaml_file = tmp_path / "comprehensive.yaml"
        yaml_file.write_text(yaml_content)

        # Load and parse
        config = load_validation_config(yaml_file)
        pipeline = parse_config(config)

        assert len(pipeline.validators) == 5

        # Run pipeline
        report = pipeline.run(sample_ir_df)
        assert report.total_validators == 5

    def test_yaml_with_fail_fast_mode(self, tmp_path):
        """Test YAML configuration with fail_fast mode."""
        yaml_content = """
validators:
  - type: positive_amounts
    params:
      account_patterns:
        - "^4[0-9]{3}"

mode: fail_fast
"""
        yaml_file = tmp_path / "fail_fast.yaml"
        yaml_file.write_text(yaml_content)

        config = load_validation_config(yaml_file)
        pipeline = parse_config(config)

        from fintran.validation.pipeline import ValidationMode

        assert pipeline.mode == ValidationMode.FAIL_FAST

    def test_yaml_with_validator_aliases(self, tmp_path, sample_ir_df):
        """Test YAML with validator aliases."""
        yaml_content = """
validators:
  - type: detect_duplicates
    params:
      fields:
        - date
        - account
  
  - type: detect_missing
    params:
      fields:
        - description
  
  - type: detect_outliers
    params:
      method: iqr
      threshold: 1.5

mode: continue
"""
        yaml_file = tmp_path / "aliases.yaml"
        yaml_file.write_text(yaml_content)

        config = load_validation_config(yaml_file)
        pipeline = parse_config(config)

        assert len(pipeline.validators) == 3

        # Run pipeline
        report = pipeline.run(sample_ir_df)
        assert report.total_validators == 3
