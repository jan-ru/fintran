"""Pytest configuration and fixtures for validation framework tests.

This module provides Hypothesis strategies and pytest fixtures for testing validators.
"""

import pytest
from hypothesis import settings

# Configure Hypothesis for validation tests
settings.register_profile("validation", max_examples=100, deadline=None)
settings.load_profile("validation")
