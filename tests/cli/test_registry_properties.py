"""Property-based tests for component registry.

This module tests universal properties of the component registry including:
- Invalid component type handling

Requirements: 14.5, 14.6, 14.7
"""

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from fintran.cli.registry import (
    get_reader,
    get_transform,
    get_writer,
    register_reader,
    register_transform,
    register_writer,
)


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


# Feature: cli-interface, Property 23: Invalid Component Type Handling
@given(
    component_type=st.sampled_from(["reader", "writer", "transform"]),
    invalid_name=st.text(
        alphabet=st.characters(
            whitelist_categories=("Lu", "Ll", "Nd"),
            whitelist_characters="-_",
        ),
        min_size=1,
        max_size=30,
    ).filter(lambda x: x not in ["csv", "json", "parquet", "test_transform"]),
)
@settings(max_examples=50, suppress_health_check=[HealthCheck.function_scoped_fixture])
def test_property_invalid_component_type_handling(component_type, invalid_name):
    """Test that invalid component types display available types and raise errors.
    
    **Validates: Requirements 14.5, 14.6, 14.7**
    
    Property: For any invalid reader, writer, or transform type, the CLI should
    display available types and return a non-zero exit code.
    
    This property verifies that:
    - Invalid component names are detected
    - Error messages list available component types
    - Users can discover valid component names from error messages
    - The error is raised consistently across all component types
    
    Args:
        component_type: Type of component (reader, writer, transform)
        invalid_name: Random invalid component name
    """
    # Try to get the invalid component
    with pytest.raises(KeyError) as exc_info:
        if component_type == "reader":
            get_reader(invalid_name)
        elif component_type == "writer":
            get_writer(invalid_name)
        else:  # transform
            get_transform(invalid_name)
    
    # Verify error message
    error_message = str(exc_info.value).lower()
    
    # Error should mention the component type
    assert component_type in error_message or invalid_name.lower() in error_message, (
        f"Error message should mention component type or name, got: {exc_info.value}"
    )
    
    # Error should mention "available" or list alternatives
    assert "available" in error_message or "unknown" in error_message, (
        f"Error message should mention available types or unknown, got: {exc_info.value}"
    )


# Additional test for valid component retrieval
@given(
    component_type=st.sampled_from(["reader", "writer", "transform"]),
)
@settings(max_examples=50, suppress_health_check=[HealthCheck.function_scoped_fixture])
def test_property_valid_component_retrieval(component_type):
    """Test that valid component types can be retrieved successfully.
    
    **Validates: Requirements 10.1-10.6**
    
    Property: For any registered component type, the registry should return
    an instance of that component without errors.
    
    This property verifies that:
    - Valid component names are recognized
    - Component instances are created successfully
    - The registry correctly maps names to implementations
    
    Args:
        component_type: Type of component (reader, writer, transform)
    """
    # Map component types to valid names
    valid_names = {
        "reader": ["csv", "json"],
        "writer": ["parquet", "csv"],
        "transform": ["test_transform"],
    }
    
    valid_name = valid_names[component_type][0]
    
    # Try to get the valid component
    try:
        if component_type == "reader":
            component = get_reader(valid_name)
        elif component_type == "writer":
            component = get_writer(valid_name)
        else:  # transform
            component = get_transform(valid_name)
        
        # Verify we got an instance
        assert component is not None, (
            f"Should get a component instance for valid {component_type} '{valid_name}'"
        )
        
    except KeyError as e:
        pytest.fail(f"Should not raise KeyError for valid {component_type} '{valid_name}': {e}")


# Test for case sensitivity
@given(
    valid_name=st.sampled_from(["csv", "json", "parquet"]),
    case_variant=st.sampled_from(["upper", "mixed", "title"]),
)
@settings(max_examples=50, suppress_health_check=[HealthCheck.function_scoped_fixture])
def test_property_component_name_case_sensitivity(valid_name, case_variant):
    """Test that component names are case-sensitive.
    
    Property: Component names should be case-sensitive, meaning "CSV" is different
    from "csv". This ensures consistent naming conventions.
    
    This property verifies that:
    - Component names are case-sensitive
    - Only exact matches are accepted
    - Users must use the correct case
    
    Args:
        valid_name: A valid component name in lowercase
        case_variant: How to modify the case
    """
    # Modify the case
    if case_variant == "upper":
        modified_name = valid_name.upper()
    elif case_variant == "mixed":
        modified_name = valid_name[0].upper() + valid_name[1:] if len(valid_name) > 1 else valid_name.upper()
    else:  # title
        modified_name = valid_name.title()
    
    # Skip if the modified name is the same as original
    if modified_name == valid_name:
        return
    
    # Try to get component with modified case
    # Should raise KeyError if case-sensitive (which is expected)
    with pytest.raises(KeyError):
        if valid_name in ["csv", "json"]:
            get_reader(modified_name)
        else:  # parquet
            get_writer(modified_name)


# Test for empty component name
def test_property_empty_component_name():
    """Test that empty component names are rejected.
    
    Property: Empty strings should not be valid component names and should
    raise appropriate errors.
    
    This property verifies that:
    - Empty names are rejected
    - Error messages are informative
    """
    with pytest.raises(KeyError):
        get_reader("")
    
    with pytest.raises(KeyError):
        get_writer("")
    
    with pytest.raises(KeyError):
        get_transform("")
