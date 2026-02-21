# Implementation Plan: Core Infrastructure

## Overview

This plan implements the foundational components for fintran's reader → IR → writer pipeline pattern. The implementation follows a bottom-up approach: first establishing the IR schema and validation, then building protocols and orchestration, and finally adding comprehensive property-based testing.

## Tasks

- [x] 1. Set up project scaffolding and dependencies
  - Initialize uv project with `uv init` to create project structure
  - Create virtual environment with `uv venv` using Python 3.13+
  - Configure pyproject.toml with all necessary sections:
    - [project] section: name, version, description, authors, requires-python = ">=3.13"
    - [project.dependencies]: Polars, PyArrow, ConnectorX, python-dotenv, Cyclopts
    - [project.optional-dependencies]: dev dependencies (pytest, Hypothesis, ruff, mypy)
    - [tool.ruff], [tool.mypy], [tool.pytest] sections (detailed configuration in later tasks)
    - [build-system]: requires = ["hatchling"], build-backend = "hatchling.build"
  - Create directory structure: fintran/core/, fintran/readers/, fintran/writers/, fintran/transforms/, tests/
  - Add __init__.py files to make packages importable
  - _Requirements: 8.1, 8.2, 8.3, 8.4, 8.5_

- [x] 2. Implement error handling framework
  - [x] 2.1 Create custom exception classes
    - Define ValidationError for schema violations
    - Define ReaderError for input parsing failures
    - Define WriterError for output serialization failures
    - Define TransformError for transformation failures
    - Each exception should include descriptive error messages and context
    - _Requirements: 9.1, 9.2, 9.3, 9.4, 9.5_

- [x] 3. Implement IR schema definition and validation service
  - [x] 3.1 Create IR schema definition in fintran/core/schema.py
    - Define IR schema with fields: date (Date), account (Utf8), amount (Decimal), currency (Utf8), description (Utf8, optional), reference (Utf8, optional)
    - Implement function to create empty IR DataFrame with correct schema
    - Implement function to return IR schema definition for validation
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5_
  
  - [x] 3.2 Implement validation service in fintran/core/schema.py
    - Verify all required fields are present (date, account, amount, currency)
    - Verify all fields have correct data types
    - Return descriptive errors for missing fields or incorrect types
    - Return validated DataFrame unchanged on success
    - Ensure validation is idempotent (does not modify input)
    - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5, 2.6_
  
  - [x] 3.3 Write property test for validation idempotence
    - **Property: Validation Idempotence**
    - **Validates: Requirements 2.6, 14.1, 14.2, 14.3**
    - For all valid IR DataFrames: validate(validate(ir)) = validate(ir)
    - Verify validation does not modify input DataFrame
  
  - [x] 3.4 Write unit tests for validation error cases
    - Test missing required fields
    - Test incorrect data types
    - Test descriptive error messages
    - _Requirements: 2.3, 2.4, 16.1, 16.2, 16.3, 16.4_

- [x] 4. Define protocol interfaces
  - [x] 4.1 Create Reader protocol in fintran/core/protocols.py
    - Define Reader protocol with read method (file_path → IR DataFrame)
    - Support optional configuration parameters
    - Document requirement to raise descriptive errors for malformed input
    - Document requirement to produce validated IR output
    - _Requirements: 3.1, 3.2, 3.3, 3.4_
  
  - [x] 4.2 Create Writer protocol in fintran/core/protocols.py
    - Define Writer protocol with write method (IR DataFrame, output_path → None)
    - Support optional configuration parameters
    - Document requirement to raise descriptive errors for write failures
    - Document requirement to validate IR input before writing
    - _Requirements: 4.1, 4.2, 4.3, 4.4_
  
  - [x] 4.3 Create Transform protocol in fintran/core/protocols.py
    - Define Transform protocol with transform method (IR DataFrame → IR DataFrame)
    - Document requirement not to mutate input DataFrame
    - Document requirement to return validated IR output
    - Document determinism requirement
    - _Requirements: 5.1, 5.2, 5.3, 5.4, 7.1, 7.2, 7.3_

- [x] 5. Implement pipeline orchestration service
  - [x] 5.1 Create Transform_Service in fintran/core/pipeline.py
    - Accept Reader, list of Transforms, and Writer as input
    - Invoke Reader with input path
    - Validate Reader output using Validation_Service
    - Apply each Transform in sequence to the IR
    - Validate final IR using Validation_Service
    - Invoke Writer with final IR and output path
    - Propagate errors with context about which step failed
    - Verify input DataFrame is not modified by Transforms (reference equality check)
    - _Requirements: 6.1, 6.2, 6.3, 6.4, 6.5, 6.6, 6.7, 7.3_
  
  - [x] 5.2 Write property test for pipeline identity
    - **Property: Pipeline Identity**
    - **Validates: Requirements 6.8, 15.1, 15.2, 15.3**
    - For pipelines with no transforms: output ≈ direct reader-to-writer connection
    - Use Hypothesis to generate random valid IR DataFrames
  
  - [x] 5.3 Write unit tests for pipeline error handling
    - Test error propagation from Reader
    - Test error propagation from Transform
    - Test error propagation from Writer
    - Verify context is added to errors
    - _Requirements: 6.7, 9.6_

- [x] 6. Checkpoint - Verify core infrastructure
  - Ensure all tests pass, ask the user if questions arise.

- [x] 7. Configure testing framework
  - [x] 7.1 Set up pytest configuration in pyproject.toml
    - Configure test discovery for tests/ directory
    - Set up test output formatting
    - Configure coverage reporting (optional)
    - _Requirements: 11.1, 11.3_
  
  - [x] 7.2 Set up Hypothesis configuration in pyproject.toml
    - Configure example generation settings
    - Set appropriate max_examples for property tests
    - Configure shrinking behavior
    - _Requirements: 11.2, 11.4_
  
  - [x] 7.3 Create pytest fixtures in tests/conftest.py
    - Fixture for creating empty IR DataFrame
    - Fixture for creating valid IR DataFrame with sample data
    - Fixture for creating invalid IR DataFrames (for error testing)
    - _Requirements: 11.5_

- [x] 8. Implement property-based test suite
  - [x] 8.1 Write property test for schema metamorphic properties
    - **Property: Schema Metamorphic**
    - **Validates: Requirements 18.1, 18.2, 18.3**
    - For all IR DataFrames: required_field_count = 4
    - For all IR DataFrames: 4 ≤ total_field_count ≤ 6
    - Use Hypothesis to generate random valid IR DataFrames
  
  - [x] 8.2 Write property test for IR invariants
    - **Property: IR Invariants**
    - **Validates: Requirements 13.1, 13.2, 13.3, 13.4**
    - For all Transforms: row count preserved or reduced
    - For all Transforms: required fields remain non-null
    - For all Transforms: schema remains valid IR
    - Use Hypothesis to generate random valid IR DataFrames
  
  - [x] 8.3 Write property test for transform determinism
    - **Property: Transform Determinism**
    - **Validates: Requirements 5.4, 17.1, 17.2, 17.3**
    - For all Transforms: transform(ir) = transform(ir) across multiple invocations
    - Use Hypothesis to generate random valid IR DataFrames
  
  - [x] 8.4 Write property test for error conditions
    - **Property: Error Handling**
    - **Validates: Requirements 16.1, 16.2, 16.3, 16.4**
    - Generate invalid IR DataFrames (missing fields, wrong types)
    - Verify they are rejected by Validation_Service
    - Verify error messages are descriptive
    - Verify errors don't crash the system

- [x] 9. Configure code quality tooling
  - [x] 9.1 Set up ruff configuration in pyproject.toml
    - Configure linting rules
    - Configure formatting rules (line length, import sorting)
    - Configure code style enforcement
    - _Requirements: 10.2, 10.4_
  
  - [x] 9.2 Set up mypy configuration in pyproject.toml
    - Require type hints for all functions
    - Disallow untyped definitions
    - Configure strict mode
    - _Requirements: 10.3, 10.5_
  
  - [x] 9.3 Create pre-commit configuration in .pre-commit-config.yaml
    - Add ruff linting hook
    - Add ruff formatting hook
    - Add mypy type checking hook
    - Configure hook execution order
    - _Requirements: 10.1_

- [x] 10. Final checkpoint - Verify complete infrastructure
  - Run all tests (unit and property-based)
  - Run all code quality checks (ruff, mypy)
  - Verify all requirements are covered
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- Tasks marked with `*` are optional and can be skipped for faster MVP
- Property-based tests use Hypothesis for automatic test case generation
- All validation operations must be idempotent and side-effect free
- IR DataFrames must never be mutated in place
- Error messages must be descriptive and actionable
- The round-trip property test (Requirement 12) will be implemented when specific reader/writer pairs are added
- Code quality tools (ruff, mypy) enforce consistency across the codebase
