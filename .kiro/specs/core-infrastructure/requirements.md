# Requirements Document: Core Infrastructure

## Introduction

This document defines the requirements for the core infrastructure of fintran, a financial document transformation tool. The core infrastructure provides the foundational components for the reader → IR → writer pipeline pattern, including the Intermediate Representation schema, validation services, pipeline orchestration, base interfaces, and project scaffolding.

The core infrastructure must align with the application architecture defined in `architecture.archimate` and follow the C4 model documentation (Context, Container, Component levels).

## Glossary

- **IR**: Intermediate Representation; a canonical Polars DataFrame with fixed schema used as the common format between readers and writers
- **Core**: Application component that defines the IR schema and orchestrates the pipeline
- **Validation_Service**: Application service that validates IR schema compliance and rejects malformed input
- **Transform_Service**: Application service that orchestrates the reader → transform → writer pipeline
- **Reader**: Format-specific module that parses source files and produces IR
- **Writer**: Format-specific module that serializes IR to target format
- **Transform**: Optional enrichment step applied between reader and writer
- **Pipeline**: The complete flow from reader through optional transforms to writer
- **Schema**: The canonical structure of the IR DataFrame (date, account, amount, currency, description, reference)

## Requirements

### Requirement 1: IR Schema Definition

**User Story:** As a developer, I want a canonical IR schema definition, so that all readers and writers use a consistent data structure.

#### Acceptance Criteria

1. THE Core SHALL define an IR schema with the following fields: date (Date type), account (Utf8 type), amount (Decimal type), currency (Utf8 type), description (Utf8 type, optional), reference (Utf8 type, optional)
2. THE Core SHALL mark date, account, amount, and currency as required fields
3. THE Core SHALL mark description and reference as optional fields
4. THE Core SHALL provide a function that creates an empty IR DataFrame with the correct schema
5. THE Core SHALL provide a function that returns the IR schema definition for validation purposes

### Requirement 2: IR Schema Validation

**User Story:** As a developer, I want IR validation, so that malformed data is rejected early with clear error messages.

#### Acceptance Criteria

1. WHEN a DataFrame is provided for validation, THE Validation_Service SHALL verify all required fields are present
2. WHEN a DataFrame is provided for validation, THE Validation_Service SHALL verify all fields have correct data types
3. IF a required field is missing, THEN THE Validation_Service SHALL return an error identifying the missing field
4. IF a field has an incorrect data type, THEN THE Validation_Service SHALL return an error identifying the field and expected type
5. WHEN validation succeeds, THE Validation_Service SHALL return the validated DataFrame unchanged
6. FOR ALL valid IR DataFrames, validating twice SHALL produce the same result as validating once (idempotence property)

### Requirement 3: Reader Protocol Definition

**User Story:** As a developer, I want a Reader protocol, so that all format-specific readers implement a consistent interface.

#### Acceptance Criteria

1. THE Core SHALL define a Reader protocol with a read method that accepts a file path and returns an IR DataFrame
2. THE Reader protocol SHALL support optional configuration parameters for format-specific options
3. THE Reader protocol SHALL require implementations to raise descriptive errors for malformed input files
4. THE Reader protocol SHALL document that implementations must produce validated IR output

### Requirement 4: Writer Protocol Definition

**User Story:** As a developer, I want a Writer protocol, so that all format-specific writers implement a consistent interface.

#### Acceptance Criteria

1. THE Core SHALL define a Writer protocol with a write method that accepts an IR DataFrame and output path
2. THE Writer protocol SHALL support optional configuration parameters for format-specific options
3. THE Writer protocol SHALL require implementations to raise descriptive errors for write failures
4. THE Writer protocol SHALL document that implementations must validate IR input before writing

### Requirement 5: Transform Protocol Definition

**User Story:** As a developer, I want a Transform protocol, so that optional enrichment steps can be composed in the pipeline.

#### Acceptance Criteria

1. THE Core SHALL define a Transform protocol with a transform method that accepts an IR DataFrame and returns an IR DataFrame
2. THE Transform protocol SHALL document that implementations must not mutate the input DataFrame
3. THE Transform protocol SHALL document that implementations must return validated IR output
4. FOR ALL Transform implementations, applying the same transform twice to the same input SHALL produce equivalent results (determinism property)

### Requirement 6: Pipeline Orchestration

**User Story:** As a developer, I want pipeline orchestration, so that I can coordinate reader → transform → writer flows.

#### Acceptance Criteria

1. THE Transform_Service SHALL accept a Reader, zero or more Transforms, and a Writer as input
2. WHEN the pipeline executes, THE Transform_Service SHALL invoke the Reader with the input path
3. WHEN the Reader completes, THE Transform_Service SHALL validate the IR output using the Validation_Service
4. WHEN validation succeeds, THE Transform_Service SHALL apply each Transform in sequence to the IR
5. WHEN all Transforms complete, THE Transform_Service SHALL validate the final IR using the Validation_Service
6. WHEN final validation succeeds, THE Transform_Service SHALL invoke the Writer with the IR and output path
7. IF any step fails, THEN THE Transform_Service SHALL propagate the error with context about which step failed
8. FOR ALL valid pipelines with no transforms, THE Transform_Service SHALL produce output equivalent to directly connecting reader to writer (identity property)

### Requirement 7: IR Immutability

**User Story:** As a developer, I want IR immutability guarantees, so that transforms cannot corrupt shared data.

#### Acceptance Criteria

1. THE Core SHALL document that IR DataFrames must not be mutated in place
2. THE Transform protocol SHALL require implementations to return new DataFrames rather than modifying input
3. WHERE a Transform is applied, THE Transform_Service SHALL verify the input DataFrame is not modified (reference equality check)

### Requirement 8: Project Scaffolding

**User Story:** As a developer, I want project scaffolding, so that the project structure supports the architecture.

#### Acceptance Criteria

1. THE project SHALL use Python 3.13 or higher
2. THE project SHALL use uv for package management and virtual environment creation
3. THE project SHALL define all project configuration in pyproject.toml including dependencies, tool configurations, and project metadata
4. THE project SHALL create the following directory structure: fintran/core/, fintran/readers/, fintran/writers/, fintran/transforms/, tests/
5. WHEN uv initializes the project, THE project SHALL create and manage a virtual environment automatically

### Requirement 9: Error Handling Framework

**User Story:** As a developer, I want a consistent error handling framework, so that all errors are descriptive and actionable.

#### Acceptance Criteria

1. THE Core SHALL define custom exception classes for validation errors, reader errors, writer errors, and transform errors
2. WHEN a validation error occurs, THE Validation_Service SHALL raise a ValidationError with details about the schema violation
3. WHEN a reader error occurs, THE Reader SHALL raise a ReaderError with details about the input file and parsing failure
4. WHEN a writer error occurs, THE Writer SHALL raise a WriterError with details about the output path and serialization failure
5. WHEN a transform error occurs, THE Transform SHALL raise a TransformError with details about the transformation failure
6. THE Transform_Service SHALL wrap errors from pipeline steps with context about which step failed

### Requirement 10: Code Quality Tooling

**User Story:** As a developer, I want code quality tooling, so that the codebase maintains high standards.

#### Acceptance Criteria

1. THE project SHALL configure pre-commit hooks for automated quality checks
2. THE project SHALL use ruff for linting and formatting
3. THE project SHALL use mypy for static type checking
4. THE project SHALL configure ruff to enforce import sorting, line length limits, and code style rules
5. THE project SHALL configure mypy to require type hints and disallow untyped definitions

### Requirement 11: Testing Framework

**User Story:** As a developer, I want a testing framework, so that I can verify correctness with both example-based and property-based tests.

#### Acceptance Criteria

1. THE project SHALL use pytest as the test framework
2. THE project SHALL use Hypothesis for property-based testing
3. THE project SHALL configure pytest to discover tests in the tests/ directory
4. THE project SHALL configure Hypothesis with appropriate example generation settings
5. THE project SHALL include pytest fixtures for creating test IR DataFrames

### Requirement 12: IR Round-Trip Property

**User Story:** As a developer, I want round-trip testing for IR serialization, so that I can verify no data is lost in transformations.

#### Acceptance Criteria

1. WHERE a Writer and corresponding Reader exist for the same format, THE test suite SHALL verify that writing then reading an IR DataFrame produces an equivalent DataFrame (round-trip property)
2. THE test suite SHALL use Hypothesis to generate random valid IR DataFrames for round-trip testing
3. THE test suite SHALL verify that all required fields are preserved exactly in round-trip operations
4. THE test suite SHALL verify that optional fields are preserved when present in round-trip operations

### Requirement 13: IR Invariant Properties

**User Story:** As a developer, I want invariant property tests, so that I can verify IR transformations preserve essential characteristics.

#### Acceptance Criteria

1. FOR ALL Transforms, THE test suite SHALL verify that the number of rows is preserved or reduced (never increased without explicit justification)
2. FOR ALL Transforms, THE test suite SHALL verify that required fields remain non-null
3. FOR ALL Transforms, THE test suite SHALL verify that the schema remains valid IR
4. THE test suite SHALL use Hypothesis to generate random valid IR DataFrames for invariant testing

### Requirement 14: Validation Idempotence Property

**User Story:** As a developer, I want validation idempotence testing, so that I can verify validation is side-effect free.

#### Acceptance Criteria

1. FOR ALL valid IR DataFrames, THE test suite SHALL verify that validating twice produces the same result as validating once
2. THE test suite SHALL use Hypothesis to generate random valid IR DataFrames for idempotence testing
3. THE test suite SHALL verify that validation does not modify the input DataFrame

### Requirement 15: Pipeline Identity Property

**User Story:** As a developer, I want pipeline identity testing, so that I can verify the pipeline adds no unintended transformations.

#### Acceptance Criteria

1. WHEN a pipeline has no transforms, THE test suite SHALL verify that the output is equivalent to directly connecting reader to writer
2. THE test suite SHALL use Hypothesis to generate random valid IR DataFrames for identity testing
3. THE test suite SHALL verify this property for all reader/writer pairs that support the same format

### Requirement 16: Error Condition Testing

**User Story:** As a developer, I want error condition testing, so that I can verify the system handles invalid input gracefully.

#### Acceptance Criteria

1. THE test suite SHALL generate invalid IR DataFrames (missing required fields, wrong types) and verify they are rejected by the Validation_Service
2. THE test suite SHALL use Hypothesis to generate malformed DataFrames for error testing
3. THE test suite SHALL verify that error messages are descriptive and identify the specific validation failure
4. THE test suite SHALL verify that validation errors do not crash the system

### Requirement 17: Transform Determinism Property

**User Story:** As a developer, I want transform determinism testing, so that I can verify transforms produce consistent results.

#### Acceptance Criteria

1. FOR ALL Transforms, THE test suite SHALL verify that applying the same transform twice to the same input produces equivalent results
2. THE test suite SHALL use Hypothesis to generate random valid IR DataFrames for determinism testing
3. THE test suite SHALL verify this property holds across multiple invocations

### Requirement 18: Schema Metamorphic Property

**User Story:** As a developer, I want schema metamorphic testing, so that I can verify schema relationships without knowing exact outputs.

#### Acceptance Criteria

1. FOR ALL IR DataFrames, THE test suite SHALL verify that the number of required fields equals 4 (date, account, amount, currency)
2. FOR ALL IR DataFrames, THE test suite SHALL verify that the total number of fields is between 4 and 6 (required + optional)
3. THE test suite SHALL use Hypothesis to generate random valid IR DataFrames for metamorphic testing

## Correctness Properties Summary

The following property-based tests must be implemented:

1. **Round-Trip Property**: For all reader/writer pairs of the same format: `read(write(ir)) ≈ ir`
2. **Validation Idempotence**: For all valid IR: `validate(validate(ir)) = validate(ir)`
3. **Transform Determinism**: For all transforms: `transform(ir) = transform(ir)` (multiple invocations)
4. **IR Invariants**: For all transforms: required fields remain non-null, schema remains valid
5. **Pipeline Identity**: For pipelines with no transforms: `pipeline(ir) ≈ direct_connection(ir)`
6. **Schema Metamorphic**: For all IR: `required_field_count = 4`, `4 ≤ total_field_count ≤ 6`
7. **Error Conditions**: Invalid IR DataFrames are rejected with descriptive errors

## Architecture Alignment

All requirements reference application components defined in `architecture.archimate`:
- Core (comp-core)
- Validation_Service (svc-validate)
- Transform_Service (svc-transform)
- IR (do-ir)
- Reader protocol (comp-readers)
- Writer protocol (comp-writers)
- Transform protocol (comp-transforms)

## Dependencies

- Python 3.13+
- Polars (IR engine)
- DuckDB (I/O support)
- Cyclopts (CLI framework)
- pytest (testing framework)
- Hypothesis (property-based testing)
- ruff (linting and formatting)
- mypy (static type checking)
- uv (package management and virtual environment)
