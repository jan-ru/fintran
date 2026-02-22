# Requirements Document: Data Validation Framework

## Introduction

This document defines the requirements for the Data Validation Framework feature of fintran. The framework extends beyond basic IR schema validation to provide business rule validation, data quality checks, custom validation rules, and comprehensive validation reporting. The framework integrates with the existing reader → IR → writer pipeline and maintains the property-based testing approach established in the core infrastructure.

The Data Validation Framework enables users to define validation rules declaratively, provides built-in validators for common financial data checks, generates detailed validation reports, and makes validation composable and reusable across different pipeline configurations.

## Glossary

- **Validation_Framework**: Application component that provides business rule validation, data quality checks, and custom validation capabilities beyond basic schema validation
- **Validator**: A callable that accepts an IR DataFrame and returns a ValidationResult indicating success or failure with detailed messages
- **ValidationResult**: Data object containing validation status (pass/fail), error messages, warnings, and metadata about what was validated
- **ValidationRule**: A declarative specification of a validation check that can be applied to IR DataFrames
- **BusinessRuleValidator**: Validator that enforces domain-specific constraints (e.g., amounts must be positive for certain transaction types)
- **DataQualityValidator**: Validator that detects data quality issues (e.g., duplicates, missing values, outliers, inconsistencies)
- **ValidationReport**: Aggregated report showing all validation results with clear pass/fail status and actionable error messages
- **ValidationPipeline**: Ordered sequence of validators that are applied to an IR DataFrame
- **IR**: Intermediate Representation; the canonical Polars DataFrame with schema defined in core infrastructure
- **Core**: Application component that defines the IR schema and basic validation (from core infrastructure)

## Requirements

### Requirement 1: Validator Protocol Definition

**User Story:** As a developer, I want a Validator protocol, so that all validators implement a consistent interface.

#### Acceptance Criteria

1. THE Validation_Framework SHALL define a Validator protocol with a validate method that accepts an IR DataFrame and returns a ValidationResult
2. THE Validator protocol SHALL support optional configuration parameters for validator-specific options
3. THE Validator protocol SHALL document that implementations must not mutate the input DataFrame
4. THE Validator protocol SHALL document that implementations must be deterministic (same input produces same result)
5. FOR ALL Validator implementations, applying the same validator twice to the same input SHALL produce equivalent results (determinism property)

### Requirement 2: ValidationResult Data Structure

**User Story:** As a developer, I want a ValidationResult data structure, so that validation outcomes are consistently represented.

#### Acceptance Criteria

1. THE Validation_Framework SHALL define a ValidationResult class with fields: is_valid (bool), errors (list of error messages), warnings (list of warning messages), validator_name (str), metadata (dict)
2. THE ValidationResult SHALL provide a method to check if validation passed (no errors)
3. THE ValidationResult SHALL provide a method to check if validation has warnings
4. THE ValidationResult SHALL provide a method to format the result as a human-readable string
5. THE ValidationResult SHALL support combining multiple ValidationResults into an aggregated result

### Requirement 3: Business Rule Validator - Positive Amounts

**User Story:** As a financial analyst, I want to validate that amounts are positive for income transactions, so that data errors are caught early.

#### Acceptance Criteria

1. THE BusinessRuleValidator SHALL provide a positive_amounts validator that accepts a list of account patterns
2. WHEN the positive_amounts validator is applied, THE BusinessRuleValidator SHALL check that all amounts are greater than zero for accounts matching the specified patterns
3. IF any amount is zero or negative for matching accounts, THEN THE BusinessRuleValidator SHALL return a ValidationResult with errors identifying the row indices and account names
4. WHEN all amounts are positive for matching accounts, THE BusinessRuleValidator SHALL return a ValidationResult indicating success
5. THE positive_amounts validator SHALL support regex patterns for account matching

### Requirement 4: Business Rule Validator - Currency Consistency

**User Story:** As a financial analyst, I want to validate that currency codes are consistent within account groups, so that mixed-currency errors are detected.

#### Acceptance Criteria

1. THE BusinessRuleValidator SHALL provide a currency_consistency validator that accepts account grouping rules
2. WHEN the currency_consistency validator is applied, THE BusinessRuleValidator SHALL check that all transactions within the same account group use the same currency
3. IF multiple currencies are found within an account group, THEN THE BusinessRuleValidator SHALL return a ValidationResult with errors identifying the account group and conflicting currencies
4. WHEN currency is consistent within all account groups, THE BusinessRuleValidator SHALL return a ValidationResult indicating success
5. WHERE no account grouping rules are provided, THE BusinessRuleValidator SHALL validate that the entire DataFrame uses a single currency

### Requirement 5: Business Rule Validator - Date Range Validation

**User Story:** As a financial analyst, I want to validate that transaction dates fall within expected ranges, so that data entry errors are caught.

#### Acceptance Criteria

1. THE BusinessRuleValidator SHALL provide a date_range validator that accepts minimum and maximum date boundaries
2. WHEN the date_range validator is applied, THE BusinessRuleValidator SHALL check that all transaction dates fall within the specified range
3. IF any date falls outside the range, THEN THE BusinessRuleValidator SHALL return a ValidationResult with errors identifying the row indices and out-of-range dates
4. WHEN all dates are within range, THE BusinessRuleValidator SHALL return a ValidationResult indicating success
5. THE date_range validator SHALL support optional boundaries (only min, only max, or both)

### Requirement 6: Data Quality Validator - Duplicate Detection

**User Story:** As a financial analyst, I want to detect duplicate transactions, so that data quality issues are identified.

#### Acceptance Criteria

1. THE DataQualityValidator SHALL provide a detect_duplicates validator that accepts a list of fields to check for uniqueness
2. WHEN the detect_duplicates validator is applied, THE DataQualityValidator SHALL identify rows with duplicate values across the specified fields
3. IF duplicates are found, THEN THE DataQualityValidator SHALL return a ValidationResult with warnings identifying the duplicate row indices and values
4. WHEN no duplicates are found, THE DataQualityValidator SHALL return a ValidationResult indicating success
5. THE detect_duplicates validator SHALL support exact match and fuzzy match modes for string fields

### Requirement 7: Data Quality Validator - Missing Value Detection

**User Story:** As a financial analyst, I want to detect missing values in optional fields, so that data completeness can be assessed.

#### Acceptance Criteria

1. THE DataQualityValidator SHALL provide a detect_missing validator that accepts a list of fields to check
2. WHEN the detect_missing validator is applied, THE DataQualityValidator SHALL identify rows with null or empty values in the specified fields
3. IF missing values are found, THEN THE DataQualityValidator SHALL return a ValidationResult with warnings identifying the field names and count of missing values
4. WHEN no missing values are found, THE DataQualityValidator SHALL return a ValidationResult indicating success
5. THE detect_missing validator SHALL report the percentage of missing values for each field

### Requirement 8: Data Quality Validator - Outlier Detection

**User Story:** As a financial analyst, I want to detect outlier amounts, so that unusual transactions can be reviewed.

#### Acceptance Criteria

1. THE DataQualityValidator SHALL provide a detect_outliers validator that accepts statistical thresholds (e.g., standard deviations from mean)
2. WHEN the detect_outliers validator is applied, THE DataQualityValidator SHALL identify amounts that fall outside the specified statistical bounds
3. IF outliers are found, THEN THE DataQualityValidator SHALL return a ValidationResult with warnings identifying the row indices and outlier amounts
4. WHEN no outliers are found, THE DataQualityValidator SHALL return a ValidationResult indicating success
5. THE detect_outliers validator SHALL support multiple outlier detection methods (z-score, IQR, percentile-based)

### Requirement 9: Custom Validator Definition

**User Story:** As a developer, I want to define custom validators, so that project-specific validation rules can be implemented.

#### Acceptance Criteria

1. THE Validation_Framework SHALL provide a decorator or base class for creating custom validators
2. WHEN a custom validator is defined, THE developer SHALL implement the validate method accepting an IR DataFrame and returning a ValidationResult
3. THE Validation_Framework SHALL provide helper functions for common validation patterns (row iteration, field access, error formatting)
4. THE custom validator SHALL support configuration parameters passed at initialization
5. THE custom validator SHALL integrate seamlessly with the ValidationPipeline

### Requirement 10: Validation Pipeline Orchestration

**User Story:** As a developer, I want to compose multiple validators into a pipeline, so that all validation rules are applied systematically.

#### Acceptance Criteria

1. THE Validation_Framework SHALL provide a ValidationPipeline class that accepts a list of validators
2. WHEN the ValidationPipeline executes, THE Validation_Framework SHALL apply each validator in sequence to the IR DataFrame
3. WHEN all validators complete, THE Validation_Framework SHALL aggregate all ValidationResults into a single ValidationReport
4. THE ValidationPipeline SHALL support fail-fast mode (stop on first error) and continue mode (run all validators)
5. THE ValidationPipeline SHALL support conditional validators (only run if previous validators passed)
6. FOR ALL ValidationPipelines with no validators, THE Validation_Framework SHALL return a ValidationResult indicating success (identity property)

### Requirement 11: Validation Report Generation

**User Story:** As a financial analyst, I want a comprehensive validation report, so that I can review all validation outcomes in one place.

#### Acceptance Criteria

1. THE Validation_Framework SHALL provide a ValidationReport class that aggregates multiple ValidationResults
2. THE ValidationReport SHALL provide a summary showing total validators run, passed, failed, and warnings
3. THE ValidationReport SHALL provide detailed results for each validator including error messages and affected row indices
4. THE ValidationReport SHALL provide a method to export the report as JSON for programmatic access
5. THE ValidationReport SHALL provide a method to format the report as human-readable text
6. THE ValidationReport SHALL provide a method to filter results by severity (errors only, warnings only, all)

### Requirement 12: Pipeline Integration

**User Story:** As a developer, I want to integrate validation into the existing pipeline, so that validation runs automatically during data processing.

#### Acceptance Criteria

1. THE Validation_Framework SHALL provide a ValidatingTransform that wraps a ValidationPipeline as a Transform
2. WHEN the ValidatingTransform is applied in a pipeline, THE Validation_Framework SHALL run all validators and attach the ValidationReport to the IR metadata
3. IF validation fails in fail-fast mode, THEN THE ValidatingTransform SHALL raise a ValidationError with the ValidationReport
4. IF validation fails in continue mode, THEN THE ValidatingTransform SHALL attach the ValidationReport to the IR and continue processing
5. THE ValidatingTransform SHALL support pre-validation (before transforms) and post-validation (after transforms) modes

### Requirement 13: Declarative Validation Rules

**User Story:** As a developer, I want to define validation rules declaratively, so that validation configuration is readable and maintainable.

#### Acceptance Criteria

1. THE Validation_Framework SHALL support defining validation rules in a declarative format (Python dict or YAML)
2. THE declarative format SHALL support specifying validator type, configuration parameters, and severity level (error or warning)
3. THE Validation_Framework SHALL provide a function to parse declarative rules and construct a ValidationPipeline
4. THE declarative format SHALL support referencing built-in validators and custom validators
5. THE declarative format SHALL support conditional rules (only apply if certain conditions are met)

### Requirement 14: Validation Rule Reusability

**User Story:** As a developer, I want to reuse validation rules across projects, so that common validation patterns are standardized.

#### Acceptance Criteria

1. THE Validation_Framework SHALL provide a ValidationRuleSet class that groups related validators
2. THE ValidationRuleSet SHALL support loading rules from external files (Python modules or YAML)
3. THE ValidationRuleSet SHALL support composing multiple rule sets into a single ValidationPipeline
4. THE Validation_Framework SHALL provide built-in rule sets for common financial validation patterns
5. THE ValidationRuleSet SHALL support versioning and documentation of rule sets

### Requirement 15: Validation Performance

**User Story:** As a developer, I want validation to be performant, so that large datasets can be validated efficiently.

#### Acceptance Criteria

1. THE Validation_Framework SHALL leverage Polars vectorized operations for validation checks where possible
2. THE Validation_Framework SHALL avoid row-by-row iteration unless necessary for complex business rules
3. THE ValidationPipeline SHALL support parallel execution of independent validators
4. THE Validation_Framework SHALL provide performance metrics (validation time per validator) in the ValidationReport
5. FOR ALL validators, validation time SHALL scale linearly with DataFrame size (O(n) complexity)

### Requirement 16: Validation Error Messages

**User Story:** As a financial analyst, I want clear and actionable error messages, so that I can fix validation failures quickly.

#### Acceptance Criteria

1. WHEN a validation error occurs, THE Validation_Framework SHALL include the validator name in the error message
2. WHEN a validation error occurs, THE Validation_Framework SHALL include the affected row indices or row count
3. WHEN a validation error occurs, THE Validation_Framework SHALL include the specific field names and values that failed validation
4. WHEN a validation error occurs, THE Validation_Framework SHALL include a suggestion for how to fix the issue
5. THE error message SHALL be formatted for readability with clear structure and formatting

### Requirement 17: Validation Determinism Property

**User Story:** As a developer, I want validation to be deterministic, so that the same data always produces the same validation results.

#### Acceptance Criteria

1. FOR ALL validators, THE test suite SHALL verify that applying the same validator twice to the same input produces equivalent results
2. THE test suite SHALL use Hypothesis to generate random valid IR DataFrames for determinism testing
3. THE test suite SHALL verify this property holds across multiple invocations
4. THE test suite SHALL verify that validator configuration does not change between invocations

### Requirement 18: Validation Idempotence Property

**User Story:** As a developer, I want validation to be idempotent, so that validation does not modify the input data.

#### Acceptance Criteria

1. FOR ALL validators, THE test suite SHALL verify that the input DataFrame is not modified during validation
2. THE test suite SHALL use Hypothesis to generate random valid IR DataFrames for idempotence testing
3. THE test suite SHALL verify that DataFrame reference equality is preserved (no in-place mutations)
4. THE test suite SHALL verify that DataFrame content equality is preserved (no data changes)

### Requirement 19: Validation Composition Property

**User Story:** As a developer, I want validation composition to be associative, so that validator ordering is predictable.

#### Acceptance Criteria

1. FOR ALL independent validators (validators that check different properties), THE test suite SHALL verify that the order of validators does not affect the final ValidationReport (commutativity property)
2. THE test suite SHALL use Hypothesis to generate random valid IR DataFrames and random validator orderings
3. THE test suite SHALL verify that all errors and warnings are reported regardless of validator order
4. WHERE validators have dependencies, THE test suite SHALL verify that dependent validators run after their prerequisites

### Requirement 20: Validation Error Condition Testing

**User Story:** As a developer, I want comprehensive error condition testing, so that validators handle edge cases correctly.

#### Acceptance Criteria

1. THE test suite SHALL generate IR DataFrames with known validation failures and verify they are detected
2. THE test suite SHALL use Hypothesis to generate edge cases (empty DataFrames, single-row DataFrames, extreme values)
3. THE test suite SHALL verify that validators do not crash on malformed input
4. THE test suite SHALL verify that error messages are descriptive for all failure modes
5. THE test suite SHALL verify that validators handle null values, empty strings, and special numeric values (NaN, infinity) correctly

### Requirement 21: Built-in Financial Validators

**User Story:** As a financial analyst, I want built-in validators for common financial checks, so that I don't have to implement them from scratch.

#### Acceptance Criteria

1. THE Validation_Framework SHALL provide a library of built-in validators for common financial validation patterns
2. THE built-in validators SHALL include: positive_amounts, currency_consistency, date_range, balance_check (debits equal credits), account_code_format, reference_uniqueness
3. THE built-in validators SHALL be documented with usage examples and configuration options
4. THE built-in validators SHALL follow the Validator protocol and integrate with ValidationPipeline
5. THE built-in validators SHALL be tested with property-based tests using Hypothesis

### Requirement 22: Validation Metadata Preservation

**User Story:** As a developer, I want validation results preserved in IR metadata, so that downstream components can access validation history.

#### Acceptance Criteria

1. THE Validation_Framework SHALL attach ValidationReport to IR DataFrame metadata after validation
2. THE metadata SHALL include validation timestamp, validator versions, and configuration used
3. THE Validation_Framework SHALL provide a function to retrieve ValidationReport from IR metadata
4. THE Validation_Framework SHALL support multiple validation runs with metadata history (pre-validation, post-transform validation)
5. THE metadata SHALL be preserved when IR is written to Parquet format

### Requirement 23: Validation Configuration Schema

**User Story:** As a developer, I want a validation configuration schema, so that validation rules are validated before use.

#### Acceptance Criteria

1. THE Validation_Framework SHALL define a schema for declarative validation configuration
2. WHEN validation configuration is loaded, THE Validation_Framework SHALL validate the configuration against the schema
3. IF configuration is invalid, THEN THE Validation_Framework SHALL raise a descriptive error identifying the schema violation
4. THE schema SHALL validate validator names, parameter types, and required fields
5. THE Validation_Framework SHALL provide a function to export the configuration schema for documentation

## Correctness Properties Summary

The following property-based tests must be implemented:

1. **Validator Determinism**: For all validators: `validate(df) = validate(df)` (multiple invocations)
2. **Validator Idempotence**: For all validators: input DataFrame is not modified during validation
3. **Validation Composition**: For independent validators: order does not affect final ValidationReport
4. **Pipeline Identity**: For ValidationPipeline with no validators: returns success result
5. **Error Conditions**: Known validation failures are detected with descriptive messages
6. **Edge Cases**: Validators handle empty DataFrames, single-row DataFrames, and extreme values correctly
7. **Metadata Preservation**: ValidationReport survives round-trip through Parquet format

## Architecture Alignment

The Data Validation Framework extends the core infrastructure with:
- Validation_Framework component (new)
- Validator protocol (extends core validation)
- ValidationPipeline (orchestration layer)
- ValidatingTransform (pipeline integration)
- Built-in validators (library of common checks)

The framework integrates with existing components:
- Core (IR schema and basic validation)
- Transform_Service (pipeline orchestration)
- Reader/Writer protocols (validation at pipeline boundaries)

## Dependencies

- Python 3.13+
- Polars (IR engine and vectorized operations)
- pytest (testing framework)
- Hypothesis (property-based testing)
- pydantic (optional, for configuration schema validation)
- PyYAML (optional, for declarative rule loading)

