# Implementation Plan: Data Validation Framework

## Overview

This implementation plan follows a bottom-up approach, building the validation framework from foundational protocols and data structures up to pipeline orchestration and integration with the existing fintran infrastructure. The framework extends the core IR validation with business rule validators, data quality checks, and comprehensive reporting capabilities.

Implementation strategy:
1. Core protocols and data structures (Validator, ValidationResult)
2. Built-in validators (business rules and data quality)
3. Pipeline orchestration (ValidationPipeline, ValidationReport)
4. Integration layer (ValidatingTransform, metadata handling)
5. Declarative configuration and rule sets
6. Comprehensive property-based testing with Hypothesis

## Tasks

- [x] 1. Set up validation framework module structure
  - Create `fintran/validation/` directory with `__init__.py`
  - Create subdirectories: `business/`, `quality/`
  - Create module files: `protocols.py`, `result.py`, `pipeline.py`, `report.py`, `transform.py`, `custom.py`, `declarative.py`, `exceptions.py`
  - Set up test directory: `tests/validation/` with `conftest.py`
  - _Requirements: All (foundation for entire framework)_

- [-] 2. Implement core validation exceptions
  - [x] 2.1 Create validation exception hierarchy in `fintran/validation/exceptions.py`
    - Define `ValidatorError` base exception
    - Define `ValidatorConfigurationError` for invalid validator configuration
    - Define `ValidatorExecutionError` for validator logic failures
    - Define `ConfigurationSchemaError` for invalid declarative configuration
    - Extend from existing `FintranError` base class
    - _Requirements: 23.3 (error handling)_

- [x] 3. Implement Validator protocol and ValidationResult
  - [x] 3.1 Define Validator protocol in `fintran/validation/protocols.py`
    - Create `Validator` protocol with `validate(df: pl.DataFrame) -> ValidationResult` method
    - Document immutability requirement (must not mutate input DataFrame)
    - Document determinism requirement (same input produces same result)
    - _Requirements: 1.1, 1.2, 1.3, 1.4_
  
  - [x] 3.2 Implement ValidationResult data structure in `fintran/validation/result.py`
    - Create dataclass with fields: `is_valid`, `errors`, `warnings`, `validator_name`, `metadata`
    - Implement `has_errors()` method
    - Implement `has_warnings()` method
    - Implement `format()` method for human-readable output
    - Implement static `combine()` method for aggregating multiple results
    - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5_
  
  - [x] 3.3 Write property test for ValidationResult combination
    - **Property 2: ValidationResult Combination Preserves Information**
    - **Validates: Requirements 2.5**
    - Test that combining ValidationResults preserves all errors and warnings
    - Test that aggregated `is_valid` is false if any individual result has errors
    - _Requirements: 2.5_

- [x] 4. Implement business rule validators
  - [x] 4.1 Implement PositiveAmountsValidator in `fintran/validation/business/amounts.py`
    - Accept `account_patterns` parameter (list of regex patterns)
    - Use Polars boolean masking to filter matching accounts with amounts <= 0
    - Return ValidationResult with errors identifying row indices and account names
    - Support regex pattern matching for accounts
    - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.5_
  
  - [x] 4.2 Write property test for PositiveAmountsValidator
    - **Property 3: Positive Amounts Validation Detects Violations**
    - **Validates: Requirements 3.2, 3.3, 3.4**
    - Generate IR DataFrames with known violations (negative/zero amounts)
    - Verify validator detects violations and returns appropriate errors
    - Verify validator passes when all amounts are positive
    - _Requirements: 3.2, 3.3, 3.4, 20.1_
  
  - [x] 4.3 Implement CurrencyConsistencyValidator in `fintran/validation/business/currency.py`
    - Accept `group_by` parameter (list of fields, default: ["account"])
    - Use Polars group_by and aggregation to count distinct currencies per group
    - Identify groups with multiple currencies
    - Return ValidationResult with errors identifying account groups and conflicting currencies
    - Support whole-DataFrame validation when no grouping specified
    - _Requirements: 4.1, 4.2, 4.3, 4.4, 4.5_
  
  - [x] 4.4 Write property test for CurrencyConsistencyValidator
    - **Property 4: Currency Consistency Validation**
    - **Validates: Requirements 4.2, 4.3, 4.4, 4.5**
    - Generate IR DataFrames with mixed currencies within groups
    - Verify validator detects inconsistencies
    - Verify validator passes when currency is consistent
    - _Requirements: 4.2, 4.3, 4.4, 4.5, 20.1_
  
  - [x] 4.5 Implement DateRangeValidator in `fintran/validation/business/dates.py`
    - Accept `min_date` and `max_date` parameters (both optional)
    - Use Polars boolean expressions to identify out-of-range dates
    - Return ValidationResult with errors identifying row indices and dates
    - Support optional boundaries (only min, only max, or both)
    - _Requirements: 5.1, 5.2, 5.3, 5.4, 5.5_
  
  - [x] 4.6 Write property test for DateRangeValidator
    - **Property 5: Date Range Validation**
    - **Validates: Requirements 5.2, 5.3, 5.4, 5.5**
    - Generate IR DataFrames with dates outside specified ranges
    - Verify validator detects out-of-range dates
    - Test with min-only, max-only, and both boundaries
    - _Requirements: 5.2, 5.3, 5.4, 5.5, 20.1_

- [x] 5. Implement data quality validators
  - [x] 5.1 Implement DuplicateDetectionValidator in `fintran/validation/quality/duplicates.py`
    - Accept `fields` parameter (list of fields to check)
    - Accept `mode` parameter ("exact" or "fuzzy")
    - Use Polars `is_duplicated()` for exact matching
    - Return ValidationResult with warnings (not errors) identifying duplicate row indices
    - _Requirements: 6.1, 6.2, 6.3, 6.4, 6.5_
  
  - [x] 5.2 Write property tests for DuplicateDetectionValidator
    - **Property 6: Duplicate Detection**
    - **Validates: Requirements 6.2, 6.3, 6.4**
    - Generate IR DataFrames with known duplicates
    - Verify validator detects duplicates and returns warnings
    - **Property 7: Duplicate Detection Mode Consistency**
    - **Validates: Requirements 6.5**
    - Verify fuzzy mode detects all exact duplicates (fuzzy is superset of exact)
    - _Requirements: 6.2, 6.3, 6.4, 6.5, 20.1_
  
  - [x] 5.3 Implement MissingValueDetectionValidator in `fintran/validation/quality/missing.py`
    - Accept `fields` parameter (list of fields to check)
    - Use Polars `null_count()` to count missing values per field
    - Calculate percentage of missing values
    - Return ValidationResult with warnings including field names, counts, and percentages
    - _Requirements: 7.1, 7.2, 7.3, 7.4, 7.5_
  
  - [x] 5.4 Write property test for MissingValueDetectionValidator
    - **Property 8: Missing Value Detection**
    - **Validates: Requirements 7.2, 7.3, 7.4, 7.5**
    - Generate IR DataFrames with null/empty values
    - Verify validator detects missing values and reports counts/percentages
    - _Requirements: 7.2, 7.3, 7.4, 7.5, 20.1_
  
  - [x] 5.5 Implement OutlierDetectionValidator in `fintran/validation/quality/outliers.py`
    - Accept `method` parameter ("zscore", "iqr", or "percentile")
    - Accept `threshold` parameter (method-specific threshold)
    - Use Polars statistical functions (mean, std, quantile)
    - Implement z-score, IQR, and percentile-based outlier detection
    - Return ValidationResult with warnings identifying row indices and outlier amounts
    - _Requirements: 8.1, 8.2, 8.3, 8.4, 8.5_
  
  - [x] 5.6 Write property tests for OutlierDetectionValidator
    - **Property 9: Outlier Detection**
    - **Validates: Requirements 8.2, 8.3, 8.4**
    - Generate IR DataFrames with extreme outliers
    - Verify validator detects outliers using each method
    - **Property 10: Outlier Detection Method Consistency**
    - **Validates: Requirements 8.5**
    - Verify all methods detect extreme outliers (10+ std devs)
    - _Requirements: 8.2, 8.3, 8.4, 8.5, 20.1_

- [x] 6. Checkpoint - Verify validator implementations
  - Run all validator tests to ensure core validators work correctly
  - Verify Polars vectorized operations are used (no row-by-row iteration)
  - Ensure all tests pass, ask the user if questions arise

- [ ] 7. Implement ValidationPipeline orchestration
  - [ ] 7.1 Create ValidationMode enum and ValidationPipeline class in `fintran/validation/pipeline.py`
    - Define `ValidationMode` enum with FAIL_FAST and CONTINUE modes
    - Implement `ValidationPipeline.__init__()` accepting validators and mode
    - Implement `ValidationPipeline.run()` to execute validators in sequence
    - Support fail-fast mode (stop on first error)
    - Support continue mode (run all validators)
    - Aggregate results into ValidationReport
    - _Requirements: 10.1, 10.2, 10.3, 10.4, 10.5_
  
  - [ ]* 7.2 Write property tests for ValidationPipeline
    - **Property 11: ValidationPipeline Execution Order**
    - **Validates: Requirements 10.2, 10.3**
    - Verify validators execute in order and results are in same order
    - **Property 12: ValidationPipeline Fail-Fast Mode**
    - **Validates: Requirements 10.4**
    - Verify fail-fast stops on first error
    - **Property 13: ValidationPipeline Continue Mode**
    - **Validates: Requirements 10.4, 12.4**
    - Verify continue mode runs all validators
    - **Property 14: ValidationPipeline Identity**
    - **Validates: Requirements 10.6**
    - Verify empty pipeline returns success
    - _Requirements: 10.2, 10.3, 10.4, 10.6_

- [ ] 8. Implement ValidationReport
  - [ ] 8.1 Create ValidationReport dataclass in `fintran/validation/report.py`
    - Define dataclass with fields: `results`, `timestamp`, `total_validators`, `passed`, `failed`, `warnings_count`
    - Implement `is_valid()` method
    - Implement `summary()` method for summary string
    - Implement `to_json()` method for JSON export
    - Implement `format()` method with severity filtering
    - _Requirements: 11.1, 11.2, 11.3, 11.4, 11.5, 11.6_
  
  - [ ]* 8.2 Write property tests for ValidationReport
    - **Property 15: ValidationReport Summary Accuracy**
    - **Validates: Requirements 11.2**
    - Verify summary counts accurately reflect aggregated results
    - **Property 16: ValidationReport JSON Round-Trip**
    - **Validates: Requirements 11.4**
    - Verify JSON export and reconstruction produces equivalent report
    - **Property 17: ValidationReport Filtering**
    - **Validates: Requirements 11.6**
    - Verify filtering by severity returns correct results
    - _Requirements: 11.2, 11.4, 11.6_

- [ ] 9. Implement ValidatingTransform for pipeline integration
  - [ ] 9.1 Create ValidatingTransform class in `fintran/validation/transform.py`
    - Implement `__init__()` accepting ValidationPipeline, fail_on_error flag, and metadata_key
    - Implement `transform()` method that runs validation and attaches report to IR metadata
    - Support fail-on-error mode (raise ValidationError on failures)
    - Support continue mode (attach report and continue)
    - Implement metadata attachment using Polars DataFrame metadata
    - _Requirements: 12.1, 12.2, 12.3, 12.4, 12.5_
  
  - [ ] 9.2 Implement metadata helper functions
    - Create `attach_validation_report()` function to store report in IR metadata
    - Create `get_validation_reports()` function to retrieve reports from IR metadata
    - Support multiple validation runs with history (pre-validation, post-validation)
    - Include timestamp, validator versions, and configuration in metadata
    - _Requirements: 22.1, 22.2, 22.3, 22.4_
  
  - [ ]* 9.3 Write property tests for ValidatingTransform
    - **Property 18: ValidatingTransform Metadata Attachment**
    - **Validates: Requirements 12.2, 22.1, 22.3**
    - Verify ValidationReport is attached to IR metadata and retrievable
    - **Property 19: ValidatingTransform Fail-Fast Error Handling**
    - **Validates: Requirements 12.3**
    - Verify fail_on_error=True raises ValidationError with report
    - _Requirements: 12.2, 12.3, 22.1, 22.3_

- [ ] 10. Implement custom validator support
  - [ ] 10.1 Create custom validator helpers in `fintran/validation/custom.py`
    - Implement `@custom_validator` decorator for creating custom validators
    - Provide helper functions for common validation patterns
    - Provide helper functions for error formatting
    - Document custom validator pattern with examples
    - _Requirements: 9.1, 9.2, 9.3, 9.4, 9.5_

- [ ] 11. Implement declarative configuration support
  - [ ] 11.1 Create declarative configuration parser in `fintran/validation/declarative.py`
    - Implement `load_validation_config()` function for loading dict/YAML configs
    - Define configuration schema (validator type, params, severity)
    - Implement `parse_config()` to construct ValidationPipeline from config
    - Support referencing built-in and custom validators
    - Support conditional rules
    - _Requirements: 13.1, 13.2, 13.3, 13.4, 13.5_
  
  - [ ] 11.2 Implement configuration schema validation
    - Define schema for declarative configuration
    - Validate configuration against schema on load
    - Raise ConfigurationSchemaError with descriptive messages for violations
    - Validate validator names, parameter types, and required fields
    - Provide function to export schema for documentation
    - _Requirements: 23.1, 23.2, 23.3, 23.4, 23.5_
  
  - [ ]* 11.3 Write property tests for declarative configuration
    - **Property 20: Declarative Configuration Parsing**
    - **Validates: Requirements 13.3**
    - Verify parsed pipeline behaves equivalently to manually constructed pipeline
    - **Property 28: Configuration Schema Validation**
    - **Validates: Requirements 23.2, 23.3**
    - Verify invalid configurations raise descriptive errors
    - _Requirements: 13.3, 23.2, 23.3_

- [ ] 12. Implement ValidationRuleSet for reusability
  - [ ] 12.1 Create ValidationRuleSet class in `fintran/validation/declarative.py`
    - Implement class for grouping related validators
    - Support loading rules from external files (Python modules or YAML)
    - Support composing multiple rule sets into single ValidationPipeline
    - Support versioning and documentation of rule sets
    - _Requirements: 14.1, 14.2, 14.3, 14.4, 14.5_
  
  - [ ] 12.2 Create built-in financial validation rule sets
    - Create rule set for common financial validation patterns
    - Include: positive_amounts, currency_consistency, date_range, balance_check, account_code_format, reference_uniqueness
    - Document each built-in validator with usage examples
    - _Requirements: 21.1, 21.2, 21.3, 21.4_
  
  - [ ]* 12.3 Write property test for ValidationRuleSet composition
    - **Property 21: ValidationRuleSet Composition**
    - **Validates: Requirements 14.3**
    - Verify composing rule sets applies all validators in order
    - _Requirements: 14.3_

- [ ] 13. Checkpoint - Verify pipeline and integration
  - Test full pipeline integration with ValidatingTransform
  - Verify metadata attachment and retrieval works correctly
  - Test declarative configuration loading and parsing
  - Ensure all tests pass, ask the user if questions arise

- [ ] 14. Implement comprehensive property-based tests
  - [ ] 14.1 Set up Hypothesis strategies in `tests/validation/conftest.py`
    - Create `valid_ir_dataframe()` strategy for generating random valid IR DataFrames
    - Create `ir_with_violations()` strategy for generating DataFrames with known violations
    - Create `validator_config()` strategy for generating random validator configurations
    - Configure Hypothesis settings (max_examples=100, deadline=None)
    - _Requirements: 17.2, 20.2_
  
  - [ ]* 14.2 Write validator determinism and immutability tests in `tests/validation/test_validator_properties.py`
    - **Property 1: Validator Determinism**
    - **Validates: Requirements 1.5, 17.1**
    - Test that applying same validator twice produces equivalent results
    - **Property 24: Validator Immutability**
    - **Validates: Requirements 18.1**
    - Test that validators don't modify input DataFrame (reference and content equality)
    - _Requirements: 1.5, 17.1, 17.2, 17.3, 17.4, 18.1, 18.2, 18.3, 18.4_
  
  - [ ]* 14.3 Write error message property tests in `tests/validation/test_error_properties.py`
    - **Property 23: Error Message Completeness**
    - **Validates: Requirements 16.1, 16.2, 16.3**
    - Verify error messages contain validator name, row indices, field names, and values
    - **Property 26: Known Failure Detection**
    - **Validates: Requirements 20.1**
    - Verify validators detect known failures with appropriate errors
    - _Requirements: 16.1, 16.2, 16.3, 16.4, 16.5, 20.1, 20.2, 20.3, 20.4, 20.5_
  
  - [ ]* 14.4 Write validator composition property test in `tests/validation/test_composition_properties.py`
    - **Property 25: Validator Composition Commutativity**
    - **Validates: Requirements 19.1**
    - Test that independent validators produce same errors/warnings regardless of order
    - _Requirements: 19.1, 19.2, 19.3, 19.4_
  
  - [ ]* 14.5 Write metadata preservation property test in `tests/validation/test_integration_properties.py`
    - **Property 27: Metadata Preservation Through Parquet**
    - **Validates: Requirements 22.2, 22.4, 22.5**
    - Test that validation metadata survives Parquet round-trip
    - _Requirements: 22.2, 22.4, 22.5_
  
  - [ ]* 14.6 Write performance property test in `tests/validation/test_performance_properties.py`
    - **Property 22: Validation Performance Linearity**
    - **Validates: Requirements 15.5**
    - Test that validation time scales linearly with DataFrame size (O(n) complexity)
    - _Requirements: 15.1, 15.2, 15.3, 15.4, 15.5_

- [ ] 15. Write edge case tests
  - [ ]* 15.1 Create edge case test suite in `tests/validation/test_edge_cases.py`
    - Test validators with empty DataFrames
    - Test validators with single-row DataFrames
    - Test validators with null values, empty strings, NaN, infinity
    - Test validators with extreme values (very large/small numbers, far future/past dates)
    - Verify validators don't crash on malformed input
    - Verify error messages are descriptive for all failure modes
    - _Requirements: 20.2, 20.3, 20.4, 20.5_

- [ ] 16. Update package exports and documentation
  - [ ] 16.1 Update `fintran/validation/__init__.py` with public API exports
    - Export Validator protocol
    - Export ValidationResult, ValidationReport
    - Export ValidationPipeline, ValidationMode
    - Export ValidatingTransform
    - Export all built-in validators
    - Export custom validator helpers
    - Export declarative configuration functions
  
  - [ ] 16.2 Update `fintran/__init__.py` to include validation module
    - Add validation module to package exports
  
  - [ ] 16.3 Add docstrings and type hints to all public APIs
    - Ensure all classes, functions, and methods have comprehensive docstrings
    - Add usage examples to key components
    - Verify type hints are complete and accurate

- [ ] 17. Final checkpoint - Complete validation framework
  - Run full test suite with pytest
  - Verify all 28 property-based tests pass
  - Verify code coverage meets 90%+ target
  - Run performance tests to verify O(n) complexity
  - Ensure all tests pass, ask the user if questions arise

## Notes

- Tasks marked with `*` are optional property-based testing tasks and can be skipped for faster MVP
- Each task references specific requirements for traceability
- Implementation follows bottom-up approach: protocols → validators → pipeline → integration
- All validators use Polars vectorized operations for performance (no row-by-row iteration)
- Property-based tests use Hypothesis with minimum 100 iterations per test
- Each property test explicitly references its design document property number
- Checkpoints ensure incremental validation at key milestones
- The framework integrates seamlessly with existing fintran pipeline via ValidatingTransform
- Metadata preservation through Parquet ensures validation history is maintained
