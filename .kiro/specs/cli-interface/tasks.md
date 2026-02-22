# Implementation Plan: CLI Interface

## Overview

This plan implements a command-line interface for the fintran financial document transformation pipeline using the Cyclopts framework. The implementation follows a bottom-up approach: infrastructure components first (exit codes, registry, output formatting), then core command implementations, and finally integration and testing. All 25 correctness properties from the design will be validated through property-based tests using Hypothesis.

## Tasks

- [x] 1. Set up CLI infrastructure and core modules
  - [x] 1.1 Create CLI module structure and exit codes
    - Create `fintran/cli/__init__.py`
    - Create `fintran/cli/exit_codes.py` with ExitCode class defining all exit codes (0-6)
    - _Requirements: 9.1, 9.2, 9.3, 9.4, 9.5, 9.6, 9.7_
  
  - [x] 1.2 Write property test for exit code constants
    - **Property 2: Exit Code Mapping**
    - **Validates: Requirements 9.1-9.7**
  
  - [x] 1.3 Implement output formatting module
    - Create `fintran/cli/output.py` with ProgressIndicator class
    - Implement TTY detection for progress indicators
    - Implement handle_error function with context formatting
    - _Requirements: 8.1, 8.2, 8.3, 8.4, 8.5, 7.6, 7.7_
  
  - [x] 1.4 Write property tests for output module
    - **Property 4: Stream Separation**
    - **Property 15: Progress Indicator Visibility**
    - **Property 16: Quiet Mode Suppression**
    - **Validates: Requirements 7.6, 7.7, 8.1-8.5**

- [x] 2. Implement component registry system
  - [x] 2.1 Create registry module with registration functions
    - Create `fintran/cli/registry.py`
    - Implement READERS, WRITERS, TRANSFORMS dictionaries
    - Implement register_reader, register_writer, register_transform functions
    - Implement get_reader, get_writer, get_transform functions with error handling
    - Implement list_readers, list_writers, list_transforms functions
    - _Requirements: 10.1, 10.2, 10.3, 10.4, 10.5, 10.6, 14.5, 14.6, 14.7_
  
  - [x] 2.2 Write unit tests for registry functions
    - Test registration and retrieval
    - Test error handling for unknown component types
    - Test list functions return correct format
    - _Requirements: 10.1-10.6, 14.5-14.7_
  
  - [x] 2.3 Write property test for invalid component handling
    - **Property 23: Invalid Component Type Handling**
    - **Validates: Requirements 14.5, 14.6, 14.7**

- [x] 3. Implement configuration file support
  - [x] 3.1 Create configuration module with loading functions
    - Create `fintran/cli/config.py`
    - Implement ConfigError exception class
    - Implement load_config function supporting JSON and YAML
    - Implement merge_config function for CLI argument precedence
    - Implement validate_config function for component existence checks
    - Add pyyaml dependency to pyproject.toml
    - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 11.2, 11.5, 11.6_
  
  - [x] 3.2 Write property tests for configuration handling
    - **Property 5: Configuration Round Trip**
    - **Property 6: Configuration Loading**
    - **Property 7: CLI Argument Precedence**
    - **Property 8: Invalid Configuration Detection**
    - **Validates: Requirements 3.1-3.6, 11.2, 11.5, 11.6**
  
  - [x] 3.3 Write unit tests for configuration edge cases
    - Test missing file handling
    - Test malformed JSON/YAML handling
    - Test auto-detection of format
    - _Requirements: 3.5, 3.6_

- [x] 4. Checkpoint - Verify infrastructure components
  - Ensure all tests pass, ask the user if questions arise.

- [x] 5. Implement core CLI commands
  - [x] 5.1 Create Cyclopts application and command routing
    - Create `fintran/cli/app.py`
    - Define main Cyclopts App with name, help, version
    - Register all subcommands (convert, validate, inspect, batch, list-*, check-config)
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7_
  
  - [x] 5.2 Implement convert command
    - Create `fintran/cli/commands.py`
    - Implement convert function with all parameters (input_path, output_path, reader, writer, transform, config, dry_run, quiet, verbose, log_level, log_file)
    - Implement file extension inference for reader/writer types
    - Implement config loading and merging
    - Implement pipeline execution with error handling
    - Implement dry-run mode
    - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5, 2.6, 2.7, 2.8, 12.1, 12.2, 12.3, 12.4, 12.5, 15.1, 15.2_
  
  - [x] 5.3 Write property tests for convert command
    - **Property 1: Pipeline Integration**
    - **Property 9: File Extension Inference**
    - **Property 18: Dry Run Behavior**
    - **Property 21: Input Validation**
    - **Property 22: Output Directory Creation**
    - **Validates: Requirements 2.1-2.8, 12.1-12.5, 14.2, 14.3, 14.4, 15.1, 15.2**
  
  - [x] 5.4 Implement validate command
    - Implement validate function with parameters (input_path, reader, verbose)
    - Implement IR validation using existing validation service
    - Implement verbose mode with schema information display
    - _Requirements: 4.1, 4.2, 4.3, 4.4, 4.5, 4.6, 15.4_
  
  - [x] 5.5 Write property tests for validate command
    - **Property 10: Validation Error Display**
    - **Validates: Requirements 4.4, 7.2**
  
  - [x] 5.6 Implement inspect command
    - Implement inspect function with parameters (input_path, reader, metadata, sample, stats)
    - Implement schema display (columns, types, row count)
    - Implement sample rows display
    - Implement statistics display
    - _Requirements: 5.1, 5.2, 5.3, 5.4, 5.5, 5.6, 5.7_
  
  - [x] 5.7 Write property tests for inspect command
    - **Property 11: Inspect Output Completeness**
    - **Validates: Requirements 5.1, 5.2**

- [x] 6. Implement batch processing command
  - [x] 6.1 Implement batch command
    - Implement batch function with parameters (input_dir, output_dir, pattern, recursive, reader, writer, transform, config, quiet)
    - Implement file pattern matching (glob and rglob)
    - Implement output directory creation with relative path preservation
    - Implement per-file processing with error isolation
    - Implement summary display (total, success, failed)
    - _Requirements: 6.1, 6.2, 6.3, 6.4, 6.5, 6.6, 6.7, 6.8, 6.9_
  
  - [x] 6.2 Write property tests for batch processing
    - **Property 12: Batch Processing Completeness**
    - **Property 13: Batch Pattern Filtering**
    - **Property 14: Batch Error Isolation**
    - **Validates: Requirements 6.1, 6.2, 6.7, 6.8, 6.9**

- [x] 7. Implement utility commands
  - [x] 7.1 Implement list commands
    - Implement list_readers function
    - Implement list_writers function
    - Implement list_transforms function
    - Format output as readable list with descriptions
    - _Requirements: 10.1, 10.2, 10.3, 10.4, 10.5, 10.6_
  
  - [x] 7.2 Implement check-config command
    - Implement check_config function with parameter (config_path)
    - Implement configuration validation using validate_config
    - Display validation errors with specific details
    - _Requirements: 11.1, 11.2, 11.3, 11.4, 11.5, 11.6_
  
  - [x] 7.3 Write property test for configuration validation
    - **Property 17: Configuration Validation**
    - **Validates: Requirements 11.2, 11.5, 11.6**

- [x] 8. Checkpoint - Verify all commands implemented
  - Ensure all tests pass, ask the user if questions arise.

- [x] 9. Implement error handling and logging
  - [x] 9.1 Enhance error handling in all commands
    - Add try-except blocks for all FintranError subclasses
    - Map exceptions to appropriate exit codes
    - Ensure error context preservation
    - Implement verbose mode stack trace display
    - _Requirements: 7.1, 7.2, 7.3, 7.4, 7.5, 7.8, 15.3, 15.5_
  
  - [x] 9.2 Write property tests for error handling
    - **Property 2: Exit Code Mapping**
    - **Property 3: Error Context Preservation**
    - **Property 24: Exception Propagation**
    - **Validates: Requirements 7.1-7.5, 7.8, 9.1-9.7, 15.3, 15.5**
  
  - [x] 9.3 Implement logging configuration
    - Create configure_logging function in commands.py or separate module
    - Implement log level mapping (debug, info, warning, error)
    - Implement log file handler setup
    - Integrate logging configuration into all commands
    - _Requirements: 13.1, 13.2, 13.3, 13.4, 13.5, 13.6_
  
  - [x] 9.4 Write property tests for logging configuration
    - **Property 19: Log Level Configuration**
    - **Property 20: Log File Output**
    - **Validates: Requirements 13.2, 13.4, 13.5**

- [x] 10. Create CLI entry point
  - [x] 10.1 Create __main__.py entry point
    - Create `fintran/__main__.py`
    - Import app from fintran.cli.app
    - Call app() and exit with returned code
    - _Requirements: 1.5, 1.6, 1.7_
  
  - [x] 10.2 Write integration tests for CLI entry point
    - Test invocation via `python -m fintran`
    - Test --help flag
    - Test --version flag
    - Test command routing
    - _Requirements: 1.5, 1.6, 1.7_

- [x] 11. Implement configuration parameter passing
  - [x] 11.1 Enhance pipeline integration for config passing
    - Ensure reader_config, writer_config, and pipeline_config are extracted from merged config
    - Pass config parameters to execute_pipeline
    - Pass config parameters to reader and writer instances
    - _Requirements: 15.6_
  
  - [x] 11.2 Write property test for configuration parameter passing
    - **Property 25: Configuration Parameter Passing**
    - **Validates: Requirements 15.6**

- [x] 12. Final integration and comprehensive testing
  - [x] 12.1 Write end-to-end integration tests
    - Test complete convert workflow (file → file)
    - Test complete batch workflow (directory → directory)
    - Test config file integration with all commands
    - Test error scenarios across all commands
    - _Requirements: All requirements_
  
  - [x] 12.2 Write remaining property tests
    - Ensure all 25 properties have corresponding tests
    - Verify property test tags reference correct design properties
    - Run full property test suite with Hypothesis
  
  - [x] 12.3 Update documentation
    - Add CLI usage examples to README or docs
    - Document exit codes in help text
    - Document configuration file format
    - Add examples for common workflows

- [x] 13. Final checkpoint - Complete verification
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- Tasks marked with `*` are optional and can be skipped for faster MVP
- Each task references specific requirements for traceability
- Property tests validate universal correctness properties from the design
- Unit tests validate specific examples and edge cases
- The implementation uses Python 3.13+ with Cyclopts and PyYAML
- All CLI code integrates with existing fintran pipeline infrastructure
- Bottom-up approach ensures infrastructure is solid before building commands
