# Requirements Document: CLI Interface

## Introduction

This document specifies requirements for a command-line interface (CLI) for the fintran financial document transformation tool. The CLI will provide users with command-line access to the existing reader → IR → writer pipeline, enabling file format conversions, data validation, IR inspection, and batch processing workflows. The CLI will integrate with the existing pipeline orchestration, validation framework, and error handling infrastructure while following CLI best practices for usability, error reporting, and scriptability.

## Glossary

- **CLI**: The command-line interface application that users interact with via terminal
- **Pipeline**: The existing fintran orchestration service that executes reader → transform → writer flows
- **IR**: Intermediate Representation - the Polars DataFrame format used internally by fintran
- **Reader**: A component that parses input files into IR format
- **Writer**: A component that serializes IR to output file formats
- **Transform**: A component that modifies IR data between read and write operations
- **Cyclopts**: The CLI framework library used to build the command-line interface
- **Config_File**: A file containing CLI configuration (reader/writer/transform settings)
- **Exit_Code**: The numeric status code returned by the CLI process (0 for success, non-zero for errors)
- **Validation_Service**: The existing fintran service that validates IR schema compliance

## Requirements

### Requirement 1: Command Structure

**User Story:** As a user, I want intuitive commands and subcommands, so that I can easily discover and use CLI functionality.

#### Acceptance Criteria

1. THE CLI SHALL provide a `convert` subcommand for file format transformations
2. THE CLI SHALL provide a `validate` subcommand for IR schema validation
3. THE CLI SHALL provide an `inspect` subcommand for viewing IR structure and metadata
4. THE CLI SHALL provide a `batch` subcommand for processing multiple files
5. WHEN invoked without arguments, THE CLI SHALL display usage information
6. WHEN invoked with `--help`, THE CLI SHALL display detailed command documentation
7. WHEN invoked with `--version`, THE CLI SHALL display the fintran version number

### Requirement 2: Convert Command

**User Story:** As a user, I want to convert files between formats, so that I can transform financial data using the pipeline.

#### Acceptance Criteria

1. WHEN the `convert` command is invoked with input and output paths, THE CLI SHALL execute the pipeline with appropriate reader and writer
2. THE CLI SHALL accept `--reader` argument to specify the reader type (sql, csv, json)
3. THE CLI SHALL accept `--writer` argument to specify the writer type (parquet, json, csv)
4. THE CLI SHALL accept `--transform` argument (repeatable) to specify transforms to apply
5. THE CLI SHALL accept `--config` argument to load configuration from a Config_File
6. WHEN reader or writer type is not specified, THE CLI SHALL infer types from file extensions
7. WHEN the pipeline completes successfully, THE CLI SHALL return Exit_Code 0
8. WHEN the pipeline fails, THE CLI SHALL return a non-zero Exit_Code

### Requirement 3: Configuration File Support

**User Story:** As a user, I want to specify complex configurations in files, so that I can reuse pipeline configurations without repeating CLI arguments.

#### Acceptance Criteria

1. THE CLI SHALL support JSON format Config_Files
2. THE CLI SHALL support YAML format Config_Files
3. WHEN a Config_File is provided, THE CLI SHALL load reader, writer, and transform configurations from the file
4. WHEN both Config_File and CLI arguments are provided, THE CLI SHALL override Config_File settings with CLI arguments
5. WHEN a Config_File path is invalid, THE CLI SHALL display an error message and return a non-zero Exit_Code
6. WHEN a Config_File contains invalid syntax, THE CLI SHALL display a descriptive error message and return a non-zero Exit_Code

### Requirement 4: Validate Command

**User Story:** As a user, I want to validate data files, so that I can verify they conform to the IR schema without performing a full conversion.

#### Acceptance Criteria

1. WHEN the `validate` command is invoked with an input path, THE CLI SHALL read the file and validate it against the IR schema
2. THE CLI SHALL accept `--reader` argument to specify the reader type
3. WHEN validation succeeds, THE CLI SHALL display a success message and return Exit_Code 0
4. WHEN validation fails, THE CLI SHALL display validation errors with field names and constraint violations
5. THE CLI SHALL accept `--verbose` flag to display detailed schema information
6. WHEN the input file cannot be read, THE CLI SHALL display an error message and return a non-zero Exit_Code

### Requirement 5: Inspect Command

**User Story:** As a user, I want to inspect IR structure and metadata, so that I can understand the data schema and contents without writing code.

#### Acceptance Criteria

1. WHEN the `inspect` command is invoked with an input path, THE CLI SHALL read the file and display IR schema information
2. THE CLI SHALL display column names, data types, and row count
3. THE CLI SHALL accept `--metadata` flag to display embedded metadata
4. THE CLI SHALL accept `--sample` argument to display the first N rows of data
5. THE CLI SHALL accept `--stats` flag to display statistical summaries (min, max, mean for numeric columns)
6. THE CLI SHALL format output in a human-readable table format
7. WHEN the input file cannot be read, THE CLI SHALL display an error message and return a non-zero Exit_Code

### Requirement 6: Batch Processing

**User Story:** As a user, I want to process multiple files in one command, so that I can efficiently transform large numbers of files.

#### Acceptance Criteria

1. WHEN the `batch` command is invoked with an input directory, THE CLI SHALL process all matching files in the directory
2. THE CLI SHALL accept `--pattern` argument to filter files by glob pattern
3. THE CLI SHALL accept `--output-dir` argument to specify the output directory
4. THE CLI SHALL accept `--recursive` flag to process subdirectories
5. THE CLI SHALL process files in parallel when possible
6. WHEN processing each file, THE CLI SHALL display progress information (current file, N of M completed)
7. WHEN a file fails, THE CLI SHALL log the error and continue processing remaining files
8. WHEN all files complete, THE CLI SHALL display a summary (total files, successful, failed)
9. WHEN any file fails, THE CLI SHALL return a non-zero Exit_Code

### Requirement 7: Error Handling and Messages

**User Story:** As a user, I want clear error messages, so that I can understand and fix problems quickly.

#### Acceptance Criteria

1. WHEN a pipeline error occurs, THE CLI SHALL display the error message from the Pipeline exception
2. WHEN a validation error occurs, THE CLI SHALL display field names and constraint violations
3. WHEN a reader error occurs, THE CLI SHALL display the input path and error details
4. WHEN a writer error occurs, THE CLI SHALL display the output path and error details
5. WHEN a transform error occurs, THE CLI SHALL display the transform name and error details
6. THE CLI SHALL write error messages to stderr
7. THE CLI SHALL write normal output to stdout
8. WHEN `--verbose` flag is provided, THE CLI SHALL display stack traces for errors

### Requirement 8: Progress Indicators

**User Story:** As a user, I want to see progress during long operations, so that I know the CLI is working and can estimate completion time.

#### Acceptance Criteria

1. WHEN processing a file, THE CLI SHALL display a progress indicator
2. WHEN processing multiple files, THE CLI SHALL display overall progress (N of M files completed)
3. THE CLI SHALL accept `--quiet` flag to suppress progress output
4. WHEN `--quiet` is specified, THE CLI SHALL only output final results and errors
5. WHEN output is redirected to a file, THE CLI SHALL disable progress indicators automatically

### Requirement 9: Exit Codes

**User Story:** As a script author, I want consistent exit codes, so that I can handle success and failure cases in automated workflows.

#### Acceptance Criteria

1. WHEN a command completes successfully, THE CLI SHALL return Exit_Code 0
2. WHEN a validation error occurs, THE CLI SHALL return Exit_Code 2
3. WHEN a reader error occurs, THE CLI SHALL return Exit_Code 3
4. WHEN a writer error occurs, THE CLI SHALL return Exit_Code 4
5. WHEN a transform error occurs, THE CLI SHALL return Exit_Code 5
6. WHEN a configuration error occurs, THE CLI SHALL return Exit_Code 6
7. WHEN an unexpected error occurs, THE CLI SHALL return Exit_Code 1
8. THE CLI SHALL document exit codes in help text

### Requirement 10: Reader and Writer Discovery

**User Story:** As a user, I want to see available readers and writers, so that I can discover what formats are supported.

#### Acceptance Criteria

1. THE CLI SHALL provide a `list-readers` subcommand
2. WHEN `list-readers` is invoked, THE CLI SHALL display all available reader types with descriptions
3. THE CLI SHALL provide a `list-writers` subcommand
4. WHEN `list-writers` is invoked, THE CLI SHALL display all available writer types with descriptions
5. THE CLI SHALL provide a `list-transforms` subcommand
6. WHEN `list-transforms` is invoked, THE CLI SHALL display all available transform types with descriptions

### Requirement 11: Configuration Validation

**User Story:** As a user, I want to validate configuration files before running pipelines, so that I can catch configuration errors early.

#### Acceptance Criteria

1. THE CLI SHALL provide a `check-config` subcommand
2. WHEN `check-config` is invoked with a Config_File path, THE CLI SHALL validate the configuration syntax and structure
3. WHEN configuration is valid, THE CLI SHALL display a success message and return Exit_Code 0
4. WHEN configuration is invalid, THE CLI SHALL display specific validation errors and return Exit_Code 6
5. THE CLI SHALL verify that referenced readers, writers, and transforms exist
6. THE CLI SHALL verify that required configuration parameters are present

### Requirement 12: Dry Run Mode

**User Story:** As a user, I want to preview pipeline execution without writing files, so that I can verify my configuration is correct.

#### Acceptance Criteria

1. THE CLI SHALL accept `--dry-run` flag for the `convert` command
2. WHEN `--dry-run` is specified, THE CLI SHALL execute the pipeline through validation but skip the write step
3. WHEN `--dry-run` is specified, THE CLI SHALL display what would be written (row count, schema, output path)
4. WHEN `--dry-run` is specified and validation succeeds, THE CLI SHALL return Exit_Code 0
5. WHEN `--dry-run` is specified and validation fails, THE CLI SHALL display errors and return a non-zero Exit_Code

### Requirement 13: Logging Configuration

**User Story:** As a user, I want to control logging verbosity, so that I can get appropriate detail for my use case.

#### Acceptance Criteria

1. THE CLI SHALL accept `--log-level` argument with values (debug, info, warning, error)
2. WHEN `--log-level` is specified, THE CLI SHALL configure Python logging to the specified level
3. THE CLI SHALL accept `--log-file` argument to write logs to a file
4. WHEN `--log-file` is specified, THE CLI SHALL write logs to the specified path
5. WHEN logging to a file, THE CLI SHALL include timestamps and log levels in each entry
6. THE CLI SHALL use the `info` log level by default

### Requirement 14: Input Validation

**User Story:** As a user, I want early validation of CLI arguments, so that I get immediate feedback on invalid inputs.

#### Acceptance Criteria

1. WHEN a required argument is missing, THE CLI SHALL display an error message and usage information
2. WHEN an input path does not exist, THE CLI SHALL display an error message and return a non-zero Exit_Code
3. WHEN an output directory does not exist, THE CLI SHALL create it if possible
4. WHEN an output directory cannot be created, THE CLI SHALL display an error message and return a non-zero Exit_Code
5. WHEN an invalid reader type is specified, THE CLI SHALL display available reader types and return a non-zero Exit_Code
6. WHEN an invalid writer type is specified, THE CLI SHALL display available writer types and return a non-zero Exit_Code
7. WHEN an invalid transform type is specified, THE CLI SHALL display available transform types and return a non-zero Exit_Code

### Requirement 15: Pipeline Integration

**User Story:** As a developer, I want the CLI to use the existing pipeline infrastructure, so that behavior is consistent between CLI and programmatic usage.

#### Acceptance Criteria

1. THE CLI SHALL invoke the `execute_pipeline` function from `fintran.core.pipeline`
2. THE CLI SHALL use the existing Reader, Writer, and Transform protocols
3. THE CLI SHALL propagate Pipeline exceptions to the error handling layer
4. THE CLI SHALL use the existing Validation_Service for IR validation
5. THE CLI SHALL preserve all Pipeline error context in CLI error messages
6. THE CLI SHALL pass configuration parameters to readers and writers using the Pipeline config mechanism
