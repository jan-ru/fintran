# Design Document: CLI Interface

## Overview

The CLI Interface provides command-line access to the fintran financial document transformation pipeline. Built on the Cyclopts framework, it exposes the existing reader → IR → writer pipeline through intuitive commands for file conversion, validation, inspection, and batch processing.

The CLI follows Unix conventions for command structure, exit codes, and stream handling (stdout/stderr). It integrates seamlessly with the existing pipeline orchestration, validation framework, and error handling infrastructure while providing user-friendly progress indicators and comprehensive error messages.

### Key Design Goals

1. **Intuitive Command Structure**: Clear subcommands (convert, validate, inspect, batch) that map to common user workflows
2. **Pipeline Integration**: Direct use of existing `execute_pipeline`, Reader/Writer/Transform protocols, and validation services
3. **Robust Error Handling**: Comprehensive error messages with context, distinct exit codes for different failure types
4. **Scriptability**: Consistent exit codes, quiet mode, and proper stream handling for automation
5. **Discoverability**: Built-in help, list commands for available components, and configuration validation

### Architecture Principles

- **Thin CLI Layer**: CLI code focuses on argument parsing, user interaction, and error formatting; all business logic remains in core pipeline
- **Zero Duplication**: Reuse all existing pipeline infrastructure rather than reimplementing logic
- **Type Safety**: Leverage Cyclopts' type-based parsing and validation
- **Testability**: CLI commands are testable through direct function invocation

## Architecture

### High-Level Component Structure

```
┌─────────────────────────────────────────────────────────────┐
│                      CLI Entry Point                         │
│                    (fintran/__main__.py)                     │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│                   Cyclopts Application                       │
│                   (fintran/cli/app.py)                       │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  Command Router (Cyclopts decorators)                │   │
│  │  - convert, validate, inspect, batch                 │   │
│  │  - list-readers, list-writers, list-transforms       │   │
│  │  - check-config                                      │   │
│  └──────────────────────────────────────────────────────┘   │
└────────────────────────┬────────────────────────────────────┘
                         │
         ┌───────────────┼───────────────┐
         ▼               ▼               ▼
┌─────────────┐  ┌─────────────┐  ┌─────────────┐
│  Commands   │  │   Config    │  │   Output    │
│  Module     │  │   Module    │  │   Module    │
└──────┬──────┘  └──────┬──────┘  └──────┬──────┘
       │                │                │
       │                │                │
       ▼                ▼                ▼
┌─────────────────────────────────────────────────────────────┐
│              Existing Pipeline Infrastructure                │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │
│  │   Pipeline   │  │  Validation  │  │  Exceptions  │      │
│  │   Service    │  │   Service    │  │   Hierarchy  │      │
│  └──────────────┘  └──────────────┘  └──────────────┘      │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │
│  │   Readers    │  │   Writers    │  │  Transforms  │      │
│  └──────────────┘  └──────────────┘  └──────────────┘      │
└─────────────────────────────────────────────────────────────┘
```

### Module Organization

```
fintran/
├── __main__.py              # CLI entry point (python -m fintran)
├── cli/
│   ├── __init__.py
│   ├── app.py               # Cyclopts application and command routing
│   ├── commands.py          # Command implementations
│   ├── config.py            # Configuration file parsing
│   ├── output.py            # Progress indicators and formatting
│   ├── registry.py          # Reader/Writer/Transform discovery
│   └── exit_codes.py        # Exit code constants
├── core/                    # Existing pipeline (unchanged)
│   ├── pipeline.py
│   ├── protocols.py
│   ├── schema.py
│   └── exceptions.py
├── readers/                 # Existing readers (unchanged)
├── writers/                 # Existing writers (unchanged)
└── transforms/              # Existing transforms (unchanged)
```

## Components and Interfaces

### CLI Application (fintran/cli/app.py)

The main Cyclopts application that defines the command structure and routing.

```python
from cyclopts import App
from fintran.cli import commands

# Create the main application
app = App(
    name="fintran",
    help="Financial document transformation tool",
    version="0.1.0",
)

# Register subcommands
app.command(commands.convert)
app.command(commands.validate)
app.command(commands.inspect)
app.command(commands.batch)
app.command(commands.list_readers, name="list-readers")
app.command(commands.list_writers, name="list-writers")
app.command(commands.list_transforms, name="list-transforms")
app.command(commands.check_config, name="check-config")
```

### Command Implementations (fintran/cli/commands.py)

Each command is implemented as a function with type-annotated parameters. Cyclopts automatically generates argument parsing from the function signature.

#### Convert Command

```python
from pathlib import Path
from typing import Annotated
from cyclopts import Parameter
from fintran.core.pipeline import execute_pipeline
from fintran.cli.config import load_config, merge_config
from fintran.cli.registry import get_reader, get_writer, get_transform
from fintran.cli.output import ProgressIndicator
from fintran.cli.exit_codes import ExitCode

def convert(
    input_path: Annotated[Path, Parameter(help="Input file path")],
    output_path: Annotated[Path, Parameter(help="Output file path")],
    reader: Annotated[str | None, Parameter(help="Reader type (sql, csv, json)")] = None,
    writer: Annotated[str | None, Parameter(help="Writer type (parquet, json, csv)")] = None,
    transform: Annotated[list[str], Parameter(help="Transform to apply (repeatable)")] = [],
    config: Annotated[Path | None, Parameter(help="Configuration file path")] = None,
    dry_run: Annotated[bool, Parameter(help="Preview without writing")] = False,
    quiet: Annotated[bool, Parameter(help="Suppress progress output")] = False,
    verbose: Annotated[bool, Parameter(help="Show detailed error information")] = False,
    log_level: Annotated[str, Parameter(help="Log level (debug, info, warning, error)")] = "info",
    log_file: Annotated[Path | None, Parameter(help="Log file path")] = None,
) -> int:
    """Convert a file from one format to another.
    
    Returns:
        Exit code (0 for success, non-zero for errors)
    """
    try:
        # Load configuration if provided
        cfg = {}
        if config:
            cfg = load_config(config)
        
        # Merge CLI arguments with config (CLI takes precedence)
        cfg = merge_config(cfg, reader=reader, writer=writer, transforms=transform)
        
        # Infer reader/writer from file extensions if not specified
        reader_type = cfg.get("reader") or infer_reader(input_path)
        writer_type = cfg.get("writer") or infer_writer(output_path)
        
        # Get component instances from registry
        reader_instance = get_reader(reader_type)
        writer_instance = get_writer(writer_type)
        transform_instances = [get_transform(t) for t in cfg.get("transforms", [])]
        
        # Setup progress indicator
        progress = ProgressIndicator(enabled=not quiet)
        progress.start(f"Converting {input_path.name}")
        
        # Execute pipeline
        if dry_run:
            # Read and validate only, skip write
            ir = reader_instance.read(input_path, **cfg.get("reader_config", {}))
            from fintran.core.schema import validate_ir
            validate_ir(ir)
            progress.success(f"Dry run: would write {len(ir)} rows to {output_path}")
        else:
            execute_pipeline(
                reader=reader_instance,
                writer=writer_instance,
                input_path=input_path,
                output_path=output_path,
                transforms=transform_instances,
                **cfg.get("pipeline_config", {})
            )
            progress.success(f"Converted {input_path.name} → {output_path.name}")
        
        return ExitCode.SUCCESS
        
    except ValidationError as e:
        handle_error(e, verbose=verbose)
        return ExitCode.VALIDATION_ERROR
    except ReaderError as e:
        handle_error(e, verbose=verbose)
        return ExitCode.READER_ERROR
    except WriterError as e:
        handle_error(e, verbose=verbose)
        return ExitCode.WRITER_ERROR
    except TransformError as e:
        handle_error(e, verbose=verbose)
        return ExitCode.TRANSFORM_ERROR
    except ConfigError as e:
        handle_error(e, verbose=verbose)
        return ExitCode.CONFIG_ERROR
    except Exception as e:
        handle_error(e, verbose=verbose)
        return ExitCode.UNEXPECTED_ERROR
```

#### Validate Command

```python
def validate(
    input_path: Annotated[Path, Parameter(help="Input file path")],
    reader: Annotated[str | None, Parameter(help="Reader type")] = None,
    verbose: Annotated[bool, Parameter(help="Show detailed schema information")] = False,
) -> int:
    """Validate a file against the IR schema.
    
    Returns:
        Exit code (0 for success, 2 for validation errors)
    """
    try:
        # Infer reader if not specified
        reader_type = reader or infer_reader(input_path)
        reader_instance = get_reader(reader_type)
        
        # Read and validate
        ir = reader_instance.read(input_path)
        from fintran.core.schema import validate_ir
        validate_ir(ir)
        
        print(f"✓ Validation successful: {len(ir)} rows")
        
        if verbose:
            print_schema_info(ir)
        
        return ExitCode.SUCCESS
        
    except ValidationError as e:
        print(f"✗ Validation failed:", file=sys.stderr)
        print(f"  {e}", file=sys.stderr)
        return ExitCode.VALIDATION_ERROR
    except Exception as e:
        handle_error(e, verbose=False)
        return ExitCode.UNEXPECTED_ERROR
```

#### Inspect Command

```python
def inspect(
    input_path: Annotated[Path, Parameter(help="Input file path")],
    reader: Annotated[str | None, Parameter(help="Reader type")] = None,
    metadata: Annotated[bool, Parameter(help="Show embedded metadata")] = False,
    sample: Annotated[int | None, Parameter(help="Show first N rows")] = None,
    stats: Annotated[bool, Parameter(help="Show statistical summaries")] = False,
) -> int:
    """Inspect IR structure and metadata.
    
    Returns:
        Exit code (0 for success, non-zero for errors)
    """
    try:
        # Infer reader if not specified
        reader_type = reader or infer_reader(input_path)
        reader_instance = get_reader(reader_type)
        
        # Read file
        ir = reader_instance.read(input_path)
        
        # Display schema information
        print(f"File: {input_path}")
        print(f"Rows: {len(ir)}")
        print(f"\nSchema:")
        for col, dtype in ir.schema.items():
            print(f"  {col}: {dtype}")
        
        # Display metadata if requested
        if metadata:
            print(f"\nMetadata:")
            # TODO: Extract metadata from Parquet files
            print("  (metadata support pending)")
        
        # Display sample rows if requested
        if sample:
            print(f"\nSample ({sample} rows):")
            print(ir.head(sample))
        
        # Display statistics if requested
        if stats:
            print(f"\nStatistics:")
            print(ir.describe())
        
        return ExitCode.SUCCESS
        
    except Exception as e:
        handle_error(e, verbose=False)
        return ExitCode.UNEXPECTED_ERROR
```

#### Batch Command

```python
def batch(
    input_dir: Annotated[Path, Parameter(help="Input directory path")],
    output_dir: Annotated[Path, Parameter(help="Output directory path")],
    pattern: Annotated[str, Parameter(help="File glob pattern")] = "*",
    recursive: Annotated[bool, Parameter(help="Process subdirectories")] = False,
    reader: Annotated[str | None, Parameter(help="Reader type")] = None,
    writer: Annotated[str | None, Parameter(help="Writer type")] = None,
    transform: Annotated[list[str], Parameter(help="Transform to apply")] = [],
    config: Annotated[Path | None, Parameter(help="Configuration file path")] = None,
    quiet: Annotated[bool, Parameter(help="Suppress progress output")] = False,
) -> int:
    """Process multiple files in batch.
    
    Returns:
        Exit code (0 if all succeed, non-zero if any fail)
    """
    try:
        # Find matching files
        if recursive:
            files = list(input_dir.rglob(pattern))
        else:
            files = list(input_dir.glob(pattern))
        
        if not files:
            print(f"No files matching pattern '{pattern}' in {input_dir}", file=sys.stderr)
            return ExitCode.UNEXPECTED_ERROR
        
        # Create output directory if needed
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Process each file
        results = {"success": 0, "failed": 0, "errors": []}
        
        for i, input_file in enumerate(files, 1):
            if not quiet:
                print(f"[{i}/{len(files)}] Processing {input_file.name}...")
            
            # Determine output path (preserve relative structure)
            rel_path = input_file.relative_to(input_dir)
            output_file = output_dir / rel_path.with_suffix(".parquet")
            output_file.parent.mkdir(parents=True, exist_ok=True)
            
            # Convert file
            exit_code = convert(
                input_path=input_file,
                output_path=output_file,
                reader=reader,
                writer=writer,
                transform=transform,
                config=config,
                quiet=True,  # Suppress individual progress
            )
            
            if exit_code == ExitCode.SUCCESS:
                results["success"] += 1
            else:
                results["failed"] += 1
                results["errors"].append((input_file.name, exit_code))
        
        # Display summary
        print(f"\nBatch processing complete:")
        print(f"  Total: {len(files)}")
        print(f"  Success: {results['success']}")
        print(f"  Failed: {results['failed']}")
        
        if results["errors"]:
            print(f"\nFailed files:")
            for filename, code in results["errors"]:
                print(f"  {filename} (exit code {code})")
        
        return ExitCode.SUCCESS if results["failed"] == 0 else ExitCode.UNEXPECTED_ERROR
        
    except Exception as e:
        handle_error(e, verbose=False)
        return ExitCode.UNEXPECTED_ERROR
```

### Configuration Module (fintran/cli/config.py)

Handles loading and parsing configuration files in JSON and YAML formats.

```python
import json
from pathlib import Path
from typing import Any
import yaml  # Will need to add pyyaml dependency

class ConfigError(Exception):
    """Configuration file error."""
    pass

def load_config(path: Path) -> dict[str, Any]:
    """Load configuration from JSON or YAML file.
    
    Args:
        path: Path to configuration file
        
    Returns:
        Configuration dictionary
        
    Raises:
        ConfigError: If file cannot be loaded or parsed
    """
    if not path.exists():
        raise ConfigError(f"Configuration file not found: {path}")
    
    try:
        content = path.read_text()
        
        # Try JSON first
        if path.suffix == ".json":
            return json.loads(content)
        
        # Try YAML
        elif path.suffix in (".yaml", ".yml"):
            return yaml.safe_load(content)
        
        else:
            # Try to auto-detect
            try:
                return json.loads(content)
            except json.JSONDecodeError:
                return yaml.safe_load(content)
                
    except json.JSONDecodeError as e:
        raise ConfigError(f"Invalid JSON in {path}: {e}")
    except yaml.YAMLError as e:
        raise ConfigError(f"Invalid YAML in {path}: {e}")
    except Exception as e:
        raise ConfigError(f"Failed to load config from {path}: {e}")

def merge_config(
    base: dict[str, Any],
    **overrides: Any
) -> dict[str, Any]:
    """Merge CLI arguments into base configuration.
    
    CLI arguments take precedence over config file values.
    
    Args:
        base: Base configuration from file
        **overrides: CLI argument overrides
        
    Returns:
        Merged configuration dictionary
    """
    merged = base.copy()
    
    for key, value in overrides.items():
        if value is not None:
            merged[key] = value
    
    return merged

def validate_config(config: dict[str, Any]) -> list[str]:
    """Validate configuration structure and referenced components.
    
    Args:
        config: Configuration dictionary to validate
        
    Returns:
        List of validation error messages (empty if valid)
    """
    errors = []
    
    # Check reader exists
    if "reader" in config:
        try:
            get_reader(config["reader"])
        except KeyError:
            errors.append(f"Unknown reader type: {config['reader']}")
    
    # Check writer exists
    if "writer" in config:
        try:
            get_writer(config["writer"])
        except KeyError:
            errors.append(f"Unknown writer type: {config['writer']}")
    
    # Check transforms exist
    if "transforms" in config:
        for transform in config["transforms"]:
            try:
                get_transform(transform)
            except KeyError:
                errors.append(f"Unknown transform type: {transform}")
    
    return errors
```

### Registry Module (fintran/cli/registry.py)

Provides discovery and instantiation of readers, writers, and transforms.

```python
from typing import Any
from fintran.core.protocols import Reader, Writer, Transform

# Registry dictionaries (populated by scanning modules)
READERS: dict[str, type[Reader]] = {}
WRITERS: dict[str, type[Writer]] = {}
TRANSFORMS: dict[str, type[Transform]] = {}

def register_reader(name: str, cls: type[Reader]) -> None:
    """Register a reader implementation."""
    READERS[name] = cls

def register_writer(name: str, cls: type[Writer]) -> None:
    """Register a writer implementation."""
    WRITERS[name] = cls

def register_transform(name: str, cls: type[Transform]) -> None:
    """Register a transform implementation."""
    TRANSFORMS[name] = cls

def get_reader(name: str) -> Reader:
    """Get reader instance by name."""
    if name not in READERS:
        available = ", ".join(sorted(READERS.keys()))
        raise KeyError(f"Unknown reader '{name}'. Available: {available}")
    return READERS[name]()

def get_writer(name: str) -> Writer:
    """Get writer instance by name."""
    if name not in WRITERS:
        available = ", ".join(sorted(WRITERS.keys()))
        raise KeyError(f"Unknown writer '{name}'. Available: {available}")
    return WRITERS[name]()

def get_transform(name: str) -> Transform:
    """Get transform instance by name."""
    if name not in TRANSFORMS:
        available = ", ".join(sorted(TRANSFORMS.keys()))
        raise KeyError(f"Unknown transform '{name}'. Available: {available}")
    return TRANSFORMS[name]()

def list_readers() -> dict[str, str]:
    """List available readers with descriptions."""
    return {
        name: cls.__doc__ or "No description"
        for name, cls in sorted(READERS.items())
    }

def list_writers() -> dict[str, str]:
    """List available writers with descriptions."""
    return {
        name: cls.__doc__ or "No description"
        for name, cls in sorted(WRITERS.items())
    }

def list_transforms() -> dict[str, str]:
    """List available transforms with descriptions."""
    return {
        name: cls.__doc__ or "No description"
        for name, cls in sorted(TRANSFORMS.items())
    }
```

### Output Module (fintran/cli/output.py)

Handles progress indicators, formatting, and stream management.

```python
import sys
from typing import TextIO

class ProgressIndicator:
    """Simple progress indicator for CLI operations."""
    
    def __init__(self, enabled: bool = True, stream: TextIO = sys.stderr):
        self.enabled = enabled and stream.isatty()
        self.stream = stream
    
    def start(self, message: str) -> None:
        """Display start message."""
        if self.enabled:
            self.stream.write(f"{message}... ")
            self.stream.flush()
    
    def success(self, message: str) -> None:
        """Display success message."""
        if self.enabled:
            self.stream.write(f"✓\n")
        print(message)
    
    def error(self, message: str) -> None:
        """Display error message."""
        if self.enabled:
            self.stream.write(f"✗\n")
        print(f"Error: {message}", file=sys.stderr)

def handle_error(error: Exception, verbose: bool = False) -> None:
    """Format and display error message.
    
    Args:
        error: Exception to display
        verbose: Whether to show stack trace
    """
    # Display error message
    print(f"Error: {error}", file=sys.stderr)
    
    # Display context if available (FintranError)
    if hasattr(error, "context") and error.context:
        print("Context:", file=sys.stderr)
        for key, value in error.context.items():
            print(f"  {key}: {value}", file=sys.stderr)
    
    # Display stack trace if verbose
    if verbose:
        import traceback
        print("\nStack trace:", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
```

### Exit Codes Module (fintran/cli/exit_codes.py)

Defines standard exit codes for different error conditions.

```python
class ExitCode:
    """Standard exit codes for CLI commands."""
    
    SUCCESS = 0
    UNEXPECTED_ERROR = 1
    VALIDATION_ERROR = 2
    READER_ERROR = 3
    WRITER_ERROR = 4
    TRANSFORM_ERROR = 5
    CONFIG_ERROR = 6
```

## Data Models

### Configuration File Schema

Configuration files support both JSON and YAML formats with the following structure:

```yaml
# Reader configuration
reader: "sql"  # Reader type
reader_config:
  connection_string: "mssql://..."
  table: "transactions"

# Writer configuration  
writer: "parquet"  # Writer type
writer_config:
  compression: "snappy"
  
# Transform pipeline
transforms:
  - "currency_normalizer"
  - "filter_positive"
  
# Pipeline configuration
pipeline_config:
  validate_intermediate: true
```

JSON equivalent:

```json
{
  "reader": "sql",
  "reader_config": {
    "connection_string": "mssql://...",
    "table": "transactions"
  },
  "writer": "parquet",
  "writer_config": {
    "compression": "snappy"
  },
  "transforms": [
    "currency_normalizer",
    "filter_positive"
  ],
  "pipeline_config": {
    "validate_intermediate": true
  }
}
```

### Command Line Argument Precedence

When both configuration file and CLI arguments are provided:

1. CLI arguments override config file values
2. Config file provides defaults for unspecified arguments
3. Built-in defaults apply when neither is specified

Example:
```bash
# Config file specifies reader="sql"
# CLI argument overrides with reader="csv"
fintran convert input.csv output.parquet --config=config.yaml --reader=csv
# Result: Uses CSV reader (CLI takes precedence)
```


## Correctness Properties

*A property is a characteristic or behavior that should hold true across all valid executions of a system—essentially, a formal statement about what the system should do. Properties serve as the bridge between human-readable specifications and machine-verifiable correctness guarantees.*

### Property Reflection

After analyzing all acceptance criteria, I identified several areas of redundancy:

1. **Exit code properties**: Multiple criteria specify exit codes for different error types (validation, reader, writer, transform, config). These can be consolidated into a single property about error-to-exit-code mapping.

2. **Error message format properties**: Multiple criteria specify that error messages should include context (path, field names, etc.). These can be consolidated into a property about error context preservation.

3. **Stream handling**: Both stdout and stderr requirements can be combined into a single property about proper stream separation.

4. **Config file format support**: JSON and YAML support can be tested together as a round-trip property.

5. **Command existence checks**: All the "CLI SHALL provide X subcommand" criteria are examples of the same pattern and can be tested together.

6. **Progress output**: Multiple criteria about progress indicators can be consolidated into properties about progress visibility and quiet mode suppression.

After consolidation, the following properties provide comprehensive coverage without redundancy:

### Property 1: Pipeline Integration

*For any* valid reader, writer, input path, and output path, when the convert command is invoked, the CLI should call `execute_pipeline` from `fintran.core.pipeline` with the correct reader, writer, and paths.

**Validates: Requirements 15.1, 15.2, 2.1**

### Property 2: Exit Code Mapping

*For any* error type (ValidationError, ReaderError, WriterError, TransformError, ConfigError, unexpected), the CLI should return the corresponding exit code (2, 3, 4, 5, 6, 1 respectively), and for successful operations, return exit code 0.

**Validates: Requirements 9.1, 9.2, 9.3, 9.4, 9.5, 9.6, 9.7, 2.7, 2.8**

### Property 3: Error Context Preservation

*For any* FintranError with context information, the CLI error output should include all context fields from the original exception (file paths, field names, transform names, etc.).

**Validates: Requirements 15.5, 7.1, 7.2, 7.3, 7.4, 7.5**

### Property 4: Stream Separation

*For any* CLI operation, error messages should be written to stderr and normal output should be written to stdout.

**Validates: Requirements 7.6, 7.7**

### Property 5: Configuration Round Trip

*For any* valid configuration dictionary, serializing to JSON or YAML and then loading should produce an equivalent configuration.

**Validates: Requirements 3.1, 3.2**

### Property 6: Configuration Loading

*For any* valid configuration file, when loaded by the CLI, all specified reader, writer, and transform settings should be present in the loaded configuration.

**Validates: Requirements 3.3**

### Property 7: CLI Argument Precedence

*For any* configuration file and CLI arguments that specify the same setting, the CLI argument value should take precedence over the configuration file value.

**Validates: Requirements 3.4**

### Property 8: Invalid Configuration Detection

*For any* configuration file with invalid syntax or non-existent file path, the CLI should return exit code 6 and display an error message.

**Validates: Requirements 3.5, 3.6, 11.4**

### Property 9: File Extension Inference

*For any* file path with a recognized extension (.csv, .json, .parquet, .sql), when reader or writer type is not specified, the CLI should infer the correct reader or writer type from the extension.

**Validates: Requirements 2.6**

### Property 10: Validation Error Display

*For any* validation error, the CLI should display field names and constraint violations in the error output.

**Validates: Requirements 4.4, 7.2**

### Property 11: Inspect Output Completeness

*For any* valid input file, the inspect command output should contain column names, data types, and row count.

**Validates: Requirements 5.1, 5.2**

### Property 12: Batch Processing Completeness

*For any* directory with files matching a pattern, the batch command should process all matching files and display a summary with total, successful, and failed counts.

**Validates: Requirements 6.1, 6.8**

### Property 13: Batch Pattern Filtering

*For any* glob pattern and directory, the batch command should process only files matching the pattern.

**Validates: Requirements 6.2**

### Property 14: Batch Error Isolation

*For any* batch operation where some files fail, the CLI should continue processing remaining files and return a non-zero exit code.

**Validates: Requirements 6.7, 6.9**

### Property 15: Progress Indicator Visibility

*For any* file processing operation, when not in quiet mode and output is to a TTY, the CLI should display progress indicators.

**Validates: Requirements 8.1, 8.2, 6.6**

### Property 16: Quiet Mode Suppression

*For any* CLI operation with the --quiet flag, progress indicators should be suppressed but final results and errors should still be displayed.

**Validates: Requirements 8.3, 8.4**

### Property 17: Configuration Validation

*For any* configuration file referencing readers, writers, or transforms, the check-config command should verify that all referenced components exist in the registry.

**Validates: Requirements 11.2, 11.5, 11.6**

### Property 18: Dry Run Behavior

*For any* convert command with --dry-run flag, the CLI should execute through validation but not write output files, and should display what would be written.

**Validates: Requirements 12.2, 12.3, 12.4, 12.5**

### Property 19: Log Level Configuration

*For any* valid log level (debug, info, warning, error), when specified via --log-level, the Python logging system should be configured to that level.

**Validates: Requirements 13.2**

### Property 20: Log File Writing

*For any* log file path specified via --log-file, log entries should be written to that file with timestamps and log levels.

**Validates: Requirements 13.4, 13.5**

### Property 21: Input Validation

*For any* non-existent input path, the CLI should display an error message and return a non-zero exit code before attempting pipeline execution.

**Validates: Requirements 14.2**

### Property 22: Output Directory Creation

*For any* non-existent output directory, the CLI should create the directory if possible, or display an error and return a non-zero exit code if creation fails.

**Validates: Requirements 14.3, 14.4**

### Property 23: Invalid Component Type Handling

*For any* invalid reader, writer, or transform type, the CLI should display available types and return a non-zero exit code.

**Validates: Requirements 14.5, 14.6, 14.7**

### Property 24: Exception Propagation

*For any* exception raised by the pipeline, the CLI should propagate it to the error handling layer without losing exception type or context.

**Validates: Requirements 15.3**

### Property 25: Configuration Parameter Passing

*For any* configuration parameters specified in config file or CLI arguments, they should be passed to readers and writers via the pipeline's config mechanism.

**Validates: Requirements 15.6**

## Error Handling

### Error Handling Strategy

The CLI implements a layered error handling approach:

1. **Early Validation**: Validate CLI arguments and file paths before invoking pipeline
2. **Exception Catching**: Catch all FintranError subclasses and map to appropriate exit codes
3. **Context Preservation**: Preserve all error context from pipeline exceptions
4. **User-Friendly Messages**: Format error messages for readability while maintaining technical detail
5. **Stream Separation**: Write errors to stderr, normal output to stdout

### Error Categories and Exit Codes

```python
class ExitCode:
    SUCCESS = 0              # Operation completed successfully
    UNEXPECTED_ERROR = 1     # Unexpected/unhandled exception
    VALIDATION_ERROR = 2     # IR schema validation failure
    READER_ERROR = 3         # Input file reading/parsing failure
    WRITER_ERROR = 4         # Output file writing/serialization failure
    TRANSFORM_ERROR = 5      # Transform operation failure
    CONFIG_ERROR = 6         # Configuration file or argument error
```

### Error Message Format

All error messages follow this format:

```
Error: <primary error message>
Context:
  <key1>: <value1>
  <key2>: <value2>
  ...

[Stack trace if --verbose flag is set]
```

Example:

```
Error: Pipeline failed at read step: File not found
Context:
  step: read
  input_path: /path/to/missing.csv
  file_path: /path/to/missing.csv
```

### Error Handling Flow

```
┌─────────────────────────────────────────────────────────────┐
│                    CLI Command Function                      │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
                    ┌─────────┐
                    │  try:   │
                    └────┬────┘
                         │
         ┌───────────────┼───────────────┐
         ▼               ▼               ▼
    ┌─────────┐    ┌─────────┐    ┌─────────┐
    │ Validate│    │ Execute │    │ Format  │
    │  Args   │───▶│Pipeline │───▶│ Output  │
    └─────────┘    └─────────┘    └─────────┘
         │               │               │
         │ Error         │ Error         │ Success
         ▼               ▼               ▼
    ┌─────────────────────────────────────────┐
    │         except FintranError:            │
    │  - Map exception type to exit code      │
    │  - Format error message with context    │
    │  - Write to stderr                      │
    │  - Return exit code                     │
    └─────────────────────────────────────────┘
```

### Verbose Mode

When `--verbose` flag is provided:
- Full stack traces are displayed for all errors
- Additional diagnostic information is included
- Useful for debugging and issue reporting

Example:

```bash
$ fintran convert input.csv output.parquet --verbose
Error: Pipeline failed at read step: File not found
Context:
  step: read
  input_path: input.csv
  file_path: input.csv

Stack trace:
Traceback (most recent call last):
  File "fintran/cli/commands.py", line 45, in convert
    ir = reader_instance.read(input_path)
  File "fintran/readers/csv.py", line 23, in read
    raise ReaderError(f"File not found: {path}", file_path=str(path))
fintran.core.exceptions.ReaderError: File not found: input.csv
```

## Testing Strategy

### Dual Testing Approach

The CLI will be tested using both unit tests and property-based tests:

**Unit Tests** focus on:
- Specific command invocations with known inputs
- Edge cases (empty files, missing arguments, special characters)
- Integration points (Cyclopts argument parsing, registry lookups)
- Error conditions (invalid paths, malformed configs)
- Output formatting (help text, version display, list commands)

**Property-Based Tests** focus on:
- Universal properties across all inputs (exit codes, error context, stream handling)
- Configuration round-tripping (JSON/YAML serialization)
- Argument precedence rules
- Batch processing behavior with generated file sets
- Error propagation and context preservation

### Property-Based Testing Configuration

Using Hypothesis for property-based testing:

```python
from hypothesis import given, strategies as st
import pytest

# Configure Hypothesis for CLI tests
@pytest.fixture(autouse=True)
def hypothesis_config():
    """Configure Hypothesis for CLI property tests."""
    # Run 100 iterations per property test
    return {"max_examples": 100}

# Example property test
@given(
    reader_type=st.sampled_from(["csv", "json", "sql"]),
    writer_type=st.sampled_from(["parquet", "json", "csv"]),
)
def test_exit_code_success_property(reader_type, writer_type, tmp_path):
    """
    Feature: cli-interface, Property 2: Exit Code Mapping
    
    For any valid reader and writer combination, successful pipeline
    execution should return exit code 0.
    """
    # Create test input file
    input_file = tmp_path / f"input.{reader_type}"
    output_file = tmp_path / f"output.{writer_type}"
    
    # Create valid input data
    create_valid_input(input_file, reader_type)
    
    # Execute convert command
    exit_code = convert(
        input_path=input_file,
        output_path=output_file,
        reader=reader_type,
        writer=writer_type,
    )
    
    # Verify exit code is 0 for success
    assert exit_code == ExitCode.SUCCESS
```

### Test Tag Format

Each property test must include a comment tag referencing the design property:

```python
"""
Feature: cli-interface, Property N: <property title>

<property description>
"""
```

### Test Organization

```
tests/
├── cli/
│   ├── __init__.py
│   ├── test_commands.py           # Unit tests for command functions
│   ├── test_config.py             # Unit tests for config loading
│   ├── test_registry.py           # Unit tests for component registry
│   ├── test_output.py             # Unit tests for output formatting
│   ├── test_cli_properties.py     # Property tests for CLI behavior
│   ├── test_exit_codes.py         # Property tests for exit code mapping
│   ├── test_error_handling.py     # Property tests for error propagation
│   └── test_config_properties.py  # Property tests for config handling
```

### Testing CLI Commands

CLI commands are testable by direct function invocation:

```python
from fintran.cli.commands import convert
from fintran.cli.exit_codes import ExitCode

def test_convert_success(tmp_path):
    """Test successful file conversion."""
    input_file = tmp_path / "input.csv"
    output_file = tmp_path / "output.parquet"
    
    # Create test input
    input_file.write_text("date,account,amount,currency\n2024-01-01,ACC001,100.00,USD\n")
    
    # Execute command
    exit_code = convert(
        input_path=input_file,
        output_path=output_file,
        reader="csv",
        writer="parquet",
    )
    
    # Verify success
    assert exit_code == ExitCode.SUCCESS
    assert output_file.exists()
```

### Testing with Mocks

For testing error handling without actual failures:

```python
from unittest.mock import Mock, patch
from fintran.core.exceptions import ReaderError

def test_reader_error_handling():
    """Test that reader errors are handled correctly."""
    with patch("fintran.cli.registry.get_reader") as mock_get_reader:
        # Configure mock to raise ReaderError
        mock_reader = Mock()
        mock_reader.read.side_effect = ReaderError(
            "Parse error",
            file_path="test.csv",
            line_number=5,
        )
        mock_get_reader.return_value = mock_reader
        
        # Execute command
        exit_code = convert(
            input_path=Path("test.csv"),
            output_path=Path("output.parquet"),
        )
        
        # Verify error handling
        assert exit_code == ExitCode.READER_ERROR
```

### Integration Testing

Integration tests verify end-to-end CLI behavior:

```python
import subprocess

def test_cli_integration(tmp_path):
    """Test CLI via subprocess invocation."""
    input_file = tmp_path / "input.csv"
    output_file = tmp_path / "output.parquet"
    
    # Create test input
    input_file.write_text("date,account,amount,currency\n2024-01-01,ACC001,100.00,USD\n")
    
    # Invoke CLI
    result = subprocess.run(
        ["python", "-m", "fintran", "convert", str(input_file), str(output_file)],
        capture_output=True,
        text=True,
    )
    
    # Verify success
    assert result.returncode == 0
    assert output_file.exists()
```

### Coverage Goals

- Unit test coverage: >90% for CLI modules
- Property test coverage: All 25 correctness properties implemented
- Integration test coverage: All major command workflows
- Error path coverage: All error types and exit codes

## Implementation Notes

### Cyclopts Integration

Cyclopts provides type-based CLI parsing with minimal boilerplate:

```python
from cyclopts import App, Parameter
from typing import Annotated
from pathlib import Path

app = App(name="fintran")

@app.command
def convert(
    input_path: Annotated[Path, Parameter(help="Input file path")],
    output_path: Annotated[Path, Parameter(help="Output file path")],
    reader: Annotated[str | None, Parameter(help="Reader type")] = None,
) -> int:
    """Convert a file from one format to another."""
    # Implementation
    return 0
```

Key Cyclopts features used:
- **Type annotations**: Automatic type conversion and validation
- **Annotated parameters**: Rich parameter metadata (help text, defaults)
- **Subcommands**: Nested command structure via `@app.command`
- **Automatic help**: Generated from function signatures and docstrings

### Registry Pattern

The registry pattern enables dynamic component discovery:

```python
# In fintran/readers/csv.py
from fintran.cli.registry import register_reader

@register_reader("csv")
class CSVReader:
    """CSV file reader."""
    def read(self, path: Path, **config) -> pl.DataFrame:
        # Implementation
        pass
```

This allows:
- Automatic discovery of available components
- Easy addition of new readers/writers/transforms
- Runtime component lookup by name
- List commands (list-readers, list-writers, list-transforms)

### Progress Indicators

Progress indicators use TTY detection to avoid polluting redirected output:

```python
import sys

class ProgressIndicator:
    def __init__(self, enabled: bool = True):
        # Disable if output is redirected (not a TTY)
        self.enabled = enabled and sys.stderr.isatty()
    
    def start(self, message: str) -> None:
        if self.enabled:
            sys.stderr.write(f"{message}... ")
            sys.stderr.flush()
    
    def success(self) -> None:
        if self.enabled:
            sys.stderr.write("✓\n")
```

### Configuration File Examples

JSON configuration:

```json
{
  "reader": "sql",
  "reader_config": {
    "connection_string": "mssql://server/database",
    "table": "transactions"
  },
  "writer": "parquet",
  "writer_config": {
    "compression": "snappy"
  },
  "transforms": ["currency_normalizer"]
}
```

YAML configuration:

```yaml
reader: sql
reader_config:
  connection_string: mssql://server/database
  table: transactions

writer: parquet
writer_config:
  compression: snappy

transforms:
  - currency_normalizer
```

### Entry Point

The CLI is invoked via Python's `-m` flag:

```bash
python -m fintran convert input.csv output.parquet
```

This requires a `__main__.py` file:

```python
# fintran/__main__.py
from fintran.cli.app import app

if __name__ == "__main__":
    exit_code = app()
    exit(exit_code)
```

### Logging Configuration

Logging is configured based on CLI arguments:

```python
import logging

def configure_logging(level: str = "info", log_file: Path | None = None) -> None:
    """Configure Python logging for CLI."""
    # Map string level to logging constant
    level_map = {
        "debug": logging.DEBUG,
        "info": logging.INFO,
        "warning": logging.WARNING,
        "error": logging.ERROR,
    }
    
    # Configure root logger
    logging.basicConfig(
        level=level_map[level],
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(log_file) if log_file else logging.StreamHandler(),
        ],
    )
```

## Dependencies

### New Dependencies Required

The CLI implementation requires adding the following dependencies:

```toml
[project]
dependencies = [
    # ... existing dependencies ...
    "cyclopts>=3.0.0",      # Already present
    "pyyaml>=6.0.0",        # For YAML config file support
]
```

### Dependency Justification

- **Cyclopts**: Modern CLI framework with type-based parsing, minimal boilerplate, and excellent help generation
- **PyYAML**: Standard YAML parser for Python, required for YAML configuration file support

## Future Enhancements

Potential future enhancements not in current scope:

1. **Shell Completion**: Generate shell completion scripts for bash/zsh/fish
2. **Interactive Mode**: Prompt for missing arguments interactively
3. **Configuration Wizard**: Interactive configuration file generation
4. **Watch Mode**: Monitor directory and auto-process new files
5. **Parallel Batch Processing**: Use multiprocessing for batch operations
6. **Progress Bars**: Rich progress bars using libraries like `rich` or `tqdm`
7. **Color Output**: Colored output for better readability (using `rich` or `colorama`)
8. **JSON Output Mode**: Machine-readable JSON output for scripting
9. **Plugin System**: Load custom readers/writers/transforms from external packages
10. **Remote Execution**: Execute pipelines on remote servers via SSH/API

