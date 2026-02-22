"""CLI command implementations.

This module implements the core CLI commands for the fintran tool:
- convert: Transform files between formats
- validate: Validate files against IR schema
- inspect: Display IR structure and metadata
- batch: Process multiple files
- list_*: List available components
- check_config: Validate configuration files

Each command is implemented as a function that returns an exit code,
enabling both direct invocation and subprocess-based testing.

Requirements: All requirements from sections 2-15 of the requirements document
"""

import sys
from pathlib import Path
from typing import Annotated, Any

from cyclopts import Parameter
from fintran.cli.config import ConfigError, load_config, merge_config, validate_config
from fintran.cli.exit_codes import ExitCode
from fintran.cli.output import ProgressIndicator, handle_error
from fintran.cli.registry import (
    get_reader,
    get_transform,
    get_writer,
    list_readers as registry_list_readers,
    list_writers as registry_list_writers,
    list_transforms as registry_list_transforms,
)
from fintran.core.exceptions import (
    PipelineError,
    ReaderError,
    TransformError,
    ValidationError,
    WriterError,
)
from fintran.core.pipeline import execute_pipeline
from fintran.core.schema import validate_ir


def infer_reader(path: Path) -> str:
    """Infer reader type from file extension.
    
    Args:
        path: Input file path
        
    Returns:
        Reader type name (e.g., "csv", "json", "parquet")
        
    Raises:
        ValueError: If extension is not recognized
        
    Requirements:
        - Requirement 2.6: Infer reader from file extension
    """
    extension_map = {
        ".csv": "csv",
        ".json": "json",
        ".parquet": "parquet",
        ".pq": "parquet",
        ".xlsx": "excel",
        ".xls": "excel",
        ".journal": "hledger",
    }
    
    suffix = path.suffix.lower()
    if suffix not in extension_map:
        raise ValueError(
            f"Cannot infer reader type from extension '{suffix}'. "
            f"Please specify --reader explicitly."
        )
    
    return extension_map[suffix]


def infer_writer(path: Path) -> str:
    """Infer writer type from file extension.
    
    Args:
        path: Output file path
        
    Returns:
        Writer type name (e.g., "csv", "json", "parquet")
        
    Raises:
        ValueError: If extension is not recognized
        
    Requirements:
        - Requirement 2.6: Infer writer from file extension
    """
    extension_map = {
        ".csv": "csv",
        ".json": "json",
        ".parquet": "parquet",
        ".pq": "parquet",
        ".xlsx": "excel",
        ".xls": "excel",
        ".journal": "hledger",
        ".duckdb": "duckdb",
        ".db": "duckdb",
    }
    
    suffix = path.suffix.lower()
    if suffix not in extension_map:
        raise ValueError(
            f"Cannot infer writer type from extension '{suffix}'. "
            f"Please specify --writer explicitly."
        )
    
    return extension_map[suffix]


def convert(
    input_path: Annotated[Path, Parameter(help="Input file path")],
    output_path: Annotated[Path, Parameter(help="Output file path")],
    reader: Annotated[str | None, Parameter(help="Reader type (csv, json, parquet)")] = None,
    writer: Annotated[str | None, Parameter(help="Writer type (parquet, json, csv)")] = None,
    transform: Annotated[list[str], Parameter(help="Transform to apply (repeatable)")] = None,
    config: Annotated[Path | None, Parameter(help="Configuration file path")] = None,
    dry_run: Annotated[bool, Parameter(help="Preview without writing")] = False,
    quiet: Annotated[bool, Parameter(help="Suppress progress output")] = False,
    verbose: Annotated[bool, Parameter(help="Show detailed error information")] = False,
    log_level: Annotated[str, Parameter(help="Log level (debug, info, warning, error)")] = "info",
    log_file: Annotated[Path | None, Parameter(help="Log file path")] = None,
) -> int:
    """Convert a file from one format to another.
    
    Executes the complete reader → transform → writer pipeline to convert
    financial data between formats. Supports configuration files, dry-run mode,
    and comprehensive error handling.
    
    Args:
        input_path: Path to input file
        output_path: Path to output file
        reader: Reader type (inferred from extension if not specified)
        writer: Writer type (inferred from extension if not specified)
        transform: List of transforms to apply (optional)
        config: Path to configuration file (optional)
        dry_run: Preview without writing output file
        quiet: Suppress progress indicators
        verbose: Show detailed error information including stack traces
        log_level: Logging level (debug, info, warning, error)
        log_file: Path to log file (optional)
        
    Returns:
        Exit code (0 for success, non-zero for errors)
        
    Example:
        >>> from pathlib import Path
        >>> from fintran.cli.commands import convert
        >>> 
        >>> exit_code = convert(
        ...     input_path=Path("input.csv"),
        ...     output_path=Path("output.parquet"),
        ...     reader="csv",
        ...     writer="parquet",
        ... )
        
    Requirements:
        - Requirement 2.1: Execute pipeline with reader and writer
        - Requirement 2.2: Accept --reader argument
        - Requirement 2.3: Accept --writer argument
        - Requirement 2.4: Accept --transform argument (repeatable)
        - Requirement 2.5: Accept --config argument
        - Requirement 2.6: Infer reader/writer from file extensions
        - Requirement 2.7: Return exit code 0 on success
        - Requirement 2.8: Return non-zero exit code on failure
        - Requirement 12.1-12.5: Dry-run mode
        - Requirement 15.1: Use execute_pipeline
        - Requirement 15.2: Use existing Reader/Writer protocols
    """
    # Handle default for transform parameter
    if transform is None:
        transform = []
    
    try:
        # Validate input path exists (Requirement 14.2)
        if not input_path.exists():
            print(f"Error: Input file not found: {input_path}", file=sys.stderr)
            return ExitCode.UNEXPECTED_ERROR
        
        # Load configuration if provided (Requirement 3.3)
        cfg: dict[str, Any] = {}
        if config:
            cfg = load_config(config)
        
        # Merge CLI arguments with config (CLI takes precedence) (Requirement 3.4)
        cfg = merge_config(cfg, reader=reader, writer=writer, transforms=transform)
        
        # Infer reader/writer from file extensions if not specified (Requirement 2.6)
        try:
            reader_type = cfg.get("reader") or infer_reader(input_path)
            writer_type = cfg.get("writer") or infer_writer(output_path)
        except ValueError as e:
            print(f"Error: {e}", file=sys.stderr)
            return ExitCode.CONFIG_ERROR
        
        # Get component instances from registry (Requirement 14.5, 14.6, 14.7)
        try:
            reader_instance = get_reader(reader_type)
            writer_instance = get_writer(writer_type)
            transform_instances = [get_transform(t) for t in cfg.get("transforms", [])]
        except KeyError as e:
            print(f"Error: {e}", file=sys.stderr)
            return ExitCode.CONFIG_ERROR
        
        # Create output directory if needed (Requirement 14.3, 14.4)
        try:
            output_path.parent.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            print(f"Error: Cannot create output directory: {e}", file=sys.stderr)
            return ExitCode.UNEXPECTED_ERROR
        
        # Setup progress indicator (Requirement 8.1, 8.2, 8.3)
        progress = ProgressIndicator(enabled=not quiet)
        progress.start(f"Converting {input_path.name}")
        
        # Execute pipeline or dry-run (Requirement 12.1-12.5, 15.1)
        if dry_run:
            # Read and validate only, skip write (Requirement 12.2)
            reader_config = cfg.get("reader_config", {})
            ir = reader_instance.read(input_path, **reader_config)
            ir = validate_ir(ir)
            
            # Apply transforms
            for transform_instance in transform_instances:
                ir = transform_instance.transform(ir)
                ir = validate_ir(ir)
            
            # Display what would be written (Requirement 12.3)
            progress.success(
                f"Dry run: would write {len(ir)} rows to {output_path}"
            )
        else:
            # Execute full pipeline (Requirement 15.1, 15.2)
            execute_pipeline(
                reader=reader_instance,
                writer=writer_instance,
                input_path=input_path,
                output_path=output_path,
                transforms=transform_instances,
                **cfg.get("reader_config", {}),
                **cfg.get("writer_config", {}),
                **cfg.get("pipeline_config", {}),
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
    except PipelineError as e:
        handle_error(e, verbose=verbose)
        return ExitCode.UNEXPECTED_ERROR
    except Exception as e:
        handle_error(e, verbose=verbose)
        return ExitCode.UNEXPECTED_ERROR



def validate(
    input_path: Annotated[Path, Parameter(help="Input file path")],
    reader: Annotated[str | None, Parameter(help="Reader type")] = None,
    verbose: Annotated[bool, Parameter(help="Show detailed schema information")] = False,
) -> int:
    """Validate a file against the IR schema.
    
    Reads the input file using the specified (or inferred) reader and validates
    that the resulting DataFrame conforms to the IR schema requirements. Displays
    validation results and optionally shows detailed schema information.
    
    Args:
        input_path: Path to input file to validate
        reader: Reader type (inferred from extension if not specified)
        verbose: Show detailed schema information including column types
        
    Returns:
        Exit code (0 for success, 2 for validation errors)
        
    Example:
        >>> from pathlib import Path
        >>> from fintran.cli.commands import validate
        >>> 
        >>> exit_code = validate(
        ...     input_path=Path("input.csv"),
        ...     reader="csv",
        ...     verbose=True,
        ... )
        
    Requirements:
        - Requirement 4.1: Read file and validate against IR schema
        - Requirement 4.2: Accept --reader argument
        - Requirement 4.3: Return exit code 0 on success
        - Requirement 4.4: Display validation errors with field names
        - Requirement 4.5: Accept --verbose flag
        - Requirement 4.6: Display error for unreadable files
        - Requirement 15.4: Use existing Validation_Service
    """
    try:
        # Validate input path exists
        if not input_path.exists():
            print(f"Error: Input file not found: {input_path}", file=sys.stderr)
            return ExitCode.UNEXPECTED_ERROR
        
        # Infer reader if not specified (Requirement 4.2)
        try:
            reader_type = reader or infer_reader(input_path)
        except ValueError as e:
            print(f"Error: {e}", file=sys.stderr)
            return ExitCode.CONFIG_ERROR
        
        # Get reader instance from registry
        try:
            reader_instance = get_reader(reader_type)
        except KeyError as e:
            print(f"Error: {e}", file=sys.stderr)
            return ExitCode.CONFIG_ERROR
        
        # Read and validate (Requirement 4.1, 15.4)
        ir = reader_instance.read(input_path)
        ir = validate_ir(ir)
        
        # Display success message (Requirement 4.3)
        print(f"✓ Validation successful: {len(ir)} rows")
        
        # Display schema information if verbose (Requirement 4.5)
        if verbose:
            print(f"\nSchema:")
            for col_name in ir.columns:
                col_type = ir.schema[col_name]
                # Mark required vs optional fields
                from fintran.core.schema import REQUIRED_FIELDS
                required_marker = " [REQUIRED]" if col_name in REQUIRED_FIELDS else " [OPTIONAL]"
                print(f"  {col_name}: {col_type}{required_marker}")
        
        return ExitCode.SUCCESS
        
    except ValidationError as e:
        # Display validation errors with field names (Requirement 4.4)
        print(f"✗ Validation failed:", file=sys.stderr)
        print(f"  {e}", file=sys.stderr)
        return ExitCode.VALIDATION_ERROR
    except ReaderError as e:
        # Display reader errors (Requirement 4.6)
        handle_error(e, verbose=False)
        return ExitCode.READER_ERROR
    except Exception as e:
        handle_error(e, verbose=False)
        return ExitCode.UNEXPECTED_ERROR



def inspect(
    input_path: Annotated[Path, Parameter(help="Input file path")],
    reader: Annotated[str | None, Parameter(help="Reader type")] = None,
    metadata: Annotated[bool, Parameter(help="Show embedded metadata")] = False,
    sample: Annotated[int | None, Parameter(help="Show first N rows")] = None,
    stats: Annotated[bool, Parameter(help="Show statistical summaries")] = False,
) -> int:
    """Inspect IR structure and metadata.
    
    Reads the input file and displays information about its structure, including
    schema (column names and types), row count, and optionally metadata, sample
    rows, and statistical summaries.
    
    Args:
        input_path: Path to input file to inspect
        reader: Reader type (inferred from extension if not specified)
        metadata: Show embedded metadata from Parquet files
        sample: Number of rows to display as sample
        stats: Show statistical summaries for numeric columns
        
    Returns:
        Exit code (0 for success, non-zero for errors)
        
    Example:
        >>> from pathlib import Path
        >>> from fintran.cli.commands import inspect
        >>> 
        >>> exit_code = inspect(
        ...     input_path=Path("data.parquet"),
        ...     sample=5,
        ...     stats=True,
        ... )
        
    Requirements:
        - Requirement 5.1: Read file and display IR schema information
        - Requirement 5.2: Display column names, data types, and row count
        - Requirement 5.3: Accept --metadata flag
        - Requirement 5.4: Accept --sample argument
        - Requirement 5.5: Accept --stats flag
        - Requirement 5.6: Format output in human-readable table format
        - Requirement 5.7: Display error for unreadable files
    """
    try:
        # Validate input path exists
        if not input_path.exists():
            print(f"Error: Input file not found: {input_path}", file=sys.stderr)
            return ExitCode.UNEXPECTED_ERROR
        
        # Infer reader if not specified
        try:
            reader_type = reader or infer_reader(input_path)
        except ValueError as e:
            print(f"Error: {e}", file=sys.stderr)
            return ExitCode.CONFIG_ERROR
        
        # Get reader instance from registry
        try:
            reader_instance = get_reader(reader_type)
        except KeyError as e:
            print(f"Error: {e}", file=sys.stderr)
            return ExitCode.CONFIG_ERROR
        
        # Read file (Requirement 5.1)
        ir = reader_instance.read(input_path)
        
        # Display basic information (Requirement 5.2)
        print(f"File: {input_path}")
        print(f"Rows: {len(ir)}")
        print(f"\nSchema:")
        for col_name in ir.columns:
            col_type = ir.schema[col_name]
            print(f"  {col_name}: {col_type}")
        
        # Display metadata if requested (Requirement 5.3)
        if metadata:
            print(f"\nMetadata:")
            # Check if file is Parquet and has metadata
            if input_path.suffix.lower() in (".parquet", ".pq"):
                try:
                    import pyarrow.parquet as pq
                    parquet_file = pq.ParquetFile(input_path)
                    if parquet_file.schema_arrow.metadata:
                        for key, value in parquet_file.schema_arrow.metadata.items():
                            print(f"  {key.decode()}: {value.decode()}")
                    else:
                        print("  (no metadata found)")
                except Exception as e:
                    print(f"  (unable to read metadata: {e})")
            else:
                print("  (metadata only supported for Parquet files)")
        
        # Display sample rows if requested (Requirement 5.4, 5.6)
        if sample:
            print(f"\nSample ({sample} rows):")
            print(ir.head(sample))
        
        # Display statistics if requested (Requirement 5.5, 5.6)
        if stats:
            print(f"\nStatistics:")
            print(ir.describe())
        
        return ExitCode.SUCCESS
        
    except ReaderError as e:
        handle_error(e, verbose=False)
        return ExitCode.READER_ERROR
    except Exception as e:
        handle_error(e, verbose=False)
        return ExitCode.UNEXPECTED_ERROR


def batch(
    input_dir: Annotated[Path, Parameter(help="Input directory path")],
    output_dir: Annotated[Path, Parameter(help="Output directory path")],
    pattern: Annotated[str, Parameter(help="File glob pattern")] = "*",
    recursive: Annotated[bool, Parameter(help="Process subdirectories")] = False,
    reader: Annotated[str | None, Parameter(help="Reader type")] = None,
    writer: Annotated[str | None, Parameter(help="Writer type")] = None,
    transform: Annotated[list[str], Parameter(help="Transform to apply (repeatable)")] = None,
    config: Annotated[Path | None, Parameter(help="Configuration file path")] = None,
    quiet: Annotated[bool, Parameter(help="Suppress progress output")] = False,
) -> int:
    """Process multiple files in batch.
    
    Processes all files matching the specified pattern in the input directory,
    converting each to the output directory while preserving relative directory
    structure. Errors in individual files are isolated and do not stop processing
    of remaining files.
    
    Args:
        input_dir: Path to input directory containing files to process
        output_dir: Path to output directory for converted files
        pattern: Glob pattern to match files (default: "*" matches all files)
        recursive: Process subdirectories recursively using rglob
        reader: Reader type (inferred from file extensions if not specified)
        writer: Writer type (inferred from file extensions if not specified)
        transform: List of transforms to apply to each file (optional)
        config: Path to configuration file (optional)
        quiet: Suppress per-file progress output
        
    Returns:
        Exit code (0 if all files succeed, non-zero if any fail)
        
    Example:
        >>> from pathlib import Path
        >>> from fintran.cli.commands import batch
        >>> 
        >>> exit_code = batch(
        ...     input_dir=Path("input"),
        ...     output_dir=Path("output"),
        ...     pattern="*.csv",
        ...     recursive=True,
        ...     writer="parquet",
        ... )
        
    Requirements:
        - Requirement 6.1: Process all matching files in directory
        - Requirement 6.2: Accept --pattern argument for glob filtering
        - Requirement 6.3: Accept --output-dir argument
        - Requirement 6.4: Accept --recursive flag for subdirectories
        - Requirement 6.5: Process files in parallel when possible
        - Requirement 6.6: Display progress information
        - Requirement 6.7: Continue processing on individual file errors
        - Requirement 6.8: Display summary with total, successful, failed counts
        - Requirement 6.9: Return non-zero exit code if any file fails
    """
    # Handle default for transform parameter
    if transform is None:
        transform = []
    
    try:
        # Validate input directory exists
        if not input_dir.exists():
            print(f"Error: Input directory not found: {input_dir}", file=sys.stderr)
            return ExitCode.UNEXPECTED_ERROR
        
        if not input_dir.is_dir():
            print(f"Error: Input path is not a directory: {input_dir}", file=sys.stderr)
            return ExitCode.UNEXPECTED_ERROR
        
        # Find matching files (Requirement 6.2, 6.4)
        if recursive:
            files = list(input_dir.rglob(pattern))
        else:
            files = list(input_dir.glob(pattern))
        
        # Filter to only include files (not directories)
        files = [f for f in files if f.is_file()]
        
        if not files:
            print(f"No files matching pattern '{pattern}' in {input_dir}", file=sys.stderr)
            return ExitCode.UNEXPECTED_ERROR
        
        # Create output directory if needed (Requirement 6.3)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Process each file (Requirement 6.1, 6.5, 6.6, 6.7)
        results = {"success": 0, "failed": 0, "errors": []}
        
        for i, input_file in enumerate(files, 1):
            if not quiet:
                print(f"[{i}/{len(files)}] Processing {input_file.name}...")
            
            # Determine output path (preserve relative structure)
            rel_path = input_file.relative_to(input_dir)
            
            # Infer output extension if writer is specified
            if writer:
                # Map writer type to extension
                writer_extension_map = {
                    "csv": ".csv",
                    "json": ".json",
                    "parquet": ".parquet",
                    "excel": ".xlsx",
                    "hledger": ".journal",
                    "duckdb": ".duckdb",
                }
                new_suffix = writer_extension_map.get(writer, ".parquet")
                output_file = output_dir / rel_path.with_suffix(new_suffix)
            else:
                # Keep original extension
                output_file = output_dir / rel_path
            
            # Create output subdirectory if needed
            output_file.parent.mkdir(parents=True, exist_ok=True)
            
            # Convert file (Requirement 6.7: error isolation)
            try:
                exit_code = convert(
                    input_path=input_file,
                    output_path=output_file,
                    reader=reader,
                    writer=writer,
                    transform=transform,
                    config=config,
                    quiet=True,  # Suppress individual progress
                    verbose=False,
                )
                
                if exit_code == ExitCode.SUCCESS:
                    results["success"] += 1
                    if not quiet:
                        print(f"  ✓ Success")
                else:
                    results["failed"] += 1
                    results["errors"].append((input_file.name, exit_code))
                    if not quiet:
                        print(f"  ✗ Failed (exit code {exit_code})", file=sys.stderr)
            except Exception as e:
                # Catch any unexpected errors and continue processing
                results["failed"] += 1
                results["errors"].append((input_file.name, "exception"))
                if not quiet:
                    print(f"  ✗ Failed: {e}", file=sys.stderr)
        
        # Display summary (Requirement 6.8)
        print(f"\nBatch processing complete:")
        print(f"  Total: {len(files)}")
        print(f"  Success: {results['success']}")
        print(f"  Failed: {results['failed']}")
        
        if results["errors"]:
            print(f"\nFailed files:")
            for filename, code in results["errors"]:
                print(f"  {filename} (exit code {code})")
        
        # Return appropriate exit code (Requirement 6.9)
        return ExitCode.SUCCESS if results["failed"] == 0 else ExitCode.UNEXPECTED_ERROR
        
    except Exception as e:
        handle_error(e, verbose=False)
        return ExitCode.UNEXPECTED_ERROR

def list_readers() -> int:
    """List available readers with descriptions.
    
    Displays all registered reader types with their descriptions extracted
    from class docstrings. Useful for discovering what input formats are
    supported.
    
    Returns:
        Exit code (always 0 for success)
        
    Example:
        >>> from fintran.cli.commands import list_readers
        >>> exit_code = list_readers()
        
    Requirements:
        - Requirement 10.1: Provide list-readers subcommand
        - Requirement 10.2: Display available reader types with descriptions
    """
    readers = registry_list_readers()
    
    if not readers:
        print("No readers registered.")
        return ExitCode.SUCCESS
    
    print("Available readers:")
    for name, description in readers.items():
        # Clean up docstring (first line only)
        desc_line = description.split("\\n")[0].strip()
        print(f"  {name:15} {desc_line}")
    
    return ExitCode.SUCCESS


def list_writers() -> int:
    """List available writers with descriptions.
    
    Displays all registered writer types with their descriptions extracted
    from class docstrings. Useful for discovering what output formats are
    supported.
    
    Returns:
        Exit code (always 0 for success)
        
    Example:
        >>> from fintran.cli.commands import list_writers
        >>> exit_code = list_writers()
        
    Requirements:
        - Requirement 10.3: Provide list-writers subcommand
        - Requirement 10.4: Display available writer types with descriptions
    """
    writers = registry_list_writers()
    
    if not writers:
        print("No writers registered.")
        return ExitCode.SUCCESS
    
    print("Available writers:")
    for name, description in writers.items():
        # Clean up docstring (first line only)
        desc_line = description.split("\\n")[0].strip()
        print(f"  {name:15} {desc_line}")
    
    return ExitCode.SUCCESS


def list_transforms() -> int:
    """List available transforms with descriptions.
    
    Displays all registered transform types with their descriptions extracted
    from class docstrings. Useful for discovering what data transformations
    are available.
    
    Returns:
        Exit code (always 0 for success)
        
    Example:
        >>> from fintran.cli.commands import list_transforms
        >>> exit_code = list_transforms()
        
    Requirements:
        - Requirement 10.5: Provide list-transforms subcommand
        - Requirement 10.6: Display available transform types with descriptions
    """
    transforms = registry_list_transforms()
    
    if not transforms:
        print("No transforms registered.")
        return ExitCode.SUCCESS
    
    print("Available transforms:")
    for name, description in transforms.items():
        # Clean up docstring (first line only)
        desc_line = description.split("\\n")[0].strip()
        print(f"  {name:15} {desc_line}")
    
    return ExitCode.SUCCESS


def check_config(
    config_path: Annotated[Path, Parameter(help="Configuration file path")],
) -> int:
    """Validate configuration file.
    
    Loads and validates a configuration file, checking syntax, structure,
    and that all referenced components (readers, writers, transforms) exist
    in the registry. Displays specific validation errors if found.
    
    Args:
        config_path: Path to configuration file to validate
        
    Returns:
        Exit code (0 for valid config, 6 for invalid config)
        
    Example:
        >>> from pathlib import Path
        >>> from fintran.cli.commands import check_config
        >>> 
        >>> exit_code = check_config(config_path=Path("config.json"))
        
    Requirements:
        - Requirement 11.1: Provide check-config subcommand
        - Requirement 11.2: Validate configuration syntax and structure
        - Requirement 11.3: Return exit code 0 for valid config
        - Requirement 11.4: Display validation errors and return exit code 6 for invalid
        - Requirement 11.5: Verify referenced readers/writers/transforms exist
        - Requirement 11.6: Verify required configuration parameters present
    """
    try:
        # Load configuration (Requirement 11.2)
        config = load_config(config_path)
        
        # Validate configuration (Requirement 11.2, 11.5, 11.6)
        errors = validate_config(config)
        
        if errors:
            # Display validation errors (Requirement 11.4)
            print(f"✗ Configuration validation failed:", file=sys.stderr)
            for error in errors:
                print(f"  {error}", file=sys.stderr)
            return ExitCode.CONFIG_ERROR
        
        # Display success message (Requirement 11.3)
        print(f"✓ Configuration is valid")
        
        # Display configuration summary
        if "reader" in config:
            print(f"  Reader: {config['reader']}")
        if "writer" in config:
            print(f"  Writer: {config['writer']}")
        if "transforms" in config and config["transforms"]:
            print(f"  Transforms: {', '.join(config['transforms'])}")
        
        return ExitCode.SUCCESS
        
    except ConfigError as e:
        # Display configuration errors (Requirement 11.4)
        print(f"✗ Configuration error:", file=sys.stderr)
        print(f"  {e}", file=sys.stderr)
        return ExitCode.CONFIG_ERROR
    except Exception as e:
        handle_error(e, verbose=False)
        return ExitCode.UNEXPECTED_ERROR