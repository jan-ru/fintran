"""Pipeline orchestration service for the reader → IR → writer flow.

This module implements the Transform_Service that coordinates the complete
pipeline execution: reading input files, applying optional transforms, and
writing output files. The service ensures IR validation at all boundaries
and provides comprehensive error handling with context.

The pipeline follows this flow:
1. Reader: Parse input file → IR DataFrame
2. Validation: Verify reader output conforms to IR schema
3. Transforms: Apply zero or more transformations in sequence
4. Validation: Verify final IR conforms to schema
5. Writer: Serialize IR to output file

All steps include error handling that propagates failures with context about
which pipeline step failed.

Requirements:
    - Requirement 6.1: Accept Reader, Transforms, and Writer as input
    - Requirement 6.2: Invoke Reader with input path
    - Requirement 6.3: Validate Reader output
    - Requirement 6.4: Apply each Transform in sequence
    - Requirement 6.5: Validate final IR
    - Requirement 6.6: Invoke Writer with final IR
    - Requirement 6.7: Propagate errors with context
    - Requirement 7.3: Verify input DataFrame is not modified by Transforms
"""

from collections.abc import Sequence
from pathlib import Path
from typing import Any

import polars as pl

from fintran.core.exceptions import (
    FintranError,
    PipelineError,
    ReaderError,
    TransformError,
    ValidationError,
    WriterError,
)
from fintran.core.protocols import Reader, Transform, Writer
from fintran.core.schema import validate_ir


def execute_pipeline(  # noqa: C901
    reader: Reader,
    writer: Writer,
    input_path: Path,
    output_path: Path,
    transforms: Sequence[Transform] = (),
    **config: Any,
) -> None:
    """Execute the complete reader → transform → writer pipeline.

    This function orchestrates the entire pipeline flow, ensuring IR validation
    at all boundaries and providing comprehensive error handling. The pipeline:

    1. Reads the input file using the provided Reader
    2. Validates the reader output conforms to IR schema
    3. Applies each Transform in sequence (if any)
    4. Validates the final IR conforms to schema
    5. Writes the output file using the provided Writer

    Immutability is enforced: transforms must return new DataFrames rather than
    modifying the input. The pipeline verifies this by checking object identity.

    Args:
        reader: Reader implementation for parsing input files
        writer: Writer implementation for serializing output files
        input_path: Path to the input file to read
        output_path: Path to the output file to write
        transforms: Sequence of transforms to apply between read and write (optional)
        **config: Configuration passed to reader and writer (format-specific options)

    Returns:
        None

    Raises:
        PipelineError: If any step fails, wrapping the original exception with
                      context about which pipeline step failed (read, validate,
                      transform N, write)

    Example:
        >>> from pathlib import Path
        >>> reader = CSVReader()
        >>> writer = ParquetWriter()
        >>> transforms = [CurrencyNormalizer(), FilterPositiveAmounts()]
        >>> execute_pipeline(
        ...     reader=reader,
        ...     writer=writer,
        ...     input_path=Path("input.csv"),
        ...     output_path=Path("output.parquet"),
        ...     transforms=transforms,
        ...     delimiter=","  # Config passed to reader/writer
        ... )

    Requirements:
        - Requirement 6.1: Accept Reader, list of Transforms, and Writer
        - Requirement 6.2: Invoke Reader with input path
        - Requirement 6.3: Validate Reader output using Validation_Service
        - Requirement 6.4: Apply each Transform in sequence to the IR
        - Requirement 6.5: Validate final IR using Validation_Service
        - Requirement 6.6: Invoke Writer with final IR and output path
        - Requirement 6.7: Propagate errors with context about which step failed
        - Requirement 7.3: Verify input DataFrame is not modified by Transforms
    """
    ir: pl.DataFrame | None = None

    try:
        # Step 1: Read input file
        try:
            ir = reader.read(input_path, **config)
        except ReaderError:
            raise  # Re-raise ReaderError as-is
        except Exception as e:
            raise PipelineError(
                f"Pipeline failed at read step: {e}",
                step="read",
                input_path=str(input_path),
            ) from e

        # Step 2: Validate reader output
        try:
            ir = validate_ir(ir)
        except ValidationError as e:
            raise PipelineError(
                f"Pipeline failed: Reader produced invalid IR: {e}",
                step="validate_reader_output",
                input_path=str(input_path),
            ) from e

        # Step 3: Apply transforms in sequence
        for i, transform in enumerate(transforms):
            try:
                # Store original object ID to verify immutability
                original_id = id(ir)

                # Apply transform
                ir = transform.transform(ir)

                # Verify immutability: transform must return a new DataFrame
                if id(ir) == original_id:
                    raise PipelineError(
                        f"Transform {i} violated immutability requirement: "
                        f"returned the same DataFrame instance instead of creating a new one",
                        step=f"transform_{i}",
                        transform_index=i,
                        transform_type=type(transform).__name__,
                    )

            except TransformError:
                raise  # Re-raise TransformError as-is
            except PipelineError:
                raise  # Re-raise PipelineError (immutability violation)
            except Exception as e:
                raise PipelineError(
                    f"Pipeline failed at transform step {i}: {e}",
                    step=f"transform_{i}",
                    transform_index=i,
                    transform_type=type(transform).__name__,
                ) from e

        # Step 4: Validate final IR
        try:
            ir = validate_ir(ir)
        except ValidationError as e:
            raise PipelineError(
                f"Pipeline failed: Final IR is invalid after transforms: {e}",
                step="validate_final_ir",
                transform_count=len(transforms),
            ) from e

        # Step 5: Write output file
        try:
            writer.write(ir, output_path, **config)
        except WriterError:
            raise  # Re-raise WriterError as-is
        except Exception as e:
            raise PipelineError(
                f"Pipeline failed at write step: {e}",
                step="write",
                output_path=str(output_path),
            ) from e

    except PipelineError:
        # Re-raise PipelineError without wrapping
        raise
    except FintranError:
        # Re-raise other fintran errors without wrapping
        raise
    except Exception as e:
        # Wrap unexpected errors
        raise PipelineError(
            f"Pipeline failed with unexpected error: {e}",
            step="unknown",
        ) from e
