"""ValidatingTransform for pipeline integration.

This module defines the ValidatingTransform class that integrates validation into the fintran pipeline.
"""

import polars as pl

from fintran.core.exceptions import ValidationError
from fintran.validation.pipeline import ValidationPipeline
from fintran.validation.report import ValidationReport


class ValidatingTransform:
    """Transform wrapper that runs validation and attaches results to IR metadata.
    
    This class integrates validation into the pipeline as a Transform,
    allowing validation to run at any pipeline stage (pre-transform or post-transform).
    
    Requirements:
        - Requirement 12.1: Provide ValidatingTransform wrapping ValidationPipeline
        - Requirement 12.2: Run validators and attach ValidationReport to IR metadata
        - Requirement 12.3: Support fail-on-error mode (raise ValidationError)
        - Requirement 12.4: Support continue mode (attach report and continue)
        - Requirement 12.5: Support pre/post validation modes
    
    Attributes:
        pipeline: ValidationPipeline to execute
        fail_on_error: If True, raise ValidationError on failures
        metadata_key: Key to use for storing report in IR metadata
    
    Example:
        >>> from fintran.validation.pipeline import ValidationPipeline
        >>> from fintran.validation.business import PositiveAmountsValidator
        >>> 
        >>> validators = [PositiveAmountsValidator(account_patterns=["^4[0-9]{3}"])]
        >>> pipeline = ValidationPipeline(validators)
        >>> transform = ValidatingTransform(pipeline, fail_on_error=True)
        >>> 
        >>> # Use in pipeline
        >>> validated_df = transform.transform(ir_dataframe)
    """
    
    def __init__(
        self,
        pipeline: ValidationPipeline,
        fail_on_error: bool = False,
        metadata_key: str = "validation_report",
    ):
        """Initialize validating transform.
        
        Args:
            pipeline: ValidationPipeline to execute
            fail_on_error: If True, raise ValidationError on failures
            metadata_key: Key to use for storing report in IR metadata
        """
        self.pipeline = pipeline
        self.fail_on_error = fail_on_error
        self.metadata_key = metadata_key
    
    def transform(self, df: pl.DataFrame) -> pl.DataFrame:
        """Run validation and attach report to IR metadata.
        
        Executes the ValidationPipeline on the input DataFrame and attaches
        the ValidationReport to the DataFrame's metadata. If fail_on_error is True
        and validation fails, raises ValidationError with the report.
        
        Args:
            df: IR DataFrame to validate
            
        Returns:
            IR DataFrame with validation report in metadata
            
        Raises:
            ValidationError: If fail_on_error=True and validation fails
            
        Example:
            >>> transform = ValidatingTransform(pipeline, fail_on_error=True)
            >>> try:
            ...     validated_df = transform.transform(ir_dataframe)
            ... except ValidationError as e:
            ...     print(f"Validation failed: {e}")
        """
        # Run validation pipeline
        report = self.pipeline.run(df)
        
        # If fail_on_error is True and validation failed, raise error
        if self.fail_on_error and not report.is_valid():
            raise ValidationError(
                message=f"Validation failed: {report.failed} of {report.total_validators} validators failed",
                validation_report=report,
            )
        
        # Attach report to metadata
        df_with_metadata = attach_validation_report(df, report, self.metadata_key)
        
        return df_with_metadata


def attach_validation_report(
    df: pl.DataFrame,
    report: ValidationReport,
    metadata_key: str = "validation_report",
) -> pl.DataFrame:
    """Attach validation report to IR DataFrame metadata.
    
    Stores the ValidationReport in the DataFrame's metadata for later retrieval.
    Supports multiple validation runs by storing reports in a list.
    
    Requirements:
        - Requirement 22.1: Attach ValidationReport to IR metadata
        - Requirement 22.2: Include timestamp, validator versions, configuration
        - Requirement 22.4: Support multiple validation runs with history
    
    Args:
        df: IR DataFrame to attach metadata to
        report: ValidationReport to attach
        metadata_key: Key to use for storing report
        
    Returns:
        New DataFrame with validation report in metadata
        
    Example:
        >>> df_with_metadata = attach_validation_report(df, report)
        >>> reports = get_validation_reports(df_with_metadata)
        >>> print(f"Found {len(reports)} validation reports")
    """
    # Convert report to JSON for storage
    report_data = report.to_json()
    
    # Get existing validation reports from the input DataFrame
    existing_reports = get_validation_reports(df, metadata_key)
    
    # Append new report to existing reports
    all_reports = existing_reports + [report_data]
    
    # Create new DataFrame (clone to avoid mutation)
    df_with_metadata = df.clone()
    
    # Store metadata as a custom attribute on the DataFrame
    setattr(df_with_metadata, "_validation_metadata", {metadata_key: all_reports})
    
    return df_with_metadata


def get_validation_reports(
    df: pl.DataFrame,
    metadata_key: str = "validation_report",
) -> list[dict]:
    """Retrieve validation reports from IR DataFrame metadata.
    
    Requirements:
        - Requirement 22.3: Provide function to retrieve ValidationReport from metadata
        - Requirement 22.4: Support multiple validation runs with history
    
    Args:
        df: IR DataFrame with validation metadata
        metadata_key: Key used for storing reports
        
    Returns:
        List of validation report dictionaries
        
    Example:
        >>> reports = get_validation_reports(df)
        >>> for report in reports:
        ...     print(f"Timestamp: {report['timestamp']}")
        ...     print(f"Passed: {report['summary']['passed']}")
    """
    # Try to get metadata from custom attribute
    if hasattr(df, "_validation_metadata"):
        metadata = getattr(df, "_validation_metadata")
        return metadata.get(metadata_key, [])
    
    # Try to get from column metadata
    try:
        column_meta = df.get_column("date").meta
        return column_meta.get(metadata_key, [])
    except Exception:
        return []
