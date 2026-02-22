"""Exit code constants for CLI commands.

This module defines standard exit codes for different error conditions,
following Unix conventions where 0 indicates success and non-zero values
indicate different types of failures.

Exit codes:
    0: SUCCESS - Operation completed successfully
    1: UNEXPECTED_ERROR - Unexpected/unhandled exception
    2: VALIDATION_ERROR - IR schema validation failure
    3: READER_ERROR - Input file reading/parsing failure
    4: WRITER_ERROR - Output file writing/serialization failure
    5: TRANSFORM_ERROR - Transform operation failure
    6: CONFIG_ERROR - Configuration file or argument error

Requirements:
    - Requirement 9.1: Return 0 for successful operations
    - Requirement 9.2: Return 2 for validation errors
    - Requirement 9.3: Return 3 for reader errors
    - Requirement 9.4: Return 4 for writer errors
    - Requirement 9.5: Return 5 for transform errors
    - Requirement 9.6: Return 6 for configuration errors
    - Requirement 9.7: Return 1 for unexpected errors
"""


class ExitCode:
    """Standard exit codes for CLI commands.
    
    These exit codes enable scripts to handle different failure types
    appropriately. All codes follow Unix conventions.
    
    Example:
        >>> from fintran.cli.exit_codes import ExitCode
        >>> import sys
        >>> 
        >>> try:
        ...     # ... operation ...
        ...     sys.exit(ExitCode.SUCCESS)
        ... except ValidationError:
        ...     sys.exit(ExitCode.VALIDATION_ERROR)
    """
    
    SUCCESS = 0
    """Operation completed successfully."""
    
    UNEXPECTED_ERROR = 1
    """Unexpected or unhandled exception occurred."""
    
    VALIDATION_ERROR = 2
    """IR DataFrame validation failed (schema violation)."""
    
    READER_ERROR = 3
    """Input file reading or parsing failed."""
    
    WRITER_ERROR = 4
    """Output file writing or serialization failed."""
    
    TRANSFORM_ERROR = 5
    """Transform operation failed."""
    
    CONFIG_ERROR = 6
    """Configuration file or argument error."""

