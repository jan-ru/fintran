"""Output formatting and progress indicators for CLI operations.

This module provides:
- ProgressIndicator: TTY-aware progress indicators for long operations
- handle_error: Formatted error messages with context and optional stack traces
"""

import sys
import traceback
from typing import TextIO


class ProgressIndicator:
    """Simple progress indicator for CLI operations.
    
    Automatically detects TTY to disable progress indicators when output
    is redirected to a file or pipe. Progress messages are written to
    stderr to keep stdout clean for actual output.
    
    Example:
        progress = ProgressIndicator(enabled=not quiet)
        progress.start("Converting file")
        # ... do work ...
        progress.success("Converted successfully")
    """
    
    def __init__(self, enabled: bool = True, stream: TextIO = sys.stderr):
        """Initialize progress indicator.
        
        Args:
            enabled: Whether progress indicators are enabled (default True)
            stream: Output stream for progress messages (default sys.stderr)
        """
        # Disable if explicitly disabled or if output is redirected (not a TTY)
        self.enabled = enabled and stream.isatty()
        self.stream = stream
    
    def start(self, message: str) -> None:
        """Display start message with ellipsis.
        
        Args:
            message: Message to display (e.g., "Converting file")
        """
        if self.enabled:
            self.stream.write(f"{message}... ")
            self.stream.flush()
    
    def success(self, message: str) -> None:
        """Display success message with checkmark.
        
        Args:
            message: Success message to display
        """
        if self.enabled:
            self.stream.write("✓\n")
        # Always print the success message to stdout
        print(message)
    
    def error(self, message: str) -> None:
        """Display error message with cross symbol.
        
        Args:
            message: Error message to display
        """
        if self.enabled:
            self.stream.write("✗\n")
        # Always print error to stderr
        print(f"Error: {message}", file=sys.stderr)


def handle_error(error: Exception, verbose: bool = False) -> None:
    """Format and display error message with context.
    
    Displays error messages to stderr with optional context fields from
    FintranError exceptions. When verbose mode is enabled, also displays
    the full stack trace.
    
    Args:
        error: Exception to display
        verbose: Whether to show stack trace (default False)
    
    Example:
        try:
            # ... operation ...
        except FintranError as e:
            handle_error(e, verbose=True)
    """
    # Display error message to stderr
    print(f"Error: {error}", file=sys.stderr)
    
    # Display context if available (FintranError has context attribute)
    if hasattr(error, "context") and error.context:
        print("Context:", file=sys.stderr)
        for key, value in error.context.items():
            print(f"  {key}: {value}", file=sys.stderr)
    
    # Display stack trace if verbose mode is enabled
    if verbose:
        print("\nStack trace:", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
