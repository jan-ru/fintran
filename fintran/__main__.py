"""CLI entry point for fintran.

Enables invocation via `python -m fintran` or `uv run fintran`.

This module imports the Cyclopts app and invokes it, exiting with the
appropriate exit code based on command execution results.

Requirements: 1.5, 1.6, 1.7
"""

import sys

from fintran.cli.app import app

if __name__ == "__main__":
    exit_code = app()
    sys.exit(exit_code if exit_code is not None else 0)

