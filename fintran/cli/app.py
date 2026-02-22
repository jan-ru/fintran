"""Cyclopts application and command routing for fintran CLI.

This module defines the main Cyclopts application and registers all subcommands
for the fintran financial document transformation tool.

The CLI provides the following commands:
- convert: Transform files between formats
- validate: Validate files against IR schema
- inspect: Display IR structure and metadata
- batch: Process multiple files
- list-readers: List available reader types
- list-writers: List available writer types
- list-transforms: List available transform types
- check-config: Validate configuration files

Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7
"""

from cyclopts import App

from fintran.cli import commands

# Create the main application
app = App(
    name="fintran",
    help="Financial document transformation tool",
    version="0.1.4",
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

