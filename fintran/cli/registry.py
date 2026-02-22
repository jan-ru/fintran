"""Component registry for readers, writers, and transforms.

This module provides a registry pattern for dynamic discovery and instantiation
of pipeline components. Components can be registered by name and retrieved
at runtime, enabling the CLI to support arbitrary reader/writer/transform
combinations without hardcoding dependencies.

The registry supports:
- Registration of reader, writer, and transform implementations
- Retrieval of component instances by name
- Listing available components with descriptions
- Error handling for unknown component types

Requirements:
    - Requirement 10.1: Provide list-readers subcommand
    - Requirement 10.2: Display available reader types with descriptions
    - Requirement 10.3: Provide list-writers subcommand
    - Requirement 10.4: Display available writer types with descriptions
    - Requirement 10.5: Provide list-transforms subcommand
    - Requirement 10.6: Display available transform types with descriptions
    - Requirement 14.5: Display available readers for invalid reader type
    - Requirement 14.6: Display available writers for invalid writer type
    - Requirement 14.7: Display available transforms for invalid transform type
"""

from typing import Any

from fintran.core.protocols import Reader, Writer, Transform


# Registry dictionaries mapping component names to their classes
READERS: dict[str, type[Reader]] = {}
WRITERS: dict[str, type[Writer]] = {}
TRANSFORMS: dict[str, type[Transform]] = {}


def register_reader(name: str, cls: type[Reader]) -> None:
    """Register a reader implementation.
    
    Args:
        name: Name to register the reader under (e.g., "csv", "json")
        cls: Reader class to register
        
    Example:
        >>> from fintran.cli.registry import register_reader
        >>> 
        >>> class CSVReader:
        ...     def read(self, path, **config):
        ...         # ... implementation ...
        ...         pass
        >>> 
        >>> register_reader("csv", CSVReader)
    """
    READERS[name] = cls


def register_writer(name: str, cls: type[Writer]) -> None:
    """Register a writer implementation.
    
    Args:
        name: Name to register the writer under (e.g., "parquet", "json")
        cls: Writer class to register
        
    Example:
        >>> from fintran.cli.registry import register_writer
        >>> 
        >>> class ParquetWriter:
        ...     def write(self, df, path, **config):
        ...         # ... implementation ...
        ...         pass
        >>> 
        >>> register_writer("parquet", ParquetWriter)
    """
    WRITERS[name] = cls


def register_transform(name: str, cls: type[Transform]) -> None:
    """Register a transform implementation.
    
    Args:
        name: Name to register the transform under (e.g., "currency_normalizer")
        cls: Transform class to register
        
    Example:
        >>> from fintran.cli.registry import register_transform
        >>> 
        >>> class CurrencyNormalizer:
        ...     def transform(self, df):
        ...         # ... implementation ...
        ...         pass
        >>> 
        >>> register_transform("currency_normalizer", CurrencyNormalizer)
    """
    TRANSFORMS[name] = cls


def get_reader(name: str) -> Reader:
    """Get reader instance by name.
    
    Args:
        name: Name of the reader to retrieve
        
    Returns:
        Instance of the requested reader
        
    Raises:
        KeyError: If reader name is not registered, with message listing
                 available readers
                 
    Example:
        >>> from fintran.cli.registry import get_reader
        >>> reader = get_reader("csv")
        >>> ir = reader.read(Path("input.csv"))
    """
    if name not in READERS:
        available = ", ".join(sorted(READERS.keys())) if READERS else "none"
        raise KeyError(f"Unknown reader '{name}'. Available: {available}")
    return READERS[name]()


def get_writer(name: str) -> Writer:
    """Get writer instance by name.
    
    Args:
        name: Name of the writer to retrieve
        
    Returns:
        Instance of the requested writer
        
    Raises:
        KeyError: If writer name is not registered, with message listing
                 available writers
                 
    Example:
        >>> from fintran.cli.registry import get_writer
        >>> writer = get_writer("parquet")
        >>> writer.write(ir, Path("output.parquet"))
    """
    if name not in WRITERS:
        available = ", ".join(sorted(WRITERS.keys())) if WRITERS else "none"
        raise KeyError(f"Unknown writer '{name}'. Available: {available}")
    return WRITERS[name]()


def get_transform(name: str) -> Transform:
    """Get transform instance by name.
    
    Args:
        name: Name of the transform to retrieve
        
    Returns:
        Instance of the requested transform
        
    Raises:
        KeyError: If transform name is not registered, with message listing
                 available transforms
                 
    Example:
        >>> from fintran.cli.registry import get_transform
        >>> transform = get_transform("currency_normalizer")
        >>> ir = transform.transform(ir)
    """
    if name not in TRANSFORMS:
        available = ", ".join(sorted(TRANSFORMS.keys())) if TRANSFORMS else "none"
        raise KeyError(f"Unknown transform '{name}'. Available: {available}")
    return TRANSFORMS[name]()


def list_readers() -> dict[str, str]:
    """List available readers with descriptions.
    
    Returns:
        Dictionary mapping reader names to their descriptions (from docstrings)
        
    Example:
        >>> from fintran.cli.registry import list_readers
        >>> readers = list_readers()
        >>> for name, desc in readers.items():
        ...     print(f"{name}: {desc}")
    """
    return {
        name: cls.__doc__ or "No description"
        for name, cls in sorted(READERS.items())
    }


def list_writers() -> dict[str, str]:
    """List available writers with descriptions.
    
    Returns:
        Dictionary mapping writer names to their descriptions (from docstrings)
        
    Example:
        >>> from fintran.cli.registry import list_writers
        >>> writers = list_writers()
        >>> for name, desc in writers.items():
        ...     print(f"{name}: {desc}")
    """
    return {
        name: cls.__doc__ or "No description"
        for name, cls in sorted(WRITERS.items())
    }


def list_transforms() -> dict[str, str]:
    """List available transforms with descriptions.
    
    Returns:
        Dictionary mapping transform names to their descriptions (from docstrings)
        
    Example:
        >>> from fintran.cli.registry import list_transforms
        >>> transforms = list_transforms()
        >>> for name, desc in transforms.items():
        ...     print(f"{name}: {desc}")
    """
    return {
        name: cls.__doc__ or "No description"
        for name, cls in sorted(TRANSFORMS.items())
    }

