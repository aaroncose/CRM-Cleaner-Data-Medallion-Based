"""Data models for the Bronze Layer."""

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any


@dataclass
class BronzeDataset:
    """Immutable representation of raw ingested data."""

    id: str
    source_file: Path
    ingestion_timestamp: datetime
    encoding: str
    row_count: int
    column_names: list[str]
    storage_path: Path
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if isinstance(self.source_file, str):
            self.source_file = Path(self.source_file)
        if isinstance(self.storage_path, str):
            self.storage_path = Path(self.storage_path)


@dataclass
class BronzeValidationResult:
    """Result of CSV structure validation."""

    is_valid: bool
    row_count: int
    column_count: int
    column_names: list[str]
    warnings: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
