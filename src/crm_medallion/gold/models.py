"""Data models for the Gold Layer."""

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any


@dataclass
class FieldStatistics:
    """Statistics for a numeric field."""

    field_name: str
    count: int
    sum: float
    mean: float
    median: float
    min: float
    max: float
    std: float


@dataclass
class IndexEntry:
    """Entry in an index mapping a key to row indices."""

    key: Any
    row_indices: list[int]
    count: int


@dataclass
class Index:
    """Index for a field enabling fast lookups."""

    field_name: str
    entries: dict[Any, IndexEntry]
    unique_values: int

    def get(self, key: Any) -> list[int]:
        """Get row indices for a key."""
        entry = self.entries.get(key)
        return entry.row_indices if entry else []

    def keys(self) -> list[Any]:
        """Get all unique keys in the index."""
        return list(self.entries.keys())


@dataclass
class GoldDataset:
    """Output of Gold layer aggregation."""

    id: str
    silver_dataset_id: str
    storage_path: Path
    aggregation_timestamp: datetime
    record_count: int
    statistics: dict[str, FieldStatistics]
    indexes: dict[str, Index]
    column_names: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if isinstance(self.storage_path, str):
            self.storage_path = Path(self.storage_path)

    def get_statistics(self, field_name: str) -> FieldStatistics | None:
        """Get statistics for a specific field."""
        return self.statistics.get(field_name)

    def get_index(self, field_name: str) -> Index | None:
        """Get index for a specific field."""
        return self.indexes.get(field_name)

    def query_by_field(self, field_name: str, value: Any) -> list[int]:
        """Query row indices by field value using index."""
        index = self.indexes.get(field_name)
        if index:
            return index.get(value)
        return []
