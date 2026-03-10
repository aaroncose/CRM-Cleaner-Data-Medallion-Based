"""Data aggregator for the Gold Layer."""

import json
import statistics
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from crm_medallion.config.framework_config import GoldConfig
from crm_medallion.gold.models import (
    FieldStatistics,
    GoldDataset,
    Index,
    IndexEntry,
    SegmentedStatistics,
)
from crm_medallion.silver.models import SilverDataset
from crm_medallion.utils.errors import FrameworkError
from crm_medallion.utils.hooks import HookExecutor, HookPhase, HookRegistry, HookResult
from crm_medallion.utils.logging import get_logger

logger = get_logger(__name__)


class DataAggregator:
    """Aggregates Silver layer data for analytics."""

    DEFAULT_INDEX_FIELDS = ["fecha", "categoria", "proveedor", "tipo", "estado_factura"]
    NUMERIC_FIELDS = ["importe_base", "iva", "importe_total", "importe_pendiente"]
    SEGMENT_FIELDS = ["tipo", "estado_factura", "categoria", "proveedor"]

    def __init__(
        self,
        config: GoldConfig | None = None,
        index_fields: list[str] | None = None,
        numeric_fields: list[str] | None = None,
        hook_registry: HookRegistry | None = None,
    ):
        """
        Initialize aggregator.

        Args:
            config: Gold layer configuration
            index_fields: Fields to create indexes for
            numeric_fields: Fields to compute statistics for
            hook_registry: Optional hook registry for extensibility
        """
        self.config = config or GoldConfig()
        self.index_fields = index_fields or self.DEFAULT_INDEX_FIELDS
        self.numeric_fields = numeric_fields or self.NUMERIC_FIELDS

        self._hook_registry = hook_registry
        self._hook_executor = HookExecutor(hook_registry) if hook_registry else None

        self._ensure_storage_directory()
        self._data: pd.DataFrame | None = None
        self._current_dataset: GoldDataset | None = None

    def _ensure_storage_directory(self) -> None:
        """Create storage directory if it doesn't exist."""
        self.config.storage_path.mkdir(parents=True, exist_ok=True)

    def _execute_hook(
        self,
        phase: HookPhase,
        data: SilverDataset | GoldDataset,
        metadata: dict | None = None,
    ) -> tuple[HookResult, SilverDataset | GoldDataset]:
        """Execute hooks if registry is configured."""
        if self._hook_executor is None:
            return HookResult.CONTINUE, data

        return self._hook_executor.execute_hooks(
            layer="gold",
            phase=phase,
            data=data,
            metadata=metadata or {},
        )

    def aggregate(self, silver_dataset: SilverDataset) -> GoldDataset:
        """
        Aggregate clean data from Silver layer.

        Args:
            silver_dataset: The Silver dataset to aggregate

        Returns:
            GoldDataset with aggregated data, statistics, and indexes
        """
        pre_result, silver_dataset = self._execute_hook(
            HookPhase.PRE,
            silver_dataset,
            {"operation": "aggregate"},
        )

        if pre_result == HookResult.SKIP:
            logger.info("Pre-hook requested skip, returning empty dataset")
            return GoldDataset(
                id=str(uuid.uuid4()),
                silver_dataset_id=silver_dataset.id,
                storage_path=self.config.storage_path / "skipped",
                aggregation_timestamp=datetime.now(),
                record_count=0,
                statistics={},
                indexes={},
                metadata={"skipped": True},
            )

        if pre_result == HookResult.ABORT:
            raise FrameworkError(
                "Gold layer processing aborted by hook",
                context={"phase": "pre"},
            )

        logger.info(f"Aggregating Silver dataset: {silver_dataset.id}")

        df = pd.read_csv(
            silver_dataset.clean_csv_path,
            parse_dates=["fecha"] if "fecha" in pd.read_csv(silver_dataset.clean_csv_path, nrows=0).columns else None,
        )

        self._data = df
        dataset_id = str(uuid.uuid4())

        stats = self._compute_statistics(df)
        indexes = self._build_indexes(df)
        segmented_stats = self._compute_segmented_statistics(df)

        storage_filename = f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{dataset_id[:8]}_gold.json"
        storage_path = self.config.storage_path / storage_filename

        self._save_aggregation(df, stats, indexes, storage_path, segmented_stats)

        gold_dataset = GoldDataset(
            id=dataset_id,
            silver_dataset_id=silver_dataset.id,
            storage_path=storage_path,
            aggregation_timestamp=datetime.now(),
            record_count=len(df),
            statistics=stats,
            indexes=indexes,
            segmented_statistics=segmented_stats,
            column_names=list(df.columns),
            metadata={
                "source_csv": str(silver_dataset.clean_csv_path),
                "silver_valid_records": silver_dataset.valid_records,
            },
        )

        self._current_dataset = gold_dataset

        logger.info(
            f"Gold layer aggregation complete: {len(df)} records, "
            f"{len(stats)} statistics, {len(indexes)} indexes"
        )

        post_result, gold_dataset = self._execute_hook(
            HookPhase.POST,
            gold_dataset,
            {"operation": "aggregate"},
        )

        if post_result == HookResult.ABORT:
            raise FrameworkError(
                "Gold layer processing aborted by post-hook",
                context={"phase": "post"},
            )

        return gold_dataset

    def _compute_statistics(self, df: pd.DataFrame) -> dict[str, FieldStatistics]:
        """Compute summary statistics for numeric fields."""
        stats = {}

        for field in self.numeric_fields:
            if field not in df.columns:
                continue

            series = pd.to_numeric(df[field], errors="coerce").dropna()

            if len(series) == 0:
                continue

            values = series.tolist()

            stats[field] = FieldStatistics(
                field_name=field,
                count=len(values),
                sum=sum(values),
                mean=statistics.mean(values),
                median=statistics.median(values),
                min=min(values),
                max=max(values),
                std=statistics.stdev(values) if len(values) > 1 else 0.0,
            )

            logger.debug(
                f"Statistics for {field}: mean={stats[field].mean:.2f}, "
                f"sum={stats[field].sum:.2f}"
            )

        return stats

    def _compute_segmented_statistics(
        self, df: pd.DataFrame
    ) -> dict[str, SegmentedStatistics]:
        """Compute statistics segmented by categorical fields."""
        segmented = {}

        for segment_field in self.SEGMENT_FIELDS:
            if segment_field not in df.columns:
                continue

            segments = {}
            grouped = df.groupby(segment_field, dropna=False)

            for segment_value, group in grouped:
                display_key = str(segment_value) if not pd.isna(segment_value) else "N/A"
                segment_stats = {
                    "count": len(group),
                }

                for numeric_field in self.numeric_fields:
                    if numeric_field not in group.columns:
                        continue

                    series = pd.to_numeric(group[numeric_field], errors="coerce").dropna()
                    if len(series) > 0:
                        segment_stats[f"{numeric_field}_sum"] = float(series.sum())
                        segment_stats[f"{numeric_field}_mean"] = float(series.mean())

                segments[display_key] = segment_stats

            segmented[segment_field] = SegmentedStatistics(
                segment_field=segment_field,
                segments=segments,
            )

            logger.debug(
                f"Segmented statistics for {segment_field}: {len(segments)} segments"
            )

        return segmented

    def _build_indexes(self, df: pd.DataFrame) -> dict[str, Index]:
        """Build indexes for common query patterns."""
        indexes = {}

        for field in self.index_fields:
            if field not in df.columns:
                continue

            entries = {}
            grouped = df.groupby(field, dropna=False)

            for key, group in grouped:
                display_key = self._normalize_key(key)
                entries[display_key] = IndexEntry(
                    key=display_key,
                    row_indices=group.index.tolist(),
                    count=len(group),
                )

            indexes[field] = Index(
                field_name=field,
                entries=entries,
                unique_values=len(entries),
            )

            logger.debug(f"Index for {field}: {len(entries)} unique values")

        return indexes

    def _normalize_key(self, key: Any) -> Any:
        """Normalize index key for consistent storage."""
        if pd.isna(key):
            return None
        if hasattr(key, "isoformat"):
            return key.isoformat()[:10]
        return key

    def _save_aggregation(
        self,
        df: pd.DataFrame,
        stats: dict[str, FieldStatistics],
        indexes: dict[str, Index],
        storage_path: Path,
        segmented_stats: dict[str, SegmentedStatistics] | None = None,
    ) -> None:
        """Save aggregation results to storage."""
        data = {
            "records": df.to_dict(orient="records"),
            "statistics": {
                name: {
                    "field_name": s.field_name,
                    "count": s.count,
                    "sum": s.sum,
                    "mean": s.mean,
                    "median": s.median,
                    "min": s.min,
                    "max": s.max,
                    "std": s.std,
                }
                for name, s in stats.items()
            },
            "indexes": {
                name: {
                    "field_name": idx.field_name,
                    "unique_values": idx.unique_values,
                    "entries": {
                        str(k): {"key": str(v.key), "count": v.count, "row_indices": v.row_indices}
                        for k, v in idx.entries.items()
                    },
                }
                for name, idx in indexes.items()
            },
            "segmented_statistics": {
                name: {
                    "segment_field": seg.segment_field,
                    "segments": seg.segments,
                }
                for name, seg in (segmented_stats or {}).items()
            },
        }

        with open(storage_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, default=str)

    def load(self, gold_dataset: GoldDataset) -> pd.DataFrame:
        """Load data from a Gold dataset."""
        with open(gold_dataset.storage_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        df = pd.DataFrame(data["records"])
        self._data = df
        self._current_dataset = gold_dataset
        return df

    def update_incremental(
        self,
        silver_dataset: SilverDataset,
        existing_dataset: GoldDataset,
    ) -> GoldDataset:
        """
        Incrementally update Gold dataset with new Silver data.

        Args:
            silver_dataset: New Silver dataset to add
            existing_dataset: Existing Gold dataset to update

        Returns:
            Updated GoldDataset
        """
        logger.info(f"Incremental update of Gold dataset: {existing_dataset.id}")

        existing_df = self.load(existing_dataset)

        new_df = pd.read_csv(
            silver_dataset.clean_csv_path,
            parse_dates=["fecha"] if "fecha" in existing_df.columns else None,
        )

        combined_df = pd.concat([existing_df, new_df], ignore_index=True)
        combined_df = combined_df.drop_duplicates(subset=["num_factura"], keep="last")

        self._data = combined_df

        stats = self._compute_statistics(combined_df)
        indexes = self._build_indexes(combined_df)

        storage_filename = f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{existing_dataset.id[:8]}_gold_updated.json"
        storage_path = self.config.storage_path / storage_filename

        self._save_aggregation(combined_df, stats, indexes, storage_path)

        updated_dataset = GoldDataset(
            id=existing_dataset.id,
            silver_dataset_id=silver_dataset.id,
            storage_path=storage_path,
            aggregation_timestamp=datetime.now(),
            record_count=len(combined_df),
            statistics=stats,
            indexes=indexes,
            column_names=list(combined_df.columns),
            metadata={
                **existing_dataset.metadata,
                "last_update": datetime.now().isoformat(),
                "previous_record_count": existing_dataset.record_count,
                "new_records_added": len(combined_df) - existing_dataset.record_count,
            },
        )

        self._current_dataset = updated_dataset

        logger.info(
            f"Incremental update complete: {existing_dataset.record_count} -> {len(combined_df)} records"
        )

        return updated_dataset

    def query(
        self,
        field: str,
        value: Any,
        gold_dataset: GoldDataset | None = None,
    ) -> pd.DataFrame:
        """
        Query records by field value using index.

        Args:
            field: Field to query by
            value: Value to match
            gold_dataset: Optional Gold dataset (uses current if not provided)

        Returns:
            DataFrame of matching records
        """
        if gold_dataset and gold_dataset != self._current_dataset:
            self.load(gold_dataset)

        if self._data is None:
            return pd.DataFrame()

        dataset = gold_dataset or self._current_dataset
        if dataset is None:
            return pd.DataFrame()

        row_indices = dataset.query_by_field(field, value)

        if row_indices:
            return self._data.iloc[row_indices]

        return self._data[self._data[field] == value]

    def get_summary(self, gold_dataset: GoldDataset | None = None) -> dict[str, Any]:
        """Get a summary of the Gold dataset."""
        dataset = gold_dataset or self._current_dataset
        if dataset is None:
            return {}

        return {
            "record_count": dataset.record_count,
            "columns": dataset.column_names,
            "statistics": {
                name: {
                    "mean": s.mean,
                    "sum": s.sum,
                    "min": s.min,
                    "max": s.max,
                }
                for name, s in dataset.statistics.items()
            },
            "indexes": {
                name: {
                    "unique_values": idx.unique_values,
                    "top_values": sorted(
                        [(k, e.count) for k, e in idx.entries.items()],
                        key=lambda x: x[1],
                        reverse=True,
                    )[:5],
                }
                for name, idx in dataset.indexes.items()
            },
        }
