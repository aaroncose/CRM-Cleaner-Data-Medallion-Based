"""Entity deduplication for the Silver Layer."""

import json
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from thefuzz import fuzz

from crm_medallion.silver.models import CleanedRecord
from crm_medallion.utils.logging import get_logger

logger = get_logger(__name__)


@dataclass
class EntityGroup:
    """A group of similar entities that may be the same."""

    canonical: str
    variants: list[str]
    similarity: float
    field: str


@dataclass
class DeduplicationResult:
    """Result of the deduplication process."""

    records: list[CleanedRecord]
    auto_merged: dict[str, list[EntityGroup]] = field(default_factory=dict)
    pending_review: dict[str, list[EntityGroup]] = field(default_factory=dict)
    total_auto_merged: int = 0
    total_pending_review: int = 0


class EntityDeduplicator:
    """
    Deduplicates entities across records using fuzzy matching.

    Three-level approach:
    - Level 1 (>=95% similarity): Auto-merge
    - Level 2 (>=75% and <95%): Flag for manual review
    - Level 3 (<75%): Ignore (treat as distinct)
    """

    AUTO_MERGE_THRESHOLD = 95
    REVIEW_THRESHOLD = 75

    def __init__(
        self,
        fields_to_dedupe: list[str],
        auto_merge_threshold: int = AUTO_MERGE_THRESHOLD,
        review_threshold: int = REVIEW_THRESHOLD,
    ):
        """
        Initialize the deduplicator.

        Args:
            fields_to_dedupe: List of field names to deduplicate
            auto_merge_threshold: Minimum similarity for auto-merge (default 95)
            review_threshold: Minimum similarity for review (default 75)
        """
        self.fields_to_dedupe = fields_to_dedupe
        self.auto_merge_threshold = auto_merge_threshold
        self.review_threshold = review_threshold

    def deduplicate(self, records: list[CleanedRecord]) -> DeduplicationResult:
        """
        Deduplicate entities across all records.

        CRITICAL: The number of records NEVER changes. Only field values are updated.

        Args:
            records: List of cleaned records to deduplicate

        Returns:
            DeduplicationResult with updated records and merge info
        """
        original_count = len(records)
        logger.info(f"Starting deduplication for {original_count} records")

        auto_merged: dict[str, list[EntityGroup]] = {}
        pending_review: dict[str, list[EntityGroup]] = {}

        for field_name in self.fields_to_dedupe:
            field_auto, field_review, mappings = self._dedupe_field(records, field_name)

            if field_auto:
                auto_merged[field_name] = field_auto
            if field_review:
                pending_review[field_name] = field_review

            if mappings:
                self._apply_mappings(records, field_name, mappings)

        assert len(records) == original_count, (
            f"CRITICAL: Record count changed during deduplication! "
            f"Original: {original_count}, After: {len(records)}"
        )

        total_auto = sum(len(groups) for groups in auto_merged.values())
        total_review = sum(len(groups) for groups in pending_review.values())

        logger.info(
            f"Deduplication complete: {total_auto} auto-merged groups, "
            f"{total_review} pending review groups"
        )

        return DeduplicationResult(
            records=records,
            auto_merged=auto_merged,
            pending_review=pending_review,
            total_auto_merged=total_auto,
            total_pending_review=total_review,
        )

    def _dedupe_field(
        self,
        records: list[CleanedRecord],
        field_name: str,
    ) -> tuple[list[EntityGroup], list[EntityGroup], dict[str, str]]:
        """
        Deduplicate a single field across all records.

        Returns:
            Tuple of (auto_merged_groups, review_groups, value_mappings)
        """
        values = [
            str(r.data.get(field_name, "")).strip()
            for r in records
            if r.data.get(field_name)
        ]

        if not values:
            return [], [], {}

        value_counts = Counter(values)
        unique_values = list(value_counts.keys())

        logger.debug(f"Field '{field_name}': {len(unique_values)} unique values")

        clusters = self._cluster_similar_values(unique_values, value_counts)

        auto_merged: list[EntityGroup] = []
        pending_review: list[EntityGroup] = []
        mappings: dict[str, str] = {}

        for cluster in clusters:
            if len(cluster["values"]) < 2:
                continue

            similarity = cluster["min_similarity"]
            canonical = cluster["canonical"]
            variants = [v for v in cluster["values"] if v != canonical]

            group = EntityGroup(
                canonical=canonical,
                variants=variants,
                similarity=similarity,
                field=field_name,
            )

            if similarity >= self.auto_merge_threshold:
                auto_merged.append(group)
                for variant in variants:
                    mappings[variant] = canonical
                logger.info(
                    f"[{field_name}] Auto-merge ({similarity:.0f}%): "
                    f"{variants} -> '{canonical}'"
                )
            elif similarity >= self.review_threshold:
                pending_review.append(group)
                logger.info(
                    f"[{field_name}] Pending review ({similarity:.0f}%): "
                    f"{[canonical] + variants}"
                )

        return auto_merged, pending_review, mappings

    def _cluster_similar_values(
        self,
        values: list[str],
        value_counts: Counter,
    ) -> list[dict[str, Any]]:
        """
        Cluster similar values together using fuzzy matching.

        Returns list of clusters, each with:
        - values: list of similar values
        - canonical: the chosen canonical form
        - min_similarity: minimum similarity within the cluster
        """
        if not values:
            return []

        processed = set()
        clusters = []

        sorted_values = sorted(values, key=lambda v: (-value_counts[v], -len(v), v))

        for value in sorted_values:
            if value in processed:
                continue

            cluster_values = [value]
            processed.add(value)
            min_sim = 100

            for other in sorted_values:
                if other in processed:
                    continue

                similarity = fuzz.ratio(value.lower(), other.lower())

                if similarity >= self.review_threshold:
                    cluster_values.append(other)
                    processed.add(other)
                    min_sim = min(min_sim, similarity)

            if len(cluster_values) > 1:
                canonical = self._choose_canonical(cluster_values, value_counts)
                clusters.append({
                    "values": cluster_values,
                    "canonical": canonical,
                    "min_similarity": min_sim,
                })

        return clusters

    def _choose_canonical(
        self,
        values: list[str],
        value_counts: Counter,
    ) -> str:
        """
        Choose the canonical form for a cluster.

        Priority:
        1. Most frequent
        2. Longest (more complete)
        3. Alphabetically first (for consistency)
        """
        return max(
            values,
            key=lambda v: (value_counts[v], len(v), v.lower()),
        )

    def _apply_mappings(
        self,
        records: list[CleanedRecord],
        field_name: str,
        mappings: dict[str, str],
    ) -> None:
        """Apply value mappings to all records."""
        for record in records:
            current_value = str(record.data.get(field_name, "")).strip()
            if current_value in mappings:
                new_value = mappings[current_value]
                record.data[field_name] = new_value
                record.cleaning_log.append(
                    f"DEDUP: {field_name}: '{current_value}' -> '{new_value}'"
                )

    def save_review_file(
        self,
        result: DeduplicationResult,
        output_path: Path,
    ) -> Path | None:
        """
        Save pending review groups to a JSON file.

        Args:
            result: Deduplication result
            output_path: Directory to save the file

        Returns:
            Path to the review file, or None if no pending reviews
        """
        if not result.pending_review:
            return None

        review_entries = []
        for field_name, groups in result.pending_review.items():
            for group in groups:
                review_entries.append({
                    "campo": field_name,
                    "grupo": [group.canonical] + group.variants,
                    "similitud": round(group.similarity / 100, 2),
                    "accion": "pendiente",
                })

        review_file = output_path / "review_entities.json"
        with open(review_file, "w", encoding="utf-8") as f:
            json.dump(review_entries, f, ensure_ascii=False, indent=2)

        logger.info(f"Review file saved: {review_file} ({len(review_entries)} entries)")
        return review_file

    def get_stats(self, result: DeduplicationResult) -> dict[str, Any]:
        """Get statistics about the deduplication."""
        stats = {
            "total_auto_merged_groups": result.total_auto_merged,
            "total_pending_review_groups": result.total_pending_review,
            "by_field": {},
        }

        for field_name in self.fields_to_dedupe:
            auto_count = len(result.auto_merged.get(field_name, []))
            review_count = len(result.pending_review.get(field_name, []))
            stats["by_field"][field_name] = {
                "auto_merged": auto_count,
                "pending_review": review_count,
            }

        return stats
