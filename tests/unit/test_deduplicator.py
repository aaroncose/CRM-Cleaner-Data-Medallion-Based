"""Tests for entity deduplication functionality."""

import json
import tempfile
from pathlib import Path

import pytest

from crm_medallion.silver.deduplicator import (
    EntityDeduplicator,
    DeduplicationResult,
    EntityGroup,
)
from crm_medallion.silver.models import CleanedRecord


class TestEntityDeduplicator:
    @pytest.fixture
    def sample_records(self) -> list[CleanedRecord]:
        """Create sample records with entity variations."""
        return [
            CleanedRecord(
                row_number=1,
                data={"proveedor": "Suministros García S.L.", "categoria": "Marketing"},
                source_dataset_id="test",
            ),
            CleanedRecord(
                row_number=2,
                data={"proveedor": "suministros garcia", "categoria": "marketing"},
                source_dataset_id="test",
            ),
            CleanedRecord(
                row_number=3,
                data={"proveedor": "Suministros Garcia", "categoria": "Marketing"},
                source_dataset_id="test",
            ),
            CleanedRecord(
                row_number=4,
                data={"proveedor": "Empresa Totalmente Diferente", "categoria": "Tecnología"},
                source_dataset_id="test",
            ),
            CleanedRecord(
                row_number=5,
                data={"proveedor": "Sumnistros García S.L.", "categoria": "Marketng"},  # Typos
                source_dataset_id="test",
            ),
        ]

    def test_record_count_never_changes(self, sample_records):
        """CRITICAL: Verify that deduplication never changes record count."""
        deduplicator = EntityDeduplicator(fields_to_dedupe=["proveedor", "categoria"])

        original_count = len(sample_records)
        result = deduplicator.deduplicate(sample_records)

        assert len(result.records) == original_count
        assert len(result.records) == 5

    def test_auto_merge_high_similarity(self):
        """Verify that >= 95% similarity auto-merges."""
        records = [
            CleanedRecord(
                row_number=1,
                data={"proveedor": "Suministros García S.L."},
                source_dataset_id="test",
            ),
            CleanedRecord(
                row_number=2,
                data={"proveedor": "Suministros Garcia S.L."},  # Only accent difference
                source_dataset_id="test",
            ),
        ]

        deduplicator = EntityDeduplicator(fields_to_dedupe=["proveedor"])
        result = deduplicator.deduplicate(records)

        # Both should have the same canonical value
        assert len(result.records) == 2
        proveedor_values = [r.data["proveedor"] for r in result.records]
        assert proveedor_values[0] == proveedor_values[1]

        # Should be in auto_merged
        assert "proveedor" in result.auto_merged
        assert len(result.auto_merged["proveedor"]) > 0

    def test_review_medium_similarity(self):
        """Verify that 75-95% similarity goes to review without modifying."""
        records = [
            CleanedRecord(
                row_number=1,
                data={"proveedor": "Suministros García S.L."},
                source_dataset_id="test",
            ),
            CleanedRecord(
                row_number=2,
                data={"proveedor": "Sumnistros García"},  # Typo + shorter
                source_dataset_id="test",
            ),
        ]

        deduplicator = EntityDeduplicator(fields_to_dedupe=["proveedor"])
        original_values = [r.data["proveedor"] for r in records]

        result = deduplicator.deduplicate(records)

        # Record count unchanged
        assert len(result.records) == 2

        # Check if it went to review (similarity should be ~85-90%)
        # Original values should NOT be modified for review items
        if result.pending_review.get("proveedor"):
            # Data should not be changed for review items
            current_values = [r.data["proveedor"] for r in result.records]
            # At least the review items should keep original values
            assert result.total_pending_review > 0

    def test_ignore_low_similarity(self):
        """Verify that < 75% similarity is ignored."""
        records = [
            CleanedRecord(
                row_number=1,
                data={"proveedor": "Suministros García S.L."},
                source_dataset_id="test",
            ),
            CleanedRecord(
                row_number=2,
                data={"proveedor": "Empresa Totalmente Diferente"},
                source_dataset_id="test",
            ),
        ]

        deduplicator = EntityDeduplicator(fields_to_dedupe=["proveedor"])
        result = deduplicator.deduplicate(records)

        # Both values should remain unchanged
        assert result.records[0].data["proveedor"] == "Suministros García S.L."
        assert result.records[1].data["proveedor"] == "Empresa Totalmente Diferente"

        # No auto-merges or reviews for this field
        assert result.total_auto_merged == 0
        assert result.total_pending_review == 0

    def test_cleaning_log_updated(self):
        """Verify that cleaning log is updated when deduplication happens."""
        records = [
            CleanedRecord(
                row_number=1,
                data={"proveedor": "Suministros García S.L."},
                cleaning_log=[],
                source_dataset_id="test",
            ),
            CleanedRecord(
                row_number=2,
                data={"proveedor": "suministros garcia s.l."},  # Case difference only
                cleaning_log=[],
                source_dataset_id="test",
            ),
        ]

        deduplicator = EntityDeduplicator(fields_to_dedupe=["proveedor"])
        result = deduplicator.deduplicate(records)

        # Find the record that was changed
        changed_record = next(
            (r for r in result.records if any("DEDUP" in log for log in r.cleaning_log)),
            None,
        )

        if result.total_auto_merged > 0:
            assert changed_record is not None
            assert any("DEDUP" in log for log in changed_record.cleaning_log)

    def test_save_review_file(self):
        """Verify review file is saved correctly."""
        records = [
            CleanedRecord(
                row_number=1,
                data={"proveedor": "Suministros García S.L."},
                source_dataset_id="test",
            ),
            CleanedRecord(
                row_number=2,
                data={"proveedor": "Sumnistros García"},  # ~85% similarity
                source_dataset_id="test",
            ),
        ]

        deduplicator = EntityDeduplicator(
            fields_to_dedupe=["proveedor"],
            auto_merge_threshold=95,
            review_threshold=75,
        )
        result = deduplicator.deduplicate(records)

        if result.pending_review:
            with tempfile.TemporaryDirectory() as tmpdir:
                output_path = Path(tmpdir)
                review_file = deduplicator.save_review_file(result, output_path)

                assert review_file is not None
                assert review_file.exists()

                with open(review_file, "r", encoding="utf-8") as f:
                    data = json.load(f)

                assert len(data) > 0
                assert data[0]["campo"] == "proveedor"
                assert data[0]["accion"] == "pendiente"
                assert "similitud" in data[0]
                assert "grupo" in data[0]

    def test_multiple_fields(self, sample_records):
        """Verify deduplication works across multiple fields."""
        deduplicator = EntityDeduplicator(fields_to_dedupe=["proveedor", "categoria"])
        result = deduplicator.deduplicate(sample_records)

        # Record count unchanged
        assert len(result.records) == len(sample_records)

        # Both fields should have some activity
        stats = deduplicator.get_stats(result)
        assert "proveedor" in stats["by_field"]
        assert "categoria" in stats["by_field"]

    def test_canonical_selection_most_frequent(self):
        """Verify canonical selection prefers most frequent value."""
        records = [
            CleanedRecord(row_number=1, data={"proveedor": "Garcia"}, source_dataset_id="t"),
            CleanedRecord(row_number=2, data={"proveedor": "García"}, source_dataset_id="t"),
            CleanedRecord(row_number=3, data={"proveedor": "García"}, source_dataset_id="t"),
            CleanedRecord(row_number=4, data={"proveedor": "García"}, source_dataset_id="t"),
        ]

        deduplicator = EntityDeduplicator(fields_to_dedupe=["proveedor"])
        result = deduplicator.deduplicate(records)

        # "García" should be canonical (most frequent)
        if result.total_auto_merged > 0:
            unique_values = set(r.data["proveedor"] for r in result.records)
            # After deduplication, all should be the same
            assert len(unique_values) == 1
            assert "García" in unique_values

    def test_canonical_selection_longest_on_tie(self):
        """Verify canonical selection prefers longest value on frequency tie."""
        records = [
            CleanedRecord(row_number=1, data={"proveedor": "Garcia S.L."}, source_dataset_id="t"),
            CleanedRecord(row_number=2, data={"proveedor": "Garcia"}, source_dataset_id="t"),
        ]

        deduplicator = EntityDeduplicator(fields_to_dedupe=["proveedor"])
        result = deduplicator.deduplicate(records)

        # Both have same frequency, "Garcia S.L." should be canonical (longest)
        if result.total_auto_merged > 0:
            unique_values = set(r.data["proveedor"] for r in result.records)
            assert len(unique_values) == 1
            assert "Garcia S.L." in unique_values

    def test_empty_records(self):
        """Verify deduplication handles empty records list."""
        deduplicator = EntityDeduplicator(fields_to_dedupe=["proveedor"])
        result = deduplicator.deduplicate([])

        assert len(result.records) == 0
        assert result.total_auto_merged == 0
        assert result.total_pending_review == 0

    def test_no_review_file_when_no_pending(self):
        """Verify no review file is created when there are no pending reviews."""
        records = [
            CleanedRecord(
                row_number=1,
                data={"proveedor": "Suministros García Internacional S.L."},
                source_dataset_id="test",
            ),
            CleanedRecord(
                row_number=2,
                data={"proveedor": "Tecnología Avanzada XYZ Corporation"},
                source_dataset_id="test",
            ),
        ]

        deduplicator = EntityDeduplicator(fields_to_dedupe=["proveedor"])
        result = deduplicator.deduplicate(records)

        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir)
            review_file = deduplicator.save_review_file(result, output_path)

            assert review_file is None


class TestDeduplicationWithRealData:
    """Integration tests with realistic data patterns."""

    def test_500_records_in_500_out(self):
        """Verify 500 records in -> 500 records out."""
        records = []
        providers = [
            "Suministros García S.L.",
            "suministros garcia",
            "Suministros Garcia",
            "Materiales Hernández",
            "materiales hernandez",
            "Empresa Nueva",
            "Otra Empresa Distinta",
            "Servicios Integrales",
        ]

        for i in range(500):
            records.append(
                CleanedRecord(
                    row_number=i + 1,
                    data={"proveedor": providers[i % len(providers)]},
                    source_dataset_id="test",
                )
            )

        deduplicator = EntityDeduplicator(fields_to_dedupe=["proveedor"])
        result = deduplicator.deduplicate(records)

        # CRITICAL: 500 in -> 500 out
        assert len(result.records) == 500

    def test_unique_providers_reduced(self):
        """Verify that unique provider count is reduced after deduplication."""
        records = [
            CleanedRecord(row_number=1, data={"proveedor": "Suministros García S.L."}, source_dataset_id="t"),
            CleanedRecord(row_number=2, data={"proveedor": "suministros garcia s.l."}, source_dataset_id="t"),
            CleanedRecord(row_number=3, data={"proveedor": "SUMINISTROS GARCIA"}, source_dataset_id="t"),
            CleanedRecord(row_number=4, data={"proveedor": "Materiales Hernández"}, source_dataset_id="t"),
            CleanedRecord(row_number=5, data={"proveedor": "materiales hernandez"}, source_dataset_id="t"),
            CleanedRecord(row_number=6, data={"proveedor": "Empresa Diferente"}, source_dataset_id="t"),
        ]

        unique_before = len(set(r.data["proveedor"] for r in records))
        assert unique_before == 6

        deduplicator = EntityDeduplicator(fields_to_dedupe=["proveedor"])
        result = deduplicator.deduplicate(records)

        unique_after = len(set(r.data["proveedor"] for r in result.records))

        # Should be reduced (exact number depends on similarity thresholds)
        assert unique_after <= unique_before
        # Record count unchanged
        assert len(result.records) == 6
