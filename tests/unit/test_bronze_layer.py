"""Tests for Bronze Layer components."""

import tempfile
from datetime import datetime
from pathlib import Path

import pytest

from crm_medallion.bronze.models import BronzeDataset, BronzeValidationResult
from crm_medallion.bronze.ingester import CSVIngester
from crm_medallion.config.framework_config import BronzeConfig
from crm_medallion.utils.errors import FrameworkError


FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"


class TestBronzeDataset:
    def test_creates_with_required_fields(self):
        dataset = BronzeDataset(
            id="test-123",
            source_file=Path("/tmp/test.csv"),
            ingestion_timestamp=datetime.now(),
            encoding="utf-8",
            row_count=100,
            column_names=["col1", "col2"],
            storage_path=Path("/tmp/bronze/test.csv"),
        )
        assert dataset.id == "test-123"
        assert dataset.row_count == 100

    def test_converts_string_paths_to_path_objects(self):
        dataset = BronzeDataset(
            id="test-123",
            source_file="/tmp/test.csv",
            ingestion_timestamp=datetime.now(),
            encoding="utf-8",
            row_count=100,
            column_names=["col1"],
            storage_path="/tmp/bronze/test.csv",
        )
        assert isinstance(dataset.source_file, Path)
        assert isinstance(dataset.storage_path, Path)

    def test_default_metadata_is_empty_dict(self):
        dataset = BronzeDataset(
            id="test-123",
            source_file=Path("/tmp/test.csv"),
            ingestion_timestamp=datetime.now(),
            encoding="utf-8",
            row_count=100,
            column_names=[],
            storage_path=Path("/tmp/bronze/test.csv"),
        )
        assert dataset.metadata == {}


class TestBronzeValidationResult:
    def test_valid_result(self):
        result = BronzeValidationResult(
            is_valid=True,
            row_count=10,
            column_count=5,
            column_names=["a", "b", "c", "d", "e"],
        )
        assert result.is_valid is True
        assert result.warnings == []
        assert result.errors == []

    def test_invalid_result_with_errors(self):
        result = BronzeValidationResult(
            is_valid=False,
            row_count=0,
            column_count=0,
            column_names=[],
            errors=["File is empty"],
        )
        assert result.is_valid is False
        assert "File is empty" in result.errors


class TestCSVIngester:
    @pytest.fixture
    def temp_storage(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            yield Path(tmpdir)

    @pytest.fixture
    def ingester(self, temp_storage):
        config = BronzeConfig(storage_path=temp_storage)
        return CSVIngester(config)

    def test_detect_encoding_utf8(self, ingester):
        csv_path = FIXTURES_DIR / "sample_valid.csv"
        encoding = ingester.detect_encoding(csv_path)
        assert encoding in ["utf-8", "ascii"]

    def test_detect_encoding_latin1(self, ingester):
        csv_path = FIXTURES_DIR / "sample_latin1.csv"
        encoding = ingester.detect_encoding(csv_path)
        assert encoding in ["latin-1", "iso-8859-1", "windows-1252"]

    def test_validate_csv_structure_valid_file(self, ingester):
        csv_path = FIXTURES_DIR / "sample_valid.csv"
        result = ingester.validate_csv_structure(csv_path)

        assert result.is_valid is True
        assert result.row_count == 3
        assert result.column_count == 10
        assert "num_factura" in result.column_names
        assert len(result.errors) == 0

    def test_validate_csv_structure_inconsistent_columns(self, ingester):
        csv_path = FIXTURES_DIR / "sample_inconsistent_columns.csv"
        result = ingester.validate_csv_structure(csv_path)

        assert result.is_valid is True
        assert len(result.warnings) > 0
        assert any("expected" in w.lower() and "column" in w.lower() for w in result.warnings)

    def test_validate_csv_structure_empty_file(self, ingester):
        csv_path = FIXTURES_DIR / "sample_empty.csv"
        result = ingester.validate_csv_structure(csv_path)

        assert result.is_valid is False
        assert len(result.errors) > 0

    def test_ingest_valid_file(self, ingester, temp_storage):
        csv_path = FIXTURES_DIR / "sample_valid.csv"
        dataset = ingester.ingest(csv_path)

        assert dataset.id is not None
        assert dataset.source_file == csv_path
        assert dataset.row_count == 3
        assert len(dataset.column_names) == 10
        assert dataset.storage_path.exists()
        assert dataset.storage_path.parent == temp_storage

    def test_ingest_preserves_original_data(self, ingester):
        csv_path = FIXTURES_DIR / "sample_valid.csv"

        with open(csv_path, "rb") as f:
            original_content = f.read()

        dataset = ingester.ingest(csv_path)

        with open(dataset.storage_path, "rb") as f:
            stored_content = f.read()

        assert stored_content == original_content

    def test_ingest_file_not_found(self, ingester):
        with pytest.raises(FileNotFoundError) as exc_info:
            ingester.ingest(Path("/nonexistent/file.csv"))

        assert "not found" in str(exc_info.value).lower()

    def test_ingest_empty_file_raises_error(self, ingester):
        csv_path = FIXTURES_DIR / "sample_empty.csv"

        with pytest.raises(FrameworkError) as exc_info:
            ingester.ingest(csv_path)

        assert "validation failed" in str(exc_info.value).lower()

    def test_ingest_sets_metadata(self, ingester):
        csv_path = FIXTURES_DIR / "sample_valid.csv"
        dataset = ingester.ingest(csv_path)

        assert "original_filename" in dataset.metadata
        assert dataset.metadata["original_filename"] == "sample_valid.csv"
        assert "file_size_bytes" in dataset.metadata
        assert dataset.metadata["file_size_bytes"] > 0

    def test_ingest_dirty_file_with_warnings(self, ingester):
        csv_path = FIXTURES_DIR / "sample_dirty.csv"
        dataset = ingester.ingest(csv_path)

        assert dataset.row_count == 3
        assert dataset.storage_path.exists()

    def test_ingest_latin1_file(self, ingester):
        csv_path = FIXTURES_DIR / "sample_latin1.csv"
        dataset = ingester.ingest(csv_path)

        assert dataset.encoding in ["latin-1", "iso-8859-1", "windows-1252"]
        assert dataset.row_count == 3

    def test_creates_storage_directory_if_not_exists(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            storage_path = Path(tmpdir) / "nested" / "bronze" / "storage"
            config = BronzeConfig(storage_path=storage_path)
            ingester = CSVIngester(config)

            assert storage_path.exists()

    def test_ingestion_timestamp_is_set(self, ingester):
        csv_path = FIXTURES_DIR / "sample_valid.csv"
        before = datetime.now()
        dataset = ingester.ingest(csv_path)
        after = datetime.now()

        assert before <= dataset.ingestion_timestamp <= after

    def test_ingest_generates_unique_ids(self, ingester):
        csv_path = FIXTURES_DIR / "sample_valid.csv"

        dataset1 = ingester.ingest(csv_path)
        dataset2 = ingester.ingest(csv_path)

        assert dataset1.id != dataset2.id
        assert dataset1.storage_path != dataset2.storage_path
