"""Tests for Gold Layer components."""

import json
import tempfile
from datetime import datetime
from pathlib import Path

import pandas as pd
import pytest

from crm_medallion.bronze.ingester import CSVIngester
from crm_medallion.config.framework_config import BronzeConfig, SilverConfig, GoldConfig
from crm_medallion.gold.models import (
    FieldStatistics,
    Index,
    IndexEntry,
    GoldDataset,
)
from crm_medallion.gold.aggregator import DataAggregator
from crm_medallion.silver.models import FacturaVenta, SilverDataset
from crm_medallion.silver.layer import SilverLayer


FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"


class TestFieldStatistics:
    def test_creates_with_all_fields(self):
        stats = FieldStatistics(
            field_name="importe_total",
            count=10,
            sum=10000.0,
            mean=1000.0,
            median=950.0,
            min=500.0,
            max=2000.0,
            std=250.0,
        )

        assert stats.field_name == "importe_total"
        assert stats.count == 10
        assert stats.mean == 1000.0


class TestIndex:
    def test_get_existing_key(self):
        entries = {
            "Marketing": IndexEntry(key="Marketing", row_indices=[0, 2, 5], count=3),
            "Tecnología": IndexEntry(key="Tecnología", row_indices=[1, 3], count=2),
        }
        index = Index(field_name="categoria", entries=entries, unique_values=2)

        result = index.get("Marketing")
        assert result == [0, 2, 5]

    def test_get_missing_key(self):
        entries = {}
        index = Index(field_name="categoria", entries=entries, unique_values=0)

        result = index.get("NonExistent")
        assert result == []

    def test_keys(self):
        entries = {
            "A": IndexEntry(key="A", row_indices=[0], count=1),
            "B": IndexEntry(key="B", row_indices=[1], count=1),
        }
        index = Index(field_name="test", entries=entries, unique_values=2)

        keys = index.keys()
        assert set(keys) == {"A", "B"}


class TestGoldDataset:
    def test_creates_with_required_fields(self):
        dataset = GoldDataset(
            id="test-123",
            silver_dataset_id="silver-456",
            storage_path=Path("/tmp/gold.json"),
            aggregation_timestamp=datetime.now(),
            record_count=100,
            statistics={},
            indexes={},
        )

        assert dataset.id == "test-123"
        assert dataset.record_count == 100

    def test_get_statistics(self):
        stats = FieldStatistics(
            field_name="importe_total",
            count=10,
            sum=10000.0,
            mean=1000.0,
            median=950.0,
            min=500.0,
            max=2000.0,
            std=250.0,
        )
        dataset = GoldDataset(
            id="test",
            silver_dataset_id="silver",
            storage_path=Path("/tmp/gold.json"),
            aggregation_timestamp=datetime.now(),
            record_count=10,
            statistics={"importe_total": stats},
            indexes={},
        )

        result = dataset.get_statistics("importe_total")
        assert result is not None
        assert result.mean == 1000.0

        result_missing = dataset.get_statistics("nonexistent")
        assert result_missing is None

    def test_query_by_field(self):
        entries = {
            "Marketing": IndexEntry(key="Marketing", row_indices=[0, 2], count=2),
        }
        index = Index(field_name="categoria", entries=entries, unique_values=1)

        dataset = GoldDataset(
            id="test",
            silver_dataset_id="silver",
            storage_path=Path("/tmp/gold.json"),
            aggregation_timestamp=datetime.now(),
            record_count=3,
            statistics={},
            indexes={"categoria": index},
        )

        result = dataset.query_by_field("categoria", "Marketing")
        assert result == [0, 2]


class TestDataAggregator:
    @pytest.fixture
    def temp_dirs(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            bronze_path = Path(tmpdir) / "bronze"
            silver_path = Path(tmpdir) / "silver"
            gold_path = Path(tmpdir) / "gold"
            bronze_path.mkdir()
            silver_path.mkdir()
            gold_path.mkdir()
            yield bronze_path, silver_path, gold_path

    @pytest.fixture
    def silver_dataset(self, temp_dirs):
        bronze_path, silver_path, _ = temp_dirs

        bronze_config = BronzeConfig(storage_path=bronze_path)
        ingester = CSVIngester(bronze_config)
        bronze_dataset = ingester.ingest(FIXTURES_DIR / "sample_valid.csv")

        silver_config = SilverConfig(output_path=silver_path)
        layer = SilverLayer(schema_model=FacturaVenta, config=silver_config)

        return layer.process(bronze_dataset)

    def test_aggregate_creates_gold_dataset(self, silver_dataset, temp_dirs):
        _, _, gold_path = temp_dirs
        gold_config = GoldConfig(storage_path=gold_path)

        aggregator = DataAggregator(config=gold_config)
        result = aggregator.aggregate(silver_dataset)

        assert result.id is not None
        assert result.silver_dataset_id == silver_dataset.id
        assert result.record_count == silver_dataset.valid_records
        assert result.storage_path.exists()

    def test_aggregate_computes_statistics(self, silver_dataset, temp_dirs):
        _, _, gold_path = temp_dirs
        gold_config = GoldConfig(storage_path=gold_path)

        aggregator = DataAggregator(config=gold_config)
        result = aggregator.aggregate(silver_dataset)

        assert "importe_total" in result.statistics
        stats = result.statistics["importe_total"]
        assert stats.count > 0
        assert stats.sum > 0
        assert stats.mean > 0

    def test_aggregate_builds_indexes(self, silver_dataset, temp_dirs):
        _, _, gold_path = temp_dirs
        gold_config = GoldConfig(storage_path=gold_path)

        aggregator = DataAggregator(config=gold_config)
        result = aggregator.aggregate(silver_dataset)

        assert "categoria" in result.indexes
        assert "tipo" in result.indexes

        categoria_index = result.indexes["categoria"]
        assert categoria_index.unique_values > 0

    def test_aggregate_saves_to_storage(self, silver_dataset, temp_dirs):
        _, _, gold_path = temp_dirs
        gold_config = GoldConfig(storage_path=gold_path)

        aggregator = DataAggregator(config=gold_config)
        result = aggregator.aggregate(silver_dataset)

        with open(result.storage_path, "r") as f:
            data = json.load(f)

        assert "records" in data
        assert "statistics" in data
        assert "indexes" in data
        assert len(data["records"]) == result.record_count

    def test_load_from_gold_dataset(self, silver_dataset, temp_dirs):
        _, _, gold_path = temp_dirs
        gold_config = GoldConfig(storage_path=gold_path)

        aggregator = DataAggregator(config=gold_config)
        gold_dataset = aggregator.aggregate(silver_dataset)

        new_aggregator = DataAggregator(config=gold_config)
        df = new_aggregator.load(gold_dataset)

        assert len(df) == gold_dataset.record_count
        assert "num_factura" in df.columns

    def test_query_by_index(self, silver_dataset, temp_dirs):
        _, _, gold_path = temp_dirs
        gold_config = GoldConfig(storage_path=gold_path)

        aggregator = DataAggregator(config=gold_config)
        gold_dataset = aggregator.aggregate(silver_dataset)

        tipo_index = gold_dataset.indexes.get("tipo")
        if tipo_index and tipo_index.entries:
            first_tipo = list(tipo_index.entries.keys())[0]
            result = aggregator.query("tipo", first_tipo, gold_dataset)
            assert len(result) > 0

    def test_get_summary(self, silver_dataset, temp_dirs):
        _, _, gold_path = temp_dirs
        gold_config = GoldConfig(storage_path=gold_path)

        aggregator = DataAggregator(config=gold_config)
        gold_dataset = aggregator.aggregate(silver_dataset)

        summary = aggregator.get_summary(gold_dataset)

        assert "record_count" in summary
        assert "statistics" in summary
        assert "indexes" in summary
        assert summary["record_count"] == gold_dataset.record_count

    def test_incremental_update(self, temp_dirs):
        bronze_path, silver_path, gold_path = temp_dirs

        csv_content1 = """num_factura,fecha,proveedor,nif_cif,tipo,categoria,importe_base,iva,importe_total,estado_factura
FAC-2024-0001,2024-01-15,Empresa A,B12345678,Ingreso,Marketing,1000.00,210.00,1210.00,Pagada
"""
        csv_path1 = bronze_path / "batch1.csv"
        csv_path1.write_text(csv_content1)

        bronze_config = BronzeConfig(storage_path=bronze_path)
        silver_config = SilverConfig(output_path=silver_path)
        gold_config = GoldConfig(storage_path=gold_path)

        ingester = CSVIngester(bronze_config)
        bronze1 = ingester.ingest(csv_path1)

        layer = SilverLayer(schema_model=FacturaVenta, config=silver_config)
        silver1 = layer.process(bronze1)

        aggregator = DataAggregator(config=gold_config)
        gold1 = aggregator.aggregate(silver1)

        assert gold1.record_count == 1

        csv_content2 = """num_factura,fecha,proveedor,nif_cif,tipo,categoria,importe_base,iva,importe_total,estado_factura
FAC-2024-0002,2024-02-20,Empresa B,A87654321,Gasto,Tecnología,2000.00,420.00,2420.00,Pendiente
"""
        csv_path2 = bronze_path / "batch2.csv"
        csv_path2.write_text(csv_content2)

        bronze2 = ingester.ingest(csv_path2)
        silver2 = layer.process(bronze2)

        gold2 = aggregator.update_incremental(silver2, gold1)

        assert gold2.record_count == 2
        assert gold2.metadata.get("previous_record_count") == 1
        assert gold2.metadata.get("new_records_added") == 1

    def test_incremental_update_deduplicates(self, temp_dirs):
        bronze_path, silver_path, gold_path = temp_dirs

        csv_content = """num_factura,fecha,proveedor,nif_cif,tipo,categoria,importe_base,iva,importe_total,estado_factura
FAC-2024-0001,2024-01-15,Empresa A,B12345678,Ingreso,Marketing,1000.00,210.00,1210.00,Pagada
"""
        csv_path = bronze_path / "batch.csv"
        csv_path.write_text(csv_content)

        bronze_config = BronzeConfig(storage_path=bronze_path)
        silver_config = SilverConfig(output_path=silver_path)
        gold_config = GoldConfig(storage_path=gold_path)

        ingester = CSVIngester(bronze_config)
        bronze = ingester.ingest(csv_path)

        layer = SilverLayer(schema_model=FacturaVenta, config=silver_config)
        silver = layer.process(bronze)

        aggregator = DataAggregator(config=gold_config)
        gold1 = aggregator.aggregate(silver)

        gold2 = aggregator.update_incremental(silver, gold1)

        assert gold2.record_count == 1

    def test_custom_index_fields(self, silver_dataset, temp_dirs):
        _, _, gold_path = temp_dirs
        gold_config = GoldConfig(storage_path=gold_path)

        aggregator = DataAggregator(
            config=gold_config,
            index_fields=["proveedor", "estado_factura"],
        )
        result = aggregator.aggregate(silver_dataset)

        assert "proveedor" in result.indexes
        assert "estado_factura" in result.indexes
        assert "categoria" not in result.indexes

    def test_custom_numeric_fields(self, silver_dataset, temp_dirs):
        _, _, gold_path = temp_dirs
        gold_config = GoldConfig(storage_path=gold_path)

        aggregator = DataAggregator(
            config=gold_config,
            numeric_fields=["importe_base"],
        )
        result = aggregator.aggregate(silver_dataset)

        assert "importe_base" in result.statistics
        assert "importe_total" not in result.statistics
