"""Tests for Silver Layer components."""

import tempfile
from datetime import datetime
from pathlib import Path

import pytest

from crm_medallion.bronze.models import BronzeDataset
from crm_medallion.bronze.ingester import CSVIngester
from crm_medallion.config.framework_config import BronzeConfig, SilverConfig
from crm_medallion.silver.models import (
    RawRecord,
    CleanedRecord,
    ProcessingStatus,
    TipoFactura,
    FacturaVenta,
)
from crm_medallion.silver.parser import RecordParser
from crm_medallion.silver.cleaner import DataCleaner
from crm_medallion.silver.validator import SchemaValidator
from crm_medallion.silver.layer import SilverLayer
from crm_medallion.silver.rules import (
    WhitespaceStripper,
    CurrencyNormalizer,
    DateNormalizer,
    CaseNormalizer,
    InvoiceNumberNormalizer,
    NifCifNormalizer,
    TipoNormalizer,
    EstadoFacturaNormalizer,
    ConsistencyChecker,
    get_default_cleaning_rules,
)


FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"


class TestWhitespaceStripper:
    def test_strips_leading_whitespace(self):
        rule = WhitespaceStripper()
        value, log = rule.clean("  test", "proveedor")
        assert value == "test"
        assert log is not None

    def test_strips_trailing_whitespace(self):
        rule = WhitespaceStripper()
        value, log = rule.clean("test  ", "proveedor")
        assert value == "test"

    def test_strips_both_sides(self):
        rule = WhitespaceStripper()
        value, log = rule.clean("  test  ", "categoria")
        assert value == "test"

    def test_no_change_if_clean(self):
        rule = WhitespaceStripper()
        value, log = rule.clean("test", "proveedor")
        assert value == "test"
        assert log is None

    def test_applies_to_text_fields(self):
        rule = WhitespaceStripper()
        assert rule.applies_to("proveedor") is True
        assert rule.applies_to("categoria") is True
        assert rule.applies_to("importe_total") is False


class TestCurrencyNormalizer:
    def test_removes_euro_symbol(self):
        rule = CurrencyNormalizer()
        value, log = rule.clean("1000€", "importe_total")
        assert value == 1000.0

    def test_removes_eur_text(self):
        rule = CurrencyNormalizer()
        value, log = rule.clean("1000 EUR", "importe_total")
        assert value == 1000.0

    def test_converts_comma_decimal(self):
        rule = CurrencyNormalizer()
        value, log = rule.clean("1000,50", "importe_total")
        assert value == 1000.50

    def test_handles_european_format(self):
        rule = CurrencyNormalizer()
        value, log = rule.clean("1.234,56", "importe_total")
        assert value == 1234.56

    def test_handles_already_float(self):
        rule = CurrencyNormalizer()
        value, log = rule.clean(1000.50, "importe_total")
        assert value == 1000.50

    def test_applies_to_numeric_fields(self):
        rule = CurrencyNormalizer()
        assert rule.applies_to("importe_base") is True
        assert rule.applies_to("iva") is True
        assert rule.applies_to("proveedor") is False


class TestDateNormalizer:
    def test_normalizes_dd_mm_yyyy_slash(self):
        rule = DateNormalizer()
        value, log = rule.clean("15/01/2024", "fecha")
        assert isinstance(value, datetime)
        assert value.year == 2024
        assert value.month == 1
        assert value.day == 15

    def test_normalizes_dd_mm_yyyy_dash(self):
        rule = DateNormalizer()
        value, log = rule.clean("15-01-2024", "fecha")
        assert isinstance(value, datetime)
        assert value.day == 15

    def test_normalizes_spanish_format(self):
        rule = DateNormalizer()
        value, log = rule.clean("15 de enero de 2024", "fecha")
        assert isinstance(value, datetime)
        assert value.year == 2024
        assert value.month == 1
        assert value.day == 15

    def test_normalizes_spanish_march(self):
        rule = DateNormalizer()
        value, log = rule.clean("10 de marzo de 2024", "fecha")
        assert isinstance(value, datetime)
        assert value.month == 3

    def test_preserves_iso_format(self):
        rule = DateNormalizer()
        value, log = rule.clean("2024-01-15", "fecha")
        assert isinstance(value, datetime)
        assert value.year == 2024

    def test_handles_datetime_input(self):
        rule = DateNormalizer()
        dt = datetime(2024, 1, 15)
        value, log = rule.clean(dt, "fecha")
        assert value == dt

    def test_applies_to_date_fields(self):
        rule = DateNormalizer()
        assert rule.applies_to("fecha") is True
        assert rule.applies_to("proveedor") is False


class TestCaseNormalizer:
    def test_normalizes_uppercase(self):
        rule = CaseNormalizer()
        value, log = rule.clean("MARKETING", "categoria")
        assert value == "Marketing"

    def test_normalizes_lowercase(self):
        rule = CaseNormalizer()
        value, log = rule.clean("marketing", "categoria")
        assert value == "Marketing"

    def test_preserves_title_case(self):
        rule = CaseNormalizer()
        value, log = rule.clean("Marketing", "categoria")
        assert value == "Marketing"
        assert log is None


class TestInvoiceNumberNormalizer:
    def test_normalizes_spaces(self):
        rule = InvoiceNumberNormalizer()
        value, log = rule.clean("FAC 2024 0001", "num_factura")
        assert value == "FAC-2024-0001"

    def test_normalizes_slashes(self):
        rule = InvoiceNumberNormalizer()
        value, log = rule.clean("FAC/2024/0001", "num_factura")
        assert value == "FAC-2024-0001"

    def test_pads_number(self):
        rule = InvoiceNumberNormalizer()
        value, log = rule.clean("FAC-2024-1", "num_factura")
        assert value == "FAC-2024-0001"

    def test_adds_fac_prefix(self):
        rule = InvoiceNumberNormalizer()
        value, log = rule.clean("2024-0001", "num_factura")
        assert value == "FAC-2024-0001"


class TestNifCifNormalizer:
    def test_removes_dashes(self):
        rule = NifCifNormalizer()
        value, log = rule.clean("B-12345678", "nif_cif")
        assert value == "B12345678"

    def test_removes_spaces(self):
        rule = NifCifNormalizer()
        value, log = rule.clean("B 12345678", "nif_cif")
        assert value == "B12345678"

    def test_uppercases(self):
        rule = NifCifNormalizer()
        value, log = rule.clean("b12345678", "nif_cif")
        assert value == "B12345678"

    def test_handles_nif_format(self):
        rule = NifCifNormalizer()
        value, log = rule.clean("12345678A", "nif_cif")
        assert value == "12345678A"


class TestTipoNormalizer:
    def test_corrects_ingreso_lowercase(self):
        rule = TipoNormalizer()
        value, log = rule.clean("ingreso", "tipo")
        assert value == "Ingreso"

    def test_corrects_ingreso_misspelling(self):
        rule = TipoNormalizer()
        value, log = rule.clean("Ingrso", "tipo")
        assert value == "Ingreso"

    def test_corrects_gasto_lowercase(self):
        rule = TipoNormalizer()
        value, log = rule.clean("gasto", "tipo")
        assert value == "Gasto"

    def test_corrects_gasto_misspelling(self):
        rule = TipoNormalizer()
        value, log = rule.clean("Gatso", "tipo")
        assert value == "Gasto"

    def test_preserves_correct_value(self):
        rule = TipoNormalizer()
        value, log = rule.clean("Ingreso", "tipo")
        assert value == "Ingreso"
        assert log is None


class TestEstadoFacturaNormalizer:
    def test_corrects_pagada_misspelling(self):
        rule = EstadoFacturaNormalizer()
        value, log = rule.clean("Pagda", "estado_factura")
        assert value == "Pagada"

    def test_corrects_pendiente_misspelling(self):
        rule = EstadoFacturaNormalizer()
        value, log = rule.clean("pendiete", "estado_factura")
        assert value == "Pendiente"

    def test_corrects_vencida_misspelling(self):
        rule = EstadoFacturaNormalizer()
        value, log = rule.clean("vencda", "estado_factura")
        assert value == "Vencida"


class TestConsistencyChecker:
    def test_detects_paid_with_pending_amount(self):
        checker = ConsistencyChecker()
        data = {"estado_factura": "Pagada", "importe_pendiente": 100.0}
        warnings = checker.check_consistency(data)
        assert len(warnings) == 1
        assert "Pagada" in warnings[0]

    def test_detects_pending_with_zero_amount(self):
        checker = ConsistencyChecker()
        data = {"estado_factura": "Pendiente", "importe_pendiente": 0}
        warnings = checker.check_consistency(data)
        assert len(warnings) == 1
        assert "Pendiente" in warnings[0]

    def test_no_warning_for_consistent_data(self):
        checker = ConsistencyChecker()
        data = {"estado_factura": "Pagada", "importe_pendiente": 0}
        warnings = checker.check_consistency(data)
        assert len(warnings) == 0


class TestDataCleaner:
    def test_applies_multiple_rules(self):
        rules = get_default_cleaning_rules()
        cleaner = DataCleaner(rules=rules)

        raw = RawRecord(
            row_number=1,
            data={
                "proveedor": "  TEST COMPANY  ",
                "importe_total": "1000€",
                "fecha": "15/01/2024",
            },
            source_dataset_id="test",
        )

        cleaned = cleaner.clean(raw)

        assert cleaned.data["proveedor"] == "Test Company"
        assert cleaned.data["importe_total"] == 1000.0
        assert isinstance(cleaned.data["fecha"], datetime)
        assert len(cleaned.cleaning_log) > 0

    def test_register_custom_rule(self):
        cleaner = DataCleaner(rules=[])
        cleaner.register_rule(WhitespaceStripper())
        assert len(cleaner.rules) == 1


class TestImportePendienteCalculation:
    """Tests for automatic importe_pendiente calculation."""

    def test_pagada_has_zero_pendiente(self):
        rules = get_default_cleaning_rules()
        cleaner = DataCleaner(rules=rules)
        raw = RawRecord(
            row_number=1,
            data={
                "estado_factura": "Pagada",
                "importe_total": "1000.00",
            },
            source_dataset_id="test",
        )

        cleaned = cleaner.clean(raw)

        assert cleaned.data["importe_pendiente"] == 0.0

    def test_pendiente_has_full_amount(self):
        rules = get_default_cleaning_rules()
        cleaner = DataCleaner(rules=rules)
        raw = RawRecord(
            row_number=1,
            data={
                "estado_factura": "Pendiente",
                "importe_total": "1500.00",
            },
            source_dataset_id="test",
        )

        cleaned = cleaner.clean(raw)

        assert cleaned.data["importe_pendiente"] == 1500.0

    def test_vencida_has_full_amount(self):
        rules = get_default_cleaning_rules()
        cleaner = DataCleaner(rules=rules)
        raw = RawRecord(
            row_number=1,
            data={
                "estado_factura": "Vencida",
                "importe_total": "2000.00",
            },
            source_dataset_id="test",
        )

        cleaned = cleaner.clean(raw)

        assert cleaned.data["importe_pendiente"] == 2000.0

    def test_parcialmente_pagada_has_half_amount(self):
        rules = get_default_cleaning_rules()
        cleaner = DataCleaner(rules=rules)
        raw = RawRecord(
            row_number=1,
            data={
                "estado_factura": "Parcialmente pagada",
                "importe_total": "1000.00",
            },
            source_dataset_id="test",
        )

        cleaned = cleaner.clean(raw)

        assert cleaned.data["importe_pendiente"] == 500.0

    def test_existing_pendiente_not_overwritten(self):
        rules = get_default_cleaning_rules()
        cleaner = DataCleaner(rules=rules)
        raw = RawRecord(
            row_number=1,
            data={
                "estado_factura": "Pendiente",
                "importe_total": "1000.00",
                "importe_pendiente": "750.00",
            },
            source_dataset_id="test",
        )

        cleaned = cleaner.clean(raw)

        # Existing value should be preserved (after currency normalization)
        assert cleaned.data["importe_pendiente"] == 750.0

    def test_missing_pendiente_gets_calculated(self):
        import math
        rules = get_default_cleaning_rules()
        cleaner = DataCleaner(rules=rules)
        raw = RawRecord(
            row_number=1,
            data={
                "estado_factura": "Pagada",
                "importe_total": "1000.00",
            },
            source_dataset_id="test",
        )

        cleaned = cleaner.clean(raw)

        # Should be calculated as 0.0 for Pagada
        assert cleaned.data["importe_pendiente"] == 0.0
        assert not math.isnan(cleaned.data["importe_pendiente"])


class TestRecordParser:
    @pytest.fixture
    def bronze_dataset(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            bronze_config = BronzeConfig(storage_path=Path(tmpdir))
            ingester = CSVIngester(bronze_config)
            dataset = ingester.ingest(FIXTURES_DIR / "sample_valid.csv")
            yield dataset

    def test_parses_all_records(self, bronze_dataset):
        parser = RecordParser()
        records = list(parser.parse(bronze_dataset))

        assert len(records) == 3
        assert all(isinstance(r, RawRecord) for r in records)

    def test_records_have_correct_row_numbers(self, bronze_dataset):
        parser = RecordParser()
        records = list(parser.parse(bronze_dataset))

        assert records[0].row_number == 2
        assert records[1].row_number == 3
        assert records[2].row_number == 4

    def test_records_have_source_dataset_id(self, bronze_dataset):
        parser = RecordParser()
        records = list(parser.parse(bronze_dataset))

        for record in records:
            assert record.source_dataset_id == bronze_dataset.id


class TestSchemaValidator:
    def test_validates_correct_record(self):
        validator = SchemaValidator(FacturaVenta)

        cleaned = CleanedRecord(
            row_number=1,
            data={
                "num_factura": "FAC-2024-0001",
                "fecha": datetime(2024, 1, 15),
                "proveedor": "Test Company",
                "nif_cif": "B12345678",
                "tipo": "Ingreso",
                "categoria": "Marketing",
                "importe_base": 1000.0,
                "iva": 210.0,
                "importe_total": 1210.0,
                "estado_factura": "Pagada",
            },
            source_dataset_id="test",
        )

        result = validator.validate(cleaned)
        assert result.success is True
        assert len(result.errors) == 0

    def test_reports_missing_required_field(self):
        validator = SchemaValidator(FacturaVenta)

        cleaned = CleanedRecord(
            row_number=1,
            data={
                "num_factura": "FAC-2024-0001",
            },
            source_dataset_id="test",
        )

        result = validator.validate(cleaned)
        assert result.success is False
        assert len(result.errors) > 0

    def test_reports_invalid_enum_value(self):
        validator = SchemaValidator(FacturaVenta)

        cleaned = CleanedRecord(
            row_number=1,
            data={
                "num_factura": "FAC-2024-0001",
                "fecha": datetime(2024, 1, 15),
                "proveedor": "Test",
                "tipo": "InvalidType",
                "categoria": "Test",
                "importe_base": 100.0,
                "iva": 21.0,
                "importe_total": 121.0,
                "estado_factura": "Pagada",
            },
            source_dataset_id="test",
        )

        result = validator.validate(cleaned)
        assert result.success is False

    def test_to_validated_record_valid(self):
        validator = SchemaValidator(FacturaVenta)

        cleaned = CleanedRecord(
            row_number=1,
            data={
                "num_factura": "FAC-2024-0001",
                "fecha": datetime(2024, 1, 15),
                "proveedor": "Test",
                "tipo": "Ingreso",
                "categoria": "Test",
                "importe_base": 100.0,
                "iva": 21.0,
                "importe_total": 121.0,
                "estado_factura": "Pagada",
            },
            source_dataset_id="test",
        )

        result = validator.validate(cleaned)
        validated = validator.to_validated_record(cleaned, result)

        assert validated.status == ProcessingStatus.VALID


class TestSilverLayer:
    @pytest.fixture
    def temp_dirs(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            bronze_path = Path(tmpdir) / "bronze"
            silver_path = Path(tmpdir) / "silver"
            bronze_path.mkdir()
            silver_path.mkdir()
            yield bronze_path, silver_path

    @pytest.fixture
    def bronze_dataset(self, temp_dirs):
        bronze_path, _ = temp_dirs
        bronze_config = BronzeConfig(storage_path=bronze_path)
        ingester = CSVIngester(bronze_config)
        return ingester.ingest(FIXTURES_DIR / "sample_valid.csv")

    def test_processes_valid_records(self, bronze_dataset, temp_dirs):
        _, silver_path = temp_dirs
        silver_config = SilverConfig(output_path=silver_path)

        layer = SilverLayer(
            schema_model=FacturaVenta,
            config=silver_config,
        )

        result = layer.process(bronze_dataset)

        assert result.total_records == 3
        assert result.valid_records == 3
        assert result.invalid_records == 0
        assert result.clean_csv_path.exists()

    def test_writes_clean_csv(self, bronze_dataset, temp_dirs):
        _, silver_path = temp_dirs
        silver_config = SilverConfig(output_path=silver_path)

        layer = SilverLayer(
            schema_model=FacturaVenta,
            config=silver_config,
        )

        result = layer.process(bronze_dataset)

        with open(result.clean_csv_path, "r") as f:
            content = f.read()

        assert "num_factura" in content
        assert "FAC-2024-0001" in content

    def test_logs_validation_errors(self, temp_dirs):
        bronze_path, silver_path = temp_dirs

        csv_content = """num_factura,fecha,proveedor,tipo,categoria,importe_base,iva,importe_total,estado_factura
FAC-2024-0001,2024-01-15,Test,InvalidType,Cat,100,21,121,Pagada
"""
        csv_path = bronze_path / "test.csv"
        csv_path.write_text(csv_content)

        bronze_config = BronzeConfig(storage_path=bronze_path)
        ingester = CSVIngester(bronze_config)
        bronze_dataset = ingester.ingest(csv_path)

        silver_config = SilverConfig(output_path=silver_path)
        layer = SilverLayer(schema_model=FacturaVenta, config=silver_config)

        result = layer.process(bronze_dataset)

        assert result.invalid_records == 1
        assert len(result.validation_errors_log) == 1

    def test_applies_cleaning_rules(self, temp_dirs):
        bronze_path, silver_path = temp_dirs

        csv_content = """num_factura,fecha,proveedor,tipo,categoria,importe_base,iva,importe_total,estado_factura
FAC-2024-0001,15/01/2024,  TEST COMPANY  ,ingreso,  MARKETING  ,1000€,210,1210,pagada
"""
        csv_path = bronze_path / "dirty.csv"
        csv_path.write_text(csv_content)

        bronze_config = BronzeConfig(storage_path=bronze_path)
        ingester = CSVIngester(bronze_config)
        bronze_dataset = ingester.ingest(csv_path)

        silver_config = SilverConfig(output_path=silver_path)
        layer = SilverLayer(schema_model=FacturaVenta, config=silver_config)

        result = layer.process(bronze_dataset)

        assert result.valid_records == 1

        with open(result.clean_csv_path, "r") as f:
            content = f.read()

        assert "Test Company" in content
        assert "2024-01-15" in content
        assert "Ingreso" in content

    def test_calculates_processing_time(self, bronze_dataset, temp_dirs):
        _, silver_path = temp_dirs
        silver_config = SilverConfig(output_path=silver_path)

        layer = SilverLayer(schema_model=FacturaVenta, config=silver_config)
        result = layer.process(bronze_dataset)

        assert result.processing_time_seconds > 0
