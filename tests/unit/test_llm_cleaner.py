"""Tests for LLM cleaning functionality."""

import tempfile
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from crm_medallion.config.framework_config import LLMConfig, BronzeConfig, SilverConfig
from crm_medallion.bronze.ingester import CSVIngester
from crm_medallion.silver.models import (
    CleanedRecord,
    FieldCorrection,
    LLMCleaningResult,
    FacturaVenta,
    ProcessingStatus,
)
from crm_medallion.silver.llm_cleaner import LLMCleaner, LLMCorrectionResponse, CorrectionItem
from crm_medallion.silver.layer import SilverLayer
from crm_medallion.utils.errors import LLMError
from crm_medallion.utils.retry import retry_with_backoff, execute_with_retry


class TestLLMCleaningResult:
    def test_creates_successful_result(self):
        record = CleanedRecord(
            row_number=1,
            data={"field": "value"},
            source_dataset_id="test",
        )
        result = LLMCleaningResult(
            original_record=record,
            corrected_data={"field": "corrected"},
            confidence_score=0.9,
            corrections=[
                FieldCorrection(
                    field="field",
                    original_value="value",
                    corrected_value="corrected",
                    reasoning="Fixed typo",
                )
            ],
            llm_reasoning="Corrected spelling error",
            success=True,
        )

        assert result.success is True
        assert result.confidence_score == 0.9
        assert len(result.corrections) == 1

    def test_creates_failed_result(self):
        record = CleanedRecord(
            row_number=1,
            data={"field": "value"},
            source_dataset_id="test",
        )
        result = LLMCleaningResult(
            original_record=record,
            corrected_data={"field": "value"},
            confidence_score=0.0,
            success=False,
            error_message="API error",
        )

        assert result.success is False
        assert result.error_message == "API error"


class TestRetryWithBackoff:
    def test_succeeds_on_first_try(self):
        call_count = 0

        @retry_with_backoff(max_retries=3, initial_delay=0.01)
        def success_func():
            nonlocal call_count
            call_count += 1
            return "success"

        result = success_func()
        assert result == "success"
        assert call_count == 1

    def test_retries_on_failure(self):
        call_count = 0

        @retry_with_backoff(max_retries=3, initial_delay=0.01)
        def fail_then_succeed():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise Exception("Temporary failure")
            return "success"

        result = fail_then_succeed()
        assert result == "success"
        assert call_count == 3

    def test_raises_after_max_retries(self):
        @retry_with_backoff(max_retries=2, initial_delay=0.01)
        def always_fail():
            raise Exception("Always fails")

        with pytest.raises(LLMError) as exc_info:
            always_fail()

        assert "Max retries" in str(exc_info.value)

    def test_execute_with_retry_function(self):
        call_count = 0

        def fail_twice():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise Exception("Fail")
            return "ok"

        result = execute_with_retry(
            fail_twice,
            max_retries=3,
            initial_delay=0.01,
        )
        assert result == "ok"
        assert call_count == 3


class TestLLMCleaner:
    @pytest.fixture
    def llm_config(self):
        return LLMConfig(
            api_key="test-api-key",
            model_name="gpt-4",
            temperature=0.0,
            confidence_threshold=0.7,
            max_retries=2,
            initial_retry_delay=0.01,
        )

    @pytest.fixture
    def sample_record(self):
        return CleanedRecord(
            row_number=1,
            data={
                "num_factura": "FAC-2024-0001",
                "fecha": datetime(2024, 1, 15),
                "proveedor": "Tset Compnay",  # Typo
                "tipo": "Ingreso",
                "categoria": "Marketng",  # Typo
                "importe_base": 1000.0,
                "iva": 210.0,
                "importe_total": 1210.0,
                "estado_factura": "Pagada",
            },
            source_dataset_id="test",
        )

    def test_format_record_data(self, llm_config, sample_record):
        cleaner = LLMCleaner(config=llm_config)
        formatted = cleaner._format_record_data(sample_record)

        assert "num_factura" in formatted
        assert "FAC-2024-0001" in formatted
        assert "proveedor" in formatted

    def test_format_validation_errors(self, llm_config):
        cleaner = LLMCleaner(config=llm_config)
        errors = ["tipo: Invalid value", "fecha: Required field"]
        formatted = cleaner._format_validation_errors(errors)

        assert "tipo: Invalid value" in formatted
        assert "fecha: Required field" in formatted

    @patch("crm_medallion.silver.llm_cleaner.LLMCleaner._call_llm")
    def test_clean_successful(self, mock_call_llm, llm_config, sample_record):
        mock_call_llm.return_value = LLMCorrectionResponse(
            corrected_fields={
                "proveedor": "Test Company",
                "categoria": "Marketing",
            },
            corrections=[
                CorrectionItem(
                    field="proveedor",
                    original="Tset Compnay",
                    corrected="Test Company",
                    reasoning="Fixed typo",
                ),
                CorrectionItem(
                    field="categoria",
                    original="Marketng",
                    corrected="Marketing",
                    reasoning="Fixed typo",
                ),
            ],
            confidence=0.95,
            reasoning="Corrected two spelling errors",
        )

        cleaner = LLMCleaner(config=llm_config)
        result = cleaner.clean(
            sample_record,
            validation_errors=["proveedor: Invalid", "categoria: Invalid"],
        )

        assert result.success is True
        assert result.confidence_score == 0.95
        assert result.corrected_data["proveedor"] == "Test Company"
        assert result.corrected_data["categoria"] == "Marketing"
        assert len(result.corrections) == 2

    @patch("crm_medallion.silver.llm_cleaner.LLMCleaner._call_llm")
    def test_clean_handles_api_error(self, mock_call_llm, llm_config, sample_record):
        mock_call_llm.side_effect = LLMError("API error")

        cleaner = LLMCleaner(config=llm_config)
        result = cleaner.clean(
            sample_record,
            validation_errors=["error"],
        )

        assert result.success is False
        assert "API error" in result.error_message
        assert result.corrected_data == sample_record.data

    def test_should_flag_for_manual_review_low_confidence(self, llm_config):
        cleaner = LLMCleaner(config=llm_config)

        record = CleanedRecord(
            row_number=1,
            data={},
            source_dataset_id="test",
        )
        result = LLMCleaningResult(
            original_record=record,
            corrected_data={},
            confidence_score=0.5,  # Below threshold of 0.7
            success=True,
        )

        assert cleaner.should_flag_for_manual_review(result) is True

    def test_should_not_flag_high_confidence(self, llm_config):
        cleaner = LLMCleaner(config=llm_config)

        record = CleanedRecord(
            row_number=1,
            data={},
            source_dataset_id="test",
        )
        result = LLMCleaningResult(
            original_record=record,
            corrected_data={},
            confidence_score=0.9,  # Above threshold
            success=True,
        )

        assert cleaner.should_flag_for_manual_review(result) is False

    @patch("crm_medallion.silver.llm_cleaner.LLMCleaner._call_llm")
    def test_batch_clean(self, mock_call_llm, llm_config, sample_record):
        mock_call_llm.return_value = LLMCorrectionResponse(
            corrected_fields={"proveedor": "Test"},
            corrections_made=[],
            confidence=0.9,
            overall_reasoning="OK",
        )

        cleaner = LLMCleaner(config=llm_config)
        records = [
            (sample_record, ["error1"]),
            (sample_record, ["error2"]),
        ]

        results = cleaner.batch_clean(records)

        assert len(results) == 2
        assert all(r.success for r in results)


class TestSilverLayerWithLLM:
    @pytest.fixture
    def temp_dirs(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            bronze_path = Path(tmpdir) / "bronze"
            silver_path = Path(tmpdir) / "silver"
            bronze_path.mkdir()
            silver_path.mkdir()
            yield bronze_path, silver_path

    @pytest.fixture
    def llm_config(self):
        return LLMConfig(
            api_key="test-key",
            model_name="gpt-4",
            confidence_threshold=0.7,
            max_retries=1,
            initial_retry_delay=0.01,
        )

    def test_layer_initializes_with_llm_config(self, temp_dirs, llm_config):
        _, silver_path = temp_dirs
        silver_config = SilverConfig(output_path=silver_path)

        layer = SilverLayer(
            schema_model=FacturaVenta,
            config=silver_config,
            llm_config=llm_config,
        )

        assert layer._llm_cleaner is not None

    def test_layer_without_llm_config(self, temp_dirs):
        _, silver_path = temp_dirs
        silver_config = SilverConfig(output_path=silver_path)

        layer = SilverLayer(
            schema_model=FacturaVenta,
            config=silver_config,
        )

        assert layer._llm_cleaner is None

    @patch("crm_medallion.silver.llm_cleaner.LLMCleaner.clean")
    def test_layer_uses_llm_for_invalid_records(
        self, mock_clean, temp_dirs, llm_config
    ):
        bronze_path, silver_path = temp_dirs

        csv_content = """num_factura,fecha,proveedor,tipo,categoria,importe_base,iva,importe_total,estado_factura
FAC-2024-0001,2024-01-15,Test,InvalidType,Cat,100,21,121,Pagada
"""
        csv_path = bronze_path / "test.csv"
        csv_path.write_text(csv_content)

        bronze_config = BronzeConfig(storage_path=bronze_path)
        ingester = CSVIngester(bronze_config)
        bronze_dataset = ingester.ingest(csv_path)

        mock_clean.return_value = LLMCleaningResult(
            original_record=CleanedRecord(
                row_number=2,
                data={},
                source_dataset_id="test",
            ),
            corrected_data={
                "num_factura": "FAC-2024-0001",
                "fecha": datetime(2024, 1, 15),
                "proveedor": "Test",
                "tipo": "Ingreso",
                "categoria": "Cat",
                "importe_base": 100.0,
                "iva": 21.0,
                "importe_total": 121.0,
                "estado_factura": "Pagada",
            },
            confidence_score=0.9,
            corrections=[
                FieldCorrection(
                    field="tipo",
                    original_value="InvalidType",
                    corrected_value="Ingreso",
                    reasoning="Fixed tipo",
                )
            ],
            llm_reasoning="Corrected tipo field",
            success=True,
        )

        silver_config = SilverConfig(output_path=silver_path)
        layer = SilverLayer(
            schema_model=FacturaVenta,
            config=silver_config,
            llm_config=llm_config,
        )

        result = layer.process(bronze_dataset)

        mock_clean.assert_called_once()
        assert result.llm_corrected_records == 1
        assert result.valid_records == 1

    @patch("crm_medallion.silver.llm_cleaner.LLMCleaner.clean")
    def test_layer_flags_low_confidence_for_review(
        self, mock_clean, temp_dirs, llm_config
    ):
        bronze_path, silver_path = temp_dirs

        csv_content = """num_factura,fecha,proveedor,tipo,categoria,importe_base,iva,importe_total,estado_factura
FAC-2024-0001,2024-01-15,Test,InvalidType,Cat,100,21,121,Pagada
"""
        csv_path = bronze_path / "test.csv"
        csv_path.write_text(csv_content)

        bronze_config = BronzeConfig(storage_path=bronze_path)
        ingester = CSVIngester(bronze_config)
        bronze_dataset = ingester.ingest(csv_path)

        mock_clean.return_value = LLMCleaningResult(
            original_record=CleanedRecord(
                row_number=2,
                data={},
                source_dataset_id="test",
            ),
            corrected_data={
                "num_factura": "FAC-2024-0001",
                "fecha": datetime(2024, 1, 15),
                "proveedor": "Test",
                "tipo": "Ingreso",
                "categoria": "Cat",
                "importe_base": 100.0,
                "iva": 21.0,
                "importe_total": 121.0,
                "estado_factura": "Pagada",
            },
            confidence_score=0.5,  # Below threshold
            corrections=[],
            llm_reasoning="Not confident",
            success=True,
        )

        silver_config = SilverConfig(output_path=silver_path)
        layer = SilverLayer(
            schema_model=FacturaVenta,
            config=silver_config,
            llm_config=llm_config,
        )

        result = layer.process(bronze_dataset)

        assert result.manual_review_records == 1
        assert result.llm_corrected_records == 0

    @patch("crm_medallion.silver.llm_cleaner.LLMCleaner.clean")
    def test_layer_handles_llm_failure(self, mock_clean, temp_dirs, llm_config):
        bronze_path, silver_path = temp_dirs

        csv_content = """num_factura,fecha,proveedor,tipo,categoria,importe_base,iva,importe_total,estado_factura
FAC-2024-0001,2024-01-15,Test,InvalidType,Cat,100,21,121,Pagada
"""
        csv_path = bronze_path / "test.csv"
        csv_path.write_text(csv_content)

        bronze_config = BronzeConfig(storage_path=bronze_path)
        ingester = CSVIngester(bronze_config)
        bronze_dataset = ingester.ingest(csv_path)

        mock_clean.return_value = LLMCleaningResult(
            original_record=CleanedRecord(
                row_number=2,
                data={},
                source_dataset_id="test",
            ),
            corrected_data={},
            confidence_score=0.0,
            success=False,
            error_message="API error",
        )

        silver_config = SilverConfig(output_path=silver_path)
        layer = SilverLayer(
            schema_model=FacturaVenta,
            config=silver_config,
            llm_config=llm_config,
        )

        result = layer.process(bronze_dataset)

        assert result.invalid_records == 1
        assert result.llm_corrected_records == 0
