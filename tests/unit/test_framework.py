"""Tests for the Framework orchestrator."""

import tempfile
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from crm_medallion import Framework, FrameworkConfig, PipelineResult
from crm_medallion.config.framework_config import (
    BronzeConfig,
    GoldConfig,
    LLMConfig,
    SilverConfig,
)
from crm_medallion.utils.errors import ConfigurationError, FrameworkError


FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"


class TestPipelineResult:
    def test_to_dict(self):
        bronze_dataset = MagicMock()
        bronze_dataset.id = "bronze-123"
        bronze_dataset.row_count = 100
        bronze_dataset.encoding = "utf-8"

        silver_dataset = MagicMock()
        silver_dataset.id = "silver-456"
        silver_dataset.total_records = 100
        silver_dataset.valid_records = 95
        silver_dataset.invalid_records = 5
        silver_dataset.llm_corrected_records = 0
        silver_dataset.manual_review_records = 0
        silver_dataset.processing_time_seconds = 1.5

        gold_dataset = MagicMock()
        gold_dataset.id = "gold-789"
        gold_dataset.record_count = 95
        gold_dataset.statistics = {"field1": MagicMock()}
        gold_dataset.indexes = {"idx1": MagicMock(), "idx2": MagicMock()}

        result = PipelineResult(
            bronze_dataset=bronze_dataset,
            silver_dataset=silver_dataset,
            gold_dataset=gold_dataset,
            start_time=datetime(2024, 1, 1, 10, 0, 0),
            end_time=datetime(2024, 1, 1, 10, 0, 5),
            total_processing_time_seconds=5.0,
        )

        result_dict = result.to_dict()

        assert result_dict["pipeline"]["total_processing_time_seconds"] == 5.0
        assert result_dict["bronze"]["dataset_id"] == "bronze-123"
        assert result_dict["bronze"]["row_count"] == 100
        assert result_dict["silver"]["valid_records"] == 95
        assert result_dict["silver"]["invalid_records"] == 5
        assert result_dict["gold"]["record_count"] == 95
        assert result_dict["gold"]["statistics_count"] == 1
        assert result_dict["gold"]["index_count"] == 2


class TestFramework:
    @pytest.fixture
    def temp_dirs(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            bronze_path = Path(tmpdir) / "bronze"
            silver_path = Path(tmpdir) / "silver"
            gold_path = Path(tmpdir) / "gold"
            bronze_path.mkdir()
            silver_path.mkdir()
            gold_path.mkdir()
            yield {
                "bronze": bronze_path,
                "silver": silver_path,
                "gold": gold_path,
            }

    @pytest.fixture
    def basic_config(self, temp_dirs):
        return FrameworkConfig(
            bronze=BronzeConfig(storage_path=temp_dirs["bronze"]),
            silver=SilverConfig(output_path=temp_dirs["silver"]),
            gold=GoldConfig(storage_path=temp_dirs["gold"], enable_rag=False),
        )

    def test_initialization(self, basic_config):
        framework = Framework(config=basic_config)

        assert framework.config == basic_config
        assert framework._bronze_ingester is None
        assert framework._silver_layer is None
        assert framework._gold_aggregator is None

    def test_process_pipeline_end_to_end(self, basic_config):
        framework = Framework(config=basic_config)

        result = framework.process_pipeline(FIXTURES_DIR / "sample_valid.csv")

        assert isinstance(result, PipelineResult)
        assert result.bronze_dataset is not None
        assert result.silver_dataset is not None
        assert result.gold_dataset is not None
        assert result.total_processing_time_seconds > 0

    def test_process_pipeline_with_progress_callback(self, basic_config):
        framework = Framework(config=basic_config)
        progress_updates = []

        def callback(stage, progress, message):
            progress_updates.append((stage, progress, message))

        result = framework.process_pipeline(
            FIXTURES_DIR / "sample_valid.csv",
            progress_callback=callback,
        )

        assert len(progress_updates) > 0
        stages = [p[0] for p in progress_updates]
        assert "bronze" in stages
        assert "silver" in stages
        assert "gold" in stages

    def test_process_pipeline_file_not_found(self, basic_config):
        framework = Framework(config=basic_config)

        with pytest.raises(FileNotFoundError):
            framework.process_pipeline(Path("/nonexistent/file.csv"))

    def test_get_summary_no_dataset(self, basic_config):
        framework = Framework(config=basic_config)

        summary = framework.get_summary()

        assert "error" in summary

    def test_get_summary_after_pipeline(self, basic_config):
        framework = Framework(config=basic_config)
        framework.process_pipeline(FIXTURES_DIR / "sample_valid.csv")

        summary = framework.get_summary()

        assert "record_count" in summary
        assert "statistics" in summary
        assert "indexes" in summary

    def test_register_hook(self, basic_config):
        framework = Framework(config=basic_config)
        hook_called = []

        def my_hook(context):
            hook_called.append(context.phase)
            return None

        framework.register_hook("bronze", "pre", my_hook)
        framework.process_pipeline(FIXTURES_DIR / "sample_valid.csv")

        assert len(hook_called) > 0

    def test_clear_hooks(self, basic_config):
        framework = Framework(config=basic_config)

        def my_hook(context):
            return None

        framework.register_hook("bronze", "pre", my_hook)
        framework.clear_hooks("bronze")

        hooks = framework._hook_registry.get_hooks("bronze", MagicMock())
        assert len(hooks) == 0

    def test_query_without_rag_raises_error(self, basic_config):
        framework = Framework(config=basic_config)

        with pytest.raises(ConfigurationError) as exc_info:
            framework.query("What is the total?")

        assert "RAG is not enabled" in str(exc_info.value)

    def test_query_without_llm_config_raises_error(self, temp_dirs):
        config = FrameworkConfig(
            bronze=BronzeConfig(storage_path=temp_dirs["bronze"]),
            silver=SilverConfig(output_path=temp_dirs["silver"]),
            gold=GoldConfig(storage_path=temp_dirs["gold"], enable_rag=True),
        )
        framework = Framework(config=config)

        with pytest.raises(ConfigurationError) as exc_info:
            framework.query("What is the total?")

        assert "LLM configuration required" in str(exc_info.value)

    def test_query_with_no_data(self, temp_dirs):
        config = FrameworkConfig(
            bronze=BronzeConfig(storage_path=temp_dirs["bronze"]),
            silver=SilverConfig(output_path=temp_dirs["silver"]),
            gold=GoldConfig(storage_path=temp_dirs["gold"], enable_rag=True),
            llm_enabled=True,
            llm_config=LLMConfig(api_key="test-key"),
        )
        framework = Framework(config=config)

        with patch.object(framework, "_get_rag_engine") as mock_get_rag:
            mock_engine = MagicMock()
            mock_engine._vectorstore = None
            mock_get_rag.return_value = mock_engine

            response = framework.query("Test query")

            assert response.clarification_needed is True
            assert "No data available" in response.answer

    def test_hooks_from_config(self, temp_dirs):
        hook_called = []

        def pre_bronze(context):
            hook_called.append("pre_bronze")
            return None

        config = FrameworkConfig(
            bronze=BronzeConfig(storage_path=temp_dirs["bronze"]),
            silver=SilverConfig(output_path=temp_dirs["silver"]),
            gold=GoldConfig(storage_path=temp_dirs["gold"], enable_rag=False),
            pre_bronze_hooks=[pre_bronze],
        )

        framework = Framework(config=config)

        from crm_medallion.utils.hooks import HookPhase
        hooks = framework._hook_registry.get_hooks("bronze", HookPhase.PRE)
        assert len(hooks) >= 1


class TestFrameworkMemoryMonitoring:
    @pytest.fixture
    def temp_dirs(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            bronze_path = Path(tmpdir) / "bronze"
            silver_path = Path(tmpdir) / "silver"
            gold_path = Path(tmpdir) / "gold"
            bronze_path.mkdir()
            silver_path.mkdir()
            gold_path.mkdir()
            yield {
                "bronze": bronze_path,
                "silver": silver_path,
                "gold": gold_path,
            }

    def test_memory_check_passes_under_limit(self, temp_dirs):
        config = FrameworkConfig(
            bronze=BronzeConfig(storage_path=temp_dirs["bronze"]),
            silver=SilverConfig(output_path=temp_dirs["silver"]),
            gold=GoldConfig(storage_path=temp_dirs["gold"], enable_rag=False),
            max_memory_mb=4096,
        )
        framework = Framework(config=config)

        framework._check_memory_limit()

    def test_memory_check_fails_over_limit(self, temp_dirs):
        config = FrameworkConfig(
            bronze=BronzeConfig(storage_path=temp_dirs["bronze"]),
            silver=SilverConfig(output_path=temp_dirs["silver"]),
            gold=GoldConfig(storage_path=temp_dirs["gold"], enable_rag=False),
            max_memory_mb=128,
        )
        framework = Framework(config=config)

        if framework._memory_monitor_enabled:
            mock_process = MagicMock()
            mock_process.memory_info.return_value.rss = 200 * 1024 * 1024

            with patch.object(framework._psutil, "Process", return_value=mock_process):
                with pytest.raises(FrameworkError) as exc_info:
                    framework._check_memory_limit()

                assert "Memory limit exceeded" in str(exc_info.value)


class TestFrameworkIntegration:
    @pytest.fixture
    def temp_dirs(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            bronze_path = Path(tmpdir) / "bronze"
            silver_path = Path(tmpdir) / "silver"
            gold_path = Path(tmpdir) / "gold"
            bronze_path.mkdir()
            silver_path.mkdir()
            gold_path.mkdir()
            yield {
                "bronze": bronze_path,
                "silver": silver_path,
                "gold": gold_path,
            }

    def test_full_pipeline_with_dirty_data(self, temp_dirs):
        config = FrameworkConfig(
            bronze=BronzeConfig(storage_path=temp_dirs["bronze"]),
            silver=SilverConfig(output_path=temp_dirs["silver"]),
            gold=GoldConfig(storage_path=temp_dirs["gold"], enable_rag=False),
        )
        framework = Framework(config=config)

        result = framework.process_pipeline(FIXTURES_DIR / "sample_dirty.csv")

        assert result.silver_dataset.total_records > 0
        assert result.gold_dataset.record_count >= 0

    def test_pipeline_creates_all_outputs(self, temp_dirs):
        config = FrameworkConfig(
            bronze=BronzeConfig(storage_path=temp_dirs["bronze"]),
            silver=SilverConfig(output_path=temp_dirs["silver"]),
            gold=GoldConfig(storage_path=temp_dirs["gold"], enable_rag=False),
        )
        framework = Framework(config=config)

        result = framework.process_pipeline(FIXTURES_DIR / "sample_valid.csv")

        assert result.bronze_dataset.storage_path.exists()
        assert result.silver_dataset.clean_csv_path.exists()
        assert result.gold_dataset.storage_path.exists()

    def test_pipeline_result_stats(self, temp_dirs):
        config = FrameworkConfig(
            bronze=BronzeConfig(storage_path=temp_dirs["bronze"]),
            silver=SilverConfig(output_path=temp_dirs["silver"]),
            gold=GoldConfig(storage_path=temp_dirs["gold"], enable_rag=False),
        )
        framework = Framework(config=config)

        result = framework.process_pipeline(FIXTURES_DIR / "sample_valid.csv")

        result_dict = result.to_dict()

        assert "pipeline" in result_dict
        assert "bronze" in result_dict
        assert "silver" in result_dict
        assert "gold" in result_dict

        assert result_dict["silver"]["validation_rate"] >= 0
        assert result_dict["silver"]["validation_rate"] <= 1
