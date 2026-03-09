"""Main Framework orchestrator for the Medallion architecture."""

import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

from crm_medallion.bronze.ingester import CSVIngester
from crm_medallion.bronze.models import BronzeDataset
from crm_medallion.config.framework_config import FrameworkConfig
from crm_medallion.gold.aggregator import DataAggregator
from crm_medallion.gold.models import GoldDataset
from crm_medallion.gold.rag_engine import RAGQueryEngine
from crm_medallion.gold.rag_models import QueryResponse
from crm_medallion.silver.layer import SilverLayer
from crm_medallion.silver.models import SilverDataset
from crm_medallion.utils.errors import ConfigurationError, FrameworkError
from crm_medallion.utils.hooks import HookExecutor, HookPhase, HookRegistry, HookResult
from crm_medallion.utils.logging import get_logger

logger = get_logger(__name__)


@dataclass
class PipelineResult:
    """Result of a complete pipeline execution."""

    bronze_dataset: BronzeDataset
    silver_dataset: SilverDataset
    gold_dataset: GoldDataset

    start_time: datetime
    end_time: datetime
    total_processing_time_seconds: float

    bronze_stats: dict[str, Any] = field(default_factory=dict)
    silver_stats: dict[str, Any] = field(default_factory=dict)
    gold_stats: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert result to dictionary for reporting."""
        return {
            "pipeline": {
                "start_time": self.start_time.isoformat(),
                "end_time": self.end_time.isoformat(),
                "total_processing_time_seconds": self.total_processing_time_seconds,
            },
            "bronze": {
                "dataset_id": self.bronze_dataset.id,
                "row_count": self.bronze_dataset.row_count,
                "encoding": self.bronze_dataset.encoding,
                **self.bronze_stats,
            },
            "silver": {
                "dataset_id": self.silver_dataset.id,
                "total_records": self.silver_dataset.total_records,
                "valid_records": self.silver_dataset.valid_records,
                "invalid_records": self.silver_dataset.invalid_records,
                "llm_corrected_records": self.silver_dataset.llm_corrected_records,
                "manual_review_records": self.silver_dataset.manual_review_records,
                "processing_time_seconds": self.silver_dataset.processing_time_seconds,
                **self.silver_stats,
            },
            "gold": {
                "dataset_id": self.gold_dataset.id,
                "record_count": self.gold_dataset.record_count,
                "statistics_count": len(self.gold_dataset.statistics),
                "index_count": len(self.gold_dataset.indexes),
                **self.gold_stats,
            },
        }


class Framework:
    """Main orchestrator for the Medallion data pipeline.

    Coordinates Bronze, Silver, and Gold layer processing with optional
    LLM enhancement and RAG query capabilities.

    Args:
        config: Framework configuration instance.

    Example:
        >>> framework = Framework(FrameworkConfig())
        >>> result = framework.process_pipeline("data.csv")
    """

    def __init__(self, config: FrameworkConfig):
        self.config = config
        self._hook_registry = HookRegistry()
        self._hook_executor = HookExecutor(self._hook_registry)

        self._register_config_hooks()

        self._bronze_ingester: CSVIngester | None = None
        self._silver_layer: SilverLayer | None = None
        self._gold_aggregator: DataAggregator | None = None
        self._rag_engine: RAGQueryEngine | None = None

        self._current_gold_dataset: GoldDataset | None = None
        self._memory_monitor_enabled = False

        try:
            import psutil
            self._memory_monitor_enabled = True
            self._psutil = psutil
        except ImportError:
            logger.debug("psutil not installed, memory monitoring disabled")

    def _register_config_hooks(self) -> None:
        """Register hooks from configuration."""
        for hook in self.config.pre_bronze_hooks:
            self._hook_registry.register("bronze", HookPhase.PRE, hook)
        for hook in self.config.post_bronze_hooks:
            self._hook_registry.register("bronze", HookPhase.POST, hook)
        for hook in self.config.pre_silver_hooks:
            self._hook_registry.register("silver", HookPhase.PRE, hook)
        for hook in self.config.post_silver_hooks:
            self._hook_registry.register("silver", HookPhase.POST, hook)
        for hook in self.config.pre_gold_hooks:
            self._hook_registry.register("gold", HookPhase.PRE, hook)
        for hook in self.config.post_gold_hooks:
            self._hook_registry.register("gold", HookPhase.POST, hook)

    def _check_memory_limit(self) -> None:
        """Check if memory usage exceeds configured limit."""
        if not self._memory_monitor_enabled:
            return

        process = self._psutil.Process()
        memory_mb = process.memory_info().rss / (1024 * 1024)

        if memory_mb > self.config.max_memory_mb:
            raise FrameworkError(
                f"Memory limit exceeded: {memory_mb:.1f}MB > {self.config.max_memory_mb}MB",
                context={
                    "current_memory_mb": memory_mb,
                    "limit_mb": self.config.max_memory_mb,
                },
            )

    def _get_bronze_ingester(self) -> CSVIngester:
        """Get or create Bronze ingester."""
        if self._bronze_ingester is None:
            self._bronze_ingester = CSVIngester(
                config=self.config.bronze,
                hook_registry=self._hook_registry,
            )
        return self._bronze_ingester

    def _get_silver_layer(self) -> SilverLayer:
        """Get or create Silver layer."""
        if self._silver_layer is None:
            if self.config.schema is None:
                from crm_medallion.silver.models import FacturaVenta
                schema_model = FacturaVenta
            else:
                schema_model = self.config.schema.to_pydantic_model()

            self._silver_layer = SilverLayer(
                schema_model=schema_model,
                config=self.config.silver,
                llm_config=self.config.llm_config if self.config.llm_enabled else None,
                hook_registry=self._hook_registry,
            )
        return self._silver_layer

    def _get_gold_aggregator(self) -> DataAggregator:
        """Get or create Gold aggregator."""
        if self._gold_aggregator is None:
            self._gold_aggregator = DataAggregator(
                config=self.config.gold,
                hook_registry=self._hook_registry,
            )
        return self._gold_aggregator

    def _get_rag_engine(self) -> RAGQueryEngine:
        """Get or create RAG engine."""
        if not self.config.gold.enable_rag:
            raise ConfigurationError(
                "RAG is not enabled in configuration",
                context={"enable_rag": self.config.gold.enable_rag},
            )

        if self.config.llm_config is None:
            raise ConfigurationError(
                "LLM configuration required for RAG queries",
                context={"llm_enabled": self.config.llm_enabled},
            )

        if self._rag_engine is None:
            self._rag_engine = RAGQueryEngine(
                llm_config=self.config.llm_config,
            )
        return self._rag_engine

    def process_pipeline(
        self,
        csv_path: Path | str,
        progress_callback: Callable[[str, float, str], None] | None = None,
    ) -> PipelineResult:
        """Execute the complete Bronze to Silver to Gold pipeline.

        Args:
            csv_path: Path to the input CSV file.
            progress_callback: Optional function called with (stage, progress, message).

        Returns:
            PipelineResult containing all datasets and processing statistics.

        Raises:
            FileNotFoundError: If the CSV file does not exist.
            FrameworkError: If processing fails or memory limit is exceeded.
        """
        if isinstance(csv_path, str):
            csv_path = Path(csv_path)

        start_time = datetime.now()
        pipeline_start = time.time()

        logger.info(f"Starting pipeline for: {csv_path.name}")

        def report_progress(stage: str, progress: float, message: str) -> None:
            logger.info(f"[{stage}] {progress:.0%} - {message}")
            if progress_callback:
                progress_callback(stage, progress, message)

        report_progress("bronze", 0.0, "Starting Bronze layer ingestion")
        self._check_memory_limit()

        bronze_ingester = self._get_bronze_ingester()
        bronze_dataset = bronze_ingester.ingest(csv_path)

        bronze_stats = {
            "source_file": str(bronze_dataset.source_file),
            "column_names": bronze_dataset.column_names,
        }

        report_progress("bronze", 1.0, f"Ingested {bronze_dataset.row_count} rows")
        self._check_memory_limit()

        report_progress("silver", 0.0, "Starting Silver layer processing")

        silver_layer = self._get_silver_layer()
        silver_dataset = silver_layer.process(bronze_dataset)

        silver_stats = {
            "validation_rate": (
                silver_dataset.valid_records / silver_dataset.total_records
                if silver_dataset.total_records > 0
                else 0.0
            ),
        }

        report_progress(
            "silver",
            1.0,
            f"Processed {silver_dataset.valid_records}/{silver_dataset.total_records} valid",
        )
        self._check_memory_limit()

        report_progress("gold", 0.0, "Starting Gold layer aggregation")

        gold_aggregator = self._get_gold_aggregator()
        gold_dataset = gold_aggregator.aggregate(silver_dataset)

        self._current_gold_dataset = gold_dataset

        gold_stats = {
            "statistics_fields": list(gold_dataset.statistics.keys()),
            "index_fields": list(gold_dataset.indexes.keys()),
        }

        report_progress(
            "gold",
            1.0,
            f"Aggregated {gold_dataset.record_count} records",
        )

        if self.config.gold.enable_rag and self.config.llm_config:
            report_progress("rag", 0.0, "Initializing RAG engine")
            try:
                rag_engine = self._get_rag_engine()
                data_records = gold_aggregator.load(gold_dataset).to_dict(orient="records")
                rag_engine.embed_data(gold_dataset, data_records)
                report_progress("rag", 1.0, "RAG engine ready")
            except Exception as e:
                logger.warning(f"RAG initialization failed: {e}")
                report_progress("rag", 1.0, f"RAG initialization failed: {e}")

        end_time = datetime.now()
        total_time = time.time() - pipeline_start

        logger.info(f"Pipeline complete in {total_time:.2f}s")

        return PipelineResult(
            bronze_dataset=bronze_dataset,
            silver_dataset=silver_dataset,
            gold_dataset=gold_dataset,
            start_time=start_time,
            end_time=end_time,
            total_processing_time_seconds=total_time,
            bronze_stats=bronze_stats,
            silver_stats=silver_stats,
            gold_stats=gold_stats,
        )

    def query(
        self,
        natural_language_query: str,
        gold_dataset: GoldDataset | None = None,
    ) -> QueryResponse:
        """Execute a natural language query using RAG.

        Args:
            natural_language_query: Question in natural language.
            gold_dataset: Gold dataset to query. Uses current dataset if None.

        Returns:
            QueryResponse with answer, supporting data, and confidence score.

        Raises:
            ConfigurationError: If RAG is not enabled or LLM config is missing.
        """
        rag_engine = self._get_rag_engine()

        dataset = gold_dataset or self._current_gold_dataset

        if dataset is None:
            return QueryResponse(
                query=natural_language_query,
                answer="No data available. Please run the pipeline first.",
                clarification_needed=True,
                clarifying_questions=["Have you run process_pipeline() yet?"],
            )

        if rag_engine._vectorstore is None:
            aggregator = self._get_gold_aggregator()
            data_records = aggregator.load(dataset).to_dict(orient="records")
            rag_engine.embed_data(dataset, data_records)

        return rag_engine.query(natural_language_query)

    def get_summary(self, gold_dataset: GoldDataset | None = None) -> dict[str, Any]:
        """
        Get a summary of the Gold dataset.

        Args:
            gold_dataset: Optional Gold dataset (uses current if not provided)

        Returns:
            Summary dictionary with statistics and index information
        """
        dataset = gold_dataset or self._current_gold_dataset

        if dataset is None:
            return {"error": "No Gold dataset available"}

        aggregator = self._get_gold_aggregator()
        return aggregator.get_summary(dataset)

    def register_hook(
        self,
        layer: str,
        phase: str,
        hook: callable,
    ) -> None:
        """
        Register a hook for extensibility.

        Args:
            layer: Layer name (bronze, silver, gold)
            phase: Hook phase (pre, post)
            hook: Callable hook function
        """
        hook_phase = HookPhase.PRE if phase == "pre" else HookPhase.POST
        self._hook_registry.register(layer, hook_phase, hook)

    def clear_hooks(self, layer: str | None = None) -> None:
        """
        Clear registered hooks.

        Args:
            layer: Optional layer to clear (clears all if None)
        """
        self._hook_registry.clear(layer=layer)
