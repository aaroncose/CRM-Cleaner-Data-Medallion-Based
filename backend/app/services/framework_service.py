"""Service that connects to the crm_medallion framework."""

import json
import os
import sys
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

# Add src to path to import crm_medallion
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent / "src"))

from crm_medallion import Framework, FrameworkConfig
from crm_medallion.config.framework_config import (
    BronzeConfig,
    GoldConfig,
    LLMConfig,
    SilverConfig,
)
from crm_medallion.bronze.ingester import CSVIngester
from crm_medallion.gold.rag_engine import RAGQueryEngine


class FrameworkService:
    """Service to interact with the CRM Medallion framework."""

    def __init__(self, data_dir: Path | None = None):
        self.data_dir = data_dir or Path("./data")
        self.uploads_dir = self.data_dir / "uploads"
        self.runs_dir = self.data_dir / "runs"
        self.uploads_dir.mkdir(parents=True, exist_ok=True)
        self.runs_dir.mkdir(parents=True, exist_ok=True)

        self._runs: dict[str, dict[str, Any]] = {}
        self._rag_engines: dict[str, RAGQueryEngine] = {}
        self._config = {
            "provider": "openai",
            "model_name": "gpt-4o-mini",
            "api_key": os.environ.get("OPENAI_API_KEY", ""),
            "confidence_threshold": 0.7,
            "dedup_auto_threshold": 0.95,
            "dedup_review_threshold": 0.75,
        }

    def save_uploaded_file(self, filename: str, content: bytes) -> dict[str, Any]:
        """Save uploaded file and return file info with preview."""
        file_id = str(uuid.uuid4())[:8]
        file_path = self.uploads_dir / f"{file_id}_{filename}"

        with open(file_path, "wb") as f:
            f.write(content)

        # Use CSVIngester to detect schema
        bronze_config = BronzeConfig(storage_path=self.data_dir / "bronze")
        ingester = CSVIngester(bronze_config)

        # Detect encoding and get preview
        encoding = ingester.detect_encoding(file_path)

        import csv
        rows = []
        columns = []

        with open(file_path, "r", encoding=encoding) as f:
            reader = csv.DictReader(f)
            # Filter out None column names and rename empty ones
            raw_columns = reader.fieldnames or []
            columns = []
            for idx, col in enumerate(raw_columns):
                if col is None or col.strip() == "":
                    columns.append(f"column_{idx}")
                else:
                    columns.append(col)

            for i, row in enumerate(reader):
                if i >= 10:
                    break
                # Clean row: convert None values to empty string, handle None keys
                clean_row = {}
                for idx, (key, value) in enumerate(row.items()):
                    clean_key = columns[idx] if idx < len(columns) else f"column_{idx}"
                    clean_row[clean_key] = "" if value is None else str(value)
                rows.append(clean_row)

        # Detect types using the new detect_schema method
        schema = ingester.detect_schema(file_path, sample_rows=20)
        detected_types = {
            field.name: field.field_type.value for field in schema.fields
        }

        # Count total rows
        with open(file_path, "r", encoding=encoding) as f:
            row_count = sum(1 for _ in f) - 1

        return {
            "file_id": file_id,
            "filename": filename,
            "path": str(file_path),
            "row_count": row_count,
            "columns": columns,
            "preview": rows,
            "detected_types": detected_types,
            "encoding": encoding,
        }

    def process_file(
        self,
        file_id: str,
        llm_enabled: bool = False,
        provider: str = "openai",
        api_key: str | None = None,
        model_name: str | None = None,
        progress_callback: Callable[[str, float, str], None] | None = None,
    ) -> str:
        """Process a file through the pipeline."""
        # Find the uploaded file
        files = list(self.uploads_dir.glob(f"{file_id}_*"))
        if not files:
            raise FileNotFoundError(f"File with ID {file_id} not found")

        file_path = files[0]
        run_id = str(uuid.uuid4())[:8]
        run_dir = self.runs_dir / run_id

        # Create run directory
        run_dir.mkdir(parents=True, exist_ok=True)

        # Configure framework
        llm_config = None
        if llm_enabled:
            llm_config = LLMConfig(
                provider=provider,
                model_name=model_name or self._config["model_name"],
                api_key=api_key or self._config["api_key"],
            )

        config = FrameworkConfig(
            bronze=BronzeConfig(storage_path=run_dir / "bronze"),
            silver=SilverConfig(output_path=run_dir / "silver"),
            gold=GoldConfig(storage_path=run_dir / "gold", enable_rag=True),
            llm_enabled=llm_enabled,
            llm_config=llm_config,
        )

        # Initialize and run framework
        framework = Framework(config)

        self._runs[run_id] = {
            "status": "processing",
            "progress": 0.0,
            "current_stage": "bronze",
            "message": "Starting pipeline...",
        }

        def internal_callback(stage: str, progress: float, message: str):
            self._runs[run_id].update({
                "current_stage": stage,
                "progress": progress,
                "message": message,
            })
            if progress_callback:
                progress_callback(stage, progress, message)

        try:
            result = framework.process_pipeline(file_path, progress_callback=internal_callback)

            self._runs[run_id].update({
                "status": "completed",
                "progress": 1.0,
                "current_stage": "completed",
                "message": "Pipeline completed successfully",
                "result": {
                    "total_records": result.silver_dataset.total_records,
                    "valid_records": result.silver_dataset.valid_records,
                    "invalid_records": result.silver_dataset.invalid_records,
                    "llm_corrected": result.silver_dataset.llm_corrected_records,
                    "manual_review": result.silver_dataset.manual_review_records,
                    "processing_time": result.total_processing_time_seconds,
                    "bronze_path": str(result.bronze_dataset.storage_path),
                    "silver_path": str(result.silver_dataset.clean_csv_path),
                    "gold_path": str(result.gold_dataset.storage_path),
                },
            })

        except Exception as e:
            self._runs[run_id].update({
                "status": "error",
                "message": str(e),
            })
            raise

        return run_id

    def get_run_status(self, run_id: str) -> dict[str, Any]:
        """Get the status of a processing run."""
        if run_id not in self._runs:
            raise ValueError(f"Run {run_id} not found")
        return self._runs[run_id]

    def get_run_result(self, run_id: str) -> dict[str, Any]:
        """Get the result of a completed run."""
        if run_id not in self._runs:
            raise ValueError(f"Run {run_id} not found")

        run_data = self._runs[run_id]
        if run_data["status"] != "completed":
            raise ValueError(f"Run {run_id} is not completed")

        return run_data.get("result", {})

    def get_gold_data(self, run_id: str) -> dict[str, Any]:
        """Get Gold layer data for a run."""
        run_dir = self.runs_dir / run_id / "gold"
        gold_files = list(run_dir.glob("*_gold.json"))

        if not gold_files:
            raise FileNotFoundError(f"Gold data not found for run {run_id}")

        with open(gold_files[0], "r", encoding="utf-8") as f:
            return json.load(f)

    def get_compare_data(self, run_id: str) -> dict[str, Any]:
        """Get comparison data between raw and clean CSV."""
        run_dir = self.runs_dir / run_id
        bronze_dir = run_dir / "bronze"
        silver_dir = run_dir / "silver"

        bronze_files = list(bronze_dir.glob("*.csv"))
        silver_files = list(silver_dir.glob("*_clean.csv"))

        if not bronze_files or not silver_files:
            raise FileNotFoundError(f"Data not found for run {run_id}")

        import csv

        raw_rows = []
        clean_rows = []

        with open(bronze_files[0], "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            raw_rows = list(reader)

        with open(silver_files[0], "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            clean_rows = list(reader)

        # Compare rows
        comparison = []
        for i, (raw, clean) in enumerate(zip(raw_rows, clean_rows)):
            modified_fields = []
            for key in raw.keys():
                if key in clean and str(raw.get(key, "")).strip() != str(clean.get(key, "")).strip():
                    modified_fields.append(key)

            comparison.append({
                "row_number": i + 1,
                "raw": raw,
                "clean": clean,
                "modified": len(modified_fields) > 0,
                "modified_fields": modified_fields,
            })

        return {
            "run_id": run_id,
            "total_rows": len(comparison),
            "modified_rows": sum(1 for r in comparison if r["modified"]),
            "rows": comparison,
        }

    def chat_query(self, message: str, run_id: str | None = None) -> dict[str, Any]:
        """Process a chat query using RAG."""
        if run_id and run_id not in self._rag_engines:
            # Check if API key is configured
            if not self._config["api_key"]:
                return {
                    "answer": "Error: No hay API key configurada. Configura tu OPENAI_API_KEY en el panel de configuración o como variable de entorno para usar el chat.",
                    "supporting_data": None,
                    "has_chart_data": False,
                    "chart_data": None,
                }

            # Initialize RAG engine for this run
            gold_data = self.get_gold_data(run_id)

            from crm_medallion.gold.models import (
                FieldStatistics,
                GoldDataset,
                Index,
                IndexEntry,
                SegmentedStatistics,
            )

            records = gold_data.get("records", [])
            statistics = {}
            for name, stats_data in gold_data.get("statistics", {}).items():
                statistics[name] = FieldStatistics(
                    field_name=stats_data.get("field_name", name),
                    count=stats_data.get("count", 0),
                    sum=stats_data.get("sum", 0.0),
                    mean=stats_data.get("mean", 0.0),
                    median=stats_data.get("median", 0.0),
                    min=stats_data.get("min", 0.0),
                    max=stats_data.get("max", 0.0),
                    std=stats_data.get("std", 0.0),
                )

            indexes = {}
            for name, idx_data in gold_data.get("indexes", {}).items():
                entries = {}
                for key, entry_data in idx_data.get("entries", {}).items():
                    entries[key] = IndexEntry(
                        key=key,
                        row_indices=entry_data.get("row_indices", []),
                        count=entry_data.get("count", 0),
                    )
                indexes[name] = Index(
                    field_name=name,
                    entries=entries,
                    unique_values=idx_data.get("unique_values", len(entries)),
                )

            segmented_statistics = {}
            for name, seg_data in gold_data.get("segmented_statistics", {}).items():
                segmented_statistics[name] = SegmentedStatistics(
                    segment_field=seg_data.get("segment_field", name),
                    segments=seg_data.get("segments", {}),
                )

            run_dir = self.runs_dir / run_id / "gold"
            gold_files = list(run_dir.glob("*_gold.json"))

            gold_dataset = GoldDataset(
                id=run_id,
                silver_dataset_id="unknown",
                storage_path=gold_files[0] if gold_files else Path(""),
                aggregation_timestamp=datetime.now(),
                record_count=len(records),
                statistics=statistics,
                indexes=indexes,
                segmented_statistics=segmented_statistics,
                column_names=list(records[0].keys()) if records else [],
            )

            llm_config = LLMConfig(
                provider=self._config["provider"],
                model_name=self._config["model_name"],
                api_key=self._config["api_key"],
            )

            rag_engine = RAGQueryEngine(llm_config=llm_config)
            rag_engine.embed_data(gold_dataset, records)
            self._rag_engines[run_id] = rag_engine

        if run_id:
            response = self._rag_engines[run_id].query(message)
            return {
                "answer": response.answer,
                "supporting_data": response.supporting_data,
                "has_chart_data": False,
                "chart_data": None,
            }
        else:
            return {
                "answer": "Por favor, procesa un archivo CSV primero para poder consultar los datos.",
                "supporting_data": None,
                "has_chart_data": False,
                "chart_data": None,
            }

    def get_config(self) -> dict[str, Any]:
        """Get current configuration."""
        return {
            "provider": self._config["provider"],
            "model_name": self._config["model_name"],
            "confidence_threshold": self._config["confidence_threshold"],
            "dedup_auto_threshold": self._config["dedup_auto_threshold"],
            "dedup_review_threshold": self._config["dedup_review_threshold"],
        }

    def update_config(self, updates: dict[str, Any]) -> dict[str, Any]:
        """Update configuration."""
        for key, value in updates.items():
            if value is not None and key in self._config:
                self._config[key] = value
        return self.get_config()

    def list_runs(self) -> list[dict[str, Any]]:
        """List all available runs."""
        runs = []
        for run_dir in sorted(self.runs_dir.iterdir(), reverse=True):
            if run_dir.is_dir():
                run_id = run_dir.name

                # Try to get run info
                gold_files = list((run_dir / "gold").glob("*_gold.json")) if (run_dir / "gold").exists() else []
                if gold_files:
                    try:
                        with open(gold_files[0], "r", encoding="utf-8") as f:
                            gold_data = json.load(f)
                            record_count = len(gold_data.get("records", []))
                    except Exception:
                        record_count = 0
                else:
                    record_count = 0

                # Get file name from bronze
                bronze_dir = run_dir / "bronze"
                csv_files = list(bronze_dir.glob("*.csv")) if bronze_dir.exists() else []
                file_name = csv_files[0].name if csv_files else "unknown"

                # Get status
                status = self._runs.get(run_id, {}).get("status", "completed" if gold_files else "unknown")

                # Get creation time
                created_at = datetime.fromtimestamp(run_dir.stat().st_mtime).isoformat()

                runs.append({
                    "run_id": run_id,
                    "file_name": file_name,
                    "status": status,
                    "record_count": record_count,
                    "created_at": created_at,
                })

        return runs[:20]  # Return last 20 runs


# Global service instance
_service: FrameworkService | None = None


def get_framework_service() -> FrameworkService:
    """Get the global framework service instance."""
    global _service
    if _service is None:
        _service = FrameworkService()
    return _service
