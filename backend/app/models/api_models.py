"""Pydantic models for API requests and responses."""

from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class LLMProvider(str, Enum):
    OPENAI = "openai"
    ANTHROPIC = "anthropic"
    GOOGLE = "google"
    OLLAMA = "ollama"


class ProcessingStatus(str, Enum):
    PENDING = "pending"
    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"
    COMPLETED = "completed"
    ERROR = "error"


class UploadResponse(BaseModel):
    file_id: str
    filename: str
    row_count: int
    columns: list[str]
    preview: list[dict[str, Any]]
    detected_types: dict[str, str]


class ColumnConfig(BaseModel):
    name: str
    type: str = "string"
    required: bool = True
    allowed_values: list[str] | None = None


class ProcessRequest(BaseModel):
    file_id: str
    schema_config: list[ColumnConfig] | None = None
    llm_enabled: bool = False
    provider: LLMProvider = LLMProvider.OPENAI
    api_key: str | None = None
    model_name: str | None = None


class ProcessStatusResponse(BaseModel):
    run_id: str
    status: ProcessingStatus
    progress: float = 0.0
    current_stage: str = ""
    message: str = ""


class ProcessResultResponse(BaseModel):
    run_id: str
    status: ProcessingStatus
    total_records: int
    valid_records: int
    invalid_records: int
    llm_corrected: int
    manual_review: int
    processing_time_seconds: float
    bronze_path: str | None = None
    silver_path: str | None = None
    gold_path: str | None = None


class DataRow(BaseModel):
    row_number: int
    data: dict[str, Any]
    modified: bool = False
    modification_type: str | None = None


class CompareResponse(BaseModel):
    run_id: str
    total_rows: int
    modified_rows: int
    rows: list[dict[str, Any]]


class EntityGroup(BaseModel):
    group_id: str
    field: str
    variations: list[str]
    canonical: str
    similarity: float
    count: int
    status: str = "pending"


class ReviewResponse(BaseModel):
    run_id: str
    total_groups: int
    pending_groups: int
    groups: list[EntityGroup]


class ApproveRequest(BaseModel):
    group_ids: list[str]
    canonical_overrides: dict[str, str] | None = None


class ChatMessage(BaseModel):
    role: str
    content: str
    timestamp: datetime = Field(default_factory=datetime.now)
    data: dict[str, Any] | None = None


class ChatRequest(BaseModel):
    message: str
    run_id: str | None = None


class ChatResponse(BaseModel):
    answer: str
    supporting_data: list[dict[str, Any]] | None = None
    has_chart_data: bool = False
    chart_data: dict[str, Any] | None = None


class MetricData(BaseModel):
    name: str
    value: float | int | str
    chart_type: str | None = None
    chart_data: dict[str, Any] | None = None


class MetricsResponse(BaseModel):
    run_id: str
    fixed_metrics: list[MetricData]
    suggested_metrics: list[MetricData] | None = None


class SchemaDetectRequest(BaseModel):
    file_id: str
    sample_rows: int = 20


class SchemaTemplate(BaseModel):
    name: str
    description: str
    columns: list[ColumnConfig]


class ConfigResponse(BaseModel):
    provider: LLMProvider
    model_name: str
    confidence_threshold: float
    dedup_auto_threshold: float
    dedup_review_threshold: float


class ConfigUpdateRequest(BaseModel):
    provider: LLMProvider | None = None
    api_key: str | None = None
    model_name: str | None = None
    confidence_threshold: float | None = None
    dedup_auto_threshold: float | None = None
    dedup_review_threshold: float | None = None
