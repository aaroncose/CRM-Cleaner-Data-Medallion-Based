"""Gold Layer: Aggregated and queryable data."""

from crm_medallion.gold.models import (
    FieldStatistics,
    IndexEntry,
    Index,
    GoldDataset,
)
from crm_medallion.gold.aggregator import DataAggregator
from crm_medallion.gold.rag_models import (
    QueryResponse,
    ConversationContext,
    ConversationMessage,
    DocumentMetadata,
)
from crm_medallion.gold.rag_engine import RAGQueryEngine

__all__ = [
    "FieldStatistics",
    "IndexEntry",
    "Index",
    "GoldDataset",
    "DataAggregator",
    "QueryResponse",
    "ConversationContext",
    "ConversationMessage",
    "DocumentMetadata",
    "RAGQueryEngine",
]
