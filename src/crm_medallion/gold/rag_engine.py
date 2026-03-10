"""RAG Query Engine for natural language queries over Gold data."""

from typing import Any

from crm_medallion.config.framework_config import LLMConfig
from crm_medallion.gold.models import GoldDataset
from crm_medallion.gold.rag_models import (
    ConversationContext,
    DocumentMetadata,
    QueryResponse,
)
from crm_medallion.utils.errors import LLMError
from crm_medallion.utils.logging import get_logger

logger = get_logger(__name__)


SYSTEM_PROMPT = """You are a helpful assistant for analyzing Spanish CRM invoice data.
You have access to invoice records and statistics. Answer questions accurately based on the provided context.

The data contains invoices with fields:
- num_factura: Invoice number
- fecha: Date
- proveedor: Supplier/provider name
- nif_cif: Tax ID
- tipo: Type (Ingreso=Income, Gasto=Expense)
- categoria: Category
- importe_base: Base amount
- iva: VAT amount
- importe_total: Total amount
- estado_factura: Status (Pagada=Paid, Pendiente=Pending, Vencida=Overdue, Parcialmente pagada=Partially paid)

When answering:
1. Be precise with numbers and dates
2. Always specify the currency (EUR) for amounts
3. If data is insufficient, say so clearly
4. For comparisons, show both values being compared
5. ALWAYS respond in Spanish (español), regardless of the question language
6. When counting or aggregating, use the COMPLETE dataset statistics provided
7. Never approximate - use exact numbers from the precomputed statistics
"""

QUERY_PROMPT_TEMPLATE = """Based on the following context, answer the user's question.

## Context:
{context}

## Conversation History:
{history}

## User Question:
{question}

IMPORTANT: Respond ONLY in Spanish (español). Never respond in English.
Provide a clear, concise answer. If you need to show data, format it nicely.
If the question is ambiguous or cannot be answered with the available data, explain why and suggest clarifying questions.
"""


class RAGQueryEngine:
    """Natural language query interface using RAG via LangChain."""

    def __init__(
        self,
        llm_config: LLMConfig,
        gold_dataset: GoldDataset | None = None,
    ):
        """
        Initialize RAG engine with LLM configuration.

        Args:
            llm_config: LLM configuration with API key
            gold_dataset: Optional Gold dataset to load immediately
        """
        self.llm_config = llm_config
        self._llm = None
        self._embeddings = None
        self._vectorstore = None
        self._initialized = False
        self._gold_dataset = gold_dataset
        self._data_records: list[dict] = []
        self._conversation = ConversationContext()

    def _ensure_initialized(self) -> None:
        """Lazy initialization of LLM and embeddings."""
        if self._initialized:
            return

        provider = getattr(self.llm_config, "provider", "openai")

        if provider == "ollama":
            self._init_ollama()
        else:
            self._init_openai()

        self._initialized = True
        logger.debug(f"RAG engine initialized with provider: {provider}")

    def _init_openai(self) -> None:
        """Initialize OpenAI LLM and embeddings."""
        try:
            from langchain_openai import ChatOpenAI, OpenAIEmbeddings

            self._llm = ChatOpenAI(
                model=self.llm_config.model_name,
                temperature=self.llm_config.temperature,
                api_key=self.llm_config.api_key,
            )

            self._embeddings = OpenAIEmbeddings(
                api_key=self.llm_config.api_key,
            )
        except ImportError as e:
            raise LLMError(
                "LangChain OpenAI package not installed. "
                "Install with: pip install 'crm-medallion[llm]'",
                context={"error": str(e)},
            ) from None

    def _init_ollama(self) -> None:
        """Initialize Ollama LLM and embeddings."""
        try:
            from langchain_community.chat_models import ChatOllama
            from langchain_community.embeddings import OllamaEmbeddings

            host = getattr(self.llm_config, "host", None) or "http://localhost:11434"

            self._llm = ChatOllama(
                model=self.llm_config.model_name,
                temperature=self.llm_config.temperature,
                base_url=host,
            )

            self._embeddings = OllamaEmbeddings(
                model=self.llm_config.model_name,
                base_url=host,
            )
        except ImportError as e:
            raise LLMError(
                "LangChain Community package not installed. "
                "Install with: pip install 'crm-medallion[ollama]'",
                context={"error": str(e)},
            ) from None

    def embed_data(self, gold_dataset: GoldDataset, data_records: list[dict]) -> None:
        """
        Create vector embeddings for RAG retrieval.

        Args:
            gold_dataset: The Gold dataset with statistics and indexes
            data_records: List of data records to embed
        """
        self._ensure_initialized()

        try:
            from langchain_community.vectorstores import Chroma
            from langchain_core.documents import Document

            self._gold_dataset = gold_dataset
            self._data_records = data_records

            documents = self._prepare_documents(gold_dataset, data_records, Document)

            logger.info(f"Creating embeddings for {len(documents)} documents")

            self._vectorstore = Chroma.from_documents(
                documents=documents,
                embedding=self._embeddings,
            )

            logger.info("Vector store created successfully")
        except ImportError as e:
            raise LLMError(
                "LangChain community or ChromaDB not installed. "
                "Install with: pip install langchain-community chromadb",
                context={"error": str(e)},
            ) from None
        except Exception as e:
            raise LLMError(
                f"Failed to create embeddings: {e}",
                context={"error_type": type(e).__name__},
            ) from None

    def _prepare_documents(
        self,
        gold_dataset: GoldDataset,
        data_records: list[dict],
        Document: type,
    ) -> list:
        """Prepare documents for embedding."""
        documents = []

        for i, record in enumerate(data_records):
            content = self._format_record_for_embedding(record)
            # Rich metadata for filtering
            metadata = {
                "doc_type": "record",
                "record_id": record.get("num_factura", str(i)),
                "tipo": record.get("tipo", ""),
                "categoria": record.get("categoria", ""),
                "proveedor": record.get("proveedor", ""),
                "estado_factura": record.get("estado_factura", ""),
                "fecha": str(record.get("fecha", "")),
                "importe_base": float(record.get("importe_base", 0)),
                "iva": float(record.get("iva", 0)),
                "importe_total": float(record.get("importe_total", 0)),
            }
            documents.append(Document(page_content=content, metadata=metadata))

        for field_name, stats in gold_dataset.statistics.items():
            content = self._format_statistics_for_embedding(field_name, stats)
            metadata = {
                "doc_type": "statistics",
                "source_field": field_name,
            }
            documents.append(Document(page_content=content, metadata=metadata))

        summary_content = self._format_summary_for_embedding(gold_dataset, data_records)
        documents.append(Document(
            page_content=summary_content,
            metadata={"doc_type": "summary"},
        ))

        for index_name, index in gold_dataset.indexes.items():
            index_content = self._format_index_for_embedding(index_name, index)
            documents.append(Document(
                page_content=index_content,
                metadata={"doc_type": "index", "source_field": index_name},
            ))

        return documents

    def _format_record_for_embedding(self, record: dict) -> str:
        """Format a single record for embedding."""
        lines = [
            f"Invoice {record.get('num_factura', 'N/A')}:",
            f"- Date: {record.get('fecha', 'N/A')}",
            f"- Supplier: {record.get('proveedor', 'N/A')}",
            f"- Type: {record.get('tipo', 'N/A')}",
            f"- Category: {record.get('categoria', 'N/A')}",
            f"- Base amount: {record.get('importe_base', 0):.2f} EUR",
            f"- VAT: {record.get('iva', 0):.2f} EUR",
            f"- Total: {record.get('importe_total', 0):.2f} EUR",
            f"- Status: {record.get('estado_factura', 'N/A')}",
        ]
        return "\n".join(lines)

    def _format_statistics_for_embedding(self, field_name: str, stats) -> str:
        """Format statistics for embedding."""
        return (
            f"Statistics for {field_name}:\n"
            f"- Count: {stats.count} records\n"
            f"- Sum: {stats.sum:.2f} EUR\n"
            f"- Average: {stats.mean:.2f} EUR\n"
            f"- Median: {stats.median:.2f} EUR\n"
            f"- Min: {stats.min:.2f} EUR\n"
            f"- Max: {stats.max:.2f} EUR\n"
            f"- Std Dev: {stats.std:.2f} EUR"
        )

    def _format_summary_for_embedding(
        self,
        gold_dataset: GoldDataset,
        data_records: list[dict],
    ) -> str:
        """Format overall summary for embedding."""
        income_records = [r for r in data_records if r.get("tipo") == "Ingreso"]
        expense_records = [r for r in data_records if r.get("tipo") == "Gasto"]

        income_total = sum(float(r.get("importe_total", 0)) for r in income_records)
        expense_total = sum(float(r.get("importe_total", 0)) for r in expense_records)

        categories = {}
        for record in data_records:
            cat = record.get("categoria", "Unknown")
            categories[cat] = categories.get(cat, 0) + 1

        return (
            f"Dataset Summary:\n"
            f"- Total records: {gold_dataset.record_count}\n"
            f"- Income (Ingreso) records: {len(income_records)}, total: {income_total:.2f} EUR\n"
            f"- Expense (Gasto) records: {len(expense_records)}, total: {expense_total:.2f} EUR\n"
            f"- Net balance: {income_total - expense_total:.2f} EUR\n"
            f"- Categories: {', '.join(f'{k} ({v})' for k, v in categories.items())}\n"
        )

    def _format_index_for_embedding(self, index_name: str, index) -> str:
        """Format index information for embedding."""
        top_entries = sorted(
            index.entries.items(),
            key=lambda x: x[1].count,
            reverse=True,
        )[:10]

        lines = [f"Index for {index_name}:"]
        lines.append(f"- {index.unique_values} unique values")
        lines.append("- Top values:")
        for key, entry in top_entries:
            lines.append(f"  - {key}: {entry.count} records")

        return "\n".join(lines)

    def query(
        self,
        natural_language_query: str,
        context: ConversationContext | None = None,
    ) -> QueryResponse:
        """
        Execute natural language query using RAG.

        Args:
            natural_language_query: User's question in natural language
            context: Optional conversation context for follow-up queries

        Returns:
            QueryResponse with answer and supporting data
        """
        self._ensure_initialized()

        if self._vectorstore is None:
            return QueryResponse(
                query=natural_language_query,
                answer="No data has been loaded. Please load a Gold dataset first.",
                clarification_needed=True,
                clarifying_questions=["Have you loaded data with embed_data()?"],
            )

        conversation = context or self._conversation
        conversation.add_user_message(natural_language_query)

        query_type = self._classify_query(natural_language_query)
        logger.debug(f"Query classified as: {query_type}")

        # For aggregation queries, use metadata filtering and pass precomputed statistics
        metadata_filter = self._build_metadata_filter(natural_language_query, query_type)
        
        k = self._get_k_for_query_type(query_type)
        
        if metadata_filter:
            logger.debug(f"Using metadata filter: {metadata_filter}")
            relevant_docs = self._vectorstore.similarity_search(
                natural_language_query,
                k=k,
                filter=metadata_filter,
            )
        else:
            relevant_docs = self._vectorstore.similarity_search(
                natural_language_query,
                k=k,
            )

        # Add precomputed statistics to context for aggregation queries
        context_parts = [doc.page_content for doc in relevant_docs]
        
        if query_type in ["statistics", "comparison"] and self._gold_dataset:
            stats_summary = self._format_precomputed_stats()
            context_parts.insert(0, stats_summary)
        
        context_text = "\n\n".join(context_parts)

        prompt = QUERY_PROMPT_TEMPLATE.format(
            context=context_text,
            history=conversation.get_history_text(),
            question=natural_language_query,
        )

        try:
            from langchain_core.messages import SystemMessage, HumanMessage

            messages = [
                SystemMessage(content=SYSTEM_PROMPT),
                HumanMessage(content=prompt),
            ]

            response = self._llm.invoke(messages)
            answer = response.content

            supporting_data = self._extract_supporting_data(relevant_docs)

            conversation.add_assistant_message(answer)

            needs_clarification = self._check_needs_clarification(answer)
            clarifying_questions = []
            if needs_clarification:
                clarifying_questions = self._generate_clarifying_questions(
                    natural_language_query
                )

            return QueryResponse(
                query=natural_language_query,
                answer=answer,
                supporting_data=supporting_data,
                confidence=0.9 if not needs_clarification else 0.6,
                query_type=query_type,
                clarification_needed=needs_clarification,
                clarifying_questions=clarifying_questions,
            )

        except Exception as e:
            logger.error(f"Query failed: {e}")
            return QueryResponse(
                query=natural_language_query,
                answer=f"Error processing query: {str(e)}",
                confidence=0.0,
                clarification_needed=True,
            )

    def _classify_query(self, query: str) -> str:
        """Classify the type of query."""
        query_lower = query.lower()

        comparison_keywords = [
            "compar", "vs", "versus", "diferencia", "más que", "menos que",
            "mayor", "menor", "qué categoría", "cuál tiene más", "cuál tiene menos",
        ]
        if any(kw in query_lower for kw in comparison_keywords):
            return "comparison"

        stats_keywords = [
            "total", "suma", "promedio", "media", "máximo", "mínimo",
            "cuánto", "cuántos", "estadística", "resumen",
        ]
        if any(kw in query_lower for kw in stats_keywords):
            return "statistics"

        filter_keywords = [
            "muestra", "lista", "filtra", "busca", "encuentra",
            "facturas de", "pendientes", "pagadas", "del proveedor",
        ]
        if any(kw in query_lower for kw in filter_keywords):
            return "filter"

        return "data"

    def _build_metadata_filter(self, query: str, query_type: str) -> dict[str, Any] | None:
        """Build metadata filter for ChromaDB based on query content."""
        query_lower = query.lower()
        filters = {}
        
        # Filter by estado_factura
        if "pendiente" in query_lower:
            filters["estado_factura"] = "Pendiente"
        elif "pagada" in query_lower or "pagadas" in query_lower:
            filters["estado_factura"] = "Pagada"
        elif "vencida" in query_lower or "vencidas" in query_lower:
            filters["estado_factura"] = "Vencida"
        elif "parcialmente" in query_lower:
            filters["estado_factura"] = "Parcialmente pagada"
        
        # Filter by tipo
        if "ingreso" in query_lower or "ingresos" in query_lower:
            filters["tipo"] = "Ingreso"
        elif "gasto" in query_lower or "gastos" in query_lower:
            filters["tipo"] = "Gasto"
        
        # Always filter to only record documents (not statistics/summary docs)
        filters["doc_type"] = "record"
        
        return filters if len(filters) > 1 else {"doc_type": "record"}

    def _format_precomputed_stats(self) -> str:
        """Format precomputed statistics from Gold dataset for LLM context."""
        if not self._gold_dataset:
            return ""

        lines = ["## PRECOMPUTED STATISTICS (COMPLETE DATASET):"]
        lines.append(f"Total records in dataset: {self._gold_dataset.record_count}")
        lines.append("")

        # Add field statistics
        for field_name, stats in self._gold_dataset.statistics.items():
            lines.append(f"### {field_name}:")
            lines.append(f"  - Count: {stats.count}")
            lines.append(f"  - Sum: {stats.sum:.2f} EUR")
            lines.append(f"  - Mean: {stats.mean:.2f} EUR")
            lines.append(f"  - Median: {stats.median:.2f} EUR")
            lines.append(f"  - Min: {stats.min:.2f} EUR")
            lines.append(f"  - Max: {stats.max:.2f} EUR")
            lines.append(f"  - Std Dev: {stats.std:.2f} EUR")
            lines.append("")

        # Add segmented statistics (CRITICAL for aggregation queries)
        if hasattr(self._gold_dataset, 'segmented_statistics') and self._gold_dataset.segmented_statistics:
            lines.append("## SEGMENTED STATISTICS (BY CATEGORY):")
            lines.append("")

            for seg_name, seg_stats in self._gold_dataset.segmented_statistics.items():
                lines.append(f"### Statistics by {seg_name}:")

                # Sort segments by count descending
                sorted_segments = sorted(
                    seg_stats.segments.items(),
                    key=lambda x: x[1].get("count", 0),
                    reverse=True,
                )

                for segment_value, metrics in sorted_segments:
                    lines.append(f"  {segment_value}:")
                    lines.append(f"    - Count: {metrics.get('count', 0)} records")

                    for metric_name, metric_value in metrics.items():
                        if metric_name != "count" and isinstance(metric_value, (int, float)):
                            lines.append(f"    - {metric_name}: {metric_value:.2f} EUR")

                lines.append("")

        # Add index information (counts by category, provider, etc.)
        for index_name, index in self._gold_dataset.indexes.items():
            lines.append(f"### Index: {index_name}")
            lines.append(f"  - Unique values: {index.unique_values}")

            # Show ALL entries for complete accuracy
            sorted_entries = sorted(
                index.entries.items(),
                key=lambda x: x[1].count,
                reverse=True,
            )

            for key, entry in sorted_entries:
                lines.append(f"  - {key}: {entry.count} records")
            lines.append("")

        return "\n".join(lines)

    def _get_k_for_query_type(self, query_type: str) -> int:
        """Get number of documents to retrieve based on query type.
        
        For aggregation/counting queries, we need ALL documents.
        For other queries, we still retrieve many documents to ensure completeness.
        """
        if self._vectorstore is None:
            return 100
        
        # Get total document count from vectorstore
        try:
            total_docs = self._vectorstore._collection.count()
        except Exception:
            # Fallback to a large number if we can't get the count
            total_docs = 10000
        
        # For statistics and comparisons, we need ALL documents
        if query_type in ["statistics", "comparison", "filter"]:
            return total_docs
        
        # For general data queries, still retrieve a large number
        return min(total_docs, 1000)

    def _extract_supporting_data(self, docs: list) -> list[dict]:
        """Extract supporting data from retrieved documents."""
        supporting_data = []
        for doc in docs:
            if doc.metadata.get("doc_type") == "record":
                supporting_data.append({
                    "type": "record",
                    "record_id": doc.metadata.get("record_id"),
                    "content": doc.page_content[:200],
                })
        return supporting_data[:5]

    def _check_needs_clarification(self, answer: str) -> bool:
        """Check if the answer indicates need for clarification."""
        clarification_indicators = [
            "no tengo suficiente información",
            "no puedo determinar",
            "necesito más detalles",
            "podrías especificar",
            "no está claro",
            "ambiguo",
            "i don't have enough",
            "could you clarify",
            "not clear",
        ]
        answer_lower = answer.lower()
        return any(ind in answer_lower for ind in clarification_indicators)

    def _generate_clarifying_questions(self, query: str) -> list[str]:
        """Generate clarifying questions for ambiguous queries."""
        return [
            "¿Podrías especificar el rango de fechas?",
            "¿Te refieres a un proveedor o categoría específica?",
            "¿Quieres ver los datos en detalle o solo un resumen?",
        ]

    def get_conversation_context(self) -> ConversationContext:
        """Get the current conversation context."""
        return self._conversation

    def clear_conversation(self) -> None:
        """Clear conversation history."""
        self._conversation.clear()

    def reset(self) -> None:
        """Reset the RAG engine state."""
        self._vectorstore = None
        self._gold_dataset = None
        self._data_records = []
        self._conversation.clear()
        logger.debug("RAG engine reset")
