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
- estado_factura: Status (Pagada=Paid, Pendiente=Pending, Vencida=Overdue)

When answering:
1. Be precise with numbers and dates
2. Always specify the currency (EUR) for amounts
3. If data is insufficient, say so clearly
4. For comparisons, show both values being compared
5. Answer in the same language as the question (Spanish if asked in Spanish)
"""

QUERY_PROMPT_TEMPLATE = """Based on the following context, answer the user's question.

## Context:
{context}

## Conversation History:
{history}

## User Question:
{question}

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

            self._initialized = True
            logger.debug("RAG engine initialized with LLM and embeddings")
        except ImportError as e:
            raise LLMError(
                "LangChain OpenAI package not installed. "
                "Install with: pip install 'crm-medallion[llm]'",
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
            metadata = {
                "doc_type": "record",
                "record_id": record.get("num_factura", str(i)),
                "tipo": record.get("tipo", ""),
                "categoria": record.get("categoria", ""),
                "proveedor": record.get("proveedor", ""),
                "estado_factura": record.get("estado_factura", ""),
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

        relevant_docs = self._vectorstore.similarity_search(
            natural_language_query,
            k=self._get_k_for_query_type(query_type),
        )

        context_text = "\n\n".join([doc.page_content for doc in relevant_docs])

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

    def _get_k_for_query_type(self, query_type: str) -> int:
        """Get number of documents to retrieve based on query type."""
        return {
            "statistics": 5,
            "comparison": 8,
            "filter": 10,
            "data": 6,
        }.get(query_type, 5)

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
