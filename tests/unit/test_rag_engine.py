"""Tests for RAG Query Engine."""

import tempfile
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from crm_medallion.config.framework_config import LLMConfig, GoldConfig
from crm_medallion.gold.models import FieldStatistics, GoldDataset, Index, IndexEntry
from crm_medallion.gold.rag_models import (
    ConversationContext,
    ConversationMessage,
    DocumentMetadata,
    QueryResponse,
)
from crm_medallion.gold.rag_engine import RAGQueryEngine
from crm_medallion.utils.errors import LLMError


class TestQueryResponse:
    def test_creates_with_required_fields(self):
        response = QueryResponse(
            query="¿Cuántas facturas hay?",
            answer="Hay 10 facturas en el sistema.",
        )

        assert response.query == "¿Cuántas facturas hay?"
        assert response.answer == "Hay 10 facturas en el sistema."
        assert response.confidence == 1.0
        assert response.clarification_needed is False

    def test_creates_with_all_fields(self):
        response = QueryResponse(
            query="test query",
            answer="test answer",
            supporting_data=[{"type": "record", "record_id": "FAC-001"}],
            confidence=0.85,
            query_type="statistics",
            clarification_needed=True,
            clarifying_questions=["¿Podrías especificar?"],
        )

        assert response.confidence == 0.85
        assert response.query_type == "statistics"
        assert len(response.supporting_data) == 1
        assert len(response.clarifying_questions) == 1


class TestConversationContext:
    def test_add_user_message(self):
        context = ConversationContext()
        context.add_user_message("Hola")

        assert len(context.messages) == 1
        assert context.messages[0].role == "user"
        assert context.messages[0].content == "Hola"

    def test_add_assistant_message(self):
        context = ConversationContext()
        context.add_assistant_message("Buenos días")

        assert len(context.messages) == 1
        assert context.messages[0].role == "assistant"

    def test_get_history_text(self):
        context = ConversationContext()
        context.add_user_message("Pregunta 1")
        context.add_assistant_message("Respuesta 1")
        context.add_user_message("Pregunta 2")

        history = context.get_history_text()

        assert "User: Pregunta 1" in history
        assert "Assistant: Respuesta 1" in history
        assert "User: Pregunta 2" in history

    def test_trim_history(self):
        context = ConversationContext(max_messages=3)

        for i in range(5):
            context.add_user_message(f"Message {i}")

        assert len(context.messages) == 3
        assert context.messages[0].content == "Message 2"
        assert context.messages[2].content == "Message 4"

    def test_clear(self):
        context = ConversationContext()
        context.add_user_message("Test")
        context.clear()

        assert len(context.messages) == 0


class TestDocumentMetadata:
    def test_creates_with_required_fields(self):
        metadata = DocumentMetadata(doc_type="record")

        assert metadata.doc_type == "record"
        assert metadata.source_field is None

    def test_creates_with_all_fields(self):
        metadata = DocumentMetadata(
            doc_type="statistics",
            source_field="importe_total",
            record_id="FAC-001",
            date_range="2024-01-01 to 2024-12-31",
        )

        assert metadata.source_field == "importe_total"
        assert metadata.record_id == "FAC-001"


class TestRAGQueryEngine:
    @pytest.fixture
    def llm_config(self):
        return LLMConfig(
            model_name="gpt-4",
            temperature=0.0,
            api_key="test-api-key",
        )

    @pytest.fixture
    def sample_gold_dataset(self):
        stats = FieldStatistics(
            field_name="importe_total",
            count=5,
            sum=5000.0,
            mean=1000.0,
            median=950.0,
            min=500.0,
            max=1500.0,
            std=300.0,
        )

        entries = {
            "Marketing": IndexEntry(key="Marketing", row_indices=[0, 2], count=2),
            "Tecnología": IndexEntry(key="Tecnología", row_indices=[1, 3, 4], count=3),
        }
        index = Index(field_name="categoria", entries=entries, unique_values=2)

        return GoldDataset(
            id="test-gold-123",
            silver_dataset_id="test-silver-456",
            storage_path=Path("/tmp/test_gold.json"),
            aggregation_timestamp=datetime.now(),
            record_count=5,
            statistics={"importe_total": stats},
            indexes={"categoria": index},
        )

    @pytest.fixture
    def sample_records(self):
        return [
            {
                "num_factura": "FAC-2024-0001",
                "fecha": "2024-01-15",
                "proveedor": "Empresa A",
                "tipo": "Ingreso",
                "categoria": "Marketing",
                "importe_base": 1000.0,
                "iva": 210.0,
                "importe_total": 1210.0,
                "estado_factura": "Pagada",
            },
            {
                "num_factura": "FAC-2024-0002",
                "fecha": "2024-02-20",
                "proveedor": "Empresa B",
                "tipo": "Gasto",
                "categoria": "Tecnología",
                "importe_base": 800.0,
                "iva": 168.0,
                "importe_total": 968.0,
                "estado_factura": "Pendiente",
            },
        ]

    def test_initialization(self, llm_config):
        engine = RAGQueryEngine(llm_config=llm_config)

        assert engine.llm_config == llm_config
        assert engine._llm is None
        assert engine._initialized is False

    def test_query_without_data_returns_error(self, llm_config):
        engine = RAGQueryEngine(llm_config=llm_config)

        with patch.object(engine, "_ensure_initialized"):
            response = engine.query("¿Cuántas facturas hay?")

        assert response.clarification_needed is True
        assert "No data has been loaded" in response.answer

    def test_ensure_initialized_creates_llm(self, llm_config):
        engine = RAGQueryEngine(llm_config=llm_config)

        mock_openai_module = MagicMock()
        mock_chat = MagicMock()
        mock_embeddings = MagicMock()
        mock_openai_module.ChatOpenAI.return_value = mock_chat
        mock_openai_module.OpenAIEmbeddings.return_value = mock_embeddings

        with patch.dict("sys.modules", {"langchain_openai": mock_openai_module}):
            engine._ensure_initialized()

        assert engine._initialized is True
        assert engine._llm is not None
        assert engine._embeddings is not None

    def test_classify_query_statistics(self, llm_config):
        engine = RAGQueryEngine(llm_config=llm_config)

        assert engine._classify_query("¿Cuál es el total de facturas?") == "statistics"
        assert engine._classify_query("¿Cuánto gastamos en total?") == "statistics"
        assert engine._classify_query("Dame el promedio de importes") == "statistics"

    def test_classify_query_comparison(self, llm_config):
        engine = RAGQueryEngine(llm_config=llm_config)

        assert engine._classify_query("Compara ingresos vs gastos") == "comparison"
        assert engine._classify_query("¿Cuál tiene más facturas?") == "comparison"
        assert engine._classify_query("Diferencia entre A y B") == "comparison"

    def test_classify_query_filter(self, llm_config):
        engine = RAGQueryEngine(llm_config=llm_config)

        assert engine._classify_query("Muestra facturas pendientes") == "filter"
        assert engine._classify_query("Lista facturas del proveedor X") == "filter"
        assert engine._classify_query("Busca facturas de Marketing") == "filter"

    def test_classify_query_data(self, llm_config):
        engine = RAGQueryEngine(llm_config=llm_config)

        assert engine._classify_query("¿Qué es una factura?") == "data"
        assert engine._classify_query("Información general") == "data"

    def test_get_k_for_query_type(self, llm_config):
        engine = RAGQueryEngine(llm_config=llm_config)

        assert engine._get_k_for_query_type("statistics") == 5
        assert engine._get_k_for_query_type("comparison") == 8
        assert engine._get_k_for_query_type("filter") == 10
        assert engine._get_k_for_query_type("data") == 6
        assert engine._get_k_for_query_type("unknown") == 5

    def test_check_needs_clarification(self, llm_config):
        engine = RAGQueryEngine(llm_config=llm_config)

        assert engine._check_needs_clarification(
            "No tengo suficiente información para responder."
        )
        assert engine._check_needs_clarification(
            "Could you clarify what you mean?"
        )
        assert not engine._check_needs_clarification(
            "El total de facturas es 5000 EUR."
        )

    def test_generate_clarifying_questions(self, llm_config):
        engine = RAGQueryEngine(llm_config=llm_config)

        questions = engine._generate_clarifying_questions("consulta ambigua")

        assert len(questions) == 3
        assert all(isinstance(q, str) for q in questions)

    def test_conversation_context_management(self, llm_config):
        engine = RAGQueryEngine(llm_config=llm_config)

        context = engine.get_conversation_context()
        assert len(context.messages) == 0

        context.add_user_message("Test")
        assert len(engine.get_conversation_context().messages) == 1

        engine.clear_conversation()
        assert len(engine.get_conversation_context().messages) == 0

    def test_reset_clears_all_state(self, llm_config, sample_gold_dataset):
        engine = RAGQueryEngine(llm_config=llm_config)
        engine._gold_dataset = sample_gold_dataset
        engine._data_records = [{"test": "data"}]
        engine._conversation.add_user_message("Test")

        engine.reset()

        assert engine._vectorstore is None
        assert engine._gold_dataset is None
        assert engine._data_records == []
        assert len(engine._conversation.messages) == 0

    def test_format_record_for_embedding(self, llm_config, sample_records):
        engine = RAGQueryEngine(llm_config=llm_config)

        formatted = engine._format_record_for_embedding(sample_records[0])

        assert "FAC-2024-0001" in formatted
        assert "2024-01-15" in formatted
        assert "Empresa A" in formatted
        assert "1210.00 EUR" in formatted
        assert "Pagada" in formatted

    def test_format_statistics_for_embedding(self, llm_config, sample_gold_dataset):
        engine = RAGQueryEngine(llm_config=llm_config)
        stats = sample_gold_dataset.statistics["importe_total"]

        formatted = engine._format_statistics_for_embedding("importe_total", stats)

        assert "importe_total" in formatted
        assert "Count: 5" in formatted
        assert "Sum: 5000.00" in formatted
        assert "Average: 1000.00" in formatted

    def test_format_summary_for_embedding(
        self, llm_config, sample_gold_dataset, sample_records
    ):
        engine = RAGQueryEngine(llm_config=llm_config)

        formatted = engine._format_summary_for_embedding(
            sample_gold_dataset, sample_records
        )

        assert "Dataset Summary" in formatted
        assert "Total records" in formatted
        assert "Income (Ingreso)" in formatted
        assert "Expense (Gasto)" in formatted

    def test_format_index_for_embedding(self, llm_config, sample_gold_dataset):
        engine = RAGQueryEngine(llm_config=llm_config)
        index = sample_gold_dataset.indexes["categoria"]

        formatted = engine._format_index_for_embedding("categoria", index)

        assert "Index for categoria" in formatted
        assert "2 unique values" in formatted
        assert "Marketing" in formatted or "Tecnología" in formatted

    def test_embed_data_creates_documents(
        self,
        llm_config,
        sample_gold_dataset,
        sample_records,
    ):
        engine = RAGQueryEngine(llm_config=llm_config)
        engine._initialized = True
        engine._embeddings = MagicMock()

        mock_chroma_module = MagicMock()
        mock_vectorstore = MagicMock()
        mock_chroma_module.Chroma.from_documents.return_value = mock_vectorstore

        mock_core_module = MagicMock()

        with patch.dict("sys.modules", {
            "langchain_community.vectorstores": mock_chroma_module,
            "langchain_core.documents": mock_core_module,
        }):
            with patch.object(engine, "_prepare_documents") as mock_prepare:
                mock_prepare.return_value = [MagicMock()]
                engine.embed_data(sample_gold_dataset, sample_records)

        assert engine._gold_dataset == sample_gold_dataset
        assert engine._data_records == sample_records
        assert engine._vectorstore == mock_vectorstore

    def test_query_with_mocked_vectorstore(self, llm_config):
        engine = RAGQueryEngine(llm_config=llm_config)
        engine._initialized = True

        mock_doc = MagicMock()
        mock_doc.page_content = "Test document content"
        mock_doc.metadata = {"doc_type": "record", "record_id": "FAC-001"}

        mock_vectorstore = MagicMock()
        mock_vectorstore.similarity_search.return_value = [mock_doc]
        engine._vectorstore = mock_vectorstore

        mock_llm = MagicMock()
        mock_response = MagicMock()
        mock_response.content = "El total de facturas es 5000 EUR."
        mock_llm.invoke.return_value = mock_response
        engine._llm = mock_llm

        mock_messages_module = MagicMock()

        with patch.dict("sys.modules", {
            "langchain_core.messages": mock_messages_module,
        }):
            response = engine.query("¿Cuál es el total?")

        assert response.query == "¿Cuál es el total?"
        assert "5000 EUR" in response.answer
        assert response.confidence > 0

    def test_extract_supporting_data(self, llm_config):
        engine = RAGQueryEngine(llm_config=llm_config)

        mock_docs = [
            MagicMock(
                metadata={"doc_type": "record", "record_id": "FAC-001"},
                page_content="Invoice FAC-001 content...",
            ),
            MagicMock(
                metadata={"doc_type": "statistics", "source_field": "importe_total"},
                page_content="Statistics content...",
            ),
            MagicMock(
                metadata={"doc_type": "record", "record_id": "FAC-002"},
                page_content="Invoice FAC-002 content...",
            ),
        ]

        supporting_data = engine._extract_supporting_data(mock_docs)

        assert len(supporting_data) == 2
        assert all(d["type"] == "record" for d in supporting_data)
        assert supporting_data[0]["record_id"] == "FAC-001"

    def test_llm_error_handling(self, llm_config):
        engine = RAGQueryEngine(llm_config=llm_config)

        with patch.dict("sys.modules", {"langchain_openai": None}):
            with pytest.raises(LLMError) as exc_info:
                engine._ensure_initialized()

        assert "LangChain OpenAI package not installed" in str(exc_info.value)


class TestRAGQueryEngineIntegration:
    """Integration tests with mocked external dependencies."""

    def test_full_workflow_mocked(self):
        llm_config = LLMConfig(api_key="test-key")

        engine = RAGQueryEngine(llm_config=llm_config)
        assert engine._llm is None

        context = engine.get_conversation_context()
        context.add_user_message("Test question 1")
        context.add_assistant_message("Test answer 1")

        history = context.get_history_text()
        assert "User: Test question 1" in history
        assert "Assistant: Test answer 1" in history

        engine.clear_conversation()
        assert len(engine.get_conversation_context().messages) == 0

    def test_initialization_with_mocked_langchain(self):
        llm_config = LLMConfig(api_key="test-key")

        mock_openai_module = MagicMock()
        mock_chat = MagicMock()
        mock_embeddings = MagicMock()
        mock_openai_module.ChatOpenAI.return_value = mock_chat
        mock_openai_module.OpenAIEmbeddings.return_value = mock_embeddings

        engine = RAGQueryEngine(llm_config=llm_config)

        with patch.dict("sys.modules", {"langchain_openai": mock_openai_module}):
            engine._ensure_initialized()

        assert engine._initialized is True
        assert engine._llm == mock_chat
        assert engine._embeddings == mock_embeddings
