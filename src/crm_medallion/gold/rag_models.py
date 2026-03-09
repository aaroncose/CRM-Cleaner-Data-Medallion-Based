"""Data models for the RAG Query Engine."""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


class QueryResponse(BaseModel):
    """Response to a natural language query."""

    query: str = Field(description="Original user query")
    answer: str = Field(description="Natural language answer")
    supporting_data: list[dict[str, Any]] = Field(
        default_factory=list,
        description="Data records supporting the answer",
    )
    confidence: float = Field(
        ge=0.0, le=1.0,
        default=1.0,
        description="Confidence score of the answer",
    )
    query_type: str = Field(
        default="general",
        description="Type of query: data, statistics, filter, comparison",
    )
    clarification_needed: bool = Field(
        default=False,
        description="Whether clarification is needed",
    )
    clarifying_questions: list[str] = Field(
        default_factory=list,
        description="Questions to ask for clarification",
    )


@dataclass
class ConversationMessage:
    """A single message in the conversation."""

    role: str  # "user" or "assistant"
    content: str
    timestamp: datetime = field(default_factory=datetime.now)


@dataclass
class ConversationContext:
    """Context for maintaining conversation history."""

    messages: list[ConversationMessage] = field(default_factory=list)
    max_messages: int = 10

    def add_user_message(self, content: str) -> None:
        """Add a user message to the conversation."""
        self.messages.append(ConversationMessage(role="user", content=content))
        self._trim_history()

    def add_assistant_message(self, content: str) -> None:
        """Add an assistant message to the conversation."""
        self.messages.append(ConversationMessage(role="assistant", content=content))
        self._trim_history()

    def _trim_history(self) -> None:
        """Keep only the last N messages."""
        if len(self.messages) > self.max_messages:
            self.messages = self.messages[-self.max_messages:]

    def get_history_text(self) -> str:
        """Get conversation history as text."""
        lines = []
        for msg in self.messages:
            role = "User" if msg.role == "user" else "Assistant"
            lines.append(f"{role}: {msg.content}")
        return "\n".join(lines)

    def clear(self) -> None:
        """Clear conversation history."""
        self.messages = []


class DocumentMetadata(BaseModel):
    """Metadata for a RAG document."""

    doc_type: str = Field(description="Type: record, statistics, or summary")
    source_field: str | None = Field(default=None, description="Source field for stats")
    record_id: str | None = Field(default=None, description="Record identifier")
    date_range: str | None = Field(default=None, description="Date range covered")
