"""Chat router for RAG queries."""

import traceback

from fastapi import APIRouter, HTTPException

from app.models.api_models import ChatRequest, ChatResponse
from app.services.framework_service import get_framework_service

router = APIRouter()


@router.post("/chat", response_model=ChatResponse)
async def chat(request: ChatRequest):
    """Send a chat message and get a response."""
    service = get_framework_service()

    try:
        result = service.chat_query(request.message, request.run_id)
        return ChatResponse(**result)
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/chat/history")
async def get_chat_history():
    """Get chat history for the current session."""
    # For now, return empty history - can be implemented with session storage
    return {"messages": []}
