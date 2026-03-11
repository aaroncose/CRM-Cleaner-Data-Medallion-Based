"""Config router for application settings."""

from fastapi import APIRouter

from app.models.api_models import ConfigResponse, ConfigUpdateRequest, LLMProvider
from app.services.framework_service import get_framework_service

router = APIRouter()

# Suggested models per provider
PROVIDER_MODELS = {
    "openai": [
        {"id": "gpt-4o", "name": "GPT-4o (Recommended)"},
        {"id": "gpt-4o-mini", "name": "GPT-4o Mini (Fast & Cheap)"},
        {"id": "gpt-4-turbo", "name": "GPT-4 Turbo"},
        {"id": "gpt-3.5-turbo", "name": "GPT-3.5 Turbo (Legacy)"},
    ],
    "anthropic": [
        {"id": "claude-sonnet-4-20250514", "name": "Claude Sonnet 4 (Recommended)"},
        {"id": "claude-opus-4-20250514", "name": "Claude Opus 4 (Most Capable)"},
        {"id": "claude-3-5-sonnet-20241022", "name": "Claude 3.5 Sonnet"},
        {"id": "claude-3-5-haiku-20241022", "name": "Claude 3.5 Haiku (Fast)"},
    ],
    "google": [
        {"id": "gemini-1.5-pro", "name": "Gemini 1.5 Pro (Recommended)"},
        {"id": "gemini-1.5-flash", "name": "Gemini 1.5 Flash (Fast)"},
        {"id": "gemini-2.0-flash", "name": "Gemini 2.0 Flash"},
    ],
    "ollama": [
        {"id": "llama3.2", "name": "Llama 3.2 (Recommended)"},
        {"id": "llama3.1", "name": "Llama 3.1"},
        {"id": "mistral", "name": "Mistral"},
        {"id": "codellama", "name": "Code Llama"},
        {"id": "phi3", "name": "Phi-3"},
    ],
}


@router.get("/config/providers")
async def get_providers():
    """Get available LLM providers and their models."""
    return {
        "providers": [
            {
                "id": "openai",
                "name": "OpenAI",
                "requires_api_key": True,
                "models": PROVIDER_MODELS["openai"],
            },
            {
                "id": "anthropic",
                "name": "Anthropic (Claude)",
                "requires_api_key": True,
                "models": PROVIDER_MODELS["anthropic"],
            },
            {
                "id": "google",
                "name": "Google (Gemini)",
                "requires_api_key": True,
                "models": PROVIDER_MODELS["google"],
            },
            {
                "id": "ollama",
                "name": "Ollama (Local)",
                "requires_api_key": False,
                "models": PROVIDER_MODELS["ollama"],
            },
        ]
    }


@router.get("/config", response_model=ConfigResponse)
async def get_config():
    """Get current configuration."""
    service = get_framework_service()
    config = service.get_config()

    return ConfigResponse(
        provider=LLMProvider(config["provider"]),
        model_name=config["model_name"],
        confidence_threshold=config["confidence_threshold"],
        dedup_auto_threshold=config["dedup_auto_threshold"],
        dedup_review_threshold=config["dedup_review_threshold"],
    )


@router.put("/config", response_model=ConfigResponse)
async def update_config(request: ConfigUpdateRequest):
    """Update configuration."""
    service = get_framework_service()

    updates = {}
    if request.provider is not None:
        updates["provider"] = request.provider.value
    if request.api_key is not None:
        updates["api_key"] = request.api_key
    if request.model_name is not None:
        updates["model_name"] = request.model_name
    if request.confidence_threshold is not None:
        updates["confidence_threshold"] = request.confidence_threshold
    if request.dedup_auto_threshold is not None:
        updates["dedup_auto_threshold"] = request.dedup_auto_threshold
    if request.dedup_review_threshold is not None:
        updates["dedup_review_threshold"] = request.dedup_review_threshold

    config = service.update_config(updates)

    return ConfigResponse(
        provider=LLMProvider(config["provider"]),
        model_name=config["model_name"],
        confidence_threshold=config["confidence_threshold"],
        dedup_auto_threshold=config["dedup_auto_threshold"],
        dedup_review_threshold=config["dedup_review_threshold"],
    )
