"""Natural language queries over processed data."""

import os
from pathlib import Path
from crm_medallion import Framework, FrameworkConfig
from crm_medallion.config.framework_config import GoldConfig, LLMConfig

# Configure with RAG enabled
config = FrameworkConfig(
    gold=GoldConfig(enable_rag=True),
    llm_enabled=True,
    llm_config=LLMConfig(api_key=os.environ["OPENAI_API_KEY"]),
)

framework = Framework(config)
framework.process_pipeline(Path("data/invoices.csv"))

# Query in natural language
queries = [
    "What is the total revenue?",
    "Which provider has the most invoices?",
    "Show pending invoices over 1000 EUR",
]

for q in queries:
    response = framework.query(q)
    print(f"Q: {q}")
    print(f"A: {response.answer}\n")
