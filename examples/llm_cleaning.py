"""LLM-enhanced cleaning for records that fail validation."""

import os
from pathlib import Path
from crm_medallion import Framework, FrameworkConfig
from crm_medallion.config.framework_config import LLMConfig

# Configure with LLM
config = FrameworkConfig(
    llm_enabled=True,
    llm_config=LLMConfig(
        api_key=os.environ["OPENAI_API_KEY"],
        model_name="gpt-4",
        confidence_threshold=0.8,
    ),
)

framework = Framework(config)
result = framework.process_pipeline(Path("data/dirty_invoices.csv"))

# Check LLM corrections
print(f"Total records: {result.silver_dataset.total_records}")
print(f"Valid (rule-based): {result.silver_dataset.valid_records - result.silver_dataset.llm_corrected_records}")
print(f"LLM corrected: {result.silver_dataset.llm_corrected_records}")
print(f"Manual review: {result.silver_dataset.manual_review_records}")
print(f"Failed: {result.silver_dataset.invalid_records}")
