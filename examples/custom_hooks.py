"""Custom hooks for monitoring and extending the pipeline."""

from pathlib import Path
from crm_medallion import Framework, FrameworkConfig

framework = Framework(FrameworkConfig())

# Track processing stats
stats = {"bronze_rows": 0, "silver_valid": 0}

def on_bronze_complete(context):
    stats["bronze_rows"] = context.data.row_count
    print(f"Bronze: ingested {context.data.row_count} rows")
    return None

def on_silver_complete(context):
    stats["silver_valid"] = context.data.valid_records
    print(f"Silver: {context.data.valid_records}/{context.data.total_records} valid")
    return None

framework.register_hook("bronze", "post", on_bronze_complete)
framework.register_hook("silver", "post", on_silver_complete)

framework.process_pipeline(Path("data/invoices.csv"))
print(f"Validation rate: {stats['silver_valid']/stats['bronze_rows']:.1%}")
