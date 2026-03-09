"""Basic pipeline: CSV to clean data without LLM."""

from pathlib import Path
from crm_medallion import Framework, FrameworkConfig

# Process CSV through all layers
config = FrameworkConfig()
framework = Framework(config)

csv_path = Path("data/invoices.csv")  # Replace with your file
result = framework.process_pipeline(csv_path)

# Print results
print(f"Input rows: {result.bronze_dataset.row_count}")
print(f"Valid records: {result.silver_dataset.valid_records}")
print(f"Invalid records: {result.silver_dataset.invalid_records}")
print(f"Clean CSV: {result.silver_dataset.clean_csv_path}")

# Get statistics
summary = framework.get_summary()
for field, stats in summary.get("statistics", {}).items():
    print(f"{field}: sum={stats['sum']:.2f}, mean={stats['mean']:.2f}")
