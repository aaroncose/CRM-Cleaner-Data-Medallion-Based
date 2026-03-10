# CRM Cleaner Data Medallion-Based

A Python library for cleaning dirty CRM data using the Medallion architecture (Bronze-Silver-Gold layers).

Transform messy CSV files with inconsistent formats, typos, and missing data into clean, validated, queryable datasets.

## Features

**Medallion Architecture** - Bronze (raw) → Silver (clean) → Gold (analytics)

**Smart Data Cleaning** - Automatic normalization of dates, currencies, whitespace, and more

**Optional LLM Enhancement** - AI-powered correction for ambiguous data

**Pre-computed Analytics** - Statistics, indexes, and segmented aggregations

**Natural Language Queries** - Ask questions about your data in Spanish (RAG)

**Multiple LLM Providers** - OpenAI or Ollama (local, free)

**Schema Auto-Detection** - Infer schema from CSV headers automatically

**Extensible** - Custom cleaning rules, hooks, and validators

**Performance** - Batch processing, streaming for large files, memory limits

## Installation

```bash
# Basic installation
pip install -e .

# With LLM and RAG features (OpenAI)
pip install -e ".[llm,rag]"

# With Ollama support (local LLM, free)
pip install -e ".[ollama,rag]"

# Full installation (all optional features)
pip install -e ".[all]"

# Development installation
pip install -e ".[dev]"
```

## Quick Start

### Python API

```python
from crm_medallion import Framework, FrameworkConfig

# Basic usage with defaults
framework = Framework(FrameworkConfig())
result = framework.process_pipeline("invoices.csv")

print(f"Valid records: {result.silver_dataset.valid_records}")
print(f"Clean CSV: {result.silver_dataset.clean_csv_path}")
print(f"Processing time: {result.total_processing_time_seconds:.2f}s")
```

### CLI Usage

```bash
# Process a CSV file
crm-medallion process data.csv

# Process with custom output directory
crm-medallion process data.csv -o ./output

# Process CSV with LLM enhancement
crm-medallion process data.csv --with-llm

# Process with verbose output
crm-medallion process data.csv -v

# Auto-detect schema from CSV
crm-medallion detect-schema data.csv -o schema.yaml

# View dataset summary
crm-medallion summary ./output/gold/gold.json

# Interactive RAG queries (OpenAI)
crm-medallion query ./output/gold/gold.json --api-key sk-...

# Interactive RAG queries (Ollama - local, free)
crm-medallion query ./output/gold/ --provider ollama

# Query with directory (auto-finds latest Gold JSON)
crm-medallion query ./output/gold/
```

## Configuration

### Generate Configuration Template

```bash
# Generate basic config
crm-medallion generate-config -o config.yaml

# Generate config with LLM settings
crm-medallion generate-config --with-llm -o config.yaml
```

### Example Configuration

```yaml
bronze:
  storage_path: ./data/bronze
  encoding_detection: true

silver:
  output_path: ./data/silver
  batch_size: 1000

gold:
  storage_path: ./data/gold
  enable_rag: true

llm_enabled: true
llm:
  provider: openai           # or "ollama" for local LLM
  model_name: gpt-4o-mini    # or "llama3.2" for Ollama
  temperature: 0.0
  api_key: ${OPENAI_API_KEY} # not required for Ollama
  confidence_threshold: 0.7
  max_retries: 5

log_level: INFO
max_memory_mb: 1024
chunk_size_mb: 10
```

### Load Configuration in Code

```python
from crm_medallion import Framework, FrameworkConfig

# From YAML file
config = FrameworkConfig.from_yaml("config.yaml")
framework = Framework(config)

# From dictionary
config = FrameworkConfig.from_dict({
    "bronze": {"storage_path": "./data/bronze"},
    "silver": {"output_path": "./data/silver"},
    "gold": {"storage_path": "./data/gold"},
})
framework = Framework(config)
```

### Validate Configuration

```bash
crm-medallion validate-config config.yaml
```

## Data Cleaning Features

The framework automatically applies these cleaning rules:

- **Whitespace normalization** - Removes leading/trailing spaces
- **Currency standardization** - `1.234,56 EUR` → `1234.56`
- **Date parsing** - Multiple formats → ISO 8601 (`2024-01-15`)
  - `DD/MM/YYYY`, `YYYY-MM-DD`, `DD-MM-YYYY`
  - Spanish dates: `"15 de enero de 2024"`
- **Invoice number normalization** - `FAC-YYYY-NNNN` format
- **NIF/CIF validation** - Spanish tax ID validation
- **Enum normalization** - `"ingreso"`, `"INGRESO"`, `"Ingrso"` → `"INGRESO"`
- **Amount parsing** - Handles spaces, currency symbols, comma/dot separators

## Schema Definition

### Auto-Detect Schema from CSV

```bash
# Auto-detect schema from CSV headers and sample data
crm-medallion detect-schema data.csv

# Save detected schema to file
crm-medallion detect-schema data.csv -o schema.yaml

# Use custom name and sample size
crm-medallion detect-schema data.csv --name MySchema --sample-rows 50
```

The schema detector analyzes sample rows to infer field types:
- **Integer** - Numeric values without decimals
- **Float** - Numeric values with decimals, currency formats
- **Date** - ISO dates, DD/MM/YYYY, Spanish dates
- **Boolean** - true/false, yes/no, si/no
- **String** - Default for text fields

### Generate Schema Template

```bash
# Generate FacturaVenta schema (default)
crm-medallion generate-schema -o schema.yaml

# Generate minimal schema
crm-medallion generate-schema --template minimal

# Generate as JSON
crm-medallion generate-schema --format json -o schema.json
```

### Example Schema (FacturaVenta)

```yaml
name: FacturaVenta
fields:
  - name: num_factura
    type: string
    required: true
  - name: fecha
    type: date
    required: true
  - name: proveedor
    type: string
    required: true
  - name: nif_cif
    type: string
    required: true
  - name: tipo
    type: string
    required: true
    allowed_values: [Ingreso, Gasto]
  - name: categoria
    type: string
    required: true
  - name: importe_base
    type: float
    required: true
  - name: iva
    type: float
    required: true
  - name: importe_total
    type: float
    required: true
  - name: estado_factura
    type: string
    required: true
    allowed_values: [Pagada, Pendiente, Vencida, Parcialmente pagada]
```

## Architecture

```
CSV Input → Bronze (raw) → Silver (clean) → Gold (analytics)
```

### Bronze Layer
- Raw data ingestion with encoding detection (UTF-8, Latin-1, etc.)
- Preserves original data without modification
- Metadata tracking (timestamp, row count, encoding)

### Silver Layer
- Schema validation using Pydantic
- Automatic data cleaning rules
- Optional LLM enhancement for ambiguous data
- Batch processing for efficiency
- Outputs clean CSV with validation statistics

### Gold Layer
- Pre-computed statistics (sum, mean, median, min, max, std)
- Indexes for fast querying (by date, category, provider)
- Vector embeddings for RAG queries
- Natural language query interface

## CLI Commands Reference

### `process` - Process CSV through pipeline

```bash
crm-medallion process <csv_file> [OPTIONS]

Options:
  -c, --config PATH       Path to YAML configuration file
  -o, --output PATH       Output directory for processed data
  --with-llm              Enable LLM cleaning (requires OPENAI_API_KEY)
  -v, --verbose           Enable verbose output
  --help                  Show help message

Examples:
  crm-medallion process data.csv
  crm-medallion process data.csv --config config.yaml
  crm-medallion process data.csv --with-llm -v
  crm-medallion process data.csv -o ./output
```

### `query` - Interactive RAG queries

```bash
crm-medallion query <gold_data> [OPTIONS]

Options:
  -c, --config PATH       Path to YAML configuration file
  -k, --api-key TEXT      OpenAI API key (or set OPENAI_API_KEY env var)
  -p, --provider TEXT     LLM provider: "openai" or "ollama" (default: openai)
  --no-support            Hide supporting data in responses
  --help                  Show help message

Examples:
  # With OpenAI
  crm-medallion query ./data/gold/gold.json --api-key sk-...

  # With Ollama (local, free - no API key needed)
  crm-medallion query ./data/gold/gold.json --provider ollama

  # Auto-find latest Gold JSON in directory
  crm-medallion query ./data/gold/

  # Hide supporting data in responses
  crm-medallion query ./data/gold/ --no-support

  # In the interactive session:
  Question: ¿Cuántas facturas tenemos de Marketing?
  Question: ¿Cuál es el importe total de facturas pagadas?
  Question: exit
```

### `detect-schema` - Auto-detect schema from CSV

```bash
crm-medallion detect-schema <csv_file> [OPTIONS]

Options:
  -o, --output PATH       Output file path (default: stdout)
  -n, --name TEXT         Name for the generated schema
  -s, --sample-rows INT   Number of rows to sample (default: 20)
  --help                  Show help message

Examples:
  crm-medallion detect-schema data.csv
  crm-medallion detect-schema data.csv -o schema.yaml
  crm-medallion detect-schema data.csv --name FacturaSchema -s 50
```

### `summary` - Display dataset statistics

```bash
crm-medallion summary <gold_data>

Examples:
  crm-medallion summary ./data/gold/gold.json
```

### `validate-config` - Validate configuration file

```bash
crm-medallion validate-config <config_file>

Examples:
  crm-medallion validate-config config.yaml
```

### `generate-config` - Generate configuration template

```bash
crm-medallion generate-config [OPTIONS]

Options:
  -o, --output PATH       Output file path (default: stdout)
  --with-llm              Include LLM configuration section
  --help                  Show help message

Examples:
  crm-medallion generate-config
  crm-medallion generate-config --with-llm -o config.yaml
```

### `generate-schema` - Generate schema template

```bash
crm-medallion generate-schema [OPTIONS]

Options:
  -o, --output PATH       Output file path (default: stdout)
  -f, --format [yaml|json] Output format (default: yaml)
  --template [factura|minimal|custom] Schema template (default: factura)
  --help                  Show help message

Examples:
  crm-medallion generate-schema
  crm-medallion generate-schema --template minimal -o schema.yaml
  crm-medallion generate-schema --format json -o schema.json
```

## LLM Enhancement (Optional)

Enable LLM-powered data cleaning for records that fail validation:

```bash
# Set your OpenAI API key
export OPENAI_API_KEY=sk-...

# Process with LLM enhancement
crm-medallion process data.csv --with-llm
```

The LLM cleaner will:
- Attempt to correct ambiguous or invalid data
- Provide confidence scores for corrections
- Flag low-confidence records for manual review
- Log all corrections with reasoning

## RAG Queries (Optional)

Query your data using natural language. **Responses are always in Spanish.**

### With OpenAI

```bash
export OPENAI_API_KEY=sk-...
crm-medallion query ./data/gold/gold.json
```

### With Ollama (Local, Free)

First, install and run Ollama:
```bash
# Install Ollama (https://ollama.ai)
# Pull a model
ollama pull llama3.2

# Run the query command
crm-medallion query ./data/gold/ --provider ollama
```

### Example Queries

- "¿Cuántas facturas tenemos de Marketing?"
- "¿Cuál es el importe total de facturas pagadas?"
- "¿Cuántas facturas están pendientes por proveedor?"
- "Muéstrame las facturas vencidas de más de 10.000 euros"
- "¿Qué proveedor tiene más facturas?"

### Query Features

- **Auto-find latest Gold JSON** - Pass a directory and it finds the most recent file
- **Segmented statistics** - Aggregations by tipo, estado_factura, categoria, proveedor
- **Hide supporting data** - Use `--no-support` or say "sin datos de soporte" in your question
- **Spanish responses** - All answers are in Spanish regardless of question language

## Extending the Framework

### Custom Cleaning Rules

```python
from crm_medallion.silver.rules import CleaningRule

class UppercaseRule(CleaningRule):
    """Convert specific fields to uppercase."""
    
    def applies_to(self, field: str) -> bool:
        return field in ["categoria", "estado_factura"]
    
    def clean(self, value, field: str):
        if isinstance(value, str):
            return value.upper()
        return value

# Register the rule
from crm_medallion import Framework, FrameworkConfig

config = FrameworkConfig()
framework = Framework(config)
framework._get_silver_layer().register_cleaning_rule(UppercaseRule())
```

### Custom Hooks

```python
def log_progress(context):
    """Log processing progress."""
    print(f"Processing {len(context.data)} records at {context.stage}")
    return None

def validate_totals(context):
    """Custom validation after Silver layer."""
    for record in context.data:
        base = record.get("importe_base", 0)
        iva = record.get("iva", 0)
        total = record.get("importe_total", 0)
        if abs((base + iva) - total) > 0.01:
            print(f"Warning: Total mismatch in record {record.get('num_factura')}")
    return None

# Register hooks
framework.register_hook("silver", "pre", log_progress)
framework.register_hook("silver", "post", validate_totals)
```

## Project Structure

```
crm-medallion/
├── src/crm_medallion/
│   ├── bronze/          # Raw data ingestion
│   ├── silver/          # Validation and cleaning
│   ├── gold/            # Aggregation and RAG
│   ├── config/          # Configuration management
│   ├── cli/             # Command-line interface
│   └── utils/           # Utilities (logging, errors, retry)
├── tests/
│   ├── unit/            # Unit tests
│   ├── integration/     # Integration tests
│   └── fixtures/        # Test data files
├── examples/            # Example scripts
├── pyproject.toml       # Package configuration
└── README.md
```

## Requirements

**Core Dependencies:**
- Python 3.9+
- pandas >= 2.0.0
- pydantic >= 2.0.0
- click >= 8.0.0
- chardet >= 5.0.0
- pyyaml >= 6.0
- thefuzz >= 0.22.0 (fuzzy matching for deduplication)

**Optional Dependencies (LLM):**
- langchain >= 0.1.0
- langchain-openai >= 0.0.5 (OpenAI integration)
- langgraph >= 0.0.1

**Optional Dependencies (RAG):**
- chromadb >= 0.4.0
- langchain-community >= 0.0.20

**Optional Dependencies (Ollama):**
- langchain-community >= 0.0.20

## Development

```bash
# Clone the repository
git clone <repository-url>
cd crm-medallion

# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install in development mode with all dependencies
pip install -e ".[dev]"

# Run tests
pytest

# Run tests with coverage
pytest --cov=crm_medallion --cov-report=html

# Run linting
ruff check src/

# Format code
ruff format src/
```




## Recent Updates (v0.2.0)

✅ **Schema Auto-Detection** - `crm-medallion detect-schema data.csv`
✅ **Ollama Support** - Use local LLMs for free with `--provider ollama`
✅ **Segmented Statistics** - Aggregations by tipo, estado, categoria, proveedor
✅ **Spanish Responses** - RAG always responds in Spanish
✅ **Auto-find Gold JSON** - Pass directory to query command
✅ **importe_pendiente Calculation** - Auto-calculated based on estado_factura
✅ **Hide Supporting Data** - `--no-support` option in query command

---

🚧 **Upcoming Features** - Currently in development:

### Web UI Dashboard
- Browser-based interface for data processing
- Drag-and-drop CSV upload
- Real-time processing progress
- Visual data quality reports
- Interactive data preview (Bronze → Silver → Gold)

### Advanced Analytics
- Time-series analysis and forecasting
- Anomaly detection in financial data
- Custom aggregation pipelines
- Export to Excel/PDF reports

### Enhanced LLM Features
- Multi-model support (Claude, Gemini)
- Batch LLM processing for cost optimization
- Fine-tuned models for domain-specific cleaning

### Data Lineage & Audit
- Track data transformations across layers
- Audit trail for compliance
- Rollback to previous versions

### Performance Improvements
- Parallel processing for large datasets
- Incremental updates (process only new rows)
- Distributed processing support

---

## License

MIT 
@AARONCORTESSERRANO
