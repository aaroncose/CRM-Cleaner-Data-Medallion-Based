"""Command-line interface for CRM Medallion Framework."""

import json
import os
import sys
from pathlib import Path

import click

from crm_medallion import Framework, FrameworkConfig, __version__
from crm_medallion.config.framework_config import GoldConfig, LLMConfig
from crm_medallion.config.schema import FieldType, SchemaDefinition
from crm_medallion.utils.errors import ConfigurationError, FrameworkError
from crm_medallion.utils.logging import get_logger

logger = get_logger(__name__)


def print_error(message: str) -> None:
    """Print error message in red."""
    click.secho(f"Error: {message}", fg="red", err=True)


def print_success(message: str) -> None:
    """Print success message in green."""
    click.secho(message, fg="green")


def print_info(message: str) -> None:
    """Print info message in blue."""
    click.secho(message, fg="blue")


def print_warning(message: str) -> None:
    """Print warning message in yellow."""
    click.secho(message, fg="yellow")


@click.group()
@click.version_option(version=__version__, prog_name="crm-medallion")
def cli():
    """CRM Data Medallion Framework - Data cleaning pipeline using Medallion architecture."""
    pass


@cli.command()
@click.argument("csv_file", type=click.Path(exists=True, path_type=Path))
@click.option(
    "--config", "-c",
    type=click.Path(exists=True, path_type=Path),
    help="Path to YAML configuration file",
)
@click.option(
    "--output", "-o",
    type=click.Path(path_type=Path),
    help="Output directory for processed data",
)
@click.option(
    "--with-llm",
    is_flag=True,
    help="Enable LLM cleaning (requires OPENAI_API_KEY env var)",
)
@click.option(
    "--verbose", "-v",
    is_flag=True,
    help="Enable verbose output",
)
def process(csv_file: Path, config: Path | None, output: Path | None, with_llm: bool, verbose: bool):
    """
    Process a CSV file through the Bronze → Silver → Gold pipeline.

    CSV_FILE: Path to the input CSV file to process.

    Examples:

        crm-medallion process data.csv

        crm-medallion process data.csv --config config.yaml

        crm-medallion process data.csv --with-llm

        crm-medallion process data.csv -o ./output -v
    """
    try:
        if with_llm:
            api_key = os.environ.get("OPENAI_API_KEY")
            if not api_key:
                print_error(
                    "OPENAI_API_KEY environment variable is required when using --with-llm.\n"
                    "Set it with: export OPENAI_API_KEY=sk-..."
                )
                sys.exit(1)

        if config:
            framework_config = FrameworkConfig.from_yaml(config)
        else:
            framework_config = FrameworkConfig()

        if with_llm:
            framework_config.llm_enabled = True
            if framework_config.llm_config is None:
                framework_config.llm_config = LLMConfig(
                    api_key=os.environ.get("OPENAI_API_KEY", "")
                )
            else:
                framework_config.llm_config.api_key = os.environ.get("OPENAI_API_KEY", "")

        if output:
            framework_config.bronze.storage_path = output / "bronze"
            framework_config.silver.output_path = output / "silver"
            framework_config.gold.storage_path = output / "gold"

        framework = Framework(config=framework_config)

        def progress_callback(stage: str, progress: float, message: str):
            if verbose:
                click.echo(f"[{stage.upper()}] {progress:.0%} - {message}")

        print_info(f"Processing: {csv_file.name}")

        result = framework.process_pipeline(csv_file, progress_callback=progress_callback)

        click.echo()
        print_success("Pipeline completed successfully!")
        click.echo()

        click.echo("Summary:")
        click.echo(f"  Total processing time: {result.total_processing_time_seconds:.2f}s")
        click.echo()

        click.echo("Bronze Layer:")
        click.echo(f"  Rows ingested: {result.bronze_dataset.row_count}")
        click.echo(f"  Encoding: {result.bronze_dataset.encoding}")
        click.echo(f"  Storage: {result.bronze_dataset.storage_path}")
        click.echo()

        click.echo("Silver Layer:")
        click.echo(f"  Total records: {result.silver_dataset.total_records}")
        click.echo(f"  Valid records: {result.silver_dataset.valid_records}")
        click.echo(f"  Invalid records: {result.silver_dataset.invalid_records}")
        if result.silver_dataset.llm_corrected_records > 0:
            click.echo(f"  LLM corrected: {result.silver_dataset.llm_corrected_records}")
        if result.silver_dataset.manual_review_records > 0:
            print_warning(f"  Manual review needed: {result.silver_dataset.manual_review_records}")
        click.echo(f"  Output: {result.silver_dataset.clean_csv_path}")
        click.echo()

        click.echo("Gold Layer:")
        click.echo(f"  Records: {result.gold_dataset.record_count}")
        click.echo(f"  Statistics: {len(result.gold_dataset.statistics)} fields")
        click.echo(f"  Indexes: {len(result.gold_dataset.indexes)} fields")
        click.echo(f"  Storage: {result.gold_dataset.storage_path}")

    except ConfigurationError as e:
        print_error(f"Configuration error: {e.message}")
        sys.exit(1)
    except FrameworkError as e:
        print_error(f"Processing error: {e.message}")
        sys.exit(1)
    except FileNotFoundError as e:
        print_error(str(e))
        sys.exit(1)
    except Exception as e:
        print_error(f"Unexpected error: {e}")
        if verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)


@cli.command()
@click.argument("gold_data", type=click.Path(exists=True, path_type=Path))
@click.option(
    "--config", "-c",
    type=click.Path(exists=True, path_type=Path),
    help="Path to YAML configuration file with LLM settings",
)
@click.option(
    "--api-key", "-k",
    envvar="OPENAI_API_KEY",
    help="OpenAI API key (or set OPENAI_API_KEY env var)",
)
def query(gold_data: Path, config: Path | None, api_key: str | None):
    """
    Start an interactive query session with Gold layer data.

    GOLD_DATA: Path to the Gold layer JSON file.

    Examples:

        crm-medallion query ./data/gold/gold.json --api-key sk-...

        crm-medallion query ./data/gold/gold.json --config config.yaml
    """
    if not api_key and not config:
        print_error("API key required. Use --api-key or set OPENAI_API_KEY environment variable.")
        sys.exit(1)

    try:
        if config:
            framework_config = FrameworkConfig.from_yaml(config)
        else:
            framework_config = FrameworkConfig(
                gold=GoldConfig(enable_rag=True),
                llm_enabled=True,
                llm_config=LLMConfig(api_key=api_key or ""),
            )

        if api_key and framework_config.llm_config:
            framework_config.llm_config.api_key = api_key

        from datetime import datetime
        from crm_medallion.gold.models import GoldDataset, FieldStatistics, Index, IndexEntry
        from crm_medallion.gold.rag_engine import RAGQueryEngine

        print_info("Loading Gold dataset...")

        with open(gold_data, "r", encoding="utf-8") as f:
            data = json.load(f)

        records = data.get("records", [])
        if not records:
            print_error("No records found in Gold dataset.")
            sys.exit(1)

        statistics = {}
        for name, stats_data in data.get("statistics", {}).items():
            statistics[name] = FieldStatistics(
                field_name=stats_data.get("field_name", name),
                count=stats_data.get("count", 0),
                sum=stats_data.get("sum", 0.0),
                mean=stats_data.get("mean", 0.0),
                median=stats_data.get("median", 0.0),
                min=stats_data.get("min", 0.0),
                max=stats_data.get("max", 0.0),
                std=stats_data.get("std", 0.0),
            )

        indexes = {}
        for name, idx_data in data.get("indexes", {}).items():
            entries = {}
            for key, entry_data in idx_data.get("entries", {}).items():
                entries[key] = IndexEntry(
                    key=key,
                    row_indices=entry_data.get("row_indices", []),
                    count=entry_data.get("count", 0),
                )
            indexes[name] = Index(
                field_name=name,
                entries=entries,
                unique_values=idx_data.get("unique_values", len(entries)),
            )

        gold_dataset = GoldDataset(
            id=data.get("id", "loaded"),
            silver_dataset_id=data.get("silver_dataset_id", "unknown"),
            storage_path=gold_data,
            aggregation_timestamp=datetime.now(),
            record_count=len(records),
            statistics=statistics,
            indexes=indexes,
            column_names=list(records[0].keys()) if records else [],
        )

        print_info(f"Loaded {len(records)} records from {gold_data.name}")
        print_info("Initializing RAG engine (this may take a moment)...")

        rag_engine = RAGQueryEngine(llm_config=framework_config.llm_config)
        rag_engine.embed_data(gold_dataset, records)

        print_success(f"RAG engine ready with {len(records)} embedded records.")

        click.echo()
        click.echo("Interactive Query Session")
        click.echo("Type your questions in natural language. Type 'exit' or 'quit' to end.")
        click.echo("-" * 60)

        while True:
            try:
                user_input = click.prompt("\nQuestion", type=str)

                if user_input.lower() in ("exit", "quit", "q"):
                    print_info("Goodbye!")
                    break

                if not user_input.strip():
                    continue

                click.echo()
                click.echo("Thinking...")

                response = rag_engine.query(user_input)

                click.echo()
                click.secho("Answer:", fg="cyan", bold=True)
                click.echo(response.answer)

                if response.supporting_data:
                    click.echo()
                    click.secho("Supporting data:", fg="cyan")
                    for item in response.supporting_data[:3]:
                        click.echo(f"  - {item.get('record_id', 'N/A')}: {item.get('content', '')[:100]}...")

                if response.clarification_needed:
                    click.echo()
                    print_warning("Clarification may be needed:")
                    for q in response.clarifying_questions:
                        click.echo(f"  - {q}")

            except KeyboardInterrupt:
                click.echo()
                print_info("Interrupted. Goodbye!")
                break
            except Exception as e:
                print_error(f"Query failed: {e}")

    except ConfigurationError as e:
        print_error(f"Configuration error: {e.message}")
        sys.exit(1)
    except Exception as e:
        print_error(f"Error: {e}")
        sys.exit(1)


@cli.command("validate-config")
@click.argument("config_file", type=click.Path(exists=True, path_type=Path))
def validate_config(config_file: Path):
    """
    Validate a configuration file.

    CONFIG_FILE: Path to the YAML configuration file to validate.

    Examples:

        crm-medallion validate-config config.yaml
    """
    try:
        config = FrameworkConfig.from_yaml(config_file)

        print_success(f"Configuration is valid: {config_file}")
        click.echo()

        click.echo("Configuration summary:")
        click.echo(f"  Bronze storage: {config.bronze.storage_path}")
        click.echo(f"  Silver output: {config.silver.output_path}")
        click.echo(f"  Gold storage: {config.gold.storage_path}")
        click.echo(f"  Batch size: {config.silver.batch_size}")
        click.echo(f"  LLM enabled: {config.llm_enabled}")
        click.echo(f"  RAG enabled: {config.gold.enable_rag}")
        click.echo(f"  Log level: {config.log_level}")
        click.echo(f"  Max memory: {config.max_memory_mb} MB")

        if config.schema:
            click.echo(f"  Schema fields: {len(config.schema.fields)}")

    except ConfigurationError as e:
        print_error(f"Invalid configuration: {e.message}")
        if e.context:
            click.echo("Details:")
            for key, value in e.context.items():
                click.echo(f"  {key}: {value}")
        sys.exit(1)
    except Exception as e:
        print_error(f"Error reading configuration: {e}")
        sys.exit(1)


@cli.command("generate-schema")
@click.option(
    "--output", "-o",
    type=click.Path(path_type=Path),
    help="Output file path (default: stdout)",
)
@click.option(
    "--format", "-f",
    type=click.Choice(["yaml", "json"]),
    default="yaml",
    help="Output format",
)
@click.option(
    "--template",
    type=click.Choice(["factura", "minimal", "custom"]),
    default="factura",
    help="Schema template to use",
)
def generate_schema(output: Path | None, format: str, template: str):
    """
    Generate a schema definition file.

    Examples:

        crm-medallion generate-schema

        crm-medallion generate-schema --template minimal -o schema.yaml

        crm-medallion generate-schema --format json -o schema.json
    """
    import yaml

    if template == "factura":
        schema_dict = {
            "name": "FacturaVenta",
            "fields": [
                {"name": "num_factura", "type": "string", "required": True},
                {"name": "fecha", "type": "date", "required": True},
                {"name": "proveedor", "type": "string", "required": True},
                {"name": "nif_cif", "type": "string", "required": True},
                {"name": "tipo", "type": "string", "required": True, "allowed_values": ["Ingreso", "Gasto"]},
                {"name": "categoria", "type": "string", "required": True},
                {"name": "importe_base", "type": "float", "required": True},
                {"name": "iva", "type": "float", "required": True},
                {"name": "importe_total", "type": "float", "required": True},
                {"name": "estado_factura", "type": "string", "required": True, "allowed_values": ["Pagada", "Pendiente", "Vencida", "Parcialmente pagada"]},
            ],
        }
    elif template == "minimal":
        schema_dict = {
            "name": "MinimalSchema",
            "fields": [
                {"name": "id", "type": "string", "required": True},
                {"name": "name", "type": "string", "required": True},
                {"name": "value", "type": "float", "required": False},
            ],
        }
    else:
        schema_dict = {
            "name": "CustomSchema",
            "fields": [
                {"name": "field1", "type": "string", "required": True},
                {"name": "field2", "type": "integer", "required": False},
            ],
        }

    if format == "yaml":
        content = yaml.dump(schema_dict, default_flow_style=False, allow_unicode=True, sort_keys=False)
    else:
        content = json.dumps(schema_dict, indent=2)

    if output:
        output.write_text(content)
        print_success(f"Schema written to: {output}")
    else:
        click.echo(content)


@cli.command("generate-config")
@click.option(
    "--output", "-o",
    type=click.Path(path_type=Path),
    help="Output file path (default: stdout)",
)
@click.option(
    "--with-llm",
    is_flag=True,
    help="Include LLM configuration section",
)
def generate_config(output: Path | None, with_llm: bool):
    """
    Generate a sample configuration file.

    Examples:

        crm-medallion generate-config

        crm-medallion generate-config --with-llm -o config.yaml
    """
    import yaml

    config_dict = {
        "bronze": {
            "storage_path": "./data/bronze",
            "encoding_detection": True,
        },
        "silver": {
            "output_path": "./data/silver",
            "batch_size": 1000,
        },
        "gold": {
            "storage_path": "./data/gold",
            "enable_rag": with_llm,
        },
        "llm_enabled": with_llm,
        "log_level": "INFO",
        "max_memory_mb": 1024,
        "chunk_size_mb": 10,
    }

    if with_llm:
        config_dict["llm"] = {
            "model_name": "gpt-4o-mini",
            "temperature": 0.0,
            "api_key": "${OPENAI_API_KEY}",
            "confidence_threshold": 0.7,
            "max_retries": 5,
        }

    content = yaml.dump(config_dict, default_flow_style=False, allow_unicode=True, sort_keys=False)

    if output:
        output.write_text(content)
        print_success(f"Configuration written to: {output}")
    else:
        click.echo(content)


@cli.command()
@click.argument("gold_data", type=click.Path(exists=True, path_type=Path))
def summary(gold_data: Path):
    """
    Display summary statistics from a Gold dataset.

    GOLD_DATA: Path to the Gold layer JSON file.

    Examples:

        crm-medallion summary ./data/gold/gold.json
    """
    try:
        with open(gold_data, "r") as f:
            data = json.load(f)

        click.echo(f"Gold Dataset Summary: {gold_data.name}")
        click.echo("=" * 60)

        records = data.get("records", [])
        click.echo(f"\nTotal records: {len(records)}")

        statistics = data.get("statistics", {})
        if statistics:
            click.echo("\nStatistics:")
            for field, stats in statistics.items():
                click.echo(f"\n  {field}:")
                click.echo(f"    Count: {stats.get('count', 'N/A')}")
                click.echo(f"    Sum: {stats.get('sum', 'N/A'):.2f}")
                click.echo(f"    Mean: {stats.get('mean', 'N/A'):.2f}")
                click.echo(f"    Min: {stats.get('min', 'N/A'):.2f}")
                click.echo(f"    Max: {stats.get('max', 'N/A'):.2f}")

        indexes = data.get("indexes", {})
        if indexes:
            click.echo("\nIndexes:")
            for field, idx in indexes.items():
                entries = idx.get("entries", {})
                click.echo(f"\n  {field}: {idx.get('unique_values', len(entries))} unique values")
                top_entries = sorted(
                    entries.items(),
                    key=lambda x: x[1].get("count", 0),
                    reverse=True,
                )[:5]
                for key, entry in top_entries:
                    click.echo(f"    - {key}: {entry.get('count', 'N/A')} records")

    except json.JSONDecodeError as e:
        print_error(f"Invalid JSON file: {e}")
        sys.exit(1)
    except Exception as e:
        print_error(f"Error reading file: {e}")
        sys.exit(1)


def main():
    """Entry point for the CLI."""
    cli()


if __name__ == "__main__":
    main()
