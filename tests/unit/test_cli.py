"""Tests for the CLI interface."""

import json
import tempfile
from pathlib import Path

import pytest
from click.testing import CliRunner

from crm_medallion.cli.main import cli


FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"


class TestCLIProcess:
    @pytest.fixture
    def runner(self):
        return CliRunner()

    @pytest.fixture
    def temp_output(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            yield Path(tmpdir)

    def test_process_valid_csv(self, runner, temp_output):
        result = runner.invoke(
            cli,
            ["process", str(FIXTURES_DIR / "sample_valid.csv"), "-o", str(temp_output)],
        )

        assert result.exit_code == 0
        assert "Pipeline completed successfully" in result.output
        assert "Bronze Layer" in result.output
        assert "Silver Layer" in result.output
        assert "Gold Layer" in result.output

    def test_process_with_verbose(self, runner, temp_output):
        result = runner.invoke(
            cli,
            ["process", str(FIXTURES_DIR / "sample_valid.csv"), "-o", str(temp_output), "-v"],
        )

        assert result.exit_code == 0
        assert "[BRONZE]" in result.output or "Bronze Layer" in result.output

    def test_process_file_not_found(self, runner):
        result = runner.invoke(
            cli,
            ["process", "/nonexistent/file.csv"],
        )

        assert result.exit_code != 0

    def test_process_creates_output_files(self, runner, temp_output):
        runner.invoke(
            cli,
            ["process", str(FIXTURES_DIR / "sample_valid.csv"), "-o", str(temp_output)],
        )

        bronze_files = list((temp_output / "bronze").glob("*.csv"))
        silver_files = list((temp_output / "silver").glob("*.csv"))
        gold_files = list((temp_output / "gold").glob("*.json"))

        assert len(bronze_files) > 0
        assert len(silver_files) > 0
        assert len(gold_files) > 0

    def test_process_with_config_file(self, runner, temp_output):
        config_content = f"""
bronze:
  storage_path: {temp_output}/bronze
silver:
  output_path: {temp_output}/silver
  batch_size: 500
gold:
  storage_path: {temp_output}/gold
  enable_rag: false
log_level: INFO
"""
        config_file = temp_output / "config.yaml"
        config_file.write_text(config_content)

        result = runner.invoke(
            cli,
            ["process", str(FIXTURES_DIR / "sample_valid.csv"), "-c", str(config_file)],
        )

        assert result.exit_code == 0

    def test_process_with_llm_no_api_key(self, runner, temp_output):
        result = runner.invoke(
            cli,
            ["process", str(FIXTURES_DIR / "sample_valid.csv"), "--with-llm", "-o", str(temp_output)],
            env={"OPENAI_API_KEY": ""},
        )

        assert result.exit_code == 1
        assert "OPENAI_API_KEY" in result.output
        assert "Error" in result.output

    def test_process_help_shows_with_llm_option(self, runner):
        result = runner.invoke(cli, ["process", "--help"])

        assert result.exit_code == 0
        assert "--with-llm" in result.output
        assert "LLM cleaning" in result.output


class TestCLIValidateConfig:
    @pytest.fixture
    def runner(self):
        return CliRunner()

    @pytest.fixture
    def temp_dir(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            yield Path(tmpdir)

    def test_validate_valid_config(self, runner, temp_dir):
        config_content = """
bronze:
  storage_path: ./data/bronze
silver:
  output_path: ./data/silver
  batch_size: 1000
gold:
  storage_path: ./data/gold
log_level: INFO
"""
        config_file = temp_dir / "valid_config.yaml"
        config_file.write_text(config_content)

        result = runner.invoke(cli, ["validate-config", str(config_file)])

        assert result.exit_code == 0
        assert "Configuration is valid" in result.output

    def test_validate_invalid_config(self, runner, temp_dir):
        config_content = """
silver:
  batch_size: -1
"""
        config_file = temp_dir / "invalid_config.yaml"
        config_file.write_text(config_content)

        result = runner.invoke(cli, ["validate-config", str(config_file)])

        assert result.exit_code != 0
        assert "Invalid configuration" in result.output or "Error" in result.output

    def test_validate_missing_file(self, runner):
        result = runner.invoke(cli, ["validate-config", "/nonexistent/config.yaml"])

        assert result.exit_code != 0


class TestCLIGenerateSchema:
    @pytest.fixture
    def runner(self):
        return CliRunner()

    @pytest.fixture
    def temp_dir(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            yield Path(tmpdir)

    def test_generate_schema_default(self, runner):
        result = runner.invoke(cli, ["generate-schema"])

        assert result.exit_code == 0
        assert "FacturaVenta" in result.output
        assert "num_factura" in result.output
        assert "importe_total" in result.output

    def test_generate_schema_minimal(self, runner):
        result = runner.invoke(cli, ["generate-schema", "--template", "minimal"])

        assert result.exit_code == 0
        assert "MinimalSchema" in result.output

    def test_generate_schema_json_format(self, runner):
        result = runner.invoke(cli, ["generate-schema", "-f", "json"])

        assert result.exit_code == 0
        data = json.loads(result.output)
        assert "name" in data
        assert "fields" in data

    def test_generate_schema_to_file(self, runner, temp_dir):
        output_file = temp_dir / "schema.yaml"

        result = runner.invoke(cli, ["generate-schema", "-o", str(output_file)])

        assert result.exit_code == 0
        assert output_file.exists()
        content = output_file.read_text()
        assert "FacturaVenta" in content


class TestCLIGenerateConfig:
    @pytest.fixture
    def runner(self):
        return CliRunner()

    @pytest.fixture
    def temp_dir(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            yield Path(tmpdir)

    def test_generate_config_default(self, runner):
        result = runner.invoke(cli, ["generate-config"])

        assert result.exit_code == 0
        assert "bronze:" in result.output
        assert "silver:" in result.output
        assert "gold:" in result.output

    def test_generate_config_with_llm(self, runner):
        result = runner.invoke(cli, ["generate-config", "--with-llm"])

        assert result.exit_code == 0
        assert "llm:" in result.output
        assert "model_name" in result.output
        assert "api_key" in result.output

    def test_generate_config_to_file(self, runner, temp_dir):
        output_file = temp_dir / "config.yaml"

        result = runner.invoke(cli, ["generate-config", "-o", str(output_file)])

        assert result.exit_code == 0
        assert output_file.exists()


class TestCLISummary:
    @pytest.fixture
    def runner(self):
        return CliRunner()

    @pytest.fixture
    def temp_dir(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            yield Path(tmpdir)

    @pytest.fixture
    def sample_gold_file(self, temp_dir):
        gold_data = {
            "records": [
                {"id": "1", "value": 100},
                {"id": "2", "value": 200},
            ],
            "statistics": {
                "value": {
                    "count": 2,
                    "sum": 300.0,
                    "mean": 150.0,
                    "min": 100.0,
                    "max": 200.0,
                }
            },
            "indexes": {
                "id": {
                    "unique_values": 2,
                    "entries": {
                        "1": {"count": 1},
                        "2": {"count": 1},
                    }
                }
            }
        }

        gold_file = temp_dir / "gold.json"
        gold_file.write_text(json.dumps(gold_data))
        return gold_file

    def test_summary_command(self, runner, sample_gold_file):
        result = runner.invoke(cli, ["summary", str(sample_gold_file)])

        assert result.exit_code == 0
        assert "Total records: 2" in result.output
        assert "Statistics:" in result.output
        assert "Indexes:" in result.output

    def test_summary_invalid_file(self, runner, temp_dir):
        invalid_file = temp_dir / "invalid.json"
        invalid_file.write_text("not valid json")

        result = runner.invoke(cli, ["summary", str(invalid_file)])

        assert result.exit_code != 0
        assert "Invalid JSON" in result.output or "Error" in result.output


class TestCLIHelp:
    @pytest.fixture
    def runner(self):
        return CliRunner()

    def test_main_help(self, runner):
        result = runner.invoke(cli, ["--help"])

        assert result.exit_code == 0
        assert "CRM Data Medallion Framework" in result.output
        assert "process" in result.output
        assert "validate-config" in result.output

    def test_process_help(self, runner):
        result = runner.invoke(cli, ["process", "--help"])

        assert result.exit_code == 0
        assert "CSV_FILE" in result.output
        assert "--config" in result.output
        assert "--output" in result.output

    def test_version(self, runner):
        result = runner.invoke(cli, ["--version"])

        assert result.exit_code == 0
        assert "crm-medallion" in result.output
