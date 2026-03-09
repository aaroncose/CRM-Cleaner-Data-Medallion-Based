"""Tests for FrameworkConfig class."""

import tempfile
from pathlib import Path

import pytest
import yaml

from crm_medallion.config.framework_config import (
    FrameworkConfig,
    LLMConfig,
    BronzeConfig,
    SilverConfig,
    GoldConfig,
)
from crm_medallion.config.schema import SchemaDefinition, FieldDefinition, FieldType
from crm_medallion.utils.errors import ConfigurationError


class TestLLMConfig:
    def test_default_values(self):
        config = LLMConfig(api_key="test-key")
        assert config.model_name == "gpt-4o-mini"
        assert config.temperature == 0.0
        assert config.confidence_threshold == 0.7
        assert config.max_retries == 5

    def test_validate_missing_api_key(self):
        config = LLMConfig()
        with pytest.raises(ConfigurationError) as exc_info:
            config.validate()
        assert "API key" in str(exc_info.value)

    def test_validate_invalid_temperature(self):
        config = LLMConfig(api_key="test", temperature=3.0)
        with pytest.raises(ConfigurationError) as exc_info:
            config.validate()
        assert "Temperature" in str(exc_info.value)

    def test_validate_invalid_confidence_threshold(self):
        config = LLMConfig(api_key="test", confidence_threshold=1.5)
        with pytest.raises(ConfigurationError) as exc_info:
            config.validate()
        assert "Confidence threshold" in str(exc_info.value)

    def test_validate_success(self):
        config = LLMConfig(api_key="test-key", temperature=0.5)
        config.validate()


class TestFrameworkConfig:
    def test_default_values(self):
        config = FrameworkConfig()
        assert config.llm_enabled is False
        assert config.log_level == "INFO"
        assert config.max_memory_mb == 1024
        assert config.chunk_size_mb == 10

    def test_validate_llm_enabled_without_config(self):
        with pytest.raises(ConfigurationError) as exc_info:
            FrameworkConfig(llm_enabled=True)
        assert "LLM config is required" in str(exc_info.value)

    def test_validate_llm_enabled_with_config(self):
        llm_config = LLMConfig(api_key="test-key")
        config = FrameworkConfig(llm_enabled=True, llm_config=llm_config)
        assert config.llm_enabled is True

    def test_validate_invalid_batch_size(self):
        with pytest.raises(ConfigurationError) as exc_info:
            config = FrameworkConfig()
            config.silver.batch_size = 0
            config.validate()
        assert "Batch size" in str(exc_info.value)

    def test_validate_invalid_max_memory(self):
        with pytest.raises(ConfigurationError) as exc_info:
            config = FrameworkConfig()
            config.max_memory_mb = 64
            config.validate()
        assert "Maximum memory" in str(exc_info.value)

    def test_validate_invalid_log_level(self):
        with pytest.raises(ConfigurationError) as exc_info:
            config = FrameworkConfig()
            config.log_level = "INVALID"
            config.validate()
        assert "Invalid log level" in str(exc_info.value)

    def test_from_dict_minimal(self):
        config_dict = {}
        config = FrameworkConfig.from_dict(config_dict)
        assert config.log_level == "INFO"
        assert config.llm_enabled is False

    def test_from_dict_with_schema(self):
        config_dict = {
            "schema": {
                "name": "TestSchema",
                "fields": [
                    {"name": "id", "type": "int"},
                ],
            },
        }
        config = FrameworkConfig.from_dict(config_dict)
        assert config.schema is not None
        assert config.schema.name == "TestSchema"

    def test_from_dict_with_llm(self):
        config_dict = {
            "llm_enabled": True,
            "llm": {
                "api_key": "test-key",
                "model_name": "gpt-3.5-turbo",
                "temperature": 0.5,
            },
        }
        config = FrameworkConfig.from_dict(config_dict)
        assert config.llm_enabled is True
        assert config.llm_config.model_name == "gpt-3.5-turbo"
        assert config.llm_config.temperature == 0.5

    def test_from_yaml(self):
        config_dict = {
            "log_level": "DEBUG",
            "bronze": {
                "storage_path": "/tmp/bronze",
            },
            "silver": {
                "batch_size": 500,
            },
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            yaml_path = Path(tmpdir) / "config.yaml"
            with open(yaml_path, "w") as f:
                yaml.dump(config_dict, f)

            config = FrameworkConfig.from_yaml(yaml_path)
            assert config.log_level == "DEBUG"
            assert config.bronze.storage_path == Path("/tmp/bronze")
            assert config.silver.batch_size == 500

    def test_from_yaml_file_not_found(self):
        with pytest.raises(ConfigurationError) as exc_info:
            FrameworkConfig.from_yaml(Path("/nonexistent/config.yaml"))
        assert "not found" in str(exc_info.value)

    def test_from_yaml_invalid_yaml(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            yaml_path = Path(tmpdir) / "invalid.yaml"
            with open(yaml_path, "w") as f:
                f.write("invalid: yaml: content: [")

            with pytest.raises(ConfigurationError) as exc_info:
                FrameworkConfig.from_yaml(yaml_path)
            assert "Invalid YAML" in str(exc_info.value)

    def test_to_dict(self):
        config = FrameworkConfig(
            log_level="DEBUG",
            max_memory_mb=2048,
        )
        result = config.to_dict()
        assert result["log_level"] == "DEBUG"
        assert result["max_memory_mb"] == 2048
        assert "bronze" in result
        assert "silver" in result
        assert "gold" in result

    def test_register_hook(self):
        config = FrameworkConfig()

        def my_hook(data):
            return data

        config.register_hook("bronze", "pre", my_hook)
        assert my_hook in config.pre_bronze_hooks

        config.register_hook("silver", "post", my_hook)
        assert my_hook in config.post_silver_hooks

    def test_register_hook_invalid(self):
        config = FrameworkConfig()

        with pytest.raises(ConfigurationError) as exc_info:
            config.register_hook("invalid", "pre", lambda x: x)
        assert "Invalid hook" in str(exc_info.value)
