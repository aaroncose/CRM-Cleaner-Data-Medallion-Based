"""Main framework configuration."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

import yaml

from crm_medallion.config.schema import SchemaDefinition
from crm_medallion.utils.errors import ConfigurationError


@dataclass
class LLMConfig:
    """Configuration for LLM features."""

    model_name: str = "gpt-4o-mini"
    temperature: float = 0.0
    api_key: str = ""
    confidence_threshold: float = 0.7
    max_retries: int = 5
    initial_retry_delay: float = 1.0
    backoff_multiplier: float = 2.0
    provider: str = "openai"  # "openai" or "ollama"

    def validate(self) -> None:
        """Validate LLM configuration."""
        valid_providers = ("openai", "anthropic", "google", "ollama")
        if self.provider not in valid_providers:
            raise ConfigurationError(
                f"Invalid provider: {self.provider}. Must be one of: {', '.join(valid_providers)}",
                context={"field": "llm_config.provider", "value": self.provider},
            )
        # API key required for cloud providers
        if self.provider in ("openai", "anthropic", "google") and not self.api_key:
            raise ConfigurationError(
                f"API key is required for {self.provider} provider",
                context={"field": "llm_config.api_key"},
            )
        if not 0.0 <= self.temperature <= 2.0:
            raise ConfigurationError(
                "Temperature must be between 0.0 and 2.0",
                context={"field": "llm_config.temperature", "value": self.temperature},
            )
        if not 0.0 <= self.confidence_threshold <= 1.0:
            raise ConfigurationError(
                "Confidence threshold must be between 0.0 and 1.0",
                context={
                    "field": "llm_config.confidence_threshold",
                    "value": self.confidence_threshold,
                },
            )


@dataclass
class OllamaConfig:
    """Configuration for Ollama (local LLM) provider."""

    host: str = "http://localhost:11434"
    model_name: str = "llama3.2"
    temperature: float = 0.0


@dataclass
class BronzeConfig:
    """Configuration for Bronze layer."""

    storage_path: Path = field(default_factory=lambda: Path("./data/bronze"))
    encoding_detection: bool = True


@dataclass
class SilverConfig:
    """Configuration for Silver layer."""

    output_path: Path = field(default_factory=lambda: Path("./data/silver"))
    batch_size: int = 1000


@dataclass
class GoldConfig:
    """Configuration for Gold layer."""

    storage_path: Path = field(default_factory=lambda: Path("./data/gold"))
    enable_rag: bool = True


Hook = Callable[[Any], Any]


@dataclass
class FrameworkConfig:
    """Main framework configuration.

    Holds all settings for Bronze, Silver, and Gold layers plus LLM options.

    Attributes:
        bronze: Bronze layer storage settings.
        silver: Silver layer output and batch settings.
        gold: Gold layer storage and RAG settings.
        llm_enabled: Whether to use LLM for data cleaning.
        llm_config: LLM provider settings (required if llm_enabled).

    Example:
        >>> config = FrameworkConfig.from_yaml("config.yaml")
        >>> config = FrameworkConfig(llm_enabled=True, llm_config=LLMConfig(...))
    """

    schema: SchemaDefinition | None = None

    bronze: BronzeConfig = field(default_factory=BronzeConfig)
    silver: SilverConfig = field(default_factory=SilverConfig)
    gold: GoldConfig = field(default_factory=GoldConfig)

    llm_enabled: bool = False
    llm_config: LLMConfig | None = None

    log_level: str = "INFO"
    log_file: Path | None = None

    max_memory_mb: int = 1024
    chunk_size_mb: int = 10

    pre_bronze_hooks: list[Hook] = field(default_factory=list)
    post_bronze_hooks: list[Hook] = field(default_factory=list)
    pre_silver_hooks: list[Hook] = field(default_factory=list)
    post_silver_hooks: list[Hook] = field(default_factory=list)
    pre_gold_hooks: list[Hook] = field(default_factory=list)
    post_gold_hooks: list[Hook] = field(default_factory=list)

    def __post_init__(self) -> None:
        """Validate configuration after initialization."""
        self.validate()

    def validate(self) -> None:
        """Validate the configuration and raise ConfigurationError if invalid."""
        if self.llm_enabled and self.llm_config is None:
            raise ConfigurationError(
                "LLM config is required when LLM is enabled",
                context={"field": "llm_config"},
            )

        if self.llm_enabled and self.llm_config:
            self.llm_config.validate()

        if self.silver.batch_size < 1:
            raise ConfigurationError(
                "Batch size must be at least 1",
                context={"field": "silver.batch_size", "value": self.silver.batch_size},
            )

        if self.max_memory_mb < 128:
            raise ConfigurationError(
                "Maximum memory must be at least 128 MB",
                context={"field": "max_memory_mb", "value": self.max_memory_mb},
            )

        if self.chunk_size_mb < 1:
            raise ConfigurationError(
                "Chunk size must be at least 1 MB",
                context={"field": "chunk_size_mb", "value": self.chunk_size_mb},
            )

        valid_log_levels = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        if self.log_level.upper() not in valid_log_levels:
            raise ConfigurationError(
                f"Invalid log level: {self.log_level}",
                context={"field": "log_level", "valid_values": list(valid_log_levels)},
            )

    @classmethod
    def from_yaml(cls, yaml_path: Path) -> FrameworkConfig:
        """Load configuration from YAML file."""
        if not yaml_path.exists():
            raise ConfigurationError(
                f"Configuration file not found: {yaml_path}",
                context={"path": str(yaml_path)},
            )

        try:
            with open(yaml_path, "r", encoding="utf-8") as f:
                data = yaml.safe_load(f)
        except yaml.YAMLError as e:
            raise ConfigurationError(
                f"Invalid YAML in configuration file: {e}",
                context={"path": str(yaml_path)},
            )

        return cls.from_dict(data)

    @classmethod
    def from_dict(cls, config_dict: dict[str, Any]) -> FrameworkConfig:
        """Create configuration from dictionary."""
        schema = None
        if "schema" in config_dict:
            schema = SchemaDefinition.from_dict(config_dict["schema"])

        bronze_data = config_dict.get("bronze", {})
        bronze = BronzeConfig(
            storage_path=Path(bronze_data.get("storage_path", "./data/bronze")),
            encoding_detection=bronze_data.get("encoding_detection", True),
        )

        silver_data = config_dict.get("silver", {})
        silver = SilverConfig(
            output_path=Path(silver_data.get("output_path", "./data/silver")),
            batch_size=silver_data.get("batch_size", 1000),
        )

        gold_data = config_dict.get("gold", {})
        gold = GoldConfig(
            storage_path=Path(gold_data.get("storage_path", "./data/gold")),
            enable_rag=gold_data.get("enable_rag", True),
        )

        llm_config = None
        if "llm" in config_dict:
            llm_data = config_dict["llm"]
            llm_config = LLMConfig(
                model_name=llm_data.get("model_name", "gpt-4o-mini"),
                temperature=llm_data.get("temperature", 0.0),
                api_key=llm_data.get("api_key", ""),
                confidence_threshold=llm_data.get("confidence_threshold", 0.7),
                max_retries=llm_data.get("max_retries", 5),
                provider=llm_data.get("provider", "openai"),
            )

        log_file = None
        if "log_file" in config_dict and config_dict["log_file"]:
            log_file = Path(config_dict["log_file"])

        return cls(
            schema=schema,
            bronze=bronze,
            silver=silver,
            gold=gold,
            llm_enabled=config_dict.get("llm_enabled", False),
            llm_config=llm_config,
            log_level=config_dict.get("log_level", "INFO"),
            log_file=log_file,
            max_memory_mb=config_dict.get("max_memory_mb", 1024),
            chunk_size_mb=config_dict.get("chunk_size_mb", 10),
        )

    def to_dict(self) -> dict[str, Any]:
        """Convert configuration to dictionary."""
        result: dict[str, Any] = {
            "bronze": {
                "storage_path": str(self.bronze.storage_path),
                "encoding_detection": self.bronze.encoding_detection,
            },
            "silver": {
                "output_path": str(self.silver.output_path),
                "batch_size": self.silver.batch_size,
            },
            "gold": {
                "storage_path": str(self.gold.storage_path),
                "enable_rag": self.gold.enable_rag,
            },
            "llm_enabled": self.llm_enabled,
            "log_level": self.log_level,
            "max_memory_mb": self.max_memory_mb,
            "chunk_size_mb": self.chunk_size_mb,
        }

        if self.schema:
            result["schema"] = self.schema.to_dict()

        if self.llm_config:
            result["llm"] = {
                "model_name": self.llm_config.model_name,
                "temperature": self.llm_config.temperature,
                "confidence_threshold": self.llm_config.confidence_threshold,
                "max_retries": self.llm_config.max_retries,
            }

        if self.log_file:
            result["log_file"] = str(self.log_file)

        return result

    def register_hook(
        self,
        layer: str,
        hook_type: str,
        hook: Hook,
    ) -> None:
        """
        Register a hook for a specific layer.

        Args:
            layer: Layer name (bronze, silver, gold)
            hook_type: Hook type (pre, post)
            hook: Callable hook function
        """
        hook_attr = f"{hook_type}_{layer}_hooks"
        if not hasattr(self, hook_attr):
            raise ConfigurationError(
                f"Invalid hook specification: {hook_type}_{layer}",
                context={"layer": layer, "hook_type": hook_type},
            )
        getattr(self, hook_attr).append(hook)
