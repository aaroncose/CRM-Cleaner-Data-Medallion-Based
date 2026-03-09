"""Schema definition for CRM data validation."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Type

import yaml
from pydantic import BaseModel, Field, create_model


class FieldType(str, Enum):
    """Supported field types for schema definition."""

    STRING = "str"
    INTEGER = "int"
    FLOAT = "float"
    BOOLEAN = "bool"
    DATETIME = "datetime"
    DATE = "date"
    ENUM = "enum"


@dataclass
class FieldDefinition:
    """Definition of a single field in the schema."""

    name: str
    field_type: FieldType
    required: bool = True
    description: str = ""
    enum_values: list[str] = field(default_factory=list)
    min_length: int | None = None
    max_length: int | None = None
    ge: float | None = None
    le: float | None = None

    def to_pydantic_field(self) -> tuple[type, Any]:
        """Convert to Pydantic field type and Field definition."""
        type_mapping = {
            FieldType.STRING: str,
            FieldType.INTEGER: int,
            FieldType.FLOAT: float,
            FieldType.BOOLEAN: bool,
            FieldType.DATETIME: datetime,
            FieldType.DATE: datetime,
        }

        if self.field_type == FieldType.ENUM and self.enum_values:
            field_type = Enum(
                f"{self.name.title()}Enum",
                {v: v for v in self.enum_values},
            )
        else:
            field_type = type_mapping.get(self.field_type, str)

        if not self.required:
            field_type = field_type | None

        field_kwargs: dict[str, Any] = {}
        if self.description:
            field_kwargs["description"] = self.description
        if self.min_length is not None:
            field_kwargs["min_length"] = self.min_length
        if self.max_length is not None:
            field_kwargs["max_length"] = self.max_length
        if self.ge is not None:
            field_kwargs["ge"] = self.ge
        if self.le is not None:
            field_kwargs["le"] = self.le

        default = ... if self.required else None
        if field_kwargs:
            return (field_type, Field(default=default, **field_kwargs))
        return (field_type, default)


@dataclass
class SchemaDefinition:
    """Defines the schema for CRM data validation."""

    name: str
    fields: list[FieldDefinition]
    description: str = ""

    @classmethod
    def from_pydantic(cls, model: Type[BaseModel]) -> SchemaDefinition:
        """Create schema from Pydantic model."""
        fields = []
        for field_name, field_info in model.model_fields.items():
            annotation = field_info.annotation

            if hasattr(annotation, "__origin__") and annotation.__origin__ is type(
                None
            ):
                required = False
                actual_type = annotation.__args__[0]
            else:
                required = field_info.is_required()
                actual_type = annotation

            field_type = cls._python_type_to_field_type(actual_type)

            enum_values = []
            if isinstance(actual_type, type) and issubclass(actual_type, Enum):
                enum_values = [e.value for e in actual_type]

            field_def = FieldDefinition(
                name=field_name,
                field_type=field_type,
                required=required,
                description=field_info.description or "",
                enum_values=enum_values,
            )
            fields.append(field_def)

        return cls(
            name=model.__name__,
            fields=fields,
            description=model.__doc__ or "",
        )

    @classmethod
    def from_yaml(cls, yaml_path: Path) -> SchemaDefinition:
        """Load schema from YAML file."""
        with open(yaml_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
        return cls.from_dict(data)

    @classmethod
    def from_dict(cls, schema_dict: dict[str, Any]) -> SchemaDefinition:
        """Create schema from dictionary."""
        fields = []
        for field_data in schema_dict.get("fields", []):
            field_type_str = field_data.get("type", "str")
            field_type = FieldType(field_type_str)

            field_def = FieldDefinition(
                name=field_data["name"],
                field_type=field_type,
                required=field_data.get("required", True),
                description=field_data.get("description", ""),
                enum_values=field_data.get("values", []),
                min_length=field_data.get("min_length"),
                max_length=field_data.get("max_length"),
                ge=field_data.get("ge"),
                le=field_data.get("le"),
            )
            fields.append(field_def)

        return cls(
            name=schema_dict.get("name", "DynamicSchema"),
            fields=fields,
            description=schema_dict.get("description", ""),
        )

    def to_pydantic_model(self) -> Type[BaseModel]:
        """Generate Pydantic model from schema definition."""
        field_definitions = {}
        for field_def in self.fields:
            field_definitions[field_def.name] = field_def.to_pydantic_field()

        model = create_model(
            self.name,
            __doc__=self.description,
            **field_definitions,
        )
        return model

    def to_dict(self) -> dict[str, Any]:
        """Convert schema to dictionary representation."""
        return {
            "name": self.name,
            "description": self.description,
            "fields": [
                {
                    "name": f.name,
                    "type": f.field_type.value,
                    "required": f.required,
                    "description": f.description,
                    **({"values": f.enum_values} if f.enum_values else {}),
                    **({"min_length": f.min_length} if f.min_length else {}),
                    **({"max_length": f.max_length} if f.max_length else {}),
                    **({"ge": f.ge} if f.ge is not None else {}),
                    **({"le": f.le} if f.le is not None else {}),
                }
                for f in self.fields
            ],
        }

    def to_yaml(self, yaml_path: Path) -> None:
        """Save schema to YAML file."""
        with open(yaml_path, "w", encoding="utf-8") as f:
            yaml.dump(self.to_dict(), f, default_flow_style=False, allow_unicode=True)

    @staticmethod
    def _python_type_to_field_type(python_type: type) -> FieldType:
        """Convert Python type to FieldType enum."""
        type_mapping = {
            str: FieldType.STRING,
            int: FieldType.INTEGER,
            float: FieldType.FLOAT,
            bool: FieldType.BOOLEAN,
            datetime: FieldType.DATETIME,
        }

        if isinstance(python_type, type) and issubclass(python_type, Enum):
            return FieldType.ENUM

        return type_mapping.get(python_type, FieldType.STRING)
