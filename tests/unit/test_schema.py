"""Tests for SchemaDefinition class."""

import tempfile
from datetime import datetime
from enum import Enum
from pathlib import Path

import pytest
from pydantic import BaseModel, Field

from crm_medallion.config.schema import (
    SchemaDefinition,
    FieldDefinition,
    FieldType,
)


class TestFieldDefinition:
    def test_default_values(self):
        field_def = FieldDefinition(name="test", field_type=FieldType.STRING)
        assert field_def.required is True
        assert field_def.description == ""
        assert field_def.enum_values == []

    def test_to_pydantic_field_string(self):
        field_def = FieldDefinition(
            name="name",
            field_type=FieldType.STRING,
            required=True,
            min_length=1,
        )
        field_type, field_info = field_def.to_pydantic_field()
        assert field_type is str

    def test_to_pydantic_field_optional(self):
        field_def = FieldDefinition(
            name="name",
            field_type=FieldType.STRING,
            required=False,
        )
        field_type, default = field_def.to_pydantic_field()
        assert default is None

    def test_to_pydantic_field_enum(self):
        field_def = FieldDefinition(
            name="tipo",
            field_type=FieldType.ENUM,
            enum_values=["INGRESO", "GASTO"],
        )
        field_type, _ = field_def.to_pydantic_field()
        assert issubclass(field_type, Enum)


class TestSchemaDefinition:
    def test_from_dict_basic(self):
        schema_dict = {
            "name": "TestSchema",
            "description": "Test schema",
            "fields": [
                {"name": "id", "type": "int", "required": True},
                {"name": "name", "type": "str", "required": True},
            ],
        }
        schema = SchemaDefinition.from_dict(schema_dict)
        assert schema.name == "TestSchema"
        assert len(schema.fields) == 2
        assert schema.fields[0].name == "id"
        assert schema.fields[0].field_type == FieldType.INTEGER

    def test_from_dict_with_enum(self):
        schema_dict = {
            "name": "WithEnum",
            "fields": [
                {
                    "name": "tipo",
                    "type": "enum",
                    "values": ["A", "B", "C"],
                },
            ],
        }
        schema = SchemaDefinition.from_dict(schema_dict)
        assert schema.fields[0].enum_values == ["A", "B", "C"]

    def test_to_pydantic_model(self):
        schema_dict = {
            "name": "TestModel",
            "fields": [
                {"name": "id", "type": "int", "required": True},
                {"name": "name", "type": "str", "required": True},
                {"name": "value", "type": "float", "required": False},
            ],
        }
        schema = SchemaDefinition.from_dict(schema_dict)
        model = schema.to_pydantic_model()

        instance = model(id=1, name="Test")
        assert instance.id == 1
        assert instance.name == "Test"
        assert instance.value is None

    def test_to_dict(self):
        schema = SchemaDefinition(
            name="Test",
            description="Test schema",
            fields=[
                FieldDefinition(name="id", field_type=FieldType.INTEGER),
            ],
        )
        result = schema.to_dict()
        assert result["name"] == "Test"
        assert len(result["fields"]) == 1
        assert result["fields"][0]["name"] == "id"

    def test_yaml_round_trip(self):
        schema = SchemaDefinition(
            name="YAMLTest",
            description="Test YAML serialization",
            fields=[
                FieldDefinition(
                    name="nombre",
                    field_type=FieldType.STRING,
                    required=True,
                    description="Nombre del proveedor",
                ),
                FieldDefinition(
                    name="importe",
                    field_type=FieldType.FLOAT,
                    required=True,
                    ge=0,
                ),
            ],
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            yaml_path = Path(tmpdir) / "schema.yaml"
            schema.to_yaml(yaml_path)

            loaded = SchemaDefinition.from_yaml(yaml_path)
            assert loaded.name == schema.name
            assert len(loaded.fields) == len(schema.fields)
            assert loaded.fields[0].name == "nombre"
            assert loaded.fields[1].ge == 0

    def test_from_pydantic_model(self):
        class SampleModel(BaseModel):
            """Sample model for testing."""

            id: int
            name: str = Field(description="The name")
            value: float | None = None

        schema = SchemaDefinition.from_pydantic(SampleModel)
        assert schema.name == "SampleModel"
        assert len(schema.fields) == 3

        id_field = next(f for f in schema.fields if f.name == "id")
        assert id_field.field_type == FieldType.INTEGER
        assert id_field.required is True

        value_field = next(f for f in schema.fields if f.name == "value")
        assert value_field.required is False
