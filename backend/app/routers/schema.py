"""Schema router for schema detection and validation."""

from fastapi import APIRouter, HTTPException

from app.models.api_models import ColumnConfig, SchemaDetectRequest, SchemaTemplate

router = APIRouter()


@router.post("/schema/detect")
async def detect_schema(request: SchemaDetectRequest):
    """Detect schema from uploaded CSV."""
    from app.services.framework_service import get_framework_service

    service = get_framework_service()

    # Find the uploaded file
    files = list(service.uploads_dir.glob(f"{request.file_id}_*"))
    if not files:
        raise HTTPException(status_code=404, detail="File not found")

    from crm_medallion.bronze.ingester import CSVIngester
    from crm_medallion.config.framework_config import BronzeConfig

    ingester = CSVIngester(BronzeConfig())
    schema = ingester.detect_schema(files[0], sample_rows=request.sample_rows)

    columns = []
    for field in schema.fields:
        columns.append(ColumnConfig(
            name=field.name,
            type=field.field_type.value,
            required=field.required,
            allowed_values=field.enum_values if field.enum_values else None,
        ))

    return {
        "name": schema.name,
        "columns": columns,
    }


@router.post("/schema/validate")
async def validate_schema(columns: list[ColumnConfig]):
    """Validate a schema configuration."""
    errors = []

    for col in columns:
        if not col.name:
            errors.append(f"Column name is required")
        if col.type not in ["string", "int", "float", "date", "datetime", "bool", "enum"]:
            errors.append(f"Invalid type '{col.type}' for column '{col.name}'")
        if col.type == "enum" and not col.allowed_values:
            errors.append(f"Enum column '{col.name}' must have allowed_values")

    if errors:
        return {"valid": False, "errors": errors}

    return {"valid": True, "errors": []}


@router.get("/schema/templates")
async def get_templates():
    """Get available schema templates."""
    templates = [
        SchemaTemplate(
            name="FacturaVenta",
            description="Schema for Spanish invoices (facturas de venta)",
            columns=[
                ColumnConfig(name="num_factura", type="string", required=True),
                ColumnConfig(name="fecha", type="date", required=True),
                ColumnConfig(name="proveedor", type="string", required=True),
                ColumnConfig(name="nif_cif", type="string", required=True),
                ColumnConfig(name="tipo", type="string", required=True, allowed_values=["Ingreso", "Gasto"]),
                ColumnConfig(name="categoria", type="string", required=True),
                ColumnConfig(name="importe_base", type="float", required=True),
                ColumnConfig(name="iva", type="float", required=True),
                ColumnConfig(name="importe_total", type="float", required=True),
                ColumnConfig(name="estado_factura", type="string", required=True, allowed_values=["Pagada", "Pendiente", "Vencida", "Parcialmente pagada"]),
            ],
        ),
        SchemaTemplate(
            name="Minimal",
            description="Minimal schema with basic fields",
            columns=[
                ColumnConfig(name="id", type="string", required=True),
                ColumnConfig(name="name", type="string", required=True),
                ColumnConfig(name="value", type="float", required=False),
            ],
        ),
    ]

    return {"templates": templates}
