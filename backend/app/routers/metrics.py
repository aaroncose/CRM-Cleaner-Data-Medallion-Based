"""Metrics router for analytics and visualizations."""

from fastapi import APIRouter, HTTPException

from app.models.api_models import MetricData, MetricsResponse
from app.services.framework_service import get_framework_service

router = APIRouter()


@router.get("/metrics/{run_id}", response_model=MetricsResponse)
async def get_metrics(run_id: str):
    """Get metrics for a run."""
    service = get_framework_service()

    try:
        gold_data = service.get_gold_data(run_id)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="Run not found")

    records = gold_data.get("records", [])
    statistics = gold_data.get("statistics", {})
    indexes = gold_data.get("indexes", {})
    segmented = gold_data.get("segmented_statistics", {})

    fixed_metrics = []

    # Total records
    fixed_metrics.append(MetricData(
        name="Total Registros",
        value=len(records),
    ))

    # Total by tipo (Ingreso vs Gasto)
    if "tipo" in segmented:
        tipo_data = segmented["tipo"].get("segments", {})
        ingreso_sum = tipo_data.get("Ingreso", {}).get("importe_total_sum", 0)
        gasto_sum = tipo_data.get("Gasto", {}).get("importe_total_sum", 0)

        fixed_metrics.append(MetricData(
            name="Ingresos vs Gastos",
            value=f"{ingreso_sum:.2f} / {gasto_sum:.2f}",
            chart_type="bar",
            chart_data={
                "labels": ["Ingresos", "Gastos"],
                "values": [ingreso_sum, gasto_sum],
            },
        ))

    # Distribution by category
    if "categoria" in indexes:
        cat_entries = indexes["categoria"].get("entries", {})
        categories = []
        counts = []
        for cat, entry in sorted(cat_entries.items(), key=lambda x: x[1].get("count", 0), reverse=True)[:10]:
            categories.append(cat)
            counts.append(entry.get("count", 0))

        fixed_metrics.append(MetricData(
            name="Distribución por Categoría",
            value=f"{len(cat_entries)} categorías",
            chart_type="pie",
            chart_data={
                "labels": categories,
                "values": counts,
            },
        ))

    # Invoice status
    if "estado_factura" in segmented:
        estado_data = segmented["estado_factura"].get("segments", {})
        labels = []
        values = []
        for estado, data in estado_data.items():
            labels.append(estado)
            values.append(data.get("count", 0))

        fixed_metrics.append(MetricData(
            name="Estado de Facturas",
            value=f"{sum(values)} facturas",
            chart_type="donut",
            chart_data={
                "labels": labels,
                "values": values,
            },
        ))

    # Top 10 providers
    if "proveedor" in indexes:
        prov_entries = indexes["proveedor"].get("entries", {})
        providers = []
        prov_counts = []
        for prov, entry in sorted(prov_entries.items(), key=lambda x: x[1].get("count", 0), reverse=True)[:10]:
            providers.append(prov[:20] + "..." if len(prov) > 20 else prov)
            prov_counts.append(entry.get("count", 0))

        fixed_metrics.append(MetricData(
            name="Top 10 Proveedores",
            value=f"{len(prov_entries)} proveedores únicos",
            chart_type="bar_horizontal",
            chart_data={
                "labels": providers,
                "values": prov_counts,
            },
        ))

    # Importe total statistics
    if "importe_total" in statistics:
        stats = statistics["importe_total"]
        fixed_metrics.append(MetricData(
            name="Importe Total",
            value=f"{stats.get('sum', 0):.2f} EUR",
            chart_type=None,
            chart_data={
                "sum": stats.get("sum", 0),
                "mean": stats.get("mean", 0),
                "min": stats.get("min", 0),
                "max": stats.get("max", 0),
            },
        ))

    return MetricsResponse(
        run_id=run_id,
        fixed_metrics=fixed_metrics,
        suggested_metrics=None,
    )


@router.post("/metrics/{run_id}/suggest")
async def suggest_metrics(run_id: str):
    """Ask LLM to suggest additional metrics."""
    # This would call the RAG engine to suggest metrics
    # For now, return empty suggestions
    return {"suggested_metrics": []}
