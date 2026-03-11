"""Pipeline router for file upload and processing."""

import asyncio
import traceback
from typing import Any

from fastapi import APIRouter, File, HTTPException, UploadFile
from fastapi.responses import StreamingResponse

from app.models.api_models import (
    ProcessRequest,
    ProcessResultResponse,
    ProcessStatusResponse,
    ProcessingStatus,
    UploadResponse,
)
from app.services.framework_service import get_framework_service

router = APIRouter()


@router.post("/upload", response_model=UploadResponse)
async def upload_file(file: UploadFile = File(...)):
    """Upload a CSV file and get preview with detected schema."""
    if not file.filename or not file.filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only CSV files are supported")

    content = await file.read()
    service = get_framework_service()

    try:
        result = service.save_uploaded_file(file.filename, content)
        return UploadResponse(**result)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/process")
async def process_file(request: ProcessRequest):
    """Start processing a file through the pipeline."""
    service = get_framework_service()

    try:
        run_id = service.process_file(
            file_id=request.file_id,
            llm_enabled=request.llm_enabled,
            provider=request.provider.value,
            api_key=request.api_key,
            model_name=request.model_name,
        )
        return {"run_id": run_id, "status": "processing"}
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/process/status/{run_id}", response_model=ProcessStatusResponse)
async def get_process_status(run_id: str):
    """Get the current status of a processing run."""
    service = get_framework_service()

    try:
        status = service.get_run_status(run_id)
        return ProcessStatusResponse(
            run_id=run_id,
            status=ProcessingStatus(status.get("status", "pending")),
            progress=status.get("progress", 0.0),
            current_stage=status.get("current_stage", ""),
            message=status.get("message", ""),
        )
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.get("/process/status/{run_id}/stream")
async def stream_process_status(run_id: str):
    """Stream processing status updates via SSE."""
    service = get_framework_service()

    async def event_generator():
        while True:
            try:
                status = service.get_run_status(run_id)
                data = {
                    "status": status.get("status", "pending"),
                    "progress": status.get("progress", 0.0),
                    "current_stage": status.get("current_stage", ""),
                    "message": status.get("message", ""),
                }
                yield f"data: {data}\n\n"

                if status.get("status") in ("completed", "error"):
                    break

                await asyncio.sleep(0.5)
            except ValueError:
                yield f"data: {{'error': 'Run not found'}}\n\n"
                break

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
    )


@router.get("/results/{run_id}", response_model=ProcessResultResponse)
async def get_results(run_id: str):
    """Get the results of a completed processing run."""
    service = get_framework_service()

    try:
        result = service.get_run_result(run_id)
        return ProcessResultResponse(
            run_id=run_id,
            status=ProcessingStatus.COMPLETED,
            total_records=result.get("total_records", 0),
            valid_records=result.get("valid_records", 0),
            invalid_records=result.get("invalid_records", 0),
            llm_corrected=result.get("llm_corrected", 0),
            manual_review=result.get("manual_review", 0),
            processing_time_seconds=result.get("processing_time", 0.0),
            bronze_path=result.get("bronze_path"),
            silver_path=result.get("silver_path"),
            gold_path=result.get("gold_path"),
        )
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.get("/data/gold/{run_id}")
async def get_gold_data(run_id: str):
    """Get Gold layer data for a run."""
    service = get_framework_service()

    try:
        return service.get_gold_data(run_id)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.get("/data/compare/{run_id}")
async def get_compare_data(run_id: str):
    """Get comparison data between raw and clean CSV."""
    service = get_framework_service()

    try:
        return service.get_compare_data(run_id)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.get("/runs")
async def list_runs():
    """List all available runs."""
    service = get_framework_service()
    runs = service.list_runs()
    return {"runs": runs}
