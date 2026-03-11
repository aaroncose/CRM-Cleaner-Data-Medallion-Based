"""FastAPI backend for CRM Medallion Framework."""

import traceback

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from app.routers import pipeline, chat, review, metrics, schema, config

app = FastAPI(
    title="CRM Medallion API",
    description="API for CRM data cleaning using Medallion architecture",
    version="0.2.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """Catch all exceptions and print full traceback."""
    traceback.print_exc()
    return JSONResponse(status_code=500, content={"detail": str(exc)})

app.include_router(pipeline.router, prefix="/api", tags=["Pipeline"])
app.include_router(chat.router, prefix="/api", tags=["Chat"])
app.include_router(review.router, prefix="/api", tags=["Review"])
app.include_router(metrics.router, prefix="/api", tags=["Metrics"])
app.include_router(schema.router, prefix="/api", tags=["Schema"])
app.include_router(config.router, prefix="/api", tags=["Config"])


@app.get("/api/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy", "version": "0.2.0"}
