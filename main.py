"""
ScionAgricos - Financial Forecasting & Shipment Planning System
FastAPI Backend
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pathlib import Path
import logging
import os

from app.routers.api import router
from app.services.data_loader import get_data
from app.db import is_db_configured, init_tables

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

app = FastAPI(
    title="ScionAgricos API",
    description="Financial Forecasting & Shipment Planning System for ScionAgricos",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

# CORS — allow all origins for local VS Code development
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount API routes
app.include_router(router, prefix="/api/v1", tags=["ScionAgricos"])

# Serve frontend static files if present
frontend_dir = Path(__file__).parent / "frontend"
if frontend_dir.exists():
    app.mount("/static", StaticFiles(directory=str(frontend_dir / "static")), name="static")

    @app.get("/", include_in_schema=False)
    def serve_frontend():
        return FileResponse(str(frontend_dir / "index.html"))


@app.on_event("startup")
async def startup_event():
    logger.info("ScionAgricos backend starting up...")
    data = get_data()
    total = sum(len(v) for v in data.values())
    logger.info(f"Loaded {total} records from {len(data)} datasets: {list(data.keys())}")

    # Initialize RDS tables if DATABASE_URL is configured
    if is_db_configured():
        try:
            init_tables()
        except Exception as e:
            logger.warning(f"RDS init skipped: {e}")
    else:
        logger.info("DATABASE_URL not set — skipping RDS init (transit_time/seasonality unavailable)")


@app.get("/", include_in_schema=False)
def root():
    return {
        "app": "ScionAgricos Financial Forecasting API",
        "version": "1.0.0",
        "docs": "/docs",
        "api_base": "/api/v1",
        "endpoints": [
            "/api/v1/health",
            "/api/v1/filters",
            "/api/v1/dashboard/kpi",
            "/api/v1/dashboard/revenue-trend",
            "/api/v1/dashboard/trader-performance",
            "/api/v1/analytics/shipments",
            "/api/v1/analytics/products",
            "/api/v1/analytics/origins",
            "/api/v1/profitability/matrix",
            "/api/v1/seasonal/monthly",
            "/api/v1/cashflow/monthly",
            "/api/v1/forecast",
            "/api/v1/scenario",
            "/api/v1/risk",
            "/api/v1/breakeven",
            "/api/v1/kpi/full",
        ]
    }
