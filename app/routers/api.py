"""
ScionAgricos API Routers
All REST endpoints for the Financial Forecasting & Shipment Planning System.
"""

from fastapi import APIRouter, Query, HTTPException
from typing import Optional
from pydantic import BaseModel, Field
from ..services import analytics
from ..services.data_loader import get_data, refresh_data
from ..db import is_db_configured, get_session
from ..models import TransitTime, Seasonality

router = APIRouter()


# ─── Data & Health ────────────────────────────────────────────────────────────

@router.get("/health")
def health_check():
    data = get_data()
    return {
        "status": "ok",
        "datasets_loaded": list(data.keys()),
        "record_counts": {k: len(v) for k, v in data.items()},
    }


@router.post("/data/refresh")
def refresh():
    data = refresh_data()
    return {
        "status": "refreshed",
        "record_counts": {k: len(v) for k, v in data.items()},
    }


@router.get("/filters")
def get_filters():
    """Return available filter options (years, traders, products, origins)."""
    return analytics.get_filter_options()


# ─── Executive Dashboard ──────────────────────────────────────────────────────

@router.get("/dashboard/kpi")
def dashboard_kpi(
    year: Optional[int] = Query(None, description="Filter by year"),
    trader: Optional[str] = Query(None),
    product: Optional[str] = Query(None),
    origin: Optional[str] = Query(None),
):
    """Top KPI cards: Revenue, Expenses, Margin%, EBITDA, Net Profit."""
    return analytics.get_kpi_summary(year, trader, product, origin)


@router.get("/dashboard/revenue-trend")
def revenue_trend(
    trader: Optional[str] = Query(None),
    product: Optional[str] = Query(None),
    origin: Optional[str] = Query(None),
):
    """3-year revenue vs expense trend for chart."""
    return analytics.get_revenue_trend(trader, product, origin)


@router.get("/dashboard/trader-performance")
def trader_performance(year: Optional[int] = Query(None)):
    """Revenue and margin breakdown per trader per year."""
    return analytics.get_trader_performance(year)


# ─── Historical Shipment Analytics ───────────────────────────────────────────

@router.get("/analytics/shipments")
def shipments_table(
    year: Optional[int] = Query(None),
    trader: Optional[str] = Query(None),
    product: Optional[str] = Query(None),
    origin: Optional[str] = Query(None),
    limit: int = Query(500, ge=1, le=2000),
):
    """Full shipments data table with all fields."""
    return analytics.get_shipments_table(year, trader, product, origin, limit)


@router.get("/analytics/products")
def product_analysis(
    year: Optional[int] = Query(None),
    trader: Optional[str] = Query(None),
):
    """Revenue, cost, margin breakdown by product."""
    return analytics.get_product_analysis(year, trader)


@router.get("/analytics/origins")
def origin_analysis(
    year: Optional[int] = Query(None),
    trader: Optional[str] = Query(None),
    product: Optional[str] = Query(None),
):
    """Revenue breakdown by origin country."""
    return analytics.get_origin_analysis(year, trader, product)


# ─── Profitability Matrix ─────────────────────────────────────────────────────

@router.get("/profitability/matrix")
def profitability_matrix(
    year: Optional[int] = Query(None),
    trader: Optional[str] = Query(None),
):
    """Product × Origin profitability matrix with margins."""
    return analytics.get_profitability_matrix(year, trader)


# ─── Seasonal Calendar ────────────────────────────────────────────────────────

@router.get("/seasonal/monthly")
def seasonal_monthly(
    year: Optional[int] = Query(None),
    trader: Optional[str] = Query(None),
    product: Optional[str] = Query(None),
):
    """Monthly seasonality: revenue, shipments, margin intensity."""
    return analytics.get_seasonal_data(year, trader, product)


# ─── Cash Flow Analysis ───────────────────────────────────────────────────────

@router.get("/cashflow/monthly")
def monthly_cashflow(
    year: Optional[int] = Query(None),
    trader: Optional[str] = Query(None),
):
    """Monthly inflows, outflows, net cash, cumulative balance."""
    return analytics.get_monthly_cashflow(year, trader)


# ─── Forecast Planning ────────────────────────────────────────────────────────

@router.get("/forecast")
def forecast(years_ahead: int = Query(3, ge=1, le=10)):
    """Revenue, cost, margin forecast using linear trend projection."""
    return analytics.get_forecast(years_ahead)


# ─── Scenario Simulator ───────────────────────────────────────────────────────

@router.get("/scenario")
def scenario_simulator(
    year: int = Query(2024, description="Base year for simulation"),
    containers: int = Query(100, ge=1, le=10000),
    cost_increase_pct: float = Query(0.0, ge=-50, le=200),
    price_decrease_pct: float = Query(0.0, ge=-50, le=100),
    freight_change_pct: float = Query(0.0, ge=-50, le=200),
    season_delay_weeks: int = Query(0, ge=0, le=26),
):
    """Real-time scenario simulation with sliders."""
    return analytics.run_scenario(
        year, containers, cost_increase_pct,
        price_decrease_pct, freight_change_pct, season_delay_weeks
    )


# ─── Risk Overview ────────────────────────────────────────────────────────────

@router.get("/risk")
def risk_overview(year: Optional[int] = Query(None)):
    """Risk assessment: low margin products, concentration risk, risk score."""
    return analytics.get_risk_overview(year)


# ─── Break-even Calculator ────────────────────────────────────────────────────

@router.get("/breakeven")
def breakeven(
    cost_per_kg: float = Query(..., description="Cost per kg in USD/EUR"),
    freight_cost: float = Query(..., description="Total freight cost per container"),
    container_capacity_kg: float = Query(21000.0, description="Container weight capacity in kg"),
    target_margin_pct: float = Query(15.0, ge=0, le=100),
):
    """Break-even calculator per container."""
    return analytics.calculate_breakeven(
        cost_per_kg, freight_cost, container_capacity_kg, target_margin_pct
    )


# ─── KPI Dashboard ───────────────────────────────────────────────────────────

@router.get("/kpi/full")
def kpi_full(
    year: Optional[int] = Query(None),
    trader: Optional[str] = Query(None),
):
    """Full KPI set: margins, growth, returns with trend data."""
    kpi = analytics.get_kpi_summary(year, trader)
    trend = analytics.get_revenue_trend(trader)
    return {"kpis": kpi, "trend": trend}


# ─── Pydantic Schemas ─────────────────────────────────────────────────────────

class TransitTimeIn(BaseModel):
    origin: str
    destination: str
    transit_type: str = Field(..., pattern="^(import|export)$")
    transit_days_min: Optional[int] = None
    transit_days_avg: int
    transit_days_max: Optional[int] = None


class SeasonalityIn(BaseModel):
    product: str
    origin: str
    month: int = Field(..., ge=1, le=12)
    availability: Optional[str] = Field(None, pattern="^(high|medium|low)$")
    is_peak_season: bool = False
    notes: Optional[str] = None


def _require_db():
    if not is_db_configured():
        raise HTTPException(
            status_code=503,
            detail="DATABASE_URL not configured. Set it in your environment to use RDS features."
        )


# ─── Transit Time ─────────────────────────────────────────────────────────────

@router.get("/reference/transit-time")
def list_transit_times(
    origin: Optional[str] = Query(None),
    destination: Optional[str] = Query(None),
    transit_type: Optional[str] = Query(None, pattern="^(import|export)$"),
):
    """List all transit time records. Filter by origin, destination, or type."""
    _require_db()
    session = get_session()
    try:
        q = session.query(TransitTime)
        if origin:
            q = q.filter(TransitTime.origin.ilike(f"%{origin}%"))
        if destination:
            q = q.filter(TransitTime.destination.ilike(f"%{destination}%"))
        if transit_type:
            q = q.filter(TransitTime.transit_type == transit_type)
        return [r.to_dict() for r in q.order_by(TransitTime.origin).all()]
    finally:
        session.close()


@router.post("/reference/transit-time", status_code=201)
def create_transit_time(payload: TransitTimeIn):
    """Add a new transit time record."""
    _require_db()
    session = get_session()
    try:
        record = TransitTime(**payload.model_dump())
        session.add(record)
        session.commit()
        session.refresh(record)
        return record.to_dict()
    except Exception as e:
        session.rollback()
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        session.close()


@router.put("/reference/transit-time/{record_id}")
def update_transit_time(record_id: int, payload: TransitTimeIn):
    """Update an existing transit time record."""
    _require_db()
    session = get_session()
    try:
        record = session.query(TransitTime).filter(TransitTime.id == record_id).first()
        if not record:
            raise HTTPException(status_code=404, detail="Record not found")
        for key, value in payload.model_dump().items():
            setattr(record, key, value)
        session.commit()
        session.refresh(record)
        return record.to_dict()
    except HTTPException:
        raise
    except Exception as e:
        session.rollback()
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        session.close()


@router.delete("/reference/transit-time/{record_id}", status_code=204)
def delete_transit_time(record_id: int):
    """Delete a transit time record."""
    _require_db()
    session = get_session()
    try:
        record = session.query(TransitTime).filter(TransitTime.id == record_id).first()
        if not record:
            raise HTTPException(status_code=404, detail="Record not found")
        session.delete(record)
        session.commit()
    except HTTPException:
        raise
    except Exception as e:
        session.rollback()
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        session.close()


# ─── Seasonality ──────────────────────────────────────────────────────────────

@router.get("/reference/seasonality")
def list_seasonality(
    product: Optional[str] = Query(None),
    origin: Optional[str] = Query(None),
    month: Optional[int] = Query(None, ge=1, le=12),
):
    """List all seasonality records. Filter by product, origin, or month."""
    _require_db()
    session = get_session()
    try:
        q = session.query(Seasonality)
        if product:
            q = q.filter(Seasonality.product.ilike(f"%{product}%"))
        if origin:
            q = q.filter(Seasonality.origin.ilike(f"%{origin}%"))
        if month:
            q = q.filter(Seasonality.month == month)
        return [r.to_dict() for r in q.order_by(Seasonality.product, Seasonality.month).all()]
    finally:
        session.close()


@router.post("/reference/seasonality", status_code=201)
def create_seasonality(payload: SeasonalityIn):
    """Add a new seasonality record."""
    _require_db()
    session = get_session()
    try:
        record = Seasonality(**payload.model_dump())
        session.add(record)
        session.commit()
        session.refresh(record)
        return record.to_dict()
    except Exception as e:
        session.rollback()
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        session.close()


@router.put("/reference/seasonality/{record_id}")
def update_seasonality(record_id: int, payload: SeasonalityIn):
    """Update an existing seasonality record."""
    _require_db()
    session = get_session()
    try:
        record = session.query(Seasonality).filter(Seasonality.id == record_id).first()
        if not record:
            raise HTTPException(status_code=404, detail="Record not found")
        for key, value in payload.model_dump().items():
            setattr(record, key, value)
        session.commit()
        session.refresh(record)
        return record.to_dict()
    except HTTPException:
        raise
    except Exception as e:
        session.rollback()
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        session.close()


@router.delete("/reference/seasonality/{record_id}", status_code=204)
def delete_seasonality(record_id: int):
    """Delete a seasonality record."""
    _require_db()
    session = get_session()
    try:
        record = session.query(Seasonality).filter(Seasonality.id == record_id).first()
        if not record:
            raise HTTPException(status_code=404, detail="Record not found")
        session.delete(record)
        session.commit()
    except HTTPException:
        raise
    except Exception as e:
        session.rollback()
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        session.close()
