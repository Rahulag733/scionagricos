"""
ScionAgricos - Analytics Service
Core business calculations: revenue, margin, cash flow, KPIs, forecasts, etc.
"""

import pandas as pd
import numpy as np
from typing import Optional
from .data_loader import get_data


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _all_transactions() -> pd.DataFrame:
    """Combine all transaction sources into one unified frame."""
    data = get_data()
    frames = []
    
    for key in ["import_spot", "global_business", "own_imports", "export_sales"]:
        df = data.get(key)
        if df is not None and not df.empty:
            # Unify revenue column
            if "total_revenue" in df.columns:
                df = df.copy()
                df["revenue"] = df["total_revenue"].fillna(df.get("invoice_amount", np.nan))
            elif "invoice_value_eur" in df.columns:
                df = df.copy()
                df["revenue"] = df["invoice_value_eur"].fillna(df.get("invoice_amount", np.nan))
            else:
                df = df.copy()
                df["revenue"] = df.get("invoice_amount", np.nan)
            
            # Unify cost column
            if "cost_at_destination" in df.columns:
                df["cost"] = df["cost_at_destination"].fillna(0)
            else:
                # Estimate cost = revenue - margin
                margin = df.get("margin", pd.Series(0, index=df.index))
                df["cost"] = df["revenue"].fillna(0) - margin.fillna(0)
            
            df["source"] = key
            frames.append(df)
    
    if not frames:
        return pd.DataFrame()
    
    combined = pd.concat(frames, ignore_index=True)
    
    # Add month/quarter for grouping
    date_col = None
    for c in ["invoice_date", "payment_date"]:
        if c in combined.columns and combined[c].notna().sum() > 0:
            date_col = c
            break
    
    if date_col:
        combined["date"] = combined[date_col]
        combined["month"] = combined["date"].dt.month
        combined["month_name"] = combined["date"].dt.strftime("%b")
        combined["quarter"] = combined["date"].dt.quarter
    else:
        combined["date"] = pd.NaT
        combined["month"] = np.nan
        combined["month_name"] = ""
        combined["quarter"] = np.nan
    
    # Normalize text columns
    for col in ["origin", "product", "trader", "customer_country"]:
        if col in combined.columns:
            combined[col] = combined[col].astype(str).str.strip().str.title()
    
    return combined


# ─── KPI Summary ──────────────────────────────────────────────────────────────

def get_kpi_summary(year: Optional[int] = None, trader: Optional[str] = None,
                    product: Optional[str] = None, origin: Optional[str] = None) -> dict:
    df = _all_transactions()
    if df.empty:
        return {}
    
    df = _filter(df, year, trader, product, origin)
    
    total_revenue = df["revenue"].sum()
    total_cost = df["cost"].sum()
    total_margin = df["margin"].fillna(0).sum() if "margin" in df.columns else (total_revenue - total_cost)
    gross_margin_pct = (total_margin / total_revenue * 100) if total_revenue else 0
    
    # Year-over-year for trend
    if year:
        prev_df = _filter(_all_transactions(), year - 1, trader, product, origin)
        prev_rev = prev_df["revenue"].sum()
        rev_growth = ((total_revenue - prev_rev) / prev_rev * 100) if prev_rev else 0
    else:
        rev_growth = None
    
    shipments = len(df)
    avg_margin = df["margin"].mean() if "margin" in df.columns else 0
    
    return {
        "total_revenue": round(float(total_revenue), 2),
        "total_cost": round(float(total_cost), 2),
        "total_margin": round(float(total_margin), 2),
        "gross_margin_pct": round(float(gross_margin_pct), 2),
        "total_shipments": int(shipments),
        "avg_margin_per_shipment": round(float(avg_margin), 2),
        "revenue_growth_pct": round(float(rev_growth), 2) if rev_growth is not None else None,
        "ebitda_estimate": round(float(total_margin * 0.85), 2),
        "net_profit_estimate": round(float(total_margin * 0.72), 2),
    }


# ─── Revenue Trend ────────────────────────────────────────────────────────────

def get_revenue_trend(trader: Optional[str] = None, product: Optional[str] = None,
                      origin: Optional[str] = None) -> list[dict]:
    df = _all_transactions()
    if df.empty:
        return []
    df = _filter(df, None, trader, product, origin)
    
    grouped = df.groupby("year").agg(
        revenue=("revenue", "sum"),
        cost=("cost", "sum"),
        margin=("margin", "sum"),
        shipments=("revenue", "count"),
    ).reset_index()
    grouped["gross_margin_pct"] = (grouped["margin"] / grouped["revenue"] * 100).round(2)
    grouped = grouped.replace([float('inf'), float('-inf')], 0).fillna(0)
    return grouped.round(2).to_dict(orient="records")


# ─── Monthly Cash Flow ────────────────────────────────────────────────────────

def get_monthly_cashflow(year: Optional[int] = None, trader: Optional[str] = None) -> list[dict]:
    df = _all_transactions()
    if df.empty:
        return []
    df = _filter(df, year, trader)
    df = df[df["month"].notna()]
    
    monthly = df.groupby(["year", "month", "month_name"]).agg(
        inflows=("revenue", "sum"),
        outflows=("cost", "sum"),
        margin=("margin", "sum"),
        shipments=("revenue", "count"),
    ).reset_index()
    
    monthly = monthly.sort_values(["year", "month"])
    monthly["net_cash"] = monthly["inflows"] - monthly["outflows"]
    
    # Cumulative balance per year
    monthly["cumulative_balance"] = monthly.groupby("year")["net_cash"].cumsum()
    
    return monthly.round(2).to_dict(orient="records")


# ─── Product Analysis ─────────────────────────────────────────────────────────

def get_product_analysis(year: Optional[int] = None, trader: Optional[str] = None) -> list[dict]:
    df = _all_transactions()
    if df.empty:
        return []
    df = _filter(df, year, trader)
    df = df[df["product"].notna() & (df["product"] != "Nan") & (df["product"] != "")]
    
    grouped = df.groupby("product").agg(
        revenue=("revenue", "sum"),
        cost=("cost", "sum"),
        margin=("margin", "sum"),
        shipments=("revenue", "count"),
        total_weight_kg=("weight_kg", "sum"),
    ).reset_index()
    grouped["gross_margin_pct"] = (grouped["margin"] / grouped["revenue"] * 100).round(2)
    grouped["revenue_pct"] = (grouped["revenue"] / grouped["revenue"].sum() * 100).round(2)
    grouped = grouped.replace([float('inf'), float('-inf')], 0).fillna(0)
    return grouped.sort_values("revenue", ascending=False).round(2).to_dict(orient="records")


# ─── Origin Analysis ──────────────────────────────────────────────────────────

def get_origin_analysis(year: Optional[int] = None, trader: Optional[str] = None,
                         product: Optional[str] = None) -> list[dict]:
    df = _all_transactions()
    if df.empty:
        return []
    df = _filter(df, year, trader, product)
    df = df[df["origin"].notna() & (df["origin"] != "Nan") & (df["origin"] != "")]
    
    grouped = df.groupby("origin").agg(
        revenue=("revenue", "sum"),
        cost=("cost", "sum"),
        margin=("margin", "sum"),
        shipments=("revenue", "count"),
    ).reset_index()
    grouped["gross_margin_pct"] = (grouped["margin"] / grouped["revenue"] * 100).round(2)
    grouped = grouped.replace([float('inf'), float('-inf')], 0).fillna(0)
    return grouped.sort_values("revenue", ascending=False).round(2).to_dict(orient="records")


# ─── Trader Performance ───────────────────────────────────────────────────────

def get_trader_performance(year: Optional[int] = None) -> list[dict]:
    df = _all_transactions()
    if df.empty:
        return []
    df = _filter(df, year)
    
    grouped = df.groupby(["trader", "year"]).agg(
        revenue=("revenue", "sum"),
        cost=("cost", "sum"),
        margin=("margin", "sum"),
        shipments=("revenue", "count"),
    ).reset_index()
    grouped["gross_margin_pct"] = (grouped["margin"] / grouped["revenue"] * 100).round(2)
    grouped = grouped.replace([float('inf'), float('-inf')], 0).fillna(0)
    return grouped.sort_values(["year", "revenue"], ascending=[True, False]).round(2).to_dict(orient="records")


# ─── Profitability Matrix ─────────────────────────────────────────────────────

def get_profitability_matrix(year: Optional[int] = None, trader: Optional[str] = None) -> list[dict]:
    df = _all_transactions()
    if df.empty:
        return []
    df = _filter(df, year, trader)
    df = df[df["product"].notna() & df["origin"].notna()]
    df = df[(df["product"] != "Nan") & (df["origin"] != "Nan")]
    
    grouped = df.groupby(["product", "origin"]).agg(
        revenue=("revenue", "sum"),
        cost=("cost", "sum"),
        margin=("margin", "sum"),
        weight_kg=("weight_kg", "sum"),
        shipments=("revenue", "count"),
    ).reset_index()
    grouped["gross_margin_pct"] = (grouped["margin"] / grouped["revenue"] * 100).round(2)
    grouped["net_margin_pct"] = (grouped["margin"] * 0.85 / grouped["revenue"] * 100).round(2)
    grouped["ebitda"] = (grouped["margin"] * 0.85).round(2)
    grouped = grouped.replace([float('inf'), float('-inf')], 0).fillna(0)
    return grouped.sort_values("gross_margin_pct", ascending=False).round(2).to_dict(orient="records")


# ─── Seasonal Calendar ────────────────────────────────────────────────────────

def get_seasonal_data(year: Optional[int] = None, trader: Optional[str] = None,
                       product: Optional[str] = None) -> list[dict]:
    df = _all_transactions()
    if df.empty:
        return []
    df = _filter(df, year, trader, product)
    df = df[df["month"].notna()]
    
    MONTH_NAMES = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
    
    seasonal = df.groupby("month").agg(
        revenue=("revenue", "sum"),
        shipments=("revenue", "count"),
        margin=("margin", "sum"),
    ).reset_index()
    
    seasonal["month_name"] = seasonal["month"].apply(lambda m: MONTH_NAMES[int(m)-1])
    seasonal["gross_margin_pct"] = (seasonal["margin"] / seasonal["revenue"] * 100).round(2)
    max_rev = seasonal["revenue"].max()
    seasonal["intensity"] = (seasonal["revenue"] / max_rev * 100).round(1)
    seasonal = seasonal.replace([float('inf'), float('-inf')], 0).fillna(0)
    return seasonal.sort_values("month").round(2).to_dict(orient="records")


# ─── Top Shipments Table ──────────────────────────────────────────────────────

def get_shipments_table(year: Optional[int] = None, trader: Optional[str] = None,
                         product: Optional[str] = None, origin: Optional[str] = None,
                         limit: int = 500) -> list[dict]:
    df = _all_transactions()
    if df.empty:
        return []
    df = _filter(df, year, trader, product, origin)
    
    cols = [c for c in ["trader", "year", "source", "origin", "product", "customer",
                         "customer_country", "supplier", "boxes", "weight_kg",
                         "invoice_amount", "revenue", "cost", "margin", "invoice_date"] 
            if c in df.columns]
    
    result = df[cols].copy()
    result = result[result["revenue"].notna()]
    result = result.sort_values("revenue", ascending=False).head(limit)
    
    # Convert dates to string for JSON
    for col in result.select_dtypes(include=["datetime64[ns]", "datetimetz"]).columns:
        result[col] = result[col].dt.strftime("%Y-%m-%d")
    
    return result.round(2).fillna("").to_dict(orient="records")


# ─── Risk Overview ────────────────────────────────────────────────────────────

def get_risk_overview(year: Optional[int] = None) -> dict:
    df = _all_transactions()
    if df.empty:
        return {}
    df = _filter(df, year)
    
    # Low margin products
    prod = df.groupby("product").agg(margin_pct=("margin", lambda x: (x.sum() / df.loc[x.index, "revenue"].sum() * 100))).reset_index()
    low_margin = prod[prod["margin_pct"] < 10].sort_values("margin_pct")["product"].tolist()
    
    # High concentration
    origin_rev = df.groupby("origin")["revenue"].sum()
    total = origin_rev.sum()
    top_origin_pct = float(origin_rev.max() / total * 100) if total else 0
    
    prod_rev = df.groupby("product")["revenue"].sum()
    top_product_pct = float(prod_rev.max() / total * 100) if total else 0
    
    # Risk score 1-10
    risk_score = min(10, round(
        (len(low_margin) * 0.5) +
        (max(0, top_origin_pct - 40) * 0.1) +
        (max(0, top_product_pct - 50) * 0.1) + 2
    ))
    
    risk_label = "Safe" if risk_score <= 3 else ("Moderate" if risk_score <= 6 else "Risky")
    
    return {
        "risk_score": risk_score,
        "risk_label": risk_label,
        "low_margin_products": low_margin,
        "top_origin_concentration_pct": round(top_origin_pct, 1),
        "top_product_concentration_pct": round(top_product_pct, 1),
        "unique_origins": int(df["origin"].nunique()),
        "unique_products": int(df["product"].nunique()),
        "unique_customers": int(df["customer"].nunique()) if "customer" in df.columns else 0,
    }


# ─── Break-even Calculator ────────────────────────────────────────────────────

def calculate_breakeven(cost_per_kg: float, freight_cost: float,
                         container_capacity_kg: float,
                         target_margin_pct: float = 15.0) -> dict:
    total_cost = (cost_per_kg * container_capacity_kg) + freight_cost
    breakeven_revenue = total_cost / (1 - target_margin_pct / 100)
    breakeven_price_per_kg = breakeven_revenue / container_capacity_kg
    required_margin_pct = target_margin_pct
    
    return {
        "total_cost_per_container": round(total_cost, 2),
        "breakeven_revenue_per_container": round(breakeven_revenue, 2),
        "breakeven_price_per_kg": round(breakeven_price_per_kg, 4),
        "required_margin_pct": round(required_margin_pct, 2),
        "expected_profit": round(breakeven_revenue - total_cost, 2),
    }


# ─── Scenario Simulator ───────────────────────────────────────────────────────

def run_scenario(year: int, containers: int, cost_increase_pct: float,
                  price_decrease_pct: float, freight_change_pct: float,
                  season_delay_weeks: int = 0) -> dict:
    df = _all_transactions()
    base = _filter(df, year)
    
    base_revenue = float(base["revenue"].sum())
    base_cost = float(base["cost"].sum())
    base_margin = float(base["margin"].fillna(0).sum()) if "margin" in base.columns else (base_revenue - base_cost)
    base_shipments = len(base)
    avg_revenue_per_ship = base_revenue / base_shipments if base_shipments else 0
    avg_cost_per_ship = base_cost / base_shipments if base_shipments else 0
    
    # Apply scenario adjustments
    sim_revenue_per = avg_revenue_per_ship * (1 - price_decrease_pct / 100)
    sim_cost_per = avg_cost_per_ship * (1 + cost_increase_pct / 100) * (1 + freight_change_pct / 100)
    
    sim_revenue = sim_revenue_per * containers
    sim_cost = sim_cost_per * containers
    sim_margin = sim_revenue - sim_cost
    sim_margin_pct = (sim_margin / sim_revenue * 100) if sim_revenue else 0
    sim_ebitda = sim_margin * 0.85
    sim_net_profit = sim_margin * 0.72
    
    # Season delay impact (rough: 2% revenue loss per week delay)
    if season_delay_weeks > 0:
        delay_impact = 1 - (season_delay_weeks * 0.02)
        sim_revenue *= delay_impact
        sim_margin *= delay_impact
    
    capital_required = sim_cost * 0.6  # Typical 60% upfront
    
    return {
        "base": {
            "revenue": round(base_revenue, 2),
            "cost": round(base_cost, 2),
            "margin": round(base_margin, 2),
            "margin_pct": round(base_margin / base_revenue * 100, 2) if base_revenue else 0,
            "shipments": base_shipments,
        },
        "simulated": {
            "containers": containers,
            "revenue": round(sim_revenue, 2),
            "cost": round(sim_cost, 2),
            "margin": round(sim_margin, 2),
            "margin_pct": round(sim_margin_pct, 2),
            "ebitda": round(sim_ebitda, 2),
            "net_profit": round(sim_net_profit, 2),
            "capital_required": round(capital_required, 2),
        },
        "delta": {
            "revenue_change_pct": round((sim_revenue - base_revenue) / base_revenue * 100, 2) if base_revenue else 0,
            "margin_change_pct": round((sim_margin - base_margin) / abs(base_margin) * 100, 2) if base_margin else 0,
        }
    }


# ─── Forecast (Linear Projection) ────────────────────────────────────────────

def get_forecast(years_ahead: int = 3) -> list[dict]:
    trend = get_revenue_trend()
    if not trend:
        return []
    
    df = pd.DataFrame(trend)
    if len(df) < 2:
        return []
    
    # Simple linear regression on available years
    x = df["year"].values
    y_rev = df["revenue"].values
    y_cost = df["cost"].values
    y_margin = df["margin"].values
    
    rev_coef = np.polyfit(x, y_rev, 1)
    cost_coef = np.polyfit(x, y_cost, 1)
    margin_coef = np.polyfit(x, y_margin, 1)
    
    last_year = int(df["year"].max())
    forecast = []
    for i in range(1, years_ahead + 1):
        yr = last_year + i
        rev = max(0, float(np.polyval(rev_coef, yr)))
        cost = max(0, float(np.polyval(cost_coef, yr)))
        margin = float(np.polyval(margin_coef, yr))
        forecast.append({
            "year": yr,
            "forecast_revenue": round(rev, 2),
            "forecast_cost": round(cost, 2),
            "forecast_margin": round(margin, 2),
            "forecast_ebitda": round(margin * 0.85, 2),
            "forecast_net_profit": round(margin * 0.72, 2),
            "is_forecast": True,
        })
    
    # Include historical for chart continuity
    historical = df[["year", "revenue", "cost", "margin"]].copy()
    historical.columns = ["year", "forecast_revenue", "forecast_cost", "forecast_margin"]
    historical["forecast_ebitda"] = (historical["forecast_margin"] * 0.85).round(2)
    historical["forecast_net_profit"] = (historical["forecast_margin"] * 0.72).round(2)
    historical["is_forecast"] = False
    
    return historical.to_dict(orient="records") + forecast


# ─── Filter helper ────────────────────────────────────────────────────────────

def _filter(df: pd.DataFrame, year=None, trader=None, product=None, origin=None) -> pd.DataFrame:
    if year and "year" in df.columns:
        df = df[df["year"] == year]
    if trader and "trader" in df.columns:
        df = df[df["trader"].str.lower() == trader.lower()]
    if product and "product" in df.columns:
        df = df[df["product"].str.lower() == product.lower()]
    if origin and "origin" in df.columns:
        df = df[df["origin"].str.lower() == origin.lower()]
    return df


# ─── Meta: Available filters ──────────────────────────────────────────────────

def get_filter_options() -> dict:
    df = _all_transactions()
    if df.empty:
        return {}
    
    def clean_list(series):
        return sorted([x for x in series.dropna().unique().tolist() if x and x != "Nan"])
    
    return {
        "years": sorted(df["year"].dropna().unique().astype(int).tolist()),
        "traders": clean_list(df["trader"]),
        "products": clean_list(df["product"]) if "product" in df.columns else [],
        "origins": clean_list(df["origin"]) if "origin" in df.columns else [],
        "countries": clean_list(df["customer_country"]) if "customer_country" in df.columns else [],
    }
