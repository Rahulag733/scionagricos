"""
ScionAgricos - Data Loader Service
Parses all Excel cash flow files for all traders (Chiru, Madhu, Mahendra, Unmesh)
across years 2023, 2024, 2025 into unified DataFrames.
"""

import os
import glob
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Optional
import logging

logger = logging.getLogger(__name__)

DATA_DIR = Path(__file__).parent.parent.parent / "data"

# ─── Column normalizers ────────────────────────────────────────────────────────

def _normalize_import_sales(df: pd.DataFrame, trader: str, year: int, sheet_type: str) -> pd.DataFrame:
    """Parse Import Sales / Spot Trading sheets."""
    # header is row index 1 (0-based after header=1 read)
    df = df.copy()
    df.columns = df.columns.str.strip()
    
    # Rename columns to standard names
    rename_map = {}
    for col in df.columns:
        cl = col.lower().strip()
        if "origin" in cl:
            rename_map[col] = "origin"
        elif "customer" in cl and "country" not in cl:
            rename_map[col] = "customer"
        elif "country" in cl:
            rename_map[col] = "customer_country"
        elif "product" in cl:
            rename_map[col] = "product"
        elif "pallets" in cl:
            rename_map[col] = "pallets"
        elif "boxes" in cl:
            rename_map[col] = "boxes"
        elif "weight" in cl or "kg" in cl:
            rename_map[col] = "weight_kg"
        elif "supplier" in cl:
            rename_map[col] = "supplier"
        elif "sales order" in cl or "order no" in cl:
            rename_map[col] = "sales_order"
        elif "partie" in cl:
            rename_map[col] = "partie"
        elif "invoice amount" in cl:
            rename_map[col] = "invoice_amount"
        elif "sales invoice number" in cl or "sales invoice no" in cl:
            rename_map[col] = "sales_invoice_number"
        elif "sales invoice date" in cl or "invoice date" in cl:
            rename_map[col] = "invoice_date"
        elif "payment date" in cl:
            rename_map[col] = "payment_date"
        elif col == "Margin" or cl == "margin":
            rename_map[col] = "margin"
        elif "invoice value" in cl and "euro" in cl:
            rename_map[col] = "invoice_value_eur"
    
    df = df.rename(columns=rename_map)
    df["trader"] = trader
    df["year"] = year
    df["sheet_type"] = sheet_type
    return df


def _normalize_global_own(df: pd.DataFrame, trader: str, year: int, sheet_type: str) -> pd.DataFrame:
    """Parse Global Business / Own Imports sheets."""
    df = df.copy()
    df.columns = df.columns.str.strip()
    
    rename_map = {}
    for col in df.columns:
        cl = col.lower().strip()
        if "origin" in cl:
            rename_map[col] = "origin"
        elif "product" in cl:
            rename_map[col] = "product"
        elif "boxes" in cl or "number of boxes" in cl:
            rename_map[col] = "boxes"
        elif "pallets" in cl:
            rename_map[col] = "pallets"
        elif "weight" in cl or "kg" in cl:
            rename_map[col] = "weight_kg"
        elif "supplier" in cl:
            rename_map[col] = "supplier"
        elif "partie" in cl:
            rename_map[col] = "partie"
        elif "invoice amount" in cl:
            rename_map[col] = "invoice_amount"
        elif "1st payment" in cl:
            rename_map[col] = "first_payment_date"
        elif "balance payment" in cl:
            rename_map[col] = "balance_payment_date"
        elif "customer" in cl and "country" not in cl:
            rename_map[col] = "customer"
        elif "country" in cl:
            rename_map[col] = "customer_country"
        elif "sales order" in cl:
            rename_map[col] = "sales_order"
        elif "sales invoice number" in cl:
            rename_map[col] = "sales_invoice_number"
        elif "sales invoice date" in cl:
            rename_map[col] = "invoice_date"
        elif "sales invoice amount" in cl or ("invoice amount" in cl and "sales" in cl):
            rename_map[col] = "sales_invoice_amount"
        elif "total revenue" in cl:
            rename_map[col] = "total_revenue"
        elif "cost at destination" in cl:
            rename_map[col] = "cost_at_destination"
        elif col == "Margin" or cl == "margin":
            rename_map[col] = "margin"
    
    df = df.rename(columns=rename_map)
    
    # Deduplicate after rename (buy-side vs sell-side columns)
    seen = {}
    final_cols = []
    for c in df.columns:
        if c in seen:
            seen[c] += 1
            final_cols.append(f"{c}_sell" if seen[c] == 1 else f"{c}_{seen[c]}")
        else:
            seen[c] = 0
            final_cols.append(c)
    df.columns = final_cols
    
    df["trader"] = trader
    df["year"] = year
    df["sheet_type"] = sheet_type
    return df


def _normalize_export(df: pd.DataFrame, trader: str, year: int, sheet_type: str) -> pd.DataFrame:
    """Parse Export Sales / Global Sales sheets (Unmesh)."""
    df = df.copy()
    df.columns = df.columns.str.strip()
    
    rename_map = {}
    for col in df.columns:
        cl = col.lower().strip().rstrip()
        if "origin" in cl:
            rename_map[col] = "origin"
        elif "customer" in cl and "country" not in cl:
            rename_map[col] = "customer"
        elif "country" in cl:
            rename_map[col] = "customer_country"
        elif "product" in cl:
            rename_map[col] = "product"
        elif "pallets" in cl:
            rename_map[col] = "pallets"
        elif "boxes" in cl:
            rename_map[col] = "boxes"
        elif "weight" in cl or "kg" in cl:
            rename_map[col] = "weight_kg"
        elif "supplier" in cl:
            rename_map[col] = "supplier"
        elif "invoice amount" in cl and "euro" not in cl:
            rename_map[col] = "invoice_amount"
        elif "invoice value" in cl and "euro" in cl:
            rename_map[col] = "invoice_value_eur"
        elif "invoice value" in cl:
            rename_map[col] = "invoice_amount"
        elif "sales invoice number" in cl or "invoice number" in cl:
            rename_map[col] = "sales_invoice_number"
        elif "invoice date" in cl or "sales invoice date" in cl:
            rename_map[col] = "invoice_date"
        elif "payment date" in cl:
            rename_map[col] = "payment_date"
        elif col.strip() == "Margin" or cl == "margin":
            rename_map[col] = "margin"
    
    df = df.rename(columns=rename_map)
    df["trader"] = trader
    df["year"] = year
    df["sheet_type"] = sheet_type
    return df


# ─── Main loader ──────────────────────────────────────────────────────────────

def load_all_data() -> dict[str, pd.DataFrame]:
    """
    Loads and parses all 12 Excel files.
    Returns a dict with keys: 'transactions', 'global_business', 'own_imports', 'export_sales'
    Each is a unified DataFrame across all traders and years.
    """
    import_rows = []
    global_rows = []
    own_rows = []
    export_rows = []

    files = sorted(glob.glob(str(DATA_DIR / "Cash_Flow_*.xlsx")))
    if not files:
        logger.error(f"No Excel files found in {DATA_DIR}")
        return {}

    for filepath in files:
        filename = os.path.basename(filepath)
        # Extract trader and year from filename: Cash_Flow_2023_-_Chiru_.xlsx
        parts = filename.replace(".xlsx", "").split("_-_")
        year = int(parts[0].split("_")[-1])
        trader = parts[1].strip().replace("_", "").strip()

        try:
            xl = pd.ExcelFile(filepath)
        except Exception as e:
            logger.warning(f"Could not read {filename}: {e}")
            continue

        for sheet in xl.sheet_names:
            try:
                # Detect header row dynamically (row 1 or 2)
                probe = pd.read_excel(filepath, sheet_name=sheet, header=None, nrows=4)
                header_row = 2  # default
                for ri in [1, 2]:
                    row_vals = probe.iloc[ri].astype(str).str.lower().str.strip()
                    if row_vals.str.contains("origin|product|weight|sl|si").any():
                        header_row = ri
                        break
                
                raw = pd.read_excel(filepath, sheet_name=sheet, header=header_row)
                # Drop fully empty rows
                raw = raw.dropna(how="all").reset_index(drop=True)
                # Deduplicate columns
                seen = {}
                new_cols = []
                for c in raw.columns:
                    c_str = str(c)
                    if c_str in seen:
                        seen[c_str] += 1
                        new_cols.append(f"{c_str}.{seen[c_str]}")
                    else:
                        seen[c_str] = 0
                        new_cols.append(c_str)
                raw.columns = new_cols
                # Drop header-like rows that crept in
                mask = raw.iloc[:, 1].astype(str).str.strip().str.lower()
                raw = raw[~mask.isin(["sl", "si"])]

                sheet_lower = sheet.lower()
                if "import sales" in sheet_lower or "spot trading" in sheet_lower:
                    norm = _normalize_import_sales(raw, trader, year, sheet.lower().replace(" ", "_"))
                    import_rows.append(norm)
                elif "global business" in sheet_lower or "global sales" in sheet_lower:
                    norm = _normalize_global_own(raw, trader, year, "global_business")
                    global_rows.append(norm)
                elif "own imports" in sheet_lower:
                    norm = _normalize_global_own(raw, trader, year, "own_imports")
                    own_rows.append(norm)
                elif "export sales" in sheet_lower:
                    norm = _normalize_export(raw, trader, year, "export_sales")
                    export_rows.append(norm)
            except Exception as e:
                logger.warning(f"Error parsing {filename}:{sheet} → {e}")
                continue

    result = {}

    if import_rows:
        df = pd.concat([r.reset_index(drop=True) for r in import_rows], ignore_index=True, sort=False)
        df = _clean_numeric(df, ["invoice_amount", "margin", "weight_kg", "boxes", "pallets"])
        df = _clean_dates(df, ["invoice_date", "payment_date"])
        result["import_spot"] = df

    if global_rows:
        df = pd.concat([r.reset_index(drop=True) for r in global_rows], ignore_index=True, sort=False)
        df = _clean_numeric(df, ["invoice_amount", "margin", "weight_kg", "boxes", "pallets", "total_revenue", "cost_at_destination"])
        df = _clean_dates(df, ["invoice_date", "first_payment_date", "balance_payment_date"])
        result["global_business"] = df

    if own_rows:
        df = pd.concat([r.reset_index(drop=True) for r in own_rows], ignore_index=True, sort=False)
        df = _clean_numeric(df, ["invoice_amount", "margin", "weight_kg", "boxes", "pallets", "total_revenue", "cost_at_destination"])
        df = _clean_dates(df, ["invoice_date", "first_payment_date", "balance_payment_date"])
        result["own_imports"] = df

    if export_rows:
        df = pd.concat([r.reset_index(drop=True) for r in export_rows], ignore_index=True, sort=False)
        df = _clean_numeric(df, ["invoice_amount", "invoice_value_eur", "margin", "weight_kg", "boxes", "pallets"])
        df = _clean_dates(df, ["invoice_date", "payment_date"])
        result["export_sales"] = df

    logger.info(f"Loaded datasets: { {k: len(v) for k, v in result.items()} }")
    return result


def _clean_numeric(df: pd.DataFrame, cols: list) -> pd.DataFrame:
    for col in cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def _clean_dates(df: pd.DataFrame, cols: list) -> pd.DataFrame:
    for col in cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
    return df


# ─── Singleton cache ──────────────────────────────────────────────────────────

_cache: Optional[dict] = None

def get_data() -> dict[str, pd.DataFrame]:
    global _cache
    if _cache is None:
        _cache = load_all_data()
    return _cache


def refresh_data() -> dict[str, pd.DataFrame]:
    global _cache
    _cache = load_all_data()
    return _cache
