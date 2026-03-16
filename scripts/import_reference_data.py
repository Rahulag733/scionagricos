"""
ScionAgricos - Import Reference Data to AWS RDS
Reads transit time and seasonality data from an Excel/CSV file
and inserts it into the PostgreSQL RDS database.

Usage:
    python scripts/import_reference_data.py --file path/to/your_file.xlsx
    python scripts/import_reference_data.py --file path/to/your_file.xlsx --clear

Options:
    --file   Path to the Excel or CSV file
    --clear  Delete existing rows before importing (default: False)
    --sheet-transit   Sheet name for transit time data  (default: auto-detect)
    --sheet-season    Sheet name for seasonality data   (default: auto-detect)

Expected columns in transit time sheet:
    origin, destination, transit_type (import/export),
    transit_days_avg, transit_days_min (optional), transit_days_max (optional)

Expected columns in seasonality sheet:
    product, origin, month (1-12 or Jan/Feb/...),
    availability (high/medium/low, optional),
    is_peak_season (True/False, optional), notes (optional)
"""

import sys
import os
import argparse
import pandas as pd
from pathlib import Path

# Allow running from project root
sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
load_dotenv()

from app.db import get_session, init_tables, is_db_configured
from app.models import TransitTime, Seasonality

MONTH_MAP = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}


def parse_month(val) -> int:
    """Convert month name or number to integer 1-12."""
    if pd.isna(val):
        raise ValueError("Month cannot be empty")
    if isinstance(val, (int, float)):
        return int(val)
    val_str = str(val).strip().lower()[:3]
    if val_str in MONTH_MAP:
        return MONTH_MAP[val_str]
    try:
        return int(val)
    except ValueError:
        raise ValueError(f"Cannot parse month: {val!r}")


def normalize_availability(val) -> str | None:
    if pd.isna(val) or str(val).strip() == "":
        return None
    v = str(val).strip().lower()
    if v in ("high", "medium", "low"):
        return v
    # Try numeric: >66 → high, 33-66 → medium, <33 → low
    try:
        n = float(v.replace("%", ""))
        if n > 66:
            return "high"
        if n > 33:
            return "medium"
        return "low"
    except ValueError:
        return None


def normalize_transit_type(val) -> str:
    v = str(val).strip().lower()
    if "export" in v:
        return "export"
    if "import" in v:
        return "import"
    raise ValueError(f"transit_type must be 'import' or 'export', got: {val!r}")


def find_sheet(xl: pd.ExcelFile, keywords: list[str]) -> str | None:
    for sheet in xl.sheet_names:
        sl = sheet.lower()
        if any(k in sl for k in keywords):
            return sheet
    return None


def import_transit_time(df: pd.DataFrame, session, clear: bool):
    # Normalize column names
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")

    col_map = {}
    for col in df.columns:
        if "origin" in col and "destination" not in col:
            col_map[col] = "origin"
        elif "destination" in col or "dest" in col:
            col_map[col] = "destination"
        elif "type" in col or "direction" in col:
            col_map[col] = "transit_type"
        elif "avg" in col or ("days" in col and "min" not in col and "max" not in col):
            col_map[col] = "transit_days_avg"
        elif "min" in col:
            col_map[col] = "transit_days_min"
        elif "max" in col:
            col_map[col] = "transit_days_max"
    df = df.rename(columns=col_map)

    required = ["origin", "destination", "transit_type", "transit_days_avg"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        print(f"  [transit_time] Missing columns: {missing}")
        print(f"  Available columns: {list(df.columns)}")
        print("  Please rename your columns or update the script's col_map.")
        return 0

    if clear:
        deleted = session.query(TransitTime).delete()
        print(f"  Cleared {deleted} existing transit_time rows")

    df = df.dropna(subset=["origin", "destination", "transit_days_avg"])
    count = 0
    for _, row in df.iterrows():
        try:
            record = TransitTime(
                origin=str(row["origin"]).strip().title(),
                destination=str(row["destination"]).strip().title(),
                transit_type=normalize_transit_type(row["transit_type"]),
                transit_days_avg=int(row["transit_days_avg"]),
                transit_days_min=int(row["transit_days_min"]) if "transit_days_min" in df.columns and not pd.isna(row.get("transit_days_min")) else None,
                transit_days_max=int(row["transit_days_max"]) if "transit_days_max" in df.columns and not pd.isna(row.get("transit_days_max")) else None,
            )
            session.add(record)
            count += 1
        except Exception as e:
            print(f"  Skipping row {_}: {e}")
    return count


def import_seasonality(df: pd.DataFrame, session, clear: bool):
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")

    col_map = {}
    for col in df.columns:
        if "product" in col:
            col_map[col] = "product"
        elif "origin" in col:
            col_map[col] = "origin"
        elif "month" in col:
            col_map[col] = "month"
        elif "avail" in col:
            col_map[col] = "availability"
        elif "peak" in col:
            col_map[col] = "is_peak_season"
        elif "note" in col or "remark" in col:
            col_map[col] = "notes"
    df = df.rename(columns=col_map)

    required = ["product", "origin", "month"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        print(f"  [seasonality] Missing columns: {missing}")
        print(f"  Available columns: {list(df.columns)}")
        print("  Please rename your columns or update the script's col_map.")
        return 0

    if clear:
        deleted = session.query(Seasonality).delete()
        print(f"  Cleared {deleted} existing seasonality rows")

    df = df.dropna(subset=["product", "origin", "month"])
    count = 0
    for _, row in df.iterrows():
        try:
            record = Seasonality(
                product=str(row["product"]).strip().title(),
                origin=str(row["origin"]).strip().title(),
                month=parse_month(row["month"]),
                availability=normalize_availability(row.get("availability")),
                is_peak_season=bool(row["is_peak_season"]) if "is_peak_season" in df.columns and not pd.isna(row.get("is_peak_season")) else False,
                notes=str(row["notes"]).strip() if "notes" in df.columns and not pd.isna(row.get("notes")) else None,
            )
            session.add(record)
            count += 1
        except Exception as e:
            print(f"  Skipping row {_}: {e}")
    return count


def main():
    parser = argparse.ArgumentParser(description="Import reference data into AWS RDS")
    parser.add_argument("--file", required=True, help="Path to Excel (.xlsx) or CSV file")
    parser.add_argument("--clear", action="store_true", help="Clear existing rows before import")
    parser.add_argument("--sheet-transit", default=None, help="Sheet name for transit time data")
    parser.add_argument("--sheet-season", default=None, help="Sheet name for seasonality data")
    args = parser.parse_args()

    if not is_db_configured():
        print("ERROR: DATABASE_URL environment variable is not set.")
        print("Example: export DATABASE_URL=postgresql://user:password@your-rds-host:5432/dbname")
        sys.exit(1)

    filepath = Path(args.file)
    if not filepath.exists():
        print(f"ERROR: File not found: {filepath}")
        sys.exit(1)

    print(f"Reading: {filepath}")
    init_tables()
    session = get_session()

    try:
        if filepath.suffix.lower() == ".csv":
            # Single CSV — try to determine which table it is
            df = pd.read_csv(filepath)
            cols = " ".join(df.columns.str.lower())
            if "destination" in cols or "transit" in cols:
                print("Detected: transit time data")
                n = import_transit_time(df, session, args.clear)
                print(f"  Inserted {n} transit time rows")
            else:
                print("Detected: seasonality data")
                n = import_seasonality(df, session, args.clear)
                print(f"  Inserted {n} seasonality rows")
        else:
            xl = pd.ExcelFile(filepath)
            print(f"Sheets found: {xl.sheet_names}")

            # Transit time sheet
            transit_sheet = args.sheet_transit or find_sheet(xl, ["transit", "route", "lead time", "shipping time"])
            if transit_sheet:
                print(f"\nImporting transit time from sheet: '{transit_sheet}'")
                df_t = pd.read_excel(filepath, sheet_name=transit_sheet)
                df_t = df_t.dropna(how="all")
                n = import_transit_time(df_t, session, args.clear)
                print(f"  Inserted {n} transit time rows")
            else:
                print("\nNo transit time sheet found. Use --sheet-transit 'Sheet Name' to specify.")

            # Seasonality sheet
            season_sheet = args.sheet_season or find_sheet(xl, ["season", "availability", "calendar"])
            if season_sheet:
                print(f"\nImporting seasonality from sheet: '{season_sheet}'")
                df_s = pd.read_excel(filepath, sheet_name=season_sheet)
                df_s = df_s.dropna(how="all")
                n = import_seasonality(df_s, session, args.clear)
                print(f"  Inserted {n} seasonality rows")
            else:
                print("\nNo seasonality sheet found. Use --sheet-season 'Sheet Name' to specify.")

        session.commit()
        print("\nDone. Data committed to RDS.")

    except Exception as e:
        session.rollback()
        print(f"ERROR: {e}")
        sys.exit(1)
    finally:
        session.close()


if __name__ == "__main__":
    main()
