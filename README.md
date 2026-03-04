# 🌿 ScionAgricos — Financial Forecasting & Shipment Planning System

## Backend API (FastAPI + Python)

Built from 12 Excel files (4 traders × 3 years: 2023–2025) with ~1,800+ shipment records.

---

## ⚡ Quick Start in VS Code

### 1. Install dependencies
```bash
pip install -r requirements.txt
```

### 2. Place your Excel data files
Copy all 12 `Cash_Flow_*.xlsx` files into the `data/` folder:
```
data/
  Cash_Flow_2023_-_Chiru_.xlsx
  Cash_Flow_2023_-_Madhu.xlsx
  Cash_Flow_2023_-_Mahendra.xlsx
  Cash_Flow_2023_-_Unmesh.xlsx
  Cash_Flow_2024_-_Chiru_.xlsx
  ... (all 12 files)
```

### 3. Run the server
```bash
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

### 4. Open in browser
- **Swagger UI** (interactive docs): http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc
- **API base**: http://localhost:8000/api/v1

---

## 📁 Project Structure

```
scionagricos/
├── main.py                        # FastAPI app entry point
├── requirements.txt
├── data/                          # Excel source files (12 files)
└── app/
    ├── routers/
    │   └── api.py                 # All REST endpoints
    └── services/
        ├── data_loader.py         # Excel parsing & data normalization
        └── analytics.py           # Business logic & calculations
```

---

## 🔌 API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/v1/health` | Server health + record counts |
| GET | `/api/v1/filters` | Available years, traders, products, origins |
| GET | `/api/v1/dashboard/kpi` | Top KPIs: Revenue, Cost, Margin%, EBITDA |
| GET | `/api/v1/dashboard/revenue-trend` | 3-year revenue vs expense trend |
| GET | `/api/v1/dashboard/trader-performance` | Per-trader breakdown |
| GET | `/api/v1/analytics/shipments` | Full shipments table (filterable) |
| GET | `/api/v1/analytics/products` | Revenue & margin by product |
| GET | `/api/v1/analytics/origins` | Revenue by origin country |
| GET | `/api/v1/profitability/matrix` | Product × Origin profitability matrix |
| GET | `/api/v1/seasonal/monthly` | Monthly seasonality & intensity |
| GET | `/api/v1/cashflow/monthly` | Monthly cash flow with cumulative |
| GET | `/api/v1/forecast` | Linear revenue/margin forecast |
| GET | `/api/v1/scenario` | Scenario simulator (sliders) |
| GET | `/api/v1/risk` | Risk overview & score |
| GET | `/api/v1/breakeven` | Break-even calculator |
| GET | `/api/v1/kpi/full` | Full KPI set with trends |

---

## 🔍 Query Parameters (Examples)

```bash
# KPIs for 2024
GET /api/v1/dashboard/kpi?year=2024

# KPIs for Madhu trader only
GET /api/v1/dashboard/kpi?trader=Madhu

# Shipments filtered by product and origin
GET /api/v1/analytics/shipments?product=Grapes&origin=India&year=2023

# Scenario: 120 containers, +10% cost, -5% price
GET /api/v1/scenario?year=2024&containers=120&cost_increase_pct=10&price_decrease_pct=5

# Break-even: grapes from India
GET /api/v1/breakeven?cost_per_kg=1.80&freight_cost=4500&container_capacity_kg=12000

# 5-year forecast
GET /api/v1/forecast?years_ahead=5
```

---

## 📊 Data Sources

| Trader | Sheets Available |
|--------|-----------------|
| Chiru | Global Business, Own Imports |
| Madhu | Import Sales, Spot Trading, Global Business, Own Imports |
| Mahendra | Import Sales, Spot Trading, Global Business, Own Imports |
| Unmesh | Export Sales, Global Sales |

### Products traded:
Grapes, Coconuts, Limes, Oranges, Pomelos, Ginger, Garlic, Sweet Potatoes

### Origins:
India, Ivory Coast, Brazil, Egypt, China

---

## 🛠 VS Code Launch Config

Create `.vscode/launch.json`:
```json
{
  "version": "0.2.0",
  "configurations": [
    {
      "name": "ScionAgricos API",
      "type": "python",
      "request": "launch",
      "module": "uvicorn",
      "args": ["main:app", "--reload", "--port", "8000"],
      "jinja": true,
      "justMyCode": true
    }
  ]
}
```

Then press **F5** to start the server.
