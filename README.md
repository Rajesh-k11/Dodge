# O2C Graph Intelligence System

A production-grade **Order-to-Cash (O2C) Graph Analytics Platform** that ingests SAP-style JSONL data, constructs an interactive entity relationship graph, and exposes a natural language chat interface backed by Gemini LLM.

---

## Live Demo

> Deploy locally — see setup below (no SaaS hosting required per the brief)

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────┐
│                     Frontend (React)                     │
│  ┌────────────────────┐   ┌──────────────────────────┐  │
│  │  vis-network Graph │   │  Chat Interface (NL Query)│  │    
│  │  Interactive nodes │   │  AI-powered answers      │  │
│  └────────┬───────────┘   └──────────┬───────────────┘  │
│           │                          │                   │
└───────────┼──────────────────────────┼───────────────────┘
            │  HTTP (REST / Axios)      │
┌───────────▼──────────────────────────▼───────────────────┐
│                   Backend (FastAPI)                        │
│  ┌────────────────┐   ┌──────────────────────────────┐   │
│  │  /api/graph    │   │  /api/query                  │   │
│  │  Typed nodes + │   │  NL → SQL → Execute → Answer │   │
│  │  edges from DB │   │  In-memory cache layer       │   │
│  └────────┬───────┘   └──────────────────────────────┘   │
│           │                                               │
│  ┌────────▼─────────────────────────────────────────┐    │
│  │            SQLite (o2c.db)                         │    │
│  │  products · orders · deliveries · invoices ···    │    │
│  └──────────────────────────────────────────────────┘    │
└───────────────────────────────────────────────────────────┘
                          │
         ┌────────────────▼────────────────┐
         │     Google Gemini 2.5 Flash      │
         │  SQL Generation + NL Summarizer  │
         └─────────────────────────────────┘
```

---

## Database Choice — SQLite

**Why SQLite:**
- Zero-setup, file-based — perfect for a normalized flat dataset from JSONL
- Full SQL expressiveness: JOINs, GROUP BY, aggregation — needed for O2C analytics
- The dataset fits comfortably in memory; no distributed infra required
- Native Python support via `sqlite3` — no ORM overhead

**Trade-off acknowledged:** Not suitable for concurrent writes or large-scale production, but ideal for this read-heavy analytics use case.

---

## Data Pipeline

The JSONL dataset is ingested via `ingest.py`:

1. **Parse** — Each JSONL file in the `data/sap-o2c-data/` folder is read line-by-line
2. **Flatten** — Nested JSON objects are recursively flattened with `camelCase → snake_case` key normalization
3. **Schema discovery** — Column names are collected dynamically; no hardcoded schema needed
4. **SQLite Tables** — One table per folder name (e.g. `products`, `orders`, `deliveries`)
5. **Batch INSERT** — Rows are inserted in batches for performance

Each entity becomes its own SQLite table, and relationships are expressed through JOIN queries at query time.

---

## Graph Modeling

### Node Types

| Type | Color | Entity |
|---|---|---|
| **Product** | 🟢 Green | Core product entity |
| **Group** | 🟠 Orange | Product group |
| **Division** | 🟡 Yellow | Business division |
| **User** | 🟣 Purple | `created_by_user` field |
| **Sector** | 🩵 Teal | Industry sector |
| **Attribute** | 🔵 Blue | Any other field value |

### Edge Relationships

```
product ──→ product_group
product ──→ division
product ──→ created_by_user
product ──→ industry_sector
```

Edges are derived from the natural foreign-key-style fields in the flat dataset and are materialized in the frontend via `convertToGraph()`.

---

## LLM Integration — Gemini 2.5 Flash

**Two-call pipeline per query:**

### Call 1: SQL Generation
```
Prompt:  Schema (JSON) + User question
Output:  { "sql": "SELECT ...", "explanation": "..." }
Config:  temperature=0.1 (deterministic)
```

### Call 2: Natural Language Answer
```
Prompt:  Question + SQL + Raw DB results (max 10 rows shown)
Output:  2-3 sentence business-friendly answer with data citations
Config:  temperature=0.1
```

**Optimizations:**
- **Caching:** MD5-keyed `QUERY_CACHE` dict prevents duplicate LLM calls for repeated queries
- **Quota handling:** 429 errors are caught at both pipeline stages; falls back to raw data with a friendly message
- **LIMIT enforcement:** All generated SQL gets `LIMIT 50` appended if not present

---

## Guardrails

```python
ALLOWED_KEYWORDS = [
    "order", "product", "customer", "invoice",
    "delivery", "payment", "sales", "billing",
    "user", "division", "group", "sector", "industry",
    "show", "list", "find", "get", "how many", "count", "top"
]
```

- Queries **not containing any allowed keyword** are rejected before touching the LLM
- Response: `"This system is designed to answer questions related to the O2C dataset only."`
- SQL injection is prevented by using parameterized queries and restricting to `SELECT`/`PRAGMA` in `execute_query()`

---

## Example Queries the System Handles

| Question | What it does |
|---|---|
| Which products are in the highest number of billing documents? | GROUP BY + JOIN across products and billing tables |
| Show me all deliveries for customer X | Filter by customer across deliveries table |
| How many orders were created by user Y? | COUNT + filter on created_by_user |
| List products in division Z | Filter by division |
| What industry sectors are present? | DISTINCT on industry_sector |

---

## API Reference

| Endpoint | Method | Description |
|---|---|---|
| `/` | GET | Health check |
| `/api/query?q=...` | GET | NL → SQL → Answer pipeline |
| `/api/graph` | GET | Returns typed graph nodes + edges |
| `/api/schema` | GET | Returns DB table schema |
| `/api/cache/stats` | GET | View cache state |
| `/api/cache/clear` | POST | Clear in-memory query cache |

---

## Local Setup

### Prerequisites
- Python 3.10+
- Node.js 18+

### Backend
```bash
cd backend
python -m venv venv
venv\Scripts\activate        # Windows
pip install -r requirements.txt
cp .env.example .env         # Add your GEMINI_API_KEY
python ingest.py             # Ingest JSONL dataset → o2c.db
uvicorn main:app --reload    # Starts on http://localhost:8000
```

### Frontend
```bash
cd frontend
npm install
npm run dev                  # Starts on http://localhost:5173
```

### Environment Variables (`backend/.env`)
```
GEMINI_API_KEY=your_key_here
```

---

## Production Deployment

### Backend (Render)
1. Link your GitHub repo to Render as a Web Service.
2. Build Command: `pip install -r requirements.txt && python ingest.py`
3. Start Command: `uvicorn main:app --host 0.0.0.0 --port $PORT`
4. Set `GEMINI_API_KEY` in Render Environment Variables.

### Frontend (Netlify)
1. Link your GitHub repo to Netlify.
2. Build Command: `npm run build`
3. Publish Directory: `dist`
4. Add Environment Variable:
   - `VITE_API_URL` = `<your-render-backend-url>` (e.g. `https://o2c-backend-6ojv.onrender.com`)

---

## Project Structure

```
Dodge/
├── backend/
│   ├── main.py              # FastAPI routes
│   ├── db.py                # SQLite helpers
│   ├── ingest.py            # JSONL → SQLite pipeline
│   ├── services/
│   │   └── llm_service.py   # Gemini NL→SQL pipeline + cache
│   ├── data/
│   │   └── sap-o2c-data/    # JSONL dataset files
│   ├── o2c.db               # SQLite database (generated)
│   └── requirements.txt
└── frontend/
    ├── src/
    │   ├── App.jsx          # Main app: state, queries, layout
    │   ├── GraphView.jsx    # vis-network wrapper component
    │   └── App.css          # Design system (light theme)
    └── package.json
```

---

## Tech Stack

| Layer | Technology | Rationale |
|---|---|---|
| Backend | FastAPI | Async, typed, auto-docs |
| Database | SQLite | Zero-config, full SQL, file-portable |
| LLM | Gemini 2.5 Flash | Free tier, fast, structured JSON output |
| Graph Viz | vis-network | Superior physics, interactive, production-ready |
| Frontend | React + Vite | Lightweight, fast HMR |
| Styling | Vanilla CSS | Zero dependency, full control |

---

## Bonus Features Implemented

- ✅ NL → SQL dynamic translation
- ✅ Graph clustering (node type grouping + filter toggles)
- ✅ Node detail inspection panel (popover on click)
- ✅ Response caching (MD5 in-memory)
- ✅ Graceful LLM quota fallback
- ✅ Graph insights: most connected node, degree count, type breakdown
- ✅ Guardrail keyword filtering
- ✅ "Analyze Graph" client-side insights (no LLM needed)
