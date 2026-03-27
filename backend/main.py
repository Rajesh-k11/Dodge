import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from dotenv import load_dotenv

from backend.services.db import seed_database
from backend.services.llm_service import ask_database

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

load_dotenv()


# ── Startup / Shutdown ────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Server starting up — seeding database…")
    try:
        seed_database()
    except Exception as e:
        logger.error(f"Database seeding failed: {e}")
    yield


# ── App ───────────────────────────────────────────────────────────────────────
app = FastAPI(title="O2C Graph Intelligence System API", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class QueryRequest(BaseModel):
    query: str


# ── Routes ────────────────────────────────────────────────────────────────────
@app.get("/")
def health_check():
    try:
        return {"status": "ok", "message": "O2C Graph Intelligence System Backend is running."}
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return {"error": "Unexpected error during health check."}


@app.get("/api/graph")
def get_graph_data():
    try:
        nodes = [
            {"id": "order_1",   "label": "Order 1001",     "type": "Order"},
            {"id": "invoice_1", "label": "Invoice INV-001", "type": "Invoice"},
            {"id": "payment_1", "label": "Payment PAY-001", "type": "Payment"},
        ]
        edges = [
            {"source": "order_1",   "target": "invoice_1", "label": "generates"},
            {"source": "invoice_1", "target": "payment_1", "label": "paid_by"},
        ]
        return {"nodes": nodes, "edges": edges}
    except Exception as e:
        logger.error(f"Error fetching graph data: {e}")
        return {"error": "Failed to fetch graph data.", "details": str(e)}


@app.post("/api/query")
def process_query(request: QueryRequest):
    try:
        if not request.query or not request.query.strip():
            return {"error": "Query cannot be empty."}
        return ask_database(request.query)
    except Exception as e:
        logger.error(f"Error processing user query: {e}")
        return {"error": "An unexpected error occurred.", "details": str(e)}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("backend.main:app", host="0.0.0.0", port=10000)