from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from services.llm_service import ask_database, QUERY_CACHE
from db import execute_query, get_schema

app = FastAPI(title="O2C Graph Intelligence API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def read_root():
    return "API running"


@app.get("/api/query")
def query(q: str):
    return ask_database(q)


@app.get("/api/graph")
def get_graph():
    """Return an initial schema graph showing products and their relationships."""
    try:
        # Fetch products with relationship fields
        data = execute_query(
            "SELECT product, product_group, division, created_by_user, industry_sector "
            "FROM products LIMIT 200"
        ) or []
    except Exception:
        data = []

    nodes = []
    edges = []
    seen_nodes = set()
    seen_edges = set()

    def add_node(nid, label, ntype):
        if nid not in seen_nodes:
            seen_nodes.add(nid)
            nodes.append({"id": nid, "label": label, "type": ntype})

    def add_edge(src, tgt):
        key = f"{src}→{tgt}"
        if key not in seen_edges:
            seen_edges.add(key)
            edges.append({"source": src, "target": tgt})

    for row in data:
        pid = str(row["product"])
        add_node(pid, pid, "Product")

        if row.get("product_group"):
            gid = f"grp_{row['product_group']}"
            add_node(gid, str(row["product_group"]), "Group")
            add_edge(pid, gid)

        if row.get("division"):
            did = f"div_{row['division']}"
            add_node(did, str(row["division"]), "Division")
            add_edge(pid, did)

        if row.get("created_by_user"):
            uid = f"usr_{row['created_by_user']}"
            add_node(uid, str(row["created_by_user"]), "User")
            add_edge(pid, uid)

        if row.get("industry_sector"):
            sid = f"sec_{row['industry_sector']}"
            add_node(sid, str(row["industry_sector"]), "Sector")
            add_edge(pid, sid)

    return {"nodes": nodes, "edges": edges}


@app.get("/api/schema")
def get_schema_info():
    """Return the DB schema for debugging."""
    return get_schema()


@app.post("/api/cache/clear")
def clear_cache():
    """Clear the in-memory query cache."""
    QUERY_CACHE.clear()
    return {"message": "Cache cleared.", "entries_cleared": len(QUERY_CACHE)}


@app.get("/api/cache/stats")
def cache_stats():
    return {"cached_queries": len(QUERY_CACHE), "keys": list(QUERY_CACHE.keys())[:10]}