import os
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from llm_interface import plan_and_execute
from sql_layer import init_database, load_data_to_sql

app = FastAPI(title='O2C Graph Query API')

# Initialize database on startup
@app.on_event("startup")
def startup_event():
    init_database()
    load_data_to_sql()

# Add CORS middleware
frontend_origin = os.getenv("FRONTEND_ORIGIN", "*")
allow_origins = [o.strip() for o in frontend_origin.split(",") if o.strip()]
if not allow_origins:
    allow_origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=allow_origins,
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods
    allow_headers=["*"],  # Allows all headers
)

# No mount, use catch-all for UI


class TraceRequest(BaseModel):
    order_id: str
    item_id: str


class LLMQueryRequest(BaseModel):
    query: str
    order_id: str | None = None
    item_id: str | None = None


@app.get('/healthz')
def health_check():
    return {'status': 'ok'}


@app.get('/broken-flows')
def broken_flows():
    from query_engine import query_broken_flow_summary
    try:
        return query_broken_flow_summary()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post('/trace-flow')
def trace_flow(body: TraceRequest):
    data = plan_and_execute('trace flow', {'order_id': body.order_id, 'item_id': body.item_id})
    if 'error' in data:
        raise HTTPException(status_code=400, detail=data['error'])
    return data


@app.post('/llm-query')
def llm_query(body: LLMQueryRequest):
    params = {'order_id': body.order_id, 'item_id': body.item_id}
    data = plan_and_execute(body.query, params)
    if 'error' in data:
        raise HTTPException(status_code=400, detail=data['error'])
    return data


class SQLQueryRequest(BaseModel):
    query: str


@app.post('/sql-query')
def sql_query(body: SQLQueryRequest):
    from query_engine import query_sql_direct
    result = query_sql_direct(body.query)
    if not result.get('success'):
        raise HTTPException(status_code=400, detail=result.get('error'))
    return result


@app.get('/demo/trace-billing/{billing_id}')
def demo_trace_billing(billing_id: str):
    from query_engine import query_trace_billing_document
    return query_trace_billing_document(billing_id)


@app.get('/demo/top-products')
def demo_top_products(limit: int = 5):
    from query_engine import query_top_products_by_invoice_count
    return query_top_products_by_invoice_count(limit)


@app.get('/demo/delivered-not-billed')
def demo_delivered_not_billed():
    from query_engine import query_orders_delivered_not_billed
    return query_orders_delivered_not_billed()


@app.get("/{path:path}")
def serve_ui(path: str):
    from pathlib import Path
    file_path = Path("ui") / path
    if file_path.exists() and file_path.is_file():
        return FileResponse(file_path)
    # For SPA, serve index.html for unknown paths
    return FileResponse("ui/index.html")
