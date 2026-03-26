O2C Graph + SQL + LLM Demo

This project is a practical, no-nonsense toolkit for order-to-cash analytics and traceability. It brings together data from multiple sources and gives you an easy way to answer both operational trace questions and business analytics questions.

The key idea is simple:
- Represent the flow as a graph to follow document lineage end-to-end.
- Use SQL for aggregated counts and ranking queries.
- Expose everything through an API that can be driven by natural language.

## 1. Problem

In enterprise IT landscapes, O2C data is usually scattered across orders systems, delivery systems, billing systems, and financial journals. That makes it awkward to ask questions like:

- “Has this billing document completed the full workflow?”
- “Which products appear most often in invoices?”

Answering that today often requires stitching together multiple tools and writing complex joins.

## 2. Architecture

The architecture is intentionally straightforward:

1. JSONL dataset input
2. SQL Layer (SQLite) + Graph Layer (NetworkX)
3. Query Planner (LLM-assisted intent classification and structured plan)
4. Execution Engine (SQL for analytics, graph for traces)
5. FastAPI backend
6. Optional UI layer (graph view + chat interface)

- Graph is cached after initial load to avoid rebuild overhead.

### Files and responsibilities

- `sql_layer.py`
  - Ingests JSONL into SQLite tables
  - Supports safe resume (progress checkpointing)
- `graph_builder.py`
  - Builds node/edge graph for O2C relationships (Order → Delivery → Invoice → Journal Entry)
- `query_engine.py`
  - Routes query intent to SQL or graph routines
  - Provides concrete endpoints used by the demo
- `main.py`
  - FastAPI app with both core and demo endpoints

## 3. Design decisions

### Why graph?

Graph traversal is the natural fit for lineage and trace scenarios:
- Move downstream from an order
- Inspect which deliveries matched invoices
- Find the journal entries linked to a billing document

### Why SQL?

SQL is the right tool for analytics that involve grouping, counting, and ranking:
- Top products by invoice volume
- Delivery-not-billed edge cases
- Summary reports at the table level

### Hybrid approach

We didn’t pick “graph only” or “SQL only.”
We use the tool that fits each need:
- Graph = relationship exploration
- SQL = numeric computation

This keeps the system clear, efficient, and easier to reason about.

## 4. LLM strategy

This is a planner-based interface, not a free-form question engine.

- User submits text.
- The system classifies it as trace vs analytics vs unsupported.
- A deterministic plan is generated.
- The plan executes safely on graph and/or SQL.
- Results are returned as structured JSON (with optional natural language summary).

### LLM generates structured execution plans (SQL or graph queries), which are then executed deterministically.

Guardrails in place:
- Only recognized O2C entities are allowed
- No arbitrary SQL injection
- Unsupported query types are rejected

## 5. Tradeoffs

| Decision | Tradeoff |
|---|---|
| In-memory graph | very fast for traversal, but not distributed scale |
| SQLite | easy setup, limited concurrency under heavy load |
| Planner-risk | reliable and stable, but more constrained than open generation |

Value focus:
- simplicity
- reliability
- fast iteration

## 6. Demo queries

1. `GET /demo/trace-billing/91150187`
   - shows end-to-end path
   - Order → Delivery → Invoice → Journal Entry
   - includes step details and metadata

2. `GET /demo/top-products?limit=5`
   - SQL aggregation baseline
   - returns top products by invoice count

3. `GET /demo/delivered-not-billed`
   - detects completed deliveries that are missing billing records
   - uses cross-table logic for “broken flow” detection

## 7. Running the project

1. Start the API:

```bash
python -m uvicorn main:app --host 127.0.0.1 --port 8002
```

2. Run tests:

```bash
python test_api.py
```

3. Verify the demo endpoints:

- `/healthz`
- `/demo/trace-billing/91150187`
- `/demo/top-products?limit=5`
- `/demo/delivered-not-billed`

## 8. Future improvements

- Replace rule-based planner with a full LLM plan generator
- Add graph caching and persistence
- Add pagination / cursor APIs for large result sets
- Improve the UI with path highlighting, filters, and a conversational chat widget
