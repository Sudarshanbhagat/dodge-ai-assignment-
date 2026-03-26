from sql_layer import run_sql, init_database, load_data_to_sql
import json
from flow_engine import trace_full_flow
from graph_builder import get_graph

# Database initialization moved to main.py startup event


def execute_plan(plan):
    """Execute a query plan using either SQL or graph execution."""
    if plan["type"] == "sql":
        return run_sql(plan["query"])
    
    elif plan["type"] == "graph":
        return trace_graph(plan["query"])


def trace_graph(query_params):
    """Execute graph-based flow tracing."""
    # Build the graph (lazy loading)
    G = get_graph()
    
    # Extract parameters from query
    start_node = query_params.get("start_node")
    if not start_node:
        return {"error": "start_node required for graph tracing"}
    
    return trace_full_flow(start_node, G)


def query_sales_order_item(order_id, item_id):
    """Query sales order item details from SQL database."""
    soi_id = f"{order_id}_{item_id}"
    results = run_sql("""
    SELECT soi_id, order_id, item_id, product_id, quantity, amount, status
    FROM sales_order_items
    WHERE soi_id = ?
    """, (soi_id,))

    if not results:
        return None

    row = results[0]
    return {
        "soi_id": row[0],
        "order_id": row[1],
        "item_id": row[2],
        "product_id": row[3],
        "quantity": row[4],
        "amount": row[5],
        "status": row[6],
        "type": "SalesOrderItem"
    }


def query_flow(order_id, item_id):
    """Query complete O2C flow for an item using SQL joins."""
    soi_id = f"{order_id}_{item_id}"

    # Get the complete flow using SQL joins
    results = run_sql("""
    SELECT
        soi.soi_id,
        soi.order_id,
        soi.item_id,
        soi.product_id,
        soi.quantity as soi_quantity,
        soi.amount as soi_amount,
        soi.status as soi_status,
        del.delivery_id,
        del.delivery_date,
        del.status as del_status,
        di.delivery_item_id,
        di.quantity as del_quantity,
        bill.billing_id,
        bill.invoice_date,
        bill.amount as bill_amount,
        bill.status as bill_status,
        je.journal_id,
        je.posting_date,
        je.amount as je_amount,
        je.account_type,
        pay.payment_id,
        pay.payment_date,
        pay.amount as pay_amount,
        pay.status as pay_status
    FROM sales_order_items soi
    LEFT JOIN delivery_items di ON soi.soi_id = di.soi_id
    LEFT JOIN outbound_deliveries del ON di.delivery_id = del.delivery_id
    LEFT JOIN billing_documents bill ON del.delivery_id = bill.delivery_id
    LEFT JOIN journal_entries je ON bill.billing_id = je.billing_id
    LEFT JOIN payments pay ON bill.billing_id = pay.billing_id
    WHERE soi.soi_id = ?
    """, (soi_id,))

    if not results:
        return {"error": "SalesOrderItem not found", "flow_path": [], "missing_links": ["SalesOrderItem not found"], "metadata": {}, "complete": False}

    row = results[0]

    # Build flow path and check completeness
    flow_path = []
    missing_links = []
    metadata = {}

    # SOI
    if row[0]:
        flow_path.append(row[0])
        metadata[row[0]] = {
            "type": "SalesOrderItem",
            "order_id": row[1],
            "item_id": row[2],
            "product_id": row[3],
            "quantity": row[4],
            "amount": row[5],
            "status": row[6]
        }

    # Delivery
    if row[7]:
        flow_path.append(row[7])
        metadata[row[7]] = {
            "type": "OutboundDelivery",
            "delivery_date": row[8],
            "status": row[9]
        }
    else:
        missing_links.append(f"Missing OutboundDelivery for {row[0]}")

    # Billing
    if row[12]:
        flow_path.append(row[12])
        metadata[row[12]] = {
            "type": "BillingDocument",
            "invoice_date": row[13],
            "amount": row[14],
            "status": row[15]
        }
    else:
        missing_links.append(f"Missing BillingDocument for delivery {row[7] or 'N/A'}")

    # Journal Entry
    if row[16]:
        flow_path.append(row[16])
        metadata[row[16]] = {
            "type": "JournalEntry",
            "posting_date": row[17],
            "amount": row[18],
            "account_type": row[19]
        }
    else:
        missing_links.append(f"Missing JournalEntry for billing {row[12] or 'N/A'}")

    # Payment
    if row[20]:
        flow_path.append(row[20])
        metadata[row[20]] = {
            "type": "Payment",
            "payment_date": row[21],
            "amount": row[22],
            "status": row[23]
        }
    else:
        missing_links.append(f"Missing Payment for billing {row[12] or 'N/A'}")

    complete = len(missing_links) == 0

    return {
        "flow_path": flow_path,
        "missing_links": missing_links,
        "metadata": metadata,
        "complete": complete
    }


def query_broken_flow_summary():
    """Query broken flow counts using SQL."""
    # SOI without deliveries
    soi_without_del = run_sql("""
    SELECT COUNT(*) FROM sales_order_items soi
    LEFT JOIN delivery_items di ON soi.soi_id = di.soi_id
    WHERE di.delivery_item_id IS NULL
    """)[0][0]

    # Deliveries without billing
    del_without_bill = run_sql("""
    SELECT COUNT(*) FROM outbound_deliveries del
    LEFT JOIN billing_documents bill ON del.delivery_id = bill.delivery_id
    WHERE bill.billing_id IS NULL
    """)[0][0]

    # Billing without journal entries
    bill_without_jrn = run_sql("""
    SELECT COUNT(*) FROM billing_documents bill
    LEFT JOIN journal_entries je ON bill.billing_id = je.billing_id
    WHERE je.journal_id IS NULL
    """)[0][0]

    return {
        "soi_without_del": soi_without_del,
        "del_without_bill": del_without_bill,
        "bill_without_jrn": bill_without_jrn
    }


def query_trace_billing_document(billing_id):
    """Trace an O2C chain by billing document."""
    # Billing -> Delivery -> Order -> Journal
    billing = run_sql("""
    SELECT b.billing_id, b.delivery_id, b.invoice_date, b.amount, b.status,
           d.order_id, d.delivery_date, d.status,
           di.delivery_item_id, di.soi_id, di.quantity
    FROM billing_documents b
    LEFT JOIN outbound_deliveries d ON b.delivery_id = d.delivery_id
    LEFT JOIN delivery_items di ON d.delivery_id = di.delivery_id
    WHERE b.billing_id = ?
    """, (billing_id,))

    if not billing:
        return {"error": f"Billing document {billing_id} not found"}

    # use the first row as header
    row = billing[0]
    order_id = row[5]
    delivery_id = row[1]

    # Get associated journal entries
    journals = run_sql("""
    SELECT journal_id, posting_date, amount, account_type
    FROM journal_entries
    WHERE billing_id = ?
    """, (billing_id,))

    path = {
        "order_id": order_id,
        "delivery_id": delivery_id,
        "billing_id": billing_id,
        "journal_ids": [j[0] for j in journals],
    }

    return {
        "trace": ["Order", "Delivery", "Invoice", "Journal"],
        "path": path,
        "billing": {
            "billing_id": row[0],
            "invoice_date": row[2],
            "amount": row[3],
            "status": row[4]
        },
        "delivery": {
            "delivery_id": row[1],
            "order_id": row[5],
            "delivery_date": row[6],
            "status": row[7]
        },
        "journal_entries": [{
            "journal_id": j[0],
            "posting_date": j[1],
            "amount": j[2],
            "account_type": j[3]
        } for j in journals]
    }


def query_top_products_by_invoice_count(limit=5):
    """Top N products by invoice count"""
    sql = """
    SELECT soi.product_id, COUNT(DISTINCT bill.billing_id) AS invoice_count
    FROM sales_order_items soi
    JOIN delivery_items di ON soi.soi_id = di.soi_id
    JOIN outbound_deliveries del ON di.delivery_id = del.delivery_id
    JOIN billing_documents bill ON del.delivery_id = bill.delivery_id
    GROUP BY soi.product_id
    ORDER BY invoice_count DESC
    LIMIT ?
    """
    results = run_sql(sql, (limit,))
    return [{"product_id": r[0], "invoice_count": r[1]} for r in results]


def query_orders_delivered_not_billed():
    """Orders with deliveries but no linked billing document."""
    results = run_sql("""
    SELECT DISTINCT del.order_id
    FROM outbound_deliveries del
    LEFT JOIN billing_documents bill ON del.delivery_id = bill.delivery_id
    WHERE bill.billing_id IS NULL AND del.order_id IS NOT NULL
    """)
    return [r[0] for r in results if r[0] is not None]


def query_sql_direct(sql_query):
    """Execute direct SQL queries for advanced analytics."""
    try:
        results = run_sql(sql_query)
        return {"results": results, "success": True}
    except Exception as e:
        return {"error": str(e), "success": False}
    """Execute direct SQL queries for advanced analytics."""
    try:
        results = run_sql(sql_query)
        return {"results": results, "success": True}
    except Exception as e:
        return {"error": str(e), "success": False}
