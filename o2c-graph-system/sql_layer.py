import sqlite3
import json
import os
import sys
from pathlib import Path

# Database setup
DB_PATH = os.getenv("DB_PATH", "o2c.db")
DATA_DIR = Path(os.getenv("DATA_PATH", "../sap-order-to-cash-dataset/sap-o2c-data"))


def get_connection():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def run_sql(query, params=None):
    """Execute SQL query and return results."""
    conn = get_connection()
    cur = conn.cursor()
    try:
        if params:
            cur.execute(query, params)
        else:
            cur.execute(query)

        if query.strip().upper().startswith(('SELECT', 'PRAGMA')):
            results = cur.fetchall()
            return results
        else:
            conn.commit()
            return cur.rowcount
    finally:
        conn.close()

def get_progress():
    """Get current loading progress."""
    try:
        tables = ['sales_orders', 'sales_order_items', 'outbound_deliveries', 'delivery_items', 'billing_documents', 'journal_entries', 'payments']
        progress = {}
        for table in tables:
            result = run_sql(f"SELECT COUNT(*) FROM {table}")
            progress[table] = result[0][0] if result else 0
        return progress
    except:
        return {}

def save_progress(stage):
    """Save current loading stage."""
    with open('load_progress.txt', 'w') as f:
        f.write(stage)

def load_progress():
    """Load last completed stage."""
    if os.path.exists('load_progress.txt'):
        with open('load_progress.txt', 'r') as f:
            return f.read().strip()
    return None

def init_database():
    """Initialize database tables."""
    # Sales Orders
    run_sql("""
    CREATE TABLE IF NOT EXISTS sales_orders (
        order_id TEXT PRIMARY KEY,
        customer_id TEXT,
        order_date TEXT,
        status TEXT
    )
    """)

    # Sales Order Items
    run_sql("""
    CREATE TABLE IF NOT EXISTS sales_order_items (
        soi_id TEXT PRIMARY KEY,
        order_id TEXT,
        item_id TEXT,
        product_id TEXT,
        quantity REAL,
        amount REAL,
        status TEXT,
        FOREIGN KEY (order_id) REFERENCES sales_orders(order_id)
    )
    """)

    # Outbound Deliveries
    run_sql("""
    CREATE TABLE IF NOT EXISTS outbound_deliveries (
        delivery_id TEXT PRIMARY KEY,
        order_id TEXT,
        delivery_date TEXT,
        status TEXT,
        FOREIGN KEY (order_id) REFERENCES sales_orders(order_id)
    )
    """)

    # Delivery Items
    run_sql("""
    CREATE TABLE IF NOT EXISTS delivery_items (
        delivery_item_id TEXT PRIMARY KEY,
        delivery_id TEXT,
        soi_id TEXT,
        quantity REAL,
        FOREIGN KEY (delivery_id) REFERENCES outbound_deliveries(delivery_id),
        FOREIGN KEY (soi_id) REFERENCES sales_order_items(soi_id)
    )
    """)

    # Billing Documents
    run_sql("""
    CREATE TABLE IF NOT EXISTS billing_documents (
        billing_id TEXT PRIMARY KEY,
        delivery_id TEXT,
        invoice_date TEXT,
        amount REAL,
        status TEXT,
        FOREIGN KEY (delivery_id) REFERENCES outbound_deliveries(delivery_id)
    )
    """)

    # Billing Items
    run_sql("""
    CREATE TABLE IF NOT EXISTS billing_items (
        billing_item_id TEXT PRIMARY KEY,
        billing_id TEXT,
        delivery_item_id TEXT,
        amount REAL,
        FOREIGN KEY (billing_id) REFERENCES billing_documents(billing_id),
        FOREIGN KEY (delivery_item_id) REFERENCES delivery_items(delivery_item_id)
    )
    """)

    # Journal Entries
    run_sql("""
    CREATE TABLE IF NOT EXISTS journal_entries (
        journal_id TEXT PRIMARY KEY,
        billing_id TEXT,
        posting_date TEXT,
        amount REAL,
        account_type TEXT,
        FOREIGN KEY (billing_id) REFERENCES billing_documents(billing_id)
    )
    """)

    # Payments
    run_sql("""
    CREATE TABLE IF NOT EXISTS payments (
        payment_id TEXT PRIMARY KEY,
        billing_id TEXT,
        payment_date TEXT,
        amount REAL,
        status TEXT,
        FOREIGN KEY (billing_id) REFERENCES billing_documents(billing_id)
    )
    """)

def load_data_to_sql(limit=None):
    """Load data from JSONL files into SQL database."""
    data_dir = DATA_DIR

    if not data_dir.exists():
        print("Data directory not found at", data_dir)
        return

    print("Loading data into SQL database...")
    if limit:
        print(f"Limited to {limit} records per table for testing")

    last_stage = load_progress()
    print(f"Resuming from stage: {last_stage or 'start'}")

    try:
        # Load sales orders
        if last_stage is None:
            so_file = data_dir / "sales_order_headers" / "part-20251119-133429-440.jsonl"
            if so_file.exists():
                print("Loading sales orders...")
                with open(so_file, 'r') as f:
                    count = 0
                    for line in f:
                        if limit and count >= limit:
                            break
                        record = json.loads(line)
                        run_sql("""
                        INSERT OR REPLACE INTO sales_orders (order_id, customer_id, order_date, status)
                        VALUES (?, ?, ?, ?)
                        """, (
                            record.get('VBELN'),  # Sales Order
                            record.get('KUNNR'),  # Customer
                            record.get('AUDAT'),  # Order Date
                            record.get('STATUS', 'Open')
                        ))
                        count += 1
                        if count % 1000 == 0:
                            print(f"Loaded {count} sales orders...")
                print(f"Loaded {count} sales orders total")
                save_progress('sales_orders')

        # Load sales order items
        if last_stage in [None, 'sales_orders']:
            soi_file = data_dir / "sales_order_items" / "part-20251119-133429-452.jsonl"
            if soi_file.exists():
                print("Loading sales order items...")
                with open(soi_file, 'r') as f:
                    count = 0
                    for line in f:
                        if limit and count >= limit:
                            break
                        record = json.loads(line)
                        soi_id = f"{record.get('VBELN')}_{record.get('POSNR')}"
                        run_sql("""
                        INSERT OR REPLACE INTO sales_order_items (soi_id, order_id, item_id, product_id, quantity, amount, status)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        """, (
                            soi_id,
                            record.get('VBELN'),  # Order ID
                            record.get('POSNR'),  # Item Number
                            record.get('MATNR'),  # Material
                            float(record.get('KWMENG', 0)),  # Quantity
                            float(record.get('NETWR', 0)),   # Net Value
                            record.get('STATUS', 'Open')
                        ))
                        count += 1
                        if count % 1000 == 0:
                            print(f"Loaded {count} sales order items...")
                print(f"Loaded {count} sales order items total")
                save_progress('sales_order_items')

        # Load outbound deliveries
        if last_stage in [None, 'sales_orders', 'sales_order_items']:
            del_file = data_dir / "outbound_delivery_headers" / "part-20251119-133431-414.jsonl"
            if del_file.exists():
                print("Loading outbound deliveries...")
                with open(del_file, 'r') as f:
                    count = 0
                    for line in f:
                        if limit and count >= limit:
                            break
                        record = json.loads(line)
                        run_sql("""
                        INSERT OR REPLACE INTO outbound_deliveries (delivery_id, order_id, delivery_date, status)
                        VALUES (?, ?, ?, ?)
                        """, (
                            record.get('VBELN'),  # Delivery
                            record.get('VGBEL'),  # Reference Order
                            record.get('WADAT'),  # Goods Issue Date
                            record.get('STATUS', 'Open')
                        ))
                        count += 1
                        if count % 500 == 0:
                            print(f"Loaded {count} deliveries...")
                print(f"Loaded {count} deliveries total")
                save_progress('outbound_deliveries')

        # Load delivery items
        if last_stage in [None, 'sales_orders', 'sales_order_items', 'outbound_deliveries']:
            deli_file = data_dir / "outbound_delivery_items" / "part-20251119-133431-439.jsonl"
            if deli_file.exists():
                print("Loading delivery items...")
                with open(deli_file, 'r') as f:
                    count = 0
                    for line in f:
                        if limit and count >= limit:
                            break
                        record = json.loads(line)
                        delivery_item_id = f"{record.get('VBELN')}_{record.get('POSNR')}"
                        soi_id = f"{record.get('VGBEL')}_{record.get('VGPOS')}"

                        # Check if delivery exists
                        delivery_exists = run_sql("SELECT 1 FROM outbound_deliveries WHERE delivery_id = ?", (record.get('VBELN'),))
                        # Check if SOI exists
                        soi_exists = run_sql("SELECT 1 FROM sales_order_items WHERE soi_id = ?", (soi_id,))

                        if delivery_exists and soi_exists:
                            run_sql("""
                            INSERT OR REPLACE INTO delivery_items (delivery_item_id, delivery_id, soi_id, quantity)
                            VALUES (?, ?, ?, ?)
                            """, (
                                delivery_item_id,
                                record.get('VBELN'),  # Delivery
                                soi_id,               # Reference SOI
                                float(record.get('LFIMG', 0))  # Delivered Quantity
                            ))
                            count += 1
                            if count % 500 == 0:
                                print(f"Loaded {count} delivery items...")
                print(f"Loaded {count} delivery items total")
                save_progress('delivery_items')

        # Load billing documents
        if last_stage in [None, 'sales_orders', 'sales_order_items', 'outbound_deliveries', 'delivery_items']:
            bill_file = data_dir / "billing_document_headers" / "part-20251119-133433-228.jsonl"
            if bill_file.exists():
                print("Loading billing documents...")
                with open(bill_file, 'r') as f:
                    count = 0
                    for line in f:
                        if limit and count >= limit:
                            break
                        record = json.loads(line)
                        # Check if delivery exists
                        delivery_exists = run_sql("SELECT 1 FROM outbound_deliveries WHERE delivery_id = ?", (record.get('VGBEL'),))
                        if delivery_exists:
                            run_sql("""
                            INSERT OR REPLACE INTO billing_documents (billing_id, delivery_id, invoice_date, amount, status)
                            VALUES (?, ?, ?, ?, ?)
                            """, (
                                record.get('VBELN'),  # Billing Document
                                record.get('VGBEL'),  # Reference Delivery
                                record.get('FKDAT'),  # Billing Date
                                float(record.get('NETWR', 0)),  # Net Value
                                record.get('STATUS', 'Open')
                            ))
                            count += 1
                            if count % 500 == 0:
                                print(f"Loaded {count} billing documents...")
                print(f"Loaded {count} billing documents total")
                save_progress('billing_documents')

        # Load journal entries
        if last_stage in [None, 'sales_orders', 'sales_order_items', 'outbound_deliveries', 'delivery_items', 'billing_documents']:
            journal_file = data_dir / "journal_entry_items_accounts_receivable" / "part-20251119-133433-74.jsonl"
            if journal_file.exists():
                print("Loading journal entries...")
                with open(journal_file, 'r') as f:
                    count = 0
                    for line in f:
                        if limit and count >= limit:
                            break
                        record = json.loads(line)
                        journal_id = f"{record.get('BUKRS')}_{record.get('BELNR')}_{record.get('BUZEI')}"
                        # Check if billing exists
                        billing_exists = run_sql("SELECT 1 FROM billing_documents WHERE billing_id = ?", (record.get('VBELN'),))
                        if billing_exists:
                            run_sql("""
                            INSERT OR REPLACE INTO journal_entries (journal_id, billing_id, posting_date, amount, account_type)
                            VALUES (?, ?, ?, ?, ?)
                            """, (
                                journal_id,
                                record.get('VBELN'),  # Billing Document
                                record.get('BUDAT'),  # Posting Date
                                float(record.get('DMBTR', 0)),  # Amount
                                record.get('KOART', 'D')  # Account Type
                            ))
                            count += 1
                            if count % 500 == 0:
                                print(f"Loaded {count} journal entries...")
                print(f"Loaded {count} journal entries total")
                save_progress('journal_entries')

        # Load payments
        if last_stage in [None, 'sales_orders', 'sales_order_items', 'outbound_deliveries', 'delivery_items', 'billing_documents', 'journal_entries']:
            payment_file = data_dir / "payments_accounts_receivable" / "part-20251119-133434-100.jsonl"
            if payment_file.exists():
                print("Loading payments...")
                with open(payment_file, 'r') as f:
                    count = 0
                    for line in f:
                        if limit and count >= limit:
                            break
                        record = json.loads(line)
                        # Check if billing exists
                        billing_exists = run_sql("SELECT 1 FROM billing_documents WHERE billing_id = ?", (record.get('VBELN'),))
                        if billing_exists:
                            run_sql("""
                            INSERT OR REPLACE INTO payments (payment_id, billing_id, payment_date, amount, status)
                            VALUES (?, ?, ?, ?, ?)
                            """, (
                                record.get('BELNR'),  # Document Number
                                record.get('VBELN'),  # Billing Document
                                record.get('BUDAT'),  # Posting Date
                                float(record.get('DMBTR', 0)),  # Amount
                                record.get('STATUS', 'Cleared')
                            ))
                            count += 1
                            if count % 500 == 0:
                                print(f"Loaded {count} payments...")
                print(f"Loaded {count} payments total")
                save_progress('payments')

        print("Data loaded into SQL database")
        if os.path.exists('load_progress.txt'):
            os.remove('load_progress.txt')

    except KeyboardInterrupt:
        print("\nLoading interrupted by user. Progress saved.")
        progress = get_progress()
        print("Current progress:", progress)
        return False
    except Exception as e:
        print(f"Error during loading: {e}")
        return False

    return True

if __name__ == "__main__":
    init_database()
    load_data_to_sql()
    print("SQL setup completed")