import json
from pathlib import Path

DATA_PATH = Path("../sap-order-to-cash-dataset/sap-o2c-data")

def load_data():
    """
    Load all JSONL files from the dataset into a dict of {entity: list of dicts}.
    Entity names are derived from folder names (e.g., 'sales_order_headers').
    """
    data = {}
    if not DATA_PATH.exists():
        raise FileNotFoundError(f"Data path {DATA_PATH} not found")
    
    for folder in DATA_PATH.iterdir():
        if folder.is_dir():
            entity = folder.name  # e.g., 'sales_order_headers'
            data[entity] = []
            for file_path in folder.glob("*.jsonl"):
                with open(file_path, 'r', encoding='utf-8') as f:
                    for line in f:
                        if line.strip():
                            data[entity].append(json.loads(line))
    return data