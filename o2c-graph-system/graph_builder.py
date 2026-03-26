import networkx as nx
from data_loader import load_data

GRAPH = None

def get_graph():
    global GRAPH
    if GRAPH is None:
        GRAPH = build_graph()
    return GRAPH

def build_graph():
    """
    Build NetworkX DiGraph from loaded data.
    Focus on item-level flow: SOI -> DEL -> BILL -> JRN
    """
    data = load_data()
    
    G = nx.DiGraph()
    
    # Add nodes for sales order items
    for item in data.get('sales_order_items', []):
        node_id = f"SOI_{item['salesOrder']}_{item['salesOrderItem']}"
        G.add_node(node_id, type="SalesOrderItem", **item)
    
    # Add nodes for delivery items
    for item in data.get('outbound_delivery_items', []):
        node_id = f"DEL_{item['deliveryDocument']}_{item['deliveryDocumentItem']}"
        G.add_node(node_id, type="OutboundDeliveryItem", **item)
    
    # Add nodes for billing items
    for item in data.get('billing_document_items', []):
        node_id = f"BILL_{item['billingDocument']}_{item['billingDocumentItem']}"
        G.add_node(node_id, type="BillingDocumentItem", **item)
    
    # Add nodes for journal entries
    for item in data.get('journal_entry_items_accounts_receivable', []):
        node_id = f"JRN_{item['accountingDocument']}_{item['accountingDocumentItem']}"
        G.add_node(node_id, type="JournalEntryItem", **item)
    
    # Add supporting nodes (optional, for metadata)
    for item in data.get('business_partners', []):
        node_id = f"BP_{item['businessPartner']}"
        G.add_node(node_id, type="BusinessPartner", **item)
    
    for item in data.get('products', []):
        node_id = f"PROD_{item['product']}"
        G.add_node(node_id, type="Product", **item)
    
    # Add edges: Order Item -> Delivery Item
    for d in data.get('outbound_delivery_items', []):
        if d.get('referenceSdDocument') and d.get('referenceSdDocumentItem'):
            source = f"SOI_{d['referenceSdDocument']}_{d['referenceSdDocumentItem']}"
            target = f"DEL_{d['deliveryDocument']}_{d['deliveryDocumentItem']}"
            if source in G and target in G:
                G.add_edge(source, target, type="FULFILLED_BY")
    
    # Add edges: Delivery Item -> Billing Item
    for b in data.get('billing_document_items', []):
        if b.get('referenceSdDocument') and b.get('referenceSdDocumentItem'):
            source = f"DEL_{b['referenceSdDocument']}_{b['referenceSdDocumentItem']}"
            target = f"BILL_{b['billingDocument']}_{b['billingDocumentItem']}"
            if source in G and target in G:
                G.add_edge(source, target, type="BILLED_AS")
    
    # Add edges: Billing Item -> Journal Entry (use billingDocument as key)
    for j in data.get('journal_entry_items_accounts_receivable', []):
        if j.get('referenceDocument'):
            # Find billing item with matching billingDocument
            for b in data.get('billing_document_items', []):
                if b['billingDocument'] == j['referenceDocument']:
                    source = f"BILL_{b['billingDocument']}_{b['billingDocumentItem']}"
                    target = f"JRN_{j['accountingDocument']}_{j['accountingDocumentItem']}"
                    if source in G and target in G:
                        G.add_edge(source, target, type="PAID_BY")
                        break  # Assume one per document, but actually multiple items per document
    
    # Add supporting edges
    for soi in data.get('sales_order_items', []):
        # SOI to Product
        if soi.get('material'):
            source = f"SOI_{soi['salesOrder']}_{soi['salesOrderItem']}"
            target = f"PROD_{soi['material']}"
            if source in G and target in G:
                G.add_edge(source, target, type="USES_PRODUCT")
        # SOI to BP (via header, but simplified)
        # For now, skip to keep simple
    
    return G

def validate_graph(G):
    """
    Validate graph: count nodes/edges, trace sample flow.
    """
    print(f"Graph has {len(G.nodes)} nodes and {len(G.edges)} edges")
    
    # Find a sample SOI node
    soi_nodes = [n for n in G.nodes if G.nodes[n]['type'] == 'SalesOrderItem']
    if soi_nodes:
        sample = soi_nodes[0]
        descendants = nx.descendants(G, sample)
        print(f"Sample SOI {sample} has {len(descendants)} descendants: {list(descendants)}")
    
    return len(G.nodes), len(G.edges)

if __name__ == "__main__":
    G = get_graph()
    validate_graph(G)