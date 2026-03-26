from graph_builder import get_graph


def trace_full_flow(start_node, G):
    """Trace the O2C item-level flow from a starting node.

    Returns:
      dict: {
        "flow_path": [node,...],
        "missing_links": [msg,...],
        "metadata": {node: {type, amount, date, status}},
        "complete": bool
      }
    """
    if start_node not in G:
        return {"error": f"Node {start_node} not found"}

    node_type = G.nodes[start_node].get("type")
    flow_sequence = {
        "SalesOrderItem": ["OutboundDeliveryItem", "BillingDocumentItem", "JournalEntryItem"],
        "OutboundDeliveryItem": ["BillingDocumentItem", "JournalEntryItem"],
        "BillingDocumentItem": ["JournalEntryItem"],
        "JournalEntryItem": [],
    }

    path = [start_node]
    missing = []
    current_nodes = [start_node]

    for expected in flow_sequence.get(node_type, []):
        next_nodes = []
        for current in current_nodes:
            successors = [n for n in G.successors(current) if G.nodes[n].get("type") == expected]
            if not successors:
                missing.append(f"Missing {expected} for {current}")
            else:
                for nxt in successors:
                    if nxt not in path:
                        path.append(nxt)
                    next_nodes.append(nxt)

        current_nodes = next_nodes

    metadata = {}
    for node in path:
        attrs = G.nodes[node]
        metadata[node] = {
            "type": attrs.get("type"),
            "amount": attrs.get("netAmount") or attrs.get("amountInTransactionCurrency") or attrs.get("totalNetAmount"),
            "date": attrs.get("creationDate") or attrs.get("bookingDate") or attrs.get("postingDate") or attrs.get("billingDocumentDate"),
            "status": attrs.get("overallDeliveryStatus") or attrs.get("overallOrdReltdBillgStatus") or attrs.get("overallGoodsMovementStatus") or attrs.get("billingDocumentIsCancelled"),
        }

    return {
        "flow_path": path,
        "missing_links": missing,
        "metadata": metadata,
        "complete": len(missing) == 0,
    }


def detect_broken_flows(G):
    """Detect incomplete item-level flows and return summary."""
    issues = {
        "soi_without_del": [],
        "del_without_bill": [],
        "bill_without_jrn": [],
    }

    for node, attrs in G.nodes(data=True):
        t = attrs.get("type")
        if t == "SalesOrderItem":
            has_child = any(G.nodes[s].get("type") == "OutboundDeliveryItem" for s in G.successors(node))
            if not has_child:
                issues["soi_without_del"].append(node)

        elif t == "OutboundDeliveryItem":
            has_child = any(G.nodes[s].get("type") == "BillingDocumentItem" for s in G.successors(node))
            if not has_child:
                issues["del_without_bill"].append(node)

        elif t == "BillingDocumentItem":
            has_child = any(G.nodes[s].get("type") == "JournalEntryItem" for s in G.successors(node))
            if not has_child:
                issues["bill_without_jrn"].append(node)

    return issues


if __name__ == "__main__":
    graph = get_graph()
    print(f"Graph nodes={graph.number_of_nodes()} edges={graph.number_of_edges()}")
    sample = "SOI_740506_10"
    result = trace_full_flow(sample, graph)
    print("Flow trace for", sample, result)
    broken = detect_broken_flows(graph)
    print("Broken flow summary:", {k: len(v) for k, v in broken.items()})
