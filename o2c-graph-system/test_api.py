"""
Simple API endpoint tests for O2C graph query system.
Run with: python test_api.py
"""
import requests
import json
import time
import sys

BASE_URL = "http://127.0.0.1:8002"

def test_health():
    """Test /healthz endpoint."""
    print("\n[TEST 1] GET /healthz")
    try:
        r = requests.get(f"{BASE_URL}/healthz", timeout=5)
        print(f"Status: {r.status_code}")
        print(f"Body: {r.json()}")
        assert r.status_code == 200
        print("✓ PASS")
        return True
    except Exception as e:
        print(f"✗ FAIL: {e}")
        return False

def test_broken_flows():
    """Test /broken-flows endpoint."""
    print("\n[TEST 2] GET /broken-flows")
    try:
        r = requests.get(f"{BASE_URL}/broken-flows", timeout=10)
        print(f"Status: {r.status_code}")
        body = r.json()
        print(f"Body: {body}")
        assert r.status_code == 200
        assert "soi_without_del" in body
        assert "del_without_bill" in body
        assert "bill_without_jrn" in body
        print("✓ PASS")
        return True
    except Exception as e:
        print(f"✗ FAIL: {e}")
        return False

def test_trace_flow():
    """Test POST /trace-flow endpoint."""
    print("\n[TEST 3] POST /trace-flow")
    try:
        payload = {"order_id": "740506", "item_id": "10"}
        r = requests.post(f"{BASE_URL}/trace-flow", json=payload, timeout=10)
        print(f"Status: {r.status_code}")
        body = r.json()
        print(f"Body: {json.dumps(body, indent=2)}")
        assert r.status_code == 200
        assert "result" in body
        result = body["result"]
        # Check for error case when item doesn't exist
        if "error" in result:
            assert result["error"] == "SalesOrderItem not found"
            assert result["complete"] == False
        else:
            assert "flow_path" in result
            assert "missing_links" in result
            assert "metadata" in result
            assert "complete" in result
            assert result["complete"] == False  # Expected broken flow
        print("✓ PASS")
        return True
    except Exception as e:
        print(f"✗ FAIL: {e}")
        return False

def test_llm_query_broken_flow():
    """Test POST /llm-query with broken flow intent."""
    print("\n[TEST 4] POST /llm-query (broken flow detection)")
    try:
        payload = {"query": "show me broken flows"}
        r = requests.post(f"{BASE_URL}/llm-query", json=payload, timeout=10)
        print(f"Status: {r.status_code}")
        body = r.json()
        print(f"Body: {json.dumps(body, indent=2)}")
        assert r.status_code == 200
        assert "intent" in body
        assert body["intent"] == "broken_flow_summary"
        assert "result" in body
        print("✓ PASS")
        return True
    except Exception as e:
        print(f"✗ FAIL: {e}")
        return False

def test_llm_query_trace_flow():
    """Test POST /llm-query with trace intent."""
    print("\n[TEST 5] POST /llm-query (trace flow)")
    try:
        payload = {
            "query": "trace the flow for this order",
            "order_id": "740506",
            "item_id": "10"
        }
        r = requests.post(f"{BASE_URL}/llm-query", json=payload, timeout=10)
        print(f"Status: {r.status_code}")
        body = r.json()
        print(f"Body: {json.dumps(body, indent=2)}")
        assert r.status_code == 200
        assert "intent" in body
        assert body["intent"] == "trace_flow"
        assert "result" in body
        print("✓ PASS")
        return True
    except Exception as e:
        print(f"✗ FAIL: {e}")
        return False

def test_llm_query_unsupported():
    """Test POST /llm-query with unsupported intent."""
    print("\n[TEST 6] POST /llm-query (unsupported query)")
    try:
        payload = {"query": "what is the meaning of life"}
        r = requests.post(f"{BASE_URL}/llm-query", json=payload, timeout=10)
        print(f"Status: {r.status_code}")
        body = r.json()
        print(f"Body: {json.dumps(body, indent=2)}")
        assert r.status_code == 400
        assert "detail" in body
        assert "Unsupported" in body["detail"]
        print("✓ PASS (correctly rejected)")
        return True
    except Exception as e:
        print(f"✗ FAIL: {e}")
        return False


def test_demo_trace_billing():
    """Test GET /demo/trace-billing/{billing_id}"""
    print("\n[TEST 7] GET /demo/trace-billing/91150187")
    try:
        r = requests.get(f"{BASE_URL}/demo/trace-billing/91150187", timeout=10)
        print(f"Status: {r.status_code}")
        body = r.json()
        print(f"Body: {json.dumps(body, indent=2)}")
        assert r.status_code == 200
        if "error" in body:
            assert "not found" in body["error"].lower()
            print("✓ PASS (billing id not found, can be verified with dataset load)")
            return True

        assert "trace" in body
        assert "path" in body
        assert "journal_entries" in body
        assert isinstance(body["journal_entries"], list)
        print("✓ PASS")
        return True
    except Exception as e:
        print(f"✗ FAIL: {e}")
        return False


def test_demo_top_products():
    """Test GET /demo/top-products?limit=5"""
    print("\n[TEST 8] GET /demo/top-products?limit=5")
    try:
        r = requests.get(f"{BASE_URL}/demo/top-products?limit=5", timeout=10)
        print(f"Status: {r.status_code}")
        body = r.json()
        print(f"Body: {json.dumps(body, indent=2)}")
        assert r.status_code == 200
        assert type(body) is list
        assert all("product_id" in row and "invoice_count" in row for row in body)
        print("✓ PASS")
        return True
    except Exception as e:
        print(f"✗ FAIL: {e}")
        return False


def test_demo_delivered_not_billed():
    """Test GET /demo/delivered-not-billed"""
    print("\n[TEST 9] GET /demo/delivered-not-billed")
    try:
        r = requests.get(f"{BASE_URL}/demo/delivered-not-billed", timeout=10)
        print(f"Status: {r.status_code}")
        body = r.json()
        print(f"Body: {json.dumps(body, indent=2)}")
        assert r.status_code == 200
        assert type(body) is list
        print("✓ PASS")
        return True
    except Exception as e:
        print(f"✗ FAIL: {e}")
        return False


if __name__ == "__main__":
    print("=" * 60)
    print("O2C Graph Query API - Endpoint Tests")
    print("=" * 60)
    print(f"Backend: {BASE_URL}\n")
    
    # Wait 2 seconds for server startup
    print("Waiting for server startup...")
    time.sleep(2)
    
    results = []
    results.append(("health", test_health()))
    results.append(("broken_flows", test_broken_flows()))
    results.append(("trace_flow", test_trace_flow()))
    results.append(("llm_query_broken", test_llm_query_broken_flow()))
    results.append(("llm_query_trace", test_llm_query_trace_flow()))
    results.append(("llm_query_unsupported", test_llm_query_unsupported()))
    results.append(("demo_trace_billing", test_demo_trace_billing()))
    results.append(("demo_top_products", test_demo_top_products()))
    results.append(("demo_delivered_not_billed", test_demo_delivered_not_billed()))
    
    print("\n" + "=" * 60)
    print("Test Summary")
    print("=" * 60)
    passed = sum(1 for _, r in results if r)
    total = len(results)
    print(f"Passed: {passed}/{total}")
    for name, result in results:
        status = "✓" if result else "✗"
        print(f"  {status} {name}")
    
    sys.exit(0 if passed == total else 1)
