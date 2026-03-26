
import os
import json
import re

try:
    from groq import Groq
    GROQ_AVAILABLE = True
except ImportError:
    GROQ_AVAILABLE = False
    Groq = None

try:
    import openai
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False

client = None


def get_llm_client():
    groq_key = (os.getenv("GROQ_API_KEY") or "").strip()
    openai_key = (os.getenv("OPENAI_API_KEY") or "").strip()

    if GROQ_AVAILABLE and groq_key:
        try:
            return {"provider": "groq", "client": Groq(api_key=groq_key)}
        except Exception as exc:
            # If Groq fails at init, allow fallback
            print(f"Groq init failed: {exc}")

    if OPENAI_AVAILABLE and openai_key:
        import openai as openai_module
        openai_module.api_key = openai_key
        return {"provider": "openai", "client": openai_module}

    return None
    

SYSTEM_PROMPT = """
You are an expert Order-to-Cash (O2C) data analyst and system planner.

Your job is to:
1. Understand the user's query
2. Generate a structured execution plan
3. Provide a clear business explanation
4. Detect potential issues in the flow

STRICT RULES:
- Return ONLY valid JSON
- No explanation outside JSON
- No markdown
- No extra text
- No comments
- Always follow the schema exactly

OUTPUT SCHEMA:
{
  "type": "graph | sql | summary",
  "query": "trace_flow | unpaid_orders | unbilled_orders | find_journal_entry | unknown",
  "parameters": {
    "id": "<document_id_if_any>"
  },
  "response": "<natural language explanation for user>",
  "insight": {
    "status": "complete | incomplete | issue_detected | unknown",
    "summary": "<short business summary>",
    "issues": ["<list of issues if any>"]
  }
}

BEHAVIOR RULES:

1. TRACE FLOW:
If user asks about tracing, tracking, or following a document:
- type = "graph"
- query = "trace_flow"

2. FIND JOURNAL ENTRY:
If user asks about accounting or journal entry:
- query = "find_journal_entry"

3. UNPAID / UNBILLED:
- unpaid → "unpaid_orders"
- unbilled → "unbilled_orders"

4. RESPONSE QUALITY:
- response must be clear, short, business-friendly
- no technical jargon

5. INSIGHT GENERATION:
- If full flow exists → status = "complete"
- If missing step → status = "incomplete"
- If anomaly → status = "issue_detected"

6. ID EXTRACTION:
- Extract any number from user query as "id"

EXAMPLES:

User: "Trace billing document 91150187"

Output:
{
  "type": "graph",
  "query": "trace_flow",
  "parameters": {"id": "91150187"},
  "response": "Tracing billing document 91150187 across the order-to-cash flow.",
  "insight": {
    "status": "unknown",
    "summary": "Flow trace initiated",
    "issues": []
  }
}

User: "find journal entry for 91150187"

Output:
{
  "type": "graph",
  "query": "find_journal_entry",
  "parameters": {"id": "91150187"},
  "response": "Looking up the journal entry linked to billing document 91150187.",
  "insight": {
    "status": "unknown",
    "summary": "Journal lookup initiated",
    "issues": []
  }
}
"""


def safe_parse_json(text):
    try:
        return json.loads(text)
    except:
        match = re.search(r'\{.*\}', text, re.DOTALL)
        if match:
            try:
                return json.loads(match.group())
            except:
                pass

    raise ValueError(f"Invalid JSON: {text}")


def validate_plan(plan):
    required_keys = {"type", "query", "parameters", "response", "insight"}

    if not isinstance(plan, dict):
        raise ValueError("Plan is not a dict")

    missing = required_keys - set(plan.keys())
    if missing:
        raise ValueError(f"Missing keys in plan: {missing}, plan: {plan}")

    return plan


def fallback_plan(user_query):
    user_query_lower = user_query.lower()
    numbers = re.findall(r"\d+", user_query)
    doc_id = numbers[0] if numbers else None

    if "trace" in user_query_lower or "flow" in user_query_lower:
        return {
            "type": "graph",
            "query": "trace_flow",
            "parameters": {"id": doc_id},
        }
    elif "unpaid" in user_query_lower:
        return {
            "type": "sql",
            "query": "unpaid_orders",
            "parameters": {},
        }
    elif "unbilled" in user_query_lower:
        return {
            "type": "sql",
            "query": "unbilled_orders",
            "parameters": {},
        }

    return {
        "type": "unknown",
        "query": "fallback",
        "parameters": {},
    }


def plan_query(user_query):
    """Use Groq or OpenAI to generate a structured query plan."""
    llm_info = get_llm_client()
    if llm_info is None:
        raise ValueError("No LLM provider configured. Set GROQ_API_KEY or OPENAI_API_KEY.")

    def _extract_text(response):
        if hasattr(response, 'choices'):
            candidates = response.choices
        else:
            candidates = response.get('choices', [])

        if not candidates:
            raise RuntimeError("LLM response missing choices")

        chunk = candidates[0]
        if hasattr(chunk, 'message'):
            return chunk.message.content

        if isinstance(chunk, dict):
            text = chunk.get('message', {}).get('content')
            if text:
                return text
            if 'text' in chunk:
                return chunk['text']

        raise RuntimeError("Unable to extract text from LLM output")

    def _call_groq(groq_client):
        response = groq_client.chat.completions.create(
            model="llama-3.1-8b-instant",
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": user_query}
            ]
        )
        return _extract_text(response)

    def _call_openai(openai_module):
        openai_client = openai_module.OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        response = openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": user_query}
            ],
            max_tokens=512,
            temperature=0.0
        )
        return _extract_text(response)

    provider = llm_info.get("provider")
    client_obj = llm_info.get("client")

    if provider == "groq":
        response_text = _call_groq(client_obj)
    elif provider == "openai":
        response_text = _call_openai(client_obj)
    else:
        raise ValueError("Invalid provider")

    try:
        print(f"[LLM][{provider}] RAW:", response_text)

        print("RAW RESPONSE >>>", response_text)

        plan = safe_parse_json(response_text)
        plan = validate_plan(plan)

        return plan

    except Exception as e:
        print("LLM parsing failed:", response_text)

        return fallback_plan(user_query)


def parse_intent(user_text: str):
    """Minimal keyword fallback - only used if LLM fails."""
    lower = user_text.lower()
    if 'broken' in lower or 'incomplete' in lower or 'orphan' in lower:
        return 'broken_flow_summary'
    if 'trace' in lower and ('billing' in lower or 'order' in lower or 'flow' in lower):
        return 'trace_flow'
    if 'item' in lower and 'order' in lower:
        return 'so_item_detail'
    return 'unsupported'


def plan_and_execute(user_text: str, params: dict):
    plan = plan_query(user_text)
    result_payload = {
        'llm_plan': plan,
        'input_query': user_text,
        'params': params,
        'response': plan.get('response', 'Query processed successfully.'),
        'insight': plan.get('insight', {'status': 'unknown', 'summary': 'Processing completed', 'issues': []})
    }

    if plan.get("type") == "error":
        return {
            **result_payload,
            'intent': 'unsupported',
            'error': 'Unsupported query: this API only supports dataset intent.',
            'explanation': plan.get('reason', 'Could not parse query.')
        }

    query_type = plan.get("query")

    if plan.get("type") == "sql":
        from query_engine import query_sql_direct
        result = query_sql_direct(query_type)
        if result.get('success'):
            return {
                **result_payload,
                'intent': 'sql_query',
                'result': result.get('results'),
                'explanation': plan.get("reason", 'Executed SQL query successfully.')
            }
        else:
            return {
                **result_payload,
                'intent': 'error',
                'error': result.get('error'),
                'explanation': 'SQL query execution failed.'
            }

    if query_type == 'broken_flow_summary':
        from query_engine import query_broken_flow_summary
        return {
            **result_payload,
            'intent': query_type,
            'result': query_broken_flow_summary(),
            'explanation': plan.get("reason", 'Returning counts of broken flow segments.')
        }

    if query_type == 'trace_flow':
        order_id = params.get('order_id') or plan.get('parameters', {}).get('id')
        item_id = params.get('item_id') or '10'  # Default item_id if not provided
        if not order_id:
            return {
                **result_payload,
                'intent': query_type,
                'error': 'order_id required for flow trace'
            }
        from query_engine import query_flow
        result = query_flow(str(order_id), str(item_id))
        return {
            **result_payload,
            'intent': query_type,
            'result': result,
            'explanation': plan.get("reason", 'Tracing item-level O2C flow and reporting missing link points.')
        }

    if query_type == 'so_item_detail':
        order_id = params.get('order_id')
        item_id = params.get('item_id')
        if not order_id or not item_id:
            return {
                **result_payload,
                'intent': query_type,
                'error': 'order_id and item_id required for item lookup'
            }
        from query_engine import query_sales_order_item
        result = query_sales_order_item(order_id, item_id)
        return {
            **result_payload,
            'intent': query_type,
            'result': result,
            'explanation': plan.get("reason", 'Sales order item attributes from graph node metadata.')
        }

    if query_type == 'find_journal_entry':
        journal_id = params.get('order_id') or plan.get('parameters', {}).get('id')
        if not journal_id:
            return {
                **result_payload,
                'intent': query_type,
                'error': 'journal_id required for journal entry lookup'
            }
        # For now, return a placeholder - you can implement the actual journal lookup logic
        return {
            **result_payload,
            'intent': query_type,
            'result': {'journal_entry': f'Journal entry for {journal_id}', 'status': 'not_implemented'},
            'explanation': plan.get("reason", 'Journal entry lookup completed.')
        }

    return {
        **result_payload,
        'intent': 'unsupported',
        'error': 'Unsupported query: this API only supports dataset intent.',
        'explanation': 'Restricting to O2C dataset queries only.'
    }
