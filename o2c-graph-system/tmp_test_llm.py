import os
os.chdir(r'D:\dodge assignment sudarshan\o2c-graph-system')
from llm_interface import validate_plan, fallback_plan, safe_parse_json
print(validate_plan({'type':'graph','query':'trace_flow','parameters':{'id':'123'}}))
print(fallback_plan('trace billing 91150187'))
print(safe_parse_json('some prefix {"type":"graph","query":"trace_flow","parameters":{}} suffix'))
