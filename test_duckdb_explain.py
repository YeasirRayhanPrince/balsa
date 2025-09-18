#!/usr/bin/env python3

import sys
import os
sys.path.append('/ssd_root/yrayhan/balsa/ntp-qo')

from dbs import duckdb

# Test simple query
test_sql = """
SELECT MIN(cn.name) AS movie_company,
       MIN(rt.role) AS character_being_played
FROM char_name AS cn,
     cast_info AS ci,
     company_name AS comp,
     company_type AS ct,
     movie_companies AS mc,
     role_type AS rt,
     title AS t
WHERE ci.note LIKE '%(voice)%'
  AND ci.note LIKE '%(uncredited)%'
  AND cn.id = ci.person_role_id
  AND rt.id = ci.role_id
  AND t.id = ci.movie_id
  AND t.id = mc.movie_id
  AND mc.company_type_id = ct.id
  AND mc.company_id = comp.id
  AND t.production_year BETWEEN 2007 AND 2010;
"""

print("=== Testing DuckDB EXPLAIN ===")
try:
    node, json_data = duckdb.SqlToPlanNode(test_sql, verbose=True, true_card=False)
    print(f"EXPLAIN Node: {node.node_type}, Cost: {node.cost}")
    print(f"JSON keys: {list(json_data.keys()) if isinstance(json_data, dict) else 'Not a dict'}")
except Exception as e:
    print(f"EXPLAIN failed: {e}")

print("\n=== Testing DuckDB EXPLAIN ANALYZE ===")
try:
    node, json_data = duckdb.SqlToPlanNode(test_sql, verbose=True, true_card=True)
    print(f"EXPLAIN ANALYZE Node: {node.node_type}, Cost: {node.cost}")
    if hasattr(node, 'actual_time_ms'):
        print(f"Actual time: {node.actual_time_ms}ms")
    print(f"Node info keys: {list(node.info.keys())}")
    
    # Check for cardinality info
    if 'estimated_rows' in node.info:
        print(f"Estimated rows: {node.info['estimated_rows']}")
    if 'actual_rows' in node.info:
        print(f"Actual rows: {node.info['actual_rows']}")
        
except Exception as e:
    print(f"EXPLAIN ANALYZE failed: {e}")
    import traceback
    traceback.print_exc()