"""DuckDB connector: issues commands and parses results."""
import json
import logging
import os
import re
import subprocess
import tempfile
from typing import Optional, Dict, Any

import pandas as pd
import duckdb

from sql_parse import ast as plans_lib

# DuckDB Configuration
# Point to our loaded IMDB database - use absolute path to ensure we find it
DUCKDB_DB_FILE = os.getenv('DUCKDB_DB_FILE', '/ssd_root/yrayhan/balsa/load-duckdb/imdbload.duckdb')
DUCKDB_DATA_DIR = os.getenv('DUCKDB_DATA_DIR', '/tmp/duckdb_data')


def GetServerVersion():
    """Get DuckDB version."""
    conn = _get_or_create_connection()
    try:
        result = conn.execute("SELECT version()").fetchone()
        return result[0] if result else "Unknown"
    finally:
        conn.close()


def GetServerConfigs():
    """Returns all live configs as [(param, value, help)]."""
    conn = _get_or_create_connection()
    try:
        # DuckDB doesn't have a direct equivalent to PostgreSQL's "show all"
        # Return basic config info
        result = conn.execute("SELECT 'version' as param, version() as value, 'DuckDB Version' as help").fetchall()
        return result
    finally:
        conn.close()


def GetServerConfigsAsDf():
    """Returns all live configs as a DataFrame."""
    data = GetServerConfigs()
    return pd.DataFrame(data, columns=['param', 'value', 'help']).drop('help', axis=1)

def SqlToPlanNode(sql,
                  comment=None,
                  verbose=False,
                  keep_scans_joins_only=False,
                  connection=None):
    """Issues EXPLAIN on a SQL string; parse into our AST node."""
    
    # Setup database and connection
    conn = _get_or_create_connection(connection)
    should_close = connection is None
    
    try:
        # Note: IMDB tables should already be loaded via load_job_duckdb.sh
        
        # Transform SQL for DuckDB compatibility
        transformed_sql = _transform_sql_for_duckdb(sql)
        
        # DuckDB EXPLAIN syntax - use JSON format for rich structured data
        explain_sql = f"EXPLAIN (FORMAT JSON) {transformed_sql}"
        if verbose:
            logging.info(f"Executing: {explain_sql}")
        
        try:
            result = conn.execute(explain_sql).fetchall()
            # DuckDB EXPLAIN (FORMAT JSON) returns the JSON in the second column
            if result and len(result[0]) > 1:
                json_text = result[0][1]  # Get the JSON text
                import json
                json_dict = json.loads(json_text)
            else:
                # Fallback if format is unexpected
                explain_text = '\n'.join([str(row[0]) for row in result])
                json_dict = _parse_duckdb_text_explain(explain_text)
            
            if verbose:
                logging.info(f"DuckDB JSON plan structure: {json.dumps(json_dict, indent=2)}")
                
        except Exception as e:
            logging.error(f"EXPLAIN failed: {e}, {sql}")
            # Create minimal fallback structure
            json_dict = {
                "Plan": {
                    "Node Type": "Error",
                    "Total Cost": 0.0,
                    "Rows": 0,
                    "error": str(e)
                }
            }
        
        # Parse into our Node structure - handle both JSON and fallback formats
        if isinstance(json_dict, list) and len(json_dict) > 0:
            # DuckDB JSON format: array of plan nodes
            node = _parse_duckdb_json_plan(json_dict[0])  # Start with root node
        else:
            # Fallback text format
            node = ParseDuckDBPlanJson(json_dict)
        
        if not keep_scans_joins_only:
            return node, json_dict
        return plans_lib.FilterScansOrJoins(node), json_dict
        
    finally:
        if should_close and conn:
            conn.close()


def _get_or_create_connection(connection=None):
    """Get existing connection or create new one with proper database."""
    if connection is not None:
        return connection
    
    # Use file-based database
    db_path = DUCKDB_DB_FILE
    
    # Validate database file exists
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"DuckDB database file not found: {db_path}. "
                               f"Please run load_job_duckdb.sh to create the database.")
    
    conn = duckdb.connect(db_path)
    
    # Quick validation that IMDB tables exist
    try:
        result = conn.execute("SELECT COUNT(*) FROM title LIMIT 1").fetchone()
        if not result or result[0] == 0:
            logging.warning("DuckDB database exists but title table is empty")
    except Exception as e:
        raise RuntimeError(f"DuckDB database exists but IMDB tables not found: {e}. "
                          f"Please run load_job_duckdb.sh to load the data.")
    
    return conn


# Note: IMDB schema creation functions removed since we use pre-loaded database


def _transform_sql_for_duckdb(sql):
    """Transform SQL for DuckDB compatibility."""
    import re
    
    # Fix reserved keyword issues: quote problematic aliases
    # Common reserved words in DuckDB that are used as aliases in JOB queries
    reserved_aliases = ['at', 'to', 'as', 'or', 'and', 'not', 'in', 'on']
    
    transformed = sql
    
    # Step 1: Quote table aliases in FROM/JOIN clauses
    for reserved in reserved_aliases:
        # Match pattern like "table_name AS reserved_word" (case insensitive)
        pattern = r'\b(\w+)\s+AS\s+(' + re.escape(reserved) + r')\b'
        replacement = r'\1 AS "\2"'
        transformed = re.sub(pattern, replacement, transformed, flags=re.IGNORECASE)
    
    # Step 2: Quote references to these aliases in WHERE/ON clauses
    for reserved in reserved_aliases:
        # Match pattern like "reserved_word.column" and replace with "reserved_word".column
        pattern = r'\b(' + re.escape(reserved) + r')(\s*\.\s*\w+)'
        replacement = r'"\1"\2'
        transformed = re.sub(pattern, replacement, transformed, flags=re.IGNORECASE)
    
    return transformed


def _parse_duckdb_json_plan(json_node):
    """Parse DuckDB JSON plan node into our AST Node structure."""
    
    print(json_node)

    node_name = json_node.get('name', 'Unknown')
    extra_info = json_node.get('extra_info', {})
    
    # Create our Node
    node = plans_lib.Node(node_name)
    
    # Extract cost information - use cardinality as cost estimate
    estimated_cardinality = extra_info.get('Estimated Cardinality')
    if estimated_cardinality:
        try:
            cardinality = int(estimated_cardinality)
            # Use log scale to convert cardinality to reasonable cost
            import math
            node.cost = math.log10(max(cardinality, 1)) * 1000.0
        except (ValueError, TypeError):
            node.cost = 1000.0
    else:
        node.cost = 1000.0
    
    # Extract table information for scan nodes
    if 'Table' in extra_info:
        node.table_name = extra_info['Table']
        # DuckDB doesn't use aliases in the same way, use table name
        node.table_alias = extra_info['Table']
    
    # Store additional information
    node.info['duckdb_extra'] = extra_info
    node.info['database_system'] = 'duckdb'
    
    # Add filters if present - ensure it's a string
    if 'Filters' in extra_info:
        filter_value = extra_info['Filters']
        # Ensure filter is always a string for compatibility with regex parsing
        node.info['filter'] = str(filter_value) if filter_value is not None else None
    
    # Add join information
    if 'Join Type' in extra_info:
        node.info['join_type'] = extra_info['Join Type']
    if 'Conditions' in extra_info:
        node.info['join_conditions'] = extra_info['Conditions']
    
    # Recursively parse children
    children = json_node.get('children', [])
    for child_json in children:
        child_node = _parse_duckdb_json_plan(child_json)
        node.children.append(child_node)
    
    return node


def _parse_duckdb_text_explain(explain_text):
    """Parse DuckDB text EXPLAIN output into JSON-like structure."""
    import re
    lines = explain_text.split('\n')
    
    # Extract basic info from DuckDB explain format
    total_cost = 1000.0  # Default cost
    estimated_rows = 1000  # Default rows
    node_type = "DuckDB Plan"
    table_names = []
    
    # Parse DuckDB's visual explain format
    for line in lines:
        original_line = line
        line = line.strip()
        
        # Remove box-drawing characters and clean up
        clean_line = re.sub(r'[│└┬┘┌┐├─┤┴┼]', '', line).strip()
        
        # Look for operation types (appear in box headers)
        # Prioritize main operations over filters/details
        main_operations = ['HASH_JOIN', 'NESTED_LOOP', 'UNGROUPED_AGGREGATE', 'AGGREGATE', 'SORT']
        scan_operations = ['SEQ_SCAN', 'INDEX_SCAN']
        detail_operations = ['FILTER', 'Filters:']
        
        # Check main operations first (highest priority)
        for op in main_operations:
            if op in clean_line.upper() and len(clean_line.strip()) < 50:
                node_type = clean_line
                break
        else:
            # Then check scan operations
            for op in scan_operations:
                if op in clean_line.upper() and len(clean_line.strip()) < 50:
                    node_type = clean_line
                    break
            else:
                # Finally check detail operations (only if no better match found)
                if node_type == "DuckDB Plan":
                    for op in detail_operations:
                        if op in clean_line and len(clean_line.strip()) < 50:
                            node_type = clean_line
                            break
        
        # Look for row estimates (format: ~X,XXX,XXX rows or ~X Rows)
        row_patterns = [
            r'~([\d,]+)\s+[Rr]ows',  # ~505,662 rows
            r'~([\d,]+)\s+[Rr]ows',  # ~505662 Rows
        ]
        for pattern in row_patterns:
            row_match = re.search(pattern, line)
            if row_match:
                try:
                    estimated_rows = int(row_match.group(1).replace(',', ''))
                    # Use row count as a cost estimate (log scale to avoid huge numbers)
                    import math
                    total_cost = math.log10(max(estimated_rows, 1)) * 1000.0
                    break
                except:
                    pass
        
        # Look for table names (format: Table: table_name)
        table_match = re.search(r'Table:\s+(\w+)', line)
        if table_match:
            table_names.append(table_match.group(1))
    
    # Use the first operation found, or a descriptive name
    if node_type == "DuckDB Plan" and table_names:
        if len(table_names) == 1:
            node_type = f"Scan {table_names[0]}"
        else:
            node_type = f"Join ({', '.join(table_names[:3])})"
    
    return {
        "Plan": {
            "Node Type": node_type,
            "Total Cost": total_cost,
            "Rows": estimated_rows,
            "Tables": table_names,
            "explain_text": explain_text
        }
    }


def ExecuteSql(sql, hint=None, verbose=False):
    """Execute SQL and return results."""
    conn = _get_or_create_connection()
    try:
        if hint:
            logging.warning("DuckDB does not support PostgreSQL-style hints")
        
        if verbose:
            logging.info(f"Executing: {sql}")
        
        result = conn.execute(sql).fetchall()
        return result
        
    finally:
        conn.close()


def ParseDuckDBPlanJson(json_dict):
    """Parse DuckDB explain output into our AST Node structure."""
    # This is a simplified implementation
    # In practice, you'd need to parse DuckDB's actual explain format
    
    plan = json_dict.get("Plan", {})
    node_type = plan.get("Node Type", "Unknown")
    cost = plan.get("Total Cost", 0.0)
    
    # Create a basic node
    node = plans_lib.Node(node_type, cost=cost)
    
    # Add explain info
    node.info['explain_json'] = json_dict
    node.info['database_system'] = 'duckdb'
    
    if 'explain_text' in plan:
        node.info['explain_text'] = plan['explain_text']
    
    return node


def GetCostFromDuckDB(sql, verbose=False):
    """Get cost estimate from DuckDB explain."""
    try:
        node, json_dict = SqlToPlanNode(sql, verbose=verbose)
        return node.cost
    except Exception as e:
        logging.error(f"Error getting cost from DuckDB: {e}")
        return None


def GetLatencyFromDuckDB(sql, verbose=False):
    """Get actual execution time from DuckDB."""
    import time
    
    conn = _get_or_create_connection()
    try:
        if verbose:
            logging.info(f"Measuring execution time for: {sql}")
        
        start_time = time.time()
        result = conn.execute(sql).fetchall()
        end_time = time.time()
        
        latency_ms = (end_time - start_time) * 1000
        return latency_ms
        
    finally:
        conn.close()


def GetAllTableNumRows(rel_names):
    """Get row counts for all specified tables."""
    conn = _get_or_create_connection()
    try:
        table_rows = {}
        for table_name in rel_names:
            try:
                result = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
                table_rows[table_name] = result[0] if result else 0
            except Exception as e:
                logging.warning(f"Could not get row count for table {table_name}: {e}")
                table_rows[table_name] = 0
        
        return table_rows
        
    finally:
        conn.close()