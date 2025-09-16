# Copyright 2022 The Balsa Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""MySQL connector: issues commands and parses results."""
import json
import logging
import os
import re
import subprocess
import tempfile
from typing import Optional, Dict, Any

import pandas as pd
import mysql.connector

from sql_parse import ast as plans_lib

# MySQL Configuration
MYSQL_HOST = os.getenv('MYSQL_HOST', 'localhost')
MYSQL_PORT = int(os.getenv('MYSQL_PORT', '3306'))
MYSQL_DATABASE = os.getenv('MYSQL_DATABASE', 'imdbload')
MYSQL_USER = os.getenv('MYSQL_USER', 'root')
MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD', '')


def GetServerVersion():
    """Get MySQL version."""
    conn = _get_or_create_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT VERSION()")
        result = cursor.fetchone()
        return result[0] if result else "Unknown"
    finally:
        conn.close()


def GetServerConfigs():
    """Returns all live configs as [(param, value, help)]."""
    conn = _get_or_create_connection()
    try:
        cursor = conn.cursor()
        # MySQL doesn't have a direct equivalent to PostgreSQL's "show all"
        # Return basic config info
        cursor.execute("SELECT 'version' as param, VERSION() as value, 'MySQL Version' as help")
        result = cursor.fetchall()
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
        # Note: IMDB tables should already be loaded via load_job_mysql.sh
        
        # Transform SQL for MySQL compatibility
        transformed_sql = _transform_sql_for_mysql(sql)
        
        # MySQL EXPLAIN syntax - use JSON format
        explain_sql = f"EXPLAIN FORMAT=JSON {transformed_sql}"
        if verbose:
            logging.info(f"Executing: {explain_sql}")
        
        try:
            cursor = conn.cursor()
            cursor.execute(explain_sql)
            result = cursor.fetchall()
            
            # MySQL EXPLAIN FORMAT=JSON returns JSON in the first column
            if result and result[0]:
                json_text = result[0][0]
                json_dict = json.loads(json_text)
            else:
                # Fallback if format is unexpected
                json_dict = {"query_block": {"message": "No plan available"}}
            
            if verbose:
                logging.info(f"MySQL JSON plan structure: {json.dumps(json_dict, indent=2)}")
                
        except Exception as e:
            logging.error(f"EXPLAIN failed: {e}, {sql}")
            # Create minimal fallback structure
            json_dict = {
                "query_block": {
                    "message": "Error",
                    "cost_info": {"query_cost": "0.0"},
                    "error": str(e)
                }
            }
        
        # Parse into our Node structure - handle MySQL JSON format
        if 'query_block' in json_dict:
            # MySQL JSON format: has query_block structure
            node = _parse_mysql_json_plan(json_dict['query_block'])
        else:
            # Fallback format
            node = _create_fallback_node(json_dict)
        
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
    
    # Create MySQL connection
    try:
        conn = mysql.connector.connect(
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            database=MYSQL_DATABASE,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD
        )
        
        # Quick validation that IMDB tables exist
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM title LIMIT 1")
        result = cursor.fetchone()
        if not result or result[0] == 0:
            logging.warning("MySQL database exists but title table is empty")
        
        return conn
        
    except mysql.connector.Error as e:
        raise RuntimeError(f"MySQL connection failed: {e}. "
                          f"Please ensure MySQL is running and IMDB data is loaded.")


def _transform_sql_for_mysql(sql):
    """Transform SQL for MySQL compatibility."""
    import re
    
    # Fix reserved keyword issues: quote problematic aliases
    # Common reserved words in MySQL that are used as aliases in JOB queries
    reserved_aliases = ['at', 'to', 'as', 'or', 'and', 'not', 'in', 'on', 'order', 'group']
    
    transformed = sql
    
    # Step 1: Quote table aliases in FROM/JOIN clauses
    for reserved in reserved_aliases:
        # Match pattern like "table_name AS reserved_word" (case insensitive)
        pattern = r'\b(\w+)\s+AS\s+(' + re.escape(reserved) + r')\b'
        replacement = r'\1 AS `\2`'  # MySQL uses backticks
        transformed = re.sub(pattern, replacement, transformed, flags=re.IGNORECASE)
    
    # Step 2: Quote references to these aliases in WHERE/ON clauses
    for reserved in reserved_aliases:
        # Match pattern like "reserved_word.column" and replace with `reserved_word`.column
        pattern = r'\b(' + re.escape(reserved) + r')(\s*\.\s*\w+)'
        replacement = r'`\1`\2'
        transformed = re.sub(pattern, replacement, transformed, flags=re.IGNORECASE)
    
    return transformed


def _parse_mysql_json_plan(query_block):
    """Parse MySQL JSON plan node into our AST Node structure."""
    
    print(query_block)

    # Extract operation type
    node_type = "MySQL Plan"
    if 'table' in query_block:
        node_type = "Table Scan"
    elif 'nested_loop' in query_block:
        node_type = "Nested Loop"
    elif 'grouping_operation' in query_block:
        node_type = "Aggregate"
    elif 'ordering_operation' in query_block:
        node_type = "Sort"
    
    # Create our Node
    node = plans_lib.Node(node_type)
    
    # Extract cost information
    cost_info = query_block.get('cost_info', {})
    query_cost = cost_info.get('query_cost', '1000.0')
    try:
        node.cost = float(query_cost)
    except (ValueError, TypeError):
        node.cost = 1000.0
    
    # Extract table information for scan nodes
    table_info = query_block.get('table', {})
    if table_info:
        node.table_name = table_info.get('table_name', 'unknown')
        # MySQL doesn't preserve aliases the same way, use table name
        node.table_alias = table_info.get('table_name', 'unknown')
    
    # Store additional information
    node.info['mysql_extra'] = query_block
    node.info['database_system'] = 'mysql'
    
    # Add filters if present - ensure it's a string
    if 'attached_condition' in query_block:
        filter_value = query_block['attached_condition']
        node.info['filter'] = str(filter_value) if filter_value is not None else None
    
    # Handle nested operations (joins, subqueries)
    if 'nested_loop' in query_block:
        nested_loop = query_block['nested_loop']
        for child_block in nested_loop:
            child_node = _parse_mysql_json_plan(child_block)
            node.children.append(child_node)
    
    return node


def _create_fallback_node(json_dict):
    """Create a fallback node when MySQL parsing fails."""
    node = plans_lib.Node("MySQL Error")
    node.cost = 0.0
    node.info['database_system'] = 'mysql'
    node.info['error'] = str(json_dict)
    return node


def ExecuteSql(sql, hint=None, verbose=False):
    """Execute SQL and return results."""
    conn = _get_or_create_connection()
    try:
        if hint:
            logging.warning("MySQL does not support PostgreSQL-style hints")
        
        if verbose:
            logging.info(f"Executing: {sql}")
        
        cursor = conn.cursor()
        cursor.execute(sql)
        result = cursor.fetchall()
        return result
        
    finally:
        conn.close()


def GetCostFromMySQL(sql, verbose=False):
    """Get cost estimate from MySQL explain."""
    try:
        node, json_dict = SqlToPlanNode(sql, verbose=verbose)
        return node.cost
    except Exception as e:
        logging.error(f"Error getting cost from MySQL: {e}")
        return None


def GetLatencyFromMySQL(sql, verbose=False):
    """Get actual execution time from MySQL."""
    import time
    
    conn = _get_or_create_connection()
    try:
        if verbose:
            logging.info(f"Measuring execution time for: {sql}")
        
        cursor = conn.cursor()
        start_time = time.time()
        cursor.execute(sql)
        result = cursor.fetchall()
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
        cursor = conn.cursor()
        for table_name in rel_names:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                result = cursor.fetchone()
                table_rows[table_name] = result[0] if result else 0
            except Exception as e:
                logging.warning(f"Could not get row count for table {table_name}: {e}")
                table_rows[table_name] = 0
        
        return table_rows
        
    finally:
        conn.close()