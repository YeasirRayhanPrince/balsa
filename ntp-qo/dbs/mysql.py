"""MySQL connector: issues commands and parses results."""
import json
import logging
import os
import pandas as pd
import mysql.connector # pyright: ignore[reportMissingImports]
from sql_parse import ast as plans_lib

# MySQL Configuration
MYSQL_HOST = os.getenv('MYSQL_HOST', 'localhost')
MYSQL_PORT = int(os.getenv('MYSQL_PORT', '3307'))
MYSQL_DATABASE = os.getenv('MYSQL_DATABASE', 'imdbload')
MYSQL_USER = os.getenv('MYSQL_USER', 'root')
MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD', None)


def GetServerVersion():
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
                  connection=None,
                  true_card=None):
    """Issues EXPLAIN(format json) on a SQL string; parse into our AST node."""
    
    # Setup database and connection
    conn = _get_or_create_connection(connection)
    should_close = connection is None
    
    try:
        transformed_sql = _transform_sql_for_mysql(sql)
        
        if true_card:
            explain_sql = f"EXPLAIN ANALYZE {transformed_sql}"
        else:
            
            explain_sql = f"EXPLAIN FORMAT=TREE {transformed_sql}"
        
        if verbose:
            logging.info(f"Executing: {explain_sql}")

        try:
            cursor = conn.cursor()
            cursor.execute(explain_sql)
            result = cursor.fetchall()
            
            # MySQL EXPLAIN ANALYZE returns text rows
            if result:
                # Combine all rows and split on newlines to get individual plan lines
                raw_text = '\n'.join([row[0] for row in result if row and row[0]])
                text_lines = raw_text.split('\n')
            else:
                text_lines = ["No plan available"]
            
            if verbose:
                logging.info(f"MySQL EXPLAIN ANALYZE output:\n" + "\n".join(text_lines))
                
        except Exception as e:
            logging.error(f"EXPLAIN ANALYZE failed: {e}, {sql}")
            # Create minimal fallback
            text_lines = [f"Error: {str(e)}"]
        
        # Parse the text-based tree format
        # print(text_lines)
        node = _parse_mysql_text_plan(text_lines)
        json_dict = {"mysql_text_plan": text_lines}
        print(node.print_tree())
        # print(json_dict)
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
        connection_params = {
            'host': MYSQL_HOST,
            'port': MYSQL_PORT,
            'database': MYSQL_DATABASE,
            'user': MYSQL_USER
        }
        if MYSQL_PASSWORD:
            connection_params['password'] = MYSQL_PASSWORD
        
        conn = mysql.connector.connect(**connection_params)
        
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
    reserved_aliases = ['at', 'to', 'as', 'or', 'and', 'not', 'in', 'on', 'order', 'group', 'character']
    
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
    
    # Step 3: Quote column aliases in SELECT clauses
    for reserved in reserved_aliases:
        # Match pattern like "expression AS reserved_word" and replace with "expression AS `reserved_word`"
        pattern = r'\bAS\s+(' + re.escape(reserved) + r')\b'
        replacement = r'AS `\1`'
        transformed = re.sub(pattern, replacement, transformed, flags=re.IGNORECASE)
    
    return transformed


def _parse_mysql_text_plan(text_lines):
    """Parse MySQL EXPLAIN ANALYZE text format into our AST Node structure."""
    import re
    
    if not text_lines:
        return _create_fallback_node({})
    
    # Debug: print lines to understand format
    # print("=== PARSING MYSQL LINES ===")
    # for i, line in enumerate(text_lines):
    #     print(f"Line {i}: '{line}'")
    # print("=== END LINES ===")
    
    # Build tree structure from indented text
    nodes_stack = []
    root_node = None
    
    for line_num, line in enumerate(text_lines):
        if not line or not line.strip():
            continue
            
        # Skip lines that don't start with -> (table borders, etc.)
        if '->' not in line:
            continue
            
        # Calculate indentation level by counting spaces before ->
        arrow_pos = line.find('->')
        if arrow_pos == -1:
            continue
            
        indent_level = arrow_pos // 4  # Assuming 4-space indents
        
        # print(f"Line {line_num}: indent_level={indent_level}, line='{line.strip()}'")
        
        # Parse the operation line
        node = _parse_mysql_operation_line(line)
        # print(f"  -> Created node: {node.node_type}, cost={node.cost}")
        
        # Handle tree structure
        if root_node is None:
            # First node becomes root
            root_node = node
            nodes_stack = [node]
        else:
            # Adjust stack to current indentation level
            # Pop nodes until stack matches current depth
            while len(nodes_stack) > indent_level + 1:
                nodes_stack.pop()
            
            # Add as child of current parent
            if nodes_stack:
                parent = nodes_stack[-1]
                parent.children.append(node)
                # print(f"  -> Added as child of: {parent.node_type}")
            
            # Push current node to stack for potential children
            nodes_stack.append(node)
    
    # print(f"=== FINAL ROOT: {root_node.node_type if root_node else 'None'} ===")
    return root_node if root_node else _create_fallback_node({})


def _parse_mysql_operation_line(line):
    """Parse a single MySQL operation line into a Node."""
    import re
    
    # Extract operation name and parameters
    # Pattern: -> Operation_Name (cost=X rows=Y) [optional actual stats]
    operation_match = re.match(r'\s*->\s*([^(]+)', line)
    operation_name = operation_match.group(1).strip() if operation_match else "Unknown"
    
    # Extract cost and rows estimates
    cost_match = re.search(r'cost=([0-9.e+\-]+)', line)
    rows_match = re.search(r'rows=([0-9.e+\-]+)', line)
    
    # Extract actual statistics if present
    actual_time_match = re.search(r'actual time=([0-9.]+)\.\.([0-9.]+)', line)
    actual_rows_match = re.search(r'actual.*rows=([0-9]+)', line)
    
    # Map MySQL operation names to our standard names
    node_type = _map_mysql_operation_to_standard(operation_name)
    
    # Create node
    node = plans_lib.Node(node_type)
    
    # Set cost
    if cost_match:
        try:
            node.cost = float(cost_match.group(1))
        except ValueError:
            node.cost = 1000.0
    else:
        node.cost = 1000.0
    
    # Set estimated rows if available (consistent with PostgreSQL format)
    if rows_match:
        try:
            node.info['estimated_rows'] = float(rows_match.group(1))
        except ValueError:
            pass
    
    # Set actual time if available
    if actual_time_match:
        try:
            actual_time = float(actual_time_match.group(2))  # End time
            node.actual_time_ms = actual_time
        except ValueError:
            pass
    
    # Set actual rows if available (consistent with PostgreSQL format)
    if actual_rows_match:
        try:
            node.info['actual_rows'] = int(actual_rows_match.group(1))
        except ValueError:
            pass
    
    # Extract table name for scan operations
    table_match = re.search(r'Table scan on (\w+)', operation_name)
    if table_match:
        node.table_name = table_match.group(1)
        node.table_alias = table_match.group(1)
    
    # Extract index information
    index_match = re.search(r'Index lookup on (\w+) using (\w+)', operation_name)
    if index_match:
        node.table_name = index_match.group(1)
        node.table_alias = index_match.group(1)
    
    # Store MySQL-specific info
    node.info['engine'] = 'mysql'
    node.info['mysql_operation'] = operation_name
    node.info['mysql_line'] = line.strip()
    
    # Extract filter conditions
    filter_match = re.search(r'Filter: (.+?)(?:\s+\(cost=|$)', line)
    if filter_match:
        node.info['filter'] = filter_match.group(1)
    
    return node


def _map_mysql_operation_to_standard(operation_name):
    """Map MySQL operation names to standard plan node types."""
    operation_lower = operation_name.lower()
    
    if 'table scan' in operation_lower:
        return 'Seq Scan'
    elif 'index lookup' in operation_lower or 'index scan' in operation_lower:
        return 'Index Scan'
    elif 'nested loop' in operation_lower:
        return 'Nested Loop'
    elif 'hash join' in operation_lower:
        return 'Hash Join'
    elif 'inner hash join' in operation_lower:
        return 'Hash Join'
    elif 'aggregate' in operation_lower:
        return 'Aggregate'
    elif 'sort' in operation_lower or 'ordering' in operation_lower:
        return 'Sort'
    elif 'filter' in operation_lower:
        return 'Filter'
    elif 'hash' in operation_lower and 'join' not in operation_lower:
        return 'Hash'
    else:
        return operation_name.strip()


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
        return cursor.fetchall()
        
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
        cursor.fetchall()  # Execute the query
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