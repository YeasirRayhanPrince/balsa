import re
from dataclasses import dataclass
from typing import List, Optional, Tuple

@dataclass
class QueryPlanNode:
    """Represents a node in a query execution plan tree"""
    operation: str
    cost: float
    table_info: Optional[str] = None
    children: List['QueryPlanNode'] = None
    indent_level: int = 0
    
    def __post_init__(self):
        if self.children is None:
            self.children = []

class QueryPlanParser:
    def __init__(self):
        self.cost_pattern = re.compile(r'cost=([0-9.]+)')
        self.table_pattern = re.compile(r'\[([^\]]+)\]')
    
    def parse_plan_text(self, plan_text: str) -> QueryPlanNode:
        """Parse a query plan from text format into a tree structure"""
        lines = plan_text.strip().split('\n')
        return self._build_tree(lines, 0)[0]
    
    def _build_tree(self, lines: List[str], start_idx: int) -> Tuple[QueryPlanNode, int]:
        """Recursively build tree from lines starting at start_idx"""
        if start_idx >= len(lines):
            return None, start_idx
        
        line = lines[start_idx]
        indent = len(line) - len(line.lstrip())
        
        # Parse the line
        node = self._parse_line(line, indent)
        current_idx = start_idx + 1
        
        # Parse children (lines with greater indentation)
        while current_idx < len(lines):
            next_line = lines[current_idx]
            next_indent = len(next_line) - len(next_line.lstrip())
            
            if next_indent <= indent:
                # Same or less indentation - not a child
                break
            elif next_indent > indent:
                # Greater indentation - this is a child
                child, current_idx = self._build_tree(lines, current_idx)
                if child:
                    node.children.append(child)
            else:
                current_idx += 1
        
        return node, current_idx
    
    def _parse_line(self, line: str, indent: int) -> QueryPlanNode:
        """Parse a single line into a QueryPlanNode"""
        # Extract cost
        cost_match = self.cost_pattern.search(line)
        cost = float(cost_match.group(1)) if cost_match else 0.0
        
        # Extract table info
        table_match = self.table_pattern.search(line)
        table_info = table_match.group(1) if table_match else None
        
        # Extract operation (everything before 'cost=')
        operation = line.strip()
        if cost_match:
            operation = line[:cost_match.start()].strip()
        
        return QueryPlanNode(
            operation=operation,
            cost=cost,
            table_info=table_info,
            indent_level=indent
        )
    
    def print_tree(self, node: QueryPlanNode, prefix: str = "", is_last: bool = True):
        """Print the tree in a nice ASCII format"""
        if node is None:
            return
        
        # Print current node
        connector = "└── " if is_last else "├── "
        cost_str = f"(cost={node.cost})"
        table_str = f" [{node.table_info}]" if node.table_info else ""
        
        print(f"{prefix}{connector}{node.operation}{table_str} {cost_str}")
        
        # Print children
        if node.children:
            for i, child in enumerate(node.children):
                is_last_child = (i == len(node.children) - 1)
                child_prefix = prefix + ("    " if is_last else "│   ")
                self.print_tree(child, child_prefix, is_last_child)
    
    def to_dict(self, node: QueryPlanNode) -> dict:
        """Convert tree to dictionary format for JSON serialization"""
        return {
            "operation": node.operation,
            "cost": node.cost,
            "table_info": node.table_info,
            "children": [self.to_dict(child) for child in node.children]
        }

def demo_parsing():
    """Demo the parser with the example query plan"""
    
    # Example query plan from the file
    plan_text = """FinalizeAggregate cost=343.517
  Gather cost=40870.0
    PartialAggregate cost=39869.7
      Nested Loop cost=39869.67
        Nested Loop cost=39869.09
          Hash Join cost=39847.8
            Nested Loop cost=39846.48
              Nested Loop cost=36361.11
                Hash Join cost=34147.62
                  Seq Scan [movie_companies AS mc] cost=27206.55
                  Hash cost=4722.92
                    Seq Scan [company_name AS cn] cost=4722.92
                Index Scan [title AS t] cost=0.52
              Index Scan [cast_info AS ci] cost=2.01
            Hash cost=1.15
              Seq Scan [role_type AS rt] cost=1.15
          Index Scan [char_name AS chn] cost=5.32
        Index Only Scan [company_type AS ct] cost=0.15"""
    
    parser = QueryPlanParser()
    
    print("=== PARSING QUERY PLAN ===")
    print("Original text:")
    print(plan_text)
    print("\n" + "="*50)
    
    # Parse into tree
    root = parser.parse_plan_text(plan_text)
    
    print("\nParsed tree structure:")
    parser.print_tree(root)
    
    print("\n" + "="*50)
    print("\nTree statistics:")
    print(f"Root operation: {root.operation}")
    print(f"Total cost: {root.cost}")
    print(f"Number of direct children: {len(root.children)}")
    
    # Count all nodes
    def count_nodes(node):
        if not node:
            return 0
        return 1 + sum(count_nodes(child) for child in node.children)
    
    print(f"Total nodes in tree: {count_nodes(root)}")
    
    # Find all table scans
    def find_table_operations(node, ops=None):
        if ops is None:
            ops = []
        if node.table_info:
            ops.append((node.operation, node.table_info, node.cost))
        for child in node.children:
            find_table_operations(child, ops)
        return ops
    
    table_ops = find_table_operations(root)
    print(f"\nTable operations found:")
    for op, table, cost in table_ops:
        print(f"  {op} on {table} (cost={cost})")

if __name__ == "__main__":
    demo_parsing()