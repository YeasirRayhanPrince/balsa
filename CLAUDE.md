# Environment Setup

To work with this project, activate the conda environment:

```bash
eval "$(conda shell.bash hook)" && conda activate balsa
```

# Data Formats in Balsa

This section documents the key data structures and file formats used throughout Balsa.

## 1. Node Objects (Query Plan Trees)

**Location**: `balsa/util/plans_lib.py:26`
**Purpose**: Represents query execution plans as tree structures

```python
class Node(object):
    def __init__(self, node_type, table_name=None, cost=None):
        self.node_type = node_type           # e.g., 'Hash Join', 'Seq Scan'
        self.cost = cost                     # PostgreSQL estimated cost
        self.actual_time_ms = None           # Actual execution time (if measured)
        self.info = {}                       # Extended metadata
        self.children = []                   # Child nodes for joins
        self.table_name = table_name         # For scan nodes only
        self.table_alias = None              # Table alias if present

# Example Node structure:
node = Node('Hash Join', cost=23968.1)
node.info = {
    'query_name': '1a.sql',
    'sql_str': 'SELECT * FROM ...',
    'explain_json': {...},                   # PostgreSQL EXPLAIN output
    'all_filters': {                         # Filter conditions
        ('title AS t', "(t.production_year > 2000)"): ...,
    },
    'all_filters_est_rows': {                # Cardinality estimates
        ('title AS t', "(t.production_year > 2000)"): 1386738,
    }
}
```

## 2. Simulation Data (Training Data)

**Location**: `sim.py:simulation_data`
**Purpose**: Training examples for reinforcement learning

```python
# Format: List of SubplanGoalCost objects
simulation_data = [
    SubplanGoalCost(
        subplan='/*+ Leading((cn (mc (t kt)))) */',     # PostgreSQL hint string
        goal='cn,kt,mc,mi,t',                           # Comma-separated table list
        cost=17530220                                   # Total execution cost
    ),
    SubplanGoalCost(
        subplan='/*+ Leading((mc (kt (t (miidx it))))) */',
        goal='cn,ct,it,kt,mc,mi,miidx,t',
        cost=21501465
    ),
    # ... more training examples
]

# Meaning:
# - subplan: Current partial join order (state)  
# - goal: Complete set of tables to join (target)
# - cost: Cost of completing the full query from this state (reward)
```

## 3. Initial Policy Data (Expert Experience)

**Location**: `data/initial_policy_data.pkl`
**Purpose**: Pre-computed expert query plans from PostgreSQL optimizer

```python
# Format: Pickled JoinOrderBenchmark workload object
workload = JoinOrderBenchmark(...)

# Contains:
workload.queries = [
    Node(...),  # Query 1 plan tree with expert execution data
    Node(...),  # Query 2 plan tree with expert execution data  
    # ... 113 JOB queries total
]

# Each Node contains expert optimization results:
node.cost = expert_latency                    # Actual execution time from expert
node.actual_time_ms = measured_execution      # Real measurements
node.info['query_name'] = '1a.sql'           # Query filename
node.info['sql_str'] = 'SELECT ...'          # Original SQL
```

## 4. Featurized Training Data (Neural Network Input)

**Location**: `data/sim-featurized-{hash}.pkl`
**Purpose**: Neural network training tensors

### **Data Format**
```python
# Format: Tuple of tensors
data = (
    all_query_vecs,    # Query features: [batch_size, 40]
    all_feat_vecs,     # Plan features: [batch_size, 123, 22] (TreeConv)  
    all_pos_vecs,      # Positional features: [batch_size, 123, 22]
    all_costs          # Target costs: [batch_size]
)
```

### **4.1 Query Feature Vectors (`all_query_vecs`)**

**Dimensions**: `(batch_size, 40)` where 40 = number of `rel_ids` in workload
**Featurizer**: `SimQueryFeaturizer` (`sim.py:126`)
**Purpose**: Encodes which tables are in the query and their filter selectivities

```python
# Query vector encoding per element:
query_vec[i] = {
    0.0:           # Table rel_ids[i] not in this query
    1.0:           # Table rel_ids[i] joined, no filter applied
    0.0-1.0:       # Table rel_ids[i] joined + filtered (selectivity fraction)
}

# Real examples from JOB workload:
# Position 9:  0.359966 = table filtered, ~36% rows remain
# Position 23: 1.0      = table joined, unfiltered  
# Position 38: 0.692045 = table filtered, ~69% rows remain
```

**Key Properties**:
- **Same query, different subplans** → **Identical query vectors**
- **Different queries** → **Different query vectors**
- **Selectivity calculation**: `estimated_rows / total_table_rows`

### **4.2 Plan Feature Vectors (`all_feat_vecs`)**

**Dimensions**: `(batch_size, 123, 22)` for TreeConv models
**Featurizer**: `TreeNodeFeaturizer` (`plans_lib.py`)
**Purpose**: Encodes join tree structure as tree-structured features

#### **TreeConv Format Structure**
```python
# TreeConv format:
# - 123 rows: Maximum tree nodes (zero-padded)
# - 22 cols:  Feature dimensions per tree node  
# - Most rows: All zeros (padding)
# - Few rows:  Non-zero (actual tree nodes)

# Example tree structure:
feat_vecs[0] = [
    [0. 1. 0. 0. 0. ...],  # Row 0:  Root join node
    [0. 0. 0. 0. 0. ...],  # Row 1:  Padding (unused)
    # ... 120 more padding rows ...
    [0. 1. 0. 1. 0. ...],  # Row 70: Left subtree join  
    # ... more padding ...
    [0. 1. 1. 0. 0. ...],  # Row 105: Right subtree join
]
```

#### **The 22 Feature Dimensions Explained**

**TreeNodeFeaturizer format**: `[operator_one_hot] + [relation_multi_hot]`

```python
# Feature calculation: |all_ops| + |rel_ids|
# For logical plans (plan_physical=False):
all_ops = ['Join', 'Scan']           # 2 operators
rel_ids = [subset of relation IDs]   # ~20 relation IDs used in training

# Total: 2 + 20 = 22 features per node

# Feature vector breakdown:
node_features = [
    # Positions 0-1: One-hot operator encoding
    0.0,    # 1.0 if this is a Join node, 0.0 otherwise
    1.0,    # 1.0 if this is a Scan node, 0.0 otherwise
    
    # Positions 2-21: Multi-hot relation encoding  
    0.0,    # 1.0 if relation_0 is under this subtree
    1.0,    # 1.0 if relation_1 is under this subtree
    0.0,    # 1.0 if relation_2 is under this subtree
    1.0,    # 1.0 if relation_3 is under this subtree
    # ... 16 more relation indicators
]
```

#### **Plan vs Position Vectors**

**Plan Vectors** (`all_feat_vecs`): **WHAT** each node represents
- **Content**: Which operator type, which tables under this subtree
- **Purpose**: Semantic information about the node

**Position Vectors** (`all_pos_vecs`): **WHERE** each node is in the tree  
- **Content**: Parent-child relationships, tree structure indices
- **Purpose**: Structural information for TreeConv neural network

```python
# Same tree, different representations:

# Plan vector for a Hash Join of tables A,B:
plan_vec = [0, 1, 0, 1, 1, 0, 0, ...]  # Join=1, A=1, B=1, others=0

# Position vector for same node:
pos_vec = [0, 2, 3, 1, 0, 0, 0, ...]   # parent=0, left=2, right=3, depth=1
```

**Key Properties**:
- **Different queries** → **Different active row positions**
- **Same query, different join orders** → **Same positions, different feature patterns**

## TreeConv vs Non-TreeConv Feature Vector Differences

### With TreeConv (`tree_conv=True`, `TreeNodeFeaturizer`)

**Format**: 3D tree structure `(batch_size, max_nodes, features_per_node)`
- **all_feat_vecs**: `(batch_size, 123, 22)` - Tree nodes with 22 features each
- **all_pos_vecs**: `(batch_size, 123, 22)` - Tree structure indices
- **Neural Network**: TreeConvolution processes tree structure

```python
# TreeConv plan vector example:
feat_vecs[0] = [
    [0. 1. 0. 0. 0. ...],  # Row 0:  Root join node
    [0. 0. 0. 0. 0. ...],  # Row 1:  Padding (unused)
    # ... 120 more padding rows ...
    [0. 1. 0. 1. 0. ...],  # Row 70: Left subtree join  
    [1. 0. 1. 0. 0. ...],  # Row 105: Right subtree scan
]
# Shape: (123, 22) where 22 = 2 operators + 20 relations
```

### Without TreeConv (`tree_conv=False`, `PreOrderSequenceFeaturizer`)

**Format**: 1D sequence `(batch_size, sequence_length)`
- **all_feat_vecs**: `(batch_size, variable_length)` - Pre-order traversal sequence
- **all_pos_vecs**: `(batch_size, variable_length)` - Parent position indices  
- **Neural Network**: Standard CNN/RNN processes sequence

```python
# Example tree:
#     Hash Join (root)
#    /            \
# Seq Scan      Seq Scan  
# (title)       (cast_info)

# Non-TreeConv plan vector:
feat_vecs[0] = [0, 1, 15, 1, 12]  # Pre-order traversal tokens

# Algorithm: _pre_order(node, vecs):
#   1. Add operator type index: vecs.append(vocab_index(node.node_type))
#   2. If leaf node: Add table name index: vecs.append(vocab_index(node.table_name))
#   3. If internal node: Recurse to children

# Breakdown:
# vocab[0] = 'Hash Join'    # Root: operator type
# vocab[1] = 'Seq Scan'     # Left: operator type  
# vocab[15] = 'title'       # Left: table name (leaf)
# vocab[1] = 'Seq Scan'     # Right: operator type
# vocab[12] = 'cast_info'   # Right: table name (leaf)

# Vocabulary: vocab = all_ops + rel_names
# E.g., ['Hash Join', 'Seq Scan', 'Nested Loop', 'title', 'cast_info', ...]
```

**Key Differences**:

| Aspect | TreeConv | Non-TreeConv |
|--------|----------|--------------|
| **Structure** | 2D matrix (nodes × features) | 1D sequence (tokens) |
| **Encoding** | Multi-hot per node | Vocab indices in pre-order |
| **Tree Info** | Explicit in structure | Implicit in traversal order |
| **Padding** | Fixed max nodes (123) | Variable length |
| **Features** | 22 per node | Op type + table name (leaves) |
| **Example** | `[0,1,0,1,1,...]` per node | `[0,1,15,1,12]` sequence |

**Code Location**: The switch happens in `balsa/experience.py:713-718`:

```python
if isinstance(self.featurizer, plans_lib.TreeNodeFeaturizer):
    # TreeConv: batch featurization with tree structure  
    all_feat_vecs, all_pa_pos_vecs = TreeConvFeaturize(self.featurizer, subplans)
else:
    # Non-TreeConv: individual sequence featurization using PreOrderSequenceFeaturizer
    for i, node in enumerate(self.nodes):
        all_feat_vecs[i] = self.featurizer(subplans[i])  # Returns vocab index sequence
```

**Core Algorithm Difference**:
- **TreeConv**: Multi-hot encoding preserving tree structure explicitly  
- **Non-TreeConv**: Pre-order traversal flattening tree into `[op_type, table_name, op_type, table_name, ...]` sequence
- **TreeConv networks** use both plan + position vectors to understand tree semantics and structure

### **4.3 Position Vectors (`all_pos_vecs`)**

**Dimensions**: `(batch_size, 123, 22)` - Same as plan features
**Purpose**: TreeConv positional encoding for parent-child relationships
**Usage**: Used by TreeConv neural networks to understand tree structure

### **4.4 Target Costs (`all_costs`)**

**Dimensions**: `(batch_size,)`
**Purpose**: Ground truth labels for supervised learning
**Values**: PostgreSQL cost estimates or actual execution times

```python
# Cost examples:
all_costs = [
    6164525,    # High cost (complex query/bad join order)
    3632919,    # Lower cost (simpler query/better join order)  
    # ...
]
```

### **4.5 Neural Network Training Process**

```python
# Training example:
query_features = [0, 0, 0.36, 0, 1.0, 0, ...]  # Which tables + selectivities
plan_features = [[0,1,0,...], [0,0,0,...], ...] # Tree structure (123x22)
target_cost = 3632919                            # PostgreSQL cost

# Network learns: f(query_features, plan_features) → predicted_cost
# Goal: predicted_cost ≈ target_cost
```

This allows Balsa to:
1. **Input**: Query + candidate join order  
2. **Output**: Predicted execution cost
3. **Optimization**: Pick join order with lowest predicted cost

## 5. Workload Objects

**Location**: `balsa/envs/envs.py:159`
**Purpose**: Manages collections of queries for training/testing

```python
class JoinOrderBenchmark(Workload):
    def __init__(self, params):
        self.query_nodes = [...]     # All query plan trees
        self.train_nodes = [...]     # Training subset  
        self.test_nodes = [...]      # Testing subset
        self.workload_info = WorkloadInfo(...)  # Metadata
        
# WorkloadInfo contains:
workload_info.rel_names = ['title', 'cast_info', ...]      # Table names
workload_info.rel_ids = ['title AS t', 'cast_info AS ci', ...] # With aliases
workload_info.scan_types = ['Seq Scan', 'Index Scan', ...]     # Scan operators  
workload_info.join_types = ['Hash Join', 'Nested Loop', ...]   # Join operators
workload_info.table_num_rows = {'title': 2528312, ...}         # Table sizes
```

## 6. Experience Buffer Data

**Location**: `balsa/experience.py:43`
**Purpose**: Manages training data for neural network

```python
class Experience(object):
    def __init__(self, data, ...):
        self.nodes = data                    # Filtered query plan nodes
        self.initial_size = len(data)        # Original data size
        self.workload_info = workload_info   # Metadata
        
    # Key methods:
    def featurize(self):
        # Returns: (query_vecs, plan_vecs, pos_vecs, costs)
        # Converts Node objects to neural network tensors
        
    def Load(self, path_glob):
        # Loads multiple experience buffers for experience replay
        # Used by Balsa-8x to combine data from multiple training runs
```

## 7. File Locations and Naming

```bash
# Raw simulation data (SubplanGoalCost objects)
data/sim-data-{hash}.pkl

# Featurized tensor data (ready for neural network)  
data/sim-featurized-{hash}.pkl

# Expert experience (initial policy)
data/initial_policy_data.pkl

# Experience replay buffers
data/replay-Balsa_JOBRandSplit-*-499iters-*.pkl

# Query files (Join Order Benchmark)
queries/join-order-benchmark/*.sql
```

## 8. Hash-based Caching

Balsa uses content hashes to cache expensive computations:

- **Simulation data hash**: Based on search method, cost model, workload params
- **Featurized data hash**: Based on simulation hash + featurization params  

This ensures data is automatically regenerated when configuration changes affect the results.
- Activate conda activate balsa
- /ssd_root/yrayhan/datasets/job has the job files