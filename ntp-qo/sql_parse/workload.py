import glob
import logging
import os
import copy
import collections
from dataclasses import dataclass
from typing import List, Optional

import numpy as np
from dbs import postgres, duckdb

def ParseSqlToNode(path, engine):
    base = os.path.basename(path)
    query_name = os.path.splitext(base)[0]
    with open(path, 'r') as f:
        sql_string = f.read()
    # ****************************** This is where the magic happens ***********************
    if engine == 'postgres':
      node, json_dict = postgres.SqlToPlanNode(sql_string)
    elif engine == 'duckdb':
      node, json_dict = duckdb.SqlToPlanNode(sql_string)
    else:
      raise ValueError(f"Unsupported engine: {engine}. Supported engines: postgres, duckdb")
    # ****************************** This is where the magic happens ***********************
    node.info['path'] = path
    node.info['sql_str'] = sql_string
    node.info['query_name'] = query_name
    node.info['explain_json'] = json_dict
    node.GetOrParseSql()
    return node

@dataclass
class WorkloadParams:
  query_dir: Optional[str] = None
  query_glob: str = '*.sql'
  loop_through_queries: bool = False
  test_query_glob: Optional[str] = None
  search_space_join_ops: List[str] = None
  search_space_scan_ops: List[str] = None
  engine: str = 'postgres'
  
  # def __post_init__(self):
  #   if self.search_space_join_ops is None:
  #     self.search_space_join_ops = ['Hash Join', 'Merge Join', 'Nested Loop']
  #   if self.search_space_scan_ops is None:
  #     self.search_space_scan_ops = ['Index Scan', 'Index Only Scan', 'Seq Scan']


class Workload(object):
  def __init__(self, params: WorkloadParams):
    self.params = params
    p = self.params
    
    # Subclasses should populate these fields.
    self.query_nodes = None
    self.workload_info = None
    self.train_nodes = None
    self.test_nodes = None

    if p.loop_through_queries:
      self.queries_permuted = False
      self.queries_ptr = 0

  def _ensure_queries_permuted(self, rng):
    """Permutes queries once."""
    if not self.queries_permuted:
      self.query_nodes = rng.permutation(self.query_nodes)
      self.queries_permuted = True

  def _get_sql_set(self, query_dir, query_glob):
    if query_glob is None:
      return set()
    else:
      globs = query_glob
      if type(query_glob) is str:
        globs = [query_glob]
      sql_files = np.concatenate([
        glob.glob('{}/{}'.format(query_dir, pattern))
        for pattern in globs
      ]).ravel()
    sql_files = set(sql_files)
    return sql_files

  def Queries(self, split='all'):
    """Returns all queries as balsa.Node objects."""
    assert split in ['all', 'train', 'test'], split
    if split == 'all':
      return self.query_nodes
    elif split == 'train':
      return self.train_nodes
    elif split == 'test':
      return self.test_nodes

  def WithQueries(self, query_nodes):
    """Replaces this Workload's queries with 'query_nodes'."""
    self.query_nodes = query_nodes
    self.workload_info = WorkloadInfo(query_nodes)

  def FilterQueries(self, query_dir, query_glob, test_query_glob):
    all_sql_set_new = self._get_sql_set(query_dir, query_glob)
    test_sql_set_new = self._get_sql_set(query_dir, test_query_glob)
    assert test_sql_set_new.issubset(all_sql_set_new), (test_sql_set_new,
                                                        all_sql_set_new)

    all_sql_set = set([n.info['path'] for n in self.query_nodes])
    assert all_sql_set_new.issubset(all_sql_set), (
      'Missing nodes in init_experience; '
      'To fix: remove data/initial_policy_data.pkl, or see README.')

    query_nodes_new = [
      n for n in self.query_nodes if n.info['path'] in all_sql_set_new
    ]
    train_nodes_new = [
      n for n in query_nodes_new
      if test_query_glob is None or n.info['path'] not in test_sql_set_new
    ]
    test_nodes_new = [
      n for n in query_nodes_new if n.info['path'] in test_sql_set_new
    ]
    assert len(train_nodes_new) > 0

    self.query_nodes = query_nodes_new
    self.train_nodes = train_nodes_new
    self.test_nodes = test_nodes_new

  def UseDialectSql(self, p):
    dialect_sql_dir = p.engine_dialect_query_dir
    for node in self.query_nodes:
      assert 'sql_str' in node.info and 'query_name' in node.info
      path = os.path.join(dialect_sql_dir,
                          node.info['query_name'] + '.sql')
      assert os.path.isfile(path), '{} does not exist'.format(path)
      with open(path, 'r') as f:
        dialect_sql_string = f.read()
      node.info['sql_str'] = dialect_sql_string


class JoinOrderBenchmark(Workload):
  def __init__(self, params: WorkloadParams):
    if params.query_dir is None:
      raise ValueError("query_dir must be specified. Example: WorkloadParams(query_dir='/path/to/job/queries')")
    # engine now has a default value of 'postgres', but can be overridden
    
    super().__init__(params)
    p = params
    logging.info('Load queries.')
    self.query_nodes, self.train_nodes, self.test_nodes = self._LoadQueries()
    logging.info('Build schema information and physical ops.')
    self.workload_info = WorkloadInfo(self.query_nodes)
    self.workload_info.SetPhysicalOps(p.search_space_join_ops, p.search_space_scan_ops)

  def _LoadQueries(self):
    """Loads all queries into balsa.Node objects."""
    p = self.params
    all_sql_set = self._get_sql_set(p.query_dir, p.query_glob)
    test_sql_set = self._get_sql_set(p.query_dir, p.test_query_glob)
    assert test_sql_set.issubset(all_sql_set)
    
    # ['queries/join-order-benchmark/10a.sql', 'queries/join-order-benchmark/10b.sql', ...
    # sorted by query id for easy debugging
    all_sql_list = sorted(all_sql_set)
    
    # ******************************* This is where the magic happens ***********************

    if os.getenv('BALSA_DEBUG_INTERACTIVE'):
      all_nodes = []
      for sqlfile in all_sql_list:
        input(f"Processing {sqlfile}. Press Enter...")
        node = ParseSqlToNode(sqlfile, p.engine)
        
        print(f"\n=== Node for {sqlfile} ===")
        print(node)  # Uses the __str__ method (calls to_str())
        node.print_tree()
        
        all_nodes.append(node)
    else:
      all_nodes = [ParseSqlToNode(sqlfile, p.engine) for sqlfile in all_sql_list]
    # ******************************* This is where the magic happens ***********************

    train_nodes = [
      n for n in all_nodes
      if p.test_query_glob is None or n.info['path'] not in test_sql_set
    ]
    test_nodes = [n for n in all_nodes if n.info['path'] in test_sql_set]
    assert len(train_nodes) > 0

    return all_nodes, train_nodes, test_nodes
  
class WorkloadInfo(object):
    """Stores sets of possible relations/aliases/join types, etc.

    From a list of all Nodes, parse
    - all relation names
    - all join types
    - all scan types.
    These can also be specified manually for a workload.

    Attributes:
      rel_names, rel_ids, scan_types, join_types, all_ops: ndarray of sorted
        strings.
    """

    def __init__(self, nodes):
        rel_names = set()
        rel_ids = set()
        scan_types = set()
        join_types = set()
        all_ops = set()

        all_attributes = set()

        all_filters = collections.defaultdict(set)

        def _fill(root, node):
            all_ops.add(node.node_type)

            if node.table_name is not None:
                rel_names.add(node.table_name)
                rel_ids.add(node.get_table_id())

            if node.info and 'filter' in node.info:
                table_id = node.get_table_id()
                all_filters[table_id].add(node.info['filter'])

            if node.info and 'sql_str' in node.info:
                # We want "all" attributes but as an optimization, we keep the
                # attributes that are known to be filter-able.
                attrs = node.GetFilteredAttributes()
                all_attributes.update(attrs)

            if 'Scan' in node.node_type:
                scan_types.add(node.node_type)
            elif node.IsJoin():
                join_types.add(node.node_type)

            for c in node.children:
                _fill(root, c)

        for node in nodes:
            _fill(node, node)

        self.rel_names = np.asarray(sorted(list(rel_names)))
        self.rel_ids = np.asarray(sorted(list(rel_ids)))
        self.scan_types = np.asarray(sorted(list(scan_types)))
        self.join_types = np.asarray(sorted(list(join_types)))
        self.all_ops = np.asarray(sorted(list(all_ops)))
        self.all_attributes = np.asarray(sorted(list(all_attributes)))

    def SetPhysicalOps(self, join_ops, scan_ops):
        old_scans = self.scan_types
        old_joins = self.join_types
        if scan_ops is not None:
            self.scan_types = np.asarray(sorted(list(scan_ops)))
        if join_ops is not None:
            self.join_types = np.asarray(sorted(list(join_ops)))
        new_all_ops = [
            op for op in self.all_ops
            if op not in old_scans and op not in old_joins
        ]
        new_all_ops = new_all_ops + list(self.scan_types) + list(
            self.join_types)
        if len(self.all_ops) != len(new_all_ops):
            print('Search space (old=query nodes; new=agent action space):')
            print('old:', old_scans, old_joins, self.all_ops)
            print('new:', self.scan_types, self.join_types, self.all_ops)
        self.all_ops = np.asarray(sorted(list(set(new_all_ops))))

    def WithJoinGraph(self, join_graph):
        """Transforms { table -> neighbors } into internal representation."""
        self.join_edge_set = set()
        for t1, neighbors in join_graph.items():
            for t2 in neighbors:
                self.join_edge_set.add((t1, t2))
                self.join_edge_set.add((t2, t1))

    def Copy(self):
        return copy.deepcopy(self)

    def HasPhysicalOps(self):
        if not np.array_equal(self.scan_types, ['Scan']):
            return True
        if not np.array_equal(self.join_types, ['Join']):
            return True
        return False

    def __repr__(self):
        fmt = 'rel_names: {}\nrel_ids: {}\nscan_types: {}\n' \
        'join_types: {}\nall_ops: {}\nall_attributes: {}'
        return fmt.format(self.rel_names, self.rel_ids, self.scan_types,
                          self.join_types, self.all_ops, self.all_attributes)

