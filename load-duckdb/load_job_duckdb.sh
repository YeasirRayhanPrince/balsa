#!/bin/bash
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

set -ex

# Check if DATA_DIR argument is provided
if [ $# -lt 1 ]; then
    echo "Usage: $0 <DATA_DIR> [DBNAME]"
    echo "  DATA_DIR: Directory containing IMDB CSV files"
    echo "  DBNAME:   DuckDB database file name (default: imdbload.duckdb)"
    exit 1
fi

DATA_DIR=$1
DBNAME=${2:-imdbload.duckdb}

# Check if DATA_DIR exists
if [ ! -d "$DATA_DIR" ]; then
    echo "Error: DATA_DIR '$DATA_DIR' does not exist or is not a directory"
    exit 1
fi

# Create/overwrite the DuckDB database file
rm -f $DBNAME

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

# Create schema (convert PostgreSQL schema to DuckDB)
duckdb $DBNAME < "$DIR/schema_duckdb.sql"

# Create indexes (convert PostgreSQL indexes to DuckDB)
duckdb $DBNAME < "$DIR/fkindexes_duckdb.sql"

# Get absolute path to database
DBNAME_ABS="$DIR/$DBNAME"

pushd $DATA_DIR

# Load data from CSV files using DuckDB COPY command
# Note: DuckDB doesn't support parallel writes, so we load sequentially
echo "Loading data sequentially (DuckDB doesn't support concurrent writes)..."

duckdb $DBNAME_ABS -c "COPY name FROM '$DATA_DIR/name.csv' (FORMAT CSV, HEADER, ESCAPE '\')"
duckdb $DBNAME_ABS -c "COPY char_name FROM '$DATA_DIR/char_name.csv' (FORMAT CSV, HEADER, ESCAPE '\')"
duckdb $DBNAME_ABS -c "COPY comp_cast_type FROM '$DATA_DIR/comp_cast_type.csv' (FORMAT CSV, HEADER, ESCAPE '\')"
duckdb $DBNAME_ABS -c "COPY company_name FROM '$DATA_DIR/company_name.csv' (FORMAT CSV, HEADER, ESCAPE '\')"
duckdb $DBNAME_ABS -c "COPY company_type FROM '$DATA_DIR/company_type.csv' (FORMAT CSV, HEADER, ESCAPE '\')"
duckdb $DBNAME_ABS -c "COPY info_type FROM '$DATA_DIR/info_type.csv' (FORMAT CSV, HEADER, ESCAPE '\')"
duckdb $DBNAME_ABS -c "COPY keyword FROM '$DATA_DIR/keyword.csv' (FORMAT CSV, HEADER, ESCAPE '\')"
duckdb $DBNAME_ABS -c "COPY kind_type FROM '$DATA_DIR/kind_type.csv' (FORMAT CSV, HEADER, ESCAPE '\')"
duckdb $DBNAME_ABS -c "COPY link_type FROM '$DATA_DIR/link_type.csv' (FORMAT CSV, HEADER, ESCAPE '\')"
duckdb $DBNAME_ABS -c "COPY role_type FROM '$DATA_DIR/role_type.csv' (FORMAT CSV, HEADER, ESCAPE '\')"
duckdb $DBNAME_ABS -c "COPY title FROM '$DATA_DIR/title.csv' (FORMAT CSV, HEADER, ESCAPE '\')"
duckdb $DBNAME_ABS -c "COPY aka_name FROM '$DATA_DIR/aka_name.csv' (FORMAT CSV, HEADER, ESCAPE '\')"
duckdb $DBNAME_ABS -c "COPY aka_title FROM '$DATA_DIR/aka_title.csv' (FORMAT CSV, HEADER, ESCAPE '\')"
duckdb $DBNAME_ABS -c "COPY cast_info FROM '$DATA_DIR/cast_info.csv' (FORMAT CSV, HEADER, ESCAPE '\')"
duckdb $DBNAME_ABS -c "COPY complete_cast FROM '$DATA_DIR/complete_cast.csv' (FORMAT CSV, HEADER, ESCAPE '\')"
duckdb $DBNAME_ABS -c "COPY movie_companies FROM '$DATA_DIR/movie_companies.csv' (FORMAT CSV, HEADER, ESCAPE '\')"
duckdb $DBNAME_ABS -c "COPY movie_info FROM '$DATA_DIR/movie_info.csv' (FORMAT CSV, HEADER, ESCAPE '\')"
duckdb $DBNAME_ABS -c "COPY movie_info_idx FROM '$DATA_DIR/movie_info_idx.csv' (FORMAT CSV, HEADER, ESCAPE '\')"
duckdb $DBNAME_ABS -c "COPY movie_keyword FROM '$DATA_DIR/movie_keyword.csv' (FORMAT CSV, HEADER, ESCAPE '\')"
duckdb $DBNAME_ABS -c "COPY movie_link FROM '$DATA_DIR/movie_link.csv' (FORMAT CSV, HEADER, ESCAPE '\')"
duckdb $DBNAME_ABS -c "COPY person_info FROM '$DATA_DIR/person_info.csv' (FORMAT CSV, HEADER, ESCAPE '\')"
popd

# Add foreign key constraints (convert PostgreSQL FKs to DuckDB)
# Note: DuckDB doesn't fully support ALTER TABLE ADD FOREIGN KEY yet, skipping for now
echo "Skipping foreign key constraints (DuckDB doesn't fully support them yet)"
# duckdb $DBNAME < "$DIR/add_fks_duckdb.sql"

# Update statistics (DuckDB equivalent of ANALYZE)
duckdb $DBNAME_ABS -c "ANALYZE;"

echo "DuckDB database '$DBNAME' created successfully!"
echo "To connect: duckdb $DBNAME_ABS"