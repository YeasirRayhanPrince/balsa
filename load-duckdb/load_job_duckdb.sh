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

# Get absolute path to database
DBNAME_ABS="$DIR/$DBNAME"

pushd $DATA_DIR

# Load data from CSV files using DuckDB COPY command
# Note: DuckDB doesn't support parallel writes, so we load sequentially
echo "Loading data sequentially (DuckDB doesn't support concurrent writes)..."

# Function to load table with error checking
# load_table() {
#     local table_name=$1
#     local csv_file="$DATA_DIR/${table_name}.csv"
    
#     if [ ! -f "$csv_file" ]; then
#         echo "Warning: CSV file not found: $csv_file"
#         return 1
#     fi
    
#     echo "Loading $table_name..."
#     if ! duckdb $DBNAME_ABS -c "COPY $table_name FROM '$csv_file' (FORMAT CSV, HEADER, IGNORE_ERRORS true)"; then
#         echo "Error: Failed to load $table_name from $csv_file"
#         return 1
#     fi
#     echo "Successfully loaded $table_name"
# }

load_table() {
    local table_name=$1
    local csv_file="$DATA_DIR/${table_name}.csv"

    if [ ! -f "$csv_file" ]; then
        echo "⚠️  Warning: CSV file not found: $csv_file"
        return 1
    fi

    echo "📥 Loading $table_name from $csv_file..."
    if ! duckdb "$DBNAME_ABS" -c "
        DROP TABLE IF EXISTS $table_name;
        CREATE TABLE $table_name AS 
        SELECT * FROM read_csv('$csv_file', 
                               delim=',', 
                               header=TRUE, 
                               quote='\"', 
                               escape='\\');
    "; then
        echo "❌ Error: Failed to load $table_name"
        return 1
    fi
    echo "✅ Successfully loaded $table_name"
}

# Load all tables
load_table "name"
load_table "char_name" 
load_table "comp_cast_type"
load_table "company_name"
load_table "company_type"
load_table "info_type"
load_table "keyword"
load_table "kind_type"
load_table "link_type"
load_table "role_type"
load_table "title"
load_table "aka_name"
load_table "aka_title"
load_table "cast_info"
load_table "complete_cast"
load_table "movie_companies"
load_table "movie_info"
load_table "movie_info_idx"
load_table "movie_keyword"
load_table "movie_link"
load_table "person_info"
popd

# Create indexes after data loading (convert PostgreSQL indexes to DuckDB)
echo "Creating indexes..."
duckdb $DBNAME_ABS < "$DIR/fkindexes_duckdb.sql"

# Add foreign key constraints (convert PostgreSQL FKs to DuckDB)
# Note: DuckDB doesn't fully support ALTER TABLE ADD FOREIGN KEY yet, skipping for now
echo "Skipping foreign key constraints (DuckDB doesn't fully support them yet)"
# duckdb $DBNAME < "$DIR/add_fks_duckdb.sql"

# Update statistics (DuckDB equivalent of ANALYZE)
duckdb $DBNAME_ABS -c "ANALYZE;"

echo "DuckDB database '$DBNAME' created successfully!"
echo "To connect: duckdb $DBNAME_ABS"



# Check row counts for all tables
echo "Checking row counts for all tables..."
duckdb $DBNAME_ABS -c "
SELECT 'name' as table_name, COUNT(*) as rows FROM name
UNION ALL SELECT 'char_name', COUNT(*) FROM char_name
UNION ALL SELECT 'comp_cast_type', COUNT(*) FROM comp_cast_type
UNION ALL SELECT 'company_name', COUNT(*) FROM company_name
UNION ALL SELECT 'company_type', COUNT(*) FROM company_type
UNION ALL SELECT 'info_type', COUNT(*) FROM info_type
UNION ALL SELECT 'keyword', COUNT(*) FROM keyword
UNION ALL SELECT 'kind_type', COUNT(*) FROM kind_type
UNION ALL SELECT 'link_type', COUNT(*) FROM link_type
UNION ALL SELECT 'role_type', COUNT(*) FROM role_type
UNION ALL SELECT 'title', COUNT(*) FROM title
UNION ALL SELECT 'aka_name', COUNT(*) FROM aka_name
UNION ALL SELECT 'aka_title', COUNT(*) FROM aka_title
UNION ALL SELECT 'cast_info', COUNT(*) FROM cast_info
UNION ALL SELECT 'complete_cast', COUNT(*) FROM complete_cast
UNION ALL SELECT 'movie_companies', COUNT(*) FROM movie_companies
UNION ALL SELECT 'movie_info', COUNT(*) FROM movie_info
UNION ALL SELECT 'movie_info_idx', COUNT(*) FROM movie_info_idx
UNION ALL SELECT 'movie_keyword', COUNT(*) FROM movie_keyword
UNION ALL SELECT 'movie_link', COUNT(*) FROM movie_link
UNION ALL SELECT 'person_info', COUNT(*) FROM person_info
ORDER BY table_name;
"



