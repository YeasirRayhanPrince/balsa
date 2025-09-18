#!/bin/bash
# MySQL JOB Dataset Loading Commands

echo "=== Dropping and Recreating imdbload Database ==="

# Drop existing database if it exists
/ssd_root/yrayhan/mysql/bin/mysql -uroot --socket=/ssd_root/yrayhan/mysql/mysql.sock --port=3307 -e "DROP DATABASE IF EXISTS imdbload;"

# Create fresh database
/ssd_root/yrayhan/mysql/bin/mysql -uroot --socket=/ssd_root/yrayhan/mysql/mysql.sock --port=3307 -e "CREATE DATABASE imdbload;"

# Create tables using schema
echo "Creating database schema..."
/ssd_root/yrayhan/mysql/bin/mysql -uroot --socket=/ssd_root/yrayhan/mysql/mysql.sock --port=3307 imdbload < /ssd_root/yrayhan/balsa/load-mysql/schema.sql

echo "=== Loading JOB Dataset into MySQL ==="

# Load keyword table with fixed parser
echo "Loading keyword table..."
python3 /ssd_root/yrayhan/balsa/load-mysql/csv_to_sql_fixed.py /ssd_root/yrayhan/datasets/job/keyword.csv keyword | /ssd_root/yrayhan/mysql/bin/mysql -uroot --socket=/ssd_root/yrayhan/mysql/mysql.sock --port=3307 imdbload
/ssd_root/yrayhan/mysql/bin/mysql -uroot --socket=/ssd_root/yrayhan/mysql/mysql.sock --port=3307 imdbload -e "SELECT 'keyword' as table_name, COUNT(*) as \`rows\` FROM keyword;"

# Load company_name table
echo "Loading company_name table..."
python3 /ssd_root/yrayhan/balsa/load-mysql/csv_to_sql_fixed.py /ssd_root/yrayhan/datasets/job/company_name.csv company_name | /ssd_root/yrayhan/mysql/bin/mysql -uroot --socket=/ssd_root/yrayhan/mysql/mysql.sock --port=3307 imdbload
/ssd_root/yrayhan/mysql/bin/mysql -uroot --socket=/ssd_root/yrayhan/mysql/mysql.sock --port=3307 imdbload -e "SELECT 'company_name' as table_name, COUNT(*) as \`rows\` FROM company_name;"

# Load title table (crucial for JOB queries)
echo "Loading title table..."
python3 /ssd_root/yrayhan/balsa/load-mysql/csv_to_sql_fixed.py /ssd_root/yrayhan/datasets/job/title.csv title | /ssd_root/yrayhan/mysql/bin/mysql -uroot --socket=/ssd_root/yrayhan/mysql/mysql.sock --port=3307 imdbload
/ssd_root/yrayhan/mysql/bin/mysql -uroot --socket=/ssd_root/yrayhan/mysql/mysql.sock --port=3307 imdbload -e "SELECT 'title' as table_name, COUNT(*) as \`rows\` FROM title;"

# Load name table
echo "Loading name table..."
python3 /ssd_root/yrayhan/balsa/load-mysql/csv_to_sql_fixed.py /ssd_root/yrayhan/datasets/job/name.csv name | /ssd_root/yrayhan/mysql/bin/mysql -uroot --socket=/ssd_root/yrayhan/mysql/mysql.sock --port=3307 imdbload
/ssd_root/yrayhan/mysql/bin/mysql -uroot --socket=/ssd_root/yrayhan/mysql/mysql.sock --port=3307 imdbload -e "SELECT 'name' as table_name, COUNT(*) as \`rows\` FROM name;"

# Load char_name table
echo "Loading char_name table..."
python3 /ssd_root/yrayhan/balsa/load-mysql/csv_to_sql_fixed.py /ssd_root/yrayhan/datasets/job/char_name.csv char_name | /ssd_root/yrayhan/mysql/bin/mysql -uroot --socket=/ssd_root/yrayhan/mysql/mysql.sock --port=3307 imdbload
/ssd_root/yrayhan/mysql/bin/mysql -uroot --socket=/ssd_root/yrayhan/mysql/mysql.sock --port=3307 imdbload -e "SELECT 'char_name' as table_name, COUNT(*) as \`rows\` FROM char_name;"

# Load reference tables
echo "Loading comp_cast_type table..."
python3 /ssd_root/yrayhan/balsa/load-mysql/csv_to_sql_fixed.py /ssd_root/yrayhan/datasets/job/comp_cast_type.csv comp_cast_type | /ssd_root/yrayhan/mysql/bin/mysql -uroot --socket=/ssd_root/yrayhan/mysql/mysql.sock --port=3307 imdbload
/ssd_root/yrayhan/mysql/bin/mysql -uroot --socket=/ssd_root/yrayhan/mysql/mysql.sock --port=3307 imdbload -e "SELECT 'comp_cast_type' as table_name, COUNT(*) as \`rows\` FROM comp_cast_type;"

echo "Loading company_type table..."
python3 /ssd_root/yrayhan/balsa/load-mysql/csv_to_sql_fixed.py /ssd_root/yrayhan/datasets/job/company_type.csv company_type | /ssd_root/yrayhan/mysql/bin/mysql -uroot --socket=/ssd_root/yrayhan/mysql/mysql.sock --port=3307 imdbload
/ssd_root/yrayhan/mysql/bin/mysql -uroot --socket=/ssd_root/yrayhan/mysql/mysql.sock --port=3307 imdbload -e "SELECT 'company_type' as table_name, COUNT(*) as \`rows\` FROM company_type;"

echo "Loading info_type table..."
python3 /ssd_root/yrayhan/balsa/load-mysql/csv_to_sql_fixed.py /ssd_root/yrayhan/datasets/job/info_type.csv info_type | /ssd_root/yrayhan/mysql/bin/mysql -uroot --socket=/ssd_root/yrayhan/mysql/mysql.sock --port=3307 imdbload
/ssd_root/yrayhan/mysql/bin/mysql -uroot --socket=/ssd_root/yrayhan/mysql/mysql.sock --port=3307 imdbload -e "SELECT 'info_type' as table_name, COUNT(*) as \`rows\` FROM info_type;"

echo "Loading kind_type table..."
python3 /ssd_root/yrayhan/balsa/load-mysql/csv_to_sql_fixed.py /ssd_root/yrayhan/datasets/job/kind_type.csv kind_type | /ssd_root/yrayhan/mysql/bin/mysql -uroot --socket=/ssd_root/yrayhan/mysql/mysql.sock --port=3307 imdbload
/ssd_root/yrayhan/mysql/bin/mysql -uroot --socket=/ssd_root/yrayhan/mysql/mysql.sock --port=3307 imdbload -e "SELECT 'kind_type' as table_name, COUNT(*) as \`rows\` FROM kind_type;"

echo "Loading link_type table..."
python3 /ssd_root/yrayhan/balsa/load-mysql/csv_to_sql_fixed.py /ssd_root/yrayhan/datasets/job/link_type.csv link_type | /ssd_root/yrayhan/mysql/bin/mysql -uroot --socket=/ssd_root/yrayhan/mysql/mysql.sock --port=3307 imdbload
/ssd_root/yrayhan/mysql/bin/mysql -uroot --socket=/ssd_root/yrayhan/mysql/mysql.sock --port=3307 imdbload -e "SELECT 'link_type' as table_name, COUNT(*) as \`rows\` FROM link_type;"

echo "Loading role_type table..."
python3 /ssd_root/yrayhan/balsa/load-mysql/csv_to_sql_fixed.py /ssd_root/yrayhan/datasets/job/role_type.csv role_type | /ssd_root/yrayhan/mysql/bin/mysql -uroot --socket=/ssd_root/yrayhan/mysql/mysql.sock --port=3307 imdbload
/ssd_root/yrayhan/mysql/bin/mysql -uroot --socket=/ssd_root/yrayhan/mysql/mysql.sock --port=3307 imdbload -e "SELECT 'role_type' as table_name, COUNT(*) as \`rows\` FROM role_type;"

# Load remaining tables
echo "Loading aka_name table..."
python3 /ssd_root/yrayhan/balsa/load-mysql/csv_to_sql_fixed.py /ssd_root/yrayhan/datasets/job/aka_name.csv aka_name | /ssd_root/yrayhan/mysql/bin/mysql -uroot --socket=/ssd_root/yrayhan/mysql/mysql.sock --port=3307 imdbload
/ssd_root/yrayhan/mysql/bin/mysql -uroot --socket=/ssd_root/yrayhan/mysql/mysql.sock --port=3307 imdbload -e "SELECT 'aka_name' as table_name, COUNT(*) as \`rows\` FROM aka_name;"

echo "Loading aka_title table..."
python3 /ssd_root/yrayhan/balsa/load-mysql/csv_to_sql_fixed.py /ssd_root/yrayhan/datasets/job/aka_title.csv aka_title | /ssd_root/yrayhan/mysql/bin/mysql -uroot --socket=/ssd_root/yrayhan/mysql/mysql.sock --port=3307 imdbload
/ssd_root/yrayhan/mysql/bin/mysql -uroot --socket=/ssd_root/yrayhan/mysql/mysql.sock --port=3307 imdbload -e "SELECT 'aka_title' as table_name, COUNT(*) as \`rows\` FROM aka_title;"

echo "Loading complete_cast table..."
python3 /ssd_root/yrayhan/balsa/load-mysql/csv_to_sql_fixed.py /ssd_root/yrayhan/datasets/job/complete_cast.csv complete_cast | /ssd_root/yrayhan/mysql/bin/mysql -uroot --socket=/ssd_root/yrayhan/mysql/mysql.sock --port=3307 imdbload
/ssd_root/yrayhan/mysql/bin/mysql -uroot --socket=/ssd_root/yrayhan/mysql/mysql.sock --port=3307 imdbload -e "SELECT 'complete_cast' as table_name, COUNT(*) as \`rows\` FROM complete_cast;"

echo "Loading movie_link table..."
python3 /ssd_root/yrayhan/balsa/load-mysql/csv_to_sql_fixed.py /ssd_root/yrayhan/datasets/job/movie_link.csv movie_link | /ssd_root/yrayhan/mysql/bin/mysql -uroot --socket=/ssd_root/yrayhan/mysql/mysql.sock --port=3307 imdbload
/ssd_root/yrayhan/mysql/bin/mysql -uroot --socket=/ssd_root/yrayhan/mysql/mysql.sock --port=3307 imdbload -e "SELECT 'movie_link' as table_name, COUNT(*) as \`rows\` FROM movie_link;"

echo "Loading movie_companies table..."
python3 /ssd_root/yrayhan/balsa/load-mysql/csv_to_sql_fixed.py /ssd_root/yrayhan/datasets/job/movie_companies.csv movie_companies | /ssd_root/yrayhan/mysql/bin/mysql -uroot --socket=/ssd_root/yrayhan/mysql/mysql.sock --port=3307 imdbload
/ssd_root/yrayhan/mysql/bin/mysql -uroot --socket=/ssd_root/yrayhan/mysql/mysql.sock --port=3307 imdbload -e "SELECT 'movie_companies' as table_name, COUNT(*) as \`rows\` FROM movie_companies;"

echo "Loading movie_keyword table..."
python3 /ssd_root/yrayhan/balsa/load-mysql/csv_to_sql_fixed.py /ssd_root/yrayhan/datasets/job/movie_keyword.csv movie_keyword | /ssd_root/yrayhan/mysql/bin/mysql -uroot --socket=/ssd_root/yrayhan/mysql/mysql.sock --port=3307 imdbload
/ssd_root/yrayhan/mysql/bin/mysql -uroot --socket=/ssd_root/yrayhan/mysql/mysql.sock --port=3307 imdbload -e "SELECT 'movie_keyword' as table_name, COUNT(*) as \`rows\` FROM movie_keyword;"

# ----------
echo "Loading person_info table..."
python3 /ssd_root/yrayhan/balsa/load-mysql/csv_to_sql_fixed.py /ssd_root/yrayhan/datasets/job/person_info.csv person_info | /ssd_root/yrayhan/mysql/bin/mysql -uroot --socket=/ssd_root/yrayhan/mysql/mysql.sock --port=3307 imdbload
/ssd_root/yrayhan/mysql/bin/mysql -uroot --socket=/ssd_root/yrayhan/mysql/mysql.sock --port=3307 imdbload -e "SELECT 'person_info' as table_name, COUNT(*) as \`rows\` FROM person_info;"

# Load the largest tables
echo "Loading movie_info table (this will take a while)..."
python3 /ssd_root/yrayhan/balsa/load-mysql/csv_to_sql_fixed.py /ssd_root/yrayhan/datasets/job/movie_info.csv movie_info | /ssd_root/yrayhan/mysql/bin/mysql -uroot --socket=/ssd_root/yrayhan/mysql/mysql.sock --port=3307 imdbload
/ssd_root/yrayhan/mysql/bin/mysql -uroot --socket=/ssd_root/yrayhan/mysql/mysql.sock --port=3307 imdbload -e "SELECT 'movie_info' as table_name, COUNT(*) as \`rows\` FROM movie_info;"

echo "Loading movie_info_idx table..."
python3 /ssd_root/yrayhan/balsa/load-mysql/csv_to_sql_fixed.py /ssd_root/yrayhan/datasets/job/movie_info_idx.csv movie_info_idx | /ssd_root/yrayhan/mysql/bin/mysql -uroot --socket=/ssd_root/yrayhan/mysql/mysql.sock --port=3307 imdbload
/ssd_root/yrayhan/mysql/bin/mysql -uroot --socket=/ssd_root/yrayhan/mysql/mysql.sock --port=3307 imdbload -e "SELECT 'movie_info_idx' as table_name, COUNT(*) as \`rows\` FROM movie_info_idx;"

echo "Loading cast_info table (largest table, will take longest)..."
python3 /ssd_root/yrayhan/balsa/load-mysql/csv_to_sql_fixed.py /ssd_root/yrayhan/datasets/job/cast_info.csv cast_info | /ssd_root/yrayhan/mysql/bin/mysql -uroot --socket=/ssd_root/yrayhan/mysql/mysql.sock --port=3307 imdbload
/ssd_root/yrayhan/mysql/bin/mysql -uroot --socket=/ssd_root/yrayhan/mysql/mysql.sock --port=3307 imdbload -e "SELECT 'cast_info' as table_name, COUNT(*) as \`rows\` FROM cast_info;"

# Final summary
echo "=== Final Table Counts ==="
/ssd_root/yrayhan/mysql/bin/mysql -uroot --socket=/ssd_root/yrayhan/mysql/mysql.sock --port=3307 imdbload -e "
SELECT 'keyword' as table_name, COUNT(*) as \`rows\` FROM keyword
UNION ALL SELECT 'company_name', COUNT(*) FROM company_name
UNION ALL SELECT 'title', COUNT(*) FROM title  
UNION ALL SELECT 'name', COUNT(*) FROM name
UNION ALL SELECT 'char_name', COUNT(*) FROM char_name
UNION ALL SELECT 'aka_name', COUNT(*) FROM aka_name
UNION ALL SELECT 'aka_title', COUNT(*) FROM aka_title
UNION ALL SELECT 'complete_cast', COUNT(*) FROM complete_cast
UNION ALL SELECT 'movie_link', COUNT(*) FROM movie_link
UNION ALL SELECT 'movie_companies', COUNT(*) FROM movie_companies
UNION ALL SELECT 'movie_keyword', COUNT(*) FROM movie_keyword
UNION ALL SELECT 'person_info', COUNT(*) FROM person_info
UNION ALL SELECT 'movie_info', COUNT(*) FROM movie_info
UNION ALL SELECT 'movie_info_idx', COUNT(*) FROM movie_info_idx
UNION ALL SELECT 'cast_info', COUNT(*) FROM cast_info
ORDER BY \`rows\` DESC;
"
# Create indexes before loading data for better performance
echo "Creating indexes..."
/ssd_root/yrayhan/mysql/bin/mysql -uroot --socket=/ssd_root/yrayhan/mysql/mysql.sock --port=3307 imdbload < /ssd_root/yrayhan/balsa/load-mysql/fkindexes.sql


echo "=== Adding Foreign Keys ==="
/ssd_root/yrayhan/mysql/bin/mysql -uroot --socket=/ssd_root/yrayhan/mysql/mysql.sock --port=3307 imdbload < /ssd_root/yrayhan/balsa/load-mysql/add_fks.sql

echo "=== Running ANALYZE TABLE ==="
/ssd_root/yrayhan/mysql/bin/mysql -uroot --socket=/ssd_root/yrayhan/mysql/mysql.sock --port=3307 imdbload -e "
ANALYZE TABLE 
aka_name, aka_title, cast_info, char_name, comp_cast_type,
company_name, company_type, complete_cast, info_type, keyword,
kind_type, link_type, movie_companies, movie_info, movie_info_idx,
movie_keyword, movie_link, name, person_info, role_type, title;
"

echo "=== MySQL JOB Dataset Loading Complete! ==="
