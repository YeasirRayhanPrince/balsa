#!/usr/bin/env python3
import csv
import sys
import os

def refine_row(row, expected_columns, table_name, line_num):
    """Refine rows with incorrect column counts by applying table-specific logic."""
    
    if len(row) == expected_columns:
        return row
    
    # Table-specific refinement logic
    if table_name.lower() == 'keyword':
        return refine_keyword_row(row, expected_columns, line_num)
    elif table_name.lower() == 'title':
        return refine_title_row(row, expected_columns, line_num)
    elif table_name.lower() == 'cast_info':
        return refine_cast_info_row(row, expected_columns, line_num)
    elif table_name.lower() == 'movie_info':
        return refine_movie_info_row(row, expected_columns, line_num)
    else:
        # Default refinement: try to merge excess columns or pad missing ones
        return refine_default_row(row, expected_columns, line_num)

def refine_keyword_row(row, expected_columns, line_num):
    """Refine keyword table rows by concatenating columns when needed."""
    if len(row) > expected_columns:
        # For keyword table, concatenate middle columns
        # Assuming structure: [id, keyword_text, phonetic_code]
        # If we have extra columns, merge them into the keyword_text field
        refined_row = [row[0]]  # Keep first column (id)
        
        # Concatenate columns 1 through (len(row) - 1) for keyword text
        merged_text = ' '.join(str(col) for col in row[1:-1] if col.strip())
        refined_row.append(merged_text)
        
        # Keep last column if it exists
        if len(row) > 2:
            refined_row.append(row[-1])
        
        # Pad with empty strings if still not enough columns
        while len(refined_row) < expected_columns:
            refined_row.append('')
            
        print(f"-- Refined keyword row {line_num}: {row} -> {refined_row}", file=sys.stderr)
        return refined_row
    
    elif len(row) < expected_columns:
        # Pad with empty strings
        refined_row = row + [''] * (expected_columns - len(row))
        print(f"-- Padded keyword row {line_num}: {row} -> {refined_row}", file=sys.stderr)
        return refined_row
    
    return row

def refine_title_row(row, expected_columns, line_num):
    """Refine title table rows."""
    if len(row) > expected_columns:
        # For title table, merge title-related columns
        refined_row = row[:2]  # Keep first two columns (id, title_type)
        
        # Merge title and related text fields
        title_text = ' '.join(str(col) for col in row[2:expected_columns] if col.strip())
        refined_row.append(title_text)
        
        # Add remaining expected columns from the end of the original row
        if len(row) > expected_columns:
            refined_row.extend(row[-(expected_columns-3):])
        
        # Ensure exact column count
        while len(refined_row) < expected_columns:
            refined_row.append('')
        refined_row = refined_row[:expected_columns]
        
        print(f"-- Refined title row {line_num}: {row} -> {refined_row}", file=sys.stderr)
        return refined_row
    
    elif len(row) < expected_columns:
        refined_row = row + [''] * (expected_columns - len(row))
        print(f"-- Padded title row {line_num}: {row} -> {refined_row}", file=sys.stderr)
        return refined_row
    
    return row

def refine_cast_info_row(row, expected_columns, line_num):
    """Refine cast_info table rows."""
    if len(row) > expected_columns:
        # For cast_info, merge character name or role-related fields
        refined_row = row[:3]  # Keep first three columns typically
        
        # Merge character/role related text
        if len(row) > 3:
            char_text = ' '.join(str(col) for col in row[3:expected_columns] if col.strip())
            refined_row.append(char_text)
        
        # Add remaining columns from end
        remaining_needed = expected_columns - len(refined_row)
        if remaining_needed > 0 and len(row) >= expected_columns:
            refined_row.extend(row[-remaining_needed:])
        
        # Ensure exact column count
        while len(refined_row) < expected_columns:
            refined_row.append('')
        refined_row = refined_row[:expected_columns]
        
        print(f"-- Refined cast_info row {line_num}: {row} -> {refined_row}", file=sys.stderr)
        return refined_row
    
    elif len(row) < expected_columns:
        refined_row = row + [''] * (expected_columns - len(row))
        print(f"-- Padded cast_info row {line_num}: {row} -> {refined_row}", file=sys.stderr)
        return refined_row
    
    return row

def refine_movie_info_row(row, expected_columns, line_num):
    """Refine movie_info table rows."""
    if len(row) > expected_columns:
        # For movie_info, merge info value fields
        refined_row = row[:2]  # Keep first two columns (id, movie_id)
        
        # Merge info_type and info value
        if len(row) > 2:
            info_text = ' '.join(str(col) for col in row[2:expected_columns] if col.strip())
            refined_row.append(info_text)
        
        # Add remaining expected columns
        remaining_needed = expected_columns - len(refined_row)
        if remaining_needed > 0:
            if len(row) >= expected_columns:
                refined_row.extend(row[-remaining_needed:])
            else:
                refined_row.extend([''] * remaining_needed)
        
        refined_row = refined_row[:expected_columns]
        
        print(f"-- Refined movie_info row {line_num}: {row} -> {refined_row}", file=sys.stderr)
        return refined_row
    
    elif len(row) < expected_columns:
        refined_row = row + [''] * (expected_columns - len(row))
        print(f"-- Padded movie_info row {line_num}: {row} -> {refined_row}", file=sys.stderr)
        return refined_row
    
    return row

def refine_default_row(row, expected_columns, line_num):
    """Default row refinement for unknown table types."""
    if len(row) > expected_columns:
        # Merge excess columns into the last expected column
        refined_row = row[:expected_columns-1]
        merged_text = ' '.join(str(col) for col in row[expected_columns-1:] if col.strip())
        refined_row.append(merged_text)
        
        print(f"-- Default refined row {line_num}: {row} -> {refined_row}", file=sys.stderr)
        return refined_row
    
    elif len(row) < expected_columns:
        # Pad with empty strings
        refined_row = row + [''] * (expected_columns - len(row))
        print(f"-- Default padded row {line_num}: {row} -> {refined_row}", file=sys.stderr)
        return refined_row
    
    return row

def csv_to_sql_batched(csv_file, table_name, batch_size=1000):
    """Convert CSV file to batched SQL INSERT statements with proper CSV handling."""
    
    with open(csv_file, 'r', encoding='utf-8') as f:
        # Use proper CSV configuration for handling quotes and escapes
        reader = csv.reader(f, 
            delimiter=',',
            quotechar='"',
            escapechar='\\',
            quoting=csv.QUOTE_MINIMAL,
            skipinitialspace=True,
            doublequote=True
        )
        
        headers = next(reader)  # Skip header row
        expected_columns = len(headers)
        print(f"-- Expected columns: {expected_columns}, Headers: {headers}", file=sys.stderr)
        
        print("SET autocommit = 1;")
        
        batch = []
        total_processed = 0
        skipped_rows = 0
        refined_rows = 0
        
        for line_num, row in enumerate(reader, start=2):
            if len(row) != expected_columns:
                print(f"-- Warning: Refining line {line_num}, expected {expected_columns} columns but got {len(row)}: {row}", file=sys.stderr)
                skipped_rows += 1
                continue
            
                # original_row_len = len(row)
                # row = refine_row(row, expected_columns, table_name, line_num)
                # print(row)
                # if row is None:
                #     skipped_rows += 1
                #     continue
                # else:
                #     refined_rows += 1
                
            # Escape values properly for MySQL and handle NULL values
            escaped_values = []
            for value in row:
                if value is None or value.strip() == '':
                    escaped_values.append('NULL')
                else:
                    # Proper MySQL escaping: escape backslashes first, then single quotes
                    escaped_value = str(value).replace('\\', '\\\\').replace("'", "\\'")
                    escaped_values.append(f"'{escaped_value}'")
            
            values_str = '(' + ', '.join(escaped_values) + ')'
            batch.append(values_str)
            
            # When batch is full, output the INSERT statement
            if len(batch) >= batch_size:
                print(f"INSERT INTO {table_name} VALUES {', '.join(batch)};")
                batch = []
                total_processed += batch_size
                
                # Add a progress comment
                # if total_processed % 10000 == 0:
                #     print(f"-- Processed {total_processed} rows")
        
        # Handle remaining batch
        if batch:
            print(f"INSERT INTO {table_name} VALUES {', '.join(batch)};")
            total_processed += len(batch)
        
        print(f"-- Total processed: {total_processed} rows, Refined: {refined_rows} rows, Skipped: {skipped_rows} rows", file=sys.stderr)

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python3 csv_to_sql_fixed.py <csv_file> <table_name>")
        sys.exit(1)
    
    csv_file = sys.argv[1]
    table_name = sys.argv[2]
    
    if not os.path.exists(csv_file):
        print(f"Error: File {csv_file} not found")
        sys.exit(1)
    
    try:
        csv_to_sql_batched(csv_file, table_name)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)