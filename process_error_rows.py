import pandas as pd
import duckdb
from deltalake.writer import write_deltalake
import os
import shutil
import glob
import argparse # Added for command-line arguments if needed in future

# --- Configuration ---
# Use relative paths, assuming script runs from project root
ERROR_INPUT_DIR = './error_input/'
PROCESSED_ERROR_DIR = os.path.join(ERROR_INPUT_DIR, 'processed')
QUARANTINE_LAKEHOUSE_PATH = './lakehouse_data/quarantined_disasters'

# Define the essential CLEAN column names expected in this version of the error CSV
# These should match the headers in the new erroneous_rows.csv
ESSENTIAL_INPUT_COLUMNS = [
    'year', 'event_date_start', 'event_date_end', 'province', 'municipality',
    'commodity', 'disaster_category', 'disaster_name',
    'area_partially_damaged_ha', 'area_totally_damaged_ha', 'area_total_affected_ha',
    'farmers_affected', 'losses_php_production_cost', 'losses_php_farm_gate',
    'losses_php_grand_total', 'source_row_number', 'error_reason'
    # 'disaster_type_raw' and 'sanitation_remarks' are optional but present in the file
]

# Define the final set of columns to keep in the Delta table
# Can be the same as input, or a subset/superset
FINAL_DELTA_COLUMNS = ESSENTIAL_INPUT_COLUMNS + ['disaster_type_raw', 'sanitation_remarks']


def process_error_file(file_path):
    """Reads, validates, cleans types, and processes a single error CSV file with the NEW structure."""
    print(f"\nProcessing error file: {os.path.basename(file_path)}...")
    try:
        # Explicitly set low_memory=False to potentially help with mixed types
        df_error = pd.read_csv(file_path, low_memory=False)
    except FileNotFoundError:
        print(f"ERROR: File not found: {file_path}")
        return False
    except Exception as e:
        print(f"ERROR reading file {os.path.basename(file_path)}: {e}")
        return False

    if df_error.empty:
        print("Skipping empty file.")
        return True # Treat as success for moving file

    # --- <<< START: ESSENTIAL COLUMN CHECK >>> ---
    print("Checking for essential columns...")
    current_columns = df_error.columns.tolist()
    missing_cols = [col for col in ESSENTIAL_INPUT_COLUMNS if col not in current_columns]
    if missing_cols:
        print(f"ERROR: Essential columns missing in {os.path.basename(file_path)}:")
        for col in missing_cols:
            print(f"  - '{col}'")
        print("Stopping processing for this file due to missing essential columns.")
        return False # Indicate failure, do not move the file
    print("All essential columns found.")
    # --- <<< END: ESSENTIAL COLUMN CHECK >>> ---


    # 1. Clean Data Types
    print("Cleaning data types...")
    # Convert date columns (object -> datetime)
    # Using errors='coerce' handles bad date formats gracefully -> becomes NaT
    if 'event_date_start' in df_error.columns:
        df_error['event_date_start'] = pd.to_datetime(df_error['event_date_start'], errors='coerce')
    if 'event_date_end' in df_error.columns:
        df_error['event_date_end'] = pd.to_datetime(df_error['event_date_end'], errors='coerce')
    print("Converted date columns.")

    # Convert numeric columns, coercing errors to NaN, then filling NaN with 0
    numeric_cols = [
        'area_partially_damaged_ha', 'area_totally_damaged_ha', 'area_total_affected_ha',
        'farmers_affected', 'losses_php_production_cost', 'losses_php_farm_gate',
        'losses_php_grand_total'
    ]
    for col in numeric_cols:
        if col in df_error.columns:
            df_error[col] = pd.to_numeric(df_error[col], errors='coerce').fillna(0)
    print("Converted numeric columns.")

    # Ensure essential string columns are strings and handle potential NaNs
    string_cols = ['province', 'municipality', 'commodity', 'disaster_category',
                   'disaster_name', 'error_reason', 'disaster_type_raw', 'sanitation_remarks']
    for col in string_cols:
         if col in df_error.columns:
            df_error[col] = df_error[col].astype(str).fillna('Unknown')
    print("Ensured string columns are strings.")

    # Ensure year and source_row_number are integer types (handle potential NaNs from coercion)
    if 'year' in df_error.columns:
         df_error['year'] = pd.to_numeric(df_error['year'], errors='coerce').astype('Int64') # Nullable Integer
    if 'source_row_number' in df_error.columns:
         df_error['source_row_number'] = pd.to_numeric(df_error['source_row_number'], errors='coerce').astype('Int64') # Nullable Integer
    print("Converted integer columns.")

    # 2. Select and Order Final Columns
    # Ensure only columns defined in FINAL_DELTA_COLUMNS that actually exist are kept
    final_columns_present = [col for col in FINAL_DELTA_COLUMNS if col in df_error.columns]
    df_final = df_error[final_columns_present].copy()
    print(f"Selected final columns for Delta table: {df_final.columns.tolist()}")


    # 3. Write to Quarantine Delta Table (Overwrite each time for simplicity)
    print(f"Writing final cleaned data to quarantine Delta table: {QUARANTINE_LAKEHOUSE_PATH}...")
    # Overwrite mode for quarantine table - processing one error file at a time effectively replaces content
    write_deltalake(
        QUARANTINE_LAKEHOUSE_PATH,
        df_final,
        mode='overwrite', # Overwrite the quarantine table each run
        schema_mode='overwrite' # Ensure schema matches the final DataFrame
    )
    print("Successfully wrote to quarantine Delta table.")

    # 4. Optional: Analyze errors
    if 'error_reason' in df_final.columns:
        print("\n--- Error Summary ---")
        print(df_final['error_reason'].value_counts())

    return True # Indicate success

# --- Main Execution ---
if __name__ == "__main__":
    print("--- Starting Error Rows Processing ---")

    # Ensure the processed directory exists
    os.makedirs(PROCESSED_ERROR_DIR, exist_ok=True)

    # Find error CSV files
    error_files = glob.glob(os.path.join(ERROR_INPUT_DIR, '*.csv'))

    if not error_files:
        print("No error CSV files found in 'error_input/'.")
    else:
        print(f"Found {len(error_files)} file(s) to process.")
        processed_count = 0
        failed_count = 0
        for file in error_files:
            if process_error_file(file):
                # Move successful file
                try:
                    processed_file_path = os.path.join(PROCESSED_ERROR_DIR, os.path.basename(file))
                    # Ensure the destination doesn't exist to avoid errors on retry
                    if os.path.exists(processed_file_path):
                         os.remove(processed_file_path)
                    shutil.move(file, processed_file_path)
                    print(f"Moved processed error file to: {processed_file_path}")
                    processed_count += 1
                except Exception as move_err:
                     print(f"ERROR moving file {os.path.basename(file)} after successful processing: {move_err}")
                     failed_count += 1
            else:
                # Keep failed file in input directory for review
                 print(f"File {os.path.basename(file)} failed processing due to errors and was NOT moved.")
                 failed_count += 1

        print(f"\nSuccessfully processed and moved {processed_count} error file(s).")
        if failed_count > 0:
            print(f"Failed to process or move {failed_count} error file(s). Please check logs and the '{ERROR_INPUT_DIR}' directory for files that were not moved.")

    print("--- Error Rows Processing Complete ---")

