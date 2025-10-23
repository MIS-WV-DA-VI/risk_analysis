import pandas as pd
from deltalake.writer import write_deltalake
import os
import shutil
import glob
import re
import argparse # Keep argparse

# --- Configuration ---
# Input directory for the original XLSX files
FARMER_INPUT_DIR = './farmer_registry_input/'
# Directory to move processed XLSX files
PROCESSED_FARMER_DIR = os.path.join(FARMER_INPUT_DIR, 'processed')
# Output Delta table path
FARMER_LAKEHOUSE_PATH = './lakehouse_data/farmer_registry'

# Expected columns in the raw XLSX file
EXPECTED_RAW_COLS = ['Municipality/Brgy', 'Count of Rice Farmers', 'Total Declared Rice Area']

def clean_municipality_name_strict(name):
    """Cleans municipality names: Uppercase, strip whitespace."""
    if pd.isna(name):
        return None
    name = str(name).strip().upper()
    return name

def is_municipality_row_strict(row):
    """Heuristic to identify municipality rows based on ALL CAPS name."""
    name_col = 'Municipality/Brgy'
    count_col = 'Count of Rice Farmers'
    area_col = 'Total Declared Rice Area'

    # Check if necessary columns exist and have values
    if name_col not in row or pd.isna(row[name_col]): return False
    if count_col not in row or pd.isna(row[count_col]): return False
    if area_col not in row or pd.isna(row[area_col]): return False

    name_str = str(row[name_col]).strip()
    # Check if the name is non-empty, fully uppercase, and contains only letters/spaces
    return bool(name_str) and name_str.isupper() and bool(re.match(r'^[A-Z\s]+$', name_str))

def process_farmer_xlsx_to_delta(input_file_path, write_mode='append'):
    """
    Reads raw farmer XLSX, cleans municipality data, and writes directly to Delta Lake.
    Uses the specified write_mode ('append' or 'overwrite').
    """
    print(f"Processing farmer registry file: {os.path.basename(input_file_path)}...")
    try:
        # Read Excel file
        # Make sure column names are stripped during read
        df = pd.read_excel(input_file_path, sheet_name=0, engine='openpyxl')
        df.columns = [str(col).strip() for col in df.columns]

    except FileNotFoundError:
        print(f"ERROR: Input file not found: {input_file_path}")
        return False
    except ImportError:
        print("ERROR: 'openpyxl' library not found. Please install it: pip install openpyxl")
        return False
    except Exception as e:
        print(f"ERROR reading file {os.path.basename(input_file_path)}: {e}")
        return False

    if df.empty:
        print("Skipping empty input file.")
        return True # Treat as success for moving file

    # --- Check for expected raw columns ---
    missing_cols = [col for col in EXPECTED_RAW_COLS if col not in df.columns]
    if missing_cols:
        print(f"ERROR: Expected columns missing in {os.path.basename(input_file_path)}:")
        for col in missing_cols: print(f"  - '{col}'")
        print("Stopping processing for this file.")
        return False

    # --- Filter for Municipality Rows ---
    municipality_rows = df[df.apply(is_municipality_row_strict, axis=1)].copy()

    if municipality_rows.empty:
        print(f"WARNING: No municipality rows identified in {os.path.basename(input_file_path)}.")
        return True # Treat as success (nothing to process), move the file

    print(f"Extracted {len(municipality_rows)} municipality rows.")

    # --- Clean and Transform ---
    print("Cleaning and transforming data...")
    municipality_rows = municipality_rows.rename(columns={
        'Municipality/Brgy': 'municipality',
        'Count of Rice Farmers': 'registered_rice_farmers',
        'Total Declared Rice Area': 'total_declared_rice_area_ha'
    })
    municipality_rows['municipality'] = municipality_rows['municipality'].apply(clean_municipality_name_strict)
    municipality_rows['province'] = 'AKLAN' # Assuming Aklan
    final_df = municipality_rows[['province', 'municipality', 'registered_rice_farmers', 'total_declared_rice_area_ha']].copy()
    numeric_cols = ['registered_rice_farmers', 'total_declared_rice_area_ha']
    for col in numeric_cols:
        final_df[col] = pd.to_numeric(final_df[col], errors='coerce').fillna(0)

    print(f"Cleaned data preview:\n{final_df.head().to_string()}")

    # --- Write Directly to Delta Table ---
    print(f"Writing municipality data to Delta table: {FARMER_LAKEHOUSE_PATH} using mode '{write_mode}'...")
    try:
        # Determine schema settings based on mode
        schema_mode = 'overwrite' if write_mode == 'overwrite' else 'merge'
        # overwrite_schema_flag = True if write_mode == 'overwrite' else False # No longer needed

        write_deltalake(
            FARMER_LAKEHOUSE_PATH,
            final_df,
            mode=write_mode,
            schema_mode=schema_mode # Use 'merge' for append, 'overwrite' for overwrite
            # overwrite_schema argument removed
        )
        print(f"Successfully wrote to farmer registry Delta table using mode '{write_mode}'.")
        return True # Indicate success
    except Exception as e:
        print(f"ERROR writing to Delta Lake table {FARMER_LAKEHOUSE_PATH}: {e}")
        return False

# --- Main Execution ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Clean Aklan farmer registry XLSX and load municipality data directly into Delta Lake.")
    parser.add_argument('--mode', type=str, choices=['overwrite', 'append'], default='append',
                        help="Write mode for Delta Lake: 'overwrite' to replace table, 'append' (default) to add data.")
    args = parser.parse_args()

    # Use the mode specified by the user (defaults to 'append')
    selected_mode = args.mode
    print(f"--- Starting Farmer Registry Processing (XLSX -> Delta) ---")
    print(f"--- Mode selected: {selected_mode.upper()} ---")


    # Ensure directories exist
    os.makedirs(FARMER_INPUT_DIR, exist_ok=True)
    os.makedirs(PROCESSED_FARMER_DIR, exist_ok=True) # Directory for processed XLSX
    os.makedirs(os.path.dirname(FARMER_LAKEHOUSE_PATH), exist_ok=True)

    # Find raw farmer registry XLSX files
    raw_files = glob.glob(os.path.join(FARMER_INPUT_DIR, 'RSBSA Aklan Rice Farmers*.xlsx'))

    processed_count = 0
    failed_count = 0
    if not raw_files:
        print(f"No raw farmer registry XLSX files found in '{FARMER_INPUT_DIR}'.")
    else:
        print(f"Found {len(raw_files)} XLSX file(s) to process.")
        # Determine the initial write mode (overwrite only applies to the first file processed in a run if specified)
        current_write_mode = selected_mode

        for i, input_file in enumerate(raw_files):
            # If mode is overwrite, only the first file overwrites. Subsequent files append.
            if selected_mode == 'overwrite' and i > 0:
                current_write_mode = 'append'

            if process_farmer_xlsx_to_delta(input_file, write_mode=current_write_mode):
                # Move successful XLSX file
                try:
                    processed_file_path = os.path.join(PROCESSED_FARMER_DIR, os.path.basename(input_file))
                    if os.path.exists(processed_file_path): os.remove(processed_file_path)
                    shutil.move(input_file, processed_file_path)
                    print(f"Moved processed XLSX file to: {processed_file_path}")
                    processed_count += 1
                except Exception as move_err:
                     print(f"ERROR moving file {os.path.basename(input_file)} after successful processing: {move_err}")
                     failed_count += 1
            else:
                 print(f"File {os.path.basename(input_file)} failed processing and was NOT moved.")
                 failed_count += 1

    print(f"\nSuccessfully processed and moved {processed_count} XLSX file(s).")
    if failed_count > 0:
        print(f"Failed to process or move {failed_count} file(s). Please check logs.")
    print("--- Farmer Registry Processing Complete ---")