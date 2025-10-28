import pandas as pd
from deltalake.writer import write_deltalake
from deltalake import DeltaTable # <-- Import DeltaTable class
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
# Output Delta table path (This will be partitioned by province)
FARMER_LAKEHOUSE_PATH = './lakehouse_data/farmer_registry'

# Expected columns in the raw XLSX file
EXPECTED_RAW_COLS = ['Municipality/Brgy', 'Count of Rice Farmers', 'Total Declared Rice Area']

# Regex to find the province name in the filename
# It looks for "RSBSA " followed by any characters (the province), and then " Rice Farmers"
FILENAME_PROVINCE_REGEX = re.compile(r"RSBSA (.*?) Rice Farmers", re.IGNORECASE)

def extract_province_from_filename(filename):
    """
    Extracts the province name from the filename using REGEX.
    e.g., "RSBSA Aklan Rice Farmers.xlsx" -> "AKLAN"
    """
    basename = os.path.basename(filename)
    match = FILENAME_PROVINCE_REGEX.search(basename)
    if match:
        return match.group(1).strip().upper()
    print(f"WARNING: Could not extract province from filename: {basename}")
    return "UNKNOWN"

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
    # Check if the name is non-empty, fully uppercase, and contains only letters/spaces/hyphens
    return bool(name_str) and name_str.isupper() and bool(re.match(r'^[A-Z\s-]+$', name_str))

def process_farmer_xlsx_to_delta(input_file_path, write_mode='append'):
    """
    Reads raw farmer XLSX, cleans municipality data, extracts province from filename,
    and writes to a partitioned Delta Lake table.
    Uses the specified write_mode ('append', 'overwrite', or 'dynamic_overwrite').
    """
    file_basename = os.path.basename(input_file_path)
    print(f"Processing farmer registry file: {file_basename}...")
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
        print(f"ERROR reading file {file_basename}: {e}")
        return False

    if df.empty:
        print("Skipping empty input file.")
        return True # Treat as success for moving file

    # --- Extract Province from Filename ---
    province_name = extract_province_from_filename(file_basename)
    if province_name == "UNKNOWN":
        print(f"ERROR: Could not determine province for {file_basename}. Skipping file.")
        return False # Failed processing
    print(f"Extracted province: {province_name}")

    # --- Check for expected raw columns ---
    missing_cols = [col for col in EXPECTED_RAW_COLS if col not in df.columns]
    if missing_cols:
        print(f"ERROR: Expected columns missing in {file_basename}:")
        for col in missing_cols: print(f"  - '{col}'")
        print("Stopping processing for this file.")
        return False

    # --- Filter for Municipality Rows ---
    municipality_rows = df[df.apply(is_municipality_row_strict, axis=1)].copy()

    if municipality_rows.empty:
        print(f"WARNING: No municipality rows identified in {file_basename}.")
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
    municipality_rows['province'] = province_name # Use extracted province
    final_df = municipality_rows[['province', 'municipality', 'registered_rice_farmers', 'total_declared_rice_area_ha']].copy()
    numeric_cols = ['registered_rice_farmers', 'total_declared_rice_area_ha']
    for col in numeric_cols:
        final_df[col] = pd.to_numeric(final_df[col], errors='coerce').fillna(0)

    print(f"Cleaned data preview:\n{final_df.head().to_string()}")

    # --- Write Directly to Delta Table ---
    
    # Ensure path is normalized for the OS
    safe_lakehouse_path = os.path.normpath(FARMER_LAKEHOUSE_PATH)
    print(f"Writing municipality data to Delta table: {safe_lakehouse_path}...")
    try:
        if write_mode == 'dynamic_overwrite':
            # --- Workaround for broken dt.delete() on partitions with spaces ---
            # Strategy: Read all *other* partitions, add this new data,
            # and do a full overwrite with the combined data.
            
            current_province = final_df['province'].iloc[0]
            print(f"Preparing for DYNAMIC OVERWRITE for province = '{current_province}'")

            df_to_write = final_df # Start with the new data
            
            # Check if table exists before trying to read
            if os.path.exists(safe_lakehouse_path):
                print(f"Loading existing table to read other partitions...")
                try:
                    dt = DeltaTable(safe_lakehouse_path)
                    # Load all data WHERE province != current_province
                    df_others = dt.to_pandas(filters=[("province", "!=", current_province)])
                    
                    if not df_others.empty:
                        print(f"Loaded {len(df_others)} rows from other partitions.")
                        # Combine old data (others) + new data (current)
                        # Use ignore_index=True to prevent duplicate index 'source.__index_level_0__' error
                        df_to_write = pd.concat([df_others, final_df], ignore_index=True)
                    else:
                        print(f"No data found for other partitions. Writing only new data.")
                
                except Exception as e:
                    print(f"Could not read existing table (may be empty or new): {e}")
                    print("Proceeding to write new data only.")
            
            print(f"Performing full table OVERWRITE with {len(df_to_write)} total rows to apply dynamic change...")
            
            # Perform a full 'overwrite' with the combined DataFrame
            write_deltalake(
                safe_lakehouse_path,
                df_to_write,
                mode='overwrite', # Use the overwrite mode that works
                schema_mode='overwrite', # Use older, compatible syntax
                partition_by=['province']
            )
            print(f"Successfully performed dynamic overwrite for {current_province}.")

        else:
            # This handles normal 'append' and the first file of 'overwrite'
            final_write_mode = write_mode
            schema_settings = {}
            if write_mode == 'overwrite':
                schema_settings['schema_mode'] = 'overwrite' # Use older, compatible syntax
            else:
                schema_settings['schema_mode'] = 'merge'

            write_deltalake(
                safe_lakehouse_path,
                final_df,
                mode=final_write_mode,
                partition_by=['province'],
                **schema_settings
            )
            print(f"Successfully wrote data using mode: {final_write_mode}")

        return True # Indicate success
    
    except Exception as e:
        print(f"ERROR writing to Delta Lake table {safe_lakehouse_path}: {e}")
        # Check if it's the known predicate error and give advice
        if "Predicate" in str(e) or "filter" in str(e) or "delete" in str(e):
             print("\n--------------")
             print("HINT: If this error involves a partition with spaces (e.g., 'NEGROS OCCIDENTAL'),")
             print("your version of 'deltalake' may have a bug in its delete function.")
             print("Try upgrading the library:")
             print("pip install --upgrade deltalake")
             print("--------------\n")
        return False

# --- Main Execution ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Clean farmer registry XLSX files and load municipality data into a partitioned Delta Lake table.")
    parser.add_argument('--mode', type=str, choices=['overwrite', 'append', 'dynamic_overwrite'], default='append',
                        help="Write mode for Delta Lake: \n"
                             "'append' (default): Add new data. \n"
                             "'overwrite': Replace entire table with the first file processed. \n"
                             "'dynamic_overwrite': Replace only the partition matching the file's province.")
    args = parser.parse_args()

    # Use the mode specified by the user (defaults to 'append')
    selected_mode = args.mode
    print(f"--- Starting Farmer Registry Processing (XLSM -> Delta) ---")
    print(f"--- Mode selected: {selected_mode.upper()} ---")


    # Ensure directories exist
    os.makedirs(FARMER_INPUT_DIR, exist_ok=True)
    os.makedirs(PROCESSED_FARMER_DIR, exist_ok=True) # Directory for processed XLSX
    os.makedirs(os.path.dirname(FARMER_LAKEHOUSE_PATH), exist_ok=True)

    # Find raw farmer registry XLSX files using the regex pattern
    # This is safer than a simple glob, but we can use glob for simplicity
    raw_files = glob.glob(os.path.join(FARMER_INPUT_DIR, 'RSBSA * Rice Farmers*.xlsx'))

    processed_count = 0
    failed_count = 0
    if not raw_files:
        print(f"No 'RSBSA ... Rice Farmers*.xlsx' files found in '{FARMER_INPUT_DIR}'.")
    else:
        print(f"Found {len(raw_files)} XLSX file(s) to process.")
        
        # Determine the initial write mode
        # 'overwrite' only applies to the *first* file.
        # 'append' and 'dynamic_overwrite' apply to *all* files.
        current_write_mode = selected_mode

        for i, input_file in enumerate(raw_files):
            # If mode is 'overwrite', only the first file overwrites. Subsequent files must append.
            if selected_mode == 'overwrite' and i > 0:
                current_write_mode = 'append'
                print(f"Switching to 'append' mode for subsequent files.")

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
                     failed_count += 1 # Count as failed if move fails
            else:
                 print(f"File {os.path.basename(input_file)} failed processing and was NOT moved.")
                 failed_count += 1

    print(f"\nSuccessfully processed and moved {processed_count} XLSX file(s).")
    if failed_count > 0:
        print(f"Failed to process or move {failed_count} file(s). Please check logs.")
    print("--- Farmer Registry Processing Complete ---")


