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
    """Extracts province name from filename using regex."""
    match = FILENAME_PROVINCE_REGEX.search(filename)
    if match:
        province_name = match.group(1).strip().upper()
        print(f"Extracted province: {province_name}")
        return province_name
    else:
        print(f"WARNING: Could not extract province from filename: {filename}. Defaulting to 'UNKNOWN'.")
        return 'UNKNOWN'

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
    Uses the specified write_mode ('append', 'overwrite', or 'dynamic_overwrite').
    """
    file_basename = os.path.basename(input_file_path)
    print(f"Processing farmer registry file: {file_basename}...")

    # --- Extract Province from Filename ---
    province_name = extract_province_from_filename(file_basename)
    if province_name == 'UNKNOWN':
        print(f"ERROR: Halting processing for {file_basename} due to unknown province.")
        return False # Fail processing if province can't be identified

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
    
    # --- Delta Write Configuration ---
    # These variables will be set based on the write_mode
    final_write_mode = 'append' # The mode passed to deltalake library
    partition_filters = None
    schema_mode = 'merge' # Default is 'merge' (safe for append/dynamic)
    
    current_province = final_df['province'].iloc[0].upper() # Should match province_name

    if write_mode == 'overwrite':
        # Full Table Overwrite: Wipes everything.
        final_write_mode = 'overwrite'
        schema_mode = 'overwrite' # Overwrite schema as well
        print(f"Writing data... Mode: FULL TABLE OVERWRITE (using data from {current_province})")
    
    elif write_mode == 'dynamic_overwrite':
        # Dynamic Partition Overwrite: Wipes only this province's partition.
        # We will use a Delete + Append strategy for broader compatibility
        final_write_mode = 'dynamic_overwrite' # Custom flag, not passed to deltalake
        schema_mode = 'merge'
        # partition_filters is no longer used here
        print(f"Preparing for: DYNAMIC PARTITION OVERWRITE for province = '{current_province}'")

    else: # 'append'
        # Standard Append: Adds data to the partition.
        final_write_mode = 'append'
        schema_mode = 'merge'
        print(f"Writing data... Mode: APPEND to province = '{current_province}'")
    # --- End Configuration ---

    print(f"Writing municipality data to Delta table: {FARMER_LAKEHOUSE_PATH}...")
    try:
        if write_mode == 'dynamic_overwrite':
            # --- Robust Dynamic Partition Overwrite (Delete + Append) ---
            # This method works across more library versions

            # Ensure the table exists before trying to load/delete from it
            if not os.path.exists(FARMER_LAKEHOUSE_PATH):
                 print("Table does not exist. Writing data for the first time (as append)...")
                 # Table doesn't exist, so we just do a normal initial write
                 write_deltalake(
                     FARMER_LAKEHOUSE_PATH,
                     final_df,
                     mode='append', # Start with append
                     partition_by=['province'],
                     schema_mode='merge'
                 )
            else:
                print(f"Loading Delta table at: {FARMER_LAKEHOUSE_PATH}")
                dt = DeltaTable(FARMER_LAKEHOUSE_PATH)
                
                # --- NEW: Safety check before deleting ---
                print(f"Checking for existing data in partition: {current_province}")
                try:
                    # Query to see if any data already exists for this partition
                    existing_data = dt.to_pandas(
                        filters=[('province', '=', current_province)],
                        columns=['province'] # Only need one column to check
                    )
                except Exception as e:
                    print(f"Could not query existing data (table might be new or empty): {e}")
                    existing_data = pd.DataFrame() # Assume empty

                if not existing_data.empty:
                    # --- FIX for spaces in predicate ---
                    # Use "IN ('VALUE')" syntax, which is often more robustly parsed
                    # than " = 'VALUE' " for strings with spaces.
                    delete_predicate = f"province IN ('{current_province}')"
                    print(f"Deleting existing data for partition using predicate: {delete_predicate}")
                    # 1. Delete the partition
                    dt.delete(predicate=delete_predicate)
                else:
                    print(f"No existing data found for {current_province}. Skipping delete step.")
                
                print(f"Appending new data for partition: {current_province}")
                # 2. Append the new data
                # FIX: Use the top-level write_deltalake() function instead of dt.write()
                write_deltalake(
                    FARMER_LAKEHOUSE_PATH, # Pass the table path
                    final_df,
                    mode='append',
                    schema_mode='merge',
                    partition_by=['province'] # Re-specify partition_by for the writer
                )
            print(f"Successfully performed dynamic overwrite for {current_province}.")

        else:
            # --- Handle Full Overwrite or Standard Append ---
            # The original write_deltalake function is fine for this.
            # Note: partition_filters is None for these modes.
            write_deltalake(
                FARMER_LAKEHOUSE_PATH,
                final_df,
                mode=final_write_mode, # This will be 'overwrite' or 'append'
                partition_by=['province'], # partition_by is required for initial write
                schema_mode=schema_mode
                # No partition_filters here
            )
            print(f"Successfully wrote data using mode: {final_write_mode}")

        return True # Indicate success
    
    except AttributeError as e:
        # Catch the specific error we just saw
        if "'DeltaTable' object has no attribute 'write'" in str(e):
             print("\n--- HINT ---")
             print("Caught the 'no attribute 'write'' error. This is a library version issue.")
             print("I have updated the script to use the top-level 'write_deltalake()' function instead.")
             print("Please try running the script again.")
             print("--------------\n")
        else:
             # Re-raise other AttributeErrors
             print(f"ERROR (AttributeError): {e}")
        return False
    except Exception as e:
        print(f"ERROR writing to Delta Lake table {FARMER_LAKEHOUSE_PATH}: {e}")
        # Add specific advice for the error we saw
        if "unexpected keyword argument 'partition_filters'" in str(e):
             print("\n--- HINT ---")
             print("The error confirms your 'deltalake' library version is too old for the original method.")
             print("I have updated the script to use a more compatible (Delete then Append) method.")
             print("Please try running the script again.")
             print("If it still fails, please upgrade your library with:")
             print("pip install --upgrade deltalake")
             print("--------------\n")
        return False

# --- Main Execution ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Clean farmer registry XLSX and load municipality data into a partitioned Delta Lake table.")
    parser.add_argument('--mode', type=str, choices=['overwrite', 'append', 'dynamic_overwrite'], default='append',
                        help="Write mode for Delta Lake: "
                             "'append' (default): Add new data to partitions. "
                             "'overwrite': Wipe the *entire table* with the first file, then append others. "
                             "'dynamic_overwrite': Overwrite *only* the specific partitions for the files being processed.")
    args = parser.parse_args()

    # Use the mode specified by the user
    selected_mode = args.mode
    print(f"--- Starting Farmer Registry Processing (XLSX -> Delta) ---")
    print(f"--- Mode selected: {selected_mode.upper()} ---")


    # Ensure directories exist
    os.makedirs(FARMER_INPUT_DIR, exist_ok=True)
    os.makedirs(PROCESSED_FARMER_DIR, exist_ok=True) # Directory for processed XLSX
    # No need to create FARMER_LAKEHOUSE_PATH, write_deltalake does it

    # Find raw farmer registry XLSX files
    # Looks for "RSBSA " followed by anything, then " Rice Farmers*.xlsx"
    raw_files_pattern = os.path.join(FARMER_INPUT_DIR, 'RSBSA * Rice Farmers*.xlsx')
    raw_files = glob.glob(raw_files_pattern)

    processed_count = 0
    failed_count = 0
    if not raw_files:
        print(f"No raw farmer registry XLSX files matching pattern '{raw_files_pattern}' found.")
    else:
        print(f"Found {len(raw_files)} XLSX file(s) to process.")
        
        for i, input_file in enumerate(raw_files):
            # Determine the write mode for *this specific file*
            current_write_mode = selected_mode

            # Special handling for 'overwrite': only first file overwrites, rest append
            if selected_mode == 'overwrite' and i > 0:
                current_write_mode = 'append'
                print(f"File {i+1}: Switched to 'append' mode after initial overwrite.")

            # For 'append' or 'dynamic_overwrite', the mode is the same for all files
            
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




