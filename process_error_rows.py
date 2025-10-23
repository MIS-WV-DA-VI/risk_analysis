import pandas as pd
import duckdb
from deltalake.writer import write_deltalake
import os
import shutil
import glob
import re # Import regular expression module

# --- Configuration ---
# Use relative paths, assuming script runs from project root
ERROR_INPUT_DIR = './error_input/'
PROCESSED_ERROR_DIR = os.path.join(ERROR_INPUT_DIR, 'processed')
QUARANTINE_LAKEHOUSE_PATH = './lakehouse_data/quarantined_disasters'

# Define the essential raw column names expected in the error CSV
# Adjust these based on the ACTUAL columns required for mapping/cleaning
ESSENTIAL_COLUMNS_RAW = [
    'YEAR (DATE OF OCCURENCE)',
    'PROVINCE AFFECTED',
    'MUNICIPALITY AFFECTED',
    'COMMODITY',
    'HYDROMETEOROLOGICAL EVENTS / GENERAL DISASTER EVENTS',
    'NAME OF DISASTER',
    'Partially Damaged (AREA AFFECTED (HA.) / MORTALITY HEADS / NO. OF UNITS AFFECTED)',
    'Totally Damaged (AREA AFFECTED (HA.) / MORTALITY HEADS / NO. OF UNITS AFFECTED)',
    'TOTAL (AREA AFFECTED (HA.) / MORTALITY HEADS / NO. OF UNITS AFFECTED)',
    'NUMBER OF FARMERS AFFECTED',
    'Total Value (Based on Cost of Production / Inputs)',
    'Total Value - Based on Farm Gate Price ', # Note trailing space
    'GRAND TOTAL',
    'source_row_number',
    'error_reason'
]


def clean_column_names(df):
    """Cleans column names by removing special chars, lowercasing, and replacing spaces."""
    new_columns = {}
    for col in df.columns:
        new_col = col.lower()
        # Remove content within parentheses, including parentheses
        new_col = re.sub(r'\s*\(.*\)\s*', '', new_col).strip()
        # Remove special characters like '/' and extra spaces, replace with underscore
        new_col = re.sub(r'[^a-z0-9\s]+', '', new_col)
        new_col = re.sub(r'\s+', '_', new_col).strip('_')
        new_columns[col] = new_col
    df = df.rename(columns=new_columns)
    return df

def parse_date_range(date_str):
    """Attempts to parse various date string formats into start/end dates."""
    if pd.isna(date_str):
        return pd.NaT, pd.NaT

    date_str = str(date_str).strip()

    # Simple cases first (e.g., "YYYY-MM-DD" or similar standard formats)
    try:
        dt = pd.to_datetime(date_str)
        return dt, dt # Assume single day if parsable directly
    except ValueError:
        pass # Continue to more complex parsing

    # Case: "Month Day-Day, Year" (e.g., "July 16- August 11, 2025") - simplified, needs robust parsing
    # This is complex due to potential month changes. For simplicity, we might take the start date.
    # A more robust solution would involve regex and dateutil library.
    # Placeholder: Extract first recognizable date part
    match = re.search(r'(\w+\s+\d+)[,-]?.*(\d{4})', date_str)
    if match:
        try:
            start_part = f"{match.group(1)}, {match.group(2)}"
            dt_start = pd.to_datetime(start_part)
            # Cannot reliably get end date without complex logic
            return dt_start, pd.NaT
        except ValueError:
            pass

    # Add more specific regex patterns for other observed formats if needed

    # Fallback if no pattern matches
    return pd.NaT, pd.NaT

def process_error_file(file_path):
    """Reads, cleans, and processes a single error CSV file."""
    print(f"Processing error file: {os.path.basename(file_path)}...")
    try:
        df_error = pd.read_csv(file_path)
    except FileNotFoundError:
        print(f"ERROR: File not found: {file_path}")
        return False
    except Exception as e:
        print(f"ERROR reading file {os.path.basename(file_path)}: {e}")
        return False

    if df_error.empty:
        print("Skipping empty file.")
        return True # Treat as success for moving file

    # --- <<< START: COLUMN CHECK >>> ---
    print("Checking for essential columns...")
    missing_cols = [col for col in ESSENTIAL_COLUMNS_RAW if col not in df_error.columns]
    if missing_cols:
        print(f"ERROR: Essential columns missing in {os.path.basename(file_path)}:")
        for col in missing_cols:
            print(f"  - '{col}'")
        print("Stopping processing for this file.")
        return False # Indicate failure, do not move the file
    print("All essential columns found.")
    # --- <<< END: COLUMN CHECK >>> ---


    # 1. Clean Column Names
    df_error = clean_column_names(df_error)
    print("Cleaned column names.")

    # 2. Rename columns to match the target 'lakehouse_disasters' schema
    # Adjust source column names based on `clean_column_names` output
    rename_map = {
        'year_date_of_occurence': 'year',
        'province_affected': 'province',
        'municipality_affected': 'municipality',
        'commodity': 'commodity', # Keep as is if name matches after cleaning
        'hydrometeorological_events_general_disaster_events': 'disaster_category', # Map from the general category
        'name_of_disaster': 'disaster_name',
        'partially_damaged_area_affected_ha_mortality_heads_no_of_units_affected': 'area_partially_damaged_ha',
        'totally_damaged_area_affected_ha_mortality_heads_no_of_units_affected': 'area_totally_damaged_ha',
        'total_area_affected_ha_mortality_heads_no_of_units_affected': 'area_total_affected_ha',
        'number_of_farmers_affected': 'farmers_affected',
        'total_value_based_on_cost_of_production_inputs': 'losses_php_production_cost',
        'total_value_based_on_farm_gate_price': 'losses_php_farm_gate', # Ensure trailing space handled
        'grand_total': 'losses_php_grand_total',
        'source_row_number': 'source_row_number', # Keep original identifiers
        'error_reason': 'error_reason'          # Keep error information
        # Add 'event_date_start' and 'event_date_end' later after parsing
    }

    # Apply renaming, only keep columns that exist in the rename_map's keys or values we want to keep
    # Make sure keys in rename_map EXACTLY match columns AFTER clean_column_names()
    df_error = df_error.rename(columns=rename_map)

    # Filter columns: keep only those that are now in the TARGET schema + identifiers/errors
    target_columns = list(rename_map.values()) + ['event_date_start', 'event_date_end']
    df_error = df_error[[col for col in target_columns if col in df_error.columns]]
    print("Renamed columns.")


    # 3. Clean/Parse Data
    print("Cleaning and parsing data types...")
    # Parse dates (this is complex due to varying formats)
    # Apply the custom parser to the 'actual_date_of_occurence' column
    if 'actual_date_of_occurence' in df_error.columns:
        dates = df_error['actual_date_of_occurence'].apply(parse_date_range)
        df_error['event_date_start'] = dates.apply(lambda x: x[0])
        df_error['event_date_end'] = dates.apply(lambda x: x[1])
        # Drop the original date column if no longer needed
        # df_error = df_error.drop(columns=['actual_date_of_occurence'])
    else:
        print("Warning: 'actual_date_of_occurence' column not found for date parsing.")
        df_error['event_date_start'] = pd.NaT
        df_error['event_date_end'] = pd.NaT


    # Convert numeric columns, coercing errors to NaN
    numeric_cols = [
        'area_partially_damaged_ha', 'area_totally_damaged_ha', 'area_total_affected_ha',
        'farmers_affected', 'losses_php_production_cost', 'losses_php_farm_gate',
        'losses_php_grand_total'
    ]
    for col in numeric_cols:
        if col in df_error.columns:
            df_error[col] = pd.to_numeric(df_error[col], errors='coerce')
        else:
             print(f"Warning: Expected numeric column '{col}' not found after renaming.")


    # Ensure essential string columns are strings
    string_cols = ['province', 'municipality', 'commodity', 'disaster_category', 'disaster_name', 'error_reason']
    for col in string_cols:
         if col in df_error.columns:
            df_error[col] = df_error[col].astype(str).fillna('Unknown')


    # Ensure year is integer (handle potential NaNs from coercion)
    if 'year' in df_error.columns:
         df_error['year'] = pd.to_numeric(df_error['year'], errors='coerce').fillna(0).astype(int)


    print(f"Cleaned data types. Final columns: {df_error.columns.tolist()}")


    # 4. Write to Quarantine Delta Table (Overwrite each time for simplicity)
    print(f"Writing cleaned data to quarantine Delta table: {QUARANTINE_LAKEHOUSE_PATH}...")
    if os.path.exists(QUARANTINE_LAKEHOUSE_PATH):
        shutil.rmtree(QUARANTINE_LAKEHOUSE_PATH)
    write_deltalake(
        QUARANTINE_LAKEHOUSE_PATH,
        df_error,
        mode='overwrite', # Overwrite the quarantine table each run
        schema_mode='overwrite' # Ensure schema matches the processed DataFrame
    )
    print("Successfully wrote to quarantine Delta table.")

    # 5. Optional: Analyze errors
    if 'error_reason' in df_error.columns:
        print("\n--- Error Summary ---")
        print(df_error['error_reason'].value_counts())

    return True # Indicate success

# --- Main Execution ---
if __name__ == "__main__":
    print("--- Starting Error Rows Processing ---")

    # Ensure the processed directory exists
    os.makedirs(PROCESSED_ERROR_DIR, exist_ok=True)

    # Find error CSV files (adjust pattern if needed)
    error_files = glob.glob(os.path.join(ERROR_INPUT_DIR, '*.csv'))

    if not error_files:
        print("No error CSV files found in 'error_input/'.")
    else:
        processed_count = 0
        failed_count = 0
        for file in error_files:
            if process_error_file(file):
                # Move successful file
                try:
                    processed_file_path = os.path.join(PROCESSED_ERROR_DIR, os.path.basename(file))
                    shutil.move(file, processed_file_path)
                    print(f"Moved processed error file to: {processed_file_path}")
                    processed_count += 1
                except Exception as move_err:
                     print(f"ERROR moving file {os.path.basename(file)} after processing: {move_err}")
                     failed_count += 1 # Count as failure if move fails
            else:
                # Keep failed file in input directory for review
                 print(f"File {os.path.basename(file)} failed processing and was NOT moved.")
                 failed_count += 1

        print(f"\nProcessed {processed_count} error files successfully.")
        if failed_count > 0:
            print(f"Failed to process or move {failed_count} error files. Please check logs and the '{ERROR_INPUT_DIR}' directory.")

    print("--- Error Rows Processing Complete ---")

