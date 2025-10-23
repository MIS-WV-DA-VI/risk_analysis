import argparse
import pandas as pd
import duckdb
from deltalake import write_deltalake
import os
import glob # Need glob to find files
import shutil # Need shutil to move files

# --- Configuration ---
# Using relative paths for local execution
BASE_DIR = '.' # Current directory
RAW_DATA_DIR = os.path.join(BASE_DIR, 'raw_data')
PROCESSED_DATA_DIR = os.path.join(RAW_DATA_DIR, 'processed')
LAKEHOUSE_PATH = os.path.join(BASE_DIR, 'lakehouse_data/lakehouse_disasters') # Main clean data table
API_OUTPUT_FILE = os.path.join(BASE_DIR, 'api_output/api_data.json')
DUCKDB_FILE = os.path.join(BASE_DIR, 'lakehouse_data/analysis_db.duckdb')


def handle_import(mode_override: str = None):
    """
    Handles importing ALL unprocessed CSV files found in the RAW_DATA_DIR
    into the MAIN Delta Lake table using APPEND mode by default.
    Moves processed files to the PROCESSED_DATA_DIR.
    An 'overwrite' mode can be forced via command-line for initial setup or resets.
    """
    print(f"--- Starting Main Data Import ---")

    # Ensure directories exist
    os.makedirs(RAW_DATA_DIR, exist_ok=True)
    os.makedirs(PROCESSED_DATA_DIR, exist_ok=True)
    os.makedirs(os.path.dirname(LAKEHOUSE_PATH), exist_ok=True)
    os.makedirs(os.path.dirname(API_OUTPUT_FILE), exist_ok=True)
    os.makedirs(os.path.dirname(DUCKDB_FILE), exist_ok=True)


    print(f"Scanning for new CSV files in: {RAW_DATA_DIR}")
    csv_files = glob.glob(os.path.join(RAW_DATA_DIR, '*.csv'))

    if not csv_files:
        print("No new CSV files found in raw_data to import.")
        print("--- Main Import Complete ---")
        return

    print(f"Found {len(csv_files)} new CSV files to process.")
    write_mode = mode_override if mode_override else 'append'
    print(f"Using write mode: {write_mode.upper()}")

    if write_mode == 'overwrite' and csv_files:
         if os.path.exists(LAKEHOUSE_PATH):
             print(f"Overwrite mode selected. Removing existing table at {LAKEHOUSE_PATH}...")
             shutil.rmtree(LAKEHOUSE_PATH)

    processed_count = 0
    for file_path in csv_files:
        print(f"\nProcessing file: {os.path.basename(file_path)}...")
        try:
            # Explicitly set low_memory=False for potentially mixed types
            df = pd.read_csv(file_path, low_memory=False)
            if df.empty:
                print("Skipping empty file.")
                shutil.move(file_path, os.path.join(PROCESSED_DATA_DIR, os.path.basename(file_path)))
                print("Moved empty file to processed directory.")
                continue

            # Standard cleaning for the main dataset
            print("Cleaning data...")
            df['event_date_start'] = pd.to_datetime(df['event_date_start'], errors='coerce')
            df['event_date_end'] = pd.to_datetime(df['event_date_end'], errors='coerce')
            df['municipality'] = df['municipality'].fillna('Unknown').astype(str) # Fill and ensure string

            # Ensure other key string cols are strings
            str_cols = ['province', 'commodity', 'disaster_category', 'disaster_name', 'disaster_type_raw', 'sanitation_remarks']
            for col in str_cols:
                if col in df.columns:
                    df[col] = df[col].fillna('Unknown').astype(str)

            # Convert numeric columns safely
            numeric_cols = [
                'year', # Year should also be numeric
                'area_partially_damaged_ha', 'area_totally_damaged_ha',
                'area_total_affected_ha', 'farmers_affected',
                'losses_php_production_cost', 'losses_php_farm_gate',
                'losses_php_grand_total'
            ]
            for col in numeric_cols:
                 if col in df.columns: # Check if column exists before converting
                    df[col] = pd.to_numeric(df[col], errors='coerce')

            # Fill remaining NaNs after numeric conversion with 0 (use with caution)
            df.fillna(0, inplace=True)
            print(f"Read and cleaned {len(df)} rows.")

            # Write to MAIN Delta Lake
            current_write_mode = write_mode if processed_count == 0 and write_mode == 'overwrite' else 'append'
            safe_lakehouse_path = os.path.normpath(LAKEHOUSE_PATH)
            write_deltalake(
                safe_lakehouse_path,
                df,
                mode=current_write_mode,
                schema_mode='merge'
            )
            print(f"Successfully wrote data to MAIN Delta table using {current_write_mode.upper()} mode.")

            # Move processed file
            processed_file_path = os.path.join(PROCESSED_DATA_DIR, os.path.basename(file_path))
            if os.path.exists(processed_file_path):
                os.remove(processed_file_path)
            shutil.move(file_path, processed_file_path)
            print(f"Moved processed file to: {processed_file_path}")
            processed_count += 1

        except Exception as e:
            print(f"ERROR processing file {os.path.basename(file_path)}: {e}")
            print("Skipping this file and continuing...")

    print(f"\nProcessed {processed_count} files for main table.")
    print("--- Main Import Complete ---")


def handle_export():
    """
    Runs the analysis query on the MAIN Delta Lake table using DuckDB
    and exports the results to a JSON file for the API. Uses a persistent DB file.
    Tries both read_delta and delta_scan function names.
    """
    print("--- Starting Export from Main Table ---")
    os.makedirs(os.path.dirname(API_OUTPUT_FILE), exist_ok=True)
    os.makedirs(os.path.dirname(DUCKDB_FILE), exist_ok=True)

    safe_lakehouse_path = os.path.normpath(LAKEHOUSE_PATH)

    if not os.path.exists(safe_lakehouse_path):
        print(f"ERROR: Main Lakehouse table not found at '{safe_lakehouse_path}'.")
        print("Please run the 'import' command first.")
        print("--- Export Failed ---")
        return

    print(f"Connecting to DuckDB file: {DUCKDB_FILE}...")
    con = None
    try:
        con = duckdb.connect(database=DUCKDB_FILE, read_only=False)
        print(f"DuckDB connection established. DuckDB version: {duckdb.__version__}") # Print version

        # --- Load Delta Extension ---
        print("Attempting to load DuckDB delta extension...")
        try:
            con.load_extension('delta')
            print("Delta extension loaded successfully (likely already installed).")
        except (duckdb.IOException, duckdb.CatalogException) as load_error1:
            print(f"Initial load failed ({load_error1}). Attempting to install...")
            try:
                con.install_extension('delta')
                print("Delta extension installed successfully.")
                con.load_extension('delta')
                print("Delta extension loaded successfully after install.")
            except (duckdb.IOException, duckdb.CatalogException) as install_error:
                 print(f"FATAL: Failed to install or load delta extension: {install_error}")
                 raise install_error

        # --- Determine Correct Delta Function Name ---
        delta_read_function = None
        print("Checking available Delta read functions...")
        try:
            # Check for read_delta first
            functions_rd = con.sql("SELECT function_name FROM duckdb_functions() WHERE function_name = 'read_delta'").df()
            if not functions_rd.empty:
                delta_read_function = 'read_delta'
                print("Found function: 'read_delta'")
            else:
                # If read_delta not found, check for delta_scan
                print("'read_delta' not found. Checking for 'delta_scan'...")
                functions_ds = con.sql("SELECT function_name FROM duckdb_functions() WHERE function_name = 'delta_scan'").df()
                if not functions_ds.empty:
                    delta_read_function = 'delta_scan'
                    print("Found function: 'delta_scan'")
                else:
                    print("FATAL: Neither 'read_delta' nor 'delta_scan' function found after loading extension!")
                    raise duckdb.CatalogException("Delta read function not available.")

        except Exception as check_err:
             print(f"FATAL: Error checking for Delta read functions: {check_err}")
             raise check_err

        # --- Create View using the determined function name ---
        print(f"Creating temporary view 'disasters_view' using '{delta_read_function}'...")
        con.sql(f"""
            CREATE OR REPLACE TEMPORARY VIEW disasters_view AS
            SELECT * FROM {delta_read_function}('{safe_lakehouse_path}');
        """)
        print("Temporary view created.")

        # --- Execute Analysis Query on the View ---
        analysis_sql = f"""
        SELECT
            province,
            disaster_category,
            SUM(losses_php_grand_total) AS total_losses_php,
            SUM(farmers_affected) AS total_farmers_affected,
            COUNT(*) AS number_of_events
        FROM disasters_view
        GROUP BY province, disaster_category
        ORDER BY total_losses_php DESC
        LIMIT 1000
        """

        print("Executing analysis query on view...")
        results_df = con.sql(analysis_sql).to_df()
        print("Analysis complete.")

        # --- Process and Save Results ---
        results_df['total_losses_php'] = results_df['total_losses_php'].astype(float)
        results_df['total_farmers_affected'] = results_df['total_farmers_affected'].astype(float)
        results_df['number_of_events'] = results_df['number_of_events'].astype(int)

        results_df.to_json(API_OUTPUT_FILE, orient='records', indent=4)
        print(f"Successfully exported {len(results_df)} summary rows to '{API_OUTPUT_FILE}'.")
        print("\n--- Exported Data Sample ---")
        print(results_df.head().to_string())

    except duckdb.CatalogException as e:
         print(f"DUCKDB CATALOG ERROR during export: {e}")
    except Exception as e:
        print(f"ERROR during export: {e}")
    finally:
        if con:
            con.close()
            print("Closed DuckDB connection.")

    print("--- Export Complete ---")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Data manager for the local disaster analysis lakehouse.")
    subparsers = parser.add_subparsers(dest='command', help="Available commands")

    import_parser = subparsers.add_parser('import', help="Import ALL new CSV files from raw_data into the MAIN Delta Lake table.")
    import_parser.add_argument('--mode', type=str, choices=['overwrite', 'append'], default=None,
                              help="Force write mode: 'overwrite' to replace MAIN table (use carefully!), defaults to 'append'.")

    export_parser = subparsers.add_parser('export', help="Analyze the MAIN lakehouse table and export results for the API.")

    args = parser.parse_args()

    if args.command == 'import':
        handle_import(args.mode)
    elif args.command == 'export':
        handle_export()
    else:
        parser.print_help()

