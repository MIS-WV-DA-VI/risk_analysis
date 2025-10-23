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
            df = pd.read_csv(file_path)
            if df.empty:
                print("Skipping empty file.")
                shutil.move(file_path, os.path.join(PROCESSED_DATA_DIR, os.path.basename(file_path)))
                print("Moved empty file to processed directory.")
                continue

            # Standard cleaning for the main dataset
            df['event_date_start'] = pd.to_datetime(df['event_date_start'], errors='coerce')
            df['event_date_end'] = pd.to_datetime(df['event_date_end'], errors='coerce')
            df['municipality'] = df['municipality'].fillna('Unknown')
            numeric_cols = [
                'area_partially_damaged_ha', 'area_totally_damaged_ha',
                'area_total_affected_ha', 'farmers_affected',
                'losses_php_production_cost', 'losses_php_farm_gate',
                'losses_php_grand_total'
            ]
            for col in numeric_cols:
                 if col in df.columns: # Check if column exists before converting
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            df.fillna(0, inplace=True) # Replace any NaN created by coercion with 0 (use with caution)

            print(f"Read and cleaned {len(df)} rows.")

            # Write to MAIN Delta Lake
            current_write_mode = write_mode if processed_count == 0 and write_mode == 'overwrite' else 'append'
            write_deltalake(
                LAKEHOUSE_PATH,
                df,
                mode=current_write_mode,
                schema_mode='merge'
            )
            print(f"Successfully wrote data to MAIN Delta table using {current_write_mode.upper()} mode.")

            # Move processed file
            processed_file_path = os.path.join(PROCESSED_DATA_DIR, os.path.basename(file_path))
            shutil.move(file_path, processed_file_path)
            print(f"Moved processed file to: {processed_file_path}")
            processed_count += 1

        except Exception as e:
            print(f"ERROR processing file {os.path.basename(file_path)}: {e}")
            print("Skipping this file and continuing...")
            # Consider moving failed files to an 'error' directory

    print(f"\nProcessed {processed_count} files for main table.")
    print("--- Main Import Complete ---")


def handle_export():
    """
    Runs the analysis query on the MAIN Delta Lake table using DuckDB
    and exports the results to a JSON file for the API.
    """
    print("--- Starting Export from Main Table ---")
    os.makedirs(os.path.dirname(API_OUTPUT_FILE), exist_ok=True)

    if not os.path.exists(LAKEHOUSE_PATH):
        print(f"ERROR: Main Lakehouse table not found at '{LAKEHOUSE_PATH}'.")
        print("Please run the 'import' command first.")
        return

    print("Connecting to DuckDB and running analysis query on main table...")
    con = None # Initialize con to None
    try:
        con = duckdb.connect()
        try:
            con.install_extension('delta')
        except duckdb.IOException as e:
             print(f"Note: DuckDB delta extension install issue: {e}. Trying to load anyway.")
        con.load_extension('delta')

        analysis_sql = f"""
        SELECT
            province,
            disaster_category,
            SUM(losses_php_grand_total) AS total_losses_php,
            SUM(farmers_affected) AS total_farmers_affected,
            COUNT(*) AS number_of_events
        FROM read_delta('{LAKEHOUSE_PATH}') -- Querying the main clean table
        GROUP BY province, disaster_category
        ORDER BY total_losses_php DESC
        """
        results_df = con.sql(analysis_sql).to_df()
        print("Analysis complete.")

        results_df.to_json(API_OUTPUT_FILE, orient='records', indent=4)
        print(f"Successfully exported {len(results_df)} summary rows to '{API_OUTPUT_FILE}'.")
        print("\n--- Exported Data Sample ---")
        print(results_df.head().to_string())

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

