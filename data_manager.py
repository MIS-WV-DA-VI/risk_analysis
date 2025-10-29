import argparse
import pandas as pd
import duckdb
from deltalake import write_deltalake
import os
import glob # Need glob to find files
import shutil # Need shutil to move files
import numpy as np # For NaN/Inf replacement

# --- Configuration ---
# Using relative paths for local execution
BASE_DIR = '.' # Current directory
RAW_DATA_DIR = os.path.join(BASE_DIR, 'raw_data')
PROCESSED_DATA_DIR = os.path.join(RAW_DATA_DIR, 'processed')
LAKEHOUSE_PATH = os.path.join(BASE_DIR, 'lakehouse_data/lakehouse_disasters') # Main clean data table
FARMER_LAKEHOUSE_PATH = os.path.join(BASE_DIR, 'lakehouse_data/farmer_registry') # Path to the farmer table
API_OUTPUT_FILE = os.path.join(BASE_DIR, 'api_output/api_data.json')
DUCKDB_FILE = os.path.join(BASE_DIR, 'lakehouse_data/analysis_db.duckdb')


def handle_import(mode_override: str = None):
    """
    Handles importing ALL unprocessed CSV files found in the RAW_DATA_DIR
    into the MAIN Delta Lake table using APPEND mode by default.
    Cleans data and ensures consistent casing for join keys.
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
    # Look for files ending in .csv (case-insensitive)
    csv_files = glob.glob(os.path.join(RAW_DATA_DIR, '*.csv')) + glob.glob(os.path.join(RAW_DATA_DIR, '*.CSV'))
    # Ensure uniqueness if both patterns match the same file
    csv_files = list(set(csv_files))


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

            # --- <<< Ensure consistent UPPERCASE for join keys >>> ---
            print("Standardizing case for province and municipality...")
            if 'province' in df.columns:
                 df['province'] = df['province'].fillna('Unknown').astype(str).str.strip().str.upper()
            else:
                 print("Warning: 'province' column not found.")
                 df['province'] = 'UNKNOWN' # Add if missing, ensure uppercase

            if 'municipality' in df.columns:
                 df['municipality'] = df['municipality'].fillna('Unknown').astype(str).str.strip().str.upper()
            else:
                 print("Warning: 'municipality' column not found.")
                 df['municipality'] = 'UNKNOWN' # Add if missing, ensure uppercase
            # --- <<< END >>> ---


            # Ensure other key string cols are strings
            str_cols = ['commodity', 'disaster_category', 'disaster_name', 'disaster_type_raw', 'sanitation_remarks']
            for col in str_cols:
                if col in df.columns:
                    # Also ensure these are uppercase if needed for consistency, or handle case-insensitively in queries
                    df[col] = df[col].fillna('Unknown').astype(str) # .str.upper() # Optional: Uppercase others too?

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
                    # Convert to numeric, errors become NaN
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                    # Use pandas nullable Int64 type for integers that might have NaNs
                    if col in ['year', 'farmers_affected']:
                         # Attempt conversion to nullable Int64
                         try:
                            df[col] = df[col].astype('Int64')
                         except (ValueError, TypeError):
                            print(f"Warning: Could not convert column '{col}' to Int64. Check data for non-numeric values.")
                            pass # Keep original type if conversion fails

            # Fill NaNs in specific numeric columns with 0.0 where appropriate
            cols_to_fill_zero = [
                 'area_partially_damaged_ha', 'area_totally_damaged_ha', 'area_total_affected_ha',
                 'losses_php_production_cost', 'losses_php_farm_gate', 'losses_php_grand_total'
            ]
            for col in cols_to_fill_zero:
                 if col in df.columns:
                      if pd.api.types.is_numeric_dtype(df[col]):
                          df[col] = df[col].fillna(0.0)
                      else:
                           print(f"Warning: Column '{col}' expected numeric but isn't. Skipping fillna(0.0).")


            print(f"Read and cleaned {len(df)} rows.")

            # Write to MAIN Delta Lake
            current_write_mode = write_mode if processed_count == 0 and write_mode == 'overwrite' else 'append'
            safe_lakehouse_path = os.path.normpath(LAKEHOUSE_PATH)
            write_deltalake(
                safe_lakehouse_path,
                df,
                mode=current_write_mode,
                schema_mode='merge' # Allow schema changes like new columns
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
            import traceback
            traceback.print_exc() # Print full traceback for import errors
            print("Skipping this file and continuing...")

    print(f"\nProcessed {processed_count} files for main table.")
    print("--- Main Import Complete ---")


def handle_export():
    """
    Runs the analysis query on the MAIN Delta Lake table using DuckDB
    and exports the results to a JSON file for the API. Uses a persistent DB file.
    Tries both read_delta and delta_scan function names.
    Includes robust integer casting fix.
    """
    print("--- Starting Export from Main Table ---")
    os.makedirs(os.path.dirname(API_OUTPUT_FILE), exist_ok=True)
    os.makedirs(os.path.dirname(DUCKDB_FILE), exist_ok=True) # Ensure DB file directory exists

    safe_lakehouse_path = os.path.normpath(LAKEHOUSE_PATH) # Ensure path is OS-correct
    safe_farmer_path = os.path.normpath(FARMER_LAKEHOUSE_PATH) # Path for farmer registry

    if not os.path.exists(safe_lakehouse_path):
        print(f"ERROR: Main Lakehouse table not found at '{safe_lakehouse_path}'.")
        print("Please run the 'import' command first.")
        print("--- Export Failed ---")
        return

    if not os.path.exists(safe_farmer_path):
        print(f"ERROR: Farmer Registry table not found at '{safe_farmer_path}'.")
        print("Please run the 'process_farmer_registry.py' script first.")
        print("--- Export Failed ---")
        return

    print(f"Connecting to DuckDB file: {DUCKDB_FILE}...")
    con = None # Initialize con to None
    try:
        # Connect to a file instead of in-memory
        con = duckdb.connect(database=DUCKDB_FILE, read_only=False)
        print("DuckDB connection established.")
        print(f"DuckDB Version: {duckdb.__version__}")


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
            # Check for read_delta first (more common)
            functions = con.sql("SELECT function_name FROM duckdb_functions() WHERE function_name IN ('read_delta', 'delta_scan')").df()
            if 'read_delta' in functions['function_name'].values:
                delta_read_function = 'read_delta'
            elif 'delta_scan' in functions['function_name'].values:
                 delta_read_function = 'delta_scan'
            else:
                 raise duckdb.CatalogException("Neither 'read_delta' nor 'delta_scan' found after loading extension!")
        except Exception as check_err:
             print(f"FATAL: Error checking for Delta read functions: {check_err}")
             raise check_err
        print(f"Using Delta read function: '{delta_read_function}'")


        # --- Simplify Execution - Read Delta into a View First ---
        print(f"Creating temporary view 'disasters_view' from Delta table: {safe_lakehouse_path}")
        # Use CREATE OR REPLACE VIEW for idempotency
        con.sql(f"CREATE OR REPLACE TEMPORARY VIEW disasters_view AS SELECT * FROM {delta_read_function}('{safe_lakehouse_path}');")
        print("Temporary view created.")

        print(f"Creating temporary view 'farmers_view' from Delta table: {safe_farmer_path}")
        con.sql(f"CREATE OR REPLACE TEMPORARY VIEW farmers_view AS SELECT * FROM {delta_read_function}('{safe_farmer_path}');")

        print("Creating pre-aggregated view 'province_farmer_summary'...")
        # Pre-aggregate farmer data by province to avoid join multiplication
        con.sql("""
            CREATE OR REPLACE TEMPORARY VIEW province_farmer_summary AS
            SELECT
                province,
                SUM(registered_rice_farmers) AS total_registered_farmers,
                SUM(total_declared_rice_area_ha) AS total_rice_area
            FROM farmers_view
            GROUP BY province;
        """)
        print("Farmer data views created.")


        # --- Execute Analysis Query on the View ---
        analysis_sql = f"""
        WITH aggregated_disasters AS (
            SELECT
                province,
                disaster_category,
                SUM(losses_php_grand_total) AS total_losses_php,
                -- Use coalesce to handle potential NULLs (represented as NaN or None by Pandas Int64) before summing
                -- DuckDB's SUM naturally ignores NULLs, but casting NULL to INTEGER might be needed depending on version
                -- Safest approach: SUM ignores NULLs, then cast result if needed.
                SUM(farmers_affected)::INTEGER AS total_farmers_affected,
                COUNT(*) AS number_of_events
            FROM disasters_view -- Query the main disasters view
            GROUP BY province, disaster_category
        )
        SELECT
            d.province,
            d.disaster_category,
            d.total_losses_php,
            d.total_farmers_affected,
            f.total_registered_farmers,
            -- Calculate percentage of affected farmers, handle division by zero
            (d.total_farmers_affected / NULLIF(f.total_registered_farmers, 0)) * 100 AS pct_farmers_affected,
            d.number_of_events,
            f.total_rice_area
        FROM aggregated_disasters d
        LEFT JOIN province_farmer_summary f ON d.province = f.province
        ORDER BY d.total_losses_php DESC
        LIMIT 1000
        """
        # Note: If farmers_affected is Int64 and contains pd.NA, DuckDB might need explicit COALESCE or CAST.
        # Alternative SUM: SUM(COALESCE(farmers_affected, 0))::INTEGER AS total_farmers_affected

        print("Executing analysis query on view...")
        results_df = con.sql(analysis_sql).to_df()
        print("Analysis complete.")

        # --- Process and Save Results ---
        print("Cleaning results for JSON...")
        # Convert types AFTER query
        results_df['total_losses_php'] = pd.to_numeric(results_df['total_losses_php'], errors='coerce').fillna(0.0)
        results_df['total_rice_area'] = pd.to_numeric(results_df['total_rice_area'], errors='coerce').fillna(0.0)
        results_df['pct_farmers_affected'] = pd.to_numeric(results_df['pct_farmers_affected'], errors='coerce').fillna(0.0)

        # Apply robust integer conversion (handle potential NULLs/NaNs from SUM if source had issues)
        results_df['total_farmers_affected'] = pd.to_numeric(results_df['total_farmers_affected'], errors='coerce').fillna(0).astype(int)
        results_df['total_registered_farmers'] = pd.to_numeric(results_df['total_registered_farmers'], errors='coerce').fillna(0).astype(int)
        results_df['number_of_events'] = pd.to_numeric(results_df['number_of_events'], errors='coerce').fillna(0).astype(int)

        # Replace NaN/Inf just before saving (important after numeric conversions)
        results_df = results_df.replace([np.inf, -np.inf], None).where(pd.notna(results_df), None)
        print("JSON cleaning complete.")


        results_df.to_json(API_OUTPUT_FILE, orient='records', indent=4)
        print(f"Successfully exported {len(results_df)} summary rows to '{API_OUTPUT_FILE}'.")
        print("\n--- Exported Data Sample ---")
        print(results_df.head().to_string())

    except duckdb.CatalogException as e:
         print(f"DUCKDB CATALOG ERROR during export: {e}")
         import traceback
         traceback.print_exc() # Print full trace for catalog errors too
    except TypeError as e:
         print(f"TYPE ERROR during export (often related to casting): {e}")
         import traceback
         traceback.print_exc() # Print full trace for type errors
    except Exception as e:
        print(f"UNEXPECTED ERROR during export: {e}")
        import traceback
        traceback.print_exc()
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

