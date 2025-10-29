import argparse
import pandas as pd
import duckdb
from deltalake import write_deltalake, DeltaTable # Import DeltaTable
import os
import glob
import shutil
import numpy as np
import json # Needed for handling GeoJSON output structure
from datetime import datetime, date # Import datetime and date for type checking

# --- Configuration ---\
BASE_DIR = '.'
RAW_DATA_DIR = os.path.join(BASE_DIR, 'raw_data')
PROCESSED_DATA_DIR = os.path.join(RAW_DATA_DIR, 'processed')
LAKEHOUSE_PATH = os.path.join(BASE_DIR, 'lakehouse_data/lakehouse_disasters')
FARMER_LAKEHOUSE_PATH = os.path.join(BASE_DIR, 'lakehouse_data/farmer_registry')
API_OUTPUT_FILE = os.path.join(BASE_DIR, 'api_output/api_data.json') # Will now store GeoJSON
DUCKDB_FILE = os.path.join(BASE_DIR, 'lakehouse_data/analysis_db.duckdb') # Using a file is better for persistence if needed

# --- GIS Configuration ---
GIS_DATA_DIR = os.path.join(BASE_DIR, 'gis_data')
BOUNDARIES_GEOJSON = os.path.join(GIS_DATA_DIR, 'WV_Municipalities.geojson') # Reverted to Municipalities file
# --- <<< IMPORTANT: Adjust these property names based on your WV_Municipalities.geojson file >>> ---
GEOJSON_MUN_PROP = 'adm3_en'        # Property name for Municipality name
GEOJSON_PROV_PROP = 'adm2_en'       # Property name for Province name
GEOJSON_PSGC_PROP = 'adm3_psgc'     # Property name for Municipality PSGC code (IDEAL JOIN KEY)
# --- <<< Adjust these based on your sanitized disaster data columns >>> ---
DISASTER_MUN_COL = 'municipality' # Column name for Municipality in Delta table
DISASTER_PROV_COL = 'province'    # Column name for Province in Delta table
DISASTER_PSGC_COL = 'psgc_code'   # Ensure this is uncommented if sanitizer adds it

def handle_import(mode_override: str = None):
    """
    Handles importing ALL unprocessed CSV files found in the RAW_DATA_DIR
    into the MAIN Delta Lake table using APPEND mode by default.
    Cleans data and ensures consistent casing for join keys.
    Moves processed files to the PROCESSED_DATA_DIR.
    An 'overwrite' mode can be forced via command-line for initial setup or resets.
    """
    print(f"--- Starting Main Data Import ---")
    os.makedirs(PROCESSED_DATA_DIR, exist_ok=True)

    # --- Construct absolute path to input CSV files ---
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
    except NameError:
        script_dir = os.getcwd()
    raw_data_abs_path = os.path.join(script_dir, RAW_DATA_DIR)
    processed_data_abs_path = os.path.join(script_dir, PROCESSED_DATA_DIR)
    lakehouse_abs_path = os.path.join(script_dir, LAKEHOUSE_PATH)
    # --- End Path Construction ---


    csv_files = glob.glob(os.path.join(raw_data_abs_path, '*.csv')) # Use absolute path

    if not csv_files:
        print(f"No new CSV files found in '{raw_data_abs_path}'.")
        return

    processed_count = 0
    failed_count = 0
    first_file = True

    # Determine initial write mode
    write_mode = 'append' # Default
    if mode_override:
        write_mode = mode_override
        print(f"Forcing initial write mode: '{write_mode}'")
    elif not os.path.exists(lakehouse_abs_path): # Use absolute path
         write_mode = 'overwrite'
         print("Lakehouse table does not exist. Setting initial write mode to 'overwrite'.")


    for i, file_path in enumerate(csv_files):
        print(f"\nProcessing file: {os.path.basename(file_path)}...")
        current_write_mode = write_mode

        if write_mode == 'overwrite' and not first_file:
            current_write_mode = 'append'
            print("Switching to 'append' mode for subsequent files.")

        try:
            df = pd.read_csv(file_path)

            if DISASTER_MUN_COL in df.columns:
                 df[DISASTER_MUN_COL] = df[DISASTER_MUN_COL].astype(str).str.upper().str.strip()
            if DISASTER_PROV_COL in df.columns:
                 df[DISASTER_PROV_COL] = df[DISASTER_PROV_COL].astype(str).str.upper().str.strip()
            # If using PSGC, ensure it's loaded as string
            # Check if DISASTER_PSGC_COL exists as a variable AND if that column name should exist based on config
            psgc_col_name = None
            psgc_col_exists_in_df = False
            try:
                psgc_col_name = DISASTER_PSGC_COL
                if isinstance(psgc_col_name, str) and psgc_col_name in df.columns:
                    df[psgc_col_name] = df[psgc_col_name].astype(str).str.strip()
                    psgc_col_exists_in_df = True
            except NameError:
                pass # DISASTER_PSGC_COL is commented out or not defined


            if 'event_date_start' in df.columns:
                df['event_date_start'] = pd.to_datetime(df['event_date_start'], errors='coerce').dt.date
            if 'event_date_end' in df.columns:
                df['event_date_end'] = pd.to_datetime(df['event_date_end'], errors='coerce').dt.date

            required_cols = ['year', 'event_date_start', 'province', 'municipality', 'losses_php_grand_total']
            if psgc_col_exists_in_df: # Add PSGC to required only if it exists
                required_cols.append(psgc_col_name)

            if not all(col in df.columns for col in required_cols):
                 missing = [col for col in required_cols if col not in df.columns]
                 print(f"ERROR: File {os.path.basename(file_path)} is missing required columns: {missing}. Skipping.")
                 failed_count += 1
                 continue

            print(f"Writing {len(df)} rows to Delta table in '{current_write_mode}' mode...")
            # Decide on schema_mode dynamically based on write_mode
            schema_mode_param = "overwrite" if current_write_mode == 'overwrite' else "merge" #"none" # Use "merge" for append? or handle mismatch
            try:
                 write_deltalake(
                     lakehouse_abs_path,
                     df,
                     mode=current_write_mode,
                     # schema_mode="overwrite" # Keep schema fixed after initial overwrite
                 )
            except Exception as write_err:
                 # Catch potential schema mismatch on append
                 if "Schema mismatch detected" in str(write_err) or "number of fields does not match" in str(write_err):
                      print("\nSCHEMA MISMATCH DETECTED:")
                      print(f"Error: {write_err}")
                      print("The schema of the CSV file being imported does not match the existing Delta table.")
                      print("This usually happens if columns were added/removed/renamed in sanitizer.py after the table was first created.")
                      print("\nOPTIONS:")
                      print("1. Re-run the import with '--mode overwrite' to replace the table (DELETES EXISTING DATA):")
                      print("   python data_manager.py import --mode overwrite")
                      print("2. (Advanced) Manually ALTER the Delta table schema or add 'schema_mode=\"merge\"'/'schema_mode=\"overwrite\"' to write_deltalake call (see Delta Lake docs).")
                      print("Skipping this file due to schema mismatch.")
                      failed_count += 1
                      continue # Skip to next file
                 else:
                      raise # Re-raise other unexpected write errors


            print("Write successful.")

            try:
                processed_file_path = os.path.join(processed_data_abs_path, os.path.basename(file_path)) # Use absolute path
                if os.path.exists(processed_file_path): os.remove(processed_file_path)
                shutil.move(file_path, processed_file_path)
                print(f"Moved processed file to: {processed_file_path}")
                processed_count += 1
            except Exception as move_err:
                 print(f"ERROR moving file {os.path.basename(file_path)} after successful processing: {move_err}")
                 failed_count += 1

        except pd.errors.EmptyDataError:
            print(f"Skipping empty file: {os.path.basename(file_path)}")
            try:
                processed_file_path = os.path.join(processed_data_abs_path, os.path.basename(file_path)) # Use absolute path
                if os.path.exists(processed_file_path): os.remove(processed_file_path)
                shutil.move(file_path, processed_file_path)
                print(f"Moved empty file to: {processed_file_path}")
            except Exception as move_err:
                print(f"ERROR moving empty file {os.path.basename(file_path)}: {move_err}")
                failed_count += 1
        except Exception as e:
            print(f"ERROR processing file {os.path.basename(file_path)}: {e}")
            import traceback
            traceback.print_exc()
            failed_count += 1

        first_file = False

    print(f"\nSuccessfully imported and moved {processed_count} CSV file(s).")
    if failed_count > 0:
        print(f"Failed to process or move {failed_count} file(s). Please check logs.")
    print("--- Import Complete ---")


def df_to_geojson(df, geometry_col='geometry_geojson'):
    """Converts a DataFrame with a GeoJSON geometry string column to a GeoJSON FeatureCollection dictionary."""
    features = []
    # Adjust required columns based on whether PSGC is expected
    base_required = ['municipality_name', 'province_name', geometry_col]
    psgc_code_alias = 'psgc_code' # Alias used in the SQL query
    try:
        # Check if DISASTER_PSGC_COL constant suggests we should expect psgc_code
        if isinstance(DISASTER_PSGC_COL, str):
            base_required.append(psgc_code_alias)
    except NameError:
        pass # PSGC not configured

    # Check if essential geometry/ID columns exist in the DataFrame
    if not all(col in df.columns for col in base_required):
        missing = [col for col in base_required if col not in df.columns]
        print(f"ERROR in df_to_geojson: DataFrame is missing required columns for GeoJSON creation: {missing}")
        return {"type": "FeatureCollection", "features": []} # Return empty valid GeoJSON

    for _, row in df.iterrows():
        # Ensure geometry is valid before proceeding
        geom_str = row.get(geometry_col)
        if pd.isna(geom_str):
            print(f"Skipping row for {row.get('municipality_name', 'Unknown')} due to missing geometry.")
            continue
        try:
             geometry_obj = json.loads(geom_str)
        except (json.JSONDecodeError, TypeError) as e:
            print(f"Skipping row for {row.get('municipality_name', 'Unknown')} due to invalid geometry string: {geom_str}, Error: {e}")
            continue

        # Prepare properties, excluding geometry
        properties = row.drop(geometry_col, errors='ignore').to_dict()
        cleaned_properties = {}
        for k, v in properties.items():
            if pd.isna(v):
                cleaned_properties[k] = None
            elif isinstance(v, (np.int64, np.int32)):
                 cleaned_properties[k] = int(v)
            elif isinstance(v, (np.float64, np.float32)):
                 if np.isnan(v) or np.isinf(v): cleaned_properties[k] = None
                 else: cleaned_properties[k] = float(v)
            elif isinstance(v, (datetime, pd.Timestamp, date)): # <-- FIXED THIS LINE
                 cleaned_properties[k] = v.isoformat()
            else:
                 cleaned_properties[k] = v

        features.append({
            "type": "Feature",
            "geometry": geometry_obj,
            "properties": cleaned_properties
        })

    return {"type": "FeatureCollection", "features": features}


def handle_export():
    """
    Analyzes the main Delta Lake table using DuckDB, joins with Municipality GIS data,
    and exports aggregated results as GeoJSON.
    """
    print("--- Starting Data Export for API ---")
    con = None

    # --- Construct absolute paths ---
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
    except NameError:
        script_dir = os.getcwd()

    duckdb_abs_path = os.path.join(script_dir, DUCKDB_FILE)
    boundaries_abs_path = os.path.join(script_dir, BOUNDARIES_GEOJSON)
    lakehouse_abs_path = os.path.join(script_dir, LAKEHOUSE_PATH)
    api_output_abs_path = os.path.join(script_dir, API_OUTPUT_FILE)
    api_output_dir = os.path.dirname(api_output_abs_path)
    # --- End Path Construction ---

    try:
        con = duckdb.connect(database=duckdb_abs_path, read_only=False)
        print("Connected to DuckDB.")
        con.sql("INSTALL spatial; LOAD spatial;")
        print("Loaded DuckDB spatial extension.")

        # --- Load Municipal Boundaries ---
        if not os.path.exists(boundaries_abs_path):
             print(f"ERROR: Boundaries GeoJSON file not found at {boundaries_abs_path}")
             return
        
        # --- [FIXED BLOCK] ---
        # This block is updated to remove 'properties->>' and handle 'geom' vs 'geometry'
        try:
            # Load Municipality data. ST_Read flattens GeoJSON properties into top-level columns.
            # We also need to find the geometry column. The original error suggested 'geom'
            # might be the name, or it could be 'geometry'.
            print(f"Attempting to load boundaries from '{boundaries_abs_path}'...")
            print(f"Using properties: MUN='{GEOJSON_MUN_PROP}', PROV='{GEOJSON_PROV_PROP}', PSGC='{GEOJSON_PSGC_PROP}'")

            # First, let's try to find the geometry column name ('geom' or 'geometry')
            # and create the table in one go.
            try:
                # Attempt 1: Assume geometry column is 'geom' and select properties directly
                con.sql(f"""
                    CREATE OR REPLACE TABLE municipal_boundaries AS
                    SELECT
                        geom, -- Assume geometry column is 'geom'
                        "{GEOJSON_MUN_PROP}" AS municipality_name,
                        "{GEOJSON_PROV_PROP}" AS province_name,
                        "{GEOJSON_PSGC_PROP}" AS psgc_code -- Municipality PSGC
                    FROM ST_Read('{boundaries_abs_path}');
                """)
                print("Successfully loaded boundaries assuming 'geom' column.")
            except duckdb.BinderException as be_geom:
                # This error means a configured property name (e.g., 'adm3_en') was not found
                if f'column "{GEOJSON_MUN_PROP}" does not exist' in str(be_geom) or \
                   f'column "{GEOJSON_PROV_PROP}" does not exist' in str(be_geom) or \
                   f'column "{GEOJSON_PSGC_PROP}" does not exist' in str(be_geom):
                    print(f"ERROR: A configured GeoJSON property column was not found: {be_geom}")
                    print("Please check your GEOJSON_*_PROP settings in the script against the GeoJSON file.")
                    raise # Re-raise this critical error
                
                # This error means 'geom' wasn't the geometry column, so we try 'geometry'
                elif 'column "geom" does not exist' in str(be_geom):
                    print("Column 'geom' not found. Retrying with 'geometry' as the geometry column...")
                    con.sql(f"""
                        CREATE OR REPLACE TABLE municipal_boundaries AS
                        SELECT
                            geometry AS geom, -- Assume geometry column is 'geometry' and alias it
                            "{GEOJSON_MUN_PROP}" AS municipality_name,
                            "{GEOJSON_PROV_PROP}" AS province_name,
                            "{GEOJSON_PSGC_PROP}" AS psgc_code -- Municipality PSGC
                        FROM ST_Read('{boundaries_abs_path}');
                    """)
                    print("Successfully loaded boundaries assuming 'geometry' column and aliasing to 'geom'.")
                else:
                    # Another binder error
                    print(f"An unexpected BinderException occurred: {be_geom}")
                    raise be_geom # Re-raise the error

            # Index on municipality identifiers
            con.sql("CREATE INDEX IF NOT EXISTS mun_bound_psgc_idx ON municipal_boundaries (psgc_code);")
            con.sql("CREATE INDEX IF NOT EXISTS mun_bound_name_idx ON municipal_boundaries (UPPER(province_name), UPPER(municipality_name));")
            print(f"Loaded and indexed boundaries from '{os.path.basename(BOUNDARIES_GEOJSON)}'.")

        except duckdb.BinderException as be:
             # This outer catch block will catch errors from the inner logic
             print("\n--- DETAILED BINDER ERROR ---")
             print(f"Failed to load GeoJSON: {be}")
             print("\nThis error usually means one of two things:")
             print(f"1. The geometry column in your GeoJSON is not named 'geom' or 'geometry'.")
             print(f"2. A property column name in your script configuration is wrong:")
             print(f"   - Municipality Property: '{GEOJSON_MUN_PROP}'")
             print(f"   - Province Property:     '{GEOJSON_PROV_PROP}'")
             print(f"   - PSGC Property:         '{GEOJSON_PSGC_PROP}'")
             print("\nTo fix, please:")
             print(f"1. Open the file: {boundaries_abs_path}")
             print(f"2. Check the *exact* spelling and case of the property names (e.g., 'adm3_en', 'adm2_en').")
             print(f"3. Update the GEOJSON..._PROP constants at the top of the script to match.")
             print("---------------------------------\n")
             import traceback; traceback.print_exc(); return
        except Exception as e:
            print(f"ERROR loading or indexing GeoJSON: {e}")
            import traceback; traceback.print_exc(); return
        # --- [END FIXED BLOCK] ---


        # --- Read Main Disaster Data from Delta Lake ---
        print("Reading main disaster data from Delta Lake...")
        try:
             if not os.path.exists(lakehouse_abs_path):
                  print(f"ERROR: Disaster Delta table path not found at {lakehouse_abs_path}")
                  return
             dt = DeltaTable(lakehouse_abs_path)
             main_disasters_df = dt.to_pandas()
             # Ensure correct types after loading from Delta/Pandas
             psgc_col_exists_in_df = False
             try:
                 # Check if DISASTER_PSGC_COL is defined and is a string
                 if isinstance(DISASTER_PSGC_COL, str) and DISASTER_PSGC_COL in main_disasters_df.columns:
                      main_disasters_df[DISASTER_PSGC_COL] = main_disasters_df[DISASTER_PSGC_COL].astype(str)
                      psgc_col_exists_in_df = True
             except NameError:
                 pass # DISASTER_PSGC_COL likely commented out

             if DISASTER_MUN_COL in main_disasters_df.columns:
                   main_disasters_df[DISASTER_MUN_COL] = main_disasters_df[DISASTER_MUN_COL].astype(str).str.upper()
             if DISASTER_PROV_COL in main_disasters_df.columns:
                   main_disasters_df[DISASTER_PROV_COL] = main_disasters_df[DISASTER_PROV_COL].astype(str).str.upper()

             con.register('main_disasters_df', main_disasters_df)
             print(f"Read {len(main_disasters_df)} rows from disaster Delta table.")
        except Exception as e:
             print(f"ERROR reading disaster Delta table at {lakehouse_abs_path}: {e}")
             import traceback; traceback.print_exc(); return

        # --- Define Join Condition ---
        if psgc_col_exists_in_df:
             join_condition = f"md.{DISASTER_PSGC_COL} = mb.psgc_code" # Join using Mun PSGC
             print(f"Using PSGC ('{DISASTER_PSGC_COL}' and 'psgc_code') for joining.")
        else:
             join_condition = f"UPPER(md.{DISASTER_MUN_COL}) = UPPER(mb.municipality_name) AND UPPER(md.{DISASTER_PROV_COL}) = UPPER(mb.province_name)"
             print("Warning: PSGC code column not found or configured in disaster data. Falling back to joining on Province and Municipality names. Ensure consistency.")

        # --- Define the Spatial Aggregation Query (Direct Join) ---
        query = f"""
        SELECT
            mb.municipality_name,
            mb.province_name,
            mb.psgc_code,
            SUM(md.losses_php_grand_total) AS total_loss_php,
            COUNT(*) AS incident_count,
            -- Add other aggregations: SUM(md.farmers_affected), AVG(md.area_total_affected_ha), etc.
            ST_AsGeoJSON(mb.geom) AS geometry_geojson -- Use 'geom' here
        FROM main_disasters_df md
        JOIN municipal_boundaries mb -- Join directly with municipal boundaries
          ON {join_condition}
        -- WHERE clauses for filtering (apply to 'md' table)
        -- e.g., WHERE md.year >= 2020
        GROUP BY
            mb.province_name,
            mb.municipality_name,
            mb.psgc_code,
            mb.geom -- Group by the municipality geometry using 'geom'
        ORDER BY
            mb.province_name,
            mb.municipality_name;
        """

        print("Executing spatial aggregation query (Municipalities)...")
        results_df = con.sql(query).df()
        print(f"Query returned {len(results_df)} aggregated municipalities.")

        # --- Convert results to GeoJSON FeatureCollection ---
        geojson_output = df_to_geojson(results_df, geometry_col='geometry_geojson')

        # --- Save GeoJSON to API output file ---
        os.makedirs(api_output_dir, exist_ok=True)
        with open(api_output_abs_path, 'w') as f:
            json.dump(geojson_output, f, indent=2)
        print(f"Successfully exported aggregated GeoJSON data to '{api_output_abs_path}'")

    # --- Error Handling ---
    except ImportError as e:
         print(f"IMPORT ERROR: {e}. Make sure necessary libraries (duckdb, deltalake, pandas, spatial extension dependencies like GDAL) are installed.")
    except duckdb.IOException as e:
         print(f"DUCKDB IO ERROR (often path related): {e}")
         import traceback; traceback.print_exc()
    except duckdb.BinderException as e:
         print(f"DUCKDB BINDER ERROR (often column name mismatch in query): {e}")
         import traceback; traceback.print_exc()
    except duckdb.CatalogException as e:
         print(f"DUCKDB CATALOG ERROR: {e}")
         import traceback; traceback.print_exc()
    except TypeError as e:
         print(f"TYPE ERROR during export (often related to casting): {e}")
         import traceback; traceback.print_exc()
    except Exception as e:
        print(f"UNEXPECTED ERROR during export: {e}")
        import traceback; traceback.print_exc()
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

    export_parser = subparsers.add_parser('export', help="Analyze the MAIN lakehouse table with Municipality GIS data and export results for the API.")

    args = parser.parse_args()

    if args.command == 'import':
        handle_import(args.mode)
    elif args.command == 'export':
        handle_export()
    else:
        parser.print_help()

