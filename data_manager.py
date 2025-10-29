import argparse
import pandas as pd
import duckdb
from deltalake import write_deltalake, DeltaTable # Import DeltaTable
import os
import glob
import shutil
import numpy as np
import json # Needed for handling GeoJSON output structure
from datetime import datetime # Import datetime for type checking

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
BOUNDARIES_GEOJSON = os.path.join(GIS_DATA_DIR, 'WV_Barangays.geojson') # Use Barangay file
# --- <<< IMPORTANT: Adjust these property names based on your WV_Barangays.geojson file >>> ---
# Property names within the Barangay GeoJSON for MUNICIPALITY level info
GEOJSON_MUN_PROP = 'adm3_en'        # Property name for Municipality name
GEOJSON_PROV_PROP = 'adm1_en'       # Property name for Province name
GEOJSON_PSGC_PROP = 'adm3_psgc'     # Property name for Municipality PSGC code (IDEAL JOIN KEY)
# --- <<< Adjust these based on your sanitized disaster data columns >>> ---
DISASTER_MUN_COL = 'municipality' # Column name for Municipality in Delta table
DISASTER_PROV_COL = 'province'    # Column name for Province in Delta table
DISASTER_PSGC_COL = 'psgc_code'   # UNCOMMENT and add to sanitizer.py if you implement Municipality PSGC mapping


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
            if DISASTER_PSGC_COL in df.columns:
                df[DISASTER_PSGC_COL] = df[DISASTER_PSGC_COL].astype(str).str.strip()


            if 'event_date_start' in df.columns:
                df['event_date_start'] = pd.to_datetime(df['event_date_start'], errors='coerce').dt.date
            if 'event_date_end' in df.columns:
                df['event_date_end'] = pd.to_datetime(df['event_date_end'], errors='coerce').dt.date

            required_cols = ['year', 'event_date_start', 'province', 'municipality', 'losses_php_grand_total']
            # Add PSGC to required if using it for join
            # if DISASTER_PSGC_COL not in COLUMN_MAPPING.values(): # Check if it should exist
            #      required_cols.append(DISASTER_PSGC_COL)

            if not all(col in df.columns for col in required_cols):
                 missing = [col for col in required_cols if col not in df.columns]
                 print(f"ERROR: File {os.path.basename(file_path)} is missing required columns: {missing}. Skipping.")
                 failed_count += 1
                 continue

            print(f"Writing {len(df)} rows to Delta table in '{current_write_mode}' mode...")
            write_deltalake(
                lakehouse_abs_path, # Use absolute path
                df,
                mode=current_write_mode,
            )
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
    required_cols = ['municipality_name', 'province_name', 'psgc_code', geometry_col] # Base required
    
    # Check if essential geometry/ID columns exist
    if not all(col in df.columns for col in required_cols):
        missing = [col for col in required_cols if col not in df.columns]
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
            # Handle specific numpy types if they appear
            elif isinstance(v, (np.int64, np.int32)):
                 cleaned_properties[k] = int(v)
            elif isinstance(v, (np.float64, np.float32)):
                 # Handle NaN/Inf specifically for floats
                 if np.isnan(v) or np.isinf(v):
                      cleaned_properties[k] = None
                 else:
                      cleaned_properties[k] = float(v)
            elif isinstance(v, (datetime, pd.Timestamp, datetime.date)): # Check type explicitly
                 cleaned_properties[k] = v.isoformat()
            else:
                 cleaned_properties[k] = v # Assume other types are JSON serializable

        features.append({
            "type": "Feature",
            "geometry": geometry_obj,
            "properties": cleaned_properties
        })

    return {"type": "FeatureCollection", "features": features}


def handle_export():
    """
    Analyzes the main Delta Lake table using DuckDB, aggregates Barangay shapes to Municipalities,
    joins with disaster data, and exports aggregated results as GeoJSON.
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

        # --- Load Barangay Boundaries ---
        if not os.path.exists(boundaries_abs_path):
             print(f"ERROR: Boundaries GeoJSON file not found at {boundaries_abs_path}")
             return
        try:
            # Load Barangay data, extracting MUNICIPALITY properties
            con.sql(f"""
                CREATE OR REPLACE TABLE barangay_boundaries AS
                SELECT
                    ST_GeomFromWKB(geometry) AS geom,
                    properties->>'{GEOJSON_MUN_PROP}' AS municipality_name,
                    properties->>'{GEOJSON_PROV_PROP}' AS province_name,
                    properties->>'{GEOJSON_PSGC_PROP}' AS mun_psgc_code -- Municipality PSGC
                FROM ST_Read('{boundaries_abs_path}');
            """)
            # Index on municipality identifiers
            con.sql("CREATE INDEX IF NOT EXISTS bgy_mun_psgc_idx ON barangay_boundaries (mun_psgc_code);")
            con.sql("CREATE INDEX IF NOT EXISTS bgy_mun_name_idx ON barangay_boundaries (UPPER(province_name), UPPER(municipality_name));")
            print(f"Loaded and indexed boundaries from '{os.path.basename(BOUNDARIES_GEOJSON)}'.")
        except Exception as e:
            print(f"ERROR loading or indexing GeoJSON: {e}")
            import traceback; traceback.print_exc(); return

        # --- Read Main Disaster Data from Delta Lake ---
        print("Reading main disaster data from Delta Lake...")
        try:
             # Ensure DeltaTable is used to read
             if not os.path.exists(lakehouse_abs_path):
                  print(f"ERROR: Disaster Delta table path not found at {lakehouse_abs_path}")
                  return
             dt = DeltaTable(lakehouse_abs_path)
             main_disasters_df = dt.to_pandas()
             # Ensure correct types after loading from Delta/Pandas
             if DISASTER_PSGC_COL in main_disasters_df.columns:
                  main_disasters_df[DISASTER_PSGC_COL] = main_disasters_df[DISASTER_PSGC_COL].astype(str)
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
        # IDEALLY JOIN ON MUNICIPALITY PSGC
        # Ensure DISASTER_PSGC_COL exists in df and uncomment line in config
        if DISASTER_PSGC_COL in main_disasters_df.columns:
             join_condition = f"md.{DISASTER_PSGC_COL} = ms.psgc_code" # Join aggregated shapes using Mun PSGC
             print(f"Using PSGC ('{DISASTER_PSGC_COL}' and 'psgc_code') for joining.")
        else:
        # Fallback Join on Names (ensure UPPERCASE consistency)
             join_condition = f"UPPER(md.{DISASTER_MUN_COL}) = UPPER(ms.municipality_name) AND UPPER(md.{DISASTER_PROV_COL}) = UPPER(ms.province_name)"
             print("Warning: PSGC code column not found in disaster data. Falling back to joining on Province and Municipality names. Ensure consistency.")

        # --- Define the Spatial Aggregation Query ---
        # 1. CTE to aggregate Barangay shapes into Municipal shapes
        # 2. Join disaster data with the aggregated shapes
        query = f"""
        WITH municipal_shapes AS (
            SELECT
                UPPER(municipality_name) as municipality_name, -- Ensure names are upper for consistency
                UPPER(province_name) as province_name,
                mun_psgc_code as psgc_code,
                ST_Union(geom) as municipal_geom -- Aggregate shapes
            FROM barangay_boundaries
            WHERE mun_psgc_code IS NOT NULL -- Exclude barangays without muni psgc if any
            GROUP BY
                municipality_name,
                province_name,
                mun_psgc_code
        )
        SELECT
            ms.municipality_name,
            ms.province_name,
            ms.psgc_code,
            SUM(md.losses_php_grand_total) AS total_loss_php,
            COUNT(*) AS incident_count,
            -- Add other aggregations: SUM(md.farmers_affected), AVG(md.area_total_affected_ha), etc.
            ST_AsGeoJSON(ms.municipal_geom) AS geometry_geojson -- Export aggregated geometry
        FROM main_disasters_df md
        JOIN municipal_shapes ms -- Join with the aggregated shapes
          ON {join_condition}
        -- WHERE clauses for filtering (apply to 'md' table)
        -- e.g., WHERE md.year >= 2020
        GROUP BY
            ms.province_name,
            ms.municipality_name,
            ms.psgc_code,
            ms.municipal_geom -- Group by the aggregated geometry
        ORDER BY
            ms.province_name,
            ms.municipality_name;
        """

        print("Executing spatial aggregation query (Barangays -> Municipalities)...")
        results_df = con.sql(query).df()
        print(f"Query returned {len(results_df)} aggregated municipalities.")

        # --- Convert results to GeoJSON FeatureCollection ---
        geojson_output = df_to_geojson(results_df, geometry_col='geometry_geojson')

        # --- Save GeoJSON to API output file ---
        os.makedirs(api_output_dir, exist_ok=True) # Use absolute path
        with open(api_output_abs_path, 'w') as f: # Use absolute path
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

    export_parser = subparsers.add_parser('export', help="Analyze the MAIN lakehouse table with GIS data (using Barangay source) and export results for the API.")

    args = parser.parse_args()

    if args.command == 'import':
        handle_import(args.mode)
    elif args.command == 'export':
        handle_export()
    else:
        parser.print_help()

