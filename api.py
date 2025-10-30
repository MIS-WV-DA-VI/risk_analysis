import json
from fastapi import FastAPI, Query, HTTPException # Import HTTPException
from fastapi.responses import JSONResponse, FileResponse # Import FileResponse for serving JSON file
from fastapi.middleware.cors import CORSMiddleware
import os
import duckdb
from deltalake import DeltaTable, __version__ as deltalake_version # Import DeltaTable and version
from typing import Optional, List # Import List for return type hint
import datetime
import math # For checking NaN
import numpy as np # Import numpy for replacing infinite values
import pandas as pd # Ensure pandas is imported

# --- Configuration ---
BASE_DIR = '.'
LAKEHOUSE_PATH = os.path.join(BASE_DIR, 'lakehouse_data/lakehouse_disasters')
# --- Path to Farmer Registry Delta table ---
FARMER_REGISTRY_PATH = os.path.join(BASE_DIR, 'lakehouse_data/farmer_registry')
# --- Path to the pre-generated GeoJSON file ---
AGGREGATED_GEOJSON_FILE = os.path.join(BASE_DIR, 'api_output/api_data.json')
# Using in-memory DuckDB for raw queries

app = FastAPI()

# --- CORS Middleware ---
origins = ["http://localhost", "http://localhost:8000", "http://127.0.0.1", "http://127.0.0.1:8000", "https://dawvinfosys.test", "https://27.110.161.135", "null"]
# origins = ["*"] # Allow all for easier local dev, restrict in production
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Helper Function to Clean Data for JSON ---
# (Handles NaN, infinity, dates etc.)
def clean_data_for_json(data):
    cleaned_data = []
    for row_dict in data:
        cleaned_row = {}
        for key, value in row_dict.items():
            if isinstance(value, (np.int64, np.int32)):
                 # Handle potential Pandas nullable integer <NA> which becomes pd.NA
                 cleaned_row[key] = int(value) if pd.notna(value) else None
            elif isinstance(value, (np.float64, np.float32)):
                if np.isnan(value) or np.isinf(value):
                    cleaned_row[key] = None # Replace NaN/Inf with None
                else:
                    # Round floats that might represent percentages or specific values
                    if key == 'percentage_farmers_affected':
                         cleaned_row[key] = round(float(value), 2)
                    else:
                         cleaned_row[key] = float(value)
            elif isinstance(value, (datetime.date, datetime.datetime, pd.Timestamp)):
                 # Format dates as YYYY-MM-DD strings, handle NaT
                 cleaned_row[key] = value.strftime('%Y-%m-%d') if pd.notna(value) else None
            elif pd.isna(value): # Catch remaining pandas NA types (like NaT, None)
                 cleaned_row[key] = None
            else:
                cleaned_row[key] = value
        cleaned_data.append(cleaned_row)
    return cleaned_data


# --- Endpoint to serve the pre-aggregated GeoJSON map data ---
@app.get("/query", response_model=dict)
async def get_aggregated_geojson_map_data(
    province: Optional[str] = Query(None, description="Filter by province (case-insensitive, exact match)"),
    municipality: Optional[str] = Query(None, description="Filter by municipality (case-insensitive, exact match)"),
    disaster_category: Optional[str] = Query(None, description="Filter by disaster category (case-insensitive, exact match)"),
    disaster_name: Optional[str] = Query(None, description="Filter by specific disaster name"),
    commodity: Optional[str] = Query(None, description="Filter by commodity name"),
    quarter: Optional[int] = Query(None, description="Filter by quarter (1-4)", ge=1, le=4),
    year: Optional[int] = Query(None, description="Filter by year (e.g., 2023)", ge=1900, le=2100),
    start_date: Optional[datetime.date] = Query(None, description="Filter by start date (YYYY-MM-DD) - inclusive"),
    end_date: Optional[datetime.date] = Query(None, description="Filter by end date (YYYY-MM-DD) - inclusive")
):
    """
    Serves a dynamic GeoJSON file.
    It loads a base GeoJSON for shapes, then runs a filtered/aggregated query
    on the disaster data, merges the results into the GeoJSON properties,
    and **filters the GeoJSON features based on province/municipality**.
    """
    print(f"API (/query): Received map request with filters: prov={province}, mun={municipality}, year={year}, qtr={quarter}")
    
    # 1. Load the base GeoJSON shapefile
    if not os.path.exists(AGGREGATED_GEOJSON_FILE):
        print(f"API ERROR (/query): Aggregated base GeoJSON file not found at {AGGREGATED_GEOJSON_FILE}")
        raise HTTPException(status_code=404, detail=f"Aggregated map data file not found. Please run 'python data_manager.py export'.")
    
    try:
        with open(AGGREGATED_GEOJSON_FILE, 'r') as f:
            geojson_data = json.load(f)
    except Exception as e:
        print(f"API ERROR (/query): Could not read base GeoJSON file: {e}")
        raise HTTPException(status_code=500, detail="Could not read base map data.")

    # --- DEBUGGING STEP ---
    if geojson_data.get('features'):
        try:
            first_feature_properties = geojson_data['features'][0].get('properties', {})
            print("--- DEBUG: GeoJSON Property Keys ---")
            print(f"API (/query): Keys in first feature: {first_feature_properties.keys()}")
            print("-------------------------------------")
        except Exception as debug_e:
            print(f"API (/query): Could not print debug keys: {debug_e}")

    # --- Filter GeoJSON Features based on Province/Municipality ---
    if province or municipality:
        print(f"API (/query): Filtering GeoJSON features for province='{province}', municipality='{municipality}'")
        original_feature_count = len(geojson_data['features'])
        filtered_features = []
        
        province_upper = province.upper() if province else None
        municipality_upper = municipality.upper() if municipality else None

        for feature in geojson_data['features']:
            props = feature.get('properties', {})
            
            # --- MODIFIED: Use the correct keys 'province_name' and 'municipality_name' ---
            feature_province = props.get('province_name') 
            feature_municipality = props.get('municipality_name')
            # --- END MODIFICATION ---

            province_match = (not province_upper) or (feature_province and feature_province.upper() == province_upper)
            municipality_match = (not municipality_upper) or (feature_municipality and feature_municipality.upper() == municipality_upper)

            if province_match and municipality_match:
                filtered_features.append(feature)
        
        geojson_data['features'] = filtered_features
        print(f"API (/query): Filtered GeoJSON from {original_feature_count} to {len(filtered_features)} features.")

    # 2. Run a filtered, aggregated query to get the new data
    con = None
    safe_lakehouse_path = os.path.normpath(LAKEHOUSE_PATH)
    if not os.path.exists(safe_lakehouse_path):
        print(f"API ERROR (/query): Main Delta table not found at '{safe_lakehouse_path}'.")
        raise HTTPException(status_code=404, detail="Main disaster data source not found.")

    try:
        print("API (/query): Connecting to DuckDB (in-memory) for aggregation...")
        con = duckdb.connect(read_only=False)
        
        try:
             con.sql("INSTALL delta; LOAD delta;")
             delta_read_function = None
             functions = con.sql("SELECT function_name FROM duckdb_functions() WHERE function_name IN ('read_delta', 'delta_scan')").df()
             if 'read_delta' in functions['function_name'].values: delta_read_function = 'read_delta'
             elif 'delta_scan' in functions['function_name'].values: delta_read_function = 'delta_scan'
             else: raise duckdb.CatalogException("Neither 'read_delta' nor 'delta_scan' found.")
             main_table_read_sql = f"SELECT * FROM {delta_read_function}('{safe_lakehouse_path}')"
             con.sql(f"CREATE OR REPLACE TEMPORARY VIEW main_disasters AS {main_table_read_sql};")
             print("API (/query): Registered main_disasters view using DuckDB Delta extension.")
        except Exception:
             print("API WARNING (/query): DuckDB Delta extension failed. Falling back to reading via pandas.")
             dt_main = DeltaTable(safe_lakehouse_path)
             df_main = dt_main.to_pandas()
             if 'event_date_start' in df_main.columns: df_main['event_date_start'] = pd.to_datetime(df_main['event_date_start'], errors='coerce')
             if 'event_date_end' in df_main.columns: df_main['event_date_end'] = pd.to_datetime(df_main['event_date_end'], errors='coerce')
             if 'year' in df_main.columns: df_main['year'] = pd.to_numeric(df_main['year'], errors='coerce').astype('Int64')
             con.register('main_disasters_df_pandas', df_main)
             con.sql("CREATE OR REPLACE TEMPORARY VIEW main_disasters AS SELECT * FROM main_disasters_df_pandas;")
             print("API (/query): Registered main_disasters view using pandas fallback.")
        
        where_clauses = []
        params = {}
        if province:
            where_clauses.append("UPPER(d.province) = UPPER($province)")
            params['province'] = province
        if municipality:
            where_clauses.append("UPPER(d.municipality) = UPPER($municipality)")
            params['municipality'] = municipality
        if disaster_category:
            where_clauses.append("UPPER(d.disaster_category) = UPPER($disaster_category)")
            params['disaster_category'] = disaster_category
        if disaster_name:
             where_clauses.append("UPPER(d.disaster_name) = UPPER($disaster_name)")
             params['disaster_name'] = disaster_name
        if commodity:
             where_clauses.append("UPPER(d.commodity) = UPPER($commodity)")
             params['commodity'] = commodity
        if start_date:
            where_clauses.append("CAST(d.event_date_end AS DATE) >= $start_date")
            params['start_date'] = start_date
        if end_date:
            where_clauses.append("CAST(d.event_date_start AS DATE) <= $end_date")
            params['end_date'] = end_date
        if quarter:
            where_clauses.append("quarter(CAST(d.event_date_start AS DATE)) = $quarter")
            params['quarter'] = quarter
        if year:
             where_clauses.append("d.year = $year")
             params['year'] = year

        where_sql = ""
        if where_clauses:
            where_sql = "WHERE " + " AND ".join(where_clauses)

        # --- Run Aggregation Query ---
        # <<< MODIFIED: Changed aliases (e.g., total_losses_php) to match raw field names (e.g., losses_php_grand_total) >>>
        agg_query = f"""
        SELECT
            UPPER(d.province) AS province,
            UPPER(d.municipality) AS municipality,
            SUM(d.losses_php_grand_total) AS losses_php_grand_total,
            SUM(d.farmers_affected) AS farmers_affected,
            SUM(d.area_partially_damaged_ha) AS area_partially_damaged_ha,
            SUM(d.area_totally_damaged_ha) AS area_totally_damaged_ha,
            SUM(d.area_total_affected_ha) AS area_total_affected_ha,
            SUM(d.losses_php_production_cost) AS losses_php_production_cost,
            SUM(d.losses_php_farm_gate) AS losses_php_farm_gate,
            list(distinct d.commodity order by d.commodity) AS commodities_affected 
        FROM
            main_disasters AS d
        {where_sql}
        GROUP BY 1, 2
        """
        
        print(f"API (/query): Executing aggregation query: {agg_query}")
        print(f"API (/query): With parameters: {params}")
        
        # <<< MODIFIED: Use fetchall() for robust list handling >>>
        # This avoids pandas conversion issues with list columns
        query_result_rows = con.execute(agg_query, params).fetchall()
        
        # Manually build the data_map to ensure correct types
        print(f"API (/query): Aggregation query returned {len(query_result_rows)} rows. Building data map...")
        data_map = {}
        for row in query_result_rows:
            # Map query result columns by index
            key = (row[0], row[1]) # (province, municipality)
            data_map[key] = {
                'losses_php_grand_total': row[2],
                'farmers_affected': row[3],
                'area_partially_damaged_ha': row[4],
                'area_totally_damaged_ha': row[5],
                'area_total_affected_ha': row[6],
                'losses_php_production_cost': row[7],
                'losses_php_farm_gate': row[8],
                'commodities_affected': row[9] # This is a list
            }
        
        print("API (/query): Data map built successfully.")
        
        # --- 3. Merge aggregated data into the (already filtered) GeoJSON ---
        for feature in geojson_data['features']:
            props = feature['properties']
            
            # Build the key using the correct properties and case
            prop_prov = props.get('province_name')
            prop_mun = props.get('municipality_name')
            key = (
                prop_prov.upper() if prop_prov else None, 
                prop_mun.upper() if prop_mun else None
            )
            
            new_data = data_map.get(key)
            
            if new_data:
                # <<< MODIFIED: Update all properties with the new aliases >>>
                props['losses_php_grand_total'] = new_data['losses_php_grand_total']
                props['farmers_affected'] = new_data['farmers_affected']
                props['area_partially_damaged_ha'] = new_data['area_partially_damaged_ha']
                props['area_totally_damaged_ha'] = new_data['area_totally_damaged_ha']
                props['area_total_affected_ha'] = new_data['area_total_affected_ha']
                props['losses_php_production_cost'] = new_data['losses_php_production_cost']
                props['losses_php_farm_gate'] = new_data['losses_php_farm_gate']
                props['commodities_affected'] = new_data['commodities_affected'] 
            else:
                # Reset all properties if no matching data
                props['losses_php_grand_total'] = 0
                props['farmers_affected'] = 0
                props['area_partially_damaged_ha'] = 0
                props['area_totally_damaged_ha'] = 0
                props['area_total_affected_ha'] = 0
                props['losses_php_production_cost'] = 0
                props['losses_php_farm_gate'] = 0
                props['commodities_affected'] = [] 
        
        print("API (/query): Successfully merged aggregated data into filtered GeoJSON.")
        return geojson_data

    except Exception as e:
        error_message = f"An error occurred during map data query execution: {str(e)}"
        print(f"API ERROR (/query): {error_message}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail="Could not retrieve map data. Please check server logs.")
    finally:
        if con:
            con.close()
            print("API (/query): Closed DuckDB connection.")


# --- Endpoint to serve raw/detailed data for charts and table ---
@app.get("/api/raw", response_model=dict)
async def get_raw_disaster_data(
    province: Optional[str] = Query(None, description="Filter by province (case-insensitive, exact match)"),
    municipality: Optional[str] = Query(None, description="Filter by municipality (case-insensitive, exact match)"),
    disaster_category: Optional[str] = Query(None, description="Filter by disaster category (case-insensitive, exact match)"),
    disaster_name: Optional[str] = Query(None, description="Filter by specific disaster name"),
    commodity: Optional[str] = Query(None, description="Filter by commodity name"),
    quarter: Optional[int] = Query(None, description="Filter by quarter (1-4)", ge=1, le=4),
    year: Optional[int] = Query(None, description="Filter by year (e.g., 2023)", ge=1900, le=2100),
    start_date: Optional[datetime.date] = Query(None, description="Filter by start date (YYYY-MM-DD) - inclusive"),
    end_date: Optional[datetime.date] = Query(None, description="Filter by end date (YYYY-MM-DD) - inclusive"),
    limit: int = Query(10000, description="Maximum number of records to return", gt=0, le=50000) # Increased limit
):
    """
    Queries the raw disaster data from the Delta Lake table, optionally joins
    with farmer registry data, applies filters, and returns detailed records.
    """
    print(f"API (/api/raw): Received request with filters: start={start_date}, end={end_date}, prov={province}, mun={municipality}, cat={disaster_category}, name={disaster_name}, com={commodity}, year={year}, qtr={quarter}")
    con = None
    safe_lakehouse_path = os.path.normpath(LAKEHOUSE_PATH)
    safe_farmer_registry_path = os.path.normpath(FARMER_REGISTRY_PATH)
    farmer_registry_exists = os.path.exists(safe_farmer_registry_path)

    if not os.path.exists(safe_lakehouse_path):
        print(f"API ERROR (/api/raw): Main Delta table not found at '{safe_lakehouse_path}'.")
        raise HTTPException(status_code=404, detail=f"Main disaster data source not found. Run data import first.")
    if not farmer_registry_exists:
        print(f"API WARNING (/api/raw): Farmer registry table not found at '{safe_farmer_registry_path}'. Proceeding without join.")

    try:
        print("API (/api/raw): Connecting to DuckDB (in-memory)...")
        con = duckdb.connect(read_only=False)
        print(f"API (/api/raw): DuckDB Version: {duckdb.__version__}")
        print(f"API (/api/raw): Deltalake Library Version: {deltalake_version}")

        # --- Read Delta Table Directly ---
        print(f"API (/api/raw): Reading main Delta table from {safe_lakehouse_path}")
        try:
             con.sql("INSTALL delta; LOAD delta;")
             delta_read_function = None
             functions = con.sql("SELECT function_name FROM duckdb_functions() WHERE function_name IN ('read_delta', 'delta_scan')").df()
             if 'read_delta' in functions['function_name'].values: delta_read_function = 'read_delta'
             elif 'delta_scan' in functions['function_name'].values: delta_read_function = 'delta_scan'
             else: raise duckdb.CatalogException("Neither 'read_delta' nor 'delta_scan' found.")

             main_table_read_sql = f"SELECT * FROM {delta_read_function}('{safe_lakehouse_path}')"
             con.sql(f"CREATE OR REPLACE TEMPORARY VIEW main_disasters AS {main_table_read_sql};")
             print("API (/api/raw): Registered main_disasters view using DuckDB Delta extension.")
        except Exception as delta_ext_err:
             print(f"API WARNING (/api/raw): DuckDB Delta extension failed ({delta_ext_err}). Falling back to reading via pandas.")
             dt_main = DeltaTable(safe_lakehouse_path)
             df_main = dt_main.to_pandas()
             # Ensure date columns are datetime after pandas load
             if 'event_date_start' in df_main.columns: df_main['event_date_start'] = pd.to_datetime(df_main['event_date_start'], errors='coerce')
             if 'event_date_end' in df_main.columns: df_main['event_date_end'] = pd.to_datetime(df_main['event_date_end'], errors='coerce')
             # Convert year column explicitly
             if 'year' in df_main.columns: df_main['year'] = pd.to_numeric(df_main['year'], errors='coerce').astype('Int64')

             con.register('main_disasters_df_pandas', df_main)
             con.sql("CREATE OR REPLACE TEMPORARY VIEW main_disasters AS SELECT * FROM main_disasters_df_pandas;")
             print(f"API (/api/raw): Registered main_disasters view using pandas fallback ({len(df_main)} rows).")


        # --- Pre-aggregate Farmer Data if exists ---
        farmer_join_view = None
        select_farmer_cols = ""
        select_calculated_cols = ""
        default_farmer_select = """,
            CAST(NULL AS INTEGER) AS registered_rice_farmers,
            CAST(NULL AS DOUBLE) AS total_declared_rice_area_ha,
            CAST(NULL AS DOUBLE) AS percentage_farmers_affected
        """

        if farmer_registry_exists:
            print(f"API (/api/raw): Reading farmer registry Delta table from {safe_farmer_registry_path}")
            try:
                 farmer_table_read_sql = f"SELECT * FROM {delta_read_function}('{safe_farmer_registry_path}')"
                 con.sql(f"CREATE OR REPLACE TEMPORARY VIEW farmer_registry_raw AS {farmer_table_read_sql};")
                 print("API (/api/raw): Registered farmer_registry_raw view using DuckDB Delta extension.")
            except Exception as delta_ext_err_farmer:
                 print(f"API WARNING (/api/raw): DuckDB Delta extension failed for farmer data ({delta_ext_err_farmer}). Falling back via pandas.")
                 dt_farmer = DeltaTable(safe_farmer_registry_path)
                 df_farmer = dt_farmer.to_pandas()
                 con.register('farmer_registry_df_pandas', df_farmer)
                 con.sql("CREATE OR REPLACE TEMPORARY VIEW farmer_registry_raw AS SELECT * FROM farmer_registry_df_pandas;")
                 print(f"API (/api/raw): Registered farmer_registry_raw view using pandas fallback ({len(df_farmer)} rows).")

            con.sql("""
                CREATE OR REPLACE TEMPORARY VIEW farmer_summary AS
                SELECT
                    province,
                    municipality,
                    CAST(COALESCE(SUM(registered_rice_farmers), 0) AS INTEGER) AS registered_rice_farmers,
                    COALESCE(SUM(total_declared_rice_area_ha), 0.0) AS total_declared_rice_area_ha
                FROM farmer_registry_raw
                GROUP BY province, municipality;
            """)
            farmer_join_view = "farmer_summary"
            select_farmer_cols = """,
                fr.registered_rice_farmers,
                fr.total_declared_rice_area_ha
            """
            select_calculated_cols = """,
                CASE
                    WHEN fr.registered_rice_farmers > 0 THEN ROUND((CAST(d.farmers_affected AS DOUBLE) / fr.registered_rice_farmers) * 100, 2)
                    ELSE NULL
                END AS percentage_farmers_affected
            """
            default_farmer_select = ""
            print("API (/api/raw): Created farmer_summary aggregation view.")


        # --- Dynamically Build WHERE Clause (Applies to disaster table 'd') ---
        where_clauses = []
        params = {}
        if province:
            where_clauses.append("UPPER(d.province) = UPPER($province)")
            params['province'] = province
        if municipality:
            where_clauses.append("UPPER(d.municipality) = UPPER($municipality)")
            params['municipality'] = municipality
        if disaster_category:
            where_clauses.append("UPPER(d.disaster_category) = UPPER($disaster_category)")
            params['disaster_category'] = disaster_category
        if disaster_name:
             where_clauses.append("UPPER(d.disaster_name) = UPPER($disaster_name)")
             params['disaster_name'] = disaster_name
        if commodity:
             where_clauses.append("UPPER(d.commodity) = UPPER($commodity)")
             params['commodity'] = commodity
        if start_date:
            where_clauses.append("CAST(d.event_date_end AS DATE) >= $start_date")
            params['start_date'] = start_date
        if end_date:
            where_clauses.append("CAST(d.event_date_start AS DATE) <= $end_date")
            params['end_date'] = end_date
        if quarter:
            where_clauses.append("quarter(CAST(d.event_date_start AS DATE)) = $quarter")
            params['quarter'] = quarter
        if year:
             where_clauses.append("d.year = $year")
             params['year'] = year

        where_sql = ""
        if where_clauses:
            where_sql = "WHERE " + " AND ".join(where_clauses)

        # --- Construct Final Query with Optional JOIN ---
        join_sql = ""
        if farmer_join_view:
            join_sql = f"""
            LEFT JOIN {farmer_join_view} AS fr
              ON UPPER(d.province) = UPPER(fr.province) AND UPPER(d.municipality) = UPPER(fr.municipality)
            """

        query = f"""
        SELECT
            d.* {select_farmer_cols} {select_calculated_cols} {default_farmer_select}
        FROM
            main_disasters AS d -- Use the view created earlier
        {join_sql} -- Add the join clause if applicable
        {where_sql} -- Apply filters using alias 'd'
        ORDER BY d.event_date_start DESC, d.province, d.municipality -- Use alias 'd'
        LIMIT $limit
        """
        params['limit'] = limit if limit > 0 else 10000

        print(f"API (/api/raw): Executing query: {query}")
        print(f"API (/api/raw): With parameters: {params}")

        result_df = con.execute(query, params).fetchdf()
        print(f"API (/api/raw): Query returned {len(result_df)} rows.")

        # Convert DataFrame to list of dictionaries and clean for JSON
        data = result_df.to_dict(orient='records')
        cleaned_data = clean_data_for_json(data)

        print("API (/api/raw): Returning cleaned data.")
        return {"status": "success", "count": len(cleaned_data), "data": cleaned_data}

    except HTTPException as http_exc:
        raise http_exc # Re-raise FastAPI specific exceptions
    except duckdb.Error as db_err: # Catch DuckDB specific errors
        error_message = f"Database query error: {str(db_err)}"
        print(f"API ERROR (/api/raw): {error_message}")
        import traceback; traceback.print_exc()
        raise HTTPException(status_code=500, detail="Error executing database query. Check server logs.")
    except Exception as e:
        error_message = f"An error occurred during raw data query execution: {str(e)}"
        print(f"API ERROR (/api/raw): {error_message}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail="Could not retrieve raw data. Please check server logs or query parameters.")
    finally:
        if con:
            con.close()
            print("API (/api/raw): Closed DuckDB connection.")

# --- Root Endpoint ---
@app.get("/")
def root():
    return {"message": "DRRM Disaster Analysis API"}

