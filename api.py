import json
from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import os
import duckdb
import deltalake # <<< Import deltalake to check its version
from typing import Optional
import datetime
import math # For checking NaN
import numpy as np # Import numpy for replacing infinite values
import pandas as pd # Ensure pandas is imported if not already globally

# --- Configuration ---
BASE_DIR = '.'
LAKEHOUSE_PATH = os.path.join(BASE_DIR, 'lakehouse_data/lakehouse_disasters')
# --- <<< ADDED: Path to Farmer Registry Delta table >>> ---
FARMER_REGISTRY_PATH = os.path.join(BASE_DIR, 'lakehouse_data/farmer_registry')
# DUCKDB_FILE = os.path.join(BASE_DIR, 'lakehouse_data/analysis_db.duckdb') # Using in-memory now

app = FastAPI()

# --- CORS Middleware ---
origins = ["http://localhost", "http://localhost:8000", "http://127.0.0.1", "http://127.0.0.1:8000", "https://dawvinfosys.test", "https://27.110.161.135", "null"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Helper Function to Query DuckDB ---
def query_lakehouse(
    province: Optional[str] = None,
    municipality: Optional[str] = None,
    start_date: Optional[datetime.date] = None,
    end_date: Optional[datetime.date] = None,
    disaster_category: Optional[str] = None,
    quarter: Optional[int] = None, # <<< ADDED: Quarter filter
    year: Optional[int] = None, # <<< ADDED: Year filter
    limit: int = 1000
    ):
    con = None
    safe_lakehouse_path = os.path.normpath(LAKEHOUSE_PATH)
    # --- <<< ADDED: Path and check for Farmer Registry >>> ---
    safe_farmer_registry_path = os.path.normpath(FARMER_REGISTRY_PATH)
    farmer_registry_exists = os.path.exists(safe_farmer_registry_path)

    if not os.path.exists(safe_lakehouse_path):
        raise FileNotFoundError(f"Main Lakehouse table not found at '{safe_lakehouse_path}'. Run data import first.")
    if not farmer_registry_exists:
        print(f"Warning: Farmer registry table not found at '{safe_farmer_registry_path}'. Proceeding without join.")


    try:
        print("Connecting to DuckDB (in-memory)...")
        con = duckdb.connect(read_only=False)
        print(f"DuckDB Version: {duckdb.__version__}")
        print(f"Deltalake Library Version: {deltalake.__version__}")

        # --- Force Load Delta Extension ---
        print("Attempting to FORCE load DuckDB delta extension...")
        try:
            con.sql("FORCE UNINSTALL delta;")
        except Exception: pass # Ignore if uninstall fails
        try:
            con.install_extension('delta')
            con.load_extension('delta')
            print("Delta extension loaded.")
        except Exception as install_load_err:
             print(f"FATAL: Failed to install or load delta extension: {install_load_err}")
             raise install_load_err

        # --- Determine Correct Delta Function Name ---
        delta_read_function = None
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


        # --- Dynamically Build WHERE Clause (Applies to disaster table 'd') ---
        where_clauses = []
        params = {}
        if province:
            # Use UPPER() for precise, case-insensitive matching (like in import)
            where_clauses.append("UPPER(d.province) = UPPER($province)")
            params['province'] = province
        if municipality:
            # Use UPPER() for precise, case-insensitive matching
            where_clauses.append("UPPER(d.municipality) = UPPER($municipality)")
            params['municipality'] = municipality
        if disaster_category:
            # Use UPPER() for precise, case-insensitive matching
            where_clauses.append("UPPER(d.disaster_category) = UPPER($disaster_category)")
            params['disaster_category'] = disaster_category
        if start_date:
            where_clauses.append("d.event_date_start >= $start_date")
            params['start_date'] = start_date
        if end_date:
            # Use event_date_start for range check for simplicity
            where_clauses.append("d.event_date_start <= $end_date")
            params['end_date'] = end_date
        
        # <<< ADDED: Quarter filter logic >>>
        if quarter:
            where_clauses.append("quarter(d.event_date_start) = $quarter")
            params['quarter'] = quarter
            
        # <<< ADDED: Year filter logic >>>
        if year:
            where_clauses.append("year(d.event_date_start) = $year")
            params['year'] = year

        where_sql = ""
        if where_clauses:
            where_sql = "WHERE " + " AND ".join(where_clauses)

        # --- Construct Final Query with JOIN ---
        join_sql = ""
        select_farmer_cols = ""
        select_calculated_cols = ""

        # Define default null values for farmer columns in case join fails or registry doesn't exist
        default_farmer_select = """,
            NULL AS registered_rice_farmers,
            NULL AS total_declared_rice_area_ha,
            NULL AS percentage_farmers_affected
        """
        
        # --- <<< ADDED: Pre-aggregate Farmer Data for Join >>> ---
        # This is more robust as it groups by both province and municipality
        farmer_join_view = ""
        if farmer_registry_exists:
            con.sql(f"""
                CREATE OR REPLACE TEMPORARY VIEW farmer_summary AS
                SELECT
                    province,
                    municipality,
                    SUM(registered_rice_farmers) AS registered_rice_farmers,
                    SUM(total_declared_rice_area_ha) AS total_declared_rice_area_ha
                FROM {delta_read_function}('{safe_farmer_registry_path}')
                GROUP BY province, municipality;
            """)
            farmer_join_view = "farmer_summary" # Use the view for the join

            # Columns to select if join is successful
            select_farmer_cols = """,
                fr.registered_rice_farmers,
                fr.total_declared_rice_area_ha
            """
            # Calculation to perform if join is successful
            select_calculated_cols = """,
                CASE
                    WHEN fr.registered_rice_farmers > 0 THEN ROUND((CAST(d.farmers_affected AS DOUBLE) / fr.registered_rice_farmers) * 100, 2)
                    ELSE NULL
                END AS percentage_farmers_affected
            """
            # The join clause itself
            join_sql = f"""
            LEFT JOIN {farmer_join_view} AS fr
              ON UPPER(d.province) = UPPER(fr.province) AND UPPER(d.municipality) = UPPER(fr.municipality)
            """
            # Use the specific select columns if joining
            default_farmer_select = "" # Clear default if join happens

        # Combine parts into the final query
        query = f"""
        SELECT
            d.* {select_farmer_cols} {select_calculated_cols} {default_farmer_select}
        FROM
            {delta_read_function}('{safe_lakehouse_path}') AS d -- Alias main table as 'd'
        {join_sql} -- Add the join clause if applicable
        {where_sql} -- Apply filters using alias 'd'
        ORDER BY d.losses_php_grand_total DESC NULLS LAST -- Changed sort to losses
        LIMIT $limit
        """
        params['limit'] = limit if limit > 0 else 1000

        print(f"Executing query: {query}")
        print(f"With parameters: {params}")
        # fetchdf() should handle basic type conversions from DuckDB to Pandas
        results = con.execute(query, params).fetchdf()
        print(f"Query returned {len(results)} rows.")

        # --- Post-process for JSON Compliance ---
        print("Cleaning results for JSON compliance (handling NaN, NaT, Inf)...")

        # Handle Date columns specifically
        for col in ['event_date_start', 'event_date_end']:
            if col in results.columns:
                # Convert to datetime if not already (might be object if contains errors)
                results[col] = pd.to_datetime(results[col], errors='coerce')
                # Format valid dates, replace NaT with None
                results[col] = results[col].dt.strftime('%Y-%m-%d').where(results[col].notna(), None)

        # Replace standard non-JSON compliant floats globally
        results = results.replace([np.inf, -np.inf], None) # Replace Inf/-Inf first
        # Replace remaining NaN specifically with None (more reliable than global replace for mixed types)
        results = results.where(pd.notna(results), None)

        # --- <<< FIX: Robust Integer Conversion >>> ---
        # Convert columns intended to be integers
        int_cols = ['year', 'farmers_affected', 'registered_rice_farmers'] # 'source_row_number' removed as it's not always present
        for col in int_cols:
             if col in results.columns:
                 # 1. Convert to numeric, coercing errors to NaN
                 # 2. Fill NaN (resulting from coercion or None) with 0
                 # 3. Cast to integer
                 results[col] = pd.to_numeric(results[col], errors='coerce').fillna(0).astype(int)
        print("Ensured integer columns are integers.")
        # --- <<< END FIX >>> ---


        # Ensure float columns are rounded where appropriate and handle potential None (now 0.0)
        float_cols = ['area_partially_damaged_ha', 'area_totally_damaged_ha', 'area_total_affected_ha',
                      'losses_php_production_cost', 'losses_php_farm_gate', 'losses_php_grand_total',
                      'total_declared_rice_area_ha', 'percentage_farmers_affected']
        for col in float_cols:
             if col in results.columns:
                 # Ensure numeric, fill remaining issues with 0.0
                 results[col] = pd.to_numeric(results[col], errors='coerce').fillna(0.0)
                 # Round percentage if it exists and is not None/0.0
                 if col == 'percentage_farmers_affected':
                     results[col] = results[col].round(2)
        print("Ensured float columns are floats and rounded percentage.")


        print("JSON cleaning complete.")

        # Convert entire DataFrame to dictionary records
        return results.to_dict(orient='records')

    except Exception as e:
        print(f"Error querying lakehouse: {e}")
        # Log the full traceback for detailed debugging
        import traceback
        traceback.print_exc()
        raise
    finally:
        if con:
            con.close()
            print("Closed DuckDB connection.")

# --- Root and API Endpoint ---
@app.get("/")
def root():
    return {"message": "Disaster Analysis API is running. Go to /api/disaster_summary"}

@app.get("/api/disaster_summary")
def get_disaster_summary(
    province: Optional[str] = Query(None, description="Filter by province (case-insensitive, exact match)"),
    municipality: Optional[str] = Query(None, description="Filter by municipality (case-insensitive, exact match)"),
    disaster_category: Optional[str] = Query(None, description="Filter by disaster category (case-insensitive, exact match)"),
    quarter: Optional[int] = Query(None, description="Filter by quarter (1-4)", ge=1, le=4), # <<< ADDED
    year: Optional[int] = Query(None, description="Filter by year (e.g., 2023)", ge=1900, le=2100), # <<< ADDED
    start_date: Optional[datetime.date] = Query(None, description="Filter by start date (YYYY-MM-DD) - inclusive"),
    end_date: Optional[datetime.date] = Query(None, description="Filter by end date (YYYY-MM-DD) - inclusive"),
    limit: int = Query(100, description="Maximum number of records to return", gt=0, le=5000)
):
    try:
        data = query_lakehouse(
            province=province,
            municipality=municipality,
            start_date=start_date,
            end_date=end_date,
            disaster_category=disaster_category,
            quarter=quarter, # <<< ADDED
            year=year, # <<< ADDED
            limit=limit
        )
        return {"status": "success", "count": len(data), "data": data}
    except FileNotFoundError as e:
         return JSONResponse(status_code=404, content={"status": "error", "message": str(e)})
    except Exception as e:
        error_message = f"An error occurred during query execution: {str(e)}"
        print(error_message) # Log the detailed error on the server
        # Add traceback print here too for API level errors during query call
        import traceback
        traceback.print_exc()

        # Check specifically for the ValueError that indicates JSON compliance issues
        if isinstance(e, ValueError) and "JSON compliant" in str(e):
             error_message = "Data contains non-JSON compliant values (NaN/Inf) after processing."
             print("Potential NaN/Inf values were not fully cleaned.")
             return JSONResponse(status_code=500, content={"status": "error", "message": error_message})
        # Check for the specific TypeError related to casting
        elif isinstance(e, TypeError) and "cannot safely cast" in str(e):
             error_message = "Data type conversion error during processing. Check server logs."
             print(f"Casting TypeError occurred: {e}")
             return JSONResponse(status_code=500, content={"status": "error", "message": error_message})


        return JSONResponse(status_code=500, content={"status": "error", "message": "Could not retrieve data. Please check server logs or query parameters."})

