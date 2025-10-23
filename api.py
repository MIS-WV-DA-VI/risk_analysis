import json
from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import os
import duckdb
import deltalake # <<< Import deltalake to check its version
from typing import Optional
import datetime

# --- Configuration ---
BASE_DIR = '.'
LAKEHOUSE_PATH = os.path.join(BASE_DIR, 'lakehouse_data/lakehouse_disasters')
DUCKDB_FILE = os.path.join(BASE_DIR, 'lakehouse_data/analysis_db.duckdb')

app = FastAPI()

# --- CORS Middleware ---
origins = ["http://localhost", "http://localhost:8000", "http://127.0.0.1", "http://127.0.0.1:8000", "null"]
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
    limit: int = 1000
    ):
    con = None
    safe_lakehouse_path = os.path.normpath(LAKEHOUSE_PATH)

    if not os.path.exists(safe_lakehouse_path):
        raise FileNotFoundError(f"Main Lakehouse table not found at '{safe_lakehouse_path}'. Run data import first.")

    try:
        # --- <<< CHANGE: Using IN-MEMORY again for simplicity, easier to ensure clean state >>> ---
        print("Connecting to DuckDB (in-memory)...")
        con = duckdb.connect(read_only=False)
        # --- <<< PRINT VERSIONS >>> ---
        print(f"DuckDB Version: {duckdb.__version__}")
        print(f"Deltalake Library Version: {deltalake.__version__}")
        # --- <<< END PRINT VERSIONS >>> ---

        # --- <<< CHANGE: Force Uninstall/Install/Load Delta Extension >>> ---
        print("Attempting to FORCE load DuckDB delta extension...")
        try:
            print("Uninstalling delta extension (if exists)...")
            con.sql("FORCE UNINSTALL delta;") # Try to remove any old state
        except Exception as uninstall_err:
            print(f"Note: Uninstall failed (likely not installed): {uninstall_err}")

        try:
            print("Installing delta extension...")
            con.install_extension('delta')
            print("Delta extension installed.")
            print("Loading delta extension...")
            con.load_extension('delta')
            print("Delta extension loaded.")
        except Exception as install_load_err:
             print(f"FATAL: Failed to install or load delta extension: {install_load_err}")
             raise install_load_err
        # --- <<< END FORCE LOAD >>> ---


        # --- Determine Correct Delta Function Name ---
        delta_read_function = None
        # (Same checking logic as before)
        try:
            functions_rd = con.sql("SELECT function_name FROM duckdb_functions() WHERE function_name = 'read_delta'").df()
            if not functions_rd.empty:
                delta_read_function = 'read_delta'
            else:
                functions_ds = con.sql("SELECT function_name FROM duckdb_functions() WHERE function_name = 'delta_scan'").df()
                if not functions_ds.empty:
                    delta_read_function = 'delta_scan'
                else:
                    raise duckdb.CatalogException("Neither 'read_delta' nor 'delta_scan' found.")
        except Exception as check_err:
             print(f"FATAL: Error checking for Delta read functions: {check_err}")
             raise check_err
        print(f"Using Delta read function: '{delta_read_function}'")


        # --- Dynamically Build WHERE Clause ---
        where_clauses = []
        params = {}
        # (Same WHERE clause building logic as before)
        if province:
            where_clauses.append("province ILIKE $province")
            params['province'] = f"%{province}%"
        if municipality:
            where_clauses.append("municipality ILIKE $municipality")
            params['municipality'] = f"%{municipality}%"
        if disaster_category:
            where_clauses.append("disaster_category ILIKE $disaster_category")
            params['disaster_category'] = f"%{disaster_category}%"
        if start_date:
            where_clauses.append("event_date_start >= $start_date")
            params['start_date'] = start_date
        if end_date:
            where_clauses.append("event_date_start <= $end_date")
            params['end_date'] = end_date

        where_sql = ""
        if where_clauses:
            where_sql = "WHERE " + " AND ".join(where_clauses)

        # --- Construct Final Query ---
        query = f"""
        SELECT *
        FROM {delta_read_function}('{safe_lakehouse_path}')
        {where_sql}
        ORDER BY event_date_start DESC NULLS LAST
        LIMIT $limit
        """
        params['limit'] = limit if limit > 0 else 1000

        print(f"Executing query: {query}")
        print(f"With parameters: {params}")
        results = con.execute(query, params).fetchdf()
        print(f"Query returned {len(results)} rows.")

        # Convert date columns for JSON
        if 'event_date_start' in results.columns:
            results['event_date_start'] = results['event_date_start'].dt.strftime('%Y-%m-%d').fillna('N/A')
        if 'event_date_end' in results.columns:
             results['event_date_end'] = results['event_date_end'].dt.strftime('%Y-%m-%d').fillna('N/A')

        return results.to_dict(orient='records')

    except Exception as e:
        print(f"Error querying lakehouse: {e}")
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
    # (Same endpoint parameters as before)
    province: Optional[str] = Query(None, description="Filter by province (case-insensitive, partial match)"),
    municipality: Optional[str] = Query(None, description="Filter by municipality (case-insensitive, partial match)"),
    disaster_category: Optional[str] = Query(None, description="Filter by disaster category (case-insensitive, partial match)"),
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
            limit=limit
        )
        return {"status": "success", "count": len(data), "data": data}
    except FileNotFoundError as e:
         return JSONResponse(status_code=404, content={"status": "error", "message": str(e)})
    except Exception as e:
        error_message = f"An error occurred during query execution: {str(e)}"
        print(error_message)
        return JSONResponse(status_code=500, content={"status": "error", "message": error_message})

