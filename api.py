import json
from fastapi import FastAPI
from fastapi.responses import JSONResponse
import os

# --- Configuration ---
# Using relative paths for local execution
BASE_DIR = '.' # Current directory
API_OUTPUT_FILE = os.path.join(BASE_DIR, 'api_output/api_data.json')

app = FastAPI()

@app.get("/")
def root():
    """
    A simple root endpoint to check if the server is running.
    """
    return {"message": "Disaster Analysis API is running. Go to /api/disaster_summary"}

@app.get("/api/disaster_summary")
def get_disaster_summary():
    """
    Reads the pre-computed analysis results from the
    JSON file and returns them.
    """
    # Ensure the parent directory exists, although handle_export should create it
    os.makedirs(os.path.dirname(API_OUTPUT_FILE), exist_ok=True)

    if not os.path.exists(API_OUTPUT_FILE):
        return JSONResponse(
            status_code=404,
            content={"status": "error", "message": f"Analysis data file not found at {API_OUTPUT_FILE}. Run the 'export' command in data_manager.py first."}
        )

    try:
        # Open the file our transform.py script created
        with open(API_OUTPUT_FILE, 'r') as f:
            data = json.load(f)
        return {"status": "success", "data": data}
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"status": "error", "message": str(e)}
        )

# If running directly (e.g., python api.py), this helps start the server
# But typically you'd use: uvicorn api:app --reload
if __name__ == "__main__":
    import uvicorn
    print(f"Attempting to start API server. Access at http://127.0.0.1:8000")
    print(f"Serving data from: {os.path.abspath(API_OUTPUT_FILE)}")
    uvicorn.run(app, host="127.0.0.1", port=8000)
