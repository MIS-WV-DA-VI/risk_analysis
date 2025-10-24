# **Disaster Analysis Local Lakehouse**

This project sets up a local data processing pipeline using Python, DuckDB, and Delta Lake to analyze agricultural disaster data and serve summarized results via a FastAPI API. It also includes steps to process related datasets like farmer registries.

## **Current Workflow:**

* **Raw Data:** Place raw disaster CSV files in ./raw\_data/, the raw farmer registry XLSX files in ./farmer\_registry\_input/, and the specific erroneous\_rows.csv file in ./error\_input/.  
* **Process Farmer Data (XLSX \-\> Delta):** Run python process\_farmer\_registry.py. This script finds all files in ./farmer\_registry\_input/ matching the pattern RSBSA \[Province Name\] Rice Farmers\*.xlsx. It automatically extracts the province name from the filename, cleans the municipality data from within the file, and writes the data to the ./lakehouse\_data/farmer\_registry Delta table, **partitioning the data by province**. Use \--mode overwrite for the initial load or to reset the table; defaults to append. The original processed XLSX files are moved to ./farmer\_registry\_input/processed/.  
* **Process Error Data (CSV \-\> Delta):** Run python process\_error\_rows.py. This reads erroneous\_rows.csv from ./error\_input/, validates, cleans types, and writes problematic rows to the ./lakehouse\_data/quarantined\_disasters Delta table (overwriting). Processed error CSVs are moved to ./error\_input/processed/.  
* **Import Main Data (CSV \-\> Delta):** Run python data\_manager.py import. This finds new disaster CSVs in ./raw\_data/, cleans them, ensures province/municipality are UPPERCASE, appends them to the main ./lakehouse\_data/lakehouse\_disasters Delta table, and moves the processed CSVs to ./raw\_data/processed/. Use \--mode overwrite for the initial load or to reset the main table.  
* **Analyze & Export (Optional):** Run python data\_manager.py export. This uses DuckDB to query the main lakehouse\_disasters table (potentially joining with farmer\_registry), performs analysis, and saves the results to ./api\_output/api\_data.json. This step is optional if only using the dynamic API.  
* **Serve API:** Run uvicorn api:app \--reload. This starts a FastAPI server that serves analysis results dynamically by querying the Delta tables (including joins) based on user filters provided via URL parameters.  
* **Visualize:** Open index.html in a browser to view an interactive dashboard that fetches data from the running API.

## **Project Structure:**

disaster\_analysis\_local/  
├── raw\_data/ \# Input for main disaster data (CSVs)  
│ ├── clean\_data.xlsx \- Sheet1.csv  
│ └── processed/ \# Successfully processed disaster CSVs moved here  
├── error\_input/ \# Input for specifically structured error CSVs  
│ ├── erroneous\_rows.csv  
│ └── processed/ \# Successfully processed error CSVs moved here  
├── farmer\_registry\_input/ \# Input for farmer registry data (XLSX)  
│ ├── RSBSA Aklan Rice Farmers.xlsx  
│ └── processed/ \# Original XLSX moved here after processing  
├── lakehouse\_data/ \# Stores Delta Lake tables  
│ ├── lakehouse\_disasters/ \# Main cleaned disaster data table (UPPERCASE keys)  
│ ├── quarantined\_disasters/ \# Cleaned error data table  
│ └── farmer\_registry/ \# Cleaned farmer registry data table (UPPERCASE keys)  
├── api\_output/ \# Output for the static API export (optional)  
│ └── api\_data.json  
├── logs/ \# Optional: For cron job logs  
├── venv/ \# Python virtual environment  
├── data\_manager.py \# Script for main data import/export (applies UPPERCASE)  
├── process\_error\_rows.py \# Script for processing specific error CSV format  
├── process\_farmer\_registry.py \# Script to clean XLSX and load farmer data to Delta  
├── api.py \# FastAPI server script (performs JOIN)  
├── index.html \# HTML dashboard for visualization  
├── .gitignore \# Specifies files/dirs for Git to ignore  
└── README.md \# This file

## **Core Technologies:**

* **Python 3:** Main programming language.  
* **Pandas:** For reading CSV/XLSX files and data manipulation.  
* **DuckDB:** Fast in-process analytical database engine for querying Delta tables.  
* **Delta Lake (deltalake library):** Python library to write Delta tables (using Parquet \+ transaction log).  
* **PyArrow:** Dependency for deltalake to handle Parquet files. Installed via pip install pyarrow.  
* **Openpyxl:** Dependency for Pandas to read .xlsx files. Install via pip install openpyxl.  
* **FastAPI:** Modern web framework for building the API.  
* **Uvicorn:** ASGI server to run the FastAPI application.  
* **Chart.js:** JavaScript library for creating charts in index.html.  
* **Tailwind CSS:** For styling index.html.

## **Setup:**

1. Ensure Python 3 and pip are installed.  
2. Clone or create the project directory (disaster\_analysis\_local).  
3. Create the necessary subdirectories: raw\_data, raw\_data/processed, error\_input, error\_input/processed, farmer\_registry\_input, farmer\_registry\_input/processed, lakehouse\_data, api\_output.  
4. Place your raw data files (.csv and .xlsx) in the corresponding input directories (raw\_data, error\_input, farmer\_registry\_input).  
5. Create the Python script files (data\_manager.py, process\_error\_rows.py, process\_farmer\_registry.py, api.py) and the index.html, .gitignore, README.md files with the provided content.  
6. Open a terminal in the project directory.  
7. Create a virtual environment: python3 \-m venv venv  
8. Activate the environment: source venv/bin/activate (Mac/Linux) or venv\\Scripts\\activate (Windows).  
9. Install libraries:  
   pip install pandas duckdb deltalake fastapi "uvicorn\[standard\]" openpyxl pyarrow

## **Running the System (Step-by-Step Instructions):**

**Prerequisite:** Ensure your terminal is in the disaster\_analysis\_local directory and the virtual environment is activated (source venv/bin/activate).

1. **Process Farmer Registry Data:**  
   * Reads .xlsx files from farmer\_registry\_input matching the RSBSA \[Province Name\] Rice Farmers\*.xlsx pattern. It extracts the province, cleans municipality data, and writes to the province-partitioned farmer\_registry Delta table. Moves original XLSX files to ./processed/.  
   * **First Run (or Reset):**  
     python process\_farmer\_registry.py \--mode overwrite

   * **Subsequent Runs (Append Mode):**  
     python process\_farmer\_registry.py

2. **Process Erroneous Data:**  
   * Reads erroneous\_rows.csv from error\_input, cleans, writes to quarantined\_disasters Delta table (always overwrites). Moves CSV to ./processed/.  
   * **Run:**  
     python process\_error\_rows.py

3. **Import Main Disaster Data:**  
   * Finds new .csv files in raw\_data, cleans, standardizes keys to UPPERCASE, loads into lakehouse\_disasters Delta table. Moves CSVs to ./processed/.  
   * **First Run (or Reset):** Crucial to run with overwrite after ensuring farmer data is processed correctly.  
     python data\_manager.py import \--mode overwrite

   * **Subsequent Runs (Adding New Files):** Uses append mode by default.  
     python data\_manager.py import

4. **Analyze and Export for API (Optional):**  
   * Runs analysis query (potentially joining tables) and saves summary to api\_output/api\_data.json.  
   * **Run:**  
     python data\_manager.py export

5. **Start the API Server:**  
   * Runs the dynamic FastAPI server. Open a **new terminal window/tab** for this, activate the environment.  
   * **Run:**  
     uvicorn api:app \--reload  
