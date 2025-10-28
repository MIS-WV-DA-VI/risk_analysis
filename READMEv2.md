# **Disaster Analysis Local Lakehouse**

This project sets up a local data processing pipeline using Python, DuckDB, and Delta Lake to analyze agricultural disaster data and serve summarized results via a FastAPI API. It also includes steps to process related datasets like farmer registries.

## **Current Workflow:**

1. **Raw Data**:  
   * Place raw disaster CSV files in ./raw\_data/.  
   * Place raw farmer registry XLSX files (e.g., RSBSA Aklan Rice Farmers.xlsx, RSBSA Iloilo Rice Farmers.xlsx) in ./farmer\_registry\_input/.  
   * Place the specific erroneous\_rows.csv file in ./error\_input/.  
2. **Process Farmer Data (XLSX \-\> Delta)**: Run python process\_farmer\_registry.py.  
   * This script reads .xlsx files from farmer\_registry\_input/ that match the pattern RSBSA \[Province Name\] Rice Farmers.xlsx.  
   * It extracts the province name (e.g., "AKLAN", "ILOILO") from the filename and uses it to populate the province column.  
   * It cleans and extracts municipality-level data.  
   * It writes the data to the ./lakehouse\_data/farmer\_registry Delta table, **partitioned by province**.  
   * Original XLSX files are moved to ./farmer\_registry\_input/processed/.  
3. **Process Error Data (CSV \-\> Delta)**: Run python process\_error\_rows.py.  
   * This reads erroneous\_rows.csv from ./error\_input/, validates, cleans, and writes problematic rows to the ./lakehouse\_data/quarantined\_disasters Delta table (always overwrites).  
   * Processed error CSVs are moved to ./error\_input/processed/.  
4. **Import Main Data (CSV \-\> Delta)**: Run python data\_manager.py import.  
   * This finds new disaster CSVs in ./raw\_data/, cleans them, **ensures province/municipality are UPPERCASE**, appends them to the main ./lakehouse\_data/lakehouse\_disasters Delta table, and moves the processed CSVs to ./raw\_data/processed/.  
   * Use \--mode overwrite for the initial load or to reset the main table.  
5. **Analyze & Export (Optional)**: Run python data\_manager.py export.  
   * This uses DuckDB to query the lakehouse\_disasters table, **joining it with the partitioned farmer\_registry table** on province and municipality.  
   * It performs analysis (e.g., calculates percentage\_farmers\_affected) and saves the results to ./api\_output/api\_data.json.  
6. **Serve API**: Run uvicorn api:app \--reload.  
   * This starts a FastAPI server that serves analysis results dynamically by querying the Delta tables (including joins) based on user filters provided via URL parameters (e.g., province, municipality, quarter, year).  
7. **Visualize**: Open index.html in a browser to view an interactive dashboard that fetches data from the running API.

## **Project Structure:**

disaster\_analysis\_local/  
├── raw\_data/                 \# Input for main disaster data (CSVs)  
│   └── processed/            \# Processed disaster CSVs moved here  
├── error\_input/              \# Input for specifically structured error CSVs  
│   └── processed/            \# Processed error CSVs moved here  
├── farmer\_registry\_input/    \# Input for farmer registry data (XLSX)  
│   ├── RSBSA Aklan Rice Farmers.xlsx  
│   ├── RSBSA Iloilo Rice Farmers.xlsx  
│   └── processed/            \# Original XLSX moved here after processing  
├── lakehouse\_data/           \# Stores Delta Lake tables  
│   ├── lakehouse\_disasters/    \# Main cleaned disaster data table  
│   ├── quarantined\_disasters/  \# Cleaned error data table  
│   └── farmer\_registry/        \# Cleaned farmer registry data table  
│       ├── province=AKLAN/     \# (Partitioned by province)  
│       └── province=ILOILO/  
├── api\_output/               \# Output for the static API export (optional)  
│   └── api\_data.json  
├── venv/                     \# Python virtual environment  
├── data\_manager.py           \# Script for main data import/export (applies UPPERCASE, runs JOIN)  
├── process\_error\_rows.py     \# Script for processing specific error CSV format  
├── process\_farmer\_registry.py \# Script to clean XLSX and load farmer data (partitioned)  
├── api.py                    \# FastAPI server script (performs JOIN, supports filters)  
├── index.html                \# HTML dashboard for visualization  
├── .gitignore  
└── README.md                 \# This file

## **Core Technologies:**

* **Python 3**: Main programming language.  
* **Pandas**: For reading CSV/XLSX files and data manipulation.  
* **DuckDB**: Fast in-process analytical database engine for querying Delta tables.  
* **Delta Lake (deltalake library)**: Python library to write Delta tables.  
* **PyArrow**: Dependency for deltalake.  
* **Openpyxl**: Dependency for Pandas to read .xlsx files.  
* **FastAPI**: Modern web framework for building the API.  
* **Uvicorn**: ASGI server to run the FastAPI application.  
* **Chart.js**: JavaScript library for creating charts in index.html.  
* **Tailwind CSS**: For styling index.html.

## **Setup:**

1. Ensure Python 3 and pip are installed.  
2. Create the necessary subdirectories: raw\_data/processed, error\_input/processed, farmer\_registry\_input/processed, lakehouse\_data, api\_output.  
3. Place your raw data files (.csv and .xlsx) in the corresponding input directories.  
4. Open a terminal in the project directory.  
5. Create a virtual environment: python3 \-m venv venv  
6. Activate the environment: source venv/bin/activate (Mac/Linux) or venv\\Scripts\\activate (Windows).  
7. Install libraries:  
   pip install pandas duckdb deltalake fastapi "uvicorn\[standard\]" openpyxl pyarrow

## **Running the System (Step-by-Step Instructions):**

**Prerequisite**: Ensure your terminal is in the disaster\_analysis\_local directory and the virtual environment is activated (source venv/bin/activate).

### **1\. Process Farmer Registry Data**

Reads .xlsx files from farmer\_registry\_input, extracts province from filename, cleans data, and writes to the partitioned farmer\_registry Delta table.

* **Mode: append (Default)**  
  * Adds data to the corresponding partition. If province=AKLAN exists, it adds new rows.  
  * python process\_farmer\_registry.py  
* **Mode: overwrite (Full Reset)**  
  * **Deletes the entire table** (all provinces) and replaces it with the data from the *first* file processed.  
  * python process\_farmer\_registry.py \--mode overwrite  
* **Mode: dynamic\_overwrite (Update a Province)**  
  * **Replaces only the partition** matching the file's province. Keeps all other provinces safe.  
  * This is the recommended mode for re-loading or updating a single province.  
  * python process\_farmer\_registry.py \--mode dynamic\_overwrite

### **2\. Process Erroneous Data**

Reads erroneous\_rows.csv from error\_input, cleans, writes to quarantined\_disasters Delta table (always overwrites).

* **Run**: python process\_error\_rows.py

### **3\. Import Main Disaster Data**

Finds new .csv files in raw\_data, cleans, standardizes keys to **UPPERCASE**, and loads into lakehouse\_disasters Delta table.

* **First Run (or Reset)**:  
  * python data\_manager.py import \--mode overwrite  
* **Subsequent Runs (Adding New Files)**:  
  * python data\_manager.py import

### **4\. Analyze and Export for API (Optional)**

Runs the analysis query, joining lakehouse\_disasters and farmer\_registry tables, and saves the summary to api\_output/api\_data.json.

* **Run**: python data\_manager.py export

### **5\. Start the API Server**

Runs the dynamic FastAPI server. Open a **new terminal window/tab** for this, and activate the environment.

* **Run**: uvicorn api:app \--reload  
* The API will be available at http://localhost:8000.

### **6\. View the Dashboard**

Open index.html in your web browser.

## **Automation: Running the API with systemd (Linux)**

To run the FastAPI server automatically as a background service and have it restart on failure, you can create a systemd service.

### **1\. Create a systemd Service File**

Create a new file:

sudo nano /etc/systemd/system/disaster\_api.service

Paste the following content. **You MUST update the paths** to match your project's location.

\[Unit\]  
Description=Disaster Analysis FastAPI API  
After=network.target

\[Service\]  
\# Replace with your actual user and group  
User=your\_username  
Group=your\_group\_name

\# Update with the absolute path to your project directory  
WorkingDirectory=/home/your\_username/disaster\_analysis\_local

\# Update with the absolute path to your virtual environment's uvicorn  
\# Use a port like 8002 and bind to 127.0.0.1 (for reverse proxy)  
ExecStart=/home/your\_username/disaster\_analysis\_local/venv/bin/uvicorn api:app \--host 127.0.0.1 \--port 8002

Restart=on-failure  
RestartSec=5s

\[Install\]  
WantedBy=multi-user.target

**How to find your paths:**

* WorkingDirectory: Run pwd in your project folder.  
* ExecStart: Run which uvicorn *while your venv is activated*.

### **2\. Example Apache Reverse Proxy**

If you are using Apache as a reverse proxy, you can add a configuration like this to /etc/apache2/sites-available/your-site.conf. This example assumes you're proxying to port 8002 (which matches the systemd file) and are listening on port 8445 with SSL.

\<VirtualHost \*:8445\>  
    ServerName 172.16.60.221

    SSLEngine on  
    SSLCertificateFile /etc/ssl/certs/172.16.60.221.pem  
    SSLCertificateKeyFile /etc/ssl/private/172.16.60.221-key.pem

    ProxyPass / http://127.0.0.1:8002/ connectiontimeout=5 timeout=60  
    ProxyPassReverse / http://127.0.0.1:8002/

    ErrorLog ${APACHE\_LOG\_DIR}/risk-analysis-error.log  
    CustomLog ${APACHE\_LOG\_DIR}/risk-analysis-access.log combined  
\</VirtualHost\>

### **3\. Configure Apache Ports and Firewall**

To make the Apache configuration work, you need to tell Apache to listen on that port and allow it through your firewall.

**a) Tell Apache to Listen on Port 8445**

Edit your ports.conf file:

sudo nano /etc/apache2/ports.conf

Add the following line (if it's not already there):

Listen 8445

**b) Allow Port 8445 through ufw Firewall**

If you are using ufw (Uncomplicated Firewall), run the following command to allow traffic on your new port:

sudo ufw allow 8445/tcp comment 'Allow Apache for Disaster API'  
sudo ufw reload

### **4\. Reload and Enable the Service**

Run the following commands to tell systemd about your new service and enable it to start on boot:

\# Reload the systemd daemon  
sudo systemctl daemon-reload

\# Enable the service (starts on boot)  
sudo systemctl enable disaster\_api.service

\# Start the service immediately  
sudo systemctl start disaster\_api.service

### **5\. Check Service Status**

You can check if the API is running correctly:

\# Check the status  
sudo systemctl status disaster\_api.service

\# View live logs (press Ctrl+C to exit)  
sudo journalctl \-u disaster\_api.service \-f  
