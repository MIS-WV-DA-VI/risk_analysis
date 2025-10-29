import pandas as pd
import os
import re
from datetime import datetime
from calendar import monthrange

# --- CONFIGURATION ---
INPUT_FILENAME = 'source_data.xlsx'
TARGET_SHEET_NAME = 'Consolidated 2010 - Present'
CLEAN_OUTPUT_FILENAME = 'clean_data.csv' # Changed to CSV
ERROR_OUTPUT_FILENAME = 'erroneous_rows.csv' # Changed to CSV

# This mapping is updated to handle the column names in your consolidated file.
COLUMN_MAPPING = {
    'YEAR (DATE OF OCCURENCE)': 'year',
    'ACTUAL DATE OF OCCURENCE': 'date_range_str',
    'PROVINCE AFFECTED': 'province',
    'MUNICIPALITY AFFECTED': 'municipality',
    'COMMODITY': 'commodity',
    'DISASTER TYPE': 'disaster_type_raw',
    'HYDROMETEOROLOGICAL EVENTS / GENERAL DISASTER EVENTS': 'disaster_category',
    'NAME OF DISASTER': 'disaster_name',
    'Totally Damaged (AREA AFFECTED (HA.) / MORTALITY HEADS / NO. OF UNITS AFFECTED)': 'area_totally_damaged_ha',
    'Partially Damaged (AREA AFFECTED (HA.) / MORTALITY HEADS / NO. OF UNITS AFFECTED)': 'area_partially_damaged_ha',
    'TOTAL (AREA AFFECTED (HA.) / MORTALITY HEADS / NO. OF UNITS AFFECTED)': 'area_total_affected_ha',
    'NUMBER OF FARMERS AFFECTED': 'farmers_affected',
    'GRAND TOTAL': 'losses_php_grand_total',
    'Total Value (Based on Cost of Production / Inputs)': 'losses_php_production_cost',
    'Total Value - Based on Farm Gate Price': 'losses_php_farm_gate',
    'Volume (MT) - Based on Farm Gate Price ': 'volume_loss_mt' # Note the trailing space
}


def clean_numeric_column(series):
    """ Cleans a pandas Series expecting numeric data. Handles commas, hyphens, and errors. """
    return pd.to_numeric(
        series.astype(str).str.replace(',', '', regex=False).str.strip().replace('-', '0', regex=False),
        errors='coerce'
    ).fillna(0)


def parse_date_range_smart(date_str, year):
    """
    Smarter date parser that handles various inconsistent formats and returns a remark.
    Returns: (start_date, end_date, remark) or (None, None, None) on failure.
    """
    if pd.isna(date_str) or pd.isna(year):
        return None, None, None

    # Handle if date_str is already a datetime object from pandas
    if isinstance(date_str, datetime):
        date = date_str.date()
        remark = "Parsed from a native Excel date format."
        return date, date, remark

    date_str = str(date_str).strip()
    # Ensure year is treated as integer if possible
    try:
        year = int(year)
    except (ValueError, TypeError):
         return None, None, "Invalid Year column value" # Added remark for invalid year
    
    try:
        # --- NEW LOGIC: Try specific text patterns FIRST ---
        
        # Pattern 1 & 4 Combined: "Month Day, Year" OR "Month Day-Day, Year"
        match = re.match(r'^([a-zA-Z]+)\s+(\d{1,2})(?:\s*-\s*(\d{1,2}))?,\s*(\d{4})$', date_str, re.IGNORECASE)
        if match:
            month_str, start_day, end_day, year_from_str = match.groups()
            year_val = int(year_from_str)
            start_date = datetime.strptime(f"{month_str} {start_day} {year_val}", "%B %d %Y").date()
            
            if end_day: # A range was found, e.g., "15-17"
                end_date = start_date.replace(day=int(end_day))
                remark = "Parsed from 'Month Day-Day, Year' format."
            else: # It's a single day
                end_date = start_date
                remark = "Parsed as a single day event."
            return start_date, end_date, remark

        # Pattern 2: "Month-Month Year" e.g., "July-August 2021"
        match = re.match(r'(\w+)-(\w+)\s+(\d{4})', date_str, re.IGNORECASE)
        if match:
            start_month_str, end_month_str, year_from_str = match.groups()
            year_val = int(year_from_str)
            start_date = datetime.strptime(f"{start_month_str} 1 {year_val}", "%B %d %Y").date()
            end_month_dt = datetime.strptime(f"{end_month_str} 1 {year_val}", "%B %d %Y")
            last_day = monthrange(end_month_dt.year, end_month_dt.month)[1]
            end_date = end_month_dt.replace(day=last_day).date()
            remark = "Parsed from month-only range; assumed full month coverage."
            return start_date, end_date, remark

        # Pattern 3: "Month Year" e.g., "November 2012"
        match = re.match(r'(\w+)\s+(\d{4})$', date_str, re.IGNORECASE)
        if match and '-' not in date_str and 'to' not in date_str.lower():
            month_str, year_from_str = match.groups()
            year_val = int(year_from_str)
            start_date = datetime.strptime(f"{month_str} 1 {year_val}", "%B %d %Y").date()
            last_day = monthrange(start_date.year, start_date.month)[1]
            end_date = start_date.replace(day=last_day)
            remark = "Parsed from month-only value; assumed full month."
            return start_date, end_date, remark
            
        # --- Fallback to pd.to_datetime for standard formats like YYYY-MM-DD ---
        # Check if the string purely matches a standard format first
        if re.match(r'^\d{4}-\d{2}-\d{2}(?:\s+\d{2}:\d{2}:\d{2})?$', date_str):
            dt_obj = pd.to_datetime(date_str, errors='coerce')
            if not pd.isna(dt_obj):
                date = dt_obj.date()
                remark = "Parsed from a standard timestamp format."
                return date, date, remark

        # --- Final fallback for other complex text ranges ---
        parts = re.split(r'\s*-\s*|\s+to\s+', date_str, flags=re.IGNORECASE)
        start_str = parts[0]
        end_str = parts[-1]
        
        remark_parts = []

        # Process Start String
        match_start_year = re.search(r'(\d{4})', start_str)
        start_year = int(match_start_year.group(1)) if match_start_year else year
        start_str_clean = re.sub(r'\s*,?\s*\d{4}', '', start_str).strip()
        
        try:
            start_date = datetime.strptime(start_str_clean, "%B %d").replace(year=start_year).date()
        except ValueError:
            # Handle month-only start, e.g., "December 2009 to..."
            try:
                start_date = datetime.strptime(start_str_clean, "%B").replace(year=start_year, day=1).date()
                remark_parts.append("Start date assumed as 1st of month.")
            except ValueError: # If month name is invalid
                return None, None, "Invalid start month name"

        # Process End String
        match_end_year = re.search(r'(\d{4})', end_str)
        # Use start_year as default if no year found in end string
        end_year = int(match_end_year.group(1)) if match_end_year else start_year
        end_str_clean = re.sub(r'\s*,?\s*\d{4}', '', end_str).strip()
        
        if len(parts) == 1:
            end_date = start_date
            # Check if it should have matched Pattern 4 but failed month name
            if not re.match(r'^([a-zA-Z]+)\s+(\d{1,2})$', start_str_clean):
                 remark_parts.append("Parsed as a single day event.")
        # Handle simple day range like "15-17"
        elif len(end_str_clean.split()) == 1 and end_str_clean.isdigit():
            end_date = start_date.replace(day=int(end_str_clean))
            remark_parts.append("Parsed as a date range within the same month.")
        else: # Handle "Month Day" or just "Month"
            try:
                end_date = datetime.strptime(end_str_clean, "%B %d").replace(year=end_year).date()
            except ValueError:
                 # Handle month-only end, e.g., "...to May 2016"
                try:
                    end_month_dt = datetime.strptime(end_str_clean, "%B").replace(year=end_year)
                    last_day = monthrange(end_month_dt.year, end_month_dt.month)[1]
                    end_date = end_month_dt.replace(day=last_day).date()
                    remark_parts.append("End date assumed as last day of month.")
                except ValueError: # If month name is invalid
                    return None, None, "Invalid end month name"
        
        if not remark_parts:
             remark_parts.append("Parsed as a standard date range.")
            
        return start_date, end_date, " ".join(remark_parts)

    except Exception as e:
        # print(f"DEBUG: Failed parsing '{date_str}' with year {year}. Error: {e}") # Uncomment for debugging
        return None, None, None


def sanitize_data(input_path, sheet_name, clean_path, error_path):
    """ Main function to orchestrate the data sanitation process. """
    print(f"--- Starting Data Sanitation for '{input_path}' ---")
    if not os.path.exists(input_path):
        print(f"Error: Input file not found at '{input_path}'.")
        return
    try:
        df = pd.read_excel(input_path, sheet_name=sheet_name, header=1, engine='openpyxl')
        
        df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
        df = df.dropna(how='all')
        original_row_count = len(df)
        print(f"Loaded {original_row_count} rows from sheet '{sheet_name}'.")
    except ValueError as e:
        print(f"Error reading Excel sheet: {e}. Make sure a sheet named '{sheet_name}' exists.")
        return

    df_original = df.copy()
    df_original['source_row_number'] = df.index + 3

    df_processed = df.rename(columns=lambda c: c.strip())
    df_processed = df_processed.rename(columns=COLUMN_MAPPING)
    
    # Clean year first as it's needed for date parsing
    df_processed['year'] = pd.to_numeric(df_processed.get('year'), errors='coerce')
    
    numeric_cols = list(set(COLUMN_MAPPING.values()) - {'year', 'date_range_str', 'province', 'municipality', 'commodity', 'disaster_type_raw', 'disaster_category', 'disaster_name'})
    for col in numeric_cols:
        if col in df_processed.columns:
            df_processed[col] = clean_numeric_column(df_processed[col])

    if 'commodity' in df_processed.columns:
        df_processed['commodity'] = df_processed['commodity'].astype(str).str.replace(r'^\d+\s*-\s*', '', regex=True).str.strip()
    
    # Parse dates using the potentially cleaned year
    parsed_dates = df_processed.apply(lambda row: parse_date_range_smart(row.get('date_range_str'), row.get('year')), axis=1)
    df_processed[['event_date_start', 'event_date_end', 'sanitation_remarks']] = pd.DataFrame(parsed_dates.tolist(), index=df_processed.index)
    # Convert dates explicitly to datetime objects for comparison (errors='coerce' will turn failures into NaT)
    df_processed['event_date_start'] = pd.to_datetime(df_processed['event_date_start'], errors='coerce')
    df_processed['event_date_end'] = pd.to_datetime(df_processed['event_date_end'], errors='coerce')


    error_reasons = []
    for index, row in df_processed.iterrows():
        reasons = []
        year_val = row.get('year')
        start_date = row.get('event_date_start')
        end_date = row.get('event_date_end')
        
        # --- Basic Validation ---
        if pd.isna(row.get('province')) or str(row.get('province')).strip() == '':
            reasons.append("Missing essential field (province, municipality, or commodity).")
        if pd.isna(year_val):
            reasons.append("Year is not a valid number.")
        if pd.isna(start_date): # Check if date parsing failed
             # Get original date string from the unprocessed dataframe for the error message
            original_date_str_series = df_original.loc[index, 'ACTUAL DATE OF OCCURENCE']
            original_date_str = original_date_str_series if isinstance(original_date_str_series, str) else str(original_date_str_series)
            reasons.append(f"Unparseable date: '{original_date_str}'")
        if row.get('losses_php_grand_total', 0) == 0:
            reasons.append("Missing or zero Grand Total for PHP loss.")
        
        # --- Date Logic Validation (only if dates are valid) ---
        if pd.notna(start_date) and pd.notna(end_date):
            # Check 1: Start date <= End date
            if start_date > end_date:
                reasons.append(f"Date range invalid: Start date ({start_date.date()}) is after end date ({end_date.date()}).")
            
            # Check 2: Year consistency (allow ±1 year difference from the 'year' column)
            if pd.notna(year_val):
                year_int = int(year_val)
                if abs(start_date.year - year_int) > 1:
                     reasons.append(f"Date inconsistency: Start year ({start_date.year}) doesn't match reported year ({year_int} ±1).")
                if abs(end_date.year - year_int) > 1:
                     reasons.append(f"Date inconsistency: End year ({end_date.year}) doesn't match reported year ({year_int} ±1).")

        # --- Area Consistency Validation ---
        partial = row.get('area_partially_damaged_ha', 0)
        totally = row.get('area_totally_damaged_ha', 0)
        total = row.get('area_total_affected_ha', 0)
        
        if (partial > 0 or totally > 0) and total > 0:
            if not abs((partial + totally) - total) < 0.01: # Use tolerance for float comparison
                reasons.append(f"Area inconsistency: Partial({partial}) + Totally({totally}) != Total({total}).")
        
        error_reasons.append("; ".join(reasons))

    # Add error reasons back to the original dataframe for the error report
    df_original['error_reason'] = error_reasons
    
    # Separate clean and erroneous rows
    is_erroneous = df_original['error_reason'] != ''
    erroneous_rows = df_original[is_erroneous].copy()
    # For clean data, use the processed dataframe
    clean_rows = df_processed[~is_erroneous].copy()
    
    # --- Perform Year Comparison Summary on Clean Data ---
    if not clean_rows.empty:
        clean_rows['year'] = clean_rows['year'].astype(int) 
        start_year_less_than_year = clean_rows[clean_rows['event_date_start'].dt.year < clean_rows['year']]
        start_year_equal_to_year = clean_rows[clean_rows['event_date_start'].dt.year == clean_rows['year']]
        count_less_than = len(start_year_less_than_year)
        count_equal_to = len(start_year_equal_to_year)
        print("\n--- Year Comparison Summary (Clean Rows Only) ---")
        print(f"Rows where event_start_date year < reported year: {count_less_than}")
        print(f"Rows where event_start_date year == reported year: {count_equal_to}")
    else:
         print("\n--- Year Comparison Summary (Clean Rows Only) ---")
         print("No clean rows found to perform year comparison.")

    # Convert dates back to string format for CSV output AFTER calculations
    clean_rows['event_date_start'] = clean_rows['event_date_start'].dt.strftime('%Y-%m-%d')
    clean_rows['event_date_end'] = clean_rows['event_date_end'].dt.strftime('%Y-%m-%d')
    
    # Select and reorder columns for the final clean output
    final_columns = [
        'year', 'event_date_start', 'event_date_end', 'province', 'municipality', 'commodity',
        'disaster_type_raw', 'disaster_category', 'disaster_name',
        'area_partially_damaged_ha', 'area_totally_damaged_ha', 'area_total_affected_ha',
        'farmers_affected', 'volume_loss_mt',
        'losses_php_production_cost', 'losses_php_farm_gate', 'losses_php_grand_total',
        'sanitation_remarks'
    ]
    existing_final_cols = [col for col in final_columns if col in clean_rows.columns]
    clean_rows = clean_rows[existing_final_cols]
    
    print(f"\nFound {len(clean_rows)} clean rows.")
    print(f"Found {len(erroneous_rows)} erroneous rows.")

    # Save clean data to CSV
    clean_rows.to_csv(clean_path, index=False, encoding='utf-8-sig') # Changed to CSV
    print(f"-> Clean data saved to '{clean_path}'")
    
    # Prepare and save erroneous data to CSV
    if not erroneous_rows.empty:
        # Limit to original columns up to "GRAND TOTAL" (Column S, index 18 based on 0-index)
        original_cols_limit = df_original.columns[:19].tolist() # Ensure this grabs the correct columns
        
        # Make sure helper columns exist before trying to use them
        if 'source_row_number' not in erroneous_rows.columns:
            # Recalculate if it got lost somehow
            erroneous_rows['source_row_number'] = erroneous_rows.index + 3
        if 'error_reason' not in erroneous_rows.columns:
            # This shouldn't happen based on logic, but as a safeguard
             erroneous_rows['error_reason'] = "Error reason missing"

        # Corrected column order: Original columns first, then helpers AT THE END
        error_report_cols = original_cols_limit + ['source_row_number', 'error_reason']

        # Filter the DataFrame to only these columns, ensuring they exist
        erroneous_rows_final = erroneous_rows[[col for col in error_report_cols if col in erroneous_rows.columns]]

        erroneous_rows_final.to_csv(error_path, index=False, encoding='utf-8-sig') # Changed to CSV
        print(f"-> Erroneous rows report saved to '{error_path}'")
    else:
        # Create an empty file if there are no errors
        pd.DataFrame().to_csv(error_path, index=False, encoding='utf-8-sig') # Changed to CSV
        print(f"-> No erroneous rows found. Empty report saved to '{error_path}'")
    
    print("\n--- Sanitation Complete ---")

if __name__ == "__main__":
    sanitize_data(INPUT_FILENAME, TARGET_SHEET_NAME, CLEAN_OUTPUT_FILENAME, ERROR_OUTPUT_FILENAME)

