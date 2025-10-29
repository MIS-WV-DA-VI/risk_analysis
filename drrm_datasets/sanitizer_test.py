import pandas as pd
import os
import re
from datetime import datetime
from calendar import monthrange

# --- CONFIGURATION ---
INPUT_FILENAME = 'source_data.xlsx'
TARGET_SHEET_NAME = 'Consolidated 2010 - Present'
OUTPUT_DIR = 'exported'
CLEAN_OUTPUT_FILENAME = 'clean_data.csv'
ERROR_OUTPUT_FILENAME = 'erroneous_rows.csv'
PSGC_LOOKUP_FILENAME = 'psgc_lookup.csv' # PSGC Lookup file

# This mapping is updated to handle the column names in your consolidated file.
COLUMN_MAPPING = {
    'YEAR (DATE OF OCCURENCE)': 'year_original', # Rename original year column
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
    series_str = series.astype(str)
    series_cleaned = series_str.str.replace(',', '', regex=False).str.strip().replace('-', '0', regex=False)
    series_numeric = pd.to_numeric(series_cleaned, errors='coerce')
    return series_numeric.fillna(0)


def parse_date_range_smart(date_str, year_original_val):
    """
    Smarter date parser that handles various inconsistent formats and returns a remark.
    Uses year_original_val primarily as a fallback if year is missing in the string.
    Returns: (start_date, end_date, remark) or (None, None, None) on failure.
    """
    if pd.isna(date_str):
        return None, None, None

    if isinstance(date_str, datetime):
        date = date_str.date()
        remark = "Parsed from a native Excel date format."
        year_to_use = date.year if pd.isna(year_original_val) else int(year_original_val)
        if pd.notna(year_original_val) and abs(date.year - year_to_use) > 1:
             return None, None, f"Year in Excel date ({date.year}) differs significantly from Year column ({year_to_use})."
        return date, date, remark

    date_str = str(date_str).strip()
    fallback_year = None
    if pd.notna(year_original_val):
        try:
            fallback_year = int(year_original_val)
        except (ValueError, TypeError):
             return None, None, "Invalid Year column value"

    try:
        # Pattern 1 & 4 Combined: "Month Day, Year" OR "Month Day-Day, Year"
        match = re.match(r'^([a-zA-Z]+)\s+(\d{1,2})(?:\s*-\s*(\d{1,2}))?,\s*(\d{4})$', date_str, re.IGNORECASE)
        if match:
            month_str, start_day, end_day, year_from_str = match.groups()
            year_val = int(year_from_str)
            start_date = datetime.strptime(f"{month_str} {start_day} {year_val}", "%B %d %Y").date()
            if end_day:
                end_date = start_date.replace(day=int(end_day))
                remark = "Parsed from 'Month Day-Day, Year' format."
            else:
                end_date = start_date
                remark = "Parsed as a single day event."
            if fallback_year is not None and abs(year_val - fallback_year) > 1:
                 return None, None, f"Year in date string ({year_val}) differs significantly from Year column ({fallback_year})."
            return start_date, end_date, remark

        # Pattern 2: "Month-Month Year" e.g., "July-August 2021"
        match = re.match(r'(\w+)-(\w+)\s+(\d{4})', date_str, re.IGNORECASE)
        if match:
            start_month_str, end_month_str, year_from_str = match.groups()
            year_val = int(year_from_str)
            if fallback_year is not None and abs(year_val - fallback_year) > 1:
                 return None, None, f"Year in date string ({year_val}) differs significantly from Year column ({fallback_year})."
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
            if fallback_year is not None and abs(year_val - fallback_year) > 1:
                 return None, None, f"Year in date string ({year_val}) differs significantly from Year column ({fallback_year})."
            start_date = datetime.strptime(f"{month_str} 1 {year_val}", "%B %d %Y").date()
            last_day = monthrange(start_date.year, start_date.month)[1]
            end_date = start_date.replace(day=last_day)
            remark = "Parsed from month-only value; assumed full month."
            return start_date, end_date, remark

        # --- Fallback to pd.to_datetime ---
        if re.match(r'^\d{4}-\d{2}-\d{2}(?:\s+\d{2}:\d{2}:\d{2})?$', date_str):
            dt_obj = pd.to_datetime(date_str, errors='coerce')
            if not pd.isna(dt_obj):
                date = dt_obj.date()
                if fallback_year is not None and abs(date.year - fallback_year) > 1:
                     return None, None, f"Year in date string ({date.year}) differs significantly from Year column ({fallback_year})."
                remark = "Parsed from a standard timestamp format."
                return date, date, remark

        # --- Final fallback ---
        if fallback_year is None:
             return None, None, "Missing Year column value needed for ambiguous date string."
        parts = re.split(r'\s*-\s*|\s+to\s+', date_str, flags=re.IGNORECASE)
        start_str, end_str = parts[0], parts[-1]
        remark_parts = []

        # Process Start String
        match_start_year = re.search(r'(\d{4})', start_str)
        start_year = int(match_start_year.group(1)) if match_start_year else fallback_year
        start_str_clean = re.sub(r'\s*,?\s*\d{4}', '', start_str).strip()
        try:
            start_date = datetime.strptime(start_str_clean, "%B %d").replace(year=start_year).date()
        except ValueError:
            try:
                start_date = datetime.strptime(start_str_clean, "%B").replace(year=start_year, day=1).date()
                remark_parts.append("Start date assumed as 1st of month.")
            except ValueError:
                return None, None, "Invalid start month name"

        # Process End String
        match_end_year = re.search(r'(\d{4})', end_str)
        end_year = int(match_end_year.group(1)) if match_end_year else start_year
        end_str_clean = re.sub(r'\s*,?\s*\d{4}', '', end_str).strip()
        if len(parts) == 1:
            end_date = start_date
            if not ("Start date assumed" in " ".join(remark_parts) and start_str_clean == end_str_clean):
                 remark_parts.append("Parsed as a single day event.")
        elif len(end_str_clean.split()) == 1 and end_str_clean.isdigit():
             try:
                 datetime.strptime(start_str_clean, "%B %d")
                 end_date = start_date.replace(day=int(end_str_clean))
                 remark_parts.append("Parsed as a date range within the same month.")
             except ValueError:
                  return None, None, "Ambiguous range (Month to Day)"
        else:
            try:
                end_date = datetime.strptime(end_str_clean, "%B %d").replace(year=end_year).date()
            except ValueError:
                try:
                    end_month_dt = datetime.strptime(end_str_clean, "%B").replace(year=end_year)
                    last_day = monthrange(end_month_dt.year, end_month_dt.month)[1]
                    end_date = end_month_dt.replace(day=last_day).date()
                    remark_parts.append("End date assumed as last day of month.")
                except ValueError:
                    return None, None, "Invalid end month name"

        # Final validation checks using fallback_year
        if abs(start_date.year - fallback_year) > 1 or abs(end_date.year - fallback_year) > 1:
             return None, None, f"Year in date string ({start_date.year}-{end_date.year}) differs significantly from Year column ({fallback_year})."
        if not remark_parts:
             remark_parts.append("Parsed as a standard date range.")
        return start_date, end_date, " ".join(remark_parts)

    except Exception as e:
        # print(f"DEBUG: Failed parsing '{date_str}' with year {fallback_year}. Error: {e}")
        return None, None, None


def sanitize_data(input_filename, sheet_name, output_dir, clean_filename, error_filename, psgc_lookup_filename):
    """ Main function to orchestrate the data sanitation process, including PSGC lookup. """
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
    except NameError:
        script_dir = os.getcwd()
        print("Warning: Could not determine script directory, using current working directory.")
    input_path = os.path.join(script_dir, input_filename)
    psgc_lookup_path = os.path.join(script_dir, psgc_lookup_filename) # Path for PSGC lookup
    output_path = os.path.join(script_dir, output_dir)
    os.makedirs(output_path, exist_ok=True) # Create output dir if needed
    clean_path = os.path.join(output_path, clean_filename)
    error_path = os.path.join(output_path, error_filename)

    print(f"--- Starting Data Sanitation for '{input_path}' ---")
    if not os.path.exists(input_path):
        print(f"Error: Input file not found at '{input_path}'.")
        return

    # --- Load PSGC Lookup Data ---
    psgc_lookup_df = None
    if not os.path.exists(psgc_lookup_path):
         print(f"Warning: PSGC Lookup file not found at '{psgc_lookup_path}'. PSGC codes will not be added.")
    else:
        try:
            psgc_lookup_df = pd.read_csv(psgc_lookup_path)
            required_psgc_cols = ['province_name', 'municipality_name', 'psgc_code']
            if not all(col in psgc_lookup_df.columns for col in required_psgc_cols):
                print(f"Error: PSGC Lookup file '{psgc_lookup_filename}' is missing required columns: {required_psgc_cols}. Skipping PSGC lookup.")
                psgc_lookup_df = None
            else:
                 psgc_lookup_df['province_name'] = psgc_lookup_df['province_name'].astype(str).str.upper().str.strip()
                 psgc_lookup_df['municipality_name'] = psgc_lookup_df['municipality_name'].astype(str).str.upper().str.strip()
                 psgc_lookup_df['psgc_code'] = psgc_lookup_df['psgc_code'].astype(str).str.strip()
                 print(f"Loaded {len(psgc_lookup_df)} PSGC lookup entries.")
                 psgc_lookup_df = psgc_lookup_df[required_psgc_cols].drop_duplicates(subset=['province_name', 'municipality_name'])
        except Exception as e:
            print(f"Error loading PSGC Lookup file '{psgc_lookup_filename}': {e}. Skipping PSGC lookup.")
            psgc_lookup_df = None

    try:
        df = pd.read_excel(input_path, sheet_name=sheet_name, header=1, engine='openpyxl')
        df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
        df = df.dropna(how='all')
        original_row_count = len(df)
        print(f"Loaded {original_row_count} rows from sheet '{sheet_name}'.")
    except ValueError as e:
        print(f"Error reading Excel sheet: {e}. Make sure a sheet named '{sheet_name}' exists.")
        return

    df_original_structure = df.copy()
    df_original_structure['source_row_number'] = df.index + 3

    df_processed = df.rename(columns=lambda c: c.strip())
    df_processed = df_processed.rename(columns=COLUMN_MAPPING)
    df_processed['year_original'] = pd.to_numeric(df_processed.get('year_original'), errors='coerce')

    # --- Uppercase Conversion (BEFORE PSGC Lookup) ---
    string_cols_to_upper = [
        'province', 'municipality', 'commodity',
        'disaster_type_raw', 'disaster_category', 'disaster_name'
    ]
    for col in string_cols_to_upper:
        if col in df_processed.columns:
            # Keep NaN as NaN for now, convert case only on actual strings
            df_processed[col] = df_processed[col].astype(str).str.upper().str.strip()
            # Replace empty strings potentially created from NaN conversion back to None/NaN
            df_processed[col] = df_processed[col].replace({'^$': None, 'NAN': None}, regex=True)


    # --- PSGC Lookup/Merge ---
    if psgc_lookup_df is not None:
         print("Performing PSGC lookup...")
         if 'province' in df_processed.columns and 'municipality' in df_processed.columns:
              df_processed = pd.merge(
                   df_processed,
                   psgc_lookup_df,
                   left_on=['province', 'municipality'],
                   right_on=['province_name', 'municipality_name'],
                   how='left'
              )
              df_processed = df_processed.drop(columns=['province_name', 'municipality_name'], errors='ignore')
              print("PSGC lookup complete.")
         else:
              print("Warning: 'province' or 'municipality' column not found after renaming. Skipping PSGC lookup.")
              df_processed['psgc_code'] = None # Add empty column if lookup fails
    else:
         df_processed['psgc_code'] = None # Add empty column if lookup file wasn't loaded

    numeric_cols = list(set(COLUMN_MAPPING.values()) - {'year_original', 'date_range_str', 'province', 'municipality', 'commodity', 'disaster_type_raw', 'disaster_category', 'disaster_name'}) \
                 + ['year']
    for col in numeric_cols:
        if col in df_processed.columns and col != 'year':
            df_processed[col] = clean_numeric_column(df_processed[col])

    # Clean commodity codes AFTER converting to upper
    if 'commodity' in df_processed.columns:
        df_processed['commodity'] = df_processed['commodity'].astype(str).str.replace(r'^\d+\s*-\s*', '', regex=True).str.strip()
        # Handle potential empty strings after stripping code
        df_processed['commodity'] = df_processed['commodity'].replace({'^$': None}, regex=True)


    parsed_dates = df_processed.apply(lambda row: parse_date_range_smart(row.get('date_range_str'), row.get('year_original')), axis=1)
    df_processed[['temp_start', 'temp_end', 'sanitation_remarks']] = pd.DataFrame(parsed_dates.tolist(), index=df_processed.index)
    df_processed['event_date_start'] = pd.to_datetime(df_processed['temp_start'], errors='coerce')
    df_processed['event_date_end'] = pd.to_datetime(df_processed['temp_end'], errors='coerce')
    df_processed = df_processed.drop(columns=['temp_start', 'temp_end'])
    df_processed['year'] = df_processed['event_date_start'].dt.year
    df_processed['year'] = df_processed['year'].astype('Int64')

    # --- Validation ---
    error_reasons = []
    processed_indices = df_processed.index
    for index, row in df_processed.iterrows():
        reasons = []
        year_original_val, start_date, end_date = row.get('year_original'), row.get('event_date_start'), row.get('event_date_end')
        psgc_val = row.get('psgc_code')
        province_val = row.get('province') # Get value after potential uppercase
        municipality_val = row.get('municipality') # Get value after potential uppercase

        # Basic Validation
        # Check for NaN or empty string after stripping
        if pd.isna(province_val) or str(province_val).strip() == '':
             reasons.append("Missing essential field (province).")
        if pd.isna(municipality_val) or str(municipality_val).strip() == '':
             reasons.append("Missing essential field (municipality).")
        # Check original year validity
        if pd.isna(year_original_val):
            original_year_str = df_original_structure.loc[index, 'YEAR (DATE OF OCCURENCE)']
            reasons.append(f"Original Year column is not a valid number: '{original_year_str}'")
        # Check date parsing validity
        if pd.isna(start_date):
            original_date_str = df_original_structure.loc[index, 'ACTUAL DATE OF OCCURENCE']
            original_date_str = original_date_str if isinstance(original_date_str, (str, int, float)) else str(original_date_str)
            parse_remark = row.get('sanitation_remarks')
            error_msg = f"Unparseable date: '{original_date_str}'"
            if parse_remark and ("Invalid" in parse_remark or "Ambiguous" in parse_remark or "differs significantly" in parse_remark): error_msg += f" ({parse_remark})"
            reasons.append(error_msg)
        if row.get('losses_php_grand_total', 0) == 0: reasons.append("Missing or zero Grand Total for PHP loss.")

        # PSGC Validation
        if pd.isna(psgc_val) and psgc_lookup_df is not None:
             # Avoid double-flagging if province/municipality was already missing
             if not ("Missing essential field" in "; ".join(reasons)):
                   prov_disp = province_val if pd.notna(province_val) else "MISSING"
                   mun_disp = municipality_val if pd.notna(municipality_val) else "MISSING"
                   reasons.append(f"PSGC code not found for province/municipality: '{prov_disp}' / '{mun_disp}'.")

        # Date Logic Validation
        if pd.notna(start_date) and pd.notna(end_date):
            if start_date > end_date: reasons.append(f"Date range invalid: Start date ({start_date.date()}) is after end date ({end_date.date()}).")

        # Area Consistency Validation
        partial, totally, total = row.get('area_partially_damaged_ha', 0), row.get('area_totally_damaged_ha', 0), row.get('area_total_affected_ha', 0)
        if (partial > 0 or totally > 0) and total > 0 and not abs((partial + totally) - total) < 0.01:
            reasons.append(f"Area inconsistency: Partial({partial}) + Totally({totally}) != Total({total}).")

        error_reasons.append("; ".join(reasons))

    error_reasons_series = pd.Series(error_reasons, index=processed_indices)
    df_processed['error_reason'] = error_reasons_series.reindex(df_processed.index)

    # --- Separate Clean and Erroneous Rows ---
    is_erroneous = df_processed['error_reason'].fillna('').astype(str) != ''
    clean_rows = df_processed[~is_erroneous].copy()
    erroneous_rows = df_processed[is_erroneous].copy()
    erroneous_rows = erroneous_rows.merge(
        df_original_structure[['source_row_number']], left_index=True, right_index=True, how='left'
    )

    # --- Year Comparison Summary (Clean Data) ---
    if not clean_rows.empty:
        clean_rows['year'] = pd.to_numeric(clean_rows['year'], errors='coerce').fillna(0).astype(int)
        valid_date_rows = clean_rows[clean_rows['event_date_start'].notna() & clean_rows['event_date_end'].notna()]
        start_year_equal_end_year = valid_date_rows[valid_date_rows['event_date_start'].dt.year == valid_date_rows['event_date_end'].dt.year]
        start_year_less_than_end_year = valid_date_rows[valid_date_rows['event_date_start'].dt.year < valid_date_rows['event_date_end'].dt.year]
        count_equal_year, count_span_year = len(start_year_equal_end_year), len(start_year_less_than_end_year)
        print("\n--- Year Span Summary (Clean Rows Only) ---")
        print(f"Rows where event start/end years are the same: {count_equal_year}")
        print(f"Rows where event spans across calendar years: {count_span_year}")
    else:
        print("\n--- Year Span Summary (Clean Rows Only) ---")
        print("No clean rows found.")

    # Convert dates to string for output
    clean_rows['event_date_start'] = clean_rows['event_date_start'].apply(lambda x: x.strftime('%Y-%m-%d') if pd.notna(x) else '')
    clean_rows['event_date_end'] = clean_rows['event_date_end'].apply(lambda x: x.strftime('%Y-%m-%d') if pd.notna(x) else '')
    erroneous_rows['event_date_start'] = erroneous_rows['event_date_start'].apply(lambda x: x.strftime('%Y-%m-%d') if pd.notna(x) else '')
    erroneous_rows['event_date_end'] = erroneous_rows['event_date_end'].apply(lambda x: x.strftime('%Y-%m-%d') if pd.notna(x) else '')
    clean_rows['year'] = clean_rows['year'].astype(str).replace('<NA>', '')
    erroneous_rows['year'] = erroneous_rows['year'].astype(str).replace('<NA>', '')
    # Convert psgc_code to string, replacing NaN with empty string
    clean_rows['psgc_code'] = clean_rows['psgc_code'].fillna('').astype(str)
    erroneous_rows['psgc_code'] = erroneous_rows['psgc_code'].fillna('').astype(str)


    # --- Define Final Columns ---
    clean_final_columns = [
        'year', 'event_date_start', 'event_date_end', 'province', 'municipality', 'psgc_code', # Added psgc_code
        'commodity', 'disaster_type_raw', 'disaster_category', 'disaster_name',
        'area_partially_damaged_ha', 'area_totally_damaged_ha', 'area_total_affected_ha',
        'farmers_affected', 'volume_loss_mt',
        'losses_php_production_cost', 'losses_php_farm_gate', 'losses_php_grand_total',
        'sanitation_remarks'
    ]
    existing_clean_cols = [col for col in clean_final_columns if col in clean_rows.columns]
    clean_rows_final = clean_rows[existing_clean_cols]

    error_final_columns = existing_clean_cols + ['source_row_number', 'error_reason']
    existing_error_cols = [col for col in error_final_columns if col in erroneous_rows.columns]
    erroneous_rows_final = erroneous_rows[existing_error_cols]


    print(f"\nFound {len(clean_rows_final)} clean rows.")
    print(f"Found {len(erroneous_rows_final)} erroneous rows.")

    # --- Save Output Files ---
    clean_rows_final.to_csv(clean_path, index=False, encoding='utf-8-sig')
    print(f"-> Clean data saved to '{clean_path}'")

    if not erroneous_rows_final.empty:
        erroneous_rows_final.to_csv(error_path, index=False, encoding='utf-8-sig')
        print(f"-> Erroneous rows report saved to '{error_path}'")
    else:
        pd.DataFrame(columns=error_final_columns).to_csv(error_path, index=False, encoding='utf-8-sig')
        print(f"-> No erroneous rows found. Empty report saved to '{error_path}'")

    print("\n--- Sanitation Complete ---")

if __name__ == "__main__":
    sanitize_data(INPUT_FILENAME, TARGET_SHEET_NAME, OUTPUT_DIR, CLEAN_OUTPUT_FILENAME, ERROR_OUTPUT_FILENAME, PSGC_LOOKUP_FILENAME)

