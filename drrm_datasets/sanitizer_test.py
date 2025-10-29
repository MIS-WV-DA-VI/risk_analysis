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
    # Convert to string first to handle potential mixed types robustly
    series_str = series.astype(str)
    # Perform cleaning steps
    series_cleaned = series_str.str.replace(',', '', regex=False).str.strip().replace('-', '0', regex=False)
    # Convert to numeric, coercing errors
    series_numeric = pd.to_numeric(series_cleaned, errors='coerce')
    # Fill any resulting NaNs (from errors or original NaNs) with 0
    return series_numeric.fillna(0)


def parse_date_range_smart(date_str, year_original_val):
    """
    Smarter date parser that handles various inconsistent formats and returns a remark.
    Uses year_original_val primarily as a fallback if year is missing in the string.
    Returns: (start_date, end_date, remark) or (None, None, None) on failure.
    """
    if pd.isna(date_str): # Year is not strictly required here if date_str has it
        return None, None, None

    # Handle if date_str is already a datetime object from pandas
    if isinstance(date_str, datetime):
        date = date_str.date()
        remark = "Parsed from a native Excel date format."
        # Use date's year if year_original_val is missing
        year_to_use = date.year if pd.isna(year_original_val) else int(year_original_val)
        if abs(date.year - year_to_use) > 1:
             return None, None, f"Year in Excel date ({date.year}) differs significantly from Year column ({year_to_use})."
        return date, date, remark

    date_str = str(date_str).strip()

    # Determine fallback year only if year_original_val is valid
    fallback_year = None
    if pd.notna(year_original_val):
        try:
            fallback_year = int(year_original_val)
        except (ValueError, TypeError):
             return None, None, "Invalid Year column value" # Fail early if fallback is invalid

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
            # Use parsed year for validation if fallback_year exists
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

        # --- Fallback to pd.to_datetime for standard formats like YYYY-MM-DD ---
        if re.match(r'^\d{4}-\d{2}-\d{2}(?:\s+\d{2}:\d{2}:\d{2})?$', date_str):
            dt_obj = pd.to_datetime(date_str, errors='coerce')
            if not pd.isna(dt_obj):
                date = dt_obj.date()
                if fallback_year is not None and abs(date.year - fallback_year) > 1:
                     return None, None, f"Year in date string ({date.year}) differs significantly from Year column ({fallback_year})."
                remark = "Parsed from a standard timestamp format."
                return date, date, remark

        # --- Final fallback for other complex text ranges ---
        if fallback_year is None: # Cannot proceed without a year context
             return None, None, "Missing Year column value needed for ambiguous date string."

        parts = re.split(r'\s*-\s*|\s+to\s+', date_str, flags=re.IGNORECASE)
        start_str = parts[0]
        end_str = parts[-1]
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
        end_year = int(match_end_year.group(1)) if match_end_year else start_year # Default to start_year
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
        # print(f"DEBUG: Failed parsing '{date_str}' with year {fallback_year}. Error: {e}") # Uncomment for debugging
        return None, None, None


def sanitize_data(input_filename, sheet_name, output_dir, clean_filename, error_filename):
    """ Main function to orchestrate the data sanitation process. """
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
    except NameError:
        script_dir = os.getcwd()
        print("Warning: Could not determine script directory, using current working directory.")
    input_path = os.path.join(script_dir, input_filename)
    output_path = os.path.join(script_dir, output_dir)
    os.makedirs(output_path, exist_ok=True)
    clean_path = os.path.join(output_path, clean_filename)
    error_path = os.path.join(output_path, error_filename)

    print(f"--- Starting Data Sanitation for '{input_path}' ---")
    if not os.path.exists(input_path):
        print(f"Error: Input file not found at '{input_path}'.")
        print(f"       Current working directory: {os.getcwd()}")
        print(f"       Script directory: {script_dir}")
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

    df_original_structure = df.copy()
    df_original_structure['source_row_number'] = df.index + 3

    # Process data in a separate DataFrame
    df_processed = df.rename(columns=lambda c: c.strip())
    df_processed = df_processed.rename(columns=COLUMN_MAPPING)

    # Clean original year column first, store it but don't overwrite later
    df_processed['year_original'] = pd.to_numeric(df_processed.get('year_original'), errors='coerce')

    # Parse dates using the original year as context
    parsed_dates = df_processed.apply(lambda row: parse_date_range_smart(row.get('date_range_str'), row.get('year_original')), axis=1)
    df_processed[['temp_start', 'temp_end', 'sanitation_remarks']] = pd.DataFrame(parsed_dates.tolist(), index=df_processed.index)
    df_processed['event_date_start'] = pd.to_datetime(df_processed['temp_start'], errors='coerce')
    df_processed['event_date_end'] = pd.to_datetime(df_processed['temp_end'], errors='coerce')
    df_processed = df_processed.drop(columns=['temp_start', 'temp_end'])

    # --- NEW: Override the 'year' column with the year from event_date_start ---
    # Create the 'year' column *after* parsing dates
    df_processed['year'] = df_processed['event_date_start'].dt.year
    # Convert to nullable integer type Int64 to handle potential NaT from parsing errors
    df_processed['year'] = df_processed['year'].astype('Int64')
    # --- End Year Override ---


    # --- Uppercase Conversion ---
    string_cols_to_upper = [
        'province', 'municipality', 'commodity',
        'disaster_type_raw', 'disaster_category', 'disaster_name'
    ]
    for col in string_cols_to_upper:
        if col in df_processed.columns:
            df_processed[col] = df_processed[col].fillna('').astype(str).str.upper()

    # Clean numeric columns (excluding the now derived 'year')
    numeric_cols = list(set(COLUMN_MAPPING.values()) - {'year_original', 'date_range_str', 'province', 'municipality', 'commodity', 'disaster_type_raw', 'disaster_category', 'disaster_name'}) \
                 + ['year'] # Add the new year col here if we need numeric cleaning (unlikely now)
    for col in numeric_cols:
         # Exclude the newly created year column if it exists in numeric_cols list
        if col in df_processed.columns and col != 'year':
            df_processed[col] = clean_numeric_column(df_processed[col])


    # Clean commodity codes AFTER converting to upper
    if 'commodity' in df_processed.columns:
        df_processed['commodity'] = df_processed['commodity'].astype(str).str.replace(r'^\d+\s*-\s*', '', regex=True).str.strip()


    # --- Validation ---
    error_reasons = []
    processed_indices = df_processed.index
    for index, row in df_processed.iterrows():
        reasons = []
        # Use year_original for checking numeric validity
        year_original_val = row.get('year_original')
        start_date = row.get('event_date_start')
        end_date = row.get('event_date_end')

        # Basic Validation
        if pd.isna(row.get('province')) or str(row.get('province')).strip() == '': reasons.append("Missing essential field (province, municipality, or commodity).")
        # Check if original year value was valid number
        if pd.isna(year_original_val):
            original_year_str = df_original_structure.loc[index, 'YEAR (DATE OF OCCURENCE)']
            reasons.append(f"Original Year column is not a valid number: '{original_year_str}'")
        # Check if date parsing itself failed
        if pd.isna(start_date):
            original_date_str = df_original_structure.loc[index, 'ACTUAL DATE OF OCCURENCE']
            original_date_str = original_date_str if isinstance(original_date_str, (str, int, float)) else str(original_date_str)
            parse_remark = row.get('sanitation_remarks')
            error_msg = f"Unparseable date: '{original_date_str}'"
            if parse_remark and ("Invalid" in parse_remark or "Ambiguous" in parse_remark or "differs significantly" in parse_remark): error_msg += f" ({parse_remark})"
            reasons.append(error_msg)
        if row.get('losses_php_grand_total', 0) == 0: reasons.append("Missing or zero Grand Total for PHP loss.")

        # Date Logic Validation (only if dates were parsed successfully)
        if pd.notna(start_date) and pd.notna(end_date):
            # Check 1: Start date <= End date
            if start_date > end_date: reasons.append(f"Date range invalid: Start date ({start_date.date()}) is after end date ({end_date.date()}).")
            # Check 2: Year consistency - REMOVED as year is now derived

        # Area Consistency Validation
        partial, totally, total = row.get('area_partially_damaged_ha', 0), row.get('area_totally_damaged_ha', 0), row.get('area_total_affected_ha', 0)
        if (partial > 0 or totally > 0) and total > 0 and not abs((partial + totally) - total) < 0.01:
            reasons.append(f"Area inconsistency: Partial({partial}) + Totally({totally}) != Total({total}).")

        error_reasons.append("; ".join(reasons))

    # Add error reasons using the stored index, aligned with df_processed
    error_reasons_series = pd.Series(error_reasons, index=processed_indices)
    df_processed['error_reason'] = error_reasons_series.reindex(df_processed.index)

    # --- Separate Clean and Erroneous Rows ---
    is_erroneous = df_processed['error_reason'].fillna('').astype(str) != ''
    clean_rows = df_processed[~is_erroneous].copy()
    erroneous_rows = df_processed[is_erroneous].copy()
    erroneous_rows = erroneous_rows.merge(
        df_original_structure[['source_row_number']], left_index=True, right_index=True, how='left'
    )

    # --- ADJUSTED Year Comparison Summary (Clean Data) ---
    # Compare start year vs end year
    if not clean_rows.empty:
        # Filter for rows where both dates are valid before comparing years
        valid_date_rows = clean_rows[clean_rows['event_date_start'].notna() & clean_rows['event_date_end'].notna()]

        start_year_equal_end_year = valid_date_rows[
            valid_date_rows['event_date_start'].dt.year == valid_date_rows['event_date_end'].dt.year
        ]
        start_year_less_than_end_year = valid_date_rows[
             valid_date_rows['event_date_start'].dt.year < valid_date_rows['event_date_end'].dt.year
        ]

        count_equal_year = len(start_year_equal_end_year)
        count_span_year = len(start_year_less_than_end_year) # Events spanning across years

        print("\n--- Year Span Summary (Clean Rows Only) ---")
        print(f"Rows where event start/end years are the same: {count_equal_year}")
        print(f"Rows where event spans across calendar years: {count_span_year}")
    else:
         print("\n--- Year Span Summary (Clean Rows Only) ---")
         print("No clean rows found.")

    # Convert dates to string for output
    # Handle potential NaT before formatting
    clean_rows['event_date_start'] = clean_rows['event_date_start'].apply(lambda x: x.strftime('%Y-%m-%d') if pd.notna(x) else '')
    clean_rows['event_date_end'] = clean_rows['event_date_end'].apply(lambda x: x.strftime('%Y-%m-%d') if pd.notna(x) else '')
    erroneous_rows['event_date_start'] = erroneous_rows['event_date_start'].apply(lambda x: x.strftime('%Y-%m-%d') if pd.notna(x) else '')
    erroneous_rows['event_date_end'] = erroneous_rows['event_date_end'].apply(lambda x: x.strftime('%Y-%m-%d') if pd.notna(x) else '')
    # Convert derived year to string, handling <NA>
    clean_rows['year'] = clean_rows['year'].astype(str).replace('<NA>', '')
    erroneous_rows['year'] = erroneous_rows['year'].astype(str).replace('<NA>', '')


    # --- Define Final Columns ---
    # Include the derived 'year' column, exclude 'year_original' from final output
    clean_final_columns = [
        'year', 'event_date_start', 'event_date_end', 'province', 'municipality', 'commodity',
        'disaster_type_raw', 'disaster_category', 'disaster_name',
        'area_partially_damaged_ha', 'area_totally_damaged_ha', 'area_total_affected_ha',
        'farmers_affected', 'volume_loss_mt',
        'losses_php_production_cost', 'losses_php_farm_gate', 'losses_php_grand_total',
        'sanitation_remarks'
    ]
    existing_clean_cols = [col for col in clean_final_columns if col in clean_rows.columns]
    clean_rows_final = clean_rows[existing_clean_cols]

    # Error columns = clean columns + helpers
    error_final_columns = existing_clean_cols + ['source_row_number', 'error_reason']
    existing_error_cols = [col for col in error_final_columns if col in erroneous_rows.columns]
    # Reorder erroneous rows columns to match the desired output structure
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
    sanitize_data(INPUT_FILENAME, TARGET_SHEET_NAME, OUTPUT_DIR, CLEAN_OUTPUT_FILENAME, ERROR_OUTPUT_FILENAME)

