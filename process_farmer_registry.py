import pandas as pd
from deltalake.writer import write_deltalake
from deltalake import DeltaTable # <-- Import DeltaTable class
import os
import shutil
import glob
import re
import argparse # Keep argparse

# --- Configuration ---
# Input directory for the original XLSX files
FARMER_INPUT_DIR = './farmer_registry_input/'
# Directory to move processed XLSX files
PROCESSED_FARMER_DIR = os.path.join(FARMER_INPUT_DIR, 'processed')
# Output Delta table path (This will be partitioned by province)
FARMER_LAKEHOUSE_PATH = './lakehouse_data/farmer_registry'

# Expected columns in the raw XLSX file
EXPECTED_RAW_COLS = ['Municipality/Brgy', 'Count of Rice Farmers', 'Total Declared Rice Area']

# Regex to find the province name in the filename
# It looks for "RSBSA " followed by any characters (the province), and then " Rice Farmers"
FILENAME_PROVINCE_REGEX = re.compile(r"RSBSA (.*?) Rice Farmers", re.IGNORECASE)

def extract_province_from_filename(filename):
[Immersive content redacted for brevity.]
    # Check if the name is non-empty, fully uppercase, and contains only letters/spaces
    return bool(name_str) and name_str.isupper() and bool(re.match(r'^[A-Z\s]+$', name_str))

def process_farmer_xlsx_to_delta(input_file_path, write_mode='append'):
[Immersive content redacted for brevity.]
    if municipality_rows.empty:
        print(f"WARNING: No municipality rows identified in {file_basename}.")
        return True # Treat as success (nothing to process), move the file
[Immersive content redacted for brevity.]
    print(f"Cleaned data preview:\n{final_df.head().to_string()}")

    # --- Write Directly to Delta Table ---
    
[Immersive content redacted for brevity.]
    print(f"Writing municipality data to Delta table: {FARMER_LAKEHOUSE_PATH}...")
    try:
        if write_mode == 'dynamic_overwrite':
            # --- Workaround for broken dt.delete() on partitions with spaces ---
            # Strategy: Read all *other* partitions, add this new data,
            # and do a full overwrite with the combined data.
            
            current_province = final_df['province'].iloc[0]
            print(f"Preparing for DYNAMIC OVERWRITE for province = '{current_province}'")

            df_to_write = final_df # Start with the new data
            
            # Check if table exists before trying to read
            if os.path.exists(FARMER_LAKEHOUSE_PATH):
                print(f"Loading existing table to read other partitions...")
                try:
                    dt = DeltaTable(FARMER_LAKEHOUSE_PATH)
                    # Load all data WHERE province != current_province
                    df_others = dt.to_pandas(filters=[("province", "!=", current_province)])
                    
                    if not df_others.empty:
                        print(f"Loaded {len(df_others)} rows from other partitions.")
                        # Combine old data (others) + new data (current)
                        df_to_write = pd.concat([df_others, final_df])
                    else:
                        print(f"No data found for other partitions. Writing only new data.")
                
                except Exception as e:
                    print(f"Could not read existing table (may be empty or new): {e}")
                    print("Proceeding to write new data only.")
            
            print(f"Performing full table OVERWRITE with {len(df_to_write)} total rows to apply dynamic change...")
            
            # Perform a full 'overwrite' with the combined DataFrame
            write_deltalake(
                FARMER_LAKEHOUSE_PATH,
                df_to_write,
                mode='overwrite', # Use the overwrite mode that works
                overwrite_schema=True, # Allow schema evolution
                partition_by=['province']
            )
            print(f"Successfully performed dynamic overwrite for {current_province}.")

        else:
            # This handles normal 'append' and the first file of 'overwrite'
            final_write_mode = write_mode
            schema_settings = {}
            if write_mode == 'overwrite':
                schema_settings['overwrite_schema'] = True
            else:
                schema_settings['schema_mode'] = 'merge'

            write_deltalake(
                FARMER_LAKEHOUSE_PATH,
                final_df,
                mode=final_write_mode,
                partition_by=['province'],
                **schema_settings
            )
            print(f"Successfully wrote data using mode: {final_write_mode}")

        return True # Indicate success
    
    except Exception as e:
[Immersive content redacted for brevity.]

