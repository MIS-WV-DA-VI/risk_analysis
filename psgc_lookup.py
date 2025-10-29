import geopandas as gpd
import pandas as pd
import os

# --- CONFIGURATION ---
# --- <<< IMPORTANT: Adjust these paths and property names as needed >>> ---
GIS_DATA_DIR = 'gis_data'
GEOJSON_FILENAME = 'WV_Municipalities.geojson'
OUTPUT_LOOKUP_FILENAME = 'psgc_lookup.csv' # Output file for sanitizer.py

# --- Property names within your WV_Municipalities.geojson file ---
GEOJSON_MUN_PROP = 'adm3_en'        # Property name for Municipality name
GEOJSON_PROV_PROP = 'adm2_en'       # Property name for Province name
GEOJSON_PSGC_PROP = 'adm3_psgc'     # Property name for Municipality PSGC code
# --- End Configuration ---

def create_lookup_from_geojson(geojson_path, output_csv_path, mun_prop, prov_prop, psgc_prop):
    """
    Reads a GeoJSON file containing municipality boundaries and extracts
    province name, municipality name, and PSGC code to create a CSV lookup table.
    """
    print(f"--- Creating PSGC Lookup from '{os.path.basename(geojson_path)}' ---")

    if not os.path.exists(geojson_path):
        print(f"Error: GeoJSON file not found at '{geojson_path}'.")
        return

    try:
        # Read the GeoJSON file using geopandas
        gdf = gpd.read_file(geojson_path)
        print(f"Successfully loaded {len(gdf)} features from GeoJSON.")

        # Check if required properties exist in the GeoDataFrame columns
        required_props = [mun_prop, prov_prop, psgc_prop]
        if not all(prop in gdf.columns for prop in required_props):
            missing = [prop for prop in required_props if prop not in gdf.columns]
            print(f"Error: GeoJSON is missing required properties: {missing}.")
            print(f"       Available properties are: {gdf.columns.tolist()}")
            print(f"       Please update the GEOJSON_*_PROP variables in the script.")
            return

        # Select and rename the relevant columns
        lookup_df = gdf[[prov_prop, mun_prop, psgc_prop]].copy()
        lookup_df.rename(columns={
            prov_prop: 'province_name',
            mun_prop: 'municipality_name',
            psgc_prop: 'psgc_code'
        }, inplace=True)

        # Standardize: Convert names to uppercase and strip whitespace
        lookup_df['province_name'] = lookup_df['province_name'].astype(str).str.upper().str.strip()
        lookup_df['municipality_name'] = lookup_df['municipality_name'].astype(str).str.upper().str.strip()
        # Ensure PSGC is a clean string
        lookup_df['psgc_code'] = lookup_df['psgc_code'].astype(str).str.strip()

        # Remove potential duplicates based on province and municipality
        original_count = len(lookup_df)
        lookup_df.drop_duplicates(subset=['province_name', 'municipality_name'], keep='first', inplace=True)
        duplicates_removed = original_count - len(lookup_df)
        if duplicates_removed > 0:
            print(f"Removed {duplicates_removed} duplicate province/municipality entries.")

        # Sort for better readability
        lookup_df.sort_values(by=['province_name', 'municipality_name'], inplace=True)

        # Save to CSV
        lookup_df.to_csv(output_csv_path, index=False, encoding='utf-8-sig')
        print(f"Successfully created PSGC lookup file: '{output_csv_path}' with {len(lookup_df)} unique entries.")
        print("--- PSGC Lookup Creation Complete ---")

    except Exception as e:
        print(f"An error occurred: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    # --- Determine script directory and construct paths ---
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
    except NameError:
        script_dir = os.getcwd() # Fallback

    geojson_full_path = os.path.join(script_dir, GIS_DATA_DIR, GEOJSON_FILENAME)
    output_csv_full_path = os.path.join(script_dir, OUTPUT_LOOKUP_FILENAME) # Save in main dir

    create_lookup_from_geojson(geojson_full_path, output_csv_full_path,
                               GEOJSON_MUN_PROP, GEOJSON_PROV_PROP, GEOJSON_PSGC_PROP)
