import pandas as pd
import numpy as np
from datetime import datetime
from sqlalchemy import text
from migration_utils import get_db_engine, clean_boolean

# =================CONFIGURATION=================
INPUT_FILE = './data/DataFinal.xlsx'
SHEET_NAME = 'Installers'  # Ensure this matches your Excel sheet name
# ===============================================

def run_installers_migration():
    print(f"--- Starting Migration for Table: installers ---")
    print(f"📂 Reading '{SHEET_NAME}' from {INPUT_FILE}...")
    
    # 1. Load Data
    try:
        df = pd.read_excel(INPUT_FILE, sheet_name=SHEET_NAME)
    except FileNotFoundError:
        print(f"❌ Error: Could not find {INPUT_FILE}.")
        return

    # 2. Deduplicate
    # Clean ID to ensure strict matching
    if 'INSTALL_ID' in df.columns:
        df['INSTALL_ID'] = df['INSTALL_ID'].astype(str).str.strip()
    
    # Drop duplicates, keeping the first occurrence
    initial_count = len(df)
    df_clean = df.drop_duplicates(subset=['INSTALL_ID'], keep='first').copy()
    print(f"ℹ️  Deduplication: {initial_count} -> {len(df_clean)} rows")

    # 3. Rename Columns (Excel Header -> Supabase Column)
    column_mapping = {
        'INSTALL_ID': 'legacy_installer_id',  # Mapping to your new column
        'FIRST_NAME': 'first_name',
        'LAST_NAME': 'last_name',
        'ADDRESS': 'street_address',
        'CITY': 'city',
        'POSTAL': 'zip_code',
        'CELL': 'phone_number',
        'EMAIL': 'email',
        'ACTIVE': 'is_active',
        'NOTE': 'notes',
        'FIRSTAID': 'has_first_aid',
        'INSURANCE': 'has_insurance',
        'COMPANY': 'company_name',
        'GSTNUMBER': 'gst_number',
        'WCBNUMBER': 'wcb_number',
        'ACCOUNTNUMBER': 'acc_number'
    }
    df_clean.rename(columns=column_mapping, inplace=True)

    # 4. Data Transformations
    print("🛠  Transforming data types...")

    # Boolean cleanup
    bool_cols = ['is_active', 'has_first_aid', 'has_insurance']
    for col in bool_cols:
        if col in df_clean.columns:
            df_clean[col] = df_clean[col].apply(clean_boolean)

    df_clean['is_active'] = df_clean['is_active'].fillna(True).astype(bool)

    # String sanitization
    string_cols = ['first_name', 'last_name', 'street_address', 'city', 'zip_code', 
                   'phone_number', 'email', 'notes', 'company_name', 
                   'gst_number', 'wcb_number', 'acc_number', 'legacy_installer_id']
    
    for col in string_cols:
        if col in df_clean.columns:
            df_clean[col] = df_clean[col].astype(str).replace({'nan': None, 'NaN': None, '<NA>': None})
            df_clean[col] = df_clean[col].apply(lambda x: x.strip() if x else None)

    # 5. Validation
    df_clean = df_clean.dropna(subset=['legacy_installer_id'])

    # 6. Select Final Columns
    # We filter to ensure we only try to insert columns that exist in our DataFrame
    expected_cols = list(column_mapping.values())
    df_final = df_clean[[c for c in expected_cols if c in df_clean.columns]]
    
    # 7. Insert into Supabase
    engine = get_db_engine()
    if engine:
        try:
            print("🚀 Inserting data into Supabase...")
            df_final.to_sql('installers', engine, if_exists='append', index=False, chunksize=500)
            print(f"✅ Successfully inserted {len(df_final)} installer records.")
        except Exception as e:
            print(f"❌ Insertion failed: {e}")
            if 'legacy_installer_id' in str(e).lower() and 'column' in str(e).lower() and 'does not exist' in str(e).lower():
                 print("❗ CRITICAL ERROR: The column 'legacy_installer_id' does not exist in the DB yet. Please add it before running.")

if __name__ == "__main__":
    run_installers_migration()