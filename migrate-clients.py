import pandas as pd
import numpy as np
from datetime import datetime
from sqlalchemy import text # keep text just in case
from migration_utils import get_db_engine, clean_date

# =================CONFIGURATION=================
INPUT_FILE = './data/DataFinal.xlsx'
SHEET_NAME = 'Client'
# ===============================================

def run_client_migration():
    print(f"--- Starting Migration for Table: client ---")
    print(f"📂 Reading '{SHEET_NAME}' from {INPUT_FILE}...")
    
    # 1. Load Data
    try:
        df = pd.read_excel(INPUT_FILE, sheet_name=SHEET_NAME)
    except FileNotFoundError:
        print(f"❌ Error: Could not find {INPUT_FILE}.")
        return

    # 2. Deduplicate
    # Clean ID to ensure strict matching
    if 'CLIENT_ID' in df.columns:
        df['CLIENT_ID'] = df['CLIENT_ID'].astype(str).str.strip()
    
    # Drop duplicates, keeping the first occurrence
    initial_count = len(df)
    df_clean = df.drop_duplicates(subset=['CLIENT_ID'], keep='first').copy()
    print(f"ℹ️  Deduplication: {initial_count} -> {len(df_clean)} rows (Removed {initial_count - len(df_clean)} dupes)")

    # 3. Rename Columns (Excel Header -> Supabase Column)
    column_mapping = {
        'CLIENT_ID': 'legacy_id',
        'FIRST_NAME': 'firstName',
        'LAST_NAME': 'lastName',
        'ADDRESS': 'street',
        'CITY': 'city',
        'PROV': 'province',
        'ZIP': 'zip',
        'PHONE1': 'phone1',
        'PHONE2': 'phone2',
        'EMAIL1': 'email1',
        'EMAIL2': 'email2',
        'REP': 'designer',
        'DATEENTER': 'createdAt'
    }
    df_clean.rename(columns=column_mapping, inplace=True)

    # 4. Data Transformations
    print("🛠  Transforming data types...")

    # Dates
    df_clean['createdAt'] = df_clean['createdAt'].apply(clean_date)
    df_clean['updatedAt'] = df_clean['createdAt'] 

    # Required Fields (lastName cannot be null in new schema)
    df_clean['lastName'] = df_clean['lastName'].fillna('Unknown')

    # String sanitization (Convert NaN to None for SQL NULL)
    string_cols = ['firstName', 'lastName', 'street', 'city', 'province', 'zip', 
                   'phone1', 'phone2', 'email1', 'email2', 'designer', 'legacy_id']
    
    for col in string_cols:
        if col in df_clean.columns:
            df_clean[col] = df_clean[col].astype(str).replace({'nan': None, 'NaN': None, '<NA>': None})
            df_clean[col] = df_clean[col].apply(lambda x: x.strip() if x else None)

    # 5. Validation: Remove rows without a legacy_id
    df_clean = df_clean.dropna(subset=['legacy_id'])

    # 6. Select Final Columns matching DB Schema
    final_columns = [
        'legacy_id', 'firstName', 'lastName', 'street', 'city', 
        'province', 'zip', 'phone1', 'phone2', 'email1', 
        'email2', 'designer', 'createdAt', 'updatedAt'
    ]
    
    # Filter only columns that exist
    df_final = df_clean[[c for c in final_columns if c in df_clean.columns]]

    # 7. Insert into Supabase
    engine = get_db_engine()
    if engine:
        try:
            print("🚀 Inserting data into Supabase...")
            
            # chunksize=500 helps prevent packet size errors on large uploads
            df_final.to_sql('client', engine, if_exists='append', index=False, chunksize=500)
            
            print(f"✅ Successfully inserted {len(df_final)} client records.")
            
        except Exception as e:
            print(f"❌ Insertion failed: {e}")
            # Optional: Detailed error about unique constraints if re-running
            if 'unique constraint' in str(e).lower():
                print("   (Hint: You might have existing data. Try truncating the table first or checking for duplicates.)")

if __name__ == "__main__":
    run_client_migration()