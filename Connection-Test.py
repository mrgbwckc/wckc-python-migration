import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
import warnings
import numpy as np
import sys # Used for exiting the script on failure

# Suppress warnings that Pandas often throws during data type conversions
warnings.filterwarnings('ignore', category=UserWarning)


# --- A. DATABASE CONNECTION CONFIGURATION ---

DATABASE_URL = f"postgresql://postgres.ajxvalhasysijvybgmhq:wckcnewpass2509@aws-0-us-west-2.pooler.supabase.com:5432/postgres"

# Initialize Engine
engine = None
try:
    engine = create_engine(DATABASE_URL)
    print("✅ Database engine configured.")
except Exception as e:
    print(f"❌ ERROR: Could not create DB engine. Check credentials. Details: {e}")


# --- B. DATA INGESTION AND MERGING FUNCTION ---
def load_and_merge_legacy_data(file_path='./data/DataFinal.xlsx'):
    """
    Loads all relevant legacy sheets and attempts to merge them into a master DataFrame.
    This tests file access and the Pandas library setup.
    """
    
    # 1. Load Core Data Sheets
    xls = pd.ExcelFile(file_path)
    
    # Loading just the necessary sheets for a robust test
    df_sales_orders = pd.read_excel(xls, 'SalesOrders').rename(columns={'SALES_OR': 'Legacy_Sales_ID'})
    df_design_checks = pd.read_excel(xls, 'DesignChecks').rename(columns={'SALES_OR': 'Legacy_Sales_ID'})
    
    print("✅ Legacy sheets loaded.")

    # 2. Create Master DataFrame (minimal merge for testing)
    master_df = df_sales_orders.copy()
    
    master_df = master_df.merge(
        df_design_checks.drop(columns=['JOB_NUM'], errors='ignore'), 
        on='Legacy_Sales_ID',
        how='left',
        suffixes=('_SO', '_DC')
    )
    
    print(f"✅ Master DataFrame created with {len(master_df)} sales order records.")
    return master_df

# --- C. DATABASE CONNECTION TEST FUNCTION ---
def test_db_connection(engine):
    """
    Tests the database connection by attempting to read from a non-empty lookup table.
    """
    if engine is None:
        print("❌ DB Engine failed to initialize in setup.")
        return False

    # We will test by trying to read the 'species' lookup table
    try:
        df_species_db = pd.read_sql_table('species', engine, columns=['Id', 'Species'])
        print(f"✅ DB Read Success: Loaded {len(df_species_db)} rows from 'public.species'.")
        print(df_species_db)
        return True

    except Exception as e:
        print(f"❌ ERROR: Database connection failed during table read. Details: {e}")
        print("   -> Possible Issues:")
        print("      1. DB Credentials (User/Password/Host) are incorrect.")
        print("      2. Supabase Firewall/Policy is blocking the connection.")
        print("      3. The table 'species' does not exist or your user lacks SELECT permission.")
        
        return False


# --- D. MAIN EXECUTION BLOCK (TEST ONLY) ---
if __name__ == '__main__':
    print("\n--- Phase 1: Environment and Data Loading Test ---")
    
    # 1. Test Excel Data Loading
    try:
        # We don't need all returns, just confirming the function runs
        master_df = load_and_merge_legacy_data() 
    except Exception as e:
        print(f"❌ CRITICAL ERROR: Failed to load Excel data. Check path and file format.")
        sys.exit(1)

    print("\n--- Phase 2: Database Connection Test ---")

    # 2. Test DB Connection
    if test_db_connection(engine):
        print("\n=======================================================")
        print("   ✅ ALL SETUP TESTS PASSED. READY FOR MIGRATION.   ")
        print("=======================================================")
    else:
        print("\n=======================================================")
        print("   🛑 SETUP FAILED. RESOLVE ERRORS BEFORE PROCEEDING. ")
        print("=======================================================")
        sys.exit(1)