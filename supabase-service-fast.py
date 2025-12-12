import pandas as pd
import numpy as np
import sys
from datetime import datetime
from sqlalchemy import text
from migration_utils import get_db_engine, clean_boolean
from psycopg2.extras import execute_values

# =================CONFIGURATION=================
INPUT_FILE = './data/DataFinal.xlsx'
# ===============================================

# --- 1. CLEANING FUNCTIONS ---
def clean_val(val):
    if val is None: return None
    if isinstance(val, float) and np.isnan(val): return None
    s = str(val).strip()
    return None if s.lower() == 'nan' or s == '' else s

def clean_int_str(val):
    """Converts 12345.0 -> '12345'."""
    v = clean_val(val)
    if v is None: return None
    try:
        return str(int(float(v)))
    except:
        return v

def clean_date(val):
    if pd.isna(val) or val == '': return None
    if isinstance(val, datetime): return val
    try: return pd.to_datetime(val, dayfirst=True)
    except: return None

def clean_timestamp_special(val):
    v = clean_val(val)
    if v is None: return None
    if v.upper() in ['Y', 'YES', 'T', 'TRUE', 'COMP', 'COMPLETE']:
        return datetime(1999, 9, 19)
    return clean_date(val)

def clean_text_multiline(val):
    v = clean_val(val)
    return v.replace('\\n', '\n').replace('\t', ' ') if v else None

# --- 2. LOOKUP FETCHING ---
def fetch_job_map(conn):
    print("🔄 Fetching Job Map from Supabase...", flush=True)
    
    # Join Jobs -> Sales Orders to get the legacy number
    sql = text("""
        SELECT j.id as job_id, so.sales_order_number 
        FROM public.jobs j
        JOIN public.sales_orders so ON j.sales_order_id = so.id
    """)
    
    df = pd.read_sql(sql, conn)
    
    # Create map: {'10020': 55, '10021': 56}
    job_map = dict(zip(df['sales_order_number'].astype(str).str.strip(), df['job_id']))
    
    print(f"✅ Loaded {len(job_map)} active Jobs.", flush=True)
    return job_map

# --- 3. DATA PREPARATION ---
def prepare_service_data(df_service, df_parts, job_map):
    """
    Prepares two lists:
    1. headers_data: List of tuples for service_orders table
    2. parts_groups: List of DataFrames/Lists containing parts for each header (index-aligned)
    """
    headers_data = []
    parts_groups = []
    skipped = []

    print(f"🔄 Preparing {len(df_service)} service orders...", flush=True)

    # 1. Group parts by SO_NO for O(1) access
    parts_dict = {k: v for k, v in df_parts.groupby('SO_NO')}

    for idx, row in df_service.iterrows():
        so_no = clean_int_str(row.get('SO_NO'))
        
        # 1. Link to Job
        legacy_sales_id = clean_int_str(row.get('SALES_OR'))
        job_id = job_map.get(legacy_sales_id)

        # Skip if no parent Job found (Required by DB Schema)
        if not job_id:
            skipped.append(so_no)
            continue

        # 2. Prepare Header Tuple
        completed_at = clean_date(row.get('DATE_COMP'))
        if completed_at is None:
            completed_at = clean_timestamp_special(row.get('COMPLETE'))

        # Get parts for calculation and storage
        current_parts = parts_dict.get(so_no, pd.DataFrame())
        
        # Calculate Total Hours from Parts
        total_hours = 0.0
        if not current_parts.empty and 'HOURS' in current_parts.columns:
            total_hours = pd.to_numeric(current_parts['HOURS'], errors='coerce').fillna(0).sum()

        # Build Tuple (Matches SQL Columns Order)
        # Note: Logic preserved (date_entered defaults to now if None)
        header_tuple = (
            job_id,
            so_no,
            clean_date(row.get('DATE_ENTER')) or datetime(1999, 9, 19),
            clean_date(row.get('DATE_DUE')),
            completed_at,
            clean_val(row.get('SER_TYPE')),
            clean_val(row.get('SERVC_BY')),
            int(total_hours) if total_hours > 0 else None,
            clean_text_multiline(row.get('COMMENTS')),
            clean_val(row.get('BO_ITEM')),
            clean_boolean(row.get('CHARGEBLE')),
            clean_val(row.get('ENTER_BY')),
            False # is_warranty_so (Default False)
        )

        headers_data.append(header_tuple)
        parts_groups.append(current_parts) # Store matched parts for later index alignment

    print(f"✅ Prepared {len(headers_data)} records. Skipped {len(skipped)} (No Job Link).", flush=True)
    return headers_data, parts_groups

# --- 4. MAIN MIGRATION ---
def migrate_service_orders():
    engine = get_db_engine()
    if not engine: return

    print(f"📂 Reading Excel Data from {INPUT_FILE}...", flush=True)
    xls = pd.ExcelFile(INPUT_FILE)
    df_service = pd.read_excel(xls, 'Service')
    df_bo = pd.read_excel(xls, 'SalesBO') # Parts

    # --- PRE-PROCESSING ---
    print("🧹 Cleaning Keys...", flush=True)
    df_service['SO_NO'] = df_service['SO_NO'].apply(clean_int_str)
    df_bo['SO_NO'] = df_bo['SO_NO'].apply(clean_int_str)
    df_service['SALES_OR'] = df_service['SALES_OR'].apply(clean_int_str)

    # Filter Valid Rows
    df_service = df_service.dropna(subset=['SO_NO'])
    df_bo = df_bo.dropna(subset=['SO_NO'])

    # Deduplicate Service Headers
    df_service = df_service.drop_duplicates(subset=['SO_NO'])

    with engine.connect() as conn:
        # Load Job Map
        job_map = fetch_job_map(conn)
        
        # Prepare Data in Memory
        headers_data, parts_groups = prepare_service_data(df_service, df_bo, job_map)
        
        if not headers_data:
            print("❌ No valid records to insert.")
            return

        print("🚀 Starting ultra-fast bulk insert...", flush=True)
        
        try:
            raw_conn = conn.connection
            cursor = raw_conn.cursor()

            # --- STEP 1: Insert Headers ---
            print("  → Inserting Service Order Headers...", flush=True)
            
            header_sql = """
                INSERT INTO public.service_orders (
                    job_id, service_order_number, date_entered, due_date, completed_at,
                    service_type, service_by, hours_estimated, comments, 
                    service_type_detail, chargeable, created_by, is_warranty_so
                ) VALUES %s RETURNING service_order_id
            """
            
            # execute_values with fetch=True returns the IDs in the order of insertion
            # This is critical: headers_data[0] corresponds to new_so_ids[0]
            new_ids_result = execute_values(cursor, header_sql, headers_data, fetch=True)
            new_so_ids = [row[0] for row in new_ids_result]
            
            # --- STEP 2: Prepare Parts Data ---
            print("  → Linking Parts to new IDs...", flush=True)
            
            all_parts_data = []
            
            # Zip the new IDs with the preserved groups of parts
            for new_id, parts_df in zip(new_so_ids, parts_groups):
                if parts_df.empty:
                    continue
                    
                for _, part_row in parts_df.iterrows():
                    part_no = clean_val(part_row.get('PART_NO'))
                    desc = clean_text_multiline(part_row.get('COMMENT'))

                    # Skip empty part lines
                    if not part_no and not desc: continue

                    qty_raw = part_row.get('QTY')
                    try:
                        qty = int(float(qty_raw)) if pd.notna(qty_raw) else 1
                    except:
                        qty = 1
                    
                    # Create tuple for bulk insert
                    part_tuple = (
                        new_id,
                        qty,
                        part_no or "-",
                        desc
                    )
                    all_parts_data.append(part_tuple)

            # --- STEP 3: Insert Parts ---
            if all_parts_data:
                print(f"  → Inserting {len(all_parts_data)} Service Parts...", flush=True)
                parts_sql = """
                    INSERT INTO public.service_order_parts (
                        service_order_id, qty, part, description
                    ) VALUES %s
                """
                execute_values(cursor, parts_sql, all_parts_data)
            
            raw_conn.commit()
            
            print("\n" + "="*50)
            print("🏁 MIGRATION COMPLETE", flush=True)
            print(f"✅ Service Orders Created: {len(new_so_ids)}", flush=True)
            print(f"✅ Parts Created: {len(all_parts_data)}", flush=True)
            print("="*50, flush=True)

        except Exception as e:
            raw_conn.rollback()
            print(f"❌ Error during bulk insert: {e}", flush=True)
            raise

if __name__ == "__main__":
    migrate_service_orders()