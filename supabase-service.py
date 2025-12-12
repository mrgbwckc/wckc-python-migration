import pandas as pd
import numpy as np
import sys
from datetime import datetime
from sqlalchemy import text
from migration_utils import get_db_engine, clean_boolean

# =================CONFIGURATION=================
INPUT_FILE = './data/DataFinal.xlsx'
# ===============================================

# --- 1. CLEANING HELPERS ---
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
    return v.replace('\\n', '\n') if v else None

# --- 2. DATABASE LOOKUPS ---
def fetch_job_map(conn):
    """
    Returns a dict: {'Legacy_Sales_Order_Num': New_Job_ID}
    Used to link the Service Order (via SALES_OR) to the correct Job.
    """
    print("🔄 Fetching Job Map from Supabase...", flush=True)
    
    # Join Jobs -> Sales Orders to get the legacy number
    sql = text("""
        SELECT j.id as job_id, so.sales_order_number 
        FROM public.jobs j
        JOIN public.sales_orders so ON j.sales_order_id = so.id
    """)
    
    df = pd.read_sql(sql, conn)
    
    # Create map: {'10020': 55, '10021': 56}
    # Ensure keys are stripped strings for matching
    job_map = dict(zip(df['sales_order_number'].astype(str).str.strip(), df['job_id']))
    
    print(f"✅ Loaded {len(job_map)} active Jobs.", flush=True)
    return job_map

# --- 3. INSERT FUNCTIONS ---
def insert_service_header(conn, data):
    sql = text("""
        INSERT INTO public.service_orders (
            job_id, service_order_number, date_entered, due_date, completed_at,
            service_type, service_by, hours_estimated, comments, 
            service_type_detail, chargeable, created_by, is_warranty_so
        ) VALUES (
            :job_id, :service_order_number, :date_entered, :due_date, :completed_at,
            :service_type, :service_by, :hours_estimated, :comments, 
            :service_type_detail, :chargeable, :created_by, :is_warranty_so
        ) RETURNING service_order_id
    """)
    return conn.execute(sql, data).fetchone()[0]

def insert_service_parts(conn, parts_list, service_order_id):
    if not parts_list: return
    
    sql = text("""
        INSERT INTO public.service_order_parts (
            service_order_id, qty, part, description
        ) VALUES (
            :service_order_id, :qty, :part, :description
        )
    """)
    
    # Add the ID to every part dictionary
    for p in parts_list:
        p['service_order_id'] = service_order_id
        
    conn.execute(sql, parts_list)

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
    # 1. Clean Keys (Convert '123.0' -> '123')
    df_service['SO_NO'] = df_service['SO_NO'].apply(clean_int_str)
    df_bo['SO_NO'] = df_bo['SO_NO'].apply(clean_int_str)
    
    df_service['SALES_OR'] = df_service['SALES_OR'].apply(clean_int_str)

    # 2. Filter Valid Rows
    df_service = df_service.dropna(subset=['SO_NO'])
    df_bo = df_bo.dropna(subset=['SO_NO'])

    # 3. Get Unique Service Orders
    all_so_ids = df_service['SO_NO'].unique()
    total_count = len(all_so_ids)
    print(f"🔎 Found {total_count} Service Orders to process.", flush=True)

    with engine.connect() as conn:
        # Load Job Map
        job_map = fetch_job_map(conn)
        conn.commit() # Clear transaction state
        
        success_count = 0
        skip_count = 0
        fail_count = 0

        for index, so_no in enumerate(all_so_ids):
            if index % 50 == 0: 
                print(f"⏳ Processing {index + 1}/{total_count}...", flush=True)

            try:
                # --- EXTRACT ---
                row = df_service[df_service['SO_NO'] == so_no].iloc[0]
                parts_rows = df_bo[df_bo['SO_NO'] == so_no]

                # --- TRANSFORM ---
                
                # 1. Link to Job
                legacy_sales_id = row.get('SALES_OR')
                job_id = job_map.get(legacy_sales_id)

                # Skip if no parent Job found (Required by DB Schema)
                if not job_id:
                    # Optional: Print specific skips if debugging
                    # print(f"⚠️ Skip SO {so_no}: Parent Sales Order {legacy_sales_id} not found in Jobs.")
                    skip_count += 1
                    continue

                # 2. Header Logic
                completed_at = clean_date(row.get('DATE_COMP'))
                if completed_at is None:
                    completed_at = clean_timestamp_special(row.get('COMPLETE'))

                # Calculate Total Hours from Parts
                total_hours = 0.0
                if not parts_rows.empty and 'HOURS' in parts_rows.columns:
                    total_hours = pd.to_numeric(parts_rows['HOURS'], errors='coerce').fillna(0).sum()

                header_payload = {
                    "job_id": job_id,
                    "service_order_number": so_no,
                    "date_entered": clean_date(row.get('DATE_ENTER')) or datetime(1999, 9, 19),
                    "due_date": clean_date(row.get('DATE_DUE')),
                    "completed_at": completed_at,
                    "service_type": clean_val(row.get('SER_TYPE')),
                    "service_by": clean_val(row.get('SERVC_BY')),
                    "hours_estimated": int(total_hours) if total_hours > 0 else None,
                    "comments": clean_text_multiline(row.get('COMMENTS')),
                    "service_type_detail": clean_val(row.get('BO_ITEM')),
                    "chargeable": clean_boolean(row.get('CHARGEBLE')),
                    "created_by": clean_val(row.get('ENTER_BY')),
                    "is_warranty_so": False
                }

                # 3. Parts Logic
                parts_payload = []
                for _, part_row in parts_rows.iterrows():
                    part_no = clean_val(part_row.get('PART_NO'))
                    desc = clean_text_multiline(part_row.get('COMMENT'))

                    # Skip empty part lines
                    if not part_no and not desc: continue

                    qty_raw = part_row.get('QTY')
                    try:
                        qty = int(float(qty_raw)) if pd.notna(qty_raw) else 1
                    except:
                        qty = 1
                    
                    parts_payload.append({
                        "qty": qty,
                        "part": part_no or "Unknown Part",
                        "description": desc
                    })

                # --- LOAD (INSERT) ---
                with conn.begin():
                    new_so_id = insert_service_header(conn, header_payload)
                    insert_service_parts(conn, parts_payload, new_so_id)
                    success_count += 1

            except Exception as e:
                print(f"❌ FAILED on Service Order {so_no}: {e}", flush=True)
                fail_count += 1

        print("\n" + "="*50)
        print("🏁 SERVICE ORDER MIGRATION COMPLETE", flush=True)
        print(f"✅ Success: {success_count}", flush=True)
        print(f"⚠️  Skipped (No Job Linked): {skip_count}", flush=True)
        print(f"❌ Failed (Errors): {fail_count}", flush=True)
        print("="*50, flush=True)

if __name__ == "__main__":
    migrate_service_orders()