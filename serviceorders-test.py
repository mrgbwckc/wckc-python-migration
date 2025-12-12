import pandas as pd
import numpy as np
import json
from datetime import datetime

# =================CONFIGURATION=================
INPUT_FILE = './data/DataFinal.xlsx'
OUTPUT_FILE = 'transformed_service_orders_preview.json'
SAMPLE_SIZE = 25000
# ===============================================

# --- HELPERS ---
class DateTimeEncoder(json.JSONEncoder):
    """Helper to handle dates in JSON output."""
    def default(self, obj):
        if isinstance(obj, (datetime, pd.Timestamp)):
            return obj.isoformat()
        return super(DateTimeEncoder, self).default(obj)

def clean_val(val):
    """Standardizes empty values to None."""
    if val is None: return None
    if isinstance(val, float) and np.isnan(val): return None
    s = str(val).strip()
    return None if s.lower() == 'nan' or s == '' else s

def clean_int_str(val):
    """
    Converts 12345.0 (float) or '12345.0' (str) -> '12345' (str).
    Returns None if empty.
    """
    v = clean_val(val)
    if v is None: return None
    try:
        # Convert to float first to handle '12345.0', then int, then str
        return str(int(float(v)))
    except:
        return v # Return original string if it's not a number (e.g. 'TBD')

def clean_boolean(val):
    """Parses legacy boolean flags."""
    v = clean_val(val)
    if v is None: return False
    return v.upper() in ['TRUE', 'T', 'YES', 'Y', '1']

def clean_date(val):
    """Parses dates safely."""
    if pd.isna(val) or val == '': return None
    if isinstance(val, datetime): return val
    try:
        return pd.to_datetime(val, dayfirst=True)
    except:
        return None

def clean_timestamp_special(val):
    """Handles 'Complete'/'Y' flags as a magic date."""
    v = clean_val(val)
    if v is None: return None
    if v.upper() in ['Y', 'YES', 'T', 'TRUE', 'COMP', 'COMPLETE']:
        return datetime(1999, 9, 19)
    return clean_date(val)

def clean_text_multiline(val):
    """Parses \n as newlines."""
    v = clean_val(val)
    return v.replace('\\n', '\n') if v else None

def prepare_service_data():
    print(f"📂 Reading Excel Data from {INPUT_FILE}...")
    try:
        xls = pd.ExcelFile(INPUT_FILE)
        df_service = pd.read_excel(xls, 'Service')
        df_bo = pd.read_excel(xls, 'SalesBO') # This sheet contains the parts
    except Exception as e:
        print(f"❌ Error reading Excel file: {e}")
        return

    # 1. Clean Keys for Merging
    df_service['SO_NO'] = df_service['SO_NO'].apply(clean_val)
    df_bo['SO_NO'] = df_bo['SO_NO'].apply(clean_val)
    
    # 2. Filter valid rows
    df_service = df_service.dropna(subset=['SO_NO'])
    
    # 3. Select Sample
    # Intersection of headers and parts preferred for better preview
    service_ids = set(df_service['SO_NO'].unique())
    parts_ids = set(df_bo['SO_NO'].unique())
    common_ids = list(service_ids.intersection(parts_ids))
    
    if len(common_ids) >= SAMPLE_SIZE:
        sample_ids = common_ids[:SAMPLE_SIZE]
    else:
        remaining = SAMPLE_SIZE - len(common_ids)
        other_ids = list(service_ids - parts_ids)[:remaining]
        sample_ids = common_ids + other_ids
        
    print(f"🔎 Selected {len(sample_ids)} Service Orders for preview.")

    final_payloads = []

    for so_no in sample_ids:
        # A. HEADER
        header_rows = df_service[df_service['SO_NO'] == so_no]
        if header_rows.empty: continue
        row = header_rows.iloc[0]

        # B. ITEMS
        parts_rows = df_bo[df_bo['SO_NO'] == so_no]

        # Logic: Combined completion date
        completed_at = clean_date(row.get('DATE_COMP'))
        if completed_at is None:
            completed_at = clean_timestamp_special(row.get('COMPLETE'))

        # Logic: Sum Hours
        total_hours = 0.0
        if not parts_rows.empty and 'HOURS' in parts_rows.columns:
            total_hours = pd.to_numeric(parts_rows['HOURS'], errors='coerce').fillna(0).sum()

        # Build Header
        service_order_data = {
            "service_order_number": so_no,
            "legacy_sales_id": clean_int_str(row.get('SALES_OR')), # FIX: Clean Int String
            "date_entered": clean_date(row.get('DATE_ENTER')),
            "due_date": clean_date(row.get('DATE_DUE')),
            "completed_at": completed_at,
            "service_type": clean_val(row.get('SER_TYPE')),
            "service_by": clean_val(row.get('SERVC_BY')),
            "hours_estimated": int(total_hours) if total_hours > 0 else None,
            "comments": clean_text_multiline(row.get('COMMENTS')), # FIX: Multiline
            "service_type_detail": clean_val(row.get('BO_ITEM')),
            "chargeable": clean_boolean(row.get('CHARGEBLE')),
            "created_by": clean_val(row.get('ENTER_BY'))
        }

        # Build Parts List
        parts_data = []
        for _, part_row in parts_rows.iterrows():
            part_no = clean_val(part_row.get('PART_NO'))
            desc = clean_text_multiline(part_row.get('COMMENT'))

            # FIX: Skip empty parts
            if not part_no and not desc:
                continue

            qty_raw = part_row.get('QTY')
            try:
                qty = int(float(qty_raw)) if pd.notna(qty_raw) else 1
            except:
                qty = 1
            
            parts_data.append({
                "qty": qty,
                "part": part_no or "Unknown Part",
                "description": desc
            })

        final_payloads.append({
            "service_order": service_order_data,
            "parts": parts_data
        })

    # 4. Save
    print("\n" + "="*60)
    print(f"💾 Saving {len(final_payloads)} preview records to '{OUTPUT_FILE}'...")
    
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(final_payloads, f, cls=DateTimeEncoder, indent=4)
    print(f"✅ Success! Open '{OUTPUT_FILE}' to verify.")

if __name__ == "__main__":
    prepare_service_data()