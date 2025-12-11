import pandas as pd
import numpy as np
import json
from datetime import datetime
from sqlalchemy import text
from migration_utils import get_db_engine, clean_boolean, clean_date

# =================CONFIGURATION=================
INPUT_FILE = './data/DataFinal.xlsx'
OUTPUT_FILE = 'transformed_jobs.json'
# ===============================================

class DateTimeEncoder(json.JSONEncoder):
    """Helper to print datetime objects in JSON format."""
    def default(self, obj):
        if isinstance(obj, (datetime, pd.Timestamp)):
            return obj.isoformat()
        return super(DateTimeEncoder, self).default(obj)

def fetch_lookups(engine):
    """
    Fetches the new IDs for Clients, Species, Colors, DoorStyles.
    """
    print("🔄 Fetching lookup maps from Supabase...")
    
    with engine.connect() as conn:
        # 1. Clients: Map legacy_id (string) -> id (int)
        clients = pd.read_sql("SELECT id, legacy_id FROM public.client", conn)
        client_map = dict(zip(clients['legacy_id'].astype(str).str.strip(), clients['id']))
        
        # 2. Species: Map Species Name -> Id
        species = pd.read_sql('SELECT "Id", "Species" FROM public.species', conn)
        species_map = dict(zip(species['Species'].str.strip(), species['Id']))
        
        # 3. Colors: Map Color Name -> Id
        colors = pd.read_sql('SELECT "Id", "Name" FROM public.colors', conn)
        color_map = dict(zip(colors['Name'].str.strip(), colors['Id']))
        
        # 4. Door Styles: Map Name -> Id
        doors = pd.read_sql("SELECT id, name FROM public.door_styles", conn)
        door_map = dict(zip(doors['name'].str.strip(), doors['id']))

        # 5. Installers: Map legacy_installer_id -> installer_id
        try:
            installers = pd.read_sql("SELECT installer_id, legacy_installer_id FROM public.installers", conn)
            installers = installers.dropna(subset=['legacy_installer_id'])
            installer_map = dict(zip(installers['legacy_installer_id'].astype(str).str.strip(), installers['installer_id']))
        except Exception:
            print("⚠️  Warning: Could not fetch installer map (legacy_installer_id might be missing).")
            installer_map = {}

    print(f"✅ Loaded Lookups: {len(client_map)} Clients, {len(species_map)} Species, {len(color_map)} Colors, {len(door_map)} Doors.")
    return client_map, species_map, color_map, door_map, installer_map

def parse_legacy_job_number(job_num_raw):
    s = str(job_num_raw).strip()
    if '-' in s:
        parts = s.split('-', 1)
        try:
            return int(parts[0]), parts[1]
        except ValueError:
            return None, s 
    else:
        try:
            return int(s), None
        except ValueError:
            return None, s

def clean_timestamp_special(val):
    if pd.isna(val) or val == '':
        return None
    
    s = str(val).strip().upper()
    if s in ['Y', 'YES', 'T', 'TRUE', 'COMP', 'COMPLETE']:
        return datetime(1999, 9, 19) 
    if s in ['N', 'NO', 'F', 'FALSE']:
        return None
        
    return clean_date(val)

def clean_money(val):
    """Safely converts money strings (e.g. '$1,000.00', ' ') to float."""
    if pd.isna(val):
        return 0.0
    s = str(val).strip()
    if s == '':
        return 0.0
    try:
        return float(s.replace('$', '').replace(',', ''))
    except ValueError:
        return 0.0

def prepare_job_data():
    engine = get_db_engine()
    if not engine:
        return

    # 1. Get Lookups
    client_map, species_map, color_map, door_map, installer_map = fetch_lookups(engine)

    # 2. Load Excel Data
    print(f"📂 Reading Excel Data from {INPUT_FILE}...")
    xls = pd.ExcelFile(INPUT_FILE)
    df_so = pd.read_excel(xls, 'SalesOrders')
    df_dc = pd.read_excel(xls, 'DesignChecks')
    df_oc = pd.read_excel(xls, 'OrderChecks')

    # Ensure joining keys are strings
    df_so['SALES_OR'] = df_so['SALES_OR'].astype(str).str.strip()
    df_dc['SALES_OR'] = df_dc['SALES_OR'].astype(str).str.strip()
    df_oc['SALES_OR'] = df_oc['SALES_OR'].astype(str).str.strip()

    # 3. Select 5 Sample Sales Orders
    sample_ids = df_so['SALES_OR'].unique()[:5]
    print(f"🔎 Selected Sample Sales Orders: {sample_ids}")

    final_payloads = []

    for so_id in sample_ids:
        print(f"   ... Processing Sales Order: {so_id}")
        
        row_so = df_so[df_so['SALES_OR'] == so_id].iloc[0]
        
        row_dc = df_dc[df_dc['SALES_OR'] == so_id]
        row_dc = row_dc.iloc[0] if not row_dc.empty else pd.Series()

        row_oc = df_oc[df_oc['SALES_OR'] == so_id]
        row_oc = row_oc.iloc[0] if not row_oc.empty else pd.Series()

        # A. Cabinet
        species_name = str(row_so.get('SPECIES', '')).strip()
        color_name = str(row_so.get('COLOR', '')).strip()
        door_name = str(row_so.get('LOWER_DOOR', '')).strip()

        cabinet_data = {
            "species_id": species_map.get(species_name),
            "color_id": color_map.get(color_name),
            "door_style_id": door_map.get(door_name),
            "finish": row_so.get('FINISH'),
            "glaze": row_so.get('GLAZE'),
            "top_drawer_front": row_so.get('DWR_FRONT'),
            "interior": row_so.get('INTERIOR'),
            "drawer_box": row_so.get('DWR'),
            "drawer_hardware": row_so.get('DWR_HRW'),
            "box": str(row_so.get('BOX', '')),
            "hinge_soft_close": clean_boolean(row_so.get('HINGE_SC')),
            "doors_parts_only": clean_boolean(row_so.get('DOORS_PARTS_ONLY')),
            "handles_supplied": clean_boolean(row_so.get('HANDLES')),
            "handles_selected": clean_boolean(row_so.get('HANDLES_SEL')),
            "glass": clean_boolean(row_so.get('GLASS')),
            "piece_count": str(row_so.get('PIECE_COUNT', '')),
            "glass_type": row_so.get('GLASS_TYPE'),
            "_debug_species_name": species_name
        }

        # B. Sales Order
        client_legacy_no = str(row_so.get('CLIENT_NO', '')).strip()
        
        sales_order_data = {
            "client_id": client_map.get(client_legacy_no),
            "stage": row_so.get('STAGE', 'QUOTE').upper() if row_so.get('STAGE') else 'QUOTE',
            "total": clean_money(row_so.get('TOTAL')),
            "deposit": clean_money(row_so.get('DEPOSIT')),
            "designer": row_so.get('DESIGNER'),
            "comments": row_so.get('COMMENTS'),
            "install": clean_boolean(row_so.get('INSTALL')),
            "order_type": row_so.get('ORDER_TYPE'),
            "delivery_type": row_so.get('DEL_TYPE'),
            "sales_order_number": so_id,
            "shipping": {
                "shipping_client_name": row_so.get('SHIP_LAST_NAME'),
                "shipping_street": row_so.get('SHIP_ADDRS'),
                "shipping_city": row_so.get('SHIP_CITY'),
                "shipping_province": row_so.get('SHIP_PROV'),
                "shipping_zip": row_so.get('SHIP_ZIP'),
                "shipping_phone_1": row_so.get('SHIP_PHONE1'),
                "shipping_phone_2": row_so.get('SHIP_PHONE2'),
                "shipping_email_1": row_so.get('SHIP_EMAIL1'),
                "shipping_email_2": row_so.get('SHIP_EMAIL2'),
            },
            "checklist": {
                "layout_date": clean_date(row_dc.get('LAYOUT')) if not row_dc.empty else None,
                "client_meeting_date": clean_date(row_dc.get('CLIENT_MEETING_DATE')) if not row_dc.empty else None,
                "follow_up_date": clean_date(row_so.get('FOLLOW_UPDATE')),
                "appliance_specs_date": clean_date(row_dc.get('APPLIANCE_SPECS')) if not row_dc.empty else None,
                "selections_date": clean_date(row_dc.get('SELECTIONS')) if not row_dc.empty else None,
                "markout_date": clean_date(row_so.get('SITE_MEASURE_DATE')),
                "review_date": clean_date(row_dc.get('REVIEW_DATE')) if not row_dc.empty else None,
                "second_markout_date": clean_date(row_so.get('SECOND_MEASURE_DATE')),
                "flooring_type": row_so.get('FLOORING_TYPE'),
                "flooring_clearance": row_so.get('FLOORING_CLEARENCE'),
            }
        }

        # C. Production
        prod_data = {
            "rush": clean_boolean(row_so.get('RUSH')),
            "placement_date": clean_date(row_so.get('PROD_IN_DATE')),
            "doors_in_schedule": clean_date(row_so.get('DATE_DOR_START')),
            "doors_out_schedule": clean_date(row_so.get('DATE_DOR_FIN')),
            "cut_finish_schedule": clean_date(row_so.get('ISSUE_DATE')),
            "cut_melamine_schedule": clean_date(row_so.get('MEL_DATE')),
            "paint_in_schedule": clean_date(row_so.get('PAINT_IN')),
            "paint_out_schedule": clean_date(row_so.get('PAINT_DATE')),
            "assembly_schedule": clean_date(row_so.get('ASS_DATE')),
            "ship_schedule": clean_date(row_so.get('DATE_SHIP')),
            "production_comments": row_so.get('PROD_MEMO'),
            "doors_completed_actual": clean_timestamp_special(row_so.get('DOORS_COMP')),
            "cut_finish_completed_actual": clean_timestamp_special(row_so.get('ISSUED')),
            "cut_melamine_completed_actual": clean_timestamp_special(row_so.get('MEL__ISSUED')),
            "paint_completed_actual": clean_timestamp_special(row_so.get('PAINT_COMP')),
            "assembly_completed_actual": clean_timestamp_special(row_so.get('ASSEMBLED')),
            "custom_finish_completed_actual": clean_date(row_so.get('F_C_DATE')),
            "ship_status": 'confirmed' if clean_boolean(row_so.get('SHIP_DATE_CONFIRM')) else 'unprocessed', 
        }

        # D. Installation
        legacy_inst_id = str(row_so.get('INSTALL_ID', '')).strip()
        install_data = {
            "installer_id": installer_map.get(legacy_inst_id),
            "has_shipped": clean_boolean(row_so.get('HAS_SHIP')),
            "installation_date": clean_date(row_so.get('INSTALL_DATE')),
            "installation_completed": clean_timestamp_special(row_so.get('STATUS')),
            "inspection_date": clean_date(row_so.get('INSPECTION_DATE')),
            "wrap_date": clean_date(row_so.get('WRAP_DATE')),
            "wrap_completed": clean_timestamp_special(row_so.get('WRAP_COMP')),
            "installation_notes": row_so.get('INSTALL_MEMO')
        }

        # E. Purchasing
        purchasing_data = {
            "doors_ordered_at": clean_timestamp_special(row_so.get('DOORS_ORDERED')),
            "glass_ordered_at": clean_timestamp_special(row_so.get('GLASS_ORD')),
            "handles_ordered_at": clean_timestamp_special(row_oc.get('HANDLES')) if not row_oc.empty else None,
            "acc_ordered_at": clean_timestamp_special(row_oc.get('ACC')) if not row_oc.empty else None,
            "purchasing_comments": row_oc.get('COMMENTS') if not row_oc.empty else None
        }

        # F. Job
        job_num_raw = row_so.get('JOB_NUM')
        base, suffix = parse_legacy_job_number(job_num_raw)
        
        job_data = {
            "job_base_number": base,
            "job_suffix": suffix,
            "is_active": True
        }

        full_record = {
            "legacy_sales_id": so_id,
            "cabinet": cabinet_data,
            "sales_order": sales_order_data,
            "production": prod_data,
            "installation": install_data,
            "purchasing": purchasing_data,
            "job": job_data
        }
        
        final_payloads.append(full_record)

    # 4. Save to JSON File
    print("\n" + "="*60)
    print(f"💾 Saving {len(final_payloads)} transformed jobs to '{OUTPUT_FILE}'...")
    
    try:
        with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
            json.dump(final_payloads, f, cls=DateTimeEncoder, indent=4)
        print("✅ Success! File created.")
    except Exception as e:
        print(f"❌ Error writing file: {e}")

if __name__ == "__main__":
    prepare_job_data()