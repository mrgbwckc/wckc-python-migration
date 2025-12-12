import pandas as pd
import numpy as np
import sys
from datetime import datetime
from sqlalchemy import text
from migration_utils import get_db_engine, clean_boolean

# =================CONFIGURATION=================
INPUT_FILE = './data/DataFinal.xlsx'
# ===============================================

# --- 1. ROBUST CLEANING FUNCTIONS ---

def clean_val(val):
    if val is None: return None
    if isinstance(val, float) and np.isnan(val): return None
    s = str(val).strip()
    return None if s.lower() == 'nan' or s == '' else s

def clean_money(val):
    v = clean_val(val)
    if v is None: return 0.0
    try: return float(v.replace('$', '').replace(',', ''))
    except ValueError: return 0.0

def clean_date_strict(val):
    if pd.isna(val) or val == '': return None
    if isinstance(val, datetime): return val
    try: return pd.to_datetime(val, dayfirst=True)
    except: return None

def clean_timestamp_special(val):
    v = clean_val(val)
    if v is None: return None
    if v.upper() in ['Y', 'YES', 'T', 'TRUE', 'COMP', 'COMPLETE']:
        return datetime(1999, 9, 19)
    return clean_date_strict(val)

def clean_text_multiline(val):
    v = clean_val(val)
    return v.replace('\\n', '\n') if v else None

def parse_legacy_job_number(job_num_raw):
    v = clean_val(job_num_raw)
    if v is None: return None, None
    if '-' in v:
        parts = v.split('-', 1)
        try: return int(parts[0]), parts[1]
        except ValueError: return None, v
    else:
        try: return int(v), None
        except ValueError: return None, v

# --- 2. LOOKUP FETCHING ---
def fetch_lookups(conn):
    print("🔄 Fetching lookup maps...", flush=True)
    
    clients = pd.read_sql("SELECT id, legacy_id FROM public.client", conn)
    client_map = dict(zip(clients['legacy_id'].astype(str).str.strip(), clients['id']))
    
    species = pd.read_sql('SELECT "Id", "Species" FROM public.species', conn)
    species_map = dict(zip(species['Species'].str.strip(), species['Id']))
    
    colors = pd.read_sql('SELECT "Id", "Name" FROM public.colors', conn)
    color_map = dict(zip(colors['Name'].str.strip(), colors['Id']))
    
    doors = pd.read_sql("SELECT id, name FROM public.door_styles", conn)
    door_map = dict(zip(doors['name'].str.strip(), doors['id']))

    try:
        installers = pd.read_sql("SELECT installer_id, legacy_installer_id FROM public.installers", conn)
        installers = installers.dropna(subset=['legacy_installer_id'])
        installer_map = dict(zip(installers['legacy_installer_id'].astype(str).str.strip(), installers['installer_id']))
    except:
        installer_map = {}

    return client_map, species_map, color_map, door_map, installer_map

# --- 3. DATABASE INSERT FUNCTIONS ---

def insert_cabinet(conn, data):
    sql = text("""
        INSERT INTO public.cabinets (
            species_id, color_id, door_style_id, finish, glaze, 
            top_drawer_front, interior, drawer_box, drawer_hardware, box, 
            hinge_soft_close, doors_parts_only, handles_supplied, handles_selected, 
            glass, piece_count, glass_type
        ) VALUES (
            :species_id, :color_id, :door_style_id, :finish, :glaze, 
            :top_drawer_front, :interior, :drawer_box, :drawer_hardware, :box, 
            :hinge_soft_close, :doors_parts_only, :handles_supplied, :handles_selected, 
            :glass, :piece_count, :glass_type
        ) RETURNING id
    """)
    return conn.execute(sql, data).fetchone()[0]

def insert_sales_order(conn, data, cabinet_id):
    params = {**data, **data['shipping'], **data['checklist'], 'cabinet_id': cabinet_id}
    
    if params.get('created_at') is None:
        sql = text("""
            INSERT INTO public.sales_orders (
                client_id, cabinet_id, stage, total, deposit, designer, comments,
                install, order_type, delivery_type, sales_order_number, 
                shipping_client_name, shipping_street, shipping_city, shipping_province, 
                shipping_zip, shipping_phone_1, shipping_phone_2, shipping_email_1, shipping_email_2,
                layout_date, client_meeting_date, follow_up_date, appliance_specs_date, 
                selections_date, markout_date, review_date, second_markout_date, 
                flooring_type, flooring_clearance
            ) VALUES (
                :client_id, :cabinet_id, :stage, :total, :deposit, :designer, :comments,
                :install, :order_type, :delivery_type, :sales_order_number, 
                :shipping_client_name, :shipping_street, :shipping_city, :shipping_province, 
                :shipping_zip, :shipping_phone_1, :shipping_phone_2, :shipping_email_1, :shipping_email_2,
                :layout_date, :client_meeting_date, :follow_up_date, :appliance_specs_date, 
                :selections_date, :markout_date, :review_date, :second_markout_date, 
                :flooring_type, :flooring_clearance
            ) RETURNING id
        """)
    else:
        sql = text("""
            INSERT INTO public.sales_orders (
                client_id, cabinet_id, stage, total, deposit, designer, comments,
                install, order_type, delivery_type, sales_order_number, created_at,
                shipping_client_name, shipping_street, shipping_city, shipping_province, 
                shipping_zip, shipping_phone_1, shipping_phone_2, shipping_email_1, shipping_email_2,
                layout_date, client_meeting_date, follow_up_date, appliance_specs_date, 
                selections_date, markout_date, review_date, second_markout_date, 
                flooring_type, flooring_clearance
            ) VALUES (
                :client_id, :cabinet_id, :stage, :total, :deposit, :designer, :comments,
                :install, :order_type, :delivery_type, :sales_order_number, :created_at,
                :shipping_client_name, :shipping_street, :shipping_city, :shipping_province, 
                :shipping_zip, :shipping_phone_1, :shipping_phone_2, :shipping_email_1, :shipping_email_2,
                :layout_date, :client_meeting_date, :follow_up_date, :appliance_specs_date, 
                :selections_date, :markout_date, :review_date, :second_markout_date, 
                :flooring_type, :flooring_clearance
            ) RETURNING id
        """)
    return conn.execute(sql, params).fetchone()[0]

def insert_production(conn, data):
    # Added in_plant_actual to the query
    sql = text("""
        INSERT INTO public.production_schedule (
            rush, placement_date, doors_in_schedule, doors_out_schedule,
            cut_finish_schedule, cut_melamine_schedule, paint_in_schedule,
            paint_out_schedule, assembly_schedule, ship_schedule, production_comments,
            in_plant_actual,
            doors_completed_actual, cut_finish_completed_actual, cut_melamine_completed_actual,
            paint_completed_actual, assembly_completed_actual, custom_finish_completed_actual,
            ship_status
        ) VALUES (
            :rush, :placement_date, :doors_in_schedule, :doors_out_schedule,
            :cut_finish_schedule, :cut_melamine_schedule, :paint_in_schedule,
            :paint_out_schedule, :assembly_schedule, :ship_schedule, :production_comments,
            :in_plant_actual,
            :doors_completed_actual, :cut_finish_completed_actual, :cut_melamine_completed_actual,
            :paint_completed_actual, :assembly_completed_actual, :custom_finish_completed_actual,
            :ship_status
        ) RETURNING prod_id
    """)
    return conn.execute(sql, data).fetchone()[0]

def insert_installation(conn, data):
    sql = text("""
        INSERT INTO public.installation (
            installer_id, has_shipped, installation_date, installation_completed,
            inspection_date, wrap_date, wrap_completed, installation_notes
        ) VALUES (
            :installer_id, :has_shipped, :installation_date, :installation_completed,
            :inspection_date, :wrap_date, :wrap_completed, :installation_notes
        ) RETURNING installation_id
    """)
    return conn.execute(sql, data).fetchone()[0]

def insert_job(conn, data, so_id, prod_id, install_id):
    params = {**data, 'sales_order_id': so_id, 'prod_id': prod_id, 'installation_id': install_id}
    sql = text("""
        INSERT INTO public.jobs (
            job_base_number, job_suffix, sales_order_id, prod_id, installation_id, is_active
        ) VALUES (
            :job_base_number, :job_suffix, :sales_order_id, :prod_id, :installation_id, :is_active
        ) RETURNING id
    """)
    return conn.execute(sql, params).fetchone()[0]

def insert_purchasing(conn, data, job_id):
    params = {**data, 'job_id': job_id}
    sql = text("""
        INSERT INTO public.purchase_tracking (
            job_id, doors_ordered_at, glass_ordered_at, handles_ordered_at, 
            acc_ordered_at, purchasing_comments
        ) VALUES (
            :job_id, :doors_ordered_at, :glass_ordered_at, :handles_ordered_at, 
            :acc_ordered_at, :purchasing_comments
        )
    """)
    conn.execute(sql, params)

# --- 4. MAIN MIGRATION LOGIC ---

def migrate_jobs():
    engine = get_db_engine()
    if not engine: return

    print(f"📂 Reading Excel Data from {INPUT_FILE}...", flush=True)
    xls = pd.ExcelFile(INPUT_FILE)
    df_so = pd.read_excel(xls, 'SalesOrders')
    df_dc = pd.read_excel(xls, 'DesignChecks')
    df_oc = pd.read_excel(xls, 'OrderChecks')

    # Aggressive ID Cleaning
    df_so['SALES_OR'] = df_so['SALES_OR'].apply(clean_val)
    df_dc['SALES_OR'] = df_dc['SALES_OR'].apply(clean_val)
    df_oc['SALES_OR'] = df_oc['SALES_OR'].apply(clean_val)
    
    df_so = df_so.dropna(subset=['SALES_OR'])
    all_ids = df_so['SALES_OR'].unique()
    total_count = len(all_ids)
    print(f"🔎 Found {total_count} Unique Sales Orders.", flush=True)

    with engine.connect() as conn:
        client_map, species_map, color_map, door_map, installer_map = fetch_lookups(conn)
        conn.commit() 
        print("✅ Lookups loaded. Starting Inserts...", flush=True)

        success_quotes = 0
        success_jobs = 0
        fail_count = 0
        skip_count = 0

        for index, so_id in enumerate(all_ids):
            if index % 10 == 0: 
                print(f"⏳ Processing {index + 1}/{total_count}...", flush=True)
            
            try:
                # --- EXTRACT ---
                row_so = df_so[df_so['SALES_OR'] == so_id].iloc[0]
                row_dc = df_dc[df_dc['SALES_OR'] == so_id].iloc[0] if not df_dc[df_dc['SALES_OR'] == so_id].empty else pd.Series()
                row_oc = df_oc[df_oc['SALES_OR'] == so_id].iloc[0] if not df_oc[df_oc['SALES_OR'] == so_id].empty else pd.Series()

                # --- TRANSFORM ---
                # A. Cabinet
                cabinet_payload = {
                    "species_id": species_map.get(clean_val(row_so.get('SPECIES'))),
                    "color_id": color_map.get(clean_val(row_so.get('COLOR'))),
                    "door_style_id": door_map.get(clean_val(row_so.get('LOWER_DOOR'))),
                    "finish": clean_val(row_so.get('FINISH')),
                    "glaze": clean_val(row_so.get('GLAZE')),
                    "top_drawer_front": clean_val(row_so.get('DWR_FRONT')),
                    "interior": clean_val(row_so.get('INTERIOR')),
                    "drawer_box": clean_val(row_so.get('DWR')),
                    "drawer_hardware": clean_val(row_so.get('DWR_HRW')),
                    "box": clean_val(row_so.get('BOX')),
                    "piece_count": clean_val(row_so.get('PIECE_COUNT')),
                    "glass_type": clean_val(row_so.get('GLASS_TYPE')),
                    "hinge_soft_close": clean_boolean(row_so.get('HINGE_SC')),
                    "doors_parts_only": clean_boolean(row_so.get('DOORS_PARTS_ONLY')),
                    "handles_supplied": clean_boolean(row_so.get('HANDLES')),
                    "handles_selected": clean_boolean(row_so.get('HANDLES_SEL')),
                    "glass": clean_boolean(row_so.get('GLASS')),
                }

                # B. Sales Order
                install_bool = clean_boolean(row_so.get('INSTALL'))
                so_payload = {
                    "client_id": client_map.get(clean_val(row_so.get('CLIENT_NO'))),
                    "stage": clean_val(row_so.get('STAGE')).upper() if clean_val(row_so.get('STAGE')) else 'QUOTE',
                    "total": clean_money(row_so.get('TOTAL')),
                    "deposit": clean_money(row_so.get('DEPOSIT')),
                    "designer": clean_val(row_so.get('DESIGNER')),
                    "comments": clean_text_multiline(row_so.get('COMMENTS')),
                    "install": install_bool if install_bool is not None else False,
                    # FIX: Default to 'Unknown'
                    "order_type": clean_val(row_so.get('ORDER_TYPE')) or "Unknown",
                    "delivery_type": clean_val(row_so.get('DEL_TYPE')) or "Unknown",
                    "sales_order_number": so_id,
                    "created_at": clean_date_strict(row_so.get('DATE_SOLD')), 
                    "shipping": {
                        "shipping_client_name": clean_val(row_so.get('SHIP_LAST_NAME')),
                        "shipping_street": clean_val(row_so.get('SHIP_ADDRS')),
                        "shipping_city": clean_val(row_so.get('SHIP_CITY')),
                        "shipping_province": clean_val(row_so.get('SHIP_PROV')),
                        "shipping_zip": clean_val(row_so.get('SHIP_ZIP')),
                        "shipping_phone_1": clean_val(row_so.get('SHIP_PHONE1')),
                        "shipping_phone_2": clean_val(row_so.get('SHIP_PHONE2')),
                        "shipping_email_1": clean_val(row_so.get('SHIP_EMAIL1')),
                        "shipping_email_2": clean_val(row_so.get('SHIP_EMAIL2')),
                    },
                    "checklist": {
                        "layout_date": clean_date_strict(row_dc.get('LAYOUT')),
                        "client_meeting_date": clean_date_strict(row_dc.get('CLIENT_MEETING_DATE')),
                        "follow_up_date": clean_date_strict(row_so.get('FOLLOW_UPDATE')),
                        "appliance_specs_date": clean_date_strict(row_dc.get('APPLIANCE_SPECS')),
                        "selections_date": clean_date_strict(row_dc.get('SELECTIONS')),
                        "markout_date": clean_date_strict(row_so.get('SITE_MEASURE_DATE')),
                        "review_date": clean_date_strict(row_dc.get('REVIEW_DATE')),
                        "second_markout_date": clean_date_strict(row_so.get('SECOND_MEASURE_DATE')),
                        "flooring_type": clean_val(row_so.get('FLOORING_TYPE')),
                        "flooring_clearance": clean_val(row_so.get('FLOORING_CLEARENCE')),
                    }
                }

                # C. Production
                rush_val = clean_boolean(row_so.get('RUSH'))
                ship_date_val = clean_date_strict(row_so.get('DATE_SHIP'))
                legacy_conf = clean_boolean(row_so.get('SHIP_DATE_CONFIRM'))
                
                if ship_date_val is None:
                    final_ship_status = 'unprocessed'
                else:
                    final_ship_status = 'confirmed' if legacy_conf else 'unprocessed'

                # FIX: In Plant Logic
                doors_comp_val = clean_timestamp_special(row_so.get('DOORS_COMP'))
                in_plant_val = doors_comp_val if doors_comp_val else None

                prod_payload = {
                    "rush": rush_val if rush_val is not None else False,
                    "placement_date": clean_date_strict(row_so.get('PROD_IN_DATE')),
                    "doors_in_schedule": clean_date_strict(row_so.get('DATE_DOR_START')),
                    "doors_out_schedule": clean_date_strict(row_so.get('DATE_DOR_FIN')),
                    "cut_finish_schedule": clean_date_strict(row_so.get('ISSUE_DATE')),
                    "cut_melamine_schedule": clean_date_strict(row_so.get('MEL_DATE')),
                    "paint_in_schedule": clean_date_strict(row_so.get('PAINT_IN')),
                    "paint_out_schedule": clean_date_strict(row_so.get('PAINT_DATE')),
                    "assembly_schedule": clean_date_strict(row_so.get('ASS_DATE')),
                    "ship_schedule": ship_date_val,
                    "production_comments": clean_text_multiline(row_so.get('PROD_MEMO')),
                    # New Logic
                    "in_plant_actual": in_plant_val,
                    "doors_completed_actual": doors_comp_val,
                    "cut_finish_completed_actual": clean_timestamp_special(row_so.get('ISSUED')),
                    "cut_melamine_completed_actual": clean_timestamp_special(row_so.get('MEL__ISSUED')),
                    "paint_completed_actual": clean_timestamp_special(row_so.get('PAINT_COMP')),
                    "assembly_completed_actual": clean_timestamp_special(row_so.get('ASSEMBLED')),
                    "custom_finish_completed_actual": clean_date_strict(row_so.get('F_C_DATE')),
                    "ship_status": final_ship_status, 
                }

                # D. Installation
                inst_payload = {
                    "installer_id": installer_map.get(clean_val(row_so.get('INSTALL_ID'))),
                    "has_shipped": clean_boolean(row_so.get('HAS_SHIP')),
                    "installation_date": clean_date_strict(row_so.get('INSTALL_DATE')),
                    "installation_completed": clean_timestamp_special(row_so.get('STATUS')),
                    "inspection_date": clean_date_strict(row_so.get('INSPECTION_DATE')),
                    "wrap_date": clean_date_strict(row_so.get('WRAP_DATE')),
                    "wrap_completed": clean_timestamp_special(row_so.get('WRAP_COMP')),
                    "installation_notes": clean_text_multiline(row_so.get('INSTALL_MEMO'))
                }

                # E. Job & Purchasing
                base, suffix = parse_legacy_job_number(row_so.get('JOB_NUM'))
                job_payload = {
                    "job_base_number": base,
                    "job_suffix": suffix,
                    "is_active": True
                }
                
                purch_payload = {
                    "doors_ordered_at": clean_timestamp_special(row_so.get('DOORS_ORDERED')),
                    "glass_ordered_at": clean_timestamp_special(row_so.get('GLASS_ORD')),
                    "handles_ordered_at": clean_timestamp_special(row_oc.get('HANDLES')),
                    "acc_ordered_at": clean_timestamp_special(row_oc.get('ACC')),
                    "purchasing_comments": clean_text_multiline(row_oc.get('COMMENTS'))
                }

                # --- LOAD (INSERT) ---
                with conn.begin():
                    # 1. Check Dependencies
                    if not so_payload['client_id']:
                         print(f"❌ SKIP (SO {so_id}): Client Lookup Failed.", flush=True)
                         skip_count += 1
                         continue

                    # 2. Always Insert Cabinet & Sales Order
                    cab_id = insert_cabinet(conn, cabinet_payload)
                    so_id_new = insert_sales_order(conn, so_payload, cab_id)

                    # 3. Conditional Job Insertion
                    if job_payload['job_base_number'] is not None:
                        prod_id = insert_production(conn, prod_payload)
                        inst_id = insert_installation(conn, inst_payload)
                        job_id = insert_job(conn, job_payload, so_id_new, prod_id, inst_id)
                        insert_purchasing(conn, purch_payload, job_id)
                        success_jobs += 1
                    else:
                        success_quotes += 1

            except Exception as e:
                print(f"❌ FAILED on SO {so_id}: {e}", flush=True)
                fail_count += 1

        print("\n" + "="*50)
        print("🏁 MIGRATION COMPLETE", flush=True)
        print(f"✅ Full Jobs Created: {success_jobs}", flush=True)
        print(f"✅ Quotes Created:    {success_quotes}", flush=True)
        print(f"⚠️  Skipped (No Client): {skip_count}", flush=True)
        print(f"❌ Failed (Errors):    {fail_count}", flush=True)
        print("="*50, flush=True)

if __name__ == "__main__":
    migrate_jobs()