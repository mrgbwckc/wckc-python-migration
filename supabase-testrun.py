import pandas as pd
import numpy as np
from datetime import datetime
from sqlalchemy import text
from migration_utils import get_db_engine, clean_boolean, clean_date

# =================CONFIGURATION=================
INPUT_FILE = './data/DataFinal.xlsx'
# ===============================================

# --- HELPER FUNCTIONS ---
def clean_money(val):
    if pd.isna(val): return 0.0
    s = str(val).strip()
    if s == '': return 0.0
    try:
        return float(s.replace('$', '').replace(',', ''))
    except ValueError:
        return 0.0

def clean_timestamp_special(val):
    if pd.isna(val) or val == '': return None
    s = str(val).strip().upper()
    if s in ['Y', 'YES', 'T', 'TRUE', 'COMP', 'COMPLETE']:
        return datetime(1999, 9, 19)
    if s in ['N', 'NO', 'F', 'FALSE']:
        return None
    return clean_date(val)

def parse_legacy_job_number(job_num_raw):
    s = str(job_num_raw).strip()
    if '-' in s:
        parts = s.split('-', 1)
        try: return int(parts[0]), parts[1]
        except ValueError: return None, s
    else:
        try: return int(s), None
        except ValueError: return None, s

def fetch_lookups(conn):
    print("🔄 Fetching lookup maps...")
    # 1. Clients
    clients = pd.read_sql("SELECT id, legacy_id FROM public.client", conn)
    client_map = dict(zip(clients['legacy_id'].astype(str).str.strip(), clients['id']))
    
    # 2. Species
    species = pd.read_sql('SELECT "Id", "Species" FROM public.species', conn)
    species_map = dict(zip(species['Species'].str.strip(), species['Id']))
    
    # 3. Colors
    colors = pd.read_sql('SELECT "Id", "Name" FROM public.colors', conn)
    color_map = dict(zip(colors['Name'].str.strip(), colors['Id']))
    
    # 4. Doors
    doors = pd.read_sql("SELECT id, name FROM public.door_styles", conn)
    door_map = dict(zip(doors['name'].str.strip(), doors['id']))

    # 5. Installers
    try:
        installers = pd.read_sql("SELECT installer_id, legacy_installer_id FROM public.installers", conn)
        installers = installers.dropna(subset=['legacy_installer_id'])
        installer_map = dict(zip(installers['legacy_installer_id'].astype(str).str.strip(), installers['installer_id']))
    except:
        installer_map = {}

    return client_map, species_map, color_map, door_map, installer_map

# --- DATABASE INSERTION FUNCTION ---
def insert_single_job(conn, data):
    """
    Inserts a single job transactionally.
    Returns the new Job ID if successful.
    """
    
    # 1. INSERT CABINET
    sql_cab = text("""
        INSERT INTO public.cabinets (
            species_id, color_id, door_style_id, 
            finish, glaze, top_drawer_front, interior, drawer_box, 
            drawer_hardware, box, hinge_soft_close, doors_parts_only, 
            handles_supplied, handles_selected, glass, piece_count, glass_type
        ) VALUES (
            :species_id, :color_id, :door_style_id, 
            :finish, :glaze, :top_drawer_front, :interior, :drawer_box, 
            :drawer_hardware, :box, :hinge_soft_close, :doors_parts_only, 
            :handles_supplied, :handles_selected, :glass, :piece_count, :glass_type
        ) RETURNING id
    """)
    result = conn.execute(sql_cab, data['cabinet'])
    cabinet_id = result.fetchone()[0]

    # 2. INSERT PRODUCTION SCHEDULE
    sql_prod = text("""
        INSERT INTO public.production_schedule (
            rush, placement_date, doors_in_schedule, doors_out_schedule,
            cut_finish_schedule, cut_melamine_schedule, paint_in_schedule,
            paint_out_schedule, assembly_schedule, ship_schedule, production_comments,
            doors_completed_actual, cut_finish_completed_actual, cut_melamine_completed_actual,
            paint_completed_actual, assembly_completed_actual, custom_finish_completed_actual,
            ship_status
        ) VALUES (
            :rush, :placement_date, :doors_in_schedule, :doors_out_schedule,
            :cut_finish_schedule, :cut_melamine_schedule, :paint_in_schedule,
            :paint_out_schedule, :assembly_schedule, :ship_schedule, :production_comments,
            :doors_completed_actual, :cut_finish_completed_actual, :cut_melamine_completed_actual,
            :paint_completed_actual, :assembly_completed_actual, :custom_finish_completed_actual,
            :ship_status
        ) RETURNING prod_id
    """)
    result = conn.execute(sql_prod, data['production'])
    prod_id = result.fetchone()[0]

    # 3. INSERT INSTALLATION
    sql_inst = text("""
        INSERT INTO public.installation (
            installer_id, has_shipped, installation_date, installation_completed,
            inspection_date, wrap_date, wrap_completed, installation_notes
        ) VALUES (
            :installer_id, :has_shipped, :installation_date, :installation_completed,
            :inspection_date, :wrap_date, :wrap_completed, :installation_notes
        ) RETURNING installation_id
    """)
    result = conn.execute(sql_inst, data['installation'])
    installation_id = result.fetchone()[0]

    # 4. INSERT SALES ORDER
    so_data = data['sales_order'].copy()
    so_data['cabinet_id'] = cabinet_id
    
    so_params = {
        **so_data, 
        **so_data['shipping'], 
        **so_data['checklist']
    }

    sql_so = text("""
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
    result = conn.execute(sql_so, so_params)
    sales_order_id = result.fetchone()[0]

    # 5. INSERT JOB
    job_params = {
        **data['job'],
        'sales_order_id': sales_order_id,
        'prod_id': prod_id,
        'installation_id': installation_id
    }
    
    sql_job = text("""
        INSERT INTO public.jobs (
            job_base_number, job_suffix, sales_order_id, prod_id, installation_id, is_active
        ) VALUES (
            :job_base_number, :job_suffix, :sales_order_id, :prod_id, :installation_id, :is_active
        ) RETURNING id
    """)
    result = conn.execute(sql_job, job_params)
    job_id = result.fetchone()[0]

    # 6. INSERT PURCHASING TRACKING
    purch_params = {**data['purchasing'], 'job_id': job_id}
    sql_purch = text("""
        INSERT INTO public.purchase_tracking (
            job_id, doors_ordered_at, glass_ordered_at, handles_ordered_at, 
            acc_ordered_at, purchasing_comments
        ) VALUES (
            :job_id, :doors_ordered_at, :glass_ordered_at, :handles_ordered_at, 
            :acc_ordered_at, :purchasing_comments
        )
    """)
    conn.execute(sql_purch, purch_params)

    return job_id

# --- MAIN EXECUTION ---
def migrate_jobs():
    engine = get_db_engine()
    if not engine: return

    # 1. Load Excel
    print(f"📂 Reading Excel Data from {INPUT_FILE}...")
    xls = pd.ExcelFile(INPUT_FILE)
    df_so = pd.read_excel(xls, 'SalesOrders')
    df_dc = pd.read_excel(xls, 'DesignChecks')
    df_oc = pd.read_excel(xls, 'OrderChecks')

    # Clean keys
    df_so['SALES_OR'] = df_so['SALES_OR'].astype(str).str.strip()
    df_dc['SALES_OR'] = df_dc['SALES_OR'].astype(str).str.strip()
    df_oc['SALES_OR'] = df_oc['SALES_OR'].astype(str).str.strip()

    # 2. Select ALL Unique IDs
    # CHANGED: Removed [:5] slice to process everything
    all_ids = df_so['SALES_OR'].unique()
    total_count = len(all_ids)
    print(f"🔎 Found {total_count} Unique Sales Orders to Process.")

    # 3. Start DB Session
    with engine.connect() as conn:
        # Get Lookups inside the connection context
        client_map, species_map, color_map, door_map, installer_map = fetch_lookups(conn)
        
        # 🟢 CRITICAL: Commit the read transaction to clear the connection state
        conn.commit()
        print("✅ Lookups loaded. Read transaction closed. Starting Inserts...")

        success_count = 0
        skip_count = 0
        fail_count = 0

        for index, so_id in enumerate(all_ids):
            # Print progress every 10 rows
            if index % 10 == 0:
                print(f"⏳ Processing {index + 1}/{total_count} (SO: {so_id})...")
            
            # --- EXTRACT & TRANSFORM ---
            try:
                row_so = df_so[df_so['SALES_OR'] == so_id].iloc[0]
                row_dc = df_dc[df_dc['SALES_OR'] == so_id].iloc[0] if not df_dc[df_dc['SALES_OR'] == so_id].empty else pd.Series()
                row_oc = df_oc[df_oc['SALES_OR'] == so_id].iloc[0] if not df_oc[df_oc['SALES_OR'] == so_id].empty else pd.Series()

                # A. Cabinet
                cabinet_data = {
                    "species_id": species_map.get(str(row_so.get('SPECIES', '')).strip()),
                    "color_id": color_map.get(str(row_so.get('COLOR', '')).strip()),
                    "door_style_id": door_map.get(str(row_so.get('LOWER_DOOR', '')).strip()),
                    "finish": row_so.get('FINISH'), "glaze": row_so.get('GLAZE'),
                    "top_drawer_front": row_so.get('DWR_FRONT'), "interior": row_so.get('INTERIOR'),
                    "drawer_box": row_so.get('DWR'), "drawer_hardware": row_so.get('DWR_HRW'),
                    "box": str(row_so.get('BOX', '')), "piece_count": str(row_so.get('PIECE_COUNT', '')),
                    "glass_type": row_so.get('GLASS_TYPE'),
                    "hinge_soft_close": clean_boolean(row_so.get('HINGE_SC')),
                    "doors_parts_only": clean_boolean(row_so.get('DOORS_PARTS_ONLY')),
                    "handles_supplied": clean_boolean(row_so.get('HANDLES')),
                    "handles_selected": clean_boolean(row_so.get('HANDLES_SEL')),
                    "glass": clean_boolean(row_so.get('GLASS')),
                }

                # B. Sales Order
                sales_order_data = {
                    "client_id": client_map.get(str(row_so.get('CLIENT_NO', '')).strip()),
                    "stage": row_so.get('STAGE', 'QUOTE').upper() if row_so.get('STAGE') else 'QUOTE',
                    "total": clean_money(row_so.get('TOTAL')), "deposit": clean_money(row_so.get('DEPOSIT')),
                    "designer": row_so.get('DESIGNER'), "comments": row_so.get('COMMENTS'),
                    "install": clean_boolean(row_so.get('INSTALL')),
                    "order_type": row_so.get('ORDER_TYPE'), "delivery_type": row_so.get('DEL_TYPE'),
                    "sales_order_number": so_id,
                    "shipping": {
                        "shipping_client_name": row_so.get('SHIP_LAST_NAME'),
                        "shipping_street": row_so.get('SHIP_ADDRS'), "shipping_city": row_so.get('SHIP_CITY'),
                        "shipping_province": row_so.get('SHIP_PROV'), "shipping_zip": row_so.get('SHIP_ZIP'),
                        "shipping_phone_1": row_so.get('SHIP_PHONE1'), "shipping_phone_2": row_so.get('SHIP_PHONE2'),
                        "shipping_email_1": row_so.get('SHIP_EMAIL1'), "shipping_email_2": row_so.get('SHIP_EMAIL2'),
                    },
                    "checklist": {
                        "layout_date": clean_date(row_dc.get('LAYOUT')),
                        "client_meeting_date": clean_date(row_dc.get('CLIENT_MEETING_DATE')),
                        "follow_up_date": clean_date(row_so.get('FOLLOW_UPDATE')),
                        "appliance_specs_date": clean_date(row_dc.get('APPLIANCE_SPECS')),
                        "selections_date": clean_date(row_dc.get('SELECTIONS')),
                        "markout_date": clean_date(row_so.get('SITE_MEASURE_DATE')),
                        "review_date": clean_date(row_dc.get('REVIEW_DATE')),
                        "second_markout_date": clean_date(row_so.get('SECOND_MEASURE_DATE')),
                        "flooring_type": row_so.get('FLOORING_TYPE'), "flooring_clearance": row_so.get('FLOORING_CLEARENCE'),
                    }
                }

                # C. Production
                rush_val = clean_boolean(row_so.get('RUSH'))
                prod_data = {
                    "rush": rush_val if rush_val is not None else False,
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
                install_data = {
                    "installer_id": installer_map.get(str(row_so.get('INSTALL_ID', '')).strip()),
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
                    "handles_ordered_at": clean_timestamp_special(row_oc.get('HANDLES')),
                    "acc_ordered_at": clean_timestamp_special(row_oc.get('ACC')),
                    "purchasing_comments": row_oc.get('COMMENTS')
                }

                # F. Job
                base, suffix = parse_legacy_job_number(row_so.get('JOB_NUM'))
                job_data = {
                    "job_base_number": base,
                    "job_suffix": suffix,
                    "is_active": True
                }

                # --- TRANSACTION INSERT ---
                with conn.begin(): 
                    full_payload = {
                        'cabinet': cabinet_data, 'sales_order': sales_order_data,
                        'production': prod_data, 'installation': install_data,
                        'purchasing': purchasing_data, 'job': job_data
                    }
                    
                    # Validation Checks
                    if not job_data['job_base_number']:
                        print(f"❌ SKIP (SO {so_id}): Missing JOB_NUM in source.")
                        skip_count += 1
                        continue
                        
                    if not sales_order_data['client_id']:
                         print(f"❌ SKIP (SO {so_id}): Client ID lookup failed for Legacy Client {row_so.get('CLIENT_NO')}")
                         skip_count += 1
                         continue

                    # Execute Insert
                    insert_single_job(conn, full_payload)
                    success_count += 1

            except Exception as e:
                print(f"❌ FAILED on SO {so_id}: {e}")
                fail_count += 1

        print("\n" + "="*50)
        print("🏁 MIGRATION COMPLETE")
        print(f"✅ Success: {success_count}")
        print(f"⚠️  Skipped: {skip_count}")
        print(f"❌ Failed:  {fail_count}")
        print("="*50)

if __name__ == "__main__":
    migrate_jobs()