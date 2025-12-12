import pandas as pd
import numpy as np
import sys
from datetime import datetime
from sqlalchemy import text
from io import StringIO
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
    return v.replace('\\n', '\n').replace('\t', ' ') if v else None

def parse_legacy_job_number(job_num_raw):
    """
    Parses Text-based Job Numbers.
    M207 -> Base: "M207", Suffix: None
    12345-S1 -> Base: "12345", Suffix: "S1"
    """
    v = clean_val(job_num_raw)
    if v is None: return None, None
    
    # Check for suffix separator
    if '-' in v:
        parts = v.split('-', 1)
        base = parts[0].strip()
        suffix = parts[1].strip()
        return base, suffix
    else:
        # No dash? The whole string is the base number.
        return v, None

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

# --- 3. DATA PREPARATION ---
def prepare_all_data(df_so, df_dc, df_oc, lookups):
    """Pre-process all data into DataFrames for bulk operations"""
    client_map, species_map, color_map, door_map, installer_map = lookups
    
    records = []
    skipped = []
    
    print(f"🔄 Processing {len(df_so)} records...", flush=True)
    
    for idx, row_so in df_so.iterrows():
        if idx % 5000 == 0 and idx > 0:
            print(f"  → Processed {idx}/{len(df_so)}", flush=True)
            
        so_id = clean_val(row_so.get('SALES_OR'))
        if not so_id:
            continue
            
        try:
            # Get related rows
            row_dc = df_dc[df_dc['SALES_OR'] == so_id].iloc[0] if not df_dc[df_dc['SALES_OR'] == so_id].empty else pd.Series()
            row_oc = df_oc[df_oc['SALES_OR'] == so_id].iloc[0] if not df_oc[df_oc['SALES_OR'] == so_id].empty else pd.Series()
            
            # Check client dependency
            client_id = client_map.get(clean_val(row_so.get('CLIENT_NO')))
            if not client_id:
                skipped.append(so_id)
                continue
            
            # Prepare helpers
            rush_val = clean_boolean(row_so.get('RUSH'))
            ship_date_val = clean_date_strict(row_so.get('DATE_SHIP'))
            legacy_conf = clean_boolean(row_so.get('SHIP_DATE_CONFIRM'))
            doors_comp_val = clean_timestamp_special(row_so.get('DOORS_COMP'))
            install_bool = clean_boolean(row_so.get('INSTALL'))
            
            # --- JOB & STAGE LOGIC ---
            # Now returns strings (e.g. "M207", "12345")
            base, suffix = parse_legacy_job_number(row_so.get('JOB_NUM'))
            
            raw_stage = clean_val(row_so.get('STAGE'))
            stage = raw_stage.upper() if raw_stage else 'QUOTE'

            if base is not None:
                # Logic: If Job Number exists but stage says Quote, force SOLD
                if stage == 'QUOTE':
                    stage = 'SOLD'
            else:
                # Logic: If No Job Number, it MUST be a Quote (Sales Order only)
                stage = 'QUOTE'

            # --- SHIP STATUS LOGIC ---
            if ship_date_val is None:
                final_ship_status = 'unprocessed'
            elif ship_date_val is not None and legacy_conf is False:
                final_ship_status = 'tentative' 
            else:
                final_ship_status = 'confirmed'
            
            record = {
                # Cabinet fields
                'cab_species_id': species_map.get(clean_val(row_so.get('SPECIES'))),
                'cab_color_id': color_map.get(clean_val(row_so.get('COLOR'))),
                'cab_door_style_id': door_map.get(clean_val(row_so.get('LOWER_DOOR'))),
                'cab_finish': clean_val(row_so.get('FINISH')),
                'cab_glaze': clean_val(row_so.get('GLAZE')),
                'cab_top_drawer_front': clean_val(row_so.get('DWR_FRONT')),
                'cab_interior': clean_val(row_so.get('INTERIOR')),
                'cab_drawer_box': clean_val(row_so.get('DWR')),
                'cab_drawer_hardware': clean_val(row_so.get('DWR_HRW')),
                'cab_box': clean_val(row_so.get('BOX')),
                'cab_piece_count': clean_val(row_so.get('PIECE_COUNT')),
                'cab_glass_type': clean_val(row_so.get('GLASS_TYPE')),
                'cab_hinge_soft_close': clean_boolean(row_so.get('HINGE_SC')) if clean_boolean(row_so.get('HINGE_SC')) is not None else False,
                'cab_doors_parts_only': clean_boolean(row_so.get('DOORS_PARTS_ONLY')) if clean_boolean(row_so.get('DOORS_PARTS_ONLY')) is not None else False,
                'cab_handles_supplied': clean_boolean(row_so.get('HANDLES')) if clean_boolean(row_so.get('HANDLES')) is not None else False,
                'cab_handles_selected': clean_boolean(row_so.get('HANDLES_SEL')) if clean_boolean(row_so.get('HANDLES_SEL')) is not None else False,
                'cab_glass': clean_boolean(row_so.get('GLASS')) if clean_boolean(row_so.get('GLASS')) is not None else False,
                
                # Sales Order fields
                'so_client_id': client_id,
                'so_stage': stage, 
                'so_total': clean_money(row_so.get('TOTAL')),
                'so_deposit': clean_money(row_so.get('DEPOSIT')),
                'so_designer': clean_val(row_so.get('DESIGNER')),
                'so_comments': clean_text_multiline(row_so.get('COMMENTS')),
                'so_install': install_bool if install_bool is not None else False,
                'so_order_type': clean_val(row_so.get('ORDER_TYPE')) or "Unknown",
                'so_delivery_type': clean_val(row_so.get('DEL_TYPE')) or "Unknown",
                'so_sales_order_number': so_id,
                'so_created_at': clean_date_strict(row_so.get('DATE_SOLD')),
                'so_shipping_client_name': clean_val(row_so.get('SHIP_LAST_NAME')),
                'so_shipping_street': clean_val(row_so.get('SHIP_ADDRS')),
                'so_shipping_city': clean_val(row_so.get('SHIP_CITY')),
                'so_shipping_province': clean_val(row_so.get('SHIP_PROV')),
                'so_shipping_zip': clean_val(row_so.get('SHIP_ZIP')),
                'so_shipping_phone_1': clean_val(row_so.get('SHIP_PHONE1')),
                'so_shipping_phone_2': clean_val(row_so.get('SHIP_PHONE2')),
                'so_shipping_email_1': clean_val(row_so.get('SHIP_EMAIL1')),
                'so_shipping_email_2': clean_val(row_so.get('SHIP_EMAIL2')),
                'so_layout_date': clean_date_strict(row_dc.get('LAYOUT')),
                'so_client_meeting_date': clean_date_strict(row_dc.get('CLIENT_MEETING_DATE')),
                'so_follow_up_date': clean_date_strict(row_so.get('FOLLOW_UPDATE')),
                'so_appliance_specs_date': clean_date_strict(row_dc.get('APPLIANCE_SPECS')),
                'so_selections_date': clean_date_strict(row_dc.get('SELECTIONS')),
                'so_markout_date': clean_date_strict(row_so.get('SITE_MEASURE_DATE')),
                'so_review_date': clean_date_strict(row_dc.get('REVIEW_DATE')),
                'so_second_markout_date': clean_date_strict(row_so.get('SECOND_MEASURE_DATE')),
                'so_flooring_type': clean_val(row_so.get('FLOORING_TYPE')),
                'so_flooring_clearance': clean_val(row_so.get('FLOORING_CLEARENCE')),
                
                # Production fields
                'prod_rush': rush_val if rush_val is not None else False,
                'prod_placement_date': clean_date_strict(row_so.get('PROD_IN_DATE')),
                'prod_doors_in_schedule': clean_date_strict(row_so.get('DATE_DOR_START')),
                'prod_doors_out_schedule': clean_date_strict(row_so.get('DATE_DOR_FIN')),
                'prod_cut_finish_schedule': clean_date_strict(row_so.get('ISSUE_DATE')),
                'prod_cut_melamine_schedule': clean_date_strict(row_so.get('MEL_DATE')),
                'prod_paint_in_schedule': clean_date_strict(row_so.get('PAINT_IN')),
                'prod_paint_out_schedule': clean_date_strict(row_so.get('PAINT_DATE')),
                'prod_assembly_schedule': clean_date_strict(row_so.get('ASS_DATE')),
                'prod_ship_schedule': ship_date_val,
                'prod_production_comments': clean_text_multiline(row_so.get('PROD_MEMO')),
                'prod_in_plant_actual': doors_comp_val if doors_comp_val else None, 
                'prod_doors_completed_actual': doors_comp_val,
                'prod_cut_finish_completed_actual': clean_timestamp_special(row_so.get('ISSUED')),
                'prod_cut_melamine_completed_actual': clean_timestamp_special(row_so.get('MEL__ISSUED')),
                'prod_paint_completed_actual': clean_timestamp_special(row_so.get('PAINT_COMP')),
                'prod_assembly_completed_actual': clean_timestamp_special(row_so.get('ASSEMBLED')),
                'prod_custom_finish_completed_actual': clean_date_strict(row_so.get('F_C_DATE')),
                'prod_ship_status': final_ship_status,
                
                # Installation fields
                'inst_installer_id': installer_map.get(clean_val(row_so.get('INSTALL_ID'))),
                'inst_has_shipped': clean_boolean(row_so.get('HAS_SHIP')) if clean_boolean(row_so.get('HAS_SHIP')) is not None else False,
                'inst_installation_date': clean_date_strict(row_so.get('INSTALL_DATE')),
                'inst_installation_completed': clean_timestamp_special(row_so.get('STATUS')),
                'inst_inspection_date': clean_date_strict(row_so.get('INSPECTION_DATE')),
                'inst_wrap_date': clean_date_strict(row_so.get('WRAP_DATE')),
                'inst_wrap_completed': clean_timestamp_special(row_so.get('WRAP_COMP')),
                'inst_installation_notes': clean_text_multiline(row_so.get('INSTALL_MEMO')),
                
                # Job fields
                'job_base_number': base, # Now passing a STRING
                'job_suffix': suffix,
                'job_is_active': True,
                
                # Purchase fields
                'purch_doors_ordered_at': clean_timestamp_special(row_so.get('DOORS_ORDERED')),
                'purch_glass_ordered_at': clean_timestamp_special(row_so.get('GLASS_ORD')),
                'purch_handles_ordered_at': clean_timestamp_special(row_oc.get('HANDLES')),
                'purch_acc_ordered_at': clean_timestamp_special(row_oc.get('ACC')),
                'purch_purchasing_comments': clean_text_multiline(row_oc.get('COMMENTS')),
            }
            
            records.append(record)
            
        except Exception as e:
            print(f"⚠️ Error preparing {so_id}: {e}", flush=True)
            skipped.append(so_id)
    
    df_records = pd.DataFrame(records)
    print(f"✅ Prepared {len(df_records)} records for insertion", flush=True)
    
    return df_records, skipped

# --- 4. MAIN MIGRATION ---
def migrate_jobs():
    engine = get_db_engine()
    if not engine:
        return

    print(f"📂 Reading Excel Data from {INPUT_FILE}...", flush=True)
    xls = pd.ExcelFile(INPUT_FILE)
    df_so = pd.read_excel(xls, 'SalesOrders')
    df_dc = pd.read_excel(xls, 'DesignChecks')
    df_oc = pd.read_excel(xls, 'OrderChecks')

    # Clean IDs
    df_so['SALES_OR'] = df_so['SALES_OR'].apply(clean_val)
    df_dc['SALES_OR'] = df_dc['SALES_OR'].apply(clean_val)
    df_oc['SALES_OR'] = df_oc['SALES_OR'].apply(clean_val)
    df_so = df_so.dropna(subset=['SALES_OR'])

    print(f"🔎 Found {len(df_so)} Sales Orders.", flush=True)

    with engine.connect() as conn:
        lookups = fetch_lookups(conn)
        
        # Prepare all data at once
        df_all, skipped = prepare_all_data(df_so, df_dc, df_oc, lookups)
        
        print("🚀 Starting ultra-fast bulk insert...", flush=True)
        
        try:
            # Use raw connection for maximum speed
            raw_conn = conn.connection
            cursor = raw_conn.cursor()
            
            # Step 1: Bulk insert cabinets
            print("  → Inserting cabinets...", flush=True)
            cab_cols = ['species_id', 'color_id', 'door_style_id', 'finish', 'glaze',
                       'top_drawer_front', 'interior', 'drawer_box', 'drawer_hardware',
                       'box', 'hinge_soft_close', 'doors_parts_only', 'handles_supplied',
                       'handles_selected', 'glass', 'piece_count', 'glass_type']
            
            # Convert pandas NaN/None to SQL NULL properly
            cab_data = []
            for _, row in df_all.iterrows():
                cab_row = [
                    int(row['cab_species_id']) if pd.notna(row['cab_species_id']) else None,
                    int(row['cab_color_id']) if pd.notna(row['cab_color_id']) else None,
                    int(row['cab_door_style_id']) if pd.notna(row['cab_door_style_id']) else None,
                    row['cab_finish'] if pd.notna(row['cab_finish']) else None,
                    row['cab_glaze'] if pd.notna(row['cab_glaze']) else None,
                    row['cab_top_drawer_front'] if pd.notna(row['cab_top_drawer_front']) else None,
                    row['cab_interior'] if pd.notna(row['cab_interior']) else None,
                    row['cab_drawer_box'] if pd.notna(row['cab_drawer_box']) else None,
                    row['cab_drawer_hardware'] if pd.notna(row['cab_drawer_hardware']) else None,
                    row['cab_box'] if pd.notna(row['cab_box']) else None,
                    bool(row['cab_hinge_soft_close']),
                    bool(row['cab_doors_parts_only']),
                    bool(row['cab_handles_supplied']),
                    bool(row['cab_handles_selected']),
                    bool(row['cab_glass']),
                    row['cab_piece_count'] if pd.notna(row['cab_piece_count']) else None,
                    row['cab_glass_type'] if pd.notna(row['cab_glass_type']) else None,
                ]
                cab_data.append(tuple(cab_row))
            
            cab_sql = f"""
                INSERT INTO public.cabinets ({','.join(cab_cols)})
                VALUES %s
                RETURNING id
            """
            cabinet_ids = execute_values(cursor, cab_sql, cab_data, fetch=True)
            cabinet_ids = [row[0] for row in cabinet_ids]
            raw_conn.commit()
            
            # Step 2: Bulk insert sales orders
            print("  → Inserting sales orders...", flush=True)
            
            # Split into with/without created_at
            so_with_date = []
            so_without_date = []
            
            for idx, row in df_all.iterrows():
                base_data = [
                    int(row['so_client_id']) if pd.notna(row['so_client_id']) else None,
                    cabinet_ids[idx],
                    row['so_stage'],
                    float(row['so_total']) if pd.notna(row['so_total']) else 0.0,
                    float(row['so_deposit']) if pd.notna(row['so_deposit']) else 0.0,
                    row['so_designer'] if pd.notna(row['so_designer']) else None,
                    row['so_comments'] if pd.notna(row['so_comments']) else None,
                    bool(row['so_install']),
                    row['so_order_type'],
                    row['so_delivery_type'],
                    row['so_sales_order_number'] if pd.notna(row['so_sales_order_number']) else None,
                    row['so_shipping_client_name'] if pd.notna(row['so_shipping_client_name']) else None,
                    row['so_shipping_street'] if pd.notna(row['so_shipping_street']) else None,
                    row['so_shipping_city'] if pd.notna(row['so_shipping_city']) else None,
                    row['so_shipping_province'] if pd.notna(row['so_shipping_province']) else None,
                    row['so_shipping_zip'] if pd.notna(row['so_shipping_zip']) else None,
                    row['so_shipping_phone_1'] if pd.notna(row['so_shipping_phone_1']) else None,
                    row['so_shipping_phone_2'] if pd.notna(row['so_shipping_phone_2']) else None,
                    row['so_shipping_email_1'] if pd.notna(row['so_shipping_email_1']) else None,
                    row['so_shipping_email_2'] if pd.notna(row['so_shipping_email_2']) else None,
                    row['so_layout_date'] if pd.notna(row['so_layout_date']) else None,
                    row['so_client_meeting_date'] if pd.notna(row['so_client_meeting_date']) else None,
                    row['so_follow_up_date'] if pd.notna(row['so_follow_up_date']) else None,
                    row['so_appliance_specs_date'] if pd.notna(row['so_appliance_specs_date']) else None,
                    row['so_selections_date'] if pd.notna(row['so_selections_date']) else None,
                    row['so_markout_date'] if pd.notna(row['so_markout_date']) else None,
                    row['so_review_date'] if pd.notna(row['so_review_date']) else None,
                    row['so_second_markout_date'] if pd.notna(row['so_second_markout_date']) else None,
                    row['so_flooring_type'] if pd.notna(row['so_flooring_type']) else None,
                    row['so_flooring_clearance'] if pd.notna(row['so_flooring_clearance']) else None,
                ]
                
                if pd.notna(row['so_created_at']):
                    so_with_date.append((idx, tuple(base_data + [row['so_created_at']])))
                else:
                    so_without_date.append((idx, tuple(base_data)))
            
            so_cols_base = ['client_id', 'cabinet_id', 'stage', 'total', 'deposit', 'designer',
                           'comments', 'install', 'order_type', 'delivery_type', 'sales_order_number',
                           'shipping_client_name', 'shipping_street', 'shipping_city', 'shipping_province',
                           'shipping_zip', 'shipping_phone_1', 'shipping_phone_2', 'shipping_email_1',
                           'shipping_email_2', 'layout_date', 'client_meeting_date', 'follow_up_date',
                           'appliance_specs_date', 'selections_date', 'markout_date', 'review_date',
                           'second_markout_date', 'flooring_type', 'flooring_clearance']
            
            so_ids = [None] * len(df_all)
            
            if so_without_date:
                so_sql = f"""
                    INSERT INTO public.sales_orders ({','.join(so_cols_base)})
                    VALUES %s RETURNING id
                """
                result = execute_values(cursor, so_sql, [d[1] for d in so_without_date], fetch=True)
                for i, (orig_idx, _) in enumerate(so_without_date):
                    so_ids[orig_idx] = result[i][0]
                raw_conn.commit()
            
            if so_with_date:
                so_sql = f"""
                    INSERT INTO public.sales_orders ({','.join(so_cols_base + ['created_at'])})
                    VALUES %s RETURNING id
                """
                result = execute_values(cursor, so_sql, [d[1] for d in so_with_date], fetch=True)
                for i, (orig_idx, _) in enumerate(so_with_date):
                    so_ids[orig_idx] = result[i][0]
                raw_conn.commit()
            
            # Step 3: Bulk insert production
            print("  → Inserting production records...", flush=True)
            prod_data = []
            for _, row in df_all.iterrows():
                if pd.notna(row['job_base_number']):
                    prod_row = [
                        bool(row['prod_rush']),
                        row['prod_placement_date'] if pd.notna(row['prod_placement_date']) else None,
                        row['prod_doors_in_schedule'] if pd.notna(row['prod_doors_in_schedule']) else None,
                        row['prod_doors_out_schedule'] if pd.notna(row['prod_doors_out_schedule']) else None,
                        row['prod_cut_finish_schedule'] if pd.notna(row['prod_cut_finish_schedule']) else None,
                        row['prod_cut_melamine_schedule'] if pd.notna(row['prod_cut_melamine_schedule']) else None,
                        row['prod_paint_in_schedule'] if pd.notna(row['prod_paint_in_schedule']) else None,
                        row['prod_paint_out_schedule'] if pd.notna(row['prod_paint_out_schedule']) else None,
                        row['prod_assembly_schedule'] if pd.notna(row['prod_assembly_schedule']) else None,
                        row['prod_ship_schedule'] if pd.notna(row['prod_ship_schedule']) else None,
                        row['prod_production_comments'] if pd.notna(row['prod_production_comments']) else None,
                        row['prod_in_plant_actual'] if pd.notna(row['prod_in_plant_actual']) else None,
                        row['prod_doors_completed_actual'] if pd.notna(row['prod_doors_completed_actual']) else None,
                        row['prod_cut_finish_completed_actual'] if pd.notna(row['prod_cut_finish_completed_actual']) else None,
                        row['prod_cut_melamine_completed_actual'] if pd.notna(row['prod_cut_melamine_completed_actual']) else None,
                        row['prod_paint_completed_actual'] if pd.notna(row['prod_paint_completed_actual']) else None,
                        row['prod_assembly_completed_actual'] if pd.notna(row['prod_assembly_completed_actual']) else None,
                        row['prod_custom_finish_completed_actual'] if pd.notna(row['prod_custom_finish_completed_actual']) else None,
                        row['prod_ship_status'] if pd.notna(row['prod_ship_status']) else 'unprocessed',
                    ]
                    prod_data.append(tuple(prod_row))
                else:
                    prod_data.append(None)
            
            real_prod_data = [p for p in prod_data if p is not None]
            
            prod_ids_map = {}
            if real_prod_data:
                prod_sql = """
                    INSERT INTO public.production_schedule (
                        rush, placement_date, doors_in_schedule, doors_out_schedule,
                        cut_finish_schedule, cut_melamine_schedule, paint_in_schedule,
                        paint_out_schedule, assembly_schedule, ship_schedule, production_comments,
                        in_plant_actual, doors_completed_actual, cut_finish_completed_actual,
                        cut_melamine_completed_actual, paint_completed_actual, assembly_completed_actual,
                        custom_finish_completed_actual, ship_status
                    ) VALUES %s RETURNING prod_id
                """
                prod_results = execute_values(cursor, prod_sql, real_prod_data, fetch=True)
                
                current_res_idx = 0
                for i, p in enumerate(prod_data):
                    if p is not None:
                        prod_ids_map[i] = prod_results[current_res_idx][0]
                        current_res_idx += 1
                raw_conn.commit()
            
            # Step 4: Bulk insert installations
            print("  → Inserting installation records...", flush=True)
            inst_data = []
            for _, row in df_all.iterrows():
                if pd.notna(row['job_base_number']):
                    inst_row = [
                        int(row['inst_installer_id']) if pd.notna(row['inst_installer_id']) else None,
                        bool(row['inst_has_shipped']),
                        row['inst_installation_date'] if pd.notna(row['inst_installation_date']) else None,
                        row['inst_installation_completed'] if pd.notna(row['inst_installation_completed']) else None,
                        row['inst_inspection_date'] if pd.notna(row['inst_inspection_date']) else None,
                        row['inst_wrap_date'] if pd.notna(row['inst_wrap_date']) else None,
                        row['inst_wrap_completed'] if pd.notna(row['inst_wrap_completed']) else None,
                        row['inst_installation_notes'] if pd.notna(row['inst_installation_notes']) else None,
                    ]
                    inst_data.append(tuple(inst_row))
                else:
                    inst_data.append(None)
            
            real_inst_data = [i for i in inst_data if i is not None]
            inst_ids_map = {}
            if real_inst_data:
                inst_sql = """
                    INSERT INTO public.installation (
                        installer_id, has_shipped, installation_date, installation_completed,
                        inspection_date, wrap_date, wrap_completed, installation_notes
                    ) VALUES %s RETURNING installation_id
                """
                inst_results = execute_values(cursor, inst_sql, real_inst_data, fetch=True)
                current_res_idx = 0
                for i, d in enumerate(inst_data):
                    if d is not None:
                        inst_ids_map[i] = inst_results[current_res_idx][0]
                        current_res_idx += 1
                raw_conn.commit()
            
            # Step 5: Insert jobs and purchases
            print("  → Inserting jobs and purchases...", flush=True)
            success_jobs = 0
            success_quotes = 0
            
            job_data = []
            purch_data = []
            
            job_insert_indices = []
            
            for idx, row in df_all.iterrows():
                if pd.notna(row['job_base_number']):
                    job_data.append([
                        str(row['job_base_number']), # Force String now
                        row['job_suffix'] if pd.notna(row['job_suffix']) else None,
                        so_ids[idx],
                        prod_ids_map[idx],
                        inst_ids_map[idx],
                        bool(row['job_is_active']),
                    ])
                    job_insert_indices.append(idx)
                    success_jobs += 1
                else:
                    success_quotes += 1
            
            if job_data:
                job_sql = """
                    INSERT INTO public.jobs (
                        job_base_number, job_suffix, sales_order_id, prod_id, installation_id, is_active
                    ) VALUES %s RETURNING id
                """
                job_ids_result = execute_values(cursor, job_sql, job_data, fetch=True)
                job_ids = [row[0] for row in job_ids_result]
                raw_conn.commit()
                
                for i, job_id in enumerate(job_ids):
                    original_idx = job_insert_indices[i]
                    row = df_all.iloc[original_idx]
                    purch_data.append([
                        job_id,
                        row['purch_doors_ordered_at'] if pd.notna(row['purch_doors_ordered_at']) else None,
                        row['purch_glass_ordered_at'] if pd.notna(row['purch_glass_ordered_at']) else None,
                        row['purch_handles_ordered_at'] if pd.notna(row['purch_handles_ordered_at']) else None,
                        row['purch_acc_ordered_at'] if pd.notna(row['purch_acc_ordered_at']) else None,
                        row['purch_purchasing_comments'] if pd.notna(row['purch_purchasing_comments']) else None,
                    ])
                
                if purch_data:
                    purch_sql = """
                        INSERT INTO public.purchase_tracking (
                            job_id, doors_ordered_at, glass_ordered_at, handles_ordered_at,
                            acc_ordered_at, purchasing_comments
                        ) VALUES %s
                    """
                    execute_values(cursor, purch_sql, purch_data)
                    raw_conn.commit()
            
            print("\n" + "="*50)
            print("🏁 MIGRATION COMPLETE", flush=True)
            print(f"✅ Full Jobs Created: {success_jobs}", flush=True)
            print(f"✅ Quotes Created:    {success_quotes}", flush=True)
            print(f"⚠️  Skipped (No Client): {len(skipped)}", flush=True)
            print("="*50, flush=True)
            
        except Exception as e:
            raw_conn.rollback()
            print(f"❌ Error during bulk insert: {e}", flush=True)
            raise

if __name__ == "__main__":
    migrate_jobs()