import pandas as pd
from sqlalchemy import text # keep text just in case, though unused in snippet
import sys
from migration_utils import get_db_engine, clean_boolean

# --- MAIN MIGRATION FUNCTION ---
def migrate_lookup_tables():
    engine = get_db_engine()
    if not engine:
        return
    print("✅ DB Engine Connected.")

    xls_path = './data/DataFinal.xlsx'
    try:
        xls = pd.ExcelFile(xls_path)
    except Exception as e:
        print(f"❌ Could not open Excel file: {e}")
        return

    # ---------------------------------------------------------
    # 1. SPECIES
    # ---------------------------------------------------------
    print("\n--- 1. Migrating SPECIES ---")
    try:
        # FIX: keep_default_na=False prevents "NA" from becoming NaN
        df_species_source = pd.read_excel(xls, 'Species', keep_default_na=False)
        
        # Transform
        df_species_source['Species'] = df_species_source['Species'].astype(str).str.strip()
        # Filter out empty strings that result from empty cells when keep_default_na=False
        df_species_source = df_species_source[df_species_source['Species'] != '']
        
        df_species_source['Prefinished'] = df_species_source['Prefinished'].apply(clean_boolean).fillna(False)
        
        # Get Existing DB Data
        existing_species = pd.read_sql("SELECT \"Species\" FROM public.species", engine)
        existing_set = set(existing_species['Species'].dropna().astype(str).str.strip().unique())
        
        # Filter
        new_species = df_species_source[~df_species_source['Species'].isin(existing_set)].copy()
        
        if not new_species.empty:
            new_species = new_species[['Species', 'Prefinished']].drop_duplicates(subset=['Species'])
            new_species.to_sql('species', engine, if_exists='append', index=False)
            print(f"✅ Inserted {len(new_species)} new Species.")
        else:
            print("ℹ️  No new Species to insert (all exist).")

    except ValueError as e:
         print(f"⚠️  Skipping Species: Sheet 'Species' not found or error: {e}")
    except Exception as e:
        print(f"❌ Error migrating Species: {e}")

    # ---------------------------------------------------------
    # 2. COLORS
    # ---------------------------------------------------------
    print("\n--- 2. Migrating COLORS ---")
    try:
        # FIX: keep_default_na=False prevents "N/A" from becoming NaN
        df_colors_source = pd.read_excel(xls, 'Colors', keep_default_na=False)
        
        # Transform
        df_colors_source['Name'] = df_colors_source['COLOR'].astype(str).str.strip()
        # Filter out empty strings
        df_colors_source = df_colors_source[df_colors_source['Name'] != '']
        
        # Get Existing DB Data
        existing_colors = pd.read_sql("SELECT \"Name\" FROM public.colors", engine)
        existing_set = set(existing_colors['Name'].dropna().astype(str).str.strip().unique())
    

        # Filter: Keep only those NOT in the existing set
        new_colors = df_colors_source[~df_colors_source['Name'].isin(existing_set)].copy()
        
        if not new_colors.empty:
            new_colors = new_colors[['Name']].drop_duplicates()
            new_colors.to_sql('colors', engine, if_exists='append', index=False)
            print(f"✅ Inserted {len(new_colors)} new Colors.")
        else:
            print("ℹ️  No new Colors to insert.")

    except ValueError as e:
         print(f"⚠️  Skipping Colors: Sheet 'Colors' not found or error: {e}")
    except Exception as e:
        print(f"❌ Error migrating Colors: {e}")

    # ---------------------------------------------------------
    # 3. DOOR STYLES
    # ---------------------------------------------------------
    print("\n--- 3. Migrating DOOR STYLES ---")
    try:
        # FIX: keep_default_na=False
        df_doors_source = pd.read_excel(xls, 'DoorStyles', keep_default_na=False)
        
        # Transform
        df_doors_source['name'] = df_doors_source['LOWER_DOOR'].astype(str).str.strip()
        # Filter out empty strings
        df_doors_source = df_doors_source[df_doors_source['name'] != '']

        df_doors_source['model'] = df_doors_source['name'] 
        df_doors_source['is_pre_manufactured'] = False
        df_doors_source['is_made_in_house'] = False

        # Get Existing DB Data
        existing_doors = pd.read_sql("SELECT name FROM public.door_styles", engine)
        existing_set = set(existing_doors['name'].dropna().astype(str).str.strip().unique())
        
        # Filter
        new_doors = df_doors_source[~df_doors_source['name'].isin(existing_set)].copy()
        
        if not new_doors.empty:
            new_doors = new_doors[['name', 'model', 'is_pre_manufactured', 'is_made_in_house']].drop_duplicates(subset=['model'])
            new_doors.to_sql('door_styles', engine, if_exists='append', index=False)
            print(f"✅ Inserted {len(new_doors)} new Door Styles.")
        else:
            print("ℹ️  No new Door Styles to insert.")

    except ValueError as e:
         print(f"⚠️  Skipping DoorStyles: Sheet 'DoorStyles' not found or error: {e}")
    except Exception as e:
        print(f"❌ Error migrating Door Styles: {e}")

if __name__ == "__main__":
    migrate_lookup_tables()