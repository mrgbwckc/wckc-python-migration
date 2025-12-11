import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime

# =================CONFIGURATION=================
# Using the connection string provided
DATABASE_URL = "postgresql://postgres.ajxvalhasysijvybgmhq:wckcnewpass2509@aws-0-us-west-2.pooler.supabase.com:5432/postgres"

def get_db_engine():
    """Creates and returns a SQLAlchemy engine."""
    try:
        engine = create_engine(DATABASE_URL)
        # Test connection
        with engine.connect() as conn:
            pass
        print("✅ Database connection successful.")
        return engine
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return None

def clean_boolean(val):
    """
    Converts 'FALSE', 'TRUE', 'F', 'T', 'YES', 'NO', 'Y', 'N' strings to Python Booleans.
    Returns None/False depending on context (adapted to cover both previous use cases).
    """
    if pd.isna(val) or str(val).strip() == '':
        return None # Return None for empty, let caller decide default or keep as None
        
    s = str(val).strip().upper()
    if s in ['TRUE', 'T', 'YES', 'Y', '1']:
        return True
    if s in ['FALSE', 'F', 'NO', 'N', '0']:
        return False
        
    return None

def clean_date(val):
    """
    Parses dates safely. Handles Excel datetime objects, strings, and NaNs.
    """
    if pd.isna(val) or val == '':
        return datetime.now()
    
    if isinstance(val, datetime):
        return val
        
    try:
        return pd.to_datetime(val, dayfirst=True) 
    except:
        return datetime.now()
