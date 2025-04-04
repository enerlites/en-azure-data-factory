'''
Packages requiremetns:
a) `pip install msal httpx python-dotenv`
'''
import os 
import pandas as pd
from sqlalchemy import create_engine
from IPython.display import display
import os 
from dotenv import load_dotenv
import urllib.parse;
import requests
from io import BytesIO

# load env vars from .env
load_dotenv()
DB_CONN = f"mssql+pyodbc://sqladmin:{urllib.parse.quote_plus(os.getenv("DB_PASS"))}@{os.getenv("DB_SERVER")}/master?driver=ODBC+Driver+17+for+SQL+Server&encrypt=yes"

def get_oneDrive_File(url, access_token):
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Accept': 'application/json'
    }
    res = requests.get(url, headers=headers)
    return BytesIO(res.content)         # return an xlsx file 

def local_2_azure(fp, sheet, table, table_cols):
    df = pd.read_excel(fp,sheet, usecols= lambda x: not x.startswith("Unnamed"))

    engine = create_engine(DB_CONN)
    try:
        df['sys_dt'] = pd.to_datetime('now')

        if "promo dt" in df.columns:
            df["promo dt"] = pd.to_datetime(df["promo dt"],format="mixed",errors='coerce')
        df.columns = table_cols
        df.to_sql(
            name=table,
            con=engine,
            schema="landing",
            index = False,
            method="multi",
            if_exists= "append"     # Given that schema is defined in SQL Server
        )
        print(f"Successfully wrote {len(df)} rows to {table}")
        
    except Exception as e:
        print(f"Error writing to database: {str(e)}")
    finally:
        engine.dispose()

def googleDrive_2_azure(fp, sheet, table, table_cols):
    pass

if __name__ == '__main__':
    sku_baseCols = ['sku','category','promo_reason','descrip','moq','socal', 'ofs','free_sku','feb_sales','inv_quantity','inv_level','sys_dt']
    local_2_azure(r"C:\Users\andrew.chen\Desktop\Enerlites\Promotion Analytics\data\Promotion Data.xlsx", 'potential_skus', 'oneDrive_promo_sku_base', sku_baseCols)

    sku_hstCols = ['promo_dt','promo_cat','sku','sys_dt']
    local_2_azure(r"C:\Users\andrew.chen\Desktop\Enerlites\Promotion Analytics\data\Promotion Data.xlsx", 'past sku promo', 'oneDrive_hst_promo_sku', sku_hstCols)