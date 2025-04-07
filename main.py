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

def oneDrive_2_db(fp, sheet, schema, table, table_cols):
    df = pd.read_excel(fp,sheet, usecols= lambda x: not x.startswith("Unnamed"))

    engine = create_engine(DB_CONN)
    try:
        df['sys_dt'] = pd.to_datetime('now')

        # handle mixed date as input
        if "promo dt" in df.columns:
            df["promo dt"] = pd.to_datetime(df["promo dt"],format="mixed",errors='coerce')
            
        # handle manual input err
        elif "Promotion Reason" in df.columns:
            df["Promotion Reason"] = df["Promotion Reason"].apply(lambda x: 'Discontinued' if x == 'Disontinued' else x)
        elif "promo category" in df.columns:
            df["promo category"] = df["promo category"].apply(lambda x: 'Discontinued' if x == 'Discontinued item' else x)            
        df.columns = table_cols
        df.to_sql(
            name=table,
            con=engine,
            schema=schema,
            index = False,
            method="multi",
            if_exists= "append"     # Given that schema is defined in SQL Server
        )
        print(f"Successfully wrote {len(df)} rows to {table}")
        
    except Exception as e:
        print(f"Error writing to database: {str(e)}")
    finally:
        engine.dispose()

'''
With 2nd normal form in consideration:
each nonKey Cols --> PK 

'''
def googleDrive_2_db(fp, table, table_cols):
    engine = create_engine(DB_CONN)
    # Skip the first 3 rows and read up until the 17th col and don't promote header
    df = pd.read_csv(fp, header= None, skiprows=3, usecols = range(len(table_cols)-1))
        
    df["sys_dt"] = pd.to_datetime('now')
    df.columns = table_cols
    
    print(df.iloc[:,5:-1].shape)
    
    # cast all numeric fields to Int64
    for col in df.columns[5:-1]:
        df[col] = df[col].astype('Int64')
    
    batch_size = 500
    for i in range(0, len(df), batch_size):
        batch = df.iloc[i:i + batch_size]
        batch.to_sql(
            name=table,
            con=engine,
            schema="landing",
            if_exists="append",
            index=False,
            method= None,
            chunksize=batch_size
        )
    print(f"Successfully wrote {len(df)} rows to {table}")


if __name__ == '__main__':
    # process OneDrive xlsx file 
    sku_baseCols = ['sku','category','promo_reason','descrip','moq','socal', 'ofs','free_sku','feb_sales','inv_quantity','inv_level','sys_dt']
    oneDrive_2_db(r"C:\Users\andrew.chen\Desktop\Enerlites\Promotion Analytics\data\Promotion Data.xlsx", 'potential_skus', 'landing', 'oneDrive_promo_sku_base', sku_baseCols)

    sku_hstCols = ['promo_dt','promo_cat','sku','sys_dt']
    oneDrive_2_db(r"C:\Users\andrew.chen\Desktop\Enerlites\Promotion Analytics\data\Promotion Data.xlsx", 'past sku promo', 'landing', 'oneDrive_hst_promo_sku', sku_hstCols)
    
    # process oceanAir Inventory file from google drive
    oceanAirInvCols = [
        "co_cd",
        "inv_level",
        "sku",
        "asin_num",
        "sku_cat",
        "en_last_120_outbound",
        "en_last_90_outbound",
        "en_last_60_outbound",
        "en_last_30_outbound",
        "tg_last_120_outbound",
        "tg_last_90_outbound",
        "tg_last_60_outbound",
        "tg_last_30_outbound",
        "ca_instock_quantity",
        "il_instock_quantity",
        "lda_instock_quantity",
        "tg_instock_quantity",
        "sys_dt"
    ]
    googleDrive_2_db(r"C:\Users\andrew.chen\Desktop\Enerlites\Promotion Analytics\data\Ocean_Air in Transit List.csv", "googleDrive_ocean_air_inv_fct", oceanAirInvCols)