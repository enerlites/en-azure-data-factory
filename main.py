import os 
import pandas as pd
from sqlalchemy import create_engine
from IPython.display import display
import os 
from dotenv import load_dotenv
import urllib.parse

# load env vars from .env
load_dotenv()
DB_CONN = f"mssql+pyodbc://sqladmin:{urllib.parse.quote_plus(os.getenv("DB_PASS"))}@{os.getenv("DB_SERVER")}/master?driver=ODBC+Driver+17+for+SQL+Server&encrypt=yes"

def oneDrive_2_azure(fp, sheet, table, table_cols):
    df = pd.read_excel(fp,sheet, usecols= lambda x: not x.startswith("Unnamed"))

    engine = create_engine(DB_CONN)
    try:
        df['sys_dt'] = pd.to_datetime('now')
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

if __name__ == '__main__':
    sku_baseCols = ['sku','category','promo_reason','descrip','moq','socal', 'ofs','free_sku','feb_sales','inv_quantity','inv_level','sys_dt']
    oneDrive_2_azure(r"C:\Users\andrew.chen\Desktop\Enerlites\Promotion Analytics\data\Promotion Data.xlsx", 'potential_skus', 'oneDrive_promo_sku_base', sku_baseCols)