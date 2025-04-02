import os 
import pandas as pd
from sqlalchemy import create_engine

promo_sku_base = pd.read_excel('./data/Promotion%20Data.xlsx', sheet_name='v2 (prefer)sku')