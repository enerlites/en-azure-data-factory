# DB class for Azure SQL db functions
class AzureDBWriter():
    def __init__(self, df, tableCols):
        load_dotenv()           # load the .env vars
        self.DB_CONN = f"mssql+pyodbc://sqladmin:{urllib.parse.quote_plus(os.getenv("DB_PASS"))}@{os.getenv("DB_SERVER")}:1433/enerlitesDB?driver=ODBC+Driver+17+for+SQL+Server&encrypt=yes"
        self.myDf = df 
        self.myCols = tableCols
        
    # OceanAir Inventory google xlsx sheet preprocessing (inplace operation)
    # Skip the first 3 rows and only read in len(tableCols) -1 cols
    def oceanAir_Inv_preprocess(self):
        self.myDf = self.myDf.iloc[2:, :len(self.myCols) - 1]
        numeric_cols = self.myDf.columns[5:-1]
        self.myDf[numeric_cols] = self.myDf[numeric_cols].astype('Int64')
        return self
        
    # commit flatFile 2 azure db 
    def flatFile2db (self, schema, table):
        engine = create_engine(self.DB_CONN)
        try:
            df = self.myDf.copy()
            tableCols = self.myCols
            # append getdate() datetim2 
            df['sys_dt'] = pd.to_datetime('now')

            '''Below section for data cleaning prior to db load'''
            if "promo dt" in df.columns:
                df["promo dt"] = pd.to_datetime(df["promo dt"],format="mixed",errors='coerce') 
            # handle manual input err
            elif "Promotion Reason" in df.columns:
                df["Promotion Reason"] = df["Promotion Reason"].apply(lambda x: 'Discontinued' if x == 'Disontinued' else x)
            elif "promo category" in df.columns:
                df["promo category"] = df["promo category"].apply(lambda x: 'Discontinued' if x == 'Discontinued item' else x)   
            # persist df name with that of defined in ssms          
            df.columns = tableCols
            self.myDf = df
            
            batch_size = 500
            for i in range(0, len(df), batch_size):
                batch = df.iloc[i:i + batch_size]
                batch.to_sql(
                    name=table,
                    con=engine,
                    schema=schema,
                    if_exists="append",
                    index=False,
                    method= None,
                    chunksize=batch_size
                )
            print(f"Successfully wrote {len(df)} rows to {table}")
            
        except Exception as e:
            print(f"Error writing to database: {str(e)}")
        finally:
            engine.dispose()