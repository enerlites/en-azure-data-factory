import datetime
import logging
from datetime import datetime
import azure.functions as func
from oneDriveETL import *

# Define a monthly scheduler that run the cron procedures at 12 AM on 15th of each month
def monthly_promotion_brochure_job():
    try:
        # create an instance to read from andrew.chen@enerlites.com
        oneDriveReader = OneDriveFlatFileReader("andrew.chen@enerlites.com")
        
        # Define file management related fields
        folderPath = "sku promotion"
        files = ['Promotion Data.xlsx', 'Ocean_Air in Transit List.xlsx']
        sku_baseCols = ['sku','category','promo_reason','descrip','moq','socal', 'ofs','free_sku','feb_sales','inv_quantity','inv_level', 'photo_url', 'sys_dt']
        sku_hstCols = ['promo_dt','promo_cat','sku','sys_dt']
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
        
        # load potential sku from OneDrive first
        sku_base_df = oneDriveReader.read_excel_from_onedrive(
            folderPath,
            files[0],
            sheet_name='potential_skus'
        )
        hst_sku_df = oneDriveReader.read_excel_from_onedrive(
            folderPath,
            files[0],
            sheet_name='past sku promo'
        )
        oceanAirInv_df = oneDriveReader.read_excel_from_onedrive(
            folderPath,
            files[1],
            sheet_name='Friday Inventory TGEN'
        )
        
        # load 2 respective tables
        sku_base_db = AzureDBWriter(sku_base_df,sku_baseCols)
        sku_base_db.flatFile2db('landing', 'oneDrive_promo_sku_base')
        hst_sku_db = AzureDBWriter(hst_sku_df,sku_hstCols)
        hst_sku_db.flatFile2db('landing', 'oneDrive_hst_promo_sku')
        
        oceanAirInv_db = AzureDBWriter(oceanAirInv_df,oceanAirInvCols)
        oceanAirInv_db.oceanAir_Inv_preprocess()
        oceanAirInv_db.flatFile2db('landing', 'googleDrive_ocean_air_inv_fct')
        print(f">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
        print(f"monthly_promotion_brochure_auto_job() executed at {datetime.now()} !\n")
        
    except Exception as e:
        print(f"{str(e)}") 

def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.now(datetime.timezone.utc).replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.warning('The timer is past due!')

    try:
        logging.info(f"Starting monthly promotion brochure job at {utc_timestamp}")
        monthly_promotion_brochure_job()  # Call your existing function
        logging.info("Monthly promotion brochure job completed successfully")
    except Exception as e:
        logging.error(f"Error in monthly promotion brochure job: {str(e)}")
        raise