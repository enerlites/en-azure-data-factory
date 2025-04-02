'''
Deploy below Python Script over Azure Fucntions (Serverless)


Requirements:
`pip install msal requests` --> use microsoft Graph API to connect with OneDrive
`pip install python-dotenv`
`pip install pyodbc`
'''
import os 
from dotenv import load_dotenv
import requests
import msal         # ms Graph API
import hashlib 
import pyodbc
import pandas as pd
from sqlalchemy import create_engine
from office365.runtime.auth.authentication_context import AuthenticationContext
from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.files.file import File
import logging
from datetime import datetime, timedelta
import jwt

# load env vars from .env
load_dotenv()
'''
OneDrive URL:
https://topgreener-my.sharepoint.com/personal/andrew_chen_enerlites_com/Documents/sku%20promotion/Promotion%20Data.xlsx

'''

# Obtain MS access token from Azure Graph API 
def get_ms_access_token():
    try:
        app = msal.ConfidentialClientApplication(
            client_id=os.getenv("CLIENT_ID"),
            client_credential = os.getenv("CLIENT_SECRET"),
            authority=os.getenv("AUTHORITY_URL")
        )
        
        # result = app.acquire_token_for_client(
        #     # username = os.getenv("ONEDRIVE_USERNAME"),
        #     # password =os.getenv("ONEDRIVE_PASS"),
        #     scopes=["https://graph.microsoft.com/.default"]
        # )

        result = app.acquire_token_by_username_password(
                    username=os.getenv("ONEDRIVE_USERNAME"),
                    password=os.getenv("ONEDRIVE_PASS"),
                    scopes=["https://graph.microsoft.com/Files.Read"]  # Ensure correct scope
                )
    
        if "access_token" in result:
            return result["access_token"]
        else:
            raise Exception (f"Fail to get access token: {result}")
    except Exception as e:
        print(f"Error: {str(e)}")
        return None
    
# https://graph.microsoft.com
# https://topgreener-my.sharepoint.com/personal/andrew_chen_enerlites_com/Documents/sku%20promotion/Promotion%20Data.xlsx
def get_file_last_update_dt(access_token):

    # Convert OneDrive URL to MS Graph API 
    ms_graph_endpoint = f"https://graph.microsoft.com/v1.0/users/{os.getenv("ONEDRIVE_USERNAME")}/drive/root:/{os.getenv("ONEDRIVE_FP")}"
    print(f"transformed ms graph endpoint = {ms_graph_endpoint}")

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    res = requests.get(ms_graph_endpoint, headers= headers).json()
    print(f"Meta data of this file:\n{res}\n")
    last_modified_ts_str = res.get('lastModifiedDateTime')
    if last_modified_ts_str:
        return datetime.strptime(last_modified_ts_str, "%Y-%m-%dT%H:%M:%SZ").strftime('%Y-%m-%d')
    return None

if __name__ == "__main__":
    ms_token = get_ms_access_token()
    decoded_token = jwt.decode(ms_token, options={"verify_signature": False})
    print(decoded_token)
    # last_updt = get_file_last_update_dt(ms_token)