'''
Below Script requires Application Permissions (API permission) --> Run as background service without signed-in user

'''
import os
import msal
import requests
import pandas as pd
from io import BytesIO
from dotenv import load_dotenv
from urllib.parse import quote
from IPython import display

# https://topgreener-my.sharepoint.com/:x:/g/personal/andrew_chen_enerlites_com/EXUvkiPOiVVEkiSkkbB6n84BkTEP2oEfcedADgU3_yES_A?e=RPbUx0

# create a class with all functionalities 
class OneDriveExcelReader:
    def __init__(self):
        load_dotenv()
        self.client_id = os.getenv("AZ_CLI_ID")
        self.client_secret = os.getenv("AZ_CLI_SECRET")
        self.tenant_id = os.getenv("AZ_TENANT_ID")
        self.user_principal = "andrew.chen@enerlites.com"
        self.base_graph_url = "https://graph.microsoft.com/v1.0"
    
    # get the Azure access token
    def get_access_token(self):
        authority = f"https://login.microsoftonline.com/{self.tenant_id}"
        app = msal.ConfidentialClientApplication(
            self.client_id,
            authority=authority,
            client_credential=self.client_secret
        )

        result = app.acquire_token_for_client(scopes=["https://graph.microsoft.com/.default"])

        if "access_token" in result:
            return result["access_token"]
        else:
            error_details = result.get("error_description", "No error description provided")
            raise Exception(f"Authentication failed: {result.get('error')} - {error_details}")
    
    # get principal user's driver_id
    def get_drive_id(self, access_token):
        url = f"{self.base_graph_url}/users/{self.user_principal}/drive"
        
        headers = {
            "Authorization": f"Bearer {access_token}"
        }

        try:
            # List all available drives (including personal OneDrive)
            response = requests.get(url, headers=headers, timeout=30)
            if response.status_code == 200:
                print(f"\n{self.user_principal} exists with drive id = \'{response.json()["id"]}\'\n")
                return response.json()["id"]
            elif response.status_code == 404:
                raise Exception("OneDrive not found. It may not be provisioned yet.")
            else:
                raise Exception(f"API Error: {response.status_code} - {response.text}")

        except requests.exceptions.RequestException as e:
            raise Exception(f"Failed to get drive info: {str(e)}")
    
    # Get file id with (access_token, driver_id, folderName)
    def get_fileDownload_url(self, access_token, driver_id, folderName, fileName):
        oneDriveBaseURL = f"{self.base_graph_url}/drives/{driver_id}"
        FolderURL = f"{oneDriveBaseURL}/root/children"
        headers = {"Authorization": f"Bearer {access_token}"}
        
        try:
            res = requests.get(FolderURL, headers= headers, timeout= 30)
            items = res.json().get('value', [])         # return a list of python dict
            folderId = None
            
            # found foldername first within the oneDrive root dir
            for item in items:
                if item['name'] == folderName:          # return specified folder id
                    print(f"\n{folderName} folder found in OneDrive !\n")
                    folderId = item["id"]
                    FileURL = f"{oneDriveBaseURL}/items/{folderId}/children"
                    res = requests.get(FileURL, headers = headers, timeout= 30)
                    fileItems = res.json().get('value', [])
                    
                    for item in fileItems:
                        if item['name'] == fileName:
                            return item['@microsoft.graph.downloadUrl']
            print(f"Given {fileName} not found in {folderName} folder !")         
            return None

        except requests.exceptions.RequestException as e:
            print(f"Error searching for folder/file: {str(e)}")
            return None
    
    # read dataframe from ms download link        
    def url2df(self, download_url, access_token, sheet_name=None):
        headers = {
            "Authorization": f"Bearer {access_token}"
        }
        
        try:
            response = requests.get(download_url, headers=headers, timeout=30)
                        
            return pd.read_excel(
                BytesIO(response.content),
                sheet_name=sheet_name,
                engine='openpyxl'  # Explicitly specify engine for better compatibility
            )
        except requests.exceptions.RequestException as e:
            raise Exception(f"File download failed: {str(e)}")
        except Exception as e:
            raise Exception(f"Excel parsing failed: {str(e)}")

    def read_excel_from_onedrive(self, folderName, fileName, sheet_name=None):
        """Main method to read Excel from OneDrive"""
        try:
            access_token = self.get_access_token()
            drive_id = self.get_drive_id(access_token)
            download_url = self.get_fileDownload_url(access_token,drive_id,folderName,fileName)
            print(f"\nDownload URL = {download_url}\n")
            df = self.url2df(download_url, access_token, sheet_name)
            display(df.head(5))
        except Exception as e:
            raise Exception(f"{str(e)}")

if __name__ == "__main__":
    try:
        reader = OneDriveExcelReader()
        
        # Relative path from the user's OneDrive root
        folderPath = "sku promotion"
        fileName = 'Promotion Data.xlsx'
        
        # Read the Excel file (specify sheet name if needed)
        df = reader.read_excel_from_onedrive(
            folderPath,
            fileName,
            sheet_name='potential_skus'  # Optional: specify which sheet to read
        )
        
        print("Successfully loaded Excel file:")
        print(df.head())
        
    except Exception as e:
        print(f"{str(e)}")