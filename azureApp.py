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
            print(f"\naccess token = {result["access_token"]}\n")
            return result["access_token"]
        else:
            error_details = result.get("error_description", "No error description provided")
            raise Exception(f"Authentication failed: {result.get('error')} - {error_details}")

    # Get the personal drive id under org OneDrive
    def get_drive_id(self, access_token):
        response = requests.get(
            f"{self.base_graph_url}/users/{self.user_principal}/drive",
            headers={"Authorization": f"Bearer {access_token}"},
            timeout=30
        )
        
        if response.status_code != 200:
            raise Exception(f"Failed to get drive info: {response.status_code} - {response.text}")
        
        print(f"\ndriver id = {response.json()["id"]}\n")
        
        return response.json()["id"]
    
    # Get file id from given folder 
    def get_file_id(self, access_token, driver_id, folderPath, fileName):
        encoded_path = quote(folderPath.strip('/'))
        url = f"{self.base_graph_url}/drives/{driver_id}/root:/{encoded_path}:/children"
        headers = {"Authorization": f"Bearer {access_token}"}
        
        try:
            res = requests.get(url, headers= headers, timeout= 30)
            res.raise_for_status()
            
            items = res.json().get('value', [])
            print(f"Items inside of the folder: {items}")
            for item in items:
                if item['name'].lower() == fileName.lower():
                    print(f"\nfile id = {item['id']}\n")
                    return item['id']
            
            # file not found
            return None
            
        except requests.exceptions.RequestException as e:
            raise Exception(f"Failed to search folder: {str(e)}")

    # get download url based on drive_id & file_id
    def get_download_url(self, drive_id, file_id):
        if not file_id:     # file not found 
            raise Exception (f"Error: File not found in OneDrive Folder !")
        
        return f"{self.base_graph_url}/drives/{drive_id}/items/{file_id}/content"
            
    def url2pd(self, download_url, access_token, sheet_name=None):
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/octet-stream"  # More appropriate for file download
        }
        
        try:
            response = requests.get(download_url, headers=headers, timeout=30)
            response.raise_for_status()  # Will raise HTTPError for 4XX/5XX status codes
            
            return pd.read_excel(
                BytesIO(response.content),
                sheet_name=sheet_name,
                engine='openpyxl'  # Explicitly specify engine for better compatibility
            )
        except requests.exceptions.RequestException as e:
            raise Exception(f"File download failed: {str(e)}")
        except Exception as e:
            raise Exception(f"Excel parsing failed: {str(e)}")

    def read_excel_from_onedrive(self, folderPath, fileName, sheet_name=None):
        """Main method to read Excel from OneDrive"""
        try:
            access_token = self.get_access_token()
            drive_id = self.get_drive_id(access_token)
            file_id = self.get_file_id(access_token, drive_id, folderPath,fileName)
            download_url = self.get_download_url(drive_id, file_id)
            print(download_url)
            # df = self.url2pd(download_url, access_token, sheet_name)
            # display(df.head(5))
        except Exception as e:
            raise Exception(f"{str(e)}")

def main():
    try:
        reader = OneDriveExcelReader()
        
        # Relative path from the user's OneDrive root
        folderPath = "Documents/sku promotion"
        fileName = 'Promotion Data.xlsx'
        
        # Read the Excel file (specify sheet name if needed)
        df = reader.read_excel_from_onedrive(
            folderPath,
            fileName,
            sheet_name='potential_skus'  # Optional: specify which sheet to read
        )
        
        print("Successfully loaded Excel file:")
        print(df.head())
        return df
        
    except Exception as e:
        print(f"{str(e)}")
        return None

if __name__ == "__main__":
    main()