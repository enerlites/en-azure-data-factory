import os
import msal
import requests
import pandas as pd
from io import BytesIO
from dotenv import load_dotenv
from urllib.parse import quote
from IPython import display

class SharePointExcelReader:
    def __init__(self):
        load_dotenv()
        self.client_id = os.getenv("AZ_CLI_ID")
        self.client_secret = os.getenv("AZ_CLI_SECRET")
        self.tenant_id = os.getenv("AZ_TENANT_ID")
        self.base_graph_url = "https://graph.microsoft.com/v1.0"
    
    def get_access_token(self):
        authority = f"https://login.microsoftonline.com/{self.tenant_id}"
        app = msal.ConfidentialClientApplication(
            self.client_id,
            authority=authority,
            client_credential=self.client_secret
        )

        # Note: Added SharePoint permissions
        result = app.acquire_token_for_client(scopes=[
            "https://graph.microsoft.com/.default"
        ])

        if "access_token" in result:
            return result["access_token"]
        else:
            error_details = result.get("error_description", "No error description provided")
            raise Exception(f"Authentication failed: {result.get('error')} - {error_details}")

    def get_site_id(self, access_token, site_name):
        """
        Get the SharePoint site ID by site name
        Format for site_name: 'yourcompany.sharepoint.com:/sites/yoursitename'
        or just 'yoursitename' for modern team sites
        """
        try:
            response = requests.get(
                f"{self.base_graph_url}/sites/{site_name}",
                headers={"Authorization": f"Bearer {access_token}"},
                timeout=30
            )
            response.raise_for_status()
            return response.json()["id"]
        except requests.exceptions.RequestException as e:
            raise Exception(f"Failed to get site ID: {str(e)}")

    def get_drive_id(self, access_token, site_id, document_library="Documents"):
        """
        Get the default document library (drive) ID for a SharePoint site
        document_library: Typically "Documents" for the default library
        """
        try:
            response = requests.get(
                f"{self.base_graph_url}/sites/{site_id}/drives",
                headers={"Authorization": f"Bearer {access_token}"},
                timeout=30
            )
            response.raise_for_status()
            
            drives = response.json().get('value', [])
            for drive in drives:
                if drive['name'] == document_library:
                    return drive['id']
            
            raise Exception(f"Document library '{document_library}' not found")
        except requests.exceptions.RequestException as e:
            raise Exception(f"Failed to get drive ID: {str(e)}")

    def get_file_id(self, access_token, drive_id, folder_path, file_name):
        """
        Get file ID from a SharePoint folder
        folder_path: Relative path from the document library root (e.g., "Shared Documents/Reports")
        """
        try:
            # Properly encode the path
            encoded_path = quote(f":/{folder_path.strip('/')}:")
            url = f"{self.base_graph_url}/drives/{drive_id}/root{encoded_path}/children"
            
            headers = {"Authorization": f"Bearer {access_token}"}
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            
            items = response.json().get('value', [])
            for item in items:
                if item['name'].lower() == file_name.lower():
                    return item['id']
            
            return None
        except requests.exceptions.RequestException as e:
            raise Exception(f"Failed to search folder: {str(e)} - URL: {url}")

    def download_file(self, access_token, drive_id, file_id):
        """Download file content from SharePoint"""
        try:
            url = f"{self.base_graph_url}/drives/{drive_id}/items/{file_id}/content"
            headers = {"Authorization": f"Bearer {access_token}"}
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            return response.content
        except requests.exceptions.RequestException as e:
            raise Exception(f"File download failed: {str(e)}")

    def read_excel_from_sharepoint(self, site_name, folder_path, file_name, sheet_name=None, document_library="Documents"):
        """Main method to read Excel from SharePoint"""
        try:
            access_token = self.get_access_token()
            site_id = self.get_site_id(access_token, site_name)
            drive_id = self.get_drive_id(access_token, site_id, document_library)
            file_id = self.get_file_id(access_token, drive_id, folder_path, file_name)
            
            if not file_id:
                raise Exception(f"File '{file_name}' not found in '{folder_path}'")
            
            file_content = self.download_file(access_token, drive_id, file_id)
            return pd.read_excel(BytesIO(file_content), sheet_name=sheet_name, engine='openpyxl')
            
        except Exception as e:
            raise Exception(f"Failed to read Excel from SharePoint: {str(e)}")
        
def main():
    try:
        # Initialize the reader
        reader = SharePointExcelReader()
        
        # SharePoint configuration
        site_name = "topgreener.sharepoint.com:/sites/andrew_chen_enerlites_com"  # or just "your-site-name"
        folder_path = "Documents/sku promotion"  # Relative to document library
        file_name = "Promotion Data.xlsx"
        sheet_name = "potential_skus"  # Optional
        
        # Read the Excel file
        df = reader.read_excel_from_sharepoint(
            site_name=site_name,
            folder_path=folder_path,
            file_name=file_name,
            sheet_name=sheet_name,
            document_library="Documents"  # Default is "Documents"
        )
        
        print("Successfully loaded Excel file from SharePoint:")
        print(df.head())
        return df
        
    except Exception as e:
        print(f"Error: {str(e)}")
        return None

if __name__ == "__main__":
    main()