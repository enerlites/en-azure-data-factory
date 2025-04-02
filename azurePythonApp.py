'''
Deploy below Python Script over Azure Fucntions (Serverless)


Requirements:
`pip install msal requests` --> use microsoft Graph API to connect with OneDrive
`pip install python-dotenv`
'''
import os 
from dotenv import load_dotenv
import requests
import msal         # ms Graph API

# load env vars from .env
load_dotenv()

# access env vars
CLIENT_ID, CLIENT_SECRET = os.getenv("CLIENT_ID"), os.getenv("CLIENT_SECRET")