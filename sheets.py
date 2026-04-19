import os
from google.oauth2.service_account import Credentials
import gspread


def get_worksheet(
    spreadsheet_url, sheet_title, service_account_file="credentials.json"
):
    SCOPES = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file(service_account_file, scopes=SCOPES)
    client = gspread.authorize(creds)
    spreadsheet = client.open_by_url(spreadsheet_url)
    worksheet = spreadsheet.worksheet(sheet_title)
    return worksheet
