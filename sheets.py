from google.oauth2.service_account import Credentials
import gspread
from google_http import GoogleSession


def get_spreadsheet(spreadsheet_url, service_account_file='credentials.json'):
    SCOPES = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file(service_account_file, scopes=SCOPES)
    session = GoogleSession(creds)
    client = gspread.authorize(creds, session=session)
    return client.open_by_url(spreadsheet_url)


def get_worksheet(spreadsheet_url, sheet_title, service_account_file='credentials.json'):
    return get_spreadsheet(spreadsheet_url, service_account_file).worksheet(sheet_title)
