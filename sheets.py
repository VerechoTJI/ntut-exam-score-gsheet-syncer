import os
from google.oauth2.service_account import Credentials
import gspread
from gspread.http_client import HTTPClient
from google.auth.transport.requests import AuthorizedSession
from retry import run
from urllib3.util import Timeout


class BoundedHTTPClient(HTTPClient):
    def request(self, *args, **kwargs):
        def attempt(remaining):
            self.set_timeout(Timeout(total=remaining))
            return super(BoundedHTTPClient, self).request(*args, **kwargs)
        return run('Google Sheets request', attempt)


def get_worksheet(
    spreadsheet_url, sheet_title, service_account_file="credentials.json"
):
    SCOPES = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file(service_account_file, scopes=SCOPES)
    session = AuthorizedSession(creds, refresh_timeout=10, max_refresh_attempts=0)
    client = gspread.authorize(creds, http_client=BoundedHTTPClient, session=session)
    spreadsheet = client.open_by_url(spreadsheet_url)
    worksheet = spreadsheet.worksheet(sheet_title)
    return worksheet
