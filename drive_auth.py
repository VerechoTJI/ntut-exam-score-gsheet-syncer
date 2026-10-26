"""Explicit desktop consent, then unattended Drive token refresh."""
from pathlib import Path
from functools import partial

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow

from exam_config import require
from timeout_settings import seconds

SCOPES = ['https://www.googleapis.com/auth/drive']


class TimedInstalledAppFlow(InstalledAppFlow):
    def fetch_token(self, **kwargs):
        # Limit token HTTP requests, not the user's browser consent time.
        kwargs['timeout'] = seconds('GOOGLE_TIMEOUT_SECONDS', 30)
        return super().fetch_token(**kwargs)


def save_token(credentials, path):
    path.parent.mkdir(parents=True, exist_ok=True)
    pending = path.with_suffix('.tmp')
    pending.write_text(credentials.to_json(), encoding='utf-8')
    pending.replace(path)


def user_credentials(token_path='drive_token.json', client_path='drive_oauth_client.json', interactive=False):
    timeout = seconds('GOOGLE_TIMEOUT_SECONDS', 30)
    token = Path(token_path)
    if interactive:
        require(Path(client_path).is_file(), 'Google Desktop OAuth client JSON required; provide --oauth-client')
        flow = TimedInstalledAppFlow.from_client_secrets_file(str(client_path), SCOPES)
        credentials = flow.run_local_server(port=0, access_type='offline', prompt='consent')
        require(credentials.refresh_token, 'OAuth consent did not provide a refresh token')
        save_token(credentials, token)
    else:
        require(token.is_file(), 'Missing Drive user credentials; run --authorize-drive with --oauth-client first')
        credentials = Credentials.from_authorized_user_file(str(token), SCOPES)
        require(credentials.refresh_token, 'OAuth credentials lack a refresh token; run --authorize-drive')
        require(credentials.has_scopes(SCOPES), 'OAuth credentials lack Drive permission; run --authorize-drive')
        if not credentials.valid:
            credentials.refresh(partial(Request(), timeout=timeout))
            save_token(credentials, token)
    return credentials
