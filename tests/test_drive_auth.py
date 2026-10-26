from unittest.mock import Mock

import pytest
from google.auth.exceptions import RefreshError

import drive_auth


def credentials(valid=True):
    return Mock(valid=valid, refresh_token='synthetic-refresh', to_json=lambda:'{"token":"synthetic"}')


def test_explicit_authorization_and_cached_credentials(monkeypatch,tmp_path):
    client=tmp_path/'client.json'; client.write_text('{}')
    token=tmp_path/'token.json'
    creds=credentials()
    flow=Mock()
    flow.run_local_server.return_value=creds
    monkeypatch.setattr(drive_auth.TimedInstalledAppFlow,'from_client_secrets_file',Mock(return_value=flow))
    assert drive_auth.user_credentials(token,client,interactive=True) is creds
    assert token.exists()
    monkeypatch.setattr(drive_auth.Credentials,'from_authorized_user_file',Mock(return_value=creds))
    assert drive_auth.user_credentials(token,client) is creds
    flow.run_local_server.assert_called_once()


def test_refresh_and_missing_authorization(monkeypatch,tmp_path):
    monkeypatch.setenv('GOOGLE_TIMEOUT_SECONDS', '45')
    token=tmp_path/'token.json'
    with pytest.raises(ValueError,match='authorize-drive'):
        drive_auth.user_credentials(token,tmp_path/'missing.json')
    with pytest.raises(ValueError,match='client'):
        drive_auth.user_credentials(token,tmp_path/'missing.json',interactive=True)
    token.write_text('{}')
    creds=credentials(False)
    request=Mock()
    monkeypatch.setattr(drive_auth,'Request',lambda:request)
    def refresh(transport):
        transport('https://oauth.test',method='POST')
        creds.valid=True
    creds.refresh.side_effect=refresh
    monkeypatch.setattr(drive_auth.Credentials,'from_authorized_user_file',Mock(return_value=creds))
    assert drive_auth.user_credentials(token,tmp_path/'unused') is creds
    assert request.call_args.kwargs['timeout']==45
    creds.refresh.assert_called_once()


def test_revocation_and_missing_refresh_token(monkeypatch,tmp_path):
    token=tmp_path/'token.json';token.write_text('{}')
    creds=credentials(False)
    monkeypatch.setattr(drive_auth.Credentials,'from_authorized_user_file',Mock(return_value=creds))
    creds.refresh.side_effect=RefreshError('invalid_grant: revoked')
    with pytest.raises(RefreshError,match='revoked'):
        drive_auth.user_credentials(token,tmp_path/'unused')
    creds.refresh.assert_called_once()
    creds.refresh_token=None
    with pytest.raises(ValueError,match='refresh token'):
        drive_auth.user_credentials(token,tmp_path/'unused')


def test_browser_token_exchange_uses_configured_http_timeout(monkeypatch):
    monkeypatch.setenv('GOOGLE_TIMEOUT_SECONDS', '23')
    fetch = Mock(return_value={'access_token': 'synthetic'})
    monkeypatch.setattr(drive_auth.InstalledAppFlow, 'fetch_token', fetch)
    flow = drive_auth.TimedInstalledAppFlow.__new__(drive_auth.TimedInstalledAppFlow)
    assert flow.fetch_token(authorization_response='https://callback.test') == {'access_token': 'synthetic'}
    fetch.assert_called_once_with(authorization_response='https://callback.test', timeout=23)


def test_actual_oauth_flow_passes_timeout_to_sdk_http_boundary(monkeypatch):
    monkeypatch.setenv('GOOGLE_TIMEOUT_SECONDS', '17')
    flow = drive_auth.TimedInstalledAppFlow.from_client_config({'installed': {
        'client_id': 'synthetic', 'client_secret': 'synthetic',
        'auth_uri': 'https://auth.test/authorize', 'token_uri': 'https://auth.test/token',
        'redirect_uris': ['http://localhost']}}, drive_auth.SCOPES)
    fetch = Mock(return_value={'access_token': 'synthetic'})
    monkeypatch.setattr(flow.oauth2session, 'fetch_token', fetch)
    flow.fetch_token(code='synthetic-code')
    assert fetch.call_args.args == ('https://auth.test/token',)
    assert fetch.call_args.kwargs['timeout'] == 17


def test_invalid_google_timeout_fails_before_browser_consent(monkeypatch,tmp_path):
    monkeypatch.setenv('GOOGLE_TIMEOUT_SECONDS', 'bad')
    start = Mock()
    monkeypatch.setattr(drive_auth.TimedInstalledAppFlow, 'from_client_secrets_file', start)
    with pytest.raises(ValueError, match='GOOGLE_TIMEOUT_SECONDS'):
        drive_auth.user_credentials(tmp_path/'token',tmp_path/'client',interactive=True)
    start.assert_not_called()
