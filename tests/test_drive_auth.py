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
    monkeypatch.setattr(drive_auth.InstalledAppFlow,'from_client_secrets_file',Mock(return_value=flow))
    assert drive_auth.user_credentials(token,client,interactive=True) is creds
    assert token.exists()
    monkeypatch.setattr(drive_auth.Credentials,'from_authorized_user_file',Mock(return_value=creds))
    assert drive_auth.user_credentials(token,client) is creds
    flow.run_local_server.assert_called_once()


def test_refresh_and_missing_authorization(monkeypatch,tmp_path):
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
    assert request.call_args.kwargs['timeout'].total==10


def test_revocation_and_missing_refresh_token(monkeypatch,tmp_path):
    token=tmp_path/'token.json';token.write_text('{}')
    creds=credentials(False)
    monkeypatch.setattr(drive_auth.Credentials,'from_authorized_user_file',Mock(return_value=creds))
    creds.refresh.side_effect=RefreshError('invalid_grant: revoked')
    with pytest.raises(drive_auth.retry.OperationFailed,match='revoked'):
        drive_auth.user_credentials(token,tmp_path/'unused')
    creds.refresh_token=None
    with pytest.raises(ValueError,match='refresh token'):
        drive_auth.user_credentials(token,tmp_path/'unused')
