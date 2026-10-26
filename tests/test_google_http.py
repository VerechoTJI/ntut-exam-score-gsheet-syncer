from io import BytesIO
from unittest.mock import Mock

import pytest
import requests

from google_http import GoogleSession


def test_sdk_refresh_replays_full_body_with_configured_timeout(monkeypatch):
    monkeypatch.setenv('GOOGLE_TIMEOUT_SECONDS', '42')
    sent = []
    def send(session, method, url, data=None, **kwargs):
        sent.append((data.read(), kwargs['timeout']))
        response = requests.Response()
        response.status_code = 401 if len(sent) == 1 else 200
        response._content = b'{}'
        return response
    monkeypatch.setattr(requests.Session, 'request', send)
    credentials = Mock()
    with GoogleSession(credentials) as session:
        assert session._refresh_timeout == 42
        assert session.request('POST', 'https://google.test', data=BytesIO(b'whole-body')).status_code == 200
    credentials.refresh.assert_called_once()
    assert sent == [(b'whole-body', 42), (b'whole-body', 42)]


def test_google_transport_errors_are_not_custom_retried(monkeypatch):
    send = Mock(side_effect=requests.Timeout('slow Google response'))
    monkeypatch.setattr(requests.Session, 'request', send)
    with GoogleSession(Mock()) as session:
        with pytest.raises(requests.Timeout, match='slow Google response'):
            session.request('GET', 'https://google.test')
    assert send.call_count == 1
    assert send.call_args.kwargs['timeout'] == 30
