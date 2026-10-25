from unittest.mock import Mock

import pytest
import requests

import retry


def test_backoff_has_total_deadline_and_preserves_cause(monkeypatch, capsys):
    now = [0.0]
    waits = []
    def sleep(seconds):
        waits.append(seconds)
        now[0] += seconds
    monkeypatch.setattr(retry, 'monotonic', lambda: now[0])
    monkeypatch.setattr(retry, 'sleep', sleep)
    operation = Mock(side_effect=requests.ConnectionError('connection refused'))
    with pytest.raises(retry.OperationFailed, match='connection refused'):
        retry.run('Fetch scores', operation)
    assert waits == [1, 2, 4, 3]
    assert [call.args[0] for call in operation.call_args_list] == [10, 9, 7, 3]
    assert 'connection refused' in capsys.readouterr().out


def test_recovery_and_request_time_count_toward_budget(monkeypatch):
    now = [0.0]
    monkeypatch.setattr(retry, 'monotonic', lambda: now[0])
    monkeypatch.setattr(retry, 'sleep', lambda delay: now.__setitem__(0, now[0] + delay))
    attempts = []
    def operation(remaining):
        attempts.append(remaining)
        now[0] += 4
        if len(attempts) == 1:
            raise requests.Timeout('read timed out')
        return 'ok'
    assert retry.run('Download', operation) == 'ok'
    assert attempts == [10, 5]


def test_deadline_and_redaction(monkeypatch, capsys):
    now = [0.0]
    monkeypatch.setattr(retry, 'monotonic', lambda: now[0])
    monkeypatch.setenv('ADMIN_TOKEN', 'sensitive-test-token')
    def operation(remaining):
        now[0] += 10
        raise requests.Timeout('Bearer sensitive-test-token read timeout')
    with pytest.raises(retry.OperationFailed):
        retry.run('Scores', operation)
    assert 'sensitive-test-token' not in capsys.readouterr().out


def test_google_error_details_and_unexpected_exception(monkeypatch):
    from gspread.exceptions import APIError
    response = Mock(status_code=403)
    response.json.return_value = {'error': {'code': 403, 'message': 'Permission denied', 'status': 'PERMISSION_DENIED'}}
    assert 'Permission denied' in retry.describe(APIError(response))
    with pytest.raises(ValueError, match='bad input'):
        retry.run('operation', Mock(side_effect=ValueError('bad input')))


def test_late_success_is_failure(monkeypatch):
    now = [0.0]
    monkeypatch.setattr(retry, 'monotonic', lambda: now[0])
    def late(remaining):
        now[0] += 11
        return 'late'
    with pytest.raises(retry.OperationFailed, match='deadline exceeded'):
        retry.run('late operation', late)


def test_nonjson_http_error_has_status():
    response = Mock(status_code=502)
    response.json.side_effect = ValueError('not JSON')
    error = requests.HTTPError('Bad Gateway', response=response)
    assert 'HTTP 502' in retry.describe(error)


def test_google_http_client_uses_deadline_timeouts():
    import sheets
    session = Mock()
    session.request.side_effect = [requests.Timeout('slow'), Mock(ok=True)]
    client = sheets.BoundedHTTPClient(Mock(), session=session)
    # Real gspread request implementation against a fake HTTP boundary.
    result = client.request('get', 'https://example.test')
    assert result.ok
    assert [call.kwargs['timeout'].total for call in session.request.call_args_list] == [10, 9]


def test_cli_stops_periodic_run_and_reports_exceptions(monkeypatch, capsys):
    import main
    monkeypatch.setenv('INTERVAL', '5')
    operation = Mock(return_value=1)
    monkeypatch.setattr(main, 'sync_once', operation)
    assert main.cli([]) == 1
    operation.assert_called_once()
    monkeypatch.setattr(main, 'sync_once', Mock(side_effect=RuntimeError('credentials missing')))
    assert main.cli([]) == 1
    assert 'RuntimeError: credentials missing' in capsys.readouterr().out


@pytest.mark.parametrize('failure', ['http', 'json', 'auth'])
def test_recoverable_failure_classes(failure):
    from google.auth.exceptions import TransportError
    errors = {'http': requests.HTTPError('503 Service Unavailable'),
              'json': requests.exceptions.JSONDecodeError('invalid JSON', '', 0),
              'auth': TransportError('token endpoint unavailable')}
    operation = Mock(side_effect=[errors[failure], 'ok'])
    assert retry.run('External request', operation) == 'ok'
    assert operation.call_count == 2


def test_oauth_secrets_are_redacted():
    message=retry.describe(RuntimeError('access_token=access123 refresh_token="refresh123" client_secret: secret123'))
    assert 'access123' not in message and 'refresh123' not in message and 'secret123' not in message
