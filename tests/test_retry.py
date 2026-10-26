from unittest.mock import Mock

import pytest
import requests

import retry


@pytest.mark.parametrize('maximum,waits', [('10', [1, 2, 4, 8]), ('2', [1, 2]), ('0.5', [])])
def test_backoff_cap_stops_before_oversized_wait(monkeypatch, capsys, maximum, waits):
    monkeypatch.setenv('BACKOFF_MAX_SECONDS', maximum)
    sleep = Mock()
    monkeypatch.setattr(retry, 'sleep', sleep)
    operation = Mock(side_effect=requests.ConnectionError('backend unavailable'))
    with pytest.raises(retry.OperationFailed, match=f'BACKOFF_MAX_SECONDS={maximum}'):
        retry.run('Fetch scores', operation)
    assert [call.args[0] for call in sleep.call_args_list] == waits
    assert operation.call_count == len(waits) + 1
    assert all(call.args == (10,) for call in operation.call_args_list)
    assert 'backend unavailable' in capsys.readouterr().out


def test_recovery_has_no_shared_wall_clock_budget():
    operation = Mock(side_effect=[requests.Timeout('read timed out'), 'ok'])
    assert retry.run('Download', operation) == 'ok'
    assert [call.args[0] for call in operation.call_args_list] == [10, 10]


def test_invalid_backoff_cap_fails_before_backend_request(monkeypatch):
    monkeypatch.setenv('BACKOFF_MAX_SECONDS', 'bad')
    operation = Mock()
    with pytest.raises(ValueError, match='BACKOFF_MAX_SECONDS'):
        retry.run('Fetch scores', operation)
    operation.assert_not_called()


def test_redaction(monkeypatch, capsys):
    monkeypatch.setenv('ADMIN_TOKEN', 'sensitive-test-token')
    operation = Mock(side_effect=requests.Timeout('Bearer sensitive-test-token read timeout'))
    with pytest.raises(retry.OperationFailed):
        retry.run('Scores', operation)
    assert 'sensitive-test-token' not in capsys.readouterr().out


def test_google_error_details_and_unexpected_exception():
    from gspread.exceptions import APIError
    from google.auth.exceptions import TransportError
    response = Mock(status_code=403)
    response.json.return_value = {'error': {'code': 403, 'message': 'Permission denied', 'status': 'PERMISSION_DENIED'}}
    assert 'Permission denied' in retry.describe(APIError(response))
    for error in (ValueError('bad input'), TransportError('Google authentication failed')):
        operation = Mock(side_effect=error)
        with pytest.raises(type(error)):
            retry.run('operation', operation)
        operation.assert_called_once()


def test_nonjson_http_error_has_status():
    response = Mock(status_code=502)
    response.json.side_effect = ValueError('not JSON')
    error = requests.HTTPError('Bad Gateway', response=response)
    assert 'HTTP 502' in retry.describe(error)


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


@pytest.mark.parametrize('error', [requests.HTTPError('503 Service Unavailable'),
    requests.exceptions.JSONDecodeError('invalid JSON', '', 0)])
def test_backend_recoverable_failure_classes(error):
    operation = Mock(side_effect=[error, 'ok'])
    assert retry.run('Backend request', operation) == 'ok'
    assert operation.call_count == 2


def test_oauth_secrets_are_redacted():
    message = retry.describe(RuntimeError('access_token=access123 refresh_token="refresh123" client_secret: secret123'))
    assert 'access123' not in message and 'refresh123' not in message and 'secret123' not in message
