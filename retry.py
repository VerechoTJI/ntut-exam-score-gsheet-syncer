"""One bounded retry policy for external operations; no nested retry loops."""
import os
import re
from time import monotonic, sleep

from google.auth.exceptions import GoogleAuthError
from gspread.exceptions import APIError
import requests


class OperationFailed(RuntimeError):
    pass


def describe(error):
    message = f'{type(error).__name__}: {error}'
    response = getattr(error, 'response', None)
    if response is not None:
        message = f'HTTP {response.status_code}: {message}'
        try:
            body = response.json()
            message += f'; provider: {body["error"]}'
            if 'code' in body:
                message += f'; code: {body["code"]}'
        except (ValueError, KeyError, TypeError):
            pass
    token = os.getenv('ADMIN_TOKEN')
    if token:
        message = message.replace(token, '[REDACTED]')
    return re.sub(r'(?i)Bearer\s+[^\s,\"\']+', 'Bearer [REDACTED]', message)


def run(label, operation):
    deadline = monotonic() + 10
    delay = 1
    attempt = 0
    cause = 'deadline exhausted'
    while monotonic() < deadline:
        remaining = deadline - monotonic()
        attempt += 1
        try:
            result = operation(remaining)
            if monotonic() >= deadline:
                raise requests.Timeout('10-second operation deadline exceeded')
            return result
        except (requests.RequestException, APIError, GoogleAuthError) as error:
            cause = describe(error)
            print(f'WARNING: {label}, attempt {attempt}: {cause}')
            remaining = deadline - monotonic()
            if remaining <= 0:
                break
            sleep(min(delay, remaining))
            delay *= 2
    raise OperationFailed(f'{label} stopped after 10 seconds: {cause}')
