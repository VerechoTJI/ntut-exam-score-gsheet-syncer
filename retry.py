"""Backend retries stop when the next exponential delay exceeds its cap."""
import os
import re
from time import sleep

import requests

from timeout_settings import seconds


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
    message = re.sub(r'(?i)Bearer\s+[^\s,\"\']+', 'Bearer [REDACTED]', message)
    return re.sub(r'''(?i)((?:access_token|refresh_token|client_secret)["']?\s*[:=]\s*["']?)[^"',\s}&]+''',
                  r'\1[REDACTED]', message)


def run(label, operation):
    maximum = seconds('BACKOFF_MAX_SECONDS', 10)
    delay = 1
    attempt = 0
    while True:
        attempt += 1
        try:
            # Backend connect/read timeout per attempt, not a total wall clock.
            return operation(10)
        except requests.RequestException as error:
            cause = describe(error)
            print(f'WARNING: {label}, attempt {attempt}: {cause}')
            if delay > maximum:
                raise OperationFailed(f'{label} stopped: next backoff {delay}s exceeds '
                                      f'BACKOFF_MAX_SECONDS={maximum:g}; {cause}') from error
            sleep(delay)
            delay *= 2
