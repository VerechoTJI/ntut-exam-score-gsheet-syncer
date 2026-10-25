"""Download the backend's binary SQLite snapshot without modifying its bytes."""
import hashlib
from contextlib import closing
from pathlib import Path
import sqlite3

import requests
from urllib3.util import Timeout

from exam_config import require
import retry


def download_backup(directory, api_url, token):
    require(api_url and token, 'BACKEND_API_URL and ADMIN_TOKEN are required for database backup')
    destination = Path(directory)/'snapshot.sqlite'
    def attempt(remaining):
        deadline = retry.monotonic()+remaining
        try:
            with requests.get(api_url.rstrip('/')+'/admin/backup',
                              headers={'Authorization':f'Bearer {token}'},
                              stream=True, timeout=Timeout(total=remaining)) as response:
                if not response.ok:
                    # Buffer the error before closing the streamed response.
                    response.content
                response.raise_for_status()
                require(response.headers.get('Content-Type','').split(';')[0] in
                        ('application/vnd.sqlite3','application/octet-stream'),
                        'Backup response is not a SQLite file')
                digest = hashlib.md5()
                size = 0
                with destination.open('wb') as output:
                    for chunk in response.iter_content(65536):
                        if retry.monotonic() >= deadline:
                            raise requests.Timeout('Database download deadline exceeded')
                        if chunk:
                            output.write(chunk)
                            digest.update(chunk)
                            size += len(chunk)
                length = response.headers.get('Content-Length')
                if length is not None and int(length) != size:
                    raise requests.ConnectionError(f'Incomplete database download: expected {length} bytes, received {size}')
            require(size > 0, 'Database backup is empty')
            with destination.open('rb') as source:
                require(source.read(16) == b'SQLite format 3\x00', 'Invalid SQLite file header')
            with closing(sqlite3.connect(destination.resolve().as_uri()+'?mode=ro', uri=True)) as database:
                require(database.execute('PRAGMA quick_check').fetchall() == [('ok',)], 'SQLite integrity check failed')
            return {'path':destination, 'size':size, 'md5':digest.hexdigest()}
        except Exception:
            destination.unlink(missing_ok=True)
            raise
    try:
        return retry.run('Download database backup', attempt)
    except Exception:
        destination.unlink(missing_ok=True)
        raise
