"""One complete backup cycle; OAuth/folder validation precedes DB extraction."""
import os
from tempfile import TemporaryDirectory

from google.auth.transport.requests import AuthorizedSession

from backup_download import download_backup
from drive_auth import user_credentials
from drive_upload import backup_name, folder_id, writable_folder, upload_backup


def backup_once(destination, classroom, token_path, client_path, dry_run=False):
    name = backup_name(classroom)
    folder = folder_id(destination)
    if dry_run:
        print(f'Dry-run: would download the v2 database and upload {name} into Drive folder {folder}; no writes performed.')
        return 0
    credentials = user_credentials(token_path, client_path)
    with AuthorizedSession(credentials, refresh_timeout=10, max_refresh_attempts=0) as session:
        writable_folder(session, folder)
        with TemporaryDirectory(prefix='ntut-exam-backup-') as directory:
            artifact = download_backup(directory, os.getenv('BACKEND_API_URL'), os.getenv('ADMIN_TOKEN'))
            upload_backup(session, artifact, folder, name)
    return 0
