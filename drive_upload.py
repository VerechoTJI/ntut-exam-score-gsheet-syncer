"""Byte-preserving Drive upload using one stable file ID across retries."""
from datetime import datetime, timedelta, timezone
import json
import re
import shutil
from tempfile import SpooledTemporaryFile
from urllib.parse import urlparse
import uuid

from urllib3.util import Timeout

from exam_config import require
import retry

API = 'https://www.googleapis.com/drive/v3/files'


def backup_name(classroom, now=None):
    require(re.fullmatch(r'[A-Za-z0-9_-]+', classroom), 'Invalid classroom label for backup filename')
    taipei = timezone(timedelta(hours=8))
    moment = datetime.now(taipei) if now is None else now.astimezone(taipei)
    return classroom + '_' + moment.strftime('%Y%m%dT%H%M%S%f%z') + '.sqlite'


def folder_id(value):
    parsed = urlparse(value)
    if parsed.scheme:
        require(parsed.scheme == 'https' and parsed.netloc == 'drive.google.com'
                and parsed.path.startswith('/drive/folders/'), 'Invalid Google Drive folder URL')
        value = parsed.path.rstrip('/').split('/')[-1]
    require(re.fullmatch(r'[A-Za-z0-9_-]+', value), 'Invalid Google Drive folder ID')
    return value


def request(session, label, method, url, allow_conflict=False, **kwargs):
    def attempt(remaining):
        body = kwargs.get('data')
        if body is not None:
            body.seek(0)
        response = session.request(method, url, timeout=Timeout(total=remaining), **kwargs)
        if not (allow_conflict and response.status_code == 409):
            response.raise_for_status()
        return response
    return retry.run(label, attempt)


def upload_backup(session, artifact, destination, name):
    folder = folder_id(destination)
    info = request(session, 'Inspect backup folder', 'GET', API+'/'+folder,
                   params={'fields':'id,mimeType,capabilities(canAddChildren)', 'supportsAllDrives':'true'}).json()
    require(info.get('mimeType') == 'application/vnd.google-apps.folder'
            and info.get('capabilities',{}).get('canAddChildren'), 'Backup folder is not writable by the OAuth user')
    ids = request(session, 'Allocate backup file ID', 'GET', API+'/generateIds',
                  params={'count':1,'space':'drive','type':'files'}).json().get('ids')
    require(isinstance(ids,list) and len(ids)==1 and isinstance(ids[0],str), 'Drive did not return a file ID')
    file_id = ids[0]
    boundary = uuid.uuid4().hex
    metadata = {'id':file_id, 'name':name, 'parents':[folder], 'mimeType':'application/vnd.sqlite3'}
    prefix = (f'--{boundary}\r\nContent-Type: application/json; charset=UTF-8\r\n\r\n'
              + json.dumps(metadata) + f'\r\n--{boundary}\r\nContent-Type: application/vnd.sqlite3\r\n\r\n').encode('utf-8')
    with SpooledTemporaryFile(max_size=1048576, mode='w+b', dir=artifact['path'].parent) as body:
        body.write(prefix)
        with artifact['path'].open('rb') as source:
            shutil.copyfileobj(source, body)
        body.write(f'\r\n--{boundary}--\r\n'.encode('ascii'))
        length = body.tell()
        request(session, 'Upload database backup', 'POST',
                'https://www.googleapis.com/upload/drive/v3/files', allow_conflict=True,
                params={'uploadType':'multipart','supportsAllDrives':'true'}, data=body,
                headers={'Content-Type':f'multipart/related; boundary={boundary}', 'Content-Length':str(length)})
    result = request(session, 'Verify database backup upload', 'GET', API+'/'+file_id,
                     params={'fields':'id,name,parents,size,md5Checksum,webViewLink','supportsAllDrives':'true'}).json()
    require(result.get('id')==file_id and result.get('name')==name
            and result.get('parents')==[folder] and str(result.get('size'))==str(artifact['size'])
            and result.get('md5Checksum')==artifact['md5'], f'Drive backup verification failed for file {file_id}')
    url = result.get('webViewLink') or f'https://drive.google.com/file/d/{file_id}/view'
    print(f'Uploaded {name}: {url}')
    return url
