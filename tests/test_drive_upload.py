from datetime import datetime, timezone
import hashlib
from unittest.mock import Mock

import pytest
import requests

import drive_upload


@pytest.mark.parametrize('destination', [
    '1DuyLIGc-eQY4lU9WOt3ziJ6tAlcFIuNq',
    'https://drive.google.com/drive/folders/1DuyLIGc-eQY4lU9WOt3ziJ6tAlcFIuNq',
    'https://drive.google.com/drive/u/5/folders/1DuyLIGc-eQY4lU9WOt3ziJ6tAlcFIuNq',
    'https://drive.google.com/drive/u/0/folders/1DuyLIGc-eQY4lU9WOt3ziJ6tAlcFIuNq/?usp=sharing#view',
    'https://drive.google.com/drive/u/12/folders/1DuyLIGc-eQY4lU9WOt3ziJ6tAlcFIuNq?usp=sharing',
])
def test_folder_id_accepts_account_scoped_urls(destination):
    assert drive_upload.folder_id(destination) == '1DuyLIGc-eQY4lU9WOt3ziJ6tAlcFIuNq'


@pytest.mark.parametrize('destination', [
    'http://drive.google.com/drive/u/5/folders/folder',
    'https://drive.google.com.evil.test/drive/u/5/folders/folder',
    'https://drive.google.com@evil.test/drive/u/5/folders/folder',
    'https://drive.google.com/drive/u/account/folders/folder',
    'https://drive.google.com/drive/u/-1/folders/folder',
    'https://drive.google.com/drive/u//folders/folder',
    'https://drive.google.com/drive/u/5/folders/',
    'https://drive.google.com/drive/folders/folder/child',
])
def test_folder_id_rejects_invalid_account_urls(destination):
    with pytest.raises(ValueError):
        drive_upload.folder_id(destination)


def response(data,status=200):
    r=Mock(status_code=status)
    r.json.return_value=data
    if status>=400:
        r.raise_for_status.side_effect=requests.HTTPError(str(status),response=r)
    return r


def session_for(artifact,name,post_fail=False,metadata_change=None):
    session=Mock()
    posted=[]
    metadata={'id':'file123','name':name,'parents':['folder123'],
              'size':str(artifact['size']),'md5Checksum':artifact['md5']}
    metadata.update(metadata_change or {})
    def request(method,url,**kwargs):
        if 'generateIds' in url:return response({'ids':['file123']})
        if method=='POST':
            posted.append(kwargs['data'].read())
            if post_fail and len(posted)==1:raise requests.ConnectionError('response lost')
            return response({},409 if post_fail else 200)
        if url.endswith('/folder123'):
            return response({'mimeType':'application/vnd.google-apps.folder','capabilities':{'canAddChildren':True}})
        return response(metadata)
    session.request.side_effect=request
    return session,posted


@pytest.mark.parametrize('lost',[False,True])
def test_upload_is_byte_exact_and_reuses_file_id(tmp_path,lost):
    path=tmp_path/'snapshot.sqlite';path.write_bytes(b'synthetic SQLite bytes')
    artifact={'path':path,'size':path.stat().st_size,'md5':hashlib.md5(path.read_bytes()).hexdigest()}
    name='1222_timestamp.sqlite'
    session,posted=session_for(artifact,name,lost)
    result=drive_upload.upload_backup(session,artifact,'https://drive.google.com/drive/folders/folder123?usp=sharing',name)
    assert result=='https://drive.google.com/file/d/file123/view'
    assert all(path.read_bytes() in body and b'"id": "file123"' in body for body in posted)
    assert len(set(posted))==1


@pytest.mark.parametrize('change',[{'size':'0'},{'md5Checksum':'wrong'},{'parents':['other']},{'name':'other'},{'id':'other'}])
def test_metadata_mismatch_is_not_success(tmp_path,change):
    path=tmp_path/'backup';path.write_bytes(b'data')
    artifact={'path':path,'size':4,'md5':hashlib.md5(b'data').hexdigest()}
    session,_=session_for(artifact,'name.sqlite',metadata_change=change)
    with pytest.raises(ValueError,match='verification'):
        drive_upload.upload_backup(session,artifact,'folder123','name.sqlite')


def test_folder_access_names_and_invalid_ids(tmp_path):
    assert drive_upload.backup_name('1222',datetime(2026,10,26,12,0,tzinfo=timezone.utc)).startswith('1222_20261026T200000')
    assert drive_upload.backup_name('1222').endswith('+0800.sqlite')
    with pytest.raises(ValueError):drive_upload.backup_name('../bad')
    with pytest.raises(ValueError):drive_upload.folder_id('https://evil.test/folder')
    session=Mock()
    session.request.return_value=response({'mimeType':'application/vnd.google-apps.folder','capabilities':{'canAddChildren':False}})
    with pytest.raises(ValueError,match='writable'):
        drive_upload.upload_backup(session,{},'folder123','name.sqlite')


def test_http_failure_and_invalid_generated_ids(tmp_path):
    session=Mock()
    session.request.return_value=response({'error':{'message':'permission denied'}},403)
    with pytest.raises(drive_upload.retry.OperationFailed,match='permission denied'):
        drive_upload.upload_backup(session,{},'folder123','name.sqlite')
    session.request.side_effect=[response({'mimeType':'application/vnd.google-apps.folder','capabilities':{'canAddChildren':True}}),response({'ids':[]})]
    with pytest.raises(ValueError,match='file ID'):
        drive_upload.upload_backup(session,{},'folder123','name.sqlite')
