import sqlite3
from contextlib import closing
from unittest.mock import MagicMock, Mock

import pytest
import requests

import backup_download


def database_bytes(tmp_path):
    path = tmp_path/'fixture.sqlite'
    with closing(sqlite3.connect(path)) as db:
        db.execute('CREATE TABLE example (value TEXT)')
        db.execute("INSERT INTO example VALUES ('synthetic')")
        db.commit()
    return path.read_bytes()


def response_for(data, headers=None):
    response = MagicMock(ok=True)
    response.__enter__.return_value = response
    response.headers = headers if headers is not None else {'Content-Type':'application/vnd.sqlite3', 'Content-Length':str(len(data))}
    response.iter_content.return_value = iter([b'',data])
    return response


def test_download_preserves_original_bytes_and_checksum(monkeypatch,tmp_path):
    data=database_bytes(tmp_path)
    get=Mock(return_value=response_for(data))
    monkeypatch.setattr(backup_download.requests,'get',get)
    artifact=backup_download.download_backup(tmp_path,'http://backend/','secret-test')
    assert artifact['path'].read_bytes()==data
    assert artifact['size']==len(data) and len(artifact['md5'])==32
    assert get.call_args.args[0]=='http://backend/admin/backup'
    assert get.call_args.kwargs['headers']['Authorization']=='Bearer secret-test'


@pytest.mark.parametrize('kind',['empty','html','truncated','corrupt','length','type'])
def test_invalid_downloads_are_not_retained(monkeypatch,tmp_path,kind):
    data=database_bytes(tmp_path)
    headers={'Content-Type':'application/vnd.sqlite3','Content-Length':str(len(data))}
    if kind=='empty': data=b'';headers['Content-Length']='0'
    elif kind=='html': data=b'<html>error';headers['Content-Length']=str(len(data))
    elif kind=='truncated': data=data[:100]
    elif kind=='corrupt': data=b'SQLite format 3\x00'+b'x'*100;headers['Content-Length']=str(len(data))
    elif kind=='length': headers['Content-Length']='not-a-number'
    else: headers['Content-Type']='application/json'
    monkeypatch.setattr(backup_download.requests,'get',lambda *a,**k:response_for(data,headers))
    with pytest.raises((ValueError,sqlite3.DatabaseError,backup_download.retry.OperationFailed)):
        backup_download.download_backup(tmp_path,'http://backend','token')
    assert not (tmp_path/'snapshot.sqlite').exists()


def test_stream_deadline_and_http_causes(monkeypatch,tmp_path,capsys):
    data=database_bytes(tmp_path)
    response=response_for(data)
    now=[0.0]
    monkeypatch.setattr(backup_download.retry,'monotonic',lambda:now[0])
    monkeypatch.setattr(backup_download.retry,'sleep',lambda delay:now.__setitem__(0,now[0]+delay))
    def chunks(size):
        now[0]+=11
        yield data
    response.iter_content.side_effect=chunks
    monkeypatch.setattr(backup_download.requests,'get',lambda *a,**k:response)
    with pytest.raises(backup_download.retry.OperationFailed,match='deadline'):
        backup_download.download_backup(tmp_path,'http://backend','token')
    response=response_for(data)
    response.ok=False
    response.status_code=503
    response.json.return_value={'error':'Unavailable','code':'BACKUP_UNAVAILABLE'}
    response.raise_for_status.side_effect=requests.HTTPError('503',response=response)
    monkeypatch.setattr(backup_download.requests,'get',lambda *a,**k:response)
    with pytest.raises(backup_download.retry.OperationFailed):
        backup_download.download_backup(tmp_path,'http://backend','token')
    assert 'BACKUP_UNAVAILABLE' in capsys.readouterr().out


def test_missing_settings_and_optional_content_length(monkeypatch,tmp_path):
    with pytest.raises(ValueError):
        backup_download.download_backup(tmp_path,'','')
    data=database_bytes(tmp_path)
    monkeypatch.setattr(backup_download.requests,'get',lambda *a,**k:response_for(data,{'Content-Type':'application/vnd.sqlite3'}))
    assert backup_download.download_backup(tmp_path,'http://backend','token')['size']==len(data)


def test_late_integrity_check_cleans_rejected_artifact(monkeypatch,tmp_path):
    data=database_bytes(tmp_path)
    now=[0.0]
    monkeypatch.setattr(backup_download.retry,'monotonic',lambda:now[0])
    connect=backup_download.sqlite3.connect
    def slow_connect(*args,**kwargs):
        now[0]+=11
        return connect(*args,**kwargs)
    monkeypatch.setattr(backup_download.sqlite3,'connect',slow_connect)
    monkeypatch.setattr(backup_download.requests,'get',lambda *a,**k:response_for(data))
    with pytest.raises(backup_download.retry.OperationFailed):
        backup_download.download_backup(tmp_path,'http://backend','token')
    assert not (tmp_path/'snapshot.sqlite').exists()
