from unittest.mock import MagicMock, Mock

import pytest

import backup_service
import scheduler


def test_independent_timers_and_no_overlap(monkeypatch):
    now=[0.0];events=[]
    monkeypatch.setattr(scheduler,'monotonic',lambda:now[0])
    monkeypatch.setattr(scheduler,'sleep',lambda delay:now.__setitem__(0,now[0]+delay))
    def score():events.append(('score',now[0]));return 0
    def backup():
        events.append(('backup',now[0]))
        if now[0]>=10:raise KeyboardInterrupt
        return 0
    assert scheduler.run_jobs([(score,2),(backup,5)])==0
    assert [t for kind,t in events if kind=='backup']==[0,5,10]
    assert [t for kind,t in events if kind=='score']==[0,2,4,6,8,10]


def test_single_shot_and_failure_stop():
    first=Mock(return_value=0);second=Mock(return_value=0)
    assert scheduler.run_jobs([(first,0),(second,0)])==0
    first.assert_called_once();second.assert_called_once()
    first.return_value=1;second.reset_mock()
    assert scheduler.run_jobs([(first,0),(second,300)])==1
    second.assert_not_called()


def test_backup_lifecycle_and_cleanup(monkeypatch,tmp_path):
    session=MagicMock()
    monkeypatch.setattr(backup_service,'GoogleSession',Mock(return_value=session))
    credentials=Mock()
    monkeypatch.setattr(backup_service,'user_credentials',credentials)
    monkeypatch.setattr(backup_service,'writable_folder',Mock(return_value='folder123'))
    paths=[]
    def download(directory,*args):
        from pathlib import Path
        path=Path(directory)/'snapshot.sqlite';path.write_bytes(b'synthetic')
        paths.append(path)
        return {'path':path,'size':9,'md5':'synthetic'}
    monkeypatch.setattr(backup_service,'download_backup',download)
    upload=Mock(return_value='url')
    monkeypatch.setattr(backup_service,'upload_backup',upload)
    assert backup_service.backup_once('folder123','1222','token.json','client.json')==0
    assert upload.called and not paths[0].exists()
    assert upload.call_args.args[3].startswith('1222_')
    upload.side_effect=RuntimeError('upload failed')
    with pytest.raises(RuntimeError):
        backup_service.backup_once('folder123','1222','token.json','client.json')
    assert not paths[-1].exists()


def test_backup_preview_needs_no_credentials(monkeypatch):
    credentials=Mock()
    monkeypatch.setattr(backup_service,'user_credentials',credentials)
    assert backup_service.backup_once('folder123','1222','missing','missing',dry_run=True)==0
    credentials.assert_not_called()


def test_backup_cli_and_authorize_modes(monkeypatch):
    import main
    authorize=Mock()
    monkeypatch.setattr(main,'user_credentials',authorize)
    assert main.cli(['--authorize-drive','--oauth-client','client.json'])==0
    authorize.assert_called_once_with('drive_token.json','client.json',interactive=True)
    assert main.cli(['--authorize-drive','--dry-run'])==0
    assert authorize.call_count==1
    backup=Mock(return_value=0)
    monkeypatch.setattr(main,'backup_once',backup)
    assert main.cli(['--backup-only','--backup-folder','folder123','--classroom','1222','--backup-interval','0'])==0
    assert backup.call_count==1
    assert main.cli(['--backup-only'])==1
    assert main.cli(['--backup-only','--backup-folder','folder123'])==1
    assert main.cli(['--backup-only','--backup-folder','folder123','--classroom','1222','--backup-interval','-1'])==1
    score=Mock(return_value=0)
    monkeypatch.setattr(main,'sync_once',score)
    assert main.cli(['--backup-folder','folder123','--classroom','1222','--dry-run'])==0
    score.assert_called_once()


def test_complete_backup_pipeline_with_real_binary_validation(monkeypatch,tmp_path):
    import hashlib
    import backup_download
    from test_backup_download import database_bytes, response_for
    from test_drive_upload import session_for
    data=database_bytes(tmp_path)
    name='1222_20261026T200000000000+0800.sqlite'
    artifact={'size':len(data),'md5':hashlib.md5(data).hexdigest()}
    session,posted=session_for(artifact,name,post_fail=True)
    context=MagicMock();context.__enter__.return_value=session
    monkeypatch.setattr(backup_service,'GoogleSession',Mock(return_value=context))
    monkeypatch.setattr(backup_service,'user_credentials',Mock())
    monkeypatch.setattr(backup_service,'backup_name',lambda classroom:name)
    monkeypatch.setenv('BACKEND_API_URL','http://backend')
    monkeypatch.setenv('ADMIN_TOKEN','test-secret')
    monkeypatch.setattr(backup_download.requests,'get',lambda *a,**kw:response_for(data))
    assert backup_service.backup_once('folder123','1222','token.json','client.json')==0
    assert len(posted)==1 and data in posted[0]


def test_cli_uses_300_second_backup_default_independently(monkeypatch):
    import main
    monkeypatch.setenv('INTERVAL','60')
    monkeypatch.setenv('SHEET_TITLE','1222')
    jobs=Mock(return_value=0)
    monkeypatch.setattr(main,'run_jobs',jobs)
    assert main.cli(['--backup-folder','folder123'])==0
    assert [interval for callback,interval in jobs.call_args.args[0]]==[60,300]


@pytest.mark.parametrize('title',['Combined','成績登記'])
def test_score_sync_rejects_computed_views_before_google_calls(monkeypatch,title):
    import main
    get=Mock()
    monkeypatch.setattr(main,'get_worksheet',get)
    monkeypatch.setenv('SHEET_TITLE',title)
    assert main.cli([])==1
    get.assert_not_called()
