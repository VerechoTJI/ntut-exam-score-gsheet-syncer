import pytest

import timeout_settings


@pytest.mark.parametrize('name,default', [('BACKOFF_MAX_SECONDS', 10), ('GOOGLE_TIMEOUT_SECONDS', 30)])
def test_defaults_and_overrides(monkeypatch, name, default):
    monkeypatch.delenv(name, raising=False)
    assert timeout_settings.seconds(name, default) == default
    for raw in ('', '  '):
        monkeypatch.setenv(name, raw)
        assert timeout_settings.seconds(name, default) == default
    monkeypatch.setenv(name, ' 2.5 ')
    assert timeout_settings.seconds(name, default) == 2.5


@pytest.mark.parametrize('raw', ['bad', '0', '-1', 'nan', 'inf', '-inf'])
def test_invalid_seconds_name_the_setting(monkeypatch, raw):
    monkeypatch.setenv('GOOGLE_TIMEOUT_SECONDS', raw)
    with pytest.raises(ValueError, match='GOOGLE_TIMEOUT_SECONDS'):
        timeout_settings.seconds('GOOGLE_TIMEOUT_SECONDS', 30)
