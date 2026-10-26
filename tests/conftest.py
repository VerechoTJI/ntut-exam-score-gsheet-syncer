import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parents[1]))


@pytest.fixture(autouse=True)
def no_live_configuration(monkeypatch):
    import db
    import main
    import retry
    monkeypatch.setattr(retry, 'sleep', lambda delay: None)
    monkeypatch.setattr(db, 'load_dotenv', lambda: None)
    monkeypatch.setattr(main, 'load_dotenv', lambda: None)
    for name in ('SHEET_URL', 'SHEET_TITLE', 'BACKEND_API_URL', 'ADMIN_TOKEN', 'INTERVAL',
                 'BACKUP_FOLDER', 'BACKUP_INTERVAL', 'CLASSROOM',
                 'BACKOFF_MAX_SECONDS', 'GOOGLE_TIMEOUT_SECONDS'):
        monkeypatch.delenv(name, raising=False)
