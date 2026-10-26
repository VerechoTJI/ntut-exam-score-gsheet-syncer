"""Google SDK authentication retries with a configurable per-request timeout."""
from google.auth.transport.requests import AuthorizedSession

from timeout_settings import seconds


class GoogleSession(AuthorizedSession):
    def __init__(self, credentials):
        super().__init__(credentials, refresh_timeout=seconds('GOOGLE_TIMEOUT_SECONDS', 30))

    def request(self, method, url, data=None, **kwargs):
        # AuthorizedSession re-enters this method after a 401/token refresh.
        # Replay streamed multipart uploads from the start on that SDK retry.
        if hasattr(data, 'seek'):
            data.seek(0)
        kwargs['timeout'] = seconds('GOOGLE_TIMEOUT_SECONDS', 30)
        return super().request(method, url, data=data, **kwargs)
