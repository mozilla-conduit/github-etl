from unittest.mock import Mock, patch

import pytest
import requests

import main


@pytest.fixture
def mock_session():
    session = Mock(spec=requests.Session)
    session.headers = {}
    return session


def _make_response(
    status_code: int, text: str = "", headers: dict | None = None
) -> Mock:
    resp = Mock()
    resp.status_code = status_code
    resp.text = text
    resp.headers = headers or {}
    return resp


@patch("time.sleep")
def test_retries_on_500_then_succeeds(mock_sleep, mock_session):
    """Transient 500 errors are retried with backoff; success is returned."""
    fail = _make_response(500, "Internal Server Error")
    fail2 = _make_response(500, "Internal Server Error")
    ok = _make_response(200)
    ok.json.return_value = {}
    mock_session.get.side_effect = [fail, fail2, ok]

    result = main.github_get(mock_session, "https://api.github.com/repos/test")

    assert result is ok
    assert mock_session.get.call_count == 3
    assert mock_sleep.call_count == 2


@patch("time.sleep")
def test_retries_on_connection_error(mock_sleep, mock_session):
    """Network-level ConnectionError triggers retry with backoff."""
    ok = _make_response(200)
    mock_session.get.side_effect = [
        requests.exceptions.ConnectionError("connection refused"),
        ok,
    ]

    result = main.github_get(mock_session, "https://api.github.com/repos/test")

    assert result is ok
    assert mock_session.get.call_count == 2
    mock_sleep.assert_called_once()


def test_refreshes_auth_on_401(mock_session):
    """401 response triggers refresh_auth then retries."""
    unauthorized = _make_response(401, "Bad credentials")
    ok = _make_response(200)
    mock_session.get.side_effect = [unauthorized, ok]

    refresh_auth = Mock()
    result = main.github_get(
        mock_session, "https://api.github.com/repos/test", refresh_auth=refresh_auth
    )

    assert result is ok
    refresh_auth.assert_called_once()
    assert mock_session.get.call_count == 2


def test_401_without_refresh_auth_fails_immediately(mock_session):
    """401 with no refresh_auth callable raises SystemExit without retrying."""
    unauthorized = _make_response(401, "Bad credentials")
    mock_session.get.return_value = unauthorized

    with pytest.raises(SystemExit) as exc_info:
        main.github_get(mock_session, "https://api.github.com/repos/test")

    assert "401" in str(exc_info.value)
    assert mock_session.get.call_count == 1


@patch("time.sleep")
def test_401_exhausts_auth_retries(mock_sleep, mock_session):
    """Persistent 401 responses exhaust the auth retry budget and raise SystemExit."""
    unauthorized = _make_response(401, "Bad credentials")
    mock_session.get.return_value = unauthorized

    refresh_auth = Mock()

    with pytest.raises(SystemExit) as exc_info:
        main.github_get(
            mock_session,
            "https://api.github.com/repos/test",
            refresh_auth=refresh_auth,
        )

    assert "401" in str(exc_info.value)
    assert mock_session.get.call_count == main._MAX_AUTH_RETRIES + 1
    assert refresh_auth.call_count == main._MAX_AUTH_RETRIES


@patch("time.sleep")
def test_html_error_page_retried(mock_sleep, mock_session):
    """GitHub HTML error pages (e.g. 503 timeouts) are detected and retried."""
    html_body = "<html><p>We couldn't respond to your request in time.</p></html>"
    html_error = _make_response(
        503, html_body, headers={"Content-Type": "text/html; charset=utf-8"}
    )
    ok = _make_response(200)
    mock_session.get.side_effect = [html_error, ok]

    result = main.github_get(mock_session, "https://api.github.com/repos/test")

    assert result is ok
    assert mock_session.get.call_count == 2
    mock_sleep.assert_called_once()
