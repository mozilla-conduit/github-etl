from unittest.mock import Mock, patch

import pytest

import main


@patch("main.sleep_for_rate_limit")
def test_rate_limit_handling_comments(mock_sleep, mock_session):
    """Test rate limit handling when fetching comments."""
    rate_limit_response = Mock()
    rate_limit_response.status_code = 403
    rate_limit_response.headers = {"X-RateLimit-Remaining": "0"}

    success_response = Mock()
    success_response.status_code = 200
    success_response.json.return_value = []

    mock_session.get.side_effect = [rate_limit_response, success_response]

    main.extract_comments(mock_session, "mozilla/firefox", 123)

    mock_sleep.assert_called_once()


def test_api_error_comments(mock_session):
    """Test API error handling when fetching comments."""
    error_response = Mock()
    error_response.status_code = 404
    error_response.text = "Not Found"

    mock_session.get.return_value = error_response

    with pytest.raises(SystemExit) as exc_info:
        main.extract_comments(mock_session, "mozilla/firefox", 123)

    assert "GitHub API error 404" in str(exc_info.value)
