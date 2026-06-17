from unittest.mock import Mock, patch

import main


@patch("time.time")
@patch("time.sleep")
def test_sleep_for_rate_limit_calculates_wait_time(mock_sleep, mock_time):
    """Test that sleep_for_rate_limit calculates correct wait time."""
    mock_time.return_value = 1000

    mock_response = Mock()
    mock_response.headers = {
        "X-RateLimit-Remaining": "0",
        "X-RateLimit-Reset": "1120",  # 120 seconds from now
    }

    main.sleep_for_rate_limit(mock_response)

    mock_sleep.assert_called_once_with(120)


@patch("time.time")
@patch("time.sleep")
def test_sleep_for_rate_limit_when_reset_already_passed(mock_sleep, mock_time):
    """Test that sleep_for_rate_limit doesn't sleep negative time."""
    mock_time.return_value = 2000

    mock_response = Mock()
    mock_response.headers = {
        "X-RateLimit-Remaining": "0",
        "X-RateLimit-Reset": "1500",  # Already passed
    }

    main.sleep_for_rate_limit(mock_response)

    # Reset already passed -> no positive wait, so we skip sleeping entirely.
    mock_sleep.assert_not_called()


@patch("time.sleep")
def test_sleep_for_rate_limit_when_remaining_not_zero(mock_sleep):
    """Test that sleep_for_rate_limit doesn't sleep when remaining > 0."""
    mock_response = Mock()
    mock_response.headers = {
        "X-RateLimit-Remaining": "5",
        "X-RateLimit-Reset": "1500",
    }

    main.sleep_for_rate_limit(mock_response)

    # Should not sleep when remaining > 0
    mock_sleep.assert_not_called()


@patch("time.sleep")
def test_sleep_for_rate_limit_with_missing_headers(mock_sleep):
    """Test sleep_for_rate_limit with missing rate limit headers."""
    mock_response = Mock()
    mock_response.headers = {}

    main.sleep_for_rate_limit(mock_response)

    # Should not sleep when headers are missing (defaults to remaining=1)
    mock_sleep.assert_not_called()


@patch("time.sleep")
def test_sleep_for_rate_limit_honors_retry_after(mock_sleep):
    """Secondary rate limit: sleep for the Retry-After delay."""
    mock_response = Mock()
    mock_response.headers = {
        "X-RateLimit-Remaining": "5",  # primary limit not exhausted
        "Retry-After": "30",
    }

    main.sleep_for_rate_limit(mock_response)

    mock_sleep.assert_called_once_with(30)


@patch("time.time")
@patch("time.sleep")
def test_sleep_for_rate_limit_takes_longer_of_reset_and_retry_after(
    mock_sleep, mock_time
):
    """When both signals are present, the longer wait wins."""
    mock_time.return_value = 1000

    mock_response = Mock()
    mock_response.headers = {
        "X-RateLimit-Remaining": "0",
        "X-RateLimit-Reset": "1060",  # 60 seconds from now
        "Retry-After": "90",
    }

    main.sleep_for_rate_limit(mock_response)

    mock_sleep.assert_called_once_with(90)


@patch("time.sleep")
def test_sleep_for_rate_limit_ignores_http_date_retry_after(mock_sleep):
    """A Retry-After HTTP-date (non-integer) is ignored without raising."""
    mock_response = Mock()
    mock_response.headers = {
        "X-RateLimit-Remaining": "5",
        "Retry-After": "Wed, 21 Oct 2026 07:28:00 GMT",
    }

    main.sleep_for_rate_limit(mock_response)

    mock_sleep.assert_not_called()


def test_is_rate_limited_primary():
    """403/429 with X-RateLimit-Remaining: 0 is a primary rate limit."""
    for status in (403, 429):
        resp = Mock()
        resp.status_code = status
        resp.headers = {"X-RateLimit-Remaining": "0"}
        assert main._is_rate_limited(resp) is True


def test_is_rate_limited_secondary():
    """403/429 with a Retry-After header is a secondary rate limit."""
    for status in (403, 429):
        resp = Mock()
        resp.status_code = status
        resp.headers = {"X-RateLimit-Remaining": "5", "Retry-After": "30"}
        assert main._is_rate_limited(resp) is True


def test_is_rate_limited_plain_forbidden():
    """A 403 with no rate-limit signals is not treated as rate-limited."""
    resp = Mock()
    resp.status_code = 403
    resp.headers = {}
    assert main._is_rate_limited(resp) is False


def test_is_rate_limited_non_rate_limit_status():
    """Non-403/429 statuses are never rate limits."""
    resp = Mock()
    resp.status_code = 500
    resp.headers = {"X-RateLimit-Remaining": "0", "Retry-After": "30"}
    assert main._is_rate_limited(resp) is False
