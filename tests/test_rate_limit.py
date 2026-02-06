#!/usr/bin/env python3
"""
Tests for sleep_for_rate_limit function.

Tests rate limit handling including wait time calculation and edge cases.
"""

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

    # Should sleep for 0 seconds (max of 0 and negative value)
    mock_sleep.assert_called_once_with(0)


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
