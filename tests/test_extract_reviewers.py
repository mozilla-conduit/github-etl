#!/usr/bin/env python3
"""
Tests for extract_reviewers function.

Tests reviewer extraction including different review states, rate limiting,
and error handling.
"""

from unittest.mock import Mock, patch

import pytest

import main


def test_extract_reviewers_basic(mock_session):
    """Test basic extraction of reviewers."""
    reviewers_response = Mock()
    reviewers_response.status_code = 200
    reviewers_response.json.return_value = [
        {
            "id": 789,
            "user": {"login": "reviewer1"},
            "state": "APPROVED",
            "submitted_at": "2024-01-01T15:00:00Z",
        },
        {
            "id": 790,
            "user": {"login": "reviewer2"},
            "state": "CHANGES_REQUESTED",
            "submitted_at": "2024-01-01T16:00:00Z",
        },
    ]

    mock_session.get.return_value = reviewers_response

    result = main.extract_reviewers(mock_session, "mozilla/firefox", 123)

    assert len(result) == 2
    assert result[0]["state"] == "APPROVED"
    assert result[1]["state"] == "CHANGES_REQUESTED"


def test_multiple_review_states(mock_session):
    """Test handling multiple different review states."""
    reviewers_response = Mock()
    reviewers_response.status_code = 200
    reviewers_response.json.return_value = [
        {"id": 1, "state": "APPROVED", "user": {"login": "user1"}},
        {"id": 2, "state": "CHANGES_REQUESTED", "user": {"login": "user2"}},
        {"id": 3, "state": "COMMENTED", "user": {"login": "user3"}},
        {"id": 4, "state": "DISMISSED", "user": {"login": "user4"}},
    ]

    mock_session.get.return_value = reviewers_response

    result = main.extract_reviewers(mock_session, "mozilla/firefox", 123)

    assert len(result) == 4
    states = [r["state"] for r in result]
    assert "APPROVED" in states
    assert "CHANGES_REQUESTED" in states
    assert "COMMENTED" in states


def test_empty_reviewers_list(mock_session):
    """Test handling PR with no reviewers."""
    reviewers_response = Mock()
    reviewers_response.status_code = 200
    reviewers_response.json.return_value = []

    mock_session.get.return_value = reviewers_response

    result = main.extract_reviewers(mock_session, "mozilla/firefox", 123)

    assert result == []


@patch("main.sleep_for_rate_limit")
def test_rate_limit_handling(mock_sleep, mock_session):
    """Test rate limit handling when fetching reviewers."""
    rate_limit_response = Mock()
    rate_limit_response.status_code = 403
    rate_limit_response.headers = {"X-RateLimit-Remaining": "0"}

    success_response = Mock()
    success_response.status_code = 200
    success_response.json.return_value = []

    mock_session.get.side_effect = [rate_limit_response, success_response]

    result = main.extract_reviewers(mock_session, "mozilla/firefox", 123)

    mock_sleep.assert_called_once()
    assert result == []


def test_api_error(mock_session):
    """Test API error handling when fetching reviewers."""
    error_response = Mock()
    error_response.status_code = 500
    error_response.text = "Internal Server Error"

    mock_session.get.return_value = error_response

    with pytest.raises(SystemExit) as exc_info:
        main.extract_reviewers(mock_session, "mozilla/firefox", 123)

    assert "GitHub API error 500" in str(exc_info.value)


def test_custom_github_api_url_reviewers(mock_session):
    """Test using custom GitHub API URL for reviewers."""
    custom_url = "https://mock-github.example.com"

    reviewers_response = Mock()
    reviewers_response.status_code = 200
    reviewers_response.json.return_value = []

    mock_session.get.return_value = reviewers_response

    main.extract_reviewers(
        mock_session, "mozilla/firefox", 123, github_api_url=custom_url
    )

    call_args = mock_session.get.call_args
    assert custom_url in call_args[0][0]
