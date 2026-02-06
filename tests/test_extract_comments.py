#!/usr/bin/env python3
"""
Tests for extract_comments function.

Tests comment extraction including endpoint verification, rate limiting,
and error handling.
"""

from unittest.mock import Mock, patch

import pytest

import main


def test_extract_comments_basic(mock_session):
    """Test basic extraction of comments."""
    comments_response = Mock()
    comments_response.status_code = 200
    comments_response.json.return_value = [
        {
            "id": 456,
            "user": {"login": "commenter1"},
            "body": "This looks good",
            "created_at": "2024-01-01T14:00:00Z",
        },
        {
            "id": 457,
            "user": {"login": "commenter2"},
            "body": "I have concerns",
            "created_at": "2024-01-01T15:00:00Z",
        },
    ]

    mock_session.get.return_value = comments_response

    result = main.extract_comments(mock_session, "mozilla/firefox", 123)

    assert len(result) == 2
    assert result[0]["id"] == 456
    assert result[1]["id"] == 457


def test_uses_issues_endpoint(mock_session):
    """Test that comments use /issues endpoint not /pulls."""
    comments_response = Mock()
    comments_response.status_code = 200
    comments_response.json.return_value = []

    mock_session.get.return_value = comments_response

    main.extract_comments(mock_session, "mozilla/firefox", 123)

    call_args = mock_session.get.call_args
    url = call_args[0][0]
    assert "/issues/123/comments" in url
    assert "/pulls/123/comments" not in url


def test_multiple_comments(mock_session):
    """Test handling multiple comments."""
    comments_response = Mock()
    comments_response.status_code = 200
    comments_response.json.return_value = [
        {"id": i, "user": {"login": f"user{i}"}, "body": f"Comment {i}"}
        for i in range(1, 11)
    ]

    mock_session.get.return_value = comments_response

    result = main.extract_comments(mock_session, "mozilla/firefox", 123)

    assert len(result) == 10


def test_empty_comments_list(mock_session):
    """Test handling PR with no comments."""
    comments_response = Mock()
    comments_response.status_code = 200
    comments_response.json.return_value = []

    mock_session.get.return_value = comments_response

    result = main.extract_comments(mock_session, "mozilla/firefox", 123)

    assert result == []


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

    result = main.extract_comments(mock_session, "mozilla/firefox", 123)

    mock_sleep.assert_called_once()
    assert result == []


def test_api_error_comments(mock_session):
    """Test API error handling when fetching comments."""
    error_response = Mock()
    error_response.status_code = 404
    error_response.text = "Not Found"

    mock_session.get.return_value = error_response

    with pytest.raises(SystemExit) as exc_info:
        main.extract_comments(mock_session, "mozilla/firefox", 123)

    assert "GitHub API error 404" in str(exc_info.value)


def test_custom_github_api_url_comments(mock_session):
    """Test using custom GitHub API URL for comments."""
    custom_url = "https://mock-github.example.com"

    comments_response = Mock()
    comments_response.status_code = 200
    comments_response.json.return_value = []

    mock_session.get.return_value = comments_response

    main.extract_comments(
        mock_session, "mozilla/firefox", 123, github_api_url=custom_url
    )

    call_args = mock_session.get.call_args
    assert custom_url in call_args[0][0]
