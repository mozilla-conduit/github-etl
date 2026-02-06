#!/usr/bin/env python3
"""
Tests for extract_commits function.

Tests commit extraction including file details, rate limiting, and error handling.
"""

from unittest.mock import Mock, patch

import pytest

import main


def test_extract_commits_with_files(mock_session):
    """Test extracting commits with file details."""
    # Mock commits list response
    commits_response = Mock()
    commits_response.status_code = 200
    commits_response.json.return_value = [
        {"sha": "abc123"},
        {"sha": "def456"},
    ]

    # Mock individual commit responses
    commit_detail_1 = Mock()
    commit_detail_1.status_code = 200
    commit_detail_1.json.return_value = {
        "sha": "abc123",
        "files": [{"filename": "file1.py", "additions": 10}],
    }

    commit_detail_2 = Mock()
    commit_detail_2.status_code = 200
    commit_detail_2.json.return_value = {
        "sha": "def456",
        "files": [{"filename": "file2.py", "deletions": 5}],
    }

    mock_session.get.side_effect = [
        commits_response,
        commit_detail_1,
        commit_detail_2,
    ]

    result = main.extract_commits(mock_session, "mozilla/firefox", 123)

    assert len(result) == 2
    assert result[0]["sha"] == "abc123"
    assert result[0]["files"][0]["filename"] == "file1.py"
    assert result[1]["sha"] == "def456"
    assert result[1]["files"][0]["filename"] == "file2.py"


def test_multiple_files_per_commit(mock_session):
    """Test handling multiple files in a single commit."""
    commits_response = Mock()
    commits_response.status_code = 200
    commits_response.json.return_value = [{"sha": "abc123"}]

    commit_detail = Mock()
    commit_detail.status_code = 200
    commit_detail.json.return_value = {
        "sha": "abc123",
        "files": [
            {"filename": "file1.py", "additions": 10},
            {"filename": "file2.py", "additions": 20},
            {"filename": "file3.py", "deletions": 5},
        ],
    }

    mock_session.get.side_effect = [commits_response, commit_detail]

    result = main.extract_commits(mock_session, "mozilla/firefox", 123)

    assert len(result) == 1
    assert len(result[0]["files"]) == 3


@patch("main.sleep_for_rate_limit")
def test_rate_limit_on_commits_list(mock_sleep, mock_session):
    """Test rate limit handling when fetching commits list."""
    # Rate limit response
    rate_limit_response = Mock()
    rate_limit_response.status_code = 403
    rate_limit_response.headers = {"X-RateLimit-Remaining": "0"}

    # Success response
    success_response = Mock()
    success_response.status_code = 200
    success_response.json.return_value = []

    mock_session.get.side_effect = [rate_limit_response, success_response]

    result = main.extract_commits(mock_session, "mozilla/firefox", 123)

    mock_sleep.assert_called_once()
    assert result == []


def test_api_error_on_commits_list(mock_session):
    """Test API error handling when fetching commits list."""
    error_response = Mock()
    error_response.status_code = 500
    error_response.text = "Internal Server Error"

    mock_session.get.return_value = error_response

    with pytest.raises(SystemExit) as exc_info:
        main.extract_commits(mock_session, "mozilla/firefox", 123)

    assert "GitHub API error 500" in str(exc_info.value)


def test_api_error_on_individual_commit(mock_session):
    """Test API error when fetching individual commit details."""
    commits_response = Mock()
    commits_response.status_code = 200
    commits_response.json.return_value = [{"sha": "abc123"}]

    commit_error = Mock()
    commit_error.status_code = 404
    commit_error.text = "Commit not found"

    mock_session.get.side_effect = [commits_response, commit_error]

    with pytest.raises(SystemExit) as exc_info:
        main.extract_commits(mock_session, "mozilla/firefox", 123)

    assert "GitHub API error 404" in str(exc_info.value)


def test_commit_without_sha_field(mock_session):
    """Test handling commits without sha field."""
    commits_response = Mock()
    commits_response.status_code = 200
    commits_response.json.return_value = [
        {"sha": "abc123"},
        {},  # Missing sha field
    ]

    commit_detail_1 = Mock()
    commit_detail_1.status_code = 200
    commit_detail_1.json.return_value = {"sha": "abc123", "files": []}

    commit_detail_2 = Mock()
    commit_detail_2.status_code = 200
    commit_detail_2.json.return_value = {"files": []}

    mock_session.get.side_effect = [
        commits_response,
        commit_detail_1,
        commit_detail_2,
    ]

    result = main.extract_commits(mock_session, "mozilla/firefox", 123)

    # Should handle the commit without sha gracefully
    assert len(result) == 2


def test_custom_github_api_url_commits(mock_session):
    """Test using custom GitHub API URL for commits."""
    custom_url = "https://mock-github.example.com"

    commits_response = Mock()
    commits_response.status_code = 200
    commits_response.json.return_value = []

    mock_session.get.return_value = commits_response

    main.extract_commits(
        mock_session, "mozilla/firefox", 123, github_api_url=custom_url
    )

    call_args = mock_session.get.call_args
    assert custom_url in call_args[0][0]


def test_empty_commits_list(mock_session):
    """Test handling PR with no commits."""
    commits_response = Mock()
    commits_response.status_code = 200
    commits_response.json.return_value = []

    mock_session.get.return_value = commits_response

    result = main.extract_commits(mock_session, "mozilla/firefox", 123)

    assert result == []
