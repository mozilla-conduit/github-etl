#!/usr/bin/env python3
from unittest.mock import Mock, patch

import pytest

import main


def test_extract_commits_with_files(mock_session):
    """Test that file details are fetched per commit and merged into commit data."""
    commits_response = Mock()
    commits_response.status_code = 200
    commits_response.json.return_value = [
        {"sha": "abc123"},
        {"sha": "def456"},
    ]

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

    # Verify the individual commit detail endpoints were fetched
    assert mock_session.get.call_count == 3
    calls = mock_session.get.call_args_list
    assert "commits/abc123" in calls[1][0][0]
    assert "commits/def456" in calls[2][0][0]

    # Verify file data from detail responses is merged into each commit
    assert result[0]["files"][0]["filename"] == "file1.py"
    assert result[1]["files"][0]["filename"] == "file2.py"


def test_multiple_files_per_commit(mock_session):
    """Test that all files from a commit detail response are merged into the commit."""
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

    assert len(result[0]["files"]) == 3


@patch("main.sleep_for_rate_limit")
def test_rate_limit_on_commits_list(mock_sleep, mock_session):
    """Test rate limit handling when fetching commits list."""
    rate_limit_response = Mock()
    rate_limit_response.status_code = 403
    rate_limit_response.headers = {"X-RateLimit-Remaining": "0"}

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
