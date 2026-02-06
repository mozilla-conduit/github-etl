#!/usr/bin/env python3
"""
Tests for extract_pull_requests function.

Tests pull request extraction including pagination, rate limiting, error handling,
and enrichment with commits, reviewers, and comments.
"""

from unittest.mock import Mock, patch

import pytest

import main


def test_extract_pull_requests_basic(mock_session):
    """Test basic extraction of pull requests."""
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = [
        {"number": 1, "title": "PR 1"},
        {"number": 2, "title": "PR 2"},
    ]
    mock_response.links = {}

    mock_session.get.return_value = mock_response

    # Mock the extract functions
    with (
        patch("main.extract_commits", return_value=[]),
        patch("main.extract_reviewers", return_value=[]),
        patch("main.extract_comments", return_value=[]),
    ):
        result = list(main.extract_pull_requests(mock_session, "mozilla/firefox"))

    assert len(result) == 1
    assert len(result[0]) == 2
    assert result[0][0]["number"] == 1
    assert result[0][1]["number"] == 2


def test_extract_multiple_pages(mock_session):
    """Test extracting data across multiple pages with pagination."""
    # First page response
    mock_response_1 = Mock()
    mock_response_1.status_code = 200
    mock_response_1.json.return_value = [
        {"number": 1, "title": "PR 1"},
        {"number": 2, "title": "PR 2"},
    ]
    mock_response_1.links = {
        "next": {"url": "https://api.github.com/repos/mozilla/firefox/pulls?page=2"}
    }

    # Second page response
    mock_response_2 = Mock()
    mock_response_2.status_code = 200
    mock_response_2.json.return_value = [{"number": 3, "title": "PR 3"}]
    mock_response_2.links = {}

    mock_session.get.side_effect = [mock_response_1, mock_response_2]

    with (
        patch("main.extract_commits", return_value=[]),
        patch("main.extract_reviewers", return_value=[]),
        patch("main.extract_comments", return_value=[]),
    ):
        result = list(main.extract_pull_requests(mock_session, "mozilla/firefox"))

    assert len(result) == 2
    assert len(result[0]) == 2
    assert len(result[1]) == 1
    assert result[0][0]["number"] == 1
    assert result[1][0]["number"] == 3


def test_enriches_prs_with_commit_data(mock_session):
    """Test that PRs are enriched with commit data."""
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = [{"number": 1, "title": "PR 1"}]
    mock_response.links = {}

    mock_session.get.return_value = mock_response

    mock_commits = [{"sha": "abc123"}]

    with (
        patch(
            "main.extract_commits", return_value=mock_commits
        ) as mock_extract_commits,
        patch("main.extract_reviewers", return_value=[]),
        patch("main.extract_comments", return_value=[]),
    ):
        result = list(main.extract_pull_requests(mock_session, "mozilla/firefox"))

    assert result[0][0]["commit_data"] == mock_commits
    mock_extract_commits.assert_called_once()


def test_enriches_prs_with_reviewer_data(mock_session):
    """Test that PRs are enriched with reviewer data."""
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = [{"number": 1, "title": "PR 1"}]
    mock_response.links = {}

    mock_session.get.return_value = mock_response

    mock_reviewers = [{"id": 789, "state": "APPROVED"}]

    with (
        patch("main.extract_commits", return_value=[]),
        patch(
            "main.extract_reviewers", return_value=mock_reviewers
        ) as mock_extract_reviewers,
        patch("main.extract_comments", return_value=[]),
    ):
        result = list(main.extract_pull_requests(mock_session, "mozilla/firefox"))

    assert result[0][0]["reviewer_data"] == mock_reviewers
    mock_extract_reviewers.assert_called_once()


def test_enriches_prs_with_comment_data(mock_session):
    """Test that PRs are enriched with comment data."""
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = [{"number": 1, "title": "PR 1"}]
    mock_response.links = {}

    mock_session.get.return_value = mock_response

    mock_comments = [{"id": 456, "body": "Great work!"}]

    with (
        patch("main.extract_commits", return_value=[]),
        patch("main.extract_reviewers", return_value=[]),
        patch(
            "main.extract_comments", return_value=mock_comments
        ) as mock_extract_comments,
    ):
        result = list(main.extract_pull_requests(mock_session, "mozilla/firefox"))

    assert result[0][0]["comment_data"] == mock_comments
    mock_extract_comments.assert_called_once()


@patch("main.sleep_for_rate_limit")
def test_handles_rate_limit(mock_sleep, mock_session):
    """Test that extract_pull_requests handles rate limiting correctly."""
    # Rate limit response
    mock_response_rate_limit = Mock()
    mock_response_rate_limit.status_code = 403
    mock_response_rate_limit.headers = {"X-RateLimit-Remaining": "0"}

    # Successful response after rate limit
    mock_response_success = Mock()
    mock_response_success.status_code = 200
    mock_response_success.json.return_value = [{"number": 1, "title": "PR 1"}]
    mock_response_success.links = {}

    mock_session.get.side_effect = [
        mock_response_rate_limit,
        mock_response_success,
    ]

    with (
        patch("main.extract_commits", return_value=[]),
        patch("main.extract_reviewers", return_value=[]),
        patch("main.extract_comments", return_value=[]),
    ):
        result = list(main.extract_pull_requests(mock_session, "mozilla/firefox"))

    mock_sleep.assert_called_once_with(mock_response_rate_limit)
    assert len(result) == 1


def test_handles_api_error_404(mock_session):
    """Test that extract_pull_requests raises SystemExit on 404."""
    mock_response = Mock()
    mock_response.status_code = 404
    mock_response.text = "Not Found"

    mock_session.get.return_value = mock_response

    with pytest.raises(SystemExit) as exc_info:
        list(main.extract_pull_requests(mock_session, "mozilla/nonexistent"))

    assert "GitHub API error 404" in str(exc_info.value)


def test_handles_api_error_500(mock_session):
    """Test that extract_pull_requests raises SystemExit on 500."""
    mock_response = Mock()
    mock_response.status_code = 500
    mock_response.text = "Internal Server Error"

    mock_session.get.return_value = mock_response

    with pytest.raises(SystemExit) as exc_info:
        list(main.extract_pull_requests(mock_session, "mozilla/firefox"))

    assert "GitHub API error 500" in str(exc_info.value)


def test_stops_on_empty_batch(mock_session):
    """Test that extraction stops when an empty batch is returned."""
    # First page with data
    mock_response_1 = Mock()
    mock_response_1.status_code = 200
    mock_response_1.json.return_value = [{"number": 1}]
    mock_response_1.links = {
        "next": {"url": "https://api.github.com/repos/mozilla/firefox/pulls?page=2"}
    }

    # Second page empty
    mock_response_2 = Mock()
    mock_response_2.status_code = 200
    mock_response_2.json.return_value = []
    mock_response_2.links = {}

    mock_session.get.side_effect = [mock_response_1, mock_response_2]

    with (
        patch("main.extract_commits", return_value=[]),
        patch("main.extract_reviewers", return_value=[]),
        patch("main.extract_comments", return_value=[]),
    ):
        result = list(main.extract_pull_requests(mock_session, "mozilla/firefox"))

    # Should only have 1 chunk from first page
    assert len(result) == 1
    assert len(result[0]) == 1


def test_invalid_page_number_handling(mock_session):
    """Test handling of invalid page number in pagination."""
    mock_response_1 = Mock()
    mock_response_1.status_code = 200
    mock_response_1.json.return_value = [{"number": 1}]
    mock_response_1.links = {
        "next": {
            "url": "https://api.github.com/repos/mozilla/firefox/pulls?page=invalid"
        }
    }

    mock_session.get.return_value = mock_response_1

    with (
        patch("main.extract_commits", return_value=[]),
        patch("main.extract_reviewers", return_value=[]),
        patch("main.extract_comments", return_value=[]),
    ):
        result = list(main.extract_pull_requests(mock_session, "mozilla/firefox"))

    # Should stop pagination on invalid page number
    assert len(result) == 1


def test_custom_github_api_url(mock_session):
    """Test using custom GitHub API URL."""
    custom_url = "https://mock-github.example.com"

    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = [{"number": 1}]
    mock_response.links = {}

    mock_session.get.return_value = mock_response

    with (
        patch("main.extract_commits", return_value=[]),
        patch("main.extract_reviewers", return_value=[]),
        patch("main.extract_comments", return_value=[]),
    ):
        list(
            main.extract_pull_requests(
                mock_session, "mozilla/firefox", github_api_url=custom_url
            )
        )

    # Verify custom URL was used
    call_args = mock_session.get.call_args
    assert custom_url in call_args[0][0]


def test_skips_prs_without_number_field(mock_session):
    """Test that PRs without 'number' field are skipped."""
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = [
        {"number": 1, "title": "PR 1"},
        {"title": "PR without number"},  # Missing number field
        {"number": 2, "title": "PR 2"},
    ]
    mock_response.links = {}

    mock_session.get.return_value = mock_response

    with (
        patch("main.extract_commits", return_value=[]) as mock_commits,
        patch("main.extract_reviewers", return_value=[]),
        patch("main.extract_comments", return_value=[]),
    ):
        list(main.extract_pull_requests(mock_session, "mozilla/firefox"))

    # extract_commits should only be called for PRs with number field
    assert mock_commits.call_count == 2
