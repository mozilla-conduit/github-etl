#!/usr/bin/env python3
"""
Comprehensive test suite for GitHub ETL main.py

This test suite provides complete coverage for all functions in main.py,
including extraction, transformation, loading, and orchestration logic.
"""

import logging
import os
import time
from datetime import datetime
from unittest.mock import Mock, MagicMock, patch
import pytest
import requests
from google.cloud import bigquery

import main

# =============================================================================
# FIXTURES
# =============================================================================


@pytest.fixture
def mock_session():
    """Provide a mocked requests.Session for testing."""
    session = Mock(spec=requests.Session)
    session.headers = {}
    return session


@pytest.fixture
def mock_bigquery_client():
    """Provide a mocked BigQuery client for testing."""
    client = Mock(spec=bigquery.Client)
    client.project = "test-project"
    client.insert_rows_json = Mock(return_value=[])
    return client


@pytest.fixture
def mock_pr_response():
    """Provide a realistic pull request response for testing."""
    return {
        "number": 123,
        "title": "Bug 1234567 - Fix login issue",
        "state": "closed",
        "created_at": "2024-01-01T10:00:00Z",
        "updated_at": "2024-01-02T10:00:00Z",
        "merged_at": "2024-01-02T10:00:00Z",
        "user": {"login": "testuser"},
        "head": {"ref": "fix-branch"},
        "base": {"ref": "main"},
        "labels": [{"name": "bug"}, {"name": "priority-high"}],
        "commit_data": [],
        "reviewer_data": [],
        "comment_data": [],
    }


@pytest.fixture
def mock_commit_response():
    """Provide a realistic commit response with files."""
    return {
        "sha": "abc123def456",
        "commit": {
            "author": {
                "name": "Test Author",
                "email": "test@example.com",
                "date": "2024-01-01T12:00:00Z",
            }
        },
        "files": [
            {
                "filename": "src/login.py",
                "additions": 10,
                "deletions": 5,
                "changes": 15,
            },
            {
                "filename": "tests/test_login.py",
                "additions": 20,
                "deletions": 2,
                "changes": 22,
            },
        ],
    }


@pytest.fixture
def mock_reviewer_response():
    """Provide a realistic reviewer response."""
    return {
        "id": 789,
        "user": {"login": "reviewer1"},
        "state": "APPROVED",
        "submitted_at": "2024-01-01T15:00:00Z",
        "body": "LGTM",
    }


@pytest.fixture
def mock_comment_response():
    """Provide a realistic comment response."""
    return {
        "id": 456,
        "user": {"login": "commenter1"},
        "created_at": "2024-01-01T14:00:00Z",
        "body": "This looks good to me",
        "pull_request_review_id": None,
    }


# =============================================================================
# TEST CLASSES
# =============================================================================


class TestSetupLogging:
    """Tests for setup_logging function."""

    def test_setup_logging_configures_logger(self):
        """Test that setup_logging configures the root logger correctly."""
        main.setup_logging()

        root_logger = logging.getLogger()
        assert root_logger.level == logging.INFO
        assert len(root_logger.handlers) > 0

        # Check that at least one handler is a StreamHandler
        has_stream_handler = any(
            isinstance(handler, logging.StreamHandler)
            for handler in root_logger.handlers
        )
        assert has_stream_handler


class TestSleepForRateLimit:
    """Tests for sleep_for_rate_limit function."""

    @patch("time.time")
    @patch("time.sleep")
    def test_sleep_for_rate_limit_when_remaining_is_zero(self, mock_sleep, mock_time):
        """Test that sleep_for_rate_limit sleeps until reset time."""
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
    def test_sleep_for_rate_limit_when_reset_already_passed(
        self, mock_sleep, mock_time
    ):
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
    def test_sleep_for_rate_limit_when_remaining_not_zero(self, mock_sleep):
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
    def test_sleep_for_rate_limit_with_missing_headers(self, mock_sleep):
        """Test sleep_for_rate_limit with missing rate limit headers."""
        mock_response = Mock()
        mock_response.headers = {}

        main.sleep_for_rate_limit(mock_response)

        # Should not sleep when headers are missing (defaults to remaining=1)
        mock_sleep.assert_not_called()


class TestExtractPullRequests:
    """Tests for extract_pull_requests function."""

    def test_extract_single_page(self, mock_session):
        """Test extracting data from a single page of results."""
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

    def test_extract_multiple_pages(self, mock_session):
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

    def test_enriches_prs_with_commit_data(self, mock_session):
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

    def test_enriches_prs_with_reviewer_data(self, mock_session):
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

    def test_enriches_prs_with_comment_data(self, mock_session):
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
    def test_handles_rate_limit(self, mock_sleep, mock_session):
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

    def test_handles_api_error_404(self, mock_session):
        """Test that extract_pull_requests raises SystemExit on 404."""
        mock_response = Mock()
        mock_response.status_code = 404
        mock_response.text = "Not Found"

        mock_session.get.return_value = mock_response

        with pytest.raises(SystemExit) as exc_info:
            list(main.extract_pull_requests(mock_session, "mozilla/nonexistent"))

        assert "GitHub API error 404" in str(exc_info.value)

    def test_handles_api_error_500(self, mock_session):
        """Test that extract_pull_requests raises SystemExit on 500."""
        mock_response = Mock()
        mock_response.status_code = 500
        mock_response.text = "Internal Server Error"

        mock_session.get.return_value = mock_response

        with pytest.raises(SystemExit) as exc_info:
            list(main.extract_pull_requests(mock_session, "mozilla/firefox"))

        assert "GitHub API error 500" in str(exc_info.value)

    def test_stops_on_empty_batch(self, mock_session):
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

    def test_invalid_page_number_handling(self, mock_session):
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

    def test_custom_github_api_url(self, mock_session):
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

    def test_skips_prs_without_number_field(self, mock_session):
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


class TestExtractCommits:
    """Tests for extract_commits function."""

    def test_fetch_commits_with_files(self, mock_session):
        """Test fetching commits with files for a PR."""
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

    def test_multiple_files_per_commit(self, mock_session):
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
    def test_rate_limit_on_commits_list(self, mock_sleep, mock_session):
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

    def test_api_error_on_commits_list(self, mock_session):
        """Test API error handling when fetching commits list."""
        error_response = Mock()
        error_response.status_code = 500
        error_response.text = "Internal Server Error"

        mock_session.get.return_value = error_response

        with pytest.raises(SystemExit) as exc_info:
            main.extract_commits(mock_session, "mozilla/firefox", 123)

        assert "GitHub API error 500" in str(exc_info.value)

    def test_api_error_on_individual_commit(self, mock_session):
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

    def test_commit_without_sha_field(self, mock_session):
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

    def test_custom_github_api_url(self, mock_session):
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

    def test_empty_commits_list(self, mock_session):
        """Test handling PR with no commits."""
        commits_response = Mock()
        commits_response.status_code = 200
        commits_response.json.return_value = []

        mock_session.get.return_value = commits_response

        result = main.extract_commits(mock_session, "mozilla/firefox", 123)

        assert result == []


class TestExtractReviewers:
    """Tests for extract_reviewers function."""

    def test_fetch_reviewers(self, mock_session):
        """Test fetching reviewers for a PR."""
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

    def test_multiple_review_states(self, mock_session):
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

    def test_empty_reviewers_list(self, mock_session):
        """Test handling PR with no reviewers."""
        reviewers_response = Mock()
        reviewers_response.status_code = 200
        reviewers_response.json.return_value = []

        mock_session.get.return_value = reviewers_response

        result = main.extract_reviewers(mock_session, "mozilla/firefox", 123)

        assert result == []

    @patch("main.sleep_for_rate_limit")
    def test_rate_limit_handling(self, mock_sleep, mock_session):
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

    def test_api_error(self, mock_session):
        """Test API error handling when fetching reviewers."""
        error_response = Mock()
        error_response.status_code = 500
        error_response.text = "Internal Server Error"

        mock_session.get.return_value = error_response

        with pytest.raises(SystemExit) as exc_info:
            main.extract_reviewers(mock_session, "mozilla/firefox", 123)

        assert "GitHub API error 500" in str(exc_info.value)

    def test_custom_github_api_url(self, mock_session):
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


class TestExtractComments:
    """Tests for extract_comments function."""

    def test_fetch_comments(self, mock_session):
        """Test fetching comments for a PR."""
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

    def test_uses_issues_endpoint(self, mock_session):
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

    def test_multiple_comments(self, mock_session):
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

    def test_empty_comments_list(self, mock_session):
        """Test handling PR with no comments."""
        comments_response = Mock()
        comments_response.status_code = 200
        comments_response.json.return_value = []

        mock_session.get.return_value = comments_response

        result = main.extract_comments(mock_session, "mozilla/firefox", 123)

        assert result == []

    @patch("main.sleep_for_rate_limit")
    def test_rate_limit_handling(self, mock_sleep, mock_session):
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

    def test_api_error(self, mock_session):
        """Test API error handling when fetching comments."""
        error_response = Mock()
        error_response.status_code = 404
        error_response.text = "Not Found"

        mock_session.get.return_value = error_response

        with pytest.raises(SystemExit) as exc_info:
            main.extract_comments(mock_session, "mozilla/firefox", 123)

        assert "GitHub API error 404" in str(exc_info.value)

    def test_custom_github_api_url(self, mock_session):
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


class TestTransformData:
    """Tests for transform_data function."""

    def test_basic_pr_transformation(self):
        """Test basic pull request field mapping."""
        raw_data = [
            {
                "number": 123,
                "title": "Fix login bug",
                "state": "closed",
                "created_at": "2024-01-01T10:00:00Z",
                "updated_at": "2024-01-02T10:00:00Z",
                "merged_at": "2024-01-02T12:00:00Z",
                "labels": [],
                "commit_data": [],
                "reviewer_data": [],
                "comment_data": [],
            }
        ]

        result = main.transform_data(raw_data, "mozilla/firefox")

        assert len(result["pull_requests"]) == 1
        pr = result["pull_requests"][0]
        assert pr["pull_request_id"] == 123
        assert pr["current_status"] == "closed"
        assert pr["date_created"] == "2024-01-01T10:00:00Z"
        assert pr["date_modified"] == "2024-01-02T10:00:00Z"
        assert pr["date_landed"] == "2024-01-02T12:00:00Z"
        assert pr["target_repository"] == "mozilla/firefox"

    def test_bug_id_extraction_basic(self):
        """Test bug ID extraction from PR title."""
        test_cases = [
            ("Bug 1234567 - Fix issue", 1234567),
            ("bug 1234567: Update code", 1234567),
            ("Fix for bug 7654321", 7654321),
            ("b=9876543 - Change behavior", 9876543),
        ]

        for title, expected_bug_id in test_cases:
            raw_data = [
                {
                    "number": 1,
                    "title": title,
                    "state": "open",
                    "labels": [],
                    "commit_data": [],
                    "reviewer_data": [],
                    "comment_data": [],
                }
            ]

            result = main.transform_data(raw_data, "mozilla/firefox")
            assert result["pull_requests"][0]["bug_id"] == expected_bug_id

    def test_bug_id_extraction_with_hash(self):
        """Test bug ID extraction with # symbol."""
        raw_data = [
            {
                "number": 1,
                "title": "Bug #1234567 - Fix issue",
                "state": "open",
                "labels": [],
                "commit_data": [],
                "reviewer_data": [],
                "comment_data": [],
            }
        ]

        result = main.transform_data(raw_data, "mozilla/firefox")
        assert result["pull_requests"][0]["bug_id"] == 1234567

    def test_bug_id_filter_large_numbers(self):
        """Test that bug IDs >= 100000000 are filtered out."""
        raw_data = [
            {
                "number": 1,
                "title": "Bug 999999999 - Invalid bug ID",
                "state": "open",
                "labels": [],
                "commit_data": [],
                "reviewer_data": [],
                "comment_data": [],
            }
        ]

        result = main.transform_data(raw_data, "mozilla/firefox")
        assert result["pull_requests"][0]["bug_id"] is None

    def test_bug_id_no_match(self):
        """Test PR title with no bug ID."""
        raw_data = [
            {
                "number": 1,
                "title": "Update documentation",
                "state": "open",
                "labels": [],
                "commit_data": [],
                "reviewer_data": [],
                "comment_data": [],
            }
        ]

        result = main.transform_data(raw_data, "mozilla/firefox")
        assert result["pull_requests"][0]["bug_id"] is None

    def test_labels_extraction(self):
        """Test labels array extraction."""
        raw_data = [
            {
                "number": 1,
                "title": "PR with labels",
                "state": "open",
                "labels": [
                    {"name": "bug"},
                    {"name": "priority-high"},
                    {"name": "needs-review"},
                ],
                "commit_data": [],
                "reviewer_data": [],
                "comment_data": [],
            }
        ]

        result = main.transform_data(raw_data, "mozilla/firefox")
        labels = result["pull_requests"][0]["labels"]
        assert len(labels) == 3
        assert "bug" in labels
        assert "priority-high" in labels
        assert "needs-review" in labels

    def test_labels_empty_list(self):
        """Test handling empty labels list."""
        raw_data = [
            {
                "number": 1,
                "title": "PR without labels",
                "state": "open",
                "labels": [],
                "commit_data": [],
                "reviewer_data": [],
                "comment_data": [],
            }
        ]

        result = main.transform_data(raw_data, "mozilla/firefox")
        assert result["pull_requests"][0]["labels"] == []

    def test_commit_transformation(self):
        """Test commit fields mapping."""
        raw_data = [
            {
                "number": 123,
                "title": "PR with commits",
                "state": "open",
                "labels": [],
                "commit_data": [
                    {
                        "sha": "abc123",
                        "commit": {
                            "author": {
                                "name": "Test Author",
                                "date": "2024-01-01T12:00:00Z",
                            }
                        },
                        "files": [
                            {
                                "filename": "src/main.py",
                                "additions": 10,
                                "deletions": 5,
                            }
                        ],
                    }
                ],
                "reviewer_data": [],
                "comment_data": [],
            }
        ]

        result = main.transform_data(raw_data, "mozilla/firefox")

        assert len(result["commits"]) == 1
        commit = result["commits"][0]
        assert commit["pull_request_id"] == 123
        assert commit["target_repository"] == "mozilla/firefox"
        assert commit["commit_sha"] == "abc123"
        assert commit["date_created"] == "2024-01-01T12:00:00Z"
        assert commit["author_username"] == "Test Author"
        assert commit["filename"] == "src/main.py"
        assert commit["lines_added"] == 10
        assert commit["lines_removed"] == 5

    def test_commit_file_flattening(self):
        """Test that each file becomes a separate row."""
        raw_data = [
            {
                "number": 123,
                "title": "PR with multiple files",
                "state": "open",
                "labels": [],
                "commit_data": [
                    {
                        "sha": "abc123",
                        "commit": {"author": {"name": "Author", "date": "2024-01-01"}},
                        "files": [
                            {"filename": "file1.py", "additions": 10, "deletions": 5},
                            {"filename": "file2.py", "additions": 20, "deletions": 2},
                            {"filename": "file3.py", "additions": 5, "deletions": 15},
                        ],
                    }
                ],
                "reviewer_data": [],
                "comment_data": [],
            }
        ]

        result = main.transform_data(raw_data, "mozilla/firefox")

        # Should have 3 rows in commits table (one per file)
        assert len(result["commits"]) == 3
        filenames = [c["filename"] for c in result["commits"]]
        assert "file1.py" in filenames
        assert "file2.py" in filenames
        assert "file3.py" in filenames

    def test_multiple_commits_with_files(self):
        """Test multiple commits with multiple files per PR."""
        raw_data = [
            {
                "number": 123,
                "title": "PR with multiple commits",
                "state": "open",
                "labels": [],
                "commit_data": [
                    {
                        "sha": "commit1",
                        "commit": {"author": {"name": "Author1", "date": "2024-01-01"}},
                        "files": [
                            {"filename": "file1.py", "additions": 10, "deletions": 0}
                        ],
                    },
                    {
                        "sha": "commit2",
                        "commit": {"author": {"name": "Author2", "date": "2024-01-02"}},
                        "files": [
                            {"filename": "file2.py", "additions": 5, "deletions": 2},
                            {"filename": "file3.py", "additions": 8, "deletions": 3},
                        ],
                    },
                ],
                "reviewer_data": [],
                "comment_data": [],
            }
        ]

        result = main.transform_data(raw_data, "mozilla/firefox")

        # Should have 3 rows total (1 file from commit1, 2 files from commit2)
        assert len(result["commits"]) == 3
        assert result["commits"][0]["commit_sha"] == "commit1"
        assert result["commits"][1]["commit_sha"] == "commit2"
        assert result["commits"][2]["commit_sha"] == "commit2"

    def test_reviewer_transformation(self):
        """Test reviewer fields mapping."""
        raw_data = [
            {
                "number": 123,
                "title": "PR with reviewers",
                "state": "open",
                "labels": [],
                "commit_data": [],
                "reviewer_data": [
                    {
                        "id": 789,
                        "user": {"login": "reviewer1"},
                        "state": "APPROVED",
                        "submitted_at": "2024-01-01T15:00:00Z",
                    }
                ],
                "comment_data": [],
            }
        ]

        result = main.transform_data(raw_data, "mozilla/firefox")

        assert len(result["reviewers"]) == 1
        reviewer = result["reviewers"][0]
        assert reviewer["pull_request_id"] == 123
        assert reviewer["target_repository"] == "mozilla/firefox"
        assert reviewer["reviewer_username"] == "reviewer1"
        assert reviewer["status"] == "APPROVED"
        assert reviewer["date_reviewed"] == "2024-01-01T15:00:00Z"

    def test_multiple_review_states(self):
        """Test handling multiple review states."""
        raw_data = [
            {
                "number": 123,
                "title": "PR with multiple reviews",
                "state": "open",
                "labels": [],
                "commit_data": [],
                "reviewer_data": [
                    {
                        "id": 1,
                        "user": {"login": "user1"},
                        "state": "APPROVED",
                        "submitted_at": "2024-01-01T15:00:00Z",
                    },
                    {
                        "id": 2,
                        "user": {"login": "user2"},
                        "state": "CHANGES_REQUESTED",
                        "submitted_at": "2024-01-01T16:00:00Z",
                    },
                    {
                        "id": 3,
                        "user": {"login": "user3"},
                        "state": "COMMENTED",
                        "submitted_at": "2024-01-01T17:00:00Z",
                    },
                ],
                "comment_data": [],
            }
        ]

        result = main.transform_data(raw_data, "mozilla/firefox")

        assert len(result["reviewers"]) == 3
        states = [r["status"] for r in result["reviewers"]]
        assert "APPROVED" in states
        assert "CHANGES_REQUESTED" in states
        assert "COMMENTED" in states

    def test_date_approved_from_earliest_approval(self):
        """Test that date_approved is set to earliest APPROVED review."""
        raw_data = [
            {
                "number": 123,
                "title": "PR with multiple approvals",
                "state": "open",
                "labels": [],
                "commit_data": [],
                "reviewer_data": [
                    {
                        "id": 1,
                        "user": {"login": "user1"},
                        "state": "APPROVED",
                        "submitted_at": "2024-01-02T15:00:00Z",
                    },
                    {
                        "id": 2,
                        "user": {"login": "user2"},
                        "state": "APPROVED",
                        "submitted_at": "2024-01-01T14:00:00Z",  # Earliest
                    },
                    {
                        "id": 3,
                        "user": {"login": "user3"},
                        "state": "APPROVED",
                        "submitted_at": "2024-01-03T16:00:00Z",
                    },
                ],
                "comment_data": [],
            }
        ]

        result = main.transform_data(raw_data, "mozilla/firefox")

        pr = result["pull_requests"][0]
        assert pr["date_approved"] == "2024-01-01T14:00:00Z"

    def test_comment_transformation(self):
        """Test comment fields mapping."""
        raw_data = [
            {
                "number": 123,
                "title": "PR with comments",
                "state": "open",
                "labels": [],
                "commit_data": [],
                "reviewer_data": [],
                "comment_data": [
                    {
                        "id": 456,
                        "user": {"login": "commenter1"},
                        "body": "This looks great!",
                        "created_at": "2024-01-01T14:00:00Z",
                        "pull_request_review_id": None,
                    }
                ],
            }
        ]

        result = main.transform_data(raw_data, "mozilla/firefox")

        assert len(result["comments"]) == 1
        comment = result["comments"][0]
        assert comment["pull_request_id"] == 123
        assert comment["target_repository"] == "mozilla/firefox"
        assert comment["comment_id"] == 456
        assert comment["author_username"] == "commenter1"
        assert comment["date_created"] == "2024-01-01T14:00:00Z"
        assert comment["character_count"] == 17

    def test_comment_character_count(self):
        """Test character count calculation for comments."""
        raw_data = [
            {
                "number": 123,
                "title": "PR",
                "state": "open",
                "labels": [],
                "commit_data": [],
                "reviewer_data": [],
                "comment_data": [
                    {
                        "id": 1,
                        "user": {"login": "user1"},
                        "body": "Short",
                        "created_at": "2024-01-01",
                    },
                    {
                        "id": 2,
                        "user": {"login": "user2"},
                        "body": "This is a much longer comment with more text",
                        "created_at": "2024-01-01",
                    },
                ],
            }
        ]

        result = main.transform_data(raw_data, "mozilla/firefox")

        assert result["comments"][0]["character_count"] == 5
        assert result["comments"][1]["character_count"] == 44

    def test_comment_status_from_review(self):
        """Test that comment status is mapped from review_id_statuses."""
        raw_data = [
            {
                "number": 123,
                "title": "PR",
                "state": "open",
                "labels": [],
                "commit_data": [],
                "reviewer_data": [
                    {
                        "id": 789,
                        "user": {"login": "reviewer"},
                        "state": "APPROVED",
                        "submitted_at": "2024-01-01",
                    }
                ],
                "comment_data": [
                    {
                        "id": 456,
                        "user": {"login": "commenter"},
                        "body": "LGTM",
                        "created_at": "2024-01-01",
                        "pull_request_review_id": 789,
                    }
                ],
            }
        ]

        result = main.transform_data(raw_data, "mozilla/firefox")

        # Comment should have status from the review
        assert result["comments"][0]["status"] == "APPROVED"

    def test_comment_empty_body(self):
        """Test handling comments with empty or None body."""
        raw_data = [
            {
                "number": 123,
                "title": "PR",
                "state": "open",
                "labels": [],
                "commit_data": [],
                "reviewer_data": [],
                "comment_data": [
                    {
                        "id": 1,
                        "user": {"login": "user1"},
                        "body": None,
                        "created_at": "2024-01-01",
                    },
                    {
                        "id": 2,
                        "user": {"login": "user2"},
                        "body": "",
                        "created_at": "2024-01-01",
                    },
                ],
            }
        ]

        result = main.transform_data(raw_data, "mozilla/firefox")

        assert result["comments"][0]["character_count"] == 0
        assert result["comments"][1]["character_count"] == 0

    def test_empty_raw_data(self):
        """Test handling empty input list."""
        result = main.transform_data([], "mozilla/firefox")

        assert result["pull_requests"] == []
        assert result["commits"] == []
        assert result["reviewers"] == []
        assert result["comments"] == []

    def test_pr_without_commits_reviewers_comments(self):
        """Test PR with no commits, reviewers, or comments."""
        raw_data = [
            {
                "number": 123,
                "title": "Minimal PR",
                "state": "open",
                "labels": [],
                "commit_data": [],
                "reviewer_data": [],
                "comment_data": [],
            }
        ]

        result = main.transform_data(raw_data, "mozilla/firefox")

        assert len(result["pull_requests"]) == 1
        assert len(result["commits"]) == 0
        assert len(result["reviewers"]) == 0
        assert len(result["comments"]) == 0

    def test_return_structure(self):
        """Test that transform_data returns dict with 4 keys."""
        raw_data = [
            {
                "number": 1,
                "title": "Test",
                "state": "open",
                "labels": [],
                "commit_data": [],
                "reviewer_data": [],
                "comment_data": [],
            }
        ]

        result = main.transform_data(raw_data, "mozilla/firefox")

        assert isinstance(result, dict)
        assert "pull_requests" in result
        assert "commits" in result
        assert "reviewers" in result
        assert "comments" in result

    def test_all_tables_have_target_repository(self):
        """Test that all tables include target_repository field."""
        raw_data = [
            {
                "number": 123,
                "title": "Test PR",
                "state": "open",
                "labels": [],
                "commit_data": [
                    {
                        "sha": "abc",
                        "commit": {"author": {"name": "Author", "date": "2024-01-01"}},
                        "files": [
                            {"filename": "test.py", "additions": 1, "deletions": 0}
                        ],
                    }
                ],
                "reviewer_data": [
                    {
                        "id": 1,
                        "user": {"login": "reviewer"},
                        "state": "APPROVED",
                        "submitted_at": "2024-01-01",
                    }
                ],
                "comment_data": [
                    {
                        "id": 2,
                        "user": {"login": "commenter"},
                        "body": "Test",
                        "created_at": "2024-01-01",
                    }
                ],
            }
        ]

        result = main.transform_data(raw_data, "mozilla/firefox")

        assert result["pull_requests"][0]["target_repository"] == "mozilla/firefox"
        assert result["commits"][0]["target_repository"] == "mozilla/firefox"
        assert result["reviewers"][0]["target_repository"] == "mozilla/firefox"
        assert result["comments"][0]["target_repository"] == "mozilla/firefox"


class TestLoadData:
    """Tests for load_data function."""

    @patch("main.datetime")
    def test_load_all_tables(self, mock_datetime, mock_bigquery_client):
        """Test loading all 4 tables to BigQuery."""
        mock_datetime.now.return_value.strftime.return_value = "2024-01-15"

        transformed_data = {
            "pull_requests": [{"pull_request_id": 1}],
            "commits": [{"commit_sha": "abc"}],
            "reviewers": [{"reviewer_username": "user1"}],
            "comments": [{"comment_id": 123}],
        }

        main.load_data(mock_bigquery_client, "test_dataset", transformed_data)

        # Should call insert_rows_json 4 times (once per table)
        assert mock_bigquery_client.insert_rows_json.call_count == 4

    @patch("main.datetime")
    def test_adds_snapshot_date(self, mock_datetime, mock_bigquery_client):
        """Test that snapshot_date is added to all rows."""
        mock_datetime.now.return_value.strftime.return_value = "2024-01-15"

        transformed_data = {
            "pull_requests": [{"pull_request_id": 1}, {"pull_request_id": 2}],
            "commits": [],
            "reviewers": [],
            "comments": [],
        }

        main.load_data(mock_bigquery_client, "test_dataset", transformed_data)

        call_args = mock_bigquery_client.insert_rows_json.call_args
        rows = call_args[0][1]
        assert all(row["snapshot_date"] == "2024-01-15" for row in rows)

    def test_constructs_correct_table_ref(self, mock_bigquery_client):
        """Test that table_ref is constructed correctly."""
        transformed_data = {
            "pull_requests": [{"pull_request_id": 1}],
            "commits": [],
            "reviewers": [],
            "comments": [],
        }

        main.load_data(mock_bigquery_client, "my_dataset", transformed_data)

        call_args = mock_bigquery_client.insert_rows_json.call_args
        table_ref = call_args[0][0]
        assert table_ref == "test-project.my_dataset.pull_requests"

    def test_empty_transformed_data_skipped(self, mock_bigquery_client):
        """Test that empty transformed_data dict is skipped."""
        transformed_data = {}

        main.load_data(mock_bigquery_client, "test_dataset", transformed_data)

        mock_bigquery_client.insert_rows_json.assert_not_called()

    def test_skips_empty_tables_individually(self, mock_bigquery_client):
        """Test that empty tables are skipped individually."""
        transformed_data = {
            "pull_requests": [{"pull_request_id": 1}],
            "commits": [],  # Empty, should be skipped
            "reviewers": [],  # Empty, should be skipped
            "comments": [{"comment_id": 456}],
        }

        main.load_data(mock_bigquery_client, "test_dataset", transformed_data)

        # Should only call insert_rows_json twice (for PRs and comments)
        assert mock_bigquery_client.insert_rows_json.call_count == 2

    def test_only_pull_requests_table(self, mock_bigquery_client):
        """Test loading only pull_requests table."""
        transformed_data = {
            "pull_requests": [{"pull_request_id": 1}],
            "commits": [],
            "reviewers": [],
            "comments": [],
        }

        main.load_data(mock_bigquery_client, "test_dataset", transformed_data)

        assert mock_bigquery_client.insert_rows_json.call_count == 1

    def test_raises_exception_on_insert_errors(self, mock_bigquery_client):
        """Test that Exception is raised on BigQuery insert errors."""
        mock_bigquery_client.insert_rows_json.return_value = [
            {"index": 0, "errors": ["Insert failed"]}
        ]

        transformed_data = {
            "pull_requests": [{"pull_request_id": 1}],
            "commits": [],
            "reviewers": [],
            "comments": [],
        }

        with pytest.raises(Exception) as exc_info:
            main.load_data(mock_bigquery_client, "test_dataset", transformed_data)

        assert "BigQuery insert errors" in str(exc_info.value)

    def test_verifies_client_insert_called_correctly(self, mock_bigquery_client):
        """Test that client.insert_rows_json is called with correct arguments."""
        transformed_data = {
            "pull_requests": [{"pull_request_id": 1}, {"pull_request_id": 2}],
            "commits": [],
            "reviewers": [],
            "comments": [],
        }

        main.load_data(mock_bigquery_client, "test_dataset", transformed_data)

        call_args = mock_bigquery_client.insert_rows_json.call_args
        table_ref, rows = call_args[0]

        assert "pull_requests" in table_ref
        assert len(rows) == 2


class TestMain:
    """Tests for main function."""

    @patch("main.setup_logging")
    @patch("main.bigquery.Client")
    @patch("requests.Session")
    def test_requires_github_repos(
        self, mock_session_class, mock_bq_client, mock_setup_logging
    ):
        """Test that GITHUB_REPOS is required."""
        with patch.dict(
            os.environ,
            {"BIGQUERY_PROJECT": "test", "BIGQUERY_DATASET": "test"},
            clear=True,
        ):
            with pytest.raises(SystemExit) as exc_info:
                main.main()

            assert "GITHUB_REPOS" in str(exc_info.value)

    @patch("main.setup_logging")
    @patch("main.bigquery.Client")
    @patch("requests.Session")
    def test_requires_bigquery_project(
        self, mock_session_class, mock_bq_client, mock_setup_logging
    ):
        """Test that BIGQUERY_PROJECT is required."""
        with patch.dict(
            os.environ,
            {"GITHUB_REPOS": "mozilla/firefox", "BIGQUERY_DATASET": "test"},
            clear=True,
        ):
            with pytest.raises(SystemExit) as exc_info:
                main.main()

            assert "BIGQUERY_PROJECT" in str(exc_info.value)

    @patch("main.setup_logging")
    @patch("main.bigquery.Client")
    @patch("requests.Session")
    def test_requires_bigquery_dataset(
        self, mock_session_class, mock_bq_client, mock_setup_logging
    ):
        """Test that BIGQUERY_DATASET is required."""
        with patch.dict(
            os.environ,
            {"GITHUB_REPOS": "mozilla/firefox", "BIGQUERY_PROJECT": "test"},
            clear=True,
        ):
            with pytest.raises(SystemExit) as exc_info:
                main.main()

            assert "BIGQUERY_DATASET" in str(exc_info.value)

    @patch("main.setup_logging")
    @patch("main.bigquery.Client")
    @patch("requests.Session")
    def test_github_token_optional_with_warning(
        self, mock_session_class, mock_bq_client, mock_setup_logging
    ):
        """Test that GITHUB_TOKEN is optional but warns if missing."""
        with (
            patch.dict(
                os.environ,
                {
                    "GITHUB_REPOS": "mozilla/firefox",
                    "BIGQUERY_PROJECT": "test",
                    "BIGQUERY_DATASET": "test",
                },
                clear=True,
            ),
            patch("main.extract_pull_requests", return_value=iter([])),
        ):
            # Should not raise, but should log warning
            result = main.main()
            assert result == 0

    @patch("main.setup_logging")
    @patch("main.bigquery.Client")
    @patch("requests.Session")
    def test_splits_github_repos_by_comma(
        self, mock_session_class, mock_bq_client, mock_setup_logging
    ):
        """Test that GITHUB_REPOS is split by comma."""
        with (
            patch.dict(
                os.environ,
                {
                    "GITHUB_REPOS": "mozilla/firefox,mozilla/gecko-dev",
                    "BIGQUERY_PROJECT": "test",
                    "BIGQUERY_DATASET": "test",
                    "GITHUB_TOKEN": "token",
                },
                clear=True,
            ),
            patch("main.extract_pull_requests", return_value=iter([])) as mock_extract,
        ):
            main.main()

            # Should be called twice (once per repo)
            assert mock_extract.call_count == 2

    @patch("main.setup_logging")
    @patch("main.bigquery.Client")
    @patch("requests.Session")
    def test_honors_github_api_url(
        self, mock_session_class, mock_bq_client, mock_setup_logging
    ):
        """Test that GITHUB_API_URL is honored."""
        with (
            patch.dict(
                os.environ,
                {
                    "GITHUB_REPOS": "mozilla/firefox",
                    "BIGQUERY_PROJECT": "test",
                    "BIGQUERY_DATASET": "test",
                    "GITHUB_TOKEN": "token",
                    "GITHUB_API_URL": "https://custom-api.example.com",
                },
                clear=True,
            ),
            patch("main.extract_pull_requests", return_value=iter([])) as mock_extract,
        ):
            main.main()

            call_kwargs = mock_extract.call_args[1]
            assert call_kwargs["github_api_url"] == "https://custom-api.example.com"

    @patch("main.setup_logging")
    @patch("main.bigquery.Client")
    @patch("requests.Session")
    def test_honors_bigquery_emulator_host(
        self, mock_session_class, mock_bq_client_class, mock_setup_logging
    ):
        """Test that BIGQUERY_EMULATOR_HOST is honored."""
        with (
            patch.dict(
                os.environ,
                {
                    "GITHUB_REPOS": "mozilla/firefox",
                    "BIGQUERY_PROJECT": "test",
                    "BIGQUERY_DATASET": "test",
                    "GITHUB_TOKEN": "token",
                    "BIGQUERY_EMULATOR_HOST": "http://localhost:9050",
                },
                clear=True,
            ),
            patch("main.extract_pull_requests", return_value=iter([])),
        ):
            main.main()

            # Verify BigQuery client was created with emulator settings
            mock_bq_client_class.assert_called_once()

    @patch("main.setup_logging")
    @patch("main.bigquery.Client")
    @patch("requests.Session")
    def test_creates_session_with_headers(
        self, mock_session_class, mock_bq_client, mock_setup_logging
    ):
        """Test that session is created with Accept and User-Agent headers."""
        mock_session = MagicMock()
        mock_session_class.return_value = mock_session

        with (
            patch.dict(
                os.environ,
                {
                    "GITHUB_REPOS": "mozilla/firefox",
                    "BIGQUERY_PROJECT": "test",
                    "BIGQUERY_DATASET": "test",
                    "GITHUB_TOKEN": "token",
                },
                clear=True,
            ),
            patch("main.extract_pull_requests", return_value=iter([])),
        ):
            main.main()

            # Verify session headers were set
            assert mock_session.headers.update.called
            call_args = mock_session.headers.update.call_args[0][0]
            assert "Accept" in call_args
            assert "User-Agent" in call_args

    @patch("main.setup_logging")
    @patch("main.bigquery.Client")
    @patch("requests.Session")
    def test_sets_authorization_header_with_token(
        self, mock_session_class, mock_bq_client, mock_setup_logging
    ):
        """Test that Authorization header is set when token provided."""
        mock_session = MagicMock()
        mock_session_class.return_value = mock_session

        with (
            patch.dict(
                os.environ,
                {
                    "GITHUB_REPOS": "mozilla/firefox",
                    "BIGQUERY_PROJECT": "test",
                    "BIGQUERY_DATASET": "test",
                    "GITHUB_TOKEN": "test-token-123",
                },
                clear=True,
            ),
            patch("main.extract_pull_requests", return_value=iter([])),
        ):
            main.main()

            # Verify Authorization header was set
            assert mock_session.headers.__setitem__.called

    @patch("main.setup_logging")
    @patch("main.bigquery.Client")
    @patch("requests.Session")
    @patch("main.extract_pull_requests")
    @patch("main.transform_data")
    @patch("main.load_data")
    def test_single_repo_successful_etl(
        self,
        mock_load,
        mock_transform,
        mock_extract,
        mock_session_class,
        mock_bq_client,
        mock_setup_logging,
    ):
        """Test successful ETL for single repository."""
        mock_extract.return_value = iter([[{"number": 1}]])
        mock_transform.return_value = {
            "pull_requests": [{"pull_request_id": 1}],
            "commits": [],
            "reviewers": [],
            "comments": [],
        }

        with patch.dict(
            os.environ,
            {
                "GITHUB_REPOS": "mozilla/firefox",
                "BIGQUERY_PROJECT": "test",
                "BIGQUERY_DATASET": "test",
                "GITHUB_TOKEN": "token",
            },
            clear=True,
        ):
            result = main.main()

        assert result == 0
        mock_extract.assert_called_once()
        mock_transform.assert_called_once()
        mock_load.assert_called_once()

    @patch("main.setup_logging")
    @patch("main.bigquery.Client")
    @patch("requests.Session")
    @patch("main.extract_pull_requests")
    @patch("main.transform_data")
    @patch("main.load_data")
    def test_multiple_repos_processing(
        self,
        mock_load,
        mock_transform,
        mock_extract,
        mock_session_class,
        mock_bq_client,
        mock_setup_logging,
    ):
        """Test processing multiple repositories."""
        mock_extract.return_value = iter([[{"number": 1}]])
        mock_transform.return_value = {
            "pull_requests": [{"pull_request_id": 1}],
            "commits": [],
            "reviewers": [],
            "comments": [],
        }

        with patch.dict(
            os.environ,
            {
                "GITHUB_REPOS": "mozilla/firefox,mozilla/gecko-dev,mozilla/addons",
                "BIGQUERY_PROJECT": "test",
                "BIGQUERY_DATASET": "test",
                "GITHUB_TOKEN": "token",
            },
            clear=True,
        ):
            result = main.main()

        assert result == 0
        # Should process 3 repositories
        assert mock_extract.call_count == 3

    @patch("main.setup_logging")
    @patch("main.bigquery.Client")
    @patch("requests.Session")
    @patch("main.extract_pull_requests")
    @patch("main.transform_data")
    @patch("main.load_data")
    def test_processes_chunks_iteratively(
        self,
        mock_load,
        mock_transform,
        mock_extract,
        mock_session_class,
        mock_bq_client,
        mock_setup_logging,
    ):
        """Test that chunks are processed iteratively from generator."""
        # Return 3 chunks
        mock_extract.return_value = iter(
            [
                [{"number": 1}],
                [{"number": 2}],
                [{"number": 3}],
            ]
        )
        mock_transform.return_value = {
            "pull_requests": [{"pull_request_id": 1}],
            "commits": [],
            "reviewers": [],
            "comments": [],
        }

        with patch.dict(
            os.environ,
            {
                "GITHUB_REPOS": "mozilla/firefox",
                "BIGQUERY_PROJECT": "test",
                "BIGQUERY_DATASET": "test",
                "GITHUB_TOKEN": "token",
            },
            clear=True,
        ):
            result = main.main()

        assert result == 0
        # Transform and load should be called 3 times (once per chunk)
        assert mock_transform.call_count == 3
        assert mock_load.call_count == 3

    @patch("main.setup_logging")
    @patch("main.bigquery.Client")
    @patch("requests.Session")
    def test_returns_zero_on_success(
        self, mock_session_class, mock_bq_client, mock_setup_logging
    ):
        """Test that main returns 0 on success."""
        with (
            patch.dict(
                os.environ,
                {
                    "GITHUB_REPOS": "mozilla/firefox",
                    "BIGQUERY_PROJECT": "test",
                    "BIGQUERY_DATASET": "test",
                    "GITHUB_TOKEN": "token",
                },
                clear=True,
            ),
            patch("main.extract_pull_requests", return_value=iter([])),
        ):
            result = main.main()

        assert result == 0


@pytest.mark.integration
class TestIntegration:
    """Integration tests that test multiple components together."""

    @patch("main.setup_logging")
    @patch("main.load_data")
    @patch("main.bigquery.Client")
    @patch("requests.Session")
    def test_end_to_end_with_mocked_github(
        self, mock_session_class, mock_bq_client, mock_load, mock_setup_logging
    ):
        """Test end-to-end flow with mocked GitHub responses."""
        mock_session = MagicMock()
        mock_session_class.return_value = mock_session

        # Mock PR response
        pr_response = Mock()
        pr_response.status_code = 200
        pr_response.json.return_value = [
            {"number": 1, "title": "Bug 1234567 - Test PR", "state": "open"}
        ]
        pr_response.links = {}

        # Mock commits, reviewers, comments responses
        empty_response = Mock()
        empty_response.status_code = 200
        empty_response.json.return_value = []

        mock_session.get.side_effect = [
            pr_response,
            empty_response,
            empty_response,
            empty_response,
        ]

        with patch.dict(
            os.environ,
            {
                "GITHUB_REPOS": "mozilla/firefox",
                "BIGQUERY_PROJECT": "test",
                "BIGQUERY_DATASET": "test",
                "GITHUB_TOKEN": "token",
            },
            clear=True,
        ):
            result = main.main()

        assert result == 0
        mock_load.assert_called_once()

        # Verify transformed data structure
        call_args = mock_load.call_args[0]
        transformed_data = call_args[2]
        assert "pull_requests" in transformed_data
        assert len(transformed_data["pull_requests"]) == 1

    @patch("main.setup_logging")
    @patch("main.load_data")
    @patch("main.bigquery.Client")
    @patch("requests.Session")
    def test_bug_id_extraction_through_pipeline(
        self, mock_session_class, mock_bq_client, mock_load, mock_setup_logging
    ):
        """Test bug ID extraction through full pipeline."""
        mock_session = MagicMock()
        mock_session_class.return_value = mock_session

        pr_response = Mock()
        pr_response.status_code = 200
        pr_response.json.return_value = [
            {
                "number": 1,
                "title": "Bug 9876543 - Fix critical issue",
                "state": "closed",
            }
        ]
        pr_response.links = {}

        empty_response = Mock()
        empty_response.status_code = 200
        empty_response.json.return_value = []

        mock_session.get.side_effect = [
            pr_response,
            empty_response,
            empty_response,
            empty_response,
        ]

        with patch.dict(
            os.environ,
            {
                "GITHUB_REPOS": "mozilla/firefox",
                "BIGQUERY_PROJECT": "test",
                "BIGQUERY_DATASET": "test",
                "GITHUB_TOKEN": "token",
            },
            clear=True,
        ):
            main.main()

        call_args = mock_load.call_args[0]
        transformed_data = call_args[2]
        pr = transformed_data["pull_requests"][0]
        assert pr["bug_id"] == 9876543

    @patch("main.setup_logging")
    @patch("main.load_data")
    @patch("main.bigquery.Client")
    @patch("requests.Session")
    def test_pagination_through_full_flow(
        self, mock_session_class, mock_bq_client, mock_load, mock_setup_logging
    ):
        """Test pagination through full ETL flow."""
        mock_session = MagicMock()
        mock_session_class.return_value = mock_session

        # First page
        pr_response_1 = Mock()
        pr_response_1.status_code = 200
        pr_response_1.json.return_value = [
            {"number": 1, "title": "PR 1", "state": "open"}
        ]
        pr_response_1.links = {
            "next": {"url": "https://api.github.com/repos/mozilla/firefox/pulls?page=2"}
        }

        # Second page
        pr_response_2 = Mock()
        pr_response_2.status_code = 200
        pr_response_2.json.return_value = [
            {"number": 2, "title": "PR 2", "state": "open"}
        ]
        pr_response_2.links = {}

        empty_response = Mock()
        empty_response.status_code = 200
        empty_response.json.return_value = []

        mock_session.get.side_effect = [
            pr_response_1,
            empty_response,
            empty_response,
            empty_response,
            pr_response_2,
            empty_response,
            empty_response,
            empty_response,
        ]

        with patch.dict(
            os.environ,
            {
                "GITHUB_REPOS": "mozilla/firefox",
                "BIGQUERY_PROJECT": "test",
                "BIGQUERY_DATASET": "test",
                "GITHUB_TOKEN": "token",
            },
            clear=True,
        ):
            main.main()

        # Should be called twice (once per chunk/page)
        assert mock_load.call_count == 2
