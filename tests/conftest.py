"""
Pytest fixtures for GitHub ETL tests.

This module provides reusable test fixtures for mocking external dependencies
and providing sample data for unit and integration tests.
"""

from datetime import datetime, timezone
from typing import Any
from unittest.mock import MagicMock, Mock

import pytest
import requests
from google.cloud import bigquery


@pytest.fixture
def mock_env_vars(monkeypatch) -> dict[str, str]:
    """
    Set up common environment variables for tests.

    Returns:
        Dictionary of environment variables that were set
    """
    env_vars = {
        "GITHUB_TOKEN": "test_token_123",
        "GITHUB_REPOS": "mozilla/firefox",
        "BIGQUERY_PROJECT": "test-project",
        "BIGQUERY_DATASET": "test_dataset",
    }
    for key, value in env_vars.items():
        monkeypatch.setenv(key, value)
    return env_vars


@pytest.fixture
def sample_github_pr() -> dict[str, Any]:
    """
    Sample GitHub pull request data from API response.

    Returns:
        Dictionary representing a single PR from GitHub API
    """
    return {
        "number": 12345,
        "state": "closed",
        "title": "Bug 1234567 - Fix memory leak in parser",
        "created_at": "2025-01-01T10:00:00Z",
        "updated_at": "2025-01-02T15:30:00Z",
        "merged_at": "2025-01-02T15:30:00Z",
        "labels": [
            {"name": "bug"},
            {"name": "priority-high"},
        ],
        "user": {
            "login": "test_user",
            "id": 123,
        },
        "head": {
            "ref": "feature-branch",
            "sha": "abc123",
        },
        "base": {
            "ref": "main",
            "sha": "def456",
        },
        "commit_data": [],
        "reviewer_data": [],
        "comment_data": [],
    }


@pytest.fixture
def sample_github_commit() -> dict[str, Any]:
    """
    Sample GitHub commit data from API response.

    Returns:
        Dictionary representing a single commit from GitHub API
    """
    return {
        "sha": "abc123def456",
        "commit": {
            "author": {
                "name": "Test Author",
                "email": "author@example.com",
                "date": "2025-01-01T10:00:00Z",
            },
            "message": "Fix bug in parser",
        },
        "files": [
            {
                "filename": "src/parser.py",
                "additions": 10,
                "deletions": 5,
                "changes": 15,
            }
        ],
    }


@pytest.fixture
def sample_github_reviewer() -> dict[str, Any]:
    """
    Sample GitHub review data from API response.

    Returns:
        Dictionary representing a single review from GitHub API
    """
    return {
        "id": 98765,
        "user": {
            "login": "reviewer_user",
            "id": 456,
        },
        "state": "APPROVED",
        "submitted_at": "2025-01-02T12:00:00Z",
        "body": "LGTM",
    }


@pytest.fixture
def sample_github_comment() -> dict[str, Any]:
    """
    Sample GitHub comment data from API response.

    Returns:
        Dictionary representing a single comment from GitHub API
    """
    return {
        "id": 111222,
        "user": {
            "login": "commenter_user",
            "id": 789,
        },
        "created_at": "2025-01-01T14:00:00Z",
        "body": "Please check the edge case for null values",
        "pull_request_review_id": None,
    }


@pytest.fixture
def sample_transformed_data() -> dict[str, list[dict]]:
    """
    Sample transformed data ready for BigQuery insertion.

    Returns:
        Dictionary with keys for each table and transformed row data
    """
    return {
        "pull_requests": [
            {
                "pull_request_id": 12345,
                "current_status": "closed",
                "date_created": "2025-01-01T10:00:00Z",
                "date_modified": "2025-01-02T15:30:00Z",
                "target_repository": "mozilla/firefox",
                "bug_id": 1234567,
                "date_landed": "2025-01-02T15:30:00Z",
                "date_approved": "2025-01-02T12:00:00Z",
                "labels": ["bug", "priority-high"],
            }
        ],
        "commits": [
            {
                "pull_request_id": 12345,
                "target_repository": "mozilla/firefox",
                "commit_sha": "abc123def456",
                "date_created": "2025-01-01T10:00:00Z",
                "author_username": "Test Author",
                "author_email": None,
                "filename": "src/parser.py",
                "lines_removed": 5,
                "lines_added": 10,
            }
        ],
        "reviewers": [
            {
                "pull_request_id": 12345,
                "target_repository": "mozilla/firefox",
                "date_reviewed": "2025-01-02T12:00:00Z",
                "reviewer_email": None,
                "reviewer_username": "reviewer_user",
                "status": "APPROVED",
            }
        ],
        "comments": [
            {
                "pull_request_id": 12345,
                "target_repository": "mozilla/firefox",
                "comment_id": 111222,
                "date_created": "2025-01-01T14:00:00Z",
                "author_email": None,
                "author_username": "commenter_user",
                "character_count": 43,
                "status": None,
            }
        ],
    }


@pytest.fixture
def mock_session() -> Mock:
    """
    Mock requests.Session with configurable responses.

    Returns:
        Mock session object with get() method
    """
    session = Mock(spec=requests.Session)
    session.headers = {}
    return session


@pytest.fixture
def mock_github_response() -> Mock:
    """
    Mock requests.Response for GitHub API calls.

    Returns:
        Mock response with status_code, json(), headers, and links
    """
    response = Mock(spec=requests.Response)
    response.status_code = 200
    response.headers = {
        "X-RateLimit-Remaining": "5000",
        "X-RateLimit-Reset": "1609459200",
    }
    response.links = {}
    response.text = ""
    return response


@pytest.fixture
def mock_rate_limited_response() -> Mock:
    """
    Mock requests.Response simulating rate limit exceeded.

    Returns:
        Mock response with 403 status and rate limit headers
    """
    response = Mock(spec=requests.Response)
    response.status_code = 403
    response.headers = {
        "X-RateLimit-Remaining": "0",
        "X-RateLimit-Reset": str(int(datetime.now(timezone.utc).timestamp()) + 3600),
    }
    response.text = "API rate limit exceeded"
    return response


@pytest.fixture
def mock_bigquery_client() -> Mock:
    """
    Mock BigQuery client for testing load operations.

    Returns:
        Mock BigQuery client with insert_rows_json() method
    """
    client = Mock(spec=bigquery.Client)
    client.project = "test-project"
    client.insert_rows_json = MagicMock(return_value=[])  # Empty list = no errors
    return client


@pytest.fixture
def mock_bigquery_client_with_errors() -> Mock:
    """
    Mock BigQuery client that returns insertion errors.

    Returns:
        Mock BigQuery client that simulates insert failures
    """
    client = Mock(spec=bigquery.Client)
    client.project = "test-project"
    client.insert_rows_json = MagicMock(
        return_value=[
            {
                "index": 0,
                "errors": [{"reason": "invalid", "message": "Invalid schema"}],
            }
        ]
    )
    return client
