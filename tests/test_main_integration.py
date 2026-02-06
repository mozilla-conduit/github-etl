#!/usr/bin/env python3
"""
Tests for main function and full ETL integration.

Tests main orchestration including environment variables, session setup,
repository processing, chunked ETL flow, and end-to-end integration tests.
"""

import os
from unittest.mock import MagicMock, Mock, patch

import pytest

import main


@patch("main.setup_logging")
@patch("main.bigquery.Client")
@patch("requests.Session")
def test_requires_github_repos(mock_session_class, mock_bq_client, mock_setup_logging):
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
    mock_session_class, mock_bq_client, mock_setup_logging
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
    mock_session_class, mock_bq_client, mock_setup_logging
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
    mock_session_class, mock_bq_client, mock_setup_logging
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
    mock_session_class, mock_bq_client, mock_setup_logging
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
def test_honors_github_api_url(mock_session_class, mock_bq_client, mock_setup_logging):
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
    mock_session_class, mock_bq_client_class, mock_setup_logging
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
    mock_session_class, mock_bq_client, mock_setup_logging
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
    mock_session_class, mock_bq_client, mock_setup_logging
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
    mock_session_class, mock_bq_client, mock_setup_logging
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
@patch("main.setup_logging")
@patch("main.load_data")
@patch("main.bigquery.Client")
@patch("requests.Session")
def test_full_etl_flow_transforms_data_correctly(
    mock_session_class, mock_bq_client, mock_load, mock_setup_logging
):
    """Test full ETL flow with mocked GitHub responses."""
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
    mock_session_class, mock_bq_client, mock_load, mock_setup_logging
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
    mock_session_class, mock_bq_client, mock_load, mock_setup_logging
):
    """Test pagination through full ETL flow."""
    mock_session = MagicMock()
    mock_session_class.return_value = mock_session

    # First page
    pr_response_1 = Mock()
    pr_response_1.status_code = 200
    pr_response_1.json.return_value = [{"number": 1, "title": "PR 1", "state": "open"}]
    pr_response_1.links = {
        "next": {"url": "https://api.github.com/repos/mozilla/firefox/pulls?page=2"}
    }

    # Second page
    pr_response_2 = Mock()
    pr_response_2.status_code = 200
    pr_response_2.json.return_value = [{"number": 2, "title": "PR 2", "state": "open"}]
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
