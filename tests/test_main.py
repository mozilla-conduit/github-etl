import os
import threading
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
def test_runs_without_auth_credentials(
    mock_session_class, mock_bq_client, mock_setup_logging
):
    """Test that auth credentials are optional; script runs with a warning."""
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
                "BIGQUERY_EMULATOR_HOST": "http://localhost:9050",
            },
            clear=True,
        ),
        patch("main.extract_pull_requests", return_value=iter([])),
    ):
        main.main()


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
        },
        clear=True,
    ):
        result = main.main()

    assert result == 0
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
        },
        clear=True,
    ):
        main.main()

    # Should be called twice (once per chunk/page)
    assert mock_load.call_count == 2


@patch("main.setup_logging")
@patch("main.bigquery.Client")
@patch("requests.Session")
@patch("main.extract_pull_requests")
@patch("main.transform_data")
@patch("main.load_data")
def test_repo_failure_continues_to_next_repo(
    mock_load,
    mock_transform,
    mock_extract,
    mock_session_class,
    mock_bq_client,
    mock_setup_logging,
):
    """A fatal error on one repo should not prevent other repos from being processed."""

    def extract_side_effect(*args, **kwargs):
        repo = args[1]
        if repo == "mozilla/firefox":
            raise main.TooManyRetriesError(
                "GitHub API error 502 for https://api.github.com/..."
            )
        return iter([[{"number": 1}]])

    mock_extract.side_effect = extract_side_effect
    mock_transform.return_value = {
        "pull_requests": [{"pull_request_id": 1}],
        "commits": [],
        "reviewers": [],
        "comments": [],
    }

    with patch.dict(
        os.environ,
        {
            "GITHUB_REPOS": "mozilla/firefox,mozilla/gecko-dev",
            "BIGQUERY_PROJECT": "test",
            "BIGQUERY_DATASET": "test",
        },
        clear=True,
    ):
        result = main.main()

    assert result == 1  # partial failure
    assert mock_extract.call_count == 2  # both repos were attempted
    mock_load.assert_called_once()  # only the successful repo loaded data


@patch("main.setup_logging")
@patch("main.bigquery.Client")
@patch("requests.Session")
@patch("main.extract_pull_requests")
@patch("main.transform_data")
@patch("main.load_data")
def test_bare_exception_on_one_repo_is_isolated(
    mock_load,
    mock_transform,
    mock_extract,
    mock_session_class,
    mock_bq_client,
    mock_setup_logging,
):
    """A bare Exception (e.g. from load_data) on one repo must not abort the others.

    The executor catches broadly, so the failing repo is recorded in failed_repos
    (overall exit code 1) while the healthy repo still completes its load.
    """
    # Fresh iterator per repo (a shared return_value iterator would be exhausted
    # by whichever repo consumes it first).
    mock_extract.side_effect = lambda *a, **k: iter([[{"number": 1}]])

    def load_side_effect(client, dataset, transformed, *args, **kwargs):
        # Fail only for firefox; gecko-dev should still load successfully.
        if transformed["pull_requests"][0].get("repo_marker") == "fail":
            raise Exception("BigQuery insert errors for table pull_requests")

    # Tag the transform output per repo so load_side_effect can decide which fails.
    def transform_side_effect(chunk, repo):
        marker = "fail" if repo == "mozilla/firefox" else "ok"
        return {
            "pull_requests": [{"pull_request_id": 1, "repo_marker": marker}],
            "commits": [],
            "reviewers": [],
            "comments": [],
        }

    mock_transform.side_effect = transform_side_effect
    mock_load.side_effect = load_side_effect

    with patch.dict(
        os.environ,
        {
            "GITHUB_REPOS": "mozilla/firefox,mozilla/gecko-dev",
            "BIGQUERY_PROJECT": "test",
            "BIGQUERY_DATASET": "test",
        },
        clear=True,
    ):
        result = main.main()

    assert result == 1  # partial failure recorded, run did not abort
    assert mock_extract.call_count == 2  # both repos were attempted
    assert mock_load.call_count == 2  # both repos reached the load step


class TestResolveMaxWorkers:
    """Tests for _resolve_max_workers worker-count resolution."""

    def test_defaults_to_repo_count_when_below_cap(self):
        with patch.dict(os.environ, {}, clear=True):
            assert main._resolve_max_workers(3) == 3

    def test_caps_at_default_for_large_repo_lists(self):
        with patch.dict(os.environ, {}, clear=True):
            assert main._resolve_max_workers(100) == main._DEFAULT_MAX_WORKERS

    def test_never_returns_less_than_one(self):
        with patch.dict(os.environ, {}, clear=True):
            assert main._resolve_max_workers(0) == 1

    def test_env_override_raises_cap(self):
        with patch.dict(os.environ, {"GITHUB_ETL_MAX_WORKERS": "20"}, clear=True):
            assert main._resolve_max_workers(15) == 15

    def test_env_override_still_bounded_by_repo_count(self):
        with patch.dict(os.environ, {"GITHUB_ETL_MAX_WORKERS": "20"}, clear=True):
            assert main._resolve_max_workers(2) == 2

    def test_invalid_env_override_falls_back_to_default(self):
        with patch.dict(os.environ, {"GITHUB_ETL_MAX_WORKERS": "abc"}, clear=True):
            assert main._resolve_max_workers(100) == main._DEFAULT_MAX_WORKERS

    def test_non_positive_env_override_falls_back_to_default(self):
        with patch.dict(os.environ, {"GITHUB_ETL_MAX_WORKERS": "0"}, clear=True):
            assert main._resolve_max_workers(100) == main._DEFAULT_MAX_WORKERS


@patch("main.setup_logging")
@patch("main.bigquery.Client")
@patch("requests.Session")
@patch("main.transform_data")
@patch("main.load_data")
def test_repos_are_processed_concurrently(
    mock_load,
    mock_transform,
    mock_session_class,
    mock_bq_client,
    mock_setup_logging,
):
    """Repos run in parallel: a barrier that all repos must reach proves overlap.

    If processing were sequential, the first repo would block forever at the
    barrier (the others never start), so barrier.wait() would time out and the
    test would fail with BrokenBarrierError.
    """
    repos = "mozilla/firefox,mozilla/gecko-dev,mozilla/addons"
    num_repos = len(repos.split(","))
    barrier = threading.Barrier(num_repos, timeout=5)

    def extract_side_effect(*args, **kwargs):
        # Every repo's worker must reach the barrier before any may proceed.
        barrier.wait()
        return iter([[{"number": 1}]])

    mock_transform.return_value = {
        "pull_requests": [{"pull_request_id": 1}],
        "commits": [],
        "reviewers": [],
        "comments": [],
    }

    with (
        patch.dict(
            os.environ,
            {
                "GITHUB_REPOS": repos,
                "BIGQUERY_PROJECT": "test",
                "BIGQUERY_DATASET": "test",
            },
            clear=True,
        ),
        patch("main.extract_pull_requests", side_effect=extract_side_effect),
    ):
        result = main.main()

    assert result == 0
    assert not barrier.broken  # all repos reached the barrier => true concurrency
    assert mock_load.call_count == num_repos
