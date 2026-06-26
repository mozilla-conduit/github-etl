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
    """Test that extract_pull_requests raises TooManyRetriesError on 404."""
    mock_response = Mock()
    mock_response.status_code = 404
    mock_response.text = "Not Found"
    mock_response.headers = {"Content-Type": "application/json"}

    mock_session.get.return_value = mock_response

    with pytest.raises(main.TooManyRetriesError) as exc_info:
        list(main.extract_pull_requests(mock_session, "mozilla/nonexistent"))

    assert "GitHub API error 404" in str(exc_info.value)


@patch("time.sleep")
def test_handles_api_error_500(mock_sleep, mock_session):
    """Test that extract_pull_requests retries on 500, then raises TooManyRetriesError."""
    mock_response = Mock()
    mock_response.status_code = 500
    mock_response.text = "Internal Server Error"
    mock_response.headers = {"Content-Type": "application/json"}

    mock_session.get.return_value = mock_response

    with pytest.raises(main.TooManyRetriesError) as exc_info:
        list(main.extract_pull_requests(mock_session, "mozilla/firefox"))

    assert "GitHub API error 500" in str(exc_info.value)
    assert mock_session.get.call_count == main._MAX_RETRIES + 1


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


def test_default_mode_sorts_by_created_ascending(mock_session):
    """Without `since`, the request keeps the original created/asc sort."""
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = [{"number": 1, "title": "PR 1"}]
    mock_response.links = {}

    mock_session.get.return_value = mock_response

    with (
        patch("main.extract_commits", return_value=[]),
        patch("main.extract_reviewers", return_value=[]),
        patch("main.extract_comments", return_value=[]),
    ):
        list(main.extract_pull_requests(mock_session, "mozilla/firefox"))

    params = mock_session.get.call_args[1]["params"]
    assert params["sort"] == "created"
    assert params["direction"] == "asc"


def test_incremental_mode_sorts_by_updated_descending(mock_session):
    """With `since`, the request switches to updated/desc for incremental sync."""
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = [
        {"number": 1, "title": "PR 1", "updated_at": "2026-06-17T12:00:00Z"},
    ]
    mock_response.links = {}

    mock_session.get.return_value = mock_response

    with (
        patch("main.extract_commits", return_value=[]),
        patch("main.extract_reviewers", return_value=[]),
        patch("main.extract_comments", return_value=[]),
    ):
        list(
            main.extract_pull_requests(
                mock_session, "mozilla/firefox", since="2026-06-01T00:00:00Z"
            )
        )

    params = mock_session.get.call_args[1]["params"]
    assert params["sort"] == "updated"
    assert params["direction"] == "desc"


def test_incremental_stops_at_watermark(mock_session):
    """PRs older than `since` are dropped and pagination stops at the watermark."""
    # Page sorted by updated_at descending: two PRs newer than `since`, then one
    # older. The older PR marks the watermark — it and anything after must stop.
    mock_response_1 = Mock()
    mock_response_1.status_code = 200
    mock_response_1.json.return_value = [
        {"number": 3, "updated_at": "2026-06-17T12:00:00Z"},
        {"number": 2, "updated_at": "2026-06-10T09:00:00Z"},
        {"number": 1, "updated_at": "2026-05-01T09:00:00Z"},  # older than `since`
    ]
    mock_response_1.links = {
        "next": {"url": "https://api.github.com/repos/mozilla/firefox/pulls?page=2"}
    }

    mock_session.get.side_effect = [mock_response_1]

    with (
        patch("main.extract_commits", return_value=[]),
        patch("main.extract_reviewers", return_value=[]),
        patch("main.extract_comments", return_value=[]),
    ):
        result = list(
            main.extract_pull_requests(
                mock_session, "mozilla/firefox", since="2026-06-01T00:00:00Z"
            )
        )

    # Only the two PRs at-or-after the watermark are yielded...
    assert len(result) == 1
    assert [pr["number"] for pr in result[0]] == [3, 2]
    # ...and the second page is never fetched despite the `next` link.
    assert mock_session.get.call_count == 1


def test_incremental_boundary_is_inclusive(mock_session):
    """A PR whose updated_at equals `since` is kept (boundary is inclusive)."""
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = [
        {"number": 1, "updated_at": "2026-06-01T00:00:00Z"},  # exactly `since`
    ]
    mock_response.links = {}

    mock_session.get.return_value = mock_response

    with (
        patch("main.extract_commits", return_value=[]),
        patch("main.extract_reviewers", return_value=[]),
        patch("main.extract_comments", return_value=[]),
    ):
        result = list(
            main.extract_pull_requests(
                mock_session, "mozilla/firefox", since="2026-06-01T00:00:00Z"
            )
        )

    assert len(result) == 1
    assert result[0][0]["number"] == 1


def test_incremental_subfetches_only_kept_prs(mock_session):
    """Sub-fetches run only for PRs newer than `since`, not the older ones."""
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = [
        {"number": 2, "updated_at": "2026-06-17T12:00:00Z"},
        {"number": 1, "updated_at": "2026-05-01T09:00:00Z"},  # older than `since`
    ]
    mock_response.links = {}

    mock_session.get.return_value = mock_response

    with (
        patch("main.extract_commits", return_value=[]) as mock_commits,
        patch("main.extract_reviewers", return_value=[]),
        patch("main.extract_comments", return_value=[]),
    ):
        result = list(
            main.extract_pull_requests(
                mock_session, "mozilla/firefox", since="2026-06-01T00:00:00Z"
            )
        )

    # Only the kept PR (#2) is enriched; the older PR (#1) is never sub-fetched.
    assert [pr["number"] for pr in result[0]] == [2]
    assert mock_commits.call_count == 1


def test_incremental_keeps_pr_with_missing_updated_at(mock_session):
    """A PR with no updated_at is kept and does not trigger the watermark stop."""
    mock_response_1 = Mock()
    mock_response_1.status_code = 200
    mock_response_1.json.return_value = [
        {"number": 2, "updated_at": "2026-06-17T12:00:00Z"},
        {"number": 1},  # missing updated_at — must be kept, must not stop paging
    ]
    mock_response_1.links = {
        "next": {"url": "https://api.github.com/repos/mozilla/firefox/pulls?page=2"}
    }

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
        result = list(
            main.extract_pull_requests(
                mock_session, "mozilla/firefox", since="2026-06-01T00:00:00Z"
            )
        )

    assert [pr["number"] for pr in result[0]] == [2, 1]
    # Pagination continued past the missing-timestamp PR to the (empty) page 2.
    assert mock_session.get.call_count == 2
