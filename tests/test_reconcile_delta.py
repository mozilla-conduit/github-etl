from datetime import date
from unittest.mock import patch

import main


def _scalar_params(job_config) -> dict:
    """Flatten scalar parameters of a QueryJobConfig into {name: (type_, value)}."""
    from google.cloud import bigquery

    return {
        p.name: (p.type_, p.value)
        for p in job_config.query_parameters
        if isinstance(p, bigquery.ScalarQueryParameter)
    }


def _array_param(job_config, name):
    """Return the ArrayQueryParameter with the given name, or None."""
    from google.cloud import bigquery

    for p in job_config.query_parameters:
        if isinstance(p, bigquery.ArrayQueryParameter) and p.name == name:
            return p
    return None


def _delta(pr_ids):
    """Build a minimal transformed-delta dict with the given pull_request_ids."""
    return {
        "pull_requests": [{"pull_request_id": i} for i in pr_ids],
        "commits": [],
        "reviewers": [],
        "comments": [],
    }


def test_reconcile_empty_delta_is_noop(mock_bigquery_client):
    """An empty delta runs no DELETE and no load."""
    with patch("main.load_data") as mock_load:
        main.reconcile_delta(
            mock_bigquery_client,
            "test_dataset",
            "mozilla/firefox",
            _delta([]),
            "2026-06-20",
        )

    mock_bigquery_client.query.assert_not_called()
    mock_load.assert_not_called()


def test_reconcile_deletes_per_table(mock_bigquery_client):
    """One DELETE ... IN UNNEST runs per table, each targeting that table."""
    with patch("main.load_data"):
        main.reconcile_delta(
            mock_bigquery_client,
            "test_dataset",
            "mozilla/firefox",
            _delta([1, 2]),
            "2026-06-20",
        )

    assert mock_bigquery_client.query.call_count == 4

    seen = set()
    for call in mock_bigquery_client.query.call_args_list:
        sql = call.args[0]
        assert "DELETE FROM" in sql
        assert "IN UNNEST(@pr_ids)" in sql
        for table in main._TABLE_COLUMNS:
            if f"test-project.test_dataset.{table}`" in sql:
                seen.add(table)

    assert seen == set(main._TABLE_COLUMNS)


def test_reconcile_binds_params(mock_bigquery_client):
    """Each DELETE binds snapshot_date, repo, and the pr_ids array."""
    with patch("main.load_data"):
        main.reconcile_delta(
            mock_bigquery_client,
            "test_dataset",
            "mozilla/firefox",
            _delta([2, 1]),
            "2026-06-20",
        )

    for call in mock_bigquery_client.query.call_args_list:
        job_config = call.kwargs["job_config"]
        scalars = _scalar_params(job_config)
        # ScalarQueryParameter coerces the "DATE"-typed string into a datetime.date.
        assert scalars["snapshot_date"] == ("DATE", date(2026, 6, 20))
        assert scalars["repo"] == ("STRING", "mozilla/firefox")

        pr_ids = _array_param(job_config, "pr_ids")
        assert pr_ids is not None
        assert pr_ids.array_type == "INT64"
        assert pr_ids.values == [1, 2]


def test_reconcile_dedupes_and_filters_pr_ids(mock_bigquery_client):
    """Duplicate ids are collapsed and None pull_request_ids are dropped."""
    delta = {
        "pull_requests": [
            {"pull_request_id": 5},
            {"pull_request_id": 5},
            {"pull_request_id": None},
            {"pull_request_id": 3},
            {"title": "no id field"},
        ],
        "commits": [],
        "reviewers": [],
        "comments": [],
    }

    with patch("main.load_data"):
        main.reconcile_delta(
            mock_bigquery_client, "test_dataset", "mozilla/firefox", delta, "2026-06-20"
        )

    pr_ids = _array_param(
        mock_bigquery_client.query.call_args_list[0].kwargs["job_config"], "pr_ids"
    )
    assert pr_ids.values == [3, 5]


def test_reconcile_loads_delta_after_deletes(mock_bigquery_client):
    """After the deletes, load_data inserts the fresh delta with the toggle."""
    delta = _delta([1])

    with patch("main.load_data") as mock_load:
        main.reconcile_delta(
            mock_bigquery_client,
            "test_dataset",
            "mozilla/firefox",
            delta,
            "2026-06-20",
            use_streaming_insert=True,
        )

    mock_load.assert_called_once_with(
        mock_bigquery_client,
        "test_dataset",
        delta,
        "2026-06-20",
        use_streaming_insert=True,
    )
