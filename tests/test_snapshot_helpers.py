from datetime import date, datetime, timezone
from types import SimpleNamespace

import pytest
from google.api_core import exceptions as api_exceptions

import main


def _params(job_config) -> dict:
    """Flatten a QueryJobConfig's parameters into {name: (type_, value)}."""
    return {p.name: (p.type_, p.value) for p in job_config.query_parameters}


# ---------------------------------------------------------------------------
# get_prior_snapshot_watermark
# ---------------------------------------------------------------------------


def test_watermark_returns_prior_date_and_watermark(mock_bigquery_client):
    """A repo with a prior snapshot returns its date (str) and modified watermark."""
    wm = datetime(2026, 6, 17, 14, 53, 58, tzinfo=timezone.utc)
    row = SimpleNamespace(prior_date=date(2026, 6, 17), watermark=wm)
    mock_bigquery_client.query.return_value.result.return_value = [row]

    prior_date, watermark = main.get_prior_snapshot_watermark(
        mock_bigquery_client, "test_dataset", "mozilla/firefox", "2026-06-20"
    )

    assert prior_date == "2026-06-17"
    assert watermark == wm

    call = mock_bigquery_client.query.call_args
    sql = call.args[0]
    assert "snapshot_date < @snapshot_date" in sql
    params = _params(call.kwargs["job_config"])
    # ScalarQueryParameter coerces the "DATE"-typed string into a datetime.date.
    assert params["snapshot_date"] == ("DATE", date(2026, 6, 20))
    assert params["repo"] == ("STRING", "mozilla/firefox")


def test_watermark_returns_none_when_no_prior_snapshot(mock_bigquery_client):
    """No earlier snapshot (aggregate row is all NULL) returns (None, None)."""
    row = SimpleNamespace(prior_date=None, watermark=None)
    mock_bigquery_client.query.return_value.result.return_value = [row]

    result = main.get_prior_snapshot_watermark(
        mock_bigquery_client, "test_dataset", "mozilla/firefox", "2026-06-20"
    )

    assert result == (None, None)


def test_watermark_missing_table_returns_none(mock_bigquery_client):
    """A missing pull_requests table (first run) is swallowed → (None, None)."""
    mock_bigquery_client.query.return_value.result.side_effect = (
        api_exceptions.NotFound(
            "Not found: Table test-project:test_dataset.pull_requests"
        )
    )

    result = main.get_prior_snapshot_watermark(
        mock_bigquery_client, "test_dataset", "mozilla/firefox", "2026-06-20"
    )

    assert result == (None, None)


def test_watermark_missing_dataset_reraises(mock_bigquery_client):
    """A missing dataset is a config error and must propagate."""
    mock_bigquery_client.query.return_value.result.side_effect = (
        api_exceptions.NotFound(
            "Not found: Dataset test-project:test_dataset path datasets/test_dataset"
        )
    )

    with pytest.raises(api_exceptions.NotFound):
        main.get_prior_snapshot_watermark(
            mock_bigquery_client, "test_dataset", "mozilla/firefox", "2026-06-20"
        )


def test_watermark_none_when_prior_date_but_no_modified(mock_bigquery_client):
    """A prior snapshot with all-NULL date_modified yields a date but no watermark."""
    row = SimpleNamespace(prior_date=date(2026, 6, 17), watermark=None)
    mock_bigquery_client.query.return_value.result.return_value = [row]

    prior_date, watermark = main.get_prior_snapshot_watermark(
        mock_bigquery_client, "test_dataset", "mozilla/firefox", "2026-06-20"
    )

    assert prior_date == "2026-06-17"
    assert watermark is None


# ---------------------------------------------------------------------------
# carry_forward_snapshot
# ---------------------------------------------------------------------------


def test_carry_forward_runs_insert_select_per_table(mock_bigquery_client):
    """One INSERT...SELECT runs per table, each targeting that table."""
    main.carry_forward_snapshot(
        mock_bigquery_client,
        "test_dataset",
        "mozilla/firefox",
        "2026-06-17",
        "2026-06-20",
    )

    assert mock_bigquery_client.query.call_count == 4

    sql_by_table = {}
    for call in mock_bigquery_client.query.call_args_list:
        sql = call.args[0]
        assert "INSERT INTO" in sql
        assert "SELECT" in sql
        assert "FROM" in sql
        for table in main._TABLE_COLUMNS:
            if f"test-project.test_dataset.{table}`" in sql:
                sql_by_table[table] = sql

    assert set(sql_by_table) == set(main._TABLE_COLUMNS)


def test_carry_forward_binds_date_and_repo_params(mock_bigquery_client):
    """Every table's statement binds today, prior date, and repo with right types."""
    main.carry_forward_snapshot(
        mock_bigquery_client,
        "test_dataset",
        "mozilla/firefox",
        "2026-06-17",
        "2026-06-20",
    )

    for call in mock_bigquery_client.query.call_args_list:
        params = _params(call.kwargs["job_config"])
        # ScalarQueryParameter coerces the "DATE"-typed strings into datetime.date.
        assert params["snapshot_date"] == ("DATE", date(2026, 6, 20))
        assert params["prior_date"] == ("DATE", date(2026, 6, 17))
        assert params["repo"] == ("STRING", "mozilla/firefox")


def test_carry_forward_pull_requests_columns(mock_bigquery_client):
    """The pull_requests copy lists every data column + snapshot_date, re-stamped."""
    main.carry_forward_snapshot(
        mock_bigquery_client,
        "test_dataset",
        "mozilla/firefox",
        "2026-06-17",
        "2026-06-20",
    )

    pr_sql = next(
        call.args[0]
        for call in mock_bigquery_client.query.call_args_list
        if "test-project.test_dataset.pull_requests`" in call.args[0]
    )

    for column in main._TABLE_COLUMNS["pull_requests"]:
        assert column in pr_sql
    # The INSERT target includes snapshot_date and the SELECT re-stamps it.
    assert "snapshot_date)" in pr_sql
    assert "@snapshot_date" in pr_sql
