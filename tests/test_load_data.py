#!/usr/bin/env python3
"""
Tests for load_data function.

Tests BigQuery data loading including table insertion, snapshot dates,
and error handling.
"""

from unittest.mock import patch

import pytest

import main


@patch("main.datetime")
def test_load_data_inserts_all_tables(mock_datetime, mock_bigquery_client):
    """Test that load_data inserts all tables correctly."""
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
def test_adds_snapshot_date(mock_datetime, mock_bigquery_client):
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


def test_constructs_correct_table_ref(mock_bigquery_client):
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


def test_empty_transformed_data_skipped(mock_bigquery_client):
    """Test that empty transformed_data dict is skipped."""
    transformed_data = {}

    main.load_data(mock_bigquery_client, "test_dataset", transformed_data)

    mock_bigquery_client.insert_rows_json.assert_not_called()


def test_skips_empty_tables_individually(mock_bigquery_client):
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


def test_only_pull_requests_table(mock_bigquery_client):
    """Test loading only pull_requests table."""
    transformed_data = {
        "pull_requests": [{"pull_request_id": 1}],
        "commits": [],
        "reviewers": [],
        "comments": [],
    }

    main.load_data(mock_bigquery_client, "test_dataset", transformed_data)

    assert mock_bigquery_client.insert_rows_json.call_count == 1


def test_raises_exception_on_insert_errors(mock_bigquery_client):
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


def test_verifies_client_insert_called_correctly(mock_bigquery_client):
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
