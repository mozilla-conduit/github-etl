from datetime import datetime, timezone
from unittest.mock import Mock, patch

import main

_API = "https://api.github.com"


def _delta_for(chunk):
    """A transform_data-shaped dict with one pull_request row per PR in the chunk."""
    return {
        "pull_requests": [{"pull_request_id": pr["number"]} for pr in chunk],
        "commits": [],
        "reviewers": [],
        "comments": [],
    }


def _run(**overrides):
    """Invoke process_repo with collaborators patched; return the patch mocks."""
    kwargs = {
        "repo": "mozilla/firefox",
        "github_app_id": None,
        "github_private_key": None,
        "github_api_url": _API,
        "bigquery_client": Mock(),
        "bigquery_dataset": "test_dataset",
        "snapshot_date": "2026-06-20",
        "use_streaming_insert": False,
        "lookback_hours": 24,
        "full_refresh": False,
    }
    kwargs.update(overrides)

    with (
        patch("main.get_prior_snapshot_watermark") as mock_watermark,
        patch("main.snapshot_exists", return_value=False),
        patch("main.delete_existing_snapshot") as mock_delete,
        patch("main.carry_forward_snapshot") as mock_carry,
        patch("main.reconcile_delta") as mock_reconcile,
        patch("main.extract_pull_requests") as mock_extract,
        patch("main.transform_data") as mock_transform,
        patch("main.load_data") as mock_load,
    ):
        mock_watermark.return_value = overrides.pop("_watermark", (None, None))
        mock_extract.return_value = overrides.pop("_chunks", iter([[{"number": 1}]]))
        mock_transform.side_effect = overrides.pop(
            "_transform", lambda chunk, repo: _delta_for(chunk)
        )
        mocks = {
            "watermark": mock_watermark,
            "delete": mock_delete,
            "carry": mock_carry,
            "reconcile": mock_reconcile,
            "extract": mock_extract,
            "transform": mock_transform,
            "load": mock_load,
        }
        # _run accepts test-only keys via overrides; strip them before the call.
        for key in ("_watermark", "_chunks", "_transform"):
            kwargs.pop(key, None)
        mocks["result"] = main.process_repo(**kwargs)
    return mocks


def test_first_run_does_full_export():
    """No prior snapshot → full export (since=None), no carry-forward/reconcile."""
    mocks = _run(_watermark=(None, None))

    _, extract_kwargs = mocks["extract"].call_args
    assert extract_kwargs.get("since") is None
    mocks["load"].assert_called_once()
    mocks["carry"].assert_not_called()
    mocks["reconcile"].assert_not_called()


def test_incremental_run_carries_forward_and_reconciles():
    """Prior snapshot + watermark → carry-forward, since-bounded fetch, reconcile."""
    watermark = datetime(2026, 6, 17, 14, 53, 58, tzinfo=timezone.utc)
    mocks = _run(_watermark=("2026-06-17", watermark))

    mocks["carry"].assert_called_once()
    carry_args = mocks["carry"].call_args.args
    assert carry_args[2] == "mozilla/firefox"
    assert carry_args[3] == "2026-06-17"  # prior_date
    assert carry_args[4] == "2026-06-20"  # today

    _, extract_kwargs = mocks["extract"].call_args
    assert extract_kwargs["since"] == "2026-06-16T14:53:58Z"  # watermark - 24h

    mocks["reconcile"].assert_called_once()
    mocks["load"].assert_not_called()  # reconcile_delta owns the insert


def test_full_refresh_forces_full_export_despite_prior():
    """full_refresh overrides an available prior snapshot."""
    watermark = datetime(2026, 6, 17, 14, 53, 58, tzinfo=timezone.utc)
    mocks = _run(_watermark=("2026-06-17", watermark), full_refresh=True)

    mocks["carry"].assert_not_called()
    mocks["reconcile"].assert_not_called()
    mocks["load"].assert_called_once()


def test_prior_date_without_watermark_does_full_export():
    """A prior snapshot with no usable watermark falls back to full export."""
    mocks = _run(_watermark=("2026-06-17", None))

    mocks["carry"].assert_not_called()
    mocks["reconcile"].assert_not_called()
    mocks["load"].assert_called_once()


def test_incremental_accumulates_delta_across_chunks():
    """Delta from every chunk is merged into the single reconcile_delta call."""
    watermark = datetime(2026, 6, 17, 14, 53, 58, tzinfo=timezone.utc)
    chunks = iter([[{"number": 1}], [{"number": 2}, {"number": 3}]])
    mocks = _run(_watermark=("2026-06-17", watermark), _chunks=chunks)

    delta = mocks["reconcile"].call_args.args[3]
    assert [r["pull_request_id"] for r in delta["pull_requests"]] == [1, 2, 3]
    assert mocks["result"] == 3
