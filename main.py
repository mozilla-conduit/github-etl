#!/usr/bin/env python3
"""
GitHub ETL for Mozilla Organization Firefox repositories

This script extracts data from GitHub repositories, transforms it,
and loads it into a BigQuery dataset using chunked processing.
"""

import logging
import os
import re
import requests
import sys
import time
from typing import List
from datetime import datetime, timezone
from typing import Iterator, Optional
from urllib.parse import parse_qs, urlparse
from google.cloud import bigquery
from google.api_core.client_options import ClientOptions
from google.auth.credentials import AnonymousCredentials


BUG_RE = re.compile(r"\b(?:bug|b=)\s*#?(\d+)\b", re.I)


def setup_logging() -> None:
    """Configure logging for the ETL process."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )


def extract_pull_requests(
    session: requests.Session,
    repo: str,
    chunk_size: int = 100,
    github_api_url: Optional[str] = None,
) -> Iterator[List[dict]]:
    """
    Extract data from GitHub repositories in chunks.

    Yields chunks of pull requests for streaming processing.

    Args:
        session: Authenticated requests session
        repo: GitHub repository name
        chunk_size: Number of PRs to yield per chunk (default: 100)
        github_api_url: Optional custom GitHub API URL (for testing/mocking)

    Yields:
        List of pull request dictionaries (up to chunk_size items)
    """
    logger = logging.getLogger(__name__)
    logger.info("Starting data extraction from GitHub repositories")

    # Support custom API URL for mocking/testing
    api_base = github_api_url or "https://api.github.com"
    base_url = f"{api_base}/repos/{repo}/pulls"
    params = {
        "state": "all",
        "per_page": chunk_size,
        "sort": "created",
        "direction": "asc",
    }

    total = 0
    pages = 0

    while True:
        resp = session.get(base_url, params=params)
        if (
            resp.status_code == 403
            and int(resp.headers.get("X-RateLimit-Remaining", "1")) == "0"
        ):
            sleep_for_rate_limit(resp)
            # retry same URL/params after sleeping
            continue
        if resp.status_code != 200:
            error_text = resp.text[:500] if resp.text else "No response text"
            raise SystemExit(f"GitHub API error {resp.status_code}: {error_text}")

        batch = resp.json()
        pages += 1
        total += len(batch)

        if len(batch) > 0:
            logger.info(
                f"Extracted page {pages} with {len(batch)} PRs (total: {total})"
            )

            for idx, pr in enumerate(batch):
                pr_number = pr.get("number")
                if not pr_number:
                    continue
                pr["commit_data"] = extract_commits(
                    session, repo, pr_number, github_api_url
                )
                pr["reviewer_data"] = extract_reviewers(
                    session, repo, pr_number, github_api_url
                )
                pr["comment_data"] = extract_comments(
                    session, repo, pr_number, github_api_url
                )

            yield batch

        # Pagination
        next_url = resp.links.get("next", {}).get("url")
        if not next_url or len(batch) == 0:
            break
        # Parse the next URL and extract the page parameter
        parsed_url = urlparse(next_url)
        query_params = parse_qs(parsed_url.query)
        # Update only the page parameter, preserving other params
        if "page" in query_params and query_params["page"]:
            try:
                page_num = int(query_params["page"][0])
                if page_num > 0:
                    params["page"] = page_num
                else:
                    logger.warning(f"Invalid page number {page_num} in next URL, stopping pagination")
                    break
            except (ValueError, IndexError) as e:
                logger.warning(f"Invalid page parameter in next URL: {e}, stopping pagination")
                break
        else:
            # If no page parameter, this is unexpected - log and stop pagination
            logger.warning("No page parameter in next URL, stopping pagination")
            break

    logger.info(f"Data extraction completed. Total PRs: {total}, Pages: {pages}")


def extract_commits(
    session: requests.Session,
    repo: str,
    pr_number: int,
    github_api_url: Optional[str] = None,
) -> List[dict]:
    """
    Extract commits for a specific pull request.

    Args:
        session: Authenticated requests session
        repo: GitHub repository name
        pr_number: Pull request number
        github_api_url: Optional custom GitHub API URL (for testing/mocking)
    Returns:
        List of commit dictionaries for the pull request
    """
    logger = logging.getLogger(__name__)
    logger.info(f"Extracting commits for PR #{pr_number}")

    # Support custom API URL for mocking/testing
    api_base = github_api_url or "https://api.github.com"
    commits_url = f"{api_base}/repos/{repo}/pulls/{pr_number}/commits"

    logger.info(f"Commits URL: {commits_url}")

    resp = session.get(commits_url)
    if (
        resp.status_code == 403
        and int(resp.headers.get("X-RateLimit-Remaining", "1")) == "0"
    ):
        sleep_for_rate_limit(resp)
        resp = session.get(commits_url)
    if resp.status_code != 200:
        raise SystemExit(f"GitHub API error {resp.status_code}: {resp.text[:500]}")

    commits = resp.json()
    logger.info(f"Extracted {len(commits)} commits for PR #{pr_number}")
    return commits


def extract_reviewers(
    session: requests.Session,
    repo: str,
    pr_number: int,
    github_api_url: Optional[str] = None,
) -> List[dict]:
    """
    Extract reviewers for a specific pull request.

    Args:
        session: Authenticated requests session
        repo: GitHub repository name
        pr_number: Pull request number
        github_api_url: Optional custom GitHub API URL (for testing/mocking)
    Returns:
        List of reviewer dictionaries for the pull request
    """
    logger = logging.getLogger(__name__)
    logger.info(f"Extracting reviews for PR #{pr_number}")

    # Support custom API URL for mocking/testing
    api_base = github_api_url or "https://api.github.com"
    reviewers_url = f"{api_base}/repos/{repo}/pulls/{pr_number}/requested_reviewers"

    logger.info(f"Reviewers URL: {reviewers_url}")

    resp = session.get(reviewers_url)
    if (
        resp.status_code == 403
        and int(resp.headers.get("X-RateLimit-Remaining", "1")) == "0"
    ):
        sleep_for_rate_limit(resp)
        resp = session.get(reviewers_url)
    if resp.status_code != 200:
        raise SystemExit(f"GitHub API error {resp.status_code}: {resp.text[:500]}")

    reviewers = resp.json()
    logger.info(
        f"Extracted {len(reviewers.get('users', []))} reviews for PR #{pr_number}"
    )
    return reviewers


def extract_comments(
    session: requests.Session,
    repo: str,
    pr_number: int,
    github_api_url: Optional[str] = None,
) -> List[dict]:
    """
    Extract comments for a specific pull request.

    Args:
        session: Authenticated requests session
        repo: GitHub repository name
        pr_number: Pull request number
        github_api_url: Optional custom GitHub API URL (for testing/mocking)
    Returns:
        List of comment dictionaries for the pull request
    """
    logger = logging.getLogger(__name__)
    logger.info(f"Extracting comments for PR #{pr_number}")

    # Support custom API URL for mocking/testing
    api_base = github_api_url or "https://api.github.com"
    comments_url = f"{api_base}/repos/{repo}/issues/{pr_number}/comments"

    logger.info(f"Comments URL: {comments_url}")

    resp = session.get(comments_url)
    if (
        resp.status_code == 403
        and int(resp.headers.get("X-RateLimit-Remaining", "1")) == "0"
    ):
        sleep_for_rate_limit(resp)
        resp = session.get(comments_url)
    if resp.status_code != 200:
        raise SystemExit(f"GitHub API error {resp.status_code}: {resp.text[:500]}")

    comments = resp.json()
    logger.info(f"Extracted {len(comments)} comments for PR #{pr_number}")
    return comments


def sleep_for_rate_limit(resp):
    """Sleep until rate limit resets."""
    remaining = int(resp.headers.get("X-RateLimit-Remaining", 1))
    reset = int(resp.headers.get("X-RateLimit-Reset", 0))
    if remaining == 0:
        sleep_time = max(0, reset - int(time.time()))
        print(
            f"Rate limit exceeded. Sleeping for {sleep_time} seconds.", file=sys.stderr
        )
        time.sleep(sleep_time)


def transform_data(raw_data: List[dict], repo: str) -> dict:
    """
    Transform GitHub pull request data into BigQuery-compatible format.

    Args:
        raw_data: List of pull request dictionaries from GitHub API

    Returns:
        List of transformed pull requests, commits, reviewers, and comments ready for BigQuery
    """
    logger = logging.getLogger(__name__)
    logger.info(f"Starting data transformation for {len(raw_data)} PRs")

    transformed_data = {
        "pull_requests": [],
        "commits": [],
        "reviewers": [],
        "comments": [],
    }

    for pr in raw_data:
        # Extract and flatten pull request data
        logger.info(f"Transforming PR #{pr.get('number')}")

        matches = [
            m
            for m in BUG_RE.finditer(pr.get("title", ""))
            if int(m.group(1)) < 100000000
        ]
        bug_id = int(matches[0].group(1)) if matches else None

        transformed_pr = {
            "pull_request_id": pr.get("number"),
            "current_status": pr.get("state"),
            "date_created": pr.get("created_at"),
            "date_modified": pr.get("updated_at"),
            "target_repository": repo,
            "bug_id": bug_id,
            "date_landed": pr.get("merged_at"),
            "date_approved": None,  # TODO Placeholder for approval date extraction logic
            "labels": [label.get("name") for label in pr.get("labels", [])]
            if pr.get("labels")
            else [],
        }
        transformed_data["pull_requests"].append(transformed_pr)

        # Extract and flatten commit data
        logger.info("Transforming commits for PR #{}".format(pr.get("number")))
        for commit in pr["commit_data"]:
            transformed_commit = {
                "pull_request_id": pr.get("number"),
                "target_repository": repo,
                "commit_sha": commit.get("sha"),
                "date_created": commit.get("commit", {}).get("author", {}).get("date"),
                "author_username": commit.get("commit", {})
                .get("author", {})
                .get("name"),
                "author_email": commit.get("commit", {}).get("author", {}).get("email"),
                "filename": None,  # TODO Placeholder for filename extraction logic
                "lines_removed": None,  # TODO Placeholder for lines removed extraction logic
                "lines_added": None,  # TODO Placeholder for lines added extraction logic
            }
            transformed_data["commits"].append(transformed_commit)

        # Extract and flatten reviewer data
        logger.info("Transforming reviewers for PR #{}".format(pr.get("number")))
        for review in pr["reviewer_data"]["users"]:
            transformed_reviewer = {
                "pull_request_id": pr.get("number"),
                "target_repository": repo,
                "date_review_requested": None,  # TODO Placeholder for actual request date
                "reviewer_email": None,  # TODO Placeholder for reviewer email extraction logic
                "reviewer_username": review.get("user", {}),
                "status": None,  # TODO Placeholder for review status extraction logic
            }
            transformed_data["reviewers"].append(transformed_reviewer)

        # Extract and flatten comment data
        logger.info("Transforming comments for PR #{}".format(pr.get("number")))
        for comment in pr["comment_data"]:
            transformed_comment = {
                "pull_request_id": pr.get("number"),
                "target_repository": repo,
                "comment_id": comment.get("id"),
                "date_created": comment.get("created_at"),
                "author_email": None,  # TODO
                "author_username": comment.get("user", {}).get("login"),
                "character_count": len(comment.get("body", ""))
                if comment.get("body")
                else 0,
                "status": None,  # TODO
            }
            transformed_data["comments"].append(transformed_comment)

    logger.info(
        f"Data transformation completed for {len(transformed_data['pull_requests'])} PRs"
    )

    return transformed_data


def load_data(
    client: bigquery.Client,
    dataset_id: str,
    transformed_data: dict,
) -> None:
    """
    Load transformed data to BigQuery using the Python client library.

    Args:
        client: BigQuery client instance
        dataset_id: BigQuery dataset ID
        transformed_data: Dictionary containing tables ('pull_requests', 'commits', 'reviewers', 'comments') mapped to lists of row dictionaries
    """
    logger = logging.getLogger(__name__)

    if not transformed_data:
        logger.warning("No data to load, skipping")
        return

    # Add snapshot date to all rows
    snapshot_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    for table, load_table_data in transformed_data.items():
        if not load_table_data:
            logger.warning(f"No data to load for table {table}, skipping")
            continue

        logger.info(
            f"Starting data loading for {len(load_table_data)} rows to {dataset_id}.{table}"
        )

        # Add snapshot date to each row
        for row in load_table_data:
            row["snapshot_date"] = snapshot_date

        # Insert rows into BigQuery
        table_ref = f"{client.project}.{dataset_id}.{table}"
        logger.info(table_ref)
        errors = client.insert_rows_json(table_ref, load_table_data)

        if errors:
            error_msg = f"BigQuery insert errors for table {table}: {errors}"
            logger.error(error_msg)
            raise Exception(error_msg)

        logger.info(
            f"Data loading completed successfully for table {table} with {len(load_table_data)} rows"
        )


def main() -> int:
    """
    Main ETL process with chunked processing.

    Processes pull requests in chunks of 100:
    1. Extract 100 PRs from GitHub
    2. Transform the data
    3. Load to BigQuery
    4. Repeat until no more data
    """
    setup_logging()
    logger = logging.getLogger(__name__)

    logger.info("Starting GitHub ETL process with chunked processing")

    github_token = os.environ.get("GITHUB_TOKEN")
    if not github_token:
        print(
            "Warning: No token provided. You will hit very low rate limits and private repos won't work.",
            file=sys.stderr,
        )

    # Read BigQuery configuration
    bigquery_project = os.environ.get("BIGQUERY_PROJECT")
    bigquery_dataset = os.environ.get("BIGQUERY_DATASET")

    if not bigquery_project:
        raise SystemExit("Environment variable BIGQUERY_PROJECT is required")
    if not bigquery_dataset:
        raise SystemExit("Environment variable BIGQUERY_DATASET is required")

    # Setup GitHub session
    session = requests.Session()
    session.headers.update(
        {
            "Accept": "application/vnd.github+json",
            "User-Agent": "gh-pr-scraper/1.0 (+https://api.github.com)",
        }
    )

    if github_token:
        session.headers["Authorization"] = f"Bearer {github_token}"

    # Support custom GitHub API URL for testing/mocking
    github_api_url = os.environ.get("GITHUB_API_URL")
    if github_api_url:
        logger.info(f"Using custom GitHub API URL: {github_api_url}")

    # Setup BigQuery client
    # Support BigQuery emulator for local testing
    emulator_host = os.environ.get("BIGQUERY_EMULATOR_HOST")
    if emulator_host:
        logger.info(f"Using BigQuery emulator at {emulator_host}")
        bigquery_client = bigquery.Client(
            project=bigquery_project,
            client_options=ClientOptions(api_endpoint=emulator_host),
            credentials=AnonymousCredentials(),
        )
    else:
        bigquery_client = bigquery.Client(project=bigquery_project)

    # Read GitHub repository configuration
    github_repos = os.getenv("GITHUB_REPOS").split(",")
    if not github_repos:
        raise SystemExit(
            "Environment variable GITHUB_REPOS is required (format: 'owner/repo,owner/repo')"
        )

    total_processed = 0
    chunk_count = 0

    for repo in github_repos:
        for chunk_count, chunk in enumerate(
            extract_pull_requests(
                session, repo, chunk_size=100, github_api_url=github_api_url
            ),
            start=1,
        ):
            logger.info(f"Processing chunk {chunk_count} with {len(chunk)} PRs")

            # Transform
            transformed_data = transform_data(chunk, repo)

            # Load
            load_data(bigquery_client, bigquery_dataset, transformed_data)

            total_processed += len(chunk)
            logger.info(
                f"Completed chunk {chunk_count}. Total PRs processed: {total_processed}"
            )

    logger.info(
        f"GitHub ETL process completed successfully. Total PRs processed: {total_processed}"
    )

    return 0


if __name__ == "__main__":
    sys.exit(main())
