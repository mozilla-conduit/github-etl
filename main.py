"""
GitHub ETL for Mozilla Organization Firefox repositories

This script extracts data from GitHub repositories, transforms it,
and loads it into a BigQuery dataset using chunked processing.
"""

import concurrent.futures
import itertools
import logging
import os
import re
import sys
import threading
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Callable, Iterator
from urllib.parse import parse_qs, urlparse

import jwt
import requests
from google.api_core import exceptions as api_exceptions
from google.api_core.client_options import ClientOptions
from google.auth.credentials import AnonymousCredentials
from google.cloud import bigquery

BUG_RE = re.compile(r"\b(?:bug|b=)\s*#?(\d+)\b", re.I)

logger = logging.getLogger(__name__)

_RETRYABLE_STATUS_CODES: frozenset[int] = frozenset({429, 500, 502, 503, 504})
_MAX_RETRIES: int = 5
_RETRY_BASE_DELAY: float = 1.0
_RETRY_MAX_DELAY: float = 60.0
_RETRY_MULTIPLIER: float = 2.0
_MAX_AUTH_RETRIES: int = 2
_REQUEST_TIMEOUT: float = 30.0

# Default ceiling on concurrent repo workers (overridable via GITHUB_ETL_MAX_WORKERS).
_DEFAULT_MAX_WORKERS: int = 8

# Max changed-PR ids inlined into a single reconcile DELETE's IN (...) list. Keeps
# the statement bounded when a large number of PRs change in one day; the ids are
# deleted in successive batches per table.
_RECONCILE_ID_BATCH_SIZE: int = 100

# Default hours subtracted from the prior-snapshot watermark when computing the
# incremental `since` floor (overridable via GITHUB_ETL_LOOKBACK_HOURS). The
# watermark looks *before* data we already have, so this only needs to be large
# enough to catch PRs updated in the same second as the watermark but not captured;
# 1h is ample for that boundary. Bump GITHUB_ETL_LOOKBACK_HOURS for manual catch-up.
_DEFAULT_LOOKBACK_HOURS: int = 1

# Ordered data columns for each BigQuery table, EXCLUDING the snapshot_date column
# (which is stamped per load). Kept in sync with data.yml / the production schema
# and with the row dicts built in transform_data(). Used to build carry-forward
# INSERT...SELECT statements (and, later, the reconcile delta) without SELECT *.
_TABLE_COLUMNS: dict[str, tuple[str, ...]] = {
    "pull_requests": (
        "pull_request_id",
        "current_status",
        "date_created",
        "date_modified",
        "target_repository",
        "bug_id",
        "date_landed",
        "date_approved",
        "labels",
    ),
    "commits": (
        "pull_request_id",
        "target_repository",
        "commit_sha",
        "date_created",
        "author_username",
        "author_email",
        "filename",
        "lines_removed",
        "lines_added",
    ),
    "reviewers": (
        "pull_request_id",
        "target_repository",
        "date_reviewed",
        "reviewer_email",
        "reviewer_username",
        "status",
    ),
    "comments": (
        "pull_request_id",
        "target_repository",
        "comment_id",
        "date_created",
        "author_email",
        "author_username",
        "character_count",
        "status",
    ),
}


class TooManyRetriesError(Exception):
    """Raised when all retry attempts for a GitHub API request are exhausted."""


def _apply_backoff(delay: float) -> float:
    """Sleep for *delay* seconds and return the next (capped) delay value."""
    time.sleep(delay)
    return min(delay * _RETRY_MULTIPLIER, _RETRY_MAX_DELAY)


@dataclass(frozen=True)
class AccessToken:
    token: str
    expires_at: datetime


repo_installation_cache: dict[str, int] = {}


class TokenStore:
    """
    Thread-safe cache of GitHub App installation access tokens.

    Tokens are cached per installation ID and reused until they are within 60
    seconds of expiry. Token creation is serialized per installation via a lock so
    that concurrent workers sharing an installation don't each POST
    /access_tokens, which would produce redundant token creations and a burst
    against GitHub's per-installation rate limit. Locks are keyed by installation
    ID so a cache miss (or rate-limit sleep) for one installation never blocks
    token creation for another. The lock is only held on the slow (cache-miss)
    path. Two short-lived guards protect the shared dicts themselves: one for the
    token cache and one for the lock map.
    """

    def __init__(self) -> None:
        self._tokens: dict[int, AccessToken] = {}
        self._tokens_guard = threading.Lock()
        self._locks: defaultdict[int, threading.Lock] = defaultdict(threading.Lock)
        self._locks_guard = threading.Lock()

    def cached_token(self, installation_id: int) -> str | None:
        """Return a still-valid cached token for the installation, or None."""
        with self._tokens_guard:
            cached = self._tokens.get(installation_id)
        if cached is not None and cached.expires_at > datetime.now(
            timezone.utc
        ) + timedelta(seconds=60):
            logger.info(
                f"Reusing cached access token for installation {installation_id}, "
                f"expires at {cached.expires_at}"
            )
            return cached.token
        return None

    def lock_for(self, installation_id: int) -> threading.Lock:
        """Return the (lazily created) token-creation lock for an installation."""
        with self._locks_guard:
            return self._locks[installation_id]

    def store(self, installation_id: int, token: AccessToken) -> None:
        """Cache *token* for *installation_id*."""
        with self._tokens_guard:
            self._tokens[installation_id] = token


# Module-level token store shared across all worker threads.
token_store = TokenStore()


def _is_rate_limited(resp: requests.Response) -> bool:
    """
    Return True when *resp* indicates an exhausted rate limit.

    GitHub uses 403 or 429 for two distinct rate limits, both handled here:

    - **Primary**: signaled by ``X-RateLimit-Remaining: 0`` (with a
      ``X-RateLimit-Reset`` epoch telling us when it replenishes).
    - **Secondary** (abuse detection): signaled by a ``Retry-After`` header,
      and frequently *without* ``X-RateLimit-Remaining: 0``.

    A 403/429 carrying neither signal (e.g. a genuine permission error) is
    deliberately not treated as rate-limited.
    """
    if resp.status_code not in (403, 429):
        return False
    if int(resp.headers.get("X-RateLimit-Remaining", "1")) == 0:
        return True
    return "Retry-After" in resp.headers


def generate_github_jwt(app_id: str, private_key_pem: str) -> str:
    """
    Generate a short-lived GitHub App JWT signed with the app's private key.

    GitHub App JWTs are valid for a maximum of 10 minutes. We use a 9-minute
    expiry and backdate iat by 60 seconds to absorb clock skew between the
    local machine and GitHub's servers.

    Args:
        app_id: GitHub App ID (numeric, found on the App's settings page)
        private_key_pem: RSA private key in PEM format

    Returns:
        Signed JWT string
    """
    now = int(time.time())
    payload = {
        "iat": now - 60,  # backdate 60s to absorb clock skew
        "exp": now + 540,  # 9 minutes (GitHub maximum is 10)
        "iss": app_id,
    }
    return jwt.encode(payload, private_key_pem, algorithm="RS256")


def get_installation_access_token(
    app_jwt: str,
    repo: str,
    github_api_url: str,
) -> str:
    """
    Get a GitHub App installation access token, returning a cached one if still valid.

    Uses the JWT (generated by ``generate_github_jwt()``) to look up the installation
    for the given repo, then exchanges it for an installation access token (valid for
    1 hour). Tokens are cached per installation ID so that repos sharing an installation
    reuse the same token, while repos on different installations each get their own.
    The repo->installation ID mapping is also cached since it never changes.

    Args:
        app_jwt: Short-lived GitHub App JWT produced by ``generate_github_jwt()``
        repo: Repository in "owner/repo" format, used to look up the installation
        github_api_url: GitHub API base URL

    Returns:
        Installation access token string
    """

    # Use a context manager so the temporary session's connection pool is closed
    # on every return path (cache hits included), rather than leaking sockets as
    # refresh_auth calls this repeatedly across concurrent repo workers.
    with _build_session() as session:
        session.headers.update(
            {
                "Authorization": f"Bearer {app_jwt}",
            }
        )

        installation_id = repo_installation_cache.get(repo)
        if installation_id is None:
            # No refresh_auth: this request authenticates with the app JWT, so a 401
            # means the JWT itself is bad and refreshing an installation token would
            # not help (and would recurse back into this function).
            resp = github_request(
                session, "GET", f"{github_api_url}/repos/{repo}/installation"
            )
            try:
                installation_id = resp.json()["id"]
            except (requests.exceptions.JSONDecodeError, KeyError) as e:
                raise RuntimeError(
                    f"Failed to parse installation response for {repo}: {e}: {resp.text}"
                )
            repo_installation_cache[repo] = installation_id

        # Fast path: serve a still-valid cached token without taking the lock.
        token = token_store.cached_token(installation_id)
        if token is not None:
            return token

        # Slow path: serialize creation so concurrent workers sharing an installation
        # don't each POST /access_tokens. The lock is specific to this installation,
        # so a rate-limit sleep here never blocks workers on other installations.
        # Re-check the cache once the lock is held in case another thread populated it
        # while we waited.
        with token_store.lock_for(installation_id):
            token = token_store.cached_token(installation_id)
            if token is not None:
                return token

            logger.info(
                f"Fetching new GitHub App installation access token for installation {installation_id}"
            )
            resp = github_request(
                session,
                "POST",
                f"{github_api_url}/app/installations/{installation_id}/access_tokens",
                expected_status=201,
            )

            try:
                data = resp.json()
            except requests.exceptions.JSONDecodeError as e:
                raise RuntimeError(
                    f"Failed to parse access token response: {e}: {resp.text}"
                )
            try:
                access_token = AccessToken(
                    token=data["token"],
                    expires_at=datetime.fromisoformat(data["expires_at"]),
                )
            except KeyError as e:
                raise RuntimeError(
                    f"Unexpected access token response structure, missing key {e}: {resp.text}"
                )
            except ValueError as e:
                raise RuntimeError(
                    f"Invalid expires_at format in access token response: {e}"
                )
            token_store.store(installation_id, access_token)
            logger.info(
                f"Obtained new access token, expires at {access_token.expires_at}"
            )
            return access_token.token


def setup_logging() -> None:
    """Configure logging for the ETL process."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
        force=True,
    )


def _parse_github_timestamp(value: str | None) -> datetime | None:
    """
    Parse a GitHub ISO-8601 timestamp (e.g. ``2026-06-17T14:53:58Z``) into an
    aware ``datetime``.

    GitHub serializes timestamps with a trailing ``Z`` for UTC, which
    ``datetime.fromisoformat`` accepts natively on the Python 3.11+ runtime we
    target. Comparing parsed datetimes (vs. raw strings) avoids subtle bugs where
    formats differ only in fractional seconds (``...:58Z`` vs ``...:58.000Z``).

    Args:
        value: Timestamp string, or None.

    Returns:
        An aware ``datetime``, or None if the value is missing/unparseable.
    """
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError, TypeError:
        return None


def extract_pull_requests(
    session: requests.Session,
    repo: str,
    chunk_size: int = 100,
    github_api_url: str = "https://api.github.com",
    refresh_auth: Callable[[], None] | None = None,
    since: str | None = None,
) -> Iterator[list[dict]]:
    """
    Extract data from GitHub repositories in chunks.

    Yields chunks of pull requests for streaming processing.

    Args:
        session: Authenticated requests session
        repo: GitHub repository name
        chunk_size: Number of PRs to yield per chunk (default: 100)
        github_api_url: GitHub API base URL
        refresh_auth: Optional callable invoked before each page fetch to refresh
            the session's Authorization header. Use this to prevent installation
            tokens (1-hour TTL) from expiring mid-extraction on large repos.
        since: Optional ISO-8601 UTC timestamp (e.g. ``2026-06-17T14:53:58Z``).
            When provided, enables *incremental* extraction: PRs are fetched
            sorted by ``updated`` descending, only PRs with
            ``updated_at >= since`` are yielded, and pagination stops as soon as
            an older PR is reached. The per-PR commit/review/comment sub-fetches
            run only for the kept PRs. A PR with a missing or unparseable
            ``updated_at`` is kept and does not trigger the stop. When None
            (default), all PRs are fetched sorted by ``created`` ascending.

            NOTE: callers deriving ``since`` from a stored watermark must format
            it as an ISO-8601 UTC string so the comparison is meaningful.

    Yields:
        List of pull request dictionaries (up to chunk_size items)
    """
    logger.info("Starting data extraction from GitHub repositories")

    # A provided-but-unparseable `since` must not enable incremental mode: doing so
    # would switch the request to updated/desc while leaving the watermark filter
    # off (since_dt is None), fetching everything in the wrong order. Treat it like
    # "not provided" and fall back to the full created/asc fetch.
    since_dt = _parse_github_timestamp(since)
    if since is not None and since_dt is None:
        logger.warning(
            f"Ignoring unparseable `since` value {since!r}; falling back to full extraction"
        )
    incremental = since_dt is not None

    base_url = f"{github_api_url}/repos/{repo}/pulls"
    params: dict = {
        "state": "all",
        "per_page": chunk_size,
        "sort": "updated" if incremental else "created",
        "direction": "desc" if incremental else "asc",
    }

    total = 0
    pages = 0

    while True:
        if refresh_auth:
            refresh_auth()
        resp = github_get(session, base_url, params=params, refresh_auth=refresh_auth)

        batch = resp.json()
        pages += 1

        # In incremental mode the batch is sorted by updated_at descending, so the
        # first PR older than `since` marks the watermark: it and everything after
        # it is older, so we keep only the PRs at-or-after `since` and stop paging.
        reached_watermark = False
        if incremental:
            kept: list[dict] = []
            for pr in batch:
                updated_dt = _parse_github_timestamp(pr.get("updated_at"))
                if updated_dt is not None and updated_dt < since_dt:
                    reached_watermark = True
                    break
                kept.append(pr)
            batch = kept

        total += len(batch)

        if len(batch) > 0:
            logger.info(
                f"Extracted page {pages} with {len(batch)} PRs (total: {total})"
            )

            for _idx, pr in enumerate(batch):
                pr_number = pr.get("number")
                if not pr_number:
                    continue
                pr["commit_data"] = extract_commits(
                    session, repo, pr_number, github_api_url, refresh_auth=refresh_auth
                )
                pr["reviewer_data"] = extract_reviewers(
                    session, repo, pr_number, github_api_url, refresh_auth=refresh_auth
                )
                pr["comment_data"] = extract_comments(
                    session, repo, pr_number, github_api_url, refresh_auth=refresh_auth
                )

            yield batch

        if reached_watermark:
            break

        # Pagination
        next_url = resp.links.get("next", {}).get("url")
        if not next_url or len(batch) == 0:
            break
        # Parse the next URL and extract the page parameter
        parsed_url = urlparse(next_url)
        query_params = parse_qs(parsed_url.query)
        # Update only the page parameter, preserving other params
        if "page" not in query_params or not query_params["page"]:
            # If no page parameter, this is unexpected - log and stop pagination
            logger.warning("No page parameter in next URL, stopping pagination")
            break

        try:
            page_num = int(query_params["page"][0])
            if page_num > 0:
                params["page"] = page_num
            else:
                logger.warning(
                    f"Invalid page number {page_num} in next URL, stopping pagination"
                )
                break
        except (ValueError, IndexError) as e:
            logger.warning(
                f"Invalid page parameter in next URL: {e}, stopping pagination"
            )
            break

    logger.info(f"Data extraction completed. Total PRs: {total}, Pages: {pages}")


def extract_commits(
    session: requests.Session,
    repo: str,
    pr_number: int,
    github_api_url: str = "https://api.github.com",
    refresh_auth: Callable[[], None] | None = None,
) -> list[dict]:
    """
    Extract commits and files for a specific pull request.

    Args:
        session: Authenticated requests session
        repo: GitHub repository name
        pr_number: Pull request number
        github_api_url: GitHub API base URL
    Returns:
        List of commit dictionaries for the pull request
    """
    logger.info(f"Extracting commits for PR #{pr_number}")

    commits_url = f"{github_api_url}/repos/{repo}/pulls/{pr_number}/commits"

    logger.info(f"Commits URL: {commits_url}")

    resp = github_get(session, commits_url, refresh_auth=refresh_auth)

    commits = resp.json()
    for commit in commits:
        commit_sha = commit.get("sha")
        commit_url = f"{github_api_url}/repos/{repo}/commits/{commit_sha}"
        commit_data = github_get(session, commit_url, refresh_auth=refresh_auth).json()
        commit["files"] = commit_data.get("files", [])

    logger.info(f"Extracted {len(commits)} commits for PR #{pr_number}")
    return commits


def extract_reviewers(
    session: requests.Session,
    repo: str,
    pr_number: int,
    github_api_url: str = "https://api.github.com",
    refresh_auth: Callable[[], None] | None = None,
) -> list[dict]:
    """
    Extract reviewers for a specific pull request.

    Args:
        session: Authenticated requests session
        repo: GitHub repository name
        pr_number: Pull request number
        github_api_url: GitHub API base URL
    Returns:
        List of reviewer dictionaries for the pull request
    """
    logger.info(f"Extracting reviewers for PR #{pr_number}")

    reviewers_url = f"{github_api_url}/repos/{repo}/pulls/{pr_number}/reviews"

    logger.info(f"Reviewers URL: {reviewers_url}")

    reviewers = github_get(session, reviewers_url, refresh_auth=refresh_auth).json()

    filtered = [r for r in reviewers if r.get("user") is not None]
    skipped = len(reviewers) - len(filtered)
    if skipped:
        logger.info(f"Skipped {skipped} reviewer(s) with null user for PR #{pr_number}")

    logger.info(f"Extracted {len(filtered)} reviewers for PR #{pr_number}")
    return filtered


def extract_comments(
    session: requests.Session,
    repo: str,
    pr_number: int,
    github_api_url: str = "https://api.github.com",
    refresh_auth: Callable[[], None] | None = None,
) -> list[dict]:
    """
    Extract comments for a specific pull request.

    Args:
        session: Authenticated requests session
        repo: GitHub repository name
        pr_number: Pull request number
        github_api_url: GitHub API base URL
    Returns:
        List of comment dictionaries for the pull request
    """
    logger.info(f"Extracting comments for PR #{pr_number}")

    comments_url = f"{github_api_url}/repos/{repo}/issues/{pr_number}/comments"

    logger.info(f"Comments URL: {comments_url}")

    comments = github_get(session, comments_url, refresh_auth=refresh_auth).json()

    filtered = [c for c in comments if c.get("user") is not None and c.get("body")]
    skipped = len(comments) - len(filtered)
    if skipped:
        logger.info(
            f"Skipped {skipped} comment(s) with null user or empty body for PR #{pr_number}"
        )

    logger.info(f"Extracted {len(filtered)} comments for PR #{pr_number}")
    return filtered


def sleep_for_rate_limit(resp: requests.Response) -> None:
    """Sleep until the rate limit resets.

    Handles both the primary limit (``X-RateLimit-Remaining: 0`` plus an
    ``X-RateLimit-Reset`` epoch) and the secondary/abuse limit (a
    ``Retry-After`` header giving a delay in seconds). When both are present
    the longer wait wins.
    """
    sleep_time = 0
    remaining = int(resp.headers.get("X-RateLimit-Remaining", 1))
    reset = int(resp.headers.get("X-RateLimit-Reset", 0))
    if remaining == 0:
        sleep_time = max(sleep_time, reset - int(time.time()))

    retry_after = resp.headers.get("Retry-After")
    if retry_after is not None:
        try:
            sleep_time = max(sleep_time, int(retry_after))
        except ValueError:
            # Retry-After may be an HTTP-date; ignore and fall back to reset.
            pass

    sleep_time = max(0, sleep_time)
    if sleep_time > 0:
        print(
            f"Rate limit exceeded. Sleeping for {sleep_time} seconds.", file=sys.stderr
        )
        time.sleep(sleep_time)


def _is_html_error_page(resp: requests.Response) -> bool:
    """Return True when GitHub returns a non-JSON error response."""
    content_type = resp.headers.get("Content-Type", "")
    return "application/json" not in content_type and resp.status_code >= 400


def github_request(
    session: requests.Session,
    method: str,
    url: str,
    *,
    params: dict | None = None,
    refresh_auth: Callable[[], None] | None = None,
    expected_status: int = 200,
) -> requests.Response:
    """
    Make a GitHub API request, retrying on transient errors and expired tokens.

    Retry behaviour:
    - 403 rate-limit: sleeps until reset, then retries (unbounded, existing behaviour).
    - 401 bad credentials: calls refresh_auth() then retries, up to
      _MAX_AUTH_RETRIES times.  If refresh_auth is None the error is
      treated as non-retryable.
    - 5xx / HTML error page: exponential backoff up to _MAX_RETRIES attempts.
    - Network-level errors (Timeout, ConnectionError): same exponential backoff.
    - All other non-200 responses (404, 422 ...): raises SystemExit immediately.

    Args:
        session: Authenticated requests session
        url: URL to fetch
        params: Optional query parameters
        refresh_auth: Optional callable that refreshes the session's Authorization
            header.  Called automatically when a 401 response is received.

    Returns:
        Successful response (status 200)

    Raises:
        SystemExit: When all retries are exhausted or a non-retryable error occurs.
    """
    auth_retries = _MAX_AUTH_RETRIES
    transient_retries = _MAX_RETRIES
    backoff = _RETRY_BASE_DELAY

    while True:
        try:
            resp = getattr(session, method.lower())(
                url, params=params, timeout=_REQUEST_TIMEOUT
            )
        except (
            requests.exceptions.Timeout,
            requests.exceptions.ConnectionError,
        ) as exc:
            if transient_retries > 0:
                logger.warning(
                    f"Network error for {url}: {exc}. "
                    f"Retrying in {backoff:.0f}s ({transient_retries} retries left)"
                )
                backoff = _apply_backoff(backoff)
                transient_retries -= 1
                continue
            raise TooManyRetriesError(
                f"GitHub API request failed after retries for {url}: {exc}"
            )

        if resp.status_code == expected_status:
            return resp

        if _is_rate_limited(resp):
            sleep_for_rate_limit(resp)
            continue

        if resp.status_code == 401:
            if auth_retries > 0 and refresh_auth is not None:
                logger.warning(
                    f"401 for {url}, refreshing auth token ({auth_retries} retries left)"
                )
                refresh_auth()
                auth_retries -= 1
                continue
            if refresh_auth is None:
                auth_error_detail = "with no refresh_auth configured"
            else:
                auth_error_detail = f"after {_MAX_AUTH_RETRIES} refresh attempts"
            raise TooManyRetriesError(
                f"GitHub API auth error 401 for {url} {auth_error_detail}: "
                f"{resp.text or 'No response text'}"
            )

        if resp.status_code in _RETRYABLE_STATUS_CODES or _is_html_error_page(resp):
            if transient_retries > 0:
                logger.warning(
                    f"Transient error {resp.status_code} for {url}. "
                    f"Retrying in {backoff:.0f}s ({transient_retries} retries left)"
                )
                backoff = _apply_backoff(backoff)
                transient_retries -= 1
                continue
            raise TooManyRetriesError(
                f"GitHub API error {resp.status_code} for {url} after {_MAX_RETRIES} retries: "
                f"{resp.text or 'No response text'}"
            )

        raise TooManyRetriesError(
            f"GitHub API error {resp.status_code} for {url}: {resp.text or 'No response text'}"
        )


def github_get(
    session: requests.Session,
    url: str,
    params: dict | None = None,
    refresh_auth: Callable[[], None] | None = None,
) -> requests.Response:
    """Convenience wrapper around :func:`github_request` for GET requests."""
    return github_request(session, "GET", url, params=params, refresh_auth=refresh_auth)


def transform_data(raw_data: list[dict], repo: str) -> dict:
    """
    Transform GitHub pull request data into BigQuery-compatible format.

    Args:
        raw_data: List of pull request dictionaries from GitHub API

    Returns:
        List of transformed pull requests, commits, reviewers, and comments ready for BigQuery
    """
    logger.info(f"Starting data transformation for {len(raw_data)} PRs")

    transformed_data: dict = {
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
            "date_approved": None,  # This will be filled later
            "labels": (
                [label.get("name") for label in pr.get("labels", [])]
                if pr.get("labels")
                else []
            ),
        }

        # Extract and flatten commit data
        logger.info(f"Transforming commits for PR #{pr.get('number')}")
        for commit in pr["commit_data"]:
            for file in commit["files"]:
                transformed_commit = {
                    "pull_request_id": pr.get("number"),
                    "target_repository": repo,
                    "commit_sha": commit.get("sha"),
                    "date_created": commit.get("commit", {})
                    .get("author", {})
                    .get("date"),
                    "author_username": commit.get("commit", {})
                    .get("author", {})
                    .get("name"),
                    "author_email": None,  # TODO Placeholder for author email extraction logic
                    "filename": file.get("filename"),
                    "lines_removed": file.get("deletions"),
                    "lines_added": file.get("additions"),
                }
                transformed_data["commits"].append(transformed_commit)

        # Extract and flatten reviewer data
        review_id_statuses = {}
        logger.info(f"Transforming reviewers for PR #{pr.get('number')}")
        for review in pr["reviewer_data"]:
            # Store the review state for adding to the comments table later
            review_id = review.get("id")
            review_id_statuses[review_id] = review.get("state")

            transformed_reviewer = {
                "pull_request_id": pr.get("number"),
                "target_repository": repo,
                "date_reviewed": review.get("submitted_at"),
                "reviewer_email": None,  # TODO Placeholder for reviewer email extraction logic
                "reviewer_username": (review.get("user") or {}).get("login"),
                "status": review.get("state"),
            }
            transformed_data["reviewers"].append(transformed_reviewer)

            # If the request is approved then store the date in the
            # date_approved for the pull request
            if review.get("state") == "APPROVED":
                approved_date = review.get("submitted_at")
                if transformed_pr.get(
                    "date_approved"
                ) is None or approved_date < transformed_pr.get("date_approved"):
                    transformed_pr["date_approved"] = approved_date

        # Extract and flatten comment data
        logger.info(f"Transforming comments for PR #{pr.get('number')}")
        for comment in pr["comment_data"]:
            transformed_comment = {
                "pull_request_id": pr.get("number"),
                "target_repository": repo,
                "comment_id": comment.get("id"),
                "date_created": comment.get("created_at"),
                "author_email": None,  # TODO Placeholder for reviewer email extraction logic
                "author_username": comment.get("user", {}).get("login"),
                "character_count": (
                    len(comment.get("body", "")) if comment.get("body") else 0
                ),
                "status": None,  # TODO
            }

            # If we stored a review state for this comment earlier we can add it now
            pr_review_id = comment.get("pull_request_review_id")
            if pr_review_id in review_id_statuses:
                transformed_comment["status"] = review_id_statuses[pr_review_id]

            transformed_data["comments"].append(transformed_comment)

        transformed_data["pull_requests"].append(transformed_pr)

    logger.info(
        f"Data transformation completed for {len(transformed_data['pull_requests'])} PRs"
    )

    return transformed_data


def snapshot_exists(
    client: bigquery.Client,
    dataset_id: str,
    repo: str,
    snapshot_date: str,
) -> bool:
    """
    Check if data already exists in BigQuery for the given repo and snapshot date.

    Queries the pull_requests table as a sentinel — if rows exist there for this
    (repo, snapshot_date) pair, we treat the repo as already processed for today.

    Args:
        client: BigQuery client instance
        dataset_id: BigQuery dataset ID
        repo: Repository in "owner/repo" format
        snapshot_date: Snapshot date string in YYYY-MM-DD format

    Returns:
        True if data already exists, False otherwise
    """
    query = f"""
        SELECT 1
        FROM `{client.project}.{dataset_id}.pull_requests`
        WHERE snapshot_date = @snapshot_date
          AND target_repository = @repo
        LIMIT 1
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("snapshot_date", "DATE", snapshot_date),
            bigquery.ScalarQueryParameter("repo", "STRING", repo),
        ]
    )
    try:
        results = list(client.query(query, job_config=job_config).result())
        return len(results) > 0
    except api_exceptions.NotFound as e:
        # A missing table is expected on first run (no snapshot yet).
        # A missing dataset means BIGQUERY_DATASET is misconfigured — re-raise so it
        # fails loudly rather than proceeding silently until insert_rows_json fails.
        if f"datasets/{dataset_id}" in str(e):
            logger.error(
                f"BigQuery dataset '{dataset_id}' not found — check BIGQUERY_DATASET config: {e}"
            )
            raise
        logger.info(
            f"Table pull_requests not found in {dataset_id}, treating as no existing snapshot"
        )
        return False


def delete_existing_snapshot(
    client: bigquery.Client,
    dataset_id: str,
    repo: str,
    snapshot_date: str,
) -> None:
    """
    Delete all rows for (repo, snapshot_date) across all tables before a fresh load.

    This makes loads idempotent: if a previous run crashed mid-way and left partial
    data, a rerun will clean up the partial write and reload everything cleanly.

    Args:
        client: BigQuery client instance
        dataset_id: BigQuery dataset ID
        repo: Repository in "owner/repo" format
        snapshot_date: Snapshot date string in YYYY-MM-DD format
    """
    tables = ["pull_requests", "commits", "reviewers", "comments"]
    for table in tables:
        dml = f"""
            DELETE FROM `{client.project}.{dataset_id}.{table}`
            WHERE snapshot_date = @snapshot_date
              AND target_repository = @repo
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("snapshot_date", "DATE", snapshot_date),
                bigquery.ScalarQueryParameter("repo", "STRING", repo),
            ]
        )
        client.query(dml, job_config=job_config).result()
        logger.info(
            f"Deleted existing snapshot rows from {table} for {repo} on {snapshot_date}"
        )


def get_prior_snapshot_watermark(
    client: bigquery.Client,
    dataset_id: str,
    repo: str,
    snapshot_date: str,
) -> tuple[str | None, datetime | None]:
    """
    Find the most recent prior snapshot for a repo and its modification watermark.

    Used by incremental sync to (a) know which snapshot to carry forward and
    (b) compute the ``since`` floor for fetching only changed PRs. Both values
    come from a single aggregate query against the pull_requests table.

    Args:
        client: BigQuery client instance
        dataset_id: BigQuery dataset ID
        repo: Repository in "owner/repo" format
        snapshot_date: Today's snapshot date (YYYY-MM-DD); the search is bounded
            to strictly earlier snapshots so a partial/in-progress run for today
            never seeds its own watermark.

    Returns:
        A ``(prior_date, watermark)`` tuple:
          - prior_date: the latest snapshot_date strictly before *snapshot_date*
            as a "YYYY-MM-DD" string, or None if this repo has no prior snapshot
            (first run / new repo / missing table).
          - watermark: the maximum date_modified across that prior data as an
            aware UTC datetime, or None. A non-None *prior_date* with a None
            *watermark* (all date_modified NULL) means no usable ``since`` floor
            exists — callers should fall back to a full fetch in that case.
    """
    # Pin the watermark to the *same* snapshot we carry forward. Computing both
    # MAX()s in one flat aggregate would let MAX(date_modified) be drawn from an
    # older snapshot than MAX(snapshot_date), so a partial latest snapshot (or one
    # with NULL date_modified rows) could yield a watermark ahead of its own data
    # and silently skip changed PRs. The CTE finds the latest prior snapshot_date
    # first, then takes the watermark only from rows in that snapshot.
    table = f"`{client.project}.{dataset_id}.pull_requests`"
    query = f"""
        WITH prior AS (
            SELECT MAX(snapshot_date) AS prior_date
            FROM {table}
            WHERE target_repository = @repo
              AND snapshot_date < @snapshot_date
        )
        SELECT prior.prior_date AS prior_date, MAX(pr.date_modified) AS watermark
        FROM prior
        LEFT JOIN {table} AS pr
          ON pr.target_repository = @repo
         AND pr.snapshot_date = prior.prior_date
        GROUP BY prior.prior_date
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("snapshot_date", "DATE", snapshot_date),
            bigquery.ScalarQueryParameter("repo", "STRING", repo),
        ]
    )
    try:
        rows = list(client.query(query, job_config=job_config).result())
    except api_exceptions.NotFound as e:
        # Mirror snapshot_exists: a missing table is expected on first run, but a
        # missing dataset is a config error that should fail loudly.
        if f"datasets/{dataset_id}" in str(e):
            logger.error(
                f"BigQuery dataset '{dataset_id}' not found — check BIGQUERY_DATASET config: {e}"
            )
            raise
        logger.info(
            f"Table pull_requests not found in {dataset_id}, no prior snapshot for {repo}"
        )
        return None, None

    # An aggregate query always yields exactly one row in BigQuery; guard against an
    # empty result anyway (e.g. a mocked client) so callers get (None, None) rather
    # than an IndexError. prior_date is NULL when no earlier snapshot exists.
    if not rows:
        return None, None
    row = rows[0]
    if row.prior_date is None:
        return None, None

    prior_date = row.prior_date
    prior_date_str = (
        prior_date.isoformat() if hasattr(prior_date, "isoformat") else str(prior_date)
    )
    return prior_date_str, row.watermark


def carry_forward_snapshot(
    client: bigquery.Client,
    dataset_id: str,
    repo: str,
    prior_date: str,
    snapshot_date: str,
) -> None:
    """
    Copy a repo's prior snapshot rows into today's snapshot via INSERT...SELECT.

    This re-stamps every row from *prior_date* with *snapshot_date* across all
    four tables, giving today a complete baseline; incremental sync then overlays
    only the PRs that changed. INSERT...SELECT keeps the data in BigQuery (no
    round-trip through this process).

    IMPORTANT: callers must run delete_existing_snapshot(snapshot_date) BEFORE
    this so a same-day re-run does not double-insert the carried-forward rows.

    Args:
        client: BigQuery client instance
        dataset_id: BigQuery dataset ID
        repo: Repository in "owner/repo" format
        prior_date: Source snapshot date (YYYY-MM-DD) to copy from, e.g. the
            prior_date returned by get_prior_snapshot_watermark().
        snapshot_date: Target snapshot date (YYYY-MM-DD), i.e. today.
    """
    for table, columns in _TABLE_COLUMNS.items():
        # Column identifiers come from the constant _TABLE_COLUMNS, never user
        # input, so joining them into the SQL is safe; only the values are bound.
        col_list = ", ".join(columns)
        # CAST the date param explicitly to DATE: it is a no-op against production
        # BigQuery but makes the projected column's type unambiguous (the emulator's
        # analyzer otherwise infers STRING and rejects the INSERT into a DATE column).
        dml = f"""
            INSERT INTO `{client.project}.{dataset_id}.{table}` ({col_list}, snapshot_date)
            SELECT {col_list}, CAST(@snapshot_date AS DATE)
            FROM `{client.project}.{dataset_id}.{table}`
            WHERE target_repository = @repo
              AND snapshot_date = @prior_date
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("snapshot_date", "DATE", snapshot_date),
                bigquery.ScalarQueryParameter("prior_date", "DATE", prior_date),
                bigquery.ScalarQueryParameter("repo", "STRING", repo),
            ]
        )
        client.query(dml, job_config=job_config).result()
        logger.info(
            f"Carried forward {table} rows for {repo} from {prior_date} to {snapshot_date}"
        )


def _insert_rows_to_table(
    client: bigquery.Client,
    table: str,
    table_ref: str,
    rows: list,
    use_streaming_insert: bool,
) -> None:
    """
    Write rows to a single BigQuery table via streaming insert or load job.

    Args:
        client: BigQuery client instance
        table: Table name (used only in error messages)
        table_ref: Fully-qualified table reference (project.dataset.table)
        rows: List of row dicts to write
        use_streaming_insert: If True, use streaming insert (emulator only).
            If False, use a load job so rows are immediately mutable (no
            streaming buffer restriction), allowing DELETE to work right away.
    """
    if use_streaming_insert:
        # The BigQuery emulator does not support load jobs; fall back to streaming
        # inserts for local testing. The streaming buffer restriction that prevents
        # immediate DELETE/UPDATE does not apply to the emulator.
        errors = client.insert_rows_json(table_ref, rows)
        if errors:
            error_msg = f"BigQuery insert errors for table {table}: {errors}"
            logger.error(error_msg)
            raise Exception(error_msg)
    else:
        # Load jobs write directly to storage, so rows are immediately mutable —
        # this allows DELETE in delete_existing_snapshot() to work without hitting
        # the streaming buffer restriction.
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        )
        load_job = client.load_table_from_json(rows, table_ref, job_config=job_config)
        load_job.result()

        if load_job.errors:
            error_msg = f"BigQuery load errors for table {table}: {load_job.errors}"
            logger.error(error_msg)
            raise Exception(error_msg)


def load_data(
    client: bigquery.Client,
    dataset_id: str,
    transformed_data: dict,
    snapshot_date: str | None = None,
    use_streaming_insert: bool = False,
) -> None:
    """
    Load transformed data to BigQuery using the Python client library.

    Args:
        client: BigQuery client instance
        dataset_id: BigQuery dataset ID
        transformed_data: Dictionary containing tables ('pull_requests',
            'commits', 'reviewers', 'comments') mapped to lists of row dictionaries
        snapshot_date: Snapshot date string in YYYY-MM-DD format, computed once by the
            caller to avoid date-boundary skew between the existence check and inserts
        use_streaming_insert: If True, use the streaming insert API instead of a load
            job. Use only against the BigQuery emulator, which does not support load
            jobs. In production, load jobs are preferred because rows are immediately
            mutable (no streaming buffer restriction).
    """

    if snapshot_date is None:
        snapshot_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    if not transformed_data:
        logger.warning("No data to load, skipping")
        return

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

        table_ref = f"{client.project}.{dataset_id}.{table}"
        logger.info(table_ref)

        _insert_rows_to_table(
            client=client,
            table=table,
            table_ref=table_ref,
            rows=load_table_data,
            use_streaming_insert=use_streaming_insert,
        )

        logger.info(
            f"Data loading completed successfully for table {table} "
            + f"with {len(load_table_data)} rows"
        )


def reconcile_delta(
    client: bigquery.Client,
    dataset_id: str,
    repo: str,
    transformed_delta: dict,
    snapshot_date: str,
    use_streaming_insert: bool = False,
) -> None:
    """
    Overlay a repo's changed-PR delta onto today's carried-forward snapshot.

    Given the freshly fetched-and-transformed data for only the PRs that changed
    since the last run, this replaces those PRs' carried-forward rows (written by
    carry_forward_snapshot) with the new data, across all four tables:

      1. The set of changed pull_request_ids (from the delta's pull_requests rows)
         is the reconcile key for EVERY table. Keying child deletes on the parent
         id set — not each child's own rows — means a changed PR's stale child rows
         (e.g. a removed comment) are deleted even when the delta now has zero
         child rows for that PR.
      2. Delete those PRs' rows from today's snapshot for this repo.
      3. Insert the fresh delta via load_data (which stamps snapshot_date and
         honors the streaming/load-job toggle).

    A PR created since the watermark is in the delta but not the carried-forward
    baseline: its delete matches nothing and its insert simply adds it.

    DELETE runs before INSERT and only removes rows produced by
    carry_forward_snapshot's INSERT...SELECT (managed storage, immediately
    mutable), so there is no streaming-buffer conflict.

    Args:
        client: BigQuery client instance
        dataset_id: BigQuery dataset ID
        repo: Repository in "owner/repo" format
        transformed_delta: transform_data() output for the changed PRs only
            (tables 'pull_requests', 'commits', 'reviewers', 'comments').
        snapshot_date: Today's snapshot date (YYYY-MM-DD).
        use_streaming_insert: Passed through to load_data (emulator only).
    """
    pr_ids = sorted(
        {
            int(row["pull_request_id"])
            for row in transformed_delta.get("pull_requests", [])
            if row.get("pull_request_id") is not None
        }
    )

    if not pr_ids:
        # Nothing changed since the watermark; the carried-forward snapshot already
        # stands as today's data, so there is nothing to replace.
        logger.info(f"No changed PRs to reconcile for {repo} on {snapshot_date}")
        return

    # pr_ids are validated integers, so inlining them as integer literals is
    # injection-safe. This is also more portable than a typed array parameter (the
    # BigQuery emulator infers an array param's element type as STRING, breaking
    # IN UNNEST against the INT64 pull_request_id column). Chunk the ids so the
    # inlined IN (...) list stays bounded no matter how many PRs changed in a day.
    for table in _TABLE_COLUMNS:
        for batch in itertools.batched(pr_ids, _RECONCILE_ID_BATCH_SIZE, strict=False):
            id_list = ", ".join(str(pr_id) for pr_id in batch)
            dml = f"""
                DELETE FROM `{client.project}.{dataset_id}.{table}`
                WHERE snapshot_date = @snapshot_date
                  AND target_repository = @repo
                  AND pull_request_id IN ({id_list})
            """
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter(
                        "snapshot_date", "DATE", snapshot_date
                    ),
                    bigquery.ScalarQueryParameter("repo", "STRING", repo),
                ]
            )
            client.query(dml, job_config=job_config).result()
        logger.info(
            f"Reconciled {table}: removed prior rows for {len(pr_ids)} changed PR(s) "
            f"for {repo} on {snapshot_date}"
        )

    load_data(
        client,
        dataset_id,
        transformed_delta,
        snapshot_date,
        use_streaming_insert=use_streaming_insert,
    )


def _build_session() -> requests.Session:
    """
    Create a ``requests.Session`` with the default GitHub API headers.

    Each worker thread gets its own session because ``requests.Session`` is not
    safe to share across threads and the per-repo ``Authorization`` header is
    rewritten on token refresh.
    """
    session = requests.Session()
    session.headers.update(
        {
            "Accept": "application/vnd.github+json",
            "User-Agent": "gh-pr-scraper/1.0 (+https://api.github.com)",
            "X-GitHub-Api-Version": "2022-11-28",
        }
    )
    return session


def _resolve_max_workers(repo_count: int) -> int:
    """
    Decide how many repos to process concurrently.

    Defaults to one worker per repo, capped at ``_DEFAULT_MAX_WORKERS`` to avoid
    spawning an unbounded number of threads (and GitHub API connections) for large
    repo lists. The cap is overridable via the ``GITHUB_ETL_MAX_WORKERS`` env var.

    Args:
        repo_count: Number of repositories to process

    Returns:
        Worker count, always at least 1.
    """
    cap = _DEFAULT_MAX_WORKERS
    override = os.environ.get("GITHUB_ETL_MAX_WORKERS")
    if override:
        try:
            parsed = int(override)
            if parsed > 0:
                cap = parsed
            else:
                logger.warning(
                    f"Ignoring non-positive GITHUB_ETL_MAX_WORKERS={override!r}"
                )
        except ValueError:
            logger.warning(f"Ignoring invalid GITHUB_ETL_MAX_WORKERS={override!r}")
    return max(1, min(repo_count, cap))


def _resolve_lookback_hours() -> int:
    """
    Resolve the incremental lookback window (hours) from the environment.

    Defaults to ``_DEFAULT_LOOKBACK_HOURS``; overridable via
    ``GITHUB_ETL_LOOKBACK_HOURS``. Accepts any value >= 0 (0 means no buffer);
    a negative or non-integer value is ignored and the default is used.

    Returns:
        Lookback window in hours.
    """
    override = os.environ.get("GITHUB_ETL_LOOKBACK_HOURS")
    if override:
        try:
            parsed = int(override)
            if parsed >= 0:
                return parsed
            logger.warning(f"Ignoring negative GITHUB_ETL_LOOKBACK_HOURS={override!r}")
        except ValueError:
            logger.warning(f"Ignoring invalid GITHUB_ETL_LOOKBACK_HOURS={override!r}")
    return _DEFAULT_LOOKBACK_HOURS


def _env_flag(name: str) -> bool:
    """Return True if env var *name* is set to a truthy value (1/true/yes/on)."""
    return os.environ.get(name, "").strip().lower() in {"1", "true", "yes", "on"}


def _format_since(watermark: datetime, lookback_hours: int) -> str:
    """
    Build the incremental ``since`` floor from a watermark and lookback window.

    Subtracts *lookback_hours* from *watermark* and formats the result as a GitHub
    ISO-8601 UTC timestamp (``YYYY-MM-DDTHH:MM:SSZ``) matching what
    extract_pull_requests compares against. The watermark is the UTC-aware
    MAX(date_modified) from BigQuery.
    """
    since_dt = watermark - timedelta(hours=lookback_hours)
    return since_dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _make_refresh_auth(
    session: requests.Session,
    repo: str,
    github_app_id: str,
    github_private_key: str,
    github_api_url: str,
) -> Callable[[], None]:
    """
    Build a callable that refreshes *session*'s Authorization header for *repo*.

    The returned callable is invoked by the extraction generator before each page
    fetch, so every API request (PRs + commits + reviewers + comments) uses a
    valid token. The token_store cache means it only hits the GitHub API when the
    cached token has <60 seconds remaining.
    """

    def refresh_auth() -> None:
        try:
            app_jwt = generate_github_jwt(github_app_id, github_private_key)
            access_token = get_installation_access_token(app_jwt, repo, github_api_url)
        except Exception as e:
            raise RuntimeError(
                f"Failed to obtain GitHub App access token for {repo}: {e}. "
                "Check that GITHUB_APP_ID is correct and GITHUB_PRIVATE_KEY "
                "is a valid PEM-encoded RSA private key."
            ) from e
        session.headers["Authorization"] = f"Bearer {access_token}"

    return refresh_auth


def process_repo(
    repo: str,
    github_app_id: str | None,
    github_private_key: str | None,
    github_api_url: str,
    bigquery_client: bigquery.Client,
    bigquery_dataset: str,
    snapshot_date: str,
    use_streaming_insert: bool,
    lookback_hours: int = _DEFAULT_LOOKBACK_HOURS,
    full_refresh: bool = False,
) -> int:
    """
    Run the extract/transform/load pipeline for a single repository.

    This is the unit of work executed per worker thread. It creates its own
    ``requests.Session`` so that repos processed concurrently never share or
    clobber each other's ``Authorization`` header (installation access tokens are
    cached per installation, and the header is rewritten on refresh).

    Chooses between two strategies per repo:
      - Full export (first run for the repo, or *full_refresh*): stream every PR
        in chunks straight into today's snapshot.
      - Incremental: carry the most recent prior snapshot forward into today, then
        fetch only PRs updated since the prior watermark (minus *lookback_hours*)
        and reconcile them into today's snapshot. A PR whose only change did not
        bump its updated_at can be missed until the next full refresh; the lookback
        buffer makes this rare.

    Args:
        repo: Repository in "owner/repo" format
        github_app_id: GitHub App ID, or None to run unauthenticated
        github_private_key: RSA private key (PEM), or None to run unauthenticated
        github_api_url: GitHub API base URL
        bigquery_client: Shared BigQuery client (thread-safe for queries/loads)
        bigquery_dataset: BigQuery dataset ID
        snapshot_date: Snapshot date string in YYYY-MM-DD format
        use_streaming_insert: Whether to use streaming inserts (emulator only)
        lookback_hours: Hours subtracted from the prior watermark to form the
            incremental ``since`` floor.
        full_refresh: When True, always do a full export regardless of prior data.

    Returns:
        Number of PRs processed for this repo.
    """
    # Each thread gets its own session; requests.Session is not safe to share
    # across threads and we rewrite the Authorization header per repo.
    session = _build_session()

    # Build a per-repo token refresh callable when running authenticated. Bound
    # once here (no None-then-redeclare) so the name has a single, clear type.
    refresh_auth = (
        _make_refresh_auth(
            session, repo, github_app_id, github_private_key, github_api_url
        )
        if github_app_id and github_private_key
        else None
    )
    if refresh_auth is not None:
        # Set the token immediately so the first generator page is authenticated.
        refresh_auth()

    # Look up the most recent prior snapshot and its modification watermark before
    # touching today's rows (this only reads snapshots earlier than today).
    prior_date, watermark = get_prior_snapshot_watermark(
        bigquery_client, bigquery_dataset, repo, snapshot_date
    )

    # Re-run safety: clear any partial/previous rows for today before (re)building.
    # If a previous run crashed mid-way, a rerun cleans up the partial write and
    # rebuilds today's snapshot cleanly.
    if snapshot_exists(bigquery_client, bigquery_dataset, repo, snapshot_date):
        logger.info(
            f"Deleting partial/existing snapshot for {repo} on {snapshot_date} before reload"
        )
        delete_existing_snapshot(bigquery_client, bigquery_dataset, repo, snapshot_date)

    # Incremental sync needs a prior snapshot to carry forward and a usable
    # watermark to bound the fetch. Without both (first run, new repo, or all
    # date_modified NULL), or when a full refresh is forced, do a full export.
    incremental = not full_refresh and prior_date is not None and watermark is not None

    if not incremental:
        if full_refresh:
            reason = "full refresh forced"
        elif prior_date is None:
            reason = "no prior snapshot"
        else:
            reason = f"prior snapshot {prior_date} has no usable watermark"
        # Drop any watermark so nothing downstream of this fork can mistake a stale
        # value for a usable incremental floor.
        watermark = None
        logger.info(f"[{repo}] Full export ({reason})")
        processed = 0
        for chunk_count, chunk in enumerate(
            extract_pull_requests(
                session,
                repo,
                chunk_size=100,
                github_api_url=github_api_url,
                refresh_auth=refresh_auth,
            ),
            start=1,
        ):
            logger.info(
                f"[{repo}] Processing chunk {chunk_count} with {len(chunk)} PRs"
            )
            transformed_data = transform_data(chunk, repo)
            load_data(
                bigquery_client,
                bigquery_dataset,
                transformed_data,
                snapshot_date,
                use_streaming_insert=use_streaming_insert,
            )
            processed += len(chunk)
            logger.info(
                f"[{repo}] Completed chunk {chunk_count}. PRs processed for repo: {processed}"
            )
        return processed

    # Incremental path: baseline today from the prior snapshot, then overlay only
    # the PRs that changed since the watermark.
    since = _format_since(watermark, lookback_hours)
    logger.info(
        f"[{repo}] Incremental export: carrying forward {prior_date}, "
        f"fetching PRs updated since {since}"
    )
    carry_forward_snapshot(
        bigquery_client, bigquery_dataset, repo, prior_date, snapshot_date
    )

    # The delta is only the changed PRs, so accumulating it in memory (rather than
    # loading per chunk like the full path) is cheap and lets reconcile run once.
    delta: dict = {
        "pull_requests": [],
        "commits": [],
        "reviewers": [],
        "comments": [],
    }
    processed = 0
    for chunk_count, chunk in enumerate(
        extract_pull_requests(
            session,
            repo,
            chunk_size=100,
            github_api_url=github_api_url,
            refresh_auth=refresh_auth,
            since=since,
        ),
        start=1,
    ):
        logger.info(
            f"[{repo}] Processing changed chunk {chunk_count} with {len(chunk)} PRs"
        )
        transformed_data = transform_data(chunk, repo)
        for key in delta:
            delta[key].extend(transformed_data.get(key, []))
        processed += len(chunk)

    reconcile_delta(
        bigquery_client,
        bigquery_dataset,
        repo,
        delta,
        snapshot_date,
        use_streaming_insert=use_streaming_insert,
    )
    logger.info(f"[{repo}] Reconciled {processed} changed PR(s)")
    return processed


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
    try:
        return _main()
    except RuntimeError as e:
        logger.error(str(e))
        return 1


def _main() -> int:
    logger.info("Starting GitHub ETL process with chunked processing")

    github_app_id = os.environ.get("GITHUB_APP_ID") or None
    github_private_key = os.environ.get("GITHUB_PRIVATE_KEY") or None
    if github_private_key:
        # Environment variables passed via Docker / CI often serialize newlines as the
        # two-character sequence \n.  The RSA PEM format requires real newlines, so we
        # normalize here before the key is used for JWT signing.
        github_private_key = github_private_key.replace("\\n", "\n")
    if not github_app_id or not github_private_key:
        logger.warning(
            "GITHUB_APP_ID and GITHUB_PRIVATE_KEY are not set; "
            "proceeding without authentication (suitable for testing only)"
        )

    # Read BigQuery configuration
    bigquery_project = os.environ.get("BIGQUERY_PROJECT")
    bigquery_dataset = os.environ.get("BIGQUERY_DATASET")

    if not bigquery_project:
        raise SystemExit("Environment variable BIGQUERY_PROJECT is required")
    if not bigquery_dataset:
        raise SystemExit("Environment variable BIGQUERY_DATASET is required")

    github_api_url = os.environ.get("GITHUB_API_URL", "https://api.github.com")
    if os.environ.get("GITHUB_API_URL"):
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
    github_repos = []
    github_repos_str = os.getenv("GITHUB_REPOS")
    if github_repos_str:
        # Deduplicate while preserving order: with concurrent processing, a repo
        # listed twice would otherwise have its delete_existing_snapshot() and
        # load_data() interleave with its duplicate, corrupting the snapshot.
        github_repos = list(
            dict.fromkeys(r.strip() for r in github_repos_str.split(",") if r.strip())
        )
    else:
        raise SystemExit(
            "Environment variable GITHUB_REPOS is required (format: 'owner/repo,owner/repo')"
        )

    total_processed = 0
    snapshot_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    # Incremental sync configuration.
    lookback_hours = _resolve_lookback_hours()
    full_refresh = _env_flag("GITHUB_ETL_FULL_REFRESH")
    if full_refresh:
        logger.info("GITHUB_ETL_FULL_REFRESH set — forcing a full export for all repos")
    else:
        logger.info(f"Incremental sync enabled (lookback {lookback_hours}h)")

    failed_repos: list[str] = []

    # Each repo is independent (its own session, token, and BigQuery rows keyed by
    # target_repository), so they are processed concurrently. The work is I/O-bound
    # (GitHub API + BigQuery), so threads — not processes — are the right fit.
    max_workers = _resolve_max_workers(len(github_repos))
    logger.info(
        f"Processing {len(github_repos)} repo(s) with up to {max_workers} worker(s)"
    )

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_repo = {
            executor.submit(
                process_repo,
                repo,
                github_app_id,
                github_private_key,
                github_api_url,
                bigquery_client,
                bigquery_dataset,
                snapshot_date,
                bool(emulator_host),
                lookback_hours,
                full_refresh,
            ): repo
            for repo in github_repos
        }

        for future in concurrent.futures.as_completed(future_to_repo):
            repo = future_to_repo[future]
            try:
                processed = future.result()
            except Exception as exc:
                # Catch broadly so one repo's failure (a TooManyRetriesError, a
                # RuntimeError, or a bare Exception from load_data) is recorded as a
                # failed repo rather than propagating out of the executor and
                # discarding the results of other in-flight repos. logger.exception
                # records the worker thread's traceback for debugging in CI/prod.
                logger.exception(f"Failed to process repo {repo}: {exc}")
                failed_repos.append(repo)
                continue
            total_processed += processed
            logger.info(
                f"Finished repo {repo}: {processed} PRs. Total so far: {total_processed}"
            )

    if failed_repos:
        logger.error(
            f"ETL completed with failures. Failed repos: {', '.join(failed_repos)}"
        )
        return 1

    logger.info(
        f"GitHub ETL process completed successfully. Total PRs processed: {total_processed}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
