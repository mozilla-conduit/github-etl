#!/usr/bin/env python3
"""
Mock GitHub API server for testing the ETL pipeline locally.

This provides a minimal implementation of the GitHub PR API endpoints
needed for testing the ETL process without hitting rate limits.
"""

import random
from datetime import datetime, timedelta, timezone

from flask import Flask, jsonify, request

app = Flask(__name__)


DATE_FORMAT = "%Y-%m-%d %H:%M:00Z"


def generate_mock_pr(pr_number: int) -> dict:
    """Generate a mock pull request object."""
    created_date = datetime.now(timezone.utc) - timedelta(days=random.randint(1, 365))
    updated_date = created_date + timedelta(days=random.randint(0, 30))

    states = ["open", "closed"]
    state = random.choice(states)

    # Generate titles with various bug ID formats (about 80% have bug IDs)
    title_templates = [
        f"Bug {1800000 + pr_number} - Fix memory leak in component",
        f"Bug {1900000 + pr_number} - Add new feature for better UX",
        f"Bug {2000000 + pr_number} - Update dependencies to latest version",
        f"Bug {1700000 + pr_number} - Refactor rendering pipeline",
        f"Bug {1600000 + pr_number} - Improve performance of data loading",
        # Some PRs without bug IDs
        f"Refactor code structure in module {pr_number}",
        "Update documentation for API changes",
    ]

    # 80% chance of having a bug ID
    if random.random() < 0.8:
        title = random.choice(title_templates[:5])
    else:
        title = random.choice(title_templates[5:])

    return {
        "number": pr_number,
        "id": 1000000 + pr_number,
        "title": title,
        "state": state,
        "created_at": created_date.strftime(DATE_FORMAT),
        "updated_at": updated_date.strftime(DATE_FORMAT),
        "closed_at": updated_date.strftime(DATE_FORMAT) if state == "closed" else None,
        "merged_at": (
            updated_date.strftime(DATE_FORMAT)
            if state == "closed" and random.random() > 0.3
            else None
        ),
        "user": {
            "login": f"user{pr_number % 10}",
            "id": 5000 + (pr_number % 10),
        },
        "body": f"This is a mock pull request body for PR #{pr_number}.\n\nIt includes some description text.",
        "html_url": f"https://github.com/mozilla/firefox/pull/{pr_number}",
        "head": {
            "ref": f"feature/branch-{pr_number}",
            "sha": f"abc123{pr_number:04d}",
        },
        "base": {
            "ref": "main",
            "sha": f"def456{pr_number:04d}",
        },
        "draft": random.choice([True, False]),
        "merged": state == "closed" and random.random() > 0.3,
        "mergeable": random.choice([True, False, None]),
        "mergeable_state": random.choice(["clean", "dirty", "unstable", "blocked"]),
        "comments": random.randint(0, 25),
        "review_comments": random.randint(0, 15),
        "commits": random.randint(1, 20),
        "additions": random.randint(1, 500),
        "deletions": random.randint(1, 300),
        "changed_files": random.randint(1, 15),
        "labels": [
            {"name": label}
            for label in random.sample(
                [
                    "bug",
                    "enhancement",
                    "documentation",
                    "help wanted",
                    "good first issue",
                ],
                k=random.randint(0, 3),
            )
        ],
        "assignees": [{"login": f"assignee{i}"} for i in range(random.randint(0, 2))],
    }


def generate_mock_commits(pr_number: int, count: int) -> list:
    """Generate a list of mock commits for a PR."""
    commits = []
    for i in range(1, count + 1):
        created_date = datetime.now(timezone.utc) - timedelta(
            days=random.randint(1, 365)
        )
        commits.append(
            {
                "sha": f"commitsha{pr_number:04d}{i:02d}",
                "commit": {
                    "message": f"Mock commit message #{i} for PR #{pr_number}",
                    "author": {
                        "name": f"Author{i % 5}",
                        "email": f"author{i % 5}@example.com",
                        "date": created_date.strftime(DATE_FORMAT),
                    },
                },
                "author": {
                    "login": f"author{i % 5}",
                    "id": 4000 + (i % 5),
                },
                "html_url": "",
            }
        )

    return commits


def generate_mock_comments(pr_number: int, count: int) -> list:
    """Generate a list of mock comments for a PR."""
    comments = []
    for i in range(1, count + 1):
        created_date = datetime.now(timezone.utc) - timedelta(
            days=random.randint(1, 365)
        )
        comments.append(
            {
                "id": 2000000 + pr_number * 100 + i,
                "body": f"This is a mock comment #{i} on PR #{pr_number}.",
                "user": {
                    "login": f"commenter{i % 5}",
                    "id": 6000 + (i % 5),
                },
                "created_at": created_date.strftime(DATE_FORMAT),
                "updated_at": created_date.strftime(DATE_FORMAT),
                "html_url": "",
            }
        )

    return comments


def generate_mock_reviewers(pr_number: int, count: int) -> list[dict]:
    """Generate a list of mock reviewers for a PR."""
    reviewers = []

    # Define possible review states with weights for realistic distribution
    # 50% approved, 30% changes requested, 15% comment, 5% dismissed
    review_states = (
        ["APPROVED"] * 50
        + ["CHANGES_REQUESTED"] * 30
        + ["COMMENTED"] * 15
        + ["DISMISSED"] * 5
    )

    # Sample review body templates
    review_bodies = [
        "Looks good to me!",
        "LGTM, great work!",
        "Please address the following issues before merging.",
        "I have some concerns about the implementation.",
        "Nice improvements! Just a few minor suggestions.",
        "This needs more work before it's ready.",
        "Great job on this feature!",
        "Consider refactoring this section for better performance.",
    ]

    # Author association options
    author_associations = ["COLLABORATOR", "CONTRIBUTOR", "MEMBER", "OWNER"]

    for i in range(1, count + 1):
        # Generate submitted_at timestamp (reviews submitted after PR creation, going backwards in time)
        submitted_date = datetime.now(timezone.utc) - timedelta(
            hours=random.randint(1, 720)
        )

        # Select random review state
        state = random.choice(review_states)

        # Generate 40-character commit SHA
        commit_sha = f"{pr_number:08d}{i:02d}" + "".join(
            random.choices("0123456789abcdef", k=30)
        )

        review_id = 3000000 + pr_number * 100 + i
        reviewer_num = i % 5

        reviewers.append(
            {
                "id": review_id,
                "node_id": f"PRR_{review_id}",
                "user": {
                    "login": f"reviewer{reviewer_num}",
                    "id": 7000 + reviewer_num,
                    "type": "User",
                    "html_url": f"https://github.com/reviewer{reviewer_num}",
                },
                "body": random.choice(review_bodies),
                "state": state,
                "html_url": f"https://github.com/mozilla/firefox/pull/{pr_number}#pullrequestreview-{review_id}",
                "pull_request_url": f"https://api.github.com/repos/mozilla/firefox/pulls/{pr_number}",
                "_links": {
                    "html": {
                        "href": f"https://github.com/mozilla/firefox/pull/{pr_number}#pullrequestreview-{review_id}"
                    },
                    "pull_request": {
                        "href": f"https://api.github.com/repos/mozilla/firefox/pulls/{pr_number}"
                    },
                },
                "submitted_at": submitted_date.isoformat(),
                "commit_id": commit_sha,
                "author_association": random.choice(author_associations),
            }
        )

    return reviewers


def generate_mock_commit_files(commit_sha: str, count: int) -> list:
    """Generate a list of mock files for a commit."""
    file_extensions = [".py", ".js", ".ts", ".cpp", ".h", ".json", ".md", ".yml"]
    file_statuses = ["modified", "added", "removed", "renamed"]

    files = []
    for i in range(1, count + 1):
        ext = random.choice(file_extensions)
        status = random.choice(file_statuses)
        filename = f"src/components/module{i % 10}/file{i}{ext}"
        additions = random.randint(1, 100) if status != "removed" else 0
        deletions = random.randint(1, 50) if status != "added" else 0

        files.append(
            {
                "filename": filename,
                "status": status,
                "additions": additions,
                "deletions": deletions,
                "changes": additions + deletions,
                "blob_url": f"https://github.com/mozilla/firefox/blob/{commit_sha}/{filename}",
                "raw_url": f"https://github.com/mozilla/firefox/raw/{commit_sha}/{filename}",
                "contents_url": f"https://api.github.com/repos/mozilla/firefox/contents/{filename}?ref={commit_sha}",
                "sha": f"file{i:04d}{commit_sha[:8]}",
                "patch": (
                    f"@@ -1,{deletions} +1,{additions} @@\n-old line\n+new line"
                    if status != "added"
                    else None
                ),
            }
        )

    return files


@app.route("/repos/<owner>/<repo>/pulls", methods=["GET"])
def get_pulls(owner: str, repo: str) -> dict:
    """Mock endpoint for listing pull requests."""
    # Get pagination parameters
    per_page = int(request.args.get("per_page", 30))
    page = int(request.args.get("page", 1))

    # Generate mock PRs
    # For testing, let's generate 250 total PRs
    total_prs = 250
    start_pr = (page - 1) * per_page + 1
    end_pr = min(start_pr + per_page, total_prs + 1)

    prs = [generate_mock_pr(i) for i in range(start_pr, end_pr)]

    # Add Link header for pagination
    response = jsonify(prs)

    if end_pr <= total_prs:
        next_page = page + 1
        # Simplified Link header (real GitHub API is more complex)
        link_header = f'<http://mock-github-api:5000/repos/{owner}/{repo}/pulls?page={next_page}&per_page={per_page}>; rel="next"'
        response.headers["Link"] = link_header

    # Add mock rate limit headers
    response.headers["X-RateLimit-Limit"] = "5000"
    response.headers["X-RateLimit-Remaining"] = "4999"
    response.headers["X-RateLimit-Reset"] = str(
        int(datetime.now(timezone.utc).timestamp()) + 3600
    )

    return response


@app.route("/repos/<owner>/<repo>/pulls/<int:pr_number>/commits", methods=["GET"])
def get_pr_commits(owner: str, repo: str, pr_number: int) -> dict:
    """Mock endpoint for listing pull request commits."""
    # For testing, generate a random number of commits between 1 and 20
    commit_count = random.randint(1, 20)
    commits = generate_mock_commits(pr_number, commit_count)

    response = jsonify(commits)

    # Add mock rate limit headers
    response.headers["X-RateLimit-Limit"] = "5000"
    response.headers["X-RateLimit-Remaining"] = "4999"
    response.headers["X-RateLimit-Reset"] = str(
        int(datetime.now(timezone.utc).timestamp()) + 3600
    )

    return response


@app.route("/repos/<owner>/<repo>/issues/<int:pr_number>/comments", methods=["GET"])
def get_pr_comments(owner: str, repo: str, pr_number: int) -> dict:
    """Mock endpoint for listing pull request comments."""
    # For testing, generate a random number of comments between 0 and 10
    comment_count = random.randint(0, 10)
    comments = generate_mock_comments(pr_number, comment_count)

    response = jsonify(comments)

    # Add mock rate limit headers
    response.headers["X-RateLimit-Limit"] = "5000"
    response.headers["X-RateLimit-Remaining"] = "4999"
    response.headers["X-RateLimit-Reset"] = str(
        int(datetime.now(timezone.utc).timestamp()) + 3600
    )

    return response


@app.route("/repos/<owner>/<repo>/pulls/<int:pr_number>/reviews", methods=["GET"])
def get_pr_reviewers(owner: str, repo: str, pr_number: int) -> dict:
    """Mock endpoint for listing pull request reviews."""
    # For testing, generate a random number of reviewers between 0 and 5
    reviewer_count = random.randint(0, 5)
    reviewers = generate_mock_reviewers(pr_number, reviewer_count)

    response = jsonify(reviewers)

    # Add mock rate limit headers
    response.headers["X-RateLimit-Limit"] = "5000"
    response.headers["X-RateLimit-Remaining"] = "4999"
    response.headers["X-RateLimit-Reset"] = str(
        int(datetime.now(timezone.utc).timestamp()) + 3600
    )

    return response


@app.route("/repos/<owner>/<repo>/commits/<ref>", methods=["GET"])
def get_commit(owner: str, repo: str, ref: str) -> dict:
    """Mock endpoint for getting a single commit with file information."""
    # Generate a random number of files between 1 and 15
    file_count = random.randint(1, 15)
    files = generate_mock_commit_files(ref, file_count)

    # Create commit data matching GitHub API response structure
    created_date = datetime.now(timezone.utc) - timedelta(days=random.randint(1, 365))
    commit_data = {
        "sha": ref,
        "commit": {
            "message": f"Mock commit message for {ref}",
            "author": {
                "name": f"Author{random.randint(0, 4)}",
                "email": f"author{random.randint(0, 4)}@example.com",
                "date": created_date.strftime(DATE_FORMAT),
            },
            "committer": {
                "name": f"Committer{random.randint(0, 4)}",
                "email": f"committer{random.randint(0, 4)}@example.com",
                "date": created_date.strftime(DATE_FORMAT),
            },
        },
        "author": {
            "login": f"author{random.randint(0, 4)}",
            "id": 4000 + random.randint(0, 4),
        },
        "committer": {
            "login": f"committer{random.randint(0, 4)}",
            "id": 4000 + random.randint(0, 4),
        },
        "html_url": f"https://github.com/{owner}/{repo}/commit/{ref}",
        "files": files,
        "stats": {
            "additions": sum(f["additions"] for f in files),
            "deletions": sum(f["deletions"] for f in files),
            "total": sum(f["changes"] for f in files),
        },
    }

    response = jsonify(commit_data)

    # Add mock rate limit headers
    response.headers["X-RateLimit-Limit"] = "5000"
    response.headers["X-RateLimit-Remaining"] = "4999"
    response.headers["X-RateLimit-Reset"] = str(
        int(datetime.now(timezone.utc).timestamp()) + 3600
    )

    return response


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
