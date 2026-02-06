#!/usr/bin/env python3
"""
Tests for transform_data function.

Tests data transformation including bug ID extraction, label processing,
commit/reviewer/comment flattening, and field mapping.
"""

import main


def test_transform_data_basic():
    """Test basic transformation of pull request data."""
    raw_data = [
        {
            "number": 123,
            "title": "Fix login bug",
            "state": "closed",
            "created_at": "2024-01-01T10:00:00Z",
            "updated_at": "2024-01-02T10:00:00Z",
            "merged_at": "2024-01-02T12:00:00Z",
            "labels": [],
            "commit_data": [],
            "reviewer_data": [],
            "comment_data": [],
        }
    ]

    result = main.transform_data(raw_data, "mozilla/firefox")

    assert len(result["pull_requests"]) == 1
    pr = result["pull_requests"][0]
    assert pr["pull_request_id"] == 123
    assert pr["current_status"] == "closed"
    assert pr["date_created"] == "2024-01-01T10:00:00Z"
    assert pr["date_modified"] == "2024-01-02T10:00:00Z"
    assert pr["date_landed"] == "2024-01-02T12:00:00Z"
    assert pr["target_repository"] == "mozilla/firefox"


def test_bug_id_extraction_basic():
    """Test bug ID extraction from PR title."""
    test_cases = [
        ("Bug 1234567 - Fix issue", 1234567),
        ("bug 1234567: Update code", 1234567),
        ("Fix for bug 7654321", 7654321),
        ("b=9876543 - Change behavior", 9876543),
    ]

    for title, expected_bug_id in test_cases:
        raw_data = [
            {
                "number": 1,
                "title": title,
                "state": "open",
                "labels": [],
                "commit_data": [],
                "reviewer_data": [],
                "comment_data": [],
            }
        ]

        result = main.transform_data(raw_data, "mozilla/firefox")
        assert result["pull_requests"][0]["bug_id"] == expected_bug_id


def test_bug_id_extraction_with_hash():
    """Test bug ID extraction with # symbol."""
    raw_data = [
        {
            "number": 1,
            "title": "Bug #1234567 - Fix issue",
            "state": "open",
            "labels": [],
            "commit_data": [],
            "reviewer_data": [],
            "comment_data": [],
        }
    ]

    result = main.transform_data(raw_data, "mozilla/firefox")
    assert result["pull_requests"][0]["bug_id"] == 1234567


def test_bug_id_filter_large_numbers():
    """Test that bug IDs >= 100000000 are filtered out."""
    raw_data = [
        {
            "number": 1,
            "title": "Bug 999999999 - Invalid bug ID",
            "state": "open",
            "labels": [],
            "commit_data": [],
            "reviewer_data": [],
            "comment_data": [],
        }
    ]

    result = main.transform_data(raw_data, "mozilla/firefox")
    assert result["pull_requests"][0]["bug_id"] is None


def test_bug_id_no_match():
    """Test PR title with no bug ID."""
    raw_data = [
        {
            "number": 1,
            "title": "Update documentation",
            "state": "open",
            "labels": [],
            "commit_data": [],
            "reviewer_data": [],
            "comment_data": [],
        }
    ]

    result = main.transform_data(raw_data, "mozilla/firefox")
    assert result["pull_requests"][0]["bug_id"] is None


def test_labels_extraction():
    """Test labels array extraction."""
    raw_data = [
        {
            "number": 1,
            "title": "PR with labels",
            "state": "open",
            "labels": [
                {"name": "bug"},
                {"name": "priority-high"},
                {"name": "needs-review"},
            ],
            "commit_data": [],
            "reviewer_data": [],
            "comment_data": [],
        }
    ]

    result = main.transform_data(raw_data, "mozilla/firefox")
    labels = result["pull_requests"][0]["labels"]
    assert len(labels) == 3
    assert "bug" in labels
    assert "priority-high" in labels
    assert "needs-review" in labels


def test_labels_empty_list():
    """Test handling empty labels list."""
    raw_data = [
        {
            "number": 1,
            "title": "PR without labels",
            "state": "open",
            "labels": [],
            "commit_data": [],
            "reviewer_data": [],
            "comment_data": [],
        }
    ]

    result = main.transform_data(raw_data, "mozilla/firefox")
    assert result["pull_requests"][0]["labels"] == []


def test_commit_transformation():
    """Test commit fields mapping."""
    raw_data = [
        {
            "number": 123,
            "title": "PR with commits",
            "state": "open",
            "labels": [],
            "commit_data": [
                {
                    "sha": "abc123",
                    "commit": {
                        "author": {
                            "name": "Test Author",
                            "date": "2024-01-01T12:00:00Z",
                        }
                    },
                    "files": [
                        {
                            "filename": "src/main.py",
                            "additions": 10,
                            "deletions": 5,
                        }
                    ],
                }
            ],
            "reviewer_data": [],
            "comment_data": [],
        }
    ]

    result = main.transform_data(raw_data, "mozilla/firefox")

    assert len(result["commits"]) == 1
    commit = result["commits"][0]
    assert commit["pull_request_id"] == 123
    assert commit["target_repository"] == "mozilla/firefox"
    assert commit["commit_sha"] == "abc123"
    assert commit["date_created"] == "2024-01-01T12:00:00Z"
    assert commit["author_username"] == "Test Author"
    assert commit["filename"] == "src/main.py"
    assert commit["lines_added"] == 10
    assert commit["lines_removed"] == 5


def test_commit_file_flattening():
    """Test that each file becomes a separate row."""
    raw_data = [
        {
            "number": 123,
            "title": "PR with multiple files",
            "state": "open",
            "labels": [],
            "commit_data": [
                {
                    "sha": "abc123",
                    "commit": {"author": {"name": "Author", "date": "2024-01-01"}},
                    "files": [
                        {"filename": "file1.py", "additions": 10, "deletions": 5},
                        {"filename": "file2.py", "additions": 20, "deletions": 2},
                        {"filename": "file3.py", "additions": 5, "deletions": 15},
                    ],
                }
            ],
            "reviewer_data": [],
            "comment_data": [],
        }
    ]

    result = main.transform_data(raw_data, "mozilla/firefox")

    # Should have 3 rows in commits table (one per file)
    assert len(result["commits"]) == 3
    filenames = [c["filename"] for c in result["commits"]]
    assert "file1.py" in filenames
    assert "file2.py" in filenames
    assert "file3.py" in filenames


def test_multiple_commits_with_files():
    """Test multiple commits with multiple files per PR."""
    raw_data = [
        {
            "number": 123,
            "title": "PR with multiple commits",
            "state": "open",
            "labels": [],
            "commit_data": [
                {
                    "sha": "commit1",
                    "commit": {"author": {"name": "Author1", "date": "2024-01-01"}},
                    "files": [
                        {"filename": "file1.py", "additions": 10, "deletions": 0}
                    ],
                },
                {
                    "sha": "commit2",
                    "commit": {"author": {"name": "Author2", "date": "2024-01-02"}},
                    "files": [
                        {"filename": "file2.py", "additions": 5, "deletions": 2},
                        {"filename": "file3.py", "additions": 8, "deletions": 3},
                    ],
                },
            ],
            "reviewer_data": [],
            "comment_data": [],
        }
    ]

    result = main.transform_data(raw_data, "mozilla/firefox")

    # Should have 3 rows total (1 file from commit1, 2 files from commit2)
    assert len(result["commits"]) == 3
    assert result["commits"][0]["commit_sha"] == "commit1"
    assert result["commits"][1]["commit_sha"] == "commit2"
    assert result["commits"][2]["commit_sha"] == "commit2"


def test_reviewer_transformation():
    """Test reviewer fields mapping."""
    raw_data = [
        {
            "number": 123,
            "title": "PR with reviewers",
            "state": "open",
            "labels": [],
            "commit_data": [],
            "reviewer_data": [
                {
                    "id": 789,
                    "user": {"login": "reviewer1"},
                    "state": "APPROVED",
                    "submitted_at": "2024-01-01T15:00:00Z",
                }
            ],
            "comment_data": [],
        }
    ]

    result = main.transform_data(raw_data, "mozilla/firefox")

    assert len(result["reviewers"]) == 1
    reviewer = result["reviewers"][0]
    assert reviewer["pull_request_id"] == 123
    assert reviewer["target_repository"] == "mozilla/firefox"
    assert reviewer["reviewer_username"] == "reviewer1"
    assert reviewer["status"] == "APPROVED"
    assert reviewer["date_reviewed"] == "2024-01-01T15:00:00Z"


def test_transform_multiple_review_states():
    """Test transforming data with multiple review states."""
    raw_data = [
        {
            "number": 123,
            "title": "PR with multiple reviews",
            "state": "open",
            "labels": [],
            "commit_data": [],
            "reviewer_data": [
                {
                    "id": 1,
                    "user": {"login": "user1"},
                    "state": "APPROVED",
                    "submitted_at": "2024-01-01T15:00:00Z",
                },
                {
                    "id": 2,
                    "user": {"login": "user2"},
                    "state": "CHANGES_REQUESTED",
                    "submitted_at": "2024-01-01T16:00:00Z",
                },
                {
                    "id": 3,
                    "user": {"login": "user3"},
                    "state": "COMMENTED",
                    "submitted_at": "2024-01-01T17:00:00Z",
                },
            ],
            "comment_data": [],
        }
    ]

    result = main.transform_data(raw_data, "mozilla/firefox")

    assert len(result["reviewers"]) == 3
    states = [r["status"] for r in result["reviewers"]]
    assert "APPROVED" in states
    assert "CHANGES_REQUESTED" in states
    assert "COMMENTED" in states


def test_date_approved_from_earliest_approval():
    """Test that date_approved is set to earliest APPROVED review."""
    raw_data = [
        {
            "number": 123,
            "title": "PR with multiple approvals",
            "state": "open",
            "labels": [],
            "commit_data": [],
            "reviewer_data": [
                {
                    "id": 1,
                    "user": {"login": "user1"},
                    "state": "APPROVED",
                    "submitted_at": "2024-01-02T15:00:00Z",
                },
                {
                    "id": 2,
                    "user": {"login": "user2"},
                    "state": "APPROVED",
                    "submitted_at": "2024-01-01T14:00:00Z",  # Earliest
                },
                {
                    "id": 3,
                    "user": {"login": "user3"},
                    "state": "APPROVED",
                    "submitted_at": "2024-01-03T16:00:00Z",
                },
            ],
            "comment_data": [],
        }
    ]

    result = main.transform_data(raw_data, "mozilla/firefox")

    pr = result["pull_requests"][0]
    assert pr["date_approved"] == "2024-01-01T14:00:00Z"


def test_comment_transformation():
    """Test comment fields mapping."""
    raw_data = [
        {
            "number": 123,
            "title": "PR with comments",
            "state": "open",
            "labels": [],
            "commit_data": [],
            "reviewer_data": [],
            "comment_data": [
                {
                    "id": 456,
                    "user": {"login": "commenter1"},
                    "body": "This looks great!",
                    "created_at": "2024-01-01T14:00:00Z",
                    "pull_request_review_id": None,
                }
            ],
        }
    ]

    result = main.transform_data(raw_data, "mozilla/firefox")

    assert len(result["comments"]) == 1
    comment = result["comments"][0]
    assert comment["pull_request_id"] == 123
    assert comment["target_repository"] == "mozilla/firefox"
    assert comment["comment_id"] == 456
    assert comment["author_username"] == "commenter1"
    assert comment["date_created"] == "2024-01-01T14:00:00Z"
    assert comment["character_count"] == 17


def test_comment_character_count():
    """Test character count calculation for comments."""
    raw_data = [
        {
            "number": 123,
            "title": "PR",
            "state": "open",
            "labels": [],
            "commit_data": [],
            "reviewer_data": [],
            "comment_data": [
                {
                    "id": 1,
                    "user": {"login": "user1"},
                    "body": "Short",
                    "created_at": "2024-01-01",
                },
                {
                    "id": 2,
                    "user": {"login": "user2"},
                    "body": "This is a much longer comment with more text",
                    "created_at": "2024-01-01",
                },
            ],
        }
    ]

    result = main.transform_data(raw_data, "mozilla/firefox")

    assert result["comments"][0]["character_count"] == 5
    assert result["comments"][1]["character_count"] == 44


def test_comment_status_from_review():
    """Test that comment status is mapped from review_id_statuses."""
    raw_data = [
        {
            "number": 123,
            "title": "PR",
            "state": "open",
            "labels": [],
            "commit_data": [],
            "reviewer_data": [
                {
                    "id": 789,
                    "user": {"login": "reviewer"},
                    "state": "APPROVED",
                    "submitted_at": "2024-01-01",
                }
            ],
            "comment_data": [
                {
                    "id": 456,
                    "user": {"login": "commenter"},
                    "body": "LGTM",
                    "created_at": "2024-01-01",
                    "pull_request_review_id": 789,
                }
            ],
        }
    ]

    result = main.transform_data(raw_data, "mozilla/firefox")

    # Comment should have status from the review
    assert result["comments"][0]["status"] == "APPROVED"


def test_comment_empty_body():
    """Test handling comments with empty or None body."""
    raw_data = [
        {
            "number": 123,
            "title": "PR",
            "state": "open",
            "labels": [],
            "commit_data": [],
            "reviewer_data": [],
            "comment_data": [
                {
                    "id": 1,
                    "user": {"login": "user1"},
                    "body": None,
                    "created_at": "2024-01-01",
                },
                {
                    "id": 2,
                    "user": {"login": "user2"},
                    "body": "",
                    "created_at": "2024-01-01",
                },
            ],
        }
    ]

    result = main.transform_data(raw_data, "mozilla/firefox")

    assert result["comments"][0]["character_count"] == 0
    assert result["comments"][1]["character_count"] == 0


def test_empty_raw_data():
    """Test handling empty input list."""
    result = main.transform_data([], "mozilla/firefox")

    assert result["pull_requests"] == []
    assert result["commits"] == []
    assert result["reviewers"] == []
    assert result["comments"] == []


def test_pr_without_commits_reviewers_comments():
    """Test PR with no commits, reviewers, or comments."""
    raw_data = [
        {
            "number": 123,
            "title": "Minimal PR",
            "state": "open",
            "labels": [],
            "commit_data": [],
            "reviewer_data": [],
            "comment_data": [],
        }
    ]

    result = main.transform_data(raw_data, "mozilla/firefox")

    assert len(result["pull_requests"]) == 1
    assert len(result["commits"]) == 0
    assert len(result["reviewers"]) == 0
    assert len(result["comments"]) == 0


def test_return_structure():
    """Test that transform_data returns dict with 4 keys."""
    raw_data = [
        {
            "number": 1,
            "title": "Test",
            "state": "open",
            "labels": [],
            "commit_data": [],
            "reviewer_data": [],
            "comment_data": [],
        }
    ]

    result = main.transform_data(raw_data, "mozilla/firefox")

    assert isinstance(result, dict)
    assert "pull_requests" in result
    assert "commits" in result
    assert "reviewers" in result
    assert "comments" in result


def test_all_tables_have_target_repository():
    """Test that all tables include target_repository field."""
    raw_data = [
        {
            "number": 123,
            "title": "Test PR",
            "state": "open",
            "labels": [],
            "commit_data": [
                {
                    "sha": "abc",
                    "commit": {"author": {"name": "Author", "date": "2024-01-01"}},
                    "files": [{"filename": "test.py", "additions": 1, "deletions": 0}],
                }
            ],
            "reviewer_data": [
                {
                    "id": 1,
                    "user": {"login": "reviewer"},
                    "state": "APPROVED",
                    "submitted_at": "2024-01-01",
                }
            ],
            "comment_data": [
                {
                    "id": 2,
                    "user": {"login": "commenter"},
                    "body": "Test",
                    "created_at": "2024-01-01",
                }
            ],
        }
    ]

    result = main.transform_data(raw_data, "mozilla/firefox")

    assert result["pull_requests"][0]["target_repository"] == "mozilla/firefox"
    assert result["commits"][0]["target_repository"] == "mozilla/firefox"
    assert result["reviewers"][0]["target_repository"] == "mozilla/firefox"
    assert result["comments"][0]["target_repository"] == "mozilla/firefox"
