# Testing Guide for GitHub ETL

This document describes comprehensive testing for the GitHub ETL pipeline, including
unit tests, integration tests, Docker testing, linting, and CI/CD workflows.

## Table of Contents

1. [Unit Testing](#unit-testing)
2. [Test Organization](#test-organization)
3. [Running Tests](#running-tests)
4. [Code Coverage](#code-coverage)
5. [Linting and Code Quality](#linting-and-code-quality)
6. [CI/CD Integration](#cicd-integration)
7. [Docker Testing](#docker-testing)
8. [Adding New Tests](#adding-new-tests)

---

## Unit Testing

The test suite in `test_main.py` provides comprehensive coverage for all functions in `main.py`.
We have **95 unit tests** covering 9 functions with 80%+ code coverage requirement.

### Test Structure

Tests are organized into 10 test classes:

1. **TestSetupLogging** (1 test) - Logging configuration
2. **TestSleepForRateLimit** (4 tests) - Rate limit handling
3. **TestExtractPullRequests** (14 tests) - PR extraction with pagination and enrichment
4. **TestExtractCommits** (9 tests) - Commit and file extraction
5. **TestExtractReviewers** (6 tests) - Reviewer extraction
6. **TestExtractComments** (7 tests) - Comment extraction (uses /issues endpoint)
7. **TestTransformData** (26 tests) - Data transformation for all 4 BigQuery tables
8. **TestLoadData** (8 tests) - BigQuery data loading
9. **TestMain** (17 tests) - Main ETL orchestration
10. **TestIntegration** (3 tests) - End-to-end integration tests (marked with `@pytest.mark.integration`)

### Fixtures

Reusable fixtures are defined at the top of `test_main.py`:

- `mock_session` - Mocked `requests.Session`
- `mock_bigquery_client` - Mocked BigQuery client
- `mock_pr_response` - Realistic pull request response
- `mock_commit_response` - Realistic commit with files
- `mock_reviewer_response` - Realistic reviewer response
- `mock_comment_response` - Realistic comment response

## Test Organization

### Function Coverage

| Function | Tests | Coverage Target | Key Test Areas |
|----------|-------|-----------------|----------------|
| `setup_logging()` | 1 | 100% | Logger configuration |
| `sleep_for_rate_limit()` | 4 | 100% | Rate limit sleep logic, edge cases |
| `extract_pull_requests()` | 14 | 90%+ | Pagination, rate limits, enrichment, error handling |
| `extract_commits()` | 9 | 85%+ | Commit/file fetching, rate limits, errors |
| `extract_reviewers()` | 6 | 85%+ | Reviewer states, rate limits, errors |
| `extract_comments()` | 7 | 85%+ | Comment fetching (via /issues), rate limits |
| `transform_data()` | 26 | 95%+ | Bug ID extraction, 4 tables, field mapping |
| `load_data()` | 8 | 90%+ | BigQuery insertion, snapshot dates, errors |
| `main()` | 17 | 85%+ | Env vars, orchestration, chunking |

**Overall Target: 85-90% coverage** (80% minimum enforced in CI)

### Critical Test Cases

#### Bug ID Extraction
Tests verify the regex pattern matches:
- `Bug 1234567 - Fix` → 1234567
- `bug 1234567` → 1234567
- `b=1234567` → 1234567
- `Bug #1234567` → 1234567
- Filters out IDs >= 100000000

#### Data Transformation
Tests ensure correct transformation for all 4 BigQuery tables:
- **pull_requests**: PR metadata, bug IDs, labels, date_approved
- **commits**: Flattened files (one row per file), commit metadata
- **reviewers**: Review states, date_approved calculation
- **comments**: Character count, status mapping from reviews

#### Rate Limiting
Tests verify rate limit handling at all API levels:
- Pull requests pagination
- Commit fetching
- Reviewer fetching
- Comment fetching

## Running Tests

### All Tests with Coverage

```bash
pytest
```

This runs all tests with coverage reporting (configured in `pytest.ini`).

### Fast Unit Tests Only (Skip Integration)

```bash
pytest -m "not integration and not slow"
```

Use this for fast feedback during development.

### Specific Test Class

```bash
pytest test_main.py::TestTransformData
```

### Specific Test Function

```bash
pytest test_main.py::TestTransformData::test_bug_id_extraction_basic -v
```

### With Verbose Output

```bash
pytest -v
```

### With Coverage Report

```bash
# Terminal report
pytest --cov=main --cov-report=term-missing

# HTML report
pytest --cov=main --cov-report=html
open htmlcov/index.html
```

### Integration Tests Only

```bash
pytest -m integration
```

## Code Coverage

### Coverage Requirements

- **Minimum**: 80% (enforced in CI via `--cov-fail-under=80`)
- **Target**: 85-90%
- **Current**: Run `pytest --cov=main` to see current coverage

### Coverage Configuration

Coverage settings are in `pytest.ini`:

```ini
[pytest]
addopts =
    --cov=main
    --cov-report=term-missing
    --cov-report=html
    --cov-branch
    --cov-fail-under=80
```

### Viewing Coverage

```bash
# Generate HTML coverage report
pytest --cov=main --cov-report=html

# Open in browser
xdg-open htmlcov/index.html  # Linux
open htmlcov/index.html      # macOS
```

The HTML report shows:
- Line-by-line coverage
- Branch coverage
- Missing lines highlighted
- Per-file coverage percentages

## Linting and Code Quality

### Available Linters

The project uses these linting tools (defined in `requirements.txt`):

- **black** - Code formatting
- **isort** - Import sorting
- **flake8** - Style and syntax checking
- **mypy** - Static type checking

### Running Linters

```bash
# Run black (auto-format)
black main.py test_main.py

# Check formatting without changes
black --check main.py test_main.py

# Sort imports
isort main.py test_main.py

# Check import sorting
isort --check-only main.py test_main.py

# Run flake8
flake8 main.py test_main.py --max-line-length=100 --extend-ignore=E203,W503

# Run mypy
mypy main.py --no-strict-optional --ignore-missing-imports
```

### All Linting Checks

```bash
# Run all linters in sequence
black --check main.py test_main.py && \
isort --check-only main.py test_main.py && \
flake8 main.py test_main.py --max-line-length=100 --extend-ignore=E203,W503 && \
mypy main.py --no-strict-optional --ignore-missing-imports
```

## CI/CD Integration

### GitHub Actions Workflow

The `.github/workflows/tests.yml` workflow runs on every pull request:

**Lint Job:**
1. Runs black (format check)
2. Runs isort (import check)
3. Runs flake8 (style check)
4. Runs mypy (type check)

**Test Job:**
1. Runs fast unit tests with 80% coverage threshold
2. Runs all tests (including integration)
3. Uploads coverage reports as artifacts

### Workflow Triggers

- Pull requests to `main` branch

### Viewing Results

- Check the Actions tab in GitHub
- Coverage artifacts are uploaded for each run
- Failed linting or tests will block merges

## Docker Testing

## Overview

The `docker-compose.yml` configuration provides a complete local testing environment with:

1. **Mock GitHub API** - A Flask-based mock service that simulates the GitHub Pull Requests API
2. **BigQuery Emulator** - A local BigQuery instance for testing data loads
3. **ETL Service** - The main GitHub ETL application configured to use the mock services

## Quick Start

### Start all services

```bash
docker-compose up --build
```

This will:

- Build and start the mock GitHub API (port 5000)
- Start the BigQuery emulator (ports 9050, 9060)
- Build and run the ETL service

The ETL service will automatically:

- Fetch 250 mock pull requests from the mock GitHub API
- Transform the data
- Load it into the BigQuery emulator

### View logs

```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f github-etl
docker-compose logs -f bigquery-emulator
docker-compose logs -f mock-github-api
```

### Stop services

```bash
docker-compose down
```

## Architecture

### Mock GitHub API Service

- **Port**: 5000
- **Endpoint**: `http://localhost:5000/repos/{owner}/{repo}/pulls`
- **Mock data**: Generates 250 sample pull requests with realistic data
- **Features**:
  - Pagination support (per_page, page parameters)
  - Realistic PR data (numbers, titles, states, timestamps, users, etc.)
  - Mock rate limit headers
  - No authentication required

### BigQuery Emulator Service

- **Ports**:
  - 9050 (BigQuery API)
  - 9060 (Discovery/Admin API)
- **Configuration**: Uses `data.yml` to define the schema
- **Project**: test-project
- **Dataset**: test_dataset
- **Table**: pull_requests

### ETL Service

The ETL service is configured via environment variables in `docker-compose.yml`:

```yaml
environment:
  GITHUB_REPOS: "mozilla/firefox"
  GITHUB_API_URL: "http://mock-github-api:5000"  # Points to mock API
  BIGQUERY_PROJECT: "test"
  BIGQUERY_DATASET: "github_etl"
  BIGQUERY_EMULATOR_HOST: "http://bigquery-emulator:9050"
```

## Customization

### Using Real GitHub API

To test with the real GitHub API instead of the mock:

1. Set `GITHUB_TOKEN` environment variable
2. Remove or comment out `GITHUB_API_URL` in docker-compose.yml
3. Update `depends_on` to not require mock-github-api

```bash
export GITHUB_TOKEN="your_github_token"
docker-compose up github-etl bigquery-emulator
```

### Adjusting Mock Data

Edit `mock_github_api.py` to customize:

- Total number of PRs (default: 250)
- PR field values
- Pagination behavior

### Modifying BigQuery Schema

Edit `data.yml` to change the table schema. The schema matches the fields
extracted in `main.py`'s `transform_data()` function.

## Querying the BigQuery Emulator

You can query the BigQuery emulator using the BigQuery Python client:

```python
from google.cloud import bigquery
from google.api_core.client_options import ClientOptions

client = bigquery.Client(
    project="test-project",
    client_options=ClientOptions(api_endpoint="http://localhost:9050")
)

query = """
SELECT pr_number, title, state, user_login
FROM `test-project.test_dataset.pull_requests`
LIMIT 10
"""

for row in client.query(query):
    print(f"PR #{row.pr_number}: {row.title} - {row.state}")
```

Or use the `bq` command-line tool with the emulator endpoint.

## Troubleshooting

### Services not starting

Check if ports are already in use:

```bash
lsof -i :5000  # Mock GitHub API
lsof -i :9050  # BigQuery emulator
```

### ETL fails to connect

Ensure services are healthy:

```bash
docker-compose ps
```

Check service logs:

```bash
docker-compose logs bigquery-emulator
docker-compose logs mock-github-api
```

### Schema mismatch errors

Verify `data.yml` schema matches fields in `main.py:transform_data()`.

## Development Workflow

1. Make changes to `main.py`
2. Restart the ETL service: `docker-compose restart github-etl`
3. View logs: `docker-compose logs -f github-etl`

The `main.py` file is mounted as a volume, so changes are reflected without rebuilding.

## Cleanup

Remove all containers and volumes:

```bash
docker-compose down -v
```

Remove built images:

```bash
docker-compose down --rmi all
```

---

## Adding New Tests

### Testing Patterns

#### 1. Mock External Dependencies

Always mock external API calls and BigQuery operations:

```python
@patch("requests.Session")
def test_api_call(mock_session_class):
    mock_session = MagicMock()
    mock_session_class.return_value = mock_session

    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = [{"id": 1}]

    mock_session.get.return_value = mock_response
    # Test code here
```

#### 2. Use Fixtures

Leverage existing fixtures for common test data:

```python
def test_with_fixtures(mock_session, mock_pr_response):
    # Use mock_session and mock_pr_response
    pass
```

#### 3. Test Edge Cases

Always test:
- Empty inputs
- None values
- Missing fields
- Rate limits
- API errors (404, 500, etc.)
- Boundary conditions

#### 4. Verify Call Arguments

Check that functions are called with correct parameters:

```python
mock_extract.assert_called_once_with(
    session=mock_session,
    repo="mozilla/firefox",
    github_api_url="https://api.github.com"
)
```

### Example: Adding a New Test

```python
class TestNewFunction:
    """Tests for new_function."""

    def test_basic_functionality(self, mock_session):
        """Test basic happy path."""
        # Arrange
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"result": "success"}
        mock_session.get.return_value = mock_response

        # Act
        result = main.new_function(mock_session, "arg1")

        # Assert
        assert result == {"result": "success"}
        mock_session.get.assert_called_once()

    def test_error_handling(self, mock_session):
        """Test error handling."""
        mock_response = Mock()
        mock_response.status_code = 500
        mock_response.text = "Internal Error"
        mock_session.get.return_value = mock_response

        with pytest.raises(SystemExit) as exc_info:
            main.new_function(mock_session, "arg1")

        assert "500" in str(exc_info.value)
```

### Test Organization Guidelines

1. **Group related tests** in test classes
2. **Use descriptive names** like `test_handles_rate_limit_on_commits`
3. **One assertion concept per test** - Test one thing at a time
4. **Arrange-Act-Assert pattern** - Structure tests clearly
5. **Add docstrings** to explain what each test verifies

### Mocking Patterns

#### Mocking Time

```python
@patch("time.time")
@patch("time.sleep")
def test_with_time(mock_sleep, mock_time):
    mock_time.return_value = 1000
    # Test code
```

#### Mocking Environment Variables

```python
with patch.dict(os.environ, {"VAR_NAME": "value"}, clear=True):
    # Test code
```

#### Mocking Generators

```python
mock_extract.return_value = iter([[{"id": 1}], [{"id": 2}]])
```

### Running Tests During Development

```bash
# Auto-run tests on file changes (requires pytest-watch)
pip install pytest-watch
ptw -- --cov=main -m "not integration"
```

### Debugging Tests

```bash
# Drop into debugger on failures
pytest --pdb

# Show print statements
pytest -s

# Verbose with full diff
pytest -vv
```

### Coverage Tips

If coverage is below 80%:

1. Run `pytest --cov=main --cov-report=term-missing` to see missing lines
2. Look for untested branches (if/else paths)
3. Check error handling paths
4. Verify edge cases are covered

## Resources

- [pytest documentation](https://docs.pytest.org/)
- [pytest-cov documentation](https://pytest-cov.readthedocs.io/)
- [unittest.mock documentation](https://docs.python.org/3/library/unittest.mock.html)

## Troubleshooting

### Tests Pass Locally But Fail in CI

- Check Python version (must be 3.11)
- Verify all dependencies are in `requirements.txt`
- Look for environment-specific issues

### Coverage Dropped Below 80%

- Run locally: `pytest --cov=main --cov-report=html`
- Open `htmlcov/index.html` to see uncovered lines
- Add tests for missing coverage

### Import Errors

- Ensure `PYTHONPATH` includes project root
- Check that `__init__.py` files exist if needed
- Verify module names match file names
