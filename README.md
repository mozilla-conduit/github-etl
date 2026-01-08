# github-etl

An ETL for the Mozilla Organization Firefox repositories

## Overview

This repository contains a Python-based ETL (Extract, Transform, Load) script
designed to process pull request data from Mozilla Organization Firefox
repositories on GitHub and load them into Google BigQuery. The application
runs in a Docker container for easy deployment and isolation.

## Features

- **Containerized**: Runs in a Docker container using the latest stable Python
- **Secure**: Runs as a non-root user (`app`) inside the container
- **Streaming Architecture**: Processes pull requests in chunks of 100 for memory efficiency
- **BigQuery Integration**: Loads data directly into BigQuery using the Python client library
- **Rate Limit Handling**: Automatically handles GitHub API rate limits
- **Comprehensive Logging**: Detailed logging for monitoring and debugging

## Quick Start

### Prerequisites

1. **GitHub Personal Access Token**: Create a [token](https://github.com/settings/tokens)
2. **Google Cloud Project**: Set up a GCP project with BigQuery enabled
3. **BigQuery Dataset**: Create a dataset in your GCP project
4. **Authentication**: Configure GCP credentials (see Authentication section below)

### Building the Docker Image

```bash
docker build -t github-etl .
```

### Running the Container

```bash
docker run --rm \
  -e GITHUB_REPOS="mozilla/firefox" \
  -e GITHUB_TOKEN="your_github_token" \
  -e BIGQUERY_PROJECT="your-gcp-project" \
  -e BIGQUERY_DATASET="your_dataset" \
  -e GOOGLE_APPLICATION_CREDENTIALS="/path/to/credentials.json" \
  -v /local/path/to/credentials.json:/path/to/credentials.json \
  github-etl
```

### Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `GITHUB_REPOS` | Yes | - | Comma separated repositories in format "owner/repo" (e.g., "mozilla/firefox") |
| `GITHUB_TOKEN` | No | - | GitHub Personal Access Token (recommended to avoid rate limits) |
| `BIGQUERY_PROJECT` | Yes | - | Google Cloud Project ID |
| `BIGQUERY_DATASET` | Yes | - | BigQuery dataset ID |
| `GOOGLE_APPLICATION_CREDENTIALS` | Yes* | - | Path to GCP service account JSON file (*or use Workload Identity) |

## Architecture

### Components

- **`main.py`**: The main ETL script containing the business logic
- **`requirements.txt`**: Python dependencies
- **`Dockerfile`**: Container configuration

### Container Specifications

- **Base Image**: `python:3.11-slim` (latest stable Python)
- **User**: `app` (uid: 1000, gid: 1000)
- **Working Directory**: `/app`
- **Ownership**: All files in `/app` are owned by the `app` user

### ETL Process

The pipeline uses a **streaming/chunked architecture** that processes pull
requests in batches of 100:

1. **Extract**: Generator yields chunks of 100 PRs from GitHub API
   - Implements pagination and rate limit handling
   - Fetches all pull requests (open, closed, merged) sorted by creation date

2. **Transform**: Flattens and structures PR data for BigQuery
   - Extracts key fields (number, title, state, timestamps, user info)
   - Flattens nested objects (user, head/base branches)
   - Converts arrays (labels, assignees) to JSON strings

3. **Load**: Inserts transformed data into BigQuery
   - Uses BigQuery Python client library
   - Adds snapshot_date timestamp to all rows
   - Immediate insertion after each chunk is transformed

**Benefits of Chunked Processing**:

- Memory-efficient for large repositories
- Incremental progress visibility
- Early failure detection
- Supports streaming data pipelines

## Authentication

### Google Cloud Authentication

The script uses the BigQuery Python client library which supports multiple
authentication methods:

1. **Service Account Key File** (Recommended for local development):

   ```bash
   export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account-key.json"
   ```

1. **Workload Identity** (Recommended for Kubernetes):
   - Configure Workload Identity on your GKE cluster
   - No explicit credentials file needed

1. **Application Default Credentials** (For local development):

   ```bash
   gcloud auth application-default login
   ```

## Development

### Local Development

Set up environment variables and run the script:

```bash
export GITHUB_REPOS="mozilla/firefox"
export GITHUB_TOKEN="your_github_token"
export BIGQUERY_PROJECT="your-gcp-project"
export BIGQUERY_DATASET="your_dataset"

python3 main.py
```

### Local Testing with Docker Compose

For local development and testing, you can use Docker Compose to run the ETL
with mocked services (no GitHub API rate limits or GCP credentials required):

```bash
# Start all services (mock GitHub API, BigQuery emulator, and ETL)
docker-compose up --build

# View logs
docker-compose logs -f github-etl

# Stop services
docker-compose down
```

This setup includes:

- **Mock GitHub API**: Generates 250 sample pull requests
- **BigQuery Emulator**: Local BigQuery instance for testing
- **ETL Service**: Configured to use both mock services
  
### Adding Dependencies

Add new Python packages to `requirements.txt` and rebuild the Docker image.

## License

This project is licensed under the Mozilla Public License Version 2.0. See the
[LICENSE](LICENSE) file for details.
