# github-etl
An ETL for the Mozilla Organization Firefox repositories

## Overview

This repository contains a Python-based ETL (Extract, Transform, Load) script designed to process data from Mozilla Organization Firefox repositories on GitHub. The application runs in a Docker container for easy deployment and isolation.

## Features

- **Containerized**: Runs in a Docker container using the latest stable Python
- **Secure**: Runs as a non-root user (`app`) inside the container
- **Structured**: Follows ETL patterns with separate extract, transform, and load phases
- **Logging**: Comprehensive logging for monitoring and debugging

## Quick Start

### Building the Docker Image

```bash
docker build -t github-etl .
```

### Running the Container

```bash
docker run --rm github-etl
```

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

1. **Extract**: Retrieves data from GitHub repositories
2. **Transform**: Processes and structures the data
3. **Load**: Stores the processed data in the target destination

## Development

### Local Development

You can run the script directly with Python:

```bash
python3 main.py
```

### Adding Dependencies

Add new Python packages to `requirements.txt` and rebuild the Docker image.

## License

This project is licensed under the Mozilla Public License Version 2.0. See the [LICENSE](LICENSE) file for details.
