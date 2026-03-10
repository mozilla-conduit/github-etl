from unittest.mock import MagicMock, Mock

import pytest
import requests
from google.cloud import bigquery


@pytest.fixture
def mock_session() -> Mock:
    session = Mock(spec=requests.Session)
    session.headers = {}
    return session


@pytest.fixture
def mock_bigquery_client() -> Mock:
    client = Mock(spec=bigquery.Client)
    client.project = "test-project"
    client.insert_rows_json = MagicMock(return_value=[])  # Empty list = no errors
    return client
