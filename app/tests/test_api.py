import os

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from unittest.mock import MagicMock, patch
from app.main import app  # Import your FastAPI app instance

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
client = TestClient(app)


@pytest.fixture
def mock_bigquery_client():
    """
    Fixture to mock the BigQuery client and its query job.
    """
    with patch("app.routers.queryrouter.client") as mock_client:
        # Simulate the row objects returned by BigQuery
        mock_row = MagicMock()
        mock_row.items.return_value = [("id", 1), ("Category", "Electronics")]
        mock_row.__iter__.return_value = iter([("id", 1), ("Category", "Electronics")])

        # Mock the query job results
        mock_query_job = MagicMock()
        mock_query_job.__iter__.return_value = [mock_row]

        mock_client.query.return_value = mock_query_job
        yield mock_client


# --- Tests ---

@pytest.fixture
def mock_bigquery_client():
    """Fixtures to mock the BigQuery client."""
    with patch("app.routers.queryrouter.client") as mock: # Patch the 'client' object in your route file
        yield mock

## --- Tests ---
def test_get_segments_success(mock_bigquery_client):
    # 1. Arrange: Setup what the mock should return
    mock_row_1 = MagicMock()
    mock_row_1.items.return_value = [("Segment", "Electronics"), ("total_amount", 5000)]
    mock_row_1.__iter__.return_value = iter([("Segment", "Electronics"), ("total_amount", 5000)])

    mock_row_2 = MagicMock()
    mock_row_2.items.return_value = [("Segment", "Clothing"), ("total_amount", 3000)]
    mock_row_2.__iter__.return_value = iter([("Segment", "Clothing"), ("total_amount", 3000)])

    # Mock the query_job to be iterable (for the results loop)
    mock_bigquery_client.query.return_value = [mock_row_1, mock_row_2]

    # 2. Act: Call the endpoint
    response = client.get("/query/segments")

    # 3. Assert: Check response matches expectations
    assert response.status_code == 200
    assert response.json() == [
        {"Segment": "Electronics", "total_amount": 5000},
        {"Segment": "Clothing", "total_amount": 3000}
    ]
def test_get_segments_not_found(mock_bigquery_client):
    # Arrange: Return an empty list to trigger the 404
    mock_bigquery_client.query.return_value = []

    # Act
    response = client.get("/segments")

    # Assert
    assert response.status_code == 404
    assert response.json()["detail"] == "Not Found"

def test_get_segments_server_error(mock_bigquery_client):
    # Arrange: Simulate a BigQuery crash/exception
    mock_bigquery_client.query.side_effect = Exception("BigQuery Connection Failed")

    # Act
    response = client.get("/segments")

    # Assert
    assert response.status_code == 404
    assert response.json()["detail"] == "Not Found"


def create_mock_row(data_dict):
    """Helper to simulate a BigQuery Row object."""
    mock_row = MagicMock()
    # Mock row.items() which is used in your: [dict(row.items()) for row in query_job]
    mock_row.items.return_value = data_dict.items()
    # Mock dict(row) just in case for the second transformation
    mock_row.__iter__.return_value = iter(data_dict.items())
    return mock_row


## --- Tests ---

@patch("app.routers.queryrouter.client")
def test_get_topproducts_success(mock_bq_client):
    # 1. Arrange: Define the fake data BigQuery would return
    fake_data = [
        {"ProductName": "Laptop", "items_sold": 50, "total_amount": 50000},
        {"ProductName": "Mouse", "items_sold": 200, "total_amount": 4000}
    ]

    # Transform dicts into our mock row objects
    mock_rows = [create_mock_row(item) for item in fake_data]
    mock_bq_client.query.return_value = mock_rows

    # 2. Act
    response = client.get("query/topproducts/")

    # 3. Assert
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 2
    assert data[0]["ProductName"] == "Laptop"
    assert data[1]["items_sold"] == 200


@patch("app.routers.queryrouter.client")
def test_get_topproducts_empty(mock_bq_client):
    # Arrange: BigQuery returns an empty iterator
    mock_bq_client.query.return_value = []

    # Act
    response = client.get("/query/topproducts/")

    # Assert
    assert response.status_code == 500
    #assert response.json()["detail"] == "Item not found"


@patch("app.routers.queryrouter.client")
def test_get_topproducts_error(mock_bq_client):
    # Arrange: Simulate a BigQuery timeout or auth error
    mock_bq_client.query.side_effect = Exception("BigQuery connection error")

    # Act
    response = client.get("/query/topproducts/")

    # Assert
    assert response.status_code == 500
    assert response.json()["detail"] == "Internal server error"



    #------------------------------------------
    # ------------------------------------------
    # ------------------------------------------


def mock_bq_row(data):
    mock = MagicMock()
    mock.items.return_value = data.items()
    mock.__iter__.return_value = iter(data.items())
    return mock


## --- Tests ---

@patch("app.routers.queryrouter.client")
def test_get_category_success(mock_bq_client):
    # 1. Arrange: Setup fake BigQuery response
    fake_row = mock_bq_row({"Category": "Electronics", "avg_amount": 450.50})
    mock_bq_client.query.return_value = [fake_row]

    # 2. Act: Pass the category_name as a query parameter
    response = client.get("/query/category/", params={"category_name": "Electronics"})

    # 3. Assert
    assert response.status_code == 200
    assert response.json() == [{"Category": "Electronics", "avg_amount": 450.50}]

    # Verify that the BigQuery client was called with the correct parameters
    args, kwargs = mock_bq_client.query.call_args
    job_config = kwargs.get("job_config")

    # Check if the scalar parameter was set correctly
    param = job_config.query_parameters[0]
    assert param.name == "category"
    assert param.value == "Electronics"


@patch("app.routers.queryrouter.client")
def test_get_category_not_found(mock_bq_client):
    # Arrange: Mock empty result
    mock_bq_client.query.return_value = []

    # Act
    response = client.get("/query/category/", params={"category_name": "NonExistent"})

    # Assert
    assert response.status_code == 404
    assert response.json()["detail"] == "Item not found"


@patch("app.routers.queryrouter.client")
def test_get_category_missing_param(mock_bq_client):
    # Act: Call without the required 'category_name' query string
    response = client.get("/query/category/")

    # Assert: FastAPI should return 422 Unprocessable Entity automatically
    assert response.status_code == 422


@patch("app.routers.queryrouter.client")
def test_get_category_server_error(mock_bq_client):
    # Arrange: Simulate a BigQuery crash
    mock_bq_client.query.side_effect = Exception("Connection Reset")

    # Act
    response = client.get("/query/category/", params={"category_name": "Books"})

    # Assert
    assert response.status_code == 404
    assert response.json()["detail"] == "Item not found"